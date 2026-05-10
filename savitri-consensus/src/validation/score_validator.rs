//!
//! ensuring nodes meet minimum requirements for participation.
//!
//! AUDIT-003 FIX: All score thresholds and weights converted from f64
//! to u32 permille (parts per 1000) for cross-architecture determinism.

use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

///
/// AUDIT-003 FIX: All f64 fields converted to u32 permille (0-1000).
#[derive(Debug, Clone)]
pub struct ScoreValidationConfig {
    /// Minimum score to be a proposer (permille, 0-1000; e.g. 700 = 70%)
    pub min_proposer_score: u32,
    pub min_validator_score: u32,
    /// Minimum uptime (permille, 0-1000; e.g. 950 = 95%)
    pub min_uptime: u32,
    /// Minimum latency score (permille, 0-1000; e.g. 600 = 60%)
    pub min_latency_score: u32,
    pub enable_decay_validation: bool,
    /// Maximum score age in seconds
    pub max_score_age_secs: u64,
    /// Score weight for uptime (permille, 0-1000)
    pub uptime_weight: u32,
    /// Score weight for latency (permille, 0-1000)
    pub latency_weight: u32,
    /// Score weight for participation (permille, 0-1000)
    pub participation_weight: u32,
}

impl Default for ScoreValidationConfig {
    fn default() -> Self {
        Self {
            min_proposer_score: 700,  // 70%
            min_validator_score: 500, // 50%
            min_uptime: 950,          // 95%
            min_latency_score: 600,   // 60%
            enable_decay_validation: true,
            max_score_age_secs: 3600,
            uptime_weight: 400,        // 40%
            latency_weight: 300,       // 30%
            participation_weight: 300, // 30%
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ScoreValidationStats {
    pub scores_validated: u64,
    pub scores_accepted: u64,
    pub scores_rejected: u64,
    pub proposer_qualifications: u64,
    pub validator_qualifications: u64,
    pub decay_rejections: u64,
}

/// Node score data
///
/// AUDIT-003 FIX: All f64 score fields converted to u32 (0-1000 permille scale).
#[derive(Debug, Clone)]
pub struct NodeScore {
    pub node_id: [u8; 32],
    /// Uptime score (permille, 0-1000)
    pub uptime_score: u32,
    /// Latency score (permille, 0-1000)
    pub latency_score: u32,
    /// Participation score (permille, 0-1000)
    pub participation_score: u32,
    /// Composite score (permille, 0-1000)
    pub composite_score: u32,
    pub last_updated: u64,
    pub epoch: u64,
}

impl NodeScore {
    /// Create new node score
    pub fn new(node_id: [u8; 32]) -> Self {
        Self {
            node_id,
            uptime_score: 0,
            latency_score: 0,
            participation_score: 0,
            composite_score: 0,
            last_updated: 0,
            epoch: 0,
        }
    }

    /// Calculate composite score using integer arithmetic
    ///
    /// AUDIT-003 FIX: Uses u64 intermediate to prevent overflow, then
    /// divides by 1000 (permille denominator).
    pub fn calculate_composite(&mut self, config: &ScoreValidationConfig) {
        let composite: u64 = (self.uptime_score as u64) * (config.uptime_weight as u64)
            + (self.latency_score as u64) * (config.latency_weight as u64)
            + (self.participation_score as u64) * (config.participation_weight as u64);
        self.composite_score = (composite / 1000).min(1000) as u32;
    }
}

pub struct ScoreValidator {
    config: ScoreValidationConfig,
    stats: Arc<RwLock<ScoreValidationStats>>,
    node_scores: Arc<RwLock<HashMap<[u8; 32], NodeScore>>>,
    current_epoch: Arc<RwLock<u64>>,
}

impl ScoreValidator {
    pub fn new(config: ScoreValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(ScoreValidationStats::default())),
            node_scores: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: Arc::new(RwLock::new(0)),
        }
    }

    /// Validate if a node qualifies as proposer
    pub async fn validate_proposer(&self, node_id: &[u8; 32]) -> Result<ValidationResult> {
        let scores = self.node_scores.read().await;

        let score = match scores.get(node_id) {
            Some(s) => s,
            None => {
                self.record_rejection().await;
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom(
                        "No score record for node".to_string(),
                    ),
                ));
            }
        };

        // Check score age
        if self.config.enable_decay_validation {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if current_time - score.last_updated > self.config.max_score_age_secs {
                self.record_decay_rejection().await;
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom("Score too old".to_string()),
                ));
            }
        }

        // Check minimum scores (all in permille)
        if score.composite_score < self.config.min_proposer_score {
            self.record_rejection().await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Composite score {} < minimum {}",
                    score.composite_score, self.config.min_proposer_score
                )),
            ));
        }

        if score.uptime_score < self.config.min_uptime {
            self.record_rejection().await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Uptime {} < minimum {}",
                    score.uptime_score, self.config.min_uptime
                )),
            ));
        }

        self.record_proposer_qualification().await;
        Ok(ValidationResult::Valid)
    }

    pub async fn validate_validator(&self, node_id: &[u8; 32]) -> Result<ValidationResult> {
        let scores = self.node_scores.read().await;

        let score = match scores.get(node_id) {
            Some(s) => s,
            None => {
                self.record_rejection().await;
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom(
                        "No score record for node".to_string(),
                    ),
                ));
            }
        };

        // Check score age
        if self.config.enable_decay_validation {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if current_time - score.last_updated > self.config.max_score_age_secs {
                self.record_decay_rejection().await;
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom("Score too old".to_string()),
                ));
            }
        }

        // Check minimum scores (all in permille)
        if score.composite_score < self.config.min_validator_score {
            self.record_rejection().await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Composite score {} < minimum {}",
                    score.composite_score, self.config.min_validator_score
                )),
            ));
        }

        self.record_validator_qualification().await;
        Ok(ValidationResult::Valid)
    }

    /// Update node score
    pub async fn update_score(&self, score: NodeScore) {
        let mut scores = self.node_scores.write().await;
        scores.insert(score.node_id, score);
    }

    /// Update node uptime (permille, 0-1000)
    pub async fn update_uptime(&self, node_id: &[u8; 32], uptime: u32) {
        let mut scores = self.node_scores.write().await;
        let score = scores
            .entry(*node_id)
            .or_insert_with(|| NodeScore::new(*node_id));
        score.uptime_score = uptime.min(1000);
        score.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        score.calculate_composite(&self.config);
    }

    /// Update node latency score (permille, 0-1000)
    pub async fn update_latency(&self, node_id: &[u8; 32], latency_score: u32) {
        let mut scores = self.node_scores.write().await;
        let score = scores
            .entry(*node_id)
            .or_insert_with(|| NodeScore::new(*node_id));
        score.latency_score = latency_score.min(1000);
        score.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        score.calculate_composite(&self.config);
    }

    /// Update node participation score (permille, 0-1000)
    pub async fn update_participation(&self, node_id: &[u8; 32], participation: u32) {
        let mut scores = self.node_scores.write().await;
        let score = scores
            .entry(*node_id)
            .or_insert_with(|| NodeScore::new(*node_id));
        score.participation_score = participation.min(1000);
        score.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        score.calculate_composite(&self.config);
    }

    /// Get node score
    pub async fn get_score(&self, node_id: &[u8; 32]) -> Option<NodeScore> {
        let scores = self.node_scores.read().await;
        scores.get(node_id).cloned()
    }

    /// Get all qualified proposers
    pub async fn get_qualified_proposers(&self) -> Vec<[u8; 32]> {
        let scores = self.node_scores.read().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        scores
            .iter()
            .filter(|(_, score)| {
                score.composite_score >= self.config.min_proposer_score
                    && score.uptime_score >= self.config.min_uptime
                    && (!self.config.enable_decay_validation
                        || current_time - score.last_updated <= self.config.max_score_age_secs)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    pub async fn get_qualified_validators(&self) -> Vec<[u8; 32]> {
        let scores = self.node_scores.read().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        scores
            .iter()
            .filter(|(_, score)| {
                score.composite_score >= self.config.min_validator_score
                    && (!self.config.enable_decay_validation
                        || current_time - score.last_updated <= self.config.max_score_age_secs)
            })
            .map(|(id, _)| *id)
            .collect()
    }

    /// Apply decay to all scores (integer arithmetic)
    ///
    /// AUDIT-003 FIX: decay_factor is in permille (0-1000).
    /// Each score is multiplied by decay_factor / 1000.
    pub async fn apply_decay(&self, decay_factor: u32) {
        let mut scores = self.node_scores.write().await;
        let factor = (decay_factor as u64).min(1000);
        for score in scores.values_mut() {
            score.composite_score = ((score.composite_score as u64 * factor) / 1000) as u32;
            score.uptime_score = ((score.uptime_score as u64 * factor) / 1000) as u32;
            score.latency_score = ((score.latency_score as u64 * factor) / 1000) as u32;
            score.participation_score = ((score.participation_score as u64 * factor) / 1000) as u32;
        }
    }

    /// Set current epoch
    pub async fn set_epoch(&self, epoch: u64) {
        let mut current = self.current_epoch.write().await;
        *current = epoch;
    }

    pub async fn stats(&self) -> ScoreValidationStats {
        self.stats.read().await.clone()
    }

    // Internal stat recording methods
    async fn record_rejection(&self) {
        let mut stats = self.stats.write().await;
        stats.scores_validated += 1;
        stats.scores_rejected += 1;
    }

    async fn record_decay_rejection(&self) {
        let mut stats = self.stats.write().await;
        stats.scores_validated += 1;
        stats.scores_rejected += 1;
        stats.decay_rejections += 1;
    }

    async fn record_proposer_qualification(&self) {
        let mut stats = self.stats.write().await;
        stats.scores_validated += 1;
        stats.scores_accepted += 1;
        stats.proposer_qualifications += 1;
    }

    async fn record_validator_qualification(&self) {
        let mut stats = self.stats.write().await;
        stats.scores_validated += 1;
        stats.scores_accepted += 1;
        stats.validator_qualifications += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_score_validator_creation() {
        let config = ScoreValidationConfig::default();
        let validator = ScoreValidator::new(config);
        let stats = validator.stats().await;
        assert_eq!(stats.scores_validated, 0);
    }

    #[tokio::test]
    async fn test_update_and_get_score() {
        let validator = ScoreValidator::new(ScoreValidationConfig::default());
        let node_id = [1u8; 32];

        // Permille values: 990 = 99%, 800 = 80%, 900 = 90%
        validator.update_uptime(&node_id, 990).await;
        validator.update_latency(&node_id, 800).await;
        validator.update_participation(&node_id, 900).await;

        let score = validator.get_score(&node_id).await.unwrap();
        assert!(score.composite_score > 0);
    }

    #[tokio::test]
    async fn test_proposer_qualification() {
        let validator = ScoreValidator::new(ScoreValidationConfig::default());
        let node_id = [1u8; 32];

        // Set high scores (permille)
        validator.update_uptime(&node_id, 990).await;
        validator.update_latency(&node_id, 900).await;
        validator.update_participation(&node_id, 900).await;

        let result = validator.validate_proposer(&node_id).await.unwrap();
        assert!(matches!(result, ValidationResult::Valid));
    }

    #[tokio::test]
    async fn test_low_score_rejection() {
        let validator = ScoreValidator::new(ScoreValidationConfig::default());
        let node_id = [1u8; 32];

        // Set low scores (permille)
        validator.update_uptime(&node_id, 500).await;
        validator.update_latency(&node_id, 300).await;
        validator.update_participation(&node_id, 200).await;

        let result = validator.validate_proposer(&node_id).await.unwrap();
        assert!(matches!(result, ValidationResult::Invalid(_)));
    }

    #[tokio::test]
    async fn test_composite_score_calculation() {
        let config = ScoreValidationConfig::default();
        let mut node = NodeScore::new([0u8; 32]);
        node.uptime_score = 1000; // 100%
        node.latency_score = 1000; // 100%
        node.participation_score = 1000; // 100%
        node.calculate_composite(&config);
        // (1000*400 + 1000*300 + 1000*300) / 1000 = 1000
        assert_eq!(node.composite_score, 1000);
    }

    #[tokio::test]
    async fn test_composite_score_weighted() {
        let config = ScoreValidationConfig::default();
        let mut node = NodeScore::new([0u8; 32]);
        node.uptime_score = 500;
        node.latency_score = 500;
        node.participation_score = 500;
        node.calculate_composite(&config);
        // (500*400 + 500*300 + 500*300) / 1000 = 500
        assert_eq!(node.composite_score, 500);
    }

    #[tokio::test]
    async fn test_apply_decay() {
        let validator = ScoreValidator::new(ScoreValidationConfig::default());
        let node_id = [1u8; 32];

        validator.update_uptime(&node_id, 1000).await;
        validator.update_latency(&node_id, 1000).await;
        validator.update_participation(&node_id, 1000).await;

        // Apply 90% decay factor (900 permille)
        validator.apply_decay(900).await;

        let score = validator.get_score(&node_id).await.unwrap();
        assert_eq!(score.uptime_score, 900);
        assert_eq!(score.latency_score, 900);
        assert_eq!(score.participation_score, 900);
    }
}
