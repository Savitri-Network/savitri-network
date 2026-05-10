//! Group Transaction Validation
//!
//! Validates transactions within group context with group-aware

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use savitri_core::Transaction;

#[derive(Debug, Clone)]
pub struct GroupValidationConfig {
    pub enable_group_validation: bool,
    pub enable_proposer_validation: bool,
    /// Minimum group membership duration in seconds
    pub min_group_membership_duration: u64,
    /// Enable transaction rate limiting per group
    pub enable_rate_limiting: bool,
    /// Max transactions per group per second
    pub max_tx_per_group_per_sec: f64,
    pub enable_geographic_validation: bool,
    pub enable_stake_validation: bool,
    /// Minimum stake for transaction submission
    pub min_stake_threshold: u64,
}

impl Default for GroupValidationConfig {
    fn default() -> Self {
        Self {
            enable_group_validation: true,
            enable_proposer_validation: true,
            min_group_membership_duration: 60,
            enable_rate_limiting: true,
            max_tx_per_group_per_sec: 10.0,
            enable_geographic_validation: false,
            enable_stake_validation: true,
            min_stake_threshold: 1000,
        }
    }
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub transaction_hash: Vec<u8>,
    pub group_id: Option<String>,
    pub proposer_id: Option<String>,
    pub validation_errors: Vec<ValidationError>,
    pub validation_timestamp: u64,
    pub confidence_score: f64,
}

/// Validation errors
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ValidationError {
    /// Invalid transaction format
    InvalidFormat,
    /// Invalid signature
    InvalidSignature,
    /// Insufficient balance
    InsufficientBalance,
    /// Nonce too low
    NonceTooLow,
    /// Nonce too high
    NonceTooHigh,
    /// Gas limit exceeded
    GasLimitExceeded,
    /// Group not found
    GroupNotFound,
    /// Proposer not in group
    ProposerNotInGroup,
    /// Group membership too recent
    GroupMembershipTooRecent,
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Geographic constraint violation
    GeographicConstraintViolation,
    /// Insufficient stake
    InsufficientStake,
    /// Transaction too old
    TransactionTooOld,
    /// Duplicate transaction
    DuplicateTransaction,
}

/// Group member information
#[derive(Debug, Clone)]
pub struct GroupMemberInfo {
    pub member_id: String,
    pub group_id: String,
    pub joined_at: u64,
    pub stake: u64,
    pub geographic_region: String,
    pub reputation_score: f64,
}

/// Validation statistics
#[derive(Debug, Clone, Default)]
pub struct ValidationStats {
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub validations_by_error: HashMap<ValidationError, u64>,
    pub average_validation_time_ms: f64,
    pub groups_validated: usize,
    pub proposers_validated: usize,
}

/// Group Transaction Validator
pub struct GroupTransactionValidator {
    config: GroupValidationConfig,
    group_members: Arc<RwLock<HashMap<String, GroupMemberInfo>>>,
    rate_limiter: Arc<RwLock<HashMap<String, RateLimiter>>>,
    validated_transactions: Arc<RwLock<HashMap<Vec<u8>, u64>>>, // tx_hash -> timestamp
    stats: Arc<RwLock<ValidationStats>>,
}

/// Rate limiter for groups
#[derive(Debug, Clone)]
struct RateLimiter {
    transactions: Vec<u64>,
    window_start: u64,
}

impl GroupTransactionValidator {
    pub fn new(config: GroupValidationConfig) -> Self {
        Self {
            config,
            group_members: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter: Arc::new(RwLock::new(HashMap::new())),
            validated_transactions: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(ValidationStats::default())),
        }
    }

    /// Register group member
    pub async fn register_group_member(&self, member_info: GroupMemberInfo) -> Result<()> {
        let mut members = self.group_members.write().await;
        members.insert(member_info.member_id.clone(), member_info.clone());

        // Initialize rate limiter for group
        let mut rate_limiters = self.rate_limiter.write().await;
        rate_limiters.insert(
            member_info.group_id.clone(),
            RateLimiter {
                transactions: Vec::new(),
                window_start: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );

        info!(
            member_id = %member_info.member_id,
            group_id = %member_info.group_id,
            "Registered group member"
        );

        Ok(())
    }

    /// Validate transaction with group context
    pub async fn validate_transaction(
        &self,
        transaction: &Transaction,
        group_id: Option<String>,
        proposer_id: Option<String>,
    ) -> Result<ValidationResult> {
        let start_time = std::time::SystemTime::now();
        let mut validation_errors = Vec::new();
        let mut confidence_score = 1.0;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_validations += 1;
        drop(stats);

        self.validate_basic_transaction(transaction, &mut validation_errors, &mut confidence_score)
            .await?;

        if let (Some(group), Some(proposer)) = (group_id.clone(), proposer_id.clone()) {
            if self.config.enable_group_validation {
                self.validate_group_context(
                    &group,
                    &proposer,
                    &mut validation_errors,
                    &mut confidence_score,
                )
                .await?;
            }

            if self.config.enable_proposer_validation {
                self.validate_proposer_context(
                    &proposer,
                    &group,
                    &mut validation_errors,
                    &mut confidence_score,
                )
                .await?;
            }

            if self.config.enable_rate_limiting {
                self.validate_rate_limit(&group, &mut validation_errors, &mut confidence_score)
                    .await?;
            }
        }

        if let Some(proposer) = proposer_id.clone() {
            if self.config.enable_geographic_validation {
                self.validate_geographic_constraints(
                    &Some(proposer),
                    &mut validation_errors,
                    &mut confidence_score,
                )
                .await?;
            }
        }

        if let Some(proposer) = proposer_id.clone() {
            if self.config.enable_stake_validation {
                self.validate_stake_constraints(
                    &Some(proposer),
                    &mut validation_errors,
                    &mut confidence_score,
                )
                .await?;
            }
        }

        // Check for duplicate
        self.validate_duplicate_transaction(
            transaction,
            &mut validation_errors,
            &mut confidence_score,
        )
        .await?;

        let is_valid = validation_errors.is_empty();
        let validation_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if is_valid {
            let mut validated = self.validated_transactions.write().await;
            validated.insert(transaction.hash().to_vec(), validation_timestamp);
        }

        // Update stats
        let duration = start_time.elapsed().unwrap().as_millis() as f64;
        let mut stats = self.stats.write().await;

        if is_valid {
            stats.successful_validations += 1;
        } else {
            stats.failed_validations += 1;
            for error in &validation_errors {}
        }

        stats.average_validation_time_ms =
            (stats.average_validation_time_ms * (stats.total_validations as f64 - 1.0) + duration)
                / stats.total_validations as f64;

        if let Some(ref group) = group_id {
            stats.groups_validated = stats.groups_validated.max(1);
        }
        if let Some(ref proposer) = proposer_id {
            stats.proposers_validated = stats.proposers_validated.max(1);
        }

        let result = ValidationResult {
            is_valid,
            transaction_hash: transaction.hash().to_vec(),
            group_id,
            proposer_id,
            validation_errors,
            validation_timestamp,
            confidence_score,
        };

        debug!(
            tx_hash = %hex::encode(&transaction.hash()),
            is_valid = result.is_valid,
            confidence_score = result.confidence_score,
            errors_count = result.validation_errors.len(),
            "Transaction validation completed"
        );

        Ok(result)
    }

    async fn validate_basic_transaction(
        &self,
        transaction: &Transaction,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        // Check transaction format
        if transaction.from.is_empty() || transaction.to.is_empty() {
            errors.push(ValidationError::InvalidFormat);
            *confidence_score *= 0.1;
        }

        // Check signature (simplified)
        if transaction.signature.iter().all(|&b| b == 0) {
            errors.push(ValidationError::InvalidSignature);
            *confidence_score *= 0.1;
        }

        // Check value and fee
        if transaction.amount == 0 && transaction.fee == 0 {
            errors.push(ValidationError::InvalidFormat);
            *confidence_score *= 0.5;
        }

        Ok(())
    }

    /// Validate group context
    async fn validate_group_context(
        &self,
        group_id: &str,
        proposer_id: &str,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        let members = self.group_members.read().await;

        // Check if proposer is in group
        let proposer_in_group = members
            .values()
            .any(|member| member.member_id == proposer_id && member.group_id == group_id);

        if !proposer_in_group {
            errors.push(ValidationError::ProposerNotInGroup);
            *confidence_score *= 0.1;
        }

        // Check group membership duration
        if let Some(member) = members.get(proposer_id) {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            if current_time - member.joined_at < self.config.min_group_membership_duration {
                errors.push(ValidationError::GroupMembershipTooRecent);
                *confidence_score *= 0.5;
            }
        }

        Ok(())
    }

    /// Validate proposer context
    async fn validate_proposer_context(
        &self,
        proposer_id: &str,
        group_id: &str,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        let members = self.group_members.read().await;

        if let Some(member) = members.get(proposer_id) {
            // Check if member is in correct group
            if member.group_id != group_id {
                errors.push(ValidationError::ProposerNotInGroup);
                *confidence_score *= 0.1;
            }

            // Adjust confidence based on reputation
            *confidence_score *= member.reputation_score;
        }

        Ok(())
    }

    /// Validate rate limit
    async fn validate_rate_limit(
        &self,
        group_id: &str,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut rate_limiters = self.rate_limiter.write().await;
        let rate_limiter = rate_limiters
            .entry(group_id.to_string())
            .or_insert_with(|| RateLimiter {
                transactions: Vec::new(),
                window_start: current_time,
            });

        // Reset window if needed (1-second window)
        if current_time - rate_limiter.window_start >= 1 {
            rate_limiter.transactions.clear();
            rate_limiter.window_start = current_time;
        }

        // Check rate limit
        if rate_limiter.transactions.len() as f64 >= self.config.max_tx_per_group_per_sec {
            errors.push(ValidationError::RateLimitExceeded);
            *confidence_score *= 0.1;
        } else {
            rate_limiter.transactions.push(current_time);
        }

        Ok(())
    }

    /// Validate geographic constraints
    async fn validate_geographic_constraints(
        &self,
        proposer_id: &Option<String>,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        if let Some(proposer) = proposer_id {
            let members = self.group_members.read().await;

            if let Some(member) = members.get(proposer) {
                if member.geographic_region == "restricted" {
                    errors.push(ValidationError::GeographicConstraintViolation);
                    *confidence_score *= 0.1;
                }
            }
        }

        Ok(())
    }

    /// Validate stake constraints
    async fn validate_stake_constraints(
        &self,
        proposer_id: &Option<String>,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        if let Some(proposer) = proposer_id {
            let members = self.group_members.read().await;

            if let Some(member) = members.get(proposer) {
                if member.stake < self.config.min_stake_threshold {
                    errors.push(ValidationError::InsufficientStake);
                    *confidence_score *= 0.5;
                } else {
                    // Boost confidence for high stake
                    let stake_ratio = member.stake as f64 / self.config.min_stake_threshold as f64;
                    *confidence_score *= (1.0 + stake_ratio * 0.1).min(2.0);
                }
            }
        }

        Ok(())
    }

    /// Validate duplicate transaction
    async fn validate_duplicate_transaction(
        &self,
        transaction: &Transaction,
        errors: &mut Vec<ValidationError>,
        confidence_score: &mut f64,
    ) -> Result<()> {
        let validated = self.validated_transactions.read().await;

        if validated.contains_key(&transaction.hash().to_vec()) {
            errors.push(ValidationError::DuplicateTransaction);
            *confidence_score *= 0.1;
        }

        Ok(())
    }

    pub async fn get_stats(&self) -> ValidationStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    pub async fn cleanup_validated_transactions(&self, timeout_secs: u64) -> Result<usize> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut validated = self.validated_transactions.write().await;
        let original_count = validated.len();

        validated.retain(|_, timestamp| (current_time - *timestamp) < timeout_secs);

        let removed = original_count - validated.len();
        if removed > 0 {
            debug!(
                removed_count = removed,
                "Cleaned up old validated transactions"
            );
        }

        Ok(removed)
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting group transaction validator");

        // Start cleanup task
        let validator = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;
                if let Err(e) = validator.cleanup_validated_transactions(300).await {
                    error!("Failed to cleanup validated transactions: {}", e);
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        info!("Stopping group transaction validator");
        Ok(())
    }
}

impl Clone for GroupTransactionValidator {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            group_members: self.group_members.clone(),
            rate_limiter: self.rate_limiter.clone(),
            validated_transactions: self.validated_transactions.clone(),
            stats: self.stats.clone(),
        }
    }
}
