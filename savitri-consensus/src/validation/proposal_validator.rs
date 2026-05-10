//!

use crate::crypto::verify_signature;
use crate::error::Result;
use crate::types::validation::ValidationError;
use crate::types::*;
use async_trait::async_trait;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ProposalValidationConfig {
    /// Maximum proposal size in bytes
    pub max_proposal_size: usize,
    /// Maximum transactions per proposal
    pub max_transactions: u32,
    /// Proposal timeout in milliseconds
    pub proposal_timeout_ms: u64,
    pub enable_signature_validation: bool,
    pub enable_timing_validation: bool,
    /// Maximum clock drift in seconds
    pub max_clock_drift_secs: u64,
    /// Minimum block interval in seconds
    pub min_block_interval_secs: u64,
}

impl Default for ProposalValidationConfig {
    fn default() -> Self {
        Self {
            max_proposal_size: 10 * 1024 * 1024, // 10 MB
            max_transactions: 10000,
            proposal_timeout_ms: 5000,
            enable_signature_validation: true,
            enable_timing_validation: true,
            max_clock_drift_secs: 30,
            min_block_interval_secs: 1,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProposalValidationStats {
    pub proposals_validated: u64,
    pub proposals_accepted: u64,
    pub proposals_rejected: u64,
    pub signature_failures: u64,
    pub timing_failures: u64,
    pub structure_failures: u64,
    pub average_validation_time_ms: f64,
}

pub struct ProposalValidator {
    config: ProposalValidationConfig,
    stats: Arc<RwLock<ProposalValidationStats>>,
    known_proposers: Arc<RwLock<Vec<[u8; 32]>>>,
    last_proposal_time: Arc<RwLock<u64>>,
}

impl ProposalValidator {
    pub fn new(config: ProposalValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(ProposalValidationStats::default())),
            known_proposers: Arc::new(RwLock::new(Vec::new())),
            last_proposal_time: Arc::new(RwLock::new(0)),
        }
    }

    /// Validate a proposal
    pub async fn validate(&self, proposal: &BlockProposal) -> Result<ValidationResult> {
        let start = std::time::Instant::now();

        if let Err(e) = self.validate_structure(proposal).await {
            self.record_rejection(ProposalRejectionReason::StructureInvalid)
                .await;
            return Ok(ValidationResult::Invalid(ValidationError::Custom(format!(
                "Structure invalid: {}",
                e
            ))));
        }

        if self.config.enable_timing_validation {
            if let Err(e) = self.validate_timing(proposal).await {
                self.record_rejection(ProposalRejectionReason::TimingInvalid)
                    .await;
                return Ok(ValidationResult::Invalid(ValidationError::Custom(format!(
                    "Timing invalid: {}",
                    e
                ))));
            }
        }

        if self.config.enable_signature_validation {
            if let Err(e) = self.validate_signature(proposal).await {
                self.record_rejection(ProposalRejectionReason::SignatureInvalid)
                    .await;
                return Ok(ValidationResult::Invalid(ValidationError::Custom(format!(
                    "Signature invalid: {}",
                    e
                ))));
            }
        }

        if let Err(e) = self.validate_proposer(proposal).await {
            self.record_rejection(ProposalRejectionReason::ProposerInvalid)
                .await;
            return Ok(ValidationResult::Invalid(ValidationError::Custom(format!(
                "Proposer invalid: {}",
                e
            ))));
        }

        // Update stats
        let elapsed = start.elapsed().as_millis() as f64;
        self.record_acceptance(elapsed).await;

        Ok(ValidationResult::Valid)
    }

    /// Validate proposal structure
    async fn validate_structure(&self, proposal: &BlockProposal) -> Result<()> {
        // Check proposal size
        let estimated_size = self.estimate_proposal_size(proposal);
        if estimated_size > self.config.max_proposal_size {
            return Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Proposal too large: {} > {}",
                estimated_size, self.config.max_proposal_size
            ))
            .into());
        }

        // Check transaction count
        if proposal.transactions.len() > self.config.max_transactions as usize {
            return Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Too many transactions: {} > {}",
                proposal.transactions.len(),
                self.config.max_transactions
            ))
            .into());
        }

        // Validate block height
        if proposal.height == 0 {
            return Err(crate::error::ConsensusError::ValidationFailed(
                "Invalid block height: 0".to_string(),
            )
            .into());
        }

        Ok(())
    }

    /// Validate proposal timing
    async fn validate_timing(&self, proposal: &BlockProposal) -> Result<()> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let proposal_time = proposal.timestamp;

        // Check clock drift
        if proposal_time > current_time + self.config.max_clock_drift_secs {
            return Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Proposal timestamp too far in future: {} > {} + {}",
                proposal_time, current_time, self.config.max_clock_drift_secs
            ))
            .into());
        }

        // Check minimum interval
        let last_time = *self.last_proposal_time.read().await;
        if last_time > 0 && proposal_time < last_time + self.config.min_block_interval_secs {
            return Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Proposal too soon after last: {} < {} + {}",
                proposal_time, last_time, self.config.min_block_interval_secs
            ))
            .into());
        }

        Ok(())
    }

    /// Validate proposal signature
    async fn validate_signature(&self, proposal: &BlockProposal) -> Result<()> {
        let block_hash = &proposal.parent_hash;
        let signature = &proposal.signature;
        let proposer_key = &proposal.proposer_pubkey;

        match verify_signature(&block_hash.0, &signature.0, &proposer_key.0) {
            Ok(true) => Ok(()),
            Ok(false) => Err(crate::error::ConsensusError::ValidationFailed(
                "Signature verification failed".to_string(),
            )
            .into()),
            Err(e) => Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Signature error: {:?}",
                e
            ))
            .into()),
        }
    }

    /// Validate proposer is authorized
    async fn validate_proposer(&self, proposal: &BlockProposal) -> Result<()> {
        let known = self.known_proposers.read().await;

        // If no known proposers, accept any (permissionless mode)
        if known.is_empty() {
            return Ok(());
        }

        // Check if proposer is in known list
        if !known.contains(&proposal.proposer_pubkey.0) {
            return Err(crate::error::ConsensusError::ValidationFailed(
                "Unknown proposer".to_string(),
            )
            .into());
        }

        Ok(())
    }

    /// Estimate proposal size in bytes
    fn estimate_proposal_size(&self, proposal: &BlockProposal) -> usize {
        let mut size = 0;

        // Proposal metadata size (approximate)
        size += std::mem::size_of::<BlockProposal>();

        // Transactions
        for tx in &proposal.transactions {
            size += std::mem::size_of::<ProposalTransaction>();
            size += tx.data.len();
        }

        // Signature
        size += proposal.signature.0.len();

        // Other fields
        size += proposal.parent_hash.0.len();
        size += proposal.state_root.0.len();
        size += proposal.tx_root.0.len();
        size += proposal.proposer_pubkey.0.len();

        size
    }

    /// Record proposal rejection
    async fn record_rejection(&self, reason: ProposalRejectionReason) {
        let mut stats = self.stats.write().await;
        stats.proposals_validated += 1;
        stats.proposals_rejected += 1;

        match reason {
            ProposalRejectionReason::SignatureInvalid => stats.signature_failures += 1,
            ProposalRejectionReason::TimingInvalid => stats.timing_failures += 1,
            ProposalRejectionReason::StructureInvalid => stats.structure_failures += 1,
            _ => {}
        }
    }

    /// Record proposal acceptance
    async fn record_acceptance(&self, validation_time_ms: f64) {
        let mut stats = self.stats.write().await;
        stats.proposals_validated += 1;
        stats.proposals_accepted += 1;

        if stats.proposals_accepted == 1 {
            stats.average_validation_time_ms = validation_time_ms;
        } else {
            let n = stats.proposals_accepted as f64;
            stats.average_validation_time_ms =
                (stats.average_validation_time_ms * (n - 1.0) + validation_time_ms) / n;
        }
    }

    /// Add known proposer
    pub async fn add_proposer(&self, proposer: [u8; 32]) {
        let mut known = self.known_proposers.write().await;
        if !known.contains(&proposer) {
            known.push(proposer);
        }
    }

    /// Remove known proposer
    pub async fn remove_proposer(&self, proposer: &[u8; 32]) {
        let mut known = self.known_proposers.write().await;
        known.retain(|p| p != proposer);
    }

    /// Update last proposal time
    pub async fn update_last_proposal_time(&self, timestamp: u64) {
        let mut last = self.last_proposal_time.write().await;
        *last = timestamp;
    }

    pub async fn stats(&self) -> ProposalValidationStats {
        self.stats.read().await.clone()
    }
}

/// Proposal rejection reasons
#[derive(Debug, Clone)]
pub enum ProposalRejectionReason {
    StructureInvalid,
    TimingInvalid,
    SignatureInvalid,
    ProposerInvalid,
    DuplicateProposal,
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_proposal_validator_creation() {
        let config = ProposalValidationConfig::default();
        let validator = ProposalValidator::new(config);
        let stats = validator.stats().await;
        assert_eq!(stats.proposals_validated, 0);
    }

    #[tokio::test]
    async fn test_add_remove_proposer() {
        let validator = ProposalValidator::new(ProposalValidationConfig::default());
        let proposer = [1u8; 32];

        validator.add_proposer(proposer).await;
        validator.remove_proposer(&proposer).await;
    }
}
