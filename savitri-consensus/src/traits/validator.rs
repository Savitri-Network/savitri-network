//! Validator trait and related types
//!

use crate::error::{ConsensusError, ValidationErrorResult};
use crate::types::validation::{ValidationError, ValidationResult};
use crate::types::{
    Block, NodeType, Proposal, ProposalMetadata, Transaction, ValidatorInfo, ValidatorMetadata,
    ValidatorStatus,
};
use crate::types::{GeographicConstraints, GroupInfo, ScoreThresholds};
use crate::ProposerInfo;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

pub trait ValidationContext: Send + Sync {
    /// Get current epoch
    fn current_epoch(&self) -> u64;

    /// Get current slot
    fn current_slot(&self) -> u64;

    /// Get current block height
    fn current_height(&self) -> u64;

    fn validator_id(&self) -> &str;

    fn validation_timestamp(&self) -> u64;

    /// Check if strict mode is enabled
    fn is_strict_mode(&self) -> bool;

    fn max_validation_time_ms(&self) -> u64;

    fn get_active_groups(&self) -> &std::collections::HashMap<String, GroupInfo>;

    /// Get known proposers
    fn get_known_proposers(&self) -> &std::collections::HashMap<String, ProposerInfo>;

    /// Check if a node is blacklisted
    fn is_blacklisted(&self, node_id: &str) -> bool;

    /// Get minimum required scores
    fn get_min_scores(&self) -> &ScoreThresholds;

    /// Get geographic constraints
    fn get_geographic_constraints(&self) -> &GeographicConstraints;

    /// Get current timestamp
    fn current_timestamp(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Clone the context
    fn clone_box(&self) -> Box<dyn ValidationContext> {
        // Default implementation - should be overridden by concrete types
        panic!("clone_box must be implemented by concrete ValidationContext types")
    }
}

pub trait Validator: Send + Sync {
    type Context: ValidationContext + Send + Sync;

    /// Validate a block
    fn validate_block(&self, block: &Block, context: &Self::Context) -> ValidationResult;

    /// Validate a transaction
    fn validate_transaction(&self, tx: &Transaction, context: &Self::Context) -> ValidationResult;

    /// Validate a proposer
    fn validate_proposer(
        &self,
        proposer: &ProposerInfo,
        context: &Self::Context,
    ) -> ValidationResult;

    /// Validate a proposal
    fn validate_proposal(
        &self,
        proposal: &dyn Proposal,
        context: &Self::Context,
    ) -> ValidationResult;

    fn validator_info(&self) -> ValidatorInfo;

    fn is_active(&self) -> bool;

    fn stake(&self) -> u64;

    fn score(&self) -> u32;

    fn update_score(&self, new_score: u32) -> crate::error::Result<()>;

    fn validation_stats(&self) -> ValidationStats;
}

/// Validation statistics
#[derive(Debug, Clone)]
pub struct ValidationStats {
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub average_validation_time_ms: f64,
    pub last_validation_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct DefaultValidationContext {
    current_epoch: u64,
    current_slot: u64,
    current_height: u64,
    validator_id: String,
    validation_timestamp: u64,
    strict_mode: bool,
    max_validation_time_ms: u64,
    active_groups: std::collections::HashMap<String, GroupInfo>,
    known_proposers: std::collections::HashMap<String, ProposerInfo>,
    blacklisted_nodes: std::collections::HashSet<String>,
    min_scores: ScoreThresholds,
    geographic_constraints: GeographicConstraints,
}

impl DefaultValidationContext {
    pub fn new(
        current_epoch: u64,
        current_slot: u64,
        current_height: u64,
        validator_id: String,
    ) -> Self {
        Self {
            current_epoch,
            current_slot,
            current_height,
            validator_id,
            validation_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            strict_mode: false,
            max_validation_time_ms: 5000,
            active_groups: std::collections::HashMap::new(),
            known_proposers: std::collections::HashMap::new(),
            blacklisted_nodes: std::collections::HashSet::new(),
            min_scores: ScoreThresholds::default(),
            geographic_constraints: GeographicConstraints::default(),
        }
    }

    pub fn with_strict_mode(mut self, strict_mode: bool) -> Self {
        self.strict_mode = strict_mode;
        self
    }

    pub fn with_validation_timeout(mut self, timeout_ms: u64) -> Self {
        self.max_validation_time_ms = timeout_ms;
        self
    }

    pub fn add_group(mut self, group: GroupInfo) -> Self {
        self.active_groups.insert(group.group_id.clone(), group);
        self
    }

    pub fn add_proposer(mut self, proposer: ProposerInfo) -> Self {
        self.known_proposers
            .insert(proposer.node_id.clone(), proposer);
        self
    }

    pub fn add_blacklisted_node(mut self, node_id: String) -> Self {
        self.blacklisted_nodes.insert(node_id);
        self
    }

    pub fn with_min_scores(mut self, scores: ScoreThresholds) -> Self {
        self.min_scores = scores;
        self
    }

    pub fn with_geographic_constraints(mut self, constraints: GeographicConstraints) -> Self {
        self.geographic_constraints = constraints;
        self
    }
}

impl ValidationContext for DefaultValidationContext {
    fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    fn current_slot(&self) -> u64 {
        self.current_slot
    }

    fn current_height(&self) -> u64 {
        self.current_height
    }

    fn validator_id(&self) -> &str {
        &self.validator_id
    }

    fn validation_timestamp(&self) -> u64 {
        self.validation_timestamp
    }

    fn is_strict_mode(&self) -> bool {
        self.strict_mode
    }

    fn max_validation_time_ms(&self) -> u64 {
        self.max_validation_time_ms
    }

    fn get_active_groups(&self) -> &std::collections::HashMap<String, GroupInfo> {
        &self.active_groups
    }

    fn get_known_proposers(&self) -> &std::collections::HashMap<String, ProposerInfo> {
        &self.known_proposers
    }

    fn is_blacklisted(&self, node_id: &str) -> bool {
        self.blacklisted_nodes.contains(node_id)
    }

    fn get_min_scores(&self) -> &ScoreThresholds {
        &self.min_scores
    }

    fn get_geographic_constraints(&self) -> &GeographicConstraints {
        &self.geographic_constraints
    }
}

pub struct DefaultValidator {
    info: ValidatorInfo,
    stats: Arc<tokio::sync::RwLock<ValidationStats>>,
    signature_validator: Arc<dyn SignatureValidator>,
    state_validator: Arc<dyn StateValidator>,
}

impl DefaultValidator {
    pub fn new(
        info: ValidatorInfo,
        signature_validator: Arc<dyn SignatureValidator>,
        state_validator: Arc<dyn StateValidator>,
    ) -> Self {
        Self {
            info,
            stats: Arc::new(tokio::sync::RwLock::new(ValidationStats::default())),
            signature_validator,
            state_validator,
        }
    }
}

impl Validator for DefaultValidator {
    type Context = DefaultValidationContext;

    fn validate_block(&self, block: &Block, context: &Self::Context) -> ValidationResult {
        if !block.is_valid() {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Validate block height is within reasonable bounds
        if block.header.height == 0 && context.current_height() > 0 {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Validate block timestamp is reasonable (not too far in the future)
        let current_time = context.current_timestamp();
        let block_time = block.header.timestamp;
        let max_time_diff = context.max_validation_time_ms() / 1000; // Convert to seconds

        if block_time > current_time + max_time_diff {
            return ValidationResult::Invalid(ValidationError::ValidationTimeout);
        }

        // Validate block parent hash (for non-genesis blocks)
        if block.header.height > 0 {
            // Check if parent hash is all zeros (invalid for non-genesis)
            if block.header.parent_hash.iter().all(|&b| b == 0) {
                return ValidationResult::Invalid(ValidationError::InvalidBlock);
            }

            if context.is_strict_mode() && block.header.parent_hash.len() != 64 {
                return ValidationResult::Invalid(ValidationError::InvalidBlock);
            }
        }

        // Validate state root and transaction root format
        if context.is_strict_mode() {
            if block.header.state_root.len() != 64 {
                return ValidationResult::Invalid(ValidationError::InvalidBlock);
            }

            if block.header.tx_root.len() != 64 {
                return ValidationResult::Invalid(ValidationError::InvalidBlock);
            }
        }

        // Validate proposer information
        if block.consensus_data.proposer_info.node_id.is_empty() {
            return ValidationResult::Invalid(ValidationError::InvalidProposer);
        }

        // Validate proposer is not blacklisted
        if context.is_blacklisted(&block.consensus_data.proposer_info.node_id) {
            return ValidationResult::Invalid(ValidationError::InvalidProposer);
        }

        // Validate proposer meets minimum score requirements
        let min_scores = context.get_min_scores();
        if block.consensus_data.proposer_info.score < min_scores.min_pou_score {
            return ValidationResult::Invalid(ValidationError::InsufficientPouScore);
        }

        // Validate proposer belongs to an active group (if group-aware)
        if let Some(group_id) = &block.consensus_data.proposer_info.group_id {
            if let Some(active_groups) = context.get_active_groups().get(group_id) {
                if active_groups.status != crate::types::GroupStatus::Active {
                    return ValidationResult::Invalid(ValidationError::GroupInactive);
                }

                if !active_groups
                    .members
                    .contains(&block.consensus_data.proposer_info.node_id)
                {
                    return ValidationResult::Invalid(ValidationError::ProposerNotInGroup);
                }
            } else {
                return ValidationResult::Invalid(ValidationError::GroupNotFound);
            }
        }

        // Validate transaction count matches actual transactions
        if block.header.tx_count != block.transactions.len() as u32 {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Validate all transactions in the block
        for tx in &block.transactions {
            let tx_validation = self.validate_transaction(tx, context);
            if !tx_validation.is_valid() {
                return tx_validation;
            }
        }

        if let Err(_) = self.signature_validator.validate_block_signatures(block) {
            return ValidationResult::Invalid(ValidationError::InvalidSignature);
        }

        if let Err(_) = self
            .state_validator
            .validate_state_transitions(block, context)
        {
            return ValidationResult::Invalid(ValidationError::InvalidStateTransition);
        }

        if let Ok(mut stats) = self.stats.try_write() {
            stats.total_validations += 1;
            stats.successful_validations += 1;
            stats.last_validation_timestamp = current_time;
        }

        ValidationResult::Valid
    }

    fn validate_transaction(&self, tx: &Transaction, _context: &Self::Context) -> ValidationResult {
        if !tx.is_valid() {
            return ValidationResult::Invalid(ValidationError::InvalidTransaction);
        }

        ValidationResult::Valid
    }

    fn validate_proposer(
        &self,
        proposer: &ProposerInfo,
        context: &Self::Context,
    ) -> ValidationResult {
        // Check if proposer is blacklisted
        if context.is_blacklisted(&proposer.node_id) {
            return ValidationResult::Invalid(ValidationError::InvalidProposer);
        }

        // Check minimum score requirements
        let min_scores = context.get_min_scores();
        if proposer.score < min_scores.min_pou_score {
            return ValidationResult::Invalid(ValidationError::InsufficientPouScore);
        }

        // Check geographic constraints
        let geo_constraints = context.get_geographic_constraints();
        if geo_constraints.enabled && !geo_constraints.allowed_regions.contains(&proposer.region) {
            return ValidationResult::Invalid(ValidationError::GeographicConstraintViolation);
        }

        ValidationResult::Valid
    }

    fn validate_proposal(
        &self,
        proposal: &dyn Proposal,
        context: &Self::Context,
    ) -> ValidationResult {
        // Validate proposal structure
        if let Err(error) = proposal.validate_structure() {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Check if proposal is expired
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if proposal.is_expired(current_time, context.max_validation_time_ms()) {
            return ValidationResult::Invalid(ValidationError::ValidationTimeout);
        }

        // Validate proposer
        let proposer_info = proposal.proposer_info();
        self.validate_proposer(&proposer_info, context)
    }

    fn validator_info(&self) -> ValidatorInfo {
        self.info.clone()
    }

    fn is_active(&self) -> bool {
        matches!(self.info.status, ValidatorStatus::Active)
    }

    fn stake(&self) -> u64 {
        self.info.stake
    }

    fn score(&self) -> u32 {
        self.info.score
    }

    fn update_score(&self, new_score: u32) -> crate::error::Result<()> {
        // In a real implementation, this would update persistent storage
        // For now, we'll just update the in-memory info
        Ok(())
    }

    fn validation_stats(&self) -> ValidationStats {
        // Note: This would need async in a real implementation
        // For now, return default stats
        ValidationStats::default()
    }
}

pub trait SignatureValidator: Send + Sync {
    fn validate_block_signatures(&self, block: &Block) -> ValidationErrorResult;
    fn validate_transaction_signature(&self, tx: &Transaction) -> ValidationErrorResult;
    fn validate_proposal_signature(&self, proposal: &dyn Proposal) -> ValidationErrorResult;
}

pub trait StateValidator: Send + Sync {
    fn validate_state_transitions(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> ValidationErrorResult;
    fn validate_account_state(&self, account: &[u8], state: &[u8]) -> ValidationErrorResult;
    fn validate_contract_state(&self, contract: &[u8], state: &[u8]) -> ValidationErrorResult;
}

pub struct MockSignatureValidator;

impl SignatureValidator for MockSignatureValidator {
    fn validate_block_signatures(&self, _block: &Block) -> ValidationErrorResult {
        Ok(())
    }

    fn validate_transaction_signature(&self, _tx: &Transaction) -> ValidationErrorResult {
        Ok(())
    }

    fn validate_proposal_signature(&self, _proposal: &dyn Proposal) -> ValidationErrorResult {
        Ok(())
    }
}

pub struct MockStateValidator;

impl StateValidator for MockStateValidator {
    fn validate_state_transitions(
        &self,
        _block: &Block,
        _context: &dyn ValidationContext,
    ) -> ValidationErrorResult {
        Ok(())
    }

    fn validate_account_state(&self, _account: &[u8], _state: &[u8]) -> ValidationErrorResult {
        Ok(())
    }

    fn validate_contract_state(&self, _contract: &[u8], _state: &[u8]) -> ValidationErrorResult {
        Ok(())
    }
}

pub struct MockValidationContext;

impl ValidationContext for MockValidationContext {
    fn current_epoch(&self) -> u64 {
        1
    }

    fn current_slot(&self) -> u64 {
        100
    }

    fn current_height(&self) -> u64 {
        100
    }

    fn validator_id(&self) -> &str {
        "mock_validator"
    }

    fn validation_timestamp(&self) -> u64 {
        1640995200 // 2022-01-01 timestamp
    }

    fn is_strict_mode(&self) -> bool {
        false
    }

    fn max_validation_time_ms(&self) -> u64 {
        5000
    }

    fn get_active_groups(&self) -> &std::collections::HashMap<String, GroupInfo> {
        static EMPTY_GROUPS: std::sync::OnceLock<std::collections::HashMap<String, GroupInfo>> =
            std::sync::OnceLock::new();
        EMPTY_GROUPS.get_or_init(std::collections::HashMap::new)
    }

    fn get_known_proposers(&self) -> &std::collections::HashMap<String, ProposerInfo> {
        static EMPTY_PROPOSERS: std::sync::OnceLock<
            std::collections::HashMap<String, ProposerInfo>,
        > = std::sync::OnceLock::new();
        EMPTY_PROPOSERS.get_or_init(std::collections::HashMap::new)
    }

    fn is_blacklisted(&self, _node_id: &str) -> bool {
        false
    }

    fn get_min_scores(&self) -> &ScoreThresholds {
        static DEFAULT_THRESHOLDS: std::sync::OnceLock<ScoreThresholds> =
            std::sync::OnceLock::new();
        DEFAULT_THRESHOLDS.get_or_init(|| ScoreThresholds::default())
    }

    fn get_geographic_constraints(&self) -> &GeographicConstraints {
        static DEFAULT_CONSTRAINTS: std::sync::OnceLock<GeographicConstraints> =
            std::sync::OnceLock::new();
        DEFAULT_CONSTRAINTS.get_or_init(|| GeographicConstraints::default())
    }

    fn clone_box(&self) -> Box<dyn ValidationContext> {
        Box::new(MockValidationContext)
    }
}

impl Default for ValidationStats {
    fn default() -> Self {
        Self {
            total_validations: 0,
            successful_validations: 0,
            failed_validations: 0,
            average_validation_time_ms: 0.0,
            last_validation_timestamp: 0,
        }
    }
}
