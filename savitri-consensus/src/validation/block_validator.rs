//!
//! that can be used across different consensus mechanisms.

use crate::error::ConsensusError;
use crate::traits::{
    DefaultValidationContext, MockSignatureValidator, MockStateValidator, MockValidationContext,
    SignatureValidator, StateValidator, ValidationContext,
};
use crate::types::block::{Block as BlockType, Transaction as BlockTransaction};
use crate::types::validation::ValidationError;
use crate::{Block, BlockHeader, Result, Transaction, ValidationResult};
use futures::future;
use std::sync::Arc;

pub struct BlockValidator {
    signature_validator: Arc<dyn SignatureValidator>,
    state_validator: Arc<dyn StateValidator>,
    config: BlockValidationConfig,
    stats: Arc<tokio::sync::RwLock<BlockValidationStats>>,
}

#[derive(Debug, Clone)]
pub struct BlockValidationConfig {
    pub enable_signature_validation: bool,
    pub enable_state_validation: bool,
    pub enable_structure_validation: bool,
    pub enable_timestamp_validation: bool,
    pub enable_parent_hash_validation: bool,
    pub enable_transaction_validation: bool,
    pub enable_multi_parent_validation: bool,
    /// Maximum parent hashes per block
    pub max_parent_hashes: usize,
    pub enable_parallel_validation: bool,
    /// Maximum block size in bytes
    pub max_block_size: u64,
    /// Maximum transactions per block
    pub max_transactions_per_block: u32,
    /// Timestamp tolerance in seconds
    pub timestamp_tolerance_secs: u64,
    /// Minimum block interval in seconds
    pub min_block_interval_secs: u64,
}

impl Default for BlockValidationConfig {
    fn default() -> Self {
        Self {
            enable_signature_validation: true,
            enable_state_validation: true,
            enable_structure_validation: true,
            enable_timestamp_validation: true,
            enable_parent_hash_validation: true,
            enable_transaction_validation: true,
            enable_multi_parent_validation: false, // Disabled by default for backward compatibility
            max_parent_hashes: 10,                 // Support up to 10 parent hashes
            enable_parallel_validation: false,     // Disabled by default
            max_block_size: 1024 * 1024,           // 1MB
            max_transactions_per_block: 10000,
            timestamp_tolerance_secs: 300, // 5 minutes
            min_block_interval_secs: 1,    // 1 second
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockValidationStats {
    pub total_blocks: u64,
    /// Valid blocks
    pub valid_blocks: u64,
    /// Invalid blocks
    pub invalid_blocks: u64,
    pub avg_validation_time_ms: f64,
    pub signature_failures: u64,
    pub state_failures: u64,
    pub structure_failures: u64,
    pub timestamp_failures: u64,
    pub transaction_failures: u64,
}

impl BlockValidator {
    pub fn new(
        signature_validator: Arc<dyn SignatureValidator>,
        state_validator: Arc<dyn StateValidator>,
        config: BlockValidationConfig,
    ) -> Self {
        Self {
            signature_validator,
            state_validator,
            config,
            stats: Arc::new(tokio::sync::RwLock::new(BlockValidationStats::default())),
        }
    }

    /// Validate a block with comprehensive checks - ESTESO per parallelismo
    pub async fn validate_block(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> ValidationResult {
        let start_time = std::time::Instant::now();

        if self.config.enable_parallel_validation && !block.header.parent_hashes.is_empty() {
            return self.validate_block_parallel(block, context).await;
        }

        // Comportamento esistente (backward compatibility)
        if self.config.enable_structure_validation {
            if let Err(error) = self.validate_structure(block) {
                self.record_failure("structure").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        if self.config.enable_timestamp_validation {
            if let Err(error) = self.validate_timestamp(block, context) {
                self.record_failure("timestamp").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        if self.config.enable_parent_hash_validation {
            if let Err(error) = self.validate_parent_hash(block, context).await {
                self.record_failure("parent_hash").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        if self.config.enable_transaction_validation {
            if let Err(error) = self.validate_transactions(block, context).await {
                self.record_failure("transaction").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        if self.config.enable_signature_validation {
            if let Err(error) = self
                .signature_validator
                .validate_block_signatures(&BlockType::from(block))
            {
                self.record_failure("signature").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        if self.config.enable_state_validation {
            if let Err(error) = self
                .state_validator
                .validate_state_transitions(&BlockType::from(block), context)
            {
                self.record_failure("state").await;
                return ValidationResult::Invalid(error.to_string());
            }
        }

        // Record success
        self.record_success(start_time.elapsed().as_millis() as f64)
            .await;

        ValidationResult::Valid
    }

    /// Validate block structure
    fn validate_structure(&self, block: &Block) -> crate::Result<()> {
        // Check basic block requirements
        if block.header.height == 0 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        if block.header.timestamp == 0 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidTimestamp,
            ));
        }

        if block.transactions.is_empty() {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        if block.header.tx_count != block.transactions.len() as u32 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        // Check block size
        let block_size = bincode::serialized_size(block).unwrap_or(0);
        if block_size > self.config.max_block_size {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        // Check transaction count
        if block.transactions.len() > self.config.max_transactions_per_block as usize {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        // Validate each transaction structure
        for (i, tx) in block.transactions.iter().enumerate() {
            if tx.hash.is_empty() || tx.from.is_empty() || tx.to.is_empty() {
                return Err(ConsensusError::ValidationError(
                    ValidationError::InvalidTransaction,
                ));
            }

            // Check for duplicate transactions
            if block.transactions[..i].contains(tx) {
                return Err(ConsensusError::ValidationError(
                    ValidationError::InvalidTransaction,
                ));
            }
        }

        Ok(())
    }

    /// Validate block timestamp
    fn validate_timestamp(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> crate::Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check if timestamp is within tolerance
        if block.header.timestamp > current_time + self.config.timestamp_tolerance_secs {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidTimestamp,
            ));
        }

        if block.header.timestamp < current_time - self.config.timestamp_tolerance_secs {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidTimestamp,
            ));
        }

        // Check minimum block interval
        if context.current_height() > 0 {
            // This would require getting the previous block timestamp
            // For now, we'll skip this check
        }

        Ok(())
    }

    /// Validate parent hash - ESTESO per DAG
    async fn validate_parent_hash(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> crate::Result<()> {
        if block.header.height == 0 {
            // Genesis block has no parent
            return Ok(());
        }

        // NUOVO: Supporto multi-genitori
        if self.config.enable_multi_parent_validation && !block.header.parent_hashes.is_empty() {
            if block.header.parent_hashes.len() > self.config.max_parent_hashes {
                return Err(ConsensusError::ValidationError(
                    ValidationError::InvalidStructure(format!(
                        "Too many parent hashes: {} > {}",
                        block.header.parent_hashes.len(),
                        self.config.max_parent_hashes
                    )),
                ));
            }

            for parent_hash in &block.header.parent_hashes {
                if parent_hash.0.len() != 64 {
                    return Err(ConsensusError::ValidationError(
                        ValidationError::InvalidParentHash,
                    ));
                }

                // Check for zero hash
                if parent_hash.0.iter().all(|&b| b == 0) {
                    return Err(ConsensusError::ValidationError(
                        ValidationError::InvalidParentHash,
                    ));
                }

                // Check for duplicates
                let count = block
                    .header
                    .parent_hashes
                    .iter()
                    .filter(|&h| h == parent_hash)
                    .count();
                if count > 1 {
                    return Err(ConsensusError::ValidationError(
                        ValidationError::InvalidStructure(
                            "Duplicate parent hash detected".to_string(),
                        ),
                    ));
                }

                // In a real implementation, we would verify each parent hash against actual parent blocks
                // For now, we'll just check that it's not all zeros and has correct format
            }
        } else {
            // Fallback a singolo parent (backward compatibility)
            if block.header.parent_hash.0 == [0u8; 64] {
                return Err(ConsensusError::ValidationError(
                    ValidationError::InvalidParentHash,
                ));
            }
        }

        Ok(())
    }

    async fn validate_single_parent(
        &self,
        parent_hash: &[u8],
        context: &dyn ValidationContext,
    ) -> crate::Result<()> {
        // Check hash length
        if parent_hash.len() != 64 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidParentHash,
            ));
        }

        // Check for zero hash
        if parent_hash.iter().all(|&b| b == 0) {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidParentHash,
            ));
        }

        // In a real implementation, we would verify the parent hash against actual parent block
        // For now, we'll just check that it's not all zeros and has correct format

        Ok(())
    }

    /// NUOVO: Validazione parallela per blocchi DAG
    async fn validate_block_parallel(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> ValidationResult {
        if !self.config.enable_parallel_validation || block.header.parent_hashes.len() <= 1 {
            match self.validate_parent_hash(block, context).await {
                Ok(_) => ValidationResult::Valid,
                Err(e) => ValidationResult::Invalid(e.to_string()),
            }
        } else {
            let validation_tasks: Vec<_> = block
                .header
                .parent_hashes
                .iter()
                .map(|parent_hash| {
                    let validator = self;
                    let context_clone = context;
                    let parent_hash = parent_hash.clone();
                    async move {
                        validator
                            .validate_single_parent(&parent_hash.0, context_clone)
                            .await
                    }
                })
                .collect();

            let results = future::join_all(validation_tasks).await;

            // Controlla risultati
            for result in results {
                if let Err(error) = result {
                    return ValidationResult::Invalid(error.to_string());
                }
            }

            ValidationResult::Valid
        }
    }

    /// Validate all transactions in the block
    async fn validate_transactions(
        &self,
        block: &Block,
        context: &dyn ValidationContext,
    ) -> crate::Result<()> {
        for tx in &block.transactions {
            // Validate individual transaction
            if tx.hash.is_empty() || tx.from.is_empty() || tx.to.is_empty() {
                return Err(ConsensusError::ValidationError(
                    ValidationError::InvalidTransaction,
                ));
            }

            // Check transaction nonce (would require account state)
            // Check transaction fee (would require gas calculation)
            // Check transaction signature (would require cryptography)
        }

        Ok(())
    }

    async fn record_success(&self, duration_ms: f64) {
        let mut stats = self.stats.write().await;
        stats.total_blocks += 1;
        stats.valid_blocks += 1;

        stats.avg_validation_time_ms = if stats.total_blocks == 1 {
            duration_ms
        } else {
            (stats.avg_validation_time_ms * (stats.total_blocks - 1) as f64 + duration_ms)
                / stats.total_blocks as f64
        };
    }

    async fn record_failure(&self, failure_type: &str) {
        let mut stats = self.stats.write().await;
        stats.total_blocks += 1;
        stats.invalid_blocks += 1;

        match failure_type {
            "signature" => stats.signature_failures += 1,
            "state" => stats.state_failures += 1,
            "structure" => stats.structure_failures += 1,
            "timestamp" => stats.timestamp_failures += 1,
            "transaction" => stats.transaction_failures += 1,
            _ => {}
        }
    }

    pub async fn get_stats(&self) -> BlockValidationStats {
        self.stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = BlockValidationStats::default();
    }
}

pub struct FastBlockValidator {
    config: BlockValidationConfig,
    stats: Arc<tokio::sync::RwLock<BlockValidationStats>>,
}

impl FastBlockValidator {
    pub fn new(config: BlockValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(tokio::sync::RwLock::new(BlockValidationStats::default())),
        }
    }

    pub async fn validate_block_fast(
        &self,
        block: &Block,
        _context: &dyn ValidationContext,
    ) -> ValidationResult {
        let start_time = std::time::Instant::now();

        if block.header.height == 0 || block.header.timestamp == 0 || block.transactions.is_empty()
        {
            self.record_failure("structure").await;
            return ValidationResult::Invalid(ValidationError::InvalidBlock.to_string());
        }

        // Check block size
        let block_size = bincode::serialized_size(block).unwrap_or(0);
        if block_size > self.config.max_block_size {
            self.record_failure("size").await;
            return ValidationResult::Invalid(ValidationError::InvalidBlock.to_string());
        }

        // Record success
        self.record_success(start_time.elapsed().as_millis() as f64)
            .await;

        ValidationResult::Valid
    }

    async fn record_success(&self, duration_ms: f64) {
        let mut stats = self.stats.write().await;
        stats.total_blocks += 1;
        stats.valid_blocks += 1;

        stats.avg_validation_time_ms = if stats.total_blocks == 1 {
            duration_ms
        } else {
            (stats.avg_validation_time_ms * (stats.total_blocks - 1) as f64 + duration_ms)
                / stats.total_blocks as f64
        };
    }

    async fn record_failure(&self, failure_type: &str) {
        let mut stats = self.stats.write().await;
        stats.total_blocks += 1;
        stats.invalid_blocks += 1;

        match failure_type {
            "structure" => stats.structure_failures += 1,
            "size" => stats.structure_failures += 1,
            _ => {}
        }
    }
}

pub struct BlockValidationUtils;

impl BlockValidationUtils {
    /// Calculate block hash
    pub fn calculate_block_hash(block: &Block) -> [u8; 64] {
        block.hash()
    }

    /// Calculate transaction root (Merkle root)
    pub fn calculate_transaction_root(transactions: &[Transaction]) -> [u8; 64] {
        if transactions.is_empty() {
            return [0u8; 64];
        }

        // Simple implementation: hash all transaction hashes
        let mut combined = Vec::new();
        for tx in transactions {
            combined.extend_from_slice(&tx.hash);
        }

        {
            let hash = blake3::hash(&combined);
            let mut result = [0u8; 64];
            result.copy_from_slice(hash.as_bytes());
            result
        }
    }

    /// Calculate state root
    pub fn calculate_state_root(_block: &Block) -> [u8; 64] {
        // In a real implementation, this would calculate the state root
        // based on the state changes from the transactions
        [0u8; 64]
    }

    /// Validate block header fields
    pub fn validate_header_fields(header: &BlockHeader) -> crate::Result<()> {
        if header.version == 0 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        if header.height == 0 && header.parent_hash != [0u8; 64] {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidParentHash,
            ));
        }

        if header.height > 0 && header.parent_hash == [0u8; 64] {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidParentHash,
            ));
        }

        Ok(())
    }

    /// Check block continuity
    pub fn check_block_continuity(current: &Block, previous: &Block) -> crate::Result<()> {
        // Check height continuity
        if current.header.height != previous.header.height + 1 {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidBlock,
            ));
        }

        // Check parent hash
        if current.header.parent_hash != crate::types::block::Hash64(previous.hash()) {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidParentHash,
            ));
        }

        // Check timestamp continuity
        if current.header.timestamp <= previous.header.timestamp {
            return Err(ConsensusError::ValidationError(
                ValidationError::InvalidTimestamp,
            ));
        }

        Ok(())
    }
}
