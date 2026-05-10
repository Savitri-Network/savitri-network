//! Adaptive Validator Implementation
//!
//! optimize threading based on workload size and system resources.

use crate::{Block, BlockHeader, Transaction, ValidationResult};
use futures::future;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::RwLock;

pub struct AdaptiveValidator {
    config: AdaptiveValidationConfig,
    stats: Arc<RwLock<AdaptiveValidationStats>>,
    thread_pool: Option<rayon::ThreadPool>,
}

#[derive(Debug, Clone)]
pub struct AdaptiveValidationConfig {
    pub enable_adaptive_threading: bool,
    pub base_thread_count: usize,
    pub max_thread_count: usize,
    pub workload_threshold_small: usize,
    pub workload_threshold_medium: usize,
    pub enable_performance_monitoring: bool,
    pub auto_reconfigure: bool,
}

#[derive(Debug, Clone, Default)]
pub struct AdaptiveValidationStats {
    pub total_validations: u64,
    pub adaptive_validations: u64,
    pub sequential_validations: u64,
    pub avg_adaptive_time_ms: f64,
    pub avg_sequential_time_ms: f64,
    pub thread_reconfigurations: u64,
    pub performance_improvements: u64,
    pub total_time_ms: f64,
}

impl AdaptiveValidator {
    pub fn new() -> Self {
        let num_cpus = thread::available_parallelism().map_or(1, |n| n.get());
        let config = AdaptiveValidationConfig::default_for_cpus(num_cpus);
        Self::with_config(config)
    }

    pub fn with_config(config: AdaptiveValidationConfig) -> Self {
        let thread_pool = if config.enable_adaptive_threading {
            ThreadPoolBuilder::new()
                .num_threads(config.base_thread_count)
                .build()
                .ok()
        } else {
            None
        };

        Self {
            config,
            stats: Arc::new(RwLock::new(AdaptiveValidationStats::default())),
            thread_pool,
        }
    }

    pub fn for_workload(workload_size: usize) -> Self {
        let num_cpus = thread::available_parallelism().map_or(1, |n| n.get());
        let config = AdaptiveValidationConfig::for_workload(workload_size, num_cpus);
        Self::with_config(config)
    }

    /// Validate a block with adaptive threading
    pub async fn validate_block(&self, block: &Block) -> ValidationResult {
        let start_time = Instant::now();

        let result = if self.config.enable_adaptive_threading {
            self.validate_block_adaptive(block).await
        } else {
            self.validate_block_sequential(block).await
        };

        // Update statistics
        let duration = start_time.elapsed().as_millis() as f64;
        self.update_stats(duration, self.config.enable_adaptive_threading)
            .await;

        result
    }

    /// Validate multiple blocks with adaptive threading
    pub async fn validate_blocks(&self, blocks: &[Block]) -> Vec<ValidationResult> {
        let start_time = Instant::now();

        let results = if self.config.enable_adaptive_threading {
            self.validate_blocks_adaptive(blocks).await
        } else {
            self.validate_blocks_sequential(blocks).await
        };

        let duration = start_time.elapsed().as_millis() as f64;
        tracing::debug!("Validated {} blocks in {}ms", blocks.len(), duration);

        results
    }

    async fn validate_block_adaptive(&self, block: &Block) -> ValidationResult {
        let workload_size = self.calculate_workload_size(block);
        let optimal_threads = self.calculate_optimal_threads(workload_size);

        // Reconfigure thread pool if needed and auto-reconfigure is enabled
        if self.config.auto_reconfigure {
            self.reconfigure_thread_pool(optimal_threads).await;
        }

        if let Some(ref pool) = self.thread_pool {
            let result =
                pool.install(|| self.validate_block_parallel_optimized(block, optimal_threads));

            match result {
                Ok(_) => ValidationResult::Valid,
                Err(e) => ValidationResult::Invalid(e),
            }
        } else {
            self.validate_block_sequential(block).await
        }
    }

    async fn validate_blocks_adaptive(&self, blocks: &[Block]) -> Vec<ValidationResult> {
        let total_workload: usize = blocks.iter().map(|b| self.calculate_workload_size(b)).sum();

        let optimal_threads = self.calculate_optimal_threads(total_workload);

        // Reconfigure thread pool if needed
        if self.config.auto_reconfigure {
            self.reconfigure_thread_pool(optimal_threads).await;
        }

        if let Some(ref pool) = self.thread_pool {
            let results: Vec<ValidationResult> = pool.install(|| {
                blocks
                    .par_iter()
                    .map(|block| {
                        match self.validate_block_parallel_optimized(block, optimal_threads) {
                            Ok(()) => ValidationResult::Valid,
                            Err(e) => ValidationResult::Invalid(e),
                        }
                    })
                    .collect()
            });

            results
        } else {
            self.validate_blocks_sequential(blocks).await
        }
    }

    fn validate_block_parallel_optimized(
        &self,
        block: &Block,
        _thread_count: usize,
    ) -> Result<(), String> {
        let structure_valid = self.validate_structure_comprehensive(&block.header);
        if !structure_valid {
            return Err("Structure validation failed".to_string());
        }

        let tx_valid = self.validate_transactions_comprehensive(&block.transactions);
        if !tx_valid {
            return Err("Transaction validation failed".to_string());
        }

        if block.header.height > 0 {
            let consensus_valid = self.validate_consensus_rules(block);
            if !consensus_valid {
                return Err("Consensus validation failed".to_string());
            }
        }

        Ok(())
    }

    fn validate_structure_comprehensive(&self, header: &crate::types::block::BlockHeader) -> bool {
        let checks = vec![
            header.height > 0,
            header.timestamp > 0,
            header.tx_count > 0,
            header.version > 0,
        ];

        checks.par_iter().all(|&valid| valid)
    }

    fn validate_timestamp(&self, header: &BlockHeader) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let checks = vec![
            header.timestamp <= current_time + 300, // Not too far in future
            header.timestamp >= current_time - 86400, // Not too far in past
            header.timestamp > 0,
        ];

        checks.par_iter().all(|&valid| valid)
    }

    fn validate_parent_relationships(&self, header: &BlockHeader) -> bool {
        if header.height == 0 {
            return true; // Genesis block has no parents
        }

        let has_valid_parents = !header.parent_hashes.is_empty() || !header.parent_hash.is_empty();
        has_valid_parents
    }

    fn validate_transactions_comprehensive(&self, transactions: &[Transaction]) -> bool {
        transactions.par_iter().all(|tx| {
            let checks = vec![
                tx.from != tx.to,
                tx.amount > 0,
                !tx.from.is_empty(),
                !tx.to.is_empty(),
            ];

            checks.par_iter().all(|&valid| valid)
        })
    }

    fn validate_consensus_rules(&self, block: &Block) -> bool {
        let rules = vec![
            self.validate_block_size(block),
            self.validate_transaction_count(block),
            self.validate_merkle_root(block),
        ];

        rules.par_iter().all(|&valid| valid)
    }

    fn validate_block_size(&self, block: &Block) -> bool {
        // Simple size check (can be made more sophisticated)
        block.transactions.len() <= 10000
    }

    fn validate_transaction_count(&self, block: &Block) -> bool {
        block.header.tx_count == block.transactions.len() as u32
    }

    fn validate_merkle_root(&self, block: &Block) -> bool {
        use crate::crypto::hashes::sha256;
        use crate::crypto::merkle::compute_tx_root;

        // If no transactions, tx_root should be zero
        if block.transactions.is_empty() {
            return block.header.tx_root == crate::types::block::Hash64([0u8; 64]);
        }

        // Compute transaction hashes
        let tx_hashes: Vec<[u8; 32]> = block
            .transactions
            .iter()
            .map(|tx| {
                // Hash transaction data
                let mut data = Vec::new();
                data.extend_from_slice(&tx.from);
                data.extend_from_slice(&tx.to);
                data.extend_from_slice(&tx.amount.to_le_bytes());
                data.extend_from_slice(&tx.nonce.to_le_bytes());
                data.extend_from_slice(&tx.data);
                sha256(&data)
            })
            .collect();

        // Compute merkle root and compare
        let computed_root = compute_tx_root(&tx_hashes);
        // Convert [u8; 32] to [u8; 64] for comparison
        let mut computed_root_64 = [0u8; 64];
        computed_root_64[..32].copy_from_slice(&computed_root);
        computed_root_64 == block.header.tx_root.0
    }

    async fn validate_block_sequential(&self, block: &Block) -> ValidationResult {
        if block.header.height == 0 {
            return ValidationResult::Invalid("Invalid block height".to_string());
        }

        if block.transactions.is_empty() {
            return ValidationResult::Invalid("Empty block".to_string());
        }

        ValidationResult::Valid
    }

    async fn validate_blocks_sequential(&self, blocks: &[Block]) -> Vec<ValidationResult> {
        let mut results = Vec::new();
        for block in blocks {
            results.push(self.validate_block_sequential(block).await);
        }
        results
    }

    /// Calculate workload size for a block
    fn calculate_workload_size(&self, block: &Block) -> usize {
        block.transactions.len() + block.header.parent_hashes.len()
    }

    /// Calculate optimal threads based on workload
    fn calculate_optimal_threads(&self, workload_size: usize) -> usize {
        let base_threads = self.config.base_thread_count;
        let max_threads = self.config.max_thread_count;

        // Small workloads: use fewer threads
        if workload_size < self.config.workload_threshold_small {
            return (base_threads / 2).max(1);
        }

        // Medium workloads: use moderate threads
        if workload_size < self.config.workload_threshold_medium {
            return (base_threads * 3 / 4).max(2);
        }

        // Large workloads: use maximum threads
        max_threads
    }

    /// Reconfigure thread pool for optimal performance
    async fn reconfigure_thread_pool(&self, optimal_threads: usize) {
        // This is a simplified version - in practice, you might want
        // more sophisticated thread pool management
        tracing::debug!("Optimal threads calculated: {}", optimal_threads);
    }

    async fn update_stats(&self, duration: f64, was_adaptive: bool) {
        let mut stats = self.stats.write().await;
        stats.total_validations += 1;
        stats.total_time_ms += duration;

        if was_adaptive {
            stats.adaptive_validations += 1;
            stats.avg_adaptive_time_ms = if stats.adaptive_validations == 1 {
                duration
            } else {
                (stats.avg_adaptive_time_ms * (stats.adaptive_validations - 1) as f64 + duration)
                    / stats.adaptive_validations as f64
            };
        } else {
            stats.sequential_validations += 1;
            stats.avg_sequential_time_ms = if stats.sequential_validations == 1 {
                duration
            } else {
                (stats.avg_sequential_time_ms * (stats.sequential_validations - 1) as f64
                    + duration)
                    / stats.sequential_validations as f64
            };
        }
    }

    pub async fn get_stats(&self) -> AdaptiveValidationStats {
        self.stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = AdaptiveValidationStats::default();
    }
}

impl AdaptiveValidationConfig {
    /// Default configuration for given CPU count
    pub fn default_for_cpus(num_cpus: usize) -> Self {
        Self {
            enable_adaptive_threading: true,
            base_thread_count: (num_cpus * 3 / 4).max(2),
            max_thread_count: num_cpus,
            workload_threshold_small: 10,
            workload_threshold_medium: 50,
            enable_performance_monitoring: true,
            auto_reconfigure: true,
        }
    }

    /// Configuration optimized for specific workload
    pub fn for_workload(workload_size: usize, num_cpus: usize) -> Self {
        let base_threads = if workload_size < 10 {
            (num_cpus / 2).max(1)
        } else if workload_size < 50 {
            (num_cpus * 3 / 4).max(2)
        } else {
            num_cpus
        };

        Self {
            enable_adaptive_threading: true,
            base_thread_count: base_threads,
            max_thread_count: num_cpus,
            workload_threshold_small: 10,
            workload_threshold_medium: 50,
            enable_performance_monitoring: true,
            auto_reconfigure: false, // Don't auto-reconfigure for specific workloads
        }
    }
}

impl Default for AdaptiveValidationConfig {
    fn default() -> Self {
        let num_cpus = thread::available_parallelism().map_or(1, |n| n.get());
        Self::default_for_cpus(num_cpus)
    }
}
