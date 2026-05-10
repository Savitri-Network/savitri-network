//! Parallel Block Validator Implementation
//!
//! working with existing Block structures without requiring modifications.

use crate::{Block, BlockHeader, Transaction, ValidationResult};
use futures::future;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

pub struct ParallelBlockValidator {
    config: ParallelValidationConfig,
    stats: Arc<tokio::sync::RwLock<ParallelValidationStats>>,
}

#[derive(Debug, Clone)]
pub struct ParallelValidationConfig {
    pub enable_parallel_validation: bool,
    pub max_concurrent_validations: usize,
    pub timeout_ms: u64,
    pub enable_adaptive_threading: bool,
    pub num_cpus: usize,
}

#[derive(Debug, Clone)]
pub struct ParallelValidationStats {
    pub total_blocks_validated: u64,
    pub parallel_validations: u64,
    pub sequential_validations: u64,
    pub avg_parallel_time_ms: f64,
    pub avg_sequential_time_ms: f64,
    pub total_time_ms: f64,
}

impl ParallelBlockValidator {
    pub fn new(config: ParallelValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(tokio::sync::RwLock::new(ParallelValidationStats::default())),
        }
    }

    pub fn new_adaptive() -> Self {
        let num_cpus = thread::available_parallelism().map_or(1, |n| n.get());
        let config = ParallelValidationConfig {
            enable_parallel_validation: true,
            max_concurrent_validations: Self::calculate_optimal_threads(num_cpus),
            timeout_ms: 5000,
            enable_adaptive_threading: true,
            num_cpus,
        };
        Self::new(config)
    }

    pub fn new_adaptive_for_workload(workload_size: usize) -> Self {
        let num_cpus = thread::available_parallelism().map_or(1, |n| n.get());
        let optimal_threads = Self::calculate_optimal_threads_for_workload(workload_size, num_cpus);
        let config = ParallelValidationConfig {
            enable_parallel_validation: true,
            max_concurrent_validations: optimal_threads,
            timeout_ms: 5000,
            enable_adaptive_threading: true,
            num_cpus,
        };
        Self::new(config)
    }

    fn calculate_optimal_threads(num_cpus: usize) -> usize {
        (num_cpus * 3 / 4).max(2)
    }

    /// Calculate optimal threads based on specific workload size
    fn calculate_optimal_threads_for_workload(workload_size: usize, num_cpus: usize) -> usize {
        // Small workloads: use fewer threads to avoid overhead
        if workload_size < 10 {
            return (num_cpus / 2).max(1);
        }

        // Medium workloads: use most threads but leave some for system
        if workload_size < 50 {
            return (num_cpus * 3 / 4).max(2);
        }

        // Large workloads: use all available threads
        num_cpus
    }

    pub async fn validate_block(&self, block: &Block) -> ValidationResult {
        let start_time = Instant::now();

        if self.config.enable_parallel_validation && !block.header.parent_hashes.is_empty() {
            let result = self.validate_block_parallel(block).await;

            // Record statistics
            let duration = start_time.elapsed().as_millis() as f64;
            let mut stats = self.stats.write().await;
            stats.total_blocks_validated += 1;
            stats.parallel_validations += 1;
            stats.avg_parallel_time_ms = if stats.parallel_validations == 1 {
                duration
            } else {
                (stats.avg_parallel_time_ms * (stats.parallel_validations - 1) as f64 + duration)
                    / stats.parallel_validations as f64
            };
            stats.total_time_ms += duration;

            return result;
        }

        let result = self.validate_block_sequential(block).await;

        // Record statistics
        let duration = start_time.elapsed().as_millis() as f64;
        let mut stats = self.stats.write().await;
        stats.total_blocks_validated += 1;
        stats.sequential_validations += 1;
        stats.avg_sequential_time_ms = if stats.sequential_validations == 1 {
            duration
        } else {
            (stats.avg_sequential_time_ms * (stats.sequential_validations - 1) as f64 + duration)
                / stats.sequential_validations as f64
        };
        stats.total_time_ms += duration;

        result
    }

    async fn validate_block_parallel(&self, block: &Block) -> ValidationResult {
        let start_time = Instant::now();

        if self.config.enable_adaptive_threading {
            return self.validate_block_parallel_adaptive(block).await;
        }

        self.validate_block_parallel_sequential(block).await
    }

    async fn validate_block_parallel_adaptive(&self, block: &Block) -> ValidationResult {
        let start_time = Instant::now();

        // Calculate optimal threads based on workload
        let workload_size = block.transactions.len() + block.header.parent_hashes.len();
        let optimal_threads =
            Self::calculate_optimal_threads_for_workload(workload_size, self.config.num_cpus);

        // Create adaptive thread pool
        let pool_result = ThreadPoolBuilder::new()
            .num_threads(optimal_threads)
            .build();

        let pool = match pool_result {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("Failed to create thread pool: {:?}", e);
                return ValidationResult::Invalid("Thread pool creation failed".to_string());
            }
        };

        let result: Result<(), String> = pool.install(|| {
            let structure_valid = self.validate_structure_parallel(&block.header);
            if !structure_valid {
                return Err("Structure validation failed".to_string());
            }

            let tx_valid = self.validate_transactions_parallel(&block.transactions);
            if !tx_valid {
                return Err("Transaction validation failed".to_string());
            }

            Ok(())
        });

        match result {
            Ok(_) => {
                let duration = start_time.elapsed();
                tracing::debug!(
                    "Adaptive parallel validation completed in {}ms ({} threads)",
                    duration.as_millis(),
                    optimal_threads
                );
                ValidationResult::Valid
            }
            Err(e) => {
                let duration = start_time.elapsed();
                tracing::warn!(
                    "Adaptive parallel validation failed in {}ms: {}",
                    duration.as_millis(),
                    e
                );
                ValidationResult::Invalid(e)
            }
        }
    }

    fn validate_structure_parallel(&self, header: &crate::types::block::BlockHeader) -> bool {
        let checks = vec![
            header.height > 0,
            header.timestamp > 0,
            !header.parent_hashes.is_empty() || !header.parent_hash.0.iter().all(|&b| b == 0),
            header.tx_count > 0,
        ];

        checks.par_iter().all(|&valid| valid)
    }

    fn validate_transactions_parallel(&self, transactions: &[Transaction]) -> bool {
        // Validate transactions in parallel
        transactions
            .par_iter()
            .all(|tx| tx.from != tx.to && tx.amount > 0)
    }

    async fn validate_block_parallel_sequential(&self, block: &Block) -> ValidationResult {
        let start_time = Instant::now();

        if block.header.height == 0 {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        if block.header.timestamp == 0 {
            return ValidationResult::Invalid("Invalid timestamp".to_string());
        }

        if block.transactions.is_empty() {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        if block.header.tx_count != block.transactions.len() as u32 {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if block.header.timestamp > current_time + 300 {
            return ValidationResult::Invalid("Invalid timestamp".to_string());
        }

        if block.header.height > 0
            && block.header.parent_hashes.is_empty()
            && block.header.parent_hash.0.iter().all(|&b| b == 0)
        {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        for tx in &block.transactions {
            if tx.from == tx.to {
                return ValidationResult::Invalid("Invalid transaction".to_string());
            }
        }

        let duration = start_time.elapsed();
        tracing::debug!(
            "Sequential parallel validation completed in {}ms",
            duration.as_millis()
        );

        ValidationResult::Valid
    }

    async fn validate_block_sequential(&self, block: &Block) -> ValidationResult {
        if block.header.height == 0 {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        if block.header.timestamp == 0 {
            return ValidationResult::Invalid("Invalid timestamp".to_string());
        }

        if block.transactions.is_empty() {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        if block.header.tx_count != block.transactions.len() as u32 {
            return ValidationResult::Invalid("Invalid block".to_string());
        }

        for tx in &block.transactions {
            if tx.from == tx.to {
                return ValidationResult::Invalid("Invalid transaction".to_string());
            }
        }

        ValidationResult::Valid
    }

    async fn validate_structure_async(&self, block: &Block) -> Result<(), ()> {
        let block = block.clone();
        tokio::spawn(async move {
            if block.header.height == 0 {
                Err(())
            } else if block.header.timestamp == 0 {
                Err(())
            } else if block.transactions.is_empty() {
                Err(())
            } else if block.header.tx_count != block.transactions.len() as u32 {
                Err(())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap()
    }

    async fn validate_timestamp_async(&self, block: &Block) -> Result<(), ()> {
        let block = block.clone();
        tokio::spawn(async move {
            let current_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if block.header.timestamp > current_time + 300 {
                Err(())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap()
    }

    async fn validate_parent_hash_async(&self, block: &Block) -> Result<(), ()> {
        let block = block.clone();
        tokio::spawn(async move {
            // For multi-parent blocks, parent_hashes should not be empty
            // For single-parent blocks, parent_hash should be set
            if block.header.height > 0
                && block.header.parent_hashes.is_empty()
                && block.header.parent_hash.0.iter().all(|&b| b == 0)
            {
                Err(())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap()
    }

    async fn validate_transactions_async(&self, block: &Block) -> Result<(), ()> {
        let block = block.clone();
        tokio::spawn(async move {
            for tx in &block.transactions {
                if tx.from == tx.to {
                    return Err(());
                }
            }
            Ok(())
        })
        .await
        .unwrap()
    }

    pub async fn get_stats(&self) -> ParallelValidationStats {
        self.stats.read().await.clone()
    }

    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = ParallelValidationStats::default();
    }
}

impl Default for ParallelValidationConfig {
    fn default() -> Self {
        let num_cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            enable_parallel_validation: false, // Disabled by default for backward compatibility
            max_concurrent_validations: (num_cpus * 3 / 4).max(2),
            timeout_ms: 5000,
            enable_adaptive_threading: false,
            num_cpus,
        }
    }
}

impl Default for ParallelValidationStats {
    fn default() -> Self {
        Self {
            total_blocks_validated: 0,
            parallel_validations: 0,
            sequential_validations: 0,
            avg_parallel_time_ms: 0.0,
            avg_sequential_time_ms: 0.0,
            total_time_ms: 0.0,
        }
    }
}
