//! Proposer Transaction Pool
//!
//! Dedicated transaction pool for proposers with priority handling
//! and group-aware transaction management.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::group_aware_selection::{PrioritizedTransaction, TransactionPriority};
use savitri_core::Transaction;

/// Proposer pool configuration
#[derive(Debug, Clone)]
pub struct ProposerPoolConfig {
    /// Maximum pool size per proposer
    pub max_pool_size: usize,
    /// Enable priority sorting
    pub enable_priority_sorting: bool,
    /// Enable fee-based prioritization
    pub enable_fee_prioritization: bool,
    /// Minimum fee threshold
    pub min_fee_threshold: u64,
    /// Pool cleanup interval in seconds
    pub cleanup_interval_secs: u64,
    /// Transaction timeout in seconds
    pub transaction_timeout_secs: u64,
}

impl Default for ProposerPoolConfig {
    fn default() -> Self {
        Self {
            max_pool_size: 1000,
            enable_priority_sorting: true,
            enable_fee_prioritization: true,
            min_fee_threshold: 1000,
            cleanup_interval_secs: 30,
            transaction_timeout_secs: 300,
        }
    }
}

/// Proposer transaction pool
#[derive(Debug)]
pub struct ProposerTransactionPool {
    proposer_id: String,
    transactions: VecDeque<PrioritizedTransaction>,
    config: ProposerPoolConfig,
    total_fees: u64,
    last_updated: u64,
    stats: ProposerPoolStats,
}

/// Proposer pool statistics
#[derive(Debug, Clone, Default)]
pub struct ProposerPoolStats {
    pub total_transactions: u64,
    pub transactions_processed: u64,
    pub total_fees_collected: u64,
    pub average_fee: f64,
    pub priority_distribution: HashMap<TransactionPriority, u64>,
    pub pool_hits: u64,
    pub pool_misses: u64,
}

/// Proposer Pool Manager
pub struct ProposerPoolManager {
    config: ProposerPoolConfig,
    pools: Arc<RwLock<HashMap<String, ProposerTransactionPool>>>,
    global_stats: Arc<RwLock<ProposerPoolStats>>,
}

impl ProposerPoolManager {
    pub fn new(config: ProposerPoolConfig) -> Self {
        Self {
            config,
            pools: Arc::new(RwLock::new(HashMap::new())),
            global_stats: Arc::new(RwLock::new(ProposerPoolStats::default())),
        }
    }

    /// Add transaction to proposer pool
    pub async fn add_transaction(
        &self,
        proposer_id: &str,
        transaction: Transaction,
        priority: TransactionPriority,
    ) -> Result<()> {
        let prioritized_tx = PrioritizedTransaction {
            transaction,
            priority: priority.clone(),
            group_score: 1.0,
            proposer_bonus: 1.0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let mut pools = self.pools.write().await;
        let pool =
            pools
                .entry(proposer_id.to_string())
                .or_insert_with(|| ProposerTransactionPool {
                    proposer_id: proposer_id.to_string(),
                    transactions: VecDeque::new(),
                    config: self.config.clone(),
                    total_fees: 0,
                    last_updated: prioritized_tx.timestamp,
                    stats: ProposerPoolStats::default(),
                });

        // Check fee threshold
        if self.config.enable_fee_prioritization
            && prioritized_tx.transaction.fee < self.config.min_fee_threshold
        {
            warn!(
                proposer_id = %proposer_id,
                fee = prioritized_tx.transaction.fee,
                threshold = self.config.min_fee_threshold,
                "Transaction fee below threshold"
            );
            return Ok(());
        }

        // Add transaction if pool not full
        if pool.transactions.len() < self.config.max_pool_size {
            pool.transactions.push_back(prioritized_tx.clone());
            pool.total_fees += prioritized_tx.transaction.fee;
            pool.last_updated = prioritized_tx.timestamp;

            // Update pool stats
            pool.stats.total_transactions += 1;
            *pool
                .stats
                .priority_distribution
                .entry(priority.clone())
                .or_insert(0) += 1;

            // Update global stats
            let mut global_stats = self.global_stats.write().await;
            global_stats.total_transactions += 1;
            global_stats.total_fees_collected += prioritized_tx.transaction.fee;
            global_stats.average_fee =
                global_stats.total_fees_collected as f64 / global_stats.total_transactions as f64;

            debug!(
                proposer_id = %proposer_id,
                tx_hash = %hex::encode(&prioritized_tx.transaction.hash()),
                priority = ?priority,
                "Added transaction to proposer pool"
            );
        } else {
            warn!(
                proposer_id = %proposer_id,
                pool_size = pool.transactions.len(),
                max_size = self.config.max_pool_size,
                "Proposer pool full, dropping transaction"
            );
        }

        Ok(())
    }

    /// Get transactions for proposer
    pub async fn get_transactions(
        &self,
        proposer_id: &str,
        max_count: usize,
    ) -> Result<Vec<Transaction>> {
        let pools = self.pools.read().await;

        if let Some(pool) = pools.get(proposer_id) {
            let transactions: Vec<Transaction> = pool
                .transactions
                .iter()
                .take(max_count)
                .map(|pt| pt.transaction.clone())
                .collect();

            // Update stats
            let mut global_stats = self.global_stats.write().await;
            global_stats.pool_hits += 1;

            debug!(
                proposer_id = %proposer_id,
                requested = max_count,
                returned = transactions.len(),
                "Retrieved transactions from proposer pool"
            );

            Ok(transactions)
        } else {
            let mut global_stats = self.global_stats.write().await;
            global_stats.pool_misses += 1;

            warn!(proposer_id = %proposer_id, "Proposer pool not found");
            Ok(vec![])
        }
    }

    /// Get prioritized transactions for proposer
    pub async fn get_prioritized_transactions(
        &self,
        proposer_id: &str,
        max_count: usize,
    ) -> Result<Vec<PrioritizedTransaction>> {
        let mut pools = self.pools.write().await;

        if let Some(pool) = pools.get_mut(proposer_id) {
            if self.config.enable_priority_sorting {
                // Sort by priority (descending)
                let mut sorted_txs: Vec<_> = pool.transactions.iter().collect();
                sorted_txs.sort_by(|a, b| {
                    // First by priority
                    b.priority.cmp(&a.priority)
                    // Then by fee if prioritization enabled
                    .then_with(|| {
                        if self.config.enable_fee_prioritization {
                            b.transaction.fee.cmp(&a.transaction.fee)
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    })
                });

                let selected_txs: Vec<PrioritizedTransaction> = sorted_txs
                    .into_iter()
                    .take(max_count)
                    .map(|pt| (*pt).clone())
                    .collect();

                // Update stats
                pool.stats.transactions_processed += selected_txs.len() as u64;

                let mut global_stats = self.global_stats.write().await;
                global_stats.pool_hits += 1;

                Ok(selected_txs)
            } else {
                // Return in FIFO order
                let transactions: Vec<PrioritizedTransaction> = pool
                    .transactions
                    .iter()
                    .take(max_count)
                    .map(|pt| pt.clone())
                    .collect();

                pool.stats.transactions_processed += transactions.len() as u64;

                let mut global_stats = self.global_stats.write().await;
                global_stats.pool_hits += 1;

                Ok(transactions)
            }
        } else {
            let mut global_stats = self.global_stats.write().await;
            global_stats.pool_misses += 1;

            warn!(proposer_id = %proposer_id, "Proposer pool not found");
            Ok(vec![])
        }
    }

    /// Remove processed transactions
    pub async fn remove_processed_transactions(
        &self,
        proposer_id: &str,
        transactions: &[Transaction],
    ) -> Result<()> {
        let tx_hashes: Vec<[u8; 32]> = transactions.iter().map(|tx| tx.hash()).collect();

        let mut pools = self.pools.write().await;
        if let Some(pool) = pools.get_mut(proposer_id) {
            let original_len = pool.transactions.len();
            pool.transactions
                .retain(|pt| !tx_hashes.contains(&pt.transaction.hash()));

            // Recalculate total fees
            pool.total_fees = pool.transactions.iter().map(|pt| pt.transaction.fee).sum();

            let removed_count = original_len - pool.transactions.len();
            if removed_count > 0 {
                info!(
                    proposer_id = %proposer_id,
                    removed_count = removed_count,
                    "Removed processed transactions from proposer pool"
                );
            }
        }

        Ok(())
    }

    /// Get pool statistics for proposer
    pub async fn get_pool_stats(&self, proposer_id: &str) -> Option<ProposerPoolStats> {
        let pools = self.pools.read().await;
        pools.get(proposer_id).map(|pool| pool.stats.clone())
    }

    /// Get global statistics
    pub async fn get_global_stats(&self) -> ProposerPoolStats {
        let stats = self.global_stats.read().await;
        stats.clone()
    }

    /// Get all proposer pool sizes
    pub async fn get_pool_sizes(&self) -> HashMap<String, usize> {
        let pools = self.pools.read().await;
        pools
            .iter()
            .map(|(id, pool)| (id.clone(), pool.transactions.len()))
            .collect()
    }

    /// Cleanup old transactions
    pub async fn cleanup_old_transactions(&self) -> Result<usize> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut total_removed = 0;

        let mut pools = self.pools.write().await;
        for (proposer_id, pool) in pools.iter_mut() {
            let original_len = pool.transactions.len();
            pool.transactions
                .retain(|pt| (current_time - pt.timestamp) < self.config.transaction_timeout_secs);

            let removed = original_len - pool.transactions.len();
            if removed > 0 {
                debug!(
                    proposer_id = %proposer_id,
                    removed_count = removed,
                    "Cleaned up old transactions from proposer pool"
                );
                total_removed += removed;
            }

            // Recalculate total fees
            pool.total_fees = pool.transactions.iter().map(|pt| pt.transaction.fee).sum();
        }

        if total_removed > 0 {
            info!(
                total_removed = total_removed,
                "Cleaned up old transactions from proposer pools"
            );
        }

        Ok(total_removed)
    }

    /// Remove empty pools
    pub async fn remove_empty_pools(&self) -> Result<usize> {
        let mut pools = self.pools.write().await;
        let original_count = pools.len();

        pools.retain(|_, pool| !pool.transactions.is_empty());

        let removed = original_count - pools.len();
        if removed > 0 {
            info!(removed_pools = removed, "Removed empty proposer pools");
        }

        Ok(removed)
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting proposer pool manager");

        // Start cleanup task
        let manager = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                manager.config.cleanup_interval_secs,
            ));

            loop {
                interval.tick().await;

                if let Err(e) = manager.cleanup_old_transactions().await {
                    error!("Failed to cleanup old transactions: {}", e);
                }

                if let Err(e) = manager.remove_empty_pools().await {
                    error!("Failed to remove empty pools: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Stop the pool manager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping proposer pool manager");
        Ok(())
    }
}

impl Clone for ProposerPoolManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            pools: self.pools.clone(),
            global_stats: self.global_stats.clone(),
        }
    }
}
