//! Queued Pool: Holds transactions with future nonces (nonce gaps)
//!
//! When a transaction arrives with nonce > account.nonce + 1, instead of
//! rejecting it outright, we park it in the queued pool. When the missing
//! nonce transactions arrive and are executed, we promote queued transactions
//! to the main mempool.
//!
//! This is the standard approach used by Ethereum, Polygon, Avalanche, etc.
//!
//! ## Security Limits
//! - Max queued transactions per account: configurable (default: 64)
//! - Max total queued transactions: configurable (default: 4096)
//! - Max nonce gap: configurable (default: 16) — how far ahead we accept
//! - TTL: queued transactions expire after a timeout (default: 300s)

use crate::mempool::nonce_limits::QUEUED_POOL_MAX_NONCE_GAP;
use crate::mempool::types::{MempoolTx, PrevalidatedTx, SenderId, TxClass, TxHandle};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Configuration for the queued pool
#[derive(Debug, Clone)]
pub struct QueuedPoolConfig {
    /// Maximum transactions per account in the queued pool
    pub max_per_account: usize,
    /// Maximum total transactions across all accounts
    pub max_total: usize,
    /// Maximum nonce gap allowed (how far ahead of account.nonce we accept)
    /// e.g., if account.nonce=5 and max_nonce_gap=16, we accept nonce up to 21
    pub max_nonce_gap: u64,
    /// Time-to-live for queued transactions
    pub ttl: Duration,
    /// Cleanup interval
    pub cleanup_interval: Duration,
}

impl Default for QueuedPoolConfig {
    fn default() -> Self {
        Self {
            max_per_account: 3000,
            max_total: 30000,
            // const_assert! enforces queued >= admission gap.
            max_nonce_gap: QUEUED_POOL_MAX_NONCE_GAP,
            ttl: Duration::from_secs(300), // 5 minutes
            cleanup_interval: Duration::from_secs(30),
        }
    }
}

/// A queued transaction waiting for nonce gap to be filled
#[derive(Debug, Clone)]
pub struct QueuedTx {
    pub pv: PrevalidatedTx,
    /// Transaction hash (if available)
    pub tx_hash: Option<[u8; 32]>,
    /// When this transaction was queued
    pub queued_at: Instant,
    /// The account nonce at the time of queueing
    pub account_nonce_at_queue: u64,
}

/// Per-account queue: sorted by nonce using BTreeMap for efficient promotion
#[derive(Debug)]
struct AccountQueue {
    /// Transactions sorted by nonce
    txs: BTreeMap<u64, QueuedTx>,
    /// Sender address (for logging)
    sender_address: [u8; 32],
}

impl AccountQueue {
    fn new(sender_address: [u8; 32]) -> Self {
        Self {
            txs: BTreeMap::new(),
            sender_address,
        }
    }

    fn len(&self) -> usize {
        self.txs.len()
    }

    fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }
}

/// Queued pool statistics
#[derive(Debug, Clone, Default)]
pub struct QueuedPoolStats {
    pub total_queued: usize,
    pub accounts_with_queued: usize,
    pub total_promoted: u64,
    pub total_expired: u64,
    pub total_rejected_full: u64,
    pub total_rejected_gap_too_large: u64,
}

/// The queued pool holds transactions with future nonces
pub struct QueuedPool {
    config: QueuedPoolConfig,
    /// Per-account queues indexed by sender_id
    accounts: HashMap<SenderId, AccountQueue>,
    /// Total count of queued transactions
    total: usize,
    /// Last cleanup timestamp
    last_cleanup: Instant,
    /// Statistics
    stats: QueuedPoolStats,
}

impl QueuedPool {
    pub fn new(config: QueuedPoolConfig) -> Self {
        Self {
            config,
            accounts: HashMap::new(),
            total: 0,
            last_cleanup: Instant::now(),
            stats: QueuedPoolStats::default(),
        }
    }

    /// Try to queue a transaction with a future nonce.
    ///
    /// Returns Ok(()) if queued, Err(reason) if rejected.
    ///
    /// # Arguments
    /// * `tx_hash` - Optional transaction hash
    /// * `account_nonce` - Current account nonce from storage
    pub fn try_queue(
        &mut self,
        pv: PrevalidatedTx,
        tx_hash: Option<[u8; 32]>,
        account_nonce: u64,
    ) -> Result<(), QueuedPoolError> {
        // Check nonce gap limit
        let gap = pv.nonce.saturating_sub(account_nonce);
        if gap > self.config.max_nonce_gap {
            self.stats.total_rejected_gap_too_large += 1;
            return Err(QueuedPoolError::NonceGapTooLarge {
                nonce: pv.nonce,
                account_nonce,
                max_gap: self.config.max_nonce_gap,
            });
        }

        // Check global capacity
        if self.total >= self.config.max_total {
            self.stats.total_rejected_full += 1;
            return Err(QueuedPoolError::PoolFull {
                total: self.total,
                max: self.config.max_total,
            });
        }

        // Get or create account queue
        let account_queue = self
            .accounts
            .entry(pv.sender_id)
            .or_insert_with(|| AccountQueue::new(pv.sender_address));

        // Check per-account capacity
        if account_queue.len() >= self.config.max_per_account {
            self.stats.total_rejected_full += 1;
            return Err(QueuedPoolError::AccountQueueFull {
                sender_id: pv.sender_id,
                count: account_queue.len(),
                max: self.config.max_per_account,
            });
        }

        // Check for duplicate nonce in queue
        if account_queue.txs.contains_key(&pv.nonce) {
            // Replace if higher fee (standard behavior)
            let existing = account_queue.txs.get(&pv.nonce).unwrap();
            if pv.max_fee > existing.pv.max_fee {
                tracing::info!(
                    sender_id = pv.sender_id,
                    nonce = pv.nonce,
                    old_fee = existing.pv.max_fee,
                    new_fee = pv.max_fee,
                    "Queued pool: replacing transaction with higher fee"
                );
                // Replace (total count stays the same)
                account_queue.txs.insert(
                    pv.nonce,
                    QueuedTx {
                        pv,
                        tx_hash,
                        queued_at: Instant::now(),
                        account_nonce_at_queue: account_nonce,
                    },
                );
                return Ok(());
            } else {
                return Err(QueuedPoolError::DuplicateNonce {
                    nonce: pv.nonce,
                    sender_id: pv.sender_id,
                });
            }
        }

        tracing::info!(
            sender_id = pv.sender_id,
            nonce = pv.nonce,
            account_nonce = account_nonce,
            gap = gap,
            "Transaction queued for future nonce"
        );

        account_queue.txs.insert(
            pv.nonce,
            QueuedTx {
                pv,
                tx_hash,
                queued_at: Instant::now(),
                account_nonce_at_queue: account_nonce,
            },
        );
        self.total += 1;
        self.stats.total_queued = self.total;
        self.stats.accounts_with_queued = self.accounts.len();

        self.export_metrics();
        Ok(())
    }

    /// Promote transactions that are now ready (nonce matches account nonce).
    ///
    /// Called after a block is committed or when account nonce advances.
    /// Returns transactions ready to be admitted to the main mempool, sorted by nonce.
    ///
    /// # Arguments
    /// * `sender_id` - The sender whose nonce advanced
    /// * `new_account_nonce` - The new account nonce after execution
    pub fn promote(
        &mut self,
        sender_id: SenderId,
        new_account_nonce: u64,
    ) -> Vec<(PrevalidatedTx, Option<[u8; 32]>)> {
        let mut promoted = Vec::new();

        let account_queue = match self.accounts.get_mut(&sender_id) {
            Some(q) => q,
            None => return promoted,
        };

        // Collect all nonces that are now ready or stale.
        // Promote ALL consecutive nonces starting from new_account_nonce,
        // not just 2. This ensures that when a block commits and the account
        // nonce advances, ALL queued txs with consecutive nonces get promoted
        // to the main pool in one shot (e.g., nonces 4,5,6,7,8 all promote
        // when new_account_nonce=4).
        let mut to_remove = Vec::new();
        let mut next_expected = new_account_nonce;
        for (&nonce, queued_tx) in account_queue.txs.iter() {
            if nonce < new_account_nonce {
                // Stale: nonce already used, remove
                to_remove.push(nonce);
            } else if nonce == next_expected {
                // Ready: promote to main mempool (consecutive nonce)
                to_remove.push(nonce);
                promoted.push((queued_tx.pv.clone(), queued_tx.tx_hash));
                next_expected += 1;
            } else {
                // Gap found, stop promoting
                break; // BTreeMap is sorted, no point checking further
            }
        }

        for nonce in &to_remove {
            account_queue.txs.remove(nonce);
            self.total = self.total.saturating_sub(1);
        }

        if account_queue.is_empty() {
            self.accounts.remove(&sender_id);
        }

        if !promoted.is_empty() {
            self.stats.total_promoted += promoted.len() as u64;
            tracing::info!(
                sender_id = sender_id,
                promoted_count = promoted.len(),
                new_account_nonce = new_account_nonce,
                remaining_queued = self.total,
                "Promoted queued transactions to mempool"
            );
        }

        self.stats.total_queued = self.total;
        self.stats.accounts_with_queued = self.accounts.len();
        self.export_metrics();

        // Sort by nonce for correct ordering
        promoted.sort_by_key(|(pv, _)| pv.nonce);
        promoted
    }

    /// Promote all accounts after a block commit.
    /// Takes a map of sender_id -> new_account_nonce for all accounts that changed.
    pub fn promote_batch(
        &mut self,
        nonce_updates: &HashMap<SenderId, u64>,
    ) -> Vec<(PrevalidatedTx, Option<[u8; 32]>)> {
        let mut all_promoted = Vec::new();
        for (&sender_id, &new_nonce) in nonce_updates {
            let promoted = self.promote(sender_id, new_nonce);
            all_promoted.extend(promoted);
        }
        all_promoted
    }

    /// Periodic cleanup: remove expired queued transactions
    pub fn cleanup_expired(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_cleanup) < self.config.cleanup_interval {
            return;
        }
        self.last_cleanup = now;

        let ttl = self.config.ttl;
        let mut empty_accounts = Vec::new();

        for (&sender_id, account_queue) in self.accounts.iter_mut() {
            let before = account_queue.len();
            account_queue
                .txs
                .retain(|_, queued_tx| now.duration_since(queued_tx.queued_at) < ttl);
            let removed = before - account_queue.len();
            self.total = self.total.saturating_sub(removed);
            self.stats.total_expired += removed as u64;

            if account_queue.is_empty() {
                empty_accounts.push(sender_id);
            }
        }

        for sender_id in empty_accounts {
            self.accounts.remove(&sender_id);
        }

        self.stats.total_queued = self.total;
        self.stats.accounts_with_queued = self.accounts.len();
        self.export_metrics();
    }

    /// Get statistics
    pub fn get_stats(&self) -> QueuedPoolStats {
        self.stats.clone()
    }

    /// Get total queued count
    pub fn len(&self) -> usize {
        self.total
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Export metrics to Prometheus
    fn export_metrics(&self) {
        metrics::gauge!("mempool_queued_pool_total").set(self.total as f64);
        metrics::gauge!("mempool_queued_pool_accounts").set(self.accounts.len() as f64);
        metrics::gauge!("mempool_queued_pool_promoted_total").set(self.stats.total_promoted as f64);
        metrics::gauge!("mempool_queued_pool_expired_total").set(self.stats.total_expired as f64);
    }
}

/// Errors from queued pool operations
#[derive(Debug, thiserror::Error)]
pub enum QueuedPoolError {
    #[error(
        "Nonce gap too large: tx nonce {nonce}, account nonce {account_nonce}, max gap {max_gap}"
    )]
    NonceGapTooLarge {
        nonce: u64,
        account_nonce: u64,
        max_gap: u64,
    },

    #[error("Queued pool full: {total}/{max} transactions")]
    PoolFull { total: usize, max: usize },

    #[error("Account queue full for sender {sender_id}: {count}/{max}")]
    AccountQueueFull {
        sender_id: SenderId,
        count: usize,
        max: usize,
    },

    #[error("Duplicate nonce {nonce} for sender {sender_id} in queued pool")]
    DuplicateNonce { nonce: u64, sender_id: SenderId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::types::TxHandle;

    fn make_pv(sender_id: SenderId, nonce: u64, fee: u64) -> PrevalidatedTx {
        PrevalidatedTx {
            sender_id,
            sender_address: [sender_id as u8; 32],
            nonce,
            max_fee: fee,
            amount: 0,
            tx_handle: TxHandle(nonce),
            class: TxClass::Financial,
            stream_nonce: None,
        }
    }

    #[test]
    fn test_basic_queue_and_promote() {
        let config = QueuedPoolConfig::default();
        let mut pool = QueuedPool::new(config);

        let sender: SenderId = 1;
        let account_nonce = 5;

        // Queue nonce 7 and 8 (gap: nonce 6 is missing)
        pool.try_queue(make_pv(sender, 7, 1000), None, account_nonce)
            .unwrap();
        pool.try_queue(make_pv(sender, 8, 1000), None, account_nonce)
            .unwrap();

        assert_eq!(pool.len(), 2);

        // Account nonce advances to 6 (nonce 5 was executed)
        let promoted = pool.promote(sender, 6);
        // Nothing promoted yet: nonce 6 is not in queue, nonce 7 > 6+1
        assert_eq!(promoted.len(), 0);

        // Account nonce advances to 7 (nonce 6 was executed)
        let promoted = pool.promote(sender, 7);
        // Now nonce 7 (== account_nonce) and 8 (== account_nonce+1) should promote
        assert_eq!(promoted.len(), 2);
        assert_eq!(promoted[0].0.nonce, 7);
        assert_eq!(promoted[1].0.nonce, 8);

        assert!(pool.is_empty());
    }

    #[test]
    fn test_nonce_gap_too_large() {
        let mut config = QueuedPoolConfig::default();
        config.max_nonce_gap = 10;
        let mut pool = QueuedPool::new(config);

        let sender: SenderId = 1;
        let account_nonce = 5;

        // Nonce 16 has gap of 11, which exceeds max_nonce_gap=10
        let result = pool.try_queue(make_pv(sender, 16, 1000), None, account_nonce);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueuedPoolError::NonceGapTooLarge { .. }
        ));

        // Nonce 15 has gap of 10, which is exactly at the limit
        let result = pool.try_queue(make_pv(sender, 15, 1000), None, account_nonce);
        assert!(result.is_ok());
    }

    #[test]
    fn test_replace_with_higher_fee() {
        let config = QueuedPoolConfig::default();
        let mut pool = QueuedPool::new(config);

        let sender: SenderId = 1;
        let account_nonce = 5;

        pool.try_queue(make_pv(sender, 7, 1000), None, account_nonce)
            .unwrap();

        // Same nonce, lower fee -> rejected
        let result = pool.try_queue(make_pv(sender, 7, 500), None, account_nonce);
        assert!(result.is_err());

        // Same nonce, higher fee -> replaces
        pool.try_queue(make_pv(sender, 7, 2000), None, account_nonce)
            .unwrap();
        assert_eq!(pool.len(), 1);

        let promoted = pool.promote(sender, 7);
        assert_eq!(promoted[0].0.max_fee, 2000);
    }

    #[test]
    fn test_pool_capacity_limits() {
        let mut config = QueuedPoolConfig::default();
        config.max_per_account = 3;
        config.max_total = 5;
        let mut pool = QueuedPool::new(config);

        let sender: SenderId = 1;
        let account_nonce = 0;

        // Fill account limit
        pool.try_queue(make_pv(sender, 2, 1000), None, account_nonce)
            .unwrap();
        pool.try_queue(make_pv(sender, 3, 1000), None, account_nonce)
            .unwrap();
        pool.try_queue(make_pv(sender, 4, 1000), None, account_nonce)
            .unwrap();

        // 4th from same account -> rejected
        let result = pool.try_queue(make_pv(sender, 5, 1000), None, account_nonce);
        assert!(result.is_err());
    }

    #[test]
    fn test_stale_nonce_removal_on_promote() {
        let config = QueuedPoolConfig::default();
        let mut pool = QueuedPool::new(config);

        let sender: SenderId = 1;

        // Queue nonces 3, 5, 7
        pool.try_queue(make_pv(sender, 3, 1000), None, 0).unwrap();
        pool.try_queue(make_pv(sender, 5, 1000), None, 0).unwrap();
        pool.try_queue(make_pv(sender, 7, 1000), None, 0).unwrap();

        // Account nonce jumps to 6 (e.g., block included nonces 0-5)
        let promoted = pool.promote(sender, 6);
        // Nonces 3 and 5 are stale (< 6), removed
        // Nonce 7 is account_nonce+1 = 7, promoted
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].0.nonce, 7);
        assert!(pool.is_empty());
    }
}
