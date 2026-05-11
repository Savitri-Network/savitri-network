//! Mempool Management Module
//!
//! This module provides automatic mempool cleanup and synchronization
//! for the anti-double spending system.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Transaction entry in the mempool
#[derive(Debug, Clone)]
pub struct MempoolEntry {
    pub tx_hash: [u8; 32],
    pub sender: [u8; 32],
    pub receiver: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    pub signature: [u8; 64],
    pub added_at: u64,
    pub priority: u32,
    pub gas_price: u64,
}

/// Mempool status for a transaction
#[derive(Debug, Clone, PartialEq)]
pub enum MempoolStatus {
    Pending,
    Confirmed,
    Rejected,
    Expired,
}

/// Mempool manager for automatic cleanup and synchronization
#[derive(Debug)]
pub struct MempoolManager {
    /// Pending transactions by hash
    pending_txs: HashMap<[u8; 32], MempoolEntry>,
    /// Confirmed transaction hashes (for duplicate detection)
    confirmed_hashes: HashSet<[u8; 32]>,
    /// Rejected transaction hashes
    rejected_hashes: HashSet<[u8; 32]>,
    /// Transaction order queue (FIFO)
    tx_queue: VecDeque<[u8; 32]>,
    /// Configuration
    config: MempoolConfig,
    /// Statistics
    stats: MempoolStats,
}

#[derive(Debug, Clone)]
pub struct MempoolConfig {
    /// Maximum number of pending transactions
    pub max_pending_txs: usize,
    /// Maximum transaction age in seconds
    pub max_tx_age_seconds: u64,
    /// Cleanup interval in seconds
    pub cleanup_interval_seconds: u64,
    /// Maximum confirmed hashes to keep
    pub max_confirmed_cache: usize,
    /// Maximum rejected hashes to keep
    pub max_rejected_cache: usize,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            max_pending_txs: 10_000,
            max_tx_age_seconds: 3600,     // 1 hour
            cleanup_interval_seconds: 60, // 1 minute
            max_confirmed_cache: 100_000,
            max_rejected_cache: 10_000,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MempoolStats {
    pub total_added: u64,
    pub total_confirmed: u64,
    pub total_rejected: u64,
    pub total_expired: u64,
    pub total_evicted: u64,
    pub last_cleanup: u64,
}

impl MempoolManager {
    pub fn new() -> Self {
        Self::with_config(MempoolConfig::default())
    }

    pub fn with_config(config: MempoolConfig) -> Self {
        Self {
            pending_txs: HashMap::new(),
            confirmed_hashes: HashSet::new(),
            rejected_hashes: HashSet::new(),
            tx_queue: VecDeque::new(),
            config,
            stats: MempoolStats::default(),
        }
    }

    /// Add a transaction to the mempool
    pub fn add_transaction(&mut self, entry: MempoolEntry) -> Result<bool, MempoolError> {
        let tx_hash = entry.tx_hash;

        // Check if already confirmed
        if self.confirmed_hashes.contains(&tx_hash) {
            return Err(MempoolError::AlreadyConfirmed);
        }

        // Check if already rejected
        if self.rejected_hashes.contains(&tx_hash) {
            return Err(MempoolError::AlreadyRejected);
        }

        // Check if already pending
        if self.pending_txs.contains_key(&tx_hash) {
            return Ok(false); // Already in mempool
        }

        // Check capacity
        if self.pending_txs.len() >= self.config.max_pending_txs {
            // Evict oldest transaction
            if let Some(oldest_hash) = self.tx_queue.pop_front() {
                self.pending_txs.remove(&oldest_hash);
                self.stats.total_evicted += 1;
            }
        }

        // Add transaction
        self.pending_txs.insert(tx_hash, entry);
        self.tx_queue.push_back(tx_hash);
        self.stats.total_added += 1;

        debug!(
            tx_hash = %hex::encode(&tx_hash[..8]),
            pending_count = self.pending_txs.len(),
            "Transaction added to mempool"
        );

        Ok(true)
    }

    /// Mark transactions as confirmed (from a finalized block)
    pub fn confirm_transactions(&mut self, tx_hashes: &[[u8; 32]]) -> usize {
        let mut confirmed_count = 0;

        for tx_hash in tx_hashes {
            // Remove from pending
            if self.pending_txs.remove(tx_hash).is_some() {
                confirmed_count += 1;
            }

            // Remove from queue
            self.tx_queue.retain(|h| h != tx_hash);

            // Add to confirmed cache
            self.confirmed_hashes.insert(*tx_hash);
        }

        self.stats.total_confirmed += confirmed_count as u64;

        // Trim confirmed cache if needed
        self.trim_confirmed_cache();

        info!(
            confirmed = confirmed_count,
            pending_remaining = self.pending_txs.len(),
            "Transactions confirmed and removed from mempool"
        );

        confirmed_count
    }

    /// Mark transactions as rejected
    pub fn reject_transactions(&mut self, tx_hashes: &[[u8; 32]]) -> usize {
        let mut rejected_count = 0;

        for tx_hash in tx_hashes {
            // Remove from pending
            if self.pending_txs.remove(tx_hash).is_some() {
                rejected_count += 1;
            }

            // Remove from queue
            self.tx_queue.retain(|h| h != tx_hash);

            // Add to rejected cache
            self.rejected_hashes.insert(*tx_hash);
        }

        self.stats.total_rejected += rejected_count as u64;

        // Trim rejected cache if needed
        self.trim_rejected_cache();

        debug!(rejected = rejected_count, "Transactions rejected");

        rejected_count
    }

    /// Process mempool sync message from masternode
    pub fn process_mempool_sync(
        &mut self,
        confirmed_txs: &[[u8; 32]],
        rejected_txs: &[[u8; 32]],
    ) -> MempoolSyncResult {
        let confirmed_count = self.confirm_transactions(confirmed_txs);
        let rejected_count = self.reject_transactions(rejected_txs);

        MempoolSyncResult {
            confirmed_count,
            rejected_count,
            remaining_pending: self.pending_txs.len(),
        }
    }

    /// Automatic cleanup of expired transactions
    pub fn cleanup_expired(&mut self) -> usize {
        let now = current_timestamp();
        let cutoff = now - self.config.max_tx_age_seconds;
        let mut expired_count = 0;

        // Find expired transactions
        let expired_hashes: Vec<[u8; 32]> = self
            .pending_txs
            .iter()
            .filter(|(_, entry)| entry.added_at < cutoff)
            .map(|(hash, _)| *hash)
            .collect();

        // Remove expired transactions
        for tx_hash in &expired_hashes {
            self.pending_txs.remove(tx_hash);
            self.tx_queue.retain(|h| h != tx_hash);
            expired_count += 1;
        }

        self.stats.total_expired += expired_count as u64;
        self.stats.last_cleanup = now;

        if expired_count > 0 {
            info!(
                expired = expired_count,
                pending_remaining = self.pending_txs.len(),
                "Expired transactions cleaned up"
            );
        }

        expired_count
    }

    /// Get transactions for block production
    pub fn get_transactions_for_block(&self, max_count: usize) -> Vec<&MempoolEntry> {
        // Get transactions sorted by priority and gas price
        let mut txs: Vec<&MempoolEntry> = self.pending_txs.values().collect();
        txs.sort_by(|a, b| {
            // Sort by priority (desc) then gas price (desc)
            b.priority
                .cmp(&a.priority)
                .then(b.gas_price.cmp(&a.gas_price))
        });
        txs.truncate(max_count);
        txs
    }

    /// Check if a transaction is in the mempool
    pub fn contains(&self, tx_hash: &[u8; 32]) -> bool {
        self.pending_txs.contains_key(tx_hash)
    }

    /// Check transaction status
    pub fn get_status(&self, tx_hash: &[u8; 32]) -> MempoolStatus {
        if self.pending_txs.contains_key(tx_hash) {
            MempoolStatus::Pending
        } else if self.confirmed_hashes.contains(tx_hash) {
            MempoolStatus::Confirmed
        } else if self.rejected_hashes.contains(tx_hash) {
            MempoolStatus::Rejected
        } else {
            MempoolStatus::Expired
        }
    }

    /// Get mempool statistics
    pub fn get_stats(&self) -> &MempoolStats {
        &self.stats
    }

    /// Get pending transaction count
    pub fn pending_count(&self) -> usize {
        self.pending_txs.len()
    }

    /// Trim confirmed cache if it exceeds maximum
    fn trim_confirmed_cache(&mut self) {
        if self.confirmed_hashes.len() > self.config.max_confirmed_cache {
            // Remove oldest entries (simple approach - clear half)
            let to_remove = self.confirmed_hashes.len() - self.config.max_confirmed_cache / 2;
            let hashes_to_remove: Vec<_> = self
                .confirmed_hashes
                .iter()
                .take(to_remove)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                self.confirmed_hashes.remove(&hash);
            }
        }
    }

    /// Trim rejected cache if it exceeds maximum
    fn trim_rejected_cache(&mut self) {
        if self.rejected_hashes.len() > self.config.max_rejected_cache {
            // Remove oldest entries (simple approach - clear half)
            let to_remove = self.rejected_hashes.len() - self.config.max_rejected_cache / 2;
            let hashes_to_remove: Vec<_> = self
                .rejected_hashes
                .iter()
                .take(to_remove)
                .cloned()
                .collect();
            for hash in hashes_to_remove {
                self.rejected_hashes.remove(&hash);
            }
        }
    }
}

impl Default for MempoolManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct MempoolSyncResult {
    pub confirmed_count: usize,
    pub rejected_count: usize,
    pub remaining_pending: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MempoolError {
    AlreadyConfirmed,
    AlreadyRejected,
    MempoolFull,
    InvalidTransaction,
}

impl std::fmt::Display for MempoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MempoolError::AlreadyConfirmed => write!(f, "Transaction already confirmed"),
            MempoolError::AlreadyRejected => write!(f, "Transaction already rejected"),
            MempoolError::MempoolFull => write!(f, "Mempool is full"),
            MempoolError::InvalidTransaction => write!(f, "Invalid transaction"),
        }
    }
}

impl std::error::Error for MempoolError {}
