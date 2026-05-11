//! Replay Prevention System for Savitri Network
//!
//! This module implements comprehensive replay prevention mechanisms to prevent
//! double-spending attacks and ensure transaction uniqueness across the blockchain.
//! It provides both nonce-based and hash-based replay detection with configurable
//! retention policies and performance optimizations.

use lru::LruCache;
use savitri_storage::{Storage, StorageTrait};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Replay prevention configuration
#[derive(Debug, Clone)]
pub struct ReplayPreventionConfig {
    /// Maximum number of transaction hashes to cache
    pub max_hash_cache_size: usize,
    /// Maximum number of account nonces to track
    pub max_nonce_cache_size: usize,
    /// Time-to-live for cached entries (in blocks)
    pub cache_ttl_blocks: u64,
    /// Cleanup interval (in blocks)
    pub cleanup_interval_blocks: u64,
}

impl Default for ReplayPreventionConfig {
    fn default() -> Self {
        Self {
            max_hash_cache_size: 100_000, // Cache up to 100K transaction hashes
            max_nonce_cache_size: 50_000, // Track up to 50K account nonces
            cache_ttl_blocks: 1000,       // Keep entries for 1000 blocks
            cleanup_interval_blocks: 100, // Cleanup every 100 blocks
        }
    }
}

/// Replay prevention entry for transaction hash
#[derive(Debug, Clone)]
struct ReplayEntry {
    /// Block height when transaction was executed
    block_height: u64,
    /// Timestamp when entry was created
    created_at: Instant,
    /// Transaction sender address
    sender_address: Vec<u8>,
    /// Transaction nonce
    nonce: u64,
}

/// Account nonce tracking entry
#[derive(Debug, Clone)]
struct NonceEntry {
    /// Current nonce for the account
    current_nonce: u64,
    /// Last updated block height
    last_updated: u64,
    /// Timestamp when entry was created
    created_at: Instant,
}

/// Comprehensive replay prevention system
pub struct ReplayPrevention {
    /// Configuration
    config: ReplayPreventionConfig,
    /// Transaction hash cache for replay detection
    hash_cache: Arc<RwLock<LruCache<[u8; 32], ReplayEntry>>>,
    /// Account nonce tracking
    nonce_cache: Arc<RwLock<HashMap<Vec<u8>, NonceEntry>>>,
    storage: Arc<Storage>,
    /// Current block height
    current_block_height: Arc<RwLock<u64>>,
    /// Last cleanup block height
    last_cleanup_height: Arc<RwLock<u64>>,
}

impl ReplayPrevention {
    /// Create new replay prevention system
    pub fn new(config: ReplayPreventionConfig, storage: Arc<Storage>) -> Self {
        Self {
            hash_cache: Arc::new(RwLock::new(LruCache::new(
                NonZeroUsize::new(config.max_hash_cache_size).unwrap(),
            ))),
            nonce_cache: Arc::new(RwLock::new(HashMap::new())),
            storage,
            current_block_height: Arc::new(RwLock::new(0)),
            last_cleanup_height: Arc::new(RwLock::new(0)),
            config,
        }
    }

    /// Create replay prevention with default configuration
    pub fn with_storage(storage: Arc<Storage>) -> Self {
        Self::new(ReplayPreventionConfig::default(), storage)
    }

    /// Update block height and trigger cleanup if needed
    pub fn update_block_height(&self, new_height: u64) {
        let mut current_height = self.current_block_height.write().unwrap();
        *current_height = new_height;

        // Check if cleanup is needed
        let mut last_cleanup = self.last_cleanup_height.write().unwrap();
        if new_height >= *last_cleanup + self.config.cleanup_interval_blocks {
            *last_cleanup = new_height;
            drop(last_cleanup);
            self.cleanup_expired_entries(new_height);
        }
    }

    /// Check if transaction is a replay (already executed)
    pub fn is_replay_transaction(
        &self,
        tx_hash: &[u8; 32],
        sender_address: &[u8],
        nonce: u64,
    ) -> bool {
        // Check hash-based replay
        if self.hash_cache.read().unwrap().contains(tx_hash) {
            // If we have the same hash, it's a replay
            return true;
        }

        // Check nonce-based replay
        if let Some(nonce_entry) = self.nonce_cache.read().unwrap().get(sender_address) {
            if nonce < nonce_entry.current_nonce {
                // Nonce too old, it's a replay
                return true;
            }
        }

        false
    }

    /// Record transaction execution to prevent future replays
    pub fn record_transaction_execution(
        &self,
        tx_hash: [u8; 32],
        sender_address: Vec<u8>,
        nonce: u64,
        block_height: u64,
    ) -> Result<(), ReplayPreventionError> {
        // Update current block height
        {
            let mut current_height = self.current_block_height.write().unwrap();
            if block_height > *current_height {
                *current_height = block_height;
            }
        }

        // Record in hash cache
        let entry = ReplayEntry {
            block_height,
            created_at: Instant::now(),
            sender_address: sender_address.clone(),
            nonce,
        };

        self.hash_cache.write().unwrap().put(tx_hash, entry);

        // Update nonce cache
        let current_height = *self.current_block_height.read().unwrap();
        let mut nonce_cache = self.nonce_cache.write().unwrap();
        let nonce_entry = nonce_cache
            .entry(sender_address.clone())
            .or_insert_with(|| NonceEntry {
                current_nonce: 0,
                last_updated: current_height,
                created_at: Instant::now(),
            });

        // Update nonce if this transaction has a higher nonce
        if nonce > nonce_entry.current_nonce {
            nonce_entry.current_nonce = nonce;
            nonce_entry.last_updated = current_height;
        }

        Ok(())
    }

    /// Validate transaction nonce against account state
    pub fn validate_transaction_nonce(
        &self,
        sender_address: &[u8],
        tx_nonce: u64,
    ) -> Result<(), ReplayPreventionError> {
        // Get account from storage
        let account_bytes = self
            .storage
            .get_account(sender_address)
            .map_err(|_| ReplayPreventionError::StorageError)?;

        let account: savitri_core::Account = if let Some(bytes) = account_bytes {
            savitri_core::Account::decode(&bytes)
                .map_err(|_| ReplayPreventionError::StorageError)?
        } else {
            // Account doesn't exist - only allow nonce 0 for new accounts
            if tx_nonce != 0 {
                return Err(ReplayPreventionError::NonceTooFar {
                    account_nonce: 0,
                    tx_nonce,
                });
            }
            return Ok(());
        };

        // Check if nonce is valid
        if tx_nonce < account.nonce {
            return Err(ReplayPreventionError::NonceTooOld {
                account_nonce: account.nonce,
                tx_nonce,
            });
        }

        // Check if nonce is too far ahead.
        // We allow a gap up to the admission MAX_MAIN_POOL_NONCE_GAP (3000).
        // Only reject obviously abusive gaps here (> 4000).
        if tx_nonce > account.nonce + 4000 {
            return Err(ReplayPreventionError::NonceTooFar {
                account_nonce: account.nonce,
                tx_nonce,
            });
        }

        Ok(())
    }

    /// Get current nonce for an account
    pub fn get_account_nonce(&self, sender_address: &[u8]) -> Result<u64, ReplayPreventionError> {
        // Try cache first
        if let Some(entry) = self.nonce_cache.read().unwrap().get(sender_address) {
            return Ok(entry.current_nonce);
        }

        // Fallback to storage
        let account_bytes = self
            .storage
            .get_account(sender_address)
            .map_err(|_| ReplayPreventionError::StorageError)?;

        let account: savitri_core::Account = if let Some(bytes) = account_bytes {
            savitri_core::Account::decode(&bytes)
                .map_err(|_| ReplayPreventionError::StorageError)?
        } else {
            // Account doesn't exist - return 0 as default nonce
            return Ok(0);
        };

        Ok(account.nonce)
    }

    /// Clean up expired entries based on TTL
    fn cleanup_expired_entries(&self, current_height: u64) {
        let cutoff_height = if current_height > self.config.cache_ttl_blocks {
            current_height - self.config.cache_ttl_blocks
        } else {
            0
        };

        // Clean up hash cache: LruCache handles eviction by size automatically,
        // but we should also trim entries older than the TTL.
        // LruCache's peek_lru() gives the least-recently-used entry.
        // We pop from the back (oldest) while entries are expired.
        {
            let mut hash_cache = self.hash_cache.write().unwrap();
            // Pop LRU entries that are older than the cutoff
            while let Some((_key, entry)) = hash_cache.peek_lru() {
                if entry.block_height < cutoff_height {
                    hash_cache.pop_lru();
                } else {
                    break; // LRU is sorted by access time; if oldest is fresh, stop
                }
            }
        }

        // Clean up nonce cache
        {
            let mut nonce_cache = self.nonce_cache.write().unwrap();
            let mut addresses_to_remove = Vec::new();

            for (address, entry) in nonce_cache.iter() {
                if entry.last_updated < cutoff_height {
                    addresses_to_remove.push(address.clone());
                }
            }

            for address in addresses_to_remove {
                nonce_cache.remove(&address);
            }
        }
    }

    /// Get replay prevention statistics
    pub fn get_stats(&self) -> ReplayPreventionStats {
        let hash_cache_size = self.hash_cache.read().unwrap().len();
        let nonce_cache_size = self.nonce_cache.read().unwrap().len();
        let current_height = *self.current_block_height.read().unwrap();
        let last_cleanup = *self.last_cleanup_height.read().unwrap();

        ReplayPreventionStats {
            hash_cache_size,
            nonce_cache_size,
            current_block_height: current_height,
            last_cleanup_height: last_cleanup,
            config: self.config.clone(),
        }
    }

    /// Clear all caches (for testing)
    pub fn clear_caches(&self) {
        self.hash_cache.write().unwrap().clear();
        self.nonce_cache.write().unwrap().clear();
    }
}

/// Replay prevention statistics
#[derive(Debug, Clone)]
pub struct ReplayPreventionStats {
    /// Number of cached transaction hashes
    pub hash_cache_size: usize,
    /// Number of cached account nonces
    pub nonce_cache_size: usize,
    /// Current block height
    pub current_block_height: u64,
    /// Last cleanup block height
    pub last_cleanup_height: u64,
    /// Configuration
    pub config: ReplayPreventionConfig,
}

/// Replay prevention error types
#[derive(Debug, thiserror::Error)]
pub enum ReplayPreventionError {
    /// Transaction hash already seen (replay attack)
    #[error("Transaction hash {hash:?} already executed at block {block_height}")]
    TransactionReplay { hash: [u8; 32], block_height: u64 },

    /// Nonce is too old (already used)
    #[error("Nonce {tx_nonce} is too old for account nonce {account_nonce}")]
    NonceTooOld { account_nonce: u64, tx_nonce: u64 },

    /// Nonce is too far ahead (gap prevention)
    #[error("Nonce {tx_nonce} is too far ahead for account nonce {account_nonce}")]
    NonceTooFar { account_nonce: u64, tx_nonce: u64 },

    /// Storage access error
    #[error("Storage access failed")]
    StorageError,

    /// Cache access error
    #[error("Cache access failed")]
    CacheError,
}

