//! Storage module for Savitri Storage Layer
//!
//! Production-ready implementation with RocksDB backend,
//! thread-safe operations, and full transaction support.

// Submodules
pub mod bonds;
/// SECURITY (PT-I01): LRU caching layer for accounts and contracts.
/// Previously dead code — now wired up and available for integration.
pub mod cache;
pub mod contracts;
pub mod fl;
pub mod governance;
/// Merkle trie for state root computation with inclusion proofs.
pub mod merkle_trie;
pub mod oracle;
pub mod pruning;
pub mod treasury;
pub mod vote_tokens;
// Note: blocks, meta, vesting modules use legacy Storage<RocksDb> generic syntax
// and are not compatible with current Storage implementation
// Their functionality is already provided by Storage methods directly
// pub mod blocks;
// pub mod meta;
// pub mod vesting;

// Re-export commonly used types from submodules
pub use bonds::{CF_BONDS, DEFAULT_BOND_AMOUNT, MAX_BOND_AMOUNT, MIN_BOND_AMOUNT};
pub use fl::FlPolicy;
pub use governance::{Proposal, ProposalAction, ProposalStatus, VoteType};
pub use oracle::OracleRole;
pub use vote_tokens::VoteTokenManager;

// Vesting types (defined inline to avoid legacy module issues)
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

// ─── Multi-group composite-key helpers ─────────────────────────────────────
//
// Each (height, group_id) pair maps to a distinct key. Empty group_id falls
// back to the legacy format so existing pre-multigroup data stays readable.
// The logic lives here (instead of inlined at call sites) to ensure the SAME
// encoding is used on every read/write path.

/// CF_BLOCKS key. Legacy (group_id == "") → bare `height_le`. Multi-group →
/// `height_le || ':' || group_id_bytes`. 8-byte prefix + ':' + ascii group_id.
pub fn build_block_key(height: u64, group_id: &str) -> Vec<u8> {
    if group_id.is_empty() {
        height.to_le_bytes().to_vec()
    } else {
        let mut k = Vec::with_capacity(8 + 1 + group_id.len());
        k.extend_from_slice(&height.to_le_bytes());
        k.push(b':');
        k.extend_from_slice(group_id.as_bytes());
        k
    }
}

/// CF_METADATA key for chain head per group. Legacy (group_id == "") →
/// `b"chain_head"`. Multi-group → `b"chain_head:<group_id>"`.
pub fn build_chain_head_key(group_id: &str) -> Vec<u8> {
    if group_id.is_empty() {
        b"chain_head".to_vec()
    } else {
        format!("chain_head:{}", group_id).into_bytes()
    }
}

/// CF_METADATA key for block-hash-by-height. Legacy (group_id == "") →
/// `b"block_hash:<h>"`. Multi-group → `b"block_hash:<group_id>:<h>"` (group
/// first so a range-scan over one group's heights is contiguous).
pub fn build_block_hash_key(height: u64, group_id: &str) -> String {
    if group_id.is_empty() {
        format!("block_hash:{}", height)
    } else {
        format!("block_hash:{}:{}", group_id, height)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VestingType {
    /// Vesting lineare: token rilasciati linearmente nel tempo without cliff period
    Linear,
    /// Vesting with cliff: no tokens released before the cliff period, then linear
    Cliff,
    Staged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VestingSchedule {
    pub address: Vec<u8>,
    /// Unique schedule id
    pub schedule_id: u64,
    /// Amount totale da vestire
    pub amount: u128,
    /// Timestamp di inizio of the vesting
    pub start_time: u64,
    /// Durata totale of the vesting in secondi
    pub duration: u64,
    /// Cliff period in secondi
    pub cliff: u64,
    /// Tipo di vesting
    pub vesting_type: VestingType,
    /// Amount già vested
    pub vested_amount: u128,
    /// Amount già rilasciato
    pub released_amount: u128,
}

impl VestingSchedule {
    pub fn new(
        address: Vec<u8>,
        schedule_id: u64,
        amount: u128,
        start_time: u64,
        duration: u64,
        cliff: u64,
        vesting_type: VestingType,
    ) -> Self {
        Self {
            address,
            schedule_id,
            amount,
            start_time,
            duration,
            cliff,
            vesting_type,
            vested_amount: 0,
            released_amount: 0,
        }
    }
}

impl Default for VestingSchedule {
    fn default() -> Self {
        Self {
            address: Vec::new(),
            schedule_id: 0,
            amount: 0,
            start_time: 0,
            duration: 0,
            cliff: 0,
            vesting_type: VestingType::Linear,
            vested_amount: 0,
            released_amount: 0,
        }
    }
}

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "rocksdb")]
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB};

#[cfg(not(feature = "rocksdb"))]
use std::collections::HashMap;
#[cfg(not(feature = "rocksdb"))]
use std::sync::RwLock;

/// Column family names for blockchain data organization
pub const CF_DEFAULT: &str = "default";
pub const CF_BLOCKS: &str = "blocks";
/// P2.6-D.1: per-group LatticeBlock chain head persistence. Key =
/// raw group_id bytes; value = the 32-byte block_hash of the most
/// recently committed LatticeBlock for that group. Survives LN
/// restart so the shadow chain does not reset to genesis on every
/// reboot.
pub const CF_LATTICE_CHAIN_HEAD: &str = "lattice_chain_head";
pub const CF_TRANSACTIONS: &str = "transactions";
pub const CF_STATE: &str = "state";
pub const CF_METADATA: &str = "metadata";
pub const CF_ACCOUNTS: &str = "accounts";
pub const CF_REWARD_BALANCES: &str = "reward_balances";

/// Thread-safe storage interface with RocksDB backend
pub struct Storage {
    #[cfg(feature = "rocksdb")]
    db: Arc<DB>,
    #[cfg(not(feature = "rocksdb"))]
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,

    // Performance counters for monitoring
    read_count: Arc<AtomicU64>,
    write_count: Arc<AtomicU64>,
    batch_count: Arc<AtomicU64>,
}

/// Batch operation for atomic transactions
pub struct StorageBatch {
    #[cfg(feature = "rocksdb")]
    batch: WriteBatch,
    #[cfg(not(feature = "rocksdb"))]
    operations: Vec<(Vec<u8>, Option<Vec<u8>>, Option<String>)>, // (key, value, cf_name)
    storage: Option<Arc<Storage>>,
}

/// Enhanced storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub path: String,
    pub cache_size: usize,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub enable_compression: bool,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub use_fsync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "storage.db".to_string(),
            cache_size: 64 * 1024 * 1024,        // 64MB
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_write_buffer_number: 4,
            enable_compression: true,
            create_if_missing: true,
            max_open_files: 1000,
            use_fsync: true,
        }
    }
}

/// Storage snapshot for backup/restore
#[derive(Debug)]
pub struct StorageSnapshot {
    pub timestamp: u64,
    pub data_size: usize,
    pub block_count: usize,
    pub transaction_count: usize,
}

/// Mock column family handle for non-RocksDB implementations
#[cfg(not(feature = "rocksdb"))]
#[derive(Debug, Clone)]
pub struct MockColumnFamily {
    pub name: String,
}

#[cfg(not(feature = "rocksdb"))]
impl MockColumnFamily {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Performance statistics for monitoring
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub read_count: u64,
    pub write_count: u64,
    pub batch_count: u64,
}

// Add Clone trait for Storage
impl Clone for Storage {
    fn clone(&self) -> Self {
        #[cfg(feature = "rocksdb")]
        {
            Self {
                db: Arc::clone(&self.db),
                read_count: Arc::clone(&self.read_count),
                write_count: Arc::clone(&self.write_count),
                batch_count: Arc::clone(&self.batch_count),
            }
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            Self {
                data: Arc::clone(&self.data),
                read_count: Arc::clone(&self.read_count),
                write_count: Arc::clone(&self.write_count),
                batch_count: Arc::clone(&self.batch_count),
            }
        }
    }
}

impl Storage {
    /// Create new storage instance with default configuration
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = StorageConfig {
            path: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Create storage with custom configuration
    pub fn with_config(config: StorageConfig) -> Result<Self> {
        #[cfg(feature = "rocksdb")]
        {
            let mut opts = Options::default();
            opts.create_if_missing(config.create_if_missing);
            opts.set_write_buffer_size(config.write_buffer_size);
            opts.set_max_write_buffer_number(config.max_write_buffer_number);
            opts.set_max_open_files(config.max_open_files);

            if config.enable_compression {
                opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
            }

            // Create database directory and always open with all known column families.
            // This avoids "Column families not opened" on restart.
            std::fs::create_dir_all(&config.path)?;
            opts.create_missing_column_families(true);

            let expected_column_families = [
                CF_DEFAULT,
                CF_BLOCKS,
                CF_TRANSACTIONS,
                CF_STATE,
                CF_METADATA,
                CF_ACCOUNTS,
                CF_REWARD_BALANCES,
                contracts::CF_CONTRACTS,
                contracts::CF_CONTRACT_STORAGE,
                contracts::CF_CONTRACT_CODE,
                // P2.6-D.1
                CF_LATTICE_CHAIN_HEAD,
            ];

            let mut column_families: Vec<String> = match DB::list_cf(&opts, &config.path) {
                Ok(existing) => existing,
                Err(_) => vec![CF_DEFAULT.to_string()],
            };
            for cf in expected_column_families {
                if !column_families.iter().any(|name| name == cf) {
                    column_families.push(cf.to_string());
                }
            }
            info!(
                "Opening existing RocksDB with column families: {:?}",
                column_families
            );

            let descriptors: Vec<ColumnFamilyDescriptor> = column_families
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect();
            let db = DB::open_cf_descriptors(&opts, &config.path, descriptors)?;

            Ok(Self {
                db: Arc::new(db),
                read_count: Arc::new(AtomicU64::new(0)),
                write_count: Arc::new(AtomicU64::new(0)),
                batch_count: Arc::new(AtomicU64::new(0)),
            })
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            std::fs::create_dir_all(&config.path)?;
            Ok(Self {
                data: Arc::new(RwLock::new(HashMap::new())),
                read_count: Arc::new(AtomicU64::new(0)),
                write_count: Arc::new(AtomicU64::new(0)),
                batch_count: Arc::new(AtomicU64::new(0)),
            })
        }
    }

    /// Check if storage is healthy
    pub fn is_healthy(&self) -> bool {
        #[cfg(feature = "rocksdb")]
        {
            // Try to read a key to verify database is accessible
            self.db.get(b"__health_check__").is_ok()
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            self.data.read().is_ok()
        }
    }

    /// Get a reference to the underlying RocksDB instance (for pruning, compaction, etc.)
    #[cfg(feature = "rocksdb")]
    pub fn get_db(&self) -> Option<&Arc<DB>> {
        Some(&self.db)
    }

    #[cfg(not(feature = "rocksdb"))]
    pub fn get_db(&self) -> Option<&()> {
        None
    }

    /// Put data in default column family
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("rocksdb_writes_total").increment(1);

        #[cfg(feature = "rocksdb")]
        {
            self.db.put(key, value)?;
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let mut data = self
                .data
                .write()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }
    }

    /// Get data from default column family
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("rocksdb_reads_total").increment(1);

        #[cfg(feature = "rocksdb")]
        {
            Ok(self.db.get(key)?)
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            Ok(data.get(key).cloned())
        }
    }

    /// Put data in specific column family
    pub fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
            self.db.put_cf(&cf, key, value)?;
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let mut data = self
                .data
                .write()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            // Simulate column families with key prefixing
            let prefixed_key = format!("{}:{}", cf_name, String::from_utf8_lossy(key));
            data.insert(prefixed_key.into_bytes(), value.to_vec());
            Ok(())
        }
    }

    /// Get data from specific column family
    pub fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "rocksdb")]
        {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
            Ok(self.db.get_cf(&cf, key)?)
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            let prefixed_key = format!("{}:{}", cf_name, String::from_utf8_lossy(key));
            Ok(data.get(prefixed_key.as_bytes()).cloned())
        }
    }

    /// Delete data from specific column family
    pub fn delete_cf(&self, cf_name: &str, key: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
            self.db.delete_cf(&cf, key)?;
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let mut data = self
                .data
                .write()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            let prefixed_key = format!("{}:{}", cf_name, String::from_utf8_lossy(key));
            data.remove(prefixed_key.as_bytes());
            Ok(())
        }
    }

    /// Delete data
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            self.db.delete(key)?;
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            let mut data = self
                .data
                .write()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            data.remove(key);
            Ok(())
        }
    }

    /// Begin batch operation for atomic writes
    pub fn begin_batch(&self) -> StorageBatch {
        self.batch_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(feature = "rocksdb")]
        {
            StorageBatch {
                batch: WriteBatch::default(),
                storage: Some(Arc::new(self.clone())),
            }
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            StorageBatch {
                operations: Vec::new(),
                storage: Some(Arc::new(self.clone())),
            }
        }
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> StorageStats {
        StorageStats {
            read_count: self.read_count.load(Ordering::Relaxed),
            write_count: self.write_count.load(Ordering::Relaxed),
            batch_count: self.batch_count.load(Ordering::Relaxed),
        }
    }

    /// Reset performance counters
    pub fn reset_stats(&self) {
        self.read_count.store(0, Ordering::Relaxed);
        self.write_count.store(0, Ordering::Relaxed);
        self.batch_count.store(0, Ordering::Relaxed);
    }

    /// Create storage snapshot
    pub fn create_snapshot(&self) -> Result<StorageSnapshot> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        #[cfg(feature = "rocksdb")]
        {
            let mut data_size = 0;
            let mut block_count = 0;
            let mut transaction_count = 0;

            // Count entries in each column family
            let cf_blocks = self
                .db
                .cf_handle(CF_BLOCKS)
                .ok_or_else(|| anyhow::anyhow!("Missing column family: {}", CF_BLOCKS))?;
            let cf_transactions = self
                .db
                .cf_handle(CF_TRANSACTIONS)
                .ok_or_else(|| anyhow::anyhow!("Missing column family: {}", CF_TRANSACTIONS))?;

            let iter = self.db.iterator_cf(&cf_blocks, IteratorMode::Start);
            for item in iter {
                if let Ok((_, value)) = item {
                    data_size += value.len();
                    block_count += 1;
                }
            }

            let iter = self.db.iterator_cf(&cf_transactions, IteratorMode::Start);
            for item in iter {
                if let Ok((_, value)) = item {
                    data_size += value.len();
                    transaction_count += 1;
                }
            }

            Ok(StorageSnapshot {
                timestamp,
                data_size,
                block_count,
                transaction_count,
            })
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            let data_size: usize = data.values().map(|v: &Vec<u8>| v.len()).sum();
            Ok(StorageSnapshot {
                timestamp,
                data_size,
                block_count: 0,
                transaction_count: 0,
            })
        }
    }

    /// SECURITY (F-10): Create a RocksDB checkpoint backup at the given path.
    ///
    /// Uses RocksDB's `Checkpoint` API which creates a consistent, point-in-time
    /// backup using hard links (very fast, minimal disk overhead). The checkpoint
    /// can be opened as a standalone RocksDB database for verification or
    /// used for `restore_from_checkpoint()`.
    ///
    /// # Arguments
    /// * `checkpoint_path` — Directory where the checkpoint will be created.
    ///   Must not already exist.
    #[cfg(feature = "rocksdb")]
    pub fn create_checkpoint<P: AsRef<Path>>(&self, checkpoint_path: P) -> Result<StorageSnapshot> {
        let path = checkpoint_path.as_ref();

        if path.exists() {
            return Err(anyhow::anyhow!(
                "Checkpoint path already exists: {}",
                path.display()
            ));
        }

        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db)
            .map_err(|e| anyhow::anyhow!("Failed to create checkpoint handle: {}", e))?;

        checkpoint.create_checkpoint(path).map_err(|e| {
            anyhow::anyhow!("Failed to create checkpoint at {}: {}", path.display(), e)
        })?;

        info!("RocksDB checkpoint created at {}", path.display());

        // Return a snapshot of what was backed up
        self.create_snapshot()
    }

    /// SECURITY (F-10): Restore database from a checkpoint backup.
    ///
    /// This creates a **new** `Storage` instance from the checkpoint directory.
    /// The caller is responsible for replacing the active storage reference.
    ///
    /// # Arguments
    /// * `checkpoint_path` — Path to a previously created checkpoint.
    #[cfg(feature = "rocksdb")]
    pub fn restore_from_checkpoint<P: AsRef<Path>>(checkpoint_path: P) -> Result<Self> {
        let path = checkpoint_path.as_ref();

        if !path.exists() {
            return Err(anyhow::anyhow!(
                "Checkpoint path does not exist: {}",
                path.display()
            ));
        }

        let config = StorageConfig {
            path: path.to_string_lossy().to_string(),
            create_if_missing: false,
            ..StorageConfig::default()
        };

        info!("Restoring storage from checkpoint at {}", path.display());

        Self::with_config(config)
    }

    // ============================================================================
    // GOVERNANCE METHODS - For savitri-contracts compatibility
    // ============================================================================

    /// Get vote token balance for an address
    pub fn get_vote_token_balance(&self, address: &[u8]) -> Result<u128> {
        let key = [b"vote_token:", address].concat();
        match self.get(&key)? {
            Some(data) if data.len() >= 16 => {
                let bytes: [u8; 16] = data[..16].try_into().unwrap_or([0; 16]);
                Ok(u128::from_le_bytes(bytes))
            }
            _ => Ok(0),
        }
    }

    /// Increment vote token balance
    pub fn increment_vote_token_balance(&self, address: &[u8], amount: u128) -> Result<()> {
        let current = self.get_vote_token_balance(address)?;
        let new_balance = current.saturating_add(amount);
        let key = [b"vote_token:", address].concat();
        self.put(&key, &new_balance.to_le_bytes())
    }

    /// Decrement vote token balance
    pub fn decrement_vote_token_balance(&self, address: &[u8], amount: u128) -> Result<()> {
        let current = self.get_vote_token_balance(address)?;
        let new_balance = current.saturating_sub(amount);
        let key = [b"vote_token:", address].concat();
        self.put(&key, &new_balance.to_le_bytes())
    }

    /// Set fee base
    pub fn set_fee_base(&self, fee_base: u64) -> Result<()> {
        self.put(b"fee:base", &fee_base.to_le_bytes())
    }

    /// Set fee max
    pub fn set_fee_max(&self, fee_max: u64) -> Result<()> {
        self.put(b"fee:max", &fee_max.to_le_bytes())
    }

    /// Put approved standard
    pub fn put_approved_standard(&self, standard_id: &[u8], data: &[u8]) -> Result<()> {
        let key = [b"standard:", standard_id].concat();
        self.put(&key, data)
    }

    /// Put non-core change
    pub fn put_non_core_change(&self, change_id: &[u8], data: &[u8]) -> Result<()> {
        let key = [b"non_core_change:", change_id].concat();
        self.put(&key, data)
    }

    /// Set FL policy
    pub fn set_fl_policy(&self, policy_data: &[u8]) -> Result<()> {
        self.put(b"fl:policy", policy_data)
    }

    /// Approve FL model
    pub fn approve_fl_model(&self, model_id: &[u8]) -> Result<()> {
        let key = [b"fl:approved_model:", model_id].concat();
        self.put(&key, b"approved")
    }

    /// Abort FL round
    pub fn abort_fl_round(&self, round_id: u64) -> Result<()> {
        let key = format!("fl:round:{}:status", round_id);
        self.put(key.as_bytes(), b"aborted")
    }

    /// Put connector info
    pub fn put_connector_info(&self, connector_id: &[u8], data: &[u8]) -> Result<()> {
        let key = [b"connector:", connector_id].concat();
        self.put(&key, data)
    }

    /// Check if connector exists
    pub fn connector_exists(&self, connector_id: &[u8]) -> Result<bool> {
        let key = [b"connector:", connector_id].concat();
        Ok(self.get(&key)?.is_some())
    }

    /// Delete connector info
    pub fn delete_connector_info(&self, connector_id: &[u8]) -> Result<()> {
        let key = [b"connector:", connector_id].concat();
        self.delete(&key)
    }

    /// Iterator over column family (abstracted)
    pub fn iterator_cf(
        &self,
        cf_name: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        #[cfg(feature = "rocksdb")]
        {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;

            let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
            let results: Vec<Result<(Vec<u8>, Vec<u8>), anyhow::Error>> = iter
                .map(|item| {
                    item.map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(|e| anyhow::anyhow!("Iterator error: {}", e))
                })
                .collect();

            Ok(Box::new(results.into_iter()))
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            let prefix = format!("{}:", cf_name);
            let results: Vec<Result<(Vec<u8>, Vec<u8>)>> = data
                .iter()
                .filter(|(k, _)| String::from_utf8_lossy(k).starts_with(&prefix))
                .map(|(k, v)| {
                    // Remove prefix from key
                    let key_str = String::from_utf8_lossy(k);
                    let key_without_prefix = key_str
                        .strip_prefix(&prefix)
                        .unwrap_or(&key_str)
                        .as_bytes()
                        .to_vec();
                    Ok((key_without_prefix, v.clone()))
                })
                .collect();
            Ok(Box::new(results.into_iter()))
        }
    }

    /// Scan a key prefix from a column family without reading unrelated rows.
    /// Returns at most `limit` entries.
    pub fn scan_cf_prefix(
        &self,
        cf_name: &str,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        #[cfg(feature = "rocksdb")]
        {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
            let mut results = Vec::with_capacity(limit.min(256));

            if reverse {
                let mut start_key = prefix.to_vec();
                start_key.push(0xFF);
                let iter = self
                    .db
                    .iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Reverse));
                for item in iter {
                    let (key, value) =
                        item.map_err(|e| anyhow::anyhow!("Iterator error: {}", e))?;
                    if !key.starts_with(prefix) {
                        if !results.is_empty() {
                            break;
                        }
                        continue;
                    }
                    results.push((key.to_vec(), value.to_vec()));
                    if results.len() >= limit {
                        break;
                    }
                }
            } else {
                let iter = self.db.prefix_iterator_cf(&cf, prefix);
                for item in iter {
                    let (key, value) =
                        item.map_err(|e| anyhow::anyhow!("Iterator error: {}", e))?;
                    if !key.starts_with(prefix) {
                        break;
                    }
                    results.push((key.to_vec(), value.to_vec()));
                    if results.len() >= limit {
                        break;
                    }
                }
            }

            Ok(results)
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            let cf_prefix = format!("{}:", cf_name);
            let mut results: Vec<(Vec<u8>, Vec<u8>)> = data
                .iter()
                .filter_map(|(k, v)| {
                    let key_str = String::from_utf8_lossy(k);
                    if !key_str.starts_with(&cf_prefix) {
                        return None;
                    }
                    let key_without_prefix = key_str
                        .strip_prefix(&cf_prefix)
                        .unwrap_or(&key_str)
                        .as_bytes()
                        .to_vec();
                    if key_without_prefix.starts_with(prefix) {
                        Some((key_without_prefix, v.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));
            if reverse {
                results.reverse();
            }
            results.truncate(limit);
            Ok(results)
        }
    }

    // ============================================================================
    // FEDERATED LEARNING METHODS - For savitri-contracts compatibility
    // ============================================================================

    /// Get FL policy
    pub fn get_fl_policy(&self) -> Result<Option<Vec<u8>>> {
        self.get(b"fl:policy")
    }

    /// Check if FL model is approved
    pub fn is_fl_model_approved(&self, model_id: &[u8]) -> Result<bool> {
        let key = [b"fl:approved_model:", model_id].concat();
        Ok(self.get(&key)?.is_some())
    }

    /// Check if FL round is aborted
    pub fn is_fl_round_aborted(&self, round_id: u64) -> Result<bool> {
        let key = format!("fl:round:{}:status", round_id);
        match self.get(key.as_bytes())? {
            Some(data) => Ok(data == b"aborted"),
            None => Ok(false),
        }
    }

    // ============================================================================
    // GOVERNANCE METHODS - For savitri-contracts compatibility
    // ============================================================================

    /// Get total vote tokens
    pub fn get_total_vote_tokens(&self) -> Result<u128> {
        match self.get(b"vote_tokens:total")? {
            Some(data) if data.len() >= 16 => {
                let bytes: [u8; 16] = data[..16].try_into().unwrap_or([0; 16]);
                Ok(u128::from_le_bytes(bytes))
            }
            _ => Ok(0),
        }
    }

    /// Get next proposal ID
    pub fn next_proposal_id(&self) -> Result<u64> {
        match self.get(b"proposals:next_id")? {
            Some(data) if data.len() >= 8 => {
                let bytes: [u8; 8] = data[..8].try_into().unwrap_or([0; 8]);
                Ok(u64::from_le_bytes(bytes))
            }
            _ => Ok(1),
        }
    }

    /// Put proposal
    pub fn put_proposal(&self, proposal_id: u64, proposal_data: &[u8]) -> Result<()> {
        let key = format!("proposal:{}", proposal_id);
        self.put(key.as_bytes(), proposal_data)
    }

    /// Get proposal
    pub fn get_proposal(&self, proposal_id: u64) -> Result<Option<Vec<u8>>> {
        let key = format!("proposal:{}", proposal_id);
        self.get(key.as_bytes())
    }

    /// Get vote for address
    pub fn get_vote(&self, voter: &[u8], proposal_id: u64) -> Result<Option<Vec<u8>>> {
        let key = format!("vote:{}:{}", hex::encode(voter), proposal_id);
        self.get(key.as_bytes())
    }

    /// Get available vote tokens for address
    pub fn get_available_vote_tokens(&self, address: &[u8]) -> Result<u128> {
        self.get_vote_token_balance(address)
    }

    /// Get locked vote tokens for address
    pub fn get_locked_vote_tokens(&self, address: &[u8]) -> Result<u128> {
        let key = format!("locked_tokens:{}", hex::encode(address));
        match self.get(key.as_bytes())? {
            Some(data) if data.len() >= 16 => {
                let bytes: [u8; 16] = data[..16].try_into().unwrap_or([0; 16]);
                Ok(u128::from_le_bytes(bytes))
            }
            _ => Ok(0),
        }
    }

    /// Put vote
    pub fn put_vote(&self, voter: &[u8], proposal_id: u64, vote_data: &[u8]) -> Result<()> {
        let key = format!("vote:{}:{}", hex::encode(voter), proposal_id);
        self.put(key.as_bytes(), vote_data)
    }

    /// Get proposal votes
    pub fn get_proposal_votes(&self, proposal_id: u64) -> Result<Vec<Vec<u8>>> {
        let prefix = format!("vote:");
        let suffix = format!(":{}", proposal_id);
        let mut votes = Vec::new();

        #[cfg(feature = "rocksdb")]
        {
            // Iterate over all keys with vote: prefix and matching proposal_id suffix
            let iter = self.db.iterator(IteratorMode::Start);
            for item in iter {
                if let Ok((key, value)) = item {
                    let key_str = String::from_utf8_lossy(&key);
                    if key_str.starts_with(&prefix) && key_str.ends_with(&suffix) {
                        votes.push(value.to_vec());
                    }
                }
            }
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            for (key, value) in data.iter() {
                let key_str = String::from_utf8_lossy(key);
                if key_str.starts_with(&prefix) && key_str.ends_with(&suffix) {
                    votes.push(value.clone());
                }
            }
        }

        Ok(votes)
    }

    /// Get all proposals
    pub fn get_all_proposals(&self) -> Result<Vec<u64>> {
        let prefix = "proposal:";
        let mut proposal_ids = Vec::new();

        #[cfg(feature = "rocksdb")]
        {
            let iter = self.db.iterator(IteratorMode::Start);
            for item in iter {
                if let Ok((key, _)) = item {
                    let key_str = String::from_utf8_lossy(&key);
                    if key_str.starts_with(prefix) {
                        // Extract proposal ID from key "proposal:{id}"
                        if let Some(id_str) = key_str.strip_prefix(prefix) {
                            if let Ok(id) = id_str.parse::<u64>() {
                                proposal_ids.push(id);
                            }
                        }
                    }
                }
            }
        }

        #[cfg(not(feature = "rocksdb"))]
        {
            let data = self
                .data
                .read()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
            for key in data.keys() {
                let key_str = String::from_utf8_lossy(key);
                if key_str.starts_with(prefix) {
                    if let Some(id_str) = key_str.strip_prefix(prefix) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            proposal_ids.push(id);
                        }
                    }
                }
            }
        }

        // Sort for consistent ordering
        proposal_ids.sort();
        Ok(proposal_ids)
    }

    /// Get contract
    pub fn get_contract(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = format!("contract:{}", hex::encode(address));
        self.get(key.as_bytes())
    }

    /// Check if contract exists
    pub fn contract_exists(&self, address: &[u8]) -> Result<bool> {
        self.get_contract(address)
            .map(|opt: Option<Vec<u8>>| opt.is_some())
    }

    /// Put contract
    pub fn put_contract(&self, address: &[u8], contract_data: &[u8]) -> Result<()> {
        let key = format!("contract:{}", hex::encode(address));
        self.put(key.as_bytes(), contract_data)
    }

    /// Update contract code
    pub fn update_contract_code(&self, address: &[u8], new_code: &[u8]) -> Result<()> {
        self.put_contract(address, new_code)
    }

    /// Commit contract storage overlay (slot -> value) into contracts CF.
    pub fn commit_contract_storage_overlay(
        &self,
        address: &[u8],
        overlay: &std::collections::BTreeMap<u64, Vec<u8>>,
    ) -> Result<()> {
        for (slot, value) in overlay.iter() {
            let mut key = Vec::with_capacity(8 + address.len() + 8);
            key.extend_from_slice(b"storage:");
            key.extend_from_slice(address);
            key.extend_from_slice(&slot.to_le_bytes());

            if value.iter().all(|&b| b == 0) {
                self.delete_cf(crate::storage::contracts::CF_CONTRACTS, &key)?;
            } else {
                self.put_cf(crate::storage::contracts::CF_CONTRACTS, &key, value)?;
            }
        }
        Ok(())
    }

    /// Get account
    pub fn get_account(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = format!("account:{}", hex::encode(address));
        self.get(key.as_bytes())
    }

    /// Put account
    pub fn put_account(&self, address: &[u8], account_data: &[u8]) -> Result<()> {
        let key = format!("account:{}", hex::encode(address));
        self.put(key.as_bytes(), account_data)
    }

    /// Get oracle schema
    pub fn get_oracle_schema(&self, schema_id: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = format!("oracle_schema:{}", hex::encode(schema_id));
        self.get(key.as_bytes())
    }

    /// Put oracle schema
    pub fn put_oracle_schema(&self, schema_id: &[u8], schema_data: &[u8]) -> Result<()> {
        let key = format!("oracle_schema:{}", hex::encode(schema_id));
        self.put(key.as_bytes(), schema_data)
    }

    /// Get oracle max sequence
    pub fn get_oracle_max_sequence(&self, schema_id: &[u8]) -> Result<Option<u64>> {
        let key = format!("oracle_max_seq:{}", hex::encode(schema_id));
        match self.get(key.as_bytes())? {
            Some(data) if data.len() >= 8 => {
                let bytes: [u8; 8] = data[..8].try_into().unwrap_or([0; 8]);
                Ok(Some(u64::from_le_bytes(bytes)))
            }
            _ => Ok(None),
        }
    }

    /// Put oracle max sequence
    pub fn put_oracle_max_sequence(&self, schema_id: &[u8], sequence: u64) -> Result<()> {
        let key = format!("oracle_max_seq:{}", hex::encode(schema_id));
        let data = sequence.to_le_bytes();
        self.put(key.as_bytes(), &data)
    }

    /// Get oracle feed
    pub fn get_oracle_feed(&self, schema_id: &[u8], sequence: u64) -> Result<Option<Vec<u8>>> {
        let key = format!("oracle_feed:{}:{}", hex::encode(schema_id), sequence);
        self.get(key.as_bytes())
    }

    /// Put oracle feed
    pub fn put_oracle_feed(&self, schema_id: &[u8], sequence: u64, feed_data: &[u8]) -> Result<()> {
        let key = format!("oracle_feed:{}:{}", hex::encode(schema_id), sequence);
        self.put(key.as_bytes(), feed_data)
    }

    /// Get oracle ACL
    pub fn get_oracle_acl(&self, schema_id: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = format!("oracle_acl:{}", hex::encode(schema_id));
        self.get(key.as_bytes())
    }

    /// Put oracle ACL
    pub fn put_oracle_acl(&self, schema_id: &[u8], acl_data: &[u8]) -> Result<()> {
        let key = format!("oracle_acl:{}", hex::encode(schema_id));
        self.put(key.as_bytes(), acl_data)
    }

    /// Check proposal quorum
    /// A proposal reaches quorum when total votes (yes + no + abstain) >= 10% of total vote tokens
    pub fn check_proposal_quorum(&self, proposal_id: u64) -> Result<bool> {
        // Get proposal data
        let proposal_data: Vec<u8> = match self.get_proposal(proposal_id)? {
            Some(data) => data,
            None => return Ok(false), // Proposal doesn't exist
        };

        // Deserialize proposal to get vote counts
        let proposal: governance::Proposal = match crate::safe_deserialize(&proposal_data) {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };

        // Get total vote tokens in the system
        let total_vote_tokens = self.get_total_vote_tokens()?;
        if total_vote_tokens == 0 {
            return Ok(false);
        }

        // Calculate total votes cast
        let total_votes = proposal
            .yes_votes
            .saturating_add(proposal.no_votes)
            .saturating_add(proposal.abstain_votes);

        // Quorum threshold: 10% of total vote tokens (1000 basis points)
        const QUORUM_THRESHOLD_BPS: u128 = 1000; // 10%
        let quorum_threshold = total_vote_tokens
            .saturating_mul(QUORUM_THRESHOLD_BPS)
            .saturating_div(10000);

        Ok(total_votes >= quorum_threshold)
    }

    /// Check proposal approval
    /// A proposal is approved when yes_votes > no_votes and quorum is reached
    pub fn check_proposal_approval(&self, proposal_id: u64) -> Result<bool> {
        // First check if quorum is reached
        if !self.check_proposal_quorum(proposal_id)? {
            return Ok(false);
        }

        // Get proposal data
        let proposal_data: Vec<u8> = match self.get_proposal(proposal_id)? {
            Some(data) => data,
            None => return Ok(false),
        };

        // Deserialize proposal to get vote counts
        let proposal: governance::Proposal = match crate::safe_deserialize(&proposal_data) {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };

        // Approval: yes votes must exceed no votes (abstain votes don't count against)
        Ok(proposal.yes_votes > proposal.no_votes)
    }

    /// Get column family handle (RocksDB specific)
    #[cfg(feature = "rocksdb")]
    pub fn cf(&self, cf_name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))
    }

    /// Put data using column family handle (RocksDB)
    #[cfg(feature = "rocksdb")]
    pub fn put_cf_handle(
        &self,
        cf: &rocksdb::ColumnFamily,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }

    /// Get data using column family handle (RocksDB)
    #[cfg(feature = "rocksdb")]
    pub fn get_cf_handle(&self, cf: &rocksdb::ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(cf, key)?)
    }

    /// Delete data using column family handle (RocksDB)
    #[cfg(feature = "rocksdb")]
    pub fn delete_cf_handle(&self, cf: &rocksdb::ColumnFamily, key: &[u8]) -> Result<()> {
        self.db.delete_cf(cf, key)?;
        Ok(())
    }

    /// Get column family handle (non-RocksDB implementation)
    #[cfg(not(feature = "rocksdb"))]
    pub fn cf(&self, cf_name: &str) -> Result<MockColumnFamily> {
        // Validate column family name exists in our known list
        let known_cfs = [
            CF_DEFAULT,
            CF_BLOCKS,
            CF_TRANSACTIONS,
            CF_STATE,
            CF_METADATA,
            CF_ACCOUNTS,
            contracts::CF_CONTRACTS,
            contracts::CF_CONTRACT_STORAGE,
            contracts::CF_CONTRACT_CODE,
            // P2.6-D.1
            CF_LATTICE_CHAIN_HEAD,
        ];

        if known_cfs.contains(&cf_name) {
            Ok(MockColumnFamily::new(cf_name))
        } else {
            Err(anyhow::anyhow!("Column family '{}' not found", cf_name))
        }
    }

    /// Put data using mock column family handle (non-RocksDB)
    #[cfg(not(feature = "rocksdb"))]
    pub fn put_cf_handle(&self, cf: &MockColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(cf.name(), key, value)
    }

    /// Get data using mock column family handle (non-RocksDB)
    #[cfg(not(feature = "rocksdb"))]
    pub fn get_cf_handle(&self, cf: &MockColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(cf.name(), key)
    }

    /// Delete data using mock column family handle (non-RocksDB)
    #[cfg(not(feature = "rocksdb"))]
    pub fn delete_cf_handle(&self, cf: &MockColumnFamily, key: &[u8]) -> Result<()> {
        self.delete_cf(cf.name(), key)
    }

    /// Put oracle anchor
    pub fn put_oracle_anchor(&self, feed_id: &[u8], anchor_data: &[u8]) -> Result<()> {
        let key = format!("oracle_anchor:{}", hex::encode(feed_id));
        self.put(key.as_bytes(), anchor_data)
    }

    // ============================================================================
    // BLOCKCHAIN DATA METHODS
    // ============================================================================

    /// Get block by height
    pub fn get_block(&self, height: u64) -> Result<Option<Vec<u8>>> {
        let key = height.to_le_bytes();
        self.get_cf(CF_BLOCKS, &key)
    }

    /// Set block at height (single-lane). Equivalent to set_block_in_group(height, "", data).
    pub fn set_block(&self, height: u64, block_data: &[u8]) -> Result<()> {
        self.set_block_in_group(height, "", block_data)
    }

    /// Set block at height within a specific group's lane. When `group_id` is
    /// empty the legacy key format (bare `height.to_le_bytes()`) is used so
    /// pre-multigroup data is still readable. Otherwise the key becomes
    /// `height_le || ':' || group_id` — distinct groups never clobber each other.
    pub fn set_block_in_group(&self, height: u64, group_id: &str, block_data: &[u8]) -> Result<()> {
        let key = build_block_key(height, group_id);
        self.put_cf(CF_BLOCKS, &key, block_data)
    }

    /// Get block at height within a specific group's lane. Empty group_id uses
    /// the legacy bare-height key.
    pub fn get_block_in_group(&self, height: u64, group_id: &str) -> Result<Option<Vec<u8>>> {
        let key = build_block_key(height, group_id);
        self.get_cf(CF_BLOCKS, &key)
    }

    /// Get chain head (latest block) — legacy single-lane. Equivalent to
    /// get_chain_head_for_group("").
    pub fn get_chain_head(&self) -> Result<Option<Vec<u8>>> {
        self.get_chain_head_for_group("")
    }

    /// Get chain head for a specific group. Under multi-group each group has
    /// its own head; under single-group (group_id="") this is the legacy key.
    pub fn get_chain_head_for_group(&self, group_id: &str) -> Result<Option<Vec<u8>>> {
        let key = build_chain_head_key(group_id);
        self.get_cf(CF_METADATA, &key)
    }

    /// Set chain head (latest block) — legacy single-lane. Equivalent to
    /// set_chain_head_for_group("").
    pub fn set_chain_head(&self, block_data: &[u8]) -> Result<()> {
        self.set_chain_head_for_group("", block_data)
    }

    /// Set chain head within a specific group's lane.
    pub fn set_chain_head_for_group(&self, group_id: &str, block_data: &[u8]) -> Result<()> {
        let key = build_chain_head_key(group_id);
        self.put_cf(CF_METADATA, &key, block_data)
    }

    /// Set transaction
    pub fn set_transaction(&self, tx_hash: &[u8], tx_data: &[u8]) -> Result<()> {
        self.put_cf(CF_TRANSACTIONS, tx_hash, tx_data)
    }

    /// Get transaction
    pub fn get_transaction(&self, tx_hash: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(CF_TRANSACTIONS, tx_hash)
    }

    /// Put supply manager state
    pub fn put_supply_manager(&self, supply_manager: &[u8]) -> Result<()> {
        self.put_cf(CF_METADATA, b"supply_manager", supply_manager)
    }

    /// Get supply manager state
    pub fn get_supply_manager(&self) -> Result<Option<Vec<u8>>> {
        self.get_cf(CF_METADATA, b"supply_manager")
    }

    /// Put genesis metadata
    pub fn put_genesis_metadata(&self, metadata: &[u8]) -> Result<()> {
        self.put_cf(CF_METADATA, b"genesis_metadata", metadata)
    }

    /// Get genesis metadata
    pub fn get_genesis_metadata(&self) -> Result<Option<Vec<u8>>> {
        self.get_cf(CF_METADATA, b"genesis_metadata")
    }

    /// Get block hash by height — legacy single-lane. Equivalent to
    /// get_block_hash_by_height_in_group(height, "").
    pub fn get_block_hash_by_height(&self, height: u64) -> Result<Option<Vec<u8>>> {
        self.get_block_hash_by_height_in_group(height, "")
    }

    /// Get block hash for a specific (height, group) pair.
    pub fn get_block_hash_by_height_in_group(
        &self,
        height: u64,
        group_id: &str,
    ) -> Result<Option<Vec<u8>>> {
        let key = build_block_hash_key(height, group_id);
        self.get_cf(CF_METADATA, key.as_bytes())
    }

    /// Set block hash for height — legacy single-lane. Equivalent to
    /// set_block_hash_for_height_in_group(height, "", hash).
    pub fn set_block_hash_for_height(&self, height: u64, hash: &[u8]) -> Result<()> {
        self.set_block_hash_for_height_in_group(height, "", hash)
    }

    /// Set block hash for a specific (height, group) pair. Different groups
    /// at the same physical height no longer clobber each other.
    pub fn set_block_hash_for_height_in_group(
        &self,
        height: u64,
        group_id: &str,
        hash: &[u8],
    ) -> Result<()> {
        let key = build_block_hash_key(height, group_id);
        self.put_cf(CF_METADATA, key.as_bytes(), hash)
    }

    /// Delete block hash for height — legacy single-lane.
    pub fn delete_block_hash_for_height(&self, height: u64) -> Result<()> {
        self.delete_block_hash_for_height_in_group(height, "")
    }

    pub fn delete_block_hash_for_height_in_group(&self, height: u64, group_id: &str) -> Result<()> {
        let key = build_block_hash_key(height, group_id);
        self.delete_cf(CF_METADATA, key.as_bytes())
    }

    /// Put block (alias for set_block for API compatibility)
    pub fn put_block(&self, height: u64, block_data: &[u8]) -> Result<()> {
        self.set_block(height, block_data)
    }

    /// Put block within a specific group's lane.
    pub fn put_block_in_group(&self, height: u64, group_id: &str, block_data: &[u8]) -> Result<()> {
        self.set_block_in_group(height, group_id, block_data)
    }

    /// Delete block
    pub fn delete_block(&self, hash: &[u8]) -> Result<()> {
        // Find the block by iterating through blocks and matching hash
        // This is inefficient but works for genesis deletion
        // For production, consider maintaining a hash-to-height index
        self.delete_cf(CF_BLOCKS, hash)
    }

    /// Delete chain head
    pub fn delete_chain_head(&self) -> Result<()> {
        self.delete_cf(CF_METADATA, b"chain_head")
    }

    /// Put vesting schedule
    pub fn put_vesting_schedule(&self, schedule: &VestingSchedule) -> Result<()> {
        let key = format!(
            "vesting:{}:{}",
            hex::encode(&schedule.address),
            schedule.schedule_id
        );
        let value = bincode::serialize(schedule)
            .map_err(|e| anyhow::anyhow!("Failed to serialize vesting schedule: {}", e))?;
        self.put_cf(CF_STATE, key.as_bytes(), &value)
    }

    /// Get vesting schedule
    pub fn get_vesting_schedule(
        &self,
        address: &[u8],
        schedule_id: u64,
    ) -> Result<Option<VestingSchedule>> {
        let key = format!("vesting:{}:{}", hex::encode(address), schedule_id);
        if let Some(bytes) = self.get_cf(CF_STATE, key.as_bytes())? {
            let schedule: VestingSchedule = crate::safe_deserialize(&bytes)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize vesting schedule: {}", e))?;
            Ok(Some(schedule))
        } else {
            Ok(None)
        }
    }

    /// Get consensus last slot
    pub fn get_consensus_last_slot(&self) -> Result<Option<u64>> {
        if let Some(bytes) = self.get_cf(CF_METADATA, b"consensus_last_slot")? {
            if bytes.len() == 8 {
                let mut array = [0u8; 8];
                array.copy_from_slice(&bytes);
                Ok(Some(u64::from_le_bytes(array)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Set consensus last slot
    pub fn set_consensus_last_slot(&self, slot: u64) -> Result<()> {
        self.put_cf(CF_METADATA, b"consensus_last_slot", &slot.to_le_bytes())
    }

    /// Get consensus slot base ms
    pub fn get_consensus_slot_base_ms(&self) -> Result<Option<u64>> {
        if let Some(bytes) = self.get_cf(CF_METADATA, b"consensus_slot_base_ms")? {
            if bytes.len() == 8 {
                let mut array = [0u8; 8];
                array.copy_from_slice(&bytes);
                Ok(Some(u64::from_le_bytes(array)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Set consensus slot base ms
    pub fn set_consensus_slot_base_ms(&self, ms: u64) -> Result<()> {
        self.put_cf(CF_METADATA, b"consensus_slot_base_ms", &ms.to_le_bytes())
    }
}

impl StorageBatch {
    /// Put data in batch (default column family)
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            self.batch.put(key, value);
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            self.operations
                .push((key.to_vec(), Some(value.to_vec()), None));
            Ok(())
        }
    }

    /// Put data in batch (specific column family)
    pub fn put_cf(&mut self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            if let Some(storage) = &self.storage {
                let cf = storage
                    .db
                    .cf_handle(cf_name)
                    .ok_or_else(|| anyhow::anyhow!("Column family '{}' not found", cf_name))?;
                self.batch.put_cf(&cf, key, value);
            }
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            self.operations.push((
                key.to_vec(),
                Some(value.to_vec()),
                Some(cf_name.to_string()),
            ));
            Ok(())
        }
    }

    /// Put data in batch using column family handle (RocksDB)
    #[cfg(feature = "rocksdb")]
    pub fn put_cf_handle(
        &mut self,
        cf: &rocksdb::ColumnFamily,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        self.batch.put_cf(cf, key, value);
        Ok(())
    }

    /// Put data in batch using mock column family handle (non-RocksDB)
    #[cfg(not(feature = "rocksdb"))]
    pub fn put_cf_handle(&mut self, cf: &MockColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        self.operations.push((
            key.to_vec(),
            Some(value.to_vec()),
            Some(cf.name().to_string()),
        ));
        Ok(())
    }

    /// Delete data in batch
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            self.batch.delete(key);
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            self.operations.push((key.to_vec(), None, None));
            Ok(())
        }
    }

    /// Commit batch atomically
    pub fn commit(self) -> Result<()> {
        #[cfg(feature = "rocksdb")]
        {
            if let Some(storage) = &self.storage {
                storage.db.write(self.batch)?;
            }
            Ok(())
        }
        #[cfg(not(feature = "rocksdb"))]
        {
            if let Some(storage) = &self.storage {
                let mut data = storage
                    .data
                    .write()
                    .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

                for (key, value, cf_name) in self.operations {
                    if let Some(cf) = cf_name {
                        let prefixed_key = format!("{}:{}", cf, String::from_utf8_lossy(&key));
                        if let Some(val) = value {
                            data.insert(prefixed_key.into_bytes(), val);
                        } else {
                            data.remove(&prefixed_key.into_bytes());
                        }
                    } else {
                        if let Some(val) = value {
                            data.insert(key, val);
                        } else {
                            data.remove(&key);
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

impl crate::traits::StorageTrait for Storage {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete(key)
    }

    fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf(cf_name, key, value)
    }

    fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(cf_name, key)
    }

    fn iterator_cf(
        &self,
        cf_name: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        self.iterator_cf(cf_name)
    }

    fn scan_cf_prefix(
        &self,
        cf_name: &str,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Storage::scan_cf_prefix(self, cf_name, prefix, limit, reverse)
    }

    fn is_healthy(&self) -> bool {
        // Check if storage is healthy by attempting a simple operation
        self.get(b"health_check").is_ok()
    }

    fn get_account(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf(CF_ACCOUNTS, address)
    }

    fn put_account(&self, address: &[u8], account_data: &[u8]) -> Result<()> {
        self.put_cf(CF_ACCOUNTS, address, account_data)
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field(
                "read_count",
                &self.read_count.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "write_count",
                &self.write_count.load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "batch_count",
                &self.batch_count.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}
