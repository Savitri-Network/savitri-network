//! Storage module for Savitri Light Node
//!
//! This module provides a lightweight storage implementation for light nodes,
//! optimized for mobile and desktop use with memory-based storage.

#![allow(dead_code)]

use anyhow::Result;
use bincode;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

use crate::tx::Block;

/// Maximum allowed size for deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads.
const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

/// Data for an atomic block commit operation.
///
/// SECURITY (AUDIT-029): Encapsulates all data that must be written atomically
/// when committing a block. Used by `commit_block_batch` to ensure crash-consistency.
pub struct BlockCommitData {
    pub block: Block,
    pub accounts: Vec<(Vec<u8>, Account)>,
    pub receipts: Vec<(Vec<u8>, Vec<u8>)>,
    pub transactions: Vec<(Vec<u8>, Vec<u8>)>,
    pub tx_inclusions: Vec<(Vec<u8>, u64)>,
    /// Multi-group lane for this commit. When None or empty string, the legacy
    /// single-lane storage keys are used — full backward compat under
    /// `SAVITRI_FORCE_SINGLE_GROUP=1`. Under real multi-group, callers must
    /// populate this with the block's group_id so two groups at the same
    /// physical height don't clobber each other's storage slots.
    pub group_id: Option<String>,
}

/// Trait for storage that supports blocks and accounts (used by genesis and account setup).
/// Implemented by in-memory Storage and by RocksDB-backed RocksDBLightnodeStorage.
pub trait BlockAndAccountStorage: Send + Sync {
    fn get_block(&self, height: u64) -> Result<Option<Block>>;
    fn set_block(&self, height: u64, block: Block) -> Result<()>;
    fn set_chain_head(&self, block: &Block) -> Result<()>;
    fn get_chain_head(&self) -> Result<Option<Block>>;

    // ─── Multi-group composite-key variants ─────────────────────────────
    //
    // Default impls delegate to the legacy single-lane methods so existing
    // impls don't need to opt in. Specialised impls (RocksDBLightnodeStorage)
    // override to use the group-aware storage keys — different groups can
    // commit blocks at the same physical height without clobbering each other.

    /// Get block at (height, group_id). Default: delegate to `get_block(height)`.
    fn get_block_in_group(&self, height: u64, _group_id: &str) -> Result<Option<Block>> {
        self.get_block(height)
    }

    /// Set block at (height, group_id). Default: delegate to `set_block(height, block)`.
    fn set_block_in_group(&self, height: u64, _group_id: &str, block: Block) -> Result<()> {
        self.set_block(height, block)
    }

    /// Set chain head for a specific group. Default: delegate to `set_chain_head`.
    fn set_chain_head_for_group(&self, _group_id: &str, block: &Block) -> Result<()> {
        self.set_chain_head(block)
    }

    /// Get chain head for a specific group. Default: delegate to `get_chain_head`.
    fn get_chain_head_for_group(&self, _group_id: &str) -> Result<Option<Block>> {
        self.get_chain_head()
    }
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>>;
    fn put_account(&self, address: &[u8], account: &Account) -> Result<()>;
    fn store_receipt(&self, key: &[u8], value: Vec<u8>) -> Result<()>;
    /// Store transaction by hash (for RPC lookup)
    fn set_transaction_by_hash(&self, tx_hash: &[u8], tx_bytes: Vec<u8>) -> Result<()>;
    /// Set tx -> block_height index (for timestamp lookup)
    fn set_tx_inclusion(&self, tx_hash: &[u8], block_height: u64) -> Result<()>;
    /// Get block height for a committed transaction
    fn get_tx_block_height(&self, tx_hash: &[u8]) -> Result<Option<u64>>;

    /// Export a bounded set of accounts for bootstrap/state recovery.
    /// Implementations should prefer deterministic ordering.
    fn export_bootstrap_accounts(&self, max_accounts: usize) -> Result<Vec<(Vec<u8>, Account)>> {
        let _ = max_accounts;
        Ok(Vec::new())
    }

    /// Prune (delete) blocks and associated data below the given height.
    /// Called after a monolith block is verified, to free storage for blocks
    /// that are now covered by the monolith commitment.
    /// Returns the number of blocks pruned.
    fn prune_blocks_below(&self, below_height: u64) -> Result<u64> {
        // Default no-op for in-memory storage (eviction handles it)
        let _ = below_height;
        Ok(0)
    }

    /// P2.6-D.1: persist a per-group LatticeBlock chain head.
    /// Stores raw 32-byte block_hash under raw group_id bytes in
    /// the dedicated `lattice_chain_head` column family.
    fn set_lattice_chain_head(
        &self,
        group_id: &str,
        block_hash: &[u8; 32],
    ) -> Result<()>;

    /// P2.6-D.1: read back a per-group chain head, if any.
    /// Returns None when the group has no head yet (genesis) OR
    /// when the underlying storage does not support persistence
    /// (mock/memory-only impl).
    fn get_lattice_chain_head(&self, group_id: &str) -> Result<Option<[u8; 32]>>;

    /// P2.6-D.1: iterate every persisted (group_id, chain_head)
    /// pair. Used by the runtime at boot to restore the in-memory
    /// `last_committed_block_hash` map. Order is implementation-defined
    /// (RocksDB returns lex order on the key bytes).
    fn list_lattice_chain_heads(&self) -> Result<Vec<(String, [u8; 32])>>;

        /// SECURITY (AUDIT-029): Atomically commit all block data in a single batch.
    ///
    /// Default implementation falls back to sequential writes (chain head last).
    /// RocksDB-backed implementations override this to use WriteBatch for true atomicity.
    fn commit_block_batch(&self, data: BlockCommitData) -> Result<()> {
        // Default: sequential writes with chain head as commit point.
        // Multi-group routing: if data.group_id is Some + non-empty, block and
        // chain-head go to that group's lane via the `_in_group` trait methods.
        // Otherwise fall back to the legacy single-lane APIs.
        let group_id = data.group_id.as_deref().unwrap_or("");
        for (address, account) in &data.accounts {
            self.put_account(address, account)?;
        }
        if group_id.is_empty() {
            self.set_block(data.block.height, data.block.clone())?;
        } else {
            self.set_block_in_group(data.block.height, group_id, data.block.clone())?;
        }
        for (key, receipt_data) in &data.receipts {
            self.store_receipt(key, receipt_data.clone())?;
        }
        for (tx_hash, tx_bytes) in &data.transactions {
            self.set_transaction_by_hash(tx_hash, tx_bytes.clone())?;
        }
        for (tx_hash, block_height) in &data.tx_inclusions {
            self.set_tx_inclusion(tx_hash, *block_height)?;
        }
        if group_id.is_empty() {
            self.set_chain_head(&data.block)?;
        } else {
            self.set_chain_head_for_group(group_id, &data.block)?;
        }
        Ok(())
    }
}

/// Combined trait for trait object: both block/account storage and savitri_storage::StorageTrait.
/// Required because Rust allows only one non-auto trait in `dyn A + B`.
pub trait BlockAndAccountStorageTrait:
    BlockAndAccountStorage + savitri_storage::StorageTrait
{
}
impl<T: BlockAndAccountStorage + savitri_storage::StorageTrait> BlockAndAccountStorageTrait for T {}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Database path
    pub db_path: PathBuf,
    /// Memory-only mode (no persistence)
    pub memory_only: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("savitri_lightnode.db"),
            memory_only: false, // Light nodes default to persistent mode
        }
    }
}

const TX_HEIGHT_PREFIX: &[u8] = b"tx_height:";

/// Maximum number of blocks retained in memory (evict oldest beyond this)
const MAX_IN_MEMORY_BLOCKS: usize = 1024;

/// Maximum number of transactions retained in memory
const MAX_IN_MEMORY_TRANSACTIONS: usize = 50_000;

/// Maximum number of tx inclusion entries retained in memory
const MAX_IN_MEMORY_TX_INCLUSIONS: usize = 50_000;

/// Storage implementation for light nodes
#[derive(Debug)]
pub struct Storage {
    /// In-memory storage for accounts
    accounts: Arc<RwLock<HashMap<Vec<u8>, Account>>>,
    /// In-memory storage for blocks
    blocks: Arc<RwLock<HashMap<u64, Block>>>,
    /// In-memory storage for transactions (receipts + tx by hash)
    transactions: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Tx hash -> block_height (for timestamp lookup)
    tx_inclusions: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
    /// In-memory storage for metadata column-family entries.
    metadata: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// In-memory storage for reward balance column-family entries.
    reward_balances: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    /// Configuration
    config: StorageConfig,
}

/// Account structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Account {
    /// Account balance
    pub balance: u128,
    /// Account nonce
    pub nonce: u64,
    /// Account data
    pub data: Vec<u8>,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            balance: 0,
            nonce: 0,
            data: Vec::new(),
        }
    }
}

impl Account {
    /// Credit account with amount
    pub fn credit(&mut self, amount: u128) -> Result<()> {
        self.balance = self
            .balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;
        Ok(())
    }

    /// Debit account with amount
    pub fn debit(&mut self, amount: u128) -> Result<()> {
        self.balance = self
            .balance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Insufficient balance"))?;
        Ok(())
    }
}

impl BlockAndAccountStorage for Storage {
    fn get_block(&self, height: u64) -> Result<Option<Block>> {
        Storage::get_block(self, height)
    }
    fn set_block(&self, height: u64, block: Block) -> Result<()> {
        self.put_block(height, &block)
    }
    fn set_chain_head(&self, block: &Block) -> Result<()> {
        self.set_block(block.height, block.clone())
    }
    fn get_chain_head(&self) -> Result<Option<Block>> {
        Storage::get_chain_head(self)
    }
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>> {
        Storage::get_account(self, address)
    }
    fn put_account(&self, address: &[u8], account: &Account) -> Result<()> {
        Storage::put_account(self, address, account)
    }
    fn store_receipt(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.set_transaction(key, value)
    }
    fn set_transaction_by_hash(&self, tx_hash: &[u8], tx_bytes: Vec<u8>) -> Result<()> {
        self.set_transaction(tx_hash, tx_bytes)
    }
    fn set_tx_inclusion(&self, tx_hash: &[u8], block_height: u64) -> Result<()> {
        let mut incl = self
            .tx_inclusions
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        incl.insert(tx_hash.to_vec(), block_height);
        // Evict when over capacity
        if incl.len() > MAX_IN_MEMORY_TX_INCLUSIONS {
            let to_remove = incl.len() - MAX_IN_MEMORY_TX_INCLUSIONS;
            let keys_to_remove: Vec<Vec<u8>> = incl.keys().take(to_remove).cloned().collect();
            for key in keys_to_remove {
                incl.remove(&key);
            }
        }
        Ok(())
    }
    fn get_tx_block_height(&self, tx_hash: &[u8]) -> Result<Option<u64>> {
        let incl = self
            .tx_inclusions
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        Ok(incl.get(tx_hash).copied())
    }

    fn export_bootstrap_accounts(&self, max_accounts: usize) -> Result<Vec<(Vec<u8>, Account)>> {
        let accounts = self
            .accounts
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let mut entries: Vec<(Vec<u8>, Account)> = accounts
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        if entries.len() > max_accounts {
            entries.truncate(max_accounts);
        }
        Ok(entries)
    }

    fn prune_blocks_below(&self, below_height: u64) -> Result<u64> {
        let mut blocks = self
            .blocks
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let heights_to_remove: Vec<u64> = blocks
            .keys()
            .filter(|&&h| h < below_height)
            .cloned()
            .collect();
        let count = heights_to_remove.len() as u64;
        for h in heights_to_remove {
            blocks.remove(&h);
        }
        Ok(count)
    }

    fn set_lattice_chain_head(
        &self,
        _group_id: &str,
        _block_hash: &[u8; 32],
    ) -> Result<()> {
        // In-memory backend has no persistence — drop silently. The
        // runtime stays correct because the in-memory map already
        // holds the chain head; loss on restart is the same fate
        // as the rest of the in-memory state.
        Ok(())
    }

    fn get_lattice_chain_head(&self, _group_id: &str) -> Result<Option<[u8; 32]>> {
        Ok(None)
    }

    fn list_lattice_chain_heads(&self) -> Result<Vec<(String, [u8; 32])>> {
        Ok(Vec::new())
    }
}

impl Storage {
    /// Create new storage instance
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = StorageConfig {
            db_path: path.as_ref().to_path_buf(),
            memory_only: true,
        };
        Self::with_config(config)
    }

    /// Create storage with configuration
    pub fn with_config(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            accounts: Arc::new(RwLock::new(HashMap::new())),
            blocks: Arc::new(RwLock::new(HashMap::new())),
            transactions: Arc::new(RwLock::new(HashMap::new())),
            tx_inclusions: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            reward_balances: Arc::new(RwLock::new(HashMap::new())),
            config,
        })
    }

    /// Get account by address
    pub fn get_account(&self, address: &[u8]) -> Result<Option<Account>> {
        let accounts = self
            .accounts
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        Ok(accounts.get(address).cloned())
    }

    /// Set account
    pub fn set_account(&self, address: &[u8], account: Account) -> Result<()> {
        let mut accounts = self
            .accounts
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        accounts.insert(address.to_vec(), account);
        Ok(())
    }

    /// Put account (alias for set_account)
    pub fn put_account(&self, address: &[u8], account: &Account) -> Result<()> {
        self.set_account(address, account.clone())
    }

    /// Get chain head (highest block)
    pub fn get_chain_head(&self) -> Result<Option<Block>> {
        let blocks = self
            .blocks
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        if let Some((height, block)) = blocks.iter().max_by_key(|(h, _)| *h) {
            Ok(Some(block.clone()))
        } else {
            Ok(None)
        }
    }

    /// Get block by height
    pub fn get_block(&self, height: u64) -> Result<Option<Block>> {
        let blocks = self
            .blocks
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        Ok(blocks.get(&height).cloned())
    }

    /// Set block (with eviction of oldest blocks beyond MAX_IN_MEMORY_BLOCKS)
    pub fn set_block(&self, height: u64, block: Block) -> Result<()> {
        let mut blocks = self
            .blocks
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        blocks.insert(height, block);
        // Evict oldest blocks if over capacity
        if blocks.len() > MAX_IN_MEMORY_BLOCKS {
            let mut heights: Vec<u64> = blocks.keys().copied().collect();
            heights.sort_unstable();
            let to_remove = blocks.len() - MAX_IN_MEMORY_BLOCKS;
            for &h in heights.iter().take(to_remove) {
                blocks.remove(&h);
            }
        }
        Ok(())
    }

    /// Put block (alias for set_block for API compatibility)
    pub fn put_block(&self, height: u64, block: &Block) -> Result<()> {
        self.set_block(height, block.clone())
    }

    /// Get transaction by hash
    pub fn get_transaction(&self, hash: &[u8]) -> Result<Option<Vec<u8>>> {
        let transactions = self
            .transactions
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        Ok(transactions.get(hash).cloned())
    }

    /// Set transaction (with eviction when over MAX_IN_MEMORY_TRANSACTIONS)
    pub fn set_transaction(&self, hash: &[u8], tx: Vec<u8>) -> Result<()> {
        let mut transactions = self
            .transactions
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        transactions.insert(hash.to_vec(), tx);
        // Evict random entries when over capacity (HashMap has no ordering)
        if transactions.len() > MAX_IN_MEMORY_TRANSACTIONS {
            let to_remove = transactions.len() - MAX_IN_MEMORY_TRANSACTIONS;
            let keys_to_remove: Vec<Vec<u8>> =
                transactions.keys().take(to_remove).cloned().collect();
            for key in keys_to_remove {
                transactions.remove(&key);
            }
        }
        Ok(())
    }

    /// Get block hash by height
    pub fn get_block_hash_by_height(&self, height: u64) -> Result<Option<[u8; 64]>> {
        let blocks = self
            .blocks
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        Ok(blocks.get(&height).map(|b| b.hash))
    }

    /// Ensure account is funded
    pub fn ensure_funded(&self, address: &[u8], amount: u128) -> Result<()> {
        let mut accounts = self
            .accounts
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let account = accounts
            .entry(address.to_vec())
            .or_insert_with(Account::default);
        account.credit(amount)?;
        Ok(())
    }

    /// Set chain head (highest block)
    pub fn set_chain_head(&self, block: &Block) -> Result<()> {
        self.set_block(block.height, block.clone())
    }

    /// Get data from column family (compatibility with RocksDB interface)
    pub fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match cf_name {
            "blocks" => {
                let blocks = self
                    .blocks
                    .read()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                if let Some((height, block)) = blocks
                    .iter()
                    .find(|(h, _)| **h == u64::from_le_bytes(key.try_into().unwrap_or([0; 8])))
                {
                    Ok(Some(bincode::serialize(block)?))
                } else {
                    Ok(None)
                }
            }
            "transactions" => {
                let transactions = self
                    .transactions
                    .read()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                Ok(transactions.get(key).cloned())
            }
            "accounts" => {
                let accounts = self
                    .accounts
                    .read()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                let account = accounts.get(key);
                if let Some(acc) = account {
                    Ok(Some(bincode::serialize(acc)?))
                } else {
                    Ok(None)
                }
            }
            "metadata" => {
                // Handle metadata requests (chain head, etc.)
                if key == b"chain_head" {
                    if let Some(block) = self.get_chain_head()? {
                        Ok(Some(bincode::serialize(&block)?))
                    } else {
                        Ok(None)
                    }
                } else if key.starts_with(b"block_hash:") {
                    let height_str = String::from_utf8_lossy(&key[11..]);
                    if let Ok(height) = height_str.parse::<u64>() {
                        if let Some(block) = self.get_block(height)? {
                            Ok(Some(block.hash.to_vec()))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                } else if key.starts_with(TX_HEIGHT_PREFIX) {
                    let tx_hash = &key[TX_HEIGHT_PREFIX.len()..];
                    let incl = self
                        .tx_inclusions
                        .read()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    Ok(incl.get(tx_hash).map(|h| h.to_le_bytes().to_vec()))
                } else {
                    let metadata = self
                        .metadata
                        .read()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    Ok(metadata.get(key).cloned())
                }
            }
            "reward_balances" => {
                let reward_balances = self
                    .reward_balances
                    .read()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                Ok(reward_balances.get(key).cloned())
            }
            "state" => {
                // Handle state storage (vesting schedules, etc.)
                // For now, return None for state requests
                Ok(None)
            }
            "contracts" => {
                // Handle contract storage
                // For now, return None for contract requests
                Ok(None)
            }
            _ => {
                warn!("Unknown column family: {}", cf_name);
                Ok(None)
            }
        }
    }

    /// Put data in column family (compatibility with RocksDB interface)
    pub fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        match cf_name {
            "blocks" => {
                if value.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Block data too large for put_cf: {} bytes (max {})",
                        value.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                let height = u64::from_le_bytes(key.try_into().unwrap_or([0; 8]));
                let block = bincode::deserialize::<Block>(value)?;
                self.put_block(height, &block)?;
            }
            "transactions" => {
                let mut transactions = self
                    .transactions
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                transactions.insert(key.to_vec(), value.to_vec());
            }
            "accounts" => {
                if value.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Account data too large for put_cf: {} bytes (max {})",
                        value.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                let account = bincode::deserialize::<Account>(value)?;
                self.put_account(key, &account)?;
            }
            "metadata" => {
                if key == b"chain_head" {
                    if value.len() > MAX_DESERIALIZE_SIZE {
                        anyhow::bail!(
                            "Chain head data too large: {} bytes (max {})",
                            value.len(),
                            MAX_DESERIALIZE_SIZE
                        );
                    }
                    let block = bincode::deserialize::<Block>(value)?;
                    self.set_chain_head(&block)?;
                } else if key.starts_with(b"block_hash:") {
                    let height_str = String::from_utf8_lossy(&key[11..]);
                    if let Ok(height) = height_str.parse::<u64>() {
                        if let Some(block) = self.get_block(height)? {
                            self.put_block_hash_for_height(height, &block.hash)?;
                        }
                    }
                } else if key.starts_with(TX_HEIGHT_PREFIX) && value.len() == 8 {
                    let tx_hash = key[TX_HEIGHT_PREFIX.len()..].to_vec();
                    let block_height = u64::from_le_bytes(value.try_into().unwrap_or([0; 8]));
                    let mut incl = self
                        .tx_inclusions
                        .write()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    incl.insert(tx_hash, block_height);
                } else {
                    let mut metadata = self
                        .metadata
                        .write()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    metadata.insert(key.to_vec(), value.to_vec());
                }
            }
            "reward_balances" => {
                let mut reward_balances = self
                    .reward_balances
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                reward_balances.insert(key.to_vec(), value.to_vec());
            }
            _ => {
                warn!("Cannot put in unknown column family: {}", cf_name);
            }
        }
        Ok(())
    }

    /// Delete data from column family
    pub fn delete_cf(&self, cf_name: &str, key: &[u8]) -> Result<()> {
        match cf_name {
            "blocks" => {
                let height = u64::from_le_bytes(key.try_into().unwrap_or([0; 8]));
                let mut blocks = self
                    .blocks
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                blocks.remove(&height);
            }
            "transactions" => {
                let mut transactions = self
                    .transactions
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                transactions.remove(key);
            }
            "accounts" => {
                let mut accounts = self
                    .accounts
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                accounts.remove(key);
            }
            "metadata" => {
                if key == b"chain_head" {
                    self.delete_chain_head()?;
                } else if key.starts_with(b"block_hash:") {
                    let height_str = String::from_utf8_lossy(&key[11..]);
                    if let Ok(height) = height_str.parse::<u64>() {
                        self.delete_block_hash_for_height(height)?;
                    }
                } else if key.starts_with(TX_HEIGHT_PREFIX) {
                    let tx_hash = &key[TX_HEIGHT_PREFIX.len()..];
                    let mut incl = self
                        .tx_inclusions
                        .write()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    incl.remove(tx_hash);
                } else {
                    let mut metadata = self
                        .metadata
                        .write()
                        .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                    metadata.remove(key);
                }
            }
            "reward_balances" => {
                let mut reward_balances = self
                    .reward_balances
                    .write()
                    .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
                reward_balances.remove(key);
            }
            _ => {
                warn!("Cannot delete from unknown column family: {}", cf_name);
            }
        }
        Ok(())
    }

    /// Delete chain head
    fn delete_chain_head(&self) -> Result<()> {
        let blocks = self
            .blocks
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        // Find and remove the highest block
        if let Some((height, _)) = blocks.iter().max_by_key(|(h, _)| *h) {
            let height_copy = *height;
            drop(blocks); // Release immutable borrow
            let mut blocks = self
                .blocks
                .write()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
            blocks.remove(&height_copy);
        }
        Ok(())
    }

    /// Delete block hash for height
    fn delete_block_hash_for_height(&self, height: u64) -> Result<()> {
        // In memory storage, block hash is stored in the block itself
        // This is a no-op for in-memory storage
        Ok(())
    }

    /// Put block hash for height
    fn put_block_hash_for_height(&self, height: u64, hash: &[u8]) -> Result<()> {
        // In memory storage, block hash is stored in the block itself
        // This is a no-op for in-memory storage
        Ok(())
    }
}

// Implement StorageTrait so the real savitri-mempool pipeline can use lightnode Storage
impl savitri_storage::StorageTrait for Storage {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf("default", key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_cf("default", key)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_cf("default", key)
    }

    fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        // Delegate to the inherent method
        Storage::put_cf(self, cf_name, key, value)
    }

    fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Delegate to the inherent method
        Storage::get_cf(self, cf_name, key)
    }

    fn is_healthy(&self) -> bool {
        true // In-memory storage is always healthy
    }

    fn get_account(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        match Storage::get_account(self, address)? {
            Some(account) => Ok(Some(bincode::serialize(&account)?)),
            None => Ok(None),
        }
    }

    fn put_account(&self, address: &[u8], account_data: &[u8]) -> Result<()> {
        if account_data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Account data too large: {} bytes (max {})",
                account_data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let account: Account = bincode::deserialize(account_data)?;
        Storage::put_account(self, address, &account)
    }

    fn iterator_cf(
        &self,
        cf: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        let entries = match cf {
            "accounts" => self
                .accounts
                .read()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?
                .iter()
                .map(|(key, value)| Ok((key.clone(), bincode::serialize(value)?)))
                .collect::<Result<Vec<_>>>()?,
            "blocks" => self
                .blocks
                .read()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?
                .iter()
                .map(|(height, block)| {
                    Ok((height.to_le_bytes().to_vec(), bincode::serialize(block)?))
                })
                .collect::<Result<Vec<_>>>()?,
            "transactions" => self
                .transactions
                .read()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect::<Result<Vec<_>>>()?,
            "metadata" => self
                .metadata
                .read()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect::<Result<Vec<_>>>()?,
            "reward_balances" => self
                .reward_balances
                .read()
                .map_err(|_| anyhow::anyhow!("Storage lock error"))?
                .iter()
                .map(|(key, value)| Ok((key.clone(), value.clone())))
                .collect::<Result<Vec<_>>>()?,
            _ => Vec::new(),
        };

        Ok(Box::new(entries.into_iter().map(Ok)))
    }

    fn scan_cf_prefix(
        &self,
        cf_name: &str,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut matches = self
            .iterator_cf(cf_name)?
            .filter_map(|entry| match entry {
                Ok((key, value)) if key.starts_with(prefix) => Some(Ok((key, value))),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
            .collect::<Result<Vec<_>>>()?;

        matches.sort_by(|a, b| a.0.cmp(&b.0));
        if reverse {
            matches.reverse();
        }
        if matches.len() > limit {
            matches.truncate(limit);
        }
        Ok(matches)
    }
}

/// RocksDB-backed storage for lightnode when memory_only = false (persistence from config).
#[derive(Clone)]
pub struct RocksDBLightnodeStorage {
    inner: Arc<savitri_storage::Storage>,
}

const CF_BLOCKS: &str = "blocks";
const CF_METADATA: &str = "metadata";
const CF_ACCOUNTS: &str = "accounts";
const CF_TRANSACTIONS: &str = "transactions";

impl RocksDBLightnodeStorage {
    pub fn new(inner: Arc<savitri_storage::Storage>) -> Self {
        Self { inner }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config = savitri_storage::StorageConfig {
            path: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        let storage = Arc::new(savitri_storage::Storage::with_config(config)?);
        Ok(Self::new(storage))
    }
}

impl BlockAndAccountStorage for RocksDBLightnodeStorage {
    fn get_block(&self, height: u64) -> Result<Option<Block>> {
        let key = height.to_le_bytes();
        match self.inner.get_cf(CF_BLOCKS, &key)? {
            Some(bytes) => {
                if bytes.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Block data too large: {} bytes (max {})",
                        bytes.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => Ok(None),
        }
    }

    fn set_block(&self, height: u64, block: Block) -> Result<()> {
        let key = height.to_le_bytes();
        self.inner
            .put_cf(CF_BLOCKS, &key, &bincode::serialize(&block)?)?;
        let hash_key = format!("block_hash:{}", height);
        self.inner
            .put_cf(CF_METADATA, hash_key.as_bytes(), &block.hash)?;
        Ok(())
    }

    /// Multi-group variant — routes to the group's lane. Empty group_id falls
    /// back to the legacy single-lane keys (full compat with SINGLE_GROUP).
    fn set_block_in_group(&self, height: u64, group_id: &str, block: Block) -> Result<()> {
        let serialised = bincode::serialize(&block)?;
        self.inner
            .set_block_in_group(height, group_id, &serialised)?;
        // Keep the height→hash metadata key in sync on the same lane.
        self.inner
            .set_block_hash_for_height_in_group(height, group_id, &block.hash)?;
        Ok(())
    }

    fn get_block_in_group(&self, height: u64, group_id: &str) -> Result<Option<Block>> {
        match self.inner.get_block_in_group(height, group_id)? {
            Some(bytes) => {
                if bytes.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Block data too large: {} bytes (max {})",
                        bytes.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => Ok(None),
        }
    }

    fn set_chain_head(&self, block: &Block) -> Result<()> {
        self.inner
            .put_cf(CF_METADATA, b"chain_head", &bincode::serialize(block)?)
    }

    fn set_chain_head_for_group(&self, group_id: &str, block: &Block) -> Result<()> {
        self.inner
            .set_chain_head_for_group(group_id, &bincode::serialize(block)?)
    }

    fn get_chain_head_for_group(&self, group_id: &str) -> Result<Option<Block>> {
        match self.inner.get_chain_head_for_group(group_id)? {
            Some(bytes) => {
                if bytes.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Chain head data too large: {} bytes (max {})",
                        bytes.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                Ok(Some(bincode::deserialize::<Block>(&bytes)?))
            }
            None => Ok(None),
        }
    }

    fn get_chain_head(&self) -> Result<Option<Block>> {
        let metadata_head = match self.inner.get_cf(CF_METADATA, b"chain_head")? {
            Some(bytes) => {
                if bytes.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Chain head data too large: {} bytes (max {})",
                        bytes.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                Some(bincode::deserialize::<Block>(&bytes)?)
            }
            None => None,
        };

        // Fast path: valid non-genesis metadata head.
        if let Some(head) = metadata_head.as_ref() {
            if head.height > 0 {
                return Ok(Some(head.clone()));
            }
        }

        // Recovery path: metadata head is missing or stuck at genesis.
        // Scan blocks CF and pick the highest height to avoid false "genesis-only" state.
        let mut max_block: Option<Block> = None;
        for entry in self.inner.iterator_cf(CF_BLOCKS)? {
            let (key, value) = match entry {
                Ok(item) => item,
                Err(err) => {
                    warn!(error = %err, "Failed to iterate blocks CF while recovering chain head");
                    continue;
                }
            };
            if key.len() != 8 {
                continue;
            }
            let height = u64::from_le_bytes(key.as_slice().try_into().unwrap_or([0; 8]));
            if value.len() > MAX_DESERIALIZE_SIZE {
                warn!(
                    height,
                    size = value.len(),
                    "Block data too large while recovering chain head, skipping"
                );
                continue;
            }
            let block: Block = match bincode::deserialize(&value) {
                Ok(b) => b,
                Err(err) => {
                    warn!(height, error = %err, "Failed to deserialize block while recovering chain head");
                    continue;
                }
            };
            match &max_block {
                Some(current) if current.height >= height => {}
                _ => max_block = Some(block),
            }
        }

        let best_head = match (metadata_head, max_block) {
            (Some(meta), Some(scanned)) if scanned.height > meta.height => Some(scanned),
            (Some(meta), _) => Some(meta),
            (None, scanned) => scanned,
        };

        // A crash during partial commit may leave the highest block stored without its
        // account effects being fully committed. Walk backwards from the candidate head
        // to find the last block whose parent_hash chain is unbroken down to genesis.
        let verified_head = if let Some(candidate) = best_head {
            if candidate.height == 0 {
                // Genesis block — nothing to verify.
                Some(candidate)
            } else {
                let candidate_height = candidate.height;

                // Walk backwards from the candidate, tracking the top of the
                // current unbroken chain segment.  When we hit a break we
                // reset `chain_top` to the parent (below the break) and keep
                // walking.  When we reach height 0 the value of `chain_top`
                // is the highest block that has a continuous parent_hash
                // chain all the way down.
                let mut chain_top = candidate.clone();
                let mut current = candidate;
                let mut found_break = false;

                while current.height > 0 {
                    let parent_height = current.height - 1;

                    let parent = match self.get_block(parent_height) {
                        Ok(Some(p)) => p,
                        Ok(None) => {
                            warn!(
                                height = current.height,
                                missing_parent_height = parent_height,
                                "AUDIT-032: Parent block missing during chain-head recovery"
                            );
                            found_break = true;
                            break;
                        }
                        Err(err) => {
                            warn!(
                                height = current.height,
                                error = %err,
                                "AUDIT-032: Error reading parent block during recovery"
                            );
                            found_break = true;
                            break;
                        }
                    };

                    if current.parent_hash != parent.hash {
                        // Chain break: current block does not link to its stored parent.
                        warn!(
                            height = current.height,
                            parent_height,
                            "AUDIT-032: Chain continuity broken — block parent_hash \
                             does not match parent block hash"
                        );
                        found_break = true;
                        // The parent itself might still anchor a valid sub-chain.
                        // Reset chain_top to the parent and keep verifying downward.
                        chain_top = parent.clone();
                        current = parent;
                        continue;
                    }

                    // Link is valid.  If this is the first valid link after a
                    // break we already set chain_top above; otherwise chain_top
                    // was set when we entered this branch or after the last break.
                    current = parent;
                }

                // If we broke out early (missing/unreadable parent) the chain
                // is only valid up to the block *below* the break, which we
                // never reached.  Fall back to genesis.
                if found_break && current.height > 0 {
                    // We could not verify all the way down to genesis.
                    match self.get_block(0) {
                        Ok(Some(genesis)) => {
                            chain_top = genesis;
                        }
                        _ => {
                            // Nothing we can do — keep whatever chain_top we have.
                        }
                    }
                }

                // If a break was detected, clean up and log.
                if found_break {
                    warn!(
                        original_height = candidate_height,
                        rolled_back_to = chain_top.height,
                        "AUDIT-032: Rolled back chain head to last consistent block"
                    );

                    // Remove orphaned blocks above the consistent head so they
                    // do not confuse future recovery attempts.
                    for orphan_h in (chain_top.height + 1)..=candidate_height {
                        let key = orphan_h.to_le_bytes();
                        if let Err(err) = self.inner.delete_cf(CF_BLOCKS, &key) {
                            warn!(
                                height = orphan_h,
                                error = %err,
                                "AUDIT-032: Failed to remove orphaned block \
                                 during recovery cleanup"
                            );
                        }
                    }
                }

                Some(chain_top)
            }
        } else {
            None
        };

        // Self-heal metadata if scanned head is better or metadata was absent.
        if let Some(ref head) = verified_head {
            if self.set_chain_head(head).is_ok() {
                info!(
                    height = head.height,
                    "Recovered and updated chain head metadata from blocks CF"
                );
            }
        }

        Ok(verified_head)
    }

    fn get_account(&self, address: &[u8]) -> Result<Option<Account>> {
        match self.inner.get_account(address)? {
            Some(bytes) => {
                if bytes.len() > MAX_DESERIALIZE_SIZE {
                    anyhow::bail!(
                        "Account data too large: {} bytes (max {})",
                        bytes.len(),
                        MAX_DESERIALIZE_SIZE
                    );
                }
                Ok(Some(bincode::deserialize(&bytes)?))
            }
            None => Ok(None),
        }
    }

    fn put_account(&self, address: &[u8], account: &Account) -> Result<()> {
        self.inner
            .put_account(address, &bincode::serialize(account)?)
    }

    fn store_receipt(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let prefixed: Vec<u8> = [b"receipt/".as_slice(), key].concat();
        self.inner.put(&prefixed, &value)
    }
    fn set_transaction_by_hash(&self, tx_hash: &[u8], tx_bytes: Vec<u8>) -> Result<()> {
        self.inner.put_cf(CF_TRANSACTIONS, tx_hash, &tx_bytes)
    }
    fn set_tx_inclusion(&self, tx_hash: &[u8], block_height: u64) -> Result<()> {
        let key: Vec<u8> = [b"tx_height:".as_slice(), tx_hash].concat();
        self.inner
            .put_cf(CF_METADATA, &key, &block_height.to_le_bytes())
    }
    fn get_tx_block_height(&self, tx_hash: &[u8]) -> Result<Option<u64>> {
        let key: Vec<u8> = [b"tx_height:".as_slice(), tx_hash].concat();
        match self.inner.get_cf(CF_METADATA, &key)? {
            Some(bytes) if bytes.len() == 8 => {
                Ok(Some(u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))))
            }
            _ => Ok(None),
        }
    }

    fn export_bootstrap_accounts(&self, max_accounts: usize) -> Result<Vec<(Vec<u8>, Account)>> {
        let mut out = Vec::new();
        for entry in self.inner.iterator_cf(CF_ACCOUNTS)? {
            let (key, value) = match entry {
                Ok(item) => item,
                Err(err) => {
                    warn!(error = %err, "Failed to iterate accounts CF for bootstrap export");
                    continue;
                }
            };
            if value.len() > MAX_DESERIALIZE_SIZE {
                continue;
            }
            let account: Account = match bincode::deserialize(&value) {
                Ok(acc) => acc,
                Err(_) => continue,
            };
            out.push((key, account));
            if out.len() >= max_accounts {
                break;
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Prune blocks below the given height from CF_BLOCKS.
    /// Called after a monolith block is verified, freeing storage for blocks
    /// now covered by the monolith commitment.
    ///
    /// Note: TX data in CF_TRANSACTIONS is not pruned here (TXs are keyed by
    /// hash, not height, and may still be needed for RPC lookups). A separate
    /// tx-pruning pass can be added later if storage pressure warrants it.
    fn prune_blocks_below(&self, below_height: u64) -> Result<u64> {
        use tracing::{info, warn};
        let mut pruned: u64 = 0;

        for height in 0..below_height {
            let key = height.to_le_bytes();

            // Check if block exists before attempting delete
            match self.inner.get_cf(CF_BLOCKS, &key) {
                Ok(Some(_)) => {
                    if let Err(err) = self.inner.delete_cf(CF_BLOCKS, &key) {
                        warn!(height, error = %err, "Failed to prune block");
                    } else {
                        let hash_key = format!("block_hash:{}", height);
                        if let Err(err) = self.inner.delete_cf(CF_METADATA, hash_key.as_bytes()) {
                            warn!(height, error = %err, "Failed to prune block hash index");
                        }
                        pruned += 1;
                    }
                }
                _ => {
                    // Block already absent at this height, skip
                }
            }
        }

        if pruned > 0 {
            info!(
                pruned_blocks = pruned,
                below_height, "Post-monolith block pruning completed"
            );
        }

        Ok(pruned)
    }

    /// SECURITY (AUDIT-029): Atomic block commit using RocksDB WriteBatch.
    ///
    /// All writes (accounts, block, receipts, tx indices, chain head) are batched
    /// into a single RocksDB WriteBatch and committed atomically. Either ALL writes
    /// succeed or NONE are applied, preventing crash-induced state inconsistency.
    ///
    /// Multi-group: when `data.group_id` is `Some` + non-empty, block + chain_head
    /// + block_hash:<h> use composite keys so different groups never clobber each
    /// other at the same physical height. When `None`/empty, legacy single-lane
    /// keys are used (SAVITRI_FORCE_SINGLE_GROUP path and backward-compat reads).
    fn commit_block_batch(&self, data: BlockCommitData) -> Result<()> {
        let mut batch = self.inner.begin_batch();

        // 1. Account state changes
        // Use DEFAULT CF with "account:{hex}" key format, matching put_account()/get_account()
        // which use Storage::put()/get() on the default column family.
        for (address, account) in &data.accounts {
            let account_data = bincode::serialize(account)?;
            let acct_key = format!("account:{}", hex::encode(address));
            batch.put(acct_key.as_bytes(), &account_data)?;
        }

        let group_id = data.group_id.as_deref().unwrap_or("");

        // 2. Block data — routed per group when group_id is non-empty.
        let block_key = savitri_storage::storage::build_block_key(data.block.height, group_id);
        let block_data = bincode::serialize(&data.block)?;
        batch.put_cf(CF_BLOCKS, &block_key, &block_data)?;
        let block_hash_key =
            savitri_storage::storage::build_block_hash_key(data.block.height, group_id);
        batch.put_cf(CF_METADATA, block_hash_key.as_bytes(), &data.block.hash)?;

        // 3. Receipts
        for (key, receipt_data) in &data.receipts {
            let prefixed: Vec<u8> = [b"receipt/".as_slice(), key.as_slice()].concat();
            batch.put(&prefixed, receipt_data)?;
        }

        // 4. Transaction indices
        for (tx_hash, tx_bytes) in &data.transactions {
            batch.put_cf(CF_TRANSACTIONS, tx_hash, tx_bytes)?;
        }
        for (tx_hash, block_height) in &data.tx_inclusions {
            let incl_key: Vec<u8> = [b"tx_height:".as_slice(), tx_hash.as_slice()].concat();
            batch.put_cf(CF_METADATA, &incl_key, &block_height.to_le_bytes())?;
        }

        // 5. Chain head (commit point — included in same atomic batch)
        // Written per-group AND to the legacy `chain_head` key so observability
        // (net_peerCount/chain_getBlockHeight) keeps advancing under multi-group.
        // The legacy key reflects whichever group last committed — imprecise but
        // "chain height is moving" is the operator-facing signal; use
        // chain_getGroupHeights (P3) when per-group detail is needed.
        let chain_head_data = bincode::serialize(&data.block)?;
        let chain_head_key = savitri_storage::storage::build_chain_head_key(group_id);
        batch.put_cf(CF_METADATA, &chain_head_key, &chain_head_data)?;
        if !group_id.is_empty() {
            batch.put_cf(CF_METADATA, b"chain_head", &chain_head_data)?;
        }

        // Atomic commit — all-or-nothing
        batch.commit()
    }

    fn set_lattice_chain_head(
        &self,
        group_id: &str,
        block_hash: &[u8; 32],
    ) -> Result<()> {
        use savitri_storage::storage::CF_LATTICE_CHAIN_HEAD;
        self.inner
            .put_cf(CF_LATTICE_CHAIN_HEAD, group_id.as_bytes(), block_hash)
            .map_err(|e| anyhow::anyhow!("set_lattice_chain_head: {}", e))
    }

    fn get_lattice_chain_head(&self, group_id: &str) -> Result<Option<[u8; 32]>> {
        use savitri_storage::storage::CF_LATTICE_CHAIN_HEAD;
        let raw = self
            .inner
            .get_cf(CF_LATTICE_CHAIN_HEAD, group_id.as_bytes())
            .map_err(|e| anyhow::anyhow!("get_lattice_chain_head: {}", e))?;
        Ok(raw.and_then(|v| {
            if v.len() == 32 {
                let mut out = [0u8; 32];
                out.copy_from_slice(&v);
                Some(out)
            } else {
                None
            }
        }))
    }

    fn list_lattice_chain_heads(&self) -> Result<Vec<(String, [u8; 32])>> {
        use savitri_storage::storage::CF_LATTICE_CHAIN_HEAD;
        let iter = self
            .inner
            .iterator_cf(CF_LATTICE_CHAIN_HEAD)
            .map_err(|e| anyhow::anyhow!("list_lattice_chain_heads iter open: {}", e))?;
        let mut out: Vec<(String, [u8; 32])> = Vec::new();
        for item in iter {
            let (k, v) = item
                .map_err(|e| anyhow::anyhow!("list_lattice_chain_heads iter item: {}", e))?;
            if v.len() != 32 {
                continue;
            }
            let group_id = match String::from_utf8(k) {
                Ok(g) => g,
                Err(_) => continue,
            };
            let mut h = [0u8; 32];
            h.copy_from_slice(&v);
            out.push((group_id, h));
        }
        Ok(out)
    }
}

impl std::fmt::Debug for RocksDBLightnodeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksDBLightnodeStorage")
            .finish_non_exhaustive()
    }
}

impl savitri_storage::StorageTrait for RocksDBLightnodeStorage {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put_cf(cf_name, key, value)
    }

    fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_cf(cf_name, key)
    }

    fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    fn get_account(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_account(address)
    }

    fn put_account(&self, address: &[u8], account_data: &[u8]) -> Result<()> {
        self.inner.put_account(address, account_data)
    }

    fn iterator_cf(
        &self,
        cf: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>> {
        self.inner.iterator_cf(cf)
    }

    fn scan_cf_prefix(
        &self,
        cf_name: &str,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_cf_prefix(cf_name, prefix, limit, reverse)
    }
}

/// Storage snapshot for rollback
#[derive(Debug, Clone)]
pub struct StorageSnapshot {
    /// Serialized accounts data
    pub accounts_data: Vec<u8>,
    /// Serialized blocks data  
    pub blocks_data: Vec<u8>,
    /// Serialized transactions data
    pub transactions_data: Vec<u8>,
    /// Snapshot timestamp
    pub timestamp: u64,
}

impl Storage {
    /// Create snapshot of current storage state
    pub fn snapshot(&self) -> Result<StorageSnapshot> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("Failed to get timestamp: {}", e))?
            .as_secs();

        // Serialize accounts
        let accounts = self
            .accounts
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let accounts_data = bincode::serialize(&*accounts)
            .map_err(|e| anyhow::anyhow!("Failed to serialize accounts: {}", e))?;
        drop(accounts);

        // Serialize blocks
        let blocks = self
            .blocks
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let blocks_data = bincode::serialize(&*blocks)
            .map_err(|e| anyhow::anyhow!("Failed to serialize blocks: {}", e))?;
        drop(blocks);

        // Serialize transactions
        let transactions = self
            .transactions
            .read()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        let transactions_data = bincode::serialize(&*transactions)
            .map_err(|e| anyhow::anyhow!("Failed to serialize transactions: {}", e))?;
        drop(transactions);

        info!(
            accounts_count = accounts_data.len(),
            blocks_count = blocks_data.len(),
            transactions_count = transactions_data.len(),
            timestamp = timestamp,
            "Created storage snapshot"
        );

        Ok(StorageSnapshot {
            accounts_data,
            blocks_data,
            transactions_data,
            timestamp,
        })
    }

    /// Restore storage from snapshot
    pub fn restore(&mut self, snapshot: StorageSnapshot) -> Result<()> {
        info!(
            timestamp = snapshot.timestamp,
            "Restoring storage from snapshot"
        );

        const MAX_SNAPSHOT_SIZE: usize = 64 * 1024 * 1024;

        // Restore accounts
        if snapshot.accounts_data.len() > MAX_SNAPSHOT_SIZE {
            anyhow::bail!(
                "Snapshot accounts data too large: {} bytes (max {})",
                snapshot.accounts_data.len(),
                MAX_SNAPSHOT_SIZE
            );
        }
        let accounts: HashMap<Vec<u8>, Account> = bincode::deserialize(&snapshot.accounts_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize accounts: {}", e))?;
        let mut accounts_lock = self
            .accounts
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        *accounts_lock = accounts;
        drop(accounts_lock);

        // Restore blocks
        if snapshot.blocks_data.len() > MAX_SNAPSHOT_SIZE {
            anyhow::bail!(
                "Snapshot blocks data too large: {} bytes (max {})",
                snapshot.blocks_data.len(),
                MAX_SNAPSHOT_SIZE
            );
        }
        let blocks: HashMap<u64, Block> = bincode::deserialize(&snapshot.blocks_data)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize blocks: {}", e))?;
        let mut blocks_lock = self
            .blocks
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        *blocks_lock = blocks;
        drop(blocks_lock);

        // Restore transactions
        if snapshot.transactions_data.len() > MAX_SNAPSHOT_SIZE {
            anyhow::bail!(
                "Snapshot transactions data too large: {} bytes (max {})",
                snapshot.transactions_data.len(),
                MAX_SNAPSHOT_SIZE
            );
        }
        let transactions: HashMap<Vec<u8>, Vec<u8>> =
            bincode::deserialize(&snapshot.transactions_data)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize transactions: {}", e))?;
        let mut transactions_lock = self
            .transactions
            .write()
            .map_err(|_| anyhow::anyhow!("Storage lock error"))?;
        *transactions_lock = transactions;
        drop(transactions_lock);

        info!(
            timestamp = snapshot.timestamp,
            "Storage restore completed successfully"
        );

        Ok(())
    }
}
