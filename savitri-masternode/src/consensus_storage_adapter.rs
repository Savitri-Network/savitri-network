//! Persistent consensus storage adapter using savitri-storage (RocksDB).
//! Implements savitri_consensus::Storage for production use.

#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;
use savitri_consensus::error::Result as ConsensusResult;
use savitri_consensus::traits::storage::{Storage, StorageStats};
use savitri_consensus::types::*;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

const CF_BLOCKS: &str = "blocks";
const CF_TRANSACTIONS: &str = "transactions";
const CF_STATE: &str = "state";
const CF_METADATA: &str = "metadata";

const KEY_CONSENSUS_STATE: &[u8] = b"consensus_state";
const KEY_LATEST_HEIGHT: &[u8] = b"latest_height";
const PREFIX_HEIGHT: &[u8] = b"height:";
const PREFIX_VALIDATOR: &[u8] = b"validator:";
const PREFIX_PROPOSAL: &[u8] = b"proposal:";
const PREFIX_GROUP: &[u8] = b"group:";
const PREFIX_SCORE: &[u8] = b"score:";
const PREFIX_ACCOUNT: &[u8] = b"account:";
const PREFIX_CONTRACT: &[u8] = b"contract:";

/// Persistent consensus storage backed by savitri-storage (RocksDB).
#[derive(Clone)]
pub struct ConsensusStorageAdapter {
    inner: Arc<savitri_storage::Storage>,
    path: std::path::PathBuf,
}

impl ConsensusStorageAdapter {
    pub fn new(inner: Arc<savitri_storage::Storage>, path: impl AsRef<Path>) -> Self {
        Self {
            inner,
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let path_str = path.to_string_lossy().to_string();
        let config = savitri_storage::StorageConfig {
            path: path_str,
            ..Default::default()
        };
        let inner = Arc::new(savitri_storage::Storage::with_config(config)?);
        Ok(Self { inner, path })
    }

    pub fn persist_latest_height(&self, height: u64) -> Result<()> {
        self.inner
            .put_cf(CF_METADATA, KEY_LATEST_HEIGHT, &height.to_le_bytes())
    }

    pub fn persist_certified_block(
        &self,
        height: u64,
        block_hash: &[u8; 64],
        block_bytes: &[u8],
    ) -> Result<()> {
        self.inner.put_cf(CF_BLOCKS, block_hash, block_bytes)?;
        self.inner
            .put_cf(CF_METADATA, &Self::block_hash_key(height), block_hash)?;
        self.inner
            .put_cf(CF_METADATA, KEY_LATEST_HEIGHT, &height.to_le_bytes())?;
        Ok(())
    }

    fn block_hash_key(height: u64) -> Vec<u8> {
        [PREFIX_HEIGHT, &height.to_le_bytes()[..]].concat()
    }

    fn run_sync<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&savitri_storage::Storage) -> T,
    {
        tokio::task::block_in_place(|| f(self.inner.as_ref()))
    }
}

#[async_trait]
impl Storage for ConsensusStorageAdapter {
    async fn store_block(&self, block: &Block) -> ConsensusResult<()> {
        let hash = block.hash();
        let height = block.header.height;
        let serialized = bincode::serialize(block)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let path = self.path.clone();
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner.put_cf(CF_BLOCKS, &hash, &serialized)?;
            inner.put_cf(CF_METADATA, &Self::block_hash_key(height), &hash)?;
            inner.put_cf(CF_METADATA, KEY_LATEST_HEIGHT, &height.to_le_bytes())?;
            Ok::<(), anyhow::Error>(())
        })
        .map_err(|e: anyhow::Error| {
            savitri_consensus::ConsensusError::StorageError(e.to_string())
        })?;
        Ok(())
    }

    async fn get_block(&self, hash: &[u8]) -> ConsensusResult<Option<Block>> {
        let hash = hash.to_vec();
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_BLOCKS, &hash)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let block = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(block)
    }

    async fn get_block_by_height(&self, height: u64) -> ConsensusResult<Option<Block>> {
        let inner = self.inner.clone();
        let key = Self::block_hash_key(height);
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        match opt {
            Some(hash) => self.get_block(&hash).await,
            None => Ok(None),
        }
    }

    async fn get_latest_block(&self) -> ConsensusResult<Option<Block>> {
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, KEY_LATEST_HEIGHT)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        match opt {
            Some(bytes) if bytes.len() >= 8 => {
                let height = u64::from_le_bytes(bytes[..8].try_into().map_err(|_| {
                    savitri_consensus::ConsensusError::StorageError(
                        "Invalid block height encoding in latest_block metadata".to_string(),
                    )
                })?);
                self.get_block_by_height(height).await
            }
            _ => Ok(None),
        }
    }

    async fn store_consensus_state(&self, state: &ConsensusState) -> ConsensusResult<()> {
        let serialized = bincode::serialize(state)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_METADATA, KEY_CONSENSUS_STATE, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_consensus_state(&self) -> ConsensusResult<Option<ConsensusState>> {
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, KEY_CONSENSUS_STATE)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let state = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(state)
    }

    async fn store_validator(&self, validator: &ValidatorInfo) -> ConsensusResult<()> {
        let key = [PREFIX_VALIDATOR, validator.validator_id.as_bytes()].concat();
        let serialized = bincode::serialize(validator)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_METADATA, &key, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_validator(&self, validator_id: &str) -> ConsensusResult<Option<ValidatorInfo>> {
        let key = [PREFIX_VALIDATOR, validator_id.as_bytes()].concat();
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let v = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(v)
    }

    async fn get_active_validators(&self) -> ConsensusResult<Vec<ValidatorInfo>> {
        let validators = self.run_sync(|inner| {
            let mut out = Vec::new();
            if let Ok(iter) = inner.iterator_cf(CF_METADATA) {
                for item in iter {
                    if let Ok((k, v)) = item {
                        if k.starts_with(PREFIX_VALIDATOR) {
                            if let Ok(info) = bincode::deserialize::<ValidatorInfo>(&v) {
                                if matches!(info.status, ValidatorStatus::Active) {
                                    out.push(info);
                                }
                            }
                        }
                    }
                }
            }
            out
        });
        Ok(validators)
    }

    async fn store_proposal(
        &self,
        proposal: &dyn savitri_consensus::types::Proposal,
    ) -> ConsensusResult<()> {
        let proposal_id = format!("{}-{}", proposal.round_id(), proposal.height());
        let key = [PREFIX_PROPOSAL, proposal_id.as_bytes()].concat();
        let boxed = proposal.clone_box();
        let bp = boxed
            .as_any()
            .downcast_ref::<savitri_consensus::types::BlockProposal>()
            .ok_or_else(|| {
                savitri_consensus::ConsensusError::StorageError(
                    "Only BlockProposal can be persisted".to_string(),
                )
            })?;
        let serialized = bincode::serialize(bp)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_METADATA, &key, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_proposal(
        &self,
        proposal_id: &str,
    ) -> ConsensusResult<Option<Box<dyn savitri_consensus::types::Proposal>>> {
        let key = [PREFIX_PROPOSAL, proposal_id.as_bytes()].concat();
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let prop = match opt {
            Some(bytes) => {
                let bp: savitri_consensus::types::BlockProposal = bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
                Some(Box::new(bp) as Box<dyn savitri_consensus::types::Proposal>)
            }
            None => None,
        };
        Ok(prop)
    }

    async fn store_transaction(&self, tx: &Transaction) -> ConsensusResult<()> {
        let key = tx.hash.0.as_slice();
        let serialized = bincode::serialize(tx)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_TRANSACTIONS, key, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_transaction(&self, hash: &[u8]) -> ConsensusResult<Option<Transaction>> {
        let inner = self.inner.clone();
        let hash = hash.to_vec();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_TRANSACTIONS, &hash)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let tx = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(tx)
    }

    async fn store_account_state(&self, address: &[u8], state: &[u8]) -> ConsensusResult<()> {
        let key = [PREFIX_ACCOUNT, address].concat();
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_STATE, &key, state)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_account_state(&self, address: &[u8]) -> ConsensusResult<Option<Vec<u8>>> {
        let key = [PREFIX_ACCOUNT, address].concat();
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_STATE, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })
    }

    async fn store_contract_state(&self, address: &[u8], state: &[u8]) -> ConsensusResult<()> {
        let key = [PREFIX_CONTRACT, address].concat();
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_STATE, &key, state)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_contract_state(&self, address: &[u8]) -> ConsensusResult<Option<Vec<u8>>> {
        let key = [PREFIX_CONTRACT, address].concat();
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_STATE, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })
    }

    async fn store_group(&self, group: &GroupInfo) -> ConsensusResult<()> {
        let key = [PREFIX_GROUP, group.group_id.as_bytes()].concat();
        let serialized = bincode::serialize(group)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_METADATA, &key, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_group(&self, group_id: &str) -> ConsensusResult<Option<GroupInfo>> {
        let key = [PREFIX_GROUP, group_id.as_bytes()].concat();
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let g = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(g)
    }

    async fn get_active_groups(&self) -> ConsensusResult<Vec<GroupInfo>> {
        let groups = self.run_sync(|inner| {
            let mut out = Vec::new();
            if let Ok(iter) = inner.iterator_cf(CF_METADATA) {
                for item in iter {
                    if let Ok((k, v)) = item {
                        if k.starts_with(PREFIX_GROUP) {
                            if let Ok(info) = bincode::deserialize::<GroupInfo>(&v) {
                                if matches!(info.status, GroupStatus::Active) {
                                    out.push(info);
                                }
                            }
                        }
                    }
                }
            }
            out
        });
        Ok(groups)
    }

    async fn store_score(&self, node_id: &str, score: &PouScoreResult) -> ConsensusResult<()> {
        let key = [PREFIX_SCORE, node_id.as_bytes()].concat();
        let serialized = bincode::serialize(score)
            .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
        let inner = self.inner.clone();
        tokio::task::block_in_place(move || {
            inner
                .put_cf(CF_METADATA, &key, &serialized)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        Ok(())
    }

    async fn get_score(&self, node_id: &str) -> ConsensusResult<Option<PouScoreResult>> {
        let key = [PREFIX_SCORE, node_id.as_bytes()].concat();
        let inner = self.inner.clone();
        let opt = tokio::task::block_in_place(move || {
            inner
                .get_cf(CF_METADATA, &key)
                .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
        })?;
        let s = match opt {
            Some(bytes) => Some(
                bincode::deserialize(&bytes)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?,
            ),
            None => None,
        };
        Ok(s)
    }

    fn stats(&self) -> StorageStats {
        let (total_blocks, total_transactions, total_validators, total_groups) =
            self.run_sync(|inner| {
                let mut blocks = 0u64;
                let mut txs = 0u64;
                let mut vals = 0u64;
                let mut grps = 0u64;
                if let Ok(iter) = inner.iterator_cf(CF_BLOCKS) {
                    for _ in iter {
                        blocks += 1;
                    }
                }
                if let Ok(iter) = inner.iterator_cf(CF_TRANSACTIONS) {
                    for _ in iter {
                        txs += 1;
                    }
                }
                if let Ok(iter) = inner.iterator_cf(CF_METADATA) {
                    for item in iter {
                        if let Ok((k, _)) = item {
                            if k.starts_with(PREFIX_VALIDATOR) {
                                vals += 1;
                            } else if k.starts_with(PREFIX_GROUP) {
                                grps += 1;
                            }
                        }
                    }
                }
                (blocks, txs, vals, grps)
            });
        let storage_size =
            self.run_sync(|_| std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0));
        StorageStats {
            total_blocks,
            total_transactions,
            total_validators,
            total_groups,
            storage_size_bytes: storage_size,
            last_update_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            read_operations: 0,
            write_operations: 0,
            avg_read_time_us: 0.0,
            avg_write_time_us: 0.0,
        }
    }

    async fn is_healthy(&self) -> bool {
        self.inner.is_healthy()
    }

    async fn backup(&self, backup_path: &str) -> ConsensusResult<()> {
        let from = self.path.clone();
        let to = std::path::PathBuf::from(backup_path);
        tokio::task::block_in_place(move || {
            if from.is_dir() {
                copy_dir_all(&from, &to)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))
            } else {
                std::fs::create_dir_all(to.parent().unwrap_or(std::path::Path::new(".")))?;
                std::fs::copy(&from, &to)
                    .map_err(|e| savitri_consensus::ConsensusError::StorageError(e.to_string()))?;
                Ok(())
            }
        })
    }

    async fn restore(&self, backup_path: &str) -> ConsensusResult<()> {
        Err(savitri_consensus::ConsensusError::StorageError(
            "Restore requires reopening storage; not supported in-place".to_string(),
        )
        .into())
    }

    async fn compact(&self) -> ConsensusResult<()> {
        // savitri_storage may not expose compact; no-op is safe
        Ok(())
    }

    async fn get_storage_size(&self) -> ConsensusResult<u64> {
        let path = self.path.clone();
        let size = tokio::task::block_in_place(move || {
            if path.is_dir() {
                dir_size(&path).unwrap_or(0)
            } else {
                std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
            }
        });
        Ok(size)
    }
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let p = entry.path();
        if ty.is_dir() {
            total += dir_size(&p)?;
        } else {
            total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
        }
    }
    Ok(total)
}
