//! Storage trait and related types
//!
//! This module defines the storage interface that all consensus implementations
//! must provide for persisting blockchain and consensus data.

use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::sync::Arc;

/// Storage trait for consensus operations
#[async_trait]
pub trait Storage: Send + Sync {
    /// Store a block
    async fn store_block(&self, block: &Block) -> Result<()>;

    /// Get a block
    async fn get_block(&self, hash: &[u8]) -> Result<Option<Block>>;

    /// Get a block by height
    async fn get_block_by_height(&self, height: u64) -> Result<Option<Block>>;

    /// Get the latest block
    async fn get_latest_block(&self) -> Result<Option<Block>>;

    /// Store consensus state
    async fn store_consensus_state(&self, state: &ConsensusState) -> Result<()>;

    /// Get consensus state
    async fn get_consensus_state(&self) -> Result<Option<ConsensusState>>;

    async fn store_validator(&self, validator: &ValidatorInfo) -> Result<()>;

    async fn get_validator(&self, validator_id: &str) -> Result<Option<ValidatorInfo>>;

    async fn get_active_validators(&self) -> Result<Vec<ValidatorInfo>>;

    /// Store proposal
    async fn store_proposal(&self, proposal: &dyn Proposal) -> Result<()>;

    /// Get proposal
    async fn get_proposal(&self, proposal_id: &str) -> Result<Option<Box<dyn Proposal>>>;

    /// Store transaction
    async fn store_transaction(&self, tx: &Transaction) -> Result<()>;

    /// Get transaction
    async fn get_transaction(&self, hash: &[u8]) -> Result<Option<Transaction>>;

    /// Store account state
    async fn store_account_state(&self, address: &[u8], state: &[u8]) -> Result<()>;

    /// Get account state
    async fn get_account_state(&self, address: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Store contract state
    async fn store_contract_state(&self, address: &[u8], state: &[u8]) -> Result<()>;

    /// Get contract state
    async fn get_contract_state(&self, address: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Store group information
    async fn store_group(&self, group: &GroupInfo) -> Result<()>;

    /// Get group information
    async fn get_group(&self, group_id: &str) -> Result<Option<GroupInfo>>;

    /// Get active groups
    async fn get_active_groups(&self) -> Result<Vec<GroupInfo>>;

    /// Store score information
    async fn store_score(&self, node_id: &str, score: &PouScoreResult) -> Result<()>;

    /// Get score information
    async fn get_score(&self, node_id: &str) -> Result<Option<PouScoreResult>>;

    /// Get storage statistics
    fn stats(&self) -> StorageStats;

    /// Check if storage is healthy
    async fn is_healthy(&self) -> bool;

    /// Backup storage
    async fn backup(&self, backup_path: &str) -> Result<()>;

    /// Restore from backup
    async fn restore(&self, backup_path: &str) -> Result<()>;

    /// Compact storage
    async fn compact(&self) -> Result<()>;

    /// Get storage size
    async fn get_storage_size(&self) -> Result<u64>;
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Total blocks stored
    pub total_blocks: u64,
    /// Total transactions stored
    pub total_transactions: u64,
    pub total_validators: u64,
    /// Total groups stored
    pub total_groups: u64,
    /// Storage size in bytes
    pub storage_size_bytes: u64,
    /// Last update timestamp
    pub last_update_timestamp: u64,
    /// Read operations count
    pub read_operations: u64,
    /// Write operations count
    pub write_operations: u64,
    /// Average read time in microseconds
    pub avg_read_time_us: f64,
    /// Average write time in microseconds
    pub avg_write_time_us: f64,
}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Database path
    pub path: String,
    /// Cache size in bytes
    pub cache_size: usize,
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Maximum write buffer number
    pub max_write_buffer_number: i32,
    /// Enable compression
    pub enable_compression: bool,
    /// Create if missing
    pub create_if_missing: bool,
    /// Maximum open files
    pub max_open_files: i32,
    /// Use fsync
    pub use_fsync: bool,
    /// Enable statistics
    pub enable_statistics: bool,
    /// Backup interval in seconds
    pub backup_interval_secs: u64,
    /// Compaction interval in seconds
    pub compaction_interval_secs: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "savitri_consensus.db".to_string(),
            cache_size: 64 * 1024 * 1024,        // 64MB
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_write_buffer_number: 4,
            enable_compression: true,
            create_if_missing: true,
            max_open_files: 1000,
            use_fsync: true,
            enable_statistics: true,
            backup_interval_secs: 3600,      // 1 hour
            compaction_interval_secs: 86400, // 24 hours
        }
    }
}

/// In-memory storage implementation for testing
pub struct MemoryStorage {
    blocks: Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, Block>>>,
    height_index: Arc<tokio::sync::RwLock<std::collections::HashMap<u64, Vec<u8>>>>,
    transactions: Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, Transaction>>>,
    validators: Arc<tokio::sync::RwLock<std::collections::HashMap<String, ValidatorInfo>>>,
    groups: Arc<tokio::sync::RwLock<std::collections::HashMap<String, GroupInfo>>>,
    scores: Arc<tokio::sync::RwLock<std::collections::HashMap<String, PouScoreResult>>>,
    account_states: Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>,
    contract_states: Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>,
    consensus_state: Arc<tokio::sync::RwLock<Option<ConsensusState>>>,
    proposals: Arc<tokio::sync::RwLock<std::collections::HashMap<String, Box<dyn Proposal>>>>,
    stats: Arc<tokio::sync::RwLock<StorageStats>>,
    config: StorageConfig,
}

impl MemoryStorage {
    pub fn new(config: StorageConfig) -> Self {
        Self {
            blocks: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            height_index: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            transactions: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            validators: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            groups: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            scores: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            account_states: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            contract_states: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            consensus_state: Arc::new(tokio::sync::RwLock::new(None)),
            proposals: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            stats: Arc::new(tokio::sync::RwLock::new(StorageStats::default())),
            config,
        }
    }

    async fn update_stats<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut StorageStats),
    {
        let mut stats = self.stats.write().await;
        update_fn(&mut *stats);
        stats.last_update_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn store_block(&self, block: &Block) -> Result<()> {
        let start_time = std::time::Instant::now();

        let hash = block.hash().to_vec();
        let height = block.header.height;

        // Store block
        {
            let mut blocks = self.blocks.write().await;
            blocks.insert(hash.clone(), block.clone());
        }

        // Update height index
        {
            let mut height_index = self.height_index.write().await;
            height_index.insert(height, hash);
        }

        // Update stats
        let duration = start_time.elapsed().as_micros() as f64;
        self.update_stats(|stats| {
            stats.total_blocks += 1;
            stats.write_operations += 1;
            stats.avg_write_time_us = if stats.write_operations == 1 {
                duration
            } else {
                (stats.avg_write_time_us * (stats.write_operations - 1) as f64 + duration)
                    / stats.write_operations as f64
            };
        })
        .await;

        Ok(())
    }

    async fn get_block(&self, hash: &[u8]) -> Result<Option<Block>> {
        let start_time = std::time::Instant::now();

        let blocks = self.blocks.read().await;
        let result = blocks.get(hash).cloned();

        // Update stats
        let duration = start_time.elapsed().as_micros() as f64;
        self.update_stats(|stats| {
            stats.read_operations += 1;
            stats.avg_read_time_us = if stats.read_operations == 1 {
                duration
            } else {
                (stats.avg_read_time_us * (stats.read_operations - 1) as f64 + duration)
                    / stats.read_operations as f64
            };
        })
        .await;

        Ok(result)
    }

    async fn get_block_by_height(&self, height: u64) -> Result<Option<Block>> {
        let height_index = self.height_index.read().await;
        if let Some(hash) = height_index.get(&height) {
            self.get_block(hash).await
        } else {
            Ok(None)
        }
    }

    async fn get_latest_block(&self) -> Result<Option<Block>> {
        let height_index = self.height_index.read().await;
        if let Some((&max_height, _)) = height_index.iter().max() {
            self.get_block_by_height(max_height).await
        } else {
            Ok(None)
        }
    }

    async fn store_consensus_state(&self, state: &ConsensusState) -> Result<()> {
        let mut consensus_state = self.consensus_state.write().await;
        *consensus_state = Some(state.clone());
        Ok(())
    }

    async fn get_consensus_state(&self) -> Result<Option<ConsensusState>> {
        let consensus_state = self.consensus_state.read().await;
        Ok(consensus_state.clone())
    }

    async fn store_validator(&self, validator: &ValidatorInfo) -> Result<()> {
        let mut validators = self.validators.write().await;
        validators.insert(validator.validator_id.clone(), validator.clone());

        self.update_stats(|stats| {
            stats.total_validators = validators.len() as u64;
        })
        .await;

        Ok(())
    }

    async fn get_validator(&self, validator_id: &str) -> Result<Option<ValidatorInfo>> {
        let validators = self.validators.read().await;
        Ok(validators.get(validator_id).cloned())
    }

    async fn get_active_validators(&self) -> Result<Vec<ValidatorInfo>> {
        let validators = self.validators.read().await;
        Ok(validators
            .values()
            .filter(|v| matches!(v.status, ValidatorStatus::Active))
            .cloned()
            .collect())
    }

    async fn store_proposal(&self, proposal: &dyn Proposal) -> Result<()> {
        let proposal_id = format!("{}-{}", proposal.round_id(), proposal.height());
        let mut proposals = self.proposals.write().await;

        // For memory storage, we'll use the ProposalClone trait
        proposals.insert(proposal_id, proposal.clone_box());
        Ok(())
    }

    async fn get_proposal(&self, proposal_id: &str) -> Result<Option<Box<dyn Proposal>>> {
        let proposals = self.proposals.read().await;
        Ok(proposals.get(proposal_id).map(|p| p.clone_box()))
    }

    async fn store_transaction(&self, tx: &Transaction) -> Result<()> {
        let mut transactions = self.transactions.write().await;
        transactions.insert(tx.hash.0.to_vec(), tx.clone());

        self.update_stats(|stats| {
            stats.total_transactions = transactions.len() as u64;
        })
        .await;

        Ok(())
    }

    async fn get_transaction(&self, hash: &[u8]) -> Result<Option<Transaction>> {
        let transactions = self.transactions.read().await;
        Ok(transactions.get(hash).cloned())
    }

    async fn store_account_state(&self, address: &[u8], state: &[u8]) -> Result<()> {
        let mut account_states = self.account_states.write().await;
        account_states.insert(address.to_vec(), state.to_vec());
        Ok(())
    }

    async fn get_account_state(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        let account_states = self.account_states.read().await;
        Ok(account_states.get(address).cloned())
    }

    async fn store_contract_state(&self, address: &[u8], state: &[u8]) -> Result<()> {
        let mut contract_states = self.contract_states.write().await;
        contract_states.insert(address.to_vec(), state.to_vec());
        Ok(())
    }

    async fn get_contract_state(&self, address: &[u8]) -> Result<Option<Vec<u8>>> {
        let contract_states = self.contract_states.read().await;
        Ok(contract_states.get(address).cloned())
    }

    async fn store_group(&self, group: &GroupInfo) -> Result<()> {
        let mut groups = self.groups.write().await;
        groups.insert(group.group_id.clone(), group.clone());

        self.update_stats(|stats| {
            stats.total_groups = groups.len() as u64;
        })
        .await;

        Ok(())
    }

    async fn get_group(&self, group_id: &str) -> Result<Option<GroupInfo>> {
        let groups = self.groups.read().await;
        Ok(groups.get(group_id).cloned())
    }

    async fn get_active_groups(&self) -> Result<Vec<GroupInfo>> {
        let groups = self.groups.read().await;
        Ok(groups
            .values()
            .filter(|g| matches!(g.status, GroupStatus::Active))
            .cloned()
            .collect())
    }

    async fn store_score(&self, node_id: &str, score: &PouScoreResult) -> Result<()> {
        let mut scores = self.scores.write().await;
        scores.insert(node_id.to_string(), score.clone());
        Ok(())
    }

    async fn get_score(&self, node_id: &str) -> Result<Option<PouScoreResult>> {
        let scores = self.scores.read().await;
        Ok(scores.get(node_id).cloned())
    }

    fn stats(&self) -> StorageStats {
        // Note: This would need async in a real implementation
        // For now, return default stats
        StorageStats::default()
    }

    async fn is_healthy(&self) -> bool {
        // Simple health check - try to read consensus state
        let _ = self.get_consensus_state().await;
        true
    }

    async fn backup(&self, _backup_path: &str) -> Result<()> {
        // For memory storage, backup is not implemented
        Err(crate::error::ConsensusError::StorageError(
            "Backup not implemented for memory storage".to_string(),
        )
        .into())
    }

    async fn restore(&self, _backup_path: &str) -> Result<()> {
        // For memory storage, restore is not implemented
        Err(crate::error::ConsensusError::StorageError(
            "Restore not implemented for memory storage".to_string(),
        )
        .into())
    }

    async fn compact(&self) -> Result<()> {
        // For memory storage, compaction is not needed
        Ok(())
    }

    async fn get_storage_size(&self) -> Result<u64> {
        // Estimate storage size for memory storage
        let blocks_size = self.blocks.read().await.len() * 1024; // Rough estimate
        let transactions_size = self.transactions.read().await.len() * 512; // Rough estimate
        Ok((blocks_size + transactions_size) as u64)
    }
}

// Helper trait for cloning proposals
trait ProposalClone {
    fn clone(&self) -> Box<dyn Proposal>;
}

impl<T: ?Sized + Proposal + Clone + 'static> ProposalClone for T {
    fn clone(&self) -> Box<dyn Proposal> {
        Box::new(self.clone())
    }
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            total_blocks: 0,
            total_transactions: 0,
            total_validators: 0,
            total_groups: 0,
            storage_size_bytes: 0,
            last_update_timestamp: 0,
            read_operations: 0,
            write_operations: 0,
            avg_read_time_us: 0.0,
            avg_write_time_us: 0.0,
        }
    }
}
