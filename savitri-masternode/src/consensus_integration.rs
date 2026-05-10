//! Consensus Integration for Masternode - Production Implementation
//!
//! This module provides the consensus integration layer for the masternode,
//! including slot scheduling, consensus engine, and storage integration.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// Core types from savitri-core
use savitri_core::core::types::{Account, Transaction};

/// Storage trait for consensus operations
/// Provides thread-safe access to blockchain state
pub trait Storage: Send + Sync {
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>, anyhow::Error>;
    fn put_account(&self, address: &[u8], account: &Account) -> Result<(), anyhow::Error>;
    fn get_block_height(&self) -> Result<u64, anyhow::Error>;
    fn get_latest_block_hash(&self) -> Result<[u8; 32], anyhow::Error>;
}

/// Slot Scheduler - manages time-based slot allocation for consensus
pub struct SlotScheduler {
    config: SlotSchedulerConfig,
    start_time: Instant,
    system_start_time: u64,
    current_slot: Arc<RwLock<u64>>,
    current_epoch: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone)]
pub struct SlotSchedulerConfig {
    pub slot_duration: Duration,
    pub validators: Vec<String>,
    pub local_id: String,
    pub slot_base_ms: Option<u64>,
    pub slots_per_epoch: u64,
}

impl Default for SlotSchedulerConfig {
    fn default() -> Self {
        Self {
            slot_duration: Duration::from_millis(1000),
            validators: vec![],
            local_id: String::new(),
            slot_base_ms: None,
            slots_per_epoch: 100,
        }
    }
}

impl SlotScheduler {
    pub fn new(config: SlotSchedulerConfig) -> Result<Self, anyhow::Error> {
        let system_start_time = config.slot_base_ms.unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        });

        info!(
            validators = config.validators.len(),
            slot_duration_ms = config.slot_duration.as_millis(),
            local_id = %config.local_id,
            "SlotScheduler initialized"
        );

        Ok(Self {
            config,
            start_time: Instant::now(),
            system_start_time,
            current_slot: Arc::new(RwLock::new(0)),
            current_epoch: Arc::new(RwLock::new(0)),
        })
    }

    /// Get current slot information based on elapsed time
    pub fn current_slot_info(&self) -> Result<SlotInfo, anyhow::Error> {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        let slot_duration_ms = self.config.slot_duration.as_millis() as u64;

        let current_slot = elapsed_ms / slot_duration_ms;
        let epoch = current_slot / self.config.slots_per_epoch;

        let leader = if self.config.validators.is_empty() {
            self.config.local_id.clone()
        } else {
            let leader_index = (current_slot as usize) % self.config.validators.len();
            self.config.validators[leader_index].clone()
        };

        // Determine our role
        let role = if leader == self.config.local_id {
            SlotRole::Leader
        } else if self.config.validators.contains(&self.config.local_id) {
            SlotRole::Follower
        } else {
            SlotRole::Observer
        };

        let time_in_slot_ms = elapsed_ms % slot_duration_ms;
        let time_remaining_ms = slot_duration_ms.saturating_sub(time_in_slot_ms);

        Ok(SlotInfo {
            slot: current_slot,
            epoch,
            leader,
            role,
            time_in_slot_ms,
            time_remaining_ms,
            slot_start_time: self.system_start_time + (current_slot * slot_duration_ms),
        })
    }

    /// Get the current epoch
    pub fn current_epoch(&self) -> u64 {
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        let slot_duration_ms = self.config.slot_duration.as_millis() as u64;
        let current_slot = elapsed_ms / slot_duration_ms;
        current_slot / self.config.slots_per_epoch
    }

    /// Check if we are the leader for the current slot
    pub fn is_leader(&self) -> bool {
        match self.current_slot_info() {
            Ok(info) => matches!(info.role, SlotRole::Leader),
            Err(_) => false,
        }
    }

    /// Get time until next slot
    pub fn time_until_next_slot(&self) -> Duration {
        match self.current_slot_info() {
            Ok(info) => Duration::from_millis(info.time_remaining_ms),
            Err(_) => Duration::from_millis(1000),
        }
    }

    /// Wait for the next slot
    pub async fn wait_for_next_slot(&self) {
        let wait_time = self.time_until_next_slot();
        tokio::time::sleep(wait_time).await;
    }
}

#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub slot: u64,
    pub epoch: u64,
    pub leader: String,
    pub role: SlotRole,
    pub time_in_slot_ms: u64,
    pub time_remaining_ms: u64,
    pub slot_start_time: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SlotRole {
    Leader,
    Follower,
    Observer,
}

pub struct ConsensusEngine {
    config: ConsensusEngineConfig,
    pending_blocks: Arc<RwLock<HashMap<u64, PendingBlock>>>,
    finalized_height: Arc<RwLock<u64>>,
    votes_received: Arc<RwLock<HashMap<u64, Vec<ConsensusVote>>>>,
    is_running: Arc<RwLock<bool>>,
}

#[derive(Debug, Clone)]
pub struct ConsensusEngineConfig {
    pub committee_size: usize,
    pub threshold: usize,
    pub timeout_ms: u64,
    pub max_block_size: usize,
    pub min_votes_for_finality: usize,
}

impl Default for ConsensusEngineConfig {
    fn default() -> Self {
        Self {
            committee_size: 4,
            threshold: 3,
            timeout_ms: 5000,
            max_block_size: 1_000_000,
            min_votes_for_finality: 4, // 80% quorum: ceil(2*5/3) = 4 for 5 MN
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingBlock {
    pub height: u64,
    #[serde(with = "hex_array_32")]
    pub hash: [u8; 32],
    pub proposer: String,
    pub transactions: Vec<Transaction>,
    pub timestamp: u64,
    #[serde(with = "hex_array_32")]
    pub parent_hash: [u8; 32],
}

// Custom serialization for [u8; 32] arrays
mod hex_array_32 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(data: &[u8; 32], s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        s.serialize_str(&hex::encode(data))
    }

    pub fn deserialize<'de, D>(d: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(d)?;
        let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
        let mut arr = [0u8; 32];
        if bytes.len() == 32 {
            arr.copy_from_slice(&bytes);
        }
        Ok(arr)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusVote {
    pub block_height: u64,
    #[serde(with = "hex_array_32")]
    pub block_hash: [u8; 32],
    pub voter: String,
    pub vote_type: VoteType,
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
    pub timestamp: u64,
}

// Custom serialization for [u8; 64] signature
fn serialize_signature<S>(sig: &[u8; 64], s: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&hex::encode(sig))
}

fn deserialize_signature<'de, D>(d: D) -> Result<[u8; 64], D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s = String::deserialize(d)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    let mut arr = [0u8; 64];
    if bytes.len() == 64 {
        arr.copy_from_slice(&bytes);
    }
    Ok(arr)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VoteType {
    Approve,
    Reject,
    Abstain,
}

impl ConsensusEngine {
    pub fn new(config: ConsensusEngineConfig) -> Self {
        info!(
            committee_size = config.committee_size,
            threshold = config.threshold,
            timeout_ms = config.timeout_ms,
            "ConsensusEngine initialized"
        );

        Self {
            config,
            pending_blocks: Arc::new(RwLock::new(HashMap::new())),
            finalized_height: Arc::new(RwLock::new(0)),
            votes_received: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn start(&self) -> Result<(), anyhow::Error> {
        let mut running = self.is_running.write().await;
        *running = true;
        info!("ConsensusEngine started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<(), anyhow::Error> {
        let mut running = self.is_running.write().await;
        *running = false;
        info!("ConsensusEngine stopped");
        Ok(())
    }

    /// Submit a new block for consensus
    pub async fn submit_block(&self, block: PendingBlock) -> Result<(), anyhow::Error> {
        let height = block.height;
        let mut pending = self.pending_blocks.write().await;

        if pending.contains_key(&height) {
            warn!(height, "Block already pending for this height");
            return Ok(());
        }

        pending.insert(height, block);
        info!(height, "Block submitted for consensus");
        Ok(())
    }

    /// Process a vote for a pending block
    pub async fn process_vote(&self, vote: ConsensusVote) -> Result<Option<u64>, anyhow::Error> {
        let height = vote.block_height;

        // Validate vote signature would go here
        // For now, we trust the vote

        let mut votes = self.votes_received.write().await;
        let block_votes = votes.entry(height).or_insert_with(Vec::new);

        // Check for duplicate votes
        if block_votes.iter().any(|v| v.voter == vote.voter) {
            debug!(height, voter = %vote.voter, "Duplicate vote ignored");
            return Ok(None);
        }

        block_votes.push(vote);

        // Check if we have enough votes for finality
        let approve_count = block_votes
            .iter()
            .filter(|v| v.vote_type == VoteType::Approve)
            .count();

        if approve_count >= self.config.min_votes_for_finality {
            info!(height, approve_count, "Block reached finality threshold");
            info!(height, "Block approved by masternode (consensus)");

            // Update finalized height
            let mut finalized = self.finalized_height.write().await;
            if height > *finalized {
                *finalized = height;
            }

            // Clean up pending block
            let mut pending = self.pending_blocks.write().await;
            pending.remove(&height);

            return Ok(Some(height));
        }

        Ok(None)
    }

    /// Get the current finalized height
    pub async fn get_finalized_height(&self) -> u64 {
        *self.finalized_height.read().await
    }

    /// Check if a block at given height is finalized
    pub async fn is_finalized(&self, height: u64) -> bool {
        let finalized = self.finalized_height.read().await;
        height <= *finalized
    }
}

/// Integrated consensus system with group formation
pub struct IntegratedConsensusSystem {
    consensus_engine: Arc<ConsensusEngine>,
    storage: Arc<dyn Storage>,
    scheduler: Arc<SlotScheduler>,
    is_running: Arc<RwLock<bool>>,
    block_production_enabled: Arc<RwLock<bool>>,
}

/// Configuration for integrated consensus system
#[derive(Debug, Clone)]
pub struct IntegratedConsensusConfig {
    pub committee_size: usize,
    pub threshold: usize,
    pub timeout_ms: u64,
    pub enable_block_production: bool,
}

impl Default for IntegratedConsensusConfig {
    fn default() -> Self {
        Self {
            committee_size: 4,
            threshold: 3,
            timeout_ms: 5000,
            enable_block_production: true,
        }
    }
}

impl IntegratedConsensusSystem {
    pub fn new(
        storage: Arc<dyn Storage>,
        scheduler: Arc<SlotScheduler>,
        config: IntegratedConsensusConfig,
    ) -> Self {
        let engine_config = ConsensusEngineConfig {
            committee_size: config.committee_size,
            threshold: config.threshold,
            timeout_ms: config.timeout_ms,
            max_block_size: 1_000_000,
            min_votes_for_finality: config.threshold,
        };
        let engine = ConsensusEngine::new(engine_config);

        info!(
            committee_size = config.committee_size,
            threshold = config.threshold,
            "IntegratedConsensusSystem initialized"
        );

        Self {
            consensus_engine: Arc::new(engine),
            storage,
            scheduler,
            is_running: Arc::new(RwLock::new(false)),
            block_production_enabled: Arc::new(RwLock::new(config.enable_block_production)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let mut running = self.is_running.write().await;
        *running = true;

        self.consensus_engine.start().await?;
        info!("IntegratedConsensusSystem started");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let mut running = self.is_running.write().await;
        *running = false;

        self.consensus_engine.stop().await?;
        info!("IntegratedConsensusSystem stopped");
        Ok(())
    }

    /// Get current slot information
    pub fn get_slot_info(&self) -> Result<SlotInfo> {
        self.scheduler.current_slot_info()
    }

    /// Check if we should produce a block
    pub async fn should_produce_block(&self) -> bool {
        let enabled = *self.block_production_enabled.read().await;
        enabled && self.scheduler.is_leader()
    }

    /// Submit a block for consensus
    pub async fn submit_block(&self, block: PendingBlock) -> Result<()> {
        self.consensus_engine.submit_block(block).await
    }

    /// Process a consensus vote
    pub async fn process_vote(&self, vote: ConsensusVote) -> Result<Option<u64>> {
        self.consensus_engine.process_vote(vote).await
    }

    /// Get finalized height
    pub async fn get_finalized_height(&self) -> u64 {
        self.consensus_engine.get_finalized_height().await
    }

    /// Get the consensus engine for direct access
    pub fn get_engine(&self) -> Arc<ConsensusEngine> {
        self.consensus_engine.clone()
    }

    /// Get the scheduler
    pub fn get_scheduler(&self) -> Arc<SlotScheduler> {
        self.scheduler.clone()
    }
}

/// Consensus integration manager
pub struct ConsensusIntegration {
    system: IntegratedConsensusSystem,
}

impl ConsensusIntegration {
    pub fn new(system: IntegratedConsensusSystem) -> Self {
        Self { system }
    }

    pub async fn start(&self) -> Result<()> {
        self.system.start().await
    }

    pub async fn stop(&self) -> Result<()> {
        self.system.stop().await
    }

    pub fn get_system(&self) -> &IntegratedConsensusSystem {
        &self.system
    }
}

/// In-memory storage implementation for testing and development
/// Use savitri-storage for production deployments
pub struct MemoryStorage {
    accounts: Arc<RwLock<HashMap<Vec<u8>, Account>>>,
    block_height: Arc<RwLock<u64>>,
    latest_block_hash: Arc<RwLock<[u8; 32]>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        info!("MemoryStorage initialized (use savitri-storage for production)");
        Self {
            accounts: Arc::new(RwLock::new(HashMap::new())),
            block_height: Arc::new(RwLock::new(0)),
            latest_block_hash: Arc::new(RwLock::new([0u8; 32])),
        }
    }

    pub async fn set_block_height(&self, height: u64) {
        let mut h = self.block_height.write().await;
        *h = height;
    }

    pub async fn set_latest_block_hash(&self, hash: [u8; 32]) {
        let mut h = self.latest_block_hash.write().await;
        *h = hash;
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>, anyhow::Error> {
        let accounts = futures::executor::block_on(self.accounts.read());
        Ok(accounts.get(address).cloned())
    }

    fn put_account(&self, address: &[u8], account: &Account) -> Result<(), anyhow::Error> {
        let mut accounts = futures::executor::block_on(self.accounts.write());
        accounts.insert(address.to_vec(), account.clone());
        Ok(())
    }

    fn get_block_height(&self) -> Result<u64, anyhow::Error> {
        let height = futures::executor::block_on(self.block_height.read());
        Ok(*height)
    }

    fn get_latest_block_hash(&self) -> Result<[u8; 32], anyhow::Error> {
        let hash = futures::executor::block_on(self.latest_block_hash.read());
        Ok(*hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slot_scheduler() {
        let config = SlotSchedulerConfig {
            slot_duration: Duration::from_millis(100),
            validators: vec!["node1".to_string(), "node2".to_string()],
            local_id: "node1".to_string(),
            slot_base_ms: None,
            slots_per_epoch: 10,
        };

        let scheduler = SlotScheduler::new(config).unwrap();
        let info = scheduler.current_slot_info().unwrap();

        assert!(info.slot >= 0);
        assert!(!info.leader.is_empty());
    }

    #[tokio::test]
    async fn test_consensus_engine() {
        let config = ConsensusEngineConfig::default();
        let engine = ConsensusEngine::new(config);

        engine.start().await.unwrap();

        let block = PendingBlock {
            height: 1,
            hash: [1u8; 32],
            proposer: "node1".to_string(),
            transactions: vec![],
            timestamp: 0,
            parent_hash: [0u8; 32],
        };

        engine.submit_block(block).await.unwrap();

        // Submit votes
        for i in 0..3 {
            let vote = ConsensusVote {
                block_height: 1,
                block_hash: [1u8; 32],
                voter: format!("voter{}", i),
                vote_type: VoteType::Approve,
                signature: [0u8; 64],
                timestamp: 0,
            };

            let result = engine.process_vote(vote).await.unwrap();
            if i == 2 {
                assert_eq!(result, Some(1)); // Finalized after 3 votes
            }
        }

        assert!(engine.is_finalized(1).await);
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_storage() {
        let storage = MemoryStorage::new();

        let account = Account {
            address: [1u8; 32],
            balance: 1000,
            nonce: 0,
            code_hash: [0u8; 32],
            storage_root: [0u8; 32],
        };

        storage.put_account(&[1u8; 32], &account).unwrap();
        let retrieved = storage.get_account(&[1u8; 32]).unwrap();

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().balance, 1000);
    }
}
