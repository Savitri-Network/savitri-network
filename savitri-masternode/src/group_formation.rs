//! Group Formation Service for Masternode
//!
//! This module implements the group formation logic that runs on masternodes
//! to organize light nodes into P2P groups for proposer election.
//!
//! ## Design Principles
//! - **Epoch-based Formation**: Groups are formed at epoch boundaries
//! - **Geographic Distribution**: Ensure groups are distributed across regions
//! - **Load Balancing**: Balance group sizes and member distribution
//! - **Fault Tolerance**: Handle node failures and network partitions
//! - **Deterministic Selection**: Same inputs produce same group assignments

use anyhow::{Context, Result};
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

// Import from core modules
use savitri_core::core::types::{Account, Transaction};

// Import from savitri-consensus instead of removed consensus_integration
use savitri_consensus::{GroupAwareConfig, StorageConfig};

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

// Minimum stake required to register a light node. Prevents zero-cost flooding
// of the group formation system with fake nodes.
const DEFAULT_MINIMUM_STAKE: u128 = 1_000_000; // 1M units (adjustable via config)
                                               // Maximum new registrations accepted per epoch. Limits the rate at which an
                                               // attacker can inject new identities into the system.
const MAX_REGISTRATIONS_PER_EPOCH: u64 = 10;

/// Create a deterministic 32-byte seed from an epoch number and a sorted list of
/// node IDs. This ensures that "same inputs produce same group assignments" as
/// required by the design principles (AUDIT-007).
fn deterministic_seed(epoch: u64, node_ids: &[String]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"savitri-group-formation-v1");
    hasher.update(epoch.to_le_bytes());
    for id in node_ids {
        hasher.update(id.as_bytes());
    }
    hasher.finalize().into()
}

// Local Storage trait for compatibility (was in consensus_integration)
pub trait Storage: Send + Sync {
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>, anyhow::Error>;
    fn put_account(&self, address: &[u8], account: &Account) -> Result<(), anyhow::Error>;
}

// Local SlotScheduler for compatibility (was in consensus_integration)
pub struct SlotScheduler {
    config: SlotSchedulerConfig,
}

#[derive(Debug, Clone)]
pub struct SlotSchedulerConfig {
    pub heartbeat_interval_ms: u64,
    pub slots_per_epoch: u64,
    pub genesis_timestamp_ms: u64,
    pub validators: Vec<String>,
    pub local_id: String,
}

impl SlotScheduler {
    pub fn new(config: SlotSchedulerConfig) -> Result<Self, anyhow::Error> {
        Ok(Self { config })
    }

    pub fn current_slot_info(&self) -> Result<SlotInfo, anyhow::Error> {
        let slot = self.current_slot();
        let epoch = self.current_epoch();
        Ok(SlotInfo {
            slot,
            epoch,
            leader: self.config.local_id.clone(),
            role: SlotRole::Leader,
            time_in_slot_ms: 0,
            time_remaining_ms: self.config.heartbeat_interval_ms,
            slot_start_time: self.config.genesis_timestamp_ms
                + slot * self.config.heartbeat_interval_ms,
        })
    }

    fn current_slot(&self) -> u64 {
        savitri_consensus::primitives::epoch::current_slot(
            savitri_consensus::primitives::epoch::now_ms(),
            self.config.genesis_timestamp_ms,
            self.config.heartbeat_interval_ms,
        )
    }

    pub fn current_epoch(&self) -> u64 {
        savitri_consensus::primitives::epoch::current_epoch(
            savitri_consensus::primitives::epoch::now_ms(),
            self.config.genesis_timestamp_ms,
            self.config.heartbeat_interval_ms,
            self.config.slots_per_epoch.max(1),
        )
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

#[derive(Debug, Clone)]
pub enum SlotRole {
    Leader,
    Follower,
    Observer,
}

// MemoryStorage implementation for compatibility
pub struct MemoryStorage {
    accounts: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Account>>,
}

impl MemoryStorage {
    pub fn new(_config: savitri_consensus::StorageConfig) -> Self {
        Self {
            accounts: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl Storage for MemoryStorage {
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>, anyhow::Error> {
        let accounts = self.accounts.lock().unwrap();
        Ok(accounts.get(address).cloned())
    }

    fn put_account(&self, address: &[u8], account: &Account) -> Result<(), anyhow::Error> {
        let mut accounts = self.accounts.lock().unwrap();
        accounts.insert(address.to_vec(), account.clone());
        Ok(())
    }
}

/// Persistent storage for group formation (savitri-storage/RocksDB).
/// Used when feature "storage" is enabled; path is under masternode storage (e.g. group_formation/).
#[cfg(feature = "storage")]
pub struct PersistentGroupFormationStorage {
    inner: Arc<savitri_storage::Storage>,
}

#[cfg(feature = "storage")]
impl PersistentGroupFormationStorage {
    pub fn new(inner: Arc<savitri_storage::Storage>) -> Self {
        Self { inner }
    }

    pub fn with_path(path: &std::path::Path) -> Result<Self, anyhow::Error> {
        let path_str = path.to_string_lossy().to_string();
        let config = savitri_storage::StorageConfig {
            path: path_str,
            ..Default::default()
        };
        let inner = Arc::new(savitri_storage::Storage::with_config(config)?);
        Ok(Self { inner })
    }

    /// Expose inner storage for reward distribution (mint_reward, etc.)
    pub fn as_savitri_storage(&self) -> Arc<savitri_storage::Storage> {
        self.inner.clone()
    }
}

#[cfg(feature = "storage")]
impl Storage for PersistentGroupFormationStorage {
    fn get_account(&self, address: &[u8]) -> Result<Option<Account>, anyhow::Error> {
        match self.inner.get_account(address)? {
            Some(bytes) => Ok(Some(Account::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    fn put_account(&self, address: &[u8], account: &Account) -> Result<(), anyhow::Error> {
        let encoded = account.encode();
        self.inner.put_account(address, &encoded)?;
        Ok(())
    }
}

// Import group consensus types
use super::group_consensus::{
    BftGroupConfig, GroupApprovalCertificate, GroupConsensusManager, GroupProposal, GroupVote,
    GroupVoteType, LeaderElectionState,
};
// Import masternode P2P message type for immediate vote publishing
use super::masternode_p2p::MasternodeMessage;

/// Trait for P2P group distribution
pub trait MonolithP2PDistributor: Send + Sync {
    fn distribute_groups(
        &self,
        groups: &[P2PGroup],
        epoch: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
}

/// Node assignment status for group formation registry.
/// Only Free nodes are eligible for new group assignment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeAssignmentStatus {
    /// Not assigned to any group — eligible for group formation
    Free,
    /// Assigned to an active group — NOT eligible for new groups
    Assigned { group_id: String },
}

impl Default for NodeAssignmentStatus {
    fn default() -> Self {
        NodeAssignmentStatus::Free
    }
}

/// Light node information for group formation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightNodeInfo {
    pub node_id: String,
    pub peer_id: String,
    pub multiaddr: String,
    pub geographic_region: String,
    pub pou_score: f64,
    pub capabilities: Vec<String>,
    pub last_seen: u64,
    pub uptime_percentage: f64,
    #[serde(with = "BigArray")]
    pub account: [u8; 32],
    /// Group assignment status: Free or Assigned(group_id)
    #[serde(default)]
    pub assignment: NodeAssignmentStatus,
}

/// P2P Group configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PGroup {
    pub group_id: String,
    pub members: Vec<String>, // Light node peer IDs
    /// Peer ID -> multiaddr so LNs can dial each other for intra-group mesh
    #[serde(default)]
    pub member_multiaddrs: HashMap<String, String>,
    pub proposer: Option<String>, // Current proposer based on PoU (lightnode)
    /// publishes the BlockAcceptanceCertificate when quorum is reached.
    #[serde(default)]
    pub group_leader_masternode: Option<String>,
    /// Backup masternode for this group — chosen randomly (deterministic hash) from
    /// the remaining masternodes. If the leader doesn't publish the certificate
    /// within BACKUP_CERT_TIMEOUT_MS, the backup takes over and publishes.
    #[serde(default)]
    pub backup_leader_masternode: Option<String>,
    pub created_at: u64,
    pub epoch: u64,
    pub geographic_region: String,
    pub status: GroupStatus,
    pub health_score: f64,
    /// Shard IDs assigned to this group for shard-aware TX processing.
    /// Populated during group formation: shard_count / num_groups shards per group.
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
}

/// Group status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupStatus {
    Forming,
    Active,
    Inactive,
    Dissolving,
}

/// Group formation configuration
#[derive(Debug, Clone)]
pub struct GroupFormationConfig {
    pub min_group_size: usize,
    pub max_group_size: usize,
    pub target_groups: usize,
    pub formation_interval_epochs: u64,
    pub health_check_interval_secs: u64,
    pub node_timeout_secs: u64,
    /// Threshold disconnessione per dissolution gruppo (es. 0.55 = 55%): se ≥55% dei membri
    pub dissolution_disconnect_threshold: f64,
    pub geographic_distribution: bool,
    pub load_balancing: bool,
    // Prevents Sybil attacks by requiring economic commitment.
    pub minimum_stake: u128,
}

impl Default for GroupFormationConfig {
    fn default() -> Self {
        Self {
            min_group_size: 5,
            max_group_size: 50,
            target_groups: 10,
            formation_interval_epochs: 1,
            health_check_interval_secs: 60,
            // ENHANCED: Increased timeout from 300s (5min) to 600s (10min) to prevent premature removal
            // Lightnodes may have temporary network issues but should remain available longer
            node_timeout_secs: 600, // 10 minutes instead of 5
            // PERF: Raised from 0.75 to 0.95 — dissolve only when nearly all members gone.
            // Prevents spurious dissolution during active block production (9-min stall fix).
            dissolution_disconnect_threshold: 0.95,
            geographic_distribution: true,
            load_balancing: true,
            minimum_stake: DEFAULT_MINIMUM_STAKE,
        }
    }
}

/// Group formation manager with BFT consensus integration
pub struct GroupFormationManager {
    storage: Arc<dyn Storage>,
    scheduler: Arc<SlotScheduler>,
    config: GroupFormationConfig,
    registered_nodes: Arc<RwLock<HashMap<String, LightNodeInfo>>>,
    active_groups: Arc<RwLock<HashMap<String, P2PGroup>>>,
    current_epoch: Arc<RwLock<u64>>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
    /// Group consensus manager for BFT approval
    group_consensus: Option<Arc<GroupConsensusManager>>,
    /// Whether this masternode should initiate group formation
    auto_initiate: bool,
    /// Pending leader election proposal to be broadcast via P2P
    pending_leader_election_proposal:
        Arc<RwLock<Option<super::group_consensus::LeaderElectionProposal>>>,
    /// Pending group proposal to be broadcast via P2P (created by elected leader)
    pending_group_proposal: Arc<RwLock<Option<GroupProposal>>>,
    /// Pending group vote to be broadcast via P2P (kept for backward compatibility, but votes are now published immediately)
    pending_group_vote: Arc<RwLock<Option<GroupVote>>>,
    /// Channel sender for immediate vote publishing (SOLUZIONE: pubblicazione immediata voti)
    masternode_publish_tx: Arc<RwLock<Option<mpsc::UnboundedSender<MasternodeMessage>>>>,
    /// Maps node_id -> staked amount. Only nodes with stake >= minimum_stake can register.
    node_stakes: Arc<RwLock<HashMap<String, u128>>>,
    /// Counter of new registrations in the current epoch. Reset on epoch change.
    registrations_this_epoch: Arc<AtomicU64>,
    /// The epoch in which registrations_this_epoch was last reset.
    registration_epoch: Arc<AtomicU64>,
}

impl GroupFormationManager {
    /// Debug helper: stable-ish id for this manager instance
    pub fn debug_id(&self) -> usize {
        self as *const Self as usize
    }
    pub fn new(
        storage: Arc<dyn Storage>,
        scheduler: Arc<SlotScheduler>,
        config: GroupFormationConfig,
    ) -> Self {
        Self {
            storage,
            scheduler,
            config,
            registered_nodes: Arc::new(RwLock::new(HashMap::new())),
            active_groups: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: Arc::new(RwLock::new(0)),
            shutdown_tx: None,
            group_consensus: None,
            auto_initiate: false,
            pending_leader_election_proposal: Arc::new(RwLock::new(None)),
            pending_group_proposal: Arc::new(RwLock::new(None)),
            pending_group_vote: Arc::new(RwLock::new(None)),
            masternode_publish_tx: Arc::new(RwLock::new(None)),
            node_stakes: Arc::new(RwLock::new(HashMap::new())),
            registrations_this_epoch: Arc::new(AtomicU64::new(0)),
            registration_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create new group formation manager with BFT consensus
    pub fn with_consensus(
        storage: Arc<dyn Storage>,
        scheduler: Arc<SlotScheduler>,
        config: GroupFormationConfig,
        group_consensus: Arc<GroupConsensusManager>,
        auto_initiate: bool,
    ) -> Self {
        Self {
            storage,
            scheduler,
            config,
            registered_nodes: Arc::new(RwLock::new(HashMap::new())),
            active_groups: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: Arc::new(RwLock::new(0)),
            shutdown_tx: None,
            group_consensus: Some(group_consensus),
            auto_initiate,
            pending_leader_election_proposal: Arc::new(RwLock::new(None)),
            pending_group_proposal: Arc::new(RwLock::new(None)),
            pending_group_vote: Arc::new(RwLock::new(None)),
            masternode_publish_tx: Arc::new(RwLock::new(None)),
            node_stakes: Arc::new(RwLock::new(HashMap::new())),
            registrations_this_epoch: Arc::new(AtomicU64::new(0)),
            registration_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set group consensus manager
    pub fn set_group_consensus(&mut self, group_consensus: Arc<GroupConsensusManager>) {
        self.group_consensus = Some(group_consensus);
    }

    /// Set auto initiation flag
    pub fn set_auto_initiate(&mut self, auto_initiate: bool) {
        self.auto_initiate = auto_initiate;
    }

    /// Set masternode publish channel sender for immediate vote publishing
    pub async fn set_masternode_publish_sender(
        &self,
        sender: mpsc::UnboundedSender<MasternodeMessage>,
    ) {
        let mut tx = self.masternode_publish_tx.write().await;
        *tx = Some(sender);
        info!("✅ SOLUZIONE: Masternode publish channel configured for immediate vote publishing");
    }

    /// Initiate group formation process
    pub async fn initiate_group_formation(&self) -> Result<()> {
        info!("🚀 GROUP FORMATION: Initiating group formation process");

        // creati direttamente. Devono passare attraverso il flusso BFT completo:
        //   leader election → GroupProposal → voti BFT → certificato → distribuzione
        if self.group_consensus.is_some() {
            warn!(
                "🔒 BFT GUARD: initiate_group_formation() called but BFT consensus is active. \
                 Gruppi NON verranno creati direttamente. \
                 La formazione avverrà tramite il flusso BFT nel loop periodico \
                 (form_and_distribute_groups → leader election → proposal → vote → certificate → distribute)"
            );
            return Ok(());
        }

        // Percorso NON-BFT: formazione diretta (solo quando non c'è consenso BFT configurato)
        info!("⚡ NON-BFT PATH: Creating groups directly (no BFT consensus configured)");
        let groups = self.form_groups(true).await?;

        if groups.is_empty() {
            info!("No groups formed yet (not enough nodes or normal at startup)");
        } else {
            info!(
                groups_count = groups.len(),
                "✅ GROUP FORMATION: Successfully formed {} groups (non-BFT path)",
                groups.len()
            );
        }

        Ok(())
    }

    /// Register a light node for group formation.
    ///
    /// SECURITY [AUDIT-005]: This is the backward-compatible entry point that accepts
    /// registrations without an explicit stake parameter. It uses the configured
    /// `minimum_stake` as the assumed stake (i.e. the caller is trusted, typically from
    /// bootstrap or masternode-to-masternode sync). For untrusted external registrations,
    /// use `register_light_node_with_stake()` which requires an explicit stake proof.
    pub async fn register_light_node(&self, node_info: LightNodeInfo) -> Result<()> {
        // as having exactly the minimum required stake.
        self.register_light_node_with_stake(node_info, self.config.minimum_stake)
            .await
    }

    /// Register a light node with explicit stake verification.
    ///
    /// SECURITY [AUDIT-005]: Sybil resistance — requires `stake >= minimum_stake` and
    /// enforces a per-epoch registration rate limit of `MAX_REGISTRATIONS_PER_EPOCH`.
    pub async fn register_light_node_with_stake(
        &self,
        node_info: LightNodeInfo,
        stake: u128,
    ) -> Result<()> {
        info!(
            node_id = %node_info.node_id,
            peer_id = %node_info.peer_id,
            account = hex::encode(&node_info.account[..8]),
            region = %node_info.geographic_region,
            pou_score = node_info.pou_score,
            capabilities = ?node_info.capabilities,
            uptime = node_info.uptime_percentage,
            stake = stake,
            "GROUP FORMATION: Registering light node for group formation"
        );

        if stake < self.config.minimum_stake {
            warn!(
                node_id = %node_info.node_id,
                stake = stake,
                minimum_stake = self.config.minimum_stake,
                "AUDIT-005: Light node registration rejected - insufficient stake"
            );
            return Err(anyhow::anyhow!(
                "AUDIT-005: Insufficient stake for registration: {} < {} minimum required",
                stake,
                self.config.minimum_stake
            ));
        }

        // SECURITY: Reject registrations with private/Docker IP or missing/unknown multiaddr.
        let multiaddr = &node_info.multiaddr;
        if multiaddr.is_empty()
            || multiaddr == "unknown"
            || multiaddr.contains("tcp/0")
            || multiaddr.contains("/ip4/172.")
            || multiaddr.contains("/ip4/10.")
            || multiaddr.contains("/ip4/192.168.")
            || multiaddr.contains("/ip4/127.0.0.1")
            || multiaddr.contains("/ip4/0.0.0.0")
        {
            warn!(
                node_id = %node_info.node_id,
                multiaddr = %multiaddr,
                "Registration rejected: private/unreachable IP address"
            );
            return Err(anyhow::anyhow!(
                "Registration rejected: private IP address {} is not reachable from public network",
                multiaddr
            ));
        }

        let mut nodes = self.registered_nodes.write().await;

        // Check if node already exists (updates are always allowed — no rate limit for re-registration)
        if nodes.contains_key(&node_info.node_id) {
            debug!(
                "Light node {} already registered, updating info",
                node_info.node_id
            );
            let existing = nodes.get_mut(&node_info.node_id).unwrap();
            *existing = node_info.clone();
            drop(nodes);
            let mut stakes = self.node_stakes.write().await;
            stakes.insert(node_info.node_id.clone(), stake);
        } else {
            // registrations per epoch to slow down Sybil flooding.
            let current_epoch = self.scheduler.current_epoch();
            let stored_epoch = self.registration_epoch.load(AtomicOrdering::SeqCst);
            if current_epoch != stored_epoch {
                // New epoch — reset the counter.
                self.registrations_this_epoch
                    .store(0, AtomicOrdering::SeqCst);
                self.registration_epoch
                    .store(current_epoch, AtomicOrdering::SeqCst);
            }
            let count = self
                .registrations_this_epoch
                .fetch_add(1, AtomicOrdering::SeqCst);
            if count >= MAX_REGISTRATIONS_PER_EPOCH {
                // Undo the increment since we are rejecting.
                self.registrations_this_epoch
                    .fetch_sub(1, AtomicOrdering::SeqCst);
                warn!(
                    node_id = %node_info.node_id,
                    registrations_this_epoch = count,
                    max = MAX_REGISTRATIONS_PER_EPOCH,
                    "AUDIT-005: Light node registration rejected - epoch rate limit exceeded"
                );
                return Err(anyhow::anyhow!(
                    "AUDIT-005: Registration rate limit exceeded: {} registrations this epoch (max {})",
                    count, MAX_REGISTRATIONS_PER_EPOCH
                ));
            }

            info!(
                "GROUP FORMATION: New node {} registering (stake: {}, reg #{} this epoch)",
                node_info.node_id,
                stake,
                count + 1
            );
            // Preserve assignment status if node is already registered (re-registration
            // from heartbeat/gossip must NOT reset Assigned→Free)
            let mut new_info = node_info.clone();
            if let Some(existing) = nodes.get(&node_info.node_id) {
                new_info.assignment = existing.assignment.clone();
            }
            nodes.insert(node_info.node_id.clone(), new_info);

            let total_nodes = nodes.len();
            info!(
                "GROUP FORMATION: Node registered successfully. Total nodes: {} -> {}",
                total_nodes - 1,
                total_nodes
            );

            let nodes_needed = self.config.min_group_size.max(1);
            if total_nodes < nodes_needed {
                info!(
                    "GROUP FORMATION: Have {} nodes, need {} more for group formation",
                    total_nodes,
                    nodes_needed - total_nodes
                );
            }

            drop(nodes);
            let mut stakes = self.node_stakes.write().await;
            stakes.insert(node_info.node_id.clone(), stake);
        }

        // If we have group consensus, also register there
        if let Some(ref consensus) = self.group_consensus {
            info!("GROUP FORMATION: Initializing available lightnodes in consensus");
            if let Err(e) = consensus.initialize_available_lightnodes().await {
                error!(
                    "GROUP FORMATION: Failed to initialize consensus lightnodes: {}",
                    e
                );
            } else {
                info!("GROUP FORMATION: Successfully initialized consensus lightnodes");
            }
        }

        Ok(())
    }

    /// Register multiple light nodes in batch for improved performance
    pub async fn register_light_nodes_batch(&self, node_infos: Vec<LightNodeInfo>) -> Result<()> {
        if node_infos.is_empty() {
            return Ok(());
        }

        info!(
            batch_size = node_infos.len(),
            "🔄 GROUP FORMATION: Registering {} light nodes in batch",
            node_infos.len()
        );

        let mut nodes = self.registered_nodes.write().await;
        let mut new_nodes = Vec::new();
        let mut updated_nodes = 0;

        for node_info in node_infos {
            // SECURITY: Reject registrations with private/Docker IP or missing/unknown multiaddr.
            let multiaddr = &node_info.multiaddr;
            if multiaddr.is_empty()
                || multiaddr == "unknown"
                || multiaddr.contains("tcp/0")
                || multiaddr.contains("/ip4/172.")
                || multiaddr.contains("/ip4/10.")
                || multiaddr.contains("/ip4/192.168.")
                || multiaddr.contains("/ip4/127.0.0.1")
                || multiaddr.contains("/ip4/0.0.0.0")
            {
                warn!(
                    node_id = %node_info.node_id,
                    multiaddr = %multiaddr,
                    "Batch registration: rejected invalid/private/unreachable address"
                );
                continue;
            }

            if nodes.contains_key(&node_info.node_id) {
                debug!(
                    "Light node {} already registered, updating info",
                    node_info.node_id
                );
                let existing = nodes.get_mut(&node_info.node_id).unwrap();
                let preserved_assignment = existing.assignment.clone();
                *existing = node_info.clone();
                existing.assignment = preserved_assignment; // Don't reset Assigned→Free
                updated_nodes += 1;
            } else {
                debug!(
                    "New node {} registering for group formation",
                    node_info.node_id
                );
                nodes.insert(node_info.node_id.clone(), node_info.clone());
                new_nodes.push(node_info);
            }
        }

        let total_nodes = nodes.len();
        info!(
            "📊 GROUP FORMATION: Batch registration completed. New: {}, Updated: {}, Total: {}",
            new_nodes.len(),
            updated_nodes,
            total_nodes
        );

        // Release lock before consensus operations
        drop(nodes);

        // Initialize consensus with new nodes if any
        if !new_nodes.is_empty() {
            if let Some(ref consensus) = self.group_consensus {
                info!(
                    "🔄 GROUP FORMATION: Initializing {} new lightnodes in consensus",
                    new_nodes.len()
                );
                if let Err(e) = consensus
                    .initialize_available_lightnodes_batch(new_nodes.clone())
                    .await
                {
                    error!(
                        "❌ GROUP FORMATION: Failed to initialize consensus lightnodes batch: {}",
                        e
                    );
                } else {
                    info!(
                        "✅ GROUP FORMATION: Successfully initialized consensus lightnodes batch"
                    );
                }
            }

            // Check if we should trigger group formation
            let nodes_needed = self.config.min_group_size.max(1);
            if total_nodes >= nodes_needed {
                info!(
                    "🚀 GROUP FORMATION: Sufficient nodes for group formation ({} >= {}), checking leader election",
                    total_nodes, nodes_needed
                );

                info!(
                    auto_initiate = self.auto_initiate,
                    "GROUP FORMATION: Sufficient nodes, checking auto_initiate"
                );
                // La formazione dei gruppi DEVE passare attraverso il consenso BFT:
                //   leader election → GroupProposal → voti BFT → certificato → distribuzione
                // Il loop periodico in main.rs chiama form_and_distribute_groups() che gestisce
                if self.group_consensus.is_some() {
                    info!(
                        "🔒 BFT ACTIVE: Sufficient nodes registered ({} >= {}). \
                         Group formation will proceed via BFT consensus in the periodic loop \
                         (form_and_distribute_groups → leader election → proposal → vote → certificate)",
                        total_nodes, nodes_needed
                    );
                } else if self.auto_initiate {
                    info!("⚡ NO BFT: Calling initiate_group_formation() directly (no consensus configured)");
                    if let Err(e) = self.initiate_group_formation().await {
                        error!(
                            "❌ GROUP FORMATION: Failed to initiate group formation: {}",
                            e
                        );
                    }
                }
            } else {
                info!(
                    "⏳ GROUP FORMATION: Have {} nodes, need {} more for group formation",
                    total_nodes,
                    nodes_needed - total_nodes
                );
            }
        }

        Ok(())
    }

    /// Get all registered light nodes
    pub async fn get_registered_nodes(&self) -> Vec<LightNodeInfo> {
        let nodes = self.registered_nodes.read().await;
        nodes.values().cloned().collect()
    }

    /// Get the minimum group size from configuration
    pub fn min_group_size(&self) -> usize {
        self.config.min_group_size
    }

    /// Update last_seen for a lightnode identified by peer_id.
    /// Returns true if an entry was updated.
    pub async fn update_last_seen_by_peer_id(&self, peer_id: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut nodes = self.registered_nodes.write().await;
        let mut updated = false;
        for node_info in nodes.values_mut() {
            if node_info.peer_id == peer_id {
                node_info.last_seen = now;
                updated = true;
            }
        }
        updated
    }

    /// Form groups based on current registered nodes and distribute them with BFT approval.
    ///
    /// With leader election enabled (BFT consensus present):
    /// 1. If no election is in progress, initiates leader election
    /// 2. If this masternode is the elected leader, creates and proposes groups
    /// 3. If another masternode is the leader, waits for their proposal
    ///
    /// Returns a `LeaderElectionAction` indicating what P2P messages need to be sent.
    pub async fn form_and_distribute_groups<M>(
        &self,
        p2p_manager: Option<&M>,
        force: bool,
    ) -> Result<Vec<P2PGroup>>
    where
        M: MonolithP2PDistributor,
    {
        // Epoch-based throttle: skip if not enough epochs have passed since the
        // last successful formation.
        //
        // `!has_active_groups`, which made the throttle ineffective whenever the
        // active_groups map was transiently empty (e.g., after a Dissolving
        // group was retained-out at line ~1681, after restart-with-no-cert, or
        // after a peer flap that caused group eviction). Each empty-map tick
        // would re-enter `create_and_propose_groups` which calls
        // `form_groups(true)`, generating a NEW `group_{epoch}_{idx}_{epoch}`
        // every cycle — observed symptom: group_14_1_14 -> group_15_1_15 ->
        // group_20_1_20 -> ... rotating every few seconds, breaking the LN<->MN
        //
        // The new bypass condition is **cold-start only** (`*epoch_guard == 0`).
        // Once the first formation has set the anchor, the throttle holds for
        // the full `formation_interval_epochs` window regardless of
        // active_groups state. Genuine emergencies must use `force=true` (only
        // the MN-failover hysteresis path in main.rs:1171 does so today).
        if !force {
            let current_epoch = self.scheduler.current_epoch();
            let epoch_guard = self.current_epoch.read().await;
            let cold_start = *epoch_guard == 0;
            if !cold_start
                && current_epoch > 0
                && (current_epoch - *epoch_guard) < self.config.formation_interval_epochs
            {
                // active_groups are still distributed to LNs. The internal init at line
                // ~1184 populates active_groups immediately for role detection, but that
                // path does NOT trigger GroupAnnouncement broadcast — only BFT approval
                // does (group_consensus.rs:1117 add_active_groups -> distribute). Post
                // staggered restart, the BFT proposal flow does not run because the
                // throttle here returns silently before reaching it. Result: LNs remain
                // unaware of their group, no election, chain stalled at last cert height.
                //
                // throttled tick. Idempotent on LN side (MESH already formed), low cost.
                drop(epoch_guard);
                if let Some(p2p) = p2p_manager {
                    let groups: Vec<P2PGroup> =
                        self.active_groups.read().await.values().cloned().collect();
                    if !groups.is_empty() {
                        let epoch_now = self.scheduler.current_epoch();
                        tracing::info!(
                            groups_count = groups.len(),
                            epoch = epoch_now,
                            "GROUP FORMATION throttled — re-distributing existing active_groups to ensure LNs receive announcement"
                        );
                        if let Err(e) = p2p.distribute_groups(&groups, epoch_now).await {
                            tracing::debug!(error = %e, "Throttled redistribute failed (non-fatal)");
                        }
                    }
                }
                return Ok(vec![]);
            }
        }

        // PERF: Healthy group preservation — if all existing groups are active with
        // enough members, skip re-formation to avoid disrupting block production.
        // Re-formation happens only every ~10 minutes (6 epochs) when groups are healthy,
        // or immediately if any group is degraded/empty.
        //
        // BFT STATUS FIX: a `Forming` group is NOT healthy by itself — it means
        // BFT approval has not landed (or a self-formed skeleton inserted by
        // form_groups that still awaits the certificate). Treating Forming as
        // healthy let mn-5 accumulate 16+ self-formed Forming groups across
        // epochs and never re-form, since the throttle accepted those zombies.
        // We now require `status == Active` (BFT-approved) for healthy
        // preservation.
        if !force {
            let active_groups = self.active_groups.read().await;
            if !active_groups.is_empty() {
                let all_healthy = active_groups.values().all(|g| {
                    g.status == GroupStatus::Active
                        && g.members.len() >= self.config.min_group_size
                });
                if all_healthy {
                    let current_epoch = self.scheduler.current_epoch();
                    let epoch_guard = self.current_epoch.read().await;
                    let epochs_since = current_epoch.saturating_sub(*epoch_guard);
                    // Healthy groups: re-form only every 6 epochs (~10 minutes)
                    const HEALTHY_RECHECK_EPOCHS: u64 = 6;
                    if epochs_since < HEALTHY_RECHECK_EPOCHS {
                        info!(
                            active_groups = active_groups.len(),
                            epochs_since,
                            "GROUP FORMATION: Healthy groups — skipping re-formation (next in {} epochs)",
                            HEALTHY_RECHECK_EPOCHS - epochs_since
                        );
                        return Ok(vec![]);
                    }
                    info!(
                        active_groups = active_groups.len(),
                        epochs_since,
                        "GROUP FORMATION: Healthy groups, 10-min recheck — proceeding"
                    );
                }
            }
        }

        // If we have BFT consensus, use leader election before group formation
        if let Some(ref consensus) = self.group_consensus {
            info!("BFT PATH ACTIVE: entering leader election/group formation flow");
            info!(
                auto_initiate = self.auto_initiate,
                "GROUP FORMATION: BFT path active, checking auto_initiate for leader election"
            );
            if self.auto_initiate {
                let registered_count = self.registered_nodes.read().await.len();
                let available_count = consensus.get_available_lightnodes_count().await;
                info!(
                    registered_count,
                    available_count,
                    min_group_size = self.config.min_group_size,
                    "GROUP FORMATION: pre-election lightnode counts"
                );
                if available_count < self.config.min_group_size
                    && registered_count >= self.config.min_group_size
                {
                    info!(
                        registered_count,
                        available_count,
                        "GROUP FORMATION: refreshing available lightnodes from registered nodes"
                    );
                    if let Err(e) = consensus.initialize_available_lightnodes().await {
                        warn!(
                            "GROUP FORMATION: failed to refresh available lightnodes: {}",
                            e
                        );
                    } else {
                        let refreshed_count = consensus.get_available_lightnodes_count().await;
                        info!(
                            refreshed_count,
                            "GROUP FORMATION: available lightnodes refreshed"
                        );
                    }
                }
                if consensus.has_ordered_masternodes().await
                    && consensus.am_i_coordinator_for_current_epoch().await
                {
                    if let Some(proposal) = consensus.create_and_propose_groups().await? {
                        // Update epoch guard for throttling after successful group creation
                        {
                            let mut epoch_guard = self.current_epoch.write().await;
                            *epoch_guard = self.scheduler.current_epoch();
                        }
                        info!(
                            proposal_id = %proposal.proposal_id,
                            groups_count = proposal.groups.len(),
                            "📋 GROUP FORMATION: Coordinatore ha creato proposta (un leader masternode per gruppo)"
                        );
                        let mut pending = self.pending_group_proposal.write().await;
                        *pending = Some(proposal);
                    }
                } else {
                    let election_state = consensus.get_leader_election_state().await;
                    let state_ptr = consensus.leader_election_state_ptr();
                    info!(
                        "GROUP FORMATION: leader election state = {:?} (state_ptr={})",
                        election_state, state_ptr
                    );

                    info!("GROUP FORMATION: entering election_state match");
                    match election_state {
                        LeaderElectionState::Idle => {
                            // Initiate leader election
                            match consensus.initiate_leader_election().await {
                                Ok(Some(proposal)) => {
                                    info!(
                                        election_id = %proposal.election_id,
                                        "🗳️ GROUP FORMATION: Leader election initiated, proposal created"
                                    );
                                    // The proposal needs to be broadcast via P2P.
                                    // Store it so main.rs can pick it up and broadcast.
                                    let mut pending =
                                        self.pending_leader_election_proposal.write().await;
                                    *pending = Some(proposal);
                                }
                                Ok(None) => {
                                    debug!("🗳️ GROUP FORMATION: Leader election not needed or not enough lightnodes");
                                }
                                Err(e) => {
                                    warn!("🗳️ GROUP FORMATION: Failed to initiate leader election: {}", e);
                                }
                            }
                        }
                        LeaderElectionState::Collecting => {
                            // Election in progress, waiting for certificates
                            debug!("🗳️ GROUP FORMATION: Leader election in progress, collecting certificates...");
                        }
                        LeaderElectionState::Elected {
                            ref leader_masternode,
                            ref election_id,
                        } => {
                            if consensus.is_elected_leader().await {
                                // We are the elected leader - create and propose groups
                                info!(
                                    election_id = %election_id,
                                    "🏆 GROUP FORMATION: This masternode is the elected leader, creating groups"
                                );

                                if let Some(proposal) =
                                    consensus.leader_create_and_propose_groups().await?
                                {
                                    // Update epoch guard for throttling after successful group creation
                                    {
                                        let mut epoch_guard = self.current_epoch.write().await;
                                        *epoch_guard = self.scheduler.current_epoch();
                                    }
                                    info!(
                                        proposal_id = %proposal.proposal_id,
                                        groups_count = proposal.groups.len(),
                                        "🏆 GROUP FORMATION: Leader created group proposal for BFT approval"
                                    );
                                    // Store for P2P broadcast by main.rs
                                    let mut pending = self.pending_group_proposal.write().await;
                                    *pending = Some(proposal);
                                } else {
                                    warn!(
                                        election_id = %election_id,
                                        "Leader election completed but no group proposal was created"
                                    );
                                }
                            } else {
                                debug!(
                                    leader = %leader_masternode,
                                    "⏳ GROUP FORMATION: Waiting for elected leader {} to create groups",
                                    leader_masternode
                                );
                            }
                        }
                        LeaderElectionState::CreatingGroups {
                            ref leader_masternode,
                            ref election_id,
                            ..
                        } => {
                            if consensus.is_elected_leader().await {
                                info!("🏆 GROUP FORMATION: Leader creating groups now...");
                                let formed = self.form_groups(true).await.unwrap_or_default();
                                if !formed.is_empty() {
                                    info!(
                                        groups = formed.len(),
                                        "✅ Leader formed {} groups, creating BFT proposal",
                                        formed.len()
                                    );
                                    // Create group proposal for BFT voting
                                    let used_lns: Vec<String> = formed
                                        .iter()
                                        .flat_map(|g| g.members.iter().cloned())
                                        .collect();
                                    let proposal = crate::group_consensus::GroupProposal {
                                        proposal_id: format!(
                                            "group_proposal_{}_{}",
                                            self.current_epoch(),
                                            election_id
                                        ),
                                        epoch: self.current_epoch(),
                                        groups: formed.clone(),
                                        proposer_masternode: leader_masternode.clone(),
                                        used_lightnodes: used_lns,
                                        timestamp: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap_or_default()
                                            .as_secs(),
                                        signature: [0u8; 64], // Will be signed during broadcast
                                    };
                                    *self.pending_group_proposal.write().await = Some(proposal);
                                    info!(
                                        "📋 Group proposal created and pending for BFT broadcast"
                                    );
                                }
                            } else {
                                debug!(
                                    leader = %leader_masternode,
                                    "⏳ GROUP FORMATION: Waiting for leader {} to finish creating groups",
                                    leader_masternode
                                );
                            }
                        }
                    }
                } // fine else (leader election path)
            }

            // 🔒 BFT FIX: Distribuisci SOLO i gruppi approvati tramite BFT (active_groups).
            // I gruppi diventano "active" solo dopo process_approval_certificate() che check
            // che il certificato sia approved=true con abbastanza voti BFT.
            // di approvazione BFT, non ancora votato/approvato.
            {
                let pending_proposal = self.pending_group_proposal.read().await;
                if let Some(ref proposal) = *pending_proposal {
                    info!(
                        proposal_id = %proposal.proposal_id,
                        groups_count = proposal.groups.len(),
                        "📋 BFT PATH: Pending proposal exists (awaiting BFT votes). NOT distributing yet."
                    );
                }
            }

            let active_groups = self.get_active_groups().await;
            if !active_groups.is_empty() {
                info!(
                    groups_count = active_groups.len(),
                    epoch = self.current_epoch(),
                    "✅ BFT PATH: Distributing BFT-approved active groups to lightnodes"
                );

                if let Some(p2p) = p2p_manager {
                    let current_epoch = self.current_epoch();
                    match p2p.distribute_groups(&active_groups, current_epoch).await {
                        Ok(_) => {
                            info!(
                                groups_count = active_groups.len(),
                                epoch = current_epoch,
                                "✅ BFT PATH: Successfully distributed BFT-approved groups"
                            );
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                groups_count = active_groups.len(),
                                "❌ BFT PATH: Failed to distribute BFT-approved groups"
                            );
                        }
                    }
                } else {
                    warn!(
                        groups_count = active_groups.len(),
                        "⚠️ BFT PATH: No P2P manager - distribution skipped"
                    );
                }
                return Ok(active_groups);
            } else {
                debug!("BFT PATH: No BFT-approved groups to distribute yet. Waiting for BFT consensus.");
            }

            // Return currently active groups
            return Ok(self.get_active_groups().await);
        }

        info!("GROUP FORMATION: Using fallback (non-BFT) group formation");
        // Fallback to original group formation without BFT
        let groups = self.form_groups(force).await?;

        if !groups.is_empty() {
            info!(
                groups_count = groups.len(),
                "Formed groups, attempting distribution"
            );

            // If P2P manager is provided, distribute the groups
            if let Some(p2p) = p2p_manager {
                let current_epoch = self.current_epoch();
                if let Err(e) = p2p.distribute_groups(&groups, current_epoch).await {
                    warn!(error = %e, "Failed to distribute groups to light nodes");
                } else {
                    info!("Successfully distributed groups to light nodes");
                }
            } else {
                warn!("No P2P manager provided - groups formed but not distributed");
            }
        }

        Ok(groups)
    }

    /// Form groups based on current registered nodes
    pub async fn form_groups(&self, force: bool) -> Result<Vec<P2PGroup>> {
        let current_epoch = self.scheduler.current_epoch();
        let mut epoch_guard = self.current_epoch.write().await;

        // Check if we should form groups
        let force = force || current_epoch == 0;
        if !force && (current_epoch - *epoch_guard) < self.config.formation_interval_epochs {
            info!("⏸️ GROUP FORMATION: Skipping group formation - not time yet (epoch: {}, last: {}, interval: {})", 
                current_epoch, *epoch_guard, self.config.formation_interval_epochs);
            return Ok(vec![]);
        }

        *epoch_guard = current_epoch;
        drop(epoch_guard);

        // When reforming groups, reset ALL registered nodes to Free so they
        // can be reassigned. Without this, nodes assigned in a previous epoch
        // stay Assigned and are permanently excluded from future formations.
        {
            let mut registered = self.registered_nodes.write().await;
            for node in registered.values_mut() {
                node.assignment = NodeAssignmentStatus::Free;
            }
        }

        let all_nodes = self.get_registered_nodes().await;
        let nodes: Vec<LightNodeInfo> = all_nodes
            .into_iter()
            .filter(|n| n.assignment == NodeAssignmentStatus::Free)
            .collect();

        if nodes.len() < self.config.min_group_size {
            info!(
                free_nodes = nodes.len(),
                required = self.config.min_group_size,
                "GROUP FORMATION: Not enough free nodes (waiting for ≥{} free LNs)",
                self.config.min_group_size
            );
            return Ok(vec![]);
        }

        info!(
            epoch = current_epoch,
            free_nodes = nodes.len(),
            "🚀 GROUP FORMATION: Starting group formation (free nodes only)"
        );

        // Sort nodes for group assignment.
        //
        // *every* time, which meant an LN's bucket index changed between
        // re-formations whenever its PoU score moved in the ranking. Under
        // load PoU fluctuates constantly → indices churn → the B1
        // index-based cert-filter fallback can't match → commits stall.
        //
        // Policy: default to a *deterministic* ordering derived from peer_id,
        // so the same LN lands in the same group_index across emergency
        // re-formations. Every REBALANCE_PERIOD_EPOCHS we drop back to PoU-
        // sort for a full rebalance (gives slow peers a chance to migrate
        // away from busy groups).
        const REBALANCE_PERIOD_EPOCHS: u64 = 60; // ~100 min at 100s/epoch
        let is_rebalance_epoch = current_epoch > 0 && current_epoch % REBALANCE_PERIOD_EPOCHS == 0;
        let mut sorted_nodes = nodes;
        if is_rebalance_epoch {
            info!(
                epoch = current_epoch,
                period = REBALANCE_PERIOD_EPOCHS,
                "GROUP FORMATION: rebalance window — sorting by PoU score"
            );
            sorted_nodes.sort_by(|a, b| {
                b.pou_score
                    .partial_cmp(&a.pou_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        } else {
            // Sticky: deterministic by peer_id hash. Same peer → same rank
            // across re-formations as long as the registered set and
            // num_groups don't change.
            sorted_nodes.sort_by(|a, b| {
                Self::peer_bucket_hash(&a.peer_id)
                    .cmp(&Self::peer_bucket_hash(&b.peer_id))
                    .then_with(|| a.peer_id.cmp(&b.peer_id))
            });
        }

        // Create groups with geographic distribution if enabled
        let groups = if self.config.geographic_distribution {
            self.create_geographically_distributed_groups(&sorted_nodes, current_epoch)
                .await?
        } else {
            self.create_simple_groups(&sorted_nodes, current_epoch)
                .await?
        };

        // Always assign the local MN as leader and insert into active_groups immediately.
        // When BFT is active, the BFT approval path (group_consensus.rs:1069 →
        // add_active_groups) will overwrite with the correct proposer-as-leader later.
        // Without this immediate insert, role detection fails (group not found in
        // active_groups) and all MNs become Participant until BFT completes.
        let mut groups = groups;
        let local_id = self.scheduler.config.local_id.clone();
        for group in groups.iter_mut() {
            group.group_leader_masternode = Some(local_id.clone());
        }
        {
            let mut active_groups = self.active_groups.write().await;
            for group in &groups {
                active_groups.insert(group.group_id.clone(), group.clone());
            }
        }

        info!(
            groups_formed = groups.len(),
            epoch = current_epoch,
            "✅ GROUP FORMATION: Group formation completed successfully"
        );

        for (i, group) in groups.iter().enumerate() {
            info!(
                "📋 GROUP {}: {} members, leader={:?}, backup={:?}",
                i + 1,
                group.members.len(),
                group.group_leader_masternode,
                group.backup_leader_masternode,
            );
        }

        Ok(groups)
    }

    /// Evenly partition `nodes` into groups, returning slice boundaries.
    ///
    /// Given N nodes and G target groups, each group gets either
    /// `floor(N/G)` or `ceil(N/G)` members — the difference between
    /// the largest and smallest group is always at most 1.
    fn even_partition(total: usize, num_groups: usize) -> Vec<(usize, usize)> {
        let base = total / num_groups;
        let extra = total % num_groups;
        let mut slices = Vec::with_capacity(num_groups);
        let mut offset = 0;
        for i in 0..num_groups {
            let size = if i < extra { base + 1 } else { base };
            slices.push((offset, offset + size));
            offset += size;
        }
        slices
    }

    /// Create simple groups without geographic distribution
    async fn create_simple_groups(
        &self,
        nodes: &[LightNodeInfo],
        epoch: u64,
    ) -> Result<Vec<P2PGroup>> {
        let num_groups = self.calculate_num_groups(nodes.len());
        if num_groups == 0 {
            return Ok(Vec::new());
        }

        let slices = Self::even_partition(nodes.len(), num_groups);
        info!(
            total_nodes = nodes.len(),
            num_groups,
            sizes = %slices.iter().map(|(a, b)| (b - a).to_string()).collect::<Vec<_>>().join(","),
            "GROUP FORMATION: creating evenly distributed groups"
        );

        let mut groups: Vec<P2PGroup> = Vec::with_capacity(num_groups);
        for (i, (start, end)) in slices.iter().enumerate() {
            let group = self
                .create_group_from_chunk(&nodes[*start..*end], epoch, i)
                .await?;
            groups.push(group);
        }

        Self::assign_shards_round_robin(&mut groups);

        Ok(groups)
    }

    /// Stable hash used to deterministically bucket a peer across
    /// re-formations. We use sha2 so the mapping is architecture-independent
    /// (unlike `DefaultHasher`, which is permitted to change across Rust
    /// versions) and take the first 8 bytes as a u64 for sorting.
    fn peer_bucket_hash(peer_id: &str) -> u64 {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(peer_id.as_bytes());
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&digest[..8]);
        u64::from_be_bytes(buf)
    }

    /// Create geographically distributed groups.
    ///
    /// deterministic — HashMap iteration order is random in Rust, so two
    /// re-formations with identical inputs previously produced different
    /// `group_index` assignments. We now sort regions lexicographically
    /// before walking them.
    async fn create_geographically_distributed_groups(
        &self,
        nodes: &[LightNodeInfo],
        epoch: u64,
    ) -> Result<Vec<P2PGroup>> {
        let mut groups: Vec<P2PGroup> = Vec::new();

        // Group nodes by geographic region (nodes were pre-sorted by the caller
        // with the appropriate deterministic / rebalance policy).
        let mut regional_nodes: HashMap<String, Vec<LightNodeInfo>> = HashMap::new();
        for node in nodes {
            regional_nodes
                .entry(node.geographic_region.clone())
                .or_default()
                .push(node.clone());
        }
        // Deterministic region order: sort by region name.
        let mut regions: Vec<String> = regional_nodes.keys().cloned().collect();
        regions.sort();

        let mut group_index = 0;
        for region in regions {
            let region_nodes = regional_nodes.remove(&region).unwrap_or_default();
            let num_groups = self.calculate_num_groups(region_nodes.len());
            if num_groups == 0 {
                continue;
            }

            let slices = Self::even_partition(region_nodes.len(), num_groups);
            for (start, end) in &slices {
                let mut group = self
                    .create_group_from_chunk(&region_nodes[*start..*end], epoch, group_index)
                    .await?;
                group.geographic_region = region.clone();
                groups.push(group);
                group_index += 1;
            }
        }

        // With geographic_distribution=true (the default) groups were
        // produced without shards, so the LN shard_filter never activated
        // and every proposer drained the whole mempool → under multi-group
        // the same sender's TXs were split across concurrent blocks,
        // producing the `tx_nonce=5 expected=0` cascade seen on testnet
        Self::assign_shards_round_robin(&mut groups);

        Ok(groups)
    }

    /// Distribute the 65,536-shard space across `groups` round-robin by
    /// group index, so `shard_id % num_groups == group_idx` (deterministic,
    /// any node can independently compute ownership). Each group gets
    /// 65536 / N shards. No-op if groups is empty.
    fn assign_shards_round_robin(groups: &mut [P2PGroup]) {
        let num_groups = groups.len();
        if num_groups == 0 {
            return;
        }
        let shard_count = 65_536u32;
        for (group_idx, group) in groups.iter_mut().enumerate() {
            let mut shards = Vec::new();
            for shard_id in 0..shard_count {
                if (shard_id as usize) % num_groups == group_idx {
                    shards.push(shard_id);
                }
            }
            group.assigned_shards = shards;
        }
    }

    /// Create a group from a chunk of nodes
    async fn create_group_from_chunk(
        &self,
        nodes: &[LightNodeInfo],
        epoch: u64,
        index: usize,
    ) -> Result<P2PGroup> {
        let members: Vec<String> = nodes.iter().map(|n| n.peer_id.clone()).collect();

        // Select proposer as highest PoU score
        let proposer = nodes
            .iter()
            .max_by(|a, b| {
                a.pou_score
                    .partial_cmp(&b.pou_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|n| n.peer_id.clone());

        // created_at is still used for the P2PGroup struct field (wall-clock time),
        // but NOT for the group_id string — see below.
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();

        // Previously each MN used SystemTime::now() as the timestamp suffix,
        // causing MN1 to create group_10_0_1773180363 while MN3 created
        // group_10_0_1773180367 (4s apart). LNs adopted one version, but other
        // MNs rejected election certs for the "wrong" group_id.
        // Using epoch as the suffix ensures all MNs produce the same group_id
        // for the same (epoch, index) pair.
        //
        // the group is a SINGLETON that must survive across epochs — the same
        // logical chain. Embedding epoch in the id renames the group every epoch
        // (group_0_0_0 → group_1_0_1 → …), which fragments per-group cert
        // tracking (B1/B2 map keyed by group_id) and makes the vote aggregator
        // treat each epoch as a fresh lane. Under the gate we anchor the id to
        // the index only so the group identity is stable.
        let force_single = std::env::var("SAVITRI_FORCE_SINGLE_GROUP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let group_id = if force_single {
            format!("group_singleton_{}", index)
        } else {
            format!("group_{}_{}_{}", epoch, index, epoch)
        };

        // Only include nodes with valid, dialable multiaddr.
        // Exclude empty, "unknown", tcp/0, and private IPs.
        let member_multiaddrs: HashMap<String, String> = nodes
            .iter()
            .filter(|n| {
                let m = &n.multiaddr;
                !m.is_empty()
                    && m != "unknown"
                    && !m.contains("tcp/0")
                    && !m.contains("/ip4/172.")
                    && !m.contains("/ip4/10.")
                    && !m.contains("/ip4/192.168.")
                    && !m.contains("/ip4/0.0.0.0")
            })
            .map(|n| (n.peer_id.clone(), n.multiaddr.clone()))
            .collect();

        let group = P2PGroup {
            group_id: group_id.clone(),
            members,
            member_multiaddrs,
            proposer,
            group_leader_masternode: None, // impostato dal consensus quando costruisce la proposta
            backup_leader_masternode: None, // impostato dal consensus insieme al leader
            created_at,
            epoch,
            geographic_region: "unknown".to_string(), // Will be set by caller
            status: GroupStatus::Forming,
            health_score: self.calculate_group_health(nodes).await,
            assigned_shards: Vec::new(), // Populated after all groups are formed
        };

        info!(
            group_id = %group_id,
            members_count = group.members.len(),
            proposer = ?group.proposer,
            "Created P2P group"
        );

        Ok(group)
    }

    /// Calculate optimal group size based on available nodes.
    ///
    /// Computes group size so that groups have at least `min_group_size` members
    /// (with an absolute floor of 2 to guarantee multi-member groups for elections).
    /// When there are fewer nodes than `target_groups * min_group_size`, fewer but
    /// Calculate how many groups to form for a given number of nodes.
    ///
    /// Returns 0 if there are not enough nodes for even one valid group.
    /// Otherwise returns a number of groups such that each group will have
    /// between `min_group_size` and `max_group_size` members, distributed
    /// as evenly as possible (difference between largest and smallest is at most 1).
    fn calculate_num_groups(&self, total_nodes: usize) -> usize {
        let effective_min = self.config.min_group_size.max(2);

        if total_nodes < effective_min {
            return 0;
        }

        // Testnet gate: force a single group so the BFT vote aggregator
        // doesn't split votes across concurrent per-group proposals for the
        // per-group DAG-lane wiring is verified end-to-end and the
        // VoteAggregator correctly partitions quorum by lane.
        if std::env::var("SAVITRI_FORCE_SINGLE_GROUP")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            tracing::info!(
                total_nodes,
                "calculate_num_groups: SAVITRI_FORCE_SINGLE_GROUP=1 — returning 1"
            );
            return 1;
        }

        // Maximum groups we can form while respecting minimum size
        let max_possible = total_nodes / effective_min;
        if max_possible == 0 {
            return 0;
        }

        // Minimum groups needed (at least 1 if we have enough nodes)
        let min_needed = 1;

        // Target: as close to target_groups as possible, clamped to [min_needed, max_possible]
        let num = self.config.target_groups.max(min_needed).min(max_possible);

        num.max(1)
    }

    /// Calculate group health score
    async fn calculate_group_health(&self, nodes: &[LightNodeInfo]) -> f64 {
        if nodes.is_empty() {
            return 0.0;
        }

        let avg_pou_score: f64 =
            nodes.iter().map(|n| n.pou_score).sum::<f64>() / nodes.len() as f64;
        let avg_uptime: f64 =
            nodes.iter().map(|n| n.uptime_percentage).sum::<f64>() / nodes.len() as f64;

        // Health score is weighted average of PoU and uptime
        (avg_pou_score * 0.7) + (avg_uptime * 0.3)
    }

    /// Get active groups
    pub async fn get_active_groups(&self) -> Vec<P2PGroup> {
        let groups = self.active_groups.read().await;
        groups.values().cloned().collect()
    }

    /// Add groups to active groups
    pub async fn add_active_groups(&self, groups: &[P2PGroup]) -> Result<()> {
        let mut active_groups = self.active_groups.write().await;
        for group in groups {
            active_groups.insert(group.group_id.clone(), group.clone());
        }
        // Mark all members as Assigned in the registry
        let mut registered = self.registered_nodes.write().await;
        for group in groups {
            for member_id in &group.members {
                if let Some(node) = registered.get_mut(member_id) {
                    node.assignment = NodeAssignmentStatus::Assigned {
                        group_id: group.group_id.clone(),
                    };
                }
            }
        }
        info!(
            groups_count = groups.len(),
            "Marked group members as Assigned in registry"
        );
        Ok(())
    }

    /// Update group status
    pub async fn update_group_status(&self, group_id: &str, status: GroupStatus) -> Result<()> {
        let mut groups = self.active_groups.write().await;

        if let Some(group) = groups.get_mut(group_id) {
            let status_clone = status.clone();
            group.status = status;
            info!(
                group_id = %group_id,
                status = ?status_clone,
                "Updated group status"
            );
        } else {
            warn!(group_id = %group_id, "Group not found for status update");
        }

        Ok(())
    }

    /// Elect new proposer for a group based on current PoU scores
    pub async fn elect_proposer(&self, group_id: &str) -> Result<Option<String>> {
        let groups = self.active_groups.read().await;
        let nodes = self.registered_nodes.read().await;

        if let Some(group) = groups.get(group_id) {
            // Find group members with current PoU scores
            let mut member_nodes = Vec::new();
            for member_id in &group.members {
                if let Some(node_info) = nodes.values().find(|n| &n.peer_id == member_id) {
                    member_nodes.push(node_info.clone());
                }
            }

            // Select member with highest PoU score
            let new_proposer = member_nodes
                .iter()
                .max_by(|a, b| {
                    a.pou_score
                        .partial_cmp(&b.pou_score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|n| n.peer_id.clone());

            if let Some(ref proposer) = new_proposer {
                info!(
                    group_id = %group_id,
                    proposer = %proposer,
                    "Elected new proposer for group"
                );
            }

            Ok(new_proposer)
        } else {
            Err(anyhow::anyhow!("Group {} not found", group_id))
        }
    }

    /// Cleanup inactive nodes and groups.
    /// - Rimuove i light node inattivi dal registro.
    ///   è disconnesso, bannato o non raggiungibile.
    pub async fn cleanup_inactive(&self) -> Result<()> {
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        // 1. Cleanup inactive nodes
        {
            let mut nodes = self.registered_nodes.write().await;
            nodes.retain(|node_id, node_info| {
                let is_active =
                    (current_time - node_info.last_seen) < self.config.node_timeout_secs;
                if !is_active {
                    info!("Removing inactive node: {}", node_id);
                }
                is_active
            });
        }

        let registered_node_ids: std::collections::HashSet<String> = {
            let nodes = self.registered_nodes.read().await;
            nodes.keys().cloned().collect()
        };

        // 2. Dissolution gruppi (55% threshold) e passaggio proprietà masternode
        {
            let available_masternode_ids = if let Some(ref consensus) = self.group_consensus {
                consensus.get_available_masternode_ids().await
            } else {
                Vec::new()
            };

            let epoch = *self.current_epoch.read().await;

            let mut groups = self.active_groups.write().await;
            for (group_id, group) in groups.iter_mut() {
                if group.status == GroupStatus::Dissolving {
                    continue;
                }

                let total_members = group.members.len();
                if total_members == 0 {
                    continue;
                }

                // Conta membri ancora attivi (registrati e non timeout)
                let active_members = group
                    .members
                    .iter()
                    .filter(|m| registered_node_ids.contains(*m))
                    .count();
                let disconnected_ratio = 1.0 - (active_members as f64 / total_members as f64);

                if disconnected_ratio >= self.config.dissolution_disconnect_threshold {
                    warn!(
                        group_id = %group_id,
                        active_members,
                        total_members,
                        disconnected_ratio = format!("{:.1}%", disconnected_ratio * 100.0),
                        threshold = format!("{:.1}%", self.config.dissolution_disconnect_threshold * 100.0),
                        "🔴 GROUP DISSOLUTION: Marking group as Dissolving (≥{}% members disconnected)",
                        self.config.dissolution_disconnect_threshold * 100.0
                    );
                    group.status = GroupStatus::Dissolving;
                    continue;
                }

                // ENHANCED: Warning if approaching dissolution threshold (50% disconnected)
                // This allows monitoring and potential recovery before dissolution
                if disconnected_ratio >= 0.50
                    && disconnected_ratio < self.config.dissolution_disconnect_threshold
                {
                    warn!(
                        group_id = %group_id,
                        active_members,
                        total_members,
                        disconnected_ratio = format!("{:.1}%", disconnected_ratio * 100.0),
                        threshold = format!("{:.1}%", self.config.dissolution_disconnect_threshold * 100.0),
                        "⚠️ GROUP WARNING: Group approaching dissolution threshold ({}% disconnected)",
                        disconnected_ratio * 100.0
                    );
                }

                if let Some(ref leader_id) = group.group_leader_masternode {
                    let leader_available = if let Some(ref consensus) = self.group_consensus {
                        consensus.is_masternode_available(leader_id).await
                    } else {
                        available_masternode_ids.contains(leader_id)
                    };

                    if !leader_available && !available_masternode_ids.is_empty() {
                        // Deterministic leader selection: seed RNG from epoch,
                        let mut seed_ids: Vec<String> =
                            available_masternode_ids.iter().cloned().collect();
                        seed_ids.push(group_id.clone());
                        seed_ids.sort();
                        let seed = deterministic_seed(epoch, &seed_ids);
                        let mut rng = ChaCha20Rng::from_seed(seed);
                        let idx = rng.gen_range(0..available_masternode_ids.len());
                        let new_leader = available_masternode_ids[idx].clone();

                        info!(
                            group_id = %group_id,
                            old_leader = %leader_id,
                            new_leader = %new_leader,
                            "🔄 GROUP OWNERSHIP: Transferring group ownership (leader disconnected/banned/unreachable)"
                        );
                        group.group_leader_masternode = Some(new_leader);
                    }
                }
            }
        }

        // 3. Rimuovi gruppi dissolti e libera i loro membri
        {
            let mut groups = self.active_groups.write().await;
            let mut freed_members: Vec<String> = Vec::new();
            groups.retain(|group_id, group| {
                let should_keep = group.status != GroupStatus::Dissolving;
                if !should_keep {
                    info!(
                        "Removing dissolved group: {} — freeing {} members",
                        group_id,
                        group.members.len()
                    );
                    freed_members.extend(group.members.clone());
                }
                should_keep
            });
            // Restore freed members to Free status in the registry
            if !freed_members.is_empty() {
                let mut registered = self.registered_nodes.write().await;
                for member_id in &freed_members {
                    if let Some(node) = registered.get_mut(member_id) {
                        node.assignment = NodeAssignmentStatus::Free;
                    }
                }
                info!(
                    freed_count = freed_members.len(),
                    "Freed dissolved group members → Free status"
                );
            }
        }

        Ok(())
    }

    /// Start the group formation manager
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting group formation manager");

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        // Start periodic cleanup task
        let cleanup_manager = self.clone();
        let mut cleanup_shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                cleanup_manager.config.health_check_interval_secs,
            ));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = cleanup_manager.cleanup_inactive().await {
                            error!("Cleanup failed: {}", e);
                        }
                    }
                    _ = cleanup_shutdown_rx.changed() => {
                        if *cleanup_shutdown_rx.borrow() {
                            info!("Cleanup task shutting down gracefully");
                            break;
                        }
                    }
                }
            }
        });

        // Start epoch-based group formation
        let formation_manager = self.clone();
        tokio::spawn(async move {
            let mut last_epoch = 0;
            let mut last_registered_count = 0usize;
            let mut formation_shutdown_rx = shutdown_rx.clone();

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                        let current_epoch = formation_manager.scheduler.current_epoch();
                        let registered_count = formation_manager.registered_nodes.read().await.len();

                        // form_groups not only on epoch advance but ALSO when the
                        // registered_nodes count changes within the same epoch.
                        // (e.g. via task #17 EPOCH TRANSITION rebroadcast) had to
                        // wait until the next epoch boundary (~16.7 min with
                        // slots_per_epoch=200) to be assigned to a group. In the
                        // meantime its old group_id stayed valid but the gossipsub
                        // TX topic /savitri/group/<old>/tx didn't match the
                        // proposer's view in the current epoch — mempool of the
                        let epoch_advanced = current_epoch > last_epoch;
                        let registration_changed = registered_count != last_registered_count;
                        if epoch_advanced || registration_changed {
                            // Force=true for both triggers: form_groups has an
                            // internal cooldown via formation_interval_epochs;
                            // mid-epoch retriggers must override it to actually
                            // reassign newly-registered LNs.
                            if let Err(e) = formation_manager.form_groups(true).await {
                                error!("Group formation failed: {}", e);
                            }
                            last_epoch = current_epoch;
                            last_registered_count = registered_count;
                        }
                    }
                    _ = formation_shutdown_rx.changed() => {
                        if *formation_shutdown_rx.borrow() {
                            info!("Group formation task shutting down gracefully");
                            break;
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the group formation manager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping group formation manager");

        if let Some(ref shutdown_tx) = self.shutdown_tx {
            let _ = shutdown_tx.send(true);
        }

        Ok(())
    }

    /// Get current epoch
    pub fn current_epoch(&self) -> u64 {
        self.scheduler.current_epoch()
    }

    /// Take pending leader election proposal for P2P broadcast (returns None if no pending proposal)
    pub async fn take_pending_leader_election_proposal(
        &self,
    ) -> Option<super::group_consensus::LeaderElectionProposal> {
        let mut pending = self.pending_leader_election_proposal.write().await;
        pending.take()
    }

    /// Take pending group proposal for P2P broadcast (returns None if no pending proposal)
    pub async fn take_pending_group_proposal(&self) -> Option<GroupProposal> {
        let mut pending = self.pending_group_proposal.write().await;
        pending.take()
    }

    /// Take pending group vote for P2P broadcast (returns None if no pending vote)
    pub async fn take_pending_group_vote(&self) -> Option<GroupVote> {
        let mut pending = self.pending_group_vote.write().await;
        pending.take()
    }

    /// Handle incoming group proposal from the network
    pub async fn handle_proposal(
        &mut self,
        proposal: &serde_json::Value,
        source: &str,
    ) -> Result<()> {
        info!("Handling group proposal from {}: {:?}", source, proposal);

        // Parse the proposal from JSON
        let group_proposal: GroupProposal =
            serde_json::from_value(proposal.clone()).context("Failed to parse group proposal")?;

        info!(
            proposal_id = %group_proposal.proposal_id,
            epoch = group_proposal.epoch,
            groups_count = group_proposal.groups.len(),
            proposer = %group_proposal.proposer_masternode,
            "Processing group proposal"
        );

        // Validate proposal
        if let Err(e) = self.validate_proposal(&group_proposal).await {
            warn!(
                proposal_id = %group_proposal.proposal_id,
                error = %e,
                "Proposal validation failed"
            );
            return Err(e);
        }

        // If we have group consensus, forward to consensus manager
        if let Some(ref consensus) = self.group_consensus {
            info!("Forwarding proposal to consensus manager");
            let vote = consensus.process_proposal(group_proposal.clone()).await?;
            info!(
                proposal_id = %vote.proposal_id,
                vote_type = ?vote.vote_type,
                "🔔 SOLUZIONE: Consensus produced group vote; publishing immediately"
            );

            let publish_tx = self.masternode_publish_tx.read().await;
            if let Some(ref tx) = *publish_tx {
                let message = MasternodeMessage::GroupVote(vote.clone());
                match tx.send(message) {
                    Ok(()) => {
                        info!(
                            proposal_id = %vote.proposal_id,
                            vote_type = ?vote.vote_type,
                            "✅ SOLUZIONE: Group vote published immediately via masternode_publish_tx"
                        );
                    }
                    Err(e) => {
                        error!(
                            proposal_id = %vote.proposal_id,
                            error = %e,
                            "❌ SOLUZIONE: Failed to publish vote immediately, falling back to queue"
                        );
                        let mut pending = self.pending_group_vote.write().await;
                        *pending = Some(vote);
                    }
                }
            } else {
                warn!(
                    proposal_id = %vote.proposal_id,
                    "⚠️ SOLUZIONE: masternode_publish_tx not configured, falling back to queue"
                );
                let mut pending = self.pending_group_vote.write().await;
                *pending = Some(vote);
            }
        } else {
            // Directly accept the proposal if no consensus manager
            info!("No consensus manager - directly accepting proposal");

            // Add groups to active groups
            for group in &group_proposal.groups {
                let mut active_groups = self.active_groups.write().await;
                active_groups.insert(group.group_id.clone(), group.clone());
                info!(
                    "Added group {} from proposal {}",
                    group.group_id, group_proposal.proposal_id
                );
            }
        }

        info!(
            proposal_id = %group_proposal.proposal_id,
            "Successfully processed group proposal from {}",
            source
        );

        Ok(())
    }

    /// Validate a group proposal
    async fn validate_proposal(&self, proposal: &GroupProposal) -> Result<()> {
        // Check if proposal is for current or future epoch
        let current_epoch = self.current_epoch();
        if proposal.epoch < current_epoch {
            return Err(anyhow::anyhow!(
                "Proposal epoch {} is in the past (current: {})",
                proposal.epoch,
                current_epoch
            ));
        }

        // Check if groups have valid members
        for group in &proposal.groups {
            if group.members.len() < self.config.min_group_size {
                return Err(anyhow::anyhow!(
                    "Group {} has insufficient members: {} < {}",
                    group.group_id,
                    group.members.len(),
                    self.config.min_group_size
                ));
            }

            if group.members.len() > self.config.max_group_size {
                return Err(anyhow::anyhow!(
                    "Group {} has too many members: {} > {}",
                    group.group_id,
                    group.members.len(),
                    self.config.max_group_size
                ));
            }
        }

        // Check if used lightnodes are registered (with tolerance for registration delays)
        let registered_nodes = self.registered_nodes.read().await;
        let total_used = proposal.used_lightnodes.len();
        let mut missing_nodes: Vec<String> = Vec::new();
        for node_id in &proposal.used_lightnodes {
            if !registered_nodes.contains_key(node_id) {
                missing_nodes.push(node_id.clone());
            }
        }
        drop(registered_nodes);

        if !missing_nodes.is_empty() {
            // Allow up to 10% missing registrations to tolerate race conditions
            // where the leader registered lightnodes that this MN hasn't seen yet
            let missing_ratio = missing_nodes.len() as f64 / total_used.max(1) as f64;
            if missing_ratio > 0.10 {
                return Err(anyhow::anyhow!(
                    "{} of {} lightnodes not registered (>{:.0}% threshold): {:?}",
                    missing_nodes.len(),
                    total_used,
                    0.10 * 100.0,
                    &missing_nodes[..std::cmp::min(missing_nodes.len(), 3)]
                ));
            } else {
                warn!(
                    missing_count = missing_nodes.len(),
                    total = total_used,
                    "⚠️ GROUP VALIDATION: Some lightnodes not yet registered locally, but within tolerance ({:.1}% < 10%)",
                    missing_ratio * 100.0
                );
            }
        }

        // Check proposal timestamp (should be recent)
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if current_time.saturating_sub(proposal.timestamp) > 300 {
            // 5 minutes
            return Err(anyhow::anyhow!(
                "Proposal timestamp is too old: {} seconds ago",
                current_time.saturating_sub(proposal.timestamp)
            ));
        }

        Ok(())
    }

    /// Handle incoming group vote from the network
    pub async fn handle_vote(&mut self, vote: &serde_json::Value, source: &str) -> Result<()> {
        info!("Handling group vote from {}: {:?}", source, vote);

        // Parse the vote from JSON
        let group_vote: GroupVote =
            serde_json::from_value(vote.clone()).context("Failed to parse group vote")?;

        info!(
            proposal_id = %group_vote.proposal_id,
            voter = %group_vote.voter_masternode,
            vote_type = ?group_vote.vote_type,
            "Processing group vote"
        );

        // Validate vote
        if let Err(e) = self.validate_vote(&group_vote).await {
            warn!(
                proposal_id = %group_vote.proposal_id,
                voter = %group_vote.voter_masternode,
                error = %e,
                "Vote validation failed"
            );
            return Err(e);
        }

        // If we have group consensus, forward to consensus manager
        if let Some(ref consensus) = self.group_consensus {
            info!("Forwarding vote to consensus manager");
            let certificate = consensus.process_vote(group_vote.clone()).await?;
            if let Some(certificate) = certificate {
                info!(
                    proposal_id = %certificate.proposal.proposal_id,
                    groups_count = certificate.proposal.groups.len(),
                    "Group consensus reached in group formation; processing approval certificate"
                );
                consensus.process_approval_certificate(certificate).await?;
            } else {
                info!(
                    proposal_id = %group_vote.proposal_id,
                    "Group vote processed; no approval certificate yet"
                );
            }
        } else {
            // No consensus manager - just log the vote
            info!("No consensus manager - logging vote only");
        }

        info!(
            proposal_id = %group_vote.proposal_id,
            voter = %group_vote.voter_masternode,
            "Successfully processed vote from {}",
            source
        );

        Ok(())
    }

    /// Validate a group vote
    async fn validate_vote(&self, vote: &GroupVote) -> Result<()> {
        // Check if proposal exists
        let active_groups = self.active_groups.read().await;

        // In a real implementation, we'd check against active proposals

        // Check voter ID is not empty
        if vote.voter_masternode.is_empty() {
            return Err(anyhow::anyhow!("Voter masternode ID cannot be empty"));
        }

        // Check proposal ID is not empty
        if vote.proposal_id.is_empty() {
            return Err(anyhow::anyhow!("Proposal ID cannot be empty"));
        }

        // Check vote timestamp (should be recent)
        let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if current_time.saturating_sub(vote.timestamp) > 300 {
            // 5 minutes
            return Err(anyhow::anyhow!(
                "Vote timestamp is too old: {} seconds ago",
                current_time.saturating_sub(vote.timestamp)
            ));
        }

        Ok(())
    }

    /// Handle incoming group sync from the network
    pub async fn handle_sync(&mut self, sync: &serde_json::Value, source: &str) -> Result<()> {
        info!("Handling group sync from {}: {:?}", source, sync);

        // Parse sync message to determine type
        if let Ok(sync_request) =
            serde_json::from_value::<crate::masternode_p2p::MasternodeMessage>(sync.clone())
        {
            match sync_request {
                crate::masternode_p2p::MasternodeMessage::GroupSyncRequest {
                    from_epoch,
                    to_epoch,
                    requester_masternode,
                } => {
                    info!(
                        "Processing group sync request from {} for epochs {}-{}",
                        requester_masternode, from_epoch, to_epoch
                    );

                    // Collect approved certificates for the requested epoch range
                    let certificates = self
                        .collect_certificates_for_epoch_range(from_epoch, to_epoch)
                        .await?;

                    info!("Collected {} certificates for sync", certificates.len());

                    // In a real implementation, we would send the response back to the requester
                    // For now, we just log what we would send
                    for cert in &certificates {
                        info!(
                            "Would send certificate for proposal {} (epoch {})",
                            cert.proposal.proposal_id, cert.proposal.epoch
                        );
                    }

                    info!("Group sync request processed successfully");
                }
                crate::masternode_p2p::MasternodeMessage::GroupSyncResponse {
                    certificates,
                    responder_masternode,
                } => {
                    info!(
                        "Processing group sync response from {} with {} certificates",
                        responder_masternode,
                        certificates.len()
                    );

                    // Process received certificates
                    for certificate in certificates {
                        if let Err(e) = self.process_approval_certificate(&certificate).await {
                            warn!(
                                proposal_id = %certificate.proposal.proposal_id,
                                error = %e,
                                "Failed to process approval certificate"
                            );
                        } else {
                            info!(
                                "Successfully processed certificate for proposal {}",
                                certificate.proposal.proposal_id
                            );
                        }
                    }

                    info!("Group sync response processed successfully");
                }
                crate::masternode_p2p::MasternodeMessage::GroupApprovalCertificate(certificate) => {
                    // CRITICAL: When we receive a broadcast GroupApprovalCertificate from another MN,
                    // we must add the groups to our active_groups so that block proposal verification
                    // (election cert: attestation signer in group_members) can succeed on this node.
                    info!(
                        proposal_id = %certificate.proposal.proposal_id,
                        groups_count = certificate.proposal.groups.len(),
                        source = %source,
                        "Processing broadcast group approval certificate (adding groups to active_groups)"
                    );
                    if let Err(e) = self.process_approval_certificate(&certificate).await {
                        warn!(
                            proposal_id = %certificate.proposal.proposal_id,
                            error = %e,
                            "Failed to process broadcast approval certificate"
                        );
                    } else {
                        info!(
                            proposal_id = %certificate.proposal.proposal_id,
                            "Successfully added groups from broadcast certificate"
                        );
                    }
                }
                _ => {
                    warn!("Received unexpected message type in group sync handler");
                    return Err(anyhow::anyhow!("Invalid sync message type"));
                }
            }
        } else {
            // Try to parse as a generic sync message
            warn!("Could not parse sync message as known type, treating as generic sync");

            // Generic sync handling - just log and accept
            info!("Generic sync message processed from {}", source);
        }

        Ok(())
    }

    /// Collect approval certificates for a range of epochs
    async fn collect_certificates_for_epoch_range(
        &self,
        from_epoch: u64,
        to_epoch: u64,
    ) -> Result<Vec<GroupApprovalCertificate>> {
        let certificates = Vec::new();

        // If we have group consensus, get certificates from there
        if let Some(ref consensus) = self.group_consensus {
            // Note: In a real implementation, this would call consensus.get_certificates_for_epoch_range()
            // For now, we'll return empty since we don't have access to the consensus certificates
            info!("No certificates available - consensus manager exists but certificates not accessible");
        } else {
            // No consensus manager - no certificates to collect
            info!("No consensus manager - no certificates to collect");
        }

        Ok(certificates)
    }

    /// Process an approval certificate and update groups
    async fn process_approval_certificate(
        &self,
        certificate: &GroupApprovalCertificate,
    ) -> Result<()> {
        if !certificate.approved {
            info!(
                "Certificate for proposal {} is not approved, skipping",
                certificate.proposal.proposal_id
            );
            return Ok(());
        }

        info!(
            proposal_id = %certificate.proposal.proposal_id,
            groups_count = certificate.proposal.groups.len(),
            votes_count = certificate.votes.len(),
            "Processing approved certificate"
        );

        // Add approved groups to active groups
        let mut active_groups = self.active_groups.write().await;
        for group in &certificate.proposal.groups {
            let mut group_clone = group.clone();
            group_clone.status = GroupStatus::Active; // Mark as active since approved

            active_groups.insert(group.group_id.clone(), group_clone);
            info!("Added approved group {} to active groups", group.group_id);
        }

        info!(
            "Successfully processed approval certificate for proposal {}",
            certificate.proposal.proposal_id
        );

        Ok(())
    }
}

impl Clone for GroupFormationManager {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            scheduler: self.scheduler.clone(),
            config: self.config.clone(),
            registered_nodes: self.registered_nodes.clone(),
            active_groups: self.active_groups.clone(),
            current_epoch: self.current_epoch.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            group_consensus: self.group_consensus.clone(),
            auto_initiate: self.auto_initiate,
            pending_leader_election_proposal: self.pending_leader_election_proposal.clone(),
            pending_group_proposal: self.pending_group_proposal.clone(),
            pending_group_vote: self.pending_group_vote.clone(),
            masternode_publish_tx: self.masternode_publish_tx.clone(),
            node_stakes: self.node_stakes.clone(),
            registrations_this_epoch: Arc::new(AtomicU64::new(
                self.registrations_this_epoch.load(AtomicOrdering::SeqCst),
            )),
            registration_epoch: Arc::new(AtomicU64::new(
                self.registration_epoch.load(AtomicOrdering::SeqCst),
            )),
        }
    }
}

/// Group formation manager
pub struct GroupFormation {
    manager: GroupFormationManager,
}

impl GroupFormation {
    pub fn new(manager: GroupFormationManager) -> Self {
        Self { manager }
    }

    pub async fn start(&mut self) -> Result<()> {
        self.manager.start().await
    }

    pub async fn stop(&self) -> Result<()> {
        self.manager.stop().await
    }
}
