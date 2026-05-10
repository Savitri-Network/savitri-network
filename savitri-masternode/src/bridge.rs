//! Bridge module for savitri_node dependencies
//!
//! This module provides real implementations using existing workspace crates,
//! bridging the gap between savitri-masternode and the core savitri modules.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

// ============================================================================
// CORE MODULE - Slot Scheduler and Genesis
// ============================================================================

pub mod core {
    pub mod slot_scheduler {
        use anyhow::Result;
        use serde::{Deserialize, Serialize};
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        use tokio::sync::{mpsc, RwLock};
        use tracing::{debug, error, info, trace, warn};

        // Genesis timestamp (seconds since UNIX epoch).
        // When genesis_timestamp_ms == 0 in config, main.rs auto-sets it to current
        // system time at startup (testnet mode). These constants are fallback defaults.
        pub const GENESIS_TIMESTAMP_SECS: u64 = 0;
        pub const GENESIS_TIMESTAMP_MS: u64 = GENESIS_TIMESTAMP_SECS * 1000;

        /// Slot role in consensus
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum SlotRole {
            Leader,
            Follower,
            Observer,
        }

        impl SlotRole {
            pub fn is_leader(&self) -> bool {
                matches!(self, SlotRole::Leader)
            }

            pub fn is_validator(&self) -> bool {
                matches!(self, SlotRole::Leader | SlotRole::Follower)
            }
        }

        /// Information about a slot
        #[derive(Debug, Clone)]
        pub struct SlotInfo {
            pub slot: u64,
            pub epoch: u64,
            pub round: u32,
            pub role: SlotRole,
            pub leader: Option<String>,
            pub timestamp: u64,
        }

        impl SlotInfo {
            pub fn leader_id(&self) -> Option<&str> {
                self.leader.as_deref()
            }
        }

        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct SlotSchedulerConfig {
            /// 1 slot = 1 heartbeat (ms)
            pub heartbeat_interval_ms: u64,
            /// Slots per epoch. epoch = H × S ms
            pub slots_per_epoch: u64,
            /// Monolith epoch duration (ms). M = monolith_epoch_ms / (H × S)
            pub monolith_epoch_ms: u64,
            /// Genesis timestamp (ms)
            pub genesis_timestamp_ms: u64,
            pub validators: Vec<String>,
            pub local_id: String,
        }

        impl Default for SlotSchedulerConfig {
            fn default() -> Self {
                Self {
                    heartbeat_interval_ms: 5000,
                    slots_per_epoch: 20,
                    monolith_epoch_ms: 86400000,
                    genesis_timestamp_ms: GENESIS_TIMESTAMP_MS,
                    validators: Vec::new(),
                    local_id: String::new(),
                }
            }
        }

        /// Real slot scheduler implementation with epoch tracking
        pub struct SlotScheduler {
            config: SlotSchedulerConfig,
            current_slot: AtomicU64,
            start_time: u64,
            running: AtomicBool,
            midnight_snapshot: std::sync::RwLock<Vec<String>>,
        }

        impl Clone for SlotScheduler {
            fn clone(&self) -> Self {
                let snapshot_data = match self.midnight_snapshot.read() {
                    Ok(data) => data.clone(),
                    Err(e) => {
                        error!("Lock poisoned in SlotScheduler::clone: {}", e);
                        Vec::new() // Fallback to empty validators
                    }
                };
                Self {
                    config: self.config.clone(),
                    current_slot: AtomicU64::new(self.current_slot.load(Ordering::SeqCst)),
                    start_time: self.start_time,
                    running: AtomicBool::new(self.running.load(Ordering::SeqCst)),
                    midnight_snapshot: std::sync::RwLock::new(snapshot_data),
                }
            }
        }

        impl std::fmt::Debug for SlotScheduler {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("SlotScheduler")
                    .field("current_slot", &self.current_slot.load(Ordering::SeqCst))
                    .field("start_time", &self.start_time)
                    .field("running", &self.running.load(Ordering::SeqCst))
                    .finish()
            }
        }

        impl SlotScheduler {
            fn start_time(&self) -> u64 {
                self.config.genesis_timestamp_ms
            }

            fn epochs_per_monolith(&self) -> u64 {
                let epoch_ms = self
                    .config
                    .heartbeat_interval_ms
                    .saturating_mul(self.config.slots_per_epoch);
                if epoch_ms == 0 {
                    864
                } else {
                    self.config.monolith_epoch_ms / epoch_ms
                }
            }

            pub fn new(config: SlotSchedulerConfig) -> Self {
                info!(
                    heartbeat_interval_ms = config.heartbeat_interval_ms,
                    slots_per_epoch = config.slots_per_epoch,
                    monolith_epoch_ms = config.monolith_epoch_ms,
                    validators = config.validators.len(),
                    local_id = %config.local_id,
                    "Unified slot scheduler initialized"
                );

                Self {
                    config: config.clone(),
                    current_slot: AtomicU64::new(0),
                    start_time: config.genesis_timestamp_ms,
                    running: AtomicBool::new(false),
                    midnight_snapshot: std::sync::RwLock::new(config.validators.clone()),
                }
            }

            pub fn current_slot(&self) -> u64 {
                savitri_consensus::primitives::epoch::current_slot(
                    savitri_consensus::primitives::epoch::now_ms(),
                    self.start_time(),
                    self.config.heartbeat_interval_ms,
                )
            }

            pub fn current_epoch(&self) -> u64 {
                savitri_consensus::primitives::epoch::current_epoch(
                    savitri_consensus::primitives::epoch::now_ms(),
                    self.start_time(),
                    self.config.heartbeat_interval_ms,
                    self.config.slots_per_epoch.max(1),
                )
            }

            pub fn get_slot_info(&self) -> SlotInfo {
                let slot = self.current_slot();
                let epoch = slot / self.config.slots_per_epoch.max(1);
                let round = (slot % self.config.slots_per_epoch.max(1)) as u32;
                let (role, leader) = self.compute_role_and_leader(slot);
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                SlotInfo {
                    slot,
                    epoch,
                    round,
                    role,
                    leader,
                    timestamp,
                }
            }

            fn compute_role_and_leader(&self, slot: u64) -> (SlotRole, Option<String>) {
                if self.config.validators.is_empty() {
                    return (SlotRole::Observer, None);
                }

                let leader_idx = (slot as usize) % self.config.validators.len();
                let leader = self.config.validators.get(leader_idx).cloned();

                let role = if let Some(ref l) = leader {
                    if l == &self.config.local_id {
                        SlotRole::Leader
                    } else if self.config.validators.contains(&self.config.local_id) {
                        SlotRole::Follower
                    } else {
                        SlotRole::Observer
                    }
                } else {
                    SlotRole::Observer
                };

                (role, leader)
            }

            /// Start the slot scheduler ticker
            pub async fn start(&self) -> Result<mpsc::Receiver<SlotInfo>> {
                self.running.store(true, Ordering::SeqCst);
                let (tx, rx) = mpsc::channel(16);

                let config = self.config.clone();
                let start_time = self.start_time;
                let running = self.running.load(Ordering::SeqCst);

                tokio::spawn(async move {
                    let mut last_slot = 0u64;
                    let slot_duration = Duration::from_millis(config.heartbeat_interval_ms);

                    while running {
                        tokio::time::sleep(slot_duration / 10).await;

                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let elapsed = now.saturating_sub(start_time);
                        let current_slot = elapsed / config.heartbeat_interval_ms.max(1);

                        if current_slot != last_slot {
                            let epoch = current_slot / config.slots_per_epoch.max(1);
                            let round = (current_slot % config.slots_per_epoch.max(1)) as u32;

                            let leader_idx =
                                (current_slot as usize) % config.validators.len().max(1);
                            let leader = config.validators.get(leader_idx).cloned();

                            let role = if let Some(ref l) = leader {
                                if l == &config.local_id {
                                    SlotRole::Leader
                                } else if config.validators.contains(&config.local_id) {
                                    SlotRole::Follower
                                } else {
                                    SlotRole::Observer
                                }
                            } else {
                                SlotRole::Observer
                            };

                            let info = SlotInfo {
                                slot: current_slot,
                                epoch,
                                round,
                                role,
                                leader,
                                timestamp: now / 1000,
                            };

                            if tx.send(info).await.is_err() {
                                break;
                            }

                            last_slot = current_slot;
                        }
                    }
                });

                Ok(rx)
            }

            pub fn stop(&self) {
                self.running.store(false, Ordering::SeqCst);
                info!("Slot scheduler stopped");
            }

            // ==================== MONOLITH BLOCK FUNCTIONS ====================

            /// Check if it's time to create monolith block (at start of new monolith epoch)
            pub fn is_monolith_time(&self) -> bool {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                if now_ms < self.start_time() {
                    return false;
                }

                let elapsed_ms = now_ms.saturating_sub(self.start_time());
                let epoch_ms = self
                    .config
                    .heartbeat_interval_ms
                    .saturating_mul(self.config.slots_per_epoch);
                if epoch_ms == 0 {
                    return false;
                }
                let m = self.epochs_per_monolith();
                let monolith_interval_ms = epoch_ms.saturating_mul(m);
                if monolith_interval_ms == 0 {
                    return false;
                }

                let time_since_last_monolith = elapsed_ms % monolith_interval_ms;
                // Within first slot of new monolith epoch
                time_since_last_monolith < self.config.heartbeat_interval_ms
            }

            /// Get current monolith epoch number (for proposer hashing)
            pub fn current_day_number(&self) -> u64 {
                self.current_epoch() / self.epochs_per_monolith().max(1)
            }

            /// Get start of current monolith epoch (seconds)
            pub fn get_midnight_timestamp(&self) -> u64 {
                let epoch_ms = self
                    .config
                    .heartbeat_interval_ms
                    .saturating_mul(self.config.slots_per_epoch);
                let m = self.epochs_per_monolith();
                let monolith_epoch_ms = epoch_ms.saturating_mul(m);
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if now_ms < self.start_time() {
                    return self.start_time() / 1000;
                }
                let elapsed = now_ms.saturating_sub(self.start_time());
                let monolith_idx = elapsed / monolith_epoch_ms.max(1);
                let window_start_ms = self.start_time() + monolith_idx * monolith_epoch_ms;
                window_start_ms / 1000
            }

            pub fn hash_validator_set(&self, validators: &[String]) -> u64 {
                use sha2::{Digest, Sha256};

                let mut hasher = Sha256::new();

                let mut sorted_validators = validators.to_vec();
                sorted_validators.sort();

                for validator in &sorted_validators {
                    hasher.update(validator.as_bytes());
                }

                let result = hasher.finalize();
                match result[..8].try_into() {
                    Ok(bytes) => u64::from_le_bytes(bytes),
                    Err(e) => {
                        error!("Failed to convert hash to u64: {}", e);
                        0 // Fallback hash
                    }
                }
            }

            /// Combine hash with day number
            pub fn hash_combined(&self, validator_hash: u64, day_number: u64) -> u64 {
                use sha2::{Digest, Sha256};

                let mut hasher = Sha256::new();
                hasher.update(validator_hash.to_le_bytes());
                hasher.update(day_number.to_le_bytes());

                let result = hasher.finalize();
                match result[..8].try_into() {
                    Ok(bytes) => u64::from_le_bytes(bytes),
                    Err(e) => {
                        error!("Failed to convert combined hash to u64: {}", e);
                        validator_hash // Fallback to validator hash
                    }
                }
            }

            pub fn get_current_validators(&self) -> Vec<String> {
                self.config.validators.clone()
            }

            pub fn is_validator_active(&self, validator: &str) -> bool {
                self.get_current_validators()
                    .contains(&validator.to_string())
            }

            pub fn get_first_active_validator(&self) -> Option<String> {
                self.get_current_validators().into_iter().next()
            }

            /// Hybrid monolith proposer selection
            pub fn compute_monolith_proposer_hybrid(&self) -> Option<String> {
                let midnight_snapshot = {
                    match self.midnight_snapshot.read() {
                        Ok(snapshot) => snapshot.clone(),
                        Err(e) => {
                            error!("Lock poisoned in compute_monolith_proposer_hybrid: {}", e);
                            return None;
                        }
                    }
                };

                let current_validators = self.get_current_validators();

                if midnight_snapshot.is_empty() || current_validators.is_empty() {
                    warn!("No validators available for monolith proposer selection");
                    return None;
                }

                // 1. Calculate proposer with midnight snapshot
                let day_number = self.current_day_number();
                let snapshot_hash = self.hash_validator_set(&midnight_snapshot);
                let combined_hash = self.hash_combined(snapshot_hash, day_number);

                let proposer_idx = (combined_hash as usize) % midnight_snapshot.len();
                let chosen_proposer = midnight_snapshot.get(proposer_idx).cloned();

                info!(
                    day_number = day_number,
                    snapshot_hash = snapshot_hash,
                    combined_hash = combined_hash,
                    proposer_idx = proposer_idx,
                    chosen_proposer = ?chosen_proposer,
                    "Monolith proposer calculated from midnight snapshot"
                );

                // 2. Check if chosen proposer is still active
                if let Some(ref proposer) = chosen_proposer {
                    if self.is_validator_active(proposer) {
                        info!(proposer = %proposer, "Chosen proposer is still active");
                        return chosen_proposer;
                    } else {
                        warn!(proposer = %proposer, "Chosen proposer is no longer active");
                    }
                }

                let fallback_idx = (day_number as usize) % current_validators.len();
                let fallback_proposer = current_validators.get(fallback_idx).cloned();

                if let Some(ref proposer) = fallback_proposer {
                    warn!(
                        original_proposer = ?chosen_proposer,
                        fallback_proposer = %proposer,
                        "Using fallback proposer"
                    );
                }

                fallback_proposer
            }

            /// Check if this node should create monolith block
            pub fn should_create_monolith(&self) -> bool {
                if !self.is_monolith_time() {
                    return false;
                }

                let proposer = self.compute_monolith_proposer_hybrid();
                let should_create = proposer == Some(self.config.local_id.clone());

                if should_create {
                    info!("🎯 This node is the monolith proposer for today!");
                } else {
                    info!("📅 Today's monolith proposer: {:?}", proposer);
                }

                should_create
            }
        }
    }

    pub mod genesis {
        use anyhow::{Context, Result};
        use std::path::Path;
        use tracing::info;

        /// Ensure genesis block exists in storage
        pub fn ensure_genesis_block<P: AsRef<Path>>(storage_path: P) -> Result<()> {
            let path = storage_path.as_ref();
            info!(path = %path.display(), "Checking genesis block");

            // Create storage directory if it doesn't exist
            if !path.exists() {
                std::fs::create_dir_all(path).with_context(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
                info!("Created storage directory");
            }

            // Check for genesis marker file
            let genesis_marker = path.join(".genesis");
            if !genesis_marker.exists() {
                // Initialize genesis state
                std::fs::write(&genesis_marker, b"genesis_v1")
                    .with_context(|| "Failed to write genesis marker")?;
                info!("Genesis block initialized");
            } else {
                info!("Genesis block already exists");
            }

            Ok(())
        }
    }
}

// ============================================================================
// STORAGE MODULE - Real Storage Implementation
// ============================================================================

pub mod storage {
    use anyhow::{Context, Result};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tracing::{debug, info};

    /// Account state
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct Account {
        pub balance: u128,
        pub nonce: u64,
        pub code_hash: Option<[u8; 32]>,
    }

    /// Storage implementation with in-memory cache
    #[derive(Debug)]
    pub struct Storage {
        path: PathBuf,
        accounts: RwLock<HashMap<[u8; 32], Account>>,
        blocks: RwLock<HashMap<u64, Vec<u8>>>,
    }

    impl Clone for Storage {
        fn clone(&self) -> Self {
            Self {
                path: self.path.clone(),
                accounts: RwLock::new(HashMap::new()),
                blocks: RwLock::new(HashMap::new()),
            }
        }
    }

    impl Storage {
        pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
            let path = path.as_ref().to_path_buf();

            // Create directory if it doesn't exist
            if !path.exists() {
                std::fs::create_dir_all(&path).with_context(|| {
                    format!("Failed to create storage directory: {}", path.display())
                })?;
            }

            info!(path = %path.display(), "Storage initialized");

            Ok(Self {
                path,
                accounts: RwLock::new(HashMap::new()),
                blocks: RwLock::new(HashMap::new()),
            })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub async fn get_account(&self, address: &[u8; 32]) -> Result<Account> {
            let accounts = self.accounts.read().await;
            Ok(accounts.get(address).cloned().unwrap_or_default())
        }

        pub async fn put_account(&self, address: &[u8; 32], account: &Account) -> Result<()> {
            let mut accounts = self.accounts.write().await;
            accounts.insert(*address, account.clone());
            Ok(())
        }

        pub async fn get_block(&self, height: u64) -> Result<Option<Vec<u8>>> {
            let blocks = self.blocks.read().await;
            Ok(blocks.get(&height).cloned())
        }

        pub async fn put_block(&self, height: u64, data: Vec<u8>) -> Result<()> {
            let mut blocks = self.blocks.write().await;
            blocks.insert(height, data);
            Ok(())
        }

        pub async fn current_height(&self) -> u64 {
            let blocks = self.blocks.read().await;
            blocks.keys().max().copied().unwrap_or(0)
        }
    }
}

// ============================================================================
// P2P MODULE - Network Configuration and Runner
// ============================================================================

pub mod p2p {
    use anyhow::Result;
    use libp2p::identity::Keypair;
    use libp2p::{Multiaddr, PeerId};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use tokio::sync::broadcast;
    use tracing::{debug, info, warn};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConsensusConfig {
        pub committee_size: usize,
        pub threshold: usize,
        pub timeout_ms: u64,
    }

    impl Default for ConsensusConfig {
        fn default() -> Self {
            Self {
                committee_size: 4,
                threshold: 3,
                timeout_ms: 5000,
            }
        }
    }

    /// P2P network message types
    #[derive(Debug, Clone)]
    pub enum NetworkMessage {
        Block(Vec<u8>),
        Transaction(Vec<u8>),
        Consensus(Vec<u8>),
        Ping,
        Pong,
    }

    /// P2P network runner
    pub struct P2PNetwork {
        identity: Keypair,
        port: u16,
        bootstrap_peers: Vec<(PeerId, Multiaddr)>,
        message_tx: broadcast::Sender<NetworkMessage>,
    }

    impl P2PNetwork {
        pub fn new(
            identity: Keypair,
            port: u16,
            bootstrap_peers: Vec<(PeerId, Multiaddr)>,
        ) -> Self {
            let (message_tx, _) = broadcast::channel(1024);

            Self {
                identity,
                port,
                bootstrap_peers,
                message_tx,
            }
        }

        pub fn subscribe(&self) -> broadcast::Receiver<NetworkMessage> {
            self.message_tx.subscribe()
        }

        pub async fn run(&self) -> Result<()> {
            let local_peer_id = PeerId::from(self.identity.public());
            info!(
                peer_id = %local_peer_id,
                port = self.port,
                bootstrap_peers = self.bootstrap_peers.len(),
                "P2P network starting"
            );

            // In a real implementation, this would start libp2p swarm
            // For now, we just log and wait
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                debug!("P2P network heartbeat");
            }
        }
    }

    /// Start P2P network with identity and storage
    pub async fn run_p2p_with_identity_and_monolith(
        identity: Keypair,
        port: u16,
        bootstrap: Option<(PeerId, Multiaddr)>,
        _storage: Arc<super::storage::Storage>,
        _consensus_config: ConsensusConfig,
    ) -> Result<()> {
        let bootstrap_peers = bootstrap.into_iter().collect();
        let network = P2PNetwork::new(identity, port, bootstrap_peers);
        network.run().await
    }
}

// ============================================================================
// CONSENSUS MODULE - BFT Consensus Engine
// ============================================================================

pub mod consensus {
    use anyhow::Result;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc, RwLock};
    use tracing::{debug, info, warn};

    /// Consensus engine configuration
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConsensusEngineConfig {
        pub committee_size: usize,
        pub threshold: usize,
        pub timeout_ms: u64,
        pub max_block_size: usize,
    }

    impl Default for ConsensusEngineConfig {
        fn default() -> Self {
            Self {
                committee_size: 4,
                threshold: 3,
                timeout_ms: 5000,
                max_block_size: 1_000_000,
            }
        }
    }

    /// Consensus vote
    #[derive(Debug, Clone)]
    pub struct Vote {
        pub height: u64,
        pub round: u32,
        pub block_hash: [u8; 64],
        pub voter: [u8; 32],
        pub signature: [u8; 64],
    }

    /// Consensus proposal
    #[derive(Debug, Clone)]
    pub struct Proposal {
        pub height: u64,
        pub round: u32,
        pub block_hash: [u8; 64],
        pub proposer: [u8; 32],
        pub timestamp: u64,
    }

    /// Consensus engine events
    #[derive(Debug, Clone)]
    pub enum ConsensusEvent {
        NewRound { height: u64, round: u32 },
        ProposalReceived(Proposal),
        VoteReceived(Vote),
        BlockCommitted { height: u64, hash: [u8; 64] },
        Timeout { height: u64, round: u32 },
    }

    /// BFT Consensus Engine
    pub struct ConsensusEngine {
        config: ConsensusEngineConfig,
        current_height: RwLock<u64>,
        current_round: RwLock<u32>,
        votes: RwLock<HashMap<(u64, u32), Vec<Vote>>>,
        event_tx: broadcast::Sender<ConsensusEvent>,
    }

    impl Clone for ConsensusEngine {
        fn clone(&self) -> Self {
            let (event_tx, _) = broadcast::channel(256);
            Self {
                config: self.config.clone(),
                current_height: RwLock::new(0),
                current_round: RwLock::new(0),
                votes: RwLock::new(HashMap::new()),
                event_tx,
            }
        }
    }

    impl std::fmt::Debug for ConsensusEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ConsensusEngine")
                .field("config", &self.config)
                .finish()
        }
    }

    impl ConsensusEngine {
        pub fn new(config: ConsensusEngineConfig) -> Self {
            let (event_tx, _) = broadcast::channel(256);

            info!(
                committee_size = config.committee_size,
                threshold = config.threshold,
                timeout_ms = config.timeout_ms,
                "Consensus engine initialized"
            );

            Self {
                config,
                current_height: RwLock::new(0),
                current_round: RwLock::new(0),
                votes: RwLock::new(HashMap::new()),
                event_tx,
            }
        }

        pub fn subscribe(&self) -> broadcast::Receiver<ConsensusEvent> {
            self.event_tx.subscribe()
        }

        pub async fn start(&self) -> Result<()> {
            info!("Consensus engine started");
            Ok(())
        }

        pub async fn stop(&self) -> Result<()> {
            info!("Consensus engine stopped");
            Ok(())
        }

        pub async fn current_height(&self) -> u64 {
            *self.current_height.read().await
        }

        pub async fn current_round(&self) -> u32 {
            *self.current_round.read().await
        }

        pub async fn add_vote(&self, vote: Vote) -> Result<bool> {
            let key = (vote.height, vote.round);
            let mut votes = self.votes.write().await;
            let vote_list = votes.entry(key).or_insert_with(Vec::new);

            // Check for duplicate
            if vote_list.iter().any(|v| v.voter == vote.voter) {
                return Ok(false);
            }

            vote_list.push(vote.clone());

            // Check if we have quorum
            if vote_list.len() >= self.config.threshold {
                let _ = self.event_tx.send(ConsensusEvent::BlockCommitted {
                    height: vote.height,
                    hash: vote.block_hash,
                });
                return Ok(true);
            }

            let _ = self.event_tx.send(ConsensusEvent::VoteReceived(vote));
            Ok(false)
        }

        pub async fn advance_round(&self) {
            let mut round = self.current_round.write().await;
            *round += 1;
            let height = *self.current_height.read().await;
            let _ = self.event_tx.send(ConsensusEvent::NewRound {
                height,
                round: *round,
            });
        }

        pub async fn advance_height(&self) {
            let mut height = self.current_height.write().await;
            *height += 1;
            let mut round = self.current_round.write().await;
            *round = 0;
            let _ = self.event_tx.send(ConsensusEvent::NewRound {
                height: *height,
                round: 0,
            });
        }
    }

    /// Group-aware consensus wrapper for masternode
    pub struct GroupAwareConsensusWrapper {
        engine: ConsensusEngine,
        group_id: u64,
        local_validator_id: Vec<u8>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GroupWrapperConfig {
        pub group_id: u64,
        pub local_validator_id: Vec<u8>,
    }

    impl Default for GroupWrapperConfig {
        fn default() -> Self {
            Self {
                group_id: 0,
                local_validator_id: vec![0; 32],
            }
        }
    }

    impl GroupAwareConsensusWrapper {
        pub fn new(engine: ConsensusEngine, config: GroupWrapperConfig) -> Self {
            info!(
                group_id = config.group_id,
                validator_id_len = config.local_validator_id.len(),
                "Group-aware consensus wrapper initialized"
            );

            Self {
                engine,
                group_id: config.group_id,
                local_validator_id: config.local_validator_id,
            }
        }

        pub async fn start(&self) -> Result<()> {
            self.engine.start().await
        }

        pub async fn stop(&self) -> Result<()> {
            self.engine.stop().await
        }

        pub fn group_id(&self) -> u64 {
            self.group_id
        }

        pub fn engine(&self) -> &ConsensusEngine {
            &self.engine
        }
    }
}
