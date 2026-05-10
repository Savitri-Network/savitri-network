#![allow(dead_code)]

use anyhow::anyhow;
use bincode;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use libp2p::kad::{store::MemoryStore, Behaviour as Kademlia};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{
    gossipsub::Behaviour as Gossipsub, identify::Behaviour as Identify, Multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};

use crate::p2p::aux_protocol::{AuxAck, AuxCodec, AuxMessage};
use crate::p2p::consensus_protocol::{
    ConsensusAck, ConsensusCodec, ConsensusMessage as DirectConsensusMessage,
};

// Helper module for serializing large arrays.
// Uses explicit serialize_tuple/deserialize_tuple to avoid format mismatch:
// serde's Serialize for [u8; N] (N>32) auto-derefs to &[u8] → serialize_bytes
// (with length prefix), but deserialize_tuple expects no prefix.
pub mod big_array {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S, const N: usize>(data: &[u8; N], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(N)?;
        for byte in data {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }

    pub fn deserialize<'de, const N: usize, D>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{SeqAccess, Visitor};
        struct ArrayVisitor<const M: usize>;
        impl<'de, const M: usize> Visitor<'de> for ArrayVisitor<M> {
            type Value = [u8; M];
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a byte array of length {}", M)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut arr = [0u8; M];
                for i in 0..M {
                    arr[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(arr)
            }
        }
        deserializer.deserialize_tuple(N, ArrayVisitor::<N>)
    }
}

// NetworkBehaviour completo con Gossipsub + Identify + Kademlia + Consensus (request-response)
#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "MyBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
pub struct MyBehaviour {
    pub gossipsub: Gossipsub,
    pub identify: Identify,
    pub kademlia: Kademlia<MemoryStore>,
    pub consensus: libp2p::request_response::Behaviour<ConsensusCodec>,
    pub aux: libp2p::request_response::Behaviour<AuxCodec>,
    pub tx_fetch: libp2p::request_response::Behaviour<crate::p2p::tx_fetch_protocol::TxFetchCodec>,
    pub relay_client: libp2p::relay::client::Behaviour,
    pub dcutr: libp2p::dcutr::Behaviour,
    pub autonat: libp2p::autonat::Behaviour,
    pub upnp: libp2p::upnp::tokio::Behaviour,
}

#[derive(Debug)]
pub enum MyBehaviourEvent {
    Gossipsub(libp2p::gossipsub::Event),
    Identify(libp2p::identify::Event),
    Kademlia(libp2p::kad::Event),
    Consensus(libp2p::request_response::Event<DirectConsensusMessage, ConsensusAck>),
    Aux(libp2p::request_response::Event<AuxMessage, AuxAck>),
    TxFetch(
        libp2p::request_response::Event<
            crate::p2p::tx_fetch_protocol::TxFetchRequest,
            crate::p2p::tx_fetch_protocol::TxFetchResponse,
        >,
    ),
    RelayClient(libp2p::relay::client::Event),
    Dcutr(libp2p::dcutr::Event),
    Autonat(libp2p::autonat::Event),
    Upnp(libp2p::upnp::Event),
}

impl From<libp2p::gossipsub::Event> for MyBehaviourEvent {
    fn from(event: libp2p::gossipsub::Event) -> Self {
        MyBehaviourEvent::Gossipsub(event)
    }
}

impl From<libp2p::identify::Event> for MyBehaviourEvent {
    fn from(event: libp2p::identify::Event) -> Self {
        MyBehaviourEvent::Identify(event)
    }
}

impl From<libp2p::kad::Event> for MyBehaviourEvent {
    fn from(event: libp2p::kad::Event) -> Self {
        MyBehaviourEvent::Kademlia(event)
    }
}

impl From<libp2p::request_response::Event<DirectConsensusMessage, ConsensusAck>>
    for MyBehaviourEvent
{
    fn from(event: libp2p::request_response::Event<DirectConsensusMessage, ConsensusAck>) -> Self {
        MyBehaviourEvent::Consensus(event)
    }
}

impl From<libp2p::request_response::Event<AuxMessage, AuxAck>> for MyBehaviourEvent {
    fn from(event: libp2p::request_response::Event<AuxMessage, AuxAck>) -> Self {
        MyBehaviourEvent::Aux(event)
    }
}

impl
    From<
        libp2p::request_response::Event<
            crate::p2p::tx_fetch_protocol::TxFetchRequest,
            crate::p2p::tx_fetch_protocol::TxFetchResponse,
        >,
    > for MyBehaviourEvent
{
    fn from(
        event: libp2p::request_response::Event<
            crate::p2p::tx_fetch_protocol::TxFetchRequest,
            crate::p2p::tx_fetch_protocol::TxFetchResponse,
        >,
    ) -> Self {
        MyBehaviourEvent::TxFetch(event)
    }
}

impl From<libp2p::relay::client::Event> for MyBehaviourEvent {
    fn from(event: libp2p::relay::client::Event) -> Self {
        MyBehaviourEvent::RelayClient(event)
    }
}

impl From<libp2p::dcutr::Event> for MyBehaviourEvent {
    fn from(event: libp2p::dcutr::Event) -> Self {
        MyBehaviourEvent::Dcutr(event)
    }
}

impl From<libp2p::autonat::Event> for MyBehaviourEvent {
    fn from(event: libp2p::autonat::Event) -> Self {
        MyBehaviourEvent::Autonat(event)
    }
}

impl From<libp2p::upnp::Event> for MyBehaviourEvent {
    fn from(event: libp2p::upnp::Event) -> Self {
        MyBehaviourEvent::Upnp(event)
    }
}

// Import real types from savitri modules
#[allow(unused_imports)]
pub use crate::tx::Block;
pub use crate::tx::SignedTx;

// P2P message types specific to lightnode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMessage {
    #[serde(with = "big_array")]
    pub hash: [u8; 64],
    pub header: BlockHeader,
    pub txs: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    pub exec_height: u64,
    pub proposer: [u8; 32], // Account address of the block proposer
    #[serde(default)]
    pub timestamp: u64, // Proposer timestamp propagated through wire format
    /// Parent block hash — propagated in wire format so receivers can set
    /// block.parent_hash correctly even without local chain context (i.e. during
    /// bootstrap/catch-up when storage doesn't yet have the previous block).
    /// Default [0;64] preserves backward compatibility with peers running older
    #[serde(default = "default_array_64", with = "big_array")]
    pub parent_hash: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hash64(#[serde(with = "big_array")] pub [u8; 64]);

impl Hash64 {
    pub fn new(bytes: [u8; 64]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaveBlock {
    #[serde(with = "big_array")]
    pub hash: [u8; 64],
    pub height: u64,
    pub exec_height: u64,
    pub tx_count: u32,
}
pub type HeartbeatMessage = crate::availability::HeartbeatMessage;
// SignedTx is now imported from savitri_mempool::core::tx

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub node_id: [u8; 32],
    pub validations_ok: u32,
    pub validations_total: u32,
    pub faults: u32,
    pub mismatches: u32,
    pub timeouts: u32,
    pub epoch_index: u64,
    pub timestamp: u64,
    #[serde(
        serialize_with = "big_array::serialize",
        deserialize_with = "big_array::deserialize"
    )]
    pub signature: [u8; 64],
}

impl IntegrityReport {
    pub fn new(
        epoch_index: u64,
        node_id: [u8; 32],
        validations_ok: u32,
        validations_total: u32,
        faults: u32,
        timeouts: u32,
        mismatches: u32,
        timestamp: u64,
        _signature: [u8; 64],
    ) -> Self {
        Self {
            node_id,
            validations_ok,
            validations_total,
            faults,
            mismatches,
            timeouts,
            epoch_index,
            timestamp,
            signature: _signature,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceClaim {
    pub node_id: [u8; 32],
    pub observed_bandwidth: u32,
    pub declared_bandwidth: u32,
    pub observed_cpu: u32,
    pub declared_cpu: u32,
    pub observed_storage: u32,
    pub declared_storage: u32,
    pub timestamp: u64,
    #[serde(
        serialize_with = "big_array::serialize",
        deserialize_with = "big_array::deserialize"
    )]
    pub signature: [u8; 64],
}

impl ResourceClaim {
    pub fn new(
        node_id: [u8; 32],
        _bw: u32,
        _cpu: u32,
        _storage: u32,
        timestamp: u64,
        signature: [u8; 64],
    ) -> Self {
        Self {
            node_id,
            observed_bandwidth: 0,
            declared_bandwidth: _bw,
            observed_cpu: 0,
            declared_cpu: _cpu,
            observed_storage: 0,
            declared_storage: _storage,
            timestamp,
            signature,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RttObservation {
    pub target_id: [u8; 32],
    pub observer_id: [u8; 32],
    pub rtt_median_us: u32,
    pub rtt_ns: u64,
    pub timestamp: u64,
    #[serde(
        serialize_with = "big_array::serialize",
        deserialize_with = "big_array::deserialize"
    )]
    pub signature: [u8; 64],
}

impl RttObservation {
    pub fn new(
        target_id: [u8; 32],
        observer_id: [u8; 32],
        rtt_median_us: u32,
        timestamp: u64,
        signature: [u8; 64],
    ) -> Self {
        Self {
            target_id,
            observer_id,
            rtt_median_us,
            rtt_ns: rtt_median_us as u64 * 1000, // Convert microseconds to nanoseconds
            timestamp,
            signature,
        }
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UptimeClaim {
    pub node_id: [u8; 32],
    pub bitmap: Vec<u8>,
    pub h_ok: u32,
    pub h_total: u32,
}

impl UptimeClaim {
    pub fn new(
        node_id: [u8; 32],
        bitmap: Vec<u8>,
        h_ok: u32,
        h_total: u32,
        _timestamp: u64,
        _signature: [u8; 64],
    ) -> Self {
        Self {
            node_id,
            bitmap,
            h_ok,
            h_total,
        }
    }
}
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tracing::debug;

use crate::p2p::pou::PouScore;

/// Maximum number of pending blocks to track before evicting oldest entries.
/// This prevents memory leaks from blocks that never receive quorum.
///
/// Memory estimation: ~1KB per entry (block hash + status + pending_data ref)
/// With MAX_PENDING_BLOCKS = 100_000: ~100MB max memory usage
pub const MAX_PENDING_BLOCKS: usize = 100_000;

/// Timeout in seconds after which a pending block entry is considered stale.
/// Stale entries are eligible for cleanup during the next eviction pass.
pub const PENDING_BLOCK_TIMEOUT_SECS: u64 = 120; // 2 minutes

pub struct NetworkHandle {
    pub task: JoinHandle<()>,
    pub tx_sender: mpsc::Sender<SignedTx>,
    pub peer_accounts: Arc<RwLock<Vec<[u8; 32]>>>,
    pub block_sender: mpsc::Sender<(BlockBroadcast, PendingBlockData)>,
    pub heartbeat_sender: mpsc::Sender<HeartbeatMessage>,
    pub pou_sender: mpsc::Sender<PouBroadcast>,
    pub local_peer: PeerId,
    pub pou_state: crate::p2p::pou::PouState,
}

#[derive(Debug, Clone)]
pub struct BlockBroadcast {
    pub have: HaveBlock,
    pub block: BlockMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouBroadcast {
    pub peer_id: String,
    pub epoch: u64,
    /// PoU score in fixed-point integer form (0..=1000).
    pub score: PouScore,
    pub index: u16,
    /// Timestamp for PoU broadcast (required for masternode compatibility)
    pub timestamp: u64,
    /// Signed availability claim for the epoch.
    /// Default (all-zero) is tolerated for forward/backward compatibility: receiving
    /// a PouBroadcast without this field yields `h_total=0` → uptime_ratio=0 → lowest
    /// PoU score, rather than causing serde to reject the whole message.
    #[serde(default)]
    pub uptime_claim: UptimeClaim,
    /// Signed latency observation (median RTT). Optional if no samples.
    pub latency_observation: Option<RttObservation>,
    /// Signed integrity report. Optional when no integrity events yet.
    pub integrity_report: Option<IntegrityReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockReceipt {
    pub block_hash: Hash64, // Hash64 already has serde support via big_array in its definition
    pub peer_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone)]
pub struct BootstrapPeer {
    pub peer_id: PeerId,
    pub addr: Multiaddr,
    pub account: Option<[u8; 32]>,
    pub priority: bool,
}

// Multiple bootstrap configuration for redundancy
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub primary_nodes: Vec<BootstrapPeer>,
    pub secondary_nodes: Vec<BootstrapPeer>,
    pub max_bootstrap_attempts: u32,
    pub bootstrap_timeout: Duration,
    pub parallel_bootstrap: bool,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            primary_nodes: Vec::new(),
            secondary_nodes: Vec::new(),
            max_bootstrap_attempts: 3,
            bootstrap_timeout: Duration::from_secs(30),
            parallel_bootstrap: true,
        }
    }
}

impl BootstrapConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_primary_node(&mut self, peer: BootstrapPeer) {
        self.primary_nodes.push(peer);
    }

    pub fn add_secondary_node(&mut self, peer: BootstrapPeer) {
        self.secondary_nodes.push(peer);
    }

    pub fn get_all_nodes(&self) -> Vec<&BootstrapPeer> {
        self.primary_nodes
            .iter()
            .chain(self.secondary_nodes.iter())
            .collect()
    }

    pub fn get_priority_nodes(&self) -> Vec<&BootstrapPeer> {
        self.primary_nodes.iter().filter(|p| p.priority).collect()
    }
}

// On-chain reputation verification system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationData {
    pub peer_id: PeerId,
    pub account: [u8; 32],
    pub reputation_score: u32, // 0-1000
    pub successful_blocks: u64,
    pub failed_blocks: u64,
    pub last_seen: u64,
    pub uptime_percentage: f32,
    pub response_time_avg: f32,
    pub consensus_participation: f32,
    pub penalties: Vec<ReputationPenalty>,
    pub rewards: Vec<ReputationReward>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationPenalty {
    pub penalty_type: PenaltyType,
    pub severity: u8, // 1-10
    pub timestamp: u64,
    pub reason: String,
    pub decay_time: u64, // When penalty expires
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReputationReward {
    pub reward_type: RewardType,
    pub amount: u32,
    pub timestamp: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PenaltyType {
    DoubleSign,
    Inactivity,
    InvalidBlock,
    LateResponse,
    NetworkAbuse,
    ConsensusViolation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RewardType {
    BlockProposal,
    ConsensusParticipation,
    FastResponse,
    HighUptime,
    NetworkContribuition,
}

#[derive(Debug, Clone)]
pub struct ReputationManager {
    pub reputations: HashMap<PeerId, ReputationData>,
    pub decay_rate: f32, // Reputation decay over time
    pub min_reputation: u32,
    pub max_reputation: u32,
    pub penalty_threshold: u32, // Below this, peer is penalized
}

impl Default for ReputationManager {
    fn default() -> Self {
        Self {
            reputations: HashMap::new(),
            decay_rate: 0.95, // 5% decay per period
            min_reputation: 0,
            max_reputation: 1000,
            penalty_threshold: 200,
        }
    }
}

impl ReputationManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_reputation(
        &mut self,
        peer_id: &PeerId,
        account: [u8; 32],
        update: ReputationUpdate,
    ) {
        let rep = self
            .reputations
            .entry(peer_id.clone())
            .or_insert_with(|| ReputationData {
                peer_id: peer_id.clone(),
                account,
                reputation_score: 500, // Start at neutral
                successful_blocks: 0,
                failed_blocks: 0,
                last_seen: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                uptime_percentage: 100.0,
                response_time_avg: 0.0,
                consensus_participation: 0.0,
                penalties: Vec::new(),
                rewards: Vec::new(),
            });

        match update {
            ReputationUpdate::SuccessfulBlock => {
                rep.successful_blocks += 1;
                rep.reputation_score = (rep.reputation_score + 10).min(self.max_reputation);
            }
            ReputationUpdate::FailedBlock => {
                rep.failed_blocks += 1;
                rep.reputation_score = rep.reputation_score.saturating_sub(20);
            }
            ReputationUpdate::Penalty(penalty) => {
                rep.penalties.push(penalty.clone());
                rep.reputation_score = rep
                    .reputation_score
                    .saturating_sub(penalty.severity as u32 * 5);
            }
            ReputationUpdate::Reward(reward) => {
                rep.rewards.push(reward.clone());
                rep.reputation_score =
                    (rep.reputation_score + reward.amount).min(self.max_reputation);
            }
            ReputationUpdate::UpdateUptime(uptime) => {
                rep.uptime_percentage = uptime;
                if uptime > 95.0 {
                    rep.reputation_score = (rep.reputation_score + 5).min(self.max_reputation);
                }
            }
            ReputationUpdate::UpdateResponseTime(time) => {
                rep.response_time_avg = time;
                if time < 1000.0 {
                    // < 1 second is good
                    rep.reputation_score = (rep.reputation_score + 3).min(self.max_reputation);
                }
            }
        }

        rep.last_seen = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn get_reputation(&self, peer_id: &PeerId) -> Option<&ReputationData> {
        self.reputations.get(peer_id)
    }

    pub fn is_peer_trusted(&self, peer_id: &PeerId) -> bool {
        self.reputations
            .get(peer_id)
            .map(|rep| rep.reputation_score >= self.penalty_threshold)
            .unwrap_or(false)
    }

    pub fn apply_decay(&mut self) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        for rep in self.reputations.values_mut() {
            // Apply time-based decay
            let hours_since_last_seen = (current_time.saturating_sub(rep.last_seen)) / 3600;
            if hours_since_last_seen > 24 {
                let decay_factor = self.decay_rate.powf(hours_since_last_seen as f32 / 24.0);
                rep.reputation_score = (rep.reputation_score as f32 * decay_factor) as u32;
            }

            // Remove expired penalties
            rep.penalties.retain(|p| p.decay_time > current_time);
        }
    }

    pub fn get_top_peers(&self, limit: usize) -> Vec<&ReputationData> {
        let mut peers: Vec<_> = self.reputations.values().collect();
        peers.sort_by(|a, b| b.reputation_score.cmp(&a.reputation_score));
        peers.into_iter().take(limit).collect()
    }
}

#[derive(Debug, Clone)]
pub enum ReputationUpdate {
    SuccessfulBlock,
    FailedBlock,
    Penalty(ReputationPenalty),
    Reward(ReputationReward),
    UpdateUptime(f32),
    UpdateResponseTime(f32),
}

#[derive(Debug)]
pub enum BlockPrepError {
    Validation(anyhow::Error),
    StateMismatch(BlockStateMismatch),
    MissingParent {
        parent_exec: [u8; 64],
        target_height: u64,
    },
}

impl fmt::Display for BlockPrepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockPrepError::Validation(err) => write!(f, "{err}"),
            BlockPrepError::StateMismatch(err) => write!(f, "{err}"),
            BlockPrepError::MissingParent { .. } => {
                write!(f, "orphan-exec: missing parent_exec block")
            }
        }
    }
}

impl From<anyhow::Error> for BlockPrepError {
    fn from(err: anyhow::Error) -> Self {
        BlockPrepError::Validation(err)
    }
}

#[derive(Debug)]
pub struct BlockStateMismatch {
    pub reason: String,
}

impl BlockStateMismatch {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for BlockStateMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

#[derive(Clone)]
pub struct PendingBlockData {
    pub block: crate::tx::Block,
    pub signed_txs: Vec<SignedTx>,
    pub source_peer: PeerId,
}

// ReceiptManager, ReceiptTracker, TimeoutHandler, TimeoutConfig, LocalBlockTracker,
// LocalBlockStatus, LocalBlockDecision, LocalReceiptResult — all removed.
// These were part of the deprecated quorum-based receipt system, replaced by
// certificate-based PoU-BFT finality (see certificate.rs).

// Connection pool for managing peer connections and retry logic
#[derive(Debug, Clone)]
pub struct ConnectionPool {
    peers: std::collections::HashSet<PeerId>,
    active_connections: std::collections::HashMap<PeerId, Multiaddr>,
    pending_connections: std::collections::HashMap<PeerId, (Multiaddr, std::time::Instant)>,
    handshake_cache: std::collections::HashMap<PeerId, HandshakeResult>,
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            peers: std::collections::HashSet::new(),
            active_connections: std::collections::HashMap::new(),
            pending_connections: std::collections::HashMap::new(),
            handshake_cache: std::collections::HashMap::new(),
        }
    }

    pub fn add_peer(&mut self, peer: &PeerId) {
        self.peers.insert(peer.clone());
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.peers.remove(peer);
        self.active_connections.remove(peer);
        self.pending_connections.remove(peer);
        self.handshake_cache.remove(peer);
    }

    pub fn get_peers(&self) -> Vec<PeerId> {
        self.peers.iter().cloned().collect()
    }

    pub async fn add_connection(&mut self, peer: PeerId, addr: Multiaddr) {
        self.peers.insert(peer.clone());
        self.active_connections.insert(peer.clone(), addr.clone());
        self.pending_connections.remove(&peer);
    }

    pub async fn get_connections_to_retry(&self) -> Vec<(PeerId, Multiaddr)> {
        let now = std::time::Instant::now();
        let retry_timeout = std::time::Duration::from_secs(30); // Retry after 30 seconds

        self.pending_connections
            .iter()
            .filter(|(_, (_, attempt_time))| now.duration_since(*attempt_time) > retry_timeout)
            .map(|(peer, (addr, _))| (peer.clone(), addr.clone()))
            .collect()
    }

    pub async fn mark_connection_active(&mut self, peer: &PeerId) {
        // Remove from pending if exists
        self.pending_connections.remove(peer);
    }

    pub async fn cache_handshake(&mut self, peer: PeerId, result: HandshakeResult) {
        self.handshake_cache.insert(peer, result);
    }

    pub async fn remove_connection(&mut self, peer: &PeerId, _reason: Option<&str>) {
        self.peers.remove(peer);
        self.active_connections.remove(peer);
        self.pending_connections.remove(peer);
        // Keep handshake cache for debugging purposes
    }

    /// Add a pending connection attempt
    pub fn add_pending_connection(&mut self, peer: PeerId, addr: Multiaddr) {
        self.pending_connections
            .insert(peer, (addr, std::time::Instant::now()));
    }

    /// Check if peer is actively connected
    pub fn is_connected(&self, peer: &PeerId) -> bool {
        self.active_connections.contains_key(peer)
    }

    /// Get connection address for peer
    pub fn get_connection_address(&self, peer: &PeerId) -> Option<&Multiaddr> {
        self.active_connections.get(peer)
    }

    /// Get all active connections
    pub fn get_active_connections(&self) -> Vec<&PeerId> {
        self.active_connections.keys().collect()
    }

    pub fn active_connection_count(&self) -> usize {
        self.active_connections.len()
    }

    /// Get handshake result for peer
    pub fn get_handshake_result(&self, peer: &PeerId) -> Option<&HandshakeResult> {
        self.handshake_cache.get(peer)
    }
}

#[derive(Debug, Clone)]
pub enum HandshakeResult {
    Success,
    Failed(String),
}

// Consensus protocol messages for BFT agreement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    /// Block proposal from current leader
    Proposal {
        block_hash: Vec<u8>, // Stored as Vec for serde compatibility
        height: u64,
        round: u32,
        proposer: [u8; 32],
        signature: Vec<u8>,
    },
    /// Vote from committee member
    Vote {
        block_hash: Vec<u8>,
        height: u64,
        round: u32,
        voter: [u8; 32],
        vote_type: VoteType,
        signature: Vec<u8>,
    },
    /// Final consensus certificate
    Certificate(ConsensusCertificate),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VoteType {
    PreVote,
    PreCommit,
}

fn default_array_64() -> [u8; 64] {
    [0u8; 64]
}

/// Wire format for masternode block certificate (JSON from MN on /savitri/consensus/cert/1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCertificateWire {
    pub round_id: u64,
    pub height: u64,
    #[serde(with = "big_array")]
    pub block_hash: [u8; 64],
    pub votes: Vec<MasternodeVoteWire>,
    pub timestamp: u64,
    #[serde(default)]
    pub group_id: String,
    /// State root from certified proposal (MN sends so LN can match re-execution)
    #[serde(default = "default_array_64", with = "big_array")]
    pub state_root: [u8; 64],
    /// Transaction root from certified proposal
    #[serde(default = "default_array_64", with = "big_array")]
    pub tx_root: [u8; 64],
    /// Parent block hash (so LN can match block hash formula)
    #[serde(default = "default_array_64", with = "big_array")]
    pub parent_hash: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasternodeVoteWire {
    pub round_id: u64,
    pub height: u64,
    #[serde(with = "big_array")]
    pub block_hash: [u8; 64],
    #[serde(with = "big_array")]
    pub voter_pubkey: [u8; 32],
    pub vote_type: MasternodeVoteTypeWire,
    #[serde(with = "big_array")]
    pub signature: [u8; 64],
    #[serde(default)]
    pub group_id: String,
    #[serde(default = "default_array_64", with = "big_array")]
    pub state_root: [u8; 64],
    #[serde(default = "default_array_64", with = "big_array")]
    pub tx_root: [u8; 64],
    #[serde(default = "default_array_64", with = "big_array")]
    pub parent_hash: [u8; 64],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MasternodeVoteTypeWire {
    Approve,
    Reject,
}

/// Single message block+certificate from MN (topic /savitri/block_final/1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockWithCertificateWire {
    pub block: BlockMessage,
    pub certificate: BlockCertificateWire,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusCertificate {
    /// Block hash being certified
    #[serde(with = "big_array")]
    pub block_hash: [u8; 64],
    /// Block height
    pub height: u64,
    /// Consensus epoch identifier
    pub epoch_id: u64,
    /// Committee identifier
    pub committee_id: u64,
    /// Consensus round
    pub round: u32,
    /// Voting committee members
    pub voters: Vec<[u8; 32]>,
    /// Aggregated BLS signature
    pub aggregated_signature: Vec<u8>,
    /// Certificate timestamp
    pub timestamp: u64,
    /// Group ID of the proposer's group. Carried through from BlockCertificateWire so the
    /// LN cert-only fallback path can update per-group certified height. Empty string on
    /// legacy peers / single-group deployments — `notify_block_certified_for_group` handles
    /// the empty case by updating only the global counter.
    #[serde(default)]
    pub group_id: String,
    /// so the cert handler can disambiguate same-height-same-group blocks
    /// (filled vs empty races) when falling back to the secondary index.
    /// Default [0;32] preserves backward compatibility — when the cert
    /// reports zero tx_root, the handler skips the precise-lookup branch.
    #[serde(default = "default_array_32", with = "big_array")]
    pub tx_root: [u8; 32],
}

fn default_array_32() -> [u8; 32] {
    [0u8; 32]
}

impl ConsensusCertificate {
    /// Create a new consensus certificate
    pub fn new(
        block_hash: [u8; 64],
        height: u64,
        epoch_id: u64,
        committee_id: u64,
        round: u32,
    ) -> Self {
        Self {
            block_hash,
            height,
            epoch_id,
            committee_id,
            round,
            voters: Vec::new(),
            aggregated_signature: Vec::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            group_id: String::new(),
            tx_root: [0u8; 32],
        }
    }

    /// Add a voter to the certificate
    pub fn add_voter(&mut self, voter: [u8; 32]) {
        if !self.voters.contains(&voter) {
            self.voters.push(voter);
        }
    }

    /// Check if certificate has sufficient votes (simple majority for now)
    pub fn has_quorum(&self, committee_size: usize) -> bool {
        self.voters.len() > committee_size / 2
    }

    /// Verify certificate structure validity
    pub fn is_valid(&self) -> bool {
        !self.voters.is_empty() && !self.aggregated_signature.is_empty() && self.height > 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Transaction(Vec<u8>),
    Block(BlockMessage),
    HaveTx(HaveTx),
    MonolithAnnounce(MonolithAnnounce),
    Tx(TxMessage),
    PeerInfo(PeerInfo),
    LightnodeRegistration(LightnodeRegistration),
    Heartbeat(HeartbeatMessage),
    HaveBlock(HaveBlock),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithAnnounce {
    pub header: MonolithHeader,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monolith_data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MonolithHeader {
    pub monolith_id: Vec<u8>,
    pub monolith_hash: Vec<u8>,
    pub monolith_size: u64,
    pub timestamp: u64,
    /// ZKP proof commitment for state transition verification.
    /// When present, lightnodes verify this proof to trust the monolith
    /// state snapshot without replaying all blocks in the window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zkp_proof: Option<Vec<u8>>,
    /// Merkle root of block headers covered by this monolith
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers_commit: Option<Vec<u8>>,
    /// State root after applying all blocks in the monolith window
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_commit: Option<Vec<u8>>,
    /// Last block height included in this monolith
    #[serde(default)]
    pub exec_height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaveTx {
    #[serde(with = "big_array")]
    pub hash: [u8; 32],
    pub tx_hashes: Vec<[u8; 32]>,
    /// Original publisher's PeerId bytes — so the proposer knows WHO has the TX data.
    /// Without this, gossipsub propagation_source (relay peer) gets used, which doesn't
    /// have the TX bytes in its TxStore.
    #[serde(default)]
    pub source_peer: Vec<u8>,
}

// Re-export HeartbeatKind from availability to avoid duplicates
pub use crate::availability::HeartbeatKind;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub address: String,
    #[serde(with = "big_array")]
    pub account: [u8; 32],
    #[serde(default)]
    pub priority: bool,
}

/// Lightnode registration message for group formation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeRegistration {
    pub node_id: String,
    pub peer_id: String,
    pub multiaddr: String,
    pub geographic_region: String,
    pub pou_score: f64,
    pub capabilities: Vec<String>,
    pub uptime_percentage: f64,
    #[serde(with = "big_array")]
    pub account: [u8; 32],
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MonolithReply {
    pub req_id: u64,
    pub header: Option<MonolithHeader>,
    pub header_leaf_hashes: Vec<Vec<u8>>,
    pub missing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxMessage {
    pub data: Vec<u8>,
    pub tx: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RequestMessage {
    Bootstrap(BootstrapRequest),
    Block(Vec<u8>),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ResponseMessage {
    Bootstrap(BootstrapReply),
    Block(Vec<u8>),
    MonolithReply(MonolithReply),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BootstrapRequest {
    pub version: u32,
    pub end_height: u64,
    pub max_blocks: u32,
}

impl BootstrapRequest {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BootstrapReply {
    pub peers: Vec<BootstrapPeerInfo>,
    pub accounts: Vec<BootstrapAccountInfo>,
    pub blocks: Vec<BootstrapBlockInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BootstrapPeerInfo {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub is_light_node: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BootstrapAccountInfo {
    pub address: Vec<u8>,
    pub balance: u128,
    pub nonce: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BootstrapBlockInfo {
    pub height: u64,
    pub hash: Vec<u8>,
    pub timestamp: u64,
    pub tx_count: u32,
}

impl BootstrapReply {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// Maximum allowed size for consensus message deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized network payloads.
const MAX_CONSENSUS_MSG_SIZE: usize = 4 * 1024 * 1024;

// Real decode functions
pub fn decode_consensus(data: &[u8]) -> Result<ConsensusMessage, anyhow::Error> {
    if data.len() > MAX_CONSENSUS_MSG_SIZE {
        anyhow::bail!(
            "Consensus message too large: {} bytes (max {})",
            data.len(),
            MAX_CONSENSUS_MSG_SIZE
        );
    }
    // Try to decode the full consensus message
    bincode::deserialize::<ConsensusMessage>(data)
        .map_err(|e| anyhow!("Failed to decode consensus message: {}", e))
}

/// Decode block certificate sent by masternode on /savitri/consensus/cert/1 (JSON).
/// Converts to ConsensusCertificate for the existing commit pipeline.
pub fn decode_consensus_cert_from_masternode(
    data: &[u8],
) -> Result<ConsensusCertificate, anyhow::Error> {
    let wire: BlockCertificateWire = serde_json::from_slice(data).map_err(|e| {
        anyhow!(
            "Failed to decode masternode block certificate (JSON): {}",
            e
        )
    })?;
    if wire.votes.is_empty() {
        anyhow::bail!("Masternode certificate has no votes");
    }
    let voters: Vec<[u8; 32]> = wire.votes.iter().map(|v| v.voter_pubkey).collect();
    let round = wire.round_id.min(u32::MAX as u64) as u32;
    let committee_id = if wire.group_id.is_empty() { 1 } else { 1 }; // non-zero for validation
    let epoch_id = 1u64;
    let aggregated_signature = wire.votes[0].signature.to_vec();
    // can disambiguate filled-vs-empty blocks at same (height, group_id).
    let mut tx_root_32 = [0u8; 32];
    tx_root_32.copy_from_slice(&wire.tx_root[..32]);
    Ok(ConsensusCertificate {
        block_hash: wire.block_hash,
        height: wire.height,
        epoch_id,
        committee_id,
        round,
        voters,
        aggregated_signature,
        timestamp: wire.timestamp,
        group_id: wire.group_id,
        tx_root: tx_root_32,
    })
}

pub fn decode_gossip(data: &[u8]) -> Result<GossipMessage, anyhow::Error> {
    serde_json::from_slice(data).map_err(|e| anyhow!("Failed to decode gossip message: {}", e))
}

pub fn decode_request(data: &[u8]) -> Result<RequestMessage, anyhow::Error> {
    serde_json::from_slice(data).map_err(|e| anyhow::anyhow!("decode_request: {}", e))
}

pub fn decode_response(data: &[u8]) -> Result<ResponseMessage, anyhow::Error> {
    serde_json::from_slice(data).map_err(|e| anyhow::anyhow!("decode_response: {}", e))
}

pub fn encode_request(msg: &RequestMessage) -> Result<Vec<u8>, anyhow::Error> {
    serde_json::to_vec(msg).map_err(|e| anyhow::anyhow!("encode_request: {}", e))
}

pub fn encode_response(msg: &ResponseMessage) -> Result<Vec<u8>, anyhow::Error> {
    serde_json::to_vec(msg).map_err(|e| anyhow::anyhow!("encode_response: {}", e))
}

// Receiver types for P2P subsystems
#[derive(Clone)]
pub struct BlockReceiver {
    pub tx: tokio::sync::mpsc::Sender<BlockBroadcast>,
}

impl BlockReceiver {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<BlockBroadcast>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        (Self { tx }, rx)
    }

    pub async fn send(
        &self,
        block: BlockBroadcast,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<BlockBroadcast>> {
        self.tx.send(block).await
    }
}

pub struct CertificateReceiver {
    pub tx: tokio::sync::mpsc::Sender<ConsensusCertificate>,
}

impl CertificateReceiver {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<ConsensusCertificate>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        (Self { tx }, rx)
    }

    pub async fn send(
        &self,
        cert: ConsensusCertificate,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ConsensusCertificate>> {
        self.tx.send(cert).await
    }
}

pub struct IntegrityReceiver {
    pub tx: tokio::sync::mpsc::Sender<crate::integrity::IntegrityEvent>,
}

impl IntegrityReceiver {
    pub fn new() -> (
        Self,
        tokio::sync::mpsc::Receiver<crate::integrity::IntegrityEvent>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        (Self { tx }, rx)
    }

    pub async fn send(
        &self,
        event: crate::integrity::IntegrityEvent,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<crate::integrity::IntegrityEvent>> {
        self.tx.send(event).await
    }
}

pub struct PouReceiver {
    pub tx: tokio::sync::mpsc::Sender<PouBroadcast>,
}

impl PouReceiver {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<PouBroadcast>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        (Self { tx }, rx)
    }

    pub async fn send(
        &self,
        pou: PouBroadcast,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<PouBroadcast>> {
        self.tx.send(pou).await
    }
}
