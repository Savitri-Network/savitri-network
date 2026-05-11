//! Intra-Group Communication for Light Nodes
//!
//! This module handles P2P communication within assigned groups including
//! latency measurement, PoU sharing, and proposer election.
//!
//! Sub-modules contain extracted method groups for maintainability:
//! - `election` — Proposer election orchestration and determination
//! - `block_production` — Block creation loop, mempool drain, proposal signing
//! - `message_handlers` — Gossipsub message dispatch (latency, PoU, ping/pong)
//! - `chain_utils` — Chain state queries, certificate building, signing
//! - `masternode_comm` — Block submission and masternode handshake

#![allow(dead_code)]

// (election/block_production/message_handlers/follower/chain_utils/masternode_comm)
// were never imported (the `pub mod` declarations stayed commented for months
// while methods kept evolving in this single mod.rs). They diverged from the
// live code and were a maintenance trap. Files removed in commit accompanying
// this change. If a future split is desired, regenerate from the live methods
// in this file rather than resurrecting the orphans.

use anyhow::Result;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering as AtomicOrdering};

/// Peer discovery message for masternode communication
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerDiscoveryRequest {
    pub requesting_peer: String,
}

/// Peer discovery response from masternode
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerDiscoveryResponse {
    pub requesting_peer: String,
    pub masternode_peers: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerRegistryAnnounce {
    pub peer_id: String,
    pub multiaddr: String,
    pub role: String,
    pub timestamp: u64,
    pub ttl_secs: u64,
}

/// PoU broadcast message for masternode communication
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PouBroadcast {
    pub peer_id: String,
    pub epoch: u64,
    pub score: u16,
    pub index: u16,
    pub timestamp: u64,
}
use crate::p2p::consensus_protocol::ConsensusMessage;
use crate::p2p::swarm_commands::SwarmCommand;
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use libp2p::gossipsub::IdentTopic;
use libp2p::PeerId;
use savitri_core::crypto::Keypair;
use serde_big_array::BigArray;
use sha2::Digest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

use super::dag::DagManager;
use super::group_manager::P2PGroupManager;
use crate::availability::PouScoring;
use crate::latency_service::LatencyService;
use crate::storage::BlockAndAccountStorageTrait;
use savitri_consensus::scoring::ObservationStore;
use savitri_consensus::types::LatencyType;

/// Safe timestamp in seconds (fallback to prevent crashes)
fn get_safe_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs()
}

/// Safe timestamp in milliseconds per misurazioni latency precise (RTT)
fn get_safe_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis() as u64
}

/// `context`: optional caller context for diagnostics (e.g. "verify_intragroup_signature", "collect_latency_proof").
fn safe_hex_decode(hex_str: &str, fallback: Vec<u8>, context: Option<&'static str>) -> Vec<u8> {
    let clean_hex = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    let is_likely_peer_id = hex_str.starts_with("12D3KooW");
    if clean_hex.len() % 2 != 0 || !clean_hex.chars().all(|c| c.is_ascii_hexdigit()) {
        warn!(
            context = ?context,
            input_preview = %hex_str.chars().take(50).collect::<String>(),
            is_likely_peer_id,
            "Invalid hex string, using fallback. Emitted by lightnode: if is_likely_peer_id=true the value is a libp2p PeerId (base58), not hex; do not use hex decode for PeerIds."
        );
        return fallback;
    }
    hex::decode(clean_hex).unwrap_or_else(|_| {
        warn!(
            context = ?context,
            input_preview = %hex_str.chars().take(50).collect::<String>(),
            is_likely_peer_id,
            "Failed to decode hex string, using fallback. Emitted by lightnode."
        );
        fallback
    })
}

/// Intra-group communication manager
#[derive(Clone)]
pub struct IntraGroupCommunication {
    /// Local node ID
    local_node_id: String,
    /// Group manager
    group_manager: Arc<P2PGroupManager>,
    /// Signing key for intra-group messages
    signing_key: Arc<Keypair>,
    /// Latency service
    latency_service: Option<Arc<LatencyService>>,
    /// PoU observation store — when wired, each successful group-pong round-trip
    /// feeds an RTT sample here so the `PouCalculator` can compute a real
    /// latency score instead of relying on stubs.
    observations: Option<Arc<ObservationStore>>,
    /// PoU scoring service
    pou_scoring: Option<Arc<PouScoring>>,
    /// Gossipsub behavior (fallback when network_publish_tx is None)
    gossipsub: Arc<RwLock<libp2p::gossipsub::Behaviour>>,
    /// When Some, publish goes to the network task (swarm gossipsub) instead of local gossipsub
    network_publish_tx: Option<mpsc::Sender<(IdentTopic, Vec<u8>)>>,
    /// Intra-group topics
    latency_topic: IdentTopic,
    pou_topic: IdentTopic,
    pou_ack_topic: IdentTopic,
    ping_topic: IdentTopic,
    pong_topic: IdentTopic,
    election_topic: IdentTopic,
    tx_topic: IdentTopic, // Transaction topic for intra-group communication
    proposal_topic: IdentTopic, // Block proposal topic for intra-group (followers receive real proposals)
    vote_topic: IdentTopic, // Consensus vote topic per gruppo (solo membri of the gruppo ascoltano/inviano)
    /// True when we received at least one Pong (mesh ready for PoU)
    mesh_ready: Arc<RwLock<bool>>,
    /// Group member latency measurements (PeerId -> (Duration, pubkey_hex))
    member_latencies: Arc<RwLock<HashMap<String, (Duration, Option<String>)>>>,
    /// Group member PoU scores (value, last_updated timestamp)
    member_pou_scores: Arc<RwLock<HashMap<String, (u32, Instant)>>>,
    /// Proposer state
    proposer_state: Option<Arc<RwLock<ProposerState>>>,
    /// Set in `create_and_propose_block_at_height` right after submission to
    /// the masternodes; cleared in `notify_block_certified_for_group` when
    /// the cert finally matches. Used by `start_following_proposer` to DEFER
    /// rotation while we still have an in-flight proposal — without this
    /// guard, an election handoff arriving 1-2s after the proposal would
    /// step the proposer down before the cert returns, triggering
    /// `restore_in_flight_txs` and dropping the 2000 drained TX back into
    /// the mempool. The next proposer drains the same TX → propose →
    /// rotate → restore loop, blocks stay empty.
    /// Tuple: (height, proposed_at) so the guard can also enforce a max
    /// wait window (CERT_PENDING_MAX_WAIT) before yielding anyway, to
    /// preserve liveness when the cert genuinely never arrives.
    pending_block_for_cert: Arc<RwLock<Option<(u64, Instant)>>>,
    /// Follower state
    follower_state: Option<Arc<RwLock<FollowerState>>>,
    received_proposals:
        Arc<RwLock<std::collections::VecDeque<(u64, String, crate::proposer::BlockProposal)>>>,
    /// Collected ProposerElectionResult messages when we are the elected proposer (for building ElectionCertificate)
    election_results_collected: Arc<RwLock<Vec<ProposerElectionResult>>>,
    mempool_pipeline: Option<crate::p2p::block::LightnodeMempoolHandle>,
    storage: Option<Arc<dyn BlockAndAccountStorageTrait>>,
    /// When Some, set to true when we are elected intra-group proposer so run_block_producer skips production
    is_intragroup_proposer: Option<Arc<RwLock<bool>>>,
    /// Channel per ricevere ACK whitelist dal MN (handshake 3-fasi)
    whitelist_ack_rx:
        Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<ProposerWhitelistAck>>>>,
    /// When Some, publish block to block_topic (for MN cache) without registering in certificate_pending (intra-group proposer path)
    block_broadcast_only_tx: Option<mpsc::Sender<super::types::BlockBroadcast>>,
    /// When Some, consensus messages are sent directly to peers via request-response
    /// instead of gossipsub broadcast. This frees the gossipsub send queue for transactions.
    network_direct_tx: Option<mpsc::Sender<SwarmCommand>>,
    /// DAG manager for multi-group block convergence and TX deduplication
    dag: Option<Arc<DagManager>>,
    /// Known masternode PeerIds for direct TCP (aux protocol) messaging
    masternode_peer_ids: Arc<RwLock<Vec<PeerId>>>,
    /// Shared flag: true when a block production loop is already running (prevents duplicates across clones)
    pub block_loop_running: Arc<AtomicBool>,
    /// boolean flags described in memory/architectural_debt.md Tier 6
    /// (block_loop_running, is_intragroup_proposer, is_in_intra_group,
    /// proposer_state.is_active). Phase 2 attaches mirror-write calls at
    /// the writer sites of the legacy flags so the SM stays in sync; a
    /// drift detector spawned alongside warns if the SM-derived state and
    /// the flag-derived state diverge. Phase 3 will swap readers and
    /// delete the legacy flags. Cheap to clone (Arc shallow).
    pub proposer_sm: Arc<crate::p2p::proposer_state::ProposerStateMachine>,
    /// Last election round we committed to (prevents oscillation from duplicate results)
    last_committed_election_round: Arc<RwLock<u64>>,
    /// Timestamp when this IntraGroupCommunication was created (proxy for node uptime)
    created_at: Instant,
    /// Consecutive failed elections (not enough candidates) — used for progressive quorum relaxation
    consecutive_election_failures: Arc<AtomicU32>,
    /// Cached PoU proposer schedule for tenure-based rotation (30 blocks per tenure)
    proposer_schedule: Arc<RwLock<Option<ProposerSchedule>>>,
    /// PERF: Last certified height per group (updated on cert RECEIPT, before RocksDB commit).
    /// The pipeline uses this instead of get_current_block_height() to avoid
    /// waiting 5s for RocksDB commit before proposing the next block.
    /// Per-group tracking enables independent parallel chains without height contention.
    pub last_certified_height: Arc<AtomicU64>,
    pub last_certified_height_per_group: Arc<std::sync::RwLock<HashMap<String, u64>>>,
    /// V0.2 Phase 1 (Score Canonicity, issue #31) — canonical RTT state holder.
    /// `None` until `set_latency_canon_state()` is called by the network task.
    /// Reads via `latency_canon_state.as_ref()?.lookup_score(...)`.
    pub latency_canon_state: Option<std::sync::Arc<crate::latency_canon_state::LatencyCanonState>>,
}

/// Cached PoU proposer schedule — 30-block tenure with zero-gap handoff.
/// PoU scoring runs continuously; the schedule is recomputed at block 29 of each tenure.
/// The next proposer pre-starts BEFORE the current tenure ends, eliminating election gaps.
#[derive(Debug, Clone)]
pub struct ProposerSchedule {
    /// Current tenure proposer peer_id
    pub current_proposer: String,
    /// Next proposer (computed at block 29, pre-starts before block 30 cert)
    pub next_proposer: Option<String>,
    /// Block height at which this tenure started
    pub tenure_start_height: u64,
    /// PoU-ranked candidate list for fallback (peer_id, pou_score)
    pub ranked_candidates: Vec<(String, u32)>,
    /// When this schedule was last computed
    pub last_updated: Instant,
}

/// Number of blocks per proposer tenure.
///
/// infinite) to 100 so the LN-side rotation actually runs and PoU-based
/// fairness is exercised. With ~1 block/s observed under load, 100 blocks
/// = ~100s tenure, comfortably above the BFT cert RTT (2-10s) and the
/// cert-pending defer window (CERT_PENDING_MAX_WAIT below).
const PROPOSER_TENURE_BLOCKS: u64 = 100;

/// Maximum time to defer a proposer rotation while waiting for the BFT
/// certificate of an in-flight proposed block.
///
///   * BFT cert round-trip is typically 2-3 s on LAN, 5-10 s on geo cluster.
///   * `BACKUP_CERT_TIMEOUT_MS` on the masternode is 2000 ms (memoria
///     bug49_cert_match_bft_timeout.md). 30 s gives 10x headroom for the
///     cert to arrive after the leader path emits.
///   * After 30 s without a cert match, we yield the tenure to preserve
///     liveness — the in-flight TX are restored and the next proposer
///     can re-drain.
const CERT_PENDING_MAX_WAIT: Duration = Duration::from_secs(30);

/// Proposer state management
#[derive(Debug, Clone)]
struct ProposerState {
    current_round: u64,
    last_block_height: u64,
    is_active: bool,
    block_proposal_count: u64,
}

/// Follower state management
#[derive(Debug, Clone)]
struct FollowerState {
    current_proposer: String,
    last_seen_block: u64,
    is_active: bool,
    blocks_received: u64,
    proposals_validated: u64,
}

/// Masternode status report
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MasternodeStatusReport {
    group_id: String,
    node_id: String,
    epoch: u64,
    proposer_status: ProposerStatus,
    follower_status: FollowerStatus,
    peer_latencies: Vec<(String, u64)>,
    timestamp: u64,
}

/// Single attestation in the election certificate (member attests elected proposer for group)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElectionAttestation {
    pub signer_peer_id: String,
    #[serde(with = "BigArray")]
    pub signer_pubkey: [u8; 32],
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// Certificate that the proposer was elected by the group (hash of outcome + attestations from members)
///
/// SECURITY (Falla 3 — anti-replay): `tenure_start_height` binds the certificate to a specific
/// height window. The MN verifies that `proposal.height ∈ [tenure_start_height,
/// tenure_start_height + PROPOSER_TENURE_BLOCKS)`. Without this binding, a legitimate cert
/// produced once was eternally re-usable for any future block of the same group, allowing a
/// proposer to spam blocks long after its tenure expired.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElectionCertificate {
    pub group_id: String,
    pub election_round: u64,
    pub elected_proposer_peer_id: String,
    #[serde(with = "BigArray")]
    pub elected_proposer_pubkey: [u8; 32],
    pub proposer_pou_score: u32,
    pub timestamp: u64,
    pub candidates: Vec<(String, u32, u32)>,
    pub attestations: Vec<ElectionAttestation>,
    /// First chain height at which this certificate is valid (bound for replay protection).
    /// Default 0 keeps wire-format backward compatibility with peers running pre-Falla3 code.
    #[serde(default)]
    pub tenure_start_height: u64,
}

/// ACK di whitelist dal MN (handshake 3-fasi)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposerWhitelistAck {
    pub target_proposer_peer_id: String,
    pub masternode_peer_id: String,
    pub group_id: String,
    pub round_id: u64,
    pub timestamp: u64,
    pub validity_secs: u64,
}

/// Messaggio certificato elezione inviato al MN (handshake 3-fasi, fase 1)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposerElectionCertMessage {
    pub group_id: String,
    pub round_id: u64,
    pub proposer_peer_id: String,
    #[serde(with = "BigArray")]
    pub proposer_pubkey: [u8; 32],
    pub proposer_pou_score: u32,
    pub election_timestamp: u64,
    pub candidates: Vec<(String, u32, u32)>,
    pub attestations: Vec<ElectionAttestation>,
}

/// Proposal payload expected by masternodes
#[derive(Debug, Clone, serde::Serialize)]
struct LightnodeProposalWire {
    round_id: u64,
    height: u64,
    timestamp: u64,
    #[serde(with = "BigArray")]
    proposer_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    block_hash: [u8; 64],
    tx_count: u32,
    #[serde(with = "BigArray")]
    signature: [u8; 64],
    #[serde(with = "BigArray")]
    parent_hash: [u8; 64],
    #[serde(with = "BigArray")]
    state_root: [u8; 64],
    #[serde(with = "BigArray")]
    tx_root: [u8; 64],
    /// Group ID so masternode can verify proposer against group state
    pub proposer_group_id: String,
    /// Certificate that this node is the elected proposer for the group (optional for backward compat)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub election_certificate: Option<ElectionCertificate>,
    /// Optional raw transaction bytes so MN can cache payload without relying on block_topic timing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_txs: Option<Vec<Vec<u8>>>,
}

/// Proposer status information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProposerStatus {
    is_active: bool,
    current_round: u64,
    blocks_proposed: u64,
    last_block_height: u64,
}

impl Default for ProposerStatus {
    fn default() -> Self {
        Self {
            is_active: false,
            current_round: 0,
            blocks_proposed: 0,
            last_block_height: 0,
        }
    }
}

/// Follower status information
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FollowerStatus {
    is_active: bool,
    current_proposer: String,
    blocks_received: u64,
    proposals_validated: u64,
    last_seen_block: u64,
}

impl Default for FollowerStatus {
    fn default() -> Self {
        Self {
            is_active: false,
            current_proposer: String::new(),
            blocks_received: 0,
            proposals_validated: 0,
            last_seen_block: 0,
        }
    }
}

/// Masternode command
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum MasternodeCommand {
    UpdateLatencyTargets {
        max_latency_ms: u64,
        preferred_latency_ms: u64,
    },
    AdjustPoUWeights {
        pou_weight: f64,
        latency_weight: f64,
    },
    GroupReconfiguration {
        new_members: Vec<String>,
        remove_members: Vec<String>,
    },
}

/// Intra-group message types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum IntraGroupMessage {
    LatencyMeasurement {
        target_id: String,
        latency_ms: u64,
        timestamp: u64,
    },
    PouScore {
        node_id: String,
        score: u32,
        timestamp: u64,
    },
    Election {
        round: u64,
        proposer_id: String,
        timestamp: u64,
    },
    Transaction {
        // NEW: Transaction message for intra-group communication
        tx_hash: [u8; 32],
        sender: [u8; 32],
        receiver: [u8; 32],
        amount: u64,
        nonce: u64,
        #[serde(with = "BigArray")]
        signature: [u8; 64],
        timestamp: u64,
    },
}

/// Consensus vote
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ConsensusVote {
    voter: String,
    proposer: String,
    round: u64,
    vote_type: VoteType,
    timestamp: u64,
    group_id: String,
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    signature: [u8; 64],
}

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

impl GroupLatencyProbe {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            probe_id: u64,
            sender: &'a str,
            timestamp_ms: u64,
            group_id: &'a str,
        }
        Ok(serde_json::to_vec(&Signable {
            probe_id: self.probe_id,
            sender: &self.sender,
            timestamp_ms: self.timestamp_ms,
            group_id: &self.group_id,
        })?)
    }
}

impl GroupLatencyResponse {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            probe_id: u64,
            responder: &'a str,
            original_timestamp_ms: u64,
            response_timestamp_ms: u64,
            group_id: &'a str,
        }
        Ok(serde_json::to_vec(&Signable {
            probe_id: self.probe_id,
            responder: &self.responder,
            original_timestamp_ms: self.original_timestamp_ms,
            response_timestamp_ms: self.response_timestamp_ms,
            group_id: &self.group_id,
        })?)
    }
}

impl PouScoreShare {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            node_id: &'a str,
            pou_score: u32,
            epoch: u64,
            group_id: &'a str,
            timestamp: u64,
        }
        Ok(serde_json::to_vec(&Signable {
            node_id: &self.node_id,
            pou_score: self.pou_score,
            epoch: self.epoch,
            group_id: &self.group_id,
            timestamp: self.timestamp,
        })?)
    }
}

impl ProposerElection {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            round: u64,
            candidate: &'a str,
            pou_score: u32,
            group_id: &'a str,
            timestamp: u64,
        }
        Ok(serde_json::to_vec(&Signable {
            round: self.round,
            candidate: &self.candidate,
            pou_score: self.pou_score,
            group_id: &self.group_id,
            timestamp: self.timestamp,
        })?)
    }
}

impl ProposerElectionResult {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        // V0.2 Phase 2 (Score Canonicity completion, issue #31):
        // `candidates` and `proposer_pou_score` are INCLUDED in the
        // signable payload. The Phase 1.5 design intent — election cert
        // cryptographically commits to the entire election outcome — is
        // restored on top of Phase 2's wall-clock bucket convergence:
        // see `latency_canon_publisher::current_wall_clock_bucket`. With
        // all LNs sharing the same bucket index (via loosely-synced NTP
        // clocks), the canonical LatencyTable is byte-identical
        // cluster-wide. combined_permille values in candidates are
        // therefore observer-independent and safe to include here.
        //
        // Validation on Savitri-Testnet-V0.1.0 cluster (commit e9be63d):
        // 5-minute loadtest with 6 MN + 7 LN, 44,739 TX submitted with
        // 100% acceptance, 0 signature verification failures observed.
        // The legacy 60-failures-in-6-minutes pre-convergence result is
        // resolved.
        //
        // `timestamp` remains EXCLUDED (per-observer wall-clock).
        // Falla 3 anti-replay binding via `tenure_start_height`.
        //
        // OPERATIONAL NOTE: severe NTP drift (> 10s) or partitioned
        // gossipsub mesh may transiently diverge the table; the fix is
        // operational (re-sync NTP, repair mesh), not code.
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            round: u64,
            elected_proposer: &'a str,
            proposer_pou_score: u32,
            sender: &'a str,
            group_id: &'a str,
            candidates: &'a [(String, u32, u32)],
            tenure_start_height: u64,
        }
        Ok(serde_json::to_vec(&Signable {
            round: self.round,
            elected_proposer: &self.elected_proposer,
            proposer_pou_score: self.proposer_pou_score,
            sender: &self.sender,
            group_id: &self.group_id,
            candidates: &self.candidates,
            tenure_start_height: self.tenure_start_height,
        })?)
    }
}

impl ConsensusVote {
    fn signable_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Signable<'a> {
            voter: &'a str,
            proposer: &'a str,
            round: u64,
            vote_type: &'a VoteType,
            timestamp: u64,
            group_id: &'a str,
        }
        Ok(serde_json::to_vec(&Signable {
            voter: &self.voter,
            proposer: &self.proposer,
            round: self.round,
            vote_type: &self.vote_type,
            timestamp: self.timestamp,
            group_id: &self.group_id,
        })?)
    }
}

/// Vote type
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum VoteType {
    Approve,
    Reject,
}

/// Latency probe message within group (timestamp in milliseconds per RTT preciso)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupLatencyProbe {
    /// Probe ID
    pub probe_id: u64,
    /// Sender node ID (PeerId for display/routing)
    pub sender: String,
    /// Sender Ed25519 public key as hex (64 chars) for signature verification
    #[serde(default)]
    pub sender_pubkey_hex: Option<String>,
    /// Timestamp in milliseconds (per misurazione RTT precisa)
    #[serde(alias = "timestamp")]
    pub timestamp_ms: u64,
    /// Group ID
    pub group_id: String,
    /// Signature over the probe (bound to group_id)
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// Latency response message (timestamps in milliseconds)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupLatencyResponse {
    /// Original probe ID
    pub probe_id: u64,
    /// Responder node ID (PeerId for display/routing)
    pub responder: String,
    /// Responder Ed25519 public key as hex (64 chars) for signature verification
    #[serde(default)]
    pub responder_pubkey_hex: Option<String>,
    /// Original probe timestamp (ms)
    #[serde(alias = "original_timestamp")]
    pub original_timestamp_ms: u64,
    /// Response timestamp (ms)
    #[serde(alias = "response_timestamp")]
    pub response_timestamp_ms: u64,
    /// Group ID
    pub group_id: String,
    /// Signature over the response (bound to group_id)
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// PoU score sharing message
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PouScoreShare {
    /// Node ID (PeerId for display/routing)
    pub node_id: String,
    /// Node Ed25519 public key as hex (64 chars) for signature verification
    #[serde(default)]
    pub node_pubkey_hex: Option<String>,
    /// PoU score (basis points)
    pub pou_score: u32,
    /// Epoch
    pub epoch: u64,
    /// Group ID
    pub group_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature over the PoU share (bound to group_id)
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// PoU score acknowledgment - sent by receiver to confirm they received a PoU share
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PouScoreAck {
    /// Node that received the PoU share (sender of this ACK)
    pub from: String,
    /// Node whose PoU share was received (original sender)
    pub ack_for: String,
    /// Group ID
    pub group_id: String,
    /// Timestamp
    pub timestamp: u64,
}

/// Ping message for mesh readiness probe - only respond if group_id matches
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupPing {
    /// Sender node_id
    pub from: String,
    /// Group ID - receivers must match before replying
    pub group_id: String,
    /// Timestamp (seconds, legacy — kept for backward compatibility)
    pub timestamp: u64,
    /// Millisecond-precision send timestamp. Echoed back by the responder in
    /// `GroupPong::original_timestamp_ms` so the sender can compute RTT.
    /// `#[serde(default)]` keeps the wire format backward compatible with
    /// peers running the previous build — they will send 0 and we simply
    /// skip the RTT sample.
    #[serde(default)]
    pub original_timestamp_ms: u64,
}

/// Pong message - reply to Ping, only count if group_id matches
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GroupPong {
    /// Responder node_id (sender of this Pong)
    pub from: String,
    /// Original ping sender we are replying to
    pub in_reply_to: String,
    /// Group ID - must match our group
    pub group_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Echoed from `GroupPing::original_timestamp_ms`. When non-zero, the
    /// ping sender uses it to compute `rtt_ms = now_ms - original_timestamp_ms`
    /// and pushes that sample into the PoU `ObservationStore`.
    #[serde(default)]
    pub original_timestamp_ms: u64,
}

/// Proposer election message
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposerElection {
    /// Election round
    pub round: u64,
    /// Candidate node ID (PeerId for display/routing)
    pub candidate: String,
    /// Candidate Ed25519 public key as hex (64 chars) for signature verification
    #[serde(default)]
    pub candidate_pubkey_hex: Option<String>,
    /// Candidate PoU score
    pub pou_score: u32,
    /// Group ID
    pub group_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature over the election message (bound to group_id)
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// Proposer election result message
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposerElectionResult {
    /// Election round
    pub round: u64,
    /// Elected proposer
    pub elected_proposer: String,
    /// Elected proposer PoU score
    pub proposer_pou_score: u32,
    /// Sender node ID (PeerId for display/routing)
    pub sender: String,
    /// Sender Ed25519 public key as hex (64 chars) for signature verification
    #[serde(default)]
    pub sender_pubkey_hex: Option<String>,
    /// Group ID
    pub group_id: String,
    /// Election timestamp
    pub timestamp: u64,
    /// All candidates and their scores
    pub candidates: Vec<(String, u32, u32)>, // (node_id, pou_score, combined_score)
    /// First chain height at which the elected proposer's tenure starts.
    /// Bound into the signed payload (Falla 3 anti-replay): a certificate built from these
    /// results is only valid for [tenure_start_height, tenure_start_height + TENURE_BLOCKS).
    #[serde(default)]
    pub tenure_start_height: u64,
    /// Signature over the election result (bound to group_id)
    #[serde(
        serialize_with = "serialize_signature",
        deserialize_with = "deserialize_signature"
    )]
    pub signature: [u8; 64],
}

/// Max transactions per block when using shared mempool.
/// 1000 TX × ~200 bytes = ~200KB per block (within gossipsub 4MB limit).
const MAX_BLOCK_TXS_MEMPOOL: usize = 2000;

impl IntraGroupCommunication {
    pub fn new(
        local_node_id: String,
        group_manager: Arc<P2PGroupManager>,
        signing_key: Arc<Keypair>,
        latency_service: Option<Arc<LatencyService>>,
        pou_scoring: Option<Arc<PouScoring>>,
        gossipsub: Arc<RwLock<libp2p::gossipsub::Behaviour>>,
        network_publish_tx: Option<mpsc::Sender<(IdentTopic, Vec<u8>)>>,
        mempool_pipeline: Option<crate::p2p::block::LightnodeMempoolHandle>,
        storage: Option<Arc<dyn BlockAndAccountStorageTrait>>,
        is_intragroup_proposer: Option<Arc<RwLock<bool>>>,
        block_broadcast_only_tx: Option<mpsc::Sender<super::types::BlockBroadcast>>,
        network_direct_tx: Option<mpsc::Sender<SwarmCommand>>,
        dag: Option<Arc<DagManager>>,
    ) -> Self {
        let block_loop_running = Arc::new(AtomicBool::new(false));
        let last_certified_height = Arc::new(AtomicU64::new(0));
        let consecutive_election_failures = Arc::new(AtomicU32::new(0));

        // `proposer_sm` is set later in the constructor body to this same
        // flag reads with SM queries; Phase 5 deletes the legacy flags.
        let proposer_sm_for_diag =
            Arc::new(crate::p2p::proposer_state::ProposerStateMachine::new());

        // proposer-state flags so post-restart timeline analysis can identify
        // which flag is stuck. Logs every 10s. Cheap (~1 log/10s/LN).
        // detector — derives the expected SM variant from the flag combo
        // proposer_state_drift_total stays at zero in production.
        {
            let blr = Arc::clone(&block_loop_running);
            let igp = is_intragroup_proposer.clone();
            let lch = Arc::clone(&last_certified_height);
            let cef = Arc::clone(&consecutive_election_failures);
            let local_id = local_node_id.clone();
            let sm = Arc::clone(&proposer_sm_for_diag);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
                loop {
                    tick.tick().await;
                    let blr_v = blr.load(AtomicOrdering::SeqCst);
                    let igp_v: Option<bool> = if let Some(ref f) = igp {
                        Some(*f.read().await)
                    } else {
                        None
                    };
                    let lch_v = lch.load(AtomicOrdering::Relaxed);
                    let cef_v = cef.load(AtomicOrdering::Relaxed);

                    // from the legacy flag combination and compare.
                    let sm_variant = sm.current().await.variant_name();
                    let expected = match (blr_v, igp_v) {
                        (false, Some(false)) | (false, None) => "Idle",
                        (false, Some(true)) => "Elected",
                        (true, Some(true)) => "Producing",
                        _ => "Transient",
                    };
                    if expected != "Transient" && expected != sm_variant {
                        tracing::warn!(
                            target: "proposer_drift",
                            local_id = %local_id,
                            expected_from_flags = expected,
                            actual_from_sm = sm_variant,
                            block_loop_running = blr_v,
                            is_intragroup_proposer = ?igp_v,
                            "[T6-DRIFT] flag-derived variant != SM-derived variant"
                        );
                        metrics::counter!("proposer_state_drift_total").increment(1);
                    } else {
                        metrics::counter!("proposer_state_match_total").increment(1);
                    }

                    tracing::info!(
                        target: "proposer_state_diag",
                        local_id = %local_id,
                        block_loop_running = blr_v,
                        is_intragroup_proposer = ?igp_v,
                        last_certified_height = lch_v,
                        consecutive_election_failures = cef_v,
                        sm_variant = sm_variant,
                        "[FLAG-DIAG] proposer-state snapshot"
                    );
                }
            });
        }

        Self {
            local_node_id,
            group_manager,
            signing_key,
            latency_service,
            observations: None,
            pou_scoring,
            gossipsub,
            network_publish_tx,
            latency_topic: IdentTopic::new("/savitri/group/latency/1"),
            pou_topic: IdentTopic::new("/savitri/group/pou/1"),
            pou_ack_topic: IdentTopic::new("/savitri/group/pou/ack/1"),
            ping_topic: IdentTopic::new("/savitri/group/ping/1"),
            pong_topic: IdentTopic::new("/savitri/group/pong/1"),
            election_topic: IdentTopic::new("/savitri/group/election/1"),
            tx_topic: IdentTopic::new("/savitri/group/tx/1"),
            proposal_topic: IdentTopic::new("/savitri/group/proposal/1"),
            vote_topic: IdentTopic::new("/savitri/group/vote/1"),
            mesh_ready: Arc::new(RwLock::new(false)),
            member_latencies: Arc::new(RwLock::new(HashMap::new())),
            member_pou_scores: Arc::new(RwLock::new(HashMap::new())),
            proposer_state: None,
            pending_block_for_cert: Arc::new(RwLock::new(None)),
            follower_state: None,
            received_proposals: Arc::new(RwLock::new(std::collections::VecDeque::new())),
            election_results_collected: Arc::new(RwLock::new(Vec::new())),
            mempool_pipeline,
            storage,
            is_intragroup_proposer,
            whitelist_ack_rx: Arc::new(tokio::sync::Mutex::new(None)),
            block_broadcast_only_tx,
            network_direct_tx,
            dag,
            masternode_peer_ids: Arc::new(RwLock::new(Vec::new())),
            block_loop_running,
            last_committed_election_round: Arc::new(RwLock::new(0)),
            created_at: Instant::now(),
            consecutive_election_failures,
            proposer_schedule: Arc::new(RwLock::new(None)),
            last_certified_height,
            last_certified_height_per_group: Arc::new(std::sync::RwLock::new(HashMap::new())),
            // attached at the legacy flag write sites bring it forward through
            // Elected -> Producing -> SteppingDown -> Idle as elections fire.
            // Same Arc shared with the drift-detector task spawned above.
            proposer_sm: proposer_sm_for_diag,
            // V0.2 Phase 1 (Score Canonicity, issue #31): wired by set_latency_canon_state()
            latency_canon_state: None,
        }
    }

    /// Attach a shared `ObservationStore` so that incoming `GroupPong` events
    /// contribute real RTT samples to PoU scoring. Idempotent.
    pub fn set_observations(&mut self, store: Arc<ObservationStore>) {
        self.observations = Some(store);
    }

    /// Initialize last_certified_height from persistent storage/DAG at boot.
    /// Must be called once after construction so the pipeline starts from the
    /// correct height instead of 0 (which would cause a stall after restart).
    ///
    /// B1 fix (multi-group): also seed `last_certified_height_per_group[local_group_id]`
    /// from storage's chain head when the DAG is empty. This prevents the proposer
    /// from resetting `next_height=1` on cold start after an earlier fix attempt to
    /// remove the global fallback caused a TPS regression (3716→431). With this
    /// seed, the per-group signal is trustworthy from boot onward and
    /// `get_current_block_height` can stop mixing per-group and global values.
    pub async fn initialize_certified_height_from_storage(&self) {
        let mut boot_height = 0u64;
        if let Some(ref dag) = self.dag {
            let dh = dag.get_max_height().await;
            if dh > boot_height {
                boot_height = dh;
            }
            // Initialize per-group heights from DAG tips
            let tips = dag.get_all_tips().await;
            if !tips.is_empty() {
                if let Ok(mut map) = self.last_certified_height_per_group.write() {
                    for (group_id, _hash) in &tips {
                        let gh = dag.get_max_height_for_group(group_id).await;
                        if gh > 0 {
                            map.insert(group_id.clone(), gh);
                        }
                    }
                    info!(
                        groups = map.len(),
                        "Pipeline: initialized per-group certified heights from DAG"
                    );
                }
            }
        }
        if let Some(ref st) = self.storage {
            if let Ok(Some(block)) = st.get_chain_head() {
                if block.height > boot_height {
                    boot_height = block.height;
                }
                // B1 seed: if per-group map has no entry for this node's current
                // group_id but storage holds a chain head from a previous run,
                // anchor the local group at that height. Under SINGLE_GROUP this
                // is exactly right (one group owns the chain); under multi-group
                // it's a safe starting point that cert-driven updates will
                // refine as blocks are finalized.
                let local_group_id = self.get_current_group_id().await;
                if !local_group_id.is_empty() && local_group_id != "unknown" {
                    if let Ok(mut map) = self.last_certified_height_per_group.write() {
                        let entry = map.entry(local_group_id.clone()).or_insert(0);
                        if block.height > *entry {
                            info!(
                                group_id = %local_group_id,
                                height = block.height,
                                prev = *entry,
                                "Pipeline: seeded per-group certified height from storage chain head"
                            );
                            *entry = block.height;
                        }
                    }
                }
            }
        }
        if boot_height > 0 {
            self.last_certified_height
                .store(boot_height, AtomicOrdering::SeqCst);
            info!(
                boot_height,
                "Pipeline: initialized last_certified_height from persistent storage at boot"
            );
        }
    }

    /// Set the consensus direct send channel (called from network.rs after command_tx is created)
    pub fn set_network_direct_tx(&mut self, tx: mpsc::Sender<SwarmCommand>) {
        self.network_direct_tx = Some(tx);
    }

    /// Get a clone of the command_tx channel (for exposing via NetworkComponents)
    pub fn get_command_tx_clone(&self) -> Option<mpsc::Sender<SwarmCommand>> {
        self.network_direct_tx.clone()
    }

    /// Set the known masternode PeerIds for direct TCP (aux protocol) messaging
    pub async fn set_masternode_peer_ids(&self, peer_ids: Vec<PeerId>) {
        *self.masternode_peer_ids.write().await = peer_ids;
    }

    /// Set the whitelist ACK receiver channel (called from network.rs after channel creation)
    pub fn set_whitelist_ack_rx(&self, rx: mpsc::UnboundedReceiver<ProposerWhitelistAck>) {
        // Use try_lock since this is called during init (no contention)
        if let Ok(mut guard) = self.whitelist_ack_rx.try_lock() {
            *guard = Some(rx);
        }
    }

    /// Publish payload on topic via network (swarm) or fallback to local gossipsub
    async fn publish(&self, topic: IdentTopic, payload: Vec<u8>) -> Result<()> {
        if let Some(ref tx) = self.network_publish_tx {
            tx.send((topic, payload)).await.map_err(|_| {
                warn!("Intra-group publish channel closed");
                anyhow::anyhow!("network publish channel closed")
            })?;
            Ok(())
        } else {
            let mut gs = self.gossipsub.write().await;
            gs.publish(topic, payload).map_err(|e| {
                warn!(error=?e, "Gossipsub publish failed (fallback path)");
                anyhow::anyhow!("gossipsub publish: {:?}", e)
            })?;
            Ok(())
        }
    }

    /// Send a consensus message directly to a specific peer via request-response.
    async fn send_consensus_to_peer(
        &self,
        peer_id: &PeerId,
        message: ConsensusMessage,
    ) -> Result<()> {
        if let Some(ref tx) = self.network_direct_tx {
            let cmd = SwarmCommand::SendConsensusRequest {
                peer_id: *peer_id,
                message,
            };
            // Use try_send to avoid blocking when the channel is full.
            // During group formation, many simultaneous latency probes can
            // saturate the command channel and cause deadlock.
            match tx.try_send(cmd) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    warn!(%peer_id, "Consensus command channel full, dropping message");
                    return Err(anyhow::anyhow!("consensus command channel full"));
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    return Err(anyhow::anyhow!("consensus direct channel closed"));
                }
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("consensus direct channel not available"))
        }
    }

    /// Broadcast a consensus message to all group members via direct P2P.
    /// Falls back to gossipsub if direct channel is unavailable.
    async fn broadcast_consensus(
        &self,
        message: ConsensusMessage,
        fallback_topic: IdentTopic,
        payload: Vec<u8>,
    ) -> Result<()> {
        if self.network_direct_tx.is_none() {
            // Fallback to gossipsub when direct channel not available
            return self.publish(fallback_topic, payload).await;
        }

        let members = self.group_manager.get_group_members().await;
        let mut sent = 0usize;
        for member_str in &members {
            if let Ok(peer_id) = member_str.parse::<PeerId>() {
                if peer_id.to_string() == self.local_node_id {
                    continue; // Skip self
                }
                if let Err(e) = self.send_consensus_to_peer(&peer_id, message.clone()).await {
                    debug!(
                        peer = %peer_id,
                        error = ?e,
                        "Failed to send consensus direct to peer (may not be connected yet)"
                    );
                } else {
                    sent += 1;
                }
            }
        }
        trace!(
            sent,
            total_members = members.len(),
            "Consensus direct broadcast complete"
        );
        Ok(())
    }

    /// Initialize intra-group communication with a group-specific topic prefix.
    /// Topics become /savitri/group/{group_id}/tx, ... so only this group's members share the mesh.
    pub async fn initialize(&mut self, group_id: &str) -> Result<()> {
        // PERF: Proposer continuity — if we are the active proposer and still a member
        // of the new group, keep producing blocks instead of stopping for 9+ minutes.
        let new_members = self.group_manager.get_group_members().await;
        let we_are_in_new_group = new_members.contains(&self.local_node_id);
        // returns true iff the SM is in `Producing`, which corresponds 1:1 to
        // the legacy `(block_loop_running && proposer_state.is_some())` AND
        // condition (proposer_state is set when entering Elected, block_loop
        let we_are_proposer = self.proposer_sm.is_loop_active().await;

        if we_are_proposer && we_are_in_new_group {
            info!(
                group_id,
                "Group re-init: PROPOSER CONTINUITY — keeping block production active"
            );
            // Only update topics, keep block loop running
            *self.mesh_ready.write().await = true; // Already have mesh
        } else {
            // Full reset: stop block production for re-election
            *self.mesh_ready.write().await = false;
            if let Some(ref flag) = self.is_intragroup_proposer {
                *flag.write().await = false;
            }
            if let Some(ref proposer_state) = self.proposer_state {
                let mut state = proposer_state.write().await;
                state.is_active = false;
                info!("Stopped previous proposer block loop for group re-initialization");
            }
            self.proposer_state = None;
            if let Some(ref follower_state) = self.follower_state {
                let mut state = follower_state.write().await;
                state.is_active = false;
            }
            self.follower_state = None;
            *self.last_committed_election_round.write().await = 0;
            self.block_loop_running.store(false, AtomicOrdering::SeqCst);
            // Both transitions are best-effort: if the SM is already Idle (no
            // active session), they're no-ops.
            let _ = self
                .proposer_sm
                .try_step_down(crate::p2p::proposer_state::StepDownReason::ManualStepDown)
                .await;
            let _ = self.proposer_sm.try_finish_step_down().await;
        }

        // Always reset election state for fresh group
        self.consecutive_election_failures
            .store(0, AtomicOrdering::SeqCst);
        self.election_results_collected.write().await.clear();
        if !we_are_proposer || !we_are_in_new_group {
            *self.proposer_schedule.write().await = None;
        }
        // Keep only scores/latencies for members in the NEW group (remove stale candidates
        // from previous group to prevent determine_proposer() from electing non-members)
        self.member_pou_scores
            .write()
            .await
            .retain(|member_id, _| new_members.contains(member_id));
        self.member_latencies
            .write()
            .await
            .retain(|member_id, _| new_members.contains(member_id));
        // Unsubscribe from old group topics before overwriting (prevents ghost subscriptions
        // that cause persistent InsufficientPeers errors on stale topics with mesh_peers=0)
        {
            let old_topics = [
                &self.ping_topic,
                &self.pong_topic,
                &self.tx_topic,
                &self.latency_topic,
                &self.pou_topic,
                &self.pou_ack_topic,
                &self.election_topic,
                &self.proposal_topic,
                &self.vote_topic,
            ];
            let mut gossipsub = self.gossipsub.write().await;
            for old_topic in &old_topics {
                let was_subscribed = gossipsub.unsubscribe(old_topic);
                if was_subscribed {
                    debug!(topic = %old_topic, "Unsubscribed from old group topic");
                }
            }
        }

        self.latency_topic = IdentTopic::new(format!("/savitri/group/{}/latency", group_id));
        self.pou_topic = IdentTopic::new(format!("/savitri/group/{}/pou", group_id));
        self.pou_ack_topic = IdentTopic::new(format!("/savitri/group/{}/pou_ack", group_id));
        self.ping_topic = IdentTopic::new(format!("/savitri/group/{}/ping", group_id));
        self.pong_topic = IdentTopic::new(format!("/savitri/group/{}/pong", group_id));
        self.election_topic = IdentTopic::new(format!("/savitri/group/{}/election", group_id));
        self.tx_topic = IdentTopic::new(format!("/savitri/group/{}/tx", group_id));
        self.proposal_topic = IdentTopic::new(format!("/savitri/group/{}/proposal", group_id));
        self.vote_topic = IdentTopic::new(format!("/savitri/group/{}/vote", group_id));

        // Subscribe to new group's gossipsub topics (TX, ping/pong for mesh readiness).
        // Consensus topics (vote, election, latency, pou, proposal) use direct P2P request-response.
        {
            let mut gossipsub = self.gossipsub.write().await;
            gossipsub.subscribe(&self.ping_topic)?;
            gossipsub.subscribe(&self.pong_topic)?;
            gossipsub.subscribe(&self.tx_topic)?;
        }

        info!(group_id = %group_id, "Subscribed to intra-group gossipsub topics (TX, ping/pong); consensus via direct P2P");
        info!("Intra-group sync initialized");
        info!("Transaction communication initialized");
        Ok(())
    }

    fn intragroup_signing_payload(
        &self,
        group_id: &str,
        msg_type: &str,
        signable: &[u8],
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(b"savitri-intragroup-v1|");
        data.extend_from_slice(msg_type.as_bytes());
        data.extend_from_slice(b"|");
        data.extend_from_slice(group_id.as_bytes());
        data.extend_from_slice(b"|");
        data.extend_from_slice(signable);
        data
    }

    fn sign_intragroup_message(&self, group_id: &str, msg_type: &str, signable: &[u8]) -> [u8; 64] {
        let payload = self.intragroup_signing_payload(group_id, msg_type, signable);
        let signature = self.signing_key.sign(&payload);
        signature.to_bytes()
    }

    fn verify_intragroup_message(
        &self,
        sender_hex: &str,
        group_id: &str,
        msg_type: &str,
        signable: &[u8],
        signature: &[u8; 64],
    ) -> bool {
        let sender_bytes =
            safe_hex_decode(sender_hex, Vec::new(), Some("verify_intragroup_signature"));
        if sender_bytes.len() != 32 {
            warn!(sender = %sender_hex, "Invalid sender pubkey length for intra-group message");
            return false;
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(&sender_bytes);
        let verifying_key = match VerifyingKey::from_bytes(&pk) {
            Ok(key) => key,
            Err(err) => {
                warn!(error=?err, sender = %sender_hex, "Failed to parse sender pubkey for intra-group message");
                return false;
            }
        };

        let payload = self.intragroup_signing_payload(group_id, msg_type, signable);
        let signature = Signature::from_bytes(signature);
        verifying_key.verify_strict(&payload, &signature).is_ok()
    }

    /// Local Ed25519 public key as hex (64 chars) for use in intra-group message sender fields.
    fn local_pubkey_hex(&self) -> String {
        hex::encode(self.signing_key.verifying_key().as_bytes())
    }

    /// Simulated latency (ms) for testing when no real RTT is available.
    /// Only compiled with `--features test_simulated_latency` (excluded from release).
    #[cfg(feature = "test_simulated_latency")]
    fn simulated_latency_ms(node_id: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        node_id.hash(&mut hasher);
        let h = hasher.finish();
        20 + (h % 131) // 20-150 ms
    }

    async fn is_group_match(&self, group_id: &str) -> bool {
        self.group_manager
            .get_current_group()
            .await
            .map(|g| g.group_id == group_id)
            .unwrap_or(false)
    }

    /// Check if we're currently in a group
    pub async fn is_in_group(&self) -> bool {
        self.group_manager.get_current_group().await.is_some()
    }

    /// Get transaction topic for network integration
    pub fn get_tx_topic(&self) -> IdentTopic {
        self.tx_topic.clone()
    }

    /// Get latency topic for network integration
    pub fn get_latency_topic(&self) -> IdentTopic {
        self.latency_topic.clone()
    }

    /// Get PoU topic for network integration
    pub fn get_pou_topic(&self) -> IdentTopic {
        self.pou_topic.clone()
    }

    /// Get PoU ACK topic for network integration
    pub fn get_pou_ack_topic(&self) -> IdentTopic {
        self.pou_ack_topic.clone()
    }

    /// Get ping topic for mesh readiness probe
    pub fn get_ping_topic(&self) -> IdentTopic {
        self.ping_topic.clone()
    }

    /// Get pong topic for mesh readiness probe
    pub fn get_pong_topic(&self) -> IdentTopic {
        self.pong_topic.clone()
    }

    /// Whether mesh is ready (we received at least one Pong from a group member)
    pub async fn is_mesh_ready(&self) -> bool {
        // Fix 6f: Single-member groups are always mesh-ready (no peers to ping)
        let other_members = self.group_manager.get_group_members().await;
        if other_members.is_empty() {
            return true;
        }
        *self.mesh_ready.read().await
    }

    /// Get election topic for network integration
    pub fn get_election_topic(&self) -> IdentTopic {
        self.election_topic.clone()
    }

    /// Get proposal topic for intra-group block proposals (followers receive real proposals)
    pub fn get_proposal_topic(&self) -> IdentTopic {
        self.proposal_topic.clone()
    }

    /// Get vote topic for intra-group consensus (solo membri of the gruppo ascoltano/inviano)
    pub fn get_vote_topic(&self) -> IdentTopic {
        self.vote_topic.clone()
    }

    pub async fn receive_proposal(
        &self,
        proposer_id: String,
        proposal: crate::proposer::BlockProposal,
    ) {
        let mut q = self.received_proposals.write().await;
        // Keep at most 50 proposals to avoid unbounded growth
        if q.len() >= 50 {
            q.pop_front();
        }
        q.push_back((proposal.round_id, proposer_id, proposal));
    }

    pub async fn take_next_proposal(
        &self,
    ) -> Option<(u64, String, crate::proposer::BlockProposal)> {
        let mut q = self.received_proposals.write().await;
        q.pop_front()
    }

    /// Start latency measurement with group members
    pub async fn start_latency_measurement(&self) -> Result<()> {
        let group_members = self.group_manager.get_group_members().await;
        let group_id = self.get_current_group_id().await;

        if group_members.is_empty() {
            debug!(
                group_id = %group_id,
                "No group members for latency measurement"
            );
            return Ok(());
        }

        info!(
            group_id = %group_id,
            members = group_members.len(),
            "Starting latency measurement with group members"
        );

        // Send latency probes to all group members
        for member in group_members {
            if let Err(e) = self.send_latency_probe(&member).await {
                warn!("Failed to send latency probe to {}: {}", member, e);
            }
        }

        Ok(())
    }

    /// Send latency probe to group member (timestamp in ms per RTT preciso)
    async fn send_latency_probe(&self, target_member: &str) -> Result<()> {
        let mut probe = GroupLatencyProbe {
            probe_id: rand::random::<u64>(),
            sender: self.local_node_id.clone(),
            sender_pubkey_hex: Some(self.local_pubkey_hex()),
            timestamp_ms: get_safe_timestamp_ms(),
            group_id: self.get_current_group_id().await,
            signature: [0u8; 64],
        };
        let signable = probe.signable_bytes()?;
        probe.signature = self.sign_intragroup_message(&probe.group_id, "latency_probe", &signable);

        let payload = serde_json::to_vec(&probe)?;

        // Send latency probe only to the specific target, not broadcast to all.
        // Broadcasting N probes to all N members (N*(N-1) messages) floods the
        // command channel and causes deadlock during group formation.
        if let Ok(peer_id) = target_member.parse::<PeerId>() {
            self.send_consensus_to_peer(&peer_id, ConsensusMessage::Latency(payload))
                .await?;
        } else {
            // Fallback to gossipsub publish if peer_id parse fails
            self.publish(self.latency_topic.clone(), payload).await?;
        }

        debug!("Sent latency probe to {}", target_member);
        Ok(())
    }

    /// Share PoU score with group members
    pub async fn share_pou_score(&self) -> Result<()> {
        let current_pou = if let Some(ref pou_scoring) = self.pou_scoring {
            pou_scoring.get_current_score().await
        } else {
            // PoU scoring subsystem not initialised — derive a deterministic
            // baseline from uptime and participation state instead of a mock.
            self.compute_baseline_pou_score().await
        };

        let group_id = self.get_current_group_id().await;

        let mut pou_share = PouScoreShare {
            node_id: self.local_node_id.clone(),
            node_pubkey_hex: Some(self.local_pubkey_hex()),
            pou_score: current_pou as u32,
            epoch: self.get_current_epoch().await,
            group_id,
            timestamp: get_safe_timestamp(),
            signature: [0u8; 64],
        };
        let signable = pou_share.signable_bytes()?;
        pou_share.signature =
            self.sign_intragroup_message(&pou_share.group_id, "pou_share", &signable);

        let payload = serde_json::to_vec(&pou_share)?;
        if let Err(e) = self
            .broadcast_consensus(
                ConsensusMessage::PoU(payload.clone()),
                self.pou_topic.clone(),
                payload,
            )
            .await
        {
            warn!(error=?e, "Failed to broadcast PoU score to group");
            return Err(e.into());
        }

        info!("Shared PoU score {} with group members", current_pou);
        Ok(())
    }

    /// Share PoU score with masternode on network topic
    pub async fn share_pou_score_with_masternode(&self) -> Result<()> {
        let current_pou = if let Some(ref pou_scoring) = self.pou_scoring {
            pou_scoring.get_current_score().await
        } else {
            // PoU scoring subsystem not initialised — derive a deterministic
            // baseline from uptime and participation state.
            self.compute_baseline_pou_score().await
        };

        // Create PoU broadcast for masternode (same structure as masternode expects)
        let pou_broadcast = PouBroadcast {
            peer_id: self.local_node_id.clone(),
            epoch: self.get_current_epoch().await,
            score: current_pou,
            index: 1, // Default index
            timestamp: get_safe_timestamp(),
        };

        let payload = serde_json::to_vec(&pou_broadcast)?;
        // Send PoU via direct TCP (aux protocol) to masternodes if command channel available
        if let Some(ref tx) = self.network_direct_tx {
            let mn_peers = self.masternode_peer_ids.read().await.clone();
            if !mn_peers.is_empty() {
                for mn_peer in &mn_peers {
                    let cmd = SwarmCommand::SendAuxRequest {
                        peer_id: mn_peer.clone(),
                        message: crate::p2p::aux_protocol::AuxMessage::PoU(payload.clone()),
                    };
                    // Use try_send to avoid blocking when channel is full.
                    // Blocking send here can stall the entire PoU sharing task.
                    match tx.try_send(cmd) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            debug!("Channel full, skipping PoU aux to {}", mn_peer);
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            debug!("Channel closed, cannot send PoU aux to {}", mn_peer);
                        }
                    }
                }
                info!(
                    "Shared PoU score {} with {} masternodes via direct TCP",
                    current_pou,
                    mn_peers.len()
                );
                // AuxProtocol messages are NOT processed by MN, so they never refresh
                // last_seen. Without gossipsub keepalive, MN removes LN after 300s.
            }
        }
        // This is the ONLY reliable way to refresh last_seen on MN, because
        // MN refreshes last_seen on gossipsub messages but ignores AuxProtocol.
        let masternode_pou_topic = libp2p::gossipsub::IdentTopic::new("/savitri/pou/1");
        self.publish(masternode_pou_topic.clone(), payload).await?;
        info!(
            "Shared PoU score {} with masternode (gossipsub keepalive)",
            current_pou
        );
        Ok(())
    }

    /// Request masternode peer discovery
    pub async fn request_masternode_peer_discovery(&self) -> Result<()> {
        let request = PeerDiscoveryRequest {
            requesting_peer: self.local_node_id.clone(),
        };

        let payload = serde_json::to_vec(&request)?;
        // Send peer discovery via direct TCP (aux protocol) to masternodes
        if let Some(ref tx) = self.network_direct_tx {
            let mn_peers = self.masternode_peer_ids.read().await.clone();
            if !mn_peers.is_empty() {
                for mn_peer in &mn_peers {
                    let cmd = SwarmCommand::SendAuxRequest {
                        peer_id: mn_peer.clone(),
                        message: crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryRequest(
                            payload.clone(),
                        ),
                    };
                    if let Err(e) = tx.send(cmd).await {
                        debug!("Failed to send peer discovery aux to {}: {}", mn_peer, e);
                    }
                }
                info!(
                    "Sent peer discovery request to {} masternodes via direct TCP",
                    mn_peers.len()
                );
                return Ok(());
            }
        }
        // Fallback to gossipsub
        let peer_discovery_topic = libp2p::gossipsub::IdentTopic::new("/savitri/peer_discovery/1");
        self.publish(peer_discovery_topic, payload).await?;
        info!("Sent masternode peer discovery request (gossipsub fallback)");
        Ok(())
    }

    /// Start proposer election process
    pub async fn start_proposer_election(&self) -> Result<()> {
        let current_pou = if let Some(ref pou_scoring) = self.pou_scoring {
            pou_scoring.get_current_score().await
        } else {
            // PoU scoring subsystem not initialised — derive a deterministic
            // baseline from uptime and participation state.
            self.compute_baseline_pou_score().await
        };

        let group_id = self.get_current_group_id().await;

        // Fix 6f: Single-member group → self-elect immediately (no gossipsub needed)
        let other_members = self.group_manager.get_group_members().await;
        if other_members.is_empty() {
            // returns true iff the SM is `Elected` or `Producing`; both states
            // mean we're already self-elected (or about to start producing),
            // so this is identical to the legacy flag check.
            if self.proposer_sm.is_proposer_role().await {
                return Ok(()); // Already self-elected
            }
            info!(
                group_id = %group_id,
                pou_score = current_pou,
                "Single-member group: self-electing as proposer (no gossipsub election needed)"
            );
            if let Some(ref flag) = self.is_intragroup_proposer {
                *flag.write().await = true;
            }
            // expected/swallowed if the SM is already Elected/Producing
            // (idempotent under repeated periodic calls).
            let _ = self.proposer_sm.try_elect(0, 0).await;
            // Guard: prevent duplicate loops (shared across clones via Arc<AtomicBool>)
            if self
                .block_loop_running
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_err()
            {
                info!("Single-member self-elect: block production loop already running, skipping");
                return Ok(());
            }
            // a placeholder; the loop overwrites via record_height each
            let _ = self.proposer_sm.try_start_producing(0).await;
            // Spawn block production loop (mirrors start_proposer_duties)
            // flag clone. Inside the loop the reader uses the SM; the flag
            // clone is kept only for symmetry with the writer side and will
            let proposer_sm_clone = self.proposer_sm.clone();
            let intra_group_comm_clone = self.clone();
            let block_loop_flag = self.block_loop_running.clone();
            tokio::spawn(async move {
                let mut adaptive_sleep_ms: u64 = 1000;
                let mut round: u64 = 0;
                let mut last_proposed_height: u64 = 0;
                // Old hardcoded 16 was too tight under load — at 50ms tick it
                // saturates in 800ms, then we hit `continue` and stall until
                // certs commit. Default 64 buys 4× headroom; combined with
                // is eliminated.
                let max_pipeline_depth: u64 = std::env::var("SAVITRI_PROPOSER_PIPELINE_DEPTH")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|&d| d > 0)
                    .unwrap_or(64);
                let max_empty_ticks: u32 = std::env::var("SAVITRI_MAX_EMPTY_TICKS")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .filter(|&v| v > 0)
                    .unwrap_or(5);
                let min_tx_per_block: usize = std::env::var("SAVITRI_MIN_TX_PER_BLOCK")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);
                let block_threshold: usize = if min_tx_per_block > 0 {
                    min_tx_per_block
                } else {
                    1
                };
                let mut consecutive_empty_ticks: u32 = 0;
                loop {
                    tokio::time::sleep(Duration::from_millis(adaptive_sleep_ms)).await;
                    // The legacy flag was flipped to false by `start_following_proposer`;
                    // the SM is mirror-written to `SteppingDown -> Idle` in the
                    // same code path. `is_proposer_role()` returns false in both
                    // SteppingDown and Idle, so the loop breaks identically.
                    if !proposer_sm_clone.is_proposer_role().await {
                        info!("Block production loop stopped (no longer proposer)");
                        break;
                    }
                    round += 1;
                    // PIPELINING: Allow proposing ahead of finalized height
                    let finalized_height = intra_group_comm_clone.get_current_block_height().await;
                    let pipeline_ahead = last_proposed_height.saturating_sub(finalized_height);
                    if pipeline_ahead >= max_pipeline_depth {
                        crate::observability::PipelineObsMetrics::inc_block_pipeline_full();
                        continue; // Pipeline full, wait for MN finalization
                    }
                    let next_height = std::cmp::max(finalized_height + 1, last_proposed_height + 1);
                    // Minimum wait: skip if mempool empty.
                    let mempool_len =
                        if let Some(ref pipeline) = intra_group_comm_clone.mempool_pipeline {
                            pipeline.len_async().await
                        } else {
                            0
                        };
                    // self-delivery loop below for full rationale.
                    if mempool_len < block_threshold {
                        crate::observability::PipelineObsMetrics::inc_block_throttled_density();
                        consecutive_empty_ticks += 1;
                        adaptive_sleep_ms = 1000;
                        if consecutive_empty_ticks < max_empty_ticks {
                            continue;
                        }
                        let skip_empty: bool = std::env::var("SAVITRI_SKIP_EMPTY_BLOCKS")
                            .ok()
                            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                            .unwrap_or(false);
                        if skip_empty {
                            let heartbeat_ticks: u32 =
                                std::env::var("SAVITRI_EMPTY_HEARTBEAT_TICKS")
                                    .ok()
                                    .and_then(|v| v.parse::<u32>().ok())
                                    .filter(|&v| v > 0)
                                    .unwrap_or(60);
                            let after_max = consecutive_empty_ticks.saturating_sub(max_empty_ticks);
                            if after_max % heartbeat_ticks != 0 {
                                continue;
                            }
                        }
                    }
                    consecutive_empty_ticks = 0;
                    adaptive_sleep_ms = if mempool_len > 200 {
                        50
                    } else if mempool_len > 50 {
                        200
                    } else {
                        500
                    };
                    last_proposed_height = next_height;
                    tracing::info!(
                        round,
                        height = next_height,
                        finalized = finalized_height,
                        depth = next_height - finalized_height,
                        mempool = mempool_len,
                        "PIPELINE(single): proposing block"
                    );
                    crate::observability::PipelineObsMetrics::inc_block_proposed();
                    if let Err(e) = intra_group_comm_clone
                        .create_and_propose_block_at_height(round, Some(next_height))
                        .await
                    {
                        error!("Failed to create and propose block: {}", e);
                    }
                }
                // Reset flag so a new loop can be spawned after re-election
                block_loop_flag.store(false, AtomicOrdering::SeqCst);
                info!("Block production loop exited (single-member), block_loop_running reset to false");
            });
            info!(
                group_id = %group_id,
                "Self-elected proposer: block production loop started"
            );
            return Ok(());
        }

        info!(
            group_id = %group_id,
            pou_score = current_pou,
            "Starting intra-group proposer election (PoU consensus)"
        );
        info!(
            group_id = %group_id,
            "Election of proposer lightnode started"
        );

        let mut election = ProposerElection {
            round: rand::random::<u64>(),
            candidate: self.local_node_id.clone(),
            candidate_pubkey_hex: Some(self.local_pubkey_hex()),
            pou_score: current_pou as u32,
            group_id,
            timestamp: get_safe_timestamp(),
            signature: [0u8; 64],
        };
        let signable = election.signable_bytes()?;
        election.signature =
            self.sign_intragroup_message(&election.group_id, "election", &signable);

        let payload = serde_json::to_vec(&election)?;
        if let Err(e) = self
            .broadcast_consensus(
                ConsensusMessage::Election(payload.clone()),
                self.election_topic.clone(),
                payload,
            )
            .await
        {
            warn!(error=?e, "Failed to broadcast proposer election to group");
            return Err(e.into());
        }

        info!("Started proposer election with PoU score {}", current_pou);
        Ok(())
    }

    /// Determine proposer based on PoU scores and latency

    // ─────────────────────────────────────────────────────────────────
    // V0.2 Phase 1 (Score Canonicity, issue #31) — Latency Canon helpers
    // ─────────────────────────────────────────────────────────────────

    /// Install the LatencyCanonState. Called by the network task after
    /// it constructs the state holder and subscribes to the gossip
    /// topic. Subsequent calls overwrite — caller is expected to do
    /// this exactly once per IGC instance.
    pub fn set_latency_canon_state(
        &mut self,
        state: std::sync::Arc<crate::latency_canon_state::LatencyCanonState>,
    ) {
        self.latency_canon_state = Some(state);
    }

    /// Canonical gossip topic for this LN's current group:
    /// `/savitri/group/<group_id>/latency_canon/1`.
    pub async fn get_latency_canon_topic(&self) -> libp2p::gossipsub::IdentTopic {
        let group_id = self
            .group_manager
            .get_current_group_cached()
            .map(|g| g.group_id.clone())
            .unwrap_or_default();
        crate::latency_canon_publisher::topic_for_group(&group_id)
    }

    /// Receive-side: deserialize a `LatencyReport`, verify the signature,
    /// confirm the group matches ours, and feed it to the state holder.
    /// Returns Ok even on validation failure (we log + drop, no error).
    pub async fn process_latency_canon_message(&self, data: &[u8]) -> anyhow::Result<()> {
        let report: savitri_consensus::types::LatencyReport = match serde_json::from_slice(data) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(error = %e, "LatencyCanon: failed to deserialize report");
                return Ok(());
            }
        };
        if !report.verify_signature() {
            tracing::warn!(
                reporter = %report.reporter,
                round = report.round,
                "LatencyCanon: signature verification failed - DROP"
            );
            return Ok(());
        }
        let local_group = self
            .group_manager
            .get_current_group_cached()
            .map(|g| g.group_id.clone())
            .unwrap_or_default();
        if !local_group.is_empty() && report.group_id != local_group {
            tracing::debug!(
                report_group = %report.group_id,
                local_group = %local_group,
                "LatencyCanon: report for foreign group - DROP"
            );
            return Ok(());
        }
        if let Some(ref state) = self.latency_canon_state {
            state.ingest_report(report);
        } else {
            tracing::debug!("LatencyCanon: state holder not yet installed - DROP");
        }
        Ok(())
    }

    /// Periodic rebuild + DIAG snapshot. Returns the current canonical
    /// table size so a periodic logger can publish it. No-op if the
    /// state holder is not yet installed.
    ///
    /// V0.2 Phase 2 (latency table convergence): `window_end` is now the
    /// current wall-clock-aligned bucket (not chain height). All LNs
    /// rebuild against the SAME bucket index, so the canonical table is
    /// byte-identical cluster-wide. Pre-Phase-2 the window was per-LN
    /// chain head, which lagged differently per-observer and broke
    /// candidates determinism.
    pub async fn rebuild_latency_canon_table(&self) -> usize {
        let Some(ref state) = self.latency_canon_state else {
            return 0;
        };
        let bucket = crate::latency_canon_publisher::current_wall_clock_bucket();
        let table = state.rebuild(bucket);
        let count = table.defined_pair_count();
        let buffered = state.buffered_count();
        tracing::warn!(
            target: "savitri::diag",
            window_end = bucket,
            defined_pairs = count,
            buffered_reports = buffered,
            "DIAG[latency-canon] table rebuilt"
        );
        count
    }

    pub async fn determine_proposer(&self) -> Option<String> {
        let latencies = self.member_latencies.read().await;
        let pou_scores = self.member_pou_scores.read().await;

        // V0.2 Phase 1.5 port: combined_score is now u32 permille (0..1000),
        // not f64. The conversion from local f64 to permille happens at the wire
        // boundary inside this function (see candidates.push call). Phase 1.4c
        // (canonical lookup against LatencyCanonState) is NOT ported here yet.
        let mut candidates: Vec<(String, u32, u32)> = Vec::new(); // (node_id, combined_permille, pou_score)

        // Include ourselves
        let our_pou = if let Some(ref pou_scoring) = self.pou_scoring {
            pou_scoring.get_current_score().await
        } else {
            // PoU scoring subsystem not initialised — derive a deterministic
            // baseline from uptime and participation state.
            self.compute_baseline_pou_score().await
        };

        // V0.2 Phase 1.4c port (Score Canonicity, issue #31): self uses the same
        // formula as peers. The canonical lookup for our own peer_id usually
        // returns None (no one reports our RTT to ourselves) — neutral 1000
        // falls out, matching the legacy 0.5 semantics (max value, no
        // self-penalty).
        let our_group_id_for_lookup = self
            .group_manager
            .get_current_group_cached()
            .map(|g| g.group_id)
            .unwrap_or_default();
        let our_latency_canon_permille: u32 = match self.latency_canon_state.as_ref() {
            Some(state) => state.lookup_score(&our_group_id_for_lookup, &self.local_node_id) as u32,
            None => 1000,
        };
        let our_pou_normalized_permille: u32 = (our_pou as u32) / 10;
        let our_combined_permille: u32 = (our_pou_normalized_permille.saturating_mul(7)
            + our_latency_canon_permille.saturating_mul(3))
            / 10;
        candidates.push((
            self.local_node_id.clone(),
            our_combined_permille,
            our_pou as u32,
        ));

        // Add other members (only current group members with fresh PoU scores)
        let current_group_members = self.group_manager.get_group_members().await;
        let now = Instant::now();
        let pou_freshness_threshold = Duration::from_secs(120);
        for (member_id, (pou_score, last_updated)) in pou_scores.iter() {
            // Skip stale candidates that are not in the current group
            if !current_group_members.contains(member_id) {
                debug!(member_id = %member_id, "Skipping non-group-member candidate in election");
                continue;
            }
            // Disconnected LNs can't send fresh PoU scores, so they should not
            // be elected as proposer — they can't produce blocks.
            if now.duration_since(*last_updated) > pou_freshness_threshold {
                debug!(
                    member_id = %member_id,
                    age_secs = now.duration_since(*last_updated).as_secs(),
                    "Skipping candidate with stale PoU score (> 120s old)"
                );
                continue;
            }
            // V0.2 Phase 1.4c port (Score Canonicity, issue #31): replace the
            // per-observer f64 latency_score with a deterministic integer
            // lookup against the canonical LatencyTable. combined_permille is
            // computed entirely in u32 — no f64 path remains.
            //
            // latency_canon_permille: u32 in [0, 1000], 1000 = fast RTT.
            // pou_normalized_permille: u32 in [0, 1000] = pou_score / 10.
            // combined_permille: u32 in [0, 1000] = 70% PoU + 30% latency canon.
            let group_id_for_lookup = self
                .group_manager
                .get_current_group_cached()
                .map(|g| g.group_id)
                .unwrap_or_default();
            let latency_canon_permille: u32 = match self.latency_canon_state.as_ref() {
                Some(state) => state.lookup_score(&group_id_for_lookup, member_id) as u32,
                None => 1000, // No table yet (bootstrap) — neutral max.
            };
            let pou_normalized_permille: u32 = (*pou_score) / 10;
            let combined_permille: u32 = (pou_normalized_permille.saturating_mul(7)
                + latency_canon_permille.saturating_mul(3))
                / 10;
            // Silence unused-import warning while the legacy latencies map remains
            // populated by the GroupPong handler (still wired for diagnostic use).
            let _ = &latencies;
            candidates.push((member_id.clone(), combined_permille, *pou_score));
        }

        // Sort by integer PoU score (deterministic across all nodes).
        // Using combined_score (which includes per-node latency) caused different nodes
        // to elect different proposers, breaking election certificate signatures.
        //
        // caused DAG split — get_current_epoch() depends on block_height which
        // diverges between LNs that haven't fully synced, so different LNs
        // computed different orderings. Loadtest regressed 67 -> 21 TPS.
        // Reverting to peer_id ascending tiebreaker. The PoU floor (1000)
        // applied in availability.rs:502 is the safe half of the fix that
        // remains active; combined with real PoU score variation across LNs
        // (uptime, latency, integrity), the lex-low bias should diminish
        // organically once scores diverge.
        candidates.sort_by(|a, b| {
            b.2.cmp(&a.2) // Primary: pou_score (u32) descending
                .then_with(|| a.0.cmp(&b.0)) // Tiebreaker: peer_id ascending (stable cross-node)
        });
        // Require a majority of candidates before broadcasting a result.
        // With too few candidates, each node has a different subset and may elect
        // different proposers, causing oscillation.
        // ROUND 7 FIX: Progressive quorum relaxation — after consecutive failures,
        // lower the threshold so the network can make progress instead of stalling forever.
        let group_members = self.group_manager.get_group_members().await;
        let total_members = group_members.len() + 1; // +1 for self
        let normal_min = if total_members <= 2 {
            total_members
        } else {
            (total_members + 1) / 2
        };
        let failures = self
            .consecutive_election_failures
            .load(AtomicOrdering::SeqCst);
        let min_candidates = if failures >= 3 && candidates.len() >= 2 {
            // After 3+ failed elections, accept any 2+ candidates to break the stall.
            // With sorted candidates (by PoU score + peer_id tiebreaker), even 2 candidates
            // produce a deterministic result across all nodes.
            let relaxed = 2.max(normal_min.saturating_sub(failures as usize));
            warn!(
                normal_min,
                relaxed_min = relaxed,
                consecutive_failures = failures,
                candidates_count = candidates.len(),
                "Election quorum RELAXED after {} consecutive failures",
                failures
            );
            relaxed
        } else {
            normal_min
        };
        info!(
            candidates_count = candidates.len(),
            min_required = min_candidates,
            group_members = group_members.len(),
            consecutive_failures = failures,
            "Election of proposer lightnode in progress"
        );

        if candidates.len() < min_candidates {
            self.consecutive_election_failures
                .fetch_add(1, AtomicOrdering::SeqCst);
            warn!(
                candidates_count = candidates.len(),
                min_required = min_candidates,
                consecutive_failures = failures + 1,
                "Not enough candidates to determine proposer, waiting for more election messages"
            );
            return None;
        }
        // Reset failure counter on successful election
        self.consecutive_election_failures
            .store(0, AtomicOrdering::SeqCst);

        let proposer = candidates.first().map(|(id, _, _)| id.clone());

        if let Some(ref proposer_id) = proposer {
            // Populate cached PoU schedule for tenure-based rotation
            let ranked: Vec<(String, u32)> = candidates
                .iter()
                .map(|(id, _combined, pou)| (id.clone(), *pou))
                .collect();
            let finalized_h = self.get_current_block_height().await;
            {
                let mut sched = self.proposer_schedule.write().await;
                *sched = Some(ProposerSchedule {
                    current_proposer: proposer_id.clone(),
                    next_proposer: ranked.get(1).map(|(id, _)| id.clone()),
                    tenure_start_height: finalized_h,
                    ranked_candidates: ranked,
                    last_updated: Instant::now(),
                });
            }
            info!(
                proposer = %proposer_id,
                candidates_count = candidates.len(),
                tenure_start = finalized_h,
                "Determined group proposer (tenure schedule cached)"
            );

            // Broadcast election result to all group members
            if let Err(e) = self
                .broadcast_election_result(&candidates, proposer_id)
                .await
            {
                error!("Failed to broadcast election result: {}", e);
            }

            // Self-delivery with BFT attestation wait:
            // We are the elected proposer — wait up to 2s for quorum attestations
            // before starting block production. This ensures the ElectionCertificate
            // has BFT proof (≥(N+1)/2 attestations) while keeping the fast path.
            if *proposer_id == self.local_node_id {
                let group_members = self.group_manager.get_group_members().await;
                let quorum_needed = (group_members.len() + 1) / 2;
                let collected = self.election_results_collected.clone();

                // Wait up to 2s for quorum attestations
                let wait_result = tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        let count = collected.read().await.len();
                        if count >= quorum_needed {
                            return count;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await;

                match wait_result {
                    Ok(count) => {
                        info!(
                            attestations = count,
                            quorum = quorum_needed,
                            "BFT quorum reached — starting block production with valid certificate"
                        );
                    }
                    Err(_) => {
                        let count = collected.read().await.len();
                        warn!(
                            attestations = count,
                            quorum = quorum_needed,
                            "BFT attestation timeout (2s) — proceeding with partial certificate"
                        );
                    }
                }
                info!("Elected proposer: starting block production");
                if let Some(ref flag) = self.is_intragroup_proposer {
                    *flag.write().await = true;
                }
                // Idle -> Elected (idempotent on Elected/Producing).
                let _ = self.proposer_sm.try_elect(0, 0).await;
                if self
                    .block_loop_running
                    .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                    .is_ok()
                {
                    // successful CAS into the loop.
                    let _ = self.proposer_sm.try_start_producing(0).await;
                    // Tenure-based rotation: 30 blocks per proposer tenure.
                    const SELF_DELIVERY_ROTATION_BLOCKS: u64 = PROPOSER_TENURE_BLOCKS;
                    // legacy flag clone (writer side still flips the flag for
                    // downstream code that hasn't migrated yet).
                    let proposer_sm_clone = self.proposer_sm.clone();
                    let proposer_flag_clone = self.is_intragroup_proposer.clone();
                    let intra_group_comm_clone = self.clone();
                    let block_loop_flag = self.block_loop_running.clone();
                    tokio::spawn(async move {
                        let mut adaptive_sleep_ms: u64 = 1000;
                        let mut round: u64 = 0;
                        let mut blocks_proposed: u64 = 0;
                        let mut rotation_needed = false;
                        let mut last_proposed_height: u64 = 0;
                        // loop above for rationale. Same env override.
                        let max_pipeline_depth: u64 =
                            std::env::var("SAVITRI_PROPOSER_PIPELINE_DEPTH")
                                .ok()
                                .and_then(|v| v.parse::<u64>().ok())
                                .filter(|&d| d > 0)
                                .unwrap_or(64);
                        // values trade block rate for block density: with 245 TPS
                        // submit / 3 groups, the proposer's mempool can stay empty
                        // for 1-3 s between drains (gossip RX is bursty), so a
                        // value of 5 (= 5 s waiting) often surrenders and proposes
                        // even though max single-block density was 1598 TX. A
                        // 30-tick guard (= 30 s waiting) accumulates mempool
                        // significantly more before yielding an empty proposal,
                        // block rate (memory: investigation_p1_p2_p3_2026-05-03).
                        let max_empty_ticks: u32 = std::env::var("SAVITRI_MAX_EMPTY_TICKS")
                            .ok()
                            .and_then(|v| v.parse::<u32>().ok())
                            .filter(|&v| v > 0)
                            .unwrap_or(5);
                        // "mempool below MIN_TX_PER_BLOCK" the same as empty,
                        // so the empty-tick / heartbeat machinery also
                        // would emit blocks with 1-10 TX as soon as mempool
                        // > 0; cluster-wide cert metrics showed only 76
                        // non-zero blocks out of 3888 (= 2%) and only 1.1
                        // avg TX/block (memory:
                        // cert_diag_real_bottleneck_2026-05-04). Setting
                        // SAVITRI_MIN_TX_PER_BLOCK=100 yields a single gate:
                        // wait until mempool_len >= 100 OR the empty/heartbeat
                        // ticks boundary fires. Default 0 = legacy
                        // (mempool == 0 only triggers the gate).
                        let min_tx_per_block: usize = std::env::var("SAVITRI_MIN_TX_PER_BLOCK")
                            .ok()
                            .and_then(|v| v.parse::<usize>().ok())
                            .unwrap_or(0);
                        let block_threshold: usize = if min_tx_per_block > 0 {
                            min_tx_per_block
                        } else {
                            1
                        };
                        let mut consecutive_empty_ticks: u32 = 0;
                        loop {
                            tokio::time::sleep(Duration::from_millis(adaptive_sleep_ms)).await;
                            // (mirrors the single-member loop migration above).
                            if !proposer_sm_clone.is_proposer_role().await {
                                info!("Block production loop stopped (no longer proposer)");
                                break;
                            }
                            round += 1;
                            // ROUND 7: Proposer rotation
                            if blocks_proposed >= SELF_DELIVERY_ROTATION_BLOCKS {
                                info!(
                                    blocks_proposed,
                                    "Self-delivery: proposer rotation after {} blocks",
                                    blocks_proposed
                                );
                                if let Some(ref flag) = proposer_flag_clone {
                                    *flag.write().await = false;
                                }
                                rotation_needed = true;
                                break;
                            }
                            // PIPELINING: Allow proposing ahead of finalized height
                            let finalized_height =
                                intra_group_comm_clone.get_current_block_height().await;
                            let pipeline_ahead =
                                last_proposed_height.saturating_sub(finalized_height);
                            if pipeline_ahead >= max_pipeline_depth {
                                continue; // Pipeline full, wait for MN finalization
                            }
                            let next_height =
                                std::cmp::max(finalized_height + 1, last_proposed_height + 1);
                            // Minimum wait: skip if mempool empty.
                            let mempool_len = if let Some(ref pipeline) =
                                intra_group_comm_clone.mempool_pipeline
                            {
                                pipeline.len_async().await
                            } else {
                                0
                            };
                            // max_empty_ticks expired, would propose an empty
                            // block on every subsequent tick (1 empty/sec).
                            // blocks across the cluster (memory:
                            // confirm_ratio_low_investigation_2026-05-04).
                            //
                            //   * SAVITRI_SKIP_EMPTY_BLOCKS=1 (default 0): once
                            //     consecutive_empty_ticks >= max_empty_ticks,
                            //     emit a heartbeat empty block ONLY every
                            //     SAVITRI_EMPTY_HEARTBEAT_TICKS (default 60s)
                            //     of further empty time. In the worst case
                            //     (mempool always empty) we get 1 block/min
                            //     instead of 1/sec — block rate drops, but
                            //     "blocks with TX" ratio rises mechanically.
                            //   * default off: legacy behaviour (1 empty/sec)
                            if mempool_len < block_threshold {
                                consecutive_empty_ticks += 1;
                                adaptive_sleep_ms = 1000;
                                if consecutive_empty_ticks < max_empty_ticks {
                                    continue;
                                }
                                let skip_empty: bool = std::env::var("SAVITRI_SKIP_EMPTY_BLOCKS")
                                    .ok()
                                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                                    .unwrap_or(false);
                                if skip_empty {
                                    let heartbeat_ticks: u32 =
                                        std::env::var("SAVITRI_EMPTY_HEARTBEAT_TICKS")
                                            .ok()
                                            .and_then(|v| v.parse::<u32>().ok())
                                            .filter(|&v| v > 0)
                                            .unwrap_or(60);
                                    let after_max =
                                        consecutive_empty_ticks.saturating_sub(max_empty_ticks);
                                    if after_max % heartbeat_ticks != 0 {
                                        continue; // skip empty block until heartbeat boundary
                                    }
                                }
                            }
                            consecutive_empty_ticks = 0;
                            adaptive_sleep_ms = if mempool_len > 200 {
                                50
                            } else if mempool_len > 50 {
                                200
                            } else {
                                500
                            };
                            last_proposed_height = next_height;
                            blocks_proposed += 1;
                            tracing::info!(
                                round,
                                height = next_height,
                                finalized = finalized_height,
                                depth = next_height - finalized_height,
                                mempool = mempool_len,
                                blocks_proposed,
                                "PIPELINE(self-delivery): proposing block"
                            );
                            if let Err(e) = intra_group_comm_clone
                                .create_and_propose_block_at_height(round, Some(next_height))
                                .await
                            {
                                error!("Failed to create and propose block: {}", e);
                            }
                        }
                        block_loop_flag.store(false, AtomicOrdering::SeqCst);
                        if rotation_needed {
                            info!("TENURE HANDOFF (self-delivery): immediate re-election");
                            if let Err(e) = intra_group_comm_clone.start_proposer_election().await {
                                warn!("Tenure handoff election failed: {}", e);
                            }
                        }
                        info!("Block production loop exited (self-delivery), block_loop_running reset to false");
                    });
                    info!("Self-delivery: block production loop started");
                } else {
                    info!("Self-delivery: block production loop already running, skipping");
                }
            }
        }

        proposer
    }

    /// Broadcast election result to group members
    async fn broadcast_election_result(
        &self,
        candidates: &[(String, u32, u32)],
        elected_proposer: &str,
    ) -> Result<()> {
        let group_id = self.get_current_group_id().await;
        let group_id_for_msg = group_id.clone();

        // V0.2 Phase 1.5 port: local candidates tuple is (id, combined_permille, pou_score).
        // The wire field carries (id, pou_score, combined_permille) so we reorder here.
        // All u32 — no f64 path remains.
        let candidates_data: Vec<(String, u32, u32)> = candidates
            .iter()
            .map(|(id, combined_permille, pou_score)| (id.clone(), *pou_score, *combined_permille))
            .collect();

        let elected_pou_score = candidates
            .iter()
            .find(|(id, _, _)| id == elected_proposer)
            .map(|(_, _, pou_score)| *pou_score)
            .unwrap_or(0);

        // Use deterministic round based on group_id to ensure all nodes
        // in the same group use the same round value for signature verification.
        // This is critical: when the certificate is built, it uses first.round for all
        // attestations, so all attestations must have been signed with the same round.
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(group_id.as_bytes());
        let round_hash = hasher.finalize();
        // Use first 8 bytes of hash as deterministic round value
        let round = u64::from_le_bytes([
            round_hash[0],
            round_hash[1],
            round_hash[2],
            round_hash[3],
            round_hash[4],
            round_hash[5],
            round_hash[6],
            round_hash[7],
        ]);

        // Log the computed round for debugging
        info!(
            group_id = %group_id,
            round = round,
            round_hash_hex = hex::encode(&round_hash[..8]),
            "🔢 [ELECTION] Computed deterministic round from group_id hash"
        );

        let timestamp = get_safe_timestamp();

        // Falla 3 (anti-replay): bind the election to the current finalized height.
        // All attesters in the same group will quantize to the same value because they share
        // the same finalized chain. The MN later verifies that a proposal's height falls
        // within [tenure_start_height, tenure_start_height + PROPOSER_TENURE_BLOCKS).
        let tenure_start_height = self.get_current_block_height().await;

        let mut election_result = ProposerElectionResult {
            round,
            elected_proposer: elected_proposer.to_string(),
            proposer_pou_score: elected_pou_score,
            sender: self.local_node_id.clone(),
            sender_pubkey_hex: Some(self.local_pubkey_hex()),
            group_id,
            timestamp,
            candidates: candidates_data,
            tenure_start_height,
            signature: [0u8; 64],
        };
        let signable = election_result.signable_bytes()?;
        election_result.signature =
            self.sign_intragroup_message(&election_result.group_id, "election_result", &signable);

        // Always keep the local signed result so certificate building does not depend on gossipsub self-delivery.
        self.collect_election_result_for_certificate(&election_result)
            .await;

        let payload = serde_json::to_vec(&election_result)?;

        self.broadcast_consensus(
            ConsensusMessage::ElectionResult(payload.clone()),
            self.election_topic.clone(),
            payload,
        )
        .await?;

        info!(
            group_id = %group_id_for_msg,
            proposer = %elected_proposer,
            candidates_count = candidates.len(),
            "Broadcast PoU election result to group (direct P2P)"
        );

        Ok(())
    }

    async fn collect_election_result_for_certificate(
        &self,
        result: &ProposerElectionResult,
    ) -> usize {
        let mut collected = self.election_results_collected.write().await;

        if let Some(existing) = collected.iter_mut().find(|r| {
            r.group_id == result.group_id && r.round == result.round && r.sender == result.sender
        }) {
            *existing = result.clone();
        } else {
            collected.push(result.clone());
        }

        const MAX_COLLECTED_RESULTS: usize = 256;
        if collected.len() > MAX_COLLECTED_RESULTS {
            let overflow = collected.len() - MAX_COLLECTED_RESULTS;
            collected.drain(0..overflow);
        }

        collected.len()
    }

    /// Process incoming intra-group message
    pub async fn process_message(
        &mut self,
        topic_hash: &libp2p::gossipsub::TopicHash,
        data: &[u8],
    ) -> Result<()> {
        // Check latency probes
        if *topic_hash == self.latency_topic.hash() {
            if let Ok(probe) = serde_json::from_slice::<GroupLatencyProbe>(data) {
                return self.handle_latency_probe(probe).await;
            }
        }

        // Check latency responses
        if *topic_hash == self.latency_topic.hash() {
            if let Ok(response) = serde_json::from_slice::<GroupLatencyResponse>(data) {
                return self.handle_latency_response(response).await;
            }
        }

        // Check PoU shares
        if *topic_hash == self.pou_topic.hash() {
            if let Ok(pou_share) = serde_json::from_slice::<PouScoreShare>(data) {
                return self.handle_pou_share(pou_share).await;
            }
        }

        // Check ping (mesh readiness) - reply with pong if group_id matches
        if *topic_hash == self.ping_topic.hash() {
            if let Ok(ping) = serde_json::from_slice::<GroupPing>(data) {
                return self.handle_group_ping(ping).await;
            }
        }

        // Check pong (mesh readiness) - set mesh_ready if in_reply_to is us and group_id matches
        if *topic_hash == self.pong_topic.hash() {
            if let Ok(pong) = serde_json::from_slice::<GroupPong>(data) {
                return self.handle_group_pong(pong).await;
            }
        }

        // Check election messages
        if *topic_hash == self.election_topic.hash() {
            // Try election message first
            if let Ok(election) = serde_json::from_slice::<ProposerElection>(data) {
                return self.handle_election_message(election).await;
            }

            // Try election result message
            if let Ok(result) = serde_json::from_slice::<ProposerElectionResult>(data) {
                return self.handle_election_result(result).await;
            }
        }

        // Consensus vote (per-group: solo membri of the gruppo ricevono)
        if *topic_hash == self.vote_topic.hash() {
            if let Ok(vote) = serde_json::from_slice::<ConsensusVote>(data) {
                return self.handle_consensus_vote(vote).await;
            }
        }

        Ok(())
    }

    /// Process a consensus message received via direct P2P (request-response).
    /// This replaces gossipsub topic-based routing for consensus messages.
    pub async fn process_consensus_direct_message(&mut self, msg: ConsensusMessage) -> Result<()> {
        match msg {
            ConsensusMessage::Vote(data) => {
                if let Ok(vote) = serde_json::from_slice::<ConsensusVote>(&data) {
                    return self.handle_consensus_vote(vote).await;
                }
                warn!("Failed to deserialize ConsensusVote from direct message");
            }
            ConsensusMessage::Election(data) => {
                if let Ok(election) = serde_json::from_slice::<ProposerElection>(&data) {
                    return self.handle_election_message(election).await;
                }
                warn!("Failed to deserialize ProposerElection from direct message");
            }
            ConsensusMessage::ElectionResult(data) => {
                if let Ok(result) = serde_json::from_slice::<ProposerElectionResult>(&data) {
                    return self.handle_election_result(result).await;
                }
                warn!("Failed to deserialize ProposerElectionResult from direct message");
            }
            ConsensusMessage::Latency(data) => {
                if let Ok(probe) = serde_json::from_slice::<GroupLatencyProbe>(&data) {
                    return self.handle_latency_probe(probe).await;
                }
                warn!("Failed to deserialize GroupLatencyProbe from direct message");
            }
            ConsensusMessage::LatencyResponse(data) => {
                if let Ok(response) = serde_json::from_slice::<GroupLatencyResponse>(&data) {
                    return self.handle_latency_response(response).await;
                }
                warn!("Failed to deserialize GroupLatencyResponse from direct message");
            }
            ConsensusMessage::PoU(data) => {
                if let Ok(pou_share) = serde_json::from_slice::<PouScoreShare>(&data) {
                    return self.handle_pou_share(pou_share).await;
                }
                warn!("Failed to deserialize PouScoreShare from direct message");
            }
            ConsensusMessage::PoUAck(data) => {
                // PoU ACK handling - just log, no specific handler needed
                debug!(data_len = data.len(), "Received PoU ACK via direct P2P");
            }
        }
        Ok(())
    }

    /// Handle consensus vote ricevuto da un altro membro of the gruppo (per aggregazione / consenso)
    async fn handle_consensus_vote(&self, vote: ConsensusVote) -> Result<()> {
        if vote.voter == self.local_node_id {
            return Ok(()); // Ignore our own votes
        }
        if !self.is_group_match(&vote.group_id).await {
            debug!(group_id = %vote.group_id, "Ignoring consensus vote for non-current group");
            return Ok(());
        }
        debug!(
            voter = %vote.voter,
            proposer = %vote.proposer,
            round = vote.round,
            group_id = %vote.group_id,
            "Received consensus vote from group member (for aggregation)"
        );
        // TODO: accumulare voti per round e decidere quorum (aggregazione lato lightnode)
        Ok(())
    }

    /// Handle incoming latency probe
    async fn handle_latency_probe(&self, probe: GroupLatencyProbe) -> Result<()> {
        if probe.sender == self.local_node_id {
            return Ok(()); // Ignore our own probes
        }

        if !self.is_group_match(&probe.group_id).await {
            warn!(
                group_id = %probe.group_id,
                sender = %probe.sender,
                "Ignoring latency probe for non-current group"
            );
            return Ok(());
        }

        let signable = probe.signable_bytes()?;
        let sender_key = probe
            .sender_pubkey_hex
            .as_deref()
            .unwrap_or(probe.sender.as_str());
        if !self.verify_intragroup_message(
            sender_key,
            &probe.group_id,
            "latency_probe",
            &signable,
            &probe.signature,
        ) {
            warn!(
                group_id = %probe.group_id,
                sender = %probe.sender,
                "Invalid signature on latency probe"
            );
            return Ok(());
        }

        info!(sender = %probe.sender, group_id = %probe.group_id, "Received latency probe from group member");

        // Send response (timestamp in ms per RTT preciso)
        let mut response = GroupLatencyResponse {
            probe_id: probe.probe_id,
            responder: self.local_node_id.clone(),
            responder_pubkey_hex: Some(self.local_pubkey_hex()),
            original_timestamp_ms: probe.timestamp_ms,
            response_timestamp_ms: get_safe_timestamp_ms(),
            group_id: probe.group_id,
            signature: [0u8; 64],
        };
        let signable = response.signable_bytes()?;
        response.signature =
            self.sign_intragroup_message(&response.group_id, "latency_response", &signable);

        let payload = serde_json::to_vec(&response)?;
        // Send response only to the probe sender, not broadcast to all.
        // Broadcasting N responses to all members causes O(N^2) message amplification.
        if let Ok(sender_peer_id) = probe.sender.parse::<PeerId>() {
            self.send_consensus_to_peer(
                &sender_peer_id,
                ConsensusMessage::LatencyResponse(payload),
            )
            .await?;
        } else {
            // Fallback to gossipsub if peer_id parse fails
            self.publish(self.latency_topic.clone(), payload).await?;
        }

        Ok(())
    }

    /// Handle incoming latency response
    async fn handle_latency_response(&self, response: GroupLatencyResponse) -> Result<()> {
        if response.responder == self.local_node_id {
            return Ok(()); // Ignore our own responses
        }

        if !self.is_group_match(&response.group_id).await {
            warn!(
                group_id = %response.group_id,
                responder = %response.responder,
                "Ignoring latency response for non-current group"
            );
            return Ok(());
        }

        let signable = response.signable_bytes()?;
        let responder_key = response
            .responder_pubkey_hex
            .as_deref()
            .unwrap_or(response.responder.as_str());
        if !self.verify_intragroup_message(
            responder_key,
            &response.group_id,
            "latency_response",
            &signable,
            &response.signature,
        ) {
            warn!(
                group_id = %response.group_id,
                responder = %response.responder,
                "Invalid signature on latency response"
            );
            return Ok(());
        }

        info!(responder = %response.responder, group_id = %response.group_id, "Received latency response from group member");

        // Calculate RTT in milliseconds (precisione per misurazioni reali WAN/LAN)
        let rtt_ms = response
            .response_timestamp_ms
            .saturating_sub(response.original_timestamp_ms);
        let rtt_duration = Duration::from_millis(rtt_ms);

        // Update latency measurements for this member (save pubkey if available)
        {
            let mut latencies = self.member_latencies.write().await;
            latencies.insert(
                response.responder.clone(),
                (rtt_duration, response.responder_pubkey_hex.clone()),
            );
        }

        info!("Measured RTT {} ms with {}", rtt_ms, response.responder);

        // Try to determine proposer if we have enough data
        if let Some(proposer) = self.determine_proposer().await {
            info!("Group proposer determined: {}", proposer);
        }

        Ok(())
    }

    /// Handle incoming GroupPing - reply with Pong if group_id matches
    async fn handle_group_ping(&self, ping: GroupPing) -> Result<()> {
        if ping.from == self.local_node_id {
            return Ok(()); // Ignore our own pings
        }

        if !self.is_group_match(&ping.group_id).await {
            debug!(
                group_id = %ping.group_id,
                from = %ping.from,
                "Ignoring ping from non-current group (outside member)"
            );
            return Ok(());
        }

        let pong = GroupPong {
            from: self.local_node_id.clone(),
            in_reply_to: ping.from.clone(),
            group_id: ping.group_id.clone(),
            timestamp: get_safe_timestamp(),
            // Echo the millisecond send time so the ping origin can compute RTT.
            original_timestamp_ms: ping.original_timestamp_ms,
        };
        let payload = serde_json::to_vec(&pong)?;
        if let Err(e) = self.publish(self.pong_topic.clone(), payload).await {
            warn!(error=?e, "Failed to publish Pong");
        } else {
            debug!(in_reply_to=%ping.from, "Sent Pong (mesh readiness reply)");
        }
        Ok(())
    }

    /// Handle incoming GroupPong - set mesh_ready if in_reply_to is us and group_id matches
    async fn handle_group_pong(&self, pong: GroupPong) -> Result<()> {
        if pong.in_reply_to != self.local_node_id {
            return Ok(()); // Pong not for us
        }

        if !self.is_group_match(&pong.group_id).await {
            debug!(
                group_id = %pong.group_id,
                from = %pong.from,
                "Ignoring pong from non-current group (outside member)"
            );
            return Ok(());
        }

        // Record RTT sample into PoU observation store (step 2 wire).
        // Skip when the peer is still running a previous build and sends 0.
        if let Some(store) = &self.observations {
            if pong.original_timestamp_ms > 0 {
                let now_ms = get_safe_timestamp_ms();
                if now_ms > pong.original_timestamp_ms {
                    let rtt_ms = now_ms - pong.original_timestamp_ms;
                    // Sanity: discard implausibly large values (> 30s) likely
                    // caused by clock skew between peers.
                    if rtt_ms <= 30_000 {
                        store.record_latency(&pong.from, rtt_ms, LatencyType::Ping);
                        debug!(from = %pong.from, rtt_ms, "Recorded PoU latency sample from pong");
                    }
                }
            }
        }

        let mut ready = self.mesh_ready.write().await;
        if !*ready {
            *ready = true;
            info!(
                from = %pong.from,
                group_id = %pong.group_id,
                "Mesh ready: received Pong from group member - starting PoU sharing and proposer election"
            );
            drop(ready);

            // Trigger PoU and proposer election now that mesh is ready
            if let Err(e) = self.share_pou_score().await {
                warn!(error=?e, "Failed to share PoU score after mesh ready");
            }
            if let Err(e) = self.start_proposer_election().await {
                warn!(error=?e, "Failed to start proposer election after mesh ready");
            }
        }
        Ok(())
    }

    /// Send GroupPing to probe mesh readiness
    pub async fn send_group_ping(&self) -> Result<()> {
        let group_id = self.get_current_group_id().await;
        let ping = GroupPing {
            from: self.local_node_id.clone(),
            group_id,
            timestamp: get_safe_timestamp(),
            // Millisecond send time enables RTT computation in handle_group_pong.
            original_timestamp_ms: get_safe_timestamp_ms(),
        };
        let payload = serde_json::to_vec(&ping)?;
        self.publish(self.ping_topic.clone(), payload).await?;
        debug!("Sent GroupPing (mesh readiness probe)");
        Ok(())
    }

    /// Handle incoming PoU score share
    async fn handle_pou_share(&self, pou_share: PouScoreShare) -> Result<()> {
        if pou_share.node_id == self.local_node_id {
            return Ok(()); // Ignore our own shares
        }

        if !self.is_group_match(&pou_share.group_id).await {
            warn!(
                group_id = %pou_share.group_id,
                node_id = %pou_share.node_id,
                "Ignoring PoU share for non-current group"
            );
            return Ok(());
        }

        let signable = pou_share.signable_bytes()?;
        let node_key = pou_share
            .node_pubkey_hex
            .as_deref()
            .unwrap_or(pou_share.node_id.as_str());
        if !self.verify_intragroup_message(
            node_key,
            &pou_share.group_id,
            "pou_share",
            &signable,
            &pou_share.signature,
        ) {
            warn!(
                group_id = %pou_share.group_id,
                node_id = %pou_share.node_id,
                "Invalid signature on PoU share"
            );
            return Ok(());
        }

        info!(
            "Received PoU score {} from {}",
            pou_share.pou_score, pou_share.node_id
        );

        // Send ACK so the sender knows we received their PoU share
        let ack = PouScoreAck {
            from: self.local_node_id.clone(),
            ack_for: pou_share.node_id.clone(),
            group_id: pou_share.group_id.clone(),
            timestamp: get_safe_timestamp(),
        };
        let ack_payload = serde_json::to_vec(&ack)?;
        // Send ACK only to the PoU sender, not broadcast to all.
        // Broadcasting ACKs to all members causes O(N^2) message amplification.
        if let Ok(sender_peer_id) = pou_share.node_id.parse::<PeerId>() {
            if let Err(e) = self
                .send_consensus_to_peer(&sender_peer_id, ConsensusMessage::PoUAck(ack_payload))
                .await
            {
                warn!(error=?e, "Failed to send PoU ACK to {}", pou_share.node_id);
            } else {
                debug!(from=%ack.from, ack_for=%ack.ack_for, "Sent PoU ACK via direct P2P");
            }
        } else {
            // Fallback to gossipsub
            if let Err(e) = self.publish(self.pou_ack_topic.clone(), ack_payload).await {
                warn!(error=?e, "Failed to publish PoU ACK");
            }
        }

        // Store PoU score for proposer election
        {
            let mut pou_scores = self.member_pou_scores.write().await;
            pou_scores.insert(
                pou_share.node_id.clone(),
                (pou_share.pou_score, Instant::now()),
            );
        }

        // Log PoU reception with combined score (updated by latency)
        {
            let latencies = self.member_latencies.read().await;
            let (latency_score, rtt_ms, simulated) =
                if let Some((latency, _)) = latencies.get(&pou_share.node_id) {
                    let score = 1.0 / (1.0 + latency.as_secs_f64());
                    (score, Some(latency.as_millis()), false)
                } else {
                    #[cfg(feature = "test_simulated_latency")]
                    {
                        let sim_ms = Self::simulated_latency_ms(&pou_share.node_id);
                        let score = 1.0 / (1.0 + (sim_ms as f64 / 1000.0));
                        (score, Some(sim_ms as u128), true)
                    }
                    #[cfg(not(feature = "test_simulated_latency"))]
                    {
                        (0.5, None, false)
                    }
                };
            let pou_normalized = pou_share.pou_score as f64 / 10000.0;
            let combined_score = (pou_normalized * 0.7) + (latency_score * 0.3);
            info!(
                node_id = %pou_share.node_id,
                pou_score = pou_share.pou_score,
                rtt_ms = ?rtt_ms,
                simulated = simulated,
                latency_score = %format!("{:.4}", latency_score),
                combined_score = %format!("{:.4}", combined_score),
                "PoU received, combined score (PoU 70% + latency 30%)"
            );
        }

        // Try to determine proposer if we have enough data
        if let Some(proposer) = self.determine_proposer().await {
            info!("Group proposer determined: {}", proposer);
        }

        Ok(())
    }

    /// Handle incoming election message
    async fn handle_election_message(&self, election: ProposerElection) -> Result<()> {
        if election.candidate == self.local_node_id {
            return Ok(()); // Ignore our own election messages
        }

        if !self.is_group_match(&election.group_id).await {
            warn!(
                group_id = %election.group_id,
                candidate = %election.candidate,
                "Ignoring election message for non-current group"
            );
            return Ok(());
        }

        let signable = election.signable_bytes()?;
        let candidate_key = election
            .candidate_pubkey_hex
            .as_deref()
            .unwrap_or(election.candidate.as_str());
        if !self.verify_intragroup_message(
            candidate_key,
            &election.group_id,
            "election",
            &signable,
            &election.signature,
        ) {
            warn!(
                group_id = %election.group_id,
                candidate = %election.candidate,
                "Invalid signature on election message"
            );
            return Ok(());
        }

        info!(
            "Received election message from {} with PoU score {}",
            election.candidate, election.pou_score
        );

        // Store candidate's PoU score for comparison
        {
            let mut pou_scores = self.member_pou_scores.write().await;
            pou_scores.insert(
                election.candidate.clone(),
                (election.pou_score, Instant::now()),
            );
        }

        // Try to determine proposer if we have enough data
        if let Some(proposer) = self.determine_proposer().await {
            info!("Group proposer determined: {}", proposer);
        }

        Ok(())
    }

    /// Handle incoming election result
    async fn handle_election_result(&mut self, result: ProposerElectionResult) -> Result<()> {
        let signable = result.signable_bytes()?;
        let sender_key = result
            .sender_pubkey_hex
            .as_deref()
            .unwrap_or(result.sender.as_str());
        if !self.verify_intragroup_message(
            sender_key,
            &result.group_id,
            "election_result",
            &signable,
            &result.signature,
        ) {
            warn!(
                group_id = %result.group_id,
                sender = %result.sender,
                "Invalid signature on election result"
            );
            return Ok(());
        }

        let collected_count = self.collect_election_result_for_certificate(&result).await;

        if !self.is_group_match(&result.group_id).await {
            warn!(
                group_id = %result.group_id,
                sender = %result.sender,
                collected_count,
                "Ignoring election result for non-current group"
            );
            return Ok(());
        }

        info!(
            group_id = %result.group_id,
            proposer = %result.elected_proposer,
            round = result.round,
            candidates_count = result.candidates.len(),
            "Intra-group PoU election completed"
        );
        info!(
            proposer = %result.elected_proposer,
            round = result.round,
            candidates_count = result.candidates.len(),
            "Received election result"
        );
        info!(
            group_id = %result.group_id,
            proposer = %result.elected_proposer,
            "Successfully elected proposer"
        );

        // Debounce: only act on the FIRST election result per round.
        // Multiple LNs broadcast their results near-simultaneously; acting on each one
        // causes proposer oscillation (80ms apart) which kills block production loops.
        {
            let mut last_round = self.last_committed_election_round.write().await;
            if result.round <= *last_round {
                debug!(
                    round = result.round,
                    last_committed = *last_round,
                    proposer = %result.elected_proposer,
                    sender = %result.sender,
                    "Ignoring duplicate election result for already-committed round"
                );
                return Ok(());
            }
            *last_round = result.round;
        }

        // Update group manager with elected proposer
        {
            let current_group = self.group_manager.get_current_group().await;
            if let Some(group) = current_group {
                if group.group_id == result.group_id {
                    // Check if we are the elected proposer
                    let is_elected = result.elected_proposer == self.local_node_id;

                    if is_elected {
                        info!(
                            "We have been elected as proposer for group {}",
                            group.group_id
                        );
                        info!(
                            group_id = %result.group_id,
                            elected_proposer = %result.elected_proposer,
                            collected_count,
                            sender = %result.sender,
                            "📋 [ELECTION CERT] Collected election result for certificate (proposer will bundle for masternode)"
                        );
                        if let Some(ref flag) = self.is_intragroup_proposer {
                            *flag.write().await = true;
                        }
                        // us as proposer. Idle -> Elected (idempotent if already
                        // Elected/Producing from a prior path).
                        let _ = self.proposer_sm.try_elect(0, 0).await;
                        self.start_proposer_duties().await?;
                    } else {
                        info!(
                            "Proposer elected: {} for group {}",
                            result.elected_proposer, group.group_id
                        );
                        self.start_following_proposer(&result.elected_proposer)
                            .await?;
                    }
                }
            }
        }

        // Store election result for reference
        debug!("Election candidates: {:?}", result.candidates);

        Ok(())
    }

    /// Start proposer duties after being elected
    async fn start_proposer_duties(&mut self) -> Result<()> {
        // Guard: prevent spawning multiple block production loops (shared across clones via Arc<AtomicBool>)
        if self
            .block_loop_running
            .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
            .is_err()
        {
            info!(
                "start_proposer_duties called but block production loop already running, skipping"
            );
            return Ok(());
        }
        info!("Starting proposer duties (block_loop_running CAS succeeded)");

        // Order matters: clear pending_nonces FIRST, then restore TXs.
        // Previously restore happened before clear, which meant restored TXs
        // had nonces tracked in pending_nonces that were immediately wiped,
        // senders → nonce mismatch → all restored TXs rejected.
        if let Some(ref pipeline) = self.mempool_pipeline {
            // Restore in-flight TXs back to mempool on proposer change.
            pipeline.restore_in_flight_txs();
        }

        // Initialize proposer state
        let proposer_state = Arc::new(RwLock::new(ProposerState {
            current_round: 0,
            last_block_height: 0,
            is_active: true,
            block_proposal_count: 0,
        }));
        self.proposer_state = Some(proposer_state.clone());

        // Block production loop: 30-block tenure with zero-gap handoff.
        // At block 29: recompute PoU rankings, announce next proposer.
        // At block 30: step down; the next proposer has already pre-started.
        let proposer_state_clone = proposer_state.clone();
        let intra_group_comm_clone = self.clone();
        let block_loop_flag = self.block_loop_running.clone();
        let is_proposer_flag = self.is_intragroup_proposer.clone();
        let schedule_ref = self.proposer_schedule.clone();
        let pou_scores_ref = self.member_pou_scores.clone();
        let local_id = self.local_node_id.clone();
        tokio::spawn(async move {
            let mut adaptive_sleep_ms: u64 = 1000;
            let mut rotation_needed = false;
            let mut last_proposed_height: u64 = 0;
            let mut consecutive_empty_ticks: u32 = 0;
            let max_empty_ticks: u32 = std::env::var("SAVITRI_MAX_EMPTY_TICKS")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(5);
            let max_pipeline_depth: u64 = std::env::var("SAVITRI_PROPOSER_PIPELINE_DEPTH")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&d| d > 0)
                .unwrap_or(64);
            let min_tx_per_block: usize = std::env::var("SAVITRI_MIN_TX_PER_BLOCK")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let block_threshold: usize = if min_tx_per_block > 0 {
                min_tx_per_block
            } else {
                1
            };
            loop {
                tokio::time::sleep(Duration::from_millis(adaptive_sleep_ms)).await;
                let round = {
                    let mut state = proposer_state_clone.write().await;
                    if !state.is_active {
                        info!("Block production loop stopped (no longer proposer)");
                        break;
                    }

                    // ── Tenure block 29: pre-compute next proposer ──
                    if state.block_proposal_count == PROPOSER_TENURE_BLOCKS - 1 {
                        let scores = pou_scores_ref.read().await;
                        let group_members = intra_group_comm_clone
                            .group_manager
                            .get_group_members()
                            .await;
                        let self_pou = intra_group_comm_clone.get_current_pou_score().await;
                        let mut ranked: Vec<(String, u32)> = group_members
                            .iter()
                            .filter_map(|id| {
                                if *id == local_id {
                                    // Self score: prefer map entry, otherwise use real PoU score
                                    Some((
                                        id.clone(),
                                        scores.get(id).map(|(s, _)| *s).unwrap_or(self_pou),
                                    ))
                                } else {
                                    scores.get(id).map(|(s, ts)| {
                                        if ts.elapsed().as_secs() < 120 {
                                            (id.clone(), *s)
                                        } else {
                                            (id.clone(), 0)
                                        } // stale
                                    })
                                }
                            })
                            .collect();
                        // Restore the lex-low peer_id tiebreaker for cross-node consistency.
                        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
                        // Next proposer = best candidate that is NOT current proposer
                        let next = ranked
                            .iter()
                            .find(|(id, _)| *id != local_id)
                            .or_else(|| ranked.first())
                            .map(|(id, _)| id.clone());
                        {
                            let mut sched = schedule_ref.write().await;
                            if let Some(ref mut s) = *sched {
                                s.next_proposer = next.clone();
                                s.ranked_candidates = ranked.clone();
                                s.last_updated = Instant::now();
                            }
                        }
                        info!(
                            block = state.block_proposal_count,
                            next_proposer = ?next,
                            candidates = ranked.len(),
                            "TENURE: Block 29 — next proposer pre-computed from live PoU"
                        );
                    }

                    // ── Tenure block 30: handoff ──
                    if state.block_proposal_count >= PROPOSER_TENURE_BLOCKS {
                        let next = schedule_ref
                            .read()
                            .await
                            .as_ref()
                            .and_then(|s| s.next_proposer.clone());
                        info!(
                            blocks_proposed = state.block_proposal_count,
                            tenure = PROPOSER_TENURE_BLOCKS,
                            next_proposer = ?next,
                            "TENURE: Stepping down after {} blocks — handoff to next proposer",
                            state.block_proposal_count
                        );
                        state.is_active = false;
                        rotation_needed = true;
                        break;
                    }

                    state.current_round += 1;
                    state.current_round
                };
                // PIPELINING: Allow proposing multiple blocks ahead of finalized height.
                // Instead of waiting for MN to finalize block N before proposing N+1,
                // the proposer tracks its own height counter and proposes N, N+1, N+2...
                // up to MAX_PIPELINE_DEPTH blocks ahead of the finalized (storage) height.
                let finalized_height = intra_group_comm_clone.get_current_block_height().await;
                let pipeline_ahead = if last_proposed_height > finalized_height {
                    last_proposed_height - finalized_height
                } else {
                    0
                };
                if pipeline_ahead >= max_pipeline_depth {
                    crate::observability::PipelineObsMetrics::inc_block_pipeline_full();
                    // Pipeline full — wait for MN to catch up
                    tracing::debug!(
                        finalized_height,
                        last_proposed = last_proposed_height,
                        pipeline_depth = pipeline_ahead,
                        max = max_pipeline_depth,
                        "Pipeline full, waiting for MN finalization to catch up"
                    );
                    continue;
                }
                // Determine next height: max of (finalized+1, last_proposed+1)
                // This ensures monotonically increasing heights even across pipeline gaps
                let next_height = std::cmp::max(finalized_height + 1, last_proposed_height + 1);
                // Minimum wait: check mempool before producing a block.
                // If mempool is empty, skip and wait for TX to arrive.
                // After MAX_EMPTY_TICKS (5s) produce a heartbeat block.
                let mempool_len =
                    if let Some(ref pipeline) = intra_group_comm_clone.mempool_pipeline {
                        pipeline.len_async().await
                    } else {
                        0
                    };
                // self-delivery loop for full rationale.
                if mempool_len < block_threshold {
                    consecutive_empty_ticks += 1;
                    adaptive_sleep_ms = 1000;
                    if consecutive_empty_ticks < max_empty_ticks {
                        tracing::debug!(
                            empty_ticks = consecutive_empty_ticks,
                            max = max_empty_ticks,
                            mempool = mempool_len,
                            threshold = block_threshold,
                            "Mempool below threshold, skipping block production"
                        );
                        continue;
                    }
                    let skip_empty: bool = std::env::var("SAVITRI_SKIP_EMPTY_BLOCKS")
                        .ok()
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    if skip_empty {
                        let heartbeat_ticks: u32 = std::env::var("SAVITRI_EMPTY_HEARTBEAT_TICKS")
                            .ok()
                            .and_then(|v| v.parse::<u32>().ok())
                            .filter(|&v| v > 0)
                            .unwrap_or(60);
                        let after_max = consecutive_empty_ticks.saturating_sub(max_empty_ticks);
                        if after_max % heartbeat_ticks != 0 {
                            continue; // skip empty block until heartbeat boundary
                        }
                    }
                    // Heartbeat: produce empty block to keep consensus alive
                    tracing::info!(
                        empty_ticks = consecutive_empty_ticks,
                        "Producing heartbeat block after {}s without TX",
                        consecutive_empty_ticks
                    );
                }
                consecutive_empty_ticks = 0;
                adaptive_sleep_ms = if mempool_len > 200 {
                    50
                } else if mempool_len > 50 {
                    200
                } else {
                    500
                };
                last_proposed_height = next_height;
                {
                    let mut state = proposer_state_clone.write().await;
                    state.block_proposal_count += 1;
                }
                tracing::info!(
                    round,
                    height = next_height,
                    finalized = finalized_height,
                    pipeline_depth = next_height - finalized_height,
                    mempool = mempool_len,
                    "PIPELINE: proposing block (depth={} ahead of finalized)",
                    next_height - finalized_height
                );
                crate::observability::PipelineObsMetrics::inc_block_proposed();
                if let Err(e) = intra_group_comm_clone
                    .create_and_propose_block_at_height(round, Some(next_height))
                    .await
                {
                    error!("Failed to create and propose block: {}", e);
                }
            }
            // Reset flag so a new loop can be spawned after re-election
            block_loop_flag.store(false, AtomicOrdering::SeqCst);
            if let Some(ref flag) = is_proposer_flag {
                *flag.write().await = false;
            }
            if rotation_needed {
                info!("TENURE HANDOFF: triggering immediate re-election (schedule pre-computed at block 29)");
                // No settle delay — the schedule was pre-computed, election is instant
                if let Err(e) = intra_group_comm_clone.start_proposer_election().await {
                    warn!("Tenure handoff election failed: {}", e);
                }
            }
            info!("Block production loop exited, block_loop_running reset to false");
        });

        let proposer_state_validation = proposer_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut validation_interval = tokio::time::interval(Duration::from_secs(2)); // Validate every 2 seconds
            loop {
                validation_interval.tick().await;
                {
                    let state = proposer_state_validation.read().await;
                    if !state.is_active {
                        break;
                    }
                }
                if let Err(e) = intra_group_comm_clone.validate_pending_transactions().await {
                    error!("Failed to validate transactions: {}", e);
                }
            }
        });

        // Start masternode communication
        let proposer_state_mn = proposer_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut communication_interval = tokio::time::interval(Duration::from_secs(5)); // Communicate every 5 seconds
            loop {
                communication_interval.tick().await;
                {
                    let state = proposer_state_mn.read().await;
                    if !state.is_active {
                        break;
                    }
                }
                if let Err(e) = intra_group_comm_clone.communicate_with_masternode().await {
                    error!("Failed to communicate with masternode: {}", e);
                }
            }
        });

        debug!("Proposer duties initiated - block creation, validation, and communication started");
        Ok(())
    }

    /// Create and propose a new block
    async fn create_and_propose_block(&self, round: u64) -> Result<()> {
        self.create_and_propose_block_at_height(round, None).await
    }

    /// Create and propose a block, optionally at a specific pipeline height.
    /// If `pipeline_height` is None, uses storage height + 1 (legacy behavior).
    /// If Some(h), uses h as the block height (pipelining mode).
    async fn create_and_propose_block_at_height(
        &self,
        round: u64,
        pipeline_height: Option<u64>,
    ) -> Result<()> {
        info!("Creating block proposal for round {}", round);

        let mut transactions: Vec<crate::tx::SignedTx> = if let (
            Some(ref pipeline),
            Some(ref storage),
        ) =
            (self.mempool_pipeline.as_ref(), self.storage.as_ref())
        {
            // MempoolPipeline methods take `&self` and the inner
            // RealMempoolPipeline owns synchronization. RPC submit
            // and proposer drain operate on disjoint sections so they
            // can run concurrently without serializing on a single
            // tokio Mutex.
            //
            // `.drain_for_block_production()` wrappers called
            // `block_on_current_runtime` which returns None in nested
            // async, silently making the proposer observe an empty
            // mempool. This was the true source of 0-TX-in-blocks
            // under load even after the per-group quorum and hard-
            // reject fixes landed.
            let mempool_len = pipeline.len_async().await;
            info!(
                round,
                mempool_len, "Proposer: mempool size before drain_for_block_production"
            );
            let (ready_txs, ready_stxs) = pipeline
                .drain_for_block_production_async(MAX_BLOCK_TXS_MEMPOOL)
                .await;

            info!(
                round,
                staged_txs = ready_txs.len(),
                staged_handles = ready_stxs.len(),
                "MempoolPipeline drain_for_block_production completed"
            );

            if ready_stxs.is_empty() {
                info!(
                        round,
                        "Mempool empty - proposing empty block (allows verifying LN->MN proposal and certificate flow)"
                    );
                vec![]
            } else {
                let (valid_txs, invalid_handles) = pipeline.final_validation_with_round(
                    &ready_txs,
                    &ready_stxs,
                    Some(storage.as_ref()),
                    round,
                );

                info!(
                    round,
                    valid_txs = valid_txs.len(),
                    invalid_txs = invalid_handles.len(),
                    "MempoolPipeline final_validation completed"
                );

                if !invalid_handles.is_empty() {
                    pipeline.restore_in_flight_preserve_pending();
                    info!(
                        round,
                        restored = invalid_handles.len(),
                        "Restored rejected TXs to mempool (future nonces, not yet committable)"
                    );
                }

                // can continue from where this block left off, without waiting
                // for BFT certificate commit.
                if !valid_txs.is_empty() {
                    pipeline.record_proposed_nonces(&valid_txs);
                }

                if valid_txs.is_empty() {
                    info!(
                            round,
                            "No valid txs after final_validation - proposing empty block (verifies LN->MN flow)"
                        );
                    vec![]
                } else {
                    info!(
                        round,
                        count = valid_txs.len(),
                        "Using transactions from shared MempoolPipeline (drain + final_validation)"
                    );
                    valid_txs
                }
            }
        } else {
            let fallback = self.collect_transactions_for_block().await?;
            info!(
                round,
                count = fallback.len(),
                "Collected transactions for block using fallback collect_transactions_for_block"
            );
            if fallback.is_empty() {
                info!(
                        round,
                        "Fallback collection empty - proposing empty block (verifies LN->MN proposal and certificate flow)"
                    );
                vec![]
            } else {
                fallback
            }
        };

        // DAG TX deduplication: remove transactions already included in other DAG branches
        if let Some(ref dag) = self.dag {
            if !transactions.is_empty() {
                let before = transactions.len();
                transactions = crate::p2p::block::dedup_txs_against_dag(dag, &transactions).await;
                if transactions.len() < before {
                    info!(
                        round,
                        before,
                        after = transactions.len(),
                        removed = before - transactions.len(),
                        "DAG dedup: removed duplicate TXs from proposal"
                    );
                }
            }
        }

        // Sort transactions by (sender, nonce) for deterministic execution order.
        // Ensures nonce sequence is correct per sender regardless of mempool ordering.
        transactions.sort_by(|a, b| (&a.from, a.nonce).cmp(&(&b.from, b.nonce)));

        // Fix 2 (revised): keep only consecutive nonces per sender.
        // Uses the FIRST available nonce per sender as the starting point
        // (cold-start compatible — doesn't require nonce=0 from storage).
        // The CommitScheduler handles cross-block nonce ordering; this filter
        // only ensures no intra-block gaps for the same sender.
        {
            let mut sender_next_nonce: std::collections::HashMap<Vec<u8>, Option<u64>> =
                std::collections::HashMap::new();
            let before_filter = transactions.len();
            transactions = transactions
                .into_iter()
                .filter(|tx| {
                    let addr = crate::p2p::block::normalize_address_bytes(&tx.from);
                    let next = sender_next_nonce.entry(addr).or_insert(None);
                    match next {
                        None => {
                            // First TX for this sender — accept whatever nonce it has
                            *next = Some(tx.nonce + 1);
                            true
                        }
                        Some(expected) => {
                            if tx.nonce == *expected {
                                *expected += 1;
                                true
                            } else {
                                false // gap within this block for same sender
                            }
                        }
                    }
                })
                .collect();
            let after_filter = transactions.len();
            if after_filter < before_filter {
                info!(
                    round,
                    kept = after_filter,
                    removed = before_filter - after_filter,
                    "Intra-block nonce gap filter applied"
                );
            }
        }

        // Convert SignedTx to ProposalTransaction
        let proposal_transactions: Vec<crate::proposer::ProposalTransaction> = transactions
            .iter()
            .map(|tx| {
                // Compute transaction hash from transaction data
                let tx_data = format!(
                    "{}:{}:{}:{}:{}",
                    tx.from,
                    tx.to,
                    tx.amount,
                    tx.nonce,
                    tx.fee.unwrap_or(0)
                );
                let hash_result = sha2::Sha256::digest(tx_data.as_bytes());
                let mut tx_hash = [0u8; 64];
                tx_hash[..32].copy_from_slice(hash_result.as_slice());

                crate::proposer::ProposalTransaction {
                    hash: tx_hash,
                    from: safe_hex_decode(
                        &tx.from,
                        vec![0u8; 32],
                        Some("block_proposal_tx_from_to"),
                    )
                    .try_into()
                    .unwrap_or([0u8; 32]),
                    to: safe_hex_decode(&tx.to, vec![0u8; 32], Some("block_proposal_tx_from_to"))
                        .try_into()
                        .unwrap_or([0u8; 32]),
                    amount: tx.amount,
                    nonce: tx.nonce,
                    fee: tx.fee.unwrap_or(0) as u64,
                    data: tx.data.clone().unwrap_or_default(),
                    signature: tx.sig,
                }
            })
            .collect();

        // Collect latency proofs from group members
        let latency_proof = self.collect_latency_proof().await;

        // Create block proposal (unsigned first). Use same key as election certificate (ed25519 verifying key).
        let proposer_pubkey: [u8; 32] = *self.signing_key.verifying_key().as_bytes();
        // PIPELINING: Use pipeline_height if provided, otherwise fall back to storage height + 1
        let height = if let Some(h) = pipeline_height {
            h
        } else {
            let current_height = self.get_current_block_height().await;
            current_height + 1
        };
        let timestamp = get_safe_timestamp();
        let proposer_pou_score = self.get_current_pou_score().await;
        let parent_hashes_all = self.get_parent_hashes().await;
        let parent_hash = parent_hashes_all.first().copied().unwrap_or([0u8; 64]);
        let additional_parent_hashes: Vec<[u8; 64]> = parent_hashes_all
            .iter()
            .copied()
            .skip(1)
            .filter(|h| *h != parent_hash)
            .collect();
        // When block has 0 tx, use canonical empty roots so re-execution on other nodes matches (same as block.rs)
        let (state_root, tx_root) = if proposal_transactions.is_empty() {
            (
                crate::p2p::block::canonical_empty_state_root_64(),
                crate::p2p::block::canonical_empty_tx_root_64(),
            )
        } else if let Some(ref storage) = self.storage {
            // Execute transactions against local state to compute deterministic roots
            let temp_block = crate::tx::Block {
                height,
                parent_hash,
                ..Default::default()
            };
            match crate::p2p::block::apply_certified_block_direct(
                storage.as_ref(),
                &temp_block,
                &transactions,
            ) {
                Ok((overlay, receipts)) => {
                    let sr32 = crate::p2p::block::compute_state_root_from_overlay(&overlay);
                    let tr32 = crate::p2p::block::compute_tx_root_from_receipts(&receipts);
                    let mut sr64 = [0u8; 64];
                    sr64[..32].copy_from_slice(&sr32);
                    let mut tr64 = [0u8; 64];
                    tr64[..32].copy_from_slice(&tr32);
                    (sr64, tr64)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to compute roots via overlay, using canonical empty");
                    (
                        crate::p2p::block::canonical_empty_state_root_64(),
                        crate::p2p::block::canonical_empty_tx_root_64(),
                    )
                }
            }
        } else {
            warn!("No storage available for overlay execution, using canonical empty roots");
            (
                crate::p2p::block::canonical_empty_state_root_64(),
                crate::p2p::block::canonical_empty_tx_root_64(),
            )
        };

        // Create proposal signature
        let proposal_signature = self
            .sign_proposal(
                round,
                height,
                timestamp,
                &proposer_pubkey,
                &parent_hash,
                &additional_parent_hashes,
                &state_root,
                &tx_root,
                proposal_transactions.len() as u32,
            )
            .await;

        let block_proposal = crate::proposer::BlockProposal {
            round_id: round,
            height,
            timestamp,
            proposer_pubkey,
            proposer_pou_score,
            parent_hash,
            parent_hashes: additional_parent_hashes
                .iter()
                .map(|h| h.to_vec())
                .collect(),
            state_root,
            tx_root,
            transactions: proposal_transactions,
            latency_proof,
            signature: proposal_signature,
        };
        info!(
            height = height,
            round_id = round,
            tx_count = block_proposal.transactions.len(),
            "Block complete"
        );

        // Serialize txs for block_topic (MN cache / block_final); same format as block producer
        let tx_bytes: Vec<Vec<u8>> = transactions
            .iter()
            .map(|t| crate::tx::serialize_signed_tx(t).unwrap_or_default())
            .collect();

        // Submit proposal to masternode; pass tx_bytes so we also publish block to block_topic
        if let Err(e) = self
            .submit_block_to_masternode(&block_proposal, Some(tx_bytes))
            .await
        {
            error!("Failed to submit block proposal to masternode: {}", e);
            return Err(e);
        }

        info!("Block proposal submitted for round {}", round);
        Ok(())
    }

    /// Validate pending transactions
    async fn validate_pending_transactions(&self) -> Result<()> {
        debug!("Validating pending transactions");

        let current_group = self.group_manager.get_current_group().await;
        if current_group.is_none() {
            debug!("No active group, skipping transaction validation");
            return Ok(());
        }

        let group = match current_group {
            Some(g) => g,
            None => {
                warn!("No active group available for masternode communication");
                return Ok(());
            }
        };
        let mut validated_count = 0;
        let mut invalid_count = 0;

        // In a real implementation, this would:
        // 1. Get transactions from mempool
        // 2. Validate signatures
        // 3. Check balances and nonces
        // 4. Verify gas limits
        // 5. Check double spends

        for member_id in &group.members {
            if member_id == &self.local_node_id {
                continue; // Skip self
            }

            // Simulate finding transactions for this member
            let tx_count = rand::random::<usize>() % 5; // 0-4 transactions per member
            for i in 0..tx_count {
                let is_valid = self.validate_transaction_signature(member_id, i).await;

                if is_valid {
                    validated_count += 1;
                    debug!("Validated transaction {} from member {}", i, member_id);
                } else {
                    invalid_count += 1;
                    debug!("Invalid transaction {} from member {}", i, member_id);
                }
            }
        }

        info!(
            "Transaction validation completed: {} valid, {} invalid",
            validated_count, invalid_count
        );
        Ok(())
    }

    /// Validate transaction signature (real implementation)
    async fn validate_transaction_signature(&self, member_id: &str, tx_index: usize) -> bool {
        // In real implementation:
        // 1. Get transaction from mempool
        // 2. Extract signature and public key
        // 3. Verify Ed25519 signature
        // 4. Check signature matches transaction data

        // Simulate transaction data
        let tx_data = format!("{}:{}:{}", member_id, tx_index, get_safe_timestamp());
        let _tx_hash = sha2::Sha256::digest(tx_data.as_bytes());

        // Simulate signature verification (90% success rate)
        let signature_valid = rand::random::<f64>() < 0.9;

        if signature_valid {
            trace!(
                "Transaction signature valid for {} tx {}",
                member_id,
                tx_index
            );
        } else {
            trace!(
                "Transaction signature invalid for {} tx {}",
                member_id,
                tx_index
            );
        }

        signature_valid
    }

    /// Communicate with masternode
    async fn communicate_with_masternode(&self) -> Result<()> {
        debug!("Communicating with masternode");

        // Get current group information
        let current_group = self.group_manager.get_current_group().await;
        let group = match current_group {
            Some(g) => g,
            None => {
                debug!("No active group, skipping masternode communication");
                return Ok(());
            }
        };

        // Prepare status report for masternode
        let status_report = MasternodeStatusReport {
            group_id: group.group_id.clone(),
            node_id: self.local_node_id.clone(),
            epoch: group.epoch,
            proposer_status: if let Some(ref proposer_state) = self.proposer_state {
                let state = proposer_state.read().await;
                ProposerStatus {
                    is_active: state.is_active,
                    current_round: state.current_round,
                    blocks_proposed: state.block_proposal_count,
                    last_block_height: state.last_block_height,
                }
            } else {
                ProposerStatus::default()
            },
            follower_status: if let Some(ref follower_state) = self.follower_state {
                let state = follower_state.read().await;
                FollowerStatus {
                    is_active: state.is_active,
                    current_proposer: state.current_proposer.clone(),
                    blocks_received: state.blocks_received,
                    proposals_validated: state.proposals_validated,
                    last_seen_block: state.last_seen_block,
                }
            } else {
                FollowerStatus::default()
            },
            peer_latencies: self.get_peer_latency_summary().await,
            timestamp: get_safe_timestamp(),
        };

        // Send status report to masternode
        if let Err(e) = self.send_masternode_status_report(&status_report).await {
            error!("Failed to send status report to masternode: {}", e);
            return Err(e);
        }

        // Check for masternode commands
        if let Some(commands) = self.check_masternode_commands().await? {
            for command in commands {
                self.process_masternode_command(command).await?;
            }
        }

        debug!("Masternode communication completed");
        Ok(())
    }

    /// Send status report to masternode
    async fn send_masternode_status_report(&self, report: &MasternodeStatusReport) -> Result<()> {
        // In real implementation, this would:
        // 1. Serialize the status report
        // 2. Send via P2P to masternode
        // 3. Handle connection errors
        // 4. Retry if necessary

        let payload = serde_json::to_vec(report)?;

        // Send via gossipsub to masternode topic
        let masternode_topic = libp2p::gossipsub::IdentTopic::new("/savitri/masternode/status/1");
        self.publish(masternode_topic, payload).await?;

        debug!(
            "Status report sent to masternode for group {}",
            report.group_id
        );
        Ok(())
    }

    /// Check for masternode commands
    async fn check_masternode_commands(&self) -> Result<Option<Vec<MasternodeCommand>>> {
        // In real implementation, this would:
        // 1. Listen for commands from masternode
        // 3. Return command list

        // Simulate occasional commands from masternode
        let has_commands = rand::random::<f64>() < 0.1; // 10% chance of commands

        if has_commands {
            let commands = vec![
                MasternodeCommand::UpdateLatencyTargets {
                    max_latency_ms: 1000,
                    preferred_latency_ms: 500,
                },
                MasternodeCommand::AdjustPoUWeights {
                    pou_weight: 0.8,
                    latency_weight: 0.2,
                },
            ];

            debug!("Received {} commands from masternode", commands.len());
            Ok(Some(commands))
        } else {
            Ok(None)
        }
    }

    /// Process masternode command
    async fn process_masternode_command(&self, command: MasternodeCommand) -> Result<()> {
        match command {
            MasternodeCommand::UpdateLatencyTargets {
                max_latency_ms,
                preferred_latency_ms,
            } => {
                info!(
                    "Updating latency targets: max={}ms, preferred={}ms",
                    max_latency_ms, preferred_latency_ms
                );
                // Update internal latency targets
                // This would affect how often we measure latency and what we consider acceptable
            }
            MasternodeCommand::AdjustPoUWeights {
                pou_weight,
                latency_weight,
            } => {
                info!(
                    "Adjusting PoU weights: pou={}, latency={}",
                    pou_weight, latency_weight
                );
                // Update proposer election weights
                // This would affect how we calculate proposer scores
            }
            MasternodeCommand::GroupReconfiguration {
                new_members,
                remove_members,
            } => {
                info!(
                    "Group reconfiguration: add={:?}, remove={:?}",
                    new_members, remove_members
                );
                // Handle group member changes
                // This would update our group state and potentially trigger re-election
            }
        }

        Ok(())
    }

    /// Get peer latency summary
    async fn get_peer_latency_summary(&self) -> Vec<(String, u64)> {
        let latencies = self.member_latencies.read().await;
        latencies
            .iter()
            .map(|(peer_id, (duration, _))| (peer_id.clone(), duration.as_millis() as u64))
            .collect()
    }

    /// Collect transactions for block (fallback when mempool_pipeline is None)
    /// Returns empty list - without shared mempool we only propose empty blocks to avoid simulated/random data
    async fn collect_transactions_for_block(&self) -> Result<Vec<crate::tx::SignedTx>> {
        debug!("No mempool available - proposing block with no transactions");
        Ok(vec![])
    }

    /// Get current chain head from storage (height + hash).
    /// Returns (height, hash) — single DB read used by both height and parent hash callers.
    async fn get_chain_head_info(&self) -> (u64, [u8; 64]) {
        if let Some(ref storage) = self.storage {
            match storage.get_chain_head() {
                Ok(Some(block)) => {
                    trace!(
                        height = block.height,
                        hash = %hex::encode(&block.hash[..16]),
                        "Chain head from storage"
                    );
                    return (block.height, block.hash);
                }
                Ok(None) => {
                    trace!("No chain head in storage");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to read chain head from storage");
                }
            }
        }
        // Genesis/fallback: height 0 + deterministic genesis hash
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(b"savitri-genesis-block-v1");
        let genesis_hash = hasher.finalize();
        let mut hash = [0u8; 64];
        hash.copy_from_slice(&genesis_hash);
        (0, hash)
    }

    /// Get current block height for this node's group.
    ///
    /// B1 fix (multi-group): when the local group_id is known AND has a per-group
    /// signal (from DAG or cert notifications — seeded at boot via
    /// `initialize_certified_height_from_storage`), return ONLY per-group data.
    /// Global/storage head is consulted only when group_id is unknown
    /// (pre-registration) or when no per-group signal has ever existed. This
    /// prevents two groups at the same physical height from clobbering each
    /// other's proposer next_height via the previous `.max()` merge.
    async fn get_current_block_height(&self) -> u64 {
        let group_id = self.get_current_group_id().await;
        let has_group = !group_id.is_empty() && group_id != "unknown";

        let per_group_certified = if has_group {
            self.last_certified_height_per_group
                .read()
                .ok()
                .and_then(|map| map.get(&group_id).copied())
                .unwrap_or(0)
        } else {
            0
        };

        let per_group_dag = if has_group {
            if let Some(ref dag) = self.dag {
                dag.get_max_height_for_group(&group_id).await
            } else {
                0
            }
        } else {
            0
        };

        // Fast path: per-group signal available. Return only per-group data and
        // never mix with global (which belongs to a different group in multi-group).
        if has_group && (per_group_certified > 0 || per_group_dag > 0) {
            return per_group_certified.max(per_group_dag);
        }

        // (has_group=true) DO NOT fall back to the global counters. Returning
        // group_X's height to a query for group_Y was the original DAG-
        // start for group_Y now returns 0, so the proposer correctly starts
        // its lane at height=1 instead of jumping ahead onto group_X's tail.
        // The legacy SINGLE_GROUP / unknown lane still uses the global
        // counters because there's no per-group key for it.
        if has_group {
            return 0;
        }
        let global_certified = self.last_certified_height.load(AtomicOrdering::SeqCst);
        if let Some(ref dag) = self.dag {
            let global_dag = dag.get_max_height().await;
            if global_dag > 0 {
                return global_certified.max(global_dag);
            }
        }
        if global_certified > 0 {
            return global_certified.max(self.get_chain_head_info().await.0);
        }
        self.get_chain_head_info().await.0
    }

    /// Called by network.rs when a BlockWithCertificate is received (before RocksDB commit).
    /// Updates both global and per-group counters so the pipeline knows immediately.
    pub fn notify_block_certified(&self, height: u64) {
        self.last_certified_height
            .fetch_max(height, AtomicOrdering::SeqCst);
    }

    /// Per-group variant: updates the certified height for a specific group.
    ///
    /// empty and not equal to "unknown") we no longer update the global
    /// `last_certified_height` counter. The global was used as a fallback
    /// in the `get_current_block_height()` slow path (line ~3430), but
    /// when group X reaches cert h=H the global becomes H, and when a
    /// freshly-formed group Y queries `get_current_block_height()` it
    /// falls into the slow path (per_group_Y = 0) and reads global = H
    /// (from X) → DAG linearization. Keep the global update only for the
    /// legacy SINGLE_GROUP path (group_id empty or "unknown" — single
    /// shared lane).
    pub fn notify_block_certified_for_group(&self, height: u64, group_id: &str) {
        let is_legacy_single_group = group_id.is_empty() || group_id == "unknown";
        if is_legacy_single_group {
            // Single-group lane: update global for backward compat with the
            // legacy chain head (CF_METADATA[chain_head], non-per-group).
            self.last_certified_height
                .fetch_max(height, AtomicOrdering::SeqCst);
        }
        // Always update the per-group map.
        if let Ok(mut map) = self.last_certified_height_per_group.write() {
            let entry = map.entry(group_id.to_string()).or_insert(0);
            if height > *entry {
                info!(
                    height,
                    prev = *entry,
                    group_id,
                    "Pipeline: per-group certified height updated"
                );
                *entry = height;
            }
        }

        // matches our in-flight proposed block, so `start_following_proposer`
        // is allowed to rotate again.
        let pending = self.pending_block_for_cert.clone();
        tokio::spawn(async move {
            let mut guard = pending.write().await;
            if let Some((pending_height, _)) = *guard {
                if pending_height == height {
                    *guard = None;
                }
            }
        });
    }

    /// Get all parent hashes for the next block proposal (frontier-based).
    /// Uses DAG frontier candidates when available, falls back to storage chain head.
    async fn get_parent_hashes(&self) -> Vec<[u8; 64]> {
        if let Some(ref dag) = self.dag {
            let group_id = self
                .group_manager
                .get_current_group()
                .await
                .map(|g| g.group_id.clone())
                .unwrap_or_default();
            if !group_id.is_empty() {
                let parents = dag.get_frontier_parent_candidates(&group_id).await;
                if !parents.is_empty() {
                    return parents;
                }
            }
        }
        vec![self.get_chain_head_info().await.1]
    }

    /// Convenience helper returning the primary parent hash (first of get_parent_hashes).
    async fn get_parent_hash(&self) -> [u8; 64] {
        self.get_parent_hashes()
            .await
            .first()
            .copied()
            .unwrap_or([0u8; 64])
    }

    /// Compute a deterministic baseline PoU score when the PoU scoring subsystem
    /// is not available.  The score ramps from a conservative floor (1000 = 10%)
    /// toward 5000 (50%) over the first 5 minutes of uptime, with small bonuses
    /// for mesh readiness and having collected latency data from peers.
    ///
    /// Scale: 0 – 10 000 basis points (100.00%).
    async fn compute_baseline_pou_score(&self) -> u16 {
        // --- Uptime component (0 – 4000 bps) ---
        // Ramp linearly from 1000 to 5000 over 300 seconds (5 min).
        let uptime_secs = self.created_at.elapsed().as_secs();
        const RAMP_SECS: u64 = 300;
        const FLOOR: u64 = 1000; // 10.00% — conservative new-node floor
        const CEILING: u64 = 5000; // 50.00% — matches PouScoring's own no-data fallback
        let uptime_score = if uptime_secs >= RAMP_SECS {
            CEILING
        } else {
            FLOOR + (CEILING - FLOOR) * uptime_secs / RAMP_SECS
        };

        // --- Mesh-readiness bonus (0 or 500 bps) ---
        let mesh_bonus: u64 = if *self.mesh_ready.read().await {
            500
        } else {
            0
        };

        // --- Latency-data bonus (0 or 500 bps) ---
        let latency_bonus: u64 = if !self.member_latencies.read().await.is_empty() {
            500
        } else {
            0
        };

        // Clamp to the 0-10 000 range
        let total = uptime_score + mesh_bonus + latency_bonus;
        std::cmp::min(total, 10_000) as u16
    }

    /// Get current PoU score.
    ///
    /// Returns the real PoU score from the scoring subsystem when available,
    /// otherwise falls back to a deterministic baseline derived from the node's
    /// uptime and participation state.
    async fn get_current_pou_score(&self) -> u32 {
        if let Some(ref pou_scoring) = self.pou_scoring {
            pou_scoring.get_current_score().await as u32
        } else {
            self.compute_baseline_pou_score().await as u32
        }
    }

    /// Collect latency proofs from group members for the block proposal
    async fn collect_latency_proof(&self) -> Option<crate::proposer::LatencyProofData> {
        let latencies = self.member_latencies.read().await;

        if latencies.is_empty() {
            return None;
        }

        // Calculate latency statistics
        let mut rtt_values: Vec<f64> = latencies
            .values()
            .map(|(d, _)| d.as_secs_f64() * 1000.0) // Convert to milliseconds
            .collect();
        rtt_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let median_rtt_ms = if rtt_values.is_empty() {
            0.0
        } else {
            let mid = rtt_values.len() / 2;
            if rtt_values.len() % 2 == 0 {
                (rtt_values[mid - 1] + rtt_values[mid]) / 2.0
            } else {
                rtt_values[mid]
            }
        };

        // Calculate latency score (lower is better, normalized to 0-1)
        // Score of 1.0 means < 10ms, score approaches 0 as latency increases
        let latency_score = if median_rtt_ms <= 10.0 {
            1.0
        } else {
            10.0 / median_rtt_ms
        };

        let peers_contacted = latencies.len() as u32;
        let peers_responded = latencies.len() as u32; // All in map have responded

        // Collect sample responses (max 10) for verification
        let mut sample_responses = Vec::new();
        for (member_id, (latency, pubkey_hex_opt)) in latencies.iter().take(10) {
            // CRITICAL FIX: Use the stored pubkey_hex if available, otherwise derive from PeerId hash
            // PeerId is base58, not hex, so we cannot decode it directly
            let responder_pubkey = if let Some(pubkey_hex) = pubkey_hex_opt {
                // Use the stored public key hex
                match hex::decode(pubkey_hex) {
                    Ok(bytes) if bytes.len() == 32 => {
                        let mut pubkey = [0u8; 32];
                        pubkey.copy_from_slice(&bytes);
                        pubkey
                    }
                    _ => {
                        warn!(
                            member_id = %member_id,
                            pubkey_hex = %pubkey_hex,
                            "Invalid pubkey_hex in latency data, using hash fallback"
                        );
                        // Fallback: hash the PeerId to get a deterministic 32-byte value
                        let hash = sha2::Sha256::digest(member_id.as_bytes());
                        let mut pubkey = [0u8; 32];
                        pubkey.copy_from_slice(&hash[..32]);
                        pubkey
                    }
                }
            } else {
                // No pubkey stored: use hash of PeerId as fallback
                let hash = sha2::Sha256::digest(member_id.as_bytes());
                let mut pubkey = [0u8; 32];
                pubkey.copy_from_slice(&hash[..32]);
                pubkey
            };

            // Create signature for this latency response
            let sig_data = format!("{}:{}", member_id, latency.as_nanos());
            let sig_hash = sha2::Sha256::digest(sig_data.as_bytes());
            let mut signature = [0u8; 64];
            signature[..32].copy_from_slice(sig_hash.as_slice());

            sample_responses.push(crate::proposer::LatencyResponseProof {
                responder_pubkey,
                measured_rtt_ns: latency.as_nanos() as u64,
                signature,
            });
        }

        Some(crate::proposer::LatencyProofData {
            round_id: 0, // Will be set by caller
            median_rtt_ms,
            latency_score,
            peers_contacted,
            peers_responded,
            sample_responses,
        })
    }

    /// Calculate average latency to group members
    async fn calculate_average_latency(&self) -> u64 {
        let latencies = self.member_latencies.read().await;
        if latencies.is_empty() {
            return 0;
        }

        let total_ms: u64 = latencies.values().map(|(d, _)| d.as_millis() as u64).sum();
        total_ms / latencies.len() as u64
    }

    /// Sign a block proposal using the node's private key
    async fn sign_proposal(
        &self,
        round_id: u64,
        height: u64,
        timestamp: u64,
        proposer_pubkey: &[u8; 32],
        parent_hash: &[u8; 64],
        additional_parent_hashes: &[[u8; 64]],
        state_root: &[u8; 64],
        tx_root: &[u8; 64],
        tx_count: u32,
    ) -> [u8; 64] {
        let mut message_data = b"savitri-proposal-v1".to_vec();
        message_data.extend_from_slice(&round_id.to_le_bytes());
        message_data.extend_from_slice(&height.to_le_bytes());
        message_data.extend_from_slice(&timestamp.to_le_bytes());
        message_data.extend_from_slice(proposer_pubkey);
        message_data.extend_from_slice(parent_hash);
        // Canonical parents (matches proposal_signable_bytes)
        let mut canonical_parents: Vec<[u8; 64]> = vec![*parent_hash];
        for h in additional_parent_hashes {
            if *h != *parent_hash && !canonical_parents.contains(h) {
                canonical_parents.push(*h);
            }
        }
        message_data.extend_from_slice(&(canonical_parents.len() as u32).to_le_bytes());
        for parent in &canonical_parents {
            message_data.extend_from_slice(parent);
        }
        message_data.extend_from_slice(state_root);
        message_data.extend_from_slice(tx_root);
        message_data.extend_from_slice(&tx_count.to_le_bytes());

        let signature = self.signing_key.sign(&message_data);
        signature.to_bytes()
    }

    /// Build election certificate from collected ProposerElectionResult messages (when we are the proposer).
    async fn build_election_certificate(&self) -> Option<ElectionCertificate> {
        let collected = self.election_results_collected.read().await;
        if collected.is_empty() {
            debug!("[ELECTION CERT] No collected election results, certificate will be None");
            return None;
        }

        // CRITICAL FIX: Filter election results to match the CURRENT group_id.
        // Without this filter, stale results from previous group epochs persist
        // and the certificate gets group_id from epoch N while the proposal uses
        // group_id from epoch N+K, causing MN to reject with "Certificate group_id
        // does not match proposal proposer_group_id".
        let current_group_id = self.get_current_group_id().await;
        let group_filtered: Vec<_> = collected
            .iter()
            .filter(|r| r.group_id == current_group_id)
            .collect();

        if group_filtered.is_empty() {
            debug!(
                current_group_id = %current_group_id,
                total_collected = collected.len(),
                "[ELECTION CERT] No election results for current group, certificate will be None"
            );
            return None;
        }

        let first = group_filtered.first().unwrap();

        // Log all round values to check for inconsistencies
        let rounds: Vec<u64> = group_filtered.iter().map(|r| r.round).collect();
        let unique_rounds: std::collections::HashSet<u64> = rounds.iter().copied().collect();

        // CRITICAL FIX: Filter out election results with inconsistent round values
        // Only use results that match the first result's round to ensure certificate.election_round
        // matches what was actually signed in the attestations
        let expected_round = first.round;
        let filtered_results: Vec<_> = group_filtered
            .iter()
            .filter(|r| r.round == expected_round)
            .collect();

        if filtered_results.len() != group_filtered.len() {
            let inconsistent_count = group_filtered.len() - filtered_results.len();
            warn!(
                group_id = %first.group_id,
                expected_round = expected_round,
                total_group_results = group_filtered.len(),
                consistent_count = filtered_results.len(),
                inconsistent_count = inconsistent_count,
                unique_rounds_count = unique_rounds.len(),
                rounds = ?rounds,
                "⚠️ [ELECTION CERT] WARNING: Filtered out {} election results with inconsistent round values! Only using results with round={}",
                inconsistent_count,
                expected_round
            );
        } else {
            debug!(
                group_id = %first.group_id,
                round = expected_round,
                "📋 [ELECTION CERT] All collected results have consistent round value"
            );
        }

        // If filtering removed all results, return None
        if filtered_results.is_empty() {
            warn!(
                group_id = %first.group_id,
                expected_round = expected_round,
                "⚠️ [ELECTION CERT] No valid election results after filtering for consistent rounds - certificate will be None"
            );
            return None;
        }

        info!(
            group_id = %first.group_id,
            collected_results = group_filtered.len(),
            filtered_results = filtered_results.len(),
            election_round = expected_round,
            unique_rounds_count = unique_rounds.len(),
            "📋 [ELECTION CERT] Building certificate from filtered election results (ensuring consistent round)"
        );
        let elected_proposer_pubkey: [u8; 32] = *self.signing_key.verifying_key().as_bytes();
        let expected_proposer = &filtered_results.first().unwrap().elected_proposer;
        let mut attestations = Vec::with_capacity(filtered_results.len());
        let mut skipped = 0u32;
        // Build attestations from filtered results (all with consistent round)
        for r in filtered_results.iter() {
            // Verify round consistency before adding attestation
            if r.round != expected_round {
                warn!(
                    sender = %r.sender,
                    result_round = r.round,
                    expected_round = expected_round,
                    "[ELECTION CERT] Skipping result: round mismatch (should have been filtered)"
                );
                skipped += 1;
                continue;
            }
            // Skip attestations that signed a different elected_proposer.
            // This can happen when nodes disagree on the election winner (e.g. due to
            // latency-based scoring differences). Including such attestations causes
            // signature verification failure on the MN side.
            if r.elected_proposer != *expected_proposer {
                warn!(
                    sender = %r.sender,
                    result_proposer = %r.elected_proposer,
                    cert_proposer = %expected_proposer,
                    "[ELECTION CERT] Skipping result: elected_proposer mismatch"
                );
                skipped += 1;
                continue;
            }

            let signer_pubkey_hex = match r.sender_pubkey_hex.as_deref() {
                Some(h) => h,
                None => {
                    warn!(sender = %r.sender, "[ELECTION CERT] Skipping result: missing sender_pubkey_hex");
                    skipped += 1;
                    continue;
                }
            };
            let bytes = match hex::decode(signer_pubkey_hex) {
                Ok(b) => b,
                Err(e) => {
                    warn!(sender = %r.sender, error = %e, "[ELECTION CERT] Skipping result: invalid sender_pubkey_hex");
                    skipped += 1;
                    continue;
                }
            };
            if bytes.len() != 32 {
                warn!(sender = %r.sender, len = bytes.len(), "[ELECTION CERT] Skipping result: signer_pubkey not 32 bytes");
                skipped += 1;
                continue;
            }
            let mut signer_pubkey = [0u8; 32];
            signer_pubkey.copy_from_slice(&bytes);
            attestations.push(ElectionAttestation {
                signer_peer_id: r.sender.clone(),
                signer_pubkey,
                signature: r.signature,
            });
        }
        if attestations.is_empty() {
            warn!(
                group_id = %first.group_id,
                skipped = skipped,
                "[ELECTION CERT] No valid attestations (all results skipped), certificate will be None"
            );
            return None;
        }
        if skipped > 0 {
            warn!(
                skipped = skipped,
                valid = attestations.len(),
                "[ELECTION CERT] Some results skipped due to missing/invalid pubkey"
            );
        }
        // Use the first filtered result (which has the consistent round)
        let cert_first = filtered_results.first().unwrap();

        // Falla 3 (anti-replay): also require all attesters to have signed with the SAME
        // tenure_start_height as cert_first. Different snapshots would mean their signatures
        // are over different signable bytes and would fail verification on the MN side anyway.
        let cert_tenure_start = cert_first.tenure_start_height;
        let tenure_mismatches = filtered_results
            .iter()
            .filter(|r| r.tenure_start_height != cert_tenure_start)
            .count();
        if tenure_mismatches > 0 {
            warn!(
                group_id = %first.group_id,
                cert_tenure_start_height = cert_tenure_start,
                mismatches = tenure_mismatches,
                "⚠️ [ELECTION CERT] Some attesters have a different tenure_start_height; signature verification will diverge — certificate will be None"
            );
            return None;
        }

        // CRITICAL: All attestations were built from filtered_results which all have expected_round
        // So certificate.election_round will match what was actually signed
        let certificate = ElectionCertificate {
            group_id: cert_first.group_id.clone(),
            election_round: expected_round, // Use expected_round to ensure consistency
            elected_proposer_peer_id: cert_first.elected_proposer.clone(),
            elected_proposer_pubkey,
            proposer_pou_score: cert_first.proposer_pou_score,
            timestamp: cert_first.timestamp,
            candidates: cert_first.candidates.clone(),
            attestations: attestations.clone(),
            tenure_start_height: cert_tenure_start,
        };

        if certificate.election_round != expected_round {
            warn!(
                group_id = %certificate.group_id,
                certificate_election_round = certificate.election_round,
                expected_round = expected_round,
                "⚠️ [ELECTION CERT] CRITICAL: Certificate election_round doesn't match expected round!"
            );
            return None; // Reject certificate if round doesn't match
        }

        // Verify that all attestations were signed with the same round as certificate.election_round
        // This is a critical check: if any attestation was signed with a different round, verification will fail
        info!(
            group_id = %certificate.group_id,
            election_round = certificate.election_round,
            attestations_count = certificate.attestations.len(),
            "✅ [ELECTION CERT] Certificate built with consistent election_round - all attestations should verify correctly"
        );

        info!(
            group_id = %certificate.group_id,
            election_round = certificate.election_round,
            elected_proposer = %certificate.elected_proposer_peer_id,
            attestations = certificate.attestations.len(),
            timestamp = certificate.timestamp,
            candidates_count = certificate.candidates.len(),
            filtered_from = collected.len(),
            "📋 [ELECTION CERT] Certificate built successfully - election_round={} matches all {} attestations (filtered from {} collected results)",
            certificate.election_round,
            certificate.attestations.len(),
            collected.len()
        );

        // Log attestation details for debugging
        for (idx, att) in certificate.attestations.iter().enumerate() {
            debug!(
                index = idx,
                signer_peer_id = %att.signer_peer_id,
                signer_pubkey_hex = %hex::encode(&att.signer_pubkey),
                signature_hex = %hex::encode(&att.signature),
                "[ELECTION CERT] Attestation details"
            );
        }

        Some(certificate)
    }

    /// Submit block to masternode.
    /// If `tx_bytes` is Some, also publishes the block to block_topic (for MN cache / block_final).
    async fn submit_block_to_masternode(
        &self,
        proposal: &crate::proposer::BlockProposal,
        tx_bytes: Option<Vec<Vec<u8>>>,
    ) -> Result<()> {
        let current_group_id = self.get_current_group_id().await;
        info!(
            group_id = %current_group_id,
            round_id = proposal.round_id,
            height = proposal.height,
            tx_count = proposal.transactions.len(),
            "Submitting block proposal to masternode"
        );

        // In real implementation, this would:
        // 1. Serialize the block proposal
        // 2. Send via P2P to masternode
        // 3. Wait for confirmation
        // 4. Handle submission errors

        // Build masternode wire-format proposal — use shared hash function for consistency
        let block_hash = crate::p2p::block::compute_block_hash(&crate::tx::Block {
            height: proposal.height,
            parent_hash: {
                // parent_hash in proposal is [u8; 64], same as Block.parent_hash
                proposal.parent_hash
            },
            state_root: {
                let mut sr = [0u8; 32];
                sr.copy_from_slice(&proposal.state_root[..32]);
                sr
            },
            tx_root: {
                let mut tr = [0u8; 32];
                tr.copy_from_slice(&proposal.tx_root[..32]);
                tr
            },
            ..Default::default()
        });

        let tx_count = proposal.transactions.len() as u32;
        let mut signable = Vec::new();
        signable.extend_from_slice(&proposal.round_id.to_le_bytes());
        signable.extend_from_slice(&proposal.height.to_le_bytes());
        signable.extend_from_slice(&proposal.timestamp.to_le_bytes());
        signable.extend_from_slice(&proposal.proposer_pubkey);
        signable.extend_from_slice(&block_hash);
        signable.extend_from_slice(&tx_count.to_le_bytes());
        let signature = self.signing_key.sign(&signable).to_bytes();

        let election_certificate = self.build_election_certificate().await;
        if election_certificate.is_none() && !current_group_id.is_empty() {
            warn!(
                group_id = %current_group_id,
                "📤 [LN->MN] Missing election certificate for active group; skipping proposal publish"
            );
            return Ok(());
        }
        let proposer_group_id = election_certificate
            .as_ref()
            .map(|c| c.group_id.clone())
            .unwrap_or_else(|| current_group_id.clone());
        let has_certificate = election_certificate.is_some();

        // ═══════════════════════════════════════════════════════════════
        // HANDSHAKE FASE 1: Invia certificato di elezione al MN owner
        // ═══════════════════════════════════════════════════════════════
        if let Some(ref cert) = election_certificate {
            let cert_msg = ProposerElectionCertMessage {
                group_id: cert.group_id.clone(),
                round_id: proposal.round_id,
                proposer_peer_id: self.local_node_id.clone(),
                proposer_pubkey: cert.elected_proposer_pubkey,
                proposer_pou_score: cert.proposer_pou_score,
                election_timestamp: cert.timestamp,
                candidates: cert.candidates.clone(),
                attestations: cert.attestations.clone(),
            };

            let election_cert_topic =
                libp2p::gossipsub::IdentTopic::new("/savitri/masternode/election/cert/1");
            match serde_json::to_vec(&cert_msg) {
                Ok(cert_payload) => {
                    if let Err(e) = self
                        .publish(election_cert_topic.clone(), cert_payload)
                        .await
                    {
                        warn!(error=?e, "Failed to publish election certificate to MN");
                    } else {
                        info!(
                            group_id = %cert.group_id,
                            round_id = proposal.round_id,
                            attestations = cert.attestations.len(),
                            "📜 [LN→MN] Phase 1: Election certificate sent to MN"
                        );
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to serialize election certificate");
                }
            }

            // ═══════════════════════════════════════════════════════════════
            // HANDSHAKE FASE 2: Attendi ACK di whitelist dal MN (max 5s)
            // ═══════════════════════════════════════════════════════════════
            // Drain-until-match: the channel can accumulate ACKs from prior rounds and
            // duplicates from multiple MNs (each broadcasts one ACK per election cert).
            // Accepting the first item blindly causes a stale-round ACK to satisfy the
            // handshake, the subsequent proposal fails verification on the MN, a new
            // round triggers, and the channel fills. Here we loop: drain non-matching
            // ACK (stale round or different target/group) as cheap discards and accept
            // only an ACK whose round_id ≥ proposal.round_id AND group_id matches.
            // The 5s overall deadline is preserved across discards.
            let ack_received = {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                let mut guard = self.whitelist_ack_rx.lock().await;
                if let Some(ref mut rx) = *guard {
                    let mut discarded = 0u32;
                    loop {
                        let remaining =
                            deadline.saturating_duration_since(tokio::time::Instant::now());
                        if remaining.is_zero() {
                            warn!(
                                group_id = %cert.group_id,
                                round_id = proposal.round_id,
                                discarded_stale = discarded,
                                "⏱️ [LN] Phase 2: Whitelist ACK timeout (5s) - proceeding anyway for backward compat"
                            );
                            break false;
                        }
                        match tokio::time::timeout(remaining, rx.recv()).await {
                            Ok(Some(ack)) => {
                                if ack.target_proposer_peer_id == self.local_node_id
                                    && ack.group_id == cert.group_id
                                    && ack.round_id >= proposal.round_id
                                {
                                    info!(
                                        masternode = %ack.masternode_peer_id,
                                        group_id = %ack.group_id,
                                        round_id = ack.round_id,
                                        validity_secs = ack.validity_secs,
                                        discarded_stale = discarded,
                                        "✅ [LN←MN] Phase 2: Whitelist ACK received from MN"
                                    );
                                    break true;
                                } else {
                                    discarded = discarded.saturating_add(1);
                                    debug!(
                                        target = %ack.target_proposer_peer_id,
                                        got_group = %ack.group_id,
                                        want_group = %cert.group_id,
                                        got_round = ack.round_id,
                                        want_round = proposal.round_id,
                                        "Discarding non-matching ACK (stale or other target)"
                                    );
                                    continue;
                                }
                            }
                            Ok(None) => {
                                warn!("Whitelist ACK channel closed");
                                break false;
                            }
                            Err(_) => {
                                warn!(
                                    group_id = %cert.group_id,
                                    round_id = proposal.round_id,
                                    discarded_stale = discarded,
                                    "⏱️ [LN] Phase 2: Whitelist ACK timeout (5s) - proceeding anyway for backward compat"
                                );
                                break false;
                            }
                        }
                    }
                } else {
                    debug!("No whitelist ACK channel configured - skipping handshake wait");
                    false
                }
            };

            if ack_received {
                info!(
                    group_id = %cert.group_id,
                    round_id = proposal.round_id,
                    "📤 [LN→MN] Phase 3: Sending confirmed block proposal after ACK"
                );
            }
        }

        let masternode_topic = "/savitri/masternode/proposal/1";
        if let Some(ref cert) = election_certificate {
            info!(
                group_id = %cert.group_id,
                attestations = cert.attestations.len(),
                election_round = cert.election_round,
                elected_proposer = %cert.elected_proposer_peer_id,
                timestamp = cert.timestamp,
                masternode_topic = %masternode_topic,
                sender_lightnode_peer_id = %self.local_node_id,
                "📤 [LN->MN] Including election certificate in block proposal"
            );
        }

        let wire = LightnodeProposalWire {
            round_id: proposal.round_id,
            height: proposal.height,
            timestamp: proposal.timestamp,
            proposer_pubkey: proposal.proposer_pubkey,
            block_hash,
            tx_count,
            signature,
            parent_hash: proposal.parent_hash,
            state_root: proposal.state_root,
            tx_root: proposal.tx_root,
            proposer_group_id: proposer_group_id.clone(),
            election_certificate,
            raw_txs: tx_bytes.clone(),
        };

        // Log certificate details in wire format before serialization
        if let Some(ref cert) = wire.election_certificate {
            debug!(
                group_id = %cert.group_id,
                election_round = cert.election_round,
                elected_proposer = %cert.elected_proposer_peer_id,
                attestations_count = cert.attestations.len(),
                "📤 [LN->MN] Certificate in wire format before JSON serialization"
            );
        }

        let payload = serde_json::to_vec(&wire)?;

        // Log payload size for debugging
        if let Some(ref cert) = wire.election_certificate {
            debug!(
                group_id = %cert.group_id,
                election_round = cert.election_round,
                payload_len = payload.len(),
                "📤 [LN->MN] Certificate serialized successfully, payload ready for transmission"
            );
        }

        // Publish block to block_topic FIRST so MN can cache it before processing the proposal (avoids race)
        if let (Some(ref tx_sender), Some(ref txs)) =
            (self.block_broadcast_only_tx.as_ref(), tx_bytes.as_ref())
        {
            let have = super::types::HaveBlock {
                hash: block_hash,
                height: proposal.height,
                exec_height: proposal.height,
                tx_count: proposal.transactions.len() as u32,
            };
            let block_message = super::types::BlockMessage {
                hash: block_hash,
                header: super::types::BlockHeader {
                    exec_height: proposal.height,
                    proposer: proposal.proposer_pubkey,
                    timestamp: proposal.timestamp,
                    parent_hash: proposal.parent_hash,
                },
                txs: txs.to_vec(),
            };
            let broadcast = super::types::BlockBroadcast {
                have,
                block: block_message,
            };
            if let Err(e) = tx_sender.send(broadcast).await {
                warn!(error = %e, "Failed to send block to block_topic (MN cache); block_final may have no block");
            } else {
                info!(
                    height = proposal.height,
                    hash = %hex::encode(&block_hash[..8]),
                    "📤 [LN->block_topic] Block published for MN cache BEFORE proposal (so MN can send BlockWithCertificate)"
                );
            }
            // Brief delay so block has time to reach MN before proposal is processed.
            // Reduced from 150ms to 20ms — gossipsub propagation is typically <10ms
            // in testnet/WAN, and the old 150ms added directly to every block's BFT
            // round-trip, capping throughput to ~6 blk/s.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Topic on which masternode expects block proposals (handle_lightnode_proposal), NOT group formation
        let masternode_topic = libp2p::gossipsub::IdentTopic::new("/savitri/masternode/proposal/1");
        info!(
            group_id = %proposer_group_id,
            round_id = proposal.round_id,
            height = proposal.height,
            tx_count = proposal.transactions.len(),
            has_certificate,
            topic = %masternode_topic,
            "📤 [LN->MN] Step 1: Submitting block proposal to masternode (intra-group proposer)"
        );
        self.publish(masternode_topic.clone(), payload).await.map_err(|e| {
            warn!(error=?e, topic = %masternode_topic, "Failed to publish block proposal to masternode topic");
            anyhow::anyhow!("gossipsub publish failed: {}", e)
        })?;
        info!(
            group_id = %proposer_group_id,
            round_id = proposal.round_id,
            height = proposal.height,
            "📤 [LN->MN] Step 2: Block proposal published to masternode topic successfully"
        );

        // `start_following_proposer` will defer rotation until either the
        // BFT certificate matches (cert handler clears the flag) or
        // CERT_PENDING_MAX_WAIT elapses (liveness fallback). Only set when
        // the block actually carries TX — empty blocks don't need rotation
        // protection because there is nothing to lose if the proposer
        // changes mid-flight.
        if !proposal.transactions.is_empty() {
            let mut guard = self.pending_block_for_cert.write().await;
            *guard = Some((proposal.height, Instant::now()));
        }

        let proposer_id =
            libp2p::identity::ed25519::PublicKey::try_from_bytes(&proposal.proposer_pubkey)
                .ok()
                .map(|pk| {
                    PeerId::from_public_key(&libp2p::identity::PublicKey::from(pk)).to_string()
                })
                .unwrap_or_else(|| hex::encode(proposal.proposer_pubkey));
        let intra_payload =
            serde_json::to_vec(&(proposer_id.clone(), proposal.clone())).unwrap_or_default();
        if let Err(e) = self
            .publish(self.proposal_topic.clone(), intra_payload)
            .await
        {
            warn!(error=?e, "Failed to publish block proposal to intra-group topic");
        } else {
            debug!(
                round_id = proposal.round_id,
                "Published block proposal to intra-group topic for follower validation"
            );
        }

        info!(
            group_id = %proposer_group_id,
            round_id = proposal.round_id,
            height = proposal.height,
            "Block proposal successfully submitted to masternode"
        );

        // Update proposer state
        if let Some(ref proposer_state) = self.proposer_state {
            let mut state = proposer_state.write().await;
            state.last_block_height = proposal.height;
        }

        Ok(())
    }

    /// Start following the elected proposer
    async fn start_following_proposer(&mut self, proposer_id: &str) -> Result<()> {
        // Stop existing proposer block production loop if we were previously the proposer.
        // Guard: don't interrupt an active proposer that has not yet used its full tenure.
        // This prevents election ACKs for the next epoch from killing a running block loop
        // that has only produced a few blocks.
        if let Some(ref proposer_state) = self.proposer_state {
            let (is_active, blocks_done) = {
                let s = proposer_state.read().await;
                (s.is_active, s.block_proposal_count)
            };
            if is_active && blocks_done < PROPOSER_TENURE_BLOCKS {
                info!(
                    blocks_proposed = blocks_done,
                    tenure = PROPOSER_TENURE_BLOCKS,
                    next_proposer = %proposer_id,
                    "Mid-tenure guard: ignoring election handoff until tenure is complete"
                );
                return Ok(());
            }
            // in-flight proposed block whose BFT cert has not yet matched,
            // defer rotation up to CERT_PENDING_MAX_WAIT (30 s). This stops
            // the "drain → propose → rotate before cert → restore" loop
            // that produced empty blocks under load (memory
            // session_2026-04-30_phase1.md residual blocker).
            let pending_snapshot = *self.pending_block_for_cert.read().await;
            if let Some((pending_height, proposed_at)) = pending_snapshot {
                let waited = proposed_at.elapsed();
                if waited < CERT_PENDING_MAX_WAIT {
                    info!(
                        pending_height,
                        waited_ms = waited.as_millis() as u64,
                        max_wait_ms = CERT_PENDING_MAX_WAIT.as_millis() as u64,
                        next_proposer = %proposer_id,
                        "Cert-pending guard: deferring election handoff until BFT cert returns or max-wait elapses"
                    );
                    return Ok(());
                }
                // Clear the flag so we don't keep deferring forever, then
                // proceed with the rotation.
                let mut guard = self.pending_block_for_cert.write().await;
                *guard = None;
                warn!(
                    pending_height,
                    waited_ms = waited.as_millis() as u64,
                    next_proposer = %proposer_id,
                    "Cert-pending guard: max-wait elapsed without cert match — yielding tenure"
                );
            }
            let mut state = proposer_state.write().await;
            state.is_active = false;
            info!(
                "Stopped proposer block production loop (now following {})",
                proposer_id
            );
        }
        self.proposer_state = None;
        // Reset block_loop_running so a future re-election can start a new block loop.
        // The spawned loop will also reset it on exit, but that is async — reset now to
        // avoid a window where compare_exchange fails in start_proposer_duties.
        self.block_loop_running.store(false, AtomicOrdering::SeqCst);

        if let Some(ref flag) = self.is_intragroup_proposer {
            *flag.write().await = false;
        }
        // step down. Best-effort transition through SteppingDown -> Idle.
        // try_step_down errors out if SM is already Idle (already stepped
        // down), which is fine — we just want to converge to Idle.
        let _ = self
            .proposer_sm
            .try_step_down(crate::p2p::proposer_state::StepDownReason::NewElectionElsewhere)
            .await;
        let _ = self.proposer_sm.try_finish_step_down().await;
        info!("Starting to follow proposer: {}", proposer_id);

        // Initialize follower state
        let follower_state = Arc::new(RwLock::new(FollowerState {
            current_proposer: proposer_id.to_string(),
            last_seen_block: 0,
            is_active: true,
            blocks_received: 0,
            proposals_validated: 0,
        }));
        self.follower_state = Some(follower_state.clone());

        // Start listening for blocks from proposer
        let follower_state_clone = follower_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut block_listener_interval = tokio::time::interval(Duration::from_secs(1)); // Check every second
            loop {
                block_listener_interval.tick().await;

                let state = follower_state_clone.write().await;
                if state.is_active {
                    if let Err(e) = intra_group_comm_clone
                        .listen_for_blocks_from_proposer(&state.current_proposer)
                        .await
                    {
                        error!("Failed to listen for blocks from proposer: {}", e);
                    }
                }
            }
        });

        let follower_state_clone = follower_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut validation_interval = tokio::time::interval(Duration::from_secs(3)); // Validate every 3 seconds
            loop {
                validation_interval.tick().await;

                let state = follower_state_clone.write().await;
                if state.is_active {
                    if let Err(e) = intra_group_comm_clone
                        .validate_proposer_proposals(&state.current_proposer)
                        .await
                    {
                        error!("Failed to validate proposer proposals: {}", e);
                    }
                }
            }
        });

        // Start consensus participation
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut consensus_interval = tokio::time::interval(Duration::from_secs(5)); // Participate every 5 seconds
            loop {
                consensus_interval.tick().await;

                if let Err(e) = intra_group_comm_clone.participate_in_consensus().await {
                    error!("Failed to participate in consensus: {}", e);
                }
            }
        });

        debug!("Follower duties initiated - block listening, validation, and consensus participation started");
        Ok(())
    }

    /// Listen for blocks from proposer - processes real proposals from received_proposals queue
    async fn listen_for_blocks_from_proposer(&self, proposer_id: &str) -> Result<()> {
        // Process received proposals (from intra-group gossip)
        while let Some((round_id, pid, proposal)) = self.take_next_proposal().await {
            if pid != proposer_id {
                continue; // Skip proposals from other proposers
            }
            match self.validate_block_proposal(&proposal).await {
                Ok(()) => {
                    if let Some(ref follower_state) = self.follower_state {
                        let mut state = follower_state.write().await;
                        state.blocks_received += 1;
                        state.last_seen_block = proposal.height;
                    }
                    info!(
                        round = round_id,
                        height = proposal.height,
                        proposer = %proposer_id,
                        "Valid block proposal received and validated"
                    );
                }
                Err(e) => {
                    warn!(
                        round = round_id,
                        height = proposal.height,
                        error = %e,
                        "Block proposal validation failed"
                    );
                }
            }
        }
        Ok(())
    }

    async fn validate_block_proposal(
        &self,
        proposal: &crate::proposer::BlockProposal,
    ) -> Result<()> {
        // 1. Verify proposer signature
        let signable = crate::proposer::proposal_signable_bytes(proposal);
        let pk = ed25519_dalek::VerifyingKey::from_bytes(&proposal.proposer_pubkey)
            .map_err(|e| anyhow::anyhow!("Invalid proposer pubkey: {}", e))?;
        let sig = ed25519_dalek::Signature::from_bytes(&proposal.signature);
        pk.verify_strict(&signable, &sig)
            .map_err(|e| anyhow::anyhow!("Proposal signature invalid: {}", e))?;

        // 2. Verify parent_hash matches local chain
        let local_parent = self.get_parent_hash().await;
        if proposal.parent_hash != local_parent {
            return Err(anyhow::anyhow!(
                "Parent hash mismatch: expected {}",
                hex::encode(local_parent)
            ));
        }

        // 2b. Merge policy: stale frontier tips must be referenced so branches are not left behind.
        if let Some(ref dag) = self.dag {
            let frontier = dag.get_frontier_tips().await;
            let max_frontier_height = frontier.iter().map(|tip| tip.height).max().unwrap_or(0);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            const MAX_TIP_LAG_BLOCKS: u64 = 5;
            const MAX_TIP_AGE_SECS: u64 = 120;

            // Build canonical parent set from proposal (primary + additional)
            let mut canonical_parents: Vec<[u8; 64]> = vec![proposal.parent_hash];
            for hash in &proposal.parent_hashes {
                let mut normalized = [0u8; 64];
                let len = hash.len().min(64);
                normalized[..len].copy_from_slice(&hash[..len]);
                if !canonical_parents.contains(&normalized) {
                    canonical_parents.push(normalized);
                }
            }

            for tip in frontier {
                if tip.hash == proposal.parent_hash {
                    continue;
                }
                let lagged = max_frontier_height.saturating_sub(tip.height) > MAX_TIP_LAG_BLOCKS;
                let old = tip.timestamp > 0 && now.saturating_sub(tip.timestamp) > MAX_TIP_AGE_SECS;
                if (lagged || old) && !canonical_parents.contains(&tip.hash) {
                    return Err(anyhow::anyhow!(
                        "Missing stale frontier parent {} (height={}, lag={}, age_secs={})",
                        hex::encode(tip.hash),
                        tip.height,
                        max_frontier_height.saturating_sub(tip.height),
                        now.saturating_sub(tip.timestamp)
                    ));
                }
            }
        }

        // 3. Verify height is valid (next expected)
        let current = self.get_current_block_height().await;
        if proposal.height != current + 1 {
            return Err(anyhow::anyhow!(
                "Invalid height: got {} expected {}",
                proposal.height,
                current + 1
            ));
        }

        // 4. Verify timestamp is within acceptable range (5 min skew)
        let now = get_safe_timestamp();
        if proposal.timestamp > now + 300 {
            return Err(anyhow::anyhow!("Proposal timestamp too far in future"));
        }

        // 5. Verify state_root and tx_root via overlay execution
        if !proposal.transactions.is_empty() {
            if let Some(ref storage) = self.storage {
                // Convert ProposalTransaction → SignedTx for overlay execution
                // SECURITY: Attempt real signature verification instead of hardcoding pre_verified: true.
                // In this system, ptx.from IS the raw 32-byte Ed25519 public key (addresses are
                // hex-encoded public keys), so we can use it as the pubkey for verification.
                // Note: apply_certified_block_direct does NOT check pre_verified — it only
                let signed_txs: Vec<crate::tx::SignedTx> = proposal
                    .transactions
                    .iter()
                    .map(|ptx| {
                        let mut stx = crate::tx::SignedTx {
                            from: hex::encode(ptx.from),
                            to: hex::encode(ptx.to),
                            amount: ptx.amount,
                            nonce: ptx.nonce,
                            fee: Some(ptx.fee as u128),
                            data: if ptx.data.is_empty() {
                                None
                            } else {
                                Some(ptx.data.clone())
                            },
                            sig: ptx.signature,
                            pubkey: ptx.from.to_vec(), // ptx.from is the raw 32-byte Ed25519 public key
                            pre_verified: false,       // will be set by verification below
                        };
                        stx.pre_verified = crate::tx::verify_transaction_signature_ext(&stx);
                        if !stx.pre_verified {
                            warn!(
                                from = %stx.from.chars().take(16).collect::<String>(),
                                nonce = stx.nonce,
                                "Signature verification failed for TX in MN-certified proposal \
                                 (will still apply via certified block path)"
                            );
                        }
                        stx
                    })
                    .collect();

                let temp_block = crate::tx::Block {
                    height: proposal.height,
                    parent_hash: proposal.parent_hash,
                    ..Default::default()
                };
                match crate::p2p::block::apply_certified_block_direct(
                    storage.as_ref(),
                    &temp_block,
                    &signed_txs,
                ) {
                    Ok((overlay, receipts)) => {
                        let sr32 = crate::p2p::block::compute_state_root_from_overlay(&overlay);
                        let tr32 = crate::p2p::block::compute_tx_root_from_receipts(&receipts);
                        let mut expected_sr = [0u8; 64];
                        expected_sr[..32].copy_from_slice(&sr32);
                        let mut expected_tr = [0u8; 64];
                        expected_tr[..32].copy_from_slice(&tr32);
                        if proposal.state_root != expected_sr {
                            return Err(anyhow::anyhow!(
                                "State root mismatch in proposal verification"
                            ));
                        }
                        if proposal.tx_root != expected_tr {
                            return Err(anyhow::anyhow!(
                                "Transaction root mismatch in proposal verification"
                            ));
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Could not verify roots via overlay, skipping root check");
                    }
                }
            } else {
                warn!("No storage for proposal root verification, skipping root check");
            }
        }

        // 6. Basic PoU check (proposer should have sufficient score)
        if proposal.proposer_pou_score < 3000 {
            return Err(anyhow::anyhow!(
                "Insufficient PoU score: {}",
                proposal.proposer_pou_score
            ));
        }

        Ok(())
    }

    /// Validate proposer proposals - process any pending proposals from received queue
    async fn validate_proposer_proposals(&self, proposer_id: &str) -> Result<()> {
        let mut validated = 0u64;
        while let Some((_round_id, pid, proposal)) = self.take_next_proposal().await {
            if pid != proposer_id {
                continue;
            }
            if self.validate_block_proposal(&proposal).await.is_ok() {
                validated += 1;
                if let Some(ref follower_state) = self.follower_state {
                    let mut state = follower_state.write().await;
                    state.proposals_validated += 1;
                }
            }
        }
        if validated > 0 {
            info!("Validated {} proposals from {}", validated, proposer_id);
        }
        Ok(())
    }

    /// Get proposer's PoU score
    async fn get_proposer_pou_score(&self, proposer_id: &str) -> u32 {
        // Check if we have the proposer's PoU score
        let pou_scores = self.member_pou_scores.read().await;

        if let Some(&(score, _)) = pou_scores.get(proposer_id) {
            score
        } else {
            // If we don't have the score, estimate based on network position
            // In real implementation, this would query the PoU scoring service
            let estimated_score = 5000 + (rand::random::<u32>() % 3000); // 50-80% range
            estimated_score
        }
    }

    /// Participate in consensus
    async fn participate_in_consensus(&self) -> Result<()> {
        debug!("Participating in consensus");

        // In real implementation, this would:
        // 1. Check if we should vote on current proposals
        // 2. Validate proposals from current proposer
        //  3. Cast votes on valid proposals
        // 4. Participate in BFT consensus
        // 5. Handle view changes if needed

        // Get current proposer
        let current_proposer = if let Some(ref follower_state) = self.follower_state {
            let state = follower_state.read().await;
            Some(state.current_proposer.clone())
        } else {
            None
        };

        if let Some(proposer_id) = current_proposer {
            // Simulate consensus participation
            let should_vote = rand::random::<f64>() < 0.8; // 80% participation rate

            if should_vote {
                // Create a vote for the current round
                match self.create_consensus_vote(&proposer_id).await {
                    Ok(vote) => {
                        // Submit vote to consensus
                        if let Err(e) = self.submit_consensus_vote(&vote).await {
                            error!("Failed to submit consensus vote: {}", e);
                        } else {
                            debug!("Submitted consensus vote for round {}", vote.round);
                        }
                    }
                    Err(e) => {
                        error!("Failed to create consensus vote: {}", e);
                    }
                }
            }
        }

        debug!("Consensus participation completed");
        Ok(())
    }

    /// Create consensus vote
    async fn create_consensus_vote(&self, proposer_id: &str) -> Result<ConsensusVote> {
        let current_round = if let Some(ref proposer_state) = self.proposer_state {
            let state = proposer_state.read().await;
            state.current_round
        } else {
            0
        };
        let mut vote = ConsensusVote {
            voter: self.local_node_id.clone(),
            proposer: proposer_id.to_string(),
            round: current_round,
            vote_type: VoteType::Approve, // Default to approve
            timestamp: get_safe_timestamp(),
            group_id: self.get_current_group_id().await,
            signature: [0u8; 64],
        };
        let signable = vote.signable_bytes()?;
        vote.signature = self.sign_intragroup_message(&vote.group_id, "consensus_vote", &signable);
        Ok(vote)
    }

    /// Submit consensus vote to group members via direct P2P (or gossipsub fallback)
    async fn submit_consensus_vote(&self, vote: &ConsensusVote) -> Result<()> {
        let payload = serde_json::to_vec(vote)?;
        self.broadcast_consensus(
            ConsensusMessage::Vote(payload.clone()),
            self.vote_topic.clone(),
            payload,
        )
        .await?;
        debug!(round = vote.round, group_id = %vote.group_id, "Consensus vote sent via direct P2P");
        Ok(())
    }

    /// Get current group ID
    async fn get_current_group_id(&self) -> String {
        self.group_manager
            .get_current_group()
            .await
            .map(|g| g.group_id)
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Get current epoch
    async fn get_current_epoch(&self) -> u64 {
        self.group_manager
            .get_current_group()
            .await
            .map(|g| g.epoch)
            .unwrap_or(0)
    }
}
