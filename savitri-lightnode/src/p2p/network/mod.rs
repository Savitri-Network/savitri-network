//! P2P Network module for lightnode.
//!
//! Sub-modules:
//! - `helpers` — TX deserialization, monolith verification, bootstrap requests

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]

// pub mod helpers; // Extracted functions (duplicated). Uncomment after removing from this file.

use crate::config::Config;
use crate::p2p::block::{register_block_in_dag, MempoolPipeline};
use crate::p2p::commit_scheduler::CommitScheduler;
use crate::p2p::conflict_keys;
use crate::p2p::dag::DagManager;
use anyhow::{Context, Result};
use ed25519_dalek::VerifyingKey as PublicKey;
use futures::FutureExt;
use hex;
use libp2p::futures::StreamExt;
use libp2p::{
    core::connection::ConnectedPoint,
    gossipsub::{Event as GossipsubEvent, IdentTopic},
    kad::{Quorum, Record, RecordKey},
    swarm::{Swarm, SwarmEvent},
    Multiaddr, PeerId,
};
#[cfg(feature = "metrics")]
use metrics::{counter, gauge};
use savitri_core::crypto::Keypair;

/// Maximum allowed size for network transaction deserialization (1 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized network payloads.
const MAX_NETWORK_TX_SIZE: usize = 1 * 1024 * 1024;

fn bytes_to_raw_tx(bytes: Vec<u8>, _peer_id: Option<u64>) -> Result<crate::tx::SignedTx> {
    if bytes.len() > MAX_NETWORK_TX_SIZE {
        anyhow::bail!(
            "Network transaction data too large: {} bytes (max {})",
            bytes.len(),
            MAX_NETWORK_TX_SIZE
        );
    }
    // Use deserialize_signed_tx which uses the canonical fixint encoding,
    crate::tx::deserialize_signed_tx(&bytes)
}

use crate::availability::{PouScoring, ScoreComponents};
use crate::p2p::types::{
    decode_consensus, decode_consensus_cert_from_masternode, decode_gossip, decode_request,
    decode_response, BlockWithCertificateWire, BootstrapReply, BootstrapRequest, ConnectionPool,
    ConsensusMessage, GossipMessage, HandshakeResult, HaveTx, HeartbeatKind, HeartbeatMessage,
    PeerInfo, RequestMessage, ResponseMessage, TxMessage,
};
use crate::resource::{FixedPoint, MAX as FP_MAX};
use crate::storage::BlockAndAccountStorage;
use crate::tx::SignedTx;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{mpsc, Mutex, RwLock},
};
use tracing::{debug, error, info, trace, warn};

use crate::{
    availability::HeartbeatEvent,
    logging::{flagged_message, FLAG_BLOCK_ATTEMPT, FLAG_MASTERNODE},
    resource::{emit_event, ResourceEvent, TrafficDirection},
};

use super::group_manager::{GroupAnnounce, P2PGroupManager};
use super::intra_group::{IntraGroupCommunication, PouScoreAck, PouScoreShare};
use super::periodic_tasks::PeriodicTaskManager;
use super::transport;
use super::types::BlockReceiver;
use crate::integrity::{self, IntegrityKind};
use crate::p2p::network_tasks::{run_logging_task, run_maintenance_task, run_publish_aggregator};
use crate::p2p::swarm_commands::{NetworkEvent, SwarmCommand};

fn peer_id_to_u64(peer_id: &PeerId) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    peer_id.hash(&mut hasher);
    hasher.finish()
}

/// Convert bind address (0.0.0.0) to a dialable address for registration.
/// If `external_ip` is set it replaces the IP4 component of the multiaddr so that
/// peers use the configured address.  Without `external_ip` the old behaviour is
/// preserved: 0.0.0.0 → 127.0.0.1 (single-machine testing).
/// If port is 0 (invalid for dial), fallback_port (configured listen port) is used.
fn make_dialable_addr(addr: &str, fallback_port: u16, external_ip: Option<&str>) -> String {
    let s = if let Some(ext) = external_ip {
        // Replace ANY /ip4/<addr> with the configured external IP.
        // This handles 0.0.0.0, 127.0.0.1, and auto-detected IPs (e.g. Hyper-V 192.168.64.1).
        if let Ok(parsed) = addr.parse::<Multiaddr>() {
            let mut out = Multiaddr::empty();
            for proto in parsed.iter() {
                match proto {
                    libp2p::multiaddr::Protocol::Ip4(_) => {
                        if let Ok(ip) = ext.parse::<std::net::Ipv4Addr>() {
                            out.push(libp2p::multiaddr::Protocol::Ip4(ip));
                        } else {
                            out.push(proto);
                        }
                    }
                    other => out.push(other),
                }
            }
            out.to_string()
        } else {
            // Fallback: simple string replacement
            addr.replace("0.0.0.0", ext).replace("127.0.0.1", ext)
        }
    } else {
        addr.replace("0.0.0.0", "127.0.0.1")
    };
    if s.contains("/tcp/0") {
        s.replace("/tcp/0", &format!("/tcp/{}", fallback_port))
    } else {
        s
    }
}

/// Quando Docker bridge (172.17.x.x) interferisce con detect_outbound_ip o
/// quando peer_info gossipped contiene IP RFC1918 cached pre-wipe, l'IP non
/// pubblico finisce nei dial retry storm. Rendere `pub` consente al modulo
/// `broadcast` di filtrare l'OUTBOUND announce e a chi processa peer_info di
/// scartare entry non dialabili in INBOUND.
pub fn is_local_or_private_multiaddr(addr: &Multiaddr) -> bool {
    addr.iter().any(|proto| match proto {
        libp2p::multiaddr::Protocol::Ip4(ip) => {
            ip.is_loopback() || ip.is_private() || ip.is_unspecified() || ip.is_link_local()
        }
        libp2p::multiaddr::Protocol::Ip6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unicast_link_local()
                || ip.is_unique_local()
        }
        _ => false,
    })
}

fn should_accept_discovered_masternode_addr(addr: &Multiaddr, prefer_public_addrs: bool) -> bool {
    !prefer_public_addrs || !is_local_or_private_multiaddr(addr)
}

/// Build a registration/advertised address that always uses the configured listen port.
/// This avoids leaking ephemeral outbound TCP source ports (e.g. from Identify observed_addr)
/// into registration data consumed by masternode group announcements.
fn normalize_registration_addr(addr: &str, listen_port: u16, external_ip: Option<&str>) -> String {
    let s = make_dialable_addr(addr, listen_port, external_ip);
    if let Ok(parsed) = s.parse::<Multiaddr>() {
        let mut out = Multiaddr::empty();
        let mut replaced_tcp = false;
        for proto in parsed.iter() {
            match proto {
                libp2p::multiaddr::Protocol::Tcp(_) if !replaced_tcp => {
                    out.push(libp2p::multiaddr::Protocol::Tcp(listen_port));
                    replaced_tcp = true;
                }
                other => out.push(other),
            }
        }
        if replaced_tcp {
            return out.to_string();
        }
    }
    s
}

fn fnv1a_hash(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325u64 ^ seed;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn choose_anchor_peer(peers: &[BootstrapPeer], seed: u64) -> Option<PeerId> {
    let mut best: Option<(u64, PeerId)> = None;
    for peer in peers.iter().filter(|p| p.priority) {
        let hash = fnv1a_hash(&peer.peer_id.to_bytes(), seed);
        match &best {
            Some((best_hash, _)) if *best_hash <= hash => {}
            _ => best = Some((hash, peer.peer_id.clone())),
        }
    }
    best.map(|(_, pid)| pid)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GroupFormedAck {
    group_id: String,
    epoch: u64,
    peer_id: String,
    timestamp: u64,
    connected_peers: usize,
    total_peers: usize,
}

fn ack_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn choose_anchor_from_ids<'a, I>(peer_ids: I, seed: u64) -> Option<PeerId>
where
    I: IntoIterator<Item = &'a PeerId>,
{
    let mut best: Option<(u64, PeerId)> = None;
    for peer_id in peer_ids {
        let hash = fnv1a_hash(&peer_id.to_bytes(), seed);
        match &best {
            Some((best_hash, _)) if *best_hash <= hash => {}
            _ => best = Some((hash, peer_id.clone())),
        }
    }
    best.map(|(_, pid)| pid)
}

fn put_kad_record(
    swarm: &mut Swarm<crate::p2p::types::MyBehaviour>,
    key: &str,
    value: Vec<u8>,
) -> Result<()> {
    let record = Record {
        key: RecordKey::new(&key),
        value,
        publisher: None,
        expires: None,
    };
    swarm
        .behaviour_mut()
        .kademlia
        .put_record(record, Quorum::One)
        .map_err(|e| anyhow::anyhow!("failed to put kademlia record {key}: {e}"))?;
    Ok(())
}

use super::{
    block::{finalize_remote_block_commit, prepare_remote_block},
    block_sync::{BlockSyncManager, BlockSyncRequest, BlockSyncResponse},
    bootstrap::{
        build_bootstrap_reply, handle_bootstrap_reply, parse_bootstrap, publish_bootstrap_reply,
        request_bootstrap_snapshot,
    },
    broadcast::{
        broadcast_block_to_lightnodes, broadcast_heartbeat, broadcast_lightnode_registration,
        broadcast_lightnode_registration_sync, broadcast_peer_info, broadcast_peer_info_sync,
        broadcast_signed_tx, send_peer_info_to_masternode,
    },
    certificate::{
        log_certificate_finality, validate_certificate, validate_certificate_masternode,
        CertificatePendingBlocks,
    },
    helpers::{maybe_redial_priority, total_voters, update_peer_directory},
    pou::PouState,
    sync::initial_bootstrap_sync,
    transport::{build_gossipsub, build_transport, load_or_generate_identity},
    types::{BlockBroadcast, BlockPrepError, BootstrapPeer, PouBroadcast},
};
use crate::p2p::intra_group::{PeerDiscoveryResponse, PeerRegistryAnnounce};

pub struct NetworkComponents {
    pub tx_sender: mpsc::Sender<SignedTx>,
    pub have_tx_sender: mpsc::Sender<HaveTx>,
    pub tx_forward_sender: mpsc::Sender<SignedTx>,
    pub local_peer: PeerId,
    pub heartbeat_sender: mpsc::Sender<crate::p2p::types::HeartbeatMessage>,
    pub pou_sender: mpsc::Sender<crate::p2p::types::PouBroadcast>,
    pub block_sender: mpsc::Sender<(
        crate::p2p::types::BlockBroadcast,
        crate::p2p::types::PendingBlockData,
    )>,
    pub pou_state: Arc<RwLock<crate::p2p::pou::PouState>>,
    pub peer_accounts: Arc<RwLock<Vec<[u8; 32]>>>,
    pub dag_manager: Arc<DagManager>,
    pub command_tx: mpsc::Sender<SwarmCommand>,
    pub network_events: tokio::sync::broadcast::Sender<NetworkEvent>,
    pub listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
    pub observed_addr: Arc<RwLock<String>>,
    pub connected_peers: Arc<RwLock<HashSet<PeerId>>>,
    pub tx_store: crate::p2p::tx_fetch_protocol::TxStore,
    pub task: tokio::task::JoinHandle<()>,
    /// P1 (shard-aware TX dispatch): map shard_id→group_id populated from
    /// group-announce handler. Used by LightnodeTxRouter (rpc feature only) to
    /// forward cross-group TX to the right group's intra-group topic instead of
    /// admitting locally.
    pub shard_to_group: Arc<tokio::sync::RwLock<std::collections::HashMap<u32, String>>>,
    /// P1: total shard count (global constant, same across all announces).
    /// Atomic u32 so the router can read cheaply on every TX.
    pub num_shards: Arc<std::sync::atomic::AtomicU32>,
    /// P1: cloned handle to the group_manager so main.rs can build a local_group
    /// closure for LightnodeTxRouter without re-finding the manager.
    pub group_manager: Arc<super::group_manager::P2PGroupManager>,
}

pub async fn start_network(
    config: Config,
    keypair: Keypair,
    producer_addr: [u8; 32],
    effective_reward_address: Option<[u8; 32]>,
    storage: Arc<dyn crate::storage::BlockAndAccountStorageTrait>,
    block_receiver: BlockReceiver,
    mut certificate_receiver: mpsc::Receiver<crate::p2p::types::ConsensusCertificate>,
    mut integrity_receiver: mpsc::Receiver<crate::integrity::IntegrityEvent>,
    mut pou_receiver: mpsc::Receiver<crate::p2p::types::PouBroadcast>,
    resource_event_sender: Option<mpsc::Sender<ResourceEvent>>,
    heartbeat_event_sender: Option<mpsc::Sender<HeartbeatEvent>>,
    integrity_event_sender: Option<mpsc::Sender<crate::integrity::IntegrityEvent>>,
    mempool_pipeline: Option<crate::p2p::block::LightnodeMempoolHandle>,
    is_intragroup_proposer: Option<std::sync::Arc<tokio::sync::RwLock<bool>>>,
    shared_pou_score: Option<std::sync::Arc<tokio::sync::RwLock<Option<u16>>>>,
    is_in_intra_group: Option<std::sync::Arc<tokio::sync::RwLock<bool>>>,
    // Optional shared PoU observation store. When supplied, the network
    // task wires it into the intra-group communication for latency
    // samples; the lightnode binary should also pass the same `Arc` to
    // `MempoolPipeline::set_fl_score_sink` so FL scores land in the
    // same store. When `None`, a fresh store is created here (used by
    // older callers / tests that don't need cross-component sharing).
    pou_observations_external: Option<std::sync::Arc<savitri_consensus::scoring::ObservationStore>>,
    // it when GroupAnnouncement gossip arrives so the tx_router can hit
    // the cache for cross-group direct-send. `None` for tests / callers
    // that don't need cross-group routing.
    proposer_cache_external: Option<crate::tx_router::peer_lookup::ProposerCache>,
) -> Result<NetworkComponents> {
    let local_pubkey = keypair.verifying_key().to_bytes();
    let local_peer_id = PeerId::from_public_key(&libp2p::identity::PublicKey::from(
        libp2p::identity::ed25519::PublicKey::try_from_bytes(&local_pubkey)?,
    ));
    let local_node_id = local_peer_id.to_string();
    let signing_key = Arc::new(keypair);

    // Account sent in LightnodeRegistration: reward/payout address when set, else producer (Ethereum/Solana style)
    let registration_reward_account = effective_reward_address.unwrap_or(producer_addr);

    info!("Starting P2P network - peer_id: {}", local_peer_id);

    let listen_port = config.listen_port;
    let bootstrap = config.bootstrap_peers.clone();
    let masternode_peers = config.masternode_peers.clone();
    let local_account = local_pubkey;
    // Use configured external_ip if present; otherwise auto-detect the outbound IP
    // so peer registrations and announcements carry the real machine address instead
    // of 127.0.0.1 or 0.0.0.0.
    let external_ip: Option<String> = config
        .external_ip
        .clone()
        .or_else(|| transport::detect_outbound_ip().map(|ip| ip.to_string()));
    match &external_ip {
        Some(ip) => info!("External IP for peer registration: {}", ip),
        None => warn!("external_ip not set and auto-detection failed; remote peers may not be able to dial this node"),
    }

    // into P2PGroupManager so its MAX_EPOCH_DRIFT guard uses the same
    // formula as the masternode. genesis_timestamp_ms falls back to the
    // SAVITRI_GENESIS_TIMESTAMP_MS env var when not set in config.
    let genesis_ms_for_groups: u64 = config
        .genesis_timestamp_ms
        .or_else(|| {
            std::env::var("SAVITRI_GENESIS_TIMESTAMP_MS")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or(0);
    if genesis_ms_for_groups == 0 {
        warn!("genesis_timestamp_ms not configured; epoch drift guard will degrade to legacy semantics");
    }
    let heartbeat_ms_for_groups: u64 = config.heartbeat_interval_secs.saturating_mul(1000).max(1);
    let slots_per_epoch_for_groups: u64 = config.slots_per_epoch.max(1) as u64;
    let group_manager = Arc::new(P2PGroupManager::new(
        local_node_id.clone(),
        genesis_ms_for_groups,
        heartbeat_ms_for_groups,
        slots_per_epoch_for_groups,
    ));
    let dag_manager = Arc::new(DagManager::new());

    // P1: shard→group map + num_shards. Populated from group-announce handler.
    // LightnodeTxRouter reads these to route RPC-submitted TX to the right
    // group's intra-group topic when the TX's sender shard belongs to another
    // group (avoiding the rot-in-local-mempool failure mode).
    let shard_to_group: Arc<tokio::sync::RwLock<std::collections::HashMap<u32, String>>> =
        Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let num_shards = Arc::new(std::sync::atomic::AtomicU32::new(0));
    // Clones for the NetworkComponents return (the originals are moved into
    // the group-announce handler closure below).
    let shard_to_group_for_return = shard_to_group.clone();
    let num_shards_for_return = num_shards.clone();
    let group_manager_for_return = group_manager.clone();

    let libp2p_keypair =
        libp2p::identity::Keypair::ed25519_from_bytes(signing_key.to_bytes().to_vec())?;
    let mut swarm: libp2p::swarm::Swarm<crate::p2p::types::MyBehaviour> =
        transport::initialize_swarm(&config, libp2p_keypair).await?;

    let (tx_sender, mut tx_receiver) = mpsc::channel::<SignedTx>(1000);
    let (have_tx_sender, mut have_tx_receiver) = mpsc::channel::<HaveTx>(1000);

    let intra_group_gossipsub = build_gossipsub(libp2p::identity::Keypair::ed25519_from_bytes(
        signing_key.to_bytes().to_vec(),
    )?)?;
    let (intra_publish_tx, mut intra_publish_rx) =
        mpsc::channel::<(libp2p::gossipsub::IdentTopic, Vec<u8>)>(64);

    // V0.2 Phase 1 (Score Canonicity, issue #31): canon publisher needs an independent
    // clone of the publish sink. The original handle is consumed by the IGC constructor.
    let intra_publish_tx_clone_for_canon = intra_publish_tx.clone();
    // V0.2 Phase 2 (Lattice ordering, issue #32): the LatticeRuntime
    // publisher needs its own clone of the publish sink so cell + attestation
    // broadcasts can run alongside the existing intra-group topics.
    let intra_publish_tx_clone_for_lattice = intra_publish_tx.clone();
    let pou_scoring = shared_pou_score
        .clone()
        .map(|s| Arc::new(crate::availability::PouScoring::with_shared(s)));
    // Canale per ACK whitelist dal MN (handshake 3-fasi)
    // Unbounded: the producer rate is naturally bounded (5 MNs × 1 ACK per election
    // round the consumer initiates). The consumer drains lazily (only when this node
    // during non-proposer periods the ACKs meant for this node (if any) accumulate
    // harmlessly instead of filling a bounded buffer and causing storm warnings.
    // Safety: memory use is O(rounds × 5 × ~200B) — negligible for the tenure length.
    let (whitelist_ack_tx, whitelist_ack_rx) =
        mpsc::unbounded_channel::<crate::p2p::intra_group::ProposerWhitelistAck>();
    // Canale per pubblicare blocco su block_topic (proposer intra_group -> MN cache per block_final)
    let (block_broadcast_only_tx, block_broadcast_only_rx) = mpsc::channel::<BlockBroadcast>(64);
    // so we can check proposer status in the grace period logic later.
    let is_intragroup_proposer_for_grace = is_intragroup_proposer.clone();
    // Shared PoU observation store. Receives RTT samples from group pongs,
    // so the PoU scorer can build a real per-peer trust score. Use the
    // store passed in by the caller (so it's the same instance the FL
    // sink in MempoolPipeline writes to); fall back to a fresh store
    // when no caller wired one.
    let pou_observations = pou_observations_external
        .clone()
        .unwrap_or_else(|| Arc::new(savitri_consensus::scoring::ObservationStore::new()));

    let intra_group_comm = Arc::new(RwLock::new(IntraGroupCommunication::new(
        local_node_id.clone(),
        group_manager.clone(),
        signing_key.clone(),
        None,
        pou_scoring,
        Arc::new(RwLock::new(intra_group_gossipsub)),
        Some(intra_publish_tx),
        mempool_pipeline.clone(),
        Some(storage.clone()),
        is_intragroup_proposer,
        Some(block_broadcast_only_tx),
        None, // network_direct_tx set below after command_tx is created
        Some(Arc::clone(&dag_manager)),
    )));

    // Wire the shared observation store so handle_group_pong can record RTT.
    intra_group_comm
        .write()
        .await
        .set_observations(Arc::clone(&pou_observations));

    // V0.2 Phase 1 (Score Canonicity, issue #31): create the canonical RTT
    // state holder, attach it to the IGC, and spawn the periodic publisher
    // task. The publisher pulls observations from the same ObservationStore
    // wired above, signs reports, and gossipsub-publishes on the per-group
    // canon topic every LATENCY_CANON_PUBLISH_INTERVAL_SECS (10s).
    let latency_canon_state =
        std::sync::Arc::new(crate::latency_canon_state::LatencyCanonState::new());
    intra_group_comm
        .write()
        .await
        .set_latency_canon_state(latency_canon_state.clone());
    {
        let gm_for_publisher = group_manager.clone();
        let igc_for_round = intra_group_comm.clone();
        crate::latency_canon_publisher::spawn_publisher(
            crate::latency_canon_publisher::LatencyCanonPublisherConfig {
                local_peer_id: local_node_id.clone(),
                signing_key: signing_key.clone(),
                observations: Arc::clone(&pou_observations),
                network_publish_tx: intra_publish_tx_clone_for_canon.clone(),
            },
            move || {
                let g = gm_for_publisher.get_current_group_cached()?;
                // V0.2 Phase 2 (latency table convergence): use a wall-clock
                // aligned bucket as the `round` field on the report instead
                // of last_certified_height. All LNs sharing a synced clock
                // land in the same bucket, so the aggregator window filter
                // produces a byte-identical canonical table across observers.
                // The legacy `last_certified_height` based round broke
                // determinism because chain head lag varies per-observer.
                let _ = &igc_for_round; // retained for future per-group counters
                let bucket = crate::latency_canon_publisher::current_wall_clock_bucket();
                Some((g.group_id, bucket))
            },
        );
    }

    // V0.2 Phase 1: periodic DIAG snapshot of the canonical table (every 10s).
    // Operators grep "DIAG[latency-canon]" to see the table converge.
    {
        let igc_for_diag = intra_group_comm.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await;
            loop {
                tick.tick().await;
                let c = igc_for_diag.read().await;
                let _ = c.rebuild_latency_canon_table().await;
            }
        });
    }

    // V0.2 Phase 2 (Lattice ordering runtime, issue #32): construct the
    // LatticeRuntime and spawn its periodic publisher + commit poller.
    // The runtime owns the LatticeAggregator behind an Arc<RwLock<>>;
    // we keep a clone of the Arc<LatticeRuntimeState> so the gossipsub
    // receive branch can call the static process_*_message helpers
    // without going through the owning value.
    let mut lattice_runtime = crate::lattice_runtime::LatticeRuntime::new(
        local_node_id.clone(),
        signing_key.clone(),
        intra_publish_tx_clone_for_lattice.clone(),
        crate::lattice_runtime::LatticeRuntimeConfig::default(),
        mempool_pipeline.as_ref().map(|p| p.inner_for_rpc()),
    );

    // P2.6-C.2 Phase A: shadow chain consumer wiring.
    //   - bounded mpsc channel for CommittedLatticeBlock values.
    //   - set_chain_sink BEFORE spawn_tasks so Arc::get_mut works.
    //   - spawn lattice_chain_consumer_loop on the receiver. Writes
    //     a JSONL audit file when SAVITRI_LATTICE_SHADOW_AUDIT is set;
    //     otherwise idles waiting for messages that never come.
    let (chain_sink_tx, chain_sink_rx) =
        tokio::sync::mpsc::channel::<crate::lattice_runtime::CommittedLatticeBlock>(64);
    if !lattice_runtime.set_chain_sink(chain_sink_tx) {
        tracing::warn!(
            "P2.6-C.2: set_chain_sink failed (Arc already cloned) - shadow consumer will not receive blocks"
        );
    }
    tokio::spawn(crate::lattice_runtime::lattice_chain_consumer_loop(chain_sink_rx));

    let lattice_runtime_state = lattice_runtime.state();
    {
        let gm_for_provider = group_manager.clone();
        let lcs_for_provider = latency_canon_state.clone();
        lattice_runtime.spawn_tasks(move || {
            let g = gm_for_provider.get_current_group_cached()?;
            // A.6b: build ranked_pou using the canonical
            // LatencyCanonState (Phase 1 V0.2) instead of the
            // locally-observed member_pou_scores used pre-A.6b.
            //
            // Why canonical, not PoU:
            //   member_pou_scores is computed independently on every
            //   LN from local heartbeat observations. Two LNs in the
            //   same group will see different scores for the same
            //   peer, so pivot_for_cycle(group, cycle, ranked) would
            //   elect different pivots on different LNs even for
            //   the same (group, cycle). When P2.6-C.2 Phase B.2
            //   broadcasts Block from the pivot, that divergence
            //   would fork the per-group chain. LatencyCanonState,
            //   by contrast, is rebuilt against current_wall_clock_bucket
            //   (see intra_group/mod.rs) so all LNs that have
            //   ingested the same reports produce byte-identical
            //   tables — pivot election becomes cluster-deterministic.
            //
            //   PoU is NOT removed from the system: it still drives
            //   V0.1 BFT election, rewards, member ranking, etc.
            //   This change only swaps the score source for Lattice
            //   pivot election, where determinism dominates over
            //   locally-observed quality.
            //
            // Cold-start: if g.members is empty (MN GroupAnnouncement
            // not yet propagated) we return None so the publisher
            // skips this tick. That is safer than falling back to
            // member_pou_scores because the fallback would reintroduce
            // non-determinism precisely during the window the cluster
            // is most likely to disagree. The publisher resumes as
            // soon as g.members populates (a few seconds post-boot).
            //
            // lookup_score returns 1000 (neutral) for peers without a
            // canonical bucket yet, so during the bootstrap window
            // all members tie on score and the canonical secondary
            // ordering (peer_id ascending, same as determine_proposer)
            // takes over deterministically.
            if g.members.is_empty() {
                return None;
            }
            let mut ranked: Vec<(String, u32)> = g
                .members
                .iter()
                .map(|m| {
                    let score = lcs_for_provider.lookup_score(&g.group_id, m) as u32;
                    (m.clone(), score)
                })
                .collect();
            ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            Some((g.group_id, ranked))
        });
    }

    // Initialize last_certified_height from persistent storage/DAG at boot so the
    // pipeline starts from the correct height instead of 0 (prevents chain stall after restart).
    intra_group_comm
        .read()
        .await
        .initialize_certified_height_from_storage()
        .await;

    // Set il receiver ACK nell'IntraGroupCommunication
    intra_group_comm
        .read()
        .await
        .set_whitelist_ack_rx(whitelist_ack_rx);

    let periodic_task_manager = Arc::new(PeriodicTaskManager::new(
        local_node_id.clone(),
        group_manager.clone(),
        intra_group_comm.clone(),
    ));

    // Keep is_in_intra_group in sync with group_manager so main block producer can skip draining when we're in a group
    if let Some(ref flag) = is_in_intra_group {
        let gm = group_manager.clone();
        let flag_clone = flag.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let in_group = gm.get_current_group().await.is_some();
                *flag_clone.write().await = in_group;
            }
        });
    }

    // Periodic DAG pruning to bound memory usage (keep last 100 blocks)
    {
        let dag_prune = Arc::clone(&dag_manager);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let max_h = dag_prune.get_max_height().await;
                if max_h > 100 {
                    dag_prune.prune_below(max_h - 100).await;
                }
            }
        });
    }

    {
        let subscribed_topic = group_manager.get_topic();
        swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&subscribed_topic)?;
        info!(
            topic = %subscribed_topic.hash(),
            topic_string = %subscribed_topic,
            "✅ Subscribed to group announcement topic - waiting for masternode messages"
        );

        // Log periodico per verificare che stiamo ancora ascoltando
        let topic_hash = subscribed_topic.hash();
        let topic_string = subscribed_topic.to_string();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                debug!(
                    topic = %topic_hash,
                    topic_string = %topic_string,
                    "📡 Still listening for group announcements from masternode..."
                );
            }
        });
    }

    let tx_topic = IdentTopic::new("/savitri/tx/1");
    let block_topic = IdentTopic::new("/savitri/block/1");
    let block_masternode_topic = IdentTopic::new("/savitri/block/masternode/1");
    let peer_info_topic = IdentTopic::new("/savitri/peerinfo/1");
    let registration_topic = IdentTopic::new("/savitri/registration/1");
    let peer_registry_topic = IdentTopic::new("/savitri/peer_registry/1");
    let heartbeat_topic = IdentTopic::new("/savitri/heartbeat/1");
    let pou_topic = IdentTopic::new("/savitri/pou/1");
    let bootstrap_req_topic = IdentTopic::new("/savitri/bootstrap/req/1");
    let bootstrap_resp_topic = IdentTopic::new("/savitri/bootstrap/resp/1");
    let monolith_topic = IdentTopic::new("/savitri/monolith/announce/1");
    let monolith_receipt_topic = IdentTopic::new("/savitri/monolith/receipt/1");
    let monolith_req_topic = IdentTopic::new("/savitri/req/monolith/1");
    let monolith_resp_topic = IdentTopic::new("/savitri/resp/monolith/1");
    let consensus_cert_topic = IdentTopic::new("/savitri/consensus/cert/1");
    let block_final_topic = IdentTopic::new("/savitri/block_final/1");
    let masternode_proposal_topic = IdentTopic::new("/savitri/masternode/proposal/1");
    let group_formed_topic = IdentTopic::new("/savitri/lightnode/group/formed/1");
    let intra_group_tx_topic = intra_group_comm.read().await.get_tx_topic();
    let intra_group_latency_topic = intra_group_comm.read().await.get_latency_topic();
    let intra_group_pou_topic = intra_group_comm.read().await.get_pou_topic();
    let intra_group_pou_ack_topic = intra_group_comm.read().await.get_pou_ack_topic();
    let intra_group_ping_topic = intra_group_comm.read().await.get_ping_topic();
    let intra_group_pong_topic = intra_group_comm.read().await.get_pong_topic();
    let intra_group_election_topic = intra_group_comm.read().await.get_election_topic();
    let intra_group_proposal_topic = intra_group_comm.read().await.get_proposal_topic();
    let intra_group_vote_topic = intra_group_comm.read().await.get_vote_topic();

    swarm.behaviour_mut().gossipsub.subscribe(&tx_topic)?;
    swarm.behaviour_mut().gossipsub.subscribe(&block_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&heartbeat_topic)?;
    swarm.behaviour_mut().gossipsub.subscribe(&pou_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&registration_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&bootstrap_req_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&bootstrap_resp_topic)?;
    swarm.behaviour_mut().gossipsub.subscribe(&monolith_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&monolith_receipt_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&monolith_req_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&monolith_resp_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&consensus_cert_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&block_final_topic)?;
    // Subscribe al topic proposal MN per garantire formazione mesh gossipsub LN<->MN
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&masternode_proposal_topic)?;
    info!("Subscribed to masternode proposal topic for mesh formation: /savitri/masternode/proposal/1");
    // Handshake 3-fasi: subscribe ai topic per certificato elezione e ACK whitelist
    let election_cert_topic = IdentTopic::new("/savitri/masternode/election/cert/1");
    let election_ack_topic = IdentTopic::new("/savitri/masternode/election/ack/1");
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&election_cert_topic)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&election_ack_topic)?;
    info!("Subscribed to proposer election handshake topics: election/cert + election/ack");

    // Dynamic MN discovery: subscribe to peer registry + peer discovery topics
    // peer_registry: real-time announcements from new MNs joining the network
    // peer_discovery: pull-based fallback to request known MN list from existing MNs
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&peer_registry_topic)?;
    let peer_discovery_topic = IdentTopic::new("/savitri/peer_discovery/1");
    swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&peer_discovery_topic)?;
    info!("Subscribed to dynamic MN discovery topics: peer_registry + peer_discovery");

    // Intra-group tx (and other) topics are subscribed after group assignment (per-group dynamic topics)

    let listen_addr: Multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}").parse()?;
    Swarm::listen_on(&mut swarm, listen_addr.clone())?;

    let mut merged: HashMap<PeerId, BootstrapPeer> = HashMap::new();
    for peer in parse_bootstrap(&bootstrap, false)?
        .into_iter()
        .chain(parse_bootstrap(&masternode_peers, true)?)
    {
        merged
            .entry(peer.peer_id.clone())
            .and_modify(|existing| {
                if existing.account.is_none() {
                    existing.account = peer.account;
                }
                existing.priority |= peer.priority;
            })
            .or_insert(peer);
    }
    let peers: Vec<BootstrapPeer> = merged.into_values().collect();
    let prefer_public_masternode_addrs = peers
        .iter()
        .filter(|p| p.priority)
        .any(|p| !is_local_or_private_multiaddr(&p.addr));
    let priority_targets: Arc<RwLock<HashMap<PeerId, Multiaddr>>> = Arc::new(RwLock::new(
        peers
            .iter()
            .filter(|p| p.priority)
            .map(|p| (p.peer_id.clone(), p.addr.clone()))
            .collect(),
    ));

    let p2p_group_peers: HashSet<PeerId> = peers
        .iter()
        .filter(|p| !p.priority)
        .map(|p| p.peer_id.clone())
        .collect();

    if !priority_targets.read().await.is_empty() {
        info!(
            count = priority_targets.read().await.len(),
            "{}",
            flagged_message(FLAG_MASTERNODE, "Tracking priority masternode peers")
        );
    }

    let seed = peer_id_to_u64(&local_peer_id);
    let anchor_peer_id = choose_anchor_peer(&peers, seed);
    if let Some(anchor) = &anchor_peer_id {
        info!(
            peer = %anchor,
            seed,
            "{}",
            flagged_message(FLAG_MASTERNODE, "Selected deterministic anchor masternode for this node")
        );
    }

    for peer in &peers {
        debug!("Connecting to peer-node {} at {}", peer.peer_id, peer.addr);

        let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer.peer_id)
            .addresses(vec![peer.addr.clone()])
            .build();

        if let Err(err) = swarm.dial(dial_opts) {
            warn!("dial error to {}: {err}", peer.peer_id);
        } else {
            if peer.priority {
                // DO NOT add MN as explicit_peer: gossipsub treats explicit peers
                // specially — it rejects their GRAFT requests and never adds them
                // to the mesh. This prevents the MN's subscriptions from being
                // visible in peers_on_topic, so flood_publish never sends to them.
                // Instead we rely on normal gossipsub mesh formation + flood_publish(true)
                // and the priority_targets reconnection logic for connection stability.
                info!(
                    peer = %peer.peer_id,
                    address = %peer.addr,
                    is_anchor = (anchor_peer_id.as_ref() == Some(&peer.peer_id)),
                    "{}",
                    flagged_message(
                        FLAG_MASTERNODE,
                        "Dialing priority masternode peer (normal mesh peer, not explicit)"
                    )
                );
            } else {
                debug!(
                    peer = %peer.peer_id,
                    address = %peer.addr,
                    "Dialing non-priority peer"
                );
            }
        }
    }

    let bootstrap_accounts: Vec<(PeerId, [u8; 32])> = peers
        .iter()
        .filter_map(|peer| peer.account.map(|acct| (peer.peer_id.clone(), acct)))
        .collect();

    let connection_pool = Arc::new(Mutex::new(ConnectionPool::new()));
    let (tx_sender_clone, tx_rx) = mpsc::channel::<SignedTx>(1024);
    let (tx_forward_sender, mut tx_forward_rx) = mpsc::channel::<SignedTx>(1024);
    let (block_sender, block_rx) =
        mpsc::channel::<(BlockBroadcast, crate::p2p::types::PendingBlockData)>(128);
    let (heartbeat_sender, heartbeat_rx) = mpsc::channel::<HeartbeatMessage>(1024);
    let (pou_sender, pou_rx) = mpsc::channel::<PouBroadcast>(128);
    let certificate_pending = Arc::new(Mutex::new(CertificatePendingBlocks::new()));

    if let Some(pipeline_arc) = mempool_pipeline.as_ref() {
        let pipeline_for_cb = Arc::clone(pipeline_arc);
        let cb = std::sync::Arc::new(move |hashes: &[[u8; 64]]| {
            // evict_stale_entries holds the CertificatePendingBlocks Mutex;
            // we must NOT block on the mempool lock here or we'd deadlock if
            // another task is waiting for the cert-pending mutex while
            // holding the mempool mutex. Spawn a task to restore async.
            let hashes_owned: Vec<[u8; 64]> = hashes.to_vec();
            let pipeline = Arc::clone(&pipeline_for_cb);
            tokio::spawn(async move {
                let mut total = 0usize;
                for h in &hashes_owned {
                    total += pipeline.restore_in_flight_for_block(h);
                }
                if total > 0 {
                    tracing::warn!(
                        evicted_blocks = hashes_owned.len(),
                        restored_txs = total,
                        "Restored in-flight TXs from evicted (uncertified) blocks"
                    );
                }
            });
        });
        certificate_pending.lock().await.set_eviction_callback(cb);
    }
    let commit_scheduler = Arc::new(Mutex::new(CommitScheduler::new(Arc::clone(&storage))));

    let priority_peer_ids: std::collections::HashSet<PeerId> =
        priority_targets.read().await.keys().cloned().collect();

    if !priority_peer_ids.is_empty() {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // Clone the snapshot for bootstrap sync (lock is not held across await)
        let priority_targets_snapshot = priority_targets.read().await.clone();
        match initial_bootstrap_sync(
            &mut swarm,
            Arc::clone(&storage),
            &bootstrap_req_topic,
            &bootstrap_resp_topic,
            &priority_peer_ids,
            &priority_targets_snapshot,
        )
        .await
        {
            Ok(Some(height)) => {
                info!(
                    height,
                    "Successfully synced with masternode before starting operations"
                );
            }
            Ok(None) => {
                info!("Bootstrap reply received; no new blocks (chain empty or already synced)");
            }
            Err(err) => {
                warn!("Bootstrap sync failed: {err}. Will retry in background...");
            }
        }
    } else {
        let chain_head = storage
            .get_chain_head()
            .context("Failed to check chain head from storage")?;

        if chain_head.is_none() {
            warn!("No masternode peers configured and chain not initialized; lightnode may not sync properly");
            info!("Chain state: Empty - needs initial sync from masternode");
        } else {
            let head_block = chain_head.unwrap();
            if head_block.height == 0 {
                warn!("Chain only contains genesis block; recent sync from masternode recommended");
                info!("Chain state: Genesis only - height: {}", head_block.height);
            } else {
                info!(
                    "Chain state: Initialized - height: {}, hash: {}",
                    head_block.height,
                    hex::encode(head_block.hash)
                );
            }
        }
    }

    // DIAGNOSTIC: dump gossipsub peer state after bootstrap sync
    {
        let all_gossip_peers: Vec<_> = swarm
            .behaviour()
            .gossipsub
            .all_peers()
            .map(|(pid, topics)| format!("{}({}topics)", pid, topics.len()))
            .collect();
        let proposal_topic_hash =
            libp2p::gossipsub::IdentTopic::new("/savitri/masternode/proposal/1").hash();
        let peers_on_proposal: Vec<_> = swarm
            .behaviour()
            .gossipsub
            .all_peers()
            .filter(|(_, topics)| topics.iter().any(|t| **t == proposal_topic_hash))
            .map(|(pid, _)| pid.to_string())
            .collect();
        info!(
            total_gossip_peers = all_gossip_peers.len(),
            peers_on_proposal_topic = peers_on_proposal.len(),
            "🔍 POST-BOOTSTRAP gossipsub state: all_peers={:?}",
            all_gossip_peers
        );
        info!(
            "🔍 POST-BOOTSTRAP peers subscribed to /savitri/masternode/proposal/1: {:?}",
            peers_on_proposal
        );
    }
    info!("NETWORK STEP 1/4: Bootstrap sync completed, starting network task setup");

    let peer_accounts = Arc::new(RwLock::new(Vec::<[u8; 32]>::new()));
    let pou_state = PouState::new(local_peer_id);
    let pou_state_for_return = pou_state.clone();

    info!("NETWORK STEP 2/4: Setting up network task variables");

    let peer_accounts_shared = Arc::clone(&peer_accounts);
    let storage_clone = Arc::clone(&storage);
    let pou_state_handle = pou_state.clone();
    let certificate_pending_handle = Arc::clone(&certificate_pending);
    let local_peer = Swarm::local_peer_id(&swarm).clone();
    let mut known_peer_accounts: HashMap<PeerId, [u8; 32]> =
        bootstrap_accounts.into_iter().collect();
    let masternode_peers_set: HashSet<PeerId> =
        priority_targets.read().await.keys().cloned().collect();

    let registration_topic = registration_topic.clone();
    let registration_topic_hash = registration_topic.hash();
    let peer_registry_topic_hash = peer_registry_topic.hash();
    let peer_discovery_topic_hash = peer_discovery_topic.hash();
    let monolith_topic_hash = monolith_topic.hash();
    // Track the highest monolith-pruned height to avoid re-pruning
    let mut last_monolith_prune_height: u64 = 0;
    /// Minimum number of recent blocks to retain after monolith pruning (safety margin)
    const MONOLITH_PRUNE_RETENTION: u64 = 200;
    let group_manager = group_manager.clone();
    let intra_group_comm = intra_group_comm.clone();
    let periodic_task_manager = periodic_task_manager.clone();
    let intra_group_latency_topic = intra_group_latency_topic.clone();
    let intra_group_pou_topic = intra_group_pou_topic.clone();
    let intra_group_pou_ack_topic = intra_group_pou_ack_topic.clone();
    let intra_group_ping_topic = intra_group_ping_topic.clone();
    let intra_group_pong_topic = intra_group_pong_topic.clone();
    let intra_group_election_topic = intra_group_election_topic.clone();
    let block_receiver_gossip = block_receiver.clone();
    let shared_pou_score_for_registration = shared_pou_score.clone();
    let pou_observations_for_gossip = Arc::clone(&pou_observations);
    let registration_listen_port = listen_port;
    // Use shared mempool_pipeline for gossip tx so local+external txs feed block production
    let mempool_pipeline_for_network = mempool_pipeline.clone();
    let dag_manager_for_return = Arc::clone(&dag_manager);
    let shared_command_tx: Arc<std::sync::Mutex<Option<mpsc::Sender<SwarmCommand>>>> =
        Arc::new(std::sync::Mutex::new(None));
    let shared_command_tx_inner = shared_command_tx.clone();
    let tx_store_shared = crate::p2p::tx_fetch_protocol::TxStore::new();
    let tx_store_for_return = tx_store_shared.clone();
    let tx_store_for_spawn = tx_store_shared.clone();
    let (network_events, _network_events_rx_unused) =
        tokio::sync::broadcast::channel::<NetworkEvent>(256);
    let network_events_for_return = network_events.clone();
    let listen_addrs_for_return =
        Arc::new(RwLock::new(swarm.listeners().cloned().collect::<Vec<_>>()));
    let listen_addrs_for_spawn = Arc::clone(&listen_addrs_for_return);
    let observed_addr_for_return = Arc::new(RwLock::new(String::new()));
    let observed_addr_for_spawn = Arc::clone(&observed_addr_for_return);
    let connected_peers_for_return = Arc::new(RwLock::new(HashSet::new()));
    let connected_peers_for_spawn = Arc::clone(&connected_peers_for_return);
    let handle = tokio::spawn(async move {
        info!("NETWORK STEP 3/4: Network task started - entering main event loop");

        let mut intra_group_tx_topic = intra_group_tx_topic;
        let mut intra_group_pou_topic = intra_group_pou_topic;
        let mut intra_group_pou_ack_topic = intra_group_pou_ack_topic;
        let mut intra_group_ping_topic = intra_group_ping_topic;
        let mut intra_group_pong_topic = intra_group_pong_topic;
        let mut intra_group_election_topic = intra_group_election_topic;
        let mut intra_group_latency_topic = intra_group_latency_topic;
        let mut intra_group_proposal_topic = intra_group_proposal_topic;
        let mut intra_group_vote_topic = intra_group_vote_topic;
        // NOTE: tx_rx, block_rx, heartbeat_rx, pou_rx, intra_publish_rx, tx_forward_rx
        // -> migrati al publish aggregator (non-biased select, zero starvation)
        // NOTE: have_tx_receiver, certificate_receiver, integrity_receiver, pou_receiver
        // -> migrati al logging task (drain-only)
        let mut heartbeat_event_sender = heartbeat_event_sender;
        let integrity_events = integrity_event_sender;
        let mut resource_events = resource_event_sender;

        let mut connected_priority_peers: HashSet<PeerId> = HashSet::new();
        let mut connected_p2p_group_peers: HashSet<PeerId> = HashSet::new();
        // di registrazione visti sul topic /savitri/registration/1. Viene used
        let mut registered_lightnodes: HashSet<PeerId> = HashSet::new();
        let mut p2p_group_peers = p2p_group_peers;
        // Addresses from last our-group announce, for group peer retry dial.
        let mut group_member_addresses: HashMap<PeerId, Multiaddr> = HashMap::new();
        let mut group_mesh_established = false;
        let mut pending_group_formed_ack = false;
        let mut intra_group_mesh_established = false;
        let mut periodic_tasks_started = false;
        let mut priority_last_attempt: HashMap<PeerId, Instant> = HashMap::new();
        let anchor_seed = seed;
        let mut anchor_peer_id = anchor_peer_id;
        // last_heartbeat_sent: migrato al publish aggregator

        // Use shared mempool_pipeline when available so gossip txs feed block production; fallback to local pipeline
        let pipeline: crate::p2p::block::LightnodeMempoolHandle = mempool_pipeline_for_network
            .clone()
            .unwrap_or_else(|| Arc::new(MempoolPipeline::new()));
        let mut tx_batch_buffer: Vec<Vec<u8>> = Vec::new();
        const BATCH_SIZE: usize = 100;
        const BATCH_TIMEOUT_MS: u64 = 10;

        #[derive(Default)]
        struct PerformanceMetrics {
            batch_count: u64,
            total_processed: u64,
            total_accepted: u64,
        }
        let metrics = Arc::new(StdMutex::new(PerformanceMetrics::default()));

        if !known_peer_accounts.is_empty() {
            update_peer_directory(&peer_accounts_shared, &known_peer_accounts).await;
        }

        let mut allow_bootstrap_overwrite = false;
        let mut last_bootstrap_request: Option<Instant> = None;
        let mut pending_bootstrap_target: Option<u64> = None;
        let mut last_peer_info_failure = Instant::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap_or_else(Instant::now);
        let mut last_tx_failure = last_peer_info_failure;

        let mut remote_u_history: HashMap<[u8; 32], FixedPoint> = HashMap::new();
        let mut remote_prev_pou: HashMap<[u8; 32], FixedPoint> = HashMap::new();
        let pou_scoring = PouScoring::new();

        let mut batch_timer = tokio::time::interval(Duration::from_millis(BATCH_TIMEOUT_MS));
        batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Periodic block sync check: log local DAG height and detect potential gaps
        let mut block_sync_interval = tokio::time::interval(Duration::from_secs(30));
        block_sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_sync_height: u64 = 0;
        let mut block_sync_stall_ticks: u32 = 0;
        let mut block_sync_probe_cursor: usize = 0;
        let mut block_sync_manager = BlockSyncManager::new();
        let mut pending_block_sync_requests: HashMap<PeerId, BlockSyncRequest> = HashMap::new();
        // PERF: Dedup block_final messages by height — prevents processing the same
        // block 5 times (once per MN cert). Only the first cert per height is processed.
        let mut committed_heights: std::collections::HashSet<u64> =
            std::collections::HashSet::new();

        // Partition detector: monitors connectivity and flags network splits
        let mut partition_detector =
            savitri_consensus::protocols::partition::PartitionDetector::new();

        // Timer migrati -> maintenance task (connection_retry, group_peer_retry, registry_announce)
        // Timer migrati -> logging task (connection_health)

        let mut registration_successful = false;
        let mut pending_registration = false;
        let registration_region = "unknown";
        let registration_uptime = 100.0;

        let (peer_discovery_tx, peer_discovery_rx) = tokio::sync::mpsc::channel::<()>(1);
        // Defer heavy intra-group init so the swarm can be polled (dial can complete); see ANALISI_ARCHITETTURA_LIBP2P.md
        let (deferred_group_init_tx, mut deferred_group_init_rx) =
            tokio::sync::mpsc::channel::<String>(1);
        let (deferred_init_done_tx, mut deferred_init_done_rx) =
            tokio::sync::mpsc::channel::<String>(1);

        // ═══════════════════════════════════════════════════════════════
        // SWARM COMMAND QUEUE PATTERN: Canali inter-task
        // ═══════════════════════════════════════════════════════════════
        // command_tx/command_rx: i task worker inviano comandi allo swarm task
        let (command_tx, mut command_rx) = mpsc::channel::<SwarmCommand>(65536);
        // Wire consensus direct P2P channel into IntraGroupComm
        intra_group_comm
            .write()
            .await
            .set_network_direct_tx(command_tx.clone());
        // Save for NetworkComponents return (outside the spawn)
        *shared_command_tx_inner.lock().unwrap() = Some(command_tx.clone());
        // Set masternode PeerIds for direct TCP (aux protocol) messaging
        {
            let mn_peer_ids: Vec<PeerId> = priority_targets.read().await.keys().cloned().collect();
            if !mn_peer_ids.is_empty() {
                info!(
                    count = mn_peer_ids.len(),
                    "Setting masternode peer IDs for aux protocol"
                );
                intra_group_comm
                    .read()
                    .await
                    .set_masternode_peer_ids(mn_peer_ids)
                    .await;
            }
        }
        // event_tx: lo swarm task emette eventi verso i task worker
        let event_tx = network_events;
        // Shared intra_group_tx_topic: aggiornato dallo swarm task dopo group init,
        // letto dal publish aggregator per il tx_forward_rx
        let shared_intra_group_tx_topic = Arc::new(RwLock::new(intra_group_tx_topic.clone()));
        let intra_group_mesh_ready = Arc::new(AtomicBool::new(false));
        // Shared listen addr per il maintenance task (registry announce)
        let shared_listen_addr = Arc::new(RwLock::new(
            swarm
                .listeners()
                .next()
                .map(|a| {
                    normalize_registration_addr(
                        &a.to_string(),
                        registration_listen_port,
                        external_ip.as_deref(),
                    )
                })
                .unwrap_or_default(),
        ));
        // Observed address (from Identify): our address as seen by remote peers; use for decentralized/NAT
        let shared_observed_addr: Arc<RwLock<String>> = observed_addr_for_spawn;
        let shared_listen_addrs = listen_addrs_for_spawn;
        let shared_connected_peers = connected_peers_for_spawn;

        // ── Spawn Task 4: Logging (drain-only channels) ─────────────
        tokio::spawn(run_logging_task(
            have_tx_receiver,
            certificate_receiver,
            integrity_receiver,
            pou_receiver,
            connection_pool.clone(),
        ));
        info!("Spawned logging task (5 drain-only channels)");

        // ── Spawn Task 2: Publish Aggregator (non-biased select) ────
        tokio::spawn(run_publish_aggregator(
            command_tx.clone(),
            tx_rx,
            block_rx,
            block_broadcast_only_rx,
            pou_rx,
            heartbeat_rx,
            intra_publish_rx,
            tx_forward_rx,
            tx_topic.clone(),
            block_topic.clone(),
            pou_topic.clone(),
            heartbeat_topic.clone(),
            shared_intra_group_tx_topic.clone(),
            certificate_pending_handle.clone(),
            pou_state_handle.clone(),
            local_peer,
            resource_events.take(),
            intra_group_mesh_ready.clone(),
            Arc::clone(&priority_targets),
        ));
        info!("Spawned publish aggregator (7 channels, non-biased select)");

        // ── Spawn Task 3: Maintenance (timers + connection retry) ───
        // Pass shared priority_targets so maintenance task sees dynamically discovered MNs
        tokio::spawn(run_maintenance_task(
            command_tx.clone(),
            event_tx.subscribe(),
            Arc::clone(&priority_targets),
            local_peer,
            shared_listen_addr.clone(),
            registration_listen_port,
            peer_discovery_rx,
            registration_successful,
        ));
        info!("Spawned maintenance task (timers + connection tracking)");

        let registration_pou_score = if let Some(ref s) = shared_pou_score_for_registration {
            let guard = s.read().await;
            guard.map(|v| v as f64 / 10000.0).unwrap_or(0.5)
        } else {
            0.5
        };

        if let Some(masternode_peer_id) = priority_targets.read().await.keys().next().cloned() {
            info!(
                masternode_peer = %masternode_peer_id,
                "🚀 STARTING: Initial peer info broadcast with retry mechanism"
            );
            let ext_addr = external_ip
                .as_ref()
                .map(|ip| format!("/ip4/{}/tcp/{}", ip, registration_listen_port));
            if let Err(err) = broadcast_peer_info(
                &mut swarm,
                &peer_info_topic,
                &local_account,
                ext_addr.as_deref(),
            )
            .await
            {
                error!(
                    error=?err,
                    masternode_peer = %masternode_peer_id,
                    "💥 CRITICAL: Failed to send initial peer info to masternodes"
                );
            } else {
                info!("✅ SUCCESS: Initial peer info sent to masternodes");
            }

            info!(
                masternode_peer = %masternode_peer_id,
                "📝 STARTING: Lightnode registration for group formation"
            );
            let listen_addr = if let Some(ext_ip) = &external_ip {
                format!("/ip4/{}/tcp/{}", ext_ip, registration_listen_port)
            } else {
                normalize_registration_addr(
                    &swarm
                        .listeners()
                        .next()
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string()),
                    registration_listen_port,
                    None,
                )
            };
            match broadcast_lightnode_registration(
                &mut swarm,
                &local_peer.to_string(),
                &listen_addr,
                &registration_reward_account,
                registration_region,
                registration_pou_score,
                registration_uptime,
            )
            .await
            {
                Ok(_) => {
                    info!("Registration message published to gossipsub (request sent; masternode may not have processed yet)");
                    registration_successful = true;
                    pending_registration = false;
                    info!("Connection retry remains active for resilience");
                    info!(
                        peer_id = %local_peer,
                        listen_addr = %listen_addr,
                        region = registration_region,
                        pou_score = registration_pou_score,
                        uptime = registration_uptime,
                        "Registration request sent - details for masternode verification"
                    );
                }
                Err(err) => {
                    let err_str = err.to_string();
                    if err_str.contains("InsufficientPeers") {
                        pending_registration = true;
                        info!("Registration deferred (waiting for gossipsub mesh to form)");
                    } else {
                        error!(
                            error=?err,
                            peer_id = %local_peer,
                            listen_addr = %listen_addr,
                            "💥 CRITICAL: Failed to send registration for group formation"
                        );
                    }
                }
            }
        } else {
            debug!("No masternode peers configured - skipping initial peer info");
        }

        // FIX: Sync connected_priority_peers with swarm state before entering the
        // event loop.  During the pre-loop registration retry (with exponential
        // back-off sleeps) the swarm is not polled, so ConnectionEstablished events
        // are buffered.  Without this sync the retry timer would try to re-dial
        // peers that are already connected, producing spurious warnings.
        for peer_id in swarm.connected_peers().cloned().collect::<Vec<_>>() {
            let is_priority = priority_targets.read().await.contains_key(&peer_id);
            if is_priority && connected_priority_peers.insert(peer_id) {
                let total_mn = priority_targets.read().await.len();
                info!(
                    peer = %peer_id,
                    connected_count = connected_priority_peers.len(),
                    total_priority = total_mn,
                    "Pre-loop sync: masternode already connected"
                );
                priority_last_attempt.remove(&peer_id);
            }
        }

        // ═══════════════════════════════════════════════════════════════
        //
        // With only 5 branches, no starvation is possible.
        //
        // Branch migrati ai task worker:
        //   -> Publish Aggregator: tx_rx, block_rx, pou_rx, heartbeat_rx,
        //      intra_publish_rx, tx_forward_rx (select! NON-biased = fair)
        //   -> Maintenance Task: connection_retry, group_peer_retry,
        //      registry_announce, peer_discovery
        //   -> Logging Task: have_tx, certificate, integrity, pou_event,
        //      connection_health
        // ═══════════════════════════════════════════════════════════════

        // Dead peer detection: track last activity per peer, disconnect stale ones.
        // Prevents ~89K "Send Queue full" events from gossipsub to frozen peers.
        let mut peer_last_activity: HashMap<PeerId, Instant> = HashMap::new();
        // Dedup group announcements: track (group_id, epoch) to skip repeated processing
        let mut last_processed_group_epoch: HashMap<String, u64> = HashMap::new();
        let mut peer_health_timer = tokio::time::interval(Duration::from_secs(30));
        peer_health_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Election watchdog: re-trigger election if no blocks produced
        // re-election fires after 30s instead of 60s, halving epoch-transition stalls.
        let mut election_watchdog_timer = tokio::time::interval(Duration::from_secs(15));
        election_watchdog_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_known_block_height: u64 = 0;
        let mut election_watchdog_stall_count: u32 = 0;
        // Idle LNs that can't form mesh were permanently stuck because the watchdog
        // skipped them entirely (`!intra_group_mesh_established` → skip).
        let mut group_join_time: Option<Instant> = None;
        let mut mesh_recovery_attempts: u32 = 0;
        // When MN sends new group announcements (every ~30s), deferred init fires
        // for each one, causing concurrent elections on old + new groups.
        // Global cooldown blocked init for NEW groups after any recent init.
        // Per-group allows first init for new groups immediately, only throttles
        // re-announcements for the SAME group.
        let mut last_group_init_times: HashMap<String, Instant> = HashMap::new();
        const GROUP_INIT_COOLDOWN_SECS: u64 = 15;

        // Periodic re-broadcast of peer_info + lightnode registration. Without
        // this, if a masternode restarts or evicts us via cleanup_inactive
        // (default node_timeout_secs = 600s), we are never re-registered —
        // broadcast_peer_info / broadcast_lightnode_registration are currently
        // only called on startup and on PeerConnected events. That left
        // `registered_nodes` on the MN silently empty and group formation
        // stalled with free_nodes=0 indefinitely.
        //
        // 60s to 30s so epoch transitions (every slots_per_epoch * heartbeat_ms
        // = 1000s with default config) are detected within 3% lag. Combined
        // with the epoch-change-detector below, this forces an IMMEDIATE
        // re-registration on epoch boundary so the MN re-issues a fresh
        // group_id for the new epoch instead of leaving the LN stuck with
        // its boot-time group_id.
        let mut registration_rebroadcast_timer = tokio::time::interval(Duration::from_secs(30));
        registration_rebroadcast_timer
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // Fast poll for epoch transition detection (every 5s). When current
        // epoch (computed via the canonical primitive) advances past the
        // last observed value, we trigger an extra rebroadcast on top of
        // the periodic one. The MN's group_formation task wakes once per
        // epoch and emits new GroupAnnouncements, but the LN side won't
        // see a fresh group_id until the LN itself re-registers with the
        // new epoch context.
        let mut epoch_poll_timer = tokio::time::interval(Duration::from_secs(5));
        epoch_poll_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_observed_epoch: u64 = if genesis_ms_for_groups > 0 {
            savitri_consensus::primitives::epoch::current_epoch(
                savitri_consensus::primitives::epoch::now_ms(),
                genesis_ms_for_groups,
                heartbeat_ms_for_groups,
                slots_per_epoch_for_groups,
            )
        } else {
            0
        };
        let mut epoch_change_pending: bool = false;

        loop {
            select! {
                event = swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            info!(%address, "P2P listening on address (incoming connections accepted)");
                            // Replace 0.0.0.0 / 127.0.0.1 with external_ip before sharing so the
                            // maintenance task's registry announces a dialable address.
                            let dialable_str = normalize_registration_addr(&address.to_string(), registration_listen_port, external_ip.as_deref());
                            let dialable: Multiaddr = dialable_str.parse().unwrap_or(address.clone());
                            if let Ok(mut addrs) = shared_listen_addrs.try_write() {
                                if !addrs.iter().any(|addr| addr == &dialable) {
                                    addrs.push(dialable.clone());
                                }
                            }
                            let _ = event_tx.send(NetworkEvent::NewListenAddr { address: dialable });
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            debug!(%peer_id, "Connected");
                            peer_last_activity.insert(peer_id, Instant::now());
                            #[cfg(feature = "metrics")]
                            counter!("p2p_connection_attempts_total").increment(1);
                            // Register connection in pool so is_connected() is true and group mesh logic sees it
                            let remote_addr = match &endpoint {
                                ConnectedPoint::Dialer { address, .. } => address.clone(),
                                ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                            };
                            {
                                let pool_clone = connection_pool.clone();
                                let pid = peer_id.clone();
                                let addr = remote_addr.clone();
                                tokio::spawn(async move {
                                    let mut pool = pool_clone.lock().await;
                                    pool.add_connection(pid.clone(), addr).await;
                                    pool.mark_connection_active(&pid).await;
                                    pool.cache_handshake(pid.clone(), HandshakeResult::Success).await;
                                    #[cfg(feature = "metrics")]
                                    gauge!("p2p_peers_connected").set(pool.get_peers().len() as f64);
                                });
                            }
                            block_sync_manager.update_peer_height(peer_id.clone(), 0);

                            let is_priority_peer = match priority_targets.try_read() {
                                Ok(pt) => pt.contains_key(&peer_id),
                                Err(_) => false,
                            };
                            if is_priority_peer {
                                info!(
                                    masternode_peer = %peer_id,
                                    "🔗 CONNECTED: Sending peer info to newly connected masternode"
                                );
                                let advertised = if let Some(ext_ip) = &external_ip {
                                    format!("/ip4/{}/tcp/{}", ext_ip, registration_listen_port)
                                } else {
                                    match shared_observed_addr.try_read() {
                                        Ok(obs) => obs.clone(),
                                        Err(_) => String::new(),
                                    }
                                };
                                let advertised_opt = if advertised.is_empty() { None } else { Some(advertised.as_str()) };
                                if let Err(err) = broadcast_peer_info_sync(&mut swarm, &peer_info_topic, &local_account, advertised_opt) {
                                    warn!(
                                        error=?err,
                                        masternode_peer = %peer_id,
                                        "failed to send peer info to connected masternode"
                                    );
                                } else {
                                    info!("✅ Sent peer info to connected masternode: {}", peer_id);
                                }
                                let listen_addr = if let Some(ext_ip) = &external_ip {
                                    format!("/ip4/{}/tcp/{}", ext_ip, registration_listen_port)
                                } else {
                                    let listen_addr_raw = match shared_observed_addr.try_read() {
                                        Ok(obs) => obs.clone(),
                                        Err(_) => String::new(),
                                    };
                                    if listen_addr_raw.is_empty() || listen_addr_raw.contains("tcp/0") {
                                        normalize_registration_addr(
                                            &swarm.listeners().next().map(|a| a.to_string())
                                                .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string()),
                                            registration_listen_port,
                                            None,
                                        )
                                    } else {
                                        normalize_registration_addr(&listen_addr_raw, registration_listen_port, None)
                                    }
                                };
                                if let Err(err) = broadcast_lightnode_registration_sync(
                                    &mut swarm,
                                    &local_peer.to_string(),
                                    &listen_addr,
                                    &registration_reward_account,
                                    registration_region,
                                    registration_pou_score,
                                    registration_uptime,
                                ) {
                                    let err_str = err.to_string();
                                    if err_str.contains("InsufficientPeers") {
                                        pending_registration = true;
                                        info!(
                                            error=?err,
                                            masternode_peer = %peer_id,
                                            "Registration deferred (waiting for gossipsub mesh)"
                                        );
                                    } else {
                                        // 🔧 FIX A: Logging diagnostico per errori non-InsufficientPeers
                                        error!(
                                            error=?err,
                                            peer_id = %local_peer,
                                            masternode_peer = %peer_id,
                                            "💥 CRITICAL: Failed to send registration to newly connected masternode"
                                        );
                                    }
                                } else {
                                    info!("✅ Sent registration to connected masternode: {}", peer_id);
                                }

                                connected_priority_peers.insert(peer_id.clone());
                                if let Ok(mut peers) = shared_connected_peers.try_write() {
                                    peers.insert(peer_id.clone());
                                }
                                let total_mn = match priority_targets.try_read() {
                                    Ok(pt) => pt.len(),
                                    Err(_) => 0,
                                };
                                info!(
                                    peer = %peer_id,
                                    connected_count = connected_priority_peers.len(),
                                    total_priority = total_mn,
                                    "{}",
                                    flagged_message(
                                        FLAG_MASTERNODE,
                                        "Connected to priority masternode peer"
                                    )
                                );
                                priority_last_attempt.remove(&peer_id);
                                // Emit per maintenance task
                                let _ = event_tx.send(NetworkEvent::PeerConnected {
                                    peer_id: peer_id.clone(),
                                    is_masternode: true,
                                });

                                if anchor_peer_id.is_none() {
                                    if let Some(new_anchor) = choose_anchor_from_ids(connected_priority_peers.iter(), anchor_seed) {
                                        // No add_explicit_peer: MN must stay as normal mesh peer
                                        // so gossipsub can GRAFT and exchange subscriptions properly.
                                        anchor_peer_id = Some(new_anchor.clone());
                                        info!(
                                            peer = %new_anchor,
                                            "{}",
                                            flagged_message(FLAG_MASTERNODE, "Failover selected new anchor masternode")
                                        );
                                    }
                                }

                                if connected_priority_peers.len() == 1 {
                                    info!("First masternode connected - will request peer discovery after mesh stabilization");
                                    let tx = peer_discovery_tx.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(Duration::from_secs(5)).await;
                                        let _ = tx.send(()).await;
                                    });
                                }
                            }

                            let is_our_group_peer = p2p_group_peers.contains(&peer_id)
                                || group_manager.get_current_group_cached()
                                    .map(|g| g.members.iter()
                                        .filter(|m| **m != local_peer.to_string())
                                        .any(|s| PeerId::from_str(s).ok().as_ref() == Some(&peer_id)))
                                    .unwrap_or(false);
                            if is_our_group_peer {
                                connected_p2p_group_peers.insert(peer_id.clone());
                                if let Ok(mut peers) = shared_connected_peers.try_write() {
                                    peers.insert(peer_id.clone());
                                }
                                // No add_explicit_peer: group peers must stay as normal mesh peers
                                // so gossipsub GRAFT/subscription exchange works correctly.
                                info!(
                                    peer = %peer_id,
                                    connected_p2p_count = connected_p2p_group_peers.len(),
                                    total_p2p = p2p_group_peers.len(),
                                    "Connected to P2P group peer (normal mesh)"
                                );
                                // Emit per maintenance task
                                let _ = event_tx.send(NetworkEvent::PeerConnected {
                                    peer_id: peer_id.clone(),
                                    is_masternode: false,
                                });
                                if !group_mesh_established
                                    && !p2p_group_peers.is_empty()
                                    && connected_p2p_group_peers.len() == p2p_group_peers.len()
                                {
                                    group_mesh_established = true;
                                    let current_group = group_manager.get_current_group_cached();
                                    let (group_id, group_epoch) = current_group
                                        .as_ref()
                                        .map(|g| (g.group_id.clone(), g.epoch))
                                        .unwrap_or_else(|| ("unknown".to_string(), 0));
                                    info!(
                                        group_id = %group_id,
                                        connected = connected_p2p_group_peers.len(),
                                        total = p2p_group_peers.len(),
                                        "Group formed: all P2P group peers connected"
                                    );

                                    if group_id == "unknown" {
                                        pending_group_formed_ack = true;
                                        warn!(
                                            local_peer = %local_peer,
                                            connected = connected_p2p_group_peers.len(),
                                            total = p2p_group_peers.len(),
                                            "Group mesh formed but no group assignment yet; deferring ACK"
                                        );
                                    } else {
                                        let ack = GroupFormedAck {
                                            group_id: group_id.clone(),
                                            epoch: group_epoch,
                                            peer_id: local_peer.to_string(),
                                            timestamp: ack_timestamp(),
                                            connected_peers: connected_p2p_group_peers.len(),
                                            total_peers: p2p_group_peers.len(),
                                        };
                                        match serde_json::to_vec(&ack) {
                                            Ok(payload) => {
                                                if let Err(err) = swarm
                                                    .behaviour_mut()
                                                    .gossipsub
                                                    .publish(group_formed_topic.clone(), payload)
                                                {
                                                    warn!(error=?err, "Failed to send group formed ACK to masternode");
                                                } else {
                                                    info!(
                                                        group_id = %group_id,
                                                        epoch = group_epoch,
                                                        local_peer = %local_peer,
                                                        connected = connected_p2p_group_peers.len(),
                                                        total = p2p_group_peers.len(),
                                                        "Group formed ACK sent to masternode"
                                                    );
                                                }
                                            }
                                            Err(err) => {
                                                warn!(error=?err, "Failed to encode group formed ACK");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                            peer_last_activity.remove(&peer_id);
                            #[cfg(feature = "metrics")]
                            counter!("p2p_peers_disconnected_total").increment(1);
                            let is_group_peer = p2p_group_peers.contains(&peer_id);
                            info!(
                                %peer_id,
                                is_group_peer,
                                cause = ?cause,
                                "Connection closed"
                            );
                            if is_group_peer {
                                info!(
                                    %peer_id,
                                    cause = ?cause,
                                    "Group peer connection closed (dial may have failed or peer disconnected)"
                                );
                            }

                            if is_group_peer {
                                swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                            }

                            {
                                let pool_clone = connection_pool.clone();
                                let pid = peer_id.clone();
                                tokio::spawn(async move {
                                    let mut pool = pool_clone.lock().await;
                                    pool.remove_connection(&pid, None).await;
                                    #[cfg(feature = "metrics")]
                                    gauge!("p2p_peers_connected").set(pool.get_peers().len() as f64);
                                });
                            }

                            connected_priority_peers.remove(&peer_id);
                            connected_p2p_group_peers.remove(&peer_id);
                            if let Ok(mut peers) = shared_connected_peers.try_write() {
                                peers.remove(&peer_id);
                            }
                            block_sync_manager.remove_peer(&peer_id);
                            // Emit per maintenance task
                            let _ = event_tx.send(NetworkEvent::PeerDisconnected { peer_id: peer_id.clone() });

                            if anchor_peer_id.as_ref() == Some(&peer_id) {
                                // No remove_explicit_peer needed since we no longer add MNs as explicit
                                anchor_peer_id = None;

                                if let Some(new_anchor) = choose_anchor_from_ids(connected_priority_peers.iter(), anchor_seed) {
                                    // No add_explicit_peer: normal mesh peer for proper GRAFT/subscription exchange
                                    anchor_peer_id = Some(new_anchor.clone());
                                    info!(
                                        peer = %new_anchor,
                                        "{}",
                                        flagged_message(FLAG_MASTERNODE, "Anchor masternode down; switched anchor")
                                    );
                                } else {
                                    warn!(
                                        "{}",
                                        flagged_message(FLAG_MASTERNODE, "Anchor masternode down; no priority peers connected (may be transient)")
                                    );
                                }
                            }

                            if known_peer_accounts.remove(&peer_id).is_some() {
                                let pa_shared = peer_accounts_shared.clone();
                                let kpa = known_peer_accounts.clone();
                                tokio::spawn(async move {
                                    update_peer_directory(&pa_shared, &kpa).await;
                                });
                            }

                            if let Ok(pt) = priority_targets.try_read() {
                                maybe_redial_priority(
                                    &mut swarm,
                                    &pt,
                                    &mut priority_last_attempt,
                                    &peer_id,
                                    "connection closed",
                                );
                            }
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                            #[cfg(feature = "metrics")]
                            counter!("p2p_connection_attempts_total").increment(1);
                            let is_group_peer = peer_id.map(|p| p2p_group_peers.contains(&p)).unwrap_or(false);
                            if is_group_peer {
                                // Connection refused is normal when other group lightnodes are not running (single-node / dev)
                                let err_str = error.to_string();
                                let is_refused = err_str.contains("Connection refused") || err_str.contains("refused");
                                if is_refused {
                                    debug!(
                                        peer_id = ?peer_id,
                                        "Group peer unreachable (other node not listening - normal if only one lightnode running)"
                                    );
                                } else {
                                    warn!(
                                        peer_id = ?peer_id,
                                        error = %error,
                                        "Group peer dial failed (timeout/refused - check other node is listening on announce address)"
                                    );
                                }
                            } else {
                                info!(
                                    peer_id = ?peer_id,
                                    error = %error,
                                    is_group_peer,
                                    "Outgoing connection error"
                                );
                            }
                            if let Some(peer_id) = peer_id {
                                if let Ok(pt) = priority_targets.try_read() {
                                    maybe_redial_priority(
                                        &mut swarm,
                                        &pt,
                                        &mut priority_last_attempt,
                                        &peer_id,
                                        "outgoing connection error",
                                    );
                                }
                            }
                        }
                        SwarmEvent::Behaviour(behaviour_event) => {
                            match behaviour_event {
                                crate::p2p::types::MyBehaviourEvent::Gossipsub(gossipsub_event) => {
                                    match gossipsub_event {
                                        GossipsubEvent::Subscribed { peer_id, topic } => {
                                            peer_last_activity.insert(peer_id, Instant::now());
                                            // Check se è il topic di annuncio gruppo
                                            let group_announce_topic = group_manager.get_topic();
                                            if topic == group_announce_topic.hash() {
                                                let is_mn = match priority_targets.try_read() {
                                                    Ok(pt) => pt.contains_key(&peer_id),
                                                    Err(_) => false,
                                                };
                                                info!(
                                                    peer = %peer_id,
                                                    topic = %topic,
                                                    topic_string = %group_announce_topic,
                                                    is_masternode = is_mn,
                                                    "🔗 MESH FORMED: Peer subscribed to group announcement topic"
                                                );
                                            }

                                            if !intra_group_mesh_established
                                                && topic == intra_group_tx_topic.hash()
                                            {
                                                // Only peers that are actually in our P2P group count for intra-group mesh.
                                                // Do not use !p2p_group_peers.is_empty() alone: that would set mesh_established
                                                // when any peer (e.g. TX_Generator or another group) subscribes.
                                                let is_actual_group_peer = p2p_group_peers.contains(&peer_id)
                                                    || match priority_targets.try_read() {
                                                        Ok(pt) => pt.contains_key(&peer_id),
                                                        Err(_) => false,
                                                    };

                                                if is_actual_group_peer {
                                                    intra_group_mesh_established = true;
                                                    intra_group_mesh_ready.store(true, AtomicOrdering::Release);
                                                    let current_group = group_manager.get_current_group_cached();
                                                    let group_id = current_group
                                                        .as_ref()
                                                        .map(|g| g.group_id.clone())
                                                        .unwrap_or_else(|| "unknown".to_string());
                                                    info!(
                                                        group_id = %group_id,
                                                        peer = %peer_id,
                                                        is_group_peer = is_actual_group_peer,
                                                        "Established mesh intragroup — TX publishing unlocked"
                                                    );
                                                } else {
                                                    debug!(
                                                        peer = %peer_id,
                                                        p2p_group_empty = p2p_group_peers.is_empty(),
                                                        "Ignoring intra-group mesh subscription from non-group peer (likely TX_Generator)"
                                                    );
                                                }
                                            }
                                            info!("🔗 MESH FORMED: Peer {} subscribed to topic {:?}", peer_id, topic);
                                            if !registration_successful
                                                && pending_registration
                                                && topic == registration_topic_hash
                                            {
                                                let listen_addr = if let Some(ext_ip) = &external_ip {
                                                    format!("/ip4/{}/tcp/{}", ext_ip, registration_listen_port)
                                                } else {
                                                    let obs = match shared_observed_addr.try_read() {
                                                        Ok(o) => o.clone(),
                                                        Err(_) => String::new(),
                                                    };
                                                    if obs.is_empty() || obs.contains("tcp/0") {
                                                        normalize_registration_addr(
                                                            &swarm.listeners().next().map(|a| a.to_string())
                                                                .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string()),
                                                            registration_listen_port,
                                                            None,
                                                        )
                                                    } else {
                                                        normalize_registration_addr(&obs, registration_listen_port, None)
                                                    }
                                                };
                                                match broadcast_lightnode_registration_sync(
                                                    &mut swarm,
                                                    &local_peer.to_string(),
                                                    &listen_addr,
                                                    &registration_reward_account,
                                                    registration_region,
                                                    registration_pou_score,
                                                    registration_uptime,
                                                ) {
                                                    Ok(_) => {
                                                        info!("✅ Registration published after mesh formation");
                                                        registration_successful = true;
                                                        pending_registration = false;
                                                    }
                                                    Err(err) => {
                                                        let err_str = err.to_string();
                                                        if err_str.contains("InsufficientPeers") {
                                                            pending_registration = true;
                                                            info!(
                                                                error=?err,
                                                                "Registration deferred (waiting for gossipsub mesh)"
                                                            );
                                                        } else {
                                                            warn!(
                                                                error=?err,
                                                                "registration publish failed after mesh formation"
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        GossipsubEvent::Message { propagation_source, message, .. } => {
                                            peer_last_activity.insert(propagation_source, Instant::now());
                                            #[cfg(feature = "metrics")] {
                                                counter!("p2p_messages_received_total").increment(1);
                                                counter!("p2p_bytes_received_total").increment(message.data.len() as u64);
                                                counter!("gossip_messages_received_total").increment(1);
                                            }
                                            debug!(
                                                from = %propagation_source,
                                                topic = %message.topic,
                                                topic_string = ?message.topic,
                                                size = message.data.len(),
                                                expected_topic = %group_manager.get_topic().hash(),
                                                "📨 RAW MESSAGE RECEIVED"
                                            );

                                            // un messaggio di registrazione sul topic /savitri/registration/1.
                                            if message.topic == registration_topic_hash {
                                                match decode_gossip(&message.data) {
                                                    Ok(GossipMessage::LightnodeRegistration(reg)) => {
                                                        if let Ok(peer_id) = PeerId::from_str(&reg.peer_id) {
                                                            registered_lightnodes.insert(peer_id.clone());
                                                            debug!(
                                                                peer = %peer_id,
                                                                node_id = %reg.node_id,
                                                                "Recorded registered lightnode from registration gossip"
                                                            );
                                                        } else {
                                                            debug!(
                                                                peer_id = %reg.peer_id,
                                                                "Failed to parse peer_id from LightnodeRegistration gossip"
                                                            );
                                                        }
                                                    }
                                                    _ => {
                                                        // altri tipi on the stesso topic are ignorati
                                                    }
                                                }
                                            }

                                            // ═══════════════════════════════════════════
                                            // MONOLITH ANNOUNCE: verify + prune old blocks
                                            // ═══════════════════════════════════════════
                                            if message.topic == monolith_topic_hash {
                                                match bincode::deserialize::<crate::p2p::types::MonolithAnnounce>(&message.data)
                                                    .or_else(|_| serde_json::from_slice::<crate::p2p::types::MonolithAnnounce>(&message.data))
                                                {
                                                    Ok(announce) => {
                                                        let monolith_data = announce.monolith_data.as_deref().unwrap_or(&[]);
                                                        let verified = verify_monolith_data(
                                                            &announce.header.monolith_id,
                                                            &announce.header.monolith_hash,
                                                            monolith_data,
                                                        );

                                                        // ZKP verification: if the monolith carries a proof, verify it
                                                        let zkp_ok = if let Some(ref proof_bytes) = announce.header.zkp_proof {
                                                            if let (Some(ref hc), Some(ref sc)) = (&announce.header.headers_commit, &announce.header.state_commit) {
                                                                match savitri_zkp::monolith::monolith_zkp::verify_monolith_proof_bytes(
                                                                    hc, sc, announce.header.exec_height, proof_bytes,
                                                                ) {
                                                                    Ok(true) => {
                                                                        info!(
                                                                            exec_height = announce.header.exec_height,
                                                                            "Monolith ZKP proof verified successfully"
                                                                        );
                                                                        true
                                                                    }
                                                                    Ok(false) => {
                                                                        warn!("Monolith ZKP proof verification FAILED — rejecting monolith");
                                                                        false
                                                                    }
                                                                    Err(e) => {
                                                                        // ZKP verification not available (mock backend or missing keys)
                                                                        // Accept monolith with hash verification only
                                                                        debug!(error = %e, "ZKP verification unavailable, accepting with hash only");
                                                                        true
                                                                    }
                                                                }
                                                            } else {
                                                                // Missing commits but has proof — suspicious, still accept with hash
                                                                debug!("Monolith has ZKP proof but missing headers/state commits");
                                                                true
                                                            }
                                                        } else {
                                                            // No ZKP proof — accept based on hash verification alone
                                                            true
                                                        };

                                                        if verified && zkp_ok {
                                                            info!(
                                                                monolith_size = announce.header.monolith_size,
                                                                timestamp = announce.header.timestamp,
                                                                exec_height = announce.header.exec_height,
                                                                has_zkp = announce.header.zkp_proof.is_some(),
                                                                "Monolith block received and verified"
                                                            );

                                                            // Determine prune target: prune blocks covered by the
                                                            // monolith, keeping a safety retention window.
                                                            let current_height = storage_clone
                                                                .get_chain_head()
                                                                .ok()
                                                                .flatten()
                                                                .map(|b| b.height)
                                                                .unwrap_or(0);
                                                            let prune_below = current_height.saturating_sub(MONOLITH_PRUNE_RETENTION);

                                                            if prune_below > last_monolith_prune_height && prune_below > 1 {
                                                                // Refresh bootstrap snapshot before pruning so a restarted LN can
                                                                // recover balances/nonces even if old blocks are no longer available.
                                                                request_bootstrap_snapshot(
                                                                    &mut swarm,
                                                                    &bootstrap_req_topic,
                                                                    u64::MAX,
                                                                    &mut last_bootstrap_request,
                                                                    &mut allow_bootstrap_overwrite,
                                                                    &mut pending_bootstrap_target,
                                                                );
                                                                let storage_for_prune = Arc::clone(&storage_clone);
                                                                let prune_target = prune_below;
                                                                tokio::spawn(async move {
                                                                    match storage_for_prune.prune_blocks_below(prune_target) {
                                                                        Ok(count) if count > 0 => {
                                                                            tracing::info!(
                                                                                pruned = count,
                                                                                below_height = prune_target,
                                                                                "Block pruning after monolith completed"
                                                                            );
                                                                        }
                                                                        Err(err) => {
                                                                            tracing::warn!(
                                                                                error = %err,
                                                                                "Block pruning after monolith failed"
                                                                            );
                                                                        }
                                                                        _ => {}
                                                                    }
                                                                });
                                                                last_monolith_prune_height = prune_below;
                                                            }
                                                        } else {
                                                            warn!(
                                                                monolith_size = announce.header.monolith_size,
                                                                "Monolith verification failed, skipping pruning"
                                                            );
                                                        }
                                                    }
                                                    Err(err) => {
                                                        debug!(
                                                            error = %err,
                                                            size = message.data.len(),
                                                            "Failed to deserialize monolith announce"
                                                        );
                                                    }
                                                }
                                            }

                                            if message.topic == group_manager.get_topic().hash() {
                                                match serde_json::from_slice::<GroupAnnounce>(&message.data) {
                                                    Ok(announce) => {
                                                        // Dedup: skip if we already processed this (group_id, epoch)
                                                        let prev_epoch = last_processed_group_epoch.get(&announce.group_id).copied();
                                                        if prev_epoch.is_some() && announce.epoch <= prev_epoch.unwrap() {
                                                            debug!(
                                                                group_id = %announce.group_id,
                                                                epoch = announce.epoch,
                                                                prev_epoch = ?prev_epoch,
                                                                "Skipping duplicate group announcement (already processed)"
                                                            );
                                                        } else {
                                                        last_processed_group_epoch.insert(announce.group_id.clone(), announce.epoch);
                                                        info!(
                                                            group_id = %announce.group_id,
                                                            epoch = announce.epoch,
                                                            members_count = announce.members.len(),
                                                            assigned_shards_count = announce.assigned_shards.len(),
                                                            num_shards = announce.num_shards,
                                                            "Group announcement received from masternode"
                                                        );

                                                        let group_members = announce.members.clone();
                                                        let group_id_str = announce.group_id.clone();
                                                        let member_addresses = announce.member_addresses.clone();
                                                        let announce_shards = announce.assigned_shards.clone();
                                                        let announce_num_shards = announce.num_shards;
                                                        let announce_epoch = announce.epoch;
                                                        let is_my_group = group_members.contains(&local_node_id);

                                                        // P1: populate shard→group map from ALL group announces (not just ours).
                                                        // The router uses this to decide whether an RPC-submitted TX belongs to
                                                        // our group (admit local) or another's (forward via gossipsub).
                                                        if announce_num_shards > 0 {
                                                            num_shards.store(
                                                                announce_num_shards as u32,
                                                                std::sync::atomic::Ordering::Relaxed,
                                                            );
                                                        }
                                                        {
                                                            // before inserting the new ones for this group.
                                                            //
                                                            // Group ids follow the pattern `group_{epoch}_{idx}_{epoch}`
                                                            // (savitri_consensus::primitives::group_id). When the cluster
                                                            // rotates from epoch N to epoch N+K, each MN broadcasts a
                                                            // GroupAnnouncement for ITS own current-epoch group only.
                                                            // The handler used to merge-insert without cleanup, so the
                                                            // shard_to_group map kept old-epoch entries forever
                                                            // (group_7_0_7, group_7_1_7, ...) alongside the new
                                                            // (group_13_0_13, ...). TxRouter::route() then routed
                                                            // intra-shard TX to dead topics (no proposer of group_7_*
                                                            // is subscribed any more) → silent drop, blocks empty.
                                                            //
                                                            // Strategy: parse the epoch out of the incoming group id,
                                                            // then drop every map entry whose group id has a STRICTLY
                                                            // OLDER epoch. Same-epoch entries from other groups stay
                                                            // (each MN announces only its own group, so other groups of
                                                            // the same epoch arrive in their own announce). We do NOT
                                                            // drop newer-epoch entries (defensive: avoid clobbering a
                                                            // race where two announces of epoch N+1 / N arrive
                                                            // out-of-order).
                                                            let group_for_shards = group_id_str.clone();
                                                            let shards_cloned = announce_shards.clone();
                                                            let map_handle = shard_to_group.clone();
                                                            tokio::spawn(async move {
                                                                fn extract_epoch(gid: &str) -> Option<u64> {
                                                                    // gid format: "group_{epoch}_{idx}_{epoch}"
                                                                    let mut parts = gid.split('_');
                                                                    let _ = parts.next()?; // "group"
                                                                    parts.next()?.parse::<u64>().ok()
                                                                }
                                                                let mut guard = map_handle.write().await;
                                                                if let Some(new_epoch) = extract_epoch(&group_for_shards) {
                                                                    // also purge entries that point to THIS group_id even
                                                                    // when same-epoch. Reason: the MN forms groups
                                                                    // gradually as LN register; group_0_0_0 may first be
                                                                    // announced with 65536 shards (single-group bootstrap),
                                                                    // then re-announced with 21845 after a 3-group split.
                                                                    // Without dropping the old 65536-shard entries, the
                                                                    // LN map keeps ~32768 stale shard→group_0_0_0
                                                                    // entries that the MN no longer owns. TX routed there
                                                                    // arrive at the wrong proposer, get rejected by
                                                                    // shard_filter (kept_local=false, rpc_accepted=false),
                                                                    // restored to the mempool, and finally evicted by
                                                                    // the 120s TTL purge → the "drain 2000 first block,
                                                                    // then mempool=0" pattern observed during loadtest.
                                                                    //
                                                                    // Drop both: (a) older-epoch entries (legacy fix),
                                                                    // (b) same-epoch entries pointing to this same group
                                                                    // id (we are about to re-write them with the new
                                                                    // canonical set).
                                                                    let stale_keys: Vec<u32> = guard
                                                                        .iter()
                                                                        .filter_map(|(k, v)| {
                                                                            if v == &group_for_shards {
                                                                                return Some(*k);
                                                                            }
                                                                            extract_epoch(v)
                                                                                .filter(|e| *e < new_epoch)
                                                                                .map(|_| *k)
                                                                        })
                                                                        .collect();
                                                                    let stale_dropped = stale_keys.len();
                                                                    for k in stale_keys {
                                                                        guard.remove(&k);
                                                                    }
                                                                    for shard in &shards_cloned {
                                                                        guard.insert(*shard, group_for_shards.clone());
                                                                    }
                                                                    tracing::warn!(
                                                                        group_id = %group_for_shards,
                                                                        new_epoch,
                                                                        stale_dropped,
                                                                        new_inserted = shards_cloned.len(),
                                                                        map_size_after = guard.len(),
                                                                        "DIAG[shard-map]: shard_to_group rebuilt for group"
                                                                    );
                                                                } else {
                                                                    for shard in &shards_cloned {
                                                                        guard.insert(*shard, group_for_shards.clone());
                                                                    }
                                                                }
                                                            });
                                                        }

                                                        // cross-group routing can use direct-send (RTT/2) instead of
                                                        // gossipsub mesh fan-out (50-200 ms hop). The cache key is the
                                                        // group_id; the value is the elected proposer's PeerId +
                                                        // tests, so `route()` always saw `try_get → None` and fell
                                                        // through to gossip even when SAVITRI_TX_ROUTER_DIRECT_SEND=1
                                                        // was set (memory: investigation_p1_p2_p3_2026-05-03).
                                                        if let Some(ref cache) = proposer_cache_external {
                                                            let proposer_node_id = announce.proposer.clone();
                                                            let proposer_addr_str = announce.member_addresses
                                                                .get(&proposer_node_id)
                                                                .cloned();
                                                            let group_id_for_cache = announce.group_id.clone();
                                                            let cache_clone = cache.clone();
                                                            tokio::spawn(async move {
                                                                let peer_id = match PeerId::from_str(&proposer_node_id) {
                                                                    Ok(p) => p,
                                                                    Err(e) => {
                                                                        debug!(
                                                                            proposer = %proposer_node_id,
                                                                            error = %e,
                                                                            "P1: cannot parse proposer node_id as PeerId, skipping cache update"
                                                                        );
                                                                        return;
                                                                    }
                                                                };
                                                                let multiaddr = proposer_addr_str.and_then(|s| {
                                                                    let with_peer = if s.contains("/p2p/") {
                                                                        s
                                                                    } else {
                                                                        format!("{}/p2p/{}", s, peer_id)
                                                                    };
                                                                    with_peer.parse::<libp2p::Multiaddr>().ok()
                                                                });
                                                                cache_clone.update_from_announce(
                                                                    group_id_for_cache,
                                                                    peer_id,
                                                                    multiaddr,
                                                                ).await;
                                                            });
                                                        }

                                                        // only to its own /savitri/group/<self>/tx; for cross-group
                                                        // forward (TxRouter::forward_via_gossip publishes on the
                                                        // TARGET group's topic) we relied on gossipsub fanout, which
                                                        // with flood_publish(false) + mesh_n=12 + cluster of ~16 LN
                                                        // measured gossip_rx=22K vs ~87K expected on LN-1).
                                                        //
                                                        // By subscribing to every group_X tx_topic, we force a real
                                                        // mesh to form for that topic across the cluster — publish
                                                        // then goes through the mesh (deterministic delivery) instead
                                                        // of the fanout cache. Side effect: every LN now also receives
                                                        // intra-group TX of OTHER groups, which the mempool's
                                                        // shard_filter drops from drain (kept_local=false). The dedup-
                                                        // on-block fix handles cleanup; the worst-case extra mempool
                                                        // pressure is bounded by the duplicate_cache (60s) at the
                                                        // gossipsub layer plus mempool TTL.
                                                        {
                                                            use std::sync::OnceLock;
                                                            use std::sync::Mutex;
                                                            use std::collections::HashSet;
                                                            static SUBSCRIBED_GROUP_TX: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
                                                            let set = SUBSCRIBED_GROUP_TX
                                                                .get_or_init(|| Mutex::new(HashSet::new()));
                                                            let already = match set.lock() {
                                                                Ok(g) => g.contains(&group_id_str),
                                                                Err(_) => false,
                                                            };
                                                            if !already {
                                                                let topic_str = format!("/savitri/group/{}/tx", group_id_str);
                                                                let topic = libp2p::gossipsub::IdentTopic::new(topic_str.clone());
                                                                let cmd_tx_clone = command_tx.clone();
                                                                let group_for_set = group_id_str.clone();
                                                                tokio::spawn(async move {
                                                                    if let Err(e) = cmd_tx_clone.send(SwarmCommand::Subscribe { topic }).await {
                                                                        tracing::warn!(error = %e, group = %group_for_set,
                                                                            "DIAG[cross-group-sub]: Subscribe command send failed");
                                                                    } else {
                                                                        tracing::warn!(group = %group_for_set,
                                                                            "DIAG[cross-group-sub]: subscribed to cross-group tx topic");
                                                                    }
                                                                });
                                                                if let Ok(mut g) = set.lock() {
                                                                    g.insert(group_id_str.clone());
                                                                }
                                                            }
                                                        }

                                                        let process_result = tokio::time::timeout(
                                                            std::time::Duration::from_secs(2),
                                                            group_manager.process_group_announcement(announce),
                                                        ).await;
                                                        let process_err = match process_result {
                                                            Ok(Ok(())) => None,
                                                            Ok(Err(e)) => Some(format!("{}", e)),
                                                            Err(_) => Some("timeout processing group announcement".to_string()),
                                                        };
                                                        if let Some(err_msg) = process_err {
                                                            warn!(error=%err_msg, "Failed to apply group announcement");
                                                        } else if group_members.contains(&local_node_id) {
                                                            if group_join_time.is_none() || !intra_group_mesh_established {
                                                                group_join_time = Some(Instant::now());
                                                                mesh_recovery_attempts = 0;
                                                            }
                                                            // CRITICAL FIX: Update p2p_group_peers ONLY when this is OUR group's announcement.
                                                            // In a decentralized network, we receive announcements for all groups; if we updated
                                                            // p2p_group_peers for every announcement, ConnectionEstablished could see wrong members
                                                            // (from the last processed group, possibly not ours).
                                                            info!(
                                                                group_id = %group_id_str,
                                                                members_count = group_members.len(),
                                                                "Converting group members from node_id to PeerId"
                                                            );
                                                            group_member_addresses.clear();
                                                            // Nessun sleep: il dial parte subito; un sleep bloccherebbe il swarm e impedirebbe
                                                            // all'altro nodo di completare l'handshake in ingresso (root cause mesh non formata).
                                                            let mut new_group_peers = HashSet::new();
                                                            for member_id in group_members {
                                                                if member_id == local_node_id {
                                                                    continue;
                                                                }

                                                                if let Ok(peer_id) = PeerId::from_str(&member_id) {
                                                                    new_group_peers.insert(peer_id);
                                                                    info!(
                                                                        peer_id = %peer_id,
                                                                        "Reached group member iteration (about to dial peer)"
                                                                    );
                                                                    // Dial se non ancora connesso. Entrambi i nodi possono dialare;
                                                                    // libp2p deduplica le connessioni.
                                                                    if !connected_p2p_group_peers.contains(&peer_id) {
                                                                        info!(
                                                                            peer_id = %peer_id,
                                                                            "Dialing group peer (libp2p deduplicates if both dial)"
                                                                        );
                                                                        let announce_addr = member_addresses.get(&member_id);
                                                                        info!(
                                                                            peer_id = %peer_id,
                                                                            member_id = %member_id,
                                                                            announce_addr = ?announce_addr,
                                                                            has_addr = announce_addr.is_some(),
                                                                            is_parseable = announce_addr.and_then(|a| a.parse::<Multiaddr>().ok()).is_some(),
                                                                            "Preparing to dial group member: info from announce"
                                                                        );
                                                                        let dial_opts = match announce_addr
                                                                            .and_then(|addr_str| addr_str.parse::<Multiaddr>().ok())
                                                                            .filter(|addr| !is_local_or_private_multiaddr(addr))
                                                                        {
                                                                            Some(addr) => {
                                                                                // Usa multiaddr completo con /p2p/peer_id per dial più affidabile (libp2p)
                                                                                let addr_with_peer = if addr.to_string().contains("/p2p/") {
                                                                                    addr.clone()
                                                                                } else {
                                                                                    format!("{}/p2p/{}", addr, peer_id).parse::<Multiaddr>().unwrap_or(addr.clone())
                                                                                };
                                                                                group_member_addresses.insert(peer_id, addr_with_peer.clone());
                                                                                info!(peer_id = %peer_id, addr = %addr_with_peer, "Dialing group member with address from announce");
                                                                                libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                                                                                    .addresses(vec![addr_with_peer])
                                                                                    .build()
                                                                            }
                                                                            None => {
                                                                                warn!(
                                                                                    peer_id = %peer_id,
                                                                                    member_id = %member_id,
                                                                                    "No address in announce for group member (masternode may have empty/tcp/0 multiaddr); dialing by peer_id only - may fail without DHT/discovery"
                                                                                );
                                                                                libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id).build()
                                                                            }
                                                                        };
                                                                        let log_addr = group_member_addresses.get(&peer_id).map(|m| m.to_string()).unwrap_or_else(|| "(peer_id only)".to_string());
                                                                        info!(
                                                                            peer_id = %peer_id,
                                                                            addr = %log_addr,
                                                                            transport_timeout_secs = transport::DIAL_TIMEOUT.as_secs(),
                                                                            "Starting dial to group peer (correlate with listener on other LN; after timeout expect OutgoingConnectionError)"
                                                                        );
                                                                        match swarm.dial(dial_opts) {
                                                                            Ok(()) => {
                                                                                info!(peer_id = %peer_id, "Dial initiated for group member");
                                                                            }
                                                                            Err(e) => {
                                                                                info!(peer_id = %peer_id, error = %e, "Failed to dial group member");
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    warn!("Invalid peer ID in group members: {}", member_id);
                                                                }
                                                            }

                                                            // Only place we set p2p_group_peers: when this is OUR group's announcement (group_members.contains above).
                                                            // Announcements for other groups are ignored here, so no overwrite with wrong members.
                                                            p2p_group_peers = new_group_peers.clone();
                                                            info!(
                                                                group_id = %group_id_str,
                                                                members = ?p2p_group_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                                                                "Updating p2p_group_peers with group members from announcement"
                                                            );
                                                            connected_p2p_group_peers.retain(|p| p2p_group_peers.contains(p));

                                                            if let Ok(pool) = connection_pool.try_lock() {
                                                                for peer_id in &p2p_group_peers {
                                                                    if pool.is_connected(peer_id) {
                                                                        connected_p2p_group_peers.insert(*peer_id);
                                                                    }
                                                                }
                                                            }
                                                            // No add_explicit_peer for group peers: they must stay as normal
                                                            // mesh peers so gossipsub GRAFT and subscription exchange works.
                                                            // Intra-group mesh forms naturally via gossipsub heartbeat.

                                                            info!(
                                                                group_id = %group_id_str,
                                                                expected_peers = p2p_group_peers.len(),
                                                                connected_peers = connected_p2p_group_peers.len(),
                                                                "Updated P2P group peers from announcement"
                                                            );

                                                            // Activate shard filter for block production:
                                                            // only include TX whose sender belongs to our assigned shards.
                                                            //
                                                            // *accepted* this announcement as current. Stale announces
                                                            // by process_group_announcement (returns Ok(())), so the
                                                            // outer check `process_err == None` is insufficient. Without
                                                            // this gate, a late-arriving epoch-N announce overwrites the
                                                            // shard_filter after the correct epoch-(N+1) announce was
                                                            // already installed → tx_router admits TX for the new group
                                                            // but shard_filter rejects them all as remote in drain →
                                                            // proposer observes staged_txs=0 forever.
                                                            if !announce_shards.is_empty() && announce_num_shards > 0 {
                                                                let is_current = group_manager
                                                                    .get_current_group_cached()
                                                                    .map(|g| g.epoch == announce_epoch
                                                                           && g.group_id == group_id_str)
                                                                    .unwrap_or(false);
                                                                if is_current {
                                                                    let pipeline_for_shard = Arc::clone(&pipeline);
                                                                    let shards = announce_shards.clone();
                                                                    let ns = announce_num_shards as usize;
                                                                    tokio::spawn(async move {
                                                                        pipeline_for_shard.set_shard_filter(ns, shards);
                                                                    });
                                                                    info!(
                                                                        group_id = %group_id_str,
                                                                        num_shards = announce_num_shards,
                                                                        assigned_count = announce_shards.len(),
                                                                        epoch = announce_epoch,
                                                                        "Shard filter activated for block production"
                                                                    );
                                                                } else {
                                                                    debug!(
                                                                        group_id = %group_id_str,
                                                                        announce_epoch,
                                                                        "Skipping shard_filter activation — announce not current in group_manager (stale/rejected)"
                                                                    );
                                                                }
                                                            }
                                                            // Emit GroupMembersUpdated per il maintenance task
                                                            let _ = event_tx.send(NetworkEvent::GroupMembersUpdated {
                                                                group_id: group_id_str.clone(),
                                                                members: p2p_group_peers.clone(),
                                                                addresses: group_member_addresses.clone(),
                                                                mesh_established: !p2p_group_peers.is_empty()
                                                                    && connected_p2p_group_peers.len() == p2p_group_peers.len(),
                                                            });

                                                            if !p2p_group_peers.is_empty() && connected_p2p_group_peers.len() == p2p_group_peers.len() {
                                                                group_mesh_established = true;
                                                                info!(
                                                                    group_id = %group_id_str,
                                                                    peer_count = connected_p2p_group_peers.len(),
                                                                    "Group mesh complete: all group peers connected (including any pre-existing connections)"
                                                                );
                                                                if let Some(current_group) = group_manager.get_current_group_cached() {
                                                                    let ack = GroupFormedAck {
                                                                        group_id: current_group.group_id.clone(),
                                                                        epoch: current_group.epoch,
                                                                        peer_id: local_peer.to_string(),
                                                                        timestamp: ack_timestamp(),
                                                                        connected_peers: connected_p2p_group_peers.len(),
                                                                        total_peers: p2p_group_peers.len(),
                                                                    };
                                                                    if let Ok(payload) = serde_json::to_vec(&ack) {
                                                                        if let Err(err) = swarm.behaviour_mut().gossipsub.publish(group_formed_topic.clone(), payload) {
                                                                            warn!(error=?err, "Failed to send group formed ACK");
                                                                        } else {
                                                                            info!("Group formed ACK sent (all peers connected)");
                                                                            pending_group_formed_ack = false;
                                                                        }
                                                                    }
                                                                }
                                                            } else {
                                                                group_mesh_established = false;
                                                            }
                                                        }

                                                        // Defer heavy init so we return from this handler quickly and the swarm can be polled
                                                        // (dial can then complete; see ANALISI_ARCHITETTURA_LIBP2P.md)
                                                        // NOTE: only init for groups where THIS LN is a member (not other groups' announcements)
                                                        if is_my_group
                                                            && group_manager.get_current_group_cached().is_some()
                                                        {
                                                            if deferred_group_init_tx.try_send(group_id_str.clone()).is_ok() {
                                                                info!(
                                                                    group_id = %group_id_str,
                                                                    "Deferred intra-group init so swarm can be polled (dial can complete)"
                                                                );
                                                            }
                                                        }
                                                        if pending_group_formed_ack && group_mesh_established {
                                                            if let Some(current_group) = group_manager.get_current_group_cached() {
                                                                let ack = GroupFormedAck {
                                                                    group_id: current_group.group_id.clone(),
                                                                    epoch: current_group.epoch,
                                                                    peer_id: local_peer.to_string(),
                                                                    timestamp: ack_timestamp(),
                                                                    connected_peers: connected_p2p_group_peers.len(),
                                                                    total_peers: p2p_group_peers.len(),
                                                                };
                                                                match serde_json::to_vec(&ack) {
                                                                    Ok(payload) => {
                                                                        if let Err(err) = swarm
                                                                            .behaviour_mut()
                                                                            .gossipsub
                                                                            .publish(group_formed_topic.clone(), payload)
                                                                        {
                                                                            warn!(error=?err, "Failed to send deferred group formed ACK to masternode");
                                                                        } else {
                                                                            info!(
                                                                                group_id = %current_group.group_id,
                                                                                epoch = current_group.epoch,
                                                                                local_peer = %local_peer,
                                                                                connected = connected_p2p_group_peers.len(),
                                                                                total = p2p_group_peers.len(),
                                                                                "Deferred group formed ACK sent after group assignment"
                                                                            );
                                                                            pending_group_formed_ack = false;
                                                                        }
                                                                    }
                                                                    Err(err) => {
                                                                        warn!(error=?err, "Failed to encode deferred group formed ACK");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    } // end dedup else
                                                    Err(err) => {
                                                        error!(
                                                            error = %err,
                                                            topic = %message.topic,
                                                            data_len = message.data.len(),
                                                            data_preview = %hex::encode(&message.data[..message.data.len().min(100)]),
                                                            expected_topic = %group_manager.get_topic().hash(),
                                                            "❌ CRITICAL: Failed to deserialize GroupAnnounce from gossipsub message"
                                                        );

                                                        if let Ok(json_str) = std::str::from_utf8(&message.data) {
                                                            warn!(
                                                                json_preview = %json_str.chars().take(200).collect::<String>(),
                                                                "Message content preview (first 200 chars)"
                                                            );
                                                        } else {
                                                            debug!("Message data is not valid UTF-8");
                                                        }

                                                        // Log of the topic per verificare che sia quello corretto
                                                        let expected_topic = group_manager.get_topic();
                                                        if message.topic != expected_topic.hash() {
                                                            error!(
                                                                received_topic = %message.topic,
                                                                expected_topic = %expected_topic.hash(),
                                                                "❌ Topic mismatch detected!"
                                                            );
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Messaggio ricevuto su topic diverso - log per debug
                                                debug!(
                                                    received_topic = %message.topic,
                                                    expected_topic = %group_manager.get_topic().hash(),
                                                    size = message.data.len(),
                                                    "📨 Message received on different topic (not group announcement)"
                                                );
                                            }

                                            // Gestisci altri topic intra-group (broadcast PoU, election, latency, ping, pong, vote)
                                            if message.topic == intra_group_latency_topic.hash()
                                                || message.topic == intra_group_pou_topic.hash()
                                                || message.topic == intra_group_election_topic.hash()
                                                || message.topic == intra_group_ping_topic.hash()
                                                || message.topic == intra_group_pong_topic.hash()
                                                || message.topic == intra_group_vote_topic.hash()
                                            {
                                                // Filtra messaggi intra-group da peer che non risultano registrati
                                                // (né come lightnode né come masternode).
                                                if !registered_lightnodes.contains(&propagation_source)
                                                    && !masternode_peers_set.contains(&propagation_source)
                                                {
                                                    debug!(
                                                        from = %propagation_source,
                                                        topic = %message.topic,
                                                        "Ignoring intra-group message from unregistered peer"
                                                    );
                                                } else {
                                                    let topic_kind = if message.topic == intra_group_latency_topic.hash() {
                                                        "latency"
                                                    } else if message.topic == intra_group_pou_topic.hash() {
                                                        info!(size = message.data.len(), "Received PoU message (raw)");
                                                        if let Ok(pou_share) = serde_json::from_slice::<PouScoreShare>(&message.data) {
                                                            info!(
                                                                node_id = %pou_share.node_id,
                                                                pou_score = pou_share.pou_score,
                                                                epoch = pou_share.epoch,
                                                                group_id = %pou_share.group_id,
                                                                "Received PoU share from group member"
                                                            );
                                                        }
                                                        "pou"
                                                    } else if message.topic == intra_group_ping_topic.hash() {
                                                        "ping"
                                                    } else if message.topic == intra_group_pong_topic.hash() {
                                                        "pong"
                                                    } else if message.topic == intra_group_vote_topic.hash() {
                                                        "vote"
                                                    } else {
                                                        "election"
                                                    };
                                                    info!(
                                                        topic = topic_kind,
                                                        topic_hash = %message.topic,
                                                        size = message.data.len(),
                                                        "Received intra-group broadcast message (swarm delivered)"
                                                    );
                                                    let comm = intra_group_comm.clone();
                                                    let topic_hash = message.topic.clone();
                                                    let data = message.data.clone();
                                                    let topic_kind_owned = topic_kind.to_string();
                                                    tokio::spawn(async move {
                                                        let mut c = comm.write().await;
                                                        if let Err(err) = c.process_message(&topic_hash, &data).await {
                                                            warn!(error=?err, topic = %topic_kind_owned, "Failed to process intra-group message");
                                                        }
                                                    });
                                                }
                                            }

                                            // V0.2 Phase 1 (Score Canonicity, issue #31) — Latency Canon receive path
                                            {
                                                let canon_topic_hash = {
                                                    let comm = intra_group_comm.read().await;
                                                    let t = comm.get_latency_canon_topic().await;
                                                    t.hash()
                                                };
                                                if message.topic == canon_topic_hash {
                                                    let comm = intra_group_comm.clone();
                                                    let data = message.data.clone();
                                                    tokio::spawn(async move {
                                                        let c = comm.read().await;
                                                        if let Err(e) = c.process_latency_canon_message(&data).await {
                                                            tracing::warn!(error = ?e, "latency_canon: process failed");
                                                        }
                                                    });
                                                }
                                            }
                                            // V0.2 Phase 2 (Lattice ordering, issue #32) — receive path
                                            {
                                                let local_gid = {
                                                    let comm = intra_group_comm.read().await;
                                                    comm
                                                        .group_manager
                                                        .get_current_group_cached()
                                                        .map(|g| g.group_id)
                                                };
                                                let local_gid_str = local_gid.clone().unwrap_or_default();
                                                let cell_topic_hash =
                                                    crate::lattice_runtime::cell_topic_for_group(&local_gid_str).hash();
                                                let att_topic_hash =
                                                    crate::lattice_runtime::attestation_topic_for_group(&local_gid_str).hash();
                                                let batch_topic_hash =
                                                    crate::lattice_runtime::batch_topic_for_group(&local_gid_str).hash();
                                                let rt_state = lattice_runtime_state.clone();
                                                let data = message.data.clone();
                                                if message.topic == cell_topic_hash {
                                                    tokio::spawn(async move {
                                                        if let Err(e) = crate::lattice_runtime::LatticeRuntime::process_cell_message(&rt_state, local_gid.as_deref(), &data).await {
                                                            tracing::warn!(error = ?e, "lattice_cell: process failed");
                                                        }
                                                    });
                                                } else if message.topic == att_topic_hash {
                                                    tokio::spawn(async move {
                                                        if let Err(e) = crate::lattice_runtime::LatticeRuntime::process_attestation_message(&rt_state, local_gid.as_deref(), &data).await {
                                                            tracing::warn!(error = ?e, "lattice_attestation: process failed");
                                                        }
                                                    });
                                                } else if message.topic == batch_topic_hash {
                                                    tokio::spawn(async move {
                                                        if let Err(e) = crate::lattice_runtime::LatticeRuntime::process_batch_message(&rt_state, local_gid.as_deref(), &data).await {
                                                            tracing::warn!(error = ?e, "lattice_batch: process failed");
                                                        }
                                                    });
                                                }
                                            }
                                            // Handle PoU ACK - confirms our PoU share was received by a peer
                                            if message.topic == intra_group_pou_ack_topic.hash() {
                                                if let Ok(ack) = serde_json::from_slice::<PouScoreAck>(&message.data) {
                                                    if ack.ack_for == local_peer.to_string() {
                                                        info!(
                                                            from = %ack.from,
                                                            "PoU ACK received: peer confirmed receipt of our PoU share"
                                                        );
                                                    }
                                                }
                                            }

                                            // Handle whitelist ACK from MN (handshake 3-fasi, fase 2)
                                            if message.topic == election_ack_topic.hash() {
                                                match serde_json::from_slice::<crate::p2p::intra_group::ProposerWhitelistAck>(&message.data) {
                                                    Ok(ack) => {
                                                        if ack.target_proposer_peer_id == local_peer.to_string() {
                                                            info!(
                                                                masternode = %ack.masternode_peer_id,
                                                                group_id = %ack.group_id,
                                                                round_id = ack.round_id,
                                                                validity_secs = ack.validity_secs,
                                                                "✅ [LN] Whitelist ACK received from MN - forwarding to proposer"
                                                            );
                                                            if let Err(e) = whitelist_ack_tx.send(ack) {
                                                                warn!(error = %e, "Failed to forward whitelist ACK to proposer (channel closed)");
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        debug!(error = %e, "Failed to deserialize whitelist ACK (may not be for us)");
                                                    }
                                                }
                                            }

                                            if message.topic == intra_group_proposal_topic.hash() {
                                                // Anche le proposte di blocco intra-group devono provenire da peer registrati.
                                                if !registered_lightnodes.contains(&propagation_source)
                                                    && !masternode_peers_set.contains(&propagation_source)
                                                {
                                                    debug!(
                                                        from = %propagation_source,
                                                        "Ignoring intra-group block proposal from unregistered peer"
                                                    );
                                                } else if let Ok((proposer_id, proposal)) = serde_json::from_slice::<(String, crate::proposer::BlockProposal)>(&message.data) {
                                                    let comm = intra_group_comm.clone();
                                                    let h = proposal.height;
                                                    let r = proposal.round_id;
                                                    tokio::spawn(async move {
                                                        comm.read().await.receive_proposal(proposer_id, proposal).await;
                                                    });
                                                    debug!(
                                                        from = %propagation_source,
                                                        height = h,
                                                        round = r,
                                                        "Received block proposal from intra-group proposer"
                                                    );
                                                } else {
                                                    debug!("Failed to deserialize intra-group block proposal");
                                                }
                                            }

                                            // Gestisci topic transazioni (rete globale)
                                            if message.topic == tx_topic.hash() {
                                                // Architettura: i MN non devono trasportare TX gossip.
                                                if masternode_peers_set.contains(&propagation_source) {
                                                    debug!(
                                                        from = %propagation_source,
                                                        "Ignoring transaction gossip from masternode peer"
                                                    );
                                                } else {
                                                    // PERF: Accept TX from ALL gossipsub peers on the global
                                                    // /savitri/tx/1 topic. The previous registered_lightnodes
                                                    // filter silently dropped TX from nodes that hadn't sent
                                                    // a registration message, starving the proposer's mempool.
                                                    // real security gate — not peer registration.
                                                    match decode_gossip(&message.data) {
                                                        Ok(GossipMessage::Tx(tx_msg)) => {
                                                            let tx_bytes = tx_msg.tx.len().max(tx_msg.data.len());
                                                            if !tx_msg.tx.is_empty() {
                                                                tx_batch_buffer.push(tx_msg.tx.clone());
                                                                pou_observations_for_gossip.record_tx_validation(
                                                                    &propagation_source.to_string(), true);
                                                                debug!(
                                                                    from = %propagation_source,
                                                                    size = tx_bytes,
                                                                    "Transaction received via gossipsub (network-wide)"
                                                                );
                                                            } else if !tx_msg.data.is_empty() {
                                                                tx_batch_buffer.push(tx_msg.data.clone());
                                                                pou_observations_for_gossip.record_tx_validation(
                                                                    &propagation_source.to_string(), true);
                                                                debug!(
                                                                    from = %propagation_source,
                                                                    size = tx_msg.data.len(),
                                                                    "Transaction received via gossipsub (network-wide)"
                                                                );
                                                            }
                                                        }
                                                        Ok(GossipMessage::Transaction(tx_bytes)) => {
                                                            if !tx_bytes.is_empty() {
                                                                let size = tx_bytes.len();
                                                                tx_batch_buffer.push(tx_bytes);
                                                                pou_observations_for_gossip.record_tx_validation(
                                                                    &propagation_source.to_string(), true);
                                                                debug!(
                                                                    from = %propagation_source,
                                                                    size,
                                                                    "Transaction received via gossipsub (network-wide)"
                                                                );
                                                            }
                                                        }
                                                        Err(err) => {
                                                            // Malformed gossip payload → mark peer's TX as invalid.
                                                            pou_observations_for_gossip.record_tx_validation(
                                                                &propagation_source.to_string(), false);
                                                            debug!(
                                                                from = %propagation_source,
                                                                error = ?err,
                                                                "Failed to decode TX gossip — marked peer tx invalid"
                                                            );
                                                        }
                                                        Ok(GossipMessage::HaveTx(have_tx)) => {
                                                            // Announce-hash: determine the peer that published this
                                                            // HaveTx (the one we know for sure has the bytes).
                                                            // Priority: 1) source_peer embedded in HaveTx message
                                                            //           2) message.source (gossipsub original author)
                                                            //           3) propagation_source (last resort, usually wrong)
                                                            let original_source = if !have_tx.source_peer.is_empty() {
                                                                PeerId::from_bytes(&have_tx.source_peer)
                                                                    .unwrap_or(propagation_source)
                                                            } else {
                                                                message.source.unwrap_or(propagation_source)
                                                            };
                                                            // Append this source to the known-holders list for each
                                                            // hash. After multiple HaveTx rounds (including re-announce
                                                            // from peers that fetched and stored the bytes), the list
                                                            // grows so fetch requests can load-balance across holders.
                                                            tx_store_for_spawn.record_announcements(&have_tx.tx_hashes, original_source);

                                                            // ALL nodes fetch missing bytes — not only the current
                                                            // proposer. Rationale: when the proposer rotated, the new
                                                            // proposer only had the TX it had fetched during its own
                                                            // turn, leading to mempool gaps and near-empty blocks.
                                                            // Every LN keeping a complete mempool is the fix.
                                                            //
                                                            // Load balancing: for each missing hash, pick_fetch_source
                                                            // returns a pseudo-random peer from the known-holders list
                                                            // (not always the original RPC publisher). Group hashes by
                                                            // chosen source so we send one request per source, not one
                                                            // request per hash. As peers fetch and re-announce, the
                                                            // source pool grows and load distributes organically.
                                                            use std::collections::HashMap as StdHashMap;
                                                            let mut by_source: StdHashMap<PeerId, Vec<[u8; 32]>> = StdHashMap::new();
                                                            for h in &have_tx.tx_hashes {
                                                                if tx_store_for_spawn.contains(h) { continue; }
                                                                let src = tx_store_for_spawn
                                                                    .pick_fetch_source(h)
                                                                    .unwrap_or(original_source);
                                                                if src == local_peer { continue; } // don't fetch from self
                                                                by_source.entry(src).or_default().push(*h);
                                                            }
                                                            for (src, hashes) in by_source {
                                                                let batch_size = hashes.len();
                                                                let req = crate::p2p::tx_fetch_protocol::TxFetchRequest { hashes };
                                                                swarm.behaviour_mut().tx_fetch.send_request(&src, req);
                                                                debug!(
                                                                    from = %src,
                                                                    batch = batch_size,
                                                                    "TX fetch request sent (load-balanced across known holders)"
                                                                );
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }

                                            // Intra-group TX topic: route received TXs into the
                                            // local mempool so the elected proposer can include them.
                                            if message.topic == intra_group_tx_topic.hash() {
                                                // Tier 8: Prometheus counter on every cross-group RX.
                                                // Gated on `rpc` because tx_router pulls the savitri-rpc
                                                // TxRouter trait; in mobile/desktop-only builds the
                                                // counters are skipped (acceptable: telemetry only).
                                                #[cfg(feature = "rpc")]
                                                {
                                                    crate::tx_router::metrics::TxRoutingMetrics::inc_cross_group_rx();
                                                    crate::tx_router::metrics::TxRoutingMetrics::observe_payload_size(message.data.len());
                                                }
                                                // count cross-group TX received via gossip (one log
                                                // every 100). Kept until Grafana confirms Tier 8 metric.
                                                {
                                                    use std::sync::atomic::{AtomicU64, Ordering};
                                                    static RX_CTR: AtomicU64 = AtomicU64::new(0);
                                                    let n = RX_CTR.fetch_add(1, Ordering::Relaxed) + 1;
                                                    if n == 1 || n % 100 == 0 {
                                                        tracing::warn!(
                                                            rx_total = n,
                                                            payload_len = message.data.len(),
                                                            "DIAG[#50] Tier 4: intra_group_tx_topic RX"
                                                        );
                                                    }
                                                }
                                                // pipeline counter even before decode so we can tell
                                                // raw RX vs successful decode rate.
                                                crate::observability::PipelineObsMetrics::inc_gossip_rx_received();
                                                match decode_gossip(&message.data) {
                                                    Ok(GossipMessage::Tx(tx_msg)) => {
                                                        crate::observability::PipelineObsMetrics::inc_gossip_rx_decoded();
                                                        if !tx_msg.tx.is_empty() {
                                                            tx_batch_buffer.push(tx_msg.tx.clone());
                                                        } else if !tx_msg.data.is_empty() {
                                                            tx_batch_buffer.push(tx_msg.data.clone());
                                                        }
                                                    }
                                                    Ok(GossipMessage::Transaction(tx_bytes)) => {
                                                        crate::observability::PipelineObsMetrics::inc_gossip_rx_decoded();
                                                        if !tx_bytes.is_empty() {
                                                            tx_batch_buffer.push(tx_bytes);
                                                        }
                                                    }
                                                    Ok(_) => {
                                                        // Decoded but not a TX variant.
                                                        crate::observability::PipelineObsMetrics::inc_gossip_rx_decoded();
                                                    }
                                                    Err(_) => {
                                                        crate::observability::PipelineObsMetrics::inc_gossip_rx_decode_fail();
                                                    }
                                                }
                                            }

                                            // Gestisci topic PoU
                                            if message.topic == pou_topic.hash() {
                                                match serde_json::from_slice::<PouBroadcast>(&message.data) {
                                                    Ok(report) => {
                                                        match PeerId::from_str(&report.peer_id) {
                                                            Ok(peer_id) => {
                                                                if peer_id != local_peer {
                                                                    let score = compute_remote_pou(
                                                                        &report,
                                                                        report.uptime_claim.node_id,
                                                                        &pou_scoring,
                                                                        &mut remote_u_history,
                                                                        &mut remote_prev_pou,
                                                                    );

                                                                    if let Some(new_score) = score {
                                                                        let pou_h = pou_state_handle.clone();
                                                                        let pid = peer_id;
                                                                        let ep = report.epoch;
                                                                        tokio::spawn(async move {
                                                                            pou_h.record_report(&pid, ep, new_score).await;
                                                                        });
                                                                        debug!(
                                                                            peer = %peer_id,
                                                                            score = new_score,
                                                                            epoch = report.epoch,
                                                                            "PoU report received from peer (network-wide)"
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                            Err(_) => {
                                                                warn!("Invalid peer_id in PoU report");
                                                            }
                                                        }
                                                    }
                                                    Err(err) => {
                                                        warn!(error=?err, "failed to decode PoU gossip payload");
                                                    }
                                                }
                                            }

                                            // Block+certificate in un solo messaggio (MN → LN, topic block_final)
                                            if message.topic == block_final_topic.hash() {
                                                match serde_json::from_slice::<BlockWithCertificateWire>(&message.data) {
                                                    Ok(msg) => {
                                                        // PERF: Skip duplicate certs for same height.
                                                        // With 5 MNs, we receive 5 BlockWithCertificate for each block.
                                                        // Only process the first one — saves 80% of commit processing.
                                                        let block_height = msg.block.header.exec_height;
                                                        if !committed_heights.insert(block_height) {
                                                            debug!(
                                                                height = block_height,
                                                                "Skipping duplicate BlockWithCertificate (already committed)"
                                                            );
                                                            continue;
                                                        }
                                                        let cert_bytes = match serde_json::to_vec(&msg.certificate) {
                                                            Ok(b) => b,
                                                            Err(e) => {
                                                                warn!(error=%e, "BlockWithCertificate: failed to re-encode cert");
                                                                continue;
                                                            }
                                                        };
                                                        let cert = match decode_consensus_cert_from_masternode(&cert_bytes) {
                                                            Ok(c) => c,
                                                            Err(e) => {
                                                                warn!(error=%e, "BlockWithCertificate: failed to decode cert");
                                                                if let Some(src) = message.source {
                                                                    pou_observations_for_gossip
                                                                        .record_block_validation(&src.to_string(), false);
                                                                }
                                                                continue;
                                                            }
                                                        };
                                                        let committee_size = masternode_peers_set.len().max(1);
                                                        if validate_certificate_masternode(&cert, committee_size).is_err() {
                                                            debug!(height = cert.height, "BlockWithCertificate: cert validation failed");
                                                            if let Some(src) = message.source {
                                                                pou_observations_for_gossip
                                                                    .record_block_validation(&src.to_string(), false);
                                                            }
                                                            continue;
                                                        }
                                                        if let Some(src) = message.source {
                                                            pou_observations_for_gossip
                                                                .record_block_validation(&src.to_string(), true);
                                                        }
                                                        let source = message.source.unwrap_or_else(|| local_peer);
                                                        block_sync_manager.update_peer_height(source.clone(), msg.block.header.exec_height);
                                                        // PERF: Notify pipeline IMMEDIATELY that this height is certified.
                                                        // This unblocks the proposer to propose the next block without
                                                        // waiting for the 5s RocksDB commit to complete.
                                                        {
                                                            let igc_notify = intra_group_comm.clone();
                                                            let height_to_certify = msg.block.header.exec_height;
                                                            let cert_group = msg.certificate.group_id.clone();
                                                            tokio::spawn(async move {
                                                                if !cert_group.is_empty() {
                                                                    igc_notify.read().await.notify_block_certified_for_group(height_to_certify, &cert_group);
                                                                } else {
                                                                    igc_notify.read().await.notify_block_certified(height_to_certify);
                                                                }
                                                            });
                                                        }
                                                        // qui (BlockWithCertificate path). Storage composite-key
                                                        // (an earlier fix) elimina il rischio di clobber, e popolare
                                                        // per tx_getTransaction multi-group.
                                                        let cert_roots = {
                                                            let sr: [u8; 32] = msg.certificate.state_root[..32].try_into().unwrap_or([0u8; 32]);
                                                            let tr: [u8; 32] = msg.certificate.tx_root[..32].try_into().unwrap_or([0u8; 32]);
                                                            Some((sr, tr))
                                                        };
                                                        let cert_parent_hash = Some(msg.certificate.parent_hash);
                                                        match prepare_remote_block(storage_clone.as_ref(), &msg.block, &source, cert_roots, cert_parent_hash, Some(msg.certificate.timestamp)) {
                                                            Ok(Some(pending_data)) => {
                                                                let block_hash = msg.block.hash;
                                                                let storage_f = Arc::clone(&storage_clone);
                                                                let integrity_f = integrity_events.clone();
                                                                let pou_state_f = pou_state_handle.clone();
                                                                let pipeline_f = Arc::clone(&pipeline);
                                                                let known_peer_accounts_f = known_peer_accounts.clone();
                                                                let masternode_addr = match priority_targets.try_read() {
                                                                    Ok(pt) => pt.keys().next().map(|p| p.to_string()).unwrap_or_default(),
                                                                    Err(_) => String::new(),
                                                                };
                                                                let cert_pending_f = Arc::clone(&certificate_pending_handle);
                                                                let dag_f = Arc::clone(&dag_manager);
                                                                // Capture group_id now, before the spawn, to avoid a race
                                                                // where the group changes before the async commit completes.
                                                                let dag_group_id_f = group_manager.get_current_group_cached()
                                                                    .map(|g| g.group_id.clone())
                                                                    .unwrap_or_else(|| "group_0_0_0".to_string());
                                                                // Cert's group_id — used to route the commit to the per-group
                                                                // storage lane (composite-key). Empty string means "legacy
                                                                // single-lane"; the storage layer treats that as the default.
                                                                let commit_group_id_f = msg.certificate.group_id.clone();
                                                                tokio::spawn(async move {
                                                                    let peer_accounts_map: HashMap<PeerId, PeerInfo> = known_peer_accounts_f
                                                                        .iter()
                                                                        .map(|(pid, account)| (*pid, PeerInfo {
                                                                            account: *account,
                                                                            peer_id: pid.to_string(),
                                                                            address: String::new(),
                                                                            priority: false,
                                                                        }))
                                                                        .collect();
                                                                    // PERF: Do NOT hold pipeline lock during RocksDB commit.
                                                                    // Pass None — avoids blocking the proposer's mempool drain.
                                                                    let commit_group_arg = if commit_group_id_f.is_empty() {
                                                                        None
                                                                    } else {
                                                                        Some(commit_group_id_f.as_str())
                                                                    };
                                                                    let commit_result = finalize_remote_block_commit(
                                                                        storage_f.as_ref(),
                                                                        &pending_data,
                                                                        &integrity_f,
                                                                        None,
                                                                        &pou_state_f,
                                                                        &peer_accounts_map,
                                                                        &known_peer_accounts_f,
                                                                        &masternode_addr,
                                                                        0,
                                                                        0,
                                                                        commit_group_arg,
                                                                    ).await;
                                                                    if commit_result {
                                                                        // Update mempool nonces after remote block commit.
                                                                        // pipeline was None during commit to avoid lock contention
                                                                        // with RocksDB; now that commit is done, apply nonce updates.
                                                                        //
                                                                        // IMPORTANT: also run the empty-block branch
                                                                        // (signed_txs.is_empty()) — on_block_committed_with_nonces
                                                                        // triggers clear_committed_pending_nonces which drops
                                                                        // stale pending_nonces from previous proposals. Without
                                                                        // this, empty certified blocks leave pending_nonces
                                                                        // zombie and the next drain produces valid=0
                                                                        // invalid=N via nonce mismatch (observed on testnet
                                                                        // chain committed 9 empty blocks while pending
                                                                        // every 2000-TX drain).
                                                                        let mut sender_max_nonce: std::collections::HashMap<Vec<u8>, u64> = std::collections::HashMap::new();
                                                                        for tx in &pending_data.signed_txs {
                                                                            let addr = crate::p2p::block::normalize_address_bytes(&tx.from);
                                                                            let entry = sender_max_nonce.entry(addr).or_insert(0);
                                                                            if tx.nonce >= *entry {
                                                                                *entry = tx.nonce + 1;
                                                                            }
                                                                        }
                                                                        // Include receivers: needed to trigger promote(receiver, 0)
                                                                        // for queued_pool entries keyed on freshly-funded accounts
                                                                        // (e.g. faucet → new sender_X waiting to release nonce=0..N).
                                                                        for tx in &pending_data.signed_txs {
                                                                            let receiver_addr = crate::p2p::block::normalize_address_bytes(&tx.to);
                                                                            sender_max_nonce.entry(receiver_addr.clone()).or_insert_with(|| {
                                                                                crate::storage::BlockAndAccountStorage::get_account(
                                                                                    storage_f.as_ref(),
                                                                                    &receiver_addr,
                                                                                )
                                                                                    .ok()
                                                                                    .flatten()
                                                                                    .map(|acc| acc.nonce)
                                                                                    .unwrap_or(0)
                                                                            });
                                                                        }
                                                                        let mut nonce_updates = std::collections::HashMap::new();
                                                                        for (addr, new_nonce) in &sender_max_nonce {
                                                                            let sid = pipeline_f.get_sender_id_for_address(addr);
                                                                            nonce_updates.insert(sid, *new_nonce);
                                                                        }
                                                                        // Always call on_block_committed_with_nonces so the
                                                                        // pending_nonces map is cleaned even for empty blocks.
                                                                        // Pass block_hash so we clear ONLY this block's in-flight
                                                                        //
                                                                        // `vec![]` for committed_handles with the comment
                                                                        // helper find_handles_from_signed_txs now exists
                                                                        // (savitri-mempool/integration.rs:1574). Without this,
                                                                        // the mempool legacy step 1 was a no-op: TX in the
                                                                        // mempool of every LN of the group (including
                                                                        // non-proposer caches) were ONLY removed by step 1b
                                                                        // (purge stale-by-nonce, only senders present in this
                                                                        // measured ~98 TX/s evicted via TTL on LN-1 alone
                                                                        // proper handle resolution, TX get removed at commit
                                                                        // time as designed.
                                                                        let committed_handles = pipeline_f
                                                                            .find_handles_from_signed_txs(&pending_data.signed_txs);
                                                                        pipeline_f.on_block_committed_with_nonces_and_hash(committed_handles, &nonce_updates, Some(block_hash));
                                                                        // le TX committate. on_block_committed riceve vec![] per
                                                                        // lunghezza di pending_data.signed_txs che è la vera
                                                                        // misura di TX contenute nel blocco certificato.
                                                                        if !pending_data.signed_txs.is_empty() {
                                                                            savitri_mempool::mempool::metrics::increment_confirmed_batch(
                                                                                pending_data.signed_txs.len() as u64
                                                                            );
                                                                        }
                                                                        tracing::info!(
                                                                            height = pending_data.block.height,
                                                                            accounts_updated = nonce_updates.len(),
                                                                            total_txs = pending_data.signed_txs.len(),
                                                                            "Updated mempool nonces from remote certified block (block_final path)"
                                                                        );
                                                                        let mut cp = cert_pending_f.lock().await;
                                                                        cp.mark_committed(block_hash);
                                                                        // Register committed block in DAG using pre-captured group_id
                                                                        let group_id = dag_group_id_f.clone();
                                                                        register_block_in_dag(
                                                                            &dag_f,
                                                                            &pending_data.block,
                                                                            &pending_data.signed_txs,
                                                                            &group_id,
                                                                            vec![pending_data.block.parent_hash],
                                                                            0,
                                                                        ).await;
                                                                        info!(
                                                                            height = pending_data.block.height,
                                                                            hash = %hex::encode(block_hash),
                                                                            "📥 [LN←MN] Block committed from block_final (block+cert single message)"
                                                                        );
                                                                    }
                                                                });
                                                                info!(
                                                                    height = msg.block.header.exec_height,
                                                                    hash = %hex::encode(block_hash),
                                                                    "📥 [LN←MN] BlockWithCertificate received, finalize spawned"
                                                                );
                                                            }
                                                            Ok(None) => {
                                                                debug!(height = msg.block.header.exec_height, "BlockWithCertificate: prepare_remote_block skipped");
                                                            }
                                                            Err(e) => {
                                                                warn!(error=?e, height = msg.block.header.exec_height, "BlockWithCertificate: prepare_remote_block failed");
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        warn!(error=%e, data_len = message.data.len(), "BlockWithCertificate: decode failed (check wire format MN↔LN)");
                                                    }
                                                }
                                            }

                                            // Gestisci topic blocchi (rete globale, gossipsub): prepare, register pending, forward
                                            if message.topic == block_topic.hash() {
                                                // I blocchi provenienti da peer non registrati non are considerati.
                                                if !registered_lightnodes.contains(&propagation_source)
                                                    && !masternode_peers_set.contains(&propagation_source)
                                                {
                                                    debug!(
                                                        from = %propagation_source,
                                                        "Ignoring block gossip from unregistered peer"
                                                    );
                                                } else {
                                                    match decode_gossip(&message.data) {
                                                        Ok(GossipMessage::Block(block_msg)) => {
                                                            let height = block_msg.header.exec_height;
                                                            block_sync_manager.update_peer_height(propagation_source.clone(), height);
                                                            let tx_count = block_msg.txs.len() as u32;
                                                            let hash_hex = hex::encode(block_msg.hash);
                                                            info!(
                                                                from = %propagation_source,
                                                                height,
                                                                hash = %hash_hex,
                                                                txs = tx_count,
                                                                "📥 [LN<-LN] Step 1: Block received via gossipsub from lightnode peer"
                                                            );
                                                            // Prepare remote block and register for certificate finality (no cert roots from gossip)
                                                            match prepare_remote_block(storage_clone.as_ref(), &block_msg, &propagation_source, None, None, None) {
                                                                Ok(Some(pending_data)) => {
                                                                    let block_hash_arr = block_msg.hash;
                                                                    let cert_pending_result = certificate_pending_handle.try_lock();
                                                                    if cert_pending_result.is_err() {
                                                                        debug!("certificate_pending lock busy, deferring block registration");
                                                                    } else {
                                                                    let mut cert_pending = cert_pending_result.unwrap();
                                                                    if cert_pending.is_committed(&block_hash_arr) {
                                                                        debug!(
                                                                            height,
                                                                            hash=%hash_hex,
                                                                            "Block already committed, skipping registration"
                                                                        );
                                                                    } else {
                                                                        // (height, group_id) così il cert può fare
                                                                        // dal block.hash registrato (cert_roots reali
                                                                        // vs zero al momento of the register).
                                                                        let local_group_id_for_reg = group_manager
                                                                            .get_current_group_cached()
                                                                            .map(|g| g.group_id.clone())
                                                                            .unwrap_or_default();
                                                                        cert_pending.register_pending_with_group(
                                                                            block_hash_arr,
                                                                            height,
                                                                            local_group_id_for_reg,
                                                                            pending_data.clone(),
                                                                            propagation_source.clone(),
                                                                        );
                                                                    }
                                                                    info!(
                                                                        from = %propagation_source,
                                                                        height,
                                                                        hash = %hash_hex,
                                                                        "📥 [LN<-LN] Step 2: Block registered for certificate finality"
                                                                    );
                                                                    } // end else (try_lock success)
                                                                }
                                                                Ok(None) => {
                                                                    debug!(height, hash=%hash_hex, "prepare_remote_block skipped (e.g. already have block)");
                                                                }
                                                                Err(e) => {
                                                                    debug!(error=?e, height, "prepare_remote_block skipped (e.g. already have block)");
                                                                }
                                                            }
                                                            // The block_receiver channel is an orphaned side-path: its Receiver
                                                            // end in main.rs is created but never consumed, so every try_send
                                                            // here fills the 8192-slot buffer once, then fails forever with
                                                            // "no available capacity" warnings — the real block registration
                                                            // already happened via cert_pending.register_pending above
                                                            // (Step 2). Keeping this dead-channel forward produced the storm
                                                            // that saturated the MN finalization pipeline under load.
                                                            let _ = tx_count; // silence unused warning
                                                            let _ = block_msg; // consumed by prepare_remote_block already
                                                        }
                                                        Ok(GossipMessage::HaveBlock(have)) => {
                                                            block_sync_manager.update_peer_height(propagation_source.clone(), have.exec_height);
                                                            info!(
                                                                from = %propagation_source,
                                                                height = have.exec_height,
                                                                hash = %hex::encode(have.hash),
                                                                "📥 [LN<-LN] HaveBlock received via gossipsub from lightnode peer"
                                                            );
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }

                                            // Masternode sends JSON (BlockCertificate); try that first, then bincode ConsensusMessage.
                                            if message.topic == consensus_cert_topic.hash() {
                                                // PERF: Quick height dedup for cert topic too
                                                // (complements block_final dedup above)
                                                debug!(
                                                    data_len = message.data.len(),
                                                    source = ?message.source,
                                                    "📥 [LN←MN] Message received on consensus cert topic"
                                                );
                                                let cert_and_from_mn = decode_consensus_cert_from_masternode(&message.data)
                                                    .ok()
                                                    .map(|c| (c, true))
                                                    .or_else(|| {
                                                        decode_consensus(&message.data).ok().and_then(|m| {
                                                            match m {
                                                                ConsensusMessage::Certificate(c) => Some((c, false)),
                                                                _ => None,
                                                            }
                                                        })
                                                    });
                                                match cert_and_from_mn {
                                                    Some((cert, from_masternode)) => {
                                                        if let Some(src) = message.source {
                                                            block_sync_manager.update_peer_height(src.clone(), cert.height);
                                                        }
                                                        info!(
                                                            height = cert.height,
                                                            round = cert.round,
                                                            voters = cert.voters.len(),
                                                            hash = %hex::encode(cert.block_hash),
                                                            from_mn_format = from_masternode,
                                                            "📥 [LN←MN] Masternode ACK: consensus certificate received (block approval)"
                                                        );
                                                        // periodic ratio logger can compute received vs match.
                                                        crate::observability::ConsensusObsMetrics::inc_cert_received();

                                                        // match_ratio=8.97% on a healthy cluster — most
                                                        // received cert events are noise (cert for blocks
                                                        // already committed, or for groups other than ours
                                                        // — the gossip topic /savitri/consensus/cert/1 is
                                                        // mesh-wide and every LN sees every group's cert).
                                                        // pass + a certificate_pending lookup; with 36
                                                        // cert/s on a 3-group cluster, 75% of that work is
                                                        // wasted CPU.  Drop cert that target a group_id
                                                        // different from our own current group BEFORE the
                                                        // SAVITRI_CERT_FILTER_BY_GROUP=1 (default off
                                                        // until we confirm no edge case relies on cross-
                                                        // group cert observation, e.g. block sync probes).
                                                        // We DO NOT filter empty group_id (legacy / cert-
                                                        // only fallback path) since those have no
                                                        // group_id information.
                                                        let cert_filter_enabled: bool = std::env::var("SAVITRI_CERT_FILTER_BY_GROUP")
                                                            .ok()
                                                            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                                                            .unwrap_or(false);
                                                        if cert_filter_enabled && !cert.group_id.is_empty() {
                                                            let local_gid = group_manager
                                                                .get_current_group_cached()
                                                                .map(|g| g.group_id.clone())
                                                                .unwrap_or_default();
                                                            if !local_gid.is_empty() && cert.group_id != local_gid {
                                                                use std::sync::atomic::{AtomicU64, Ordering};
                                                                static FILTERED_CTR: AtomicU64 = AtomicU64::new(0);
                                                                let n = FILTERED_CTR.fetch_add(1, Ordering::Relaxed) + 1;
                                                                if n == 1 || n % 500 == 0 {
                                                                    debug!(
                                                                        total_filtered = n,
                                                                        cert_group = %cert.group_id,
                                                                        local_group = %local_gid,
                                                                        height = cert.height,
                                                                        "DIAG[NEW-1]: cert dropped (different group)"
                                                                    );
                                                                }
                                                                continue;
                                                            }
                                                        }
                                                        let committee_size = masternode_peers_set.len().max(1);
                                                        let valid = if from_masternode {
                                                            validate_certificate_masternode(&cert, committee_size).is_ok()
                                                        } else {
                                                            validate_certificate(&cert, committee_size).is_ok()
                                                        };
                                                        if !valid {
                                                            crate::observability::ConsensusObsMetrics::inc_cert_invalid();
                                                            debug!(
                                                                committee_size,
                                                                voters_count = cert.voters.len(),
                                                                "Certificate validation failed"
                                                            );
                                                        } else {
                                                            crate::observability::ConsensusObsMetrics::inc_cert_valid();
                                                            // Notify pipeline IMMEDIATELY that this height is certified.
                                                            // Without this, the proposer's `last_certified_height` atomic
                                                            // never advances when the MN publishes cert-only messages on
                                                            // /savitri/consensus/cert/1 (the "fallback / legacy" path used
                                                            // when the MN does not have the full block to bundle). Only
                                                            // the block_final topic handler was calling this, so the
                                                            // proposer's pipeline filled up (depth 8) and block production
                                                            // halted with `finalized` stuck at the last cert received on
                                                            // block_final. Mirrors the same call in the block_final
                                                            // handler above.
                                                            {
                                                                let igc_notify = intra_group_comm.clone();
                                                                let height_to_certify = cert.height;
                                                                // B2 fix: ConsensusCertificate now carries group_id from
                                                                // BlockCertificateWire. When non-empty, update the per-group
                                                                // certified height so the proposer's pipeline doesn't stall on
                                                                // cert-only fallback broadcasts from the MN.
                                                                let cert_group = cert.group_id.clone();
                                                                tokio::spawn(async move {
                                                                    let igc = igc_notify.read().await;
                                                                    if !cert_group.is_empty() {
                                                                        igc.notify_block_certified_for_group(height_to_certify, &cert_group);
                                                                    } else {
                                                                        igc.notify_block_certified(height_to_certify);
                                                                    }
                                                                });
                                                            }
                                                            // RIMOSSO. Lo storage è ora composite-key (an earlier fix commit
                                                            // without clobber. Filtrare per local group impediva al
                                                            // LN-RPC di popolare CF_BLOCKS con i blocchi degli altri
                                                            // gruppi → tx_getTransaction NotFound → istogramma
                                                            // ricevuto via gossip block_topic.
                                                            // sopra) e verrà ulteriormente verificato in
                                                            // finalize_remote_block_commit.
                                                            let block_hash = cert.block_hash;
                                                            // try_lock + silent (None,false,false,false) on Err was
                                                            // dropping certs under load when the lock was held by a
                                                            // concurrent commit task. Per-group cert notification at
                                                            // line 2924 was ALREADY spawned, so other LN's
                                                            // last_certified_height advanced — but commit_block_batch
                                                            // for OUR own pending block (gated on entry.is_some())
                                                            // never ran, our `last_certified_height` stayed pinned,
                                                            // pipeline_ahead saturated MAX_PIPELINE_DEPTH=16, the
                                                            // proposer loop hit `continue` and froze. After
                                                            // CERT_PENDING_MAX_WAIT=30s rotation flipped the SM to
                                                            // post-loadtest.
                                                            //
                                                            // Switching to `.lock().await` is safe: this branch is
                                                            // already inside an async task spawned for cert handling
                                                            // and the lock is uncontended in the steady state. Under
                                                            // load, awaiting briefly is strictly better than
                                                            // becomes always-zero and is removed (was misleading —
                                                            // it counted skipped certs, not contention).
                                                            let mut cert_pending = certificate_pending_handle.lock().await;
                                                            let (entry, was_committed, had_pending) = if cert_pending.is_committed(&block_hash) {
                                                                debug!(
                                                                    height = cert.height,
                                                                    hash = %hex::encode(block_hash),
                                                                    "Certificate received but block already committed, skipping"
                                                                );
                                                                (None, true, false)
                                                            } else {
                                                                //   1. lookup per hash (path veloce);
                                                                //   2. fallback PRECISE per (height, group_id, tx_root)
                                                                //      così cert per block FILLED non match accident-
                                                                //      almente block EMPTY registrato alla stessa
                                                                //      (height, group_id) da un proposer in race;
                                                                //   3. ultimo fallback per (height, group_id) per
                                                                //      (es. block davvero vuoto, tx_root canonical).
                                                                let cert_tx_root_32: [u8; 32] = {
                                                                    let mut t = [0u8; 32];
                                                                    t.copy_from_slice(&cert.tx_root[..32]);
                                                                    t
                                                                };
                                                                let taken = cert_pending
                                                                    .take_pending(&block_hash)
                                                                    .or_else(|| cert_pending
                                                                        .take_pending_by_height_group_tx_root(
                                                                            cert.height,
                                                                            &cert.group_id,
                                                                            &cert_tx_root_32,
                                                                        ))
                                                                    .or_else(|| cert_pending
                                                                        .take_pending_by_height_group(
                                                                            cert.height,
                                                                            &cert.group_id,
                                                                        ));
                                                                let had = taken.is_some();
                                                                (taken, false, had)
                                                            };
                                                            // Drop the guard before spawning further async work to
                                                            // avoid holding it across awaits in the entry.is_some()
                                                            // path below (commit task is spawned, not awaited inline).
                                                            drop(cert_pending);
                                                            let lock_taken = true;
                                                            {
                                                                // Tier 8: Prometheus counters per outcome.
                                                                if !lock_taken {
                                                                    crate::observability::ConsensusObsMetrics::inc_cert_lock_busy();
                                                                } else if entry.is_some() {
                                                                    crate::observability::ConsensusObsMetrics::inc_cert_match();
                                                                } else if !was_committed {
                                                                    crate::observability::ConsensusObsMetrics::inc_cert_miss();
                                                                }
                                                                static MISS_CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                static MATCH_CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                static LOCK_BUSY_CTR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                if !lock_taken {
                                                                    let n = LOCK_BUSY_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                    if n == 1 || n % 200 == 0 {
                                                                    }
                                                                } else if entry.is_some() {
                                                                    let n = MATCH_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                    if n == 1 || n % 50 == 0 {
                                                                    }
                                                                    // just won the round. Restore TXs for any of *our*
                                                                    // own proposed blocks at the same height with a
                                                                    // different hash — they will never be certified.
                                                                    // Without this, drained TXs sit orphan in
                                                                    // in_flight_by_block until the 300s tracker
                                                                    // timeout, the loadtest/client keeps incrementing
                                                                    // nonces optimistically, and the pool fills with
                                                                    let pipeline_restore = Arc::clone(&pipeline);
                                                                    let cert_height = cert.height;
                                                                    let cert_block_hash = block_hash;
                                                                    tokio::spawn(async move {
                                                                        let restored = pipeline_restore.restore_orphaned_at_height(cert_height, &cert_block_hash);
                                                                        if restored > 0 {
                                                                            warn!(
                                                                                committed_height = cert_height,
                                                                                restored_txs = restored,
                                                                                "an earlier fix Path C: restored orphaned TXs after rival block won height"
                                                                            );
                                                                        }
                                                                    });
                                                                } else if !was_committed {
                                                                    let n = MISS_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                    if n == 1 || n % 500 == 0 {
                                                                    }
                                                                    // a block at cert.height proposed by another node /
                                                                    // group. Any of *our* proposed blocks at the same
                                                                    // height will never be certified — restore their
                                                                    // TXs so they aren't orphaned for 300s. This is the
                                                                    // path that actually fires under multi-group fork
                                                                    // observed on testnet ln-4 (heights non-monotone in
                                                                    // commits while our blocks rot in in_flight).
                                                                    let pipeline_restore = Arc::clone(&pipeline);
                                                                    let cert_height = cert.height;
                                                                    let cert_block_hash = block_hash;
                                                                    tokio::spawn(async move {
                                                                        let restored = pipeline_restore.restore_orphaned_at_height(cert_height, &cert_block_hash);
                                                                        if restored > 0 {
                                                                            warn!(
                                                                                committed_height = cert_height,
                                                                                restored_txs = restored,
                                                                                "an earlier fix Path C: restored orphaned TXs after foreign block committed at height"
                                                                            );
                                                                        }
                                                                    });
                                                                }
                                                                let _ = had_pending; // silence unused in happy paths
                                                            }
                                                            if let Some(entry) = entry {
                                                                let pending_data = entry.pending_data.clone();
                                                                let storage_f = Arc::clone(&storage_clone);
                                                                let integrity_f = integrity_events.clone();
                                                                let pou_state_f = pou_state_handle.clone();
                                                                let pipeline_f = Arc::clone(&pipeline);
                                                                let known_peer_accounts_f = known_peer_accounts.clone();
                                                                let pt_clone = priority_targets.clone();
                                                                let cert_pending_f = Arc::clone(&certificate_pending_handle);
                                                                let block_hash_for_commit = block_hash;
                                                                let dag_f2 = Arc::clone(&dag_manager);
                                                                let commit_scheduler_f = Arc::clone(&commit_scheduler);
                                                                // Cert's group_id for composite-key storage lane routing.
                                                                let commit_group_id_f2 = cert.group_id.clone();
                                                                // Capture group_id before spawn to avoid race with group transition.
                                                                let dag_group_id_f2 = group_manager.get_current_group_cached()
                                                                    .map(|g| g.group_id.clone())
                                                                    .unwrap_or_else(|| "group_0_0_0".to_string());
                                                                tokio::spawn(async move {
                                                                    // scheduled (would not log if tokio executor is starved).
                                                                    {
                                                                        // Tier 8: Prometheus counter.
                                                                        crate::observability::ConsensusObsMetrics::inc_cert_spawn();
                                                                        static SPAWN_ENTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                        let n = SPAWN_ENTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                        if n == 1 || n % 50 == 0 {
                                                                        }
                                                                    }
                                                                    let masternode_addr = pt_clone.read().await.keys().next().map(|p| p.to_string()).unwrap_or_default();
                                                                    // ── Step 1: Speculative execution to get overlay + conflict keys ──
                                                                    let (overlay, _receipts) = match crate::p2p::block::execute_block_transactions(
                                                                        storage_f.as_ref(),
                                                                        &pending_data.block,
                                                                        &pending_data.signed_txs,
                                                                    ) {
                                                                        Ok(result) => result,
                                                                        Err(e) => {
                                                                            warn!(
                                                                                height = pending_data.block.height,
                                                                                error = ?e,
                                                                                "DIAG[D]: Speculative execution FAILED; block skipped (CommitScheduler is sole commit path)"
                                                                            );
                                                                            return;
                                                                        }
                                                                    };
                                                                    {
                                                                        // Tier 8: Prometheus counter (success path only — failure
                                                                        // path returns early above before reaching here).
                                                                        crate::observability::ConsensusObsMetrics::inc_speculative_exec(true);
                                                                        static EXEC_OK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                        let n = EXEC_OK.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                        if n == 1 || n % 50 == 0 {
                                                                        }
                                                                    }

                                                                    let ck = conflict_keys::extract_conflict_keys(&pending_data.signed_txs);
                                                                    let (read_set, write_set) = conflict_keys::extract_read_write_sets(&overlay, &pending_data.signed_txs);

                                                                    // ── Step 2: Build ExecutedBlockMeta ──
                                                                    let meta = conflict_keys::ExecutedBlockMeta {
                                                                        block_hash: pending_data.block.hash,
                                                                        height: pending_data.block.height,
                                                                        group_id: dag_group_id_f2.clone(),
                                                                        status: conflict_keys::BlockStatus::Pending,
                                                                        read_set,
                                                                        write_set,
                                                                        conflict_keys: ck,
                                                                        state_diff: overlay,
                                                                        topo_rank: pending_data.block.height,
                                                                        pou_score: 0,
                                                                        timestamp: pending_data.block.timestamp,
                                                                        pending_data: pending_data.clone(),
                                                                    };

                                                                    // ── Step 3: Admit to CommitScheduler ──
                                                                    {
                                                                        let mut scheduler = commit_scheduler_f.lock().await;
                                                                        scheduler.admit_block(meta);
                                                                    }
                                                                    {
                                                                        // Tier 8: Prometheus counter.
                                                                        crate::observability::ConsensusObsMetrics::inc_commit_admit();
                                                                        static ADMIT_OK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                        let n = ADMIT_OK.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                        if n == 1 || n % 50 == 0 {
                                                                        }
                                                                    }

                                                                    // ── Step 4: Drain ready blocks and commit them ──
                                                                    let mut drain_iterations = 0usize;
                                                                    let mut ready_total = 0usize;
                                                                    loop {
                                                                        let ready_blocks = {
                                                                            let mut scheduler = commit_scheduler_f.lock().await;
                                                                            scheduler.drain_ready()
                                                                        };

                                                                        if ready_blocks.is_empty() {
                                                                            // DIAG: only emit once per spawn when we get to
                                                                            // here with nothing ready — quantifies how often
                                                                            // admit yields zero drain.
                                                                            if drain_iterations == 0 {
                                                                                // Tier 8: Prometheus counter on first empty drain per spawn.
                                                                                crate::observability::ConsensusObsMetrics::inc_commit_drain_empty();
                                                                                static DRAIN_EMPTY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                                let n = DRAIN_EMPTY.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                                if n == 1 || n % 100 == 0 {
                                                                                }
                                                                            }
                                                                            break;
                                                                        }
                                                                        drain_iterations += 1;
                                                                        ready_total = ready_total.saturating_add(ready_blocks.len());
                                                                        {
                                                                            // Tier 8: Prometheus counter + histogram.
                                                                            crate::observability::ConsensusObsMetrics::inc_commit_drain_hit(ready_blocks.len());
                                                                            static DRAIN_HIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                                                            let n = DRAIN_HIT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                                                                            if n == 1 || n % 50 == 0 {
                                                                            }
                                                                        }

                                                                        for (_hash, block_meta) in ready_blocks {
                                                                            let peer_accounts_map: HashMap<PeerId, PeerInfo> = known_peer_accounts_f
                                                                                .iter()
                                                                                .map(|(pid, account)| (*pid, PeerInfo {
                                                                                    account: *account,
                                                                                    peer_id: pid.to_string(),
                                                                                    address: String::new(),
                                                                                    priority: false,
                                                                                }))
                                                                                .collect();
                                                                            // PERF: Do NOT hold pipeline lock during RocksDB commit.
                                                                            // Pass None — avoids blocking the proposer's mempool drain.
                                                                            let commit_group_arg = if commit_group_id_f2.is_empty() {
                                                                                None
                                                                            } else {
                                                                                Some(commit_group_id_f2.as_str())
                                                                            };
                                                                            let commit_result = finalize_remote_block_commit(
                                                                                storage_f.as_ref(),
                                                                                &block_meta.pending_data,
                                                                                &integrity_f,
                                                                                None,
                                                                                &pou_state_f,
                                                                                &peer_accounts_map,
                                                                                &known_peer_accounts_f,
                                                                                &masternode_addr,
                                                                                0,
                                                                                0,
                                                                                commit_group_arg,
                                                                            ).await;

                                                                            if commit_result {
                                                                                // Update mempool nonces after remote block commit.
                                                                                // Always invoke on_block_committed_with_nonces —
                                                                                // even for empty blocks — so the proposer's
                                                                                // stale pending_nonces get cleared. See the
                                                                                // twin callsite above (block_final path) for
                                                                                // the full rationale.
                                                                                let mut nonce_updates = std::collections::HashMap::new();
                                                                                if !block_meta.pending_data.signed_txs.is_empty() {
                                                                                    let mut sender_max_nonce: std::collections::HashMap<Vec<u8>, u64> = std::collections::HashMap::new();
                                                                                    let mut sorted_txs_for_nonce = block_meta.pending_data.signed_txs.clone();
                                                                                    sorted_txs_for_nonce.sort_by(|a, b| {
                                                                                        crate::p2p::block::normalize_address_bytes(&a.from)
                                                                                            .cmp(&crate::p2p::block::normalize_address_bytes(&b.from))
                                                                                            .then(a.nonce.cmp(&b.nonce))
                                                                                    });
                                                                                    for tx in &sorted_txs_for_nonce {
                                                                                        let addr = crate::p2p::block::normalize_address_bytes(&tx.from);
                                                                                        let entry = sender_max_nonce.entry(addr.clone()).or_insert_with(|| {
                                                                                            crate::storage::BlockAndAccountStorage::get_account(storage_f.as_ref(), &addr)
                                                                                                .ok()
                                                                                                .flatten()
                                                                                                .map(|acc| acc.nonce)
                                                                                                .unwrap_or(0)
                                                                                        });
                                                                                        if tx.nonce == *entry {
                                                                                            *entry += 1;
                                                                                        }
                                                                                    }
                                                                                    // and include receivers in the nonce map. promote_queued() inside
                                                                                    // on_block_committed_with_nonces_and_hash relies on the receiver
                                                                                    // entry (with current account nonce) to release queued TX whose
                                                                                    // sender was just funded by this block (e.g. faucet → sender_X
                                                                                    // waiting to submit nonce=0..N). Without this, admitted_total
                                                                                    // stays 0 because queued TX never get promoted to the main pool.
                                                                                    for tx in &block_meta.pending_data.signed_txs {
                                                                                        let receiver_addr = crate::p2p::block::normalize_address_bytes(&tx.to);
                                                                                        sender_max_nonce.entry(receiver_addr.clone()).or_insert_with(|| {
                                                                                            crate::storage::BlockAndAccountStorage::get_account(
                                                                                                storage_f.as_ref(),
                                                                                                &receiver_addr,
                                                                                            )
                                                                                                .ok()
                                                                                                .flatten()
                                                                                                .map(|acc| acc.nonce)
                                                                                                .unwrap_or(0)
                                                                                        });
                                                                                    }
                                                                                    for (addr, new_nonce) in &sender_max_nonce {
                                                                                        let sid = pipeline_f.get_sender_id_for_address(addr);
                                                                                        nonce_updates.insert(sid, *new_nonce);
                                                                                    }
                                                                                }
                                                                                let committed_hash = block_meta.block_hash;
                                                                                // from find_handles_from_signed_txs so the mempool
                                                                                // dedup-on-commit (legacy step 1) actually runs.
                                                                                let committed_handles = pipeline_f
                                                                                    .find_handles_from_signed_txs(&block_meta.pending_data.signed_txs);
                                                                                pipeline_f.on_block_committed_with_nonces_and_hash(committed_handles, &nonce_updates, Some(committed_hash));
                                                                                if !block_meta.pending_data.signed_txs.is_empty() {
                                                                                    savitri_mempool::mempool::metrics::increment_confirmed_batch(
                                                                                        block_meta.pending_data.signed_txs.len() as u64
                                                                                    );
                                                                                }
                                                                                tracing::info!(
                                                                                    height = block_meta.pending_data.block.height,
                                                                                    accounts_updated = nonce_updates.len(),
                                                                                    total_txs = block_meta.pending_data.signed_txs.len(),
                                                                                    "Updated mempool nonces from remote certified block (commit scheduler path)"
                                                                                );
                                                                                let mut cert_pending = cert_pending_f.lock().await;
                                                                                cert_pending.mark_committed(committed_hash);

                                                                                // Register committed block in DAG using pre-captured group_id
                                                                                let group_id = dag_group_id_f2.clone();
                                                                                register_block_in_dag(
                                                                                    &dag_f2,
                                                                                    &block_meta.pending_data.block,
                                                                                    &block_meta.pending_data.signed_txs,
                                                                                    &group_id,
                                                                                    vec![block_meta.pending_data.block.parent_hash],
                                                                                    0,
                                                                                ).await;

                                                                                // Wake deferred blocks and GC old entries
                                                                                {
                                                                                    let mut scheduler = commit_scheduler_f.lock().await;
                                                                                    scheduler.wake_deferred();
                                                                                    let gc_height = block_meta.height.saturating_sub(100);
                                                                                    scheduler.gc(gc_height);
                                                                                }

                                                                                info!(
                                                                                    height = block_meta.pending_data.block.height,
                                                                                    hash = %hex::encode(committed_hash),
                                                                                    "CommitScheduler: block committed via conflict-aware pipeline"
                                                                                );
                                                                            } else {
                                                                                warn!(
                                                                                    height = block_meta.pending_data.block.height,
                                                                                    hash = %hex::encode(block_meta.block_hash),
                                                                                    "Commit failed, re-registering block in pending for retry"
                                                                                );
                                                                                let source_peer = block_meta.pending_data.source_peer.clone();
                                                                                let mut cert_pending = cert_pending_f.lock().await;
                                                                                cert_pending.register_pending(
                                                                                    block_meta.block_hash,
                                                                                    block_meta.pending_data.block.height,
                                                                                    block_meta.pending_data.clone(),
                                                                                    source_peer,
                                                                                );
                                                                            }
                                                                        }
                                                                    }
                                                                });
                                                                info!(
                                                                    height = cert.height,
                                                                    hash = %hex::encode(block_hash),
                                                                    "📥 [LN←MN] Certificate matched pending block, finalize spawned"
                                                                );
                                                            } else {
                                                                debug!(
                                                                    height = cert.height,
                                                                    hash = %hex::encode(block_hash),
                                                                    "📥 [LN←MN] Certificate received but no pending block (legacy path; prefer block_final topic)"
                                                                );
                                                            }
                                                        }
                                                    }
                                                    None => {
                                                        if let Ok(other) = decode_consensus(&message.data) {
                                                            info!(
                                                                variant = ?other,
                                                                "📥 [LN←MN] Consensus cert topic: message decoded but not a Certificate variant, ignoring"
                                                            );
                                                        } else {
                                                            info!(
                                                                data_len = message.data.len(),
                                                                "📥 [LN←MN] Consensus cert topic: decode failed (tried JSON masternode format and bincode)"
                                                            );
                                                        }
                                                    }
                                                }
                                            }

                                            // ── Dynamic MN Discovery: PeerRegistryAnnounce ──
                                            // When a MN publishes its registry announce on /savitri/peer_registry/1,
                                            // detect new masternodes and add them as explicit_peer + dial.
                                            if message.topic == peer_registry_topic_hash {
                                                match serde_json::from_slice::<PeerRegistryAnnounce>(&message.data) {
                                                    Ok(announce) if announce.role == "masternode" => {
                                                        if let Ok(new_mn_peer_id) = PeerId::from_str(&announce.peer_id) {
                                                            if new_mn_peer_id != local_peer {
                                                                let is_new = match priority_targets.try_read() {
                                                                    Ok(pt) => !pt.contains_key(&new_mn_peer_id),
                                                                    Err(_) => false,
                                                                };
                                                                if is_new {
                                                                    // Parse multiaddr and add to priority_targets
                                                                    if let Ok(addr) = announce.multiaddr.parse::<Multiaddr>() {
                                                                        if !should_accept_discovered_masternode_addr(
                                                                            &addr,
                                                                            prefer_public_masternode_addrs,
                                                                        ) {
                                                                            warn!(
                                                                                peer = %new_mn_peer_id,
                                                                                multiaddr = %announce.multiaddr,
                                                                                "Ignoring discovered masternode with localhost/private address; configured public masternode peers are preferred"
                                                                            );
                                                                            continue;
                                                                        }
                                                                        if let Ok(mut pt) = priority_targets.try_write() {
                                                                            pt.insert(new_mn_peer_id, addr.clone());
                                                                        }
                                                                        // No add_explicit_peer: MN stays as normal mesh peer
                                                                        // so gossipsub GRAFT and subscription exchange works.
                                                                        // Dial if not already connected
                                                                        if !swarm.is_connected(&new_mn_peer_id) {
                                                                            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(new_mn_peer_id)
                                                                                .addresses(vec![addr])
                                                                                .build();
                                                                            if let Err(e) = swarm.dial(dial_opts) {
                                                                                warn!(peer = %new_mn_peer_id, error = %e, "Failed to dial newly discovered MN");
                                                                            }
                                                                        }
                                                                        info!(
                                                                            peer = %new_mn_peer_id,
                                                                            multiaddr = %announce.multiaddr,
                                                                            "🔍 [MN-DISCOVERY] New masternode discovered via peer_registry gossipsub (normal mesh peer)"
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Ok(_) => {
                                                        // Non-masternode announce (e.g. lightnode) - ignore for MN discovery
                                                    }
                                                    Err(e) => {
                                                        debug!("Failed to decode PeerRegistryAnnounce: {}", e);
                                                    }
                                                }
                                            }

                                            // ── Dynamic MN Discovery: PeerDiscoveryResponse ──
                                            // When a MN responds to our PeerDiscoveryRequest with a list of known MN multiaddrs,
                                            // discover and connect to any new masternodes.
                                            if message.topic == peer_discovery_topic_hash {
                                                if let Ok(response) = serde_json::from_slice::<PeerDiscoveryResponse>(&message.data) {
                                                    for mn_addr_str in &response.masternode_peers {
                                                        if let Ok(addr) = mn_addr_str.parse::<Multiaddr>() {
                                                            // Extract PeerId from multiaddr (last /p2p/<peer_id> component)
                                                            let maybe_peer_id = addr.iter().find_map(|proto| {
                                                                if let libp2p::multiaddr::Protocol::P2p(peer_id) = proto {
                                                                    Some(peer_id)
                                                                } else {
                                                                    None
                                                                }
                                                            });
                                                            if let Some(new_mn_peer_id) = maybe_peer_id {
                                                                if new_mn_peer_id != local_peer {
                                                                    let is_new = match priority_targets.try_read() {
                                                                        Ok(pt) => !pt.contains_key(&new_mn_peer_id),
                                                                        Err(_) => false,
                                                                    };
                                                                    if is_new {
                                                                        if !should_accept_discovered_masternode_addr(
                                                                            &addr,
                                                                            prefer_public_masternode_addrs,
                                                                        ) {
                                                                            warn!(
                                                                                peer = %new_mn_peer_id,
                                                                                source_addr = %mn_addr_str,
                                                                                "Ignoring discovered masternode with localhost/private address; configured public masternode peers are preferred"
                                                                            );
                                                                            continue;
                                                                        }
                                                                        if let Ok(mut pt) = priority_targets.try_write() {
                                                                            pt.insert(new_mn_peer_id, addr.clone());
                                                                        }
                                                                        // No add_explicit_peer: MN stays as normal mesh peer
                                                                        // so gossipsub GRAFT and subscription exchange works.
                                                                        if !swarm.is_connected(&new_mn_peer_id) {
                                                                            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(new_mn_peer_id)
                                                                                .addresses(vec![addr])
                                                                                .build();
                                                                            if let Err(e) = swarm.dial(dial_opts) {
                                                                                warn!(peer = %new_mn_peer_id, error = %e, "Failed to dial MN from discovery response");
                                                                            }
                                                                        }
                                                                        info!(
                                                                            peer = %new_mn_peer_id,
                                                                            source_addr = %mn_addr_str,
                                                                            "🔍 [MN-DISCOVERY] New masternode discovered via PeerDiscoveryResponse (normal mesh peer)"
                                                                        );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        GossipsubEvent::SlowPeer { peer_id, .. } => {
                                            // SlowPeer. The previous heuristic (keep group_member +
                                            // priority_targets, disconnect everyone else) caused a
                                            // hard outage:
                                            //   * `priority_targets` is populated ONCE at boot from
                                            //     the static peers config (line 613). MN discovered
                                            //     dynamically via PeerDiscoveryResponse (line 3479)
                                            //     are NEVER added to it.
                                            //   * `p2p_group_peers` is populated ONCE at boot too
                                            //     (line 621). When the cluster rotates epoch and
                                            //     this LN moves to a new group, the new group peers
                                            //     are NOT in the set.
                                            //   * Any MN that was discovered runtime AND is the
                                            //     announcer for the new epoch gets classified
                                            //     "non-group non-priority" → disconnected → this
                                            //     LN never receives its GroupAnnouncement →
                                            //     shard_to_group goes stale → TX gossip ends in
                                            //     dead topics → empty blocks (memoria
                                            //     session_2026-04-30_phase3 root cause).
                                            //
                                            // Gossipsub already handles slow peers via the built-in
                                            // peer scoring + mesh graft/prune. Manual disconnect on
                                            // SlowPeer events is redundant and harmful: it breaks
                                            // the announcement / registration / consensus flow that
                                            // depends on a stable connection. We just log the event
                                            // for visibility; the protocol layer will demote the
                                            // peer if it stays slow.
                                            let is_group_member = p2p_group_peers.contains(&peer_id);
                                            let is_priority = match priority_targets.try_read() {
                                                Ok(pt) => pt.contains_key(&peer_id),
                                                Err(_) => false,
                                            };
                                            debug!(
                                                %peer_id,
                                                is_group_member,
                                                is_priority,
                                                "Gossipsub SlowPeer detected — keeping connection (peer scoring handles demotion)"
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                                // ── Kademlia event handler (fallback for MN discovery via DHT) ──
                                crate::p2p::types::MyBehaviourEvent::Kademlia(event) => {
                                    match event {
                                        libp2p::kad::Event::OutboundQueryProgressed { result, .. } => {
                                            if let libp2p::kad::QueryResult::GetRecord(Ok(ok)) = result {
                                                if let libp2p::kad::GetRecordOk::FoundRecord(found) = ok {
                                                    let key = String::from_utf8_lossy(found.record.key.as_ref()).to_string();
                                                    if key.starts_with("peer_registry:") {
                                                        // Deserialize PeerRegistryAnnounce from Kademlia record
                                                        match serde_json::from_slice::<PeerRegistryAnnounce>(&found.record.value) {
                                                            Ok(announce) if announce.role == "masternode" => {
                                                                if let Ok(new_mn_peer_id) = PeerId::from_str(&announce.peer_id) {
                                                                    if new_mn_peer_id != local_peer {
                                                                        let is_new = match priority_targets.try_read() {
                                                                            Ok(pt) => !pt.contains_key(&new_mn_peer_id),
                                                                            Err(_) => false,
                                                                        };
                                                                        if is_new {
                                                                            if let Ok(addr) = announce.multiaddr.parse::<Multiaddr>() {
                                                                                if !should_accept_discovered_masternode_addr(
                                                                                    &addr,
                                                                                    prefer_public_masternode_addrs,
                                                                                ) {
                                                                                    warn!(
                                                                                        peer = %new_mn_peer_id,
                                                                                        key = %key,
                                                                                        multiaddr = %announce.multiaddr,
                                                                                        "Ignoring discovered masternode with localhost/private address; configured public masternode peers are preferred"
                                                                                    );
                                                                                    continue;
                                                                                }
                                                                                if let Ok(mut pt) = priority_targets.try_write() {
                                                                                    pt.insert(new_mn_peer_id, addr.clone());
                                                                                }
                                                                                // No add_explicit_peer: MN stays as normal mesh peer
                                                                                // so gossipsub GRAFT and subscription exchange works.
                                                                                if !swarm.is_connected(&new_mn_peer_id) {
                                                                                    let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(new_mn_peer_id)
                                                                                        .addresses(vec![addr])
                                                                                        .build();
                                                                                    let _ = swarm.dial(dial_opts);
                                                                                }
                                                                                info!(
                                                                                    peer = %new_mn_peer_id,
                                                                                    key = %key,
                                                                                    "🔍 [MN-DISCOVERY] New masternode discovered via Kademlia DHT record (normal mesh peer)"
                                                                                );
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                // Identify: use observed address so others can dial us (decentralized/NAT)
                                crate::p2p::types::MyBehaviourEvent::Identify(event) => {
                                    if let libp2p::identify::Event::Received { peer_id, info, .. } = event {
                                        // addresses so subsequent peer_id-only dials can resolve without
                                        // needing a GroupAnnounce to carry the multiaddr. Fixes 50+ "No
                                        // address for group peer — attempting peer_id-only dial" warnings
                                        // per hour observed on LN-1 when some group members' addrs are
                                        // missing from announces.
                                        for addr in &info.listen_addrs {
                                            // Skip loopback, private, and unroutable addresses.
                                            let s = addr.to_string();
                                            if s.contains("/ip4/127.") || s.contains("/ip4/0.0.0.0")
                                                || s.contains("/ip4/169.254.") || s.contains("tcp/0")
                                                || s.contains("udp/0") || is_local_or_private_multiaddr(addr) {
                                                continue;
                                            }
                                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                                            trace!(peer=%peer_id, addr=%addr, "Identify: added peer address to Kademlia");
                                        }

                                        let obs = normalize_registration_addr(
                                            &info.observed_addr.to_string(),
                                            registration_listen_port,
                                            external_ip.as_deref(),
                                        );
                                        if !obs.is_empty() && !obs.contains("tcp/0") {
                                            if let Ok(mut cur) = shared_observed_addr.try_write() {
                                                if *cur != obs {
                                                    *cur = obs.clone();
                                                    let is_non_local = !obs.contains("127.0.0.1");
                                                    drop(cur);
                                                    debug!(
                                                        observed_addr = %obs,
                                                        "Identify: using observed address for peer/registration (decentralized)"
                                                    );
                                                    // Re-register so masternode gets VM/real IP for group formation (not 127.0.0.1)
                                                    if is_non_local {
                                                        pending_registration = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                // ── Consensus direct P2P (request-response) ──
                                crate::p2p::types::MyBehaviourEvent::Consensus(event) => {
                                    match event {
                                        libp2p::request_response::Event::Message { peer, message, .. } => {
                                            match message {
                                                libp2p::request_response::Message::Request { request, channel, .. } => {
                                                    debug!(
                                                        peer = %peer,
                                                        "Received consensus direct message from peer"
                                                    );
                                                    // Route to intra-group handler
                                                    {
                                                        let igc_consensus = intra_group_comm.clone();
                                                        let peer_for_log = peer;
                                                        tokio::spawn(async move {
                                                            let mut ig = igc_consensus.write().await;
                                                            if let Err(e) = ig.process_consensus_direct_message(request).await {
                                                                warn!(error = ?e, peer = %peer_for_log, "Failed to process consensus direct message");
                                                            }
                                                        });
                                                    }
                                                    // Send ACK response
                                                    let ack = crate::p2p::consensus_protocol::ConsensusAck { ok: true };
                                                    let _ = swarm.behaviour_mut().consensus.send_response(channel, ack);
                                                }
                                                libp2p::request_response::Message::Response { response, .. } => {
                                                    if !response.ok {
                                                        warn!(peer = %peer, "Consensus peer responded with nack");
                                                    }
                                                }
                                            }
                                        }
                                        libp2p::request_response::Event::OutboundFailure { peer, error, .. } => {
                                            debug!(
                                                peer = %peer,
                                                error = ?error,
                                                "Consensus direct send failed (peer may be offline)"
                                            );
                                        }
                                        libp2p::request_response::Event::InboundFailure { peer, error, .. } => {
                                            debug!(
                                                peer = %peer,
                                                error = ?error,
                                                "Consensus direct receive failed"
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                                // ── Auxiliary direct P2P (heartbeat, PoU, peer_discovery, peer_registry) ──
                                crate::p2p::types::MyBehaviourEvent::Aux(event) => {
                                    match event {
                                        libp2p::request_response::Event::Message { peer, message, .. } => {
                                            match message {
                                                libp2p::request_response::Message::Request { request, channel, .. } => {
                                                    let mut ack = crate::p2p::aux_protocol::AuxAck { ok: true, payload: None };
                                                    match request {
                                                        crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryResponse(data) => {
                                                            debug!(peer = %peer, "Received peer discovery response via direct TCP");
                                                            // Process peer discovery response data
                                                            if let Ok(response) = serde_json::from_slice::<serde_json::Value>(&data) {
                                                                debug!(peer = %peer, response = ?response, "Peer discovery response received");
                                                            }
                                                        }
                                                        crate::p2p::aux_protocol::AuxMessage::BlockSync(payload) => {
                                                            match bincode::deserialize::<BlockSyncRequest>(&payload) {
                                                                Ok(req) => {
                                                                    let local_tip = storage_clone
                                                                        .get_chain_head()
                                                                        .ok()
                                                                        .flatten()
                                                                        .map(|b| b.height)
                                                                        .unwrap_or(0);
                                                                    let mut blocks = Vec::new();
                                                                    let mut reason: Option<String> = None;
                                                                    let end = req.end_height.unwrap_or(local_tip).min(local_tip);

                                                                    if req.start_height == u64::MAX {
                                                                        reason = Some("invalid_start_height_u64_max".to_string());
                                                                    } else if req.start_height > local_tip {
                                                                        reason = Some(format!(
                                                                            "start_height {} above responder tip {}",
                                                                            req.start_height, local_tip
                                                                        ));
                                                                    } else if end < req.start_height {
                                                                        reason = Some(format!(
                                                                            "empty_range start={} end={}",
                                                                            req.start_height, end
                                                                        ));
                                                                    } else {
                                                                        let mut next_height = req.start_height;
                                                                        while next_height <= end && blocks.len() < req.batch_size {
                                                                            match storage_clone.get_block(next_height) {
                                                                                Ok(Some(block)) => {
                                                                                    blocks.push(block);
                                                                                    next_height = next_height.saturating_add(1);
                                                                                }
                                                                                Ok(None) => {
                                                                                    reason = Some(format!("missing_block_at_height {}", next_height));
                                                                                    break;
                                                                                }
                                                                                Err(err) => {
                                                                                    reason = Some(format!("storage_error_at_height {}: {}", next_height, err));
                                                                                    break;
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                    let is_final = blocks.last().map(|b| b.height >= end).unwrap_or(true);
                                                                    let response = BlockSyncResponse {
                                                                        blocks,
                                                                        tip_height: local_tip,
                                                                        is_final,
                                                                        reason,
                                                                    };
                                                                    match bincode::serialize(&response) {
                                                                        Ok(bytes) => ack.payload = Some(bytes),
                                                                        Err(err) => {
                                                                            ack.ok = false;
                                                                            warn!(error = %err, peer = %peer, "Failed to serialize block sync response");
                                                                        }
                                                                    }
                                                                }
                                                                Err(err) => {
                                                                    ack.ok = false;
                                                                    warn!(error = %err, peer = %peer, "Failed to decode block sync request");
                                                                }
                                                            }
                                                        }
                                                        crate::p2p::aux_protocol::AuxMessage::TxForward(payload) => {
                                                            // directly to the elected proposer of the target group.
                                                            // Decode the wire bytes (same format as gossipsub TX
                                                            // topic) into a SignedTx and submit to the local
                                                            // mempool. ACK ok=true on accept; sender falls back to
                                                            // gossipsub on ok=false. Uses `pipeline` (the network
                                                            // task's mempool handle, equal to the proposer drain
                                                            // handle when one was wired by the binary).
                                                            match crate::tx::deserialize_signed_tx(&payload) {
                                                                Ok(signed_tx) => {
                                                                    match pipeline.add_transaction(signed_tx) {
                                                                        Ok(_) => {
                                                                            debug!(peer = %peer, payload_bytes = payload.len(), "TxForward: TX admitted via direct aux channel");
                                                                        }
                                                                        Err(err) => {
                                                                            ack.ok = false;
                                                                            debug!(peer = %peer, error = %err, "TxForward: mempool rejected TX (sender will fall back to gossip)");
                                                                        }
                                                                    }
                                                                }
                                                                Err(err) => {
                                                                    ack.ok = false;
                                                                    warn!(peer = %peer, error = %err, payload_bytes = payload.len(), "TxForward: failed to deserialize signed tx");
                                                                }
                                                            }
                                                        }
                                                        other => {
                                                            debug!(peer = %peer, msg = ?std::mem::discriminant(&other), "Received aux message via direct TCP");
                                                        }
                                                    }
                                                    let _ = swarm.behaviour_mut().aux.send_response(channel, ack);
                                                }
                                                libp2p::request_response::Message::Response { response, .. } => {
                                                    if !response.ok {
                                                        debug!(peer = %peer, "Aux peer responded with nack");
                                                    } else if let Some(req) = pending_block_sync_requests.remove(&peer) {
                                                        if let Some(payload) = response.payload {
                                                            match bincode::deserialize::<BlockSyncResponse>(&payload) {
                                                                Ok(sync_response) => {
                                                                    block_sync_manager.update_peer_height(peer.clone(), sync_response.tip_height);
                                                                    let mut highest_applied = 0u64;
                                                                    let mut expected_height = req.start_height;
                                                                    let mut ordered_blocks = sync_response.blocks.clone();
                                                                    ordered_blocks.sort_by_key(|b| b.height);
                                                                    for block in &ordered_blocks {
                                                                        if block.height == u64::MAX {
                                                                            warn!(peer = %peer, "Skipping sync block with invalid height u64::MAX");
                                                                            continue;
                                                                        }
                                                                        if block.height < req.start_height {
                                                                            continue;
                                                                        }
                                                                        if block.height < expected_height {
                                                                            continue;
                                                                        }
                                                                        if block.height > expected_height {
                                                                            warn!(
                                                                                peer = %peer,
                                                                                expected_height,
                                                                                got_height = block.height,
                                                                                "Block sync response has a height gap; stopping apply for this batch"
                                                                            );
                                                                            break;
                                                                        }
                                                                        expected_height = block.height.saturating_add(1);
                                                                        match storage_clone.get_block(block.height) {
                                                                            Ok(Some(existing)) if existing.hash == block.hash => {}
                                                                            Ok(Some(existing)) => {
                                                                                warn!(
                                                                                    peer = %peer,
                                                                                    height = block.height,
                                                                                    existing_hash = %hex::encode(&existing.hash[..16]),
                                                                                    remote_hash = %hex::encode(&block.hash[..16]),
                                                                                    "Replacing conflicting local block during sync"
                                                                                );
                                                                                if storage_clone.set_block(block.height, block.clone()).is_ok() {
                                                                                    highest_applied = highest_applied.max(block.height);
                                                                                    let dag_sync = Arc::clone(&dag_manager);
                                                                                    let block_sync = block.clone();
                                                                                    tokio::spawn(async move {
                                                                                        register_block_in_dag(
                                                                                            &dag_sync,
                                                                                            &block_sync,
                                                                                            &[],
                                                                                            "sync",
                                                                                            vec![block_sync.parent_hash],
                                                                                            0,
                                                                                        ).await;
                                                                                    });
                                                                                }
                                                                            }
                                                                            _ => {
                                                                                if storage_clone.set_block(block.height, block.clone()).is_ok() {
                                                                                    highest_applied = highest_applied.max(block.height);
                                                                                    let dag_sync = Arc::clone(&dag_manager);
                                                                                    let block_sync = block.clone();
                                                                                    tokio::spawn(async move {
                                                                                        register_block_in_dag(
                                                                                            &dag_sync,
                                                                                            &block_sync,
                                                                                            &[],
                                                                                            "sync",
                                                                                            vec![block_sync.parent_hash],
                                                                                            0,
                                                                                        ).await;
                                                                                    });
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                    if highest_applied > 0 {
                                                                        if let Ok(Some(new_head)) = storage_clone.get_block(highest_applied) {
                                                                            match storage_clone.get_chain_head() {
                                                                                Ok(Some(head)) => {
                                                                                    if highest_applied > head.height {
                                                                                        let _ = storage_clone.set_chain_head(&new_head);
                                                                                    }
                                                                                }
                                                                                Ok(None) => {
                                                                                    let _ = storage_clone.set_chain_head(&new_head);
                                                                                }
                                                                                Err(err) => {
                                                                                    warn!(error = %err, "Failed to read chain head while applying block sync");
                                                                                    let _ = storage_clone.set_chain_head(&new_head);
                                                                                }
                                                                            }
                                                                        }
                                                                        info!(
                                                                            peer = %peer,
                                                                            applied_upto = highest_applied,
                                                                            blocks_in_batch = ordered_blocks.len(),
                                                                            "Applied block sync batch"
                                                                        );
                                                                    }
                                                                    let sync_reason = sync_response.reason.clone();
                                                                    if let Some(next_req) = block_sync_manager.handle_sync_response(sync_response, req) {
                                                                        if let Ok(payload) = bincode::serialize(&next_req) {
                                                                            pending_block_sync_requests.insert(peer.clone(), next_req);
                                                                            let _ = swarm.behaviour_mut().aux.send_request(
                                                                                &peer,
                                                                                crate::p2p::aux_protocol::AuxMessage::BlockSync(payload),
                                                                            );
                                                                        }
                                                                    } else if highest_applied == 0 {
                                                                        debug!(peer = %peer, reason = ?sync_reason, "Block sync response applied no new blocks");
                                                                    }
                                                                }
                                                                Err(err) => {
                                                                    warn!(error = %err, peer = %peer, "Failed to decode block sync response");
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        libp2p::request_response::Event::OutboundFailure { peer, error, .. } => {
                                            pending_block_sync_requests.remove(&peer);
                                            debug!(peer = %peer, error = ?error, "Aux direct send failed");
                                        }
                                        libp2p::request_response::Event::InboundFailure { peer, error, .. } => {
                                            debug!(peer = %peer, error = ?error, "Aux direct receive failed");
                                        }
                                        _ => {}
                                    }
                                }
                                // ── Relay client events (NAT traversal) ──
                                crate::p2p::types::MyBehaviourEvent::RelayClient(event) => {
                                    match &event {
                                        libp2p::relay::client::Event::ReservationReqAccepted { relay_peer_id, renewal, .. } => {
                                            info!(
                                                relay = %relay_peer_id,
                                                renewal = renewal,
                                                "Relay: reservation accepted — inbound via relay enabled"
                                            );
                                        }
                                        libp2p::relay::client::Event::OutboundCircuitEstablished { relay_peer_id, .. } => {
                                            info!(relay = %relay_peer_id, "Relay: outbound circuit established");
                                        }
                                        libp2p::relay::client::Event::InboundCircuitEstablished { src_peer_id, .. } => {
                                            info!(src = %src_peer_id, "Relay: inbound circuit established");
                                        }
                                        _ => {
                                            debug!(?event, "Relay client event");
                                        }
                                    }
                                }
                                // ── DCUTR hole-punching events ──
                                crate::p2p::types::MyBehaviourEvent::Dcutr(event) => {
                                    match &event.result {
                                        Ok(conn_id) => {
                                            info!(
                                                peer = %event.remote_peer_id,
                                                ?conn_id,
                                                "DCUtR: hole-punch succeeded — direct connection established"
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                peer = %event.remote_peer_id,
                                                error = %e,
                                                "DCUtR: hole-punch failed — staying on relay"
                                            );
                                        }
                                    }
                                }
                                // ── TxFetch protocol events ──
                                crate::p2p::types::MyBehaviourEvent::TxFetch(event) => {
                                    use libp2p::request_response::Event as RrEvent;
                                    use libp2p::request_response::Message as RrMessage;
                                    match event {
                                        RrEvent::Message { peer, message, .. } => {
                                            match message {
                                                RrMessage::Request { request, channel, .. } => {
                                                    // Serve TX bytes from local store — sync RwLock, no .await
                                                    let batch = tx_store_for_spawn.get_batch(&request.hashes);
                                                    info!(peer = %peer, requested = request.hashes.len(), found = batch.len(), "TX fetch: serving request");
                                                    let resp = crate::p2p::tx_fetch_protocol::TxFetchResponse { txs: batch };
                                                    let _ = swarm.behaviour_mut().tx_fetch.send_response(channel, resp);
                                                }
                                                RrMessage::Response { response, .. } => {
                                                    if !response.txs.is_empty() {
                                                        let count = response.txs.len();
                                                        info!(peer = %peer, txs = count, "TX fetch: received, adding to mempool");
                                                        // Store TX bytes locally (sync) so we can serve them to others
                                                        let mut deserialized = Vec::with_capacity(count);
                                                        let mut fetched_hashes = Vec::with_capacity(count);
                                                        for (hash, tx_bytes) in response.txs {
                                                            tx_store_for_spawn.insert(hash, tx_bytes.clone());
                                                            fetched_hashes.push(hash);
                                                            deserialized.push(crate::tx::deserialize_signed_tx(&tx_bytes));
                                                        }
                                                        // Mark as fetched so we don't re-request
                                                        tx_store_for_spawn.mark_fetched(&fetched_hashes);
                                                        // Also record ourselves as a holder for these hashes — any
                                                        // TxFetchRequest that arrives will be served from our local
                                                        // store. No re-announce needed: gossipsub mesh relay carries
                                                        // the original HaveTx to every peer already, and re-publishing
                                                        // after each fetch caused an 18×-amplification storm that
                                                        // saturated the RPC ingress nodes (measured: 209 submit TPS
                                                        // and 950 HTTP errors vs 537 / 0 baseline).
                                                        tx_store_for_spawn.record_announcements(&fetched_hashes, local_peer);

                                                        // Insert into mempool in background
                                                        let pipeline_c = Arc::clone(&pipeline);
                                                        tokio::spawn(async move {
                                                            let added = pipeline_c.process_raw_transactions(deserialized).await;
                                                            tracing::info!(added, "TX fetch: added to mempool (spawned)");
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        RrEvent::OutboundFailure { peer, error, .. } => {
                                            debug!(peer = %peer, error = ?error, "TX fetch outbound failure");
                                        }
                                        RrEvent::InboundFailure { peer, error, .. } => {
                                            debug!(peer = %peer, error = ?error, "TX fetch inbound failure");
                                        }
                                        _ => {}
                                    }
                                }
                                // ── AutoNAT status detection → trigger relay reservation ──
                                crate::p2p::types::MyBehaviourEvent::Autonat(event) => {
                                    match &event {
                                        libp2p::autonat::Event::StatusChanged { old, new } => {
                                            info!(?old, ?new, "AutoNAT status changed");
                                            match new {
                                                libp2p::autonat::NatStatus::Private => {
                                                    warn!("NAT detected Private — requesting relay reservations from bootstrap peers");
                                                    // Request relay reservation from each bootstrap/masternode peer
                                                    for entry in bootstrap.iter().chain(masternode_peers.iter()) {
                                                        let trimmed = entry.trim();
                                                        if let Some((peer_part, addr_part)) = trimmed.split_once('#').map(|(b, _)| b).unwrap_or(trimmed).split_once('@') {
                                                            if let (Ok(relay_pid), Ok(relay_maddr)) = (
                                                                peer_part.trim().parse::<libp2p::PeerId>(),
                                                                addr_part.trim().parse::<libp2p::Multiaddr>(),
                                                            ) {
                                                                let circuit_addr = relay_maddr
                                                                    .with(libp2p::multiaddr::Protocol::P2p(relay_pid))
                                                                    .with(libp2p::multiaddr::Protocol::P2pCircuit);
                                                                match swarm.listen_on(circuit_addr.clone()) {
                                                                    Ok(_) => info!(relay = %relay_pid, addr = %circuit_addr, "Requesting relay reservation"),
                                                                    Err(e) => warn!(relay = %relay_pid, error = %e, "Failed to request relay reservation"),
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                libp2p::autonat::NatStatus::Public(addr) => {
                                                    info!(%addr, "Public IP confirmed — direct connections OK, no relay needed");
                                                    swarm.add_external_address(addr.clone());
                                                }
                                                libp2p::autonat::NatStatus::Unknown => {
                                                    debug!("NAT status unknown — waiting for probes");
                                                }
                                            }
                                        }
                                        _ => {
                                            debug!(?event, "AutoNAT event");
                                        }
                                    }
                                }
                                // ── UPnP: auto port-forward for residential routers ──
                                crate::p2p::types::MyBehaviourEvent::Upnp(event) => {
                                    match event {
                                        libp2p::upnp::Event::NewExternalAddr(addr) => {
                                            info!(%addr, "UPnP: external address auto-discovered via IGD");
                                            swarm.add_external_address(addr);
                                        }
                                        libp2p::upnp::Event::ExpiredExternalAddr(addr) => {
                                            warn!(%addr, "UPnP: external address expired");
                                        }
                                        libp2p::upnp::Event::GatewayNotFound => {
                                            debug!("UPnP: no IGD gateway (router doesn't support UPnP)");
                                        }
                                        libp2p::upnp::Event::NonRoutableGateway => {
                                            warn!("UPnP: gateway found but non-routable address");
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                _ = batch_timer.tick() => {
                    if !tx_batch_buffer.is_empty() {
                        let batch_to_process = tx_batch_buffer.clone();
                        let batch_size = batch_to_process.len();
                        tx_batch_buffer.clear();

                        let pipeline_clone = Arc::clone(&pipeline);

                        tokio::spawn(async move {
                            let raw_txs: Vec<_> = batch_to_process
                                .into_iter()
                                .map(|bytes| bytes_to_raw_tx(bytes, None))
                                .collect();

                            let batch_start = Instant::now();
                            let accepted_count = pipeline_clone.process_raw_transactions(raw_txs).await;
                            let processing_time_us = batch_start.elapsed().as_micros() as u64;

                            // gossip-RX TX actually made it into the local mempool.
                            // batch_size = decoded TX from the gossip topic, accepted =
                            // is the silent rejection bucket (nonce gap, balance, dedup).
                            crate::observability::PipelineObsMetrics::add_gossip_rx_forwarded(accepted_count as u64);
                            if accepted_count < batch_size {
                                let dropped = batch_size - accepted_count;
                                use std::sync::atomic::{AtomicU64, Ordering};
                                static GOSSIP_DROP_CTR: AtomicU64 = AtomicU64::new(0);
                                let n = GOSSIP_DROP_CTR.fetch_add(dropped as u64, Ordering::Relaxed) + dropped as u64;
                                if n == 1 || n % 500 == 0 {
                                    tracing::warn!(
                                        batch_size,
                                        accepted = accepted_count,
                                        dropped_now = dropped,
                                        dropped_total = n,
                                        "DIAG[gossip-rx-drop]: gossip TX rejected by mempool admission"
                                    );
                                }
                            }

                            if accepted_count > 0 {
                                debug!(
                                    batch_size = batch_size,
                                    accepted = accepted_count,
                                    processing_time_us = processing_time_us,
                                    "Processed transaction batch (timer)"
                                );
                            }
                        });
                    }
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH: Periodic block sync height check
                // Logs local DAG height to detect stalled block propagation.
                // ═══════════════════════════════════════════════════════════
                _ = block_sync_interval.tick() => {
                    let chain_head_height = storage_clone
                        .get_chain_head()
                        .ok()
                        .flatten()
                        .map(|b| b.height)
                        .unwrap_or(0);
                    let mut current_dag_height = chain_head_height;
                    if current_dag_height == u64::MAX {
                        let dag_height_snapshot = chain_head_height;
                        warn!(
                            dag_height = dag_height_snapshot,
                            chain_head_height,
                            "Invalid local height u64::MAX detected; clamping to chain head"
                        );
                        current_dag_height = chain_head_height;
                    }
                    let connected_peers = swarm.connected_peers().count();
                    // Partition detection: check if we have enough peers for BFT quorum
                    let expected_peers = p2p_group_peers.len().max(5); // at least 5 expected
                    partition_detector.update(connected_peers, expected_peers);
                    if partition_detector.should_pause_production() {
                        tracing::error!(
                            connected = connected_peers,
                            expected = expected_peers,
                            "PARTITION DETECTED: insufficient peers for BFT quorum, block production should pause"
                        );
                    }

                    let height_delta = current_dag_height.saturating_sub(last_sync_height);
                    let stalled = current_dag_height > 0 && height_delta == 0 && last_sync_height > 0;

                    if current_dag_height > 0 {
                        if stalled {
                            tracing::warn!(
                                local_height = current_dag_height,
                                connected_peers,
                                "Block sync stall detected: no new blocks in 30s"
                            );
                        } else {
                            tracing::info!(
                                local_height = current_dag_height,
                                new_blocks_since_last = height_delta,
                                connected_peers,
                                "Block sync check"
                            );
                        }
                    }

                    if stalled {
                        block_sync_stall_ticks = block_sync_stall_ticks.saturating_add(1);
                    } else {
                        block_sync_stall_ticks = 0;
                    }
                    last_sync_height = current_dag_height;

                    if let Some((peer_id, req)) = block_sync_manager.check_sync_needed(current_dag_height) {
                        if let Ok(payload) = bincode::serialize(&req) {
                            pending_block_sync_requests.insert(peer_id.clone(), req);
                            let _ = swarm.behaviour_mut().aux.send_request(
                                &peer_id,
                                crate::p2p::aux_protocol::AuxMessage::BlockSync(payload),
                            );
                            info!(peer = %peer_id, local_height = current_dag_height, "Requested pull-based block sync");
                        }
                    } else if stalled {
                        // No known higher peer yet: probe connected peers directly to discover remote tip heights.
                        let connected_candidates: Vec<PeerId> = swarm.connected_peers().cloned().collect();
                        if !connected_candidates.is_empty() {
                            let idx = block_sync_probe_cursor % connected_candidates.len();
                            block_sync_probe_cursor = block_sync_probe_cursor.wrapping_add(1);
                            let probe_peer = connected_candidates[idx].clone();
                            if !pending_block_sync_requests.contains_key(&probe_peer) {
                                if let Some((peer_id, req)) = block_sync_manager.make_probe_request(current_dag_height, probe_peer) {
                                    if let Ok(payload) = bincode::serialize(&req) {
                                        pending_block_sync_requests.insert(peer_id.clone(), req);
                                        let _ = swarm.behaviour_mut().aux.send_request(
                                            &peer_id,
                                            crate::p2p::aux_protocol::AuxMessage::BlockSync(payload),
                                        );
                                        info!(
                                            peer = %peer_id,
                                            local_height = current_dag_height,
                                            "Requested block sync probe from connected peer"
                                        );
                                    }
                                }
                            }
                        }

                        // Repeated stall fallback: request full bootstrap snapshot (accounts + blocks)
                        // before continuing, so node can recover state even when gossip propagation is patchy.
                        if block_sync_stall_ticks >= 2 && !connected_priority_peers.is_empty() {
                            request_bootstrap_snapshot(
                                &mut swarm,
                                &bootstrap_req_topic,
                                u64::MAX,
                                &mut last_bootstrap_request,
                                &mut allow_bootstrap_overwrite,
                                &mut pending_bootstrap_target,
                            );
                            info!(
                                stall_ticks = block_sync_stall_ticks,
                                local_height = current_dag_height,
                                "Requested bootstrap snapshot after repeated sync stall"
                            );
                            block_sync_stall_ticks = 0;
                        }
                    }
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH: Dead peer health check
                // Group peers are managed by gossipsub mesh and election logic.
                // Disconnecting group peers after 20s was fragmenting the mesh
                // and causing permanent stalls (non-proposer LNs don't generate
                // traffic every 20s, so they appeared "stale" but were healthy).
                // ═══════════════════════════════════════════════════════════
                _ = peer_health_timer.tick() => {
                    let now = Instant::now();
                    let stale_threshold = Duration::from_secs(20);
                    let stale_peers: Vec<PeerId> = peer_last_activity.iter()
                        .filter(|(_, last)| now.duration_since(**last) > stale_threshold)
                        .map(|(pid, _)| *pid)
                        .collect();
                    for stale_peer in stale_peers {
                        if p2p_group_peers.contains(&stale_peer) {
                            // healthy non-proposer LNs with low traffic. Only log.
                            debug!(
                                %stale_peer,
                                "Group peer inactive for 20s (not disconnecting — managed by gossipsub)"
                            );
                        } else {
                            debug!(
                                %stale_peer,
                                "Disconnecting stale non-group peer (no activity for 20s)"
                            );
                            let _ = swarm.disconnect_peer_id(stale_peer);
                            peer_last_activity.remove(&stale_peer);
                        }
                    }
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH: Election watchdog - re-trigger election if stalled
                // ═══════════════════════════════════════════════════════════
                _ = election_watchdog_timer.tick() => {
                    // PERF: Use max(storage_height, certified_height) to detect progress.
                    // Without this, the watchdog fires false stalls because RocksDB commit
                    // takes 5s, but the block IS certified (just not persisted yet).
                    let storage_height = storage_clone
                        .get_chain_head()
                        .ok()
                        .flatten()
                        .map(|b| b.height)
                        .unwrap_or(0);
                    let certified_height = match intra_group_comm.try_read() {
                        Ok(igc) => igc.last_certified_height.load(std::sync::atomic::Ordering::SeqCst),
                        Err(_) => 0,
                    };
                    let current_height = storage_height.max(certified_height);
                    if current_height > last_known_block_height {
                        last_known_block_height = current_height;
                        election_watchdog_stall_count = 0;
                    } else if !intra_group_mesh_established {
                        // just skipped, leaving LNs permanently stuck if they couldn't
                        // form mesh with group peers. Now we re-dial group members after 60s.
                        if let Some(join_time) = group_join_time {
                            let elapsed = join_time.elapsed().as_secs();
                            if elapsed >= 60 && mesh_recovery_attempts < 5 {
                                mesh_recovery_attempts += 1;
                                warn!(
                                    elapsed_secs = elapsed,
                                    attempt = mesh_recovery_attempts,
                                    group_peers = p2p_group_peers.len(),
                                    connected = connected_p2p_group_peers.len(),
                                    "ROUND 9: Mesh not formed after {}s — re-dialing group peers (attempt {})",
                                    elapsed, mesh_recovery_attempts
                                );
                                // Re-dial all unconnected group members
                                for peer_id in &p2p_group_peers {
                                    if !connected_p2p_group_peers.contains(peer_id) {
                                        let dial_opts = if let Some(addr) = group_member_addresses
                                            .get(peer_id)
                                            .filter(|addr| !is_local_or_private_multiaddr(addr))
                                        {
                                            libp2p::swarm::dial_opts::DialOpts::peer_id(*peer_id)
                                                .addresses(vec![addr.clone()])
                                                .build()
                                        } else {
                                            libp2p::swarm::dial_opts::DialOpts::peer_id(*peer_id).build()
                                        };
                                        match swarm.dial(dial_opts) {
                                            Ok(()) => info!(peer_id = %peer_id, "Mesh recovery: re-dial initiated"),
                                            Err(e) => debug!(peer_id = %peer_id, error = %e, "Mesh recovery: re-dial failed"),
                                        }
                                    }
                                }
                                // Reset join time so next attempt is 60s later
                                group_join_time = Some(Instant::now());
                            }
                        }
                    } else {
                        election_watchdog_stall_count += 1;
                        if election_watchdog_stall_count >= 2 {
                            // No block progress for 30s (2 * 15s) → re-trigger election
                            // ROUND 7: Share PoU scores BEFORE election to ensure candidates
                            // have fresh data. Without this, determine_proposer() may fail
                            // because member_pou_scores is stale/empty after mesh fragmentation.
                            // PERF: Only re-elect if WE are the proposer. Followers seeing
                            // slow block progress should NOT trigger re-elections — the
                            // proposer may just be waiting for gossipsub delivery.
                            let we_are_proposer = match intra_group_comm.try_read() {
                                Ok(igc) => igc.block_loop_running.load(std::sync::atomic::Ordering::SeqCst),
                                Err(_) => false,
                            };
                            if !we_are_proposer {
                                debug!(
                                    height = current_height,
                                    "Watchdog: stall detected but we are follower — skipping re-election"
                                );
                                election_watchdog_stall_count = 0;
                            } else {
                            warn!(
                                height = current_height,
                                stall_ticks = election_watchdog_stall_count,
                                "Block production stalled for 30s, sharing PoU + re-triggering election"
                            );
                            let igc_watchdog = intra_group_comm.clone();
                            tokio::spawn(async move {
                                let igc = igc_watchdog.read().await;
                                if let Err(e) = igc.share_pou_score().await {
                                    warn!(error=?e, "Watchdog: failed to share PoU score");
                                }
                                drop(igc);
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                let igc = igc_watchdog.read().await;
                                if let Err(e) = igc.start_proposer_election().await {
                                    warn!(error=?e, "Watchdog: failed to re-trigger election");
                                }
                            });
                            election_watchdog_stall_count = 0;
                            } // end we_are_proposer
                        }
                    }
                }

                // The MN's group_formation tick (1Hz) emits new
                // GroupAnnouncements every epoch boundary. Without this
                // detector, the LN keeps using its boot-time group_id and
                // the gossipsub TX topic /savitri/group/<old_group>/tx
                // diverges from peers that booted in different epochs —
                // mempool of the rotating proposer stays at 0 forever.
                _ = epoch_poll_timer.tick() => {
                    if genesis_ms_for_groups == 0 {
                        continue;
                    }
                    let current_epoch = savitri_consensus::primitives::epoch::current_epoch(
                        savitri_consensus::primitives::epoch::now_ms(),
                        genesis_ms_for_groups,
                        heartbeat_ms_for_groups,
                        slots_per_epoch_for_groups,
                    );
                    if current_epoch > last_observed_epoch {
                        info!(
                            old_epoch = last_observed_epoch,
                            new_epoch = current_epoch,
                            "🔄 EPOCH TRANSITION detected — flagging immediate registration rebroadcast"
                        );
                        last_observed_epoch = current_epoch;
                        epoch_change_pending = true;
                        // Force the periodic rebroadcast timer to fire on
                        // its next poll (next iteration of the select!).
                        // tokio::time::Interval::reset_immediately() is
                        // available since tokio 1.30; if missing, the
                        // 30s tick will still pick up `epoch_change_pending`
                        // within at most one period (acceptable lag).
                        registration_rebroadcast_timer.reset_immediately();
                    }
                }

                // Periodic rebroadcast of peer_info + registration so the
                // masternodes keep our `registered_nodes` entry fresh. Skipped
                // until initial registration has succeeded, otherwise we race
                // with the startup retry loop.
                _ = registration_rebroadcast_timer.tick() => {
                    if !registration_successful {
                        continue;
                    }
                    let have_mn = match priority_targets.try_read() {
                        Ok(pt) => !pt.is_empty(),
                        Err(_) => false,
                    };
                    if !have_mn {
                        continue;
                    }
                    // triggered by an epoch transition, log it so operators
                    // can correlate the rebroadcast with the underlying cause.
                    let triggered_by_epoch = epoch_change_pending;
                    epoch_change_pending = false;
                    if triggered_by_epoch {
                        info!(
                            epoch = last_observed_epoch,
                            "📤 EPOCH TRANSITION rebroadcast — re-registering with masternodes for new epoch"
                        );
                    }

                    // Rebuild the advertised listen address using the same
                    // logic as the on-PeerConnected path (mod.rs:1129-1146)
                    // so MN sees a consistent multiaddr across refreshes.
                    let listen_addr = if let Some(ext_ip) = &external_ip {
                        format!("/ip4/{}/tcp/{}", ext_ip, registration_listen_port)
                    } else {
                        let obs = match shared_observed_addr.try_read() {
                            Ok(g) => g.clone(),
                            Err(_) => String::new(),
                        };
                        if obs.is_empty() || obs.contains("tcp/0") {
                            normalize_registration_addr(
                                &swarm.listeners().next().map(|a| a.to_string())
                                    .unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".to_string()),
                                registration_listen_port,
                                None,
                            )
                        } else {
                            normalize_registration_addr(&obs, registration_listen_port, None)
                        }
                    };

                    let advertised_opt = if listen_addr.is_empty() {
                        None
                    } else {
                        Some(listen_addr.as_str())
                    };
                    if let Err(err) = broadcast_peer_info_sync(
                        &mut swarm,
                        &peer_info_topic,
                        &local_account,
                        advertised_opt,
                    ) {
                        debug!(error=?err, "periodic peer_info rebroadcast failed");
                    }
                    if let Err(err) = broadcast_lightnode_registration_sync(
                        &mut swarm,
                        &local_peer.to_string(),
                        &listen_addr,
                        &registration_reward_account,
                        registration_region,
                        registration_pou_score,
                        registration_uptime,
                    ) {
                        debug!(error=?err, "periodic registration rebroadcast failed");
                    } else {
                        debug!("periodic registration rebroadcast sent");
                    }
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH 2: Esecuzione comandi dallo Swarm Command Queue
                // I task worker (publish aggregator, maintenance) inviano
                // comandi qui per accedere allo swarm.
                // ═══════════════════════════════════════════════════════════
                Some(cmd) = command_rx.recv() => {
                    execute_swarm_command(&mut swarm, cmd);
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH 4: Deferred group init (one-time)
                // ═══════════════════════════════════════════════════════════
                msg = deferred_group_init_rx.recv() => {
                    // Init pesante (initialize) in un task separato cosi' il select! loop resta libero
                    if let Some(group_id_str) = msg {
                        // Without this, every group announcement (every ~30s) triggers a full
                        // initialize() which resets block loops and spawns new elections,
                        // causing 2.67x election explosion after 10 min.
                        // Only re-announcements for the SAME group within 15s are throttled.
                        let should_init = match last_group_init_times.get(&group_id_str) {
                            Some(t) if t.elapsed().as_secs() < GROUP_INIT_COOLDOWN_SECS => {
                                tracing::info!(
                                    group_id = %group_id_str,
                                    elapsed_secs = t.elapsed().as_secs(),
                                    cooldown = GROUP_INIT_COOLDOWN_SECS,
                                    "ROUND 12: Skipping same-group re-init (per-group cooldown active)"
                                );
                                false
                            }
                            _ => true,
                        };
                        if should_init {
                            last_group_init_times.insert(group_id_str.clone(), Instant::now());
                            let comm = intra_group_comm.clone();
                            let done_tx = deferred_init_done_tx.clone();
                            // proposer, delay initialize() by 15s so the old proposer keeps
                            // producing blocks while the new group forms and elects a new
                            // proposer. This eliminates the epoch-transition liveness gap
                            // where no blocks are produced.
                            let is_proposer_now = if let Some(ref flag) = is_intragroup_proposer_for_grace {
                                flag.try_read().map(|v| *v).unwrap_or(false)
                            } else {
                                false
                            };
                            let grace_secs: u64 = if is_proposer_now { 15 } else { 0 };
                            tokio::spawn(async move {
                                if grace_secs > 0 {
                                    tracing::info!(
                                        group_id = %group_id_str,
                                        grace_secs = grace_secs,
                                        "ROUND 13: Grace period — delaying group re-init (currently active proposer)"
                                    );
                                    tokio::time::sleep(std::time::Duration::from_secs(grace_secs)).await;
                                }
                                if let Err(err) = comm.write().await.initialize(&group_id_str).await {
                                    tracing::warn!(error=?err, "Failed to initialize intra-group communication (deferred)");
                                    return;
                                }
                                tracing::info!(group_id = %group_id_str, "Intra-group communication initialized (deferred run, in background)");
                                let _ = done_tx.send(group_id_str).await;
                            });
                        }
                    }
                }

                // ═══════════════════════════════════════════════════════════
                // BRANCH 5: Deferred init done - subscribe topics (one-time)
                // ═══════════════════════════════════════════════════════════
                done_msg = deferred_init_done_rx.recv() => {
                    // Subscribe e start_tasks richiedono swarm/task_manager: li facciamo qui nel loop principale.
                    if let Some(group_id_str) = done_msg {
                        // Unsubscribe from OLD group topics before updating to new ones
                        for old_topic in [
                            &intra_group_tx_topic, &intra_group_pou_topic, &intra_group_pou_ack_topic,
                            &intra_group_ping_topic, &intra_group_pong_topic, &intra_group_election_topic,
                            &intra_group_latency_topic, &intra_group_proposal_topic, &intra_group_vote_topic,
                        ] {
                            let _ = swarm.behaviour_mut().gossipsub.unsubscribe(old_topic);
                        }

                        if let Ok(igc_ref) = intra_group_comm.try_read() {
                            intra_group_tx_topic = igc_ref.get_tx_topic();
                            intra_group_pou_topic = igc_ref.get_pou_topic();
                            intra_group_pou_ack_topic = igc_ref.get_pou_ack_topic();
                            intra_group_ping_topic = igc_ref.get_ping_topic();
                            intra_group_pong_topic = igc_ref.get_pong_topic();
                            intra_group_election_topic = igc_ref.get_election_topic();
                            intra_group_latency_topic = igc_ref.get_latency_topic();
                            intra_group_proposal_topic = igc_ref.get_proposal_topic();
                            intra_group_vote_topic = igc_ref.get_vote_topic();
                        } else {
                            warn!("Could not acquire intra_group_comm read lock for topic refresh; using stale topics");
                        }
                        if let Ok(mut shared_tx) = shared_intra_group_tx_topic.try_write() {
                            *shared_tx = intra_group_tx_topic.clone();
                        }
                        let mut subscribed_ok = 0u8;
                        for (topic, name) in [
                            (intra_group_tx_topic.clone(), "tx"),
                            (intra_group_pou_topic.clone(), "pou"),
                            (intra_group_pou_ack_topic.clone(), "pou_ack"),
                            (intra_group_ping_topic.clone(), "ping"),
                            (intra_group_pong_topic.clone(), "pong"),
                            (intra_group_election_topic.clone(), "election"),
                            (intra_group_latency_topic.clone(), "latency"),
                            // V0.2 Phase 1 (Score Canonicity, issue #31)
                            (crate::latency_canon_publisher::topic_for_group(&group_id_str), "latency_canon"),
                            // V0.2 Phase 2 (Lattice ordering, issue #32)
                            (crate::lattice_runtime::cell_topic_for_group(&group_id_str), "lattice_cell"),
                            (crate::lattice_runtime::attestation_topic_for_group(&group_id_str), "lattice_attestation"),
                            (crate::lattice_runtime::batch_topic_for_group(&group_id_str), "lattice_batch"),
                            (intra_group_proposal_topic.clone(), "proposal"),
                            (intra_group_vote_topic.clone(), "vote"),
                        ] {
                            if swarm.behaviour_mut().gossipsub.subscribe(&topic).is_ok() {
                                subscribed_ok += 1;
                                info!(topic = %name, "Swarm subscribed to intra-group broadcast topic (deferred)");
                            }
                        }
                        info!(subscribed = subscribed_ok, "Intra-group broadcast subscription done (deferred)");
                        intra_group_mesh_established = false;
                        // Reset mesh_ready flag so watchdog doesn't fire during re-election
                        intra_group_mesh_ready.store(false, AtomicOrdering::Release);
                        // Reset watchdog stall count for the new group
                        election_watchdog_stall_count = 0;
                        {
                            let igc_ping = intra_group_comm.clone();
                            tokio::spawn(async move {
                                if let Err(err) = igc_ping.read().await.send_group_ping().await {
                                    warn!(error=?err, "Failed to send initial GroupPing");
                                }
                            });
                        }
                        // Only start periodic tasks once (they use shared refs and keep running)
                        if !periodic_tasks_started {
                            periodic_tasks_started = true;
                            let ptm = periodic_task_manager.clone();
                            tokio::spawn(async move {
                                if let Err(err) = ptm.start_tasks().await {
                                    warn!(error=?err, "Failed to start periodic intra-group tasks");
                                } else {
                                    info!("Periodic intra-group tasks started (deferred)");
                                }
                            });
                        } else {
                            info!("Group re-initialized; periodic tasks already running");
                            // Spawn immediate re-election task so we don't wait 60s (PoU) + 300s (election).
                            // Don't gate on is_mesh_ready() — gossipsub mesh for new topics may never
                            // form (peers' deferred init delays), but elections use direct P2P which
                            // works as long as peers are connected (they always are after re-init).
                            let re_election_comm = intra_group_comm.clone();
                            tokio::spawn(async move {
                                // First election attempt now at 3s instead of 8s.
                                // If it fails, retry aggressively (3 attempts, 5s apart)
                                // before falling back to the 30s watchdog.
                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                                info!("Triggering immediate PoU + election after group re-init (no mesh gate)");
                                let comm = re_election_comm.read().await;
                                if let Err(e) = comm.share_pou_score().await {
                                    warn!("Re-init PoU sharing failed: {}", e);
                                }
                                drop(comm);
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                                // Attempt election up to 3 times with 5s intervals
                                for attempt in 1..=3 {
                                    let comm = re_election_comm.read().await;
                                    match comm.start_proposer_election().await {
                                        Ok(()) => {
                                            info!(
                                                attempt = attempt,
                                                "Re-init proposer election triggered successfully"
                                            );
                                            break;
                                        }
                                        Err(e) => {
                                            warn!(
                                                attempt = attempt,
                                                error = %e,
                                                "Re-init election attempt failed, {}",
                                                if attempt < 3 { "retrying in 5s" } else { "falling back to watchdog" }
                                            );
                                        }
                                    }
                                    drop(comm);
                                    if attempt < 3 {
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                        // Re-share PoU before retry
                                        let comm = re_election_comm.read().await;
                                        if let Err(e) = comm.share_pou_score().await {
                                            warn!("Re-init PoU re-share failed: {}", e);
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
    });

    info!("NETWORK STEP 4/4: ✅ Network setup complete");

    let pou_state_arc = Arc::new(RwLock::new(pou_state_for_return));
    // Wait briefly for the spawn to set command_tx, then retrieve it
    let command_tx_for_return = loop {
        if let Some(tx) = shared_command_tx.lock().unwrap().clone() {
            break tx;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    Ok(NetworkComponents {
        tx_sender: tx_sender_clone,
        have_tx_sender,
        tx_forward_sender,
        local_peer: local_peer_id,
        heartbeat_sender,
        pou_sender,
        block_sender,
        pou_state: pou_state_arc,
        peer_accounts: Arc::clone(&peer_accounts),
        dag_manager: dag_manager_for_return,
        command_tx: command_tx_for_return,
        network_events: network_events_for_return,
        listen_addrs: listen_addrs_for_return,
        observed_addr: observed_addr_for_return,
        connected_peers: connected_peers_for_return,
        tx_store: tx_store_for_return,
        task: handle,
        shard_to_group: shard_to_group_for_return,
        num_shards: num_shards_for_return,
        group_manager: group_manager_for_return,
    })
}

// Helper functions
fn compute_remote_pou(
    report: &PouBroadcast,
    node_id: [u8; 32],
    scoring: &PouScoring,
    histories: &mut HashMap<[u8; 32], FixedPoint>,
    prev_scores: &mut HashMap<[u8; 32], FixedPoint>,
) -> Option<u16> {
    let prev_score = prev_scores
        .get(&node_id)
        .copied()
        .unwrap_or(FixedPoint::from_raw_u32(0));

    let uptime_ratio = if report.uptime_claim.h_total > 0 {
        report.uptime_claim.h_ok as f64 / report.uptime_claim.h_total as f64
    } else {
        0.0
    };

    let new_score = if uptime_ratio >= 0.95 {
        FixedPoint::from_raw_u32(1000)
    } else if uptime_ratio >= 0.5 {
        FixedPoint::from_raw_u32(500)
    } else {
        FixedPoint::from_raw_u32(100)
    };

    histories.insert(node_id, new_score);
    prev_scores.insert(node_id, new_score);

    Some((new_score.raw() / 1000) as u16)
}

fn matches_digest_prefix(candidate: &[u8], sha256: &[u8], sha512: &[u8], min_len: usize) -> bool {
    if candidate.len() < min_len {
        return false;
    }
    let matches_256 = candidate.len() <= sha256.len() && candidate == &sha256[..candidate.len()];
    let matches_512 = candidate.len() <= sha512.len() && candidate == &sha512[..candidate.len()];
    matches_256 || matches_512
}

fn verify_monolith_data(monolith_id: &[u8], monolith_hash: &[u8], monolith_data: &[u8]) -> bool {
    if monolith_id.is_empty() || monolith_data.is_empty() {
        return false;
    }

    const MAX_MONOLITH_SIZE: usize = 10 * 1024 * 1024;
    if monolith_data.len() > MAX_MONOLITH_SIZE {
        warn!(
            "Monolith data too large: {} bytes (max: {})",
            monolith_data.len(),
            MAX_MONOLITH_SIZE
        );
        return false;
    }

    use sha2::{Digest, Sha256, Sha512};
    let sha256_hash = Sha256::digest(monolith_data);
    let sha512_hash = Sha512::digest(monolith_data);

    if monolith_id.len() < 8 {
        warn!(
            "Monolith ID too short: {} bytes (min: 8)",
            monolith_id.len()
        );
        return false;
    }

    // Compatibility: accept both SHA-256 and SHA-512 prefixes/lengths so
    // masternode/lightnode hash-schema differences do not break verification.
    let id_ok = matches_digest_prefix(
        monolith_id,
        sha256_hash.as_slice(),
        sha512_hash.as_slice(),
        8,
    );
    if !id_ok {
        warn!(
            monolith_id_len = monolith_id.len(),
            "Monolith ID does not match SHA-256/SHA-512 digest prefix"
        );
        return false;
    }

    if !monolith_hash.is_empty() {
        let hash_ok = matches_digest_prefix(
            monolith_hash,
            sha256_hash.as_slice(),
            sha512_hash.as_slice(),
            8,
        );
        if !hash_ok {
            warn!(
                monolith_hash_len = monolith_hash.len(),
                "Monolith hash does not match SHA-256/SHA-512 digest prefix"
            );
            return false;
        }
    }

    true
}

fn compute_monolith_hash(monolith_data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(monolith_data);
    Ok(hasher.finalize().to_vec())
}

// ═══════════════════════════════════════════════════════════════════════════
// Swarm Command Queue Pattern: Esecuzione comandi
// ═══════════════════════════════════════════════════════════════════════════

/// Runs un `SwarmCommand` sul swarm.
///
/// E' l'unico punto in cui i task worker possono interagire con lo swarm.
fn execute_swarm_command(swarm: &mut Swarm<crate::p2p::types::MyBehaviour>, cmd: SwarmCommand) {
    match cmd {
        SwarmCommand::Publish { topic, payload } => {
            // Diagnostica: conta i peer noti per il topic (mesh + explicit)
            let mesh_peers = swarm
                .behaviour()
                .gossipsub
                .mesh_peers(&topic.hash())
                .count();
            let all_peers = swarm.behaviour().gossipsub.all_peers().count();

            // DIAGNOSTIC: enumerate peers_on_topic for critical topics
            let topic_str_diag = topic.to_string();
            if topic_str_diag.contains("proposal") || topic_str_diag.contains("election") {
                let peers_with_topic: Vec<_> = swarm
                    .behaviour()
                    .gossipsub
                    .all_peers()
                    .filter(|(_, topics)| topics.iter().any(|t| **t == topic.hash()))
                    .map(|(pid, topics)| format!("{}({}topics)", pid, topics.len()))
                    .collect();
                let peers_without_topic: Vec<_> = swarm
                    .behaviour()
                    .gossipsub
                    .all_peers()
                    .filter(|(_, topics)| !topics.iter().any(|t| **t == topic.hash()))
                    .map(|(pid, topics)| format!("{}({}topics)", pid, topics.len()))
                    .collect();
                let mesh_peer_ids: Vec<_> = swarm
                    .behaviour()
                    .gossipsub
                    .mesh_peers(&topic.hash())
                    .map(|pid| pid.to_string())
                    .collect();
                info!(
                    topic = %topic,
                    peers_on_topic = peers_with_topic.len(),
                    peers_off_topic = peers_without_topic.len(),
                    "🔍 DIAGNOSTIC peers_on_topic: {:?}", peers_with_topic
                );
                info!(
                    topic = %topic,
                    "🔍 DIAGNOSTIC peers_OFF_topic: {:?}", peers_without_topic
                );
                info!(
                    topic = %topic,
                    "🔍 DIAGNOSTIC mesh_peers: {:?}", mesh_peer_ids
                );
            }

            match swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), payload)
            {
                Ok(mid) => {
                    let topic_str = topic.to_string();
                    if topic_str.contains("proposal") || topic_str.contains("election") {
                        info!(
                            topic = %topic,
                            topic_hash = %topic.hash(),
                            message_id = ?mid,
                            mesh_peers = mesh_peers,
                            total_peers = all_peers,
                            "📤 SwarmCommand::Publish executed (critical topic)"
                        );
                    } else {
                        debug!(
                            topic_hash = %topic.hash(),
                            message_id = ?mid,
                            mesh_peers = mesh_peers,
                            "SwarmCommand::Publish executed"
                        );
                    }
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    if err_str.contains("InsufficientPeers") && mesh_peers == 0 {
                        debug!(
                            error = ?e,
                            topic = %topic,
                            mesh_peers = mesh_peers,
                            total_peers = all_peers,
                            "SwarmCommand::Publish failed (no mesh peers on topic)"
                        );
                    } else {
                        warn!(
                            error = ?e,
                            topic = %topic,
                            topic_hash = %topic.hash(),
                            mesh_peers = mesh_peers,
                            total_peers = all_peers,
                            "SwarmCommand::Publish failed"
                        );
                    }
                }
            }
        }
        SwarmCommand::PublishBatch { messages } => {
            for (topic, payload) in messages {
                match swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(topic.clone(), payload)
                {
                    Ok(mid) => {
                        debug!(
                            topic_hash = %topic.hash(),
                            message_id = ?mid,
                            "SwarmCommand::PublishBatch item executed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            error = ?e,
                            topic_hash = %topic.hash(),
                            "SwarmCommand::PublishBatch item failed"
                        );
                    }
                }
            }
        }
        SwarmCommand::Dial { peer_id, addresses } => {
            let original_address_count = addresses.len();
            let addresses: Vec<_> = addresses
                .into_iter()
                .filter(|addr| !is_local_or_private_multiaddr(addr))
                .collect();
            if original_address_count > 0 && addresses.is_empty() {
                debug!(
                    peer_id = %peer_id,
                    "SwarmCommand::Dial skipped after filtering private addresses"
                );
                return;
            }
            let dial_opts = if addresses.is_empty() {
                libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id).build()
            } else {
                libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                    .addresses(addresses)
                    .build()
            };
            match swarm.dial(dial_opts) {
                Ok(()) => {
                    debug!(peer_id = %peer_id, "SwarmCommand::Dial initiated");
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("already connected")
                        || err_str.contains("PeerCondition")
                        || err_str.contains("Dialing")
                    {
                        debug!(
                            peer_id = %peer_id,
                            "SwarmCommand::Dial skipped (already connected or in progress)"
                        );
                    } else {
                        warn!(
                            peer_id = %peer_id,
                            error = %e,
                            "SwarmCommand::Dial failed"
                        );
                    }
                }
            }
        }
        SwarmCommand::Subscribe { topic } => {
            match swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                Ok(_) => {
                    info!(topic = %topic, "SwarmCommand::Subscribe executed");
                }
                Err(e) => {
                    warn!(error = %e, topic = %topic, "SwarmCommand::Subscribe failed");
                }
            }
        }
        SwarmCommand::Unsubscribe { topic } => {
            if swarm.behaviour_mut().gossipsub.unsubscribe(&topic) {
                info!(topic = %topic, "SwarmCommand::Unsubscribe executed");
            } else {
                warn!(topic = %topic, "SwarmCommand::Unsubscribe failed (not subscribed)");
            }
        }
        SwarmCommand::AddExplicitPeer { peer_id } => {
            // WARNING: adding a peer as explicit prevents gossipsub from adding
            // it to the mesh (GRAFT rejected) and may prevent subscription exchange.
            // This should NOT be used for masternodes or group peers.
            warn!(peer_id = %peer_id, "SwarmCommand::AddExplicitPeer executed — verify this peer is not a masternode/group peer");
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
        }
        SwarmCommand::RemoveExplicitPeer { peer_id } => {
            swarm
                .behaviour_mut()
                .gossipsub
                .remove_explicit_peer(&peer_id);
            debug!(peer_id = %peer_id, "SwarmCommand::RemoveExplicitPeer executed");
        }
        SwarmCommand::KadPutRecord { key, value } => {
            let record = libp2p::kad::Record {
                key: libp2p::kad::RecordKey::new(&key),
                value,
                publisher: None,
                expires: None,
            };
            match swarm
                .behaviour_mut()
                .kademlia
                .put_record(record, libp2p::kad::Quorum::One)
            {
                Ok(_) => {
                    debug!(key = %key, "SwarmCommand::KadPutRecord executed");
                }
                Err(e) => {
                    debug!(key = %key, error = %e, "SwarmCommand::KadPutRecord failed");
                }
            }
        }
        SwarmCommand::KadGetRecord { key } => {
            let record_key = libp2p::kad::RecordKey::new(&key);
            swarm.behaviour_mut().kademlia.get_record(record_key);
            debug!(key = %key, "SwarmCommand::KadGetRecord executed");
        }
        SwarmCommand::SendConsensusRequest { peer_id, message } => {
            let msg_type = match &message {
                crate::p2p::consensus_protocol::ConsensusMessage::Vote(_) => "vote",
                crate::p2p::consensus_protocol::ConsensusMessage::Election(_) => "election",
                crate::p2p::consensus_protocol::ConsensusMessage::ElectionResult(_) => {
                    "election_result"
                }
                crate::p2p::consensus_protocol::ConsensusMessage::Latency(_) => "latency",
                crate::p2p::consensus_protocol::ConsensusMessage::LatencyResponse(_) => {
                    "latency_response"
                }
                crate::p2p::consensus_protocol::ConsensusMessage::PoU(_) => "pou",
                crate::p2p::consensus_protocol::ConsensusMessage::PoUAck(_) => "pou_ack",
            };
            let req_id = swarm
                .behaviour_mut()
                .consensus
                .send_request(&peer_id, message);
            debug!(
                peer_id = %peer_id,
                msg_type = msg_type,
                request_id = ?req_id,
                "SwarmCommand::SendConsensusRequest executed"
            );
        }
        SwarmCommand::SendAuxRequest { peer_id, message } => {
            let msg_type = match &message {
                crate::p2p::aux_protocol::AuxMessage::Heartbeat(_) => "heartbeat",
                crate::p2p::aux_protocol::AuxMessage::PoU(_) => "pou",
                crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryRequest(_) => {
                    "peer_discovery_req"
                }
                crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryResponse(_) => {
                    "peer_discovery_resp"
                }
                crate::p2p::aux_protocol::AuxMessage::PeerRegistry(_) => "peer_registry",
                crate::p2p::aux_protocol::AuxMessage::BlockSync(_) => "block_sync",
                crate::p2p::aux_protocol::AuxMessage::TxForward(_) => "tx_forward",
            };
            let req_id = swarm.behaviour_mut().aux.send_request(&peer_id, message);
            debug!(
                peer_id = %peer_id,
                msg_type = msg_type,
                request_id = ?req_id,
                "SwarmCommand::SendAuxRequest executed (direct TCP)"
            );
        }
        SwarmCommand::SendBlockSyncRequest { peer_id, request } => {
            match bincode::serialize(&request) {
                Ok(payload) => {
                    let req_id = swarm.behaviour_mut().aux.send_request(
                        &peer_id,
                        crate::p2p::aux_protocol::AuxMessage::BlockSync(payload),
                    );
                    debug!(
                        peer_id = %peer_id,
                        request_id = ?req_id,
                        start_height = request.start_height,
                        "SwarmCommand::SendBlockSyncRequest executed"
                    );
                }
                Err(err) => {
                    warn!(peer_id = %peer_id, error = %err, "Failed to serialize block sync request");
                }
            }
        }
        SwarmCommand::Shutdown => {
            info!("SwarmCommand::Shutdown received");
        }
    }
}
