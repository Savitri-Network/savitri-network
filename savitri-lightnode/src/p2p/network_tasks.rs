//! Task separati per il pattern Swarm Command Queue.
//!
//! con lo swarm task tramite `SwarmCommand` e `NetworkEvent`.
//!
//! - `run_logging_task`: draina canali di sola lettura (zero rischio)
//! - `run_maintenance_task`: timer e retry di connessione

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use libp2p::{gossipsub::IdentTopic, Multiaddr, PeerId};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::integrity::IntegrityEvent;
use crate::p2p::broadcast::{encode_gossip, hash_signed_tx_bytes};
use crate::p2p::certificate::CertificatePendingBlocks;
use crate::p2p::intra_group::{PeerDiscoveryRequest, PeerRegistryAnnounce};
use crate::p2p::network::is_local_or_private_multiaddr;
use crate::p2p::pou::PouState;
use crate::p2p::swarm_commands::{NetworkEvent, SwarmCommand};
use crate::p2p::types::{
    BlockBroadcast, ConnectionPool, GossipMessage, HaveTx, HeartbeatMessage, PendingBlockData,
    PouBroadcast, SignedTx, TxMessage,
};
use crate::resource::{emit_event, ResourceEvent, TrafficDirection};

// ─────────────────────────────────────────────────────────────────────────────
// Task 4: Logging / Drain-only channels
// ─────────────────────────────────────────────────────────────────────────────

/// Task dedicato a drenare i canali che eseguono solo logging.
///
///
/// Canali gestiti:
/// - `have_tx_receiver`: debug log per HaveTx
/// - `certificate_receiver`: info log per ConsensusCertificate
/// - `integrity_receiver`: debug log per IntegrityEvent
/// - `pou_receiver`: debug log per PouBroadcast
/// - `connection_health_timer`: check periodico connessioni (read-only)
pub async fn run_logging_task(
    mut have_tx_receiver: mpsc::Receiver<HaveTx>,
    mut certificate_receiver: mpsc::Receiver<crate::p2p::types::ConsensusCertificate>,
    mut integrity_receiver: mpsc::Receiver<IntegrityEvent>,
    mut pou_receiver: mpsc::Receiver<PouBroadcast>,
    connection_pool: Arc<Mutex<ConnectionPool>>,
) {
    let mut connection_health_timer = tokio::time::interval(Duration::from_secs(3));
    connection_health_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!("Logging task started (5 drain-only channels)");

    loop {
        tokio::select! {
            maybe_have_tx = have_tx_receiver.recv() => {
                match maybe_have_tx {
                    Some(have_tx) => {
                        let first_hash = have_tx.tx_hashes.first().copied().unwrap_or([0u8; 32]);
                        debug!("Received HaveTx message: {}", hex::encode(first_hash));
                    }
                    None => {
                        warn!("HaveTx channel closed (logging task)");
                        break;
                    }
                }
            }

            maybe_cert = certificate_receiver.recv() => {
                match maybe_cert {
                    Some(cert) => {
                        info!(
                            height = cert.height,
                            round = cert.round,
                            voters = cert.voters.len(),
                            hash = %hex::encode(cert.block_hash),
                            "Masternode ACK: certificate received on channel"
                        );
                        info!(
                            height = cert.height,
                            hash = %hex::encode(cert.block_hash),
                            "Certificate received; will trigger block finality when pending block matches"
                        );
                    }
                    None => {
                        warn!("Certificate channel closed (logging task)");
                        break;
                    }
                }
            }

            maybe_integrity = integrity_receiver.recv() => {
                match maybe_integrity {
                    Some(event) => {
                        debug!("Received integrity event: {:?}", event);
                    }
                    None => {
                        warn!("Integrity channel closed (logging task)");
                        break;
                    }
                }
            }

            maybe_pou_event = pou_receiver.recv() => {
                match maybe_pou_event {
                    Some(event) => {
                        debug!("Received PoU event: {:?}", event);
                    }
                    None => {
                        warn!("PoU event channel closed (logging task)");
                        break;
                    }
                }
            }

            _ = connection_health_timer.tick() => {
                let pool = connection_pool.lock().await;
                let active_count = pool.get_active_connections().len();
                if active_count > 0 {
                    debug!(
                        active_connections = active_count,
                        "Connection health check (logging task)"
                    );
                }
            }
        }
    }

    warn!("Logging task exiting");
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 2: Publish Aggregator (non-biased, fair scheduling)
// ─────────────────────────────────────────────────────────────────────────────

/// Task dedicato alla pubblicazione di messaggi gossipsub (v2 aux).
///
/// dal numero di nodi.
///
/// `SwarmCommand::Publish` allo swarm task.
/// Maximum number of TX commands buffered while waiting for intra-group mesh.
const TX_MESH_BUFFER_MAX: usize = 2000;

pub async fn run_publish_aggregator(
    command_tx: mpsc::Sender<SwarmCommand>,
    mut tx_rx: mpsc::Receiver<SignedTx>,
    mut block_rx: mpsc::Receiver<(BlockBroadcast, PendingBlockData)>,
    mut block_broadcast_only_rx: mpsc::Receiver<BlockBroadcast>,
    mut pou_rx: mpsc::Receiver<PouBroadcast>,
    mut heartbeat_rx: mpsc::Receiver<HeartbeatMessage>,
    mut intra_publish_rx: mpsc::Receiver<(IdentTopic, Vec<u8>)>,
    mut tx_forward_rx: mpsc::Receiver<SignedTx>,
    tx_topic: IdentTopic,
    block_topic: IdentTopic,
    pou_topic: IdentTopic,
    heartbeat_topic: IdentTopic,
    intra_group_tx_topic: Arc<RwLock<IdentTopic>>,
    certificate_pending: Arc<Mutex<CertificatePendingBlocks>>,
    pou_state: PouState,
    local_peer: PeerId,
    mut resource_events: Option<mpsc::Sender<ResourceEvent>>,
    intra_group_mesh_ready: Arc<AtomicBool>,
    masternode_peers: Arc<RwLock<HashMap<PeerId, Multiaddr>>>,
) {
    let mut last_heartbeat_sent = Instant::now();
    let mut tx_pending_buffer: VecDeque<SwarmCommand> = VecDeque::new();

    info!("Publish aggregator started (7 channels, non-biased select)");

    loop {
        // Flush buffered TX commands once intra-group mesh is ready
        if intra_group_mesh_ready.load(Ordering::Relaxed) && !tx_pending_buffer.is_empty() {
            let count = tx_pending_buffer.len();
            info!(
                buffered = count,
                "Intra-group mesh ready: flushing buffered TX commands"
            );
            while let Some(cmd) = tx_pending_buffer.pop_front() {
                if let Err(e) = command_tx.send(cmd).await {
                    warn!(error = %e, "Failed to flush buffered tx command to swarm");
                    break;
                }
            }
            tx_pending_buffer.clear();
        }

        // IMPORTANTE: select! SENZA biased = fair scheduling.
        tokio::select! {
            // ── Transazioni globali LN↔LN (topic /savitri/tx/1) ───────────
            maybe_tx = tx_rx.recv() => {
                match maybe_tx {
                    Some(tx) => {
                        match serialize_signed_tx_for_gossip(&tx, &tx_topic) {
                            Ok(messages) => {
                                let cmd = SwarmCommand::PublishBatch { messages };
                                if let Err(e) = command_tx.send(cmd).await {
                                    warn!(error = %e, "Failed to send tx publish command to swarm");
                                }
                            }
                            Err(e) => {
                                debug!("Failed to serialize transaction for broadcast: {}", e);
                            }
                        }
                    }
                    None => {
                        warn!("TX channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── Blocchi (broadcast a lightnodes) ────────────────────────
            maybe_block = block_rx.recv() => {
                match maybe_block {
                    Some((block_msg, pending_data)) => {
                        let block_hash_arr = block_msg.block.hash;
                        let block_height = block_msg.have.exec_height;

                        info!(
                            height = block_height,
                            hash = %hex::encode(block_hash_arr),
                            txs = block_msg.have.tx_count,
                            "📦 [LN->LN] Block from internal producer - publishing via aggregator"
                        );

                        match serialize_block_broadcast(&block_msg, &block_topic) {
                            Ok(messages) => {
                                let total_bytes: usize = messages.iter().map(|(_, p)| p.len()).sum();
                                let cmd = SwarmCommand::PublishBatch { messages };
                                if let Err(e) = command_tx.send(cmd).await {
                                    warn!(error = %e, "Failed to send block publish command to swarm");
                                } else {
                                    emit_event(
                                        &mut resource_events,
                                        ResourceEvent::Gossip {
                                            direction: TrafficDirection::Outbound,
                                            bytes: total_bytes,
                                        },
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to serialize block for broadcast: {}", e);
                            }
                        }

                        // Registra il blocco come pending per la finalita' of the certificato
                        {
                            let mut cert_pending = certificate_pending.lock().await;
                            cert_pending.register_pending(
                                block_hash_arr,
                                block_height,
                                pending_data,
                                local_peer,
                            );
                        }
                    }
                    None => {
                        warn!("Block channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── Blocco solo broadcast (intra-group proposer -> block_topic per MN cache) ──
            maybe_block_only = block_broadcast_only_rx.recv() => {
                match maybe_block_only {
                    Some(block_msg) => {
                        info!(
                            height = block_msg.have.exec_height,
                            hash = %hex::encode(block_msg.block.hash),
                            txs = block_msg.have.tx_count,
                            "📦 [LN->block_topic] Block from intra-group proposer - publishing for MN cache (block_final)"
                        );
                        match serialize_block_broadcast(&block_msg, &block_topic) {
                            Ok(messages) => {
                                let total_bytes: usize = messages.iter().map(|(_, p)| p.len()).sum();
                                let cmd = SwarmCommand::PublishBatch { messages };
                                if let Err(e) = command_tx.send(cmd).await {
                                    warn!(error = %e, "Failed to send block_broadcast_only publish to swarm");
                                } else {
                                    emit_event(
                                        &mut resource_events,
                                        ResourceEvent::Gossip {
                                            direction: TrafficDirection::Outbound,
                                            bytes: total_bytes,
                                        },
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Failed to serialize block_broadcast_only: {}", e);
                            }
                        }
                    }
                    None => {
                        warn!("Block broadcast-only channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── PoU reports ─────────────────────────────────────────────
            maybe_pou = pou_rx.recv() => {
                match maybe_pou {
                    Some(report) => {
                        match serde_json::to_vec(&report) {
                            Ok(payload) => {
                                pou_state.record_report(&local_peer, report.epoch, report.score).await;

                                // Send PoU via direct TCP to masternodes (aux protocol)
                                let mn_peers: Vec<PeerId> = masternode_peers.read().await.keys().cloned().collect();
                                if !mn_peers.is_empty() {
                                    for mn_peer in &mn_peers {
                                        let cmd = SwarmCommand::SendAuxRequest {
                                            peer_id: mn_peer.clone(),
                                            message: crate::p2p::aux_protocol::AuxMessage::PoU(payload.clone()),
                                        };
                                        if let Err(e) = command_tx.send(cmd).await {
                                            debug!("Failed to send PoU aux to {}: {}", mn_peer, e);
                                        }
                                    }
                                } else {
                                    // Fallback to gossipsub if no masternodes known yet
                                    let cmd = SwarmCommand::Publish {
                                        topic: pou_topic.clone(),
                                        payload,
                                    };
                                    if let Err(e) = command_tx.send(cmd).await {
                                        debug!("Failed to send PoU publish command: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to encode PoU: {}", e);
                            }
                        }
                    }
                    None => {
                        warn!("PoU channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── Heartbeat ───────────────────────────────────────────────
            maybe_heartbeat = heartbeat_rx.recv() => {
                match maybe_heartbeat {
                    Some(hb) => {
                        // Rate limit: max 1 heartbeat al secondo
                        if last_heartbeat_sent.elapsed() < Duration::from_millis(1000) {
                            continue;
                        }

                        match encode_gossip(&GossipMessage::Heartbeat(hb)) {
                            Ok(payload) => {
                                let payload_len = payload.len();
                                // Send heartbeat via direct TCP to masternodes (aux protocol)
                                // instead of gossipsub to prevent Send Queue saturation
                                let mn_peers: Vec<PeerId> = masternode_peers.read().await.keys().cloned().collect();
                                if !mn_peers.is_empty() {
                                    for mn_peer in &mn_peers {
                                        let cmd = SwarmCommand::SendAuxRequest {
                                            peer_id: mn_peer.clone(),
                                            message: crate::p2p::aux_protocol::AuxMessage::Heartbeat(payload.clone()),
                                        };
                                        if let Err(e) = command_tx.send(cmd).await {
                                            debug!("Failed to send heartbeat aux to {}: {}", mn_peer, e);
                                        }
                                    }
                                    emit_event(
                                        &mut resource_events,
                                        ResourceEvent::Gossip {
                                            direction: TrafficDirection::Outbound,
                                            bytes: payload_len * mn_peers.len(),
                                        },
                                    );
                                } else {
                                    // Fallback to gossipsub if no masternodes known yet
                                    let cmd = SwarmCommand::Publish {
                                        topic: heartbeat_topic.clone(),
                                        payload,
                                    };
                                    if let Err(e) = command_tx.send(cmd).await {
                                        debug!("Failed to send heartbeat publish command: {}", e);
                                    }
                                }
                                last_heartbeat_sent = Instant::now();
                            }
                            Err(e) => {
                                debug!("Failed to encode heartbeat: {}", e);
                            }
                        }
                    }
                    None => {
                        warn!("Heartbeat channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── Intra-group publish (block proposals, etc.) ─────────────
            msg = intra_publish_rx.recv() => {
                match msg {
                    Some((topic, payload)) => {
                        info!(
                            topic = %topic,
                            payload_bytes = payload.len(),
                            "📤 [PUBLISH-AGG] Intra-group publish request -> SwarmCommand::Publish"
                        );
                        let cmd = SwarmCommand::Publish { topic, payload };
                        if let Err(e) = command_tx.send(cmd).await {
                            warn!(error = %e, "Failed to send intra-group publish command to swarm");
                        }
                    }
                    None => {
                        warn!("Intra-group publish channel closed (publish aggregator)");
                        break;
                    }
                }
            }

            // ── TX forward (intra-group transactions) ───────────────────
            maybe_tx_forward = tx_forward_rx.recv() => {
                match maybe_tx_forward {
                    Some(tx) => {
                        let current_topic = intra_group_tx_topic.read().await.clone();
                        match serialize_signed_tx_for_gossip(&tx, &current_topic) {
                            Ok(messages) => {
                                let cmd = SwarmCommand::PublishBatch { messages };
                                if intra_group_mesh_ready.load(Ordering::Relaxed) {
                                    if let Err(e) = command_tx.send(cmd).await {
                                        debug!("Failed to send intra-group tx publish command: {}", e);
                                    } else {
                                        info!(
                                            nonce = tx.nonce,
                                            amount = tx.amount,
                                            "Transaction communication in progress (intra-group)"
                                        );
                                    }
                                } else {
                                    if tx_pending_buffer.len() >= TX_MESH_BUFFER_MAX {
                                        tx_pending_buffer.pop_front();
                                    }
                                    tx_pending_buffer.push_back(cmd);
                                }
                            }
                            Err(e) => {
                                debug!("Failed to serialize intra-group transaction: {}", e);
                            }
                        }
                    }
                    None => {
                        info!("Transaction communication end (intra-group TX channel closed)");
                        warn!("TX forward channel closed (publish aggregator)");
                    }
                }
            }
        }
    }

    warn!("Publish aggregator exiting");
}

// ─────────────────────────────────────────────────────────────────────────────
// Task 3: Maintenance (timers, connection retry, registry announce)
// ─────────────────────────────────────────────────────────────────────────────

///
/// Receives `NetworkEvent` from the swarm task to track the state of
/// connessioni, e gestisce i timer di retry e registry announce.
///
/// Invia `SwarmCommand::Dial`, `SwarmCommand::KadPutRecord`, etc.
/// allo swarm task quando necessario.
pub async fn run_maintenance_task(
    command_tx: mpsc::Sender<SwarmCommand>,
    mut event_rx: broadcast::Receiver<NetworkEvent>,
    priority_targets: Arc<RwLock<HashMap<PeerId, Multiaddr>>>,
    local_peer: PeerId,
    local_listen_addr: Arc<RwLock<String>>,
    listen_port: u16,
    mut peer_discovery_rx: mpsc::Receiver<()>,
    initial_registration_successful: bool,
) {
    let mut connection_retry_timer = tokio::time::interval(Duration::from_secs(5));
    connection_retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut group_peer_retry_timer = tokio::time::interval(Duration::from_secs(2));
    group_peer_retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut group_peer_retry_skip_first_tick = true;

    let mut registry_announce_timer = tokio::time::interval(Duration::from_secs(10));
    registry_announce_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Periodic MN discovery timer (every 60s, pull-based fallback via gossipsub PeerDiscoveryRequest)
    let mut mn_discovery_timer = tokio::time::interval(Duration::from_secs(60));
    mn_discovery_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Stato locale tracciato dagli eventi
    let mut connected_priority_peers: HashSet<PeerId> = HashSet::new();
    let mut connected_p2p_group_peers: HashSet<PeerId> = HashSet::new();
    let mut p2p_group_peers: HashSet<PeerId> = HashSet::new();
    let mut group_member_addresses: HashMap<PeerId, Multiaddr> = HashMap::new();
    let mut priority_last_attempt: HashMap<PeerId, Instant> = HashMap::new();
    let registration_successful = initial_registration_successful;

    let pt_count = priority_targets.read().await.len();
    info!(
        priority_targets = pt_count,
        "Maintenance task started (timers + connection tracking)"
    );

    loop {
        tokio::select! {
            // ── Ricezione eventi dallo swarm task ───────────────────────
            event = event_rx.recv() => {
                match event {
                    Ok(NetworkEvent::PeerConnected { peer_id, is_masternode }) => {
                        if is_masternode {
                            connected_priority_peers.insert(peer_id);
                            priority_last_attempt.remove(&peer_id);
                            let total_mn = priority_targets.read().await.len();
                            debug!(
                                peer = %peer_id,
                                connected = connected_priority_peers.len(),
                                total = total_mn,
                                "Maintenance: masternode peer connected"
                            );
                        }
                        if p2p_group_peers.contains(&peer_id) {
                            connected_p2p_group_peers.insert(peer_id);
                            debug!(
                                peer = %peer_id,
                                connected = connected_p2p_group_peers.len(),
                                total = p2p_group_peers.len(),
                                "Maintenance: group peer connected"
                            );
                        }
                    }
                    Ok(NetworkEvent::PeerDisconnected { peer_id }) => {
                        connected_priority_peers.remove(&peer_id);
                        connected_p2p_group_peers.remove(&peer_id);
                        debug!(peer = %peer_id, "Maintenance: peer disconnected");
                    }
                    Ok(NetworkEvent::OutgoingConnectionError { peer_id, error }) => {
                        debug!(peer_id = ?peer_id, error = %error, "Maintenance: outgoing connection error");
                    }
                    Ok(NetworkEvent::GroupMembersUpdated { group_id, members, addresses, mesh_established }) => {
                        p2p_group_peers = members;
                        group_member_addresses = addresses
                            .into_iter()
                            .filter(|(_, addr)| !is_local_or_private_multiaddr(addr))
                            .collect();
                        connected_p2p_group_peers.retain(|p| p2p_group_peers.contains(p));
                        info!(
                            group_id = %group_id,
                            members = p2p_group_peers.len(),
                            connected = connected_p2p_group_peers.len(),
                            mesh_established,
                            "Maintenance: group members updated"
                        );
                    }
                    Ok(NetworkEvent::NewListenAddr { address }) => {
                        let mut addr = local_listen_addr.write().await;
                        *addr = address.to_string();
                        debug!(address = %address, "Maintenance: listen address updated");
                    }
                    Ok(_) => {
                        // GossipMessage, GossipSubscribed: non rilevanti per maintenance
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "Maintenance event receiver lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        warn!("Maintenance event channel closed");
                        break;
                    }
                }
            }

            // ── Connection retry timer ──────────────────────────────────
            _ = connection_retry_timer.tick() => {
                let pt_guard = priority_targets.read().await;
                let disconnected_priority: Vec<_> = pt_guard.iter()
                    .filter(|(pid, _)| !connected_priority_peers.contains(pid))
                    .map(|(pid, addr)| (*pid, addr.clone()))
                    .collect();
                drop(pt_guard);

                if disconnected_priority.is_empty() {
                    continue;
                }

                let retry_cooldown = if registration_successful {
                    Duration::from_secs(15)
                } else {
                    Duration::from_secs(30)
                };

                for (peer_id, addr) in disconnected_priority {
                    let should_retry = priority_last_attempt
                        .get(&peer_id)
                        .map_or(true, |last| last.elapsed() > retry_cooldown);

                    if should_retry {
                        if registration_successful {
                            warn!(
                                peer = %peer_id,
                                "🔄 Post-registration reconnection attempt to masternode \
                                 (needed for group selection certificates)"
                            );
                        }

                        let cmd = SwarmCommand::Dial {
                            peer_id,
                            addresses: vec![addr],
                        };
                        if let Err(e) = command_tx.send(cmd).await {
                            warn!(error = %e, "Failed to send dial command for priority peer retry");
                        } else {
                            priority_last_attempt.insert(peer_id, Instant::now());
                            info!("🔄 Retry dial command sent for masternode {}", peer_id);
                        }
                    }
                }
            }

            // ── Group peer retry timer ──────────────────────────────────
            _ = group_peer_retry_timer.tick() => {
                if group_peer_retry_skip_first_tick {
                    group_peer_retry_skip_first_tick = false;
                    continue;
                }

                let to_dial: Vec<_> = p2p_group_peers.iter()
                    .filter(|pid| !connected_p2p_group_peers.contains(pid))
                    .cloned()
                    .collect();

                if !to_dial.is_empty() {
                    let connected = connected_p2p_group_peers.len();
                    if connected > 0 {
                        info!(
                            count = to_dial.len(),
                            total_group = p2p_group_peers.len(),
                            connected,
                            "Retry dial to group peers (2s timer, maintenance task)"
                        );
                    } else {
                        debug!(
                            count = to_dial.len(),
                            total_group = p2p_group_peers.len(),
                            connected,
                            "Retry dial to group peers (other nodes may not be running)"
                        );
                    }
                }

                for peer_id in to_dial {
                    let addresses = match group_member_addresses.get(&peer_id) {
                        Some(addr) if !is_local_or_private_multiaddr(addr) => vec![addr.clone()],
                        Some(_) => {
                            debug!(
                                peer_id = %peer_id,
                                "Skipping private group peer address in maintenance retry"
                            );
                            continue;
                        }
                        None => {
                            // Fallback: try to reconstruct address from external_ip pattern.
                            // In local testnet, lightnodes listen on sequential ports starting at 5001.
                            // Extract port hint from the peer_id's last 4 bytes as a fingerprint lookup.
                            // If no address is available at all, still attempt peer_id-only dial
                            // (libp2p may resolve via Kademlia if the peer is in the routing table).
                            warn!(
                                peer_id = %peer_id,
                                "No address for group peer — attempting peer_id-only dial (Kademlia may resolve)"
                            );
                            vec![]
                        }
                    };

                    let cmd = SwarmCommand::Dial {
                        peer_id,
                        addresses,
                    };
                    if let Err(e) = command_tx.send(cmd).await {
                        warn!(error = %e, peer_id = %peer_id, "Failed to send group peer dial command");
                    } else {
                        debug!(peer_id = %peer_id, "Group peer retry dial command sent");
                    }
                }
            }

            // ── Registry announce timer ─────────────────────────────────
            _ = registry_announce_timer.tick() => {
                let listen_addr = local_listen_addr.read().await.clone();
                if listen_addr.is_empty() {
                    continue;
                }

                let full_addr = if listen_addr.contains("/p2p/") {
                    listen_addr.clone()
                } else {
                    format!("{}/p2p/{}", listen_addr, local_peer)
                };

                let announce = PeerRegistryAnnounce {
                    peer_id: local_peer.to_string(),
                    multiaddr: full_addr,
                    role: "lightnode".to_string(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    ttl_secs: 120,
                };

                if let Ok(payload) = serde_json::to_vec(&announce) {
                    let key = format!("peer_registry:{}", local_peer);
                    let cmd = SwarmCommand::KadPutRecord {
                        key,
                        value: payload,
                    };
                    if let Err(e) = command_tx.send(cmd).await {
                        debug!("Failed to send registry announce command: {}", e);
                    }
                }
            }

            // ── Peer discovery (on-demand trigger from main loop) ──────
            _ = peer_discovery_rx.recv() => {
                info!("Processing delayed peer discovery request (maintenance task)");
                // Send PeerDiscoveryRequest via gossipsub instead of broken KadGetRecord
                let request = PeerDiscoveryRequest {
                    requesting_peer: local_peer.to_string(),
                };
                if let Ok(payload) = serde_json::to_vec(&request) {
                    let cmd = SwarmCommand::Publish {
                        topic: libp2p::gossipsub::IdentTopic::new("/savitri/peer_discovery/1"),
                        payload,
                    };
                    if let Err(e) = command_tx.send(cmd).await {
                        debug!("Failed to send peer discovery request: {}", e);
                    } else {
                        info!("Sent PeerDiscoveryRequest via gossipsub to discover masternodes");
                    }
                }
            }

            // ── Periodic MN discovery (every 60s pull-based fallback) ──
            _ = mn_discovery_timer.tick() => {
                let request = PeerDiscoveryRequest {
                    requesting_peer: local_peer.to_string(),
                };
                if let Ok(payload) = serde_json::to_vec(&request) {
                    let cmd = SwarmCommand::Publish {
                        topic: libp2p::gossipsub::IdentTopic::new("/savitri/peer_discovery/1"),
                        payload,
                    };
                    if let Err(e) = command_tx.send(cmd).await {
                        debug!("Failed to send periodic MN discovery request: {}", e);
                    } else {
                        debug!("Periodic MN discovery: sent PeerDiscoveryRequest via gossipsub");
                    }
                }
            }
        }
    }

    warn!("Maintenance task exiting");
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers di serializzazione per il Publish Aggregator
// ─────────────────────────────────────────────────────────────────────────────

/// Serializza una SignedTx in due messaggi gossipsub (Tx + HaveTx).
fn serialize_signed_tx_for_gossip(
    tx: &SignedTx,
    topic: &IdentTopic,
) -> anyhow::Result<Vec<(IdentTopic, Vec<u8>)>> {
    let tx_bytes = crate::tx::serialize_signed_tx(tx)
        .map_err(|e| anyhow::anyhow!("Failed to serialize transaction: {}", e))?;
    let hash = hash_signed_tx_bytes(&tx_bytes);

    let tx_msg = GossipMessage::Tx(TxMessage {
        data: Vec::new(),
        tx: tx_bytes,
    });
    let tx_payload = encode_gossip(&tx_msg)?;

    let have_msg = GossipMessage::HaveTx(HaveTx {
        hash,
        tx_hashes: vec![hash],
        source_peer: Vec::new(), // empty = use message.source from gossipsub
    });
    let have_payload = encode_gossip(&have_msg)?;

    Ok(vec![
        (topic.clone(), tx_payload),
        (topic.clone(), have_payload),
    ])
}

/// Serializza un BlockBroadcast in due messaggi gossipsub (HaveBlock + Block).
fn serialize_block_broadcast(
    msg: &BlockBroadcast,
    block_topic: &IdentTopic,
) -> anyhow::Result<Vec<(IdentTopic, Vec<u8>)>> {
    let have_payload = encode_gossip(&GossipMessage::HaveBlock(msg.have.clone()))?;
    let block_payload = encode_gossip(&GossipMessage::Block(msg.block.clone()))?;

    Ok(vec![
        (block_topic.clone(), have_payload),
        (block_topic.clone(), block_payload),
    ])
}
