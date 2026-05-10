#![allow(dead_code)]

// Initial chain synchronization module
// Ensures lightnode is synced with masternode before starting operations

use crate::p2p::types::{
    decode_request, decode_response, MyBehaviour, MyBehaviourEvent, RequestMessage, ResponseMessage,
};
use crate::storage::BlockAndAccountStorageTrait;
use anyhow::{Context, Result};
use hex;
use libp2p::{
    futures::StreamExt,
    gossipsub::IdentTopic,
    swarm::{Swarm, SwarmEvent},
    Multiaddr, PeerId,
};
use std::{
    collections::HashSet,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time;
use tracing::{debug, info, warn};

use crate::p2p::bootstrap::{
    build_bootstrap_reply, handle_bootstrap_reply, publish_bootstrap_reply,
    publish_bootstrap_request,
};
use crate::p2p::network::is_local_or_private_multiaddr;

const SYNC_TIMEOUT: Duration = Duration::from_secs(30); // Ridotto da 120 a 30 secondi
const SYNC_POLL_INTERVAL: Duration = Duration::from_millis(500); // Ridotto da 1000ms a 500ms
const BOOTSTRAP_REPLY_TIMEOUT: Duration = Duration::from_secs(15); // Ridotto da 45 a 15 secondi

/// Perform initial bootstrap sync by waiting for masternode connection and syncing chain.
/// Returns the synced height, or None if no sync was needed.
/// This function consumes the swarm and returns it back, modifying it in-place during sync.
pub async fn initial_bootstrap_sync(
    swarm: &mut Swarm<MyBehaviour>,
    storage: Arc<dyn BlockAndAccountStorageTrait>,
    bootstrap_req_topic: &IdentTopic,
    bootstrap_resp_topic: &IdentTopic,
    priority_peers: &HashSet<PeerId>,
    priority_addresses: &std::collections::HashMap<PeerId, Multiaddr>,
) -> Result<Option<u64>> {
    // Check if we already have chain data
    let current_head = storage
        .get_chain_head()
        .context("Failed to check chain head from storage")?;

    if let Some(head_block) = current_head {
        if head_block.height > 0 {
            info!(
                height = head_block.height,
                hash = hex::encode(head_block.hash),
                "Chain already initialized; skipping bootstrap sync"
            );
            return Ok(Some(head_block.height));
        } else {
            info!(
                height = head_block.height,
                hash = hex::encode(head_block.hash),
                "Chain has only genesis block; proceeding with bootstrap sync"
            );
        }
    } else {
        info!("No chain data found; starting fresh bootstrap sync");
    }

    info!("Starting initial bootstrap sync with masternode...");

    // Wait for at least one priority peer (masternode) to connect
    let start_time = Instant::now();
    let mut connected_priority_peers: HashSet<PeerId> = swarm
        .connected_peers()
        .filter(|peer_id| priority_peers.contains(peer_id))
        .cloned()
        .collect();
    let mut last_bootstrap_request: Option<Instant> = None; // Used for retry timing
    let mut synced_height: Option<u64> = None;
    let mut connection_errors: Vec<(PeerId, String)> = Vec::new();
    let mut last_dial_attempt: std::collections::HashMap<PeerId, Instant> =
        std::collections::HashMap::new();
    let mut first_connection_time: Option<Instant> = None; // Used for bootstrap timeout
    const RETRY_INTERVAL: Duration = Duration::from_secs(3);

    if !connected_priority_peers.is_empty() {
        first_connection_time = Some(Instant::now());
        info!(
            connected_peers = connected_priority_peers.len(),
            peers = ?connected_priority_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "Detected already-connected priority masternode peers before bootstrap loop"
        );
    }

    loop {
        if start_time.elapsed() > SYNC_TIMEOUT {
            let mut error_msg = if connected_priority_peers.is_empty() {
                format!(
                    "Bootstrap sync timeout: no masternode peer connected within {:?}. ",
                    SYNC_TIMEOUT
                )
            } else {
                format!(
                    "Bootstrap sync timeout: connected to {} masternode peer(s), but no valid bootstrap reply was applied within {:?}. ",
                    connected_priority_peers.len(),
                    SYNC_TIMEOUT
                )
            };
            error_msg.push_str(&format!(
                "Attempted to connect to {} masternode peer(s): ",
                priority_peers.len()
            ));
            for peer_id in priority_peers {
                error_msg.push_str(&format!("{} ", peer_id));
            }
            if !connection_errors.is_empty() {
                error_msg.push_str(". Connection errors: ");
                for (peer_id, err) in &connection_errors {
                    error_msg.push_str(&format!("{}: {}; ", peer_id, err));
                }
            } else {
                error_msg.push_str(". No connection errors received - masternode may not be running or unreachable.");
            }
            anyhow::bail!("{}", error_msg);
        }

        // Check for new connections and bootstrap responses
        tokio::select! {
            _ = time::sleep(SYNC_POLL_INTERVAL) => {
                // Reconcile state in case connection events happened before this loop started.
                // Bootstrap must trust current swarm connectivity, not only event history.
                let currently_connected_priority: HashSet<PeerId> = swarm
                    .connected_peers()
                    .filter(|peer_id| priority_peers.contains(peer_id))
                    .cloned()
                    .collect();
                if connected_priority_peers != currently_connected_priority {
                    connected_priority_peers = currently_connected_priority;
                    info!(
                        connected_peers = connected_priority_peers.len(),
                        peers = ?connected_priority_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                        "Refreshed connected priority peer set from live swarm state"
                    );
                }
                if first_connection_time.is_none() && !connected_priority_peers.is_empty() {
                    first_connection_time = Some(Instant::now());
                }

                // Retry dialing priority peers that aren't connected
                for (peer_id, addr) in priority_addresses {
                    if !connected_priority_peers.contains(peer_id) {
                        let should_retry = last_dial_attempt
                            .get(peer_id)
                            .map(|last| last.elapsed() > RETRY_INTERVAL)
                            .unwrap_or(true);

                        if should_retry {
                            debug!(peer = %peer_id, address = %addr, "Retrying connection to priority masternode peer");
                            if let Err(err) = swarm.dial(addr.clone()) {
                                warn!(peer = %peer_id, error = ?err, "Failed to dial priority masternode peer");
                            } else {
                                debug!(peer = %peer_id, "Dial attempt initiated");
                            }
                            last_dial_attempt.insert(peer_id.clone(), Instant::now());
                        }
                    }
                }

                // Request bootstrap if we have connected priority peers but haven't received response yet
                if !connected_priority_peers.is_empty() && synced_height.is_none() {
                    // Delay bootstrap until mesh has at least 2 peers or 20s have elapsed.
                    if let Some(conn_time) = first_connection_time {
                        let mesh_peers = swarm
                            .behaviour_mut()
                            .gossipsub
                            .mesh_peers(&bootstrap_req_topic.hash())
                            .count();
                        if mesh_peers < 2 && conn_time.elapsed() < Duration::from_secs(20) {
                            debug!(
                                elapsed = ?conn_time.elapsed(),
                                mesh_peers = mesh_peers,
                                "Waiting for gossipsub mesh to stabilize (mesh_peers>=2 or 20s)"
                            );
                            continue;
                        }
                    }

                    info!(
                        connected_peers = connected_priority_peers.len(),
                        "Attempting to publish bootstrap request after connection delay"
                    );

                    // Do not treat connectivity as bootstrap success.
                    // Initial sync is complete only after a valid bootstrap reply is applied.

                    // OTTIMIZZAZIONE: Retry bootstrap request con intervalli piu brevi
                    if last_bootstrap_request.is_none()
                        || last_bootstrap_request.as_ref().map(|t| t.elapsed()).unwrap_or_else(|| Duration::from_secs(10)) > Duration::from_secs(5)  // Ridotto da 20 a 5 secondi
                    {
                        if let Err(err) = publish_bootstrap_request(swarm, bootstrap_req_topic, u64::MAX) {
                            let connected_peers: Vec<String> = swarm.connected_peers().map(|p| p.to_string()).collect();
                            let error_str = err.to_string();

                            // Gestione migliorata degli errori
                            if error_str.contains("InsufficientPeers") {
                                debug!(
                                    connected_peers = ?connected_peers,
                                    attempting_peers = ?priority_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                                    "Bootstrap request failed due to insufficient mesh peers (expected during mesh formation with mesh_n_low=1)"
                                );
                            } else {
                                warn!(
                                    error=?err,
                                    connected_peers = ?connected_peers,
                                    attempting_peers = ?priority_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                                    "Bootstrap request failed with unexpected error"
                                );
                            }
                        } else {
                            info!("Published bootstrap sync request - waiting for GossipSub mesh to propagate (mesh_n_low=1)");
                            last_bootstrap_request = Some(Instant::now());
                            if let Some(ts) = last_bootstrap_request {
                                debug!(elapsed_ms = ts.elapsed().as_millis(), "Bootstrap request timestamp recorded");
                            }
                        }
                    }
                    if let (Some(conn_time), Some(last_req)) = (first_connection_time, last_bootstrap_request) {
                        if conn_time.elapsed() > BOOTSTRAP_REPLY_TIMEOUT && last_req.elapsed() > BOOTSTRAP_REPLY_TIMEOUT {
                            warn!(
                                connected_peers = connected_priority_peers.len(),
                                waited_for_reply_secs = last_req.elapsed().as_secs(),
                                "Connected to masternode(s) but no bootstrap reply yet; continuing to retry"
                            );
                        }
                    }

                // PROTOCOL MISMATCH FALLBACK: If we are getting protocol errors,
                // keep retrying and fail timeout instead of reporting false sync success.
                // This prevents a genesis-only node from being marked as synced.
                } else if synced_height.is_none() && !connection_errors.is_empty() {
                    // Check if we have protocol/handshake errors (not network errors)
                    let protocol_errors: Vec<_> = connection_errors
                        .iter()
                        .filter(|(pid, err)| {
                            priority_peers.contains(pid) &&
                            (err.contains("Noise") || err.contains("handshake") ||
                             err.contains("protocol") || err.contains("Yamux") ||
                             err.contains("multistream"))
                        })
                        .collect();

                    if !protocol_errors.is_empty() && start_time.elapsed() > BOOTSTRAP_REPLY_TIMEOUT {
                        warn!(
                            protocol_errors = protocol_errors.len(),
                            "Masternode(s) reachable but protocol mismatch detected; waiting for valid bootstrap reply"
                        );
                    }
                }
            }
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        if priority_peers.contains(&peer_id) {
                            connected_priority_peers.insert(peer_id.clone());
                            last_dial_attempt.remove(&peer_id); // Clear retry tracking on success
                            info!(peer = %peer_id, "Connected to priority masternode peer");

                            // Track first connection time for fallback
                            if first_connection_time.is_none() {
                                first_connection_time = Some(Instant::now());
                                if let Some(ts) = first_connection_time {
                                    info!(
                                        elapsed_ms = ts.elapsed().as_millis(),
                                        "First masternode connection established - starting bootstrap reply timeout"
                                    );
                                }
                            }

                            // Initial bootstrap request intentionally disabled.
                            // We only bootstrap via the main loop after mesh stabilization
                            // (mesh>=2 or 20s), to avoid early publish failures.
                        }
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        if let Some(pid) = peer_id {
                            if priority_peers.contains(&pid) {
                                let error_str = format!("{:?}", error);
                                connection_errors.push((pid, error_str));
                                warn!(
                                    peer = %pid,
                                    error = ?error,
                                    "Failed to connect to priority masternode peer"
                                );
                            }
                        }
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        if priority_peers.contains(&peer_id) {
                            connected_priority_peers.remove(&peer_id);
                            warn!(peer = %peer_id, "Connection to priority masternode peer closed");
                        }
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub_event)) => {
                        match gossipsub_event {
                            libp2p::gossipsub::Event::Message {
                                propagation_source,
                                message,
                                ..
                            } => {
                                if message.topic == bootstrap_resp_topic.hash() {
                                    let message_author = message.source.clone();
                                    let accepted_source = message_author
                                        .clone()
                                        .filter(|source| priority_peers.contains(source))
                                        .or_else(|| {
                                            if priority_peers.contains(&propagation_source) {
                                                Some(propagation_source.clone())
                                            } else {
                                                None
                                            }
                                        });

                                    let Some(reply_source) = accepted_source else {
                                        // GossipSub can relay a valid masternode-authored message through
                                        // a non-masternode propagation peer. Prefer the signed author when
                                        // present, but reject replies with no configured masternode source.
                                        debug!(
                                            propagation_source = %propagation_source,
                                            message_author = ?message_author,
                                            "Ignoring bootstrap reply without configured masternode source"
                                        );
                                        continue;
                                    };

                                    match decode_response(&message.data) {
                                        Ok(ResponseMessage::Bootstrap(reply)) => {
                                            info!(
                                                peer = %reply_source,
                                                propagation_source = %propagation_source,
                                                blocks = reply.blocks.len(),
                                                accounts = reply.accounts.len(),
                                                peers = reply.peers.len(),
                                                "Received bootstrap reply from masternode"
                                            );

                                            // Use peer list (peer_id + addresses) to dial peers on other VMs
                                            // so Kademlia/local discovery is extended across networks
                                            const MAX_BOOTSTRAP_DIALS: usize = 30;
                                            let mut dial_count = 0usize;
                                            for peer_info in &reply.peers {
                                                if dial_count >= MAX_BOOTSTRAP_DIALS {
                                                    break;
                                                }
                                                let Ok(peer_id) = PeerId::from_str(peer_info.peer_id.trim()) else {
                                                    continue;
                                                };
                                                let addrs: Vec<Multiaddr> = peer_info
                                                    .addresses
                                                    .iter()
                                                    .filter(|a| !a.is_empty() && !a.contains("tcp/0"))
                                                    .filter_map(|a| a.parse().ok())
                                                    .filter(|addr| !is_local_or_private_multiaddr(addr))
                                                    .take(3)
                                                    .collect();
                                                if addrs.is_empty() {
                                                    continue;
                                                }
                                                let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                                                    .addresses(addrs)
                                                    .build();
                                                if swarm.dial(dial_opts).is_ok() {
                                                    dial_count += 1;
                                                    debug!(
                                                        peer_id = %peer_id,
                                                        "Dial initiated to peer from bootstrap reply (cross-VM discovery)"
                                                    );
                                                }
                                            }
                                            if dial_count > 0 {
                                                info!(
                                                    dial_count,
                                                    "Initiated dial to peers from bootstrap reply (peer_id + IP)"
                                                );
                                            }

                                            if let Ok(Some(existing)) = storage.get_chain_head() {
                                                // Calculate max height from reply blocks
                                                let reply_max_height = reply.blocks.iter()
                                                    .filter(|block| block.height != u64::MAX)
                                                    .map(|block| block.height)
                                                    .max()
                                                    .unwrap_or(0);
                                                if reply_max_height == 0 && !reply.blocks.is_empty() {
                                                    warn!(
                                                        blocks = reply.blocks.len(),
                                                        "Bootstrap reply contains only invalid block heights; ignoring"
                                                    );
                                                    continue;
                                                }

                                                if existing.height >= reply_max_height {
                                                    info!(
                                                        stored_height = existing.height,
                                                        reply_height = reply_max_height,
                                                        "Bootstrap reply already applied; skipping re-bootstrap"
                                                    );
                                                    synced_height = Some(existing.height);
                                                } else {
                                                    match handle_bootstrap_reply(storage.as_ref(), &reply, true) {
                                                        Ok(Some(height)) => {
                                                            info!(
                                                                height,
                                                                blocks = reply.blocks.len(),
                                                                accounts = reply.accounts.len(),
                                                                "Successfully synced with masternode chain"
                                                            );
                                                            synced_height = Some(height);
                                                        }
                                                        Ok(None) => {
                                                            warn!("Bootstrap reply empty; retrying...");
                                                        }
                                                        Err(err) => {
                                                            warn!(error=?err, "Failed to apply bootstrap reply; retrying...")
                                                        }
                                                    }
                                                }
                                            } else {
                                                match handle_bootstrap_reply(storage.as_ref(), &reply, true) {
                                                    Ok(Some(height)) => {
                                                        info!(
                                                            height,
                                                            blocks = reply.blocks.len(),
                                                            accounts = reply.accounts.len(),
                                                            "Successfully synced with masternode chain"
                                                        );
                                                        synced_height = Some(height);
                                                    }
                                                    Ok(None) => {
                                                        warn!("Bootstrap reply empty; retrying...");
                                                    }
                                                    Err(err) => {
                                                        warn!(error=?err, "Failed to apply bootstrap reply; retrying...")
                                                    }
                                                }
                                            }
                                        }
                                        Ok(other) => {
                                            warn!(?other, "unexpected bootstrap response payload");
                                        }
                                        Err(err) => {
                                            warn!(error=?err, "failed to decode bootstrap response");
                                        }
                                    }
                                } else if message.topic == bootstrap_req_topic.hash() {
                                    // Handle incoming bootstrap requests (we may be acting as a relay)
                                    if let Ok(RequestMessage::Bootstrap(req)) = decode_request(&message.data) {
                                        if let Err(err) = req.validate() {
                                            warn!(error=?err, "invalid bootstrap request");
                                            continue;
                                        }
                                        let reply = build_bootstrap_reply(
                                            storage.as_ref(),
                                            req.end_height,
                                            req.max_blocks as usize,
                                        );
                                        if let Err(err) = publish_bootstrap_reply(
                                            swarm,
                                            bootstrap_resp_topic,
                                            &reply,
                                        ) {
                                            let connected_peers: Vec<String> = swarm.connected_peers().map(|p| p.to_string()).collect();
                                            warn!(
                                                error=?err,
                                                connected_peers = ?connected_peers,
                                                attempting_peers = ?priority_peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                                                "failed to publish bootstrap reply"
                                            );
                                        } else {
                                            info!("Published bootstrap reply");
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Ignore other topics
                            }
                        }
                    }
                    _ => {
                        // Ignore other events
                    }
                }
            }
        }

        // Check if we've successfully synced
        if let Some(height) = synced_height {
            // Verify the sync persisted by checking storage
            return match storage.get_chain_head() {
                Ok(Some(head_block)) => {
                    let stored_height = head_block.height;
                    if stored_height >= height {
                        info!(
                            synced_height = height,
                            stored_height = stored_height,
                            block_hash = hex::encode(head_block.hash),
                            "Chain sync completed and verified"
                        );
                        Ok(Some(stored_height))
                    } else {
                        warn!(
                            expected_height = height,
                            stored_height = stored_height,
                            "Sync height mismatch, continuing sync"
                        );
                        Ok(None)
                    }
                }
                Ok(None) => {
                    warn!("No chain head found in storage after sync, continuing");
                    Ok(None)
                }
                Err(e) => {
                    warn!(error = %e, "Failed to verify sync from storage");
                    Ok(None)
                }
            };
        }

        // Continue loop until synced_height is set or timeout at top of loop
    }
}
