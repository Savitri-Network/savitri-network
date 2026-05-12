#![allow(dead_code)]

use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::p2p::types::{Hash64, MyBehaviour};
use crate::{
    logging::{flagged_message, FLAG_MASTERNODE},
    resource::{emit_event, ResourceEvent, TrafficDirection},
};
use libp2p::swarm::Swarm;

/// Convert Hash64 to a 64-byte array.
pub fn hash64_to_array(hash: &Hash64) -> [u8; 64] {
    *hash.as_bytes()
}

/// Get current Unix timestamp in seconds.
pub fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Calculate total voters (lightnode peers + local node, excluding masternode peers).
pub fn total_voters(
    peers: &HashMap<PeerId, [u8; 32]>,
    masternode_peers: &std::collections::HashSet<PeerId>,
) -> usize {
    let lightnode_count = peers
        .keys()
        .filter(|peer_id| !masternode_peers.contains(peer_id))
        .count();
    lightnode_count + 1 // +1 for local node
}

/// Attempt to redial a priority peer if connection fails.
pub fn maybe_redial_priority(
    swarm: &mut Swarm<MyBehaviour>,
    priority_targets: &HashMap<PeerId, Multiaddr>,
    priority_last_attempt: &mut HashMap<PeerId, Instant>,
    peer_id: &PeerId,
    reason: &str,
) {
    if let Some(addr) = priority_targets.get(peer_id) {
        let allow = priority_last_attempt
            .get(peer_id)
            .map(|stamp| stamp.elapsed() > Duration::from_secs(5))
            .unwrap_or(true);
        if !allow {
            return;
        }
        if !is_publicly_dialable_multiaddr(addr) {
            warn!(
                peer = %peer_id,
                address = %addr,
                "skipping re-dial for priority masternode: address is private or unroutable"
            );
            priority_last_attempt.insert(peer_id.clone(), Instant::now());
            return;
        }
        let target_addr = addr.clone();
        match swarm.dial(target_addr.clone()) {
            Ok(_) => {
                tracing::info!(
                    peer = %peer_id,
                    address = %target_addr,
                    "{}",
                    flagged_message(
                        FLAG_MASTERNODE,
                        format!("Re-dialing priority masternode peer ({reason})")
                    )
                );
            }
            Err(err) => {
                warn!(
                    peer = %peer_id,
                    error = ?err,
                    "failed to dial priority masternode peer"
                );
            }
        }
        priority_last_attempt.insert(peer_id.clone(), Instant::now());
    }
}

fn is_publicly_dialable_multiaddr(addr: &Multiaddr) -> bool {
    let mut saw_ip = false;
    for proto in addr.iter() {
        match proto {
            Protocol::Ip4(ip) => {
                saw_ip = true;
                if ip.is_loopback() || ip.is_private() || ip.is_unspecified() || ip.is_link_local()
                {
                    return false;
                }
            }
            Protocol::Ip6(ip) => {
                saw_ip = true;
                if ip.is_loopback()
                    || ip.is_unspecified()
                    || ip.is_unicast_link_local()
                    || ip.is_unique_local()
                {
                    return false;
                }
            }
            Protocol::Tcp(0) | Protocol::Udp(0) => return false,
            _ => {}
        }
    }

    saw_ip
}

/// Update the shared peer accounts directory.
pub async fn update_peer_directory(
    shared: &Arc<RwLock<Vec<[u8; 32]>>>,
    registry: &HashMap<PeerId, [u8; 32]>,
) {
    let mut accounts: Vec<[u8; 32]> = registry.values().copied().collect();
    accounts.sort();
    accounts.dedup();
    let mut guard = shared.write().await;
    *guard = accounts;
}

/// Emit a resource event if the sender is available.
pub fn emit_resource_event(
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
    direction: TrafficDirection,
    bytes: usize,
) {
    emit_event(resource_events, ResourceEvent::Gossip { direction, bytes });
}
