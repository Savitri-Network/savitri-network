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

#[cfg(test)]
mod tests {
    use super::is_publicly_dialable_multiaddr;
    use libp2p::Multiaddr;

    #[test]
    fn rejects_rfc1918_ipv4_addresses() {
        let docker_addr: Multiaddr = "/ip4/172.17.0.1/tcp/4001".parse().unwrap();
        assert!(!is_publicly_dialable_multiaddr(&docker_addr));

        let lan_addr: Multiaddr = "/ip4/192.168.10.3/tcp/4001".parse().unwrap();
        assert!(!is_publicly_dialable_multiaddr(&lan_addr));
    }

    #[test]
    fn accepts_public_ipv4_addresses() {
        let public_addr: Multiaddr = "/ip4/198.51.100.10/udp/4001/quic-v1".parse().unwrap();
        assert!(is_publicly_dialable_multiaddr(&public_addr));
    }
}

/// Raccoglie i dati P2P (PoU scores e account) per la distribuzione dei fee.
///
/// 3. Esclude il masternode dalla list
/// 4. Converte i PoU scores (0-1000) a u64 (0-1000)
///
/// # Parametri
/// - `pou_state`: Stato PoU che contiene i scores dei peer
/// - `peer_accounts`: Directory degli account dei peer (PeerId -> account)
///
/// # Ritorna
pub async fn collect_p2p_nodes_for_fee_distribution(
    pou_state: &crate::p2p::pou::PouState,
    peer_accounts: &Arc<RwLock<Vec<[u8; 32]>>>,
    known_peer_accounts: &HashMap<PeerId, [u8; 32]>,
    masternode_address: Option<&[u8; 32]>,
) -> Option<Vec<([u8; 32], u64)>> {
    let peer_scores = pou_state.get_all_peer_scores().await;

    if peer_scores.is_empty() {
        debug!("No PoU scores available for P2P fee distribution");
        return None;
    }

    // Ottieni la directory degli account dei peer
    let _accounts_guard = peer_accounts.read().await;

    let mut p2p_nodes = Vec::new();

    for (peer_id, score) in peer_scores.iter() {
        // Trova l'account of the peer
        let account = known_peer_accounts.get(peer_id).copied();

        if let Some(account_address) = account {
            // Escludi il masternode se specificato
            if let Some(masternode_addr) = masternode_address {
                if account_address == *masternode_addr {
                    continue; // Escludi masternode
                }
            }

            // Converti PoU score (0-1000) a u64 (0-1000)
            let pou_score_u64 = u64::from(*score);

            p2p_nodes.push((account_address, pou_score_u64));
        } else {
            debug!(
                peer = %peer_id,
                "Peer has PoU score but no account address found, skipping for fee distribution"
            );
        }
    }

    if p2p_nodes.is_empty() {
        debug!("No P2P nodes with both PoU scores and account addresses found");
        return None;
    }

    debug!(
        p2p_nodes_count = p2p_nodes.len(),
        "Collected P2P nodes for fee distribution"
    );

    Some(p2p_nodes)
}
