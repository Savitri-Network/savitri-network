use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use rand::seq::SliceRandom;

use super::wire::PeerRecord;

const REQUIRED_PROTOCOLS: &[&str] = &[
    "/savitri/1.0.0",
    crate::p2p::aux_protocol::AUX_PROTOCOL,
    crate::p2p::consensus_protocol::CONSENSUS_PROTOCOL,
    crate::p2p::tx_fetch_protocol::TX_FETCH_PROTOCOL,
];

#[derive(Debug, Clone)]
pub struct DialCandidate {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
}

#[derive(Debug)]
pub struct DialSelectionState {
    connected: HashSet<PeerId>,
    pending: HashMap<PeerId, Instant>,
    cooldowns: HashMap<PeerId, Instant>,
    pending_timeout: Duration,
    failure_cooldown: Duration,
}

impl DialSelectionState {
    pub fn new(pending_timeout: Duration, failure_cooldown: Duration) -> Self {
        Self {
            connected: HashSet::new(),
            pending: HashMap::new(),
            cooldowns: HashMap::new(),
            pending_timeout,
            failure_cooldown,
        }
    }

    pub fn mark_connected(&mut self, peer_id: PeerId) {
        self.connected.insert(peer_id);
        self.pending.remove(&peer_id);
        self.cooldowns.remove(&peer_id);
    }

    pub fn mark_disconnected(&mut self, peer_id: &PeerId) {
        self.connected.remove(peer_id);
    }

    pub fn mark_pending(&mut self, peer_id: PeerId, now: Instant) {
        self.pending.insert(peer_id, now);
    }

    pub fn mark_failed(&mut self, peer_id: PeerId, now: Instant) {
        self.pending.remove(&peer_id);
        self.cooldowns.insert(peer_id, now + self.failure_cooldown);
    }

    pub fn cleanup(&mut self, now: Instant) {
        self.pending
            .retain(|_, started_at| now.duration_since(*started_at) < self.pending_timeout);
        self.cooldowns.retain(|_, until| *until > now);
    }

    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.connected.contains(peer_id)
    }

    pub fn is_pending(&self, peer_id: &PeerId) -> bool {
        self.pending.contains_key(peer_id)
    }

    pub fn is_in_cooldown(&self, peer_id: &PeerId) -> bool {
        self.cooldowns.contains_key(peer_id)
    }
}

fn is_private_or_loopback(addr: &Multiaddr) -> bool {
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(ip) => {
                return ip.is_private()
                    || ip.is_loopback()
                    || ip.is_link_local()
                    || ip.is_broadcast()
                    || matches!(
                        ip.octets(),
                        [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
                    )
            }
            Protocol::Ip6(ip) => {
                return ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.is_unicast_link_local()
                    || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
            }
            _ => {}
        }
    }
    false
}

fn protocol_compatible(record: &PeerRecord) -> bool {
    if record.supported_protocols.is_empty() {
        return true;
    }
    record.supported_protocols.iter().any(|protocol| {
        REQUIRED_PROTOCOLS
            .iter()
            .any(|required| protocol == required || protocol.starts_with("/savitri/"))
    })
}

fn sanitize_addresses(record: &PeerRecord) -> Vec<Multiaddr> {
    let mut addresses: Vec<Multiaddr> = record
        .listen_addresses
        .iter()
        .filter_map(|addr| addr.parse::<Multiaddr>().ok())
        .filter(|addr| {
            !addr
                .iter()
                .any(|protocol| matches!(protocol, Protocol::P2p(_)))
                && !is_private_or_loopback(addr)
        })
        .collect();
    addresses.sort_by_key(|addr| {
        let public_score = if is_private_or_loopback(addr) { 0 } else { 1 };
        let quic_score = if addr
            .iter()
            .any(|protocol| matches!(protocol, Protocol::QuicV1))
        {
            1
        } else {
            0
        };
        std::cmp::Reverse((public_score, quic_score, addr.to_string()))
    });
    addresses.dedup();
    addresses
}

pub fn select_candidates(
    peers: &[PeerRecord],
    state: &mut DialSelectionState,
    local_peer_id: &PeerId,
    expected_network_id: &str,
    limit: usize,
    now: Instant,
) -> Vec<DialCandidate> {
    state.cleanup(now);

    let mut shuffled = peers.to_vec();
    shuffled.shuffle(&mut rand::thread_rng());

    shuffled
        .into_iter()
        .filter_map(|record| {
            let peer_id = record.peer_id.parse::<PeerId>().ok()?;
            if &peer_id == local_peer_id {
                return None;
            }
            if record
                .network_id
                .as_deref()
                .map(|network_id| network_id != expected_network_id)
                .unwrap_or(false)
            {
                return None;
            }
            if state.is_connected(&peer_id)
                || state.is_pending(&peer_id)
                || state.is_in_cooldown(&peer_id)
            {
                return None;
            }
            if !protocol_compatible(&record) {
                return None;
            }
            let addresses = sanitize_addresses(&record);
            if addresses.is_empty() {
                return None;
            }
            Some(DialCandidate { peer_id, addresses })
        })
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use libp2p::PeerId;

    use super::{select_candidates, DialSelectionState};
    use crate::peer_server::wire::PeerRecord;

    fn peer_record(peer_id: &PeerId, addresses: &[&str]) -> PeerRecord {
        PeerRecord {
            peer_id: peer_id.to_string(),
            network_id: Some("testnet".to_string()),
            listen_addresses: addresses.iter().map(|addr| (*addr).to_string()).collect(),
            supported_protocols: vec!["/savitri/1.0.0".to_string()],
            roles: vec!["lightnode".to_string()],
            agent_version: None,
            build_version: None,
            rpc_endpoint: None,
            last_seen: None,
        }
    }

    #[test]
    fn does_not_dial_self_or_pending_or_connected_peers() {
        let local_peer = PeerId::random();
        let other_peer = PeerId::random();
        let connected_peer = PeerId::random();
        let mut state = DialSelectionState::new(Duration::from_secs(30), Duration::from_secs(60));
        let now = Instant::now();
        state.mark_pending(other_peer, now);
        state.mark_connected(connected_peer);

        let peers = vec![
            peer_record(&local_peer, &["/ip4/203.0.113.10/tcp/4001"]),
            peer_record(&other_peer, &["/ip4/203.0.113.11/tcp/4001"]),
            peer_record(&connected_peer, &["/ip4/203.0.113.12/tcp/4001"]),
        ];

        let selected = select_candidates(&peers, &mut state, &local_peer, "testnet", 10, now);
        assert!(selected.is_empty());
    }

    #[test]
    fn applies_failure_cooldown_then_releases_peer() {
        let local_peer = PeerId::random();
        let remote_peer = PeerId::random();
        let mut state = DialSelectionState::new(Duration::from_secs(30), Duration::from_secs(60));
        let now = Instant::now();
        state.mark_failed(remote_peer, now);

        let peers = vec![peer_record(&remote_peer, &["/ip4/203.0.113.11/tcp/4001"])];
        let selected = select_candidates(&peers, &mut state, &local_peer, "testnet", 10, now);
        assert!(selected.is_empty());

        let selected_after = select_candidates(
            &peers,
            &mut state,
            &local_peer,
            "testnet",
            10,
            now + Duration::from_secs(61),
        );
        assert_eq!(selected_after.len(), 1);
    }

    #[test]
    fn rejects_wrong_network_and_invalid_addresses() {
        let local_peer = PeerId::random();
        let remote_peer = PeerId::random();
        let mut state = DialSelectionState::new(Duration::from_secs(30), Duration::from_secs(60));
        let now = Instant::now();
        let mut record = peer_record(&remote_peer, &["not-a-multiaddr"]);
        record.network_id = Some("wrongnet".to_string());

        let selected = select_candidates(&[record], &mut state, &local_peer, "testnet", 10, now);
        assert!(selected.is_empty());
    }
}
