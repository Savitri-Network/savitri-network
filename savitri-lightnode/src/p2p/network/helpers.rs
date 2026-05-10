//! Helper functions for the P2P network module.
//! Contains TX deserialization, monolith verification, bootstrap snapshot requests,
//! and other utility functions used by the main event loop.

use anyhow::Result;
use bincode::Options;
use tracing::{info, warn, debug};
use std::collections::HashMap;
use libp2p::swarm::Swarm;

use crate::resource::FixedPoint;
use crate::availability::PouScoring;
use crate::p2p::types::PouBroadcast;
use crate::p2p::swarm_commands::SwarmCommand;
use super::is_local_or_private_multiaddr;

/// Maximum allowed size for network transaction deserialization (1 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized network payloads.
pub(crate) const MAX_NETWORK_TX_SIZE: usize = 1 * 1024 * 1024;

pub(crate) fn bytes_to_raw_tx(bytes: Vec<u8>, _peer_id: Option<u64>) -> Result<crate::tx::SignedTx> {
    if bytes.len() > MAX_NETWORK_TX_SIZE {
        anyhow::bail!(
            "Network transaction data too large: {} bytes (max {})",
            bytes.len(),
            MAX_NETWORK_TX_SIZE
        );
    }
    // Canonical format: TransactionExt with fixint encoding
    if let Ok(tx_ext) = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_NETWORK_TX_SIZE as u64)
        .deserialize::<crate::tx::TransactionExt>(&bytes)
    {
        if !tx_ext.pre_verified {
            let verified = crate::tx::verify_transaction_signature_ext(&tx_ext);
            if verified {
                Ok(crate::tx::TransactionExt {
                    from: tx_ext.from,
                    to: tx_ext.to,
                    amount: tx_ext.amount,
                    nonce: tx_ext.nonce,
                    fee: tx_ext.fee,
                    data: tx_ext.data,
                    pubkey: tx_ext.pubkey,
                    sig: tx_ext.sig,
                    pre_verified: true,
                })
            } else {
                Ok(tx_ext) // Mantiene pre_verified=false se la check fallisce
            }
        } else {
            Ok(tx_ext)
        }
    } else {
        let core_tx = crate::core::tx::deserialize_signed_tx(&bytes)?;

        Ok(crate::tx::TransactionExt {
            from: hex::encode(&core_tx.from),
            to: hex::encode(&core_tx.to),
            amount: core_tx.amount as u64,
            nonce: core_tx.nonce,
            fee: core_tx.fee,
            data: None,
            pubkey: core_tx.pubkey,
            sig: core_tx.sig,
            pre_verified: core_tx.pre_verified,
        })
    }
}

// Helper functions
pub(crate) fn compute_remote_pou(
    report: &PouBroadcast,
    node_id: [u8; 32],
    scoring: &PouScoring,
    histories: &mut HashMap<[u8; 32], FixedPoint>,
    prev_scores: &mut HashMap<[u8; 32], FixedPoint>,
) -> Option<u16> {
    let prev_score = prev_scores.get(&node_id).copied().unwrap_or(FixedPoint::from_raw_u32(0));

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

pub(crate) fn matches_digest_prefix(candidate: &[u8], sha256: &[u8], sha512: &[u8], min_len: usize) -> bool {
    if candidate.len() < min_len {
        return false;
    }
    let matches_256 = candidate.len() <= sha256.len() && candidate == &sha256[..candidate.len()];
    let matches_512 = candidate.len() <= sha512.len() && candidate == &sha512[..candidate.len()];
    matches_256 || matches_512
}

pub(crate) async fn verify_monolith_data(monolith_id: &[u8], monolith_hash: &[u8], monolith_data: &[u8]) -> bool {
    if monolith_id.is_empty() || monolith_data.is_empty() {
        return false;
    }

    const MAX_MONOLITH_SIZE: usize = 10 * 1024 * 1024;
    if monolith_data.len() > MAX_MONOLITH_SIZE {
        warn!("Monolith data too large: {} bytes (max: {})", monolith_data.len(), MAX_MONOLITH_SIZE);
        return false;
    }

    use sha2::{Digest, Sha256, Sha512};
    let sha256_hash = Sha256::digest(monolith_data);
    let sha512_hash = Sha512::digest(monolith_data);

    if monolith_id.len() < 8 {
        warn!("Monolith ID too short: {} bytes (min: 8)", monolith_id.len());
        return false;
    }

    // Compatibility: accept both SHA-256 and SHA-512 prefixes/lengths so
    // masternode/lightnode hash-schema differences do not break verification.
    let id_ok = matches_digest_prefix(monolith_id, sha256_hash.as_slice(), sha512_hash.as_slice(), 8);
    if !id_ok {
        warn!(
            monolith_id_len = monolith_id.len(),
            "Monolith ID does not match SHA-256/SHA-512 digest prefix"
        );
        return false;
    }

    if !monolith_hash.is_empty() {
        let hash_ok = matches_digest_prefix(monolith_hash, sha256_hash.as_slice(), sha512_hash.as_slice(), 8);
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

pub(crate) fn compute_monolith_hash(monolith_data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use sha2::{Sha256, Digest};
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
pub(crate) fn execute_swarm_command(
    swarm: &mut Swarm<crate::p2p::types::MyBehaviour>,
    cmd: SwarmCommand,
) {
    match cmd {
        SwarmCommand::Publish { topic, payload } => {
            // Diagnostica: conta i peer noti per il topic (mesh + explicit)
            let mesh_peers = swarm.behaviour().gossipsub.mesh_peers(&topic.hash()).count();
            let all_peers = swarm.behaviour().gossipsub.all_peers().count();

            // DIAGNOSTIC: enumerate peers_on_topic for critical topics
            let topic_str_diag = topic.to_string();
            if topic_str_diag.contains("proposal") || topic_str_diag.contains("election") {
                let peers_with_topic: Vec<_> = swarm.behaviour().gossipsub.all_peers()
                    .filter(|(_, topics)| topics.iter().any(|t| **t == topic.hash()))
                    .map(|(pid, topics)| format!("{}({}topics)", pid, topics.len()))
                    .collect();
                let peers_without_topic: Vec<_> = swarm.behaviour().gossipsub.all_peers()
                    .filter(|(_, topics)| !topics.iter().any(|t| **t == topic.hash()))
                    .map(|(pid, topics)| format!("{}({}topics)", pid, topics.len()))
                    .collect();
                let mesh_peer_ids: Vec<_> = swarm.behaviour().gossipsub.mesh_peers(&topic.hash())
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

            match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
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
                match swarm.behaviour_mut().gossipsub.publish(topic.clone(), payload) {
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
            swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
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
                crate::p2p::consensus_protocol::ConsensusMessage::ElectionResult(_) => "election_result",
                crate::p2p::consensus_protocol::ConsensusMessage::Latency(_) => "latency",
                crate::p2p::consensus_protocol::ConsensusMessage::LatencyResponse(_) => "latency_response",
                crate::p2p::consensus_protocol::ConsensusMessage::PoU(_) => "pou",
                crate::p2p::consensus_protocol::ConsensusMessage::PoUAck(_) => "pou_ack",
            };
            let req_id = swarm.behaviour_mut().consensus.send_request(&peer_id, message);
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
                crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryRequest(_) => "peer_discovery_req",
                crate::p2p::aux_protocol::AuxMessage::PeerDiscoveryResponse(_) => "peer_discovery_resp",
                crate::p2p::aux_protocol::AuxMessage::PeerRegistry(_) => "peer_registry",
                crate::p2p::aux_protocol::AuxMessage::BlockSync(_) => "block_sync",
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
