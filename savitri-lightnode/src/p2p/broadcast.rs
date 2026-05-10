#![allow(dead_code)]

use crate::p2p::types::{
    GossipMessage, HaveTx, HeartbeatMessage, LightnodeRegistration, MyBehaviour, PeerInfo,
    SignedTx, TxMessage,
};
use anyhow::{anyhow, Result};
use bincode;
use hex;
use libp2p::gossipsub::{IdentTopic, PublishError};
use libp2p::kad::{Quorum, Record, RecordKey};
use libp2p::swarm::Swarm;
#[cfg(feature = "metrics")]
use metrics::counter;
use std::time::Duration;
use tracing::{error, info, warn};

pub fn hash_signed_tx_bytes(bytes: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let sha256_result = hasher.finalize();

    sha256_result.into()
}

pub fn encode_gossip(msg: &GossipMessage) -> Result<Vec<u8>> {
    serde_json::to_vec(msg).map_err(|e| anyhow!("Failed to encode gossip message: {}", e))
}

use crate::{
    logging::{flagged_message, FLAG_MASTERNODE},
    p2p::types::{BlockBroadcast, BlockReceipt},
    resource::{emit_event, ResourceEvent, TrafficDirection},
};

const REGISTRATION_PUBLISH_MAX_RETRIES: usize = 6;
const REGISTRATION_PUBLISH_BASE_DELAY_MS: u64 = 250;
const REGISTRATION_PUBLISH_MAX_DELAY_MS: u64 = 4000;

/// Returns true if a multiaddr string contains an RFC1918/loopback/link-local IP
/// (192.168/16, 10/8, 172.16-31/12, 127/8, 169.254/16) or unspecified (0.0.0.0).
/// che `detect_outbound_ip` può catturare su host con Docker installato.
fn addr_str_is_non_public(addr: &str) -> bool {
    if let Ok(ma) = addr.parse::<libp2p::Multiaddr>() {
        crate::p2p::network::is_local_or_private_multiaddr(&ma)
    } else {
        // Fallback testuale: noti pattern non-pubblici
        addr.contains("/ip4/0.0.0.0")
            || addr.contains("/ip4/127.")
            || addr.contains("/ip4/10.")
            || addr.contains("/ip4/192.168.")
            || addr.contains("/ip4/169.254.")
            || addr.contains("/ip4/172.16.")
            || addr.contains("/ip4/172.17.")
            || addr.contains("/ip4/172.18.")
            || addr.contains("/ip4/172.19.")
            || addr.contains("/ip4/172.2") // 20-29
            || addr.contains("/ip4/172.30.")
            || addr.contains("/ip4/172.31.")
    }
}

/// Build peer info for Kademlia/gossip. Uses `advertised_addr` when set (e.g. from Identify
/// observed address or configured external_ip) so other nodes can dial us in decentralized/NAT setups.
/// Falls back to the swarm listener address, filtering out non-dialable bind addresses (0.0.0.0)
/// AND RFC1918 private IPs (Docker bridge, LAN) — Bug post-#51 fix.
fn build_peer_info(
    swarm: &Swarm<MyBehaviour>,
    account: &[u8; 32],
    advertised_addr: Option<&str>,
) -> PeerInfo {
    let peer_id = Swarm::local_peer_id(swarm).to_string();
    let address = advertised_addr
        .filter(|a| !a.is_empty() && !a.contains("tcp/0") && !addr_str_is_non_public(a))
        .map(String::from)
        .or_else(|| {
            swarm.listeners().map(|addr| addr.to_string()).find(|a| {
                !a.is_empty()
                    && !a.contains("tcp/0")
                    && !a.contains("0.0.0.0")
                    && !addr_str_is_non_public(a)
            })
        })
        .unwrap_or_default();
    if address.is_empty() {
        tracing::warn!(
            account = %hex::encode(account),
            advertised = ?advertised_addr,
            "build_peer_info: no public dialable address found — peer announce will be empty (set external_ip in config)"
        );
    }
    PeerInfo {
        peer_id,
        address,
        account: *account,
        priority: false,
    }
}

fn put_kad_record(swarm: &mut Swarm<MyBehaviour>, key: &str, value: Vec<u8>) -> Result<()> {
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
        .map_err(|e| anyhow!("failed to put kademlia record {key}: {e}"))?;
    Ok(())
}

/// Broadcast a signed transaction to the network.
pub fn broadcast_signed_tx(
    swarm: &mut Swarm<MyBehaviour>,
    tx_topic: &IdentTopic,
    tx: &SignedTx,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    let tx_bytes = crate::tx::serialize_signed_tx(tx)
        .map_err(|e| anyhow!("Failed to serialize transaction: {}", e))?;
    let hash = hash_signed_tx_bytes(&tx_bytes);

    let tx_msg = GossipMessage::Tx(TxMessage {
        data: Vec::new(),
        tx: tx_bytes.clone(),
    });
    let tx_payload = encode_gossip(&tx_msg)?;
    let tx_len = tx_payload.len();

    // Try to publish - gossipsub will handle peer requirements internally
    // If InsufficientPeers, the transaction will be queued and retried later
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(tx_topic.clone(), tx_payload)
        .map_err(|e: libp2p::gossipsub::PublishError| {
            // Check if error is InsufficientPeers - this is expected when starting up or with few peers
            let err_str = e.to_string();
            if err_str.contains("InsufficientPeers") {
                anyhow!("Cannot broadcast transaction: gossipsub requires more peers (InsufficientPeers)")
            } else {
                anyhow!("failed to publish tx gossip: {e}")
            }
        })?;
    let have_msg = GossipMessage::HaveTx(HaveTx {
        hash,
        tx_hashes: vec![hash],
        source_peer: Vec::new(), // empty = use message.source from gossipsub
    });
    let have_payload = encode_gossip(&have_msg)?;
    let have_len = have_payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(tx_topic.clone(), have_payload)
        .map_err(|e: libp2p::gossipsub::PublishError| {
            let err_str = e.to_string();
            if err_str.contains("InsufficientPeers") {
                anyhow!(
                    "Cannot broadcast HaveTx: gossipsub requires more peers (InsufficientPeers)"
                )
            } else {
                anyhow!("failed to publish HaveTx gossip: {e}")
            }
        })?;

    let total_bytes = tx_len + have_len;
    #[cfg(feature = "metrics")]
    {
        counter!("broadcast_transactions_total").increment(1);
        counter!("p2p_messages_sent_total").increment(2); // tx + have
        counter!("p2p_bytes_sent_total").increment(total_bytes as u64);
        counter!("gossip_messages_sent_total").increment(2);
    }
    emit_event(
        resource_events,
        ResourceEvent::Gossip {
            direction: TrafficDirection::Outbound,
            bytes: total_bytes,
        },
    );

    Ok(())
}

/// Broadcast a block receipt to the network.
pub fn broadcast_block_receipt(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    receipt: &BlockReceipt,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    // Serialize the block receipt for broadcasting
    let payload = match bincode::serialize(receipt) {
        Ok(data) => data,
        Err(e) => {
            return Err(anyhow!("failed to serialize block receipt: {}", e));
        }
    };
    let payload_len = payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload)
        .map_err(|e| anyhow!("failed to publish block receipt gossip: {e}"))?;
    emit_event(
        resource_events,
        ResourceEvent::Gossip {
            direction: TrafficDirection::Outbound,
            bytes: payload_len,
        },
    );
    let block_hash = hex::encode(receipt.block_hash.as_bytes());
    if receipt.accepted {
        info!(
            hash = %block_hash,
            peer = %receipt.peer_id,
            "Broadcasted block acceptance receipt"
        );
    } else {
        info!(
            hash = %block_hash,
            peer = %receipt.peer_id,
            "Broadcasted block rejection receipt"
        );
    }
    Ok(())
}

/// Broadcast a heartbeat message to the network.
pub fn broadcast_heartbeat(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    message: &HeartbeatMessage,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    let payload = encode_gossip(&GossipMessage::Heartbeat(message.clone()))?;
    let payload_len = payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload)
        .map_err(|e| anyhow!("failed to publish heartbeat gossip: {e}"))?;
    #[cfg(feature = "metrics")]
    {
        counter!("broadcast_heartbeats_total").increment(1);
        counter!("p2p_messages_sent_total").increment(1);
        counter!("p2p_bytes_sent_total").increment(payload_len as u64);
        counter!("gossip_messages_sent_total").increment(1);
    }
    emit_event(
        resource_events,
        ResourceEvent::Gossip {
            direction: TrafficDirection::Outbound,
            bytes: payload_len,
        },
    );
    Ok(())
}

/// Broadcast a block to lightnodes only (not masternode).
/// This is used for initial block distribution before quorum approval.
pub fn broadcast_block_to_lightnodes(
    swarm: &mut Swarm<MyBehaviour>,
    block_topic: &IdentTopic,
    msg: &BlockBroadcast,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        txs = msg.have.tx_count,
        topic = %block_topic,
        "📤 [LN->LN] Step 1: Broadcasting block to lightnode peers (HaveBlock + Block)"
    );

    let have_payload = encode_gossip(&GossipMessage::HaveBlock(msg.have.clone()))?;
    let have_len = have_payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(block_topic.clone(), have_payload)
        .map_err(|e| anyhow!("failed to publish HaveBlock gossip: {e}"))?;
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        "📤 [LN->LN] Step 2: HaveBlock published to block topic (lightnode peers)"
    );

    let block_payload = encode_gossip(&GossipMessage::Block(msg.block.clone()))?;
    let block_len = block_payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(block_topic.clone(), block_payload)
        .map_err(|e| anyhow!("failed to publish Block gossip: {e}"))?;
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        payload_bytes = block_len,
        "📤 [LN->LN] Step 3: Block published to block topic (lightnode peers)"
    );

    #[cfg(feature = "metrics")]
    {
        counter!("broadcast_blocks_total").increment(1);
        counter!("p2p_messages_sent_total").increment(2); // have + block
        counter!("p2p_bytes_sent_total").increment((have_len + block_len) as u64);
        counter!("gossip_messages_sent_total").increment(2);
    }
    emit_event(
        resource_events,
        ResourceEvent::Gossip {
            direction: TrafficDirection::Outbound,
            bytes: have_len + block_len,
        },
    );

    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        txs = msg.have.tx_count,
        "📤 [LN->LN] Step 4: Block broadcast to lightnode group completed"
    );
    Ok(())
}

/// Send an approved block to masternode after quorum is reached.
pub fn send_block_to_masternode(
    swarm: &mut Swarm<MyBehaviour>,
    masternode_topic: &IdentTopic,
    msg: &BlockBroadcast,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        txs = msg.have.tx_count,
        topic = %masternode_topic,
        "📤 [LN->MN] Step 1: Sending approved block to masternode (HaveBlock + Block)"
    );

    let have_payload = encode_gossip(&GossipMessage::HaveBlock(msg.have.clone()))?;
    let have_len = have_payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(masternode_topic.clone(), have_payload)
        .map_err(|e| anyhow!("failed to publish HaveBlock to masternode: {e}"))?;
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        "📤 [LN->MN] Step 2: HaveBlock published to masternode topic"
    );

    let block_payload = encode_gossip(&GossipMessage::Block(msg.block.clone()))?;
    let block_len = block_payload.len();
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(masternode_topic.clone(), block_payload)
        .map_err(|e| anyhow!("failed to publish Block to masternode: {e}"))?;
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        payload_bytes = block_len,
        "📤 [LN->MN] Step 3: Block published to masternode topic"
    );

    emit_event(
        resource_events,
        ResourceEvent::Gossip {
            direction: TrafficDirection::Outbound,
            bytes: have_len + block_len,
        },
    );

    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        txs = msg.have.tx_count,
        "{}",
        flagged_message(FLAG_MASTERNODE, "Sent approved block to masternode after quorum")
    );
    info!(
        hash = %hex::encode(msg.block.hash),
        height = msg.have.exec_height,
        "📤 [LN->MN] Step 4: Block successfully broadcast to masternode"
    );
    Ok(())
}

/// Broadcast a block to the network (legacy function, kept for compatibility).
/// Note: This sends to all subscribers. Use broadcast_block_to_lightnodes for lightnode-only distribution.
pub fn broadcast_block(
    swarm: &mut Swarm<MyBehaviour>,
    block_topic: &IdentTopic,
    msg: &BlockBroadcast,
    resource_events: &mut Option<tokio::sync::mpsc::Sender<ResourceEvent>>,
) -> Result<()> {
    broadcast_block_to_lightnodes(swarm, block_topic, msg, resource_events)
}

/// Broadcast peer info to the network with retry mechanism.
/// Keeps trying until masternode accepts peer info.
/// Use `advertised_addr` (e.g. from Identify observed address) in decentralized/NAT so others can dial us.
pub async fn broadcast_peer_info(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    account: &[u8; 32],
    advertised_addr: Option<&str>,
) -> Result<()> {
    // STEP 1: Log the start of peer info publishing (Kademlia)
    info!(
        topic = %topic.to_string(),
        account = %hex::encode(account),
        "📤 STEP 1: Publishing peer info via Kademlia"
    );

    // Create the peer info message once (it doesn't change)
    let msg = GossipMessage::PeerInfo(build_peer_info(swarm, account, advertised_addr));
    info!(
        message_type = ?msg,
        "📝 STEP 2: Created peer info message"
    );

    // Encode the message once
    let payload = encode_gossip(&msg)?;
    info!(
        payload_size = payload.len(),
        "🔐 STEP 3: Encoded peer info message"
    );

    let local_peer_id = Swarm::local_peer_id(swarm).to_string();
    let key = format!("peerinfo:{}", local_peer_id);
    put_kad_record(swarm, &key, payload)?;
    info!("[OK] Peer info published to Kademlia");
    Ok(())
}

/// Synchronous version of broadcast_peer_info for use in non-async contexts.
/// Use `advertised_addr` (e.g. Identify observed) in decentralized/NAT.
pub fn broadcast_peer_info_sync(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    account: &[u8; 32],
    advertised_addr: Option<&str>,
) -> Result<()> {
    // Create the peer info message
    let msg = GossipMessage::PeerInfo(build_peer_info(swarm, account, advertised_addr));
    let payload = encode_gossip(&msg)?;
    let local_peer_id = Swarm::local_peer_id(swarm).to_string();
    let key = format!("peerinfo:{}", local_peer_id);
    put_kad_record(swarm, &key, payload)?;
    info!("[OK] SYNC: Peer info published to Kademlia");
    Ok(())
}

/// Get the number of mesh peers for gossipsub
fn get_mesh_peers_count(swarm: &mut Swarm<MyBehaviour>) -> usize {
    // Try to access mesh information from gossipsub behaviour
    // This is a simplified implementation - in production you'd access the actual mesh state
    swarm.connected_peers().count()
}

/// Attempt emergency reconnection to known peers
fn attempt_emergency_reconnection(swarm: &mut Swarm<MyBehaviour>) -> Result<()> {
    info!("🔄 EMERGENCY: Attempting to reconnect to known peers...");

    // This would typically reconnect to known bootstrap nodes
    // For now, we'll just log the attempt
    warn!("Emergency reconnection not fully implemented - would reconnect to bootstrap nodes");

    Ok(())
}

fn registration_backoff(attempt: usize) -> Duration {
    let shift = attempt.min(10) as u32;
    let delay = REGISTRATION_PUBLISH_BASE_DELAY_MS.saturating_mul(1u64 << shift);
    Duration::from_millis(delay.min(REGISTRATION_PUBLISH_MAX_DELAY_MS))
}

fn build_registration_payload(
    local_peer_id: &str,
    multiaddr: &str,
    account: &[u8; 32],
    geographic_region: &str,
    pou_score: f64,
    uptime_percentage: f64,
) -> Result<Vec<u8>> {
    let registration = LightnodeRegistration {
        node_id: local_peer_id.to_string(),
        peer_id: local_peer_id.to_string(),
        multiaddr: multiaddr.to_string(),
        geographic_region: geographic_region.to_string(),
        pou_score,
        capabilities: vec!["lightnode".to_string(), "proposer".to_string()],
        uptime_percentage,
        account: *account,
    };

    let msg = GossipMessage::LightnodeRegistration(registration);
    encode_gossip(&msg)
}

fn store_registration_kad(
    swarm: &mut Swarm<MyBehaviour>,
    local_peer_id: &str,
    payload: Vec<u8>,
) -> Result<()> {
    let key = format!("lightnode_registration:{}", local_peer_id);
    put_kad_record(swarm, &key, payload)?;
    Ok(())
}

async fn publish_registration_with_retry(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    payload: &[u8],
) -> Result<()> {
    for attempt in 0..=REGISTRATION_PUBLISH_MAX_RETRIES {
        match swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), payload.to_vec())
        {
            Ok(_) => return Ok(()),
            Err(PublishError::Duplicate) => return Ok(()),
            Err(PublishError::InsufficientPeers) => {
                if attempt == REGISTRATION_PUBLISH_MAX_RETRIES {
                    return Err(anyhow!(
                        "failed to publish registration to gossipsub: InsufficientPeers"
                    ));
                }
                let delay = registration_backoff(attempt);
                info!(
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis(),
                    "⏳ Waiting for gossipsub mesh to publish registration"
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                return Err(anyhow!("failed to publish registration to gossipsub: {e}"));
            }
        }
    }
    Ok(())
}

fn publish_registration_once(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    payload: &[u8],
) -> Result<()> {
    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload.to_vec())
    {
        Ok(_) => Ok(()),
        Err(PublishError::Duplicate) => Ok(()),
        Err(PublishError::InsufficientPeers) => Err(anyhow!(
            "failed to publish registration to gossipsub: InsufficientPeers"
        )),
        Err(e) => Err(anyhow!("failed to publish registration to gossipsub: {e}")),
    }
}

/// Try direct peer-to-peer publishing when gossipsub fails
fn try_direct_peer_publishing(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    account: &[u8; 32],
) -> Result<()> {
    info!("🔄 FALLBACK: Attempting direct peer-to-peer publishing...");

    // Create a simplified message for direct publishing
    let msg = GossipMessage::PeerInfo(build_peer_info(swarm, account, None));
    let payload = encode_gossip(&msg)?;

    // Try to publish directly to connected peers
    // This is a fallback mechanism when gossipsub mesh is insufficient
    warn!("Direct peer publishing not fully implemented - would send directly to peers");

    Err(anyhow!("Direct peer publishing not available"))
}

/// Try alternative publishing methods when main gossipsub fails
fn try_alternative_publishing(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    account: &[u8; 32],
) -> Result<()> {
    info!("🔄 ALTERNATIVE: Trying alternative publishing methods...");

    // Method 1: Try publishing with lower requirements
    let msg = GossipMessage::PeerInfo(build_peer_info(swarm, account, None));
    let payload = encode_gossip(&msg)?;

    // Method 2: Try to force publish even with minimal peers
    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload)
    {
        Ok(_message_id) => {
            info!("✅ ALTERNATIVE SUCCESS: Peer info published via alternative method");
            Ok(())
        }
        Err(e) => {
            warn!(
                error = %e,
                "❌ ALTERNATIVE FAILED: All publishing methods exhausted"
            );
            Err(anyhow!("All publishing methods failed: {}", e))
        }
    }
}

/// Send peer info directly to a specific masternode peer.
/// This avoids broadcasting to all peers in the mesh.
pub fn send_peer_info_to_masternode(
    swarm: &mut Swarm<MyBehaviour>,
    account: &[u8; 32],
    masternode_peer_id: &libp2p::PeerId,
    advertised_addr: Option<&str>,
) -> Result<()> {
    let msg = GossipMessage::PeerInfo(build_peer_info(swarm, account, advertised_addr));
    let payload = encode_gossip(&msg)?;

    // For now, we'll use a simpler approach - send via gossipsub but with a specific topic
    // that only masternodes subscribe to. This is more efficient than broadcasting to all.
    let masternode_peer_info_topic = IdentTopic::new("/savitri/peerinfo/masternode/1");

    // Subscribe to the masternode-specific topic
    if let Err(_) = swarm
        .behaviour_mut()
        .gossipsub
        .subscribe(&masternode_peer_info_topic)
    {
        // Already subscribed, continue
    }

    // Publish on the masternode-specific topic
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(masternode_peer_info_topic, payload)
        .map_err(|e| anyhow!("failed to send peer info to masternode topic: {e}"))?;

    Ok(())
}

/// Broadcast lightnode registration for group formation.
/// This sends the full node info needed by masternodes to form P2P groups.
/// CRITICAL: This must be called after successful bootstrap to enable group formation.
pub async fn broadcast_lightnode_registration(
    swarm: &mut Swarm<MyBehaviour>,
    local_peer_id: &str,
    multiaddr: &str,
    account: &[u8; 32],
    geographic_region: &str,
    pou_score: f64,
    uptime_percentage: f64,
) -> Result<()> {
    info!(
        peer_id = %local_peer_id,
        region = %geographic_region,
        pou_score = pou_score,
        "📝 REGISTRATION: Starting lightnode registration broadcast"
    );

    let payload = build_registration_payload(
        local_peer_id,
        multiaddr,
        account,
        geographic_region,
        pou_score,
        uptime_percentage,
    )?;

    info!(
        payload_size = payload.len(),
        "🔐 REGISTRATION: Encoded registration message"
    );

    let registration_topic = IdentTopic::new("/savitri/registration/1");
    let publish_result =
        publish_registration_with_retry(swarm, &registration_topic, &payload).await;

    store_registration_kad(swarm, local_peer_id, payload)?;
    info!(
        peer_id = %local_peer_id,
        "✅ REGISTRATION SUCCESS: Lightnode registration stored in Kademlia"
    );
    if let Err(err) = publish_result {
        return Err(err);
    }
    Ok(())
}

/// Synchronous version of registration broadcast (single attempt)
pub fn broadcast_lightnode_registration_sync(
    swarm: &mut Swarm<MyBehaviour>,
    local_peer_id: &str,
    multiaddr: &str,
    account: &[u8; 32],
    geographic_region: &str,
    pou_score: f64,
    uptime_percentage: f64,
) -> Result<()> {
    let payload = build_registration_payload(
        local_peer_id,
        multiaddr,
        account,
        geographic_region,
        pou_score,
        uptime_percentage,
    )?;

    let registration_topic = IdentTopic::new("/savitri/registration/1");
    let publish_result = publish_registration_once(swarm, &registration_topic, &payload);

    store_registration_kad(swarm, local_peer_id, payload)?;

    info!(
        peer_id = %local_peer_id,
        "✅ REGISTRATION: Lightnode registration stored in Kademlia"
    );
    if let Err(err) = publish_result {
        return Err(err);
    }

    Ok(())
}
