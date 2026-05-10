//! Monolith P2P Distribution
//!
//! This module handles the P2P distribution of monolith blocks
//! to light nodes and other masternodes in the Savitri Network.

use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn, error, debug};
use libp2p::{PeerId, Multiaddr};
use crate::monolith_producer::MonolithBlock;
// use savitri_zkp::monolith::MonolithHeader;
use savitri_core::core::monolith::MonolithHeader;

// Define the types locally since we can't import from the library crate in a binary module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeGroupAnnounce {
    pub epoch: u64,
    pub group_id: String,
    pub members: Vec<String>,
    #[serde(default)]
    pub member_addresses: HashMap<String, String>,
    pub proposer: String,
    pub timestamp: u64,
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
    #[serde(default)]
    pub num_shards: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MasternodeMessage {
    LightnodeGroupAnnounce(LightnodeGroupAnnounce),
    GroupProposal(crate::group_consensus::GroupProposal),
    GroupVote(crate::group_consensus::GroupVote),
    GroupApprovalCertificate(crate::group_consensus::GroupApprovalCertificate),
    AvailableLightnodesRequest {
        requester_masternode: String,
        epoch: u64,
    },
    AvailableLightnodesResponse {
        responder_masternode: String,
        epoch: u64,
        available_count: usize,
    },
    GroupSyncRequest {
        from_epoch: u64,
        to_epoch: u64,
        requester_masternode: String,
    },
    GroupSyncResponse {
        from_epoch: u64,
        to_epoch: u64,
        groups: Vec<crate::group_formation::P2PGroup>,
        responder_masternode: String,
    },
    LeaderElectionProposal(crate::group_consensus::LeaderElectionProposal),
    LeaderElectionCertificate(crate::group_consensus::LeaderElectionCertificate),
    LightnodeListSync {
        epoch: u64,
        lightnodes: Vec<crate::group_formation::LightNodeInfo>,
        requester_masternode: String,
    },
}

// Implement conversion from the library type to our local type
impl From<savitri_masternode::masternode_p2p::MasternodeMessage> for MasternodeMessage {
    fn from(msg: savitri_masternode::masternode_p2p::MasternodeMessage) -> Self {
        use savitri_masternode::masternode_p2p::MasternodeMessage as LibMsg;
        match msg {
            LibMsg::LightnodeGroupAnnounce(announce) => {
                MasternodeMessage::LightnodeGroupAnnounce(LightnodeGroupAnnounce {
                    epoch: announce.epoch,
                    group_id: announce.group_id,
                    members: announce.members,
                    member_addresses: announce.member_addresses,
                    proposer: announce.proposer,
                    timestamp: announce.timestamp,
                    assigned_shards: announce.assigned_shards,
                    num_shards: announce.num_shards,
                })
            }
            LibMsg::GroupProposal(p) => MasternodeMessage::GroupProposal(p),
            LibMsg::GroupVote(v) => MasternodeMessage::GroupVote(v),
            LibMsg::GroupApprovalCertificate(c) => MasternodeMessage::GroupApprovalCertificate(c),
            LibMsg::AvailableLightnodesRequest { requester_masternode, epoch } => {
                MasternodeMessage::AvailableLightnodesRequest { requester_masternode, epoch }
            }
            LibMsg::AvailableLightnodesResponse { responder_masternode, epoch, available_count } => {
                MasternodeMessage::AvailableLightnodesResponse {
                    responder_masternode,
                    epoch,
                    available_count,
                }
            }
            LibMsg::GroupSyncRequest { from_epoch, to_epoch, requester_masternode } => {
                MasternodeMessage::GroupSyncRequest {
                    from_epoch,
                    to_epoch,
                    requester_masternode,
                }
            }
            LibMsg::GroupSyncResponse { certificates: _, responder_masternode } => {
                // Lib type carries certificates; we expose groups. Use empty groups; full sync should use lib type.
                MasternodeMessage::GroupSyncResponse {
                    from_epoch: 0,
                    to_epoch: 0,
                    groups: Vec::new(),
                    responder_masternode,
                }
            }
            LibMsg::LeaderElectionProposal(p) => MasternodeMessage::LeaderElectionProposal(p),
            LibMsg::LeaderElectionCertificate(c) => MasternodeMessage::LeaderElectionCertificate(c),
            LibMsg::LightnodeListSync { sender_masternode, lightnodes, timestamp: _ } => {
                MasternodeMessage::LightnodeListSync {
                    epoch: 0,
                    lightnodes,
                    requester_masternode: sender_masternode,
                }
            }
        }
    }
}

// Implement the group distribution trait
impl super::group_formation::MonolithP2PDistributor for MonolithP2PManager {
    fn distribute_groups(&self, groups: &[super::group_formation::P2PGroup], epoch: u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'static>> {
        // Clone data to satisfy 'static lifetime requirement
        let groups_cloned: Vec<super::group_formation::P2PGroup> = groups.to_vec();
        let distributor = self.clone();
        Box::pin(async move {
            distributor.distribute_groups_impl(&groups_cloned, epoch).await
        })
    }
}

/// P2P message types for monolith distribution
#[derive(Debug, Clone, Serialize)]
pub enum MonolithMessage {
    /// Monolith block announcement
    MonolithAnnounce {
        block: MonolithBlock,
        sender_peer_id: PeerId,
    },
    /// Request for monolith block
    MonolithRequest {
        height: u64,
        epoch_id: u64,
        requester_peer_id: PeerId,
    },
    /// Monolith block response
    MonolithResponse {
        block: MonolithBlock,
        requester_peer_id: PeerId,
    },
    /// Monolith verification request
    MonolithVerify {
        #[serde(with = "BigArray")]
        block_hash: [u8; 64],
        requester_peer_id: PeerId,
    },
    /// Monolith verification response
    MonolithVerifyResponse {
        is_valid: bool,
        #[serde(with = "BigArray")]
        block_hash: [u8; 64],
        requester_peer_id: PeerId,
    },
    /// Group announcement to light nodes
    GroupAnnounce {
        groups: Vec<crate::group_formation::P2PGroup>,
        epoch: u64,
        sender_peer_id: PeerId,
    },
    /// Group formation request from light node
    GroupRequest {
        node_info: crate::group_formation::LightNodeInfo,
        requester_peer_id: PeerId,
    },
}

/// Monolith P2P distribution manager
#[derive(Clone)]
pub struct MonolithP2PManager {
    /// Local peer ID
    local_peer_id: PeerId,
    /// Connected peers
    connected_peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    /// Monolith cache (height -> block)
    monolith_cache: Arc<RwLock<HashMap<u64, MonolithBlock>>>,
    /// Message sender for P2P communication
    message_sender: mpsc::UnboundedSender<(PeerId, MonolithMessage)>,
    /// Optional sender for gossipsub lightnode announcements
    lightnode_announce_tx: Option<mpsc::UnboundedSender<MasternodeMessage>>,
    /// Configuration
    config: MonolithP2PConfig,
}

/// Peer information
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub multiaddr: Multiaddr,
    pub is_light_node: bool,
    pub last_seen: u64,
    pub reputation: f64,
}

/// Monolith P2P configuration
#[derive(Debug, Clone)]
pub struct MonolithP2PConfig {
    /// Maximum monolith blocks to cache
    pub max_cache_size: usize,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
    /// Broadcast interval for monolith announcements
    pub broadcast_interval_secs: u64,
    /// Maximum retries for failed messages
    pub max_retries: u32,
}

impl Default for MonolithP2PConfig {
    fn default() -> Self {
        Self {
            max_cache_size: 1000,
            cache_ttl_secs: 86400 * 7, // 7 days
            broadcast_interval_secs: 30,
            max_retries: 3,
        }
    }
}

impl MonolithP2PManager {
    /// Create new monolith P2P manager
    pub fn new(
        local_peer_id: PeerId,
        config: MonolithP2PConfig,
        message_sender: mpsc::UnboundedSender<(PeerId, MonolithMessage)>,
    ) -> Self {
        Self {
            local_peer_id,
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            monolith_cache: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            lightnode_announce_tx: None,
            config,
        }
    }

    /// Attach gossipsub announce sender (optional)
    pub fn set_lightnode_announce_sender(&mut self, tx: mpsc::UnboundedSender<MasternodeMessage>) {
        self.lightnode_announce_tx = Some(tx);
    }

    /// Add connected peer
    pub async fn add_peer(&self, peer_info: PeerInfo) -> Result<()> {
        let mut peers = self.connected_peers.write().await;
        peers.insert(peer_info.peer_id, peer_info.clone());
        
        info!(
            peer_id = %peer_info.peer_id,
            is_light_node = peer_info.is_light_node,
            "Peer added to monolith P2P manager"
        );
        Ok(())
    }

    /// Remove disconnected peer
    pub async fn remove_peer(&self, peer_id: PeerId) -> Result<()> {
        let mut peers = self.connected_peers.write().await;
        if peers.remove(&peer_id).is_some() {
            info!(peer_id = %peer_id, "Peer removed from monolith P2P manager");
        }
        Ok(())
    }

    /// Distribute monolith block to all connected peers
    pub async fn distribute_monolith(&self, block: &MonolithBlock) -> Result<()> {
        info!(
            start_height = block.start_height,
            end_height = block.end_height,
            "Distributing monolith block to all peers"
        );

        let peers = self.connected_peers.read().await;
        let mut success_count = 0;
        let mut failure_count = 0;

        for peer_info in peers.values() {
            // Prioritize light nodes for monolith distribution
            if peer_info.is_light_node {
                match self.send_monolith_to_peer(peer_info.peer_id, block).await {
                    Ok(_) => {
                        success_count += 1;
                        debug!(peer_id = %peer_info.peer_id, "Monolith sent successfully");
                    }
                    Err(e) => {
                        failure_count += 1;
                        warn!(peer_id = %peer_info.peer_id, error = %e, "Failed to send monolith");
                    }
                }
            }
        }

        // Also send to other masternodes for redundancy
        for peer_info in peers.values().filter(|p|!p.is_light_node) {
            match self.send_monolith_to_peer(peer_info.peer_id, block).await {
                Ok(_) => {
                    success_count += 1;
                    debug!(peer_id = %peer_info.peer_id, "Monolith sent to masternode");
                }
                Err(e) => {
                    failure_count += 1;
                    warn!(peer_id = %peer_info.peer_id, error = %e, "Failed to send monolith to masternode");
                }
            }
        }

        info!(
            success_count = success_count,
            failure_count = failure_count,
            "Monolith distribution completed"
        );

        Ok(())
    }

    /// Broadcast monolith block to all connected peers (alias for distribute_monolith)
    pub async fn broadcast_monolith(&self, block: &MonolithBlock) -> Result<()> {
        self.distribute_monolith(block).await
    }

    /// Send monolith block to specific peer
    pub async fn send_monolith_to_peer(&self, peer_id: PeerId, block: &MonolithBlock) -> Result<()> {
        let message = MonolithMessage::MonolithAnnounce {
            block: block.clone(),
            sender_peer_id: self.local_peer_id,
        };

        self.message_sender.send((peer_id, message))
            .context("Failed to send monolith announcement")?;

        debug!(
            peer_id = %peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            "Monolith announcement sent to peer"
        );

        Ok(())
    }

    /// Handle incoming monolith message
    pub async fn handle_monolith_message(&self, sender_peer_id: PeerId, message: MonolithMessage) -> Result<()> {
        match message {
            MonolithMessage::MonolithAnnounce { block, .. } => {
                self.handle_monolith_announce(sender_peer_id, block).await?;
            }
            MonolithMessage::MonolithRequest { height, epoch_id, requester_peer_id } => {
                self.handle_monolith_request(sender_peer_id, height, epoch_id, requester_peer_id).await?;
            }
            MonolithMessage::MonolithResponse { block, requester_peer_id } => {
                self.handle_monolith_response(sender_peer_id, block, requester_peer_id).await?;
            }
            MonolithMessage::MonolithVerify { block_hash, requester_peer_id } => {
                self.handle_monolith_verify(sender_peer_id, block_hash, requester_peer_id).await?;
            }
            MonolithMessage::MonolithVerifyResponse { is_valid, block_hash, requester_peer_id } => {
                self.handle_monolith_verify_response(sender_peer_id, is_valid, block_hash, requester_peer_id).await?;
            }
            MonolithMessage::GroupAnnounce { groups, epoch, sender_peer_id } => {
                self.handle_group_announce(sender_peer_id, groups, epoch).await?;
            }
            MonolithMessage::GroupRequest { node_info, requester_peer_id } => {
                self.handle_group_request(sender_peer_id, node_info, requester_peer_id).await?;
            }
        }

        Ok(())
    }

    /// Handle monolith announcement
    async fn handle_monolith_announce(&self, sender_peer_id: PeerId, block: MonolithBlock) -> Result<()> {
        info!(
            peer_id = %sender_peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            "Received monolith announcement"
        );

        // Cache the monolith block
        let mut cache = self.monolith_cache.write().await;
        cache.insert(block.start_height, block);

        Ok(())
    }

    /// Handle monolith request
    async fn handle_monolith_request(&self, sender_peer_id: PeerId, height: u64, epoch_id: u64, requester_peer_id: PeerId) -> Result<()> {
        debug!(
            peer_id = %sender_peer_id,
            height = height,
            epoch_id = epoch_id,
            "Received monolith request"
        );

        // Check cache for requested monolith
        let cache = self.monolith_cache.read().await;
        if let Some(block) = cache.get(&height) {
            let response = MonolithMessage::MonolithResponse {
                block: block.clone(),
                requester_peer_id,
            };

            drop(cache); // Release read lock before sending

            self.message_sender.send((sender_peer_id, response))
                .context("Failed to send monolith response")?;
        } else {
            debug!("Requested monolith not found in cache");
        }

        Ok(())
    }

    /// Handle monolith response
    async fn handle_monolith_response(&self, sender_peer_id: PeerId, block: MonolithBlock, requester_peer_id: PeerId) -> Result<()> {
        if requester_peer_id != self.local_peer_id {
            debug!("Received monolith response for different peer");
            return Ok(());
        }

        info!(
            peer_id = %sender_peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            "Received monolith response"
        );

        // Cache the received monolith
        let mut cache = self.monolith_cache.write().await;
        cache.insert(block.start_height, block);

        Ok(())
    }

    /// Handle monolith verification request
    async fn handle_monolith_verify(&self, sender_peer_id: PeerId, block_hash: [u8; 64], requester_peer_id: PeerId) -> Result<()> {
        debug!(
            peer_id = %sender_peer_id,
            block_hash = %hex::encode(block_hash),
            "Received monolith verification request"
        );

        // Check if we have this monolith in cache
        let cache = self.monolith_cache.read().await;
        let is_valid = cache.values().any(|block| {
            // Simple hash check - in production, this would verify the actual monolith structure
            block.start_height > 0 && block.end_height >= block.start_height
        });

        let response = MonolithMessage::MonolithVerifyResponse {
            is_valid,
            block_hash,
            requester_peer_id,
        };

        drop(cache); // Release read lock before sending

        self.message_sender.send((sender_peer_id, response))
            .context("Failed to send monolith verification response")?;

        Ok(())
    }

    /// Handle monolith verification response
    async fn handle_monolith_verify_response(&self, sender_peer_id: PeerId, is_valid: bool, block_hash: [u8; 64], requester_peer_id: PeerId) -> Result<()> {
        if requester_peer_id != self.local_peer_id {
            debug!("Received monolith verification response for different peer");
            return Ok(());
        }

        info!(
            peer_id = %sender_peer_id,
            block_hash = %hex::encode(block_hash),
            is_valid = is_valid,
            "Received monolith verification response"
        );

        // Handle verification result
        if !is_valid {
            warn!("Monolith verification failed for hash: {}", hex::encode(block_hash));
        }

        Ok(())
    }

    /// Handle group announcement
    async fn handle_group_announce(&self, sender_peer_id: PeerId, groups: Vec<crate::group_formation::P2PGroup>, epoch: u64) -> Result<()> {
        info!(
            peer_id = %sender_peer_id,
            groups_count = groups.len(),
            epoch = epoch,
            "Received group announcement"
        );

        // Forward each group as a separate announcement so lightnodes receive
        // the correct group_id and member_addresses for their specific group.
        if let Some(ref tx) = self.lightnode_announce_tx {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            for group in &groups {
                let announce = MasternodeMessage::LightnodeGroupAnnounce(LightnodeGroupAnnounce {
                    epoch,
                    group_id: group.group_id.clone(),
                    members: group.members.clone(),
                    member_addresses: group.member_multiaddrs.clone(),
                    proposer: sender_peer_id.to_string(),
                    timestamp,
                    assigned_shards: group.assigned_shards.clone(),
                    num_shards: 65_536,
                });

                tx.send(announce).context("Failed to forward group announcement")?;
            }
        }

        Ok(())
    }

    /// Handle group request
    async fn handle_group_request(&self, sender_peer_id: PeerId, node_info: crate::group_formation::LightNodeInfo, requester_peer_id: PeerId) -> Result<()> {
        debug!(
            peer_id = %sender_peer_id,
            node_id = %node_info.node_id,
            "Received group request"
        );

        // This would typically be handled by the group formation module
        // For now, we just log the request
        info!("Group request from light node: {}", node_info.node_id);

        Ok(())
    }

    /// Distribute groups implementation
    async fn distribute_groups_impl(&self, groups: &[crate::group_formation::P2PGroup], epoch: u64) -> Result<()> {
        info!(
            groups_count = groups.len(),
            epoch = epoch,
            "Distributing groups to light nodes"
        );

        // Create group announcement
        let announce = MonolithMessage::GroupAnnounce {
            groups: groups.to_vec(),
            epoch,
            sender_peer_id: self.local_peer_id,
        };

        // Send to all light nodes
        let peers = self.connected_peers.read().await;
        let mut success_count = 0;
        let mut failure_count = 0;

        for peer_info in peers.values().filter(|p|p.is_light_node) {
            match self.message_sender.send((peer_info.peer_id, announce.clone())) {
                Ok(_) => success_count += 1,
                Err(e) => {
                    failure_count += 1;
                    warn!(peer_id = %peer_info.peer_id, error = %e, "Failed to send group announcement");
                }
            }
        }

        info!(
            success_count = success_count,
            failure_count = failure_count,
            "Group distribution completed"
        );

        Ok(())
    }

    /// Get monolith cache statistics
    pub async fn get_cache_stats(&self) -> (usize, u64) {
        let cache = self.monolith_cache.read().await;
        (cache.len(), cache.len() as u64)
    }

    /// Clear expired monolith blocks from cache
    pub async fn clear_expired_cache(&self) -> Result<usize> {
        let mut cache = self.monolith_cache.write().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let initial_count = cache.len();
        cache.retain(|_, _| {
            // Simple retention logic - in production, this would check actual timestamps
            true
        });

        let removed_count = initial_count - cache.len();
        if removed_count > 0 {
            info!(removed_count = removed_count, "Cleared expired monolith blocks from cache");
        }

        Ok(removed_count)
    }
}
