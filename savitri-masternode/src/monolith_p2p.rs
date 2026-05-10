//! Monolith P2P Distribution
//!
//! This module handles the P2P distribution of monolith blocks
//! to light nodes and other masternodes in the Savitri Network.

use crate::monolith_producer::MonolithBlock;
use anyhow::{Context, Result};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};
// use savitri_zkp::monolith::MonolithHeader;
use savitri_core::core::monolith::MonolithHeader;

// Define the types locally since we can't import from the library crate in a binary module
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeGroupAnnounce {
    pub epoch: u64,
    pub group_id: String,
    pub members: Vec<String>,
    /// Peer ID -> multiaddr so lightnodes can dial each other for intra-group mesh
    #[serde(default)]
    pub member_addresses: std::collections::HashMap<String, String>,
    pub proposer: String,
    pub timestamp: u64,
    /// Ed25519 signature from the masternode that issued the announcement (hex-encoded)
    #[serde(default)]
    pub signature: Option<String>,
    /// Public key of the signing masternode (hex-encoded, 32 bytes)
    #[serde(default)]
    pub signer_pubkey: Option<String>,
    /// Shard IDs assigned to this group (65,536 total shards / num_groups per group).
    /// Lightnodes use this to route TX: only process TX whose sender's shard is in this list.
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
    /// Total number of shards in the network (for shard_id = hash(address) % num_shards)
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

// Implement the group distribution trait
impl super::group_formation::MonolithP2PDistributor for MonolithP2PManager {
    fn distribute_groups(
        &self,
        groups: &[super::group_formation::P2PGroup],
        epoch: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'static>> {
        // Clone data to satisfy 'static lifetime requirement
        let groups_cloned: Vec<super::group_formation::P2PGroup> = groups.to_vec();
        let distributor = self.clone();
        Box::pin(async move {
            distributor
                .distribute_groups_impl(&groups_cloned, epoch)
                .await
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
    /// Connected peers (used when shared_peers is None, e.g. in tests)
    connected_peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    /// Shared peer map from Libp2pNetwork - when set, used for get_stats/distribute (source of truth)
    shared_peers: Option<Arc<RwLock<HashMap<PeerId, PeerInfo>>>>,
    /// Monolith cache (height -> block)
    monolith_cache: Arc<RwLock<HashMap<u64, MonolithBlock>>>,
    /// Message sender for P2P communication
    message_sender: mpsc::UnboundedSender<(PeerId, MonolithMessage)>,
    /// Optional sender for gossipsub lightnode announcements
    lightnode_announce_tx: Option<mpsc::UnboundedSender<MasternodeMessage>>,
    /// Ed25519 signing key for group announcements
    signing_key: Option<ed25519_dalek::SigningKey>,
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
    /// Create new monolith P2P manager (uses internal peer map, for tests)
    pub fn new(
        local_peer_id: PeerId,
        config: MonolithP2PConfig,
        message_sender: mpsc::UnboundedSender<(PeerId, MonolithMessage)>,
    ) -> Self {
        Self {
            local_peer_id,
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            shared_peers: None,
            monolith_cache: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            lightnode_announce_tx: None,
            signing_key: None,
            config,
        }
    }

    /// Create monolith P2P manager with shared peer map from Libp2pNetwork.
    /// Uses shared_peers for get_stats/distribute so BFT quorum sees actual masternode connections.
    pub fn with_shared_peers(
        local_peer_id: PeerId,
        config: MonolithP2PConfig,
        message_sender: mpsc::UnboundedSender<(PeerId, MonolithMessage)>,
        shared_peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    ) -> Self {
        Self {
            local_peer_id,
            connected_peers: Arc::new(RwLock::new(HashMap::new())),
            shared_peers: Some(shared_peers),
            monolith_cache: Arc::new(RwLock::new(HashMap::new())),
            message_sender,
            lightnode_announce_tx: None,
            signing_key: None,
            config,
        }
    }

    /// Set the Ed25519 signing key for group announcement signatures
    pub fn set_signing_key(&mut self, key: ed25519_dalek::SigningKey) {
        self.signing_key = Some(key);
    }

    /// Peers map for reads: shared_peers if set, else internal connected_peers
    async fn peers_for_read(&self) -> tokio::sync::RwLockReadGuard<'_, HashMap<PeerId, PeerInfo>> {
        match &self.shared_peers {
            Some(sp) => sp.read().await,
            None => self.connected_peers.read().await,
        }
    }

    /// Attach gossipsub announce sender (optional)
    pub fn set_lightnode_announce_sender(&mut self, tx: mpsc::UnboundedSender<MasternodeMessage>) {
        info!("🔔 RACCOMANDAZIONE #3: Setting lightnode_announce_tx channel");
        self.lightnode_announce_tx = Some(tx);
        info!("✅ lightnode_announce_tx channel initialized successfully");
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

        let peers = self.peers_for_read().await;
        let mut success_count = 0;
        let mut failure_count = 0;

        for peer_info in peers.values() {
            // Prioritize light nodes for monolith distribution
            if peer_info.is_light_node {
                info!(
                    peer_id = %peer_info.peer_id,
                    start_height = block.start_height,
                    end_height = block.end_height,
                    "📤 [MN->LN] Step 1: Sending monolith block to lightnode peer"
                );
                match self
                    .send_monolith_to_peer(peer_info.peer_id, block, true)
                    .await
                {
                    Ok(_) => {
                        success_count += 1;
                        info!(
                            peer_id = %peer_info.peer_id,
                            "📤 [MN->LN] Step 2: Monolith block queued for lightnode successfully"
                        );
                    }
                    Err(e) => {
                        failure_count += 1;
                        warn!(peer_id = %peer_info.peer_id, error = %e, "Failed to send monolith to lightnode");
                    }
                }
            }
        }

        // Also send to other masternodes for redundancy
        for peer_info in peers.values().filter(|p| !p.is_light_node) {
            info!(
                peer_id = %peer_info.peer_id,
                start_height = block.start_height,
                end_height = block.end_height,
                "📤 [MN->MN] Step 1: Sending monolith block to masternode peer"
            );
            match self
                .send_monolith_to_peer(peer_info.peer_id, block, false)
                .await
            {
                Ok(_) => {
                    success_count += 1;
                    info!(
                        peer_id = %peer_info.peer_id,
                        "📤 [MN->MN] Step 2: Monolith block queued for masternode successfully"
                    );
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
    async fn send_monolith_to_peer(
        &self,
        peer_id: PeerId,
        block: &MonolithBlock,
        is_light_node: bool,
    ) -> Result<()> {
        let direction = if is_light_node { "MN->LN" } else { "MN->MN" };
        info!(
            peer_id = %peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            direction = direction,
            "📤 [{}] Step 3: Queuing MonolithAnnounce for P2P send", direction
        );

        let message = MonolithMessage::MonolithAnnounce {
            block: block.clone(),
            sender_peer_id: self.local_peer_id,
        };

        // Send message via P2P channel
        match self.message_sender.send((peer_id, message)) {
            Ok(_) => {
                info!(
                    peer_id = %peer_id,
                    direction = direction,
                    "📤 [{}] Step 4: MonolithAnnounce message sent to channel", direction
                );
            }
            Err(e) => {
                error!(peer_id = %peer_id, error = %e, "Failed to send monolith message - channel may be closed");
                return Err(anyhow::anyhow!("Channel closed: {}", e));
            }
        }

        Ok(())
    }

    /// Distribute P2P groups to all connected light nodes
    pub async fn distribute_groups_impl(
        &self,
        groups: &[crate::group_formation::P2PGroup],
        epoch: u64,
    ) -> Result<()> {
        let peers = self.peers_for_read().await;
        let lightnode_count = peers.values().filter(|p| p.is_light_node).count();
        let total_peers = peers.len();
        drop(peers);

        info!(
            groups_count = groups.len(),
            epoch = epoch,
            lightnodes_connected = lightnode_count,
            total_peers = total_peers,
            "🔄 DISTRIBUTE_GROUPS CALLED - Checking if conditions are met"
        );

        if groups.is_empty() {
            info!("No groups to publish yet (normal at startup or not enough nodes)");
            return Ok(());
        }

        if lightnode_count == 0 {
            warn!(
                connected_lightnodes = lightnode_count,
                groups_count = groups.len(),
                "⏳ NO LIGHTNODES CONNECTED - Cannot distribute groups"
            );
            return Ok(());
        }

        info!(
            groups_count = groups.len(),
            epoch = epoch,
            lightnodes_connected = lightnode_count,
            "✅ CONDITIONS MET - Proceeding with group distribution"
        );

        info!(
            groups_count = groups.len(),
            epoch = epoch,
            "🔔 RACCOMANDAZIONE #2: distribute_groups_impl called - Distributing P2P groups to light nodes"
        );

        for group in groups {
            info!(
                group_id = %group.group_id,
                members_count = group.members.len(),
                proposer = ?group.proposer,
                "Group to be distributed"
            );
        }

        // Short delay before distributing groups to allow lightnodes to stabilize connections.
        // Reduced from 2000ms to 200ms: the original 2s blocked the swarm event loop,
        // preventing the Masternode from receiving any gossipsub messages (including block
        // proposals from lightnodes) during distribution. Lightnodes handle late announcements
        // via retry logic, so a shorter delay is sufficient.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        info!("Group distribution delay completed (200ms) - proceeding with distribution");

        let peers = self.peers_for_read().await;
        let mut success_count = 0;
        let mut failure_count = 0;
        let lightnode_count = peers.values().filter(|p| p.is_light_node).count();

        info!(
            lightnodes = lightnode_count,
            groups_count = groups.len(),
            epoch = epoch,
            "Broadcasting group announcement to light nodes"
        );

        // Only send to light nodes
        for peer_info in peers.values().filter(|p| p.is_light_node) {
            match self
                .send_groups_to_peer(peer_info.peer_id, groups, epoch)
                .await
            {
                Ok(_) => {
                    success_count += 1;
                    debug!(peer_id = %peer_info.peer_id, "Groups sent successfully");
                }
                Err(e) => {
                    failure_count += 1;
                    warn!(peer_id = %peer_info.peer_id, error = %e, "Failed to send groups");
                }
            }
        }

        // Also publish group announcements via gossipsub for lightnodes
        if let Some(tx) = &self.lightnode_announce_tx {
            info!(
                groups_count = groups.len(),
                "🔔 RACCOMANDAZIONE #3: lightnode_announce_tx is available, publishing group announcements via gossipsub"
            );
            let timestamp = chrono::Utc::now().timestamp() as u64;
            let mut published_count = 0;
            for group in groups {
                let proposer = group
                    .proposer
                    .clone()
                    .unwrap_or_else(|| group.members.first().cloned().unwrap_or_default());
                // Only propagate registration-derived, dialable listen addresses.
                // Connection remote_addr may be an ephemeral client port and must not be used
                // for LN<->LN dialing via group announcement.
                let member_addresses = group.member_multiaddrs.clone();
                let mut announce = LightnodeGroupAnnounce {
                    epoch: group.epoch,
                    group_id: group.group_id.clone(),
                    members: group.members.clone(),
                    member_addresses,
                    proposer,
                    timestamp,
                    signature: None,
                    signer_pubkey: None,
                    assigned_shards: group.assigned_shards.clone(),
                    num_shards: 65_536,
                };
                // Sign the group announcement so lightnodes can verify authenticity
                // Message format must match lightnode verification:
                //   epoch(LE8) || group_id || proposer || timestamp(LE8) || members_count(LE8)
                if let Some(ref sk) = self.signing_key {
                    use ed25519_dalek::Signer;
                    let mut msg = Vec::new();
                    msg.extend_from_slice(&announce.epoch.to_le_bytes());
                    msg.extend_from_slice(announce.group_id.as_bytes());
                    msg.extend_from_slice(announce.proposer.as_bytes());
                    msg.extend_from_slice(&announce.timestamp.to_le_bytes());
                    msg.extend_from_slice(&(announce.members.len() as u64).to_le_bytes());
                    let sig = sk.sign(&msg);
                    announce.signature = Some(hex::encode(sig.to_bytes()));
                    announce.signer_pubkey = Some(hex::encode(sk.verifying_key().to_bytes()));
                }
                if announce.member_addresses.is_empty() {
                    warn!(
                        group_id = %announce.group_id,
                        members_count = announce.members.len(),
                        "Group announce: member_addresses is EMPTY - LNs may dial by peer_id only"
                    );
                } else {
                    for (mb_peer_id, mb_addr) in &announce.member_addresses {
                        info!(
                            group_id = %announce.group_id,
                            peer_id = %mb_peer_id,
                            member_addr = %mb_addr,
                            "Group announce: member_addresses entry"
                        );
                    }
                }
                info!(
                    group_id = %announce.group_id,
                    epoch = announce.epoch,
                    members_count = announce.members.len(),
                    "🔔 RACCOMANDAZIONE #3: Publishing lightnode group announce via gossipsub"
                );
                if let Err(e) = tx.send(MasternodeMessage::LightnodeGroupAnnounce(announce)) {
                    error!(
                        error = %e,
                        group_id = %group.group_id,
                        "❌ Failed to queue lightnode group announce via gossipsub"
                    );
                } else {
                    published_count += 1;
                    info!(
                        group_id = %group.group_id,
                        "✅ Lightnode group announce queued successfully"
                    );
                }
            }
            info!(
                published_count = published_count,
                total_groups = groups.len(),
                "Group announcements published via gossipsub"
            );
        } else {
            warn!(
                groups_count = groups.len(),
                "❌ RACCOMANDAZIONE #3: lightnode_announce_tx is None - group announcements will NOT be published via gossipsub"
            );
        }

        info!(
            lightnodes = lightnode_count,
            success = success_count,
            failed = failure_count,
            "Group announcement broadcast completed"
        );

        info!(
            success_count = success_count,
            failure_count = failure_count,
            "Group distribution completed"
        );

        Ok(())
    }

    /// Send groups to specific peer
    async fn send_groups_to_peer(
        &self,
        peer_id: PeerId,
        groups: &[crate::group_formation::P2PGroup],
        epoch: u64,
    ) -> Result<()> {
        let message = MonolithMessage::GroupAnnounce {
            groups: groups.to_vec(),
            epoch,
            sender_peer_id: self.local_peer_id,
        };

        // Send message via P2P channel
        match self.message_sender.send((peer_id, message)) {
            Ok(_) => {
                debug!(peer_id = %peer_id, "Groups message sent successfully");
                info!(
                    peer_id = %peer_id,
                    groups_count = groups.len(),
                    epoch = epoch,
                    "Announce sent (M->L)"
                );
            }
            Err(e) => {
                error!(peer_id = %peer_id, error = %e, "Failed to send groups message - channel may be closed");
                return Err(anyhow::anyhow!("Channel closed: {}", e));
            }
        }

        Ok(())
    }

    /// Handle incoming monolith message
    pub async fn handle_message(
        &self,
        sender_peer_id: PeerId,
        message: MonolithMessage,
    ) -> Result<()> {
        match message {
            MonolithMessage::MonolithAnnounce {
                block,
                sender_peer_id: _,
            } => {
                self.handle_monolith_announce(sender_peer_id, block).await?;
            }
            MonolithMessage::MonolithRequest {
                height,
                epoch_id,
                requester_peer_id,
            } => {
                self.handle_monolith_request(sender_peer_id, height, epoch_id, requester_peer_id)
                    .await?;
            }
            MonolithMessage::MonolithResponse {
                block,
                requester_peer_id,
            } => {
                self.handle_monolith_response(sender_peer_id, block, requester_peer_id)
                    .await?;
            }
            MonolithMessage::MonolithVerify {
                block_hash,
                requester_peer_id,
            } => {
                self.handle_monolith_verify(sender_peer_id, block_hash, requester_peer_id)
                    .await?;
            }
            MonolithMessage::MonolithVerifyResponse {
                is_valid,
                block_hash,
                requester_peer_id,
            } => {
                self.handle_monolith_verify_response(
                    sender_peer_id,
                    is_valid,
                    block_hash,
                    requester_peer_id,
                )
                .await?;
            }
            MonolithMessage::GroupAnnounce {
                groups,
                epoch,
                sender_peer_id: _,
            } => {
                self.handle_group_announce(sender_peer_id, groups, epoch)
                    .await?;
            }
            MonolithMessage::GroupRequest {
                node_info,
                requester_peer_id,
            } => {
                self.handle_group_request(sender_peer_id, node_info, requester_peer_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Handle monolith announcement (received from another peer - LN or MN)
    async fn handle_monolith_announce(
        &self,
        sender_peer_id: PeerId,
        block: MonolithBlock,
    ) -> Result<()> {
        info!(
            sender_peer_id = %sender_peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            "📥 [MN<-peer] Step 1: Received MonolithAnnounce from peer"
        );

        // Cache the monolith block
        let mut cache = self.monolith_cache.write().await;
        cache.insert(block.end_height, block.clone());
        info!(
            sender_peer_id = %sender_peer_id,
            end_height = block.end_height,
            cache_size = cache.len(),
            "📥 [MN<-peer] Step 2: Monolith block cached"
        );

        // Limit cache size
        if cache.len() > self.config.max_cache_size {
            self.cleanup_cache(&mut cache).await?;
        }

        // Verify the monolith if we have the capability
        if let Err(e) = self.verify_monolith_integrity(&block).await {
            warn!(error = %e, "Monolith verification failed");
        } else {
            info!(
                sender_peer_id = %sender_peer_id,
                end_height = block.end_height,
                "📥 [MN<-peer] Step 3: Monolith block received and processed successfully"
            );
        }

        Ok(())
    }

    /// Handle monolith request
    async fn handle_monolith_request(
        &self,
        sender_peer_id: PeerId,
        height: u64,
        epoch_id: u64,
        requester_peer_id: PeerId,
    ) -> Result<()> {
        debug!(
            requester_peer_id = %requester_peer_id,
            height = height,
            epoch_id = epoch_id,
            "Received monolith request"
        );

        // Check if we have the requested monolith
        let cache = self.monolith_cache.read().await;
        if let Some(block) = cache.get(&height) {
            // Send the monolith block
            let response = MonolithMessage::MonolithResponse {
                block: block.clone(),
                requester_peer_id,
            };

            self.message_sender
                .send((sender_peer_id, response))
                .map_err(|e| {
                    error!(error = %e, "Failed to send monolith response - channel may be closed");
                    anyhow::anyhow!("Channel closed: {}", e)
                })?;

            info!(
                requester_peer_id = %requester_peer_id,
                height = height,
                "Sent monolith block in response"
            );
        } else {
            warn!(
                requester_peer_id = %requester_peer_id,
                height = height,
                "Requested monolith not found in cache"
            );
        }

        Ok(())
    }

    /// Handle monolith response
    async fn handle_monolith_response(
        &self,
        sender_peer_id: PeerId,
        block: MonolithBlock,
        requester_peer_id: PeerId,
    ) -> Result<()> {
        if requester_peer_id != self.local_peer_id {
            warn!("Received monolith response for different peer");
            return Ok(());
        }

        info!(
            sender_peer_id = %sender_peer_id,
            start_height = block.start_height,
            end_height = block.end_height,
            "Received monolith block response"
        );

        // Cache the received monolith
        let mut cache = self.monolith_cache.write().await;
        cache.insert(block.end_height, block.clone());

        // Verify integrity
        if let Err(e) = self.verify_monolith_integrity(&block).await {
            warn!(error = %e, "Received monolith verification failed");
        }

        Ok(())
    }

    /// Handle monolith verification request
    async fn handle_monolith_verify(
        &self,
        sender_peer_id: PeerId,
        block_hash: [u8; 64],
        requester_peer_id: PeerId,
    ) -> Result<()> {
        debug!(
            requester_peer_id = %requester_peer_id,
            "Received monolith verification request"
        );

        // Find the monolith block with the given hash
        let cache = self.monolith_cache.read().await;
        let is_valid = cache.values().any(|block| {
            // Calculate block hash (simplified)
            self.calculate_block_hash(block) == block_hash
        });

        // Send verification response
        let response = MonolithMessage::MonolithVerifyResponse {
            is_valid,
            block_hash,
            requester_peer_id,
        };

        self.message_sender
            .send((sender_peer_id, response))
            .map_err(|e| {
                error!(error = %e, "Failed to send verification response - channel may be closed");
                anyhow::anyhow!("Channel closed: {}", e)
            })?;

        info!(
            requester_peer_id = %requester_peer_id,
            is_valid = is_valid,
            "Sent monolith verification response"
        );

        Ok(())
    }

    /// Handle monolith verify response
    async fn handle_monolith_verify_response(
        &self,
        sender_peer_id: PeerId,
        is_valid: bool,
        block_hash: [u8; 64],
        requester_peer_id: PeerId,
    ) -> Result<()> {
        if requester_peer_id != self.local_peer_id {
            return Ok(());
        }

        info!(
            sender_peer_id = %sender_peer_id,
            is_valid = is_valid,
            "Received monolith verification response"
        );

        if !is_valid {
            warn!(block_hash = ?block_hash, "Monolith verification failed - removing from cache");

            // Remove invalid monolith from cache
            let mut cache = self.monolith_cache.write().await;
            cache.retain(|_, block| self.calculate_block_hash(block) != block_hash);
        }

        Ok(())
    }

    /// Verify monolith block integrity
    async fn verify_monolith_integrity(&self, block: &MonolithBlock) -> Result<()> {
        // Verify headers commitment
        // This would use the actual verification logic from MonolithProducer
        // For now, just do basic checks

        if block.start_height >= block.end_height {
            return Err(anyhow::anyhow!("Invalid block range"));
        }

        if block.block_count == 0 {
            return Err(anyhow::anyhow!("Empty monolith block"));
        }

        info!("Monolith integrity verification passed");
        Ok(())
    }

    /// Calculate block hash (simplified)
    fn calculate_block_hash(&self, block: &MonolithBlock) -> [u8; 64] {
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(block.end_height.to_le_bytes());
        hasher.update(block.start_height.to_le_bytes());
        hasher.update(block.creator_id.as_bytes());
        let result = hasher.finalize();
        result.as_slice().try_into().unwrap()
    }

    /// Handle group announcement from masternode
    async fn handle_group_announce(
        &self,
        sender_peer_id: PeerId,
        groups: Vec<crate::group_formation::P2PGroup>,
        epoch: u64,
    ) -> Result<()> {
        info!(
            sender_peer_id = %sender_peer_id,
            groups_count = groups.len(),
            epoch = epoch,
            "Received group announcement from masternode"
        );

        // This would typically be handled by a light node
        for group in &groups {
            debug!(
                group_id = %group.group_id,
                members_count = group.members.len(),
                proposer = ?group.proposer,
                "Received P2P group"
            );
        }

        Ok(())
    }

    /// Handle group request from light node
    async fn handle_group_request(
        &self,
        sender_peer_id: PeerId,
        node_info: crate::group_formation::LightNodeInfo,
        requester_peer_id: PeerId,
    ) -> Result<()> {
        info!(
            sender_peer_id = %sender_peer_id,
            node_id = %node_info.node_id,
            peer_id = %node_info.peer_id,
            "Received group request from light node"
        );
        info!(
            node_id = %node_info.node_id,
            peer_id = %node_info.peer_id,
            "Announce received (L->M)"
        );

        // This would trigger group formation for the requesting node
        // For now, just log the request
        debug!(
            geographic_region = %node_info.geographic_region,
            pou_score = node_info.pou_score,
            "Light node requesting group formation"
        );

        Ok(())
    }

    /// Cleanup old entries from cache
    async fn cleanup_cache(&self, cache: &mut HashMap<u64, MonolithBlock>) -> Result<()> {
        if cache.len() <= self.config.max_cache_size {
            return Ok(());
        }

        // Sort by height and remove oldest entries
        let mut heights: Vec<u64> = cache.keys().copied().collect();
        heights.sort_unstable();

        let remove_count = cache.len() - self.config.max_cache_size;
        for height in heights.iter().take(remove_count) {
            cache.remove(height);
        }

        info!(
            removed_count = remove_count,
            cache_size = cache.len(),
            "Cleaned up monolith cache"
        );

        Ok(())
    }

    /// Get monolith statistics
    pub async fn get_stats(&self) -> MonolithP2PStats {
        let peers = self.peers_for_read().await;
        let cache = self.monolith_cache.read().await;

        let light_nodes = peers.values().filter(|p| p.is_light_node).count();
        let masternodes = peers.values().filter(|p| !p.is_light_node).count();

        MonolithP2PStats {
            connected_peers: peers.len(),
            light_nodes,
            masternodes,
            cached_monoliths: cache.len(),
            cache_size_bytes: self.calculate_cache_size(&cache),
        }
    }

    /// Return peer IDs of ALL masternodes (connected + self), for group consensus ordering.
    /// CRITICAL: The local peer ID MUST be included so that all masternodes have the same
    /// sorted list. Without it, each MN has a different list (N-1 peers, each excluding
    /// itself), leading to inconsistent leader assignments where no MN recognizes itself
    /// as group owner.
    pub async fn get_connected_masternode_ids(&self) -> Vec<String> {
        let peers = self.peers_for_read().await;
        let mut ids: Vec<String> = peers
            .values()
            .filter(|p| !p.is_light_node)
            .map(|p| p.peer_id.to_string())
            .collect();
        // Include our own peer ID for consistent ordering across all masternodes
        ids.push(self.local_peer_id.to_string());
        ids
    }

    /// Calculate cache size in bytes
    fn calculate_cache_size(&self, cache: &HashMap<u64, MonolithBlock>) -> usize {
        cache
            .values()
            .map(|block| {
                // Rough estimation of block size
                std::mem::size_of::<MonolithBlock>() + block.zkp_proof.len()
            })
            .sum()
    }
}

/// Monolith P2P statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithP2PStats {
    pub connected_peers: usize,
    pub light_nodes: usize,
    pub masternodes: usize,
    pub cached_monoliths: usize,
    pub cache_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_monolith_p2p_manager() {
        let local_peer_id = PeerId::random();
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = MonolithP2PManager::new(local_peer_id, MonolithP2PConfig::default(), tx);

        // Test adding peers
        let peer1 = PeerInfo {
            peer_id: PeerId::random(),
            multiaddr: "/ip4/127.0.0.1/tcp/8080".parse().unwrap(),
            is_light_node: true,
            last_seen: 123456789,
            reputation: 0.95,
        };

        manager.add_peer(peer1.clone()).await.unwrap();

        // Test getting stats
        let stats = manager.get_stats().await;
        assert_eq!(stats.connected_peers, 1);
        assert_eq!(stats.light_nodes, 1);
        assert_eq!(stats.masternodes, 0);

        println!("✅ Monolith P2P manager test passed!");
        println!("📊 Connected peers: {}", stats.connected_peers);
        println!("💡 Light nodes: {}", stats.light_nodes);
    }

    #[tokio::test]
    async fn test_monolith_distribution() {
        let local_peer_id = PeerId::random();
        let (tx, _rx) = mpsc::unbounded_channel();
        let manager = MonolithP2PManager::new(local_peer_id, MonolithP2PConfig::default(), tx);

        // Create test monolith block
        let block = MonolithBlock {
            header: MonolithHeader {
                headers_commit: [1; 64],
                state_commit: [2; 64],
                exec_height: 1000,
                epoch_id: 123,
            },
            start_height: 900,
            end_height: 1000,
            block_count: 100,
            total_transactions: 15000,
            created_at: 123456789,
            creator_id: "test_node".to_string(),
            zkp_proof: vec![1, 2, 3, 4],
        };

        // Add a light node peer
        let peer = PeerInfo {
            peer_id: PeerId::random(),
            multiaddr: "/ip4/127.0.0.1/tcp/8080".parse().unwrap(),
            is_light_node: true,
            last_seen: 123456789,
            reputation: 0.95,
        };

        manager.add_peer(peer).await.unwrap();

        // Test distribution
        manager.distribute_monolith(&block).await.unwrap();

        println!("✅ Monolith distribution test passed!");
    }
}
