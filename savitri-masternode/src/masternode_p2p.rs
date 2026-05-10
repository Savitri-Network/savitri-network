//! Masternode P2P Communication Protocol
//!
//! This module handles P2P communication between masternodes for
//! group proposal, voting, and synchronization.

use anyhow::{Context, Result};
use libp2p::{
    gossipsub::{Behaviour as Gossipsub, IdentTopic},
    PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use super::group_consensus::{
    GroupApprovalCertificate, GroupProposal, GroupVote, LeaderElectionCertificate,
    LeaderElectionProposal,
};

/// Group announcement payload for lightnodes (gossipsub)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeGroupAnnounce {
    pub epoch: u64,
    pub group_id: String,
    pub members: Vec<String>,
    /// Peer ID -> multiaddr so lightnodes can dial each other for intra-group mesh
    #[serde(default)]
    pub member_addresses: HashMap<String, String>,
    pub proposer: String,
    pub timestamp: u64,
    /// Ed25519 signature from the masternode (hex-encoded)
    #[serde(default)]
    pub signature: Option<String>,
    /// Public key of the signing masternode (hex-encoded, 32 bytes)
    #[serde(default)]
    pub signer_pubkey: Option<String>,
    /// Shard IDs assigned to this group for TX routing
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
    /// Total shard count (65,536)
    #[serde(default)]
    pub num_shards: u32,
}

/// Masternode P2P message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MasternodeMessage {
    /// Group proposal for BFT approval
    GroupProposal(GroupProposal),
    /// Vote on group proposal
    GroupVote(GroupVote),
    /// Group approval certificate
    GroupApprovalCertificate(GroupApprovalCertificate),
    /// Request for available lightnodes count
    AvailableLightnodesRequest {
        requester_masternode: String,
        epoch: u64,
    },
    /// Response with available lightnodes count
    AvailableLightnodesResponse {
        responder_masternode: String,
        epoch: u64,
        available_count: usize,
    },
    /// Sync request for approved groups
    GroupSyncRequest {
        from_epoch: u64,
        to_epoch: u64,
        requester_masternode: String,
    },
    /// Sync response with approved groups
    GroupSyncResponse {
        certificates: Vec<GroupApprovalCertificate>,
        responder_masternode: String,
    },
    /// Leader election proposal - masternode proposes itself as group creator
    LeaderElectionProposal(LeaderElectionProposal),
    /// Leader election certificate - approval for a leader proposal
    LeaderElectionCertificate(LeaderElectionCertificate),
    /// Lightnode list sync - broadcast full list of registered lightnodes
    LightnodeListSync {
        sender_masternode: String,
        timestamp: u64,
        lightnodes: Vec<super::group_formation::LightNodeInfo>,
    },
    /// Lightnode group announcement - published to lightnode gossipsub topic
    LightnodeGroupAnnounce(LightnodeGroupAnnounce),
}

/// Masternode P2P manager for inter-masternode communication
#[derive(Clone)]
pub struct MasternodeP2PManager {
    /// Local masternode ID
    local_masternode_id: String,
    /// Connected masternode peers
    connected_masternodes: Arc<RwLock<HashMap<String, PeerId>>>,
    /// Gossipsub behavior
    gossipsub: Arc<RwLock<Gossipsub>>,
    /// Message sender to main loop
    message_sender: mpsc::UnboundedSender<(PeerId, MasternodeMessage)>,
    /// Topics for masternode communication
    proposal_topic: IdentTopic,
    vote_topic: IdentTopic,
    sync_topic: IdentTopic,
    /// Topic for leader election
    leader_election_topic: IdentTopic,
    /// Topic for lightnode list synchronization
    lightnode_list_sync_topic: IdentTopic,
}

impl MasternodeP2PManager {
    pub fn new(
        local_masternode_id: String,
        gossipsub: Gossipsub,
        message_sender: mpsc::UnboundedSender<(PeerId, MasternodeMessage)>,
    ) -> Self {
        Self {
            local_masternode_id,
            connected_masternodes: Arc::new(RwLock::new(HashMap::new())),
            gossipsub: Arc::new(RwLock::new(gossipsub)),
            message_sender,
            proposal_topic: IdentTopic::new("/savitri/masternode/group/proposal/1"),
            vote_topic: IdentTopic::new("/savitri/masternode/group/vote/1"),
            sync_topic: IdentTopic::new("/savitri/masternode/group/sync/1"),
            leader_election_topic: IdentTopic::new("/savitri/masternode/leader/election/1"),
            lightnode_list_sync_topic: IdentTopic::new("/savitri/masternode/lightnode_list/sync/1"),
        }
    }

    /// Initialize topics
    pub async fn initialize_topics(&self) -> Result<()> {
        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.subscribe(&self.proposal_topic)?;
        gossipsub.subscribe(&self.vote_topic)?;
        gossipsub.subscribe(&self.sync_topic)?;
        gossipsub.subscribe(&self.leader_election_topic)?;
        gossipsub.subscribe(&self.lightnode_list_sync_topic)?;
        info!("Subscribed to masternode P2P topics (including leader election and lightnode list sync)");
        Ok(())
    }

    /// Add connected masternode
    pub async fn add_masternode_peer(&self, masternode_id: String, peer_id: PeerId) {
        let mut connected = self.connected_masternodes.write().await;
        connected.insert(masternode_id.clone(), peer_id);
        info!("Added masternode peer: {}", masternode_id);
    }

    /// Remove disconnected masternode
    pub async fn remove_masternode_peer(&self, masternode_id: &str) {
        let mut connected = self.connected_masternodes.write().await;
        connected.remove(masternode_id);
        info!("Removed masternode peer: {}", masternode_id);
    }

    /// Broadcast group proposal to all masternodes
    pub async fn broadcast_group_proposal(&self, proposal: GroupProposal) -> Result<()> {
        let message = MasternodeMessage::GroupProposal(proposal);
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.proposal_topic.clone(), payload)?;

        info!(
            proposal_id = %message.get_proposal_id(),
            "Broadcast group proposal to masternodes"
        );
        Ok(())
    }

    /// Send group vote to specific masternode
    pub async fn send_group_vote(&self, target_masternode: &str, vote: GroupVote) -> Result<()> {
        let message = MasternodeMessage::GroupVote(vote);
        let payload = serde_json::to_vec(&message)?;

        let connected = self.connected_masternodes.read().await;
        if let Some(&peer_id) = connected.get(target_masternode) {
            let mut gossipsub = self.gossipsub.write().await;
            gossipsub.publish(self.vote_topic.clone(), payload)?;

            info!(
                proposal_id = %message.get_proposal_id(),
                target = %target_masternode,
                "Sent group vote to masternode"
            );
        } else {
            warn!("Target masternode not connected: {}", target_masternode);
        }

        Ok(())
    }

    /// Broadcast group approval certificate
    pub async fn broadcast_approval_certificate(
        &self,
        certificate: GroupApprovalCertificate,
    ) -> Result<()> {
        let message = MasternodeMessage::GroupApprovalCertificate(certificate);
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.sync_topic.clone(), payload)?;

        info!(
            epoch = message.get_epoch(),
            groups_count = message.get_groups_count(),
            "Broadcast group approval certificate"
        );
        Ok(())
    }

    /// Request available lightnodes count from other masternodes
    pub async fn request_available_lightnodes(&self, epoch: u64) -> Result<()> {
        let message = MasternodeMessage::AvailableLightnodesRequest {
            requester_masternode: self.local_masternode_id.clone(),
            epoch,
        };
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.sync_topic.clone(), payload)?;

        info!("Requested available lightnodes count from masternodes");
        Ok(())
    }

    /// Respond to available lightnodes request
    pub async fn respond_available_lightnodes(
        &self,
        requester: &str,
        epoch: u64,
        count: usize,
    ) -> Result<()> {
        let message = MasternodeMessage::AvailableLightnodesResponse {
            responder_masternode: self.local_masternode_id.clone(),
            epoch,
            available_count: count,
        };
        let payload = serde_json::to_vec(&message)?;

        let connected = self.connected_masternodes.read().await;
        if let Some(&peer_id) = connected.get(requester) {
            let mut gossipsub = self.gossipsub.write().await;
            gossipsub.publish(self.sync_topic.clone(), payload)?;

            debug!(
                "Responded to available lightnodes request from {}",
                requester
            );
        }

        Ok(())
    }

    /// Request group synchronization
    pub async fn request_group_sync(&self, from_epoch: u64, to_epoch: u64) -> Result<()> {
        let message = MasternodeMessage::GroupSyncRequest {
            from_epoch,
            to_epoch,
            requester_masternode: self.local_masternode_id.clone(),
        };
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.sync_topic.clone(), payload)?;

        info!(
            "Requested group sync for epochs {}-{}",
            from_epoch, to_epoch
        );
        Ok(())
    }

    /// Respond to group sync request
    pub async fn respond_group_sync(
        &self,
        requester: &str,
        certificates: Vec<GroupApprovalCertificate>,
    ) -> Result<()> {
        let message = MasternodeMessage::GroupSyncResponse {
            certificates,
            responder_masternode: self.local_masternode_id.clone(),
        };
        let payload = serde_json::to_vec(&message)?;

        let connected = self.connected_masternodes.read().await;
        if let Some(&peer_id) = connected.get(requester) {
            let mut gossipsub = self.gossipsub.write().await;
            gossipsub.publish(self.sync_topic.clone(), payload)?;

            info!("Responded to group sync request from {}", requester);
        }

        Ok(())
    }

    /// Send group sync response (alias for respond_group_sync)
    pub async fn send_group_sync_response(
        &self,
        requester: &str,
        response: MasternodeMessage,
    ) -> Result<()> {
        match response {
            MasternodeMessage::GroupSyncResponse { certificates, .. } => {
                self.respond_group_sync(requester, certificates).await
            }
            _ => Err(anyhow::anyhow!(
                "Invalid message type for group sync response"
            )),
        }
    }

    /// Broadcast leader election proposal to all masternodes
    pub async fn broadcast_leader_election_proposal(
        &self,
        proposal: LeaderElectionProposal,
    ) -> Result<()> {
        let message = MasternodeMessage::LeaderElectionProposal(proposal);
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.leader_election_topic.clone(), payload)?;

        info!("🗳️ Broadcast leader election proposal to masternodes");
        Ok(())
    }

    /// Broadcast leader election certificate to all masternodes
    pub async fn broadcast_leader_election_certificate(
        &self,
        certificate: LeaderElectionCertificate,
    ) -> Result<()> {
        let message = MasternodeMessage::LeaderElectionCertificate(certificate);
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.leader_election_topic.clone(), payload)?;

        info!("🗳️ Broadcast leader election certificate to masternodes");
        Ok(())
    }

    /// Broadcast lightnode list to all masternodes
    pub async fn broadcast_lightnode_list(
        &self,
        lightnodes: Vec<super::group_formation::LightNodeInfo>,
    ) -> Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let message = MasternodeMessage::LightnodeListSync {
            sender_masternode: self.local_masternode_id.clone(),
            timestamp,
            lightnodes: lightnodes.clone(),
        };
        let payload = serde_json::to_vec(&message)?;

        let mut gossipsub = self.gossipsub.write().await;
        gossipsub.publish(self.lightnode_list_sync_topic.clone(), payload)?;

        info!(
            count = lightnodes.len(),
            "Broadcast lightnode list to masternodes"
        );
        Ok(())
    }

    /// Process incoming message
    pub async fn process_message(
        &self,
        topic_hash: &libp2p::gossipsub::TopicHash,
        data: &[u8],
    ) -> Result<MasternodeMessage> {
        let message: MasternodeMessage = serde_json::from_slice(data)?;

        match &message {
            MasternodeMessage::GroupProposal(proposal) => {
                info!(
                    proposal_id = %proposal.proposal_id,
                    proposer = %proposal.proposer_masternode,
                    "Received group proposal"
                );
            }
            MasternodeMessage::GroupVote(vote) => {
                info!(
                    proposal_id = %vote.proposal_id,
                    voter = %vote.voter_masternode,
                    vote_type = ?vote.vote_type,
                    "Received group vote"
                );
            }
            MasternodeMessage::GroupApprovalCertificate(certificate) => {
                info!(
                    epoch = certificate.proposal.epoch,
                    groups_count = certificate.proposal.groups.len(),
                    "Received group approval certificate"
                );
            }
            MasternodeMessage::AvailableLightnodesRequest {
                requester_masternode,
                epoch,
            } => {
                debug!(
                    "Received available lightnodes request from {} for epoch {}",
                    requester_masternode, epoch
                );
            }
            MasternodeMessage::AvailableLightnodesResponse {
                responder_masternode,
                available_count,
                ..
            } => {
                debug!(
                    "Received available lightnodes response from {}: {} nodes",
                    responder_masternode, available_count
                );
            }
            MasternodeMessage::GroupSyncRequest {
                requester_masternode,
                from_epoch,
                to_epoch,
            } => {
                debug!(
                    "Received group sync request from {} for epochs {}-{}",
                    requester_masternode, from_epoch, to_epoch
                );
            }
            MasternodeMessage::GroupSyncResponse {
                responder_masternode,
                certificates,
            } => {
                info!(
                    "Received group sync response from {} with {} certificates",
                    responder_masternode,
                    certificates.len()
                );
            }
            MasternodeMessage::LeaderElectionProposal(proposal) => {
                info!(
                    election_id = %proposal.election_id,
                    proposer = %proposal.proposer_masternode,
                    timestamp = proposal.timestamp,
                    "🗳️ Received leader election proposal"
                );
            }
            MasternodeMessage::LeaderElectionCertificate(certificate) => {
                info!(
                    election_id = %certificate.election_id,
                    approver = %certificate.approver_masternode,
                    "🗳️ Received leader election certificate"
                );
            }
            MasternodeMessage::LightnodeListSync {
                sender_masternode,
                lightnodes,
                timestamp,
            } => {
                info!(
                    sender = %sender_masternode,
                    count = lightnodes.len(),
                    timestamp = timestamp,
                    "📋 Received lightnode list sync from masternode"
                );
            }
            MasternodeMessage::LightnodeGroupAnnounce(announce) => {
                info!(
                    group_id = %announce.group_id,
                    epoch = announce.epoch,
                    members = announce.members.len(),
                    "📣 Received lightnode group announce command"
                );
            }
        }

        Ok(message)
    }

    /// Send message to main loop for processing
    pub async fn send_to_main_loop(
        &self,
        peer_id: PeerId,
        message: MasternodeMessage,
    ) -> Result<()> {
        self.message_sender
            .send((peer_id, message))
            .map_err(|e| anyhow::anyhow!("Failed to send message to main loop: {}", e))?;
        Ok(())
    }

    /// Set gossipsub behavior reference
    pub fn set_gossipsub(&mut self, gossipsub: Gossipsub) {
        self.gossipsub = Arc::new(RwLock::new(gossipsub));
    }

    /// Get connected masternodes count
    pub async fn get_connected_masternodes_count(&self) -> usize {
        let connected = self.connected_masternodes.read().await;
        connected.len()
    }
}

// Helper methods for message extraction
impl MasternodeMessage {
    pub fn get_proposal_id(&self) -> String {
        match self {
            MasternodeMessage::GroupProposal(p) => p.proposal_id.clone(),
            MasternodeMessage::GroupVote(v) => v.proposal_id.clone(),
            MasternodeMessage::GroupApprovalCertificate(c) => c.proposal.proposal_id.clone(),
            MasternodeMessage::LeaderElectionProposal(p) => p.election_id.clone(),
            MasternodeMessage::LeaderElectionCertificate(c) => c.election_id.clone(),
            _ => "unknown".to_string(),
        }
    }

    pub fn get_epoch(&self) -> u64 {
        match self {
            MasternodeMessage::GroupProposal(p) => p.epoch,
            MasternodeMessage::GroupVote(v) => 0, // Votes don't have epoch directly
            MasternodeMessage::GroupApprovalCertificate(c) => c.proposal.epoch,
            MasternodeMessage::AvailableLightnodesRequest { epoch, .. } => *epoch,
            MasternodeMessage::AvailableLightnodesResponse { epoch, .. } => *epoch,
            MasternodeMessage::LeaderElectionProposal(p) => p.epoch,
            MasternodeMessage::LeaderElectionCertificate(_) => 0,
            MasternodeMessage::GroupSyncRequest { .. } => 0,
            MasternodeMessage::GroupSyncResponse { .. } => 0,
            MasternodeMessage::LightnodeListSync { .. } => 0,
            MasternodeMessage::LightnodeGroupAnnounce(a) => a.epoch,
        }
    }

    pub fn get_groups_count(&self) -> usize {
        match self {
            MasternodeMessage::GroupProposal(p) => p.groups.len(),
            MasternodeMessage::GroupApprovalCertificate(c) => c.proposal.groups.len(),
            MasternodeMessage::LightnodeGroupAnnounce(_) => 1,
            _ => 0,
        }
    }
}
