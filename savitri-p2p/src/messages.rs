//! P2P Messages for Savitri Network
//! 
//! This module implements the actual P2P message handling using shared types
//! from savitri-core to avoid dependency cycles.

use serde::{Deserialize, Serialize};
use savitri_core::core::p2p_messages::{ConsensusCertificate, ConsensusProposal, ConsensusVote};
use savitri_core::core::shared_types::{NetworkMessage, MessageType, NetworkConfig};

/// P2P message wrapper that extends the base NetworkMessage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PMessage {
    pub network_message: NetworkMessage,
    pub routing_info: RoutingInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingInfo {
    pub source_peer: [u8; 32],
    pub destination_peer: Option<[u8; 32]>,
    pub hop_count: u32,
    pub ttl: u32,
}

/// Message handler for P2P layer
pub struct MessageHandler {
    config: NetworkConfig,
    message_queue: Vec<P2PMessage>,
}

impl MessageHandler {
    pub fn new(config: NetworkConfig) -> Self {
        Self {
            config,
            message_queue: Vec::new(),
        }
    }
    
    pub fn handle_consensus_proposal(&mut self, proposal: ConsensusProposal) -> anyhow::Result<()> {
        // Convert to network message and handle
        let network_msg = NetworkMessage {
            message_type: MessageType::Consensus,
            payload: bincode::serialize(&proposal)?,
            sender: proposal.proposer,
            timestamp: proposal.timestamp,
            signature: None, // Will be added by networking layer
        };
        
        let p2p_msg = P2PMessage {
            network_message: network_msg,
            routing_info: RoutingInfo {
                source_peer: [0u8; 32], // Will be filled by networking layer
                destination_peer: None, // Broadcast
                hop_count: 0,
                ttl: self.config.message_timeout,
            },
        };
        
        self.message_queue.push(p2p_msg);
        Ok(())
    }
    
    pub fn handle_consensus_vote(&mut self, vote: ConsensusVote) -> anyhow::Result<()> {
        let network_msg = NetworkMessage {
            message_type: MessageType::Consensus,
            payload: bincode::serialize(&vote)?,
            sender: vote.validator,
            timestamp: vote.timestamp,
            signature: Some(vote.signature),
        };
        
        let p2p_msg = P2PMessage {
            network_message: network_msg,
            routing_info: RoutingInfo {
                source_peer: [0u8; 32],
                destination_peer: None,
                hop_count: 0,
                ttl: self.config.message_timeout,
            },
        };
        
        self.message_queue.push(p2p_msg);
        Ok(())
    }
    
    pub fn handle_consensus_certificate(&mut self, cert: ConsensusCertificate) -> anyhow::Result<()> {
        let network_msg = NetworkMessage {
            message_type: MessageType::Consensus,
            payload: bincode::serialize(&cert)?,
            sender: cert.validator_signature, // Using signature as sender identifier
            timestamp: cert.timestamp,
            signature: Some(cert.validator_signature),
        };
        
        let p2p_msg = P2PMessage {
            network_message: network_msg,
            routing_info: RoutingInfo {
                source_peer: [0u8; 32],
                destination_peer: None,
                hop_count: 0,
                ttl: self.config.message_timeout,
            },
        };
        
        self.message_queue.push(p2p_msg);
        Ok(())
    }
    
    pub fn get_pending_messages(&mut self) -> Vec<P2PMessage> {
        std::mem::take(&mut self.message_queue)
    }
    
    pub fn validate_message(&self, msg: &P2PMessage) -> anyhow::Result<()> {
        // Validate routing info
        if msg.routing_info.hop_count > msg.routing_info.ttl {
            anyhow::bail!("Message TTL exceeded");
        }
        
        // Validate timestamp (not too old)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        if now.saturating_sub(msg.network_message.timestamp) > self.config.heartbeat_interval * 2 {
            anyhow::bail!("Message too old");
        }
        
        Ok(())
    }
}

/// Message serialization utilities
pub mod serialization {
    use super::*;
    
    pub fn serialize_message(msg: &P2PMessage) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(msg).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
    }
    
    pub fn deserialize_message(data: &[u8]) -> anyhow::Result<P2PMessage> {
        bincode::deserialize(data).map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
    }
    
    pub fn message_size_estimate(msg: &P2PMessage) -> usize {
        // Rough estimate for routing and bandwidth management
        bincode::serialized_size(msg).unwrap_or(1024) as usize
    }
}
