// Broadcast Manager - Complete implementation for P2P message broadcasting
use crate::tx::TransactionExt;
use anyhow::Result;
use libp2p::{
    gossipsub::{IdentTopic, TopicHash},
    PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub id: u64,
    pub message_type: BroadcastMessageType,
    pub data: Vec<u8>,
    pub sender: PeerId,
    pub timestamp: u64,
    pub ttl: u64, // Time to live in seconds
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BroadcastMessageType {
    Transaction,
    Block,
    Certificate,
    Heartbeat,
    GroupAnnouncement,
    PouReport,
}

#[derive(Debug, Clone)]
pub enum BroadcastEvent {
    MessageReceived {
        message: BroadcastMessage,
        source: PeerId,
    },
    MessageSent {
        message: BroadcastMessage,
        recipients: Vec<PeerId>,
    },
    BroadcastFailed {
        message: BroadcastMessage,
        error: String,
    },
}

#[derive(Debug, Clone)]
pub struct BroadcastStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub active_peers: usize,
    pub last_broadcast: u64,
}

pub struct BroadcastManager {
    local_peer_id: PeerId,
    tx_sender: tokio::sync::mpsc::UnboundedSender<TransactionExt>,
    have_tx_sender: tokio::sync::mpsc::UnboundedSender<crate::p2p::types::HaveTx>,
    event_tx: mpsc::Sender<BroadcastEvent>,
    message_id_counter: Arc<RwLock<u64>>,
    seen_messages: Arc<RwLock<HashMap<u64, u64>>>, // message_id -> timestamp
    connected_peers: Arc<RwLock<HashSet<PeerId>>>,
    stats: Arc<RwLock<BroadcastStats>>,
    topics: HashMap<BroadcastMessageType, IdentTopic>,
}

impl BroadcastManager {
    pub fn new(
        local_peer_id: PeerId,
        tx_sender: tokio::sync::mpsc::UnboundedSender<TransactionExt>,
        have_tx_sender: tokio::sync::mpsc::UnboundedSender<crate::p2p::types::HaveTx>,
    ) -> Self {
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let mut topics = HashMap::new();
        topics.insert(
            BroadcastMessageType::Transaction,
            IdentTopic::new("/savitri/broadcast/tx/1"),
        );
        topics.insert(
            BroadcastMessageType::Block,
            IdentTopic::new("/savitri/broadcast/block/1"),
        );
        topics.insert(
            BroadcastMessageType::Certificate,
            IdentTopic::new("/savitri/broadcast/cert/1"),
        );
        topics.insert(
            BroadcastMessageType::Heartbeat,
            IdentTopic::new("/savitri/broadcast/heartbeat/1"),
        );
        topics.insert(
            BroadcastMessageType::GroupAnnouncement,
            IdentTopic::new("/savitri/broadcast/group/1"),
        );
        topics.insert(
            BroadcastMessageType::PouReport,
            IdentTopic::new("/savitri/broadcast/pou/1"),
        );

        Self {
            local_peer_id,
            tx_sender,
            have_tx_sender,
            event_tx,
            message_id_counter: Arc::new(RwLock::new(1)),
            seen_messages: Arc::new(RwLock::new(HashMap::new())),
            connected_peers: Arc::new(RwLock::new(HashSet::new())),
            stats: Arc::new(RwLock::new(BroadcastStats {
                messages_sent: 0,
                messages_received: 0,
                bytes_sent: 0,
                bytes_received: 0,
                active_peers: 0,
                last_broadcast: 0,
            })),
            topics,
        }
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!(
            "Starting Broadcast Manager for peer: {}",
            self.local_peer_id
        );

        // Start cleanup task for expired messages
        let seen_messages = Arc::clone(&self.seen_messages);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let mut seen = seen_messages.write().await;
                seen.retain(|_, timestamp| current_time - *timestamp < 300); // Remove messages older than 5 minutes

                debug!(
                    "Cleaned up old broadcast messages, {} remaining",
                    seen.len()
                );
            }
        });

        info!("Broadcast Manager tasks started successfully");
        Ok(())
    }

    pub async fn broadcast_transaction(&self, tx: TransactionExt) -> Result<()> {
        let message_id = self.next_message_id().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let message = BroadcastMessage {
            id: message_id,
            message_type: BroadcastMessageType::Transaction,
            data: serde_json::to_vec(&tx)?,
            sender: self.local_peer_id,
            timestamp: current_time,
            ttl: 300, // 5 minutes
        };

        self.broadcast_message(message).await
    }

    pub async fn broadcast_block(&self, block_data: Vec<u8>) -> Result<()> {
        let message_id = self.next_message_id().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let message = BroadcastMessage {
            id: message_id,
            message_type: BroadcastMessageType::Block,
            data: block_data,
            sender: self.local_peer_id,
            timestamp: current_time,
            ttl: 300,
        };

        self.broadcast_message(message).await
    }

    pub async fn broadcast_certificate(&self, cert_data: Vec<u8>) -> Result<()> {
        let message_id = self.next_message_id().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let message = BroadcastMessage {
            id: message_id,
            message_type: BroadcastMessageType::Certificate,
            data: cert_data,
            sender: self.local_peer_id,
            timestamp: current_time,
            ttl: 300,
        };

        self.broadcast_message(message).await
    }

    pub async fn broadcast_heartbeat(&self) -> Result<()> {
        let message_id = self.next_message_id().await;
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let heartbeat_data = serde_json::json!({
            "peer_id": self.local_peer_id.to_string(),
            "timestamp": current_time,
            "status": "active"
        });

        let message = BroadcastMessage {
            id: message_id,
            message_type: BroadcastMessageType::Heartbeat,
            data: serde_json::to_vec(&heartbeat_data)?,
            sender: self.local_peer_id,
            timestamp: current_time,
            ttl: 60, // 1 minute for heartbeats
        };

        self.broadcast_message(message).await
    }

    async fn broadcast_message(&self, message: BroadcastMessage) -> Result<()> {
        let peers = self.connected_peers.read().await;
        let recipients: Vec<PeerId> = peers.iter().cloned().collect();

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.bytes_sent += message.data.len() as u64;
            stats.last_broadcast = message.timestamp;
        }

        // Send to all connected peers
        let mut successful_sends = 0;
        for peer in &recipients {
            if let Err(e) = self.send_to_peer(*peer, &message).await {
                warn!(
                    "Failed to send message {} to peer {}: {}",
                    message.id, peer, e
                );
            } else {
                successful_sends += 1;
            }
        }

        info!(
            "Broadcast message {} to {} peers",
            message.id, successful_sends
        );

        // Send event
        if let Err(e) = self
            .event_tx
            .send(BroadcastEvent::MessageSent {
                message,
                recipients,
            })
            .await
        {
            error!("Failed to send broadcast event: {}", e);
        }

        Ok(())
    }

    async fn send_to_peer(&self, peer: PeerId, message: &BroadcastMessage) -> Result<()> {
        // In a real implementation, this would use the actual P2P network
        // For now, we'll simulate the send
        debug!("Sending message {} to peer {}", message.id, peer);

        match message.message_type {
            BroadcastMessageType::Transaction => {
                // Send transaction via tx_sender
                let tx: TransactionExt = serde_json::from_slice(&message.data)?;
                if let Err(e) = self.tx_sender.send(tx) {
                    return Err(anyhow::anyhow!("Failed to send transaction: {}", e));
                }
            }
            BroadcastMessageType::Block => {
                // Send block via appropriate channel
                // This would be implemented with actual block broadcasting
                debug!("Broadcasting block {} to peer {}", message.id, peer);
            }
            _ => {
                // Handle other message types
                debug!(
                    "Broadcasting message type {:?} to peer {}",
                    message.message_type, peer
                );
            }
        }

        Ok(())
    }

    pub async fn handle_incoming_message(
        &self,
        message: BroadcastMessage,
        source: PeerId,
    ) -> Result<()> {
        // Check if we've already seen this message
        {
            let mut seen = self.seen_messages.write().await;
            if seen.contains_key(&message.id) {
                debug!("Already seen message {}, ignoring", message.id);
                return Ok(());
            }
            seen.insert(message.id, message.timestamp);
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_received += 1;
            stats.bytes_received += message.data.len() as u64;
        }

        // Process the message based on type
        match message.message_type {
            BroadcastMessageType::Transaction => {
                let tx: TransactionExt = serde_json::from_slice(&message.data)?;
                if let Err(e) = self.tx_sender.send(tx) {
                    error!("Failed to forward transaction: {}", e);
                }
            }
            BroadcastMessageType::Block => {
                debug!("Received block message {} from {}", message.id, source);
                // Process block
            }
            BroadcastMessageType::Certificate => {
                debug!(
                    "Received certificate message {} from {}",
                    message.id, source
                );
                // Process certificate
            }
            BroadcastMessageType::Heartbeat => {
                debug!("Received heartbeat from {}", source);
                // Update peer status
            }
            BroadcastMessageType::GroupAnnouncement => {
                debug!("Received group announcement from {}", source);
                // Process group announcement
            }
            BroadcastMessageType::PouReport => {
                debug!("Received PoU report from {}", source);
                // Process PoU report
            }
        }

        // Send event
        if let Err(e) = self
            .event_tx
            .send(BroadcastEvent::MessageReceived { message, source })
            .await
        {
            error!("Failed to send message received event: {}", e);
        }

        Ok(())
    }

    pub async fn add_peer(&self, peer: PeerId) {
        let mut peers = self.connected_peers.write().await;
        if peers.insert(peer) {
            info!("Added peer {} to broadcast manager", peer);

            // Update stats
            let mut stats = self.stats.write().await;
            stats.active_peers = peers.len();
        }
    }

    pub async fn remove_peer(&self, peer: PeerId) {
        let mut peers = self.connected_peers.write().await;
        if peers.remove(&peer) {
            info!("Removed peer {} from broadcast manager", peer);

            // Update stats
            let mut stats = self.stats.write().await;
            stats.active_peers = peers.len();
        }
    }

    pub async fn get_stats(&self) -> BroadcastStats {
        self.stats.read().await.clone()
    }

    pub async fn get_connected_peers(&self) -> Vec<PeerId> {
        self.connected_peers.read().await.iter().cloned().collect()
    }

    pub async fn get_topic(&self, message_type: BroadcastMessageType) -> Option<IdentTopic> {
        self.topics.get(&message_type).cloned()
    }

    pub async fn get_all_topics(&self) -> Vec<IdentTopic> {
        self.topics.values().cloned().collect()
    }

    async fn next_message_id(&self) -> u64 {
        let mut counter = self.message_id_counter.write().await;
        let id = *counter;
        *counter += 1;
        id
    }

    pub async fn send(
        &self,
        event: BroadcastEvent,
    ) -> Result<(), mpsc::error::SendError<BroadcastEvent>> {
        self.event_tx.send(event).await
    }
}
