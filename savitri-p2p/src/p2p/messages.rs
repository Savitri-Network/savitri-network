//! Fixed P2P Messages Module
//!
//! Conservative implementation compatible with Savitri architecture

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Message routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRoutingConfig {
    pub enable_flooding: bool,
    pub enable_gossip: bool,
    pub max_hops: u8,
    pub ttl_seconds: u64,
}

impl Default for MessageRoutingConfig {
    fn default() -> Self {
        Self {
            enable_flooding: true,
            enable_gossip: true,
            max_hops: 10,
            ttl_seconds: 300,
        }
    }
}

/// Message types supported by the P2P network
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// Ping message for connectivity testing
    Ping,
    /// Pong response to ping
    Pong,
    /// Gossip message
    Gossip,
    /// Direct message
    Direct,
    /// Block announcement
    BlockAnnouncement,
    /// Transaction message
    Transaction,
    /// Consensus message
    Consensus,
    /// Peer exchange message
    PeerExchange,
    /// Custom message type
    Custom(String),
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::Ping
    }
}

/// Message priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for MessagePriority {
    fn default() -> Self {
        MessagePriority::Normal
    }
}

/// Message wrapper with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Message type
    pub message_type: MessageType,
    /// Message ID (unique identifier)
    pub message_id: String,
    /// Source peer ID (serialized as string)
    pub source_peer: String,
    /// Target peer ID (None for broadcast, serialized as string)
    pub target_peer: Option<String>,
    /// Message payload
    pub payload: Vec<u8>,
    /// Timestamp (serialized as u64)
    pub timestamp: u64,
    /// Message version for compatibility
    pub version: String,
    /// Message priority
    pub priority: MessagePriority,
    /// TTL (time to live) in seconds
    pub ttl: Option<u64>,
    /// Message signature
    pub signature: Option<String>,
}

impl Message {
    pub fn new(message_type: MessageType, source_peer: String, payload: Vec<u8>) -> Self {
        Self {
            message_type,
            message_id: format!(
                "msg_{}_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                rand::random::<u32>()
            ),
            source_peer,
            target_peer: None,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            version: "1.0".to_string(),
            priority: MessagePriority::Normal,
            ttl: Some(300), // 5 minutes default TTL
            signature: None,
        }
    }

    pub fn with_target(mut self, target_peer: String) -> Self {
        self.target_peer = Some(target_peer);
        self
    }

    pub fn with_priority(mut self, priority: MessagePriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now > self.timestamp + ttl
        } else {
            false
        }
    }

    pub fn is_broadcast(&self) -> bool {
        self.target_peer.is_none()
    }

    /// Get the canonical bytes used for signing/verification.
    /// Covers message_id, source_peer, payload, and timestamp to prevent tampering.
    pub fn signing_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(self.message_id.as_bytes());
        buf.extend_from_slice(self.source_peer.as_bytes());
        buf.extend_from_slice(&self.payload);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf
    }

    /// Sign this message using an Ed25519 signing key.
    /// Stores the hex-encoded signature in the `signature` field.
    pub fn sign(&mut self, signing_key: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        let bytes = self.signing_bytes();
        let sig = signing_key.sign(&bytes);
        self.signature = Some(hex::encode(sig.to_bytes()));
    }

    /// Verify the message signature against the given public key bytes.
    /// Returns true if the signature is valid.
    pub fn verify_signature(&self, public_key_bytes: &[u8; 32]) -> bool {
        use ed25519_dalek::Verifier;
        let sig_hex = match &self.signature {
            Some(s) => s,
            None => return false,
        };
        let sig_bytes = match hex::decode(sig_hex) {
            Ok(b) if b.len() == 64 => b,
            _ => return false,
        };
        let sig_arr: [u8; 64] = match sig_bytes.try_into() {
            Ok(a) => a,
            Err(_) => return false,
        };
        let sig = ed25519_dalek::Signature::from_bytes(&sig_arr);
        let pk = match ed25519_dalek::VerifyingKey::from_bytes(public_key_bytes) {
            Ok(pk) => pk,
            Err(_) => return false,
        };
        pk.verify(&self.signing_bytes(), &sig).is_ok()
    }

    pub fn size(&self) -> usize {
        self.payload.len()
    }
}

/// Message handler trait
#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync {
    /// Handle incoming message
    async fn handle_message(
        &mut self,
        message: Message,
    ) -> Result<Option<Message>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get supported message types
    fn supported_message_types(&self) -> Vec<MessageType>;

    /// Get handler name
    fn handler_name(&self) -> &str;
}

/// Message statistics
#[derive(Debug, Clone, Default)]
pub struct MessageStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub messages_failed: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub average_message_size: f64,
    pub messages_by_type: HashMap<MessageType, u64>,
}

/// Message router
pub struct MessageRouter {
    handlers: HashMap<MessageType, Vec<String>>,
    stats: MessageStats,
    event_sender: mpsc::UnboundedSender<MessageRouterEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<MessageRouterEvent>>,
}

/// Message router events
#[derive(Debug, Clone)]
pub enum MessageRouterEvent {
    MessageReceived { message: Message },
    MessageSent { message_id: String, target: String },
    MessageHandled { message_id: String, handler: String },
    MessageFailed { message_id: String, error: String },
}

impl MessageRouter {
    pub fn new() -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self {
            handlers: HashMap::new(),
            stats: MessageStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
        }
    }

    pub fn add_handler(&mut self, handler: Box<dyn MessageHandler>) {
        let supported_types = handler.supported_message_types();
        let handler_name = handler.handler_name().to_string();

        for message_type in supported_types {
            self.handlers
                .entry(message_type)
                .or_insert_with(Vec::new)
                .push(handler_name.clone());
        }

        tracing::info!("Added message handler: {}", handler_name);
    }

    pub async fn route_message(
        &mut self,
        message: Message,
    ) -> Result<Vec<Message>, Box<dyn std::error::Error + Send + Sync>> {
        // Update stats
        self.stats.messages_received += 1;
        self.stats.bytes_received += message.size() as u64;
        *self
            .stats
            .messages_by_type
            .entry(message.message_type.clone())
            .or_insert(0) += 1;

        // Send event
        let _ = self.event_sender.send(MessageRouterEvent::MessageReceived {
            message: message.clone(),
        });

        // Route to handlers
        let responses = Vec::new();

        if let Some(handlers) = self.handlers.get(&message.message_type) {
            for handler_name in handlers {
                // In a real implementation, you would call the actual handler
                // For now, we'll just simulate handling
                tracing::debug!("Routing message to handler: {}", handler_name);

                // Send event
                let _ = self.event_sender.send(MessageRouterEvent::MessageHandled {
                    message_id: message.message_id.clone(),
                    handler: handler_name.clone(),
                });
            }
        } else {
            tracing::warn!(
                "No handlers found for message type: {:?}",
                message.message_type
            );
        }

        Ok(responses)
    }

    pub fn send_message(
        &mut self,
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Update stats
        self.stats.messages_sent += 1;
        self.stats.bytes_sent += message.size() as u64;

        // Send event
        let _ = self.event_sender.send(MessageRouterEvent::MessageSent {
            message_id: message.message_id.clone(),
            target: message
                .target_peer
                .clone()
                .unwrap_or("broadcast".to_string()),
        });

        tracing::debug!(
            "Sent message: {} -> {}",
            message.source_peer,
            message
                .target_peer
                .as_ref()
                .map_or("broadcast", |v| v.as_str())
        );

        Ok(())
    }

    pub fn get_stats(&self) -> MessageStats {
        self.stats.clone()
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<MessageRouterEvent>> {
        self.event_receiver.take()
    }
}

impl Default for MessageRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple ping handler for testing
pub struct PingHandler;

#[async_trait::async_trait]
impl MessageHandler for PingHandler {
    async fn handle_message(
        &mut self,
        message: Message,
    ) -> Result<Option<Message>, Box<dyn std::error::Error + Send + Sync>> {
        match message.message_type {
            MessageType::Ping => {
                // Create pong response
                let pong = Message::new(MessageType::Pong, "handler".to_string(), b"pong".to_vec())
                    .with_target(message.source_peer);

                Ok(Some(pong))
            }
            _ => Ok(None),
        }
    }

    fn supported_message_types(&self) -> Vec<MessageType> {
        vec![MessageType::Ping]
    }

    fn handler_name(&self) -> &str {
        "PingHandler"
    }
}
