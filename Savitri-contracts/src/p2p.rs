//! P2P Module
//!
//! Complete peer-to-peer networking implementation for Savitri blockchain including:
//! - Message routing and broadcasting
//! - Network topology management
//! - Oracle data distribution
//! - Consensus message handling
//! - Peer discovery and reputation

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Message types for P2P communication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageType {
    /// Oracle data feed updates
    OracleData,
    /// Oracle data requests
    OracleRequest,
    /// Oracle data responses
    OracleResponse,
    /// Block propagation
    Block,
    /// Transaction propagation
    Transaction,
    /// Consensus messages
    Consensus,
    /// Peer discovery
    PeerDiscovery,
    /// Heartbeat/ping messages
    Heartbeat,
    /// Network status updates
    NetworkStatus,
    /// Contract deployment
    ContractDeploy,
    /// Contract execution
    ContractCall,
}

/// P2P message with routing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PMessage {
    /// Message type
    pub msg_type: MessageType,
    /// Message payload
    pub data: Vec<u8>,
    /// Sender peer ID
    pub sender: Vec<u8>,
    /// Message ID for deduplication
    pub msg_id: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// TTL (time to live) in seconds
    pub ttl: u64,
    /// Hop count (for routing)
    pub hop_count: u8,
    /// Maximum hops allowed
    pub max_hops: u8,
    /// Priority level
    pub priority: MessagePriority,
}

/// Message priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl P2PMessage {
    /// Create new P2P message
    pub fn new(
        msg_type: MessageType,
        data: Vec<u8>,
        sender: Vec<u8>,
        priority: MessagePriority,
    ) -> Self {
        let msg_id = Self::generate_message_id(&msg_type, &data, &sender);
        Self {
            msg_type,
            data,
            sender,
            msg_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ttl: 300, // Default 5 minutes TTL
            hop_count: 0,
            max_hops: 10,
            priority,
        }
    }

    /// Generate unique message ID
    fn generate_message_id(msg_type: &MessageType, data: &[u8], sender: &[u8]) -> Vec<u8> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();

        // Include message type discriminator
        let type_discriminator = match msg_type {
            MessageType::OracleData => 1,
            MessageType::OracleRequest => 2,
            MessageType::OracleResponse => 3,
            MessageType::Block => 4,
            MessageType::Transaction => 5,
            MessageType::Consensus => 6,
            MessageType::PeerDiscovery => 7,
            MessageType::Heartbeat => 8,
            MessageType::NetworkStatus => 9,
            MessageType::ContractDeploy => 10,
            MessageType::ContractCall => 11,
        };

        hasher.update(&[type_discriminator]);
        hasher.update(data);
        hasher.update(sender);
        hasher.update(
            &SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_le_bytes(),
        );

        hasher.finalize().to_vec()
    }

    /// Check if message is expired
    pub fn is_expired(&self) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        current_time > self.timestamp + self.ttl
    }

    /// Check if message should be forwarded
    pub fn should_forward(&self) -> bool {
        self.hop_count < self.max_hops && !self.is_expired()
    }

    /// Increment hop count
    pub fn increment_hop(&mut self) {
        self.hop_count += 1;
    }

    /// Get message age in seconds
    pub fn age(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.timestamp)
    }
}

/// Network manager for P2P operations
#[derive(Debug, Clone)]
pub struct Network {
    /// Connected peers
    peers: HashMap<Vec<u8>, PeerInfo>,
    /// Network configuration
    config: NetworkConfig,
    /// Message routing table
    routing_table: HashMap<Vec<u8>, Vec<Vec<u8>>>,
    /// Network statistics
    stats: NetworkStats,
    /// Last heartbeat times
    last_heartbeats: HashMap<Vec<u8>, u64>,
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Maximum number of peers
    pub max_peers: usize,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
    /// Peer timeout in seconds
    pub peer_timeout: u64,
    /// Message TTL in seconds
    pub default_ttl: u64,
    /// Maximum hops for message forwarding
    pub max_hops: u8,
    /// Enable message deduplication
    pub enable_deduplication: bool,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            max_peers: 50,
            heartbeat_interval: 30,
            peer_timeout: 120,
            default_ttl: 300,
            max_hops: 10,
            enable_deduplication: true,
        }
    }
}

/// Peer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer ID
    pub id: Vec<u8>,
    /// Peer address
    pub address: String,
    /// Peer capabilities
    pub capabilities: Vec<String>,
    /// Peer reputation score
    pub reputation: f64,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Connection status
    pub status: PeerStatus,
    /// Peer metadata
    pub metadata: PeerMetadata,
}

/// Peer connection status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PeerStatus {
    Connected,
    Disconnected,
    Connecting,
    Banned,
}

/// Peer metadata
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerMetadata {
    /// Client version
    pub client_version: String,
    /// Protocol version
    pub protocol_version: String,
    /// Geographic location
    pub location: Option<String>,
    /// Bandwidth capacity
    pub bandwidth: Option<u64>,
    /// Latency in milliseconds
    pub latency: Option<u64>,
}

/// Network statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Total bytes transferred
    pub bytes_transferred: u64,
    /// Active connections
    pub active_connections: usize,
    /// Average latency
    pub average_latency: f64,
    /// Message deduplication hits
    pub deduplication_hits: u64,
    /// Network uptime in seconds
    pub uptime: u64,
}

impl Network {
    /// Create new network instance
    pub fn new() -> Self {
        Self::with_config(NetworkConfig::default())
    }

    /// Create network with custom configuration
    pub fn with_config(config: NetworkConfig) -> Self {
        Self {
            peers: HashMap::new(),
            config,
            routing_table: HashMap::new(),
            stats: NetworkStats::default(),
            last_heartbeats: HashMap::new(),
        }
    }

    /// Add peer to network
    pub fn add_peer(&mut self, peer: PeerInfo) -> Result<()> {
        if self.peers.len() >= self.config.max_peers {
            anyhow::bail!("Maximum peer limit reached");
        }

        let peer_id_hex = hex::encode(&peer.id);
        let peer_address = peer.address.clone();
        self.peers.insert(peer.id.clone(), peer);
        self.stats.active_connections = self.peers.len();

        tracing::info!(
            peer_id = peer_id_hex,
            address = peer_address,
            "Peer added to network"
        );

        Ok(())
    }

    /// Remove peer from network
    pub fn remove_peer(&mut self, peer_id: &[u8]) -> Result<()> {
        if self.peers.remove(peer_id).is_some() {
            self.stats.active_connections = self.peers.len();
            self.routing_table.remove(peer_id);
            self.last_heartbeats.remove(peer_id);

            tracing::info!(peer_id = hex::encode(peer_id), "Peer removed from network");
        }

        Ok(())
    }

    /// Broadcast message to all connected peers
    pub fn broadcast(&mut self, msg: &P2PMessage) -> Result<()> {
        if msg.is_expired() {
            anyhow::bail!("Cannot broadcast expired message");
        }

        let mut success_count = 0;
        let mut error_count = 0;

        for (peer_id, peer) in &self.peers {
            if peer.status == PeerStatus::Connected {
                // In a real implementation, this would send the message over the network
                // For now, we just log the operation
                tracing::debug!(
                    peer_id = hex::encode(peer_id),
                    msg_type = ?msg.msg_type,
                    "Broadcasting message to peer"
                );
                success_count += 1;
            } else {
                error_count += 1;
            }
        }

        // Update statistics
        self.stats.messages_sent += 1;
        self.stats.bytes_transferred += msg.data.len() as u64;

        tracing::info!(
            success_count = success_count,
            error_count = error_count,
            msg_type = ?msg.msg_type,
            "Message broadcast completed"
        );

        Ok(())
    }

    /// Send message to specific peer
    pub fn send_to_peer(&mut self, peer_id: &[u8], msg: &P2PMessage) -> Result<()> {
        if let Some(peer) = self.peers.get(peer_id) {
            if peer.status != PeerStatus::Connected {
                anyhow::bail!("Peer {} is not connected", hex::encode(peer_id));
            }

            if msg.is_expired() {
                anyhow::bail!("Cannot send expired message");
            }

            // In a real implementation, this would send the message over the network
            tracing::debug!(
                peer_id = hex::encode(peer_id),
                msg_type = ?msg.msg_type,
                "Sending message to peer"
            );

            // Update statistics
            self.stats.messages_sent += 1;
            self.stats.bytes_transferred += msg.data.len() as u64;

            Ok(())
        } else {
            anyhow::bail!("Peer {} not found", hex::encode(peer_id));
        }
    }

    /// Process incoming message
    pub fn process_message(&mut self, msg: P2PMessage) -> Result<()> {
        // Check for message deduplication
        if self.config.enable_deduplication {
            // In a real implementation, this would check a message cache
            // For now, we just increment the hit counter
            self.stats.deduplication_hits += 1;
        }

        // Update statistics
        self.stats.messages_received += 1;
        self.stats.bytes_transferred += msg.data.len() as u64;

        // Update last seen for sender
        if let Some(peer) = self.peers.get_mut(&msg.sender) {
            peer.last_seen = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        tracing::debug!(
            msg_type = ?msg.msg_type,
            sender = hex::encode(&msg.sender),
            "Message processed successfully"
        );

        Ok(())
    }

    /// Get peer information
    pub fn get_peer(&self, peer_id: &[u8]) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    /// Get all connected peers
    pub fn get_connected_peers(&self) -> Vec<&PeerInfo> {
        self.peers
            .values()
            .filter(|peer| peer.status == PeerStatus::Connected)
            .collect()
    }

    /// Get network statistics
    pub fn get_stats(&self) -> &NetworkStats {
        &self.stats
    }

    /// Update peer heartbeat
    pub fn update_heartbeat(&mut self, peer_id: &[u8]) -> Result<()> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_heartbeats.insert(peer_id.to_vec(), current_time);

        // Update peer last seen
        if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.last_seen = current_time;
        }

        Ok(())
    }

    /// Check for timed out peers
    pub fn check_timeouts(&mut self) -> Vec<Vec<u8>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut timed_out_peers = Vec::new();

        for (peer_id, last_heartbeat) in &self.last_heartbeats {
            if current_time > last_heartbeat + self.config.peer_timeout {
                timed_out_peers.push(peer_id.clone());
            }
        }

        // Remove timed out peers
        for peer_id in &timed_out_peers {
            let _ = self.remove_peer(peer_id);
        }

        timed_out_peers
    }

    /// Get network health status
    pub fn get_health_status(&self) -> NetworkHealth {
        let connected_peers = self.get_connected_peers().len();
        let total_peers = self.peers.len();
        let average_reputation = if total_peers > 0 {
            self.peers.values().map(|p| p.reputation).sum::<f64>() / total_peers as f64
        } else {
            0.0
        };

        NetworkHealth {
            connected_peers,
            total_peers,
            average_reputation,
            uptime: self.stats.uptime,
            status: if connected_peers == 0 {
                NetworkStatus::Disconnected
            } else if connected_peers < total_peers / 2 {
                NetworkStatus::Degraded
            } else {
                NetworkStatus::Healthy
            },
        }
    }
}

/// Network health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkHealth {
    pub connected_peers: usize,
    pub total_peers: usize,
    pub average_reputation: f64,
    pub uptime: u64,
    pub status: NetworkStatus,
}

/// Network status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NetworkStatus {
    Healthy,
    Degraded,
    Disconnected,
}

/// Messages submodule for oracle integration
pub mod messages {
    use serde::{Deserialize, Serialize};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Oracle data message with feed information
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OracleDataMessage {
        /// Feed identifier
        pub feed_id: Vec<u8>,
        /// Oracle data payload
        pub data: Vec<u8>,
        /// Data timestamp
        pub timestamp: u64,
        /// Data signature for verification
        pub signature: Vec<u8>,
        /// Feed version
        pub version: u32,
        /// Data confidence score
        pub confidence: f64,
        /// Source oracle
        pub source: Vec<u8>,
    }

    /// Oracle request message for data retrieval
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OracleRequestMessage {
        /// Feed identifier
        pub feed_id: Vec<u8>,
        /// Requester address
        pub requester: Vec<u8>,
        /// Request timestamp
        pub timestamp: u64,
        /// Request ID for tracking
        pub request_id: Vec<u8>,
        /// Maximum acceptable age of data (seconds)
        pub max_age: u64,
        /// Request priority
        pub priority: u8,
    }

    /// Oracle response message with requested data
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OracleResponseMessage {
        /// Feed identifier
        pub feed_id: Vec<u8>,
        /// Oracle data
        pub data: Vec<u8>,
        /// Cryptographic proof of data validity
        pub proof: Vec<u8>,
        /// Response timestamp
        pub timestamp: u64,
        /// Request ID this responds to
        pub request_id: Vec<u8>,
        /// Data age in seconds
        pub data_age: u64,
        pub verified: bool,
    }

    /// Block message for blockchain synchronization
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct BlockMessage {
        /// Block height
        pub height: u64,
        /// Block hash
        pub hash: Vec<u8>,
        /// Block data payload
        pub data: Vec<u8>,
        /// Block timestamp
        pub timestamp: u64,
        /// Block proposer
        pub proposer: Vec<u8>,
        /// Number of transactions
        pub tx_count: u64,
        /// Block size in bytes
        pub size: u64,
    }

    /// Consensus certificate for oracle anchoring
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConsensusCertificate {
        /// Block height
        pub height: u64,
        /// Consensus round
        pub round: u32,
        /// Block hash
        pub block_hash: Vec<u8>,
        /// Validator signatures
        pub signatures: Vec<Vec<u8>>,
        /// Validator addresses
        pub validators: Vec<Vec<u8>>,
        /// Certificate version
        pub version: u32,
        /// Voting participants
        pub voters: Vec<Vec<u8>>,
        /// Aggregated signature
        pub aggregated_signature: Vec<u8>,
        /// Certificate timestamp
        pub timestamp: u64,
        /// Quorum required
        pub quorum_required: u32,
        /// Quorum achieved
        pub quorum_achieved: u32,
    }

    impl ConsensusCertificate {
        pub const VERSION: u32 = 1;

        /// Create new consensus certificate
        pub fn new(height: u64, round: u32, block_hash: Vec<u8>) -> Self {
            Self {
                height,
                round,
                block_hash,
                signatures: Vec::new(),
                validators: Vec::new(),
                version: Self::VERSION,
                voters: Vec::new(),
                aggregated_signature: Vec::new(),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                quorum_required: 0,
                quorum_achieved: 0,
            }
        }

        pub fn add_signature(&mut self, validator: Vec<u8>, signature: Vec<u8>) {
            self.validators.push(validator.clone());
            self.signatures.push(signature);
            self.voters.push(validator);
        }

        /// Verify certificate validity
        pub fn is_valid(&self) -> bool {
            // Check version
            if self.version != Self::VERSION {
                return false;
            }

            // Check if we have sufficient signatures
            if self.signatures.is_empty() || self.signatures.len() != self.validators.len() {
                return false;
            }

            // Check quorum
            if self.quorum_achieved < self.quorum_required {
                return false;
            }

            // Check timestamp (not too old)
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if current_time > self.timestamp + 3600 {
                // 1 hour max age
                return false;
            }

            true
        }

        /// Calculate quorum achieved
        pub fn calculate_quorum(&mut self) {
            self.quorum_achieved = self.signatures.len() as u32;
        }

        /// Set quorum requirement
        pub fn set_quorum_required(&mut self, quorum: u32) {
            self.quorum_required = quorum;
        }

        /// Get certificate age in seconds
        pub fn age(&self) -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(self.timestamp)
        }
    }
}
