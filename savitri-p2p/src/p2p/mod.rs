//! Savitri P2P Module - Production libp2p Implementation
//!
//! This module provides the real P2P networking stack using libp2p.
//! It includes network management, peer discovery, gossipsub, and protocol handling.
//!
//! ## Architecture
//! - `network`: Core networking with libp2p Swarm, transport, and connection management
//! - `discovery`: Peer discovery using mDNS, DNS, and Kademlia DHT
//! - `gossip`: Gossipsub pubsub protocol for message broadcasting
//! - `protocols`: Custom protocol handling and request-response patterns
//! - `messages`: Message types, routing, and serialization
//!
//! ## Usage
//! For production, always use `with_keypair()` constructors to ensure stable node identity.

pub mod constants;

// Real libp2p implementations
pub mod discovery;
pub mod gossip;
pub mod kademlia;
pub mod messages;
pub mod network;
pub mod protocols;
pub mod secure_transport;

// Re-export constants and safety limits
pub use constants::{
    CacheEvictionPolicy, GossipConfig as ConstantsGossipConfig, GossipSafety, MemoryUsage,
    NetworkState, GOSSIP_CACHE_MAX_AGE, GOSSIP_CACHE_MAX_SIZE, GOSSIP_CLEANUP_INTERVAL,
    GOSSIP_MAX_HEIGHT_DIFF, GOSSIP_MAX_MESSAGES_PER_ROUND, GOSSIP_MAX_ROUNDS,
    NETWORK_MIN_SUCCESS_RATE, PEER_INACTIVITY_TIMEOUT, PEER_MAX_CONNECTIONS,
};

// Re-export main types from libp2p-based implementations
pub use network::{
    BehaviourEvent, NetworkBehaviour, NetworkConfig, NetworkEvent, NetworkManager, NetworkStats,
};

pub use discovery::{DiscoveryConfig, DiscoveryEvent, DiscoveryManager, DiscoveryStats, PeerInfo};

pub use gossip::{
    GossipConfig, GossipEvent, GossipManager, GossipMessage, GossipStats, TopicManager,
};

pub use protocols::{ProtocolConfig, ProtocolEvent, ProtocolHandler, ProtocolStats};

pub use messages::{Message, MessagePriority, MessageRoutingConfig, MessageType};

/// P2P configuration combining all sub-configurations
#[derive(Debug, Clone)]
pub struct P2PConfig {
    pub network: NetworkConfig,
    pub protocols: ProtocolConfig,
    pub discovery: DiscoveryConfig,
    pub gossip: crate::p2p::gossip::GossipConfig,
}

impl Default for P2PConfig {
    fn default() -> Self {
        Self {
            network: NetworkConfig::default(),
            protocols: ProtocolConfig::default(),
            discovery: DiscoveryConfig::default(),
            gossip: crate::p2p::gossip::GossipConfig::default(),
        }
    }
}

/// Main P2P manager combining all functionality using real libp2p stack
#[allow(dead_code)]
pub struct P2PManager {
    #[allow(dead_code)]
    config: P2PConfig,
    /// Real libp2p network manager with Swarm
    network_manager: NetworkManager,
    /// Real libp2p discovery manager (mDNS + DNS + Kademlia)
    discovery_manager: DiscoveryManager,
    /// Real libp2p gossipsub manager
    gossip_manager: GossipManager,
    /// Node identity keypair for stable identity
    #[allow(dead_code)]
    keypair: Option<libp2p::identity::Keypair>,
    #[allow(dead_code)]
    stats: P2PStats,
    event_sender: tokio::sync::mpsc::UnboundedSender<P2PEvent>,
    event_receiver: Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>>,
    /// Start time for uptime tracking
    start_time: Option<std::time::Instant>,
}

/// P2P statistics combining all subsystem stats
#[derive(Debug, Clone, Default)]
pub struct P2PStats {
    pub network: NetworkStats,
    pub discovery: DiscoveryStats,
    pub gossip: GossipStats,
    pub uptime: std::time::Duration,
}

/// P2P events
#[derive(Debug, Clone)]
pub enum P2PEvent {
    Network(NetworkEvent),
    Discovery(DiscoveryEvent),
    Gossip(GossipEvent),
    PeerConnected { peer_id: String },
    PeerDisconnected { peer_id: String },
    MessageReceived { message_id: String, from: String },
    MessageSent { message_id: String, to: String },
}

impl P2PManager {
    /// Create a new P2P manager without a keypair (not recommended for production).
    /// Use `with_keypair()` for production to ensure stable node identity.
    pub fn new(config: P2PConfig) -> anyhow::Result<Self> {
        Self::with_keypair(config, None)
    }

    /// Create a new P2P manager with a specific keypair for stable identity.
    /// This is the recommended constructor for production use.
    pub fn with_keypair(
        config: P2PConfig,
        keypair: Option<libp2p::identity::Keypair>,
    ) -> anyhow::Result<Self> {
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel();

        // Create network manager with optional keypair
        let keypair_str = keypair
            .as_ref()
            .map(|k| format!("{:?}", k))
            .unwrap_or_default();
        let network_manager =
            NetworkManager::with_keypair(config.network.clone(), keypair_str.clone())?;

        // Create discovery manager
        let discovery_manager = DiscoveryManager::new(config.discovery.clone());

        // Create gossip manager with optional keypair for stable message signing
        let gossip_manager = GossipManager::with_keypair(config.gossip.clone(), keypair_str);

        Ok(Self {
            config,
            network_manager,
            discovery_manager,
            gossip_manager,
            keypair,
            stats: P2PStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            start_time: None, // Will be set when start() is called
        })
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        tracing::info!("Starting P2P manager with real libp2p stack");

        // Set start time for uptime tracking
        self.start_time = Some(std::time::Instant::now());

        // Start network manager (creates Swarm, starts listening)
        self.network_manager
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        // Start discovery manager (mDNS, DNS, peer exchange)
        self.discovery_manager.start().await?;

        // Start gossip manager
        self.gossip_manager.start().await?;

        tracing::info!("P2P manager started successfully");
        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        tracing::info!("Stopping P2P manager");

        // Stop gossip manager
        self.gossip_manager.stop().await?;

        // Stop discovery manager
        self.discovery_manager.stop().await?;

        // Stop network manager
        self.network_manager
            .stop()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        tracing::info!("P2P manager stopped");
        Ok(())
    }

    /// Broadcast a message to a gossipsub topic
    pub async fn broadcast(&mut self, topic: &str, data: Vec<u8>) -> anyhow::Result<()> {
        self.gossip_manager.broadcast(topic, data).await
    }

    /// Subscribe to a gossipsub topic
    pub async fn subscribe(&mut self, topic: &str) -> anyhow::Result<()> {
        self.gossip_manager.subscribe(topic).await
    }

    /// Get connected peers from the network manager
    pub async fn connected_peers(&self) -> Vec<String> {
        self.network_manager.connected_peers().await
    }

    /// Get discovered peers from the discovery manager
    pub fn discovered_peers(&self) -> Vec<String> {
        self.discovery_manager.get_all_peers()
    }

    /// Get network statistics
    pub fn get_stats(&self) -> P2PStats {
        let uptime = if let Some(start_time) = self.start_time {
            start_time.elapsed()
        } else {
            std::time::Duration::from_secs(0)
        };

        P2PStats {
            network: self.network_manager.get_stats(),
            discovery: self.discovery_manager.get_stats(),
            gossip: self.gossip_manager.get_stats(),
            uptime,
        }
    }

    /// Initialize masternode-specific topics
    pub async fn initialize_topics(&mut self) -> anyhow::Result<()> {
        tracing::info!("Initializing masternode P2P topics");

        // Subscribe to masternode-specific gossipsub topics
        let masternode_topics = [
            "/savitri/masternode/group/proposal/1",
            "/savitri/masternode/group/vote/1",
            "/savitri/masternode/group/sync/1",
            "/savitri/masternode/consensus/1",
            "/savitri/masternode/heartbeat/1",
            "/savitri/masternode/pou/1",
            "/savitri/masternode/peerinfo/1",
            "/savitri/masternode/block/1",
        ];

        for topic in masternode_topics {
            self.subscribe(topic).await?;
            tracing::debug!("Subscribed to masternode topic: {}", topic);
        }

        tracing::info!("Masternode P2P topics initialized successfully");
        Ok(())
    }

    /// Get local peer ID
    pub fn local_peer_id(&self) -> String {
        self.network_manager.local_peer_id()
    }

    /// Dial a peer by address
    pub async fn dial(&mut self, addr: String) -> anyhow::Result<()> {
        self.network_manager
            .dial(addr)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }

    pub fn take_event_receiver(
        &mut self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<P2PEvent>> {
        self.event_receiver.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_p2p_manager_creation() {
        let config = P2PConfig::default();
        let manager = P2PManager::new(config);
        assert!(manager.is_ok());
    }

    #[test]
    fn test_p2p_config_default() {
        let config = P2PConfig::default();

        // Test that all sub-configs are properly initialized
        assert_eq!(config.protocols.name, "default");
        assert_eq!(config.discovery.max_peers, 100);
    }

    #[test]
    fn test_p2p_stats_default() {
        let stats = P2PStats::default();
        assert_eq!(stats.uptime, std::time::Duration::from_secs(0));
    }
}
