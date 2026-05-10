//! Fixed P2P Discovery Module
//!
//! Conservative implementation compatible with Savitri architecture

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Discovery manager for peer discovery
pub struct DiscoveryManager {
    peers: HashMap<String, PeerInfo>,
    #[allow(dead_code)]
    config: DiscoveryConfig,
}

impl DiscoveryManager {
    pub fn new(config: DiscoveryConfig) -> Self {
        Self {
            peers: HashMap::new(),
            config,
        }
    }

    pub fn get_all_peers(&self) -> Vec<String> {
        self.peers.keys().cloned().collect()
    }

    pub fn get_stats(&self) -> DiscoveryStats {
        DiscoveryStats {
            peers_discovered: self.peers.len() as u64,
            active_peers: self.peers.len(),
            peers_lost: 0,
            mdns_queries_sent: 0,
            mdns_responses_received: 0,
            kademlia_queries_sent: 0,
            kademlia_responses_received: 0,
            average_peer_reputation: 50.0,
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("Starting P2P discovery manager");

        // Start discovery interval task
        let config = self.config.clone();
        let mut peers = self.peers.clone();
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();

        // Start bootstrap connections
        if !config.bootstrap_nodes.is_empty() {
            for bootstrap_node in &config.bootstrap_nodes {
                info!("Connecting to bootstrap node: {}", bootstrap_node);
                self.connect_to_peer(bootstrap_node).await?;
            }
        }

        // Start periodic discovery
        let event_sender_clone = event_sender.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.discovery_interval);
            loop {
                interval.tick().await;

                // Perform peer discovery
                if let Err(e) =
                    Self::perform_discovery_cycle(&mut peers, &config, &event_sender_clone).await
                {
                    error!("Discovery cycle failed: {}", e);
                }
            }
        });

        // Start event processing
        tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                // Handle discovery events
                tracing::info!("Discovery event: {:?}", event);
                // In a real implementation, this would update peer state and notify other components
            }
        });

        info!("P2P discovery manager started successfully");
        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("Stopping P2P discovery manager");

        // Clean up all peer connections
        let peer_ids: Vec<String> = self.peers.keys().cloned().collect();
        for peer_id in peer_ids {
            if let Err(e) = self.disconnect_peer(&peer_id).await {
                warn!("Failed to disconnect from peer {}: {}", peer_id, e);
            }
        }

        // Clear peers
        self.peers.clear();

        info!("P2P discovery manager stopped successfully");
        Ok(())
    }

    /// Connect to a peer
    async fn connect_to_peer(&mut self, peer_address: &str) -> anyhow::Result<()> {
        let peer_id = format!(
            "peer_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        let peer_info = PeerInfo::new(peer_id.clone(), peer_address.to_string());

        // In a real implementation, this would establish actual network connection
        // For now, we'll simulate the connection
        info!("Connected to peer {} at {}", peer_id, peer_address);
        self.peers.insert(peer_id, peer_info);
        Ok(())
    }

    /// Disconnect from a peer
    async fn disconnect_peer(&mut self, peer_id: &str) -> anyhow::Result<()> {
        if let Some(peer_info) = self.peers.remove(peer_id) {
            info!(
                "Disconnected from peer {} ({})",
                peer_id,
                peer_info.addresses.join(", ")
            );
        } else {
            warn!("Peer {} not found for disconnection", peer_id);
        }
        Ok(())
    }

    /// Perform a single discovery cycle
    async fn perform_discovery_cycle(
        peers: &mut HashMap<String, PeerInfo>,
        config: &DiscoveryConfig,
        event_sender: &mpsc::UnboundedSender<DiscoveryEvent>,
    ) -> anyhow::Result<()> {
        // Perform mDNS discovery if enabled
        if config.enable_mdns {
            Self::perform_mdns_discovery(peers, config, event_sender).await?;
        }

        // Perform Kademlia discovery if enabled
        if config.enable_kademlia {
            Self::perform_kademlia_discovery(peers, config, event_sender).await?;
        }

        // Perform peer health checks
        Self::perform_health_checks(peers, config, event_sender).await?;

        Ok(())
    }

    /// Perform mDNS discovery
    async fn perform_mdns_discovery(
        peers: &mut HashMap<String, PeerInfo>,
        _config: &DiscoveryConfig,
        event_sender: &mpsc::UnboundedSender<DiscoveryEvent>,
    ) -> anyhow::Result<()> {
        // In a real implementation, this would send mDNS queries
        // For now, we'll simulate mDNS discovery

        let query = "_p2p._tcp.local";
        let _ = event_sender.send(DiscoveryEvent::MdnsQuerySent {
            query: query.to_string(),
        });

        // Simulate receiving mDNS responses
        for (peer_id, peer_info) in peers.iter() {
            if peer_info
                .addresses
                .iter()
                .any(|addr| addr.contains("local"))
            {
                let _ = event_sender.send(DiscoveryEvent::MdnsResponseReceived {
                    peer: peer_id.clone(),
                });
            }
        }

        Ok(())
    }

    /// Perform Kademlia discovery
    async fn perform_kademlia_discovery(
        peers: &mut HashMap<String, PeerInfo>,
        config: &DiscoveryConfig,
        event_sender: &mpsc::UnboundedSender<DiscoveryEvent>,
    ) -> anyhow::Result<()> {
        // In a real implementation, this would perform Kademlia DHT operations
        // For now, we'll simulate Kademlia discovery

        let key = "node_info";
        let _ = event_sender.send(DiscoveryEvent::KademliaQuerySent {
            key: key.to_string(),
        });

        // Simulate receiving Kademlia responses
        let discovered_peers: Vec<String> = peers
            .keys()
            .take(config.kademlia_replication_factor as usize)
            .cloned()
            .collect();
        let _ = event_sender.send(DiscoveryEvent::KademliaResponseReceived {
            peers: discovered_peers,
        });

        Ok(())
    }

    /// Perform health checks on all peers
    async fn perform_health_checks(
        peers: &mut HashMap<String, PeerInfo>,
        config: &DiscoveryConfig,
        event_sender: &mpsc::UnboundedSender<DiscoveryEvent>,
    ) -> anyhow::Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut peers_to_remove = Vec::new();
        let peer_ids: Vec<_> = peers.keys().cloned().collect();

        for peer_id in peer_ids {
            let peer_info = peers.get(&peer_id).unwrap();
            let time_since_last_seen = current_time.saturating_sub(peer_info.last_seen);

            // Remove peer if it's been too long since last seen
            if time_since_last_seen > config.peer_timeout.as_secs() {
                peers_to_remove.push(peer_id.clone());
                let _ = event_sender.send(DiscoveryEvent::PeerLost(peer_id.clone()));
            } else {
                // Update last seen time
                let peer_info = peers.get_mut(&peer_id).unwrap();
                peer_info.last_seen = current_time;
                let _ = event_sender.send(DiscoveryEvent::PeerUpdated {
                    peer: peer_info.clone(),
                });
            }
        }

        // Remove timed-out peers
        for peer_id in peers_to_remove {
            peers.remove(&peer_id);
        }

        Ok(())
    }
}

/// Discovery configuration
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub bootstrap_nodes: Vec<String>,
    pub discovery_interval: Duration,
    pub enable_kademlia: bool,
    pub enable_relay: bool,
    pub enable_mdns: bool,
    pub mdns_ttl: Duration,
    pub kademlia_replication_factor: u16,
    pub max_peers: usize,
    pub peer_timeout: Duration,
    pub bootstrap_peers: Vec<String>,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            bootstrap_nodes: vec![],
            discovery_interval: Duration::from_secs(30),
            enable_kademlia: true,
            enable_relay: false,
            enable_mdns: true,
            mdns_ttl: Duration::from_secs(300),
            kademlia_replication_factor: 20,
            max_peers: 50,
            peer_timeout: Duration::from_secs(600),
            bootstrap_peers: vec![],
        }
    }
}

/// Discovery statistics
#[derive(Debug, Clone, Default)]
pub struct DiscoveryStats {
    pub peers_discovered: u64,
    pub active_peers: usize,
    pub peers_lost: u64,
    pub mdns_queries_sent: u64,
    pub mdns_responses_received: u64,
    pub kademlia_queries_sent: u64,
    pub kademlia_responses_received: u64,
    pub average_peer_reputation: f64,
}

/// Discovery events
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    PeerDiscovered(PeerInfo),
    PeerLost(String),
    PeerUpdated { peer: PeerInfo },
    MdnsQuerySent { query: String },
    MdnsResponseReceived { peer: String },
    KademliaQuerySent { key: String },
    KademliaResponseReceived { peers: Vec<String> },
    BootstrapCompleted { peers_connected: usize },
}

/// Peer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub addresses: Vec<String>,
    pub last_seen: u64,
    pub reputation: f64,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, String>,
}

impl PeerInfo {
    pub fn new(id: String, address: String) -> Self {
        Self {
            id,
            addresses: vec![address],
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            reputation: 50.0, // Default neutral reputation
            capabilities: vec![],
            metadata: HashMap::new(),
        }
    }

    pub fn add_address(&mut self, address: String) {
        if !self.addresses.contains(&address) {
            self.addresses.push(address);
        }
    }

    pub fn update_last_seen(&mut self) {
        self.last_seen = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn is_stale(&self, timeout: Duration) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now > self.last_seen + timeout.as_secs()
    }

    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.contains(&capability.to_string())
    }

    pub fn add_capability(&mut self, capability: String) {
        if !self.has_capability(&capability) {
            self.capabilities.push(capability);
        }
    }
}

/// Peer discovery manager
pub struct PeerDiscovery {
    config: DiscoveryConfig,
    peers: HashMap<String, PeerInfo>,
    stats: DiscoveryStats,
    event_sender: mpsc::UnboundedSender<DiscoveryEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<DiscoveryEvent>>,
    local_peer_id: String,
}

impl PeerDiscovery {
    pub fn new(config: DiscoveryConfig, local_peer_id: String) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self {
            config,
            peers: HashMap::new(),
            stats: DiscoveryStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            local_peer_id,
        }
    }

    pub fn add_peer(
        &mut self,
        peer: PeerInfo,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if peer.id == self.local_peer_id {
            return Err("Cannot add local peer as discovered peer".into());
        }

        if self.peers.len() >= self.config.max_peers {
            return Err("Maximum peer limit reached".into());
        }

        let is_new = !self.peers.contains_key(&peer.id);

        self.peers.insert(peer.id.clone(), peer.clone());

        if is_new {
            self.stats.peers_discovered += 1;

            // Send event
            let _ = self
                .event_sender
                .send(DiscoveryEvent::PeerDiscovered(peer.clone()));

            tracing::info!(
                "Discovered new peer: {} with {} addresses",
                peer.id,
                peer.addresses.len()
            );
        } else {
            // Send update event
            let _ = self
                .event_sender
                .send(DiscoveryEvent::PeerUpdated { peer: peer.clone() });

            tracing::debug!("Updated peer: {}", peer.id);
        }

        self.update_stats();
        Ok(())
    }

    pub fn remove_peer(&mut self, peer_id: &str) -> Option<PeerInfo> {
        if let Some(peer) = self.peers.remove(peer_id) {
            self.stats.peers_lost += 1;

            // Send event
            let _ = self
                .event_sender
                .send(DiscoveryEvent::PeerLost(peer_id.to_string()));

            tracing::info!("Removed peer: {}", peer_id);
            self.update_stats();
            Some(peer)
        } else {
            None
        }
    }

    pub fn get_peer(&self, peer_id: &str) -> Option<&PeerInfo> {
        self.peers.get(peer_id)
    }

    pub fn get_all_peers(&self) -> Vec<&PeerInfo> {
        self.peers.values().collect()
    }

    pub fn get_peers_by_capability(&self, capability: &str) -> Vec<&PeerInfo> {
        self.peers
            .values()
            .filter(|peer| peer.has_capability(capability))
            .collect()
    }

    pub fn update_peer_reputation(
        &mut self,
        peer_id: &str,
        delta: f64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let peer_reputation = if let Some(peer) = self.peers.get_mut(peer_id) {
            peer.reputation = (peer.reputation + delta).clamp(0.0, 100.0);
            peer.update_last_seen();

            // Clone peer data for event before releasing mutable borrow
            let _peer_clone = peer.clone();
            peer.reputation
        } else {
            return Err(format!("Peer {} not found", peer_id).into());
        };

        // Send update event
        let _ = self.event_sender.send(DiscoveryEvent::PeerUpdated {
            peer: self.peers.get(peer_id).unwrap().clone(),
        });

        self.update_stats();
        tracing::debug!("Updated reputation for {}: {:.2}", peer_id, peer_reputation);
        Ok(())
    }

    pub async fn start_discovery(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting peer discovery with config: {:?}", self.config);

        // Add bootstrap peers
        let bootstrap_peers = self.config.bootstrap_peers.clone();
        for bootstrap_peer in &bootstrap_peers {
            let peer_info =
                PeerInfo::new(bootstrap_peer.clone(), format!("{}:8333", bootstrap_peer));
            let _ = self.add_peer(peer_info);
        }

        // Start mDNS if enabled
        if self.config.enable_mdns {
            self.start_mdns_discovery().await?;
        }

        // Start Kademlia if enabled
        if self.config.enable_kademlia {
            self.start_kademlia_discovery().await?;
        }

        tracing::info!("Discovery started successfully");
        Ok(())
    }

    async fn start_mdns_discovery(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting mDNS discovery");

        // Simulate mDNS discovery
        let query = "savitri-p2p._tcp.local".to_string();
        self.stats.mdns_queries_sent += 1;

        // Send event
        let _ = self.event_sender.send(DiscoveryEvent::MdnsQuerySent {
            query: query.clone(),
        });

        // Simulate finding peers via mDNS
        tokio::time::sleep(Duration::from_millis(500)).await;

        for i in 1..=3 {
            let peer_id = format!("mdns_peer_{}", i);
            let mut peer_info = PeerInfo::new(peer_id.clone(), format!("192.168.1.{}", 100 + i));
            peer_info.add_capability("mdns".to_string());

            if self.add_peer(peer_info).is_ok() {
                self.stats.mdns_responses_received += 1;

                // Send event
                let _ = self
                    .event_sender
                    .send(DiscoveryEvent::MdnsResponseReceived { peer: peer_id });
            }
        }

        tracing::info!("mDNS discovery completed");
        Ok(())
    }

    async fn start_kademlia_discovery(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting Kademlia discovery");

        // Simulate Kademlia DHT lookup
        let key = "savitri-network-peers".to_string();
        self.stats.kademlia_queries_sent += 1;

        // Send event
        let _ = self
            .event_sender
            .send(DiscoveryEvent::KademliaQuerySent { key: key.clone() });

        // Simulate finding peers via Kademlia
        tokio::time::sleep(Duration::from_millis(800)).await;

        let mut discovered_peers = Vec::new();
        for i in 1..=5 {
            let peer_id = format!("kad_peer_{}", i);
            let mut peer_info = PeerInfo::new(peer_id.clone(), format!("10.0.0.{}", 50 + i));
            peer_info.add_capability("kademlia".to_string());

            if self.add_peer(peer_info).is_ok() {
                discovered_peers.push(peer_id.clone());
            }
        }

        self.stats.kademlia_responses_received += discovered_peers.len() as u64;

        // Send event
        let _ = self
            .event_sender
            .send(DiscoveryEvent::KademliaResponseReceived {
                peers: discovered_peers,
            });

        tracing::info!("Kademlia discovery completed");
        Ok(())
    }

    pub fn cleanup_stale_peers(&mut self) {
        let stale_peers: Vec<String> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.is_stale(self.config.peer_timeout))
            .map(|(id, _)| id.clone())
            .collect();

        for peer_id in stale_peers {
            tracing::warn!("Removing stale peer: {}", peer_id);
            self.remove_peer(&peer_id);
        }
    }

    pub fn get_best_peers(&self, count: usize, capability: Option<&str>) -> Vec<&PeerInfo> {
        let mut peers: Vec<&PeerInfo> = if let Some(cap) = capability {
            self.get_peers_by_capability(cap)
        } else {
            self.get_all_peers()
        };

        // Sort by reputation (descending)
        peers.sort_by(|a, b| {
            b.reputation
                .partial_cmp(&a.reputation)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Return top N peers
        peers.into_iter().take(count).collect()
    }

    fn update_stats(&mut self) {
        self.stats.active_peers = self.peers.len();

        if !self.peers.is_empty() {
            let total_reputation: f64 = self.peers.values().map(|peer| peer.reputation).sum();
            self.stats.average_peer_reputation = total_reputation / self.peers.len() as f64;
        } else {
            self.stats.average_peer_reputation = 0.0;
        }
    }

    pub fn get_stats(&self) -> DiscoveryStats {
        self.stats.clone()
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<DiscoveryEvent>> {
        self.event_receiver.take()
    }
}

impl Default for PeerDiscovery {
    fn default() -> Self {
        Self::new(DiscoveryConfig::default(), "local_peer".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_info_creation() {
        let peer = PeerInfo::new("peer1".to_string(), "127.0.0.1:8333".to_string());

        assert_eq!(peer.id, "peer1");
        assert_eq!(peer.addresses, vec!["127.0.0.1:8333"]);
        assert_eq!(peer.reputation, 50.0);
        assert!(!peer.is_stale(Duration::from_secs(1)));
    }

    #[test]
    fn test_peer_info_capabilities() {
        let mut peer = PeerInfo::new("peer1".to_string(), "127.0.0.1:8333".to_string());

        assert!(!peer.has_capability("consensus"));

        peer.add_capability("consensus".to_string());
        assert!(peer.has_capability("consensus"));

        // Adding duplicate capability should not create duplicates
        peer.add_capability("consensus".to_string());
        assert_eq!(peer.capabilities.len(), 1);
    }

    #[test]
    fn test_discovery_config_default() {
        let config = DiscoveryConfig::default();

        assert!(config.enable_mdns);
        assert!(config.enable_kademlia);
        assert!(!config.enable_relay);
        assert_eq!(config.max_peers, 50);
        assert_eq!(config.kademlia_replication_factor, 20);
    }

    #[tokio::test]
    async fn test_peer_discovery() {
        let config = DiscoveryConfig::default();
        let mut discovery = PeerDiscovery::new(config, "local_peer".to_string());

        // Add a peer
        let peer = PeerInfo::new("peer1".to_string(), "127.0.0.1:8333".to_string());
        assert!(discovery.add_peer(peer).is_ok());

        // Check peer exists
        assert!(discovery.get_peer("peer1").is_some());
        assert_eq!(discovery.get_all_peers().len(), 1);

        // Update reputation
        assert!(discovery.update_peer_reputation("peer1", 10.0).is_ok());
        let updated_peer = discovery.get_peer("peer1").unwrap();
        assert_eq!(updated_peer.reputation, 60.0);

        // Remove peer
        assert!(discovery.remove_peer("peer1").is_some());
        assert!(discovery.get_peer("peer1").is_none());
    }

    #[tokio::test]
    async fn test_discovery_start() {
        let mut config = DiscoveryConfig::default();
        config.bootstrap_peers = vec!["bootstrap1".to_string(), "bootstrap2".to_string()];

        let mut discovery = PeerDiscovery::new(config, "local_peer".to_string());

        // Start discovery
        assert!(discovery.start_discovery().await.is_ok());

        // Should have bootstrap peers + discovered peers
        assert!(discovery.get_all_peers().len() > 2);

        // Check stats
        let stats = discovery.get_stats();
        assert!(stats.peers_discovered > 0);
        assert!(stats.mdns_queries_sent > 0);
        assert!(stats.kademlia_queries_sent > 0);
    }

    #[test]
    fn test_peer_selection() {
        let mut discovery =
            PeerDiscovery::new(DiscoveryConfig::default(), "local_peer".to_string());

        // Add peers with different reputations
        for i in 1..=5 {
            let mut peer = PeerInfo::new(format!("peer{}", i), format!("127.0.0.1:{}", 8333 + i));
            peer.reputation = (i as f64) * 10.0; // peer1: 10, peer2: 20, etc.

            if i % 2 == 0 {
                peer.add_capability("consensus".to_string());
            }

            discovery.add_peer(peer).unwrap();
        }

        // Get best peers (should be sorted by reputation)
        let best_peers = discovery.get_best_peers(3, None);
        assert_eq!(best_peers.len(), 3);
        assert_eq!(best_peers[0].id, "peer5"); // Highest reputation
        assert_eq!(best_peers[1].id, "peer4");
        assert_eq!(best_peers[2].id, "peer3");

        // Get best peers with consensus capability
        let consensus_peers = discovery.get_best_peers(2, Some("consensus"));
        assert_eq!(consensus_peers.len(), 2);
        assert_eq!(consensus_peers[0].id, "peer4"); // peer4 (40) > peer2 (20)
        assert_eq!(consensus_peers[1].id, "peer2");
    }

    #[tokio::test]
    async fn test_stale_peer_cleanup() {
        let config = DiscoveryConfig {
            peer_timeout: Duration::from_millis(100),
            ..Default::default()
        };

        let mut discovery = PeerDiscovery::new(config, "local_peer".to_string());

        // Add a peer
        let peer = PeerInfo::new("peer1".to_string(), "127.0.0.1:8333".to_string());
        discovery.add_peer(peer).unwrap();

        assert_eq!(discovery.get_all_peers().len(), 1);

        // Wait for peer to become stale
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Cleanup stale peers
        discovery.cleanup_stale_peers();

        assert_eq!(discovery.get_all_peers().len(), 0);
    }
}
