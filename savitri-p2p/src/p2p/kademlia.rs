//! SECURITY (F-03): Real Kademlia DHT peer discovery
//!
//! Wraps libp2p's Kademlia implementation for decentralized peer discovery.
//! Provides bootstrap, peer lookup, record storage, and periodic random walks.

use libp2p::identity::Keypair;
use libp2p::kad::{self, store::MemoryStore, Mode, QueryResult, Record, RecordKey};
use libp2p::{Multiaddr, PeerId};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Protocol ID for Savitri Kademlia network
const PROTOCOL_ID: &str = "/savitri/kad/1.0.0";

/// Configuration for the Kademlia DHT
#[derive(Debug, Clone)]
pub struct KademliaConfig {
    /// Replication factor (k-bucket size), default 20
    pub replication_factor: usize,
    /// Number of concurrent disjoint paths for lookups
    pub disjoint_query_paths: usize,
    /// Interval between random walks for routing table maintenance
    pub random_walk_interval: Duration,
    /// Record TTL (how long records live in the DHT)
    pub record_ttl: Duration,
    /// Provider record TTL
    pub provider_record_ttl: Duration,
    /// Bootstrap nodes
    pub bootstrap_peers: Vec<(PeerId, Multiaddr)>,
}

impl Default for KademliaConfig {
    fn default() -> Self {
        Self {
            replication_factor: 20,
            disjoint_query_paths: 3,
            random_walk_interval: Duration::from_secs(300),
            record_ttl: Duration::from_secs(3600),
            provider_record_ttl: Duration::from_secs(3600),
            bootstrap_peers: Vec::new(),
        }
    }
}

/// Kademlia DHT manager
///
/// Manages the Kademlia DHT instance, handles bootstrapping, and provides
/// methods for peer discovery and record operations.
pub struct KademliaDht {
    /// libp2p Kademlia behaviour (to be embedded in a Swarm)
    behaviour: kad::Behaviour<MemoryStore>,
    /// Configuration
    config: KademliaConfig,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Tracked peers and their addresses
    known_peers: HashMap<PeerId, Vec<Multiaddr>>,
    /// Whether bootstrap has completed
    bootstrapped: bool,
    /// Last random walk time
    last_random_walk: Option<Instant>,
    /// Statistics
    stats: KademliaStats,
}

/// Kademlia statistics
#[derive(Debug, Clone, Default)]
pub struct KademliaStats {
    pub bootstrap_attempts: u64,
    pub bootstrap_successes: u64,
    pub queries_sent: u64,
    pub queries_completed: u64,
    pub peers_discovered: u64,
    pub records_stored: u64,
    pub records_retrieved: u64,
    pub random_walks_performed: u64,
    pub routing_table_size: usize,
}

impl KademliaDht {
    /// Create a new Kademlia DHT instance.
    ///
    /// # Arguments
    /// * `keypair` — The node's identity keypair for PeerId derivation.
    /// * `config` — Kademlia configuration.
    pub fn new(keypair: &Keypair, config: KademliaConfig) -> Self {
        let local_peer_id = PeerId::from(keypair.public());

        // Build the Kademlia store with the local peer ID
        let store = MemoryStore::new(local_peer_id);

        // Configure Kademlia behaviour
        let mut kad_config = kad::Config::new(
            libp2p::StreamProtocol::try_from_owned(PROTOCOL_ID.to_string())
                .expect("valid protocol id"),
        );
        kad_config.set_replication_factor(
            std::num::NonZeroUsize::new(config.replication_factor)
                .unwrap_or(std::num::NonZeroUsize::new(20).unwrap()),
        );
        kad_config.disjoint_query_paths(config.disjoint_query_paths > 0);
        kad_config.set_record_ttl(Some(config.record_ttl));
        kad_config.set_provider_record_ttl(Some(config.provider_record_ttl));

        let mut behaviour = kad::Behaviour::with_config(local_peer_id, store, kad_config);

        // Set server mode so this node responds to queries
        behaviour.set_mode(Some(Mode::Server));

        // Add bootstrap peers to the routing table
        for (peer_id, addr) in &config.bootstrap_peers {
            behaviour.add_address(peer_id, addr.clone());
            tracing::info!(
                "Added bootstrap peer {} at {} to Kademlia routing table",
                peer_id,
                addr
            );
        }

        Self {
            behaviour,
            config,
            local_peer_id,
            known_peers: HashMap::new(),
            bootstrapped: false,
            last_random_walk: None,
            stats: KademliaStats::default(),
        }
    }

    /// Get a reference to the underlying Kademlia behaviour (for Swarm integration).
    pub fn behaviour(&self) -> &kad::Behaviour<MemoryStore> {
        &self.behaviour
    }

    /// Get a mutable reference to the underlying Kademlia behaviour.
    pub fn behaviour_mut(&mut self) -> &mut kad::Behaviour<MemoryStore> {
        &mut self.behaviour
    }

    /// Start the bootstrap process.
    ///
    /// This initiates FIND_NODE queries to bootstrap peers to populate
    /// the routing table. Should be called after the Swarm is started.
    pub fn bootstrap(&mut self) -> Result<(), String> {
        self.stats.bootstrap_attempts += 1;

        if self.config.bootstrap_peers.is_empty() {
            tracing::warn!("No bootstrap peers configured — Kademlia will rely on mDNS or manual peer addition");
            return Ok(());
        }

        match self.behaviour.bootstrap() {
            Ok(_query_id) => {
                tracing::info!(
                    "Kademlia bootstrap started with {} peers",
                    self.config.bootstrap_peers.len()
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!("Kademlia bootstrap failed: {:?}", e);
                Err(format!("Bootstrap failed: {:?}", e))
            }
        }
    }

    /// Add a peer's address to the routing table.
    pub fn add_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
        self.behaviour.add_address(&peer_id, addr.clone());
        self.known_peers.entry(peer_id).or_default().push(addr);
        self.stats.peers_discovered += 1;
    }

    /// Initiate a peer lookup (FIND_NODE) for the given PeerId.
    pub fn find_peer(&mut self, peer_id: PeerId) {
        self.behaviour.get_closest_peers(peer_id);
        self.stats.queries_sent += 1;
        tracing::debug!("Initiated FIND_NODE query for {}", peer_id);
    }

    /// Store a record in the DHT (PUT_VALUE).
    pub fn put_record(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), String> {
        let record = Record {
            key: RecordKey::new(&key),
            value,
            publisher: Some(self.local_peer_id),
            expires: Some(Instant::now() + self.config.record_ttl),
        };

        self.behaviour
            .put_record(record, kad::Quorum::Majority)
            .map_err(|e| format!("Failed to store record: {:?}", e))?;

        self.stats.records_stored += 1;
        Ok(())
    }

    /// Retrieve a record from the DHT (GET_VALUE).
    pub fn get_record(&mut self, key: Vec<u8>) {
        self.behaviour.get_record(RecordKey::new(&key));
        self.stats.queries_sent += 1;
    }

    /// Perform a random walk for routing table maintenance.
    ///
    /// Generates a random PeerId and performs a FIND_NODE for it,
    /// which populates the routing table with peers along the path.
    pub fn random_walk(&mut self) {
        let random_peer = PeerId::random();
        self.behaviour.get_closest_peers(random_peer);
        self.stats.random_walks_performed += 1;
        self.last_random_walk = Some(Instant::now());
        tracing::debug!("Kademlia random walk initiated");
    }

    /// Check if a random walk is due based on the configured interval.
    pub fn should_random_walk(&self) -> bool {
        match self.last_random_walk {
            None => true,
            Some(last) => last.elapsed() >= self.config.random_walk_interval,
        }
    }

    /// Process a Kademlia query result event.
    ///
    /// Call this from the Swarm event loop when a KademliaEvent is received.
    pub fn handle_event(&mut self, event: kad::Event) {
        match event {
            kad::Event::OutboundQueryProgressed { result, .. } => {
                self.stats.queries_completed += 1;
                match result {
                    QueryResult::Bootstrap(Ok(ok)) => {
                        if ok.num_remaining == 0 {
                            self.bootstrapped = true;
                            self.stats.bootstrap_successes += 1;
                            tracing::info!("Kademlia bootstrap completed successfully");
                        }
                    }
                    QueryResult::Bootstrap(Err(e)) => {
                        tracing::warn!("Kademlia bootstrap error: {:?}", e);
                    }
                    QueryResult::GetClosestPeers(Ok(ok)) => {
                        tracing::info!("Kademlia FIND_NODE found {} peers", ok.peers.len());
                        for peer_info in &ok.peers {
                            self.known_peers.entry(peer_info.peer_id).or_default();
                        }
                    }
                    QueryResult::GetClosestPeers(Err(e)) => {
                        tracing::debug!("Kademlia FIND_NODE error: {:?}", e);
                    }
                    QueryResult::GetRecord(Ok(ok)) => {
                        self.stats.records_retrieved += 1;
                        tracing::debug!("Kademlia GET_VALUE returned record: {:?}", ok);
                    }
                    QueryResult::GetRecord(Err(e)) => {
                        tracing::debug!("Kademlia GET_VALUE error: {:?}", e);
                    }
                    QueryResult::PutRecord(Ok(_)) => {
                        tracing::debug!("Kademlia PUT_VALUE succeeded");
                    }
                    QueryResult::PutRecord(Err(e)) => {
                        tracing::warn!("Kademlia PUT_VALUE error: {:?}", e);
                    }
                    _ => {}
                }
            }
            kad::Event::RoutingUpdated {
                peer, addresses, ..
            } => {
                let addrs: Vec<Multiaddr> = addresses.iter().cloned().collect();
                self.known_peers.insert(peer, addrs);
                self.stats.routing_table_size = self.known_peers.len();
                tracing::debug!("Kademlia routing table updated — peer {}", peer);
            }
            kad::Event::InboundRequest { request } => {
                tracing::debug!("Kademlia inbound request: {:?}", request);
            }
            _ => {}
        }
    }

    /// Whether the bootstrap process has completed.
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }

    /// Get the local peer ID.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    /// Get all known peers.
    pub fn known_peers(&self) -> &HashMap<PeerId, Vec<Multiaddr>> {
        &self.known_peers
    }

    /// Get statistics.
    pub fn stats(&self) -> &KademliaStats {
        &self.stats
    }
}
