//! Production-Grade Gossip Module with Structural Invariants
//!
//! This module implements gossip protocol with safety limits and memory management
//! to prevent exhaustion attacks and ensure production safety.

use crate::p2p::{
    constants::{CacheEvictionPolicy, GossipSafety, MemoryUsage, NetworkState},
    messages::Message,
};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Gossip manager for message propagation
pub struct GossipManager {
    #[allow(dead_code)]
    config: GossipConfig,
    #[allow(dead_code)]
    message_cache: HashMap<String, GossipMessage>,
    topics: HashMap<String, TopicManager>,
    /// SECURITY (PT-I03): Track actual gossip statistics
    messages_sent: u64,
    messages_received: u64,
    bytes_broadcast: u64,
    bytes_received: u64,
    /// SECURITY (AUDIT-024): Ed25519 signing key for message authentication
    signing_key: Option<SigningKey>,
}

impl GossipManager {
    pub fn new(config: GossipConfig) -> Self {
        Self {
            config,
            message_cache: HashMap::new(),
            topics: HashMap::new(),
            messages_sent: 0,
            messages_received: 0,
            bytes_broadcast: 0,
            bytes_received: 0,
            signing_key: None,
        }
    }

    /// Create a GossipManager with an Ed25519 signing key for message authentication.
    /// SECURITY (AUDIT-024): Messages broadcast through this manager will be
    /// cryptographically signed, enabling recipients to verify authenticity.
    pub fn with_signing_key(config: GossipConfig, signing_key: SigningKey) -> Self {
        Self {
            config,
            message_cache: HashMap::new(),
            topics: HashMap::new(),
            messages_sent: 0,
            messages_received: 0,
            bytes_broadcast: 0,
            bytes_received: 0,
            signing_key: Some(signing_key),
        }
    }

    pub fn with_keypair(config: GossipConfig, _keypair: String) -> Self {
        Self::new(config)
    }

    /// SECURITY (PT-I03): Return actual stats instead of hardcoded zeros
    pub fn get_stats(&self) -> GossipStats {
        GossipStats {
            messages_sent: self.messages_sent,
            messages_received: self.messages_received,
            active_topics: self.topics.len(),
            messages_broadcast: self.messages_sent,
            messages_dropped: 0,
            duplicates_filtered: 0,
            bytes_broadcast: self.bytes_broadcast,
            bytes_received: self.bytes_received,
            active_subscribers: 0,
            average_message_size: if self.messages_sent > 0 {
                self.bytes_broadcast as f64 / self.messages_sent as f64
            } else {
                0.0
            },
            topics_subscribed: self.topics.len(),
        }
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("Starting gossip manager");

        // Start periodic cleanup task
        let config = self.config.clone();
        let message_cache = self.message_cache.clone();
        let topics = self.topics.clone();
        let (event_sender, mut event_receiver) = mpsc::unbounded_channel();

        // Start cleanup task
        let event_sender_clone = event_sender.clone();
        let mut message_cache_clone = message_cache;
        let mut topics_clone = topics;
        let config_clone = config;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config_clone.cleanup_interval);
            loop {
                interval.tick().await;

                // Perform cleanup
                if let Err(e) = Self::perform_cleanup(
                    &mut message_cache_clone,
                    &mut topics_clone,
                    &config_clone,
                )
                .await
                {
                    error!("Gossip cleanup failed: {}", e);
                }

                // Send heartbeat event (UnboundedSender::send is sync, no await needed)
                let _ = event_sender_clone.send(GossipEvent::Heartbeat);
            }
        });

        // Start event processing
        tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                // Handle gossip events
                tracing::info!("Gossip event: {:?}", event);
                // In a real implementation, this would update state and notify other components
            }
        });

        info!("Gossip manager started successfully");
        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("Stopping gossip manager");

        // Clean up all topics
        self.topics.clear();

        // Clear message cache
        self.message_cache.clear();

        info!("Gossip manager stopped successfully");
        Ok(())
    }

    pub async fn broadcast(&mut self, topic: &str, data: Vec<u8>) -> anyhow::Result<()> {
        use ed25519_dalek::Signer;
        use sha2::{Digest, Sha256};

        // Validate message size
        if data.len() > self.config.max_message_size {
            return Err(anyhow::anyhow!(
                "Message size {} exceeds maximum {}",
                data.len(),
                self.config.max_message_size
            ));
        }

        // Create gossip message
        let message_id = format!(
            "msg_{}_{}",
            topic,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Signable bytes = SHA-256(topic || data || timestamp_le_bytes)
        let (sender, signature) = if let Some(ref sk) = self.signing_key {
            let verifying_key = sk.verifying_key();
            let mut hasher = Sha256::new();
            hasher.update(topic.as_bytes());
            hasher.update(&data);
            hasher.update(&timestamp.to_le_bytes());
            let hash = hasher.finalize();
            let sig = sk.sign(&hash);
            (
                Some(hex::encode(verifying_key.to_bytes())),
                Some(sig.to_bytes().to_vec()),
            )
        } else {
            (None, None)
        };

        let gossip_message = GossipMessage {
            id: message_id.clone(),
            topic: topic.to_string(),
            data: data.clone(),
            timestamp,
            sender,
            signature,
        };

        // Add to message cache
        self.message_cache
            .insert(message_id, gossip_message.clone());

        // Get topic manager
        let topic_manager = self
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicManager::new(topic.to_string()));

        // Broadcast to all subscribers
        let subscriber_count = topic_manager.subscribers.len();
        if subscriber_count == 0 {
            warn!("No subscribers for topic {}", topic);
            return Ok(());
        }

        // In a real implementation, this would send to actual network peers
        info!(
            "Broadcasting message to {} subscribers for topic {}",
            subscriber_count, topic
        );

        // Update stats (PT-I03)
        self.messages_sent += 1;
        self.bytes_broadcast += data.len() as u64;

        // Simulate broadcasting
        for subscriber_id in &topic_manager.subscribers {
            debug!("Sending message to subscriber: {}", subscriber_id);
        }

        Ok(())
    }

    pub async fn subscribe(&mut self, topic: &str) -> anyhow::Result<()> {
        // Get or create topic manager
        let topic_manager = self
            .topics
            .entry(topic.to_string())
            .or_insert_with(|| TopicManager::new(topic.to_string()));

        // Generate subscriber ID
        let subscriber_id = format!(
            "sub_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        // Add subscriber
        topic_manager.subscribers.insert(subscriber_id.clone());

        info!("Subscriber {} subscribed to topic {}", subscriber_id, topic);

        Ok(())
    }

    /// Perform cleanup of expired messages and inactive topics
    async fn perform_cleanup(
        message_cache: &mut HashMap<String, GossipMessage>,
        topics: &mut HashMap<String, TopicManager>,
        config: &GossipConfig,
    ) -> anyhow::Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Clean up expired messages
        let mut messages_to_remove = Vec::new();
        for (message_id, message) in message_cache.iter() {
            let age = current_time.saturating_sub(message.timestamp);
            if age > config.message_ttl.as_secs() {
                messages_to_remove.push(message_id.clone());
            }
        }

        for message_id in messages_to_remove {
            message_cache.remove(&message_id);
        }

        // Clean up inactive topics (no subscribers)
        let mut topics_to_remove = Vec::new();
        for (topic_name, topic_manager) in topics.iter() {
            if topic_manager.subscribers.is_empty() {
                topics_to_remove.push(topic_name.clone());
            }
        }

        for topic_name in topics_to_remove {
            topics.remove(&topic_name);
        }

        // Enforce cache size limits
        if message_cache.len() > config.duplicate_cache_size {
            let messages_to_remove = message_cache.len() - config.duplicate_cache_size;
            let removed_count = 0;
            message_cache.retain(|_, _| removed_count < messages_to_remove);
        }

        Ok(())
    }
}

/// Gossip message wrapper
#[derive(Debug, Clone)]
pub struct GossipMessage {
    pub id: String,
    pub topic: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
    /// Sender peer ID for authentication
    pub sender: Option<String>,
    /// Ed25519 signature over (topic || data || timestamp) for message integrity
    pub signature: Option<Vec<u8>>,
}

/// Topic manager for specific gossip topics
#[derive(Debug, Clone)]
pub struct TopicManager {
    pub topic: String,
    pub subscribers: HashSet<String>,
}

impl TopicManager {
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            subscribers: HashSet::new(),
        }
    }
}

/// Gossip statistics
#[derive(Debug, Clone, Default)]
pub struct GossipStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub active_topics: usize,
    pub messages_broadcast: u64,
    pub messages_dropped: u64,
    pub duplicates_filtered: u64,
    pub bytes_broadcast: u64,
    pub bytes_received: u64,
    pub active_subscribers: usize,
    pub average_message_size: f64,
    pub topics_subscribed: usize,
}

/// Gossip events
#[derive(Debug, Clone)]
pub enum GossipEvent {
    MessageReceived(GossipMessage),
    TopicSubscribed(String),
    TopicUnsubscribed(String),
    MessageBroadcast {
        message_id: String,
        topic: String,
        peers: usize,
    },
    MessageDropped {
        message_id: String,
        reason: String,
    },
    SubscriberJoined {
        peer: String,
        topic: String,
    },
    SubscriberLeft {
        peer: String,
        topic: String,
    },
    Heartbeat,
}

/// Production-grade gossip configuration with safety limits
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    pub max_message_size: usize,
    pub message_ttl: Duration,
    pub max_peers: usize,
    pub heartbeat_interval: Duration,
    pub duplicate_cache_size: usize,
    pub enable_validation: bool,
    pub enable_compression: bool,
    // New safety fields
    pub max_rounds_per_message: u8,
    pub max_messages_per_round: usize,
    pub cleanup_interval: Duration,
    pub max_height_diff: u64,
    pub eviction_policy: CacheEvictionPolicy,
    // SECURITY (H-06): Per-peer rate limiting
    /// Maximum messages per second per peer (token bucket capacity)
    pub peer_rate_limit: u32,
    /// Maximum messages per second globally across all peers
    pub global_rate_limit: u32,
    /// Interval for cleaning up stale peer rate limit entries
    pub rate_limit_cleanup_secs: u64,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024,         // 1MB
            message_ttl: Duration::from_secs(300), // 5 minutes
            max_peers: 100,
            heartbeat_interval: Duration::from_secs(30),
            duplicate_cache_size: 10000,
            enable_validation: true,
            enable_compression: false,
            // Safety limits
            max_rounds_per_message: 5,
            max_messages_per_round: 100,
            cleanup_interval: Duration::from_secs(10),
            max_height_diff: 100,
            eviction_policy: CacheEvictionPolicy::Combined,
            // Per-peer rate limiting: 100 msg/s per peer, 1000 msg/s global
            peer_rate_limit: 100,
            global_rate_limit: 1000,
            rate_limit_cleanup_secs: 60,
        }
    }
}

// ── SECURITY (H-06): Per-peer rate limiting ──────────────────────────────

/// Token bucket for rate limiting a single peer or global traffic.
///
/// Refills at `capacity` tokens per second. Each message consumes one token.
/// When tokens reach 0, messages are rejected until refill.
#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    capacity: u32,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            capacity,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if allowed, false if rate-limited.
    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.capacity as f64).min(self.capacity as f64);
        self.last_refill = now;
    }
}

/// Per-peer rate limiter using token buckets.
///
/// Each peer gets an independent token bucket with `peer_rate_limit` tokens/sec.
/// A global bucket caps total throughput at `global_rate_limit` tokens/sec.
/// Stale peer entries are cleaned up periodically.
#[derive(Debug)]
pub struct PeerRateLimiter {
    peer_buckets: HashMap<String, TokenBucket>,
    global_bucket: TokenBucket,
    peer_capacity: u32,
    last_cleanup: Instant,
    cleanup_interval: Duration,
    /// Count of messages rejected per peer (for monitoring)
    pub rejected_counts: HashMap<String, u64>,
}

impl PeerRateLimiter {
    pub fn new(peer_rate_limit: u32, global_rate_limit: u32, cleanup_secs: u64) -> Self {
        Self {
            peer_buckets: HashMap::new(),
            global_bucket: TokenBucket::new(global_rate_limit),
            peer_capacity: peer_rate_limit,
            last_cleanup: Instant::now(),
            cleanup_interval: Duration::from_secs(cleanup_secs),
            rejected_counts: HashMap::new(),
        }
    }

    /// Check if a message from the given peer should be allowed.
    /// Returns Ok(()) if allowed, Err(reason) if rate-limited.
    pub fn check_rate_limit(&mut self, peer_id: &str) -> Result<(), String> {
        // Global rate limit check
        if !self.global_bucket.try_consume() {
            warn!("Global rate limit exceeded (peer: {})", peer_id);
            return Err("Global rate limit exceeded".to_string());
        }

        // Per-peer rate limit check
        let bucket = self
            .peer_buckets
            .entry(peer_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.peer_capacity));

        if !bucket.try_consume() {
            *self.rejected_counts.entry(peer_id.to_string()).or_insert(0) += 1;
            warn!(
                "Per-peer rate limit exceeded for peer {} (rejected {} total)",
                peer_id,
                self.rejected_counts.get(peer_id).unwrap_or(&0)
            );
            return Err(format!("Rate limit exceeded for peer {}", peer_id));
        }

        // Periodic cleanup of stale entries
        self.maybe_cleanup();

        Ok(())
    }

    /// Remove peer entries that haven't been used recently (bucket is full = idle).
    fn maybe_cleanup(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_cleanup) < self.cleanup_interval {
            return;
        }
        self.last_cleanup = now;

        // Remove peers whose buckets are full (idle for a while)
        let stale_peers: Vec<String> = self
            .peer_buckets
            .iter()
            .filter(|(_, bucket)| {
                // If the bucket is at capacity, peer has been idle
                bucket.tokens >= bucket.capacity as f64 - 0.1
            })
            .map(|(k, _)| k.clone())
            .collect();

        for peer_id in &stale_peers {
            self.peer_buckets.remove(peer_id);
        }

        if !stale_peers.is_empty() {
            debug!(
                "Cleaned up {} stale peer rate limit entries",
                stale_peers.len()
            );
        }
    }

    /// Get the number of tracked peers.
    pub fn tracked_peer_count(&self) -> usize {
        self.peer_buckets.len()
    }
}

/// Bounded gossip cache with TTL and multiple eviction policies
#[derive(Debug, Clone)]
pub struct BoundedGossipCache {
    entries: HashMap<String, CacheEntry>,
    config: GossipConfig,
    network_state: NetworkState,
    last_cleanup: Instant,
    memory_usage: MemoryUsage,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    timestamp: Instant,
    message_id: String,
    height: u64,
    #[allow(dead_code)]
    epoch: u64,
    #[allow(dead_code)]
    access_count: u64,
    last_access: Instant,
}

impl BoundedGossipCache {
    pub fn new(config: GossipConfig) -> Self {
        Self {
            entries: HashMap::new(),
            network_state: NetworkState::default(),
            last_cleanup: Instant::now(),
            memory_usage: MemoryUsage::default(),
            config,
        }
    }

    /// Check if message exists in cache
    pub fn contains(&self, message_id: &str) -> bool {
        self.entries.contains_key(message_id)
    }

    /// Insert message into cache with safety checks
    pub fn insert(&mut self, message_id: String, height: u64, epoch: u64) -> bool {
        // Enforce invariants before insertion
        self.enforce_invariants();

        // Check if already exists
        if self.entries.contains_key(&message_id) {
            return false;
        }

        // Check size limits
        if self.entries.len() >= self.config.duplicate_cache_size {
            self.evict_by_policy();
        }

        // Insert new entry
        let now = Instant::now();
        self.entries.insert(
            message_id.clone(),
            CacheEntry {
                timestamp: now,
                message_id,
                height,
                epoch,
                access_count: 1,
                last_access: now,
            },
        );

        self.update_memory_usage();
        true
    }

    /// Enforce all structural invariants
    fn enforce_invariants(&mut self) {
        let now = Instant::now();

        // Periodic cleanup
        if now.duration_since(self.last_cleanup) > self.config.cleanup_interval {
            self.perform_maintenance_cleanup();
            self.last_cleanup = now;
        }

        // Size-based eviction
        if self.entries.len() > self.config.duplicate_cache_size {
            self.evict_by_policy();
        }

        // Height-based eviction
        if self.network_state.should_cleanup_by_height() {
            self.evict_by_height();
            self.network_state.mark_cleanup_done();
        }
    }

    /// Perform maintenance cleanup based on configured policy
    fn perform_maintenance_cleanup(&mut self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        match self.config.eviction_policy {
            CacheEvictionPolicy::TimeBased | CacheEvictionPolicy::Combined => {
                // Remove expired entries
                for (id, entry) in &self.entries {
                    if now.duration_since(entry.timestamp) > self.config.message_ttl {
                        to_remove.push(id.clone());
                    }
                }
            }
            _ => {}
        }

        // Remove expired entries
        for id in &to_remove {
            self.entries.remove(id);
        }

        // Update memory usage
        self.update_memory_usage();

        if !to_remove.is_empty() {
            println!(
                "🧹 Cache maintenance: removed {} entries, size now {}",
                to_remove.len(),
                self.entries.len()
            );
        }
    }

    /// Evict entries based on configured policy
    fn evict_by_policy(&mut self) {
        match self.config.eviction_policy {
            CacheEvictionPolicy::LRU => {
                self.evict_lru_entries();
            }
            CacheEvictionPolicy::HeightBased => {
                self.evict_by_height();
            }
            CacheEvictionPolicy::Combined => {
                self.evict_lru_entries();
                self.evict_by_height();
            }
            CacheEvictionPolicy::TimeBased => {
                // Already handled in maintenance cleanup
            }
        }
    }

    /// Evict least recently used entries
    fn evict_lru_entries(&mut self) {
        let target_size = self.config.duplicate_cache_size;
        if self.entries.len() <= target_size {
            return;
        }

        let excess = self.entries.len() - target_size;
        let mut entries_by_access: Vec<_> = self.entries.values().cloned().collect();

        // Sort by last access time (oldest first)
        entries_by_access.sort_by_key(|e| e.last_access);

        // Remove oldest entries
        for entry in entries_by_access.iter().take(excess) {
            self.entries.remove(&entry.message_id);
        }

        println!("🧹 LRU eviction: removed {} entries", excess);
    }

    /// Evict entries from old blockchain heights
    fn evict_by_height(&mut self) {
        let cutoff_height = self
            .network_state
            .last_cleanup_height
            .saturating_sub(self.config.max_height_diff);

        let mut to_remove = Vec::new();
        for (id, entry) in &self.entries {
            if entry.height < cutoff_height {
                to_remove.push(id.clone());
            }
        }

        for id in &to_remove {
            self.entries.remove(id);
        }

        if !to_remove.is_empty() {
            println!(
                "🧹 Height-based eviction: removed {} entries",
                to_remove.len()
            );
        }
    }

    /// Update memory usage tracking
    fn update_memory_usage(&mut self) {
        self.memory_usage.cache_size = self.entries.len();
        self.memory_usage.cache_memory_mb = self.memory_usage.calculate_memory_mb();
    }

    /// Update network state
    pub fn update_network_state(&mut self, height: u64, epoch: u64) {
        self.network_state.update_height(height);
        self.network_state.update_epoch(epoch);
    }

    /// Get current memory usage
    pub fn memory_usage(&self) -> &MemoryUsage {
        &self.memory_usage
    }

    /// Check if cache is within safe limits
    pub fn is_within_limits(&self) -> bool {
        self.memory_usage.is_within_limits()
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.entries.len(),
            max_size: self.config.duplicate_cache_size,
            memory_mb: self.memory_usage.cache_memory_mb,
            current_height: self.network_state.current_height,
            last_cleanup_height: self.network_state.last_cleanup_height,
        }
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub max_size: usize,
    pub memory_mb: f64,
    pub current_height: u64,
    pub last_cleanup_height: u64,
}

/// Production-grade gossip network with safety limits
#[derive(Debug)]
pub struct GossipNetwork {
    config: GossipConfig,
    cache: BoundedGossipCache,
    peers: HashSet<String>,
    message_stats: MessageStats,
    network_state: NetworkState,
    /// SECURITY (H-06): Per-peer rate limiter
    rate_limiter: PeerRateLimiter,
}

#[derive(Debug, Clone, Default)]
pub struct MessageStats {
    pub total_sent: u64,
    pub total_received: u64,
    pub duplicates_filtered: u64,
    pub expired_filtered: u64,
    pub rounds_completed: u64,
}

impl GossipNetwork {
    pub fn new(config: GossipConfig) -> Self {
        let rate_limiter = PeerRateLimiter::new(
            config.peer_rate_limit,
            config.global_rate_limit,
            config.rate_limit_cleanup_secs,
        );
        Self {
            cache: BoundedGossipCache::new(config.clone()),
            config,
            peers: HashSet::new(),
            message_stats: MessageStats::default(),
            network_state: NetworkState::default(),
            rate_limiter,
        }
    }

    /// Add peer to network
    pub fn add_peer(&mut self, peer_id: String) -> Result<(), String> {
        if self.peers.len() >= self.config.max_peers {
            return Err("Maximum peers reached".to_string());
        }

        self.peers.insert(peer_id);
        Ok(())
    }

    /// Remove peer from network
    pub fn remove_peer(&mut self, peer_id: &str) {
        self.peers.remove(peer_id);
    }

    /// Broadcast message with safety limits
    pub fn broadcast(&mut self, message: Message) -> Result<Vec<String>, String> {
        // Validate message
        self.validate_message(&message)?;

        // Update network state (simulated height/epoch)
        let height = self.network_state.current_height + 1;
        let epoch = self.network_state.current_epoch;
        self.cache.update_network_state(height, epoch);

        // Check cache
        if self.cache.contains(&message.message_id) {
            self.message_stats.duplicates_filtered += 1;
            return Ok(vec![]);
        }

        // Insert into cache
        if !self.cache.insert(message.message_id.clone(), height, epoch) {
            return Ok(vec![]);
        }

        // Simulate broadcast to peers
        let mut delivered_peers = Vec::new();
        let mut rounds = 0;
        let mut current_round = self.peers.clone();

        while !current_round.is_empty() && rounds < self.config.max_rounds_per_message {
            let mut next_round = HashSet::new();
            let mut messages_this_round = 0;

            for peer_id in current_round {
                if messages_this_round >= self.config.max_messages_per_round {
                    break;
                }

                // Simulate message delivery
                delivered_peers.push(peer_id.clone());
                next_round.insert(peer_id);
                messages_this_round += 1;
            }

            current_round = next_round;
            rounds += 1;
            self.message_stats.rounds_completed += 1;
        }

        self.message_stats.total_sent += 1;
        Ok(delivered_peers)
    }

    /// Validate message against safety limits
    ///
    /// 1. Size limits to prevent memory exhaustion
    /// 2. Timestamp freshness to prevent replay attacks
    /// 3. Sender presence check for authentication
    /// 4. Duplicate detection via cache
    fn validate_message(&self, message: &Message) -> Result<(), String> {
        // Layer 1: Size limits
        if message.payload.len() > self.config.max_message_size {
            return Err(format!(
                "Message too large: {} bytes (max {})",
                message.payload.len(),
                self.config.max_message_size
            ));
        }

        if message.payload.is_empty() {
            return Err("Empty message payload".to_string());
        }

        if !self.config.enable_validation {
            return Ok(());
        }

        // Layer 2: Timestamp freshness — reject messages older than TTL
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let msg_time = Duration::from_secs(message.timestamp);
        let msg_age = now.checked_sub(msg_time).unwrap_or_default();
        if msg_age > self.config.message_ttl {
            return Err(format!(
                "Message too old: {:?} (max TTL {:?})",
                msg_age, self.config.message_ttl
            ));
        }

        // Layer 3: Reject messages with timestamps in the future (>30s clock skew tolerance)
        let future_tolerance = Duration::from_secs(30);
        if msg_time > now + future_tolerance {
            return Err("Message timestamp is in the future".to_string());
        }

        // Layer 4: Ed25519 signature verification
        // instead of merely checking for its presence.
        {
            let sig_hex = match &message.signature {
                Some(s) => s,
                None => return Err("Message missing required signature".to_string()),
            };

            // Parse the hex-encoded signature (64 bytes = 128 hex chars)
            let sig_bytes =
                hex::decode(sig_hex).map_err(|e| format!("Invalid signature encoding: {}", e))?;
            if sig_bytes.len() != 64 {
                return Err(format!(
                    "Invalid signature length: expected 64 bytes, got {}",
                    sig_bytes.len()
                ));
            }
            let sig_arr: [u8; 64] = sig_bytes
                .try_into()
                .map_err(|_| "Failed to convert signature bytes".to_string())?;
            let sig = ed25519_dalek::Signature::from_bytes(&sig_arr);

            // Parse the sender's public key from source_peer (hex-encoded 32 bytes)
            let pk_bytes = hex::decode(&message.source_peer)
                .map_err(|e| format!("Invalid source_peer public key encoding: {}", e))?;
            if pk_bytes.len() != 32 {
                return Err(format!(
                    "Invalid public key length: expected 32 bytes, got {}",
                    pk_bytes.len()
                ));
            }
            let pk_arr: [u8; 32] = pk_bytes
                .try_into()
                .map_err(|_| "Failed to convert public key bytes".to_string())?;
            let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pk_arr)
                .map_err(|e| format!("Invalid Ed25519 public key: {}", e))?;

            // Verify signature over the canonical signing bytes
            use ed25519_dalek::Verifier;
            verifying_key
                .verify(&message.signing_bytes(), &sig)
                .map_err(|e| format!("Ed25519 signature verification failed: {}", e))?;
        }

        // Layer 5: Duplicate detection
        if self.cache.contains(&message.message_id) {
            return Err("Duplicate message".to_string());
        }

        Ok(())
    }

    ///
    /// SECURITY (H-06): Enforces per-peer and global rate limits before
    /// processing the message to prevent flooding/DoS attacks.
    pub fn receive(&mut self, message: Message, from_peer: &str) -> Result<(), String> {
        // Rate limit check — must be first to minimize work under DoS
        self.rate_limiter.check_rate_limit(from_peer)?;

        // Validate message
        self.validate_message(&message)?;

        // Dedup check
        if self.cache.contains(&message.message_id) {
            self.message_stats.duplicates_filtered += 1;
            return Ok(());
        }

        let height = self.network_state.current_height;
        let epoch = self.network_state.current_epoch;
        self.cache.insert(message.message_id.clone(), height, epoch);
        self.message_stats.total_received += 1;
        Ok(())
    }

    /// Get network statistics
    pub fn stats(&self) -> NetworkStats {
        NetworkStats {
            peer_count: self.peers.len(),
            max_peers: self.config.max_peers,
            cache_stats: self.cache.stats(),
            message_stats: self.message_stats.clone(),
            memory_usage: self.cache.memory_usage().clone(),
            network_state: self.network_state.clone(),
        }
    }

    /// Check if network is healthy
    pub fn is_healthy(&self) -> bool {
        let stats = self.stats();

        stats.cache_stats.size <= stats.cache_stats.max_size
            && stats.memory_usage.is_within_limits()
            && stats.peer_count <= stats.max_peers
    }
}

/// Network statistics for monitoring
#[derive(Debug, Clone)]
pub struct NetworkStats {
    pub peer_count: usize,
    pub max_peers: usize,
    pub cache_stats: CacheStats,
    pub message_stats: MessageStats,
    pub memory_usage: MemoryUsage,
    pub network_state: NetworkState,
}

impl GossipSafety for GossipNetwork {
    fn validate_message(&self, message: &Message) -> Result<(), String> {
        self.validate_message(message)
    }

    fn should_limit_gossip(&self, rounds: u8, messages_per_round: usize) -> bool {
        rounds >= self.config.max_rounds_per_message
            || messages_per_round >= self.config.max_messages_per_round
    }

    fn enforce_cache_limits(&mut self) {
        self.cache.enforce_invariants();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::messages::MessageType;

    #[test]
    fn test_bounded_cache_limits() {
        let config = GossipConfig {
            duplicate_cache_size: 10,
            ..Default::default()
        };

        let mut cache = BoundedGossipCache::new(config);

        // Fill cache to limit
        for i in 0..15 {
            cache.insert(format!("msg_{}", i), 100, 1);
        }

        assert!(cache.entries.len() <= 10);
    }

    #[test]
    fn test_gossip_safety_validation() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        // Generate a signing key for test messages
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let source_peer = hex::encode(signing_key.verifying_key().to_bytes());

        // Create a properly signed valid message
        let mut valid_message =
            Message::new(MessageType::Transaction, source_peer.clone(), vec![0; 100]);
        valid_message.sign(&signing_key);

        assert!(network.validate_message(&valid_message).is_ok());

        // Oversized messages should be rejected regardless of signature
        let oversized_message = Message::new(
            MessageType::Transaction,
            source_peer,
            vec![0; 2 * 1024 * 1024], // 2MB
        );

        assert!(network.validate_message(&oversized_message).is_err());
    }

    #[test]
    fn test_network_health_check() {
        let config = GossipConfig::default();
        let mut network = GossipNetwork::new(config);

        assert!(network.is_healthy());

        // Add peers up to limit
        for i in 0..100 {
            network.add_peer(format!("peer_{}", i)).unwrap();
        }

        assert!(network.is_healthy());

        // Try to exceed limit
        assert!(network.add_peer("overflow".to_string()).is_err());
    }
}

/// Simple gossip network implementation
pub struct SimpleGossipNetwork {
    config: GossipConfig,
    subscribers: HashMap<String, HashSet<String>>, // topic -> peers
    message_cache: HashMap<String, Instant>,       // message_id -> timestamp
    stats: GossipStats,
    event_sender: mpsc::UnboundedSender<GossipEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<GossipEvent>>,
    local_peer_id: String,
    /// SECURITY (H-06): Per-peer rate limiter
    rate_limiter: PeerRateLimiter,
}

impl SimpleGossipNetwork {
    pub fn new(config: GossipConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let rate_limiter = PeerRateLimiter::new(
            config.peer_rate_limit,
            config.global_rate_limit,
            config.rate_limit_cleanup_secs,
        );

        Self {
            config,
            subscribers: HashMap::new(),
            message_cache: HashMap::new(),
            stats: GossipStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            local_peer_id: "local_peer".to_string(),
            rate_limiter,
        }
    }

    pub fn with_local_peer_id(mut self, peer_id: String) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Starting gossip network");

        // Subscribe to default topics
        self.subscribe("savitri/transactions").await?;
        self.subscribe("savitri/blocks").await?;
        self.subscribe("savitri/consensus").await?;

        tracing::info!(
            "Gossip network started with {} topics",
            self.stats.topics_subscribed
        );
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Stopping gossip network");

        // Unsubscribe from all topics
        let topics: Vec<String> = self.subscribers.keys().cloned().collect();
        for topic in topics {
            self.unsubscribe(&topic).await?;
        }

        tracing::info!("Gossip network stopped");
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        topic: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.subscribers.contains_key(topic) {
            self.subscribers.insert(topic.to_string(), HashSet::new());
            self.stats.topics_subscribed += 1;

            // Send event
            let _ = self
                .event_sender
                .send(GossipEvent::TopicSubscribed(topic.to_string()));

            tracing::info!("Subscribed to topic: {}", topic);
        }

        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        topic: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.subscribers.remove(topic).is_some() {
            self.stats.topics_subscribed = self.stats.topics_subscribed.saturating_sub(1);

            // Send event
            let _ = self
                .event_sender
                .send(GossipEvent::TopicUnsubscribed(topic.to_string()));

            tracing::info!("Unsubscribed from topic: {}", topic);
        }

        Ok(())
    }

    pub async fn add_subscriber(
        &mut self,
        peer: String,
        topic: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(subscribers) = self.subscribers.get_mut(&topic) {
            if subscribers.insert(peer.clone()) {
                self.stats.active_subscribers += 1;

                // Send event
                let _ = self.event_sender.send(GossipEvent::SubscriberJoined {
                    peer: peer.clone(),
                    topic: topic.clone(),
                });

                tracing::debug!("Added subscriber {} to topic {}", peer, topic);
            }
        } else {
            return Err(format!("Topic {} not found", topic).into());
        }

        Ok(())
    }

    pub async fn remove_subscriber(
        &mut self,
        peer: &str,
        topic: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(subscribers) = self.subscribers.get_mut(topic) {
            if subscribers.remove(peer) {
                self.stats.active_subscribers = self.stats.active_subscribers.saturating_sub(1);

                // Send event
                let _ = self.event_sender.send(GossipEvent::SubscriberLeft {
                    peer: peer.to_string(),
                    topic: topic.to_string(),
                });

                tracing::debug!("Removed subscriber {} from topic {}", peer, topic);
            }
        }

        Ok(())
    }

    pub async fn broadcast_message(
        &mut self,
        message: Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check message size
        if message.size() > self.config.max_message_size {
            self.stats.messages_dropped += 1;

            // Send event
            let _ = self.event_sender.send(GossipEvent::MessageDropped {
                message_id: message.message_id.clone(),
                reason: "Message too large".to_string(),
            });

            return Err("Message too large".into());
        }

        // Check for duplicates
        if self.message_cache.contains_key(&message.message_id) {
            self.stats.duplicates_filtered += 1;
            return Ok(());
        }

        // Add to cache
        self.message_cache
            .insert(message.message_id.clone(), Instant::now());

        // Determine topic based on message type
        let topic = self.message_type_to_topic(&message.message_type);

        // Get subscribers for this topic
        let subscribers = if let Some(subs) = self.subscribers.get(&topic) {
            subs.clone()
        } else {
            HashSet::new()
        };

        // Broadcast to all subscribers (except self)
        let mut peers_notified = 0;
        for peer in subscribers {
            if peer != self.local_peer_id {
                // In a real implementation, you would send the actual message
                // For now, we'll just simulate the broadcast
                peers_notified += 1;
            }
        }

        // Update stats
        self.stats.messages_broadcast += 1;
        self.stats.bytes_broadcast += message.size() as u64;

        // Update average message size
        if self.stats.messages_broadcast > 0 {
            self.stats.average_message_size = (self.stats.average_message_size
                * (self.stats.messages_broadcast - 1) as f64
                + message.size() as f64)
                / self.stats.messages_broadcast as f64;
        }

        // Send event
        let _ = self.event_sender.send(GossipEvent::MessageBroadcast {
            message_id: message.message_id.clone(),
            topic: topic.clone(),
            peers: peers_notified,
        });

        tracing::debug!(
            "Broadcast message {} to {} peers in topic {}",
            message.message_id,
            peers_notified,
            topic
        );

        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        message: Message,
        from: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // SECURITY (H-06): Per-peer rate limit check — must be first to prevent DoS
        if let Err(reason) = self.rate_limiter.check_rate_limit(&from) {
            self.stats.messages_dropped += 1;
            let _ = self.event_sender.send(GossipEvent::MessageDropped {
                message_id: message.message_id.clone(),
                reason: format!("Rate limited: {}", reason),
            });
            return Err(reason.into());
        }

        // SECURITY: Validate message size before processing
        if message.size() > self.config.max_message_size {
            self.stats.messages_dropped += 1;
            let _ = self.event_sender.send(GossipEvent::MessageDropped {
                message_id: message.message_id.clone(),
                reason: format!("Message too large from {}", from),
            });
            return Err(format!(
                "Message size {} exceeds limit {}",
                message.size(),
                self.config.max_message_size
            )
            .into());
        }

        // Check for duplicates
        if self.message_cache.contains_key(&message.message_id) {
            self.stats.duplicates_filtered += 1;
            return Ok(());
        }

        // SECURITY (PT-M04): Enforce cache size limit before inserting.
        // Evict expired entries first, then oldest entries if still over limit.
        if self.message_cache.len() >= self.config.duplicate_cache_size {
            self.cleanup_expired_messages();
        }
        if self.message_cache.len() >= self.config.duplicate_cache_size {
            // Still over limit — evict oldest entries
            let mut entries: Vec<(String, Instant)> = self.message_cache.drain().collect();
            entries.sort_by_key(|(_, ts)| *ts);
            // Keep only the newest half
            let keep = self.config.duplicate_cache_size / 2;
            self.message_cache = entries.into_iter().rev().take(keep).collect();
            tracing::warn!(
                "Gossip cache over limit, evicted to {} entries",
                self.message_cache.len()
            );
        }

        // Add to cache
        self.message_cache
            .insert(message.message_id.clone(), Instant::now());

        // Determine topic
        let topic = self.message_type_to_topic(&message.message_type);

        // Update stats
        self.stats.messages_received += 1;
        self.stats.bytes_received += message.size() as u64;

        // Send event
        let gossip_message = GossipMessage {
            id: message.message_id.clone(),
            topic: topic.clone(),
            data: message.payload.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            sender: Some(from.clone()),
            signature: None,
        };
        let _ = self
            .event_sender
            .send(GossipEvent::MessageReceived(gossip_message));

        tracing::debug!(
            "Received message {} from {} in topic {}",
            message.message_id,
            from,
            topic
        );

        Ok(())
    }

    fn message_type_to_topic(&self, message_type: &crate::p2p::messages::MessageType) -> String {
        use crate::p2p::messages::MessageType;

        match message_type {
            MessageType::Transaction => "savitri/transactions".to_string(),
            MessageType::BlockAnnouncement => "savitri/blocks".to_string(),
            MessageType::Consensus => "savitri/consensus".to_string(),
            MessageType::PeerExchange => "savitri/peers".to_string(),
            _ => "savitri/general".to_string(),
        }
    }

    pub fn cleanup_expired_messages(&mut self) {
        let now = Instant::now();
        let expired_messages: Vec<String> = self
            .message_cache
            .iter()
            .filter(|(_, timestamp)| now.duration_since(**timestamp) > self.config.message_ttl)
            .map(|(id, _)| id.clone())
            .collect();

        let expired_count = expired_messages.len();
        for message_id in expired_messages {
            self.message_cache.remove(&message_id);
        }

        if expired_count > 0 {
            tracing::debug!("Cleaned up {} expired messages", expired_count);
        }
    }

    pub fn get_stats(&self) -> GossipStats {
        self.stats.clone()
    }

    pub fn get_subscribed_topics(&self) -> Vec<String> {
        self.subscribers.keys().cloned().collect()
    }

    pub fn get_topic_subscribers(&self, topic: &str) -> Vec<String> {
        self.subscribers
            .get(topic)
            .map(|subscribers| subscribers.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<GossipEvent>> {
        self.event_receiver.take()
    }
}

impl Default for GossipNetwork {
    fn default() -> Self {
        Self::new(GossipConfig::default())
    }
}

#[cfg(test)]
mod simple_gossip_tests {
    use super::*;
    use crate::p2p::messages::{Message, MessageType};

    #[test]
    fn test_gossip_config_default() {
        let config = GossipConfig::default();
        assert_eq!(config.max_message_size, 1024 * 1024);
        assert_eq!(config.max_peers, 100);
        assert!(config.enable_validation);
        assert!(!config.enable_compression); // default is false
    }

    #[tokio::test]
    async fn test_gossip_network_lifecycle() {
        let config = GossipConfig::default();
        let mut gossip = SimpleGossipNetwork::new(config);

        // Test start
        assert!(gossip.start().await.is_ok());
        assert_eq!(gossip.get_subscribed_topics().len(), 3); // Default topics

        // Test stop
        assert!(gossip.stop().await.is_ok());
        assert_eq!(gossip.get_subscribed_topics().len(), 0);
    }

    #[tokio::test]
    async fn test_topic_subscription() {
        let config = GossipConfig::default();
        let mut gossip = SimpleGossipNetwork::new(config);

        // Subscribe to custom topic
        assert!(gossip.subscribe("custom/topic").await.is_ok());
        assert!(gossip
            .get_subscribed_topics()
            .contains(&"custom/topic".to_string()));

        // Unsubscribe from topic
        assert!(gossip.unsubscribe("custom/topic").await.is_ok());
        assert!(!gossip
            .get_subscribed_topics()
            .contains(&"custom/topic".to_string()));
    }

    #[tokio::test]
    async fn test_subscriber_management() {
        let config = GossipConfig::default();
        let mut gossip = SimpleGossipNetwork::new(config);

        // Subscribe to topic first
        gossip.subscribe("test/topic").await.unwrap();

        // Add subscribers
        assert!(gossip
            .add_subscriber("peer1".to_string(), "test/topic".to_string())
            .await
            .is_ok());
        assert!(gossip
            .add_subscriber("peer2".to_string(), "test/topic".to_string())
            .await
            .is_ok());

        let subscribers = gossip.get_topic_subscribers("test/topic");
        assert_eq!(subscribers.len(), 2);
        assert!(subscribers.contains(&"peer1".to_string()));
        assert!(subscribers.contains(&"peer2".to_string()));

        // Remove subscriber
        assert!(gossip
            .remove_subscriber("peer1", "test/topic")
            .await
            .is_ok());
        let subscribers = gossip.get_topic_subscribers("test/topic");
        assert_eq!(subscribers.len(), 1);
        assert!(!subscribers.contains(&"peer1".to_string()));
    }

    #[tokio::test]
    async fn test_message_broadcast() {
        let config = GossipConfig::default();
        let mut gossip = SimpleGossipNetwork::new(config);

        // Setup topic and subscribers
        gossip.subscribe("savitri/transactions").await.unwrap();
        gossip
            .add_subscriber("peer1".to_string(), "savitri/transactions".to_string())
            .await
            .unwrap();
        gossip
            .add_subscriber("peer2".to_string(), "savitri/transactions".to_string())
            .await
            .unwrap();

        // Create and broadcast message
        let message = Message::new(
            MessageType::Transaction,
            "local_peer".to_string(),
            b"transaction_data".to_vec(),
        );

        assert!(gossip.broadcast_message(message).await.is_ok());

        // Check stats
        let stats = gossip.get_stats();
        assert_eq!(stats.messages_broadcast, 1);
        assert_eq!(stats.bytes_broadcast, 16); // b"transaction_data".len()
    }

    #[tokio::test]
    async fn test_duplicate_filtering() {
        let config = GossipConfig::default();
        let mut gossip = SimpleGossipNetwork::new(config);

        gossip.subscribe("savitri/general").await.unwrap();

        let message = Message::new(
            MessageType::Ping,
            "local_peer".to_string(),
            b"ping".to_vec(),
        );

        // Broadcast same message twice
        assert!(gossip.broadcast_message(message.clone()).await.is_ok());
        assert!(gossip.broadcast_message(message).await.is_ok()); // Should be filtered

        let stats = gossip.get_stats();
        assert_eq!(stats.messages_broadcast, 1);
        assert_eq!(stats.duplicates_filtered, 1);
    }

    #[tokio::test]
    async fn test_message_cleanup() {
        let config = GossipConfig {
            message_ttl: Duration::from_millis(100),
            ..Default::default()
        };

        let mut gossip = SimpleGossipNetwork::new(config);
        gossip.subscribe("savitri/general").await.unwrap();

        let message = Message::new(
            MessageType::Ping,
            "local_peer".to_string(),
            b"ping".to_vec(),
        );

        // Broadcast message
        gossip.broadcast_message(message).await.unwrap();
        assert_eq!(gossip.stats.messages_broadcast, 1);

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Cleanup expired messages
        gossip.cleanup_expired_messages();

        // Message should be removed from cache but stats remain
        assert_eq!(gossip.stats.messages_broadcast, 1);
    }

    // ── Rate Limiting Tests (H-06) ──────────────────────────────────

    #[test]
    fn test_peer_rate_limiter_allows_within_limit() {
        let mut limiter = PeerRateLimiter::new(5, 100, 60);
        // 5 messages should be allowed immediately
        for i in 0..5 {
            assert!(
                limiter.check_rate_limit("peer_a").is_ok(),
                "msg {} should be allowed",
                i
            );
        }
    }

    #[test]
    fn test_peer_rate_limiter_rejects_over_limit() {
        let mut limiter = PeerRateLimiter::new(3, 100, 60);
        // Exhaust the bucket
        for _ in 0..3 {
            assert!(limiter.check_rate_limit("peer_a").is_ok());
        }
        // 4th message should be rejected
        assert!(limiter.check_rate_limit("peer_a").is_err());
    }

    #[test]
    fn test_peer_rate_limiter_per_peer_isolation() {
        let mut limiter = PeerRateLimiter::new(2, 100, 60);
        // Exhaust peer_a
        assert!(limiter.check_rate_limit("peer_a").is_ok());
        assert!(limiter.check_rate_limit("peer_a").is_ok());
        assert!(limiter.check_rate_limit("peer_a").is_err());
        // peer_b should still be allowed
        assert!(limiter.check_rate_limit("peer_b").is_ok());
        assert!(limiter.check_rate_limit("peer_b").is_ok());
        assert!(limiter.check_rate_limit("peer_b").is_err());
    }

    #[test]
    fn test_peer_rate_limiter_global_limit() {
        let mut limiter = PeerRateLimiter::new(100, 3, 60);
        // Global limit of 3 across all peers
        assert!(limiter.check_rate_limit("peer_a").is_ok());
        assert!(limiter.check_rate_limit("peer_b").is_ok());
        assert!(limiter.check_rate_limit("peer_c").is_ok());
        // 4th message should hit global limit
        assert!(limiter.check_rate_limit("peer_d").is_err());
    }

    #[tokio::test]
    async fn test_receive_message_rate_limited() {
        let config = GossipConfig {
            peer_rate_limit: 2,
            global_rate_limit: 100,
            ..Default::default()
        };
        let mut gossip = SimpleGossipNetwork::new(config);

        let make_msg = |id: &str| {
            Message::new(
                MessageType::Ping,
                "peer_a".to_string(),
                id.as_bytes().to_vec(),
            )
        };

        // First 2 messages from peer_a should be accepted
        assert!(gossip
            .receive_message(make_msg("m1"), "peer_a".to_string())
            .await
            .is_ok());
        assert!(gossip
            .receive_message(make_msg("m2"), "peer_a".to_string())
            .await
            .is_ok());
        // 3rd should be rate-limited
        assert!(gossip
            .receive_message(make_msg("m3"), "peer_a".to_string())
            .await
            .is_err());
        assert_eq!(gossip.stats.messages_dropped, 1);
    }

    #[test]
    fn test_validate_message_rejects_missing_signature() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        // Message without signature should be rejected
        let message = Message::new(
            MessageType::Transaction,
            "not_a_valid_hex_peer".to_string(),
            vec![0; 100],
        );

        let result = network.validate_message(&message);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing required signature"));
    }

    #[test]
    fn test_validate_message_rejects_invalid_signature() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        // Generate a key to get a valid-looking source_peer
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let source_peer = hex::encode(signing_key.verifying_key().to_bytes());

        // Create message with a bogus signature (valid hex, wrong bytes)
        let mut message = Message::new(MessageType::Transaction, source_peer, vec![0; 100]);
        message.signature = Some(hex::encode([0xABu8; 64]));

        let result = network.validate_message(&message);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("signature verification failed"));
    }

    #[test]
    fn test_validate_message_accepts_valid_signature() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let source_peer = hex::encode(signing_key.verifying_key().to_bytes());

        let mut message = Message::new(MessageType::Transaction, source_peer, vec![42; 100]);
        message.sign(&signing_key);

        assert!(network.validate_message(&message).is_ok());
    }

    #[test]
    fn test_validate_message_rejects_tampered_payload() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let source_peer = hex::encode(signing_key.verifying_key().to_bytes());

        let mut message = Message::new(MessageType::Transaction, source_peer, vec![42; 100]);
        message.sign(&signing_key);

        // Tamper with payload after signing
        message.payload[0] = 0xFF;

        let result = network.validate_message(&message);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("signature verification failed"));
    }

    #[test]
    fn test_validate_message_rejects_wrong_sender_key() {
        let config = GossipConfig::default();
        let network = GossipNetwork::new(config);

        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let other_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        // Sign with signing_key but claim to be other_key
        let source_peer = hex::encode(other_key.verifying_key().to_bytes());

        let mut message = Message::new(MessageType::Transaction, source_peer, vec![42; 100]);
        // Sign with the wrong key
        message.sign(&signing_key);

        let result = network.validate_message(&message);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("signature verification failed"));
    }

    #[test]
    fn test_gossip_manager_with_signing_key() {
        let signing_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());
        let config = GossipConfig::default();
        let manager = GossipManager::with_signing_key(config, signing_key);
        assert!(manager.signing_key.is_some());
    }

    #[test]
    fn test_validate_skipped_when_validation_disabled() {
        let config = GossipConfig {
            enable_validation: false,
            ..Default::default()
        };
        let network = GossipNetwork::new(config);

        let message = Message::new(
            MessageType::Transaction,
            "any_peer".to_string(),
            vec![0; 100],
        );

        assert!(network.validate_message(&message).is_ok());
    }
}
