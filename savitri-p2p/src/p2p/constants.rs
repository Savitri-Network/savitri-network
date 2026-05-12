//! P2P Protocol Constants and Limits
//!
//! This module defines all structural invariants and limits for the P2P layer
//! to prevent memory exhaustion and ensure production safety.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Gossip cache size limits to prevent memory exhaustion
/// Ottimizzato per testnet con 100 LN + 20 MN (120 nodi)
pub const GOSSIP_CACHE_MAX_SIZE: usize = 20_000;

/// Maximum age for gossip cache entries before automatic eviction
pub const GOSSIP_CACHE_MAX_AGE: Duration = Duration::from_secs(300); // 5 minutes

/// Maximum number of gossip rounds per message to prevent infinite propagation
pub const GOSSIP_MAX_ROUNDS: u8 = 5;

/// Interval for automatic cache maintenance and cleanup
pub const GOSSIP_CLEANUP_INTERVAL: Duration = Duration::from_secs(10);

/// Maximum blockchain height difference for cache entries
pub const GOSSIP_MAX_HEIGHT_DIFF: u64 = 100;

/// Maximum number of messages per round to prevent explosion
pub const GOSSIP_MAX_MESSAGES_PER_ROUND: usize = 100;

/// Maximum hop count for message propagation
pub const GOSSIP_MAX_HOP_COUNT: u8 = 10;

/// Maximum message payload size (1MB)
pub const GOSSIP_MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Connection limits per peer
pub const PEER_MAX_CONNECTIONS: usize = 50;

/// Peer inactivity timeout for cleanup
pub const PEER_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(600); // 10 minutes

/// Bandwidth limit per peer (2MB/s per testnet 120 nodi)
pub const PEER_BANDWIDTH_LIMIT: u64 = 2 * 1024 * 1024;

/// Network partition detection threshold
pub const NETWORK_PARTITION_THRESHOLD: f64 = 0.3; // 30%

/// Success rate thresholds for network health
pub const NETWORK_MIN_SUCCESS_RATE: f64 = 0.8; // 80%
pub const NETWORK_MIN_CONNECTIVITY: f64 = 0.05; // 5%
pub const NETWORK_MIN_REPUTATION: f64 = 40.0; // 40 points

/// Cache eviction policies
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CacheEvictionPolicy {
    /// Evict entries older than max age
    TimeBased,
    /// Evict least recently used entries when size limit reached
    LRU,
    /// Evict entries from old blockchain heights
    HeightBased,
    /// Combined policy: time + size + height
    Combined,
}

impl Default for CacheEvictionPolicy {
    fn default() -> Self {
        Self::Combined
    }
}

/// Gossip protocol configuration with safety limits
#[derive(Debug, Clone)]
pub struct GossipConfig {
    pub max_cache_size: usize,
    pub max_message_age: Duration,
    pub max_rounds: u8,
    pub cleanup_interval: Duration,
    pub max_height_diff: u64,
    pub max_messages_per_round: usize,
    pub eviction_policy: CacheEvictionPolicy,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            max_cache_size: GOSSIP_CACHE_MAX_SIZE,
            max_message_age: GOSSIP_CACHE_MAX_AGE,
            max_rounds: GOSSIP_MAX_ROUNDS,
            cleanup_interval: GOSSIP_CLEANUP_INTERVAL,
            max_height_diff: GOSSIP_MAX_HEIGHT_DIFF,
            max_messages_per_round: GOSSIP_MAX_MESSAGES_PER_ROUND,
            eviction_policy: CacheEvictionPolicy::Combined,
        }
    }
}

/// Network state tracking for height-based eviction
#[derive(Debug, Clone)]
pub struct NetworkState {
    pub current_height: u64,
    pub current_epoch: u64,
    pub last_cleanup_height: u64,
    pub last_cleanup_epoch: u64,
}

impl Default for NetworkState {
    fn default() -> Self {
        Self {
            current_height: 0,
            current_epoch: 0,
            last_cleanup_height: 0,
            last_cleanup_epoch: 0,
        }
    }
}

impl NetworkState {
    /// Update network state with new height/epoch
    pub fn update_height(&mut self, height: u64) {
        if height > self.current_height {
            self.current_height = height;
        }
    }

    /// Update network state with new epoch
    pub fn update_epoch(&mut self, epoch: u64) {
        if epoch > self.current_epoch {
            self.current_epoch = epoch;
            self.last_cleanup_epoch = epoch;
        }
    }

    /// Check if height-based cleanup should be triggered
    pub fn should_cleanup_by_height(&self) -> bool {
        self.current_height > self.last_cleanup_height + GOSSIP_MAX_HEIGHT_DIFF
    }

    /// Mark cleanup as performed
    pub fn mark_cleanup_done(&mut self) {
        self.last_cleanup_height = self.current_height;
    }
}

pub trait GossipSafety {
    /// Validate message against safety limits
    fn validate_message(&self, message: &crate::messages::Message) -> Result<(), String>;

    /// Check if gossip should be limited
    fn should_limit_gossip(&self, rounds: u8, messages_per_round: usize) -> bool;

    /// Enforce cache size limits
    fn enforce_cache_limits(&mut self);
}

/// Memory usage tracking for monitoring
#[derive(Debug, Clone, Default)]
pub struct MemoryUsage {
    pub cache_size: usize,
    pub cache_memory_mb: f64,
    pub peer_count: usize,
    pub connection_count: usize,
    pub message_queue_size: usize,
}

impl MemoryUsage {
    /// Calculate estimated memory usage in MB
    pub fn calculate_memory_mb(&self) -> f64 {
        // Rough estimation: cache entries + peer data + message queues
        let cache_bytes = self.cache_size * 100; // ~100 bytes per cache entry
        let peer_bytes = self.peer_count * 1024; // ~1KB per peer
        let queue_bytes = self.message_queue_size * 1024; // ~1KB per queued message

        (cache_bytes + peer_bytes + queue_bytes) as f64 / 1_048_576.0 // Convert to MB
    }

    /// Check if memory usage is within safe limits
    pub fn is_within_limits(&self) -> bool {
        self.calculate_memory_mb() < 100.0 // 100MB limit
    }
}
