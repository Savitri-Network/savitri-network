//! Savitri P2P Networking Layer
//!
//! This crate provides the peer-to-peer networking infrastructure for Savitri Network.
//! It includes gossip protocols, peer discovery, message routing, and compression
//! capabilities built on top of libp2p.

pub mod groups;
pub mod networking;
pub mod p2p;

// Re-export core types for convenience
pub use p2p::*;

// Re-export group-aware networking
pub use groups::{
    ConsensusMessageRouter, DiscoveryConfig, DiscoveryStats, GroupNetworkConfig, GroupNetworkStats,
    GroupPeerDiscovery, GroupRoutingTable, MessageRoutingConfig, P2PGroupManager, RoutingConfig,
    RoutingMetrics, RoutingStats,
};

#[cfg(feature = "gossipsub")]
pub use p2p::gossip;

#[cfg(feature = "compression")]
pub use savitri_core::compression;

/// P2P network version for compatibility
pub const P2P_VERSION: &str = "0.1.0";

/// Default network port for P2P communication
pub const DEFAULT_P2P_PORT: u16 = 8333;

/// Maximum message size (in bytes)
pub const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024; // 4MB

/// Default connection limits
pub const DEFAULT_MAX_CONNECTIONS: usize = 50;
pub const DEFAULT_MAX_PENDING_CONNECTIONS: usize = 20;

/// Default timeout values (in seconds)
pub const DEFAULT_CONNECTION_TIMEOUT: u64 = 30;
pub const DEFAULT_IDLE_TIMEOUT: u64 = 300;
