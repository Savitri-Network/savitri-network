//! Group-Aware P2P Networking
//!
//! This module provides group-aware networking functionality for Savitri P2P layer,
//! integrating with the masternode group formation system for efficient group communication.

pub mod consensus_messaging;
pub mod group_manager;
pub mod group_routing;
pub mod peer_discovery;

// Re-export main types for convenience
pub use consensus_messaging::{
    ConsensusMessageRouter, GroupConsensusMessage, MessageRoutingConfig, RoutingStats,
};
pub use group_manager::MessagePriority;
pub use group_manager::{
    GroupInfo, GroupMessage, GroupMessageHandler, GroupMessageType, GroupNetworkConfig,
    GroupNetworkStats, GroupStatus, P2PGroupManager,
};
pub use group_routing::{GroupRoute, GroupRoutingTable, RouteType, RoutingConfig, RoutingMetrics};
pub use peer_discovery::{
    DiscoveryConfig, DiscoveryStats, GroupPeerDiscovery, GroupPeerInfo, PeerGroupStatus,
};
