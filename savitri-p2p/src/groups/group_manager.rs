//! Group-Aware P2P Network Manager
//!
//! Integrates P2P networking with masternode group formation system
//! for efficient group-based communication and message routing.

use crate::p2p::network::{NetworkConfig, NetworkManager};
use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Group network configuration
#[derive(Debug, Clone)]
pub struct GroupNetworkConfig {
    /// Enable group-based routing
    pub enable_group_routing: bool,
    /// Group message timeout in seconds
    pub group_message_timeout_secs: u64,
    /// Maximum group size for routing
    pub max_group_size: usize,
    /// Enable group health monitoring
    pub enable_health_monitoring: bool,
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    /// Enable message compression
    pub enable_compression: bool,
    /// Maximum message size in bytes
    pub max_message_size: usize,
}

impl Default for GroupNetworkConfig {
    fn default() -> Self {
        Self {
            enable_group_routing: true,
            group_message_timeout_secs: 30,
            max_group_size: 50,
            enable_health_monitoring: true,
            health_check_interval_secs: 60,
            enable_compression: true,
            max_message_size: 1_000_000, // 1MB
        }
    }
}

/// Group message for P2P communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMessage {
    pub message_id: String,
    pub group_id: String,
    pub sender: PeerId,
    pub recipient: Option<PeerId>, // None for broadcast
    pub message_type: GroupMessageType,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub priority: MessagePriority,
}

/// Group message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GroupMessageType {
    /// Group formation announcement
    GroupFormation,
    /// Group assignment notification
    GroupAssignment,
    /// Proposer election message
    ProposerElection,
    /// Block proposal
    BlockProposal,
    /// Consensus vote
    ConsensusVote,
    /// Health check
    HealthCheck,
    /// Custom message
    Custom(String),
}

/// Message priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Group network statistics
#[derive(Debug, Clone, Default)]
pub struct GroupNetworkStats {
    pub total_groups: usize,
    pub active_groups: usize,
    pub total_messages_sent: u64,
    pub total_messages_received: u64,
    pub failed_messages: u64,
    pub average_message_size: f64,
    pub group_health_checks: u64,
    pub routing_table_size: usize,
}

/// P2P Group Manager
pub struct P2PGroupManager {
    config: GroupNetworkConfig,
    local_peer_id: PeerId,
    active_groups: Arc<RwLock<HashMap<String, GroupInfo>>>,
    message_handlers: Arc<RwLock<HashMap<String, Box<dyn GroupMessageHandler>>>>,
    stats: Arc<RwLock<GroupNetworkStats>>,
    network_manager: Arc<RwLock<NetworkManager>>,
}

/// Group information
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: String,
    pub members: Vec<PeerId>,
    pub proposer: Option<PeerId>,
    pub created_at: u64,
    pub last_activity: u64,
    pub health_score: f64,
    pub status: GroupStatus,
}

/// Group status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupStatus {
    Forming,
    Active,
    Inactive,
    Dissolving,
}

/// Trait for handling group messages
#[async_trait::async_trait]
pub trait GroupMessageHandler: Send + Sync {
    async fn handle_message(&self, message: GroupMessage) -> Result<()>;
    fn message_types(&self) -> Vec<GroupMessageType>;
}

impl P2PGroupManager {
    pub fn new(config: GroupNetworkConfig, local_peer_id: PeerId) -> Self {
        // Create network manager with default config
        let network_config = NetworkConfig::default();
        let network_manager =
            NetworkManager::new(network_config).expect("Failed to create network manager");

        Self {
            config,
            local_peer_id,
            active_groups: Arc::new(RwLock::new(HashMap::new())),
            message_handlers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(GroupNetworkStats::default())),
            network_manager: Arc::new(RwLock::new(network_manager)),
        }
    }

    pub fn with_network_manager(
        config: GroupNetworkConfig,
        local_peer_id: PeerId,
        network_manager: NetworkManager,
    ) -> Self {
        Self {
            config,
            local_peer_id,
            active_groups: Arc::new(RwLock::new(HashMap::new())),
            message_handlers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(GroupNetworkStats::default())),
            network_manager: Arc::new(RwLock::new(network_manager)),
        }
    }

    /// Register a new group
    pub async fn register_group(
        &self,
        group_id: String,
        members: Vec<PeerId>,
        proposer: Option<PeerId>,
    ) -> Result<()> {
        let group_info = GroupInfo {
            group_id: group_id.clone(),
            members,
            proposer,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            last_activity: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            health_score: 1.0,
            status: GroupStatus::Active,
        };

        let mut groups = self.active_groups.write().await;
        groups.insert(group_id.clone(), group_info);

        let mut stats = self.stats.write().await;
        stats.total_groups = groups.len();
        stats.active_groups = groups
            .values()
            .filter(|g| g.status == GroupStatus::Active)
            .count();

        info!(
            group_id = %group_id,
            members_count = groups[&group_id].members.len(),
            "Registered P2P group"
        );

        Ok(())
    }

    /// Send message to group
    pub async fn send_group_message(
        &self,
        group_id: &str,
        message_type: GroupMessageType,
        payload: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<String> {
        let message_id = format!(
            "msg_{}_{}",
            group_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );

        let message = GroupMessage {
            message_id: message_id.clone(),
            group_id: group_id.to_string(),
            sender: self.local_peer_id,
            recipient: None, // Broadcast to group
            message_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            priority,
        };

        // Get group members
        let groups = self.active_groups.read().await;
        let group = groups
            .get(group_id)
            .ok_or_else(|| anyhow::anyhow!("Group {} not found", group_id))?;

        // Send to all group members (except self)
        for member in &group.members {
            if member != &self.local_peer_id {
                // Actually send via libp2p network
                let message_data = serde_json::to_vec(&message)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;

                let peer_id_str = member.to_string();
                let mut network_manager = self.network_manager.write().await;

                match network_manager
                    .send_message(peer_id_str, message_data)
                    .await
                {
                    Ok(()) => {
                        debug!(
                            from = %self.local_peer_id,
                            to = %member,
                            group_id = %group_id,
                            message_id = %message_id,
                            "Successfully sent group message"
                        );
                    }
                    Err(e) => {
                        error!(
                            from = %self.local_peer_id,
                            to = %member,
                            group_id = %group_id,
                            message_id = %message_id,
                            error = %e,
                            "Failed to send group message"
                        );
                    }
                }
            }
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_messages_sent += 1;
        stats.average_message_size = (stats.average_message_size
            * (stats.total_messages_sent - 1) as f64
            + message.payload.len() as f64)
            / stats.total_messages_sent as f64;

        Ok(message_id)
    }

    /// Send message to specific peer in group
    pub async fn send_peer_message(
        &self,
        peer_id: PeerId,
        group_id: &str,
        message_type: GroupMessageType,
        payload: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<String> {
        let message_id = format!(
            "msg_{}_{}_{}",
            peer_id,
            group_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );

        let _message = GroupMessage {
            message_id: message_id.clone(),
            group_id: group_id.to_string(),
            sender: self.local_peer_id,
            recipient: Some(peer_id),
            message_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            priority,
        };

        // Actually send via libp2p network
        let message_data = serde_json::to_vec(&_message)
            .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;

        let peer_id_str = peer_id.to_string();
        let mut network_manager = self.network_manager.write().await;

        match network_manager
            .send_message(peer_id_str, message_data)
            .await
        {
            Ok(()) => {
                debug!(
                    from = %self.local_peer_id,
                    to = %peer_id,
                    group_id = %group_id,
                    message_id = %message_id,
                    "Successfully sent peer message"
                );
            }
            Err(e) => {
                error!(
                    from = %self.local_peer_id,
                    to = %peer_id,
                    group_id = %group_id,
                    message_id = %message_id,
                    error = %e,
                    "Failed to send peer message"
                );
            }
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_messages_sent += 1;

        Ok(message_id)
    }

    /// Handle received message
    pub async fn handle_received_message(&self, message: GroupMessage) -> Result<()> {
        // Update group activity
        if let Some(group) = self.active_groups.write().await.get_mut(&message.group_id) {
            group.last_activity = message.timestamp;
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_messages_received += 1;

        // Route to appropriate handlers
        let handlers = self.message_handlers.read().await;
        for handler in handlers.values() {
            if handler.message_types().contains(&message.message_type) {
                if let Err(e) = handler.handle_message(message.clone()).await {
                    error!(
                        message_id = %message.message_id,
                        error = %e,
                        "Failed to handle message"
                    );

                    stats.failed_messages += 1;
                }
            }
        }

        Ok(())
    }

    /// Register message handler
    pub async fn register_message_handler(
        &self,
        handler_id: String,
        handler: Box<dyn GroupMessageHandler>,
    ) {
        let mut handlers = self.message_handlers.write().await;
        handlers.insert(handler_id, handler);
        info!("Registered message handler");
    }

    /// Get group information
    pub async fn get_group_info(&self, group_id: &str) -> Option<GroupInfo> {
        let groups = self.active_groups.read().await;
        groups.get(group_id).cloned()
    }

    /// Get all active groups
    pub async fn get_active_groups(&self) -> Vec<GroupInfo> {
        let groups = self.active_groups.read().await;
        groups
            .values()
            .filter(|g| g.status == GroupStatus::Active)
            .cloned()
            .collect()
    }

    /// Update group health score
    pub async fn update_group_health(&self, group_id: &str, health_score: f64) -> Result<()> {
        let mut groups = self.active_groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            group.health_score = health_score;
            info!(
                group_id = %group_id,
                health_score = health_score,
                "Updated group health score"
            );
        } else {
            warn!(group_id = %group_id, "Group not found for health update");
        }
        Ok(())
    }

    /// Remove group
    pub async fn remove_group(&self, group_id: &str) -> Result<()> {
        let mut groups = self.active_groups.write().await;
        groups.remove(group_id);

        let mut stats = self.stats.write().await;
        stats.total_groups = groups.len();
        stats.active_groups = groups
            .values()
            .filter(|g| g.status == GroupStatus::Active)
            .count();

        info!(group_id = %group_id, "Removed group");
        Ok(())
    }

    /// Get network statistics
    pub async fn get_stats(&self) -> GroupNetworkStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Check if a peer is known (has an active connection)
    pub async fn is_known_peer(&self, peer_id: &PeerId) -> Result<bool> {
        let network_manager = self.network_manager.read().await;
        let connected_peers = network_manager.connected_peers().await;
        let peer_id_str = peer_id.to_string();

        // Check if peer is in network manager's connections
        Ok(connected_peers.contains(&peer_id_str))
    }

    /// Get network manager for advanced operations
    pub async fn get_network_manager(&self) -> Arc<RwLock<NetworkManager>> {
        self.network_manager.clone()
    }

    /// Perform health check on all groups
    pub async fn perform_health_checks(&self) -> Result<Vec<(String, f64)>> {
        let mut results = Vec::new();
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let groups = self.active_groups.read().await;
        for (group_id, group) in groups.iter() {
            // Simple health calculation based on activity and member count
            let activity_score = if current_time - group.last_activity < 300 {
                1.0
            } else {
                0.5
            };

            let member_score = if group.members.len() >= 3 {
                1.0
            } else {
                group.members.len() as f64 / 3.0
            };

            let health_score = (activity_score + member_score) / 2.0;
            results.push((group_id.clone(), health_score));
        }

        let mut stats = self.stats.write().await;
        stats.group_health_checks += 1;

        Ok(results)
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting P2P group manager");

        if self.config.enable_health_monitoring {
            let manager = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                    manager.config.health_check_interval_secs,
                ));

                loop {
                    interval.tick().await;
                    if let Err(e) = manager.perform_health_checks().await {
                        error!("Health check failed: {}", e);
                    }
                }
            });
        }

        Ok(())
    }

    /// Stop the group manager
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping P2P group manager");
        Ok(())
    }
}

impl Clone for P2PGroupManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_peer_id: self.local_peer_id,
            active_groups: self.active_groups.clone(),
            message_handlers: self.message_handlers.clone(),
            stats: self.stats.clone(),
            network_manager: self.network_manager.clone(),
        }
    }
}
