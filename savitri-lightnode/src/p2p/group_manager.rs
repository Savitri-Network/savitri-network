//! P2P Group Manager for Light Nodes
//!
//! This module handles receiving and managing P2P group assignments from masternodes.

#![allow(dead_code)]

use anyhow::Result;
use libp2p::{gossipsub::IdentTopic, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Maximum allowed members per group (prevents memory exhaustion from forged announcements)
const MAX_GROUP_MEMBERS: usize = 256;

/// Maximum clock skew allowed for announcement timestamps (5 minutes)
const MAX_ANNOUNCE_TIMESTAMP_SKEW_SECS: u64 = 300;

/// Maximum number of simultaneous groups a lightnode can participate in
const MAX_ACTIVE_GROUPS: usize = 3;

/// Group announcement message from masternode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAnnounce {
    /// Epoch for this group assignment
    pub epoch: u64,
    /// Unique group identifier
    pub group_id: String,
    /// List of lightnode members in this group
    pub members: Vec<String>,
    /// Peer ID -> multiaddr so we can dial group members for intra-group mesh
    #[serde(default)]
    pub member_addresses: std::collections::HashMap<String, String>,
    /// Group proposer (first member)
    pub proposer: String,
    /// Announcement timestamp
    pub timestamp: u64,
    /// Ed25519 signature from the masternode that issued the announcement (hex-encoded)
    #[serde(default)]
    pub signature: Option<String>,
    /// Public key of the signing masternode (hex-encoded, 32 bytes)
    #[serde(default)]
    pub signer_pubkey: Option<String>,
    /// Shard IDs assigned to this group (for TX routing)
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
    /// Total number of shards (65,536)
    #[serde(default)]
    pub num_shards: u32,
}

/// P2P group manager for light nodes (supports up to MAX_ACTIVE_GROUPS simultaneous groups)
pub struct P2PGroupManager {
    /// Local node ID
    local_node_id: String,
    /// Active groups indexed by group_id (up to MAX_ACTIVE_GROUPS)
    active_groups: Arc<RwLock<HashMap<String, GroupAnnounce>>>,
    /// Group members (peer connections)
    group_members: Arc<RwLock<HashMap<String, PeerId>>>,
    /// Groups where this node is proposer
    proposer_groups: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Group communication topic
    group_topic: IdentTopic,
    /// Known masternode public keys (hex-encoded) authorized to send group announcements
    known_masternodes: Arc<RwLock<std::collections::HashSet<String>>>,
    /// Last processed epoch — reject stale/replayed announcements
    last_epoch: Arc<RwLock<u64>>,
    /// Used to compute the local epoch estimate the same way the masternode
    /// does, so the MAX_EPOCH_DRIFT guard does not falsely reject announces.
    genesis_timestamp_ms: u64,
    /// Heartbeat interval in milliseconds (slot duration).
    heartbeat_interval_ms: u64,
    /// Number of slots per epoch.
    slots_per_epoch: u64,
}

impl P2PGroupManager {
    pub fn new(
        local_node_id: String,
        genesis_timestamp_ms: u64,
        heartbeat_interval_ms: u64,
        slots_per_epoch: u64,
    ) -> Self {
        Self {
            local_node_id,
            active_groups: Arc::new(RwLock::new(HashMap::new())),
            group_members: Arc::new(RwLock::new(HashMap::new())),
            proposer_groups: Arc::new(RwLock::new(std::collections::HashSet::new())),
            group_topic: IdentTopic::new("/savitri/lightnode/group/announce/1"),
            known_masternodes: Arc::new(RwLock::new(std::collections::HashSet::new())),
            last_epoch: Arc::new(RwLock::new(0)),
            genesis_timestamp_ms,
            heartbeat_interval_ms,
            slots_per_epoch,
        }
    }

    /// Register a known masternode public key (hex-encoded) that is authorized to send announcements.
    pub async fn add_known_masternode(&self, pubkey_hex: String) {
        self.known_masternodes.write().await.insert(pubkey_hex);
    }

    /// Get the group topic for subscription
    pub fn get_topic(&self) -> IdentTopic {
        self.group_topic.clone()
    }

    /// Validate group announcement fields and signature before processing.
    fn validate_announcement(announce: &GroupAnnounce) -> Result<()> {
        // SECURITY: Reject announcements with too many members (memory exhaustion)
        if announce.members.len() > MAX_GROUP_MEMBERS {
            anyhow::bail!(
                "Group announcement has {} members, exceeding limit of {}",
                announce.members.len(),
                MAX_GROUP_MEMBERS
            );
        }

        // SECURITY: Validate timestamp is recent (prevents replayed announcements)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if announce.timestamp > now + MAX_ANNOUNCE_TIMESTAMP_SKEW_SECS {
            anyhow::bail!(
                "Group announcement timestamp {} is too far in the future (now: {})",
                announce.timestamp,
                now
            );
        }

        if announce.timestamp < now.saturating_sub(MAX_ANNOUNCE_TIMESTAMP_SKEW_SECS) {
            anyhow::bail!(
                "Group announcement timestamp {} is too old (now: {})",
                announce.timestamp,
                now
            );
        }

        // SECURITY: Validate group_id is not excessively long (prevent memory abuse)
        if announce.group_id.len() > 256 {
            anyhow::bail!("Group ID is too long: {} bytes", announce.group_id.len());
        }

        // SECURITY: Validate proposer is in the members list
        if !announce.members.contains(&announce.proposer) {
            anyhow::bail!(
                "Proposer {} is not a member of the group",
                announce.proposer
            );
        }

        Ok(())
    }

    /// Verify announcement signature if present (signature is from the masternode that issued it).
    async fn verify_announcement_signature(&self, announce: &GroupAnnounce) -> Result<bool> {
        let (sig_hex, pubkey_hex) = match (&announce.signature, &announce.signer_pubkey) {
            (Some(sig), Some(pk)) => (sig.clone(), pk.clone()),
            _ => {
                // SECURITY: Reject unsigned announcements
                warn!(
                    group_id = %announce.group_id,
                    "Group announcement has no signature — rejected"
                );
                return Ok(false);
            }
        };

        // SECURITY: Check signer is a known masternode
        let known = self.known_masternodes.read().await;
        if !known.is_empty() && !known.contains(&pubkey_hex) {
            warn!(
                group_id = %announce.group_id,
                signer = %pubkey_hex,
                "Group announcement signed by unknown masternode — rejected"
            );
            return Ok(false);
        }
        drop(known);

        // Verify Ed25519 signature
        let sig_bytes =
            hex::decode(&sig_hex).map_err(|e| anyhow::anyhow!("Invalid signature hex: {}", e))?;
        let pk_bytes =
            hex::decode(&pubkey_hex).map_err(|e| anyhow::anyhow!("Invalid pubkey hex: {}", e))?;

        if sig_bytes.len() != 64 || pk_bytes.len() != 32 {
            warn!(
                group_id = %announce.group_id,
                "Invalid signature/pubkey length in group announcement"
            );
            return Ok(false);
        }

        let pk_array: [u8; 32] = pk_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid pubkey length"))?;
        let sig_array: [u8; 64] = sig_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;

        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pk_array)
            .map_err(|e| anyhow::anyhow!("Invalid verifying key: {}", e))?;
        let signature = ed25519_dalek::Signature::from_bytes(&sig_array);

        // Reconstruct the signed message: epoch || group_id || proposer || timestamp || members_count
        let mut message = Vec::new();
        message.extend_from_slice(&announce.epoch.to_le_bytes());
        message.extend_from_slice(announce.group_id.as_bytes());
        message.extend_from_slice(announce.proposer.as_bytes());
        message.extend_from_slice(&announce.timestamp.to_le_bytes());
        message.extend_from_slice(&(announce.members.len() as u64).to_le_bytes());

        use ed25519_dalek::Verifier;
        match verifying_key.verify(&message, &signature) {
            Ok(()) => Ok(true),
            Err(e) => {
                warn!(
                    group_id = %announce.group_id,
                    error = %e,
                    "Group announcement signature verification failed"
                );
                Ok(false)
            }
        }
    }

    /// Process incoming group announcement from masternode
    pub async fn process_group_announcement(&self, announce: GroupAnnounce) -> Result<()> {
        // SECURITY: Validate announcement fields first
        Self::validate_announcement(&announce)?;

        // SECURITY: Verify announcement signature (if present)
        if !self.verify_announcement_signature(&announce).await? {
            anyhow::bail!(
                "Group announcement for group {} failed signature verification",
                announce.group_id
            );
        }

        // SECURITY: Reject unreasonably future epochs.
        //
        // `unix_ms / 100_000` (wall-clock absolute) while the masternode
        // computes `(unix_ms - genesis_ms) / 100_000` (genesis-relative).
        // The drift between the two is `genesis_ms / 100_000` ≈ 17 million
        // epoch units in 2026, which made the lightnode reject every
        // GroupAnnounce for `MAX_EPOCH_DRIFT=100`. Switching to the
        // canonical primitive in savitri-consensus fixes the desync.
        {
            const MAX_EPOCH_DRIFT: u64 = 100;
            let estimated_current_epoch = savitri_consensus::primitives::epoch::current_epoch(
                savitri_consensus::primitives::epoch::now_ms(),
                self.genesis_timestamp_ms,
                self.heartbeat_interval_ms,
                self.slots_per_epoch,
            );
            if announce.epoch > estimated_current_epoch + MAX_EPOCH_DRIFT {
                warn!(
                    announce_epoch = announce.epoch,
                    estimated_current = estimated_current_epoch,
                    "Rejecting group announcement with unreasonably future epoch"
                );
                return Ok(());
            }
        }

        // SECURITY: Reject stale epoch AND update atomically (single write lock)
        // Holding last_epoch write lock across the check+update prevents TOCTOU races
        // where a concurrent announcement could slip through during the gap.
        {
            let mut last_epoch_guard = self.last_epoch.write().await;
            if announce.epoch < *last_epoch_guard {
                warn!(
                    group_id = %announce.group_id,
                    announce_epoch = announce.epoch,
                    last_epoch = *last_epoch_guard,
                    "Rejecting stale group announcement (epoch regression)"
                );
                return Ok(());
            }

            info!(
                group_id = %announce.group_id,
                epoch = announce.epoch,
                members_count = announce.members.len(),
                proposer = %announce.proposer,
                local_node_id = %self.local_node_id,
                "ANNOUNCE RECEIVED (L<-M)"
            );
            info!(
                group_id = %announce.group_id,
                epoch = announce.epoch,
                members_count = announce.members.len(),
                "Received group announcement from masternode"
            );
            debug!(
                group_id = %announce.group_id,
                local_node_id = %self.local_node_id,
                members = ?announce.members,
                "Group announcement members list"
            );

            // Check if we're a member of this group
            if !announce.members.contains(&self.local_node_id) {
                warn!(
                    group_id = %announce.group_id,
                    local_node_id = %self.local_node_id,
                    "Received group announcement but we're not a member"
                );
                return Ok(());
            }

            // Atomically update epoch + groups + proposer under lock
            // Add to active_groups (up to MAX_ACTIVE_GROUPS)
            let mut active = self.active_groups.write().await;

            // If this group_id already exists, update it in place
            if active.contains_key(&announce.group_id) {
                active.insert(announce.group_id.clone(), announce.clone());
            } else if active.len() < MAX_ACTIVE_GROUPS {
                active.insert(announce.group_id.clone(), announce.clone());
            } else {
                // At capacity — evict oldest group (lowest epoch) to make room
                let mut pg = self.proposer_groups.write().await;
                if let Some(oldest_id) = active
                    .iter()
                    .min_by_key(|(_, g)| g.epoch)
                    .map(|(id, _)| id.clone())
                {
                    info!(
                        evicted_group = %oldest_id,
                        new_group = %announce.group_id,
                        "Evicting oldest group to make room for new assignment (max {} groups)",
                        MAX_ACTIVE_GROUPS,
                    );
                    active.remove(&oldest_id);
                    pg.remove(&oldest_id);
                }
                drop(pg);
                active.insert(announce.group_id.clone(), announce.clone());
            }
            drop(active);

            *last_epoch_guard = announce.epoch;
        }

        // Update proposer status
        {
            let is_proposer_in_this_group = announce.proposer == self.local_node_id;
            let mut pg = self.proposer_groups.write().await;
            if is_proposer_in_this_group {
                pg.insert(announce.group_id.clone());
            } else {
                pg.remove(&announce.group_id);
            }
        }

        let is_proposer_value = !self.proposer_groups.read().await.is_empty();
        let active_count = self.active_groups.read().await.len();
        info!(
            group_id = %announce.group_id,
            is_proposer = %is_proposer_value,
            active_groups = active_count,
            "Updated group assignment"
        );
        info!(
            group_id = %announce.group_id,
            local_node_id = %self.local_node_id,
            is_member = true,
            active_groups = active_count,
            "GROUP ASSIGNED (L) - local node is in group"
        );

        // Start intra-group communication
        self.start_intra_group_communication(&announce).await?;
        info!(
            group_id = %announce.group_id,
            "Intra-group sync initialized"
        );

        Ok(())
    }

    /// Set default group assignment from local config
    pub async fn set_default_group(&self, mut announce: GroupAnnounce) -> Result<()> {
        if !announce.members.contains(&self.local_node_id) {
            announce.members.push(self.local_node_id.clone());
        }

        self.process_group_announcement(announce).await
    }

    /// Start communication with other group members
    async fn start_intra_group_communication(&self, group: &GroupAnnounce) -> Result<()> {
        info!("Starting intra-group communication for {}", group.group_id);
        info!(
            group_id = %group.group_id,
            "Initialized Sync"
        );

        // Start latency measurement with group members
        self.start_latency_measurement(group).await?;

        info!(
            group_id = %group.group_id,
            epoch = group.epoch,
            members_count = group.members.len(),
            "Starting intra-group PoU consensus"
        );

        // Start PoU score sharing
        self.start_pou_sharing(group).await?;

        // Start proposer election
        self.start_proposer_election(group).await?;

        Ok(())
    }

    /// Start latency measurement with group members
    async fn start_latency_measurement(&self, group: &GroupAnnounce) -> Result<()> {
        info!(
            "Starting latency measurement with {} group members",
            group.members.len()
        );

        // Trigger intra-group communication to start latency measurement
        // This will be handled by the IntraGroupCommunication instance
        debug!("Triggered latency measurement for group members");

        Ok(())
    }

    /// Start PoU score sharing with group members
    async fn start_pou_sharing(&self, group: &GroupAnnounce) -> Result<()> {
        info!("Starting PoU score sharing in group {}", group.group_id);

        // Trigger intra-group communication to share PoU scores
        // This will be handled by the IntraGroupCommunication instance
        debug!("Triggered PoU score sharing for group members");

        Ok(())
    }

    /// Start proposer election process
    async fn start_proposer_election(&self, group: &GroupAnnounce) -> Result<()> {
        info!("Starting proposer election for group {}", group.group_id);

        // Trigger intra-group communication to start proposer election
        // This will be handled by the IntraGroupCommunication instance
        debug!("Triggered proposer election process");

        Ok(())
    }

    /// Check if we're the current proposer (derived from proposer_groups)
    pub async fn is_current_proposer(&self) -> bool {
        !self.proposer_groups.read().await.is_empty()
    }

    /// Get current group information (returns the group with highest epoch from active_groups)
    pub async fn get_current_group(&self) -> Option<GroupAnnounce> {
        let active = self.active_groups.read().await;
        active.values().max_by_key(|g| g.epoch).cloned()
    }

    /// Non-blocking version of get_current_group using try_read().
    /// Returns None if the lock cannot be acquired immediately.
    pub fn get_current_group_cached(&self) -> Option<GroupAnnounce> {
        match self.active_groups.try_read() {
            Ok(active) => active.values().max_by_key(|g| g.epoch).cloned(),
            Err(_) => None,
        }
    }

    /// Get group members (excluding self) from the current primary group
    pub async fn get_group_members(&self) -> Vec<String> {
        if let Some(group) = self.get_current_group().await {
            group
                .members
                .iter()
                .filter(|member| *member != &self.local_node_id)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Update group member peer connection
    pub async fn update_member_peer(&self, member_id: String, peer_id: PeerId) {
        let mut members = self.group_members.write().await;
        members.insert(member_id, peer_id);
        debug!("Updated peer mapping for group member");
    }

    /// Get peer ID for group member
    pub async fn get_member_peer(&self, member_id: &str) -> Option<PeerId> {
        let members = self.group_members.read().await;
        members.get(member_id).copied()
    }

    /// Get all active groups this lightnode participates in
    pub async fn get_active_groups(&self) -> HashMap<String, GroupAnnounce> {
        self.active_groups.read().await.clone()
    }

    /// Get a specific active group by ID
    pub async fn get_group(&self, group_id: &str) -> Option<GroupAnnounce> {
        self.active_groups.read().await.get(group_id).cloned()
    }

    /// Check if this node is proposer in a specific group
    pub async fn is_proposer_in_group(&self, group_id: &str) -> bool {
        self.proposer_groups.read().await.contains(group_id)
    }

    /// Get all group IDs where this node is proposer
    pub async fn get_proposer_group_ids(&self) -> Vec<String> {
        self.proposer_groups.read().await.iter().cloned().collect()
    }

    /// Get the number of active groups
    pub async fn active_group_count(&self) -> usize {
        self.active_groups.read().await.len()
    }

    /// Assign a transaction to a group using hash-based partitioning.
    /// Returns the group_id the transaction should be proposed in.
    /// Uses `tx_hash % num_active_groups` for deterministic assignment.
    pub async fn assign_tx_to_group(&self, tx_hash: &[u8]) -> Option<String> {
        let groups = self.active_groups.read().await;
        if groups.is_empty() {
            return None;
        }
        let mut group_ids: Vec<&String> = groups.keys().collect();
        group_ids.sort(); // deterministic ordering
                          // Simple hash-based assignment
        let hash_value = if tx_hash.len() >= 8 {
            u64::from_le_bytes(tx_hash[..8].try_into().unwrap_or([0u8; 8]))
        } else {
            tx_hash
                .iter()
                .fold(0u64, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as u64))
        };
        let idx = (hash_value % group_ids.len() as u64) as usize;
        Some(group_ids[idx].clone())
    }

    /// Remove a specific group from active groups (e.g., on group dissolution)
    pub async fn remove_group(&self, group_id: &str) {
        let mut active = self.active_groups.write().await;
        active.remove(group_id);
        let remaining = active.len();
        drop(active);

        self.proposer_groups.write().await.remove(group_id);

        info!(
            removed_group = %group_id,
            remaining_groups = remaining,
            "Removed group from active set"
        );
    }
}
