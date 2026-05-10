//! Sharding module for Savitri Light Node
//!
//! This module provides sharding functionality for light nodes.

// Sharding module — group-to-shard mapping integrated

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hasher;
use tracing::{info, warn};

/// Shard configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// Number of shards
    pub shard_count: u32,
    /// Shard size
    pub shard_size: u64,
    /// Replication factor
    pub replication_factor: u32,
    /// Shard algorithm
    pub algorithm: ShardAlgorithm,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            shard_count: 65_536,
            shard_size: 1000000, // 1M items per shard
            replication_factor: 3,
            algorithm: ShardAlgorithm::ConsistentHash,
        }
    }
}

/// Shard algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ShardAlgorithm {
    /// Consistent hashing
    ConsistentHash,
    /// Range-based sharding
    Range,
    /// Random sharding
    Random,
}

/// Shard identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(pub u32);

impl ShardId {
    /// Create new shard ID
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get shard number
    pub fn shard_num(&self) -> u32 {
        self.0
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// Convert from bytes
    pub fn from_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }
}

/// Shard manager
#[derive(Debug)]
pub struct ShardManager {
    /// Configuration
    config: ShardConfig,
    /// Shard metadata
    shards: HashMap<ShardId, ShardMetadata>,
    /// Ring for consistent hashing
    ring: Option<consistent_hash::Ring>,
    /// Group-to-shard mapping: each group handles a subset of shards.
    /// Populated by `assign_shards_to_groups()`.
    group_shards: HashMap<String, Vec<ShardId>>,
    /// Reverse mapping: shard → group_id
    shard_to_group: HashMap<ShardId, String>,
}

/// Shard metadata
#[derive(Debug, Clone)]
pub struct ShardMetadata {
    /// Shard ID
    pub id: ShardId,
    /// Shard size
    pub size: u64,
    /// Node assignments
    pub nodes: Vec<String>,
    /// Creation timestamp
    pub created_at: u64,
    /// Last updated timestamp
    pub updated_at: u64,
    /// Health status
    pub health: ShardHealth,
}

/// Shard health status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ShardHealth {
    /// Healthy
    Healthy,
    /// Degraded
    Degraded,
    /// Unhealthy
    Unhealthy,
    /// Offline
    Offline,
}

impl ShardManager {
    /// Create new shard manager
    pub fn new(config: ShardConfig) -> Result<Self> {
        let mut shards = HashMap::new();

        // Initialize shards
        for i in 0..config.shard_count {
            let shard_id = ShardId::new(i);
            let metadata = ShardMetadata {
                id: shard_id,
                size: 0,
                nodes: Vec::new(),
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                health: ShardHealth::Healthy,
            };
            shards.insert(shard_id, metadata);
        }

        let ring = match config.algorithm {
            ShardAlgorithm::ConsistentHash => {
                let mut ring = consistent_hash::Ring::new(config.shard_count as usize);
                for i in 0..config.shard_count {
                    ring.add(format!("shard-{}", i).as_bytes());
                }
                Some(ring)
            }
            _ => None,
        };

        Ok(Self {
            config,
            shards,
            ring,
            group_shards: HashMap::new(),
            shard_to_group: HashMap::new(),
        })
    }

    /// Get shard for key
    pub fn get_shard(&self, key: &[u8]) -> Result<ShardId> {
        match &self.ring {
            Some(ring) => {
                let shard_name = ring.get(key)?;
                let shard_num = shard_name
                    .strip_prefix(b"shard-")
                    .and_then(|s| std::str::from_utf8(s).ok())
                    .and_then(|s: &str| s.parse::<u32>().ok())
                    .ok_or_else(|| anyhow::anyhow!("Invalid shard name"))?;
                Ok(ShardId::new(shard_num))
            }
            None => {
                // Fallback to simple hash-based sharding
                let hash = self.simple_hash(key);
                let shard_num = (hash % self.config.shard_count as u64) as u32;
                Ok(ShardId::new(shard_num))
            }
        }
    }

    /// Get multiple shards for key (for replication)
    pub fn get_shards_for_key(&self, key: &[u8]) -> Result<Vec<ShardId>> {
        let primary_shard = self.get_shard(key)?;
        let mut shards = vec![primary_shard];

        // Add replica shards
        for i in 1..self.config.replication_factor {
            let replica_shard =
                ShardId::new((primary_shard.shard_num() + i) % self.config.shard_count);
            if !shards.contains(&replica_shard) {
                shards.push(replica_shard);
            }
        }

        Ok(shards)
    }

    /// Get shard metadata
    pub fn get_shard_metadata(&self, shard_id: ShardId) -> Option<&ShardMetadata> {
        self.shards.get(&shard_id)
    }

    /// Update shard metadata
    pub fn update_shard_metadata(
        &mut self,
        shard_id: ShardId,
        metadata: ShardMetadata,
    ) -> Result<()> {
        self.shards.insert(shard_id, metadata);
        Ok(())
    }

    /// Get all healthy shards
    pub fn get_healthy_shards(&self) -> Vec<ShardId> {
        self.shards
            .iter()
            .filter(|(_, m)| m.health == ShardHealth::Healthy)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get shard statistics
    pub fn get_shard_stats(&self) -> ShardStats {
        let total_shards = self.shards.len() as u32;
        let healthy_shards = self.get_healthy_shards().len() as u32;
        let total_size: u64 = self.shards.values().map(|m| m.size).sum();

        ShardStats {
            total_shards,
            healthy_shards,
            total_size,
            average_size: if total_shards > 0 {
                total_size / total_shards as u64
            } else {
                0
            },
            replication_factor: self.config.replication_factor,
        }
    }

    /// Simple hash function for fallback
    fn simple_hash(&self, key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::default();
        hasher.write(key);
        hasher.finish()
    }

    /// Rebalance shards
    pub fn rebalance(&mut self) -> Result<()> {
        info!("Starting rebalance of {} shards", self.config.shard_count);

        let mut total_size: u64 = 0;
        let mut healthy_shards = Vec::new();
        let mut overloaded_shards = Vec::new();
        let mut underloaded_shards = Vec::new();

        // Analyze current shard distribution
        for (shard_id, metadata) in &self.shards {
            total_size += metadata.size;

            match metadata.health {
                ShardHealth::Healthy => {
                    healthy_shards.push((*shard_id, metadata.size));

                    // Identify load imbalance
                    let avg_size = if self.config.shard_count > 0 {
                        total_size / self.config.shard_count as u64
                    } else {
                        0
                    };

                    if metadata.size > avg_size * 12 / 10 {
                        // 120% of average
                        overloaded_shards.push((*shard_id, metadata.size));
                    } else if metadata.size < avg_size * 8 / 10 {
                        // 80% of average
                        underloaded_shards.push((*shard_id, metadata.size));
                    }
                }
                ShardHealth::Degraded | ShardHealth::Unhealthy | ShardHealth::Offline => {
                    warn!(shard_id = %shard_id.0, health = ?metadata.health, "Skipping unhealthy shard in rebalance");
                }
            }
        }

        if overloaded_shards.is_empty() || underloaded_shards.is_empty() {
            info!("No rebalancing needed - shards are balanced");
            return Ok(());
        }

        info!(
            overloaded_count = overloaded_shards.len(),
            underloaded_count = underloaded_shards.len(),
            "Rebalancing shards"
        );

        // Sort overloaded shards by size (descending) and underloaded by size (ascending)
        overloaded_shards.sort_by(|a, b| b.1.cmp(&a.1));
        underloaded_shards.sort_by(|a, b| a.1.cmp(&b.1));

        let mut moved_data = 0u64;
        let rebalance_threshold = self.config.shard_size / 10; // 10% of shard size

        // Redistribute data from overloaded to underloaded shards
        for (overloaded_id, overloaded_size) in &overloaded_shards {
            for (underloaded_id, underloaded_size) in &underloaded_shards {
                if moved_data >= rebalance_threshold {
                    break;
                }

                let target_size =
                    (total_size / self.config.shard_count as u64).min(self.config.shard_size);

                if *overloaded_size > target_size && *underloaded_size < target_size {
                    let amount_to_move = ((*overloaded_size - target_size)
                        .min(target_size - *underloaded_size))
                    .min(rebalance_threshold - moved_data);

                    if amount_to_move > 0 {
                        // Update shard sizes (in real implementation, this would move actual data)
                        if let Some(overloaded_meta) = self.shards.get_mut(overloaded_id) {
                            overloaded_meta.size =
                                overloaded_meta.size.saturating_sub(amount_to_move);
                            overloaded_meta.updated_at = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                        }

                        if let Some(underloaded_meta) = self.shards.get_mut(underloaded_id) {
                            underloaded_meta.size =
                                underloaded_meta.size.saturating_add(amount_to_move);
                            underloaded_meta.updated_at = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                        }

                        moved_data += amount_to_move;

                        info!(
                            from_shard = overloaded_id.0,
                            to_shard = underloaded_id.0,
                            amount = amount_to_move,
                            "Moved data between shards"
                        );
                    }
                }
            }
        }

        // Update consistent hash ring if needed
        if let Some(ring) = &mut self.ring {
            // Rebuild ring with updated shard distribution
            *ring = consistent_hash::Ring::new(self.config.shard_count as usize);
            for shard_id in self.shards.keys() {
                if self.shards[shard_id].health == ShardHealth::Healthy {
                    ring.add(format!("shard-{}", shard_id.0).as_bytes());
                }
            }
        }

        info!(
            moved_data = moved_data,
            total_shards = self.config.shard_count,
            "Rebalancing completed"
        );

        Ok(())
    }

    /// Add node to shard
    pub fn add_node_to_shard(&mut self, shard_id: ShardId, node_id: String) -> Result<()> {
        if let Some(metadata) = self.shards.get_mut(&shard_id) {
            if !metadata.nodes.contains(&node_id) {
                metadata.nodes.push(node_id);
                metadata.updated_at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        }
        Ok(())
    }

    /// Remove node from shard
    pub fn remove_node_from_shard(&mut self, shard_id: ShardId, node_id: &str) -> Result<()> {
        if let Some(metadata) = self.shards.get_mut(&shard_id) {
            metadata.nodes.retain(|n| n != node_id);
            metadata.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }
        Ok(())
    }

    /// Update shard health
    pub fn update_shard_health(&mut self, shard_id: ShardId, health: ShardHealth) -> Result<()> {
        if let Some(metadata) = self.shards.get_mut(&shard_id) {
            metadata.health = health;
            metadata.updated_at = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }
        Ok(())
    }
}

/// Shard statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStats {
    /// Total number of shards
    pub total_shards: u32,
    /// Number of healthy shards
    pub healthy_shards: u32,
    /// Total size across all shards
    pub total_size: u64,
    /// Average shard size
    pub average_size: u64,
    /// Replication factor
    pub replication_factor: u32,
}

/// Consistent hashing ring (simplified implementation)
pub mod consistent_hash {
    use std::collections::HashMap;

    /// Simple consistent hashing ring
    #[derive(Debug)]
    pub struct Ring {
        /// Ring entries
        entries: Vec<Vec<u8>>,
        /// Virtual nodes per physical node
        virtual_nodes: usize,
        /// Hash map for lookups
        hash_map: HashMap<u64, usize>,
    }

    impl Ring {
        /// Create new ring
        pub fn new(shard_count: usize) -> Self {
            let virtual_nodes = 150; // Standard number of virtual nodes
            let mut ring = Self {
                entries: Vec::new(),
                virtual_nodes,
                hash_map: HashMap::new(),
            };

            // Add virtual nodes
            for i in 0..shard_count {
                for j in 0..virtual_nodes {
                    let virtual_node = format!("{}-{}", i, j);
                    ring.add(virtual_node.as_bytes());
                }
            }

            ring
        }

        /// Add entry to ring
        pub fn add(&mut self, entry: &[u8]) {
            let index = self.entries.len();
            self.entries.push(entry.to_vec());

            // Add multiple hash mappings for better distribution
            for i in 0..3 {
                let hash = self.hash_with_seed(entry, i);
                self.hash_map.insert(hash, index);
            }
        }

        /// Get entry from ring
        pub fn get(&self, key: &[u8]) -> Result<&Vec<u8>, anyhow::Error> {
            let hash = self.hash_with_seed(key, 0);
            match self.hash_map.get(&hash) {
                Some(index) => Ok(&self.entries[*index]),
                None => Err(anyhow::anyhow!("Key not found in ring")),
            }
        }

        /// Hash function with seed
        fn hash_with_seed(&self, key: &[u8], _seed: u32) -> u64 {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::Hasher;
            let mut hasher = DefaultHasher::default();
            hasher.write(key);
            hasher.write_u32(0); // Use fixed seed 0
            hasher.finish()
        }
    }
}

impl ShardManager {
    /// Assign shards to groups for group-aware block production.
    /// Each group handles shard_count/num_groups shards (round-robin).
    /// Example: 16 shards, 4 groups → group_0 handles shards 0,4,8,12.
    pub fn assign_shards_to_groups(&mut self, group_ids: &[String]) {
        self.group_shards.clear();
        self.shard_to_group.clear();

        if group_ids.is_empty() {
            warn!("No groups provided for shard assignment");
            return;
        }

        for (shard_idx, shard_id) in (0..self.config.shard_count).map(ShardId::new).enumerate() {
            let group_idx = shard_idx % group_ids.len();
            let group_id = &group_ids[group_idx];

            self.group_shards
                .entry(group_id.clone())
                .or_default()
                .push(shard_id);
            self.shard_to_group.insert(shard_id, group_id.clone());
        }

        for (group_id, shards) in &self.group_shards {
            info!(
                group_id = %group_id,
                shard_count = shards.len(),
                shards = ?shards.iter().map(|s| s.0).collect::<Vec<_>>(),
                "Shards assigned to group"
            );
        }
    }

    /// Get the group responsible for a transaction based on sender address.
    /// Returns None if shards haven't been assigned to groups yet.
    pub fn get_group_for_tx(&self, sender_address: &[u8]) -> Option<String> {
        let shard_id = self.get_shard(sender_address).ok()?;
        self.shard_to_group.get(&shard_id).cloned()
    }

    /// Get all shard IDs assigned to a specific group.
    pub fn get_shards_for_group(&self, group_id: &str) -> Vec<ShardId> {
        self.group_shards.get(group_id).cloned().unwrap_or_default()
    }

    /// Check if a transaction belongs to the specified group's shards.
    /// Used by block proposers to filter TX during block production.
    pub fn is_tx_in_group(&self, sender_address: &[u8], group_id: &str) -> bool {
        match self.get_shard(sender_address) {
            Ok(shard_id) => self
                .shard_to_group
                .get(&shard_id)
                .map(|gid| gid == group_id)
                .unwrap_or(false),
            Err(_) => false,
        }
    }
}

/// Cross-shard transaction coordinator
#[derive(Debug)]
pub struct CrossShardCoordinator {
    /// Shard manager
    shard_manager: ShardManager,
    /// Pending cross-shard transactions
    pending_transactions: HashMap<String, CrossShardTransaction>,
}

/// Cross-shard transaction
#[derive(Debug, Clone)]
pub struct CrossShardTransaction {
    /// Transaction ID
    pub tx_id: String,
    /// Source shard
    pub source_shard: ShardId,
    /// Target shards
    pub target_shards: Vec<ShardId>,
    /// Transaction data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Status
    pub status: CrossShardStatus,
}

/// Cross-shard transaction status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrossShardStatus {
    /// Pending
    Pending,
    /// In progress
    InProgress,
    /// Completed
    Completed,
    /// Failed
    Failed,
}

impl CrossShardCoordinator {
    /// Create new coordinator
    pub fn new(shard_manager: ShardManager) -> Self {
        Self {
            shard_manager,
            pending_transactions: HashMap::new(),
        }
    }

    /// Submit cross-shard transaction
    pub fn submit_transaction(&mut self, tx: CrossShardTransaction) -> Result<()> {
        self.pending_transactions.insert(tx.tx_id.clone(), tx);
        Ok(())
    }

    /// Get transaction status
    pub fn get_transaction_status(&self, tx_id: &str) -> Option<&CrossShardTransaction> {
        self.pending_transactions.get(tx_id)
    }

    /// Process pending transactions
    pub fn process_pending(&mut self) -> Result<Vec<String>> {
        let mut completed = Vec::new();
        let mut failed = Vec::new();
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Process each pending transaction
        for (tx_id, tx) in self.pending_transactions.iter_mut() {
            match tx.status {
                CrossShardStatus::Pending => {
                    // Check if all target shards are healthy
                    let all_healthy = tx.target_shards.iter().all(|&shard_id| {
                        self.shard_manager
                            .shards
                            .get(&shard_id)
                            .map(|meta| meta.health == ShardHealth::Healthy)
                            .unwrap_or(false)
                    });

                    if all_healthy {
                        tx.status = CrossShardStatus::InProgress;
                        info!(tx_id = %tx_id, "Started cross-shard transaction");
                    } else {
                        warn!(tx_id = %tx_id, "Cannot start transaction - some target shards are unhealthy");
                        tx.status = CrossShardStatus::Failed;
                        failed.push(tx_id.clone());
                    }
                }
                CrossShardStatus::InProgress => {
                    // Check transaction timeout (5 minutes)
                    let timeout = 300; // 5 minutes in seconds
                    if current_time > tx.timestamp + timeout {
                        warn!(tx_id = %tx_id, "Transaction timed out");
                        tx.status = CrossShardStatus::Failed;
                        failed.push(tx_id.clone());
                    } else {
                        // Simulate transaction processing
                        // In real implementation, this would coordinate with actual shards
                        let processing_complete = current_time > tx.timestamp + 10; // 10 seconds processing time

                        if processing_complete {
                            // Verify all shards acknowledged the transaction
                            let all_acknowledged = tx.target_shards.iter().all(|&shard_id| {
                                // In real implementation, check acknowledgments from each shard
                                // For now, assume all healthy shards acknowledge
                                self.shard_manager
                                    .shards
                                    .get(&shard_id)
                                    .map(|meta| meta.health == ShardHealth::Healthy)
                                    .unwrap_or(false)
                            });

                            if all_acknowledged {
                                tx.status = CrossShardStatus::Completed;
                                completed.push(tx_id.clone());
                                info!(tx_id = %tx_id, "Cross-shard transaction completed successfully");
                            } else {
                                // Some shards didn't acknowledge, mark as failed
                                tx.status = CrossShardStatus::Failed;
                                failed.push(tx_id.clone());
                                warn!(tx_id = %tx_id, "Transaction failed - some shards didn't acknowledge");
                            }
                        }
                    }
                }
                CrossShardStatus::Completed | CrossShardStatus::Failed => {
                    // Already processed, add to appropriate list
                    if tx.status == CrossShardStatus::Completed {
                        completed.push(tx_id.clone());
                    } else {
                        failed.push(tx_id.clone());
                    }
                }
            }
        }

        // Remove processed transactions from pending
        for tx_id in &completed {
            self.pending_transactions.remove(tx_id);
        }
        for tx_id in &failed {
            self.pending_transactions.remove(tx_id);
        }

        info!(
            completed = completed.len(),
            failed = failed.len(),
            remaining = self.pending_transactions.len(),
            "Processed cross-shard transactions"
        );

        Ok(completed)
    }
}
