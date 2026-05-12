use super::{Storage, RocksDb};
//! Storage layer for active nodes tracking
//!
//! This module provides storage operations for tracking active nodes
//! per epoch for reward distribution purposes.

use super::Storage;
use super::CF_ACTIVE_NODES;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Prefix for epoch-based node keys
const EPOCH_PREFIX: &[u8] = b"epoch:";
/// Prefix for node activity keys
const ACTIVITY_PREFIX: &[u8] = b"activity:";

/// Node activity record for an epoch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeActivityRecord {
    /// Node address (32 bytes)
    pub node_address: [u8; 32],
    /// Epoch ID
    pub epoch_id: u64,
    /// Node type (0 = LightNode, 1 = Masternode)
    pub node_type: u8,
    /// Number of blocks proposed
    pub blocks_proposed: u32,
    pub blocks_validated: u32,
    /// Successful consensus participations
    pub consensus_participations: u32,
    /// Uptime percentage (0-10000 basis points)
    pub uptime_bp: u16,
    /// Last activity timestamp
    pub last_activity: u64,
    /// Registration timestamp
    pub registered_at: u64,
    /// Is currently active
    pub is_active: bool,
}

impl Default for NodeActivityRecord {
    fn default() -> Self {
        Self {
            node_address: [0u8; 32],
            epoch_id: 0,
            node_type: 0,
            blocks_proposed: 0,
            blocks_validated: 0,
            consensus_participations: 0,
            uptime_bp: 0,
            last_activity: 0,
            registered_at: 0,
            is_active: false,
        }
    }
}

impl Storage<RocksDb> {
    /// Build key for epoch-node combination
    fn build_active_node_key(epoch_id: u64, node_address: &[u8; 32]) -> Vec<u8> {
        let mut key = EPOCH_PREFIX.to_vec();
        key.extend_from_slice(&epoch_id.to_be_bytes());
        key.push(b':');
        key.extend_from_slice(node_address);
        key
    }

    /// Build key for node activity (cross-epoch)
    fn build_node_activity_key(node_address: &[u8; 32]) -> Vec<u8> {
        let mut key = ACTIVITY_PREFIX.to_vec();
        key.extend_from_slice(node_address);
        key
    }

    /// Register a node as active for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Current epoch ID
    /// * `node_address` - 32-byte node address
    /// * `node_type` - 0 for LightNode, 1 for Masternode
    /// * `timestamp` - Current timestamp
    pub fn register_active_node(
        &self,
        epoch_id: u64,
        node_address: &[u8; 32],
        node_type: u8,
        timestamp: u64,
    ) -> Result<()> {
        let key = Self::build_active_node_key(epoch_id, node_address);
        
        // Check if already registered
        if let Some(_) = self.get_cf(CF_ACTIVE_NODES, &key)? {
            return Ok(()); // Already registered
        }

        let record = NodeActivityRecord {
            node_address: *node_address,
            epoch_id,
            node_type,
            blocks_proposed: 0,
            blocks_validated: 0,
            consensus_participations: 0,
            uptime_bp: 10000, // Start with 100% uptime
            last_activity: timestamp,
            registered_at: timestamp,
            is_active: true,
        };

        let value = bincode::serialize(&record)
            .context("Failed to serialize node activity record")?;
        
        self.put_cf(CF_ACTIVE_NODES, &key, value)
    }

    /// Update node activity for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Current epoch ID
    /// * `node_address` - 32-byte node address
    /// * `blocks_proposed` - Increment for blocks proposed
    /// * `timestamp` - Current timestamp
    pub fn update_node_activity(
        &self,
        epoch_id: u64,
        node_address: &[u8; 32],
        blocks_proposed: u32,
        blocks_validated: u32,
        timestamp: u64,
    ) -> Result<()> {
        let key = Self::build_active_node_key(epoch_id, node_address);
        
        let mut record = match self.get_cf(CF_ACTIVE_NODES, &key)? {
            Some(ref bytes) => crate::safe_deserialize(&bytes[..])
                .context("Failed to deserialize node activity record")?,
            None => {
                // Auto-register if not found
                NodeActivityRecord {
                    node_address: *node_address,
                    epoch_id,
                    node_type: 0, // Default to LightNode
                    registered_at: timestamp,
                    is_active: true,
                    ..Default::default()
                }
            }
        };

        record.blocks_proposed = record.blocks_proposed.saturating_add(blocks_proposed);
        record.blocks_validated = record.blocks_validated.saturating_add(blocks_validated);
        record.consensus_participations = record.consensus_participations.saturating_add(1);
        record.last_activity = timestamp;

        let value = bincode::serialize(&record)
            .context("Failed to serialize updated node activity record")?;
        
        self.put_cf(CF_ACTIVE_NODES, &key, value)
    }

    /// Get all active nodes for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Epoch ID to query
    ///
    /// # Returns
    /// Vector of node addresses that were active in the epoch
    pub fn get_active_nodes_for_epoch(&self, epoch_id: u64) -> Result<Vec<[u8; 32]>> {
        let cf = self.cf(CF_ACTIVE_NODES)?;
        let prefix = {
            let mut p = EPOCH_PREFIX.to_vec();
            p.extend_from_slice(&epoch_id.to_be_bytes());
            p.push(b':');
            p
        };

        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let mut nodes = Vec::new();

        for item in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = item?;
            
            // Check if key starts with our prefix
            if !key.starts_with(&prefix) {
                break;
            }

            // Deserialize to check if active
            let record: NodeActivityRecord = crate::safe_deserialize(&value[..])?;
            if record.is_active {
                nodes.push(record.node_address);
            }
        }

        Ok(nodes)
    }

    /// Get node activity record for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Epoch ID
    /// * `node_address` - 32-byte node address
    ///
    /// # Returns
    /// Node activity record if found
    pub fn get_node_activity(
        &self,
        epoch_id: u64,
        node_address: &[u8; 32],
    ) -> Result<Option<NodeActivityRecord>> {
        let key = Self::build_active_node_key(epoch_id, node_address);
        
        match self.get_cf(CF_ACTIVE_NODES, &key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)
                .context("Failed to deserialize node activity record")?),
            None => Ok(None),
        }
    }

    /// Mark a node as inactive for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Epoch ID
    /// * `node_address` - 32-byte node address
    pub fn deactivate_node(
        &self,
        epoch_id: u64,
        node_address: &[u8; 32],
    ) -> Result<()> {
        let key = Self::build_active_node_key(epoch_id, node_address);
        
        if let Some(ref bytes) = self.get_cf(CF_ACTIVE_NODES, &key)? {`n                let bytes: &[u8] = bytes;
            let mut record: NodeActivityRecord = crate::safe_deserialize(&bytes)?;
            record.is_active = false;
            
            let value = bincode::serialize(&record)?;
            self.put_cf(CF_ACTIVE_NODES, &key, value)?;
        }
        
        Ok(())
    }

    /// Get all active node records for an epoch (with full details)
    ///
    /// # Arguments
    /// * `epoch_id` - Epoch ID to query
    ///
    /// # Returns
    /// Vector of node activity records
    pub fn get_active_node_records(&self, epoch_id: u64) -> Result<Vec<NodeActivityRecord>> {
        let cf = self.cf(CF_ACTIVE_NODES)?;
        let prefix = {
            let mut p = EPOCH_PREFIX.to_vec();
            p.extend_from_slice(&epoch_id.to_be_bytes());
            p.push(b':');
            p
        };

        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let mut records = Vec::new();

        for item in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = item?;
            
            if !key.starts_with(&prefix) {
                break;
            }

            let record: NodeActivityRecord = crate::safe_deserialize(&value[..])?;
            if record.is_active {
                records.push(record);
            }
        }

        Ok(records)
    }

    /// Cleanup old epoch data (keep last N epochs)
    ///
    /// # Arguments
    /// * `current_epoch` - Current epoch ID
    /// * `keep_epochs` - Number of epochs to keep
    pub fn cleanup_old_active_nodes(&self, current_epoch: u64, keep_epochs: u64) -> Result<u64> {
        if current_epoch < keep_epochs {
            return Ok(0);
        }

        let cutoff_epoch = current_epoch - keep_epochs;
        let cf = self.cf(CF_ACTIVE_NODES)?;
        let mut deleted = 0u64;

        // Iterate through epochs 0 to cutoff
        for epoch in 0..=cutoff_epoch {
            let prefix = {
                let mut p = EPOCH_PREFIX.to_vec();
                p.extend_from_slice(&epoch.to_be_bytes());
                p.push(b':');
                p
            };

            let iter = self.db.prefix_iterator_cf(&cf, &prefix);
            
            for item in iter {
                let (key, _): (Box<[u8]>, Box<[u8]>) = item?;
                
                if !key.starts_with(&prefix) {
                    break;
                }

                self.delete_cf(CF_ACTIVE_NODES, &key)?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }

    /// Count active nodes for an epoch
    ///
    /// # Arguments
    /// * `epoch_id` - Epoch ID to count
    ///
    /// # Returns
    /// Number of active nodes
    pub fn count_active_nodes(&self, epoch_id: u64) -> Result<u64> {
        let nodes = self.get_active_nodes_for_epoch(epoch_id)?;
        Ok(nodes.len() as u64)
    }

    /// Get all active nodes across all epochs (for global queries)
    ///
    /// Note: This is a potentially expensive operation as it scans all epochs.
    /// Use `get_active_nodes_for_epoch` when possible.
    ///
    /// # Returns
    /// Vector of unique node addresses that are active in any epoch
    pub fn get_all_active_nodes(&self) -> Result<Vec<[u8; 32]>> {
        let cf = self.cf(CF_ACTIVE_NODES)?;
        let iter = self.db.prefix_iterator_cf(&cf, EPOCH_PREFIX);
        
        let mut unique_nodes = std::collections::HashSet::new();
        
        for item in iter {
            let (_key, value): (Box<[u8]>, Box<[u8]>) = item?;
            let record: NodeActivityRecord = crate::safe_deserialize(&value[..])?;
            if record.is_active {
                unique_nodes.insert(record.node_address);
            }
        }
        
        Ok(unique_nodes.into_iter().collect())
    }
}

