use super::{Storage, RocksDb};
//! PoU scoring storage - persistence for scores and history
//!
//! Stores PoU scores per node per epoch and maintains history for smoothing calculations.

use super::{Storage, CF_POU_HISTORY, CF_POU_SCORES};
use crate::pou::fixed_point::{FixedPoint, SCALE};
use crate::pou::score::ScoreComponents;
use anyhow::Result;
use bincode;
use rocksdb::IteratorMode;
use serde::{Deserialize, Serialize};
use serde_big_array;
use std::cmp::Reverse;
use std::collections::HashMap;
use thiserror::Error;

/// PoU storage error types
#[derive(Debug, Error)]
pub enum PouError {
    #[error("Node not found: {0:?}")]
    NodeNotFound([u8; 32]),
    
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Calculation error: {0}")]
    CalculationError(String),
    
    #[error("Cache error: {0}")]
    CacheError(String),
    
    #[error("Invalid epoch: {0}")]
    InvalidEpoch(u64),
}

impl From<anyhow::Error> for PouError {
    fn from(err: anyhow::Error) -> Self {
        PouError::StorageError(err.to_string())
    }
}

/// Node identifier (32 bytes)
pub type NodeId = [u8; 32];

/// Epoch identifier
pub type Epoch = u64;

/// Complete light node information for P2P selection
/// Per ARCHITECTURE_POU.md Section 6.1
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LightNodeInfo {
    /// Node public key (32 bytes)
    #[serde(with = "serde_big_array::BigArray")]
    pub pubkey: NodeId,
    
    /// Current PoU score (basis points: 0-10000)
    pub pou_score: u32,
    
    /// Individual PoU components
    pub components: PoUComponents,
    
    /// Network address (multiaddr format)
    pub network_address: String,
    
    /// Geographic region (for latency clustering)
    pub geographic_region: GeographicRegion,
    
    /// Last seen timestamp (Unix epoch)
    pub last_seen: u64,
    
    /// Uptime percentage (last 24h)
    pub uptime_24h: f64,
    
    /// Current epoch number
    pub current_epoch: u64,
    
    /// Minimum stake required (0 for testnet, 150 for mainnet)
    pub stake_amount: u64,
    
    /// Whether node is eligible for proposer election
    pub is_eligible: bool,
    
    /// Number of successful proposals
    pub successful_proposals: u32,
    
    /// Number of failed proposals (for slashing calculation)
    pub failed_proposals: u32,
}

/// PoU score components (U, L, I, R, P)
/// Per ARCHITECTURE_POU.md Section 6.1
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoUComponents {
    /// Availability score (0-10000 basis points)
    pub availability: u32,
    
    /// Latency score (0-10000 basis points)
    pub latency: u32,
    
    /// Integrity score (0-10000 basis points)
    pub integrity: u32,
    
    /// Resource score (0-10000 basis points)
    pub resource: u32,
    
    /// Peer rating score (0-10000 basis points, 0 in initial phase)
    pub peer_rating: u32,
}

impl PoUComponents {
    /// Calculate total weighted PoU score
    /// Weights: U=30%, L=10%, I=25%, R=20%, P=15% (after activation)
    pub fn total_score(&self, current_epoch: u64) -> u32 {
        // Check if Peer Rating is activated (90 days after mainnet)
        const PEER_RATING_ACTIVATION_EPOCH: u64 = 1_296_000; // 90 days * 24h * 60min * 10 rounds
        
        let p_weight = if current_epoch >= PEER_RATING_ACTIVATION_EPOCH {
            1500 // 15%
        } else {
            0    // 0% before activation
        };
        
        // Adjust other weights when P is activated
        let (u_weight, l_weight, i_weight, r_weight) = if p_weight > 0 {
            (2550, 1000, 2000, 1500) // U=25.5%, L=10%, I=20%, R=15%
        } else {
            (3000, 1000, 2500, 2000) // U=30%, L=10%, I=25%, R=20%
        };
        
        let total_weight = u_weight + l_weight + i_weight + r_weight + p_weight;
        
        let weighted_sum = 
            self.availability as u64 * u_weight +
            self.latency as u64 * l_weight +
            self.integrity as u64 * i_weight +
            self.resource as u64 * r_weight +
            self.peer_rating as u64 * p_weight;
        
        (weighted_sum / total_weight) as u32
    }
}

/// Geographic regions for latency clustering
/// Per ARCHITECTURE_POU.md Section 6.1
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum GeographicRegion {
    Europe,
    NorthAmerica,
    SouthAmerica,
    Asia,
    Africa,
    Oceania,
    Unknown,
}

impl GeographicRegion {
    /// Get typical latency range to other regions (ms)
    pub fn latency_range_to(&self, other: &GeographicRegion) -> (u64, u64) {
        match (self, other) {
            // Same region: 5-50ms
            (a, b) if a == b => (5, 50),
            
            // Cross-region typical ranges
            (GeographicRegion::Europe, GeographicRegion::NorthAmerica) => (70, 150),
            (GeographicRegion::Europe, GeographicRegion::Asia) => (100, 200),
            (GeographicRegion::Europe, GeographicRegion::Africa) => (30, 100),
            (GeographicRegion::NorthAmerica, GeographicRegion::Asia) => (120, 250),
            (GeographicRegion::NorthAmerica, GeographicRegion::SouthAmerica) => (80, 180),
            (GeographicRegion::Asia, GeographicRegion::Oceania) => (60, 150),
            
            // Default for unknown combinations
            _ => (50, 300),
        }
    }
}

/// Stored PoU score entry
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PouScoreEntry {
    /// Node ID
    pub node_id: NodeId,
    /// Epoch number
    pub epoch: Epoch,
    /// Score components (U, L, I, R, P, total) in fixed-point
    pub components: ScoreComponentsStored,
    /// Timestamp of score calculation
    pub timestamp: u64,
}

/// Stored score components (using u32 for fixed-point values)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScoreComponentsStored {
    /// Availability component (fixed-point)
    pub u: u32,
    /// Latency component (fixed-point)
    pub l: u32,
    /// Integrity component (fixed-point)
    pub i: u32,
    /// Resource component (fixed-point)
    pub r: u32,
    /// Peer rating component (fixed-point, 0 in initial phase)
    pub p: u32,
    /// Total weighted score (fixed-point)
    pub total: u32,
}

impl From<ScoreComponents> for ScoreComponentsStored {
    fn from(components: ScoreComponents) -> Self {
        Self {
            u: clamp_fp(components.u),
            l: clamp_fp(components.l),
            i: clamp_fp(components.i),
            r: 0, // Resource component removed - set to 0
            p: clamp_fp(components.p),
            total: clamp_fp(components.total),
        }
    }
}

impl From<ScoreComponentsStored> for ScoreComponents {
    fn from(stored: ScoreComponentsStored) -> Self {
        use crate::pou::fixed_point::FixedPoint;
        ScoreComponents {
            u: FixedPoint::from_raw(stored.u),
            l: FixedPoint::from_raw(stored.l),
            i: FixedPoint::from_raw(stored.i),
            p: FixedPoint::from_raw(stored.p),
            total: FixedPoint::from_raw(stored.total),
        }
    }
}

/// Stored PoU history entry
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PouHistoryEntry {
    /// Node ID
    pub node_id: NodeId,
    /// Previous U history value (fixed-point)
    pub u_history: u32,
    /// Current miss streak
    pub miss_streak: u32,
    /// Longest miss streak
    pub longest_miss_streak: u32,
    /// Last applied penalties
    pub last_penalties: u32,
    /// Last epoch updated
    pub last_epoch: Epoch,
}

impl Storage<RocksDb> {
    /// Store PoU score for a node at a specific epoch
    pub fn put_pou_score(
        &self,
        node_id: &NodeId,
        epoch: Epoch,
        components: &ScoreComponents,
        timestamp: u64,
    ) -> Result<()> {
        let entry = PouScoreEntry {
            node_id: *node_id,
            epoch,
            components: ScoreComponentsStored::from(*components),
            timestamp,
        };

        let key = pou_score_key(node_id, epoch);

        let value = bincode::serialize(&entry)?;
        self.put_cf(CF_POU_SCORES, key, value)?;

        Ok(())
    }

    /// Get PoU score for a node at a specific epoch
    pub fn get_pou_score(
        &self,
        node_id: &NodeId,
        epoch: Epoch,
    ) -> Result<Option<PouScoreEntry>> {
        let key = pou_score_key(node_id, epoch);

        match self.get_cf(CF_POU_SCORES, key)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let entry: PouScoreEntry = crate::safe_deserialize(&bytes[..])?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Store PoU history for a node
    pub fn put_pou_history(
        &self,
        node_id: &NodeId,
        u_history: FixedPoint,
        miss_streak: u32,
        longest_miss_streak: u32,
        last_penalties: u32,
        last_epoch: Epoch,
    ) -> Result<()> {
        let entry = PouHistoryEntry {
            node_id: *node_id,
            u_history: u_history.raw(),
            miss_streak,
            longest_miss_streak,
            last_penalties,
            last_epoch,
        };

        // Key: node_id (32 bytes)
        let key = node_id.as_slice();
        let value = bincode::serialize(&entry)?;
        self.put_cf(CF_POU_HISTORY, key, value)?;

        Ok(())
    }

    /// Get PoU history for a node
    pub fn get_pou_history(&self, node_id: &NodeId) -> Result<Option<PouHistoryEntry>> {
        let key = node_id.as_slice();
        match self.get_cf(CF_POU_HISTORY, key)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let entry: PouHistoryEntry = crate::safe_deserialize(&bytes[..])?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Purge old PoU scores (keep last N epochs per node)
    /// 
    /// # Arguments
    /// * `keep_epochs` - Number of recent epochs to keep (default: 100)
    pub fn purge_old_pou_scores(&self, current_epoch: Epoch, keep_epochs: u64) -> Result<()> {
        let cf = self.cf(CF_POU_SCORES)?;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        // Collect epochs per node to enforce per-node retention.
        let mut per_node: HashMap<NodeId, Vec<(Epoch, Vec<u8>)>> = HashMap::new();
        for item in iter {
            let (key, _): (Box<[u8]>, Box<[u8]>) = item?;
            if key.len() != 40 {
                continue;
            }

            let mut node_id = [0u8; 32];
            node_id.copy_from_slice(&key[..32]);
            let epoch = Epoch::from_le_bytes(
                key[32..40]
                    .try_into()
                    .expect("pou epoch encoding must be 8 bytes"),
            );

            per_node
                .entry(node_id)
                .or_default()
                .push((epoch, key.to_vec()));
        }

        for (_node, mut entries) in per_node {
            // Keep newest `keep_epochs` epochs, delete the rest.
            entries.sort_by_key(|(epoch, _)| Reverse(*epoch));
            if entries.len() as u64 <= keep_epochs {
                continue;
            }

            for (_epoch, key) in entries.into_iter().skip(keep_epochs as usize) {
                self.delete_cf(CF_POU_SCORES, key)?;
            }
        }

        // Extra guard: delete anything older than (current_epoch - keep_epochs) to cap growth
        let cutoff_epoch = current_epoch.saturating_sub(keep_epochs);
        let iter_cutoff = self.db.iterator_cf(&cf, IteratorMode::Start);
        for item in iter_cutoff {
            let (key, _): (Box<[u8]>, Box<[u8]>) = item?;
            if key.len() != 40 {
                continue;
            }
            let epoch = Epoch::from_le_bytes(key[32..40].try_into().unwrap());
            if epoch < cutoff_epoch {
                self.delete_cf(CF_POU_SCORES, key)?;
            }
        }

        Ok(())
    }
}

fn clamp_fp(fp: FixedPoint) -> u32 {
    fp.raw().min(SCALE)
}

fn pou_score_key(node_id: &NodeId, epoch: Epoch) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[..32].copy_from_slice(node_id);
    key[32..].copy_from_slice(&epoch.to_le_bytes());
    key
}

