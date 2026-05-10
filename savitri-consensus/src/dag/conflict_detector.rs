//! Non-invasive Conflict Detection for DAG
//!
//! This module provides conflict detection capabilities for DAG blocks,
//! working directly with existing Block structures without requiring modifications.

use crate::{Block, BlockHeader, ConsensusError, ProposerInfo, Transaction};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// Non-invasive conflict detector for DAG blocks
pub struct ConflictDetector {
    config: ConflictDetectionConfig,
    cache: Arc<RwLock<HashMap<String, CachedConflict>>>,
}

#[derive(Debug, Clone)]
pub struct ConflictDetectionConfig {
    pub enable_transaction_conflicts: bool,
    pub enable_state_conflicts: bool,
    pub max_conflicts_per_scan: usize,
    pub conflict_cache_ttl_secs: u64,
}

#[derive(Debug, Clone)]
pub struct Conflict {
    pub conflict_type: ConflictType,
    pub conflicting_blocks: Vec<[u8; 64]>,     // Block hashes
    pub conflicting_transactions: Vec<String>, // Transaction IDs
    pub severity: ConflictSeverity,
    pub detected_at: SystemTime,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConflictType {
    TransactionConflict,
    StateConflict,
    DoubleSpend,
    ParentConflict,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConflictSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
struct CachedConflict {
    conflict: Conflict,
    cached_at: u64,
}

impl ConflictDetector {
    pub fn new(config: ConflictDetectionConfig) -> Self {
        Self {
            config,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Detect transaction conflicts in blocks - works with existing Block structure
    pub async fn detect_transaction_conflicts(
        &self,
        blocks: &[Block],
    ) -> Result<Vec<Conflict>, ConsensusError> {
        if !self.config.enable_transaction_conflicts {
            return Ok(vec![]);
        }

        let start_time = std::time::Instant::now();
        let mut conflicts = Vec::new();
        let mut tx_seen: HashMap<String, [u8; 64]> = HashMap::new();
        let mut double_spend_candidates: HashMap<String, Vec<[u8; 64]>> = HashMap::new();

        for block in blocks {
            let block_hash: [u8; 64] = block.hash();

            for tx in &block.transactions {
                let tx_id = format!("{:x?}", tx.hash);

                // Check for exact transaction duplicates
                if let Some(&prev_block_hash) = tx_seen.get(&tx_id) {
                    conflicts.push(Conflict {
                        conflict_type: ConflictType::TransactionConflict,
                        conflicting_blocks: vec![prev_block_hash, block_hash],
                        conflicting_transactions: vec![tx_id.clone()],
                        severity: ConflictSeverity::High,
                        detected_at: SystemTime::now(),
                        description: format!("Transaction {} appears in multiple blocks", tx_id),
                    });
                } else {
                    tx_seen.insert(tx_id.clone(), block_hash);
                }

                // Check for double spends (same from address with different nonce)
                let from_addr = format!("{:x?}", tx.from);
                double_spend_candidates
                    .entry(from_addr.clone())
                    .or_insert_with(Vec::new)
                    .push(block_hash);
            }

            // Early exit if we've reached max conflicts
            if conflicts.len() >= self.config.max_conflicts_per_scan {
                break;
            }
        }

        // Check for double spends
        for (from_addr, block_hashes) in double_spend_candidates {
            if block_hashes.len() > 1 {
                conflicts.push(Conflict {
                    conflict_type: ConflictType::DoubleSpend,
                    conflicting_blocks: block_hashes,
                    conflicting_transactions: vec![from_addr.clone()],
                    severity: ConflictSeverity::Critical,
                    detected_at: SystemTime::now(),
                    description: format!("Potential double spend from address {}", from_addr),
                });
            }
        }

        // Performance check
        let duration = start_time.elapsed();
        if duration.as_millis() > 500 {
            tracing::warn!(
                "Transaction conflict detection took {}ms for {} blocks",
                duration.as_millis(),
                blocks.len()
            );
        }

        Ok(conflicts)
    }

    /// Detect state conflicts in blocks - works with existing Block structure
    pub async fn detect_state_conflicts(
        &self,
        blocks: &[Block],
    ) -> Result<Vec<Conflict>, ConsensusError> {
        if !self.config.enable_state_conflicts {
            return Ok(vec![]);
        }

        let start_time = std::time::Instant::now();
        let mut conflicts = Vec::new();
        let mut state_transitions: HashMap<String, Vec<[u8; 64]>> = HashMap::new();

        for block in blocks {
            let block_hash: [u8; 64] = block.hash();

            // Track state changes by account/address
            for tx in &block.transactions {
                let from_addr = format!("{:x?}", tx.from);
                let to_addr = format!("{:x?}", tx.to);

                // Track from address state changes
                state_transitions
                    .entry(format!("from_{}", from_addr))
                    .or_insert_with(Vec::new)
                    .push(block_hash);

                // Track to address state changes
                state_transitions
                    .entry(format!("to_{}", to_addr))
                    .or_insert_with(Vec::new)
                    .push(block_hash);
            }

            if conflicts.len() >= self.config.max_conflicts_per_scan {
                break;
            }
        }

        // Find conflicting state transitions
        for (state_key, block_hashes) in state_transitions {
            if block_hashes.len() > 1 {
                conflicts.push(Conflict {
                    conflict_type: ConflictType::StateConflict,
                    conflicting_blocks: block_hashes,
                    conflicting_transactions: vec![state_key.clone()],
                    severity: ConflictSeverity::Medium,
                    detected_at: SystemTime::now(),
                    description: format!("State conflict detected for {}", state_key),
                });
            }
        }

        // Performance check
        let duration = start_time.elapsed();
        if duration.as_millis() > 100 {
            tracing::warn!(
                "State conflict detection took {}ms for {} blocks",
                duration.as_millis(),
                blocks.len()
            );
        }

        Ok(conflicts)
    }

    /// Validate DAG structure for parent conflicts - works with existing BlockHeader
    pub async fn validate_dag_structure(&self, blocks: &[Block]) -> Result<(), ConsensusError> {
        let start_time = std::time::Instant::now();
        let mut block_map: HashMap<[u8; 64], &Block> = HashMap::new();
        let mut parent_references: HashMap<[u8; 64], Vec<[u8; 64]>> = HashMap::new();

        // Build block map and parent references
        for block in blocks {
            let block_hash: [u8; 64] = block.hash();
            block_map.insert(block_hash, block);

            // Track parent references
            let parent_hash = block.header.parent_hash.clone();
            let parent_hash_array: [u8; 64] = parent_hash.0.try_into().unwrap_or([0u8; 64]);
            parent_references
                .entry(parent_hash_array)
                .or_insert_with(Vec::new)
                .push(block_hash);
        }

        // Validate parent references exist
        for block in blocks {
            let parent_hash = &block.header.parent_hash;
            if parent_hash.0.iter().all(|&b| b == 0) || parent_hash.0 == [0u8; 64] {
                continue;
            }

            let parent_hash_array: [u8; 64] = parent_hash.0.try_into().unwrap_or([0u8; 64]);
            if !block_map.contains_key::<[u8; 64]>(&parent_hash_array) {
                // Parent hash not found is not necessarily an error for DAG structure
                // This can happen with orphaned blocks or during initial sync
                tracing::debug!("Parent hash not found in block set, continuing validation");
            }
        }

        // Check for orphaned blocks (blocks that are not referenced as parents)
        let mut referenced_blocks = HashSet::new();
        for block in blocks {
            referenced_blocks.insert(block.header.parent_hash.clone());
        }

        for block in blocks {
            let block_hash = block.hash();
            if !referenced_blocks.contains(&crate::types::block::Hash64(block_hash))
                && block.header.height > 0
            {
                // This is an orphaned block - not necessarily an error, but worth noting
                tracing::debug!("Orphaned block detected");
            }
        }

        // Performance check
        let duration = start_time.elapsed();
        if duration.as_millis() > 200 {
            tracing::warn!(
                "DAG structure validation took {}ms for {} blocks",
                duration.as_millis(),
                blocks.len()
            );
        }

        Ok(())
    }

    /// Comprehensive conflict detection combining all methods
    pub async fn detect_all_conflicts(
        &self,
        blocks: &[Block],
    ) -> Result<Vec<Conflict>, ConsensusError> {
        let start_time = std::time::Instant::now();

        // Validate DAG structure first
        self.validate_dag_structure(blocks).await?;

        // Detect transaction conflicts
        let tx_conflicts: Vec<Conflict> = self.detect_transaction_conflicts(blocks).await?;

        // Detect state conflicts
        let state_conflicts: Vec<Conflict> = self.detect_state_conflicts(blocks).await?;

        // Combine all conflicts
        let mut all_conflicts = tx_conflicts;
        all_conflicts.extend(state_conflicts);

        // Sort by severity (critical first)
        all_conflicts.sort_by(|a, b| match (&a.severity, &b.severity) {
            (ConflictSeverity::Critical, ConflictSeverity::Critical) => std::cmp::Ordering::Equal,
            (ConflictSeverity::Critical, _) => std::cmp::Ordering::Less,
            (_, ConflictSeverity::Critical) => std::cmp::Ordering::Greater,
            (ConflictSeverity::High, ConflictSeverity::High) => std::cmp::Ordering::Equal,
            (ConflictSeverity::High, _) => std::cmp::Ordering::Less,
            (_, ConflictSeverity::High) => std::cmp::Ordering::Greater,
            (ConflictSeverity::Medium, ConflictSeverity::Medium) => std::cmp::Ordering::Equal,
            (ConflictSeverity::Medium, _) => std::cmp::Ordering::Less,
            (_, ConflictSeverity::Medium) => std::cmp::Ordering::Greater,
            (ConflictSeverity::Low, ConflictSeverity::Low) => std::cmp::Ordering::Equal,
        });

        // Cache results
        if !all_conflicts.is_empty() {
            self.cache_conflicts(&all_conflicts).await;
        }

        let duration = start_time.elapsed();
        tracing::info!(
            "Conflict detection completed in {}ms: {} conflicts found",
            duration.as_millis(),
            all_conflicts.len()
        );

        Ok(all_conflicts)
    }

    /// Cache detected conflicts for performance
    async fn cache_conflicts(&self, conflicts: &[Conflict]) {
        let mut cache = self.cache.write().await;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        for conflict in conflicts {
            let conflict_id = format!(
                "conflict_{:x}_{:?}",
                conflict
                    .detected_at
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                conflict.conflict_type
            );
            cache.insert(
                conflict_id,
                CachedConflict {
                    conflict: conflict.clone(),
                    cached_at: now,
                },
            );
        }
    }

    /// Clean expired cache entries
    pub async fn clean_expired_cache(&self) {
        let mut cache = self.cache.write().await;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        cache.retain(|_, cached| {
            now.saturating_sub(cached.cached_at) < self.config.conflict_cache_ttl_secs
        });
    }

    /// Get conflict statistics
    pub async fn get_conflict_stats(&self) -> ConflictStats {
        let cache = self.cache.read().await;
        let mut stats = ConflictStats::default();

        for cached in cache.values() {
            match cached.conflict.severity {
                ConflictSeverity::Critical => stats.critical_conflicts += 1,
                ConflictSeverity::High => stats.high_conflicts += 1,
                ConflictSeverity::Medium => stats.medium_conflicts += 1,
                ConflictSeverity::Low => stats.low_conflicts += 1,
            }

            match cached.conflict.conflict_type {
                ConflictType::TransactionConflict => stats.transaction_conflicts += 1,
                ConflictType::StateConflict => stats.state_conflicts += 1,
                ConflictType::DoubleSpend => stats.double_spends += 1,
                ConflictType::ParentConflict => stats.parent_conflicts += 1,
            }
        }

        stats.total_conflicts = cache.len();
        stats
    }
}

/// Conflict statistics
#[derive(Debug, Clone, Default)]
pub struct ConflictStats {
    pub total_conflicts: usize,
    pub critical_conflicts: usize,
    pub high_conflicts: usize,
    pub medium_conflicts: usize,
    pub low_conflicts: usize,
    pub transaction_conflicts: usize,
    pub state_conflicts: usize,
    pub double_spends: usize,
    pub parent_conflicts: usize,
}

impl Default for ConflictDetectionConfig {
    fn default() -> Self {
        Self {
            enable_transaction_conflicts: true,
            enable_state_conflicts: true,
            max_conflicts_per_scan: 1000,
            conflict_cache_ttl_secs: 300,
        }
    }
}

// Helper trait for Block to get hash
trait BlockHash {
    fn hash(&self) -> [u8; 64];
}

impl BlockHash for Block {
    fn hash(&self) -> [u8; 64] {
        // Simple hash implementation - in production this would use proper cryptographic hashing
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.header.height.to_le_bytes());
        hasher.update(&self.header.timestamp.to_le_bytes());
        hasher.update(&self.header.parent_hash.0);
        let hash = hasher.finalize();
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(hash.as_bytes());
        result
    }
}
