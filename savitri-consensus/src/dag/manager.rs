//! DAG Manager
//!
//! This module provides the main DAG management functionality, working with
//! existing BlockHeader structures without requiring modifications.

use crate::dag::conflict_detector::{ConflictDetectionConfig, ConflictDetector};
use crate::dag::types::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

pub struct DAGManager {
    config: DAGConfig,
    branch_tracker: Arc<RwLock<HashMap<String, BranchInfo>>>,
    conflict_detector: Arc<ConflictDetector>,
    statistics: Arc<RwLock<DAGStats>>,
}

impl DAGManager {
    /// Create a new DAG manager with the given configuration
    pub fn new(config: DAGConfig) -> Self {
        Self {
            config: config.clone(),
            branch_tracker: Arc::new(RwLock::new(HashMap::new())),
            conflict_detector: Arc::new(ConflictDetector::new(ConflictDetectionConfig {
                enable_transaction_conflicts: config.conflict_config.enable_transaction_conflicts,
                enable_state_conflicts: config.conflict_config.enable_state_conflicts,
                max_conflicts_per_scan: config.conflict_config.max_tracked_conflicts,
                conflict_cache_ttl_secs: config.conflict_config.resolution_timeout_ms / 1000,
            })),
            statistics: Arc::new(RwLock::new(DAGStats::default())),
        }
    }

    /// Validate a multi-parent block header
    pub async fn validate_multi_parent_block(&self, header: &crate::BlockHeader) -> DAGResult<()> {
        // Check if this is a multi-parent block
        if !header.is_multi_parent() {
            return Ok(()); // Single parent blocks are always valid
        }

        // Validate parent hashes format
        let parent_hashes = header.get_all_parents();
        let parent_hashes_vec: Vec<Vec<u8>> = parent_hashes.iter().map(|h| h.to_vec()).collect();
        utils::validate_parent_hashes(&parent_hashes_vec)?;

        // Check for zero hashes
        for parent_hash in &parent_hashes {
            if *parent_hash == [0u8; 64] {
                return Err(DAGError::ValidationError(
                    "Invalid parent hash: zero hash".to_string(),
                ));
            }
        }

        // Check if parent hashes would create conflicts
        let has_conflicts = self.check_parent_conflicts(&parent_hashes_vec).await?;
        if has_conflicts {
            return Err(DAGError::ConflictDetected(
                "Parent hashes would create conflicts".to_string(),
            ));
        }

        Ok(())
    }

    /// Detect conflicts in a set of blocks
    ///
    /// SECURITY (F-08): Delegates to ConflictDetector for transaction duplicate,
    pub async fn detect_conflicts(
        &self,
        blocks: &[crate::Block],
    ) -> DAGResult<Vec<crate::dag::conflict_detector::Conflict>> {
        let conflicts = self
            .conflict_detector
            .detect_all_conflicts(blocks)
            .await
            .map_err(|e| DAGError::ConflictDetected(format!("Conflict detection error: {}", e)))?;

        // Update statistics and log conflicts
        if !conflicts.is_empty() {
            let mut stats = self.statistics.write().await;
            stats.total_conflicts += conflicts.len();

            for conflict in &conflicts {
                if conflict.severity == crate::dag::conflict_detector::ConflictSeverity::Critical {
                    tracing::error!(
                        "CRITICAL DAG conflict detected: {:?} — {}",
                        conflict.conflict_type,
                        conflict.description
                    );
                } else {
                    tracing::warn!(
                        "DAG conflict detected: {:?} severity={:?} — {}",
                        conflict.conflict_type,
                        conflict.severity,
                        conflict.description
                    );
                }
            }
        }

        Ok(conflicts)
    }

    /// Create a new branch with the given parent hashes
    pub async fn create_branch(&self, parent_hashes: Vec<Vec<u8>>) -> DAGResult<String> {
        // Validate parent hashes
        utils::validate_parent_hashes(&parent_hashes)?;

        // Check branch limit
        let tracker = self.branch_tracker.read().await;
        if tracker.len() >= self.config.max_branches {
            return Err(DAGError::MaxBranchesExceeded(self.config.max_branches));
        }
        drop(tracker);

        // Generate branch ID
        let branch_id = utils::generate_branch_id();

        // Create branch info
        let branch_info = BranchInfo {
            id: branch_id.clone(),
            parent_hashes: parent_hashes.clone(),
            created_at: SystemTime::now(),
            last_activity: SystemTime::now(),
            status: BranchStatus::Active,
            block_count: 0,
            depth: parent_hashes.len() as u64,
        };

        // Store branch
        let mut tracker = self.branch_tracker.write().await;
        tracker.insert(branch_id.clone(), branch_info);

        Ok(branch_id)
    }

    /// Get all active branches
    pub async fn get_active_branches(&self) -> Vec<BranchInfo> {
        let tracker = self.branch_tracker.read().await;
        tracker
            .values()
            .filter(|b| b.status == BranchStatus::Active)
            .cloned()
            .collect()
    }

    /// Get branch information by ID
    pub async fn get_branch(&self, branch_id: &str) -> DAGResult<BranchInfo> {
        let tracker = self.branch_tracker.read().await;
        tracker
            .get(branch_id)
            .cloned()
            .ok_or_else(|| DAGError::BranchNotFound(branch_id.to_string()))
    }

    /// Update branch status
    pub async fn update_branch_status(
        &self,
        branch_id: &str,
        status: BranchStatus,
    ) -> DAGResult<()> {
        let mut tracker = self.branch_tracker.write().await;

        if let Some(branch) = tracker.get_mut(branch_id) {
            branch.status = status;
            branch.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(DAGError::BranchNotFound(branch_id.to_string()))
        }
    }

    /// Add a block to a branch
    pub async fn add_block_to_branch(&self, branch_id: &str, block_hash: Vec<u8>) -> DAGResult<()> {
        let mut tracker = self.branch_tracker.write().await;

        if let Some(branch) = tracker.get_mut(branch_id) {
            branch.block_count += 1;
            branch.last_activity = SystemTime::now();
            Ok(())
        } else {
            Err(DAGError::BranchNotFound(branch_id.to_string()))
        }
    }

    /// Merge a branch into another
    pub async fn merge_branch(
        &self,
        source_branch_id: &str,
        target_branch_id: &str,
    ) -> DAGResult<()> {
        let mut tracker = self.branch_tracker.write().await;

        let source_branch = tracker
            .get(source_branch_id)
            .cloned()
            .ok_or_else(|| DAGError::BranchNotFound(source_branch_id.to_string()))?;

        if let Some(target_branch) = tracker.get_mut(target_branch_id) {
            // Update target branch
            target_branch.block_count += source_branch.block_count;
            target_branch.depth = target_branch.depth.max(source_branch.depth);
            target_branch.last_activity = SystemTime::now();

            // Mark source branch as merged
            if let Some(source_branch_mut) = tracker.get_mut(source_branch_id) {
                source_branch_mut.status = BranchStatus::Merged;
                source_branch_mut.last_activity = SystemTime::now();
            }

            Ok(())
        } else {
            Err(DAGError::BranchNotFound(target_branch_id.to_string()))
        }
    }

    /// Abandon a branch due to conflicts
    pub async fn abandon_branch(&self, branch_id: &str) -> DAGResult<()> {
        self.update_branch_status(branch_id, BranchStatus::Abandoned)
            .await
    }

    /// Get DAG statistics
    pub async fn get_statistics(&self) -> DAGStats {
        let tracker = self.branch_tracker.read().await;
        let conflicts = self.conflict_detector.get_conflict_stats().await;

        let total_branches = tracker.len();
        let active_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Active)
            .count();
        let merged_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Merged)
            .count();
        let abandoned_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Abandoned)
            .count();

        let avg_branch_depth = if !tracker.is_empty() {
            let total_depth: u64 = tracker.values().map(|b| b.depth).sum();
            total_depth as f64 / tracker.len() as f64
        } else {
            0.0
        };

        let max_branch_depth = tracker.values().map(|b| b.depth).max().unwrap_or(0);

        DAGStats {
            total_branches,
            active_branches,
            merged_branches,
            abandoned_branches,
            total_conflicts: conflicts.total_conflicts,
            resolved_conflicts: 0, // Not tracked in new ConflictDetector
            avg_branch_depth,
            max_branch_depth,
            last_updated: std::time::SystemTime::now(),
        }
    }

    /// Clean up inactive branches
    pub async fn cleanup_inactive_branches(&self) -> DAGResult<usize> {
        let mut tracker = self.branch_tracker.write().await;
        let mut cleaned = 0;

        let timeout_duration = Duration::from_secs(self.config.branch_timeout_secs);
        let now = SystemTime::now();

        tracker.retain(|_, branch| {
            let is_active = branch.status == BranchStatus::Active;
            let is_timed_out = now
                .duration_since(branch.last_activity)
                .unwrap_or(Duration::ZERO)
                > timeout_duration;

            if is_active && is_timed_out {
                branch.status = BranchStatus::Paused;
                cleaned += 1;
                false // Remove from active tracking (will be marked as paused)
            } else {
                true // Keep branch
            }
        });

        Ok(cleaned)
    }

    /// Find the best branch according to the specified criteria
    pub async fn find_best_branch(&self) -> DAGResult<Option<BranchInfo>> {
        let tracker = self.branch_tracker.read().await;
        let active_branches: Vec<BranchInfo> = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Active)
            .cloned()
            .collect();

        if active_branches.is_empty() {
            return Ok(None);
        }

        // Find branch with highest block count (longest chain)
        let best_branch = active_branches
            .iter()
            .max_by_key(|b| (b.block_count, b.depth))
            .cloned();

        Ok(best_branch)
    }

    /// Check if parent hashes would create conflicts
    async fn check_parent_conflicts(&self, parent_hashes: &[Vec<u8>]) -> DAGResult<bool> {
        let active_branches = self.get_active_branches().await;

        // Check if any parent hash is used by multiple active branches
        let mut parent_usage: HashMap<Vec<u8>, usize> = HashMap::new();

        for branch in &active_branches {
            for parent_hash in &branch.parent_hashes {
                *parent_usage.entry(parent_hash.clone()).or_insert(0) += 1;
            }
        }

        for parent_hash in parent_hashes {
            if parent_usage.get(parent_hash).unwrap_or(&0) > &1 {
                return Ok(true); // Conflict detected
            }
        }

        Ok(false) // No conflicts
    }

    /// Update internal statistics
    async fn update_statistics(&self) {
        let tracker = self.branch_tracker.read().await;
        let conflicts = self.conflict_detector.get_conflict_stats().await;

        let mut stats = self.statistics.write().await;
        stats.total_branches = tracker.len();
        stats.active_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Active)
            .count();
        stats.merged_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Merged)
            .count();
        stats.abandoned_branches = tracker
            .values()
            .filter(|b| b.status == BranchStatus::Abandoned)
            .count();
        stats.total_conflicts = conflicts.total_conflicts;
        stats.resolved_conflicts = 0; // Not tracked in new ConflictDetector

        if !tracker.is_empty() {
            let total_depth: u64 = tracker.values().map(|b| b.depth).sum();
            stats.avg_branch_depth = total_depth as f64 / tracker.len() as f64;
            stats.max_branch_depth = tracker.values().map(|b| b.depth).max().unwrap_or(0);
        }

        stats.last_updated = SystemTime::now();
    }

    /// Get configuration
    pub fn config(&self) -> &DAGConfig {
        &self.config
    }

    /// Update configuration
    pub async fn update_config(&mut self, new_config: DAGConfig) -> DAGResult<()> {
        // Validate new configuration
        if new_config.max_branches == 0 {
            return Err(DAGError::InvalidBranchConfig(
                "max_branches must be greater than 0".to_string(),
            ));
        }

        self.config = new_config.clone();

        // Update conflict detector configuration
        // Note: This would require recreating the conflict detector in a real implementation

        Ok(())
    }

    /// Reset all DAG state (for testing)
    #[cfg(test)]
    pub async fn reset(&self) -> DAGResult<()> {
        let mut tracker = self.branch_tracker.write().await;
        tracker.clear();

        let mut stats = self.statistics.write().await;
        *stats = DAGStats::default();

        Ok(())
    }
}
