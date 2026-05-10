//! DAG (Directed Acyclic Graph) Manager for multi-group block convergence.
//!
//! Tracks DAG tips per group, manages cross-group parent references,
//! provides deterministic ordering for finalization, and handles TX deduplication.

// DAG Manager is actively used in the block commit pipeline for multi-group ordering.
// Architecture: DAG orders blocks → BFT certifies (masternodes) → PoU elects proposer (lightnodes)

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Block hash type (uses [u8; 64] to match Block::hash wire format; only first 32 bytes populated)
pub type BlockHash = [u8; 64];

/// Metadata for a block in the DAG
#[derive(Debug, Clone)]
pub struct DagBlock {
    pub hash: BlockHash,
    pub height: u64,
    pub group_id: String,
    /// All parent hashes (own group parent + cross-group references)
    pub parent_hashes: Vec<BlockHash>,
    /// Transaction hashes included in this block
    pub tx_hashes: Vec<[u8; 32]>,
    /// PoU score of the proposer (used for PHANTOM ordering)
    pub proposer_pou_score: u32,
    /// Timestamp of block creation
    pub timestamp: u64,
    /// Proposer public key (Ed25519, 32 bytes) — used for equivocation detection
    pub proposer: [u8; 32],
}

/// Frontier tip metadata used for parent selection and merge policies.
#[derive(Debug, Clone)]
pub struct DagFrontierTip {
    pub hash: BlockHash,
    pub height: u64,
    pub group_id: String,
    pub timestamp: u64,
}

/// Inner state for DagManager, protected by a single RwLock to eliminate
/// nested lock acquisition and ensure consistent reads/writes.
struct DagState {
    /// All blocks in the DAG, indexed by block hash
    blocks: HashMap<BlockHash, DagBlock>,
    /// Current tip (latest block hash) per group
    tips: HashMap<String, BlockHash>,
    /// Transaction hashes already included in any DAG branch (for deduplication)
    seen_tx_hashes: HashSet<[u8; 32]>,
    /// Blocks indexed by height for ordering
    blocks_by_height: BTreeMap<u64, Vec<BlockHash>>,
    /// Maximum height seen across all groups
    max_height: u64,
    /// Maximum height per group — enables independent parallel chains
    max_height_per_group: HashMap<String, u64>,
    /// Equivocation index: (height, proposer) -> first seen block hash
    /// Detects when the same proposer submits different blocks at the same height
    proposer_at_height: HashMap<(u64, [u8; 32]), BlockHash>,
}

/// Evidence of equivocation (two different blocks from same proposer at same height)
#[derive(Debug, Clone)]
pub struct EquivocationProof {
    pub proposer: [u8; 32],
    pub height: u64,
    pub block_hash_a: BlockHash,
    pub block_hash_b: BlockHash,
}

impl DagState {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            tips: HashMap::new(),
            seen_tx_hashes: HashSet::new(),
            blocks_by_height: BTreeMap::new(),
            max_height: 0,
            max_height_per_group: HashMap::new(),
            proposer_at_height: HashMap::new(),
        }
    }
}

/// DAG Manager: tracks block DAG structure across multiple groups.
/// Uses a single coarse lock for consistency (all fields are read/written together).
pub struct DagManager {
    state: Arc<RwLock<DagState>>,
}

impl DagManager {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(DagState::new())),
        }
    }

    /// Add a committed block to the DAG.
    /// Updates tips, tracks TX hashes for deduplication, indexes by height.
    /// Returns `Some(EquivocationProof)` if the same proposer already has a different
    /// block at this height (evidence of double-signing).
    pub async fn add_block(&self, block: DagBlock) -> Option<EquivocationProof> {
        let hash = block.hash;
        let height = block.height;
        let group_id = block.group_id.clone();
        let proposer = block.proposer;
        if height == u64::MAX {
            warn!(
                group_id = %group_id,
                hash = %hex::encode(&hash[..16]),
                "Rejected DAG block with invalid height u64::MAX"
            );
            return None;
        }

        let mut s = self.state.write().await;

        // Equivocation detection: same proposer + same height + different hash
        let mut equivocation = None;
        if proposer != [0u8; 32] {
            // Skip zero-key (genesis or unknown)
            let key = (height, proposer);
            if let Some(&existing_hash) = s.proposer_at_height.get(&key) {
                if existing_hash != hash {
                    warn!(
                        proposer = %hex::encode(&proposer[..8]),
                        height,
                        block_a = %hex::encode(&existing_hash[..16]),
                        block_b = %hex::encode(&hash[..16]),
                        "EQUIVOCATION DETECTED: same proposer submitted different blocks at same height"
                    );
                    equivocation = Some(EquivocationProof {
                        proposer,
                        height,
                        block_hash_a: existing_hash,
                        block_hash_b: hash,
                    });
                }
            } else {
                s.proposer_at_height.insert(key, hash);
            }
        }

        // Track transaction hashes for deduplication
        for tx_hash in &block.tx_hashes {
            s.seen_tx_hashes.insert(*tx_hash);
        }

        // Index by height
        s.blocks_by_height.entry(height).or_default().push(hash);

        // Update max height (global + per-group)
        if height > s.max_height {
            s.max_height = height;
        }
        let group_max = s.max_height_per_group.entry(group_id.clone()).or_insert(0);
        if height > *group_max {
            *group_max = height;
        }

        // Update group tip (only if at or beyond current tip height)
        let should_update = if let Some(current_tip) = s.tips.get(&group_id) {
            s.blocks
                .get(current_tip)
                .map(|b| height >= b.height)
                .unwrap_or(true)
        } else {
            true
        };
        if should_update {
            s.tips.insert(group_id.clone(), hash);
        }

        // Store block
        s.blocks.insert(hash, block);

        info!(
            height,
            group_id = %group_id,
            hash = %hex::encode(&hash[..16]),
            tx_count = s.blocks.get(&hash).map(|b| b.tx_hashes.len()).unwrap_or(0),
            "Block added to DAG"
        );

        equivocation
    }

    /// Get parent hashes for a new block proposal in the given group.
    /// Returns: own group's tip + tips from other known groups (cross-group edges).
    pub async fn get_parent_hashes(&self, group_id: &str) -> Vec<BlockHash> {
        let s = self.state.read().await;
        let mut parents = Vec::new();

        // Own group tip first (primary parent)
        if let Some(own_tip) = s.tips.get(group_id) {
            parents.push(*own_tip);
        }

        // Add tips from other groups (cross-group references) in deterministic order.
        let mut other_groups: Vec<_> = s
            .tips
            .iter()
            .filter(|(gid, _)| gid.as_str() != group_id)
            .collect();
        other_groups.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (_, tip_hash) in other_groups {
            parents.push(*tip_hash);
        }

        parents
    }

    /// Get current DAG frontier (all blocks with no children), sorted deterministically.
    pub async fn get_frontier_tips(&self) -> Vec<DagFrontierTip> {
        let s = self.state.read().await;
        let mut referenced: HashSet<BlockHash> = HashSet::new();
        for block in s.blocks.values() {
            for parent in &block.parent_hashes {
                if *parent != [0u8; 64] {
                    referenced.insert(*parent);
                }
            }
        }

        let mut frontier: Vec<DagFrontierTip> = s
            .blocks
            .values()
            .filter(|b| !referenced.contains(&b.hash))
            .map(|b| DagFrontierTip {
                hash: b.hash,
                height: b.height,
                group_id: b.group_id.clone(),
                timestamp: b.timestamp,
            })
            .collect();

        // Oldest frontier first to encourage merge of lagging branches.
        frontier.sort_by(|a, b| a.height.cmp(&b.height).then_with(|| a.hash.cmp(&b.hash)));
        frontier
    }

    /// Build deterministic parent candidates from the live frontier.
    /// Primary parent preference: current group tip if present in frontier; otherwise highest frontier tip.
    pub async fn get_frontier_parent_candidates(&self, group_id: &str) -> Vec<BlockHash> {
        let frontier = self.get_frontier_tips().await;
        if frontier.is_empty() {
            return Vec::new();
        }

        let mut parents: Vec<BlockHash> = Vec::with_capacity(frontier.len());
        if let Some(primary) = frontier
            .iter()
            .find(|tip| tip.group_id == group_id)
            .map(|tip| tip.hash)
            .or_else(|| frontier.last().map(|tip| tip.hash))
        {
            parents.push(primary);
        }

        for tip in frontier {
            if !parents.contains(&tip.hash) {
                parents.push(tip.hash);
            }
        }
        parents
    }

    /// Get the current tip hash for a specific group
    pub async fn get_group_tip(&self, group_id: &str) -> Option<BlockHash> {
        self.state.read().await.tips.get(group_id).copied()
    }

    /// Get all current tips across all groups
    pub async fn get_all_tips(&self) -> HashMap<String, BlockHash> {
        self.state.read().await.tips.clone()
    }

    /// Get the maximum block height across all groups (DAG height)
    pub async fn get_max_height(&self) -> u64 {
        let s = self.state.read().await;
        if s.max_height == u64::MAX {
            return s.blocks_by_height.keys().next_back().copied().unwrap_or(0);
        }
        s.max_height
    }

    /// Get the maximum block height for a specific group.
    /// Each group maintains its own independent height counter, enabling
    /// parallel block production across groups without height contention.
    pub async fn get_max_height_for_group(&self, group_id: &str) -> u64 {
        self.state
            .read()
            .await
            .max_height_per_group
            .get(group_id)
            .copied()
            .unwrap_or(0)
    }

    /// Check if a transaction has already been included in any DAG branch.
    /// Used for TX deduplication before block proposal/execution.
    pub async fn is_tx_seen(&self, tx_hash: &[u8; 32]) -> bool {
        self.state.read().await.seen_tx_hashes.contains(tx_hash)
    }

    /// Filter out transactions that have already been included in the DAG.
    /// Returns only unseen transactions (single lock acquisition for the batch).
    pub async fn dedup_transactions(&self, tx_hashes: &[[u8; 32]]) -> Vec<[u8; 32]> {
        let s = self.state.read().await;
        tx_hashes
            .iter()
            .filter(|h| !s.seen_tx_hashes.contains(*h))
            .copied()
            .collect()
    }

    pub async fn has_block(&self, hash: &BlockHash) -> bool {
        self.state.read().await.blocks.contains_key(hash)
    }

    /// Validate that all parent hashes in a block reference known blocks.
    /// Returns list of missing parent hashes (empty = all valid).
    pub async fn validate_parents(&self, parent_hashes: &[BlockHash]) -> Vec<BlockHash> {
        let s = self.state.read().await;
        parent_hashes
            .iter()
            .filter(|h| !s.blocks.contains_key(*h) && **h != [0u8; 64]) // skip genesis zero-hash
            .copied()
            .collect()
    }

    /// Get canonical ordering of blocks for deterministic finalization.
    /// Alias for `get_ordering()` — used by CommitScheduler for conflict resolution.
    pub async fn get_canonical_order(&self) -> Vec<BlockHash> {
        self.get_ordering().await
    }

    /// Get deterministic ordering of blocks using PoU-weighted PHANTOM-style ordering.
    /// Returns block hashes in finalization order (lower height first, then by PoU score).
    pub async fn get_ordering(&self) -> Vec<BlockHash> {
        let s = self.state.read().await;

        let mut ordered = Vec::new();
        for (_height, hashes) in s.blocks_by_height.iter() {
            // Sort blocks at same height by PoU score (descending), then hash (deterministic tiebreak)
            let mut height_blocks: Vec<_> = hashes
                .iter()
                .filter_map(|h| s.blocks.get(h).map(|b| (h, b)))
                .collect();
            height_blocks.sort_by(|(h_a, a), (h_b, b)| {
                b.proposer_pou_score
                    .cmp(&a.proposer_pou_score)
                    .then_with(|| h_a.cmp(h_b))
            });
            for (hash, _) in height_blocks {
                ordered.push(*hash);
            }
        }
        ordered
    }

    /// Get a block by its hash
    pub async fn get_block(&self, hash: &BlockHash) -> Option<DagBlock> {
        self.state.read().await.blocks.get(hash).cloned()
    }

    /// Get all blocks at a specific height
    pub async fn get_blocks_at_height(&self, height: u64) -> Vec<DagBlock> {
        let s = self.state.read().await;
        s.blocks_by_height
            .get(&height)
            .map(|hashes| {
                hashes
                    .iter()
                    .filter_map(|h| s.blocks.get(h).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Prune old blocks below a finalized height to bound memory usage.
    /// Keeps all blocks at or above `min_height`.
    /// Also prunes `seen_tx_hashes` for transactions in removed blocks and
    /// removes stale group tips that point to pruned blocks.
    /// Returns the number of blocks pruned.
    pub async fn prune_below(&self, min_height: u64) -> usize {
        let mut s = self.state.write().await;

        let heights_to_remove: Vec<u64> = s
            .blocks_by_height
            .range(..min_height)
            .map(|(h, _)| *h)
            .collect();

        let mut pruned = 0usize;
        for height in heights_to_remove {
            if let Some(hashes) = s.blocks_by_height.remove(&height) {
                for hash in hashes {
                    if let Some(block) = s.blocks.remove(&hash) {
                        for tx_hash in &block.tx_hashes {
                            s.seen_tx_hashes.remove(tx_hash);
                        }
                        // Also clean equivocation index
                        s.proposer_at_height.remove(&(height, block.proposer));
                        pruned += 1;
                    }
                }
            }
        }

        // Clean up stale tips pointing to pruned blocks
        let stale_groups: Vec<String> = s
            .tips
            .iter()
            .filter(|(_, tip_hash)| !s.blocks.contains_key(*tip_hash))
            .map(|(gid, _)| gid.clone())
            .collect();
        for gid in stale_groups {
            s.tips.remove(&gid);
        }
        s.max_height = s.blocks_by_height.keys().next_back().copied().unwrap_or(0);

        if pruned > 0 {
            info!(
                pruned_blocks = pruned,
                min_height,
                remaining_seen_txs = s.seen_tx_hashes.len(),
                "Pruned old DAG blocks and associated TX hashes"
            );
        }

        pruned
    }

    /// Get current tips: (group_id, hash, height) per group
    pub async fn get_tips(&self) -> Vec<(String, BlockHash, u64)> {
        let s = self.state.read().await;
        s.tips
            .iter()
            .filter_map(|(gid, hash)| s.blocks.get(hash).map(|b| (gid.clone(), *hash, b.height)))
            .collect()
    }

    /// Get statistics about the DAG
    pub async fn stats(&self) -> DagStats {
        let s = self.state.read().await;

        DagStats {
            total_blocks: s.blocks.len(),
            active_groups: s.tips.len(),
            max_height: s.max_height,
            seen_tx_count: s.seen_tx_hashes.len(),
        }
    }
}

/// DAG statistics for monitoring
#[derive(Debug, Clone)]
pub struct DagStats {
    pub total_blocks: usize,
    pub active_groups: usize,
    pub max_height: u64,
    pub seen_tx_count: usize,
}

impl Default for DagManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(height: u64, proposer: [u8; 32], unique: u8) -> DagBlock {
        let mut hash = [0u8; 64];
        hash[0] = unique;
        hash[1] = (height & 0xff) as u8;
        DagBlock {
            hash,
            height,
            group_id: "test_group".to_string(),
            parent_hashes: vec![],
            tx_hashes: vec![],
            proposer_pou_score: 800,
            timestamp: 1000 + height,
            proposer,
        }
    }

    #[tokio::test]
    async fn equivocation_detected_same_proposer_same_height() {
        let dag = DagManager::new();
        let proposer = [42u8; 32];

        let block_a = make_block(10, proposer, 1);
        assert!(dag.add_block(block_a.clone()).await.is_none());

        let block_b = make_block(10, proposer, 2);
        let proof = dag.add_block(block_b.clone()).await;
        assert!(proof.is_some(), "MUST detect equivocation");

        let p = proof.unwrap();
        assert_eq!(p.proposer, proposer);
        assert_eq!(p.height, 10);
        assert_eq!(p.block_hash_a, block_a.hash);
        assert_eq!(p.block_hash_b, block_b.hash);
    }

    #[tokio::test]
    async fn no_equivocation_different_proposers() {
        let dag = DagManager::new();
        assert!(dag.add_block(make_block(10, [1u8; 32], 1)).await.is_none());
        assert!(dag.add_block(make_block(10, [2u8; 32], 2)).await.is_none());
    }

    #[tokio::test]
    async fn no_equivocation_same_proposer_different_height() {
        let dag = DagManager::new();
        let p = [42u8; 32];
        assert!(dag.add_block(make_block(10, p, 1)).await.is_none());
        assert!(dag.add_block(make_block(11, p, 2)).await.is_none());
    }

    #[tokio::test]
    async fn no_equivocation_duplicate_block() {
        let dag = DagManager::new();
        let block = make_block(10, [42u8; 32], 1);
        assert!(dag.add_block(block.clone()).await.is_none());
        assert!(dag.add_block(block).await.is_none());
    }

    #[tokio::test]
    async fn equivocation_zero_proposer_skipped() {
        let dag = DagManager::new();
        assert!(dag.add_block(make_block(10, [0u8; 32], 1)).await.is_none());
        assert!(dag.add_block(make_block(10, [0u8; 32], 2)).await.is_none());
    }

    #[tokio::test]
    async fn prune_removes_old_blocks() {
        let dag = DagManager::new();
        for h in 1..=20u64 {
            dag.add_block(make_block(h, [h as u8; 32], h as u8)).await;
        }
        assert_eq!(dag.stats().await.total_blocks, 20);

        let pruned = dag.prune_below(11).await;
        assert_eq!(pruned, 10);
        assert_eq!(dag.stats().await.total_blocks, 10);
        assert_eq!(dag.stats().await.max_height, 20);
    }

    #[tokio::test]
    async fn prune_cleans_equivocation_index() {
        let dag = DagManager::new();
        let proposer = [42u8; 32];
        dag.add_block(make_block(5, proposer, 1)).await;
        dag.add_block(make_block(15, proposer, 2)).await;

        // Prune below 10 — block at height 5 should be gone
        dag.prune_below(10).await;

        // Re-insert at height 5 with same proposer should NOT trigger equivocation
        // (old entry was cleaned from index)
        let result = dag.add_block(make_block(5, proposer, 3)).await;
        assert!(
            result.is_none(),
            "Pruned proposer_at_height should not trigger false equivocation"
        );
    }

    #[tokio::test]
    async fn prune_cleans_stale_tips() {
        let dag = DagManager::new();
        dag.add_block(make_block(5, [1u8; 32], 1)).await;
        dag.add_block(make_block(15, [2u8; 32], 2)).await;

        dag.prune_below(10).await;
        // Group tip for height-5 block should be removed
        assert_eq!(dag.stats().await.active_groups, 1);
    }
}
