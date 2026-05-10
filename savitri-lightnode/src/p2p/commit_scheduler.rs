//! Canonical Commit Scheduler for Parallel DAG Execution
//!
//! Manages block lifecycle from speculative admission through canonical commit,
//! implementing global conflict sets with deterministic winner selection and
//! deferred commit for nonce-gap recovery.
//!
//! ## Pipeline
//! 2. **Conflict Detection**: Conflict keys checked against existing blocks
//! 3. **Resolution**: Conflicting blocks resolved via deterministic winner selection
//! 5. **Commit/Defer**: Block committed or deferred (awaiting dependencies)

// Module is actively integrated

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;
use tracing::{debug, info, warn};

use super::conflict_keys::*;
use crate::storage::BlockAndAccountStorageTrait;

/// The canonical commit scheduler.
pub struct CommitScheduler {
    storage: std::sync::Arc<dyn BlockAndAccountStorageTrait>,

    // ── Block tracking ───────────────────────────
    block_meta: HashMap<BlockHash, ExecutedBlockMeta>,

    // ── Commit queues ────────────────────────────
    ready_queue: VecDeque<BlockHash>,
    deferred_by_nonce: HashMap<Vec<u8>, Vec<BlockHash>>,

    // ── Conflict tracking ────────────────────────
    conflict_sets: HashMap<u64, ConflictSet>,
    conflict_key_index: HashMap<ConflictKey, HashSet<u64>>,
    block_conflict_sets: HashMap<BlockHash, HashSet<u64>>,
    next_conflict_id: u64,

    // ── Canonical state ──────────────────────────
    account_cursors: AccountCommitCursor,
    committed_blocks: HashSet<BlockHash>,

    // ── Stats ────────────────────────────────────
    pub stats: CommitStats,
}

impl CommitScheduler {
    pub fn new(storage: std::sync::Arc<dyn BlockAndAccountStorageTrait>) -> Self {
        Self {
            storage,
            block_meta: HashMap::new(),
            ready_queue: VecDeque::new(),
            deferred_by_nonce: HashMap::new(),
            conflict_sets: HashMap::new(),
            conflict_key_index: HashMap::new(),
            block_conflict_sets: HashMap::new(),
            next_conflict_id: 0,
            account_cursors: AccountCommitCursor::new(),
            committed_blocks: HashSet::new(),
            stats: CommitStats::default(),
        }
    }

    /// Admit a speculatively executed block into the commit pipeline.
    pub fn admit_block(&mut self, meta: ExecutedBlockMeta) -> BlockStatus {
        let hash = meta.block_hash;

        if self.block_meta.contains_key(&hash) || self.committed_blocks.contains(&hash) {
            debug!(hash = %hex::encode(&hash[..8]), "Block already known, skipping admission");
            return BlockStatus::Committed;
        }

        let conflict_keys = meta.conflict_keys.clone();

        // Collect existing blocks that conflict (avoid borrow issues)
        let existing_entries: Vec<(BlockHash, Vec<ConflictKey>)> = self
            .block_meta
            .iter()
            .filter(|(h, m)| {
                **h != hash
                    && !matches!(
                        m.status,
                        BlockStatus::Committed | BlockStatus::Rejected | BlockStatus::ResolvedLoser
                    )
            })
            .map(|(h, m)| (*h, m.conflict_keys.clone()))
            .collect();

        let mut conflicting_set_ids = HashSet::new();
        for key in &conflict_keys {
            for (existing_hash, existing_keys) in &existing_entries {
                if existing_keys.contains(key) {
                    let set_id =
                        self.find_or_create_conflict_set(key.clone(), *existing_hash, hash);
                    conflicting_set_ids.insert(set_id);
                }
            }
        }

        let status = if conflicting_set_ids.is_empty() {
            BlockStatus::Accepted
        } else {
            self.block_conflict_sets
                .insert(hash, conflicting_set_ids.clone());
            BlockStatus::Conflicting
        };

        let mut meta = meta;
        meta.status = status;
        self.block_meta.insert(hash, meta);

        if status == BlockStatus::Accepted {
            self.ready_queue.push_back(hash);
        }

        // Auto-resolve conflict sets
        let ids_to_resolve: Vec<u64> = conflicting_set_ids.into_iter().collect();
        for set_id in ids_to_resolve {
            self.try_resolve_conflict_set(set_id);
        }

        info!(
            hash = %hex::encode(&hash[..8]),
            status = ?status,
            "Block admitted to commit scheduler"
        );

        status
    }

    fn find_or_create_conflict_set(
        &mut self,
        key: ConflictKey,
        existing_block: BlockHash,
        new_block: BlockHash,
    ) -> u64 {
        if let Some(set_ids) = self.conflict_key_index.get(&key) {
            for &set_id in set_ids {
                if let Some(set) = self.conflict_sets.get_mut(&set_id) {
                    if set.members.contains(&existing_block) && !set.resolved {
                        if !set.members.contains(&new_block) {
                            set.members.push(new_block);
                        }
                        return set_id;
                    }
                }
            }
        }

        let set_id = self.next_conflict_id;
        self.next_conflict_id += 1;

        let conflict_set = ConflictSet {
            id: set_id,
            key: key.clone(),
            members: vec![existing_block, new_block],
            resolved: false,
            winner: None,
            created_at: Instant::now(),
        };

        self.conflict_sets.insert(set_id, conflict_set);
        self.conflict_key_index
            .entry(key)
            .or_default()
            .insert(set_id);

        self.block_conflict_sets
            .entry(existing_block)
            .or_default()
            .insert(set_id);
        self.block_conflict_sets
            .entry(new_block)
            .or_default()
            .insert(set_id);

        self.stats.conflict_sets_created += 1;
        set_id
    }

    fn try_resolve_conflict_set(&mut self, set_id: u64) {
        // Extract candidates first to avoid borrow conflict
        let (candidates, member_hashes) = {
            let set = match self.conflict_sets.get(&set_id) {
                Some(s) if !s.resolved => s,
                _ => return,
            };
            let candidates: Vec<BlockMeta> = set
                .members
                .iter()
                .filter_map(|hash| {
                    self.block_meta.get(hash).map(|meta| BlockMeta {
                        block_hash: *hash,
                        topo_rank: meta.topo_rank,
                        pou_score: meta.pou_score,
                        height: meta.height,
                    })
                })
                .collect();
            if candidates.len() < 2 {
                return;
            }
            (candidates, set.members.clone())
        };

        let winner = match choose_winner(&candidates) {
            Some(w) => w,
            None => return,
        };

        // Now mutate
        if let Some(set) = self.conflict_sets.get_mut(&set_id) {
            set.resolved = true;
            set.winner = Some(winner);
        }
        self.stats.conflict_sets_resolved += 1;

        for hash in &member_hashes {
            if *hash == winner {
                let all_resolved = self
                    .block_conflict_sets
                    .get(hash)
                    .map(|ids| {
                        ids.iter().all(|id| {
                            self.conflict_sets
                                .get(id)
                                .map(|s| s.resolved)
                                .unwrap_or(true)
                        })
                    })
                    .unwrap_or(true);
                if all_resolved {
                    if let Some(meta) = self.block_meta.get_mut(hash) {
                        meta.status = BlockStatus::ResolvedWinner;
                    }
                    self.ready_queue.push_back(*hash);
                }
            } else {
                if let Some(meta) = self.block_meta.get_mut(hash) {
                    meta.status = BlockStatus::ResolvedLoser;
                }
            }
        }

        info!(
            set_id,
            winner = %hex::encode(&winner[..8]),
            members = member_hashes.len(),
            "Conflict set resolved"
        );
    }

    /// Process the ready queue. Returns blocks ready for storage commit.
    pub fn drain_ready(&mut self) -> Vec<(BlockHash, ExecutedBlockMeta)> {
        let mut committed = Vec::new();
        let mut to_defer: Vec<(BlockHash, Vec<u8>)> = Vec::new();

        while let Some(hash) = self.ready_queue.pop_front() {
            let status = self.block_meta.get(&hash).map(|m| m.status);
            match status {
                Some(BlockStatus::Committed)
                | Some(BlockStatus::Rejected)
                | Some(BlockStatus::ResolvedLoser) => continue,
                None => continue,
                _ => {}
            }

            let has_unresolved = self
                .block_conflict_sets
                .get(&hash)
                .map(|ids| {
                    ids.iter().any(|id| {
                        self.conflict_sets
                            .get(id)
                            .map(|s| !s.resolved)
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false);
            if has_unresolved {
                continue;
            }

            // Validate TX nonces against canonical cursor
            let signed_txs: Vec<crate::tx::SignedTx> = match self.block_meta.get(&hash) {
                Some(m) => m.pending_data.signed_txs.clone(),
                None => continue,
            };

            let mut all_ok = true;
            let mut defer_account: Option<Vec<u8>> = None;

            for tx in &signed_txs {
                let sender = crate::p2p::block::normalize_address_bytes(&tx.from);
                let check =
                    self.account_cursors
                        .check_tx_nonce(&sender, tx.nonce, self.storage.as_ref());
                match check {
                    TxCommitCheck::Ok => {}
                    TxCommitCheck::Deferred => {
                        all_ok = false;
                        defer_account = Some(sender);
                        break;
                    }
                    TxCommitCheck::Reject => {
                        all_ok = false;
                        break;
                    }
                }
            }

            if !all_ok {
                if let Some(account) = defer_account {
                    to_defer.push((hash, account));
                    self.stats.blocks_deferred += 1;
                }
                continue;
            }

            // All checks passed — mark as committed
            if let Some(meta) = self.block_meta.get_mut(&hash) {
                meta.status = BlockStatus::Committed;
            }

            // Update canonical nonce cursors
            if let Some(meta) = self.block_meta.get(&hash) {
                for (addr, account) in &meta.state_diff {
                    self.account_cursors.advance(addr, account.nonce);
                }
                self.stats.blocks_committed += 1;
                self.stats.txs_committed += meta.pending_data.signed_txs.len() as u64;
            }

            self.committed_blocks.insert(hash);

            if let Some(meta) = self.block_meta.get(&hash).cloned() {
                committed.push((hash, meta));
            }
        }

        // Process deferrals
        for (hash, account) in to_defer {
            if let Some(meta) = self.block_meta.get_mut(&hash) {
                meta.status = BlockStatus::Deferred;
            }
            self.deferred_by_nonce
                .entry(account)
                .or_default()
                .push(hash);
        }

        committed
    }

    /// Wake deferred blocks after a commit advances the canonical cursor.
    pub fn wake_deferred(&mut self) {
        let mut woken = Vec::new();

        for (account, block_hashes) in &self.deferred_by_nonce {
            let cursor_nonce = self.account_cursors.next_nonce(account).unwrap_or_else(|| {
                crate::storage::BlockAndAccountStorage::get_account(self.storage.as_ref(), account)
                    .ok()
                    .flatten()
                    .map(|a| a.nonce)
                    .unwrap_or(0)
            });
            for hash in block_hashes {
                if let Some(meta) = self.block_meta.get(hash) {
                    let has_matching = meta.pending_data.signed_txs.iter().any(|tx| {
                        let sender = crate::p2p::block::normalize_address_bytes(&tx.from);
                        sender == *account && tx.nonce == cursor_nonce
                    });
                    if has_matching {
                        woken.push(*hash);
                    }
                }
            }
        }

        for hash in &woken {
            if let Some(meta) = self.block_meta.get_mut(hash) {
                meta.status = BlockStatus::Accepted;
            }
            self.ready_queue.push_back(*hash);
            for block_list in self.deferred_by_nonce.values_mut() {
                block_list.retain(|h| h != hash);
            }
        }
        self.deferred_by_nonce.retain(|_, v| !v.is_empty());

        if !woken.is_empty() {
            info!(
                woken = woken.len(),
                "Woke deferred blocks after cursor advance"
            );
        }
    }

    pub fn block_status(&self, hash: &BlockHash) -> Option<BlockStatus> {
        if self.committed_blocks.contains(hash) {
            return Some(BlockStatus::Committed);
        }
        self.block_meta.get(hash).map(|m| m.status)
    }

    pub fn is_known(&self, hash: &BlockHash) -> bool {
        self.block_meta.contains_key(hash) || self.committed_blocks.contains(hash)
    }

    pub fn get_stats(&self) -> &CommitStats {
        &self.stats
    }

    /// Garbage collect old block metadata.
    pub fn gc(&mut self, keep_height: u64) {
        let to_remove: Vec<BlockHash> = self
            .block_meta
            .iter()
            .filter(|(_, meta)| {
                meta.height < keep_height
                    && matches!(
                        meta.status,
                        BlockStatus::Committed | BlockStatus::Rejected | BlockStatus::ResolvedLoser
                    )
            })
            .map(|(hash, _)| *hash)
            .collect();

        for hash in &to_remove {
            self.block_meta.remove(hash);
            self.block_conflict_sets.remove(hash);
            self.committed_blocks.remove(hash);
        }

        self.conflict_sets.retain(|_, set| {
            !set.resolved || set.members.iter().any(|h| self.block_meta.contains_key(h))
        });

        if !to_remove.is_empty() {
            debug!(
                removed = to_remove.len(),
                keep_height, "GC: removed old block metadata"
            );
        }
    }

    pub fn pending_count(&self) -> usize {
        self.block_meta
            .values()
            .filter(|m| {
                !matches!(
                    m.status,
                    BlockStatus::Committed | BlockStatus::Rejected | BlockStatus::ResolvedLoser
                )
            })
            .count()
    }

    pub fn unresolved_conflicts(&self) -> usize {
        self.conflict_sets.values().filter(|s| !s.resolved).count()
    }
}
