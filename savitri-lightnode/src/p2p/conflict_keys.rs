//! Conflict key types for DAG-based parallel execution with global conflict sets.
//!
//! Every block's transactions produce conflict keys that determine whether
//! two blocks can be committed in parallel (non-conflicting) or require
//! deterministic winner selection (conflicting).

// Module is actively integrated

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Instant;

pub type BlockHash = [u8; 64];
pub type StateKey = Vec<u8>;

/// Conflict key for DAG-based parallel execution.
/// Two blocks conflict if they share any ConflictKey.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum ConflictKey {
    /// Exact transaction hash — prevents duplicate inclusion
    TxHash([u8; 32]),
    /// Account + nonce pair — prevents double-spend / nonce collision
    AccountNonce { sender: [u8; 32], nonce: u64 },
    /// Phase 2: Contract storage slot — prevents concurrent state mutation
    StorageSlot { contract: [u8; 32], slot: [u8; 32] },
    /// Phase 2: Resource identifier — prevents concurrent resource access
    Resource {
        account: [u8; 32],
        resource_type: u16,
    },
}

/// Status of a block in the DAG commit pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockStatus {
    Pending,
    /// Validated and admitted to DAG — no conflicts detected
    Accepted,
    /// Admitted to DAG but conflicts detected with other blocks
    Conflicting,
    /// Winner of a resolved conflict set — ready for commit
    ResolvedWinner,
    /// Loser of a resolved conflict set — will not be committed
    ResolvedLoser,
    /// Successfully committed to canonical state
    Committed,
    /// Invalid or permanently uncommittable
    Rejected,
    /// Valid but waiting for nonce/state dependencies to be satisfied
    Deferred,
}

/// A set of blocks that conflict on the same ConflictKey.
#[derive(Debug, Clone)]
pub struct ConflictSet {
    pub id: u64,
    pub key: ConflictKey,
    pub members: Vec<BlockHash>,
    pub resolved: bool,
    pub winner: Option<BlockHash>,
    pub created_at: Instant,
}

/// Metadata for a speculatively executed block, including its read/write sets
/// and conflict keys, used by the commit scheduler.
#[derive(Clone)]
pub struct ExecutedBlockMeta {
    pub block_hash: BlockHash,
    pub height: u64,
    pub group_id: String,
    pub status: BlockStatus,
    /// State keys read during speculative execution
    pub read_set: BTreeSet<StateKey>,
    /// State keys written during speculative execution
    pub write_set: BTreeSet<StateKey>,
    /// Conflict keys derived from transactions
    pub conflict_keys: Vec<ConflictKey>,
    /// Speculative state diff (account changes)
    pub state_diff: BTreeMap<Vec<u8>, savitri_core::Account>,
    /// Topological rank in DAG (lower = earlier)
    pub topo_rank: u64,
    /// Proposer's PoU score
    pub pou_score: u32,
    /// Block timestamp
    pub timestamp: u64,
    /// The pending block data for commit
    pub pending_data: crate::p2p::types::PendingBlockData,
}

/// Tracks the canonical committed nonce per account.
#[derive(Debug, Clone, Default)]
pub struct AccountCommitCursor {
    next_nonces: HashMap<Vec<u8>, u64>,
}

impl AccountCommitCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next expected nonce for an account.
    pub fn next_nonce(&self, account: &[u8]) -> Option<u64> {
        self.next_nonces.get(account).copied()
    }

    /// Advance the nonce cursor for an account after successful commit.
    pub fn advance(&mut self, account: &[u8], new_nonce: u64) {
        let entry = self.next_nonces.entry(account.to_vec()).or_insert(0);
        if new_nonce > *entry {
            *entry = new_nonce;
        }
    }

    /// Check if a transaction's nonce is committable against canonical state.
    pub fn check_tx_nonce(
        &self,
        sender: &[u8],
        tx_nonce: u64,
        storage: &dyn crate::storage::BlockAndAccountStorageTrait,
    ) -> TxCommitCheck {
        let expected = self.next_nonces.get(sender).copied().unwrap_or_else(|| {
            crate::storage::BlockAndAccountStorage::get_account(storage, sender)
                .ok()
                .flatten()
                .map(|acc| acc.nonce)
                .unwrap_or(0)
        });
        if tx_nonce == expected {
            TxCommitCheck::Ok
        } else if tx_nonce > expected {
            TxCommitCheck::Deferred
        } else {
            TxCommitCheck::Reject
        }
    }
}

/// Result of checking a transaction against the canonical nonce cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxCommitCheck {
    Ok,
    Deferred,
    Reject,
}

/// Statistics for the commit scheduler.
#[derive(Debug, Clone, Default)]
pub struct CommitStats {
    pub blocks_committed: u64,
    pub blocks_deferred: u64,
    pub blocks_rejected: u64,
    pub conflict_sets_created: u64,
    pub conflict_sets_resolved: u64,
    pub txs_committed: u64,
    pub txs_rejected_nonce: u64,
}

// ── Conflict key extraction ──────────────────────────────────────────

/// Extract conflict keys from a set of signed transactions.
pub fn extract_conflict_keys(signed_txs: &[crate::tx::SignedTx]) -> Vec<ConflictKey> {
    let mut keys = Vec::with_capacity(signed_txs.len() * 2);
    for tx in signed_txs {
        // 1. TxHash conflict key
        if let Ok(bytes) = crate::tx::serialize_signed_tx(tx) {
            let hash = crate::tx::hash_signed_tx_bytes(&bytes);
            let mut h = [0u8; 32];
            let len = hash.len().min(32);
            h[..len].copy_from_slice(&hash[..len]);
            keys.push(ConflictKey::TxHash(h));
        }

        // 2. AccountNonce conflict key
        let sender_bytes = hex::decode(&tx.from).unwrap_or_default();
        let mut sender = [0u8; 32];
        let len = sender_bytes.len().min(32);
        sender[..len].copy_from_slice(&sender_bytes[..len]);
        keys.push(ConflictKey::AccountNonce {
            sender,
            nonce: tx.nonce,
        });
    }
    keys
}

/// Extract read/write sets from speculative execution overlay.
pub fn extract_read_write_sets(
    overlay: &BTreeMap<Vec<u8>, savitri_core::Account>,
    signed_txs: &[crate::tx::SignedTx],
) -> (BTreeSet<StateKey>, BTreeSet<StateKey>) {
    let mut read_set = BTreeSet::new();
    let mut write_set = BTreeSet::new();

    for key in overlay.keys() {
        write_set.insert(key.clone());
    }
    for tx in signed_txs {
        if let Ok(sender) = hex::decode(&tx.from) {
            read_set.insert(sender);
        }
        if let Ok(recipient) = hex::decode(&tx.to) {
            read_set.insert(recipient);
        }
    }
    (read_set, write_set)
}

// ── Deterministic winner selection ───────────────────────────────────

/// Metadata used for deterministic winner selection in a conflict set.
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub block_hash: BlockHash,
    pub topo_rank: u64,
    pub pou_score: u32,
    pub height: u64,
}

/// Deterministic winner selection for a conflict set.
/// Ordering: lowest topo_rank -> highest score -> lowest height -> lexicographic hash.
pub fn choose_winner(candidates: &[BlockMeta]) -> Option<BlockHash> {
    candidates
        .iter()
        .min_by(|a, b| {
            a.topo_rank
                .cmp(&b.topo_rank)
                .then(b.pou_score.cmp(&a.pou_score))
                .then(a.height.cmp(&b.height))
                .then(a.block_hash.cmp(&b.block_hash))
        })
        .map(|m| m.block_hash)
}
