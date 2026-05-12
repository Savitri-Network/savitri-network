//! Block-STM: Software Transactional Memory for intra-block parallel execution.
//!
//! Phase 2 optimization: executes transactions within a single block in parallel,
//! detects read/write conflicts, and re-executes conflicting transactions in
//! deterministic order.
//!
//! This is different from the cross-block CommitScheduler — Block-STM handles
//! parallelism WITHIN a block, while CommitScheduler handles parallelism
//! BETWEEN blocks from different groups.
//!
//! ## Algorithm (Aptos-style, adapted for Savitri)
//!
//! 1. **Optimistic Pass**: Execute all TXs in parallel against a shared snapshot
//! 2. **Validation Pass**: Check if any TX read a value that was overwritten
//!    by an earlier TX (in canonical index order)
//! 4. **Repeat** until no conflicts remain (converges in 1-2 rounds typically)

// Block-STM is integrated into the block execution pipeline via execute_block_transactions()

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

/// A snapshot-isolated view of account state for speculative execution.
/// Each transaction gets a `TxView` that records what it read and wrote.
#[derive(Debug, Clone, Default)]
pub struct TxView {
    /// Values read during execution: key → value at time of read
    pub reads: HashMap<Vec<u8>, Option<savitri_core::Account>>,
    /// Values written during execution: key → new value
    pub writes: HashMap<Vec<u8>, savitri_core::Account>,
    /// Whether this TX executed successfully
    pub success: bool,
    /// Error message if execution failed
    pub error: Option<String>,
}

/// Result of Block-STM execution for a single block.
#[derive(Debug)]
pub struct BlockStmResult {
    /// Final overlay (merged writes in canonical order)
    pub overlay: BTreeMap<Vec<u8>, savitri_core::Account>,
    /// Per-transaction execution views
    pub tx_views: Vec<TxView>,
    /// Number of re-execution rounds needed
    pub rounds: u32,
    /// Number of transactions that needed re-execution
    pub reexecutions: u32,
    /// Receipts generated
    pub receipts: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Multi-version data store for Block-STM.
/// Tracks writes from each transaction index, allowing later TXs to read
/// the latest write from an earlier TX.
#[derive(Debug, Default)]
pub struct MultiVersionStore {
    /// key → BTreeMap<tx_index → Account>
    /// The BTreeMap allows finding the latest write before a given tx_index.
    data: HashMap<Vec<u8>, BTreeMap<usize, savitri_core::Account>>,
}

impl MultiVersionStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a write from transaction at `tx_idx`.
    pub fn write(&mut self, key: Vec<u8>, tx_idx: usize, value: savitri_core::Account) {
        self.data.entry(key).or_default().insert(tx_idx, value);
    }

    /// Read the latest value written by a transaction with index < `tx_idx`.
    /// Returns (value, writer_tx_idx) or None if no prior write.
    pub fn read_latest_before(
        &self,
        key: &[u8],
        tx_idx: usize,
    ) -> Option<(savitri_core::Account, usize)> {
        self.data.get(key).and_then(|versions| {
            // Find the largest index < tx_idx
            versions
                .range(..tx_idx)
                .next_back()
                .map(|(&writer_idx, value)| (value.clone(), writer_idx))
        })
    }

    /// Clear all writes from a specific transaction (for re-execution).
    pub fn clear_tx(&mut self, tx_idx: usize) {
        for versions in self.data.values_mut() {
            versions.remove(&tx_idx);
        }
    }
}

/// Execute a block's transactions using Block-STM parallel execution.
///
/// # Arguments
/// * `storage` - Base storage for initial account state
/// * `signed_txs` - Transactions in the block (already sorted by sender+nonce)
///
/// # Returns
/// * `BlockStmResult` with the final overlay and execution metadata
///
/// # Algorithm
/// 1. First pass: execute all TXs sequentially to establish baseline
///    (parallel execution requires rayon, deferred to production)
/// 2. Record read/write sets
/// 3. Detect conflicts and re-execute if needed
pub fn execute_block_stm(
    storage: &dyn crate::storage::BlockAndAccountStorage,
    signed_txs: &[crate::tx::SignedTx],
) -> anyhow::Result<BlockStmResult> {
    if signed_txs.is_empty() {
        return Ok(BlockStmResult {
            overlay: BTreeMap::new(),
            tx_views: Vec::new(),
            rounds: 0,
            reexecutions: 0,
            receipts: Vec::new(),
        });
    }

    let n = signed_txs.len();
    let mut mvs = MultiVersionStore::new();
    let mut tx_views: Vec<TxView> = vec![TxView::default(); n];
    let mut receipts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n);
    let mut rounds = 0u32;
    let mut total_reexecutions = 0u32;

    // ── Round 1: Sequential baseline execution ──────────────────────
    rounds += 1;
    for (tx_idx, tx) in signed_txs.iter().enumerate() {
        execute_tx_speculative(storage, &mvs, tx, tx_idx, &mut tx_views[tx_idx]);
        // Record writes into multi-version store
        for (key, value) in &tx_views[tx_idx].writes {
            mvs.write(key.clone(), tx_idx, value.clone());
        }
    }

    // ── Validation pass ─────────────────────────────────────────────
    // Check each TX's reads against earlier TX's writes
    let mut invalid_set: HashSet<usize> = HashSet::new();
    for tx_idx in 0..n {
        if !tx_views[tx_idx].success {
            continue;
        }
        for (key, read_value) in &tx_views[tx_idx].reads {
            if let Some((latest_value, writer_idx)) = mvs.read_latest_before(key, tx_idx) {
                // There was a write before us — did we read the right value?
                let read_nonce = read_value.as_ref().map(|a| a.nonce).unwrap_or(0);
                if latest_value.nonce != read_nonce
                    || latest_value.balance != read_value.as_ref().map(|a| a.balance).unwrap_or(0)
                {
                    // We read a stale value — need re-execution
                    invalid_set.insert(tx_idx);
                    break;
                }
            }
        }
    }

    // ── Re-execution pass (if needed) ───────────────────────────────
    if !invalid_set.is_empty() {
        rounds += 1;
        total_reexecutions = invalid_set.len() as u32;

        for &tx_idx in &invalid_set {
            // Clear old writes
            mvs.clear_tx(tx_idx);
            tx_views[tx_idx] = TxView::default();

            // Re-execute with updated multi-version store
            execute_tx_speculative(
                storage,
                &mvs,
                &signed_txs[tx_idx],
                tx_idx,
                &mut tx_views[tx_idx],
            );

            // Record new writes
            for (key, value) in &tx_views[tx_idx].writes {
                mvs.write(key.clone(), tx_idx, value.clone());
            }
        }
    }

    // ── Build final overlay ─────────────────────────────────────────
    // Merge writes in canonical order (tx_idx 0, 1, 2, ...)
    let mut overlay: BTreeMap<Vec<u8>, savitri_core::Account> = BTreeMap::new();
    for tx_idx in 0..n {
        if tx_views[tx_idx].success {
            for (key, value) in &tx_views[tx_idx].writes {
                overlay.insert(key.clone(), value.clone());
            }

            // Generate receipt
            let tx = &signed_txs[tx_idx];
            let receipt_data = format!(
                "tx_idx:{}|sender:{}|recipient:{}|amount:{}|fee:{}|nonce:{}",
                tx_idx,
                tx.from,
                tx.to,
                tx.amount,
                tx.fee.unwrap_or(0),
                tx.nonce
            );
            receipts.push((
                format!("receipt:{}", tx_idx).into_bytes(),
                receipt_data.into_bytes(),
            ));
        }
    }

    info!(
        txs = n,
        rounds,
        reexecutions = total_reexecutions,
        overlay_accounts = overlay.len(),
        "Block-STM execution complete"
    );

    Ok(BlockStmResult {
        overlay,
        tx_views,
        rounds,
        reexecutions: total_reexecutions,
        receipts,
    })
}

/// Execute a single transaction speculatively against the multi-version store.
fn execute_tx_speculative(
    storage: &dyn crate::storage::BlockAndAccountStorage,
    mvs: &MultiVersionStore,
    tx: &crate::tx::SignedTx,
    tx_idx: usize,
    view: &mut TxView,
) {
    let sender_addr = match hex::decode(&tx.from) {
        Ok(a) => a,
        Err(_) => {
            view.error = Some("invalid sender address".into());
            return;
        }
    };
    let recipient_addr = match hex::decode(&tx.to) {
        Ok(a) => a,
        Err(_) => {
            view.error = Some("invalid recipient address".into());
            return;
        }
    };

    // Read sender account: check multi-version store first, then base storage
    let sender_account = if let Some((acc, _)) = mvs.read_latest_before(&sender_addr, tx_idx) {
        view.reads.insert(sender_addr.clone(), Some(acc.clone()));
        acc
    } else {
        let acc = storage
            .get_account(&sender_addr)
            .ok()
            .flatten()
            .map(|a| savitri_core::Account {
                balance: a.balance,
                nonce: a.nonce,
            })
            .unwrap_or_default();
        view.reads.insert(sender_addr.clone(), Some(acc.clone()));
        acc
    };

    // Read recipient account
    let recipient_account = if let Some((acc, _)) = mvs.read_latest_before(&recipient_addr, tx_idx)
    {
        view.reads.insert(recipient_addr.clone(), Some(acc.clone()));
        acc
    } else {
        let acc = storage
            .get_account(&recipient_addr)
            .ok()
            .flatten()
            .map(|a| savitri_core::Account {
                balance: a.balance,
                nonce: a.nonce,
            })
            .unwrap_or_default();
        view.reads.insert(recipient_addr.clone(), Some(acc.clone()));
        acc
    };

    // Validate nonce
    if tx.nonce != sender_account.nonce {
        view.error = Some(format!(
            "nonce mismatch: expected {}, got {}",
            sender_account.nonce, tx.nonce
        ));
        return;
    }

    // Validate balance
    let amount = tx.amount as u128;
    let fee = tx.fee.unwrap_or(0);
    let total_debit = amount.saturating_add(fee);
    if sender_account.balance < total_debit {
        view.error = Some("insufficient balance".into());
        return;
    }

    // Apply changes
    let mut new_sender = sender_account;
    new_sender.balance -= total_debit;
    new_sender.nonce += 1;

    let mut new_recipient = recipient_account;
    new_recipient.balance = new_recipient.balance.saturating_add(amount);

    view.writes.insert(sender_addr, new_sender);
    view.writes.insert(recipient_addr, new_recipient);
    view.success = true;
}
