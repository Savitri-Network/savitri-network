use super::{RocksDb, Storage};
use crate::core::block::Block;
use crate::core::crypto::compute_tx_root;
use crate::core::types::Account;
use hex;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

//
// `commit_overlay_and_block*` historically calls `batch.commit()` which
// writes the WriteBatch with default WriteOptions (WAL on, sync false).
// Under the loadtest cluster the chain commit shows up as a sustained
// fsync-bound bottleneck on the proposer LN: every block (~1/s per group
// proposer) triggers a WAL append + sync barrier inside RocksDB even
// though the rest of the consensus pipeline doesn't need crash recovery
// at sub-block granularity (the certificate quorum is the durable record).
//
// `DbBatch::commit_batch_optimized` already exists and disables WAL +
// async sync + no_slowdown. Wire it in via env so we can A/B-test on
// the live cluster without touching the call sites:
//
//   * SAVITRI_STORAGE_COMMIT_OPTIMIZED=1 -> use commit_batch_optimized
//   * default (unset/0)                  -> legacy commit() (durable WAL)
//
// We cache the env lookup in an atomic to avoid hitting std::env on every
// block commit (env::var allocates and locks the global env table).
fn commit_optimized_enabled() -> bool {
    // 0 = unread, 1 = false, 2 = true
    static CACHED: AtomicU8 = AtomicU8::new(0);
    match CACHED.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var("SAVITRI_STORAGE_COMMIT_OPTIMIZED")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            CACHED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            // log once
            static LOGGED: AtomicBool = AtomicBool::new(false);
            if !LOGGED.swap(true, Ordering::Relaxed) {
            }
            on
        }
    }
}

fn commit_batch_or_optimized(batch: super::StorageBatch) -> anyhow::Result<()> {
    if commit_optimized_enabled() {
        batch.commit_batch_optimized()
    } else {
        batch.commit()
    }
}

fn write_tx_indexes(
    batch: &mut super::StorageBatch,
    block_height: u64,
    account_history: &[(Vec<u8>, [u8; 64])],
) -> anyhow::Result<()> {
    use crate::storage::CF_META;

    let mut seen_account_entries: HashSet<(Vec<u8>, [u8; 64])> = HashSet::new();
    let mut seen_tx_hashes: HashSet<[u8; 64]> = HashSet::new();

    for (account, tx_hash) in account_history.iter() {
        if seen_account_entries.insert((account.clone(), *tx_hash)) {
            let mut key = b"account_history::".to_vec();
            key.extend_from_slice(account);
            key.push(b':');
            key.extend_from_slice(&block_height.to_be_bytes());
            key.push(b':');
            key.extend_from_slice(tx_hash);
            batch.put_cf(CF_META, key, &[])?;
        }

        if seen_tx_hashes.insert(*tx_hash) {
            let mut by_block_key = b"block_txs::".to_vec();
            by_block_key.extend_from_slice(&block_height.to_be_bytes());
            by_block_key.push(b':');
            by_block_key.extend_from_slice(tx_hash);
            batch.put_cf(CF_META, by_block_key, &[])?;

            let mut inclusion_key = b"tx_height:".to_vec();
            inclusion_key.extend_from_slice(tx_hash);
            batch.put_cf(CF_META, inclusion_key, block_height.to_le_bytes())?;
        }
    }

    Ok(())
}

impl Storage<RocksDb> {
    // Atomically commit overlayed accounts, receipts, and block with overlay-computed state_root
    pub fn commit_overlay_and_block(
        &self,
        overlay: &std::collections::BTreeMap<Vec<u8>, Account>,
        receipts: &[(Vec<u8>, Vec<u8>)],
        account_history: &[(Vec<u8>, [u8; 64])],
        mut block: Block,
    ) -> anyhow::Result<()> {
        // Compute roots and header hash (binds parents and state)
        block.tx_root = compute_tx_root(&block.transactions);
        block.state_root = self.compute_state_root_overlay(overlay)?;
        block.hash = block.header_hash();

        // Replay safety: if this block already exists (by hash) or height->hash already matches, skip writes
        if (self.get_block(&block.hash)?).is_some() {
            return Ok(());
        }
        if let Some(existing) = self.get_block_hash_by_height(block.height)? {
            if existing == block.hash {
                return Ok(());
            } else {
                anyhow::bail!("height {} already mapped to a different hash", block.height);
            }
        }

        // Prepare batch
        let mut batch = self.begin_batch();
        // Apply overlay as puts/deletes with empty-account deletion rule
        for (k, acc) in overlay.iter() {
            if *acc == Account::default() {
                batch.delete_account(k)?;
            } else {
                batch.put_account(k, acc)?;
            }
        }
        // Receipts
        for (k, v) in receipts.iter() {
            batch.put_receipt_bytes(k, v)?;
        }
        // Account history + block transaction indexes
        write_tx_indexes(&mut batch, block.height, account_history)?;
        // Block commit with freshly computed hash
        batch.put_block(&block)?;
        // Update chain head
        batch.set_chain_head(block.height, &block.hash)?;
        // Record height -> hash mapping
        batch.set_block_hash_for_height(block.height, &block.hash)?;
        commit_batch_or_optimized(batch)?;
        Ok(())
    }

    // Atomically commit overlayed accounts, receipts, and block using predefined state_root and tx_root
    // This is used when committing blocks received from other nodes that already have computed roots
    pub fn commit_overlay_and_block_with_roots(
        &self,
        overlay: &std::collections::BTreeMap<Vec<u8>, Account>,
        receipts: &[(Vec<u8>, Vec<u8>)],
        account_history: &[(Vec<u8>, [u8; 64])],
        block: Block,
    ) -> anyhow::Result<()> {
        // Strong verification: verify state_root matches overlay computation
        let computed_state_root = self.compute_state_root_overlay(overlay)?;
        if block.state_root != computed_state_root {
            anyhow::bail!(
                "state_root mismatch: expected {} got {}",
                hex::encode(computed_state_root),
                hex::encode(block.state_root)
            );
        }

        // Strong verification: verify tx_root matches transactions
        let computed_tx_root = compute_tx_root(&block.transactions);
        if block.tx_root != computed_tx_root {
            anyhow::bail!(
                "tx_root mismatch: expected {} got {}",
                hex::encode(computed_tx_root),
                hex::encode(block.tx_root)
            );
        }

        // Verify hash matches (sanity check - must match after root verification)
        let expected_hash = block.header_hash();
        if block.hash != expected_hash {
            anyhow::bail!(
                "block hash mismatch: expected {} got {}",
                hex::encode(expected_hash),
                hex::encode(block.hash)
            );
        }

        // Replay safety: if this block already exists (by hash) or height->hash already matches, skip writes
        if (self.get_block(&block.hash)?).is_some() {
            return Ok(());
        }
        if let Some(existing) = self.get_block_hash_by_height(block.height)? {
            if existing == block.hash {
                return Ok(());
            } else {
                anyhow::bail!("height {} already mapped to a different hash", block.height);
            }
        }

        // Prepare batch
        let mut batch = self.begin_batch();
        // Apply overlay as puts/deletes with empty-account deletion rule
        for (k, acc) in overlay.iter() {
            if *acc == Account::default() {
                batch.delete_account(k)?;
            } else {
                batch.put_account(k, acc)?;
            }
        }
        // Receipts
        for (k, v) in receipts.iter() {
            batch.put_receipt_bytes(k, v)?;
        }
        // Account history + block transaction indexes
        write_tx_indexes(&mut batch, block.height, account_history)?;
        // Block commit with predefined hash
        batch.put_block(&block)?;
        // Update chain head
        batch.set_chain_head(block.height, &block.hash)?;
        // Record height -> hash mapping
        batch.set_block_hash_for_height(block.height, &block.hash)?;
        commit_batch_or_optimized(batch)?;
        Ok(())
    }
}
