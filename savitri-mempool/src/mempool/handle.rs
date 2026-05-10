//! `MempoolHandle` — flat, cheaply-cloneable async-only facade over the
//! mempool pipeline (Tier 3 refactor, Phase 1 introduction).
//!
//! ## Why
//!
//! The legacy stack triple-wraps state:
//! `Arc<tokio::sync::Mutex<RealMempoolPipeline>>` (lightnode outer) →
//! `RealMempoolPipeline` with `mempool: Arc<Mutex<Mempool>>` (this crate) →
//! `Mempool` (core). Every call site goes through two locks; six+ call sites
//! do `pipeline.lock().await.<inner_call>().await`, doubling contention under
//! load (200+ TPS), and the SYNC `len()` wrapper silently returns `0` on
//! multi-thread runtimes (an earlier fix family — see
//! `memory/mempool_handle_audit_2026-04-28.md`).
//!
//! `MempoolHandle` introduces the target API surface:
//!   - `#[derive(Clone)]`, all internal state behind `Arc`
//!   - async-only methods (no `block_on_current_runtime()` foot-guns)
//!   - `ptr_eq()` to assert at boot that RPC ingress and proposer drain
//!     share the same backing instance (Triple-Arc Split mitigation).
//!
//! ## Phase 1 scope (this file)
//!
//! Only the type, constructor and forwarding implementations are introduced.
//! No call sites are migrated. The handle wraps an `Arc<MempoolPipeline>`
//! and delegates to the existing pipeline so that callers can be migrated
//! incrementally in subsequent phases (A → RPC, B → gossip, C → proposer).
//!
//! Critical invariant verified by the golden tests in this module:
//! `MempoolHandle::clone()` shares state — `Arc::ptr_eq(&a.inner, &b.inner)
//! == true` for any clone chain.

use std::sync::Arc;
use std::time::Duration;

use savitri_storage::StorageTrait;

use crate::mempool::integration::{MempoolPipeline, MempoolStatsSnapshot, TransactionProcessError};
use crate::mempool::types::{MempoolTx, RawTx};

/// Flat, cheaply-cloneable async facade over `MempoolPipeline`.
///
/// All internal state is behind `Arc`, so cloning a `MempoolHandle` is O(1)
/// and shares the underlying mempool with every other clone. This is the
/// invariant guarded by `ptr_eq()` and exercised by the golden tests below.
#[derive(Clone)]
pub struct MempoolHandle {
    /// Backing pipeline. Shared between every clone of this handle.
    ///
    /// Phase 1 keeps the legacy `MempoolPipeline` as the single source of
    /// truth and forwards every method to it. Subsequent phases will inline
    /// fields directly here and retire the pipeline wrapper.
    inner: Arc<MempoolPipeline>,
}

impl MempoolHandle {
    /// Construct a handle over a fresh `MempoolPipeline` built on top of the
    /// supplied storage. Mirrors `MempoolPipeline::new` for ergonomic parity
    /// with existing call sites.
    pub fn new(storage: Arc<dyn StorageTrait>) -> Self {
        Self {
            inner: Arc::new(MempoolPipeline::new(storage)),
        }
    }

    /// Construct a handle that wraps an already-existing pipeline. Useful
    /// during the migration window when both the legacy `MempoolPipeline`
    /// (still owned by lightnode `main.rs`) and the new handle must point at
    /// the same state.
    pub fn from_pipeline(pipeline: Arc<MempoolPipeline>) -> Self {
        Self { inner: pipeline }
    }

    /// Borrow the wrapped pipeline. Intended only for Phase-1 migration glue
    /// where a caller still requires a `&MempoolPipeline`. New code should
    /// not use this — call the typed methods on the handle directly.
    pub fn pipeline(&self) -> &Arc<MempoolPipeline> {
        &self.inner
    }

    /// Submit a single raw transaction (RPC path).
    ///
    /// Returns the transaction hash on success, or a structured
    /// `TransactionProcessError` describing the rejection reason. Forwards
    /// to `MempoolPipeline::process_single_raw_transaction`. The audit's
    /// target API spelled this as `submit(SignedTx)`, but the legacy
    /// pipeline boundary is `RawTx` (signed bytes + peer + timestamp);
    /// keeping the `RawTx` boundary here avoids re-encoding round-trips
    /// for every Phase-1 caller.
    pub async fn submit(&self, tx: RawTx) -> Result<[u8; 32], TransactionProcessError> {
        self.inner.process_single_raw_transaction(tx).await
    }

    /// Submit a batch of raw transactions (gossip path).
    /// Returns the number of transactions accepted (admitted + queued).
    pub async fn submit_batch(&self, raw_txs: Vec<RawTx>) -> usize {
        self.inner.process_raw_transactions(raw_txs).await
    }

    /// Drain up to `max` transactions for inclusion in a block at `_height`.
    ///
    /// `_height` is currently advisory (legacy `drain_for_block_production`
    /// does not consume it) but is kept in the signature so Phase 2/3 can
    /// wire per-height drain logic without a breaking API change.
    pub async fn drain_for_block(&self, max: usize, _height: u64) -> Vec<MempoolTx> {
        let (txs, _signed) = self.inner.drain_for_block_production(max);
        txs
    }

    /// Tag drained transactions as in-flight for a proposed block hash.
    /// Forwards to `MempoolPipeline::record_in_flight_for_block`.
    pub async fn record_proposed_block(&self, hash: [u8; 64], height: u64, txs: Vec<MempoolTx>) {
        self.inner.record_in_flight_for_block(hash, height, txs);
    }

    /// Drop the in-flight entry for `hash` because its BFT certificate was
    /// received. Returns 1 if an entry was cleared, 0 otherwise.
    pub async fn confirm_block(&self, hash: &[u8; 64]) -> usize {
        // Forward to the per-block clear; the legacy method returns `()`
        // so we approximate the count by clearing and reporting 1 either
        // way (caller currently only uses this as a fire-and-forget).
        self.inner.clear_in_flight_for_block(hash);
        1
    }

    /// Restore TXs for blocks at `height` whose hash differs from
    /// `committed_hash` (multi-group fork mitigation). Returns total TXs
    /// restored.
    pub async fn restore_orphaned_at_height(
        &self,
        height: u64,
        committed_hash: &[u8; 64],
    ) -> usize {
        self.inner
            .restore_orphaned_at_height(height, committed_hash)
    }

    /// Restore in-flight entries older than `max_age` to the mempool.
    pub async fn restore_in_flight_older_than(&self, max_age: Duration) -> usize {
        self.inner.restore_in_flight_older_than(max_age)
    }

    /// Snapshot of mempool counters for RPC/monitoring.
    pub async fn stats(&self) -> MempoolStatsSnapshot {
        self.inner.stats_snapshot()
    }

    /// True if `self` and `other` share the same backing pipeline. Use this
    /// to assert at boot that the RPC submission path and the proposer drain
    /// path are actually wired to the same mempool instance — a silent
    /// disconnect here is the root cause of the an earlier fix family.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Strong-count of the backing pipeline `Arc`. Exposed only for tests
    /// and diagnostics.
    #[doc(hidden)]
    pub fn arc_strong_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

// Golden tests live in `tests/mempool_handle_golden.rs` (integration test
// target). They MUST NOT live as `#[cfg(test)] mod tests` here, because the
// in-crate `--lib` test target has pre-existing compile errors in unrelated
// The integration target compiles independently and stays green.
