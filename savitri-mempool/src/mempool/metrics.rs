//! Mempool Metrics for Prometheus monitoring
//!
//! This module provides Prometheus metrics for mempool operations including
//! size, pending/ready transaction counts, admission/rejection/eviction rates.
//!
//! Strangler pattern — adds drain/shard-filter observation API on top of the
//! existing static AtomicU64 counters. New API emits Prometheus metric directly
//! without local atomics; the legacy atomics remain for backward compat with
//! `get_*` accessors and will be sunset in a follow-up phase.

use std::sync::atomic::{AtomicU64, Ordering};

// Atomic counters for mempool statistics
static MEMPOOL_SIZE: AtomicU64 = AtomicU64::new(0);
static MEMPOOL_SIZE_BYTES: AtomicU64 = AtomicU64::new(0);
static PENDING_TX_COUNT: AtomicU64 = AtomicU64::new(0);
static READY_TX_COUNT: AtomicU64 = AtomicU64::new(0);
static ADMISSIONS: AtomicU64 = AtomicU64::new(0);
static REJECTIONS: AtomicU64 = AtomicU64::new(0);
static EVICTIONS: AtomicU64 = AtomicU64::new(0);
static REMOVALS: AtomicU64 = AtomicU64::new(0);
static CONFIRMED: AtomicU64 = AtomicU64::new(0);

/// Update mempool size
pub fn set_mempool_size(size: u64) {
    MEMPOOL_SIZE.store(size, Ordering::Relaxed);
    metrics::gauge!("mempool_size").set(size as f64);
}

/// Get current mempool size
pub fn get_mempool_size() -> u64 {
    MEMPOOL_SIZE.load(Ordering::Relaxed)
}

/// Update pending transaction count
pub fn set_pending_tx_count(count: u64) {
    PENDING_TX_COUNT.store(count, Ordering::Relaxed);
    metrics::gauge!("mempool_pending_tx_count").set(count as f64);
}

/// Get current pending transaction count
pub fn get_pending_tx_count() -> u64 {
    PENDING_TX_COUNT.load(Ordering::Relaxed)
}

/// Update ready transaction count
pub fn set_ready_tx_count(count: u64) {
    READY_TX_COUNT.store(count, Ordering::Relaxed);
    metrics::gauge!("mempool_ready_tx_count").set(count as f64);
}

/// Get current ready transaction count
pub fn get_ready_tx_count() -> u64 {
    READY_TX_COUNT.load(Ordering::Relaxed)
}

/// Increment admission counter
pub fn increment_admission() {
    let _count = ADMISSIONS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::counter!("mempool_admission_total").increment(1);
    update_admission_rate();
}

/// Increment rejection counter
pub fn increment_rejection() {
    let _count = REJECTIONS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::counter!("mempool_rejection_total").increment(1);
    update_admission_rate();
}

/// Update admission rate gauge (percentage of admitted transactions)
fn update_admission_rate() {
    let admissions = ADMISSIONS.load(Ordering::Relaxed);
    let rejections = REJECTIONS.load(Ordering::Relaxed);
    let total = admissions + rejections;

    if total > 0 {
        let admission_rate = (admissions as f64 / total as f64) * 100.0;
        metrics::gauge!("mempool_admission_rate").set(admission_rate);

        let rejection_rate = (rejections as f64 / total as f64) * 100.0;
        metrics::gauge!("mempool_rejection_rate").set(rejection_rate);
    }
}

/// Get current admission rate (0.0-100.0)
pub fn get_admission_rate() -> f64 {
    let admissions = ADMISSIONS.load(Ordering::Relaxed);
    let rejections = REJECTIONS.load(Ordering::Relaxed);
    let total = admissions + rejections;

    if total > 0 {
        (admissions as f64 / total as f64) * 100.0
    } else {
        0.0
    }
}

/// Get cumulative admitted transaction count.
pub fn get_admission_count() -> u64 {
    ADMISSIONS.load(Ordering::Relaxed)
}

/// Get current rejection rate (0.0-100.0)
pub fn get_rejection_rate() -> f64 {
    let admissions = ADMISSIONS.load(Ordering::Relaxed);
    let rejections = REJECTIONS.load(Ordering::Relaxed);
    let total = admissions + rejections;

    if total > 0 {
        (rejections as f64 / total as f64) * 100.0
    } else {
        0.0
    }
}

/// Get cumulative rejected transaction count.
pub fn get_rejection_count() -> u64 {
    REJECTIONS.load(Ordering::Relaxed)
}

/// Update mempool size in bytes
pub fn set_mempool_size_bytes(size_bytes: u64) {
    MEMPOOL_SIZE_BYTES.store(size_bytes, Ordering::Relaxed);
    metrics::gauge!("mempool_size_bytes").set(size_bytes as f64);
}

/// Get current mempool size in bytes
pub fn get_mempool_size_bytes() -> u64 {
    MEMPOOL_SIZE_BYTES.load(Ordering::Relaxed)
}

/// Increment removal counter (transactions removed for block production)
pub fn increment_removal() {
    let _count = REMOVALS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::counter!("mempool_transactions_removed_total").increment(1);
}

/// Increment removal counter by a batch amount
pub fn increment_removal_batch(count: u64) {
    REMOVALS.fetch_add(count, Ordering::Relaxed);
    metrics::counter!("mempool_transactions_removed_total").increment(count);
}

/// Increment eviction counter
pub fn increment_eviction() {
    let count = EVICTIONS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::counter!("mempool_eviction_total").increment(1);
    metrics::gauge!("mempool_eviction_rate").set(count as f64);
}

/// Get current eviction count
pub fn get_eviction_count() -> u64 {
    EVICTIONS.load(Ordering::Relaxed)
}

/// Get cumulative removal count
pub fn get_removal_count() -> u64 {
    REMOVALS.load(Ordering::Relaxed)
}

/// Increment confirmed counter (TX included in a committed block).
/// Called from `on_block_committed` with the number of handles removed
/// because they were actually included in the certified block. Distinct
/// from `removed_total`, which also covers drain, TTL eviction and
/// restore-paths.
pub fn increment_confirmed_batch(count: u64) {
    CONFIRMED.fetch_add(count, Ordering::Relaxed);
    metrics::counter!("mempool_transactions_confirmed_total").increment(count);
}

/// Get cumulative confirmed count.
pub fn get_confirmed_count() -> u64 {
    CONFIRMED.load(Ordering::Relaxed)
}

/// Update all mempool metrics at once (convenience function)
pub fn update_mempool_metrics(size: u64, pending: u64, ready: u64) {
    set_mempool_size(size);
    set_pending_tx_count(pending);
    set_ready_tx_count(ready);
}

// Replaces ad-hoc `static AtomicU64 DRAIN_CTR/FILTER_CTR/NOFILTER_CTR` with
// Prometheus-native counters/histograms emitted directly via metrics:: macros.

/// Observe a drain operation: caller asked for `request` TXs, the pool had
/// `total_before` available, the drain yielded `fair_out` (after shard-filter
/// + nonce-gap policy + fairness rotation).
///
/// Tier 8 replacement for `static DRAIN_CTR: AtomicU64` in
/// `savitri-mempool/src/mempool/integration.rs:1309` (DIAG[#50][drain]).
pub fn observe_drain(request: usize, total_before: usize, fair_out: usize) {
    metrics::histogram!("mempool_drain_request").record(request as f64);
    metrics::histogram!("mempool_drain_total_before").record(total_before as f64);
    metrics::histogram!("mempool_drain_yield").record(fair_out as f64);

    // counters so an external alert can fire when admit_total > 0 but
    // drain_called_total stays flat (== proposer not running) OR when
    // drain_called >> drain_non_empty (== proposer drains an empty mempool
    // even though TX are admitted, which was the 99.9% eviction symptom we
    // tracked in memory tx_broadcast_empty_blocks.md). Gives a single-line
    // Prometheus-side health check for the entire drain → block-prod path
    // without needing to parse log lines.
    metrics::counter!("mempool_drain_called_total").increment(1);
    if fair_out > 0 {
        metrics::counter!("mempool_drain_non_empty_total").increment(1);
        metrics::counter!("mempool_drain_yielded_tx_total").increment(fair_out as u64);
    } else {
        metrics::counter!("mempool_drain_empty_total").increment(1);
    }
}

/// Observe shard-filter breakdown of one drain pass.
///
/// Tier 8 replacement for `static FILTER_CTR: AtomicU64` in
/// `savitri-mempool/src/mempool/integration.rs:1375` (DIAG[#50][filter]).
pub fn observe_shard_filter(kept_rpc: usize, kept_local: usize, dropped_remote: usize) {
    metrics::counter!("mempool_drain_kept_total", "via" => "rpc_accepted")
        .increment(kept_rpc as u64);
    metrics::counter!("mempool_drain_kept_total", "via" => "is_local").increment(kept_local as u64);
    metrics::counter!("mempool_drain_dropped_remote_total").increment(dropped_remote as u64);
}

/// Increment counter for drains executed without a shard-filter (force-include path).
///
/// Tier 8 replacement for `static NOFILTER_CTR: AtomicU64` in
/// `savitri-mempool/src/mempool/integration.rs:1394` (DIAG[#50][nofilter]).
pub fn inc_drain_no_filter() {
    metrics::counter!("mempool_drain_no_filter_total").increment(1);
}
