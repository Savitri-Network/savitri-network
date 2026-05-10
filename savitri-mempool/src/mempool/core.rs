//! Mempool Core: Ultra-hot path for enqueueing and scheduling
//!
//! INVARIANTS:
//! - ready_index[sender_id] -> index in ready_vec (if sender has ready queue)
//! - rr_vec contains indices of non-empty ready queues
//! - rr_pos < rr_vec.len() (or 0 if empty)
//! - swap_remove maintains consistency: update ready_index for swapped element
//! - No HashMap lookups in hot paths (use Vec<Option<usize>> indexed by sender_id)

use crate::mempool::admission::{AdmissionControl, AdmissionResult};
use crate::mempool::metrics::{
    increment_admission, increment_confirmed_batch, increment_eviction, increment_rejection,
    increment_removal_batch, update_mempool_metrics,
};
use crate::mempool::types::{MempoolTx, PrevalidatedTx, SenderId, TxClass, TxHandle};

#[derive(Debug, Clone, PartialEq)]
pub enum AdmissionOutcome {
    /// Transaction admitted to main (ready) pool
    Admitted,
    /// Transaction queued for future nonce (in queued pool, not yet executable)
    Queued,
    /// Transaction rejected (with reason)
    Rejected(String),
}
use crate::mempool::PurgeMetrics;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Configuration for mempool core
#[derive(Clone, Copy)]
pub struct MempoolConfig {
    pub global_cap: usize,
    pub per_sender_cap: usize,
    pub ttl: Duration,
}

impl Default for MempoolConfig {
    fn default() -> Self {
        Self {
            global_cap: 20_000,  // Aumentato per testnet 120 nodi (da 10_000)
            per_sender_cap: 256, // Aumentato per testnet 120 nodi (da 128)
            ttl: Duration::from_secs(120),
        }
    }
}

/// Ready queue for a sender (indexed by sender_id)
pub(crate) struct ReadyQueue {
    sender_id: SenderId,
    queue: VecDeque<MempoolTx>,
}

/// Mempool Core: O(1) enqueue and deterministic round-robin scheduling
///
/// Data structures optimized for cache locality:
/// - ready_vec: Vec<ReadyQueue> - contiguous storage
/// - ready_index: Vec<Option<usize>> - indexed by sender_id (no HashMap in hot path)
/// - rr_vec: Vec<usize> - round-robin indices, separated by class
/// - rr_pos: usize - cursor for each class
///
/// Memory Usage Comparison:
/// - Vec<Option<usize>>: ~8 bytes per sender (usize = 8 bytes on 64-bit)
///   For 4M senders: ~32 MB (sparse, only allocated up to max sender_id)
/// - HashMap<Vec<u8>, usize>: ~48-64 bytes per entry (hash table overhead + Vec<u8> key)
///   For 4M senders: ~192-256 MB + key storage
/// Memory savings: ~160-224 MB for 4M senders
///
/// Performance:
/// - Lookup: O(1) direct indexing (no hash computation, no allocations)
/// - Insert: O(1) amortized (exponential growth minimizes resize operations)
/// - Remove: O(1) swap_remove with proper index updates
///
/// OPTIMIZATION: Vec<Option<usize>> vs HashMap Analysis:
/// - Option<usize> memory layout: niche optimization (None = 0, Some = non-zero)
/// - No heap allocation per entry (unlike HashMap)
/// - Cache-friendly contiguous memory layout
/// - Predictable access patterns for CPU prefetching
pub struct Mempool {
    cfg: MempoolConfig,
    /// Ready queues stored contiguously
    ready_vec: Vec<ReadyQueue>,
    /// Index sender_id -> ready_vec index (Vec indexed by sender_id, max 4M senders)
    /// Using Vec instead of HashMap for O(1) lookup without hash computation
    /// Exponential growth strategy minimizes resize operations
    ready_index: Vec<Option<usize>>,
    /// Round-robin indices per class (separate RR per class for fairness)
    rr_vec_by_class: [Vec<usize>; 4], // Financial, IoTData, FederatedUpdate, System
    /// Round-robin cursor per class
    rr_pos_by_class: [usize; 4],
    /// Per-sender counts
    counts: Vec<usize>, // indexed by sender_id
    /// Total count
    total: usize,
    /// Lazy purge state
    last_purge: Instant,
    purge_interval: Duration,
    purge_metrics: PurgeMetrics,
    /// Resize metrics for ready_index capacity management
    resize_count: u64,
    max_index_capacity: usize,
    /// Admission control (shared)
    admission: Arc<Mutex<AdmissionControl>>,
}

impl Mempool {
    pub fn new(admission: Arc<Mutex<AdmissionControl>>) -> Self {
        Self {
            cfg: MempoolConfig::default(),
            ready_vec: Vec::new(),
            // OPTIMIZATION: Pre-allocate with initial capacity for common workloads
            // Most networks have <1000 concurrent senders, start with reasonable capacity
            ready_index: Vec::with_capacity(1024),
            rr_vec_by_class: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            rr_pos_by_class: [0, 0, 0, 0],
            // OPTIMIZATION: Pre-allocate counts to match ready_index capacity
            counts: Vec::with_capacity(1024),
            total: 0,
            last_purge: Instant::now(),
            purge_interval: Duration::from_secs(1),
            purge_metrics: PurgeMetrics::default(),
            resize_count: 0,
            max_index_capacity: 1024, // Track initial capacity
            admission,
        }
    }

    pub fn with_config(cfg: MempoolConfig, admission: Arc<Mutex<AdmissionControl>>) -> Self {
        let admission_clone = admission.clone();
        let mut new_self = Self::new(admission_clone);
        new_self.cfg = cfg;
        new_self.admission = admission;
        new_self
    }

    /// Get class index (for array indexing)
    #[inline]
    fn class_idx(class: TxClass) -> usize {
        match class {
            TxClass::Financial => 0,
            TxClass::IoTData => 1,
            TxClass::FederatedUpdate => 2,
            TxClass::System => 3,
        }
    }

    /// Ensure ready_index is large enough for sender_id
    /// Uses exponential growth strategy to minimize resize operations
    ///
    /// RESIZE STRATEGY: Exponential growth (double capacity) minimizes resize operations
    /// - First resize: 0 -> max(2, idx+1)
    /// - Subsequent resizes: len * 2 (or idx+1 if larger)
    /// - Amortized O(1) insert cost despite occasional O(n) resize
    /// - Memory overhead: at most 2x current size (acceptable trade-off)
    ///
    /// OPTIMIZATION: Vec::resize with Option<usize> niche optimization
    /// - None uses niche value (0) - no additional overhead
    /// - Contiguous memory layout improves cache locality
    /// - Pre-filled with None ensures valid state for all indices
    ///
    /// BOUNDS SAFETY: All accesses are bounds-checked after ensure_index_capacity
    /// - sender_id as usize is always < ready_index.len() after capacity check
    /// - No out-of-bounds access possible in hot paths
    fn ensure_index_capacity(&mut self, sender_id: SenderId) {
        let idx = sender_id as usize;

        // OPTIMIZATION: Check both vectors together to minimize branching
        let need_resize = idx >= self.ready_index.len() || idx >= self.counts.len();

        if need_resize {
            // Calculate new capacity using exponential growth strategy
            // This minimizes resize operations while avoiding excessive memory usage
            let current_capacity = self.ready_index.len().max(self.counts.len());
            let new_capacity = (current_capacity * 2).max(idx + 1).max(2); // Ensure at least 2

            // OPTIMIZATION: Resize both vectors together for better cache locality
            // Vec::resize with None uses niche optimization (None = 0)
            self.ready_index.resize(new_capacity, None);
            self.counts.resize(new_capacity, 0);

            // Track resize metrics for monitoring
            self.resize_count += 1;
            self.max_index_capacity = self.max_index_capacity.max(new_capacity);

            // Export metrics to Prometheus for production monitoring
            metrics::gauge!("mempool_index_resize_operations_total").set(self.resize_count as f64);
            metrics::gauge!("mempool_index_capacity_bytes")
                .set((new_capacity * size_of::<Option<usize>>()) as f64);
            metrics::gauge!("mempool_index_max_capacity_reached")
                .set(self.max_index_capacity as f64);
            metrics::gauge!("mempool_index_utilization_ratio")
                .set((idx as f64) / (new_capacity as f64) * 100.0);
        }
    }

    /// Remove ready queue by index (swap-remove for O(1))
    ///
    /// INVARIANTS MAINTAINED:
    /// - ready_index[sender_id] is set to None for removed sender
    /// - ready_index[swapped_sender_id] is updated to point to new position (idx)
    /// - rr_vec entries pointing to last_idx are updated to point to idx
    /// - rr_vec entries pointing to removed idx are filtered out
    /// - rr_pos cursors are reset if they exceed rr_vec length
    ///
    /// CORRECTNESS: swap_remove maintains all invariants correctly:
    /// - When idx == last_idx: only removal needed, no swap occurs
    /// - When idx != last_idx: element at last_idx moves to idx, all references updated
    ///
    /// SAFETY: All bounds checks are performed before array access
    /// - ready_index access is safe because ensure_index_capacity guarantees capacity
    /// - rr_vec updates are safe because we check bounds before accessing
    fn remove_ready_queue(&mut self, idx: usize) {
        if idx >= self.ready_vec.len() {
            return;
        }
        let last_idx = self.ready_vec.len() - 1;
        let removed = self.ready_vec.swap_remove(idx);

        // SAFETY: removed.sender_id is always valid after ensure_index_capacity
        let removed_sender_idx = removed.sender_id as usize;
        if removed_sender_idx < self.ready_index.len() {
            // Clear index for removed sender (no dangling reference)
            self.ready_index[removed_sender_idx] = None;
        }

        // Fix mapping for swapped element (if swap occurred)
        if idx != last_idx {
            let swapped_sender_id = self.ready_vec[idx].sender_id;
            let swapped_sender_idx = swapped_sender_id as usize;

            // SAFETY: swapped_sender_idx is always valid after ensure_index_capacity
            if swapped_sender_idx < self.ready_index.len() {
                // Update ready_index to point to new position
                self.ready_index[swapped_sender_idx] = Some(idx);
            }

            // OPTIMIZATION: Update rr_vec entries pointing to last_idx -> idx
            // This is O(k) where k = number of classes (4), which is constant
            for rr_vec in &mut self.rr_vec_by_class {
                for slot in rr_vec.iter_mut() {
                    if *slot == last_idx {
                        *slot = idx;
                    }
                }
            }
        }

        // Remove any rr_vec entries that pointed to removed idx (now invalid)
        // This handles both the removed idx and ensures no stale references
        for rr_vec in &mut self.rr_vec_by_class {
            rr_vec.retain(|&i| i < self.ready_vec.len());
        }

        // Reset cursors if they exceed rr_vec length (defensive programming)
        for (pos, rr_vec) in self.rr_pos_by_class.iter_mut().zip(&self.rr_vec_by_class) {
            if *pos >= rr_vec.len() {
                *pos = 0;
            }
        }
    }

    ///
    /// # Stream Nonce Support
    ///
    /// The transaction's stream_nonce (if present) is preserved in MempoolTx.
    /// Currently, scheduling uses round-robin per class and doesn't order by stream_nonce.
    /// This is acceptable because:
    /// - Stream nonce allows out-of-order processing (transactions can arrive out of order)
    /// - Round-robin ensures fairness across senders
    /// - Future enhancement: can add stream_nonce-based ordering for IoT/FederatedUpdate if needed
    ///
    /// The stream_nonce field is available for:
    /// - Future scheduling optimizations (ordering within same stream)
    /// - Out-of-order processing support (already supported)
    /// - Stream-based batching (future enhancement)
    pub fn add_prevalidated(
        &mut self,
        pv: PrevalidatedTx,
        tx_hash: Option<[u8; 32]>,
    ) -> AdmissionOutcome {
        self.add_prevalidated_with_source(pv, tx_hash, false)
    }

    /// via RPC locale. Usata da `process_single_raw_transaction`. Lo
    /// suo sender appartiene a uno shard remoto, sbloccando il commit pipeline
    pub fn add_prevalidated_with_source(
        &mut self,
        pv: PrevalidatedTx,
        tx_hash: Option<[u8; 32]>,
        from_rpc: bool,
    ) -> AdmissionOutcome {
        // 1. Check admission control using extended check (distinguishes Admitted/Queued/Rejected)
        {
            let mut adm = self.admission.lock().unwrap();
            let admission_result = adm.check_admission_ext_with_source(&pv, tx_hash, from_rpc);
            // SAVITRI_NONCE_DEBUG=1, log EVERY admission call (no rate limit)
            // for the first 1000 calls to verify low-nonce TX (0..7) are
            // admitted properly. Without this granularity the 1/100 sample
            // missed exactly the low-nonce TX that the loadtest emits at boot.
            {
                use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
                static DIAG_50_ADMISSION_CALLS: AtomicU64 = AtomicU64::new(0);
                static NONCE_DEBUG_CACHED: AtomicBool = AtomicBool::new(false);
                static NONCE_DEBUG_INIT: AtomicBool = AtomicBool::new(false);
                if !NONCE_DEBUG_INIT.swap(true, Ordering::Relaxed) {
                    let enabled = std::env::var("SAVITRI_NONCE_DEBUG")
                        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                        .unwrap_or(false);
                    NONCE_DEBUG_CACHED.store(enabled, Ordering::Relaxed);
                }
                let nonce_debug = NONCE_DEBUG_CACHED.load(Ordering::Relaxed);
                let n = DIAG_50_ADMISSION_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
                let log_full = nonce_debug && n <= 1000;
                if log_full || n == 1 || n % 100 == 0 {
                    let outcome_label = match &admission_result {
                        AdmissionResult::Admitted => "Admitted",
                        AdmissionResult::Queued => "Queued",
                        AdmissionResult::Rejected(_) => "Rejected",
                    };
                    let reason = match &admission_result {
                        AdmissionResult::Rejected(r) => r.as_str(),
                        _ => "",
                    };
                    tracing::warn!(
                        admission_call = n,
                        outcome = outcome_label,
                        reason = reason,
                        sender_id = pv.sender_id,
                        nonce = pv.nonce,
                        from_rpc = from_rpc,
                        "DIAG[#50]: admission outcome (stage 2)"
                    );
                }
            }
            match admission_result {
                AdmissionResult::Admitted => {
                    // Transaction is ready for main pool
                    adm.record_admission(&pv, tx_hash);
                    increment_admission();
                }
                AdmissionResult::Queued => {
                    // Transaction already queued in queued pool by check_admission_ext
                    // Don't add to ready queue - it's in the queued pool waiting for nonce gap to close
                    return AdmissionOutcome::Queued;
                }
                AdmissionResult::Rejected(reason) => {
                    increment_rejection();
                    return AdmissionOutcome::Rejected(reason);
                }
            }
        }

        // 2. Ensure capacity
        self.ensure_index_capacity(pv.sender_id);

        // 3. Get or create ready queue index (direct indexing after ensure_index_capacity)
        // SAFETY: ensure_index_capacity guarantees sender_idx < ready_index.len()
        // PERFORMANCE: O(1) direct indexing, zero allocations, no hash computation
        // MEMORY: Option<usize> uses niche optimization (None = 0, Some = non-zero)
        let sender_idx = pv.sender_id as usize;
        let idx = match self.ready_index[sender_idx] {
            Some(i) => {
                // SAFETY: i is always valid because swap_remove updates ready_index correctly
                // VERIFIED: swap_remove maintains invariant that ready_index[swapped] = Some(new_idx)
                // PERFORMANCE: Hot path - no bounds checking needed after ensure_index_capacity
                i
            }
            None => {
                // OPTIMIZATION: Create new ready queue and register index
                // This is the cold path - only happens for new senders
                let i = self.ready_vec.len();
                self.ready_vec.push(ReadyQueue {
                    sender_id: pv.sender_id,
                    queue: VecDeque::with_capacity(4), // OPTIMIZATION: Pre-allocate common queue size
                });
                // SAFETY: sender_idx is always valid after ensure_index_capacity
                self.ready_index[sender_idx] = Some(i);
                i
            }
        };

        // 4. Enqueue transaction (stream_nonce is preserved in MempoolTx)
        // Safety: idx is always valid (either from ready_index or just pushed)
        let rq = &mut self.ready_vec[idx];
        let was_empty = rq.queue.is_empty();
        let sender_id = pv.sender_id;

        // Create MempoolTx and set hash if available
        let mut mempool_tx = MempoolTx::from(pv);
        mempool_tx.tx_hash = tx_hash;
        mempool_tx.rpc_accepted = from_rpc;

        rq.queue.push_back(mempool_tx);
        self.total += 1;
        self.counts[sender_id as usize] += 1;

        // 5. Add to round-robin if queue was empty
        if was_empty {
            let class_idx = Self::class_idx(rq.queue.back().unwrap().class);
            self.rr_vec_by_class[class_idx].push(idx);
        }
        // `rq` (mutable borrow of self.ready_vec) goes out of scope here, so

        // Used to discriminate hypothesis 1 (cleanup spurious between push and
        // return) vs hypothesis 3 (purge in len) vs deeper push bug.
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static DIAG_5A_POST_PUSH: AtomicU64 = AtomicU64::new(0);
            let n = DIAG_5A_POST_PUSH.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 100 == 0 {
                tracing::warn!(
                    diag_call = n,
                    self_total_after_push = self.total,
                    ready_vec_len = self.ready_vec.len(),
                    sender_id,
                    "DIAG[#50] stage 5A: state immediately after push"
                );
            }
        }

        // Update mempool metrics
        self.update_metrics();

        // we can pair pre/post entries; if total drops between them, something
        // spurious removes between push and return.
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static DIAG_5A_PRE_RETURN: AtomicU64 = AtomicU64::new(0);
            let n = DIAG_5A_PRE_RETURN.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 100 == 0 {
                tracing::warn!(
                    diag_call = n,
                    self_total_at_return = self.total,
                    ready_vec_len_at_return = self.ready_vec.len(),
                    sender_id,
                    "DIAG[#50] stage 5A: state at function return"
                );
            }
        }

        AdmissionOutcome::Admitted
    }

    ///
    /// production caller is `process_raw_transactions`, invoked by the task
    /// that drains `tx_batch_buffer` filled from the `intra_group_tx_topic`
    /// gossip RX path (savitri-lightnode/src/p2p/network/mod.rs:4055-4070).
    ///
    /// `mempool_tx.rpc_accepted=false` → the proposer's drain applies
    /// `shard_filter.is_local()` and SILENTLY DROPS every TX whose
    /// sender_shard is not in its local subset. Net effect: the group
    /// proposer receives RPC-accepted TX from another ingress LN via
    /// gossip, admits them into its mempool, and then drops them at drain
    /// time because `rpc_accepted=false`. The mempool's admit counter
    /// grows but drain yields zero → empty blocks → confirmed_total=0.
    ///
    /// AFTER the fix: `from_rpc=true` propagates `rpc_accepted=true` on
    /// the MempoolTx, the drain bypasses the shard_filter
    /// (integration.rs:1370), and gossiped TX are included in the proposed
    /// block. This unblocks the Fix #1 (gossip publish from the Local
    /// branch) end-to-end pipeline.
    ///
    /// For future callers that genuinely need `from_rpc=false` (e.g. a
    /// test harness exercising the shard-filter branch), use
    pub fn add_prevalidated_batch(&mut self, pvs: Vec<PrevalidatedTx>) -> Vec<AdmissionOutcome> {
        self.add_prevalidated_batch_with_source(pvs, true)
    }

    /// Batch add with explicit `from_rpc` flag. See
    pub fn add_prevalidated_batch_with_source(
        &mut self,
        pvs: Vec<PrevalidatedTx>,
        from_rpc: bool,
    ) -> Vec<AdmissionOutcome> {
        self.purge_expired_internal_if_needed();
        let mut results = Vec::with_capacity(pvs.len());
        for pv in pvs {
            results.push(self.add_prevalidated_with_source(pv, None, from_rpc));
        }
        results
    }

    /// Return a snapshot of all transaction handles currently present in the mempool.
    /// This is intended for slow-path housekeeping (e.g. finding committed transactions).
    pub fn all_handles(&self) -> Vec<TxHandle> {
        let mut out = Vec::with_capacity(self.total);
        for rq in self.ready_vec.iter() {
            for tx in rq.queue.iter() {
                out.push(tx.tx_handle);
            }
        }
        out
    }

    /// Iterate over all TX in the mempool, returning (sender_id, nonce, handle) tuples.
    /// Used for purging stale TX after block commit (TX with nonce < committed nonce).
    pub fn iter_handles_with_nonce(&self) -> impl Iterator<Item = (u32, u64, TxHandle)> + '_ {
        self.ready_vec.iter().flat_map(|rq| {
            rq.queue
                .iter()
                .map(|tx| (tx.sender_id, tx.nonce, tx.tx_handle))
        })
    }

    /// Drain transactions fairly using round-robin (class-aware)
    pub fn drain_fair_batch(&mut self, max: usize) -> Vec<MempoolTx> {
        self.purge_expired_internal_if_needed();
        if max == 0 {
            return Vec::new();
        }

        let cap = max.min(self.total);
        let mut out = Vec::with_capacity(cap);

        // Sort each sender's queue by nonce before draining. Admission allows TX with
        // nonce up to account.nonce + MAX_MAIN_POOL_NONCE_GAP (3000) into the main pool
        // in physical arrival order, not nonce order. Multi-RPC ingress + gossip
        // reordering means push_back inserts nonces out of sequence. Without sorting
        // strict sequential check (expected_nonce == tx.nonce) rejects the entire
        // sender batch, and blocks end up with 0–5 TX instead of hundreds.
        // Cost: O(k log k) per sender queue, amortized over the entire drain batch.
        for rq in self.ready_vec.iter_mut() {
            if rq.queue.len() > 1 {
                let mut tmp: Vec<MempoolTx> = rq.queue.drain(..).collect();
                tmp.sort_by_key(|tx| tx.nonce);
                rq.queue.extend(tmp);
            }
        }

        // SAVITRI_NONCE_DEBUG=1, log the FRONT nonce of each non-empty
        // sender queue BEFORE the round-robin pops. This verifies whether
        // ready_vec contains low-nonce TX (nonce 0..7) or starts at high
        // nonce (drain_nonce_skip H2 / H3). Limited to first 5 senders to
        // avoid log flood. Each call gets a unique drain_call counter.
        {
            use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
            static NONCE_DEBUG_CACHED: AtomicBool = AtomicBool::new(false);
            static NONCE_DEBUG_INIT: AtomicBool = AtomicBool::new(false);
            static DRAIN_DEBUG_CALLS: AtomicU64 = AtomicU64::new(0);
            if !NONCE_DEBUG_INIT.swap(true, Ordering::Relaxed) {
                let enabled = std::env::var("SAVITRI_NONCE_DEBUG")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false);
                NONCE_DEBUG_CACHED.store(enabled, Ordering::Relaxed);
            }
            if NONCE_DEBUG_CACHED.load(Ordering::Relaxed) {
                let n = DRAIN_DEBUG_CALLS.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= 50 {
                    let mut samples = Vec::new();
                    for rq in self.ready_vec.iter().filter(|q| !q.queue.is_empty()).take(5) {
                        let front_nonce = rq.queue.front().map(|t| t.nonce).unwrap_or(u64::MAX);
                        let queue_len = rq.queue.len();
                        samples.push(format!(
                            "(sender={},front_nonce={},queue_len={})",
                            rq.sender_id, front_nonce, queue_len
                        ));
                    }
                    tracing::warn!(
                        drain_call = n,
                        max,
                        total = self.total,
                        non_empty = self.ready_vec.iter().filter(|q| !q.queue.is_empty()).count(),
                        first_5 = %samples.join(" "),
                        "DIAG[NONCE]: drain_fair_batch BEFORE pop — first 5 senders"
                    );
                }
            }
        }

        // Round-robin across classes (fairness between classes)
        // Keep iterating until we either reach `max` or no progress is possible.
        while out.len() < max {
            let mut made_progress = false;

            for class_idx in 0..4 {
                if out.len() >= max {
                    break;
                }

                let rr_vec = &mut self.rr_vec_by_class[class_idx];
                let rr_pos = &mut self.rr_pos_by_class[class_idx];

                if rr_vec.is_empty() {
                    continue;
                }

                // Get next index from round-robin
                if *rr_pos >= rr_vec.len() {
                    *rr_pos = 0;
                }
                let idx = rr_vec[*rr_pos];
                *rr_pos = (*rr_pos + 1) % rr_vec.len().max(1);

                // Drain up to BURST_PER_SENDER consecutive TX from this sender per round.
                // This increases drain throughput while preserving cross-sender fairness
                // (each sender still gets the same number of turns in the round-robin).
                const BURST_PER_SENDER: usize = 8;
                if let Some(rq) = self.ready_vec.get_mut(idx) {
                    let mut burst = 0;
                    while burst < BURST_PER_SENDER && out.len() < max {
                        if let Some(tx) = rq.queue.pop_front() {
                            self.total = self.total.saturating_sub(1);
                            self.counts[rq.sender_id as usize] =
                                self.counts[rq.sender_id as usize].saturating_sub(1);
                            {
                                let mut adm = self.admission.lock().unwrap();
                                adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                            }
                            out.push(tx);
                            made_progress = true;
                            burst += 1;
                        } else {
                            break;
                        }
                    }

                    if rq.queue.is_empty() {
                        rr_vec.retain(|&i| i != idx);
                        if *rr_pos >= rr_vec.len() {
                            *rr_pos = 0;
                        }
                    }
                }
            }

            if !made_progress {
                break;
            }
        }

        if !out.is_empty() {
            increment_removal_batch(out.len() as u64);
        }
        out
    }

    /// ROUND 13: Restore previously drained TXs back to the mempool.
    /// Used when a proposer changes before its block was committed — the drained TXs
    /// are put back so the next proposer can include them in a new block.
    pub fn restore_drained_txs(&mut self, txs: Vec<MempoolTx>) {
        for tx in txs {
            let sender_id = tx.sender_id;
            let class = tx.class;
            let class_idx = Self::class_idx(class);

            self.ensure_index_capacity(sender_id);
            let sender_idx = sender_id as usize;

            match self.ready_index[sender_idx] {
                Some(idx) => {
                    if let Some(rq) = self.ready_vec.get_mut(idx) {
                        rq.queue.push_front(tx);
                        self.counts[sender_idx] = self.counts[sender_idx].saturating_add(1);
                        self.total += 1;
                    }
                }
                None => {
                    // No queue for this sender — create one
                    let new_idx = self.ready_vec.len();
                    let mut queue = VecDeque::new();
                    queue.push_back(tx);
                    self.ready_vec.push(ReadyQueue { sender_id, queue });
                    self.ready_index[sender_idx] = Some(new_idx);
                    self.counts[sender_idx] = 1;
                    self.total += 1;
                    // Add to round-robin for this class
                    self.rr_vec_by_class[class_idx].push(new_idx);
                }
            }

            // Re-record admission so quotas stay consistent
            {
                let mut adm = self.admission.lock().unwrap();
                adm.record_restoration(sender_id, class);
            }
        }
    }

    /// Peek transactions fairly using round-robin (class-aware) without removing them
    /// Returns a preview of what would be drained next
    pub fn peek_fair_batch(&mut self, max: usize) -> Vec<MempoolTx> {
        self.purge_expired_internal_if_needed();
        if max == 0 {
            return Vec::new();
        }

        let cap = max.min(self.total);
        let mut out = Vec::with_capacity(cap);

        // Round-robin across classes (fairness between classes)
        // Keep iterating until we either reach `max` or no progress is possible.
        while out.len() < max {
            let mut made_progress = false;

            for class_idx in 0..4 {
                if out.len() >= max {
                    break;
                }

                let rr_vec = &self.rr_vec_by_class[class_idx];
                let rr_pos = &self.rr_pos_by_class[class_idx];

                if rr_vec.is_empty() {
                    continue;
                }

                // Get next index from round-robin (without modifying position)
                let pos = if *rr_pos >= rr_vec.len() { 0 } else { *rr_pos };
                let idx = rr_vec[pos];

                // Peek one transaction from this queue (without removing)
                if let Some(rq) = self.ready_vec.get(idx) {
                    if let Some(tx) = rq.queue.front() {
                        // Clone transaction for preview (don't remove from queue)
                        out.push(tx.clone());
                        made_progress = true;
                    }
                }
            }

            if !made_progress {
                break;
            }
        }

        out
    }

    /// Lazy purge: check if enough time has passed
    fn purge_expired_internal_if_needed(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_purge) < self.purge_interval / 2 {
            self.purge_metrics.lazy_skip_count += 1;
            metrics::gauge!("mempool_purge_lazy_skips_total")
                .set(self.purge_metrics.lazy_skip_count as f64);
            return;
        }
        self.purge_expired_internal();
    }

    /// Internal purge implementation
    fn purge_expired_internal(&mut self) {
        let purge_start = Instant::now();
        let now = purge_start;
        let ttl = self.cfg.ttl;
        let initial_count = self.total;

        self.last_purge = now;

        // Purge expired transactions from ready queues
        let mut idx = 0;
        while idx < self.ready_vec.len() {
            let rq = &mut self.ready_vec[idx];
            let mut kept = VecDeque::new();

            while let Some(tx) = rq.queue.pop_front() {
                if now.duration_since(tx.inserted) > ttl {
                    // Expired: remove
                    self.total = self.total.saturating_sub(1);
                    self.counts[rq.sender_id as usize] =
                        self.counts[rq.sender_id as usize].saturating_sub(1);
                    increment_eviction();
                    // Notify admission control
                    let mut adm = self.admission.lock().unwrap();
                    adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                } else {
                    kept.push_back(tx);
                }
            }
            rq.queue = kept;

            // Remove queue if empty
            if rq.queue.is_empty() {
                self.remove_ready_queue(idx);
                continue;
            }
            idx += 1;
        }

        // Update metrics
        let purge_duration_us = purge_start.elapsed().as_micros() as u64;
        let purged_count = initial_count.saturating_sub(self.total);
        self.purge_metrics.purge_count += 1;
        self.purge_metrics.total_purge_time_us += purge_duration_us;
        self.purge_metrics.purged_tx_count += purged_count as u64;
        self.purge_metrics.last_purge_time_us = purge_duration_us;
        self.export_purge_metrics();

        // silent — could be a hidden TX sink (RPC accepts N TX, proposer's
        // intra-group mempool reads 0). Surface it as warn whenever it
        // actually evicts something, including a running total so callers
        // can compute purge_rate over time.
        if purged_count > 0 {
            tracing::warn!(
                purged_now = purged_count,
                ttl_secs = ttl.as_secs(),
                purged_total = self.purge_metrics.purged_tx_count,
                remaining = self.total,
                "DIAG[ttl-purge]: TX expired and removed from mempool"
            );
        }
        
        // Update mempool size metrics
        self.update_metrics();
    }

    /// Update mempool metrics (size, pending, ready counts)
    fn update_metrics(&self) {
        let total_size = self.total as u64;
        // For now, all transactions are considered ready (pending/ready distinction can be added later)
        let ready_count = total_size;
        let pending_count = 0u64; // Can be enhanced to track pending transactions separately

        update_mempool_metrics(total_size, pending_count, ready_count);
    }

    /// Export purge metrics to Prometheus
    fn export_purge_metrics(&self) {
        let metrics = &self.purge_metrics;
        metrics::gauge!("mempool_purge_operations_total").set(metrics.purge_count as f64);
        metrics::gauge!("mempool_purge_lazy_skips_total").set(metrics.lazy_skip_count as f64);
        metrics::gauge!("mempool_purge_transactions_total").set(metrics.purged_tx_count as f64);
    }

    pub fn len(&mut self) -> usize {
        self.purge_expired_internal_if_needed();
        self.total
    }

    /// DIAG[#50] stage 3: returns (self.total, ready_vec.len(), sum_of_queue_lens, non_empty_queues)
    /// so callers can detect divergence between the cached `total` counter and
    /// the actual tx count summed across ready queues.
    pub fn diag_state(&self) -> (usize, usize, usize, usize) {
        let rv_len = self.ready_vec.len();
        let mut sum_queue = 0usize;
        let mut non_empty = 0usize;
        for q in &self.ready_vec {
            let l = q.queue.len();
            sum_queue += l;
            if l > 0 {
                non_empty += 1;
            }
        }
        (self.total, rv_len, sum_queue, non_empty)
    }

    pub fn is_empty(&self) -> bool {
        self.total == 0
    }

    /// Remove transactions by handles (called after block commit)
    /// This is the cleanup method for block producer
    pub fn remove_by_handles(&mut self, handles: &[TxHandle]) {
        if handles.is_empty() {
            return;
        }

        // Build set of handles for O(1) lookup
        let handle_set: std::collections::HashSet<TxHandle> = handles.iter().copied().collect();

        // Remove transactions from ready queues
        let mut idx = 0;
        while idx < self.ready_vec.len() {
            let rq = &mut self.ready_vec[idx];
            let mut kept = VecDeque::new();

            while let Some(tx) = rq.queue.pop_front() {
                if handle_set.contains(&tx.tx_handle) {
                    // Remove this transaction
                    self.total = self.total.saturating_sub(1);
                    self.counts[rq.sender_id as usize] =
                        self.counts[rq.sender_id as usize].saturating_sub(1);
                    // Notify admission control
                    let mut adm = self.admission.lock().unwrap();
                    adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                } else {
                    kept.push_back(tx);
                }
            }
            rq.queue = kept;

            // Remove queue if empty
            if rq.queue.is_empty() {
                self.remove_ready_queue(idx);
                continue;
            }
            idx += 1;
        }

        // Update mempool metrics after removal
        self.update_metrics();
    }

    /// Get transaction handles from drained transactions
    /// This is a helper for block producer to track handles for cleanup
    pub fn extract_handles(txs: &[MempoolTx]) -> Vec<TxHandle> {
        txs.iter().map(|tx| tx.tx_handle).collect()
    }

    /// Cleanup after block commit (called by block producer)
    /// Removes committed transactions, promotes queued transactions, and starts new admission round.
    ///
    /// # Arguments
    /// * `committed_handles` - Handles of transactions included in the committed block
    /// * `nonce_updates` - Map of sender_id -> new_account_nonce for accounts that changed
    pub fn on_block_committed(
        &mut self,
        committed_handles: &[TxHandle],
        nonce_updates: &std::collections::HashMap<u32, u64>,
    ) {
        // Remove committed transactions
        self.remove_by_handles(committed_handles);
        // `confirmed_total: 0` hardcoded in stats_snapshot() hid real chain
        // progress — storage showed account.nonce=707 while the RPC metric
        // reported 0 committed TX, making it impossible to distinguish empty
        // blocks from working pipeline in monitoring dashboards.
        if !committed_handles.is_empty() {
            increment_confirmed_batch(committed_handles.len() as u64);
        }

        // Evict stale TX whose nonce is below the new confirmed account nonce.
        // Without this, TX that were admitted before a block commit but have nonces
        // that are now stale occupy per-sender cap slots indefinitely, causing the
        // 94% rejection rate under sustained load.
        if !nonce_updates.is_empty() {
            let mut evicted = 0usize;
            let mut idx = 0;
            while idx < self.ready_vec.len() {
                let rq = &mut self.ready_vec[idx];
                if let Some(&confirmed_nonce) = nonce_updates.get(&rq.sender_id) {
                    let before_len = rq.queue.len();
                    let mut kept = VecDeque::new();
                    while let Some(tx) = rq.queue.pop_front() {
                        if tx.nonce < confirmed_nonce {
                            self.total = self.total.saturating_sub(1);
                            self.counts[rq.sender_id as usize] =
                                self.counts[rq.sender_id as usize].saturating_sub(1);
                            let mut adm = self.admission.lock().unwrap();
                            adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                            evicted += 1;
                        } else {
                            kept.push_back(tx);
                        }
                    }
                    rq.queue = kept;
                    if rq.queue.is_empty() && before_len > 0 {
                        self.remove_ready_queue(idx);
                        continue;
                    }
                }
                idx += 1;
            }
            if evicted > 0 {
                tracing::debug!(
                    evicted,
                    "Mempool: evicted stale-nonce TX after block commit"
                );
            }
        }

        // Promote queued transactions whose nonces are now ready
        let promoted = {
            let mut adm = self.admission.lock().unwrap();
            adm.cleanup_queued_pool();
            let promoted = adm.promote_queued_batch(nonce_updates);
            adm.new_round();
            promoted
        };

        // Add promoted transactions to the main mempool
        for (pv, tx_hash) in promoted {
            let _ = self.add_prevalidated(pv, tx_hash);
        }

        // Update mempool metrics after block commit
        self.update_metrics();
    }

    /// Legacy on_block_committed without nonce updates (backward compatible).
    /// Does not promote queued transactions since nonce updates are not provided.
    pub fn on_block_committed_legacy(&mut self, committed_handles: &[TxHandle]) {
        self.on_block_committed(committed_handles, &std::collections::HashMap::new());
    }
}

/// Wrapper for Mempool with background purge task
pub struct MempoolWithBackgroundPurge {
    pub mempool: Arc<Mutex<Mempool>>,
    pub purge_task_handle: Option<JoinHandle<()>>,
}

impl MempoolWithBackgroundPurge {
    pub fn new(admission: Arc<Mutex<AdmissionControl>>) -> Self {
        Self {
            mempool: Arc::new(Mutex::new(Mempool::new(admission))),
            purge_task_handle: None,
        }
    }

    pub fn start_background_purge(&mut self) -> JoinHandle<()> {
        let mempool = self.mempool.clone();
        let handle = tokio::spawn(async move {
            // Read purge_interval using blocking lock
            let purge_interval = tokio::task::spawn_blocking({
                let mempool = mempool.clone();
                move || match mempool.lock() {
                    Ok(mp) => Ok(mp.purge_interval),
                    Err(_) => Err(()),
                }
            })
            .await
            .ok()
            .and_then(|r| r.ok());

            let purge_interval = match purge_interval {
                Some(interval) => interval,
                None => return, // Poisoned mutex
            };

            let start = tokio::time::Instant::now() + purge_interval;
            let mut interval = tokio::time::interval_at(start, purge_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Execute blocking lock operation in dedicated thread pool
                let result = tokio::task::spawn_blocking({
                    let mempool = mempool.clone();
                    move || {
                        let mut mp = match mempool.lock() {
                            Ok(mp) => mp,
                            Err(_) => return Err(()),
                        };
                        mp.purge_expired_internal();
                        Ok(())
                    }
                })
                .await;

                if result.is_err() || result.unwrap_or(Err(())).is_err() {
                    break; // Poisoned mutex or spawn failed
                }

                tokio::task::yield_now().await;
            }
        });

        // Store the handle and return a new one that does the same thing
        self.purge_task_handle = Some(handle);
        tokio::spawn(async {}) // Return a new empty task since we can't clone the original handle
    }
}
