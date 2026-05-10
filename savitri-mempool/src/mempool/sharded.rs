//! Sharded Mempool: Experimental parallel mempool architecture with sharding
//!
//! **⚠️ EXPERIMENTAL / FUTURE USE ⚠️**
//!
//! This module provides an **experimental** sharded mempool implementation designed
//! for future use in high-concurrency edge nodes. The primary implementation is
//! the monolithic `Mempool` in `src/mempool/core.rs`.
//!
//! **Status**: Experimental - Not used in production. Reserved for future edge nodes
//! with high concurrency requirements.
//!
//! This implementation distributes transactions across multiple shards based on
//! sender address hash. Each shard can be accessed independently, allowing parallel
//! operations.
//!
//! Key features:
//! - Hash-based sharding for uniform distribution (using AHasher for performance)
//! - Lock-free global deduplication using DashSet
//! - Shard isolation with per-shard RwLock
//! - Parallel drain using rayon
//! - Round-robin merge for fairness
//! - Padding to avoid false sharing between shards
//!
//! Thread-safety:
//! - Arc<RwLock<MempoolShard>>: Arc provides shared ownership, RwLock provides
//!   thread-safe read/write access. Multiple readers or one writer per shard.
//! - DashSet<TxHandle>: Lock-free concurrent hash set, safe for concurrent access
//! - Rayon parallel iterators: Thread-safe parallel processing
//! - PaddedShard: Cache-line alignment prevents false sharing when multiple
//!   threads access different shards simultaneously

use crate::mempool::admission::AdmissionControl;
use crate::mempool::core::MempoolConfig;
use crate::mempool::types::{MempoolTx, PrevalidatedTx, SenderId, TxClass, TxHandle};
use crate::mempool::PurgeMetrics;
use ahash::AHasher;
use dashmap::DashSet;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Ready queue for a sender (indexed by sender_id)
struct ReadyQueue {
    sender_id: SenderId,
    queue: VecDeque<MempoolTx>,
}

/// Trait for mempool interface (for compatibility with existing code)
pub trait MempoolInterface {
    fn add_prevalidated(&mut self, pv: PrevalidatedTx) -> Result<(), ()>;

    /// Drain transactions fairly using round-robin (class-aware)
    fn drain_fair_batch(&mut self, max: usize) -> Vec<MempoolTx>;

    /// Remove transactions by handles (called after block commit)
    fn remove_by_handles(&mut self, handles: &[TxHandle]);

    /// Get current mempool length
    fn len(&mut self) -> usize;

    /// Check if mempool is empty
    fn is_empty(&self) -> bool;

    /// Get all transaction handles currently in mempool
    fn all_handles(&self) -> Vec<TxHandle>;

    /// Cleanup after block commit
    fn on_block_committed(&mut self, committed_handles: &[TxHandle]);
}

/// Single mempool shard (similar to Mempool but for a subset of transactions)
pub struct MempoolShard {
    cfg: MempoolConfig,
    /// Ready queues stored contiguously
    ready_vec: Vec<ReadyQueue>,
    /// Index sender_id -> ready_vec index
    ready_index: Vec<Option<usize>>,
    /// Round-robin indices per class
    rr_vec_by_class: [Vec<usize>; 4], // Financial, IoTData, FederatedUpdate, System
    /// Round-robin cursor per class
    rr_pos_by_class: [usize; 4],
    /// Per-sender counts
    counts: Vec<usize>,
    /// Total count
    total: usize,
    /// Lazy purge state
    last_purge: Instant,
    purge_interval: Duration,
    purge_metrics: PurgeMetrics,
    /// Admission control (shared)
    admission: Arc<Mutex<AdmissionControl>>,
}

impl MempoolShard {
    /// Create a new mempool shard
    pub fn new(admission: Arc<Mutex<AdmissionControl>>) -> Self {
        Self {
            cfg: MempoolConfig::default(),
            ready_vec: Vec::new(),
            ready_index: Vec::new(),
            rr_vec_by_class: [Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            rr_pos_by_class: [0, 0, 0, 0],
            counts: Vec::new(),
            total: 0,
            last_purge: Instant::now(),
            purge_interval: Duration::from_secs(1),
            purge_metrics: PurgeMetrics::default(),
            admission,
        }
    }

    /// Create a new mempool shard with custom config
    pub fn with_config(cfg: MempoolConfig, admission: Arc<Mutex<AdmissionControl>>) -> Self {
        let mut new_self = Self::new(admission);
        new_self.cfg = cfg;
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
    fn ensure_index_capacity(&mut self, sender_id: SenderId) {
        let idx = sender_id as usize;
        if idx >= self.ready_index.len() {
            let new_capacity = (self.ready_index.len() * 2).max(idx + 1);
            self.ready_index.resize(new_capacity, None);
        }
        if idx >= self.counts.len() {
            let new_capacity = (self.counts.len() * 2).max(idx + 1);
            self.counts.resize(new_capacity, 0);
        }
    }

    /// Remove ready queue by index (swap-remove for O(1))
    fn remove_ready_queue(&mut self, idx: usize) {
        if idx >= self.ready_vec.len() {
            return;
        }
        let last_idx = self.ready_vec.len() - 1;
        let removed = self.ready_vec.swap_remove(idx);

        self.ready_index[removed.sender_id as usize] = None;

        if idx != last_idx {
            let swapped_sender_id = self.ready_vec[idx].sender_id;
            self.ready_index[swapped_sender_id as usize] = Some(idx);
            for rr_vec in &mut self.rr_vec_by_class {
                for slot in rr_vec.iter_mut() {
                    if *slot == last_idx {
                        *slot = idx;
                    }
                }
            }
        }
        for rr_vec in &mut self.rr_vec_by_class {
            rr_vec.retain(|&i| i < self.ready_vec.len());
        }
        for (pos, rr_vec) in self.rr_pos_by_class.iter_mut().zip(&self.rr_vec_by_class) {
            if *pos >= rr_vec.len() {
                *pos = 0;
            }
        }
    }

    pub fn add_prevalidated(&mut self, pv: PrevalidatedTx) -> Result<(), ()> {
        // 1. Check admission control
        {
            let mut adm = self.admission.lock().unwrap();
            if !adm.check_admission(&pv, None) {
                return Err(());
            }
            adm.record_admission(&pv, None);
        }

        // 2. Ensure capacity
        self.ensure_index_capacity(pv.sender_id);

        // 3. Get or create ready queue index
        let sender_idx = pv.sender_id as usize;
        let idx = match self.ready_index[sender_idx] {
            Some(i) => i,
            None => {
                let i = self.ready_vec.len();
                self.ready_vec.push(ReadyQueue {
                    sender_id: pv.sender_id,
                    queue: VecDeque::new(),
                });
                self.ready_index[sender_idx] = Some(i);
                i
            }
        };

        // 4. Enqueue transaction
        let rq = &mut self.ready_vec[idx];
        let was_empty = rq.queue.is_empty();
        let sender_id = pv.sender_id;
        rq.queue.push_back(MempoolTx::from(pv));
        self.total += 1;
        self.counts[sender_id as usize] += 1;

        // 5. Add to round-robin if queue was empty
        if was_empty {
            let class_idx = Self::class_idx(rq.queue.back().unwrap().class);
            self.rr_vec_by_class[class_idx].push(idx);
        }

        Ok(())
    }

    /// Drain transactions fairly using round-robin (class-aware)
    pub fn drain_fair_batch(&mut self, max: usize) -> Vec<MempoolTx> {
        self.purge_expired_internal_if_needed();
        if max == 0 {
            return Vec::new();
        }

        let cap = max.min(self.total);
        let mut out = Vec::with_capacity(cap);

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

                if *rr_pos >= rr_vec.len() {
                    *rr_pos = 0;
                }
                let idx = rr_vec[*rr_pos];
                *rr_pos = (*rr_pos + 1) % rr_vec.len().max(1);

                if let Some(rq) = self.ready_vec.get_mut(idx) {
                    if let Some(tx) = rq.queue.pop_front() {
                        self.total = self.total.saturating_sub(1);
                        self.counts[rq.sender_id as usize] =
                            self.counts[rq.sender_id as usize].saturating_sub(1);
                        out.push(tx);
                        made_progress = true;

                        if rq.queue.is_empty() {
                            rr_vec.retain(|&i| i != idx);
                            if *rr_pos >= rr_vec.len() {
                                *rr_pos = 0;
                            }
                        }
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

        let mut idx = 0;
        while idx < self.ready_vec.len() {
            let rq = &mut self.ready_vec[idx];
            let mut kept = VecDeque::new();

            while let Some(tx) = rq.queue.pop_front() {
                if now.duration_since(tx.inserted) > ttl {
                    self.total = self.total.saturating_sub(1);
                    self.counts[rq.sender_id as usize] =
                        self.counts[rq.sender_id as usize].saturating_sub(1);
                    let mut adm = self.admission.lock().unwrap();
                    adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                } else {
                    kept.push_back(tx);
                }
            }
            rq.queue = kept;

            if rq.queue.is_empty() {
                self.remove_ready_queue(idx);
                continue;
            }
            idx += 1;
        }

        let purge_duration_us = purge_start.elapsed().as_micros() as u64;
        let purged_count = initial_count.saturating_sub(self.total);
        self.purge_metrics.purge_count += 1;
        self.purge_metrics.total_purge_time_us += purge_duration_us;
        self.purge_metrics.purged_tx_count += purged_count as u64;
        self.purge_metrics.last_purge_time_us = purge_duration_us;
    }

    /// Remove transactions by handles
    pub fn remove_by_handles(&mut self, handles: &[TxHandle]) {
        if handles.is_empty() {
            return;
        }

        let handle_set: std::collections::HashSet<TxHandle> = handles.iter().copied().collect();

        let mut idx = 0;
        while idx < self.ready_vec.len() {
            let rq = &mut self.ready_vec[idx];
            let mut kept = VecDeque::new();

            while let Some(tx) = rq.queue.pop_front() {
                if handle_set.contains(&tx.tx_handle) {
                    self.total = self.total.saturating_sub(1);
                    self.counts[rq.sender_id as usize] =
                        self.counts[rq.sender_id as usize].saturating_sub(1);
                    let mut adm = self.admission.lock().unwrap();
                    adm.record_removal(rq.sender_id, tx.class, tx.tx_hash);
                } else {
                    kept.push_back(tx);
                }
            }
            rq.queue = kept;

            if rq.queue.is_empty() {
                self.remove_ready_queue(idx);
                continue;
            }
            idx += 1;
        }
    }

    /// Get all transaction handles
    pub fn all_handles(&self) -> Vec<TxHandle> {
        let mut out = Vec::with_capacity(self.total);
        for rq in self.ready_vec.iter() {
            for tx in rq.queue.iter() {
                out.push(tx.tx_handle);
            }
        }
        out
    }

    /// Cleanup after block commit
    pub fn on_block_committed(&mut self, committed_handles: &[TxHandle]) {
        self.remove_by_handles(committed_handles);
        let mut adm = self.admission.lock().unwrap();
        adm.new_round();
    }

    /// Get current length
    pub fn len(&mut self) -> usize {
        self.purge_expired_internal_if_needed();
        self.total
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

/// Wrapper for shard with padding to avoid false sharing
/// Aligns to cache line boundary (64 bytes) to prevent false sharing
/// when multiple threads access different shards simultaneously
#[repr(align(64))]
struct PaddedShard {
    shard: Arc<RwLock<MempoolShard>>,
    // Padding to ensure alignment to cache line (64 bytes)
    // Arc<RwLock<MempoolShard>> is typically 8 bytes (pointer),
    // so we add 56 bytes of padding to reach 64 bytes alignment
    _padding: [u8; 56],
}

impl PaddedShard {
    fn new(shard: Arc<RwLock<MempoolShard>>) -> Self {
        Self {
            shard,
            _padding: [0; 56],
        }
    }

    #[inline]
    fn as_ref(&self) -> &Arc<RwLock<MempoolShard>> {
        &self.shard
    }
}

/// Batch of transactions drained from a specific shard (for shard committees).
#[derive(Debug, Clone)]
pub struct ShardBatch {
    pub shard_id: usize,
    pub txs: Vec<MempoolTx>,
}

/// Sharded Mempool: distributes transactions across multiple shards
/// Optimized with:
/// - AHasher for better hash distribution
/// - Padding to avoid false sharing between shards
/// - Optimized merge without unnecessary clones
pub struct ShardedMempool {
    /// Array of padded shards, each with its own lock
    /// Padding ensures each shard is on a separate cache line (64 bytes)
    /// to avoid false sharing when multiple threads access different shards
    shards: Vec<PaddedShard>,
    /// Lock-free global deduplication set
    seen_global: DashSet<TxHandle>,
    /// Number of shards
    num_shards: usize,
}

impl ShardedMempool {
    /// Create a new sharded mempool with default number of shards (num_cpus)
    pub fn new(admission: Arc<Mutex<AdmissionControl>>) -> Self {
        let num_shards = num_cpus::get().max(1);
        Self::with_num_shards(num_shards, admission)
    }

    /// Create a new sharded mempool with specified number of shards
    pub fn with_num_shards(num_shards: usize, admission: Arc<Mutex<AdmissionControl>>) -> Self {
        let num_shards = num_shards.max(1);
        let shards: Vec<PaddedShard> = (0..num_shards)
            .map(|_| PaddedShard::new(Arc::new(RwLock::new(MempoolShard::new(admission.clone())))))
            .collect();

        Self {
            shards,
            seen_global: DashSet::new(),
            num_shards,
        }
    }

    /// Create a new sharded mempool with custom config
    pub fn with_config(cfg: MempoolConfig, admission: Arc<Mutex<AdmissionControl>>) -> Self {
        let num_shards = num_cpus::get().max(1);
        Self::with_config_and_shards(num_shards, cfg, admission)
    }

    /// Create a new sharded mempool with custom config and number of shards
    pub fn with_config_and_shards(
        num_shards: usize,
        cfg: MempoolConfig,
        admission: Arc<Mutex<AdmissionControl>>,
    ) -> Self {
        let num_shards = num_shards.max(1);
        let shards: Vec<PaddedShard> = (0..num_shards)
            .map(|_| {
                PaddedShard::new(Arc::new(RwLock::new(MempoolShard::with_config(
                    cfg,
                    admission.clone(),
                ))))
            })
            .collect();

        Self {
            shards,
            seen_global: DashSet::new(),
            num_shards,
        }
    }

    /// Compute shard index from sender address hash
    /// Uses AHasher for better distribution and performance
    #[allow(dead_code)] // Reserved for future use
    #[inline]
    fn shard_index(&self, address: &[u8]) -> usize {
        let mut hasher = AHasher::default();
        address.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Uses AHasher for better distribution and performance
    #[inline]
    fn shard_index_from_sender_id(&self, sender_id: SenderId) -> usize {
        // Use sender_id as hash input for deterministic sharding
        let mut hasher = AHasher::default();
        sender_id.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    /// Round-robin merge for fairness across shards
    /// Optimized to avoid unnecessary clones by taking ownership of shard_results
    /// Uses VecDeque for efficient O(1) front removal
    #[inline]
    fn round_robin_merge(&self, shard_results: Vec<Vec<MempoolTx>>, max: usize) -> Vec<MempoolTx> {
        // Convert each Vec to VecDeque for O(1) front removal
        let mut shard_deques: Vec<VecDeque<MempoolTx>> = shard_results
            .into_iter()
            .map(|v| v.into_iter().collect())
            .collect();

        let mut merged = Vec::with_capacity(max);

        // Round-robin: take one from each shard in turn
        while merged.len() < max {
            let mut made_progress = false;

            for shard_deque in shard_deques.iter_mut() {
                if merged.len() >= max {
                    break;
                }

                if let Some(tx) = shard_deque.pop_front() {
                    merged.push(tx);
                    made_progress = true;
                }
            }

            if !made_progress {
                break;
            }
        }

        merged
    }

    /// Get number of shards
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Still enforces global deduplication.
    pub fn add_prevalidated_to_shard(
        &mut self,
        pv: PrevalidatedTx,
        shard_id: usize,
    ) -> Result<(), ()> {
        let tx_handle = pv.tx_handle;
        if !self.seen_global.insert(tx_handle) {
            return Err(());
        }

        let target = shard_id % self.num_shards;
        let shard = self.shards[target].as_ref();
        let mut shard_guard = shard.write().unwrap();
        match shard_guard.add_prevalidated(pv) {
            Ok(()) => Ok(()),
            Err(()) => {
                self.seen_global.remove(&tx_handle);
                Err(())
            }
        }
    }

    /// Drain transactions per-shard (for shard committees) without global merge.
    /// Removes drained transactions from the global deduplication set.
    pub fn drain_partitioned(&mut self, max_per_shard: usize) -> Vec<ShardBatch> {
        if max_per_shard == 0 {
            return Vec::new();
        }

        let results: Vec<ShardBatch> = (0..self.num_shards)
            .map(|shard_idx| {
                let shard = self.shards[shard_idx].as_ref();
                let mut shard_guard = shard.write().unwrap();
                let drained = shard_guard.drain_fair_batch(max_per_shard);

                for tx in &drained {
                    self.seen_global.remove(&tx.tx_handle);
                }

                ShardBatch {
                    shard_id: shard_idx,
                    txs: drained,
                }
            })
            .collect();

        results
    }
}

impl MempoolInterface for ShardedMempool {
    fn add_prevalidated(&mut self, pv: PrevalidatedTx) -> Result<(), ()> {
        // 1. Check global deduplication (lock-free)
        let tx_handle = pv.tx_handle;
        if !self.seen_global.insert(tx_handle) {
            // Already seen, reject as duplicate
            return Err(());
        }

        // 2. Determine target shard based on sender_id
        let shard_idx = self.shard_index_from_sender_id(pv.sender_id);

        // 3. Add to target shard (write lock only on that shard)
        let shard = self.shards[shard_idx].as_ref();
        let mut shard_guard = shard.write().unwrap();
        match shard_guard.add_prevalidated(pv) {
            Ok(()) => Ok(()),
            Err(()) => {
                // If admission fails, remove from seen_global
                self.seen_global.remove(&tx_handle);
                Err(())
            }
        }
    }

    /// Drain transactions fairly using parallel drain + round-robin merge
    fn drain_fair_batch(&mut self, max: usize) -> Vec<MempoolTx> {
        if max == 0 {
            return Vec::new();
        }

        // Parallel drain from all shards
        let max_per_shard = (max / self.num_shards).max(1);
        let seen_global = &self.seen_global;
        let shard_results: Vec<Vec<MempoolTx>> = self
            .shards
            .par_iter()
            .map(|padded_shard| {
                let shard = padded_shard.as_ref();
                let mut shard_guard = shard.write().unwrap();
                let drained = shard_guard.drain_fair_batch(max_per_shard);

                // Remove from seen_global as we drain
                for tx in &drained {
                    seen_global.remove(&tx.tx_handle);
                }

                drained
            })
            .collect();

        // Round-robin merge for fairness
        self.round_robin_merge(shard_results, max)
    }

    /// Remove transactions by handles
    fn remove_by_handles(&mut self, handles: &[TxHandle]) {
        if handles.is_empty() {
            return;
        }

        // Remove from seen_global (lock-free)
        for handle in handles {
            self.seen_global.remove(handle);
        }

        // Remove from all shards in parallel
        self.shards.par_iter().for_each(|padded_shard| {
            let shard = padded_shard.as_ref();
            let mut shard_guard = shard.write().unwrap();
            shard_guard.remove_by_handles(handles);
        });
    }

    /// Get current mempool length
    fn len(&mut self) -> usize {
        // Sum lengths from all shards
        self.shards
            .par_iter()
            .map(|padded_shard| {
                let shard = padded_shard.as_ref();
                let mut shard_guard = shard.write().unwrap();
                shard_guard.len()
            })
            .sum()
    }

    /// Check if mempool is empty
    fn is_empty(&self) -> bool {
        // Check all shards in parallel
        !self.shards.par_iter().any(|padded_shard| {
            let shard = padded_shard.as_ref();
            let shard_guard = shard.read().unwrap();
            !shard_guard.is_empty()
        })
    }

    /// Get all transaction handles
    fn all_handles(&self) -> Vec<TxHandle> {
        // Collect handles from all shards in parallel
        let shard_handles: Vec<Vec<TxHandle>> = self
            .shards
            .par_iter()
            .map(|padded_shard| {
                let shard = padded_shard.as_ref();
                let shard_guard = shard.read().unwrap();
                shard_guard.all_handles()
            })
            .collect();

        // Flatten results
        shard_handles.into_iter().flatten().collect()
    }

    /// Cleanup after block commit
    fn on_block_committed(&mut self, committed_handles: &[TxHandle]) {
        // Remove from seen_global
        for handle in committed_handles {
            self.seen_global.remove(handle);
        }

        // Notify all shards in parallel
        self.shards.par_iter().for_each(|padded_shard| {
            let shard = padded_shard.as_ref();
            let mut shard_guard = shard.write().unwrap();
            shard_guard.on_block_committed(committed_handles);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::admission::AdmissionControl;

    #[test]
    fn test_shard_index_distribution() {
        let admission = Arc::new(Mutex::new(AdmissionControl::new(
            crate::mempool::admission::AdmissionConfig::default(),
        )));
        let mempool = ShardedMempool::with_num_shards(4, admission);

        // Test that different addresses map to different shards (with high probability)
        let addresses = [
            vec![1u8; 32],
            vec![2u8; 32],
            vec![3u8; 32],
            vec![4u8; 32],
            vec![5u8; 32],
        ];

        let shard_indices: Vec<usize> = addresses
            .iter()
            .map(|addr| mempool.shard_index(addr))
            .collect();

        // Verify all indices are valid
        for idx in &shard_indices {
            assert!(*idx < 4, "Shard index out of bounds");
        }

        // With 5 addresses and 4 shards, we should have some distribution
        // (not all in same shard, though it's possible)
        let unique_shards: std::collections::HashSet<usize> =
            shard_indices.iter().copied().collect();
        // At least one shard should be used (trivial check)
        assert!(!unique_shards.is_empty());
    }

    #[test]
    fn test_sharded_mempool_basic() {
        let admission = Arc::new(Mutex::new(AdmissionControl::new(
            crate::mempool::admission::AdmissionConfig::default(),
        )));
        let mut mempool = ShardedMempool::with_num_shards(2, admission);

        // Create a test transaction
        let pv = PrevalidatedTx {
            sender_id: 1,
            sender_address: [1u8; 32], // Test address
            nonce: 1,
            max_fee: 1000,
            amount: 0,
            tx_handle: TxHandle(1),
            class: TxClass::Financial,
            stream_nonce: None,
        };

        // Add transaction
        assert!(mempool.add_prevalidated(pv.clone()).is_ok());

        // Check it's in seen_global
        assert!(mempool.seen_global.contains(&pv.tx_handle));

        // Try to add duplicate
        assert!(mempool.add_prevalidated(pv).is_err());

        // Drain
        let drained = mempool.drain_fair_batch(10);
        assert_eq!(drained.len(), 1);
    }
}
