//! Integration layer for class-aware mempool architecture
//!
//! This module provides a unified interface that connects:

use crate::dispatcher::MempoolState;
use crate::executor::dispatcher::ExecutionDispatcher;
use crate::mempool::admission::{AdmissionConfig, AdmissionControl};
use crate::mempool::core::Mempool;
use crate::mempool::prevalidation::Prevalidator;
use crate::mempool::types::{MempoolTx, PrevalidatedTx, RawTx, SignedTx, TxClass, TxHandle};
use crate::mempool::PrevalidationResult;
use crate::types::PipelinePrefetcher;
use crate::DispatcherConfig;
use anyhow::Result;
use bincode;
use hex;
use savitri_core::core::types::Transaction;
use savitri_core::Account;
use savitri_storage::{Storage, StorageTrait};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

/// Deserialize signed transaction from bytes using bincode
fn deserialize_signed_tx(bytes: &[u8]) -> Option<Transaction> {
    // Check if bytes are empty or too short before attempting deserialization
    if bytes.is_empty() {
        tracing::debug!("Skipping empty transaction bytes");
        return None;
    }

    // Minimum expected size for a Transaction (rough estimate: addresses + amount + nonce + fee + signature)
    // This is a heuristic check - actual size depends on serialization format
    const MIN_EXPECTED_SIZE: usize = 64; // At least addresses (32+32) + some metadata
    if bytes.len() < MIN_EXPECTED_SIZE {
        tracing::debug!(
            bytes_len = bytes.len(),
            min_expected = MIN_EXPECTED_SIZE,
            "Transaction bytes too short, skipping deserialization"
        );
        return None;
    }

    // Use bincode to deserialize the transaction
    match bincode::deserialize::<Transaction>(bytes) {
        Ok(tx) => Some(tx),
        Err(e) => {
            // Only log as warning if it's not an "unexpected end of file" error (which is expected for incomplete bytes)
            if e.to_string().contains("unexpected end of file") {
                tracing::debug!(
                    bytes_len = bytes.len(),
                    error = %e,
                    "Transaction bytes incomplete or truncated, skipping"
                );
            } else {
                tracing::warn!(
                    bytes_len = bytes.len(),
                    error = %e,
                    "Failed to deserialize transaction"
                );
            }
            None
        }
    }
}

/// Create a SignedTx from raw bytes using real deserialization
///
/// Pre-fix this function returned a zombie SignedTx (from=[0;32], nonce=0,
/// sig=[0;64]) when ALL three deserialize attempts failed. The zombie was
/// `Some(zombie)` in the caller's filter_map, so undeserializable bytes were
/// kept in signed_txs, causing index misalignment with mempool_txs (which
/// mempool_tx[i].sender_id with signed_tx[i].from from a different handle,
/// breaking the running_nonces tracking and triggering blocked_senders
/// cascades unrelated to actual nonce mismatches.
///
/// Returning None propagates cleanly through the filter_map at
/// integration.rs:1442; mempool_txs is then truncated to match via the
/// `min_len.take()` step at integration.rs:1517.
fn create_signed_tx_from_bytes(raw_tx: &[u8]) -> Option<SignedTx> {
    use bincode::Options;
    use serde_big_array::BigArray;

    // CANONICAL FORMAT: TransactionExt with fixint encoding + big_array sig
    // This is the wire format used by lightnode, rpc-loadtest, and gossipsub TX broadcast.
    {
        #[derive(serde::Deserialize)]
        struct TransactionExtCompat {
            from: String,
            to: String,
            amount: u64,
            nonce: u64,
            fee: Option<u128>,
            data: Option<Vec<u8>>,
            pubkey: Vec<u8>,
            #[serde(with = "BigArray")]
            sig: [u8; 64],
            pre_verified: bool,
        }

        if let Ok(tx) = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(1_048_576)
            .deserialize::<TransactionExtCompat>(raw_tx)
        {
            if tx.from.len() == 64 && tx.pubkey.len() == 32 {
                let from_bytes =
                    hex::decode(&tx.from).unwrap_or_else(|_| tx.from.as_bytes().to_vec());
                let to_bytes = hex::decode(&tx.to).unwrap_or_else(|_| tx.to.as_bytes().to_vec());
                tracing::debug!(
                    from = %&tx.from[..16], nonce = tx.nonce,
                    "Deserialized canonical TransactionExt (drain path)"
                );
                return Some(SignedTx {
                    from: from_bytes,
                    to: to_bytes,
                    amount: tx.amount,
                    nonce: tx.nonce,
                    fee: tx.fee.unwrap_or(1000) as u64,
                    pubkey: tx.pubkey,
                    sig: tx.sig.to_vec(),
                    pre_verified: tx.pre_verified,
                });
            }
        }
    }

    // INTERNAL FORMAT: SignedTx (Vec<u8> fields, u64 fee, default bincode)
    if let Ok(signed_tx) = bincode::deserialize::<SignedTx>(raw_tx) {
        let valid = signed_tx.from.len() == 32
            && signed_tx.to.len() == 32
            && signed_tx.sig.len() == 64
            && signed_tx.pubkey.len() == 32;
        if valid {
            return Some(signed_tx);
        }
    }

    // Fallback: core Transaction format
    if let Some(transaction) = deserialize_signed_tx(raw_tx) {
        return Some(SignedTx {
            from: hex::decode(&transaction.from)
                .unwrap_or_else(|_| transaction.from.as_bytes().to_vec()),
            to: hex::decode(&transaction.to).unwrap_or_else(|_| transaction.to.as_bytes().to_vec()),
            amount: transaction.amount,
            nonce: transaction.nonce,
            fee: transaction.fee,
            pubkey: vec![0u8; 32],
            sig: transaction.signature,
            pre_verified: false,
        });
    }

    tracing::warn!(
        bytes_len = raw_tx.len(),
        "Failed to deserialize TX (all formats) — dropping"
    );
    None
}

/// Create default transaction with basic structure
fn create_default_transaction() -> Transaction {
    Transaction::default()
}

/// Deserialize call transaction from bytes
fn deserialize_call_tx(bytes: &[u8]) -> Option<Transaction> {
    // Similar to deserialize_signed_tx but for call transactions
    deserialize_signed_tx(bytes)
}

/// Hash signed transaction bytes using BLAKE3 (internal dedup, not on-chain)
fn hash_signed_tx_bytes(bytes: &[u8]) -> [u8; 32] {
    *blake3::hash(bytes).as_bytes()
}

/// Serialize SignedTx for hash calculation
fn serialize_signed_tx_for_hash(tx: &SignedTx) -> Vec<u8> {
    let mut bytes = Vec::new();

    // Add from address
    bytes.extend_from_slice(&tx.from);
    // Add to address
    bytes.extend_from_slice(&tx.to);
    // Add amount
    bytes.extend_from_slice(&tx.amount.to_le_bytes());
    // Add nonce
    bytes.extend_from_slice(&tx.nonce.to_le_bytes());
    // Add fee
    bytes.extend_from_slice(&tx.fee.to_le_bytes());
    // Add public key
    bytes.extend_from_slice(&tx.pubkey);
    // Add signature
    bytes.extend_from_slice(&tx.sig);
    // Add pre_verified flag
    bytes.push(if tx.pre_verified { 1 } else { 0 });

    bytes
}

/// Mempool state tracker with rolling history.
/// Holds stats for the last N blocks to drive weight adaptation.
#[derive(Debug, Clone)]
pub struct MempoolStateTracker {
    /// Rolling history of mempool states (last N blocks).
    state_history: Vec<MempoolState>,
    /// Maximum number of states retained (default: 10).
    max_history_size: usize,
    /// Block counter for periodic updates.
    block_counter: usize,
    /// Weight refresh interval (default: every K blocks).
    update_interval: usize,
    /// Timestamp of the last update.
    last_update_timestamp: u64,
}

impl MempoolStateTracker {
    /// Build a tracker with default configuration.
    pub fn new() -> Self {
        Self {
            state_history: Vec::new(),
            max_history_size: 10, // Keep the last 10 blocks
            block_counter: 0,
            update_interval: 5, // Refresh every 5 blocks
            last_update_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Build a tracker with custom configuration.
    pub fn with_config(max_history_size: usize, update_interval: usize) -> Self {
        Self {
            state_history: Vec::new(),
            max_history_size,
            block_counter: 0,
            update_interval,
            last_update_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Append a mempool state to the rolling history.
    pub fn add_state(&mut self, state: MempoolState) {
        self.state_history.push(state);

        // Keep only the last max_history_size entries.
        if self.state_history.len() > self.max_history_size {
            self.state_history.remove(0);
        }

        self.block_counter += 1;
        self.last_update_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Returns true when it is time to refresh the weights.
    pub fn should_update_weights(&self) -> bool {
        self.block_counter % self.update_interval == 0 && !self.state_history.is_empty()
    }

    /// Returns the rolling state history.
    pub fn get_state_history(&self) -> &[MempoolState] {
        &self.state_history
    }

    /// Returns the most recent state.
    pub fn get_latest_state(&self) -> Option<&MempoolState> {
        self.state_history.last()
    }

    /// Compute aggregate stats over the recorded history.
    pub fn calculate_aggregate_stats(&self) -> Option<MempoolAggregateStats> {
        if self.state_history.is_empty() {
            return None;
        }

        let mut all_fees = Vec::new();
        let mut all_classes = Vec::new();
        let mut all_throughputs = Vec::new();

        for state in &self.state_history {
            all_fees.extend(&state.fee_distribution);
            all_classes.extend(&state.class_distribution);
            all_throughputs.extend(&state.historical_throughput);
        }

        Some(MempoolAggregateStats {
            avg_fee: all_fees.iter().sum::<u64>() as f64 / all_fees.len() as f64,
            median_fee: {
                let mut sorted_fees = all_fees;
                sorted_fees.sort_unstable();
                sorted_fees[sorted_fees.len() / 2] as f64
            },
            class_distribution: self.calculate_class_distribution(&all_classes),
            avg_throughput: all_throughputs.iter().sum::<f64>() / all_throughputs.len() as f64,
            total_samples: self.state_history.len(),
        })
    }

    fn calculate_class_distribution(
        &self,
        classes: &[TxClass],
    ) -> std::collections::HashMap<TxClass, f64> {
        let mut class_counts = std::collections::HashMap::new();
        for class in classes {
            *class_counts.entry(*class).or_insert(0) += 1;
        }

        let total = classes.len() as f64;
        class_counts
            .into_iter()
            .map(|(class, count)| (class, count as f64 / total))
            .collect()
    }

    /// Resetta il tracker
    pub fn reset(&mut self) {
        self.state_history.clear();
        self.block_counter = 0;
        self.last_update_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

/// Statistiche aggregate of the mempool
#[derive(Debug, Clone)]
pub struct MempoolAggregateStats {
    pub avg_fee: f64,
    pub median_fee: f64,
    pub class_distribution: std::collections::HashMap<TxClass, f64>,
    /// Throughput medio
    pub avg_throughput: f64,
    /// Numero totale di campioni
    pub total_samples: usize,
}

/// Point-in-time snapshot of mempool counters for RPC/monitoring.
///
/// Cumulative `*_total` fields are monotonically increasing — used by the window
/// tracker in `savitri-rpc` to compute 1-minute and 1-hour rate deltas.
#[derive(Debug, Clone, Copy, Default)]
pub struct MempoolStatsSnapshot {
    /// Current number of pending (not-yet-ready) transactions
    pub pending: u64,
    /// Current number of ready (queued for execution) transactions
    pub queued: u64,
    /// Current total transactions in the mempool
    pub total: u64,
    /// Cumulative count of admitted transactions
    pub admitted_total: u64,
    /// Cumulative count of queued transactions (same as admitted for this impl)
    pub queued_total: u64,
    /// Cumulative count of rejected transactions
    pub rejected_total: u64,
    /// Cumulative count of transactions removed for block production
    pub removed_total: u64,
    /// Cumulative count of evicted transactions
    pub evicted_total: u64,
    /// Cumulative count of confirmed transactions (tracked externally; 0 until block finalization hooks are wired)
    pub confirmed_total: u64,
}

/// Metriche per IoT data batching performance
#[derive(Debug, Clone)]
pub struct IoTBatchingMetrics {
    /// Numero totale di transazioni processate
    pub total_transactions: usize,
    /// Numero totale di batch creati
    pub total_batches: usize,
    /// Numero totale di device/sender diversi
    pub total_devices: usize,
    /// Dimensione totale dei dati originali (bytes)
    pub total_original_size: usize,
    /// Dimensione totale dei dati batchati (bytes)
    pub total_batched_size: usize,
    /// Tempo totale di processing (microsecondi)
    pub total_processing_time_us: u64,
    /// Tempo massimo per un singolo batch (microsecondi)
    pub peak_batch_time_us: u64,
    pub skipped_transactions: usize,
}

impl IoTBatchingMetrics {
    /// Creates nuove metriche vuote
    pub fn new() -> Self {
        Self {
            total_transactions: 0,
            total_batches: 0,
            total_devices: 0,
            total_original_size: 0,
            total_batched_size: 0,
            total_processing_time_us: 0,
            peak_batch_time_us: 0,
            skipped_transactions: 0,
        }
    }

    /// Compute batch efficiency (riduzione numero di transazioni)
    pub fn batch_efficiency(&self) -> f64 {
        if self.total_transactions == 0 {
            return 0.0;
        }
        let reduction = 1.0 - (self.total_batches as f64 / self.total_transactions as f64);
        reduction * 100.0
    }

    /// Compute compression ratio (riduzione dimensione dati)
    pub fn compression_ratio(&self) -> f64 {
        if self.total_original_size == 0 {
            return 0.0;
        }
        let reduction = 1.0 - (self.total_batched_size as f64 / self.total_original_size as f64);
        reduction * 100.0
    }

    /// Compute overhead reduction (riduzione overhead storage/execution)
    /// Considera: overhead per transazione (headers, signatures, etc.)
    pub fn overhead_reduction(&self) -> f64 {
        if self.total_transactions == 0 {
            return 0.0;
        }
        // Stima overhead per transazione: ~200 bytes (headers, signatures, etc.)
        const OVERHEAD_PER_TX: usize = 200;
        let original_overhead = self.total_transactions * OVERHEAD_PER_TX;
        let batched_overhead = self.total_batches * OVERHEAD_PER_TX;

        if original_overhead == 0 {
            return 0.0;
        }
        let reduction = 1.0 - (batched_overhead as f64 / original_overhead as f64);
        reduction * 100.0
    }

    /// Compute throughput (transazioni per secondo)
    pub fn throughput_tps(&self) -> f64 {
        if self.total_processing_time_us == 0 {
            return 0.0;
        }
        (self.total_transactions as f64 / self.total_processing_time_us as f64) * 1_000_000.0
    }

    pub fn avg_batch_latency_us(&self) -> Option<f64> {
        if self.total_batches == 0 {
            return None;
        }
        Some(self.total_processing_time_us as f64 / self.total_batches as f64)
    }
}

/// Error types for transaction processing in RPC layer
#[derive(Debug, Clone)]
pub enum TransactionProcessError {
    PrevalidationFailed(String),
    PrevalidationError(String),
    /// Admission control rejected transaction (quota/cap exceeded)
    AdmissionRejected(String),
}

impl std::fmt::Display for TransactionProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionProcessError::PrevalidationFailed(s) => {
                write!(f, "PrevalidationFailed: {}", s)
            }
            TransactionProcessError::PrevalidationError(s) => {
                write!(f, "PrevalidationError: {}", s)
            }
            TransactionProcessError::AdmissionRejected(s) => write!(f, "AdmissionRejected: {}", s),
        }
    }
}

/// Integrated mempool pipeline
/// Manages the complete flow from raw transactions to block production
///
/// # Thread Safety
///
/// `MempoolPipeline` is fully thread-safe and can be safely shared across multiple threads:
/// - Uses `Arc` for shared ownership of all components
/// - Uses `Mutex` for internal synchronization (admission control, mempool core, tx storage)
/// - Safe to use with async RPC operations (tokio) - operations are fast enough that blocking is minimal
/// - Storage is shared via `Arc<dyn StorageTrait>` which is thread-safe (RocksDB MultiThreaded mode)
///
/// # Async Compatibility
///
/// While `MempoolPipeline` uses `std::sync::Mutex` (blocking), it's safe for async use because:
/// - Operations are fast (<1ms typically) - won't block async runtime significantly
/// - For high-throughput scenarios, consider using `tokio::sync::Mutex` in future optimizations
/// - Current implementation prioritizes simplicity and correctness over async-native primitives
///
/// # Storage Sharing
///
/// Storage is shared correctly:
/// - `Arc<dyn StorageTrait>` is passed to `MempoolPipeline::new()` and cloned internally
/// - Storage is thread-safe (RocksDB MultiThreaded mode) and can be accessed concurrently
/// proposed block's height and the insertion timestamp alongside the drained
/// TXs, so the cert-MATCHED handler can restore TXs of *other* blocks the node
/// proposed at the same height (Path C), and a background worker can restore
/// entries older than a timeout (Path B).
pub(crate) struct InFlightEntry {
    pub height: u64,
    pub inserted_at: std::time::Instant,
    pub txs: Vec<MempoolTx>,
}

pub struct MempoolPipeline {
    pub(crate) prevalidator: Arc<Prevalidator>,
    /// Admission control (shared, synchronized via Mutex)
    #[allow(dead_code)] // Kept for future use and to maintain reference
    admission: Arc<Mutex<AdmissionControl>>,
    /// Mempool core (shared, synchronized via Mutex)
    mempool: Arc<Mutex<Mempool>>,
    tx_storage: Arc<Mutex<crate::mempool::prevalidation::TxStorage>>,
    storage: Arc<dyn StorageTrait>,
    /// Current block height (for replay prevention in dispatcher)
    current_block_height: Arc<Mutex<u64>>,
    /// Execution dispatcher configuration (optional, for fee-aware scheduling)
    dispatcher_config: Option<DispatcherConfig>,
    /// Pipeline prefetcher for lookahead buffer (optional, for overlap I/O/CPU)
    pipeline_prefetcher: Option<Arc<PipelinePrefetcher>>,
    /// Current batch ID for lookahead coordination
    current_batch_id: Arc<Mutex<u64>>,
    /// Mempool state tracking for adaptive weights
    mempool_state_tracker: Arc<Mutex<MempoolStateTracker>>,
    /// Pending nonces: tracks the next expected nonce per sender_id for TXs that have been
    /// when multiple drain rounds happen before block commitment.
    // pending_nonces removed: was causing stale nonce state when proposed blocks
    /// ROUND 13: In-flight TXs — drained from mempool but not yet committed to a finalized block.
    /// On proposer change, these are restored to the mempool so they can be re-proposed
    /// by the next proposer. Prevents permanent TX loss on rotation/epoch transitions.
    in_flight_txs: Arc<Mutex<Vec<MempoolTx>>>,
    /// orphan-on-eviction bug (see memory/in_flight_orphan_bug.md): when a
    /// proposed block is evicted without its BFT certificate, we restore
    /// only that block's TXs to the mempool, and when a different block
    /// receives a certificate we clear only that block's TXs — avoiding
    /// the all-or-nothing wipe of the legacy `in_flight_txs` vec.
    ///
    /// height and the insertion timestamp. Height enables event-driven
    /// restore when a *different* block at the same height receives a
    /// certificate (multi-group fork case observed on testnet ln-4: chain
    /// commits blocks of other groups, this node's proposed-but-not-winning
    /// blocks were orphaned). Timestamp enables a timeout-based safety net.
    in_flight_by_block: Arc<Mutex<std::collections::HashMap<[u8; 64], InFlightEntry>>>,
    /// Shard filter: when set, drain_for_block_production only returns TX
    /// whose sender belongs to one of the specified shards. This ensures
    /// each group only proposes blocks with TX for its assigned shards.
    /// Set to None for legacy (unsharded) mode.
    shard_filter: Arc<std::sync::RwLock<Option<ShardFilter>>>,
    /// Optional sink for per-client FL contribution scores produced by
    /// `aggregate_federated_updates`. When wired, the lightnode forwards
    /// each `(peer_hex, round_id, score_permille)` tuple into the
    /// `ObservationStore::record_fl_contribution` so the PoU scorer sees
    /// the FL trust signal. Decoupled via callback to avoid a hard
    /// `savitri-consensus` dependency from the mempool crate.
    fl_score_sink: Arc<std::sync::RwLock<Option<Arc<dyn Fn(&str, u64, u16) + Send + Sync>>>>,
    /// Optional provider returning the current PoU score (permille) for
    /// a given peer hex. When wired, FedAvg weights become
    /// `weight_i = (tx.amount / 1e18) * (pou_score_i / 1000)` so the same
    /// observation surface that gates consensus eligibility also
    /// modulates FL aggregation strength. Returns 1.0 modifier when the
    /// provider is missing.
    pou_score_provider: Arc<std::sync::RwLock<Option<Arc<dyn Fn(&str) -> u16 + Send + Sync>>>>,
}

/// Shard filter configuration for block production
#[derive(Clone)]
pub struct ShardFilter {
    /// Total number of shards
    pub num_shards: usize,
    /// Shard IDs assigned to this group
    pub local_shards: std::collections::HashSet<u32>,
}

impl ShardFilter {
    /// Check if a sender address belongs to this group's shards.
    ///
    /// `savitri_core::sharding::shard_for_sender` (canonical). Previously the
    /// DefaultHasher recipe was inlined here and in two other places (lightnode
    /// `tx_router::resolution::shard_for_sender`, mempool
    /// `ShardRouter::route_to_shard`). All three now go through one helper so
    /// the routing→filter parity is enforced by the type system rather than
    /// by parallel comments.
    pub fn is_local(&self, sender_address: &[u8]) -> bool {
        let shard =
            savitri_core::sharding::shard_for_sender(sender_address, self.num_shards as u32);
        self.local_shards.contains(&shard)
    }
}

impl MempoolPipeline {
    /// Lock mempool with error handling and recovery
    pub fn lock_mempool(&self) -> Result<MutexGuard<'_, Mempool>, TransactionProcessError> {
        self.mempool.lock().map_err(|_poisoned| {
            // Log error for monitoring/debugging
            eprintln!(
                "WARNING: Mempool mutex poisoned - another thread panicked while holding the lock. \
                 This indicates a serious bug. Attempting recovery..."
            );
            // Use poisoned mutex for recovery (Rust allows this)
            // This allows the system to continue operating
            TransactionProcessError::PrevalidationError(
                "internal error: mempool mutex poisoned (system attempting recovery)".to_string(),
            )
        })
    }

    /// Helper function to lock mempool with automatic recovery
    ///
    /// For cases where we cannot return an error (e.g., drain_for_block_production),
    /// this function automatically recovers from mutex poisoning by using the poisoned mutex.
    /// Returns the guard even if mutex was poisoned (allows system to continue operating).
    fn lock_mempool_with_recovery(&self) -> MutexGuard<'_, Mempool> {
        match self.mempool.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Mutex is poisoned - log error and recover by using poisoned mutex
                eprintln!(
                    "WARNING: Mempool mutex poisoned - another thread panicked while holding the lock. \
                     Attempting recovery by using poisoned mutex..."
                );
                // Rust allows using poisoned mutex for recovery
                // This allows the system to continue operating even if a thread panicked
                poisoned.into_inner()
            }
        }
    }

    /// Helper function to lock tx_storage with graceful error handling
    /// Made public for use in helper functions
    pub(crate) fn lock_tx_storage(
        tx_storage: &Arc<Mutex<crate::mempool::prevalidation::TxStorage>>,
    ) -> Result<MutexGuard<'_, crate::mempool::prevalidation::TxStorage>, TransactionProcessError>
    {
        tx_storage.lock().map_err(|_poisoned| {
            eprintln!(
                "WARNING: TxStorage mutex poisoned - another thread panicked while holding the lock. \
                 Attempting recovery..."
            );
            TransactionProcessError::PrevalidationError(
                "internal error: tx storage mutex poisoned (system attempting recovery)".to_string(),
            )
        })
    }

    /// Create a new mempool pipeline with all components initialized
    pub fn new(storage: Arc<dyn StorageTrait>) -> Self {
        Self::with_admission_config(storage, AdmissionConfig::default())
    }

    /// Create a new mempool pipeline with custom admission control configuration
    pub fn with_admission_config(
        storage: Arc<dyn StorageTrait>,
        admission_config: AdmissionConfig,
    ) -> Self {
        Self::with_config(storage, admission_config, None)
    }

    /// Create a new mempool pipeline with custom admission control and dispatcher configuration
    pub fn with_config(
        storage: Arc<dyn StorageTrait>,
        admission_config: AdmissionConfig,
        dispatcher_config: Option<DispatcherConfig>,
    ) -> Self {
        Self::with_full_config(storage, admission_config, dispatcher_config, false)
    }

    /// Create a new mempool pipeline with full configuration including pipeline prefetcher
    pub fn with_full_config(
        storage: Arc<dyn StorageTrait>,
        admission_config: AdmissionConfig,
        dispatcher_config: Option<DispatcherConfig>,
        enable_pipeline_prefetch: bool,
    ) -> Self {
        let prevalidator = Arc::new(Prevalidator::new(storage.clone()));

        let tx_storage = prevalidator.tx_storage.clone();

        // Create admission control with custom config and storage reference
        let admission = Arc::new(Mutex::new(AdmissionControl::with_storage(
            admission_config,
            storage.clone(),
        )));

        // Create mempool core
        let mempool = Arc::new(Mutex::new(Mempool::new(admission.clone())));

        // Create pipeline prefetcher if enabled
        let pipeline_prefetcher = if enable_pipeline_prefetch {
            Some(Arc::new(PipelinePrefetcher::new(storage.clone())))
        } else {
            None
        };

        // Create mempool state tracker
        let mempool_state_tracker = Arc::new(Mutex::new(MempoolStateTracker::new()));

        Self {
            prevalidator,
            admission,
            mempool,
            tx_storage,
            storage,
            current_block_height: Arc::new(Mutex::new(0)),
            dispatcher_config,
            pipeline_prefetcher,
            current_batch_id: Arc::new(Mutex::new(0)),
            mempool_state_tracker,
            // pending_nonces removed (see field comment above)
            in_flight_txs: Arc::new(Mutex::new(Vec::new())),
            in_flight_by_block: Arc::new(Mutex::new(std::collections::HashMap::new())),
            shard_filter: Arc::new(std::sync::RwLock::new(None)),
            fl_score_sink: Arc::new(std::sync::RwLock::new(None)),
            pou_score_provider: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// Wire a sink that receives per-client FL contribution scores from
    /// the robust aggregation pipeline. Idempotent. Used by the lightnode
    /// `main.rs` to forward scores into the PoU `ObservationStore`.
    pub fn set_fl_score_sink(&self, sink: Arc<dyn Fn(&str, u64, u16) + Send + Sync>) {
        if let Ok(mut slot) = self.fl_score_sink.write() {
            *slot = Some(sink);
        }
    }

    /// Wire a provider that returns the current PoU score (permille) for
    /// a peer hex id. Used by `aggregate_federated_updates` to scale
    /// FedAvg weights by the same trust score that gates consensus
    /// eligibility. Idempotent.
    pub fn set_pou_score_provider(&self, provider: Arc<dyn Fn(&str) -> u16 + Send + Sync>) {
        if let Ok(mut slot) = self.pou_score_provider.write() {
            *slot = Some(provider);
        }
    }

    /// Set shard filter for block production (called when group receives shard assignment).
    pub fn set_shard_filter(&self, num_shards: usize, local_shards: Vec<u32>) {
        let filter = ShardFilter {
            num_shards,
            local_shards: local_shards.into_iter().collect(),
        };
        *self.shard_filter.write().unwrap() = Some(filter);
        tracing::info!(num_shards, "Shard filter activated for block production");
    }

    /// Clear shard filter (unsharded mode)
    pub fn clear_shard_filter(&self) {
        *self.shard_filter.write().unwrap() = None;
    }

    /// ROUND 13: Restore in-flight TXs back to the mempool.
    /// Called when the proposer changes (rotation, epoch transition, disconnect).
    /// TXs that were drained but never committed to a finalized block are put back
    /// so the next proposer can include them.
    pub fn restore_in_flight_txs(&self) {
        let txs = {
            let mut in_flight = self.in_flight_txs.lock().unwrap();
            std::mem::take(&mut *in_flight)
        };
        if txs.is_empty() {
            return;
        }
        // account.nonce (storage). Durante la finestra tra drain e BFT
        // failure, altri blocchi possono aver avanzato lo storage_nonce del
        // sender (via committing TX arrivate per gossip/fetch). Without questo
        // le scarta come nonce mismatch, invalid_handles le fa tornare al
        // ready_vec via restore, loop infinito → 2772 warn/test osservati.
        // Il filter usa una lookup batch (dedup per sender) per limitare
        // l'overhead di storage.get_account a ≤ N_senders chiamate.
        let count_before = txs.len();
        let mut unique_senders: std::collections::HashMap<Vec<u8>, u64> =
            std::collections::HashMap::new();
        for tx in &txs {
            if !unique_senders.contains_key(&tx.sender_address) {
                let nonce = match self.storage.get_account(&tx.sender_address) {
                    Ok(Some(account_bytes)) => bincode::deserialize::<Account>(&account_bytes)
                        .map(|a| a.nonce)
                        .or_else(|_| Account::decode(&account_bytes).map(|a| a.nonce))
                        .unwrap_or(0),
                    _ => 0,
                };
                unique_senders.insert(tx.sender_address.clone(), nonce);
            }
        }
        let (keep, drop_stale): (Vec<_>, Vec<_>) = txs.into_iter().partition(|tx| {
            let sn = unique_senders.get(&tx.sender_address).copied().unwrap_or(0);
            tx.nonce >= sn
        });
        let dropped_stale = drop_stale.len();
        if dropped_stale > 0 {
            // Registra removal di ciascuna TX scartata così admitted_nonces si
            let mut adm = self.admission.lock().unwrap();
            for tx in &drop_stale {
                adm.record_removal(tx.sender_id, tx.class, tx.tx_hash);
            }
            crate::mempool::metrics::increment_removal_batch(dropped_stale as u64);
        }
        let count = keep.len();
        if count == 0 {
            tracing::info!(
                dropped_stale,
                count_before,
                "ROUND 13: all in-flight TXs were stale vs storage nonce — none restored"
            );
            return;
        }
        let mut mp = self.lock_mempool_with_recovery();
        mp.restore_drained_txs(keep);
        tracing::info!(
            restored_count = count,
            dropped_stale,
            count_before,
            "ROUND 13: Restored in-flight TXs to mempool (proposer changed before commit)"
        );
    }

    /// ROUND 13: Clear in-flight TXs after a block is committed.
    /// Called when the MN certificate confirms the block — TXs are now permanently finalized.
    pub fn clear_in_flight_txs(&self) {
        let mut in_flight = self.in_flight_txs.lock().unwrap();
        let count = in_flight.len();
        if count > 0 {
            in_flight.clear();
            tracing::debug!(cleared = count, "Cleared in-flight TXs (block committed)");
        }
    }

    /// Called by the proposer after build_block has computed the block's hash,
    /// so that subsequent commit/evict events can target this block's TXs
    /// precisely instead of wiping the whole in_flight_txs vec.
    ///
    /// Idempotent: calling twice with the same hash replaces the entry.
    ///
    /// handler can identify and restore orphaned TXs of *other* blocks
    /// proposed by this node at the same height (multi-group fork case).
    pub fn record_in_flight_for_block(
        &self,
        block_hash: [u8; 64],
        height: u64,
        txs: Vec<MempoolTx>,
    ) {
        if txs.is_empty() {
            return;
        }
        let count = txs.len();
        let mut by_block = self.in_flight_by_block.lock().unwrap();
        by_block.insert(
            block_hash,
            InFlightEntry {
                height,
                inserted_at: std::time::Instant::now(),
                txs,
            },
        );
        // triplet is visible at default log level — the triplet is how we
        // verify the orphan-on-eviction fix keeps working under load without
        tracing::info!(
            block_hash = %hex::encode(&block_hash[..8]),
            height,
            txs = count,
            total_blocks_in_flight = by_block.len(),
            "Recorded in-flight TXs for proposed block"
        );
    }

    /// certificate has arrived and the block is now finalized. Does NOT touch
    /// other blocks' in-flight entries — this is the key difference from
    /// `clear_in_flight_txs()`, which wipes the legacy unified vec and thus
    /// orphans the drained TXs of other still-pending proposed blocks.
    pub fn clear_in_flight_for_block(&self, block_hash: &[u8; 64]) {
        let mut by_block = self.in_flight_by_block.lock().unwrap();
        if let Some(entry) = by_block.remove(block_hash) {
            tracing::info!(
                block_hash = %hex::encode(&block_hash[..8]),
                txs = entry.txs.len(),
                remaining_blocks_in_flight = by_block.len(),
                "Cleared in-flight TXs for committed block"
            );
        }
    }

    /// because the block was evicted without ever receiving its BFT certificate
    /// (typically 300s timeout on PendingBlockTracker). Without this hook,
    /// drained TXs for never-finalised blocks become permanent orphans — see
    /// memory/in_flight_orphan_bug.md.
    ///
    /// Returns the number of TXs restored (0 if no entry existed).
    pub fn restore_in_flight_for_block(&self, block_hash: &[u8; 64]) -> usize {
        let entry = {
            let mut by_block = self.in_flight_by_block.lock().unwrap();
            match by_block.remove(block_hash) {
                Some(entry) => entry,
                None => return 0,
            }
        };
        let count = entry.txs.len();
        if count == 0 {
            return 0;
        }
        let mut mp = self.lock_mempool_with_recovery();
        mp.restore_drained_txs(entry.txs);
        tracing::info!(
            block_hash = %hex::encode(&block_hash[..8]),
            height = entry.height,
            restored = count,
            "Restored in-flight TXs for evicted block (no BFT certificate received)"
        );
        count
    }

    /// block at `committed_height` whose hash differs from any of this node's
    /// own proposed blocks at that height, those locally-proposed blocks will
    /// never receive a certificate (another group/proposer won the height).
    /// Their drained TXs would otherwise sit in `in_flight_by_block` until the
    /// 300s `PendingBlockTracker` timeout, during which the loadtest /
    /// real client keeps incrementing nonces optimistically and the pool
    /// for nonce gap — see memory/bug50_next_step.md).
    ///
    /// This method scans `in_flight_by_block` for entries whose `height`
    /// matches `committed_height` and whose hash is NOT `committed_hash`,
    /// removes them, and restores their TXs to the mempool so they can be
    /// re-proposed. Returns total TXs restored across all matching entries.
    pub fn restore_orphaned_at_height(
        &self,
        committed_height: u64,
        committed_hash: &[u8; 64],
    ) -> usize {
        let orphaned: Vec<([u8; 64], InFlightEntry)> = {
            let mut by_block = self.in_flight_by_block.lock().unwrap();
            let to_remove: Vec<[u8; 64]> = by_block
                .iter()
                .filter(|(hash, entry)| entry.height == committed_height && *hash != committed_hash)
                .map(|(hash, _)| *hash)
                .collect();
            to_remove
                .into_iter()
                .filter_map(|h| by_block.remove(&h).map(|e| (h, e)))
                .collect()
        };
        if orphaned.is_empty() {
            return 0;
        }
        let mut total = 0usize;
        let mut mp = self.lock_mempool_with_recovery();
        for (hash, entry) in &orphaned {
            let count = entry.txs.len();
            if count == 0 {
                continue;
            }
            mp.restore_drained_txs(entry.txs.clone());
            total += count;
            tracing::info!(
                committed_height,
                orphan_hash = %hex::encode(&hash[..8]),
                committed_hash = %hex::encode(&committed_hash[..8]),
                restored = count,
                "Restored orphaned in-flight TXs (other block won this height)"
            );
        }
        total
    }

    /// `in_flight_by_block` for entries older than `max_age` and restores
    /// their TXs to the mempool. Catches every orphan cause not covered by
    /// `restore_orphaned_at_height` (proposer crash, cert dropped on the
    /// network, partitions, etc.). Returns total TXs restored.
    ///
    /// Should be invoked periodically (e.g., every 10s) by a background
    /// task with `max_age` > BFT round-trip (recommended ≥ 30s).
    pub fn restore_in_flight_older_than(&self, max_age: std::time::Duration) -> usize {
        let now = std::time::Instant::now();
        let stale: Vec<([u8; 64], InFlightEntry)> = {
            let mut by_block = self.in_flight_by_block.lock().unwrap();
            let to_remove: Vec<[u8; 64]> = by_block
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.inserted_at) >= max_age)
                .map(|(h, _)| *h)
                .collect();
            to_remove
                .into_iter()
                .filter_map(|h| by_block.remove(&h).map(|e| (h, e)))
                .collect()
        };
        if stale.is_empty() {
            return 0;
        }
        let mut total = 0usize;
        let mut mp = self.lock_mempool_with_recovery();
        for (hash, entry) in &stale {
            let count = entry.txs.len();
            if count == 0 {
                continue;
            }
            mp.restore_drained_txs(entry.txs.clone());
            total += count;
            tracing::warn!(
                block_hash = %hex::encode(&hash[..8]),
                height = entry.height,
                age_secs = now.duration_since(entry.inserted_at).as_secs(),
                restored = count,
                "Restored stale in-flight TXs (no commit/evict event within timeout)"
            );
        }
        total
    }

    /// Enable execution dispatcher with default configuration
    pub fn with_dispatcher(mut self) -> Self {
        self.dispatcher_config = Some(DispatcherConfig::default());
        self
    }

    /// Enable execution dispatcher with custom configuration
    pub fn with_dispatcher_config(mut self, config: DispatcherConfig) -> Self {
        self.dispatcher_config = Some(config);
        self
    }

    /// Enable pipeline prefetcher for lookahead buffer (overlap I/O/CPU)
    pub fn with_pipeline_prefetch(self, _storage: Arc<dyn StorageTrait>) -> Self {
        // self.pipeline_prefetcher = Some(Arc::new(PipelinePrefetcher::new(storage)));
        self
    }

    /// Enable mempool state tracking with custom configuration
    pub fn with_state_tracking(mut self, max_history_size: usize, update_interval: usize) -> Self {
        self.mempool_state_tracker = Arc::new(Mutex::new(MempoolStateTracker::with_config(
            max_history_size,
            update_interval,
        )));
        self
    }

    /// Get mempool state tracker reference
    pub fn get_state_tracker(&self) -> Arc<Mutex<MempoolStateTracker>> {
        self.mempool_state_tracker.clone()
    }

    /// Track mempool state for adaptive weights
    pub fn track_mempool_state(&self, mempool_txs: &[MempoolTx]) {
        if let Ok(mut tracker) = self.mempool_state_tracker.lock() {
            // Create mempool state from current transactions
            let mut fee_distribution = Vec::with_capacity(mempool_txs.len());
            let mut class_distribution = Vec::with_capacity(mempool_txs.len());

            for tx in mempool_txs {
                fee_distribution.push(tx.fee);
                class_distribution.push(tx.class);
            }

            // Mock historical throughput (in production, use real data)
            let historical_throughput = vec![950.0, 1050.0, 980.0, 1100.0, 1020.0];

            let state = MempoolState {
                fee_distribution,
                class_distribution,
                historical_throughput,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };

            tracker.add_state(state);
        }
    }

    /// Check if weights should be updated based on block count
    pub fn should_update_weights(&self) -> bool {
        if let Ok(tracker) = self.mempool_state_tracker.lock() {
            tracker.should_update_weights()
        } else {
            false
        }
    }

    /// Get aggregate mempool statistics
    pub fn get_aggregate_stats(&self) -> Option<MempoolAggregateStats> {
        if let Ok(tracker) = self.mempool_state_tracker.lock() {
            tracker.calculate_aggregate_stats()
        } else {
            None
        }
    }

    /// Return a point-in-time snapshot of mempool counters for RPC/monitoring.
    pub fn stats_snapshot(&self) -> MempoolStatsSnapshot {
        use crate::mempool::metrics;
        MempoolStatsSnapshot {
            pending: metrics::get_pending_tx_count(),
            queued: metrics::get_ready_tx_count(),
            total: metrics::get_mempool_size(),
            admitted_total: metrics::get_admission_count(),
            queued_total: metrics::get_admission_count(),
            rejected_total: metrics::get_rejection_count(),
            removed_total: metrics::get_removal_count(),
            evicted_total: metrics::get_eviction_count(),
            confirmed_total: metrics::get_confirmed_count(),
        }
    }

    /// Process raw transactions from Network/RPC layer
    /// Returns number of successfully accepted transactions
    pub async fn process_raw_transactions(&self, raw_txs: Vec<RawTx>) -> usize {
        if raw_txs.is_empty() {
            return 0;
        }

        let prevalidation_results = self.prevalidator.prevalidate_batch(raw_txs).await;

        let mut reject_invalid_nonce = 0usize;
        let mut reject_signature = 0usize;
        let mut reject_duplicate = 0usize;
        let mut reject_overflow = 0usize;
        let mut reject_other = 0usize;
        let prevalidated: Vec<PrevalidatedTx> = prevalidation_results
            .into_iter()
            .filter_map(|r| match r {
                PrevalidationResult::Valid(pv) => Some(pv),
                PrevalidationResult::Invalid(reason) => {
                    if reason.contains("invalid nonce") {
                        reject_invalid_nonce += 1;
                    } else if reason.contains("signature") {
                        reject_signature += 1;
                    } else if reason.contains("duplicate") {
                        reject_duplicate += 1;
                    } else if reason.contains("overflow") {
                        reject_overflow += 1;
                    } else {
                        reject_other += 1;
                    }
                    None
                }
            })
            .collect();

        if prevalidated.is_empty() {
            tracing::warn!(
                total_input = reject_invalid_nonce
                    + reject_signature
                    + reject_duplicate
                    + reject_overflow
                    + reject_other,
                reject_invalid_nonce,
                reject_signature,
                reject_duplicate,
                reject_overflow,
                reject_other,
                "Prevalidation: ALL transactions rejected (breakdown by reason)"
            );
            return 0;
        }

        // Step 3: Add to mempool (admission check happens inside)
        let results = match self.lock_mempool() {
            Ok(mut mp) => mp.add_prevalidated_batch(prevalidated),
            Err(_) => {
                // Mutex poisoned - return 0 (no transactions accepted)
                // Error already logged in lock_mempool()
                return 0;
            }
        };

        // Count transactions by outcome (admitted to main pool + queued for future nonce)
        let admitted = results
            .iter()
            .filter(|r| matches!(r, crate::mempool::core::AdmissionOutcome::Admitted))
            .count();
        let queued = results
            .iter()
            .filter(|r| matches!(r, crate::mempool::core::AdmissionOutcome::Queued))
            .count();
        if queued > 0 {
            tracing::info!(
                admitted = admitted,
                queued = queued,
                rejected = results.len() - admitted - queued,
                "Mempool admission breakdown: admitted (main pool) + queued (future nonce)"
            );
        }
        // Return admitted + queued as total "accepted" count (both are successfully stored)
        admitted + queued
    }

    /// Process a single raw transaction from RPC layer
    /// Returns Result with transaction hash on success, or error details on failure
    ///
    /// This is optimized for RPC use cases where we need detailed error information
    /// and transaction hash for the response.
    pub async fn process_single_raw_transaction(
        &self,
        raw_tx: RawTx,
    ) -> Result<[u8; 32], TransactionProcessError> {
        let prevalidation_result = self.prevalidator.prevalidate(raw_tx.clone()).await;

        let prevalidated = match prevalidation_result {
            Ok(PrevalidationResult::Valid(pv)) => pv,
            Ok(PrevalidationResult::Invalid(reason)) => {
                return Err(TransactionProcessError::PrevalidationFailed(reason));
            }
            Err(e) => {
                return Err(TransactionProcessError::PrevalidationError(format!(
                    "{}",
                    e
                )));
            }
        };

        // Step 3: Add to mempool (admission check happens inside)
        // Calculate hash first for duplicate detection
        let tx_hash = hash_signed_tx_bytes(&raw_tx.bytes);
        // is about to write to. Compared against the pointer logged in
        // drain_for_block_production: if they differ, RPC and proposer are
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static RPC_PTR_CTR: AtomicU64 = AtomicU64::new(0);
            let n = RPC_PTR_CTR.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 500 == 0 {
                tracing::warn!(
                    rpc_ptr_call = n,
                    mempool_arc_ptr = format_args!("{:p}", std::sync::Arc::as_ptr(&self.mempool)),
                    "DIAG[#50]: RPC path mempool Arc pointer"
                );
            }
        }
        let result = {
            let mut mp = self.lock_mempool()?;
            // anche se shard_filter classificherebbe il sender come "remote".
            mp.add_prevalidated_with_source(prevalidated, Some(tx_hash), true)
        };

        // Step 4: Check admission result
        match result {
            crate::mempool::core::AdmissionOutcome::Admitted => {
                // Success: transaction in main pool, return hash
                Ok(tx_hash)
            }
            crate::mempool::core::AdmissionOutcome::Queued => {
                // Queued for future nonce - still a success from the caller's perspective
                Ok(tx_hash)
            }
            crate::mempool::core::AdmissionOutcome::Rejected(reason) => {
                // Admission rejected
                Err(TransactionProcessError::AdmissionRejected(format!(
                    "transaction rejected by admission control: {}",
                    reason
                )))
            }
        }
    }

    /// Drain transactions for block production
    /// Returns (mempool_txs, signed_txs) - both are needed for execution and cleanup
    ///
    /// # Error Handling
    ///
    /// If mutex is poisoned, automatically recovers and continues operation (graceful degradation)
    ///
    /// # Execution Dispatcher
    ///
    /// If dispatcher is enabled (via `with_dispatcher()` or `with_dispatcher_config()`),
    /// transactions are scheduled using fee-aware algorithm with adaptive weights.
    /// Otherwise, uses original round-robin fairness scheduling.
    ///
    /// # Adaptive Weights Integration
    ///
    /// - Tracks mempool state for historical analysis
    /// - Updates weights every K blocks based on aggregate statistics
    /// - Uses smoothing to avoid oscillations
    ///
    /// # Pipeline Prefetching
    ///
    /// If pipeline prefetcher is enabled, triggers lookahead prefetch for batch N+1
    /// after extracting batch N, enabling overlap I/O/CPU.
    pub fn drain_for_block_production(&self, max_txs: usize) -> (Vec<MempoolTx>, Vec<SignedTx>) {
        // Step 1: Drain from mempool (round-robin, class-aware)
        // Use recovery helper since we cannot return an error from this function
        let drain_t0 = std::time::Instant::now();
        // pointers don't match, the proposer is draining a different Mempool
        // instance than the one the RPC consumer writes to — which would be
        {
            use std::sync::atomic::{AtomicU64, Ordering};
            static DRAIN_PTR_CTR: AtomicU64 = AtomicU64::new(0);
            let n = DRAIN_PTR_CTR.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 500 == 0 {
                tracing::warn!(
                    drain_ptr_call = n,
                    mempool_arc_ptr = format_args!("{:p}", std::sync::Arc::as_ptr(&self.mempool)),
                    "DIAG[#50]: drain path mempool Arc pointer"
                );
            }
        }
        let (pool_total_before, rv_len, sum_queue, non_empty_queues) = {
            let mp = self.lock_mempool_with_recovery();
            mp.diag_state()
        };
        let mut mempool_txs = {
            let mut mp = self.lock_mempool_with_recovery();
            mp.drain_fair_batch(max_txs)
        };
        let after_fair = mempool_txs.len();

        // Logs every 50th call to keep volume sustainable. Aggregates the three
        // critical signals: pool size, fair-batch yield, max_txs requested.
        // Stage 3: also logs ready_vec.len, sum(queue.len), non_empty_queues to
        // detect divergence between the cached `self.total` counter (==
        // pool_total_before) and the actual transactions summed from queues.
        {
            // (no rate-limit). The rate-limited tracing::warn below stays as a
            crate::mempool::metrics::observe_drain(max_txs, pool_total_before, after_fair);

            use std::sync::atomic::{AtomicU64, Ordering};
            static DRAIN_CTR: AtomicU64 = AtomicU64::new(0);
            let n = DRAIN_CTR.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n % 50 == 0 {
                tracing::warn!(
                    drain_call = n,
                    max_txs,
                    pool_total_before,
                    rv_len,
                    sum_queue,
                    non_empty_queues,
                    fair_batch_out = after_fair,
                    "DIAG[#50]: drain_for_block_production stage 1 — drain_fair_batch"
                );
            }
        }

        if mempool_txs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // nonces per sender. Without this, gossip-received TX arrive out of order and
        // the per-sender queue has nonce 50 before nonce 3 (FIFO insertion).
        mempool_txs.sort_by(|a, b| a.sender_id.cmp(&b.sender_id).then(a.nonce.cmp(&b.nonce)));

        // Shard filter: keep only TX whose sender belongs to our group's shards.
        // TX from other shards are returned to the mempool for the correct group to process.
        //
        // Le TX entrate via tx_sendTransaction RPC locale are incluse nel drain
        // ai proposer remoti non funziona (added=0 osservato) — refactor in
        // Opzione C/B/D of the piano an earlier fix.
        if let Ok(guard) = self.shard_filter.read() {
            if let Some(ref filter) = *guard {
                let before = mempool_txs.len();
                // Count breakdown for diag: how many kept by rpc_accepted vs is_local vs neither
                let mut kept_rpc = 0usize;
                let mut kept_local = 0usize;
                let mut dropped_remote = 0usize;
                let (local_txs, remote_txs): (Vec<_>, Vec<_>) =
                    mempool_txs.into_iter().partition(|tx| {
                        if tx.rpc_accepted {
                            kept_rpc += 1;
                            true
                        } else if filter.is_local(&tx.sender_address) {
                            kept_local += 1;
                            true
                        } else {
                            dropped_remote += 1;
                            false
                        }
                    });
                mempool_txs = local_txs;
                let after = mempool_txs.len();
                let local_shards_count = filter.local_shards.len();

                // Return remote TX to mempool (they'll be drained by their group's proposer)
                if !remote_txs.is_empty() {
                    let mut mp = self.lock_mempool_with_recovery();
                    mp.restore_drained_txs(remote_txs);
                }

                {
                    // Tier 8: Prometheus counters on every filter pass.
                    crate::mempool::metrics::observe_shard_filter(
                        kept_rpc,
                        kept_local,
                        dropped_remote,
                    );

                    use std::sync::atomic::{AtomicU64, Ordering};
                    static FILTER_CTR: AtomicU64 = AtomicU64::new(0);
                    let n = FILTER_CTR.fetch_add(1, Ordering::Relaxed) + 1;
                    if n == 1 || n % 50 == 0 {
                        tracing::warn!(
                            filter_call = n,
                            before,
                            after,
                            kept_rpc_accepted = kept_rpc,
                            kept_is_local = kept_local,
                            dropped_remote_shard = dropped_remote,
                            num_shards = filter.num_shards,
                            local_shards_count,
                            "DIAG[#50]: drain stage 2 — shard_filter breakdown"
                        );
                    }
                }
            } else {
                // No shard filter configured — all TX pass through.
                // Tier 8: Prometheus counter on every no-filter drain.
                crate::mempool::metrics::inc_drain_no_filter();

                use std::sync::atomic::{AtomicU64, Ordering};
                static NOFILTER_CTR: AtomicU64 = AtomicU64::new(0);
                let n = NOFILTER_CTR.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 1 || n % 50 == 0 {
                    tracing::warn!(
                        nofilter_call = n,
                        passthrough = mempool_txs.len(),
                        "DIAG[#50]: drain stage 2 — no shard filter (all TX pass)"
                    );
                }
            }
        }
        let _ = drain_t0; // reserved for future timing diag

        // These will be restored to the mempool if the proposer changes before commit.
        {
            let mut in_flight = self.in_flight_txs.lock().unwrap();
            in_flight.extend(mempool_txs.iter().cloned());
        }

        // Step 2: Track mempool state for adaptive weights
        self.track_mempool_state(&mempool_txs);

        // Step 3: Retrieve transaction bytes from storage
        let tx_bytes =
            get_tx_bytes_from_handles(&self.tx_storage, &Mempool::extract_handles(&mempool_txs));

        // Step 4: Create transactions from bytes (parallel deserialization).
        // now returns Option<SignedTx>; filter_map drops both missing bytes
        // (None opt_bytes) AND undeserializable bytes (Some(None) from helper).
        // This keeps signed_txs aligned with the surviving subset of mempool_txs;
        // the min_len.take() step at line ~1517 truncates mempool_txs to match.
        use rayon::prelude::*;
        let signed_txs: Vec<SignedTx> = tx_bytes
            .into_par_iter()
            .filter_map(|opt_bytes| {
                let bytes = opt_bytes?;
                create_signed_tx_from_bytes(&bytes)
            })
            .collect();

        // Step 5: Pipeline Prefetching - Trigger lookahead for batch N+1
        if let Some(ref prefetcher) = self.pipeline_prefetcher {
            // Increment batch ID for coordination
            {
                let mut batch_id = self.current_batch_id.lock().unwrap();
                *batch_id += 1;
            }

            // Extract preview of next batch (first K transactions for lookahead)
            let lookahead_txs = self.extract_lookahead_preview(max_txs / 2); // Use half size for lookahead

            if !lookahead_txs.is_empty() {
                // Extract addresses from lookahead transactions
                let mut addresses = Vec::new();
                for tx in &lookahead_txs {
                    addresses.push(tx.from.clone());
                    addresses.push(tx.to.clone());
                }

                // Start prefetch for batch N+1
                let current_batch_id = *self.current_batch_id.lock().unwrap();
                // Prefetching will be implemented when start_lookahead_prefetch method is available
                // if let Err(e) = prefetcher.start_lookahead_prefetch(current_batch_id + 1, addresses) {
                //     log::warn!("Pipeline prefetch failed: {}", e);
                // }
            }
        }

        // Step 6: Apply execution dispatcher with adaptive weights if enabled
        if let Some(ref dispatcher_config) = self.dispatcher_config {
            // Create dispatcher with config (max_txs_per_sender calculated from max_txs)
            let mut config = dispatcher_config.clone();
            config.max_txs_per_sender = max_txs / 10;

            // Check if we should use adaptive weights
            let use_adaptive = self.should_update_weights();
            let mut dispatcher = if use_adaptive {
                // Enable adaptive weights with default config
                ExecutionDispatcher::new(config).with_adaptive_weights(true)
            } else {
                // Use static configuration
                ExecutionDispatcher::new(config).with_adaptive_weights(false)
            };

            // Get current block height for replay prevention
            let current_block_height = *self.current_block_height.lock().unwrap();

            // Convert SignedTx to Transaction for dispatcher
            let transactions: Vec<Transaction> = signed_txs
                .iter()
                .map(|_tx| create_default_transaction())
                .collect();
            let signed_txs_clone = signed_txs.clone(); // Clone before using
            match dispatcher.schedule_transactions_safe(
                mempool_txs,
                transactions,
                self.storage.clone(),
                current_block_height,
            ) {
                Ok((scheduled_mempool, scheduled_signed)) => {
                    // Ensure both vectors have the same length (safety check)
                    let min_len: usize =
                        std::cmp::min(scheduled_mempool.len(), scheduled_signed.len());
                    (
                        scheduled_mempool.into_iter().take(min_len).collect(),
                        // Convert back to SignedTx using cloned vector
                        signed_txs_clone.into_iter().take(min_len).collect(),
                    )
                }
                Err(_) => {
                    // Fallback to empty vectors if scheduling fails
                    // This should not happen in normal operation, but provides graceful degradation
                    (Vec::new(), Vec::new())
                }
            }
        } else {
            // Original behavior: return as-is (round-robin fairness)
            // Ensure both vectors have the same length (safety check)
            let min_len = mempool_txs.len().min(signed_txs.len());
            (
                mempool_txs.into_iter().take(min_len).collect(),
                signed_txs.into_iter().take(min_len).collect(),
            )
        }
    }

    /// Extract preview of next batch for lookahead prefetching
    /// Returns a sample of transactions that would be in the next batch
    fn extract_lookahead_preview(&self, preview_size: usize) -> Vec<SignedTx> {
        // Get a preview of what would be drained next
        let preview_txs = {
            let mut mp = self.lock_mempool_with_recovery();
            mp.peek_fair_batch(preview_size)
        };

        if preview_txs.is_empty() {
            return Vec::new();
        }

        // Get transaction bytes and deserialize
        let tx_bytes =
            get_tx_bytes_from_handles(&self.tx_storage, &Mempool::extract_handles(&preview_txs));

        tx_bytes
            .into_iter()
            .filter_map(|opt_bytes| {
                let bytes = opt_bytes?;
                create_signed_tx_from_bytes(&bytes)
            })
            .collect()
    }

    /// Find transaction handles from signed transactions by matching hashes
    ///
    /// This is used when committing remote blocks where we have signed_txs
    /// but need to find the corresponding handles in the mempool.
    ///
    /// # Returns
    ///
    /// Vector of handles found in mempool. If a transaction is not in mempool
    /// (e.g., remote block), it will not be included in the result.
    pub fn find_handles_from_signed_txs(&self, signed_txs: &[SignedTx]) -> Vec<TxHandle> {
        use crate::executor::dispatcher::hash_signed_tx_bytes;

        // Build hash set for O(1) lookup
        let mut tx_hashes = std::collections::HashSet::new();
        for signed_tx in signed_txs {
            // Serialize transaction for hashing
            let bytes = serialize_signed_tx_for_hash(&signed_tx);
            let hash = hash_signed_tx_bytes(&bytes);
            tx_hashes.insert(hash);
        }

        if tx_hashes.is_empty() {
            return Vec::new();
        }

        // Lock mempool and search for matching transactions
        let mp = self.lock_mempool_with_recovery();

        // Slow-path scan: list all handles, fetch bytes, compare hashes.
        let mempool_handles = mp.all_handles();
        if mempool_handles.is_empty() {
            return Vec::new();
        }

        let tx_bytes = get_tx_bytes_from_handles(&self.tx_storage, &mempool_handles);
        let mut handles = Vec::new();

        for (handle, opt_bytes) in mempool_handles.into_iter().zip(tx_bytes.into_iter()) {
            let Some(bytes) = opt_bytes else {
                continue;
            };

            // Calculate hash of mempool transaction and check if it matches
            let mempool_hash = hash_signed_tx_bytes(&bytes);
            if tx_hashes.contains(&mempool_hash) {
                handles.push(handle);
            }
        }

        handles
    }

    /// Cleanup after block commit
    /// Removes committed transactions and starts new admission round
    /// Also updates account snapshot to reflect latest state after block commit
    ///
    /// # Error Handling
    ///
    /// If mutex is poisoned, automatically recovers and continues operation (graceful degradation)
    /// Snapshot update errors are logged but don't prevent cleanup from proceeding
    pub fn on_block_committed(&self, committed_handles: &[TxHandle]) {
        // Step 1: Remove committed transactions from mempool
        // Use recovery helper since we cannot return an error from this function
        let mut mp = self.lock_mempool_with_recovery();
        mp.on_block_committed_legacy(committed_handles);

        // Step 2: Update account snapshot after block commit
        // Extract addresses from committed transactions and update snapshot
        // This ensures snapshot reflects latest state after block commit
        if !committed_handles.is_empty() {
            // Retrieve transaction bytes from storage to extract addresses
            let tx_bytes = get_tx_bytes_from_handles(&self.tx_storage, committed_handles);

            // Extract unique addresses from committed transactions
            let mut addresses: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();
            for opt_bytes in tx_bytes.iter() {
                if let Some(bytes) = opt_bytes {
                    // Extract addresses from transaction bytes
                    if bytes.len() >= 64 {
                        // Extract from address (first 32 bytes)
                        let from_addr = bytes[0..32].to_vec();
                        addresses.insert(from_addr);

                        // Extract to address (next 32 bytes) if not empty
                        let to_addr = bytes[32..64].to_vec();
                        if !to_addr.iter().all(|&b| b == 0) {
                            // For contract calls, 'to' is contract address
                            // For transfers, 'to' is recipient address
                            // We update snapshot for both sender and recipient
                            addresses.insert(to_addr);
                        }
                    }
                }
            }

            // Update snapshot with addresses that were modified in committed block
            let addresses_vec: Vec<Vec<u8>> = addresses.into_iter().collect();
            if !addresses_vec.is_empty() {
                // Update snapshot asynchronously to avoid blocking cleanup
                // Use tokio spawn if available, otherwise std::thread
                let snapshot_state = self.prevalidator.account_snapshot().clone();
                if tokio::runtime::Handle::try_current().is_ok() {
                    tokio::spawn(async move {
                        if let Err(e) = snapshot_state.update_after_block_commit(&addresses_vec) {
                            eprintln!("Error updating snapshot after block commit: {}", e);
                        }
                    });
                } else {
                    std::thread::spawn(move || {
                        if let Err(e) = snapshot_state.update_after_block_commit(&addresses_vec) {
                            eprintln!("Error updating snapshot after block commit: {}", e);
                        }
                    });
                }
            }
        }
    }

    /// Enhanced block commit handler that also promotes queued transactions.
    ///
    /// When nonce_updates are provided, queued transactions whose nonces
    /// are now ready will be promoted to the main mempool automatically.
    /// If nonce_updates is empty, this function will automatically build it
    /// from the committed transactions by reading account nonces from storage.
    pub fn on_block_committed_with_nonces(
        &self,
        committed_handles: &[TxHandle],
        nonce_updates: &std::collections::HashMap<u32, u64>,
    ) {
        // Step 0: Build nonce_updates automatically if empty
        let nonce_updates = if nonce_updates.is_empty() && !committed_handles.is_empty() {
            self.build_nonce_updates_from_committed_txs(committed_handles)
        } else {
            nonce_updates.clone()
        };

        // Step 1: Remove committed transactions and promote queued ones
        let mut mp = self.lock_mempool_with_recovery();
        mp.on_block_committed(committed_handles, &nonce_updates);

        // Step 1b: Purge stale TX from mempool — remove all TX with nonce < committed nonce
        // for each sender affected by this block. This handles duplicate copies of TX
        // received via gossipsub that remain in the mempool after the original was committed.
        // Without this, stale nonce=0 TX persist and get rejected on every drain cycle.
        if !nonce_updates.is_empty() {
            let stale_handles: Vec<TxHandle> = mp
                .iter_handles_with_nonce()
                .filter(|(sender_id, nonce, _handle)| {
                    if let Some(&committed_nonce) = nonce_updates.get(sender_id) {
                        *nonce < committed_nonce
                    } else {
                        false
                    }
                })
                .map(|(_sid, _nonce, handle)| handle)
                .collect();
            if !stale_handles.is_empty() {
                tracing::info!(
                    stale_count = stale_handles.len(),
                    "Purging stale TX from mempool (nonce < committed nonce)"
                );
                mp.remove_by_handles(&stale_handles);
            }
        }

        // Step 2: Update account snapshot (same as on_block_committed)
        if !committed_handles.is_empty() {
            let tx_bytes = get_tx_bytes_from_handles(&self.tx_storage, committed_handles);
            let mut addresses: std::collections::HashSet<Vec<u8>> =
                std::collections::HashSet::new();
            for opt_bytes in tx_bytes.iter() {
                if let Some(bytes) = opt_bytes {
                    if bytes.len() >= 64 {
                        let from_addr = bytes[0..32].to_vec();
                        addresses.insert(from_addr);
                        let to_addr = bytes[32..64].to_vec();
                        if !to_addr.iter().all(|&b| b == 0) {
                            addresses.insert(to_addr);
                        }
                    }
                }
            }
            let addresses_vec: Vec<Vec<u8>> = addresses.into_iter().collect();
            if !addresses_vec.is_empty() {
                let snapshot_state = self.prevalidator.account_snapshot().clone();
                if tokio::runtime::Handle::try_current().is_ok() {
                    tokio::spawn(async move {
                        if let Err(e) = snapshot_state.update_after_block_commit(&addresses_vec) {
                            eprintln!("Error updating snapshot after block commit: {}", e);
                        }
                    });
                } else {
                    std::thread::spawn(move || {
                        if let Err(e) = snapshot_state.update_after_block_commit(&addresses_vec) {
                            eprintln!("Error updating snapshot after block commit: {}", e);
                        }
                    });
                }
            }
        }
    }

    /// Build nonce_updates map from committed transactions by reading account nonces from storage.
    ///
    /// This function:
    /// 1. Deserializes transactions from handles
    /// 2. Extracts sender addresses
    /// 3. Converts addresses to sender_id using SenderRegistry
    /// 4. Reads new account nonces from storage
    /// 5. Returns a map of sender_id -> new_account_nonce
    fn build_nonce_updates_from_committed_txs(
        &self,
        committed_handles: &[TxHandle],
    ) -> std::collections::HashMap<u32, u64> {
        use crate::mempool::prevalidation::Prevalidator;
        use crate::mempool::types::SignedTx as MempoolSignedTx;

        let mut nonce_updates = std::collections::HashMap::new();

        // Get transaction bytes from handles
        let tx_bytes = get_tx_bytes_from_handles(&self.tx_storage, committed_handles);

        // Extract unique sender addresses and their new nonces
        let mut sender_addresses = std::collections::HashSet::new();

        for opt_bytes in tx_bytes.iter() {
            let Some(bytes) = opt_bytes else {
                continue;
            };

            let tx: MempoolSignedTx = Prevalidator::deserialize_transaction_from_bytes(bytes);

            // Include BOTH sender and receiver: a committed block mutates both
            // (sender: nonce++, balance-; receiver: balance+, may be new account).
            // If we only rebuild nonce for `from`, queued_pool entries keyed on a
            // freshly-funded receiver (e.g. faucet → new sender_X) never get
            // promote() called, and nonce=0,1,2,... stay frozen forever.
            sender_addresses.insert(tx.from.clone());
            sender_addresses.insert(tx.to.clone());
        }

        // For each unique sender address:
        // 1. Get sender_id from registry
        // 2. Read new nonce from storage
        // 3. Add to nonce_updates map
        for sender_address in sender_addresses {
            // Get sender_id from registry (this will allocate if not exists)
            let sender_id = self.prevalidator.get_or_allocate_sender_id(&sender_address);

            // Read new account nonce from storage
            // After block commit, the account nonce should be updated in storage
            let new_nonce = match self.storage.get_account(&sender_address) {
                Ok(Some(account_bytes)) => {
                    // Deserialize account from bytes
                    match bincode::deserialize::<savitri_core::Account>(&account_bytes) {
                        Ok(account) => account.nonce,
                        Err(_) => {
                            // Fallback: try Account::decode
                            match savitri_core::Account::decode(&account_bytes) {
                                Ok(account) => account.nonce,
                                Err(_) => {
                                    tracing::warn!(
                                        sender_address = %hex::encode(&sender_address[..8]),
                                        "Failed to decode account, skipping nonce update"
                                    );
                                    continue;
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Account doesn't exist (shouldn't happen after block commit, but handle gracefully)
                    tracing::debug!(
                        sender_address = %hex::encode(&sender_address[..8]),
                        "Account not found in storage after block commit, skipping nonce update"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        sender_address = %hex::encode(&sender_address[..8]),
                        error = %e,
                        "Failed to read account from storage, skipping nonce update"
                    );
                    continue;
                }
            };

            nonce_updates.insert(sender_id, new_nonce);

            tracing::debug!(
                sender_id = sender_id,
                sender_address = %hex::encode(&sender_address[..8]),
                new_nonce = new_nonce,
                "Built nonce update from committed transaction"
            );
        }

        if !nonce_updates.is_empty() {
            tracing::info!(
                accounts_updated = nonce_updates.len(),
                "Built nonce_updates map from {} committed transactions",
                committed_handles.len()
            );
        }

        nonce_updates
    }

    /// Get sender_id for an address (converts address to sender_id using sender registry)
    /// This is used when building nonce_updates from overlay in lightnode
    pub fn get_sender_id_for_address(&self, address: &[u8]) -> u32 {
        self.prevalidator.get_or_allocate_sender_id(address)
    }

    /// Get mempool size
    ///
    /// # Error Handling
    ///
    /// Returns 0 if mutex is poisoned (graceful degradation)
    pub fn len(&self) -> usize {
        match self.lock_mempool() {
            Ok(mut mp) => mp.len(),
            Err(_) => {
                // Mutex poisoned - return 0 (graceful degradation)
                // Error already logged in lock_mempool()
                0
            }
        }
    }

    /// every counter relevant to the "RPC accept=N but proposer mempool=0"
    /// divergence. Called from a periodic 10s logger spawned in main.rs so the
    /// loadtest run produces a flight recorder showing exactly where TX go.
    /// Returns (main_total, ready_vec_len, sum_queue, non_empty_queues,
    /// queued_total, queued_accounts, queued_promoted, queued_expired,
    /// queued_rej_full, queued_rej_gap).
    pub fn diag_full_state(
        &self,
    ) -> (usize, usize, usize, usize, usize, usize, u64, u64, u64, u64) {
        let (main_total, rv_len, sum_queue, non_empty) = match self.lock_mempool() {
            Ok(mp) => mp.diag_state(),
            Err(_) => (0, 0, 0, 0),
        };
        let qstats = match self.admission.lock() {
            Ok(adm) => adm.queued_pool_stats(),
            Err(_) => return (main_total, rv_len, sum_queue, non_empty, 0, 0, 0, 0, 0, 0),
        };
        (
            main_total,
            rv_len,
            sum_queue,
            non_empty,
            qstats.total_queued,
            qstats.accounts_with_queued,
            qstats.total_promoted,
            qstats.total_expired,
            qstats.total_rejected_full,
            qstats.total_rejected_gap_too_large,
        )
    }

    /// Peek at pending transactions without removing them from the mempool.
    /// Returns up to `max` transactions as a preview.
    pub fn peek_pending(&self, max: usize) -> Vec<MempoolTx> {
        match self.lock_mempool() {
            Ok(mut mp) => mp.peek_fair_batch(max),
            Err(_) => Vec::new(),
        }
    }

    /// P2.6-B.4a: peek up to `max` pending TX and resolve each to its
    /// raw signed bytes via the tx_storage lookup. Returns pairs of
    /// (MempoolTx, raw bytes) so callers (e.g. the Lattice publisher)
    /// can:
    ///   - use signature_hash from MempoolTx to compute the batch_root
    ///     (replay-resistant content commitment), and
    ///   - ship the raw signed bytes side-band so co-grouped peers can
    ///     reconstruct the batch when a cycle commits without having
    ///     to drain their own mempool.
    ///
    /// TX whose handle is missing from tx_storage (race / TTL purge)
    /// are silently skipped so the caller never sees a placeholder.
    pub fn peek_pending_with_bytes(&self, max: usize) -> Vec<(MempoolTx, Vec<u8>)> {
        let txs = self.peek_pending(max);
        let bytes = get_tx_bytes_from_handles(
            &self.tx_storage,
            &txs.iter().map(|t| t.tx_handle).collect::<Vec<_>>(),
        );
        txs.into_iter()
            .zip(bytes.into_iter())
            .filter_map(|(tx, b)| b.map(|raw| (tx, raw)))
            .collect()
    }

    /// Check if mempool is empty
    ///
    /// # Error Handling
    ///
    /// Returns true if mutex is poisoned (assumes empty for safety)
    pub fn is_empty(&self) -> bool {
        match self.lock_mempool() {
            Ok(mp) => mp.is_empty(),
            Err(_) => {
                // Mutex poisoned - return true (assume empty for safety)
                // Error already logged in lock_mempool()
                true
            }
        }
    }

    /// Returns (valid_txs, invalid_handles) - invalid transactions are removed from mempool
    ///
    /// The snapshot provides a point-in-time view that won't change during block preparation,
    /// Invalid transactions are marked and will be removed from mempool.
    ///
    /// # Safety
    /// - Requires `mempool_txs` and `signed_txs` to have the same length (panics if not)
    /// - Uses checked arithmetic to prevent overflow
    /// - Handles mutex poisoning gracefully
    /// - All edge cases are handled (empty arrays, storage errors, etc.)
    pub fn final_validation(
        &self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[SignedTx],
        storage: &dyn StorageTrait,
    ) -> (Vec<SignedTx>, Vec<TxHandle>) {
        // Round id 0 = unknown / not in a tracked round. Callers that have
        // a real round number (intra-group block production, periodic
        self.final_validation_with_pending(mempool_txs, signed_txs, storage, None, 0)
    }

    /// Variant that exposes `round_id` so the FL score sink (if wired)
    /// receives `(peer, round_id, score_permille)` tuples — required for
    /// `ObservationStore::bad_fl_streak` to work across rounds.
    pub fn final_validation_with_round(
        &self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[SignedTx],
        storage: &dyn StorageTrait,
        round_id: u64,
    ) -> (Vec<SignedTx>, Vec<TxHandle>) {
        self.final_validation_with_pending(mempool_txs, signed_txs, storage, None, round_id)
    }

    /// `pending_nonces` maps sender address bytes → next expected nonce from blocks
    /// that have been proposed but not yet committed (awaiting BFT certificate).
    /// This allows consecutive blocks to include TX without waiting for BFT round-trip.
    /// `round_id` is forwarded to the FL score sink for cross-round streak
    /// tracking; callers without a round context should pass 0.
    pub fn final_validation_with_pending(
        &self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[SignedTx],
        storage: &dyn StorageTrait,
        pending_nonces: Option<&std::collections::HashMap<Vec<u8>, u64>>,
        round_id: u64,
    ) -> (Vec<SignedTx>, Vec<TxHandle>) {
        // Early return for empty input
        if mempool_txs.is_empty() || signed_txs.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Safety check: ensure arrays are aligned (defensive programming)
        // This prevents silent failures if caller passes mismatched arrays
        if mempool_txs.len() != signed_txs.len() {
            // Log error and return all as invalid (safer than panicking in production)
            debug_assert_eq!(
                mempool_txs.len(),
                signed_txs.len(),
                "mempool_txs and signed_txs must have same length"
            );
            // In release, mark all as invalid and return empty
            let invalid_handles: Vec<TxHandle> =
                mempool_txs.iter().map(|tx| tx.tx_handle).collect();
            return (Vec::new(), invalid_handles);
        }

        let mut valid_txs = Vec::new();
        let mut invalid_handles = Vec::new();

        // Group transactions by class for federated aggregation (only valid ones)
        let mut federated_updates: Vec<(TxHandle, SignedTx)> = Vec::new();
        let mut iot_data: Vec<(TxHandle, SignedTx)> = Vec::new();

        // during block preparation, even if other threads modify storage concurrently
        // Snapshot functionality will be implemented when available in Storage trait
        // let snapshot = storage.create_snapshot();

        // Deduplicate sender addresses to avoid redundant storage reads,
        // then decode accounts (parallel decoding via rayon)
        use rayon::prelude::*;
        let mut unique_senders: std::collections::HashMap<Vec<u8>, Account> =
            std::collections::HashMap::new();
        for signed_tx in signed_txs.iter() {
            if !unique_senders.contains_key(&signed_tx.from) {
                let account = match storage.get_account(&signed_tx.from) {
                    Ok(Some(account_bytes)) => {
                        match bincode::deserialize::<Account>(&account_bytes) {
                            Ok(acc) => acc,
                            Err(_) => match Account::decode(&account_bytes) {
                                Ok(acc) => acc,
                                Err(e) => {
                                    tracing::warn!(
                                        sender_address = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                                        error = %e,
                                        "Failed to decode account in final validation, using default"
                                    );
                                    Account::default()
                                }
                            },
                        }
                    }
                    Ok(None) => Account::default(),
                    Err(e) => {
                        tracing::warn!(
                            sender_address = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                            error = %e,
                            "Storage error fetching account in final validation, using default"
                        );
                        Account::default()
                    }
                };
                unique_senders.insert(signed_tx.from.clone(), account);
            }
        }
        // Map back to per-TX account vector
        let accounts: Vec<Account> = signed_txs
            .iter()
            .map(|tx| unique_senders.get(&tx.from).cloned().unwrap_or_default())
            .collect();

        // Safety: ensure accounts length matches before iterating
        // This prevents index out of bounds if storage returns wrong length
        let accounts_len = accounts.len();
        let txs_len = mempool_txs.len().min(signed_txs.len());
        let iter_len = accounts_len.min(txs_len);

        // Build sorted indices by (sender_id, nonce) so we process consecutive
        // nonces per sender in order. This allows the running nonce tracker
        let mut sorted_indices: Vec<usize> = (0..iter_len).collect();
        sorted_indices.sort_by(|&a, &b| {
            let sender_cmp = mempool_txs[a].sender_id.cmp(&mempool_txs[b].sender_id);
            if sender_cmp == std::cmp::Ordering::Equal {
                mempool_txs[a].nonce.cmp(&mempool_txs[b].nonce)
            } else {
                sender_cmp
            }
        });

        // Track running nonce per sender: starts at max(storage_nonce, pending_nonce).
        //
        // storage_nonce = committed state (from RocksDB after BFT finalization)
        // pending_nonce = next expected nonce from blocks proposed but not yet committed
        //
        // Without pending_nonces, blocks N+1..N+k are all empty while waiting for
        // block N's BFT certificate (3-5s round-trip). With pending_nonces, each
        // block picks up where the previous proposed block left off.
        //
        // Pending nonces are cleared on proposer change or BFT failure by the caller,
        // and TX are restored to mempool via restore_in_flight_txs().
        let mut running_nonces: std::collections::HashMap<u32, u64> =
            std::collections::HashMap::new();
        // Fast-skip senders whose first TX has a nonce gap. Once a sender
        // is blocked, every subsequent TX for that sender is also rejected
        // without expensive HashMap/balance lookups. This avoids wasting
        let mut blocked_senders: std::collections::HashSet<u32> = std::collections::HashSet::new();

        for &i in &sorted_indices {
            let mempool_tx = &mempool_txs[i];
            // Fast path: skip already-blocked senders (O(1) HashSet lookup)
            if blocked_senders.contains(&mempool_tx.sender_id) {
                invalid_handles.push(mempool_tx.tx_handle);
                continue;
            }
            let signed_tx = &signed_txs[i];
            let account = &accounts[i];
            let expected_nonce = running_nonces
                .entry(mempool_tx.sender_id)
                .or_insert_with(|| {
                    let storage_nonce = account.nonce;
                    let pending = pending_nonces
                        .and_then(|pn| pn.get(&signed_tx.from).copied())
                        .unwrap_or(0);
                    let effective_nonce = std::cmp::max(storage_nonce, pending);
                    // storage_nonce and pending are 0 (genesis state, no prior
                    // commit observed) AND the first drained TX for this sender
                    // has nonce > 0, accept it as the starting point. The
                    //   1. drain pulls tx_nonce=N (low)
                    //   3. restore + TTL purge evicts old TX
                    //   4. next round drain pulls tx_nonce=M (M>N, even higher)
                    //   5. repeat — chain never commits, storage never advances
                    // The risk window is one nonce per sender at first contact
                    // — acceptable for testnet. Mainnet should disable this
                    // path via env (`SAVITRI_TRUST_FIRST_NONCE=0`).
                    let trust_first = std::env::var("SAVITRI_TRUST_FIRST_NONCE")
                        .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
                        .unwrap_or(true);
                    if trust_first && effective_nonce == 0 && mempool_tx.nonce > 0 {
                        tracing::warn!(
                            sender_id = mempool_tx.sender_id,
                            sender = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                            adopted_start_nonce = mempool_tx.nonce,
                            "drain_doom_loop fix #1: storage=0+pending=0 — adopting first drain tx_nonce as start"
                        );
                        return mempool_tx.nonce;
                    }
                    if effective_nonce != storage_nonce {
                        tracing::info!(
                            sender_id = mempool_tx.sender_id,
                            storage_nonce,
                            pending_nonce = pending,
                            effective_nonce,
                            tx_nonce = mempool_tx.nonce,
                            "Using pending nonce (block proposed but not yet committed)"
                        );
                    }
                    effective_nonce
                });
            tracing::debug!(
                tx_nonce = mempool_tx.nonce,
                expected_nonce = *expected_nonce,
                account_nonce = account.nonce,
                account_balance = %account.balance,
                tx_amount = signed_tx.amount,
                tx_fee = signed_tx.fee,
                sender = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                "final_validation: checking tx"
            );
            if mempool_tx.nonce != *expected_nonce {
                // Block this sender — all subsequent TX will also fail
                // (sorted_indices is ordered by sender_id then nonce, so
                // a gap here means every later TX for this sender has a
                // higher nonce and will also mismatch).
                //
                // WARN-log on first block per sender per drain round. This
                // surfaces the silent rejection path that was invisible in
                // batches marked valid=0 invalid=N with no diagnostic —
                // the root cause turned out to be the sender_id vs storage
                // nonce registry mismatch noted in p2p/block.rs:468-471).
                // Without this log, "blocked_senders cascade" silently
                let pending_snap = pending_nonces
                    .and_then(|pn| pn.get(&signed_tx.from).copied())
                    .unwrap_or(0);
                tracing::warn!(
                    sender_id = mempool_tx.sender_id,
                    sender = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                    tx_nonce = mempool_tx.nonce,
                    expected_nonce = *expected_nonce,
                    storage_nonce = account.nonce,
                    pending_nonce = pending_snap,
                    "final_validation REJECTED: nonce mismatch — blocking sender for this drain"
                );
                blocked_senders.insert(mempool_tx.sender_id);
                invalid_handles.push(mempool_tx.tx_handle);
                continue;
            }
            // Advance running nonce for this sender
            *expected_nonce += 1;

            // Use checked arithmetic to prevent overflow (memory safety)
            let fee = signed_tx.fee;
            let total_required = match signed_tx.amount.checked_add(fee) {
                Some(total) => total,
                None => {
                    // Overflow: amount + fee exceeds u128::MAX
                    // This is an invalid transaction - mark and skip
                    tracing::info!("REJECTED: amount + fee overflow");
                    invalid_handles.push(mempool_tx.tx_handle);
                    continue;
                }
            };

            // Edge case: if total_required is 0, transaction is valid (no cost)
            // But we still need to check balance >= 0 (always true for u128)
            // So we only check if total_required > 0
            if total_required > 0 && account.balance < total_required as u128 {
                tracing::info!(
                    account_balance = %account.balance,
                    total_required = total_required,
                    "REJECTED: insufficient balance"
                );
                invalid_handles.push(mempool_tx.tx_handle);
                continue;
            }

            match mempool_tx.class {
                TxClass::FederatedUpdate | TxClass::IoTData => {
                    // For FederatedUpdate/IoTData, verify quota availability
                    let min_quota_requirement = match mempool_tx.class {
                        TxClass::FederatedUpdate => {
                            // Federated updates require significant resources (model training, aggregation)
                            // Base requirement + additional based on transaction size
                            let base_requirement = 1000u128;
                            let size_factor = (signed_tx.to.len() as u128).saturating_mul(10);
                            base_requirement.saturating_add(size_factor)
                        }
                        TxClass::IoTData => {
                            // IoT data submissions are lighter but still require resources
                            // Base requirement + smaller size factor
                            let base_requirement = 100u128;
                            let size_factor = (signed_tx.to.len() as u128).saturating_mul(2);
                            base_requirement.saturating_add(size_factor)
                        }
                        _ => 0u128,
                    };

                    // Check if account has sufficient quota
                    if account.balance < min_quota_requirement {
                        tracing::debug!(
                            class = ?mempool_tx.class,
                            required = min_quota_requirement,
                            balance = account.balance,
                            "Insufficient quota for transaction"
                        );
                        invalid_handles.push(mempool_tx.tx_handle);
                        continue;
                    }

                    // This would be enhanced with a proper quota tracking system
                    tracing::debug!(
                        class = ?mempool_tx.class,
                        quota_used = min_quota_requirement,
                        remaining = account.balance.saturating_sub(min_quota_requirement),
                        "Quota validation passed"
                    );
                }
                TxClass::System => {
                    // For System transactions, verify stake requirements
                    let min_stake_requirement = match signed_tx.to.get(0) {
                        Some(&op_code) => {
                            // Different system operations require different stakes
                            match op_code {
                                0x01 => 1000u128, // Governance operations
                                0x02 => 500u128,  // Configuration changes
                                0x03 => 200u128,  // Maintenance operations
                                _ => 500u128,     // Default system operation
                            }
                        }
                        None => 500u128, // Default when no operation code
                    };

                    if account.balance < min_stake_requirement {
                        tracing::debug!(
                            required = min_stake_requirement,
                            balance = account.balance,
                            "Insufficient stake for system transaction"
                        );
                        invalid_handles.push(mempool_tx.tx_handle);
                        continue;
                    }

                    tracing::debug!(
                        stake_required = min_stake_requirement,
                        remaining = account.balance.saturating_sub(min_stake_requirement),
                        "Stake validation passed"
                    );
                }
                _ => {
                    // Financial transactions: no quota/stake checks needed
                }
            }

            match mempool_tx.class {
                TxClass::FederatedUpdate => {
                    federated_updates.push((mempool_tx.tx_handle, signed_tx.clone()));
                }
                TxClass::IoTData => {
                    iot_data.push((mempool_tx.tx_handle, signed_tx.clone()));
                }
                _ => {
                    // Financial and System transactions go directly to valid
                    valid_txs.push(signed_tx.clone());
                }
            }
        }

        // NOTE: pending_nonces are no longer updated here. We use only storage_nonce
        // state when proposed blocks failed BFT and were never committed.

        // Federated aggregation: aggregate FederatedUpdate transactions.
        // Robust pipeline (dim/NaN/norm gates + cosine vs coordinate-wise
        // FL score sink is wired (lightnode `main.rs`), forward each
        // `(peer, round_id, score)` into the PoU `ObservationStore`.
        if !federated_updates.is_empty() {
            let (aggregated, fl_scores) = self.aggregate_federated_updates(&federated_updates);
            if let Ok(sink_slot) = self.fl_score_sink.read() {
                if let Some(sink) = sink_slot.as_ref() {
                    for (peer_hex, score) in &fl_scores {
                        sink(peer_hex, round_id, *score);
                    }
                }
            }
            valid_txs.extend(aggregated);
        }

        // IoT data aggregation: batch IoT transactions
        if !iot_data.is_empty() {
            let batched = Self::batch_iot_data(&iot_data, &self.tx_storage);
            valid_txs.extend(batched);
        }

        // Remove invalid transactions from mempool
        // Handle mutex poisoning gracefully (memory safety)
        if !invalid_handles.is_empty() {
            // Use recovery helper for consistent error handling
            let mut mp = self.lock_mempool_with_recovery();
            mp.remove_by_handles(&invalid_handles);
        }

        (valid_txs, invalid_handles)
    }

    /// Aggregate federated learning updates with Byzantine-robust filtering.
    ///
    /// # Pipeline
    ///
    /// 1. **Decode** each TX: extract the gradient bytes (little-endian
    ///    f64 sequence) from either `CallTransaction` calldata or the
    ///    legacy SignedTx `to` field.
    ///
    /// 2. **Dimension lock**: the first valid gradient fixes
    ///    `expected_dim` for the round. Any later submission with a
    ///    different length is rejected (score = 0). This closes the
    ///    "pad-with-zeros" attack of the legacy implementation.
    ///
    /// 3. **Byzantine-robust scoring** via
    ///    `savitri_core::fl_robust::score_gradients_vs_median`:
    ///       * NaN/Inf → score 0 (drop)
    ///       * `||g||_2 > NORM_CLIP_THRESHOLD` → score 0 (drop)
    ///         to permille
    ///       * `included = score >= MALICIOUS_GRADIENT_THRESHOLD_PERMILLE`
    ///
    /// 4. **Inclusion gate**: only clients with `included = true` feed
    ///    into FedAvg. Malicious/outlier gradients never touch the
    ///    aggregated model.
    ///
    /// 5. **FedAvg on survivors**: weighted average with weight derived
    ///    from `tx.amount` (as before), but now computed on the filtered
    ///    subset so attackers cannot dominate via inflated amounts.
    ///
    /// # Returns
    ///
    /// * `Vec<SignedTx>` — the single aggregated transaction (or empty
    ///   if no gradient passed the gates).
    /// * `Vec<(peer_hex, score_permille)>` — per-client score for every
    ///   original submission. Caller forwards to the PoU
    ///   `ObservationStore::record_fl_contribution`.
    fn aggregate_federated_updates(
        &self,
        updates: &[(TxHandle, SignedTx)],
    ) -> (Vec<SignedTx>, Vec<(String, u16)>) {
        let tx_storage = &self.tx_storage;
        // Edge case: empty updates.
        if updates.is_empty() {
            return (Vec::new(), Vec::new());
        }

        // Snapshot the PoU score provider once so each survivor sees a
        // consistent score even if the closure mutates internal state.
        let pou_provider: Option<Arc<dyn Fn(&str) -> u16 + Send + Sync>> = self
            .pou_score_provider
            .read()
            .ok()
            .and_then(|guard| guard.clone());

        // Retrieve original transaction bytes for proper decoding.
        let tx_bytes_map: std::collections::HashMap<TxHandle, Option<Vec<u8>>> = {
            let handles: Vec<TxHandle> = updates.iter().map(|(handle, _)| *handle).collect();
            let bytes_vec = get_tx_bytes_from_handles(tx_storage, &handles);
            handles.into_iter().zip(bytes_vec.into_iter()).collect()
        };

        // Step 1: decode gradients per client. Clients whose gradient
        // cannot be decoded are kept in the score output (with score 0)
        // so the ObservationStore learns they sent garbage — but they
        // are not forwarded to the robust scorer (which only sees the
        // decodable subset).
        #[derive(Clone)]
        struct DecodedClient {
            peer_hex: String,
            gradient: Vec<f64>,
            weight: f64,
            tx_ref_index: usize,
        }
        let mut decoded: Vec<DecodedClient> = Vec::with_capacity(updates.len());
        let mut undecodable: Vec<String> = Vec::new();

        for (idx, (handle, tx)) in updates.iter().enumerate() {
            let peer_hex = hex::encode(&tx.from);
            let raw_gradient_bytes = if let Some(Some(tx_bytes)) = tx_bytes_map.get(handle) {
                let is_call_transaction = !tx_bytes.is_empty() && tx_bytes[0] == 0x01;
                if is_call_transaction && tx_bytes.len() > 1 {
                    let end = std::cmp::min(1 + 64, tx_bytes.len());
                    Some(tx_bytes[1..end].to_vec())
                } else if !tx.to.is_empty() {
                    Some(tx.to.clone())
                } else {
                    None
                }
            } else if !tx.to.is_empty() {
                Some(tx.to.clone())
            } else {
                None
            };

            let Some(bytes) = raw_gradient_bytes else {
                undecodable.push(peer_hex);
                continue;
            };
            if bytes.is_empty() || bytes.len() % 8 != 0 {
                undecodable.push(peer_hex);
                continue;
            }
            let gradient: Vec<f64> = bytes
                .chunks_exact(8)
                .map(|c| f64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]]))
                .collect();
            // Base weight from token amount (legacy behaviour). When the
            // PoU score provider is wired, scale by `pou_score / 1000`
            // so a peer's consensus trust modulates its FL contribution
            // strength: an amount-rich but PoU-poor peer can no longer
            // dominate the aggregate. Missing provider → modifier = 1.0.
            let amount_weight = if tx.amount > 0 {
                (tx.amount as f64) / 1e18
            } else {
                0.0
            };
            let pou_modifier = pou_provider
                .as_ref()
                .map(|p| (p(&peer_hex) as f64) / 1000.0)
                .unwrap_or(1.0);
            let weight = amount_weight * pou_modifier;
            decoded.push(DecodedClient {
                peer_hex,
                gradient,
                weight,
                tx_ref_index: idx,
            });
        }

        // Peer scores to return to caller: start with 0 for undecodable.
        let mut peer_scores: Vec<(String, u16)> =
            undecodable.into_iter().map(|p| (p, 0u16)).collect();

        // If no client produced a decodable gradient, bail out.
        if decoded.is_empty() {
            return (Vec::new(), peer_scores);
        }

        // Step 2: dimension lock — the first decodable gradient sets the
        // expected dimension. Any client with a different length fails
        // the robust scorer's gate (score = 0).
        let expected_dim = decoded[0].gradient.len();

        // Step 3: Byzantine-robust scoring via savitri-core.
        let scorable: Vec<(String, Vec<f64>)> = decoded
            .iter()
            .map(|c| (c.peer_hex.clone(), c.gradient.clone()))
            .collect();
        let scored = savitri_core::fl_robust::score_gradients_vs_median(&scorable, expected_dim);

        // Step 4: inclusion gate — only gradients that passed the gates
        // AND the threshold feed FedAvg. Malicious/outlier gradients are
        // dropped completely.
        let mut survivors: Vec<(&DecodedClient, u16)> = Vec::new();
        for (client, sg) in decoded.iter().zip(scored.iter()) {
            peer_scores.push((sg.peer_id.clone(), sg.score_permille));
            if sg.included {
                survivors.push((client, sg.score_permille));
            }
        }

        if survivors.is_empty() {
            // Every client failed the robust gate. Return no aggregate —
            // better to skip a poisoned round than produce a tainted one.
            return (Vec::new(), peer_scores);
        }

        // Step 5: FedAvg on survivors.
        let mut weights: Vec<f64> = survivors.iter().map(|(c, _)| c.weight).collect();
        let mut total_weight: f64 = weights.iter().sum();
        if total_weight == 0.0 {
            let uniform = 1.0 / weights.len() as f64;
            weights = vec![uniform; weights.len()];
            total_weight = weights.iter().sum();
            if total_weight == 0.0 {
                return (Vec::new(), peer_scores);
            }
        }

        let mut aggregated_gradient = vec![0.0_f64; expected_dim];
        for (j, (client, _)) in survivors.iter().enumerate() {
            for i in 0..expected_dim {
                aggregated_gradient[i] += weights[j] * client.gradient[i];
            }
        }
        for v in aggregated_gradient.iter_mut() {
            *v /= total_weight;
        }

        // Serialise the aggregated gradient back to bytes.
        let mut aggregated_data = Vec::with_capacity(expected_dim * 8);
        for v in aggregated_gradient.iter() {
            aggregated_data.extend_from_slice(&v.to_le_bytes());
        }

        // Template from the highest-scoring survivor — its peer record is
        // still the best proxy for aggregator metadata until the FL
        // contract owns the aggregated transaction identity.
        let (template, _) = survivors
            .iter()
            .max_by_key(|(_, score)| *score)
            .copied()
            .map(|(c, s)| (updates[c.tx_ref_index].1.clone(), s))
            .unwrap_or_else(|| (updates[survivors[0].0.tx_ref_index].1.clone(), 1000));
        let mut aggregated_tx = template;
        aggregated_tx.to = aggregated_data;
        aggregated_tx.amount = (total_weight * 1e18) as u64;

        (vec![aggregated_tx], peer_scores)
    }

    /// Batch IoT data transactions
    ///
    /// Groups IoT transactions by device/sender and creates efficient batch transactions.
    ///
    /// # Algorithm
    ///
    /// 1. Group transactions by sender (`from` field) - each sender represents a device
    /// 2. For each group, create batch transactions:
    ///    - Extract IoT data from transaction (from `to` field or calldata)
    ///    - Combine data into batch format: array of entries (length + data)
    ///    - Respect limits: max 100 TX or 10KB per batch
    /// 3. Create batch transaction with combined data
    ///
    /// # Batch Format
    ///
    /// Batch data structure:
    /// - Header: number of entries (u32, little-endian)
    /// - For each entry:
    ///   - Entry length (u32, little-endian)
    ///   - Entry data (raw bytes)
    ///
    /// # Edge Cases
    ///
    /// - Single transaction: returns as-is (no batching needed)
    /// - Empty data: returns empty Vec
    /// - Transactions exceed limit: creates multiple batches
    /// - Different senders: creates separate batches per sender
    ///
    /// # Transaction Format Support
    ///
    /// Supports two transaction formats:
    /// 1. CallTransaction: IoT data extracted from `calldata` field
    /// 2. SignedTx: IoT data extracted from `to` field (fallback/legacy format)
    #[allow(unused_assignments)] // batch_metrics fields are used in debug mode
    fn batch_iot_data(
        data: &[(TxHandle, SignedTx)],
        tx_storage: &Arc<Mutex<crate::mempool::prevalidation::TxStorage>>,
    ) -> Vec<SignedTx> {
        // Profiling: start timing
        let start_time = Instant::now();
        let mut batch_metrics = IoTBatchingMetrics::new();
        batch_metrics.total_transactions = data.len();

        // Edge case: empty data
        if data.is_empty() {
            return Vec::new();
        }

        // Edge case: single transaction - return as-is (no batching needed)
        if data.len() == 1 {
            return vec![data[0].1.clone()];
        }

        // Constants for batching limits
        const MAX_BATCH_SIZE: usize = 100; // Max transactions per batch
        const MAX_BATCH_SIZE_BYTES: usize = 10 * 1024; // Max 10KB per batch

        // Profiling: grouping phase
        let grouping_start = Instant::now();

        // Group transactions by sender (device_id)
        // Optimization: Use capacity hint to reduce reallocations
        // Estimate: assume ~10% unique senders (conservative estimate)
        let estimated_devices = (data.len() / 10).max(16).min(1024);
        let mut grouped_by_sender: std::collections::HashMap<Vec<u8>, Vec<(TxHandle, SignedTx)>> =
            std::collections::HashMap::with_capacity(estimated_devices);

        for (handle, tx) in data.iter() {
            grouped_by_sender
                .entry(tx.from.clone())
                .or_insert_with(|| Vec::with_capacity(8)) // Pre-allocate for common case
                .push((*handle, tx.clone()));
        }

        batch_metrics.total_devices = grouped_by_sender.len();
        let _grouping_time_us = grouping_start.elapsed().as_micros() as u64;

        // Profiling: storage retrieval phase
        let _storage_start = Instant::now();

        // Retrieve original transaction bytes from storage for proper decoding
        let tx_bytes_map: std::collections::HashMap<TxHandle, Option<Vec<u8>>> = {
            let handles: Vec<TxHandle> = data.iter().map(|(handle, _)| *handle).collect();
            let bytes_vec = get_tx_bytes_from_handles(tx_storage, &handles);
            handles.into_iter().zip(bytes_vec.into_iter()).collect()
        };

        // Profiling: batching phase
        let _batching_start = Instant::now();

        let mut batched_transactions = Vec::new();
        let mut total_original_size = 0usize;
        let mut total_batched_size = 0usize;

        // Process each sender group separately
        for (sender, transactions) in grouped_by_sender.into_iter() {
            let batch_start = Instant::now();
            let _sender_vec: Vec<u8> = sender.clone();

            // Extract IoT data from transactions
            // Optimization: Pre-allocate with estimated capacity
            let mut iot_data_entries: Vec<Vec<u8>> =
                Vec::with_capacity(transactions.len().min(MAX_BATCH_SIZE));

            for (handle, tx) in transactions.iter() {
                // Step 1: Extract IoT data from transaction
                // Support both CallTransaction and SignedTx formats
                let iot_data = if let Some(Some(tx_bytes)) = tx_bytes_map.get(handle) {
                    // Try to detect CallTransaction format
                    let is_call_transaction = tx_bytes.len() > 0 && tx_bytes[0] == 0x02; // IoT operation code

                    if is_call_transaction {
                        // Extract IoT data from CallTransaction calldata
                        if tx_bytes.len() > 1 {
                            let data_start = 1; // Skip operation code
                            let data_end = std::cmp::min(data_start + 128, tx_bytes.len()); // Max 128 bytes for IoT data
                            tx_bytes[data_start..data_end].to_vec()
                        } else {
                            tx.to.clone()
                        }
                    } else {
                        // Use SignedTx format: extract from 'to' field
                        if !tx.to.is_empty() {
                            tx.to.clone()
                        } else {
                            Vec::new()
                        }
                    }
                } else {
                    // Fallback to SignedTx format
                    if !tx.to.is_empty() {
                        tx.to.clone()
                    } else {
                        Vec::new()
                    }
                };

                // Track original size for batch_metrics
                total_original_size += iot_data.len();

                // Only add non-empty data
                if !iot_data.is_empty() {
                    iot_data_entries.push(iot_data);
                }
            }

            // Edge case: no valid data entries
            if iot_data_entries.is_empty() {
                // Return transactions as-is if no valid data
                batch_metrics.skipped_transactions += transactions.len();
                batched_transactions.extend(transactions.into_iter().map(|(_, tx)| tx));
                continue;
            }

            // Edge case: single entry - return as-is (no batching needed)
            if iot_data_entries.len() == 1 {
                batch_metrics.skipped_transactions += 1;
                batched_transactions.push(transactions[0].1.clone());
                continue;
            }

            // Create batches respecting limits (max 100 TX or 10KB)
            // Optimization: Pre-allocate batch entries Vec
            let mut current_batch_entries: Vec<Vec<u8>> = Vec::with_capacity(MAX_BATCH_SIZE);
            let mut current_batch_size_bytes = 0usize;

            for entry_data in iot_data_entries.into_iter() {
                // Calculate size: 4 bytes (u32 length) + entry_data.len()
                let entry_size = 4 + entry_data.len();

                // Check if adding this entry would exceed limits
                let would_exceed_tx_limit = current_batch_entries.len() >= MAX_BATCH_SIZE;
                let would_exceed_size_limit =
                    (current_batch_size_bytes + entry_size) > MAX_BATCH_SIZE_BYTES;

                // If current batch is full or would exceed limits, create batch and start new one
                if !current_batch_entries.is_empty()
                    && (would_exceed_tx_limit || would_exceed_size_limit)
                {
                    // Create batch transaction from current_batch_entries
                    let batch_tx = Self::create_iot_batch_transaction(
                        &sender,
                        &current_batch_entries,
                        &transactions,
                    );

                    // Track batched size for batch_metrics
                    total_batched_size += batch_tx.to.len();
                    batch_metrics.total_batches += 1;

                    batched_transactions.push(batch_tx);

                    // Reset for new batch
                    current_batch_entries.clear();
                    current_batch_entries.reserve(MAX_BATCH_SIZE); // Pre-allocate for next batch
                    current_batch_size_bytes = 0;
                }

                // Add entry to current batch
                current_batch_entries.push(entry_data);
                current_batch_size_bytes += entry_size;
            }

            // Create final batch if there are remaining entries
            if !current_batch_entries.is_empty() {
                let batch_tx = Self::create_iot_batch_transaction(
                    &sender,
                    &current_batch_entries,
                    &transactions,
                );

                // Track batched size for batch_metrics
                total_batched_size += batch_tx.to.len();
                batch_metrics.total_batches += 1;

                batched_transactions.push(batch_tx);
            }

            // Track peak batch time
            let batch_time_us = batch_start.elapsed().as_micros() as u64;
            if batch_time_us > batch_metrics.peak_batch_time_us {
                batch_metrics.peak_batch_time_us = batch_time_us;
            }
        }

        // Update batch_metrics
        batch_metrics.total_original_size = total_original_size;
        batch_metrics.total_batched_size = total_batched_size;
        batch_metrics.total_processing_time_us = start_time.elapsed().as_micros() as u64;

        #[cfg(debug_assertions)]
        {
            if batch_metrics.total_transactions > 0 {
                eprintln!(
                    "IoT Batching Metrics: {} TX -> {} batches ({} devices), efficiency: {:.2}%, compression: {:.2}%, overhead reduction: {:.2}%, throughput: {:.0} TPS, peak batch: {}µs",
                    batch_metrics.total_transactions,
                    batch_metrics.total_batches,
                    batch_metrics.total_devices,
                    batch_metrics.batch_efficiency(),
                    batch_metrics.compression_ratio(),
                    batch_metrics.overhead_reduction(),
                    batch_metrics.throughput_tps(),
                    batch_metrics.peak_batch_time_us
                );
            }
        }

        batched_transactions
    }

    /// Create a batch transaction from IoT data entries
    ///
    /// # Batch Format
    ///
    /// - Header: number of entries (u32, little-endian)
    /// - For each entry:
    ///   - Entry length (u32, little-endian)
    ///   - Entry data (raw bytes)
    ///
    /// # Arguments
    ///
    /// * `sender` - Sender address (device_id) - used for verification
    /// * `entries` - Vector of IoT data entries to batch
    /// * `original_transactions` - Original transactions (for metadata like fee, pubkey, sig)
    fn create_iot_batch_transaction(
        sender: &[u8],
        entries: &[Vec<u8>],
        original_transactions: &[(TxHandle, SignedTx)],
    ) -> SignedTx {
        // Build batch data: header (u32) + entries (length + data for each)
        let mut batch_data = Vec::new();

        // Header: number of entries (u32, little-endian)
        batch_data.extend_from_slice(&(entries.len() as u32).to_le_bytes());

        // For each entry: length (u32) + data
        for entry in entries.iter() {
            // Entry length (u32, little-endian)
            batch_data.extend_from_slice(&(entry.len() as u32).to_le_bytes());
            // Entry data
            batch_data.extend_from_slice(entry);
        }

        // Use first transaction as template for metadata
        let base_tx = &original_transactions[0].1;

        // Verify that sender matches base_tx.from (safety check)
        debug_assert_eq!(
            base_tx.from.as_slice(),
            sender,
            "Sender mismatch in batch transaction creation"
        );

        // Create batch transaction
        let mut batch_tx = base_tx.clone();

        // Update `to` field with batch data
        batch_tx.to = batch_data;

        // Sum amounts from all original transactions (if needed)
        // For now, keep original amount or sum them
        let total_amount: u128 = original_transactions
            .iter()
            .map(|(_, tx)| tx.amount)
            .fold(0u128, |acc: u128, amt| acc.saturating_add(amt.into()));
        batch_tx.amount = total_amount as u64;

        // Sum fees from all original transactions (if present)
        let total_fee: u128 = original_transactions
            .iter()
            .filter_map(|(_, tx)| Some(tx.fee))
            .fold(0u128, |acc: u128, fee| acc.saturating_add(fee.into()));
        if total_fee > 0 {
            batch_tx.fee = total_fee as u64;
        }

        batch_tx
    }

    /// Helper method for tests: add transaction bytes to tx_storage
    /// This allows tests to set up transaction storage for testing aggregation
    pub fn test_add_tx_bytes(&self, handle: TxHandle, bytes: Vec<u8>) {
        if let Ok(mut storage) = self.tx_storage.lock() {
            storage.put(handle, bytes);
        }
    }
}

/// Helper: Convert raw transaction bytes to RawTx
pub fn bytes_to_raw_tx(bytes: Vec<u8>, peer_id: Option<u64>) -> RawTx {
    RawTx {
        bytes,
        peer_id,
        recv_ts: Instant::now(),
    }
}

/// Helper: Extract transaction bytes from handles
/// This requires access to the shared tx_storage
///
/// # Error Handling
///
/// Returns empty Vec if mutex is poisoned (graceful degradation)
pub fn get_tx_bytes_from_handles(
    tx_storage: &Arc<Mutex<crate::mempool::prevalidation::TxStorage>>,
    handles: &[TxHandle],
) -> Vec<Option<Vec<u8>>> {
    // Use helper function for graceful error handling
    match MempoolPipeline::lock_tx_storage(tx_storage) {
        Ok(storage) => handles
            .iter()
            .map(|handle| storage.get(*handle).cloned())
            .collect(),
        Err(_) => {
            // Mutex poisoned - return empty Vec (graceful degradation)
            // Error already logged in lock_tx_storage()
            vec![None; handles.len()]
        }
    }
}
