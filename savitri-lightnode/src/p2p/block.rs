#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::core::tx::{MempoolTx, TxHandle};
use crate::integrity::{self, IntegrityKind};
use crate::storage::{BlockAndAccountStorage, BlockCommitData};
use crate::tx::{hash_signed_tx_bytes, Block, SignedTx};
use crate::{
    p2p::fee_distribution::{calculate_fee_distribution, collect_p2p_nodes_for_fee_distribution},
    p2p::types::{BlockMessage, HaveBlock, PeerInfo},
    p2p::PouState,
};
use anyhow::{Context, Result};
use bincode;
use hex;
use libp2p::PeerId;
#[cfg(feature = "metrics")]
use metrics::{counter, gauge};
use savitri_storage::StorageTrait;
use sha2::Digest;
use std::future::Future;
use std::sync::Arc;

// Type aliases for the real mempool types (used internally)
use savitri_mempool::mempool::admission::AdmissionConfig;
use savitri_mempool::mempool::integration::MempoolPipeline as RealMempoolPipeline;
use savitri_mempool::mempool::types::MempoolTx as RealMempoolTx;
use savitri_mempool::mempool::types::RawTx;
use savitri_mempool::mempool::types::SignedTx as MempoolSignedTx;
use savitri_mempool::mempool::types::TxHandle as RealTxHandle;

// ─── Type conversion helpers ────────────────────────────────────────────────

pub(crate) fn normalize_address_bytes(address: &str) -> Vec<u8> {
    let trimmed = address.trim();
    let no_prefix = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    hex::decode(no_prefix).unwrap_or_else(|_| trimmed.as_bytes().to_vec())
}
/// Convert lightnode SignedTx (String addresses, Option<u128> fee, [u8;64] sig)
/// to mempool SignedTx (Vec<u8> addresses, u64 fee, Vec<u8> sig)
fn lightnode_tx_to_mempool_tx(tx: &crate::tx::SignedTx) -> MempoolSignedTx {
    MempoolSignedTx {
        from: normalize_address_bytes(&tx.from),
        to: normalize_address_bytes(&tx.to),
        amount: tx.amount,
        nonce: tx.nonce,
        fee: tx.fee.unwrap_or(0) as u64,
        pubkey: tx.pubkey.clone(),
        sig: tx.sig.to_vec(),
        pre_verified: tx.pre_verified,
    }
}

/// Convert mempool SignedTx back to lightnode SignedTx
fn mempool_tx_to_lightnode_tx(tx: &MempoolSignedTx) -> crate::tx::SignedTx {
    let mut sig = [0u8; 64];
    let copy_len = std::cmp::min(tx.sig.len(), 64);
    sig[..copy_len].copy_from_slice(&tx.sig[..copy_len]);

    crate::tx::SignedTx {
        from: hex::encode(&tx.from),
        to: hex::encode(&tx.to),
        amount: tx.amount,
        nonce: tx.nonce,
        fee: if tx.fee > 0 {
            Some(tx.fee as u128)
        } else {
            None
        },
        data: None,
        pubkey: tx.pubkey.clone(),
        sig,
        pre_verified: tx.pre_verified,
    }
}

/// Convert lightnode SignedTx to raw bytes (for feeding into RawTx).
/// CRITICAL: serialize as MempoolSignedTx (Vec<u8> addresses, u64 fee, Vec<u8> sig)
/// so that mempool's deserialize_transaction_from_bytes can decode it correctly.
fn lightnode_tx_to_raw_bytes(tx: &crate::tx::SignedTx) -> Vec<u8> {
    let mempool_tx = lightnode_tx_to_mempool_tx(tx);
    bincode::serialize(&mempool_tx).unwrap_or_default()
}

// ─── MempoolPipeline adapter ────────────────────────────────────────────────

/// MempoolPipeline adapter that wraps the real savitri-mempool pipeline.
///
/// Keeps the same public API that the rest of the lightnode uses, but internally
/// delegates to `savitri_mempool::mempool::integration::MempoolPipeline` so that
/// from that same queue.
/// Holds Arc to the real mempool pipeline. The inner `RealMempoolPipeline` is
/// already internally synchronized (`Arc<std::sync::Mutex<Mempool>>`), so the
/// previous outer `tokio::sync::Mutex` was redundant. Removing it eliminates
/// the silent-disconnect risk between RPC ingress and proposer drain (an earlier fix)
/// and lets `inner_for_rpc()` return an Arc that is `ptr_eq` to the proposer's
/// handle.
///
/// keeps its own isolated `tokio::sync::Mutex`.
/// `Arc<tokio::sync::Mutex<MempoolPipeline>>` outer wrapper. Cheap to clone
/// (`Arc` shallow), `ptr_eq` works natively for the boot-time invariant
/// assert in `main.rs:862-867`. Each method on `MempoolPipeline` already
/// takes `&self`; the inner `Mempool` `std::sync::Mutex` provides the
/// only synchronization needed because drain and RPC submit operate on
/// disjoint sections of the mempool.
///
/// New caller migration (preferred): take `LightnodeMempoolHandle` and call
/// `handle.X().await` directly instead of `pipeline.lock().await.X()`.
/// See `tier3_fase2` audit memo + the in-progress migration tracked in
/// task #16 (full migration of intra_group/mod.rs deferred to dedicated
/// PR; the type alias and 1-2 isolated callers ship in this commit as
/// the strangler scaffolding).
pub type LightnodeMempoolHandle = Arc<MempoolPipeline>;

pub struct MempoolPipeline {
    /// The real, full mempool pipeline from savitri-mempool (shared with RPC).
    /// Plain `Arc` — internal synchronization lives inside `RealMempoolPipeline`.
    inner: Arc<RealMempoolPipeline>,
    storage: Arc<dyn StorageTrait>,
    /// Pending nonces: tracks the next expected nonce for senders whose TX have been
    /// proposed in blocks but not yet committed (awaiting BFT certificate).
    /// Key: sender address bytes, Value: (next expected nonce, last-update timestamp).
    /// rejecting all TX for a sender until the previous block is committed — causing
    /// ~80% empty blocks and limiting TPS to ~13 instead of hundreds.
    ///
    /// The timestamp is used to auto-evict stale entries: if BFT consensus stalls and
    /// a proposed block never commits, the entry expires after `PENDING_NONCE_TTL_SECS`
    /// so the proposer doesn't permanently block senders behind dead pending nonces.
    /// produced `valid=0 invalid=100` because pending_nonces=5 (proposed) vs tx_nonce=0
    /// (still in mempool) caused cascade blocking via blocked_senders HashSet.
    ///
    /// Wrapped in its own `tokio::sync::Mutex` so that mutation methods can keep
    /// `&self` signatures (avoids cloning the wrapper just to update a counter).
    pending_nonces:
        Arc<tokio::sync::Mutex<std::collections::HashMap<Vec<u8>, (u64, std::time::Instant)>>>,
}

/// TTL for stale pending nonces. Matches the certificate tracker's
/// PENDING_BLOCK_TIMEOUT_SECS (300s) — once a proposed block has been evicted
/// for missing its BFT certificate, the pending_nonce it installed is also dead
/// and must be cleared so later drains can succeed.
const PENDING_NONCE_TTL_SECS: u64 = 300;

/// Execute an async future from sync code when already inside a Tokio runtime.
/// Uses `block_in_place` to avoid nested-runtime panics.
fn block_on_current_runtime<F>(fut: F) -> Option<F::Output>
where
    F: Future,
{
    let handle = tokio::runtime::Handle::try_current().ok()?;
    Some(tokio::task::block_in_place(|| handle.block_on(fut)))
}

impl std::fmt::Debug for MempoolPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.len();
        f.debug_struct("MempoolPipeline")
            .field("len", &len)
            .finish()
    }
}

impl MempoolPipeline {
    /// Create a new mempool pipeline backed by the real savitri-mempool implementation
    pub fn new_with_storage(storage: Arc<dyn StorageTrait>) -> Self {
        Self::new_with_storage_and_config(storage, None)
    }

    /// Create with optional admission config (e.g. AdmissionConfig::testnet_fees() for testnet)
    pub fn new_with_storage_and_config(
        storage: Arc<dyn StorageTrait>,
        admission_config: Option<AdmissionConfig>,
    ) -> Self {
        let inner = match admission_config {
            Some(config) => RealMempoolPipeline::with_admission_config(storage.clone(), config),
            None => RealMempoolPipeline::new(storage.clone()),
        };
        let inner = Arc::new(inner);
        Self {
            inner,
            storage,
            pending_nonces: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Get the inner pipeline for RPC (send_raw_transaction, faucet).
    ///
    /// Returns the same `Arc<RealMempoolPipeline>` that the proposer drains
    /// from. Cloning the Arc preserves `Arc::ptr_eq` so the boot path can
    /// assert that RPC ingress and proposer drain share state.
    pub fn inner_for_rpc(&self) -> Arc<RealMempoolPipeline> {
        self.inner.clone()
    }

    /// Forward an FL score sink down to the inner real pipeline. The sink
    /// must be `'static + Send + Sync` because the mempool stores it
    /// behind an `Arc` and may invoke it from any thread that drives
    /// mutex briefly via the current tokio runtime.
    pub fn set_fl_score_sink(&self, sink: Arc<dyn Fn(&str, u64, u16) + Send + Sync>) {
        self.inner.set_fl_score_sink(sink);
    }

    /// Forward a PoU score provider down to the inner real pipeline so
    /// `aggregate_federated_updates` can scale FedAvg weights by the
    /// per-peer trust score. Same threading constraints as
    /// `set_fl_score_sink`.
    pub fn set_pou_score_provider(&self, provider: Arc<dyn Fn(&str) -> u16 + Send + Sync>) {
        self.inner.set_pou_score_provider(provider);
    }

    /// Create a new mempool pipeline with default capacity (legacy compat)
    /// NOTE: This creates a pipeline with a no-op storage. Prefer new_with_storage().
    pub fn new() -> Self {
        // Create a minimal in-memory storage for backward compat
        let storage = Arc::new(
            crate::storage::Storage::with_config(crate::storage::StorageConfig::default())
                .expect("Failed to create default storage for MempoolPipeline"),
        );
        Self::new_with_storage(storage)
    }

    /// Create a new mempool pipeline with custom capacity (legacy compat)
    pub fn with_capacity(_max_size: usize) -> Self {
        Self::new()
    }

    /// Add a signed transaction to the mempool via the real pipeline
    pub fn add_transaction(&self, tx: crate::tx::SignedTx) -> Result<u64> {
        // (block.rs:377). `lightnode_tx_to_raw_bytes` re-serializes a
        // `SignedTx` (`TransactionExt` wire format with `from: String`) into
        // a different `MempoolSignedTx` struct (`from: Vec<u8>`) using
        // deserializes back as `TransactionExtCompat` with FIXINT — the
        // length-prefix mismatch makes every TX fail signature verification.
        // Symptom on `add_transaction` callers: TxForward handler
        // (network/mod.rs:3864) silently drops every cross-group TX
        // delivered via direct-send → with the P1 cache fix activating
        // 41.5 to 1 because cross-group TX bypassed the (working) gossip
        // path and hit this (broken) one. Fix: use the canonical
        // serializer to mirror loadtest / RPC ingress format.
        let raw_bytes = match crate::tx::serialize_signed_tx(&tx) {
            Ok(b) => b,
            Err(e) => {
                anyhow::bail!("serialize_signed_tx failed: {}", e);
            }
        };
        let raw_tx = RawTx {
            bytes: raw_bytes,
            peer_id: None,
            recv_ts: std::time::Instant::now(),
        };

        // Use tokio runtime to call async method synchronously
        let rt = tokio::runtime::Handle::try_current();
        let accepted = match rt {
            Ok(handle) => {
                // We're inside a tokio runtime, use block_in_place
                let inner = self.inner.clone();
                tokio::task::block_in_place(|| {
                    handle
                        .block_on(async move { inner.process_raw_transactions(vec![raw_tx]).await })
                })
            }
            Err(_) => {
                // Fallback: no runtime available
                0
            }
        };

        if accepted > 0 {
            let len = self.inner.len();
            tracing::debug!(
                mempool_size = len,
                "Added transaction to real mempool pipeline"
            );
            Ok(len as u64)
        } else {
            anyhow::bail!("Transaction rejected by real mempool pipeline")
        }
    }

    /// Find transaction handles from signed transactions
    pub fn find_handles_from_signed_txs(&self, txs: &[crate::tx::SignedTx]) -> Vec<u64> {
        let mempool_txs: Vec<MempoolSignedTx> =
            txs.iter().map(lightnode_tx_to_mempool_tx).collect();
        let real_handles = self.inner.find_handles_from_signed_txs(&mempool_txs);

        let handles: Vec<u64> = real_handles.iter().map(|h| h.0).collect();

        tracing::debug!(
            found_handles = handles.len(),
            searched_txs = txs.len(),
            "Found transaction handles in real mempool pipeline"
        );

        handles
    }

    /// Get sender_id for an address (converts address to sender_id using mempool's sender registry)
    /// This is used when building nonce_updates from overlay
    pub fn get_sender_id_for_address(&self, address: &[u8]) -> u32 {
        self.inner.get_sender_id_for_address(address)
    }

    /// Remove transactions from mempool after block commitment
    pub fn on_block_committed(&self, handles: Vec<u64>) {
        self.on_block_committed_with_nonces(handles, &std::collections::HashMap::new());
    }

    /// Remove transactions from mempool after block commitment and promote queued transactions.
    /// Also clears pending nonces since storage is now updated.
    ///
    /// # Arguments
    /// * `handles` - Transaction handles to remove
    /// * `nonce_updates` - Map of sender_id -> new_account_nonce for accounts that changed
    pub fn on_block_committed_with_nonces(
        &self,
        handles: Vec<u64>,
        nonce_updates: &std::collections::HashMap<u32, u64>,
    ) {
        self.on_block_committed_with_nonces_and_hash(handles, nonce_updates, None);
    }

    /// per-block in-flight map when the block hash is known. Avoids the
    /// blanket `clear_in_flight_txs` that orphaned TXs of still-pending
    /// proposed blocks (see memory/in_flight_orphan_bug.md).
    pub fn on_block_committed_with_nonces_and_hash(
        &self,
        handles: Vec<u64>,
        nonce_updates: &std::collections::HashMap<u32, u64>,
        block_hash: Option<[u8; 64]>,
    ) {
        let real_handles: Vec<RealTxHandle> = handles.iter().map(|&h| RealTxHandle(h)).collect();
        let n = handles.len();
        let promoted = nonce_updates.len();
        self.inner
            .on_block_committed_with_nonces(&real_handles, nonce_updates);
        // known — only drops this block's in-flight entry, leaves
        // other blocks' drained TXs intact. Fall back to the legacy
        // blanket clear when hash is not available (back-compat for
        // callers that don't track block_hash yet).
        match block_hash {
            Some(hash) => self.inner.clear_in_flight_for_block(&hash),
            None => self.inner.clear_in_flight_txs(),
        }
        let remaining = self.inner.len();
        if !nonce_updates.is_empty() {
            tracing::info!(
                removed = n,
                promoted_accounts = promoted,
                remaining,
                "Removed committed transactions and promoted queued"
            );
        } else {
            tracing::debug!(removed = n, remaining, "Removed committed transactions");
        }
        // Clear pending nonces — storage is now updated with committed nonces,
        self.clear_committed_pending_nonces();
    }

    /// Process raw transactions and add valid ones to mempool.
    /// Accepts Vec<Result<SignedTx>> for API compat with existing callers.
    /// Converts to RawTx and feeds the real pipeline.
    pub async fn process_raw_transactions(
        &self,
        txs: Vec<Result<crate::tx::SignedTx, anyhow::Error>>,
    ) -> usize {
        let mut total = 0usize;
        let mut skipped_not_preverified = 0usize;
        let mut deser_errors = 0usize;

        let raw_txs: Vec<RawTx> = txs
            .into_iter()
            .filter_map(|r| {
                total += 1;
                match r {
                    Ok(tx) => {
                        if !tx.pre_verified {
                            skipped_not_preverified += 1;
                            tracing::warn!(
                                from = %tx.from.chars().take(16).collect::<String>(),
                                to = %tx.to.chars().take(16).collect::<String>(),
                                nonce = tx.nonce,
                                "Transaction not pre-verified, skipping"
                            );
                            return None;
                        }
                        // deserializes the raw bytes back as the canonical
                        // `TransactionExt` wire format using bincode FIXINT
                        // encoding. The legacy `lightnode_tx_to_raw_bytes` path
                        // here re-serialized as a *different* `MempoolSignedTx`
                        // struct (Vec<u8> addresses, u64 fee, Vec<u8> sig) using
                        // then read the variable-int bytes as fixint → the first
                        // u64 length prefix decoded as 32 (raw pubkey length)
                        // instead of 64 (hex-string length), `tx.from` was reborn
                        // as 32 raw bytes that didn't match `tx.pubkey`, the
                        // address-spoofing gate fired, and EVERY gossip-received
                        // TX was rejected with `signature verification failed` —
                        // total_input=N reject_signature=N` and `submitted=N
                        // accepted=0` end-to-end. RPC submit was unaffected
                        // because it stores the original loadtest bytes verbatim
                        // and never goes through the repackaging step.
                        //
                        // FIX: re-serialize using the canonical helper
                        // (`tx::serialize_signed_tx`) which mirrors loadtest /
                        // RPC encoding (TransactionExt + fixint) so the
                        // verifies.
                        let bytes = match crate::tx::serialize_signed_tx(&tx) {
                            Ok(b) => b,
                            Err(e) => {
                                deser_errors += 1;
                                tracing::warn!(
                                    error = %e,
                                    "Failed to re-serialize gossip TX into canonical wire format"
                                );
                                return None;
                            }
                        };
                        Some(RawTx {
                            bytes,
                            peer_id: None,
                            recv_ts: std::time::Instant::now(),
                        })
                    }
                    Err(e) => {
                        deser_errors += 1;
                        tracing::warn!(
                            error = %e,
                            "Failed to deserialize transaction"
                        );
                        None
                    }
                }
            })
            .collect();

        if raw_txs.is_empty() {
            tracing::info!(
                total,
                skipped_not_preverified,
                deser_errors,
                "NO transactions submitted to real mempool pipeline (all filtered before RawTx conversion)"
            );
            return 0;
        }

        let count = raw_txs.len();
        let accepted = self.inner.process_raw_transactions(raw_txs).await;

        if accepted > 0 {
            let mempool_len = self.inner.len();
            tracing::info!(
                submitted = count,
                accepted = accepted,
                total = mempool_len,
                total_inputs = total,
                skipped_not_preverified,
                deser_errors,
                "Processed raw transactions through real mempool pipeline - transactions accepted"
            );
        } else if count > 0 {
            tracing::warn!(
                submitted = count,
                accepted = accepted,
                total_inputs = total,
                skipped_not_preverified,
                deser_errors,
                "Processed raw transactions - NO transactions accepted"
            );
        } else {
            let mempool_len = self.inner.len();
            tracing::info!(
                submitted = count,
                accepted = accepted,
                total = mempool_len,
                total_inputs = total,
                skipped_not_preverified,
                deser_errors,
                "Processed raw transactions through real mempool pipeline"
            );
        }

        accepted
    }

    /// Get current mempool size (sync bridge — only safe from a BLOCKING
    /// context, i.e. never call this from inside an async task because
    /// `block_on_current_runtime` returns None under nested async and
    /// this falls back to `unwrap_or(0)` silently, reporting a phantom
    /// empty mempool. Prefer [`Self::len_async`] from async callers.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Async-safe mempool size query. Kept for API compatibility with
    /// callers that already use `.await`; equivalent to [`Self::len`] now
    /// that the outer Mutex has been removed (Tier 3 Fase 2a).
    pub async fn len_async(&self) -> usize {
        self.inner.len()
    }

    /// `RealMempoolPipeline::diag_full_state` for the periodic 10s flight
    /// recorder spawned in main.rs.
    pub fn diag_full_state(
        &self,
    ) -> (usize, usize, usize, usize, usize, usize, u64, u64, u64, u64) {
        self.inner.diag_full_state()
    }

    /// Check if mempool is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Async-safe variant of [`Self::is_empty`].
    pub async fn is_empty_async(&self) -> bool {
        self.len_async().await == 0
    }

    /// Usare SOLO quando la rotation of the proposer è confermata (start proposer,
    /// epoch change, BFT failure definitivo). NON durante normal operation:
    /// non avanza).
    pub fn restore_in_flight_txs(&self) {
        self.reset_pending_nonces();
        self.restore_in_flight_preserve_pending();
    }

    /// Variante "soft": NON resetta pending_nonces, solo restore. Usare nel
    /// path normale dove il proposer corrente returns al mempool TX che
    /// balance insufficiente, etc.). Preservando pending_nonces il blocco
    /// successivo continua la sequenza in-flight without ricominciare.
    pub fn restore_in_flight_preserve_pending(&self) {
        self.inner.restore_in_flight_txs();
    }

    /// Set shard filter for block production (only include TX for our group's shards)
    pub fn set_shard_filter(&self, num_shards: usize, local_shards: Vec<u32>) {
        self.inner.set_shard_filter(num_shards, local_shards);
    }

    /// ROUND 13: Clear in-flight TXs after block commit.
    pub fn clear_in_flight_txs(&self) {
        self.inner.clear_in_flight_txs();
    }

    /// block so later commit/evict can target them precisely — see
    /// `RealMempoolPipeline::record_in_flight_for_block`.
    pub fn record_in_flight_for_block(
        &self,
        block_hash: [u8; 64],
        height: u64,
        signed_txs: &[crate::tx::SignedTx],
    ) {
        if signed_txs.is_empty() {
            return;
        }
        // Convert lightnode SignedTx → RealMempoolTx with stable sender_id.
        // restored later via restore_in_flight_for_block carry the correct
        // sender_id and nonce and can be requeued without re-admission.
        let real_txs: Vec<RealMempoolTx> = signed_txs
            .iter()
            .map(|stx| {
                let sid = self.get_sender_id_for_address(&normalize_address_bytes(&stx.from));
                RealMempoolTx {
                    sender_id: sid,
                    nonce: stx.nonce,
                    fee: stx.fee.unwrap_or(0) as u64,
                    tx_handle: RealTxHandle(0),
                    class: savitri_mempool::mempool::types::TxClass::Financial,
                    stream_nonce: None,
                    inserted: std::time::Instant::now(),
                    tx_hash: None,
                    sender_address: normalize_address_bytes(&stx.from),
                    signature_hash: [0u8; 32],
                    gas_limit: 0,
                    max_fee: stx.fee.unwrap_or(0) as u64,
                    received_at: std::time::Instant::now(),
                    rpc_accepted: false,
                }
            })
            .collect();
        self.inner
            .record_in_flight_for_block(block_hash, height, real_txs);
    }

    /// `RealMempoolPipeline::restore_orphaned_at_height`. Called by the
    /// cert-MATCHED handler in p2p/network/mod.rs whenever a certified
    /// block at height H wins the round, so any of this node's own
    /// proposed blocks at height H (with a different hash) get their TXs
    /// restored instead of being orphaned for 300s.
    pub fn restore_orphaned_at_height(
        &self,
        committed_height: u64,
        committed_hash: &[u8; 64],
    ) -> usize {
        self.inner
            .restore_orphaned_at_height(committed_height, committed_hash)
    }

    /// `RealMempoolPipeline::restore_in_flight_older_than`. Intended to be
    /// invoked by a periodic background task in the lightnode/masternode
    /// main loop (e.g., every 10s with a 30s threshold).
    pub fn restore_in_flight_older_than(&self, max_age: std::time::Duration) -> usize {
        self.inner.restore_in_flight_older_than(max_age)
    }

    /// BFT certificate arrives. Unlike `clear_in_flight_txs`, this does not
    /// wipe other blocks' entries — essential for multi-block pipelining.
    pub fn clear_in_flight_for_block(&self, block_hash: &[u8; 64]) {
        self.inner.clear_in_flight_for_block(block_hash);
    }

    /// received before the tracker timeout). Without this hook the TXs are
    /// permanently lost — see memory/in_flight_orphan_bug.md.
    pub fn restore_in_flight_for_block(&self, block_hash: &[u8; 64]) -> usize {
        self.inner.restore_in_flight_for_block(block_hash)
    }

    /// Drain transactions for block production.
    /// Returns (Vec<MempoolTx>, Vec<SignedTx>) in lightnode types for API compat.
    /// Sync bridge — **do not call from async tasks**; see [`Self::drain_for_block_production_async`].
    pub fn drain_for_block_production(
        &self,
        max_txs: usize,
    ) -> (Vec<MempoolTx>, Vec<crate::tx::SignedTx>) {
        let (real_mempool_txs, real_signed_txs) = self.inner.drain_for_block_production(max_txs);

        // Convert real mempool types to lightnode types
        let mempool_txs: Vec<MempoolTx> = real_mempool_txs
            .iter()
            .map(|rtx| MempoolTx {
                id: rtx.tx_handle.0,
                signed_tx: crate::tx::SignedTx::default(), // placeholder, actual tx is in signed_txs
            })
            .collect();

        let signed_txs: Vec<crate::tx::SignedTx> = real_signed_txs
            .iter()
            .map(mempool_tx_to_lightnode_tx)
            .collect();

        // Fill in the actual signed_tx in mempool_txs
        let mempool_txs: Vec<MempoolTx> = mempool_txs
            .into_iter()
            .zip(signed_txs.iter())
            .map(|(mut mtx, stx)| {
                mtx.signed_tx = stx.clone();
                mtx
            })
            .collect();

        let remaining = self.len();
        tracing::info!(
            drained = mempool_txs.len(),
            remaining,
            "Drained transactions from real mempool pipeline for block production"
        );

        (mempool_txs, signed_txs)
    }

    /// Async variant — **use this from proposer / block production
    /// paths**. The sync [`Self::drain_for_block_production`] is unsafe
    /// from an async task because `block_on_current_runtime` returns
    /// None under nested async and the drain silently returns empty.
    /// That phantom-empty drain is what made multi-group chains produce
    /// 0 non-empty blocks under load even with admission accepting 11k+
    pub async fn drain_for_block_production_async(
        &self,
        max_txs: usize,
    ) -> (Vec<MempoolTx>, Vec<crate::tx::SignedTx>) {
        let (real_mempool_txs, real_signed_txs) = self.inner.drain_for_block_production(max_txs);

        let mempool_txs: Vec<MempoolTx> = real_mempool_txs
            .iter()
            .map(|rtx| MempoolTx {
                id: rtx.tx_handle.0,
                signed_tx: crate::tx::SignedTx::default(),
            })
            .collect();
        let signed_txs: Vec<crate::tx::SignedTx> = real_signed_txs
            .iter()
            .map(mempool_tx_to_lightnode_tx)
            .collect();
        let mempool_txs: Vec<MempoolTx> = mempool_txs
            .into_iter()
            .zip(signed_txs.iter())
            .map(|(mut mtx, stx)| {
                mtx.signed_tx = stx.clone();
                mtx
            })
            .collect();

        let remaining = self.inner.len();
        tracing::info!(
            drained = mempool_txs.len(),
            remaining,
            "Drained transactions (async) from real mempool pipeline for block production"
        );

        (mempool_txs, signed_txs)
    }

    ///
    /// Back-compat shim for callers without a round context — uses
    /// `round_id = 0`, which means FL score sink samples (if any) won't
    /// participate in cross-round streak tracking. New callers should
    pub fn final_validation(
        &self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[crate::tx::SignedTx],
        _storage: Option<&dyn BlockAndAccountStorage>,
    ) -> (Vec<crate::tx::SignedTx>, Vec<TxHandle>) {
        self.final_validation_with_round(mempool_txs, signed_txs, _storage, 0)
    }

    /// pipeline so FL score sink samples carry the round number — needed
    /// for `ObservationStore::bad_fl_streak` to work.
    pub fn final_validation_with_round(
        &self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[crate::tx::SignedTx],
        _storage: Option<&dyn BlockAndAccountStorage>,
        round_id: u64,
    ) -> (Vec<crate::tx::SignedTx>, Vec<TxHandle>) {
        // Convert lightnode types to real mempool types
        // FIX: Use the pipeline's stable sender_id registry instead of local
        // incremental counters. Local counters produced different sender_ids
        // for the same address across drain rounds, causing nonce mismatch
        // to point to wrong senders → nonce mismatch → all TXs rejected.
        let real_mempool_txs: Vec<RealMempoolTx> = mempool_txs
            .iter()
            .zip(signed_txs.iter())
            .map(|(mtx, stx)| {
                let mempool_stx = lightnode_tx_to_mempool_tx(stx);
                // Use the pipeline's persistent sender registry for stable IDs
                let sid = self.get_sender_id_for_address(&normalize_address_bytes(&stx.from));
                RealMempoolTx {
                    sender_id: sid,
                    nonce: stx.nonce,
                    fee: stx.fee.unwrap_or(0) as u64,
                    tx_handle: RealTxHandle(mtx.id),
                    class: savitri_mempool::mempool::types::TxClass::Financial,
                    stream_nonce: None,
                    inserted: std::time::Instant::now(),
                    tx_hash: None,
                    sender_address: normalize_address_bytes(&stx.from),
                    signature_hash: [0u8; 32],
                    gas_limit: 0,
                    max_fee: stx.fee.unwrap_or(0) as u64,
                    received_at: std::time::Instant::now(),
                    rpc_accepted: false,
                }
            })
            .collect();

        let real_signed_txs: Vec<MempoolSignedTx> =
            signed_txs.iter().map(lightnode_tx_to_mempool_tx).collect();

        for (i, stx) in real_signed_txs.iter().enumerate() {
            let addr_hex = hex::encode(&stx.from[..std::cmp::min(8, stx.from.len())]);
            match self.storage.get_account(&stx.from) {
                Ok(Some(bytes)) => match bincode::deserialize::<savitri_core::Account>(&bytes) {
                    Ok(acc) => tracing::info!(
                        idx = i,
                        sender = %addr_hex,
                        tx_nonce = real_mempool_txs[i].nonce,
                        account_nonce = acc.nonce,
                        account_balance = %acc.balance,
                        from_len = stx.from.len(),
                        "DIAG final_validation: account found in storage"
                    ),
                    Err(e) => tracing::warn!(
                        idx = i, sender = %addr_hex, error = %e,
                        "DIAG final_validation: account bytes found but deserialize failed"
                    ),
                },
                Ok(None) => tracing::warn!(
                    idx = i, sender = %addr_hex, from_len = stx.from.len(),
                    "DIAG final_validation: account NOT FOUND in storage (will use default nonce=0, balance=0)"
                ),
                Err(e) => tracing::warn!(
                    idx = i, sender = %addr_hex, error = %e,
                    "DIAG final_validation: storage error"
                ),
            }
        }

        // entries older than PENDING_NONCE_TTL_SECS. Stale entries come from
        // BFT-failed blocks: the proposal was signed, pending_nonces was
        // bumped, but the BFT certificate never arrived and the block was
        // evicted. Without filtering those here, every future drain for those
        // the blocked_senders cascade — producing the valid=0 invalid=100
        //
        // The actual map is pruned lazily by record_proposed_nonces (which
        // takes &mut self); here we only need to filter the view.
        let ttl = std::time::Duration::from_secs(PENDING_NONCE_TTL_SECS);
        let now = std::time::Instant::now();
        let pending_view: std::collections::HashMap<Vec<u8>, u64> = match block_on_current_runtime(
            async {
                let guard = self.pending_nonces.lock().await;
                guard
                    .iter()
                    .filter(|(_, (_, ts))| now.duration_since(*ts) < ttl)
                    .map(|(k, (nonce, _))| (k.clone(), *nonce))
                    .collect::<std::collections::HashMap<_, _>>()
            },
        ) {
            Some(view) => view,
            None => {
                tracing::warn!(
                    "final_validation: cannot read pending_nonces outside Tokio runtime; using empty view"
                );
                std::collections::HashMap::new()
            }
        };
        let pending_ref = if pending_view.is_empty() {
            None
        } else {
            Some(&pending_view)
        };
        let (valid_real_txs, invalid_handles): (Vec<MempoolSignedTx>, Vec<RealTxHandle>) = {
            let result = self.inner.final_validation_with_pending(
                &real_mempool_txs,
                &real_signed_txs,
                self.storage.as_ref(),
                pending_ref,
                round_id,
            );
            tracing::info!(
                valid = result.0.len(),
                invalid = result.1.len(),
                "final_validation: real mempool pipeline returned result"
            );
            result
        };

        // Convert back to lightnode types
        let valid_txs: Vec<crate::tx::SignedTx> = valid_real_txs
            .iter()
            .map(mempool_tx_to_lightnode_tx)
            .collect();

        // Convert invalid handles to lightnode TxHandle
        let handles: Vec<TxHandle> = invalid_handles.iter().map(|h| TxHandle(h.0)).collect();

        tracing::info!(
            valid = valid_txs.len(),
            invalid = handles.len(),
            "Final validation completed through real mempool pipeline"
        );

        (valid_txs, handles)
    }

    /// Record pending nonces from TX that were included in a proposed block.
    /// instead of waiting for the BFT certificate to commit them to storage.
    pub fn record_proposed_nonces(&self, valid_txs: &[crate::tx::SignedTx]) {
        // Prune stale entries first — keeps the bounded-size invariant.
        self.prune_stale_pending_nonces();

        let now = std::time::Instant::now();
        let pending = self.pending_nonces.clone();
        let valid_txs_owned: Vec<(Vec<u8>, u64)> = valid_txs
            .iter()
            .map(|tx| (normalize_address_bytes(&tx.from), tx.nonce + 1))
            .collect();
        let txs_count = valid_txs.len();
        let senders = block_on_current_runtime(async move {
            let mut guard = pending.lock().await;
            for (addr, next_nonce) in valid_txs_owned {
                let entry = guard.entry(addr).or_insert((0, now));
                if next_nonce > entry.0 {
                    entry.0 = next_nonce;
                }
                // Always refresh the timestamp when we record a new proposal so
                // the TTL is measured from the most recent proposer activity, not
                // from the first time this sender was seen.
                entry.1 = now;
            }
            guard.len()
        });
        if txs_count > 0 {
            tracing::info!(
                senders = senders.unwrap_or(0),
                txs = txs_count,
                "Pending nonces recorded for proposed block"
            );
        }
    }

    /// Clear pending nonces for senders whose blocks have been committed.
    /// Called when BFT certificate is received and block is finalized.
    /// Storage nonces are now updated, so pending tracking is no longer needed.
    pub fn clear_committed_pending_nonces(&self) {
        let pending = self.pending_nonces.clone();
        let cleared = block_on_current_runtime(async move {
            let mut guard = pending.lock().await;
            let count = guard.len();
            if count > 0 {
                guard.clear();
            }
            count
        })
        .unwrap_or(0);
        if cleared > 0 {
            tracing::info!(cleared, "Cleared pending nonces after block commit");
        }
    }

    /// Reset all pending nonces. Called on proposer change or BFT failure
    /// to avoid stale nonces that would permanently block senders.
    pub fn reset_pending_nonces(&self) {
        let pending = self.pending_nonces.clone();
        let cleared = block_on_current_runtime(async move {
            let mut guard = pending.lock().await;
            let count = guard.len();
            if count > 0 {
                guard.clear();
            }
            count
        })
        .unwrap_or(0);
        if cleared > 0 {
            tracing::warn!(
                cleared,
                "Reset ALL pending nonces (proposer change or BFT failure)"
            );
        }
    }

    /// Remove pending_nonces entries older than PENDING_NONCE_TTL_SECS.
    /// don't permanently block future drains. Cheap O(n) scan; n is bounded by
    /// max_per_sender * active_senders, typically a few thousand entries at most.
    fn prune_stale_pending_nonces(&self) {
        let cutoff = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(PENDING_NONCE_TTL_SECS));
        let Some(cutoff) = cutoff else { return };
        let pending = self.pending_nonces.clone();
        let (removed, remaining) = block_on_current_runtime(async move {
            let mut guard = pending.lock().await;
            let before = guard.len();
            guard.retain(|_, (_, ts)| *ts >= cutoff);
            let after = guard.len();
            (before.saturating_sub(after), after)
        })
        .unwrap_or((0, 0));
        if removed > 0 {
            tracing::warn!(
                removed,
                remaining,
                ttl_secs = PENDING_NONCE_TTL_SECS,
                "Pruned stale pending_nonces (BFT likely failed; blocks never committed)"
            );
        }
    }
}

// Transaction type alias
pub type Transaction = crate::tx::TransactionExt;

fn validate_block_header(block: &Block) -> Result<()> {
    // Validate block structure
    if block.height == 0 {
        if block.parent_hash != [0u8; 64] {
            anyhow::bail!("Genesis block must have zero parent hash");
        }
        // Note: transactions are not stored in Block struct, only tx_root
    } else {
        if block.parent_hash == [0u8; 64] {
            anyhow::bail!("Regular block must have non-zero parent hash");
        }
        if block.height < 1 {
            anyhow::bail!("Block height must be positive for regular blocks");
        }
    }

    // Validate block hash
    if block.hash == [0u8; 64] {
        anyhow::bail!("Block hash cannot be zero");
    }

    // Validate timestamp (basic check)
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if block.timestamp > current_time + 3600 {
        // Allow 1 hour future tolerance
        anyhow::bail!("Block timestamp is too far in the future");
    }

    if block.timestamp < current_time - 86400 * 30 {
        // Reject blocks older than 30 days
        anyhow::bail!("Block timestamp is too far in the past");
    }

    // Validate state root and transaction root
    if block.state_root == [0u8; 32] {
        anyhow::bail!("State root cannot be zero");
    }

    if block.tx_root == [0u8; 32] {
        anyhow::bail!("Transaction root cannot be zero");
    }

    // Validate transaction count matches transaction root
    // In a real implementation, this would compute the Merkle root of transactions
    // and compare it with block.tx_root

    tracing::debug!(
        height = block.height,
        hash = %hex::encode(block.hash),
        tx_root = %hex::encode(block.tx_root),
        "Block header validation passed"
    );

    Ok(())
}

fn apply_signed_tx_overlay(
    storage: &dyn BlockAndAccountStorage,
    overlay: &mut std::collections::BTreeMap<Vec<u8>, savitri_core::Account>,
    receipts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    tx_bytes: &[u8],
    idx: u64,
) -> Result<()> {
    // Check if transaction bytes are empty or too short
    if tx_bytes.is_empty() {
        anyhow::bail!("Transaction {} has empty bytes", idx);
    }

    // Deserialize the signed transaction using crate::tx (TransactionExt format)
    // which matches the bincode format produced by serialize_signed_tx.
    // NOTE: crate::core::tx::Transaction has incompatible field types (Vec<u8> from/to, u128 amount).
    let signed_tx = match crate::tx::deserialize_signed_tx(tx_bytes) {
        Ok(tx) => tx,
        Err(e) => {
            let error_msg = if e.to_string().contains("unexpected end of file")
                || e.to_string().contains("io error")
            {
                format!(
                    "Transaction {} bytes incomplete or truncated (len={}): {}",
                    idx,
                    tx_bytes.len(),
                    e
                )
            } else {
                format!(
                    "Failed to deserialize transaction {} (len={}): {}",
                    idx,
                    tx_bytes.len(),
                    e
                )
            };
            tracing::warn!(
                error = %error_msg,
                tx_idx = idx,
                bytes_len = tx_bytes.len(),
                "Failed to deserialize signed transaction"
            );
            return Err(anyhow::anyhow!(error_msg));
        }
    };

    // Verify the transaction is pre-verified
    if !signed_tx.pre_verified {
        anyhow::bail!("Transaction {} is not pre-verified", idx);
    }

    // Convert hex-encoded addresses to raw bytes for storage lookup
    let sender_address_bytes = hex::decode(&signed_tx.from)
        .map_err(|e| anyhow::anyhow!("Invalid sender hex address: {}", e))?;
    let recipient_address_bytes = hex::decode(&signed_tx.to)
        .map_err(|e| anyhow::anyhow!("Invalid recipient hex address: {}", e))?;

    // Get or create sender account from overlay or storage
    let mut sender_account = overlay
        .get(&sender_address_bytes)
        .cloned()
        .or_else(|| {
            storage
                .get_account(&sender_address_bytes)
                .ok()
                .flatten()
                .map(|acc| savitri_core::Account {
                    balance: acc.balance,
                    nonce: acc.nonce,
                })
        })
        .unwrap_or_else(|| savitri_core::Account::default());

    // Get or create recipient account from overlay or storage
    let mut recipient_account = overlay
        .get(&recipient_address_bytes)
        .cloned()
        .or_else(|| {
            storage
                .get_account(&recipient_address_bytes)
                .ok()
                .flatten()
                .map(|acc| savitri_core::Account {
                    balance: acc.balance,
                    nonce: acc.nonce,
                })
        })
        .unwrap_or_else(|| savitri_core::Account::default());

    // Validate sender has sufficient balance (amount is u64, widen to u128 for balance check)
    let amount_u128 = signed_tx.amount as u128;
    let total_required = amount_u128
        .checked_add(signed_tx.fee.unwrap_or(0))
        .ok_or_else(|| anyhow::anyhow!("amount + fee overflow"))?;
    if sender_account.balance < total_required {
        anyhow::bail!(
            "Insufficient balance: sender has {}, required {}",
            sender_account.balance,
            total_required
        );
    }

    // Validate nonce
    if sender_account.nonce != signed_tx.nonce {
        anyhow::bail!(
            "Invalid nonce: expected {}, got {}",
            sender_account.nonce,
            signed_tx.nonce
        );
    }

    // Apply transaction to accounts
    sender_account.balance = sender_account
        .balance
        .checked_sub(total_required)
        .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;
    sender_account.nonce += 1;

    recipient_account.balance = recipient_account
        .balance
        .checked_add(amount_u128)
        .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

    // Update accounts in overlay
    overlay.insert(sender_address_bytes.to_vec(), sender_account);
    overlay.insert(recipient_address_bytes.to_vec(), recipient_account);

    // Create receipt
    let receipt_data = format!(
        "tx_idx:{}|sender:{}|recipient:{}|amount:{}|fee:{}|nonce:{}",
        idx,
        hex::encode(&sender_address_bytes),
        hex::encode(&recipient_address_bytes),
        signed_tx.amount,
        signed_tx.fee.unwrap_or(0),
        signed_tx.nonce
    );

    let receipt_key = format!("receipt:{}", idx);
    receipts.push((receipt_key.into_bytes(), receipt_data.into_bytes()));

    tracing::debug!(
        tx_idx = idx,
        sender = %hex::encode(&sender_address_bytes),
        recipient = %hex::encode(&recipient_address_bytes),
        amount = signed_tx.amount,
        fee = signed_tx.fee.unwrap_or(0),
        "Applied transaction to overlay"
    );

    Ok(())
}

///
/// # AUDIT-029: Atomic Block Commit
///
/// Uses `commit_block_batch()` to write all block data (accounts, block, receipts,
/// transactions, chain head) in a single atomic operation. For RocksDB-backed storage,
/// this uses a WriteBatch ensuring all-or-nothing semantics. For in-memory storage,
/// the default sequential implementation is used (already consistent within locks).
fn execute_and_commit_block(
    storage: &dyn BlockAndAccountStorage,
    block: &Block,
    overlay: &std::collections::BTreeMap<Vec<u8>, savitri_core::Account>,
    receipts: &[(Vec<u8>, Vec<u8>)],
    signed_txs: &[SignedTx],
) -> Result<()> {
    execute_and_commit_block_for_group(storage, block, overlay, receipts, signed_txs, None)
}

/// Multi-group variant: when `group_id` is `Some`, chain head / height→hash
/// storage writes are scoped to that group's lane. When `None`, legacy
/// single-lane behaviour (same as the caller above).
pub(crate) fn execute_and_commit_block_for_group(
    storage: &dyn BlockAndAccountStorage,
    block: &Block,
    overlay: &std::collections::BTreeMap<Vec<u8>, savitri_core::Account>,
    receipts: &[(Vec<u8>, Vec<u8>)],
    signed_txs: &[SignedTx],
    group_id: Option<&str>,
) -> Result<()> {
    // Validate block before committing
    validate_block_header(block)?;

    tracing::debug!(
        height = block.height,
        hash = %hex::encode(block.hash),
        account_changes = overlay.len(),
        receipts = receipts.len(),
        group_id = ?group_id,
        "Executing and committing block (atomic batch)"
    );

    // Check if chain head should be updated — read the group-specific head
    // when a group_id is provided, the global one otherwise.
    let head_opt = match group_id {
        Some(gid) if !gid.is_empty() => storage.get_chain_head_for_group(gid),
        _ => storage.get_chain_head(),
    };
    let should_update = match head_opt {
        Ok(Some(current_head)) => {
            if block.height > current_head.height {
                tracing::debug!(
                    new_height = block.height,
                    old_height = current_head.height,
                    "Updating chain head"
                );
                true
            } else {
                false
            }
        }
        Ok(None) => {
            tracing::debug!(height = block.height, "Setting initial chain head");
            true
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                height = block.height,
                "Failed to read current chain head; will attempt commit anyway"
            );
            true
        }
    };

    if !should_update {
        // Block height is not advancing, but we still need to update account nonces
        // from the overlay. This handles out-of-order block arrival (e.g., height=2
        // committed before height=1). We take the MAX nonce to never go backwards.
        if !overlay.is_empty() {
            for (address, account) in overlay.iter() {
                let existing = storage.get_account(address).ok().flatten();
                let existing_nonce = existing.as_ref().map(|a| a.nonce).unwrap_or(0);
                if account.nonce > existing_nonce {
                    // Update just the account with the higher nonce, preserving existing data
                    let existing_data = existing
                        .as_ref()
                        .map(|a| a.data.clone())
                        .unwrap_or_default();
                    let updated = crate::storage::Account {
                        balance: existing.map(|a| a.balance).unwrap_or(account.balance),
                        nonce: account.nonce,
                        data: existing_data,
                    };
                    let _ = storage.put_account(address, &updated);
                    tracing::debug!(
                        address = %hex::encode(&address[..address.len().min(8)]),
                        old_nonce = existing_nonce,
                        new_nonce = account.nonce,
                        "Updated account nonce from out-of-order block"
                    );
                }
            }
        }
        // contenute in blocchi out-of-order o duplicati per altezza (caso
        // multi-group: 2 proposer producono block alla stessa height nel
        // proprio gruppo; il primo committato avanza la chain head, il
        // secondo trova should_update=false e usciva without indicizzare).
        if !signed_txs.is_empty() {
            for signed_tx in signed_txs {
                let tx_bytes = match crate::tx::serialize_signed_tx(signed_tx) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::debug!(error = %e, "Skipping TX index for out-of-order block: serialize failed");
                        continue;
                    }
                };
                let tx_hash = hash_signed_tx_bytes(&tx_bytes);
                let _ = storage.set_transaction_by_hash(&tx_hash, tx_bytes);
                let _ = storage.set_tx_inclusion(&tx_hash, block.height);
            }
            tracing::debug!(
                height = block.height,
                tx_count = signed_txs.len(),
                "Indexed TXs from out-of-order block (chain head not advanced)"
            );
        }
        tracing::debug!(
            height = block.height,
            "Block height not advancing chain head; skipping full commit (accounts updated)"
        );
        return Ok(());
    }

    // Build the accounts list
    let accounts: Vec<(Vec<u8>, crate::storage::Account)> = overlay
        .iter()
        .map(|(address, account)| {
            // Preserve existing account data (contract state/metadata)
            let existing_data = storage
                .get_account(address)
                .ok()
                .flatten()
                .map(|a| a.data)
                .unwrap_or_default();
            (
                address.clone(),
                crate::storage::Account {
                    balance: account.balance,
                    nonce: account.nonce,
                    data: existing_data,
                },
            )
        })
        .collect();

    // Build transaction data
    let mut transactions = Vec::new();
    let mut tx_inclusions = Vec::new();
    for signed_tx in signed_txs {
        let tx_bytes = crate::tx::serialize_signed_tx(signed_tx)
            .map_err(|e| anyhow::anyhow!("Failed to serialize tx: {}", e))?;
        let tx_hash = hash_signed_tx_bytes(&tx_bytes);
        tx_inclusions.push((tx_hash.to_vec(), block.height));
        transactions.push((tx_hash.to_vec(), tx_bytes));
    }

    let commit_data = BlockCommitData {
        block: block.clone(),
        accounts,
        receipts: receipts.to_vec(),
        transactions,
        tx_inclusions,
        group_id: group_id.map(|s| s.to_string()),
    };

    storage.commit_block_batch(commit_data).with_context(|| {
        format!(
            "AUDIT-029: Atomic block commit failed at height {}. \
             No partial writes — storage is consistent at previous head.",
            block.height
        )
    })?;

    tracing::info!(
        height = block.height,
        hash = %hex::encode(block.hash),
        accounts_updated = overlay.len(),
        "Block executed and committed successfully (atomic)"
    );

    Ok(())
}
use std::collections::BTreeMap;
use tracing::{info, warn};

use crate::p2p::dag::{DagBlock, DagManager};
use crate::p2p::helpers::hash64_to_array;
use crate::p2p::types::{BlockPrepError, BlockStateMismatch, PendingBlockData};

/// Register a committed block with the DAG manager.
/// Call this after every successful block commit (local or remote).
pub async fn register_block_in_dag(
    dag: &DagManager,
    block: &Block,
    signed_txs: &[SignedTx],
    group_id: &str,
    parent_hashes: Vec<[u8; 64]>,
    proposer_pou_score: u32,
) {
    // Collect TX hashes for deduplication tracking
    let tx_hashes: Vec<[u8; 32]> = signed_txs
        .iter()
        .filter_map(|tx| {
            crate::tx::serialize_signed_tx(tx).ok().map(|bytes| {
                let full_hash = hash_signed_tx_bytes(&bytes);
                let mut h = [0u8; 32];
                h.copy_from_slice(&full_hash[..32]);
                h
            })
        })
        .collect();

    info!(
        height = block.height,
        group_id,
        tx_count = signed_txs.len(),
        hash = %hex::encode(&block.hash[..16]),
        "Registering block in DAG"
    );

    let dag_block = DagBlock {
        hash: block.hash,
        height: block.height,
        group_id: group_id.to_string(),
        parent_hashes,
        tx_hashes,
        proposer_pou_score,
        timestamp: block.timestamp,
        proposer: block.proposer,
    };

    if let Some(proof) = dag.add_block(dag_block).await {
        tracing::error!(
            proposer = %hex::encode(&proof.proposer[..8]),
            height = proof.height,
            block_a = %hex::encode(&proof.block_hash_a[..16]),
            block_b = %hex::encode(&proof.block_hash_b[..16]),
            "EQUIVOCATION: proposer signed two different blocks at same height — slashing required"
        );
        // TODO: wire to SlashingManager::report_misbehavior() when consensus integration is ready
    }
}

/// Filter signed transactions to remove those already included in the DAG.
/// Returns only transactions not yet seen in any DAG branch.
/// Uses batch deduplication (single lock acquisition) for efficiency.
pub async fn dedup_txs_against_dag(dag: &DagManager, signed_txs: &[SignedTx]) -> Vec<SignedTx> {
    // Collect all TX hashes first, then batch-check against DAG
    let mut tx_hashes: Vec<[u8; 32]> = Vec::with_capacity(signed_txs.len());
    for tx in signed_txs {
        if let Ok(bytes) = crate::tx::serialize_signed_tx(tx) {
            let full_hash = hash_signed_tx_bytes(&bytes);
            let mut h = [0u8; 32];
            h.copy_from_slice(&full_hash[..32]);
            tx_hashes.push(h);
        } else {
            // Can't compute hash — keep it (unseen)
            tx_hashes.push([0u8; 32]);
        }
    }

    // Single lock acquisition for the entire batch
    let unseen_hashes = dag.dedup_transactions(&tx_hashes).await;
    let unseen_set: std::collections::HashSet<[u8; 32]> = unseen_hashes.into_iter().collect();

    let mut result = Vec::with_capacity(signed_txs.len());
    for (tx, h) in signed_txs.iter().zip(tx_hashes.iter()) {
        if unseen_set.contains(h) {
            result.push(tx.clone());
        }
    }

    if result.len() < signed_txs.len() {
        tracing::info!(
            original = signed_txs.len(),
            after_dedup = result.len(),
            duplicates_removed = signed_txs.len() - result.len(),
            "DAG TX deduplication: removed duplicate transactions"
        );
    }
    result
}

/// Execute block transactions and compute state root WITHOUT committing to storage.
/// Uses sequential batch execution (optimized with caching and write batching).
///
/// # Overlay Lifetime & Memory Management
///
/// This function returns an overlay (BTreeMap of account changes) and receipts.
/// **IMPORTANT**: The caller should NOT store the overlay long-term. The overlay
/// is only needed to compute the state_root and tx_root for the block header.
///
/// ## Recommended Usage Pattern
///
/// ```ignore
/// // 1. Execute without commit - overlay is temporary
/// let (block, overlay, receipts) = execute_block_without_commit(storage, block, txs)?;
/// // overlay is used here only to compute roots (already done inside the function)
///
/// // 2. Drop overlay immediately - don't store it
/// drop(overlay);
/// drop(receipts);
///
/// // 3. Store only PendingBlockData (block + signed_txs) for later commit
/// let pending = PendingBlockData { block, signed_txs, source_peer };
///
/// // 4. On commit, execute_and_commit_block() will re-execute transactions
/// // This is safe because execution is deterministic
/// ```
///
/// ## Why Not Store the Overlay?
///
/// 1. **Memory**: Overlay can be large (all modified accounts)
/// 2. **Determinism**: Re-execution produces identical results
/// 3. **Simplicity**: No need to manage overlay lifecycle across async boundaries
/// 4. **Safety**: If block is rejected, overlay is already dropped (no cleanup needed)
///
/// # Thread Safety
///
/// This function is thread-safe for concurrent calls with different blocks.
/// The overlay is local to each call and doesn't share state.
/// However, concurrent execute + commit for the SAME block should be avoided
/// (the caller should serialize these operations via the receipt quorum mechanism).
pub fn execute_block_without_commit(
    storage: &dyn BlockAndAccountStorage,
    mut block: Block,
    signed_txs: &[SignedTx],
) -> Result<(
    Block,
    BTreeMap<Vec<u8>, savitri_core::Account>,
    Vec<(Vec<u8>, Vec<u8>)>,
)> {
    // Validate block header first
    validate_block_header(&block)?;

    tracing::debug!(
        height = block.height,
        tx_count = signed_txs.len(),
        "Executing block without commit"
    );

    // Create overlay for account changes
    let mut overlay: BTreeMap<Vec<u8>, savitri_core::Account> = BTreeMap::new();
    let mut receipts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    // Sort transactions by (sender, nonce) for correct execution order
    let mut sorted_indices: Vec<usize> = (0..signed_txs.len()).collect();
    sorted_indices.sort_by(|&a, &b| {
        let tx_a = &signed_txs[a];
        let tx_b = &signed_txs[b];
        (&tx_a.from, tx_a.nonce).cmp(&(&tx_b.from, tx_b.nonce))
    });

    // Execute each transaction in nonce order per sender
    for &idx in &sorted_indices {
        let signed_tx = &signed_txs[idx];
        // Serialize the transaction using the same options as deserialize_signed_tx
        let tx_bytes = match crate::tx::serialize_signed_tx(signed_tx) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    tx_idx = idx,
                    "Failed to serialize transaction"
                );
                anyhow::bail!("Failed to serialize transaction {}: {}", idx, e);
            }
        };

        // Apply transaction to overlay
        apply_signed_tx_overlay(storage, &mut overlay, &mut receipts, &tx_bytes, idx as u64)
            .with_context(|| format!("Failed to apply transaction {} to overlay", idx))?;
    }

    // Compute state root from overlay
    // In a real implementation, this would compute a Merkle root of all account states
    let state_root = compute_state_root_from_overlay(&overlay);
    block.state_root = state_root;

    // Compute transaction root from receipts
    // In a real implementation, this would compute a Merkle root of all receipts
    let tx_root = compute_tx_root_from_receipts(&receipts);
    block.tx_root = tx_root;

    // Recompute block hash with updated roots
    block.hash = compute_block_hash(&block);

    tracing::debug!(
        height = block.height,
        hash = %hex::encode(block.hash),
        state_root = %hex::encode(block.state_root),
        tx_root = %hex::encode(block.tx_root),
        accounts_updated = overlay.len(),
        receipts = receipts.len(),
        "Block execution completed without commit"
    );

    Ok((block, overlay, receipts))
}

///
/// When a block carries a valid `BlockAcceptanceCertificate`, the masternode quorum has
/// and harmful (local state may diverge from proposer's state, causing overlay failures).
///
/// This function applies balance/nonce changes directly from the signed transactions:
/// - For each TX: debit sender (amount + fee), credit receiver (amount), increment sender nonce
/// - Returns nonce_updates map for mempool cleanup
///
/// Execute block transactions using Block-STM (parallel) or sequential fallback.
///
/// Block-STM is used when there are >1 transactions, providing parallel execution
/// with conflict detection and re-execution. Single-TX blocks or Block-STM failures
/// fall back to the sequential `apply_certified_block_direct` implementation.
///
/// # Safety
/// Only call this for blocks that have been certified by the masternode quorum.
pub fn execute_block_transactions(
    storage: &dyn BlockAndAccountStorage,
    block: &Block,
    signed_txs: &[SignedTx],
) -> Result<(
    BTreeMap<Vec<u8>, savitri_core::Account>,
    Vec<(Vec<u8>, Vec<u8>)>,
)> {
    // Use Block-STM for blocks with multiple transactions
    if signed_txs.len() > 1 {
        match crate::p2p::block_stm::execute_block_stm(storage, signed_txs) {
            Ok(result) => {
                tracing::info!(
                    height = block.height,
                    txs = signed_txs.len(),
                    rounds = result.rounds,
                    reexecutions = result.reexecutions,
                    overlay_accounts = result.overlay.len(),
                    "Block executed via Block-STM (parallel)"
                );
                return Ok((result.overlay, result.receipts));
            }
            Err(e) => {
                tracing::warn!(
                    height = block.height,
                    error = %e,
                    "Block-STM execution failed, falling back to sequential"
                );
                // Fall through to sequential execution
            }
        }
    }

    // Sequential fallback (single TX or Block-STM failure)
    apply_certified_block_direct(storage, block, signed_txs)
}

/// Sequential block execution (fallback when Block-STM is not applicable).
///
/// # Safety
/// Only call this for blocks that have been certified by the masternode quorum.
pub fn apply_certified_block_direct(
    storage: &dyn BlockAndAccountStorage,
    block: &Block,
    signed_txs: &[SignedTx],
) -> Result<(
    BTreeMap<Vec<u8>, savitri_core::Account>,
    Vec<(Vec<u8>, Vec<u8>)>,
)> {
    tracing::debug!(
        height = block.height,
        tx_count = signed_txs.len(),
        "Applying MN-certified block directly (skip overlay validation)"
    );

    let mut overlay: BTreeMap<Vec<u8>, savitri_core::Account> = BTreeMap::new();
    let mut receipts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    // Sort transactions by (sender, nonce) so they execute in correct order.
    // TXs may arrive out-of-order but must be applied sequentially per sender.
    let mut sorted_indices: Vec<usize> = (0..signed_txs.len()).collect();
    sorted_indices.sort_by(|&a, &b| {
        let tx_a = &signed_txs[a];
        let tx_b = &signed_txs[b];
        (&tx_a.from, tx_a.nonce).cmp(&(&tx_b.from, tx_b.nonce))
    });

    for &idx in &sorted_indices {
        let signed_tx = &signed_txs[idx];
        let sender_address = hex::decode(&signed_tx.from)
            .map_err(|e| anyhow::anyhow!("Invalid sender hex address: {}", e))?;
        let recipient_address = hex::decode(&signed_tx.to)
            .map_err(|e| anyhow::anyhow!("Invalid recipient hex address: {}", e))?;

        // Get sender from overlay first (for multi-tx from same sender), then storage
        let mut sender_account = overlay
            .get(&sender_address)
            .cloned()
            .or_else(|| {
                storage
                    .get_account(&sender_address)
                    .ok()
                    .flatten()
                    .map(|acc| savitri_core::Account {
                        balance: acc.balance,
                        nonce: acc.nonce,
                    })
            })
            .unwrap_or_default();

        let mut recipient_account = overlay
            .get(&recipient_address)
            .cloned()
            .or_else(|| {
                storage
                    .get_account(&recipient_address)
                    .ok()
                    .flatten()
                    .map(|acc| savitri_core::Account {
                        balance: acc.balance,
                        nonce: acc.nonce,
                    })
            })
            .unwrap_or_default();

        let amount_u128 = signed_tx.amount as u128;
        let fee = signed_tx.fee.unwrap_or(0);
        let total_debit = amount_u128.saturating_add(fee);

        // Accept exact match (normal case) or skip if below (already applied).
        // For nonce ahead of local state: fast-forward to trust MN certificate,
        // since local storage may lag behind when blocks from other groups
        // arrive out-of-order.
        if signed_tx.nonce < sender_account.nonce {
            continue; // Already applied
        }
        if signed_tx.nonce > sender_account.nonce {
            // Trust MN certificate: advance local nonce to match
            tracing::debug!(
                height = block.height,
                sender = %signed_tx.from.chars().take(16).collect::<String>(),
                local_nonce = sender_account.nonce,
                tx_nonce = signed_tx.nonce,
                "Fast-forwarding nonce for MN-certified block (local state lagging)"
            );
            sender_account.nonce = signed_tx.nonce;
        }

        // Enforce sufficient balance even for MN-certified blocks
        if sender_account.balance < total_debit {
            tracing::warn!(
                height = block.height,
                tx_idx = idx,
                sender = %signed_tx.from,
                balance = sender_account.balance,
                required = total_debit,
                "Skipping TX with insufficient balance in MN-certified block"
            );
            continue;
        }

        sender_account.balance -= total_debit;
        sender_account.nonce += 1;

        recipient_account.balance = recipient_account.balance.saturating_add(amount_u128);

        overlay.insert(sender_address.clone(), sender_account);
        overlay.insert(recipient_address.clone(), recipient_account);

        // Build receipt
        let receipt_data = format!(
            "tx_idx:{}|sender:{}|recipient:{}|amount:{}|fee:{}|nonce:{}",
            idx,
            hex::encode(&sender_address),
            hex::encode(&recipient_address),
            signed_tx.amount,
            fee,
            signed_tx.nonce,
        );
        receipts.push((
            format!("receipt:{}", idx).into_bytes(),
            receipt_data.into_bytes(),
        ));
    }

    tracing::info!(
        height = block.height,
        accounts_updated = overlay.len(),
        txs_applied = signed_txs.len(),
        "MN-certified block applied directly (no overlay validation)"
    );

    Ok((overlay, receipts))
}

/// Compute state root from account overlay
pub fn compute_state_root_from_overlay(
    overlay: &BTreeMap<Vec<u8>, savitri_core::Account>,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();

    // Sort accounts by address for deterministic hashing
    let mut sorted_accounts: Vec<_> = overlay.iter().collect();
    sorted_accounts.sort_by_key(|(addr, _)| *addr);

    for (address, account) in sorted_accounts {
        // Hash address and account state
        hasher.update(address);
        hasher.update(&account.balance.to_le_bytes());
        hasher.update(&account.nonce.to_le_bytes());
        // Note: Account only has balance and nonce, no data field
    }

    let result = hasher.finalize();
    let mut state_root = [0u8; 32];
    state_root.copy_from_slice(&result);
    state_root
}

/// Compute transaction root from receipts
pub fn compute_tx_root_from_receipts(receipts: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();

    // Sort receipts by key for deterministic hashing
    let mut sorted_receipts: Vec<_> = receipts.iter().collect();
    sorted_receipts.sort_by_key(|(key, _)| key.clone());

    for (key, receipt_data) in sorted_receipts {
        hasher.update(key);
        hasher.update(receipt_data);
    }

    let result = hasher.finalize();
    let mut tx_root = [0u8; 32];
    tx_root.copy_from_slice(&result);
    tx_root
}

/// Canonical empty state root (same as compute_state_root_from_overlay with empty overlay).
/// Used so proposer and re-execution agree when block has no state changes.
pub fn canonical_empty_state_root_32() -> [u8; 32] {
    compute_state_root_from_overlay(&std::collections::BTreeMap::new())
}

/// Canonical empty tx root (same as compute_tx_root_from_receipts with empty receipts).
/// Used so proposer and re-execution agree when block has 0 transactions.
pub fn canonical_empty_tx_root_32() -> [u8; 32] {
    compute_tx_root_from_receipts(&[])
}

/// Canonical empty roots as 64-byte arrays (for proposal wire format).
pub fn canonical_empty_state_root_64() -> [u8; 64] {
    let r = canonical_empty_state_root_32();
    let mut out = [0u8; 64];
    out[..32].copy_from_slice(&r);
    out
}

pub fn canonical_empty_tx_root_64() -> [u8; 64] {
    let r = canonical_empty_tx_root_32();
    let mut out = [0u8; 64];
    out[..32].copy_from_slice(&r);
    out
}

/// Compute block hash from block data.
///
/// savitri_consensus. The formula is unchanged: SHA-256 over
/// `parent_hash ‖ state_root_pad64 ‖ tx_root_pad64 ‖ height_le`,
/// zero-padded to 64 bytes. Wrapper kept for API stability; callers
/// pass `&Block` and we forward the primitive fields.
pub fn compute_block_hash(block: &Block) -> [u8; 64] {
    savitri_consensus::primitives::hashing::compute_block_hash(
        &block.parent_hash,
        &block.state_root,
        &block.tx_root,
        block.height,
    )
}

/// Optional roots from the masternode certificate (state_root, tx_root).
/// When provided (e.g. from BlockWithCertificate), the block is built with these roots
/// so the expected hash matches the MN and re-execution can succeed if parent state matches.
pub type RemoteBlockRoots = ([u8; 32], [u8; 32]);

///
/// If `cert_roots` is `Some((state_root, tx_root))` (e.g. from BlockWithCertificate),
/// the block is built with these roots instead of deriving them, so the block hash
/// matches the MN and re-execution only fails if parent state diverges.
/// When `cert_parent_hash` is `Some`, the block's parent_hash is set from the certificate
/// so the block hash formula (parent_hash || state_root || tx_root || height) matches the MN.
pub fn prepare_remote_block(
    storage: &dyn BlockAndAccountStorage,
    msg: &BlockMessage,
    source_peer: &PeerId,
    cert_roots: Option<RemoteBlockRoots>,
    cert_parent_hash: Option<[u8; 64]>,
    cert_timestamp: Option<u64>,
) -> Result<Option<PendingBlockData>, BlockPrepError> {
    tracing::debug!(
        hash = %hex::encode(msg.hash),
        height = msg.header.exec_height,
        peer = %source_peer,
        "Preparing remote block"
    );

    // Decode transactions from block message
    let signed_txs = match decode_block_txs(msg) {
        Ok(txs) => txs,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to decode block transactions");
            return Err(BlockPrepError::Validation(e));
        }
    };

    // Convert signed transactions to public transactions
    let transactions = match signed_to_public_transactions(&signed_txs) {
        Ok(txs) => txs,
        Err(e) => {
            tracing::warn!(error = %e, "Failed to convert transactions");
            return Err(BlockPrepError::Validation(e));
        }
    };

    // Create block from message
    let mut block = block_from_message(msg, transactions);
    // Use MN-agreed roots from certificate when present (so expected hash matches MN)
    if let Some((state_root, tx_root)) = cert_roots {
        let non_zero = state_root != [0u8; 32] || tx_root != [0u8; 32];
        if non_zero {
            block.state_root = state_root;
            block.tx_root = tx_root;
            tracing::debug!(
                height = block.height,
                "Using state_root/tx_root from certificate"
            );
        }
    }
    // Parent hash: from certificate when provided (so block hash matches MN), else from storage
    if let Some(ph) = cert_parent_hash {
        block.parent_hash = ph;
        tracing::debug!(height = block.height, "Using parent_hash from certificate");
    } else if block.height > 0 {
        if let Ok(Some(parent)) = storage.get_block(block.height - 1) {
            block.parent_hash = parent.hash;
        }
    }
    // Timestamp: from certificate when provided
    if let Some(ts) = cert_timestamp {
        if ts > 0 {
            block.timestamp = ts;
        }
    }

    // Diagnostic: verify recomputed hash matches wire hash after applying cert values
    if cert_roots.is_some() || cert_parent_hash.is_some() {
        let recomputed = compute_block_hash(&block);
        if recomputed == msg.hash {
            tracing::debug!(
                height = block.height,
                "Block hash matches after applying certificate roots"
            );
        } else {
            tracing::warn!(
                height = block.height,
                wire_hash = %hex::encode(&msg.hash[..8]),
                recomputed_hash = %hex::encode(&recomputed[..8]),
                "Block hash mismatch after applying cert roots"
            );
        }
    }

    // Validate the block
    if let Err(e) = validate_block_header(&block) {
        tracing::warn!(error = %e, "Block validation failed");
        return Err(BlockPrepError::Validation(e));
    }

    // Check if we already have this block
    if let Ok(Some(existing_block)) = storage.get_block(block.height) {
        if existing_block.hash == block.hash {
            // il rate di cert ridondanti vs altri skip path. Counter visibile
            // nel diagnostic dashboard.
            tracing::info!(
                height = block.height,
                hash = %hex::encode(&block.hash[..8]),
                "prepare_remote_block: SKIP duplicate (block already at this height with same hash)"
            );
            return Ok(None);
        }
    }

    // Check if we have the parent block
    if block.height > 0 {
        if storage.get_block(block.height - 1).is_err() {
            tracing::warn!(
                height = block.height,
                parent_height = block.height - 1,
                "prepare_remote_block: MISSING PARENT — cert dropped, no retry"
            );
            return Err(BlockPrepError::MissingParent {
                parent_exec: [0u8; 64],
                target_height: block.height,
            });
        }
    }

    let pending_data = PendingBlockData {
        block,
        signed_txs,
        source_peer: *source_peer, // Use PeerId directly instead of string
    };

    tracing::debug!(
        height = pending_data.block.height,
        hash = %hex::encode(pending_data.block.hash),
        "Remote block prepared successfully"
    );

    Ok(Some(pending_data))
}

/// Decode transactions from a block message.
fn decode_block_txs(msg: &BlockMessage) -> anyhow::Result<Vec<Transaction>> {
    let mut signed_txs: Vec<Transaction> = Vec::new();

    for tx_bytes in &msg.txs {
        // Skip empty or too short transaction bytes
        if tx_bytes.is_empty() {
            tracing::debug!("Skipping empty transaction bytes in block message");
            continue;
        }

        match crate::tx::deserialize_signed_tx(tx_bytes) {
            Ok(tx) => {
                signed_txs.push(tx);
            }
            Err(e) => {
                // Only log as warning if it's not an "unexpected end of file" error
                if e.to_string().contains("unexpected end of file")
                    || e.to_string().contains("io error")
                {
                    tracing::debug!(
                        bytes_len = tx_bytes.len(),
                        error = %e,
                        "Transaction bytes incomplete or truncated in block message, skipping"
                    );
                } else {
                    tracing::warn!(
                        bytes_len = tx_bytes.len(),
                        error = %e,
                        "Failed to deserialize transaction from block message"
                    );
                }
                // Continue with other transactions instead of failing completely
            }
        }
    }

    tracing::debug!(
        total_txs = msg.txs.len(),
        decoded_txs = signed_txs.len(),
        "Decoded transactions from block message"
    );

    Ok(signed_txs)
}

/// Check if an error indicates a missing parent block.
fn is_missing_parent_error(err: &anyhow::Error) -> bool {
    err.to_string()
        .contains("orphan-exec: missing parent_exec block")
}

/// Convert signed transactions to public transactions.
fn signed_to_public_transactions(signed: &[SignedTx]) -> anyhow::Result<Vec<Transaction>> {
    let mut public_txs = Vec::new();

    for signed_tx in signed {
        // Convert signed transaction to public transaction
        // In a real implementation, this would extract the public data
        let public_tx = Transaction {
            from: signed_tx.from.clone(),
            to: signed_tx.to.clone(),
            amount: signed_tx.amount,
            nonce: signed_tx.nonce,
            fee: signed_tx.fee,
            data: None,         // Remove data for public transaction
            pubkey: Vec::new(), // Remove pubkey for public transaction
            sig: [0u8; 64],     // Remove signature for public transaction
            pre_verified: false,
        };

        public_txs.push(public_tx);
    }

    tracing::debug!(
        signed_count = signed.len(),
        public_count = public_txs.len(),
        "Converted signed transactions to public transactions"
    );

    Ok(public_txs)
}

/// Convert a block message to a Block struct.
/// Populates all fields available from the message (proposer from header) and
fn block_from_message(msg: &BlockMessage, transactions: Vec<Transaction>) -> Block {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&msg.hash);
    hasher.update(&msg.header.exec_height.to_le_bytes());
    for tx in &transactions {
        hasher.update(tx.from.as_bytes());
        hasher.update(tx.to.as_bytes());
        hasher.update(&tx.amount.to_le_bytes());
    }
    let tx_root: [u8; 32] = hasher.finalize().into();
    let mut state_hasher = Sha256::new();
    state_hasher.update(&tx_root);
    state_hasher.update(&msg.hash[..32]);
    let state_root: [u8; 32] = state_hasher.finalize().into();

    Block {
        hash: msg.hash,
        height: msg.header.exec_height,
        timestamp: if msg.header.timestamp > 0 {
            msg.header.timestamp
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        },
        // parent_hash propagated in BlockHeader (wire format). Older peers without
        // the field serialize as [0;64] via serde default — prepare_remote_block
        // non-genesis. With updated peers this now carries the correct value end-to-end.
        parent_hash: msg.header.parent_hash,
        state_root,
        tx_root,
        proposer: msg.header.proposer,
        signature: [0u8; 64], // Not in BlockMessage
        parent_exec_hash: [0u8; 64],
        parent_ref_hash: [0u8; 64],
        version: 1,
    }
}

/// Commit a pending block to storage.
///
/// # Parametri
/// - `storage`: Storage per committare il blocco
/// - `pending`: Dati of the blocco da committare
/// - `p2p_nodes`: Opzionale list di (account_address, pou_score) per i nodi P2P.
///   Se fornita, la distribuzione dei fee includerà la distribuzione P2P proporzionale al PoU.
///   Se None, tutto il proposer+P2P reward va al proposer (comportamento legacy).
///
/// # Overlay Re-execution
///
/// This function calls `execute_and_commit_block()` which re-executes all transactions
/// to produce the overlay and then commits it to storage. This is intentional:
///
/// - The overlay from `execute_block_without_commit()` was dropped after block production
/// - Re-execution is safe because transaction execution is deterministic
/// - This avoids holding overlay memory during the quorum wait period
///
/// # Thread Safety
///
/// This function acquires write locks on storage. Concurrent commits for different
/// blocks are safe. However, the caller should ensure that:
/// 1. Only one commit is attempted per block hash
/// 2. Commit is only called after receiving quorum/masternode acceptance
///
/// The certificate-based finality (PoU-BFT) ensures these invariants:
/// blocks are only committed after receiving a valid ConsensusCertificate.
pub fn commit_pending_block(
    storage: &dyn BlockAndAccountStorage,
    pending: PendingBlockData,
    p2p_nodes: Option<Vec<([u8; 32], u64)>>,
    is_mn_certified: bool,
    group_id: Option<&str>,
) -> Result<[u8; 64]> {
    tracing::debug!(
        height = pending.block.height,
        hash = %hex::encode(pending.block.hash),
        tx_count = pending.signed_txs.len(),
        is_mn_certified,
        "Committing pending block"
    );

    // For MN-certified blocks, skip overlay re-execution (it may fail due to state divergence).
    let (executed_block, overlay, receipts) = if is_mn_certified {
        let (overlay, receipts) =
            apply_certified_block_direct(storage, &pending.block, &pending.signed_txs)?;
        (pending.block.clone(), overlay, receipts)
    } else {
        let (executed_block, overlay, receipts) =
            execute_block_without_commit(storage, pending.block.clone(), &pending.signed_txs)?;

        // Verify the executed block matches the pending block
        if executed_block.hash != pending.block.hash {
            warn!(
                expected_hash = %hex::encode(pending.block.hash),
                actual_hash = %hex::encode(executed_block.hash),
                expected_state_root = %hex::encode(pending.block.state_root),
                actual_state_root = %hex::encode(executed_block.state_root),
                expected_tx_root = %hex::encode(pending.block.tx_root),
                actual_tx_root = %hex::encode(executed_block.tx_root),
                height = pending.block.height,
                tx_count = pending.signed_txs.len(),
                "Block hash mismatch after execution - committing with local state (root computation mismatch)"
            );
        }
        (executed_block, overlay, receipts)
    };

    // Commit the block with overlay and receipts (group-scoped when cert carries group_id)
    execute_and_commit_block_for_group(
        storage,
        &executed_block,
        &overlay,
        &receipts,
        &pending.signed_txs,
        group_id,
    )?;

    // Distribute collected transaction fees (burn 50%, treasury 30%, proposer 20%)
    let total_fees: u128 = pending
        .signed_txs
        .iter()
        .map(|tx| tx.fee.unwrap_or(0))
        .fold(0u128, |acc, fee| acc.saturating_add(fee));
    if total_fees > 0 {
        distribute_block_fees(
            storage,
            total_fees,
            pending.block.height,
            &pending.block.proposer,
        )?;
    }

    // Handle P2P node reward distribution if nodes are provided
    if let Some(nodes) = p2p_nodes {
        distribute_fees_to_p2p_nodes(storage, &nodes, pending.block.height)?;
    }

    #[cfg(feature = "metrics")]
    {
        gauge!("block_height").set(pending.block.height as f64);
        counter!("blocks_produced_total").increment(1);
        counter!("transactions_confirmed_total").increment(pending.signed_txs.len() as u64);
        if total_fees > 0 {
            counter!("fee_collected_total").increment(total_fees as u64);
            counter!("fee_burned_total").increment((total_fees / 2) as u64);
            counter!("fee_distributed_total").increment((total_fees - total_fees / 2) as u64);
        }
    }
    tracing::info!(
        height = pending.block.height,
        hash = %hex::encode(pending.block.hash),
        total_fees = total_fees,
        "Block committed successfully"
    );

    Ok(pending.block.hash)
}

/// Distribute fees to P2P nodes
fn distribute_fees_to_p2p_nodes(
    storage: &dyn BlockAndAccountStorage,
    p2p_nodes: &[([u8; 32], u64)],
    block_height: u64,
) -> Result<()> {
    for (account_address, pou_score) in p2p_nodes {
        // Calculate reward based on PoU score
        let reward = calculate_p2p_reward(*pou_score);

        // Get or create P2P node account
        let mut account = storage
            .get_account(account_address)
            .ok()
            .flatten()
            .map(|acc| savitri_core::Account {
                balance: acc.balance,
                nonce: acc.nonce,
            })
            .unwrap_or_else(|| savitri_core::Account::default());

        // Add reward to account
        account.balance = account
            .balance
            .checked_add(reward)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow for P2P node"))?;

        // Update account in storage, preserving existing data
        let existing_data = storage
            .get_account(account_address)
            .ok()
            .flatten()
            .map(|a| a.data)
            .unwrap_or_default();
        let storage_account = crate::storage::Account {
            balance: account.balance,
            nonce: account.nonce,
            data: existing_data,
        };
        storage.put_account(account_address, &storage_account)?;

        tracing::debug!(
            account = %hex::encode(account_address),
            reward = reward,
            height = block_height,
            "Distributed reward to P2P node"
        );
    }

    Ok(())
}

/// Calculate P2P reward based on PoU score
fn calculate_p2p_reward(pou_score: u64) -> u128 {
    // Simple linear reward calculation based on PoU score
    // In a real implementation, this would use a more sophisticated formula
    const BASE_REWARD: u128 = 1000; // Base reward in smallest units
    const SCORE_MULTIPLIER: u128 = 10; // Multiplier for PoU score

    BASE_REWARD + (pou_score as u128 * SCORE_MULTIPLIER)
}

/// Well-known treasury address for fee collection (matches tokens.toml treasury_address)
const TREASURY_ADDRESS: [u8; 32] = {
    let mut addr = [0u8; 32];
    addr[31] = 0x01;
    addr
};

/// Distribute collected transaction fees from a block.
///
/// Fee split (tokenomics):
///   - 50% burn (deflationary - not credited anywhere)
///   - 30% treasury (well-known address 0x...01)
///   - 20% block proposer
fn distribute_block_fees(
    storage: &dyn BlockAndAccountStorage,
    total_fees: u128,
    block_height: u64,
    proposer: &[u8; 32],
) -> Result<()> {
    if total_fees == 0 {
        return Ok(());
    }

    let burn_amount = total_fees / 2; // 50% burn
    let remaining = total_fees - burn_amount;
    // 30% of total to treasury, 20% of total to proposer
    // Using integer math: treasury = remaining * 60 / 100, proposer = remaining - treasury
    let treasury_amount = remaining * 60 / 100; // 30% of total
    let proposer_amount = remaining - treasury_amount; // 20% of total

    // Credit treasury (preserve existing account data)
    if treasury_amount > 0 {
        let mut treasury_account = storage
            .get_account(&TREASURY_ADDRESS)
            .unwrap_or(None)
            .unwrap_or_else(|| crate::storage::Account {
                balance: 0,
                nonce: 0,
                data: Vec::new(),
            });
        treasury_account.balance = treasury_account.balance.saturating_add(treasury_amount);
        storage.put_account(&TREASURY_ADDRESS, &treasury_account)?;
    }

    // Credit proposer (preserve existing account data)
    if proposer_amount > 0 {
        let mut proposer_account = storage
            .get_account(proposer)
            .unwrap_or(None)
            .unwrap_or_else(|| crate::storage::Account {
                balance: 0,
                nonce: 0,
                data: Vec::new(),
            });
        proposer_account.balance = proposer_account.balance.saturating_add(proposer_amount);
        storage.put_account(proposer, &proposer_account)?;
    }

    tracing::info!(
        height = block_height,
        total_fees = total_fees,
        burned = burn_amount,
        treasury = treasury_amount,
        proposer_reward = proposer_amount,
        proposer = %hex::encode(proposer),
        "Block fees distributed"
    );

    Ok(())
}

/// Finalize and commit a remote block after receipt quorum.
///
/// Migrated to use handle-based cleanup instead of hash-based cleanup.
/// Uses MempoolPipeline::find_handles_from_signed_txs() to find handles
/// for committed transactions, then calls on_block_committed() with handles.
/// Finalize and commit a remote block after certificate receipt.
///
/// Returns `true` if commit succeeded, `false` if it failed (for retry mechanism).
pub async fn finalize_remote_block_commit(
    storage: &dyn BlockAndAccountStorage,
    pending: &PendingBlockData,
    integrity_events: &Option<tokio::sync::mpsc::Sender<crate::integrity::IntegrityEvent>>,
    pipeline: Option<&mut MempoolPipeline>,
    pou_state: &PouState,
    peer_accounts: &std::collections::HashMap<libp2p::PeerId, crate::p2p::types::PeerInfo>,
    known_peer_accounts: &std::collections::HashMap<libp2p::PeerId, [u8; 32]>,
    masternode_address: &str,
    proposer_reward: u64,
    p2p_reward: u64,
    group_id: Option<&str>,
) -> bool {
    // MempoolPipeline already defined at module level
    use tracing::{debug, info, warn};

    let height = pending.block.height;
    let hash_hex = hex::encode(pending.block.hash);
    let source_peer = pending.source_peer.clone();
    let txs_count = pending.signed_txs.len();
    let signed_txs = pending.signed_txs.clone();

    // Collect P2P nodes for fee distribution
    let p2p_nodes = collect_p2p_nodes_for_fee_distribution(
        &pou_state,
        &peer_accounts,
        &known_peer_accounts,
        masternode_address,
    )
    .await;

    // Calculate fee distribution
    let fee_distribution = calculate_fee_distribution(
        proposer_reward.into(),
        p2p_reward.into(),
        &p2p_nodes,
        txs_count,
    );

    // Log fee distribution summary (actual distribution happens in commit_pending_block)
    info!(
        height,
        proposer_fee = %fee_distribution.proposer_fee,
        p2p_nodes = fee_distribution.p2p_fees.len(),
        total = %fee_distribution.total_distributed,
        "Fee distribution calculated for block"
    );

    // Validate fee distribution
    if let Err(e) = crate::p2p::fee_distribution::validate_fee_distribution(
        &fee_distribution,
        proposer_reward.into(),
        p2p_reward.into(),
    ) {
        warn!("Fee distribution validation failed: {}", e);
    }
    // Build nonce_updates directly from signed transactions for MN-certified blocks.
    // Skip overlay re-execution: it often fails for remote blocks because local state
    // diverges from proposer's state (different nonce/balance). The MN certificate
    // guarantees all TXs were valid on the proposer. For each sender: new_nonce = max(tx.nonce) + 1.
    let mut nonce_updates = std::collections::HashMap::new();
    if let Some(ref pipeline_ref) = pipeline {
        // Fix 4: compute nonce_updates only for TXs that were actually applied
        // (consecutive from storage committed nonce), not max(tx.nonce)+1 which
        // would advance the mempool past nonces that were rejected due to gaps.
        let mut sender_committed: std::collections::HashMap<Vec<u8>, u64> =
            std::collections::HashMap::new();
        let mut sorted_for_nonce = signed_txs.clone();
        sorted_for_nonce.sort_by(|a, b| {
            let addr_a = normalize_address_bytes(&a.from);
            let addr_b = normalize_address_bytes(&b.from);
            addr_a.cmp(&addr_b).then(a.nonce.cmp(&b.nonce))
        });
        for tx in &sorted_for_nonce {
            let sender_address = normalize_address_bytes(&tx.from);
            let expected = sender_committed
                .entry(sender_address.clone())
                .or_insert_with(|| {
                    storage
                        .get_account(&sender_address)
                        .ok()
                        .flatten()
                        .map(|acc| acc.nonce)
                        .unwrap_or(0)
                });
            if tx.nonce == *expected {
                *expected += 1;
            }
            // if tx.nonce != expected, it was rejected — don't advance
        }
        // Include receivers: a transfer to a fresh account creates it with nonce=0
        // and may unblock queued_pool entries (nonce=0,1,...) the loadtest client
        // submitted before funding arrived. Without this, promote(receiver, 0) is
        // never called and those TXs are frozen forever.
        for tx in &sorted_for_nonce {
            let receiver_address = normalize_address_bytes(&tx.to);
            sender_committed
                .entry(receiver_address.clone())
                .or_insert_with(|| {
                    storage
                        .get_account(&receiver_address)
                        .ok()
                        .flatten()
                        .map(|acc| acc.nonce)
                        .unwrap_or(0)
                });
        }
        let sender_max_nonce = sender_committed; // compat with downstream code
        for (sender_address, new_nonce) in &sender_max_nonce {
            let sender_id = pipeline_ref.get_sender_id_for_address(sender_address);
            nonce_updates.insert(sender_id, *new_nonce);
            debug!(
                sender_id = sender_id,
                sender_address = %hex::encode(&sender_address[..sender_address.len().min(8)]),
                new_nonce = new_nonce,
                "Built nonce update from MN-certified signed_txs"
            );
        }
        if !nonce_updates.is_empty() {
            info!(
                accounts_updated = nonce_updates.len(),
                total_txs = signed_txs.len(),
                "Built nonce_updates from MN-certified block for {} accounts",
                nonce_updates.len()
            );
        }
    }

    // Commit with is_mn_certified=true: skips overlay re-execution inside commit_pending_block,
    // applies certified TXs directly to storage via apply_certified_block_direct().
    match commit_pending_block(storage, pending.clone(), None, true, group_id) {
        Ok(committed_hash) => {
            // Remove committed transactions from mempool using handle-based cleanup
            // Find handles for committed transactions (some may not be in local mempool if remote block)
            if let Some(pipeline_ref) = pipeline {
                let committed_handles = pipeline_ref.find_handles_from_signed_txs(&signed_txs);
                let handles_count = committed_handles.len();
                // Always call on_block_committed_with_nonces to promote queued TXs,
                // even when committed_handles is empty (remote blocks from other LNs).
                // The mempool handles empty handles gracefully (remove_by_handles is a no-op).
                pipeline_ref.on_block_committed_with_nonces(committed_handles, &nonce_updates);
                if handles_count > 0 {
                    info!(
                        handles_removed = handles_count,
                        promoted_accounts = nonce_updates.len(),
                        total_txs = txs_count,
                        "Removed {} committed transactions and promoted queued transactions from mempool",
                        handles_count
                    );
                } else if !nonce_updates.is_empty() {
                    info!(
                        promoted_accounts = nonce_updates.len(),
                        total_txs = txs_count,
                        "Promoted queued transactions for {} accounts (remote block, no local handles)",
                        nonce_updates.len()
                    );
                }
            }

            let timestamp_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            #[cfg(feature = "metrics")]
            {
                gauge!("block_height").set(height as f64);
                counter!("blocks_received_total").increment(1);
                counter!("transactions_confirmed_total").increment(txs_count as u64);
            }
            info!(
                hash = %hash_hex,
                committed_hash = %hex::encode(committed_hash),
                height,
                txs = txs_count,
                peer = %source_peer,
                "Stored remote block after quorum receipts"
            );
            info!(
                block_hash = %hash_hex,
                height,
                txs_count = txs_count,
                timestamp = timestamp_secs,
                "✅ Block committed (block_hash, height, txs_count, timestamp)"
            );
            if txs_count > 0 {
                info!(
                    block_hash = %hash_hex,
                    block_height = height,
                    count = txs_count,
                    timestamp = timestamp_secs,
                    "Transactions confirmed: {} transaction(s) in block",
                    txs_count
                );
                for (idx, tx) in signed_txs.iter().enumerate() {
                    let tx_hash_hex = crate::tx::serialize_signed_tx(tx)
                        .ok()
                        .as_ref()
                        .map(|b| hex::encode(hash_signed_tx_bytes(b)))
                        .unwrap_or_else(|| "?".to_string());
                    info!(
                        block_hash = %hash_hex,
                        block_height = height,
                        tx_index = idx,
                        tx_hash = %tx_hash_hex,
                        from = %tx.from,
                        to = %tx.to,
                        timestamp = timestamp_secs,
                        "Transaction confirmed: tx_hash={} in block height={}",
                        tx_hash_hex,
                        height
                    );
                }
            }
            info!(
                height,
                hash = %hash_hex,
                timestamp = timestamp_secs,
                "Block committed after certificate (finality path)"
            );
            if let Some(sender) = integrity_events.as_ref() {
                integrity::emit_event(sender, &source_peer, IntegrityKind::Success);
            }
            true // Commit riuscito
        }
        Err(err) => {
            warn!(
                hash = %hash_hex,
                height,
                peer = %source_peer,
                error = ?err,
                "Failed to commit remote block after certificate receipt"
            );
            // Even though the commit failed, the block WAS certified by masternodes.
            // CRITICAL ORDER: Update STORAGE nonces AND balances FIRST, then promote mempool.
            // Promotion re-runs admission control which reads storage nonces.
            // If storage isn't updated first, promoted txs get re-queued.
            //
            // Step 1: Apply balance+nonce changes from certified block directly to storage.
            // Reuse apply_certified_block_direct to compute the overlay, then write to storage.
            {
                match apply_certified_block_direct(storage, &pending.block, &signed_txs) {
                    Ok((overlay, _receipts)) => {
                        let mut storage_updates = 0usize;
                        for (address, core_account) in &overlay {
                            // Preserve existing account data (contract state/metadata)
                            let existing_data = storage
                                .get_account(address)
                                .ok()
                                .flatten()
                                .map(|a| a.data)
                                .unwrap_or_default();
                            let updated = crate::storage::Account {
                                balance: core_account.balance,
                                nonce: core_account.nonce,
                                data: existing_data,
                            };
                            if storage.put_account(address, &updated).is_ok() {
                                storage_updates += 1;
                            }
                        }
                        if storage_updates > 0 {
                            info!(
                                storage_updates,
                                height,
                                "Updated {} storage accounts (nonces+balances) from MN-certified block (commit failed)",
                                storage_updates
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            height,
                            error = %e,
                            "Failed to apply certified block in fallback recovery"
                        );
                    }
                }
            }
            // Step 2: NOW promote mempool (admission control will see updated storage nonces).
            if !nonce_updates.is_empty() {
                if let Some(pipeline_ref) = pipeline {
                    let committed_handles = pipeline_ref.find_handles_from_signed_txs(&signed_txs);
                    pipeline_ref.on_block_committed_with_nonces(committed_handles, &nonce_updates);
                    info!(
                        promoted_accounts = nonce_updates.len(),
                        total_txs = txs_count,
                        height,
                        "Promoted queued txs from certified block (commit failed but block is MN-certified)"
                    );
                }
            }
            if let Some(sender) = integrity_events.as_ref() {
                integrity::emit_event(sender, &source_peer, IntegrityKind::Fault);
            }
            false // Commit fallito
        }
    }
}
