//! Savitri RPC - Public HTTP API for lightnode and masternode

mod contract;
pub mod dag;
mod handlers;
mod jsonrpc;
mod pou;
mod server;
mod types;

pub use contract::{
    CallContractRequest, CallContractResponse, ContractExecutor, DeployContractRequest,
    DeployContractResponse,
};
pub use handlers::*;
pub use pou::{MasternodePouInfo, MasternodePouReader, NetworkReader, PouGroupInfo, PouReader};
pub use server::run_server;
pub use types::*;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use savitri_mempool::mempool::integration::{MempoolPipeline, MempoolStatsSnapshot};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MempoolCounterTotals {
    pub admitted_total: u64,
    pub queued_total: u64,
    pub rejected_total: u64,
    pub removed_total: u64,
    pub evicted_total: u64,
    pub confirmed_total: u64,
}

impl MempoolCounterTotals {
    fn saturating_sub(self, baseline: Self) -> Self {
        Self {
            admitted_total: self.admitted_total.saturating_sub(baseline.admitted_total),
            queued_total: self.queued_total.saturating_sub(baseline.queued_total),
            rejected_total: self.rejected_total.saturating_sub(baseline.rejected_total),
            removed_total: self.removed_total.saturating_sub(baseline.removed_total),
            evicted_total: self.evicted_total.saturating_sub(baseline.evicted_total),
            confirmed_total: self
                .confirmed_total
                .saturating_sub(baseline.confirmed_total),
        }
    }
}

impl From<&MempoolStatsSnapshot> for MempoolCounterTotals {
    fn from(snapshot: &MempoolStatsSnapshot) -> Self {
        Self {
            admitted_total: snapshot.admitted_total,
            queued_total: snapshot.queued_total,
            rejected_total: snapshot.rejected_total,
            removed_total: snapshot.removed_total,
            evicted_total: snapshot.evicted_total,
            confirmed_total: snapshot.confirmed_total,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MempoolCounterWindows {
    pub one_minute: MempoolCounterTotals,
    pub one_hour: MempoolCounterTotals,
}

#[derive(Debug, Default)]
pub(crate) struct MempoolStatsWindowTracker {
    samples: std::collections::VecDeque<(Instant, MempoolCounterTotals)>,
}

impl MempoolStatsWindowTracker {
    const RETENTION: Duration = Duration::from_secs(2 * 60 * 60);

    pub fn observe(&mut self, snapshot: &MempoolStatsSnapshot) -> MempoolCounterWindows {
        let now = Instant::now();
        let totals = MempoolCounterTotals::from(snapshot);
        self.samples.push_back((now, totals));
        self.prune_old(now);

        MempoolCounterWindows {
            one_minute: self.window_delta(now, Duration::from_secs(60), totals),
            one_hour: self.window_delta(now, Duration::from_secs(60 * 60), totals),
        }
    }

    fn prune_old(&mut self, now: Instant) {
        let cutoff = now - Self::RETENTION;
        while let Some((ts, _)) = self.samples.front() {
            if *ts >= cutoff {
                break;
            }
            self.samples.pop_front();
        }
    }

    fn window_delta(
        &self,
        now: Instant,
        window: Duration,
        latest: MempoolCounterTotals,
    ) -> MempoolCounterTotals {
        let cutoff = now - window;
        let baseline = self
            .samples
            .iter()
            .rev()
            .find_map(|(ts, totals)| if *ts <= cutoff { Some(*totals) } else { None })
            .or_else(|| self.samples.front().map(|(_, totals)| *totals))
            .unwrap_or(latest);

        latest.saturating_sub(baseline)
    }
}

/// Faucet configuration (testnet only)
#[derive(Clone)]
pub struct FaucetConfig {
    /// Ed25519 keypairs for the 10 faucet wallets (round-robin)
    pub keypairs: Vec<ed25519_dalek::SigningKey>,
    /// Amount to send per claim (e.g. 5 SAVT = 5 * 10^18)
    pub amount_per_claim: u64,
    /// Fee for the faucet tx (e.g. 1 SAVT)
    pub fee: u128,
}

impl FaucetConfig {
    /// 5 SAVT (5 * 10^18). Note: 5k SAVT exceeds u64, using 5 SAVT for testnet.
    pub const AMOUNT_PER_CLAIM: u64 = 5_000_000_000_000_000_000;
    /// 1 SAVT
    pub const FEE: u128 = 1_000_000_000_000_000_000;

    /// Load faucet config from JSON file (faucet_keys_testnet.json)
    /// Format: { "private_keys": ["hex64", "hex64", ...] }
    pub fn load_from_json_path(path: &std::path::Path) -> anyhow::Result<Arc<Self>> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read faucet keys: {}", e))?;
        let json: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("invalid faucet keys JSON: {}", e))?;
        let keys = json
            .get("private_keys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("missing private_keys array"))?;
        let mut keypairs = Vec::with_capacity(keys.len());
        for (i, key_hex) in keys.iter().enumerate() {
            let s = key_hex
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("private key {} is not a string", i))?;
            let bytes = hex::decode(s.trim_start_matches("0x"))
                .map_err(|e| anyhow::anyhow!("invalid hex in key {}: {}", i, e))?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("key {} must be 32 bytes", i))?;
            keypairs.push(ed25519_dalek::SigningKey::from_bytes(&arr));
        }
        if keypairs.is_empty() {
            anyhow::bail!("faucet config requires at least one keypair");
        }
        Ok(Arc::new(Self {
            keypairs,
            amount_per_claim: Self::AMOUNT_PER_CLAIM,
            fee: Self::FEE,
        }))
    }

    /// Return a new FaucetConfig that only owns the slice of keypairs this
    /// shard is responsible for. `shard_idx` must be in `[0, shard_count)`.
    /// Keys are split as evenly as possible: shard i owns
    /// `keypairs[floor(i*N/M) .. floor((i+1)*N/M)]` where N = total keys and
    /// M = shard_count.
    ///
    /// When every RPC node is assigned a disjoint shard, the round-robin
    /// selection inside `faucet_claim` can never pick the same keypair on
    /// two nodes concurrently, so the cross-node nonce race ("same key,
    /// same committed nonce, two parallel submits → one duplicate") is
    /// eliminated entirely. Intra-node races are handled separately by
    /// `faucet_pending_nonces`.
    pub fn with_shard(
        self: &Arc<Self>,
        shard_idx: usize,
        shard_count: usize,
    ) -> anyhow::Result<Arc<Self>> {
        if shard_count == 0 {
            anyhow::bail!("shard_count must be > 0");
        }
        if shard_idx >= shard_count {
            anyhow::bail!("shard_idx {} >= shard_count {}", shard_idx, shard_count);
        }
        let n = self.keypairs.len();
        let start = (shard_idx * n) / shard_count;
        let end = ((shard_idx + 1) * n) / shard_count;
        if start >= end {
            anyhow::bail!(
                "shard {}/{} of {} keys produces empty slice; not enough keys",
                shard_idx,
                shard_count,
                n
            );
        }
        let sliced: Vec<_> = self.keypairs[start..end].iter().cloned().collect();
        Ok(Arc::new(Self {
            keypairs: sliced,
            amount_per_claim: self.amount_per_claim,
            fee: self.fee,
        }))
    }
}

/// RPC server state (lightnode or masternode)
pub struct RpcState {
    pub storage: Option<Arc<dyn savitri_storage::StorageTrait>>,
    /// pipeline is internally synchronized, so the previous outer `Mutex`
    /// was redundant and risked silent ptr_eq drift between RPC ingress and
    /// proposer drain (an earlier fix root cause).
    pub mempool: Option<Arc<MempoolPipeline>>,
    pub pou_reader: Option<Arc<dyn PouReader>>,
    pub network_reader: Option<Arc<dyn NetworkReader>>,
    pub masternode_pou_reader: Option<Arc<dyn MasternodePouReader>>,
    pub contract_executor: Option<Arc<dyn ContractExecutor>>,
    /// Faucet config for testnet (optional)
    pub faucet_config: Option<Arc<FaucetConfig>>,
    /// Round-robin index for faucet wallet selection
    pub faucet_index: std::sync::atomic::AtomicUsize,
    /// Lock to serialize faucet claims (prevents TOCTOU race conditions)
    pub faucet_lock: Mutex<()>,
    /// Per-faucet-key monotonic next-nonce cache. Keyed on the public key
    /// (32 bytes). The faucet handler increments this under faucet_lock on
    /// every successful mempool admission — regardless of when the tx is
    /// committed on-chain — so back-to-back claims on the same key don't
    /// re-read a stale `storage.get_account().nonce` and submit with the
    /// same nonce ("Duplicate nonce 0 for sender N" / "duplicate
    /// transaction" errors under load). First claim on a key seeds the
    /// entry from storage.
    pub faucet_pending_nonces: Mutex<std::collections::HashMap<[u8; 32], u64>>,
    /// Rolling window tracker for mempool cumulative counters.
    pub(crate) mempool_stats_tracker: Mutex<MempoolStatsWindowTracker>,
    /// Semaphore to limit concurrent TX submissions (prevents spam DoS from starving block production)
    pub tx_submission_semaphore: Arc<tokio::sync::Semaphore>,
    /// DAG reader for dag_* RPC endpoints
    pub dag_reader: Option<Arc<dyn dag::DagReader>>,
    /// Channel-based TX ingestion: RPC pushes here, consumer task writes to mempool.
    /// Decouples RPC from mempool lock — block production never contends with RPC.
    pub tx_channel: Option<tokio::sync::mpsc::Sender<TxSubmission>>,
    /// P1: shard-aware TX dispatch. When set and the TX's sender shard belongs
    /// to a different group than the local node's, the router forwards the raw
    /// TX bytes via gossipsub to the target group's topic and returns the tx_hash,
    /// skipping local mempool admission. When None (SINGLE_GROUP or
    /// pre-registration), TX flows through the existing channel → mempool path.
    pub tx_router: Option<Arc<dyn TxRouter>>,
    /// `consensus_getProposer` RPC so load-test clients can route TX directly
    /// to the node currently producing blocks for the local group, bypassing
    /// the HaveTx/TxFetch propagation delay that causes 99% of TX to expire
    /// via TTL before reaching the proposer's mempool.
    pub proposer_state: Option<Arc<dyn ProposerStateReader>>,
}

/// Read-only view of consensus proposer state, used by `consensus_getProposer`.
///
/// The lightnode implements this by wrapping the
/// `is_intragroup_proposer: Arc<RwLock<bool>>` flag, local peer id, and the
/// current `P2PGroupManager` snapshot. Masternodes return a sentinel answer
/// since they don't produce intra-group blocks.
#[async_trait::async_trait]
pub trait ProposerStateReader: Send + Sync + std::fmt::Debug {
    /// True iff this node is currently the elected proposer of its group.
    async fn is_local_proposer(&self) -> bool;
    /// Local libp2p peer id (string form).
    fn local_node_id(&self) -> String;
    /// Group id this node belongs to, if the group has already been assigned.
    async fn current_group_id(&self) -> Option<String>;
    /// Full shard_id → group_id map learned from GroupAnnouncements.
    /// Used by benchmark tooling (rpc-loadtest) to compute the (src_group,
    /// dst_group) matrix of a workload locally without round-tripping per
    /// TX. Returns an empty map before the first announcement is processed.
    async fn shard_map(&self) -> std::collections::HashMap<u32, String>;
    /// Total number of shards (global constant, same across all groups).
    async fn num_shards(&self) -> u32;
}

/// Decision from the TX router for an incoming RPC-submitted raw transaction.
#[derive(Debug)]
pub enum TxRouteDecision {
    /// Admit into the local mempool (same-group or no-group-assigned yet).
    Local,
    /// Forward to another group via gossipsub; the router has already published
    /// the payload — the RPC handler just returns the tx_hash to the client.
    /// Retained for back-compat / best-effort mode; under the hard-affinity
    /// policy (Q3=a) the lightnode router prefers `Reject` instead, because
    /// cross-group gossip forward was the silent source of "TX lost in
    /// queued pool forever" under load.
    Forwarded { tx_hash: [u8; 32] },
    /// The router could not determine the target group; fall back to local admit.
    FallbackLocal,
    /// Hard-reject: the TX's sender shard belongs to a different group. The
    /// client is expected to retry against an RPC node that is in the target
    /// group. `target_group_id` is returned to the caller in the error body
    /// so the client can look up / learn the correct endpoint.
    ///
    /// implementation no longer produces this variant — cross-group TX are
    /// always forwarded (gossip + optional direct-send via env-gated
    /// `SAVITRI_TX_ROUTER_DIRECT_SEND`). Kept here for back-compat with
    /// external `TxRouter` impls; will be removed once all known
    /// implementations migrate.
    #[deprecated(
        since = "0.1.0",
        note = "Tier 4 Fase 2: lightnode router never emits Reject; cross-group TX always forwarded"
    )]
    Reject {
        tx_hash: [u8; 32],
        target_group_id: String,
        local_group_id: String,
        shard_id: u32,
    },
    /// (target group == local group) but the intra-group gossip publish
    /// FAILED — typically because the swarm command channel was full or
    /// closed. Without gossip the elected proposer (a peer in the same
    /// group, post-rotation) never sees the TX, and the ingress LN's local
    /// admit alone produces a stranded TX that expires unconsumed.
    /// Returning this variant lets the RPC handler emit a retryable error
    /// (HTTP 503-equivalent) so the client backs off and retries instead of
    /// silently losing the TX. Pre-fix the router swallowed the publish
    /// failure and returned `Local`, with the observed pathology documented
    /// in `memory/rpc_local_no_gossip_2026-04-29.md`.
    RetryGossipUnavailable {
        tx_hash: [u8; 32],
        local_group_id: String,
        reason: String,
    },
}

/// Pluggable router that the RPC handler calls before local admit. Implementations
/// must be cheap — this runs on the hot submission path.
pub trait TxRouter: Send + Sync + std::fmt::Debug {
    fn route(&self, raw_bytes: &[u8]) -> TxRouteDecision;
}

/// A TX submission through the channel: raw bytes + oneshot for the result
pub struct TxSubmission {
    pub raw_tx: savitri_mempool::mempool::types::RawTx,
    pub response: tokio::sync::oneshot::Sender<Result<[u8; 32], String>>,
}

/// Spawn the mempool consumer task. Reads from the channel, batches TX,
/// acquires the mempool lock once per batch. After insertion, broadcasts
/// accepted TX via the gossipsub sender so other nodes (including the proposer)
/// receive them.
pub fn spawn_mempool_consumer(
    mut rx: tokio::sync::mpsc::Receiver<TxSubmission>,
    pipeline: Arc<MempoolPipeline>,
    tx_broadcast: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // When this task exits silently (panic or channel close) all
        // subsequent `handlers::send_raw_transaction` submissions look
        // like 'TX queue full' from the client, with no server-side
        // evidence of the cause. These log lines let us see
        //   * that the task was spawned in the first place
        //   * whether it reached the loop
        //   * whether it panicked (stderr from the runtime is not
        //     visible in the tracing log otherwise)
        //   * whether it exited because the RPC channel closed
        tracing::info!("TX consumer: task spawned, entering recv loop");

        // Run the main loop inside an AssertUnwindSafe boundary so a
        // doesn't vanish into thin air.
        let consumer_fut = async move {
            let mut total_batches: u64 = 0;
            let mut total_txs: u64 = 0;
            let mut batch: Vec<TxSubmission> = Vec::with_capacity(100);
            loop {
                batch.clear();
                match rx.recv().await {
                    Some(tx) => batch.push(tx),
                    None => {
                        tracing::warn!(
                            total_batches,
                            total_txs,
                            "TX consumer: recv channel CLOSED, task exiting cleanly"
                        );
                        break;
                    }
                }
                while batch.len() < 100 {
                    match rx.try_recv() {
                        Ok(tx) => batch.push(tx),
                        Err(_) => break,
                    }
                }
                let batch_size = batch.len();
                total_batches += 1;
                total_txs += batch_size as u64;
                if total_batches == 1 || total_batches % 100 == 0 {
                    tracing::info!(
                        total_batches,
                        total_txs,
                        batch_size,
                        "TX consumer: batch processed (rate-limited heartbeat)"
                    );
                }
                for submission in batch.drain(..) {
                    let raw_bytes = submission.raw_tx.bytes.clone();
                    let result = pipeline
                        .process_single_raw_transaction(submission.raw_tx)
                        .await
                        .map_err(|e| format!("{:?}", e));
                    if result.is_ok() {
                        if let Some(ref broadcaster) = tx_broadcast {
                            if let Err(e) = broadcaster.try_send(raw_bytes) {
                                tracing::warn!("TX consumer: broadcast channel send failed: {}", e);
                            }
                        } else {
                            tracing::warn!("TX consumer: no broadcast channel configured");
                        }
                    } else if let Err(ref reason) = result {
                        static REJECT_CTR: std::sync::atomic::AtomicU64 =
                            std::sync::atomic::AtomicU64::new(0);
                        let n = REJECT_CTR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if n == 1 || n % 500 == 0 {
                            tracing::warn!(
                                total_reject = n,
                                reason = %reason,
                                "TX consumer: process_single_raw_transaction rejected"
                            );
                        }
                    }
                    let _ = submission.response.send(result);
                }
            }
        };

        // Run the loop in an inner spawned task so we can observe panics
        // via the JoinHandle. tokio converts a panicked task into
        // JoinError::is_panic() on await.
        let inner = tokio::spawn(consumer_fut);
        match inner.await {
            Ok(()) => {
                tracing::info!("TX consumer: task completed normally");
            }
            Err(join_err) => {
                if join_err.is_panic() {
                    let payload = join_err.into_panic();
                    let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
                        (*s).to_string()
                    } else if let Some(s) = payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "(non-string panic payload)".to_string()
                    };
                    tracing::error!(
                        panic = %msg,
                        "TX consumer: task PANICKED — all future RPC submissions will back up on the channel"
                    );
                } else {
                    tracing::error!(
                        ?join_err,
                        "TX consumer: task exited with JoinError (cancelled)"
                    );
                }
            }
        }
    })
}

impl RpcState {
    /// Create for lightnode (storage + mempool required).
    /// Call `with_tx_channel` after to enable channel-based TX ingestion.
    pub fn new(
        storage: Arc<dyn savitri_storage::StorageTrait>,
        mempool: Arc<MempoolPipeline>,
    ) -> Self {
        Self {
            storage: Some(storage),
            mempool: Some(mempool),
            pou_reader: None,
            network_reader: None,
            masternode_pou_reader: None,
            contract_executor: None,
            faucet_config: None,
            faucet_index: std::sync::atomic::AtomicUsize::new(0),
            faucet_lock: Mutex::new(()),
            faucet_pending_nonces: Mutex::new(std::collections::HashMap::new()),
            mempool_stats_tracker: Mutex::new(MempoolStatsWindowTracker::default()),
            tx_submission_semaphore: Arc::new(tokio::sync::Semaphore::new(500)),
            tx_channel: None,
            dag_reader: None,
            tx_router: None,
            proposer_state: None,
        }
    }

    /// Enable channel-based TX ingestion (call after new())
    pub fn with_tx_channel(mut self, sender: tokio::sync::mpsc::Sender<TxSubmission>) -> Self {
        self.tx_channel = Some(sender);
        self
    }

    /// Enable shard-aware TX routing (P1). See `TxRouter` trait docs.
    pub fn with_tx_router(mut self, router: Arc<dyn TxRouter>) -> Self {
        self.tx_router = Some(router);
        self
    }

    pub fn with_proposer_state(mut self, reader: Arc<dyn ProposerStateReader>) -> Self {
        self.proposer_state = Some(reader);
        self
    }

    /// Set DAG reader for dag_* RPC endpoints
    pub fn with_dag_reader(mut self, reader: Arc<dyn dag::DagReader>) -> Self {
        self.dag_reader = Some(reader);
        self
    }

    /// Create for masternode (storage optional, masternode_pou_reader required)
    pub fn for_masternode(
        masternode_pou_reader: Arc<dyn MasternodePouReader>,
        storage: Option<Arc<dyn savitri_storage::StorageTrait>>,
    ) -> Self {
        Self {
            storage,
            mempool: None,
            pou_reader: None,
            network_reader: None,
            masternode_pou_reader: Some(masternode_pou_reader),
            contract_executor: None,
            faucet_config: None,
            faucet_index: std::sync::atomic::AtomicUsize::new(0),
            faucet_lock: Mutex::new(()),
            faucet_pending_nonces: Mutex::new(std::collections::HashMap::new()),
            mempool_stats_tracker: Mutex::new(MempoolStatsWindowTracker::default()),
            tx_submission_semaphore: Arc::new(tokio::sync::Semaphore::new(500)),
            tx_channel: None,
            dag_reader: None,
            tx_router: None,
            proposer_state: None,
        }
    }

    pub fn with_pou_reader(mut self, pou_reader: Arc<dyn PouReader>) -> Self {
        self.pou_reader = Some(pou_reader);
        self
    }

    pub fn with_network_reader(mut self, network_reader: Arc<dyn NetworkReader>) -> Self {
        self.network_reader = Some(network_reader);
        self
    }

    pub fn with_masternode_pou_reader(mut self, reader: Arc<dyn MasternodePouReader>) -> Self {
        self.masternode_pou_reader = Some(reader);
        self
    }

    pub fn with_contract_executor(mut self, executor: Arc<dyn ContractExecutor>) -> Self {
        self.contract_executor = Some(executor);
        self
    }

    /// Enable faucet for testnet (10 wallet keypairs)
    pub fn with_faucet(mut self, config: Arc<FaucetConfig>) -> Self {
        self.faucet_config = Some(config);
        self
    }
}

/// RPC configuration
#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub enabled: bool,
    pub bind_addr: String,
    pub port: u16,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: "127.0.0.1".into(),
            port: 8545,
        }
    }
}
