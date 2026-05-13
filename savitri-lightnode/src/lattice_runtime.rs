//! Lattice runtime — gossip wiring + periodic cell publisher + periodic
//! commit task for the V0.2 Phase 2 DAG ordering layer.
//!
//! This module is the lightnode-side counterpart to the
//! `savitri-consensus::lattice` aggregator. It owns the shared
//! `LatticeAggregator` state behind an `Arc<RwLock<…>>`, spawns:
//!
//! 1. A **cell publisher** task that every
//!    `LATTICE_ROUND_DURATION_SECS` builds a fresh `LatticeCell`,
//!    signs it with the LN identity key, and publishes on the
//!    `/savitri/group/<gid>/lattice/cell/1` topic.
//!
//! 2. A **commit poller** task that every two lattice rounds (one
//!    cycle) computes the pivot via `pivot_for_cycle` and calls
//!    `LineageCommit::try_commit`. The outcome is logged via
//!    `DIAG[lattice-commit]` and — in observation-only mode (the
//!    default while `SAVITRI_CONSENSUS_VERSION` is unset or `v1`) —
//!    has no effect on chain state. When the env var is set to `v2`,
//!    a commit emits a chain notification (wired by callers in a
//!    follow-up patch).
//!
//! The receive-side handlers (`process_cell_message`,
//! `process_attestation_message`) verify the incoming gossipsub
//! message and feed the aggregator. After a cell is observed, the
//! local LN automatically publishes its own attestation if it is a
//! member of the cell's group — this is what produces the BFT 2f+1
//! quorum needed for the cell to be certified.
//!
//! ## Migration mode
//!
//! Env var `SAVITRI_CONSENSUS_VERSION`:
//!   - unset, `v1`, or empty → **observation-only**. The Lattice runs,
//!     records cells + attestations, logs commit decisions, but the
//!     chain state machine is governed by the V0.1 single-proposer
//!     `BlockCertificate` path.
//!   - `v2` → **authoritative**. Lattice cycle commits drive chain
//!     state. This mode requires the cluster-wide flag day described
//!     in `docs/CONSENSUS_V0.2_DESIGN.md` §5.2.
//!
//! ## Status
//!
//! Phase 2.4.2: skeleton + publisher + commit poller + observability
//! counters + per-cycle DIAG snapshot. Observation-only by default.
//! Authoritative-mode chain hook remains a TODO marked in code with
//! `phase2-authoritative` comment; spec for the hook will land in
//! issue #33 (Phase 2.5 design).

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::Signer;
use libp2p::gossipsub::IdentTopic;
#[cfg(feature = "metrics")]
use metrics::{counter, gauge};
use savitri_consensus::lattice::{
    AggregatorConfig, AttestationOutcome, CommitDecision, LatticeAggregator, LineageCommit,
};
use savitri_consensus::types::lattice::{
    BatchRoot, CellAttestation, CellCertificate, CellId, CycleIndex, LatticeCell, LatticeRound,
    LATTICE_ROUND_DURATION_SECS,
};
use savitri_core::crypto::Keypair;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Observability counters (Phase 2.4.2)
// ---------------------------------------------------------------------------
//
// Counters are emitted via the `metrics` crate when the feature is enabled.
// Naming follows the pattern documented in `docs/CONSENSUS_V0.2_DESIGN.md` §6.1
// for LatencyCanon. All counters carry a `group` label so per-group rates are
// visible in Prometheus.
//
//   lattice_cells_received_total{group}        // bytes off the gossip wire
//   lattice_cells_observed_total{group, outcome=accepted|rejected}
//   lattice_attestations_observed_total{group, outcome=certified|pending|already|rejected}
//   lattice_cycles_committed_total{group}
//   lattice_commit_decisions_total{group, outcome=committed|anchor_missing|below_follower_quorum}
//
// Gauges sampled in the commit-poller loop (so they refresh ~every cycle):
//   lattice_pending_cells{group}
//   lattice_certified_cells{group}
//   lattice_last_committed_cycle{group}
//
// `metrics` is a no-op crate when the `metrics` feature is off — emitting a
// counter is then a literal nothing-burger at runtime, so the cfg gating is
// mostly for build-time dep hygiene (the `metrics` crate is optional on
// `savitri-lightnode`).

/// Gossip topic prefix for raw cell broadcasts.
pub const LATTICE_CELL_TOPIC_PREFIX: &str = "/savitri/group/";
pub const LATTICE_CELL_TOPIC_SUFFIX: &str = "/lattice/cell/1";

/// Gossip topic prefix for cell-attestation broadcasts.
pub const LATTICE_ATTESTATION_TOPIC_PREFIX: &str = "/savitri/group/";
pub const LATTICE_ATTESTATION_TOPIC_SUFFIX: &str = "/lattice/attestation/1";

/// P2.6-B.4a: gossip topic prefix for batch envelope broadcasts.
/// When the Lattice publisher peeks a non-empty TX batch from the
/// mempool, it ships (a) the cell on the cell topic and (b) a
/// BatchEnvelope on this topic so co-grouped peers can populate
/// their batch_cache and the chain hook (P2.6-C) can reconstruct
/// the cycle's TX content without re-draining the mempool.
pub const LATTICE_BATCH_TOPIC_PREFIX: &str = "/savitri/group/";
pub const LATTICE_BATCH_TOPIC_SUFFIX: &str = "/lattice/batch/1";

/// Env var controlling whether the Lattice runtime is authoritative
/// over chain commits. Default behaviour (unset or `v1`) is
/// observation-only.
pub const CONSENSUS_VERSION_ENV: &str = "SAVITRI_CONSENSUS_VERSION";

/// Build the cell gossip topic for a given group.
#[inline]
pub fn cell_topic_for_group(group_id: &str) -> IdentTopic {
    IdentTopic::new(format!(
        "{}{}{}",
        LATTICE_CELL_TOPIC_PREFIX, group_id, LATTICE_CELL_TOPIC_SUFFIX
    ))
}

/// Build the attestation gossip topic for a given group.
#[inline]
pub fn attestation_topic_for_group(group_id: &str) -> IdentTopic {
    IdentTopic::new(format!(
        "{}{}{}",
        LATTICE_ATTESTATION_TOPIC_PREFIX, group_id, LATTICE_ATTESTATION_TOPIC_SUFFIX
    ))
}

/// P2.6-B.4a: build the batch envelope gossip topic for a group.
#[inline]
pub fn batch_topic_for_group(group_id: &str) -> IdentTopic {
    IdentTopic::new(format!(
        "{}{}{}",
        LATTICE_BATCH_TOPIC_PREFIX, group_id, LATTICE_BATCH_TOPIC_SUFFIX
    ))
}

/// Convenience: is the runtime authoritative on chain commits?
#[inline]
pub fn is_authoritative_mode() -> bool {
    std::env::var(CONSENSUS_VERSION_ENV)
        .map(|v| v.eq_ignore_ascii_case("v2"))
        .unwrap_or(false)
}

/// P2.6-C.2 Phase A: env var that turns ON the shadow chain consumer
/// (audit-only mode). When set to a truthy value AND a `chain_sink`
/// is wired, the commit_poller_loop forwards every CommittedLatticeBlock
/// to the consumer task. The consumer writes a JSONL audit file but
/// does NOT touch the V0.1 chain — safe to enable in production-like
/// observation runs.
///
/// Independent from CONSENSUS_VERSION_ENV: shadow audit can be ON
/// while authoritative mode is OFF (the intended combo for offline
/// validation of the chain hook before the flag-day cutover).
pub const SHADOW_AUDIT_ENV: &str = "SAVITRI_LATTICE_SHADOW_AUDIT";

/// P2.6-C.2 Phase A: env var that lets operators override the audit
/// file path. Default `/host-state/lattice-blocks.jsonl` matches the
/// container bind-mount layout (visible on the host as
/// `/opt/savitri/lattice-blocks.jsonl`).
pub const SHADOW_AUDIT_FILE_ENV: &str = "SAVITRI_LATTICE_AUDIT_FILE";

#[inline]
pub fn is_shadow_audit_mode() -> bool {
    std::env::var(SHADOW_AUDIT_ENV)
        .map(|v| matches!(v.trim(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}

#[inline]
pub fn shadow_audit_file_path() -> std::path::PathBuf {
    std::env::var(SHADOW_AUDIT_FILE_ENV)
        .ok()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("/host-state/lattice-blocks.jsonl"))
}

/// P2.6-B.4a: wire-format envelope for the batch broadcast topic.
///
/// Published alongside each LatticeCell that contains a non-empty
/// peeked TX batch. Receivers verify the batch_root matches the
/// signature_hashes carried in the envelope (replay-resistant
/// content commitment), then insert the raw signed bytes into
/// batch_cache for the chain hook in P2.6-C.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BatchEnvelope {
    /// Hash identifier of the cell whose batch_root commits to this
    /// payload. Used as the batch_cache key on the receiver side.
    pub cell_id: CellId,
    /// The cell's `batch_root` field — receiver recomputes from
    /// sig_hashes and drops the envelope on mismatch.
    pub batch_root: BatchRoot,
    /// Lattice round of the parent cell (for batch_root V2
    /// reconstruction).
    pub round: LatticeRound,
    /// Group identifier of the parent cell (for batch_root V2
    /// reconstruction).
    pub group_id: String,
    /// Peer_id of the cell author (for batch_root V2
    /// reconstruction).
    pub author: String,
    /// Per-TX signature_hash values, parallel to `raw_bytes`. Used
    /// for batch_root verification on the receiver.
    pub sig_hashes: Vec<[u8; 32]>,
    /// Raw signed TX bytes, parallel to `sig_hashes`. Stored in
    /// batch_cache verbatim once verification passes.
    pub raw_bytes: Vec<Vec<u8>>,
}

/// P2.6-C.1: in-process message emitted by the commit_poller_loop
/// whenever a cycle commits. Carries enough state for the chain hook
/// (or any other consumer) to reconstruct the committed batch in
/// canonical order — without going back to the aggregator.
///
/// Lives in-process only (no wire format) — the consumer is the
/// chain state machine, attached via `LatticeRuntimeState::chain_sink`.
#[derive(Clone, Debug)]
pub struct CommittedLatticeBlock {
    pub cycle_index: CycleIndex,
    pub group_id: String,
    pub pivot: String,
    /// Committed cells in canonical order (round-major asc, then
    /// author lex asc) — mirrors `Cycle::committed_cells`.
    pub committed_cells: Vec<CellId>,
    /// Number of cells whose batch was found in the local cache.
    pub batch_hits: usize,
    /// Number of cells whose batch was NOT in the local cache
    /// (e.g. arrived from a peer before B.4a was deployed, or fell
    /// out of the retention window before commit). Such cells are
    /// dropped from `merged_tx_bytes`.
    pub batch_misses: usize,
    /// Total bytes count (sum of all raw_bytes len) — convenient
    /// metric for DIAG counters.
    pub total_tx_bytes_len: usize,
    /// Flattened raw signed TX bytes in canonical commit order. The
    /// chain consumer can deserialize each `Vec<u8>` as a SignedTx
    /// and append to the next block.
    pub merged_tx_bytes: Vec<Vec<u8>>,
}

/// Wire-format envelope for an attestation gossipsub message. We need
/// to carry both the `cell_id` (so the receiver can look up the cell)
/// and the `CellAttestation` itself (with signer pubkey + signature).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AttestationEnvelope {
    /// 32-byte cell id (blake3 over `cell.signable_bytes()`).
    pub cell_id: [u8; 32],
    /// The attestation itself.
    pub attestation: CellAttestation,
}

/// State + configuration for the Lattice runtime, shared between the
/// publisher task, the commit poller, and the receive handlers.
pub struct LatticeRuntimeState {
    /// Stable peer_id of this lightnode.
    pub local_peer_id: String,
    /// Signing key used to sign our own cells and attestations.
    pub signing_key: Arc<Keypair>,
    /// The shared aggregator. Writers: receive handlers, publisher,
    /// commit poller. Readers: same.
    pub aggregator: Arc<RwLock<LatticeAggregator>>,
    /// Channel into the libp2p task for publishing on gossipsub.
    pub network_publish_tx: mpsc::Sender<(IdentTopic, Vec<u8>)>,
    /// P2.6-B.2: optional mempool handle for peeking TX into batch_root.
    /// When None (or tx_per_cell_cap=0), the publisher falls back to
    /// the dynamic placeholder batch_root (P2.6-B.1).
    pub mempool_handle:
        Option<std::sync::Arc<savitri_mempool::mempool::integration::MempoolPipeline>>,
    /// P2.6-B.2: cached cap for the publisher (mirrors config to avoid
    /// crossing the runtime->state boundary inside the loop).
    pub tx_per_cell_cap: usize,
    /// P2.6-B.3 + P2.6-B.4a: local cache of (cell_id -> raw signed
    /// TX bytes) for cells that are either authored by THIS lightnode
    /// OR received via the batch broadcast topic (P2.6-B.4a). Used by
    /// the chain hook (P2.6-C) to recover the TX content of a
    /// committed cycle. Bounded by retention window — entries are
    /// evicted on commit_poller tick after the matching certified
    /// cell ages out of the aggregator.
    ///
    /// Stored as raw signed bytes (Vec<u8>) rather than MempoolTx
    /// because the chain hook re-deserializes them as SignedTx
    /// before chain submission; the bytes are also the canonical
    /// form on the wire (BatchEnvelope.raw_bytes).
    pub batch_cache: Arc<RwLock<std::collections::BTreeMap<CellId, Vec<Vec<u8>>>>>,
    /// P2.6-C.1: optional chain-side sink. When Some and
    /// `is_authoritative_mode() == true`, the commit poller forwards
    /// every committed cycle (CommittedLatticeBlock) here for chain
    /// state-machine consumption. None disables the forward (the
    /// default in observation-only mode).
    pub chain_sink: Option<mpsc::Sender<CommittedLatticeBlock>>,
}

/// Configuration knobs for the runtime tasks. Defaults are tuned for
/// the testnet operating profile.
#[derive(Clone, Debug)]
pub struct LatticeRuntimeConfig {
    /// How often the publisher emits a cell. Should match
    /// `LATTICE_ROUND_DURATION_SECS` (default 1s) so each tick is one
    /// lattice round.
    pub publish_interval_secs: u64,
    /// How often the commit poller attempts to commit the next cycle.
    /// Defaults to `2 * publish_interval_secs` (one cycle = two rounds).
    pub commit_poll_interval_secs: u64,
    /// P2.6-B.2: max number of mempool TX peeked per published cell.
    /// When the mempool handle is wired, the publisher peeks up to this
    /// many pending TX (non-destructive) and folds their signature
    /// hashes into the cell batch_root. Override via env
    /// SAVITRI_LATTICE_TX_PER_CELL. Default 200 keeps cluster-wide
    /// drain rate well below mempool cap. Set to 0 to disable mempool
    /// integration (fall back to the dynamic placeholder batch_root).
    pub tx_per_cell_cap: usize,
    /// Aggregator configuration (group size, retention, etc.).
    pub aggregator: AggregatorConfig,
}

impl Default for LatticeRuntimeConfig {
    fn default() -> Self {
        let tx_per_cell_cap = std::env::var("SAVITRI_LATTICE_TX_PER_CELL")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(200);
        Self {
            publish_interval_secs: LATTICE_ROUND_DURATION_SECS,
            commit_poll_interval_secs: LATTICE_ROUND_DURATION_SECS,
            tx_per_cell_cap,
            aggregator: AggregatorConfig::default(),
        }
    }
}

/// The runtime handle. Construct via [`LatticeRuntime::new`], then call
/// [`LatticeRuntime::spawn_tasks`] to start the publisher + commit poller.
///
/// The receive handlers (`process_cell_message`,
/// `process_attestation_message`) are method calls — the caller (the
/// libp2p network task) invokes them from its gossipsub dispatch
/// branch.
pub struct LatticeRuntime {
    state: Arc<LatticeRuntimeState>,
    config: LatticeRuntimeConfig,
}

impl LatticeRuntime {
    /// Construct a new runtime. No tasks spawned yet.
    /// P2.6-C.1: replace the chain_sink on the state Arc. Used by
    /// the network task once the chain side of the integration is
    /// ready to consume committed cycles. No-op if the inner Arc
    /// has more than one strong reference (i.e. spawn_tasks has
    /// already cloned the state). In that case wire the sink BEFORE
    /// calling spawn_tasks.
    pub fn set_chain_sink(&mut self, sink: mpsc::Sender<CommittedLatticeBlock>) -> bool {
        match Arc::get_mut(&mut self.state) {
            Some(s) => {
                s.chain_sink = Some(sink);
                true
            }
            None => false,
        }
    }

    pub fn new(
        local_peer_id: String,
        signing_key: Arc<Keypair>,
        network_publish_tx: mpsc::Sender<(IdentTopic, Vec<u8>)>,
        config: LatticeRuntimeConfig,
        mempool_handle: Option<
            std::sync::Arc<savitri_mempool::mempool::integration::MempoolPipeline>,
        >,
    ) -> Self {
        let aggregator = Arc::new(RwLock::new(LatticeAggregator::new(
            config.aggregator.clone(),
        )));
        let tx_per_cell_cap = config.tx_per_cell_cap;
        let batch_cache = Arc::new(RwLock::new(std::collections::BTreeMap::new()));
        let state = Arc::new(LatticeRuntimeState {
            local_peer_id,
            signing_key,
            aggregator,
            network_publish_tx,
            mempool_handle,
            tx_per_cell_cap,
            batch_cache,
            // P2.6-C.1: chain sink remains None until the chain
            // integration wires it in P2.6-C.2.
            chain_sink: None,
        });
        Self { state, config }
    }

    /// Clone the inner state handle. Used by the network task to share
    /// the aggregator with the receive-side dispatch branch.
    pub fn state(&self) -> Arc<LatticeRuntimeState> {
        Arc::clone(&self.state)
    }

    /// Spawn the periodic publisher + commit poller. The provided
    /// `group_provider` is called every tick to fetch the current
    /// group_id and the ranked PoU table (peer_id, pou_score)
    /// descending — exactly the same input shape used by the Phase 1
    /// election. Returning `None` skips the tick.
    pub fn spawn_tasks<F>(&self, group_provider: F)
    where
        F: Fn() -> Option<(String, Vec<(String, u32)>)> + Send + Sync + 'static + Clone,
    {
        let mode = if is_authoritative_mode() {
            "authoritative"
        } else {
            "observation-only"
        };
        info!(
            target: "savitri::lattice",
            mode = mode,
            publish_interval_secs = self.config.publish_interval_secs,
            commit_poll_interval_secs = self.config.commit_poll_interval_secs,
            "LatticeRuntime starting tasks"
        );

        // Publisher task: builds + signs + publishes a cell every tick.
        {
            let state = Arc::clone(&self.state);
            let interval = self.config.publish_interval_secs;
            let provider = group_provider.clone();
            tokio::spawn(async move {
                publisher_loop(state, provider, interval).await;
            });
        }

        // Commit poller task: attempts the next cycle commit every tick.
        {
            let state = Arc::clone(&self.state);
            let interval = self.config.commit_poll_interval_secs;
            let provider = group_provider.clone();
            tokio::spawn(async move {
                commit_poller_loop(state, provider, interval).await;
            });
        }
    }

    /// Receive handler: deserialize a raw cell from gossip, verify the
    /// author signature, feed it to the aggregator. If we are a group
    /// member of the cell's group, publish our own attestation to
    /// drive quorum.
    ///
    /// `local_group_id` is the caller's current group identity (or
    /// `None` if not yet in a group). The handler short-circuits if
    /// the cell's group does not match ours.
    pub async fn process_cell_message(
        state: &LatticeRuntimeState,
        local_group_id: Option<&str>,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let cell: LatticeCell = match serde_json::from_slice(data) {
            Ok(c) => c,
            Err(e) => {
                debug!(error = %e, "Lattice: failed to deserialize cell, ignoring");
                return Ok(());
            }
        };
        let group_label = cell.group_id.clone();
        #[cfg(feature = "metrics")]
        counter!("lattice_cells_received_total", "group" => group_label.clone()).increment(1);
        // Group filter.
        match local_group_id {
            Some(gid) if gid == cell.group_id => {}
            Some(_) => {
                debug!(
                    cell_group = %cell.group_id,
                    "Lattice: cell for foreign group, ignoring"
                );
                return Ok(());
            }
            None => return Ok(()),
        }

        let cell_id = {
            let mut agg = state.aggregator.write().await;
            match agg.observe_cell(cell.clone()) {
                Ok(id) => {
                    #[cfg(feature = "metrics")]
                    counter!(
                        "lattice_cells_observed_total",
                        "group" => group_label.clone(),
                        "outcome" => "accepted"
                    )
                    .increment(1);
                    id
                }
                Err(e) => {
                    #[cfg(feature = "metrics")]
                    counter!(
                        "lattice_cells_observed_total",
                        "group" => group_label.clone(),
                        "outcome" => "rejected"
                    )
                    .increment(1);
                    warn!(error = %e, "Lattice: observe_cell rejected, ignoring");
                    return Ok(());
                }
            }
        };

        // Publish our own attestation on the cell (we are a group
        // member by definition since the group filter above passed).
        // Skip if the cell author is us (no point attesting our own
        // cell twice — the publisher already signs as author).
        if cell.author == state.local_peer_id {
            return Ok(());
        }
        publish_attestation(state, cell_id, &cell).await;
        Ok(())
    }

    /// Receive handler: deserialize an attestation envelope, feed it
    /// to the aggregator. If quorum is reached, log the outcome
    /// (DIAG[lattice-cert]).
    pub async fn process_attestation_message(
        state: &LatticeRuntimeState,
        local_group_id: Option<&str>,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let env: AttestationEnvelope = match serde_json::from_slice(data) {
            Ok(e) => e,
            Err(e) => {
                debug!(error = %e, "Lattice: failed to deserialize attestation, ignoring");
                return Ok(());
            }
        };
        // We can't easily filter by group here without doing the
        // aggregator lookup first (the envelope itself doesn't carry
        // group_id). The aggregator will return `UnknownCell` for
        // attestations whose cell we never observed; if the cell was
        // for a foreign group, our cell handler filtered it out, so
        // the aggregator never saw it, so `observe_attestation`
        // returns UnknownCell — which is exactly what we want.
        let _ = local_group_id;

        let outcome = {
            let mut agg = state.aggregator.write().await;
            agg.observe_attestation(env.cell_id, env.attestation)
        };
        match outcome {
            AttestationOutcome::Certified(cert) => {
                #[cfg(feature = "metrics")]
                counter!(
                    "lattice_attestations_observed_total",
                    "group" => cert.cell.group_id.clone(),
                    "outcome" => "certified"
                )
                .increment(1);
                info!(
                    target: "savitri::lattice",
                    cell_round = cert.cell.round,
                    cell_author = %cert.cell.author,
                    cell_id = %hex::encode(cert.cell_id()),
                    attestations = cert.attestations.len(),
                    "DIAG[lattice-cert] cell certified"
                );
            }
            AttestationOutcome::Pending {
                signer_count,
                quorum,
            } => {
                #[cfg(feature = "metrics")]
                counter!(
                    "lattice_attestations_observed_total",
                    "outcome" => "pending"
                )
                .increment(1);
                debug!(
                    target: "savitri::lattice",
                    signer_count,
                    quorum,
                    "Lattice attestation: below quorum"
                );
            }
            AttestationOutcome::AlreadyCertified => {
                #[cfg(feature = "metrics")]
                counter!(
                    "lattice_attestations_observed_total",
                    "outcome" => "already_certified"
                )
                .increment(1);
                debug!(
                    target: "savitri::lattice",
                    "Lattice attestation: cell already certified, ignoring"
                );
            }
            AttestationOutcome::Rejected(err) => {
                #[cfg(feature = "metrics")]
                counter!(
                    "lattice_attestations_observed_total",
                    "outcome" => "rejected"
                )
                .increment(1);
                debug!(
                    target: "savitri::lattice",
                    error = %err,
                    "Lattice attestation: rejected"
                );
            }
        }
        Ok(())
    }

    /// P2.6-B.4a: receive handler for the batch-broadcast topic.
    ///
    /// Steps:
    ///   1. Deserialize the envelope (drop on parse error).
    ///   2. Drop if the envelope's group_id does not match this LN's
    ///      current group (cross-group leakage protection).
    ///   3. Recompute `compute_batch_root_v2` from `sig_hashes` and
    ///      the envelope's (round, group_id, author) tuple — drop on
    ///      mismatch.
    ///   4. Insert `raw_bytes` into batch_cache under `cell_id`.
    pub async fn process_batch_message(
        state: &LatticeRuntimeState,
        local_group_id: Option<&str>,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let envelope: BatchEnvelope = match serde_json::from_slice(data) {
            Ok(e) => e,
            Err(e) => {
                debug!(error = %e, "Lattice batch: failed to deserialize envelope, ignoring");
                return Ok(());
            }
        };

        if let Some(lg) = local_group_id {
            if lg != envelope.group_id {
                debug!(
                    target: "savitri::lattice",
                    local = %lg,
                    envelope_group = %envelope.group_id,
                    "Lattice batch: cross-group envelope dropped"
                );
                return Ok(());
            }
        }

        let expected = compute_batch_root_v2(
            envelope.round,
            &envelope.group_id,
            &envelope.author,
            &envelope.sig_hashes,
        );
        if expected != envelope.batch_root {
            warn!(
                target: "savitri::lattice",
                "Lattice batch: batch_root mismatch, dropping envelope"
            );
            return Ok(());
        }

        if envelope.sig_hashes.len() != envelope.raw_bytes.len() {
            warn!(
                target: "savitri::lattice",
                sig_count = envelope.sig_hashes.len(),
                byte_count = envelope.raw_bytes.len(),
                "Lattice batch: envelope length mismatch, dropping"
            );
            return Ok(());
        }

        {
            let mut cache = state.batch_cache.write().await;
            cache.insert(envelope.cell_id, envelope.raw_bytes);
        }
        debug!(
            target: "savitri::lattice",
            author = %envelope.author,
            sig_count = envelope.sig_hashes.len(),
            "Lattice batch: envelope accepted, cached"
        );
        Ok(())
    }
}

/// Inner loop: publisher.
async fn publisher_loop<F>(state: Arc<LatticeRuntimeState>, group_provider: F, interval_secs: u64)
where
    F: Fn() -> Option<(String, Vec<(String, u32)>)> + Send + Sync + 'static,
{
    let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick — let the aggregator + group
    // assignment settle.
    tick.tick().await;

    loop {
        tick.tick().await;
        let Some((group_id, _ranked_pou)) = group_provider() else {
            debug!(target: "savitri::lattice", "publisher: no current group, skipping");
            continue;
        };

        // Determine our author round = the current lattice round.
        // Phase 2 uses chain-independent timing: round = unix_secs /
        // LATTICE_ROUND_DURATION_SECS. All cluster members at any
        // given wall-clock tick share the same round index.
        let round = current_lattice_round();

        // P2.6-A.10: Parents window widened from strict round-1 to
        // round-K..=round-1 (K=3). This collects certified cells that
        // arrived late (attestation propagation jitter cross-VM), so
        // the pivot anchor at round 2k still appears as a parent in
        // follow cells at round 2k+1 even when 2k certification lands
        // a tick or two later. Cap stays at 16 cells total to keep
        // the wire small.
        const PARENTS_LOOKBACK_ROUNDS: u64 = 7;
        let parents: Vec<CellId> = if round == 0 {
            Vec::new()
        } else {
            let agg = state.aggregator.read().await;
            let lo = round.saturating_sub(PARENTS_LOOKBACK_ROUNDS);
            let hi = round.saturating_sub(1);
            let mut acc: Vec<CellId> = Vec::with_capacity(16);
            // Walk recent rounds in descending order so we prefer the
            // most-recent certified cells when the 16-cap saturates.
            for r in (lo..=hi).rev() {
                for cert in agg.certified_at_round(r) {
                    if acc.len() >= 16 { break; }
                    acc.push(cert.cell_id());
                }
                if acc.len() >= 16 { break; }
            }
            acc
        };

        // Build + sign the cell. batch_root is a placeholder until the
        // Lattice data-availability layer ships (Phase 2.6+).
        let author_pubkey = state.signing_key.verifying_key().to_bytes();
        // P2.6-B.2 + P2.6-B.3 + P2.6-B.4a: peek up to tx_per_cell_cap
        // from the mempool (non-destructive), derive a
        // content-addressing batch_root, and capture (sig_hashes,
        // raw_bytes) pairs for the BatchEnvelope broadcast.
        let cap = state.tx_per_cell_cap;
        let (batch_root, peeked_sigs, peeked_bytes): (BatchRoot, Vec<[u8; 32]>, Vec<Vec<u8>>) =
            if cap > 0 {
                if let Some(mempool) = state.mempool_handle.as_ref() {
                    let peeked = mempool.peek_pending_with_bytes(cap);
                    if peeked.is_empty() {
                        (
                            compute_dynamic_batch_root(round, &group_id, &state.local_peer_id),
                            Vec::new(),
                            Vec::new(),
                        )
                    } else {
                        let sigs: Vec<[u8; 32]> =
                            peeked.iter().map(|(tx, _)| tx.signature_hash).collect();
                        let bytes: Vec<Vec<u8>> =
                            peeked.into_iter().map(|(_, b)| b).collect();
                        let root = compute_batch_root_v2(round, &group_id, &state.local_peer_id, &sigs);
                        (root, sigs, bytes)
                    }
                } else {
                    (
                        compute_dynamic_batch_root(round, &group_id, &state.local_peer_id),
                        Vec::new(),
                        Vec::new(),
                    )
                }
            } else {
                (
                    compute_dynamic_batch_root(round, &group_id, &state.local_peer_id),
                    Vec::new(),
                    Vec::new(),
                )
            };
        let mut cell = LatticeCell::with_sorted_parents(
            round,
            group_id.clone(),
            state.local_peer_id.clone(),
            author_pubkey,
            parents,
            batch_root,
            [0u8; 64],
        );
        let payload = cell.signable_bytes();
        cell.author_signature = state.signing_key.sign(&payload).to_bytes();

        // P2.6-B.3: stash own peeked TX batch under the (future) cell_id.
        // We need cell_id which becomes available after observe_cell;
        // we'll insert into batch_cache once we have it (see below).
        // Push into our own aggregator so we have the cell available
        // for our own attestation (LN attests its own cell as
        // author).
        let cell_id = {
            let mut agg = state.aggregator.write().await;
            match agg.observe_cell(cell.clone()) {
                Ok(id) => id,
                Err(e) => {
                    warn!(target: "savitri::lattice", error = %e, "publisher: own observe_cell rejected");
                    continue;
                }
            }
        };

        // Phase 2.5 fix: emit our own attestation on our own cell
        // so the author contributes to the 2f+1 quorum. Without
        // this, with 2 LN/group, certified=0 forever (see DIAG
        // snapshot from the 2026-05-12 observation run).
        // P2.6-B.3 + P2.6-B.4a: stash the peeked raw signed bytes
        // keyed by cell_id (own-authored entry) AND broadcast a
        // BatchEnvelope on the batch topic so co-grouped peers can
        // populate their own caches. Both skipped on V1 fallback.
        if !peeked_bytes.is_empty() {
            {
                let mut cache = state.batch_cache.write().await;
                cache.insert(cell_id, peeked_bytes.clone());
            }
            let envelope = BatchEnvelope {
                cell_id,
                batch_root,
                round,
                group_id: group_id.clone(),
                author: state.local_peer_id.clone(),
                sig_hashes: peeked_sigs,
                raw_bytes: peeked_bytes,
            };
            match serde_json::to_vec(&envelope) {
                Ok(buf) => {
                    let topic = batch_topic_for_group(&group_id);
                    if state.network_publish_tx.send((topic, buf)).await.is_err() {
                        debug!(
                            target: "savitri::lattice",
                            "publisher: batch envelope channel closed, skipping"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        target: "savitri::lattice",
                        error = %e,
                        "publisher: BatchEnvelope serialize failed"
                    );
                }
            }
        }
        publish_attestation(&state, cell_id, &cell).await;

        // Serialize + publish.
        let encoded = match serde_json::to_vec(&cell) {
            Ok(v) => v,
            Err(e) => {
                warn!(target: "savitri::lattice", error = %e, "publisher: serialize failed");
                continue;
            }
        };
        let topic = cell_topic_for_group(&group_id);
        if state
            .network_publish_tx
            .send((topic, encoded))
            .await
            .is_err()
        {
            debug!(target: "savitri::lattice", "publisher: publish channel closed, exiting");
            return;
        }

        debug!(
            target: "savitri::lattice",
            round,
            cell_id = %hex::encode(cell_id),
            "Lattice cell published"
        );
    }
}

/// Inner loop: commit poller.
async fn commit_poller_loop<F>(
    state: Arc<LatticeRuntimeState>,
    group_provider: F,
    interval_secs: u64,
) where
    F: Fn() -> Option<(String, Vec<(String, u32)>)> + Send + Sync + 'static,
{
    let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await;
    // Track the last cycle we successfully committed to avoid
    // re-attempting the same cycle every tick.
    let mut last_committed_cycle: Option<CycleIndex> = None;

    loop {
        tick.tick().await;
        let Some((group_id, ranked_pou)) = group_provider() else {
            continue;
        };

        // Compute the current cycle from the wall-clock-derived
        // lattice round. cycle = round / 2.
        let current_round = current_lattice_round();
        let current_cycle = current_round / 2;
        // Attempt every uncommitted cycle from last_committed+1 up to
        // current_cycle - 1 (commit cycle k requires having round
        // 2k+1 cells, i.e. we are at round >= 2k+1).
        //
        // P2.6-A fix: when there is no last_committed_cycle yet (cold
        // start), do NOT scan from cycle 0 — the wall-clock-derived
        // cycle index is ~0.9 billion. Start from current_cycle minus a
        // small lookback window so the first probe lands inside the
        // aggregator retention window (64 rounds / 32 cycles).
        const COMMIT_LOOKBACK_CYCLES: u64 = 4;
        let max_attempt_cycle = current_cycle.saturating_sub(1);
        let start_cycle = last_committed_cycle
            .map(|c| c + 1)
            .unwrap_or_else(|| current_cycle.saturating_sub(COMMIT_LOOKBACK_CYCLES));

        for cycle in start_cycle..=max_attempt_cycle {
            let pivot =
                match savitri_consensus::lattice::pivot_for_cycle(&group_id, cycle, &ranked_pou) {
                    Some(p) => p,
                    None => break,
                };
            let agg = state.aggregator.read().await;
            let outcome = LineageCommit::try_commit(&agg, cycle, &pivot, &group_id);
            drop(agg);
            match outcome {
                CommitDecision::Committed(cy) => {
                    #[cfg(feature = "metrics")]
                    {
                        counter!(
                            "lattice_cycles_committed_total",
                            "group" => group_id.clone()
                        )
                        .increment(1);
                        counter!(
                            "lattice_commit_decisions_total",
                            "group" => group_id.clone(),
                            "outcome" => "committed"
                        )
                        .increment(1);
                    }
                    info!(
                        target: "savitri::lattice",
                        cycle = cy.index,
                        pivot = %cy.pivot,
                        committed_cells = cy.committed_cells.len(),
                        "DIAG[lattice-commit] cycle committed"
                    );
                    last_committed_cycle = Some(cycle);

                    // P2.6-C.1: build a CommittedLatticeBlock by walking
                    // cy.committed_cells (canonical order), looking up
                    // each cell's batch_cache entry, and concatenating
                    // raw bytes. Track hits/misses for DIAG visibility.
                    let cache_snapshot = state.batch_cache.read().await;
                    let mut hits: usize = 0;
                    let mut misses: usize = 0;
                    let mut merged: Vec<Vec<u8>> = Vec::new();
                    let mut total_bytes: usize = 0;
                    for cell_id in &cy.committed_cells {
                        match cache_snapshot.get(cell_id) {
                            Some(bytes_vec) => {
                                hits += 1;
                                for b in bytes_vec {
                                    total_bytes += b.len();
                                    merged.push(b.clone());
                                }
                            }
                            None => {
                                misses += 1;
                            }
                        }
                    }
                    drop(cache_snapshot);

                    info!(
                        target: "savitri::lattice",
                        cycle = cy.index,
                        cells = cy.committed_cells.len(),
                        batch_hits = hits,
                        batch_misses = misses,
                        tx_count = merged.len(),
                        bytes = total_bytes,
                        "DIAG[lattice-block] chain-block reconstructed"
                    );

                    #[cfg(feature = "metrics")]
                    {
                        counter!(
                            "lattice_block_batch_hits_total",
                            "group" => group_id.clone()
                        )
                        .increment(hits as u64);
                        counter!(
                            "lattice_block_batch_misses_total",
                            "group" => group_id.clone()
                        )
                        .increment(misses as u64);
                        gauge!(
                            "lattice_block_tx_count",
                            "group" => group_id.clone()
                        )
                        .set(merged.len() as f64);
                    }

                    // P2.6-C.1 + P2.6-C.2 Phase A: forward to chain sink
                    // when either authoritative mode is on (v2) OR the
                    // shadow audit mode is on. Best-effort send — if
                    // the chain consumer dropped its receiver we log
                    // and continue (it can catch up later via
                    // persistence in P2.6-D).
                    if is_authoritative_mode() || is_shadow_audit_mode() {
                        if let Some(sink) = state.chain_sink.as_ref() {
                            let block = CommittedLatticeBlock {
                                cycle_index: cy.index,
                                group_id: cy.group_id.clone(),
                                pivot: cy.pivot.clone(),
                                committed_cells: cy.committed_cells.clone(),
                                batch_hits: hits,
                                batch_misses: misses,
                                total_tx_bytes_len: total_bytes,
                                merged_tx_bytes: merged,
                            };
                            if sink.try_send(block).is_err() {
                                warn!(
                                    target: "savitri::lattice",
                                    cycle = cy.index,
                                    "phase2-authoritative: chain_sink send failed (full or closed)"
                                );
                            }
                        } else {
                            debug!(
                                target: "savitri::lattice",
                                "phase2-authoritative: no chain_sink wired, dropping block"
                            );
                        }
                    }
                }
                CommitDecision::AnchorNotCertified => {
                    #[cfg(feature = "metrics")]
                    counter!(
                        "lattice_commit_decisions_total",
                        "group" => group_id.clone(),
                        "outcome" => "anchor_missing"
                    )
                    .increment(1);
                    info!(
                        target: "savitri::lattice",
                        cycle,
                        anchor_round = cycle * 2,
                        pivot = %pivot,
                        "DIAG[lattice-commit-decision] AnchorNotCertified"
                    );
                    // P2.6-A skip-ahead: with wall-clock cycles and
                    // jittered attestation timing, individual cycles
                    // may fail while later ones succeed. Continue
                    // probing instead of breaking. walk_causal_history
                    // on a later anchor will still include the parent
                    // history transitively.
                    continue;
                }
                CommitDecision::BelowFollowerQuorum { follower_count, quorum } => {
                    #[cfg(feature = "metrics")]
                    counter!(
                        "lattice_commit_decisions_total",
                        "group" => group_id.clone(),
                        "outcome" => "below_follower_quorum"
                    )
                    .increment(1);
                    info!(
                        target: "savitri::lattice",
                        cycle,
                        anchor_round = cycle * 2,
                        follow_round = cycle * 2 + 1,
                        pivot = %pivot,
                        follower_count,
                        quorum,
                        "DIAG[lattice-commit-decision] BelowFollowerQuorum"
                    );
                    // P2.6-A skip-ahead (same rationale as
                    // AnchorNotCertified above).
                    continue;
                }
            }
        }

        // Phase 2.4.2: sample aggregator state + emit DIAG line every
        // commit-poller tick (i.e. every cycle = 2 lattice rounds, so
        // every ~10s with the default LATTICE_ROUND_DURATION_SECS).
        let (pending_count, certified_count) = {
            let agg = state.aggregator.read().await;
            (agg.pending_count(), agg.certified_count())
        };
        let batch_cache_size = state.batch_cache.read().await.len();
        #[cfg(feature = "metrics")]
        {
            gauge!("lattice_pending_cells", "group" => group_id.clone()).set(pending_count as f64);
            gauge!("lattice_certified_cells", "group" => group_id.clone())
                .set(certified_count as f64);
            gauge!("lattice_last_committed_cycle", "group" => group_id.clone())
                .set(last_committed_cycle.unwrap_or(0) as f64);
        }
        info!(
            target: "savitri::lattice",
            group = %group_id,
            pending = pending_count,
            certified = certified_count,
            last_cycle = last_committed_cycle.unwrap_or(0),
            batch_cache = batch_cache_size,
            "DIAG[lattice] aggregator state snapshot"
        );

        // GC old cells. Also prune batch_cache entries whose cell is no
        // longer present in the aggregator (P2.6-B.3 bounded retention).
        let evicted = {
            let mut agg = state.aggregator.write().await;
            agg.gc_old_cells()
        };
        if evicted > 0 {
            // Collect surviving certified cell IDs and intersect with cache.
            let agg = state.aggregator.read().await;
            let alive: std::collections::BTreeSet<CellId> = (0..=agg.high_water_round())
                .flat_map(|r| agg.certified_at_round(r).map(|c| c.cell_id()))
                .collect();
            drop(agg);
            let mut cache = state.batch_cache.write().await;
            cache.retain(|cid, _| alive.contains(cid));
        }
        if evicted > 0 {
            debug!(
                target: "savitri::lattice",
                evicted,
                "Lattice GC: cells evicted"
            );
        }
    }
}

/// Publish our attestation on a freshly-observed cell.
async fn publish_attestation(state: &LatticeRuntimeState, cell_id: CellId, cell: &LatticeCell) {
    let pk = state.signing_key.verifying_key().to_bytes();
    let sig = state.signing_key.sign(&cell.signable_bytes()).to_bytes();
    let att = CellAttestation {
        signer: state.local_peer_id.clone(),
        signer_pubkey: pk,
        signature: sig,
    };

    // Feed our own attestation into our local aggregator.
    let outcome = {
        let mut agg = state.aggregator.write().await;
        agg.observe_attestation(cell_id, att.clone())
    };
    if let AttestationOutcome::Certified(cert) = &outcome {
        info!(
            target: "savitri::lattice",
            cell_round = cert.cell.round,
            cell_author = %cert.cell.author,
            "DIAG[lattice-cert] cell certified (via local attestation)"
        );
    }

    // Broadcast the attestation to peers.
    let env = AttestationEnvelope {
        cell_id,
        attestation: att,
    };
    let encoded = match serde_json::to_vec(&env) {
        Ok(v) => v,
        Err(e) => {
            warn!(target: "savitri::lattice", error = %e, "attestation: serialize failed");
            return;
        }
    };
    let topic = attestation_topic_for_group(&cell.group_id);
    let _ = state.network_publish_tx.send((topic, encoded)).await;
}

/// Compute the current lattice round from the wall clock.
/// `unix_secs / LATTICE_ROUND_DURATION_SECS`. All cluster members at
/// any given wall-clock tick share the same index.
#[inline]
fn current_lattice_round() -> LatticeRound {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / LATTICE_ROUND_DURATION_SECS.max(1))
        .unwrap_or(0)
}

/// P2.6-B Phase 1: deterministic batch_root that varies per
/// (round, group_id, author). Domain-separated SHA-256 over canonical
/// encoding. Replaces the static placeholder which was identical for
/// every cell — that was a no-op for data availability and observers
/// could not distinguish two cells with different TX content.
///
/// Phase 2 will replace this with `hash(SignedTx[])` once the mempool
/// drain wiring lands (P2.6-B.2). Until then the batch_root is only a
/// per-cell unique tag, not a content commitment — sufficient for
/// observation-only mode but NOT for authoritative chain commits.
fn compute_dynamic_batch_root(
    round: LatticeRound,
    group_id: &str,
    peer_id: &str,
) -> BatchRoot {
    let mut buf = Vec::with_capacity(64 + group_id.len() + peer_id.len());
    buf.extend_from_slice(b"SAVITRI-LATTICE-BATCH-ROOT-V1 ");
    buf.extend_from_slice(&round.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(group_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(peer_id.as_bytes());
    savitri_core::crypto::hash::hash(&buf)
}


/// P2.6-B.2: real batch_root derived from the mempool TX preview.
/// Domain-separated SHA-256 over the cell author / round / cap /
/// concatenated signature_hash of each peeked TX.
///
/// We hash signature_hash (32 bytes per TX, replay-resistant) rather
/// than the raw serialized TX bytes because:
///   - signature_hash is content-addressable and deterministic;
///   - it is already computed at admission time (no extra serialization);
///   - the cell wire stays small while still committing to the batch
///     content (the side-channel storage layer in P2.6-B.3 will keep
///     the full TXs keyed by this digest).
fn compute_batch_root_v2(
    round: LatticeRound,
    group_id: &str,
    peer_id: &str,
    sig_hashes: &[[u8; 32]],
) -> BatchRoot {
    let mut buf =
        Vec::with_capacity(64 + group_id.len() + peer_id.len() + sig_hashes.len() * 32);
    buf.extend_from_slice(b"SAVITRI-LATTICE-BATCH-ROOT-V2\0");
    buf.extend_from_slice(&round.to_be_bytes());
    buf.push(0);
    buf.extend_from_slice(group_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(peer_id.as_bytes());
    buf.push(0);
    buf.extend_from_slice(&(sig_hashes.len() as u32).to_be_bytes());
    for h in sig_hashes {
        buf.extend_from_slice(h);
    }
    savitri_core::crypto::hash::hash(&buf)
}

/// P2.6-B.2 publisher-side convenience: derive batch_root from
/// MempoolTx values by extracting their signature_hash and forwarding
/// to compute_batch_root_v2.
fn compute_batch_root_from_txs(
    round: LatticeRound,
    group_id: &str,
    peer_id: &str,
    txs: &[savitri_mempool::mempool::types::MempoolTx],
) -> BatchRoot {
    let sigs: Vec<[u8; 32]> = txs.iter().map(|t| t.signature_hash).collect();
    compute_batch_root_v2(round, group_id, peer_id, &sigs)
}

/// P2.6-C.2 Phase A: shadow-mode consumer that drains
/// CommittedLatticeBlock values from the channel and writes one
/// JSONL line per block to the audit file. Designed for
/// observation-only mode — does NOT touch the V0.1 chain.
///
/// Each line carries the metrics needed to:
///   - confirm the chain hook fired (cycle, group, pivot);
///   - measure batch reconstruction success (hits/misses ratio);
///   - estimate per-cycle TX throughput (tx_count, total_bytes_len);
///   - correlate across LNs by cycle_index + group_id.
///
/// Best-effort writes: if the file cannot be opened or a line write
/// fails, the consumer logs the error and continues — the channel
/// is never blocked, so the commit_poller_loop is never throttled
/// by audit I/O.
pub async fn lattice_chain_consumer_loop(
    mut rx: tokio::sync::mpsc::Receiver<CommittedLatticeBlock>,
) {
    use tokio::io::AsyncWriteExt;

    let path = shadow_audit_file_path();
    info!(
        target: "savitri::lattice",
        path = %path.display(),
        "P2.6-C.2 shadow consumer: starting"
    );

    let mut total_blocks: u64 = 0;
    let mut total_misses: u64 = 0;
    let mut total_hits: u64 = 0;

    while let Some(block) = rx.recv().await {
        total_blocks += 1;
        total_hits += block.batch_hits as u64;
        total_misses += block.batch_misses as u64;

        // Build a single JSONL line. Use a hand-rolled JSON to avoid
        // an extra serde derive on CommittedLatticeBlock (which would
        // pull merged_tx_bytes serialization into the audit file — not
        // wanted; we only want metadata here).
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let line = format!(
            "{{\"ts_ms\":{},\"cycle\":{},\"group\":{:?},\"pivot\":{:?},\"cell_count\":{},\"batch_hits\":{},\"batch_misses\":{},\"tx_count\":{},\"bytes\":{}}}\n",
            ts_ms,
            block.cycle_index,
            block.group_id,
            block.pivot,
            block.committed_cells.len(),
            block.batch_hits,
            block.batch_misses,
            block.merged_tx_bytes.len(),
            block.total_tx_bytes_len,
        );

        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
        {
            Ok(mut f) => {
                if let Err(e) = f.write_all(line.as_bytes()).await {
                    warn!(
                        target: "savitri::lattice",
                        error = %e,
                        path = %path.display(),
                        "shadow consumer: write_all failed"
                    );
                }
            }
            Err(e) => {
                warn!(
                    target: "savitri::lattice",
                    error = %e,
                    path = %path.display(),
                    "shadow consumer: cannot open audit file"
                );
            }
        }

        if total_blocks % 50 == 0 {
            info!(
                target: "savitri::lattice",
                total_blocks,
                total_hits,
                total_misses,
                "DIAG[lattice-audit] shadow consumer rolling stats"
            );
        }
    }

    info!(
        target: "savitri::lattice",
        total_blocks,
        total_hits,
        total_misses,
        "P2.6-C.2 shadow consumer: channel closed, exiting"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_topic_format() {
        let t = cell_topic_for_group("group_42_0");
        assert_eq!(format!("{}", t), "/savitri/group/group_42_0/lattice/cell/1");
    }

    #[test]
    fn attestation_topic_format() {
        let t = attestation_topic_for_group("group_42_0");
        assert_eq!(
            format!("{}", t),
            "/savitri/group/group_42_0/lattice/attestation/1"
        );
    }

    #[test]
    fn current_lattice_round_returns_sane_value() {
        // Sanity: round should be enough to fit recent unix time / 1s.
        let r = current_lattice_round();
        assert!(r > 1_500_000_000); // post-2017
    }

    #[test]
    fn compute_dynamic_batch_root_is_deterministic() {
        let a = compute_dynamic_batch_root(42, "g", "p");
        let b = compute_dynamic_batch_root(42, "g", "p");
        assert_eq!(a, b);
    }

    #[test]
    fn is_authoritative_mode_default_false() {
        // Default: env var unset → observation-only.
        // Save and restore env so we don't bleed into other tests.
        let prev = std::env::var(CONSENSUS_VERSION_ENV).ok();
        std::env::remove_var(CONSENSUS_VERSION_ENV);
        assert!(!is_authoritative_mode());
        if let Some(v) = prev {
            std::env::set_var(CONSENSUS_VERSION_ENV, v);
        }
    }
}
