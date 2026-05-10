//! Tier 8 (DIAG counters consolidation) — observability struct sinks.
//!
//! Centralizes all the ZST `*Metrics` structs that wrap `metrics::counter!/
//! histogram!` calls into typed methods. Living in one file keeps the import
//! surface small (`use crate::observability::*`) and avoids the
//! sub-module-per-subsystem fragmentation that the architectural debt audit
//! explicitly warns against (Tier 8 line 113: "ogni fix lascia il suo
//! counter come 'tomb of past bugs'").
//!
//! Naming clash safety: do NOT use `ConsensusMetrics` (exists in
//! `savitri-consensus/src/types/consensus.rs:234`, public API) or
//! `RoutingMetrics` (exists in `savitri-p2p/src/groups/group_routing.rs:72`).
//! We use the suffix `ObsMetrics` for the lightnode-only Prometheus sink
//! flavour.
//!
//! sections 2.13-2.16.

/// ZST sink for consensus-layer counters (cert-match path, speculative
/// execution, commit scheduler).
///
/// Replaces the historical `static AtomicU64` blocks in
/// `savitri-lightnode/src/p2p/network/mod.rs:2951-3132` (DIAG[A]/[D]).
///
/// PROCESS-LOCAL leggibili da un periodic logger nel main.rs. I counter
/// atomici espongono lo stesso dato direttamente without infrastruttura
/// rate cluster-wide (memory: fix_A_C_D_results_2026-05-04).
use std::sync::atomic::{AtomicU64, Ordering};
pub static CERT_MATCH_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static CERT_MISS_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static CERT_LOCK_BUSY_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static CERT_VALID_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static CERT_INVALID_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static CERT_RECEIVED_LOCAL: AtomicU64 = AtomicU64::new(0);

pub struct ConsensusObsMetrics;

impl ConsensusObsMetrics {
    /// Cert MATCHED with a pending block (DIAG[D] match).
    #[inline]
    pub fn inc_cert_match() {
        metrics::counter!("consensus_cert_match_total", "outcome" => "match").increment(1);
        CERT_MATCH_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    /// Cert received but no matching pending block (DIAG[D] miss).
    #[inline]
    pub fn inc_cert_miss() {
        metrics::counter!("consensus_cert_match_total", "outcome" => "miss").increment(1);
        CERT_MISS_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    /// `try_lock` BUSY on `certificate_pending` (DIAG[A]).
    #[inline]
    pub fn inc_cert_lock_busy() {
        metrics::counter!("consensus_cert_lock_busy_total").increment(1);
        CERT_LOCK_BUSY_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_cert_valid() {
        metrics::counter!("consensus_cert_validated_total", "result" => "valid").increment(1);
        CERT_VALID_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_cert_invalid() {
        metrics::counter!("consensus_cert_validated_total", "result" => "invalid").increment(1);
        CERT_INVALID_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_cert_received() {
        metrics::counter!("consensus_cert_received_total").increment(1);
        CERT_RECEIVED_LOCAL.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of all cert local atomics — for periodic logger.
    pub fn cert_snapshot() -> (u64, u64, u64, u64, u64, u64) {
        (
            CERT_RECEIVED_LOCAL.load(Ordering::Relaxed),
            CERT_VALID_LOCAL.load(Ordering::Relaxed),
            CERT_INVALID_LOCAL.load(Ordering::Relaxed),
            CERT_MATCH_LOCAL.load(Ordering::Relaxed),
            CERT_MISS_LOCAL.load(Ordering::Relaxed),
            CERT_LOCK_BUSY_LOCAL.load(Ordering::Relaxed),
        )
    }

    /// Spawn task post-cert effectively executed.
    #[inline]
    pub fn inc_cert_spawn() {
        metrics::counter!("consensus_cert_spawn_total").increment(1);
    }

    /// Speculative execution outcome.
    #[inline]
    pub fn inc_speculative_exec(ok: bool) {
        let result = if ok { "ok" } else { "fail" };
        metrics::counter!("consensus_speculative_exec_total", "result" => result).increment(1);
    }

    /// `commit_scheduler.admit_block` returned successfully.
    #[inline]
    pub fn inc_commit_admit() {
        metrics::counter!("consensus_commit_scheduler_admit_total").increment(1);
    }

    /// `commit_scheduler.drain_ready` empty result.
    #[inline]
    pub fn inc_commit_drain_empty() {
        metrics::counter!("consensus_commit_scheduler_drain_total", "result" => "empty")
            .increment(1);
    }

    /// `commit_scheduler.drain_ready` returned `ready` blocks.
    #[inline]
    pub fn inc_commit_drain_hit(ready: usize) {
        metrics::counter!("consensus_commit_scheduler_drain_total", "result" => "hit").increment(1);
        metrics::histogram!("consensus_commit_scheduler_ready_size").record(ready as f64);
    }
}

// process-local atomics so a periodic logger can paint a complete picture
// of where TX go from RPC ingress to block production. No Prometheus
// scrape needed.
//
// Pipeline stages covered:
//   * RPC ingestion         -> RPC_TX_INGEST_TOTAL  (already via RpcConsumerMetrics)
//   * TxRouter decisions    -> ROUTER_DECISION_*
//   * TxRouter forward path -> ROUTER_FORWARD_*
//   * Gossip RX TX          -> GOSSIP_RX_TX_*
//   * Mempool admission     -> MEMPOOL_ADMIT_* (handled inside savitri-mempool)
//   * TTL purge             -> MEMPOOL_PURGE_TOTAL (handled inside savitri-mempool)
//   * Block prod throttle   -> BLOCK_PROD_THROTTLE_*
//
// All counters are incremented at strategic points and snapshotted by a
pub static ROUTER_ROUTES: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_DECIDED_LOCAL: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_DECIDED_LOCAL_NO_GROUP: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_DECIDED_FORWARD: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_DECIDED_RETRY: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_DECIDED_FALLBACK: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_FORWARD_DIRECT_OK: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_FORWARD_DIRECT_FAIL: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_FORWARD_GOSSIP_OK: AtomicU64 = AtomicU64::new(0);
pub static ROUTER_FORWARD_GOSSIP_FAIL: AtomicU64 = AtomicU64::new(0);

pub static GOSSIP_RX_TX_RECEIVED: AtomicU64 = AtomicU64::new(0);
pub static GOSSIP_RX_TX_DECODED: AtomicU64 = AtomicU64::new(0);
pub static GOSSIP_RX_TX_DECODE_FAIL: AtomicU64 = AtomicU64::new(0);
pub static GOSSIP_RX_TX_FORWARDED_TO_MEMPOOL: AtomicU64 = AtomicU64::new(0);

pub static BLOCK_PROD_PROPOSED: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_PROD_THROTTLED_DENSITY: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_PROD_HEARTBEAT_EMITTED: AtomicU64 = AtomicU64::new(0);
pub static BLOCK_PROD_PIPELINE_FULL: AtomicU64 = AtomicU64::new(0);

pub struct PipelineObsMetrics;

impl PipelineObsMetrics {
    #[inline] pub fn inc_router_route() { ROUTER_ROUTES.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_local() { ROUTER_DECIDED_LOCAL.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_local_no_group() { ROUTER_DECIDED_LOCAL_NO_GROUP.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_forward() { ROUTER_DECIDED_FORWARD.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_retry() { ROUTER_DECIDED_RETRY.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_fallback() { ROUTER_DECIDED_FALLBACK.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_forward_direct_ok() { ROUTER_FORWARD_DIRECT_OK.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_forward_direct_fail() { ROUTER_FORWARD_DIRECT_FAIL.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_forward_gossip_ok() { ROUTER_FORWARD_GOSSIP_OK.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_router_forward_gossip_fail() { ROUTER_FORWARD_GOSSIP_FAIL.fetch_add(1, Ordering::Relaxed); }

    #[inline] pub fn inc_gossip_rx_received() { GOSSIP_RX_TX_RECEIVED.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_gossip_rx_decoded() { GOSSIP_RX_TX_DECODED.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_gossip_rx_decode_fail() { GOSSIP_RX_TX_DECODE_FAIL.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn add_gossip_rx_forwarded(n: u64) { GOSSIP_RX_TX_FORWARDED_TO_MEMPOOL.fetch_add(n, Ordering::Relaxed); }

    #[inline] pub fn inc_block_proposed() { BLOCK_PROD_PROPOSED.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_block_throttled_density() { BLOCK_PROD_THROTTLED_DENSITY.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_block_heartbeat() { BLOCK_PROD_HEARTBEAT_EMITTED.fetch_add(1, Ordering::Relaxed); }
    #[inline] pub fn inc_block_pipeline_full() { BLOCK_PROD_PIPELINE_FULL.fetch_add(1, Ordering::Relaxed); }

    /// Snapshot the entire pipeline. Order: router (route, local, local_no_group,
    /// forward, retry, fallback, direct_ok, direct_fail, gossip_ok, gossip_fail),
    /// gossip_rx (received, decoded, decode_fail, forwarded_to_mempool),
    /// block_prod (proposed, throttled_density, heartbeat, pipeline_full).
    pub fn snapshot() -> [u64; 18] {
        [
            ROUTER_ROUTES.load(Ordering::Relaxed),
            ROUTER_DECIDED_LOCAL.load(Ordering::Relaxed),
            ROUTER_DECIDED_LOCAL_NO_GROUP.load(Ordering::Relaxed),
            ROUTER_DECIDED_FORWARD.load(Ordering::Relaxed),
            ROUTER_DECIDED_RETRY.load(Ordering::Relaxed),
            ROUTER_DECIDED_FALLBACK.load(Ordering::Relaxed),
            ROUTER_FORWARD_DIRECT_OK.load(Ordering::Relaxed),
            ROUTER_FORWARD_DIRECT_FAIL.load(Ordering::Relaxed),
            ROUTER_FORWARD_GOSSIP_OK.load(Ordering::Relaxed),
            ROUTER_FORWARD_GOSSIP_FAIL.load(Ordering::Relaxed),
            GOSSIP_RX_TX_RECEIVED.load(Ordering::Relaxed),
            GOSSIP_RX_TX_DECODED.load(Ordering::Relaxed),
            GOSSIP_RX_TX_DECODE_FAIL.load(Ordering::Relaxed),
            GOSSIP_RX_TX_FORWARDED_TO_MEMPOOL.load(Ordering::Relaxed),
            BLOCK_PROD_PROPOSED.load(Ordering::Relaxed),
            BLOCK_PROD_THROTTLED_DENSITY.load(Ordering::Relaxed),
            BLOCK_PROD_HEARTBEAT_EMITTED.load(Ordering::Relaxed),
            BLOCK_PROD_PIPELINE_FULL.load(Ordering::Relaxed),
        ]
    }
}

/// ZST sink for the RPC TX consumer task counters.
///
/// Replaces `static REJECT_CTR: AtomicU64` in `savitri-rpc/src/lib.rs:380`.
/// Lives in lightnode rather than savitri-rpc because savitri-rpc does not
/// (yet) depend on the `metrics` crate; the lightnode binary that links
/// both can call this from its consumer-task closure.
pub struct RpcConsumerMetrics;

impl RpcConsumerMetrics {
    #[inline]
    pub fn inc_consumer_rejection(reason: &'static str) {
        metrics::counter!("rpc_tx_consumer_rejected_total", "reason" => reason).increment(1);
    }

    #[inline]
    pub fn inc_consumer_accepted() {
        metrics::counter!("rpc_tx_consumer_accepted_total").increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_consensus_obs_methods_dont_panic() {
        ConsensusObsMetrics::inc_cert_match();
        ConsensusObsMetrics::inc_cert_miss();
        ConsensusObsMetrics::inc_cert_lock_busy();
        ConsensusObsMetrics::inc_cert_spawn();
        ConsensusObsMetrics::inc_speculative_exec(true);
        ConsensusObsMetrics::inc_speculative_exec(false);
        ConsensusObsMetrics::inc_commit_admit();
        ConsensusObsMetrics::inc_commit_drain_empty();
        ConsensusObsMetrics::inc_commit_drain_hit(42);
    }

    #[test]
    fn smoke_rpc_consumer_methods_dont_panic() {
        RpcConsumerMetrics::inc_consumer_rejection("test_reason");
        RpcConsumerMetrics::inc_consumer_accepted();
    }
}
