//! Tier 8 (DIAG consolidation) — TxRouter metrics.
//!
//! Extracts the ~4 `static AtomicU64` DIAG counters scattered in the legacy
//! `tx_router.rs` (ENTRY_CTR, ROUTE_CTR, LOCAL_CTR, FWD_CTR) into a typed ZST
//! struct. Strangler pattern: the legacy atomics + rate-limited tracing::warn
//! lines remain in the call sites until Grafana confirms full migration, then
//! sunset.
//!
//! All methods emit via `metrics::counter!/histogram!` directly — no local
//! AtomicU64 state.

/// ZST holder for the tx_router lane metrics. Pattern: static calls
/// `TxRoutingMetrics::inc_X()` — no instances ever exist.
pub struct TxRoutingMetrics;

impl TxRoutingMetrics {
    /// Counter incremented at the entry of `LightnodeTxRouter::route()`.
    /// Replaces the historical `static ENTRY_CTR: AtomicU64`.
    #[inline]
    pub fn inc_route_entry() {
        metrics::counter!("routing_route_entry_total").increment(1);
    }

    /// Counter incremented when the routing decision is `Local`.
    #[inline]
    pub fn inc_local_decision() {
        metrics::counter!("routing_decisions_total", "decision" => "local").increment(1);
    }

    /// Counter incremented when the routing decision is `Forwarded`
    /// (cross-group gossip publish).
    #[inline]
    pub fn inc_forward_decision() {
        metrics::counter!("routing_decisions_total", "decision" => "forward").increment(1);
    }

    /// Counter incremented when the decision is `FallbackLocal` (parse error,
    /// num_shards=0, shard_to_group busy, encode_gossip fail, swarm channel full).
    #[inline]
    pub fn inc_fallback_local(reason: &'static str) {
        metrics::counter!("routing_fallback_local_total", "reason" => reason).increment(1);
    }

    /// Counter incremented for every cross-group TX RECEIVED via gossip
    /// (handler `intra_group_tx_topic` in `network/mod.rs`).
    #[inline]
    pub fn inc_cross_group_rx() {
        metrics::counter!("routing_cross_group_rx_total").increment(1);
    }

    /// Histogram of cross-group TX payload size in bytes.
    #[inline]
    pub fn observe_payload_size(bytes: usize) {
        metrics::histogram!("routing_cross_group_rx_bytes").record(bytes as f64);
    }

    /// Counter incremented when route() decides Local because the LN has not
    /// yet received a GroupAnnouncement (`local_group` is empty). This is the
    /// bootstrap-gate footgun documented in architectural_debt.md "B.4 —
    /// GroupAnnouncement always arrives": until the first announcement RX
    /// every TX silently routes Local-and-hope, which becomes a problem when
    /// the chosen target group has no membership coverage from this LN.
    /// Distinct from `inc_local_decision` (which counts the legit
    /// target == local case).
    #[inline]
    pub fn inc_local_decision_no_group_yet() {
        metrics::counter!("routing_decisions_total", "decision" => "local_no_group_yet")
            .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_inc_methods_dont_panic() {
        // Only verifies compilation + no panic; no value assertion because the
        // metric handles live in a global recorder (would require metrics-util).
        TxRoutingMetrics::inc_route_entry();
        TxRoutingMetrics::inc_local_decision();
        TxRoutingMetrics::inc_forward_decision();
        TxRoutingMetrics::inc_fallback_local("test");
        TxRoutingMetrics::inc_cross_group_rx();
        TxRoutingMetrics::observe_payload_size(1024);
    }
}
