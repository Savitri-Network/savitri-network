//! Shard-aware TX dispatcher (Tier 4 Fase 2).
//!
//! Consulted by the RPC handler before local admit. Decodes the TX to extract
//! the sender, looks up the target group for that shard, and:
//!   - returns `Local` if target == local_group -> handler admits into the
//!     local mempool, then the existing tx_broadcast does intra-group fan-out.
//!   - publishes raw bytes on the target group's intra-group topic and returns
//!     `Forwarded { tx_hash }` -> handler skips local admit; members of the
//!     target group receive via their intra_group_tx_topic subscription
//!     (see `network/mod.rs` handler).
//!
//! Under `SAVITRI_FORCE_SINGLE_GROUP=1` the shard->group map assigns every
//! shard to `group_singleton_0`, so `route()` returns `Local` for every TX and
//! the path is zero-overhead vs pre-P1.
//!
//! Step 1 of 5:
//!   - `metrics`: ZST `TxRoutingMetrics` with typed Prometheus counters.
//!   - `resolution`: pure `extract_sender_and_hash`, `shard_for_sender` helpers
//!     (testable in isolation, parity with `ShardFilter::is_local`).
//!   - `dispatch` (future): gossipsub publish + future peer direct-send.
//!   - `peer_lookup` (future): ProposerCache + Kademlia DHT fallback.
//!
//! Strangler pattern: legacy DIAG `static AtomicU64` blocks + rate-limited log
//! lines remain alongside the new `TxRoutingMetrics::*` calls until Grafana
//! confirms full migration, then sunset in a follow-up PR.

pub mod dispatch;
pub mod metrics;
pub mod peer_lookup;
pub mod resolution;

use std::collections::HashMap;
use std::sync::Arc;

use libp2p::gossipsub::IdentTopic;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, trace, warn};

use crate::p2p::swarm_commands::SwarmCommand;
use savitri_rpc::{TxRouteDecision, TxRouter};

use self::dispatch::{DispatchOutcome, Dispatcher};
use self::metrics::TxRoutingMetrics;
use self::peer_lookup::{ProposerCache, ProposerSource};
use self::resolution::{extract_sender_and_hash, shard_for_sender};

/// Snapshot of the shard->group map. Built and maintained by the `group-announce`
/// handler in `network/mod.rs`. We use `u32` as the shard_id to match the rest
/// of the codebase (`sharding/sharding.rs ShardId::new(u32)`).
pub type ShardGroupMap = Arc<RwLock<HashMap<u32, String>>>;

/// Accessor for the local node's current group_id. We use a Fn callback instead
/// of a direct `P2PGroupManager` handle so the type signature stays independent
/// of the P2P layer (easier to test, easier to swap).
pub type LocalGroupFn = Arc<dyn Fn() -> Option<String> + Send + Sync>;

/// Concrete router used by the lightnode.
pub struct LightnodeTxRouter {
    shard_to_group: ShardGroupMap,
    num_shards: Arc<std::sync::atomic::AtomicU32>,
    local_group: LocalGroupFn,
    swarm_command_tx: mpsc::Sender<SwarmCommand>,
    /// Tier 4 Fase 2 step 3: per-group proposer cache populated from
    /// GroupAnnouncement gossip and Kademlia DHT lookups.
    proposer_cache: ProposerCache,
    /// Tier 4 Fase 2 step 5: direct-send dispatcher (gossip + AuxRequest).
    /// Reads `SAVITRI_TX_ROUTER_DIRECT_SEND` at construction time; default
    /// is gossip-only (legacy Tier 4 Fase 1 behaviour).
    dispatcher: Dispatcher,
}

impl std::fmt::Debug for LightnodeTxRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LightnodeTxRouter")
            .field(
                "num_shards",
                &self.num_shards.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

impl LightnodeTxRouter {
    pub fn new(
        shard_to_group: ShardGroupMap,
        num_shards: Arc<std::sync::atomic::AtomicU32>,
        local_group: LocalGroupFn,
        swarm_command_tx: mpsc::Sender<SwarmCommand>,
    ) -> Self {
        let proposer_cache =
            ProposerCache::default().with_swarm_tx(swarm_command_tx.clone());
        Self::new_with_cache(shard_to_group, num_shards, local_group, swarm_command_tx, proposer_cache)
    }

    /// `ProposerCache`. Lets the network task (which receives
    /// GroupAnnouncement gossip in `network/mod.rs`) populate the SAME
    /// cache instance the router consults at `route()` time. Without this
    /// shared instance the cache is module-private to the router and
    /// `update_from_announce` was never called from production code paths
    /// (only from unit tests), leaving direct-send permanently disabled
    /// even with `SAVITRI_TX_ROUTER_DIRECT_SEND=1` (memory:
    /// investigation_p1_p2_p3_2026-05-03).
    pub fn new_with_cache(
        shard_to_group: ShardGroupMap,
        num_shards: Arc<std::sync::atomic::AtomicU32>,
        local_group: LocalGroupFn,
        swarm_command_tx: mpsc::Sender<SwarmCommand>,
        proposer_cache: ProposerCache,
    ) -> Self {
        let dispatcher = Dispatcher::new(swarm_command_tx.clone());
        Self {
            shard_to_group,
            num_shards,
            local_group,
            swarm_command_tx,
            proposer_cache,
            dispatcher,
        }
    }

    /// Public accessor so the GroupAnnouncement handler in `network/mod.rs`
    /// can populate the cache when fresh gossip arrives. Returns a cheap
    /// `Clone` (Arc shallow).
    pub fn proposer_cache(&self) -> ProposerCache {
        self.proposer_cache.clone()
    }
}

impl TxRouter for LightnodeTxRouter {
    fn route(&self, raw_bytes: &[u8]) -> TxRouteDecision {
        // rate-limited tracing::warn removed — `routing_route_entry_total`
        // covers the same signal via Prometheus without the AtomicU64 cache
        // line contention or log-line noise.
        TxRoutingMetrics::inc_route_entry();
        crate::observability::PipelineObsMetrics::inc_router_route();

        // 1. Extract sender; on parse failure fall back to `FallbackLocal` so
        //    local admit can return a precise error to the client.
        let (sender, tx_hash) = match extract_sender_and_hash(raw_bytes) {
            Some(x) => x,
            None => {
                trace!("TxRouter: failed to extract sender → FallbackLocal");
                TxRoutingMetrics::inc_fallback_local("parse_fail");
                crate::observability::PipelineObsMetrics::inc_router_fallback();
                return TxRouteDecision::FallbackLocal;
            }
        };

        // 2. Lookup target group: shard = hash(sender) % num_shards; target = map[shard].
        //    If num_shards or the map is not yet populated, fall back to FallbackLocal.
        let num_shards = self.num_shards.load(std::sync::atomic::Ordering::Relaxed);
        if num_shards == 0 {
            trace!("TxRouter: num_shards=0 (map not populated yet) → FallbackLocal");
            TxRoutingMetrics::inc_fallback_local("num_shards_zero");
            crate::observability::PipelineObsMetrics::inc_router_fallback();
            return TxRouteDecision::FallbackLocal;
        }
        let shard = shard_for_sender(&sender, num_shards);
        let target_group = match self.shard_to_group.try_read() {
            Ok(guard) => guard.get(&shard).cloned(),
            Err(_) => {
                trace!("TxRouter: shard_to_group busy → FallbackLocal");
                TxRoutingMetrics::inc_fallback_local("shard_map_busy");
                crate::observability::PipelineObsMetrics::inc_router_fallback();
                return TxRouteDecision::FallbackLocal;
            }
        };
        let target_group = match target_group {
            Some(g) => g,
            None => {
                trace!(shard, "TxRouter: no target group for shard → FallbackLocal");
                TxRoutingMetrics::inc_fallback_local("no_target_group");
                crate::observability::PipelineObsMetrics::inc_router_fallback();
                return TxRouteDecision::FallbackLocal;
            }
        };

        // 3. Compare with the local group. If equal (or we don't know our group),
        //    admit locally -- the existing intra-group tx_broadcast does fan-out.
        let local_group = (self.local_group)().unwrap_or_default();

        // AtomicU64 counters removed. Same signals are now exposed by the
        // Tier 8 Prometheus metrics:
        //   * routing_decisions_total{decision="local"}
        //   * routing_decisions_total{decision="local_no_group_yet"}
        //   * routing_decisions_total{decision="forward"}
        //   * routing_route_entry_total
        // No code-side per-call counter overhead, no log firehose, single
        // dashboard query answers "what fraction of routes were local vs
        // forward vs bootstrap-fallback".

        if local_group.is_empty() || target_group == local_group {
            // "legit same-group" from "bootstrap fallback because no GroupAnnouncement
            // received yet" — the latter is a footgun (architectural_debt.md B.4)
            // that hides under the umbrella Local count.
            if local_group.is_empty() {
                TxRoutingMetrics::inc_local_decision_no_group_yet();
                crate::observability::PipelineObsMetrics::inc_router_local_no_group();
            } else {
                TxRoutingMetrics::inc_local_decision();
                crate::observability::PipelineObsMetrics::inc_router_local();
            }
            // no broadcast needed because the ingress LN is always the proposer".
            // Under multi-LN-per-group (proposer rotation introduced after that
            // commit), the elected proposer is a peer in the same group, not
            // necessarily self. We must publish on the local group's TX topic so
            // the proposer receives the TX via gossip and admits it into its own
            // mempool — without this the TX rots in the ingress LN's mempool
            // until eviction (observed: 9108 admit / 8988 evict / 0 confirmed in
            // a 5-min loadtest, with the proposer drain reading mempool_len=0
            // every round).
            //
            // We skip the publish when local_group is empty (LN has not yet
            // received any GroupAnnouncement) — in that case the legacy "Local
            // and hope someone fishes it out" semantics is preserved.
            //
            // "ingress-proposer-split"): consolidate the ingress→broadcast
            // pipeline so all admit paths (RPC + tx_channel + forward_transactions)
            // route through a single broadcast point, removing the assumption
            // diffused across the codebase.
            if !local_group.is_empty() {
                // identical to the cross-group Forwarded branch below (line ~303).
                // Receivers (network/mod.rs:2428) call decode_gossip() and expect
                // a serde_json envelope; raw_bytes were silently dropped by the
                // decoder, leaving Fix #1 (gossip publish) effectively no-op.
                let msg = crate::p2p::types::GossipMessage::Transaction(raw_bytes.to_vec());
                match crate::p2p::broadcast::encode_gossip(&msg) {
                    Ok(payload) => {
                        if let Err(reason) =
                            self.dispatcher.forward_via_gossip(&local_group, payload)
                        {
                            // returned Local on failure and the TX rotted in
                            // the ingress LN's mempool until eviction (the
                            // proposer is a different peer in the same group
                            // and never received the TX). Return
                            // RetryGossipUnavailable so the RPC handler emits
                            // a retryable error and the client backs off.
                            TxRoutingMetrics::inc_fallback_local(reason);
                            crate::observability::PipelineObsMetrics::inc_router_forward_gossip_fail();
                            crate::observability::PipelineObsMetrics::inc_router_retry();
                            return TxRouteDecision::RetryGossipUnavailable {
                                tx_hash,
                                local_group_id: local_group.clone(),
                                reason: reason.to_string(),
                            };
                        }
                        crate::observability::PipelineObsMetrics::inc_router_forward_gossip_ok();
                    }
                    Err(e) => {
                        // Encoding failure: hard reject — without gossip the
                        // proposer would never see this TX. Surface as
                        // retryable so client retries (transient pressure on
                        // serde, OOM, etc).
                        TxRoutingMetrics::inc_fallback_local("encode_gossip_fail");
                        return TxRouteDecision::RetryGossipUnavailable {
                            tx_hash,
                            local_group_id: local_group.clone(),
                            reason: format!("encode_gossip: {}", e),
                        };
                    }
                }
            }
            return TxRouteDecision::Local;
        }

        //   * Peek proposer cache for the target group.
        //   * If direct-send is enabled (env-gated) AND we have a fresh
        //     directly (request-response, ~RTT/2 latency).
        //   * Otherwise (cache miss, stale entry, env off, or direct-send
        //     channel full), fall through to gossipsub publish (legacy path).
        //   * On cache miss we additionally fire-and-forget a Kademlia
        //     refresh so the next route() may hit the cache.
        let proposer_entry = self.proposer_cache.try_get(&target_group);
        let direct_attempted =
            if self.dispatcher.is_direct_enabled()
                && proposer_entry
                    .as_ref()
                    .map(|e| e.source != ProposerSource::Stale)
                    .unwrap_or(false)
            {
                let entry = proposer_entry.as_ref().unwrap();
                match self.dispatcher.forward_direct(entry.peer_id, raw_bytes.to_vec()) {
                    DispatchOutcome::DirectQueued => {
                        TxRoutingMetrics::inc_forward_decision();
                        crate::observability::PipelineObsMetrics::inc_router_forward();
                        crate::observability::PipelineObsMetrics::inc_router_forward_direct_ok();
                        debug!(
                            target_group = %target_group,
                            peer = %entry.peer_id,
                            tx_hash = %hex::encode(&tx_hash[..8]),
                            "TxRouter: forwarded cross-group TX via direct send"
                        );
                        return TxRouteDecision::Forwarded { tx_hash };
                    }
                    DispatchOutcome::DirectChannelUnavailable { reason: _ } => {
                        // Fall through to gossip (counter already bumped).
                        crate::observability::PipelineObsMetrics::inc_router_forward_direct_fail();
                        true
                    }
                }
            } else {
                if proposer_entry.is_none() {
                    // Fire-and-forget Kad refresh so future routes may hit.
                    let cache = self.proposer_cache.clone();
                    let group = target_group.clone();
                    tokio::spawn(async move { cache.refresh_async(&group).await; });
                }
                false
            };
        let _ = direct_attempted;

        // Legacy default path: best-effort forward via gossipsub fan-out.
        let msg = crate::p2p::types::GossipMessage::Transaction(raw_bytes.to_vec());
        let payload = match crate::p2p::broadcast::encode_gossip(&msg) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "TxRouter: encode_gossip failed → FallbackLocal");
                TxRoutingMetrics::inc_fallback_local("encode_gossip_fail");
                return TxRouteDecision::FallbackLocal;
            }
        };
        if let Err(reason) = self.dispatcher.forward_via_gossip(&target_group, payload) {
            TxRoutingMetrics::inc_fallback_local(reason);
            crate::observability::PipelineObsMetrics::inc_router_forward_gossip_fail();
            crate::observability::PipelineObsMetrics::inc_router_fallback();
            return TxRouteDecision::FallbackLocal;
        }

        TxRoutingMetrics::inc_forward_decision();
        crate::observability::PipelineObsMetrics::inc_router_forward();
        crate::observability::PipelineObsMetrics::inc_router_forward_gossip_ok();
        debug!(
            target_group = %target_group,
            local_group = %local_group,
            shard,
            tx_hash = %hex::encode(&tx_hash[..8]),
            "TxRouter: forwarded cross-group TX via gossip"
        );
        TxRouteDecision::Forwarded { tx_hash }
    }
}
