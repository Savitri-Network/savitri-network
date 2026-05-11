//! ProposerCache + Kademlia DHT fallback for cross-group routing
//! (Tier 4 Fase 2 step 3+4).
//!
//! Caches the (peer_id, multiaddr) of each remote group's elected proposer
//! so that `Dispatcher::forward_direct` can send TX directly to the peer
//! who is currently producing blocks for that group, instead of relying
//! exclusively on gossipsub fan-out (which adds 50-200 ms hop latency
//! and depends on mesh topology).
//!
//! The cache is populated from two complementary sources:
//!   1. `update_from_announce()` — called by the GroupAnnouncement handler
//!      in `network/mod.rs` when a fresh group-state gossip arrives.
//!   2. `refresh_async()` — fires a `SwarmCommand::KadGetRecord` for
//!      `group_proposer:<group_id>` and the response handler in
//!      `network/mod.rs` calls `update_from_kad()` to populate the entry.
//!
//! Cache entries have a TTL (default 30 s). Stale entries are still
//! returned (best-effort) but are flagged via `ProposerSource::Stale` so
//! the dispatcher can fall back to gossipsub if the dial fails.
//!
//! Strangler safety: this module is wired into `LightnodeTxRouter` but
//! `route()` does NOT consume it yet (only telemetry — cache_hit / miss
//! counters). Activation is gated by `SAVITRI_TX_ROUTER_DIRECT_SEND=1`
//! (Step 5 of the Tier 4 Fase 2 plan).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, RwLock};

use super::metrics::TxRoutingMetrics;
use crate::p2p::swarm_commands::SwarmCommand;

/// Kademlia DHT key prefix for cross-group proposer discovery.
/// Records under this namespace are written by the elected proposer of
/// each group every ~30s and consumed by remote groups via
/// `ProposerCache::refresh_async`.
pub const KAD_PROPOSER_KEY_PREFIX: &str = "group_proposer:";

/// Default TTL for a cache entry. Past this, lookups still succeed but
/// are flagged `Stale` so the dispatcher can preempt-fallback.
// before the proposer tenure (PROPOSER_TENURE_BLOCKS = 30 blocks ~30 s)
// became the dominant rotation cadence. Under load, GroupAnnouncement
// frequency from the masternode is sparser than 30 s (~60 s+), so most
// of the time `try_get` returned the entry as `Stale` and the dispatcher
// with forward=17402 in 3 min — direct path effectively dead). With
// 120 s TTL the cache covers ~4 announcements before going stale; even
// if the proposer rotated at 30-block boundary, the new proposer is
// typically a peer in the same group_id so the ProposerCache key still
// resolves to a live peer (worst case: ACK timeout on the wrong peer
// triggers a Kademlia refresh that repopulates).
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(120);

/// Where the cache entry came from. Affects how the dispatcher trusts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposerSource {
    /// Latest GroupAnnouncement gossip from the proposer itself.
    /// Highest trust — proposer is actively broadcasting.
    GroupAnnounce,
    /// Resolved via Kademlia DHT lookup of `group_proposer:<gid>`.
    /// Trust: medium — DHT records are signed by the proposer but may be
    /// stale if proposer just rotated.
    KademliaDht,
    /// Entry past TTL but still usable as best-effort.
    Stale,
}

/// One cached proposer for a remote group.
#[derive(Clone, Debug)]
pub struct ProposerEntry {
    pub peer_id: PeerId,
    /// Optional multiaddr — `None` means "peer_id-only, let Kademlia resolve".
    pub multiaddr: Option<Multiaddr>,
    pub last_seen: Instant,
    pub source: ProposerSource,
}

impl ProposerEntry {
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        self.last_seen.elapsed() < ttl
    }
}

/// Per-group-id cache of known elected proposers.
/// Cheap to clone (`Arc` shallow).
#[derive(Clone)]
pub struct ProposerCache {
    inner: Arc<RwLock<HashMap<String, ProposerEntry>>>,
    ttl: Duration,
    /// Optional swarm command channel for fire-and-forget Kademlia lookups.
    /// `None` in tests; populated via `with_swarm_tx()` on the live router.
    swarm_tx: Option<mpsc::Sender<SwarmCommand>>,
}

impl Default for ProposerCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_TTL)
    }
}

impl ProposerCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            swarm_tx: None,
        }
    }

    /// Builder method: attach the swarm command channel so `refresh_async`
    /// can dispatch real Kademlia lookups. Without this the cache still
    /// works in announce-only mode (tests, embedded use).
    pub fn with_swarm_tx(mut self, tx: mpsc::Sender<SwarmCommand>) -> Self {
        self.swarm_tx = Some(tx);
        self
    }

    /// Hot-path lookup. Tries `try_read` to avoid blocking the route()
    /// hot path; if the lock is busy, returns `None` (caller falls back
    /// to gossipsub). Emits cache_hit / cache_miss Prometheus counters.
    ///
    /// Returns `None` when:
    ///   * lock is contested (highly unlikely with `RwLock`),
    ///   * group_id has no entry yet (caller should call `refresh_async`),
    ///   * entry is expired AND we want to force a refresh first.
    ///
    /// Returns `Some(entry)` even past TTL with `source = Stale`. The
    /// dispatcher decides what to do with stale entries.
    pub fn try_get(&self, group_id: &str) -> Option<ProposerEntry> {
        let guard = match self.inner.try_read() {
            Ok(g) => g,
            Err(_) => {
                metrics::counter!("routing_proposer_cache_total", "result" => "lock_busy")
                    .increment(1);
                return None;
            }
        };
        match guard.get(group_id) {
            Some(entry) if entry.is_fresh(self.ttl) => {
                metrics::counter!("routing_proposer_cache_total", "result" => "hit").increment(1);
                Some(entry.clone())
            }
            Some(entry) => {
                // Past TTL but still return as Stale (best-effort).
                metrics::counter!("routing_proposer_cache_total", "result" => "stale").increment(1);
                let mut stale = entry.clone();
                stale.source = ProposerSource::Stale;
                Some(stale)
            }
            None => {
                metrics::counter!("routing_proposer_cache_total", "result" => "miss").increment(1);
                None
            }
        }
    }

    /// Sync update path — called by the GroupAnnouncement handler in
    /// `network/mod.rs` whenever a fresh group-state gossip arrives.
    ///
    /// Overwrites the existing entry (if any) with `source = GroupAnnounce`,
    /// since gossip is the most authoritative signal.
    pub async fn update_from_announce(
        &self,
        group_id: String,
        peer_id: PeerId,
        multiaddr: Option<Multiaddr>,
    ) {
        let entry = ProposerEntry {
            peer_id,
            multiaddr,
            last_seen: Instant::now(),
            source: ProposerSource::GroupAnnounce,
        };
        self.inner.write().await.insert(group_id, entry);
        metrics::counter!("routing_proposer_cache_update_total", "via" => "announce").increment(1);
    }

    /// Sync update path — called by the Kademlia GetRecord response
    /// handler in `network/mod.rs` when a `group_proposer:*` record
    /// resolves successfully.
    ///
    /// Does NOT overwrite a fresh `GroupAnnounce` entry (gossip wins),
    /// only fills in cache misses or stale entries.
    pub async fn update_from_kad(
        &self,
        group_id: String,
        peer_id: PeerId,
        multiaddr: Option<Multiaddr>,
    ) {
        let mut guard = self.inner.write().await;
        let should_overwrite = match guard.get(&group_id) {
            Some(existing) => {
                existing.source != ProposerSource::GroupAnnounce || !existing.is_fresh(self.ttl)
            }
            None => true,
        };
        if should_overwrite {
            guard.insert(
                group_id,
                ProposerEntry {
                    peer_id,
                    multiaddr,
                    last_seen: Instant::now(),
                    source: ProposerSource::KademliaDht,
                },
            );
            metrics::counter!("routing_proposer_cache_update_total", "via" => "kad").increment(1);
        }
    }

    /// Mark the entry stale (without removing it). Called by the dispatcher
    /// when a dial to the cached peer fails (peer evicted, gone offline).
    /// Next `try_get` will return `Stale` and the caller can trigger Kad
    /// refresh + fall back to gossipsub.
    pub async fn mark_stale(&self, group_id: &str) {
        if let Some(entry) = self.inner.write().await.get_mut(group_id) {
            entry.source = ProposerSource::Stale;
            // Do not touch last_seen — that drives the TTL eviction.
            metrics::counter!("routing_proposer_cache_mark_stale_total").increment(1);
        }
    }

    /// Fire-and-forget Kademlia DHT lookup for `group_proposer:<group_id>`.
    /// The response is asynchronous — when the Kademlia handler in
    /// `network/mod.rs` resolves the key it must call
    /// `update_from_kad()` to populate the cache.
    ///
    /// `try_send` is non-blocking: on a full channel we record a counter
    /// and skip the refresh (the next route() will retry).
    pub async fn refresh_async(&self, group_id: &str) {
        let Some(tx) = self.swarm_tx.as_ref() else {
            // No channel attached (test build or not wired yet); count and skip.
            metrics::counter!("routing_proposer_cache_kad_refresh_total", "result" => "no_channel")
                .increment(1);
            return;
        };
        let key = format!("{}{}", KAD_PROPOSER_KEY_PREFIX, group_id);
        match tx.try_send(SwarmCommand::KadGetRecord { key }) {
            Ok(()) => {
                metrics::counter!("routing_proposer_cache_kad_refresh_total", "result" => "queued")
                    .increment(1);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                metrics::counter!("routing_proposer_cache_kad_refresh_total", "result" => "full")
                    .increment(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                metrics::counter!("routing_proposer_cache_kad_refresh_total", "result" => "closed")
                    .increment(1);
            }
        }
    }

    /// Snapshot of the current cache size. For telemetry only.
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}
