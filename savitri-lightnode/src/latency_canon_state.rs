//! Latency Canon state holder — receive-side buffer + table rebuild.
//!
//! Part of Savitri V0.2 Phase 1 (Score Canonicity, issue #31). See
//! `docs/CONSENSUS_V0.2_DESIGN.md` §3.4 for the full specification.
//!
//! Every subscriber (LN + MN) of the gossip topic
//! `/savitri/group/<gid>/latency_canon/1` calls [`ingest_report`] on each
//! verified `LatencyReport`. The state holder keeps a bounded buffer and
//! rebuilds the canonical [`LatencyTable`] on demand. Reads via
//! [`lookup_bucket`] and [`current_table`] are cheap (RwLock read).
//!
//! ## Determinism
//!
//! As long as two nodes have ingested the same set of valid reports, their
//! tables are byte-identical. Out-of-window reports are filtered during the
//! rebuild step (not at ingest time), so a slow consumer that catches up
//! later sees the same window-end as its faster peers given the same
//! observed `(window_end, window_size)` arguments.

use std::collections::VecDeque;
use std::sync::RwLock;

use savitri_consensus::types::{LatencyReport, LatencyTable, WINDOW_SIZE};

/// Maximum buffered reports. Old entries are evicted FIFO when full. Sized
/// to hold ~20 publication intervals × ~10 LN per group = 200 reports per
/// group; one state holder per node covers the local group plus margin.
pub const MAX_BUFFERED_REPORTS: usize = 512;

/// Receive-side buffer + canonical table cache.
pub struct LatencyCanonState {
    inner: RwLock<Inner>,
}

struct Inner {
    /// In-flight reports awaiting aggregation. Bounded FIFO.
    buffer: VecDeque<LatencyReport>,
    /// Cached table. Invalidated (cleared) on every new ingest; rebuilt
    /// lazily on the next `rebuild_with` call.
    cached: Option<LatencyTable>,
}

impl Default for LatencyCanonState {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyCanonState {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                buffer: VecDeque::with_capacity(MAX_BUFFERED_REPORTS),
                cached: None,
            }),
        }
    }

    /// Ingest a verified report. Callers must call
    /// `report.verify_signature()` and confirm `report.group_id` matches
    /// the local group BEFORE handing the report to this function — we do
    /// not re-verify here, both for performance and to keep the receive
    /// path tight.
    pub fn ingest_report(&self, report: LatencyReport) {
        let Ok(mut g) = self.inner.write() else {
            return;
        };
        if g.buffer.len() >= MAX_BUFFERED_REPORTS {
            g.buffer.pop_front();
        }
        g.buffer.push_back(report);
        // Invalidate cache so the next read sees the fresh report.
        g.cached = None;
    }

    /// Rebuild the canonical table with the explicit observation window
    /// `[window_end - window_size, window_end]`. Caller decides what
    /// `window_end` and `window_size` mean — typically `(current_round,
    /// WINDOW_SIZE)`. The result is cached until the next ingest.
    pub fn rebuild_with(&self, window_end: u64, window_size: u64) -> LatencyTable {
        let table = {
            let Ok(g) = self.inner.read() else {
                return LatencyTable::new();
            };
            let reports: Vec<LatencyReport> = g.buffer.iter().cloned().collect();
            LatencyTable::rebuild_from_reports(&reports, window_end, window_size)
        };
        if let Ok(mut g) = self.inner.write() {
            g.cached = Some(table.clone());
        }
        table
    }

    /// Cheap path: rebuild with the default `WINDOW_SIZE`, treating
    /// `current_round` as the most recent observation. Most call sites
    /// should use this; the explicit-window variant is for tests and
    /// future Phase 2 use.
    pub fn rebuild(&self, current_round: u64) -> LatencyTable {
        self.rebuild_with(current_round, WINDOW_SIZE)
    }

    /// Lookup the canonical bucket for a `(group, peer)` pair using the
    /// cached table. If no cache exists yet (no `rebuild` call has
    /// happened), returns `None` — caller treats `None` as "neutral
    /// score".
    pub fn lookup_bucket(&self, group_id: &str, peer_id: &str) -> Option<u8> {
        let Ok(g) = self.inner.read() else {
            return None;
        };
        g.cached.as_ref().and_then(|t| t.lookup(group_id, peer_id))
    }

    /// Read the cached table (if any). Primarily for diagnostics.
    pub fn current_table(&self) -> Option<LatencyTable> {
        let Ok(g) = self.inner.read() else {
            return None;
        };
        g.cached.clone()
    }

    /// Number of buffered reports. Diagnostics.
    pub fn buffered_count(&self) -> usize {
        self.inner.read().map(|g| g.buffer.len()).unwrap_or(0)
    }

    /// Convert a bucket value back to a canonical integer latency score
    /// in the V0.1 score range `[0, POU_SCORE_MAX]`. Used by the election
    /// logic to feed the canonical score into the candidates list while
    /// keeping signable bytes deterministic.
    ///
    /// Mapping: bucket 0 (RTT < 5ms) → 1000; each bucket subtracts 5
    /// points; bucket 200 (1000ms) and beyond → 0. Linear, no f64.
    #[inline]
    pub fn bucket_to_score(bucket: u8) -> u16 {
        let penalty = (bucket as u16).saturating_mul(5);
        1000u16.saturating_sub(penalty)
    }

    /// Convenience: lookup the bucket and convert to score, falling back
    /// to a neutral `POU_SCORE_DEFAULT` if no canonical value is known
    /// yet (bootstrap window).
    pub fn lookup_score(&self, group_id: &str, peer_id: &str) -> u16 {
        match self.lookup_bucket(group_id, peer_id) {
            Some(b) => Self::bucket_to_score(b),
            None => 1000, // Neutral max during bootstrap, see §3.9.
        }
    }
}
