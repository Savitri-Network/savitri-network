//! Latency Table — canonical chain-state mapping `(group_id, peer_id) → bucket`.
//!
//! Part of Savitri V0.2 Phase 1 (Score Canonicity). See
//! `docs/CONSENSUS_V0.2_DESIGN.md` §3.4–§3.5 for the full specification.
//!
//! The table is computed every block by aggregating the in-flight
//! [`LatencyReport`](crate::types::latency_canon::LatencyReport) buffer. All
//! observers receiving the same buffer produce a byte-identical table; this
//! is the foundation on top of which `latency_score` becomes deterministic.
//!
//! ## Aggregation
//!
//! For each `(group_id, peer_id)` pair the canonical bucket is the median of
//! all `rtt_ms_bucket` values reported for that peer in the observation
//! window. The lower median is used on even count to keep the result
//! deterministic without floating point.
//!
//! ## Bootstrap
//!
//! Until `MIN_REPORTERS` distinct reporters have contributed a sample for a
//! peer, the canonical bucket is `None`. The PoU score lookup interprets
//! `None` as "neutral score" (max), so a fresh group does not penalize its
//! members during the warmup window.

use crate::types::latency_canon::LatencyReport;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Minimum reporter sample count for an observation to be eligible for the
/// median. Observations with fewer samples are dropped.
pub const MIN_SAMPLES: u8 = 3;

/// Minimum number of distinct reporters required to produce a canonical
/// bucket. If fewer reporters contributed, the lookup returns `None`.
pub const MIN_REPORTERS: usize = 2;

/// Default observation window (in rounds). The aggregator considers reports
/// whose `round` is within `[window_end - WINDOW_SIZE, window_end]`.
pub const WINDOW_SIZE: u64 = 3;

/// Canonical per-group, per-peer RTT bucket table. Reproducible from the
/// gossip buffer by any observer.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LatencyTable {
    /// Map from `(group_id, peer_id)` to the canonical bucket. Absent keys
    /// (or present-but-`None`) mean "no canonical value yet — caller falls
    /// back to neutral score".
    pub entries: HashMap<(String, String), Option<u8>>,
    /// Last round at which the table was rebuilt. Useful for telemetry and
    /// staleness checks.
    pub last_update_round: u64,
}

impl LatencyTable {
    /// Construct an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lookup the canonical bucket for a `(group, peer)` pair. Returns
    /// `None` if no entry exists OR if the entry exists but is `None`
    /// (insufficient reporters). Caller treats `None` as "neutral score".
    pub fn lookup(&self, group_id: &str, peer_id: &str) -> Option<u8> {
        self.entries
            .get(&(group_id.to_string(), peer_id.to_string()))
            .copied()
            .flatten()
    }

    /// Rebuild the table from a gossip buffer. The buffer can include
    /// reports for multiple groups; the aggregator partitions by `group_id`
    /// internally.
    ///
    /// Caller is expected to have verified report signatures before passing
    /// them in (this function does NOT call `verify_signature` — pre-check
    /// upstream so the verification cost is paid only on report receive,
    /// not at every aggregation).
    pub fn rebuild_from_reports(
        reports: &[LatencyReport],
        window_end: u64,
        window_size: u64,
    ) -> Self {
        let mut entries: HashMap<(String, String), Option<u8>> = HashMap::new();

        // Group reports by group_id for efficient per-group processing.
        let mut by_group: HashMap<&str, Vec<&LatencyReport>> = HashMap::new();
        for r in reports {
            if r.round + window_size < window_end + 1 && r.round != window_end {
                // Outside window. Permit `r.round == window_end` and the
                // last `window_size` rounds before it inclusive.
                if window_end.saturating_sub(r.round) > window_size {
                    continue;
                }
            }
            by_group.entry(r.group_id.as_str()).or_default().push(r);
        }

        for (group_id, group_reports) in by_group {
            // Enumerate every peer mentioned anywhere in this group's
            // reports. The set of mentioned peers is the union, not just
            // the intersection — a peer with no observations is simply
            // absent from the result map.
            let mut target_peers: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for r in &group_reports {
                for obs in &r.observations {
                    target_peers.insert(obs.peer_id.clone());
                }
            }

            for target in target_peers {
                let canonical = compute_canonical_bucket(&group_reports, &target);
                entries.insert((group_id.to_string(), target), canonical);
            }
        }

        Self {
            entries,
            last_update_round: window_end,
        }
    }

    /// Number of `(group, peer)` pairs with a defined canonical bucket
    /// (i.e. `Some(_)`). Useful for telemetry.
    pub fn defined_pair_count(&self) -> usize {
        self.entries.values().filter(|v| v.is_some()).count()
    }
}

/// Internal: compute the canonical bucket for one `(group, peer)` pair given
/// the reports of one group. Returns `None` if fewer than `MIN_REPORTERS`
/// distinct reporters contributed (after dropping low-sample observations).
fn compute_canonical_bucket(group_reports: &[&LatencyReport], target: &str) -> Option<u8> {
    // Collect (reporter, bucket) pairs to ensure we count distinct reporters,
    // not distinct observations. A single reporter who somehow includes the
    // target twice counts once.
    let mut by_reporter: HashMap<&str, u8> = HashMap::new();
    for r in group_reports {
        for obs in &r.observations {
            if obs.peer_id != target {
                continue;
            }
            if obs.samples < MIN_SAMPLES {
                continue;
            }
            // Self-observation defense: a reporter cannot report on its own
            // RTT (peers report RTT-to-X, not X about X). Drop silently.
            if r.reporter == target {
                continue;
            }
            // Last-write-wins for the same reporter — but reporters should
            // not publish two reports in the same window. Safe to overwrite.
            by_reporter.insert(r.reporter.as_str(), obs.rtt_ms_bucket);
        }
    }

    if by_reporter.len() < MIN_REPORTERS {
        return None;
    }

    let mut buckets: Vec<u8> = by_reporter.into_values().collect();
    buckets.sort_unstable();
    // Lower median for deterministic tiebreak on even count. With n samples,
    // index = (n - 1) / 2 picks slot 0 for n=1, slot 1 for n=2/3, slot 2 for
    // n=4/5, etc. — the canonical lower-median definition.
    let idx = (buckets.len() - 1) / 2;
    Some(buckets[idx])
}

