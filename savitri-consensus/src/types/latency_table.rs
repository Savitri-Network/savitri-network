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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::latency_canon::PeerLatencyObservation;

    fn make_report(
        round: u64,
        group: &str,
        reporter: &str,
        observations: Vec<(&str, u8, u8)>,
    ) -> LatencyReport {
        LatencyReport {
            round,
            group_id: group.to_string(),
            reporter: reporter.to_string(),
            observations: observations
                .into_iter()
                .map(|(p, b, s)| PeerLatencyObservation {
                    peer_id: p.to_string(),
                    rtt_ms_bucket: b,
                    samples: s,
                })
                .collect(),
            reporter_pubkey: [0u8; 32],
            signature: [0u8; 64],
        }
    }

    #[test]
    fn empty_buffer_produces_empty_table() {
        let t = LatencyTable::rebuild_from_reports(&[], 10, 3);
        assert_eq!(t.last_update_round, 10);
        assert!(t.entries.is_empty());
    }

    #[test]
    fn single_reporter_below_min_returns_none() {
        // MIN_REPORTERS = 2, but we have only 1 distinct reporter for ln-A.
        let reports = vec![make_report(10, "g", "ln-1", vec![("ln-A", 8, 5)])];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        assert_eq!(t.lookup("g", "ln-A"), None);
    }

    #[test]
    fn median_of_two_uses_lower() {
        // n=2 → lower median = first element after sort.
        let reports = vec![
            make_report(10, "g", "ln-1", vec![("ln-A", 8, 5)]),
            make_report(10, "g", "ln-2", vec![("ln-A", 12, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        assert_eq!(t.lookup("g", "ln-A"), Some(8));
    }

    #[test]
    fn median_of_four_uses_lower() {
        let reports = vec![
            make_report(10, "g", "ln-1", vec![("ln-A", 8, 5)]),
            make_report(10, "g", "ln-2", vec![("ln-A", 12, 5)]),
            make_report(10, "g", "ln-3", vec![("ln-A", 20, 5)]),
            make_report(10, "g", "ln-4", vec![("ln-A", 30, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        // Sorted [8, 12, 20, 30]; idx = (4-1)/2 = 1 → 12.
        assert_eq!(t.lookup("g", "ln-A"), Some(12));
    }

    #[test]
    fn byzantine_minority_cannot_shift_median() {
        // n=5, BFT f=1 → at most 1 byzantine. Honest cluster reports
        // bucket ~10; byzantine reports 255 (max).
        let reports = vec![
            make_report(10, "g", "ln-1", vec![("ln-A", 9, 10)]),
            make_report(10, "g", "ln-2", vec![("ln-A", 10, 10)]),
            make_report(10, "g", "ln-3", vec![("ln-A", 11, 10)]),
            make_report(10, "g", "ln-4", vec![("ln-A", 10, 10)]),
            make_report(10, "g", "ln-5", vec![("ln-A", 255, 10)]), // Byzantine
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        // Sorted [9, 10, 10, 11, 255]; idx = (5-1)/2 = 2 → 10. Byzantine
        // outlier does not move the median.
        assert_eq!(t.lookup("g", "ln-A"), Some(10));
    }

    #[test]
    fn observation_window_filters_old_reports() {
        // window_end=10, window_size=3 → admit rounds [7, 10] inclusive.
        // Round 6 must be filtered out.
        let reports = vec![
            make_report(6, "g", "ln-1", vec![("ln-A", 8, 5)]), // too old
            make_report(8, "g", "ln-2", vec![("ln-A", 8, 5)]),
            make_report(9, "g", "ln-3", vec![("ln-A", 8, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        // ln-1 dropped; remaining 2 reporters → MIN_REPORTERS met → Some(8).
        assert_eq!(t.lookup("g", "ln-A"), Some(8));
    }

    #[test]
    fn low_sample_observations_are_dropped() {
        // MIN_SAMPLES = 3. Reporters with samples < 3 are filtered out.
        let reports = vec![
            make_report(10, "g", "ln-1", vec![("ln-A", 8, 2)]), // dropped
            make_report(10, "g", "ln-2", vec![("ln-A", 9, 1)]), // dropped
            make_report(10, "g", "ln-3", vec![("ln-A", 10, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        // Only ln-3 survives → fewer than MIN_REPORTERS → None.
        assert_eq!(t.lookup("g", "ln-A"), None);
    }

    #[test]
    fn self_observations_are_ignored() {
        // ln-A cannot report on its own RTT to itself; should be dropped.
        let reports = vec![
            make_report(10, "g", "ln-A", vec![("ln-A", 0, 100)]), // self → drop
            make_report(10, "g", "ln-1", vec![("ln-A", 10, 5)]),
            make_report(10, "g", "ln-2", vec![("ln-A", 12, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        assert_eq!(t.lookup("g", "ln-A"), Some(10)); // lower median of [10, 12]
    }

    #[test]
    fn groups_are_partitioned_independently() {
        let reports = vec![
            make_report(10, "g1", "ln-1", vec![("ln-A", 5, 5)]),
            make_report(10, "g1", "ln-2", vec![("ln-A", 7, 5)]),
            make_report(10, "g2", "ln-3", vec![("ln-A", 50, 5)]),
            make_report(10, "g2", "ln-4", vec![("ln-A", 60, 5)]),
        ];
        let t = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        // Same peer_id in different groups → independent canonical values.
        assert_eq!(t.lookup("g1", "ln-A"), Some(5));
        assert_eq!(t.lookup("g2", "ln-A"), Some(50));
    }

    #[test]
    fn table_is_observer_independent() {
        // The actual core claim of this whole module: two different
        // observers fed the same report buffer produce byte-identical
        // tables. We construct two distinct call sites and compare.
        let reports = vec![
            make_report(10, "g", "ln-1", vec![("ln-A", 8, 5), ("ln-B", 20, 5)]),
            make_report(10, "g", "ln-2", vec![("ln-A", 12, 5), ("ln-B", 25, 5)]),
            make_report(10, "g", "ln-3", vec![("ln-A", 15, 5), ("ln-B", 22, 5)]),
        ];
        let observer_a = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        let observer_b = LatencyTable::rebuild_from_reports(&reports, 10, 3);
        assert_eq!(observer_a, observer_b);
        // Sanity: both observers see the same canonical values.
        assert_eq!(observer_a.lookup("g", "ln-A"), Some(12)); // lower median of [8,12,15]
        assert_eq!(observer_a.lookup("g", "ln-B"), Some(22)); // lower median of [20,22,25]
    }
}
