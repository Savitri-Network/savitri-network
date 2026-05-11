//! Lineage commit — Bullshark-family rule that decides cycle commits
//! and emits the deterministic ordering of committed cells.
//!
//! Part of Savitri V0.2 Phase 2 (Lattice ordering, issue #32 follow-up
//! to #31). Sits on top of [`crate::lattice::aggregator::LatticeAggregator`]
//! and produces commit decisions.
//!
//! ## The rule
//!
//! For cycle `k` with elected pivot `P` at anchor round `2k`:
//!   1. Locate `P`'s certified cell at round `2k` (call it `anchor_cell`).
//!   2. Count cells in the follow round `2k+1` whose `parents` set
//!      contains `anchor_cell.cell_id()`.
//!   3. If the count meets the BFT quorum, the cycle commits.
//!   4. Committed cells = the causal history of `anchor_cell` walked
//!      backwards through certified-cell `parents` edges, in
//!      deterministic topological order:
//!        - round-major ascending (round 0 first)
//!        - within the same round, author lexicographic ascending.
//!
//! ## Determinism
//!
//! Two observers feeding the same `LatticeAggregator` state to this
//! module produce byte-identical [`CommitDecision`] outputs. There is
//! no f64, no wall-clock, no per-observer state. The only source of
//! non-determinism would be uncertified cells — and by construction we
//! only walk certified ones.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::lattice::aggregator::LatticeAggregator;
use crate::types::lattice::{CellId, Cycle, CycleIndex, LatticeRound};

/// Outcome of an attempted cycle commit.
#[derive(Debug, PartialEq, Eq)]
pub enum CommitDecision {
    /// The cycle committed. The returned [`Cycle`] carries the
    /// ordered list of committed cell ids.
    Committed(Cycle),
    /// The cycle's anchor cell is not yet certified. The runtime
    /// retries when more cells arrive.
    AnchorNotCertified,
    /// The anchor exists but does not yet have quorum followers in
    /// the next round. Retry on more follow-round cells.
    BelowFollowerQuorum {
        /// Distinct followers observed so far.
        follower_count: usize,
        /// Quorum threshold (informational).
        quorum: usize,
    },
}

/// Pure stateless walker that consumes an aggregator snapshot and
/// produces a [`CommitDecision`]. No mutable state inside — the caller
/// may invoke this repeatedly with the same aggregator until the
/// outcome moves to `Committed`.
pub struct LineageCommit;

impl LineageCommit {
    /// Attempt to commit a single cycle.
    ///
    /// Arguments:
    /// - `aggregator`: read-only view of the DAG state.
    /// - `cycle_index`: `k` to attempt.
    /// - `pivot_author`: result of `pivot_for_cycle(group_id, k, ...)`.
    /// - `group_id`: identity to put in the emitted `Cycle`.
    pub fn try_commit(
        aggregator: &LatticeAggregator,
        cycle_index: CycleIndex,
        pivot_author: &str,
        group_id: &str,
    ) -> CommitDecision {
        let anchor_round = cycle_index.saturating_mul(2);
        let follow_round = anchor_round.saturating_add(1);
        let quorum = aggregator.quorum();

        // Step 1: locate the pivot's anchor cell.
        let anchor_cert = match aggregator.certified_get(anchor_round, pivot_author) {
            Some(c) => c,
            None => return CommitDecision::AnchorNotCertified,
        };
        let anchor_cell_id = anchor_cert.cell_id();

        // Step 2: count followers in round 2k+1.
        let follower_count = aggregator
            .certified_at_round(follow_round)
            .filter(|c| c.cell.parents.contains(&anchor_cell_id))
            .count();

        if follower_count < quorum {
            return CommitDecision::BelowFollowerQuorum {
                follower_count,
                quorum,
            };
        }

        // Step 3: walk causal history of the anchor.
        let committed_cells = walk_causal_history(aggregator, anchor_cell_id, anchor_round);

        CommitDecision::Committed(Cycle {
            index: cycle_index,
            group_id: group_id.to_string(),
            pivot: pivot_author.to_string(),
            pivot_cell: anchor_cell_id,
            committed_cells,
        })
    }
}

/// Walk backwards from `start` through certified `parents` edges and
/// return the visited cell ids in deterministic topological order:
/// round-major ascending, then author lexicographic ascending.
///
/// Algorithm: BFS from `start`, collecting `(round, author, cell_id)`
/// triples; sort by `(round, author)` for the final order.
fn walk_causal_history(
    aggregator: &LatticeAggregator,
    start: CellId,
    start_round: LatticeRound,
) -> Vec<CellId> {
    // Map cell_id -> (round, author) so we can collect + sort.
    let mut visited: BTreeMap<(LatticeRound, String), CellId> = BTreeMap::new();
    let mut queue: VecDeque<(CellId, LatticeRound)> = VecDeque::new();
    let mut seen: BTreeSet<CellId> = BTreeSet::new();

    queue.push_back((start, start_round));
    seen.insert(start);

    while let Some((cell_id, _round)) = queue.pop_front() {
        // Find this cell in the certified table. We may not always
        // know its `(round, author)` without scanning — but the
        // aggregator's `certified` BTreeMap is keyed by them, so we
        // do a linear scan over a small range. Cheaper alternative
        // for future work: maintain a reverse index `cell_id ->
        // (round, author)` inside the aggregator.
        let (cell_round, cell_author, cell_ref) = match find_by_id(aggregator, cell_id) {
            Some(t) => t,
            None => continue, // not certified — skip
        };
        visited.insert((cell_round, cell_author), cell_id);
        // Enqueue parents.
        for parent in &cell_ref.cell.parents {
            if seen.insert(*parent) {
                queue.push_back((*parent, cell_round.saturating_sub(1)));
            }
        }
    }

    // BTreeMap iteration is already sorted by (round, author), which is
    // exactly the canonical commit order.
    visited.into_values().collect()
}

/// Linear scan helper — finds the certified cert whose cell hashes to
/// `id`. Only called inside the BFS, so cost is `O(|certified|)` per
/// step. Future work: maintain an `id_index` in the aggregator.
fn find_by_id<'a>(
    aggregator: &'a LatticeAggregator,
    id: CellId,
) -> Option<(
    LatticeRound,
    String,
    &'a crate::types::lattice::CellCertificate,
)> {
    // The aggregator does not currently expose a cell_id-keyed index,
    // so we iterate. The certified BTreeMap is bounded by
    // retention_rounds × group_size, so this stays cheap in practice.
    // We expose this via a simple iter over rounds with `aggregator.certified_at_round`
    // — but we don't know the round here. Walk the high_water region.
    let high = aggregator.high_water_round();
    for r in (0..=high).rev() {
        for cert in aggregator.certified_at_round(r) {
            if cert.cell_id() == id {
                return Some((r, cert.cell.author.clone(), cert));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lattice::aggregator::{AggregatorConfig, LatticeAggregator};
    use crate::types::lattice::{CellAttestation, LatticeCell};
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    /// Build + insert a certified cell at `(round, author)` with the
    /// given parents and group_size=4 (quorum=3).
    fn add_certified(
        agg: &mut LatticeAggregator,
        round: LatticeRound,
        author: &str,
        parents: Vec<CellId>,
    ) -> CellId {
        let mut csprng = OsRng;
        let sk = SigningKey::generate(&mut csprng);
        let pk = sk.verifying_key();
        let mut cell = LatticeCell::with_sorted_parents(
            round,
            "g".to_string(),
            author.to_string(),
            pk.to_bytes(),
            parents,
            [round as u8; 32],
            [0u8; 64],
        );
        cell.author_signature = sk.sign(&cell.signable_bytes()).to_bytes();
        let id = agg.observe_cell(cell.clone()).unwrap();
        for signer in ["a1", "a2", "a3"] {
            let ssk = SigningKey::generate(&mut csprng);
            let spk = ssk.verifying_key();
            let att = CellAttestation {
                signer: signer.to_string(),
                signer_pubkey: spk.to_bytes(),
                signature: ssk.sign(&cell.signable_bytes()).to_bytes(),
            };
            let _ = agg.observe_attestation(id, att);
        }
        id
    }

    #[test]
    fn anchor_not_certified_returns_signal() {
        let agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        let outcome = LineageCommit::try_commit(&agg, 0, "ln-pivot", "g");
        assert_eq!(outcome, CommitDecision::AnchorNotCertified);
    }

    #[test]
    fn below_follower_quorum_returns_signal() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        // Cycle 0 → anchor round 0, follow round 1.
        let anchor = add_certified(&mut agg, 0, "ln-pivot", vec![]);
        // 0 followers in round 1.
        let outcome = LineageCommit::try_commit(&agg, 0, "ln-pivot", "g");
        match outcome {
            CommitDecision::BelowFollowerQuorum {
                follower_count: 0,
                quorum: 3,
            } => {}
            other => panic!("expected BelowFollowerQuorum, got {:?}", other),
        }
        // Sanity: anchor exists.
        assert!(agg.certified_get(0, "ln-pivot").is_some());
        let _ = anchor;
    }

    #[test]
    fn commit_when_quorum_followers_reference_anchor() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        // Anchor + 3 followers.
        let anchor = add_certified(&mut agg, 0, "ln-pivot", vec![]);
        for follower in ["ln-a", "ln-b", "ln-c"] {
            let _ = add_certified(&mut agg, 1, follower, vec![anchor]);
        }
        let outcome = LineageCommit::try_commit(&agg, 0, "ln-pivot", "g");
        match outcome {
            CommitDecision::Committed(cycle) => {
                assert_eq!(cycle.index, 0);
                assert_eq!(cycle.pivot, "ln-pivot");
                assert_eq!(cycle.pivot_cell, anchor);
                // Anchor is in committed_cells (its own causal history
                // includes itself).
                assert!(cycle.committed_cells.contains(&anchor));
            }
            other => panic!("expected Committed, got {:?}", other),
        }
    }

    #[test]
    fn commit_walks_causal_history_in_canonical_order() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        // Round 0: three cells (A, B, C). All authored, all certified.
        let r0_a = add_certified(&mut agg, 0, "ln-a", vec![]);
        let r0_b = add_certified(&mut agg, 0, "ln-b", vec![]);
        let r0_c = add_certified(&mut agg, 0, "ln-c", vec![]);
        // Anchor round = 0, pivot = ln-a.
        // Pivot anchor cell needs followers in round 1 referencing it.
        // We add 3 round-1 cells (ln-a, ln-b, ln-c) each referencing
        // r0_a. Their causal history reach r0_a (referenced) only —
        // r0_b and r0_c are NOT in r0_a's history.
        for author in ["ln-a", "ln-b", "ln-c"] {
            let _ = add_certified(&mut agg, 1, author, vec![r0_a]);
        }
        let outcome = LineageCommit::try_commit(&agg, 0, "ln-a", "g");
        match outcome {
            CommitDecision::Committed(cycle) => {
                // Should contain only r0_a (the anchor itself).
                // r0_b, r0_c are not in the causal lineage of r0_a.
                assert!(cycle.committed_cells.contains(&r0_a));
                assert!(!cycle.committed_cells.contains(&r0_b));
                assert!(!cycle.committed_cells.contains(&r0_c));
            }
            other => panic!("expected Committed, got {:?}", other),
        }
    }

    #[test]
    fn commit_includes_transitive_parents() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        // Round 0: A.
        let r0_a = add_certified(&mut agg, 0, "ln-a", vec![]);
        // Round 1: B parents = [A]. C parents = [A]. D parents = [A].
        let r1_b = add_certified(&mut agg, 1, "ln-b", vec![r0_a]);
        let _r1_c = add_certified(&mut agg, 1, "ln-c", vec![r0_a]);
        let _r1_d = add_certified(&mut agg, 1, "ln-d", vec![r0_a]);
        // Round 2: P (the pivot at cycle 1). parents = [B].
        // P then commits B + A (transitive).
        let p = add_certified(&mut agg, 2, "ln-pivot", vec![r1_b]);
        // Follow round 3: 3 followers of P.
        for f in ["fa", "fb", "fc"] {
            let _ = add_certified(&mut agg, 3, f, vec![p]);
        }
        let outcome = LineageCommit::try_commit(&agg, 1, "ln-pivot", "g");
        match outcome {
            CommitDecision::Committed(cycle) => {
                assert!(cycle.committed_cells.contains(&p));
                assert!(cycle.committed_cells.contains(&r1_b));
                assert!(cycle.committed_cells.contains(&r0_a));
                // r1_c and r1_d are NOT reachable from p.
                // Order: round 0 < round 1 < round 2. Within
                // round 1, only "ln-b". So the order should be
                // [r0_a, r1_b, p].
                let order: Vec<_> = cycle.committed_cells.iter().copied().collect();
                assert_eq!(order, vec![r0_a, r1_b, p]);
            }
            other => panic!("expected Committed, got {:?}", other),
        }
    }
}
