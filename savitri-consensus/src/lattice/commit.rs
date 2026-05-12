//! Lineage commit — implements the **Bullshark commit rule** adapted
//! to Savitri's wall-clock-bucketed round model.
//!
//! ## Academic provenance
//!
//! - **Bullshark** [Spiegelman, Giridharan, Sonnino, Kokoris-Kogias,
//!   "Bullshark: DAG BFT Protocols Made Practical", ACM CCS 2022,
//!   <https://doi.org/10.1145/3548606.3559361>,
//!   <https://arxiv.org/abs/2201.05677>]
//!   provides the anchor + follower commit rule. Savitri reuses the
//!   2f+1 follower-vote criterion verbatim.
//!
//! - **DAG-Rider** [Keidar, Kokoris-Kogias, Naor, Spiegelman, PODC 2021,
//!   <https://doi.org/10.1145/3465084.3467905>,
//!   <https://arxiv.org/abs/2102.08325>]
//!   is the foundational DAG-BFT design Bullshark refines.
//!
//! ## Savitri-specific deviations from the papers
//!
//! 1. **Deterministic topological ordering**: round-major ascending
//!    then author-lex ascending. The papers admit any total order
//!    extending DAG order; we fix one so two observers produce
//!    byte-identical commit outputs without coordination.
//! 2. **Wall-clock-bucketed rounds** (see `lattice_runtime.rs`):
//!    rounds are `unix_secs / N` rather than derived from DAG depth.
//!    Trades NTP dependence for global-view convergence.
//! 3. **PoU-weighted pivot selection** (see `lattice::pivot`): reuses
//!    Savitri's existing PoU EMA score in place of stake-weighted or
//!    uniform random pivot.
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
