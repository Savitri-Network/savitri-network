//! Cycle pivot election — maps a cycle index to the elected pivot
//! author using the existing PoU-weighted round-robin schedule.
//!
//! Part of Savitri V0.2 Phase 2 (Lattice ordering). The election
//! primitive is unchanged from Phase 1: the helper
//! `build_weighted_proposer_schedule` already produces a deterministic
//! sequence of slots, each filled with a peer chosen proportionally to
//! their PoU score. Phase 2 only changes *what one slot represents* —
//! from "block index" to "cycle index".
//!
//! ## Why this module exists
//!
//! - Keep the call-site for cycle pivot election in one place so
//!   future maintenance (e.g. dynamic group rotation) lands here.
//! - Document the slot-semantics change so the next engineer reading
//!   `build_weighted_proposer_schedule` knows both call shapes.
//! - Allow unit tests for the cycle → pivot mapping to live next to
//!   the rest of the Lattice runtime.
//!
//! ## Determinism contract
//!
//! Given `(group_id, cycle_index, ranked_pou)`, the output is a stable
//! `String` peer_id. Two observers with the same `ranked_pou` input
//! (which the Phase 1 wall-clock-bucket convergence guarantees, after
//! the canonical `LatencyTable` settles) elect the same pivot for the
//! same cycle.

use crate::types::lattice::CycleIndex;

/// Number of cycle slots in one schedule rotation. Matches
/// `PROPOSER_TENURE_BLOCKS` in the lightnode (currently 10).
///
/// Re-defined here as a constant rather than imported from the
/// lightnode to keep the consensus crate's dependency surface clean.
/// If the lightnode value changes, this constant MUST be updated in
/// lockstep — otherwise pivot election diverges between LN and the
/// commit walker.
pub const PIVOT_TENURE_SLOTS: usize = 10;

/// Elect the pivot for cycle `k` in group `group_id`.
///
/// The `ranked_pou` slice contains `(peer_id, pou_score)` pairs already
/// sorted by score descending (with peer_id ascending as canonical
/// tiebreak — same as the helper used by `determine_proposer`). The
/// caller pulls this from the canonical `LatencyTable`-driven scoring;
/// because the table is byte-identical cluster-wide post Phase 2
/// convergence, every observer hands the same sorted vector to this
/// function.
///
/// Returns `None` if `ranked_pou` is empty.
///
/// The function delegates to the existing weighted-RR helper. Phase 2
/// re-implements the schedule generation inline here (rather than
/// reaching across crates to the lightnode) to keep the dependency
/// arrow consistent: lightnode depends on consensus, never the
/// reverse.
pub fn pivot_for_cycle(
    group_id: &str,
    cycle_index: CycleIndex,
    ranked_pou: &[(String, u32)],
) -> Option<String> {
    if ranked_pou.is_empty() {
        return None;
    }
    let schedule = build_weighted_pivot_schedule(ranked_pou, cycle_index, group_id);
    let slot = (cycle_index as usize) % PIVOT_TENURE_SLOTS;
    schedule.get(slot).cloned()
}

/// Build a deterministic weighted round-robin schedule of length
/// `PIVOT_TENURE_SLOTS`, with each peer occupying a number of slots
/// proportional to their PoU score, then deterministically shuffled.
///
/// Mirrors the lightnode's `build_weighted_proposer_schedule` — see
/// the lightnode source for the original rationale comments. The
/// schedule changes deterministically at every
/// `cycle_index / PIVOT_TENURE_SLOTS` boundary (a "tenure window"),
/// so consecutive cycles within the same window share a stable
/// rotation while still scheduling all eligible peers.
fn build_weighted_pivot_schedule(
    ranked: &[(String, u32)],
    cycle_index: CycleIndex,
    group_id: &str,
) -> Vec<String> {
    let n_slots = PIVOT_TENURE_SLOTS;
    let total: u64 = ranked
        .iter()
        .map(|(_, s)| (*s as u64).max(1))
        .sum::<u64>()
        .max(1);

    // Slot allocation: each peer gets ⌊(score / total) * N⌋ slots,
    // distributed sequentially. The padding floor at the end picks
    // the top-PoU peer for any rounding-down deficit.
    let mut schedule: Vec<String> = Vec::with_capacity(n_slots);
    let mut acc = 0u64;
    for (peer, score) in ranked {
        let s = (*score as u64).max(1);
        let target = ((acc + s) * n_slots as u64) / total;
        while (schedule.len() as u64) < target {
            schedule.push(peer.clone());
        }
        acc += s;
    }
    while schedule.len() < n_slots {
        schedule.push(ranked[0].0.clone());
    }

    // Deterministic shuffle (Fisher-Yates with blake3-seeded
    // xorshift64). Seed = `(group_id, cycle_index / N)` so the
    // shuffle is stable across all cycles in a tenure window and
    // changes only at boundaries.
    let tenure_window = cycle_index / PIVOT_TENURE_SLOTS as u64;
    let seed_input = format!("savitri-lattice-pivot-v1|{}|{}", group_id, tenure_window);
    let seed_hash = blake3::hash(seed_input.as_bytes());
    let seed_bytes = seed_hash.as_bytes();
    let mut rng_state = u64::from_le_bytes(seed_bytes[0..8].try_into().unwrap_or([1u8; 8]));
    if rng_state == 0 {
        rng_state = 0xdead_beef_dead_beef;
    }
    for i in (1..schedule.len()).rev() {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        let j = (rng_state as usize) % (i + 1);
        schedule.swap(i, j);
    }

    schedule
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranked(items: &[(&str, u32)]) -> Vec<(String, u32)> {
        items.iter().map(|(s, n)| (s.to_string(), *n)).collect()
    }

    #[test]
    fn empty_ranked_returns_none() {
        assert!(pivot_for_cycle("g", 0, &[]).is_none());
    }

    #[test]
    fn single_peer_always_elected() {
        let r = ranked(&[("ln-1", 500)]);
        for cycle in 0..20u64 {
            assert_eq!(pivot_for_cycle("g", cycle, &r).as_deref(), Some("ln-1"));
        }
    }

    #[test]
    fn schedule_deterministic_across_observers() {
        let r = ranked(&[("ln-a", 800), ("ln-b", 600), ("ln-c", 400), ("ln-d", 200)]);
        // Two observers, same input → same output for the same cycle.
        for cycle in 0..PIVOT_TENURE_SLOTS as u64 {
            let o1 = pivot_for_cycle("g42", cycle, &r);
            let o2 = pivot_for_cycle("g42", cycle, &r);
            assert_eq!(o1, o2);
        }
    }

    #[test]
    fn high_pou_peer_dominates_slots() {
        // Pou score 700 + 200 + 100 = 1000 total. Over a tenure of
        // 10 slots, ln-a should get ~7 slots, ln-b ~2, ln-c ~1.
        // Exact distribution depends on the shuffle, but the
        // dominant peer should be elected more than the rest combined.
        let r = ranked(&[("ln-a", 700), ("ln-b", 200), ("ln-c", 100)]);
        let mut counts = std::collections::HashMap::new();
        for cycle in 0..PIVOT_TENURE_SLOTS as u64 {
            let p = pivot_for_cycle("g", cycle, &r).unwrap();
            *counts.entry(p).or_insert(0) += 1;
        }
        let a_count = counts.get("ln-a").copied().unwrap_or(0);
        let others: u32 = counts
            .iter()
            .filter(|(k, _)| k.as_str() != "ln-a")
            .map(|(_, v)| *v)
            .sum();
        assert!(
            a_count > others,
            "ln-a={} others={}, expected dominance",
            a_count,
            others
        );
    }

    #[test]
    fn shuffle_changes_between_tenure_windows() {
        let r = ranked(&[("ln-a", 500), ("ln-b", 500), ("ln-c", 500), ("ln-d", 500)]);
        // Same cycle_index within a tenure window → same pivot.
        // Different tenure window → potentially different pivot
        // (shuffle re-seeded with `cycle_index / N`).
        let window_0 = pivot_for_cycle("g", 0, &r).unwrap();
        let same_window = pivot_for_cycle("g", 0, &r).unwrap();
        assert_eq!(window_0, same_window);

        let window_1 = pivot_for_cycle("g", PIVOT_TENURE_SLOTS as u64, &r).unwrap();
        let same_window_1 = pivot_for_cycle("g", PIVOT_TENURE_SLOTS as u64, &r).unwrap();
        assert_eq!(window_1, same_window_1);

        // With 4 equal-score peers and a deterministic shuffle, the
        // window 0 and window 1 first slots are most-likely
        // different. We don't enforce that strictly (collisions are
        // possible with small N) but we do check the two windows
        // produce SOME different cycle outputs.
        let mut diffs = 0;
        for c in 0..PIVOT_TENURE_SLOTS as u64 {
            let a = pivot_for_cycle("g", c, &r).unwrap();
            let b = pivot_for_cycle("g", c + PIVOT_TENURE_SLOTS as u64, &r).unwrap();
            if a != b {
                diffs += 1;
            }
        }
        assert!(diffs > 0, "expected some different pivots across windows");
    }
}
