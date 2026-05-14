//! Cycle pivot election — maps a cycle index to the elected pivot
//! author using the existing PoU-weighted round-robin schedule.
//!
//! ## Academic provenance
//!
//! The notion of a *pivot author* (called "anchor" or "leader" in the
//! DAG-BFT literature) comes from:
//!
//! - **DAG-Rider** [Keidar et al., PODC 2021, <https://arxiv.org/abs/2102.08325>]
//!   uses a random oracle to pick the leader of each wave.
//! - **Bullshark** [Spiegelman et al., CCS 2022, <https://arxiv.org/abs/2201.05677>]
//!   uses a public-coin shared randomness beacon for anchor selection.
//!
//! Weighted (rather than uniform) leader/committee selection follows
//! the **Algorand** lineage:
//!
//! - **Algorand committee selection** [Gilad, Hemo, Micali, Vlachos,
//!   Zeldovich, "Algorand: Scaling Byzantine Agreements for
//!   Cryptocurrencies", SOSP 2017,
//!   <https://doi.org/10.1145/3132747.3132757>] uses VRF-based
//!   cryptographic sortition weighted by stake.
//!
//! ## Savitri-specific deviations
//!
//! 1. **VRF substituted by blake3-seeded Fisher-Yates shuffle**: deterministic
//!    given `(group_id, cycle_index/ranked.len())` seed. Trade-off:
//!    no per-slot unpredictability that VRF gives, but full
//!    cluster-wide convergence without a distributed randomness beacon.
//! 2. **PoU score in place of stake**: Savitri's 5-component reputation
//!    EMA (availability 25%, latency 20%, integrity 20%, reputation 20%,
//!    participation 15%) replaces capital-stake as the weighting input.
//!    This makes pivot selection **behaviour-weighted**, not
//!    capital-weighted. To the best of our knowledge, no production L1
//!    has shipped a multi-attribute reputation score for DAG-BFT
//!    anchor selection — this is one of Savitri's original
//!    contributions.
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
    // A.7: rotate every cycle by indexing into a schedule whose
    // length matches the number of ranked candidates. ranked_pou is
    // already verified non-empty above, so the modulo is safe.
    let slot = (cycle_index as usize) % ranked_pou.len();
    schedule.get(slot).cloned()
}

/// Build a deterministic weighted round-robin schedule of length
/// `ranked.len()`, with each peer occupying a number of slots
/// proportional to their PoU score, then deterministically shuffled.
///
/// Mirrors the lightnode's `build_weighted_proposer_schedule` — see
/// the lightnode source for the original rationale comments. The
/// schedule changes deterministically at every
/// `cycle_index / ranked.len()` boundary (a "tenure window"), so
/// consecutive cycles within the same window share a stable
/// rotation while still scheduling all eligible peers. With
/// `ranked.len() == N`, the pivot rotates every cycle within a
/// tenure window of N consecutive cycles.
///
/// A.7: pre-A.7 this used a global `PIVOT_TENURE_SLOTS` constant.
/// When that constant was pinned at 1 in an earlier iteration, the
/// proportional-allocation loop floored every per-peer slot to 0
/// except the last peer, collapsing the schedule to a single fixed
/// pivot and disabling rotation entirely. Sizing the schedule to
/// `ranked.len()` makes every peer get at least one slot when
/// scores tie (canonical bootstrap), and a proportional number of
/// slots when scores differ.
fn build_weighted_pivot_schedule(
    ranked: &[(String, u32)],
    cycle_index: CycleIndex,
    group_id: &str,
) -> Vec<String> {
    let n_slots = ranked.len();
    if n_slots == 0 {
        return Vec::new();
    }
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
    let tenure_window = cycle_index / n_slots as u64;
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
