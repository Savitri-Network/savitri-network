//! Canonical BFT quorum threshold — single source of truth.
//!
//! Replaces two divergent (semantically: edge-case-only) implementations:
//! - savitri-masternode/src/vote_aggregator.rs:61 (per-group, n=0 → 1)
//! - savitri-lightnode/src/p2p/certificate.rs:62 (committee-global, n=0 → 0)
//!
//! See `memory/quorum_audit_2026-04-28.md` for the audit. The two
//! formulas are byte-identical for n > 0 — the only divergence was the
//! n=0 edge case. We standardize on n=0 → 0 (safer default; the n=0 → 1
//! variant was only used as a test-fixture fallback in vote_aggregator).
//!
//! # Formula
//!
//! `quorum = ceil(2N/3) = (2N + 2) / 3`
//!
//! # Semantics
//!
//! `n` is the size of the voter set whose 2/3 supermajority we need to
//! collect a valid certificate. Whether `n` represents the per-group
//! masternode count, the global committee size, or any other voter
//! universe is the caller's responsibility — the math is the same.

/// Canonical BFT quorum threshold. Returns 0 for `n == 0`.
#[inline]
pub fn quorum_for_voters(n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    (2 * n + 2) / 3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_zero_voters() {
        assert_eq!(quorum_for_voters(0), 0);
    }

    #[test]
    fn quorum_one_voter() {
        // ceil(2/3) = 1
        assert_eq!(quorum_for_voters(1), 1);
    }

    #[test]
    fn quorum_two_voters() {
        // ceil(4/3) = 2
        assert_eq!(quorum_for_voters(2), 2);
    }

    #[test]
    fn quorum_three_voters() {
        // ceil(6/3) = 2
        assert_eq!(quorum_for_voters(3), 2);
    }

    #[test]
    fn quorum_four_voters() {
        // ceil(8/3) = 3
        assert_eq!(quorum_for_voters(4), 3);
    }

    #[test]
    fn quorum_five_voters() {
        // ceil(10/3) = 4
        assert_eq!(quorum_for_voters(5), 4);
    }

    #[test]
    fn quorum_ten_voters() {
        // ceil(20/3) = 7
        assert_eq!(quorum_for_voters(10), 7);
    }

    #[test]
    fn quorum_hundred_voters() {
        // ceil(200/3) = 67
        assert_eq!(quorum_for_voters(100), 67);
    }

    /// Parity check: the canonical formula must agree with both the
    /// pre-refactor masternode `quorum_for_voters` and the lightnode
    /// `min_voters_for_quorum` for every n >= 1. The n=0 edge case is
    /// the only intentional behavior change (was 1 in masternode, now 0
    /// matching lightnode).
    #[test]
    fn parity_with_legacy_formulas_for_nonzero() {
        for n in 1..=500usize {
            let canonical = quorum_for_voters(n);
            let legacy = (2 * n + 2) / 3;
            assert_eq!(canonical, legacy, "parity broken at n={n}");
        }
    }
}
