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
