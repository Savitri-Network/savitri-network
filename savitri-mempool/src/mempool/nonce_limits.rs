//! Centralized nonce-gap and nonce-window limits.
//!
//! separate files and were required to satisfy a tight ordering invariant
//! (see below). Drift between them caused two previous incidents per
//! `architectural_debt.md`. They now live here, with `const_assert!` checks
//! that fail at compile time if anyone perturbs the ordering.
//!
//! Invariants (top-down):
//!
//! ```text
//!   PREVALIDATION_NONCE_WINDOW
//!     >= QUEUED_POOL_MAX_NONCE_GAP
//!     >= ADMISSION_MAX_MAIN_POOL_NONCE_GAP
//!     >  TX_GENERATOR_MAX_NONCE_GAP   (loadtest constant; not enforced here)
//!   ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC
//!     >= ADMISSION_MAX_MAIN_POOL_NONCE_GAP
//! ```
//!
//!   TX with `nonce > snapshot_nonce + window`. Must be the largest because
//!   admission and queued_pool layers sit downstream.
//! - `QUEUED_POOL_MAX_NONCE_GAP` — what the queued_pool itself accepts to
//! - `ADMISSION_MAX_MAIN_POOL_NONCE_GAP` — gossip / untrusted path: nonces
//!   within this gap of `account.nonce` go directly to the main pool;
//!   beyond it land in the queued_pool. Tighter than queued_pool's gap so
//!   the cliff between "main" and "queued" is well-defined.
//! - `ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC` — trusted RPC path
//!   (from_rpc=true). Widened to 100K (audit §2.1) so a stalled consensus
//!   doesn't trap RPC-accepted TX in queued_pool waiting for an
//!   account.nonce advance that never comes.

/// per-sender storage snapshot.
pub const PREVALIDATION_NONCE_WINDOW: u64 = 3500;

/// Queued pool accepts up to this gap of `account.nonce` for parking.
pub const QUEUED_POOL_MAX_NONCE_GAP: u64 = 3000;

/// Admission gossip / untrusted path: direct-to-main if within gap, else
/// queued. 3000 with 512 headroom over the loadtest's MAX_NONCE_GAP.
pub const ADMISSION_MAX_MAIN_POOL_NONCE_GAP: u64 = 3000;

/// Admission trusted RPC path. Widened to 100K (audit §2.1) so RPC-accepted
/// TX from trusted clients aren't trapped in queued_pool when consensus
/// stalls and `account.nonce` stays pinned at the last committed value.
pub const ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC: u64 = 100_000;

// Compile-time invariant checks. If anyone perturbs the ordering above this
// translation unit fails to compile with a pointer to this file.
const _: () = {
    assert!(
        PREVALIDATION_NONCE_WINDOW >= QUEUED_POOL_MAX_NONCE_GAP,
        "PREVALIDATION_NONCE_WINDOW must be >= QUEUED_POOL_MAX_NONCE_GAP"
    );
    assert!(
        QUEUED_POOL_MAX_NONCE_GAP >= ADMISSION_MAX_MAIN_POOL_NONCE_GAP,
        "QUEUED_POOL_MAX_NONCE_GAP must be >= ADMISSION_MAX_MAIN_POOL_NONCE_GAP"
    );
    assert!(
        ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC >= ADMISSION_MAX_MAIN_POOL_NONCE_GAP,
        "RPC gap must be >= gossip gap (RPC is the trusted, wider path)"
    );
};
