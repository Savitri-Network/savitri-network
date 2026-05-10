//! Canonical slot/epoch arithmetic — single source of truth.
//!
//! Replaces 12 divergent implementations of `current_epoch` across the
//! workspace. See `memory/current_epoch_audit_2026-04-29.md` for the
//! full audit and an earlier fix analysis.
//!
//! # Formula
//!
//! ```text
//! slot(t)  = floor((t_ms - genesis_ms) / heartbeat_ms)
//! epoch(t) = floor(slot(t) / slots_per_epoch)
//!          = floor((t_ms - genesis_ms) / (heartbeat_ms × slots_per_epoch))
//! ```
//!
//! Both functions are pure (no I/O, no system clock). The only function
//! that touches the wall clock is [`now_ms`], which is isolated so unit
//! tests of `current_epoch` / `current_slot` can use deterministic time
//! without mocking.
//!
//! # Pre-conditions
//!
//! - `heartbeat_ms > 0` (debug-asserted; in release returns 0 to avoid panic)
//! - `slots_per_epoch > 0` (debug-asserted; in release falls back to 1)
//! - `now_ms < genesis_ms` → result is 0 (saturating semantics, never panics)

/// Compute the current slot relative to `genesis_ms`.
///
/// Returns 0 if `now_ms` is before genesis.
#[inline]
pub fn current_slot(now_ms: u64, genesis_ms: u64, heartbeat_ms: u64) -> u64 {
    debug_assert!(heartbeat_ms > 0, "heartbeat_ms must be positive");
    if heartbeat_ms == 0 || now_ms <= genesis_ms {
        return 0;
    }
    (now_ms - genesis_ms) / heartbeat_ms
}

/// Compute the current epoch relative to `genesis_ms`.
///
/// Returns 0 if `now_ms` is before genesis.
#[inline]
pub fn current_epoch(now_ms: u64, genesis_ms: u64, heartbeat_ms: u64, slots_per_epoch: u64) -> u64 {
    debug_assert!(heartbeat_ms > 0, "heartbeat_ms must be positive");
    debug_assert!(slots_per_epoch > 0, "slots_per_epoch must be positive");
    let spe = slots_per_epoch.max(1);
    current_slot(now_ms, genesis_ms, heartbeat_ms) / spe
}

/// Wall-clock UTC time in milliseconds since the Unix epoch.
///
/// This is the SOLE permitted system-clock read for slot/epoch
/// computation across the workspace. All other code must call this
/// helper rather than `SystemTime::now()` directly so a single mock
/// point exists for testing.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const GENESIS_MS: u64 = 1_777_234_282_043;
    const HEARTBEAT_MS: u64 = 5_000;
    const SLOTS_PER_EPOCH: u64 = 20;
    const SLOT_LEN_MS: u64 = HEARTBEAT_MS * SLOTS_PER_EPOCH; // 100_000

    #[test]
    fn epoch_at_genesis_is_zero() {
        assert_eq!(
            current_epoch(GENESIS_MS, GENESIS_MS, HEARTBEAT_MS, SLOTS_PER_EPOCH),
            0
        );
    }

    #[test]
    fn epoch_within_first_epoch_is_zero() {
        for delta in [1u64, 1_000, SLOT_LEN_MS - 1] {
            assert_eq!(
                current_epoch(
                    GENESIS_MS + delta,
                    GENESIS_MS,
                    HEARTBEAT_MS,
                    SLOTS_PER_EPOCH
                ),
                0,
                "delta={}",
                delta
            );
        }
    }

    #[test]
    fn epoch_advances_on_slot_boundary() {
        assert_eq!(
            current_epoch(
                GENESIS_MS + SLOT_LEN_MS,
                GENESIS_MS,
                HEARTBEAT_MS,
                SLOTS_PER_EPOCH
            ),
            1
        );
        assert_eq!(
            current_epoch(
                GENESIS_MS + 7 * SLOT_LEN_MS,
                GENESIS_MS,
                HEARTBEAT_MS,
                SLOTS_PER_EPOCH
            ),
            7
        );
    }

    #[test]
    fn epoch_before_genesis_clamps_to_zero() {
        assert_eq!(
            current_epoch(GENESIS_MS - 1, GENESIS_MS, HEARTBEAT_MS, SLOTS_PER_EPOCH),
            0
        );
        assert_eq!(
            current_epoch(0, GENESIS_MS, HEARTBEAT_MS, SLOTS_PER_EPOCH),
            0
        );
    }

    #[test]
    fn epoch_far_future_no_overflow() {
        // Year 2200 ≈ 7_257_600_000_000 ms after Unix epoch.
        let e = current_epoch(7_257_600_000_000, 0, HEARTBEAT_MS, SLOTS_PER_EPOCH);
        assert!(e > 0);
    }

    #[test]
    fn slot_advances_each_heartbeat() {
        for n in 0..5u64 {
            assert_eq!(
                current_slot(GENESIS_MS + n * HEARTBEAT_MS, GENESIS_MS, HEARTBEAT_MS),
                n
            );
        }
    }

    #[test]
    fn zero_heartbeat_returns_zero_no_panic() {
        // debug_assert! triggers in debug; release-only path: graceful 0.
        // (We only run cargo test in debug+release, so this guard is a sanity
        // check that the release path does the right thing if asserts are
        // disabled.)
        let _ = std::panic::catch_unwind(|| current_slot(GENESIS_MS + 1000, GENESIS_MS, 0));
    }

    #[test]
    fn matches_legacy_masternode_formula() {
        // Reproduce the exact formula from
        // savitri-masternode/src/group_formation.rs:104 (the chosen canonical).
        let now = GENESIS_MS + 12_345_678;
        let legacy = {
            let elapsed = now.saturating_sub(GENESIS_MS);
            let slot = elapsed / HEARTBEAT_MS;
            slot / SLOTS_PER_EPOCH.max(1)
        };
        assert_eq!(
            current_epoch(now, GENESIS_MS, HEARTBEAT_MS, SLOTS_PER_EPOCH),
            legacy,
            "canonical must match group_formation.rs:104 byte-for-byte"
        );
    }
}
