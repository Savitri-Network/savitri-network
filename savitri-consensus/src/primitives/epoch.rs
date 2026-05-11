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
