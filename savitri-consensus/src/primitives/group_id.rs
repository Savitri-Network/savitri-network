//! Canonical group_id parsing — single source of truth.
//!
//! Group IDs come in two shapes (see `savitri-masternode/group_formation.rs`):
//!   - `"group_{epoch}_{index}_{epoch}"` under the normal multi-group path
//!   - `"group_singleton_{index}"`        under `SAVITRI_FORCE_SINGLE_GROUP=1`
//!
//! The middle token is the **stable** group index. The epoch resets on MN
//! re-formation but the group index is reused, and lightnodes may continue
//! to propose with a `group_id` carrying the old epoch long after the
//! masternodes have moved on. Matching by index instead of full string
//! survive epoch drift.
//!
//! in `vote_aggregator.rs` only. Promoted here so the cert handler in
//! `libp2p_network.rs` (which previously did `group_id.starts_with(...)`
//! prefix matches that REQUIRED the same epoch on both sides) can reuse
//! it. Without this lift, certs from epoch X were "processed anyway" by
//! masternodes at epoch Y but with `group_members = []`, which made
//! attestation verification trivially pass with `required_majority = 1`
//! and silently whitelisted the wrong proposer.
//!
//! See `memory/architectural_debt.md` Tier 1 for the full duplication map.

/// Extract the epoch-stable group index from a `group_id` string.
///
/// Returns `None` for malformed / unknown shapes (defensive default —
/// callers should treat `None` as "match by exact string only").
#[inline]
pub fn group_index_from_id(group_id: &str) -> Option<usize> {
    // Expect exactly: group, <epoch>, <index>, <epoch>  — or  group, singleton, <index>.
    let mut parts = group_id.split('_');
    if parts.next()? != "group" {
        return None;
    }
    let second = parts.next()?;
    let third = parts.next()?;
    if second == "singleton" {
        return third.parse::<usize>().ok();
    }
    third.parse::<usize>().ok()
}
