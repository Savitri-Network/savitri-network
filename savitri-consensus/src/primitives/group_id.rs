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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_normal_multigroup_id() {
        assert_eq!(group_index_from_id("group_3_0_3"), Some(0));
        assert_eq!(group_index_from_id("group_3_1_3"), Some(1));
        assert_eq!(group_index_from_id("group_8_2_8"), Some(2));
    }

    #[test]
    fn parses_singleton_id() {
        assert_eq!(group_index_from_id("group_singleton_0"), Some(0));
        assert_eq!(group_index_from_id("group_singleton_5"), Some(5));
    }

    #[test]
    fn epoch_drift_yields_same_index() {
        // Cert from old epoch and active_group at new epoch resolve to the
        let cert_old = group_index_from_id("group_3_0_3");
        let active_new = group_index_from_id("group_8_0_8");
        assert_eq!(cert_old, active_new);
    }

    #[test]
    fn rejects_malformed_inputs() {
        assert_eq!(group_index_from_id(""), None);
        assert_eq!(group_index_from_id("group"), None);
        assert_eq!(group_index_from_id("group_3"), None);
        assert_eq!(group_index_from_id("not_a_group_0_0_0"), None);
        assert_eq!(group_index_from_id("group_a_b_c"), None);
    }
}
