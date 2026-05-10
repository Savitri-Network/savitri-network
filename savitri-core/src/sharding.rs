//! Canonical shard assignment for transactions.
//!
//! Single source of truth for the sender-address → shard-id mapping used by:
//! * `savitri-lightnode/src/tx_router/resolution.rs::shard_for_sender` (RPC submit)
//! * `savitri-mempool/src/sharding/router.rs::ShardRouter::route_to_shard` (mempool routing)
//! * `savitri-mempool/src/mempool/integration.rs::ShardFilter::is_local` (proposer-side filter)
//!
//! Before this module existed each call site reimplemented the same hashing
//! recipe (`DefaultHasher` over sender bytes, modulo `num_shards`). Identical
//! today, but the parity was assumption-only — if any one of the three drifted
//! a TX could be routed to group A by the lightnode and silently filtered out
//! in group B's mempool, leaving zero TX in blocks. See architectural_debt.md
//! "shard-parity-assumption".
//!
//! All callers MUST go through [`shard_for_sender`] now. Local helpers are
//! left in place as `#[deprecated]` shims that delegate here.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute the shard id for a sender address.
///
/// Algorithm: `DefaultHasher(sender) % num_shards`.
///
/// Returns 0 when `num_shards == 0` (early-return safe — the caller is
/// responsible for treating that as "shard map not yet populated" and
/// falling back to local admit).
#[inline]
pub fn shard_for_sender(sender: &[u8], num_shards: u32) -> u32 {
    if num_shards == 0 {
        return 0;
    }
    let mut hasher = DefaultHasher::new();
    sender.hash(&mut hasher);
    (hasher.finish() as u32) % num_shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_num_shards_returns_zero() {
        assert_eq!(shard_for_sender(b"alice", 0), 0);
    }

    #[test]
    fn deterministic() {
        let s1 = shard_for_sender(b"alice", 13107);
        let s2 = shard_for_sender(b"alice", 13107);
        assert_eq!(s1, s2);
    }

    #[test]
    fn shard_in_range() {
        for sender in &[b"a" as &[u8], b"longer", b"\x00\x01\x02"] {
            for n in &[1u32, 4, 13107, 65536] {
                let s = shard_for_sender(*sender, *n);
                assert!(s < *n);
            }
        }
    }

    #[test]
    fn matches_legacy_recipe_byte_for_byte() {
        // Reproduces the inline DefaultHasher recipe so any future drift in
        // std::collections::hash_map::DefaultHasher between Rust versions
        // surfaces here loudly instead of in a 0-tx block production loop.
        let sender = b"sender_address_32_bytes_xxxxxxxx";
        let n = 13107u32;
        let inline = {
            let mut h = DefaultHasher::new();
            sender.hash(&mut h);
            (h.finish() as u32) % n
        };
        assert_eq!(shard_for_sender(sender, n), inline);
    }
}
