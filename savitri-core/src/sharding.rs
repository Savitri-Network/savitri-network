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
