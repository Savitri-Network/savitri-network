//! TX sender + shard resolution helpers, extracted from the monolithic
//! `tx_router.rs` as part of the Tier 4 Fase 2 refactor.
//!
//! The logic MUST match `savitri-mempool::ShardFilter::is_local` bit-for-bit;
//! otherwise forwarded routing and cross-group drain diverge and TX end up
//! "in flight" without ever being admitted. See
//! memory/tx_router_audit_2026-04-28.md.

/// Compute the shard id for a sender address.
///
/// The canonical implementation lives there so this module, the mempool
/// `ShardFilter::is_local`, and the mempool `ShardRouter::route_to_shard` all
/// agree on the same recipe and cannot drift apart silently.
#[inline]
pub fn shard_for_sender(sender: &[u8], num_shards: u32) -> u32 {
    savitri_core::sharding::shard_for_sender(sender, num_shards)
}

/// Extract sender bytes (decoded from hex) and tx_hash from raw TX bytes.
///
/// Returns `None` when deserialization fails, `from` is empty, or hex decode
/// fails. In those cases the caller emits `FallbackLocal` so that local admit
/// can return a precise error to the RPC client.
pub fn extract_sender_and_hash(raw_bytes: &[u8]) -> Option<(Vec<u8>, [u8; 32])> {
    let tx = crate::tx::deserialize_signed_tx(raw_bytes).ok()?;
    if tx.from.is_empty() {
        return None;
    }
    let sender_bytes = hex::decode(tx.from.trim_start_matches("0x")).ok()?;
    if sender_bytes.is_empty() {
        return None;
    }
    let hash = crate::tx::hash_signed_tx_bytes(raw_bytes);
    Some((sender_bytes, hash))
}
