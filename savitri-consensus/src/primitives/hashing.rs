//! Canonical block / proposal hashing — single source of truth.
//!
//! Replaces 4 divergent implementations of `compute_block_hash` (and a
//! separate cert-shaped variant) scattered across the workspace. See
//! `memory/block_hash_audit_2026-04-28.md` for the full audit.
//!
//! # Two distinct primitives
//!
//! 1. [`compute_block_hash`] — block IDENTITY hash. Used everywhere a
//!    block is referenced (storage key, parent_hash chain, certificate
//!    target). SHA-256 over `parent ‖ state_root ‖ tx_root ‖ height_le`,
//!    zero-padded to 64 bytes for compatibility with the rest of the
//!    consensus types (which already use [u8; 64]).
//!
//! 2. [`compute_signed_proposal_hash`] — BFT signed-proposal hash. Used
//!    by the masternode to verify the lightnode's proposal signature.
//!    SHA-512 over a 9-field BFT-specific layout that includes
//!    `round_id`, `proposer_pubkey`, `tx_count`, etc. Distinct from the
//!    block identity hash by design — different inputs, different
//!    output, different purpose.
//!
//! # Rationale for the dual API
//!
//! Pre-refactor, `savitri-masternode/src/libp2p_network.rs:5056`
//! (`compute_compat_block_hash`) collapsed both concepts into one
//! "block hash" function with the BFT layout. Two of the other three
//! implementations used a simpler identity layout (SHA-256, no
//! round_id, no proposer_pubkey, no timestamp). The conflation made
//! cert-vs-block hash mismatches a recurring source of bugs (#48, #49,
//! #50). Splitting into two clearly-named primitives prevents that.

use sha2::{Digest, Sha256, Sha512};

/// Compute the canonical block identity hash.
///
/// Formula: `SHA-256(parent_hash ‖ state_root_pad64 ‖ tx_root_pad64 ‖ height_le)`,
/// zero-padded to 64 bytes.
///
/// `state_root` and `tx_root` are 32-byte SHA-256 hashes themselves;
/// they are zero-padded to 64 bytes before being fed into the outer
/// hash to match the legacy lightnode/masternode layout.
///
/// This identity must remain stable across the workspace — it appears
/// in storage keys, parent_hash chains, and BFT certificate targets.
/// Changing the formula requires a coordinated testnet wipe.
pub fn compute_block_hash(
    parent_hash: &[u8; 64],
    state_root: &[u8; 32],
    tx_root: &[u8; 32],
    height: u64,
) -> [u8; 64] {
    let mut state_root_pad = [0u8; 64];
    state_root_pad[..32].copy_from_slice(state_root);
    let mut tx_root_pad = [0u8; 64];
    tx_root_pad[..32].copy_from_slice(tx_root);

    let mut hasher = Sha256::new();
    hasher.update(parent_hash);
    hasher.update(&state_root_pad);
    hasher.update(&tx_root_pad);
    hasher.update(&height.to_le_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 64];
    out[..32].copy_from_slice(digest.as_slice());
    out
}

/// Compute the BFT signed-proposal hash that the masternode verifies
/// against the lightnode's signature.
///
/// Formula: `SHA-512( "savitri-lightnode-proposal-v1"
///                  ‖ round_id_le
///                  ‖ height_le
///                  ‖ timestamp_le
///                  ‖ proposer_pubkey
///                  ‖ parent_hash
///                  ‖ state_root
///                  ‖ tx_root
///                  ‖ tx_count_le )`
///
/// This is intentionally separate from [`compute_block_hash`] because
/// it incorporates round-dependent fields (round_id, timestamp,
/// proposer_pubkey, tx_count) that change between BFT rounds even when
/// the block contents are equivalent. Conflating the two has been the
/// cause of recurring cert-vs-block mismatch bugs.
///
/// `state_root` and `tx_root` are passed as 64-byte values for backward
/// compatibility with the masternode's pre-refactor layout. Only the
/// first 32 bytes contain entropy; the second 32 are zero padding.
pub fn compute_signed_proposal_hash(
    round_id: u64,
    height: u64,
    timestamp: u64,
    proposer_pubkey: &[u8; 32],
    parent_hash: &[u8; 64],
    state_root: &[u8; 64],
    tx_root: &[u8; 64],
    tx_count: u32,
) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"savitri-lightnode-proposal-v1");
    hasher.update(&round_id.to_le_bytes());
    hasher.update(&height.to_le_bytes());
    hasher.update(&timestamp.to_le_bytes());
    hasher.update(proposer_pubkey);
    hasher.update(parent_hash);
    hasher.update(state_root);
    hasher.update(tx_root);
    hasher.update(&tx_count.to_le_bytes());

    let out = hasher.finalize();
    let mut block_hash = [0u8; 64];
    block_hash.copy_from_slice(&out);
    block_hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_hash_genesis_is_pinned() {
        let h = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
        // SHA-256(64+64+64+8 = 200 zero bytes), first 8 bytes hex
        let hex_prefix = hex::encode(&h[..8]);
        assert_eq!(
            hex_prefix, "6d9c54dee5660c46",
            "compute_block_hash genesis pin broken"
        );
        // Output is 32-byte SHA-256 zero-padded to 64 bytes
        assert_eq!(&h[32..], &[0u8; 32], "second half must be zero padding");
    }

    #[test]
    fn block_hash_changes_on_height() {
        let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
        let h1 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 1);
        assert_ne!(h0, h1);
    }

    #[test]
    fn block_hash_changes_on_parent() {
        let mut parent = [0u8; 64];
        parent[0] = 1;
        let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
        let h1 = compute_block_hash(&parent, &[0; 32], &[0; 32], 0);
        assert_ne!(h0, h1);
    }

    #[test]
    fn block_hash_changes_on_state_root() {
        let mut sr = [0u8; 32];
        sr[0] = 1;
        let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
        let h1 = compute_block_hash(&[0; 64], &sr, &[0; 32], 0);
        assert_ne!(h0, h1);
    }

    #[test]
    fn block_hash_changes_on_tx_root() {
        let mut tr = [0u8; 32];
        tr[0] = 1;
        let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
        let h1 = compute_block_hash(&[0; 64], &[0; 32], &tr, 0);
        assert_ne!(h0, h1);
    }

    #[test]
    fn block_hash_deterministic() {
        let h_a = compute_block_hash(&[7; 64], &[3; 32], &[5; 32], 42);
        let h_b = compute_block_hash(&[7; 64], &[3; 32], &[5; 32], 42);
        assert_eq!(h_a, h_b);
    }

    #[test]
    fn signed_proposal_hash_genesis_is_pinned() {
        let h = compute_signed_proposal_hash(0, 0, 0, &[0; 32], &[0; 64], &[0; 64], &[0; 64], 0);
        // SHA-512 of (29 + 8 + 8 + 8 + 32 + 64 + 64 + 64 + 4 = 281 bytes)
        let hex_prefix = hex::encode(&h[..8]);
        assert_eq!(
            hex_prefix, "429658957538869b",
            "compute_signed_proposal_hash genesis pin broken"
        );
    }

    #[test]
    fn signed_proposal_hash_changes_on_round_id() {
        let h0 = compute_signed_proposal_hash(0, 0, 0, &[0; 32], &[0; 64], &[0; 64], &[0; 64], 0);
        let h1 = compute_signed_proposal_hash(1, 0, 0, &[0; 32], &[0; 64], &[0; 64], &[0; 64], 0);
        assert_ne!(h0, h1);
    }

    #[test]
    fn signed_proposal_hash_uses_full_64_bytes() {
        // SHA-512 fills all 64 bytes (vs compute_block_hash which pads).
        let h = compute_signed_proposal_hash(1, 1, 1, &[1; 32], &[1; 64], &[1; 64], &[1; 64], 1);
        assert!(
            h[32..].iter().any(|&b| b != 0),
            "SHA-512 must use second half"
        );
    }

    #[test]
    fn block_hash_matches_legacy_lightnode_formula() {
        // Reproduce savitri-lightnode/src/p2p/block.rs:1783 inline:
        let parent = [42u8; 64];
        let state_root = [7u8; 32];
        let tx_root = [13u8; 32];
        let height = 99u64;

        let canonical = compute_block_hash(&parent, &state_root, &tx_root, height);

        let legacy = {
            let mut state_root_64 = [0u8; 64];
            state_root_64[..32].copy_from_slice(&state_root);
            let mut tx_root_64 = [0u8; 64];
            tx_root_64[..32].copy_from_slice(&tx_root);
            let mut hasher = Sha256::new();
            hasher.update(&parent);
            hasher.update(&state_root_64);
            hasher.update(&tx_root_64);
            hasher.update(&height.to_le_bytes());
            let digest = hasher.finalize();
            let mut hash = [0u8; 64];
            hash[..32].copy_from_slice(digest.as_slice());
            hash
        };

        assert_eq!(
            canonical, legacy,
            "canonical must match legacy lightnode formula byte-for-byte"
        );
    }
}
