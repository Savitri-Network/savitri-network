//! Merkle Patricia Trie for state root computation.
//!
//! Provides a trie-based state root that supports:
//! - O(log N) inclusion proofs for individual accounts
//! - Deterministic root hash for state verification
//! - Integration with RocksDB for persistent trie nodes
//!
//! The trie key is the account address (32 bytes), and the value is the
//! serialized Account (balance + nonce). The trie root is a 32-byte hash
//! that changes when any account is modified.
//!
//! ## Implementation
//!
//! Uses a simplified binary Merkle trie (not full Ethereum MPT):
//! - Keys are hashed to 256 bits via SHA-256
//! - Interior nodes: H(left_child || right_child)
//! - Leaf nodes: H(key || value)
//! - Empty nodes: [0u8; 32]
//!
//! This is sufficient for testnet V0.1.0. A full Ethereum-compatible MPT
//! (with extension/branch/leaf nodes) can replace this for mainnet.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// A 32-byte hash used for trie nodes and the state root.
pub type TrieHash = [u8; 32];

/// Empty node hash (all zeros).
pub const EMPTY_HASH: TrieHash = [0u8; 32];

/// Compute a Merkle trie root from an ordered set of key-value pairs.
///
/// Keys must be sorted lexicographically. Each key is hashed to a fixed-length
/// path, and the trie is built bottom-up.
///
/// This is a stateless computation (no persistent trie nodes).
/// For incremental updates with persistence, use `StateTrie`.
pub fn compute_merkle_root(entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> TrieHash {
    if entries.is_empty() {
        return EMPTY_HASH;
    }

    // Build leaf hashes
    let leaves: Vec<TrieHash> = entries
        .iter()
        .map(|(key, value)| hash_leaf(key, value))
        .collect();

    // Build tree bottom-up
    merkle_root_from_leaves(&leaves)
}

/// Hash a leaf node: H("LEAF" || key || value)
pub fn hash_leaf(key: &[u8], value: &[u8]) -> TrieHash {
    let mut hasher = Sha256::new();
    hasher.update(b"LEAF");
    hasher.update(key);
    hasher.update(value);
    hasher.finalize().into()
}

/// Hash an interior node: H("NODE" || left || right)
pub fn hash_node(left: &TrieHash, right: &TrieHash) -> TrieHash {
    let mut hasher = Sha256::new();
    hasher.update(b"NODE");
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Build Merkle root from a list of leaf hashes.
/// Uses a balanced binary tree (padding with EMPTY_HASH if odd count).
fn merkle_root_from_leaves(leaves: &[TrieHash]) -> TrieHash {
    if leaves.is_empty() {
        return EMPTY_HASH;
    }
    if leaves.len() == 1 {
        return leaves[0];
    }

    let mut current_level = leaves.to_vec();

    while current_level.len() > 1 {
        let mut next_level = Vec::with_capacity((current_level.len() + 1) / 2);

        for chunk in current_level.chunks(2) {
            let left = &chunk[0];
            let right = if chunk.len() > 1 {
                &chunk[1]
            } else {
                &EMPTY_HASH
            };
            next_level.push(hash_node(left, right));
        }

        current_level = next_level;
    }

    current_level[0]
}

/// Generate a Merkle proof for a specific key.
/// Returns the list of sibling hashes needed to verify inclusion.
pub fn generate_proof(entries: &BTreeMap<Vec<u8>, Vec<u8>>, key: &[u8]) -> Option<MerkleProof> {
    let keys: Vec<&Vec<u8>> = entries.keys().collect();
    let index = keys.iter().position(|k| k.as_slice() == key)?;

    let leaves: Vec<TrieHash> = entries.iter().map(|(k, v)| hash_leaf(k, v)).collect();

    let value = entries.get(key)?.clone();
    let siblings = collect_proof_siblings(&leaves, index);

    Some(MerkleProof {
        key: key.to_vec(),
        value,
        leaf_index: index,
        siblings,
        root: merkle_root_from_leaves(&leaves),
    })
}

/// Verify a Merkle proof.
pub fn verify_proof(proof: &MerkleProof) -> bool {
    let mut hash = hash_leaf(&proof.key, &proof.value);
    let mut index = proof.leaf_index;

    for sibling in &proof.siblings {
        if index % 2 == 0 {
            hash = hash_node(&hash, sibling);
        } else {
            hash = hash_node(sibling, &hash);
        }
        index /= 2;
    }

    hash == proof.root
}

/// Collect sibling hashes for a Merkle proof at a given leaf index.
fn collect_proof_siblings(leaves: &[TrieHash], leaf_index: usize) -> Vec<TrieHash> {
    let mut siblings = Vec::new();
    let mut current_level = leaves.to_vec();
    let mut index = leaf_index;

    while current_level.len() > 1 {
        // Find sibling
        let sibling_index = if index % 2 == 0 { index + 1 } else { index - 1 };
        let sibling = if sibling_index < current_level.len() {
            current_level[sibling_index]
        } else {
            EMPTY_HASH
        };
        siblings.push(sibling);

        // Move up
        let mut next_level = Vec::with_capacity((current_level.len() + 1) / 2);
        for chunk in current_level.chunks(2) {
            let left = &chunk[0];
            let right = if chunk.len() > 1 {
                &chunk[1]
            } else {
                &EMPTY_HASH
            };
            next_level.push(hash_node(left, right));
        }
        current_level = next_level;
        index /= 2;
    }

    siblings
}

/// A Merkle inclusion proof for a single key-value pair.
#[derive(Debug, Clone)]
pub struct MerkleProof {
    /// The key being proved
    pub key: Vec<u8>,
    /// The value at this key
    pub value: Vec<u8>,
    /// Index of the leaf in the sorted entry list
    pub leaf_index: usize,
    /// Sibling hashes from leaf to root
    pub siblings: Vec<TrieHash>,
    /// The expected root hash
    pub root: TrieHash,
}

/// Compute state root from a pre-serialized account overlay.
/// The overlay maps address bytes → serialized account bytes.
/// This is the trie-based replacement for the flat SHA-512 `compute_state_root_from_overlay`.
pub fn compute_state_root_from_overlay_bytes(overlay: &BTreeMap<Vec<u8>, Vec<u8>>) -> TrieHash {
    compute_merkle_root(overlay)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_trie() {
        let entries = BTreeMap::new();
        assert_eq!(compute_merkle_root(&entries), EMPTY_HASH);
    }

    #[test]
    fn test_single_entry() {
        let mut entries = BTreeMap::new();
        entries.insert(b"key1".to_vec(), b"value1".to_vec());
        let root = compute_merkle_root(&entries);
        assert_ne!(root, EMPTY_HASH);
    }

    #[test]
    fn test_deterministic() {
        let mut entries = BTreeMap::new();
        entries.insert(b"alice".to_vec(), b"100".to_vec());
        entries.insert(b"bob".to_vec(), b"200".to_vec());
        let root1 = compute_merkle_root(&entries);
        let root2 = compute_merkle_root(&entries);
        assert_eq!(root1, root2);
    }

    #[test]
    fn test_order_independence() {
        // BTreeMap is always sorted, so insertion order doesn't matter
        let mut entries1 = BTreeMap::new();
        entries1.insert(b"bob".to_vec(), b"200".to_vec());
        entries1.insert(b"alice".to_vec(), b"100".to_vec());

        let mut entries2 = BTreeMap::new();
        entries2.insert(b"alice".to_vec(), b"100".to_vec());
        entries2.insert(b"bob".to_vec(), b"200".to_vec());

        assert_eq!(
            compute_merkle_root(&entries1),
            compute_merkle_root(&entries2)
        );
    }

    #[test]
    fn test_proof_generation_and_verification() {
        let mut entries = BTreeMap::new();
        entries.insert(b"alice".to_vec(), b"100".to_vec());
        entries.insert(b"bob".to_vec(), b"200".to_vec());
        entries.insert(b"charlie".to_vec(), b"300".to_vec());
        entries.insert(b"dave".to_vec(), b"400".to_vec());

        for key in entries.keys() {
            let proof = generate_proof(&entries, key).expect("proof should exist");
            assert!(
                verify_proof(&proof),
                "proof should verify for key {:?}",
                key
            );
        }
    }

    #[test]
    fn test_proof_fails_for_wrong_value() {
        let mut entries = BTreeMap::new();
        entries.insert(b"alice".to_vec(), b"100".to_vec());
        entries.insert(b"bob".to_vec(), b"200".to_vec());

        let mut proof = generate_proof(&entries, b"alice").unwrap();
        proof.value = b"999".to_vec(); // tamper with value
        assert!(!verify_proof(&proof), "tampered proof should fail");
    }
}
