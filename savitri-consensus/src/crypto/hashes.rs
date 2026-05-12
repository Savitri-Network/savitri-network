//! Hash functions for consensus operations
//!
//! This module provides cryptographic hash functions for computing block hashes,
//! transaction hashes, and Merkle tree nodes.

use blake2::{Blake2b512, Blake2s256};
use sha2::{Digest, Sha256, Sha512};

/// Hash error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum HashError {
    #[error("Invalid input length: expected {expected}, got {actual}")]
    InvalidInputLength { expected: usize, actual: usize },
    #[error("Hash computation failed: {0}")]
    ComputationFailed(String),
}

/// Result type for hash operations
pub type HashResult<T> = std::result::Result<T, HashError>;

/// Compute SHA-256 hash (32 bytes)
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}

/// Compute SHA-512 hash (64 bytes)
pub fn sha512(data: &[u8]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut output = [0u8; 64];
    output.copy_from_slice(&result);
    output
}

/// Compute Blake2b-512 hash (64 bytes)
pub fn blake2b512(data: &[u8]) -> [u8; 64] {
    let mut hasher = Blake2b512::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut output = [0u8; 64];
    output.copy_from_slice(&result);
    output
}

/// Compute Blake2s-256 hash (32 bytes)
pub fn blake2s256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Blake2s256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}

/// Compute double SHA-256 hash
pub fn double_sha256(data: &[u8]) -> [u8; 32] {
    sha256(&sha256(data))
}

// the canonical primitive in `savitri_consensus::primitives::hashing::compute_block_hash`.
// The old 5-parameter SHA-512 signature was unreachable from production
// code (verified via workspace-wide grep). The new canonical takes
// `(parent, state_root, tx_root, height)` as primitives and uses
// SHA-256 zero-padded to 64 bytes, matching the lightnode + masternode
// formula. For BFT-signed proposals (round-dependent), use
// `savitri_consensus::primitives::hashing::compute_signed_proposal_hash`.

/// Compute transaction hash (SHA-256)
pub fn compute_tx_hash(
    from: &[u8; 32],
    to: &[u8; 32],
    amount: u64,
    nonce: u64,
    data: &[u8],
) -> [u8; 32] {
    let mut input = Vec::new();
    input.extend_from_slice(from);
    input.extend_from_slice(to);
    input.extend_from_slice(&amount.to_le_bytes());
    input.extend_from_slice(&nonce.to_le_bytes());
    input.extend_from_slice(data);
    sha256(&input)
}

/// Hash two nodes together for Merkle tree
pub fn hash_nodes(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined = Vec::with_capacity(64);
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    sha256(&combined)
}

/// Hash a leaf node for Merkle tree
pub fn hash_leaf(data: &[u8]) -> [u8; 32] {
    let mut prefixed = Vec::with_capacity(data.len() + 1);
    prefixed.push(0x00); // Leaf prefix
    prefixed.extend_from_slice(data);
    sha256(&prefixed)
}

/// Hash an internal node for Merkle tree
pub fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut combined = Vec::with_capacity(65);
    combined.push(0x01); // Internal node prefix
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    sha256(&combined)
}

/// Compute state root hash
pub fn compute_state_root(state_entries: &[(&[u8], &[u8])]) -> [u8; 32] {
    if state_entries.is_empty() {
        return [0u8; 32];
    }

    let mut hasher = Sha256::new();
    for (key, value) in state_entries {
        hasher.update(key);
        hasher.update(value);
    }
    let result = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}

/// Hash concatenation utility
pub fn hash_concat(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    let result = hasher.finalize();
    let mut output = [0u8; 32];
    output.copy_from_slice(&result);
    output
}

/// Convert hash to hex string
pub fn hash_to_hex(hash: &[u8]) -> String {
    hex::encode(hash)
}

/// Convert hex string to hash bytes
pub fn hex_to_hash_32(hex_str: &str) -> HashResult<[u8; 32]> {
    let bytes = hex::decode(hex_str).map_err(|e| HashError::ComputationFailed(e.to_string()))?;
    if bytes.len() != 32 {
        return Err(HashError::InvalidInputLength {
            expected: 32,
            actual: bytes.len(),
        });
    }
    let mut output = [0u8; 32];
    output.copy_from_slice(&bytes);
    Ok(output)
}

/// Convert hex string to 64-byte hash
pub fn hex_to_hash_64(hex_str: &str) -> HashResult<[u8; 64]> {
    let bytes = hex::decode(hex_str).map_err(|e| HashError::ComputationFailed(e.to_string()))?;
    if bytes.len() != 64 {
        return Err(HashError::InvalidInputLength {
            expected: 64,
            actual: bytes.len(),
        });
    }
    let mut output = [0u8; 64];
    output.copy_from_slice(&bytes);
    Ok(output)
}
