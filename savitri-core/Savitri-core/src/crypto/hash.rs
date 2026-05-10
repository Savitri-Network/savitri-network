//! Cryptographic Hashing utilities for Savitri Network
//! 
//! This module provides various hash functions and utilities used throughout
//! the Savitri ecosystem, including SHA-256, SHA-512, BLAKE3, and Merkle trees.

use sha2::{Digest, Sha256, Sha512};
use blake3::Hasher as Blake3Hasher;
use rand::RngCore;

/// Compute SHA-256 hash of data
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute SHA-512 hash of data
pub fn sha512(data: &[u8]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute BLAKE3 hash of data
pub fn blake3(data: &[u8]) -> [u8; 32] {
    let mut hasher = Blake3Hasher::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Generic hash function using SHA-256 (default)
pub fn hash(data: &[u8]) -> [u8; 32] {
    sha256(data)
}

/// Compute hash with domain separation
pub fn hash_with_domain(domain: &str, data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute hash with domain separation using SHA-512
pub fn hash_with_domain_512(domain: &str, data: &[u8]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(domain.as_bytes());
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute Merkle root from list of hashes
pub fn merkle_root(hashes: &[[u8; 32]]) -> [u8; 32] {
    if hashes.is_empty() {
        return [0u8; 32];
    }
    
    if hashes.len() == 1 {
        return hashes[0];
    }
    
    let mut level = hashes.to_vec();
    
    while level.len() > 1 {
        let mut next_level = Vec::new();
        
        for chunk in level.chunks(2) {
            if chunk.len() == 2 {
                let combined = [chunk[0], chunk[1]].concat();
                next_level.push(sha256(&combined));
            } else {
                // Odd number of elements, duplicate the last one
                let combined = [chunk[0], chunk[0]].concat();
                next_level.push(sha256(&combined));
            }
        }
        
        level = next_level;
    }
    
    level[0]
}

/// Compute Merkle root from list of data items
pub fn merkle_root_from_data(items: &[&[u8]]) -> [u8; 32] {
    let hashes: Vec<[u8; 32]> = items.iter().map(|item| sha256(item)).collect();
    merkle_root(&hashes)
}

/// Generate a random hash (for testing purposes)
pub fn random_hash() -> [u8; 32] {
    let mut rng = rand::rngs::OsRng;
    let mut hash = [0u8; 32];
    rng.fill_bytes(&mut hash);
    hash
}

/// Generate a random 64-byte hash (for testing purposes)
pub fn random_hash_64() -> [u8; 64] {
    let mut rng = rand::rngs::OsRng;
    let mut hash = [0u8; 64];
    rng.fill_bytes(&mut hash);
    hash
}

/// Verify that data matches a given hash
pub fn verify_hash(data: &[u8], expected_hash: &[u8; 32]) -> bool {
    let computed = sha256(data);
    &computed == expected_hash
}

/// Verify that data matches a given SHA-512 hash
pub fn verify_hash_512(data: &[u8], expected_hash: &[u8; 64]) -> bool {
    let computed = sha512(data);
    &computed == expected_hash
}

/// Compute hash of concatenated data efficiently
pub fn hash_concat(data1: &[u8], data2: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data1);
    hasher.update(data2);
    hasher.finalize().into()
}

/// Compute double hash (hash of hash) for additional security
pub fn double_hash(data: &[u8]) -> [u8; 32] {
    let first = sha256(data);
    sha256(&first)
}

/// Hash a string to a 64-bit integer
pub fn hash_to_u64(data: &str) -> u64 {
    let hash = sha256(data.as_bytes());
    u64::from_le_bytes([
        hash[0], hash[1], hash[2], hash[3],
        hash[4], hash[5], hash[6], hash[7],
    ])
}

/// Hash a string to a 128-bit integer
pub fn hash_to_u128(data: &str) -> u128 {
    let hash = sha256(data.as_bytes());
    u128::from_le_bytes([
        hash[0], hash[1], hash[2], hash[3],
        hash[4], hash[5], hash[6], hash[7],
        hash[8], hash[9], hash[10], hash[11],
        hash[12], hash[13], hash[14], hash[15],
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_hashing() {
        let data = b"Hello, Savitri!";
        
        let sha256_hash = sha256(data);
        let sha512_hash = sha512(data);
        let blake3_hash = blake3(data);
        let default_hash = hash(data);
        
        // All should be different lengths where appropriate
        assert_eq!(sha256_hash.len(), 32);
        assert_eq!(sha512_hash.len(), 64);
        assert_eq!(blake3_hash.len(), 32);
        assert_eq!(default_hash.len(), 32);
        
        // Default should equal SHA-256
        assert_eq!(default_hash, sha256_hash);
        
        // SHA-256 and BLAKE3 should produce different results
        assert_ne!(sha256_hash, blake3_hash);
    }

    #[test]
    fn test_domain_separation() {
        let data = b"test data";
        
        let hash1 = hash_with_domain("DOMAIN1", data);
        let hash2 = hash_with_domain("DOMAIN2", data);
        let hash3 = hash_with_domain_512("DOMAIN1", data);
        
        // Different domains should produce different hashes
        assert_ne!(hash1, hash2);
        
        // Different hash functions should produce different results
        assert_ne!(hash1[..], hash3[..]);
    }

    #[test]
    fn test_merkle_root() {
        let data1 = b"data1";
        let data2 = b"data2";
        let data3 = b"data3";
        let data4 = b"data4";
        
        let hashes = vec![
            sha256(data1),
            sha256(data2),
            sha256(data3),
            sha256(data4),
        ];
        
        let root = merkle_root(&hashes);
        
        // Merkle root should be deterministic
        let root2 = merkle_root(&hashes);
        assert_eq!(root, root2);
        
        // Different order should produce different root
        let mut reversed_hashes = hashes.clone();
        reversed_hashes.reverse();
        let reversed_root = merkle_root(&reversed_hashes);
        assert_ne!(root, reversed_root);
    }

    #[test]
    fn test_merkle_root_odd_number() {
        let data1 = b"data1";
        let data2 = b"data2";
        let data3 = b"data3";
        
        let hashes = vec![
            sha256(data1),
            sha256(data2),
            sha256(data3),
        ];
        
        let root = merkle_root(&hashes);
        
        // Should handle odd number of elements
        assert_ne!(root, [0u8; 32]);
        
        // Should be deterministic
        let root2 = merkle_root(&hashes);
        assert_eq!(root, root2);
    }

    #[test]
    fn test_merkle_root_empty() {
        let hashes: Vec<[u8; 32]> = vec![];
        let root = merkle_root(&hashes);
        assert_eq!(root, [0u8; 32]);
    }

    #[test]
    fn test_merkle_root_single() {
        let data = b"single item";
        let hash = sha256(data);
        let root = merkle_root(&[hash]);
        assert_eq!(root, hash);
    }

    #[test]
    fn test_hash_verification() {
        let data = b"test data";
        let hash = sha256(data);
        
        assert!(verify_hash(data, &hash));
        
        let wrong_data = b"wrong data";
        assert!(!verify_hash(wrong_data, &hash));
    }

    #[test]
    fn test_hash_concat() {
        let data1 = b"hello";
        let data2 = b"world";
        
        let concat_hash = hash_concat(data1, data2);
        let mut combined = Vec::new();
        combined.extend_from_slice(data1);
        combined.extend_from_slice(data2);
        let manual_hash = sha256(&combined);
        
        assert_eq!(concat_hash, manual_hash);
    }

    #[test]
    fn test_double_hash() {
        let data = b"test data";
        
        let single = sha256(data);
        let double = double_hash(data);
        let manual_double = sha256(&single);
        
        assert_eq!(double, manual_double);
        assert_ne!(single, double);
    }

    #[test]
    fn test_hash_to_integers() {
        let data = "test string";
        
        let hash_u64 = hash_to_u64(data);
        let hash_u128 = hash_to_u128(data);
        
        // Should be deterministic
        let hash_u64_2 = hash_to_u64(data);
        let hash_u128_2 = hash_to_u128(data);
        
        assert_eq!(hash_u64, hash_u64_2);
        assert_eq!(hash_u128, hash_u128_2);
        
        // Different strings should produce different hashes
        let different_data = "different string";
        assert_ne!(hash_u64, hash_to_u64(different_data));
        assert_ne!(hash_u128, hash_to_u128(different_data));
    }

    #[test]
    fn test_random_hash() {
        let hash1 = random_hash();
        let hash2 = random_hash();
        
        // Should be different
        assert_ne!(hash1, hash2);
        
        // Should be correct length
        assert_eq!(hash1.len(), 32);
        assert_eq!(hash2.len(), 32);
        
        // Should not be all zeros
        assert_ne!(hash1, [0u8; 32]);
        assert_ne!(hash2, [0u8; 32]);
    }

    #[test]
    fn test_random_hash_64() {
        let hash1 = random_hash_64();
        let hash2 = random_hash_64();
        
        // Should be different
        assert_ne!(hash1, hash2);
        
        // Should be correct length
        assert_eq!(hash1.len(), 64);
        assert_eq!(hash2.len(), 64);
        
        // Should not be all zeros
        assert_ne!(hash1, [0u8; 64]);
        assert_ne!(hash2, [0u8; 64]);
    }

    #[test]
    fn test_deterministic_behavior() {
        let data = b"deterministic test";
        
        // Multiple calls should produce same result
        let hash1 = sha256(data);
        let hash2 = sha256(data);
        let hash3 = sha256(data);
        
        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
    }
}
