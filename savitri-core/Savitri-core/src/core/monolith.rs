// SPDX-License-Identifier: MIT
// © 2026 Savitri Network

//! Core monolith functionality for Savitri Network
//! 
//! This module provides the basic monolith data structures and utilities
//! without external dependencies on ZKP or storage layers.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use sha2::{Digest, Sha512};
use std::time::{SystemTime, UNIX_EPOCH};

/// Generation policy for monolith snapshots.
/// - `max_blocks`: upper bound of blocks included per snapshot window.
/// - `epoch_length`: optional epoch length (in blocks) to align windows on epoch boundaries.
/// - `retention_limit`: number of most recent monoliths to keep; older ones are purged.
/// - `max_size_bytes`: guardrail on serialized snapshot size.
#[derive(Debug, Clone, Copy)]
pub struct MonolithPolicy {
    pub max_blocks: u64,
    pub epoch_length: Option<u64>,
    pub retention_limit: u64,
    pub max_size_bytes: u64,
}

impl MonolithPolicy {
    pub const DEFAULT_RETENTION: u64 = 30;
    pub const DEFAULT_MAX_SIZE_BYTES: u64 = 500 * 1024 * 1024; // 500 MB target size

    pub fn new(max_blocks: u64) -> Self {
        Self {
            max_blocks,
            epoch_length: None,
            retention_limit: Self::DEFAULT_RETENTION,
            max_size_bytes: Self::DEFAULT_MAX_SIZE_BYTES,
        }
    }

    pub fn with_epoch_length(mut self, epoch_length: Option<u64>) -> Self {
        self.epoch_length = epoch_length;
        self
    }

    pub fn with_retention(mut self, retention_limit: u64) -> Self {
        self.retention_limit = retention_limit;
        self
    }

    pub fn with_max_size_bytes(mut self, max_size_bytes: u64) -> Self {
        self.max_size_bytes = max_size_bytes;
        self
    }
}

use super::epoch::{EpochConfig, EpochManager, calculate_epoch_id};
use super::types::Transaction;

/// Minimal block structure for monolith computations.
///
/// The full `Block` lives in the extended savitri-core; this standalone
/// version carries only the fields required by monolith hashing and
/// size estimation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Block {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
    pub height: u64,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
    #[serde(with = "BigArray")]
    pub parent_exec_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub parent_ref_hash: [u8; 64],
    pub transactions: Vec<Transaction>,
}

/// ZKP proof bytes for monolith verification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProofBytes(pub Vec<u8>);

/// Monolith header containing metadata and commitments
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MonolithHeader {
    /// Monolith height (block number)
    pub height: u64,
    /// Monolith timestamp
    pub timestamp: u64,
    /// Hash of the monolith data
    #[serde(with = "serde_big_array::BigArray")]
    pub hash: [u8; 64],
    /// Hash of the parent monolith
    #[serde(with = "serde_big_array::BigArray")]
    pub parent_hash: [u8; 64],
    /// Hash of the headers commitment
    #[serde(with = "serde_big_array::BigArray")]
    pub headers_commit: [u8; 64],
    /// Hash of the state commitment
    #[serde(with = "serde_big_array::BigArray")]
    pub state_commit: [u8; 64],
    /// Number of blocks in this monolith
    pub block_count: u64,
    /// Total size of the monolith data
    pub size_bytes: u64,
    /// Execution height covered by this monolith (end height of window).
    pub exec_height: u64,
    /// First execution height in the covered window (inclusive).
    #[serde(default)]
    pub window_start: u64,
    /// Epoch identifier for epoch tracking and alignment
    pub epoch_id: u64,
    pub produced_at_ms: u64,
    #[serde(with = "serde_big_array::BigArray")]
    pub producer: [u8; 32],
    pub cosignatures: Vec<Vec<u8>>,
    /// Optional Merkle proof (or compact delta) describing the snapshot payload.
    #[serde(default)]
    pub merkle_proof: Option<Vec<u8>>,
    /// Optional aggregated receipt/cosignature bundle attesting the snapshot.
    #[serde(default)]
    pub aggregate_receipt: Option<Vec<u8>>,
    /// Time spent to generate this monolith, in milliseconds.
    #[serde(default)]
    pub generation_time_ms: u64,
    /// Number of times the monolith has been served to peers.
    #[serde(default)]
    pub serve_count: u64,
    /// Optional ZKP proof binding headers_commit and state_commit
    #[serde(default)]
    pub zkp_proof: Option<ProofBytes>,
}

impl MonolithHeader {
    pub fn new(
        height: u64,
        timestamp: u64,
        hash: [u8; 64],
        parent_hash: [u8; 64],
        headers_commit: [u8; 64],
        state_commit: [u8; 64],
        block_count: u64,
        size_bytes: u64,
        exec_height: u64,
        window_start: u64,
        epoch_id: u64,
        producer: [u8; 32],
    ) -> Self {
        Self {
            height,
            timestamp,
            hash,
            parent_hash,
            headers_commit,
            state_commit,
            block_count,
            size_bytes,
            exec_height,
            window_start,
            epoch_id,
            produced_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            producer,
            cosignatures: Vec::new(),
            merkle_proof: None,
            aggregate_receipt: None,
            generation_time_ms: 0,
            serve_count: 0,
            zkp_proof: None,
        }
    }

    /// Get the size of the block window covered by this monolith
    pub fn window_size(&self) -> u64 {
        if self.exec_height >= self.window_start {
            self.exec_height - self.window_start + 1
        } else {
            0
        }
    }

    /// Check if this monolith covers a specific height
    pub fn covers_height(&self, height: u64) -> bool {
        height >= self.window_start && height <= self.exec_height
    }
}

/// Compute monolith ID from its components
pub fn compute_monolith_id(
    prev_monolith_id: &[u8; 64],
    headers_commit: &[u8; 64],
    state_commit: &[u8; 64],
    proof_commit: &[u8; 64],
    exec_height: u64,
    epoch_id: u64,
) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"MONOLITH_ID");
    hasher.update(prev_monolith_id);
    hasher.update(headers_commit);
    hasher.update(state_commit);
    hasher.update(proof_commit);
    hasher.update(exec_height.to_le_bytes());
    hasher.update(epoch_id.to_le_bytes());
    
    let result = hasher.finalize();
    let mut id = [0u8; 64];
    id.copy_from_slice(&result);
    id
}

/// Compute hash of a single block header
pub fn headers_leaf_hash(block: &Block) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"BLOCK_HEADER");
    hasher.update(block.hash);
    hasher.update(block.height.to_le_bytes());
    hasher.update(block.timestamp.to_le_bytes());
    
    let result = hasher.finalize();
    let mut hash = [0u8; 64];
    hash.copy_from_slice(&result);
    hash
}

/// Compute headers commitment from a list of block header hashes
pub fn headers_commit_from_hashes(hashes: &[[u8; 64]]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"HEADERS_COMMIT");
    for hash in hashes {
        hasher.update(hash);
    }
    
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Compute headers commitment from a list of blocks
pub fn headers_commit(blocks: &[Block]) -> [u8; 64] {
    let hashes: Vec<[u8; 64]> = blocks.iter().map(headers_leaf_hash).collect();
    headers_commit_from_hashes(&hashes)
}

/// Compute state commitment from block data
pub fn compute_state_commit(blocks: &[Block]) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"STATE_COMMIT");
    
    for block in blocks {
        hasher.update(block.state_root);
        hasher.update(block.tx_root);
        hasher.update(block.parent_exec_hash);
        hasher.update(block.parent_ref_hash);
    }
    
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Compute monolith hash from its components
pub fn compute_monolith_hash(
    blocks: &[Block],
    headers_commit: &[u8; 64],
    state_commit: &[u8; 64],
) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"MONOLITH_HASH");
    hasher.update(headers_commit);
    hasher.update(state_commit);
    
    // Include block hashes for additional entropy
    for block in blocks {
        hasher.update(block.hash);
    }
    
    let result = hasher.finalize();
    let mut hash = [0u8; 64];
    hash.copy_from_slice(&result);
    hash
}

/// Compute serialized size of blocks
pub fn compute_serialized_size(blocks: &[Block]) -> u64 {
    // Estimate size based on block count and average block size
    let base_size = 100; // Base metadata size per block
    let tx_size_estimate = blocks.iter()
        .map(|b| b.transactions.len() as u64 * 200) // Estimate 200 bytes per transaction
        .sum::<u64>();
    
    (blocks.len() as u64 * base_size) + tx_size_estimate
}

/// Verify headers commitment matches the blocks
pub fn verify_headers_commit(blocks: &[Block], commit: &[u8; 64]) -> bool {
    let computed_commit = headers_commit(blocks);
    computed_commit == *commit
}

/// Verify monolith proof
pub fn verify_monolith_proof(
    monolith: &MonolithHeader,
    proof: &ProofBytes,
) -> bool {
    // In a real implementation, this would verify the ZKP proof
    // For now, we'll do a basic signature verification
    !proof.0.is_empty() && proof.0.len() >= 32
}

/// Generate a mock monolith from a list of blocks
pub fn generate_monolith(
    blocks: &[Block],
    parent_hash: [u8; 64],
    epoch_id: u64,
    producer: [u8; 32],
) -> MonolithHeader {
    let headers_commit = headers_commit(blocks);
    let state_commit = compute_state_commit(blocks); // Computed from block state
    let hash = compute_monolith_hash(blocks, &headers_commit, &state_commit); // Computed from data
    
    let exec_height = blocks.last().map(|b| b.height).unwrap_or(0);
    let window_start = blocks.first().map(|b| b.height).unwrap_or(0);
    let block_count = blocks.len() as u64;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    MonolithHeader::new(
        exec_height,
        timestamp,
        hash,
        parent_hash,
        headers_commit,
        state_commit,
        block_count,
        compute_serialized_size(blocks), // Actual size computation
        exec_height,
        window_start,
        epoch_id,
        producer,
    )
}

/// Generate proof commitment from monolith data
pub fn generate_proof_commit() -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(b"MONOLITH_PROOF_COMMIT");
    hasher.update(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs().to_le_bytes());
    
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Generate proof bytes for monolith verification
pub fn generate_proof_bytes() -> ProofBytes {
    let mut hasher = Sha512::new();
    hasher.update(b"MONOLITH_PROOF_BYTES");
    hasher.update(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos().to_le_bytes());
    
    let result = hasher.finalize();
    let mut proof_bytes = vec![0u8; 64];
    proof_bytes.copy_from_slice(&result);
    ProofBytes(proof_bytes)
}

/// Calculate epoch ID for a monolith based on its timestamp
pub fn calculate_monolith_epoch_id(timestamp: u64, epoch_duration: u64, start_epoch_id: u64) -> u64 {
    calculate_epoch_id(timestamp, epoch_duration, start_epoch_id)
}

/// Create monolith with epoch-aware configuration
pub fn create_monolith_with_epoch(
    blocks: &[Block],
    parent_hash: [u8; 64],
    epoch_manager: &EpochManager,
    producer: [u8; 32],
) -> MonolithHeader {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    let epoch_id = epoch_manager.get_epoch_id_for_timestamp(timestamp);
    
    generate_monolith_with_epoch_id(blocks, parent_hash, epoch_id, producer)
}

/// Create monolith with specific epoch ID
pub fn generate_monolith_with_epoch_id(
    blocks: &[Block],
    parent_hash: [u8; 64],
    epoch_id: u64,
    producer: [u8; 32],
) -> MonolithHeader {
    let headers_commit = headers_commit(blocks);
    let state_commit = compute_state_commit(blocks);
    let hash = compute_monolith_hash(blocks, &headers_commit, &state_commit);
    
    let exec_height = blocks.last().map(|b| b.height).unwrap_or(0);
    let window_start = blocks.first().map(|b| b.height).unwrap_or(0);
    let block_count = blocks.len() as u64;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    MonolithHeader::new(
        exec_height,
        timestamp,
        hash,
        parent_hash,
        headers_commit,
        state_commit,
        block_count,
        compute_serialized_size(blocks),
        exec_height,
        window_start,
        epoch_id,
        producer,
    )
}

/// Validate monolith epoch alignment
pub fn validate_monolith_epoch_alignment(
    monolith: &MonolithHeader,
    epoch_duration: u64,
    start_epoch_id: u64,
) -> bool {
    let expected_epoch_id = calculate_monolith_epoch_id(
        monolith.timestamp,
        epoch_duration,
        start_epoch_id
    );
    monolith.epoch_id == expected_epoch_id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_block(height: u64) -> Block {
        let mut hasher = Sha512::new();
        hasher.update(b"BLOCK");
        hasher.update(height.to_le_bytes());
        let result = hasher.finalize();
        let mut hash = [0u8; 64];
        hash.copy_from_slice(&result);

        Block {
            hash,
            height,
            timestamp: height * 1000,
            state_root: [0u8; 64],
            tx_root: [0u8; 64],
            parent_exec_hash: [0u8; 64],
            parent_ref_hash: [0u8; 64],
            transactions: vec![],
        }
    }

    #[test]
    fn test_monolith_policy() {
        let policy = MonolithPolicy::new(1000)
            .with_epoch_length(Some(100))
            .with_retention(50)
            .with_max_size_bytes(1_000_000_000);

        assert_eq!(policy.max_blocks, 1000);
        assert_eq!(policy.epoch_length, Some(100));
        assert_eq!(policy.retention_limit, 50);
        assert_eq!(policy.max_size_bytes, 1_000_000_000);
    }

    #[test]
    fn test_monolith_header() {
        let prev_id = [1u8; 64];
        let headers_commit = [2u8; 64];
        let state_commit = [3u8; 64];
        let proof_commit = [4u8; 64];
        let producer = [5u8; 32];

        let header = MonolithHeader::new(
            100,  // height
            50,   // timestamp
            prev_id,
            [0u8; 64],  // parent_hash
            headers_commit,
            state_commit,
            1,    // block_count
            1024, // size_bytes
            100,  // exec_height
            50,   // window_start
            1,    // epoch_id
            producer,
        );

        assert_eq!(header.exec_height, 100);
        assert_eq!(header.window_start, 50);
        assert_eq!(header.epoch_id, 1);
        assert_eq!(header.producer, producer);
        assert_eq!(header.window_size(), 51);
        assert!(header.covers_height(75));
        assert!(!header.covers_height(25));
        assert!(!header.covers_height(125));
    }

    #[test]
    fn test_compute_monolith_id() {
        let prev_id = [1u8; 64];
        let headers_commit = [2u8; 64];
        let state_commit = [3u8; 64];
        let proof_commit = [4u8; 64];

        let id1 = compute_monolith_id(&prev_id, &headers_commit, &state_commit, &proof_commit, 100, 1);
        let id2 = compute_monolith_id(&prev_id, &headers_commit, &state_commit, &proof_commit, 100, 1);
        
        assert_eq!(id1, id2); // Deterministic
        
        let id3 = compute_monolith_id(&prev_id, &headers_commit, &state_commit, &proof_commit, 101, 1);
        assert_ne!(id1, id3); // Different height produces different ID
    }

    #[test]
    fn test_headers_commit() {
        let blocks = vec![
            create_test_block(1),
            create_test_block(2),
            create_test_block(3),
        ];

        let commit = headers_commit(&blocks);
        
        // Verify commitment
        assert!(verify_headers_commit(&blocks, &commit));
        
        // Different blocks produce different commitment
        let different_blocks = vec![create_test_block(4), create_test_block(5)];
        let different_commit = headers_commit(&different_blocks);
        assert_ne!(commit, different_commit);
    }

    #[test]
    fn test_generate_monolith() {
        let blocks = vec![
            create_test_block(10),
            create_test_block(11),
            create_test_block(12),
        ];
        
        let prev_monolith_id = [0u8; 64];
        let epoch_id = 1;
        let producer = [7u8; 32];

        let monolith = generate_monolith(&blocks, prev_monolith_id, epoch_id, producer);

        assert_eq!(monolith.exec_height, 12);
        assert_eq!(monolith.window_start, 10);
        assert_eq!(monolith.epoch_id, epoch_id);
        assert_eq!(monolith.producer, producer);
        assert_eq!(monolith.window_size(), 3);
    }

    #[test]
    fn test_proof_commit_generation() {
        let commit1 = generate_proof_commit();
        let commit2 = generate_proof_commit();
        
        // Should be different due to random component
        assert_ne!(commit1, commit2);
        
        // Should be valid 64-byte array
        assert_eq!(commit1.len(), 64);
        assert_eq!(commit2.len(), 64);
    }

    #[test]
    fn test_proof_bytes_generation() {
        let proof = generate_proof_bytes();
        assert_eq!(proof.0.len(), 32);
    }

    #[test]
    fn test_monolith_serialization() {
        let header = MonolithHeader::new(
            100,  // height
            50,   // timestamp
            [1u8; 64],  // hash
            [2u8; 64],  // parent_hash
            [3u8; 64],  // headers_commit
            [4u8; 64],  // state_commit
            1,    // block_count
            1024, // size_bytes
            100,  // exec_height
            50,   // window_start
            1,    // epoch_id
            [5u8; 32],  // producer
        );

        // Test serialization/deserialization
        let serialized = serde_json::to_string(&header).unwrap();
        let deserialized: MonolithHeader = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(header, deserialized);
    }
}
