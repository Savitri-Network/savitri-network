//! Merkle tree implementation for consensus
//!
//! This module provides Merkle tree construction and verification for
//! transaction roots and state proofs.

use super::hashes::{hash_internal, hash_leaf, sha256};

/// Merkle tree error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum MerkleError {
    #[error("Empty input")]
    EmptyInput,
    #[error("Invalid proof")]
    InvalidProof,
    #[error("Index out of bounds: {index} >= {len}")]
    IndexOutOfBounds { index: usize, len: usize },
}

/// Result type for Merkle operations
pub type MerkleResult<T> = std::result::Result<T, MerkleError>;

/// Direction in Merkle proof path
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofDirection {
    Left,
    Right,
}

/// Single step in a Merkle proof
#[derive(Debug, Clone)]
pub struct ProofStep {
    pub hash: [u8; 32],
    pub direction: ProofDirection,
}

/// Merkle proof for a single leaf
#[derive(Debug, Clone)]
pub struct MerkleProof {
    pub leaf_hash: [u8; 32],
    pub path: Vec<ProofStep>,
    pub root: [u8; 32],
}

impl MerkleProof {
    /// Verify the proof
    pub fn verify(&self) -> bool {
        let mut current = self.leaf_hash;

        for step in &self.path {
            current = match step.direction {
                ProofDirection::Left => hash_internal(&step.hash, &current),
                ProofDirection::Right => hash_internal(&current, &step.hash),
            };
        }

        current == self.root
    }
}

/// Merkle tree structure
#[derive(Debug, Clone)]
pub struct MerkleTree {
    leaves: Vec<[u8; 32]>,
    nodes: Vec<Vec<[u8; 32]>>,
    root: [u8; 32],
}

impl MerkleTree {
    /// Build a Merkle tree from leaf data
    pub fn from_leaves(data: &[&[u8]]) -> MerkleResult<Self> {
        if data.is_empty() {
            return Err(MerkleError::EmptyInput);
        }

        // Hash all leaves
        let leaves: Vec<[u8; 32]> = data.iter().map(|d| hash_leaf(d)).collect();

        Self::from_leaf_hashes(leaves)
    }

    /// Build a Merkle tree from pre-computed leaf hashes
    pub fn from_leaf_hashes(leaves: Vec<[u8; 32]>) -> MerkleResult<Self> {
        if leaves.is_empty() {
            return Err(MerkleError::EmptyInput);
        }

        let mut nodes: Vec<Vec<[u8; 32]>> = Vec::new();
        nodes.push(leaves.clone());

        // Build tree level by level
        let mut current_level = leaves.clone();

        while current_level.len() > 1 {
            let mut next_level = Vec::new();

            // Pad with duplicate if odd number
            if current_level.len() % 2 == 1 {
                current_level.push(*current_level.last().unwrap());
            }

            for chunk in current_level.chunks(2) {
                let parent = hash_internal(&chunk[0], &chunk[1]);
                next_level.push(parent);
            }

            nodes.push(next_level.clone());
            current_level = next_level;
        }

        let root = current_level[0];

        Ok(Self {
            leaves,
            nodes,
            root,
        })
    }

    /// Get the root hash
    pub fn root(&self) -> [u8; 32] {
        self.root
    }

    /// Get number of leaves
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Check if tree is empty
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Generate proof for a leaf at given index
    pub fn proof(&self, index: usize) -> MerkleResult<MerkleProof> {
        if index >= self.leaves.len() {
            return Err(MerkleError::IndexOutOfBounds {
                index,
                len: self.leaves.len(),
            });
        }

        let mut path = Vec::new();
        let mut current_index = index;

        for level in 0..self.nodes.len() - 1 {
            let level_nodes = &self.nodes[level];
            let sibling_index = if current_index % 2 == 0 {
                current_index + 1
            } else {
                current_index - 1
            };

            if sibling_index < level_nodes.len() {
                let direction = if current_index % 2 == 0 {
                    ProofDirection::Right
                } else {
                    ProofDirection::Left
                };

                path.push(ProofStep {
                    hash: level_nodes[sibling_index],
                    direction,
                });
            }

            current_index /= 2;
        }

        Ok(MerkleProof {
            leaf_hash: self.leaves[index],
            path,
            root: self.root,
        })
    }

    /// Verify a leaf exists in the tree
    pub fn verify_leaf(&self, leaf_data: &[u8], index: usize) -> MerkleResult<bool> {
        let leaf_hash = hash_leaf(leaf_data);
        if index >= self.leaves.len() || self.leaves[index] != leaf_hash {
            return Ok(false);
        }

        let proof = self.proof(index)?;
        Ok(proof.verify())
    }
}

/// Compute transaction root from transaction hashes
pub fn compute_tx_root(tx_hashes: &[[u8; 32]]) -> [u8; 32] {
    if tx_hashes.is_empty() {
        return [0u8; 32];
    }

    if tx_hashes.len() == 1 {
        return tx_hashes[0];
    }

    match MerkleTree::from_leaf_hashes(tx_hashes.to_vec()) {
        Ok(tree) => tree.root(),
        Err(_) => [0u8; 32],
    }
}

/// Verify transaction inclusion in a block
pub fn verify_tx_inclusion(tx_hash: &[u8; 32], tx_root: &[u8; 32], proof: &MerkleProof) -> bool {
    proof.leaf_hash == *tx_hash && proof.root == *tx_root && proof.verify()
}

/// Compute Merkle root from arbitrary data
pub fn compute_merkle_root(data: &[&[u8]]) -> MerkleResult<[u8; 32]> {
    let tree = MerkleTree::from_leaves(data)?;
    Ok(tree.root())
}

/// Multi-proof for verifying multiple leaves efficiently
#[derive(Debug, Clone)]
pub struct MultiProof {
    pub leaf_indices: Vec<usize>,
    pub leaf_hashes: Vec<[u8; 32]>,
    pub proof_hashes: Vec<[u8; 32]>,
    pub root: [u8; 32],
}

impl MerkleTree {
    /// Generate a multi-proof for multiple leaves
    pub fn multi_proof(&self, indices: &[usize]) -> MerkleResult<MultiProof> {
        let mut leaf_hashes = Vec::new();
        let mut proof_hashes = Vec::new();

        for &idx in indices {
            if idx >= self.leaves.len() {
                return Err(MerkleError::IndexOutOfBounds {
                    index: idx,
                    len: self.leaves.len(),
                });
            }
            leaf_hashes.push(self.leaves[idx]);
        }

        // Collect proof hashes (simplified - full implementation would be more efficient)
        for &idx in indices {
            let proof = self.proof(idx)?;
            for step in proof.path {
                if !proof_hashes.contains(&step.hash) {
                    proof_hashes.push(step.hash);
                }
            }
        }

        Ok(MultiProof {
            leaf_indices: indices.to_vec(),
            leaf_hashes,
            proof_hashes,
            root: self.root,
        })
    }
}
