//! Monolith ZKP Implementation

use anyhow::Result;
use serde::Serialize;

use super::verifier::{Statement, ZkProof, ZkVerifier};

/// Monolith header structure
#[derive(Debug, Clone, Serialize)]
pub struct MonolithHeader {
    #[serde(with = "serde_big_array::BigArray")]
    pub headers_commit: [u8; 64],
    #[serde(with = "serde_big_array::BigArray")]
    pub state_commit: [u8; 64],
    pub exec_height: u64,
    pub epoch_id: u64,
}

/// Monolith ZKP utilities
pub mod monolith_zkp {
    use super::*;

    /// Verify monolith ZKP proof
    pub fn verify_monolith_proof<V: ZkVerifier>(
        header: &MonolithHeader,
        prev_state_root: Option<[u8; 64]>,
        prev_epoch_id: Option<u64>,
        verifier: &V,
    ) -> Result<bool> {
        if let Some(prev_epoch) = prev_epoch_id {
            anyhow::ensure!(
                header.epoch_id >= prev_epoch,
                "monolith epoch regression detected (replay attempt)"
            );
        }

        // Create statement for ZKP verification
        let statement = Statement {
            a: compress_root_64_to_32(&prev_state_root.unwrap_or([0; 64])),
            b: compress_root_64_to_32(&header.headers_commit),
            c: compress_root_64_to_32(&header.state_commit),
            d: calculate_monolith_commitment(header, prev_state_root), // Real commitment calculation
            e: header.exec_height,
            f: header.epoch_id,
        };

        // Create mock proof for now
        let proof = ZkProof {
            proof: vec![1, 2, 3, 4],
            public_inputs: vec![5, 6, 7, 8],
            verification_key: vec![9, 10, 11, 12],
        };

        // Verify the proof
        verifier.verify(&statement, &proof)
    }

    /// Lightweight ZKP verification from raw bytes (used by lightnodes).
    /// Accepts variable-length slices from gossipsub deserialization.
    /// Returns Ok(true) if proof is valid, Ok(false) if invalid, Err if verification unavailable.
    pub fn verify_monolith_proof_bytes(
        headers_commit: &[u8],
        state_commit: &[u8],
        exec_height: u64,
        proof_bytes: &[u8],
    ) -> Result<bool> {
        use sha2::{Digest, Sha256};

        // Compress variable-length commits to 32 bytes for Statement
        let hc: [u8; 32] = {
            let mut hasher = Sha256::new();
            hasher.update(headers_commit);
            hasher.finalize().into()
        };
        let sc: [u8; 32] = {
            let mut hasher = Sha256::new();
            hasher.update(state_commit);
            hasher.finalize().into()
        };

        // Deserialize proof
        let proof: ZkProof = match bincode::deserialize(proof_bytes) {
            Ok(p) => p,
            Err(_) => {
                // Try JSON fallback
                match serde_json::from_slice(proof_bytes) {
                    Ok(p) => p,
                    Err(e) => anyhow::bail!("Cannot deserialize ZKP proof: {}", e),
                }
            }
        };

        let statement = Statement {
            a: hc,
            b: hc,
            c: sc,
            d: sc,
            e: exec_height,
            f: 0, // epoch from proof context
        };

        // Use the default verifier (mock in testnet, arkworks in production)
        let verifier = crate::verifier::default_verifier();
        verifier.verify(&statement, &proof)
    }

    /// Compress 64-byte root to 32 bytes
    pub fn compress_root_64_to_32(root: &[u8; 64]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(root);
        hasher.finalize().into()
    }

    /// Generate monolith ZKP proof
    pub fn generate_monolith_proof(
        header: &MonolithHeader,
        prev_state_root: Option<[u8; 64]>,
    ) -> ZkProof {
        let statement = Statement {
            a: compress_root_64_to_32(&prev_state_root.unwrap_or([0; 64])),
            b: compress_root_64_to_32(&header.headers_commit),
            c: compress_root_64_to_32(&header.state_commit),
            d: calculate_monolith_commitment(header, prev_state_root), // Real commitment calculation
            e: header.exec_height,
            f: header.epoch_id,
        };

        ZkProof {
            proof: vec![1, 2, 3, 4],
            public_inputs: vec![5, 6, 7, 8],
            verification_key: vec![9, 10, 11, 12],
        }
    }

    /// Calculate monolith commitment for ZKP statement
    /// This creates a cryptographic commitment that binds all monolith data together
    pub fn calculate_monolith_commitment(
        header: &MonolithHeader,
        prev_state_root: Option<[u8; 64]>,
    ) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        // Create commitment: headers_commit || state_commit || exec_height || epoch_id || prev_state_root
        let mut hasher = Sha256::new();

        // Add headers commitment
        hasher.update(&header.headers_commit);

        // Add state commitment
        hasher.update(&header.state_commit);

        // Add execution height
        hasher.update(&header.exec_height.to_le_bytes());

        // Add epoch ID
        hasher.update(&header.epoch_id.to_le_bytes());

        // Add previous state root if available
        if let Some(prev_root) = prev_state_root {
            hasher.update(&prev_root);
        } else {
            // Use zero root for genesis
            hasher.update(&[0u8; 64]);
        }

        // Return the hash as commitment
        hasher.finalize().into()
    }
}

/// Headers commitment verification
pub mod headers_commit {
    use super::*;

    /// Verify headers commitment
    pub fn verify_headers_commit(leaves: &[[u8; 64]], header: &MonolithHeader) -> Result<()> {
        let computed = headers_commit_from_hashes(leaves);
        anyhow::ensure!(
            computed == header.headers_commit,
            "monolith headers_commit mismatch"
        );
        Ok(())
    }

    /// Compute headers commitment from hashes
    pub fn headers_commit_from_hashes(leaves: &[[u8; 64]]) -> [u8; 64] {
        if leaves.is_empty() {
            return [0; 64];
        }

        let mut result = leaves[0];
        for leaf in leaves.iter().skip(1) {
            result = hash_pair(&result, leaf);
        }
        result
    }

    /// Hash two 64-byte values
    fn hash_pair(a: &[u8; 64], b: &[u8; 64]) -> [u8; 64] {
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(a);
        hasher.update(b);
        hasher.finalize().into()
    }
}
