//! ZKP Integration for Savitri Masternode
//!
//! and integrity verification in the masternode.

#[cfg(feature = "zkp")]
use anyhow::Result;
#[cfg(feature = "zkp")]
use savitri_consensus::{BlockHeader, BlockProposal};
#[cfg(feature = "zkp")]
use savitri_zkp::zkp::proof;
#[cfg(feature = "zkp")]
use savitri_zkp::{create_verifier, Statement, ZkProof, ZkpBackend, ZkpConfig};
#[cfg(feature = "zkp")]
use tracing::{error, info, warn};

/// ZKP Integration Manager
#[cfg(feature = "zkp")]
pub struct ZkpIntegrationManager {
    verifier: Box<dyn savitri_zkp::ZkVerifier>,
    config: ZkpConfig,
}

#[cfg(feature = "zkp")]
impl ZkpIntegrationManager {
    /// Create new ZKP integration manager
    pub fn new(config: ZkpConfig) -> Result<Self> {
        let verifier = create_verifier(config.clone());

        info!(
            backend = ?config.backend,
            "ZKP integration initialized with backend"
        );

        Ok(Self { verifier, config })
    }

    /// Generate ZKP proof for block proposal
    pub async fn generate_block_proof(&self, proposal: &BlockProposal) -> Result<Vec<u8>> {
        // Create statement from block data
        let statement = Statement {
            a: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.parent_hash[..32]);
                arr
            },
            b: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.state_root[..32]);
                arr
            },
            c: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.tx_root[..32]);
                arr
            },
            d: [0u8; 32], // Reserved for future use
            e: proposal.height,
            f: proposal.timestamp,
        };

        // Generate mock proof (in production, this would use real ZKP generation)
        let proof = proof::generate_mock_proof(&statement);

        info!(
            height = proposal.height,
            proof_size = proof.proof.len(),
            "Generated ZKP proof for block proposal"
        );

        Ok(proof.proof)
    }

    /// Validate ZKP proof for block proposal
    pub async fn validate_block_proof(
        &self,
        proposal: &BlockProposal,
        proof_data: &[u8],
    ) -> Result<bool> {
        // Create statement from block data
        let statement = Statement {
            a: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.parent_hash[..32]);
                arr
            },
            b: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.state_root[..32]);
                arr
            },
            c: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.tx_root[..32]);
                arr
            },
            d: [0u8; 32], // Reserved for future use
            e: proposal.height,
            f: proposal.timestamp,
        };

        // Reconstruct ZKP proof
        let proof = ZkProof {
            proof: proof_data.to_vec(),
            public_inputs: {
                let mut inputs = Vec::new();
                inputs.extend_from_slice(&proposal.parent_hash[..32]);
                inputs.extend_from_slice(&proposal.state_root[..32]);
                inputs.extend_from_slice(&proposal.tx_root[..32]);
                inputs.extend_from_slice(&[0u8; 32]);
                inputs.extend_from_slice(&proposal.height.to_le_bytes());
                inputs.extend_from_slice(&proposal.timestamp.to_le_bytes());
                inputs
            },
            verification_key: proposal.proposer_pubkey.to_vec(),
        };

        // Verify the proof
        match self.verifier.verify(&statement, &proof) {
            Ok(is_valid) => {
                if is_valid {
                    info!(height = proposal.height, "ZKP proof validation successful");
                } else {
                    warn!(height = proposal.height, "ZKP proof validation failed");
                }
                Ok(is_valid)
            }
            Err(e) => {
                error!(
                    height = proposal.height,
                    error = %e,
                    "ZKP proof validation error"
                );
                Err(e.into())
            }
        }
    }

    /// Get ZKP configuration
    pub fn config(&self) -> &ZkpConfig {
        &self.config
    }

    /// Check if ZKP is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self.config.backend, ZkpBackend::Mock)
    }
}

/// ZKP Integration utilities
#[cfg(feature = "zkp")]
pub struct ZkpUtils;

#[cfg(feature = "zkp")]
impl ZkpUtils {
    /// Create ZKP statement from block header
    pub fn block_header_to_statement(header: &BlockHeader) -> Statement {
        Statement {
            a: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&header.parent_hash[..32]);
                arr
            },
            b: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&header.state_root[..32]);
                arr
            },
            c: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&header.tx_root[..32]);
                arr
            },
            d: [0u8; 32], // Reserved for future use
            e: header.height,
            f: header.timestamp,
        }
    }

    /// Create ZKP statement from block proposal
    pub fn block_proposal_to_statement(proposal: &BlockProposal) -> Statement {
        Statement {
            a: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.parent_hash[..32]);
                arr
            },
            b: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.state_root[..32]);
                arr
            },
            c: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.tx_root[..32]);
                arr
            },
            d: [0u8; 32], // Reserved for future use
            e: proposal.height,
            f: proposal.timestamp,
        }
    }

    /// Calculate ZKP proof hash
    pub fn proof_hash(proof_data: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(proof_data);
        hasher.finalize().into()
    }
}

#[cfg(test)]
#[cfg(feature = "zkp")]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zkp_integration() {
        let config = ZkpConfig::production();
        let manager = ZkpIntegrationManager::new(config).unwrap();

        let proposal = BlockProposal {
            round_id: 1,
            height: 100,
            timestamp: 1234567890,
            proposer_pubkey: [1u8; 32],
            proposer_pou_score: 800,
            parent_hash: [2u8; 64],
            state_root: [3u8; 64],
            tx_root: [4u8; 64],
            transactions: vec![],
            signature: [5u8; 64],
            zkp_proof: None,
        };

        // Generate proof
        let proof = manager.generate_block_proof(&proposal).await.unwrap();
        assert!(!proof.is_empty());

        // Validate proof
        let is_valid = manager
            .validate_block_proof(&proposal, &proof)
            .await
            .unwrap();
        assert!(is_valid);
    }

    #[test]
    fn test_zkp_utils() {
        let header = BlockHeader {
            version: 1,
            height: 100,
            timestamp: 1234567890,
            parent_hash: [1u8; 64],
            state_root: [2u8; 64],
            tx_root: [3u8; 64],
            proposer: [4u8; 32],
            slot: 1,
            epoch: 1,
            tx_count: 10,
            zkp_proof: None,
        };

        let statement = ZkpUtils::block_header_to_statement(&header);
        assert_eq!(statement.e, 100);
        assert_eq!(statement.f, 1234567890);

        let proof_hash = ZkpUtils::proof_hash(&[1, 2, 3, 4]);
        assert_ne!(proof_hash, [0u8; 32]);
    }
}
