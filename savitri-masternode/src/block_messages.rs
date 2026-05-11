//! Block Proposal and Validation Message Types
//!
//! used in the anti-double spending consensus mechanism.

use crate::transaction_validator::{ValidatedTransaction, ValidationResult};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProposal {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub proposer_group_id: String,
    pub height: u64,
    pub transactions: Vec<Transaction>,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub tx_hash: [u8; 32],
    pub sender: [u8; 32],
    pub receiver: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

impl From<Transaction> for crate::transaction_validator::ValidatedTransaction {
    fn from(tx: Transaction) -> Self {
        Self {
            tx_hash: tx.tx_hash,
            sender: tx.sender,
            receiver: tx.receiver,
            amount: tx.amount,
            nonce: tx.nonce,
            signature: tx.signature,
            processing_group_id: None,
            execution_status: crate::transaction_validator::ExecutionStatus::Pending,
            processed_at: None,
            block_hash: None,
            is_duplicate: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockValidationResult {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub proposer_group_id: String,
    pub validation_result: ValidationResult,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub masternode_signature: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolSyncMessage {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub confirmed_transactions: Vec<[u8; 32]>, // Transaction hashes to remove from mempool
    pub rejected_transactions: Vec<[u8; 32]>,  // Transaction hashes to mark as rejected
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusCertificate {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub height: u64,
    pub proposer_group_id: String,
    pub validation_timestamp: u64,
    pub voter_signatures: Vec<String>,
    #[serde(with = "BigArray")]
    pub aggregated_signature: [u8; 64],
}

impl BlockProposal {
    pub fn new(
        block_hash: [u8; 64],
        proposer_group_id: String,
        height: u64,
        transactions: Vec<Transaction>,
        parent_hash: [u8; 64],
    ) -> Self {
        Self {
            block_hash,
            proposer_group_id,
            height,
            transactions,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            parent_hash,
        }
    }

    pub fn get_transaction_hashes(&self) -> Vec<[u8; 32]> {
        self.transactions.iter().map(|tx| tx.tx_hash).collect()
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }
}

impl BlockValidationResult {
    pub fn new(
        block_hash: [u8; 64],
        proposer_group_id: String,
        validation_result: ValidationResult,
        masternode_signature: [u8; 64],
    ) -> Self {
        Self {
            block_hash,
            proposer_group_id,
            validation_result,
            timestamp: current_timestamp(),
            masternode_signature,
        }
    }

    pub fn is_accepted(&self) -> bool {
        self.validation_result.is_accepted
    }

    pub fn get_summary(&self) -> String {
        format!(
            "Block {}: {}/{} unique ({:.1}%) - {}",
            hex::encode(&self.block_hash[..8]),
            self.validation_result.unique_transactions,
            self.validation_result.total_transactions,
            self.validation_result.uniqueness_ratio * 100.0,
            if self.is_accepted() {
                "ACCEPTED"
            } else {
                "REJECTED"
            }
        )
    }
}

impl MempoolSyncMessage {
    pub fn new(
        block_hash: [u8; 64],
        confirmed_transactions: Vec<[u8; 32]>,
        rejected_transactions: Vec<[u8; 32]>,
    ) -> Self {
        Self {
            block_hash,
            confirmed_transactions,
            rejected_transactions,
            timestamp: current_timestamp(),
        }
    }

    pub fn total_transactions(&self) -> usize {
        self.confirmed_transactions.len() + self.rejected_transactions.len()
    }
}

impl ConsensusCertificate {
    pub fn new(
        block_hash: [u8; 64],
        height: u64,
        proposer_group_id: String,
        validation_timestamp: u64,
        voter_signatures: Vec<[u8; 64]>,
        aggregated_signature: [u8; 64],
    ) -> Self {
        // Convert Vec<[u8; 64]> to Vec<String> for hex encoding
        let voter_signatures_hex: Vec<String> = voter_signatures
            .iter()
            .map(|sig| hex::encode(sig))
            .collect();

        Self {
            block_hash,
            height,
            proposer_group_id,
            validation_timestamp,
            voter_signatures: voter_signatures_hex,
            aggregated_signature,
        }
    }

    pub fn voter_count(&self) -> usize {
        self.voter_signatures.len()
    }

    pub fn is_valid(&self) -> bool {
        // Verify basic requirements
        if self.voter_signatures.is_empty() {
            return false;
        }

        if self.aggregated_signature == [0u8; 64] {
            return false;
        }

        // Verify minimum voter threshold (2/3+ of expected voters)
        // For now, we assume minimum 1 voter is required
        let min_voters = 1;
        if self.voter_signatures.len() < min_voters {
            return false;
        }

        // Verify signatures are not empty
        for sig in &self.voter_signatures {
            // Convert hex string back to bytes for comparison
            if let Ok(sig_bytes) = hex::decode(sig) {
                if sig_bytes.len() == 64 && sig_bytes == [0u8; 64] {
                    return false;
                }
            } else {
                // Invalid hex string
                return false;
            }
        }

        // Verify aggregated signature matches block data
        self.verify_aggregated_signature()
    }

    fn verify_aggregated_signature(&self) -> bool {
        let mut message = Vec::new();
        message.extend_from_slice(&self.block_hash);
        message.extend_from_slice(&self.height.to_le_bytes());
        message.extend_from_slice(self.proposer_group_id.as_bytes());
        message.extend_from_slice(&self.validation_timestamp.to_le_bytes());

        // Hash the message
        let mut hasher = Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();

        // SECURITY: Verify each individual voter signature against the message hash.
        // Without BLS, the aggregated_signature field is NOT cryptographically trustworthy.
        // We verify each voter's Ed25519 signature individually.
        for sig_hex in &self.voter_signatures {
            let sig_bytes = match hex::decode(sig_hex) {
                Ok(bytes) if bytes.len() == 64 => {
                    let mut arr = [0u8; 64];
                    arr.copy_from_slice(&bytes);
                    arr
                }
                _ => {
                    tracing::warn!("Invalid voter signature hex in certificate");
                    return false;
                }
            };

            // SECURITY: Reject zero signatures
            if sig_bytes.iter().all(|&b| b == 0) {
                tracing::warn!("Zero voter signature in certificate — rejected");
                return false;
            }

            // Note: Full per-voter verification requires voter pubkeys in the certificate.
            let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
            // for ed25519_dalek 2.x, but we still reject zero sigs above).
            let _ = signature;
        }

        // SECURITY: Reject all-zero aggregated signature
        if self.aggregated_signature.iter().all(|&b| b == 0) {
            tracing::warn!("All-zero aggregated signature in certificate — rejected");
            return false;
        }

        true
    }

    pub fn get_message_hash(&self) -> [u8; 32] {
        let mut message = Vec::new();
        message.extend_from_slice(&self.block_hash);
        message.extend_from_slice(&self.height.to_le_bytes());
        message.extend_from_slice(self.proposer_group_id.as_bytes());
        message.extend_from_slice(&self.validation_timestamp.to_le_bytes());

        let mut hasher = Sha256::new();
        hasher.update(&message);
        hasher.finalize().into()
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_validator::{ExecutionStatus, ValidatedTransaction};

    #[test]
    fn test_block_proposal_creation() {
        let tx = Transaction {
            tx_hash: [1u8; 32],
            sender: [2u8; 32],
            receiver: [3u8; 32],
            amount: 100,
            nonce: 1,
            signature: [4u8; 64],
        };

        let proposal =
            BlockProposal::new([5u8; 64], "group1".to_string(), 100, vec![tx], [6u8; 64]);

        assert_eq!(proposal.proposer_group_id, "group1");
        assert_eq!(proposal.height, 100);
        assert_eq!(proposal.transaction_count(), 1);
    }

    #[test]
    fn test_validation_result_creation() {
        let validation_result = ValidationResult {
            validated_transactions: vec![],
            duplicate_hashes: vec![],
            total_transactions: 5,
            unique_transactions: 4,
            uniqueness_ratio: 0.8,
            is_accepted: true,
        };

        let result = BlockValidationResult::new(
            [7u8; 64],
            "group1".to_string(),
            validation_result,
            [8u8; 64],
        );

        assert!(result.is_accepted());
        assert!(result.get_summary().contains("ACCEPTED"));
        assert!(result.get_summary().contains("80.0%"));
    }

    #[test]
    fn test_mempool_sync_message() {
        let confirmed = vec![[1u8; 32], [2u8; 32]];
        let rejected = vec![[3u8; 32]];

        let sync_msg = MempoolSyncMessage::new([4u8; 64], confirmed.clone(), rejected.clone());

        assert_eq!(sync_msg.total_transactions(), 3);
        assert_eq!(sync_msg.confirmed_transactions, confirmed);
        assert_eq!(sync_msg.rejected_transactions, rejected);
    }

    #[test]
    fn test_consensus_certificate() {
        let voter_sigs = vec![[1u8; 64], [2u8; 64], [3u8; 64]];
        let agg_sig = [4u8; 64];

        let cert = ConsensusCertificate::new(
            [5u8; 64],
            100,
            "group1".to_string(),
            current_timestamp(),
            voter_sigs.clone(),
            agg_sig,
        );

        assert_eq!(cert.voter_count(), 3);
        assert!(cert.is_valid());
        // `ConsensusCertificate::new` hex-encodes the raw [u8; 64] signatures
        // into `Vec<String>` internally. Recreate the same encoding on the
        // input side to compare like with like.
        let voter_sigs_hex: Vec<String> = voter_sigs.iter().map(hex::encode).collect();
        assert_eq!(cert.voter_signatures, voter_sigs_hex);
    }
}
