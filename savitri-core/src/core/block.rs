use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use sha2::{Digest, Sha512};

use super::types::Transaction;
use crate::core::crypto::{compute_tx_root, sign_data, verify_signature};
use ed25519_dalek::{Signature, SigningKey as Keypair, VerifyingKey as PublicKey};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Block {
    pub version: u8,
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],

    pub transactions: Vec<Transaction>,

    #[serde(with = "BigArray")]
    pub proposer: [u8; 32],

    #[serde(with = "BigArray")]
    pub signature: [u8; 64],

    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],

    #[serde(with = "BigArray")]
    pub parent_exec_hash: [u8; 64],

    #[serde(with = "BigArray")]
    pub parent_ref_hash: [u8; 64],

    pub height: u64,
    pub timestamp: u64,

    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
}

impl Block {
    /// Создаёт новый пустой блок с заранее известным набором транзакций и публичным ключом (пропозером).
    pub fn new(transactions: Vec<Transaction>, proposer: [u8; 32]) -> Self {
        let mut block = Block {
            version: 1,
            hash: [0; 64],
            transactions,
            proposer,
            signature: [0; 64],
            state_root: [0; 64],
            parent_exec_hash: [0; 64],
            parent_ref_hash: [0; 64],
            height: 0,
            timestamp: 0,
            tx_root: [0; 64],
        };
        block.tx_root = compute_tx_root(&block.transactions);
        block.hash = block.header_hash();
        block
    }

    pub fn header_hash(&self) -> [u8; 64] {
        let mut hasher = Sha512::new();
        // SECURITY: Domain separation tag per spec — prevents cross-protocol hash collisions
        hasher.update(b"BLK");
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.proposer);
        hasher.update(&self.signature);
        hasher.update(&self.state_root);
        hasher.update(&self.parent_exec_hash);
        hasher.update(&self.parent_ref_hash);
        hasher.update(&self.height.to_le_bytes());
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(&self.tx_root);

        let result = hasher.finalize();
        let mut hash = [0u8; 64];
        hash.copy_from_slice(&result);
        hash
    }

    /// Подписывает блок ключом.
    pub fn sign(&mut self, keypair: &Keypair) {
        let sig = sign_data(keypair, &self.hash);
        self.signature.copy_from_slice(&sig.to_bytes());
    }

    /// Проверяет подпись блока.
    pub fn verify(&self) -> bool {
        let public_key = match PublicKey::from_bytes(&self.proposer) {
            Ok(pk) => pk,
            Err(_) => return false,
        };

        // In ed25519-dalek 1.x, Signature::from_bytes returns Result<Signature, Error>
        let signature = match Signature::try_from(&self.signature) {
            Ok(sig) => sig,
            Err(_) => return false,
        };

        verify_signature(&public_key, &self.hash, &signature)
    }

    /// Create a new block with parent hashes and height
    pub fn new_with_parent(
        transactions: Vec<Transaction>,
        proposer: [u8; 32],
        parent_exec_hash: [u8; 64],
        parent_ref_hash: [u8; 64],
        height: u64,
        timestamp: u64,
    ) -> Self {
        let mut block = Block {
            version: 1,
            hash: [0; 64],
            transactions,
            proposer,
            signature: [0; 64],
            state_root: [0; 64],
            parent_exec_hash,
            parent_ref_hash,
            height,
            timestamp,
            tx_root: [0; 64],
        };
        block.tx_root = compute_tx_root(&block.transactions);
        block.hash = block.header_hash();
        block
    }

    /// Set the state root
    pub fn set_state_root(&mut self, state_root: [u8; 64]) {
        self.state_root = state_root;
        self.hash = self.header_hash();
    }

    /// Get block size in bytes
    pub fn size(&self) -> usize {
        bincode::serialize(self).unwrap_or_default().len()
    }

    /// Check if this is a genesis block
    pub fn is_genesis(&self) -> bool {
        self.height == 0
    }

    /// Get the number of transactions in the block
    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }

    /// Check if block is empty (no transactions)
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Get the total value of all transactions
    pub fn total_value(&self) -> u128 {
        self.transactions.iter().map(|tx| tx.amount as u128).sum()
    }

    /// Validate block structure
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err("Invalid block version".to_string());
        }

        if self.height == 0 && !self.is_genesis() {
            return Err("Invalid height for non-genesis block".to_string());
        }

        if self.timestamp == 0 {
            return Err("Invalid timestamp".to_string());
        }

        if self.proposer == [0u8; 32] {
            return Err("Invalid proposer".to_string());
        }

        // Verify block hash
        let computed_hash = self.header_hash();
        if self.hash != computed_hash {
            return Err("Invalid block hash".to_string());
        }

        // Verify transaction root
        let computed_tx_root = compute_tx_root(&self.transactions);
        if self.tx_root != computed_tx_root {
            return Err("Invalid transaction root".to_string());
        }

        Ok(())
    }

    /// Create a copy with new signature
    pub fn with_signature(&self, signature: [u8; 64]) -> Self {
        let mut block = self.clone();
        block.signature = signature;
        block.hash = block.header_hash();
        block
    }

    /// Get block hash as hex string
    pub fn hash_hex(&self) -> String {
        hex::encode(self.hash)
    }

    /// Get proposer as hex string
    pub fn proposer_hex(&self) -> String {
        hex::encode(self.proposer)
    }

    /// Get signature as hex string
    pub fn signature_hex(&self) -> String {
        hex::encode(self.signature)
    }

    /// Get state root as hex string
    pub fn state_root_hex(&self) -> String {
        hex::encode(self.state_root)
    }

    /// Get transaction root as hex string
    pub fn tx_root_hex(&self) -> String {
        hex::encode(self.tx_root)
    }

    /// Get parent exec hash as hex string
    pub fn parent_exec_hash_hex(&self) -> String {
        hex::encode(self.parent_exec_hash)
    }

    /// Get parent ref hash as hex string
    pub fn parent_ref_hash_hex(&self) -> String {
        hex::encode(self.parent_ref_hash)
    }

    /// Serialize block to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Maximum allowed size for block deserialization (4 MB).
    /// SECURITY (AUDIT-020): Prevents DoS via oversized payloads.
    const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

    /// Deserialize block from bytes with size limit.
    ///
    /// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB to prevent
    /// memory exhaustion from maliciously crafted network data.
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        if data.len() > Self::MAX_DESERIALIZE_SIZE {
            return Err(Box::new(bincode::ErrorKind::Custom(format!(
                "Data too large for deserialization: {} bytes (max {})",
                data.len(),
                Self::MAX_DESERIALIZE_SIZE
            ))));
        }
        bincode::deserialize(data)
    }

    /// Create a genesis block
    pub fn genesis(transactions: Vec<Transaction>, proposer: [u8; 32]) -> Self {
        Self::new_with_parent(
            transactions,
            proposer,
            [0u8; 64], // parent_exec_hash
            [0u8; 64], // parent_ref_hash
            0,         // height
            1000000,   // timestamp
        )
    }

    /// Get block age in seconds
    pub fn age_seconds(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.timestamp)
    }

    /// Check if block is recent (within last N seconds)
    pub fn is_recent(&self, within_seconds: u64) -> bool {
        self.age_seconds() <= within_seconds
    }

    /// Get block summary
    pub fn summary(&self) -> BlockSummary {
        BlockSummary {
            hash: self.hash.to_vec(),
            height: self.height,
            timestamp: self.timestamp,
            transaction_count: self.transaction_count(),
            total_value: self.total_value(),
            proposer: self.proposer,
            size_bytes: self.size(),
            is_genesis: self.is_genesis(),
            age_seconds: self.age_seconds(),
        }
    }
}

/// Block summary for quick display
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlockSummary {
    #[serde(with = "serde_bytes")]
    pub hash: Vec<u8>,
    pub height: u64,
    pub timestamp: u64,
    pub transaction_count: usize,
    pub total_value: u128,
    pub proposer: [u8; 32],
    pub size_bytes: usize,
    pub is_genesis: bool,
    pub age_seconds: u64,
}

impl BlockSummary {
    /// Get hash as hex string
    pub fn hash_hex(&self) -> String {
        hex::encode(self.hash.clone())
    }

    /// Get proposer as hex string
    pub fn proposer_hex(&self) -> String {
        hex::encode(self.proposer)
    }

    /// Get formatted age
    pub fn formatted_age(&self) -> String {
        let age = self.age_seconds;
        if age < 60 {
            format!("{}s", age)
        } else if age < 3600 {
            format!("{}m {}s", age / 60, age % 60)
        } else if age < 86400 {
            format!("{}h {}m", age / 3600, (age % 3600) / 60)
        } else {
            format!("{}d {}h", age / 86400, (age % 86400) / 3600)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[test]
    fn test_block_creation() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::new(transactions.clone(), proposer);

        assert_eq!(block.version, 1);
        assert_eq!(block.proposer, proposer);
        assert_eq!(block.height, 0);
        assert_eq!(block.transaction_count(), 1);
        assert_eq!(block.total_value(), 1000);
        assert!(!block.is_empty());
    }

    #[test]
    fn test_genesis_block() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::genesis(transactions.clone(), proposer);

        assert!(block.is_genesis());
        assert_eq!(block.height, 0);
        assert_eq!(block.parent_exec_hash, [0u8; 64]);
        assert_eq!(block.parent_ref_hash, [0u8; 64]);
    }

    #[test]
    fn test_block_with_parent() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];
        let parent_exec_hash = [2u8; 64];
        let parent_ref_hash = [3u8; 64];

        let block = Block::new_with_parent(
            transactions.clone(),
            proposer,
            parent_exec_hash,
            parent_ref_hash,
            10,
            1234567890,
        );

        assert_eq!(block.height, 10);
        assert_eq!(block.parent_exec_hash, parent_exec_hash);
        assert_eq!(block.parent_ref_hash, parent_ref_hash);
        assert_eq!(block.timestamp, 1234567890);
    }

    #[test]
    fn test_block_signing() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let keypair = SigningKey::from_bytes(&[42u8; 32]);
        let proposer = keypair.verifying_key().to_bytes();

        let mut block = Block::new(transactions, proposer);
        block.sign(&keypair);

        assert!(block.verify());
        assert_ne!(block.signature, [0u8; 64]);
    }

    #[test]
    fn test_block_validation() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::new(transactions, proposer);

        assert!(block.validate().is_ok());

        // Test invalid version
        let mut invalid_block = block.clone();
        invalid_block.version = 2;
        assert!(invalid_block.validate().is_err());
    }

    #[test]
    fn test_block_serialization() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::new(transactions, proposer);

        let serialized = block.serialize().unwrap();
        let deserialized = Block::deserialize(&serialized).unwrap();

        assert_eq!(block, deserialized);
    }

    #[test]
    fn test_block_hex_methods() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::new(transactions, proposer);

        let hash_hex = block.hash_hex();
        assert_eq!(hash_hex.len(), 128); // 64 bytes * 2 hex chars

        let proposer_hex = block.proposer_hex();
        assert_eq!(proposer_hex.len(), 64); // 32 bytes * 2 hex chars
    }

    #[test]
    fn test_block_summary() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let block = Block::new(transactions, proposer);
        let summary = block.summary();

        assert_eq!(summary.transaction_count, 1);
        assert_eq!(summary.total_value, 1000);
        assert_eq!(summary.proposer, proposer);
        assert!(!summary.is_genesis);
        assert_eq!(summary.size_bytes, block.size());
    }

    #[test]
    fn test_block_age() {
        let transactions = vec![Transaction {
            from: "0x1234567890abcdef".to_string(),
            to: "0xfedcba0987654321".to_string(),
            amount: 1000,
        }];
        let proposer = [1u8; 32];

        let mut block = Block::new(transactions, proposer);
        block.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            - 100; // 100 seconds ago

        assert!(block.age_seconds() >= 100);
        assert!(block.is_recent(150)); // Within 150 seconds
        assert!(!block.is_recent(50)); // Not within 50 seconds
    }

    #[test]
    fn test_block_summary_formatted_age() {
        let mut summary = BlockSummary {
            hash: [1u8; 64],
            height: 100,
            timestamp: 0,
            transaction_count: 1,
            total_value: 1000,
            proposer: [2u8; 32],
            size_bytes: 500,
            is_genesis: false,
            age_seconds: 45,
        };

        assert_eq!(summary.formatted_age(), "45s");

        summary.age_seconds = 125;
        assert_eq!(summary.formatted_age(), "2m 5s");

        summary.age_seconds = 3661;
        assert_eq!(summary.formatted_age(), "1h 1m");

        summary.age_seconds = 86401;
        assert_eq!(summary.formatted_age(), "1d 0h");
    }
}
