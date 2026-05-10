//! Block types and structures
//!
//! This module defines the standardized block structures used across
//! all consensus implementations.

use crate::types::ConsensusType;
use crate::ProposerInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Wrapper for [u8; 64] to support serialization
#[derive(Debug, Clone, PartialEq, Copy, Eq, Hash)]
pub struct Hash64(pub [u8; 64]);

impl Default for Hash64 {
    fn default() -> Self {
        Self([0u8; 64])
    }
}

impl std::ops::Deref for Hash64 {
    type Target = [u8; 64];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash64 {
    /// Get the length of the hash (always 64)
    pub fn len(&self) -> usize {
        64
    }

    /// Check if the hash is empty (always false for Hash64)
    pub fn is_empty(&self) -> bool {
        false
    }
}

impl serde::Serialize for Hash64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as a byte array
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Hash64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Hash64Visitor;
        impl<'de> serde::de::Visitor<'de> for Hash64Visitor {
            type Value = Hash64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a 64-byte hash")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                if bytes.len() == 64 {
                    let mut array = [0u8; 64];
                    array.copy_from_slice(bytes);
                    Ok(Hash64(array))
                } else {
                    Err(serde::de::Error::invalid_length(bytes.len(), &self))
                }
            }
        }

        deserializer.deserialize_bytes(Hash64Visitor)
    }
}

impl From<[u8; 64]> for Hash64 {
    fn from(value: [u8; 64]) -> Self {
        Hash64(value)
    }
}

impl From<Hash64> for Vec<u8> {
    fn from(value: Hash64) -> Self {
        value.0.to_vec()
    }
}

impl From<Hash32> for Vec<u8> {
    fn from(value: Hash32) -> Self {
        value.0.to_vec()
    }
}

/// Wrapper for [u8; 32] to support serialization
#[derive(Debug, Clone, PartialEq, Copy)]
pub struct Hash32(pub [u8; 32]);

impl Default for Hash32 {
    fn default() -> Self {
        Self([0u8; 32])
    }
}

impl std::fmt::Display for Hash32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display as hex string
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl serde::Serialize for Hash32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize as a byte array
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Hash32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Hash32Visitor;
        impl<'de> serde::de::Visitor<'de> for Hash32Visitor {
            type Value = Hash32;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a 32-byte hash")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                if bytes.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(bytes);
                    Ok(Hash32(array))
                } else {
                    Err(serde::de::Error::invalid_length(bytes.len(), &self))
                }
            }
        }

        deserializer.deserialize_bytes(Hash32Visitor)
    }
}

impl From<[u8; 32]> for Hash32 {
    fn from(value: [u8; 32]) -> Self {
        Hash32(value)
    }
}

impl From<Hash32> for [u8; 32] {
    fn from(value: Hash32) -> Self {
        value.0
    }
}

/// Standardized block structure
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Block {
    /// Block header containing metadata
    pub header: BlockHeader,
    /// Transactions included in this block
    pub transactions: Vec<Transaction>,
    /// Consensus-specific data
    pub consensus_data: ConsensusData,
    pub signatures: Vec<Signature>,
}

/// Block header containing metadata
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Block version
    pub version: u32,
    /// Block height
    pub height: u64,
    /// Block timestamp (Unix timestamp)
    pub timestamp: u64,
    /// Parent block hash
    pub parent_hash: Hash64,
    /// State root after executing transactions
    pub state_root: Hash64,
    /// Transaction root (Merkle root of transactions)
    pub tx_root: Hash64,
    /// Consensus root (hash of consensus data)
    pub consensus_root: Hash64,
    /// Proposer's public key
    pub proposer: Hash32,
    /// Slot number for this block
    pub slot: u64,
    /// Epoch number
    pub epoch: u64,
    /// Block size in bytes
    pub size: u64,
    /// Number of transactions
    pub tx_count: u32,
    /// Parent hashes for DAG support
    pub parent_hashes: Vec<Hash64>,
}

/// Transaction structure
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Transaction {
    /// Transaction hash
    pub hash: Hash64,
    /// Sender's public key
    pub from: Hash32,
    /// Recipient's public key
    pub to: Hash32,
    /// Amount to transfer
    pub amount: u64,
    /// Transaction nonce
    pub nonce: u64,
    /// Transaction fee
    pub fee: u64,
    /// Transaction data/payload
    pub data: Vec<u8>,
    /// Transaction signature
    pub signature: Hash64,
    /// Transaction timestamp
    pub timestamp: u64,
}

/// Consensus-specific data attached to blocks
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ConsensusData {
    /// Type of consensus used
    pub consensus_type: ConsensusType,
    /// Proposer information
    pub proposer_info: ProposerInfo,
    /// Validation proof
    pub validation_proof: ValidationProof,
    /// Consensus round information
    pub round_info: RoundInfo,
}

/// Validation proof
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ValidationProof {
    pub proof_type: ValidationProofType,
    /// Proof data
    pub data: Vec<u8>,
    /// Validators who signed the proof
    pub validators: Vec<Hash32>,
    /// Proof timestamp
    pub timestamp: u64,
}

impl ValidationProof {
    pub fn new(
        proof_type: ValidationProofType,
        data: Vec<u8>,
        validators: Vec<Hash32>,
        timestamp: u64,
    ) -> Self {
        Self {
            proof_type,
            data,
            validators,
            timestamp,
        }
    }
}

/// Validation proof type
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum ValidationProofType {
    /// BFT quorum signature
    #[default]
    BftQuorum,
    /// PoU score proof
    PouScore,
    /// Group membership proof
    GroupMembership,
    /// Latency proof
    Latency,
    /// Availability proof
    Availability,
    /// Custom proof type
    Custom(String),
}

/// Round information
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RoundInfo {
    /// Round number
    pub round: u64,
    /// Slot number
    pub slot: u64,
    /// Epoch number
    pub epoch: u64,
    /// Round start time
    pub start_time: u64,
    /// Round duration in milliseconds
    pub duration_ms: u64,
    /// Number of participants
    pub participants: u32,
    /// Validators who participated
    pub validators: Vec<Hash32>,
}

impl RoundInfo {
    pub fn new(
        epoch: u64,
        slot: u64,
        round: u64,
        start_time: u64,
        duration_ms: u64,
        participants: u32,
        validators: Vec<Hash32>,
    ) -> Self {
        Self {
            round,
            slot,
            epoch,
            start_time,
            duration_ms,
            participants,
            validators,
        }
    }
}

/// Signature structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signature {
    /// Signer's public key
    pub public_key: Hash32,
    /// Signature data
    pub signature: Hash64,
    /// Signature timestamp
    pub timestamp: u64,
}

impl Block {
    /// Create a new block
    pub fn new(
        header: BlockHeader,
        transactions: Vec<Transaction>,
        consensus_data: ConsensusData,
    ) -> Self {
        Self {
            header,
            transactions,
            consensus_data,
            signatures: Vec::new(),
        }
    }

    /// Get block hash
    pub fn hash(&self) -> [u8; 64] {
        // AUDIT: Return all-zeros sentinel on serialize failure to prevent
        // hash collisions (multiple failed blocks hashing to the same value).
        let data = match bincode::serialize(self) {
            Ok(d) => d,
            Err(_) => return [0u8; 64],
        };
        let hash = blake3::hash(&data);
        let mut result = [0u8; 64];
        result.copy_from_slice(hash.as_bytes());
        result
    }

    /// Get block size
    pub fn size(&self) -> u64 {
        bincode::serialized_size(self).unwrap_or(0)
    }

    /// Check if block is valid
    pub fn is_valid(&self) -> bool {
        self.header.height > 0
            && self.header.timestamp > 0
            && self.header.slot > 0
            && !self.transactions.is_empty()
            && self.header.tx_count == self.transactions.len() as u32
    }

    /// Get current timestamp
    pub fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

impl BlockHeader {
    /// Create a new block header
    pub fn new(
        version: u32,
        height: u64,
        timestamp: u64,
        parent_hash: [u8; 64],
        state_root: [u8; 64],
        tx_root: [u8; 64],
        consensus_root: [u8; 64],
        proposer: [u8; 32],
        slot: u64,
        epoch: u64,
        size: u64,
        tx_count: u32,
    ) -> Self {
        Self {
            version,
            height,
            timestamp,
            parent_hash: Hash64(parent_hash),
            state_root: Hash64(state_root),
            tx_root: Hash64(tx_root),
            consensus_root: Hash64(consensus_root),
            proposer: Hash32(proposer),
            slot,
            epoch,
            size,
            tx_count,
            parent_hashes: Vec::new(),
        }
    }

    /// Get block header hash
    pub fn hash(&self) -> [u8; 64] {
        let data = match bincode::serialize(self) {
            Ok(d) => d,
            Err(_) => return [0u8; 64],
        };
        let hash = blake3::hash(&data);
        let mut result = [0u8; 64];
        result.copy_from_slice(hash.as_bytes());
        result
    }
}

impl Transaction {
    /// Create a new transaction
    pub fn new(
        from: [u8; 32],
        to: [u8; 32],
        amount: u64,
        nonce: u64,
        fee: u64,
        data: Vec<u8>,
    ) -> Self {
        let timestamp = Block::current_timestamp();

        Self {
            hash: Hash64([0u8; 64]), // Will be calculated
            from: Hash32(from),
            to: Hash32(to),
            amount,
            nonce,
            fee,
            data,
            signature: Hash64([0u8; 64]), // Will be signed
            timestamp,
        }
    }

    /// Calculate transaction hash
    pub fn calculate_hash(&self) -> [u8; 64] {
        let data = match bincode::serialize(self) {
            Ok(d) => d,
            Err(_) => return [0u8; 64],
        };
        let hash = blake3::hash(&data);
        let mut result = [0u8; 64];
        result.copy_from_slice(hash.as_bytes());
        result
    }

    /// Check if transaction is valid
    pub fn is_valid(&self) -> bool {
        self.amount > 0
            && self.fee > 0
            && self.timestamp > 0
            && !self.signature.0.iter().all(|&b| b == 0)
    }
}

impl ConsensusData {
    /// Create new consensus data
    pub fn new(
        consensus_type: ConsensusType,
        proposer_info: ProposerInfo,
        validation_proof: ValidationProof,
        round_info: RoundInfo,
    ) -> Self {
        Self {
            consensus_type,
            proposer_info,
            validation_proof,
            round_info,
        }
    }
}

impl serde::Serialize for Block {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Block", 4)?;
        state.serialize_field("header", &self.header)?;
        state.serialize_field("transactions", &self.transactions)?;
        state.serialize_field("consensus_data", &self.consensus_data)?;
        state.serialize_field("signatures", &self.signatures)?;
        state.end()
    }
}

impl serde::Serialize for Transaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Transaction", 9)?;
        state.serialize_field("hash", &self.hash)?;
        state.serialize_field("from", &self.from)?;
        state.serialize_field("to", &self.to)?;
        state.serialize_field("amount", &self.amount)?;
        state.serialize_field("nonce", &self.nonce)?;
        state.serialize_field("fee", &self.fee)?;
        state.serialize_field("data", &self.data)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl serde::Serialize for ConsensusType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ConsensusType::GroupAware => {
                serializer.serialize_unit_variant("ConsensusType", 0, "GroupAware")
            }
            ConsensusType::PouBased => {
                serializer.serialize_unit_variant("ConsensusType", 1, "PouBased")
            }
            ConsensusType::Hybrid => {
                serializer.serialize_unit_variant("ConsensusType", 2, "Hybrid")
            }
            ConsensusType::Unknown => {
                serializer.serialize_unit_variant("ConsensusType", 3, "Unknown")
            }
        }
    }
}

// Conversion between Block types
impl From<&crate::Block> for Block {
    fn from(block: &crate::Block) -> Self {
        Self {
            header: block.header.clone(),
            transactions: block
                .transactions
                .iter()
                .map(|tx| Transaction {
                    hash: crate::types::block::Hash64({
                        let mut hash = [0u8; 64];
                        let len = std::cmp::min(tx.hash.len(), 64);
                        hash[..len].copy_from_slice(&tx.hash[..len]);
                        hash
                    }),
                    from: crate::types::block::Hash32({
                        let mut hash = [0u8; 32];
                        let len = std::cmp::min(tx.from.len(), 32);
                        hash[..len].copy_from_slice(&tx.from[..len]);
                        hash
                    }),
                    to: crate::types::block::Hash32({
                        let mut hash = [0u8; 32];
                        let len = std::cmp::min(tx.to.len(), 32);
                        hash[..len].copy_from_slice(&tx.to[..len]);
                        hash
                    }),
                    amount: tx.amount,
                    nonce: tx.nonce,
                    fee: tx.fee,
                    data: tx.data.clone(),
                    signature: crate::types::block::Hash64({
                        let mut hash = [0u8; 64];
                        let len = std::cmp::min(tx.signature.len(), 64);
                        hash[..len].copy_from_slice(&tx.signature[..len]);
                        hash
                    }),
                    timestamp: tx.timestamp,
                })
                .collect(),
            consensus_data: ConsensusData::default(),
            signatures: vec![],
        }
    }
}
