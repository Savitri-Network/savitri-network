//! Shared types for Savitri ecosystem
//!
//! This module contains types that are shared across multiple Savitri crates
//! to avoid dependency cycles while maintaining type consistency.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

mod big_array_option {
    use serde::{Deserializer, Serializer};
    use serde_big_array::BigArray;

    pub fn serialize<S>(option: &Option<[u8; 64]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match option {
            Some(arr) => BigArray::serialize(arr, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 64]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Some(BigArray::deserialize(deserializer)?))
    }
}

/// Network message types shared across all Savitri modules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMessage {
    pub message_type: MessageType,
    pub payload: Vec<u8>,
    pub sender: [u8; 32],
    pub timestamp: u64,
    #[serde(with = "big_array_option")]
    pub signature: Option<[u8; 64]>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    Transaction,
    Block,
    Consensus,
    Heartbeat,
    DataRequest,
    DataResponse,
}

/// Consensus-related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusData {
    pub round: u64,
    pub step: u8,
    pub value: Vec<u8>,
    pub validator: [u8; 32],
}

/// Storage-related types shared across modules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageKey {
    pub namespace: Vec<u8>,
    pub key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageValue {
    pub data: Vec<u8>,
    pub version: u64,
    pub timestamp: u64,
}

/// Mempool-related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolEntry {
    pub transaction: Vec<u8>, // Serialized transaction
    pub timestamp: u64,
    pub gas_price: u128,
    pub sender: [u8; 32],
}

/// Block-related types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
    pub number: u64,
    pub timestamp: u64,
    pub proposer: [u8; 32],
    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
}

/// P2P network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub max_peers: usize,
    pub min_peers: usize,
    pub heartbeat_interval: u64,
    pub message_timeout: u64,
}

/// Common error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SavitriError {
    NetworkError(String),
    ValidationError(String),
    CryptoError(String),
    StorageError(String),
    TimeoutError(String),
    ConsensusError(String),
    MempoolError(String),
}

impl std::fmt::Display for SavitriError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SavitriError::NetworkError(msg) => write!(f, "Network Error: {}", msg),
            SavitriError::ValidationError(msg) => write!(f, "Validation Error: {}", msg),
            SavitriError::CryptoError(msg) => write!(f, "Crypto Error: {}", msg),
            SavitriError::StorageError(msg) => write!(f, "Storage Error: {}", msg),
            SavitriError::TimeoutError(msg) => write!(f, "Timeout Error: {}", msg),
            SavitriError::ConsensusError(msg) => write!(f, "Consensus Error: {}", msg),
            SavitriError::MempoolError(msg) => write!(f, "Mempool Error: {}", msg),
        }
    }
}

impl std::error::Error for SavitriError {}

/// Common result type
pub type SavitriResult<T> = Result<T, SavitriError>;
