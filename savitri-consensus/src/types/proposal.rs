//! Proposal types and structures
//!
//! This module defines the standardized proposal structures used across
//! all consensus implementations.

use crate::types::block::{Block, BlockHeader, Hash32, Hash64};
use crate::types::ConsensusType;
use crate::ProposerInfo;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Block proposal created by a proposer
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BlockProposal {
    /// Round/slot number for this proposal
    pub round_id: u64,
    /// Block height
    pub height: u64,
    /// Timestamp of proposal creation
    pub timestamp: u64,
    /// Proposer's public key
    pub proposer_pubkey: Hash32,
    /// Proposer's PoU score at time of election
    pub proposer_pou_score: u32, // basis points (0-10000)
    /// Parent block hash
    pub parent_hash: Hash64,
    /// State root after executing transactions
    pub state_root: Hash64,
    /// Transaction root (merkle root of transactions)
    pub tx_root: Hash64,
    /// Transactions included in this block
    pub transactions: Vec<ProposalTransaction>,
    /// Latency measurement proof
    pub latency_proof: Option<LatencyProofData>,
    /// Availability proof
    pub availability_proof: Option<AvailabilityProofData>,
    /// Group membership proof (for group-aware consensus)
    pub group_proof: Option<GroupProofData>,
    /// Proposer's signature over the proposal
    pub signature: Hash64,
    /// Proposal metadata
    pub metadata: ProposalMetadata,
}

/// Simplified transaction for proposal
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProposalTransaction {
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
}

/// Latency proof data included in block proposal.
///
/// AUDIT-003: RTT fields use microseconds (u64) instead of f64 milliseconds.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LatencyProofData {
    /// Round ID for this proof
    pub round_id: u64,
    /// Median RTT in microseconds (integer for cross-platform determinism)
    pub median_rtt_us: u64,
    /// Number of peers measured
    pub peer_count: u32,
    /// Individual RTT measurements
    pub rtt_measurements: Vec<RttMeasurement>,
    /// Proof signature
    pub signature: Hash64,
    /// Proof timestamp
    pub timestamp: u64,
}

/// Individual RTT measurement.
///
/// AUDIT-003: RTT uses microseconds (u64) instead of f64 milliseconds.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RttMeasurement {
    /// Peer ID
    pub peer_id: String,
    /// RTT in microseconds (integer for cross-platform determinism)
    pub rtt_us: u64,
    /// Measurement timestamp
    pub timestamp: u64,
}

/// Availability proof data.
///
/// AUDIT-003: uptime uses permille (u32) instead of f64 percentage.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AvailabilityProofData {
    /// Round ID for this proof
    pub round_id: u64,
    /// Uptime in permille (0–1000). AUDIT-003.
    pub uptime_permille: u32,
    /// Number of successful pings
    pub successful_pings: u32,
    /// Total number of pings
    pub total_pings: u32,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Proof signature
    pub signature: Hash64,
    /// Proof timestamp
    pub timestamp: u64,
}

/// Group membership proof data.
///
/// AUDIT-003: health_score uses permille (u32) instead of f64.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct GroupProofData {
    /// Group ID
    pub group_id: String,
    /// Group epoch
    pub epoch: u64,
    /// Group members
    pub members: Vec<String>,
    /// Proposer role in group
    pub is_proposer: bool,
    /// Group health score in permille (0–1000). AUDIT-003.
    pub health_score_permille: u32,
    /// Group signature
    pub group_signature: Hash64,
    /// Proof timestamp
    pub timestamp: u64,
}

/// Proposal metadata
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ProposalMetadata {
    /// Proposal version
    pub version: u32,
    /// Proposal type
    pub proposal_type: ProposalType,
    /// Additional data
    pub extra_data: Vec<u8>,
    /// Gas limit
    pub gas_limit: u64,
    /// Gas used
    pub gas_used: u64,
}

/// Proposal types
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum ProposalType {
    /// Standard block proposal
    #[default]
    Standard,
    /// Emergency proposal
    Emergency,
    /// Reorganization proposal
    Reorganization,
    /// Upgrade proposal
    Upgrade,
    /// Custom proposal type
    Custom(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProposalValidationResult {
    /// Whether the proposal is valid
    pub is_valid: bool,
    /// Validation score (0-10000)
    pub score: u32,
    /// Validation details
    pub details: Vec<ValidationDetail>,
    /// Validation timestamp
    pub timestamp: u64,
    /// Validator ID
    pub validator_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationDetail {
    /// Validation check name
    pub check: String,
    /// Whether the check passed
    pub passed: bool,
    /// Check result message
    pub message: String,
    /// Check execution time in microseconds (integer). AUDIT-003.
    pub execution_time_us: u64,
}

/// Proposal trait for common operations
pub trait Proposal: Send + Sync + std::any::Any {
    /// Get proposal hash
    fn hash(&self) -> [u8; 64];

    /// Get proposer information
    fn proposer_info(&self) -> ProposerInfo;

    /// Get proposal timestamp
    fn timestamp(&self) -> u64;

    /// Get proposal round ID
    fn round_id(&self) -> u64;

    /// Get proposal height
    fn height(&self) -> u64;

    /// Validate proposal structure
    fn validate_structure(&self) -> Result<(), ProposalValidationError>;

    /// Get proposal size
    fn size(&self) -> u64;

    /// Check if proposal is expired
    fn is_expired(&self, current_time: u64, timeout_ms: u64) -> bool;

    /// Get as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;

    /// Clone the proposal into a Box
    fn clone_box(&self) -> Box<dyn Proposal>;
}

impl Proposal for BlockProposal {
    fn hash(&self) -> [u8; 64] {
        // AUDIT: Return all-zeros sentinel if serialization fails instead of
        // hashing empty bytes (which would cause hash collisions).
        let data = match bincode::serialize(self) {
            Ok(d) => d,
            Err(_) => return [0u8; 64],
        };
        let hash = blake3::hash(&data);
        let mut result = [0u8; 64];
        result.copy_from_slice(hash.as_bytes());
        result
    }

    fn proposer_info(&self) -> ProposerInfo {
        // Return a new ProposerInfo instead of a reference
        ProposerInfo {
            node_id: "unknown".to_string(),
            peer_id: "unknown".to_string(),
            public_key: self.proposer_pubkey.0,
            score: self.proposer_pou_score,
            group_id: None,
            region: "unknown".to_string(),
            capabilities: Vec::new(),
        }
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }

    fn round_id(&self) -> u64 {
        self.round_id
    }

    fn height(&self) -> u64 {
        self.height
    }

    fn validate_structure(&self) -> Result<(), ProposalValidationError> {
        if self.height == 0 {
            return Err(ProposalValidationError::InvalidHeight);
        }

        if self.timestamp == 0 {
            return Err(ProposalValidationError::InvalidTimestamp);
        }

        if self.proposer_pou_score > 10000 {
            return Err(ProposalValidationError::InvalidScore);
        }

        if self.signature.0.iter().all(|&b| b == 0) {
            return Err(ProposalValidationError::InvalidSignature);
        }

        Ok(())
    }

    fn size(&self) -> u64 {
        bincode::serialized_size(self).unwrap_or(0)
    }

    fn is_expired(&self, current_time: u64, timeout_ms: u64) -> bool {
        let elapsed = current_time.saturating_sub(self.timestamp);
        elapsed > timeout_ms / 1000
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn clone_box(&self) -> Box<dyn Proposal> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProposalValidationError {
    /// Invalid block height
    InvalidHeight,
    /// Invalid timestamp
    InvalidTimestamp,
    /// Invalid proposer score
    InvalidScore,
    /// Invalid signature
    InvalidSignature,
    /// Invalid parent hash
    InvalidParentHash,
    /// Invalid state root
    InvalidStateRoot,
    /// Invalid transaction root
    InvalidTxRoot,
    /// Invalid transactions
    InvalidTransactions,
    /// Invalid latency proof
    InvalidLatencyProof,
    /// Invalid availability proof
    InvalidAvailabilityProof,
    /// Invalid group proof
    InvalidGroupProof,
    /// Proposal expired
    Expired,
    /// Custom error
    Custom(String),
}

impl std::fmt::Display for ProposalValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposalValidationError::InvalidHeight => write!(f, "Invalid block height"),
            ProposalValidationError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            ProposalValidationError::InvalidScore => write!(f, "Invalid proposer score"),
            ProposalValidationError::InvalidSignature => write!(f, "Invalid signature"),
            ProposalValidationError::InvalidParentHash => write!(f, "Invalid parent hash"),
            ProposalValidationError::InvalidStateRoot => write!(f, "Invalid state root"),
            ProposalValidationError::InvalidTxRoot => write!(f, "Invalid transaction root"),
            ProposalValidationError::InvalidTransactions => write!(f, "Invalid transactions"),
            ProposalValidationError::InvalidLatencyProof => write!(f, "Invalid latency proof"),
            ProposalValidationError::InvalidAvailabilityProof => {
                write!(f, "Invalid availability proof")
            }
            ProposalValidationError::InvalidGroupProof => write!(f, "Invalid group proof"),
            ProposalValidationError::Expired => write!(f, "Proposal expired"),
            ProposalValidationError::Custom(msg) => write!(f, "Custom error: {}", msg),
        }
    }
}

impl serde::Serialize for BlockProposal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("BlockProposal", 13)?;
        state.serialize_field("round_id", &self.round_id)?;
        state.serialize_field("height", &self.height)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("proposer_pubkey", &self.proposer_pubkey)?;
        state.serialize_field("proposer_pou_score", &self.proposer_pou_score)?;
        state.serialize_field("parent_hash", &self.parent_hash)?;
        state.serialize_field("state_root", &self.state_root)?;
        state.serialize_field("tx_root", &self.tx_root)?;
        state.serialize_field("transactions", &self.transactions)?;
        state.serialize_field("latency_proof", &self.latency_proof)?;
        state.serialize_field("availability_proof", &self.availability_proof)?;
        state.serialize_field("group_proof", &self.group_proof)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("metadata", &self.metadata)?;
        state.end()
    }
}

impl serde::Serialize for ProposalTransaction {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ProposalTransaction", 8)?;
        state.serialize_field("hash", &self.hash)?;
        state.serialize_field("from", &self.from)?;
        state.serialize_field("to", &self.to)?;
        state.serialize_field("amount", &self.amount)?;
        state.serialize_field("nonce", &self.nonce)?;
        state.serialize_field("fee", &self.fee)?;
        state.serialize_field("data", &self.data)?;
        state.serialize_field("signature", &self.signature)?;
        state.end()
    }
}

impl serde::Serialize for LatencyProofData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("LatencyProofData", 6)?;
        state.serialize_field("round_id", &self.round_id)?;
        state.serialize_field("median_rtt_us", &self.median_rtt_us)?;
        state.serialize_field("peer_count", &self.peer_count)?;
        state.serialize_field("rtt_measurements", &self.rtt_measurements)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl serde::Serialize for RttMeasurement {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("RttMeasurement", 3)?;
        state.serialize_field("peer_id", &self.peer_id)?;
        state.serialize_field("rtt_us", &self.rtt_us)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl serde::Serialize for AvailabilityProofData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("AvailabilityProofData", 6)?;
        state.serialize_field("round_id", &self.round_id)?;
        state.serialize_field("uptime_permille", &self.uptime_permille)?;
        state.serialize_field("successful_pings", &self.successful_pings)?;
        state.serialize_field("total_pings", &self.total_pings)?;
        state.serialize_field("last_seen", &self.last_seen)?;
        state.serialize_field("signature", &self.signature)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl serde::Serialize for GroupProofData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("GroupProofData", 7)?;
        state.serialize_field("group_id", &self.group_id)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("members", &self.members)?;
        state.serialize_field("is_proposer", &self.is_proposer)?;
        state.serialize_field("health_score_permille", &self.health_score_permille)?;
        state.serialize_field("group_signature", &self.group_signature)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.end()
    }
}

impl serde::Serialize for ProposalValidationResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ProposalValidationResult", 5)?;
        state.serialize_field("is_valid", &self.is_valid)?;
        state.serialize_field("score", &self.score)?;
        state.serialize_field("details", &self.details)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("validator_id", &self.validator_id)?;
        state.end()
    }
}

impl serde::Serialize for ValidationDetail {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ValidationDetail", 4)?;
        state.serialize_field("check", &self.check)?;
        state.serialize_field("passed", &self.passed)?;
        state.serialize_field("message", &self.message)?;
        state.serialize_field("execution_time_us", &self.execution_time_us)?;
        state.end()
    }
}
