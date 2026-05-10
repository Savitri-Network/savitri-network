//! P2P Message Types for Savitri Core
//!
//! This module contains P2P message types without creating a dependency cycle.
//! These are the core message structures that both core and p2p modules can use.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

/// Consensus certificate message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusCertificate {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub validator_signature: [u8; 64],
    pub timestamp: u64,
}

/// Consensus proposal message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusProposal {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub proposer: [u8; 32],
    pub transactions_hash: [u8; 32],
    pub timestamp: u64,
}

impl ConsensusProposal {
    /// Get the estimated size in bytes
    pub fn estimated_size(&self) -> usize {
        // Base size: 64 (block_hash) + 32 (proposer) + 32 (transactions_hash) + 8 (timestamp) = 136 bytes
        136
    }
}

/// Consensus vote message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusVote {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub validator: [u8; 32],
    pub vote: bool, // true for approve, false for reject
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    pub timestamp: u64,
}

impl ConsensusVote {
    /// Get the estimated size in bytes
    pub fn estimated_size(&self) -> usize {
        169
    }
}

/// Message size constants
pub const MAX_PROPOSAL_BYTES: usize = 1024 * 64; // 64 KB
pub const MAX_TX_BYTES: usize = 1024 * 128; // 128 KB
pub const MAX_VOTE_BYTES: usize = 1024; // 1 KB

/// Consensus message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    Proposal(ConsensusProposal),
    Vote(ConsensusVote),
    Certificate(ConsensusCertificate),
}

impl ConsensusMessage {
    /// Get the message size in bytes
    pub fn size(&self) -> usize {
        match self {
            ConsensusMessage::Proposal(_) => MAX_PROPOSAL_BYTES,
            ConsensusMessage::Vote(_) => MAX_VOTE_BYTES,
            ConsensusMessage::Certificate(_) => MAX_PROPOSAL_BYTES,
        }
    }

    /// Validate message format
    pub fn validate(&self) -> Result<(), String> {
        match self {
            ConsensusMessage::Proposal(proposal) => {
                if proposal.block_hash == [0u8; 64] {
                    return Err("Invalid block hash".to_string());
                }
                if proposal.proposer == [0u8; 32] {
                    return Err("Invalid proposer".to_string());
                }
                Ok(())
            }
            ConsensusMessage::Vote(vote) => {
                if vote.block_hash == [0u8; 64] {
                    return Err("Invalid block hash".to_string());
                }
                if vote.validator == [0u8; 32] {
                    return Err("Invalid validator".to_string());
                }
                if vote.signature == [0u8; 64] {
                    return Err("Invalid signature".to_string());
                }
                Ok(())
            }
            ConsensusMessage::Certificate(cert) => {
                if cert.block_hash == [0u8; 64] {
                    return Err("Invalid block hash".to_string());
                }
                if cert.validator_signature == [0u8; 64] {
                    return Err("Invalid validator signature".to_string());
                }
                Ok(())
            }
        }
    }
}
