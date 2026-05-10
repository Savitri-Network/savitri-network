#![allow(dead_code, unused_variables, unused_imports)]

//! Savitri Masternode Library
//!
//! This library provides the core functionality for Savitri Network masternode operations,

pub mod adaptive_batch_collector;
pub mod batch_collector;
pub mod block_messages;
pub mod bootstrap;
pub mod bridge;
pub mod config;
pub mod consensus_protocol;
pub mod error_handling;
pub mod gossipsub_ext;
pub mod group_consensus;
pub mod group_formation;
pub mod integration_tests;
pub mod libp2p_network;
pub mod masternode_p2p;
pub mod mempool_manager;
pub mod monolith_p2p;
pub mod monolith_producer;
pub mod monolith_storage;
pub mod performance;
pub mod proposal_validator;
pub mod retry_manager;
pub mod rewards;
pub mod signature_verifier;
pub mod transaction_validator;

#[cfg(feature = "storage")]
pub mod consensus_storage_adapter;

#[cfg(feature = "contracts")]
pub mod contract_executor;

#[cfg(feature = "rpc")]
pub mod rpc;

// Re-export main components
pub use config::MasternodeConfig;
pub use group_consensus::{BftGroupConfig, GroupConsensusManager};
pub use group_formation::GroupFormation;
pub use masternode_p2p::{LightnodeGroupAnnounce, MasternodeMessage};
pub use proposal_validator::ProposalValidator;
