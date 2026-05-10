//! Savitri Contracts: Smart contracts, governance, and oracle framework
//!
//! This crate provides:
//! - Smart contracts platform with runtime and storage
//! - Decentralized governance system (DAO)
//! - Oracle framework for external data feeds
//! - Standard token (SAVITRI-20, SAVITRI-721, SAVITRI-1155)
//! - Federated Learning contracts
#![allow(clippy::needless_option_as_deref)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::doc_overindented_list_items)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::default_constructed_unit_structs)]
#![allow(clippy::manual_saturating_arithmetic)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::identity_op)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::unnecessary_map_or)]
#![allow(clippy::ptr_arg)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::unnecessary_min_or_max)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::unnecessary_fallible_conversions)]
#![allow(clippy::manual_div_ceil)]
#![allow(clippy::manual_strip)]
#![allow(clippy::len_zero)]
#![allow(clippy::absurd_extreme_comparisons)]
#![allow(clippy::let_and_return)]
#![allow(clippy::new_without_default)]
#![allow(ambiguous_glob_reexports)]
#![allow(deprecated)]
#![allow(unused_mut)]
#![allow(unused_variables)]
#![allow(unused_comparisons)]

pub mod connectors;
pub mod fee;
pub mod p2p;
pub mod storage;

#[cfg(feature = "governance")]
pub mod governance;

#[cfg(feature = "oracle")]
pub mod oracle;

pub mod contracts;

// Re-export commonly used types
pub use contracts::{BaseContract, CallTransaction, DeployTransaction, Runtime};

#[cfg(feature = "governance")]
pub use governance::{Proposal, ProposalAction, ProposalStatus, VoteToken, VotingSystem};

#[cfg(feature = "oracle")]
pub use oracle::{Feed, FeedData, FeedId, OracleProof, ProofVerifier, Schema, SchemaRegistry};
