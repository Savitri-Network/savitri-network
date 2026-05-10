//! Common types and structures for consensus operations
//!
//! This module defines the shared data structures used across all consensus
//! implementations, ensuring type safety and compatibility.

pub mod block;
pub mod consensus;
pub mod proposal;
pub mod score;
pub mod slashing;
pub mod validation;

// Re-export all types
pub use block::*;
pub use consensus::ConsensusType; // Explicit import to avoid ambiguity
pub use consensus::*;
pub use proposal::*;
pub use score::*;
pub use validation::*;
