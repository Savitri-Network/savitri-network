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
// V0.2 Phase 1 (Score Canonicity, issue #31)
pub mod latency_canon;
pub mod latency_table;

// Re-export all types
pub use block::*;
pub use consensus::ConsensusType; // Explicit import to avoid ambiguity
pub use consensus::*;
pub use proposal::*;
pub use score::*;
pub use validation::*;
// V0.2 Phase 1 (Score Canonicity, issue #31) — wire format + table re-exports
pub use latency_canon::{
    bucket_from_rtt_ms, LatencyReport, PeerLatencyObservation, RTT_BUCKET_MAX, RTT_BUCKET_MS,
};
pub use latency_table::{LatencyTable, MIN_REPORTERS, MIN_SAMPLES, WINDOW_SIZE};
