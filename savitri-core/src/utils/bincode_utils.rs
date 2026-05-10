//! Unified bincode configuration for Savitri Network
//!
//! This module provides a single, canonical bincode configuration
//! that must be used throughout the entire codebase to ensure
//! binary compatibility and prevent encoding mismatches.
//!
//! # Configuration
//!
//! - Fixed-width integer encoding (not variable-length)
//! - Little endian byte order (compatible with serde_big_array)
//! - Allow trailing bytes (for future compatibility)
//!
//! # Usage
//!
//! ```rust
//! use savitri_node::bincode_utils::consensus_bincode;
//!
//! // Serialize
//! let bytes = consensus_bincode().serialize(&data)?;
//!
//! // Deserialize
//! let data: MyType = consensus_bincode().deserialize(&bytes)?;
//! ```

use bincode::Options;

/// Canonical bincode configuration for all consensus-critical data
///
/// This configuration MUST be used for:
/// - ConsensusCertificate
/// - Block persistence
/// - All storage operations
/// - Network messages
/// - Any data that participates in consensus
///
/// Configuration:
/// - Fixed-width integers (deterministic size)
/// - Little endian (default, compatible with serde_big_array)
/// - Allow trailing bytes (forward compatibility)
///
/// NOTE: Changed from big endian to little endian because serde_big_array
/// is not compatible with big endian byte order during deserialization.
///
/// CRITICAL: Fixed-width encoding prevents variable-length integer serialization
/// which was causing "unexpected end of file" errors in MonolithHeader.
pub fn consensus_bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding() // CRITICAL: Always use fixed-width integers
        .allow_trailing_bytes()
}

/// Convenience function for serialization
pub fn serialize_consensus<T: serde::Serialize>(data: &T) -> anyhow::Result<Vec<u8>> {
    consensus_bincode()
        .serialize(data)
        .map_err(|e| anyhow::anyhow!("Failed to serialize consensus data: {}", e))
}

/// Convenience function for deserialization
pub fn deserialize_consensus<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    consensus_bincode()
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize consensus data: {}", e))
}
