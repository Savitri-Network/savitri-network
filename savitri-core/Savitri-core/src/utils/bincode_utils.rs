//! Unified bincode configuration for Savitri Network Core
//!
//! This module provides a single, canonical bincode configuration
//! that must be used throughout the entire codebase to ensure
//! binary compatibility and prevent encoding mismatches.

use bincode::Options;
use serde::{Serialize, Deserialize};

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
        .with_fixint_encoding()  // CRITICAL: Always use fixed-width integers
        .allow_trailing_bytes()
}

/// Convenience function for serialization
pub fn serialize_consensus<T: Serialize>(data: &T) -> anyhow::Result<Vec<u8>> {
    consensus_bincode()
        .serialize(data)
        .map_err(|e| anyhow::anyhow!("Failed to serialize consensus data: {}", e))
}

/// Convenience function for deserialization
pub fn deserialize_consensus<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    consensus_bincode()
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize consensus data: {}", e))
}

/// Bincode configuration for non-critical data (e.g., metrics, logs)
pub fn default_bincode() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
}

/// Serialize non-critical data
pub fn serialize_default<T: Serialize>(data: &T) -> anyhow::Result<Vec<u8>> {
    default_bincode()
        .serialize(data)
        .map_err(|e| anyhow::anyhow!("Failed to serialize data: {}", e))
}

/// Deserialize non-critical data
pub fn deserialize_default<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    default_bincode()
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize data: {}", e))
}

/// Calculate serialized size of data without allocating buffer
pub fn serialized_size<T: Serialize>(data: &T) -> anyhow::Result<usize> {
    let size = consensus_bincode()
        .serialized_size(data)
        .map_err(|e| anyhow::anyhow!("Failed to calculate serialized size: {}", e))?;
    Ok(size as usize)
}

/// Utility for checking if data can be deserialized without errors
pub fn can_deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> bool {
    consensus_bincode().deserialize::<T>(bytes).is_ok()
}

/// Serialize to hex string for debugging
pub fn serialize_to_hex<T: Serialize>(data: &T) -> anyhow::Result<String> {
    let bytes = serialize_consensus(data)?;
    Ok(hex::encode(bytes))
}

/// Deserialize from hex string for debugging
pub fn deserialize_from_hex<T: for<'de> Deserialize<'de>>(hex_str: &str) -> anyhow::Result<T> {
    let bytes = hex::decode(hex_str)
        .map_err(|e| anyhow::anyhow!("Failed to decode hex: {}", e))?;
    deserialize_consensus(&bytes)
}

/// Batch serialization utilities
pub mod batch {
    use super::*;

    /// Serialize multiple items efficiently
    pub fn serialize_batch<T: Serialize>(items: &[T]) -> anyhow::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        for item in items {
            let serialized = serialize_consensus(item)?;
            buffer.extend_from_slice(&serialized);
        }
        Ok(buffer)
    }

    /// Deserialize multiple items from a buffer
    pub fn deserialize_batch<T: for<'de> Deserialize<'de> + serde::Serialize>(
        bytes: &[u8],
        count: usize,
    ) -> anyhow::Result<Vec<T>> {
        let mut items = Vec::with_capacity(count);
        let mut offset = 0;
        
        for _ in 0..count {
            if offset >= bytes.len() {
                return Err(anyhow::anyhow!("Insufficient data for batch deserialization"));
            }
            
            let item = deserialize_consensus(&bytes[offset..])?;
            offset += serialized_size(&item)?;
            items.push(item);
        }
        
        Ok(items)
    }

    /// Stream deserialization (useful for large batches)
    pub fn deserialize_stream<T: for<'de> Deserialize<'de> + Serialize>(
        bytes: &[u8],
    ) -> impl Iterator<Item = anyhow::Result<T>> + '_ {
        let mut offset = 0;
        
        std::iter::from_fn(move || {
            if offset >= bytes.len() {
                return None;
            }
            
            match deserialize_consensus(&bytes[offset..]) {
                Ok(item) => {
                    // Try to get size for next iteration
                    match serialized_size(&item) {
                        Ok(size) => {
                            offset += size;
                            Some(Ok(item))
                        }
                        Err(_) => {
                            // If we can't get size, we can't continue
                            offset = bytes.len();
                            Some(Err(anyhow::anyhow!("Failed to get item size")))
                        }
                    }
                }
                Err(e) => {
                    offset = bytes.len();
                    Some(Err(e))
                }
            }
        })
    }
}

/// Compression utilities for serialized data
pub mod compression {
    use super::*;

    /// Compress serialized data using simple run-length encoding
    pub fn compress_rle(data: &[u8]) -> Vec<u8> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut compressed = Vec::new();
        let mut current = data[0];
        let mut count = 1u8;

        for &byte in &data[1..] {
            if byte == current && count < 255 {
                count += 1;
            } else {
                compressed.push(current);
                compressed.push(count);
                current = byte;
                count = 1;
            }
        }

        compressed.push(current);
        compressed.push(count);
        compressed
    }

    /// Decompress run-length encoded data
    pub fn decompress_rle(compressed: &[u8]) -> Vec<u8> {
        if compressed.is_empty() {
            return Vec::new();
        }

        let mut decompressed = Vec::new();
        
        for chunk in compressed.chunks(2) {
            if chunk.len() == 2 {
                let byte = chunk[0];
                let count = chunk[1];
                decompressed.extend(std::iter::repeat(byte).take(count as usize));
            }
        }

        decompressed
    }

    /// Serialize and compress in one step
    pub fn serialize_compressed<T: Serialize>(data: &T) -> anyhow::Result<Vec<u8>> {
        let serialized = serialize_consensus(data)?;
        Ok(compress_rle(&serialized))
    }

    /// Decompress and deserialize in one step
    pub fn deserialize_compressed<T: for<'de> Deserialize<'de>>(
        compressed: &[u8],
    ) -> anyhow::Result<T> {
        let decompressed = decompress_rle(compressed);
        deserialize_consensus(&decompressed)
    }
}

/// Version-aware serialization utilities
pub mod versioning {
    use super::*;

    /// Serialize with version prefix
    pub fn serialize_with_version<T: Serialize>(data: &T, version: u32) -> anyhow::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&version.to_le_bytes());
        let serialized = serialize_consensus(data)?;
        buffer.extend_from_slice(&serialized);
        Ok(buffer)
    }

    /// Deserialize with version check
    pub fn deserialize_with_version<T: for<'de> Deserialize<'de>>(
        bytes: &[u8],
        expected_version: u32,
    ) -> anyhow::Result<T> {
        if bytes.len() < 4 {
            return Err(anyhow::anyhow!("Insufficient data for version header"));
        }

        let version = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if version != expected_version {
            return Err(anyhow::anyhow!(
                "Version mismatch: expected {}, got {}",
                expected_version,
                version
            ));
        }

        deserialize_consensus(&bytes[4..])
    }

    /// Get version from serialized data
    pub fn get_version(bytes: &[u8]) -> Option<u32> {
        if bytes.len() < 4 {
            return None;
        }
        Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
        value: f64,
    }

    impl TestData {
        fn new(id: u64, name: &str, value: f64) -> Self {
            Self {
                id,
                name: name.to_string(),
                value,
            }
        }
    }

    #[test]
    fn test_basic_serialization() {
        let data = TestData::new(42, "test", 3.14);
        
        let serialized = serialize_consensus(&data).unwrap();
        let deserialized: TestData = deserialize_consensus(&serialized).unwrap();
        
        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_serialized_size() {
        let data = TestData::new(42, "test", 3.14);
        let size = serialized_size(&data).unwrap();
        assert!(size > 0);
    }

    #[test]
    fn test_can_deserialize() {
        let data = TestData::new(42, "test", 3.14);
        let serialized = serialize_consensus(&data).unwrap();
        
        assert!(can_deserialize::<TestData>(&serialized));
        assert!(!can_deserialize::<TestData>(&[1, 2, 3]));
    }

    #[test]
    fn test_hex_serialization() {
        let data = TestData::new(42, "test", 3.14);
        
        let hex_str = serialize_to_hex(&data).unwrap();
        let deserialized: TestData = deserialize_from_hex(&hex_str).unwrap();
        
        assert_eq!(data, deserialized);
    }

    #[test]
    fn test_batch_serialization() {
        let items = vec![
            TestData::new(1, "first", 1.0),
            TestData::new(2, "second", 2.0),
            TestData::new(3, "third", 3.0),
        ];
        
        let serialized = batch::serialize_batch(&items).unwrap();
        let deserialized = batch::deserialize_batch::<TestData>(&serialized, 3).unwrap();
        
        assert_eq!(items, deserialized);
    }

    #[test]
    fn test_stream_deserialization() {
        let items = vec![
            TestData::new(1, "first", 1.0),
            TestData::new(2, "second", 2.0),
        ];
        
        let serialized = batch::serialize_batch(&items).unwrap();
        let deserialized: Vec<TestData> = batch::deserialize_stream(&serialized).collect::<Result<Vec<_>, _>>().unwrap();
        
        assert_eq!(items, deserialized);
    }

    #[test]
    fn test_compression() {
        let data = TestData::new(42, "test", 3.14);
        
        let compressed = compression::serialize_compressed(&data).unwrap();
        let decompressed: TestData = compression::deserialize_compressed(&compressed).unwrap();
        
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_rle_compression() {
        let data = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
        let compressed = compression::compress_rle(&data);
        let decompressed = compression::decompress_rle(&compressed);
        
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_versioning() {
        let data = TestData::new(42, "test", 3.14);
        let version = 1u32;
        
        let serialized = versioning::serialize_with_version(&data, version).unwrap();
        let deserialized: TestData = versioning::deserialize_with_version(&serialized, version).unwrap();
        
        assert_eq!(data, deserialized);
        
        // Test wrong version
        let result: anyhow::Result<TestData> = versioning::deserialize_with_version(&serialized, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_version() {
        let data = TestData::new(42, "test", 3.14);
        let version = 1u32;
        
        let serialized = versioning::serialize_with_version(&data, version).unwrap();
        let extracted_version = versioning::get_version(&serialized).unwrap();
        
        assert_eq!(extracted_version, version);
    }

    #[test]
    fn test_default_bincode() {
        let data = TestData::new(42, "test", 3.14);
        
        let serialized = serialize_default(&data).unwrap();
        let deserialized: TestData = deserialize_default(&serialized).unwrap();
        
        assert_eq!(data, deserialized);
    }
}
