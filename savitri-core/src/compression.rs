//! Compression utilities for Savitri Network
//!
//! This module provides compression and decompression functionality
//! for network messages and storage data.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Compression algorithms supported by Savitri
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Snappy compression (fast, moderate compression)
    Snappy,
    /// Zstandard compression (good compression ratio)
    Zstd,
    /// LZ4 compression (very fast, moderate compression)
    Lz4,
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::Snappy
    }
}

/// Compression configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// Algorithm to use
    pub algorithm: CompressionAlgorithm,
    /// Compression level (1-9, where applicable)
    pub level: Option<u8>,
    /// Minimum size threshold for compression
    pub min_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::default(),
            level: None,
            min_size: 1024, // Only compress data larger than 1KB
        }
    }
}

/// Compress data using the specified algorithm
pub fn compress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>> {
    if data.len() < config.min_size {
        return Ok(data.to_vec());
    }

    match config.algorithm {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Snappy => compress_snappy(data),
        CompressionAlgorithm::Zstd => compress_zstd(data, config.level),
        CompressionAlgorithm::Lz4 => compress_lz4(data, config.level),
    }
}

/// Decompress data using the specified algorithm
pub fn decompress(compressed_data: &[u8], algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
    match algorithm {
        CompressionAlgorithm::None => Ok(compressed_data.to_vec()),
        CompressionAlgorithm::Snappy => decompress_snappy(compressed_data),
        CompressionAlgorithm::Zstd => decompress_zstd(compressed_data),
        CompressionAlgorithm::Lz4 => decompress_lz4(compressed_data),
    }
}

/// Compress data using Snappy algorithm
#[cfg(feature = "snap")]
fn compress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = snap::raw::Encoder::new();
    encoder
        .compress_vec(data)
        .map_err(|e| anyhow::anyhow!("Snappy compression failed: {}", e))
}

/// Decompress data using Snappy algorithm
#[cfg(feature = "snap")]
fn decompress_snappy(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder
        .decompress_vec(compressed_data)
        .map_err(|e| anyhow::anyhow!("Snappy decompression failed: {}", e))
}

/// Compress data using Zstandard algorithm
#[cfg(feature = "zstd")]
fn compress_zstd(data: &[u8], level: Option<u8>) -> Result<Vec<u8>> {
    use zstd::encode_all;
    let level: i32 = level.unwrap_or(3).into();
    encode_all(data, level).map_err(|e| anyhow::anyhow!("Zstd compression failed: {}", e))
}

/// Decompress data using Zstandard algorithm
#[cfg(feature = "zstd")]
fn decompress_zstd(compressed_data: &[u8]) -> Result<Vec<u8>> {
    use zstd::decode_all;
    decode_all(compressed_data).map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))
}

/// Compress data using LZ4 algorithm
#[cfg(feature = "lz4")]
fn compress_lz4(data: &[u8], _level: Option<u8>) -> Result<Vec<u8>> {
    // `lz4 1.28` requires `(src, mode, prepend_size)`; we pass the default
    // mode and `prepend_size=true` so `decompress` can recover the original
    // length without an external hint.
    lz4::block::compress(data, None, true)
        .map_err(|e| anyhow::anyhow!("LZ4 compression failed: {}", e))
}

/// Decompress data using LZ4 algorithm
#[cfg(feature = "lz4")]
fn decompress_lz4(compressed_data: &[u8]) -> Result<Vec<u8>> {
    lz4::block::decompress(compressed_data, None)
        .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
}

// Fallback implementation when snap feature is not enabled
#[cfg(not(feature = "snap"))]
fn compress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    // Simple run-length encoding fallback
    let mut compressed = Vec::new();
    let mut i = 0;

    while i < data.len() {
        let current_byte = data[i];
        let mut count = 1;

        // Count consecutive identical bytes
        while i + count < data.len() && data[i + count] == current_byte && count < 255 {
            count += 1;
        }

        // If we have a run of 3 or more identical bytes, compress it
        if count >= 3 {
            compressed.push(0xFF); // Escape marker for compressed runs
            compressed.push(current_byte);
            compressed.push(count as u8);
        } else {
            // For short runs or non-repeating bytes, store as-is
            for _j in 0..count {
                compressed.push(current_byte);
            }
        }

        i += count;
    }

    // Only return compressed data if it's actually smaller
    if compressed.len() < data.len() {
        Ok(compressed)
    } else {
        Ok(data.to_vec())
    }
}

#[cfg(not(feature = "snap"))]
fn decompress_snappy(compressed_data: &[u8]) -> Result<Vec<u8>> {
    let mut decompressed = Vec::new();
    let mut i = 0;

    while i < compressed_data.len() {
        if compressed_data[i] == 0xFF && i + 2 < compressed_data.len() {
            // Found a compressed run
            let byte_value = compressed_data[i + 1];
            let count = compressed_data[i + 2] as usize;

            // Expand the run
            for _ in 0..count {
                decompressed.push(byte_value);
            }

            i += 3;
        } else {
            // Regular byte, copy as-is
            decompressed.push(compressed_data[i]);
            i += 1;
        }
    }

    Ok(decompressed)
}

#[cfg(not(feature = "zstd"))]
fn compress_zstd(_data: &[u8], _level: Option<u8>) -> Result<Vec<u8>> {
    Err(anyhow::anyhow!("Zstd compression not available"))
}

#[cfg(not(feature = "zstd"))]
fn decompress_zstd(_compressed_data: &[u8]) -> Result<Vec<u8>> {
    Err(anyhow::anyhow!("Zstd decompression not available"))
}

#[cfg(not(feature = "lz4"))]
fn compress_lz4(_data: &[u8], _level: Option<u8>) -> Result<Vec<u8>> {
    Err(anyhow::anyhow!("LZ4 compression not available"))
}

#[cfg(not(feature = "lz4"))]
fn decompress_lz4(_compressed_data: &[u8]) -> Result<Vec<u8>> {
    Err(anyhow::anyhow!("LZ4 decompression not available"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.algorithm, CompressionAlgorithm::Snappy);
        assert_eq!(config.min_size, 1024);
        assert_eq!(config.level, None);
    }

    #[test]
    fn test_no_compression() {
        let data = b"hello world";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::None,
            level: None,
            min_size: 0,
        };

        let compressed = compress(data, &config).unwrap();
        assert_eq!(compressed, data);

        let decompressed = decompress(&compressed, CompressionAlgorithm::None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_size_threshold() {
        let data = b"small";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Snappy,
            level: None,
            min_size: 100,
        };

        let compressed = compress(data, &config).unwrap();
        assert_eq!(compressed, data); // Should not compress due to size threshold
    }

    #[test]
    fn test_fallback_snappy_compression() {
        let data = b"aaaaabbbcccccddddddeeeee";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Snappy,
            level: None,
            min_size: 0,
        };

        let compressed = compress(data, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionAlgorithm::Snappy).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_fallback_snappy_no_compression_benefit() {
        let data = b"abcdef"; // No repeating bytes
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Snappy,
            level: None,
            min_size: 0,
        };

        let compressed = compress(data, &config).unwrap();
        let decompressed = decompress(&compressed, CompressionAlgorithm::Snappy).unwrap();
        assert_eq!(decompressed, data);
        assert_eq!(compressed, data); // Should return original if no compression benefit
    }
}
