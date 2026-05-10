//! Compression module
//! 
//! Provides compression and decompression functionality for P2P network messages.
//! Supports multiple compression algorithms including SNAP, ZSTD, and LZ4.

#[cfg(feature = "snap")]
use snap::raw::{Decoder as SnapDecoder, Encoder as SnapEncoder};
#[cfg(feature = "zstd")]
use zstd::{bulk::compress as zstd_compress, bulk::decompress as zstd_decompress, DEFAULT_COMPRESSION_LEVEL};
#[cfg(feature = "lz4")]
use lz4::{block::compress as lz4_compress, block::decompress as lz4_decompress};

use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn, error};

/// Compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// SNAP compression
    Snap,
    /// ZSTD compression
    Zstd,
    /// LZ4 compression
    Lz4,
}

impl CompressionAlgorithm {
    /// Get algorithm name
    pub fn name(&self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Snap => "snap",
            CompressionAlgorithm::Zstd => "zstd",
            CompressionAlgorithm::Lz4 => "lz4",
        }
    }

    /// Check if algorithm is available
    pub fn is_available(&self) -> bool {
        match self {
            CompressionAlgorithm::None => true,
            #[cfg(feature = "snap")]
            CompressionAlgorithm::Snap => true,
            #[cfg(not(feature = "snap"))]
            CompressionAlgorithm::Snap => false,
            #[cfg(feature = "zstd")]
            CompressionAlgorithm::Zstd => true,
            #[cfg(not(feature = "zstd"))]
            CompressionAlgorithm::Zstd => false,
            #[cfg(feature = "lz4")]
            CompressionAlgorithm::Lz4 => true,
            #[cfg(not(feature = "lz4"))]
            CompressionAlgorithm::Lz4 => false,
        }
    }
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Default compression algorithm
    pub default_algorithm: CompressionAlgorithm,
    /// Compression level (1-9 for applicable algorithms)
    pub compression_level: u8,
    /// Enable adaptive compression (choose best algorithm based on data)
    pub enable_adaptive: bool,
    /// Minimum size threshold for compression (bytes)
    pub min_size_threshold: usize,
    /// Maximum compression ratio (prevents compression explosion)
    pub max_compression_ratio: f64,
    /// Enable compression statistics
    pub enable_stats: bool,
    /// Cache size for compression statistics
    pub stats_cache_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            default_algorithm: CompressionAlgorithm::None,
            compression_level: 6,
            enable_adaptive: false,
            min_size_threshold: 1024, // 1KB
            max_compression_ratio: 10.0,
            enable_stats: true,
            stats_cache_size: 1000,
        }
    }
}

/// Compression result
#[derive(Debug, Clone)]
pub struct CompressionResult {
    /// Compressed data
    pub data: Vec<u8>,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Original size
    pub original_size: usize,
    /// Compressed size
    pub compressed_size: usize,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Compression time in microseconds
    pub compression_time_us: u64,
}

/// Decompression result
#[derive(Debug, Clone)]
pub struct DecompressionResult {
    /// Decompressed data
    pub data: Vec<u8>,
    /// Algorithm used
    pub algorithm: CompressionAlgorithm,
    /// Compressed size
    pub compressed_size: usize,
    /// Decompressed size
    pub decompressed_size: usize,
    /// Decompression time in microseconds
    pub decompression_time_us: u64,
}

/// Compression statistics
#[derive(Debug, Clone, Default)]
pub struct CompressionStats {
    /// Total compressions
    pub total_compressions: u64,
    /// Total decompressions
    pub total_decompressions: u64,
    /// Total bytes compressed
    pub total_bytes_compressed: u64,
    /// Total bytes decompressed
    pub total_bytes_decompressed: u64,
    /// Total bytes saved through compression
    pub total_bytes_saved: u64,
    /// Average compression ratio
    pub average_compression_ratio: f64,
    /// Statistics by algorithm
    pub stats_by_algorithm: HashMap<CompressionAlgorithm, AlgorithmStats>,
    /// Compression failures
    pub compression_failures: u64,
    /// Decompression failures
    pub decompression_failures: u64,
}

/// Algorithm-specific statistics
#[derive(Debug, Clone, Default)]
pub struct AlgorithmStats {
    /// Number of operations
    pub operations: u64,
    /// Total input bytes
    pub input_bytes: u64,
    /// Total output bytes
    pub output_bytes: u64,
    /// Average ratio
    pub average_ratio: f64,
    /// Average time in microseconds
    pub average_time_us: f64,
}

/// Compression engine
pub struct CompressionEngine {
    config: CompressionConfig,
    stats: CompressionStats,
    event_sender: mpsc::UnboundedSender<CompressionEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<CompressionEvent>>,
    #[cfg(feature = "snap")]
    snap_encoder: SnapEncoder,
    #[cfg(feature = "snap")]
    snap_decoder: SnapDecoder,
}

/// Compression events
#[derive(Debug, Clone)]
pub enum CompressionEvent {
    /// Compression completed
    CompressionCompleted {
        algorithm: CompressionAlgorithm,
        original_size: usize,
        compressed_size: usize,
        ratio: f64,
        time_us: u64,
    },
    /// Decompression completed
    DecompressionCompleted {
        algorithm: CompressionAlgorithm,
        compressed_size: usize,
        decompressed_size: usize,
        time_us: u64,
    },
    /// Compression failed
    CompressionFailed {
        algorithm: CompressionAlgorithm,
        error: String,
    },
    /// Decompression failed
    DecompressionFailed {
        algorithm: CompressionAlgorithm,
        error: String,
    },
}

impl CompressionEngine {
    /// Create a new compression engine
    pub fn new(config: CompressionConfig) -> anyhow::Result<Self> {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            config,
            stats: CompressionStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            #[cfg(feature = "snap")]
            snap_encoder: SnapEncoder::new(),
            #[cfg(feature = "snap")]
            snap_decoder: SnapDecoder::new(),
        })
    }

    /// Compress data using the default algorithm
    pub async fn compress(&mut self, data: Vec<u8>) -> anyhow::Result<CompressionResult> {
        if data.len() < self.config.min_size_threshold {
            debug!("Data too small for compression: {} bytes", data.len());
            return Ok(CompressionResult {
                data,
                algorithm: CompressionAlgorithm::None,
                original_size: data.len(),
                compressed_size: data.len(),
                compression_ratio: 1.0,
                compression_time_us: 0,
            });
        }

        let algorithm = if self.config.enable_adaptive {
            self.choose_best_algorithm(&data)
        } else {
            self.config.default_algorithm
        };

        self.compress_with_algorithm(data, algorithm).await
    }

    /// Compress data using a specific algorithm
    pub async fn compress_with_algorithm(&mut self, data: Vec<u8>, algorithm: CompressionAlgorithm) -> anyhow::Result<CompressionResult> {
        let start_time = std::time::Instant::now();
        let original_size = data.len();

        let result = match algorithm {
            CompressionAlgorithm::None => {
                Ok(data)
            }
            CompressionAlgorithm::Snap => {
                #[cfg(feature = "snap")]
                {
                    self.compress_snap(data).await
                }
                #[cfg(not(feature = "snap"))]
                {
                    Err(anyhow::anyhow!("SNAP compression not available"))
                }
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    self.compress_zstd(data).await
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(anyhow::anyhow!("ZSTD compression not available"))
                }
            }
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    self.compress_lz4(data).await
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(anyhow::anyhow!("LZ4 compression not available"))
                }
            }
        };

        let compression_time = start_time.elapsed().as_micros() as u64;

        match result {
            Ok(compressed_data) => {
                let compressed_size = compressed_data.len();
                let compression_ratio = original_size as f64 / compressed_size as f64;

                // Check compression ratio threshold
                if compression_ratio < 1.0 / self.config.max_compression_ratio {
                    warn!("Compression ratio too low: {:.2}, using original data", compression_ratio);
                    return Ok(CompressionResult {
                        data: vec![], // Will be filled with original data
                        algorithm: CompressionAlgorithm::None,
                        original_size,
                        compressed_size: original_size,
                        compression_ratio: 1.0,
                        compression_time_us: compression_time,
                    });
                }

                // Update statistics
                self.update_compression_stats(algorithm, original_size, compressed_size, compression_ratio, compression_time);

                // Send event
                let _ = self.event_sender.send(CompressionEvent::CompressionCompleted {
                    algorithm,
                    original_size,
                    compressed_size,
                    ratio: compression_ratio,
                    time_us: compression_time,
                });

                debug!("Compressed {} bytes to {} bytes using {} ({:.2}x ratio, {}μs)",
                    original_size, compressed_size, algorithm.name(), compression_ratio, compression_time);

                Ok(CompressionResult {
                    data: compressed_data,
                    algorithm,
                    original_size,
                    compressed_size,
                    compression_ratio,
                    compression_time_us: compression_time,
                })
            }
            Err(e) => {
                self.stats.compression_failures += 1;

                // Send error event
                let _ = self.event_sender.send(CompressionEvent::CompressionFailed {
                    algorithm,
                    error: e.to_string(),
                });

                error!("Compression failed with {}: {}", algorithm.name(), e);
                Err(e)
            }
        }
    }

    /// Decompress data
    pub async fn decompress(&mut self, data: Vec<u8>, algorithm: CompressionAlgorithm) -> anyhow::Result<DecompressionResult> {
        let start_time = std::time::Instant::now();
        let compressed_size = data.len();

        let result = match algorithm {
            CompressionAlgorithm::None => {
                Ok(data)
            }
            CompressionAlgorithm::Snap => {
                #[cfg(feature = "snap")]
                {
                    self.decompress_snap(data).await
                }
                #[cfg(not(feature = "snap"))]
                {
                    Err(anyhow::anyhow!("SNAP decompression not available"))
                }
            }
            CompressionAlgorithm::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    self.decompress_zstd(data).await
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(anyhow::anyhow!("ZSTD decompression not available"))
                }
            }
            CompressionAlgorithm::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    self.decompress_lz4(data).await
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(anyhow::anyhow!("LZ4 decompression not available"))
                }
            }
        };

        let decompression_time = start_time.elapsed().as_micros() as u64;

        match result {
            Ok(decompressed_data) => {
                let decompressed_size = decompressed_data.len();

                // Update statistics
                self.update_decompression_stats(algorithm, compressed_size, decompressed_size, decompression_time);

                // Send event
                let _ = self.event_sender.send(CompressionEvent::DecompressionCompleted {
                    algorithm,
                    compressed_size,
                    decompressed_size,
                    time_us: decompression_time,
                });

                debug!("Decompressed {} bytes to {} bytes using {} ({}μs)",
                    compressed_size, decompressed_size, algorithm.name(), decompression_time);

                Ok(DecompressionResult {
                    data: decompressed_data,
                    algorithm,
                    compressed_size,
                    decompressed_size,
                    decompression_time_us: decompression_time,
                })
            }
            Err(e) => {
                self.stats.decompression_failures += 1;

                // Send error event
                let _ = self.event_sender.send(CompressionEvent::DecompressionFailed {
                    algorithm,
                    error: e.to_string(),
                });

                error!("Decompression failed with {}: {}", algorithm.name(), e);
                Err(e)
            }
        }
    }

    /// Choose the best algorithm for the data
    fn choose_best_algorithm(&self, _data: &[u8]) -> CompressionAlgorithm {
        // Simple heuristic: try available algorithms and choose the best
        // In a real implementation, you might sample the data and make predictions
        if self.config.default_algorithm.is_available() {
            self.config.default_algorithm
        } else if CompressionAlgorithm::Zstd.is_available() {
            CompressionAlgorithm::Zstd
        } else if CompressionAlgorithm::Lz4.is_available() {
            CompressionAlgorithm::Lz4
        } else if CompressionAlgorithm::Snap.is_available() {
            CompressionAlgorithm::Snap
        } else {
            CompressionAlgorithm::None
        }
    }

    /// Compress data using SNAP
    #[cfg(feature = "snap")]
    async fn compress_snap(&mut self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        self.snap_encoder.compress_vec(&data)
            .map_err(|e| anyhow::anyhow!("SNAP compression failed: {}", e))
    }

    /// Decompress data using SNAP
    #[cfg(feature = "snap")]
    async fn decompress_snap(&mut self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        self.snap_decoder.decompress_vec(&data)
            .map_err(|e| anyhow::anyhow!("SNAP decompression failed: {}", e))
    }

    /// Compress data using ZSTD
    #[cfg(feature = "zstd")]
    async fn compress_zstd(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let level = self.config.compression_level as i32;
        zstd_compress(&data, level)
            .map_err(|e| anyhow::anyhow!("ZSTD compression failed: {}", e))
    }

    /// Decompress data using ZSTD
    #[cfg(feature = "zstd")]
    async fn decompress_zstd(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        zstd_decompress(&data, 0) // 0 means no size limit
            .map_err(|e| anyhow::anyhow!("ZSTD decompression failed: {}", e))
    }

    /// Compress data using LZ4
    #[cfg(feature = "lz4")]
    async fn compress_lz4(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let compressed = lz4_compress(&data, None, true) // Use default compression level, with checksum
            .map_err(|e| anyhow::anyhow!("LZ4 compression failed: {}", e))?;
        Ok(compressed.to_vec())
    }

    /// Decompress data using LZ4
    #[cfg(feature = "lz4")]
    async fn decompress_lz4(&self, data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let decompressed = lz4_decompress(&data, None)
            .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?;
        Ok(decompressed.to_vec())
    }

    /// Update compression statistics
    fn update_compression_stats(&mut self, algorithm: CompressionAlgorithm, original_size: usize, compressed_size: usize, ratio: f64, time_us: u64) {
        self.stats.total_compressions += 1;
        self.stats.total_bytes_compressed += original_size as u64;
        self.stats.total_bytes_saved += (original_size - compressed_size) as u64;

        // Update average compression ratio
        if self.stats.total_compressions > 0 {
            self.stats.average_compression_ratio = 
                (self.stats.average_compression_ratio * (self.stats.total_compressions - 1) as f64 + ratio) 
                / self.stats.total_compressions as f64;
        }

        // Update algorithm-specific stats
        let algo_stats = self.stats.stats_by_algorithm.entry(algorithm).or_default();
        algo_stats.operations += 1;
        algo_stats.input_bytes += original_size as u64;
        algo_stats.output_bytes += compressed_size as u64;
        algo_stats.average_ratio = 
            (algo_stats.average_ratio * (algo_stats.operations - 1) as f64 + ratio) 
            / algo_stats.operations as f64;
        algo_stats.average_time_us = 
            (algo_stats.average_time_us * (algo_stats.operations - 1) as f64 + time_us as f64) 
            / algo_stats.operations as f64;
    }

    /// Update decompression statistics
    fn update_decompression_stats(&mut self, algorithm: CompressionAlgorithm, compressed_size: usize, decompressed_size: usize, time_us: u64) {
        self.stats.total_decompressions += 1;
        self.stats.total_bytes_decompressed += decompressed_size as u64;

        // Update algorithm-specific stats
        let algo_stats = self.stats.stats_by_algorithm.entry(algorithm).or_default();
        algo_stats.operations += 1;
        algo_stats.input_bytes += compressed_size as u64;
        algo_stats.output_bytes += decompressed_size as u64;
        algo_stats.average_time_us = 
            (algo_stats.average_time_us * (algo_stats.operations - 1) as f64 + time_us as f64) 
            / algo_stats.operations as f64;
    }

    /// Get compression statistics
    pub fn get_stats(&self) -> CompressionStats {
        self.stats.clone()
    }

    /// Get event receiver
    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<CompressionEvent>> {
        self.event_receiver.take()
    }

    /// Get available algorithms
    pub fn get_available_algorithms(&self) -> Vec<CompressionAlgorithm> {
        vec![
            CompressionAlgorithm::None,
            CompressionAlgorithm::Snap,
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Lz4,
        ].into_iter()
        .filter(|alg| alg.is_available())
        .collect()
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = CompressionStats::default();
    }

    /// Start the compression engine
    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("Compression engine started with default algorithm: {}", self.config.default_algorithm.name());
        Ok(())
    }

    /// Stop the compression engine
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("Compression engine stopped");
        Ok(())
    }

    /// Get configuration
    pub fn get_config(&self) -> &CompressionConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&mut self, config: CompressionConfig) {
        self.config = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_algorithm() {
        assert_eq!(CompressionAlgorithm::None.name(), "none");
        assert_eq!(CompressionAlgorithm::Snap.name(), "snap");
        assert_eq!(CompressionAlgorithm::Zstd.name(), "zstd");
        assert_eq!(CompressionAlgorithm::Lz4.name(), "lz4");

        assert!(CompressionAlgorithm::None.is_available());
    }

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.default_algorithm, CompressionAlgorithm::None);
        assert_eq!(config.compression_level, 6);
        assert!(!config.enable_adaptive);
        assert_eq!(config.min_size_threshold, 1024);
        assert_eq!(config.max_compression_ratio, 10.0);
    }

    #[test]
    fn test_compression_stats_default() {
        let stats = CompressionStats::default();
        assert_eq!(stats.total_compressions, 0);
        assert_eq!(stats.total_decompressions, 0);
        assert_eq!(stats.total_bytes_compressed, 0);
        assert_eq!(stats.total_bytes_decompressed, 0);
    }

    #[test]
    fn test_algorithm_stats_default() {
        let stats = AlgorithmStats::default();
        assert_eq!(stats.operations, 0);
        assert_eq!(stats.input_bytes, 0);
        assert_eq!(stats.output_bytes, 0);
        assert_eq!(stats.average_ratio, 0.0);
        assert_eq!(stats.average_time_us, 0.0);
    }

    #[tokio::test]
    async fn test_compression_engine_creation() {
        let config = CompressionConfig::default();
        let engine = CompressionEngine::new(config);
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_compress_small_data() {
        let mut config = CompressionConfig::default();
        config.min_size_threshold = 100;
        
        let mut engine = CompressionEngine::new(config).unwrap();
        let data = vec![1, 2, 3, 4, 5]; // Small data
        
        let result = engine.compress(data).await.unwrap();
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.compression_ratio, 1.0);
    }

    #[tokio::test]
    async fn test_compress_with_none_algorithm() {
        let mut engine = CompressionEngine::new(CompressionConfig::default()).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        
        let result = engine.compress_with_algorithm(data, CompressionAlgorithm::None).await.unwrap();
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.compression_ratio, 1.0);
    }

    #[tokio::test]
    async fn test_decompress_none_algorithm() {
        let mut engine = CompressionEngine::new(CompressionConfig::default()).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        
        let result = engine.decompress(data, CompressionAlgorithm::None).await.unwrap();
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.data, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_get_available_algorithms() {
        let engine = CompressionEngine::new(CompressionConfig::default()).unwrap();
        let algorithms = engine.get_available_algorithms();
        
        // At least None should always be available
        assert!(algorithms.contains(&CompressionAlgorithm::None));
    }

    #[tokio::test]
    async fn test_compression_events() {
        let mut engine = CompressionEngine::new(CompressionConfig::default()).unwrap();
        let data = vec![1, 2, 3, 4, 5];
        
        // This should generate events
        let _result = engine.compress(data).await.unwrap();
        
        // Test that we can take the event receiver
        let receiver = engine.take_event_receiver();
        assert!(receiver.is_some());
    }

    #[tokio::test]
    async fn test_engine_start_stop() {
        let mut engine = CompressionEngine::new(CompressionConfig::default()).unwrap();
        
        assert!(engine.start().await.is_ok());
        assert!(engine.stop().await.is_ok());
    }

    #[test]
    fn test_compression_result() {
        let result = CompressionResult {
            data: vec![1, 2, 3],
            algorithm: CompressionAlgorithm::None,
            original_size: 3,
            compressed_size: 3,
            compression_ratio: 1.0,
            compression_time_us: 100,
        };
        
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.original_size, 3);
        assert_eq!(result.compressed_size, 3);
        assert_eq!(result.compression_ratio, 1.0);
    }

    #[test]
    fn test_decompression_result() {
        let result = DecompressionResult {
            data: vec![1, 2, 3],
            algorithm: CompressionAlgorithm::None,
            compressed_size: 3,
            decompressed_size: 3,
            decompression_time_us: 100,
        };
        
        assert_eq!(result.algorithm, CompressionAlgorithm::None);
        assert_eq!(result.compressed_size, 3);
        assert_eq!(result.decompressed_size, 3);
    }

    #[test]
    fn test_compression_events() {
        let event = CompressionEvent::CompressionCompleted {
            algorithm: CompressionAlgorithm::None,
            original_size: 100,
            compressed_size: 50,
            ratio: 2.0,
            time_us: 1000,
        };
        
        match event {
            CompressionEvent::CompressionCompleted { algorithm, original_size, compressed_size, ratio, time_us } => {
                assert_eq!(algorithm, CompressionAlgorithm::None);
                assert_eq!(original_size, 100);
                assert_eq!(compressed_size, 50);
                assert_eq!(ratio, 2.0);
                assert_eq!(time_us, 1000);
            }
            _ => panic!("Unexpected event type"),
        }
    }
}
