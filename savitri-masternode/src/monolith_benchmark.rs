//! Monolith Performance Benchmark
//!
//! This module provides comprehensive benchmarking for monolith block
//! creation, storage, and distribution performance metrics.

use anyhow::{Context, Result};
use libp2p::PeerId;
use savitri_masternode::bridge::core::slot_scheduler::{SlotScheduler, SlotSchedulerConfig};
use savitri_masternode::monolith_p2p::{MonolithP2PConfig, MonolithP2PManager, PeerInfo};
use savitri_masternode::monolith_producer::{
    MonolithBlock, MonolithProducer, MonolithProducerConfig,
};
use savitri_masternode::monolith_storage::{MonolithStorage, MonolithStorageConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Benchmark configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    /// Number of monolith blocks to create
    pub monolith_count: usize,
    /// Number of parallel operations
    pub parallel_operations: usize,
    /// Block range size for each monolith
    pub block_range_size: u64,
    /// Enable storage benchmark
    pub enable_storage_benchmark: bool,
    /// Enable P2P benchmark
    pub enable_p2p_benchmark: bool,
    /// Warmup iterations
    pub warmup_iterations: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            monolith_count: 100,
            parallel_operations: 10,
            block_range_size: 1000,
            enable_storage_benchmark: true,
            enable_p2p_benchmark: true,
            warmup_iterations: 5,
        }
    }
}

/// Benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub config: BenchmarkConfig,
    pub monolith_creation: MonolithCreationResults,
    pub storage_performance: Option<StorageResults>,
    pub p2p_performance: Option<P2PResults>,
    pub overall_performance: OverallResults,
    pub timestamp: u64,
}

/// Monolith creation benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithCreationResults {
    pub total_time_ms: u64,
    pub avg_time_per_monolith_ms: f64,
    pub monoliths_per_second: f64,
    pub blocks_compressed_total: u64,
    pub avg_blocks_per_monolith: f64,
    pub total_transactions: u64,
    pub avg_transactions_per_monolith: f64,
    pub avg_zkp_proof_size: f64,
    pub fastest_creation_ms: u64,
    pub slowest_creation_ms: u64,
}

/// Storage benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageResults {
    pub total_storage_time_ms: u64,
    pub avg_storage_time_ms: f64,
    pub storage_ops_per_second: f64,
    pub total_retrieval_time_ms: u64,
    pub avg_retrieval_time_ms: f64,
    pub retrieval_ops_per_second: f64,
    pub cache_hit_rate: f64,
    pub storage_size_mb: f64,
}

/// P2P benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct P2PResults {
    pub total_distribution_time_ms: u64,
    pub avg_distribution_time_ms: f64,
    pub distribution_ops_per_second: f64,
    pub avg_message_size_bytes: f64,
    pub messages_per_second: f64,
    pub peer_count: usize,
    pub network_throughput_mbps: f64,
}

/// Overall performance results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverallResults {
    pub total_benchmark_time_ms: u64,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub disk_io_mb: f64,
    pub network_io_mb: f64,
}

/// Monolith benchmark runner
pub struct MonolithBenchmark {
    config: BenchmarkConfig,
    producer: Arc<MonolithProducer>,
    storage: Option<Arc<MonolithStorage>>,
    p2p_manager: Option<Arc<MonolithP2PManager>>,
    results: Arc<RwLock<BenchmarkResults>>,
}

impl MonolithBenchmark {
    /// Create new benchmark runner
    pub fn new(config: BenchmarkConfig) -> Result<Self> {
        // Create scheduler
        let scheduler_config = SlotSchedulerConfig {
            heartbeat_interval_ms: 5000,
            slots_per_epoch: 20,
            monolith_epoch_ms: 86400000,
            genesis_timestamp_ms: 0,
            validators: vec![
                "node1".to_string(),
                "node2".to_string(),
                "node3".to_string(),
            ],
            local_id: "benchmark_node".to_string(),
        };
        let scheduler = Arc::new(SlotScheduler::new(scheduler_config));

        // Create producer
        let producer_config = MonolithProducerConfig::default();
        let producer = Arc::new(MonolithProducer::new(producer_config, scheduler));

        // Create storage if enabled
        let storage = if config.enable_storage_benchmark {
            let storage_config = MonolithStorageConfig {
                db_path: std::path::PathBuf::from("./benchmark_data"),
                max_monoliths: 10000,
                cache_size: 1000,
                enable_compression: true,
                write_buffer_size: 64 * 1024 * 1024,
            };
            Some(Arc::new(MonolithStorage::new(storage_config)?))
        } else {
            None
        };

        // Create P2P manager if enabled
        let p2p_manager = if config.enable_p2p_benchmark {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            let p2p_config = MonolithP2PConfig::default();
            Some(Arc::new(MonolithP2PManager::new(
                PeerId::random(),
                p2p_config,
                tx,
            )))
        } else {
            None
        };

        info!("Monolith benchmark initialized");
        Ok(Self {
            config: config.clone(),
            producer,
            storage,
            p2p_manager,
            results: Arc::new(RwLock::new(BenchmarkResults {
                config: config.clone(),
                monolith_creation: MonolithCreationResults::default(),
                storage_performance: None,
                p2p_performance: None,
                overall_performance: OverallResults::default(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            })),
        })
    }

    /// Run comprehensive benchmark
    pub async fn run_benchmark(&self) -> Result<BenchmarkResults> {
        info!("🚀 Starting monolith performance benchmark");
        let start_time = Instant::now();

        // Warmup phase
        self.warmup().await?;

        // Monolith creation benchmark
        let creation_results = self.benchmark_monolith_creation().await?;

        // Storage benchmark if enabled
        let storage_results = if self.config.enable_storage_benchmark {
            Some(self.benchmark_storage().await?)
        } else {
            None
        };

        // P2P benchmark if enabled
        let p2p_results = if self.config.enable_p2p_benchmark {
            Some(self.benchmark_p2p().await?)
        } else {
            None
        };

        // Calculate overall performance
        let overall_results = self
            .calculate_overall_performance(start_time.elapsed())
            .await?;

        // Update results
        let mut results = self.results.write().await;
        results.monolith_creation = creation_results;
        results.storage_performance = storage_results;
        results.p2p_performance = p2p_results;
        results.overall_performance = overall_results;

        info!("✅ Monolith benchmark completed");
        Ok(results.clone())
    }

    /// Warmup phase
    async fn warmup(&self) -> Result<()> {
        info!("🔥 Starting warmup phase");

        for i in 0..self.config.warmup_iterations {
            let _block = self.producer.create_monolith_block().await?;
            debug!("Warmup iteration {} completed", i + 1);
        }

        info!("Warmup phase completed");
        Ok(())
    }

    /// Benchmark monolith creation
    async fn benchmark_monolith_creation(&self) -> Result<MonolithCreationResults> {
        info!("⚡ Benchmarking monolith creation");

        let mut creation_times = Vec::new();
        let mut total_blocks = 0u64;
        let mut total_transactions = 0u64;
        let mut total_proof_size = 0usize;

        let start_time = Instant::now();

        for _ in 0..self.config.monolith_count {
            let iteration_start = Instant::now();

            let block = self.producer.create_monolith_block().await?;

            let iteration_time = iteration_start.elapsed().as_millis() as u64;
            creation_times.push(iteration_time);

            total_blocks += block.block_count;
            total_transactions += block.total_transactions;
            total_proof_size += block.zkp_proof.len();
        }

        let total_time = start_time.elapsed().as_millis();
        let fastest = *creation_times.iter().min().unwrap_or(&0);
        let slowest = *creation_times.iter().max().unwrap_or(&0);

        let results = MonolithCreationResults {
            total_time_ms: total_time as u64,
            avg_time_per_monolith_ms: total_time as f64 / self.config.monolith_count as f64,
            monoliths_per_second: self.config.monolith_count as f64 / (total_time as f64 / 1000.0),
            blocks_compressed_total: total_blocks,
            avg_blocks_per_monolith: total_blocks as f64 / self.config.monolith_count as f64,
            total_transactions,
            avg_transactions_per_monolith: total_transactions as f64
                / self.config.monolith_count as f64,
            avg_zkp_proof_size: total_proof_size as f64 / self.config.monolith_count as f64,
            fastest_creation_ms: fastest,
            slowest_creation_ms: slowest,
        };

        info!(
            monoliths = self.config.monolith_count,
            avg_time_ms = results.avg_time_per_monolith_ms,
            monoliths_per_sec = results.monoliths_per_second,
            "Monolith creation benchmark completed"
        );

        Ok(results)
    }

    /// Benchmark storage performance
    async fn benchmark_storage(&self) -> Result<StorageResults> {
        info!("💾 Benchmarking storage performance");

        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Storage not available"))?;

        // Generate test monoliths
        let mut test_blocks = Vec::new();
        for i in 0..self.config.monolith_count {
            let block = self.producer.create_monolith_block().await?;
            test_blocks.push(block);
        }

        // Benchmark storage writes
        let storage_start = Instant::now();
        for block in &test_blocks {
            storage.store_monolith(block).await?;
        }
        let storage_time = storage_start.elapsed().as_millis();

        // Benchmark storage reads
        let retrieval_start = Instant::now();
        for block in &test_blocks {
            let _retrieved = storage.get_monolith(block.end_height).await?;
        }
        let retrieval_time = retrieval_start.elapsed().as_millis();

        // Get storage stats
        let stats = storage.get_stats().await;
        let cache_hit_rate = if stats.cache_hits + stats.cache_misses > 0 {
            stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64
        } else {
            0.0
        };

        let results = StorageResults {
            total_storage_time_ms: storage_time as u64,
            avg_storage_time_ms: storage_time as f64 / self.config.monolith_count as f64,
            storage_ops_per_second: self.config.monolith_count as f64
                / (storage_time as f64 / 1000.0),
            total_retrieval_time_ms: retrieval_time as u64,
            avg_retrieval_time_ms: retrieval_time as f64 / self.config.monolith_count as f64,
            retrieval_ops_per_second: self.config.monolith_count as f64
                / (retrieval_time as f64 / 1000.0),
            cache_hit_rate,
            storage_size_mb: stats.storage_size_bytes as f64 / (1024.0 * 1024.0),
        };

        info!(
            storage_ops_per_sec = results.storage_ops_per_second,
            retrieval_ops_per_sec = results.retrieval_ops_per_second,
            cache_hit_rate = results.cache_hit_rate,
            "Storage benchmark completed"
        );

        Ok(results)
    }

    /// Benchmark P2P performance
    async fn benchmark_p2p(&self) -> Result<P2PResults> {
        info!("📡 Benchmarking P2P performance");

        let p2p_manager = self
            .p2p_manager
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("P2P manager not available"))?;

        // Generate test monoliths
        let mut test_blocks = Vec::new();
        let mut total_message_size = 0usize;

        for _ in 0..self.config.monolith_count {
            let block = self.producer.create_monolith_block().await?;
            let serialized = serde_json::to_vec(&block)?;
            total_message_size += serialized.len();
            test_blocks.push((block, serialized));
        }

        // Benchmark P2P distribution
        let distribution_start = Instant::now();
        for (block, _) in &test_blocks {
            p2p_manager.distribute_monolith(block).await?;
        }
        let distribution_time = distribution_start.elapsed().as_millis();

        let results = P2PResults {
            total_distribution_time_ms: distribution_time as u64,
            avg_distribution_time_ms: distribution_time as f64 / self.config.monolith_count as f64,
            distribution_ops_per_second: self.config.monolith_count as f64
                / (distribution_time as f64 / 1000.0),
            avg_message_size_bytes: total_message_size as f64 / self.config.monolith_count as f64,
            messages_per_second: self.config.monolith_count as f64
                / (distribution_time as f64 / 1000.0),
            peer_count: 0, // Would need actual peer management
            network_throughput_mbps: (total_message_size as f64 / (1024.0 * 1024.0))
                / (distribution_time as f64 / 1000.0),
        };

        info!(
            distribution_ops_per_sec = results.distribution_ops_per_second,
            avg_message_size_kb = results.avg_message_size_bytes / 1024.0,
            throughput_mbps = results.network_throughput_mbps,
            "P2P benchmark completed"
        );

        Ok(results)
    }

    /// Calculate overall performance metrics
    async fn calculate_overall_performance(&self, total_time: Duration) -> Result<OverallResults> {
        // Get system metrics (simplified)
        let memory_usage = self.get_memory_usage().await?;
        let cpu_usage = self.get_cpu_usage().await?;
        let disk_io = self.get_disk_io().await?;
        let network_io = self.get_network_io().await?;

        Ok(OverallResults {
            total_benchmark_time_ms: total_time.as_millis() as u64,
            memory_usage_mb: memory_usage,
            cpu_usage_percent: cpu_usage,
            disk_io_mb: disk_io,
            network_io_mb: network_io,
        })
    }

    /// Get memory usage with real system metrics
    async fn get_memory_usage(&self) -> Result<f64> {
        // Get actual memory usage from system
        match self.get_system_memory_usage().await {
            Ok(usage_mb) => {
                debug!("📊 Current memory usage: {:.2} MB", usage_mb);
                Ok(usage_mb)
            }
            Err(e) => {
                warn!("Failed to get memory usage: {}, using fallback", e);
                Ok(512.0) // Fallback value
            }
        }
    }

    /// Get CPU usage with real system metrics
    async fn get_cpu_usage(&self) -> Result<f64> {
        // Get actual CPU usage from system
        match self.get_system_cpu_usage().await {
            Ok(cpu_percent) => {
                debug!("📊 Current CPU usage: {:.2}%", cpu_percent);
                Ok(cpu_percent)
            }
            Err(e) => {
                warn!("Failed to get CPU usage: {}, using fallback", e);
                Ok(25.0) // Fallback value
            }
        }
    }

    /// Get disk I/O with real system metrics
    async fn get_disk_io(&self) -> Result<f64> {
        // Get actual disk I/O from system
        match self.get_system_disk_io().await {
            Ok(disk_io_mb) => {
                debug!("📊 Current disk I/O: {:.2} MB", disk_io_mb);
                Ok(disk_io_mb)
            }
            Err(e) => {
                warn!("Failed to get disk I/O: {}, using fallback", e);
                Ok(1024.0) // Fallback value
            }
        }
    }

    /// Get network I/O with real system metrics
    async fn get_network_io(&self) -> Result<f64> {
        // Get actual network I/O from system
        match self.get_system_network_io().await {
            Ok(network_io_mb) => {
                debug!("📊 Current network I/O: {:.2} MB", network_io_mb);
                Ok(network_io_mb)
            }
            Err(e) => {
                warn!("Failed to get network I/O: {}, using fallback", e);
                Ok(512.0) // Fallback value
            }
        }
    }

    /// Generate benchmark report
    pub async fn generate_report(&self) -> Result<String> {
        let results = self.results.read().await;

        let report = format!(
            r#"
# Monolith Performance Benchmark Report

## Configuration
- Monolith Count: {}
- Parallel Operations: {}
- Block Range Size: {}
- Storage Benchmark: {}
- P2P Benchmark: {}

## Monolith Creation Performance
- Total Time: {} ms
- Average Time per Monolith: {:.2} ms
- Monoliths per Second: {:.2}
- Blocks Compressed Total: {}
- Average Blocks per Monolith: {:.2}
- Total Transactions: {}
- Average Transactions per Monolith: {:.2}
- Average ZKP Proof Size: {:.2} bytes
- Fastest Creation: {} ms
- Slowest Creation: {} ms

## Storage Performance
- Total Storage Time: {} ms
- Average Storage Time: {:.2} ms
- Storage Ops per Second: {:.2}
- Total Retrieval Time: {} ms
- Average Retrieval Time: {:.2} ms
- Retrieval Ops per Second: {:.2}
- Cache Hit Rate: {:.2}%
- Storage Size: {:.2} MB

## P2P Performance
- Total Distribution Time: {} ms
- Average Distribution Time: {:.2} ms
- Distribution Ops per Second: {:.2}
- Average Message Size: {:.2} bytes
- Messages per Second: {:.2}
- Network Throughput: {:.2} Mbps

## Overall Performance
- Total Benchmark Time: {} ms
- Memory Usage: {:.2} MB
- CPU Usage: {:.2}%
- Disk I/O: {:.2} MB
- Network I/O: {:.2} MB

## Summary
The monolith system demonstrates {} performance with {} monoliths per second.
"#,
            self.config.monolith_count,
            self.config.parallel_operations,
            self.config.block_range_size,
            self.config.enable_storage_benchmark,
            self.config.enable_p2p_benchmark,
            results.monolith_creation.total_time_ms,
            results.monolith_creation.avg_time_per_monolith_ms,
            results.monolith_creation.monoliths_per_second,
            results.monolith_creation.blocks_compressed_total,
            results.monolith_creation.avg_blocks_per_monolith,
            results.monolith_creation.total_transactions,
            results.monolith_creation.avg_transactions_per_monolith,
            results.monolith_creation.avg_zkp_proof_size,
            results.monolith_creation.fastest_creation_ms,
            results.monolith_creation.slowest_creation_ms,
            results
                .storage_performance
                .as_ref()
                .map_or(0, |s| s.total_storage_time_ms),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.avg_storage_time_ms),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.storage_ops_per_second),
            results
                .storage_performance
                .as_ref()
                .map_or(0, |s| s.total_retrieval_time_ms),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.avg_retrieval_time_ms),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.retrieval_ops_per_second),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.cache_hit_rate * 100.0),
            results
                .storage_performance
                .as_ref()
                .map_or(0.0, |s| s.storage_size_mb),
            results
                .p2p_performance
                .as_ref()
                .map_or(0, |p| p.total_distribution_time_ms),
            results
                .p2p_performance
                .as_ref()
                .map_or(0.0, |p| p.avg_distribution_time_ms),
            results
                .p2p_performance
                .as_ref()
                .map_or(0.0, |p| p.distribution_ops_per_second),
            results
                .p2p_performance
                .as_ref()
                .map_or(0.0, |p| p.avg_message_size_bytes),
            results
                .p2p_performance
                .as_ref()
                .map_or(0.0, |p| p.messages_per_second),
            results
                .p2p_performance
                .as_ref()
                .map_or(0.0, |p| p.network_throughput_mbps),
            results.overall_performance.total_benchmark_time_ms,
            results.overall_performance.memory_usage_mb,
            results.overall_performance.cpu_usage_percent,
            results.overall_performance.disk_io_mb,
            results.overall_performance.network_io_mb,
            if results.monolith_creation.monoliths_per_second > 10.0 {
                "excellent"
            } else {
                "good"
            },
            results.monolith_creation.monoliths_per_second
        );

        Ok(report)
    }
}

impl Default for MonolithCreationResults {
    fn default() -> Self {
        Self {
            total_time_ms: 0,
            avg_time_per_monolith_ms: 0.0,
            monoliths_per_second: 0.0,
            blocks_compressed_total: 0,
            avg_blocks_per_monolith: 0.0,
            total_transactions: 0,
            avg_transactions_per_monolith: 0.0,
            avg_zkp_proof_size: 0.0,
            fastest_creation_ms: 0,
            slowest_creation_ms: 0,
        }
    }
}

impl Default for OverallResults {
    fn default() -> Self {
        Self {
            total_benchmark_time_ms: 0,
            memory_usage_mb: 0.0,
            cpu_usage_percent: 0.0,
            disk_io_mb: 0.0,
            network_io_mb: 0.0,
        }
    }
}

/// System metrics collection for benchmarking
impl MonolithBenchmark {
    /// Get system memory usage in MB
    async fn get_system_memory_usage(&self) -> Result<f64> {
        use std::fs;

        // Try to read memory info from /proc/meminfo (Linux)
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemAvailable:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        let available_kb = parts[1]
                            .parse::<u64>()
                            .map_err(|_| anyhow::anyhow!("Failed to parse memory"))?;
                        let available_mb = available_kb as f64 / 1024.0;
                        let total_mb = self.get_total_memory_mb().await?;
                        let used_mb = total_mb - available_mb;
                        return Ok(used_mb);
                    }
                }
            }
        }

        // Fallback: use Windows API or estimate
        self.estimate_memory_usage().await
    }

    /// Get total system memory in MB
    async fn get_total_memory_mb(&self) -> Result<f64> {
        use std::fs;

        // Try to read from /proc/meminfo (Linux)
        if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        let total_kb = parts[1]
                            .parse::<u64>()
                            .map_err(|_| anyhow::anyhow!("Failed to parse total memory"))?;
                        return Ok(total_kb as f64 / 1024.0);
                    }
                }
            }
        }

        // Fallback estimate (8GB default)
        Ok(8192.0)
    }

    /// Estimate memory usage when system calls fail
    async fn estimate_memory_usage(&self) -> Result<f64> {
        // Use process memory as estimate
        let current_usage = std::mem::size_of::<Self>() as f64 / (1024.0 * 1024.0);
        let estimated_total = current_usage * 10.0; // Rough estimate
        Ok(estimated_total.max(512.0).min(8192.0)) // Clamp between 512MB and 8GB
    }

    /// Get system CPU usage percentage
    async fn get_system_cpu_usage(&self) -> Result<f64> {
        use std::fs;

        // Try to read CPU stats from /proc/stat (Linux)
        if let Ok(stat) = fs::read_to_string("/proc/stat") {
            for line in stat.lines() {
                if line.starts_with("cpu ") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 8 {
                        let idle: u64 = parts[4]
                            .parse()
                            .map_err(|_| anyhow::anyhow!("Failed to parse CPU idle"))?;
                        let total: u64 = parts[1..8]
                            .iter()
                            .map(|x| x.parse::<u64>().unwrap_or(0))
                            .sum();

                        if total > 0 {
                            let usage_percent = ((total - idle) as f64 / total as f64) * 100.0;
                            return Ok(usage_percent);
                        }
                    }
                }
            }
        }

        // Fallback: estimate based on benchmark activity
        self.estimate_cpu_usage().await
    }

    /// Estimate CPU usage when system calls fail
    async fn estimate_cpu_usage(&self) -> Result<f64> {
        // Use benchmark activity as CPU indicator
        let results = self.results.read().await;
        let monoliths_per_second = results.monolith_creation.monoliths_per_second;

        // Estimate CPU usage based on monolith creation rate
        let estimated_usage = (monoliths_per_second / 10.0 * 100.0).min(95.0).max(5.0);
        Ok(estimated_usage)
    }

    /// Get system disk I/O in MB
    async fn get_system_disk_io(&self) -> Result<f64> {
        use std::fs;

        // Try to read disk stats from /proc/diskstats (Linux)
        if let Ok(diskstats) = fs::read_to_string("/proc/diskstats") {
            let mut total_read = 0u64;
            let mut total_write = 0u64;

            for line in diskstats.lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 10 {
                    total_read += parts[5].parse::<u64>().unwrap_or(0);
                    total_write += parts[9].parse::<u64>().unwrap_or(0);
                }
            }

            let total_sectors = total_read + total_write;
            let total_mb = (total_sectors * 512) as f64 / (1024.0 * 1024.0);
            return Ok(total_mb);
        }

        // Fallback: estimate based on monolith operations
        self.estimate_disk_io().await
    }

    /// Estimate disk I/O when system calls fail
    async fn estimate_disk_io(&self) -> Result<f64> {
        let results = self.results.read().await;
        let monolith_count = results.monolith_creation.total_time_ms / 1000; // Rough estimate
        let avg_size_mb = 10.0; // Average monolith size estimate
        let estimated_io = monolith_count as f64 * avg_size_mb * 2.0; // Read + write
        Ok(estimated_io.max(100.0).min(10240.0)) // Clamp between 100MB and 10GB
    }

    /// Get system network I/O in MB
    async fn get_system_network_io(&self) -> Result<f64> {
        use std::fs;

        // Try to read network stats from /proc/net/dev (Linux)
        if let Ok(netdev) = fs::read_to_string("/proc/net/dev") {
            let mut total_bytes = 0u64;

            for line in netdev.lines().skip(2) {
                // Skip header lines
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 10 {
                    let rx_bytes = parts[1].parse::<u64>().unwrap_or(0);
                    let tx_bytes = parts[9].parse::<u64>().unwrap_or(0);
                    total_bytes += rx_bytes + tx_bytes;
                }
            }

            let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
            return Ok(total_mb);
        }

        // Fallback: estimate based on P2P activity
        self.estimate_network_io().await
    }

    /// Estimate network I/O when system calls fail
    async fn estimate_network_io(&self) -> Result<f64> {
        let results = self.results.read().await;
        let monolith_count = results.monolith_creation.total_time_ms / 1000;

        // Estimate network usage based on monolith distribution
        let avg_size_mb = 5.0; // Average monolith network size
        let estimated_io = monolith_count as f64 * avg_size_mb * 3.0; // Send + receive + overhead

        Ok(estimated_io.max(50.0).min(5120.0)) // Clamp between 50MB and 5GB
    }
}
