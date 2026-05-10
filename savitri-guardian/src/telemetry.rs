use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use metrics::gauge;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;
use savitri_storage::Storage;
use sysinfo::System;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static PROMETHEUS_HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();
static METRICS_COLLECTOR: OnceCell<Arc<MetricsCollector>> = OnceCell::new();
static ARCHIVE_MONOLITH_COUNT: std::sync::Mutex<u64> = std::sync::Mutex::new(0);
pub const METRICS_LISTEN_ADDR: &str = "0.0.0.0:9898";

/// Metrics collector for system and archive monitoring
#[derive(Debug, Default)]
pub struct MetricsCollector {
    system_metrics: std::sync::Mutex<SystemMetrics>,
    storage_metrics: std::sync::Mutex<StorageMetrics>,
    block_metrics: std::sync::Mutex<BlockMetrics>,
}

/// System-related metrics
#[derive(Debug, Default, Clone)]
pub struct SystemMetrics {
    pub system_cpu_usage_percent: f64,
    pub system_cpu_cores: u32,
    pub system_memory_total_bytes: u64,
    pub system_memory_used_bytes: u64,
    pub system_memory_free_bytes: u64,
    pub system_memory_available_bytes: u64,
    pub system_memory_usage_percent: f64,
    pub process_uptime_seconds: u64,
    pub process_memory_rss_bytes: u64,
    pub process_threads: u32,
}

/// Storage-related metrics
#[derive(Debug, Default, Clone)]
pub struct StorageMetrics {
    pub storage_size_bytes: u64,
    pub storage_blocks_count: u64,
}

/// Block-related metrics
#[derive(Debug, Default, Clone)]
pub struct BlockMetrics {
    pub chain_height: u64,
}

/// Archive metrics from storage
#[derive(Debug, Default)]
pub struct ArchiveMetrics {
    pub size_bytes: u64,
    pub block_count: u64,
    pub monolith_count: u64,
    pub chain_height: Option<u64>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Update system metrics
    pub fn update_system<F>(&self, updater: F)
    where
        F: FnOnce(&mut SystemMetrics),
    {
        if let Ok(mut metrics) = self.system_metrics.lock() {
            updater(&mut *metrics);
        }
    }

    /// Update storage metrics
    pub fn update_storage<F>(&self, updater: F)
    where
        F: FnOnce(&mut StorageMetrics),
    {
        if let Ok(mut metrics) = self.storage_metrics.lock() {
            updater(&mut *metrics);
        }
    }

    /// Update block metrics
    pub fn update_block<F>(&self, updater: F)
    where
        F: FnOnce(&mut BlockMetrics),
    {
        if let Ok(mut metrics) = self.block_metrics.lock() {
            updater(&mut *metrics);
        }
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            system: self
                .system_metrics
                .lock()
                .map(|g| g.clone())
                .unwrap_or_default(),
            storage: self
                .storage_metrics
                .lock()
                .map(|g| g.clone())
                .unwrap_or_default(),
            block: self
                .block_metrics
                .lock()
                .map(|g| g.clone())
                .unwrap_or_default(),
        }
    }
}

/// Complete metrics snapshot
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub system: SystemMetrics,
    pub storage: StorageMetrics,
    pub block: BlockMetrics,
}

pub fn init_logging() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .try_init()
        .context("failed to initialize global tracing subscriber")
}

pub fn init_metrics() -> Result<()> {
    let addr: SocketAddr = METRICS_LISTEN_ADDR
        .parse()
        .context("invalid metrics listen address")?;
    let builder = PrometheusBuilder::new().with_http_listener(addr);
    let handle = builder
        .install_recorder()
        .context("failed to install Prometheus metrics recorder")?;
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow!("metrics recorder already initialized"))?;

    // Register base system metrics
    register_system_metrics();

    tracing::info!(listen = %addr, "metrics endpoint ready");
    Ok(())
}

/// Initialize metrics with MetricsCollector integration
pub fn init_metrics_with_collector(collector: Arc<MetricsCollector>) -> Result<()> {
    // Store the collector reference
    METRICS_COLLECTOR
        .set(collector.clone())
        .map_err(|_| anyhow!("metrics collector already initialized"))?;

    // Initialize the base Prometheus exporter
    init_metrics()?;

    tracing::info!("MetricsCollector integrated with Prometheus exporter");
    Ok(())
}

/// Register system metrics in Prometheus.
/// In metrics 0.24+, gauges are lazily registered on first use,
/// so we initialize them with a zero value to ensure they appear
/// in the Prometheus scrape output from the start.
fn register_system_metrics() {
    // CPU metrics
    gauge!("system_cpu_usage_percent").set(0.0);
    gauge!("system_cpu_cores").set(0.0);

    // Memory metrics
    gauge!("system_memory_total_bytes").set(0.0);
    gauge!("system_memory_used_bytes").set(0.0);
    gauge!("system_memory_free_bytes").set(0.0);
    gauge!("system_memory_usage_percent").set(0.0);

    // Process metrics
    gauge!("process_uptime_seconds").set(0.0);
    gauge!("process_memory_rss_bytes").set(0.0);

    // Guardian-specific archive metrics
    gauge!("archive_storage_size_bytes").set(0.0);
    gauge!("archive_storage_size_gb").set(0.0);
    gauge!("archive_block_count").set(0.0);
    gauge!("archive_monolith_count").set(0.0);
    gauge!("archive_chain_height").set(0.0);
}

pub fn metrics_handle() -> Option<&'static PrometheusHandle> {
    PROMETHEUS_HANDLE.get()
}

pub async fn update_system_metrics_periodically(
    collector: Arc<MetricsCollector>,
    startup_time: Instant,
) {
    let mut system = System::new_all();

    // Wait a bit for initial CPU usage calculation
    tokio::time::sleep(Duration::from_millis(100)).await;

    let pid = sysinfo::get_current_pid().ok();

    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        // Refresh system information
        system.refresh_all();

        // Update MetricsCollector with system metrics
        collector.update_system(|m| {
            // CPU metrics
            m.system_cpu_usage_percent = system.global_cpu_info().cpu_usage() as f64;
            m.system_cpu_cores = system.cpus().len() as u32;

            // Memory metrics
            m.system_memory_total_bytes = system.total_memory();
            m.system_memory_used_bytes = system.used_memory();
            m.system_memory_free_bytes = system.free_memory();
            m.system_memory_available_bytes = system.available_memory();
            m.system_memory_usage_percent = if system.total_memory() > 0 {
                (system.used_memory() as f64 / system.total_memory() as f64) * 100.0
            } else {
                0.0
            };

            // Process metrics
            m.process_uptime_seconds = startup_time.elapsed().as_secs();

            if let Some(pid) = pid {
                if let Some(process) = system.process(pid) {
                    m.process_memory_rss_bytes = process.memory();
                    m.process_threads = system.cpus().len() as u32;
                }
            }
        });

        // Sync metrics from MetricsCollector to Prometheus
        sync_metrics_to_prometheus(&collector);
    }
}

pub async fn update_archive_metrics_periodically(
    collector: Arc<MetricsCollector>,
    storage: Arc<Storage>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30)); // Update every 30 seconds

    loop {
        interval.tick().await;

        // Get archive metrics from storage
        if let Ok(archive_metrics) = get_archive_metrics(storage.as_ref()) {
            // Update MetricsCollector with archive metrics
            collector.update_storage(|m| {
                m.storage_size_bytes = archive_metrics.size_bytes;
                m.storage_blocks_count = archive_metrics.block_count;
            });

            // Update block metrics with chain height
            collector.update_block(|m| {
                if let Some(height) = archive_metrics.chain_height {
                    m.chain_height = height;
                }
            });

            // Store monolith count in static variable for Prometheus export
            {
                let mut monolith_count = ARCHIVE_MONOLITH_COUNT
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *monolith_count = archive_metrics.monolith_count;
            }
        }

        // Sync metrics from MetricsCollector to Prometheus
        sync_metrics_to_prometheus(&collector);
    }
}

fn sync_metrics_to_prometheus(collector: &MetricsCollector) {
    let metrics = collector.get_metrics();

    // Update Prometheus gauges with system metrics
    gauge!("system_cpu_usage_percent").set(metrics.system.system_cpu_usage_percent);
    gauge!("system_cpu_cores").set(metrics.system.system_cpu_cores as f64);
    gauge!("system_memory_total_bytes").set(metrics.system.system_memory_total_bytes as f64);
    gauge!("system_memory_used_bytes").set(metrics.system.system_memory_used_bytes as f64);
    gauge!("system_memory_free_bytes").set(metrics.system.system_memory_free_bytes as f64);
    gauge!("system_memory_usage_percent").set(metrics.system.system_memory_usage_percent);
    gauge!("process_uptime_seconds").set(metrics.system.process_uptime_seconds as f64);
    gauge!("process_memory_rss_bytes").set(metrics.system.process_memory_rss_bytes as f64);

    // Update Prometheus gauges with guardian-specific archive metrics
    gauge!("archive_storage_size_bytes").set(metrics.storage.storage_size_bytes as f64);
    gauge!("archive_storage_size_gb")
        .set(metrics.storage.storage_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0));
    gauge!("archive_block_count").set(metrics.storage.storage_blocks_count as f64);
    gauge!("archive_monolith_count").set(
        *ARCHIVE_MONOLITH_COUNT
            .lock()
            .unwrap_or_else(|e| e.into_inner()) as f64,
    );
    gauge!("archive_chain_height").set(metrics.block.chain_height as f64);
}

/// Get archive metrics from storage
pub fn get_archive_metrics(storage: &Storage) -> Result<ArchiveMetrics> {
    // Calculate storage size by estimating database size
    let storage_size_bytes = estimate_storage_size(storage)?;

    // Get block count from storage (this would need to be implemented in Storage)
    let block_count = get_block_count_from_storage(storage)?;

    // Get monolith count (this would need to be implemented in Storage)
    let monolith_count = get_monolith_count_from_storage(storage)?;

    // Get chain height (this would need to be implemented in Storage)
    let chain_height = get_chain_height_from_storage(storage)?;

    Ok(ArchiveMetrics {
        size_bytes: storage_size_bytes,
        block_count,
        monolith_count,
        chain_height,
    })
}

/// Estimate storage size from internal stats (heuristic).
fn estimate_storage_size(_storage: &Storage) -> Result<u64> {
    // Storage doesn't expose a db_path() method; return 0 for now.
    // In production this would query RocksDB properties directly.
    Ok(0)
}

/// Get block count from storage
fn get_block_count_from_storage(storage: &Storage) -> Result<u64> {
    // This function requires implementation in the Storage trait
    // The Storage trait should provide a method to count blocks
    // For now, returns 0 as the storage interface is not yet implemented
    Ok(0)
}

/// Get monolith count from storage
fn get_monolith_count_from_storage(storage: &Storage) -> Result<u64> {
    // This function requires implementation in the Storage trait
    // The Storage trait should provide a method to count monoliths
    // For now, returns 0 as the storage interface is not yet implemented
    Ok(0)
}

/// Get chain height from storage
fn get_chain_height_from_storage(storage: &Storage) -> Result<Option<u64>> {
    // This function requires implementation in the Storage trait
    // The Storage trait should provide a method to get the current chain height
    // For now, returns None as the storage interface is not yet implemented
    Ok(None)
}
