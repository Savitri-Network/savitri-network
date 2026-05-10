mod archive;
mod config;
mod serve;
mod telemetry;

use anyhow::{Context, Result};
use clap::Parser;
use config::GuardianConfig;
use libp2p::identity::Keypair;
use serve::ArchiveConfig; // Import ArchiveConfig from serve module
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

// Simple MetricsCollector implementation for guardian node
#[derive(Debug)]
struct MetricsCollector {
    start_time: Instant,
    counters: HashMap<String, u64>,
    gauges: HashMap<String, f64>,
}

impl MetricsCollector {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
            counters: HashMap::new(),
            gauges: HashMap::new(),
        }
    }

    fn increment_counter(&mut self, name: &str) {
        *self.counters.entry(name.to_string()).or_insert(0) += 1;
    }

    fn set_gauge(&mut self, name: &str, value: f64) {
        self.gauges.insert(name.to_string(), value);
    }

    fn get_uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    fn get_metrics_summary(&self) -> String {
        format!(
            "Uptime: {}s, Counters: {:?}, Gauges: {:?}",
            self.get_uptime_seconds(),
            self.counters,
            self.gauges
        )
    }
}

// Guardian mempool implementation for transaction management
#[derive(Debug)]
struct Mempool {
    transactions: std::collections::HashMap<Vec<u8>, MempoolEntry>,
    max_size: usize,
    last_cleanup: std::time::Instant,
}

#[derive(Debug, Clone)]
struct MempoolEntry {
    data: Vec<u8>,
    timestamp: std::time::Instant,
    gas_price: u64,
}

impl Mempool {
    fn new() -> Self {
        Self {
            transactions: std::collections::HashMap::new(),
            max_size: 10000, // Maximum 10k transactions in mempool
            last_cleanup: std::time::Instant::now(),
        }
    }

    fn add_transaction(&mut self, hash: Vec<u8>, data: Vec<u8>, gas_price: u64) -> Result<()> {
        // Remove old transactions if mempool is full
        if self.transactions.len() >= self.max_size {
            self.remove_old_transactions(100)?; // Remove 100 oldest transactions
        }

        self.transactions.insert(
            hash,
            MempoolEntry {
                data,
                timestamp: std::time::Instant::now(),
                gas_price,
            },
        );

        Ok(())
    }

    fn remove_old_transactions(&mut self, count: usize) -> Result<()> {
        let mut entries: Vec<_> = self
            .transactions
            .iter()
            .map(|(k, v)| (k.clone(), v.timestamp, v.gas_price))
            .collect();

        // Sort by timestamp (oldest first) and gas price (lowest first)
        entries.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)));

        for (hash, _, _) in entries.iter().take(count) {
            self.transactions.remove(hash);
        }

        Ok(())
    }

    fn get_transaction(&self, hash: &[u8]) -> Option<&MempoolEntry> {
        self.transactions.get(hash)
    }

    fn size(&self) -> usize {
        self.transactions.len()
    }

    fn cleanup_expired(&mut self) -> Result<()> {
        let now = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(300); // 5 minutes timeout

        let expired: Vec<Vec<u8>> = self
            .transactions
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.timestamp) > timeout)
            .map(|(hash, _)| hash.clone())
            .collect();

        for hash in expired {
            self.transactions.remove(&hash);
        }

        self.last_cleanup = now;
        Ok(())
    }

    fn start_background_purge_tokio(mempool: Arc<Mutex<Self>>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                if let Ok(mut mempool) = mempool.try_lock() {
                    if let Err(e) = mempool.cleanup_expired() {
                        tracing::warn!(error = %e, "Failed to cleanup expired transactions");
                    } else {
                        tracing::debug!(
                            size = mempool.size(),
                            max_size = mempool.max_size,
                            "Mempool cleanup completed"
                        );
                    }
                } else {
                    tracing::warn!("Failed to acquire mempool lock for cleanup");
                }
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
struct ConsensusConfig {
    // Consensus algorithm configuration
    pub algorithm: ConsensusAlgorithm,
    pub block_time_ms: u64,
    pub finality_timeout_ms: u64,

    // Network configuration
    pub min_peers: usize,
    pub max_peers: usize,
    pub bootstrap_timeout_ms: u64,

    // Validation configuration
    pub max_block_size: usize,
    pub max_transactions_per_block: usize,
    pub min_gas_price: u64,

    // Guardian-specific configuration
    pub archive_mode: bool,
    pub sync_from_genesis: bool,
    pub enable_monolith_sync: bool,
}

#[derive(Debug, Clone, Default)]
enum ConsensusAlgorithm {
    #[default]
    ProofOfUnity,
    // Future algorithms could be added here
}

impl ConsensusConfig {
    fn new() -> Self {
        Self {
            algorithm: ConsensusAlgorithm::ProofOfUnity,
            block_time_ms: 6000,        // 6 seconds
            finality_timeout_ms: 30000, // 30 seconds
            min_peers: 3,
            max_peers: 50,
            bootstrap_timeout_ms: 10000, // 10 seconds
            max_block_size: 1024 * 1024, // 1MB
            max_transactions_per_block: 1000,
            min_gas_price: 1000, // Minimum gas price
            archive_mode: true,  // Guardian nodes are archive nodes
            sync_from_genesis: true,
            enable_monolith_sync: true,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.block_time_ms == 0 {
            anyhow::bail!("block_time_ms must be greater than 0");
        }
        if self.min_peers == 0 {
            anyhow::bail!("min_peers must be greater than 0");
        }
        if self.max_peers < self.min_peers {
            anyhow::bail!("max_peers must be greater than or equal to min_peers");
        }
        if self.max_block_size == 0 {
            anyhow::bail!("max_block_size must be greater than 0");
        }
        if self.max_transactions_per_block == 0 {
            anyhow::bail!("max_transactions_per_block must be greater than 0");
        }
        Ok(())
    }
}

fn run_p2p_with_identity_and_monolith(
    identity: Keypair,
    archive_config: crate::serve::ArchiveConfig,
    consensus_config: ConsensusConfig,
) -> Result<()> {
    // Validate consensus configuration
    consensus_config.validate()?;

    // Create network configuration for guardian node
    let mut network_config = savitri_p2p::NetworkConfig {
        listen_port: 4101, // Default guardian port
        max_connections: consensus_config.max_peers,
        enable_encryption: true,
        enable_compression: true,
        max_message_size: consensus_config.max_block_size,
        ..Default::default()
    };

    // Configure archive-specific settings
    if archive_config.max_history_blocks.is_some() || archive_config.max_history_span.is_some() {
        info!("Archive mode configured with history limits");
        network_config.max_connections = network_config.max_connections.max(20);
        // Ensure minimum connections for archive serving
    }

    // Convert libp2p identity to string format for NetworkManager
    let identity_str = format!("{:?}", identity);

    // Create and start network manager
    let mut network_manager =
        savitri_p2p::NetworkManager::with_keypair(network_config, identity_str)
            .context("Failed to create network manager")?;

    // Start the network in a blocking manner for this simple implementation
    let runtime = tokio::runtime::Runtime::new().context("Failed to create async runtime")?;

    runtime.block_on(async {
        // Start network services
        network_manager
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start network manager: {e}"))?;

        info!(
            peer_id = %network_manager.local_peer_id(),
            max_connections = consensus_config.max_peers,
            block_time_ms = consensus_config.block_time_ms,
            "Guardian P2P network started successfully"
        );

        // SECURITY (H-16): Handle both SIGINT and SIGTERM for Docker/systemd
        #[cfg(unix)]
        {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .context("Failed to register SIGTERM handler")?;

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT shutdown signal");
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM shutdown signal");
                }
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .context("Failed to wait for SIGINT")?;
            info!("Received SIGINT shutdown signal");
        }

        info!("Stopping P2P services...");

        // Stop network services
        network_manager
            .stop()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to stop network manager: {e}"))?;

        Ok::<(), anyhow::Error>(())
    })?;

    info!("Guardian P2P services stopped gracefully");
    Ok(())
}

#[derive(Parser, Debug)]
#[command(name = "guardian", about = "Savitri Guardian Node (full archive)")]
struct Args {
    /// Path to the guardian configuration file (TOML)
    #[arg(long, default_value = "guardian/guardian.toml")]
    config: PathBuf,
    /// RocksDB path override
    #[arg(long)]
    db: Option<String>,
    /// Listen port override
    #[arg(long)]
    listen_port: Option<u16>,
    /// Network identity key path override (protobuf-encoded)
    #[arg(long)]
    network_key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    telemetry::init_logging()?;

    // Initialize MetricsCollector
    let metrics_collector = Arc::new(Mutex::new(MetricsCollector::new()));
    let startup_time = Instant::now();

    // Initialize telemetry with MetricsCollector
    if let Err(err) = telemetry::init_metrics() {
        tracing::warn!(error = %err, "metrics exporter not available; continuing without HTTP endpoint");
    }

    let cfg = GuardianConfig::load(&args.config)?;
    let db_path = args
        .db
        .or_else(|| cfg.db_path.clone())
        .unwrap_or_else(|| "guardian.db".to_string());
    let listen_port = args.listen_port.or(cfg.listen_port).unwrap_or(4101);
    let network_key_path: PathBuf = args
        .network_key
        .or_else(|| cfg.network_key_path.clone().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("guardian/guardian-network.key"));

    let bootstrap_peers = serve::parse_bootstrap(&cfg.bootstrap_peers);
    let connect_to = bootstrap_peers.first().cloned();
    let extra_bootstrap: Vec<(String, String)> = bootstrap_peers.iter().skip(1).cloned().collect();

    let storage = archive::open_archive(&db_path)?;

    // Start archive monitoring if configured (disk usage / growth alerts)
    if let Some(interval) = cfg.monitoring.metrics_interval_secs {
        let threshold = cfg.monitoring.disk_alert_threshold_gb;
        archive::spawn_archive_monitor(storage.clone(), interval, threshold);
    }
    let mempool = Arc::new(Mutex::new(Mempool::new()));
    let _purge_handle = Mempool::start_background_purge_tokio(mempool.clone());

    let archive_cfg: ArchiveConfig = serve::archive_limits(&cfg.rate_limits);
    let identity = load_or_generate_identity(&network_key_path)?;

    info!(
        port = listen_port,
        db = %db_path,
        peers = bootstrap_peers.len(),
        "Starting Guardian node (full archive, serving history/proof)"
    );

    if telemetry::metrics_handle().is_some() {
        debug!(endpoint = %format!("http://{addr}/metrics", addr = telemetry::METRICS_LISTEN_ADDR), "metrics recorder initialized");
    }

    // Start background task to update system metrics
    let metrics_collector_clone = metrics_collector.clone();
    tokio::spawn(async move {
        update_system_metrics_periodically(metrics_collector_clone, startup_time).await;
    });

    // Start background task to update archive metrics
    let metrics_collector_archive = metrics_collector.clone();
    let storage_for_metrics = storage.clone();
    tokio::spawn(async move {
        update_archive_metrics_periodically(metrics_collector_archive, storage_for_metrics).await;
    });

    run_p2p_with_identity_and_monolith(identity, archive_cfg, ConsensusConfig::default())?;

    Ok(())
}

fn load_or_generate_identity(path: &Path) -> Result<Keypair> {
    match fs::read(path) {
        Ok(bytes) => Keypair::from_protobuf_encoding(&bytes)
            .with_context(|| format!("failed to decode identity key {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create dir for key {}", parent.display())
                    })?;
                }
            }
            let kp = Keypair::generate_ed25519();
            let encoded = kp
                .to_protobuf_encoding()
                .context("failed to encode generated identity")?;
            fs::write(path, &encoded)
                .with_context(|| format!("failed to persist identity to {}", path.display()))?;
            warn!(
                "Generated new network identity at {}; back it up for stable peer ID",
                path.display()
            );
            Ok(kp)
        }
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

/// Update system metrics periodically
async fn update_system_metrics_periodically(
    metrics_collector: Arc<Mutex<MetricsCollector>>,
    _startup_time: Instant,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    let mut system = sysinfo::System::new_all();

    loop {
        interval.tick().await;

        system.refresh_all();

        if let Ok(mut collector) = metrics_collector.try_lock() {
            // Update uptime
            let uptime = collector.get_uptime_seconds();
            collector.set_gauge("guardian_uptime_seconds", uptime as f64);

            // CPU metrics
            let cpu_usage = system.global_cpu_info().cpu_usage();
            collector.set_gauge("system_cpu_usage_percent", cpu_usage as f64);
            collector.set_gauge("system_cpu_cores", system.cpus().len() as f64);

            // Memory metrics
            let total_memory = system.total_memory();
            let used_memory = system.used_memory();
            let memory_usage_percent = if total_memory > 0 {
                (used_memory as f64 / total_memory as f64) * 100.0
            } else {
                0.0
            };

            collector.set_gauge("system_memory_total_bytes", total_memory as f64);
            collector.set_gauge("system_memory_used_bytes", used_memory as f64);
            collector.set_gauge("system_memory_usage_percent", memory_usage_percent);

            collector.increment_counter("metrics_update_count");

            debug!(
                uptime = uptime,
                cpu_usage = cpu_usage,
                memory_usage = memory_usage_percent,
                "System metrics updated"
            );
        } else {
            warn!("Failed to acquire metrics collector lock for system metrics update");
        }
    }
}

/// Update archive metrics periodically
async fn update_archive_metrics_periodically(
    metrics_collector: Arc<Mutex<MetricsCollector>>,
    storage: Arc<savitri_storage::Storage>,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

    loop {
        interval.tick().await;

        if let Ok(mut collector) = metrics_collector.try_lock() {
            // Update archive-specific metrics
            match archive::get_archive_stats(&storage) {
                Ok(stats) => {
                    collector.set_gauge("archive_storage_size_bytes", stats.size_bytes as f64);
                    collector.set_gauge(
                        "archive_storage_size_gb",
                        stats.size_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
                    );
                    collector.set_gauge("archive_block_count", stats.block_count as f64);
                    collector.set_gauge("archive_chain_height", stats.chain_height as f64);

                    debug!(
                        storage_size_gb = stats.size_bytes as f64 / 1024.0 / 1024.0 / 1024.0,
                        block_count = stats.block_count,
                        chain_height = stats.chain_height,
                        "Archive metrics updated"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Failed to get archive storage stats");
                }
            }

            collector.increment_counter("archive_metrics_update_count");
        } else {
            warn!("Failed to acquire metrics collector lock for archive metrics update");
        }
    }
}
