#![allow(dead_code)]

use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};

pub const DEFAULT_LISTEN_PORT: u16 = 4001;
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub const DEFAULT_SLOTS_PER_EPOCH: u32 = 20; // Unified: 1 epoch = 100 s with H=5s
pub const DEFAULT_PEER_SERVER_NETWORK_ID: &str = "savitri-testnet-v0.1.0";

#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub masternode_peers: Vec<String>,
    #[serde(default = "default_listen_port")]
    pub listen_port: u16,
    #[serde(default)]
    pub tx_interval_secs: Option<u64>,
    #[serde(default)]
    pub min_tx_per_second_per_recipient: Option<u32>,
    #[serde(default)]
    pub block_interval_secs: Option<u64>,
    #[serde(default)]
    pub max_block_txs: Option<usize>,
    #[serde(default)]
    pub resources: Option<ResourceConfig>,
    #[serde(default)]
    pub network_key_path: std::path::PathBuf,
    #[serde(default)]
    pub resource_weight_bandwidth: Option<f64>,
    #[serde(default)]
    pub resource_weight_cpu: Option<f64>,
    #[serde(default)]
    pub resource_weight_storage: Option<f64>,
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_slots_per_epoch")]
    pub slots_per_epoch: u32,
    /// Genesis wall-clock timestamp in milliseconds since UNIX epoch.
    /// CRITICAL for epoch arithmetic — must match the masternode value.
    /// Read from env `SAVITRI_GENESIS_TIMESTAMP_MS` at boot if not set in config.
    /// `(now_ms - genesis_timestamp_ms) / heartbeat_ms / slots_per_epoch`,
    /// not the legacy `unix_ms / 100_000`.
    #[serde(default)]
    pub genesis_timestamp_ms: Option<u64>,
    /// If true, use in-memory storage only. If false (default), use RocksDB at db_path (or --db).
    #[serde(default)]
    pub memory_only: Option<bool>,
    /// Database path when memory_only = false. If absent, main uses args.db as fallback.
    #[serde(default)]
    pub db_path: Option<std::path::PathBuf>,
    /// Enable RPC server
    #[serde(default)]
    pub rpc_enabled: Option<bool>,
    /// RPC server port (default 8545)
    #[serde(default)]
    pub rpc_port: Option<u16>,
    /// RPC bind address (default 127.0.0.1)
    #[serde(default)]
    pub rpc_bind_addr: Option<String>,
    /// Path to faucet_keys_testnet.json for testnet faucet (optional)
    #[serde(default)]
    pub faucet_keys_path: Option<std::path::PathBuf>,
    /// Use testnet fee schedule (1 SAVT normal, 5 SAVT contract, 0.05 SAVT IoT)
    #[serde(default)]
    pub testnet_fees: Option<bool>,
    /// External/announce IP address for inbound connections from other nodes.
    /// Set this to the machine's reachable IP when nodes run on separate VMs or machines.
    /// Replaces 0.0.0.0 and 127.0.0.1 in peer registrations and announcements.
    /// Example: "1.2.3.4" or "192.168.1.10"
    #[serde(default)]
    pub external_ip: Option<String>,
    /// Relay server peers (masternodes) for NAT traversal.
    /// If empty, bootstrap_peers are used as relay candidates.
    #[serde(default)]
    pub relay_servers: Vec<String>,
    /// Peer registry URL — fetched on startup to populate bootstrap_peers/masternode_peers
    /// if they are empty. Expected JSON schema: see deploy/cloudflare/worker.js.
    /// Example: "https://peers.savitrinetwork.com/peers.json"
    #[serde(default)]
    pub peer_registry_url: Option<String>,
    /// Enable UPnP auto port-forwarding on residential routers.
    /// Default: true. Set false to disable if your router has buggy UPnP.
    #[serde(default)]
    pub enable_upnp: Option<bool>,
    /// Reward/payout address (hex 64 chars). Node earnings credited here; user-controlled wallet (Ethereum/Solana style). If unset, producer key address is used.
    #[serde(default)]
    pub reward_address: Option<String>,
    /// Pre-funded accounts seeded at startup (hex-encoded 32-byte public keys).
    /// Every node in the network must list the same set so that sender accounts
    /// exist in all storages before the first transaction is broadcast.
    #[serde(default)]
    pub genesis_accounts: Vec<String>,
    /// Centralized peer server bootstrap/discovery integration.
    #[serde(default)]
    pub peer_server: PeerServerConfig,
}

fn default_slots_per_epoch() -> u32 {
    DEFAULT_SLOTS_PER_EPOCH
}

fn default_listen_port() -> u16 {
    DEFAULT_LISTEN_PORT
}

fn default_heartbeat_interval_secs() -> u64 {
    DEFAULT_HEARTBEAT_INTERVAL_SECS
}

fn default_peer_server_network_id() -> String {
    DEFAULT_PEER_SERVER_NETWORK_ID.to_string()
}

fn default_peer_server_register_on_startup() -> bool {
    true
}

fn default_peer_server_heartbeat_interval_secs() -> u64 {
    30
}

fn default_peer_server_get_peers_interval_secs() -> u64 {
    60
}

fn default_peer_server_peer_request_limit() -> usize {
    25
}

fn default_peer_server_auto_unregister_on_shutdown() -> bool {
    true
}

fn default_peer_server_request_timeout_secs() -> u64 {
    5
}

fn default_peer_server_allow_start_without_server() -> bool {
    true
}

fn default_peer_server_roles() -> Vec<String> {
    vec!["lightnode".to_string()]
}

#[derive(Debug, Deserialize, Clone)]
pub struct PeerServerConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default = "default_peer_server_network_id")]
    pub network_id: String,
    #[serde(default = "default_peer_server_register_on_startup")]
    pub register_on_startup: bool,
    #[serde(default = "default_peer_server_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_peer_server_get_peers_interval_secs")]
    pub get_peers_interval_secs: u64,
    #[serde(default = "default_peer_server_peer_request_limit")]
    pub peer_request_limit: usize,
    #[serde(default)]
    pub include_rpc_endpoints: bool,
    #[serde(default = "default_peer_server_auto_unregister_on_shutdown")]
    pub auto_unregister_on_shutdown: bool,
    #[serde(default = "default_peer_server_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_peer_server_allow_start_without_server")]
    pub allow_start_without_server: bool,
    #[serde(default = "default_peer_server_roles")]
    pub roles: Vec<String>,
    /// Publish private, loopback, and RFC1918 addresses to the peer server.
    /// Disabled by default so the registry stays externally dialable.
    #[serde(default)]
    pub publish_private_addresses: bool,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: None,
            network_id: default_peer_server_network_id(),
            register_on_startup: default_peer_server_register_on_startup(),
            heartbeat_interval_secs: default_peer_server_heartbeat_interval_secs(),
            get_peers_interval_secs: default_peer_server_get_peers_interval_secs(),
            peer_request_limit: default_peer_server_peer_request_limit(),
            include_rpc_endpoints: false,
            auto_unregister_on_shutdown: default_peer_server_auto_unregister_on_shutdown(),
            request_timeout_secs: default_peer_server_request_timeout_secs(),
            allow_start_without_server: default_peer_server_allow_start_without_server(),
            roles: default_peer_server_roles(),
            publish_private_addresses: false,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ResourceConfig {
    #[serde(default)]
    pub bandwidth_mbps: Option<f64>,
    #[serde(default)]
    pub cpu_cores: Option<f64>,
    #[serde(default)]
    pub storage_gb: Option<f64>,
    #[serde(default = "ResourceConfig::default_epoch_secs")]
    pub epoch_secs: u64,
    #[serde(default = "ResourceConfig::default_tolerance")]
    pub tolerance: f64,
    #[serde(default)]
    pub weights: ResourceWeights,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AdaptiveLatencyConfig {
    #[serde(default = "AdaptiveLatencyConfig::default_enable_adaptive")]
    pub enable_adaptive_latency: bool,
    #[serde(default = "AdaptiveLatencyConfig::default_base_latency_threshold")]
    pub base_latency_threshold_ms: u64,
    #[serde(default = "AdaptiveLatencyConfig::default_latency_adaptation_factor")]
    pub latency_adaptation_factor: f64,
    #[serde(default = "AdaptiveLatencyConfig::default_latency_recalculation_epochs")]
    pub latency_recalculation_epochs: u64,
    #[serde(default = "AdaptiveLatencyConfig::default_base_latency")]
    pub base_latency_ms: u64,
    #[serde(default = "AdaptiveLatencyConfig::default_max_adjustment")]
    pub max_adjustment_percent: f64,
    #[serde(default = "AdaptiveLatencyConfig::default_window_size")]
    pub window_size: usize,
    #[serde(default = "AdaptiveLatencyConfig::default_threshold")]
    pub adjustment_threshold: f64,
}

impl Default for AdaptiveLatencyConfig {
    fn default() -> Self {
        Self {
            enable_adaptive_latency: true,
            base_latency_threshold_ms: 150,
            latency_adaptation_factor: 1.5,
            latency_recalculation_epochs: 50,
            base_latency_ms: 100,
            max_adjustment_percent: 50.0,
            window_size: 10,
            adjustment_threshold: 0.2,
        }
    }
}

impl AdaptiveLatencyConfig {
    fn default_enable_adaptive() -> bool {
        true
    }
    fn default_base_latency_threshold() -> u64 {
        150
    }
    fn default_latency_adaptation_factor() -> f64 {
        1.5
    }
    fn default_latency_recalculation_epochs() -> u64 {
        50
    }
    fn default_base_latency() -> u64 {
        100
    }
    fn default_max_adjustment() -> f64 {
        50.0
    }
    fn default_window_size() -> usize {
        10
    }
    fn default_threshold() -> f64 {
        0.2
    }
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            bandwidth_mbps: None,
            cpu_cores: None,
            storage_gb: None,
            epoch_secs: Self::default_epoch_secs(),
            tolerance: Self::default_tolerance(),
            weights: ResourceWeights::default(),
        }
    }
}

impl ResourceConfig {
    const fn default_epoch_secs() -> u64 {
        60
    }

    const fn default_tolerance() -> f64 {
        0.05
    }
}

#[derive(Debug, Deserialize, Clone, Copy)]
pub struct ResourceWeights {
    #[serde(default = "ResourceWeights::default_bandwidth_weight")]
    pub bandwidth: f64,
    #[serde(default = "ResourceWeights::default_cpu_weight")]
    pub cpu: f64,
    #[serde(default = "ResourceWeights::default_storage_weight")]
    pub storage: f64,
}

impl Default for ResourceWeights {
    fn default() -> Self {
        Self {
            bandwidth: Self::default_bandwidth_weight(),
            cpu: Self::default_cpu_weight(),
            storage: Self::default_storage_weight(),
        }
    }
}

impl ResourceWeights {
    const fn default_bandwidth_weight() -> f64 {
        0.4
    }

    const fn default_cpu_weight() -> f64 {
        0.3
    }

    const fn default_storage_weight() -> f64 {
        0.3
    }

    pub fn normalized(self) -> Self {
        let sum = self.bandwidth + self.cpu + self.storage;
        if sum <= f64::EPSILON {
            return Self::default();
        }
        Self {
            bandwidth: self.bandwidth / sum,
            cpu: self.cpu / sum,
            storage: self.storage / sum,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Option<Self>> {
        if path.as_os_str().is_empty() {
            return Ok(None);
        }

        match fs::read_to_string(path) {
            Ok(contents) => {
                let cfg: Self =
                    toml::from_str(&contents).context("failed to parse lightnode config")?;
                Ok(Some(cfg))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_PEER_SERVER_NETWORK_ID};

    #[test]
    fn parses_peer_server_config_section() {
        let cfg: Config = toml::from_str(
            r#"
listen_port = 4001

[peer_server]
enabled = true
base_url = "https://peers.example.com"
network_id = "custom-testnet"
register_on_startup = true
heartbeat_interval_secs = 15
get_peers_interval_secs = 45
peer_request_limit = 12
include_rpc_endpoints = true
auto_unregister_on_shutdown = false
request_timeout_secs = 9
allow_start_without_server = false
roles = ["lightnode", "rpc"]
publish_private_addresses = true
"#,
        )
        .expect("config should parse");

        assert!(cfg.peer_server.enabled);
        assert_eq!(
            cfg.peer_server.base_url.as_deref(),
            Some("https://peers.example.com")
        );
        assert_eq!(cfg.peer_server.network_id, "custom-testnet");
        assert_eq!(cfg.peer_server.heartbeat_interval_secs, 15);
        assert_eq!(cfg.peer_server.get_peers_interval_secs, 45);
        assert_eq!(cfg.peer_server.peer_request_limit, 12);
        assert!(cfg.peer_server.include_rpc_endpoints);
        assert!(!cfg.peer_server.auto_unregister_on_shutdown);
        assert_eq!(cfg.peer_server.request_timeout_secs, 9);
        assert!(!cfg.peer_server.allow_start_without_server);
        assert_eq!(cfg.peer_server.roles, vec!["lightnode", "rpc"]);
        assert!(cfg.peer_server.publish_private_addresses);
    }

    #[test]
    fn peer_server_defaults_are_backward_compatible() {
        let cfg: Config = toml::from_str("listen_port = 4001").expect("config should parse");

        assert!(!cfg.peer_server.enabled);
        assert!(cfg.peer_server.base_url.is_none());
        assert_eq!(cfg.peer_server.network_id, DEFAULT_PEER_SERVER_NETWORK_ID);
        assert!(cfg.peer_server.register_on_startup);
        assert_eq!(cfg.peer_server.roles, vec!["lightnode"]);
        assert!(cfg.peer_server.allow_start_without_server);
        assert!(!cfg.peer_server.publish_private_addresses);
    }
}
