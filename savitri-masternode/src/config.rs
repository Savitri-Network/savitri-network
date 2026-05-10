//! Masternode Configuration

//! This module provides configuration management for Savitri masternodes.
//! It handles loading and parsing TOML configuration files with all necessary
//! parameters for masternode operation including network settings, storage paths,

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

fn default_monolith_interval_secs() -> u64 {
    30
}
fn default_monolith_max_blocks() -> u64 {
    10_000
}
fn default_heartbeat_interval_ms() -> u64 {
    5000
}
// proposer ~10x longer tenure (1 epoch = heartbeat_ms * slots_per_epoch =
// 5s * 200 = 1000s = ~16min). The previous 100s window was too short for
// the LN<->MN 3-fasi handshake to complete under high TX load -- group_id
// rotation kicked in before the proposer could drain the mempool. See
fn default_slots_per_epoch() -> u64 {
    200
}
fn default_monolith_epoch_ms() -> u64 {
    86400000
}
fn default_genesis_timestamp_ms() -> u64 {
    0
}
fn default_tx_interval_secs() -> u64 {
    1
}
fn default_block_interval_secs() -> u64 {
    5
}
fn default_max_block_txs() -> usize {
    5000
}
fn default_p2p_port() -> u16 {
    4021
}
fn default_validator_stake() -> u64 {
    10_000
} // 10k SAV coins
fn default_group_health_check_interval_secs() -> u64 {
    60
}
fn default_group_node_timeout_secs() -> u64 {
    300
}
// of stable group identity (~ 10 * slots_per_epoch * heartbeat_ms). At the new
// P2 default of 200 slots * 5s = 1000s per epoch, this yields ~167 minutes of
// group_id stability, which lets the LN<->MN 3-fasi handshake complete and the
// proposer drain the mempool without group churn breaking tenure. Combined
// with the throttle hardening in group_formation.rs (cold-start bypass only,
// no longer triggered by transient active_groups eviction), this fixes the
// observed group_14_1_14 -> group_15_1_15 -> ... rotation every few seconds.
fn default_formation_interval_epochs() -> u64 {
    10
}
fn default_registry_ttl_secs() -> u64 {
    300
}
fn default_reward_period_secs() -> u64 {
    86400
}
fn default_initial_bootstrap_blocks() -> u64 {
    10
}
fn default_min_group_size() -> usize {
    5
}
fn default_max_group_size() -> usize {
    8
}
fn default_total_masternodes() -> usize {
    5
}
fn default_min_masternodes() -> usize {
    4
}

#[derive(Debug, Deserialize)]
pub struct MasternodeConfig {
    pub storage_path: PathBuf,
    pub monolith_storage_path: Option<String>,
    /// Base directory for resolving relative storage paths. If set, paths like
    /// storage_path and monolith_storage_path are resolved relative to this.
    /// Can be absolute (e.g. /var/lib/savitri) or relative to config file dir (e.g. "../..").
    /// If not set, paths are resolved relative to current working directory (legacy).
    #[serde(default)]
    pub data_dir: Option<String>,
    pub network_key_path: PathBuf,
    #[serde(default = "default_validator_stake")]
    pub validator_stake: u64,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub validators: Vec<String>,
    #[serde(default = "default_p2p_port")]
    pub p2p_port: u16,
    /// External/announce IP address used for libp2p identify and peer registry
    /// announcements when this node is reachable from other machines.
    #[serde(default)]
    pub external_ip: Option<String>,
    #[serde(default = "default_monolith_interval_secs")]
    pub monolith_interval_secs: u64,
    #[serde(default = "default_monolith_max_blocks")]
    pub monolith_max_blocks: u64,
    #[serde(default = "default_tx_interval_secs")]
    pub tx_interval_secs: u64,
    #[serde(default = "default_block_interval_secs")]
    pub block_interval_secs: u64,
    #[serde(default = "default_max_block_txs")]
    pub max_block_txs: usize,
    #[serde(default = "default_group_health_check_interval_secs")]
    pub group_health_check_interval_secs: u64,
    #[serde(default = "default_group_node_timeout_secs")]
    pub group_node_timeout_secs: u64,
    /// Minimum number of epochs between two successive group formation cycles
    /// (e.g. 10 means group composition can rotate at most once every 10 epochs).
    /// The throttle in `group_formation::form_and_distribute_groups` and the
    /// inner `form_groups` rate-limiter both honor this value. After the first
    /// successful formation the anchor (`current_epoch`) is set, and subsequent
    /// formation attempts are skipped until `slots_per_epoch * heartbeat_ms *
    /// formation_interval_epochs` ms have elapsed -- even if `active_groups` is
    #[serde(default = "default_formation_interval_epochs")]
    pub formation_interval_epochs: u64,
    #[serde(default = "default_registry_ttl_secs")]
    pub registry_ttl_secs: u64,
    /// Reward distribution interval in seconds. Default is 24h; test/debug
    /// deployments can lower this to observe reward accounting faster.
    #[serde(default = "default_reward_period_secs")]
    pub reward_period_secs: u64,
    /// Unified slot: 1 slot = 1 heartbeat (ms)
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    /// Unified slot: slots per epoch (1 epoch = heartbeat_interval_ms × slots_per_epoch)
    #[serde(default = "default_slots_per_epoch")]
    pub slots_per_epoch: u64,
    /// Unified slot: monolith epoch duration (ms). 1 monolith epoch = monolith_epoch_ms
    #[serde(default = "default_monolith_epoch_ms")]
    pub monolith_epoch_ms: u64,
    /// Genesis timestamp (ms) for slot 0
    #[serde(default = "default_genesis_timestamp_ms")]
    pub genesis_timestamp_ms: u64,
    /// Blocchi "simulati" in bootstrap quando la catena è vuota (default 10 per dev/test).
    #[serde(default = "default_initial_bootstrap_blocks")]
    pub initial_bootstrap_blocks: u64,
    /// Minimum lightnodes per group (default 5). Use 2 for minimal mesh test (e.g. 2 LNs, 1 group).
    #[serde(default = "default_min_group_size")]
    pub min_group_size: usize,
    /// Maximum lightnodes per group (default 8).
    #[serde(default = "default_max_group_size")]
    pub max_group_size: usize,
    /// Total masternodes in network for BFT (default 5). Use 1 for single-MN test.
    #[serde(default = "default_total_masternodes")]
    pub total_masternodes: usize,
    /// Minimum masternodes required for BFT quorum (default 4). Use 1 for single-MN test.
    #[serde(default = "default_min_masternodes")]
    pub min_masternodes: usize,
    /// Enable RPC server for PoU endpoints.
    #[serde(default)]
    pub rpc_enabled: Option<bool>,
    /// RPC server port (default 8545).
    #[serde(default)]
    pub rpc_port: Option<u16>,
    /// RPC bind address (default 127.0.0.1).
    #[serde(default)]
    pub rpc_bind_addr: Option<String>,
    /// ZKP backend for monolith proofs: "arkworks" (production), "plonk", or "mock" (dev only).
    /// Production must use "arkworks" and compile with --features zkp-arkworks.
    #[serde(default)]
    pub zkp_backend: Option<String>,
    /// Reward/payout address (hex 64 chars). Masternode earnings credited here; user-controlled wallet (Ethereum/Solana style). If unset, address is derived from node ID.
    #[serde(default)]
    pub reward_address: Option<String>,
    /// Pre-funded accounts seeded at startup (hex-encoded 32-byte public keys).
    /// Every node in the network must list the same set so that sender accounts
    /// exist in all storages before the first transaction is broadcast.
    #[serde(default)]
    pub genesis_accounts: Vec<String>,
    /// Balance (in whole SAVT tokens) for each genesis account. Default: 1.
    /// For testnet with faucet, set to 10_000_000 (10M SAVT).
    #[serde(default = "default_genesis_account_balance_savt")]
    pub genesis_account_balance_savt: u64,
}

fn default_genesis_account_balance_savt() -> u64 {
    1
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<MasternodeConfig> {
    let content = std::fs::read_to_string(path.as_ref())
        .with_context(|| format!("failed to read config file: {:?}", path.as_ref()))?;
    let config: MasternodeConfig =
        toml::from_str(&content).with_context(|| "failed to parse config file")?;
    Ok(config)
}

/// Resolve a storage path relative to data_dir or current working directory.
/// Used for storage_path and monolith_storage_path to ensure correct paths
/// regardless of process CWD (e.g. when running as service, from Docker, etc.).
pub fn resolve_storage_path(config_path: &Path, data_dir: Option<&str>, path: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let config_full = if path.is_absolute() {
        return path.to_path_buf();
    } else if config_path.is_absolute() {
        config_path.to_path_buf()
    } else {
        cwd.join(config_path)
    };
    let config_dir = config_full
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    let base = match data_dir {
        None => cwd,
        Some(dd) => {
            let p = PathBuf::from(dd);
            if p.is_absolute() {
                p
            } else {
                config_dir.join(p)
            }
        }
    };

    base.join(path)
}
