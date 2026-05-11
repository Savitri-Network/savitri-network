#![allow(dead_code, unused_variables, unused_imports)]

mod adaptive_latency;
mod availability;
#[cfg(feature = "contracts")]
mod contract_executor;
mod core;
mod latency_service;
mod p2p;
mod p2p_block_receiver;
mod p2p_broadcast;
mod p2p_certificate;
mod p2p_group_manager;
mod p2p_integrity;
mod p2p_intra_group;
mod p2p_periodic_tasks;
mod p2p_pou;
mod peer_server;
mod proposer;
mod resource;
#[cfg(feature = "rpc")]
mod rpc;
mod storage;
mod tx;
#[cfg(feature = "rpc")]
mod tx_router;
// p2p_fee_distribution removed: duplicate of p2p/fee_distribution.rs
mod signer;
mod telemetry;
use signer::load_or_generate_ed25519;

#[cfg(feature = "desktop")]
mod config;
#[cfg(feature = "desktop")]
mod integrity;
#[cfg(feature = "desktop")]
mod logging;
use crate::storage::{BlockAndAccountStorageTrait, RocksDBLightnodeStorage, Storage};
// RpcConsumerMetrics) used by p2p::network::mod and rpc consumer paths.
#[cfg(feature = "desktop")]
mod observability;
use anyhow::{bail, Context, Result};
use bincode;
use clap::Parser;
use ed25519_dalek::SigningKey as DalekKeypair;
use ed25519_dalek::{Signature, Signer, SigningKey as Keypair, VerifyingKey as PublicKey};
use hex;
use libp2p::PeerId;
use p2p::block::MempoolPipeline;
use p2p::start_network;
use savitri_core::crypto::sign_data;
use savitri_core::crypto::signature::sign;
use tracing::{error, info, warn};

#[cfg(feature = "desktop")]
use crate::logging::{flagged_message, FLAG_BLOCK_ATTEMPT, FLAG_POU};

/// Maximum allowed size for network transaction deserialization (1 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized network payloads.
const MAX_NETWORK_TX_SIZE: usize = 1 * 1024 * 1024;

// Convert bytes to signed transaction
fn bytes_to_raw_tx(
    bytes: Vec<u8>,
    _peer_id: Option<u64>,
) -> Result<crate::tx::SignedTx, anyhow::Error> {
    if bytes.len() > MAX_NETWORK_TX_SIZE {
        anyhow::bail!(
            "Network transaction data too large: {} bytes (max {})",
            bytes.len(),
            MAX_NETWORK_TX_SIZE
        );
    }
    // Prova prima a deserializzare come TransactionExt (formato used da broadcast_signed_tx)
    // CRITICAL: use the same fixint encoding as serialize_signed_tx()
    if let Ok(tx_ext) = crate::tx::deserialize_signed_tx(&bytes) {
        if !tx_ext.pre_verified {
            let verified = crate::tx::verify_transaction_signature_ext(&tx_ext);
            if verified {
                Ok(crate::tx::TransactionExt {
                    from: tx_ext.from,
                    to: tx_ext.to,
                    amount: tx_ext.amount,
                    nonce: tx_ext.nonce,
                    fee: tx_ext.fee,
                    data: tx_ext.data,
                    pubkey: tx_ext.pubkey,
                    sig: tx_ext.sig,
                    pre_verified: true,
                })
            } else {
                Ok(tx_ext) // Mantiene pre_verified=false se la check fallisce
            }
        } else {
            Ok(tx_ext)
        }
    } else {
        let core_tx = crate::core::tx::deserialize_signed_tx(&bytes)?;

        Ok(crate::tx::TransactionExt {
            from: hex::encode(&core_tx.from),
            to: hex::encode(&core_tx.to),
            amount: core_tx.amount as u64,
            nonce: core_tx.nonce,
            fee: core_tx.fee,
            data: None,
            pubkey: core_tx.pubkey,
            sig: core_tx.sig,
            pre_verified: core_tx.pre_verified,
        })
    }
}
use crate::tx::{
    deserialize_signed_tx, ensure_genesis_block, hash_signed_tx_bytes, Block, SignedTx,
    StorageBlockExt,
};

// Real signature verification implementation using core::tx
use crate::core::tx::verify_transaction_signature;

#[derive(Debug, Clone)]
pub struct VerifiedTx {
    pub tx_bytes: Vec<u8>,
    pub is_valid: bool,
    pub tx: Option<crate::tx::SignedTx>, // Store parsed transaction for reuse
}

pub struct SigVerifyStage;

impl SigVerifyStage {
    pub fn new() -> Self {
        Self
    }

    pub async fn process_batch(&mut self, tx_batch: Vec<Vec<u8>>) -> Vec<VerifiedTx> {
        let mut verified_txs = Vec::new();

        for tx_bytes in tx_batch {
            // Try to deserialize the transaction
            let verification_result = match crate::tx::deserialize_signed_tx(&tx_bytes) {
                Ok(signed_tx) => {
                    // Perform real cryptographic signature verification
                    let is_valid = crate::tx::verify_transaction_signature_ext(&signed_tx);

                    if !is_valid {
                        info!(
                            tx_bytes_len = tx_bytes.len(),
                            from = %signed_tx.from,
                            to = %signed_tx.to,
                            amount = signed_tx.amount,
                            nonce = signed_tx.nonce,
                            pubkey_hex = hex::encode(&signed_tx.pubkey),
                            sig_first8 = hex::encode(&signed_tx.sig[..8]),
                            pre_verified = signed_tx.pre_verified,
                            "🔴 SigVerify FAILED: deserialized OK but signature invalid"
                        );
                    } else {
                        debug!(
                            tx_hash = hex::encode(crate::tx::hash_signed_tx_bytes(&tx_bytes)),
                            is_valid = is_valid,
                            "Cryptographic signature verification completed"
                        );
                    }

                    VerifiedTx {
                        tx_bytes,
                        is_valid,
                        tx: Some(signed_tx),
                    }
                }
                Err(e) => {
                    info!(
                        error = %e,
                        tx_bytes_len = tx_bytes.len(),
                        tx_first16 = hex::encode(&tx_bytes[..tx_bytes.len().min(16)]),
                        "🔴 SigVerify FAILED: deserialization error"
                    );

                    VerifiedTx {
                        tx_bytes,
                        is_valid: false,
                        tx: None,
                    }
                }
            };

            verified_txs.push(verification_result);
        }

        debug!(
            total_txs = verified_txs.len(),
            valid_txs = verified_txs.iter().filter(|v| v.is_valid).count(),
            "Batch signature verification completed"
        );

        verified_txs
    }
}
use crate::p2p::types::BootstrapPeer;
use savitri_mempool::types::MemoryStorageExt;
use std::str::FromStr;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[allow(unused_imports)]
use tokio::sync::Mutex;
use tokio::{
    sync::{mpsc, RwLock},
    time,
};
use tracing::debug;

const DEFAULT_POU_SCORES: &[(&str, u16, &str)] = &[
    (
        "12D3KooWSVEN7mLHu5N3rBSRFdLSEf7p37ybtE7fpah1e1em2wSa",
        873,
        "/ip4/127.0.0.1/tcp/4002",
    ),
    (
        "12D3KooWMhnoLgDdGJ1kiNvRruJ7BJUKeCowXqE8NUSUahHUtDNv",
        820,
        "/ip4/127.0.0.1/tcp/4001",
    ),
];

fn save_peer_id_to_file(peer_id: libp2p::PeerId, port: u16) -> Result<()> {
    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;
    let peer_file = data_dir.join(format!("peer_id_{}.txt", port));
    std::fs::write(&peer_file, peer_id.to_string())
        .with_context(|| format!("failed to write peer id file {}", peer_file.display()))?;
    Ok(())
}

// Function to discover real masternode peer IDs from their data files
fn discover_masternode_peers() -> Vec<String> {
    let mut peers = Vec::new();
    let masternode_base = PathBuf::from("../savitri-masternode");

    for port in 5021..=5025 {
        let peer_file = masternode_base.join(format!("data/peer_id_{}.txt", port));

        if let Ok(peer_id) = std::fs::read_to_string(&peer_file) {
            let peer_id = peer_id.trim();
            let addr = format!("/ip4/127.0.0.1/tcp/{}", port);
            peers.push(format!("{}@{}", peer_id, addr));
            debug!("Discovered masternode peer: {} on port {}", peer_id, port);
        } else {
            // Fallback: try to connect by address only (libp2p will resolve peer ID)
            let addr = format!("/ip4/127.0.0.1/tcp/{}", port);
            warn!(
                "Could not read peer ID for port {}, using address-only connection",
                port
            );
            // Note: This won't work with current libp2p API, but shows the intent
        }
    }

    if peers.is_empty() {
        warn!("No masternode peer IDs discovered, falling back to hardcoded peer ID for port 5021");
        // Fallback to the known peer ID for port 5021
        peers.push(
            "12D3KooWM3w8SwaveXkWdwi5cbHnN8KJntTdcECviaCU2yxzHBpD@/ip4/127.0.0.1/tcp/5021"
                .to_string(),
        );
    }

    peers
}

const DEFAULT_MASTERNODE_PEERS: &[(&str, &str)] = &[
    // This is now just a fallback - the real discovery happens above
    (
        "12D3KooWM3w8SwaveXkWdwi5cbHnN8KJntTdcECviaCU2yxzHBpD",
        "/ip4/127.0.0.1/tcp/5021",
    ),
];

const DEFAULT_PEER_GROUP: &[(&str, &str)] = &[
    (
        "12D3KooWSVEN7mLHu5N3rBSRFdLSEf7p37ybtE7fpah1e1em2wSa",
        "/ip4/127.0.0.1/tcp/4002",
    ),
    (
        "12D3KooWMhnoLgDdGJ1kiNvRruJ7BJUKeCowXqE8NUSUahHUtDNv",
        "/ip4/127.0.0.1/tcp/4001",
    ),
];

const USE_DEFAULT_POU_ONLY: bool = false;

#[derive(Parser, Debug)]
struct Args {
    /// RocksDB path for local state.
    #[arg(long, default_value = "lightnode.db")]
    db: String,
    /// Path to the light node configuration file (TOML).
    #[arg(long, default_value = "lightnode/lightnode.toml")]
    config: PathBuf,
    /// Path to the libp2p identity key (protobuf-encoded).
    #[arg(long, default_value = "lightnode-network.key")]
    network_key_path: PathBuf,
    /// Path to the block signing key (ed25519).
    #[arg(long, default_value = "lightnode-producer.key")]
    producer_key_path: PathBuf,
    /// Path to the transaction signing key (ed25519). If not provided, will be auto-generated.
    #[arg(long, default_value = "lightnode-tx.key")]
    tx_key_path: PathBuf,
    /// Interval between synthetic transaction generation (seconds); set to 0 to disable.
    #[arg(long)]
    tx_interval_secs: Option<u64>,
    /// Recipient addresses (hex-encoded 32-byte values) for the synthetic tx generator.
    /// If omitted, destinations are randomized.
    #[arg(long = "tx-recipient", value_delimiter = ',')]
    tx_recipients: Vec<String>,
    /// Sender key offset for the tx generator, so multiple generators use
    /// non-overlapping sender pools (0, 50, 100, …).
    #[arg(long, default_value = "0")]
    tx_gen_offset: usize,
    /// Additional private keys used by tx-generator senders.
    /// Repeat `--tx-gen-sender-key <path>` for multiple files.
    #[arg(long = "tx-gen-sender-key")]
    tx_gen_sender_keys: Vec<PathBuf>,
    /// Interval between block production attempts (seconds); set to 0 to disable.
    #[arg(long)]
    block_interval_secs: Option<u64>,
    /// Maximum number of signed transactions per block.
    #[arg(long)]
    max_block_txs: Option<usize>,
    /// Bootstrap peers in `<peer_id>@<multiaddr>` form.
    #[arg(long = "bootstrap")]
    bootstrap: Vec<String>,
    /// TCP port to listen on for libp2p gossip (overrides config/default).
    #[arg(long)]
    listen_port: Option<u16>,
    /// Declared outbound bandwidth capacity in MB/s for Resource Quality scoring.
    #[arg(long)]
    resource_bandwidth_mbps: Option<f64>,
    /// Declared compute capacity (effective CPU cores) for Resource Quality scoring.
    #[arg(long)]
    resource_cpu_cores: Option<f64>,
    /// Declared storage commitment in GB for Resource Quality scoring.
    #[arg(long)]
    resource_storage_gb: Option<f64>,
    /// Override the Resource Quality epoch window in seconds.
    #[arg(long)]
    resource_epoch_secs: Option<u64>,
    /// Override tolerance between claimed and observed Resource Quality ratios.
    #[arg(long)]
    resource_tolerance: Option<f64>,
    /// Override weight for bandwidth contribution inside Resource Quality (0-1).
    #[arg(long)]
    resource_weight_bandwidth: Option<f64>,
    /// Override weight for CPU contribution inside Resource Quality (0-1).
    #[arg(long)]
    resource_weight_cpu: Option<f64>,
    /// Override weight for storage contribution inside Resource Quality (0-1).
    #[arg(long)]
    resource_weight_storage: Option<f64>,
    /// Pre-seed mempool with N transactions for capacity testing.
    /// Generates N pre-signed TX and injects directly into the local mempool
    /// before block production starts. Bypasses gossipsub entirely.
    /// Example: --preseed-tx 50000 (50K TX per node)
    #[arg(long)]
    preseed_tx: Option<usize>,
    /// Enable RPC server.
    #[arg(long, default_value = "false")]
    rpc: bool,
    /// RPC server port (default 8545).
    #[arg(long)]
    rpc_port: Option<u16>,
    /// RPC bind address (default 127.0.0.1).
    #[arg(long)]
    rpc_bind: Option<String>,
    /// Centralized peer server base URL. Resolution priority: CLI > PEER_SERVER_URL > config.
    #[arg(long = "peer-server-url")]
    peer_server_url: Option<String>,
    /// Path to faucet_keys_testnet.json for testnet faucet.
    #[arg(long)]
    faucet_keys_path: Option<std::path::PathBuf>,
    /// Faucet key shard index (0-based). Together with --faucet-shard-count
    /// partitions the shared faucet key pool across RPC nodes so each node
    /// owns a disjoint subset; eliminates cross-node "same key / same
    /// committed nonce" duplicate-nonce races under concurrent claim load.
    /// Example (5 RPC nodes, 10 keys): pass --faucet-shard-idx=0..4 with
    /// --faucet-shard-count=5 so each node gets 2 keys.
    #[arg(long)]
    faucet_shard_idx: Option<usize>,
    /// Total number of faucet key shards (i.e. number of RPC nodes sharing
    /// the faucet key file). Required with --faucet-shard-idx.
    #[arg(long)]
    faucet_shard_count: Option<usize>,
    /// Use testnet fee schedule (1/5/0.05 SAVT).
    #[arg(long, default_value = "false")]
    testnet_fees: bool,
    /// Wallet address to receive PoU rewards (hex, 64 chars / 32 bytes).
    /// If not set, the producer key address is used.
    #[arg(long)]
    reward_address: Option<String>,
}

async fn run_main() -> Result<()> {
    load_env_file();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info")
            // Hide libp2p dial retry warnings (they are normal during network startup)
            .add_directive("libp2p::swarm::dial=warn".parse().unwrap())
            .add_directive("libp2p::swarm::connection=warn".parse().unwrap())
            .add_directive("libp2p::identify=warn".parse().unwrap())
            // Hide specific libp2p warnings that are informational
            .add_directive("swarm=warn".parse().unwrap())
            .add_directive("noise=warn".parse().unwrap())
            .add_directive("yamux=warn".parse().unwrap())
            // ROUND 7: Suppress "Send Queue full" warnings from libp2p_gossipsub.
            // These log the ENTIRE failed message payload (~1-2KB each), causing
            // 120MB+ log files (56K+ warnings in 6 minutes). The queue saturation
            // is handled by SlowPeer disconnect; the verbose logging is noise.
            .add_directive("libp2p_gossipsub::behaviour=error".parse().unwrap())
    });
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_ansi(false)
        .with_target(false)
        .init();
    debug!("Savitri light node starting");

    // Initialize telemetry
    let startup_time = Instant::now();
    let mut metrics_shutdown = if let Err(err) = crate::telemetry::init_metrics() {
        tracing::warn!(error = %err, "metrics exporter not available; continuing without HTTP endpoint");
        None
    } else {
        Some(crate::telemetry::update_system_metrics_periodically(startup_time).await)
    };

    let args = Args::parse();
    let file_cfg = crate::config::Config::load(&args.config)
        .with_context(|| format!("failed to load config {}", args.config.display()))?;
    let resolved_peer_server = crate::peer_server::resolve_runtime_config(
        file_cfg.as_ref(),
        args.peer_server_url.clone(),
    )?;
    if resolved_peer_server.peer_server.enabled {
        match &resolved_peer_server.resolved_url {
            Some(url) => {
                info!(
                    url = %url.value,
                    source = url.source,
                    "Peer server URL resolved"
                );
            }
            None => {
                warn!(
                    allow_start_without_server =
                        resolved_peer_server.peer_server.allow_start_without_server,
                    "Peer server enabled but no URL was resolved"
                );
            }
        }
    }
    let resource_overrides = resource::Overrides {
        bandwidth_mbps: args.resource_bandwidth_mbps,
        cpu_cores: args.resource_cpu_cores,
        storage_gb: args.resource_storage_gb,
        epoch_secs: args.resource_epoch_secs,
        tolerance: args.resource_tolerance,
        weight_bandwidth: args.resource_weight_bandwidth,
        weight_cpu: args.resource_weight_cpu,
        weight_storage: args.resource_weight_storage,
    };
    let resource_monitor_cfg = resource::MonitorConfig::from_sources(
        file_cfg.as_ref().and_then(|cfg| {
            cfg.resources.clone().map(|r| config::ResourceConfig {
                bandwidth_mbps: r.bandwidth_mbps,
                cpu_cores: r.cpu_cores,
                storage_gb: r.storage_gb,
                epoch_secs: r.epoch_secs,
                tolerance: r.tolerance,
                weights: config::ResourceWeights {
                    bandwidth: r.weights.bandwidth,
                    cpu: r.weights.cpu,
                    storage: r.weights.storage,
                },
            })
        }),
        resource_overrides,
    );
    let resource_monitor_cfg = (!USE_DEFAULT_POU_ONLY).then_some(resource_monitor_cfg);
    let mut bootstrap_peers = if !args.bootstrap.is_empty() {
        args.bootstrap.clone()
    } else {
        file_cfg
            .as_ref()
            .map(|cfg| cfg.bootstrap_peers.clone())
            .unwrap_or_default()
    };
    let mut masternode_peers = file_cfg
        .as_ref()
        .map(|cfg| cfg.masternode_peers.clone())
        .unwrap_or_default();

    // Peer registry auto-bootstrap: if bootstrap_peers is empty and a registry URL
    // is configured, fetch the canonical peer list before failing.
    if bootstrap_peers.is_empty() {
        if let Some(url) = file_cfg
            .as_ref()
            .and_then(|c| c.peer_registry_url.as_deref())
        {
            info!(url = %url, "bootstrap_peers empty — fetching from peer registry");
            savitri_lightnode::peer_registry::seed_from_url(
                url,
                &mut bootstrap_peers,
                &mut masternode_peers,
            )
            .await;
        }
    }

    if masternode_peers.is_empty() {
        masternode_peers = discover_masternode_peers();
        debug!(
            "discovered {} masternode peers through dynamic discovery",
            masternode_peers.len()
        );
    }
    if !masternode_peers.is_empty() {
        debug!(
            count = masternode_peers.len(),
            "Loaded masternode priority peers"
        );
    }
    if bootstrap_peers.is_empty() {
        anyhow::bail!(
            "no bootstrap peers configured; provide --bootstrap, add bootstrap_peers to {}, or set peer_registry_url",
            args.config.display()
        );
    }

    let listen_port = resolve_listen_port(args.listen_port, file_cfg.as_ref())?;
    debug!(port = listen_port, "Configured libp2p listen port");

    // Resolve DB path (CLI override > config > per-port default).
    let memory_only = file_cfg
        .as_ref()
        .and_then(|c| c.memory_only)
        .unwrap_or(false);
    let db_path = if args.db != "lightnode.db" {
        std::path::PathBuf::from(&args.db)
    } else {
        file_cfg
            .as_ref()
            .and_then(|c| c.db_path.as_ref())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from(format!("lightnode-{}.db", listen_port)))
    };
    if let Some(parent) = db_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("failed to create DB parent directory {}", parent.display())
            })?;
        }
    }

    let storage: Arc<dyn BlockAndAccountStorageTrait> = if memory_only {
        Arc::new(Storage::new(&db_path)?)
    } else {
        Arc::new(RocksDBLightnodeStorage::from_path(&db_path)?)
    };
    ensure_genesis_block(storage.as_ref())?;

    // Resolve key paths (CLI override > per-port default).
    let producer_key_path =
        if args.producer_key_path != std::path::PathBuf::from("lightnode-producer.key") {
            args.producer_key_path.clone()
        } else {
            std::path::PathBuf::from(format!("lightnode-producer-{}.key", listen_port))
        };
    if let Some(parent) = producer_key_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create producer key parent directory {}",
                    parent.display()
                )
            })?;
        }
    }
    let producer_kp: std::sync::Arc<DalekKeypair> =
        std::sync::Arc::new(load_or_generate_ed25519(&producer_key_path)?);
    let producer_addr = producer_kp.verifying_key().to_bytes();
    ensure_funded_account(storage.as_ref(), &producer_addr)?;
    debug!(
        public_key = %hex::encode(producer_addr),
        "Loaded producer signing key from {}",
        producer_key_path.display()
    );

    let tx_key_path = if args.tx_key_path != std::path::PathBuf::from("lightnode-tx.key") {
        args.tx_key_path.clone()
    } else {
        std::path::PathBuf::from(format!("lightnode-tx-{}.key", listen_port))
    };
    if let Some(parent) = tx_key_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create transaction key parent directory {}",
                    parent.display()
                )
            })?;
        }
    }
    let tx_kp: std::sync::Arc<DalekKeypair> =
        std::sync::Arc::new(load_or_generate_ed25519(&tx_key_path)?);
    let tx_addr = tx_kp.verifying_key().to_bytes();
    ensure_funded_account(storage.as_ref(), &tx_addr)?;
    info!(
        public_key = %hex::encode(tx_addr),
        "Loaded transaction signing key from {}",
        tx_key_path.display()
    );

    let bootstrap_accounts = bootstrap_accounts_from_config(&bootstrap_peers)?;
    seed_bootstrap_accounts(storage.as_ref(), &bootstrap_accounts)?;

    // Seed genesis accounts from config so every node has the same funded set.
    let configured_genesis_accounts: Vec<[u8; 32]> = if let Some(ref cfg) = file_cfg {
        if !cfg.genesis_accounts.is_empty() {
            let genesis_addrs = parse_genesis_accounts(&cfg.genesis_accounts)?;
            seed_bootstrap_accounts(storage.as_ref(), &genesis_addrs)?;
            info!(
                "Seeded {} genesis accounts from config",
                genesis_addrs.len()
            );
            genesis_addrs
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    let tx_recipients = parse_tx_recipients(&args.tx_recipients)?;
    let mut tx_gen_sender_keys: Vec<Arc<DalekKeypair>> = Vec::new();
    if !args.tx_gen_sender_keys.is_empty() {
        let mut seen_sender_addrs = HashSet::<[u8; 32]>::new();
        for key_path in &args.tx_gen_sender_keys {
            if !key_path.exists() {
                bail!(
                    "tx generator sender key file does not exist: {}",
                    key_path.display()
                );
            }
            let kp = Arc::new(load_or_generate_ed25519(key_path)?);
            let sender_addr = kp.verifying_key().to_bytes();
            if seen_sender_addrs.insert(sender_addr) {
                tx_gen_sender_keys.push(kp);
            }
        }
        info!(
            extra_sender_keys = tx_gen_sender_keys.len(),
            "Loaded additional tx generator sender keys"
        );
    }

    for (peer, score, addr) in DEFAULT_POU_SCORES {
        debug!(peer = %peer, address = %addr, pou = %score, "Loaded default PoU score");
    }

    let cfg_ref = file_cfg.as_ref();
    let tx_interval_secs = args
        .tx_interval_secs
        .or_else(|| cfg_ref.and_then(|cfg| cfg.tx_interval_secs));
    // hardcodes `heartbeat_interval_secs=5` while the operator sets a different
    // SAVITRI_HEARTBEAT_INTERVAL_MS at wipe time silently uses the TOML value,
    // producing epoch_ms = TOML_heartbeat × env_slots that mismatches every
    // other node. Result: group_id format `group_<E>_<idx>_<E>` diverges
    // across the cluster, intra_group_tx_topic names diverge, gossipsub mesh
    // fragments, and the elected proposer never receives cross-group TX —
    // observed as `main_total=0` on Elected nodes while non-proposer members
    // hold all the local-admit TX (memory diag_mempool_proposer_asymmetry_2026-05-03).
    let heartbeat_interval_secs = std::env::var("SAVITRI_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v >= 1000) // millis → secs, must be ≥ 1s
        .map(|ms| ms / 1000)
        .or_else(|| cfg_ref.map(|cfg| cfg.heartbeat_interval_secs))
        .unwrap_or(config::DEFAULT_HEARTBEAT_INTERVAL_SECS)
        .max(1);
    let heartbeat_interval = Duration::from_secs(heartbeat_interval_secs);
    let block_interval_secs = args
        .block_interval_secs
        .or_else(|| cfg_ref.and_then(|cfg| cfg.block_interval_secs));
    // tx_interval_secs = 0 means max speed (tx.rs converts Duration::ZERO to 100ms).
    // tx_interval_secs = None (not set) means disabled (no tx generator).
    let tx_interval = match tx_interval_secs {
        None => None,
        Some(0) => Some(Duration::ZERO), // tx.rs converts to 100ms → ~2000 TPS with 10 recipients
        Some(secs) => Some(Duration::from_secs(secs)),
    };
    // block_interval_secs = 0 means disabled.
    // If not provided at all, keep a conservative default polling cadence.
    let block_interval = match block_interval_secs {
        Some(0) => None,
        Some(secs) => Some(Duration::from_secs(secs)),
        None => Some(Duration::from_secs(1)), // consensus-driven default
    };

    let (heartbeat_event_tx, heartbeat_event_rx) = if USE_DEFAULT_POU_ONLY {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel::<availability::HeartbeatEvent>(1024);
        (Some(tx), Some(rx))
    };
    let (resource_event_tx, resource_event_rx) = if USE_DEFAULT_POU_ONLY {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel::<resource::ResourceEvent>(2048);
        (Some(tx), Some(rx))
    };
    let (integrity_event_tx, integrity_event_rx) = if USE_DEFAULT_POU_ONLY {
        (None, None)
    } else {
        #[cfg(feature = "desktop")]
        {
            let (tx, rx) = mpsc::channel::<integrity::IntegrityEvent>(1024);
            (Some(tx), Some(rx))
        }
        #[cfg(not(feature = "desktop"))]
        {
            (None, None)
        }
    };

    let network_key_path =
        if args.network_key_path != std::path::PathBuf::from("lightnode-network.key") {
            args.network_key_path.clone()
        } else {
            std::path::PathBuf::from(format!("lightnode-network-{}.key", listen_port))
        };
    if let Some(parent) = network_key_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create network key parent directory {}",
                    parent.display()
                )
            })?;
        }
    }

    info!("Initializing network and syncing with masternode...");
    info!("STEP 1/7: Starting network initialization");

    // Create network configuration
    let network_config = crate::config::Config {
        listen_port,
        bootstrap_peers: bootstrap_peers.clone(),
        masternode_peers: masternode_peers.clone(),
        network_key_path: network_key_path.clone(),
        tx_interval_secs: args.tx_interval_secs,
        block_interval_secs: args.block_interval_secs,
        max_block_txs: args.max_block_txs,
        min_tx_per_second_per_recipient: file_cfg
            .as_ref()
            .and_then(|c| c.min_tx_per_second_per_recipient),
        resource_weight_bandwidth: args.resource_weight_bandwidth,
        resource_weight_cpu: args.resource_weight_cpu,
        resource_weight_storage: args.resource_weight_storage,
        heartbeat_interval_secs,
        // (default 20), while MN read SAVITRI_SLOTS_PER_EPOCH env (=200). Result:
        // 21 LN computed epoch ≈ 1486 (slots=20), 5 MN computed epoch ≈ 151
        // (slots=200) — same wall-clock, divergent epoch by 10×. Group_id format
        // "group_{epoch}_{idx}_{epoch}" embeds the epoch, so LN and MN created
        // disjoint group topics. TX gossip on /savitri/group/group_1486_2_1486/tx
        // never reached the MN-side proposal handler, mempool stayed at 0, blocks
        // were tx_count=0. The TOML default was never bumped to 200 because the
        // P2 commit, per MN comment). Mirror the read so the LN converges.
        slots_per_epoch: std::env::var("SAVITRI_SLOTS_PER_EPOCH")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .or_else(|| cfg_ref.map(|c| c.slots_per_epoch))
            .unwrap_or(config::DEFAULT_SLOTS_PER_EPOCH),
        // `genesis_timestamp_ms = <old_wipe_ts>` silently overrode the env
        // SAVITRI_GENESIS_TIMESTAMP_MS the wipe script sets. The MN main.rs
        // already enforces ENV > DB > cfg (lines 386–433); here we mirror
        // that for the LN so all nodes converge on the same wall-clock
        // origin and current_epoch stays consistent across the cluster.
        genesis_timestamp_ms: std::env::var("SAVITRI_GENESIS_TIMESTAMP_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .or_else(|| cfg_ref.and_then(|c| c.genesis_timestamp_ms)),
        resources: file_cfg.as_ref().and_then(|cfg| {
            cfg.resources.clone().map(|r| config::ResourceConfig {
                bandwidth_mbps: r.bandwidth_mbps,
                cpu_cores: r.cpu_cores,
                storage_gb: r.storage_gb,
                epoch_secs: r.epoch_secs,
                tolerance: r.tolerance,
                weights: config::ResourceWeights {
                    bandwidth: r.weights.bandwidth,
                    cpu: r.weights.cpu,
                    storage: r.weights.storage,
                },
            })
        }),
        memory_only: file_cfg.as_ref().and_then(|c| c.memory_only),
        db_path: file_cfg.as_ref().and_then(|c| c.db_path.clone()),
        rpc_enabled: file_cfg.as_ref().and_then(|c| c.rpc_enabled),
        rpc_port: file_cfg.as_ref().and_then(|c| c.rpc_port),
        rpc_bind_addr: file_cfg.as_ref().and_then(|c| c.rpc_bind_addr.clone()),
        faucet_keys_path: args
            .faucet_keys_path
            .clone()
            .or_else(|| file_cfg.as_ref().and_then(|c| c.faucet_keys_path.clone())),
        testnet_fees: Some(
            args.testnet_fees
                || file_cfg
                    .as_ref()
                    .and_then(|c| c.testnet_fees)
                    .unwrap_or(false),
        ),
        external_ip: resolve_external_ip(file_cfg.as_ref()).await?,
        relay_servers: file_cfg
            .as_ref()
            .map(|c| c.relay_servers.clone())
            .unwrap_or_default(),
        peer_registry_url: file_cfg.as_ref().and_then(|c| c.peer_registry_url.clone()),
        peer_server: resolved_peer_server.peer_server.clone(),
        enable_upnp: file_cfg.as_ref().and_then(|c| c.enable_upnp),
        reward_address: args
            .reward_address
            .clone()
            .or_else(|| file_cfg.as_ref().and_then(|c| c.reward_address.clone())),
        genesis_accounts: file_cfg
            .as_ref()
            .map(|c| c.genesis_accounts.clone())
            .unwrap_or_default(),
    };
    let slots_per_epoch = network_config.slots_per_epoch;

    // Reward/payout address (Ethereum/Solana style): earnings go here when set; else producer key
    let effective_reward_address: Option<[u8; 32]> = network_config
        .reward_address
        .as_ref()
        .and_then(|s| hex::decode(s.trim()).ok().and_then(|v| v.try_into().ok()));
    if let Some(ref addr) = effective_reward_address {
        info!(reward_address = %hex::encode(addr), "Reward/payout address set; node earnings will be credited here");
    }

    let network_keypair = crate::signer::load_or_generate_ed25519(&network_key_path)?;

    // Generate and log the unique peer ID for this lightnode instance
    let local_peer_id = libp2p::PeerId::from_public_key(&libp2p::identity::PublicKey::from(
        libp2p::identity::ed25519::PublicKey::try_from_bytes(
            &network_keypair.verifying_key().to_bytes(),
        )?,
    ));
    info!(
        "Lightnode unique peer ID: {} (port: {}, key: {})",
        local_peer_id,
        listen_port,
        network_key_path.display()
    );
    if let Err(e) = save_peer_id_to_file(local_peer_id, listen_port) {
        warn!(
            error = %e,
            port = listen_port,
            "Failed to persist local peer ID file"
        );
    } else {
        info!(
            peer_id = %local_peer_id,
            port = listen_port,
            "Saved local peer ID file"
        );
    }

    // Create receivers
    let (block_tx, block_rx) = mpsc::channel::<crate::p2p::types::BlockBroadcast>(8192);
    let block_receiver = crate::p2p::types::BlockReceiver { tx: block_tx };

    let (_certificate_tx, certificate_rx) =
        mpsc::channel::<crate::p2p::types::ConsensusCertificate>(8192);

    let (_integrity_tx, integrity_rx) = mpsc::channel::<crate::integrity::IntegrityEvent>(1024);

    let (pou_tx, pou_rx) = mpsc::channel::<crate::p2p::types::PouBroadcast>(1024);
    let shared_pou_score: availability::SharedPouScore = Arc::new(tokio::sync::RwLock::new(None));

    // Shared mempool and flag: block producer and intra-group proposer use the same pipeline;
    // when we are the intra-group elected proposer, run_block_producer skips production.
    let use_testnet_fees = args.testnet_fees
        || file_cfg
            .as_ref()
            .and_then(|c| c.testnet_fees)
            .unwrap_or(false);
    let admission_config = if use_testnet_fees {
        Some(savitri_mempool::mempool::admission::AdmissionConfig::testnet_fees())
    } else {
        None
    };
    let mempool_adapter = MempoolPipeline::new_with_storage_and_config(
        storage.clone() as Arc<dyn savitri_storage::StorageTrait>,
        admission_config,
    );

    // Node-wide PoU observation store. Shared between:
    //   * the MempoolPipeline FL aggregation pipeline (writes per-client
    //     gradient scores via `record_fl_contribution`),
    //   * the network task (writes RTT samples via `record_latency` and
    //   * the consensus PoU calculator (reads them to build the score).
    // Owning the store at the binary top-level is the only way to keep
    // these three layers writing to the same instance without leaking a
    // savitri-consensus dependency into savitri-mempool.
    let pou_observations = Arc::new(savitri_consensus::scoring::ObservationStore::new());

    // Wire the mempool's FL score sink into the shared observation store.
    // Each gradient that passes through `aggregate_federated_updates`
    // produces a `(peer_hex, round_id, score_permille)` tuple; the sink
    // forwards it so the PoU `fl_integrity_score` component reflects FL
    // quality across rounds. Capturing the Arc by clone keeps the closure
    // 'static and sharable across threads.
    {
        let store = Arc::clone(&pou_observations);
        mempool_adapter.set_fl_score_sink(Arc::new(
            move |peer_hex: &str, round_id: u64, score: u16| {
                store.record_fl_contribution(peer_hex, round_id, score);
            },
        ));
    }

    // Wire the PoU score provider — closes the C3 narrative loop:
    // the same observation surface that drives `PouCalculator.calculate_score`
    // (and therefore proposer eligibility) is now also queried inside
    // `aggregate_federated_updates` to scale FedAvg weights. A peer with
    // PoU=300 has its FL contribution effectively counted at 30% of its
    // raw token-amount weight; a peer with PoU=900 at 90%.
    {
        let store = Arc::clone(&pou_observations);
        mempool_adapter.set_pou_score_provider(Arc::new(move |peer_hex: &str| -> u16 {
            store.derive_observation_score(peer_hex)
        }));
    }

    // Slashing manager wired to the same observation store. Slashes
    // recorded here (whether triggered by BFT layer or the FL streak
    // daemon below) mirror into the store as `SlashEvent`s, which the
    // integrity scorer in PoU then uses to penalise the peer.
    let mut _slasher_init = savitri_consensus::slashing::SlashingManager::with_defaults();
    _slasher_init.set_observations(Arc::clone(&pou_observations));
    let slasher = Arc::new(_slasher_init);

    // FL streak daemon — closes the B4 TODO. Polls every 60s; for every
    // peer whose recent FL scores fell below 200 permille for 3 rounds
    // straight, files a MaliciousGradient slash. The slasher rejects
    // duplicates within the cooldown window so re-firing is harmless.
    let _fl_streak_daemon = savitri_consensus::scoring::streak_daemon::spawn_fl_streak_daemon(
        Arc::clone(&pou_observations),
        Arc::clone(&slasher),
        savitri_consensus::scoring::streak_daemon::DEFAULT_STREAK_POLL_SECS,
        savitri_consensus::scoring::streak_daemon::default_epoch_provider(),
    );

    let mempool_for_rpc = mempool_adapter.inner_for_rpc();
    // RealMempoolPipeline owns its own synchronization (RPC submit and
    // proposer drain operate on disjoint sections), so the historical
    // `Arc<tokio::sync::Mutex<MempoolPipeline>>` outer wrapper was
    // redundant — and risky, because it serialized RPC ingress against
    // the long-running drain critical section.
    let mempool_pipeline: crate::p2p::block::LightnodeMempoolHandle = Arc::new(mempool_adapter);

    // proposer drain handle point to the SAME `RealMempoolPipeline` Arc.
    // Without this invariant the RPC could write to one instance while the
    // proposer drains another (an earlier fix silent-disconnect pattern). The clone
    // taken below is dropped immediately — only the pointer identity matters.
    {
        let proposer_view = mempool_pipeline.inner_for_rpc();
        assert!(
            std::sync::Arc::ptr_eq(&mempool_for_rpc, &proposer_view),
            "an earlier fix invariant violated: RPC mempool handle is not the same Arc as the proposer mempool handle"
        );
        tracing::info!(
            "Mempool ptr_eq invariant verified: RPC handle and proposer handle share state"
        );
    }

    // in_flight_by_block entries older than the configured threshold. Path C
    // (cert-MATCHED restore) covers the multi-group fork case deterministically;
    // this worker catches everything else (proposer crash mid-round, cert
    // dropped on the network, partition healing, etc.). Tunables via env:
    //   SAVITRI_INFLIGHT_RESTORE_INTERVAL_SECS (default 10)
    //   SAVITRI_INFLIGHT_RESTORE_MAX_AGE_SECS  (default 30, must exceed BFT round-trip)
    {
        let pipeline_for_restore = mempool_pipeline.clone();
        let interval_secs: u64 = std::env::var("SAVITRI_INFLIGHT_RESTORE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        let max_age_secs: u64 = std::env::var("SAVITRI_INFLIGHT_RESTORE_MAX_AGE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        tokio::spawn(async move {
            let max_age = std::time::Duration::from_secs(max_age_secs);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let restored = pipeline_for_restore.restore_in_flight_older_than(max_age);
                if restored > 0 {
                    tracing::warn!(
                        restored,
                        max_age_secs,
                        "an earlier fix Path B: timeout-restored stale in-flight TXs"
                    );
                }
            }
        });
    }

    // logger of cert lifecycle counters (received/valid/invalid/match/
    // miss/lock_busy). The cert handler (network/mod.rs ~line 2960) now
    // increments process-local atomics on every cert event, exposed via
    // `ConsensusObsMetrics::cert_snapshot`. The ratios computed here
    // (match/received and miss/match) tell us whether the bottleneck is
    // (b) cert handler congestion (high lock_busy%) or
    // (c) BFT vote aggregation timeout (low received% vs propose rate).
    // Single 10s snapshot per binary instance — cheap, zero-allocation.
    {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let (received, valid, invalid, matched, miss, lock_busy) =
                    crate::observability::ConsensusObsMetrics::cert_snapshot();
                let match_ratio_pct = if received > 0 {
                    (matched as f64 / received as f64) * 100.0
                } else {
                    0.0
                };
                let valid_ratio_pct = if received > 0 {
                    (valid as f64 / received as f64) * 100.0
                } else {
                    0.0
                };
                tracing::warn!(
                    received,
                    valid,
                    invalid,
                    matched,
                    miss,
                    lock_busy,
                    match_ratio_pct = format!("{:.2}", match_ratio_pct),
                    valid_ratio_pct = format!("{:.2}", valid_ratio_pct),
                    "DIAG[cert-rate]: 10s snapshot"
                );
            }
        });
    }

    // periodic 10s snapshot of the entire RPC->router->gossip->mempool->
    // block-production pipeline. Counters are process-local atomics
    // bumped at every stage; this is the single log line that says
    // "where do TX go and why aren't they getting into blocks".
    //
    // Output fields (one log line, one per LN, every 10s):
    //   ROUTER:    routes, local, local_no_grp, forward, retry, fallback
    //              fwd_direct_ok / _fail, fwd_gossip_ok / _fail
    //   GOSSIP_RX: received, decoded, decode_fail, fwd_to_mempool
    //   BLOCK:     proposed, throttled (mempool < MIN_TX_PER_BLOCK),
    //              heartbeat_emitted, pipeline_full
    {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let s = crate::observability::PipelineObsMetrics::snapshot();
                tracing::warn!(
                    routes = s[0],
                    local = s[1],
                    local_no_grp = s[2],
                    forward = s[3],
                    retry = s[4],
                    fallback = s[5],
                    fwd_direct_ok = s[6],
                    fwd_direct_fail = s[7],
                    fwd_gossip_ok = s[8],
                    fwd_gossip_fail = s[9],
                    gossip_rx = s[10],
                    gossip_decoded = s[11],
                    gossip_decode_fail = s[12],
                    gossip_fwd_mempool = s[13],
                    blocks_proposed = s[14],
                    blocks_throttled_density = s[15],
                    blocks_heartbeat = s[16],
                    blocks_pipeline_full = s[17],
                    "DIAG[pipeline]: 10s snapshot"
                );
            }
        });
    }

    // recorder of every counter that distinguishes "TX in main_pool",
    // "TX in queued_pool", and "TX rejected by queued_pool gates". Built
    // because the loadtest sees RPC accept=N but proposer drain reads
    // mempool=0 — this log lets us see exactly which bucket the TX land in.
    {
        let pipeline_for_diag = mempool_pipeline.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(10));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let (
                    main_total,
                    rv_len,
                    sum_queue,
                    non_empty,
                    queued_total,
                    queued_accounts,
                    queued_promoted,
                    queued_expired,
                    queued_rej_full,
                    queued_rej_gap,
                ) = pipeline_for_diag.diag_full_state();
                tracing::warn!(
                    main_total,
                    rv_len,
                    sum_queue,
                    non_empty,
                    queued_total,
                    queued_accounts,
                    queued_promoted,
                    queued_expired,
                    queued_rej_full,
                    queued_rej_gap,
                    "DIAG[mempool-zero]: 10s snapshot"
                );
            }
        });
    }

    let is_intragroup_proposer = Arc::new(tokio::sync::RwLock::new(false));
    // When true, this node is in an intra-group: only the elected proposer should drain mempool; main block producer must not drain.
    let is_in_intra_group = Arc::new(tokio::sync::RwLock::new(false));

    // between the network task (writer — populates from GroupAnnouncement
    // gossip) and the tx_router (reader — consults at route() time for
    // cache internally and the network task had no handle to it, so
    // update_from_announce was never invoked outside unit tests.
    let proposer_cache_shared = crate::tx_router::peer_lookup::ProposerCache::default();

    let network = start_network(
        network_config,
        network_keypair,
        producer_addr,
        effective_reward_address,
        storage.clone(),
        block_receiver,
        certificate_rx,
        integrity_rx,
        pou_rx,
        resource_event_tx.clone(),
        heartbeat_event_tx.clone(),
        integrity_event_tx.clone(),
        Some(mempool_pipeline.clone()),
        Some(is_intragroup_proposer.clone()),
        Some(Arc::clone(&shared_pou_score)),
        Some(is_in_intra_group.clone()),
        Some(Arc::clone(&pou_observations)),
        Some(proposer_cache_shared.clone()),
    )
    .await?;

    #[cfg(not(feature = "rpc"))]
    {
        if args.rpc {
            tracing::error!("--rpc flag passed but binary was compiled WITHOUT the 'rpc' feature. Rebuild with: cargo build --release -p savitri-lightnode --features rpc");
        }
    }
    #[cfg(feature = "rpc")]
    {
        let rpc_enabled = args.rpc
            || file_cfg
                .as_ref()
                .and_then(|c| c.rpc_enabled)
                .unwrap_or(false);
        if rpc_enabled {
            let rpc_port = args
                .rpc_port
                .or_else(|| file_cfg.as_ref().and_then(|c| c.rpc_port))
                .unwrap_or(8545);
            let rpc_bind = args
                .rpc_bind
                .clone()
                .or_else(|| file_cfg.as_ref().and_then(|c| c.rpc_bind_addr.clone()))
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let addr: std::net::SocketAddr = format!("{}:{}", rpc_bind, rpc_port)
                .parse()
                .context("invalid RPC bind address")?;
            let storage_rpc = storage.clone() as Arc<dyn savitri_storage::StorageTrait>;
            let pou_reader = Arc::new(rpc::LightnodePouReader::new(network.pou_state.clone()));
            let network_reader = Arc::new(rpc::LightnodeNetworkReader::new(
                network.connected_peers.clone(),
            ));
            let faucet_path = args
                .faucet_keys_path
                .clone()
                .or_else(|| file_cfg.as_ref().and_then(|c| c.faucet_keys_path.clone()));
            // Channel-based TX ingestion: decouples RPC from mempool lock
            // Block production never contends with RPC — consumer is the only writer
            // tx_broadcast: after mempool insertion, raw TX bytes are sent here for gossipsub propagation
            let (tx_sender, tx_receiver) =
                tokio::sync::mpsc::channel::<savitri_rpc::TxSubmission>(2000);
            let (tx_broadcast_tx, mut tx_broadcast_rx) =
                tokio::sync::mpsc::channel::<Vec<u8>>(2000);
            let _consumer_handle = savitri_rpc::spawn_mempool_consumer(
                tx_receiver,
                mempool_for_rpc.clone(),
                Some(tx_broadcast_tx),
            );

            // Spawn announce-hash TX broadcaster.
            //
            // Publishes HaveTx(hashes) — 32-byte hash announcements via gossipsub
            // on /savitri/tx/1. Receiving nodes use TxFetch (request-response)
            // to pull the full bytes from any peer that has them (see
            // p2p/network/mod.rs HaveTx handler).
            //
            // Batch tuning: 256 hashes × 500 ms. Reverted from 64 × 100 ms after
            // measurements showed the smaller-batch variant capped end-to-end
            // 10× higher gossipsub publish rate starved block production on
            // the same tokio runtime; the mesh flood completed against the
            // proposer's drain loop for CPU/lock contention.
            let broadcast_cmd_tx = network.command_tx.clone();
            let tx_store_for_broadcast = network.tx_store.clone();
            let local_peer_bytes = local_peer_id.to_bytes();
            tokio::spawn(async move {
                tracing::info!("TX announce-hash broadcaster started (HaveTx, 256×500ms batches)");
                let mut total = 0u64;
                let mut first_recv_logged = false;
                let tx_topic = libp2p::gossipsub::IdentTopic::new("/savitri/tx/1");
                const BATCH_MAX: usize = 256;
                let flush_interval = tokio::time::Duration::from_millis(500);
                let mut hash_buffer: Vec<[u8; 32]> = Vec::with_capacity(BATCH_MAX);

                loop {
                    let tx_bytes = match tx_broadcast_rx.recv().await {
                        Some(b) => b,
                        None => {
                            tracing::warn!("TX announce broadcaster: recv returned None (channel closed), task exiting");
                            break;
                        }
                    };
                    if !first_recv_logged {
                        tracing::info!(
                            len = tx_bytes.len(),
                            "TX announce broadcaster: first TX received"
                        );
                        first_recv_logged = true;
                    }

                    // Store bytes locally (sync — no .await) and queue the hash.
                    let hash = crate::p2p::broadcast::hash_signed_tx_bytes(&tx_bytes);
                    tx_store_for_broadcast.insert(hash, tx_bytes);
                    hash_buffer.push(hash);

                    // Drain channel for up to flush_interval to batch hashes.
                    let deadline = tokio::time::Instant::now() + flush_interval;
                    while hash_buffer.len() < BATCH_MAX {
                        match tokio::time::timeout_at(deadline, tx_broadcast_rx.recv()).await {
                            Ok(Some(tx_bytes)) => {
                                let h = crate::p2p::broadcast::hash_signed_tx_bytes(&tx_bytes);
                                tx_store_for_broadcast.insert(h, tx_bytes);
                                hash_buffer.push(h);
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }

                    // Publish HaveTx with batch of hashes — includes source_peer so
                    // receivers know who has the full TX data (not the gossipsub relay).
                    if !hash_buffer.is_empty() {
                        let batch_size = hash_buffer.len();
                        total += batch_size as u64;
                        let have_msg =
                            crate::p2p::types::GossipMessage::HaveTx(crate::p2p::types::HaveTx {
                                hash: hash_buffer[0],
                                tx_hashes: std::mem::take(&mut hash_buffer),
                                source_peer: local_peer_bytes.clone(),
                            });
                        match crate::p2p::broadcast::encode_gossip(&have_msg) {
                            Ok(payload) => {
                                let payload_len = payload.len();
                                let cmd = crate::p2p::swarm_commands::SwarmCommand::Publish {
                                    topic: tx_topic.clone(),
                                    payload,
                                };
                                let t_send = std::time::Instant::now();
                                match broadcast_cmd_tx.send(cmd).await {
                                    Err(e) => {
                                        tracing::warn!(error = ?e, batch_size, "TX announce publish failed: cmd channel send Err");
                                    }
                                    Ok(()) => {
                                        let send_us = t_send.elapsed().as_micros() as u64;
                                        if total <= 10 || total % 2000 == 0 || send_us > 100_000 {
                                            tracing::info!(
                                                total,
                                                batch_size,
                                                payload_len,
                                                send_us,
                                                "TX announce: HaveTx published"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(error = ?e, batch_size,
                                    "TX announce publish failed: encode_gossip Err — HaveTx serialization broken");
                            }
                        }
                    }
                }
            });
            info!("TX ingestion channel started (capacity: 2000, gossipsub broadcast enabled)");

            let dag_reader = Arc::new(rpc::LightnodeDagReader::new(network.dag_manager.clone()));

            // P1: build shard-aware TX router so cross-group RPC-submitted TX get
            // forwarded to the right group's topic instead of rotting in our mempool.
            // Safe under SAVITRI_FORCE_SINGLE_GROUP=1: the shard→group map collapses
            // every shard to `group_singleton_0` which always equals local_group,
            // so route() returns Local and this path is zero-overhead.
            let tx_router: Arc<dyn savitri_rpc::TxRouter> = {
                let gm_for_closure = network.group_manager.clone();
                let local_group_fn: crate::tx_router::LocalGroupFn = Arc::new(move || {
                    gm_for_closure
                        .get_current_group_cached()
                        .map(|g| g.group_id.clone())
                });
                // the SAME ProposerCache instance the network task populates.
                let cache_with_swarm = proposer_cache_shared
                    .clone()
                    .with_swarm_tx(network.command_tx.clone());
                Arc::new(crate::tx_router::LightnodeTxRouter::new_with_cache(
                    network.shard_to_group.clone(),
                    network.num_shards.clone(),
                    local_group_fn,
                    network.command_tx.clone(),
                    cache_with_swarm,
                ))
            };

            let proposer_state_reader: Arc<dyn savitri_rpc::ProposerStateReader> =
                Arc::new(rpc::LightnodeProposerState::new(
                    is_intragroup_proposer.clone(),
                    network.group_manager.clone(),
                    local_peer_id.to_string(),
                    network.shard_to_group.clone(),
                    network.num_shards.clone(),
                ));
            let mut rpc_state = savitri_rpc::RpcState::new(storage_rpc, mempool_for_rpc.clone())
                .with_pou_reader(pou_reader)
                .with_network_reader(network_reader)
                .with_tx_channel(tx_sender)
                .with_dag_reader(dag_reader)
                .with_tx_router(tx_router)
                .with_proposer_state(proposer_state_reader);
            #[cfg(feature = "contracts")]
            {
                rpc_state = rpc_state.with_contract_executor(Arc::new(
                    rpc::LightnodeContractExecutorImpl::new(storage.clone()),
                ));
            }
            if let Some(ref path) = faucet_path {
                if let Ok(faucet_cfg_full) = savitri_rpc::FaucetConfig::load_from_json_path(path) {
                    let total_keys = faucet_cfg_full.keypairs.len();
                    // Apply shard filter if both CLI args provided. Ignored
                    // (full key set used) otherwise — matches legacy behavior.
                    let faucet_cfg = match (args.faucet_shard_idx, args.faucet_shard_count) {
                        (Some(idx), Some(count)) => match faucet_cfg_full.with_shard(idx, count) {
                            Ok(sharded) => {
                                info!(
                                    shard_idx = idx,
                                    shard_count = count,
                                    keys_assigned = sharded.keypairs.len(),
                                    total_keys,
                                    "Faucet key sharding enabled"
                                );
                                sharded
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "faucet shard config rejected; falling back to full key set");
                                faucet_cfg_full
                            }
                        },
                        _ => faucet_cfg_full,
                    };
                    let n = faucet_cfg.keypairs.len();
                    rpc_state = rpc_state.with_faucet(faucet_cfg);
                    info!("Faucet enabled ({} keys) from {:?}", n, path);
                } else {
                    tracing::warn!(path = ?path, "Failed to load faucet keys, faucet disabled");
                }
            }
            let _rpc_handle = tokio::spawn(async move {
                if let Err(e) = savitri_rpc::run_server(rpc_state, addr).await {
                    tracing::error!(error = %e, "RPC server error");
                }
            });
            info!("RPC server listening on {}:{}", rpc_bind, rpc_port);
        }
    }

    // peer_accounts field doesn't exist in NetworkComponents - removed
    let mut tx_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut block_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut router_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut availability_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut resource_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut integrity_handle: Option<tokio::task::JoinHandle<()>> = None;
    // Keep the block input channel alive in block-only mode (no tx forwarder attached).
    let mut _block_input_keepalive: Option<mpsc::Sender<SignedTx>> = None;

    if let (Some(resource_monitor_cfg), Some(resource_event_rx), Some(heartbeat_event_rx)) =
        (resource_monitor_cfg, resource_event_rx, heartbeat_event_rx)
    {
        info!("STEP 2/7: Starting resource monitor task");
        let resource_scores: Arc<RwLock<HashMap<PeerId, resource::ResourceSnapshot>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let integrity_scores: Arc<RwLock<HashMap<PeerId, integrity::IntegritySnapshot>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let resource_task = tokio::spawn(resource::run_resource_monitor(
            resource_event_rx,
            resource_monitor_cfg,
            PathBuf::from(&args.db),
            network.local_peer.clone(),
            producer_addr,
            producer_kp.clone(),
            Arc::clone(&resource_scores),
        ));
        resource_handle = Some(resource_task);
        info!("STEP 2/7: ✅ Resource monitor task started");

        info!("STEP 3/7: Starting availability monitor task");
        let availability_task = tokio::spawn(availability::run_availability_monitor(
            heartbeat_event_rx,
            network.heartbeat_sender.clone(),
            network.local_peer.clone(),
            Arc::clone(&integrity_scores),
            producer_addr,
            producer_kp.clone(),
            network.pou_sender.clone(),
            heartbeat_interval,
            Some(Arc::clone(&shared_pou_score)),
            slots_per_epoch,
        ));
        availability_handle = Some(availability_task);
        info!("STEP 3/7: ✅ Availability monitor task started");

        info!("STEP 4/7: Starting integrity monitor task");
        let (integrity_adapter_tx, integrity_adapter_rx) =
            mpsc::channel::<crate::integrity::IntegrityEvent>(1024);
        let integrity_adapter_task = tokio::spawn(async move {
            if let Some(mut rx) = integrity_event_rx {
                while let Some(event) = rx.recv().await {
                    let _ = integrity_adapter_tx.send(event).await;
                }
            }
        });

        let integrity_task = tokio::spawn(integrity::run_integrity_monitor(
            integrity_adapter_rx,
            Arc::clone(&integrity_scores),
            network.local_peer.clone(),
            producer_addr,
            producer_kp.clone(),
        ));
        integrity_handle = Some(integrity_task);
        info!("STEP 4/7: ✅ Integrity monitor task started");
    } else {
        info!("STEP 2-4/7: ⚠️ Monitor tasks disabled (USE_DEFAULT_POU_ONLY=true)");
    }

    let max_block_txs = args
        .max_block_txs
        .or_else(|| cfg_ref.and_then(|cfg| cfg.max_block_txs))
        .unwrap_or(5000)
        .max(1);

    // Pre-seed mempool if --preseed-tx N was specified
    if let Some(preseed_count) = args.preseed_tx {
        if preseed_count > 0 {
            info!(
                preseed_count,
                "PRESEED: Starting mempool pre-seed for capacity testing"
            );
            let injected = tx::preseed_mempool(
                storage.clone(),
                tx_kp.clone(),
                preseed_count,
                tx_gen_sender_keys.clone(),
                configured_genesis_accounts.clone(),
                &mempool_pipeline,
            )
            .await;
            info!(
                injected,
                preseed_count, "PRESEED: Mempool pre-seed completed"
            );
        }
    }

    let min_tx_per_second_per_recipient = cfg_ref
        .and_then(|cfg| cfg.min_tx_per_second_per_recipient)
        .unwrap_or(50);
    match (tx_interval, block_interval) {
        (Some(tx_interval), Some(block_interval)) => {
            info!("STEP 5/7: Starting transaction and block production tasks");
            let (tx_tx, tx_rx) = mpsc::channel::<SignedTx>(8192);
            let (block_tx, block_rx) = mpsc::channel::<SignedTx>(8192);

            let recipient_strategy = if !tx_recipients.is_empty() {
                tx::RecipientStrategy::Static(tx_recipients.clone())
            } else {
                tx::RecipientStrategy::Shared(Arc::clone(&network.peer_accounts))
            };

            info!("STEP 5.1/7: Starting transaction generator task");
            tx_handle = Some(tokio::spawn(tx::run_tx_generator(
                storage.clone(),
                tx_kp.clone(), // Use separate transaction key
                tx_interval,
                min_tx_per_second_per_recipient,
                tx_tx,
                recipient_strategy,
                args.tx_gen_offset,
                tx_gen_sender_keys.clone(),
                configured_genesis_accounts.clone(),
            )));
            info!("STEP 5.1/7: ✅ Transaction generator task started");

            info!("STEP 5.2/7: Starting transaction forwarder task");
            router_handle = Some(tokio::spawn(forward_transactions(
                tx_rx,
                network.tx_sender.clone(),
                Some(block_tx),
                Some(network.tx_forward_sender.clone()),
                mempool_pipeline.clone(),
            )));
            info!("STEP 5.2/7: ✅ Transaction forwarder task started");

            info!("STEP 5.3/7: Starting block producer task");
            block_handle = Some(tokio::spawn(run_block_producer(
                storage.clone(),
                producer_kp.clone(),
                block_interval,
                max_block_txs,
                block_rx,
                network.block_sender.clone(),
                resource_event_tx.clone(),
                integrity_event_tx.clone(),
                network.local_peer.clone(),
                network.pou_state.clone(),
                mempool_pipeline.clone(),
                Some(is_intragroup_proposer.clone()),
                Some(is_in_intra_group.clone()),
            )));
            info!("STEP 5.3/7: ✅ Block producer task started");
            info!("STEP 5/7: ✅ All transaction and block tasks started");
        }
        (Some(tx_interval), None) => {
            info!("STEP 5/7: Starting transaction-only tasks (no block production)");
            let (tx_tx, tx_rx) = mpsc::channel::<SignedTx>(8192);
            let recipient_strategy = if !tx_recipients.is_empty() {
                tx::RecipientStrategy::Static(tx_recipients.clone())
            } else {
                tx::RecipientStrategy::Shared(Arc::clone(&network.peer_accounts))
            };
            info!("STEP 5.1/7: Starting transaction generator task");
            tx_handle = Some(tokio::spawn(tx::run_tx_generator(
                storage.clone(),
                tx_kp.clone(), // Use separate transaction key
                tx_interval,
                min_tx_per_second_per_recipient,
                tx_tx,
                recipient_strategy,
                args.tx_gen_offset,
                tx_gen_sender_keys.clone(),
                configured_genesis_accounts.clone(),
            )));
            info!("STEP 5.1/7: ✅ Transaction generator task started");

            info!("STEP 5.2/7: Starting transaction forwarder task");
            router_handle = Some(tokio::spawn(forward_transactions(
                tx_rx,
                network.tx_sender.clone(),
                None,
                Some(network.tx_forward_sender.clone()),
                mempool_pipeline.clone(),
            )));
            info!("STEP 5.2/7: ✅ Transaction forwarder task started");
            info!("STEP 5/7: ✅ Transaction-only tasks started");
        }
        (None, Some(block_interval)) => {
            info!("STEP 5/7: Starting block-only tasks (transaction generator disabled)");
            let (block_tx, block_rx) = mpsc::channel::<SignedTx>(8192);
            _block_input_keepalive = Some(block_tx);
            block_handle = Some(tokio::spawn(run_block_producer(
                storage.clone(),
                producer_kp.clone(),
                block_interval,
                max_block_txs,
                block_rx,
                network.block_sender.clone(),
                resource_event_tx.clone(),
                integrity_event_tx.clone(),
                network.local_peer.clone(),
                network.pou_state.clone(),
                mempool_pipeline.clone(),
                Some(is_intragroup_proposer.clone()),
                Some(is_in_intra_group.clone()),
            )));
            info!("STEP 5.1/7: ✅ Block producer task started (block-only mode)");
        }
        (None, None) => {
            info!("STEP 5/7: ⚠️ No transaction or block production configured");
        }
    }

    let network_command_tx = network.command_tx.clone();
    let mut network_handle = network.task;
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);
    let mut shutdown_requested = false;

    // The peer-server runtime is opt-in and is not wired up in this
    // open-source release. Keep the `Option` slot so the existing
    // `peer_server_task.take()` shutdown calls remain valid no-ops.
    // Future work: spawn `crate::peer_server::spawn(runtime)` here
    // when `resolved_peer_server.peer_server.enabled` is true.
    let mut peer_server_task: Option<crate::peer_server::PeerServerTaskHandle> = None;

    if tx_handle.is_none()
        && block_handle.is_none()
        && router_handle.is_none()
        && availability_handle.is_none()
        && resource_handle.is_none()
        && integrity_handle.is_none()
    {
        info!("STEP 6/7: No background tasks to monitor - running network task only");
        tokio::select! {
            res = &mut network_handle => {
                if let Err(err) = res {
                    warn!(error=?err, "network task failed");
                }
            }
            _ = &mut shutdown_signal => {
                shutdown_requested = true;
                info!("Shutdown signal received");
            }
        }
        if let Some(task) = peer_server_task.take() {
            task.shutdown().await;
        }
        if let Some(sender) = metrics_shutdown.take() {
            let _ = sender.send(true);
        }
        if shutdown_requested {
            let _ = network_command_tx
                .send(crate::p2p::swarm_commands::SwarmCommand::Shutdown)
                .await;
            if !network_handle.is_finished() {
                let _ = tokio::time::timeout(Duration::from_secs(5), &mut network_handle).await;
            }
            if !network_handle.is_finished() {
                network_handle.abort();
                let _ = network_handle.await;
            }
        }
        return Ok(());
    }
    info!("STEP 6/7: Starting main event loop - monitoring all background tasks");
    info!("STEP 7/7: ✅ Lightnode initialization complete - entering operational phase");

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                shutdown_requested = true;
                info!("Shutdown signal received");
                break;
            }
            res = &mut network_handle => {
                if let Err(err) = res {
                    error!(error=?err, "network task failed - attempting to continue");
                    info!("Network task failure detected - lightnode will continue operating with degraded functionality");
                    info!("Check network connectivity and peer availability if this persists");
                    // Don't break - try to continue or restart network
                    // In production, you might want to implement network restart logic
                } else {
                    info!("Network task completed successfully");
                }
                // Don't break - keep running even if network fails
            }
            res = async {
                if let Some(handle) = router_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if router_handle.is_some() => {
                match res {
                    Ok(_) => {
                        info!("Transaction forwarder task completed successfully");
                        warn!("Transaction forwarder task exited - no more transactions to forward");
                    }
                    Err(err) => {
                        error!(error=?err, "Transaction forwarder task failed");
                        info!("Transaction forwarding has stopped - transactions may not reach the network");
                        info!("This affects the ability to send transactions to masternodes");
                    }
                }
                router_handle = None;
            }
            res = async {
                if let Some(handle) = tx_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if tx_handle.is_some() => {
                match res {
                    Ok(_) => {
                        info!("Transaction generator task completed successfully");
                        warn!("Transaction generator task exited - no more transactions will be generated");
                    }
                    Err(err) => {
                        error!(error=?err, "Transaction generator task failed");
                        info!("Transaction generation has stopped - no new transactions will be created");
                        info!("This affects network activity and transaction throughput");
                    }
                }
                tx_handle = None;
            }
            res = async {
                if let Some(handle) = block_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if block_handle.is_some() => {
                match res {
                    Ok(_) => {
                        info!("Block producer task completed successfully");
                        warn!("Block producer task exited - no more blocks will be produced");
                    }
                    Err(err) => {
                        error!(error=?err, "Block producer task failed");
                        info!("Block production has stopped - no new blocks will be created");
                        info!("This affects blockchain finality and transaction processing");
                    }
                }
                block_handle = None;
            }
            res = async {
                if let Some(handle) = availability_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if availability_handle.is_some() => {
                match res {
                    Ok(_) => debug!("availability monitor task exited"),
                    Err(err) => {
                        warn!(error=?err, "availability monitor task failed");
                        info!("Availability monitoring has stopped - heartbeat messages may not be sent");
                        info!("This affects peer reputation and network visibility");
                    }
                }
                availability_handle = None;
            }
            res = async {
                if let Some(handle) = resource_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if resource_handle.is_some() => {
                match res {
                    Ok(_) => debug!("resource monitor task exited"),
                    Err(err) => {
                        warn!(error=?err, "resource monitor task failed");
                        info!("Resource monitoring has stopped - system resource tracking disabled");
                        info!("This affects PoU scoring and resource-based reputation");
                    }
                }
                resource_handle = None;
            }
            res = async {
                if let Some(handle) = integrity_handle.as_mut() {
                    handle.await
                } else {
                    Ok(())
                }
            }, if integrity_handle.is_some() => {
                match res {
                    Ok(_) => debug!("integrity monitor task exited"),
                    Err(err) => {
                        warn!(error=?err, "integrity monitor task failed");
                        info!("Integrity monitoring has stopped - transaction validation may be affected");
                        info!("This affects the ability to detect malicious or invalid transactions");
                    }
                }
                integrity_handle = None;
            }
        }

        if tx_handle.is_none()
            && block_handle.is_none()
            && router_handle.is_none()
            && availability_handle.is_none()
            && resource_handle.is_none()
            && integrity_handle.is_none()
        {
            if let Err(err) = (&mut network_handle).await {
                warn!(error=?err, "network task failed");
            }
            break;
        }
    }

    if let Some(task) = peer_server_task.take() {
        task.shutdown().await;
    }
    if let Some(sender) = metrics_shutdown.take() {
        let _ = sender.send(true);
    }
    if shutdown_requested {
        let _ = network_command_tx
            .send(crate::p2p::swarm_commands::SwarmCommand::Shutdown)
            .await;
        if !network_handle.is_finished() {
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut network_handle).await;
        }
        if !network_handle.is_finished() {
            network_handle.abort();
            let _ = network_handle.await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run_main().await {
        eprintln!("savitri-lightnode error: {}", e);
        std::process::exit(1);
    }
}

fn resolve_listen_port(
    cli_port: Option<u16>,
    file_cfg: Option<&crate::config::Config>,
) -> Result<u16> {
    const ENV_KEYS: [&str; 3] = ["LIGHTNODE_P2P_PORT", "LIGHTNODE_LISTEN_PORT", "P2P_PORT"];
    for key in ENV_KEYS {
        if let Ok(value) = std::env::var(key) {
            let port: u16 = value.parse().with_context(|| {
                format!("environment variable {key} must be a valid u16 port number, got '{value}'")
            })?;
            if port == 0 {
                bail!("environment variable {key} must be greater than zero");
            }
            return Ok(port);
        }
    }

    if let Some(port) = cli_port {
        if port == 0 {
            bail!("command-line flag --listen-port must be greater than zero");
        }
        return Ok(port);
    }

    let port = file_cfg
        .map(|cfg| cfg.listen_port)
        .unwrap_or(config::DEFAULT_LISTEN_PORT);
    if port == 0 {
        bail!("configuration field 'listen_port' must be greater than zero");
    }
    Ok(port)
}

async fn resolve_external_ip(file_cfg: Option<&crate::config::Config>) -> Result<Option<String>> {
    const ENV_KEYS: [&str; 3] = ["LIGHTNODE_EXTERNAL_IP", "EXTERNAL_IP", "PUBLIC_IP"];
    for key in ENV_KEYS {
        if let Ok(value) = std::env::var(key) {
            let ip = value.trim();
            if ip.is_empty() {
                continue;
            }
            ip.parse::<std::net::IpAddr>().with_context(|| {
                format!("environment variable {key} must be a valid IP address, got '{value}'")
            })?;
            return Ok(Some(ip.to_string()));
        }
    }

    if let Some(ip) = file_cfg.and_then(|cfg| cfg.external_ip.clone()) {
        ip.parse::<std::net::IpAddr>().with_context(|| {
            format!("configuration field external_ip must be a valid IP address, got '{ip}'")
        })?;
        return Ok(Some(ip));
    }

    Ok(crate::p2p::transport::detect_public_ip()
        .await
        .map(|ip| ip.to_string()))
}

fn load_env_file() {
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            let exe_env = exe_dir.join(".env");
            if exe_env.is_file() {
                match dotenvy::from_path(&exe_env) {
                    Ok(_) => {
                        info!(path = %exe_env.display(), "Loaded .env from executable directory");
                        return;
                    }
                    Err(err) => {
                        warn!(
                            path = %exe_env.display(),
                            error = %err,
                            "Failed to load .env from executable directory"
                        );
                    }
                }
            }
        }
    }

    if let Ok(path) = dotenvy::dotenv() {
        info!(path = %path.display(), "Loaded .env from working directory search");
    }
}

fn ensure_funded_account(
    storage: &dyn crate::storage::BlockAndAccountStorage,
    addr: &[u8; 32],
) -> Result<()> {
    // Check if account exists, if not create it with initial balance
    // Balance calcolato per supportare molte transazioni con fee reale:
    // SAVT has 8 decimals: 1 SAVT = 100_000_000 base units
    // - fee per tx: 1_000 (0.00001 SAVT)
    // - amount per tx: 1 (0.00000001 SAVT)
    // Fund each genesis account with 10_000 SAVT (enough for millions of txs)
    const MINIMUM_BALANCE_THRESHOLD: u128 = 100_000_000; // 1 SAVT
    const INITIAL_FUNDING: u128 = 1_000_000_000_000; // 10_000 SAVT (8 decimals)

    match storage.get_account(addr)? {
        Some(storage_account) => {
            // Convert to core account
            let account = savitri_core::core::types::Account {
                balance: storage_account.balance,
                nonce: storage_account.nonce,
            };
            // Check if account has sufficient balance
            if account.balance < MINIMUM_BALANCE_THRESHOLD {
                // Fund the account with initial balance sufficiente per molte transazioni con fee reale
                let mut funded_storage_account = storage_account;
                funded_storage_account.balance = INITIAL_FUNDING;
                storage.put_account(addr, &funded_storage_account)?;
                debug!(
                    "Funded account {} with initial balance {}",
                    hex::encode(addr),
                    INITIAL_FUNDING
                );
            }
        }
        None => {
            // Account doesn't exist, create it with initial balance sufficiente per molte transazioni con fee reale
            let new_storage_account = crate::storage::Account {
                balance: INITIAL_FUNDING,
                nonce: 0,
                data: Vec::new(),
            };
            storage.put_account(addr, &new_storage_account)?;
            debug!(
                "Created and funded new account {} with initial balance {}",
                hex::encode(addr),
                INITIAL_FUNDING
            );
        }
    }
    Ok(())
}

fn seed_bootstrap_accounts(
    storage: &dyn crate::storage::BlockAndAccountStorage,
    accounts: &[[u8; 32]],
) -> Result<()> {
    // SAVT has 8 decimals: 1 SAVT = 100_000_000 base units
    const BOOTSTRAP_MINIMUM_BALANCE: u128 = 100_000_000; // 1 SAVT
    const BOOTSTRAP_FUNDING: u128 = 1_000_000_000_000; // 10_000 SAVT

    for account_addr in accounts {
        // Ensure each bootstrap account has sufficient balance
        match storage.get_account(account_addr)? {
            Some(storage_account) => {
                let mut account = savitri_core::core::types::Account {
                    balance: storage_account.balance,
                    nonce: storage_account.nonce,
                };
                if account.balance < BOOTSTRAP_MINIMUM_BALANCE {
                    account.balance = BOOTSTRAP_FUNDING;
                    let mut updated_storage_account = storage_account;
                    updated_storage_account.balance = account.balance;
                    storage.put_account(account_addr, &updated_storage_account)?;
                    debug!(
                        "Seeded bootstrap account {} with bootstrap balance {}",
                        hex::encode(account_addr),
                        BOOTSTRAP_FUNDING
                    );
                }
            }
            None => {
                // Create bootstrap account if it doesn't exist
                let bootstrap_storage_account = crate::storage::Account {
                    balance: BOOTSTRAP_FUNDING,
                    nonce: 0,
                    data: Vec::new(),
                };
                storage.put_account(account_addr, &bootstrap_storage_account)?;
                debug!(
                    "Created and seeded bootstrap account {} with bootstrap balance {}",
                    hex::encode(account_addr),
                    BOOTSTRAP_FUNDING
                );
            }
        }
    }
    info!("Seeded {} bootstrap accounts", accounts.len());
    Ok(())
}

fn bootstrap_accounts_from_config(entries: &[String]) -> Result<Vec<[u8; 32]>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((_endpoint, acct_part)) = trimmed.split_once('#') {
            let body = acct_part.trim();
            if body.is_empty() {
                continue;
            }
            let body = body.strip_prefix("0x").unwrap_or(body);
            let bytes = hex::decode(body)
                .with_context(|| format!("bootstrap account '{body}' must be valid hex"))?;
            if bytes.len() != 32 {
                bail!(
                    "bootstrap account must decode to 32 bytes, got {} bytes",
                    bytes.len()
                );
            }
            let mut addr = [0u8; 32];
            addr.copy_from_slice(&bytes);
            if seen.insert(addr) {
                out.push(addr);
            }
        }
    }
    Ok(out)
}

/// Parse genesis account addresses from config (hex-encoded 32-byte public keys).
fn parse_genesis_accounts(entries: &[String]) -> Result<Vec<[u8; 32]>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for entry in entries {
        let body = entry.trim().strip_prefix("0x").unwrap_or(entry.trim());
        if body.is_empty() {
            continue;
        }
        let bytes = hex::decode(body)
            .with_context(|| format!("genesis account '{body}' must be valid hex"))?;
        if bytes.len() != 32 {
            bail!(
                "genesis account must decode to 32 bytes, got {} bytes",
                bytes.len()
            );
        }
        let mut addr = [0u8; 32];
        addr.copy_from_slice(&bytes);
        if seen.insert(addr) {
            out.push(addr);
        }
    }
    Ok(out)
}

fn parse_tx_recipients(raw: &[String]) -> Result<Vec<[u8; 32]>> {
    let mut out = Vec::with_capacity(raw.len());
    for entry in raw {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            bail!("transaction recipient entries must not be empty");
        }
        let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
        let bytes = hex::decode(body)
            .with_context(|| format!("transaction recipient '{trimmed}' is not valid hex"))?;
        if bytes.len() != 32 {
            bail!(
                "transaction recipient '{trimmed}' must decode to 32 bytes, got {} bytes",
                bytes.len()
            );
        }
        let mut addr = [0u8; 32];
        addr.copy_from_slice(&bytes);
        out.push(addr);
    }
    Ok(out)
}

async fn forward_transactions(
    mut source: mpsc::Receiver<SignedTx>,
    network_sender: mpsc::Sender<SignedTx>,
    mut block_sender: Option<mpsc::Sender<SignedTx>>,
    intra_group_sender: Option<mpsc::Sender<SignedTx>>,
    mempool_pipeline: crate::p2p::block::LightnodeMempoolHandle,
) {
    let mut sig_verify_stage = SigVerifyStage::new();

    // Buffer per accumulare transazioni prima di processarle in batch
    let mut tx_batch_buffer: Vec<Vec<u8>> = Vec::new();
    const BATCH_SIZE: usize = 2000; // Processa batch di 2000 transazioni (scaled for ~2000 TPS per generator)
    const BATCH_TIMEOUT_MS: u64 = 50; // Flush ogni 50ms per dare tempo di accumulare più tx
    const MIN_BATCH_FOR_TIMEOUT: usize = 4; // On timeout, flush only if we have at least N tx (avoids 1-tx batches)
    const STALE_FLUSH_MS: u128 = 100; // Flush batch piccoli dopo 100ms (was 2s, reduced for high-throughput)

    let mut last_batch_process = Instant::now();

    let mut batch_timer = time::interval(Duration::from_millis(BATCH_TIMEOUT_MS));
    batch_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            // Ricevi transazioni dal tx generator
            maybe_tx = source.recv() => {
                match maybe_tx {
                    Some(_tx) => {
                        // Serializza la transazione per il batch processing
                        match crate::tx::serialize_signed_tx(&_tx) {
                            Ok(tx_bytes) => {
                                // Accumula la transazione nel buffer per batch processing
                                tx_batch_buffer.push(tx_bytes);

                                // Processa il batch se è pieno
                                if tx_batch_buffer.len() >= BATCH_SIZE {
                                    if let Err(err) = process_and_forward_batch(
                                        &mut sig_verify_stage,
                                        &mut tx_batch_buffer,
                                        &network_sender,
                                        &mut block_sender,
                                        &intra_group_sender,
                                        &mempool_pipeline,
                                    ).await {
                                        warn!(error=?err, "failed to process batch; stopping forwarder");
                                        break;
                                    }
                                    last_batch_process = Instant::now();
                                }
                            }
                            Err(err) => {
                                debug!(error=?err, "failed to serialize transaction for batch processing");
                            }
                        }
                    }
                    None => {
                        // Processa eventuali transazioni rimanenti nel buffer prima di chiudere
                        if !tx_batch_buffer.is_empty() {
                            let _ = process_and_forward_batch(
                                &mut sig_verify_stage,
                                &mut tx_batch_buffer,
                                &network_sender,
                                &mut block_sender,
                                &intra_group_sender,
                                &mempool_pipeline,
                            ).await;
                        }
                        warn!("transaction stream closed; stopping forwarder");
                        break;
                    }
                }
            }
            _ = batch_timer.tick() => {
                let elapsed_ms = last_batch_process.elapsed().as_millis();
                let can_flush_by_size = tx_batch_buffer.len() >= MIN_BATCH_FOR_TIMEOUT;
                let can_flush_by_time = elapsed_ms >= BATCH_TIMEOUT_MS as u128 && tx_batch_buffer.len() >= MIN_BATCH_FOR_TIMEOUT;
                let can_flush_stale = elapsed_ms >= STALE_FLUSH_MS; // batch da 1-3 tx dopo 2s
                if !tx_batch_buffer.is_empty() && (can_flush_by_size || can_flush_by_time || can_flush_stale) {
                    if let Err(err) = process_and_forward_batch(
                        &mut sig_verify_stage,
                        &mut tx_batch_buffer,
                        &network_sender,
                        &mut block_sender,
                        &intra_group_sender,
                        &mempool_pipeline,
                    ).await {
                        warn!(error=?err, "failed to process batch; stopping forwarder");
                        break;
                    }
                    last_batch_process = Instant::now();
                }
            }
        }
    }
}

/// Processa un batch di transazioni attraverso SigVerifyStage e inoltra quelle verificate
async fn process_and_forward_batch(
    sig_verify_stage: &mut SigVerifyStage,
    tx_batch_buffer: &mut Vec<Vec<u8>>,
    network_sender: &mpsc::Sender<SignedTx>,
    block_sender: &mut Option<mpsc::Sender<SignedTx>>,
    intra_group_sender: &Option<mpsc::Sender<SignedTx>>,
    mempool_pipeline: &crate::p2p::block::LightnodeMempoolHandle,
) -> Result<()> {
    if tx_batch_buffer.is_empty() {
        return Ok(());
    }

    // Processa il batch attraverso SigVerifyStage
    let verified_results: Vec<crate::VerifiedTx> = sig_verify_stage
        .process_batch(tx_batch_buffer.to_vec())
        .await;

    let mut valid_count = 0;
    let mut invalid_count = 0;

    // Inoltra solo le transazioni verificate (valide) alla rete e al block producer.
    // If block_sender is active, avoid direct mempool insertion here to prevent
    // duplicate local ingestion (forwarder path + block-producer path).
    let block_path_active = block_sender.is_some();
    for verified_tx in verified_results {
        if verified_tx.is_valid {
            // Deserializza la transazione verificata (ora con pre_verified = true)
            match deserialize_signed_tx(&verified_tx.tx_bytes) {
                Ok(verified_signed_tx) => {
                    // Inoltra alla rete
                    if let Err(err) = network_sender.send(verified_signed_tx.clone()).await {
                        anyhow::bail!("failed to forward verified transaction to network: {}", err);
                    }

                    // Inoltra al block producer se presente.
                    // BUGFIX: use try_send instead of send().await to avoid blocking the
                    // forwarder when block_sender is full. A full channel means the block
                    // producer is temporarily saturated; dropping the tx here is safe because
                    // the tx was already broadcast to the network above. If the channel is
                    // closed (block producer exited), propagate the error and stop the forwarder.
                    if let Some(sender) = block_sender.as_mut() {
                        match sender.try_send(verified_signed_tx.clone()) {
                            Ok(_) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                debug!("block_sender channel full; dropping tx for block producer (already broadcast to network)");
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                anyhow::bail!("block producer channel closed; stopping forwarder");
                            }
                        }
                    }

                    if let Some(sender) = intra_group_sender.as_ref() {
                        let _ = sender.send(verified_signed_tx.clone()).await;
                    }

                    // Insert directly into mempool only when no block-producer input path exists.
                    if !block_path_active {
                        let _ = mempool_pipeline.add_transaction(verified_signed_tx.clone());
                    }

                    valid_count += 1;
                }
                Err(err) => {
                    debug!(error=?err, "failed to deserialize verified transaction");
                    invalid_count += 1;
                }
            }
        } else {
            invalid_count += 1;
        }
    }

    if valid_count > 0 || invalid_count > 0 {
        info!(
            batch_size = tx_batch_buffer.len(),
            valid = valid_count,
            invalid = invalid_count,
            "📤 [Forwarder] SigVerify batch -> forwarded to network + block_producer"
        );
    }

    // Reset buffer
    tx_batch_buffer.clear();

    Ok(())
}

async fn run_block_producer(
    storage: Arc<dyn BlockAndAccountStorageTrait>,
    producer_kp: Arc<DalekKeypair>,
    interval: Duration,
    max_block_txs: usize,
    mut tx_rx: mpsc::Receiver<SignedTx>,
    block_sender: mpsc::Sender<(p2p::BlockBroadcast, crate::p2p::types::PendingBlockData)>,
    resource_events: Option<mpsc::Sender<resource::ResourceEvent>>,
    integrity_sender: Option<mpsc::Sender<integrity::IntegrityEvent>>,
    local_peer: PeerId,
    pou_state: Arc<RwLock<p2p::PouState>>,
    pipeline: crate::p2p::block::LightnodeMempoolHandle,
    is_intragroup_proposer: Option<Arc<tokio::sync::RwLock<bool>>>,
    is_in_intra_group: Option<Arc<tokio::sync::RwLock<bool>>>,
) {
    let mut sig_verify_stage = SigVerifyStage::new();
    // Buffer per accumulare transazioni prima di processarle in batch attraverso SigVerifyStage
    let mut tx_batch_buffer: Vec<Vec<u8>> = Vec::new();
    const BATCH_SIZE: usize = 64; // Processa batch di 64 transazioni
    const BATCH_TIMEOUT_MS: u64 = 10; // Processa batch ogni 10ms anche se non pieno
    let mut last_batch_process = Instant::now();
    // Timer dedicato per flush of the batch - CRITICO: without questo, le tx rimangono
    const BATCH_FLUSH_INTERVAL_MS: u64 = 50;
    let mut batch_flush_timer = time::interval(Duration::from_millis(BATCH_FLUSH_INTERVAL_MS));
    batch_flush_timer.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    // BUGFIX: block_produce_ticker must be created ONCE before the loop.
    // Using sleep(100ms) inside select! recreates the future every iteration —
    // with biased; and a non-empty tx_rx, the sleep is permanently reset and
    // block production never fires. A persistent interval is unaffected by
    // other branches firing.
    let mut block_produce_ticker = time::interval(Duration::from_millis(100));
    block_produce_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut pool_log_ticker = time::interval(Duration::from_secs(5));
    pool_log_ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut resource_events = resource_events;
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ElectionKey {
        Unknown,
        Epoch(u64),
    }
    let mut last_election_notice: Option<ElectionKey> = None;
    let mut last_epoch_score_log: Option<u64> = None;
    let mut default_leader_logged = false;

    loop {
        tokio::select! {
            biased;
            maybe_tx = tx_rx.recv() => {
                match maybe_tx {
                    Some(tx) => {
                        match crate::tx::serialize_signed_tx(&tx) {
                            Ok(tx_bytes) => {
                                // Accumula la transazione nel buffer per batch processing
                                tx_batch_buffer.push(tx_bytes);

                                // Processa il batch se è pieno o se è passato il timeout
                                let should_process_batch = tx_batch_buffer.len() >= BATCH_SIZE
                                    || last_batch_process.elapsed().as_millis() >= BATCH_TIMEOUT_MS as u128;

                                if should_process_batch && !tx_batch_buffer.is_empty() {
                                    // Processa il batch attraverso SigVerifyStage
                                    let verified_results: Vec<_> = sig_verify_stage.process_batch(tx_batch_buffer.to_vec()).await;

                                    // Aggiungi le transazioni verificate alla pipeline
                                    // Converti le transazioni verificate in RawTx per la pipeline
                                    let raw_txs: Vec<_> = verified_results
                                        .into_iter()
                                        .filter(|v| v.is_valid)
                                        .map(|v| bytes_to_raw_tx(v.tx_bytes, None))
                                        .collect();

                                    let valid_count = raw_txs.len();
                                    let invalid_count = tx_batch_buffer.len() - valid_count;
                                    let accepted_count = pipeline.process_raw_transactions(raw_txs).await;
                                    let pipeline_total = pipeline.len_async().await;

                                    info!(
                                        batch_size = tx_batch_buffer.len(),
                                        valid = valid_count,
                                        invalid = invalid_count,
                                        accepted = accepted_count,
                                        pipeline_total = pipeline_total,
                                        "📦 [BlockProducer] Inline-flush: batch SigVerify -> pipeline"
                                    );

                                    // Reset buffer e timer
                                    tx_batch_buffer.clear();
                                    last_batch_process = Instant::now();
                                }
                            }
                            Err(err) => debug!(error=?err, "failed to serialize transaction for mempool ingestion"),
                        }
                    }
                    None => {
                        // Processa eventuali transazioni rimanenti nel buffer prima di chiudere
                        if !tx_batch_buffer.is_empty() {
                            let verified_results: Vec<crate::VerifiedTx> = sig_verify_stage.process_batch(tx_batch_buffer.to_vec()).await;
                            let raw_txs: Vec<_> = verified_results
                                .into_iter()
                                .filter(|v| v.is_valid)
                                .map(|v| bytes_to_raw_tx(v.tx_bytes, None))
                                .collect();
                            let _ = pipeline.process_raw_transactions(raw_txs).await;
                            tx_batch_buffer.clear();
                        }
                        warn!("transaction stream closed; stopping block producer");
                        break;
                    }
                }
            }
            _ = pool_log_ticker.tick() => {
                let total = pipeline.len();
                let is_empty = pipeline.is_empty();
                debug!(total_txs = total, is_empty = is_empty, "Mempool pipeline occupancy snapshot");
            }
            // BATCH FLUSH TIMER: flush of the batch accumulato anche se non pieno.
            _ = batch_flush_timer.tick() => {
                if !tx_batch_buffer.is_empty() {
                    let batch_size = tx_batch_buffer.len();
                    let verified_results: Vec<crate::VerifiedTx> = sig_verify_stage.process_batch(tx_batch_buffer.to_vec()).await;
                    let valid_count = verified_results.iter().filter(|v| v.is_valid).count();
                    let invalid_count = verified_results.iter().filter(|v| !v.is_valid).count();
                    let raw_txs: Vec<_> = verified_results
                        .into_iter()
                        .filter(|v| v.is_valid)
                        .map(|v| bytes_to_raw_tx(v.tx_bytes, None))
                        .collect();
                    let accepted_count = pipeline.process_raw_transactions(raw_txs).await;
                    let pipeline_total = pipeline.len_async().await;
                    info!(
                        batch_size = batch_size,
                        valid = valid_count,
                        invalid = invalid_count,
                        accepted = accepted_count,
                        pipeline_total = pipeline_total,
                        "📦 [BlockProducer] Timer-flush: batch SigVerify -> pipeline"
                    );
                    tx_batch_buffer.clear();
                    last_batch_process = Instant::now();
                }
            }
            _ = ticker.tick() => {
                let snapshot = pou_state.read().await.snapshot().await;
                let mut election_ready = snapshot.election_ready;
                let mut local_is_leader = snapshot.local_is_leader;
                let mut leader = snapshot.leader.clone();
                let mut leader_score = snapshot.leader_score;
                let mut using_default_leader = false;
                if USE_DEFAULT_POU_ONLY {
                    election_ready = false;
                } else if snapshot.election_ready {
                    if let Some(epoch) = snapshot.epoch {
                        if last_epoch_score_log != Some(epoch)
                            && snapshot.local_score.is_some()
                            && snapshot.best_remote_score.is_some()
                        {
                            let local_score_fmt = snapshot
                                .local_score
                                .map(crate::p2p::pou::format_pou_score_4dp)
                                .unwrap();
                            let remote_score_fmt = snapshot
                                .best_remote_score
                                .map(crate::p2p::pou::format_pou_score_4dp)
                                .unwrap();
                            let leader_display = snapshot
                                .leader
                                .as_ref()
                                .map(|p: &PeerId| p.to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            info!(
                                epoch,
                                leader = %leader_display,
                                local_is_leader = snapshot.local_is_leader,
                                "{}",
                                flagged_message(
                                    FLAG_POU,
                                    format!(
                                        "FINAL VALUE POU SCORE={local_score_fmt} | POU SCORE REMOTE={remote_score_fmt}"
                                    )
                                )
                            );
                            last_epoch_score_log = Some(epoch);
                        }
                    }
                    default_leader_logged = false;
                }
                if !election_ready {
                    if let Some((fallback_leader, fallback_score, fallback_is_local)) =
                        resolve_default_pou_leader(&local_peer)
                    {
                        election_ready = true;
                        local_is_leader = fallback_is_local;
                        leader_score = Some(fallback_score);
                        leader = Some(fallback_leader);
                        using_default_leader = true;
                    }
                }
                if !election_ready {
                    if last_election_notice != Some(ElectionKey::Unknown) {
                        debug!("PoU election not ready; waiting for remote scores before producing blocks");
                        last_election_notice = Some(ElectionKey::Unknown);
                    }
                    continue;
                }

                if !local_is_leader {
                    let marker = match snapshot.epoch {
                        Some(epoch) => ElectionKey::Epoch(epoch),
                        None => ElectionKey::Unknown,
                    };
                    if last_election_notice != Some(marker) {
                        match (snapshot.epoch, leader.as_ref()) {
                            (Some(epoch), Some(leader)) => {
                                if using_default_leader {
                                    debug!(
                                        leader = %leader,
                                        score = leader_score.unwrap_or(0),
                                        "Default PoU leadership held by remote peer; skipping block production until live scores arrive"
                                    );
                                } else {
                                    debug!(
                                        epoch,
                                        leader = %leader,
                                        score = snapshot.leader_score.unwrap_or(0),
                                        "PoU leadership held by remote peer; skipping block production for this epoch"
                                    );
                                }
                            }
                            _ => {
                                debug!("Waiting for local PoU election before producing blocks");
                            }
                        }
                        last_election_notice = Some(marker);
                    }
                    continue;
                } else {
                    last_election_notice = None;
                }

                if using_default_leader && !default_leader_logged {
                    match leader.as_ref() {
                        Some(leader_peer) if *leader_peer == local_peer => {
                            debug!(
                                leader = %leader_peer,
                                score = leader_score.unwrap_or(0),
                                "Assuming provisional PoU leadership from default scores until remote votes arrive"
                            );
                            default_leader_logged = true;
                        }
                        _ => {}
                    }
                }
            }

            // Second branch: timeout for block production.
            // BUGFIX: uses block_produce_ticker (interval created before loop) instead of
            // sleep() — sleep() inside select! resets on every iteration, so block production
            // would never fire while tx_rx had pending messages (biased; priority).
            _ = block_produce_ticker.tick() => {
                // When we are the intra-group elected proposer, only the intra-group path produces blocks
                if let Some(ref flag) = is_intragroup_proposer {
                    if *flag.read().await {
                        continue;
                    }
                }
                // When we are in an intra-group, only the elected proposer (intra_group path) should drain the mempool.
                // Otherwise the main block producer would drain and the proposer would see an empty mempool.
                if let Some(ref in_group) = is_in_intra_group {
                    if *in_group.read().await {
                        continue;
                    }
                }
                // Drain transactions from pipeline (returns mempool_txs and signed_txs)
                // NOTE: Transactions are NOT removed from mempool here - they will be removed
                // only after finality confirmation (see finalize_remote_block_commit in network.rs)
                let (ready_txs, ready_stxs) = pipeline.drain_for_block_production(max_block_txs);
            if ready_stxs.is_empty() {
                continue;
            }

            // The block height we are about to produce serves as the FL round
            // identifier so per-client gradient scores feed into PoU streak
            // tracking with a meaningful key.
            let next_height_for_round = match storage.get_chain_head() {
                Ok(Some(b)) => b.height + 1,
                _ => 1,
            };
            let (valid_txs, invalid_handles): (Vec<crate::tx::SignedTx>, Vec<crate::core::tx::TxHandle>) = pipeline.final_validation_with_round(
                &ready_txs,
                &ready_stxs,
                Some(storage.as_ref()),
                next_height_for_round,
            );

            if !invalid_handles.is_empty() {
                debug!(
                    invalid_count = invalid_handles.len(),
                    "Final validation removed {} invalid transactions from mempool", invalid_handles.len()
                );
            }

            if valid_txs.is_empty() {
                continue;
            }

            info!(
                txs = valid_txs.len(),
                invalid = invalid_handles.len(),
                "{}",
                flagged_message(
                    FLAG_BLOCK_ATTEMPT,
                    format!("Attempting block production with {} valid txs ({} invalid removed)", valid_txs.len(), invalid_handles.len())
                )
            );

                match produce_block(
                    storage.clone(),
                    &producer_kp,
                    valid_txs,
                    &mut resource_events,
                    local_peer,
                ).await {
                    Ok(Some((block_msg, _proposed_txs, pending_data))) => {
                        // IMPORTANT: Do NOT cleanup mempool here!
                        // The block has been executed but NOT committed to storage.
                        // Mempool cleanup will happen ONLY after receiving finality/certificate
                        // from the masternode (handled in network.rs via finalize_remote_block_commit).
                        //
                        // Flow:
                        // 1. execute_block_without_commit() -> done above
                        // 2. Broadcast proposal to network -> done below
                        // 3. Wait for finality/certificate (handled by receipt manager in network.rs)
                        // 4. commit_pending_block() + mempool cleanup -> only after finality
                        //
                        // If the block is rejected or times out:
                        // - No DB writes occurred (execute_block_without_commit doesn't write)
                        // - Transactions remain in mempool for next block attempt
                        // - No state pollution
                        //
                        // See: docs/architettura/DECISIONE_ARCHITETTURALE_EXECUTION.md

                        debug!(
                            hash = %hex::encode(pending_data.block.hash),
                            height = pending_data.block.height,
                            txs = pending_data.signed_txs.len(),
                            "Block produced (not committed), broadcasting proposal and awaiting finality"
                        );

                        if let Err(err) = block_sender.send((block_msg.clone(), pending_data.clone())).await {
                            warn!(error=?err, "failed to forward block to network; stopping block producer");
                            if let Some(sender) = integrity_sender.as_ref() {
                                integrity::emit_event(sender, &local_peer, integrity::IntegrityKind::Fault);
                            }
                            break;
                        } else {
                            info!(hash = %hex::encode(pending_data.block.hash), height = pending_data.block.height, txs = pending_data.signed_txs.len(), "Forwarded block to network block_sender");
                            if let Some(sender) = integrity_sender.as_ref() {
                                integrity::emit_event(sender, &local_peer, integrity::IntegrityKind::Success);
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        warn!(error=?err, "block production failed");
                        if let Some(sender) = integrity_sender.as_ref() {
                            integrity::emit_event(sender, &local_peer, integrity::IntegrityKind::Fault);
                        }
                    }
                }
            }
        }
    }
}

async fn produce_block(
    storage: Arc<dyn BlockAndAccountStorageTrait>,
    producer_kp: &Keypair,
    signed_txs: Vec<SignedTx>,
    resource_events: &mut Option<mpsc::Sender<resource::ResourceEvent>>,
    local_peer: PeerId,
) -> Result<
    Option<(
        p2p::BlockBroadcast,
        Vec<SignedTx>,
        crate::p2p::types::PendingBlockData,
    )>,
> {
    if signed_txs.is_empty() {
        return Ok(None);
    }

    let mut deduped: Vec<SignedTx> = Vec::new();
    let mut seen_hashes: HashSet<[u8; 32]> = HashSet::new();
    for tx in signed_txs.into_iter() {
        let bytes = crate::tx::serialize_signed_tx(&tx).unwrap_or_default();
        let hash = hash_signed_tx_bytes(&bytes);
        if seen_hashes.insert(hash) {
            deduped.push(tx);
        }
    }
    if deduped.is_empty() {
        return Ok(None);
    }
    // No nonce filtering - use all transactions as-is
    let filtered = deduped;
    if filtered.is_empty() {
        return Ok(None);
    }

    let proposer = producer_kp.verifying_key().to_bytes();
    let producer_account =
        match crate::storage::BlockAndAccountStorage::get_account(storage.as_ref(), &proposer)? {
            Some(storage_account) => savitri_core::core::types::Account {
                balance: storage_account.balance,
                nonce: storage_account.nonce,
            },
            None => savitri_core::core::types::Account::default(),
        };
    debug!(
        account = %hex::encode(&proposer),
        balance = producer_account.balance,
        "Producer balance before block execution"
    );
    let mut block = Block::new(Vec::new(), proposer);
    // Get current chain head to determine parent hash
    let head = match storage.get_chain_head() {
        Ok(Some(block)) => Some((block.height, block.hash)),
        Ok(None) => None,
        Err(e) => {
            warn!(error = %e, "Failed to get chain head, assuming no blocks");
            None
        }
    };

    // Determine parent hash: use chain head if available, otherwise use genesis
    let parent_hash = if let Some((_, hash)) = head {
        hash
    } else {
        // No chain head, but we should have genesis block
        // Get genesis block hash as parent for the first block after genesis
        match storage.get_block(0) {
            Ok(Some(genesis_block)) => genesis_block.hash,
            Ok(None) => {
                anyhow::bail!("Cannot create block: no chain head and no genesis block found. Ensure genesis block is initialized.");
            }
            Err(e) => {
                anyhow::bail!("Failed to get genesis block hash: {}", e);
            }
        }
    };

    block.height = head.map_or(1, |(h, _)| h + 1);
    // ("Regular block must have non-zero parent hash"). Without this assignment
    // the block was sent with a zero parent_hash despite parent_exec_hash being
    // set correctly, leading to cascading rejections across the network.
    block.parent_hash = parent_hash;
    block.parent_exec_hash = parent_hash;
    block.parent_ref_hash = [0u8; 64];
    block.timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Execute block WITHOUT committing to storage.
    // The block will be committed only after receiving finality/certificate from masternode.
    // This prevents state pollution if the block is rejected or times out.
    //
    // Flow:
    // 1. execute_block_without_commit() -> compute overlay, receipts, roots
    // 2. Broadcast proposal to network
    // 3. Wait for finality/certificate (handled by receipt manager)
    // 4. commit_pending_block() only after finality confirmed
    //
    // See: docs/architettura/DECISIONE_ARCHITETTURALE_EXECUTION.md
    let exec_start = Instant::now();
    let (block_executed, _overlay, _receipts) =
        crate::p2p::block::execute_block_without_commit(storage.as_ref(), block.clone(), &filtered)
            .context("block execution failed in produce_block")?;

    debug!(
        tx_count = filtered.len(),
        height = block_executed.height,
        state_root = %hex::encode(block_executed.state_root),
        tx_root = %hex::encode(block_executed.tx_root),
        "Block executed WITHOUT commit (pending finality)"
    );

    let exec_duration = exec_start.elapsed();
    info!(
        tx_count = filtered.len(),
        height = block_executed.height,
        duration_ms = exec_duration.as_millis(),
        execution_mode = "sequential", // execute_block_without_commit uses sequential execution
        "Block execution completed (pending finality, not committed yet)"
    );
    resource::emit_event(
        resource_events,
        resource::ResourceEvent::BlockExecution {
            duration: exec_duration,
        },
    );

    // Block is ready with computed roots but NOT committed to DB yet
    // Commit will happen in finalize_remote_block_commit() after finality

    // Sign the block - for lightnode we use block hash directly as header hash
    // In a full implementation this would involve proper cryptographic signing
    let header_hash = block_executed.hash;
    // Verify the hash matches (block_executed.hash was computed by execute_block_without_commit)
    if block_executed.hash != header_hash {
        anyhow::bail!(
            "block hash mismatch after execution: expected {} got {}",
            hex::encode(block_executed.hash),
            hex::encode(header_hash)
        );
    }

    let sig = sign(&header_hash, &producer_kp);
    let mut block_signed = block_executed;
    block_signed.signature.copy_from_slice(&sig.to_bytes());
    let block_hash_hex = hex::encode(block_signed.hash);

    let block_message = crate::p2p::types::BlockMessage {
        hash: block_signed.hash,
        header: crate::p2p::types::BlockHeader {
            exec_height: block_signed.height,
            proposer: [0u8; 32],
            timestamp: block_signed.timestamp,
            parent_hash: block_signed.parent_hash,
        },
        txs: filtered
            .iter()
            .filter_map(|tx| crate::tx::serialize_signed_tx(tx).ok())
            .collect(),
    };

    let have_message = crate::p2p::types::HaveBlock {
        hash: block_signed.hash,
        height: block_signed.height,
        exec_height: block_signed.height,
        tx_count: filtered.len() as u32,
    };

    let tx_details: Vec<String> = filtered
        .iter()
        .enumerate()
        .map(|(idx, tx)| {
            format!(
                "#{idx}: from={} to={} amount={} sig={}",
                hex::encode(&tx.from),
                hex::encode(&tx.to),
                tx.amount,
                hex::encode(tx.sig)
            )
        })
        .collect();

    debug!(
        height = block_signed.height,
        hash = %block_hash_hex,
        txs = filtered.len(),
        details = ?tx_details,
        "Block payload ready for gossip"
    );
    debug!(
        height = block_signed.height,
        hash = %block_hash_hex,
        txs = filtered.len(),
        exec_duration_ms = exec_duration.as_millis(),
        "Block executed (not committed), ready for broadcast and finality"
    );

    // Create PendingBlockData for committing after quorum receipts
    let pending_data = crate::p2p::types::PendingBlockData {
        block: block_signed.clone(),
        signed_txs: filtered.clone(),
        source_peer: local_peer, // Will be set when registering
    };

    Ok(Some((
        p2p::BlockBroadcast {
            have: have_message,
            block: block_message,
        },
        filtered,
        pending_data,
    )))
}

fn resolve_default_pou_leader(
    local_peer: &PeerId,
) -> Option<(PeerId, crate::p2p::pou::PouScore, bool)> {
    let mut best: Option<(PeerId, u16)> = None;
    for (peer_str, score, _) in DEFAULT_POU_SCORES {
        if let Ok(peer_id) = PeerId::from_str(peer_str) {
            match best {
                Some((_, best_score)) if *score <= best_score => continue,
                _ => best = Some((peer_id, *score)),
            }
        }
    }
    best.map(|(peer, score)| {
        let is_local = peer == *local_peer;
        (peer, score, is_local)
    })
}
