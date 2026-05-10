#![allow(dead_code, unused_variables, unused_imports)]

//! Savitri Masternode - Full Implementation
//!
//! This is the masternode implementation for Savitri Network.

// Import shared modules from the library crate (these are defined in lib.rs)
use savitri_masternode::{
    bootstrap, config, group_consensus, group_formation, group_formation::MonolithP2PDistributor,
    libp2p_network, masternode_p2p::MasternodeMessage as LibMasternodeMessage, proposal_validator,
};

// Binary-only modules (not in lib.rs)
mod monolith_benchmark;
mod p2p;
mod p2p_block_receiver;
mod telemetry;
mod vote_aggregator;
// bridge, monolith_p2p, monolith_producer, monolith_storage are in the library crate (lib.rs)

use std::collections::HashMap;
use std::path::{Path, PathBuf};

// Function to save peer ID to file for lightnode discovery
fn save_peer_id_to_file(peer_id: libp2p::PeerId, port: u16) -> Result<()> {
    use std::fs;

    // Create data directory if it doesn't exist
    let data_dir = PathBuf::from("data");
    fs::create_dir_all(&data_dir)?;

    // Save peer ID to file
    let peer_file = data_dir.join(format!("peer_id_{}.txt", port));
    fs::write(&peer_file, peer_id.to_string())?;

    Ok(())
}

#[cfg(feature = "zkp-all")]
mod zkp_integration;
#[cfg(feature = "zkp-all")]
#[cfg(test)]
mod zkp_tests;
#[cfg(test)]
mod monolith_test {
    use savitri_masternode::bridge::core::slot_scheduler::{SlotScheduler, SlotSchedulerConfig};

    #[test]
    fn test_monolith_proposer_selection() {
        // Create test configuration
        let config = SlotSchedulerConfig {
            heartbeat_interval_ms: 5000,
            slots_per_epoch: 20,
            monolith_epoch_ms: 86400000,
            genesis_timestamp_ms: 0,
            validators: vec![
                "node1".to_string(),
                "node2".to_string(),
                "node3".to_string(),
                "node4".to_string(),
                "node5".to_string(),
            ],
            local_id: "node1".to_string(),
        };

        let scheduler = SlotScheduler::new(config);

        // Test day number calculation
        let day_number = scheduler.current_day_number();
        assert!(day_number > 0);

        let validators = scheduler.get_current_validators();
        let hash1 = scheduler.hash_validator_set(&validators);
        let hash2 = scheduler.hash_validator_set(&validators);
        assert_eq!(hash1, hash2); // Should be deterministic

        // Test combined hashing
        let combined1 = scheduler.hash_combined(hash1, day_number);
        let combined2 = scheduler.hash_combined(hash1, day_number);
        assert_eq!(combined1, combined2); // Should be deterministic

        // Test proposer selection
        let proposer = scheduler.compute_monolith_proposer_hybrid();
        assert!(proposer.is_some());
        let proposer_str = proposer.as_ref().unwrap();
        assert!(validators.contains(proposer_str));

        println!("✅ Monolith proposer selection test passed!");
        println!("📅 Day number: {}", day_number);
        println!("🔢 Validator hash: {}", hash1);
        println!("🎯 Combined hash: {}", combined1);
        println!("👤 Selected proposer: {:?}", proposer);
    }

    #[test]
    fn test_monolith_time_detection() {
        let config = SlotSchedulerConfig {
            heartbeat_interval_ms: 5000,
            slots_per_epoch: 20,
            monolith_epoch_ms: 86400000,
            genesis_timestamp_ms: 0,
            validators: vec!["node1".to_string()],
            local_id: "node1".to_string(),
        };

        let scheduler = SlotScheduler::new(config);

        // Test monolith time detection
        let is_monolith_time = scheduler.is_monolith_time();
        println!("⏰ Is monolith time: {}", is_monolith_time);

        // Test should_create_monolith
        let should_create = scheduler.should_create_monolith();
        println!("🚀 Should create monolith: {}", should_create);
    }
}

use anyhow::{anyhow, Context, Result};
use config::MasternodeConfig;
use ed25519_dalek;
use group_formation::{GroupFormationConfig, GroupFormationManager};
use hex;
use libp2p::{identity::Keypair, PeerId};
use proposal_validator::ProposalValidator;
use rand;
use std::fs;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

// Import bridge module for slot scheduler
use crate::vote_aggregator::VoteAggregator;
use savitri_masternode::bootstrap::BootstrapHandler;
use savitri_masternode::bridge::core::slot_scheduler;
use savitri_masternode::group_consensus::{BftGroupConfig, GroupConsensusManager};
use savitri_masternode::masternode_p2p::MasternodeMessage;
use savitri_masternode::monolith_p2p::{MonolithP2PConfig, MonolithP2PManager, PeerInfo};
use savitri_masternode::monolith_producer::{MonolithProducer, MonolithProducerConfig};
use savitri_masternode::monolith_storage::{MonolithStorage, MonolithStorageConfig};
use savitri_storage::storage::{CF_BLOCKS, CF_METADATA};

// Re-export consensus types from the unified library
// pub use savitri_consensus::{GroupAwareConsensus, GroupAwareConfig, PouBasedConsensus, PouConfig};

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::init_logging()?;

    let startup_time = Instant::now();

    // Initialize metrics exporter
    if let Err(err) = telemetry::init_metrics() {
        warn!(error = %err, "metrics exporter not available; continuing without HTTP endpoint");
    } else {
        // Initialize system metrics collection
        let metrics_shutdown_tx = telemetry::update_system_metrics_periodically(startup_time).await;
    }

    if let Err(err) = run().await {
        error!(error = ?err, "masternode startup failed");
        return Err(err);
    }

    Ok(())
}

const DEFAULT_CONFIG_PATH: &str = "masternode/masternode.toml";

fn print_help() {
    println!("Savitri Masternode v{}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("USAGE:");
    println!("    savitri-masternode [CONFIG_PATH]");
    println!("    savitri-masternode --help");
    println!();
    println!("ARGUMENTS:");
    println!("    <CONFIG_PATH>    Path to configuration file (TOML format)");
    println!();
    println!("OPTIONS:");
    println!("    -h, --help       Print this help information");
    println!();
    println!("EXAMPLES:");
    println!("    savitri-masternode config/masternode.toml");
    println!("    savitri-masternode masternode.toml");
    println!();
    println!("CONFIGURATION:");
    println!("    Copy masternode.example.toml to masternode.toml");
    println!("    and customize the fields for your environment.");
    println!();
    println!("FOR MORE INFORMATION:");
    println!("    https://github.com/savitri-network/savitri-masternode");
}

fn read_chain_head_height(storage: &savitri_storage::Storage) -> Result<Option<u64>> {
    let raw = storage.get_cf(CF_METADATA, b"chain_head")?;
    match raw {
        Some(bytes) => {
            if bytes.len() < 72 {
                return Err(anyhow!(
                    "chain_head payload too short: expected at least 72 bytes, got {}",
                    bytes.len()
                ));
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes[64..72]);
            Ok(Some(u64::from_le_bytes(arr)))
        }
        None => Ok(None),
    }
}

fn read_consensus_latest_height(consensus_db_path: &Path) -> Result<Option<u64>> {
    // Avoid creating a fresh RocksDB just to probe startup state.
    if !consensus_db_path.join("CURRENT").exists() {
        return Ok(None);
    }
    let config = savitri_storage::StorageConfig {
        path: consensus_db_path.to_string_lossy().to_string(),
        ..Default::default()
    };
    let storage = savitri_storage::Storage::with_config(config)?;
    let raw = storage.get_cf(CF_METADATA, b"latest_height")?;
    match raw {
        Some(bytes) if bytes.len() >= 8 => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes[..8]);
            Ok(Some(u64::from_le_bytes(arr)))
        }
        Some(_) => Ok(None),
        None => Ok(None),
    }
}

fn persist_chain_head(
    storage: &savitri_storage::Storage,
    height: u64,
    block_hash: &[u8; 64],
) -> Result<()> {
    // Keep the existing chain_head encoding used by startup probe:
    // [0..64) = block_hash, [64..72) = little-endian height.
    let mut chain_head = Vec::with_capacity(72);
    chain_head.extend_from_slice(block_hash);
    chain_head.extend_from_slice(&height.to_le_bytes());
    storage.put_cf(CF_METADATA, b"chain_head", &chain_head)?;
    Ok(())
}

fn persist_certified_block(
    storage: &savitri_storage::Storage,
    proposal: &proposal_validator::LightnodeProposal,
) -> Result<Vec<u8>> {
    let block_bytes = serde_json::to_vec(proposal)?;
    storage.put_cf(CF_BLOCKS, &proposal.block_hash, &block_bytes)?;
    Ok(block_bytes)
}

async fn run() -> Result<()> {
    let startup_time = Instant::now();

    let mut args: Vec<String> = std::env::args().collect();
    args.remove(0); // Remove program name

    // Parse arguments
    let config_path = if args.is_empty() {
        DEFAULT_CONFIG_PATH.to_string()
    } else if args[0] == "--help" || args[0] == "-h" {
        print_help();
        return Ok(());
    } else if args[0] == "--config" || args[0] == "-c" {
        // Handle --config <path> format
        if args.len() < 2 {
            anyhow::bail!("--config requires a path argument");
        }
        args[1].clone()
    } else {
        // Direct path format: savitri-masternode config.toml
        args[0].clone()
    };

    let cfg = config::load_config(&config_path)?;

    // Resolve storage paths relative to data_dir or config file (robust for services, Docker, etc.)
    let config_path_ref = Path::new(&config_path);
    let resolved_storage_path =
        config::resolve_storage_path(config_path_ref, cfg.data_dir.as_deref(), &cfg.storage_path);
    let resolved_monolith_path = cfg.monolith_storage_path.as_ref().map(|p| {
        config::resolve_storage_path(config_path_ref, cfg.data_dir.as_deref(), Path::new(p))
    });

    let cwd = std::env::current_dir().ok();
    info!(
        config_path = %config_path,
        cwd = ?cwd,
        bootstrap_peers = cfg.bootstrap_peers.len(),
        "Loaded masternode config"
    );
    if !cfg.bootstrap_peers.is_empty() {
        info!(
            bootstrap_peers = ?cfg.bootstrap_peers,
            "Bootstrap peers configured"
        );
    }
    if cfg.bootstrap_peers.is_empty() {
        warn!(
            config_path = %config_path,
            "Config has no bootstrap peers; masternode will not dial any peers"
        );
    }
    let p2p_port = cfg.p2p_port;
    let identity = load_identity(&cfg)?;
    let node_id = PeerId::from(identity.public());
    let node_id_str = node_id.to_string();

    if cfg.validator_stake < 1_000_000 {
        warn!(
            validator_stake = cfg.validator_stake,
            "Validator stake below minimum requirement (1M SAV coins)"
        );
    }

    info!(
        node_id = %node_id,
        p2p_port = p2p_port,
        storage_path = %resolved_storage_path.display(),
        validators = cfg.validators.len(),
        validator_stake = cfg.validator_stake,
        "Masternode starting",
    );

    // Persistent masternode storage handle used for finalized height metadata updates.
    let chain_storage = Arc::new(savitri_storage::Storage::with_config(
        savitri_storage::StorageConfig {
            path: resolved_storage_path.to_string_lossy().to_string(),
            ..Default::default()
        },
    )?);
    #[cfg(feature = "storage")]
    let consensus_db_path = resolved_storage_path.join("consensus");

    match read_chain_head_height(chain_storage.as_ref()) {
        Ok(Some(height)) => {
            info!(height = height, db = %resolved_storage_path.display(), "Loaded chain head height from DB")
        }
        Ok(None) => {
            info!(db = %resolved_storage_path.display(), "No chain head found in DB (fresh or not yet initialized)")
        }
        Err(e) => {
            warn!(error = %e, db = %resolved_storage_path.display(), "Failed to read chain head height from DB")
        }
    }

    // Initialize group-aware consensus engine using savitri-consensus library
    let consensus_config = savitri_consensus::GroupAwareConfig::default();
    #[cfg(feature = "storage")]
    let consensus_storage_adapter = {
        std::fs::create_dir_all(&consensus_db_path)?;
        match read_consensus_latest_height(&consensus_db_path) {
            Ok(Some(height)) => {
                info!(height = height, db = %consensus_db_path.display(), "Loaded consensus latest height from DB")
            }
            Ok(None) => {
                info!(db = %consensus_db_path.display(), "No consensus latest height found in DB")
            }
            Err(e) => {
                warn!(error = %e, db = %consensus_db_path.display(), "Failed to read consensus latest height from DB")
            }
        }
        Arc::new(
            savitri_masternode::consensus_storage_adapter::ConsensusStorageAdapter::with_path(
                &consensus_db_path,
            )?,
        )
    };
    #[cfg(feature = "storage")]
    let consensus_storage: Arc<dyn savitri_consensus::traits::storage::Storage> =
        consensus_storage_adapter.clone();
    #[cfg(not(feature = "storage"))]
    let consensus_storage: Arc<dyn savitri_consensus::traits::storage::Storage> = Arc::new(
        savitri_consensus::MemoryStorage::new(savitri_consensus::StorageConfig::default()),
    );
    let consensus_engine = Arc::new(savitri_consensus::GroupAwareConsensus::new(
        consensus_config,
        consensus_storage,
    )?);
    info!("Group-aware consensus engine initialized with savitri-consensus library");

    // Resolve genesis_timestamp_ms with priority: ENV > DB > config (>0).
    // current_slot/epoch è genesis-relative; auto-set a now() per nodo causa epoch desync.
    let genesis_ts = {
        let env_genesis: Option<u64> = std::env::var("SAVITRI_GENESIS_TIMESTAMP_MS")
            .ok()
            .and_then(|s| s.parse().ok());
        let db_genesis: Option<u64> = chain_storage
            .get_cf(CF_METADATA, b"genesis_ts")
            .ok()
            .flatten()
            .and_then(|raw| {
                if raw.len() == 8 {
                    Some(u64::from_le_bytes(raw.as_slice().try_into().unwrap()))
                } else {
                    None
                }
            });
        let cfg_genesis: Option<u64> = if cfg.genesis_timestamp_ms > 0 {
            Some(cfg.genesis_timestamp_ms)
        } else {
            None
        };

        let resolved = match (env_genesis, db_genesis, cfg_genesis) {
            (Some(env), Some(db), _) if env != db => {
                error!(
                    env = env,
                    db = db,
                    "Genesis timestamp mismatch ENV vs DB. Wipe DB or unset env to recover."
                );
                anyhow::bail!("Genesis mismatch: ENV={} DB={}. Wipe required.", env, db);
            }
            (Some(env), _, _) => env,
            (None, Some(db), _) => db,
            (None, None, Some(c)) => c,
            (None, None, None) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                warn!(genesis_ts = now, "No SAVITRI_GENESIS_TIMESTAMP_MS env var, no DB genesis, no config: auto-setting to now(). NOTE: per evitare epoch desync (an earlier fix) impostare la stessa env var su tutti i nodi al wipe.");
                now
            }
        };

        // Persist on DB if not already there (idempotent for stable boots).
        if db_genesis.is_none() {
            if let Err(e) =
                chain_storage.put_cf(CF_METADATA, b"genesis_ts", &resolved.to_le_bytes())
            {
                warn!(error = %e, "Failed to persist genesis_ts to DB");
            } else {
                info!(genesis_ts = resolved, "Persisted genesis_ts to DB");
            }
        }
        info!(genesis_ts = resolved, env = ?env_genesis, db = ?db_genesis, cfg = ?cfg_genesis, "Resolved genesis_timestamp_ms");
        resolved
    };
    // operators can tune epoch duration without re-rolling the binary. Same
    // pattern as SAVITRI_GENESIS_TIMESTAMP_MS above. Falls back to the TOML
    // value (default 200 since P2 commit).
    let env_slots_per_epoch: Option<u64> = std::env::var("SAVITRI_SLOTS_PER_EPOCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &u64| *v > 0);
    let resolved_slots_per_epoch = env_slots_per_epoch.unwrap_or(cfg.slots_per_epoch);
    if env_slots_per_epoch.is_some() {
        info!(
            env = env_slots_per_epoch,
            cfg = cfg.slots_per_epoch,
            resolved = resolved_slots_per_epoch,
            "Resolved slots_per_epoch (env override active)"
        );
    }
    let monolith_scheduler_config = slot_scheduler::SlotSchedulerConfig {
        heartbeat_interval_ms: cfg.heartbeat_interval_ms,
        slots_per_epoch: resolved_slots_per_epoch,
        monolith_epoch_ms: cfg.monolith_epoch_ms,
        genesis_timestamp_ms: genesis_ts,
        validators: cfg.validators.clone(),
        local_id: node_id_str.clone(),
    };
    let monolith_scheduler = Arc::new(slot_scheduler::SlotScheduler::new(
        monolith_scheduler_config,
    ));

    // === GENESIS BLOCK ===
    // Check if the chain is empty; if so, create and persist the genesis block at height=0
    {
        let chain_head = chain_storage
            .get_cf(CF_METADATA, b"chain_head")
            .ok()
            .flatten();
        if chain_head.is_none() {
            let genesis_height: u64 = 0;
            let genesis_timestamp = genesis_ts;
            let mut genesis_hash = [0u8; 64];
            {
                use sha2::Digest;
                let mut hasher = sha2::Sha512::new();
                hasher.update(b"SAVITRI_GENESIS_v1");
                hasher.update(&genesis_timestamp.to_le_bytes());
                let result = hasher.finalize();
                genesis_hash.copy_from_slice(&result);
            }
            if let Err(e) =
                persist_chain_head(chain_storage.as_ref(), genesis_height, &genesis_hash)
            {
                error!("Failed to persist genesis block: {}", e);
            } else {
                info!(
                    height = genesis_height,
                    genesis_ts = genesis_timestamp,
                    hash = %hex::encode(&genesis_hash[..16]),
                    "GENESIS BLOCK created and persisted - epoch/slot/round calculations anchored"
                );
            }
        }
    }

    // === GENESIS ACCOUNTS ===
    // Seed pre-funded accounts so that lightnode sender keys are recognised by all nodes.
    if !cfg.genesis_accounts.is_empty() {
        let mut seeded = 0usize;
        for entry in &cfg.genesis_accounts {
            let body = entry.trim().strip_prefix("0x").unwrap_or(entry.trim());
            if body.is_empty() {
                continue;
            }
            let bytes = match hex::decode(body) {
                Ok(b) if b.len() == 32 => b,
                _ => {
                    warn!(entry = %entry, "Skipping invalid genesis account (must be 64 hex chars)");
                    continue;
                }
            };
            // Encode Account { balance, nonce: 0 } as 24 bytes (16 LE balance + 8 LE nonce).
            let balance: u128 =
                (cfg.genesis_account_balance_savt as u128) * 1_000_000_000_000_000_000;
            let mut encoded = [0u8; 24];
            encoded[0..16].copy_from_slice(&balance.to_le_bytes());
            // nonce bytes are already 0
            if let Err(e) = chain_storage.put_account(&bytes, &encoded) {
                warn!(account = %entry, error = %e, "Failed to seed genesis account");
            } else {
                seeded += 1;
            }
        }
        info!(count = seeded, "Seeded genesis accounts from config");
    }

    // Initialize monolith producer (zkp_backend from config; production uses "arkworks" only)
    let mut monolith_producer_config = MonolithProducerConfig::default();
    if let Some(ref zkp) = cfg.zkp_backend {
        monolith_producer_config.zkp_backend = zkp.clone();
    }
    // Initialize monolith producer with storage
    let mut monolith_storage_config = MonolithStorageConfig::default();
    // Use resolved monolith storage path (robust regardless of CWD)
    if let Some(ref path) = resolved_monolith_path {
        monolith_storage_config.db_path = path.clone();
        info!(
            "Using monolith storage path: {:?}",
            monolith_storage_config.db_path
        );
    } else {
        info!(
            "Using default monolith storage path: {:?}",
            monolith_storage_config.db_path
        );
    }
    info!(
        "Final monolith storage path: {:?}",
        monolith_storage_config.db_path
    );
    let monolith_producer = Arc::new(MonolithProducer::with_storage(
        monolith_producer_config,
        monolith_scheduler.clone(),
        monolith_storage_config,
    )?);
    info!("Monolith producer with storage initialized");

    // Initialize masternode P2P communication (single gossipsub in Libp2pNetwork)
    let (masternode_tx, mut masternode_rx) = mpsc::unbounded_channel();
    let (masternode_publish_tx, masternode_publish_rx) =
        mpsc::unbounded_channel::<LibMasternodeMessage>();

    // UNIFIED PEERS: Single shared peer map used by both Libp2pNetwork and MonolithP2PManager.
    // Libp2pNetwork writes on ConnectionEstablished/ConnectionClosed,
    // MonolithP2PManager reads for distribute_groups and get_stats.
    let shared_peers: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<libp2p::PeerId, PeerInfo>>,
    > = std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

    // Initialize monolith P2P manager with shared_peers so get_stats sees Libp2pNetwork connections
    let (monolith_tx, mut monolith_rx) = mpsc::unbounded_channel();
    let monolith_p2p_config = MonolithP2PConfig::default();
    let mut monolith_p2p_manager = MonolithP2PManager::with_shared_peers(
        node_id,
        monolith_p2p_config,
        monolith_tx,
        shared_peers.clone(),
    );
    // Wire gossipsub sender so group announcements reach lightnodes
    // Note: monolith_p2p expects its local MasternodeMessage type, but we need to convert
    // For now, we'll create a separate channel for monolith announcements
    info!("🔔 RACCOMANDAZIONE #3: Creating monolith_announce channel for group announcements");
    let (monolith_announce_tx, monolith_announce_rx) =
        mpsc::unbounded_channel::<savitri_masternode::monolith_p2p::MasternodeMessage>();
    monolith_p2p_manager.set_lightnode_announce_sender(monolith_announce_tx);
    // Extract signing key early so group announcements are signed
    {
        let sk = match identity.clone().try_into_ed25519() {
            Ok(ed25519_keypair) => {
                let secret_bytes: [u8; 32] = ed25519_keypair
                    .secret()
                    .as_ref()
                    .try_into()
                    .expect("Ed25519 secret key should be 32 bytes");
                ed25519_dalek::SigningKey::from_bytes(&secret_bytes)
            }
            Err(_) => ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng),
        };
        monolith_p2p_manager.set_signing_key(sk);
    }
    info!("✅ Monolith announce channel created and attached to P2P manager");
    let monolith_p2p_manager = Arc::new(monolith_p2p_manager);
    info!("Monolith P2P manager initialized");

    // (default) che parteziona gli LN per region e poi chiama
    // calculate_num_groups(region_nodes.len()). Con 4 VM × ~4 LN/VM e
    // 6 LN orphan su 16 (37%).
    //
    // Fix C: due env override
    //   * SAVITRI_GEOGRAPHIC_DISTRIBUTION=0 (default) → usa simple_groups
    //     LN registrato in qualche gruppo.
    //   * SAVITRI_MIN_GROUP_SIZE=4 (default) → BFT 2f+1=3 con f=1 OK,
    //     consente fino a num_groups=floor(total/4) (16/4=4 gruppi).
    let env_min_group: usize = std::env::var("SAVITRI_MIN_GROUP_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v >= 2)
        .unwrap_or(cfg.min_group_size);
    let env_geo: bool = std::env::var("SAVITRI_GEOGRAPHIC_DISTRIBUTION")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    info!(
        env_min_group_size = env_min_group,
        env_geographic_distribution = env_geo,
        cfg_min_group_size = cfg.min_group_size,
        cfg_max_group_size = cfg.max_group_size,
        "GROUP FORMATION: applying env-tunable group config (P3 fix C)"
    );
    let group_config = GroupFormationConfig {
        health_check_interval_secs: cfg.group_health_check_interval_secs,
        node_timeout_secs: cfg.group_node_timeout_secs,
        formation_interval_epochs: cfg.formation_interval_epochs,
        min_group_size: env_min_group,
        max_group_size: cfg.max_group_size,
        geographic_distribution: env_geo,
        ..GroupFormationConfig::default()
    };
    let mut bft_config = BftGroupConfig::default();
    bft_config.total_masternodes = cfg.total_masternodes;
    bft_config.min_masternodes = cfg.min_masternodes;
    // override above so the BFT consensus engine accepts groups sized to
    // the new floor.
    bft_config.min_group_size = env_min_group;
    bft_config.max_group_size = cfg.max_group_size;
    bft_config.max_faulty = cfg.total_masternodes.saturating_sub(1) / 3;
    bft_config.approval_threshold = 0.67;

    // Create local storage and scheduler for group formation
    #[cfg(feature = "storage")]
    let (storage, rewards_storage) = {
        let group_formation_path = resolved_storage_path.join("group_formation");
        std::fs::create_dir_all(&group_formation_path)?;
        let persistent =
            group_formation::PersistentGroupFormationStorage::with_path(&group_formation_path)?;
        let rewards_storage = persistent.as_savitri_storage();
        (
            Arc::new(persistent) as Arc<dyn group_formation::Storage>,
            rewards_storage,
        )
    };
    #[cfg(not(feature = "storage"))]
    let storage: Arc<dyn group_formation::Storage> = Arc::new(group_formation::MemoryStorage::new(
        savitri_consensus::StorageConfig::default(),
    ));
    // P2: same env override applies to the group_formation scheduler so the
    // throttle's epoch boundary aligns with the monolith one.
    let scheduler = Arc::new(group_formation::SlotScheduler::new(
        group_formation::SlotSchedulerConfig {
            heartbeat_interval_ms: cfg.heartbeat_interval_ms,
            slots_per_epoch: resolved_slots_per_epoch,
            genesis_timestamp_ms: genesis_ts,
            validators: cfg.validators.clone(),
            local_id: node_id_str.clone(),
        },
    )?);

    // Create group formation manager first
    let mut group_manager =
        GroupFormationManager::new(storage.clone(), scheduler.clone(), group_config);

    // Create group consensus manager
    let mut group_consensus_inner = GroupConsensusManager::new(
        node_id_str.clone(),
        Arc::new(group_manager.clone()),
        bft_config,
    );

    // Set P2P distributor before wrapping in Arc
    group_consensus_inner.set_p2p_distributor(monolith_p2p_manager.clone());

    // Now wrap in Arc
    let group_consensus = Arc::new(group_consensus_inner);

    // Update group formation manager with consensus
    group_manager.set_group_consensus(group_consensus.clone());
    group_manager.set_auto_initiate(true);

    info!("🔔 SOLUZIONE: Configuring masternode_publish_tx for immediate vote publishing");
    group_manager
        .set_masternode_publish_sender(masternode_publish_tx.clone())
        .await;

    // BFT IDENTITY FIX (Tendermint-style ValidatorSet):
    // not from dynamic libp2p peer connectivity. This is the canonical pattern
    // used by Tendermint, HotStuff, Diem/Aptos and Cosmos-SDK chains: the
    // governance), not connectivity-derived.
    //
    // Why this fix matters:
    // 1) get_connected_masternode_ids() returned libp2p PeerIds (multihash of
    //    Ed25519 pubkey, e.g. "12D3KooW..."), but proposals/votes/certificates
    //    use logical masternode IDs ("mn-1".."mn-5"). The two domains are
    //    incompatible: am_i_coordinator_for_current_epoch never matched, the
    //    deterministic-coordinator path was dead code, and the system always
    //    fell through to non-deterministic leader-election dance.
    // 2) Each MN saw a different list (depending on its peer connectivity at
    //    the moment of update) -> coordinator selection diverged across
    //    cluster -> mn-5 boot delay produced standalone Forming groups with
    //    leader=self that never reached BFT approval.
    // 3) The static set guarantees identical ordering on every node regardless
    //    for every MN in the cluster. This eliminates the BFT split that
    //    leaves late-joining MNs orphaned.
    {
        let mut validator_ids = cfg.validators.clone();
        validator_ids.sort();
        info!(
            validators = ?validator_ids,
            local = %node_id_str,
            "BFT: initializing ordered_masternode_ids from static validator set (Tendermint-style)"
        );
        group_consensus
            .set_ordered_masternode_ids(validator_ids)
            .await;
    }

    group_manager.start().await?;
    info!("Group formation manager with BFT consensus initialized");

    // Create a shared reference to group manager for main loop and rewards
    let group_manager = Arc::new(tokio::sync::RwLock::new(group_manager));

    // Set the group manager reference in consensus
    group_consensus.initialize_available_lightnodes().await?;

    // Spawn rewards task (testnet SAVI: 50 light, 100 masternode per cycle)
    #[cfg(feature = "storage")]
    {
        use savitri_masternode::rewards::NodeType;
        use savitri_masternode::rewards::{spawn_rewards_task, EligibleNode, RewardsConfig};
        use sha2::{Digest, Sha256};

        let storage = rewards_storage.clone();
        let gm = group_manager.clone();
        let gc = group_consensus.clone();
        let local_mn_id = node_id_str.clone();
        let masternode_reward_address: Option<[u8; 32]> = cfg
            .reward_address
            .as_ref()
            .and_then(|s| hex::decode(s.trim()).ok().and_then(|v| v.try_into().ok()));
        if let Some(ref hex_addr) = cfg.reward_address {
            info!(reward_address = %hex_addr, "Reward/payout address set; masternode earnings will be credited here");
        }
        let node_provider: Arc<dyn Fn() -> Vec<EligibleNode> + Send + Sync> = Arc::new(move || {
            let rt = match tokio::runtime::Handle::try_current() {
                Ok(h) => h,
                Err(_) => return Vec::new(),
            };
            rt.block_on(async {
                let mut eligible = Vec::new();
                {
                    let gm_guard = gm.read().await;
                    for ln in gm_guard.get_registered_nodes().await {
                        eligible.push(EligibleNode {
                            address: ln.account,
                            node_type: NodeType::LightNode,
                            uptime_percent: ln.uptime_percentage,
                            pou_score: None,
                        });
                    }
                }
                for mn_id in gc.get_available_masternode_ids().await {
                    let address = if mn_id == local_mn_id {
                        masternode_reward_address.unwrap_or_else(|| {
                            let hash = Sha256::digest(mn_id.as_bytes());
                            let mut addr = [0u8; 32];
                            addr.copy_from_slice(&hash[..32]);
                            addr
                        })
                    } else {
                        let hash = Sha256::digest(mn_id.as_bytes());
                        let mut addr = [0u8; 32];
                        addr.copy_from_slice(&hash[..32]);
                        addr
                    };
                    eligible.push(EligibleNode {
                        address,
                        node_type: NodeType::Masternode,
                        uptime_percent: 100.0,
                        pou_score: None,
                    });
                }
                eligible
            })
        });
        let rewards_config = RewardsConfig {
            reward_period_secs: cfg.reward_period_secs,
            ..RewardsConfig::default()
        };
        let rewards_period_secs = rewards_config.reward_period_secs;
        let _rewards_handle = spawn_rewards_task(storage, rewards_config, node_provider);
        info!(reward_period_secs = rewards_period_secs, "Rewards job spawned");
    }

    // Extract Ed25519 signing key from libp2p identity
    let signing_key = {
        // Try to get Ed25519 keypair from libp2p identity
        match identity.clone().try_into_ed25519() {
            Ok(ed25519_keypair) => {
                // Get the secret key bytes from the Ed25519 keypair
                let secret_bytes: [u8; 32] = ed25519_keypair
                    .secret()
                    .as_ref()
                    .try_into()
                    .expect("Ed25519 secret key should be 32 bytes");
                ed25519_dalek::SigningKey::from_bytes(&secret_bytes)
            }
            Err(_) => {
                // Fallback: generate a new key if the identity is not Ed25519
                warn!("libp2p identity is not Ed25519, generating new signing key");
                ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng)
            }
        }
    };

    let validator = Arc::new(ProposalValidator::from_signing_key(signing_key, 3));
    info!("Proposal validator initialized with real Ed25519 cryptography");

    // Initialize vote aggregator
    let vote_aggregator = Arc::new(VoteAggregator::new(cfg.validators.len()));
    info!(
        "Vote aggregator initialized with {} validators",
        cfg.validators.len()
    );

    // into the vote_aggregator so quorum threshold adapts to actual group size.
    // Without this, groups with fewer MN than the global (2N+2)/3 threshold
    // stall forever because no bucket can accumulate enough approve votes.
    {
        let vote_agg = vote_aggregator.clone();
        let gm_ref = group_manager.clone();
        let cluster_mn_count = cfg.validators.len();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_logged_count: Option<usize> = None;
            loop {
                tick.tick().await;
                let groups = {
                    let gm = gm_ref.read().await;
                    gm.get_active_groups().await
                };
                if groups.is_empty() {
                    // Log once per "run of empties" so we notice when the MN is
                    // stuck without any active group and thus always falling
                    // back to the global quorum threshold.
                    if last_logged_count != Some(0) {
                        tracing::warn!(
                            "group→MN-count sync: active_groups is empty; votes will use fallback quorum"
                        );
                        last_logged_count = Some(0);
                    }
                    continue;
                }
                // not just `leader+backup`. The previous formula gave n=1 for
                // every group (each MN auto-assigns itself leader and backup
                // is None) → quorum_for_voters(1) = 1 → cert valid with a
                // single vote = NO BFT consensus, security broken.
                //
                // Correct model: the leader of a group proposes the block; the
                // entire MN committee (cluster-wide, e.g. 6 MN) participates in
                // BFT voting on that proposal. quorum_for_voters(6) = 4-of-6
                // (canonical 2N/3 + 1 BFT). A cert with voters=1 is then
                // correctly rejected as below quorum and the proposer loops
                // until the real BFT majority is collected.
                let sizes: std::collections::HashMap<String, usize> = groups
                    .iter()
                    .map(|g| (g.group_id.clone(), cluster_mn_count.max(1)))
                    .collect();
                if last_logged_count != Some(sizes.len()) {
                    // First time we see this group count → emit a one-shot summary.
                    let sample: Vec<String> = sizes
                        .iter()
                        .take(5)
                        .map(|(g, n)| format!("{}={}", g, n))
                        .collect();
                    tracing::info!(
                        count = sizes.len(),
                        sample = %sample.join(","),
                        "group→MN-count sync: refreshed per-group quorum table"
                    );
                    last_logged_count = Some(sizes.len());
                }
                vote_agg.set_group_mn_sizes(sizes).await;
            }
        });
        info!("Group→MN-count sync task started (5s interval)");
    }

    // Initialize P2P block receiver channels
    let (proposal_tx, mut proposal_rx) =
        mpsc::unbounded_channel::<proposal_validator::ProposalWithRole>();
    let (vote_tx, mut vote_rx) = mpsc::unbounded_channel::<proposal_validator::MasternodeVote>();
    // Channel for broadcasting our own votes to other masternodes
    let (vote_broadcast_tx, vote_broadcast_rx) =
        mpsc::unbounded_channel::<proposal_validator::MasternodeVote>();
    // Channel for broadcasting certificates to lightnodes
    let (certificate_broadcast_tx, certificate_broadcast_rx) =
        mpsc::unbounded_channel::<proposal_validator::BlockCertificate>();
    // Channel for broadcasting BlockAcceptanceCertificate (owner MN -> other MNs)
    let (block_acceptance_publish_tx, block_acceptance_publish_rx) =
        mpsc::unbounded_channel::<proposal_validator::BlockAcceptanceCertificate>();

    // Track peer count to avoid spam logging
    let last_peer_count = 0usize;

    info!("P2P block receiver channels initialized");

    // Initialize REAL libp2p network with gossipsub (MUST match lightnode protocol)
    info!(
        "Initializing libp2p network on port {} with Noise/Yamux/Gossipsub",
        p2p_port
    );

    let mut libp2p_network: crate::libp2p_network::Libp2pNetwork =
        match crate::libp2p_network::Libp2pNetwork::with_group_manager(
            identity.clone(),
            p2p_port,
            cfg.external_ip.clone(),
            group_manager.clone(),
            cfg.bootstrap_peers.clone(),
            cfg.registry_ttl_secs,
            0, // Real genesis at height=0, no simulated bootstrap blocks
            masternode_tx.clone(),
            masternode_publish_rx,
            shared_peers.clone(),
            chain_storage.clone(),
        )
        .await
        {
            Ok(network) => network,
            Err(e) => {
                error!("Failed to initialize libp2p network: {}", e);
                return Err(e);
            }
        };
    let local_peer_id = libp2p_network.local_peer_id();
    info!(
        "Libp2p network started - peer_id: {} on port {}",
        local_peer_id, p2p_port
    );

    // Configure proposal, vote, and certificate channels for P2P network
    libp2p_network.set_proposal_channels(
        proposal_tx.clone(),
        vote_tx.clone(),
        vote_broadcast_rx,
        certificate_broadcast_rx,
    );
    libp2p_network.set_block_acceptance_channel(block_acceptance_publish_rx);
    let pending_proposals = libp2p_network.pending_proposals_ref();
    info!("P2P channels connected to libp2p network");

    // Save peer ID to file for lightnode discovery
    if let Err(e) = save_peer_id_to_file(local_peer_id, p2p_port) {
        warn!("Failed to save peer ID to file: {}", e);
    } else {
        debug!(
            "Saved peer ID {} to file for port {}",
            local_peer_id, p2p_port
        );
    }

    // Log bootstrap peers
    for bootstrap_peer in &cfg.bootstrap_peers {
        info!("Bootstrap peer configured: {}", bootstrap_peer);
    }

    // Initialize system metrics collection
    let metrics_shutdown_tx = telemetry::update_system_metrics_periodically(startup_time).await;

    // Track peer count for logging
    let last_peer_count = 0;

    info!("Masternode running - waiting for block proposals from light nodes");

    #[cfg(feature = "rpc")]
    {
        let rpc_enabled = cfg.rpc_enabled.unwrap_or(false);
        if rpc_enabled {
            let rpc_port = cfg.rpc_port.unwrap_or(8545);
            let rpc_bind = cfg
                .rpc_bind_addr
                .clone()
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let addr: std::net::SocketAddr = format!("{}:{}", rpc_bind, rpc_port)
                .parse()
                .context("invalid RPC bind address")?;
            let pou_reader = Arc::new(savitri_masternode::rpc::MasternodePouReaderImpl::new(
                group_manager.clone(),
            ));
            let rpc_state =
                savitri_rpc::RpcState::for_masternode(pou_reader, Some(chain_storage.clone()));
            #[cfg(feature = "contracts")]
            let rpc_state = rpc_state.with_contract_executor(Arc::new(
                savitri_masternode::rpc::MasternodeContractExecutorImpl::new(chain_storage.clone()),
            ));
            let _rpc_handle = tokio::spawn(async move {
                if let Err(e) = savitri_rpc::run_server(rpc_state, addr).await {
                    error!("RPC server error: {}", e);
                }
            });
            info!("RPC server listening on {}:{}", rpc_bind, rpc_port);
        }
    }

    info!("Masternode ready - waiting for block proposals from light nodes");

    // Run libp2p network in a LocalSet to allow !Sync types (libp2p Swarm is Send but not Sync)
    let local_set = tokio::task::LocalSet::new();
    local_set.spawn_local(async move {
        info!("Starting libp2p network event loop...");
        if let Err(e) = libp2p_network.poll().await {
            // CRITICAL: If poll() returns Err, the entire network task has terminated.
            // The swarm is dropped, ALL TCP connections are closed, and no gossipsub
            // communication is possible. This previously happened due to
            // PublishError::Duplicate being propagated as fatal via the ? operator.
            // All message handlers now use if-let-Err to prevent this, so reaching
            // this point indicates a truly unexpected/unrecoverable error.
            error!("🔴 CRITICAL: Libp2p network task terminated with error: {}", e);
            error!("🔴 CRITICAL: All peer connections have been dropped. The masternode network layer is DEAD.");
            error!("🔴 CRITICAL: This should not happen - all gossipsub publish errors are now handled gracefully.");
            error!("🔴 CRITICAL: The masternode process will continue but cannot communicate with the network.");
        } else {
            warn!("Libp2p network poll() returned Ok - event loop exited normally (unexpected)");
        }
    });

    // Create interval for periodic checks
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    // MN health check: fast failover detection (every 3s), with hysteresis.
    //
    // load — gossipsub backpressure causes MN connections to flap every few
    // seconds, each flap fired `form_and_distribute_groups(force=true)` which
    // reset every LN to Free and shuffled group IDs. Result: 24 re-formations
    // in 40 min, pipelined block commits orphaned each time.
    //
    // Hysteresis: emergency re-formation fires only if
    //   (a) the drop is ≥ MN_DIP_THRESHOLD masternodes, AND
    //   (b) the drop has persisted for ≥ MN_DIP_SUSTAINED_TICKS consecutive
    //       ticks (3 × 3s = 9s), AND
    //   (c) `last_mn_count` baseline is only updated on (b) fire or on recovery
    //       — a single-MN flap never moves the baseline, so oscillations around
    //       the same count don't accumulate into a false emergency.
    let mut mn_health_interval = tokio::time::interval(tokio::time::Duration::from_secs(3));
    let mut last_mn_count: usize = cfg.validators.len();
    let mut mn_dip_ticks: u32 = 0;
    const MN_DIP_THRESHOLD: usize = 2; // absolute drop in MN count
    const MN_DIP_SUSTAINED_TICKS: u32 = 3; // must persist this long

    // Create interval for lightnode list sync every 5 minutes
    let mut lightnode_sync_interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
    // Group sync request: ask other MN for GroupApprovalCertificates so we align active_groups (e.g. after bootstrap)
    let mut group_sync_request_interval =
        tokio::time::interval(tokio::time::Duration::from_secs(60));

    // Create shutdown channel for periodic task
    let (periodic_shutdown_tx, mut periodic_shutdown_rx) = tokio::sync::oneshot::channel::<bool>();

    // Spawn periodic task for monolith and group checks
    let periodic_monolith_scheduler = monolith_scheduler.clone();
    let periodic_monolith_producer = monolith_producer.clone();
    let periodic_monolith_p2p_manager = monolith_p2p_manager.clone();
    let periodic_group_manager = group_manager.clone();
    let periodic_masternode_p2p = masternode_publish_tx.clone();
    let node_id_str_for_periodic = node_id_str.clone();
    let periodic_group_consensus = group_consensus.clone();
    let periodic_chain_storage = chain_storage.clone();
    #[cfg(feature = "storage")]
    let periodic_consensus_storage_adapter = consensus_storage_adapter.clone();
    let periodic_task = tokio::spawn(async move {
        // registered_nodes count + current_epoch across ticks. If EITHER
        // changes (new LN joins, or epoch advances and existing LNs need
        // group reassignment for the new epoch's group_<E>_<idx>_<E>),
        // pass force=true to form_and_distribute_groups so the
        // healthy-groups skip path at group_formation.rs:820 is bypassed
        // and the cluster re-forms.
        //
        // Why both: registered_count alone misses the epoch case (LNs
        // already in the registered_nodes map at boot, count stable
        // forever, healthy-skip prevents reformation). Epoch alone misses
        // the new-LN-joins-mid-epoch case. Both together cover both
        // scenarios — the fix needed for task #17 EPOCH TRANSITION on the
        // LN side to actually result in group reassignment on the MN
        // side.
        let mut last_registered_count: usize = 0;
        let mut last_observed_epoch: u64 = 0;
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    info!("Masternode active - waiting for block proposals");

                    // Check if it's time to create monolith block
                    if periodic_monolith_scheduler.should_create_monolith() {
                        info!("🚀 Creating monolith block for today!");

                        // Create monolith block
                        match periodic_monolith_producer.create_monolith_block().await {
                            Ok(monolith_block) => {
                                info!("✅ Monolith block created successfully!");

                                // Verify monolith block
                                match periodic_monolith_producer.verify_monolith_block(&monolith_block).await {
                                    Ok(true) => {
                                        info!("✅ Monolith block verification passed!");

                                        // Store monolith block in persistent storage
                                        if let Err(e) = periodic_monolith_producer.store_monolith(&monolith_block).await {
                                            error!("Failed to store monolith block: {}", e);
                                        } else {
                                            info!("💾 Monolith block stored successfully!");
                                        }

                                        // Distribute monolith block via P2P
                                        if let Err(e) = periodic_monolith_p2p_manager.broadcast_monolith(&monolith_block).await {
                                            error!("Failed to broadcast monolith block: {}", e);
                                        } else {
                                            info!("📡 Monolith block broadcasted successfully!");
                                        }
                                    }
                                    Ok(false) => {
                                        error!("❌ Monolith block verification failed!");
                                    }
                                    Err(e) => {
                                        error!("Error during monolith block verification: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to create monolith block: {}", e);
                            }
                        }
                    }

                    // 📊 DYNAMIC QUORUM: Update active masternodes count from connected peers.
                    // BFT IDENTITY FIX: Do NOT overwrite ordered_masternode_ids with libp2p
                    // initialization at startup is the source of truth (Tendermint-style).
                    // Updating from connectivity here caused per-MN view divergence and the
                    // mn-5 standalone-Forming-group bug.
                    {
                        let stats = periodic_monolith_p2p_manager.get_stats().await;
                        periodic_group_consensus.update_active_masternodes_count(stats.masternodes).await;
                    }

                    // Periodic group formation and distribution check (with leader election)
                    {
                        let gm = periodic_group_manager.read().await;
                        // count changed OR epoch advanced since last tick.
                        // Both triggers needed; see periodic_task spawn comment.
                        let registered_count = gm.get_registered_nodes().await.len();
                        let registration_changed = registered_count != last_registered_count;
                        let current_epoch = periodic_monolith_scheduler.current_epoch();
                        let epoch_advanced = current_epoch > last_observed_epoch;
                        last_registered_count = registered_count;
                        last_observed_epoch = current_epoch;
                        let force = registration_changed || epoch_advanced;
                        if force {
                            info!(
                                registered = registered_count,
                                current_epoch,
                                registration_changed,
                                epoch_advanced,
                                "🔄 task #18: registration/epoch trigger — forcing group reformation"
                            );
                        }
                        if let Err(e) = gm.form_and_distribute_groups(Some(periodic_monolith_p2p_manager.as_ref()), force).await {
                            debug!("Group formation/distribution check: {}", e);
                        }

                        // Broadcast any pending leader election proposal
                        if let Some(proposal) = gm.take_pending_leader_election_proposal().await {
                            info!("🗳️ Broadcasting pending leader election proposal: {}", proposal.election_id);
                            if let Err(e) = periodic_masternode_p2p.send(LibMasternodeMessage::LeaderElectionProposal(proposal)) {
                                error!("Failed to queue leader election proposal: {}", e);
                            }
                        }

                        info!(
                            manager_id = gm.debug_id(),
                            "Periodic tick: checking pending group broadcasts"
                        );
                        // Broadcast any pending group proposal (from elected leader)
                        if let Some(proposal) = gm.take_pending_group_proposal().await {
                            info!("🏆 Broadcasting pending group proposal from elected leader: {}", proposal.proposal_id);
                            if let Err(e) = periodic_masternode_p2p.send(LibMasternodeMessage::GroupProposal(proposal)) {
                                error!("Failed to queue group proposal: {}", e);
                            }
                        } else {
                            debug!("No pending group proposal to broadcast");
                        }

                        // Broadcast any pending group vote (from consensus processing)
                        if let Some(vote) = gm.take_pending_group_vote().await {
                            info!(
                                proposal_id = %vote.proposal_id,
                                vote_type = ?vote.vote_type,
                                "🗳️ Broadcasting pending group vote"
                            );
                            if let Err(e) = periodic_masternode_p2p.send(LibMasternodeMessage::GroupVote(vote)) {
                                error!("Failed to queue group vote: {}", e);
                            }
                        } else {
                            info!(
                                manager_id = gm.debug_id(),
                                "No pending group vote to broadcast"
                            );
                        }
                    }
                }
                // FIX 3: Periodic re-announce of active groups to lightnodes
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
                    let gm = periodic_group_manager.read().await;
                    let active_groups = gm.get_active_groups().await;
                    if !active_groups.is_empty() {
                        info!(
                            groups_count = active_groups.len(),
                            "🔄 PERIODIC RE-ANNOUNCE: Re-distributing {} active groups to lightnodes",
                            active_groups.len()
                        );
                        let current_epoch = gm.current_epoch();
                        if let Err(e) = periodic_monolith_p2p_manager.distribute_groups(&active_groups, current_epoch).await {
                            debug!("Periodic group re-announce failed: {}", e);
                        }
                    }
                }
                // Group sync request: when we have enough MN connected, request approved certificates
                // so we align active_groups with the rest of the network (fixes late-joining MN).
                //
                // BFT CATCHUP FIX (mn-5 stuck-Forming case):
                // The previous floor `stats.masternodes >= min_mn - 1` (≥3 connected for default
                // min=4) blocked late-joining MNs from requesting sync until the cluster reached
                // a healthy connectivity. But a late MN that misses cert broadcasts during boot
                // is exactly the case that needs aggressive catchup. Lower the threshold to 1
                // (any single connected MN can serve us) AND force the request whenever we have
                // ZERO Active groups (the unambiguous "we are out of sync" signal).
                _ = group_sync_request_interval.tick() => {
                    let stats = periodic_monolith_p2p_manager.get_stats().await;
                    let epoch = {
                        let gm = periodic_group_manager.read().await;
                        gm.current_epoch()
                    };
                    // Detect "we are out of sync": no Active groups locally — the unambiguous
                    // signal that we missed certificate broadcasts during boot or a partition.
                    let no_active_groups = {
                        let gm = periodic_group_manager.read().await;
                        let active = gm.get_active_groups().await;
                        active.iter().all(|g| g.status != group_formation::GroupStatus::Active)
                    };
                    // Request sync if we have at least 1 other MN connected, OR if we are
                    // out of sync (the catchup case where blocking on connectivity makes
                    // the problem worse, not better).
                    if stats.masternodes >= 1 || no_active_groups {
                        let msg = LibMasternodeMessage::GroupSyncRequest {
                            from_epoch: epoch.saturating_sub(2), // pull last 3 epochs to recover from dips
                            to_epoch: epoch,
                            requester_masternode: node_id_str_for_periodic.clone(),
                        };
                        if let Err(e) = periodic_masternode_p2p.send(msg) {
                            debug!("Failed to queue group sync request: {}", e);
                        } else {
                            info!(
                                epoch,
                                connected_mn = stats.masternodes,
                                no_active_groups,
                                "📥 Group sync requested for epochs {}-{} (align active_groups, catchup-aware)",
                                epoch.saturating_sub(2),
                                epoch
                            );
                        }
                    }
                    // Re-broadcast our approved certificate for current (and previous) epoch so
                    // late-joining or temporarily disconnected MN can still receive and align.
                    let epochs_to_send: Vec<u64> = if epoch == 0 {
                        vec![0]
                    } else {
                        vec![epoch, epoch.saturating_sub(1)]
                    };
                    for e in epochs_to_send {
                        if let Some(cert) = periodic_group_consensus.get_approved_certificate(e).await {
                            if let Err(err) = periodic_masternode_p2p.send(LibMasternodeMessage::GroupApprovalCertificate(cert)) {
                                debug!("Failed to re-broadcast approval certificate: {}", err);
                            } else {
                                info!(epoch = e, "📤 Re-broadcast GroupApprovalCertificate for epoch {} (sync topic)", e);
                            }
                        }
                    }
                }
                // MN health check: fast failover with hysteresis.
                // See the `mn_health_interval` definition above for the hysteresis contract.
                _ = mn_health_interval.tick() => {
                    let stats = periodic_monolith_p2p_manager.get_stats().await;
                    let current_mn = stats.masternodes + 1; // +1 for self
                    let drop = last_mn_count.saturating_sub(current_mn);

                    if current_mn >= last_mn_count {
                        // Count stable or recovered — clear dip window and accept baseline.
                        if mn_dip_ticks > 0 {
                            debug!(current = current_mn, previous_baseline = last_mn_count,
                                "MN count recovered — clearing dip window");
                        }
                        mn_dip_ticks = 0;
                        last_mn_count = current_mn;
                    } else if drop < MN_DIP_THRESHOLD {
                        // Transient 1-MN flap — ignore entirely. Do NOT update baseline,
                        // so the next recovery tick above resets state cleanly without
                        // any intermediate emergency.
                        debug!(current = current_mn, baseline = last_mn_count, drop,
                            "MN dip below threshold; ignoring");
                    } else {
                        // drop >= MN_DIP_THRESHOLD — count toward sustained-drop window.
                        mn_dip_ticks += 1;
                        if mn_dip_ticks < MN_DIP_SUSTAINED_TICKS {
                            debug!(current = current_mn, baseline = last_mn_count, drop,
                                tick = mn_dip_ticks, required = MN_DIP_SUSTAINED_TICKS,
                                "MN dip detected, waiting for sustain window");
                        } else {
                            // Fire emergency re-formation.
                            warn!(
                                previous = last_mn_count,
                                current = current_mn,
                                drop,
                                sustained_ticks = mn_dip_ticks,
                                "⚠️ MN FAILOVER: sustained drop of {} MN over {} ticks ({}s), triggering emergency group re-formation",
                                drop, mn_dip_ticks, mn_dip_ticks * 3
                            );
                            // Update quorum count.
                            // BFT IDENTITY FIX: ordered_masternode_ids stays bound to the
                            // it from get_connected_masternode_ids() here either — even
                            // dynamic quorum count adapts to live MNs.
                            periodic_group_consensus.update_active_masternodes_count(stats.masternodes).await;

                            // Emergency group re-formation: re-distribute all groups immediately
                            let gm = periodic_group_manager.read().await;
                            if let Err(e) = gm.form_and_distribute_groups(Some(periodic_monolith_p2p_manager.as_ref()), true).await {
                                warn!("Emergency group re-formation failed: {}", e);
                            } else {
                                info!("✅ Emergency group re-formation completed — groups re-distributed to surviving MN");
                            }
                            // Re-announce immediately
                            let active_groups = gm.get_active_groups().await;
                            if !active_groups.is_empty() {
                                let epoch = gm.current_epoch();
                                if let Err(e) = periodic_monolith_p2p_manager.distribute_groups(&active_groups, epoch).await {
                                    warn!("Emergency group re-announce failed: {}", e);
                                }
                            }
                            // Accept new baseline and reset counter so we don't re-fire
                            // until either another sustained drop or a recovery.
                            mn_dip_ticks = 0;
                            last_mn_count = current_mn;
                        }
                    }
                }
                // Lightnode list sync every 5 minutes
                _ = lightnode_sync_interval.tick() => {
                    let gm = periodic_group_manager.read().await;
                    let lightnodes = gm.get_registered_nodes().await;
                    if !lightnodes.is_empty() {
                        info!("📋 Broadcasting lightnode list sync with {} nodes", lightnodes.len());
                        let msg = LibMasternodeMessage::LightnodeListSync {
                            sender_masternode: node_id_str_for_periodic.clone(),
                            timestamp: chrono::Utc::now().timestamp() as u64,
                            lightnodes,
                        };
                        if let Err(e) = periodic_masternode_p2p.send(msg) {
                            error!("Failed to queue lightnode list sync: {}", e);
                        }
                    } else {
                        debug!("No lightnodes registered yet, skipping list sync");
                    }
                }
                // Handle shutdown signal (from main loop's ctrl_c handler via oneshot channel).
                // NOTE: Do NOT add tokio::signal::ctrl_c() here — on Windows, console
                // events propagate to ALL processes in the same console group, causing
                // every masternode in a local test to shut down simultaneously.
                _ = &mut periodic_shutdown_rx => {
                    info!("Periodic task received shutdown signal");
                    break;
                }
            }
        }
    });

    // Main processing loop - wait for block proposals and handle P2P connections
    let mut monolith_announce_rx = monolith_announce_rx;
    let mut certified_proposal_cache: HashMap<
        (u64, u64, [u8; 64], String),
        proposal_validator::LightnodeProposal,
    > = HashMap::new();
    loop {
        tokio::select! {
            // Run LocalSet in the main loop
            _ = local_set.run_until(async {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await
            }) => {
                // LocalSet completed or yielded, continue loop
            }

            // Handle monolith announcements from monolith_p2p
            Some(msg) = monolith_announce_rx.recv() => {
                info!("🔔 RACCOMANDAZIONE #3: Received message from monolith_announce_rx channel");
                // Convert monolith_p2p::MasternodeMessage to LibMasternodeMessage
                let lib_msg = match msg {
                    savitri_masternode::monolith_p2p::MasternodeMessage::LightnodeGroupAnnounce(announce) => {
                        info!(
                            group_id = %announce.group_id,
                            epoch = announce.epoch,
                            members_count = announce.members.len(),
                            "🔔 RACCOMANDAZIONE #3: Converting LightnodeGroupAnnounce for gossipsub publication"
                        );
                        LibMasternodeMessage::LightnodeGroupAnnounce(
                            savitri_masternode::masternode_p2p::LightnodeGroupAnnounce {
                                epoch: announce.epoch,
                                group_id: announce.group_id,
                                members: announce.members,
                                member_addresses: announce.member_addresses,
                                proposer: announce.proposer,
                                timestamp: announce.timestamp,
                                signature: announce.signature,
                                signer_pubkey: announce.signer_pubkey,
                                assigned_shards: announce.assigned_shards,
                                num_shards: announce.num_shards,
                            }
                        )
                    }
                    // Handle other conversions as needed
                    _ => {
                        debug!("Ignoring non-group-announce message from monolith_p2p");
                        continue;
                    }
                };

                info!("🔔 RACCOMANDAZIONE #3: Sending converted message to masternode_publish_tx for gossipsub");
                if let Err(e) = masternode_publish_tx.send(lib_msg) {
                    error!("❌ Failed to forward monolith announcement: {}", e);
                } else {
                    info!("✅ Message forwarded to masternode_publish_tx successfully");
                }
            }

            // The role (Leader / Backup / Participant) only affects whether this
            // MN publishes the BlockAcceptanceCertificate (and when).
            Some(proposal_with_role) = proposal_rx.recv() => {
                let proposal = proposal_with_role.proposal;
                let role = proposal_with_role.role;
                let proposal_key = (
                    proposal.height,
                    proposal.round_id,
                    proposal.block_hash,
                    proposal.proposer_group_id.clone(),
                );
                certified_proposal_cache.insert(proposal_key, proposal.clone());
                info!(
                    height = proposal.height,
                    round_id = proposal.round_id,
                    tx_count = proposal.tx_count,
                    role = ?role,
                    "📥 Received block proposal from light node (role={:?})", role,
                );

                if let Some(vote) = validator.vote_on_proposal(&proposal).await {
                    info!(
                        height = vote.height,
                        round_id = vote.round_id,
                        vote_type = ?vote.vote_type,
                        role = ?role,
                        "✅ Voted on proposal, broadcasting to network"
                    );

                    // direct-cert paths used to short-circuit the BFT vote
                    // aggregation: leader signed and published a single-vote
                    // certificate immediately on its own approval, backup did
                    // the same after a 2 s timeout. The light nodes accepted
                    // only voters>=1) and finalized blocks WITHOUT the
                    // cluster-wide BFT majority — i.e. one MN could finalize
                    // unilaterally, breaking the consensus security model.
                    //
                    // Correct flow (BFT-only): every MN that receives this
                    // broadcasts its vote to the other MN (vote_broadcast_tx
                    // below), and feeds it to the local `vote_aggregator`.
                    // When an MN's aggregator collects >= quorum_for_voters(N)
                    // votes (see set_group_mn_sizes patched at line ~782 to use
                    // the cluster-wide MN count), it emits a real
                    // BlockCertificate carrying every voter's public key and
                    // an aggregated signature. That cert path is the only
                    // legitimate one — the leader/backup direct paths are
                    // disabled.
                    let _ = role; // role-specific direct cert paths removed (BFT-only).

                    // Drop the proposal entry from pending_proposals immediately
                    // since we no longer need the backup-timeout fallback.
                    if !proposal.proposer_group_id.is_empty() {
                        let mut pending = pending_proposals.write().await;
                        pending.remove(&(
                            proposal.proposer_group_id.clone(),
                            proposal.height,
                            proposal.round_id,
                        ));
                    }

                    // Broadcast vote to other masternodes
                    if let Err(e) = vote_broadcast_tx.send(vote.clone()) {
                        error!("Failed to send vote for broadcast: {}", e);
                    }

                    // Also send to aggregator
                    if let Err(e) = vote_tx.send(vote) {
                        error!("Failed to send vote to aggregator: {}", e);
                    }
                } else {
                    warn!("Proposal validation failed, not voting");
                }
            }

            // Process incoming votes from other masternodes
            Some(vote) = vote_rx.recv() => {
                info!(
                    height = vote.height,
                    round_id = vote.round_id,
                    vote_type = ?vote.vote_type,
                    voter = %hex::encode(&vote.voter_pubkey[..8]),
                    "Received vote from another masternode"
                );

                // Aggregate votes and check if quorum is reached
                if let Some(certificate) = vote_aggregator.add_vote(vote).await {
                    let cert_height = certificate.height;
                    let cert_round = certificate.round_id;
                    let cert_hash = certificate.block_hash;
                    let cert_group = certificate.group_id.clone();
                    info!(
                        height = cert_height,
                        round_id = certificate.round_id,
                        votes = certificate.votes.len(),
                        "✅ Block certificate created - quorum reached!"
                    );

                    // LATENCY FIX: Broadcast certificate FIRST, persist to RocksDB AFTER.
                    // The LN pipeline blocks on receiving the certificate — every ms of
                    // delay here directly reduces block production rate. RocksDB persistence
                    // is durable but not latency-critical; the data is in-memory in the
                    // proposal cache until persistence completes.
                    if let Err(e) = certificate_broadcast_tx.send(certificate) {
                        error!("Failed to send certificate for broadcast: {}", e);
                    } else {
                        info!(
                            height = cert_height,
                            "📤 [MN->MN/MN->LN] Certificate queued for broadcast (persistence follows)"
                        );
                    }

                    // Now persist (non-blocking for the LN pipeline)
                    if let Err(e) = persist_chain_head(periodic_chain_storage.as_ref(), cert_height, &cert_hash) {
                        error!(
                            height = cert_height,
                            error = %e,
                            "Failed to persist chain head after quorum certificate"
                        );
                    }

                    let proposal_key = (cert_height, cert_round, cert_hash, cert_group.clone());
                    let cached_proposal = if let Some(proposal) = certified_proposal_cache.remove(&proposal_key) {
                        Some(proposal)
                    } else {
                        let fallback_key = certified_proposal_cache
                            .keys()
                            .find(|(h, r, hash, _)| *h == cert_height && *r == cert_round && *hash == cert_hash)
                            .cloned();
                        fallback_key.and_then(|k| certified_proposal_cache.remove(&k))
                    };
                    if let Some(proposal) = cached_proposal {
                        match persist_certified_block(periodic_chain_storage.as_ref(), &proposal) {
                            Ok(block_bytes) => {
                                debug!(
                                    height = cert_height,
                                    "💾 Persisted certified block payload to RocksDB"
                                );
                                #[cfg(feature = "storage")]
                                {
                                    if let Err(e) = periodic_consensus_storage_adapter.persist_certified_block(
                                        cert_height,
                                        &cert_hash,
                                        &block_bytes,
                                    ) {
                                        error!(
                                            height = cert_height,
                                            error = %e,
                                            "Failed to persist certified block payload to consensus RocksDB"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    height = cert_height,
                                    error = %e,
                                    "Failed to persist certified block payload after quorum certificate"
                                );
                            }
                        }
                    } else {
                        warn!(
                            height = cert_height,
                            round_id = cert_round,
                            group_id = %cert_group,
                            "Quorum certificate finalized but no cached proposal payload found to persist block body"
                        );
                    }
                    #[cfg(feature = "storage")]
                    {
                        if let Err(e) = periodic_consensus_storage_adapter.persist_latest_height(cert_height) {
                            error!(
                                height = cert_height,
                                error = %e,
                                "Failed to persist consensus latest_height after quorum certificate"
                            );
                        }
                    }
                }
            }

            // Handle monolith P2P messages
            Some((sender_peer_id, message)) = monolith_rx.recv() => {
                if let Err(e) = monolith_p2p_manager.handle_message(sender_peer_id, message).await {
                    error!("Failed to handle monolith P2P message: {}", e);
                }
            }

            // Handle masternode P2P messages
            Some((sender_peer_id, masternode_message)) = masternode_rx.recv() => {
                // Process masternode messages (group proposals, votes, etc.)
                match masternode_message {
                    LibMasternodeMessage::GroupProposal(proposal) => {
                        info!("Processing group proposal from masternode");
                        let vote = group_consensus.process_proposal(proposal).await?;

                        // Send vote back to proposer via P2P
                        info!("Broadcasting vote for proposal: {}", vote.proposal_id);
                        if let Err(e) = masternode_publish_tx.send(LibMasternodeMessage::GroupVote(vote)) {
                            error!("Failed to queue vote: {}", e);
                        }
                    }
                    LibMasternodeMessage::GroupVote(vote) => {
                        info!("🔔 RACCOMANDAZIONE #1: Processing group vote from masternode");
                        let certificate = group_consensus.process_vote(vote).await?;

                        if let Some(certificate) = certificate {
                            info!(
                                proposal_id = %certificate.proposal.proposal_id,
                                groups_count = certificate.proposal.groups.len(),
                                "🔔 RACCOMANDAZIONE #1: Group consensus reached, broadcasting certificate"
                            );
                            info!(
                                proposal_id = %certificate.proposal.proposal_id,
                                "Queueing group approval certificate for broadcast"
                            );
                            // Process locally so distribution to lightnodes happens
                            info!(
                                proposal_id = %certificate.proposal.proposal_id,
                                "🔔 RACCOMANDAZIONE #1: Calling process_approval_certificate to trigger distribution"
                            );
                            group_consensus.process_approval_certificate(certificate.clone()).await?;
                            if let Err(e) = masternode_publish_tx.send(LibMasternodeMessage::GroupApprovalCertificate(certificate)) {
                                error!("Failed to queue approval certificate: {}", e);
                            }

                            // Reset leader election state after successful group approval
                            group_consensus.on_group_approval_complete().await;
                        } else {
                            info!("Group vote processed but consensus not yet reached");
                        }
                    }
                    LibMasternodeMessage::GroupApprovalCertificate(certificate) => {
                        info!("Processing group approval certificate");
                        group_consensus.process_approval_certificate(certificate).await?;
                        // Reset leader election state after successful group approval
                        group_consensus.on_group_approval_complete().await;
                    }
                    LibMasternodeMessage::AvailableLightnodesRequest { requester_masternode, epoch } => {
                        let available_count = group_consensus.get_available_lightnodes_count().await;
                        let msg = LibMasternodeMessage::AvailableLightnodesResponse {
                            responder_masternode: node_id_str.clone(),
                            epoch,
                            available_count,
                        };
                        if let Err(e) = masternode_publish_tx.send(msg) {
                            error!("Failed to queue available lightnodes response: {}", e);
                        }
                    }
                    LibMasternodeMessage::AvailableLightnodesResponse { responder_masternode, available_count, .. } => {
                        debug!("Available lightnodes response from {}: {}", responder_masternode, available_count);
                    }
                    LibMasternodeMessage::GroupSyncRequest { from_epoch, to_epoch, requester_masternode, .. } => {
                        info!("Group sync request for epochs {}-{}", from_epoch, to_epoch);

                        // Collect approved certificates for the requested epoch range
                        let mut certificates = Vec::new();
                        for epoch in from_epoch..=to_epoch {
                            if let Some(certificate) = group_consensus.get_approved_certificate(epoch).await {
                                certificates.push(certificate);
                            }
                        }

                        // Send sync response
                        let sync_response = LibMasternodeMessage::GroupSyncResponse {
                            certificates,
                            responder_masternode: node_id_str.clone(),
                        };

                        // Send sync response back to requester
                        if let Err(e) = masternode_publish_tx.send(sync_response) {
                            error!("Failed to queue group sync response: {}", e);
                        } else {
                            debug!("Sent group sync response to {}", requester_masternode);
                        }
                    }
                    LibMasternodeMessage::GroupSyncResponse { certificates, .. } => {
                        info!("Received {} group certificates in sync response", certificates.len());
                        for certificate in certificates {
                            group_consensus.process_approval_certificate(certificate).await?;
                        }
                    }
                    LibMasternodeMessage::LeaderElectionProposal(proposal) => {
                        info!(
                            election_id = %proposal.election_id,
                            proposer = %proposal.proposer_masternode,
                            "🗳️ Processing leader election proposal"
                        );
                        // Process the proposal and generate a certificate if valid
                        match group_consensus.process_leader_election_proposal(proposal).await {
                            Ok(Some(certificate)) => {
                                info!(
                                    election_id = %certificate.election_id,
                                    "🗳️ Approved leader election, broadcasting certificate"
                                );
                                if let Err(e) = masternode_publish_tx.send(LibMasternodeMessage::LeaderElectionCertificate(certificate)) {
                                    error!("Failed to queue leader election certificate: {}", e);
                                }
                            }
                            Ok(None) => {
                                debug!("Leader election proposal not approved or already processed");
                            }
                            Err(e) => {
                                error!("Failed to process leader election proposal: {}", e);
                            }
                        }
                    }
                    LibMasternodeMessage::LeaderElectionCertificate(certificate) => {
                        info!(
                            election_id = %certificate.election_id,
                            approver = %certificate.approver_masternode,
                            "🗳️ Processing leader election certificate"
                        );
                        if let Err(e) = group_consensus.process_leader_election_certificate(certificate).await {
                            error!("Failed to process leader election certificate: {}", e);
                        }
                    }
                    LibMasternodeMessage::LightnodeListSync { sender_masternode, lightnodes, timestamp } => {
                        info!(
                            sender = %sender_masternode,
                            count = lightnodes.len(),
                            timestamp = timestamp,
                            "📋 Processing lightnode list sync from masternode"
                        );
                        // Merge received lightnodes into local group manager
                        let gm = group_manager.write().await;
                        for lightnode in lightnodes {
                            if let Err(e) = gm.register_light_node(lightnode).await {
                                warn!("Failed to register lightnode from sync: {}", e);
                            }
                        }
                    }
                    LibMasternodeMessage::LightnodeGroupAnnounce(_) => {
                        debug!("Ignoring lightnode group announce on masternode control channel");
                    }
                }
            },
            // Handle shutdown signal (Ctrl+C)
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
                // Send shutdown signal to periodic task
                let _ = periodic_shutdown_tx.send(true);
                break;
            }
        }
    }

    // Graceful shutdown
    let _ = group_manager.write().await.stop().await;

    // Graceful shutdown for metrics
    let _ = metrics_shutdown_tx.send(true);

    // Wait for periodic task to complete
    match periodic_task.await {
        Ok(_) => {
            info!("Periodic task shutdown completed");
        }
        Err(e) => {
            if e.is_panic() {
                error!("Periodic task panicked during shutdown: {}", e);
            } else if e.is_cancelled() {
                info!("Periodic task was cancelled during shutdown");
            } else {
                error!("Error waiting for periodic task to shutdown: {}", e);
            }
        }
    }

    info!("Masternode shutdown complete");

    Ok(())
}

fn load_identity(cfg: &MasternodeConfig) -> Result<Keypair> {
    let key_path = &cfg.network_key_path;
    info!("Loading identity from path: {}", key_path.display());

    // Ensure parent directory exists
    if let Some(parent) = std::path::Path::new(key_path).parent() {
        info!("Creating parent directory: {:?}", parent);
        fs::create_dir_all(parent)?;
    }

    if std::path::Path::new(key_path).exists() {
        info!("Key file exists, reading from: {}", key_path.display());
        let key_bytes = fs::read(key_path)?;
        Keypair::from_protobuf_encoding(&key_bytes).context("Failed to parse identity key")
    } else {
        info!(
            "Key file does not exist, generating new key at: {}",
            key_path.display()
        );
        let key = Keypair::generate_ed25519();
        let key_bytes = key.to_protobuf_encoding()?;
        fs::write(key_path, key_bytes)?;
        info!("Generated new identity key at {}", key_path.display());
        Ok(key)
    }
}
