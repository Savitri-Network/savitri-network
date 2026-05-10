//! Rewards distribution job for Savitri SAVI
//!
//! Two modes:
//!   - Testnet (fixed): distributes flat 50/100 SAVI per 24h.
//!   - Mainnet (pool-based): splits a fixed annual budget across all active nodes
//!     weighted by node type and PoU score.  Formula:
//!       reward = (annual_budget × type_weight × pou_multiplier) / total_weighted_nodes / 365
//!
//! Pool-based mode is emission-schedule-aware: the annual budget decrements on a
//! 4-year halving cycle aligned with the HalvingEngine in savitri-contracts.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Local node type used by rewards logic.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum NodeType {
    LightNode,
    Masternode,
    Guardian,
}

/// Local PoU snapshot used for reward accounting.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PouSnapshot {
    pub epoch_id: u64,
    pub pou_score: u16,
    pub blocks_proposed: u32,
    pub blocks_validated: u32,
    pub uptime_percent: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RewardMintRecord {
    node_address: [u8; 32],
    amount: u128,
    node_type: NodeType,
    pou_snapshot: PouSnapshot,
    current_epoch: u64,
}

/// Reward config (testnet: 50/100 SAVI per 24h)
#[derive(Debug, Clone)]
pub struct RewardsConfig {
    pub light_node_base: u128,
    pub masternode_base: u128,
    pub reward_period_secs: u64,
    pub min_uptime_percent: f64,
}

impl Default for RewardsConfig {
    fn default() -> Self {
        Self {
            light_node_base: 50_000_000_000_000_000_000,  // 50 SAVI
            masternode_base: 100_000_000_000_000_000_000, // 100 SAVI
            reward_period_secs: 86400,                    // 24h
            min_uptime_percent: 80.0,
        }
    }
}

// ---------------------------------------------------------------------------
// Pool-based reward system (mainnet)
// ---------------------------------------------------------------------------

/// Emission phases driven by the staking allocation schedule.
///
/// Total staking allocation: 900M SAVI.
/// Phases are defined as (year_start, year_end, annual_rate_pct) where the
/// rate is applied to the original 900M pool.
const EMISSION_PHASES: &[(u32, u32, f64)] = &[
    (0, 2, 15.0),  // Phase 1: 135M SAVI/year
    (2, 4, 12.0),  // Phase 2: 108M SAVI/year
    (4, 8, 8.0),   // Phase 3:  72M SAVI/year
    (8, 16, 4.0),  // Phase 4:  36M SAVI/year
    (16, 50, 2.0), // Phase 5:  18M SAVI/year
];

/// Total staking allocation (900M SAVI, 18 decimals)
const STAKING_POOL_SAVI: u128 = 900_000_000;
const ONE_SAVI: u128 = 1_000_000_000_000_000_000u128;

/// Relative weights: MN earns 3× a LN; Guardian earns 1.5×.
pub const LN_WEIGHT: f64 = 1.0;
pub const MN_WEIGHT: f64 = 3.0;
pub const GUARDIAN_WEIGHT: f64 = 1.5;

/// PoU score tiers and their multipliers (score 0-1000).
const POU_TIERS: &[(u16, f64)] = &[
    (900, 3.0), // Platinum
    (700, 2.0), // Gold
    (500, 1.5), // Silver
    (300, 1.0), // Bronze
    (0, 0.5),   // Below minimum (partial reward)
];

/// Returns the PoU multiplier for a given score (0–1000).
pub fn pou_multiplier(score: u16) -> f64 {
    for &(threshold, multiplier) in POU_TIERS {
        if score >= threshold {
            return multiplier;
        }
    }
    0.5 // fallback
}

/// Annual budget (in whole SAVI) for staking rewards at the given network year.
/// `network_year` starts at 0 on mainnet genesis.
pub fn annual_budget_savi(network_year: u32) -> u128 {
    for &(start, end, rate_pct) in EMISSION_PHASES {
        if network_year >= start && network_year < end {
            // rate_pct of 900M pool
            return (STAKING_POOL_SAVI as f64 * rate_pct / 100.0) as u128;
        }
    }
    // After year 50 — minimal residual emission
    (STAKING_POOL_SAVI as f64 * 0.5 / 100.0) as u128
}

/// Pool-based config for mainnet reward distribution.
#[derive(Debug, Clone)]
pub struct PoolRewardsConfig {
    /// Network year (0-based from mainnet genesis) — determines emission phase.
    pub network_year: u32,
    /// Number of currently active lightnodes.
    pub active_lightnodes: u32,
    /// Number of currently active masternodes.
    pub active_masternodes: u32,
    /// Number of currently active guardians.
    pub active_guardians: u32,
    /// Minimum uptime % to be eligible.
    pub min_uptime_percent: f64,
    /// Minimum PoU score to receive any reward.
    pub min_pou_score: u16,
}

impl Default for PoolRewardsConfig {
    fn default() -> Self {
        Self {
            network_year: 0,
            active_lightnodes: 100,
            active_masternodes: 10,
            active_guardians: 5,
            min_uptime_percent: 80.0,
            min_pou_score: 300,
        }
    }
}

/// Compute the pool reward (in raw SAVI units, 18 decimals) for a single node.
///
/// Formula:
///   total_weight = Σ(type_weight_i × pou_multiplier_i) for all eligible nodes
///   reward_i = (annual_budget × type_weight_i × pou_multiplier_i / total_weight) / 365
///
/// This function computes the budget share for one node given pre-computed aggregate
/// `total_weighted_nodes` so the caller can avoid re-computing per node.
pub fn compute_pool_reward(
    network_year: u32,
    node_type: NodeType,
    pou_score: u16,
    total_weighted_nodes: f64,
) -> u128 {
    if total_weighted_nodes <= 0.0 {
        return 0;
    }
    let type_weight = match node_type {
        NodeType::LightNode => LN_WEIGHT,
        NodeType::Masternode => MN_WEIGHT,
        NodeType::Guardian => GUARDIAN_WEIGHT,
    };
    let annual_savi = annual_budget_savi(network_year);
    let daily_savi = annual_savi / 365;
    let multiplier = pou_multiplier(pou_score);
    let node_share = type_weight * multiplier / total_weighted_nodes;
    let reward_whole_savi = (daily_savi as f64 * node_share) as u128;
    reward_whole_savi.saturating_mul(ONE_SAVI)
}

/// Compute the total weighted node count from a pool config (for passing into
/// `compute_pool_reward`).  Call once per cycle.
pub fn total_weight(cfg: &PoolRewardsConfig, nodes: &[EligibleNode]) -> f64 {
    nodes
        .iter()
        .filter(|n| n.uptime_percent >= cfg.min_uptime_percent)
        .filter(|n| n.pou_score.unwrap_or(0) >= cfg.min_pou_score)
        .map(|n| {
            let tw = match n.node_type {
                NodeType::LightNode => LN_WEIGHT,
                NodeType::Masternode => MN_WEIGHT,
                NodeType::Guardian => GUARDIAN_WEIGHT,
            };
            tw * pou_multiplier(n.pou_score.unwrap_or(0))
        })
        .sum()
}

/// Run one pool-based rewards cycle.
pub fn run_pool_rewards_cycle(
    storage: &savitri_storage::Storage,
    nodes: &[EligibleNode],
    cfg: &PoolRewardsConfig,
) -> Result<usize> {
    let period_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400; // daily

    let tw = total_weight(cfg, nodes);
    info!(
        period_id,
        network_year = cfg.network_year,
        candidate_nodes = nodes.len(),
        total_weighted_nodes = tw,
        min_uptime_percent = cfg.min_uptime_percent,
        min_pou_score = cfg.min_pou_score,
        "Pool rewards cycle started"
    );
    if tw == 0.0 {
        debug!(
            period_id,
            candidate_nodes = nodes.len(),
            "Pool rewards cycle skipped because no nodes met the eligibility thresholds"
        );
        return Ok(0);
    }

    let mut minted_count = 0;
    for node in nodes {
        if node.uptime_percent < cfg.min_uptime_percent {
            debug!(
                address = %hex::encode(&node.address[..8]),
                node_type = ?node.node_type,
                uptime_percent = node.uptime_percent,
                min_uptime_percent = cfg.min_uptime_percent,
                "Skipping pool reward mint because uptime is below threshold"
            );
            continue;
        }
        let score = node.pou_score.unwrap_or(0);
        if score < cfg.min_pou_score {
            debug!(
                address = %hex::encode(&node.address[..8]),
                node_type = ?node.node_type,
                pou_score = score,
                min_pou_score = cfg.min_pou_score,
                "Skipping pool reward mint because PoU score is below threshold"
            );
            continue;
        }
        let amount = compute_pool_reward(cfg.network_year, node.node_type, score, tw);
        if amount == 0 {
            debug!(
                address = %hex::encode(&node.address[..8]),
                node_type = ?node.node_type,
                pou_score = score,
                total_weighted_nodes = tw,
                "Skipping pool reward mint because computed reward amount is zero"
            );
            continue;
        }
        debug!(
            address = %hex::encode(&node.address[..8]),
            node_type = ?node.node_type,
            pou_score = score,
            uptime_percent = node.uptime_percent,
            amount_savi = amount / ONE_SAVI,
            period_id,
            "Minting pool reward"
        );
        let pou_snapshot = PouSnapshot {
            epoch_id: period_id,
            pou_score: score,
            blocks_proposed: 0,
            blocks_validated: 0,
            uptime_percent: node.uptime_percent,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        if let Err(e) = mint_reward_compat(
            storage,
            &node.address,
            amount,
            node.node_type,
            pou_snapshot,
            period_id,
        ) {
            warn!(
                address = %hex::encode(&node.address[..8]),
                error = %e,
                "Pool reward mint failed"
            );
            continue;
        }
        minted_count += 1;
        info!(
            address = %hex::encode(&node.address[..8]),
            node_type = ?node.node_type,
            pou_score = score,
            amount_savi = amount / ONE_SAVI,
            "Pool reward minted"
        );
    }
    info!(
        period_id,
        minted_count,
        candidate_nodes = nodes.len(),
        "Pool rewards cycle completed"
    );
    Ok(minted_count)
}

/// Node eligible for rewards
#[derive(Debug, Clone)]
pub struct EligibleNode {
    pub address: [u8; 32],
    pub node_type: NodeType,
    pub uptime_percent: f64,
    /// PoU score 0–1000; None treated as 0 (below min).
    pub pou_score: Option<u16>,
}

/// Run one rewards cycle: mint for each eligible node
pub fn run_rewards_cycle(
    storage: &savitri_storage::Storage,
    nodes: &[EligibleNode],
    config: &RewardsConfig,
) -> Result<usize> {
    let period_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / config.reward_period_secs;

    info!(
        period_id,
        candidate_nodes = nodes.len(),
        reward_period_secs = config.reward_period_secs,
        min_uptime_percent = config.min_uptime_percent,
        "Rewards cycle started"
    );

    let mut minted_count = 0;
    for node in nodes {
        if node.uptime_percent < config.min_uptime_percent {
            debug!(
                address = %hex::encode(&node.address[..8]),
                node_type = ?node.node_type,
                uptime_percent = node.uptime_percent,
                min_uptime_percent = config.min_uptime_percent,
                "Skipping reward mint because uptime is below threshold"
            );
            continue;
        }

        let amount = match node.node_type {
            NodeType::LightNode => config.light_node_base,
            NodeType::Masternode => config.masternode_base,
            NodeType::Guardian => config.masternode_base, // treat as masternode
        };
        debug!(
            address = %hex::encode(&node.address[..8]),
            node_type = ?node.node_type,
            uptime_percent = node.uptime_percent,
            amount,
            period_id,
            "Minting fixed reward"
        );

        let pou_snapshot = PouSnapshot {
            epoch_id: period_id,
            pou_score: (node.uptime_percent as u16).min(1000),
            blocks_proposed: 0,
            blocks_validated: 0,
            uptime_percent: node.uptime_percent,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        if let Err(e) = mint_reward_compat(
            storage,
            &node.address,
            amount,
            node.node_type,
            pou_snapshot,
            period_id,
        ) {
            warn!(
                address = %hex::encode(&node.address[..8]),
                error = %e,
                "Failed to mint reward"
            );
            continue;
        }

        minted_count += 1;
        info!(
            address = %hex::encode(&node.address[..8]),
            node_type = ?node.node_type,
            amount = amount,
            "Minted SAVI reward"
        );
    }

    info!(
        period_id,
        minted_count,
        candidate_nodes = nodes.len(),
        "Rewards cycle completed"
    );
    Ok(minted_count)
}

/// Compatibility mint path while `savitri-storage` reward APIs are not exposed.
fn mint_reward_compat(
    storage: &savitri_storage::Storage,
    node_address: &[u8; 32],
    amount: u128,
    node_type: NodeType,
    pou_snapshot: PouSnapshot,
    current_epoch: u64,
) -> Result<()> {
    let key = format!(
        "reward_mint:{}:{}",
        hex::encode(node_address),
        current_epoch
    );
    let record = RewardMintRecord {
        node_address: *node_address,
        amount,
        node_type,
        pou_snapshot,
        current_epoch,
    };
    let value = bincode::serialize(&record)?;
    storage.put_cf(
        savitri_storage::storage::CF_METADATA,
        key.as_bytes(),
        &value,
    )?;
    Ok(())
}

/// Spawn the 24h rewards task
pub fn spawn_rewards_task(
    storage: Arc<savitri_storage::Storage>,
    config: RewardsConfig,
    node_provider: Arc<dyn Fn() -> Vec<EligibleNode> + Send + Sync>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(
            reward_period_secs = config.reward_period_secs,
            min_uptime_percent = config.min_uptime_percent,
            "Rewards task started"
        );
        let mut interval = tokio::time::interval(Duration::from_secs(config.reward_period_secs));
        interval.tick().await; // skip first immediate tick
        loop {
            interval.tick().await;
            let nodes = node_provider();
            debug!(
                candidate_nodes = nodes.len(),
                "Rewards task fetched nodes for cycle"
            );
            if nodes.is_empty() {
                tracing::debug!("Rewards cycle: no eligible nodes");
                continue;
            }
            match run_rewards_cycle(storage.as_ref(), &nodes, &config) {
                Ok(n) => info!("Rewards cycle: minted for {} nodes", n),
                Err(e) => warn!("Rewards cycle failed: {}", e),
            }
        }
    })
}
