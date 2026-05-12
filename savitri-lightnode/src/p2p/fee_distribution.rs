//! P2P Fee Distribution Module
//!
//! This module handles fee distribution among P2P nodes for block rewards.

#![allow(dead_code)]
#![allow(unused_variables)]

use libp2p::PeerId;
use std::collections::HashMap;
use tracing::{debug, info};

use anyhow::Result;

/// P2P node information for fee distribution
#[derive(Debug, Clone)]
pub struct P2PNode {
    /// Peer ID
    pub peer_id: PeerId,
    /// PoU score (basis points)
    pub pou_score: u32,
    /// Contribution weight
    pub contribution_weight: f64,
}

/// Fee distribution result
#[derive(Debug, Clone)]
pub struct FeeDistribution {
    /// Proposer fee amount
    pub proposer_fee: u128,
    /// P2P node fees
    pub p2p_fees: HashMap<PeerId, u128>,
    /// Total distributed
    pub total_distributed: u128,
}

/// Collect P2P nodes for fee distribution
pub async fn collect_p2p_nodes_for_fee_distribution(
    pou_state: &crate::p2p::pou::PouState,
    peer_accounts: &HashMap<PeerId, crate::p2p::types::PeerInfo>,
    known_peer_accounts: &HashMap<PeerId, [u8; 32]>,
    masternode_address: &str,
) -> Vec<P2PNode> {
    let mut p2p_nodes = Vec::new();

    for (peer_id, peer_info) in peer_accounts {
        // Skip masternode
        if masternode_address == hex::encode(peer_info.account) {
            continue;
        }

        // Get PoU score for this peer
        let pou_score = if let Some(account) = known_peer_accounts.get(peer_id) {
            pou_state.get_score(account).await.unwrap_or(0) as u32
        } else {
            0
        };

        // Calculate contribution weight based on PoU score and activity
        let contribution_weight = calculate_contribution_weight(pou_score, peer_info);

        p2p_nodes.push(P2PNode {
            peer_id: peer_id.clone(),
            pou_score,
            contribution_weight,
        });
    }

    // Sort by contribution weight (highest first)
    p2p_nodes.sort_by(|a, b| {
        b.contribution_weight
            .partial_cmp(&a.contribution_weight)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    p2p_nodes
}

/// Calculate contribution weight for a P2P node
fn calculate_contribution_weight(pou_score: u32, peer_info: &crate::p2p::types::PeerInfo) -> f64 {
    let pou_weight = pou_score as f64 / 10000.0; // Convert basis points to 0-1
    let activity_weight = if peer_info.priority { 1.0 } else { 0.5 }; // Priority nodes get higher weight

    (pou_weight * 0.7) + (activity_weight * 0.3) // 70% PoU, 30% activity
}

/// Calculate fee distribution among P2P nodes
pub fn calculate_fee_distribution(
    proposer_reward: u128,
    p2p_reward: u128,
    p2p_nodes: &[P2PNode],
    tx_count: usize,
) -> FeeDistribution {
    let mut p2p_fees = HashMap::new();
    let mut total_p2p_weight = 0.0;

    // Calculate total weight
    for node in p2p_nodes {
        total_p2p_weight += node.contribution_weight;
    }

    // Distribute P2P reward based on contribution weight
    if total_p2p_weight > 0.0 {
        for node in p2p_nodes {
            let fee_share = (node.contribution_weight / total_p2p_weight) * p2p_reward as f64;
            p2p_fees.insert(node.peer_id.clone(), fee_share as u128);
        }
    }

    let total_distributed = proposer_reward + p2p_reward;

    FeeDistribution {
        proposer_fee: proposer_reward,
        p2p_fees,
        total_distributed,
    }
}

/// Process block with fee distribution
// process_block_with_fee_distribution removed: was log-only placeholder.
// Actual fee distribution is handled by commit_pending_block() in block.rs
// which credits proposer + P2P node accounts via storage.put_account().

/// Validate fee distribution
pub fn validate_fee_distribution(
    fee_distribution: &FeeDistribution,
    proposer_reward: u128,
    p2p_reward: u128,
) -> Result<()> {
    // Check proposer fee matches expected
    if fee_distribution.proposer_fee != proposer_reward {
        return Err(anyhow::anyhow!(
            "Proposer fee mismatch: {} != {}",
            fee_distribution.proposer_fee,
            proposer_reward
        )
        .into());
    }

    // Check total distributed matches expected
    let expected_total = proposer_reward + p2p_reward;
    if fee_distribution.total_distributed != expected_total {
        return Err(anyhow::anyhow!(
            "Total distributed mismatch: {} != {}",
            fee_distribution.total_distributed,
            expected_total
        )
        .into());
    }

    // Check P2P fees don't exceed P2P reward
    let total_p2p_fees: u128 = fee_distribution.p2p_fees.values().sum();
    if total_p2p_fees > p2p_reward {
        return Err(anyhow::anyhow!(
            "P2P fees exceed reward: {} > {}",
            total_p2p_fees,
            p2p_reward
        )
        .into());
    }

    Ok(())
}
