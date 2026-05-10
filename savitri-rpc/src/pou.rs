//! PoU reader traits for RPC endpoints (lightnode and masternode)

use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;

use crate::types::PouLocalResponse;

/// Trait for reading PoU state (lightnode peers)
#[async_trait]
pub trait PouReader: Send + Sync {
    async fn get_local(&self) -> PouLocalResponse;
    async fn get_all_peers(&self) -> HashMap<String, u16>;
}

/// Trait for reading live network connection state.
#[async_trait]
pub trait NetworkReader: Send + Sync {
    async fn get_connected_peers(&self) -> Vec<String>;
}

/// Group info for masternode RPC
#[derive(Debug, Clone, Serialize)]
pub struct PouGroupInfo {
    pub group_id: String,
    pub health_score: f64,
    pub members: Vec<String>,
    pub proposer: Option<String>,
    pub group_leader_masternode: Option<String>,
    pub epoch: u64,
}

/// Masternode PoU info
#[derive(Debug, Clone, Serialize)]
pub struct MasternodePouInfo {
    pub node_id: String,
    pub pou_score: f64,
    pub health_score: f64,
}

/// Trait for reading masternode PoU (groups, masternodes, nodes per group)
#[async_trait]
pub trait MasternodePouReader: Send + Sync {
    async fn get_groups(&self) -> Vec<PouGroupInfo>;
    async fn get_masternodes(&self) -> Vec<MasternodePouInfo>;
    async fn get_group_nodes(&self, group_id: &str) -> HashMap<String, u16>;
}
