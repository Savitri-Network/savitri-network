//! DAG reader trait for RPC endpoints

use serde::{Deserialize, Serialize};

/// Block info from the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagBlockInfo {
    pub hash: String,
    pub height: u64,
    pub group_id: String,
    pub proposer: String,
    pub parent_hashes: Vec<String>,
    pub transactions_count: usize,
    pub timestamp: u64,
    pub pou_score: u32,
}

/// Tip info from the DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagTipInfo {
    pub group_id: String,
    pub hash: String,
    pub height: u64,
}

/// Group info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagGroupInfo {
    pub group_id: String,
    pub members: Vec<String>,
    #[serde(rename = "type")]
    pub node_type: String,
}

/// Trait for reading DAG state from the lightnode
#[async_trait::async_trait]
pub trait DagReader: Send + Sync {
    /// Get all blocks at a specific height
    async fn get_blocks_at_height(&self, height: u64) -> Vec<DagBlockInfo>;
    /// Get current tips (latest block per group)
    async fn get_tips(&self) -> Vec<DagTipInfo>;
    /// Get current groups
    async fn get_groups(&self) -> Vec<DagGroupInfo>;
}
