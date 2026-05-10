//! RPC request/response types for JSON-RPC 2.0
//!
//! All types used by the Savitri JSON-RPC server.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Block types ────────────────────────────────────────────────────────────

/// Block response (used by chain_getBlock, chain_getBlockByNumber, chain_getBlockByHash, chain_getLatestBlock)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResponse {
    pub hash: String,
    pub height: u64,
    pub timestamp: u64,
    pub parent_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_hashes: Option<Vec<String>>,
    pub state_root: String,
    pub tx_root: String,
    pub proposer: String,
    pub version: u32,
    pub transaction_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions: Option<Vec<TransactionReceiptResponse>>,
}

/// Chain info response (chain_getChainInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainInfoResponse {
    pub chain_id: String,
    pub chain_name: String,
    pub block_height: u64,
    pub latest_block_hash: String,
    pub latest_block_timestamp: u64,
    pub protocol_version: String,
}

/// Canonical-chain throughput as observed from this node's committed storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStatsResponse {
    pub scope: String,
    pub window_seconds: u64,
    pub latest_block_height: u64,
    pub latest_block_timestamp: u64,
    pub sampled_blocks: u64,
    pub sampled_transactions: u64,
    pub transactions_per_second: f64,
    pub blocks_per_second: f64,
    pub blocks_per_minute: f64,
}

/// Paginated block list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockListResponse {
    pub blocks: Vec<BlockResponse>,
    pub offset: u64,
    pub limit: u64,
    pub has_more: bool,
}

/// DAG blocks for a specific height.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagBlocksByHeightResponse {
    pub height: u64,
    pub blocks: Vec<BlockResponse>,
    pub total: u64,
}

/// DAG node for visualization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagGraphNode {
    pub hash: String,
    pub height: u64,
    pub timestamp: u64,
    pub parent_hash: String,
}

/// DAG edge for visualization (parent -> child).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagGraphEdge {
    pub from: String,
    pub to: String,
}

/// Paginated DAG graph snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagGraphResponse {
    pub nodes: Vec<DagGraphNode>,
    pub edges: Vec<DagGraphEdge>,
    pub tips: Vec<String>,
    pub offset: u64,
    pub limit: u64,
    pub has_more: bool,
    pub total: u64,
    pub min_height: Option<u64>,
    pub max_height: Option<u64>,
}

// ─── Transaction types ──────────────────────────────────────────────────────

/// Transaction response (tx_getTransaction)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub nonce: u64,
    pub fee: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u64>,
}

/// Transaction receipt response (tx_getTransactionReceipt)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceiptResponse {
    pub hash: String,
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub fee: Option<u128>,
    pub block_height: Option<u64>,
    pub block_hash: Option<String>,
    pub timestamp: Option<u64>,
    pub status: String, // "confirmed", "pending", "not_found"
}

/// Paginated transaction list response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionListResponse {
    pub transactions: Vec<TransactionReceiptResponse>,
    pub offset: u64,
    pub limit: u64,
    pub has_more: bool,
}

/// Send raw transaction request
#[derive(Debug, Clone, Deserialize)]
pub struct SendRawTransactionRequest {
    pub raw_tx_hex: String,
}

/// Payload for an Ikarus compatibility transaction wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IkarusCompatPayload {
    pub ikarus_tx_raw: String,
    pub ikarus_signable: String,
    pub ikarus_signature: String,
    pub ikarus_public_key: String,
}

/// RPC request for `savitri_sendIkarusCompatTransaction`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IkarusCompatTransactionRequest {
    pub version: u32,
    #[serde(rename = "type")]
    pub tx_type: String,
    pub sender: String,
    pub nonce: u64,
    pub payload: IkarusCompatPayload,
    pub signature: String,
}

/// RPC response for `savitri_sendIkarusCompatTransaction`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IkarusCompatTransactionResponse {
    pub status: String,
    pub savitri_tx_hash: String,
    pub ikarus_tx_hash: String,
}

// ─── Account types ──────────────────────────────────────────────────────────

/// Account response (account_getAccount, account_getBalance, account_getNonce)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountResponse {
    pub address: String,
    pub balance: String,
    pub nonce: u64,
}

/// Per-block reward entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardHistoryEntryResponse {
    pub block_height: u64,
    pub block_hash: String,
    pub amount: String,
    pub reward_type: String,
    pub timestamp: u64,
}

/// Paginated reward history plus aggregate totals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RewardHistoryResponse {
    pub address: String,
    pub total_rewards: String,
    pub group_check_rewards: String,
    pub reward_balance: String,
    pub balance: String,
    pub nonce: u64,
    pub rewards: Vec<RewardHistoryEntryResponse>,
    pub offset: u64,
    pub limit: u64,
    pub has_more: bool,
}

/// Token balance response (account_getTokenBalance, token_getTokenBalance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalanceResponse {
    pub address: String,
    pub token_id: String,
    pub balance: String,
}

// ─── Network types ──────────────────────────────────────────────────────────

/// Node info response (net_nodeInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfoResponse {
    pub node_id: String,
    pub protocol_version: String,
    pub network: String,
    pub listening: bool,
    pub peer_count: u64,
    pub block_height: u64,
    pub syncing: bool,
    pub mode: String, // "masternode" or "lightnode"
}

/// Peer info response (net_peers)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfoResponse {
    pub peer_id: String,
    pub score: Option<u16>,
}

// ─── PoU / Consensus types ─────────────────────────────────────────────────

/// PoU local state response (pou_getConsensusState)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouLocalResponse {
    pub local_score: Option<u16>,
    pub leader: Option<String>,
    pub leader_score: Option<u16>,
    pub epoch: Option<u64>,
    pub local_is_leader: bool,
    pub election_ready: bool,
}

/// PoU peers response (peer_id -> score)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouPeersResponse {
    pub peers: HashMap<String, u16>,
}

/// Validator info (pou_getValidators)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub node_id: String,
    pub pou_score: f64,
    pub health_score: f64,
}

/// Stake info response (pou_getStakeInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeInfoResponse {
    pub address: String,
    pub stake_amount: String,
    pub is_validator: bool,
}

/// Epoch info response (pou_getEpochInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochInfoResponse {
    pub current_epoch: u64,
    pub epoch_start_block: u64,
    pub blocks_in_epoch: u64,
    pub validators_count: u64,
}

// ─── Token types ────────────────────────────────────────────────────────────

/// Token info response (token_getTokenInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfoResponse {
    pub token_id: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub total_supply: String,
}

/// Token transfer record (token_getTokenTransfers)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenTransferResponse {
    pub tx_hash: String,
    pub token_id: String,
    pub from: String,
    pub to: String,
    pub amount: String,
    pub block_height: u64,
    pub timestamp: u64,
}

// ─── Mempool types ──────────────────────────────────────────────────────────

/// Mempool size response (mempool_getSize)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolSizeResponse {
    pub pending: u64,
    pub queued: u64,
}

/// Mempool counters and confirmation totals.
///
/// `queued_total` were misleading — they actually expose the "ready" pool
/// (drainable for block production) and the cumulative admission counter.
/// The new canonical names are `ready` / `cumulative_admitted`. The previous
/// names are kept as serde aliases for ONE release so existing clients
/// (loadtest, dashboard) continue to deserialize the response unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolStatsResponse {
    /// Transactions currently held in the pending sub-pool.
    /// Source: `metrics::get_pending_tx_count()` (semantics: future-nonce or
    /// otherwise non-immediately-executable TX awaiting promotion).
    pub pending: u64,
    /// Transactions ready to be drained for block production (main pool).
    /// Source: `metrics::get_ready_tx_count()`.
    /// Previously serialized as `queued` (misleading — it is NOT the queued pool).
    #[serde(alias = "queued")]
    pub ready: u64,
    /// Total transactions currently in the mempool (pending + ready).
    pub total: u64,
    /// Cumulative count of TX admitted into the mempool since process start.
    pub admitted_total: u64,
    /// Cumulative count of TX admitted (alias of `admitted_total`).
    /// Previously serialized as `queued_total` (misleading — it is the
    /// admission counter, not a queued-pool counter).
    #[serde(alias = "queued_total")]
    pub cumulative_admitted: u64,
    pub rejected_total: u64,
    pub removed_total: u64,
    pub evicted_total: u64,
    pub confirmed_total: u64,
    pub window_1m: MempoolCounterWindowResponse,
    pub window_1h: MempoolCounterWindowResponse,
}

/// Rolling-window delta of cumulative mempool counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolCounterWindowResponse {
    pub admitted_total: u64,
    /// Window delta of admitted TX (renamed from `queued_total`).
    /// Previous name kept as serde alias for backward compatibility.
    #[serde(alias = "queued_total")]
    pub cumulative_admitted: u64,
    pub rejected_total: u64,
    pub removed_total: u64,
    pub evicted_total: u64,
    pub confirmed_total: u64,
}

/// Mempool transaction status (mempool_getTransactionStatus)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolTxStatusResponse {
    pub hash: String,
    pub status: String, // "pending", "queued", "not_found", "confirmed"
}

// ─── Utility types ──────────────────────────────────────────────────────────

/// Syncing status response (savitri_syncing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncingResponse {
    pub syncing: bool,
    pub current_block: u64,
    pub highest_block: u64,
}

/// Gas estimation response (savitri_estimateGas)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GasEstimateResponse {
    pub estimated_gas: u64,
    pub gas_price: String,
}

// ─── Faucet types ───────────────────────────────────────────────────────────

/// Faucet claim request (POST body)
#[derive(Debug, Clone, Deserialize)]
pub struct FaucetClaimRequest {
    /// Recipient address (32 bytes hex-encoded, with or without 0x prefix)
    pub address: String,
}

// ─── Monolith types ─────────────────────────────────────────────────────

/// Lightweight monolith metadata for discovery/listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithInfoResponse {
    pub exec_height: u64,
    pub window_start: u64,
    pub epoch_id: u64,
    pub block_count: u64,
    pub size_bytes: u64,
    pub monolith_id: String,
    pub produced_at_ms: u64,
    pub cosignature_count: usize,
}

/// Full monolith block response (header + metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithBlockResponse {
    pub header: serde_json::Value,
    pub start_height: u64,
    pub end_height: u64,
    pub block_count: u64,
    pub total_transactions: u64,
    pub created_at: u64,
    pub creator_id: String,
}

// ─── Health types ───────────────────────────────────────────────────────────

/// Health check response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
    pub mode: String,
}
