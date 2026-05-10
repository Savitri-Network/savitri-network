//! Public types for Savitri SDK
//!
//! Types matching the savitri-rpc JSON-RPC 2.0 response structures.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Type aliases ──────────────────────────────────────────────────────────

/// Account address (32 bytes hex-encoded, 64 hex characters).
pub type Address = String;

/// Transaction hash (hex-encoded).
pub type TransactionHash = String;

/// Balance in smallest unit (string representation of u128).
pub type Balance = u128;

/// Account nonce.
pub type Nonce = u64;

// ─── SDK configuration ────────────────────────────────────────────────────

/// SDK configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkConfig {
    /// RPC node URL (e.g. "http://localhost:8545").
    pub rpc_url: String,
    /// Network identifier.
    pub network_id: String,
    /// Request timeout in seconds.
    pub timeout: Option<u64>,
}

impl Default for SdkConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://localhost:8545".to_string(),
            network_id: "testnet".to_string(),
            timeout: Some(30),
        }
    }
}

// ─── JSON-RPC 2.0 protocol types ──────────────────────────────────────────

/// JSON-RPC 2.0 request envelope.
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcRequest {
    /// Must be "2.0".
    pub jsonrpc: String,
    /// The method name (e.g. "savitri_blockNumber").
    pub method: String,
    /// Method parameters (positional array or named object).
    pub params: serde_json::Value,
    /// Request identifier.
    pub id: u64,
}

/// JSON-RPC 2.0 response envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcResponse {
    /// Must be "2.0".
    pub jsonrpc: String,
    /// Successful result (mutually exclusive with error).
    pub result: Option<serde_json::Value>,
    /// Error object (mutually exclusive with result).
    pub error: Option<JsonRpcError>,
    /// Request identifier echoed back.
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured error data.
    pub data: Option<serde_json::Value>,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}

// ─── RPC response types (match savitri-rpc/src/types.rs) ──────────────────

/// Block information returned by `savitri_getBlockByHeight`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResponse {
    /// Block hash (hex-encoded).
    pub hash: String,
    /// Block height.
    pub height: u64,
    /// Block timestamp (unix seconds).
    pub timestamp: u64,
    /// Parent block hash (hex-encoded).
    pub parent_hash: String,
    /// State root hash (hex-encoded).
    #[serde(default)]
    pub state_root: String,
    /// Transaction root hash (hex-encoded).
    #[serde(default)]
    pub tx_root: String,
    /// Block proposer public key (hex-encoded).
    pub proposer: String,
    /// Block version.
    #[serde(default)]
    pub version: u32,
    /// Number of transactions in the block.
    #[serde(default)]
    pub transaction_count: u64,
}

/// Account information returned by `savitri_getAccount`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountResponse {
    /// Account address (hex-encoded, present in server response).
    #[serde(default)]
    pub address: String,
    /// Balance as decimal string (u128).
    pub balance: String,
    /// Current nonce.
    pub nonce: u64,
}

/// Transaction information returned by `savitri_getTransaction`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResponse {
    /// Transaction hash (hex-encoded).
    pub hash: String,
    /// Sender address (hex-encoded).
    pub from: String,
    /// Recipient address (hex-encoded).
    pub to: String,
    /// Transfer amount.
    pub amount: u64,
    /// Transaction nonce.
    pub nonce: u64,
    /// Transaction fee.
    pub fee: Option<u128>,
    /// Block timestamp (if confirmed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    /// Block height (if confirmed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u64>,
}

/// Health check response from `savitri_health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Status string ("ok").
    pub status: String,
    /// Service name.
    pub service: String,
    /// Node mode: "lightnode", "masternode", or "unknown".
    pub mode: String,
}

/// PoU local score response from `savitri_pouLocal`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouLocalResponse {
    /// Local node PoU score.
    pub local_score: Option<u16>,
    /// Current leader peer ID.
    pub leader: Option<String>,
    /// Leader PoU score.
    pub leader_score: Option<u16>,
    /// Current epoch number.
    pub epoch: Option<u64>,
    /// Whether this node is the current leader.
    pub local_is_leader: bool,
    /// Whether election is ready.
    pub election_ready: bool,
}

/// PoU peers response from `savitri_pouPeers`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouPeersResponse {
    /// Map of peer_id -> PoU score.
    pub peers: HashMap<String, u16>,
}

/// PoU group info from `savitri_pouGroups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouGroupInfo {
    /// Group identifier.
    pub group_id: String,
    /// Group health score.
    pub health_score: f64,
    /// Member peer IDs.
    pub members: Vec<String>,
    /// Current proposer.
    pub proposer: Option<String>,
    /// Group leader masternode.
    pub group_leader_masternode: Option<String>,
    /// Current epoch.
    pub epoch: u64,
}

/// PoU groups response from `savitri_pouGroups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouGroupsResponse {
    /// List of groups.
    pub groups: Vec<PouGroupInfo>,
}

/// Masternode PoU info from `savitri_pouMasternodes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasternodePouInfo {
    /// Node identifier.
    pub node_id: String,
    /// PoU score.
    pub pou_score: f64,
    /// Health score.
    pub health_score: f64,
}

/// Masternodes response from `savitri_pouMasternodes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasternodesResponse {
    /// List of masternodes.
    pub masternodes: Vec<MasternodePouInfo>,
}

/// Group nodes response from `savitri_pouGroupNodes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouGroupNodesResponse {
    /// Group identifier.
    pub group_id: String,
    /// Map of node_id -> PoU score.
    pub nodes: HashMap<String, u16>,
}

/// Faucet claim result from `savitri_faucetClaim`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaucetClaimResponse {
    /// Transaction hash of the faucet transfer.
    pub tx_hash: String,
    /// Amount sent (string representation).
    pub amount: String,
}

// ─── SDK transaction types ────────────────────────────────────────────────

/// Unsigned transaction (before signing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnsignedTransaction {
    /// Sender address (hex-encoded, 64 chars).
    pub from: Address,
    /// Recipient address (None for contract deploy).
    pub to: Option<Address>,
    /// Transfer amount.
    pub value: Balance,
    /// Sender nonce.
    pub nonce: Nonce,
    /// Fee (optional).
    pub fee: Option<Balance>,
    /// Payload data (for contract calls).
    pub data: Option<Vec<u8>>,
}

/// Signed transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTransaction {
    /// The unsigned transaction.
    pub transaction: UnsignedTransaction,
    /// Public key (32 bytes, hex-encoded for serde).
    pub public_key: Vec<u8>,
    /// Ed25519 signature (64 bytes, as Vec for serde compatibility).
    pub signature: Vec<u8>,
}

/// Result of sending a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendTransactionResult {
    /// Transaction hash (hex-encoded, with 0x prefix).
    pub tx_hash: String,
}

// ─── SDK error types ──────────────────────────────────────────────────────

/// Errors specific to the Savitri SDK.
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// JSON-RPC error returned by the server.
    #[error("RPC error {code}: {message}")]
    RpcError {
        /// Numeric error code.
        code: i64,
        /// Error message.
        message: String,
        /// Optional error data.
        data: Option<serde_json::Value>,
    },

    /// HTTP transport error.
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Invalid response from server.
    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    /// Invalid parameter.
    #[error("Invalid parameter: {0}")]
    InvalidParam(String),

    /// Wallet not connected to RPC.
    #[error("RPC client not configured on wallet")]
    NoRpcClient,

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

impl From<JsonRpcError> for SdkError {
    fn from(e: JsonRpcError) -> Self {
        SdkError::RpcError {
            code: e.code,
            message: e.message,
            data: e.data,
        }
    }
}
