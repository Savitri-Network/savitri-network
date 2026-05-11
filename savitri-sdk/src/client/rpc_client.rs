//! JSON-RPC 2.0 Client for Savitri Network
//!
//! Sends all requests as JSON-RPC 2.0 POST to the node endpoint.
//! The Savitri RPC server accepts requests at POST `/` or POST `/rpc`.
//!
//! Supported methods:
//!   savitri_health              - Health check
//!   savitri_blockNumber         - Current block height
//!   savitri_getBlockByHeight    - Block by height
//!   savitri_getBlockHash        - Block hash by height
//!   savitri_getAccount          - Account balance and nonce
//!   savitri_getTransaction      - Transaction by hash
//!   savitri_sendRawTransaction  - Submit raw transaction
//!   savitri_faucetClaim         - Testnet faucet claim
//!   savitri_pouLocal            - Local PoU score
//!   savitri_pouPeers            - All peer PoU scores
//!   savitri_pouGroups           - PoU groups (masternode)
//!   savitri_pouMasternodes      - Masternode info (masternode)
//!   savitri_pouGroupNodes       - Nodes in a PoU group (masternode)

use crate::types::{
    AccountResponse, BlockResponse, FaucetClaimResponse, HealthResponse, JsonRpcRequest,
    JsonRpcResponse, MasternodesResponse, PouGroupNodesResponse, PouGroupsResponse,
    PouLocalResponse, PouPeersResponse, SdkError, SendTransactionResult, TransactionResponse,
};
use serde::de::DeserializeOwned;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Configuration for the RPC client.
#[derive(Debug, Clone)]
pub struct RpcClientConfig {
    /// Base URL of the RPC node (e.g. "http://localhost:8545").
    pub url: String,
    /// Request timeout in seconds (default: 30).
    pub timeout: Option<u64>,
    /// Allow insecure HTTP connections to remote (non-localhost) endpoints.
    /// Default: false. Only set to true for testing or development.
    pub allow_insecure: bool,
}

impl Default for RpcClientConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8545".to_string(),
            timeout: Some(30),
            allow_insecure: false,
        }
    }
}

/// JSON-RPC 2.0 client for the Savitri Network.
///
/// All methods send a `POST` request containing a JSON-RPC 2.0 envelope to the
/// node endpoint.  The client automatically manages request IDs.
pub struct RpcClient {
    http: reqwest::Client,
    /// The full URL used for JSON-RPC POST requests.
    endpoint: String,
    /// Monotonically increasing request ID.
    next_id: AtomicU64,
}

impl RpcClient {
    /// Create a new RPC client from a full configuration.
    ///
    /// # Security
    ///
    /// By default, only HTTPS and localhost HTTP URLs are accepted.
    /// Remote HTTP endpoints are rejected unless `allow_insecure` is set.
    pub fn new(config: RpcClientConfig) -> Result<Self, SdkError> {
        let url = config.url.trim_end_matches('/');

        // Allow HTTP only for localhost/127.0.0.1/[::1] (local development).
        if !config.allow_insecure {
            let is_localhost = url.starts_with("http://127.0.0.1")
                || url.starts_with("http://localhost")
                || url.starts_with("http://[::1]");
            let is_https = url.starts_with("https://");

            if !is_localhost && !is_https {
                return Err(SdkError::InvalidResponse(
                    "Only HTTPS is allowed for remote RPC endpoints. \
                     HTTP is permitted only for localhost/127.0.0.1. \
                     Set allow_insecure=true to override (not recommended)."
                        .into(),
                ));
            }
        }

        let timeout = config
            .timeout
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(30));

        let mut builder = reqwest::Client::builder().timeout(timeout);

        if url.starts_with("https://") {
            builder = builder.min_tls_version(reqwest::tls::Version::TLS_1_2);
        }

        let http = builder.build().map_err(SdkError::HttpError)?;

        let endpoint = format!("{}/rpc", url);

        Ok(Self {
            http,
            endpoint,
            next_id: AtomicU64::new(1),
        })
    }

    /// Create a new RPC client from a URL string using default settings.
    pub fn from_url(url: impl Into<String>) -> Result<Self, SdkError> {
        Self::new(RpcClientConfig {
            url: url.into(),
            timeout: Some(30),
            allow_insecure: false,
        })
    }

    // ─── Low-level JSON-RPC call ───────────────────────────────────────────

    /// Send a raw JSON-RPC 2.0 request and return the parsed result.
    ///
    /// This is the core transport method.  All typed helper methods delegate to
    /// this function.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, SdkError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id,
        };

        let http_response = self
            .http
            .post(&self.endpoint)
            .json(&request)
            .send()
            .await
            .map_err(SdkError::HttpError)?;

        let rpc_response: JsonRpcResponse =
            http_response.json().await.map_err(SdkError::HttpError)?;

        // Check for JSON-RPC error
        if let Some(err) = rpc_response.error {
            return Err(SdkError::RpcError {
                code: err.code,
                message: err.message,
                data: err.data,
            });
        }

        // Extract result
        let result_value = rpc_response
            .result
            .ok_or_else(|| SdkError::InvalidResponse("Missing result in response".into()))?;

        serde_json::from_value(result_value).map_err(SdkError::JsonError)
    }

    /// Send a raw JSON-RPC 2.0 request and return the raw `serde_json::Value`.
    pub async fn call_raw(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, SdkError> {
        self.call(method, params).await
    }

    // ─── Health ────────────────────────────────────────────────────────────

    /// Check if the RPC node is healthy.
    ///
    /// Calls `savitri_health`.
    pub async fn health(&self) -> Result<HealthResponse, SdkError> {
        self.call("savitri_health", serde_json::json!([])).await
    }

    /// Ping the node by performing a health check.
    ///
    /// Returns `true` if the node responds, `false` otherwise.
    pub async fn ping(&self) -> Result<bool, SdkError> {
        match self.health().await {
            Ok(_) => Ok(true),
            Err(SdkError::HttpError(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    // ─── Chain queries ─────────────────────────────────────────────────────

    /// Get the current block height.
    ///
    /// Calls `savitri_blockNumber`.  Returns a JSON object with a `result` field
    /// containing the height as u64.
    pub async fn get_block_number(&self) -> Result<u64, SdkError> {
        let val: serde_json::Value = self
            .call("savitri_blockNumber", serde_json::json!([]))
            .await?;
        // The handler returns {"result": <u64>}
        val.get("result")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| SdkError::InvalidResponse("Expected {\"result\": <u64>}".into()))
    }

    /// Get a block by height.
    ///
    /// Calls `savitri_getBlockByHeight` with params `[height]`.
    pub async fn get_block_by_height(&self, height: u64) -> Result<BlockResponse, SdkError> {
        self.call("savitri_getBlockByHeight", serde_json::json!([height]))
            .await
    }

    /// Get a block hash by height.
    ///
    /// Calls `savitri_getBlockHash` with params `[height]`.  Returns the hex
    /// hash string.
    pub async fn get_block_hash(&self, height: u64) -> Result<String, SdkError> {
        let val: serde_json::Value = self
            .call("savitri_getBlockHash", serde_json::json!([height]))
            .await?;
        val.get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| SdkError::InvalidResponse("Expected {\"result\": \"<hash>\"}".into()))
    }

    // ─── Account queries ───────────────────────────────────────────────────

    /// Get account balance and nonce.
    ///
    /// Calls `savitri_getAccount` with params `[address]`.
    pub async fn get_account(&self, address: &str) -> Result<AccountResponse, SdkError> {
        self.call("savitri_getAccount", serde_json::json!([address]))
            .await
    }

    /// Convenience: get only the balance string.
    pub async fn get_balance(&self, address: &str) -> Result<String, SdkError> {
        let acc = self.get_account(address).await?;
        Ok(acc.balance)
    }

    /// Convenience: get only the nonce.
    pub async fn get_nonce(&self, address: &str) -> Result<u64, SdkError> {
        let acc = self.get_account(address).await?;
        Ok(acc.nonce)
    }

    // ─── Transaction operations ────────────────────────────────────────────

    /// Get a transaction by hash.
    ///
    /// Calls `savitri_getTransaction` with params `[hash]`.
    pub async fn get_transaction(&self, hash: &str) -> Result<TransactionResponse, SdkError> {
        self.call("savitri_getTransaction", serde_json::json!([hash]))
            .await
    }

    /// Submit a signed raw transaction.
    ///
    /// Calls `savitri_sendRawTransaction` with params `[raw_tx_hex]`.
    /// Returns the result containing the transaction hash.
    pub async fn send_raw_transaction(
        &self,
        raw_tx_hex: &str,
    ) -> Result<SendTransactionResult, SdkError> {
        let val: serde_json::Value = self
            .call(
                "savitri_sendRawTransaction",
                serde_json::json!([raw_tx_hex]),
            )
            .await?;
        // Handler returns {"result": "0x..."}
        let tx_hash = val
            .get("result")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| SdkError::InvalidResponse("Expected {\"result\": \"0x...\"}".into()))?;
        Ok(SendTransactionResult { tx_hash })
    }

    // ─── Faucet ────────────────────────────────────────────────────────────

    /// Claim testnet faucet tokens.
    ///
    /// Calls `savitri_faucetClaim` with params `[address]`.
    pub async fn faucet_claim(&self, address: &str) -> Result<FaucetClaimResponse, SdkError> {
        let val: serde_json::Value = self
            .call("savitri_faucetClaim", serde_json::json!([address]))
            .await?;
        // Handler returns {"result": {"tx_hash": "0x...", "amount": "..."}}
        let result_obj = val
            .get("result")
            .ok_or_else(|| SdkError::InvalidResponse("Expected result object".into()))?;
        serde_json::from_value(result_obj.clone()).map_err(SdkError::JsonError)
    }

    // ─── PoU / Consensus queries ───────────────────────────────────────────

    /// Get local PoU score and leader info.
    ///
    /// Calls `savitri_pouLocal`.
    pub async fn pou_local(&self) -> Result<PouLocalResponse, SdkError> {
        self.call("savitri_pouLocal", serde_json::json!([])).await
    }

    /// Get all peer PoU scores.
    ///
    /// Calls `savitri_pouPeers`.
    pub async fn pou_peers(&self) -> Result<PouPeersResponse, SdkError> {
        self.call("savitri_pouPeers", serde_json::json!([])).await
    }

    /// Get PoU groups (masternode only).
    ///
    /// Calls `savitri_pouGroups`.
    pub async fn pou_groups(&self) -> Result<PouGroupsResponse, SdkError> {
        self.call("savitri_pouGroups", serde_json::json!([])).await
    }

    /// Get masternode PoU info (masternode only).
    ///
    /// Calls `savitri_pouMasternodes`.
    pub async fn pou_masternodes(&self) -> Result<MasternodesResponse, SdkError> {
        self.call("savitri_pouMasternodes", serde_json::json!([]))
            .await
    }

    /// Get nodes in a specific PoU group (masternode only).
    ///
    /// Calls `savitri_pouGroupNodes` with params `[group_id]`.
    pub async fn pou_group_nodes(&self, group_id: &str) -> Result<PouGroupNodesResponse, SdkError> {
        self.call("savitri_pouGroupNodes", serde_json::json!([group_id]))
            .await
    }

    // ─── Batch requests ────────────────────────────────────────────────────

    /// Send a batch of JSON-RPC 2.0 requests.
    ///
    /// Returns a vector of raw JSON-RPC response values.
    pub async fn batch(
        &self,
        requests: Vec<(&str, serde_json::Value)>,
    ) -> Result<Vec<serde_json::Value>, SdkError> {
        let batch: Vec<serde_json::Value> = requests
            .into_iter()
            .map(|(method, params)| {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": method,
                    "params": params,
                    "id": id
                })
            })
            .collect();

        let http_response = self
            .http
            .post(&self.endpoint)
            .json(&batch)
            .send()
            .await
            .map_err(SdkError::HttpError)?;

        let responses: Vec<serde_json::Value> =
            http_response.json().await.map_err(SdkError::HttpError)?;

        Ok(responses)
    }
}
