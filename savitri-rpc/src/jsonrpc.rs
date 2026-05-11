//! JSON-RPC 2.0 handler layer for Savitri RPC
//!
//! All RPC methods are exposed via JSON-RPC 2.0 over HTTP POST.
//! Single endpoint: POST / (or POST /rpc)
//!
//! Method namespaces:
//!   chain_*    - Chain/block queries
//!   tx_*       - Transaction queries and submission
//!   account_*  - Account balance and nonce
//!   net_*      - Network information
//!   mempool_*  - Mempool status
//!   pou_*      - Proof of Utility / consensus
//!   token_*    - Token information
//!   savitri_*  - Protocol utilities

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::handlers::{self, RpcError};
use crate::types::IkarusCompatTransactionRequest;
use crate::RpcState;

// ─── JSON-RPC 2.0 protocol types ──────────────────────────────────────────

/// JSON-RPC 2.0 Request
#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Value,
}

/// JSON-RPC 2.0 Response
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Value,
}

/// JSON-RPC 2.0 Error
#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// Standard JSON-RPC error codes
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
// Custom error codes (application-specific, -32000 to -32099)
const RESOURCE_NOT_FOUND: i64 = -32001;
const SERVICE_UNAVAILABLE: i64 = -32002;
const NOT_IMPLEMENTED: i64 = -32003;

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data,
            }),
            id,
        }
    }
}

/// Convert internal RpcError to JSON-RPC error response.
///
/// SECURITY (PT-M01): Error details are sanitized before returning to the client.
/// Internal errors return a generic message. NotFound/BadRequest messages are
/// stripped of implementation details (deserialization errors, internal formats).
fn rpc_error_to_response(id: Value, err: RpcError) -> JsonRpcResponse {
    let (code, message) = match &err {
        RpcError::StorageUnavailable => (SERVICE_UNAVAILABLE, "Storage not available".to_string()),
        RpcError::MempoolUnavailable => (SERVICE_UNAVAILABLE, "Mempool not available".to_string()),
        RpcError::NotFound(msg) => {
            // Strip internal details from NotFound messages
            let safe_msg = if msg.contains("deserialize") || msg.contains("Failed to") {
                "Resource not found".to_string()
            } else {
                msg.clone()
            };
            (RESOURCE_NOT_FOUND, safe_msg)
        }
        RpcError::BadRequest(msg) => {
            // Log full reason server-side for triage; client gets stripped msg
            // unless SAVITRI_RPC_VERBOSE_ERRORS=1 (dev/testnet only).
            let verbose = std::env::var("SAVITRI_RPC_VERBOSE_ERRORS")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            let safe_msg = if msg.contains("rejected:") {
                tracing::warn!(detail = %msg, "RPC BadRequest (tx rejection)");
                if verbose {
                    msg.clone()
                } else {
                    "Transaction rejected".to_string()
                }
            } else {
                msg.clone()
            };
            (INVALID_PARAMS, safe_msg)
        }
        RpcError::Internal(msg) => {
            tracing::error!(detail = %msg, "RPC internal error");
            (INTERNAL_ERROR, "Internal server error".to_string())
        }
        RpcError::NotImplemented(msg) => (NOT_IMPLEMENTED, msg.clone()),
        RpcError::ServiceUnavailable(msg) => {
            tracing::warn!(detail = %msg, "RPC ServiceUnavailable (retryable)");
            (
                SERVICE_UNAVAILABLE,
                "Service temporarily unavailable, retry".to_string(),
            )
        }
    };
    JsonRpcResponse::error(id, code, message, None)
}

// ─── Parameter extraction helpers ──────────────────────────────────────────

/// Extract a string parameter from params array or object
fn get_string_param(params: &Value, index: usize, key: &str) -> Option<String> {
    if let Some(arr) = params.as_array() {
        if let Some(val) = arr.get(index) {
            return val.as_str().map(|s| s.to_string());
        }
    }
    if let Some(obj) = params.as_object() {
        if let Some(val) = obj.get(key) {
            return val.as_str().map(|s| s.to_string());
        }
    }
    None
}

/// Extract a u64 parameter from params array or object
fn get_u64_param(params: &Value, index: usize, key: &str) -> Option<u64> {
    if let Some(arr) = params.as_array() {
        if let Some(val) = arr.get(index) {
            return val.as_u64();
        }
    }
    if let Some(obj) = params.as_object() {
        if let Some(val) = obj.get(key) {
            return val.as_u64();
        }
    }
    None
}

// ─── Main JSON-RPC 2.0 dispatcher ─────────────────────────────────────────

/// SECURITY (PT-C02): Maximum number of requests in a single JSON-RPC batch.
const MAX_BATCH_SIZE: usize = 100;

/// Main JSON-RPC 2.0 entry point (handles single and batch requests)
pub async fn handle_jsonrpc(
    State(state): State<Arc<RpcState>>,
    Json(body): Json<Value>,
) -> Json<Value> {
    // Check for batch request (array of requests)
    if let Some(requests) = body.as_array() {
        if requests.is_empty() {
            return Json(json!(JsonRpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                "Empty batch request",
                None,
            )));
        }

        // SECURITY (PT-C02): Reject oversized batches to prevent amplification DoS
        if requests.len() > MAX_BATCH_SIZE {
            return Json(json!(JsonRpcResponse::error(
                Value::Null,
                INVALID_REQUEST,
                format!(
                    "Batch too large: {} requests (max {})",
                    requests.len(),
                    MAX_BATCH_SIZE
                ),
                None,
            )));
        }

        let mut responses = Vec::with_capacity(requests.len());
        for req_value in requests {
            let resp = dispatch_single(state.clone(), req_value.clone()).await;
            responses.push(json!(resp));
        }
        return Json(Value::Array(responses));
    }

    // Single request
    let resp = dispatch_single(state, body).await;
    Json(json!(resp))
}

/// Dispatch a single JSON-RPC request to the appropriate handler
async fn dispatch_single(state: Arc<RpcState>, body: Value) -> JsonRpcResponse {
    // Parse the request
    let req: JsonRpcRequest = match serde_json::from_value(body) {
        Ok(r) => r,
        Err(_) => {
            return JsonRpcResponse::error(
                Value::Null,
                PARSE_ERROR,
                "Invalid JSON-RPC request",
                None,
            );
        }
    };

    // Validate jsonrpc version
    if req.jsonrpc != "2.0" {
        return JsonRpcResponse::error(
            req.id,
            INVALID_REQUEST,
            "Only JSON-RPC 2.0 is supported",
            None,
        );
    }

    let id = req.id.clone();
    let params = &req.params;

    // Route to handler based on method name
    match req.method.as_str() {
        // ─── Chain methods ──────────────────────────────────────────
        "chain_getBlock" | "savitri_getBlockByHeight" => {
            rpc_chain_get_block(&state, params, id).await
        }
        "chain_getBlockByNumber" => rpc_chain_get_block(&state, params, id).await,
        "chain_getBlockByHash" => rpc_chain_get_block_by_hash(&state, params, id).await,
        "chain_getLatestBlock" => rpc_chain_get_latest_block(&state, id).await,
        "chain_getBlocks" => rpc_chain_get_blocks(&state, params, id).await,
        "chain_getDagBlockByHash" => rpc_chain_get_dag_block_by_hash(&state, params, id).await,
        "chain_getDagBlocksByHeight" => {
            rpc_chain_get_dag_blocks_by_height(&state, params, id).await
        }
        "chain_getDagGraph" => rpc_chain_get_dag_graph(&state, params, id).await,
        "chain_getDagParents" => rpc_chain_get_dag_parents(&state, params, id).await,
        "chain_getDagChildren" => rpc_chain_get_dag_children(&state, params, id).await,
        "chain_getBlockHeight" | "savitri_blockNumber" => {
            rpc_chain_get_block_height(&state, id).await
        }
        // height. Returns a JSON object {group_id: height, ...}; the empty
        // key "" means legacy single-group lane.
        "chain_getGroupHeights" => rpc_chain_get_group_heights(&state, id).await,
        "chain_getChainInfo" => rpc_chain_get_chain_info(&state, id).await,
        "chain_getGlobalStats" | "savitri_getGlobalStats" | "savitri_globalStats" => {
            rpc_chain_get_global_stats(&state, params, id).await
        }
        "savitri_getBlockHash" => rpc_chain_get_block_hash(&state, params, id).await,
        "savitri_getBlockHashes" => rpc_chain_get_block_hashes(&state, params, id).await,

        // ─── Transaction methods ────────────────────────────────────
        "tx_sendTransaction" | "savitri_sendRawTransaction" | "savitri_sendTransaction" => {
            rpc_tx_send(&state, params, id).await
        }
        "savitri_sendIkarusCompatTransaction" | "tx_sendIkarusCompatTransaction" => {
            rpc_tx_send_ikarus_compat(&state, params, id).await
        }
        "tx_getTransaction" | "savitri_getTransaction" => rpc_tx_get(&state, params, id).await,
        "tx_getTransactionReceipt" => rpc_tx_get_receipt(&state, params, id).await,
        "tx_getTransactionsByBlock" => rpc_tx_get_by_block(&state, params, id).await,
        "tx_getLatestTransactions" => rpc_tx_get_latest(&state, params, id).await,
        "tx_getPendingTransactions" => rpc_tx_get_pending(&state, params, id).await,
        "wallet_getTransactionHistory" => {
            rpc_wallet_get_transaction_history(&state, params, id).await
        }

        // ─── Account methods ────────────────────────────────────────
        "account_getBalance" => rpc_account_get_balance(&state, params, id).await,
        "account_getNonce" => rpc_account_get_nonce(&state, params, id).await,
        "account_getAccount" | "savitri_getAccount" => rpc_account_get(&state, params, id).await,
        "account_getTokenBalance" => rpc_account_get_token_balance(&state, params, id).await,

        // ─── Network methods ────────────────────────────────────────
        "net_version" => rpc_net_version(&state, id).await,
        "net_peerCount" => rpc_net_peer_count(&state, id).await,
        "net_listening" => rpc_net_listening(id).await,
        "net_peers" => rpc_net_peers(&state, id).await,
        "net_nodeInfo" => rpc_net_node_info(&state, id).await,

        // ─── Mempool methods ────────────────────────────────────────
        "mempool_getSize" => rpc_mempool_get_size(&state, id).await,
        "mempool_getStats" => rpc_mempool_get_stats(&state, id).await,
        "mempool_getPendingTransactions" => rpc_tx_get_pending(&state, params, id).await,
        "mempool_getTransactionStatus" => rpc_mempool_get_tx_status(&state, params, id).await,

        // ─── PoU / Consensus methods ────────────────────────────────
        "pou_getValidators" => rpc_pou_get_validators(&state, id).await,
        "pou_getStakeInfo" => rpc_pou_get_stake_info(&state, params, id).await,
        "pou_getEpochInfo" => rpc_pou_get_epoch_info(&state, id).await,
        "pou_getConsensusState" | "savitri_pouLocal" => {
            rpc_pou_get_consensus_state(&state, id).await
        }
        "consensus_getProposer" | "savitri_getProposer" => {
            rpc_consensus_get_proposer(&state, id).await
        }
        "consensus_getShardMap" | "savitri_getShardMap" => {
            rpc_consensus_get_shard_map(&state, id).await
        }
        "consensus_getGroupHeights" | "savitri_getGroupHeights" => {
            rpc_consensus_get_group_heights(&state, id).await
        }
        "savitri_pouPeers" => rpc_pou_peers(&state, id).await,
        "savitri_pouGroups" => rpc_pou_groups(&state, id).await,
        "savitri_pouMasternodes" => rpc_pou_masternodes(&state, id).await,
        "savitri_pouGroupNodes" => rpc_pou_group_nodes(&state, params, id).await,

        // ─── Token methods ──────────────────────────────────────────
        "token_getTokenInfo" => rpc_token_get_info(&state, params, id).await,
        "token_getTokenBalance" => rpc_account_get_token_balance(&state, params, id).await,
        "token_getTokenTransfers" => rpc_token_get_transfers(&state, params, id).await,

        // ─── Utility methods ────────────────────────────────────────
        "savitri_protocolVersion" => rpc_protocol_version(id).await,
        "savitri_syncing" => rpc_syncing(&state, id).await,
        "savitri_gasPrice" => rpc_gas_price(id).await,
        "savitri_estimateGas" => rpc_estimate_gas(id).await,
        "savitri_callContract" => rpc_call_contract(&state, params, id).await,
        "savitri_deployContract" => rpc_deploy_contract(&state, params, id).await,
        "savitri_health" => rpc_health(&state, id).await,
        "dag_getBlocksAtHeight" => rpc_dag_blocks_at_height(&state, params, id).await,
        "dag_getTips" => rpc_dag_tips(&state, id).await,
        "dag_getGroups" => rpc_dag_groups(&state, id).await,
        "savitri_getRewards" => rpc_get_rewards(&state, params, id).await,
        "savitri_getRewardHistory" => rpc_get_reward_history(&state, params, id).await,
        "savitri_faucetClaim" => rpc_faucet_claim(&state, params, id).await,

        // ─── Monolith methods ──────────────────────────────────────
        "savitri_getMonolithHead" => rpc_monolith_get_head(&state, id).await,
        "savitri_getMonolithsForRange" => rpc_monolith_get_range(&state, params, id).await,
        "savitri_getMonolith" => rpc_monolith_get(&state, params, id).await,

        // ─── Unknown method ─────────────────────────────────────────
        _ => JsonRpcResponse::error(
            id,
            METHOD_NOT_FOUND,
            format!("Method '{}' not found", req.method),
            None,
        ),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Individual method handlers
// ═══════════════════════════════════════════════════════════════════════════

// ─── Chain methods ─────────────────────────────────────────────────────────

async fn rpc_chain_get_block(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let height =
        match get_u64_param(params, 0, "height").or_else(|| get_u64_param(params, 0, "number")) {
            Some(h) => h,
            None => {
                return JsonRpcResponse::error(
                    id,
                    INVALID_PARAMS,
                    "Missing required parameter: height (u64)",
                    None,
                );
            }
        };

    match handlers::get_block_by_number(state, height) {
        Ok(block) => JsonRpcResponse::success(id, json!(block)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_block_by_hash(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_block_by_hash(state, &hash) {
        Ok(block) => JsonRpcResponse::success(id, json!(block)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_latest_block(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_latest_block(state) {
        Ok(block) => JsonRpcResponse::success(id, json!(block)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_blocks(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let offset = get_u64_param(params, 0, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 1, "limit").unwrap_or(50);

    match handlers::get_blocks_page(state, offset, limit) {
        Ok(blocks) => JsonRpcResponse::success(id, json!(blocks)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_dag_block_by_hash(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_dag_block_by_hash(state, &hash) {
        Ok(block) => JsonRpcResponse::success(id, json!(block)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_dag_blocks_by_height(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let height =
        match get_u64_param(params, 0, "height").or_else(|| get_u64_param(params, 0, "number")) {
            Some(h) => h,
            None => {
                return JsonRpcResponse::error(
                    id,
                    INVALID_PARAMS,
                    "Missing required parameter: height (u64)",
                    None,
                );
            }
        };
    let offset = get_u64_param(params, 1, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 2, "limit").unwrap_or(50);

    match handlers::get_dag_blocks_by_height(state, height, offset, limit) {
        Ok(blocks) => JsonRpcResponse::success(id, json!(blocks)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_dag_graph(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let offset = get_u64_param(params, 0, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 1, "limit").unwrap_or(200);

    match handlers::get_dag_graph(state, offset, limit) {
        Ok(graph) => JsonRpcResponse::success(id, json!(graph)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_dag_parents(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_dag_parents(state, &hash) {
        Ok(parents) => {
            JsonRpcResponse::success(id, json!({ "hash": hash, "parent_hashes": parents }))
        }
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_dag_children(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_dag_children(state, &hash) {
        Ok(children) => JsonRpcResponse::success(id, json!({ "hash": hash, "children": children })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_block_height(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_block_height(state) {
        Ok(height) => JsonRpcResponse::success(id, json!(height)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_group_heights(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_group_heights(state) {
        Ok(map) => JsonRpcResponse::success(id, json!(map)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_chain_info(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_chain_info(state) {
        Ok(info) => JsonRpcResponse::success(id, json!(info)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_global_stats(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let window_seconds = get_u64_param(params, 0, "window_seconds").unwrap_or(60);
    let max_blocks = get_u64_param(params, 1, "max_blocks").unwrap_or(1_000);
    match handlers::get_global_stats(state, window_seconds, max_blocks) {
        Ok(stats) => JsonRpcResponse::success(id, json!(stats)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_block_hash(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let height = match get_u64_param(params, 0, "height") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: height (u64)",
                None,
            );
        }
    };

    match handlers::get_block_hash(state, height) {
        Ok(hash) => JsonRpcResponse::success(id, json!({ "result": hash })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_chain_get_block_hashes(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let height = match get_u64_param(params, 0, "height") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: height (u64)",
                None,
            );
        }
    };

    match handlers::get_block_hashes(state, height) {
        Ok(hashes) => JsonRpcResponse::success(id, json!({ "height": height, "hashes": hashes })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── Transaction methods ───────────────────────────────────────────────────

async fn rpc_tx_send(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    if let Some(wrapper_value) = extract_ikarus_compat_wrapper(params) {
        return rpc_tx_send_ikarus_compat_value(state, wrapper_value, id);
    }

    let raw_tx_hex = match get_string_param(params, 0, "raw_tx_hex")
        .or_else(|| get_string_param(params, 0, "data"))
    {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: raw_tx_hex (hex string)",
                None,
            );
        }
    };

    match handlers::send_raw_transaction(state, &raw_tx_hex).await {
        Ok(hash) => JsonRpcResponse::success(id, json!({ "result": hash })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

fn extract_ikarus_compat_wrapper(params: &Value) -> Option<Value> {
    let candidate = if let Some(arr) = params.as_array() {
        arr.first().cloned()
    } else if let Some(obj) = params.as_object() {
        obj.get("transaction")
            .or_else(|| obj.get("wrapper"))
            .cloned()
            .or_else(|| Some(Value::Object(obj.clone())))
    } else {
        None
    }?;

    let tx_type = candidate.get("type").and_then(Value::as_str)?;
    (tx_type == "IKARUS_COMPAT").then_some(candidate)
}

async fn rpc_tx_send_ikarus_compat(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let wrapper_value = extract_ikarus_compat_wrapper(params);

    let Some(wrapper_value) = wrapper_value else {
        return JsonRpcResponse::error(
            id,
            INVALID_PARAMS,
            "Missing required parameter: IKARUS_COMPAT wrapper",
            None,
        );
    };

    rpc_tx_send_ikarus_compat_value(state, wrapper_value, id)
}

fn rpc_tx_send_ikarus_compat_value(
    state: &RpcState,
    wrapper_value: Value,
    id: Value,
) -> JsonRpcResponse {
    let request: IkarusCompatTransactionRequest = match serde_json::from_value(wrapper_value) {
        Ok(request) => request,
        Err(e) => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("Invalid IKARUS_COMPAT wrapper: {}", e),
                None,
            );
        }
    };

    match handlers::send_ikarus_compat_transaction(state, request) {
        Ok(response) => JsonRpcResponse::success(id, json!(response)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_tx_get(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_transaction(state, &hash) {
        Ok(tx) => JsonRpcResponse::success(id, json!(tx)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_tx_get_receipt(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    match handlers::get_transaction_receipt(state, &hash) {
        Ok(receipt) => JsonRpcResponse::success(id, json!(receipt)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_tx_get_by_block(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let height = match get_u64_param(params, 0, "height")
        .or_else(|| get_u64_param(params, 0, "block_number"))
    {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: height (u64)",
                None,
            );
        }
    };
    let offset = get_u64_param(params, 1, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 2, "limit").unwrap_or(50);

    match handlers::get_transactions_by_block(state, height, offset, limit) {
        Ok(transactions) => JsonRpcResponse::success(id, json!(transactions)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_tx_get_latest(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let offset = get_u64_param(params, 0, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 1, "limit").unwrap_or(50);

    match handlers::get_latest_transactions(state, offset, limit) {
        Ok(transactions) => JsonRpcResponse::success(id, json!(transactions)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_tx_get_pending(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let offset = get_u64_param(params, 0, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 1, "limit").unwrap_or(50);

    match handlers::get_pending_transactions_async(state, offset, limit).await {
        Ok((total, txs)) => JsonRpcResponse::success(
            id,
            json!({
                "pending_count": total,
                "transactions": txs,
                "offset": offset,
                "limit": limit,
            }),
        ),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_wallet_get_transaction_history(
    state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };
    let offset = get_u64_param(params, 1, "offset")
        .or_else(|| get_u64_param(params, 1, "from"))
        .unwrap_or(0);
    let limit = get_u64_param(params, 2, "limit").unwrap_or(50);

    match handlers::get_wallet_transaction_history(state, &address, offset, limit) {
        Ok(history) => JsonRpcResponse::success(id, json!(history)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── Account methods ───────────────────────────────────────────────────────

async fn rpc_account_get(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };

    match handlers::get_account(state, &address) {
        Ok(acc) => JsonRpcResponse::success(id, json!(acc)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_account_get_balance(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };

    match handlers::get_balance(state, &address) {
        Ok(balance) => JsonRpcResponse::success(id, json!({ "balance": balance })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_account_get_nonce(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };

    match handlers::get_nonce(state, &address) {
        Ok(nonce) => JsonRpcResponse::success(id, json!({ "nonce": nonce })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_account_get_token_balance(
    _state: &RpcState,
    params: &Value,
    id: Value,
) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };
    let token_id = get_string_param(params, 1, "token_id").unwrap_or_else(|| "SAVT".to_string());

    // Token balances are tracked separately from native balances.
    // For the native SAVT token, delegate to account_getBalance.
    if token_id == "SAVT" {
        match handlers::get_balance(_state, &address) {
            Ok(balance) => JsonRpcResponse::success(
                id,
                json!({
                    "address": address.trim_start_matches("0x").to_lowercase(),
                    "token_id": "SAVT",
                    "balance": balance,
                }),
            ),
            Err(e) => rpc_error_to_response(id, e),
        }
    } else {
        // Non-native tokens: not yet implemented in storage
        JsonRpcResponse::success(
            id,
            json!({
                "address": address.trim_start_matches("0x").to_lowercase(),
                "token_id": token_id,
                "balance": "0",
            }),
        )
    }
}

// ─── Network methods ───────────────────────────────────────────────────────

async fn rpc_net_version(_state: &RpcState, id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, json!("savitri-1"))
}

async fn rpc_net_peer_count(state: &RpcState, id: Value) -> JsonRpcResponse {
    if let Some(reader) = state.network_reader.as_ref() {
        let peers = reader.get_connected_peers().await;
        JsonRpcResponse::success(id, json!(peers.len()))
    } else if let Some(reader) = state.pou_reader.as_ref() {
        let peers = reader.get_all_peers().await;
        JsonRpcResponse::success(id, json!(peers.len()))
    } else if let Some(reader) = state.masternode_pou_reader.as_ref() {
        let masternodes = reader.get_masternodes().await;
        JsonRpcResponse::success(id, json!(masternodes.len()))
    } else {
        JsonRpcResponse::success(id, json!(0))
    }
}

async fn rpc_net_listening(id: Value) -> JsonRpcResponse {
    // If the RPC server is running, the node is listening
    JsonRpcResponse::success(id, json!(true))
}

async fn rpc_net_peers(state: &RpcState, id: Value) -> JsonRpcResponse {
    if let Some(reader) = state.network_reader.as_ref() {
        let peers = reader.get_connected_peers().await;
        let peer_list: Vec<serde_json::Value> = peers
            .iter()
            .map(|peer_id| {
                json!({
                    "peer_id": peer_id,
                    "connected": true,
                })
            })
            .collect();
        JsonRpcResponse::success(id, json!(peer_list))
    } else if let Some(reader) = state.pou_reader.as_ref() {
        let peers = reader.get_all_peers().await;
        let peer_list: Vec<serde_json::Value> = peers
            .iter()
            .map(|(peer_id, score)| {
                json!({
                    "peer_id": peer_id,
                    "score": score,
                })
            })
            .collect();
        JsonRpcResponse::success(id, json!(peer_list))
    } else {
        JsonRpcResponse::success(id, json!([]))
    }
}

async fn rpc_net_node_info(state: &RpcState, id: Value) -> JsonRpcResponse {
    let mode = handlers::get_node_mode(state);
    let block_height = match handlers::get_block_height(state) {
        Ok(height) => height,
        Err(e) => return rpc_error_to_response(id, e),
    };
    let peer_count = if let Some(reader) = state.network_reader.as_ref() {
        reader.get_connected_peers().await.len() as u64
    } else if let Some(reader) = state.pou_reader.as_ref() {
        reader.get_all_peers().await.len() as u64
    } else if let Some(reader) = state.masternode_pou_reader.as_ref() {
        reader.get_masternodes().await.len() as u64
    } else {
        0
    };

    let info = crate::types::NodeInfoResponse {
        node_id: String::new(), // Node ID not directly available from RPC state
        protocol_version: "1.0.0".to_string(),
        network: "savitri-1".to_string(),
        listening: true,
        peer_count,
        block_height,
        syncing: false,
        mode: mode.to_string(),
    };
    JsonRpcResponse::success(id, json!(info))
}

// ─── Mempool methods ───────────────────────────────────────────────────────

async fn rpc_mempool_get_size(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_mempool_size_async(state).await {
        Ok(size) => JsonRpcResponse::success(id, json!(size)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_mempool_get_stats(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_mempool_stats_async(state).await {
        Ok(stats) => JsonRpcResponse::success(id, json!(stats)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_mempool_get_tx_status(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let hash = match get_string_param(params, 0, "hash") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: hash (hex string)",
                None,
            );
        }
    };

    // Check if tx is confirmed in storage
    match handlers::get_transaction_receipt(state, &hash) {
        Ok(receipt) => JsonRpcResponse::success(
            id,
            json!({
                "hash": receipt.hash,
                "status": receipt.status,
            }),
        ),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── PoU / Consensus methods ───────────────────────────────────────────────

async fn rpc_pou_get_validators(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_masternodes(state).await {
        Ok(validators) => JsonRpcResponse::success(id, json!(validators)),
        Err(_) => {
            // Fallback: try PoU peers
            if let Some(reader) = state.pou_reader.as_ref() {
                let peers = reader.get_all_peers().await;
                let validators: Vec<serde_json::Value> = peers
                    .iter()
                    .map(|(id, score)| {
                        json!({
                            "node_id": id,
                            "pou_score": *score as f64 / 1000.0,
                            "health_score": 1.0,
                        })
                    })
                    .collect();
                JsonRpcResponse::success(id, json!(validators))
            } else {
                JsonRpcResponse::success(id, json!([]))
            }
        }
    }
}

async fn rpc_pou_get_stake_info(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };

    let is_validator = if let Some(reader) = state.masternode_pou_reader.as_ref() {
        let masternodes = reader.get_masternodes().await;
        masternodes.iter().any(|m| m.node_id == address)
    } else {
        false
    };

    let info = crate::types::StakeInfoResponse {
        address: address.trim_start_matches("0x").to_lowercase(),
        stake_amount: "0".to_string(),
        is_validator,
    };
    JsonRpcResponse::success(id, json!(info))
}

async fn rpc_pou_get_epoch_info(state: &RpcState, id: Value) -> JsonRpcResponse {
    let block_height = match handlers::get_block_height(state) {
        Ok(height) => height,
        Err(e) => return rpc_error_to_response(id, e),
    };

    // Epoch info derived from PoU state if available
    let current_epoch = if let Some(reader) = state.pou_reader.as_ref() {
        let local = reader.get_local().await;
        local.epoch.unwrap_or(0)
    } else {
        0
    };

    let validators_count = if let Some(reader) = state.masternode_pou_reader.as_ref() {
        reader.get_masternodes().await.len() as u64
    } else if let Some(reader) = state.pou_reader.as_ref() {
        reader.get_all_peers().await.len() as u64
    } else {
        0
    };

    let info = crate::types::EpochInfoResponse {
        current_epoch,
        epoch_start_block: 0,
        blocks_in_epoch: block_height,
        validators_count,
    };
    JsonRpcResponse::success(id, json!(info))
}

async fn rpc_pou_get_consensus_state(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_pou_local(state).await {
        Ok(pou) => JsonRpcResponse::success(id, json!(pou)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

/// Return whether this node is currently the elected intra-group proposer.
/// Used by load-test clients to route TX directly to the producing node
/// instead of relying on HaveTx/TxFetch propagation (which in practice
/// delivers < 1% of announced TX to the proposer before TTL eviction).
///
/// Response shape:
/// ```json
/// {
///   "is_proposer": true,
///   "node_id": "12D3KooW...",
///   "group_id": "group_60_2_60"
/// }
/// ```
async fn rpc_consensus_get_proposer(state: &RpcState, id: Value) -> JsonRpcResponse {
    let (is_proposer, node_id, group_id) = match &state.proposer_state {
        Some(reader) => {
            let is_p = reader.is_local_proposer().await;
            let nid = reader.local_node_id();
            let gid = reader.current_group_id().await;
            (is_p, nid, gid)
        }
        None => (false, String::new(), None),
    };
    JsonRpcResponse::success(
        id,
        json!({
            "is_proposer": is_proposer,
            "node_id": node_id,
            "group_id": group_id,
        }),
    )
}

/// Return the full `shard_id → group_id` map this node has accumulated from
/// GroupAnnouncements, plus the total shard count. Used by the benchmark
/// loadtest to compute the (src_group, dst_group) distribution of a
/// workload without round-tripping one RPC per TX.
///
/// Response shape:
/// ```json
/// {
///   "num_shards": 65536,
///   "shard_map": { "0": "group_60_2_60", "1": "group_60_0_60", ... }
/// }
/// ```
async fn rpc_consensus_get_shard_map(state: &RpcState, id: Value) -> JsonRpcResponse {
    let (map, n) = match &state.proposer_state {
        Some(reader) => {
            let m = reader.shard_map().await;
            let n = reader.num_shards().await;
            (m, n)
        }
        None => (std::collections::HashMap::new(), 0),
    };
    // Serialize with string keys so the JSON is compact & consumable by any client.
    let string_map: std::collections::HashMap<String, String> =
        map.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    JsonRpcResponse::success(
        id,
        json!({
            "num_shards": n,
            "shard_map": string_map,
        }),
    )
}

/// `consensus_getGroupHeights` (alias `savitri_getGroupHeights`).
///
/// `savitri_blockNumber` returns a single global max height that hides the
/// real multi-group state of the DAG (architectural_debt.md "DAG appears
/// linear under multi-group"). This endpoint scans CF_METADATA for keys
/// `chain_head:{group_id}` and returns the per-group height map so clients
/// can see when groups diverge or one group is lagging the others.
///
/// Response shape:
/// ```json
/// {
///   "global_max_height": 1234,
///   "groups": {
///     "group_12_0_12": 1234,
///     "group_12_1_12": 1230,
///     "group_12_2_12": 1232
///   },
///   "legacy_chain_head_height": 1234
/// }
/// ```
async fn rpc_consensus_get_group_heights(state: &RpcState, id: Value) -> JsonRpcResponse {
    use savitri_storage::storage::CF_METADATA;
    let storage = match &state.storage {
        Some(s) => s.clone(),
        None => {
            return rpc_error_to_response(id, RpcError::StorageUnavailable);
        }
    };

    // Decode chain_head bytes → height. Two formats coexist (mirrors
    // handlers::recover_chain_head logic):
    //   * Format 1 (lightnode): bincode-serialized BlockWire — has `height` field
    //   * Format 2 (masternode compact, 72 bytes): [hash(64) | height_le(8)]
    fn decode_height(bytes: &[u8]) -> u64 {
        // Try bincode BlockWire first. BlockWire is the canonical lightnode
        // chain_head format. If decode succeeds, take the height field.
        if let Ok(block) = bincode::deserialize::<crate::handlers::BlockWire>(bytes) {
            return block.height;
        }
        // Masternode compact format: [hash(64) | height_le(8)] = 72 bytes.
        if bytes.len() >= 72 {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&bytes[64..72]);
            return u64::from_le_bytes(arr);
        }
        // Unknown format — return 0 rather than parse junk as height.
        0
    }

    // Legacy single-lane chain_head (group_id = "")
    let legacy_height = storage
        .get_cf(CF_METADATA, b"chain_head")
        .ok()
        .flatten()
        .map(|b| decode_height(&b))
        .unwrap_or(0);

    // Per-group chain_head:{gid}
    let mut groups: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    if let Ok(scan) = storage.scan_cf_prefix(CF_METADATA, b"chain_head:", 1024, false) {
        for (key, value) in scan {
            // key is `chain_head:{group_id}` — strip prefix to recover group_id
            if let Some(gid_bytes) = key.strip_prefix(b"chain_head:".as_slice()) {
                if let Ok(gid) = std::str::from_utf8(gid_bytes) {
                    let h = decode_height(&value);
                    groups.insert(gid.to_string(), h);
                }
            }
        }
    }

    let global_max = groups
        .values()
        .copied()
        .max()
        .unwrap_or(0)
        .max(legacy_height);

    JsonRpcResponse::success(
        id,
        json!({
            "global_max_height": global_max,
            "groups": groups,
            "legacy_chain_head_height": legacy_height,
        }),
    )
}

async fn rpc_pou_peers(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_pou_peers(state).await {
        Ok(peers) => JsonRpcResponse::success(id, json!(peers)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_pou_groups(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_pou_groups(state).await {
        Ok(groups) => JsonRpcResponse::success(id, groups),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_pou_masternodes(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_masternodes(state).await {
        Ok(validators) => JsonRpcResponse::success(id, json!({ "masternodes": validators })),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_pou_group_nodes(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let group_id = match get_string_param(params, 0, "group_id") {
        Some(g) => g,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: group_id (string)",
                None,
            );
        }
    };

    match handlers::get_pou_group_nodes(state, &group_id).await {
        Ok(nodes) => JsonRpcResponse::success(id, nodes),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── Token methods ─────────────────────────────────────────────────────────

async fn rpc_token_get_info(_state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let token_id = get_string_param(params, 0, "token_id").unwrap_or_else(|| "SAVT".to_string());

    if token_id == "SAVT" {
        let info = crate::types::TokenInfoResponse {
            token_id: "SAVT".to_string(),
            name: "Savitri Token".to_string(),
            symbol: "SAVT".to_string(),
            decimals: 18,
            total_supply: "1000000000000000000000000000".to_string(), // 1 billion * 10^18
        };
        JsonRpcResponse::success(id, json!(info))
    } else {
        JsonRpcResponse::error(
            id,
            RESOURCE_NOT_FOUND,
            format!("Token '{}' not found", token_id),
            None,
        )
    }
}

async fn rpc_token_get_transfers(_state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let _token_id = get_string_param(params, 0, "token_id").unwrap_or_else(|| "SAVT".to_string());

    // Token transfer history requires a dedicated index; not yet implemented
    JsonRpcResponse::success(id, json!({ "transfers": [] }))
}

// ─── Utility methods ───────────────────────────────────────────────────────

async fn rpc_protocol_version(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::success(id, json!("1.0.0"))
}

async fn rpc_syncing(state: &RpcState, id: Value) -> JsonRpcResponse {
    let current_block = match handlers::get_block_height(state) {
        Ok(height) => height,
        Err(e) => return rpc_error_to_response(id, e),
    };
    let resp = crate::types::SyncingResponse {
        syncing: false,
        current_block,
        highest_block: current_block,
    };
    JsonRpcResponse::success(id, json!(resp))
}

async fn rpc_gas_price(id: Value) -> JsonRpcResponse {
    // Savitri uses fixed fee model; gas price is the minimum fee in smallest unit
    JsonRpcResponse::success(id, json!("1000000000000000000")) // 1 SAVT
}

async fn rpc_estimate_gas(id: Value) -> JsonRpcResponse {
    let resp = crate::types::GasEstimateResponse {
        estimated_gas: 21000,
        gas_price: "1000000000000000000".to_string(),
    };
    JsonRpcResponse::success(id, json!(resp))
}

async fn rpc_call_contract(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let request: crate::CallContractRequest = match serde_json::from_value(params.clone()) {
        Ok(request) => request,
        Err(err) => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("Invalid contract call params: {}", err),
                None,
            );
        }
    };

    match handlers::call_contract(state, request).await {
        Ok(response) => JsonRpcResponse::success(id, json!(response)),
        Err(err) => rpc_error_to_response(id, err),
    }
}

async fn rpc_deploy_contract(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let request: crate::DeployContractRequest = match serde_json::from_value(params.clone()) {
        Ok(request) => request,
        Err(err) => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                format!("Invalid contract deploy params: {}", err),
                None,
            );
        }
    };

    match handlers::deploy_contract(state, request).await {
        Ok(response) => JsonRpcResponse::success(id, json!(response)),
        Err(err) => rpc_error_to_response(id, err),
    }
}

async fn rpc_health(state: &RpcState, id: Value) -> JsonRpcResponse {
    let health = handlers::get_health(state);
    JsonRpcResponse::success(id, json!(health))
}

async fn rpc_faucet_claim(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string)",
                None,
            );
        }
    };

    match handlers::faucet_claim(state, &address).await {
        Ok(val) => JsonRpcResponse::success(id, val),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── Rewards ────────────────────────────────────────────────────────────

async fn rpc_get_rewards(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string, 64 chars)",
                None,
            );
        }
    };

    match handlers::get_rewards(state, &address) {
        Ok(val) => JsonRpcResponse::success(id, val),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── DAG methods ────────────────────────────────────────────────────────

async fn rpc_get_reward_history(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let address = match get_string_param(params, 0, "address") {
        Some(a) => a,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: address (hex string, 64 chars)",
                None,
            );
        }
    };
    let offset = get_u64_param(params, 1, "offset").unwrap_or(0);
    let limit = get_u64_param(params, 2, "limit").unwrap_or(50);

    match handlers::get_reward_history(state, &address, offset, limit) {
        Ok(val) => JsonRpcResponse::success(id, json!(val)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_dag_blocks_at_height(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let height = match params
        .get(0)
        .and_then(|v| v.as_u64())
        .or_else(|| params.get("height").and_then(|v| v.as_u64()))
    {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: height (u64)",
                None,
            );
        }
    };

    match handlers::dag_get_blocks_at_height(state, height).await {
        Ok(val) => JsonRpcResponse::success(id, val),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_dag_tips(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::dag_get_tips(state).await {
        Ok(val) => JsonRpcResponse::success(id, val),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_dag_groups(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::dag_get_groups(state).await {
        Ok(val) => JsonRpcResponse::success(id, val),
        Err(e) => rpc_error_to_response(id, e),
    }
}

// ─── Monolith methods ───────────────────────────────────────────────────

async fn rpc_monolith_get_head(state: &RpcState, id: Value) -> JsonRpcResponse {
    match handlers::get_monolith_head(state) {
        Ok(Some(info)) => JsonRpcResponse::success(id, json!(info)),
        Ok(None) => JsonRpcResponse::success(id, Value::Null),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_monolith_get_range(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let from = match get_u64_param(params, 0, "from") {
        Some(v) => v,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: from (u64)",
                None,
            );
        }
    };
    let to = match get_u64_param(params, 1, "to") {
        Some(v) => v,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: to (u64)",
                None,
            );
        }
    };

    match handlers::get_monoliths_for_range(state, from, to) {
        Ok(list) => JsonRpcResponse::success(id, json!(list)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

async fn rpc_monolith_get(state: &RpcState, params: &Value, id: Value) -> JsonRpcResponse {
    let height = match get_u64_param(params, 0, "height") {
        Some(h) => h,
        None => {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "Missing required parameter: height (u64)",
                None,
            );
        }
    };

    match handlers::get_monolith_by_height(state, height) {
        Ok(monolith) => JsonRpcResponse::success(id, json!(monolith)),
        Err(e) => rpc_error_to_response(id, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde::Serialize;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use savitri_storage::storage::{CF_BLOCKS, CF_METADATA};

    struct DummyMasternodePouReader;

    #[derive(Serialize)]
    struct TestTransactionWire {
        from: String,
        to: String,
        amount: u64,
        nonce: u64,
        fee: Option<u128>,
        data: Option<Vec<u8>>,
        pubkey: Vec<u8>,
        // `serde` only derives `Serialize` for `[T; N]` up to N=32; the
        // 64-byte Ed25519 signature needs `serde_big_array::BigArray`,
        // matching the pattern used by `TestRewardLedgerEntryWire` below.
        #[serde(with = "serde_big_array::BigArray")]
        sig: [u8; 64],
        pre_verified: bool,
    }

    #[derive(Serialize)]
    struct TestRewardLedgerEntryWire {
        #[serde(with = "serde_big_array::BigArray")]
        address: [u8; 32],
        block_height: u64,
        #[serde(with = "serde_big_array::BigArray")]
        block_hash: [u8; 64],
        amount: u128,
        reward_type: String,
        timestamp: u64,
    }

    #[async_trait]
    impl crate::MasternodePouReader for DummyMasternodePouReader {
        async fn get_groups(&self) -> Vec<crate::PouGroupInfo> {
            Vec::new()
        }

        async fn get_masternodes(&self) -> Vec<crate::MasternodePouInfo> {
            Vec::new()
        }

        async fn get_group_nodes(&self, _group_id: &str) -> HashMap<String, u16> {
            HashMap::new()
        }
    }

    fn unique_db_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("{}_{}_{}", prefix, std::process::id(), nanos))
    }

    fn masternode_state_with_height(height: u64) -> RpcState {
        let db_path = unique_db_path("savitri_rpc_mn_height_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        let mut block_hash = [0u8; 64];
        block_hash[0] = 0xAB;

        let block_json = serde_json::json!({
            "height": height,
            "timestamp": 1_700_000_000u64
        });
        storage
            .put_cf(
                CF_BLOCKS,
                &block_hash,
                &serde_json::to_vec(&block_json).expect("serialize block json"),
            )
            .expect("put block");

        // Masternode compact format: [hash(64) | height_le(8)]
        let mut compact_head = Vec::with_capacity(72);
        compact_head.extend_from_slice(&block_hash);
        compact_head.extend_from_slice(&height.to_le_bytes());
        storage
            .put_cf(CF_METADATA, b"chain_head", &compact_head)
            .expect("put chain_head");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    fn masternode_state_with_latest_height_only(height: u64) -> RpcState {
        let db_path = unique_db_path("savitri_rpc_mn_latest_height_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        storage
            .put_cf(CF_METADATA, b"latest_height", &height.to_le_bytes())
            .expect("put latest_height");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    fn masternode_state_with_block_page() -> RpcState {
        let db_path = unique_db_path("savitri_rpc_mn_blocks_page_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        for height in 1u64..=3 {
            let mut hash = [0u8; 64];
            hash[0] = height as u8;
            let block = crate::handlers::BlockWire {
                hash,
                height,
                timestamp: 1_700_000_000 + height,
                ..Default::default()
            };
            storage
                .put_cf(
                    CF_BLOCKS,
                    &height.to_le_bytes(),
                    &bincode::serialize(&block).expect("serialize block"),
                )
                .expect("put block");
        }

        let chain_head = crate::handlers::BlockWire {
            hash: [3u8; 64],
            height: 3,
            timestamp: 1_700_000_003,
            ..Default::default()
        };
        storage
            .put_cf(
                CF_METADATA,
                b"chain_head",
                &bincode::serialize(&chain_head).expect("serialize chain head"),
            )
            .expect("put chain head");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    fn masternode_state_with_wallet_history() -> RpcState {
        let db_path = unique_db_path("savitri_rpc_wallet_history_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        let address = vec![0x11; 32];
        let tx_hash = vec![0xAA; 32];
        let tx_wire = TestTransactionWire {
            from: hex::encode(&address),
            to: hex::encode(vec![0x22; 32]),
            amount: 7,
            nonce: 1,
            fee: Some(2),
            data: None,
            pubkey: Vec::new(),
            sig: [0u8; 64],
            pre_verified: true,
        };
        storage
            .put_cf(
                savitri_storage::storage::CF_TRANSACTIONS,
                &tx_hash,
                &bincode::serialize(&tx_wire).expect("serialize tx wire"),
            )
            .expect("put tx");
        let mut tx_height_key = b"tx_height:".to_vec();
        tx_height_key.extend_from_slice(&tx_hash);
        storage
            .put_cf(CF_METADATA, &tx_height_key, &5u64.to_le_bytes())
            .expect("put tx height");

        let block = crate::handlers::BlockWire {
            hash: [5u8; 64],
            height: 5,
            timestamp: 1_700_000_005,
            ..Default::default()
        };
        storage
            .put_cf(
                CF_BLOCKS,
                &5u64.to_le_bytes(),
                &bincode::serialize(&block).expect("serialize block"),
            )
            .expect("put block");
        storage
            .put_cf(
                CF_METADATA,
                b"chain_head",
                &bincode::serialize(&block).expect("serialize chain head"),
            )
            .expect("put chain head");

        let mut history_key = b"account_history::".to_vec();
        history_key.extend_from_slice(&address);
        history_key.push(b':');
        history_key.extend_from_slice(&5u64.to_be_bytes());
        history_key.push(b':');
        history_key.extend_from_slice(&tx_hash);
        storage
            .put_cf(CF_METADATA, &history_key, b"")
            .expect("put history");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    fn masternode_state_with_latest_transactions() -> RpcState {
        let db_path = unique_db_path("savitri_rpc_latest_transactions_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        let mk_tx = |from: u8, to: u8, amount: u64, nonce: u64| TestTransactionWire {
            from: hex::encode(vec![from; 32]),
            to: hex::encode(vec![to; 32]),
            amount,
            nonce,
            fee: Some(1),
            data: None,
            pubkey: Vec::new(),
            sig: [0u8; 64],
            pre_verified: true,
        };

        let tx_hash_old = vec![0x01; 32];
        let tx_hash_new = vec![0x02; 32];
        let tx_hash_pending = vec![0x03; 32];

        storage
            .put_cf(
                savitri_storage::storage::CF_TRANSACTIONS,
                &tx_hash_old,
                &bincode::serialize(&mk_tx(0x11, 0x21, 10, 1)).expect("serialize tx old"),
            )
            .expect("put tx old");
        storage
            .put_cf(
                savitri_storage::storage::CF_TRANSACTIONS,
                &tx_hash_new,
                &bincode::serialize(&mk_tx(0x12, 0x22, 20, 2)).expect("serialize tx new"),
            )
            .expect("put tx new");
        storage
            .put_cf(
                savitri_storage::storage::CF_TRANSACTIONS,
                &tx_hash_pending,
                &bincode::serialize(&mk_tx(0x13, 0x23, 30, 3)).expect("serialize tx pending"),
            )
            .expect("put tx pending");

        let mut tx_height_key = b"tx_height:".to_vec();
        tx_height_key.extend_from_slice(&tx_hash_old);
        storage
            .put_cf(CF_METADATA, &tx_height_key, &5u64.to_le_bytes())
            .expect("put tx old height");
        let mut tx_height_key = b"tx_height:".to_vec();
        tx_height_key.extend_from_slice(&tx_hash_new);
        storage
            .put_cf(CF_METADATA, &tx_height_key, &7u64.to_le_bytes())
            .expect("put tx new height");

        let block_5 = crate::handlers::BlockWire {
            hash: [5u8; 64],
            height: 5,
            timestamp: 1_700_000_005,
            ..Default::default()
        };
        let block_7 = crate::handlers::BlockWire {
            hash: [7u8; 64],
            height: 7,
            timestamp: 1_700_000_007,
            ..Default::default()
        };
        storage
            .put_cf(
                CF_BLOCKS,
                &5u64.to_le_bytes(),
                &bincode::serialize(&block_5).expect("serialize block 5"),
            )
            .expect("put block 5");
        storage
            .put_cf(
                CF_BLOCKS,
                &7u64.to_le_bytes(),
                &bincode::serialize(&block_7).expect("serialize block 7"),
            )
            .expect("put block 7");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    fn masternode_state_with_reward_history() -> RpcState {
        const GROUP_CHECK_REWARD_PER_BLOCK: u128 = 1_000_000_000_000_000_000;

        let db_path = unique_db_path("savitri_rpc_reward_history_test");
        let storage = savitri_storage::Storage::with_config(savitri_storage::StorageConfig {
            path: db_path.to_string_lossy().to_string(),
            ..Default::default()
        })
        .expect("storage init");

        let address = [0x33; 32];
        let reward_total_key = format!("reward_total::{}", hex::encode(address));
        storage
            .put_cf(
                CF_METADATA,
                reward_total_key.as_bytes(),
                &(GROUP_CHECK_REWARD_PER_BLOCK * 2).to_le_bytes(),
            )
            .expect("put reward total");

        let entry_one = bincode::serialize(&TestRewardLedgerEntryWire {
            address,
            block_height: 9,
            block_hash: [0x09u8; 64],
            amount: GROUP_CHECK_REWARD_PER_BLOCK,
            reward_type: "group_check".to_string(),
            timestamp: 1_700_000_009,
        })
        .expect("serialize reward entry one");
        let entry_two = bincode::serialize(&TestRewardLedgerEntryWire {
            address,
            block_height: 10,
            block_hash: [0x0Au8; 64],
            amount: GROUP_CHECK_REWARD_PER_BLOCK,
            reward_type: "group_check".to_string(),
            timestamp: 1_700_000_010,
        })
        .expect("serialize reward entry two");

        storage
            .put_cf(
                CF_METADATA,
                format!(
                    "reward_history::{}:{:020}:{}",
                    hex::encode(address),
                    9u64,
                    "09".repeat(64)
                )
                .as_bytes(),
                &entry_one,
            )
            .expect("put reward entry one");
        storage
            .put_cf(
                CF_METADATA,
                format!(
                    "reward_history::{}:{:020}:{}",
                    hex::encode(address),
                    10u64,
                    "0a".repeat(64)
                )
                .as_bytes(),
                &entry_two,
            )
            .expect("put reward entry two");

        let storage_arc: Arc<dyn savitri_storage::StorageTrait> = Arc::new(storage);
        RpcState::for_masternode(Arc::new(DummyMasternodePouReader), Some(storage_arc))
    }

    #[tokio::test]
    async fn chain_get_block_height_returns_numeric_result_for_masternode() {
        let state = Arc::new(masternode_state_with_height(42));
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "chain_getBlockHeight",
            "params": []
        });

        let resp = dispatch_single(state, req).await;
        assert_eq!(resp.result, Some(serde_json::json!(42)));
        assert_eq!(resp.error.is_none(), true);
    }

    #[tokio::test]
    async fn savitri_block_number_alias_returns_same_height_for_masternode() {
        let state = Arc::new(masternode_state_with_height(77));
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "savitri_blockNumber",
            "params": []
        });

        let resp = dispatch_single(state, req).await;
        assert_eq!(resp.result, Some(serde_json::json!(77)));
        assert_eq!(resp.error.is_none(), true);
    }

    #[tokio::test]
    async fn syncing_uses_latest_height_fallback_for_masternode() {
        let state = Arc::new(masternode_state_with_latest_height_only(123));
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "savitri_syncing",
            "params": []
        });

        let resp = dispatch_single(state, req).await;
        assert_eq!(
            resp.result,
            Some(serde_json::json!({
                "syncing": false,
                "current_block": 123,
                "highest_block": 123
            }))
        );
        assert!(resp.error.is_none());
    }

    #[tokio::test]
    async fn chain_get_blocks_returns_paginated_recent_blocks() {
        let state = Arc::new(masternode_state_with_block_page());
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "chain_getBlocks",
            "params": { "offset": 0, "limit": 2 }
        });

        let resp = dispatch_single(state, req).await;
        let result = resp.result.expect("result");
        assert_eq!(result["blocks"].as_array().expect("blocks").len(), 2);
        assert_eq!(result["blocks"][0]["height"], 3);
        assert_eq!(result["blocks"][1]["height"], 2);
        assert_eq!(result["has_more"], true);
    }

    #[tokio::test]
    async fn wallet_history_returns_paginated_indexed_transactions() {
        let state = Arc::new(masternode_state_with_wallet_history());
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "wallet_getTransactionHistory",
            "params": {
                "address": format!("0x{}", "11".repeat(32)),
                "offset": 0,
                "limit": 10
            }
        });

        let resp = dispatch_single(state, req).await;
        let result = resp.result.expect("result");
        assert_eq!(
            result["transactions"]
                .as_array()
                .expect("transactions")
                .len(),
            1
        );
        assert_eq!(result["transactions"][0]["amount"], 7);
        assert_eq!(result["transactions"][0]["block_height"], 5);
        assert_eq!(result["has_more"], false);
    }

    #[tokio::test]
    async fn latest_transactions_returns_global_paginated_transactions() {
        let state = Arc::new(masternode_state_with_latest_transactions());
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tx_getLatestTransactions",
            "params": {
                "offset": 0,
                "limit": 2
            }
        });

        let resp = dispatch_single(state.clone(), req).await;
        let result = resp.result.expect("result");
        assert_eq!(
            result["transactions"]
                .as_array()
                .expect("transactions")
                .len(),
            2
        );
        assert_eq!(result["transactions"][0]["block_height"], 7);
        assert_eq!(result["transactions"][1]["block_height"], 5);
        assert_eq!(result["has_more"], true);

        let req_page_2 = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tx_getLatestTransactions",
            "params": {
                "offset": 2,
                "limit": 2
            }
        });
        let resp_page_2 = dispatch_single(state, req_page_2).await;
        let result_page_2 = resp_page_2.result.expect("result page 2");
        assert_eq!(
            result_page_2["transactions"]
                .as_array()
                .expect("transactions page 2")
                .len(),
            1
        );
        assert_eq!(result_page_2["transactions"][0]["status"], "pending");
        assert_eq!(result_page_2["has_more"], false);
    }

    #[tokio::test]
    async fn reward_history_returns_per_block_rewards_and_total() {
        let state = Arc::new(masternode_state_with_reward_history());
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "savitri_getRewardHistory",
            "params": {
                "address": format!("0x{}", "33".repeat(32)),
                "offset": 0,
                "limit": 10
            }
        });

        let resp = dispatch_single(state, req).await;
        let result = resp.result.expect("reward history result");
        assert_eq!(result["total_rewards"], "2000000000000000000");
        assert_eq!(result["group_check_rewards"], "2000000000000000000");
        assert_eq!(result["rewards"].as_array().expect("rewards").len(), 2);
        assert_eq!(result["rewards"][0]["block_height"], 10);
        assert_eq!(result["rewards"][1]["block_height"], 9);
    }
}
