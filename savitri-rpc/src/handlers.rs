//! Internal data-access functions for the Savitri RPC layer.
//!
//! These functions interact with storage and mempool and return typed results.
//! They are called by the JSON-RPC dispatcher in jsonrpc.rs.
//! NO REST/HTTP handler code lives here.

use bincode::Options;
use serde_big_array::BigArray;

use crate::types::*;
use crate::RpcState;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use savitri_mempool::mempool::integration::bytes_to_raw_tx;
use savitri_storage::storage::{CF_BLOCKS, CF_METADATA, CF_TRANSACTIONS};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use tracing::warn;

// ─── Wire formats for bincode deserialization ──────────────────────────────

/// Block wire format for bincode deserialization (matches lightnode Block layout)
#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct BlockWire {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
    pub height: u64,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub state_root: [u8; 32],
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 32],
    #[serde(with = "BigArray")]
    pub proposer: [u8; 32],
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    #[serde(with = "BigArray")]
    pub parent_exec_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub parent_ref_hash: [u8; 64],
    pub version: u32,
}

impl Default for BlockWire {
    fn default() -> Self {
        Self {
            hash: [0u8; 64],
            height: 0,
            timestamp: 0,
            parent_hash: [0u8; 64],
            state_root: [0u8; 32],
            tx_root: [0u8; 32],
            proposer: [0u8; 32],
            signature: [0u8; 64],
            parent_exec_hash: [0u8; 64],
            parent_ref_hash: [0u8; 64],
            version: 0,
        }
    }
}

fn parse_fixed_64_bytes(value: &serde_json::Value) -> Option<[u8; 64]> {
    if let Some(s) = value.as_str() {
        let trimmed = s.strip_prefix("0x").unwrap_or(s);
        let bytes = hex::decode(trimmed).ok()?;
        if bytes.len() != 64 {
            return None;
        }
        let mut out = [0u8; 64];
        out.copy_from_slice(&bytes);
        return Some(out);
    }

    let arr = value.as_array()?;
    if arr.len() != 64 {
        return None;
    }
    let mut out = [0u8; 64];
    for (idx, v) in arr.iter().enumerate() {
        let n = v.as_u64()?;
        if n > u8::MAX as u64 {
            return None;
        }
        out[idx] = n as u8;
    }
    Some(out)
}

fn extract_parent_hash(val: &serde_json::Value) -> Option<[u8; 64]> {
    let direct = [
        val.get("parent_hash"),
        val.get("parentHash"),
        val.get("parent_exec_hash"),
        val.get("parentExecHash"),
        val.get("header").and_then(|h| h.get("parent_hash")),
        val.get("header").and_then(|h| h.get("parentHash")),
        val.get("header").and_then(|h| h.get("parent_exec_hash")),
        val.get("header").and_then(|h| h.get("parentExecHash")),
    ];

    for maybe in direct {
        if let Some(hash) = maybe.and_then(parse_fixed_64_bytes) {
            return Some(hash);
        }
    }

    let parent_hashes = [
        val.get("parent_hashes"),
        val.get("parentHashes"),
        val.get("header").and_then(|h| h.get("parent_hashes")),
        val.get("header").and_then(|h| h.get("parentHashes")),
    ];

    for maybe in parent_hashes {
        let Some(arr) = maybe.and_then(|v| v.as_array()) else {
            continue;
        };
        let Some(first) = arr.first() else {
            continue;
        };
        if let Some(hash) = parse_fixed_64_bytes(first) {
            return Some(hash);
        }
    }

    None
}

/// Parse a masternode JSON block value (BlockMessageWire or LightnodeProposal).
/// Returns `(height, timestamp, parent_hash)` or `None` on failure.
fn parse_mn_block_json(bytes: &[u8]) -> Option<(u64, u64, [u8; 64])> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;

    // BlockMessageWire has header.exec_height, LightnodeProposal has top-level height.
    let height = val
        .get("header")
        .and_then(|h| h.get("exec_height"))
        .and_then(|v| v.as_u64())
        .or_else(|| val.get("height").and_then(|v| v.as_u64()))?;
    if height == 0 {
        return None;
    }

    let timestamp = val.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0);
    let parent_hash = extract_parent_hash(&val).unwrap_or([0u8; 64]);

    Some((height, timestamp, parent_hash))
}

/// Transaction wire format for bincode deserialization
#[derive(serde::Deserialize)]
pub(crate) struct TransactionWire {
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub nonce: u64,
    pub fee: Option<u128>,
    #[allow(dead_code)]
    pub data: Option<Vec<u8>>,
    #[allow(dead_code)]
    pub pubkey: Vec<u8>,
    #[allow(dead_code)]
    #[serde(with = "BigArray")]
    pub sig: [u8; 64],
    #[allow(dead_code)]
    pub pre_verified: bool,
}

/// Account wire format
#[derive(serde::Deserialize)]
pub(crate) struct AccountWire {
    pub balance: u128,
    pub nonce: u64,
    #[allow(dead_code)]
    pub data: Vec<u8>,
}

#[derive(serde::Deserialize)]
struct RewardLedgerEntryWire {
    #[serde(with = "BigArray")]
    address: [u8; 32],
    pub block_height: u64,
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub amount: u128,
    pub reward_type: String,
    pub timestamp: u64,
}

const TX_HEIGHT_PREFIX: &[u8] = b"tx_height:";
const BLOCK_TXS_PREFIX: &[u8] = b"block_txs::";
const ACCOUNT_HISTORY_PREFIX: &[u8] = b"account_history::";
const REWARD_HISTORY_PREFIX: &[u8] = b"reward_history::";
const IKARUS_COMPAT_SAVITRI_DOMAIN: &[u8] = b"SAVITRI-IKARUS-COMPAT-v1";
const IKARUS_COMPAT_IKARUS_TO_SAVITRI_PREFIX: &[u8] = b"ikarus_compat:ikarus_to_savitri:";
const IKARUS_COMPAT_SAVITRI_TO_IKARUS_PREFIX: &[u8] = b"ikarus_compat:savitri_to_ikarus:";
const IKARUS_COMPAT_SENDER_NONCE_PREFIX: &[u8] = b"ikarus_compat:sender_nonce:";
const MAX_PAGE_LIMIT: u64 = 200;
const MAX_TX_FALLBACK_SCAN: usize = 50_000;
const MAX_LATEST_TX_SCAN: usize = 50_000;

fn tx_inclusion_key(tx_hash: &[u8]) -> Vec<u8> {
    let mut key = TX_HEIGHT_PREFIX.to_vec();
    key.extend_from_slice(tx_hash);
    key
}

fn block_txs_prefix(height: u64) -> Vec<u8> {
    let mut key = BLOCK_TXS_PREFIX.to_vec();
    key.extend_from_slice(&height.to_be_bytes());
    key.push(b':');
    key
}

fn account_history_prefix(address: &[u8]) -> Vec<u8> {
    let mut key = ACCOUNT_HISTORY_PREFIX.to_vec();
    key.extend_from_slice(address);
    key.push(b':');
    key
}

fn reward_history_prefix(address_hex: &str) -> Vec<u8> {
    let mut key = REWARD_HISTORY_PREFIX.to_vec();
    key.extend_from_slice(address_hex.as_bytes());
    key.push(b':');
    key
}

fn clamp_page_limit(limit: u64) -> usize {
    limit.clamp(1, MAX_PAGE_LIMIT) as usize
}

fn parse_height_le(bytes: &[u8]) -> Option<u64> {
    if bytes.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Some(u64::from_le_bytes(arr))
    } else {
        None
    }
}

fn deserialize_block(bytes: &[u8]) -> Result<BlockWire, RpcError> {
    bincode::deserialize(bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to deserialize block: {}", e)))
}

fn decode_block_entry(key: &[u8], bytes: &[u8]) -> Option<BlockWire> {
    decode_block_entry_mode(key, bytes, false)
}

fn decode_block_entry_mode(key: &[u8], bytes: &[u8], include_dag_keys: bool) -> Option<BlockWire> {
    if key.len() == 8 {
        // Lightnode format: 8-byte LE height key, bincode-serialized Block value.
        let key_height = u64::from_le_bytes(key.try_into().ok()?);
        let mut block = bincode::deserialize::<BlockWire>(bytes).ok()?;
        // Treat the column-family key as authoritative if the stored height is stale.
        if block.height != key_height {
            block.height = key_height;
        }
        return Some(block);
    }

    // Layout: `<height_le_8> || ':' || <group_id_bytes>` (see
    // savitri-storage/src/storage/mod.rs:48 build_block_key). The bytes
    // payload is the same bincode-serialized BlockWire used by the legacy
    // 8-byte-key path. Without this branch, RPC handlers that iterate
    // CF_BLOCKS or look up a specific height never see the per-group
    // blocks → chain_getBlock(N) returns NotFound even when storage holds
    // 250+ committed blocks under the multi-group composite-key.
    if key.len() > 9 && key[8] == b':' {
        let mut height_arr = [0u8; 8];
        height_arr.copy_from_slice(&key[0..8]);
        let key_height = u64::from_le_bytes(height_arr);
        let mut block = bincode::deserialize::<BlockWire>(bytes).ok()?;
        if block.height != key_height {
            block.height = key_height;
        }
        return Some(block);
    }

    if key.len() == 64 {
        // Masternode format: 64-byte block-hash key, JSON-serialized block value
        // (BlockMessageWire or LightnodeProposal).
        let (height, timestamp, parent_hash) = parse_mn_block_json(bytes)?;
        let mut block = BlockWire::default();
        block.hash.copy_from_slice(key);
        block.height = height;
        block.timestamp = timestamp;
        block.parent_hash = parent_hash;
        return Some(block);
    }

    if include_dag_keys && key.starts_with(b"dag:") && key.len() >= 4 + 8 + 64 {
        // Lightnode DAG format: b"dag:" + <height_le_8> + <hash_64>.
        let mut height_arr = [0u8; 8];
        height_arr.copy_from_slice(&key[4..12]);
        let key_height = u64::from_le_bytes(height_arr);

        let mut key_hash = [0u8; 64];
        key_hash.copy_from_slice(&key[12..76]);

        // Prefer full block from value when available.
        let mut block = bincode::deserialize::<BlockWire>(bytes)
            .ok()
            .unwrap_or_default();
        block.height = key_height;
        block.hash = key_hash;
        return Some(block);
    }

    None
}

fn collect_dag_blocks(
    storage: &dyn savitri_storage::StorageTrait,
) -> Result<Vec<BlockWire>, RpcError> {
    let mut seen = HashSet::new();
    let mut blocks = Vec::new();
    for entry in storage
        .iterator_cf(CF_BLOCKS)
        .map_err(|e| RpcError::Internal(format!("Failed to iterate blocks CF: {}", e)))?
    {
        let (key, value) =
            entry.map_err(|e| RpcError::Internal(format!("Failed to read block entry: {}", e)))?;
        if let Some(block) = decode_block_entry_mode(&key, &value, true) {
            if seen.insert(block.hash) {
                blocks.push(block);
            }
        }
    }
    Ok(blocks)
}

/// Indicates whether chain_head was found in lightnode (bincode) format.
/// The masternode uses a compact [hash(64)|height(8)] format that must not
/// be overwritten with a bincode BlockWire during self-healing.
enum ChainHeadFormat {
    Bincode,
    MasternodeCompact,
    Missing,
}

fn recover_chain_head(
    storage: &dyn savitri_storage::StorageTrait,
) -> Result<Option<BlockWire>, RpcError> {
    let raw_head = storage
        .get_cf(CF_METADATA, b"chain_head")
        .map_err(|e| RpcError::Internal(format!("Failed to read chain_head: {}", e)))?;

    // Try to parse chain_head using multiple encodings:
    //   Format 1 (lightnode): bincode-serialized BlockWire
    //   Format 2 (masternode): [block_hash(64) | height_le(8)] = 72 bytes
    let (metadata_head, head_format) = match raw_head.as_deref() {
        None => (None, ChainHeadFormat::Missing),
        Some(bytes) => {
            // Try bincode first (lightnode format).
            if let Ok(block) = bincode::deserialize::<BlockWire>(bytes) {
                (Some(block), ChainHeadFormat::Bincode)
            } else if bytes.len() >= 72 {
                // Masternode compact format: bytes[0..64]=hash, bytes[64..72]=height_le.
                let mut height_arr = [0u8; 8];
                height_arr.copy_from_slice(&bytes[64..72]);
                let height = u64::from_le_bytes(height_arr);

                let mut hash = [0u8; 64];
                hash.copy_from_slice(&bytes[..64]);

                // Try to load the full block data by hash (masternode keys CF_BLOCKS by hash).
                let block = if let Ok(Some(block_bytes)) = storage.get_cf(CF_BLOCKS, &hash) {
                    decode_block_entry(&hash, &block_bytes).unwrap_or_else(|| {
                        let mut b = BlockWire::default();
                        b.hash = hash;
                        b.height = height;
                        b
                    })
                } else {
                    let mut b = BlockWire::default();
                    b.hash = hash;
                    b.height = height;
                    b
                };

                if height > 0 {
                    (Some(block), ChainHeadFormat::MasternodeCompact)
                } else {
                    warn!("chain_head compact format found but height is 0");
                    (None, ChainHeadFormat::MasternodeCompact)
                }
            } else {
                warn!(
                    len = bytes.len(),
                    "chain_head has unrecognised encoding (not bincode BlockWire, not 72-byte compact)"
                );
                (None, ChainHeadFormat::Missing)
            }
        }
    };

    let mut best_block = metadata_head.clone().filter(|block| block.height > 0);

    let iter = storage
        .iterator_cf(CF_BLOCKS)
        .map_err(|e| RpcError::Internal(format!("Failed to iterate blocks CF: {}", e)))?;

    for entry in iter {
        let (key, value) = entry
            .map_err(|e| RpcError::Internal(format!("Failed to read blocks CF entry: {}", e)))?;

        let Some(block) = decode_block_entry(&key, &value) else {
            continue;
        };

        match best_block.as_ref() {
            Some(current) if current.height >= block.height => {}
            _ => best_block = Some(block),
        }
    }

    let recovered = best_block.or(metadata_head);

    // Self-heal: only update chain_head when it was originally in bincode format
    // (lightnode). Avoid overwriting the masternode's compact [hash|height] format.
    if matches!(head_format, ChainHeadFormat::Bincode) {
        if let Some(block) = recovered.as_ref() {
            let should_heal = storage
                .get_cf(CF_METADATA, b"chain_head")
                .ok()
                .flatten()
                .and_then(|bytes| bincode::deserialize::<BlockWire>(&bytes).ok())
                .map(|existing| existing.height != block.height)
                .unwrap_or(true);

            if should_heal {
                match bincode::serialize(block) {
                    Ok(bytes) => {
                        if let Err(err) = storage.put_cf(CF_METADATA, b"chain_head", &bytes) {
                            warn!(error = %err, height = block.height, "Failed to self-heal metadata chain_head");
                        }
                    }
                    Err(err) => {
                        warn!(error = %err, height = block.height, "Failed to serialize recovered chain_head");
                    }
                }
            }
        }
    }

    Ok(recovered)
}

// ─── Error type for internal handlers ─────────────────────────────────────

/// Internal RPC error codes
#[derive(Debug)]
pub enum RpcError {
    StorageUnavailable,
    MempoolUnavailable,
    NotFound(String),
    BadRequest(String),
    Internal(String),
    NotImplemented(String),
    /// failed and the proposer for this group is a peer in the same group
    /// (post-rotation), so a local-only admit would strand the TX. Maps to
    /// HTTP 503 / JSON-RPC server-error so the client retries with backoff.
    ServiceUnavailable(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::StorageUnavailable => write!(f, "Storage not available"),
            RpcError::MempoolUnavailable => write!(f, "Mempool not available"),
            RpcError::NotFound(msg) => write!(f, "Not found: {}", msg),
            RpcError::BadRequest(msg) => write!(f, "Bad request: {}", msg),
            RpcError::Internal(msg) => write!(f, "Internal error: {}", msg),
            RpcError::NotImplemented(msg) => write!(f, "Not implemented: {}", msg),
            RpcError::ServiceUnavailable(msg) => write!(f, "Service unavailable: {}", msg),
        }
    }
}

// ─── Health ───────────────────────────────────────────────────────────────

pub fn get_health(state: &RpcState) -> HealthResponse {
    let mode = if state.masternode_pou_reader.is_some() {
        "masternode"
    } else if state.storage.is_some() && state.mempool.is_some() {
        "lightnode"
    } else {
        "unknown"
    };
    HealthResponse {
        status: "ok".to_string(),
        service: "savitri-rpc".to_string(),
        mode: mode.to_string(),
    }
}

pub fn get_node_mode(state: &RpcState) -> &'static str {
    if state.masternode_pou_reader.is_some() {
        "masternode"
    } else if state.storage.is_some() && state.mempool.is_some() {
        "lightnode"
    } else {
        "unknown"
    }
}

pub async fn deploy_contract(
    state: &RpcState,
    request: crate::DeployContractRequest,
) -> Result<crate::DeployContractResponse, RpcError> {
    let executor = state
        .contract_executor
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Contract executor not configured".to_string(),
        ))?;

    executor
        .deploy_contract(request)
        .await
        .map_err(|e| RpcError::Internal(format!("Contract deployment failed: {}", e)))
}

pub async fn call_contract(
    state: &RpcState,
    request: crate::CallContractRequest,
) -> Result<crate::CallContractResponse, RpcError> {
    let executor = state
        .contract_executor
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Contract executor not configured".to_string(),
        ))?;

    executor
        .call_contract(request)
        .await
        .map_err(|e| RpcError::Internal(format!("Contract call failed: {}", e)))
}

// ─── Chain methods ────────────────────────────────────────────────────────

/// Get the current block height from chain_head metadata
pub fn get_block_height(state: &RpcState) -> Result<u64, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let recovered_height = recover_chain_head(storage.as_ref())?
        .map(|block| block.height)
        .unwrap_or(0);

    // Compatibility fallback: some deployments persist the finalized tip as
    // metadata/latest_height (u64 LE). Prefer the higher of both sources.
    let latest_height = storage
        .get_cf(CF_METADATA, b"latest_height")
        .map_err(|e| RpcError::Internal(format!("Failed to read latest_height: {}", e)))?
        .and_then(|bytes| {
            if bytes.len() >= 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);

    Ok(recovered_height.max(latest_height))
}

/// the group_id suffix to return the per-group max height. The legacy
/// `chain_getBlockHeight` returns a single global height (max across all
/// groups), which makes `cluster_tps_eval.sh` see only one lane and hides
/// the parallel multi-group throughput. This helper exposes the full
/// per-group view so an evaluator can sum TX across lanes correctly.
///
/// Composite block-key format (set by the proposer commit pipeline):
///   `[height_le_8bytes, b':', group_id_bytes...]`
///
/// Legacy single-group keys are exactly 8 bytes (no `':'` separator); they
/// are reported under the empty-string group key so callers can distinguish.
///
/// Bounded scan: caps at MAX_KEYS to prevent unbounded RPC cost. With the
/// current testnet (height ~4000 × 3 groups = ~12k entries) the cap is
/// generous; bump or paginate if a real chain ever exceeds it.
pub fn get_group_heights(state: &RpcState) -> Result<std::collections::BTreeMap<String, u64>, RpcError> {
    const MAX_KEYS: usize = 200_000;
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let iter = storage
        .iterator_cf(CF_BLOCKS)
        .map_err(|e| RpcError::Internal(format!("iterator_cf(blocks) failed: {}", e)))?;

    let mut out: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut scanned = 0usize;
    for entry in iter {
        scanned += 1;
        if scanned > MAX_KEYS {
            break;
        }
        let (key, _value) = match entry {
            Ok(kv) => kv,
            Err(_) => continue,
        };
        if key.len() < 8 {
            continue;
        }
        let mut h_arr = [0u8; 8];
        h_arr.copy_from_slice(&key[..8]);
        let height = u64::from_le_bytes(h_arr);

        let group_id = if key.len() == 8 {
            String::new() // legacy single-group lane
        } else if key[8] == b':' {
            String::from_utf8_lossy(&key[9..]).into_owned()
        } else {
            // Unknown key shape — likely something other than a block. Skip.
            continue;
        };
        let entry = out.entry(group_id).or_insert(0);
        if height > *entry {
            *entry = height;
        }
    }
    Ok(out)
}

/// Get block by height
/// (key = height_le) first; if missing, falls back to a prefix scan with
/// `height_le || ':'` to find any per-group composite-key block at that
/// height. Returns the FIRST match — for multi-group RPC that's the local
/// group's block when only one group's data is on this node.
fn get_block_bytes_any_group(
    storage: &dyn savitri_storage::StorageTrait,
    height: u64,
) -> Result<Option<Vec<u8>>, RpcError> {
    let legacy_key = height.to_le_bytes();
    if let Some(bytes) = storage
        .get_cf(CF_BLOCKS, &legacy_key)
        .map_err(|e| RpcError::Internal(format!("Failed to read block (legacy): {}", e)))?
    {
        return Ok(Some(bytes));
    }
    // Composite-key fallback: keys are `height_le || ':' || group_id_bytes`.
    // Scan with prefix to catch any group's block at this height.
    let mut prefix = legacy_key.to_vec();
    prefix.push(b':');
    let scan = storage
        .scan_cf_prefix(CF_BLOCKS, &prefix, 1, false)
        .map_err(|e| RpcError::Internal(format!("Failed to prefix-scan blocks: {}", e)))?;
    Ok(scan.into_iter().next().map(|(_, v)| v))
}

pub fn get_block_by_number(state: &RpcState, height: u64) -> Result<BlockResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let block_bytes = get_block_bytes_any_group(storage.as_ref(), height)?
        .ok_or_else(|| RpcError::NotFound(format!("Block at height {}", height)))?;
    let mut block = deserialize_block(&block_bytes)?;
    if block.height != height {
        block.height = height;
    }
    block.parent_hash = resolve_parent_hash(storage.as_ref(), block.height, block.parent_hash);
    let txs = load_block_transactions(storage.as_ref(), block.height);
    Ok(block_wire_to_response_with_txs(&block, txs))
}

/// Get block by hash (hex string)
///
/// SECURITY (PT-C01): Scan is capped at MAX_HASH_SCAN_BLOCKS to prevent
/// O(N) full-chain DoS from requests with non-existent hashes.
pub fn get_block_by_hash(state: &RpcState, hash_hex: &str) -> Result<BlockResponse, RpcError> {
    /// Maximum number of blocks to scan when searching by hash.
    /// Prevents linear-scan DoS (PT-C01). Production should use a
    /// hash→height index in RocksDB instead of scanning.
    const MAX_HASH_SCAN_BLOCKS: u64 = 1000;

    let hash_bytes = hex::decode(hash_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid block hash hex".to_string()))?;

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let height = get_block_height(state)?;
    let scan_start = height.saturating_sub(MAX_HASH_SCAN_BLOCKS);

    for h in (scan_start..=height).rev() {
        let key = h.to_le_bytes();
        // an earlier fix: composite-key aware lookup
        let bytes_opt = get_block_bytes_any_group(storage.as_ref(), h)?;
        if let Some(bytes) = bytes_opt {
            if let Some(bw) = decode_block_entry(&key, &bytes) {
                if bw.hash[..] == hash_bytes[..] {
                    let mut bw = bw;
                    bw.parent_hash =
                        resolve_parent_hash(storage.as_ref(), bw.height, bw.parent_hash);
                    let txs = load_block_transactions(storage.as_ref(), bw.height);
                    return Ok(block_wire_to_response_with_txs(&bw, txs));
                }
            }
        }
    }
    Err(RpcError::NotFound(format!(
        "Block with hash {} not found in last {} blocks",
        hash_hex, MAX_HASH_SCAN_BLOCKS
    )))
}

/// Get the latest block
pub fn get_latest_block(state: &RpcState) -> Result<BlockResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    if let Some(mut block) = recover_chain_head(storage.as_ref())? {
        block.parent_hash = resolve_parent_hash(storage.as_ref(), block.height, block.parent_hash);
        let txs = load_block_transactions(storage.as_ref(), block.height);
        return Ok(block_wire_to_response_with_txs(&block, txs));
    }
    Err(RpcError::NotFound("No blocks in chain".to_string()))
}

/// Get DAG block by hash (scans all block entries, including DAG keys).
pub fn get_dag_block_by_hash(state: &RpcState, hash_hex: &str) -> Result<BlockResponse, RpcError> {
    let hash_bytes = hex::decode(hash_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid block hash hex".to_string()))?;
    if hash_bytes.len() != 64 {
        return Err(RpcError::BadRequest(
            "Invalid block hash length: expected 64 bytes".to_string(),
        ));
    }
    let mut hash_arr = [0u8; 64];
    hash_arr.copy_from_slice(&hash_bytes);

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let blocks = collect_dag_blocks(storage.as_ref())?;
    let mut block = blocks
        .into_iter()
        .find(|b| b.hash == hash_arr)
        .ok_or_else(|| RpcError::NotFound(format!("DAG block {} not found", hash_hex)))?;
    block.parent_hash = resolve_parent_hash(storage.as_ref(), block.height, block.parent_hash);
    let txs = load_block_transactions(storage.as_ref(), block.height);
    Ok(block_wire_to_response_with_txs(&block, txs))
}

/// Get all DAG blocks at a specific height.
pub fn get_dag_blocks_by_height(
    state: &RpcState,
    height: u64,
    offset: u64,
    limit: u64,
) -> Result<DagBlocksByHeightResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let page_limit = clamp_page_limit(limit);

    let mut blocks: Vec<BlockWire> = collect_dag_blocks(storage.as_ref())?
        .into_iter()
        .filter(|b| b.height == height)
        .collect();
    blocks.sort_by(|a, b| a.hash.cmp(&b.hash));
    let total = blocks.len() as u64;

    let page: Vec<BlockResponse> = blocks
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
        .map(|b| {
            let mut b = b;
            b.parent_hash = resolve_parent_hash(storage.as_ref(), b.height, b.parent_hash);
            let tx_count = get_block_transaction_count(storage.as_ref(), b.height).unwrap_or(0);
            block_wire_to_response(&b, tx_count)
        })
        .collect();

    Ok(DagBlocksByHeightResponse {
        height,
        blocks: page,
        total,
    })
}

/// Get all block hashes at a specific height from DAG storage.
pub fn get_block_hashes(state: &RpcState, height: u64) -> Result<Vec<String>, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let mut hashes: Vec<String> = collect_dag_blocks(storage.as_ref())?
        .into_iter()
        .filter(|b| b.height == height)
        .map(|b| format!("0x{}", hex::encode(b.hash)))
        .collect();
    hashes.sort();
    hashes.dedup();
    Ok(hashes)
}

/// Get parent hashes for a DAG block (currently derives from stored block parent_hash).
pub fn get_dag_parents(state: &RpcState, hash_hex: &str) -> Result<Vec<String>, RpcError> {
    let block = get_dag_block_by_hash(state, hash_hex)?;
    Ok(vec![format!("0x{}", block.parent_hash)])
}

/// Get children hashes for a DAG block (blocks that reference it as parent_hash).
pub fn get_dag_children(state: &RpcState, hash_hex: &str) -> Result<Vec<String>, RpcError> {
    let hash_bytes = hex::decode(hash_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid block hash hex".to_string()))?;
    if hash_bytes.len() != 64 {
        return Err(RpcError::BadRequest(
            "Invalid block hash length: expected 64 bytes".to_string(),
        ));
    }
    let target = format!("0x{}", hex::encode(hash_bytes));
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let mut children: Vec<String> = collect_dag_blocks(storage.as_ref())?
        .into_iter()
        .filter(|b| format!("0x{}", hex::encode(b.parent_hash)) == target)
        .map(|b| format!("0x{}", hex::encode(b.hash)))
        .collect();
    children.sort();
    children.dedup();
    Ok(children)
}

/// Get a paginated DAG graph snapshot for visualization.
/// Returns nodes + in-page edges + tips for the selected page.
pub fn get_dag_graph(
    state: &RpcState,
    offset: u64,
    limit: u64,
) -> Result<DagGraphResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let page_limit = clamp_page_limit(limit);

    let mut blocks = collect_dag_blocks(storage.as_ref())?;
    blocks.sort_by(|a, b| b.height.cmp(&a.height).then_with(|| b.hash.cmp(&a.hash)));
    let total = blocks.len() as u64;
    let has_more = blocks.len() > offset as usize + page_limit;

    let page_blocks: Vec<BlockWire> = blocks
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
        .collect();

    let mut nodes = Vec::with_capacity(page_blocks.len());
    let mut node_hashes = HashSet::with_capacity(page_blocks.len());
    let mut min_height: Option<u64> = None;
    let mut max_height: Option<u64> = None;

    for block in &page_blocks {
        let hash = format!("0x{}", hex::encode(block.hash));
        let parent_hash = format!("0x{}", hex::encode(block.parent_hash));

        min_height = Some(min_height.map_or(block.height, |m| m.min(block.height)));
        max_height = Some(max_height.map_or(block.height, |m| m.max(block.height)));
        node_hashes.insert(hash.clone());

        nodes.push(DagGraphNode {
            hash,
            height: block.height,
            timestamp: block.timestamp,
            parent_hash,
        });
    }

    let mut edges = Vec::new();
    let mut inbound_children: HashMap<String, usize> = HashMap::new();
    let zero_hash = format!("0x{}", "00".repeat(64));
    for node in &nodes {
        if node.parent_hash != "0x"
            && node.parent_hash != zero_hash
            && node_hashes.contains(&node.parent_hash)
        {
            edges.push(DagGraphEdge {
                from: node.parent_hash.clone(),
                to: node.hash.clone(),
            });
            *inbound_children
                .entry(node.parent_hash.clone())
                .or_insert(0) += 1;
        }
    }

    let mut tips: Vec<String> = nodes
        .iter()
        .filter(|n| !inbound_children.contains_key(&n.hash))
        .map(|n| n.hash.clone())
        .collect();
    tips.sort();
    tips.dedup();

    Ok(DagGraphResponse {
        nodes,
        edges,
        tips,
        offset,
        limit: page_limit as u64,
        has_more,
        total,
        min_height,
        max_height,
    })
}

/// Get chain info
pub fn get_chain_info(state: &RpcState) -> Result<ChainInfoResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    match recover_chain_head(storage.as_ref())? {
        Some(block) => Ok(ChainInfoResponse {
            chain_id: "savitri-mainnet".to_string(),
            chain_name: "Savitri Network".to_string(),
            block_height: block.height,
            latest_block_hash: hex::encode(block.hash),
            latest_block_timestamp: block.timestamp,
            protocol_version: format!("{}", block.version),
        }),
        None => Ok(ChainInfoResponse {
            chain_id: "savitri-mainnet".to_string(),
            chain_name: "Savitri Network".to_string(),
            block_height: 0,
            latest_block_hash: String::new(),
            latest_block_timestamp: 0,
            protocol_version: "1".to_string(),
        }),
    }
}

/// Get canonical-chain throughput stats for a bounded recent window.
pub fn get_global_stats(
    state: &RpcState,
    window_seconds: u64,
    max_blocks: u64,
) -> Result<GlobalStatsResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let mut blocks = collect_dag_blocks(storage.as_ref())?;
    blocks.sort_by(|a, b| {
        b.height
            .cmp(&a.height)
            .then_with(|| b.timestamp.cmp(&a.timestamp))
    });

    let latest = blocks.first();
    let latest_block_height = latest.map(|b| b.height).unwrap_or(0);
    let latest_block_timestamp = latest.map(|b| b.timestamp).unwrap_or(0);
    let window_start = latest_block_timestamp.saturating_sub(window_seconds);
    let sample_limit = max_blocks.clamp(1, 10_000) as usize;

    let sampled: Vec<BlockWire> = blocks
        .into_iter()
        .filter(|b| latest_block_timestamp == 0 || b.timestamp >= window_start)
        .take(sample_limit)
        .collect();

    let sampled_blocks = sampled.len() as u64;
    let sampled_transactions = sampled
        .iter()
        .map(|b| get_block_transaction_count(storage.as_ref(), b.height).unwrap_or(0))
        .sum::<u64>();

    let elapsed_seconds = if sampled.len() >= 2 {
        let newest = sampled.first().map(|b| b.timestamp).unwrap_or(0);
        let oldest = sampled.last().map(|b| b.timestamp).unwrap_or(newest);
        newest.saturating_sub(oldest).max(1)
    } else {
        window_seconds.max(1)
    };
    let elapsed = elapsed_seconds as f64;

    Ok(GlobalStatsResponse {
        scope: "canonical".to_string(),
        window_seconds,
        latest_block_height,
        latest_block_timestamp,
        sampled_blocks,
        sampled_transactions,
        transactions_per_second: sampled_transactions as f64 / elapsed,
        blocks_per_second: sampled_blocks as f64 / elapsed,
        blocks_per_minute: sampled_blocks as f64 * 60.0 / elapsed,
    })
}

/// Get block hash by height
pub fn get_block_hash(state: &RpcState, height: u64) -> Result<String, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let key = format!("block_hash:{}", height);
    let hash = storage
        .get_cf(CF_METADATA, key.as_bytes())
        .map_err(|e| RpcError::Internal(format!("Failed to read block_hash: {}", e)))?;
    Ok(hash
        .map(|h| format!("0x{}", hex::encode(h)))
        .unwrap_or_else(|| "0x".to_string()))
}

fn resolve_parent_hash(
    storage: &dyn savitri_storage::StorageTrait,
    height: u64,
    current_parent: [u8; 64],
) -> [u8; 64] {
    if current_parent.iter().any(|&b| b != 0) || height == 0 {
        return current_parent;
    }

    let key = format!("block_hash:{}", height.saturating_sub(1));
    let Some(parent_bytes) = storage.get_cf(CF_METADATA, key.as_bytes()).ok().flatten() else {
        return current_parent;
    };

    if parent_bytes.len() != 64 {
        return current_parent;
    }

    let mut out = [0u8; 64];
    out.copy_from_slice(&parent_bytes);
    out
}

/// Get a paginated list of recent blocks without returning the entire chain.
pub fn get_blocks_page(
    state: &RpcState,
    offset: u64,
    limit: u64,
) -> Result<BlockListResponse, RpcError> {
    let page_limit = clamp_page_limit(limit);
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let mut decoded_blocks = Vec::new();
    for entry in storage
        .iterator_cf(CF_BLOCKS)
        .map_err(|e| RpcError::Internal(format!("Failed to iterate blocks CF: {}", e)))?
    {
        let (key, value) =
            entry.map_err(|e| RpcError::Internal(format!("Failed to read block entry: {}", e)))?;
        if let Some(block) = decode_block_entry(&key, &value) {
            decoded_blocks.push(block);
        }
    }
    decoded_blocks.sort_by(|a, b| b.height.cmp(&a.height));

    let has_more = decoded_blocks.len() > offset as usize + page_limit;
    let mut blocks = Vec::with_capacity(page_limit);
    for block in decoded_blocks
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
    {
        let mut block = block;
        block.parent_hash = resolve_parent_hash(storage.as_ref(), block.height, block.parent_hash);
        if block.height > 0 && block.parent_hash.iter().all(|&b| b == 0) {
            continue;
        }
        let transaction_count = get_block_transaction_count(storage.as_ref(), block.height)?;
        blocks.push(block_wire_to_response(&block, transaction_count));
    }

    Ok(BlockListResponse {
        blocks,
        offset,
        limit: page_limit as u64,
        has_more,
    })
}

fn load_transaction_receipt(
    storage: &dyn savitri_storage::StorageTrait,
    tx_hash: &[u8],
) -> Result<TransactionReceiptResponse, RpcError> {
    let tx_bytes = storage
        .get_cf(CF_TRANSACTIONS, tx_hash)
        .map_err(|e| RpcError::Internal(format!("Failed to read transaction: {}", e)))?;
    let tx_bytes = tx_bytes
        .ok_or_else(|| RpcError::NotFound(format!("Transaction {}", hex::encode(tx_hash))))?;
    let tx: TransactionWire = bincode::deserialize(&tx_bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to deserialize tx: {}", e)))?;

    let block_height = storage
        .get_cf(CF_METADATA, &tx_inclusion_key(tx_hash))
        .ok()
        .flatten()
        .and_then(|b| parse_height_le(&b));

    let (block_hash, timestamp) = match block_height {
        Some(h) => {
            let key = h.to_le_bytes();
            match storage.get_cf(CF_BLOCKS, &key).ok().flatten() {
                Some(block_bytes) => match decode_block_entry(&key, &block_bytes) {
                    Some(block) => (Some(hex::encode(block.hash)), Some(block.timestamp)),
                    None => (None, None),
                },
                None => (None, None),
            }
        }
        None => (None, None),
    };

    Ok(TransactionReceiptResponse {
        hash: hex::encode(tx_hash),
        from: tx.from,
        to: tx.to,
        amount: tx.amount,
        fee: tx.fee,
        block_height,
        block_hash,
        timestamp,
        status: if block_height.is_some() {
            "confirmed".to_string()
        } else {
            "pending".to_string()
        },
    })
}

/// Load all transactions for a block by height (for embedding in block response).
/// Returns empty vec if no transactions found; never errors on zero-tx blocks.
fn load_block_transactions(
    storage: &dyn savitri_storage::StorageTrait,
    height: u64,
) -> Vec<TransactionReceiptResponse> {
    let prefix = block_txs_prefix(height);
    let entries = match storage.scan_cf_prefix(CF_METADATA, &prefix, 2001, false) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };

    let mut txs = Vec::with_capacity(entries.len());
    for (key, _) in entries {
        if key.len() <= prefix.len() {
            continue;
        }
        if let Ok(receipt) = load_transaction_receipt(storage, &key[prefix.len()..]) {
            txs.push(receipt);
        }
    }
    txs
}

/// Get paginated transactions for a block. Requires the per-block tx index.
pub fn get_transactions_by_block(
    state: &RpcState,
    height: u64,
    offset: u64,
    limit: u64,
) -> Result<TransactionListResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let page_limit = clamp_page_limit(limit);
    let prefix = block_txs_prefix(height);
    let scan_limit = offset as usize + page_limit + 1;
    let entries = storage
        .scan_cf_prefix(CF_METADATA, &prefix, scan_limit, false)
        .map_err(|e| RpcError::Internal(format!("Failed to scan block tx index: {}", e)))?;

    let mut transactions = Vec::with_capacity(page_limit);
    let mut has_more = entries.len() > offset as usize + page_limit;

    if !entries.is_empty() {
        for (key, _) in entries.into_iter().skip(offset as usize).take(page_limit) {
            if key.len() <= prefix.len() {
                continue;
            }
            transactions.push(load_transaction_receipt(
                storage.as_ref(),
                &key[prefix.len()..],
            )?);
        }
    } else {
        // Compatibility fallback for data committed before the block_txs index existed.
        // This is bounded and opportunistically backfills the block index for matched txs.
        let mut matched_hashes = Vec::new();
        let mut scanned = 0usize;
        for entry in storage
            .iterator_cf(CF_TRANSACTIONS)
            .map_err(|e| RpcError::Internal(format!("Failed to iterate transactions CF: {}", e)))?
        {
            let (tx_hash, _) = match entry {
                Ok(item) => item,
                Err(e) => {
                    return Err(RpcError::Internal(format!(
                        "Failed to read transaction entry: {}",
                        e
                    )))
                }
            };

            scanned += 1;
            if scanned > MAX_TX_FALLBACK_SCAN {
                break;
            }

            let Some(tx_height) = storage
                .get_cf(CF_METADATA, &tx_inclusion_key(&tx_hash))
                .map_err(|e| RpcError::Internal(format!("Failed to read tx inclusion: {}", e)))?
                .and_then(|b| parse_height_le(&b))
            else {
                continue;
            };

            if tx_height == height {
                let mut index_key = prefix.clone();
                index_key.extend_from_slice(&tx_hash);
                let _ = storage.put_cf(CF_METADATA, &index_key, &[]);
                matched_hashes.push(tx_hash);
            }
        }

        matched_hashes.sort();
        has_more = matched_hashes.len() > offset as usize + page_limit;
        for tx_hash in matched_hashes
            .into_iter()
            .skip(offset as usize)
            .take(page_limit)
        {
            transactions.push(load_transaction_receipt(storage.as_ref(), &tx_hash)?);
        }
    }

    Ok(TransactionListResponse {
        transactions,
        offset,
        limit: page_limit as u64,
        has_more,
    })
}

/// Get a global paginated list of latest transactions across all blocks.
///
/// Sorting order:
/// 1) block height DESC (confirmed before pending)
/// 2) timestamp DESC
/// 3) tx hash ASC (stable tie-breaker)
pub fn get_latest_transactions(
    state: &RpcState,
    offset: u64,
    limit: u64,
) -> Result<TransactionListResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let page_limit = clamp_page_limit(limit);

    let mut transactions = Vec::new();
    let mut scanned = 0usize;

    for entry in storage
        .iterator_cf(CF_TRANSACTIONS)
        .map_err(|e| RpcError::Internal(format!("Failed to iterate transactions CF: {}", e)))?
    {
        let (tx_hash, _) = entry
            .map_err(|e| RpcError::Internal(format!("Failed to read transaction entry: {}", e)))?;
        scanned += 1;
        if scanned > MAX_LATEST_TX_SCAN {
            break;
        }
        transactions.push(load_transaction_receipt(storage.as_ref(), &tx_hash)?);
    }

    transactions.sort_by(|a, b| {
        b.block_height
            .unwrap_or(0)
            .cmp(&a.block_height.unwrap_or(0))
            .then_with(|| b.timestamp.unwrap_or(0).cmp(&a.timestamp.unwrap_or(0)))
            .then_with(|| a.hash.cmp(&b.hash))
    });

    let has_more = transactions.len() > offset as usize + page_limit;
    let page = transactions
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
        .collect();

    Ok(TransactionListResponse {
        transactions: page,
        offset,
        limit: page_limit as u64,
        has_more,
    })
}

/// Get paginated wallet history from the account history index.
pub fn get_wallet_transaction_history(
    state: &RpcState,
    address_hex: &str,
    offset: u64,
    limit: u64,
) -> Result<TransactionListResponse, RpcError> {
    let address_clean = address_hex.trim_start_matches("0x").to_lowercase();
    let address = hex::decode(&address_clean)
        .map_err(|_| RpcError::BadRequest("Invalid address hex".to_string()))?;
    if address.len() != 32 {
        return Err(RpcError::BadRequest(
            "Address must be 32 bytes (64 hex chars)".to_string(),
        ));
    }

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let page_limit = clamp_page_limit(limit);
    let prefix = account_history_prefix(&address);
    let scan_limit = offset as usize + page_limit + 1;
    let entries = storage
        .scan_cf_prefix(CF_METADATA, &prefix, scan_limit, true)
        .map_err(|e| RpcError::Internal(format!("Failed to scan account history: {}", e)))?;

    let has_more = entries.len() > offset as usize + page_limit;
    let mut transactions = Vec::with_capacity(page_limit);

    for (key, _) in entries.into_iter().skip(offset as usize).take(page_limit) {
        if key.len() <= prefix.len() + 9 {
            continue;
        }
        let tx_hash = &key[(prefix.len() + 9)..];
        transactions.push(load_transaction_receipt(storage.as_ref(), tx_hash)?);
    }

    Ok(TransactionListResponse {
        transactions,
        offset,
        limit: page_limit as u64,
        has_more,
    })
}

// ─── Transaction methods ──────────────────────────────────────────────────

/// Get transaction by hash
pub fn get_transaction(state: &RpcState, hash_hex: &str) -> Result<TransactionResponse, RpcError> {
    let tx_hash = hex::decode(hash_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid transaction hash hex".to_string()))?;
    if tx_hash.is_empty() || tx_hash.len() > 64 {
        return Err(RpcError::BadRequest(
            "Transaction hash must be 1-64 bytes".to_string(),
        ));
    }
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let tx_bytes = storage
        .get_cf(CF_TRANSACTIONS, &tx_hash)
        .map_err(|e| RpcError::Internal(format!("Failed to read transaction: {}", e)))?;
    let tx_bytes =
        tx_bytes.ok_or_else(|| RpcError::NotFound(format!("Transaction {}", hash_hex)))?;
    let tx: TransactionWire = bincode::deserialize(&tx_bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to deserialize transaction: {}", e)))?;

    let block_height = storage
        .get_cf(CF_METADATA, &tx_inclusion_key(&tx_hash))
        .ok()
        .flatten()
        .and_then(|b| {
            if b.len() == 8 {
                Some(u64::from_le_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            } else {
                None
            }
        });

    let timestamp = block_height.and_then(|h| {
        let key = h.to_le_bytes();
        storage
            .get_cf(CF_BLOCKS, &key)
            .ok()
            .flatten()
            .and_then(|block_bytes| {
                decode_block_entry(&key, &block_bytes).map(|block| block.timestamp)
            })
    });

    Ok(TransactionResponse {
        hash: hex::encode(&tx_hash),
        from: tx.from,
        to: tx.to,
        amount: tx.amount,
        nonce: tx.nonce,
        fee: tx.fee,
        timestamp,
        block_height,
    })
}

/// Get transaction receipt
pub fn get_transaction_receipt(
    state: &RpcState,
    hash_hex: &str,
) -> Result<TransactionReceiptResponse, RpcError> {
    let tx_hash = hex::decode(hash_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid transaction hash hex".to_string()))?;
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    match load_transaction_receipt(storage.as_ref(), &tx_hash) {
        Ok(receipt) => Ok(receipt),
        Err(RpcError::NotFound(_)) => Ok(TransactionReceiptResponse {
            hash: hex::encode(&tx_hash),
            from: String::new(),
            to: String::new(),
            amount: 0,
            fee: None,
            block_height: None,
            block_hash: None,
            timestamp: None,
            status: "not_found".to_string(),
        }),
        Err(e) => Err(e),
    }
}

/// Send a raw transaction (hex-encoded bytes)
///
/// Uses channel-based ingestion when available (no mempool lock contention with block
/// production). Falls back to direct mempool access with semaphore protection.
pub async fn send_raw_transaction(state: &RpcState, raw_tx_hex: &str) -> Result<String, RpcError> {
    let raw_bytes = hex::decode(raw_tx_hex.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest("Invalid hex-encoded transaction".to_string()))?;

    // P1: shard-aware dispatch. If this LN is in group A and the sender's shard
    // belongs to group B, the router decides: forward via gossipsub (legacy
    // best-effort), or hard-reject with an actionable error (Q3=a, default
    //
    // The hard-reject path prevents TX from silently rotting in a wrong-group
    // mempool: under multi-group each sender's committed_nonce diverges per
    // group, so a TX admitted in the wrong group's queued pool never promotes
    // to the main pool and never makes it into a block.
    if let Some(ref router) = state.tx_router {
        match router.route(&raw_bytes) {
            crate::TxRouteDecision::Forwarded { tx_hash } => {
                return Ok(format!("0x{}", hex::encode(tx_hash)));
            }
            crate::TxRouteDecision::Reject {
                tx_hash,
                target_group_id,
                local_group_id,
                shard_id,
            } => {
                return Err(RpcError::BadRequest(format!(
                    "WRONG_GROUP tx_hash=0x{} sender_shard={} belongs_to_group={} local_group={} — resubmit to an RPC endpoint in group {}",
                    hex::encode(tx_hash),
                    shard_id,
                    target_group_id,
                    local_group_id,
                    target_group_id
                )));
            }
            crate::TxRouteDecision::Local | crate::TxRouteDecision::FallbackLocal => {
                // fall through to normal local-admit path
            }
            crate::TxRouteDecision::RetryGossipUnavailable { tx_hash, local_group_id, reason } => {
                // mempool but intra-group gossip publish failed, so the
                // group's elected proposer (a different peer) would never
                // receive it. Surface as retryable error rather than admit
                // and silently lose the TX.
                return Err(RpcError::ServiceUnavailable(format!(
                    "RETRY_GOSSIP_UNAVAILABLE tx_hash=0x{} local_group={} reason={} — retry after brief backoff",
                    hex::encode(tx_hash),
                    local_group_id,
                    reason
                )));
            }
        }
    }

    let raw_tx = bytes_to_raw_tx(raw_bytes, None);

    // Path A: Channel-based (preferred — zero lock contention with block production)
    //
    // channel whose consumer processes one TX at a time through signature
    // within the first second and `try_send` then silently returned
    // `BadRequest("TX queue full, try again later")` for ~99% of TX.
    // `send_timeout(200ms)` lets the consumer make progress while briefly
    // backpressuring the RPC, which is the desired behaviour under burst
    // load. If the queue still can't accept within the window we return the
    // explicit timeout error so clients can back off / retry.
    if let Some(ref channel) = state.tx_channel {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let submission = crate::TxSubmission {
            raw_tx,
            response: resp_tx,
        };
        match tokio::time::timeout(
            std::time::Duration::from_millis(200),
            channel.send(submission),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_closed)) => {
                return Err(RpcError::BadRequest(
                    "TX channel closed (consumer task exited)".to_string(),
                ));
            }
            Err(_elapsed) => {
                return Err(RpcError::BadRequest(
                    "TX queue backpressured (consumer not draining within 200ms)".to_string(),
                ));
            }
        }

        match resp_rx.await {
            Ok(Ok(hash)) => return Ok(format!("0x{}", hex::encode(hash))),
            Ok(Err(e)) => return Err(RpcError::BadRequest(format!("Transaction rejected: {}", e))),
            Err(_) => return Err(RpcError::BadRequest("TX processing timeout".to_string())),
        }
    }

    // Path B: Direct mempool access (fallback when channel not configured)
    let _permit = state.tx_submission_semaphore.try_acquire().map_err(|_| {
        RpcError::BadRequest("TX submission rate exceeded, try again later".to_string())
    })?;

    let mempool = state.mempool.as_ref().ok_or_else(|| {
        tracing::warn!("send_raw_transaction: mempool not available (masternode mode)");
        RpcError::MempoolUnavailable
    })?;

    match mempool.process_single_raw_transaction(raw_tx).await {
        Ok(tx_hash) => Ok(format!("0x{}", hex::encode(tx_hash))),
        Err(e) => Err(RpcError::BadRequest(format!(
            "Transaction rejected: {:?}",
            e
        ))),
    }
}

// ─── Account methods ──────────────────────────────────────────────────────

fn decode_hex_field(field: &str, value: &str) -> Result<Vec<u8>, RpcError> {
    hex::decode(value.trim_start_matches("0x"))
        .map_err(|_| RpcError::BadRequest(format!("{} must be valid hex", field)))
}

fn decode_fixed_32_hex(field: &str, value: &str) -> Result<[u8; 32], RpcError> {
    let bytes = decode_hex_field(field, value)?;
    bytes
        .try_into()
        .map_err(|_| RpcError::BadRequest(format!("{} must be 32 bytes", field)))
}

fn decode_fixed_64_hex(field: &str, value: &str) -> Result<[u8; 64], RpcError> {
    let bytes = decode_hex_field(field, value)?;
    bytes
        .try_into()
        .map_err(|_| RpcError::BadRequest(format!("{} must be 64 bytes", field)))
}

fn ikarus_compat_sender_nonce_key(sender: &[u8; 32]) -> Vec<u8> {
    let mut key = IKARUS_COMPAT_SENDER_NONCE_PREFIX.to_vec();
    key.extend_from_slice(sender);
    key
}

fn ikarus_compat_mapping_key(prefix: &[u8], hash: &[u8; 32]) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(hash);
    key
}

fn parse_stored_nonce(bytes: &[u8]) -> Option<u64> {
    if bytes.len() == 8 {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Some(u64::from_le_bytes(arr))
    } else {
        None
    }
}

fn build_ikarus_compat_signable(
    sender: &[u8; 32],
    nonce: u64,
    ikarus_tx_raw: &[u8],
    ikarus_signable: &[u8],
    ikarus_signature: &[u8; 64],
    ikarus_public_key: &[u8; 32],
) -> Result<Vec<u8>, RpcError> {
    let raw_len = u32::try_from(ikarus_tx_raw.len()).map_err(|_| {
        RpcError::BadRequest("payload.ikarus_tx_raw is too large for canonical layout".to_string())
    })?;
    let signable_len = u32::try_from(ikarus_signable.len()).map_err(|_| {
        RpcError::BadRequest(
            "payload.ikarus_signable is too large for canonical layout".to_string(),
        )
    })?;

    let mut signable = Vec::with_capacity(
        IKARUS_COMPAT_SAVITRI_DOMAIN.len()
            + 32
            + 8
            + 4
            + ikarus_tx_raw.len()
            + 4
            + ikarus_signable.len()
            + 64
            + 32,
    );
    signable.extend_from_slice(IKARUS_COMPAT_SAVITRI_DOMAIN);
    signable.extend_from_slice(sender);
    signable.extend_from_slice(&nonce.to_le_bytes());
    signable.extend_from_slice(&raw_len.to_le_bytes());
    signable.extend_from_slice(ikarus_tx_raw);
    signable.extend_from_slice(&signable_len.to_le_bytes());
    signable.extend_from_slice(ikarus_signable);
    signable.extend_from_slice(ikarus_signature);
    signable.extend_from_slice(ikarus_public_key);
    Ok(signable)
}

fn verify_ed25519_message(public_key: &[u8; 32], message: &[u8], signature: &[u8; 64]) -> bool {
    let Ok(key) = VerifyingKey::from_bytes(public_key) else {
        return false;
    };
    let sig = Signature::from_bytes(signature);
    key.verify(message, &sig).is_ok()
}

fn verify_ed25519_sha256_digest(
    public_key: &[u8; 32],
    signable: &[u8],
    signature: &[u8; 64],
) -> bool {
    let digest = Sha256::digest(signable);
    verify_ed25519_message(public_key, digest.as_slice(), signature)
}

fn current_ikarus_compat_nonce(
    storage: &dyn savitri_storage::StorageTrait,
    sender: &[u8; 32],
) -> Result<u64, RpcError> {
    let nonce_key = ikarus_compat_sender_nonce_key(sender);
    if let Some(bytes) = storage
        .get_cf(CF_METADATA, &nonce_key)
        .map_err(|e| RpcError::Internal(format!("Failed to read Ikarus nonce index: {}", e)))?
    {
        return parse_stored_nonce(&bytes).ok_or_else(|| {
            RpcError::Internal("Corrupt Ikarus compatibility nonce index".to_string())
        });
    }

    let account_bytes = storage
        .get_account(sender)
        .map_err(|e| RpcError::Internal(format!("Failed to read account nonce: {}", e)))?;
    let Some(bytes) = account_bytes else {
        return Ok(0);
    };

    match savitri_core::Account::decode(&bytes) {
        Ok(account) => Ok(account.nonce),
        Err(_) => {
            let account: AccountWire = bincode::deserialize(&bytes).map_err(|e| {
                RpcError::Internal(format!("Failed to deserialize account nonce: {}", e))
            })?;
            Ok(account.nonce)
        }
    }
}

pub fn send_ikarus_compat_transaction(
    state: &RpcState,
    request: IkarusCompatTransactionRequest,
) -> Result<IkarusCompatTransactionResponse, RpcError> {
    if request.version != 1 {
        return Err(RpcError::BadRequest("version must be 1".to_string()));
    }
    if request.tx_type != "IKARUS_COMPAT" {
        return Err(RpcError::BadRequest(
            "type must be IKARUS_COMPAT".to_string(),
        ));
    }

    let sender = decode_fixed_32_hex("sender", &request.sender)?;
    let ikarus_public_key = decode_fixed_32_hex(
        "payload.ikarus_public_key",
        &request.payload.ikarus_public_key,
    )?;
    if sender != ikarus_public_key {
        return Err(RpcError::BadRequest(
            "sender must match payload.ikarus_public_key".to_string(),
        ));
    }

    let ikarus_signature = decode_fixed_64_hex(
        "payload.ikarus_signature",
        &request.payload.ikarus_signature,
    )?;
    let wrapper_signature = decode_fixed_64_hex("signature", &request.signature)?;
    let ikarus_tx_raw = decode_hex_field("payload.ikarus_tx_raw", &request.payload.ikarus_tx_raw)?;
    if ikarus_tx_raw.is_empty() {
        return Err(RpcError::BadRequest(
            "payload.ikarus_tx_raw must not be empty".to_string(),
        ));
    }
    let ikarus_signable =
        decode_hex_field("payload.ikarus_signable", &request.payload.ikarus_signable)?;
    if ikarus_signable.is_empty() {
        return Err(RpcError::BadRequest(
            "payload.ikarus_signable must not be empty".to_string(),
        ));
    }

    if !verify_ed25519_message(&ikarus_public_key, &ikarus_signable, &ikarus_signature) {
        return Err(RpcError::BadRequest(
            "Invalid Ikarus legacy signature".to_string(),
        ));
    }

    let wrapper_signable = build_ikarus_compat_signable(
        &sender,
        request.nonce,
        &ikarus_tx_raw,
        &ikarus_signable,
        &ikarus_signature,
        &ikarus_public_key,
    )?;
    if !verify_ed25519_sha256_digest(&sender, &wrapper_signable, &wrapper_signature) {
        return Err(RpcError::BadRequest(
            "Invalid Savitri wrapper signature".to_string(),
        ));
    }

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let expected_nonce = current_ikarus_compat_nonce(storage.as_ref(), &sender)?;
    if request.nonce != expected_nonce {
        return Err(RpcError::BadRequest(format!(
            "Invalid nonce: expected {}, got {}",
            expected_nonce, request.nonce
        )));
    }

    let savitri_tx_hash: [u8; 32] = Sha256::digest(&wrapper_signable).into();
    // TODO: replace with exact legacy Ikarus hash once test vectors are available.
    let ikarus_tx_hash: [u8; 32] = Sha256::digest(&ikarus_tx_raw).into();

    let wrapper_bytes = serde_json::to_vec(&request)
        .map_err(|e| RpcError::Internal(format!("Failed to encode Ikarus wrapper: {}", e)))?;
    storage
        .put_cf(CF_TRANSACTIONS, &savitri_tx_hash, &wrapper_bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to store Ikarus wrapper: {}", e)))?;
    storage
        .put_cf(
            CF_METADATA,
            &ikarus_compat_mapping_key(IKARUS_COMPAT_IKARUS_TO_SAVITRI_PREFIX, &ikarus_tx_hash),
            &savitri_tx_hash,
        )
        .map_err(|e| RpcError::Internal(format!("Failed to store Ikarus hash index: {}", e)))?;
    storage
        .put_cf(
            CF_METADATA,
            &ikarus_compat_mapping_key(IKARUS_COMPAT_SAVITRI_TO_IKARUS_PREFIX, &savitri_tx_hash),
            &ikarus_tx_hash,
        )
        .map_err(|e| RpcError::Internal(format!("Failed to store Savitri hash index: {}", e)))?;
    storage
        .put_cf(
            CF_METADATA,
            &ikarus_compat_sender_nonce_key(&sender),
            &request
                .nonce
                .checked_add(1)
                .ok_or_else(|| RpcError::BadRequest("nonce overflow".to_string()))?
                .to_le_bytes(),
        )
        .map_err(|e| RpcError::Internal(format!("Failed to store Ikarus nonce index: {}", e)))?;

    Ok(IkarusCompatTransactionResponse {
        status: "accepted".to_string(),
        savitri_tx_hash: format!("0x{}", hex::encode(savitri_tx_hash)),
        ikarus_tx_hash: format!("0x{}", hex::encode(ikarus_tx_hash)),
    })
}

/// Get account info (balance + nonce)
pub fn get_account(state: &RpcState, address_hex: &str) -> Result<AccountResponse, RpcError> {
    let address_clean = address_hex.trim_start_matches("0x").to_lowercase();
    let address = hex::decode(&address_clean)
        .map_err(|_| RpcError::BadRequest("Invalid address hex".to_string()))?;
    if address.len() != 32 {
        return Err(RpcError::BadRequest(
            "Address must be 32 bytes (64 hex chars)".to_string(),
        ));
    }
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let acc_bytes = storage
        .get_account(&address)
        .map_err(|e| RpcError::Internal(format!("Failed to read account: {}", e)))?;
    match acc_bytes {
        Some(bytes) => {
            // Try the core Account::decode first, then fall back to AccountWire
            match savitri_core::Account::decode(&bytes) {
                Ok(acc) => Ok(AccountResponse {
                    address: address_clean,
                    balance: acc.balance.to_string(),
                    nonce: acc.nonce,
                }),
                Err(_) => {
                    // Fallback: try bincode deserialization
                    let acc: AccountWire = bincode::deserialize(&bytes).map_err(|e| {
                        RpcError::Internal(format!("Failed to deserialize account: {}", e))
                    })?;
                    Ok(AccountResponse {
                        address: address_clean,
                        balance: acc.balance.to_string(),
                        nonce: acc.nonce,
                    })
                }
            }
        }
        None => Ok(AccountResponse {
            address: address_clean,
            balance: "0".to_string(),
            nonce: 0,
        }),
    }
}

/// Get account balance
pub fn get_balance(state: &RpcState, address_hex: &str) -> Result<String, RpcError> {
    let acc = get_account(state, address_hex)?;
    Ok(acc.balance)
}

/// Get account nonce
pub fn get_nonce(state: &RpcState, address_hex: &str) -> Result<u64, RpcError> {
    let acc = get_account(state, address_hex)?;
    Ok(acc.nonce)
}

// ─── Mempool methods ──────────────────────────────────────────────────────

/// Get mempool size (async, locks the mempool properly)
pub async fn get_mempool_size_async(state: &RpcState) -> Result<MempoolSizeResponse, RpcError> {
    let mempool = state.mempool.as_ref().ok_or(RpcError::MempoolUnavailable)?;

    let total = mempool.len() as u64;
    Ok(MempoolSizeResponse {
        // Current MempoolPipeline API exposes total size only.
        // Keep backward-compatible shape by reporting everything as pending.
        pending: total,
        queued: 0,
    })
}

/// Get pending transactions from mempool with pagination.
/// Returns (total_pending, transactions_page).
pub async fn get_pending_transactions_async(
    state: &RpcState,
    offset: u64,
    limit: u64,
) -> Result<(u64, Vec<serde_json::Value>), RpcError> {
    let mempool = state.mempool.as_ref().ok_or(RpcError::MempoolUnavailable)?;
    let page_limit = clamp_page_limit(limit);

    let total = mempool.len() as u64;
    // Peek enough to cover offset + limit
    let peek_count = (offset as usize + page_limit).min(total as usize);
    let peeked = mempool.peek_pending(peek_count);

    let txs: Vec<serde_json::Value> = peeked
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
        .map(|tx| {
            serde_json::json!({
                "hash": tx.tx_hash.map(|h| hex::encode(h)).unwrap_or_default(),
                "from": hex::encode(&tx.sender_address),
                "nonce": tx.nonce,
                "fee": tx.fee,
                "gas_limit": tx.gas_limit,
                "status": "pending",
            })
        })
        .collect();

    Ok((total, txs))
}

/// Get mempool stats including admitted/rejected/confirmed counters.
pub async fn get_mempool_stats_async(state: &RpcState) -> Result<MempoolStatsResponse, RpcError> {
    let mempool = state.mempool.as_ref().ok_or(RpcError::MempoolUnavailable)?;

    let snapshot = mempool.stats_snapshot();

    let windows = {
        let mut tracker = state.mempool_stats_tracker.lock().await;
        tracker.observe(&snapshot)
    };

    Ok(MempoolStatsResponse {
        pending: snapshot.pending,
        ready: snapshot.queued,
        total: snapshot.total,
        admitted_total: snapshot.admitted_total,
        cumulative_admitted: snapshot.queued_total,
        rejected_total: snapshot.rejected_total,
        removed_total: snapshot.removed_total,
        evicted_total: snapshot.evicted_total,
        confirmed_total: snapshot.confirmed_total,
        window_1m: MempoolCounterWindowResponse {
            admitted_total: windows.one_minute.admitted_total,
            cumulative_admitted: windows.one_minute.queued_total,
            rejected_total: windows.one_minute.rejected_total,
            removed_total: windows.one_minute.removed_total,
            evicted_total: windows.one_minute.evicted_total,
            confirmed_total: windows.one_minute.confirmed_total,
        },
        window_1h: MempoolCounterWindowResponse {
            admitted_total: windows.one_hour.admitted_total,
            cumulative_admitted: windows.one_hour.queued_total,
            rejected_total: windows.one_hour.rejected_total,
            removed_total: windows.one_hour.removed_total,
            evicted_total: windows.one_hour.evicted_total,
            confirmed_total: windows.one_hour.confirmed_total,
        },
    })
}

// ─── PoU / Consensus methods ──────────────────────────────────────────────

/// Get PoU local state
pub async fn get_pou_local(state: &RpcState) -> Result<PouLocalResponse, RpcError> {
    let reader = state.pou_reader.as_ref().ok_or(RpcError::NotImplemented(
        "PoU reader not configured".to_string(),
    ))?;
    Ok(reader.get_local().await)
}

/// Get PoU peers
pub async fn get_pou_peers(state: &RpcState) -> Result<PouPeersResponse, RpcError> {
    let reader = state.pou_reader.as_ref().ok_or(RpcError::NotImplemented(
        "PoU reader not configured".to_string(),
    ))?;
    let peers = reader.get_all_peers().await;
    Ok(PouPeersResponse { peers })
}

/// Get PoU groups (masternode only)
pub async fn get_pou_groups(state: &RpcState) -> Result<serde_json::Value, RpcError> {
    let reader = state
        .masternode_pou_reader
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Masternode PoU reader not configured".to_string(),
        ))?;
    let groups = reader.get_groups().await;
    Ok(serde_json::json!({ "groups": groups }))
}

/// Get masternodes info (masternode only)
pub async fn get_masternodes(state: &RpcState) -> Result<Vec<ValidatorInfo>, RpcError> {
    let reader = state
        .masternode_pou_reader
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Masternode PoU reader not configured".to_string(),
        ))?;
    let masternodes = reader.get_masternodes().await;
    Ok(masternodes
        .into_iter()
        .map(|m| ValidatorInfo {
            node_id: m.node_id,
            pou_score: m.pou_score,
            health_score: m.health_score,
        })
        .collect())
}

/// Get nodes in a PoU group (masternode only)
pub async fn get_pou_group_nodes(
    state: &RpcState,
    group_id: &str,
) -> Result<serde_json::Value, RpcError> {
    let reader = state
        .masternode_pou_reader
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Masternode PoU reader not configured".to_string(),
        ))?;
    let nodes = reader.get_group_nodes(group_id).await;
    Ok(serde_json::json!({ "group_id": group_id, "nodes": nodes }))
}

// ─── Faucet ───────────────────────────────────────────────────────────────

/// Faucet claim key prefix in CF_METADATA
const FAUCET_CLAIMED_PREFIX: &str = "faucet_claimed:";
/// Rolling window for claim rate limiting: 24 hours
const FAUCET_WINDOW_SECS: u64 = 86400;
/// Maximum number of claims per address inside FAUCET_WINDOW_SECS.
/// Previously 1 claim per 24h — too restrictive for benchmarks/tests that
/// reuse deterministic addresses across runs. 100 claims/24h keeps abuse
/// bounded (same rate limit × 100 in total output) while letting tooling
/// refund addresses without having to rotate keys each run.
const FAUCET_MAX_CLAIMS_PER_WINDOW: usize = 100;

fn faucet_claimed_key(address_hex: &str) -> Vec<u8> {
    let mut key = FAUCET_CLAIMED_PREFIX.as_bytes().to_vec();
    key.extend_from_slice(address_hex.as_bytes());
    key
}

/// Parse the stored claim history value. Two formats supported:
/// - New: comma-separated list of u64 timestamps inside the rolling window
///   e.g. "1776088017,1776088042,1776088067"
/// - Legacy: single u64 timestamp from the old 1-claim-per-24h scheme.
///   Treated as a single-entry list for backward compatibility.
fn parse_claim_history(raw: &[u8]) -> Vec<u64> {
    std::str::from_utf8(raw)
        .ok()
        .map(|s| {
            s.split(',')
                .filter_map(|t| t.trim().parse::<u64>().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn serialize_claim_history(ts_list: &[u64]) -> Vec<u8> {
    ts_list
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

fn current_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Build and sign faucet transaction (TransactionExt format for mempool)
fn build_faucet_tx(
    keypair: &ed25519_dalek::SigningKey,
    from_hex: &str,
    to_hex: &str,
    amount: u64,
    nonce: u64,
    fee: u128,
) -> Vec<u8> {
    use ed25519_dalek::Signer;
    use sha2::Digest;

    let pubkey = keypair.verifying_key().to_bytes();
    // SECURITY (PT-M08): Include domain tag and chain ID to prevent cross-chain replay attacks
    let mut message = Vec::new();
    message.extend_from_slice(b"SAVITRI-TX-v1");
    message.extend_from_slice(b"savitri-mainnet");
    message.extend_from_slice(from_hex.as_bytes());
    message.extend_from_slice(to_hex.as_bytes());
    message.extend_from_slice(&amount.to_le_bytes());
    message.extend_from_slice(&nonce.to_le_bytes());
    message.extend_from_slice(&fee.to_le_bytes());
    let message_hash = sha2::Sha256::digest(&message);
    let sig = keypair.sign(message_hash.as_slice());
    let sig_bytes: [u8; 64] = sig.to_bytes();

    #[derive(serde::Serialize)]
    struct TxWire {
        from: String,
        to: String,
        amount: u64,
        nonce: u64,
        fee: Option<u128>,
        data: Option<Vec<u8>>,
        pubkey: Vec<u8>,
        #[serde(with = "serde_big_array::BigArray")]
        sig: [u8; 64],
        pre_verified: bool,
    }
    let tx = TxWire {
        from: from_hex.to_string(),
        to: to_hex.to_string(),
        amount,
        nonce,
        fee: Some(fee),
        data: None,
        pubkey: pubkey.to_vec(),
        sig: sig_bytes,
        pre_verified: false,
    };
    // Use fixint encoding to match canonical TransactionExt format
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(&tx)
        .unwrap_or_default()
}

/// Execute faucet claim
///
/// SECURITY (PT-C03): Serialized via `state.faucet_lock` to prevent TOCTOU
/// race conditions on claim check and nonce read. Uses SeqCst ordering
/// for the round-robin index (PT-M02).
pub async fn faucet_claim(state: &RpcState, address: &str) -> Result<serde_json::Value, RpcError> {
    let address_hex = address.trim_start_matches("0x").to_lowercase();
    let address_bytes = hex::decode(&address_hex)
        .map_err(|_| RpcError::BadRequest("Address must be 64 hex chars (32 bytes)".to_string()))?;
    if address_bytes.len() != 32 {
        return Err(RpcError::BadRequest(
            "Address must be 32 bytes (64 hex chars)".to_string(),
        ));
    }

    let faucet_config = state
        .faucet_config
        .as_ref()
        .ok_or(RpcError::NotImplemented(
            "Faucet not available on this node".to_string(),
        ))?;

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let mempool = state.mempool.as_ref().ok_or(RpcError::MempoolUnavailable)?;

    // SECURITY (PT-C03): Acquire faucet lock to serialize claim check + nonce read + tx submit.
    // This prevents two concurrent requests for the same address from both passing the claim check,
    // and prevents two requests from reading the same nonce.
    let _faucet_guard = state.faucet_lock.lock().await;

    let claim_key = faucet_claimed_key(&address_hex);
    let now = current_timestamp_secs();
    let window_start = now.saturating_sub(FAUCET_WINDOW_SECS);
    // Load & prune claim history: keep only timestamps within the rolling window.
    let mut recent_claims: Vec<u64> = match storage
        .get_cf(CF_METADATA, &claim_key)
        .map_err(|e| RpcError::Internal(format!("Failed to check claim: {}", e)))?
    {
        Some(raw) => parse_claim_history(&raw)
            .into_iter()
            .filter(|ts| *ts >= window_start)
            .collect(),
        None => Vec::new(),
    };
    if recent_claims.len() >= FAUCET_MAX_CLAIMS_PER_WINDOW {
        // Tell caller when the earliest recorded claim leaves the window so they
        // know how long to wait before the next slot frees up.
        let oldest = *recent_claims.iter().min().unwrap_or(&now);
        let secs_until_free = oldest
            .saturating_add(FAUCET_WINDOW_SECS)
            .saturating_sub(now);
        let hours = secs_until_free / 3600;
        let minutes = (secs_until_free % 3600) / 60;
        return Err(RpcError::BadRequest(format!(
            "Faucet limit reached: {} claims in last 24h; next slot in {}h {}m",
            recent_claims.len(),
            hours,
            minutes
        )));
    }

    // SECURITY (PT-M02): Use SeqCst ordering for correct cross-core visibility
    let idx = state
        .faucet_index
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        % faucet_config.keypairs.len();
    let keypair = &faucet_config.keypairs[idx];
    let from_addr_bytes = keypair.verifying_key().to_bytes();
    let from_addr_hex = hex::encode(from_addr_bytes);

    // Nonce: use the in-memory pending cache, seeded from storage on first
    // use per key. Reading from storage every claim would see stale data
    // (the previous claim's tx is still in mempool, not yet committed) and
    // reuse the same nonce — producing "Duplicate nonce" / "duplicate
    // transaction" failures that were the dominant cause of low funding
    // ratios under load. Incremented on successful mempool admission.
    // The `faucet_lock` held above serializes all accesses within this node.
    let nonce = {
        let mut cache = state.faucet_pending_nonces.lock().await;
        match cache.get(&from_addr_bytes).copied() {
            Some(n) => n,
            None => {
                let storage_nonce = storage
                    .get_account(&from_addr_bytes)
                    .map_err(|e| RpcError::Internal(format!("Failed to read faucet nonce: {}", e)))?
                    .and_then(|b| savitri_core::Account::decode(&b).ok().map(|a| a.nonce))
                    .unwrap_or(0);
                cache.insert(from_addr_bytes, storage_nonce);
                storage_nonce
            }
        }
    };

    let tx_bytes = build_faucet_tx(
        keypair,
        &from_addr_hex,
        &address_hex,
        faucet_config.amount_per_claim,
        nonce,
        faucet_config.fee,
    );

    let raw_tx = bytes_to_raw_tx(tx_bytes, None);
    let tx_hash = mempool
        .process_single_raw_transaction(raw_tx)
        .await
        .map_err(|e| {
            tracing::warn!(error = ?e, "faucet_claim: mempool rejected tx");
            RpcError::BadRequest(format!("Faucet transaction rejected: {:?}", e))
        })?;

    // Bump the pending-nonce cache so the next claim on this faucet key
    // uses nonce+1 (even before the tx commits). Matches the mempool's
    // view: it just accepted a tx with `nonce`, so the next legal nonce
    // for this sender is `nonce+1`.
    {
        let mut cache = state.faucet_pending_nonces.lock().await;
        cache.insert(from_addr_bytes, nonce.saturating_add(1));
    }

    // Record this claim timestamp in the rolling-window history AFTER successful
    // mempool submission (still under faucet_lock). `recent_claims` already has
    // stale entries pruned by the check above.
    recent_claims.push(current_timestamp_secs());
    let encoded = serialize_claim_history(&recent_claims);
    storage
        .put_cf(CF_METADATA, &claim_key, &encoded)
        .map_err(|e| {
            RpcError::Internal(format!(
                "Transaction sent but failed to record claim: {}",
                e
            ))
        })?;

    Ok(serde_json::json!({
        "tx_hash": format!("0x{}", hex::encode(tx_hash)),
        "amount": faucet_config.amount_per_claim.to_string(),
    }))
}

// ─── Rewards ──────────────────────────────────────────────────────────────

/// Get reward info for an address (balance + nonce + reward coins)
pub fn get_rewards(state: &RpcState, address_hex: &str) -> Result<serde_json::Value, RpcError> {
    let address_clean = address_hex.trim_start_matches("0x").to_lowercase();
    let address_bytes: [u8; 32] = hex::decode(&address_clean)
        .map_err(|_| RpcError::BadRequest("Invalid address hex".to_string()))?
        .try_into()
        .map_err(|_| RpcError::BadRequest("Address must be 32 bytes (64 hex chars)".to_string()))?;

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    // Get regular account balance
    let account = storage
        .get_account(&address_bytes)
        .ok()
        .flatten()
        .and_then(|b| savitri_core::Account::decode(&b).ok());

    // Get reward balance from reward_balances CF
    let reward_raw = storage
        .get_cf("reward_balances", &address_bytes)
        .ok()
        .flatten();

    let reward_balance = reward_raw
        .as_ref()
        .and_then(|b| bincode::deserialize::<u128>(b).ok())
        .unwrap_or(0);

    Ok(serde_json::json!({
        "address": address_clean,
        "balance": account.as_ref().map(|a| a.balance.to_string()).unwrap_or_else(|| "0".to_string()),
        "nonce": account.as_ref().map(|a| a.nonce).unwrap_or(0),
        "reward_balance": reward_balance.to_string(),
    }))
}

// ─── DAG methods ──────────────────────────────────────────────────────────

/// Get paginated reward history for an address.
pub fn get_reward_history(
    state: &RpcState,
    address_hex: &str,
    offset: u64,
    limit: u64,
) -> Result<RewardHistoryResponse, RpcError> {
    let address_clean = address_hex.trim_start_matches("0x").to_lowercase();
    let address_bytes: [u8; 32] = hex::decode(&address_clean)
        .map_err(|_| RpcError::BadRequest("Invalid address hex".to_string()))?
        .try_into()
        .map_err(|_| RpcError::BadRequest("Address must be 32 bytes (64 hex chars)".to_string()))?;

    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;
    let account = storage
        .get_account(&address_bytes)
        .ok()
        .flatten()
        .and_then(|b| savitri_core::Account::decode(&b).ok());

    let reward_raw = storage
        .get_cf("reward_balances", &address_bytes)
        .ok()
        .flatten();
    let reward_balance = reward_raw
        .as_ref()
        .and_then(|b| bincode::deserialize::<u128>(b).ok())
        .unwrap_or(0);

    let total_key = format!("reward_total::{}", address_clean);
    let total_rewards = storage
        .get_cf(CF_METADATA, total_key.as_bytes())
        .map_err(|e| RpcError::Internal(format!("Failed to read reward total: {}", e)))?
        .and_then(|bytes| {
            if bytes.len() == 16 {
                let mut arr = [0u8; 16];
                arr.copy_from_slice(&bytes);
                Some(u128::from_le_bytes(arr))
            } else {
                bincode::deserialize::<u128>(&bytes).ok()
            }
        })
        .unwrap_or(0);

    let prefix = reward_history_prefix(&address_clean);
    let entries = storage
        .scan_cf_prefix(CF_METADATA, &prefix, 50_000, false)
        .map_err(|e| RpcError::Internal(format!("Failed to scan reward history: {}", e)))?;

    let mut decoded = Vec::new();
    for (_, value) in entries {
        let Ok(entry) = bincode::deserialize::<RewardLedgerEntryWire>(&value) else {
            continue;
        };
        if entry.address == address_bytes {
            decoded.push(entry);
        }
    }

    decoded.sort_by(|a, b| {
        b.block_height
            .cmp(&a.block_height)
            .then_with(|| b.block_hash.cmp(&a.block_hash))
    });

    let group_check_rewards = decoded
        .iter()
        .filter(|entry| entry.reward_type == "group_check")
        .map(|entry| entry.amount)
        .sum::<u128>();

    let page_limit = clamp_page_limit(limit);
    let has_more = decoded.len() > offset as usize + page_limit;
    let rewards = decoded
        .into_iter()
        .skip(offset as usize)
        .take(page_limit)
        .map(|entry| RewardHistoryEntryResponse {
            block_height: entry.block_height,
            block_hash: format!("0x{}", hex::encode(entry.block_hash)),
            amount: entry.amount.to_string(),
            reward_type: entry.reward_type,
            timestamp: entry.timestamp,
        })
        .collect();

    Ok(RewardHistoryResponse {
        address: address_clean,
        total_rewards: total_rewards.to_string(),
        group_check_rewards: group_check_rewards.to_string(),
        reward_balance: reward_balance.to_string(),
        balance: account
            .as_ref()
            .map(|a| a.balance.to_string())
            .unwrap_or_else(|| "0".to_string()),
        nonce: account.as_ref().map(|a| a.nonce).unwrap_or(0),
        rewards,
        offset,
        limit: page_limit as u64,
        has_more,
    })
}

/// Get all blocks at a specific height in the DAG
pub async fn dag_get_blocks_at_height(
    state: &RpcState,
    height: u64,
) -> Result<serde_json::Value, RpcError> {
    let reader = state.dag_reader.as_ref().ok_or(RpcError::NotImplemented(
        "DAG reader not available on this node".to_string(),
    ))?;
    let blocks = reader.get_blocks_at_height(height).await;
    Ok(serde_json::json!({ "blocks": blocks }))
}

/// Get current DAG tips (latest block per group)
pub async fn dag_get_tips(state: &RpcState) -> Result<serde_json::Value, RpcError> {
    let reader = state.dag_reader.as_ref().ok_or(RpcError::NotImplemented(
        "DAG reader not available on this node".to_string(),
    ))?;
    let tips = reader.get_tips().await;
    Ok(serde_json::json!({ "tips": tips }))
}

/// Get current groups
pub async fn dag_get_groups(state: &RpcState) -> Result<serde_json::Value, RpcError> {
    let reader = state.dag_reader.as_ref().ok_or(RpcError::NotImplemented(
        "DAG reader not available on this node".to_string(),
    ))?;
    let groups = reader.get_groups().await;
    Ok(serde_json::json!({ "groups": groups }))
}

// ─── Monolith methods ─────────────────────────────────────────────────────

/// Column family name for monolith data (matches savitri-storage internal constant)
const CF_MONOLITHS: &str = "monoliths";

/// Column family name for low-level metadata (matches savitri-storage CF_META)
const CF_META_INTERNAL: &str = "meta";

/// Key for monolith head height in CF_META
const META_MONOLITH_HEAD_HEIGHT_KEY: &str = "monolith_head_height";

/// Key for monolith head ID in CF_META
const META_MONOLITH_HEAD_ID_KEY: &str = "monolith_head_id";

/// Height index prefix in CF_MONOLITHS
const MONOLITH_HEIGHT_PREFIX: &[u8] = b"height::";

fn monolith_height_key(height: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(MONOLITH_HEIGHT_PREFIX.len() + 8);
    key.extend_from_slice(MONOLITH_HEIGHT_PREFIX);
    key.extend_from_slice(&height.to_be_bytes());
    key
}

/// Get monolith head (latest monolith info)
pub fn get_monolith_head(state: &RpcState) -> Result<Option<MonolithInfoResponse>, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let height_bytes = storage
        .get_cf(CF_META_INTERNAL, META_MONOLITH_HEAD_HEIGHT_KEY.as_bytes())
        .map_err(|e| RpcError::Internal(format!("Failed to read monolith head height: {}", e)))?;
    let id_bytes = storage
        .get_cf(CF_META_INTERNAL, META_MONOLITH_HEAD_ID_KEY.as_bytes())
        .map_err(|e| RpcError::Internal(format!("Failed to read monolith head id: {}", e)))?;

    match (height_bytes, id_bytes) {
        (Some(hb), Some(ib)) => {
            if hb.len() != 8 || ib.len() != 64 {
                return Err(RpcError::Internal(
                    "Invalid monolith head encoding".to_string(),
                ));
            }

            // Look up the full monolith header by its ID
            match storage.get_cf(CF_MONOLITHS, &ib) {
                Ok(Some(bytes)) => {
                    let header: savitri_core::MonolithHeader = bincode::deserialize(&bytes)
                        .map_err(|e| {
                            RpcError::Internal(format!("Failed to deserialize monolith: {}", e))
                        })?;
                    Ok(Some(MonolithInfoResponse {
                        exec_height: header.exec_height,
                        window_start: header.window_start,
                        epoch_id: header.epoch_id,
                        block_count: header.exec_height.saturating_sub(header.window_start),
                        size_bytes: header.size_bytes,
                        monolith_id: hex::encode(header.monolith_id),
                        produced_at_ms: header.produced_at_ms,
                        cosignature_count: header.cosignatures.len(),
                    }))
                }
                _ => {
                    // Head metadata exists but monolith data is missing — return partial info
                    let height = u64::from_le_bytes(hb[..8].try_into().map_err(|_| {
                        RpcError::Internal("Invalid monolith head height encoding".to_string())
                    })?);
                    Ok(Some(MonolithInfoResponse {
                        exec_height: height,
                        window_start: 0,
                        epoch_id: 0,
                        block_count: 0,
                        size_bytes: 0,
                        monolith_id: hex::encode(&ib),
                        produced_at_ms: 0,
                        cosignature_count: 0,
                    }))
                }
            }
        }
        _ => Ok(None),
    }
}

/// Get monoliths covering a height range
pub fn get_monoliths_for_range(
    state: &RpcState,
    from_height: u64,
    to_height: u64,
) -> Result<Vec<MonolithInfoResponse>, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    if from_height > to_height {
        return Err(RpcError::BadRequest(
            "from_height must be <= to_height".to_string(),
        ));
    }

    // Cap the scan range to avoid excessive lookups
    const MAX_RANGE: u64 = 100_000;
    const MAX_RESULTS: usize = 50;
    let effective_to = from_height.saturating_add(MAX_RANGE).min(to_height);

    let mut results = Vec::new();
    for height in from_height..=effective_to {
        let key = monolith_height_key(height);
        if let Ok(Some(id_bytes)) = storage.get_cf(CF_MONOLITHS, &key) {
            if id_bytes.len() == 64 {
                if let Ok(Some(header_bytes)) = storage.get_cf(CF_MONOLITHS, &id_bytes) {
                    if let Ok(header) =
                        bincode::deserialize::<savitri_core::MonolithHeader>(&header_bytes)
                    {
                        results.push(MonolithInfoResponse {
                            exec_height: header.exec_height,
                            window_start: header.window_start,
                            epoch_id: header.epoch_id,
                            block_count: header.exec_height.saturating_sub(header.window_start),
                            size_bytes: header.size_bytes,
                            monolith_id: hex::encode(header.monolith_id),
                            produced_at_ms: header.produced_at_ms,
                            cosignature_count: header.cosignatures.len(),
                        });
                    }
                }
            }
            if results.len() >= MAX_RESULTS {
                break;
            }
        }
    }

    Ok(results)
}

/// Get a full monolith by exec_height
pub fn get_monolith_by_height(
    state: &RpcState,
    height: u64,
) -> Result<MonolithBlockResponse, RpcError> {
    let storage = state.storage.as_ref().ok_or(RpcError::StorageUnavailable)?;

    let key = monolith_height_key(height);
    let id_bytes = storage
        .get_cf(CF_MONOLITHS, &key)
        .map_err(|e| RpcError::Internal(format!("Failed to read monolith index: {}", e)))?
        .ok_or_else(|| RpcError::NotFound(format!("No monolith at height {}", height)))?;

    if id_bytes.len() != 64 {
        return Err(RpcError::Internal(
            "Invalid monolith id in height index".to_string(),
        ));
    }

    let header_bytes = storage
        .get_cf(CF_MONOLITHS, &id_bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to read monolith: {}", e)))?
        .ok_or_else(|| {
            RpcError::NotFound(format!("Monolith data missing for height {}", height))
        })?;

    let header: savitri_core::MonolithHeader = bincode::deserialize(&header_bytes)
        .map_err(|e| RpcError::Internal(format!("Failed to deserialize monolith: {}", e)))?;

    let header_json = serde_json::to_value(&header)
        .map_err(|e| RpcError::Internal(format!("Failed to serialize monolith header: {}", e)))?;

    Ok(MonolithBlockResponse {
        header: header_json,
        start_height: header.window_start,
        end_height: header.exec_height,
        block_count: header.exec_height.saturating_sub(header.window_start),
        total_transactions: 0, // Not tracked in MonolithHeader
        created_at: header.produced_at_ms,
        creator_id: hex::encode(header.producer),
    })
}

// ─── Helpers ──────────────────────────────────────────────────────────────

fn get_block_transaction_count(
    storage: &dyn savitri_storage::StorageTrait,
    height: u64,
) -> Result<u64, RpcError> {
    let prefix = block_txs_prefix(height);
    let indexed_entries = storage
        .scan_cf_prefix(CF_METADATA, &prefix, usize::MAX, false)
        .map_err(|e| RpcError::Internal(format!("Failed to scan block tx index: {}", e)))?;

    if !indexed_entries.is_empty() {
        return Ok(indexed_entries.len() as u64);
    }

    // Compatibility fallback for data committed before the block_txs index existed.
    // Scan is bounded to avoid unbounded full-table iteration.
    let mut count = 0u64;
    let mut scanned = 0usize;
    for entry in storage
        .iterator_cf(CF_TRANSACTIONS)
        .map_err(|e| RpcError::Internal(format!("Failed to iterate transactions CF: {}", e)))?
    {
        let (tx_hash, _) = match entry {
            Ok(item) => item,
            Err(e) => {
                return Err(RpcError::Internal(format!(
                    "Failed to read transaction entry: {}",
                    e
                )))
            }
        };

        scanned += 1;
        if scanned > MAX_TX_FALLBACK_SCAN {
            break;
        }

        let Some(tx_height) = storage
            .get_cf(CF_METADATA, &tx_inclusion_key(&tx_hash))
            .map_err(|e| RpcError::Internal(format!("Failed to read tx inclusion: {}", e)))?
            .and_then(|b| parse_height_le(&b))
        else {
            continue;
        };

        if tx_height == height {
            count += 1;
            let mut index_key = prefix.clone();
            index_key.extend_from_slice(&tx_hash);
            let _ = storage.put_cf(CF_METADATA, &index_key, &[]);
        }
    }

    Ok(count)
}

fn block_wire_to_response(block: &BlockWire, transaction_count: u64) -> BlockResponse {
    BlockResponse {
        hash: hex::encode(block.hash),
        height: block.height,
        timestamp: block.timestamp,
        parent_hash: hex::encode(block.parent_hash),
        parent_hashes: Some(vec![hex::encode(block.parent_hash)]),
        state_root: hex::encode(block.state_root),
        tx_root: hex::encode(block.tx_root),
        proposer: hex::encode(block.proposer),
        version: block.version,
        transaction_count,
        transactions: None,
    }
}

fn block_wire_to_response_with_txs(
    block: &BlockWire,
    transactions: Vec<TransactionReceiptResponse>,
) -> BlockResponse {
    let count = transactions.len() as u64;
    BlockResponse {
        hash: hex::encode(block.hash),
        height: block.height,
        timestamp: block.timestamp,
        parent_hash: hex::encode(block.parent_hash),
        parent_hashes: Some(vec![hex::encode(block.parent_hash)]),
        state_root: hex::encode(block.state_root),
        tx_root: hex::encode(block.tx_root),
        proposer: hex::encode(block.proposer),
        version: block.version,
        transaction_count: count,
        transactions: Some(transactions),
    }
}
