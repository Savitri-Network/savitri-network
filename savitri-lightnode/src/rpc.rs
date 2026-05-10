//! RPC integration for Savitri Lightnode
//!
//! Provides PouReader implementation for savitri-rpc endpoints.

#![cfg(feature = "rpc")]

use async_trait::async_trait;
use libp2p::PeerId;
use savitri_rpc::{NetworkReader, PouLocalResponse, PouReader, ProposerStateReader};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::p2p::PouState;

/// Adapter that exposes the lightnode's proposer state via the
/// `consensus_getProposer` RPC. Holds three weak-ish handles:
///  * `is_intragroup_proposer` — the `Arc<RwLock<bool>>` toggled by
///    `start_proposer_duties` / `Block production loop exited`.
///  * `group_manager` — for the current group-id lookup.
///  * `local_peer_id` — string form of the libp2p peer id.
pub struct LightnodeProposerState {
    is_intragroup_proposer: Arc<RwLock<bool>>,
    group_manager: Arc<crate::p2p::group_manager::P2PGroupManager>,
    local_peer_id: String,
    shard_to_group: Arc<tokio::sync::RwLock<HashMap<u32, String>>>,
    num_shards: Arc<std::sync::atomic::AtomicU32>,
}

impl std::fmt::Debug for LightnodeProposerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LightnodeProposerState")
            .field("local_peer_id", &self.local_peer_id)
            .finish()
    }
}

impl LightnodeProposerState {
    pub fn new(
        is_intragroup_proposer: Arc<RwLock<bool>>,
        group_manager: Arc<crate::p2p::group_manager::P2PGroupManager>,
        local_peer_id: String,
        shard_to_group: Arc<tokio::sync::RwLock<HashMap<u32, String>>>,
        num_shards: Arc<std::sync::atomic::AtomicU32>,
    ) -> Self {
        Self {
            is_intragroup_proposer,
            group_manager,
            local_peer_id,
            shard_to_group,
            num_shards,
        }
    }
}

#[async_trait]
impl ProposerStateReader for LightnodeProposerState {
    async fn is_local_proposer(&self) -> bool {
        *self.is_intragroup_proposer.read().await
    }

    fn local_node_id(&self) -> String {
        self.local_peer_id.clone()
    }

    async fn current_group_id(&self) -> Option<String> {
        self.group_manager
            .get_current_group_cached()
            .map(|g| g.group_id)
    }

    async fn shard_map(&self) -> HashMap<u32, String> {
        self.shard_to_group.read().await.clone()
    }

    async fn num_shards(&self) -> u32 {
        self.num_shards.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Adapter that implements PouReader using lightnode's PouState
pub struct LightnodePouReader {
    pou_state: Arc<RwLock<PouState>>,
}

impl LightnodePouReader {
    pub fn new(pou_state: Arc<RwLock<PouState>>) -> Self {
        Self { pou_state }
    }
}

pub struct LightnodeNetworkReader {
    connected_peers: Arc<RwLock<HashSet<PeerId>>>,
}

impl LightnodeNetworkReader {
    pub fn new(connected_peers: Arc<RwLock<HashSet<PeerId>>>) -> Self {
        Self { connected_peers }
    }
}

#[async_trait]
impl NetworkReader for LightnodeNetworkReader {
    async fn get_connected_peers(&self) -> Vec<String> {
        let peers = self.connected_peers.read().await;
        let mut peers: Vec<String> = peers.iter().map(ToString::to_string).collect();
        peers.sort();
        peers
    }
}

#[async_trait]
impl PouReader for LightnodePouReader {
    async fn get_local(&self) -> PouLocalResponse {
        let state = self.pou_state.read().await;
        let view = state.snapshot().await;
        PouLocalResponse {
            local_score: view.local_score,
            leader: view.leader.map(|p| p.to_string()),
            leader_score: view.leader_score,
            epoch: view.epoch,
            local_is_leader: view.local_is_leader,
            election_ready: view.election_ready,
        }
    }

    async fn get_all_peers(&self) -> HashMap<String, u16> {
        let state = self.pou_state.read().await;
        let scores = state.get_all_peer_scores().await;
        scores
            .into_iter()
            .map(|(peer_id, score)| (peer_id.to_string(), score))
            .collect()
    }
}

#[cfg(feature = "contracts")]
pub struct LightnodeContractExecutorImpl {
    storage: Arc<savitri_storage::Storage>,
}

#[cfg(feature = "contracts")]
impl LightnodeContractExecutorImpl {
    pub fn new(storage: Arc<savitri_storage::Storage>) -> Self {
        Self { storage }
    }
}

#[cfg(feature = "contracts")]
#[async_trait]
impl savitri_rpc::ContractExecutor for LightnodeContractExecutorImpl {
    async fn deploy_contract(
        &self,
        request: savitri_rpc::DeployContractRequest,
    ) -> anyhow::Result<savitri_rpc::DeployContractResponse> {
        let deployer_hex = request.deployer.trim_start_matches("0x");
        let deployer = hex::decode(deployer_hex)
            .map_err(|e| anyhow::anyhow!("invalid deployer hex: {}", e))?;

        if deployer.len() != 32 {
            anyhow::bail!("deployer must be 32 bytes");
        }

        let bytecode = hex::decode(request.bytecode_hex.trim_start_matches("0x"))
            .map_err(|e| anyhow::anyhow!("invalid bytecode hex: {}", e))?;
        let constructor_args = match request.constructor_args_hex {
            Some(args_hex) => hex::decode(args_hex.trim_start_matches("0x"))
                .map_err(|e| anyhow::anyhow!("invalid constructor args hex: {}", e))?,
            None => Vec::new(),
        };

        let block_timestamp = request.block_timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });
        let gas_limit = request.gas_limit.unwrap_or(10_000_000);

        let contract_address = crate::contract_executor::execute_deploy(
            self.storage.as_ref(),
            &deployer,
            bytecode,
            constructor_args,
            request.nonce,
            block_timestamp,
            gas_limit,
        )?;

        Ok(savitri_rpc::DeployContractResponse {
            contract_address: format!("0x{}", hex::encode(contract_address)),
        })
    }

    async fn call_contract(
        &self,
        request: savitri_rpc::CallContractRequest,
    ) -> anyhow::Result<savitri_rpc::CallContractResponse> {
        let contract_hex = request.contract_address.trim_start_matches("0x");
        let contract_address = hex::decode(contract_hex)
            .map_err(|e| anyhow::anyhow!("invalid contract address hex: {}", e))?;
        if contract_address.len() != 32 {
            anyhow::bail!("contract_address must be 32 bytes");
        }

        let caller_hex = request.caller.trim_start_matches("0x");
        let caller =
            hex::decode(caller_hex).map_err(|e| anyhow::anyhow!("invalid caller hex: {}", e))?;
        if caller.len() != 32 {
            anyhow::bail!("caller must be 32 bytes");
        }

        let function_selector = if let Some(signature) = request.function_signature.as_ref() {
            savitri_contracts::CallTransaction::calculate_selector(signature)
        } else if let Some(selector_hex) = request.function_selector_hex.as_ref() {
            let selector = hex::decode(selector_hex.trim_start_matches("0x"))
                .map_err(|e| anyhow::anyhow!("invalid function selector hex: {}", e))?;
            if selector.len() != 4 {
                anyhow::bail!("function_selector_hex must be 4 bytes");
            }
            let mut selector_bytes = [0u8; 4];
            selector_bytes.copy_from_slice(&selector);
            selector_bytes
        } else {
            anyhow::bail!("either function_signature or function_selector_hex is required");
        };

        let calldata = match request.calldata_hex {
            Some(calldata_hex) => hex::decode(calldata_hex.trim_start_matches("0x"))
                .map_err(|e| anyhow::anyhow!("invalid calldata hex: {}", e))?,
            None => Vec::new(),
        };

        let value = match request.value {
            Some(value) => value
                .parse::<u128>()
                .map_err(|e| anyhow::anyhow!("invalid value: {}", e))?,
            None => 0,
        };

        let block_timestamp = request.block_timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });
        let gas_limit = request.gas_limit.unwrap_or(1_000_000);

        let return_data = crate::contract_executor::execute_call(
            self.storage.as_ref(),
            &contract_address,
            function_selector,
            calldata,
            &caller,
            value,
            block_timestamp,
            gas_limit,
        )?;

        Ok(savitri_rpc::CallContractResponse {
            return_data_hex: format!("0x{}", hex::encode(return_data)),
        })
    }
}

// ─── DAG Reader ─────────────────────────────────────────────────────────

use crate::p2p::dag::DagManager;
use savitri_rpc::dag::{DagBlockInfo, DagGroupInfo, DagReader, DagTipInfo};

/// Adapter that implements DagReader using lightnode's DagManager
pub struct LightnodeDagReader {
    dag: Arc<DagManager>,
    /// Group info from intra-group communication
    group_info: Arc<RwLock<Vec<DagGroupInfo>>>,
}

impl LightnodeDagReader {
    pub fn new(dag: Arc<DagManager>) -> Self {
        Self {
            dag,
            group_info: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn group_info(&self) -> Arc<RwLock<Vec<DagGroupInfo>>> {
        self.group_info.clone()
    }
}

#[async_trait]
impl DagReader for LightnodeDagReader {
    async fn get_blocks_at_height(&self, height: u64) -> Vec<DagBlockInfo> {
        let blocks = self.dag.get_blocks_at_height(height).await;
        blocks
            .into_iter()
            .map(|b| DagBlockInfo {
                hash: format!("0x{}", hex::encode(&b.hash[..32])),
                height: b.height,
                group_id: b.group_id.clone(),
                proposer: format!("0x{}", hex::encode(&b.proposer[..16])),
                parent_hashes: b
                    .parent_hashes
                    .iter()
                    .map(|h| format!("0x{}", hex::encode(&h[..32])))
                    .collect(),
                transactions_count: b.tx_hashes.len(),
                timestamp: b.timestamp,
                pou_score: b.proposer_pou_score,
            })
            .collect()
    }

    async fn get_tips(&self) -> Vec<DagTipInfo> {
        let tips = self.dag.get_tips().await;
        tips.into_iter()
            .map(|(group_id, hash, height)| DagTipInfo {
                group_id,
                hash: format!("0x{}", hex::encode(&hash[..32])),
                height,
            })
            .collect()
    }

    async fn get_groups(&self) -> Vec<DagGroupInfo> {
        self.group_info.read().await.clone()
    }
}
