//! RPC integration for Savitri Masternode
//!
//! Provides MasternodePouReader implementation for savitri-rpc endpoints.

#![cfg(feature = "rpc")]

use async_trait::async_trait;
use savitri_rpc::{MasternodePouInfo, MasternodePouReader, PouGroupInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::group_formation::GroupFormationManager;
use tokio::sync::RwLock;

/// Adapter that implements MasternodePouReader using GroupFormationManager
pub struct MasternodePouReaderImpl {
    group_manager: Arc<RwLock<GroupFormationManager>>,
}

impl MasternodePouReaderImpl {
    pub fn new(group_manager: Arc<RwLock<GroupFormationManager>>) -> Self {
        Self { group_manager }
    }
}

#[cfg(feature = "contracts")]
pub struct MasternodeContractExecutorImpl {
    storage: Arc<savitri_storage::Storage>,
}

#[cfg(feature = "contracts")]
impl MasternodeContractExecutorImpl {
    pub fn new(storage: Arc<savitri_storage::Storage>) -> Self {
        Self { storage }
    }
}

#[cfg(feature = "contracts")]
#[async_trait]
impl savitri_rpc::ContractExecutor for MasternodeContractExecutorImpl {
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
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
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
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
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

#[async_trait]
impl MasternodePouReader for MasternodePouReaderImpl {
    async fn get_groups(&self) -> Vec<PouGroupInfo> {
        let gm = self.group_manager.read().await;
        let groups = gm.get_active_groups().await;
        groups
            .into_iter()
            .map(|g| PouGroupInfo {
                group_id: g.group_id,
                health_score: g.health_score,
                members: g.members,
                proposer: g.proposer,
                group_leader_masternode: g.group_leader_masternode,
                epoch: g.epoch,
            })
            .collect()
    }

    async fn get_masternodes(&self) -> Vec<MasternodePouInfo> {
        let gm = self.group_manager.read().await;
        let groups = gm.get_active_groups().await;
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for g in groups {
            if let Some(ref mn) = g.group_leader_masternode {
                if !seen.contains(mn) {
                    seen.insert(mn.clone());
                    result.push(MasternodePouInfo {
                        node_id: mn.clone(),
                        pou_score: 1.0, // Placeholder - masternode PoU can be extended
                        health_score: g.health_score,
                    });
                }
            }
        }
        result
    }

    async fn get_group_nodes(&self, group_id: &str) -> HashMap<String, u16> {
        let gm = self.group_manager.read().await;
        let groups = gm.get_active_groups().await;
        let nodes = gm.get_registered_nodes().await;
        let mut node_scores: HashMap<String, u16> = HashMap::new();
        for g in groups {
            if g.group_id == group_id {
                for member_id in &g.members {
                    if let Some(node) = nodes.iter().find(|n| n.node_id == *member_id) {
                        let score = (node.pou_score * 1000.0).round().min(1000.0) as u16;
                        node_scores.insert(member_id.clone(), score);
                    }
                }
                break;
            }
        }
        node_scores
    }
}
