use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct DeployContractRequest {
    pub deployer: String,
    pub bytecode_hex: String,
    #[serde(default)]
    pub constructor_args_hex: Option<String>,
    pub nonce: u64,
    #[serde(default)]
    pub gas_limit: Option<u64>,
    #[serde(default)]
    pub block_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeployContractResponse {
    pub contract_address: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CallContractRequest {
    pub contract_address: String,
    #[serde(default)]
    pub function_signature: Option<String>,
    #[serde(default)]
    pub function_selector_hex: Option<String>,
    #[serde(default)]
    pub calldata_hex: Option<String>,
    pub caller: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub gas_limit: Option<u64>,
    #[serde(default)]
    pub block_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CallContractResponse {
    pub return_data_hex: String,
}

#[async_trait]
pub trait ContractExecutor: Send + Sync {
    async fn deploy_contract(
        &self,
        request: DeployContractRequest,
    ) -> anyhow::Result<DeployContractResponse>;

    async fn call_contract(
        &self,
        request: CallContractRequest,
    ) -> anyhow::Result<CallContractResponse>;
}
