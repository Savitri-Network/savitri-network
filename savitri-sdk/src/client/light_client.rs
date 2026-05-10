//! Light Client for Savitri Network
//!
//! A convenience wrapper around RpcClient providing a reduced API surface
//! optimised for light-weight consumers.

use super::rpc_client::RpcClient;
use crate::types::{
    AccountResponse, BlockResponse, HealthResponse, PouLocalResponse, SdkError,
    SendTransactionResult,
};

/// Light client with a simplified interface.
///
/// Delegates every call to the underlying [`RpcClient`] using JSON-RPC 2.0.
pub struct LightClient {
    rpc: RpcClient,
}

impl LightClient {
    /// Create a new light client connected to the given URL.
    pub fn new(url: impl Into<String>) -> Result<Self, SdkError> {
        let rpc = RpcClient::from_url(url)?;
        Ok(Self { rpc })
    }

    /// Return a reference to the inner RPC client for advanced usage.
    pub fn rpc(&self) -> &RpcClient {
        &self.rpc
    }

    /// Check if the node is reachable.
    pub async fn is_connected(&self) -> bool {
        self.rpc.ping().await.unwrap_or(false)
    }

    /// Get the health status of the connected node.
    pub async fn health(&self) -> Result<HealthResponse, SdkError> {
        self.rpc.health().await
    }

    /// Get the current block height.
    pub async fn get_block_number(&self) -> Result<u64, SdkError> {
        self.rpc.get_block_number().await
    }

    /// Get a block by height.
    pub async fn get_block(&self, height: u64) -> Result<BlockResponse, SdkError> {
        self.rpc.get_block_by_height(height).await
    }

    /// Get account balance and nonce.
    pub async fn get_account(&self, address: &str) -> Result<AccountResponse, SdkError> {
        self.rpc.get_account(address).await
    }

    /// Get the balance of an account.
    pub async fn get_balance(&self, address: &str) -> Result<String, SdkError> {
        self.rpc.get_balance(address).await
    }

    /// Submit a signed raw transaction.
    pub async fn send_raw_transaction(
        &self,
        raw_tx_hex: &str,
    ) -> Result<SendTransactionResult, SdkError> {
        self.rpc.send_raw_transaction(raw_tx_hex).await
    }

    /// Get local PoU status.
    pub async fn pou_local(&self) -> Result<PouLocalResponse, SdkError> {
        self.rpc.pou_local().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_light_client_creation() {
        let client = LightClient::new("http://localhost:8545");
        assert!(client.is_ok());
    }
}
