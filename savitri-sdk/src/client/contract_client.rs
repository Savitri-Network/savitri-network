//! Contract Client for Savitri Network
//!
//! High-level helpers for interacting with smart contracts, oracles, and
//! governance via the Savitri JSON-RPC 2.0 interface.

use crate::client::{RpcClient, Wallet};
use crate::types::{Address, Balance, SignedTransaction, TransactionHash};
use crate::utils::TransactionBuilder;
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// High-level client for contract interactions.
///
/// Wraps an [`RpcClient`] and a [`Wallet`] to provide build-sign-send helpers.
pub struct ContractClient {
    rpc: RpcClient,
    wallet: Wallet,
}

impl ContractClient {
    /// Create a new contract client.
    pub fn new(rpc: RpcClient, wallet: Wallet) -> Self {
        Self { rpc, wallet }
    }

    /// Create from a URL and wallet.
    pub fn from_url_and_wallet(url: impl Into<String>, wallet: Wallet) -> Result<Self> {
        let rpc = RpcClient::from_url(url).map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(Self::new(rpc, wallet))
    }

    /// Obtain an oracle client bound to this contract client.
    pub fn oracle(&self) -> OracleClient<'_> {
        OracleClient { client: self }
    }

    /// Obtain a governance client bound to this contract client.
    pub fn governance(&self) -> GovernanceClient<'_> {
        GovernanceClient { client: self }
    }

    /// Build, sign, and submit a contract call transaction.
    ///
    /// Returns the transaction hash on success.
    pub async fn call_contract(
        &self,
        contract_address: &Address,
        function_selector: &[u8],
        args: &[u8],
        value: Option<Balance>,
    ) -> Result<TransactionHash> {
        let mut call_data = function_selector.to_vec();
        call_data.extend_from_slice(args);

        let tx = TransactionBuilder::new()
            .from(self.wallet.address())
            .to(contract_address)
            .value(value.unwrap_or(0))
            .data(call_data)
            .build_and_sign(&self.wallet)?;

        let raw_hex = Self::serialize_signed_tx(&tx);
        let result = self
            .rpc
            .send_raw_transaction(&raw_hex)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;
        Ok(result.tx_hash)
    }

    /// Serialize a signed transaction to hex for `savitri_sendRawTransaction`.
    ///
    /// The format matches what the mempool `bytes_to_raw_tx` expects:
    /// bincode-serialized TransactionExt bytes, hex-encoded.
    fn serialize_signed_tx(tx: &SignedTransaction) -> String {
        let mut payload = Vec::new();
        // from (hex string)
        payload.extend_from_slice(tx.transaction.from.as_bytes());
        // to (hex string or zero)
        let zero_addr = "0".repeat(64);
        let to = tx.transaction.to.as_deref().unwrap_or(&zero_addr);
        payload.extend_from_slice(to.as_bytes());
        // amount (u64 LE -- truncate from u128)
        let amount = tx.transaction.value as u64;
        payload.extend_from_slice(&amount.to_le_bytes());
        // nonce (u64 LE)
        payload.extend_from_slice(&tx.transaction.nonce.to_le_bytes());
        // fee (u128 LE)
        let fee = tx.transaction.fee.unwrap_or(0);
        payload.extend_from_slice(&fee.to_le_bytes());
        // pubkey
        payload.extend_from_slice(&tx.public_key);
        // signature
        payload.extend_from_slice(&tx.signature);
        // data
        if let Some(ref data) = tx.transaction.data {
            payload.extend_from_slice(data);
        }
        hex::encode(payload)
    }
}

// ─── Oracle client ─────────────────────────────────────────────────────────

/// Client for oracle system interactions.
pub struct OracleClient<'a> {
    client: &'a ContractClient,
}

impl<'a> OracleClient<'a> {
    /// Request data from an oracle contract.
    pub async fn request_data(
        &self,
        oracle_address: &Address,
        data_type: &str,
        params: &[u8],
    ) -> Result<TransactionHash> {
        let function_selector = b"request_data";
        let mut args = data_type.as_bytes().to_vec();
        args.push(0); // separator
        args.extend_from_slice(params);

        self.client
            .call_contract(oracle_address, function_selector, &args, Some(0))
            .await
    }

    /// Submit a response to an oracle request.
    pub async fn submit_response(
        &self,
        oracle_address: &Address,
        request_id: u64,
        response: &[u8],
    ) -> Result<TransactionHash> {
        let function_selector = b"submit_response";
        let mut args = request_id.to_le_bytes().to_vec();
        args.extend_from_slice(response);

        self.client
            .call_contract(oracle_address, function_selector, &args, Some(0))
            .await
    }

    /// Verify oracle data on-chain.
    pub async fn verify_data(&self, oracle_address: &Address, data: &[u8]) -> Result<bool> {
        let function_selector = b"verify_data";
        let tx_hash = self
            .client
            .call_contract(oracle_address, function_selector, data, Some(0))
            .await?;
        Ok(!tx_hash.is_empty())
    }
}

// ─── Governance client ─────────────────────────────────────────────────────

/// Client for governance system interactions.
pub struct GovernanceClient<'a> {
    client: &'a ContractClient,
}

impl<'a> GovernanceClient<'a> {
    /// Create a new FL governance proposal.
    pub async fn create_proposal(
        &self,
        governance_address: &Address,
        title: &str,
        description: &str,
        voting_period: u64,
    ) -> Result<TransactionHash> {
        let function_selector = b"create_fl_proposal";
        let mut args = title.as_bytes().to_vec();
        args.push(0);
        args.extend_from_slice(description.as_bytes());
        args.push(0);
        args.extend_from_slice(&voting_period.to_le_bytes());

        self.client
            .call_contract(governance_address, function_selector, &args, Some(0))
            .await
    }

    /// Vote on a governance proposal.
    pub async fn vote(
        &self,
        governance_address: &Address,
        proposal_id: u64,
        support: bool,
    ) -> Result<TransactionHash> {
        let function_selector = b"vote";
        let mut args = proposal_id.to_le_bytes().to_vec();
        args.push(if support { 1 } else { 0 });

        self.client
            .call_contract(governance_address, function_selector, &args, Some(0))
            .await
    }

    /// Execute an approved proposal.
    pub async fn execute(
        &self,
        governance_address: &Address,
        proposal_id: u64,
    ) -> Result<TransactionHash> {
        let function_selector = b"execute";
        let args = proposal_id.to_le_bytes();

        self.client
            .call_contract(governance_address, function_selector, &args, Some(0))
            .await
    }

    /// Query a proposal's status.
    ///
    /// Note: in the current implementation this sends a transaction and returns
    /// a placeholder status.  A future version will use a read-only RPC call.
    pub async fn get_proposal_status(
        &self,
        governance_address: &Address,
        proposal_id: u64,
    ) -> Result<ProposalStatus> {
        let function_selector = b"get_proposal_status";
        let args = proposal_id.to_le_bytes();

        let _tx_hash = self
            .client
            .call_contract(governance_address, function_selector, &args, Some(0))
            .await?;

        // Placeholder until a read-only query RPC is available.
        Ok(ProposalStatus {
            id: proposal_id,
            title: String::new(),
            votes_for: 0,
            votes_against: 0,
            status: "unknown".to_string(),
            executed: false,
        })
    }
}

/// Governance proposal status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalStatus {
    /// Proposal ID.
    pub id: u64,
    /// Proposal title.
    pub title: String,
    /// Votes in favour.
    pub votes_for: u64,
    /// Votes against.
    pub votes_against: u64,
    /// Status string (e.g. "active", "passed", "rejected").
    pub status: String,
    /// Whether the proposal has been executed.
    pub executed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proposal_status_serialization() {
        let status = ProposalStatus {
            id: 1,
            title: "Test Proposal".to_string(),
            votes_for: 100,
            votes_against: 50,
            status: "active".to_string(),
            executed: false,
        };

        let serialized = serde_json::to_string(&status).unwrap();
        let deserialized: ProposalStatus = serde_json::from_str(&serialized).unwrap();

        assert_eq!(status.id, deserialized.id);
        assert_eq!(status.title, deserialized.title);
    }
}
