//! Transaction builder for Savitri SDK
//!
//! Fluent builder for constructing, signing, and serialising transactions
//! compatible with the Savitri mempool.

use crate::client::Wallet;
use crate::types::{SignedTransaction, UnsignedTransaction};
use anyhow::Result;

/// Governance actions that can be encoded into a transaction.
#[derive(Debug, Clone)]
pub enum GovernanceAction {
    /// Vote on a proposal (true = support, false = oppose).
    Vote(bool),
    /// Execute an approved proposal.
    Execute,
}

/// Fluent builder for constructing Savitri transactions.
pub struct TransactionBuilder {
    from: Option<String>,
    to: Option<String>,
    value: u128,
    nonce: Option<u64>,
    fee: Option<u128>,
    data: Option<Vec<u8>>,
}

impl TransactionBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            from: None,
            to: None,
            value: 0,
            nonce: None,
            fee: None,
            data: None,
        }
    }

    /// Set the sender address.
    pub fn from(mut self, address: impl Into<String>) -> Self {
        self.from = Some(address.into());
        self
    }

    /// Set the recipient address.
    pub fn to(mut self, address: impl Into<String>) -> Self {
        self.to = Some(address.into());
        self
    }

    /// Set the transfer value.
    pub fn value(mut self, amount: u128) -> Self {
        self.value = amount;
        self
    }

    /// Set the nonce.
    pub fn nonce(mut self, nonce: u64) -> Self {
        self.nonce = Some(nonce);
        self
    }

    /// Set the fee.
    pub fn fee(mut self, fee: u128) -> Self {
        self.fee = Some(fee);
        self
    }

    /// Set the data payload (for contract calls).
    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = Some(data);
        self
    }

    /// Build an unsigned transaction.
    pub fn build(self) -> Result<UnsignedTransaction> {
        let from = self
            .from
            .ok_or_else(|| anyhow::anyhow!("From address is required"))?;

        Ok(UnsignedTransaction {
            from,
            to: self.to,
            value: self.value,
            nonce: self.nonce.unwrap_or(0),
            fee: self.fee,
            data: self.data,
        })
    }

    /// Build and sign a transaction with the given wallet.
    ///
    /// If `from` was not explicitly set, the wallet address is used.
    ///
    /// The signing scheme matches the one used by `savitri-rpc` faucet handler:
    ///   message = from_hex || to_hex || amount_le || nonce_le || fee_le
    ///   hash    = SHA-256(message)
    ///   sig     = Ed25519_sign(hash)
    pub fn build_and_sign(mut self, wallet: &Wallet) -> Result<SignedTransaction> {
        // Default sender to wallet address if not explicitly set.
        if self.from.is_none() {
            self.from = Some(wallet.address().to_string());
        }

        let tx = self.build()?;

        let message = Self::create_signing_message(&tx);
        let signature = wallet.sign_message(&message);
        let public_key = wallet.public_key().as_bytes().to_vec();

        Ok(SignedTransaction {
            transaction: tx,
            public_key,
            signature: signature.to_vec(),
        })
    }

    /// Encode an oracle call into the transaction data.
    pub fn oracle_call(
        self,
        oracle_address: impl Into<String>,
        function: &str,
        params: &[u8],
    ) -> Self {
        let mut call_data = function.as_bytes().to_vec();
        call_data.extend_from_slice(params);
        self.to(oracle_address).data(call_data)
    }

    /// Encode a governance call into the transaction data.
    pub fn governance_call(
        self,
        governance_address: impl Into<String>,
        proposal_id: u64,
        action: GovernanceAction,
    ) -> Self {
        let call_data = match action {
            GovernanceAction::Vote(support) => {
                let mut data = b"vote".to_vec();
                data.extend_from_slice(&proposal_id.to_le_bytes());
                data.push(if support { 1 } else { 0 });
                data
            }
            GovernanceAction::Execute => {
                let mut data = b"execute".to_vec();
                data.extend_from_slice(&proposal_id.to_le_bytes());
                data
            }
        };
        self.to(governance_address).data(call_data)
    }

    /// Encode an FL proposal creation into the transaction data.
    pub fn create_fl_proposal(
        self,
        governance_address: impl Into<String>,
        title: &str,
        description: &str,
        voting_period: u64,
    ) -> Self {
        let mut call_data = b"create_fl_proposal".to_vec();
        call_data.extend_from_slice(title.as_bytes());
        call_data.push(0); // separator
        call_data.extend_from_slice(description.as_bytes());
        call_data.push(0); // separator
        call_data.extend_from_slice(&voting_period.to_le_bytes());
        self.to(governance_address).data(call_data)
    }

    /// Create the byte message that is signed.
    ///
    /// The format matches the faucet signing in `savitri-rpc/src/handlers.rs`:
    ///   from_hex_bytes || to_hex_bytes || amount_le_u64 || nonce_le_u64 || fee_le_u128
    /// then SHA-256 hashed.
    fn create_signing_message(tx: &UnsignedTransaction) -> Vec<u8> {
        use sha2::Digest;

        let mut message = Vec::new();
        message.extend_from_slice(tx.from.as_bytes());
        if let Some(ref to) = tx.to {
            message.extend_from_slice(to.as_bytes());
        }
        let amount = tx.value as u64;
        message.extend_from_slice(&amount.to_le_bytes());
        message.extend_from_slice(&tx.nonce.to_le_bytes());
        let fee = tx.fee.unwrap_or(0);
        message.extend_from_slice(&fee.to_le_bytes());

        let hash = sha2::Sha256::digest(&message);
        hash.to_vec()
    }
}

impl Default for TransactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}
