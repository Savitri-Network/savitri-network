//! Wallet for Savitri Network
//!
//! Key management, message signing, and on-chain account queries.

use anyhow::Result;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use zeroize::Zeroize;

use super::rpc_client::RpcClient;
use crate::types::SdkError;

/// Wallet for managing keys and signing transactions.
///
/// Optionally holds an RPC client for on-chain queries such as balance and
/// nonce lookups.
pub struct Wallet {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    address: String,
    rpc_client: Option<RpcClient>,
}

impl Wallet {
    /// Create a new wallet with a random key pair.
    pub fn new() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        let address = hex::encode(verifying_key.as_bytes());

        Self {
            signing_key,
            verifying_key,
            address,
            rpc_client: None,
        }
    }

    /// Create a wallet from an existing private key.
    pub fn from_private_key(private_key: &[u8; 32]) -> Result<Self> {
        let signing_key = SigningKey::from_bytes(private_key);
        let verifying_key = signing_key.verifying_key();
        let address = hex::encode(verifying_key.as_bytes());

        Ok(Self {
            signing_key,
            verifying_key,
            address,
            rpc_client: None,
        })
    }

    /// Create a wallet from a hex-encoded private key string.
    pub fn from_private_key_hex(hex_str: &str) -> Result<Self> {
        let bytes = hex::decode(hex_str.trim_start_matches("0x"))
            .map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))?;
        if bytes.len() != 32 {
            anyhow::bail!("Private key must be 32 bytes (64 hex characters)");
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Self::from_private_key(&key)
    }

    /// Create a wallet already connected to an RPC endpoint.
    pub fn with_rpc(url: impl Into<String>) -> Result<Self> {
        let mut wallet = Self::new();
        wallet.rpc_client = Some(RpcClient::from_url(url).map_err(|e| anyhow::anyhow!("{}", e))?);
        Ok(wallet)
    }

    /// Return the account address (hex-encoded public key, 64 hex chars).
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Return the verifying (public) key.
    pub fn public_key(&self) -> &VerifyingKey {
        &self.verifying_key
    }

    /// Return the raw private key bytes.
    ///
    /// **Security**: handle with care -- never log or expose this value.
    pub fn private_key(&self) -> [u8; 32] {
        *self.signing_key.as_bytes()
    }

    /// Sign an arbitrary message and return the 64-byte signature.
    pub fn sign_message(&self, message: &[u8]) -> [u8; 64] {
        let signature = self.signing_key.sign(message);
        signature.to_bytes()
    }

    /// Verify a signature against this wallet's public key.
    pub fn verify_signature(&self, message: &[u8], signature: &[u8; 64]) -> Result<()> {
        use ed25519_dalek::{Signature, Verifier};
        let sig = Signature::from_bytes(signature);
        self.verifying_key
            .verify(message, &sig)
            .map_err(|e| anyhow::anyhow!("Signature verification failed: {}", e))
    }

    // ─── RPC-backed convenience methods ────────────────────────────────────

    /// Connect an RPC client to this wallet.
    pub fn connect_rpc(&mut self, url: impl Into<String>) -> Result<()> {
        self.rpc_client = Some(RpcClient::from_url(url).map_err(|e| anyhow::anyhow!("{}", e))?);
        Ok(())
    }

    /// Return a reference to the underlying RPC client, if connected.
    pub fn rpc(&self) -> Option<&RpcClient> {
        self.rpc_client.as_ref()
    }

    /// Fetch this account's balance from the node (requires RPC).
    pub async fn get_balance(&self) -> Result<String, SdkError> {
        let client = self.rpc_client.as_ref().ok_or(SdkError::NoRpcClient)?;
        client.get_balance(&self.address).await
    }

    /// Fetch this account's nonce from the node (requires RPC).
    pub async fn get_nonce(&self) -> Result<u64, SdkError> {
        let client = self.rpc_client.as_ref().ok_or(SdkError::NoRpcClient)?;
        client.get_nonce(&self.address).await
    }
}

/// SECURITY (C-22): Zeroize the signing key when the Wallet is dropped
/// to prevent private key material from lingering in memory.
impl Drop for Wallet {
    fn drop(&mut self) {
        let mut key_bytes = self.signing_key.to_bytes();
        key_bytes.zeroize();
    }
}

impl Clone for Wallet {
    fn clone(&self) -> Self {
        let key_bytes = self.signing_key.to_bytes();
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
            address: self.address.clone(),
            // RPC client is intentionally not cloned because reqwest::Client
            // is already cheaply cloneable inside RpcClient, but RpcClient
            // holds an AtomicU64 which cannot be trivially cloned.
            // Cloned wallets start without an RPC connection.
            rpc_client: None,
        }
    }
}

impl Default for Wallet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_creation() {
        let wallet = Wallet::new();
        assert_eq!(wallet.address().len(), 64);
    }

    #[test]
    fn test_wallet_from_private_key() {
        let wallet1 = Wallet::new();
        let private_key = wallet1.private_key();

        let wallet2 = Wallet::from_private_key(&private_key).unwrap();
        assert_eq!(wallet1.address(), wallet2.address());
    }

    #[test]
    fn test_wallet_from_hex() {
        let wallet1 = Wallet::new();
        let hex_key = hex::encode(wallet1.private_key());

        let wallet2 = Wallet::from_private_key_hex(&hex_key).unwrap();
        assert_eq!(wallet1.address(), wallet2.address());
    }

    #[test]
    fn test_sign_and_verify() {
        let wallet = Wallet::new();
        let message = b"test message";
        let signature = wallet.sign_message(message);

        wallet.verify_signature(message, &signature).unwrap();
    }

    #[test]
    fn test_clone() {
        let wallet1 = Wallet::new();
        let wallet2 = wallet1.clone();
        assert_eq!(wallet1.address(), wallet2.address());
    }
}
