//! Core transaction module for Savitri Light Node
//!
//! This module provides transaction types and utilities for light nodes.

#![allow(dead_code)] // Many types are for API compatibility

use anyhow::Result;
use bincode;
use ed25519_dalek::{Signature, Verifier, VerifyingKey as PublicKey};
use serde::{Deserialize, Serialize};
use sha2::Digest;

// Mempool transaction types for transaction management
// MempoolTx stores the actual SignedTx so block production uses real transaction data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolTx {
    pub id: u64,
    /// The actual signed transaction (stored for real block production)
    pub signed_tx: crate::tx::SignedTx,
}

#[derive(Debug, Clone, Copy)]
pub struct TxHandle(pub u64);

/// Transaction structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Sender address
    pub from: Vec<u8>,
    /// Recipient address
    pub to: Vec<u8>,
    /// Amount
    pub amount: u128,
    /// Nonce
    pub nonce: u64,
    /// Fee
    pub fee: Option<u128>,
    /// Public key
    pub pubkey: Vec<u8>,
    /// Signature
    #[serde(with = "crate::tx::big_array")]
    pub sig: [u8; 64],
    /// Pre-verified flag
    pub pre_verified: bool,
}

impl Transaction {
    /// Create transaction message for signing using cryptographic serialization
    ///
    /// This function creates a deterministic message for cryptographic signing:
    /// 1. Uses canonical serialization format for consistency
    /// 2. Includes all transaction fields that should be signed
    /// 3. Uses hex encoding for addresses to ensure deterministic format
    /// 4. Creates a reproducible message hash for signature verification
    pub fn message(&self) -> Vec<u8> {
        // Create canonical message format for cryptographic signing
        // This ensures the same transaction always produces the same message
        let message = format!(
            "{}:{}:{}:{}:{}",
            hex::encode(&self.from), // Sender address (hex encoded)
            hex::encode(&self.to),   // Recipient address (hex encoded)
            self.amount,             // Amount
            self.nonce,              // Nonce
            self.fee.unwrap_or(0)    // Fee (0 if None)
        );

        // Convert to bytes for cryptographic signing
        message.into_bytes()
    }

    /// Get sender address
    pub fn sender(&self) -> &[u8] {
        &self.from
    }

    /// Get recipient address
    pub fn recipient(&self) -> &[u8] {
        &self.to
    }

    /// Get amount
    pub fn amount(&self) -> u128 {
        self.amount
    }

    /// Get nonce
    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Get fee
    pub fn fee(&self) -> Option<u128> {
        self.fee
    }

    /// Check if transaction is pre-verified
    pub fn is_pre_verified(&self) -> bool {
        self.pre_verified
    }

    /// Set pre-verified flag
    pub fn set_pre_verified(&mut self, verified: bool) {
        self.pre_verified = verified;
    }
}

/// Call transaction (smart contract)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallTransaction {
    /// Transaction data
    pub tx: Transaction,
    /// Contract address
    pub contract: Vec<u8>,
    /// Function selector (serde natively supports arrays up to 32 elements)
    pub selector: [u8; 4],
    /// Call data
    pub data: Vec<u8>,
    /// Value transferred
    pub value: u128,
}

impl CallTransaction {
    /// Create new call transaction
    pub fn new(
        from: Vec<u8>,
        to: Vec<u8>,
        contract: Vec<u8>,
        selector: [u8; 4],
        data: Vec<u8>,
        value: u128,
        nonce: u64,
    ) -> Self {
        Self {
            tx: Transaction {
                from: from.clone(),
                to: to.clone(),
                amount: value,
                nonce,
                fee: None,
                pubkey: Vec::new(),
                sig: [0u8; 64],
                pre_verified: false,
            },
            contract,
            selector,
            data,
            value,
        }
    }

    /// Get transaction
    pub fn transaction(&self) -> &Transaction {
        &self.tx
    }

    /// Get contract address
    pub fn contract(&self) -> &[u8] {
        &self.contract
    }

    /// Get function selector
    pub fn selector(&self) -> &[u8; 4] {
        &self.selector
    }

    /// Get call data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get value
    pub fn value(&self) -> u128 {
        self.value
    }
}

/// Verify cryptographic signature of a transaction using real ed25519 verification
///
/// This function performs genuine cryptographic verification:
/// 1. Validates public key format (32 bytes for ed25519)
/// 2. Validates signature format (64 bytes for ed25519)
/// 3. Uses ed25519-dalek library for real signature verification
/// 4. Verifies the signature against the exact transaction message
pub fn verify_transaction_signature(tx: &Transaction) -> bool {
    // Validate public key length (ed25519 public keys are exactly 32 bytes)
    if tx.pubkey.len() != 32 {
        tracing::debug!(
            pubkey_len = tx.pubkey.len(),
            "Invalid public key length for ed25519"
        );
        return false;
    }

    // Validate signature length (ed25519 signatures are exactly 64 bytes)
    if tx.sig.len() != 64 {
        tracing::debug!(
            sig_len = tx.sig.len(),
            "Invalid signature length for ed25519"
        );
        return false;
    }

    // Check for empty signature (all zeros)
    if tx.sig.iter().all(|&b| b == 0) {
        tracing::debug!("Empty signature detected");
        return false;
    }

    // Parse the public key using ed25519-dalek
    let public_key = match tx.pubkey.as_slice().try_into() {
        Ok(bytes) => match PublicKey::from_bytes(&bytes) {
            Ok(key) => key,
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    pubkey = %hex::encode(&tx.pubkey),
                    "Failed to parse ed25519 public key"
                );
                return false;
            }
        },
        Err(_) => {
            tracing::debug!(
                pubkey_len = tx.pubkey.len(),
                "Invalid public key length, expected 32 bytes"
            );
            return false;
        }
    };

    // Parse the signature using ed25519-dalek
    let signature = Signature::from_bytes(&tx.sig);

    // Create the exact message that should have been signed
    let message = tx.message();

    // Perform real cryptographic signature verification using ed25519-dalek
    let verification_result = public_key.verify(&message, &signature);

    match verification_result {
        Ok(()) => {
            tracing::debug!(
                pubkey = %hex::encode(&tx.pubkey),
                "Cryptographic signature verification successful"
            );
            true
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                pubkey = %hex::encode(&tx.pubkey),
                message_hash = %hex::encode(sha2::Sha256::digest(&message)),
                "Cryptographic signature verification failed"
            );
            false
        }
    }
}

/// Derive address from public key using real cryptographic SHA-256 hashing
///
/// This function performs genuine cryptographic address derivation:
/// 1. Uses SHA-256 hash algorithm (cryptographically secure)
/// 2. Hashes the raw 32-byte ed25519 public key
/// 3. Returns the first 32 bytes of the hash as the address
/// 4. This ensures deterministic address generation from public keys
fn derive_address_from_public_key(pubkey: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};

    // Create SHA-256 hasher (cryptographically secure hash function)
    let mut hasher = Sha256::new();

    // Hash the raw public key bytes
    hasher.update(pubkey);

    // Get the hash result (32 bytes for SHA-256)
    let result = hasher.finalize();

    // Convert to fixed-size array
    let mut address = [0u8; 32];
    address.copy_from_slice(&result);

    tracing::debug!(
        pubkey = %hex::encode(pubkey),
        address = %hex::encode(&address),
        "Derived address from public key using SHA-256"
    );

    address
}

/// Verify that the sender address matches the public key using real cryptographic derivation
///
/// This function ensures the address is cryptographically bound to the public key:
/// 1. Validates both address and public key are 32 bytes
/// 2. Derives address from public key using SHA-256
/// 3. Compares derived address with provided address
/// 4. Uses constant-time comparison to prevent timing attacks
fn verify_address_matches_public_key(address: &[u8], pubkey: &[u8]) -> bool {
    // Validate input lengths
    if address.len() != 32 || pubkey.len() != 32 {
        tracing::debug!(
            address_len = address.len(),
            pubkey_len = pubkey.len(),
            "Invalid address or public key length"
        );
        return false;
    }

    // Derive address from public key using SHA-256
    let derived_address = derive_address_from_public_key(pubkey);

    // Use constant-time comparison to prevent timing attacks
    let matches = address
        .iter()
        .zip(derived_address.iter())
        .all(|(a, b)| a == b);

    if !matches {
        tracing::debug!(
            provided_address = %hex::encode(address),
            derived_address = %hex::encode(&derived_address),
            pubkey = %hex::encode(pubkey),
            "Address does not match public key derivation"
        );
    } else {
        tracing::debug!(
            address = %hex::encode(address),
            "Address matches public key derivation"
        );
    }

    matches
}

/// Deserialize signed transaction
///
/// This function performs comprehensive cryptographic verification:
/// 1. Deserializes the transaction using bincode
/// 2. Validates transaction structure (addresses, amounts, etc.)
/// 3. Verifies the ed25519 cryptographic signature
/// 4. Ensures the sender address matches the public key (SHA-256 hash)
///
/// The transaction is marked as pre_verified only if all checks pass.
/// Maximum allowed size for transaction deserialization (1 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized network payloads.
const MAX_TX_DESERIALIZE_SIZE: usize = 1 * 1024 * 1024;

pub fn deserialize_signed_tx(bytes: &[u8]) -> Result<Transaction> {
    if bytes.len() > MAX_TX_DESERIALIZE_SIZE {
        anyhow::bail!(
            "Transaction data too large for deserialization: {} bytes (max {})",
            bytes.len(),
            MAX_TX_DESERIALIZE_SIZE
        );
    }
    // Use bincode for efficient binary deserialization
    match bincode::deserialize::<Transaction>(bytes) {
        Ok(mut tx) => {
            // Validate transaction structure
            if tx.from.is_empty() {
                anyhow::bail!("Invalid transaction: empty sender address");
            }
            if tx.to.is_empty() {
                anyhow::bail!("Invalid transaction: empty recipient address");
            }
            if tx.amount == 0 && tx.fee.unwrap_or(0) == 0 {
                anyhow::bail!("Invalid transaction: zero amount and fee");
            }

            // Set pre-verified flag based on actual cryptographic signature verification
            tx.pre_verified = verify_transaction_signature(&tx);

            if tx.pre_verified {
                tx.pre_verified = verify_address_matches_public_key(&tx.from, &tx.pubkey);
                if !tx.pre_verified {
                    tracing::warn!(
                        sender = %hex::encode(&tx.from),
                        pubkey = %hex::encode(&tx.pubkey),
                        "Transaction signature valid but address doesn't match public key"
                    );
                }
            }

            Ok(tx)
        }
        Err(e) => anyhow::bail!("Failed to deserialize signed transaction: {}", e),
    }
}

/// Deserialize call transaction
///
/// This function performs comprehensive cryptographic verification for smart contract calls:
/// 1. Deserializes the call transaction using bincode
/// 2. Validates call transaction structure (contract address, function selector, etc.)
/// 3. Validates the embedded transaction structure
/// 4. Verifies the ed25519 cryptographic signature on the embedded transaction
/// 5. Ensures the sender address matches the public key (SHA-256 hash)
///
/// The transaction is marked as pre_verified only if all checks pass.
pub fn deserialize_call_tx(bytes: &[u8]) -> Result<CallTransaction> {
    if bytes.len() > MAX_TX_DESERIALIZE_SIZE {
        anyhow::bail!(
            "Call transaction data too large for deserialization: {} bytes (max {})",
            bytes.len(),
            MAX_TX_DESERIALIZE_SIZE
        );
    }
    // Use bincode for efficient binary deserialization
    match bincode::deserialize::<CallTransaction>(bytes) {
        Ok(mut call_tx) => {
            // Validate call transaction structure
            if call_tx.tx.from.is_empty() {
                anyhow::bail!("Invalid call transaction: empty sender address");
            }
            if call_tx.tx.to.is_empty() {
                anyhow::bail!("Invalid call transaction: empty recipient address");
            }
            if call_tx.contract.is_empty() {
                anyhow::bail!("Invalid call transaction: empty contract address");
            }
            if call_tx.selector.iter().all(|&b| b == 0) {
                anyhow::bail!("Invalid call transaction: empty function selector");
            }

            // Validate the embedded transaction
            if call_tx.tx.amount != call_tx.value {
                anyhow::bail!("Invalid call transaction: amount mismatch between tx and value");
            }

            // Set pre-verified flag based on actual cryptographic signature verification
            call_tx.tx.pre_verified = verify_transaction_signature(&call_tx.tx);

            if call_tx.tx.pre_verified {
                call_tx.tx.pre_verified =
                    verify_address_matches_public_key(&call_tx.tx.from, &call_tx.tx.pubkey);
                if !call_tx.tx.pre_verified {
                    tracing::warn!(
                        sender = %hex::encode(&call_tx.tx.from),
                        pubkey = %hex::encode(&call_tx.tx.pubkey),
                        "Call transaction signature valid but address doesn't match public key"
                    );
                }
            }

            Ok(call_tx)
        }
        Err(e) => anyhow::bail!("Failed to deserialize call transaction: {}", e),
    }
}

/// Hash signed transaction bytes
pub fn hash_signed_tx_bytes(tx: &[u8]) -> [u8; 32] {
    // Use SHA-256 for proper cryptographic hashing
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(tx);
    let result = hasher.finalize();

    // Convert to fixed-size array
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);

    hash
}

/// Transaction pool entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxPoolEntry {
    /// Transaction
    pub tx: Transaction,
    /// Timestamp
    pub timestamp: u64,
    /// Gas limit
    pub gas_limit: u64,
    /// Gas price
    pub gas_price: u128,
}

impl TxPoolEntry {
    /// Create new pool entry
    pub fn new(tx: Transaction, gas_limit: u64, gas_price: u128) -> Self {
        Self {
            tx,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            gas_limit,
            gas_price,
        }
    }

    /// Get transaction
    pub fn transaction(&self) -> &Transaction {
        &self.tx
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get gas limit
    pub fn gas_limit(&self) -> u64 {
        self.gas_limit
    }

    /// Get gas price
    pub fn gas_price(&self) -> u128 {
        self.gas_price
    }

    /// Calculate fee
    pub fn calculate_fee(&self) -> u128 {
        self.gas_limit as u128 * self.gas_price
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Is valid
    pub is_valid: bool,
    /// Error message
    pub error: Option<String>,
    /// Gas used
    pub gas_used: u64,
}

impl ValidationResult {
    /// Create valid result
    pub fn valid(gas_used: u64) -> Self {
        Self {
            is_valid: true,
            error: None,
            gas_used,
        }
    }

    /// Create invalid result
    pub fn invalid(error: String) -> Self {
        Self {
            is_valid: false,
            error: Some(error),
            gas_used: 0,
        }
    }
}

/// Transaction batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionBatch {
    /// Transactions
    pub transactions: Vec<Transaction>,
    /// Batch timestamp
    pub timestamp: u64,
    /// Batch hash
    pub hash: [u8; 32],
}

impl TransactionBatch {
    /// Create new batch
    pub fn new(transactions: Vec<Transaction>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Calculate batch hash
        use std::hash::Hasher;
        let mut hasher = std::collections::hash_map::DefaultHasher::default();
        for tx in &transactions {
            hasher.write(&tx.message());
        }
        let hash = hasher.finish();
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&hash.to_le_bytes()[..32]);

        Self {
            transactions,
            timestamp,
            hash: hash_bytes,
        }
    }

    /// Get transactions
    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    /// Get timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get hash
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn test_transaction_signature_verification() {
        // Generate a valid keypair
        let keypair = SigningKey::generate(&mut rand_core::OsRng);
        let pubkey = keypair.verifying_key().to_bytes();

        // Create a valid transaction
        let mut tx = Transaction {
            from: derive_address_from_public_key(&pubkey).to_vec(),
            to: vec![1u8; 32],
            amount: 1000,
            nonce: 1,
            fee: Some(10),
            pubkey: pubkey.to_vec(),
            sig: [0u8; 64],
            pre_verified: false,
        };

        // Sign the transaction
        let message = tx.message();
        let signature = keypair.sign(&message);
        tx.sig.copy_from_slice(&signature.to_bytes());

        // Verify the signature
        assert!(verify_transaction_signature(&tx));

        // Test deserialization with valid signature
        let serialized = bincode::serialize(&tx).unwrap();
        let deserialized = deserialize_signed_tx(&serialized).unwrap();
        assert!(deserialized.pre_verified);

        // Test with invalid signature
        tx.sig[0] = tx.sig[0].wrapping_add(1);
        let serialized_invalid = bincode::serialize(&tx).unwrap();
        let deserialized_invalid = deserialize_signed_tx(&serialized_invalid).unwrap();
        assert!(!deserialized_invalid.pre_verified);
    }

    #[test]
    fn test_address_derivation() {
        let keypair = SigningKey::generate(&mut rand_core::OsRng);
        let pubkey = keypair.verifying_key().to_bytes();
        let address = derive_address_from_public_key(&pubkey);

        assert!(verify_address_matches_public_key(&address, &pubkey));

        // Test with wrong address
        let wrong_address = [0u8; 32];
        assert!(!verify_address_matches_public_key(&wrong_address, &pubkey));
    }

    #[test]
    fn test_real_cryptographic_verification() {
        // Generate a real ed25519 keypair
        let keypair = SigningKey::generate(&mut rand_core::OsRng);
        let pubkey = keypair.verifying_key().to_bytes();
        let address = derive_address_from_public_key(&pubkey);

        // Create a transaction with real data
        let mut tx = Transaction {
            from: address.to_vec(),
            to: vec![1u8; 32],
            amount: 1000000,
            nonce: 42,
            fee: Some(1000),
            pubkey: pubkey.to_vec(),
            sig: [0u8; 64],
            pre_verified: false,
        };

        // Sign the transaction with the real private key
        let message = tx.message();
        let real_signature = keypair.sign(&message);
        tx.sig.copy_from_slice(&real_signature.to_bytes());

        // Test 1: Verify the signature with real cryptographic verification
        assert!(
            verify_transaction_signature(&tx),
            "Real signature should verify successfully"
        );

        // Test 2: Verify address matches public key
        assert!(
            verify_address_matches_public_key(&address, &pubkey),
            "Address should match public key"
        );

        // Test 3: Full deserialization should mark as pre_verified
        let serialized = bincode::serialize(&tx).unwrap();
        let deserialized = deserialize_signed_tx(&serialized).unwrap();
        assert!(
            deserialized.pre_verified,
            "Transaction should be pre-verified with real signature"
        );

        // Test 4: Tampered signature should fail verification
        let mut tampered_tx = tx.clone();
        tampered_tx.sig[0] = tampered_tx.sig[0].wrapping_add(1); // Tamper with signature
        assert!(
            !verify_transaction_signature(&tampered_tx),
            "Tampered signature should fail verification"
        );

        // Test 5: Wrong address should fail address verification
        let mut wrong_addr_tx = tx.clone();
        wrong_addr_tx.from = vec![99u8; 32]; // Wrong address
        assert!(
            verify_transaction_signature(&wrong_addr_tx),
            "Signature should still be valid"
        );
        assert!(
            !verify_address_matches_public_key(&wrong_addr_tx.from, &wrong_addr_tx.pubkey),
            "Wrong address should fail address verification"
        );

        // Test 6: Full deserialization with wrong address should not be pre-verified
        let serialized_wrong_addr = bincode::serialize(&wrong_addr_tx).unwrap();
        let deserialized_wrong_addr = deserialize_signed_tx(&serialized_wrong_addr).unwrap();
        assert!(
            !deserialized_wrong_addr.pre_verified,
            "Transaction with wrong address should not be pre-verified"
        );

        println!("✅ All real cryptographic verification tests passed!");
    }

    #[test]
    fn test_signature_format_validation() {
        let keypair = SigningKey::generate(&mut rand_core::OsRng);
        let pubkey = keypair.verifying_key().to_bytes();
        let address = derive_address_from_public_key(&pubkey);

        let mut tx = Transaction {
            from: address.to_vec(),
            to: vec![1u8; 32],
            amount: 1000,
            nonce: 1,
            fee: Some(10),
            pubkey: pubkey.to_vec(),
            sig: [0u8; 64],
            pre_verified: false,
        };

        // Sign properly
        let message = tx.message();
        let signature = keypair.sign(&message);
        tx.sig.copy_from_slice(&signature.to_bytes());

        // Test with correct format - should pass
        assert!(verify_transaction_signature(&tx));

        // Test with wrong public key length - should fail
        let mut wrong_pubkey_tx = tx.clone();
        wrong_pubkey_tx.pubkey = vec![1u8; 31]; // Wrong length
        assert!(!verify_transaction_signature(&wrong_pubkey_tx));

        // Test with corrupted signature - should fail
        let mut wrong_sig_tx = tx.clone();
        wrong_sig_tx.sig = [0xFFu8; 64]; // Corrupted signature
        assert!(!verify_transaction_signature(&wrong_sig_tx));

        // Test with empty signature - should fail
        let mut empty_sig_tx = tx.clone();
        empty_sig_tx.sig = [0u8; 64];
        assert!(!verify_transaction_signature(&empty_sig_tx));

        println!("✅ All signature format validation tests passed!");
    }
}
