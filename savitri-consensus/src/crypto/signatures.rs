//! Signature operations for consensus
//!
//! This module provides Ed25519 signature operations for block signing,

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha512};
use zeroize::Zeroize;

/// Signature error types
#[derive(Debug, Clone, thiserror::Error)]
pub enum SignatureError {
    #[error("Invalid signature length: expected 64, got {0}")]
    InvalidSignatureLength(usize),
    #[error("Invalid public key length: expected 32, got {0}")]
    InvalidPublicKeyLength(usize),
    #[error("Invalid private key length: expected 32, got {0}")]
    InvalidPrivateKeyLength(usize),
    #[error("Signature verification failed")]
    VerificationFailed,
    #[error("Key generation failed: {0}")]
    KeyGenerationFailed(String),
    #[error("Signing failed: {0}")]
    SigningFailed(String),
}

/// Result type for signature operations
pub type SignatureResult<T> = std::result::Result<T, SignatureError>;

/// Key pair for signing operations
#[derive(Clone)]
pub struct KeyPair {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

// SECURITY (HIGH-03): ed25519_dalek::SigningKey implements ZeroizeOnDrop in 2.x.
// The previous manual Drop called `to_bytes()` which returns a *copy* and then
// zeroized that copy — the original key bytes were never cleared. The manual
// impl is removed; the field's own ZeroizeOnDrop handles secure cleanup.

impl KeyPair {
    /// Generate a new random key pair
    pub fn generate() -> SignatureResult<Self> {
        // SECURITY (HIGH-04): Use OsRng (OS CSPRNG) instead of thread_rng
        // which is a PRNG seeded from the OS and may have weaker guarantees.
        let mut secret_key_bytes = [0u8; 32];
        OsRng.fill_bytes(&mut secret_key_bytes);
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_key_bytes);
        let verifying_key = signing_key.verifying_key();
        // SECURITY (C-22): Zeroize the temporary key bytes
        secret_key_bytes.zeroize();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Create key pair from private key bytes
    pub fn from_private_key(private_key: &[u8]) -> SignatureResult<Self> {
        if private_key.len() != 32 {
            return Err(SignatureError::InvalidPrivateKeyLength(private_key.len()));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(private_key);
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = signing_key.verifying_key();
        // SECURITY (C-22): Zeroize the temporary key bytes
        key_bytes.zeroize();
        Ok(Self {
            signing_key,
            verifying_key,
        })
    }

    /// Get public key bytes
    pub fn public_key(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        let signature = self.signing_key.sign(message);
        signature.to_bytes()
    }

    /// Verify a signature
    pub fn verify(&self, message: &[u8], signature: &[u8]) -> SignatureResult<bool> {
        if signature.len() != 64 {
            return Err(SignatureError::InvalidSignatureLength(signature.len()));
        }
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(signature);
        let sig = Signature::from_bytes(&sig_bytes);
        Ok(self.verifying_key.verify(message, &sig).is_ok())
    }
}

/// Sign a message with a private key
pub fn sign_message(message: &[u8], private_key: &[u8]) -> SignatureResult<[u8; 64]> {
    let keypair = KeyPair::from_private_key(private_key)?;
    Ok(keypair.sign(message))
}

/// Verify a signature with a public key
pub fn verify_signature(
    message: &[u8],
    signature: &[u8],
    public_key: &[u8],
) -> SignatureResult<bool> {
    if signature.len() != 64 {
        return Err(SignatureError::InvalidSignatureLength(signature.len()));
    }
    if public_key.len() != 32 {
        return Err(SignatureError::InvalidPublicKeyLength(public_key.len()));
    }

    let mut pk_bytes = [0u8; 32];
    pk_bytes.copy_from_slice(public_key);
    let verifying_key =
        VerifyingKey::from_bytes(&pk_bytes).map_err(|e| SignatureError::VerificationFailed)?;

    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(signature);
    let sig = Signature::from_bytes(&sig_bytes);

    Ok(verifying_key.verify(message, &sig).is_ok())
}

/// Sign block hash
pub fn sign_block_hash(block_hash: &[u8; 64], private_key: &[u8]) -> SignatureResult<[u8; 64]> {
    sign_message(block_hash, private_key)
}

/// Verify block signature
pub fn verify_block_signature(
    block_hash: &[u8; 64],
    signature: &[u8],
    proposer_public_key: &[u8],
) -> SignatureResult<bool> {
    verify_signature(block_hash, signature, proposer_public_key)
}

/// Sign transaction
pub fn sign_transaction(tx_hash: &[u8; 32], private_key: &[u8]) -> SignatureResult<[u8; 64]> {
    sign_message(tx_hash, private_key)
}

/// Verify transaction signature
pub fn verify_transaction_signature(
    tx_hash: &[u8; 32],
    signature: &[u8],
    sender_public_key: &[u8],
) -> SignatureResult<bool> {
    verify_signature(tx_hash, signature, sender_public_key)
}

/// Aggregate signatures (simple concatenation for now)
pub struct AggregateSignature {
    signatures: Vec<[u8; 64]>,
    public_keys: Vec<[u8; 32]>,
}

impl AggregateSignature {
    /// Create new aggregate signature
    pub fn new() -> Self {
        Self {
            signatures: Vec::new(),
            public_keys: Vec::new(),
        }
    }

    /// Add a signature to the aggregate
    pub fn add(&mut self, signature: [u8; 64], public_key: [u8; 32]) {
        self.signatures.push(signature);
        self.public_keys.push(public_key);
    }

    /// Get number of signatures
    pub fn len(&self) -> usize {
        self.signatures.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.signatures.is_empty()
    }

    /// Verify all signatures against a message
    pub fn verify_all(&self, message: &[u8]) -> SignatureResult<bool> {
        for (sig, pk) in self.signatures.iter().zip(self.public_keys.iter()) {
            if !verify_signature(message, sig, pk)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Get all signatures
    pub fn signatures(&self) -> &[[u8; 64]] {
        &self.signatures
    }

    /// Get all public keys
    pub fn public_keys(&self) -> &[[u8; 32]] {
        &self.public_keys
    }
}

impl Default for AggregateSignature {
    fn default() -> Self {
        Self::new()
    }
}
