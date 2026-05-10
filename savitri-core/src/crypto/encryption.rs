//! Encryption utilities for Savitri Network
//!
//! This module provides cryptographic encryption and decryption functionality
//! using AES-256-GCM for symmetric encryption and ECIES (Ed25519→X25519 ECDH
//! + HKDF-SHA256) for hybrid/asymmetric encryption.

use aes_gcm::{aead::Aead, Aes256Gcm, Key, KeyInit, Nonce};
use anyhow::Result;
use curve25519_dalek::Scalar;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256, Sha512};
use zeroize::Zeroizing;

/// AES-256-GCM encrypted data structure
#[derive(Debug, Clone)]
pub struct EncryptedData {
    /// The encrypted ciphertext
    pub ciphertext: Vec<u8>,
    /// The nonce used for encryption
    pub nonce: Vec<u8>,
    /// The authentication tag
    pub tag: Vec<u8>,
}

/// Key pair for asymmetric encryption
#[derive(Debug, Clone)]
pub struct EncryptionKeyPair {
    /// The public verifying key
    pub public_key: VerifyingKey,
    /// The private signing key
    pub private_key: SigningKey,
}

// ─── HKDF and ECDH primitives ────────────────────────────────────────────

/// HKDF-SHA256 (RFC 5869) — extract-then-expand, single-block output (32 bytes).
fn hkdf_sha256(ikm: &[u8], salt: Option<&[u8]>, info: &[u8]) -> [u8; 32] {
    type HmacSha256 = Hmac<Sha256>;

    // Extract phase: PRK = HMAC-SHA256(salt, IKM)
    let default_salt = [0u8; 32];
    let salt = salt.unwrap_or(&default_salt);
    let mut extract_mac =
        <HmacSha256 as Mac>::new_from_slice(salt).expect("HMAC accepts any key length");
    extract_mac.update(ikm);
    let prk = extract_mac.finalize().into_bytes();

    // Expand phase (single block → 32 bytes): OKM = HMAC-SHA256(PRK, info || 0x01)
    let mut expand_mac =
        <HmacSha256 as Mac>::new_from_slice(&prk).expect("HMAC accepts any key length");
    expand_mac.update(info);
    expand_mac.update(&[1u8]);
    let okm = expand_mac.finalize().into_bytes();

    let mut key = [0u8; 32];
    key.copy_from_slice(&okm);
    key
}

/// Convert Ed25519 private key seed to an X25519-compatible scalar.
///
/// Follows the standard Ed25519→X25519 conversion:
/// 1. SHA-512(seed) → take first 32 bytes
/// 2. Apply X25519 clamping per RFC 7748
fn ed25519_to_x25519_scalar(signing_key: &SigningKey) -> Scalar {
    let hash = Sha512::digest(signing_key.to_bytes());
    let mut scalar_bytes = [0u8; 32];
    scalar_bytes.copy_from_slice(&hash[..32]);
    // X25519 clamping per RFC 7748 §5
    scalar_bytes[0] &= 248;
    scalar_bytes[31] &= 127;
    scalar_bytes[31] |= 64;
    Scalar::from_bytes_mod_order(scalar_bytes)
}

/// Perform ECDH key agreement using Ed25519 keys converted to X25519.
///
/// Returns the raw shared secret (32 bytes). Always process through
/// HKDF before use as an encryption key.
fn ecdh(private_key: &SigningKey, public_key: &VerifyingKey) -> [u8; 32] {
    let scalar = ed25519_to_x25519_scalar(private_key);
    let point = public_key.to_montgomery();
    let shared = &point * &scalar;
    shared.to_bytes()
}

// ─── Symmetric Encryption ────────────────────────────────────────────────

/// Symmetric encryption using AES-256-GCM.
///
/// # Security
/// - HIGH-01 (fixed): The `key` field is wrapped in `Zeroizing<[u8; 32]>` so it is
///   automatically cleared when `SymmetricEncryption` is dropped.
/// - HIGH-04 (fixed): OsRng is used for key and nonce generation.
/// - MED-06 (fixed): `get_key()` removed — do not expose the raw key bytes.
pub struct SymmetricEncryption {
    cipher: Aes256Gcm,
    key: Zeroizing<[u8; 32]>,
}

impl SymmetricEncryption {
    /// Create new symmetric encryption with a cryptographically random key.
    pub fn new() -> Result<Self> {
        let mut key_bytes = Zeroizing::new([0u8; 32]);
        OsRng.fill_bytes(key_bytes.as_mut());
        let key = Key::<Aes256Gcm>::from_slice(key_bytes.as_ref());
        let cipher = Aes256Gcm::new(key);

        Ok(Self {
            cipher,
            key: key_bytes,
        })
    }

    /// Create symmetric encryption from an existing key.
    pub fn from_key(key: &[u8; 32]) -> Result<Self> {
        let key_ref = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(key_ref);

        Ok(Self {
            cipher,
            key: Zeroizing::new(*key),
        })
    }

    /// Encrypt data using AES-256-GCM
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| anyhow::anyhow!("Encryption failed: {}", e))?;

        // Split ciphertext and tag (last 16 bytes are the tag)
        let tag_start = ciphertext.len() - 16;
        let tag = ciphertext[tag_start..].to_vec();
        let ciphertext_without_tag = ciphertext[..tag_start].to_vec();

        Ok(EncryptedData {
            ciphertext: ciphertext_without_tag,
            nonce: nonce_bytes.to_vec(),
            tag,
        })
    }

    /// Decrypt data using AES-256-GCM
    pub fn decrypt(&self, encrypted_data: &EncryptedData) -> Result<Vec<u8>> {
        let nonce = Nonce::from_slice(&encrypted_data.nonce);

        // Combine ciphertext and tag
        let mut ciphertext_with_tag = encrypted_data.ciphertext.clone();
        ciphertext_with_tag.extend_from_slice(&encrypted_data.tag);

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext_with_tag.as_ref())
            .map_err(|e| anyhow::anyhow!("Decryption failed: {}", e))?;

        Ok(plaintext)
    }

    // MED-06: get_key() removed — raw key bytes must not be exposed via public API.
}

// ─── Asymmetric (signing + ECDH) ────────────────────────────────────────

/// Asymmetric operations using Ed25519 (signing) and X25519 (key exchange).
pub struct AsymmetricEncryption;

impl AsymmetricEncryption {
    /// Generate new encryption key pair
    pub fn generate_keypair() -> EncryptionKeyPair {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let verifying_key = signing_key.verifying_key();

        EncryptionKeyPair {
            public_key: verifying_key,
            private_key: signing_key,
        }
    }

    /// Derive a shared symmetric key from a public key and private key.
    ///
    /// Uses Ed25519→X25519 conversion + ECDH + HKDF-SHA256.
    /// The result is suitable as an AES-256-GCM key.
    pub fn derive_shared_key(public_key: &VerifyingKey, private_key: &SigningKey) -> [u8; 32] {
        // SECURITY: Proper ECDH key agreement + HKDF key derivation.
        // 1. Convert Ed25519 keys to X25519 and perform Diffie-Hellman
        let shared_secret = ecdh(private_key, public_key);
        // 2. Derive encryption key via HKDF-SHA256 with domain separation
        hkdf_sha256(&shared_secret, None, b"savitri-shared-key-v1")
    }

    /// Sign data with private key
    pub fn sign(private_key: &SigningKey, data: &[u8]) -> Vec<u8> {
        let signature = private_key.sign(data);
        signature.to_bytes().to_vec()
    }

    /// Verify signature with public key
    pub fn verify(public_key: &VerifyingKey, data: &[u8], signature: &[u8]) -> Result<bool> {
        use ed25519_dalek::Signature;

        let sig_array: &[u8; 64] = signature
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;
        let sig = Signature::from_bytes(sig_array);

        Ok(public_key.verify(data, &sig).is_ok())
    }
}

// ─── Hybrid / ECIES Encryption ──────────────────────────────────────────

/// ECIES-style hybrid encryption using ephemeral ECDH + AES-256-GCM.
///
/// Encrypt flow:
///   1. Generate ephemeral Ed25519 keypair
///   2. ECDH(ephemeral_private, recipient_public) → shared secret
///   3. HKDF-SHA256(shared_secret, info) → AES key
///   4. AES-256-GCM encrypt data
///   5. Output: ephemeral_public(32) || nonce(12) || ciphertext || tag(16)
///
/// Decrypt flow:
///   1. Extract ephemeral_public from ciphertext
///   2. ECDH(recipient_private, ephemeral_public) → same shared secret
///   3. HKDF → same AES key
///   4. AES-256-GCM decrypt
pub struct HybridEncryption;

impl HybridEncryption {
    /// Encrypt data for a recipient using ECIES.
    pub fn encrypt(data: &[u8], recipient_public_key: &VerifyingKey) -> Result<EncryptedData> {
        // 1. Generate ephemeral keypair
        let ephemeral_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let ephemeral_public = ephemeral_key.verifying_key();

        // 2. ECDH: shared_secret = DH(ephemeral_private, recipient_public)
        let shared_secret = ecdh(&ephemeral_key, recipient_public_key);

        // 3. HKDF: derive AES-256 key from shared secret
        let aes_key = hkdf_sha256(&shared_secret, None, b"savitri-ecies-v1");

        // 4. AES-256-GCM encrypt
        let symmetric = SymmetricEncryption::from_key(&aes_key)?;
        let encrypted_data = symmetric.encrypt(data)?;

        // 5. Assemble: ephemeral_public(32) || nonce(12) || ciphertext || tag(16)
        let mut combined = Vec::with_capacity(
            32 + encrypted_data.nonce.len()
                + encrypted_data.ciphertext.len()
                + encrypted_data.tag.len(),
        );
        combined.extend_from_slice(ephemeral_public.as_bytes());
        combined.extend_from_slice(&encrypted_data.nonce);
        combined.extend_from_slice(&encrypted_data.ciphertext);
        combined.extend_from_slice(&encrypted_data.tag);

        Ok(EncryptedData {
            ciphertext: combined,
            nonce: vec![],
            tag: vec![],
        })
    }

    /// Decrypt ECIES-encrypted data using the recipient's private key.
    pub fn decrypt(encrypted_data: &EncryptedData, private_key: &SigningKey) -> Result<Vec<u8>> {
        // Minimum: 32 (ephemeral pub) + 12 (nonce) + 16 (tag) = 60 bytes
        if encrypted_data.ciphertext.len() < 60 {
            return Err(anyhow::anyhow!(
                "Invalid ECIES data: too short ({} bytes)",
                encrypted_data.ciphertext.len()
            ));
        }

        // 1. Extract ephemeral public key (first 32 bytes)
        let ephemeral_public_bytes: [u8; 32] = encrypted_data.ciphertext[..32]
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid ephemeral public key length"))?;
        let ephemeral_public = VerifyingKey::from_bytes(&ephemeral_public_bytes)
            .map_err(|e| anyhow::anyhow!("Invalid ephemeral public key: {}", e))?;

        // 2. ECDH: shared_secret = DH(recipient_private, ephemeral_public)
        // This produces the SAME shared secret as in encrypt() because
        // DH(a, B) == DH(b, A) for the ECDH operation.
        let shared_secret = ecdh(private_key, &ephemeral_public);

        // 3. HKDF: derive same AES-256 key
        let aes_key = hkdf_sha256(&shared_secret, None, b"savitri-ecies-v1");

        // 4. Parse nonce(12) || ciphertext || tag(16)
        let remaining = &encrypted_data.ciphertext[32..];
        let nonce = &remaining[..12];
        let ciphertext_and_tag = &remaining[12..];
        let tag_start = ciphertext_and_tag.len() - 16;
        let ciphertext = &ciphertext_and_tag[..tag_start];
        let tag = &ciphertext_and_tag[tag_start..];

        // 5. AES-256-GCM decrypt
        let symmetric = SymmetricEncryption::from_key(&aes_key)?;
        let encrypted_struct = EncryptedData {
            ciphertext: ciphertext.to_vec(),
            nonce: nonce.to_vec(),
            tag: tag.to_vec(),
        };

        symmetric.decrypt(&encrypted_struct)
    }
}

/// Utility functions for common encryption operations
pub mod utils {
    use super::*;

    /// Generate random encryption key using the OS CSPRNG.
    pub fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }

    /// Derive key from password using PBKDF2
    pub fn derive_key_from_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
        use pbkdf2::pbkdf2_hmac;
        use sha2::Sha256;

        let mut key = [0u8; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut key);
        key
    }

    /// Generate a secure random nonce using the OS CSPRNG.
    pub fn generate_nonce() -> [u8; 12] {
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);
        nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symmetric_encryption() {
        let symmetric = SymmetricEncryption::new().unwrap();
        let plaintext = b"Hello, Savitri Network!";

        let encrypted = symmetric.encrypt(plaintext).unwrap();
        let decrypted = symmetric.decrypt(&encrypted).unwrap();

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_asymmetric_signing() {
        let keypair = AsymmetricEncryption::generate_keypair();
        let data = b"Test data for signing";

        let signature = AsymmetricEncryption::sign(&keypair.private_key, data);
        let verified = AsymmetricEncryption::verify(&keypair.public_key, data, &signature).unwrap();

        assert!(verified);
    }

    #[test]
    fn test_hybrid_encryption() {
        let recipient_keypair = AsymmetricEncryption::generate_keypair();
        let plaintext = b"Hybrid encryption test data";

        let encrypted =
            HybridEncryption::encrypt(plaintext, &recipient_keypair.public_key).unwrap();
        let decrypted =
            HybridEncryption::decrypt(&encrypted, &recipient_keypair.private_key).unwrap();

        assert_eq!(plaintext.to_vec(), decrypted);
    }

    #[test]
    fn test_derive_shared_key_symmetric() {
        // ECDH must produce the same shared key regardless of who is "sender" vs "receiver"
        let alice = AsymmetricEncryption::generate_keypair();
        let bob = AsymmetricEncryption::generate_keypair();

        let key_alice =
            AsymmetricEncryption::derive_shared_key(&bob.public_key, &alice.private_key);
        let key_bob = AsymmetricEncryption::derive_shared_key(&alice.public_key, &bob.private_key);

        assert_eq!(key_alice, key_bob, "ECDH shared keys must match");
    }

    #[test]
    fn test_hybrid_wrong_key_fails() {
        let recipient = AsymmetricEncryption::generate_keypair();
        let wrong_key = AsymmetricEncryption::generate_keypair();
        let plaintext = b"Secret data";

        let encrypted = HybridEncryption::encrypt(plaintext, &recipient.public_key).unwrap();
        // Decrypting with wrong key should fail (AES-GCM authentication failure)
        assert!(HybridEncryption::decrypt(&encrypted, &wrong_key.private_key).is_err());
    }

    #[test]
    fn test_hybrid_tampered_ciphertext_fails() {
        let recipient = AsymmetricEncryption::generate_keypair();
        let plaintext = b"Secret data";

        let mut encrypted = HybridEncryption::encrypt(plaintext, &recipient.public_key).unwrap();
        // Tamper with the ciphertext
        if let Some(byte) = encrypted.ciphertext.get_mut(50) {
            *byte ^= 0xFF;
        }
        // Decrypting tampered data should fail
        assert!(HybridEncryption::decrypt(&encrypted, &recipient.private_key).is_err());
    }

    #[test]
    fn test_hkdf_deterministic() {
        let ikm = b"input key material";
        let info = b"test info";
        let key1 = hkdf_sha256(ikm, None, info);
        let key2 = hkdf_sha256(ikm, None, info);
        assert_eq!(key1, key2);

        // Different info → different key
        let key3 = hkdf_sha256(ikm, None, b"other info");
        assert_ne!(key1, key3);
    }
}
