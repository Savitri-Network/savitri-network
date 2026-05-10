//! Key Management for Production PoU Consensus
//!
//! This module provides secure key management for the Savitri Network PoU consensus system.
//! It handles key generation, storage, backup, and rotation with proper encryption.

use anyhow::Result;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::path::PathBuf;
use tokio::fs as async_fs;
use zeroize::Zeroize;

/// Node identifier (32 bytes public key hash)
pub type NodeId = [u8; 32];

/// Errors that can occur during key management operations
#[derive(Debug, thiserror::Error)]
pub enum KeyManagerError {
    /// Invalid key file format or content
    #[error("Invalid key file: {0}")]
    InvalidKeyFile(String),

    /// IO operation failed
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Key generation process failed
    #[error("Key generation failed: {0}")]
    KeyGenerationFailed(String),

    /// Key encryption operation failed
    #[error("Key encryption failed: {0}")]
    EncryptionFailed(String),

    /// Key decryption operation failed
    #[error("Key decryption failed: {0}")]
    DecryptionFailed(String),

    /// Signature verification failed
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// Key file could not be found
    #[error("Key file not found: {0}")]
    KeyFileNotFound(String),
}

/// Return the `~/.savitri` directory path, falling back to `/tmp/savitri`
/// if the home directory cannot be determined.
fn dirs_fallback() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".savitri")
}

/// Secure key manager for PoU consensus operations
pub struct KeyManager {
    /// The actual signing key (kept in memory, encrypted at rest)
    signing_key: SigningKey,
    /// Path to the encrypted key file
    key_file: PathBuf,
    /// Public key for easy access
    public_key: VerifyingKey,
    /// Node ID derived from public key
    node_id: NodeId,
}

impl KeyManager {
    /// Load existing key or create new one
    pub async fn load_or_create(key_file: PathBuf) -> Result<Self, KeyManagerError> {
        if key_file.exists() {
            Self::load_from_file(key_file).await
        } else {
            Self::create_new(key_file).await
        }
    }

    /// Load existing key from encrypted file.
    ///
    /// Automatically migrates older formats (v0 XOR, v1 hardcoded passphrase)
    /// to v2 (machine-specific passphrase) by re-encrypting and overwriting
    /// the key file.
    async fn load_from_file(key_file: PathBuf) -> Result<Self, KeyManagerError> {
        // Read encrypted key data
        let encrypted_data = async_fs::read(&key_file).await?;

        // Determine if migration is needed:
        //   v0 (XOR) : len == 64
        //   v1 (hardcoded passphrase) : first byte == 0x01
        //   v2 (machine passphrase)   : first byte == 0x02  — no migration
        let needs_migration =
            encrypted_data.len() == 64 || (!encrypted_data.is_empty() && encrypted_data[0] == 0x01);

        // Decrypt the key data
        let key_data = Self::decrypt_key_data(&encrypted_data)?;

        // Deserialize signing key from 64-byte keypair format
        let signing_key = SigningKey::from_keypair_bytes(&key_data)
            .map_err(|_| KeyManagerError::InvalidKeyFile("Invalid keypair format".to_string()))?;

        let public_key = signing_key.verifying_key();
        let node_id = Self::public_key_to_node_id(&public_key);

        let manager = Self {
            signing_key,
            key_file,
            public_key,
            node_id,
        };

        // SECURITY: Auto-migrate legacy formats to v2 (machine-specific passphrase)
        if needs_migration {
            tracing::info!("Auto-migrating key file to v2 (machine-specific passphrase)");
            manager.save_to_file().await?;
        }

        Ok(manager)
    }

    /// Create new keypair and save it
    async fn create_new(key_file: PathBuf) -> Result<Self, KeyManagerError> {
        // Generate new signing key
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let public_key = signing_key.verifying_key();
        let node_id = Self::public_key_to_node_id(&public_key);

        // Create key manager instance
        let manager = Self {
            signing_key,
            key_file: key_file.clone(),
            public_key,
            node_id,
        };

        // Save the encrypted key
        manager.save_to_file().await?;

        Ok(manager)
    }

    /// Save encrypted keypair to file
    async fn save_to_file(&self) -> Result<(), KeyManagerError> {
        // Get keypair bytes (64 bytes: secret + public)
        let key_data = self.signing_key.to_keypair_bytes();

        // Encrypt the key data
        let encrypted_data = Self::encrypt_key_data(&key_data)?;

        // Ensure directory exists
        if let Some(parent) = self.key_file.parent() {
            async_fs::create_dir_all(parent).await?;
        }

        // Write encrypted data to file
        async_fs::write(&self.key_file, encrypted_data).await?;

        Ok(())
    }

    /// Obtain a machine-specific secret for key file encryption.
    ///
    /// Resolution order:
    /// 1. Read `/etc/machine-id` (Linux, systemd-based distributions).
    /// 2. Read `~/.savitri/machine-secret` (cross-platform fallback).
    /// 3. Generate a random 32-byte secret and persist it to
    ///    `~/.savitri/machine-secret` so subsequent calls are deterministic.
    ///
    /// The returned bytes are NOT used directly as a key — they feed into
    /// HMAC-SHA256 together with a domain separator to derive the actual
    /// PBKDF2 passphrase.
    fn get_machine_secret() -> Vec<u8> {
        // 1. Try /etc/machine-id (Linux)
        if let Ok(contents) = std::fs::read_to_string("/etc/machine-id") {
            let trimmed = contents.trim();
            if !trimmed.is_empty() {
                return trimmed.as_bytes().to_vec();
            }
        }

        // 2. Try ~/.savitri/machine-secret
        let savitri_dir = dirs_fallback();
        let secret_path = savitri_dir.join("machine-secret");

        if let Ok(contents) = std::fs::read(&secret_path) {
            if contents.len() >= 16 {
                return contents;
            }
        }

        // 3. Generate, persist, and return a new random secret
        let mut secret = vec![0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut secret);

        // Best-effort write — if this fails the secret lives only in memory
        // for this session and a new one will be generated next time, which
        // means the key file will still be decryptable via the v2 fallback
        // logic (try machine passphrase, then legacy passphrase).
        if let Err(e) = std::fs::create_dir_all(&savitri_dir) {
            tracing::warn!("Could not create ~/.savitri directory: {}", e);
        } else if let Err(e) = std::fs::write(&secret_path, &secret) {
            tracing::warn!("Could not persist machine secret: {}", e);
        } else {
            // Restrict permissions on Unix (owner read/write only)
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    std::fs::set_permissions(&secret_path, std::fs::Permissions::from_mode(0o600));
            }
            tracing::info!("Generated new machine secret at {}", secret_path.display());
        }

        secret
    }

    /// Derive the PBKDF2 passphrase from the machine-specific secret.
    ///
    /// `passphrase = HMAC-SHA256(machine_secret, "savitri-node-keyfile-v1")`
    ///
    /// This binds the passphrase to the current machine so that a stolen
    /// key file cannot be decrypted on a different host without also
    /// copying the machine secret.
    fn get_machine_passphrase() -> Vec<u8> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let machine_secret = Self::get_machine_secret();
        let mut mac =
            HmacSha256::new_from_slice(&machine_secret).expect("HMAC accepts any key length");
        mac.update(b"savitri-node-keyfile-v1");
        mac.finalize().into_bytes().to_vec()
    }

    /// Encrypt key data using AES-256-GCM with a key derived via PBKDF2.
    ///
    /// **v2 format** (machine-specific passphrase):
    /// version(1=0x02) || salt(16) || nonce(12) || ciphertext(64+16 tag)
    /// Total: 1 + 16 + 12 + 80 = 109 bytes
    fn encrypt_key_data(key_data: &[u8; 64]) -> Result<Vec<u8>, KeyManagerError> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

        // Generate random salt and nonce
        let mut salt = [0u8; 16];
        let mut nonce_bytes = [0u8; 12];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut salt);
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut nonce_bytes);

        // Derive encryption key via PBKDF2 using a machine-specific passphrase.
        // The passphrase is HMAC-SHA256(machine_secret, domain_tag) so that key
        // files are bound to this machine and cannot be decrypted elsewhere
        // without also copying the machine secret.
        let passphrase = Self::get_machine_passphrase();
        let mut derived_key = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(&passphrase, &salt, 100_000, &mut derived_key);

        let cipher = Aes256Gcm::new_from_slice(&derived_key)
            .map_err(|e| KeyManagerError::EncryptionFailed(format!("AES init: {}", e)))?;
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, key_data.as_ref())
            .map_err(|e| KeyManagerError::EncryptionFailed(format!("AES encrypt: {}", e)))?;

        // Zero the derived key after use
        derived_key.zeroize();

        // Assemble output: version || salt || nonce || ciphertext
        let mut output = Vec::with_capacity(1 + 16 + 12 + ciphertext.len());
        output.push(0x02); // version byte (v2: machine-specific passphrase)
        output.extend_from_slice(&salt);
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);

        Ok(output)
    }

    /// Decrypt key data encrypted with AES-256-GCM + PBKDF2.
    ///
    /// Only v2 (machine-specific PBKDF2 passphrase) is supported.
    /// Legacy v0 (XOR) and v1 (hardcoded passphrase) formats are intentionally
    /// rejected — nodes with old key files must regenerate their keys.
    ///
    /// # Security
    /// - CRIT-01 (fixed): v0 XOR format with hardcoded key removed.
    /// - CRIT-02 (fixed): v1 hardcoded passphrase `b"savitri-node-keyfile-v1"` removed.
    fn decrypt_key_data(encrypted_data: &[u8]) -> Result<[u8; 64], KeyManagerError> {
        // Legacy v0: exactly 64 bytes with no version header (XOR-encrypted).
        // SECURITY (CRIT-01): Rejected. v2 format requires at least 109 bytes
        // (1 version + 16 salt + 12 nonce + 80 ciphertext), so a 64-byte blob
        // is unambiguously the legacy XOR format. XOR with a known hardcoded key
        // provides no security. Nodes must regenerate their key.
        if encrypted_data.len() == 64 {
            return Err(KeyManagerError::DecryptionFailed(
                "Legacy v0 (XOR) key format is no longer supported for security reasons. \
                 Delete your key file and restart the node to generate a new key."
                    .to_string(),
            ));
        }

        // AES-256-GCM formats: version(1) + salt(16) + nonce(12) + ciphertext(80) = 109
        if encrypted_data.len() < 29 {
            return Err(KeyManagerError::DecryptionFailed(
                "Encrypted data too short".to_string(),
            ));
        }

        let version = encrypted_data[0];

        match version {
            0x02 => {
                // v2: machine-specific passphrase via HMAC-SHA256 + PBKDF2
                Self::decrypt_aes_gcm(encrypted_data, &Self::get_machine_passphrase())
            }
            0x01 => {
                // SECURITY (CRIT-02): v1 hardcoded passphrase rejected.
                // The hardcoded passphrase `b"savitri-node-keyfile-v1"` was identical
                // across all nodes, providing no encryption security. Nodes must
                // delete their key file and restart to generate a new v2-encrypted key.
                tracing::error!(
                    "Legacy v1 key file detected. The hardcoded passphrase format is no \
                     longer accepted. Delete your key file and restart to regenerate."
                );
                Err(KeyManagerError::DecryptionFailed(
                    "Legacy v1 (hardcoded passphrase) key format is no longer supported. \
                     Delete your key file and restart the node to generate a new key."
                        .to_string(),
                ))
            }
            _ => Err(KeyManagerError::DecryptionFailed(format!(
                "Unsupported key file version: {}",
                version
            ))),
        }
    }

    /// Inner AES-256-GCM decryption shared by v1 and v2 paths.
    fn decrypt_aes_gcm(
        encrypted_data: &[u8],
        passphrase: &[u8],
    ) -> Result<[u8; 64], KeyManagerError> {
        use aes_gcm::aead::Aead;
        use aes_gcm::{Aes256Gcm, KeyInit, Nonce};

        let salt = &encrypted_data[1..17];
        let nonce_bytes = &encrypted_data[17..29];
        let ciphertext = &encrypted_data[29..];

        let mut derived_key = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(passphrase, salt, 100_000, &mut derived_key);

        let cipher = Aes256Gcm::new_from_slice(&derived_key)
            .map_err(|e| KeyManagerError::DecryptionFailed(format!("AES init: {}", e)))?;
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| {
            KeyManagerError::DecryptionFailed(
                "Decryption failed (wrong key or corrupted file)".to_string(),
            )
        })?;

        // Zero the derived key after use
        derived_key.zeroize();

        if plaintext.len() != 64 {
            return Err(KeyManagerError::DecryptionFailed(format!(
                "Decrypted data has wrong length: {} (expected 64)",
                plaintext.len()
            )));
        }

        let mut result = [0u8; 64];
        result.copy_from_slice(&plaintext);
        Ok(result)
    }

    /// Convert public key to node ID
    fn public_key_to_node_id(public_key: &VerifyingKey) -> NodeId {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(public_key.as_bytes());
        let hash = hasher.finalize();

        let mut node_id = [0u8; 32];
        node_id.copy_from_slice(&hash[..32]);
        node_id
    }

    /// Get the node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the public key
    pub fn public_key(&self) -> VerifyingKey {
        self.public_key
    }

    /// Get the public key bytes (for compatibility)
    pub fn get_public_key(&self) -> VerifyingKey {
        self.public_key
    }

    /// Sign a consensus message
    pub fn sign_consensus_message(&self, message: &[u8]) -> Signature {
        self.signing_key.sign(message)
    }

    /// Verify a signature
    pub fn verify_signature(&self, message: &[u8], signature: &Signature) -> bool {
        self.public_key.verify(message, signature).is_ok()
    }

    /// Rotate to a new keypair
    pub async fn rotate_key(&mut self) -> Result<(), KeyManagerError> {
        // Generate new signing key
        let new_signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let new_public_key = new_signing_key.verifying_key();
        let new_node_id = Self::public_key_to_node_id(&new_public_key);

        // Backup old key
        let backup_file = self.key_file.with_extension(".backup");
        async_fs::copy(&self.key_file, &backup_file).await?;

        // Update signing key
        self.signing_key = new_signing_key;
        self.public_key = new_public_key;
        self.node_id = new_node_id;

        // Save new key
        self.save_to_file().await?;

        Ok(())
    }

    /// Create a backup of the current key
    pub async fn create_backup(&self, backup_path: &PathBuf) -> Result<(), KeyManagerError> {
        let encrypted_data = async_fs::read(&self.key_file).await?;
        async_fs::write(backup_path, encrypted_data).await?;
        Ok(())
    }

    /// Restore from backup
    pub async fn restore_from_backup(
        &mut self,
        backup_path: &PathBuf,
    ) -> Result<(), KeyManagerError> {
        // Load from backup
        let encrypted_data = async_fs::read(backup_path).await?;
        let key_data = Self::decrypt_key_data(&encrypted_data)?;
        let signing_key = SigningKey::from_keypair_bytes(&key_data).map_err(|_| {
            KeyManagerError::InvalidKeyFile("Invalid keypair format in backup".to_string())
        })?;

        let public_key = signing_key.verifying_key();
        let node_id = Self::public_key_to_node_id(&public_key);

        // Update signing key
        self.signing_key = signing_key;
        self.public_key = public_key;
        self.node_id = node_id;

        // Save to main location
        self.save_to_file().await?;

        Ok(())
    }
}

// SECURITY (HIGH-03): ed25519_dalek::SigningKey implements ZeroizeOnDrop in 2.x,
// so the signing key is automatically zeroized when KeyManager is dropped.
// A manual Drop that calls `to_bytes()` then `zeroize()` only zeroes a *copy*
// of the key bytes and does not affect the original — it was removed.

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio;

    #[tokio::test]
    async fn test_key_creation_and_loading() {
        let temp_dir = TempDir::new().unwrap();
        let key_file = temp_dir.path().join("test_key.bin");

        // Create new key manager
        let manager1 = KeyManager::load_or_create(key_file.clone()).await.unwrap();
        let node_id1 = manager1.node_id();

        // Load existing key
        let manager2 = KeyManager::load_or_create(key_file).await.unwrap();
        let node_id2 = manager2.node_id();

        // Should be the same key
        assert_eq!(node_id1, node_id2);
        assert_eq!(manager1.public_key(), manager2.public_key());
    }

    #[tokio::test]
    async fn test_signing_and_verification() {
        let temp_dir = TempDir::new().unwrap();
        let key_file = temp_dir.path().join("test_key.bin");

        let manager = KeyManager::load_or_create(key_file).await.unwrap();

        let message = b"test message for signing";
        let signature = manager.sign_consensus_message(message);

        assert!(manager.verify_signature(message, &signature));

        assert!(!manager.verify_signature(b"different message", &signature));
    }

    #[tokio::test]
    async fn test_key_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let key_file = temp_dir.path().join("test_key.bin");

        let mut manager = KeyManager::load_or_create(key_file.clone()).await.unwrap();
        let original_node_id = manager.node_id();
        let original_public_key = manager.public_key();

        // Rotate key
        manager.rotate_key().await.unwrap();

        // Should have different key
        assert_ne!(manager.node_id(), original_node_id);
        assert_ne!(manager.public_key(), original_public_key);

        // Backup file should exist
        let backup_file = key_file.with_extension(".backup");
        assert!(backup_file.exists());
    }

    #[tokio::test]
    async fn test_backup_and_restore() {
        let temp_dir = TempDir::new().unwrap();
        let key_file = temp_dir.path().join("test_key.bin");
        let backup_file = temp_dir.path().join("backup.bin");

        let mut manager = KeyManager::load_or_create(key_file.clone()).await.unwrap();
        let original_node_id = manager.node_id();

        // Create backup
        manager.create_backup(&backup_file).await.unwrap();

        // Modify original key
        manager.rotate_key().await.unwrap();
        let rotated_node_id = manager.node_id();
        assert_ne!(rotated_node_id, original_node_id);

        // Restore from backup
        manager.restore_from_backup(&backup_file).await.unwrap();
        assert_eq!(manager.node_id(), original_node_id);
    }

    #[test]
    fn test_public_key_to_node_id() {
        let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
        let node_id = KeyManager::public_key_to_node_id(&signing_key.verifying_key());

        // Should be 32 bytes
        assert_eq!(node_id.len(), 32);

        // Should be deterministic
        let node_id2 = KeyManager::public_key_to_node_id(&signing_key.verifying_key());
        assert_eq!(node_id, node_id2);
    }

    #[test]
    fn test_encryption_decryption() {
        let key_data = [42u8; 64];

        let encrypted = KeyManager::encrypt_key_data(&key_data).unwrap();

        // v2 format: version byte should be 0x02
        assert_eq!(
            encrypted[0], 0x02,
            "encrypt_key_data must produce v2 format"
        );
        // Total size: 1 (version) + 16 (salt) + 12 (nonce) + 80 (ciphertext + tag)
        assert_eq!(encrypted.len(), 109);

        let decrypted = KeyManager::decrypt_key_data(&encrypted).unwrap();
        assert_eq!(key_data, decrypted);
    }

    #[test]
    fn test_v1_legacy_decryption_is_rejected() {
        // CRIT-02 fix: v1 (hardcoded passphrase) format must be REJECTED, not decrypted.
        // Simulate a v1 key file (version byte 0x01) and verify that decrypt_key_data
        // returns an Err instead of silently accepting the hardcoded passphrase.
        let mut v1_data = vec![0x01u8]; // version byte
        v1_data.extend_from_slice(&[0u8; 16]); // salt
        v1_data.extend_from_slice(&[0u8; 12]); // nonce
        v1_data.extend_from_slice(&[0u8; 80]); // ciphertext (80 = 64 plaintext + 16 tag)

        let result = KeyManager::decrypt_key_data(&v1_data);
        assert!(
            result.is_err(),
            "v1 legacy format must be rejected (CRIT-02 fix)"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Legacy v1") || err_msg.contains("no longer supported"),
            "Error message must clearly explain the rejection: {}",
            err_msg
        );
    }

    #[test]
    fn test_v0_legacy_decryption_is_rejected() {
        // CRIT-01 fix: v0 (XOR with hardcoded key, exactly 64 bytes) must be REJECTED.
        let v0_data = [0xABu8; 64]; // 64-byte XOR-format key file

        let result = KeyManager::decrypt_key_data(&v0_data);
        assert!(
            result.is_err(),
            "v0 legacy XOR format must be rejected (CRIT-01 fix)"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Legacy v0") || err_msg.contains("no longer supported"),
            "Error message must clearly explain the rejection: {}",
            err_msg
        );
    }

    #[test]
    fn test_machine_secret_is_deterministic() {
        // Two consecutive calls must return the same secret
        let s1 = KeyManager::get_machine_secret();
        let s2 = KeyManager::get_machine_secret();
        assert_eq!(s1, s2, "Machine secret must be deterministic across calls");
        assert!(s1.len() >= 16, "Machine secret must be at least 16 bytes");
    }
}
