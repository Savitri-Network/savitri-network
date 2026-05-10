//! Key management for Savitri Network
//! 
//! This module provides key generation, storage, and management utilities
//! for cryptographic keys used throughout the Savitri ecosystem.

use ed25519_dalek::{SigningKey as Keypair, VerifyingKey as PublicKey, Signature};
use crate::crypto::hash;

/// Key pair with additional metadata
#[derive(Debug, Clone)]
pub struct KeyPair {
    pub keypair: Keypair,
    pub public_key: PublicKey,
    pub key_id: String,
    pub created_at: u64,
}

impl KeyPair {
    /// Generate a new key pair with metadata
    pub fn new() -> Self {
        let keypair = crate::crypto::signature::generate_keypair();
        let public_key = keypair.verifying_key();
        let key_id = Self::generate_key_id(&public_key);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            keypair,
            public_key,
            key_id,
            created_at,
        }
    }

    /// Generate key ID from public key
    fn generate_key_id(public_key: &PublicKey) -> String {
        let pk_bytes = crate::crypto::signature::public_key_to_bytes(public_key);
        let hash = hash::sha256(&pk_bytes);
        hex::encode(&hash[..16]) // Use first 16 bytes of hash as ID
    }

    /// Get the public key bytes
    pub fn public_key_bytes(&self) -> [u8; 32] {
        crate::crypto::signature::public_key_to_bytes(&self.public_key)
    }

    /// Get the keypair bytes
    pub fn keypair_bytes(&self) -> [u8; 64] {
        crate::crypto::signature::keypair_to_bytes(&self.keypair)
    }

    /// Sign a message
    pub fn sign(&self, message: &[u8]) -> Signature {
        crate::crypto::signature::sign(message, &self.keypair)
    }

    /// Verify a message with this public key
    pub fn verify(&self, message: &[u8], signature: &Signature) -> bool {
        crate::crypto::signature::verify(message, signature, &self.public_key)
    }
}

/// Key storage interface
pub trait KeyStorage {
    fn store_key(&mut self, key_id: &str, keypair: &KeyPair) -> Result<(), Box<dyn std::error::Error>>;
    fn load_key(&self, key_id: &str) -> Result<Option<KeyPair>, Box<dyn std::error::Error>>;
    fn list_keys(&self) -> Result<Vec<String>, Box<dyn std::error::Error>>;
    fn delete_key(&mut self, key_id: &str) -> Result<bool, Box<dyn std::error::Error>>;
}

/// In-memory key storage (for testing and development)
#[derive(Debug, Default)]
pub struct MemoryKeyStorage {
    keys: std::collections::HashMap<String, KeyPair>,
}

impl MemoryKeyStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KeyStorage for MemoryKeyStorage {
    fn store_key(&mut self, key_id: &str, keypair: &KeyPair) -> Result<(), Box<dyn std::error::Error>> {
        self.keys.insert(key_id.to_string(), keypair.clone());
        Ok(())
    }

    fn load_key(&self, key_id: &str) -> Result<Option<KeyPair>, Box<dyn std::error::Error>> {
        Ok(self.keys.get(key_id).cloned())
    }

    fn list_keys(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(self.keys.keys().cloned().collect())
    }

    fn delete_key(&mut self, key_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.keys.remove(key_id).is_some())
    }
}

/// Key manager that handles key operations
#[derive(Debug)]
pub struct KeyManager<S: KeyStorage> {
    storage: S,
    default_key_id: Option<String>,
}

impl<S: KeyStorage> KeyManager<S> {
    /// Create a new key manager with given storage
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            default_key_id: None,
        }
    }

    /// Generate and store a new key
    pub fn generate_key(&mut self, key_id: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
        let keypair = KeyPair::new();
        let id = key_id.unwrap_or(&keypair.key_id);
        
        self.storage.store_key(id, &keypair)?;
        
        if self.default_key_id.is_none() {
            self.default_key_id = Some(id.to_string());
        }
        
        Ok(id.to_string())
    }

    /// Load a key by ID
    pub fn load_key(&self, key_id: &str) -> Result<Option<KeyPair>, Box<dyn std::error::Error>> {
        self.storage.load_key(key_id)
    }

    /// Get the default key
    pub fn get_default_key(&self) -> Result<Option<KeyPair>, Box<dyn std::error::Error>> {
        if let Some(ref default_id) = self.default_key_id {
            self.storage.load_key(default_id)
        } else {
            Ok(None)
        }
    }

    /// Set the default key
    pub fn set_default_key(&mut self, key_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        if self.storage.load_key(key_id)?.is_some() {
            self.default_key_id = Some(key_id.to_string());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// List all stored keys
    pub fn list_keys(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        self.storage.list_keys()
    }

    /// Delete a key
    pub fn delete_key(&mut self, key_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let deleted = self.storage.delete_key(key_id)?;
        
        // Clear default key if it was deleted
        if let Some(ref default_id) = self.default_key_id {
            if default_id == key_id {
                self.default_key_id = None;
            }
        }
        
        Ok(deleted)
    }

    /// Sign with default key
    pub fn sign_with_default(&self, message: &[u8]) -> Result<Option<Signature>, Box<dyn std::error::Error>> {
        if let Some(keypair) = self.get_default_key()? {
            Ok(Some(keypair.sign(message)))
        } else {
            Ok(None)
        }
    }

    /// Verify with a specific key
    pub fn verify_with_key(&self, key_id: &str, message: &[u8], signature: &Signature) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(keypair) = self.storage.load_key(key_id)? {
            Ok(keypair.verify(message, signature))
        } else {
            Ok(false)
        }
    }
}

/// Key derivation utilities
pub mod derivation {
    use super::*;
    use crate::crypto::hash;
    use hmac::{Hmac, Mac};
    use sha2::Sha512;

    /// Derive a child key from a parent key and a path
    /// Uses deterministic key derivation based on HMAC-SHA512
    pub fn derive_child_key(parent_key: &KeyPair, path: &str) -> KeyPair {
        let parent_bytes = parent_key.keypair_bytes();
        let path_bytes = path.as_bytes();
        
        // Use HMAC-SHA512 for proper key derivation
        let mut hmac = Hmac::<Sha512>::new_from_slice(&parent_bytes).expect("HMAC key init");
        hmac.update(path_bytes);
        let result = hmac.finalize().into_bytes();

        // Use the HMAC result as seed for deterministic key generation
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(&result[..32]);
        key_bytes[32..].copy_from_slice(&result[32..64]);

        // SECURITY: Never fall back to random key generation — that would break
        // deterministic derivation and make wallet recovery impossible.
        let keypair = match crate::crypto::signature::keypair_from_bytes(&key_bytes) {
            Ok(kp) => kp,
            Err(_) => {
                // If first attempt fails, re-hash and try once more
                let fallback_seed = crate::crypto::hash::sha512(&result);
                key_bytes[..32].copy_from_slice(&fallback_seed[..32]);
                key_bytes[32..].copy_from_slice(&fallback_seed[32..64]);

                crate::crypto::signature::keypair_from_bytes(&key_bytes)
                    .expect("Deterministic key derivation failed after fallback hash — this is a bug")
            }
        };
        
        let public_key = keypair.verifying_key();
        let key_id = KeyPair::generate_key_id(&public_key);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        KeyPair {
            keypair,
            public_key,
            key_id,
            created_at,
        }
    }

    /// Derive multiple keys from a master key
    pub fn derive_key_hierarchy(master_key: &KeyPair, paths: &[&str]) -> Vec<KeyPair> {
        paths.iter()
            .map(|path| derive_child_key(master_key, path))
            .collect()
    }

    /// Derive a key with additional context (for enhanced security)
    pub fn derive_child_key_with_context(parent_key: &KeyPair, path: &str, context: &[u8]) -> KeyPair {
        let parent_bytes = parent_key.keypair_bytes();
        let path_bytes = path.as_bytes();
        
        // Include context in the derivation
        let mut hmac = Hmac::<Sha512>::new_from_slice(&parent_bytes).expect("HMAC key init");
        hmac.update(path_bytes);
        hmac.update(context);
        let result = hmac.finalize().into_bytes();
        
        let mut key_bytes = [0u8; 64];
        key_bytes[..32].copy_from_slice(&result[..32]);
        key_bytes[32..].copy_from_slice(&result[32..64]);
        
        // SECURITY: Never fall back to random key — breaks deterministic derivation
        let keypair = crate::crypto::signature::keypair_from_bytes(&key_bytes)
            .expect("Deterministic key derivation with context failed — this is a bug");
        
        let public_key = keypair.verifying_key();
        let key_id = KeyPair::generate_key_id(&public_key);
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        KeyPair {
            keypair,
            public_key,
            key_id,
            created_at,
        }
    }

    /// Validate that a derived key matches the expected derivation
    pub fn validate_derivation(parent_key: &KeyPair, path: &str, derived_key: &KeyPair) -> bool {
        let expected_key = derive_child_key(parent_key, path);
        expected_key.key_id == derived_key.key_id &&
        expected_key.public_key_bytes() == derived_key.public_key_bytes()
    }
}

pub mod validation {
    use super::*;
    use ed25519_dalek::Signer;

    /// Validate that a keypair is properly formed
    pub fn validate_keypair(keypair_bytes: &[u8; 64]) -> bool {
        // First, try to parse with the existing function
        if crate::crypto::signature::keypair_from_bytes(keypair_bytes).is_err() {
            return false;
        }
        
        // All zeros is invalid
        if keypair_bytes.iter().all(|&b| b == 0) {
            return false;
        }
        
        // All 0xFF is invalid (unlikely to be a valid Ed25519 keypair)
        if keypair_bytes.iter().all(|&b| b == 0xFF) {
            return false;
        }
        
        // Check for repeated patterns that are unlikely to be valid
        let first_32 = &keypair_bytes[..32];
        let second_32 = &keypair_bytes[32..];
        
        // Private and public keys should not be identical
        if first_32 == second_32 {
            return false;
        }
        
        // Additional check: try to create a signing key and verify it works
        match crate::crypto::signature::keypair_from_bytes(keypair_bytes) {
            Ok(keypair) => {
                // Try to sign a test message
                let test_message = b"validation_test";
                let signature = keypair.sign(test_message);
                
                // Verify the signature with the derived public key
                crate::crypto::signature::verify_with_security_level(
                    test_message,
                    &crate::crypto::signature::signature_to_bytes(&signature),
                    &crate::crypto::signature::public_key_to_bytes(&keypair.verifying_key()),
                )
            }
            Err(_) => false,
        }
    }

    /// Validate that a public key is properly formed
    pub fn validate_public_key(public_key_bytes: &[u8; 32]) -> bool {
        // First, try to parse with the existing function
        if crate::crypto::signature::public_key_from_bytes(public_key_bytes).is_err() {
            return false;
        }
        
        // All zeros is invalid
        if public_key_bytes.iter().all(|&b| b == 0) {
            return false;
        }
        
        // All 0xFF is invalid (unlikely to be a valid Ed25519 public key)
        if public_key_bytes.iter().all(|&b| b == 0xFF) {
            return false;
        }
        
        // Try to use the public key for verification
        match crate::crypto::signature::public_key_from_bytes(public_key_bytes) {
            Ok(_public_key) => {
                // Create a test signature and try to verify it
                let test_message = b"validation_test";
                let test_keypair = crate::crypto::signature::generate_keypair();
                let test_signature = test_keypair.sign(test_message);
                
                // This should fail since we're using a different public key
                // But the fact that it doesn't panic means the public key is well-formed
                !crate::crypto::signature::verify_with_security_level(
                    test_message,
                    &crate::crypto::signature::signature_to_bytes(&test_signature),
                    public_key_bytes,
                )
            }
            Err(_) => false,
        }
    }

    /// Validate that a signature is properly formed
    pub fn validate_signature(signature_bytes: &[u8; 64]) -> bool {
        // First, try to parse with the existing function
        if crate::crypto::signature::signature_from_bytes(signature_bytes).is_err() {
            return false;
        }
        
        // All zeros is invalid
        if signature_bytes.iter().all(|&b| b == 0) {
            return false;
        }
        
        // All 0xFF is invalid (unlikely to be a valid Ed25519 signature)
        if signature_bytes.iter().all(|&b| b == 0xFF) {
            return false;
        }
        
        // Try to use the signature for verification
        match crate::crypto::signature::signature_from_bytes(signature_bytes) {
            Ok(_signature) => {
                // Create a test message and keypair
                let test_message = b"validation_test";
                let test_keypair = crate::crypto::signature::generate_keypair();
                
                // This should fail since we're using a different signature
                // But the fact that it doesn't panic means the signature is well-formed
                !crate::crypto::signature::verify_with_security_level(
                    test_message,
                    signature_bytes,
                    &crate::crypto::signature::public_key_to_bytes(&test_keypair.verifying_key()),
                )
            }
            Err(_) => false,
        }
    }

    /// Check if a key ID matches a public key
    pub fn validate_key_id(public_key: &PublicKey, key_id: &str) -> bool {
        let expected_id = KeyPair::generate_key_id(public_key);
        expected_id == key_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keypair_generation() {
        let keypair = KeyPair::new();
        
        assert!(!keypair.key_id.is_empty());
        assert!(keypair.created_at > 0);
        assert_eq!(keypair.key_id.len(), 32); // 16 bytes * 2 hex chars
    }

    #[test]
    fn test_keypair_signing() {
        let keypair = KeyPair::new();
        let message = b"test message";
        
        let signature = keypair.sign(message);
        assert!(keypair.verify(message, &signature));
        
        let wrong_message = b"wrong message";
        assert!(!keypair.verify(wrong_message, &signature));
    }

    #[test]
    fn test_memory_key_storage() {
        let mut storage = MemoryKeyStorage::new();
        let keypair = KeyPair::new();
        
        // Store key
        storage.store_key(&keypair.key_id, &keypair).unwrap();
        
        // Load key
        let loaded = storage.load_key(&keypair.key_id).unwrap();
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().key_id, keypair.key_id);
        
        // List keys
        let keys = storage.list_keys().unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], keypair.key_id);
        
        // Delete key
        let deleted = storage.delete_key(&keypair.key_id).unwrap();
        assert!(deleted);
        
        let loaded_after_delete = storage.load_key(&keypair.key_id).unwrap();
        assert!(loaded_after_delete.is_none());
    }

    #[test]
    fn test_key_manager() {
        let mut manager = KeyManager::new(MemoryKeyStorage::new());
        
        // Generate key
        let key_id = manager.generate_key(None).unwrap();
        
        // Load key
        let loaded = manager.load_key(&key_id).unwrap();
        assert!(loaded.is_some());
        
        // Default key should be set
        let default = manager.get_default_key().unwrap();
        assert!(default.is_some());
        assert_eq!(default.unwrap().key_id, key_id);
        
        // Sign with default
        let message = b"test message";
        let signature = manager.sign_with_default(message).unwrap();
        assert!(signature.is_some());
        
        // Verify with key
        let verified = manager.verify_with_key(&key_id, message, &signature.unwrap()).unwrap();
        assert!(verified);
    }

    #[test]
    fn test_key_derivation() {
        let master_key = KeyPair::new();
        let path = "child/1";
        
        let child_key = derivation::derive_child_key(&master_key, path);
        
        // Child key should be different from master
        assert_ne!(child_key.key_id, master_key.key_id);
        assert_ne!(child_key.public_key_bytes(), master_key.public_key_bytes());
        
        // Child key should be deterministic
        let child_key2 = derivation::derive_child_key(&master_key, path);
        assert_eq!(child_key.key_id, child_key2.key_id);
        assert_eq!(child_key.public_key_bytes(), child_key2.public_key_bytes());
    }

    #[test]
    fn test_key_hierarchy() {
        let master_key = KeyPair::new();
        let paths = vec!["account/1", "account/2", "device/1"];
        
        let hierarchy = derivation::derive_key_hierarchy(&master_key, &paths);
        
        assert_eq!(hierarchy.len(), 3);
        
        // All keys should be different
        for i in 0..hierarchy.len() {
            for j in (i + 1)..hierarchy.len() {
                assert_ne!(hierarchy[i].key_id, hierarchy[j].key_id);
            }
        }
        
        // Derivation should be deterministic
        let hierarchy2 = derivation::derive_key_hierarchy(&master_key, &paths);
        for (k1, k2) in hierarchy.iter().zip(hierarchy2.iter()) {
            assert_eq!(k1.key_id, k2.key_id);
        }
    }

    #[test]
    fn test_key_validation() {
        let keypair = KeyPair::new();
        
        // Validate valid keys
        assert!(validation::validate_keypair(&keypair.keypair_bytes()));
        assert!(validation::validate_public_key(&keypair.public_key_bytes()));
        
        let signature = keypair.sign(b"test");
        let signature_bytes = crate::crypto::signature::signature_to_bytes(&signature);
        assert!(validation::validate_signature(&signature_bytes));
        
        // Validate key ID
        assert!(validation::validate_key_id(&keypair.public_key, &keypair.key_id));
        
        // Test invalid keys
        let invalid_keypair = [0u8; 64]; // All zeros - invalid
        assert!(!validation::validate_keypair(&invalid_keypair));
        
        let invalid_keypair2 = [255u8; 64]; // All 0xFF - invalid
        assert!(!validation::validate_keypair(&invalid_keypair2));
        
        let invalid_keypair3 = {
            let mut keypair = [0u8; 64];
            // Make private and public keys identical (invalid)
            keypair[..32].copy_from_slice(&[42u8; 32]);
            keypair[32..].copy_from_slice(&[42u8; 32]);
            keypair
        };
        assert!(!validation::validate_keypair(&invalid_keypair3));
        
        let invalid_public_key = [0u8; 32]; // All zeros - invalid
        assert!(!validation::validate_public_key(&invalid_public_key));
        
        let invalid_public_key2 = [255u8; 32]; // All 0xFF - invalid
        assert!(!validation::validate_public_key(&invalid_public_key2));
        
        let invalid_signature = [0u8; 64]; // All zeros - invalid
        assert!(!validation::validate_signature(&invalid_signature));
        
        let invalid_signature2 = [255u8; 64]; // All 0xFF - invalid
        assert!(!validation::validate_signature(&invalid_signature2));
        
        // Test invalid key ID
        assert!(!validation::validate_key_id(&keypair.public_key, "invalid_id"));
    }
}
