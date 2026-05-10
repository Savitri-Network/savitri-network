//! Encryption utilities for Savitri Network
//! 
//! This module provides encryption and decryption utilities for secure data
//! transmission and storage throughout the Savitri ecosystem.

use sha2::Digest;
use rand::Rng;
use crate::crypto::hash;
use rand::rngs::OsRng;
use rand::RngCore;
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit};

// Use AesGcmCipher for all encryption needs.

/// AES-256 GCM cipher for production-grade encryption.
///
/// SECURITY: A fresh random nonce is generated for each encryption operation.
/// The nonce is prepended to the ciphertext output and extracted on decryption.
pub struct AesGcmCipher {
    cipher: Aes256Gcm,
}

impl AesGcmCipher {
    /// Create a new AES-256 GCM cipher with the given key
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
        Self { cipher }
    }

    /// Encrypt data with AES-256 GCM.
    /// Returns: nonce (12 bytes) || ciphertext || tag (16 bytes)
    /// SECURITY: A fresh random nonce is generated for each call.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);

        // Aead::encrypt returns ciphertext || tag
        let ciphertext_with_tag = self.cipher.encrypt(&nonce, plaintext)
            .map_err(|_| "AES-GCM encryption failed")?;

        // Return: nonce || ciphertext || tag
        let mut result = Vec::with_capacity(12 + ciphertext_with_tag.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext_with_tag);
        Ok(result)
    }

    /// Decrypt data with AES-256 GCM.
    /// Expects: nonce (12 bytes) || ciphertext || tag (16 bytes)
    pub fn decrypt(&self, encrypted_data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        if encrypted_data.len() < 28 { // 12 (nonce) + 0 (min ciphertext) + 16 (tag)
            return Err("Invalid encrypted data length".into());
        }

        let nonce = Nonce::from_slice(&encrypted_data[..12]);
        let ciphertext_with_tag = &encrypted_data[12..];

        self.cipher.decrypt(nonce, ciphertext_with_tag)
            .map_err(|_| "Decryption failed".into())
    }
}

/// Password-based key derivation
pub struct Pbkdf2Params {
    pub iterations: u32,
    pub salt_length: usize,
    pub key_length: usize,
}

impl Default for Pbkdf2Params {
    fn default() -> Self {
        Self {
            iterations: 100_000,
            salt_length: 32,
            key_length: 32,
        }
    }
}

/// Simple PBKDF2 implementation (for core library)
/// For production, use proper crypto libraries like argon2
pub fn derive_key_pbkdf2(password: &str, salt: &[u8], params: &Pbkdf2Params) -> Vec<u8> {
    let mut result = vec![0u8; params.key_length];
    let mut current = Vec::new();
    
    // Simple iterative hashing (not real PBKDF2, but functional for core library)
    for i in 1..=params.iterations {
        let mut hasher = sha2::Sha256::new();
        hasher.update(password.as_bytes());
        hasher.update(salt);
        hasher.update(i.to_le_bytes());
        if !current.is_empty() {
            hasher.update(&current);
        }
        current = hasher.finalize().to_vec();
        
        // XOR with result
        for (j, &byte) in current.iter().enumerate() {
            if j < result.len() {
                result[j] ^= byte;
            }
        }
    }
    
    result
}

/// Improved PBKDF2 implementation using HMAC-SHA256
pub fn derive_key_pbkdf2_improved(password: &str, salt: &[u8], params: &Pbkdf2Params) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    
    let mut result = vec![0u8; params.key_length];
    let mut current = Vec::new();
    
    for i in 1..=params.iterations {
        let mut hmac = <Hmac<Sha256> as Mac>::new_from_slice(password.as_bytes()).expect("HMAC key init");
        hmac.update(salt);
        hmac.update(&i.to_le_bytes());
        if i > 1 {
            hmac.update(&current);
        }
        current = hmac.finalize().into_bytes().to_vec();
        
        // XOR with result
        for (j, &byte) in current.iter().enumerate() {
            if j < result.len() {
                result[j] ^= byte;
            }
        }
    }
    
    result
}

/// Generate a random salt
pub fn generate_salt(length: usize) -> Vec<u8> {
    let mut rng = OsRng;
    let mut salt = vec![0u8; length];
    rng.fill_bytes(&mut salt);
    salt
}

/// Encrypt data with password-derived key using HMAC-SHA256 PBKDF2 + AES-256-GCM.
pub fn encrypt_with_password(data: &[u8], password: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let params = Pbkdf2Params::default();
    let salt = generate_salt(params.salt_length);
    let key = derive_key_pbkdf2_improved(password, &salt, &params);
    
    let cipher = AesGcmCipher::new(&key.try_into().map_err(|_| "Invalid key length")?);
    let encrypted = cipher.encrypt(data)?;

    // Format: salt_length (1 byte) + salt + encrypted_data
    let mut result = Vec::new();
    result.push(params.salt_length as u8);
    result.extend_from_slice(&salt);
    result.extend_from_slice(&encrypted);
    
    Ok(result)
}

/// Decrypt data with password-derived key using HMAC-SHA256 PBKDF2 + AES-256-GCM.
pub fn decrypt_with_password(encrypted_data: &[u8], password: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if encrypted_data.is_empty() {
        return Err("Empty encrypted data".into());
    }
    
    let salt_length = encrypted_data[0] as usize;
    if encrypted_data.len() < 1 + salt_length {
        return Err("Invalid encrypted data format".into());
    }
    
    let salt = &encrypted_data[1..1 + salt_length];
    let encrypted = &encrypted_data[1 + salt_length..];
    
    let params = Pbkdf2Params {
        salt_length,
        ..Default::default()
    };
    
    let key = derive_key_pbkdf2_improved(password, salt, &params);
    let cipher = AesGcmCipher::new(&key.try_into().map_err(|_| "Invalid key length")?);
    let decrypted = cipher.decrypt(encrypted)?;
    
    Ok(decrypted)
}

/// Secure random number generator
pub struct SecureRng {
    rng: OsRng,
}

impl SecureRng {
    /// Create a new secure RNG
    pub fn new() -> Self {
        Self {
            rng: OsRng,
        }
    }

    /// Generate random bytes
    pub fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.rng.fill_bytes(dest);
    }

    /// Generate a random u64
    pub fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Generate a random u32
    pub fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    /// Generate a random number in range
    pub fn gen_range(&mut self, range: std::ops::Range<u64>) -> u64 {
        self.rng.gen_range(range)
    }
}

impl Default for SecureRng {
    fn default() -> Self {
        Self::new()
    }
}

/// Utility functions for secure data handling
pub mod utils {
    use super::*;

    /// Securely compare two byte arrays in constant time.
    ///
    /// SECURITY: Does not leak length information via timing. Both slices
    /// are padded to the same length before comparison.
    pub fn constant_time_compare(a: &[u8], b: &[u8]) -> bool {
        let max_len = a.len().max(b.len());
        let mut result = (a.len() ^ b.len()) as u8; // non-zero if lengths differ

        for i in 0..max_len {
            let byte_a = if i < a.len() { a[i] } else { 0 };
            let byte_b = if i < b.len() { b[i] } else { 0 };
            result |= byte_a ^ byte_b;
        }

        result == 0
    }

    /// Zero out sensitive data
    pub fn zeroize(data: &mut [u8]) {
        for byte in data.iter_mut() {
            *byte = 0;
        }
    }

    /// Generate a secure random nonce
    pub fn generate_nonce(length: usize) -> Vec<u8> {
        generate_salt(length)
    }

    /// Hash a password securely
    pub fn hash_password(password: &str, salt: Option<&[u8]>) -> (Vec<u8>, Vec<u8>) {
        let salt = salt.unwrap_or(&generate_salt(32)).to_vec();
        let params = Pbkdf2Params::default();
        let hash = derive_key_pbkdf2(password, &salt, &params);
        (hash, salt)
    }

    /// Verify a password hash
    pub fn verify_password(password: &str, hash: &[u8], salt: &[u8]) -> bool {
        let params = Pbkdf2Params::default();
        let computed_hash = derive_key_pbkdf2(password, salt, &params);
        constant_time_compare(hash, &computed_hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes_gcm_cipher() {
        let key = [1u8; 32]; // 256-bit key
        let cipher = AesGcmCipher::new(&key);

        let data = b"Hello, Savitri Network with AES-GCM encryption!";
        let encrypted = cipher.encrypt(data).unwrap();
        let decrypted = cipher.decrypt(&encrypted).unwrap();

        assert_eq!(data.to_vec(), decrypted);
        assert_ne!(data.to_vec(), encrypted);

        // Verify each encrypt() call uses a different nonce (different ciphertext)
        let encrypted2 = cipher.encrypt(data).unwrap();
        assert_ne!(encrypted, encrypted2, "Same plaintext must produce different ciphertext (fresh nonce)");
        let decrypted2 = cipher.decrypt(&encrypted2).unwrap();
        assert_eq!(data.to_vec(), decrypted2);
    }

    #[test]
    fn test_aes_gcm_cipher_different_keys() {
        let key1 = [1u8; 32];
        let key2 = [2u8; 32];

        let cipher1 = AesGcmCipher::new(&key1);
        let cipher2 = AesGcmCipher::new(&key2);

        let data = b"Test data";
        let encrypted1 = cipher1.encrypt(data).unwrap();
        let encrypted2 = cipher2.encrypt(data).unwrap();

        assert_ne!(encrypted1, encrypted2);

        // Decryption should fail with wrong key
        let decrypted_wrong = cipher2.decrypt(&encrypted1);
        assert!(decrypted_wrong.is_err());

        // Correct decryption
        let decrypted_correct = cipher1.decrypt(&encrypted1).unwrap();
        assert_eq!(data.to_vec(), decrypted_correct);
    }

    #[test]
    fn test_improved_pbkdf2() {
        let password = "test_password_123";
        let salt = b"test_salt_123";
        let params = Pbkdf2Params::default();
        
        let key1 = derive_key_pbkdf2_improved(password, salt, &params);
        let key2 = derive_key_pbkdf2_improved(password, salt, &params);
        
        // Should be deterministic
        assert_eq!(key1, key2);
        
        // Should be 32 bytes
        assert_eq!(key1.len(), 32);
        
        // Different password should produce different key
        let key3 = derive_key_pbkdf2_improved("different_password", salt, &params);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_password_encryption_hmac_pbkdf2() {
        let data = b"Sensitive data that needs encryption";
        let password = "strong_password_123";

        let encrypted = encrypt_with_password(data, password).unwrap();
        let decrypted = decrypt_with_password(&encrypted, password).unwrap();

        assert_eq!(data.to_vec(), decrypted);
        assert_ne!(data.to_vec(), encrypted);

        // Wrong password should fail
        let decrypted_wrong = decrypt_with_password(&encrypted, "wrong_password");
        assert!(decrypted_wrong.is_err());
    }

    #[test]
    fn test_key_derivation() {
        let password = "test_password";
        let salt = generate_salt(32);
        let params = Pbkdf2Params::default();
        
        let key1 = derive_key_pbkdf2(password, &salt, &params);
        let key2 = derive_key_pbkdf2(password, &salt, &params);
        
        assert_eq!(key1, key2);
        assert_eq!(key1.len(), params.key_length);
    }

    #[test]
    fn test_key_derivation_different_params() {
        let password = "test_password";
        let salt = generate_salt(32);
        
        let params1 = Pbkdf2Params::default();
        let mut params2 = Pbkdf2Params::default();
        params2.iterations = 50_000; // Different iteration count
        
        let key1 = derive_key_pbkdf2(password, &salt, &params1);
        let key2 = derive_key_pbkdf2(password, &salt, &params2);
        
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_secure_rng() {
        let mut rng = SecureRng::new();
        
        let bytes1 = rng.next_u64();
        let bytes2 = rng.next_u64();
        
        // Should be different
        assert_ne!(bytes1, bytes2);
        
        // Test range generation
        let range_value = rng.gen_range(10..100);
        assert!(range_value >= 10 && range_value < 100);
    }

    #[test]
    fn test_constant_time_compare() {
        let data1 = b"same_data";
        let data2 = b"same_data";
        let data3 = b"different_data";
        
        assert!(utils::constant_time_compare(data1, data2));
        assert!(!utils::constant_time_compare(data1, data3));
        assert!(!utils::constant_time_compare(data1, b"short"));
    }

    #[test]
    fn test_zeroize() {
        let mut data = vec![1, 2, 3, 4, 5];
        utils::zeroize(&mut data);
        
        assert_eq!(data, vec![0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_password_hashing() {
        let password = "user_password";
        
        let (hash1, salt1) = utils::hash_password(password, None);
        let (hash2, salt2) = utils::hash_password(password, None);
        
        // Hashes should be different due to different salts
        assert_ne!(hash1, hash2);
        assert_ne!(salt1, salt2);
        
        assert!(utils::verify_password(password, &hash1, &salt1));
        assert!(utils::verify_password(password, &hash2, &salt2));
        
        // Wrong password should fail
        assert!(!utils::verify_password("wrong_password", &hash1, &salt1));
    }

    #[test]
    fn test_password_hashing_with_salt() {
        let password = "user_password";
        let salt = generate_salt(32);
        
        let (hash1, _) = utils::hash_password(password, Some(&salt));
        let (hash2, _) = utils::hash_password(password, Some(&salt));
        
        // Same salt should produce same hash
        assert_eq!(hash1, hash2);
        
        assert!(utils::verify_password(password, &hash1, &salt));
    }

    #[test]
    fn test_encryption_format() {
        let password = "test_password";
        let data = b"test data";

        let encrypted = encrypt_with_password(data, password).unwrap();

        // Check format: salt_length (1 byte) + salt + nonce (12) + ciphertext + tag (16)
        assert!(encrypted.len() > 1);
        let salt_length = encrypted[0] as usize;
        // total = 1 + salt_length + 12 (nonce) + data.len() + 16 (tag)
        assert_eq!(encrypted.len(), 1 + salt_length + 12 + data.len() + 16);
    }

    #[test]
    fn test_invalid_encrypted_data() {
        let password = "test_password";
        
        // Empty data
        assert!(decrypt_with_password(&[], password).is_err());
        
        // Invalid format
        assert!(decrypt_with_password(&[255], password).is_err());
        
        // Truncated data
        assert!(decrypt_with_password(&[32, 1, 2], password).is_err());
    }
}
