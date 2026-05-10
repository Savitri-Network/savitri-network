// SPDX-License-Identifier: MIT
// © 2026 Savitri Network

//! Cryptographic primitives for Savitri Network
//! 
//! This module provides the foundational cryptographic functions needed
//! throughout the Savitri ecosystem, including signatures, hashing,
//! key management, and encryption.

use sha2::Digest;
use serde_big_array::BigArray;
use std::path::Path;
use ed25519_dalek::{Signer, Verifier};

pub mod hash;
pub mod signature;
pub mod keys;
pub mod encryption;

// Re-export commonly used functions
pub use hash::{sha256, sha512, blake3, hash, merkle_root, hash_with_domain};
pub use signature::{sign, verify, generate_keypair, verify_with_security_level};
pub use keys::{KeyPair, KeyManager, MemoryKeyStorage, KeyStorage};
pub use encryption::{AesGcmCipher, encrypt_with_password, decrypt_with_password, SecureRng};

/// Compute transaction root hash (compatibility function)
pub fn compute_tx_root(txs: &[&[u8]]) -> [u8; 32] {
    hash::merkle_root_from_data(txs)
}

/// Sign data with private key (production-ready compatibility function)
pub fn sign_data(data: &[u8], key: &[u8]) -> Vec<u8> {
    // Try to use proper Ed25519 signature if key length matches
    if key.len() == 64 {
        if let Ok(kp) = signature::keypair_from_bytes(&key.try_into().unwrap_or([0u8; 64])) {
            let signature = signature::sign(data, &kp);
            return signature_to_bytes(&signature).to_vec();
        }
    }
    
    // Try to parse as 32-byte private key (common format)
    if key.len() == 32 {
        let arr: [u8; 32] = key.try_into().unwrap_or([0u8; 32]);
        let sk = ed25519_dalek::SigningKey::from_bytes(&arr);
        let signature = sk.sign(data);
        return signature.to_bytes().to_vec();
    }
    
    // Ed25519 signature.  Returning an empty Vec signals failure; callers
    // must check the length and treat it as an error.
    log::error!(
        "sign_data: unsupported key length {} (expected 32 or 64 bytes for Ed25519)",
        key.len()
    );
    Vec::new()
}

/// Load or generate identity keypair with persistence
pub fn load_or_generate_identity(path: &str) -> anyhow::Result<Vec<u8>> {
    use std::path::Path;

    let key_path = Path::new(path);

    // Try to load existing identity
    if key_path.exists() {
        let key_data = std::fs::read(&key_path)
            .map_err(|e| anyhow::anyhow!("Failed to read identity file: {}", e))?;

        if key_data.len() >= 64 {
            // Validate that the first 32 bytes form a valid signing key
            let secret: [u8; 32] = key_data[..32].try_into()
                .map_err(|_| anyhow::anyhow!("Invalid identity key data"))?;
            let sk = ed25519_dalek::SigningKey::from_bytes(&secret);
            let mut out = sk.to_bytes().to_vec();
            out.extend_from_slice(sk.verifying_key().as_bytes());
            log::info!("Loaded existing identity");
            Ok(out)
        } else {
            log::warn!("Invalid identity file size, generating new identity");
            generate_and_save_identity(path)
        }
    } else {
        log::info!("Identity file not found, generating new identity");
        generate_and_save_identity(path)
    }
}

/// Generate and save a new identity keypair
fn generate_and_save_identity(path: &str) -> anyhow::Result<Vec<u8>> {
    use std::path::Path;

    let sk = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);
    let mut key_data = sk.to_bytes().to_vec();
    key_data.extend_from_slice(sk.verifying_key().as_bytes());
    
    // Create directory if it doesn't exist
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("Failed to create directory: {}", e))?;
    }
    
    // Save the keypair to file
    std::fs::write(path, &key_data)
        .map_err(|e| anyhow::anyhow!("Failed to save identity file: {}", e))?;
    
    log::info!("Generated and saved new identity");
    Ok(key_data)
}

/// Sign message with keypair (enhanced compatibility function)
pub fn sign_message(message: &[u8], keypair: &[u8]) -> Vec<u8> {
    // Try 64-byte keypair format first
    if keypair.len() == 64 {
        if let Ok(kp) = signature::keypair_from_bytes(&keypair.try_into().unwrap_or([0u8; 64])) {
            let signature = signature::sign(message, &kp);
            return signature_to_bytes(&signature).to_vec();
        }
    }
    
    // Try 32-byte private key format
    if keypair.len() == 32 {
        let arr: [u8; 32] = keypair.try_into().unwrap_or([0u8; 32]);
        let sk = ed25519_dalek::SigningKey::from_bytes(&arr);
        let signature = sk.sign(message);
        return signature.to_bytes().to_vec();
    }
    
    // Fallback to sign_data
    sign_data(message, keypair)
}

/// Verify signed data with key (companion to sign_data)
pub fn verify_signed_data(data: &[u8], signature: &[u8], key: &[u8]) -> bool {
    // Try Ed25519 verification for 64-byte keypair
    if key.len() == 64 && signature.len() == 64 {
        if let Ok(kp) = signature::keypair_from_bytes(&key.try_into().unwrap_or([0u8; 64])) {
            if let Ok(sig) = signature::signature_from_bytes(&signature.try_into().unwrap_or([0u8; 64])) {
                return kp.verify(data, &sig).is_ok();
            }
        }
    }
    
    // Try Ed25519 verification for 32-byte private key (derive public key)
    if key.len() == 32 && signature.len() == 64 {
        let arr: [u8; 32] = key.try_into().unwrap_or([0u8; 32]);
        let sk = ed25519_dalek::SigningKey::from_bytes(&arr);
        if let Ok(sig) = signature::signature_from_bytes(&signature.try_into().unwrap_or([0u8; 64])) {
            return sk.verify(data, &sig).is_ok();
        }
    }
    
    // are accepted.  A 32-byte "signature" would have come from the old HMAC
    // fallback in sign_data(), which is no longer produced.
    false
}

// Re-export for backward compatibility
pub use ed25519_dalek::SigningKey as Keypair;
pub use signature::{keypair_to_bytes, keypair_from_bytes, public_key_to_bytes, public_key_from_bytes};
pub use signature::{signature_to_bytes, signature_from_bytes};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_data_with_keypair() {
        let keypair = signature::generate_keypair();
        let keypair_bytes = signature::keypair_to_bytes(&keypair);
        
        let message = b"test message";
        let signature = sign_data(message, &keypair_bytes);
        
        // Verify with the original keypair
        let parsed_signature = signature::signature_from_bytes(&signature.try_into().unwrap_or([0u8; 64])).unwrap();
        assert!(keypair.verify(message, &parsed_signature).is_ok());
    }

    #[test]
    fn test_sign_data_with_private_key() {
        let private_key = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);
        let private_key_bytes = private_key.as_bytes();
        
        let message = b"test message";
        let signature = sign_data(message, private_key_bytes);
        
        // Verify with the original private key
        let parsed_signature = signature::signature_from_bytes(&signature.try_into().unwrap_or([0u8; 64])).unwrap();
        assert!(private_key.verify(message, &parsed_signature).is_ok());
    }

    #[test]
    fn test_sign_data_rejects_invalid_key_length() {
        let key = b"test_key_12345";
        let message = b"test message";

        let signature = sign_data(message, key);
        assert!(
            signature.is_empty(),
            "sign_data must return empty Vec for unsupported key lengths"
        );
    }

    #[test]
    fn test_verify_signed_data() {
        let keypair = signature::generate_keypair();
        let keypair_bytes = signature::keypair_to_bytes(&keypair);
        
        let message = b"test message";
        let signature = sign_data(message, &keypair_bytes);
        
        // Should verify correctly
        assert!(verify_signed_data(message, &signature, &keypair_bytes));
        
        // Wrong signature should fail
        let wrong_signature = [0u8; 64];
        assert!(!verify_signed_data(message, &wrong_signature, &keypair_bytes));
    }

    #[test]
    fn test_sign_message_enhanced() {
        let keypair = signature::generate_keypair();
        let keypair_bytes = signature::keypair_to_bytes(&keypair);
        
        let message = b"test message";
        let signature = sign_message(message, &keypair_bytes);
        
        // Verify with the original keypair
        let parsed_signature = signature::signature_from_bytes(&signature.try_into().unwrap_or([0u8; 64])).unwrap();
        assert!(keypair.verify(message, &parsed_signature).is_ok());
    }

    #[test]
    fn test_identity_persistence() {
        use std::fs;
        use std::env;
        
        let temp_dir = env::temp_dir();
        let identity_path = temp_dir.join("test_identity.key");
        
        // Generate identity
        let key_data = load_or_generate_identity(identity_path.to_str().unwrap()).unwrap();
        assert_eq!(key_data.len(), 64); // Should be 64 bytes for Ed25519 keypair
        
        // Load existing identity
        let key_data2 = load_or_generate_identity(identity_path.to_str().unwrap()).unwrap();
        assert_eq!(key_data, key_data2); // Should be the same
        
        // Cleanup
        let _ = fs::remove_file(identity_path);
    }
}
