/// Cryptographic primitives for Savitri Network
pub mod key_manager;
// pub mod signing; // Module disabled - signing.rs.disabled
pub mod encryption;
pub mod hash;
pub mod keys;
pub mod signature;

// Re-export generate_keypair from core::crypto
pub use crate::core::crypto::generate_keypair;

// Type alias for ed25519_dalek::SigningKey to maintain Keypair compatibility
pub use ed25519_dalek::SigningKey as Keypair;

use sha2::{Digest, Sha256};

/// Compute transaction root hash
pub fn compute_tx_root(txs: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for tx in txs {
        hasher.update(tx);
    }
    hasher.finalize().into()
}

/// Sign data with private key using Ed25519
pub fn sign_data(data: &[u8], private_key: &[u8]) -> Result<Vec<u8>, String> {
    use ed25519_dalek::{Signer, SigningKey};

    // Convert private key bytes to SigningKey
    let private_key_array: &[u8; 32] = private_key
        .try_into()
        .map_err(|_| "Invalid private key length".to_string())?;
    let signing_key = SigningKey::from_bytes(private_key_array);

    // Sign the data
    let signature = signing_key.sign(data);

    Ok(signature.to_bytes().to_vec())
}

/// Verify signature against public key using Ed25519.
///
/// SECURITY: Always performs real cryptographic verification.
/// The old environment-variable-based security level has been removed
/// because it allowed bypassing signature checks at runtime.
pub fn verify_signature(data: &[u8], signature: &[u8], public_key: &[u8]) -> bool {
    verify_signature_production_internal(data, signature, public_key, true)
}

/// Internal production verification implementation
fn verify_signature_production_internal(
    data: &[u8],
    signature: &[u8],
    public_key: &[u8],
    full_logging: bool,
) -> bool {
    if signature.len() != 64 || public_key.len() != 32 {
        if full_logging {
            tracing::warn!(target: "crypto", "Invalid signature or public key length: sig={}, pk={}", 
                           signature.len(), public_key.len());
        }
        return false;
    }

    // Convert bytes to Ed25519 types
    let sig_bytes: [u8; 64] = match signature.try_into() {
        Ok(bytes) => bytes,
        Err(_) => {
            if full_logging {
                tracing::warn!(target: "crypto", "Failed to convert signature bytes");
            }
            return false;
        }
    };

    let pk_bytes: [u8; 32] = match public_key.try_into() {
        Ok(bytes) => bytes,
        Err(_) => {
            if full_logging {
                tracing::warn!(target: "crypto", "Failed to convert public key bytes");
            }
            return false;
        }
    };

    // Parse Ed25519 signature and public key
    let signature = match ed25519_dalek::Signature::try_from(&sig_bytes) {
        Ok(sig) => sig,
        Err(e) => {
            if full_logging {
                tracing::warn!(target: "crypto", "Invalid signature format: {}", e);
            }
            return false;
        }
    };

    let public_key = match ed25519_dalek::VerifyingKey::from_bytes(&pk_bytes) {
        Ok(pk) => pk,
        Err(e) => {
            if full_logging {
                tracing::warn!(target: "crypto", "Invalid public key format: {}", e);
            }
            return false;
        }
    };

    // Verify signature using Ed25519
    match public_key.verify_strict(data, &signature) {
        Ok(()) => {
            if full_logging {
                tracing::debug!(target: "crypto", "Signature verification successful");
            }
            true
        }
        Err(e) => {
            if full_logging {
                tracing::warn!(target: "crypto", "Signature verification failed: {}", e);
            }
            false
        }
    }
}

/// Load or generate identity keypair
pub fn load_or_generate_identity(path: &str) -> anyhow::Result<Vec<u8>> {
    use ed25519_dalek::SigningKey;
    use std::fs;
    use std::path::Path;

    let key_path = Path::new(path);

    // Try to load existing keypair
    if key_path.exists() {
        let key_bytes = fs::read(key_path)?;
        if key_bytes.len() == 32 {
            return Ok(key_bytes);
        } else {
            return Err(anyhow::anyhow!("Invalid keypair file format"));
        }
    }

    // Generate new keypair
    let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
    let key_bytes = signing_key.as_bytes().to_vec();

    // Create parent directory if it doesn't exist
    if let Some(parent) = key_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Save keypair to file
    fs::write(key_path, &key_bytes)?;

    Ok(key_bytes)
}

/// Sign message with keypair using Ed25519
pub fn sign_message(message: &[u8], keypair: &[u8]) -> Result<Vec<u8>, String> {
    use ed25519_dalek::{Signer, SigningKey};

    // Convert keypair bytes to SigningKey
    let keypair_array: &[u8; 32] = keypair
        .try_into()
        .map_err(|_| "Invalid keypair length".to_string())?;
    let signing_key = SigningKey::from_bytes(keypair_array);

    // Sign the message
    let signature = signing_key.sign(message);

    Ok(signature.to_bytes().to_vec())
}
