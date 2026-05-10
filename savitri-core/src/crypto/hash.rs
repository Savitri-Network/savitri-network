//! Hash functions

use sha2::{Digest, Sha256};

/// Compute SHA-256 hash of the given data
///
/// # Arguments
/// * `data` - The data to hash
///
/// # Returns
/// A 32-byte array containing the SHA-256 hash
pub fn hash(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}
