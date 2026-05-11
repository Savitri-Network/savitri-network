//! Address utilities for Savitri SDK
//!
//! Validation and conversion helpers for 32-byte Savitri addresses.

use anyhow::{bail, Result};

/// Utilities for working with Savitri addresses.
///
/// A Savitri address is a 32-byte (64 hex character) Ed25519 public key.
/// The `0x` prefix is accepted on input and stripped during normalisation.
pub struct AddressUtils;

impl AddressUtils {
    /// Validate that `address` is a well-formed 32-byte hex string.
    ///
    /// Accepts an optional `0x` prefix.
    pub fn validate(address: &str) -> Result<()> {
        let clean = address.trim_start_matches("0x");
        let bytes = hex::decode(clean).map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))?;

        if bytes.len() != 32 {
            bail!(
                "Address must be 32 bytes (64 hex characters), got {}",
                bytes.len()
            );
        }

        Ok(())
    }

    /// Convert a 32-byte slice to a hex address string.
    pub fn from_bytes(bytes: &[u8]) -> Result<String> {
        if bytes.len() != 32 {
            bail!("Address bytes must be 32 bytes, got {}", bytes.len());
        }
        Ok(hex::encode(bytes))
    }

    /// Convert a hex address string to a 32-byte array.
    ///
    /// Accepts an optional `0x` prefix.
    pub fn to_bytes(address: &str) -> Result<[u8; 32]> {
        let clean = address.trim_start_matches("0x");
        let bytes = hex::decode(clean).map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))?;

        if bytes.len() != 32 {
            bail!("Address must be 32 bytes, got {}", bytes.len());
        }

        let mut result = [0u8; 32];
        result.copy_from_slice(&bytes);
        Ok(result)
    }

    /// Return `true` if the address string is valid.
    pub fn is_valid(address: &str) -> bool {
        Self::validate(address).is_ok()
    }

    /// Normalise an address to lowercase hex without the `0x` prefix.
    pub fn normalize(address: &str) -> Result<String> {
        let clean = address.trim_start_matches("0x");
        Self::validate(clean)?;
        Ok(clean.to_lowercase())
    }
}
