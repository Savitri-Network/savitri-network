use super::{Storage, RocksDb};
//! Storage per esecuzione proposte governance
//!
//! - Parametri fee (base_fee, max_fee)
//! - Standard approvati
//! - Modifiche non-core

use super::{Storage, CF_GOVERNANCE, CF_META, RocksDb};
use serde::{Deserialize, Serialize};

// Keys per fee parameters
const FEE_BASE_KEY: &[u8] = b"fee_base";
const FEE_MAX_KEY: &[u8] = b"fee_max";

// Prefix per standard approvati
const STANDARD_PREFIX: u8 = 0x03;

// Prefix per modifiche non-core
const NON_CORE_PREFIX: u8 = 0x04;

/// Standard approvato
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovedStandard {
    pub name: String,
    pub version: String,
    pub approved_at: u64,
}

/// Modifica non-core
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NonCoreChange {
    pub timestamp: u64,
    pub description: String,
}

impl Storage<RocksDb> {
    /// Set il fee base
    pub fn set_fee_base(&self, base_fee: u128) -> anyhow::Result<()> {
        let value = base_fee.to_le_bytes();
        self.put_cf(CF_META, FEE_BASE_KEY, value)
    }

    pub fn get_fee_base(&self) -> anyhow::Result<Option<u128>> {
        match self.get_cf(CF_META, FEE_BASE_KEY)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let bytes: &[u8] = bytes;`n
                if bytes.len() != 16 {
                    anyhow::bail!("invalid fee base encoding");
                }
                Ok(Some(u128::from_le_bytes(bytes.try_into().unwrap())))
            }
            None => Ok(None),
        }
    }

    /// Set il fee max
    pub fn set_fee_max(&self, max_fee: u128) -> anyhow::Result<()> {
        let value = max_fee.to_le_bytes();
        self.put_cf(CF_META, FEE_MAX_KEY, value)
    }

    pub fn get_fee_max(&self) -> anyhow::Result<Option<u128>> {
        match self.get_cf(CF_META, FEE_MAX_KEY)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let bytes: &[u8] = bytes;`n
                if bytes.len() != 16 {
                    anyhow::bail!("invalid fee max encoding");
                }
                Ok(Some(u128::from_le_bytes(bytes.try_into().unwrap())))
            }
            None => Ok(None),
        }
    }

    /// Registra uno standard approvato
    pub fn put_approved_standard(
        &self,
        standard_name: &str,
        standard_version: &str,
    ) -> anyhow::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let standard = ApprovedStandard {
            name: standard_name.to_string(),
            version: standard_version.to_string(),
            approved_at: timestamp,
        };

        // Key: STANDARD_PREFIX + standard_name + standard_version
        let mut key = vec![STANDARD_PREFIX];
        key.extend_from_slice(standard_name.as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(standard_version.as_bytes());

        let value = bincode::serialize(&standard)?;
        self.put_cf(CF_GOVERNANCE, key, value)
    }

    pub fn get_approved_standard(
        &self,
        standard_name: &str,
        standard_version: &str,
    ) -> anyhow::Result<Option<ApprovedStandard>> {
        let mut key = vec![STANDARD_PREFIX];
        key.extend_from_slice(standard_name.as_bytes());
        key.push(0); // Separator
        key.extend_from_slice(standard_version.as_bytes());

        match self.get_cf(CF_GOVERNANCE, key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)),
            None => Ok(None),
        }
    }

    /// Registra una modifica non-core
    pub fn put_non_core_change(&self, timestamp: u64, description: &str) -> anyhow::Result<()> {
        let change = NonCoreChange {
            timestamp,
            description: description.to_string(),
        };

        // Key: NON_CORE_PREFIX + timestamp (u64, little-endian)
        let mut key = vec![NON_CORE_PREFIX];
        key.extend_from_slice(&timestamp.to_le_bytes());

        let value = bincode::serialize(&change)?;
        self.put_cf(CF_GOVERNANCE, key, value)
    }

    pub fn get_non_core_change(&self, timestamp: u64) -> anyhow::Result<Option<NonCoreChange>> {
        let mut key = vec![NON_CORE_PREFIX];
        key.extend_from_slice(&timestamp.to_le_bytes());

        match self.get_cf(CF_GOVERNANCE, key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)),
            None => Ok(None),
        }
    }
}
