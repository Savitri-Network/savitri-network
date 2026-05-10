use super::{Storage, RocksDb};
//! Storage layer for TEST token operations
//! 
//! This module provides storage operations for the TEST token,
//! including balance management, faucet claims, and supply tracking.

use super::CF_TEST_TOKENS;
use anyhow::{Context, Result};
use serde::{Serialize, Deserialize};

/// Special keys for TEST token metadata
const KEY_TEST_TOTAL_SUPPLY: &[u8] = b"__test_total_supply__";
const KEY_TEST_TOTAL_BURNED: &[u8] = b"__test_total_burned__";
const FAUCET_PREFIX: &[u8] = b"faucet:";

/// Faucet claim state for an address
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FaucetClaim {
    /// Last timestamp when faucet was used
    pub last_claim_timestamp: u64,
    /// Total number of claims made by this address
    pub total_claims: u64,
    /// Total TEST tokens received from faucet
    pub total_received: u128,
}

impl Storage<RocksDb> {
    /// Store TEST token balance for an address
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// * `balance` - Balance in smallest unit (10^18)
    pub fn put_test_token_balance(
        &self,
        address: &[u8; 32],
        balance: u128,
    ) -> Result<()> {
        let value = balance.to_le_bytes();
        self.put_cf(CF_TEST_TOKENS, address, value)
    }

    /// Get TEST token balance for an address
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// 
    /// # Returns
    /// Balance in smallest unit (10^18), or 0 if not found
    pub fn get_test_token_balance(&self, address: &[u8; 32]) -> Result<u128> {
        match self.get_cf(CF_TEST_TOKENS, address)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Ok(u128::from_le_bytes(arr))
            }
            Some(_) => anyhow::bail!("Invalid TEST token balance encoding"),
            None => Ok(0),
        }
    }

    /// Set total TEST token supply
    /// 
    /// This should only be called once during genesis initialization
    pub fn set_test_token_total_supply(&self, supply: u128) -> Result<()> {
        let value = supply.to_le_bytes();
        self.put_cf(CF_TEST_TOKENS, KEY_TEST_TOTAL_SUPPLY, value)
    }

    /// Get total TEST token supply
    /// 
    /// # Returns
    /// Total supply in smallest unit (10^18)
    pub fn get_test_token_total_supply(&self) -> Result<u128> {
        match self.get_cf(CF_TEST_TOKENS, KEY_TEST_TOTAL_SUPPLY)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Ok(u128::from_le_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Set total TEST tokens burned
    /// 
    /// Tracks cumulative burn amount for supply calculations
    pub fn set_test_token_total_burned(&self, burned: u128) -> Result<()> {
        let value = burned.to_le_bytes();
        self.put_cf(CF_TEST_TOKENS, KEY_TEST_TOTAL_BURNED, value)
    }

    /// Get total TEST tokens burned
    /// 
    /// # Returns
    /// Total burned amount in smallest unit (10^18)
    pub fn get_test_token_total_burned(&self) -> Result<u128> {
        match self.get_cf(CF_TEST_TOKENS, KEY_TEST_TOTAL_BURNED)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Ok(u128::from_le_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Store faucet claim state for an address
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// * `claim` - Faucet claim state to store
    pub fn put_faucet_claim(
        &self,
        address: &[u8; 32],
        claim: &FaucetClaim,
    ) -> Result<()> {
        let mut key = FAUCET_PREFIX.to_vec();
        key.extend_from_slice(address);
        let value = bincode::serialize(claim)
            .context("Failed to serialize faucet claim")?;
        self.put_cf(CF_TEST_TOKENS, key, value)
    }

    /// Get faucet claim state for an address
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// 
    /// # Returns
    /// Faucet claim state if found, None otherwise
    pub fn get_faucet_claim(&self, address: &[u8; 32]) -> Result<Option<FaucetClaim>> {
        let mut key = FAUCET_PREFIX.to_vec();
        key.extend_from_slice(address);
        match self.get_cf(CF_TEST_TOKENS, key)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                Ok(Some(crate::safe_deserialize(&bytes[..])
                    .context("Failed to deserialize faucet claim")?))
            }
            None => Ok(None),
        }
    }

    /// Get circulating TEST token supply
    /// 
    /// Circulating supply = total_supply - total_burned
    /// 
    /// # Returns
    /// Circulating supply in smallest unit (10^18)
    pub fn get_test_token_circulating_supply(&self) -> Result<u128> {
        let total_supply = self.get_test_token_total_supply()?;
        let total_burned = self.get_test_token_total_burned()?;
        Ok(total_supply.saturating_sub(total_burned))
    }

    /// Check if address has TEST tokens
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// 
    /// # Returns
    /// True if balance > 0, false otherwise
    pub fn has_test_tokens(&self, address: &[u8; 32]) -> Result<bool> {
        let balance = self.get_test_token_balance(address)?;
        Ok(balance > 0)
    }

    /// Get TEST token statistics
    /// 
    /// # Returns
    /// Tuple of (total_supply, total_burned, circulating_supply)
    pub fn get_test_token_stats(&self) -> Result<(u128, u128, u128)> {
        let total_supply = self.get_test_token_total_supply()?;
        let total_burned = self.get_test_token_total_burned()?;
        let circulating = total_supply.saturating_sub(total_burned);
        Ok((total_supply, total_burned, circulating))
    }

    /// Batch get TEST token balances for multiple addresses
    /// 
    /// # Arguments
    /// * `addresses` - Vector of 32-byte addresses
    /// 
    /// # Returns
    /// Vector of balances corresponding to input addresses
    pub fn get_test_token_balances_batch(
        &self,
        addresses: &[[u8; 32]],
    ) -> Result<Vec<u128>> {
        let mut balances = Vec::with_capacity(addresses.len());
        for address in addresses {
            balances.push(self.get_test_token_balance(address)?);
        }
        Ok(balances)
    }

    /// Clear TEST token balance for an address (testing only)
    /// 
    /// # Arguments
    /// * `address` - 32-byte address
    /// 
    /// # Safety
    /// This should only be used for testing purposes
    #[cfg(test)]
    pub fn clear_test_token_balance(&self, address: &[u8; 32]) -> Result<()> {
        self.delete_cf(CF_TEST_TOKENS, address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_storage() -> Storage {
        let dir = tempdir().unwrap();
        let path = dir.path();
        Storage<RocksDb>::new(path).unwrap()
    }

    #[test]
    fn test_test_token_balance_operations() {
        let storage = create_test_storage();
        let address = [1u8; 32];
        let balance = 1_000_000_000_000_000_000u128; // 1 TEST

        // Test put and get
        storage.put_test_token_balance(&address, balance).unwrap();
        let retrieved = storage.get_test_token_balance(&address).unwrap();
        assert_eq!(retrieved, balance);

        // Test non-existent address
        let non_existent = [2u8; 32];
        let zero_balance = storage.get_test_token_balance(&non_existent).unwrap();
        assert_eq!(zero_balance, 0);
    }

    #[test]
    fn test_test_token_supply_tracking() {
        let storage = create_test_storage();
        
        // Test initial state
        assert_eq!(storage.get_test_token_total_supply().unwrap(), 0);
        assert_eq!(storage.get_test_token_total_burned().unwrap(), 0);

        // Set supply
        let supply = 100_000_000_000_000_000_000_000_000u128; // 100M TEST
        storage.set_test_token_total_supply(supply).unwrap();
        assert_eq!(storage.get_test_token_total_supply().unwrap(), supply);

        // Test burn tracking
        let burned = 1_000_000_000_000_000_000u128; // 0.001 TEST
        storage.set_test_token_total_burned(burned).unwrap();
        assert_eq!(storage.get_test_token_total_burned().unwrap(), burned);

        // Test circulating supply
        let circulating = storage.get_test_token_circulating_supply().unwrap();
        assert_eq!(circulating, supply - burned);
    }

    #[test]
    fn test_faucet_claim_operations() {
        let storage = create_test_storage();
        let address = [1u8; 32];
        
        // Test initial state
        assert!(storage.get_faucet_claim(&address).unwrap().is_none());

        // Test storing claim
        let claim = FaucetClaim {
            last_claim_timestamp: 1640995200, // 2022-01-01
            total_claims: 1,
            total_received: 1_000_000_000_000_000_000_000, // 1000 TEST
        };
        storage.put_faucet_claim(&address, &claim).unwrap();

        // Test retrieving claim
        let retrieved = storage.get_faucet_claim(&address).unwrap().unwrap();
        assert_eq!(retrieved, claim);

        // Test updating claim
        let updated_claim = FaucetClaim {
            last_claim_timestamp: 1641081600, // 2022-01-02
            total_claims: 2,
            total_received: 2_000_000_000_000_000_000_000, // 2000 TEST
        };
        storage.put_faucet_claim(&address, &updated_claim).unwrap();
        let retrieved_updated = storage.get_faucet_claim(&address).unwrap().unwrap();
        assert_eq!(retrieved_updated, updated_claim);
    }

    #[test]
    fn test_batch_balance_operations() {
        let storage = create_test_storage();
        let addresses = [
            [1u8; 32],
            [2u8; 32],
            [3u8; 32],
        ];
        let balances = [
            1_000_000_000_000_000_000u128, // 1 TEST
            2_000_000_000_000_000_000u128, // 2 TEST
            3_000_000_000_000_000_000u128, // 3 TEST
        ];

        // Store balances
        for (i, address) in addresses.iter().enumerate() {
            storage.put_test_token_balance(address, balances[i]).unwrap();
        }

        // Batch retrieve
        let retrieved_balances = storage.get_test_token_balances_batch(&addresses).unwrap();
        assert_eq!(retrieved_balances, balances);
    }

    #[test]
    fn test_has_test_tokens() {
        let storage = create_test_storage();
        let address_with_tokens = [1u8; 32];
        let address_without_tokens = [2u8; 32];

        // Store balance for first address
        storage.put_test_token_balance(&address_with_tokens, 1_000_000_000_000_000_000u128).unwrap();

        // Test has tokens
        assert!(storage.has_test_tokens(&address_with_tokens).unwrap());
        assert!(!storage.has_test_tokens(&address_without_tokens).unwrap());
    }

    #[test]
    fn test_test_token_stats() {
        let storage = create_test_storage();
        
        // Set up test data
        let total_supply = 100_000_000_000_000_000_000_000_000u128; // 100M TEST
        let total_burned = 1_000_000_000_000_000_000u128; // 0.001 TEST
        
        storage.set_test_token_total_supply(total_supply).unwrap();
        storage.set_test_token_total_burned(total_burned).unwrap();

        // Get stats
        let (supply, burned, circulating) = storage.get_test_token_stats().unwrap();
        assert_eq!(supply, total_supply);
        assert_eq!(burned, total_burned);
        assert_eq!(circulating, total_supply - total_burned);
    }
}
