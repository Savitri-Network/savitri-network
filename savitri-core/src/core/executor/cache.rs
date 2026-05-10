//! Executor Cache for Savitri Core
//!
//! This module provides caching mechanisms for smart contract execution
//! to improve performance and reduce redundant computations.

use anyhow::Result;
use std::collections::HashMap;

/// Account cache for storing contract state
pub struct ExecutorAccountCache {
    cache: HashMap<Vec<u8>, Vec<u8>>,
}

impl ExecutorAccountCache {
    /// Create a new account cache
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// Get cached state for an account
    pub fn get(&self, address: &[u8]) -> Option<&Vec<u8>> {
        self.cache.get(address)
    }

    /// Set cached state for an account
    pub fn set(&mut self, address: Vec<u8>, state: Vec<u8>) {
        self.cache.insert(address, state);
    }

    /// Remove cached state for an account
    pub fn remove(&mut self, address: &[u8]) -> Option<Vec<u8>> {
        self.cache.remove(address)
    }

    /// Clear all cached state
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get the number of cached accounts
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Insert contract state for an address
    ///
    /// This is an alias for `set` that returns a Result for compatibility
    /// with contract execution code.
    pub fn insert_contract_state(&mut self, address: &[u8], state: Vec<u8>) -> Result<()> {
        self.cache.insert(address.to_vec(), state);
        Ok(())
    }

    /// Get contract state for an address
    ///
    /// This returns an empty Vec if the contract is not found,
    /// allowing callers to check for contract existence.
    pub fn get_contract_state(&self, address: &[u8]) -> Result<Vec<u8>> {
        Ok(self.cache.get(address).cloned().unwrap_or_default())
    }
}

impl Default for ExecutorAccountCache {
    fn default() -> Self {
        Self::new()
    }
}
