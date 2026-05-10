//! Vote Tokens Storage: Implementation for Savitri Network
//!
//! This module implements vote token management for governance and staking.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Column family for vote tokens
pub const CF_VOTE_TOKENS: &str = "vote_tokens";

/// Vote token structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteToken {
    pub address: Vec<u8>,
    pub balance: u128,
    pub locked_amount: u128,
    pub voting_power: u128,
    pub last_vote_block: u64,
    pub delegate_to: Option<Vec<u8>>,
    pub created_at: u64,
    pub updated_at: u64,
}

impl VoteToken {
    pub fn new(address: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            address,
            balance: 0,
            locked_amount: 0,
            voting_power: 0,
            last_vote_block: 0,
            delegate_to: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn calculate_voting_power(&self) -> u128 {
        // Voting power = balance + locked_amount (with multiplier)
        let base_power = self.balance;
        let locked_multiplier = 2; // Locked tokens count double
        let locked_power = self.locked_amount * locked_multiplier;

        base_power + locked_power
    }

    pub fn update_voting_power(&mut self) {
        self.voting_power = self.calculate_voting_power();
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }
}

/// Vote token manager with full storage integration
pub struct VoteTokenManager {
    storage: Option<std::sync::Arc<dyn crate::traits::StorageTrait>>,
    cache: std::sync::RwLock<HashMap<Vec<u8>, VoteToken>>,
}

impl std::fmt::Debug for VoteTokenManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VoteTokenManager")
            .field("storage", &self.storage.is_some())
            .field(
                "cache_entries",
                &self.cache.try_read().map(|guard| guard.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl VoteTokenManager {
    pub fn new() -> Self {
        Self {
            storage: None,
            cache: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn with_storage(storage: std::sync::Arc<dyn crate::traits::StorageTrait>) -> Self {
        Self {
            storage: Some(storage),
            cache: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get vote token balance for an address
    pub fn get_balance(&self, address: &[u8]) -> u128 {
        // Try cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(token) = cache.get(address) {
                return token.balance;
            }
        }

        // Try storage
        if let Some(storage) = &self.storage {
            let key = format!("vote_token:{}", hex::encode(address));
            if let Ok(Some(data)) = storage.get(key.as_bytes()) {
                if let Ok(token) = crate::safe_deserialize::<VoteToken>(&data) {
                    // Update cache
                    if let Ok(mut cache) = self.cache.write() {
                        cache.insert(address.to_vec(), token.clone());
                    }
                    return token.balance;
                }
            }
        }

        0 // Default balance
    }

    /// Set vote token balance for an address
    pub fn set_balance(&self, address: &[u8], balance: u128) -> Result<()> {
        let mut token = self.get_or_create_token(address)?;
        token.balance = balance;
        token.update_voting_power();

        self.save_token(&token)?;
        Ok(())
    }

    /// Get voting power for an address
    pub fn get_voting_power(&self, address: &[u8]) -> u128 {
        if let Ok(cache) = self.cache.read() {
            if let Some(token) = cache.get(address) {
                return token.voting_power;
            }
        }

        if let Some(storage) = &self.storage {
            let key = format!("vote_token:{}", hex::encode(address));
            if let Ok(Some(data)) = storage.get(key.as_bytes()) {
                if let Ok(token) = crate::safe_deserialize::<VoteToken>(&data) {
                    if let Ok(mut cache) = self.cache.write() {
                        cache.insert(address.to_vec(), token.clone());
                    }
                    return token.voting_power;
                }
            }
        }

        0
    }

    /// Lock tokens for voting
    pub fn lock_tokens(&self, address: &[u8], amount: u128) -> Result<()> {
        let mut token = self.get_or_create_token(address)?;

        if token.balance < amount {
            return Err(anyhow::anyhow!("Insufficient balance to lock tokens"));
        }

        token.balance -= amount;
        token.locked_amount += amount;
        token.update_voting_power();

        self.save_token(&token)?;
        Ok(())
    }

    /// Unlock tokens
    pub fn unlock_tokens(&self, address: &[u8], amount: u128) -> Result<()> {
        let mut token = self.get_or_create_token(address)?;

        if token.locked_amount < amount {
            return Err(anyhow::anyhow!("Insufficient locked tokens to unlock"));
        }

        token.locked_amount -= amount;
        token.balance += amount;
        token.update_voting_power();

        self.save_token(&token)?;
        Ok(())
    }

    /// Delegate voting power to another address
    pub fn delegate(&self, from_address: &[u8], to_address: &[u8]) -> Result<()> {
        let mut token = self.get_or_create_token(from_address)?;
        token.delegate_to = Some(to_address.to_vec());
        token.update_voting_power();

        self.save_token(&token)?;
        Ok(())
    }

    /// Get all delegated voting power for an address
    pub fn get_delegated_power(&self, address: &[u8]) -> u128 {
        // This would require scanning all tokens in a real implementation
        // For now, return the token's own voting power
        self.get_voting_power(address)
    }

    /// Get or create token for address
    fn get_or_create_token(&self, address: &[u8]) -> Result<VoteToken> {
        // Try cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(token) = cache.get(address) {
                return Ok(token.clone());
            }
        }

        // Try storage
        if let Some(storage) = &self.storage {
            let key = format!("vote_token:{}", hex::encode(address));
            if let Ok(Some(data)) = storage.get(key.as_bytes()) {
                if let Ok(token) = crate::safe_deserialize::<VoteToken>(&data) {
                    if let Ok(mut cache) = self.cache.write() {
                        cache.insert(address.to_vec(), token.clone());
                    }
                    return Ok(token);
                }
            }
        }

        // Create new token
        let token = VoteToken::new(address.to_vec());
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(address.to_vec(), token.clone());
        }
        Ok(token)
    }

    /// Save token to storage and cache
    fn save_token(&self, token: &VoteToken) -> Result<()> {
        // Update cache
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(token.address.clone(), token.clone());
        }

        // Save to storage
        if let Some(storage) = &self.storage {
            let key = format!("vote_token:{}", hex::encode(&token.address));
            let data = bincode::serialize(token)?;
            storage.put(key.as_bytes(), &data)?;
        }

        Ok(())
    }

    /// Get total voting power in the system
    pub fn get_total_voting_power(&self) -> u128 {
        if let Ok(cache) = self.cache.read() {
            cache.values().map(|token| token.voting_power).sum()
        } else {
            0
        }
    }

    /// Get top token holders by voting power
    pub fn get_top_holders(&self, limit: usize) -> Vec<(Vec<u8>, u128)> {
        if let Ok(cache) = self.cache.read() {
            let mut holders: Vec<_> = cache
                .iter()
                .map(|(addr, token)| (addr.clone(), token.voting_power))
                .collect();

            holders.sort_by(|a, b| b.1.cmp(&a.1));
            holders.into_iter().take(limit).collect()
        } else {
            Vec::new()
        }
    }
}

impl Default for VoteTokenManager {
    fn default() -> Self {
        Self::new()
    }
}
