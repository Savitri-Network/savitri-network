//! Secure REWARD Token Storage Implementation
//! 
//! This module provides secure storage operations for REWARD tokens with

use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use rocksdb::{IteratorMode, BoundColumnFamily};

use crate::storage::{Storage, CF_REWARD_BALANCES, RocksDb};
use super::reward_coins::{RewardBalance, NodeType};

/// Secure REWARD token storage with soul-bound enforcement
pub struct SecureRewardCoinStorage<'a> {
    db: Arc<RocksDb>,
    reward_balances: Arc<BoundColumnFamily<'a>>,
    address_registry: HashMap<[u8; 32], [u8; 32]>, // node_id -> registered_address
    total_earned_tracker: HashMap<[u8; 32], u128>,  // node_id -> total_earned (never decreases)
}

impl<'a> SecureRewardCoinStorage<'a> {
    pub fn new(storage: &'a Storage<RocksDb>) -> Result<Self> {
        let db = storage.db.clone();
        let reward_balances = storage.db.cf_handle(CF_REWARD_BALANCES)
            .ok_or_else(|| anyhow::anyhow!("missing column family: {}", CF_REWARD_BALANCES))?;
        
        let mut secure_storage = Self {
            db,
            reward_balances,
            address_registry: HashMap::new(),
            total_earned_tracker: HashMap::new(),
        };
        
        secure_storage.initialize_validation_data()?;
        
        Ok(secure_storage)
    }
    
    fn initialize_validation_data(&mut self) -> Result<()> {
        let iter = self.db.iterator_cf(&self.reward_balances, IteratorMode::Start);
        
        for item in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = item?;
            if key.len() >= 32 {
                let mut node_id = [0u8; 32];
                node_id.copy_from_slice(&key[..32]);
                
                let balance: RewardBalance = crate::safe_deserialize(&value[..])?;
                
                // Register address mapping
                self.address_registry.insert(node_id, balance.node_address);
                
                // Track total earned (never decreases)
                let current_total = self.total_earned_tracker.get(&node_id).unwrap_or(&0);
                if balance.total_earned > *current_total {
                    self.total_earned_tracker.insert(node_id, balance.total_earned);
                }
            }
        }
        
        Ok(())
    }
    
    /// Generate storage key for node balance
    fn balance_key(node_id: &[u8; 32]) -> Vec<u8> {
        node_id.to_vec()
    }
    
    pub fn get_balance(&self, node_id: &[u8; 32]) -> Result<Option<RewardBalance>> {
        let key = Self::balance_key(node_id);
        
        match self.db.get_cf(&self.reward_balances, &key)? {
            Some(value) => {
                let balance: RewardBalance = crate::safe_deserialize(&value[..])?;
                
                // Validate balance integrity
                self.validate_balance_integrity(node_id, &balance)?;
                
                Ok(Some(balance))
            }
            None => Ok(None),
        }
    }
    
    pub fn set_balance(&mut self, node_id: &[u8; 32], balance: &RewardBalance) -> Result<()> {
        // VALIDATION 1: Address ownership check
        if balance.node_address != *node_id {
            return Err(anyhow::anyhow!(
                "SECURITY VIOLATION: Balance address mismatch - potential tampering. Expected: {}, Got: {}",
                hex::encode(node_id),
                hex::encode(&balance.node_address)
            ));
        }
        
        // VALIDATION 2: Total earned monotonicity (prevents theft)
        if let Some(current_total) = self.total_earned_tracker.get(node_id) {
            if balance.total_earned < *current_total {
                return Err(anyhow::anyhow!(
                    "SECURITY VIOLATION: Cannot decrease total_earned from {} to {} - potential theft detected",
                    current_total,
                    balance.total_earned
                ));
            }
        }
        
        // VALIDATION 3: Claimable amount consistency
        let max_claimable = balance.total_earned.saturating_sub(balance.claimed);
        if balance.claimable > max_claimable {
            return Err(anyhow::anyhow!(
                "SECURITY VIOLATION: Invalid claimable amount {}. Max allowed: {}",
                balance.claimable,
                max_claimable
            ));
        }
        
        // VALIDATION 4: Node type consistency
        if !self.is_valid_node_type(&balance.node_type) {
            return Err(anyhow::anyhow!(
                "SECURITY VIOLATION: Invalid node type: {:?}",
                balance.node_type
            ));
        }
        
        // VALIDATION 5: Epoch consistency
        if balance.last_reward_epoch < balance.registration_epoch {
            return Err(anyhow::anyhow!(
                "SECURITY VIOLATION: Last reward epoch ({}) cannot be before registration epoch ({})",
                balance.last_reward_epoch,
                balance.registration_epoch
            ));
        }
        
        let key = Self::balance_key(node_id);
        let value = bincode::serialize(balance)?;
        
        self.db.put_cf(&self.reward_balances, &key, &value)?;
        
        self.total_earned_tracker.insert(*node_id, balance.total_earned);
        self.address_registry.insert(*node_id, balance.node_address);
        
        Ok(())
    }
    
    /// SECURE: REJECT all transfer attempts (soul-bound enforcement)
    pub fn transfer_rewards(&self, _from: &[u8; 32], _to: &[u8; 32], _amount: u128) -> Result<()> {
        // CRITICAL: REWARD tokens are soul-bound and cannot be transferred
        Err(anyhow::anyhow!(
            "SECURITY POLICY: REWARD tokens are soul-bound and cannot be transferred. Transfer from {} to {} for amount {} rejected.",
            hex::encode(_from),
            hex::encode(_to),
            _amount
        ))
    }
    
    pub fn claim_rewards(&mut self, claimant: &[u8; 32], target_node: &[u8; 32]) -> Result<u128> {
        // VALIDATION: Only claim own rewards
        if claimant != target_node {
            return Err(anyhow::anyhow!(
                "SECURITY VIOLATION: Cannot claim rewards for another node. Claimant: {}, Target: {}",
                hex::encode(claimant),
                hex::encode(target_node)
            ));
        }
        
        let balance = self.get_balance(target_node)?
            .ok_or_else(|| anyhow::anyhow!("No balance found for node"))?;
        
        if balance.claimable == 0 {
            return Ok(0); // No rewards to claim
        }
        
        // Create updated balance
        let claimable = balance.claimable;
        
        // Create updated balance
        let mut updated_balance = balance;
        updated_balance.claimed += claimable;
        updated_balance.claimable = 0;
        
        // Store updated balance
        self.set_balance(target_node, &updated_balance)?;
        
        Ok(claimable)
    }
    
    pub fn distribute_rewards_batch(&mut self, rewards: &[( [u8; 32], u128 )]) -> Result<()> {
        // Validate all rewards before processing
        for (node_id, amount) in rewards {
            if *amount == 0 {
                continue; // Skip zero amounts
            }
            
            // Get current balance
            let current_balance = self.get_balance(node_id)?;
            
            // Validate reward amount (reasonable limits)
            if *amount > 1_000_000_000u128 {
                return Err(anyhow::anyhow!(
                    "SECURITY VIOLATION: Excessive reward amount {} for node {}",
                    amount,
                    hex::encode(node_id)
                ));
            }
            
            // Update or create balance
            let updated_balance = if let Some(mut balance) = current_balance {
                // Update existing balance
                balance.total_earned += amount;
                balance.claimable += amount;
                balance
            } else {
                // Create new balance
                RewardBalance {
                    node_address: *node_id,
                    total_earned: *amount,
                    claimed: 0,
                    claimable: *amount,
                    node_type: NodeType::LightNode, // Default type
                    registration_epoch: 1, // Should be set by caller
                    last_reward_epoch: 1, // Should be set by caller
                    pou_snapshots: vec![],
                }
            };
            
            self.set_balance(node_id, &updated_balance)?;
        }
        
        Ok(())
    }
    
    /// Validate balance integrity
    fn validate_balance_integrity(&self, node_id: &[u8; 32], balance: &RewardBalance) -> Result<()> {
        // Check 1: Address consistency
        if let Some(registered_address) = self.address_registry.get(node_id) {
            if registered_address != &balance.node_address {
                return Err(anyhow::anyhow!(
                    "INTEGRITY VIOLATION: Address mismatch for node {}. Registered: {}, Current: {}",
                    hex::encode(node_id),
                    hex::encode(registered_address),
                    hex::encode(&balance.node_address)
                ));
            }
        }
        
        // Check 2: Total earned consistency
        if let Some(tracked_total) = self.total_earned_tracker.get(node_id) {
            if balance.total_earned != *tracked_total {
                return Err(anyhow::anyhow!(
                    "INTEGRITY VIOLATION: Total earned mismatch for node {}. Tracked: {}, Current: {}",
                    hex::encode(node_id),
                    tracked_total,
                    balance.total_earned
                ));
            }
        }
        
        // Check 3: Mathematical consistency
        let calculated_claimable = balance.total_earned.saturating_sub(balance.claimed);
        if balance.claimable != calculated_claimable {
            return Err(anyhow::anyhow!(
                "INTEGRITY VIOLATION: Claimable amount inconsistency for node {}. Expected: {}, Actual: {}",
                hex::encode(node_id),
                calculated_claimable,
                balance.claimable
            ));
        }
        
        Ok(())
    }
    
    /// Validate node type
    fn is_valid_node_type(&self, node_type: &NodeType) -> bool {
        matches!(node_type, NodeType::LightNode | NodeType::Masternode | NodeType::Guardian)
    }
    
    /// Get total supply of REWARD tokens
    pub fn get_total_supply(&self) -> Result<u128> {
        let mut total = 0u128;
        let iter = self.db.iterator_cf(&self.reward_balances, IteratorMode::Start);
        
        for item in iter {
            let (_key, value): (Box<[u8]>, Box<[u8]>) = item?;
            let balance: RewardBalance = crate::safe_deserialize(&value[..])?;
            total += balance.total_earned;
        }
        
        Ok(total)
    }
    
    /// Get security statistics
    pub fn get_security_stats(&self) -> Result<SecurityStats> {
        Ok(SecurityStats {
            registered_nodes: self.address_registry.len(),
            total_earned_tracked: self.total_earned_tracker.values().sum(),
            total_supply: self.get_total_supply()?,
            validation_checks_enabled: true,
        })
    }
}

/// Security statistics for monitoring
#[derive(Debug, Serialize, Deserialize)]
pub struct SecurityStats {
    pub registered_nodes: usize,
    pub total_earned_tracked: u128,
    pub total_supply: u128,
    pub validation_checks_enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::TempDir;
    
    #[test]
    fn test_soul_bound_enforcement() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Storage<RocksDb>::new(temp_dir.path())?;
        let mut secure_storage = SecureRewardCoinStorage<RocksDb>::new(&storage)?;
        
        let node_a = [1u8; 32];
        let node_b = [2u8; 32];
        
        // Give node A some REWARD tokens
        let balance = RewardBalance {
            node_address: node_a,
            total_earned: 1000,
            claimed: 0,
            claimable: 1000,
            node_type: NodeType::LightNode,
            registration_epoch: 1,
            last_reward_epoch: 1,
            pou_snapshots: vec![],
        };
        
        secure_storage.set_balance(&node_a, &balance)?;
        
        // Attempt transfer (should fail)
        let transfer_result = secure_storage.transfer_rewards(&node_a, &node_b, 500);
        assert!(transfer_result.is_err());
        assert!(transfer_result.unwrap_err().to_string().contains("soul-bound"));
        
        Ok(())
    }
    
    #[test]
    fn test_balance_tampering_protection() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Storage<RocksDb>::new(temp_dir.path())?;
        let mut secure_storage = SecureRewardCoinStorage<RocksDb>::new(&storage)?;
        
        let node_id = [3u8; 32];
        
        // Create initial balance
        let initial_balance = RewardBalance {
            node_address: node_id,
            total_earned: 1000,
            claimed: 0,
            claimable: 1000,
            node_type: NodeType::LightNode,
            registration_epoch: 1,
            last_reward_epoch: 1,
            pou_snapshots: vec![],
        };
        
        secure_storage.set_balance(&node_id, &initial_balance)?;
        
        // Attempt to decrease total_earned (should fail)
        let tampered_balance = RewardBalance {
            total_earned: 500, // Decreased from 1000
            ..initial_balance.clone()
        };
        
        let tamper_result = secure_storage.set_balance(&node_id, &tampered_balance);
        assert!(tamper_result.is_err());
        assert!(tamper_result.unwrap_err().to_string().contains("Cannot decrease total_earned"));
        
        // Attempt to change node_address (should fail)
        let wrong_address = [4u8; 32];
        let address_tampered_balance = RewardBalance {
            node_address: wrong_address,
            ..initial_balance
        };
        
        let address_tamper_result = secure_storage.set_balance(&node_id, &address_tampered_balance);
        assert!(address_tamper_result.is_err());
        assert!(address_tamper_result.unwrap_err().to_string().contains("address mismatch"));
        
        Ok(())
    }
    
    #[test]
    fn test_claim_authorization() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Storage<RocksDb>::new(temp_dir.path())?;
        let mut secure_storage = SecureRewardCoinStorage<RocksDb>::new(&storage)?;
        
        let node_a = [5u8; 32];
        let node_b = [6u8; 32];
        
        // Setup balance for node A
        let balance = RewardBalance {
            node_address: node_a,
            total_earned: 1000,
            claimed: 0,
            claimable: 1000,
            node_type: NodeType::LightNode,
            registration_epoch: 1,
            last_reward_epoch: 1,
            pou_snapshots: vec![],
        };
        
        secure_storage.set_balance(&node_a, &balance)?;
        
        // Claim own rewards (should succeed)
        let own_claim = secure_storage.claim_rewards(&node_a, &node_a)?;
        assert_eq!(own_claim, 1000);
        
        // Claim rewards for another node (should fail)
        let cross_claim = secure_storage.claim_rewards(&node_b, &node_a);
        assert!(cross_claim.is_err());
        assert!(cross_claim.unwrap_err().to_string().contains("Cannot claim rewards for another node"));
        
        Ok(())
    }
}
