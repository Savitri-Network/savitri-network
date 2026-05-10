use super::{Storage, RocksDb};
//! Storage layer for REWARD coin operations
//! 
//! This module provides storage operations for the REWARD coin,
//! which is a soul-bound token used for node rewards and mainnet conversion.

use super::CF_REWARD_BALANCES;
use anyhow::{Context, Result};
use serde::{Serialize, Deserialize};

/// Special keys for REWARD coin metadata
const KEY_REWARD_TOTAL_MINTED: &[u8] = b"__reward_total_minted__";
const KEY_REWARD_TOTAL_CONVERTED: &[u8] = b"__reward_total_converted__";

/// Node type for reward distribution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeType {
    /// Light node (mobile/embedded)
    LightNode,
    Masternode,
    /// Guardian node (security/governance)
    Guardian,
}

/// PoU snapshot for mainnet migration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PouSnapshot {
    /// Epoch ID when this snapshot was taken
    pub epoch_id: u64,
    /// PoU score (0-1000 scale)
    pub pou_score: u16,
    /// Number of blocks proposed in this epoch
    pub blocks_proposed: u32,
    pub blocks_validated: u32,
    /// Uptime percentage (0.0-100.0)
    pub uptime_percent: f64,
    /// Timestamp when snapshot was created
    pub timestamp: u64,
}

/// Complete REWARD coin balance for a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RewardBalance {
    /// Node address (32 bytes)
    pub node_address: [u8; 32],
    
    /// Total REWARD coins earned (lifetime)
    pub total_earned: u128,
    
    /// REWARD coins available to claim
    pub claimable: u128,
    
    /// REWARD coins already claimed
    pub claimed: u128,
    
    /// Type of node (LightNode or Masternode)
    pub node_type: NodeType,
    
    /// Epoch when node was first registered
    pub registration_epoch: u64,
    
    /// Last epoch when rewards were distributed
    pub last_reward_epoch: u64,
    
    /// Historical PoU snapshots for mainnet migration
    /// Limited to prevent storage bloat
    pub pou_snapshots: Vec<PouSnapshot>,
}

impl Default for RewardBalance {
    fn default() -> Self {
        Self {
            node_address: [0u8; 32],
            total_earned: 0,
            claimable: 0,
            claimed: 0,
            node_type: NodeType::LightNode,
            registration_epoch: 0,
            last_reward_epoch: 0,
            pou_snapshots: Vec::new(),
        }
    }
}

impl Storage<RocksDb> {
    /// Store complete REWARD balance for a node
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// * `balance` - Complete reward balance structure
    pub fn put_reward_balance(
        &self,
        node_address: &[u8; 32],
        balance: &RewardBalance,
    ) -> Result<()> {
        let value = bincode::serialize(balance)
            .context("Failed to serialize reward balance")?;
        self.put_cf(CF_REWARD_BALANCES, node_address, value)
    }

    /// Get REWARD balance for a node
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// 
    /// # Returns
    /// Complete reward balance if found, None otherwise
    pub fn get_reward_balance(&self, node_address: &[u8; 32]) -> Result<Option<RewardBalance>> {
        match self.get_cf(CF_REWARD_BALANCES, node_address)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                Ok(Some(crate::safe_deserialize(&bytes[..])
                    .context("Failed to deserialize reward balance")?))
            }
            None => Ok(None),
        }
    }

    /// Get total REWARD coins minted
    /// 
    /// # Returns
    /// Total minted amount in smallest unit (10^18)
    pub fn get_reward_total_minted(&self) -> Result<u128> {
        match self.get_cf(CF_REWARD_BALANCES, KEY_REWARD_TOTAL_MINTED)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Ok(u128::from_le_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Set total REWARD coins minted
    /// 
    /// # Arguments
    /// * `amount` - Total minted amount in smallest unit (10^18)
    pub fn set_reward_total_minted(&self, amount: u128) -> Result<()> {
        let value = amount.to_le_bytes();
        self.put_cf(CF_REWARD_BALANCES, KEY_REWARD_TOTAL_MINTED, value)
    }

    /// Get total REWARD coins converted to mainnet
    /// 
    /// # Returns
    /// Total converted amount in smallest unit (10^18)
    pub fn get_reward_total_converted(&self) -> Result<u128> {
        match self.get_cf(CF_REWARD_BALANCES, KEY_REWARD_TOTAL_CONVERTED)? {
            Some(ref bytes) => {
                let bytes: &[u8] = &bytes;
                let mut arr = [0u8; 16];
                arr.copy_from_slice(bytes);
                Ok(u128::from_le_bytes(arr))
            }
            _ => Ok(0),
        }
    }

    /// Set total REWARD coins converted to mainnet
    /// 
    /// # Arguments
    /// * `amount` - Total converted amount in smallest unit (10^18)
    pub fn set_reward_total_converted(&self, amount: u128) -> Result<()> {
        let value = amount.to_le_bytes();
        self.put_cf(CF_REWARD_BALANCES, KEY_REWARD_TOTAL_CONVERTED, value)
    }

    /// Mint REWARD coins for a node
    /// 
    /// This method creates or updates a reward balance and adds the specified amount
    /// to both total_earned and claimable fields.
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// * `amount` - Amount to mint in smallest unit (10^18)
    /// * `node_type` - Type of node (LightNode or Masternode)
    /// * `pou_snapshot` - PoU snapshot for this reward distribution
    /// * `current_epoch` - Current epoch number
    pub fn mint_reward(
        &self,
        node_address: &[u8; 32],
        amount: u128,
        node_type: NodeType,
        pou_snapshot: PouSnapshot,
        current_epoch: u64,
    ) -> Result<()> {
        // Get existing balance or create new one
        let mut balance = self.get_reward_balance(node_address)?
            .unwrap_or_else(|| RewardBalance {
                node_address: *node_address,
                registration_epoch: current_epoch,
                node_type,
                ..Default::default()
            });

        // Update balance
        balance.total_earned = balance.total_earned
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("REWARD balance overflow"))?;
        balance.claimable = balance.claimable
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("REWARD claimable overflow"))?;
        balance.last_reward_epoch = current_epoch;

        // Add PoU snapshot (with limit to prevent storage bloat)
        if balance.pou_snapshots.len() < 1000 {
            balance.pou_snapshots.push(pou_snapshot);
        }

        // Store updated balance
        self.put_reward_balance(node_address, &balance)?;

        // Update total minted
        let total_minted = self.get_reward_total_minted()?;
        self.set_reward_total_minted(total_minted.checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Total REWARD minted overflow"))?)?;

        Ok(())
    }

    /// Claim REWARD coins for a node
    /// 
    /// Moves all claimable REWARD coins to claimed field.
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// 
    /// # Returns
    /// Amount claimed in smallest unit (10^18)
    pub fn claim_rewards(&self, node_address: &[u8; 32]) -> Result<u128> {
        let mut balance = self.get_reward_balance(node_address)?
            .ok_or_else(|| anyhow::anyhow!("No REWARD balance found for node"))?;

        anyhow::ensure!(balance.claimable > 0, "No REWARD coins available to claim");

        let amount = balance.claimable;
        balance.claimable = 0;
        balance.claimed = balance.claimed
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("REWARD claimed overflow"))?;

        self.put_reward_balance(node_address, &balance)?;
        Ok(amount)
    }

    /// Convert REWARD coins to mainnet tokens
    /// 
    /// This method converts claimed REWARD coins to mainnet tokens based on
    /// PoU history and conversion rate. The REWARD coins are marked as converted.
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// * `conversion_rate` - Rate of conversion (e.g., 0.8 = 1 REWARD = 0.8 MAINNET)
    /// * `pou_bonus_threshold_high` - PoU threshold for high bonus (e.g., 90.0)
    /// * `pou_bonus_threshold_mid` - PoU threshold for medium bonus (e.g., 75.0)
    /// * `pou_bonus_high_percent` - High bonus percentage (e.g., 0.20 = 20%)
    /// * `pou_bonus_mid_percent` - Medium bonus percentage (e.g., 0.10 = 10%)
    /// 
    /// # Returns
    /// Mainnet tokens received (in smallest unit)
    pub fn convert_to_mainnet(
        &self,
        node_address: &[u8; 32],
        conversion_rate: f64,
        pou_bonus_threshold_high: f64,
        pou_bonus_threshold_mid: f64,
        pou_bonus_high_percent: f64,
        pou_bonus_mid_percent: f64,
    ) -> Result<u128> {
        let mut balance = self.get_reward_balance(node_address)?
            .ok_or_else(|| anyhow::anyhow!("No REWARD balance found for node"))?;

        anyhow::ensure!(balance.claimed > 0, "No claimed REWARD coins to convert");

        // Calculate average PoU score from history
        let avg_pou = if balance.pou_snapshots.is_empty() {
            0.0
        } else {
            let sum: f64 = balance.pou_snapshots.iter()
                .map(|s| s.pou_score as f64)
                .sum();
            sum / balance.pou_snapshots.len() as f64
        };

        // Calculate PoU bonus
        let pou_bonus = if avg_pou >= pou_bonus_threshold_high {
            pou_bonus_high_percent
        } else if avg_pou >= pou_bonus_threshold_mid {
            pou_bonus_mid_percent
        } else {
            0.0
        };

        // Calculate final conversion rate
        let final_rate = conversion_rate * (1.0 + pou_bonus);

        // Calculate mainnet amount
        let reward_amount = balance.claimed;
        let mainnet_amount = (reward_amount as f64 * final_rate) as u128;

        // Mark REWARD as converted (set claimed to 0)
        balance.claimed = 0;
        self.put_reward_balance(node_address, &balance)?;

        // Update total converted
        let total_converted = self.get_reward_total_converted()?;
        self.set_reward_total_converted(total_converted.checked_add(reward_amount)
            .ok_or_else(|| anyhow::anyhow!("Total REWARD converted overflow"))?)?;

        Ok(mainnet_amount)
    }

    /// Get PoU history for a node (REWARD coins)
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// 
    /// # Returns
    /// Vector of PoU snapshots for mainnet migration
    pub fn get_pou_history_reward(&self, node_address: &[u8; 32]) -> Result<Vec<PouSnapshot>> {
        Ok(self
            .get_reward_balance(node_address)?
            .map(|b| b.pou_snapshots)
            .unwrap_or_default())
    }

    /// Get all REWARD balances
    /// 
    /// # Returns
    /// Vector of all reward balances in the system
    pub fn iter_reward_balances(&self) -> Result<Vec<RewardBalance>> {
        let cf = self.cf(CF_REWARD_BALANCES)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        
        let mut balances = Vec::new();
        for item in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = item?;
            // Skip special keys
            if key.starts_with(b"__") {
                continue;
            }
            if key.len() == 32 {
                let balance: RewardBalance = crate::safe_deserialize(&value[..])?;
                balances.push(balance);
            }
        }
        
        Ok(balances)
    }

    /// Get REWARD statistics
    /// 
    /// # Returns
    /// Tuple of (total_minted, total_converted, total_claimable, total_claimed)
    pub fn get_reward_stats(&self) -> Result<(u128, u128, u128, u128)> {
        let total_minted = self.get_reward_total_minted()?;
        let total_converted = self.get_reward_total_converted()?;
        
        let balances = self.iter_reward_balances()?;
        let total_claimable: u128 = balances.iter().map(|b| b.claimable).sum();
        let total_claimed: u128 = balances.iter().map(|b| b.claimed).sum();
        
        Ok((total_minted, total_converted, total_claimable, total_claimed))
    }

    /// Get nodes by type
    /// 
    /// # Arguments
    /// * `node_type` - Type of node to filter by
    /// 
    /// # Returns
    /// Vector of node addresses of the specified type
    pub fn get_nodes_by_type(&self, node_type: NodeType) -> Result<Vec<[u8; 32]>> {
        let balances = self.iter_reward_balances()?;
        Ok(balances
            .into_iter()
            .filter(|b| b.node_type == node_type)
            .map(|b| b.node_address)
            .collect())
    }

    /// Get nodes with claimable rewards
    /// 
    /// # Returns
    /// Vector of (node_address, claimable_amount) tuples
    pub fn get_nodes_with_claimable(&self) -> Result<Vec<([u8; 32], u128)>> {
        let balances = self.iter_reward_balances()?;
        Ok(balances
            .into_iter()
            .filter(|b| b.claimable > 0)
            .map(|b| (b.node_address, b.claimable))
            .collect())
    }

    /// Batch get REWARD balances for multiple nodes
    /// 
    /// # Arguments
    /// * `node_addresses` - Vector of 32-byte node addresses
    /// 
    /// # Returns
    /// Vector of reward balances (None for nodes without rewards)
    pub fn get_reward_balances_batch(
        &self,
        node_addresses: &[[u8; 32]],
    ) -> Result<Vec<Option<RewardBalance>>> {
        let mut balances = Vec::with_capacity(node_addresses.len());
        for address in node_addresses {
            balances.push(self.get_reward_balance(address)?);
        }
        Ok(balances)
    }

    /// Clear REWARD balance for a node (testing only)
    /// 
    /// # Arguments
    /// * `node_address` - 32-byte node address
    /// 
    /// # Safety
    /// This should only be used for testing purposes
    #[cfg(test)]
    pub fn clear_reward_balance(&self, node_address: &[u8; 32]) -> Result<()> {
        self.delete_cf(CF_REWARD_BALANCES, node_address)
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
    fn test_reward_balance_operations() {
        let storage = create_test_storage();
        let node_address = [1u8; 32];
        
        // Test initial state
        assert!(storage.get_reward_balance(&node_address).unwrap().is_none());

        // Test storing balance
        let balance = RewardBalance {
            node_address,
            total_earned: 100_000_000_000_000_000_000u128, // 100 REWARD
            claimable: 50_000_000_000_000_000_000u128,   // 50 REWARD
            claimed: 50_000_000_000_000_000_000u128,      // 50 REWARD
            node_type: NodeType::LightNode,
            registration_epoch: 100,
            last_reward_epoch: 105,
            pou_snapshots: Vec::new(),
        };
        storage.put_reward_balance(&node_address, &balance).unwrap();

        // Test retrieving balance
        let retrieved = storage.get_reward_balance(&node_address).unwrap().unwrap();
        assert_eq!(retrieved, balance);
    }

    #[test]
    fn test_reward_minting() {
        let storage = create_test_storage();
        let node_address = [1u8; 32];
        let amount = 10_000_000_000_000_000_000u128; // 10 REWARD
        let node_type = NodeType::Masternode;
        let current_epoch = 100;
        
        let pou_snapshot = PouSnapshot {
            epoch_id: current_epoch,
            pou_score: 850,
            blocks_proposed: 1,
            blocks_validated: 5,
            uptime_percent: 99.5,
            timestamp: 1640995200,
        };

        // Mint rewards
        storage.mint_reward(&node_address, amount, node_type, pou_snapshot.clone(), current_epoch).unwrap();

        // Verify balance
        let balance = storage.get_reward_balance(&node_address).unwrap().unwrap();
        assert_eq!(balance.total_earned, amount);
        assert_eq!(balance.claimable, amount);
        assert_eq!(balance.node_type, node_type);
        assert_eq!(balance.registration_epoch, current_epoch);
        assert_eq!(balance.last_reward_epoch, current_epoch);
        assert_eq!(balance.pou_snapshots.len(), 1);

        // Verify total minted
        let total_minted = storage.get_reward_total_minted().unwrap();
        assert_eq!(total_minted, amount);

        // Mint again
        storage.mint_reward(&node_address, amount, node_type, pou_snapshot, current_epoch + 1).unwrap();
        let balance_updated = storage.get_reward_balance(&node_address).unwrap().unwrap();
        assert_eq!(balance_updated.total_earned, amount * 2);
        assert_eq!(balance_updated.claimable, amount * 2);
    }

    #[test]
    fn test_reward_claiming() {
        let storage = create_test_storage();
        let node_address = [1u8; 32];
        let amount = 10_000_000_000_000_000_000u128; // 10 REWARD
        
        // Mint rewards first
        let pou_snapshot = PouSnapshot {
            epoch_id: 100,
            pou_score: 850,
            blocks_proposed: 0,
            blocks_validated: 5,
            uptime_percent: 99.5,
            timestamp: 1640995200,
        };
        storage.mint_reward(&node_address, amount, NodeType::LightNode, pou_snapshot, 100).unwrap();

        // Claim rewards
        let claimed_amount = storage.claim_rewards(&node_address).unwrap();
        assert_eq!(claimed_amount, amount);

        // Verify balance after claim
        let balance = storage.get_reward_balance(&node_address).unwrap().unwrap();
        assert_eq!(balance.claimable, 0);
        assert_eq!(balance.claimed, amount);

        // Try claiming again (should fail)
        let result = storage.claim_rewards(&node_address);
        assert!(result.is_err());
    }

    #[test]
    fn test_mainnet_conversion() {
        let storage = create_test_storage();
        let node_address = [1u8; 32];
        let amount = 10_000_000_000_000_000_000u128; // 10 REWARD
        
        // Mint and claim rewards first
        let pou_snapshot_high = PouSnapshot {
            epoch_id: 100,
            pou_score: 950, // High score
            blocks_proposed: 0,
            blocks_validated: 5,
            uptime_percent: 99.5,
            timestamp: 1640995200,
        };
        storage.mint_reward(&node_address, amount, NodeType::LightNode, pou_snapshot_high, 100).unwrap();
        storage.claim_rewards(&node_address).unwrap();

        // Convert with high PoU bonus
        let mainnet_amount = storage.convert_to_mainnet(
            &node_address,
            0.8,  // Base conversion rate
            90.0, // High bonus threshold
            75.0, // Mid bonus threshold
            0.20, // High bonus percent
            0.10, // Mid bonus percent
        ).unwrap();

        // Expected: 10 REWARD * 0.8 * 1.20 = 9.6 MAINNET
        let expected = (10.0 * 0.8 * 1.20) as u128;
        assert_eq!(mainnet_amount, expected);

        // Verify total converted
        let total_converted = storage.get_reward_total_converted().unwrap();
        assert_eq!(total_converted, amount);

        // Verify balance after conversion
        let balance = storage.get_reward_balance(&node_address).unwrap().unwrap();
        assert_eq!(balance.claimed, 0);
    }

    #[test]
    fn test_pou_history_tracking() {
        let storage = create_test_storage();
        let node_address = [1u8; 32];
        
        // Create multiple PoU snapshots
        let snapshots = vec![
            PouSnapshot {
                epoch_id: 100,
                pou_score: 800,
                blocks_proposed: 0,
                blocks_validated: 5,
                uptime_percent: 95.0,
                timestamp: 1640995200,
            },
            PouSnapshot {
                epoch_id: 101,
                pou_score: 850,
                blocks_proposed: 0,
                blocks_validated: 5,
                uptime_percent: 98.0,
                timestamp: 1641081600,
            },
            PouSnapshot {
                epoch_id: 102,
                pou_score: 900,
                blocks_proposed: 0,
                blocks_validated: 5,
                uptime_percent: 99.0,
                timestamp: 1641168000,
            },
        ];

        // Mint rewards with snapshots
        for (i, snapshot) in snapshots.iter().enumerate() {
            storage.mint_reward(
                &node_address,
                10_000_000_000_000_000_000u128,
                NodeType::LightNode,
                snapshot.clone(),
                100 + i as u64,
            ).unwrap();
        }

        // Get PoU history
        let history = storage.get_pou_history(&node_address).unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history, snapshots);

        // Calculate average PoU
        let avg_pou = history.iter().map(|s| s.pou_score as f64).sum::<f64>() / history.len() as f64;
        assert_eq!(avg_pou, 850.0);
    }

    #[test]
    fn test_reward_statistics() {
        let storage = create_test_storage();
        
        // Create multiple nodes with different balances
        let nodes = [
            ([1u8; 32], NodeType::LightNode, 100_000_000_000_000_000_000u128, 50_000_000_000_000_000_000u128),
            ([2u8; 32], NodeType::Masternode, 200_000_000_000_000_000_000u128, 100_000_000_000_000_000_000u128),
            ([3u8; 32], NodeType::LightNode, 50_000_000_000_000_000_000u128, 25_000_000_000_000_000_000u128),
        ];

        for (address, node_type, total, claimable) in nodes {
            let balance = RewardBalance {
                node_address: address,
                total_earned: total,
                claimable,
                claimed: total - claimable,
                node_type,
                registration_epoch: 100,
                last_reward_epoch: 105,
                pou_snapshots: Vec::new(),
            };
            storage.put_reward_balance(&address, &balance).unwrap();
        }

        // Get statistics
        let (total_minted, total_converted, total_claimable, total_claimed) = storage.get_reward_stats().unwrap();
        
        assert_eq!(total_minted, 350_000_000_000_000_000_000u128);
        assert_eq!(total_converted, 0);
        assert_eq!(total_claimable, 175_000_000_000_000_000_000u128);
        assert_eq!(total_claimed, 175_000_000_000_000_000_000u128);
    }

    #[test]
    fn test_nodes_by_type() {
        let storage = create_test_storage();
        
        // Create nodes of different types
        let light_node = RewardBalance {
            node_address: [1u8; 32],
            node_type: NodeType::LightNode,
            ..Default::default()
        };
        
        let masternode = RewardBalance {
            node_address: [2u8; 32],
            node_type: NodeType::Masternode,
            ..Default::default()
        };

        storage.put_reward_balance(&light_node.node_address, &light_node).unwrap();
        storage.put_reward_balance(&masternode.node_address, &masternode).unwrap();

        // Get nodes by type
        let light_nodes = storage.get_nodes_by_type(NodeType::LightNode).unwrap();
        let masternodes = storage.get_nodes_by_type(NodeType::Masternode).unwrap();

        assert_eq!(light_nodes.len(), 1);
        assert_eq!(masternodes.len(), 1);
        assert_eq!(light_nodes[0], [1u8; 32]);
        assert_eq!(masternodes[0], [2u8; 32]);
    }

    #[test]
    fn test_batch_balance_operations() {
        let storage = create_test_storage();
        let addresses = [[1u8; 32], [2u8; 32], [3u8; 32]];
        
        // Store balances for first two addresses
        let balance1 = RewardBalance {
            node_address: addresses[0],
            total_earned: 100_000_000_000_000_000_000u128,
            claimable: 50_000_000_000_000_000_000u128,
            claimed: 50_000_000_000_000_000_000u128,
            node_type: NodeType::LightNode,
            registration_epoch: 100,
            last_reward_epoch: 105,
            pou_snapshots: Vec::new(),
        };
        
        let balance2 = RewardBalance {
            node_address: addresses[1],
            total_earned: 200_000_000_000_000_000_000u128,
            claimable: 100_000_000_000_000_000_000u128,
            claimed: 100_000_000_000_000_000_000u128,
            node_type: NodeType::Masternode,
            registration_epoch: 100,
            last_reward_epoch: 105,
            pou_snapshots: Vec::new(),
        };

        storage.put_reward_balance(&addresses[0], &balance1).unwrap();
        storage.put_reward_balance(&addresses[1], &balance2).unwrap();

        // Batch retrieve
        let balances = storage.get_reward_balances_batch(&addresses).unwrap();
        assert_eq!(balances.len(), 3);
        assert!(balances[0].is_some());
        assert!(balances[1].is_some());
        assert!(balances[2].is_none()); // Third address has no balance
        
        assert_eq!(balances[0].as_ref().unwrap().total_earned, 100_000_000_000_000_000_000u128);
        assert_eq!(balances[1].as_ref().unwrap().total_earned, 200_000_000_000_000_000_000u128);
    }
}
