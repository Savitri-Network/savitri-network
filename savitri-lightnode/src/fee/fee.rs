//! Fee engine module for Savitri Light Node
//!
//! This module provides fee calculation and management for transactions.

#![allow(dead_code)] // Fee engine types for future use

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Fee configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    /// Base fee
    pub base_fee: u128,
    /// Fee per byte
    pub fee_per_byte: u128,
    /// Fee per gas unit
    pub fee_per_gas: u128,
    /// Minimum fee
    pub min_fee: u128,
    /// Maximum fee
    pub max_fee: u128,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            base_fee: 1000,   // 0.001 tokens
            fee_per_byte: 1,  // 1 token per KB
            fee_per_gas: 1,   // 1 token per gas unit
            min_fee: 1000,    // 0.001 tokens minimum
            max_fee: 1000000, // 1 token maximum
        }
    }
}

/// Fee engine
#[derive(Debug)]
pub struct FeeEngine {
    /// Configuration
    config: FeeConfig,
    /// Priority fee levels
    priority_levels: Vec<FeeLevel>,
}

/// Fee priority level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeLevel {
    /// Level name
    pub name: String,
    /// Minimum fee
    pub min_fee: u128,
    /// Maximum fee
    pub max_fee: u128,
    /// Multiplier for base fee
    pub multiplier: f64,
}

impl FeeEngine {
    /// Create new fee engine
    pub fn new(config: FeeConfig) -> Self {
        let priority_levels = vec![
            FeeLevel {
                name: "low".to_string(),
                min_fee: config.min_fee,
                max_fee: config.base_fee * 10,
                multiplier: 1.0,
            },
            FeeLevel {
                name: "medium".to_string(),
                min_fee: config.base_fee * 10,
                max_fee: config.base_fee * 100,
                multiplier: 2.0,
            },
            FeeLevel {
                name: "high".to_string(),
                min_fee: config.base_fee * 100,
                max_fee: config.max_fee,
                multiplier: 5.0,
            },
        ];

        Self {
            config,
            priority_levels,
        }
    }

    /// Calculate transaction fee
    pub fn calculate_fee(&self, tx_size: usize, gas_limit: u64, priority: &str) -> Result<u128> {
        // Find priority level
        let level = self
            .priority_levels
            .iter()
            .find(|l| l.name == priority)
            .unwrap_or(&self.priority_levels[0]);

        // Calculate base fee
        let base_fee = (self.config.base_fee as f64 * level.multiplier) as u128;

        // Calculate size fee
        let size_fee = (tx_size as u128) * self.config.fee_per_byte;

        // Calculate gas fee
        let gas_fee = (gas_limit as u128) * self.config.fee_per_gas;

        // Total fee
        let total_fee = base_fee + size_fee + gas_fee;

        // Apply min/max limits
        let final_fee = total_fee
            .max(level.min_fee)
            .min(level.max_fee)
            .max(self.config.min_fee)
            .min(self.config.max_fee);

        Ok(final_fee)
    }

    /// Get fee level by fee amount
    pub fn get_fee_level(&self, fee: u128) -> &str {
        for level in &self.priority_levels {
            if fee >= level.min_fee && fee <= level.max_fee {
                return &level.name;
            }
        }
        &self.priority_levels[0].name
    }

    /// Estimate fee for transaction
    pub fn estimate_fee(
        &self,
        tx_size: usize,
        gas_limit: u64,
        priority: &str,
    ) -> Result<FeeEstimate> {
        let fee = self.calculate_fee(tx_size, gas_limit, priority)?;
        let level = self.get_fee_level(fee);

        Ok(FeeEstimate {
            fee,
            level: level.to_string(),
            gas_limit,
            tx_size,
            priority: priority.to_string(),
        })
    }

    /// Validate fee
    pub fn validate_fee(&self, fee: u128, tx_size: usize, gas_limit: u64) -> Result<bool> {
        // Check minimum fee
        if fee < self.config.min_fee {
            return Ok(false);
        }

        // Check maximum fee
        if fee > self.config.max_fee {
            return Ok(false);
        }

        // Check if fee covers size cost
        let size_cost = (tx_size as u128) * self.config.fee_per_byte;
        if fee < size_cost {
            return Ok(false);
        }

        // Check if fee covers gas cost
        let gas_cost = (gas_limit as u128) * self.config.fee_per_gas;
        if fee < gas_cost {
            return Ok(false);
        }

        Ok(true)
    }
}

/// Fee estimation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeEstimate {
    /// Estimated fee
    pub fee: u128,
    /// Fee level
    pub level: String,
    /// Gas limit
    pub gas_limit: u64,
    /// Transaction size
    pub tx_size: usize,
    /// Priority
    pub priority: String,
}

/// Dual token engine for advanced fee calculation
#[derive(Debug)]
pub struct DualTokenEngine {
    /// Primary fee engine
    primary: FeeEngine,
    /// Secondary fee engine
    secondary: FeeEngine,
    /// Exchange rate
    exchange_rate: f64,
}

impl DualTokenEngine {
    /// Create new dual token engine
    pub fn new(primary_config: FeeConfig, secondary_config: FeeConfig, exchange_rate: f64) -> Self {
        Self {
            primary: FeeEngine::new(primary_config),
            secondary: FeeEngine::new(secondary_config),
            exchange_rate,
        }
    }

    /// Calculate fee in primary tokens
    pub fn calculate_primary_fee(
        &self,
        tx_size: usize,
        gas_limit: u64,
        priority: &str,
    ) -> Result<u128> {
        self.primary.calculate_fee(tx_size, gas_limit, priority)
    }

    /// Calculate fee in secondary tokens
    pub fn calculate_secondary_fee(
        &self,
        tx_size: usize,
        gas_limit: u64,
        priority: &str,
    ) -> Result<u128> {
        let primary_fee = self.primary.calculate_fee(tx_size, gas_limit, priority)?;
        let secondary_fee = (primary_fee as f64 / self.exchange_rate) as u128;
        Ok(secondary_fee)
    }

    /// Convert primary to secondary tokens
    pub fn primary_to_secondary(&self, amount: u128) -> u128 {
        (amount as f64 / self.exchange_rate) as u128
    }

    /// Convert secondary to primary tokens
    pub fn secondary_to_primary(&self, amount: u128) -> u128 {
        (amount as f64 * self.exchange_rate) as u128
    }

    /// Get exchange rate
    pub fn exchange_rate(&self) -> f64 {
        self.exchange_rate
    }

    /// Update exchange rate
    pub fn update_exchange_rate(&mut self, new_rate: f64) {
        self.exchange_rate = new_rate;
    }
}

/// Fee market for dynamic pricing
#[derive(Debug)]
pub struct FeeMarket {
    /// Base fee
    pub base_fee: u128,
    /// Current demand
    pub demand: u64,
    /// Current supply
    pub supply: u64,
    /// Fee history
    pub fee_history: Vec<(u64, u128)>, // (timestamp, fee)
}

impl FeeMarket {
    /// Create new fee market
    pub fn new(base_fee: u128) -> Self {
        Self {
            base_fee,
            demand: 0,
            supply: 0,
            fee_history: Vec::new(),
        }
    }

    /// Calculate market fee
    pub fn calculate_market_fee(&self) -> u128 {
        if self.demand == 0 {
            return self.base_fee;
        }

        // Simple supply/demand pricing
        let demand_ratio = self.demand as f64 / (self.supply.max(1) as f64);
        let multiplier = 1.0 + (demand_ratio - 1.0).max(0.0).min(10.0); // Max 10x multiplier
        (self.base_fee as f64 * multiplier) as u128
    }

    /// Update demand
    pub fn update_demand(&mut self, demand: u64) {
        self.demand = demand;
    }

    /// Update supply
    pub fn update_supply(&mut self, supply: u64) {
        self.supply = supply;
    }

    /// Record fee
    pub fn record_fee(&mut self, fee: u128) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.fee_history.push((timestamp, fee));

        // Keep only last 1000 records
        if self.fee_history.len() > 1000 {
            self.fee_history.remove(0);
        }
    }

    /// Get average fee over last N records
    pub fn get_average_fee(&self, last_n: usize) -> Option<u128> {
        let len = self.fee_history.len();
        if len == 0 {
            return None;
        }

        let start = if len > last_n { len - last_n } else { 0 };
        let sum: u128 = self.fee_history[start..].iter().map(|(_, fee)| *fee).sum();

        Some(sum / (len - start) as u128)
    }
}
