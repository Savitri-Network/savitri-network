//! Fee Module
//!
//! Complete fee-related functionality for the Savitri blockchain including
//! fee calculation, distribution, treasury management, and halving mechanics.

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Fee configuration for the blockchain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    /// Base fee for all transactions (in wei units)
    pub base_fee: u64,
    /// Priority fee multiplier for fast processing
    pub priority_fee: u64,
    /// Gas price (wei per gas unit)
    pub gas_price: u64,
    /// Maximum gas limit per block
    pub max_gas_limit: u64,
    /// Minimum fee to prevent spam
    pub min_fee: u64,
}

impl Default for FeeConfig {
    fn default() -> Self {
        Self {
            base_fee: 1000,            // 1000 wei base fee
            priority_fee: 2000,        // 2000 wei for priority
            gas_price: 1,              // 1 wei per gas
            max_gas_limit: 10_000_000, // 10M gas per block
            min_fee: 100,              // 100 wei minimum
        }
    }
}

/// Fee calculator for transaction processing
#[derive(Debug, Clone)]
pub struct FeeCalculator {
    config: FeeConfig,
    metrics: FeeMetrics,
}

impl FeeCalculator {
    /// Create new fee calculator with configuration
    pub fn new(config: FeeConfig) -> Self {
        Self {
            config,
            metrics: FeeMetrics::default(),
        }
    }

    /// Calculate transaction fee based on gas usage and priority
    pub fn calculate_fee(&self, gas_used: u64, is_priority: bool) -> u64 {
        let base_fee = self.config.base_fee.max(self.config.min_fee);
        let gas_cost = gas_used.saturating_mul(self.config.gas_price);

        let priority_surcharge = if is_priority {
            self.config.priority_fee
        } else {
            0
        };

        let total_fee = base_fee
            .saturating_add(gas_cost)
            .saturating_add(priority_surcharge);

        total_fee
    }

    /// Calculate minimum fee required for transaction
    pub fn calculate_min_fee(&self) -> u64 {
        self.config.min_fee
    }

    /// Get current fee metrics
    pub fn get_metrics(&self) -> &FeeMetrics {
        &self.metrics
    }

    /// Reset fee metrics
    pub fn reset_metrics(&mut self) {
        self.metrics = FeeMetrics::default();
    }
}

/// Fee metrics tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FeeMetrics {
    /// Total fees collected
    pub total_fees: u128,
    /// Average fee per transaction
    pub avg_fee: u64,
    /// Number of transactions processed
    pub transaction_count: u64,
    /// Total gas consumed
    pub total_gas: u64,
    /// Average gas per transaction
    pub avg_gas: u64,
}

impl FeeMetrics {
    /// Record a fee transaction
    pub fn record_fee(&mut self, fee: u64) {
        self.total_fees += fee as u128;
        self.transaction_count += 1;

        if self.transaction_count > 0 {
            self.avg_fee = (self.total_fees / self.transaction_count as u128) as u64;
        }
    }

    /// Record gas usage
    pub fn record_gas(&mut self, gas: u64) {
        self.total_gas += gas;

        if self.transaction_count > 0 {
            self.avg_gas = self.total_gas / self.transaction_count;
        }
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Treasury for managing collected fees
#[derive(Debug, Clone, Default)]
pub struct Treasury {
    balance: u128,
    total_collected: u128,
    total_burned: u128,
    last_update: u64,
}

impl Treasury {
    /// Create new treasury instance
    pub fn new() -> Self {
        Self {
            balance: 0,
            total_collected: 0,
            total_burned: 0,
            last_update: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Deposit fees into treasury
    pub fn deposit(&mut self, amount: u128) -> Result<()> {
        self.balance = self.balance.saturating_add(amount);
        self.total_collected = self.total_collected.saturating_add(amount);
        self.last_update = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Ok(())
    }

    /// Withdraw funds from treasury
    pub fn withdraw(&mut self, amount: u128) -> Result<()> {
        if self.balance >= amount {
            self.balance = self.balance.saturating_sub(amount);
            self.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Ok(())
        } else {
            anyhow::bail!(
                "Insufficient treasury balance: available={}, required={}",
                self.balance,
                amount
            )
        }
    }

    /// Spend from treasury to a destination address (destination is informational).
    ///
    /// SECURITY: Enforces a 5% cap per spending proposal to prevent treasury drain.
    /// A single proposal cannot spend more than 5% of the current treasury balance.
    pub fn spend(
        &mut self,
        _storage: &crate::storage::Storage,
        amount: u128,
        _destination: &str,
    ) -> Result<()> {
        // SECURITY FIX: Enforce 5% cap per proposal
        let max_spend = self.balance / 20; // 5% of current balance
        if amount > max_spend {
            anyhow::bail!(
                "Amount {} exceeds 5% treasury limit (max spend: {}, balance: {})",
                amount,
                max_spend,
                self.balance
            );
        }
        self.withdraw(amount)
    }

    /// Burn tokens (reduce total supply)
    pub fn burn(&mut self, amount: u128) -> Result<()> {
        if self.balance >= amount {
            self.balance = self.balance.saturating_sub(amount);
            self.total_burned = self.total_burned.saturating_add(amount);
            self.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Ok(())
        } else {
            anyhow::bail!(
                "Insufficient treasury balance for burning: available={}, required={}",
                self.balance,
                amount
            )
        }
    }

    /// Get current treasury balance
    pub fn get_balance(&self) -> u128 {
        self.balance
    }

    /// Get total fees collected
    pub fn get_total_collected(&self) -> u128 {
        self.total_collected
    }

    /// Get total fees burned
    pub fn get_total_burned(&self) -> u128 {
        self.total_burned
    }

    /// Get last update timestamp
    pub fn get_last_update(&self) -> u64 {
        self.last_update
    }
}

/// Fee distribution module for managing fee allocation
pub mod distribution {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// Fee distributor for allocating fees to different recipients
    #[derive(Debug, Clone)]
    pub struct FeeDistributor {
        config: FeeDistribution,
        treasury: Treasury,
    }

    impl FeeDistributor {
        /// Create new fee distributor with configuration
        pub fn new(config: FeeDistribution, treasury: Treasury) -> Self {
            Self { config, treasury }
        }

        /// Distribute fees according to configured rates
        pub fn distribute(&mut self, total_amount: u128) -> Result<DistributionAmounts> {
            let amounts = DistributionAmounts::from_total(
                total_amount,
                self.config.burn_rate,
                self.config.treasury_rate,
                self.config.validator_rate,
            );

            // Execute the distribution
            if amounts.burn_amount > 0 {
                self.treasury.burn(amounts.burn_amount)?;
            }

            if amounts.treasury_amount > 0 {
                self.treasury.deposit(amounts.treasury_amount)?;
            }

            // For now, we just log them
            tracing::info!(
                burn_amount = amounts.burn_amount,
                treasury_amount = amounts.treasury_amount,
                validator_amount = amounts.validator_amount,
                proposer_amount = amounts.proposer_amount,
                "Fee distribution completed"
            );

            Ok(amounts)
        }
    }

    /// Distribution amounts for fee splitting
    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct DistributionAmounts {
        pub burn_amount: u128,
        pub treasury_amount: u128,
        pub validator_amount: u128,
        pub proposer_amount: u128,
        pub treasury: u128,
        pub masternode: u128,
        pub proposer_p2p: u128,
    }

    impl DistributionAmounts {
        /// Create distribution amounts from total and percentages
        pub fn from_total(
            total: u128,
            burn_pct: u16,
            treasury_pct: u16,
            validator_pct: u16,
        ) -> Self {
            let burn_amount = (total * burn_pct as u128) / 10000;
            let treasury_amount = (total * treasury_pct as u128) / 10000;
            let validator_amount = (total * validator_pct as u128) / 10000;
            let proposer_amount =
                total.saturating_sub(burn_amount + treasury_amount + validator_amount);

            Self {
                burn_amount,
                treasury_amount,
                validator_amount,
                proposer_amount,
                treasury: 0,
                masternode: 0,
                proposer_p2p: 0,
            }
        }

        /// Get total distributed amount
        pub fn total(&self) -> u128 {
            self.burn_amount + self.treasury_amount + self.validator_amount + self.proposer_amount
        }
    }
}

/// Burn engine for token burning operations
#[derive(Debug, Clone)]
pub struct BurnEngine {
    total_burned: u128,
    burn_rate: u16,
    current_volume: u128,
}

impl BurnEngine {
    /// Create new burn engine
    pub fn new(burn_rate: u16) -> Self {
        Self {
            total_burned: 0,
            burn_rate,
            current_volume: 0,
        }
    }

    /// Burn specified amount of tokens
    pub fn burn(&mut self, amount: u128) -> Result<()> {
        if amount > 0 {
            self.total_burned = self.total_burned.saturating_add(amount);
            tracing::info!(
                amount = amount,
                total_burned = self.total_burned,
                "Tokens burned successfully"
            );
        }
        Ok(())
    }

    /// Update burn volume tracking
    pub fn update_volume(&mut self, volume: u128) -> Result<()> {
        self.current_volume = volume;
        tracing::debug!(volume = volume, "Burn volume updated");
        Ok(())
    }

    /// Calculate burn amount based on storage state
    pub fn calculate_burn_amount_from_storage(
        &self,
        _storage: &crate::storage::Storage,
    ) -> Result<u128> {
        // In a real implementation, this would calculate based on current
        // storage state and burn rate
        let base_amount = 1000u128; // Base burn amount
        let volume_adjusted = (self.current_volume * self.burn_rate as u128) / 10000;
        Ok(base_amount.saturating_add(volume_adjusted))
    }

    /// Execute burn operation
    pub fn execute_burn(&mut self, amount: u128) -> Result<()> {
        self.burn(amount)
    }
}

impl Default for BurnEngine {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Fee distribution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeDistribution {
    /// Percentage of fees to burn (basis points, 10000 = 100%)
    pub burn_rate: u16,
    /// Percentage of fees to treasury (basis points, 10000 = 100%)
    pub treasury_rate: u16,
    pub validator_rate: u16,
}

impl Default for FeeDistribution {
    fn default() -> Self {
        Self {
            burn_rate: 5000,      // 50% burn
            treasury_rate: 3000,  // 30% to treasury
            validator_rate: 2000, // 20% to validators
        }
    }
}

impl FeeDistribution {
    pub fn distribute_fees_after_burn(
        &self,
        net_fee: u128,
        _storage: &crate::storage::Storage,
        _overlay: &mut std::collections::BTreeMap<Vec<u8>, savitri_core::core::types::Account>,
        _halving_engine: &HalvingEngine,
        _current_timestamp: u64,
        _masternode_address: &[u8; 32],
        _proposer_address: &[u8; 32],
        _p2p_nodes: Option<Vec<([u8; 32], u64)>>,
    ) -> Result<distribution::DistributionAmounts> {
        // Keep a simple deterministic split for compatibility with current callers.
        let treasury = net_fee / 10;
        let masternode = net_fee / 10;
        let proposer_p2p = net_fee.saturating_sub(treasury).saturating_sub(masternode);
        Ok(distribution::DistributionAmounts {
            burn_amount: 0,
            treasury_amount: treasury,
            validator_amount: masternode,
            proposer_amount: proposer_p2p,
            treasury,
            masternode,
            proposer_p2p,
        })
    }
}

/// Main fee engine for coordinating all fee operations
#[derive(Debug, Clone)]
pub struct FeeEngine {
    calculator: FeeCalculator,
    distributor: distribution::FeeDistributor,
    treasury: Treasury,
    burn_engine: BurnEngine,
    distribution: FeeDistribution,
}

impl FeeEngine {
    /// Create new fee engine with configuration
    pub fn new(config: FeeConfig, distribution: FeeDistribution) -> Self {
        Self {
            calculator: FeeCalculator::new(config),
            distributor: distribution::FeeDistributor::new(distribution.clone(), Treasury::new()),
            treasury: Treasury::new(),
            burn_engine: BurnEngine::new(distribution.burn_rate),
            distribution,
        }
    }

    /// Process fee collection and distribution
    pub fn process_fees(&mut self, total_amount: u128) -> Result<()> {
        // Distribute fees according to configuration
        let distribution_amounts = self.distributor.distribute(total_amount)?;

        tracing::info!(
            total_amount = total_amount,
            distribution = distribution_amounts.total(),
            "Fee processing completed"
        );

        Ok(())
    }

    /// Get fee calculator
    pub fn calculator(&self) -> &FeeCalculator {
        &self.calculator
    }

    /// Get treasury
    pub fn treasury(&self) -> &Treasury {
        &self.treasury
    }

    /// Get burn engine
    pub fn burn_engine(&self) -> &BurnEngine {
        &self.burn_engine
    }

    /// Get current configuration
    pub fn get_config(&self) -> FeeConfig {
        self.calculator.config.clone()
    }

    /// Update configuration
    pub fn update_config(&mut self, config: FeeConfig) {
        self.calculator = FeeCalculator::new(config);
    }

    /// Update distribution configuration
    pub fn update_distribution(&mut self, distribution: FeeDistribution) {
        self.distribution = distribution.clone();
        self.distributor =
            distribution::FeeDistributor::new(distribution.clone(), self.treasury.clone());
    }
}

impl Default for FeeEngine {
    fn default() -> Self {
        Self::new(FeeConfig::default(), FeeDistribution::default())
    }
}

/// Halving engine for managing block rewards
#[derive(Debug, Clone)]
pub struct HalvingEngine {
    current_epoch: u64,
    halving_interval: u64,
    initial_reward: u128,
}

impl HalvingEngine {
    /// Create new halving engine
    pub fn new(halving_interval: u64, initial_reward: u128) -> Self {
        Self {
            current_epoch: 0,
            halving_interval,
            initial_reward,
        }
    }

    /// Load halving engine from storage
    pub fn from_storage(_storage: &crate::storage::Storage) -> Result<Self> {
        // In a real implementation, this would load from storage
        // For now, use default values
        // 25_000_000 blocks × 5s ≈ 4 years (Bitcoin-style halving cycle)
        Ok(Self::new(25_000_000, 50_000_000_000))
    }

    /// Get current block reward
    pub fn get_current_reward(&self) -> u128 {
        let halvings = self.current_epoch / self.halving_interval;
        self.initial_reward >> halvings.min(63) // Prevent overflow
    }

    /// Get current epoch
    pub fn get_current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Advance to next epoch
    pub fn advance_epoch(&mut self) {
        self.current_epoch += 1;
        tracing::info!(
            new_epoch = self.current_epoch,
            reward = self.get_current_reward(),
            "Advanced to new epoch"
        );
    }

    /// Check if halving event is due
    pub fn is_halving_due(&self) -> bool {
        self.current_epoch > 0 && (self.current_epoch % self.halving_interval == 0)
    }

    /// Get blocks until next halving
    pub fn blocks_until_halving(&self) -> u64 {
        let next_halving_epoch =
            ((self.current_epoch / self.halving_interval) + 1) * self.halving_interval;
        next_halving_epoch - self.current_epoch
    }

    /// Get halving schedule information
    pub fn get_halving_info(&self) -> HalvingInfo {
        let next_halving_epoch =
            ((self.current_epoch / self.halving_interval) + 1) * self.halving_interval;
        let blocks_until_halving = next_halving_epoch - self.current_epoch;

        HalvingInfo {
            current_epoch: self.current_epoch,
            current_reward: self.get_current_reward(),
            next_halving_epoch,
            blocks_until_halving,
            halving_interval: self.halving_interval,
            total_halvings: self.current_epoch / self.halving_interval,
        }
    }
}

/// Information about halving schedule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HalvingInfo {
    pub current_epoch: u64,
    pub current_reward: u128,
    pub next_halving_epoch: u64,
    pub blocks_until_halving: u64,
    pub halving_interval: u64,
    pub total_halvings: u64,
}
