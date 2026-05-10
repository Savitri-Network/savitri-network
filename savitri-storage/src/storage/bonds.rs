//! Bond/Stake System: Storage-backed bond management
//!
//! persistent storage support.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Column family per bond e stake
pub const CF_BONDS: &str = "bonds";

/// Default bond amount in base token (1,000,000 tokens = 1M)
pub const DEFAULT_BOND_AMOUNT: u128 = 1_000_000_000_000_000;

/// Minimum bond amount
pub const MIN_BOND_AMOUNT: u128 = 100_000_000_000_000;

/// Maximum bond amount
pub const MAX_BOND_AMOUNT: u128 = 10_000_000_000_000_000;

/// Slashing percentages (in basis points, 10,000 = 100%)
pub const DEFAULT_SLASH_PCT_EQUIVOCATION: u16 = 5_000;
pub const DEFAULT_SLASH_PCT_DOUBLE_VOTE: u16 = 2_500;
pub const DEFAULT_SLASH_PCT_INVALID_ATTESTATION: u16 = 1_000;

/// Minimum slashing percentage
pub const MIN_SLASH_PCT: u16 = 100;

/// Maximum slashing percentage
pub const MAX_SLASH_PCT: u16 = 10_000;

/// Unbonding period in blocks
pub const UNBONDING_PERIOD_BLOCKS: u64 = 21_600; // ~3 days at 12s block time

/// Bond info struct
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BondInfo {
    pub validator: Vec<u8>,
    pub amount: u128,
    pub bonded_at: u64,
    pub status: BondStatus,
    pub unbonding_start: Option<u64>,
}

impl BondInfo {
    /// Create a new active bond
    pub fn new(validator: Vec<u8>, amount: u128, bonded_at: u64) -> Self {
        Self {
            validator,
            amount,
            bonded_at,
            status: BondStatus::Active,
            unbonding_start: None,
        }
    }

    /// Check if bond is active
    pub fn is_active(&self) -> bool {
        self.status == BondStatus::Active
    }

    /// Start unbonding process
    pub fn start_unbonding(&mut self, current_block: u64) {
        self.status = BondStatus::Unbonding;
        self.unbonding_start = Some(current_block);
    }

    /// Check if unbonding period is complete
    pub fn is_unbonding_complete(&self, current_block: u64) -> bool {
        if let Some(start) = self.unbonding_start {
            current_block >= start + UNBONDING_PERIOD_BLOCKS
        } else {
            false
        }
    }
}

/// Bond status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum BondStatus {
    Active,
    Unbonding,
    Slashed,
}

/// Slashing reason for audit trail
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SlashingReason {
    Equivocation,
    DoubleVote,
    InvalidAttestation,
}

/// Slashing parameters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SlashingParams {
    pub equivocation_pct: u16,
    pub double_vote_pct: u16,
    pub invalid_attestation_pct: u16,
    pub min_bond_amount: u128,
    pub slash_pct_equivocation: u16,
    pub slash_pct_double_vote: u16,
    pub slash_pct_invalid_attestation: u16,
    pub updated_at: u64,
}

impl Default for SlashingParams {
    fn default() -> Self {
        Self {
            equivocation_pct: DEFAULT_SLASH_PCT_EQUIVOCATION,
            double_vote_pct: DEFAULT_SLASH_PCT_DOUBLE_VOTE,
            invalid_attestation_pct: DEFAULT_SLASH_PCT_INVALID_ATTESTATION,
            min_bond_amount: MIN_BOND_AMOUNT,
            slash_pct_equivocation: DEFAULT_SLASH_PCT_EQUIVOCATION,
            slash_pct_double_vote: DEFAULT_SLASH_PCT_DOUBLE_VOTE,
            slash_pct_invalid_attestation: DEFAULT_SLASH_PCT_INVALID_ATTESTATION,
            updated_at: 0,
        }
    }
}

impl SlashingParams {
    /// Get slashing percentage for a given reason
    pub fn get_slash_pct(&self, reason: SlashingReason) -> u16 {
        match reason {
            SlashingReason::Equivocation => self.slash_pct_equivocation,
            SlashingReason::DoubleVote => self.slash_pct_double_vote,
            SlashingReason::InvalidAttestation => self.slash_pct_invalid_attestation,
        }
    }

    /// Calculate slash amount for a bond
    pub fn calculate_slash_amount(&self, bond_amount: u128, reason: SlashingReason) -> u128 {
        let pct = self.get_slash_pct(reason) as u128;
        bond_amount.saturating_mul(pct).saturating_div(10_000)
    }
}

/// Slashing event for history tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashingEvent {
    pub validator: Vec<u8>,
    pub reason: SlashingReason,
    pub amount_slashed: u128,
    pub block_height: u64,
    pub timestamp: u64,
}

/// Bond manager with in-memory cache and storage persistence
#[derive(Debug, Clone)]
pub struct BondManager {
    bonds: Arc<RwLock<HashMap<Vec<u8>, BondInfo>>>,
    slashing_params: Arc<RwLock<SlashingParams>>,
    slashing_history: Arc<RwLock<Vec<SlashingEvent>>>,
    total_bonded: Arc<RwLock<u128>>,
}

impl Default for BondManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BondManager {
    /// Create a new BondManager
    pub fn new() -> Self {
        Self {
            bonds: Arc::new(RwLock::new(HashMap::new())),
            slashing_params: Arc::new(RwLock::new(SlashingParams::default())),
            slashing_history: Arc::new(RwLock::new(Vec::new())),
            total_bonded: Arc::new(RwLock::new(0)),
        }
    }

    pub fn has_active_bond(&self, validator: &[u8]) -> bool {
        if let Ok(bonds) = self.bonds.read() {
            bonds.get(validator).map(|b| b.is_active()).unwrap_or(false)
        } else {
            false
        }
    }

    pub fn get_bond(&self, validator: &[u8]) -> Option<BondInfo> {
        self.bonds.read().ok()?.get(validator).cloned()
    }

    pub fn create_bond(
        &self,
        validator: Vec<u8>,
        amount: u128,
        current_block: u64,
    ) -> anyhow::Result<()> {
        // Validate amount
        let params = self
            .slashing_params
            .read()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        if amount < params.min_bond_amount {
            anyhow::bail!(
                "Bond amount {} is below minimum {}",
                amount,
                params.min_bond_amount
            );
        }

        if amount > MAX_BOND_AMOUNT {
            anyhow::bail!("Bond amount {} exceeds maximum {}", amount, MAX_BOND_AMOUNT);
        }

        drop(params);

        let mut bonds = self
            .bonds
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        if let Some(existing) = bonds.get(&validator) {
            if existing.is_active() {
                anyhow::bail!("Validator already has an active bond");
            }
        }

        let bond = BondInfo::new(validator.clone(), amount, current_block);
        bonds.insert(validator, bond);

        // Update total bonded
        let mut total = self
            .total_bonded
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        *total = total.saturating_add(amount);

        Ok(())
    }

    pub fn start_unbonding(&self, validator: &[u8], current_block: u64) -> anyhow::Result<()> {
        let mut bonds = self
            .bonds
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let bond = bonds
            .get_mut(validator)
            .ok_or_else(|| anyhow::anyhow!("Validator has no bond"))?;

        if !bond.is_active() {
            anyhow::bail!("Bond is not active");
        }

        bond.start_unbonding(current_block);
        Ok(())
    }

    /// Complete unbonding and release funds
    pub fn complete_unbonding(&self, validator: &[u8], current_block: u64) -> anyhow::Result<u128> {
        let mut bonds = self
            .bonds
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let bond = bonds
            .get(validator)
            .ok_or_else(|| anyhow::anyhow!("Validator has no bond"))?;

        if bond.status != BondStatus::Unbonding {
            anyhow::bail!("Bond is not in unbonding state");
        }

        if !bond.is_unbonding_complete(current_block) {
            anyhow::bail!("Unbonding period not complete");
        }

        let amount = bond.amount;
        bonds.remove(validator);

        // Update total bonded
        let mut total = self
            .total_bonded
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        *total = total.saturating_sub(amount);

        Ok(amount)
    }

    pub fn slash(
        &self,
        validator: &[u8],
        reason: SlashingReason,
        block_height: u64,
    ) -> anyhow::Result<u128> {
        let params = self
            .slashing_params
            .read()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let mut bonds = self
            .bonds
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let bond = bonds
            .get_mut(validator)
            .ok_or_else(|| anyhow::anyhow!("Validator has no bond"))?;

        let slash_amount = params.calculate_slash_amount(bond.amount, reason);
        bond.amount = bond.amount.saturating_sub(slash_amount);
        bond.status = BondStatus::Slashed;

        drop(params);
        drop(bonds);

        // Record slashing event
        let event = SlashingEvent {
            validator: validator.to_vec(),
            reason,
            amount_slashed: slash_amount,
            block_height,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        };

        if let Ok(mut history) = self.slashing_history.write() {
            history.push(event);
        }

        // Update total bonded
        if let Ok(mut total) = self.total_bonded.write() {
            *total = total.saturating_sub(slash_amount);
        }

        Ok(slash_amount)
    }

    /// Get current slashing parameters
    pub fn get_slashing_params(&self) -> SlashingParams {
        self.slashing_params
            .read()
            .map(|p| p.clone())
            .unwrap_or_default()
    }

    /// Update slashing parameters
    pub fn update_slashing_params(&self, params: SlashingParams) -> anyhow::Result<()> {
        // Validate parameters
        if params.slash_pct_equivocation < MIN_SLASH_PCT
            || params.slash_pct_equivocation > MAX_SLASH_PCT
        {
            anyhow::bail!("Equivocation slash percentage out of range");
        }
        if params.slash_pct_double_vote < MIN_SLASH_PCT
            || params.slash_pct_double_vote > MAX_SLASH_PCT
        {
            anyhow::bail!("Double vote slash percentage out of range");
        }
        if params.slash_pct_invalid_attestation < MIN_SLASH_PCT
            || params.slash_pct_invalid_attestation > MAX_SLASH_PCT
        {
            anyhow::bail!("Invalid attestation slash percentage out of range");
        }
        if params.min_bond_amount < MIN_BOND_AMOUNT {
            anyhow::bail!("Minimum bond amount too low");
        }

        let mut current = self
            .slashing_params
            .write()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        *current = params;

        Ok(())
    }

    /// Get total bonded amount
    pub fn get_total_bonded(&self) -> u128 {
        self.total_bonded.read().map(|t| *t).unwrap_or(0)
    }

    pub fn get_active_validators(&self) -> Vec<Vec<u8>> {
        self.bonds
            .read()
            .map(|bonds| {
                bonds
                    .iter()
                    .filter(|(_, b)| b.is_active())
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn get_slashing_history(&self, validator: &[u8]) -> Vec<SlashingEvent> {
        self.slashing_history
            .read()
            .map(|history| {
                history
                    .iter()
                    .filter(|e| e.validator == validator)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Serialize bonds for storage persistence
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let bonds = self
            .bonds
            .read()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        let params = self
            .slashing_params
            .read()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;
        let total = *self
            .total_bonded
            .read()
            .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

        let data = (bonds.clone(), params.clone(), total);
        Ok(bincode::serialize(&data)?)
    }

    /// Deserialize bonds from storage
    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        let (bonds, params, total): (HashMap<Vec<u8>, BondInfo>, SlashingParams, u128) =
            crate::safe_deserialize(data)?;

        Ok(Self {
            bonds: Arc::new(RwLock::new(bonds)),
            slashing_params: Arc::new(RwLock::new(params)),
            slashing_history: Arc::new(RwLock::new(Vec::new())),
            total_bonded: Arc::new(RwLock::new(total)),
        })
    }
}
