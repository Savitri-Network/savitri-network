//! Unified Slot/Epoch System
//!
//! Single source of truth for slot, round, epoch, and monolith epoch.
//! See UNIFIED_SLOT_CONFIG_RECOMMENDATION.md for formulas.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

/// Unified slot/epoch configuration. All values in milliseconds.
#[derive(Debug, Clone)]
pub struct UnifiedSlotConfig {
    /// 1 slot = 1 heartbeat (ms). Default 5000.
    pub heartbeat_interval_ms: u64,
    /// Slots per epoch. Default 20 → epoch = 100000 ms.
    pub slots_per_epoch: u64,
    /// Target monolith epoch duration (ms). Default 86400000 (24 h).
    pub monolith_epoch_ms: u64,
    /// Genesis timestamp (ms).
    pub genesis_timestamp_ms: u64,
}

impl Default for UnifiedSlotConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 5000,
            slots_per_epoch: 20,
            monolith_epoch_ms: 86400000,
            genesis_timestamp_ms: 0,
        }
    }
}

impl UnifiedSlotConfig {
    pub fn new(
        heartbeat_interval_ms: u64,
        slots_per_epoch: u64,
        monolith_epoch_ms: u64,
        genesis_timestamp_ms: u64,
    ) -> Self {
        Self {
            heartbeat_interval_ms: heartbeat_interval_ms.max(1),
            slots_per_epoch: slots_per_epoch.max(1),
            monolith_epoch_ms,
            genesis_timestamp_ms,
        }
    }

    /// Epochs per monolith epoch: M = monolith_epoch_ms / (H × S)
    pub fn epochs_per_monolith(&self) -> u64 {
        let epoch_ms = self
            .heartbeat_interval_ms
            .saturating_mul(self.slots_per_epoch);
        if epoch_ms == 0 {
            return 864; // fallback
        }
        self.monolith_epoch_ms / epoch_ms
    }

    /// Elapsed time since genesis (ms)
    pub fn elapsed_ms(&self) -> Result<u64> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before UNIX_EPOCH")?;
        let now_ms: u64 = now.as_millis().try_into().context("time beyond u64")?;
        Ok(now_ms.saturating_sub(self.genesis_timestamp_ms))
    }

    /// current_slot = τ / H
    pub fn current_slot(&self) -> Result<u64> {
        let elapsed = self.elapsed_ms()?;
        Ok(elapsed / self.heartbeat_interval_ms.max(1))
    }

    /// current_epoch = current_slot / S
    pub fn current_epoch(&self) -> Result<u64> {
        Ok(self.current_slot()? / self.slots_per_epoch.max(1))
    }

    /// current_round = current_slot mod S
    pub fn current_round(&self) -> Result<u32> {
        let slot = self.current_slot()?;
        Ok((slot % self.slots_per_epoch.max(1)) as u32)
    }

    /// current_monolith_epoch = current_epoch / M
    pub fn current_monolith_epoch(&self) -> Result<u64> {
        Ok(self.current_epoch()? / self.epochs_per_monolith().max(1))
    }

    /// Epoch duration in ms: H × S
    pub fn epoch_duration_ms(&self) -> u64 {
        self.heartbeat_interval_ms
            .saturating_mul(self.slots_per_epoch)
    }

    /// Slot start time (ms since UNIX epoch) for a given slot
    pub fn slot_start_ms(&self, slot: u64) -> u64 {
        self.genesis_timestamp_ms
            .saturating_add(slot.saturating_mul(self.heartbeat_interval_ms))
    }

    /// Compute slot/epoch/round at a given timestamp (ms)
    pub fn at_timestamp_ms(&self, timestamp_ms: u64) -> (u64, u64, u32) {
        let elapsed = timestamp_ms.saturating_sub(self.genesis_timestamp_ms);
        let slot = elapsed / self.heartbeat_interval_ms.max(1);
        let epoch = slot / self.slots_per_epoch.max(1);
        let round = (slot % self.slots_per_epoch.max(1)) as u32;
        (slot, epoch, round)
    }

    /// True when we have entered a new monolith epoch (produce monolith block)
    pub fn is_monolith_boundary(&self) -> Result<bool> {
        let elapsed = self.elapsed_ms()?;
        let epoch_ms = self.epoch_duration_ms();
        if epoch_ms == 0 {
            return Ok(false);
        }
        let current_epoch = elapsed / epoch_ms;
        let m = self.epochs_per_monolith();
        if m == 0 {
            return Ok(false);
        }
        // At boundary: current_epoch % M == 0 and we're at start of epoch
        let time_in_epoch = elapsed % epoch_ms;
        Ok(current_epoch % m == 0 && time_in_epoch < self.heartbeat_interval_ms)
    }
}
