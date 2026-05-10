// SPDX-License-Identifier: MIT
// © 2026 Savitri Network

//! Core slot scheduler for Savitri Network
//! 
//! This module provides deterministic slot scheduling and leader rotation
//! without external dependencies. Designed for use in any Savitri component.

use std::time::{Duration, SystemTime, UNIX_EPOCH};
use anyhow::{bail, Context, Result};

/// Role of a node in a given slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRole {
    Leader,
    Follower,
    Observer,
}

impl SlotRole {
    pub fn is_leader(self) -> bool {
        matches!(self, SlotRole::Leader)
    }

    pub fn is_validator(self) -> bool {
        matches!(self, SlotRole::Leader | SlotRole::Follower)
    }
}

/// Information about a specific slot
#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub slot: u64,
    pub round: u32,
    pub leader: Option<String>,
    pub role: SlotRole,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl SlotInfo {
    pub fn is_leader(&self) -> bool {
        self.role.is_leader()
    }

    pub fn leader_id(&self) -> Option<&str> {
        self.leader.as_deref()
    }
}

/// Configuration for slot scheduler
#[derive(Debug, Clone)]
pub struct SlotSchedulerConfig {
    pub slot_duration: Duration,
    pub validators: Vec<String>,
    pub local_id: String,
    pub slot_base_ms: Option<u64>,
}

impl SlotSchedulerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.slot_duration.is_zero() {
            bail!("slot_duration must be greater than zero");
        }
        if self.validators.is_empty() {
            bail!("validator set must not be empty");
        }
        if self.local_id.trim().is_empty() {
            bail!("local validator id must not be empty");
        }
        if self.slot_base_ms.is_none() {
            bail!("slot_base_ms must be configured to ensure deterministic leader rotation");
        }
        Ok(())
    }
}

/// Core slot scheduler implementation
#[derive(Debug, Clone)]
pub struct SlotScheduler {
    slot_duration_ms: u64,
    base_ms: u64,
    validators: Vec<String>,
    local_id: String,
    is_validator: bool,
    last_slot: u64,
}

impl SlotScheduler {
    /// Create a new slot scheduler with given configuration
    pub fn new(cfg: SlotSchedulerConfig) -> Result<Self> {
        cfg.validate()?;
        
        let slot_duration_ms: u64 = cfg
            .slot_duration
            .as_millis()
            .try_into()
            .context("slot_duration exceeds u64 range")?;

        let base_ms = cfg.slot_base_ms
            .context("slot_base_ms must be provided when initializing slot scheduler")?;

        let is_validator = cfg.validators.iter().any(|v| v == &cfg.local_id);
        
        let now_slot = slot_from_time(base_ms, slot_duration_ms, current_millis()?);

        Ok(Self {
            slot_duration_ms,
            base_ms,
            validators: cfg.validators,
            local_id: cfg.local_id,
            is_validator,
            last_slot: now_slot,
        })
    }

    /// Get current slot information
    pub fn current_slot_info(&self) -> Result<SlotInfo> {
        let now_ms = current_millis()?;
        self.slot_info_at(now_ms)
    }

    /// Get slot information at a specific timestamp
    pub fn slot_info_at(&self, now_ms: u64) -> Result<SlotInfo> {
        let mut slot = slot_from_time(self.base_ms, self.slot_duration_ms, now_ms);
        if slot < self.last_slot {
            slot = self.last_slot;
        }

        let start_ms = self.base_ms + slot * self.slot_duration_ms;
        let end_ms = start_ms + self.slot_duration_ms;

        let leader = if self.validators.is_empty() {
            None
        } else {
            let idx = (slot as usize) % self.validators.len();
            Some(self.validators[idx].clone())
        };

        let role = match (&leader, self.is_validator) {
            (Some(l), true) if *l == self.local_id => SlotRole::Leader,
            (Some(_), true) => SlotRole::Follower,
            _ => SlotRole::Observer,
        };

        let round = if self.validators.is_empty() {
            0
        } else {
            (slot % self.validators.len() as u64) as u32
        };

        Ok(SlotInfo {
            slot,
            round,
            leader,
            role,
            start_ms,
            end_ms,
        })
    }

    /// Get the next slot start timestamp
    pub fn next_slot_start_ms(&self) -> Result<u64> {
        let now_ms = current_millis()?;
        let mut slot = slot_from_time(self.base_ms, self.slot_duration_ms, now_ms);
        if slot < self.last_slot {
            slot = self.last_slot;
        }
        let next_slot = slot.saturating_add(1);
        Ok(self.base_ms + next_slot * self.slot_duration_ms)
    }

    /// Update the last processed slot
    pub fn update_last_slot(&mut self, slot: u64) {
        if slot > self.last_slot {
            self.last_slot = slot;
        }
    }

    pub fn is_validator(&self) -> bool {
        self.is_validator
    }

    pub fn validators(&self) -> &[String] {
        &self.validators
    }

    /// Get slot duration in milliseconds
    pub fn slot_duration_ms(&self) -> u64 {
        self.slot_duration_ms
    }
}

/// Calculate slot number from timestamp
fn slot_from_time(base_ms: u64, slot_duration_ms: u64, now_ms: u64) -> u64 {
    if now_ms <= base_ms {
        0
    } else {
        (now_ms - base_ms) / slot_duration_ms
    }
}

/// Get current timestamp in milliseconds
fn current_millis() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?;
    Ok(now
        .as_millis()
        .try_into()
        .context("system time beyond u64 range")?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> SlotSchedulerConfig {
        SlotSchedulerConfig {
            slot_duration: Duration::from_millis(1000),
            validators: vec![
                "validator1".to_string(),
                "validator2".to_string(),
                "validator3".to_string(),
            ],
            local_id: "validator1".to_string(),
            slot_base_ms: Some(1000000),
        }
    }

    #[test]
    fn test_slot_scheduler_creation() {
        let config = create_test_config();
        let scheduler = SlotScheduler::new(config).unwrap();
        
        assert!(scheduler.is_validator());
        assert_eq!(scheduler.validators().len(), 3);
        assert_eq!(scheduler.slot_duration_ms(), 1000);
    }

    #[test]
    fn test_slot_info_calculation() {
        let config = create_test_config();
        let scheduler = SlotScheduler::new(config).unwrap();
        
        let slot_info = scheduler.current_slot_info().unwrap();
        
        assert!(slot_info.slot >= 0);
        assert!(slot_info.round < 3);
        assert_eq!(slot_info.start_ms, 1000000 + slot_info.slot * 1000);
        assert_eq!(slot_info.end_ms, slot_info.start_ms + 1000);
    }

    #[test]
    fn test_leader_rotation() {
        let mut config = create_test_config();
        
        for (i, validator_id) in ["validator1", "validator2", "validator3"].iter().enumerate() {
            config.local_id = validator_id.to_string();
            let scheduler = SlotScheduler::new(config.clone()).unwrap();
            
            for slot_offset in 0..10 {
                let test_ms = 1000000 + (slot_offset as u64 * 1000) + 500; // Middle of slot
                let slot_info = scheduler.slot_info_at(test_ms).unwrap();
                
                if slot_info.slot as usize % 3 == i {
                    assert_eq!(slot_info.role, SlotRole::Leader);
                    assert_eq!(slot_info.leader_id(), Some(*validator_id));
                    break;
                }
            }
        }
    }

    #[test]
    fn test_observer_role() {
        let mut config = create_test_config();
        config.local_id = "non_validator".to_string();
        
        let scheduler = SlotScheduler::new(config).unwrap();
        
        assert!(!scheduler.is_validator());
        
        let slot_info = scheduler.current_slot_info().unwrap();
        assert_eq!(slot_info.role, SlotRole::Observer);
    }

    #[test]
    fn test_follower_role() {
        let mut config = create_test_config();
        config.local_id = "validator2".to_string();
        
        let scheduler = SlotScheduler::new(config).unwrap();
        
        for slot_offset in 0..10 {
            let test_ms = 1000000 + (slot_offset as u64 * 1000) + 500;
            let slot_info = scheduler.slot_info_at(test_ms).unwrap();
            
            if slot_info.leader_id() == Some("validator1") {
                assert_eq!(slot_info.role, SlotRole::Follower);
                break;
            }
        }
    }

    #[test]
    fn test_next_slot_start() {
        let config = create_test_config();
        let scheduler = SlotScheduler::new(config).unwrap();
        
        let next_start = scheduler.next_slot_start_ms().unwrap();
        let current_info = scheduler.current_slot_info().unwrap();
        
        // Next slot start should be after current slot ends
        assert!(next_start >= current_info.end_ms);
    }

    #[test]
    fn test_invalid_config() {
        let mut config = create_test_config();
        
        // Test zero slot duration
        config.slot_duration = Duration::from_millis(0);
        assert!(SlotScheduler::new(config.clone()).is_err());
        
        config.slot_duration = Duration::from_millis(1000);
        config.validators.clear();
        assert!(SlotScheduler::new(config.clone()).is_err());
        
        // Test empty local ID
        config.validators = vec!["validator1".to_string()];
        config.local_id = "".to_string();
        assert!(SlotScheduler::new(config.clone()).is_err());
        
        // Test missing base timestamp
        config.local_id = "validator1".to_string();
        config.slot_base_ms = None;
        assert!(SlotScheduler::new(config).is_err());
    }

    #[test]
    fn test_deterministic_behavior() {
        let config = create_test_config();
        let scheduler1 = SlotScheduler::new(config.clone()).unwrap();
        let scheduler2 = SlotScheduler::new(config).unwrap();
        
        // Same configuration should produce same results
        let info1 = scheduler1.current_slot_info().unwrap();
        let info2 = scheduler2.current_slot_info().unwrap();
        
        assert_eq!(info1.slot, info2.slot);
        assert_eq!(info1.leader, info2.leader);
        assert_eq!(info1.role, info2.role);
    }
}
