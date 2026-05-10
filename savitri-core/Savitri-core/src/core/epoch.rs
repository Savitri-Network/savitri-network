//! Epoch tracking and management for Savitri Network
//! 
//! This module provides epoch tracking functionality for aligning
//! monolith generation and other time-based operations.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Epoch configuration and tracking information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochConfig {
    /// Epoch identifier
    pub epoch_id: u64,
    /// Start time of the epoch (Unix timestamp in seconds)
    pub start_time: u64,
    /// End time of the epoch (Unix timestamp in seconds)
    pub end_time: u64,
    /// Duration of the epoch in seconds
    pub duration: u64,
    /// Block height at epoch start
    pub start_height: u64,
    /// Block height at epoch end
    pub end_height: u64,
    /// Number of monoliths in this epoch
    pub monolith_count: u64,
    /// Epoch status
    pub status: EpochStatus,
}

/// Epoch status enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EpochStatus {
    /// Epoch is currently active
    Active,
    /// Epoch has completed
    Completed,
    /// Epoch is scheduled for future
    Scheduled,
}

impl EpochConfig {
    /// Create a new epoch configuration
    pub fn new(epoch_id: u64, start_time: u64, duration: u64, start_height: u64) -> Self {
        let end_time = start_time + duration;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let status = if current_time < start_time {
            EpochStatus::Scheduled
        } else if current_time >= end_time {
            EpochStatus::Completed
        } else {
            EpochStatus::Active
        };
        
        Self {
            epoch_id,
            start_time,
            end_time,
            duration,
            start_height,
            end_height: start_height, // Will be updated when epoch ends
            monolith_count: 0,
            status,
        }
    }
    
    /// Check if a timestamp falls within this epoch
    pub fn contains_timestamp(&self, timestamp: u64) -> bool {
        timestamp >= self.start_time && timestamp < self.end_time
    }
    
    /// Check if a block height falls within this epoch
    pub fn contains_height(&self, height: u64) -> bool {
        height >= self.start_height && height <= self.end_height
    }
    
    /// Get the progress of the epoch (0.0 to 1.0)
    pub fn progress(&self) -> f64 {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        if current_time <= self.start_time {
            0.0
        } else if current_time >= self.end_time {
            1.0
        } else {
            (current_time - self.start_time) as f64 / self.duration as f64
        }
    }
    
    /// Update epoch end height and mark as completed
    pub fn complete(&mut self, end_height: u64) {
        self.end_height = end_height;
        self.status = EpochStatus::Completed;
    }
    
    /// Increment monolith count for this epoch
    pub fn increment_monolith_count(&mut self) {
        self.monolith_count += 1;
    }
}

/// Epoch manager for tracking multiple epochs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochManager {
    /// Current epoch configuration
    pub current_epoch: EpochConfig,
    /// Epoch duration in seconds (default: 1 week)
    pub epoch_duration: u64,
    /// Block height for epoch boundaries
    pub blocks_per_epoch: u64,
    /// Starting epoch ID
    pub start_epoch_id: u64,
}

impl EpochManager {
    /// Create a new epoch manager
    pub fn new(epoch_duration: u64, blocks_per_epoch: u64, start_epoch_id: u64) -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let epoch_start_time = (current_time / epoch_duration) * epoch_duration;
        let epoch_id = start_epoch_id + (current_time / epoch_duration);
        let start_height = (epoch_id - start_epoch_id) * blocks_per_epoch;
        
        let current_epoch = EpochConfig::new(epoch_id, epoch_start_time, epoch_duration, start_height);
        
        Self {
            current_epoch,
            epoch_duration,
            blocks_per_epoch,
            start_epoch_id,
        }
    }
    
    /// Get the epoch ID for a given timestamp
    pub fn get_epoch_id_for_timestamp(&self, timestamp: u64) -> u64 {
        self.start_epoch_id + (timestamp / self.epoch_duration)
    }
    
    /// Get the epoch ID for a given block height
    pub fn get_epoch_id_for_height(&self, height: u64) -> u64 {
        self.start_epoch_id + (height / self.blocks_per_epoch)
    }
    
    /// Check if we should advance to the next epoch
    pub fn should_advance_epoch(&self, current_height: u64) -> bool {
        current_height >= self.current_epoch.end_height
    }
    
    /// Advance to the next epoch
    pub fn advance_epoch(&mut self, current_height: u64) -> EpochConfig {
        // Complete current epoch
        let mut completed_epoch = self.current_epoch.clone();
        completed_epoch.complete(current_height);
        
        // Create next epoch
        let next_epoch_id = self.current_epoch.epoch_id + 1;
        let next_start_time = self.current_epoch.end_time;
        let next_start_height = self.current_epoch.end_height + 1;
        
        self.current_epoch = EpochConfig::new(next_epoch_id, next_start_time, self.epoch_duration, next_start_height);
        
        completed_epoch
    }
    
    /// Get epoch summary for logging/debugging
    pub fn get_epoch_summary(&self) -> String {
        format!(
            "Epoch {}: {} - {} ({} blocks), Status: {:?}, Progress: {:.2}%",
            self.current_epoch.epoch_id,
            self.current_epoch.start_time,
            self.current_epoch.end_time,
            self.current_epoch.end_height - self.current_epoch.start_height + 1,
            self.current_epoch.status,
            self.current_epoch.progress() * 100.0
        )
    }
}

impl Default for EpochManager {
    fn default() -> Self {
        Self::new(
            7 * 24 * 60 * 60, // 1 week in seconds
            10080,           // ~10080 blocks per week (assuming 10s block time)
            0,                // Start from epoch 0
        )
    }
}

/// Calculate epoch ID from timestamp and duration
pub fn calculate_epoch_id(timestamp: u64, epoch_duration: u64, start_epoch_id: u64) -> u64 {
    start_epoch_id + (timestamp / epoch_duration)
}

/// Validate epoch configuration
pub fn validate_epoch_config(config: &EpochConfig) -> Result<(), String> {
    if config.end_time <= config.start_time {
        return Err("Epoch end time must be after start time".to_string());
    }
    
    if config.duration != config.end_time - config.start_time {
        return Err("Epoch duration mismatch".to_string());
    }
    
    if config.end_height < config.start_height {
        return Err("Epoch end height must be >= start height".to_string());
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_epoch_creation() {
        let epoch = EpochConfig::new(1, 1000000, 604800, 0); // 1 week epoch
        
        assert_eq!(epoch.epoch_id, 1);
        assert_eq!(epoch.start_time, 1000000);
        assert_eq!(epoch.end_time, 1604800);
        assert_eq!(epoch.duration, 604800);
        assert_eq!(epoch.start_height, 0);
        assert_eq!(epoch.monolith_count, 0);
    }
    
    #[test]
    fn test_epoch_contains_timestamp() {
        let epoch = EpochConfig::new(1, 1000000, 604800, 0);
        
        assert!(epoch.contains_timestamp(1000000));  // Start time
        assert!(epoch.contains_timestamp(1300000));  // Middle
        assert!(epoch.contains_timestamp(1604799));  // End - 1
        assert!(!epoch.contains_timestamp(1604800)); // End time (exclusive)
        assert!(!epoch.contains_timestamp(999999));  // Before start
    }
    
    #[test]
    fn test_epoch_manager() {
        let manager = EpochManager::default();
        
        assert_eq!(manager.epoch_duration, 7 * 24 * 60 * 60);
        assert_eq!(manager.blocks_per_epoch, 10080);
        assert_eq!(manager.start_epoch_id, 0);
    }
    
    #[test]
    fn test_epoch_id_calculation() {
        let epoch_id = calculate_epoch_id(1000000, 604800, 0);
        assert_eq!(epoch_id, 1); // 1000000 / 604800 = 1
        
        let epoch_id = calculate_epoch_id(2000000, 604800, 5);
        assert_eq!(epoch_id, 8); // (2000000 / 604800) + 5 = 8
    }
}
