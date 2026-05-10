//! Adaptive Latency Management
//!
//! This module provides adaptive latency management based on network conditions
//! similar to the masternode implementation.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::config::AdaptiveLatencyConfig;

/// Adaptive latency manager
pub struct AdaptiveLatencyManager {
    config: AdaptiveLatencyConfig,
    latency_history: HashMap<String, Vec<u64>>,
    current_epoch: u64,
    last_recalculation_epoch: u64,
    current_adaptive_threshold: u64,
}

impl AdaptiveLatencyManager {
    /// Create a new adaptive latency manager
    pub fn new(config: AdaptiveLatencyConfig) -> Self {
        Self {
            current_adaptive_threshold: config.base_latency_threshold_ms,
            config,
            latency_history: HashMap::new(),
            current_epoch: 0,
            last_recalculation_epoch: 0,
        }
    }

    /// Record latency measurement for a peer
    pub fn record_latency(&mut self, peer_id: &str, latency_ms: u64) {
        let history = self
            .latency_history
            .entry(peer_id.to_string())
            .or_insert_with(Vec::new);
        history.push(latency_ms);

        // Keep only last 100 measurements per peer
        if history.len() > 100 {
            history.drain(0..history.len() - 100);
        }

        debug!("Recorded latency for {}: {}ms", peer_id, latency_ms);
    }

    /// Calculate adaptive latency based on network average for epoch
    pub async fn calculate_adaptive_latency(&mut self, current_epoch: u64) -> u64 {
        self.current_epoch = current_epoch;

        if !self.config.enable_adaptive_latency {
            return self.config.base_latency_threshold_ms;
        }

        // Check if we should recalculate
        if current_epoch < self.last_recalculation_epoch + self.config.latency_recalculation_epochs
        {
            return self.current_adaptive_threshold;
        }

        let mut all_latencies: Vec<u64> = self
            .latency_history
            .values()
            .flat_map(|history| history.iter().copied())
            .collect();

        if all_latencies.is_empty() {
            warn!("No latency data available for adaptive calculation");
            return self.config.base_latency_threshold_ms;
        }

        // Calculate network average latency
        all_latencies.sort();
        let median_idx = all_latencies.len() / 2;
        let avg_latency = if all_latencies.len() % 2 == 0 {
            (all_latencies[median_idx - 1] + all_latencies[median_idx]) / 2
        } else {
            all_latencies[median_idx]
        };

        // Calculate adaptive threshold
        let adaptive_threshold =
            (avg_latency as f64 * self.config.latency_adaptation_factor) as u64;

        // Ensure minimum threshold
        let final_threshold = adaptive_threshold.max(self.config.base_latency_threshold_ms);

        self.current_adaptive_threshold = final_threshold;
        self.last_recalculation_epoch = current_epoch;

        info!(
            "Calculated adaptive latency: {}ms (avg: {}ms, factor: {:.2})",
            final_threshold, avg_latency, self.config.latency_adaptation_factor
        );

        final_threshold
    }

    /// Get current adaptive latency threshold
    pub fn current_threshold(&self) -> u64 {
        self.current_adaptive_threshold
    }

    /// Check if a peer is within adaptive latency threshold
    pub fn is_peer_within_threshold(&self, peer_id: &str) -> bool {
        if let Some(history) = self.latency_history.get(peer_id) {
            if let Some(&latest_latency) = history.last() {
                return latest_latency <= self.current_adaptive_threshold;
            }
        }
        false
    }

    /// Get peer statistics
    pub fn get_peer_stats(&self, peer_id: &str) -> Option<PeerLatencyStats> {
        let history = self.latency_history.get(peer_id)?;
        if history.is_empty() {
            return None;
        }

        let sorted = {
            let mut sorted = history.clone();
            sorted.sort();
            sorted
        };

        Some(PeerLatencyStats {
            peer_id: peer_id.to_string(),
            latest_latency: history.last().copied().unwrap_or(0),
            average_latency: if history.is_empty() {
                0.0
            } else {
                history.iter().sum::<u64>() as f64 / history.len() as f64
            },
            median_latency: if sorted.is_empty() {
                0
            } else {
                sorted[sorted.len() / 2]
            },
            measurements_count: history.len(),
            within_threshold: history.last().copied().unwrap_or(0)
                <= self.current_adaptive_threshold,
        })
    }

    /// Get all peer statistics
    pub fn get_all_peer_stats(&self) -> Vec<PeerLatencyStats> {
        self.latency_history
            .keys()
            .filter_map(|peer_id| self.get_peer_stats(peer_id))
            .collect()
    }

    /// Cleanup old latency data
    pub fn cleanup_old_data(&mut self, max_age: Duration) {
        let cutoff = Instant::now() - max_age;

        // This is a simplified cleanup - in a real implementation,
        // we'd need to track timestamps for each measurement
        for history in self.latency_history.values_mut() {
            if history.len() > 50 {
                history.drain(0..history.len() - 50);
            }
        }

        debug!("Cleaned up old latency data");
    }
}

/// Peer latency statistics
#[derive(Debug, Clone)]
pub struct PeerLatencyStats {
    pub peer_id: String,
    pub latest_latency: u64,
    pub average_latency: f64,
    pub median_latency: u64,
    pub measurements_count: usize,
    pub within_threshold: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_adaptive_latency_calculation() {
        let config = AdaptiveLatencyConfig {
            enable_adaptive_latency: true,
            base_latency_threshold_ms: 150,
            latency_adaptation_factor: 1.5,
            latency_recalculation_epochs: 50,
            base_latency_ms: 100,
            max_adjustment_percent: 50.0,
            window_size: 10,
            adjustment_threshold: 0.2,
        };

        let mut manager = AdaptiveLatencyManager::new(config);

        // Add some test latency data
        for i in 0..10 {
            manager.record_latency(&format!("peer_{}", i), 100 + (i * 10));
        }

        let threshold = manager.calculate_adaptive_latency(100).await;
        assert!(threshold >= 150); // Should be at least base threshold
        assert!(threshold <= 200); // Should be reasonable
    }

    #[test]
    fn test_peer_within_threshold() {
        let config = AdaptiveLatencyConfig::default();
        let mut manager = AdaptiveLatencyManager::new(config);

        manager.record_latency("test_peer", 100);
        let threshold = manager.current_threshold();

        assert!(manager.is_peer_within_threshold("test_peer"));
        assert!(!manager.is_peer_within_threshold("unknown_peer"));
    }
}
