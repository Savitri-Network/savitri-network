//! Local Retry Manager for Masternode Registration Processing
//!
//! This module implements a local retry mechanism for failed lightnode registrations
//! to improve resilience and ensure all registrations are processed even during
//! network congestion or temporary failures.

use anyhow::{anyhow, Result};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::libp2p_network::LightnodeRegistrationMessage as LightnodeRegistration;

/// Configuration for retry behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Retry intervals in milliseconds (exponential backoff)
    pub retry_intervals_ms: Vec<u64>,
    /// Enable retry during peak traffic
    pub retry_on_peak: bool,
    /// Maximum age of failed registrations before cleanup
    pub max_failure_age_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_intervals_ms: vec![500, 1000, 2000], // 0.5s, 1s, 2s
            retry_on_peak: true,
            max_failure_age_secs: 300, // 5 minutes
        }
    }
}

/// Failed registration entry with retry tracking
#[derive(Debug, Clone)]
pub struct FailedRegistration {
    /// The registration data
    pub registration: LightnodeRegistration,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Timestamp of last failure
    pub last_failure: Instant,
    /// Original failure reason
    pub failure_reason: String,
    /// Peer ID for tracking
    pub peer_id: PeerId,
}

/// Local retry manager for handling failed registrations
pub struct LocalRetryManager {
    /// Failed registrations waiting for retry
    failed_registrations: HashMap<String, FailedRegistration>,
    /// Retry configuration
    config: RetryConfig,
    /// Statistics tracking
    stats: RetryStats,
    /// Peak traffic detection flag
    is_peak_traffic: bool,
}

/// Statistics for retry operations
#[derive(Debug, Default, Clone, Serialize)]
pub struct RetryStats {
    /// Total failed registrations
    pub total_failures: u64,
    /// Total successful retries
    pub successful_retries: u64,
    /// Total abandoned registrations (max retries exceeded)
    pub abandoned_registrations: u64,
    /// Average retry count
    pub avg_retry_count: f64,
    /// Current pending retries
    pub pending_retries: usize,
}

impl LocalRetryManager {
    /// Create a new retry manager with default configuration
    pub fn new() -> Self {
        Self::with_config(RetryConfig::default())
    }

    /// Create a new retry manager with custom configuration
    pub fn with_config(config: RetryConfig) -> Self {
        Self {
            failed_registrations: HashMap::new(),
            config,
            stats: RetryStats::default(),
            is_peak_traffic: false,
        }
    }

    /// Register a failed registration for retry
    pub fn register_failure(
        &mut self,
        peer_id: PeerId,
        registration: LightnodeRegistration,
        reason: String,
    ) {
        let peer_id_str = peer_id.to_string();

        // Check if already exists
        if let Some(existing) = self.failed_registrations.get_mut(&peer_id_str) {
            existing.retry_count += 1;
            existing.last_failure = Instant::now();
            existing.failure_reason = reason.clone();

            if existing.retry_count >= self.config.max_retries {
                warn!(
                    peer_id = %peer_id,
                    retry_count = existing.retry_count,
                    max_retries = self.config.max_retries,
                    "🔄 RETRY: Max retries exceeded for peer"
                );
                self.failed_registrations.remove(&peer_id_str);
                self.stats.abandoned_registrations += 1;
                return;
            }
        } else {
            let failed_reg = FailedRegistration {
                registration: registration.clone(),
                retry_count: 0,
                last_failure: Instant::now(),
                failure_reason: reason.clone(),
                peer_id,
            };

            self.failed_registrations
                .insert(peer_id_str.clone(), failed_reg);
            self.stats.total_failures += 1;
        }

        info!(
            peer_id = %peer_id,
            reason = %reason,
            retry_count = self.failed_registrations[&peer_id_str].retry_count,
            max_retries = self.config.max_retries,
            "🔄 RETRY: Registered failed registration for retry"
        );
    }

    /// Get registrations ready for retry based on timing
    pub fn get_ready_retries(&mut self) -> Vec<(PeerId, LightnodeRegistration)> {
        let mut ready_retries = Vec::new();
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (peer_id_str, failed_reg) in &self.failed_registrations {
            let retry_interval = self.get_retry_interval(failed_reg.retry_count);
            let time_since_failure = now.duration_since(failed_reg.last_failure);

            // Check if enough time has passed for retry
            if time_since_failure >= retry_interval {
                // Check if registration is not too old
                if time_since_failure.as_secs() <= self.config.max_failure_age_secs {
                    ready_retries.push((failed_reg.peer_id, failed_reg.registration.clone()));
                    to_remove.push(peer_id_str.clone());
                } else {
                    warn!(
                        peer_id = %failed_reg.peer_id,
                        age_secs = time_since_failure.as_secs(),
                        max_age = self.config.max_failure_age_secs,
                        "🔄 RETRY: Registration too old, abandoning"
                    );
                    to_remove.push(peer_id_str.clone());
                    self.stats.abandoned_registrations += 1;
                }
            }
        }

        // Remove processed registrations
        for peer_id_str in to_remove {
            self.failed_registrations.remove(&peer_id_str);
        }

        if !ready_retries.is_empty() {
            info!(
                retry_count = ready_retries.len(),
                pending = self.failed_registrations.len(),
                "🔄 RETRY: Processing {} ready retries",
                ready_retries.len()
            );
        }

        ready_retries
    }

    /// Get retry interval based on retry count (exponential backoff)
    fn get_retry_interval(&self, retry_count: u32) -> Duration {
        if retry_count < self.config.retry_intervals_ms.len() as u32 {
            Duration::from_millis(self.config.retry_intervals_ms[retry_count as usize])
        } else {
            // If we have more retries than configured intervals, use the last one
            let last_interval = self.config.retry_intervals_ms.last().unwrap_or(&2000);
            Duration::from_millis(*last_interval)
        }
    }

    /// Mark retry as successful
    pub fn mark_retry_success(&mut self, peer_id: &PeerId) {
        let peer_id_str = peer_id.to_string();

        if self.failed_registrations.remove(&peer_id_str).is_some() {
            self.stats.successful_retries += 1;
            info!(
                peer_id = %peer_id,
                "✅ RETRY: Registration retry successful"
            );
        }
    }

    /// Mark retry as failed (will be retried again if under max retries)
    pub fn mark_retry_failed(&mut self, peer_id: &PeerId, reason: String) {
        let peer_id_str = peer_id.to_string();

        if let Some(failed_reg) = self.failed_registrations.get_mut(&peer_id_str) {
            failed_reg.retry_count += 1;
            failed_reg.last_failure = Instant::now();
            failed_reg.failure_reason = reason.clone();

            if failed_reg.retry_count >= self.config.max_retries {
                warn!(
                    peer_id = %peer_id,
                    retry_count = failed_reg.retry_count,
                    max_retries = self.config.max_retries,
                    "🔄 RETRY: Max retries exceeded during retry"
                );
                self.failed_registrations.remove(&peer_id_str);
                self.stats.abandoned_registrations += 1;
            } else {
                info!(
                    peer_id = %peer_id,
                    retry_count = failed_reg.retry_count,
                    reason = %reason,
                    "🔄 RETRY: Retry failed, will retry again"
                );
            }
        }
    }

    /// Set peak traffic flag (affects retry behavior)
    pub fn set_peak_traffic(&mut self, is_peak: bool) {
        self.is_peak_traffic = is_peak;
        if is_peak {
            info!("🔥 PEAK: Peak traffic detected, enabling aggressive retry");
        } else {
            info!("📉 PEAK: Peak traffic ended, normal retry behavior");
        }
    }

    /// Cleanup old failed registrations
    pub fn cleanup_old_failures(&mut self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (peer_id_str, failed_reg) in &self.failed_registrations {
            let age = now.duration_since(failed_reg.last_failure);
            if age.as_secs() > self.config.max_failure_age_secs {
                to_remove.push(peer_id_str.clone());
                self.stats.abandoned_registrations += 1;
            }
        }

        for peer_id_str in &to_remove {
            self.failed_registrations.remove(peer_id_str);
        }

        if !to_remove.is_empty() {
            info!(
                cleaned_count = to_remove.len(),
                "🧹 RETRY: Cleaned up {} old failed registrations",
                to_remove.len()
            );
        }
    }

    /// Get current retry statistics
    pub fn get_stats(&self) -> RetryStats {
        let mut stats = self.stats.clone();
        stats.pending_retries = self.failed_registrations.len();

        // Calculate average retry count
        if self.stats.total_failures > 0 {
            let total_retries: u32 = self
                .failed_registrations
                .values()
                .map(|r| r.retry_count)
                .sum();
            stats.avg_retry_count = total_retries as f64 / self.stats.total_failures as f64;
        }

        stats
    }

    /// Get retry interval for next retry (adaptive based on peak traffic)
    pub fn get_adaptive_retry_interval(&self, retry_count: u32) -> Duration {
        let base_interval = self.get_retry_interval(retry_count);

        if self.is_peak_traffic && self.config.retry_on_peak {
            // Reduce retry interval during peak traffic for faster recovery
            let reduced_interval = base_interval / 2;
            debug!(
                base_interval_ms = base_interval.as_millis(),
                reduced_interval_ms = reduced_interval.as_millis(),
                "🔄 RETRY: Using reduced retry interval during peak traffic"
            );
            reduced_interval
        } else {
            base_interval
        }
    }

    /// Check if peer has failed registrations
    pub fn has_failed_registration(&self, peer_id: &PeerId) -> bool {
        self.failed_registrations.contains_key(&peer_id.to_string())
    }

    /// Get failure reason for a peer
    pub fn get_failure_reason(&self, peer_id: &PeerId) -> Option<String> {
        self.failed_registrations
            .get(&peer_id.to_string())
            .map(|r| r.failure_reason.clone())
    }

    /// Force retry of all pending registrations (useful during recovery)
    pub fn force_retry_all(&mut self) -> Vec<(PeerId, LightnodeRegistration)> {
        let mut all_retries = Vec::new();

        for (peer_id_str, failed_reg) in &self.failed_registrations {
            all_retries.push((failed_reg.peer_id, failed_reg.registration.clone()));
        }

        // Clear all pending retries
        let count = self.failed_registrations.len();
        self.failed_registrations.clear();

        if !all_retries.is_empty() {
            info!(
                retry_count = all_retries.len(),
                "🔄 RETRY: Force retrying {} pending registrations",
                all_retries.len()
            );
        }

        all_retries
    }

    /// Reset statistics (useful for testing or monitoring)
    pub fn reset_stats(&mut self) {
        self.stats = RetryStats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::libp2p_network::LightnodeRegistrationMessage as LightnodeRegistration;

    fn create_test_registration(peer_id: &str) -> LightnodeRegistration {
        LightnodeRegistration {
            node_id: peer_id.to_string(),
            peer_id: peer_id.to_string(),
            multiaddr: format!("/ip4/127.0.0.1/tcp/{}", 5000),
            geographic_region: "test".to_string(),
            pou_score: 0.5,
            capabilities: vec!["test".to_string()],
            uptime_percentage: 100.0,
            account: [0u8; 32],
        }
    }

    #[tokio::test]
    async fn test_retry_registration() {
        let mut retry_manager = LocalRetryManager::new();
        let peer_id = PeerId::random();
        let registration = create_test_registration(&peer_id.to_string());

        // Register failure
        retry_manager.register_failure(peer_id, registration.clone(), "Test failure".to_string());

        // Should have pending retry
        assert_eq!(retry_manager.get_stats().pending_retries, 1);

        // Get ready retries (should be ready immediately for test)
        let ready_retries = retry_manager.get_ready_retries();
        assert_eq!(ready_retries.len(), 1);
        assert_eq!(ready_retries[0].0, peer_id);

        // Mark as successful
        retry_manager.mark_retry_success(&peer_id);
        assert_eq!(retry_manager.get_stats().pending_retries, 0);
    }

    #[tokio::test]
    async fn test_max_retries() {
        let mut config = RetryConfig::default();
        config.max_retries = 2;
        let mut retry_manager = LocalRetryManager::with_config(config);

        let peer_id = PeerId::random();
        let registration = create_test_registration(&peer_id.to_string());

        // Register failure multiple times
        for i in 0..3 {
            retry_manager.register_failure(peer_id, registration.clone(), format!("Failure {}", i));
        }

        // Should be abandoned after max retries
        assert_eq!(retry_manager.get_stats().abandoned_registrations, 1);
        assert_eq!(retry_manager.get_stats().pending_retries, 0);
    }

    #[tokio::test]
    async fn test_peak_traffic_adaptive_retry() {
        let mut retry_manager = LocalRetryManager::new();
        let peer_id = PeerId::random();
        let registration = create_test_registration(&peer_id.to_string());

        // Register failure
        retry_manager.register_failure(peer_id, registration.clone(), "Test failure".to_string());

        // Normal retry interval
        let normal_interval = retry_manager.get_retry_interval(0);

        // Set peak traffic
        retry_manager.set_peak_traffic(true);

        // Adaptive retry interval should be shorter
        let adaptive_interval = retry_manager.get_adaptive_retry_interval(0);
        assert!(adaptive_interval < normal_interval);
    }
}
