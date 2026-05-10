//! Adaptive Batch Collector with Peak Detection and Retry Integration
//!
//! This module implements an adaptive batch collector that can handle peak traffic
//! patterns and integrate with the retry mechanism for improved resilience.

use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use super::retry_manager::{LocalRetryManager, RetryConfig};
use crate::libp2p_network::LightnodeRegistrationMessage as LightnodeRegistration;

/// Configuration for adaptive batch processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptiveBatchConfig {
    /// Maximum number of messages to collect before auto-flush
    pub max_batch_size: usize,
    /// Base timeout for batch processing
    pub base_timeout_ms: u64,
    /// Enable adaptive timeout based on traffic patterns
    pub adaptive_timeout: bool,
    /// Enable peak detection
    pub peak_detection: bool,
    /// Peak detection threshold (messages per second)
    pub peak_threshold: usize,
    /// Peak detection window in seconds
    pub peak_window_seconds: u64,
    /// Timeout multiplier during peak traffic
    pub peak_timeout_multiplier: f64,
    /// Minimum batch size for immediate processing during peak
    pub peak_immediate_batch_size: usize,
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 8,
            base_timeout_ms: 50, // Reduced from 500ms → 100ms → 50ms for maximum finalization throughput
            adaptive_timeout: true,
            peak_detection: true,
            peak_threshold: 5, // 5+ messages per second = peak
            peak_window_seconds: 1,
            peak_timeout_multiplier: 1.5, // 50ms × 1.5 = 75ms during peak (was 1500ms)
            peak_immediate_batch_size: 3, // Process immediately if 3+ messages during peak
        }
    }
}

/// Peak traffic detector
#[derive(Debug)]
pub struct PeakDetector {
    /// Recent message timestamps
    recent_messages: VecDeque<Instant>,
    /// Peak detection configuration
    threshold: usize,
    window: Duration,
    /// Current peak state
    is_in_peak: bool,
    /// Peak start time
    peak_start: Option<Instant>,
    /// Peak statistics
    peak_stats: PeakStats,
}

/// Statistics for peak detection
#[derive(Debug, Default, Clone, Serialize)]
pub struct PeakStats {
    /// Number of peaks detected
    pub total_peaks: u64,
    /// Total time spent in peak state
    pub total_peak_duration_ms: u64,
    /// Average peak duration
    pub avg_peak_duration_ms: f64,
    /// Current peak duration
    pub current_peak_duration_ms: u64,
}

impl PeakDetector {
    /// Create a new peak detector
    pub fn new(threshold: usize, window_seconds: u64) -> Self {
        Self {
            recent_messages: VecDeque::new(),
            threshold,
            window: Duration::from_secs(window_seconds),
            is_in_peak: false,
            peak_start: None,
            peak_stats: PeakStats::default(),
        }
    }

    /// Register a message event and detect peaks
    pub fn register_message(&mut self) -> bool {
        let now = Instant::now();
        self.recent_messages.push_back(now);

        // Clean up old messages outside the window
        while let Some(&front) = self.recent_messages.front() {
            if now.duration_since(front) > self.window {
                self.recent_messages.pop_front();
            } else {
                break;
            }
        }

        let message_rate = self.recent_messages.len();
        let was_in_peak = self.is_in_peak;
        self.is_in_peak = message_rate >= self.threshold;

        // Detect peak transitions
        if self.is_in_peak && !was_in_peak {
            // Peak started
            self.peak_start = Some(now);
            self.peak_stats.total_peaks += 1;
            info!(
                message_rate = message_rate,
                threshold = self.threshold,
                "🔥 PEAK: Peak traffic detected"
            );
            return true;
        } else if !self.is_in_peak && was_in_peak {
            // Peak ended
            if let Some(start) = self.peak_start {
                let duration = now.duration_since(start);
                self.peak_stats.total_peak_duration_ms += duration.as_millis() as u64;

                if self.peak_stats.total_peaks > 0 {
                    self.peak_stats.avg_peak_duration_ms = self.peak_stats.total_peak_duration_ms
                        as f64
                        / self.peak_stats.total_peaks as f64;
                }

                info!(
                    duration_ms = duration.as_millis(),
                    "📉 PEAK: Peak traffic ended"
                );
            }
            self.peak_start = None;
        }

        false // No new peak started
    }

    /// Check if currently in peak state
    pub fn is_in_peak(&self) -> bool {
        self.is_in_peak
    }

    /// Get current peak duration
    pub fn current_peak_duration(&self) -> Duration {
        if let Some(start) = self.peak_start {
            Instant::now().duration_since(start)
        } else {
            Duration::ZERO
        }
    }

    /// Get peak statistics
    pub fn get_stats(&self) -> PeakStats {
        let mut stats = self.peak_stats.clone();
        stats.current_peak_duration_ms = self.current_peak_duration().as_millis() as u64;
        stats
    }
}

/// Adaptive batch collector with peak detection
pub struct AdaptivePeerInfoBatchCollector {
    /// Collected messages waiting to be processed
    messages: Vec<(PeerId, Vec<u8>)>,
    /// When the current batch started collection
    batch_start: Instant,
    /// Configuration for adaptive batch behavior
    config: AdaptiveBatchConfig,
    /// Peak detector
    peak_detector: PeakDetector,
    /// Retry manager
    retry_manager: LocalRetryManager,
    /// Statistics tracking
    stats: AdaptiveBatchStats,
    /// Current adaptive timeout
    current_timeout: Duration,
}

/// Statistics for adaptive batch processing
#[derive(Debug, Default, Clone, Serialize)]
pub struct AdaptiveBatchStats {
    /// Total messages processed
    pub total_messages: usize,
    /// Total batches processed
    pub total_batches: usize,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Total time saved by batching
    pub time_saved_ms: u64,
    /// Number of peaks handled
    pub peaks_handled: u64,
    /// Average processing time per batch
    pub avg_processing_time_ms: f64,
    /// Number of adaptive timeout adjustments
    pub timeout_adjustments: u64,
}

impl AdaptivePeerInfoBatchCollector {
    /// Create a new adaptive batch collector with default configuration
    pub fn new() -> Self {
        Self::with_config(AdaptiveBatchConfig::default())
    }

    /// Create a new adaptive batch collector with custom configuration
    pub fn with_config(config: AdaptiveBatchConfig) -> Self {
        let base_timeout = Duration::from_millis(config.base_timeout_ms);
        let peak_detector = PeakDetector::new(config.peak_threshold, config.peak_window_seconds);

        Self {
            messages: Vec::with_capacity(config.max_batch_size),
            batch_start: Instant::now(),
            current_timeout: base_timeout,
            config,
            peak_detector,
            retry_manager: LocalRetryManager::new(),
            stats: AdaptiveBatchStats::default(),
        }
    }

    /// Add a new message to the batch with adaptive logic
    /// Returns Some(batch) if the batch is ready to be processed
    pub fn add_message(
        &mut self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Option<Vec<(PeerId, Vec<u8>)>> {
        // Register message for peak detection
        let new_peak_detected = self.peak_detector.register_message();

        // Update retry manager peak state
        self.retry_manager
            .set_peak_traffic(self.peak_detector.is_in_peak());

        // Adjust timeout if peak detected or ended
        if new_peak_detected || (self.peak_detector.is_in_peak() && self.config.adaptive_timeout) {
            self.adjust_timeout_for_peak();
        }

        info!(
            peer_id = %peer_id,
            data_size = data.len(),
            batch_size_before = self.messages.len(),
            is_peak = self.peak_detector.is_in_peak(),
            current_timeout_ms = self.current_timeout.as_millis(),
            "📦 ADAPTIVE BATCH: Adding message to batch collector"
        );

        self.messages.push((peer_id, data));
        self.stats.total_messages += 1;

        // Check if batch should be flushed with adaptive logic
        if self.should_flush_adaptive() {
            info!(
                batch_size = self.messages.len(),
                is_peak = self.peak_detector.is_in_peak(),
                "🚀 ADAPTIVE BATCH: Flushing batch - ready for processing"
            );
            self.flush_batch()
        } else {
            debug!(
                batch_size = self.messages.len(),
                timeout_ms = self.current_timeout.as_millis(),
                time_until_flush_ms = self.time_until_flush().as_millis(),
                "⏳ ADAPTIVE BATCH: Not ready to flush yet"
            );
            None
        }
    }

    /// Adjust timeout based on peak detection
    fn adjust_timeout_for_peak(&mut self) {
        if self.peak_detector.is_in_peak() {
            let new_timeout = Duration::from_millis(
                (self.config.base_timeout_ms as f64 * self.config.peak_timeout_multiplier) as u64,
            );

            if new_timeout != self.current_timeout {
                self.current_timeout = new_timeout;
                self.stats.timeout_adjustments += 1;

                info!(
                    old_timeout_ms = Duration::from_millis(self.config.base_timeout_ms).as_millis(),
                    new_timeout_ms = new_timeout.as_millis(),
                    multiplier = self.config.peak_timeout_multiplier,
                    "🔥 ADAPTIVE BATCH: Extended timeout for peak traffic"
                );
            }
        } else {
            // Reset to base timeout when peak ends
            let base_timeout = Duration::from_millis(self.config.base_timeout_ms);
            if self.current_timeout != base_timeout {
                self.current_timeout = base_timeout;
                self.stats.timeout_adjustments += 1;

                info!(
                    new_timeout_ms = base_timeout.as_millis(),
                    "📉 ADAPTIVE BATCH: Reset timeout to base value"
                );
            }
        }
    }

    /// Check if batch should be flushed with adaptive logic
    fn should_flush_adaptive(&self) -> bool {
        let elapsed = self.batch_start.elapsed();

        // Always flush if we reached max batch size
        if self.messages.len() >= self.config.max_batch_size {
            debug!("Batch reached max size, flushing");
            return true;
        }

        // During peak traffic, flush immediately if we have enough messages
        if self.peak_detector.is_in_peak()
            && self.messages.len() >= self.config.peak_immediate_batch_size
        {
            debug!("Peak traffic with sufficient batch size, flushing immediately");
            return true;
        }

        // Flush if timeout exceeded and we have minimum messages
        if elapsed >= self.current_timeout && self.messages.len() >= 1 {
            debug!("Batch timeout reached, flushing");
            return true;
        }

        false
    }

    /// Flush the current batch and return messages for processing
    fn flush_batch(&mut self) -> Option<Vec<(PeerId, Vec<u8>)>> {
        if self.messages.is_empty() {
            return None;
        }

        let batch_size = self.messages.len();
        let elapsed = self.batch_start.elapsed();

        // Update statistics
        self.stats.total_batches += 1;
        self.stats.avg_batch_size =
            (self.stats.avg_batch_size * (self.stats.total_batches - 1) as f64 + batch_size as f64)
                / self.stats.total_batches as f64;

        // Update processing time statistics
        let processing_time_ms = elapsed.as_millis() as f64;
        self.stats.avg_processing_time_ms = (self.stats.avg_processing_time_ms
            * (self.stats.total_batches - 1) as f64
            + processing_time_ms)
            / self.stats.total_batches as f64;

        // Estimate time saved
        if batch_size > 1 {
            self.stats.time_saved_ms += ((batch_size - 1) * 500) as u64; // 500ms saved per additional message
        }

        // Update peak statistics
        if self.peak_detector.is_in_peak() {
            self.stats.peaks_handled += 1;
        }

        info!(
            batch_size = batch_size,
            elapsed_ms = elapsed.as_millis(),
            total_batches = self.stats.total_batches,
            avg_batch_size = format!("{:.2}", self.stats.avg_batch_size),
            is_peak = self.peak_detector.is_in_peak(),
            current_timeout_ms = self.current_timeout.as_millis(),
            "Flushing adaptive batch"
        );

        // Take the messages and reset for next batch
        let batch = std::mem::take(&mut self.messages);
        self.batch_start = Instant::now();

        // Reset timeout to base value for next batch
        self.current_timeout = Duration::from_millis(self.config.base_timeout_ms);

        Some(batch)
    }

    /// Force flush of current batch regardless of conditions
    pub fn force_flush(&mut self) -> Option<Vec<(PeerId, Vec<u8>)>> {
        info!(
            batch_size = self.messages.len(),
            "⏰ ADAPTIVE BATCH: Force flush requested"
        );

        let batch = self.flush_batch();
        if batch.is_some() {
            info!(
                "Force flush completed with {} messages",
                batch.as_ref().unwrap().len()
            );
        }
        batch
    }

    /// Get time until next auto-flush
    pub fn time_until_flush(&self) -> Duration {
        let elapsed = self.batch_start.elapsed();
        if elapsed >= self.current_timeout {
            Duration::ZERO
        } else {
            self.current_timeout - elapsed
        }
    }

    /// Get current batch size
    pub fn current_batch_size(&self) -> usize {
        self.messages.len()
    }

    /// Check if currently in peak traffic
    pub fn is_in_peak(&self) -> bool {
        self.peak_detector.is_in_peak()
    }

    /// Get current peak duration
    pub fn current_peak_duration(&self) -> Duration {
        self.peak_detector.current_peak_duration()
    }

    /// Get retry manager reference
    pub fn get_retry_manager(&mut self) -> &mut LocalRetryManager {
        &mut self.retry_manager
    }

    /// Process failed registrations (call this after batch processing fails)
    pub fn process_batch_failure(&mut self, failed_batch: Vec<(PeerId, Vec<u8>)>, reason: String) {
        for (peer_id, data) in failed_batch {
            // Try to decode as registration
            if let Ok(registration) = serde_json::from_slice::<LightnodeRegistration>(&data) {
                self.retry_manager
                    .register_failure(peer_id, registration, reason.clone());
            } else {
                warn!(
                    peer_id = %peer_id,
                    "🔄 ADAPTIVE BATCH: Failed to decode registration for retry"
                );
            }
        }
    }

    /// Get registrations ready for retry
    pub fn get_ready_retries(&mut self) -> Vec<(PeerId, LightnodeRegistration)> {
        self.retry_manager.get_ready_retries()
    }

    /// Mark retry as successful
    pub fn mark_retry_success(&mut self, peer_id: &PeerId) {
        self.retry_manager.mark_retry_success(peer_id);
    }

    /// Mark retry as failed
    pub fn mark_retry_failed(&mut self, peer_id: &PeerId, reason: String) {
        self.retry_manager.mark_retry_failed(peer_id, reason);
    }

    /// Cleanup old failures and update statistics
    pub fn maintenance(&mut self) {
        self.retry_manager.cleanup_old_failures();

        // Reset peak detector if peak has been inactive for too long
        if self.peak_detector.is_in_peak()
            && self.peak_detector.current_peak_duration() > Duration::from_secs(30)
        {
            warn!(
                duration_ms = self.peak_detector.current_peak_duration().as_millis(),
                "🔥 ADAPTIVE BATCH: Peak duration exceeded 30s, forcing reset"
            );
            // Force reset by creating new peak detector
            self.peak_detector =
                PeakDetector::new(self.config.peak_threshold, self.config.peak_window_seconds);
        }
    }

    /// Get comprehensive statistics
    pub fn get_stats(&self) -> AdaptiveBatchCollectorStats {
        AdaptiveBatchCollectorStats {
            batch_stats: self.stats.clone(),
            peak_stats: self.peak_detector.get_stats(),
            retry_stats: self.retry_manager.get_stats(),
            current_state: AdaptiveBatchState {
                current_batch_size: self.messages.len(),
                current_timeout_ms: self.current_timeout.as_millis() as u64,
                is_in_peak: self.peak_detector.is_in_peak(),
                current_peak_duration_ms: self.peak_detector.current_peak_duration().as_millis()
                    as u64,
                time_until_flush_ms: self.time_until_flush().as_millis() as u64,
            },
        }
    }
}

/// Comprehensive statistics for the adaptive batch collector
#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveBatchCollectorStats {
    pub batch_stats: AdaptiveBatchStats,
    pub peak_stats: PeakStats,
    pub retry_stats: super::retry_manager::RetryStats,
    pub current_state: AdaptiveBatchState,
}

/// Current state of the adaptive batch collector
#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveBatchState {
    pub current_batch_size: usize,
    pub current_timeout_ms: u64,
    pub is_in_peak: bool,
    pub current_peak_duration_ms: u64,
    pub time_until_flush_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[tokio::test]
    async fn test_peak_detection() {
        let mut detector = PeakDetector::new(3, 1); // 3 messages per second threshold

        // Send messages slowly (should not trigger peak)
        for i in 0..2 {
            detector.register_message();
            thread::sleep(Duration::from_millis(600));
        }
        assert!(!detector.is_in_peak());

        // Send messages quickly (should trigger peak)
        for i in 0..3 {
            detector.register_message();
            thread::sleep(Duration::from_millis(100));
        }
        assert!(detector.is_in_peak());
    }

    #[tokio::test]
    async fn test_adaptive_batch_processing() {
        let mut collector = AdaptivePeerInfoBatchCollector::new();
        let peer_id = PeerId::random();

        // Add messages during normal conditions
        for i in 0..2 {
            collector.add_message(peer_id, vec![i as u8]);
        }
        assert_eq!(collector.current_batch_size(), 2);

        // Simulate peak by adding messages quickly
        for i in 0..4 {
            collector.add_message(peer_id, vec![i as u8]);
        }

        // Should flush immediately during peak
        assert!(collector.current_batch_size() < collector.config.max_batch_size);
        assert!(collector.is_in_peak());
    }

    #[tokio::test]
    async fn test_retry_integration() {
        let mut collector = AdaptivePeerInfoBatchCollector::new();
        let peer_id = PeerId::random();

        // Simulate batch failure
        let failed_batch = vec![(peer_id, vec![1, 2, 3])];
        collector.process_batch_failure(failed_batch, "Test failure".to_string());

        // Should have failed registration in retry manager
        assert!(collector
            .get_retry_manager()
            .has_failed_registration(&peer_id));
    }
}
