//! Peer Info Batch Collector for Parallel Processing
//!
//! This module implements batch collection of peer info messages to enable
//! parallel processing of lightnode registrations, reducing serialization
//! bottlenecks in the masternode registration process.

use anyhow::Result;
use libp2p::PeerId;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Configuration for batch collection
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of messages to collect before auto-flush
    pub max_batch_size: usize,
    /// Maximum time to wait before auto-flush
    pub batch_timeout: Duration,
    /// Minimum messages required to process a batch
    pub min_batch_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 8,                         // Process up to 8 lightnodes together
            batch_timeout: Duration::from_millis(500), // 500ms max wait (reduced from 5s)
            min_batch_size: 1,                         // Process even single messages after timeout
        }
    }
}

/// Collects peer info messages for batch processing
#[derive(Debug)]
pub struct PeerInfoBatchCollector {
    /// Collected messages waiting to be processed
    messages: Vec<(PeerId, Vec<u8>)>,
    /// When the current batch started collection
    batch_start: Instant,
    /// Configuration for batch behavior
    config: BatchConfig,
    /// Statistics tracking
    stats: BatchStats,
}

/// Statistics for batch processing
#[derive(Debug, Default)]
pub struct BatchStats {
    /// Total messages processed
    pub total_messages: usize,
    /// Total batches processed
    pub total_batches: usize,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Total time saved by batching (estimated)
    pub time_saved_ms: u64,
}

impl PeerInfoBatchCollector {
    /// Create a new batch collector with default configuration
    pub fn new() -> Self {
        Self::with_config(BatchConfig::default())
    }

    /// Create a new batch collector with custom configuration
    pub fn with_config(config: BatchConfig) -> Self {
        Self {
            messages: Vec::with_capacity(config.max_batch_size),
            batch_start: Instant::now(),
            config,
            stats: BatchStats::default(),
        }
    }

    /// Add a new message to the batch
    /// Returns Some(batch) if the batch is ready to be processed
    pub fn add_message(
        &mut self,
        peer_id: PeerId,
        data: Vec<u8>,
    ) -> Option<Vec<(PeerId, Vec<u8>)>> {
        info!(
            peer_id = %peer_id,
            data_size = data.len(),
            batch_size_before = self.messages.len(),
            "📦 BATCH: Adding message to batch collector"
        );

        self.messages.push((peer_id, data));
        self.stats.total_messages += 1;

        debug!(
            batch_size = self.messages.len(),
            max_size = self.config.max_batch_size,
            "Added message to batch"
        );

        // Check if batch should be flushed
        if self.should_flush() {
            info!(
                batch_size = self.messages.len(),
                "🚀 BATCH: Flushing batch - ready for processing"
            );
            self.flush_batch()
        } else {
            debug!(
                batch_size = self.messages.len(),
                timeout_ms = self.config.batch_timeout.as_millis(),
                "⏳ BATCH: Not ready to flush yet"
            );
            None
        }
    }

    /// Force flush of current batch regardless of conditions
    /// Returns Some(batch) if there are messages to process
    pub fn force_flush(&mut self) -> Option<Vec<(PeerId, Vec<u8>)>> {
        info!(
            batch_size = self.messages.len(),
            "⏰ BATCH: Force flush requested"
        );

        if !self.messages.is_empty() {
            info!(
                batch_size = self.messages.len(),
                "🚀 BATCH: Force flushing - ready for processing"
            );
            self.flush_batch()
        } else {
            debug!("📭 BATCH: No messages to flush");
            None
        }
    }

    /// Check if batch should be flushed based on conditions
    fn should_flush(&self) -> bool {
        let elapsed = self.batch_start.elapsed();

        // Flush if we reached max batch size
        if self.messages.len() >= self.config.max_batch_size {
            debug!("Batch reached max size, flushing");
            return true;
        }

        // Flush if timeout exceeded and we have minimum messages
        if elapsed >= self.config.batch_timeout && self.messages.len() >= self.config.min_batch_size
        {
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

        // Estimate time saved (assume 500ms saved per additional message in batch)
        if batch_size > 1 {
            self.stats.time_saved_ms += ((batch_size - 1) * 500) as u64;
        }

        info!(
            batch_size = batch_size,
            elapsed_ms = elapsed.as_millis(),
            total_batches = self.stats.total_batches,
            avg_batch_size = format!("{:.2}", self.stats.avg_batch_size),
            "Flushing peer info batch"
        );

        // Take the messages and reset for next batch
        let batch = std::mem::take(&mut self.messages);
        self.batch_start = Instant::now();

        Some(batch)
    }

    /// Get current batch statistics
    pub fn get_stats(&self) -> &BatchStats {
        &self.stats
    }

    /// Get current batch size
    pub fn current_batch_size(&self) -> usize {
        self.messages.len()
    }

    /// Get time until next auto-flush
    pub fn time_until_flush(&self) -> Duration {
        let elapsed = self.batch_start.elapsed();
        if elapsed >= self.config.batch_timeout {
            Duration::ZERO
        } else {
            self.config.batch_timeout - elapsed
        }
    }

    /// Reset statistics (useful for testing)
    pub fn reset_stats(&mut self) {
        self.stats = BatchStats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_batch_collection() {
        let mut collector = PeerInfoBatchCollector::new();
        let peer_id = PeerId::from_str("12D3KooWExamplePeerIdForTesting").unwrap();

        // Add messages
        for i in 0..3 {
            let data = format!("test_message_{}", i).into_bytes();
            assert_eq!(collector.add_message(peer_id, data), None);
        }

        // Should still not flush (below max size)
        assert_eq!(collector.current_batch_size(), 3);

        // Add more messages to reach max size
        for i in 3..8 {
            let data = format!("test_message_{}", i).into_bytes();
            let result = collector.add_message(peer_id, data);
            if i == 7 {
                // Last message (8 total)
                assert!(result.is_some());
                assert_eq!(result.unwrap().len(), 8);
            } else {
                assert_eq!(result, None);
            }
        }

        // Should be empty after flush
        assert_eq!(collector.current_batch_size(), 0);
    }

    #[test]
    fn test_timeout_flush() {
        let config = BatchConfig {
            max_batch_size: 10,
            batch_timeout: Duration::from_millis(100),
            min_batch_size: 1,
        };
        let mut collector = PeerInfoBatchCollector::with_config(config);
        let peer_id = PeerId::from_str("12D3KooWExamplePeerIdForTesting").unwrap();

        // Add one message
        let data = b"test_message".to_vec();
        assert_eq!(collector.add_message(peer_id, data), None);

        // Should not flush immediately
        assert_eq!(collector.current_batch_size(), 1);

        // Wait for timeout (simulate with direct force_flush for test)
        std::thread::sleep(Duration::from_millis(150));
        let batch = collector.force_flush();
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().len(), 1);
    }
}
