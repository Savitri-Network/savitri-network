//! Memory monitoring utilities for smart contracts
//!
//! This module provides memory monitoring and tracking capabilities
//! for the Savitri smart contract platform to ensure efficient
//! memory usage and prevent memory leaks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Memory monitor for tracking contract memory usage
#[derive(Debug, Clone)]
pub struct MemoryMonitor {
    /// Total memory allocated for contracts
    total_allocated: Arc<AtomicU64>,
    /// Peak memory usage
    peak_usage: Arc<AtomicU64>,
    /// Current memory usage
    current_usage: Arc<AtomicU64>,
    /// Last update timestamp
    last_update: Arc<std::sync::Mutex<Instant>>,
}

impl MemoryMonitor {
    /// Create a new memory monitor
    pub fn new() -> Self {
        Self {
            total_allocated: Arc::new(AtomicU64::new(0)),
            peak_usage: Arc::new(AtomicU64::new(0)),
            current_usage: Arc::new(AtomicU64::new(0)),
            last_update: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Record memory allocation
    pub fn record_allocation(&self, size: u64) {
        let current = self.current_usage.fetch_add(size, Ordering::Relaxed) + size;
        self.total_allocated.fetch_add(size, Ordering::Relaxed);

        // Update peak usage if necessary
        let mut peak = self.peak_usage.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_usage.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }

        // Update last update time
        if let Ok(mut last_update) = self.last_update.lock() {
            *last_update = Instant::now();
        }
    }

    /// Record memory deallocation
    pub fn record_deallocation(&self, size: u64) {
        self.current_usage.fetch_sub(size, Ordering::Relaxed);

        // Update last update time
        if let Ok(mut last_update) = self.last_update.lock() {
            *last_update = Instant::now();
        }
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> u64 {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Get peak memory usage
    pub fn peak_usage(&self) -> u64 {
        self.peak_usage.load(Ordering::Relaxed)
    }

    /// Get total allocated memory
    pub fn total_allocated(&self) -> u64 {
        self.total_allocated.load(Ordering::Relaxed)
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.current_usage.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.total_allocated.store(0, Ordering::Relaxed);

        if let Ok(mut last_update) = self.last_update.lock() {
            *last_update = Instant::now();
        }
    }

    /// Check if memory usage is within limits
    pub fn is_within_limit(&self, limit: u64) -> bool {
        self.current_usage() <= limit
    }

    /// Check overlay size against memory limits
    ///
    /// # Arguments
    /// * `overlay_size` - Size of the overlay in bytes
    ///
    /// # Returns
    /// Ok(()) if within limits, Err with description if exceeded
    pub fn check_overlay_size(&self, overlay_size: usize) -> Result<(), String> {
        // Allow overlay up to 10x the current memory usage as a reasonable limit
        let limit = self.current_usage() * 10;
        if overlay_size as u64 > limit {
            Err(format!(
                "Overlay size {} exceeds limit {}",
                overlay_size, limit
            ))
        } else {
            Ok(())
        }
    }

    /// Get memory usage statistics
    pub fn stats(&self) -> MemoryStats {
        MemoryStats {
            current_usage: self.current_usage(),
            peak_usage: self.peak_usage(),
            total_allocated: self.total_allocated(),
        }
    }

    /// Check batch size against memory limits
    ///
    /// # Arguments
    /// * `num_txs` - Number of transactions in the batch
    ///
    /// # Returns
    /// Ok(()) if within limits, Err with description if exceeded
    pub fn check_batch_size(&self, num_txs: usize) -> Result<(), String> {
        // Allow reasonable batch sizes (e.g., up to 1000 transactions)
        let max_batch_size = 1000;
        if num_txs > max_batch_size {
            Err(format!(
                "Batch size {} exceeds maximum {}",
                num_txs, max_batch_size
            ))
        } else {
            Ok(())
        }
    }

    /// Check transaction memory usage
    ///
    /// # Arguments
    /// * `memory_usage` - Memory usage in bytes
    ///
    /// # Returns
    /// Ok(()) if within limits, Err with description if exceeded
    pub fn check_tx_memory(&self, memory_usage: u64) -> Result<(), String> {
        // Allow reasonable transaction memory (e.g., up to 1MB)
        let max_tx_memory = 1024 * 1024; // 1MB
        if memory_usage > max_tx_memory {
            Err(format!(
                "Transaction memory {} exceeds maximum {}",
                memory_usage, max_tx_memory
            ))
        } else {
            Ok(())
        }
    }

    /// Check call frames depth
    ///
    /// # Arguments
    /// * `call_depth` - Current call stack depth
    ///
    /// # Returns
    /// Ok(()) if within limits, Err with description if exceeded
    pub fn check_call_frames(&self, call_depth: usize) -> Result<(), String> {
        // Allow reasonable call depth (e.g., up to 1024 frames)
        let max_call_depth = 1024;
        if call_depth > max_call_depth {
            Err(format!(
                "Call depth {} exceeds maximum {}",
                call_depth, max_call_depth
            ))
        } else {
            Ok(())
        }
    }
}

impl Default for MemoryMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory usage statistics
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryStats {
    /// Current memory usage in bytes
    pub current_usage: u64,
    /// Peak memory usage in bytes
    pub peak_usage: u64,
    /// Total allocated memory in bytes
    pub total_allocated: u64,
}

impl MemoryStats {
    /// Create new memory stats
    pub fn new(current: u64, peak: u64, total: u64) -> Self {
        Self {
            current_usage: current,
            peak_usage: peak,
            total_allocated: total,
        }
    }

    /// Get memory efficiency ratio (current / peak)
    pub fn efficiency_ratio(&self) -> f64 {
        if self.peak_usage == 0 {
            0.0
        } else {
            self.current_usage as f64 / self.peak_usage as f64
        }
    }

    /// Get memory utilization percentage
    pub fn utilization_percent(&self, limit: u64) -> f64 {
        if limit == 0 {
            0.0
        } else {
            (self.current_usage as f64 / limit as f64) * 100.0
        }
    }
}

/// Memory usage threshold configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryThreshold {
    /// Warning threshold in bytes
    pub warning_threshold: u64,
    /// Critical threshold in bytes
    pub critical_threshold: u64,
    /// Maximum allowed memory in bytes
    pub max_memory: u64,
}

impl MemoryThreshold {
    /// Create new memory threshold
    pub fn new(warning: u64, critical: u64, max: u64) -> Self {
        Self {
            warning_threshold: warning,
            critical_threshold: critical,
            max_memory: max,
        }
    }

    /// Check memory status against thresholds
    pub fn check_status(&self, current_usage: u64) -> MemoryStatus {
        if current_usage >= self.max_memory {
            MemoryStatus::Exceeded
        } else if current_usage >= self.critical_threshold {
            MemoryStatus::Critical
        } else if current_usage >= self.warning_threshold {
            MemoryStatus::Warning
        } else {
            MemoryStatus::Normal
        }
    }
}

impl Default for MemoryThreshold {
    fn default() -> Self {
        Self::new(
            100 * 1024 * 1024, // 100MB warning
            200 * 1024 * 1024, // 200MB critical
            500 * 1024 * 1024, // 500MB max
        )
    }
}

/// Memory usage status
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MemoryStatus {
    /// Memory usage is normal
    Normal,
    /// Memory usage is approaching warning threshold
    Warning,
    /// Memory usage is approaching critical threshold
    Critical,
    /// Memory usage has exceeded maximum limit
    Exceeded,
}

/// Global memory monitor instance
static GLOBAL_MEMORY_MONITOR: std::sync::LazyLock<MemoryMonitor> =
    std::sync::LazyLock::new(MemoryMonitor::new);

/// Get the global memory monitor instance
pub fn global_memory_monitor() -> &'static MemoryMonitor {
    &GLOBAL_MEMORY_MONITOR
}

/// Helper function to format bytes in human readable format
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.2} {}", size, UNITS[unit_index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_monitor() {
        let monitor = MemoryMonitor::new();

        // Test allocation
        monitor.record_allocation(1024);
        assert_eq!(monitor.current_usage(), 1024);
        assert_eq!(monitor.peak_usage(), 1024);
        assert_eq!(monitor.total_allocated(), 1024);

        // Test more allocation
        monitor.record_allocation(2048);
        assert_eq!(monitor.current_usage(), 3072);
        assert_eq!(monitor.peak_usage(), 3072);
        assert_eq!(monitor.total_allocated(), 3072);

        // Test deallocation
        monitor.record_deallocation(1024);
        assert_eq!(monitor.current_usage(), 2048);
        assert_eq!(monitor.peak_usage(), 3072); // Peak should remain
        assert_eq!(monitor.total_allocated(), 3072); // Total should remain
    }

    #[test]
    fn test_memory_threshold() {
        let threshold = MemoryThreshold::new(1000, 2000, 3000);

        assert_eq!(threshold.check_status(500), MemoryStatus::Normal);
        assert_eq!(threshold.check_status(1500), MemoryStatus::Warning);
        assert_eq!(threshold.check_status(2500), MemoryStatus::Critical);
        assert_eq!(threshold.check_status(3500), MemoryStatus::Exceeded);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
    }

    #[test]
    fn test_memory_stats() {
        let stats = MemoryStats::new(1000, 2000, 3000);

        assert_eq!(stats.efficiency_ratio(), 0.5);
        assert_eq!(stats.utilization_percent(4000), 25.0);
    }
}
