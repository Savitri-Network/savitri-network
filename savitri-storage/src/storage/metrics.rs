//! Storage Metrics for Prometheus monitoring
//!
//! This module provides Prometheus metrics for storage operations including
//! read/write latency, cache hit rate, compaction duration, disk usage, and key count.

use metrics::{counter, gauge, histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// Atomic counters for storage statistics
static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static KEY_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record read latency in milliseconds
pub fn record_read_latency_ms(latency_ms: u64) {
    histogram!("storage_read_latency_ms", latency_ms as f64);
}

/// Record write latency in milliseconds
pub fn record_write_latency_ms(latency_ms: u64) {
    histogram!("storage_write_latency_ms", latency_ms as f64);
}

/// Record cache hit
pub fn record_cache_hit() {
    let _hits = CACHE_HITS.fetch_add(1, Ordering::Relaxed) + 1;
    counter!("storage_cache_hits_total", 1);
    update_cache_hit_rate();
}

/// Record cache miss
pub fn record_cache_miss() {
    let _misses = CACHE_MISSES.fetch_add(1, Ordering::Relaxed) + 1;
    counter!("storage_cache_misses_total", 1);
    update_cache_hit_rate();
}

/// Update cache hit rate gauge
fn update_cache_hit_rate() {
    let hits = CACHE_HITS.load(Ordering::Relaxed);
    let misses = CACHE_MISSES.load(Ordering::Relaxed);
    let total = hits + misses;
    
    if total > 0 {
        let hit_rate = (hits as f64 / total as f64) * 100.0;
        gauge!("storage_cache_hit_rate", hit_rate);
    }
}

/// Get current cache hit rate (0.0-100.0)
pub fn get_cache_hit_rate() -> f64 {
    let hits = CACHE_HITS.load(Ordering::Relaxed);
    let misses = CACHE_MISSES.load(Ordering::Relaxed);
    let total = hits + misses;
    
    if total > 0 {
        (hits as f64 / total as f64) * 100.0
    } else {
        0.0
    }
}

/// Record compaction duration in seconds
pub fn record_compaction_duration_sec(duration_sec: u64) {
    histogram!("storage_compaction_duration_sec", duration_sec as f64);
}

/// Update disk usage in bytes
pub fn set_disk_usage_bytes(usage_bytes: u64) {
    gauge!("storage_disk_usage_bytes", usage_bytes as f64);
}

/// Update key count
pub fn set_key_count(count: u64) {
    KEY_COUNT.store(count, Ordering::Relaxed);
    gauge!("storage_key_count", count as f64);
}

/// Get current key count
pub fn get_key_count() -> u64 {
    KEY_COUNT.load(Ordering::Relaxed)
}

/// Increment key count
pub fn increment_key_count() {
    let count = KEY_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    set_key_count(count);
}

/// Decrement key count
pub fn decrement_key_count() {
    let count = KEY_COUNT.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
    set_key_count(count);
}

/// Helper to measure read operation with metrics
pub fn measure_read<F, T>(f: F) -> Result<T, anyhow::Error>
where
    F: FnOnce() -> Result<T, anyhow::Error>,
{
    let start = Instant::now();
    let result = f();
    let elapsed_ms = start.elapsed().as_millis() as u64;
    record_read_latency_ms(elapsed_ms);
    result
}

/// Helper to measure write operation with metrics
pub fn measure_write<F, T>(f: F) -> Result<T, anyhow::Error>
where
    F: FnOnce() -> Result<T, anyhow::Error>,
{
    let start = Instant::now();
    let result = f();
    let elapsed_ms = start.elapsed().as_millis() as u64;
    record_write_latency_ms(elapsed_ms);
    result
}

/// Helper to measure compaction with metrics
pub fn measure_compaction<F, T>(f: F) -> Result<T, anyhow::Error>
where
    F: FnOnce() -> Result<T, anyhow::Error>,
{
    let start = Instant::now();
    let result = f();
    let elapsed_sec = start.elapsed().as_secs();
    record_compaction_duration_sec(elapsed_sec);
    result
}

