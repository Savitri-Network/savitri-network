//! Performance Tuning Module
//!
//! This module provides optimized data structures and caching mechanisms

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// LRU Cache for transaction lookups with O(1) access
#[derive(Debug)]
pub struct TransactionLRUCache<V> {
    cache: HashMap<[u8; 32], CacheEntry<V>>,
    max_size: usize,
    access_counter: AtomicU64,
    stats: CacheStats,
}

#[derive(Debug, Clone)]
struct CacheEntry<V> {
    value: V,
    last_access: u64,
    created_at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub insertions: u64,
}

impl<V: Clone> TransactionLRUCache<V> {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            max_size,
            access_counter: AtomicU64::new(0),
            stats: CacheStats::default(),
        }
    }

    /// Get a value from the cache
    pub fn get(&mut self, key: &[u8; 32]) -> Option<&V> {
        if let Some(entry) = self.cache.get_mut(key) {
            entry.last_access = self.access_counter.fetch_add(1, Ordering::Relaxed);
            self.stats.hits += 1;
            Some(&entry.value)
        } else {
            self.stats.misses += 1;
            None
        }
    }

    /// Insert a value into the cache
    pub fn insert(&mut self, key: [u8; 32], value: V) {
        // Evict if at capacity
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&key) {
            self.evict_lru();
        }

        let entry = CacheEntry {
            value,
            last_access: self.access_counter.fetch_add(1, Ordering::Relaxed),
            created_at: Instant::now(),
        };

        self.cache.insert(key, entry);
        self.stats.insertions += 1;
    }

    /// Check if key exists
    pub fn contains(&self, key: &[u8; 32]) -> bool {
        self.cache.contains_key(key)
    }

    /// Remove a key from the cache
    pub fn remove(&mut self, key: &[u8; 32]) -> Option<V> {
        self.cache.remove(key).map(|e| e.value)
    }

    /// Evict least recently used entry
    fn evict_lru(&mut self) {
        if let Some((&lru_key, _)) = self.cache.iter().min_by_key(|(_, entry)| entry.last_access) {
            self.cache.remove(&lru_key);
            self.stats.evictions += 1;
        }
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> &CacheStats {
        &self.stats
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let total = self.stats.hits + self.stats.misses;
        if total == 0 {
            0.0
        } else {
            self.stats.hits as f64 / total as f64
        }
    }
}

/// Bloom filter for fast negative lookups
#[derive(Debug)]
pub struct TransactionBloomFilter {
    bits: Vec<bool>,
    num_hash_functions: usize,
    size: usize,
    count: usize,
}

impl TransactionBloomFilter {
    /// Create a new bloom filter with target false positive rate
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // Calculate optimal size and hash functions
        let size = Self::optimal_size(expected_items, false_positive_rate);
        let num_hash_functions = Self::optimal_hash_count(size, expected_items);

        Self {
            bits: vec![false; size],
            num_hash_functions,
            size,
            count: 0,
        }
    }

    /// Insert a transaction hash
    pub fn insert(&mut self, tx_hash: &[u8; 32]) {
        for i in 0..self.num_hash_functions {
            let idx = self.hash(tx_hash, i);
            self.bits[idx] = true;
        }
        self.count += 1;
    }

    /// Check if transaction might exist (false positives possible)
    pub fn might_contain(&self, tx_hash: &[u8; 32]) -> bool {
        for i in 0..self.num_hash_functions {
            let idx = self.hash(tx_hash, i);
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }

    /// Get approximate count
    pub fn count(&self) -> usize {
        self.count
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.bits.fill(false);
        self.count = 0;
    }

    /// Calculate hash for given index
    fn hash(&self, tx_hash: &[u8; 32], index: usize) -> usize {
        // Use different portions of the hash for different indices
        let mut hash: u64 = 0;
        for (j, &byte) in tx_hash.iter().enumerate() {
            hash = hash.wrapping_add((byte as u64).wrapping_mul((j + index + 1) as u64));
            hash = hash.wrapping_mul(31);
        }
        (hash as usize) % self.size
    }

    fn optimal_size(n: usize, p: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        ((-1.0 * n as f64 * p.ln()) / (ln2 * ln2)).ceil() as usize
    }

    fn optimal_hash_count(m: usize, n: usize) -> usize {
        ((m as f64 / n as f64) * std::f64::consts::LN_2).ceil() as usize
    }
}

#[derive(Debug)]
pub struct BatchProcessor {
    batch_size: usize,
    processing_timeout: Duration,
    stats: BatchStats,
}

#[derive(Debug, Clone, Default)]
pub struct BatchStats {
    pub batches_processed: u64,
    pub items_processed: u64,
    pub avg_batch_time_us: u64,
    pub max_batch_time_us: u64,
}

impl BatchProcessor {
    pub fn new(batch_size: usize, timeout_ms: u64) -> Self {
        Self {
            batch_size,
            processing_timeout: Duration::from_millis(timeout_ms),
            stats: BatchStats::default(),
        }
    }

    /// Process items in batches with timing
    pub fn process_batch<T, F, R>(&mut self, items: Vec<T>, processor: F) -> Vec<R>
    where
        F: Fn(&T) -> R,
    {
        let start = Instant::now();
        let results: Vec<R> = items.iter().map(&processor).collect();
        let elapsed = start.elapsed();

        self.stats.batches_processed += 1;
        self.stats.items_processed += items.len() as u64;

        let batch_time_us = elapsed.as_micros() as u64;
        if batch_time_us > self.stats.max_batch_time_us {
            self.stats.max_batch_time_us = batch_time_us;
        }

        // Update average
        let total_time =
            self.stats.avg_batch_time_us * (self.stats.batches_processed - 1) + batch_time_us;
        self.stats.avg_batch_time_us = total_time / self.stats.batches_processed;

        results
    }

    /// Get batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Get statistics
    pub fn get_stats(&self) -> &BatchStats {
        &self.stats
    }
}

/// Rate limiter for network requests
#[derive(Debug)]
pub struct RateLimiter {
    max_requests_per_second: u32,
    window_size: Duration,
    request_times: std::collections::VecDeque<Instant>,
}

impl RateLimiter {
    pub fn new(max_requests_per_second: u32) -> Self {
        Self {
            max_requests_per_second,
            window_size: Duration::from_secs(1),
            request_times: std::collections::VecDeque::new(),
        }
    }

    /// Check if request is allowed
    pub fn check(&mut self) -> bool {
        let now = Instant::now();

        // Remove old requests outside window
        while let Some(&oldest) = self.request_times.front() {
            if now.duration_since(oldest) > self.window_size {
                self.request_times.pop_front();
            } else {
                break;
            }
        }

        // Check if under limit
        if self.request_times.len() < self.max_requests_per_second as usize {
            self.request_times.push_back(now);
            true
        } else {
            false
        }
    }

    /// Get current request count in window
    pub fn current_count(&self) -> usize {
        self.request_times.len()
    }
}
