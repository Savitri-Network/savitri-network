//! Score Cache System for ExecutionDispatcher
//!
//! per evitare ricalcoli ridondanti e migliorare le performance.

use crate::mempool::types::TxClass;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Cache entry con score e timestamp
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Score calcolato
    score: f64,
    /// Timestamp di inserimento
    timestamp: Instant,
}

impl CacheEntry {
    fn new(score: f64) -> Self {
        Self {
            score,
            timestamp: Instant::now(),
        }
    }

    /// Usa >= per i test veloci e maggiore precisione
    fn is_expired(&self, ttl: Duration) -> bool {
        self.timestamp.elapsed() >= ttl
    }
}

/// Score Cache System per ExecutionDispatcher
#[derive(Debug)]
pub struct ScoreCache {
    /// Cache interna: (fee, class) -> (score, timestamp)
    cache: HashMap<(u64, TxClass), CacheEntry>,
    max_size: usize,
    /// TTL per le entries
    ttl: Duration,
    /// Contatore cache hits
    hits: AtomicU64,
    /// Contatore cache misses
    misses: AtomicU64,
}

impl ScoreCache {
    pub fn new() -> Self {
        Self::with_config(10_000, Duration::from_secs(1600))
    }

    pub fn with_config(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size),
            max_size,
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn get_cached_score(&self, fee: u64, class: TxClass) -> Option<f64> {
        let key = (fee, class);

        if let Some(entry) = self.cache.get(&key) {
            // CORREZIONE: Usa un controllo inclusivo per il TTL
            if entry.timestamp.elapsed() < self.ttl {
                self.hits.fetch_add(1, Ordering::SeqCst);
                metrics::counter!("dispatcher_score_cache_hits_total").increment(1);
                self.update_hit_rate_gauge();
                return Some(entry.score);
            }
            // Se arriviamo qui, l'entry è tecnicamente scaduta ma non la rimuoviamo
            // per mantenere il metodo &self
        }

        self.misses.fetch_add(1, Ordering::SeqCst);
        metrics::counter!("dispatcher_score_cache_misses_total").increment(1);
        self.update_hit_rate_gauge();
        None
    }

    pub fn cache_score(&mut self, fee: u64, class: TxClass, score: f64) {
        let key = (fee, class);

        if self.cache.len() >= self.max_size {
            self.evict_oldest();
        }

        self.cache.insert(key, CacheEntry::new(score));

        // CORREZIONE GAUGE: Rimossa virgola tra nome e valore
        metrics::gauge!("dispatcher_score_cache_size").set(self.cache.len() as f64);
    }

    pub fn cleanup_expired(&mut self) -> usize {
        let initial_size = self.cache.len();
        self.cache.retain(|_, entry| !entry.is_expired(self.ttl));
        let removed_count = initial_size - self.cache.len();

        // CORREZIONE GAUGE: Rimossa virgola
        metrics::gauge!("dispatcher_score_cache_size").set(self.cache.len() as f64);

        removed_count
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        // Reset totale con barriera SeqCst (Sequential Consistency)
        self.hits.store(0, Ordering::SeqCst);
        self.misses.store(0, Ordering::SeqCst);

        metrics::gauge!("dispatcher_score_cache_size").set(0.0);
        metrics::gauge!("dispatcher_score_cache_hit_rate").set(0.0);
    }

    fn evict_oldest(&mut self) {
        if self.cache.is_empty() {
            return;
        }

        let mut oldest_key = None;
        let mut oldest_time = Instant::now();

        for (key, entry) in &self.cache {
            if entry.timestamp < oldest_time {
                oldest_time = entry.timestamp;
                oldest_key = Some(key.clone());
            }
        }

        if let Some(key) = oldest_key {
            self.cache.remove(&key);
        }

        if self.cache.len() >= self.max_size {
            let remove_count = self.max_size / 10;
            let mut entries_to_remove: Vec<_> = self
                .cache
                .iter()
                .map(|(key, entry)| (key.clone(), entry.timestamp))
                .collect();

            entries_to_remove.sort_by_key(|(_, timestamp)| *timestamp);

            for (key, _) in entries_to_remove.into_iter().take(remove_count) {
                self.cache.remove(&key);
            }
        }

        // Sync cache size gauge after eviction
        metrics::gauge!("dispatcher_score_cache_size").set(self.cache.len() as f64);
    }

    pub fn get_stats(&self) -> (u64, u64, f64, usize) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        (hits, misses, hit_rate, self.cache.len())
    }

    pub fn size(&self) -> usize {
        self.cache.len()
    }
    pub fn max_size(&self) -> usize {
        self.max_size
    }
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn reset_stats(&mut self) {
        self.hits.store(0, Ordering::SeqCst);
        self.misses.store(0, Ordering::SeqCst);
        // CORREZIONE GAUGE: Rimossa virgola
        metrics::gauge!("dispatcher_score_cache_hit_rate").set(0.0);
    }

    fn update_hit_rate_gauge(&self) {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        // CORREZIONE GAUGE: Rimossa virgola
        metrics::gauge!("dispatcher_score_cache_hit_rate").set(hit_rate * 100.0);
    }
}

impl Default for ScoreCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ScoreCache {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            max_size: self.max_size,
            ttl: self.ttl,
            hits: AtomicU64::new(self.hits.load(Ordering::Relaxed)),
            misses: AtomicU64::new(self.misses.load(Ordering::Relaxed)),
        }
    }
}
// Seguono i test...

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::types::TxClass;

    #[test]
    fn test_score_cache_basic_operations() {
        let mut cache = ScoreCache::new();

        // Test cache miss
        assert_eq!(cache.get_cached_score(1000, TxClass::Financial), None);

        // Test cache score
        cache.cache_score(1000, TxClass::Financial, 0.5);

        // Test cache hit
        assert_eq!(cache.get_cached_score(1000, TxClass::Financial), Some(0.5));

        // Test stats
        let (hits, misses, hit_rate, size) = cache.get_stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
        assert_eq!(hit_rate, 0.5);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_score_cache_ttl_expiration() {
        let mut cache = ScoreCache::with_config(100, Duration::from_millis(10));

        // Cache un score
        cache.cache_score(1000, TxClass::Financial, 0.5);

        // Check che sia presente
        assert_eq!(cache.get_cached_score(1000, TxClass::Financial), Some(0.5));

        // Attendi scadenza TTL
        std::thread::sleep(Duration::from_millis(20));

        // Check che sia expired
        assert_eq!(cache.get_cached_score(1000, TxClass::Financial), None);
    }

    #[test]
    fn test_score_cache_lru_eviction() {
        let mut cache = ScoreCache::with_config(2, Duration::from_secs(100));

        // Riempi la cache fino al limit
        cache.cache_score(1000, TxClass::Financial, 0.5);
        cache.cache_score(2000, TxClass::IoTData, 0.7);

        assert_eq!(cache.size(), 2);

        cache.cache_score(3000, TxClass::System, 0.3);

        assert_eq!(cache.size(), 2);

        assert_eq!(cache.get_cached_score(1000, TxClass::Financial), None);

        assert_eq!(cache.get_cached_score(2000, TxClass::IoTData), Some(0.7));
        assert_eq!(cache.get_cached_score(3000, TxClass::System), Some(0.3));
    }

    #[test]
    fn test_score_cache_cleanup_expired() {
        let mut cache = ScoreCache::with_config(100, Duration::from_millis(10));

        // Cache alcune entries
        cache.cache_score(1000, TxClass::Financial, 0.5);
        cache.cache_score(2000, TxClass::IoTData, 0.7);
        cache.cache_score(3000, TxClass::System, 0.3);

        assert_eq!(cache.size(), 3);

        // Attendi scadenza TTL
        std::thread::sleep(Duration::from_millis(20));

        // Cleanup expired entries
        let removed = cache.cleanup_expired();
        assert_eq!(removed, 3);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_score_cache_stats() {
        let mut cache = ScoreCache::new();

        // Test initial stats
        let (hits, misses, hit_rate, size) = cache.get_stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 0);
        assert_eq!(hit_rate, 0.0);
        assert_eq!(size, 0);

        // Cache operations
        cache.get_cached_score(1000, TxClass::Financial); // miss (cache vuota)
        cache.cache_score(1000, TxClass::Financial, 0.5);
        cache.get_cached_score(1000, TxClass::Financial); // hit
        cache.get_cached_score(2000, TxClass::Financial); // miss

        // Verify stats
        let (hits, misses, hit_rate, size) = cache.get_stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 2);
        assert_eq!(hit_rate, 1.0 / 3.0);
        assert_eq!(size, 1);
    }

    #[test]
    fn test_score_cache_clear() {
        let mut cache = ScoreCache::new();

        // Add some entries
        cache.cache_score(1000, TxClass::Financial, 0.5);
        cache.cache_score(2000, TxClass::IoTData, 0.7);

        // Generate some hits/misses
        cache.get_cached_score(1000, TxClass::Financial);
        cache.get_cached_score(3000, TxClass::Financial);

        assert!(!cache.is_empty());

        // Clear cache
        cache.clear();

        assert!(cache.is_empty());

        // Stats should be reset
        let (hits, misses, hit_rate, size) = cache.get_stats();
        assert_eq!(hits, 0);
        assert_eq!(misses, 0);
        assert_eq!(hit_rate, 0.0);
        assert_eq!(size, 0);
    }
}
