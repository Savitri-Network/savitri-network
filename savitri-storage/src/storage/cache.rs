//! Storage Caching Layer with LRU eviction
//!
//! riducendo le letture ripetute dal database e migliorando significativamente
//!
//! # Thread Safety
//!
//! La cache usa `RwLock` per garantire thread-safety:
//! - Compatibile con l'architettura multi-thread di Storage

use crate::storage::contracts::ContractInfo;
use lru::LruCache;
use serde::{Deserialize, Serialize};

/// Minimal Account type for cache layer (mirrors savitri-core Account)
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Account {
    pub balance: u128,
    pub nonce: u64,
}
use std::num::NonZeroUsize;
use std::sync::{RwLock, RwLockReadGuard};

/// Cache configuration
///
/// # Memory Usage Considerations
///
/// - `account_cache_size`: default 10,000 entries ≈ ~800 KB memory
/// - `contract_cache_size`: default 1,000 entries ≈ ~500 KB - 2 MB memory (variabile)
/// - Totale stimato: ~1.3 - 2.8 MB per configurazione default
///
pub struct CacheConfig {
    /// Numero massimo di account in cache (default: 10,000)
    ///
    /// Stima memory: ~80 bytes per entry (key + value + overhead)
    /// Totale: account_cache_size * 80 bytes
    pub account_cache_size: usize,
    /// Numero massimo di contratti in cache (default: 1,000)
    ///
    /// Stima memory: ~500 bytes - 2 KB per entry (variabile, dipende da code size)
    /// Totale: contract_cache_size * (500-2000) bytes
    pub contract_cache_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            account_cache_size: 10_000,
            contract_cache_size: 1_000,
        }
    }
}

/// Storage cache con LRU eviction per account e contratti
///
/// La cache riduce significativamente le letture ripetute dal database,
///
/// # Performance
///
/// - Cache hit rate target: >70% per account frequenti
/// - Contract read latency: -80-90% per contract cached
/// - Memory usage: controllato tramite LRU eviction
///
/// # Memory Usage
///
/// Stima memory usage approssimativa (worst case):
/// - Account cache (10k entries): ~320 KB
///   - Key (32 bytes address) + Account (16 bytes) = 48 bytes per entry
///   - Overhead HashMap/LRU: ~32 bytes per entry
///   - Totale: ~10k * 80 bytes ≈ 800 KB (con overhead strutture dati)
/// - Contract cache (1k entries): ~500 KB - 2 MB (variabile)
///   - Key (32 bytes) + ContractInfo (variabile, tipicamente 500 bytes - 2 KB)
///   - Overhead: ~32 bytes per entry
///   - Totale: ~1k * (32 + 500-2000) bytes ≈ 500 KB - 2 MB
/// - **Totale stimato: ~1.3 - 2.8 MB** (worst case con cache piena)
///
/// La cache è progettata per evitare OOM:
/// - Limiti fissi su numero di entries (non su memory size)
/// - LRU eviction automatica quando cache è piena
/// - Memory usage controllato e prevedibile
///
/// # Eviction Policy
///
/// - Account e contratti letti frequentemente rimangono in cache
/// - Elementi non used are evictati automaticamente
/// - Promozione automatica su accesso tramite `get()` (non `peek()`)
/// - Bilanciamento tra hit rate e memory usage
#[derive(Debug)]
pub struct StorageCache {
    /// LRU cache per account (key: address Vec<u8>, value: Account)
    account_cache: RwLock<LruCache<Vec<u8>, Account>>,
    /// LRU cache per contratti (key: address Vec<u8>, value: ContractInfo)
    contract_cache: RwLock<LruCache<Vec<u8>, ContractInfo>>,
}

impl StorageCache {
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }

    pub fn with_config(config: CacheConfig) -> Self {
        let account_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(config.account_cache_size)
                .unwrap_or(NonZeroUsize::new(10_000).unwrap()),
        ));
        let contract_cache = RwLock::new(LruCache::new(
            NonZeroUsize::new(config.contract_cache_size)
                .unwrap_or(NonZeroUsize::new(1_000).unwrap()),
        ));

        Self {
            account_cache,
            contract_cache,
        }
    }

    /// Ottiene un account dalla cache
    ///
    /// L'elemento viene promosso come "recently used" per ottimizzare l'eviction policy LRU.
    ///
    /// # Performance
    ///
    /// elementi letti frequentemente non vengano evictati prematuramente.
    pub fn get_account(&self, address: &[u8]) -> Option<Account> {
        // Usa write lock per promuovere l'elemento come "recently used"
        let mut cache = self.account_cache.write().ok()?;
        cache.get(address).copied()
    }

    /// Inserisce un account in the cache
    ///
    /// Se la cache è piena, l'entry meno recentemente used viene rimossa (LRU eviction)
    pub fn put_account(&self, address: Vec<u8>, account: Account) {
        if let Ok(mut cache) = self.account_cache.write() {
            cache.put(address, account);
        }
    }

    ///
    /// L'elemento viene promosso come "recently used" per ottimizzare l'eviction policy LRU.
    ///
    /// # Performance
    ///
    /// contratti letti frequentemente non vengano evictati prematuramente.
    pub fn get_contract(&self, address: &[u8]) -> Option<ContractInfo> {
        // Usa write lock per promuovere l'elemento come "recently used"
        let mut cache = self.contract_cache.write().ok()?;
        cache.get(address).cloned()
    }

    ///
    /// Se la cache è piena, l'entry meno recentemente used viene rimossa (LRU eviction)
    pub fn put_contract(&self, address: Vec<u8>, contract: ContractInfo) {
        if let Ok(mut cache) = self.contract_cache.write() {
            cache.put(address, contract);
        }
    }

    pub fn invalidate_account(&self, address: &[u8]) {
        if let Ok(mut cache) = self.account_cache.write() {
            cache.pop(address);
        }
    }

    pub fn invalidate_contract(&self, address: &[u8]) {
        if let Ok(mut cache) = self.contract_cache.write() {
            cache.pop(address);
        }
    }

    /// Pulisce completamente la cache degli account
    pub fn clear_account_cache(&self) {
        if let Ok(mut cache) = self.account_cache.write() {
            cache.clear();
        }
    }

    /// Pulisce completamente la cache of contracts
    pub fn clear_contract_cache(&self) {
        if let Ok(mut cache) = self.contract_cache.write() {
            cache.clear();
        }
    }

    /// Ottiene statistiche on the cache (utile per metriche)
    pub fn stats(&self) -> CacheStats {
        let account_cache = self.account_cache.read().ok();
        let contract_cache = self.contract_cache.read().ok();

        CacheStats {
            account_cache_len: account_cache
                .as_ref()
                .map(|guard: &RwLockReadGuard<LruCache<Vec<u8>, Account>>| guard.len())
                .unwrap_or(0),
            account_cache_cap: account_cache
                .as_ref()
                .map(|guard: &RwLockReadGuard<LruCache<Vec<u8>, Account>>| guard.cap().get())
                .unwrap_or(0),
            contract_cache_len: contract_cache
                .as_ref()
                .map(|guard: &RwLockReadGuard<LruCache<Vec<u8>, ContractInfo>>| guard.len())
                .unwrap_or(0),
            contract_cache_cap: contract_cache
                .as_ref()
                .map(|guard: &RwLockReadGuard<LruCache<Vec<u8>, ContractInfo>>| guard.cap().get())
                .unwrap_or(0),
        }
    }
}

impl Default for StorageCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistiche on the cache (utile per metriche e monitoring)
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub account_cache_len: usize,
    pub account_cache_cap: usize,
    pub contract_cache_len: usize,
    pub contract_cache_cap: usize,
}

impl CacheStats {
    pub fn account_fill_rate(&self) -> f64 {
        if self.account_cache_cap == 0 {
            return 0.0;
        }
        self.account_cache_len as f64 / self.account_cache_cap as f64
    }

    pub fn contract_fill_rate(&self) -> f64 {
        if self.contract_cache_cap == 0 {
            return 0.0;
        }
        self.contract_cache_len as f64 / self.contract_cache_cap as f64
    }

    /// Stima memory usage approssimativa in bytes
    ///
    /// - Account: ~80 bytes per entry (key + value + overhead)
    /// - Contract: ~1 KB per entry (stima conservativa)
    pub fn estimated_memory_usage_bytes(&self) -> usize {
        // Account cache: ~80 bytes per entry
        let account_memory = self.account_cache_len * 80;
        // Contract cache: ~1 KB per entry (stima conservativa)
        let contract_memory = self.contract_cache_len * 1024;
        account_memory + contract_memory
    }
}
