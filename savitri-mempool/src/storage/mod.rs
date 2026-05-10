//! Storage module for mempool persistence with lock-free optimizations

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use dashmap::DashMap; // Lock-free hash map for better concurrency

/// Lock-free storage wrapper for high-performance account access
#[derive(Debug)]
pub struct LockFreeStorage {
    /// Lock-free account cache
    account_cache: DashMap<Vec<u8>, crate::core::Account>,
    /// Backend storage
    backend: Arc<dyn StorageTraitInterface>,
    /// Cache statistics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    /// Cache TTL
    cache_ttl: Duration,
}

impl LockFreeStorage {
    pub fn new(backend: Arc<dyn StorageTraitInterface>) -> Self {
        Self {
            account_cache: DashMap::new(),
            backend,
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_ttl: Duration::from_secs(300), // 5 minutes cache TTL
        }
    }
    
    /// Get account with lock-free caching
    pub fn get_account_cached(&self, address: &[u8]) -> Result<Option<crate::core::Account>, StorageError> {
        let address_vec = address.to_vec();
        
        // Try cache first (lock-free)
        if let Some(account) = self.account_cache.get(&address_vec) {
            // Check if cache entry is still valid
            if Instant::now().duration_since(account.last_updated) < self.cache_ttl {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(account.clone()));
            } else {
                // Remove expired entry
                self.account_cache.remove(&address_vec);
            }
        }
        
        // Cache miss - fetch from backend
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        let account = self.backend.get_account(address)?;
        
        // Cache the result
        if let Some(ref acc) = account {
            let mut cached_account = acc.clone();
            cached_account.last_updated = Instant::now();
            self.account_cache.insert(address_vec, cached_account);
        }
        
        Ok(account)
    }
    
    /// Batch get accounts for better performance
    pub fn get_accounts_batch_cached(&self, addresses: &[Vec<u8>]) -> Result<Vec<Option<crate::core::Account>>, StorageError> {
        let mut results = Vec::with_capacity(addresses.len());
        
        for address in addresses {
            results.push(self.get_account_cached(address)?);
        }
        
        Ok(results)
    }
    
    /// Update account in cache and backend
    pub fn put_account_cached(&self, account: &crate::core::Account) -> Result<(), StorageError> {
        let mut cached_account = account.clone();
        cached_account.last_updated = Instant::now();
        
        // Update cache
        self.account_cache.insert(account.address.clone(), cached_account);
        
        // Update backend
        self.backend.put_account(account)
    }
    
    /// Get cache statistics
    pub fn get_cache_stats(&self) -> (u64, u64, f64) {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
        (hits, misses, hit_rate)
    }
    
    /// Clear cache
    pub fn clear_cache(&self) {
        self.account_cache.clear();
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
    }
}

// Extend Account to include cache timestamp
impl crate::core::Account {
    pub fn with_cache_timestamp(mut self) -> Self {
        self.last_updated = Instant::now();
        self
    }
}

pub trait StorageInterface: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;
    fn batch_write(&self, entries: Vec<(&[u8], &[u8])>) -> Result<(), StorageError>;
    
    // Additional methods needed by the codebase
    fn get_account(&self, address: &[u8]) -> Result<Option<crate::core::Account>, StorageError> {
        match self.get(address) {
            Ok(Some(bytes)) => {
                match crate::core::Account::from_bytes(&bytes) {
                    Some(account) => Ok(Some(account)),
                    None => Err(StorageError::SerializationError("Invalid account data".to_string())),
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    fn put_account(&self, account: &crate::core::Account) -> Result<(), StorageError> {
        self.put(&account.address, &account.to_bytes())
    }
    
    fn get_accounts_batch(&self, addresses: &[Vec<u8>]) -> Result<Vec<Option<crate::core::Account>>, StorageError> {
        let mut results = Vec::new();
        for address in addresses {
            results.push(self.get_account(address)?);
        }
        Ok(results)
    }
    
    fn get_account_bytes(&self, address: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.get(address)
    }
    
    fn create_snapshot(&self) -> Result<u64, StorageError> {
        // Simple snapshot implementation - returns a timestamp
        use std::time::{SystemTime, UNIX_EPOCH};
        Ok(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
    }
    
    fn get_test_token_total_burned(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_total_burned")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
    
    fn set_test_token_total_burned(&self, amount: u128) -> Result<(), StorageError> {
        self.put(b"test_token_total_burned", &amount.to_le_bytes())
    }
    
    fn get_test_token_total_supply(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_total_supply")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
    
    fn get_test_token_circulating_supply(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_circulating_supply")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
}

// Type alias for compatibility
pub type Storage = dyn StorageTraitInterface;

pub trait StorageTraitInterface: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError>;
    fn delete(&self, key: &[u8]) -> Result<(), StorageError>;
    fn batch_write(&self, entries: Vec<(&[u8], &[u8])>) -> Result<(), StorageError>;
    
    // Additional methods needed by the codebase
    fn get_account(&self, address: &[u8]) -> Result<Option<crate::core::Account>, StorageError> {
        match self.get(address) {
            Ok(Some(bytes)) => {
                match crate::core::Account::from_bytes(&bytes) {
                    Some(account) => Ok(Some(account)),
                    None => Err(StorageError::SerializationError("Invalid account data".to_string())),
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
    
    fn put_account(&self, account: &crate::core::Account) -> Result<(), StorageError> {
        self.put(&account.address, &account.to_bytes())
    }
    
    fn get_accounts_batch(&self, addresses: &[Vec<u8>]) -> Result<Vec<Option<crate::core::Account>>, StorageError> {
        let mut results = Vec::new();
        for address in addresses {
            results.push(self.get_account(address)?);
        }
        Ok(results)
    }
    
    fn get_account_bytes(&self, address: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.get(address)
    }
    
    fn create_snapshot(&self) -> Result<u64, StorageError> {
        // Simple snapshot implementation - returns a timestamp
        use std::time::{SystemTime, UNIX_EPOCH};
        Ok(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs())
    }
    
    fn get_test_token_total_burned(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_total_burned")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
    
    fn set_test_token_total_burned(&self, amount: u128) -> Result<(), StorageError> {
        self.put(b"test_token_total_burned", &amount.to_le_bytes())
    }
    
    fn get_test_token_total_supply(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_total_supply")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
    
    fn get_test_token_circulating_supply(&self) -> Result<u128, StorageError> {
        self.get(b"test_token_circulating_supply")?
            .and_then(|bytes| {
                if bytes.len() == 16 {
                    Some(u128::from_le_bytes(bytes.try_into().unwrap()))
                } else {
                    None
                }
            })
            .ok_or(StorageError::NotFound)
    }
}

#[derive(Debug, Clone)]
pub enum StorageError {
    NotFound,
    SerializationError(String),
    IoError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "Storage entry not found"),
            StorageError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            StorageError::IoError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

impl std::error::Error for StorageError {}

#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl StorageInterface for MemoryStorage {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
        Ok(())
    }

    fn batch_write(&self, entries: Vec<(&[u8], &[u8])>) -> Result<(), StorageError> {
        let mut data = self.data.write().unwrap();
        for (key, value) in entries {
            data.insert(key.to_vec(), value.to_vec());
        }
        Ok(())
    }
}

impl StorageTraitInterface for MemoryStorage {
    // Delegate to StorageInterface implementation
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        StorageInterface::get(self, key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        StorageInterface::put(self, key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), StorageError> {
        StorageInterface::delete(self, key)
    }

    fn batch_write(&self, entries: Vec<(&[u8], &[u8])>) -> Result<(), StorageError> {
        StorageInterface::batch_write(self, entries)
    }
}
