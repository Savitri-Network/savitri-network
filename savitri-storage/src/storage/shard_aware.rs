//! Shard-Aware Storage Implementation
//! 
//! This module implements a shard-aware storage system that eliminates the global state_lock bottleneck
//! by providing granular locks at the shard level while maintaining backward compatibility.

#[cfg(feature = "rocksdb")]
use crate::storage::{
    CF_ACCOUNTS_SHARDS, CF_DEFAULT,
    Storage, StorageSnapshot, DbBatch, RocksDb
};
#[cfg(feature = "rocksdb")]
use crate::sharding::{ShardingConfig, ShardId};
use anyhow::Result;
#[cfg(feature = "rocksdb")]
use std::sync::{Arc, RwLock};
#[cfg(feature = "rocksdb")]
use std::collections::HashMap;
#[cfg(feature = "rocksdb")]
use rocksdb::BoundColumnFamily;

/// Shard-aware storage with granular locking
/// 
/// This implementation provides:
/// - Individual locks per shard for true parallelism
/// - Global lock only for cross-shard operations
/// - Backward compatibility with existing Storage API
/// - Performance monitoring and metrics
#[cfg(feature = "rocksdb")]
#[derive(Debug)]
pub struct ShardAwareStorage {
    /// Original storage instance (maintained for compatibility)
    storage: Storage<RocksDb>,
    /// Shard-level locks for accounts (one per shard)
    pub account_shard_locks: Vec<Arc<RwLock<()>>>,
    /// Shard-level locks for contracts (one per shard)
    contract_shard_locks: Vec<Arc<RwLock<()>>>,
    /// Global lock for cross-shard operations only
    global_lock: Arc<RwLock<()>>,
    /// Performance metrics
    metrics: ShardAwareMetrics,
}

/// Performance metrics for shard-aware operations
#[derive(Debug, Default)]
pub struct ShardAwareMetrics {
    /// Number of shard-level lock acquisitions
    pub shard_lock_acquisitions: u64,
    /// Number of global lock acquisitions
    pub global_lock_acquisitions: u64,
    /// Total time spent in shard locks (nanoseconds)
    pub total_shard_lock_time_ns: u64,
    /// Total time spent in global locks (nanoseconds)
    pub total_global_lock_time_ns: u64,
    /// Lock contention events
    pub lock_contentions: u64,
}

/// Lock acquisition result for metrics
#[cfg(feature = "rocksdb")]
#[derive(Debug, Clone)]
pub struct LockResult {
    /// Whether the lock was acquired successfully
    pub acquired: bool,
    /// Time taken to acquire the lock (nanoseconds)
    pub acquisition_time_ns: u64,
    /// Whether there was contention
    pub had_contention: bool,
}

/// Shard operation context for batch operations
#[cfg(feature = "rocksdb")]
#[derive(Debug)]
pub struct ShardOperationContext {
    /// Shards involved in the operation
    pub involved_shards: Vec<ShardId>,
    /// Whether this is a cross-shard operation
    pub is_cross_shard: bool,
    /// Operation type for metrics
    pub operation_type: ShardOperationType,
}

/// Types of shard operations
#[cfg(feature = "rocksdb")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardOperationType {
    Read,
    Write,
    Delete,
    Batch,
    Migration,
}

#[cfg(feature = "rocksdb")]
impl ShardAwareStorage {
    /// Create new shard-aware storage from existing storage
    pub fn new(storage: Storage<RocksDb>) -> Self {
        let num_shards = CF_ACCOUNTS_SHARDS.len();
        
        Self {
            account_shard_locks: (0..num_shards)
                .map(|_| Arc::new(RwLock::new(())))
                .collect(),
            contract_shard_locks: (0..num_shards)
                .map(|_| Arc::new(RwLock::new(())))
                .collect(),
            global_lock: Arc::new(RwLock::new(())),
            storage,
            metrics: ShardAwareMetrics::default(),
        }
    }

    /// Get reference to underlying storage for compatibility
    pub fn as_storage(&self) -> &Storage<RocksDb><RocksDb> {
        &self.storage
    }

    /// Get mutable reference to underlying storage for compatibility
    pub fn as_storage_mut(&mut self) -> &mut Storage<RocksDb><RocksDb> {
        &mut self.storage
    }

    /// Acquire shard lock for accounts with metrics
    pub fn acquire_account_shard_lock(&mut self, shard_id: ShardId) -> LockResult {
        let start = std::time::Instant::now();
        
        let lock_result = if shard_id as usize >= self.account_shard_locks.len() {
            LockResult {
                acquired: false,
                acquisition_time_ns: 0,
                had_contention: false,
            }
        } else {
            // Try to acquire lock with timeout detection
            let lock = &self.account_shard_locks[shard_id as usize];
            let acquisition_time = start.elapsed();
            
            // Check if there was contention by attempting a non-blocking read first
            let had_contention = lock.try_read().is_err();
            
            // Acquire the lock (blocking)
            let _guard = lock.read().unwrap();
            
            LockResult {
                acquired: true,
                acquisition_time_ns: acquisition_time.as_nanos() as u64,
                had_contention,
            }
        };

        // Update metrics
        if lock_result.acquired {
            self.metrics.shard_lock_acquisitions += 1;
            self.metrics.total_shard_lock_time_ns += lock_result.acquisition_time_ns;
            if lock_result.had_contention {
                self.metrics.lock_contentions += 1;
            }
        }

        lock_result
    }

    /// Acquire shard lock for contracts with metrics
    pub fn acquire_contract_shard_lock(&mut self, shard_id: ShardId) -> LockResult {
        let start = std::time::Instant::now();
        
        let lock_result = if shard_id as usize >= self.contract_shard_locks.len() {
            LockResult {
                acquired: false,
                acquisition_time_ns: 0,
                had_contention: false,
            }
        } else {
            let lock = &self.contract_shard_locks[shard_id as usize];
            let acquisition_time = start.elapsed();
            let had_contention = lock.try_read().is_err();
            let _guard = lock.read().unwrap();
            
            LockResult {
                acquired: true,
                acquisition_time_ns: acquisition_time.as_nanos() as u64,
                had_contention,
            }
        };

        if lock_result.acquired {
            self.metrics.shard_lock_acquisitions += 1;
            self.metrics.total_shard_lock_time_ns += lock_result.acquisition_time_ns;
            if lock_result.had_contention {
                self.metrics.lock_contentions += 1;
            }
        }

        lock_result
    }

    /// Acquire global lock for cross-shard operations
    pub fn acquire_global_lock(&mut self) -> LockResult {
        let start = std::time::Instant::now();
        let lock = &self.global_lock;
        let acquisition_time = start.elapsed();
        let had_contention = lock.try_read().is_err();
        let _guard = lock.read().unwrap();
        
        let lock_result = LockResult {
            acquired: true,
            acquisition_time_ns: acquisition_time.as_nanos() as u64,
            had_contention,
        };

        self.metrics.global_lock_acquisitions += 1;
        self.metrics.total_global_lock_time_ns += lock_result.acquisition_time_ns;
        if lock_result.had_contention {
            self.metrics.lock_contentions += 1;
        }

        lock_result
    }

    /// Execute operation with proper shard locking
    pub fn execute_with_shard_lock<F, R>(
        &mut self,
        shard_id: ShardId,
        _operation: ShardOperationType,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&Storage<RocksDb>) -> Result<R>,
    {
        let _lock = self.account_shard_locks[shard_id as usize]
            .write()
            .map_err(|_| anyhow::anyhow!("Shard lock poisoned"))?;
        
        f(&self.storage)
    }

    /// Execute cross-shard operation with ordered locking
    pub fn execute_cross_shard<F, R>(
        &mut self,
        shard_ids: &[ShardId],
        _operation: ShardOperationType,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&Storage<RocksDb>) -> Result<R>,
    {
        if shard_ids.len() <= 1 {
            // Single shard operation
            if let Some(&shard_id) = shard_ids.first() {
                return self.execute_with_shard_lock(shard_id, ShardOperationType::Read, f);
            } else {
                return f(&self.storage);
            }
        }

        // Cross-shard operation: acquire locks in order to prevent deadlock
        let mut sorted_shards = shard_ids.to_vec();
        sorted_shards.sort();
        sorted_shards.dedup();

        // Acquire all shard locks
        let mut locks = Vec::with_capacity(sorted_shards.len());
        for &shard_id in &sorted_shards {
            if (shard_id as usize) < self.account_shard_locks.len() {
                locks.push(self.account_shard_locks[shard_id as usize]
                    .write()
                    .map_err(|_| anyhow::anyhow!("Shard {} lock poisoned", shard_id))?);
            }
        }

        // Execute operation
        let result = f(&self.storage);
        
        // Locks are released automatically when dropped
        result
    }

    /// Get performance metrics
    pub fn get_metrics(&self) -> ShardAwareMetrics {
        ShardAwareMetrics {
            shard_lock_acquisitions: self.metrics.shard_lock_acquisitions,
            global_lock_acquisitions: self.metrics.global_lock_acquisitions,
            total_shard_lock_time_ns: self.metrics.total_shard_lock_time_ns,
            total_global_lock_time_ns: self.metrics.total_global_lock_time_ns,
            lock_contentions: self.metrics.lock_contentions,
        }
    }

    /// Reset performance metrics
    pub fn reset_metrics(&mut self) {
        self.metrics = ShardAwareMetrics::default();
    }

    /// Get lock statistics for monitoring
    pub fn get_lock_statistics(&self) -> HashMap<String, u64> {
        let mut stats = HashMap::new();
        
        stats.insert("shard_lock_acquisitions".to_string(), self.metrics.shard_lock_acquisitions);
        stats.insert("global_lock_acquisitions".to_string(), self.metrics.global_lock_acquisitions);
        stats.insert("total_shard_lock_time_ns".to_string(), self.metrics.total_shard_lock_time_ns);
        stats.insert("total_global_lock_time_ns".to_string(), self.metrics.total_global_lock_time_ns);
        stats.insert("lock_contentions".to_string(), self.metrics.lock_contentions);
        
        // Calculate averages
        if self.metrics.shard_lock_acquisitions > 0 {
            stats.insert("avg_shard_lock_time_ns".to_string(), 
                self.metrics.total_shard_lock_time_ns / self.metrics.shard_lock_acquisitions);
        }
        
        if self.metrics.global_lock_acquisitions > 0 {
            stats.insert("avg_global_lock_time_ns".to_string(), 
                self.metrics.total_global_lock_time_ns / self.metrics.global_lock_acquisitions);
        }
        
        // Calculate contention rate
        let total_acquisitions = self.metrics.shard_lock_acquisitions + self.metrics.global_lock_acquisitions;
        if total_acquisitions > 0 {
            stats.insert("contention_rate_percent".to_string(), 
                (self.metrics.lock_contentions * 100) / total_acquisitions);
        }
        
        stats
    }

    /// Analyze lock performance and provide recommendations
    pub fn analyze_performance(&self) -> Vec<String> {
        let mut recommendations = Vec::new();
        let stats = self.get_lock_statistics();

        // Check for high contention
        if let Some(&contention_rate) = stats.get("contention_rate_percent") {
            if contention_rate > 10 {
                recommendations.push(format!(
                    "High lock contention detected: {}% (should be < 10%)",
                    contention_rate
                ));
            }
        }

        // Check for high average lock times
        if let Some(&avg_shard_time) = stats.get("avg_shard_lock_time_ns") {
            if avg_shard_time > 1_000_000 { // 1ms
                recommendations.push(format!(
                    "High average shard lock time: {:.2}ms (should be < 1ms)",
                    avg_shard_time as f64 / 1_000_000.0
                ));
            }
        }

        if let Some(&avg_global_time) = stats.get("avg_global_lock_time_ns") {
            if avg_global_time > 5_000_000 { // 5ms
                recommendations.push(format!(
                    "High average global lock time: {:.2}ms (should be < 5ms)",
                    avg_global_time as f64 / 1_000_000.0
                ));
            }
        }

        // Check global vs shard lock ratio
        let shard_locks = stats.get("shard_lock_acquisitions").unwrap_or(&0);
        let global_locks = stats.get("global_lock_acquisitions").unwrap_or(&0);
        
        if *global_locks > 0 && *shard_locks > 0 {
            let ratio = *global_locks as f64 / *shard_locks as f64;
            if ratio > 0.2 { // More than 20% global locks
                recommendations.push(format!(
                    "High global lock usage: {:.1}% of total locks (should be < 20%)",
                    ratio * 100.0
                ));
            }
        }

        if recommendations.is_empty() {
            recommendations.push("Lock performance is optimal".to_string());
        }

        recommendations
    }
}

#[cfg(feature = "rocksdb")]
/// Batch operations with shard-aware locking
impl ShardAwareStorage {
    /// Create operation context for batch operations
    pub fn create_operation_context(
        &self,
        addresses: &[&[u8]],
        config: &ShardingConfig,
    ) -> ShardOperationContext {
        let mut involved_shards = std::collections::HashSet::new();
        
        for addr in addresses {
            let shard_id = config.shard_for_address(addr);
            involved_shards.insert(shard_id);
        }
        
        let mut shard_vec: Vec<_> = involved_shards.into_iter().collect();
        shard_vec.sort();
        
        ShardOperationContext {
            involved_shards: shard_vec.clone(),
            is_cross_shard: shard_vec.len() > 1,
            operation_type: ShardOperationType::Batch,
        }
    }

    /// Create a batch operation with automatic shard locking
    pub fn batch_with_shard_locks(&mut self, addresses: &[&[u8]], config: &ShardingConfig) -> Result<ShardAwareBatch<'_>> {
        let context = self.create_operation_context(addresses, config);
        
        // Acquire necessary locks
        let mut locks = Vec::new();
        if context.is_cross_shard {
            // For cross-shard operations, acquire all involved locks
            for &shard_id in &context.involved_shards {
                if (shard_id as usize) < self.account_shard_locks.len() {
                    locks.push(self.account_shard_locks[shard_id as usize]
                        .write()
                        .map_err(|_| anyhow::anyhow!("Shard {} lock poisoned", shard_id))?);
                }
            }
        } else if let Some(&shard_id) = context.involved_shards.first() {
            // Single shard operation
            if (shard_id as usize) < self.account_shard_locks.len() {
                locks.push(self.account_shard_locks[shard_id as usize]
                    .write()
                    .map_err(|_| anyhow::anyhow!("Shard {} lock poisoned", shard_id))?);
            }
        }

        // Create batch using underlying storage
        let batch = self.storage.begin_batch();
        
        Ok(ShardAwareBatch {
            batch,
            _locks: locks,
            context,
        })
    }
}

/// Shard-aware batch operation
pub struct ShardAwareBatch<'a> {
    batch: DbBatch<'a>,
    _locks: Vec<std::sync::RwLockWriteGuard<'a, ()>>,
    context: ShardOperationContext,
}

impl<'a> ShardAwareBatch<'a> {
    /// Get reference to underlying batch
    pub fn as_batch(&mut self) -> &mut DbBatch<'a> {
        &mut self.batch
    }

    /// Commit the batch (locks are released automatically)
    pub fn commit(self) -> Result<()> {
        self.batch.commit()
    }

    /// Get operation context
    pub fn context(&self) -> &ShardOperationContext {
        &self.context
    }
}

#[cfg(feature = "rocksdb")]
/// Compatibility layer for existing Storage API
impl ShardAwareStorage {
    /// Forward to underlying storage methods for compatibility
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.storage.get_cf(super::CF_DEFAULT, key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.storage.put_cf(super::CF_DEFAULT, key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.storage.delete_cf(super::CF_DEFAULT, key)
    }

    pub fn batch(&self) -> DbBatch<'_> {
        self.storage.begin_batch()
    }

    #[cfg(feature = "rocksdb")]
    pub fn create_snapshot(&self) -> StorageSnapshot<'_> {
        StorageSnapshot { db: &self.storage.db }
    }

    pub fn cf(&self, name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.storage.cf(name)
    }

    pub fn get_cf(&self, cf: &rocksdb::ColumnFamily, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Get column family name from the ColumnFamily handle
        let cf_name = self.get_cf_name(cf)?;
        self.storage.get_cf(&cf_name, key)
    }

    pub fn put_cf(&self, cf: &rocksdb::ColumnFamily, key: &[u8], value: &[u8]) -> Result<()> {
        // Get column family name from the ColumnFamily handle
        let cf_name = self.get_cf_name(cf)?;
        self.storage.put_cf(&cf_name, key, value)
    }

    pub fn delete_cf(&self, cf: &rocksdb::ColumnFamily, key: &[u8]) -> Result<()> {
        // Get column family name from the ColumnFamily handle
        let cf_name = self.get_cf_name(cf)?;
        self.storage.delete_cf(&cf_name, key)
    }

    pub fn iterator_cf<'a>(&'a self, cf: &'a rocksdb::ColumnFamily) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a> {
        self.storage.iterator_cf(cf)
    }
}

