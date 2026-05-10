//! Monolith Storage Integration
//!
//! This module provides real storage integration for monolith blocks
//! using RocksDB for persistent storage and retrieval.

use crate::monolith_producer::MonolithBlock;
use anyhow::{Context, Result};
use bincode;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
// use savitri_zkp::monolith::MonolithHeader;
use savitri_core::core::monolith::MonolithHeader;

/// Monolith storage configuration
#[derive(Debug, Clone)]
pub struct MonolithStorageConfig {
    /// Database path
    pub db_path: PathBuf,
    /// Maximum number of monoliths to keep
    pub max_monoliths: usize,
    /// Cache size in memory
    pub cache_size: usize,
    /// Enable compression
    pub enable_compression: bool,
    /// Write buffer size
    pub write_buffer_size: usize,
}

impl Default for MonolithStorageConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("./data/monoliths"),
            max_monoliths: 10000,
            cache_size: 1000,
            enable_compression: true,
            write_buffer_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

/// Monolith storage manager
pub struct MonolithStorage {
    config: MonolithStorageConfig,
    db: Arc<RwLock<Option<DB>>>,
    cache: Arc<RwLock<lru::LruCache<u64, MonolithBlock>>>,
    stats: Arc<RwLock<MonolithStorageStats>>,
}

/// Storage statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MonolithStorageStats {
    pub total_monoliths: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub storage_size_bytes: u64,
    pub last_cleanup: u64,
    pub db_operations: u64,
}

impl MonolithStorage {
    /// Create new monolith storage
    pub fn new(config: MonolithStorageConfig) -> Result<Self> {
        // Create database directory and check for lock files
        std::fs::create_dir_all(&config.db_path)
            .context("Failed to create monolith storage directory")?;

        // Check for and remove stale lock files
        let lock_file_path = config.db_path.join("LOCK");
        if lock_file_path.exists() {
            warn!("Found stale LOCK file, attempting to remove it");
            if let Err(e) = std::fs::remove_file(&lock_file_path) {
                warn!("Failed to remove LOCK file: {}", e);
            } else {
                info!("Successfully removed stale LOCK file");
            }
        }

        // Initialize RocksDB with minimal configuration
        let mut db_options = Options::default();
        db_options.create_if_missing(true);

        // Use most basic settings to avoid corruption
        db_options.set_write_buffer_size(4 * 1024 * 1024); // 4MB
        db_options.set_max_write_buffer_number(2);
        db_options.set_target_file_size_base(64 * 1024 * 1024); // 64MB

        let db = match DB::open(&db_options, &config.db_path) {
            Ok(db) => {
                info!("Successfully opened monolith storage database");
                db
            }
            Err(e) => {
                warn!(
                    "Failed to open monolith storage database, attempting recovery: {}",
                    e
                );

                // Try to open with read-only first to check if database is corrupted
                match DB::open_for_read_only(&db_options, &config.db_path, false) {
                    Ok(_) => {
                        warn!("Database exists but is locked, trying to create new instance");
                        // Database is locked, try with different options
                        let mut recovery_options = db_options.clone();
                        recovery_options.set_use_fsync(false);

                        DB::open(&recovery_options, &config.db_path).context(
                            "Failed to open monolith storage database even with recovery options",
                        )?
                    }
                    Err(_) => {
                        warn!("Database appears corrupted or inaccessible, creating new database");
                        // Remove corrupted database and create new one
                        if let Err(cleanup_err) = std::fs::remove_dir_all(&config.db_path) {
                            warn!(
                                "Failed to clean up corrupted database directory: {}",
                                cleanup_err
                            );
                        }

                        // Recreate directory
                        std::fs::create_dir_all(&config.db_path)
                            .context("Failed to recreate monolith storage directory")?;

                        DB::open(&db_options, &config.db_path)
                            .context("Failed to create new monolith storage database")?
                    }
                }
            }
        };

        // Initialize LRU cache
        let cache_size = std::num::NonZeroUsize::new(config.cache_size).unwrap_or_else(|| {
            std::num::NonZeroUsize::new(1000)
                .unwrap_or_else(|| std::num::NonZeroUsize::new(1).unwrap())
        });
        let cache = Arc::new(RwLock::new(lru::LruCache::new(cache_size)));

        info!(
            db_path = %config.db_path.display(),
            max_monoliths = config.max_monoliths,
            cache_size = config.cache_size,
            "Monolith storage initialized"
        );

        Ok(Self {
            config,
            db: Arc::new(RwLock::new(Some(db))),
            cache,
            stats: Arc::new(RwLock::new(MonolithStorageStats::default())),
        })
    }

    /// Store monolith block
    pub async fn store_monolith(&self, block: &MonolithBlock) -> Result<()> {
        let block_height = block.end_height;

        // Store in cache first
        {
            let cache_result =
                tokio::time::timeout(Duration::from_secs(5), self.cache.write()).await;

            match cache_result {
                Ok(mut cache) => {
                    cache.put(block_height, block.clone());
                }
                Err(_) => {
                    error!("Timeout acquiring cache write lock for monolith storage");
                    return Err(anyhow::anyhow!("Cache lock timeout"));
                }
            }
        }

        // Store in database
        let db_result = tokio::time::timeout(Duration::from_secs(5), self.db.read()).await;

        match db_result {
            Ok(db) => {
                if let Some(db) = db.as_ref() {
                    let key = Self::height_to_key(block_height);
                    let value = block
                        .to_bytes()
                        .context("Failed to serialize monolith block")?;

                    db.put(key.as_bytes(), &value)
                        .context("Failed to store monolith in database")?;

                    // Update stats
                    let stats_result =
                        tokio::time::timeout(Duration::from_secs(2), self.stats.write()).await;

                    match stats_result {
                        Ok(mut stats) => {
                            stats.total_monoliths += 1;
                            stats.storage_size_bytes += value.len() as u64;
                            stats.db_operations += 1;
                        }
                        Err(_) => {
                            warn!("Timeout acquiring stats write lock - continuing without stats update");
                        }
                    }

                    info!(
                        height = block_height,
                        size_bytes = value.len(),
                        "Monolith block stored successfully"
                    );

                    // Cleanup old monoliths if needed
                    if let Err(e) = self.cleanup_old_monoliths().await {
                        warn!("Failed to cleanup old monoliths: {}", e);
                    }
                }
            }
            Err(_) => {
                error!("Timeout acquiring database read lock for monolith storage");
                return Err(anyhow::anyhow!("Database lock timeout"));
            }
        }

        Ok(())
    }

    /// Retrieve monolith block by height
    pub async fn get_monolith(&self, height: u64) -> Result<Option<MonolithBlock>> {
        // Check cache first
        {
            let cache_result =
                tokio::time::timeout(Duration::from_secs(3), self.cache.write()).await;

            match cache_result {
                Ok(mut cache) => {
                    if let Some(block) = cache.get(&height) {
                        let stats_result =
                            tokio::time::timeout(Duration::from_secs(2), self.stats.write()).await;

                        if let Ok(mut stats) = stats_result {
                            stats.cache_hits += 1;
                        } else {
                            warn!("Timeout acquiring stats write lock - continuing");
                        }

                        debug!(height = height, "Monolith found in cache");
                        return Ok(Some(block.clone()));
                    }
                }
                Err(_) => {
                    warn!("Timeout acquiring cache write lock - checking database directly");
                }
            }
        }

        // Check database
        let db_result = tokio::time::timeout(Duration::from_secs(5), self.db.read()).await;

        match db_result {
            Ok(db) => {
                if let Some(db) = db.as_ref() {
                    let key = Self::height_to_key(height);

                    match db.get(key.as_bytes()) {
                        Ok(Some(value)) => {
                            let block = MonolithBlock::from_bytes(&value)
                                .context("Failed to deserialize monolith block")?;

                            // Store in cache
                            let cache_result =
                                tokio::time::timeout(Duration::from_secs(3), self.cache.write())
                                    .await;

                            if let Ok(mut cache) = cache_result {
                                cache.put(height, block.clone());
                            } else {
                                warn!("Timeout acquiring cache write lock for storing retrieved monolith");
                            }

                            let stats_result =
                                tokio::time::timeout(Duration::from_secs(2), self.stats.write())
                                    .await;

                            if let Ok(mut stats) = stats_result {
                                stats.cache_misses += 1;
                                stats.db_operations += 1;
                            } else {
                                warn!("Timeout acquiring stats write lock - continuing");
                            }

                            info!(height = height, "Monolith retrieved from database");
                            Ok(Some(block))
                        }
                        Ok(None) => {
                            let stats_result =
                                tokio::time::timeout(Duration::from_secs(2), self.stats.write())
                                    .await;

                            if let Ok(mut stats) = stats_result {
                                stats.cache_misses += 1;
                                stats.db_operations += 1;
                            } else {
                                warn!("Timeout acquiring stats write lock - continuing");
                            }

                            debug!(height = height, "Monolith not found");
                            Ok(None)
                        }
                        Err(e) => {
                            error!(height = height, error = %e, "Database error retrieving monolith");
                            Err(e.into())
                        }
                    }
                } else {
                    warn!("Database not available");
                    Ok(None)
                }
            }
            Err(_) => {
                error!("Timeout acquiring database read lock for monolith retrieval");
                Err(anyhow::anyhow!("Database lock timeout"))
            }
        }
    }

    /// Get monoliths in height range
    pub async fn get_monoliths_in_range(
        &self,
        start_height: u64,
        end_height: u64,
    ) -> Result<Vec<MonolithBlock>> {
        let mut monoliths = Vec::new();

        for height in start_height..=end_height {
            if let Some(block) = self.get_monolith(height).await? {
                monoliths.push(block);
            }
        }

        info!(
            start_height = start_height,
            end_height = end_height,
            found_count = monoliths.len(),
            "Retrieved monoliths in range"
        );

        Ok(monoliths)
    }

    /// Get latest monolith
    pub async fn get_latest_monolith(&self) -> Result<Option<MonolithBlock>> {
        // Find the highest height in cache or database
        let mut latest_height = None;

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some((&height, _)) = cache.peek_lru() {
                latest_height = Some(height);
            }
        }

        // Check database if not found in cache
        if latest_height.is_none() {
            let db = self.db.read().await;
            if let Some(db) = db.as_ref() {
                let iter = db.iterator(IteratorMode::End);
                for item in iter {
                    if let Ok((key, _)) = item {
                        let height = Self::key_to_height(&key)?;
                        if latest_height.is_none() || height > latest_height.unwrap_or(0) {
                            latest_height = Some(height);
                        }
                    }
                }
            }
        }

        if let Some(height) = latest_height {
            self.get_monolith(height).await
        } else {
            Ok(None)
        }
    }

    /// Delete monolith by height
    pub async fn delete_monolith(&self, height: u64) -> Result<bool> {
        // Remove from cache
        {
            let mut cache = self.cache.write().await;
            cache.pop(&height);
        }

        // Remove from database
        let db = self.db.read().await;
        if let Some(db) = db.as_ref() {
            let key = Self::height_to_key(height);
            match db.get(key.as_bytes()) {
                Ok(Some(value)) => {
                    let size = value.len();
                    db.delete(key.as_bytes())
                        .context("Failed to delete monolith from database")?;

                    // Update stats
                    {
                        let mut stats = self.stats.write().await;
                        stats.total_monoliths = stats.total_monoliths.saturating_sub(1);
                        stats.storage_size_bytes =
                            stats.storage_size_bytes.saturating_sub(size as u64);
                        stats.db_operations += 1;
                    }

                    info!(height = height, "Monolith deleted successfully");
                    Ok(true)
                }
                Ok(None) => {
                    debug!(height = height, "Monolith not found for deletion");
                    Ok(false)
                }
                Err(e) => {
                    error!(height = height, error = %e, "Database error deleting monolith");
                    Err(e.into())
                }
            }
        } else {
            warn!("Database not available for deletion");
            Ok(false)
        }
    }

    /// Cleanup old monoliths
    pub async fn cleanup_old_monoliths(&self) -> Result<()> {
        let current_count = {
            let stats = self.stats.read().await;
            stats.total_monoliths
        };

        if current_count <= self.config.max_monoliths as u64 {
            return Ok(());
        }

        let to_remove = current_count - self.config.max_monoliths as u64;
        let mut removed = 0;

        // Get all monolith heights and sort
        let mut heights = Vec::new();
        let db = self.db.read().await;
        if let Some(db) = db.as_ref() {
            let iter = db.iterator(IteratorMode::Start);
            for item in iter {
                if let Ok((key, _)) = item {
                    if let Ok(height) = Self::key_to_height(&key) {
                        heights.push(height);
                    }
                }
            }
        }

        heights.sort_unstable();

        // Remove oldest monoliths
        for height in heights.iter().take(to_remove as usize) {
            if self.delete_monolith(*height).await? {
                removed += 1;
            }
        }

        if removed > 0 {
            info!(
                removed = removed,
                remaining = current_count - removed,
                "Cleaned up old monoliths"
            );
        }

        // Update cleanup timestamp
        {
            let mut stats = self.stats.write().await;
            stats.last_cleanup = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        Ok(())
    }

    /// Get storage statistics
    pub async fn get_stats(&self) -> MonolithStorageStats {
        self.stats.read().await.clone()
    }

    /// Compact database
    pub async fn compact_database(&self) -> Result<()> {
        let db = self.db.read().await;
        if let Some(db) = db.as_ref() {
            info!("Starting database compaction...");
            db.compact_range::<&[u8], &[u8]>(None, None);
            info!("Database compaction completed");
        }
        Ok(())
    }

    /// Convert height to database key
    fn height_to_key(height: u64) -> String {
        format!("monolith_{:020}", height)
    }

    /// Convert database key to height
    fn key_to_height(key: &[u8]) -> Result<u64> {
        let key_str = std::str::from_utf8(key).context("Invalid key format")?;

        if key_str.starts_with("monolith_") {
            let height_str = &key_str[9..];
            height_str
                .parse::<u64>()
                .context("Failed to parse height from key")
        } else {
            Err(anyhow::anyhow!("Invalid key format"))
        }
    }

    /// Close storage
    pub async fn close(&self) -> Result<()> {
        let mut db = self.db.write().await;
        if let Some(database) = db.take() {
            drop(database);
            info!("Monolith storage closed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_monolith_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = MonolithStorageConfig {
            db_path: temp_dir.path().to_path_buf(),
            max_monoliths: 100,
            cache_size: 10,
            enable_compression: false,
            write_buffer_size: 1024,
        };

        let storage = MonolithStorage::new(config).unwrap();

        // Create test monolith
        let block = MonolithBlock {
            header: MonolithHeader {
                headers_commit: [1; 64],
                state_commit: [2; 64],
                exec_height: 1000,
                epoch_id: 123,
            },
            start_height: 900,
            end_height: 1000,
            block_count: 100,
            total_transactions: 15000,
            created_at: 123456789,
            creator_id: "test_node".to_string(),
            zkp_proof: vec![1, 2, 3, 4],
        };

        // Test storage and retrieval
        storage.store_monolith(&block).await.unwrap();
        let retrieved = storage.get_monolith(1000).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().end_height, 1000);

        // Test stats
        let stats = storage.get_stats().await;
        assert_eq!(stats.total_monoliths, 1);
        assert_eq!(stats.cache_hits, 1);

        println!("✅ Monolith storage test passed!");
    }

    #[tokio::test]
    async fn test_monolith_range_query() {
        let temp_dir = TempDir::new().unwrap();
        let config = MonolithStorageConfig {
            db_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let storage = MonolithStorage::new(config).unwrap();

        // Store multiple monoliths
        for i in 1000..=1010 {
            let block = MonolithBlock {
                header: MonolithHeader {
                    headers_commit: [i as u8; 64],
                    state_commit: [(i + 1) as u8; 64],
                    exec_height: i,
                    epoch_id: 123,
                },
                start_height: i - 100,
                end_height: i,
                block_count: 100,
                total_transactions: 15000,
                created_at: 123456789,
                creator_id: "test_node".to_string(),
                zkp_proof: vec![1, 2, 3, 4],
            };
            storage.store_monolith(&block).await.unwrap();
        }

        // Test range query
        let monoliths = storage.get_monoliths_in_range(1005, 1008).await.unwrap();
        assert_eq!(monoliths.len(), 4);

        println!("✅ Monolith range query test passed!");
    }
}
