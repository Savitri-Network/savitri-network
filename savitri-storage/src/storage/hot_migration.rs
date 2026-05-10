//! Hot Migration Engine for Zero-Downtime Shard Migration
//! 
//! This module implements a comprehensive hot migration system that allows
//! seamless migration of accounts between different shard configurations
//! without system downtime.

use crate::storage::{Storage, CF_ACCOUNT_TO_SHARD, CF_ACCOUNTS_SHARDS, RocksDb};
use crate::sharding::{ShardingConfig, ShardId};
use crate::storage::shard_aware::ShardAwareStorage;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
#[cfg(feature = "tokio")]
use tokio::sync::Notify;

#[cfg(not(feature = "tokio"))]
struct Notify;

#[cfg(not(feature = "tokio"))]
impl Notify {
    fn new() -> Self {
        Self
    }
    
    fn notify_one(&self) {
        // Mock implementation - does nothing
    }
    
    async fn notified(&self) {
        // Mock implementation - never resolves
        std::future::pending().await
    }
}

/// Migration phase tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    Preparation,
    DataCopy,
    Validation,
    Cleanup,
    Completed,
    Failed,
}

/// Migration progress tracking
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    pub phase: MigrationPhase,
    pub total_accounts: u64,
    pub migrated_accounts: u64,
    pub validated_accounts: u64,
    pub failed_accounts: u64,
    pub start_time: Instant,
    pub estimated_completion: Option<Instant>,
    pub current_shard: Option<ShardId>,
    pub error_message: Option<String>,
}

impl Default for MigrationProgress {
    fn default() -> Self {
        Self {
            phase: MigrationPhase::Preparation,
            total_accounts: 0,
            migrated_accounts: 0,
            validated_accounts: 0,
            failed_accounts: 0,
            start_time: Instant::now(),
            estimated_completion: None,
            current_shard: None,
            error_message: None,
        }
    }
}

/// Migration statistics
#[derive(Debug, Clone)]
pub struct MigrationStatistics {
    pub accounts_moved: HashMap<(ShardId, ShardId), u64>,
    pub total_bytes_copied: u64,
    pub copy_time_ms: u64,
    pub validation_time_ms: u64,
    pub cleanup_time_ms: u64,
    pub total_migration_time_ms: u64,
    pub peak_memory_usage_mb: u64,
}

impl Default for MigrationStatistics {
    fn default() -> Self {
        Self {
            accounts_moved: HashMap::new(),
            total_bytes_copied: 0,
            copy_time_ms: 0,
            validation_time_ms: 0,
            cleanup_time_ms: 0,
            total_migration_time_ms: 0,
            peak_memory_usage_mb: 0,
        }
    }
}

/// Hot migration engine configuration
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Batch size for account processing
    pub batch_size: usize,
    /// Timeout for individual operations
    pub operation_timeout: Duration,
    /// Maximum concurrent operations
    pub max_concurrent_operations: usize,
    /// Progress reporting interval
    pub progress_report_interval: Duration,
    /// Enable detailed logging
    pub enable_detailed_logging: bool,
    /// Memory limit for migration (MB)
    pub memory_limit_mb: u64,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            operation_timeout: Duration::from_secs(30),
            max_concurrent_operations: 4,
            progress_report_interval: Duration::from_secs(5),
            enable_detailed_logging: true,
            memory_limit_mb: 1024, // 1GB
        }
    }
}

/// Hot migration engine for zero-downtime shard migration
pub struct HotMigrationEngine {
    /// Storage instance
    storage: Arc<Storage<RocksDb>>,
    /// Shard-aware storage for granular operations
    shard_aware_storage: Arc<ShardAwareStorage>,
    /// Old sharding configuration
    old_config: ShardingConfig,
    /// New sharding configuration
    new_config: ShardingConfig,
    /// Migration configuration
    config: MigrationConfig,
    /// Migration state
    state: Arc<Mutex<MigrationState>>,
    /// Progress notification
    progress_notify: Arc<Notify>,
}

/// Internal migration state
struct MigrationState {
    phase: MigrationPhase,
    progress: MigrationProgress,
    statistics: MigrationStatistics,
    is_cancelled: bool,
    pause_requested: bool,
}

impl HotMigrationEngine {
    /// Create new hot migration engine
    pub fn new(
        storage: Arc<Storage<RocksDb>>,
        old_config: ShardingConfig,
        new_config: ShardingConfig,
        config: MigrationConfig,
    ) -> Result<Self> {
        let shard_aware_storage = Arc::new(ShardAwareStorage::new((*storage).clone()));
        
        Ok(Self {
            storage,
            shard_aware_storage,
            old_config,
            new_config,
            config,
            state: Arc::new(Mutex::new(MigrationState {
                phase: MigrationPhase::Preparation,
                progress: MigrationProgress::default(),
                statistics: MigrationStatistics::default(),
                is_cancelled: false,
                pause_requested: false,
            })),
            progress_notify: Arc::new(Notify::new()),
        })
    }

    /// Start hot migration process
    pub async fn start_migration(&self) -> Result<MigrationHandle> {
        let handle = MigrationHandle::new(self.state.clone(), self.progress_notify.clone());
        
        // Start migration in background
        #[cfg(feature = "tokio")]
        {
            let engine = self.clone();
            tokio::spawn(async move {
                if let Err(e) = engine.execute_migration().await {
                    engine.set_failed(format!("Migration failed: {}", e));
                }
            });
        }
        
        #[cfg(not(feature = "tokio"))]
        {
            // Synchronous migration for non-tokio builds
            if let Err(e) = std::thread::spawn(move || {
                // For now, just return an error as we need async
                Err::<anyhow::Result<()>, anyhow::Error>(anyhow::anyhow!("Migration requires tokio feature"))
            }).join() {
                self.set_failed(format!("Migration failed: {:?}", e));
            }
        }
        
        Ok(handle)
    }

    /// Execute the migration process
    async fn execute_migration(&self) -> Result<()> {
        self.set_phase(MigrationPhase::Preparation);
        let migration_plan = self.prepare_migration().await?;
        
        self.set_phase(MigrationPhase::DataCopy);
        self.copy_accounts_incrementally(&migration_plan).await?;
        
        self.set_phase(MigrationPhase::Validation);
        self.validate_migration().await?;
        
        self.set_phase(MigrationPhase::Cleanup);
        self.cleanup_old_shards().await?;
        
        // Phase 5: Completion
        self.set_phase(MigrationPhase::Completed);
        
        Ok(())
    }

    /// Prepare migration by analyzing accounts to migrate
    async fn prepare_migration(&self) -> Result<MigrationPlan> {
        let start_time = Instant::now();
        let mut migration_map = HashMap::new();
        let mut total_accounts = 0u64;
        
        // Scan all existing shards
        for old_shard_id in 0..self.old_config.normalized_num_shards() {
            let cf_name = Self::get_shard_cf_name(old_shard_id as u16)?;
            let cf = self.storage.cf(&cf_name)?;
            
            let iter = self.storage.iterator_cf(&cf)?;
            for item_result in iter {
                match item_result {
                    Ok((addr, _)) => {
                        let addr_vec: Vec<u8> = addr.to_vec();
                        let old_shard = self.old_config.shard_for_address(&addr_vec);
                        let new_shard = self.new_config.shard_for_address(&addr_vec);
                        
                        if old_shard != new_shard {
                            migration_map.insert(addr.to_vec(), (old_shard, new_shard));
                            total_accounts += 1;
                        }
                    }
                    Err(_) => continue,
                }
                
                // Check for cancellation
                if self.is_cancelled() {
                    return Err(anyhow::anyhow!("Migration cancelled during preparation"));
                }
            }
        }
        
        let preparation_time = start_time.elapsed();
        
        // Update progress
        {
            let mut state = self.state.lock().unwrap();
            state.progress.total_accounts = total_accounts;
            state.progress.estimated_completion = Some(
                Instant::now() + Duration::from_secs((total_accounts / 1000).max(60))
            );
        }
        
        if self.config.enable_detailed_logging {
            println!("Migration preparation completed:");
            println!("  Total accounts to migrate: {}", total_accounts);
            println!("  Preparation time: {:?}", preparation_time);
            println!("  Estimated completion: {:?}", 
                Instant::now() + Duration::from_secs((total_accounts / 1000).max(60)));
        }
        
        Ok(MigrationPlan {
            migration_map,
            total_accounts,
            preparation_time_ms: preparation_time.as_millis() as u64,
        })
    }

    /// Copy accounts incrementally with progress tracking
    async fn copy_accounts_incrementally(&self, plan: &MigrationPlan) -> Result<()> {
        let start_time = Instant::now();
        let mut processed_accounts = 0u64;
        let mut bytes_copied = 0u64;
        
        // Process accounts in batches
        let mut account_iter = plan.migration_map.iter().collect::<Vec<_>>();
        account_iter.sort_by_key(|(_, &(old_shard, _))| old_shard); // Group by source shard
        
        for batch_start in (0..account_iter.len()).step_by(self.config.batch_size) {
            let batch_end = (batch_start + self.config.batch_size).min(account_iter.len());
            let batch = &account_iter[batch_start..batch_end];
            
            // Group batch by destination shard for efficient processing
            let mut dest_groups: HashMap<ShardId, Vec<(Vec<u8>, (ShardId, ShardId))>> = HashMap::new();
            for (addr, &(old_shard, new_shard)) in batch {
                let addr_vec: Vec<u8> = (*addr).to_vec();
                dest_groups.entry(new_shard).or_default().push((addr_vec, (old_shard, new_shard)));
            }
            
            // Process each destination shard group
            for (dest_shard, accounts) in dest_groups {
                // Acquire destination shard lock
                let _lock = self.shard_aware_storage
                    .account_shard_locks[dest_shard as usize]
                    .write()
                    .map_err(|_| anyhow::anyhow!("Destination shard {} lock poisoned", dest_shard))?;
                
                // Copy accounts to destination shard
                for (addr, (old_shard, new_shard)) in accounts {
                    // Check for cancellation
                    if self.is_cancelled() {
                        return Err(anyhow::anyhow!("Migration cancelled during data copy"));
                    }
                    
                    // Get account data from source shard
                    let source_cf_name = Self::get_shard_cf_name(old_shard)?;
                    let _source_cf = self.storage.cf(&source_cf_name)?;
                    
                    if let Some(account_data) = self.storage.get_cf(&source_cf_name, &addr)? {
                        // Copy to destination shard
                        let dest_cf_name = Self::get_shard_cf_name(new_shard)?;
                        let _dest_cf = self.storage.cf(&dest_cf_name)?;
                        
                        self.storage.put_cf(&dest_cf_name, &addr, &account_data)?;
                        bytes_copied += account_data.len() as u64;
                        
                        // Update mapping
                        self.storage.put_cf(CF_ACCOUNT_TO_SHARD, &addr, &new_shard.to_le_bytes())?;
                        
                        // Update statistics
                        {
                            let mut state = self.state.lock().unwrap();
                            let key = (old_shard, new_shard);
                            *state.statistics.accounts_moved.entry(key).or_insert(0) += 1;
                            state.progress.migrated_accounts += 1;
                            state.progress.current_shard = Some(new_shard);
                        }
                        
                        processed_accounts += 1;
                    }
                }
            }
            
            // Progress reporting
            if processed_accounts % (self.config.batch_size as u64 * 10) == 0 {
                self.progress_notify.notify_one();
                
                if self.config.enable_detailed_logging {
                    let progress_percent = (processed_accounts as f64 / plan.total_accounts as f64) * 100.0;
                    println!("Migration progress: {:.1}% ({}/{})", 
                            progress_percent, processed_accounts, plan.total_accounts);
                }
            }
        }
        
        let copy_time = start_time.elapsed();
        
        // Update statistics
        {
            let mut state = self.state.lock().unwrap();
            state.statistics.total_bytes_copied = bytes_copied;
            state.statistics.copy_time_ms = copy_time.as_millis() as u64;
        }
        
        if self.config.enable_detailed_logging {
            println!("Data copy completed:");
            println!("  Accounts processed: {}", processed_accounts);
            println!("  Bytes copied: {} MB", bytes_copied / 1_048_576);
            println!("  Copy time: {:?}", copy_time);
        }
        
        Ok(())
    }

    /// Validate migration integrity
    async fn validate_migration(&self) -> Result<()> {
        let start_time = Instant::now();
        let mut validated_accounts = 0u64;
        let mut failed_accounts = 0u64;
        
        // Get migration state
        let _total_migrated = {
            let state = self.state.lock().unwrap();
            state.progress.migrated_accounts
        };
        
        if self.config.enable_detailed_logging {
            println!("Starting migration validation...");
        }
        
        // Validate all migrated accounts
        for old_shard_id in 0..self.old_config.normalized_num_shards() {
            let source_cf_name = Self::get_shard_cf_name(old_shard_id as u16)?;
            let source_cf = self.storage.cf(&source_cf_name)?;
            
            let iter = self.storage.iterator_cf(&source_cf)?;
            for item in iter {
                if let Ok((addr, _)) = item {
                    // Check for cancellation
                    if self.is_cancelled() {
                        return Err(anyhow::anyhow!("Migration cancelled during validation"));
                    }
                    
                    let old_shard = self.old_config.shard_for_address(&addr);
                    let new_shard = self.new_config.shard_for_address(&addr);
                    
                    if old_shard != new_shard {
                        // Verify account exists in new shard
                        let dest_cf_name = Self::get_shard_cf_name(new_shard)?;
                        let _dest_cf = self.storage.cf(&dest_cf_name)?;
                        
                        match self.storage.get_cf(&dest_cf_name, &addr) {
                            Ok(Some(_)) => {
                                validated_accounts += 1;
                            },
                            Ok(None) => {
                                failed_accounts += 1;
                                if self.config.enable_detailed_logging {
                                    eprintln!("Validation failed: Account {:?} not found in destination shard {}", 
                                            addr, new_shard);
                                }
                            },
                            Err(e) => {
                                failed_accounts += 1;
                                if self.config.enable_detailed_logging {
                                    eprintln!("Validation error for account {:?}: {}", addr, e);
                                }
                            }
                        }
                        
                        // Update progress
                        {
                            let mut state = self.state.lock().unwrap();
                            state.progress.validated_accounts = validated_accounts;
                            state.progress.failed_accounts = failed_accounts;
                        }
                    }
                }
            }
        }
        
        let validation_time = start_time.elapsed();
        
        // Update statistics
        {
            let mut state = self.state.lock().unwrap();
            state.statistics.validation_time_ms = validation_time.as_millis() as u64;
        }
        
        if self.config.enable_detailed_logging {
            println!("Migration validation completed:");
            println!("  Validated accounts: {}", validated_accounts);
            println!("  Failed accounts: {}", failed_accounts);
            println!("  Validation time: {:?}", validation_time);
        }
        
        if failed_accounts > 0 {
            return Err(anyhow::anyhow!("Migration validation failed: {} accounts could not be validated", 
                                       failed_accounts));
        }
        
        Ok(())
    }

    /// Cleanup old shard data and mappings
    async fn cleanup_old_shards(&self) -> Result<()> {
        let start_time = Instant::now();
        let mut cleanup_count = 0u64;
        
        if self.config.enable_detailed_logging {
            println!("Starting cleanup of old shard data...");
        }
        
        // Remove old mappings for migrated accounts
        for old_shard_id in 0..self.old_config.normalized_num_shards() {
            let source_cf_name = Self::get_shard_cf_name(old_shard_id as u16)?;
            let source_cf = self.storage.cf(&source_cf_name)?;
            
            let iter = self.storage.iterator_cf(&source_cf)?;
            for item in iter {
                if let Ok((addr, _)) = item {
                    // Check for cancellation
                    if self.is_cancelled() {
                        return Err(anyhow::anyhow!("Migration cancelled during cleanup"));
                    }
                    
                    let old_shard = self.old_config.shard_for_address(&addr);
                    let new_shard = self.new_config.shard_for_address(&addr);
                    
                    if old_shard != new_shard {
                        // Remove old mapping
                        self.storage.delete_cf(CF_ACCOUNT_TO_SHARD, &addr)?;
                        cleanup_count += 1;
                    }
                }
            }
        }
        
        let cleanup_time = start_time.elapsed();
        
        // Update statistics
        {
            let mut state = self.state.lock().unwrap();
            state.statistics.cleanup_time_ms = cleanup_time.as_millis() as u64;
            
            let total_time = state.progress.start_time.elapsed();
            state.statistics.total_migration_time_ms = total_time.as_millis() as u64;
        }
        
        if self.config.enable_detailed_logging {
            println!("Cleanup completed:");
            println!("  Old mappings removed: {}", cleanup_count);
            println!("  Cleanup time: {:?}", cleanup_time);
        }
        
        Ok(())
    }

    /// Get shard column family name
    fn get_shard_cf_name(shard_id: u16) -> Result<&'static str> {
        let idx = shard_id as usize;
        if idx >= CF_ACCOUNTS_SHARDS.len() {
            anyhow::bail!("shard id {} out of bounds (max {})", shard_id, CF_ACCOUNTS_SHARDS.len() - 1);
        }
        Ok(CF_ACCOUNTS_SHARDS[idx])
    }

    /// Set migration phase
    fn set_phase(&self, phase: MigrationPhase) {
        let mut state = self.state.lock().unwrap();
        state.phase = phase;
        state.progress.phase = phase;
        self.progress_notify.notify_one();
    }

    /// Check if migration is cancelled
    fn is_cancelled(&self) -> bool {
        self.state.lock().unwrap().is_cancelled
    }

    /// Set migration as failed
    fn set_failed(&self, error: String) {
        let mut state = self.state.lock().unwrap();
        state.phase = MigrationPhase::Failed;
        state.progress.phase = MigrationPhase::Failed;
        state.progress.error_message = Some(error);
        self.progress_notify.notify_one();
    }

    /// Cancel migration
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.is_cancelled = true;
    }

    /// Get current migration progress
    pub fn get_progress(&self) -> MigrationProgress {
        let state = self.state.lock().unwrap();
        state.progress.clone()
    }

    /// Get migration statistics
    pub fn get_statistics(&self) -> MigrationStatistics {
        let state = self.state.lock().unwrap();
        state.statistics.clone()
    }
}

/// Migration plan with account mappings
#[derive(Debug)]
struct MigrationPlan {
    migration_map: HashMap<Vec<u8>, (ShardId, ShardId)>,
    total_accounts: u64,
    preparation_time_ms: u64,
}

/// Migration handle for monitoring and control
pub struct MigrationHandle {
    state: Arc<Mutex<MigrationState>>,
    progress_notify: Arc<Notify>,
}

impl MigrationHandle {
    fn new(state: Arc<Mutex<MigrationState>>, progress_notify: Arc<Notify>) -> Self {
        Self {
            state,
            progress_notify,
        }
    }

    /// Wait for migration completion
    pub async fn wait_completion(&self) -> Result<MigrationProgress> {
        loop {
            {
                let state = self.state.lock().unwrap();
                match state.phase {
                    MigrationPhase::Completed => {
                        return Ok(state.progress.clone());
                    },
                    MigrationPhase::Failed => {
                        return Err(anyhow::anyhow!(
                            "Migration failed: {}",
                            state.progress.error_message.as_deref().unwrap_or("Unknown error")
                        ));
                    },
                    _ => {}
                }
            }
            
            // Wait for progress notification
            self.progress_notify.notified().await;
        }
    }

    /// Get current progress
    pub fn get_progress(&self) -> MigrationProgress {
        let state = self.state.lock().unwrap();
        state.progress.clone()
    }

    /// Get current phase
    pub fn get_phase(&self) -> MigrationPhase {
        let state = self.state.lock().unwrap();
        state.phase
    }

    /// Cancel migration
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.is_cancelled = true;
    }

    /// Check if migration is completed
    pub fn is_completed(&self) -> bool {
        let state = self.state.lock().unwrap();
        matches!(state.phase, MigrationPhase::Completed | MigrationPhase::Failed)
    }

    /// Check if migration is successful
    pub fn is_successful(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.phase == MigrationPhase::Completed
    }
}

impl Clone for HotMigrationEngine {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            shard_aware_storage: Arc::clone(&self.shard_aware_storage),
            old_config: self.old_config.clone(),
            new_config: self.new_config.clone(),
            config: self.config.clone(),
            state: Arc::clone(&self.state),
            progress_notify: Arc::clone(&self.progress_notify),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::TempDir;
    use std::time::Duration;

    #[test]
    fn test_migration_engine_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Arc::new(Storage::<RocksDb>::new(temp_dir.path())?);
        
        let old_config = ShardingConfig::with_num_shards(4);
        let new_config = ShardingConfig::with_num_shards(8);
        let config = MigrationConfig::default();
        
        let engine = HotMigrationEngine::new(storage, old_config, new_config, config)?;
        
        // Verify initial state
        let progress = engine.get_progress();
        assert_eq!(progress.phase, MigrationPhase::Preparation);
        assert_eq!(progress.total_accounts, 0);
        
        Ok(())
    }

    #[test]
    fn test_migration_config() {
        let config = MigrationConfig::default();
        
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.operation_timeout, Duration::from_secs(30));
        assert_eq!(config.max_concurrent_operations, 4);
        assert!(config.enable_detailed_logging);
        assert_eq!(config.memory_limit_mb, 1024);
    }

    #[test]
    fn test_migration_progress() {
        let mut progress = MigrationProgress::default();
        
        assert_eq!(progress.phase, MigrationPhase::Preparation);
        assert_eq!(progress.total_accounts, 0);
        assert_eq!(progress.migrated_accounts, 0);
        
        progress.phase = MigrationPhase::DataCopy;
        progress.total_accounts = 1000;
        progress.migrated_accounts = 500;
        
        assert_eq!(progress.phase, MigrationPhase::DataCopy);
        assert_eq!(progress.total_accounts, 1000);
        assert_eq!(progress.migrated_accounts, 500);
    }

    #[tokio::test]
    async fn test_migration_handle() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = Arc::new(Storage::<RocksDb>::new(temp_dir.path())?);
        
        let old_config = ShardingConfig::with_num_shards(4);
        let new_config = ShardingConfig::with_num_shards(8);
        let config = MigrationConfig::default();
        
        let engine = HotMigrationEngine::new(storage, old_config, new_config, config)?;
        let handle = engine.start_migration().await?;
        
        // Test initial state
        assert!(!handle.is_completed());
        assert_eq!(handle.get_phase(), MigrationPhase::Preparation);
        
        // Cancel migration
        handle.cancel();
        
        Ok(())
    }
}
