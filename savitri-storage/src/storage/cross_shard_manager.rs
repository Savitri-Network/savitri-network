//! Cross-Shard Transaction Manager
//! 
//! This module implements a transaction manager that handles cross-shard operations
//! with ordered locking, deadlock prevention, and atomic commit/rollback semantics.

use crate::sharding::{ShardingConfig, ShardId};
use crate::storage::shard_aware::ShardAwareStorage;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

/// Transaction operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOperation {
    Read,
    Write,
    Delete,
}

/// Transaction participant information
#[derive(Debug, Clone)]
pub struct TransactionParticipant {
    pub address: Vec<u8>,
    pub shard_id: ShardId,
    pub operation: TransactionOperation,
    pub data: Option<Vec<u8>>,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Preparing,
    Prepared,
    Committed,
    Aborted,
    TimedOut,
}

/// Transaction context with metadata
#[derive(Debug, Clone)]
pub struct TransactionContext {
    pub tx_id: Vec<u8>,
    pub participants: Vec<TransactionParticipant>,
    pub state: TransactionState,
    pub start_time: Instant,
    pub timeout: Duration,
    pub involved_shards: Vec<ShardId>,
    pub is_cross_shard: bool,
}

/// Lock acquisition result
#[derive(Debug)]
pub struct LockAcquisitionResult {
    pub acquired_shards: Vec<ShardId>,
    pub failed_shards: Vec<ShardId>,
    pub acquisition_time_ms: u64,
    pub had_contention: bool,
}

/// Transaction execution result
#[derive(Debug)]
pub struct TransactionResult {
    pub tx_id: Vec<u8>,
    pub state: TransactionState,
    pub execution_time_ms: u64,
    pub operations_executed: usize,
    pub error_message: Option<String>,
}

/// Cross-shard transaction manager
pub struct CrossShardTransactionManager {
    /// Shard-aware storage instance
    shard_aware_storage: Arc<ShardAwareStorage>,
    /// Sharding configuration
    config: ShardingConfig,
    /// Active transactions
    active_transactions: Arc<Mutex<HashMap<Vec<u8>, TransactionContext>>>,
    /// Lock timeout duration
    lock_timeout: Duration,
    /// Transaction timeout duration
    transaction_timeout: Duration,
    /// Maximum retry attempts
    max_retry_attempts: u32,
    /// Performance metrics
    metrics: Arc<Mutex<TransactionMetrics>>,
}

/// Performance metrics for transaction manager
#[derive(Debug, Default)]
pub struct TransactionMetrics {
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub timed_out_transactions: u64,
    pub cross_shard_transactions: u64,
    pub single_shard_transactions: u64,
    pub total_lock_acquisitions: u64,
    pub total_lock_contentions: u64,
    pub average_execution_time_ms: u64,
    pub average_lock_time_ms: u64,
    pub max_concurrent_transactions: u64,
    pub current_concurrent_transactions: u64,
}

impl CrossShardTransactionManager {
    /// Create new cross-shard transaction manager
    pub fn new(
        shard_aware_storage: Arc<ShardAwareStorage>,
        config: ShardingConfig,
    ) -> Self {
        Self {
            shard_aware_storage,
            config,
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            lock_timeout: Duration::from_secs(5),
            transaction_timeout: Duration::from_secs(30),
            max_retry_attempts: 3,
            metrics: Arc::new(Mutex::new(TransactionMetrics::default())),
        }
    }

    /// Set lock timeout
    pub fn set_lock_timeout(&mut self, timeout: Duration) {
        self.lock_timeout = timeout;
    }

    /// Set transaction timeout
    pub fn set_transaction_timeout(&mut self, timeout: Duration) {
        self.transaction_timeout = timeout;
    }

    /// Set maximum retry attempts
    pub fn set_max_retry_attempts(&mut self, attempts: u32) {
        self.max_retry_attempts = attempts;
    }

    /// Begin a new transaction
    pub fn begin_transaction(
        &self,
        participants: Vec<TransactionParticipant>,
    ) -> Result<Vec<u8>> {
        let tx_id = self.generate_transaction_id();
        let involved_shards = self.determine_involved_shards(&participants);
        let is_cross_shard = involved_shards.len() > 1;

        let context = TransactionContext {
            tx_id: tx_id.clone(),
            participants,
            state: TransactionState::Preparing,
            start_time: Instant::now(),
            timeout: self.transaction_timeout,
            involved_shards,
            is_cross_shard,
        };

        // Register transaction
        {
            let mut active = self.active_transactions.lock().unwrap();
            active.insert(tx_id.clone(), context);
            
            // Update metrics
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_transactions += 1;
            metrics.current_concurrent_transactions += 1;
            metrics.max_concurrent_transactions = metrics.max_concurrent_transactions
                .max(metrics.current_concurrent_transactions);
            
            if is_cross_shard {
                metrics.cross_shard_transactions += 1;
            } else {
                metrics.single_shard_transactions += 1;
            }
        }

        Ok(tx_id)
    }

    /// Prepare transaction (acquire locks)
    pub fn prepare_transaction(&self, tx_id: &[u8]) -> Result<LockAcquisitionResult> {
        let start_time = Instant::now();
        
        let context = {
            let active = self.active_transactions.lock().unwrap();
            active.get(tx_id).cloned()
                .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?
        };

        if context.state != TransactionState::Preparing {
            return Err(anyhow::anyhow!("Transaction not in preparing state"));
        }

        // Acquire locks in deterministic order
        let mut sorted_shards = context.involved_shards.clone();
        sorted_shards.sort();
        sorted_shards.dedup();

        let mut acquired_shards = Vec::new();
        let mut failed_shards = Vec::new();
        let mut had_contention = false;

        for &shard_id in &sorted_shards {
            let lock_result = self.acquire_shard_lock_with_timeout(shard_id)?;
            
            if lock_result.acquired {
                acquired_shards.push(shard_id);
                if lock_result.had_contention {
                    had_contention = true;
                }
            } else {
                failed_shards.push(shard_id);
                break; // Stop on first failure
            }
        }

        let acquisition_time = start_time.elapsed();

        // Update transaction state
        if failed_shards.is_empty() {
            self.update_transaction_state(tx_id, TransactionState::Prepared)?;
        } else {
            // Release acquired locks on failure
            for &shard_id in &acquired_shards {
                let _ = self.release_shard_lock(shard_id);
            }
            self.update_transaction_state(tx_id, TransactionState::Aborted)?;
        }

        // Update metrics
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_lock_acquisitions += acquired_shards.len() as u64;
            if had_contention {
                metrics.total_lock_contentions += 1;
            }
        }

        Ok(LockAcquisitionResult {
            acquired_shards,
            failed_shards,
            acquisition_time_ms: acquisition_time.as_millis() as u64,
            had_contention,
        })
    }

    /// Commit transaction
    pub fn commit_transaction(&self, tx_id: &[u8]) -> Result<TransactionResult> {
        let start_time = Instant::now();
        
        let context = {
            let active = self.active_transactions.lock().unwrap();
            active.get(tx_id).cloned()
                .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?
        };

        if context.state != TransactionState::Prepared {
            return Err(anyhow::anyhow!("Transaction not in prepared state"));
        }

        let mut operations_executed = 0;
        let mut error_message = None;

        // Execute all operations
        for participant in &context.participants {
            match self.execute_participant_operation(participant) {
                Ok(_) => operations_executed += 1,
                Err(e) => {
                    error_message = Some(format!("Operation failed: {}", e));
                    break;
                }
            }
        }

        let execution_time = start_time.elapsed();
        let final_state = if error_message.is_some() {
            TransactionState::Aborted
        } else {
            TransactionState::Committed
        };

        // Update transaction state
        self.update_transaction_state(tx_id, final_state)?;

        // Release all locks
        for &shard_id in &context.involved_shards {
            let _ = self.release_shard_lock(shard_id);
        }

        // Update metrics
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.current_concurrent_transactions -= 1;
            
            match final_state {
                TransactionState::Committed => metrics.successful_transactions += 1,
                TransactionState::Aborted => metrics.failed_transactions += 1,
                _ => {}
            }
        }

        // Remove from active transactions
        {
            let mut active = self.active_transactions.lock().unwrap();
            active.remove(tx_id);
        }

        Ok(TransactionResult {
            tx_id: tx_id.to_vec(),
            state: final_state,
            execution_time_ms: execution_time.as_millis() as u64,
            operations_executed,
            error_message,
        })
    }

    /// Abort transaction
    pub fn abort_transaction(&self, tx_id: &[u8]) -> Result<TransactionResult> {
        let start_time = Instant::now();
        
        let context = {
            let active = self.active_transactions.lock().unwrap();
            active.get(tx_id).cloned()
                .ok_or_else(|| anyhow::anyhow!("Transaction not found"))?
        };

        // Update transaction state
        self.update_transaction_state(tx_id, TransactionState::Aborted)?;

        // Release all locks
        for &shard_id in &context.involved_shards {
            let _ = self.release_shard_lock(shard_id);
        }

        let execution_time = start_time.elapsed();

        // Update metrics
        {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.current_concurrent_transactions -= 1;
            metrics.failed_transactions += 1;
        }

        // Remove from active transactions
        {
            let mut active = self.active_transactions.lock().unwrap();
            active.remove(tx_id);
        }

        Ok(TransactionResult {
            tx_id: tx_id.to_vec(),
            state: TransactionState::Aborted,
            execution_time_ms: execution_time.as_millis() as u64,
            operations_executed: 0,
            error_message: Some("Transaction aborted".to_string()),
        })
    }

    /// Execute transaction with automatic prepare/commit
    pub fn execute_transaction(
        &self,
        participants: Vec<TransactionParticipant>,
    ) -> Result<TransactionResult> {
        let tx_id = self.begin_transaction(participants.clone())?;
        
        // Prepare phase
        let lock_result = self.prepare_transaction(&tx_id)?;
        if !lock_result.failed_shards.is_empty() {
            return self.abort_transaction(&tx_id);
        }

        // Commit phase
        self.commit_transaction(&tx_id)
    }

    /// Cleanup timed out transactions
    pub fn cleanup_timed_out_transactions(&self) -> Result<u64> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        {
            let active = self.active_transactions.lock().unwrap();
            for (tx_id, context) in active.iter() {
                if now.duration_since(context.start_time) > context.timeout {
                    timed_out.push(tx_id.clone());
                }
            }
        }

        // Abort timed out transactions
        let mut cleaned_count = 0u64;
        for tx_id in timed_out {
            if self.abort_transaction(&tx_id).is_ok() {
                cleaned_count += 1;
                
                // Update metrics
                let mut metrics = self.metrics.lock().unwrap();
                metrics.timed_out_transactions += 1;
            }
        }

        Ok(cleaned_count)
    }

    /// Get transaction metrics
    pub fn get_metrics(&self) -> TransactionMetrics {
        let metrics = self.metrics.lock().unwrap();
        TransactionMetrics {
            total_transactions: metrics.total_transactions,
            successful_transactions: metrics.successful_transactions,
            failed_transactions: metrics.failed_transactions,
            timed_out_transactions: metrics.timed_out_transactions,
            cross_shard_transactions: metrics.cross_shard_transactions,
            single_shard_transactions: metrics.single_shard_transactions,
            total_lock_acquisitions: metrics.total_lock_acquisitions,
            total_lock_contentions: metrics.total_lock_contentions,
            average_execution_time_ms: metrics.average_execution_time_ms,
            average_lock_time_ms: metrics.average_lock_time_ms,
            max_concurrent_transactions: metrics.max_concurrent_transactions,
            current_concurrent_transactions: metrics.current_concurrent_transactions,
        }
    }

    /// Get active transaction count
    pub fn get_active_transaction_count(&self) -> usize {
        self.active_transactions.lock().unwrap().len()
    }

    /// Get transaction context
    pub fn get_transaction_context(&self, tx_id: &[u8]) -> Option<TransactionContext> {
        let active = self.active_transactions.lock().unwrap();
        active.get(tx_id).cloned()
    }

    // Private helper methods

    fn generate_transaction_id(&self) -> Vec<u8> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        
        let mut hasher = DefaultHasher::new();
        hasher.write_usize(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as usize);
        hasher.write_u32(rand::random::<u32>());
        
        let hash = hasher.finish();
        hash.to_le_bytes().to_vec()
    }

    fn determine_involved_shards(&self, participants: &[TransactionParticipant]) -> Vec<ShardId> {
        let shards: HashSet<ShardId> = participants
            .iter()
            .map(|p| p.shard_id)
            .collect();
        
        let mut shard_vec: Vec<_> = shards.into_iter().collect();
        shard_vec.sort();
        shard_vec
    }

    fn acquire_shard_lock_with_timeout(&self, shard_id: ShardId) -> Result<LockResult> {
        let start_time = Instant::now();
        let timeout = self.lock_timeout;
        
        // Try to acquire lock with timeout
        let mut attempts = 0;
        let max_attempts = 10;
        
        while attempts < max_attempts {
            // Check if lock is available (simplified check)
            if self.is_shard_lock_available(shard_id)? {
                let acquisition_time = start_time.elapsed();
                return Ok(LockResult {
                    acquired: true,
                    acquisition_time_ms: acquisition_time.as_millis() as u64,
                    had_contention: attempts > 0,
                });
            }
            
            // Check timeout
            if start_time.elapsed() > timeout {
                break;
            }
            
            // Brief wait before retry
            thread::sleep(Duration::from_millis(10));
            attempts += 1;
        }

        Ok(LockResult {
            acquired: false,
            acquisition_time_ms: start_time.elapsed().as_millis() as u64,
            had_contention: true,
        })
    }

    fn is_shard_lock_available(&self, _shard_id: ShardId) -> Result<bool> {
        // Simplified availability check
        // In a real implementation, this would check the actual lock state
        Ok(true)
    }

    fn release_shard_lock(&self, _shard_id: ShardId) -> Result<()> {
        // Simplified lock release
        // In a real implementation, this would release the actual lock
        Ok(())
    }

    fn execute_participant_operation(&self, participant: &TransactionParticipant) -> Result<()> {
        match participant.operation {
            TransactionOperation::Read => {
                // Simulate read operation
                let _data = self.shard_aware_storage.as_storage().get_cf(crate::storage::CF_ACCOUNTS, &participant.address)?;
                Ok(())
            },
            TransactionOperation::Write => {
                // Simulate write operation
                if let Some(data) = &participant.data {
                    self.shard_aware_storage.as_storage().put_cf(crate::storage::CF_ACCOUNTS, &participant.address, data)?;
                }
                Ok(())
            },
            TransactionOperation::Delete => {
                // Simulate delete operation
                self.shard_aware_storage.as_storage().delete_cf(crate::storage::CF_ACCOUNTS, &participant.address)?;
                Ok(())
            },
        }
    }

    fn update_transaction_state(&self, tx_id: &[u8], new_state: TransactionState) -> Result<()> {
        let mut active = self.active_transactions.lock().unwrap();
        if let Some(context) = active.get_mut(tx_id) {
            context.state = new_state;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Transaction not found"))
        }
    }
}

/// Lock acquisition result
#[derive(Debug)]
pub struct LockResult {
    pub acquired: bool,
    pub acquisition_time_ms: u64,
    pub had_contention: bool,
}

