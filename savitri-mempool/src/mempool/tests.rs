//! Advanced Integration Tests for Mempool Logic
//! 
//! the atomic nonce resolution logic and complex mempool scenarios.

use crate::mempool::{Mempool, MempoolConfig, MempoolTx, TxClass, TxHandle};
use crate::mempool::admission::{AdmissionControl, AdmissionConfig};
use crate::executor::dispatcher::{ExecutionDispatcher, DispatcherConfig, MempoolState};
use crate::storage::Storage;
use crate::tx::{SignedTx, CallTx, hash_signed_tx_bytes};
use crate::crypto::{sign_message, verify_signature};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use tokio::time::timeout;
use anyhow::Result;

/// PendingQueue configuration for atomic nonce resolution tests
#[derive(Debug, Clone)]
pub struct PendingQueueConfig {
    /// Maximum number of pending slots
    pub max_slots: usize,
    /// Timeout for pending transactions
    pub pending_timeout: Duration,
    /// Maximum recursion depth for nonce resolution
    pub max_recursion_depth: usize,
}

impl Default for PendingQueueConfig {
    fn default() -> Self {
        Self {
            max_slots: 1000,
            pending_timeout: Duration::from_secs(30),
            max_recursion_depth: 10,
        }
    }
}

/// Pending transaction entry for nonce gap resolution
#[derive(Debug, Clone)]
pub struct PendingTx {
    /// The transaction waiting for nonce resolution
    pub tx: MempoolTx,
    /// Signed transaction data
    pub signed_tx: SignedTx,
    /// Expected nonce for this transaction
    pub expected_nonce: u64,
    /// When this transaction was parked
    pub parked_at: Instant,
    /// Recursion depth for resolution attempts
    pub recursion_depth: usize,
}

/// Atomic Nonce Resolution System
/// 
/// This system handles out-of-order transaction arrival by parking
/// transactions with nonce gaps and recursively resolving them when
/// the missing nonces become available.
#[derive(Debug)]
pub struct AtomicNonceResolver {
    /// Configuration for the pending queue
    config: PendingQueueConfig,
    /// Queue of pending transactions waiting for nonce resolution
    pending_queue: VecDeque<PendingTx>,
    /// Map of sender -> expected nonce for next transaction
    sender_expected_nonces: std::collections::HashMap<Vec<u8>, u64>,
    /// Statistics for the resolver
    stats: AtomicNonceResolverStats,
}

/// Statistics for the atomic nonce resolver
#[derive(Debug, Default, Clone)]
pub struct AtomicNonceResolverStats {
    /// Total transactions parked
    pub total_parked: u64,
    /// Total transactions resolved
    pub total_resolved: u64,
    /// Total transactions expired
    pub total_expired: u64,
    /// Current pending queue size
    pub pending_size: usize,
    /// Maximum recursion depth reached
    pub max_recursion_depth: usize,
    /// Total resolution time (microseconds)
    pub total_resolution_time_us: u64,
}

impl AtomicNonceResolver {
    /// Create a new atomic nonce resolver with default configuration
    pub fn new() -> Self {
        Self::with_config(PendingQueueConfig::default())
    }

    /// Create a new atomic nonce resolver with custom configuration
    pub fn with_config(config: PendingQueueConfig) -> Self {
        let max_slots = config.max_slots;
        Self {
            config,
            pending_queue: VecDeque::with_capacity(max_slots),
            sender_expected_nonces: std::collections::HashMap::new(),
            stats: AtomicNonceResolverStats::default(),
        }
    }

    /// Park a transaction that cannot be executed due to nonce gap
    pub fn park_transaction(&mut self, tx: MempoolTx, signed_tx: SignedTx, expected_nonce: u64) -> Result<()> {
        // Check if we have space in the pending queue
        if self.pending_queue.len() >= self.config.max_slots {
            return Err(anyhow::anyhow!("Pending queue is full"));
        }

        // Create pending transaction entry
        let pending_tx = PendingTx {
            tx: tx.clone(),
            signed_tx,
            expected_nonce,
            parked_at: Instant::now(),
            recursion_depth: 0,
        };

        // Add to pending queue
        self.pending_queue.push_back(pending_tx);
        self.stats.total_parked += 1;
        self.stats.pending_size = self.pending_queue.len();

        println!("🔄 Parked transaction: nonce={}, sender={:x?}", expected_nonce, &tx.sender_id.to_le_bytes()[..8]);
        
        Ok(())
    }

    /// Attempt to resolve pending transactions after executing a transaction
    /// This implements the recursive resolution logic
    pub fn resolve_pending_transactions(&mut self, sender_address: &[u8], mut current_nonce: u64) -> Result<Vec<(MempoolTx, SignedTx)>> {
        let mut resolved_transactions = Vec::new();
        let resolution_start = Instant::now();

        // Update expected nonce for this sender
        self.sender_expected_nonces.insert(sender_address.to_vec(), current_nonce);

        // Recursive resolution: keep trying to resolve pending transactions
        // until no more can be resolved in this atomic unit
        let mut resolved_in_this_round = true;
        let mut recursion_depth = 0;

        while resolved_in_this_round && recursion_depth < self.config.max_recursion_depth {
            resolved_in_this_round = false;
            recursion_depth += 1;

            // Find transactions that can now be executed
            let mut to_remove = Vec::new();

            for (index, pending_tx) in self.pending_queue.iter().enumerate() {
                // Check if this transaction's nonce is now valid
                if pending_tx.expected_nonce == current_nonce {
                    to_remove.push(index);
                    resolved_in_this_round = true;

                    // Add to resolved transactions
                    resolved_transactions.push((pending_tx.tx.clone(), pending_tx.signed_tx.clone()));
                    
                    // Update current nonce for next transaction in sequence
                    let next_nonce = current_nonce + 1;
                    current_nonce = next_nonce;

                    println!("✅ Resolved pending transaction: nonce={}, depth={}", 
                            pending_tx.expected_nonce, recursion_depth);
                }
            }

            // Remove resolved transactions from pending queue (in reverse order)
            for &index in to_remove.iter().rev() {
                self.pending_queue.remove(index);
                self.stats.total_resolved += 1;
            }

            // Update max recursion depth
            if recursion_depth > self.stats.max_recursion_depth {
                self.stats.max_recursion_depth = recursion_depth;
            }
        }

        // Update statistics
        self.stats.pending_size = self.pending_queue.len();
        self.stats.total_resolution_time_us += resolution_start.elapsed().as_micros() as u64;

        println!("🎯 Resolution complete: {} transactions resolved, depth={}, remaining pending: {}", 
                resolved_transactions.len(), recursion_depth, self.pending_queue.len());

        Ok(resolved_transactions)
    }

    /// Clean up expired pending transactions
    pub fn cleanup_expired(&mut self) -> usize {
        let now = Instant::now();
        let initial_size = self.pending_queue.len();
        
        // Remove expired transactions
        self.pending_queue.retain(|pending_tx| {
            let is_valid = now.duration_since(pending_tx.parked_at) < self.config.pending_timeout;
            if !is_valid {
                self.stats.total_expired += 1;
                println!("⏰ Expired pending transaction: nonce={}", pending_tx.expected_nonce);
            }
            is_valid
        });

        let expired_count = initial_size - self.pending_queue.len();
        self.stats.pending_size = self.pending_queue.len();
        
        expired_count
    }

    /// Get current statistics
    pub fn get_stats(&self) -> &AtomicNonceResolverStats {
        &self.stats
    }

    /// Check if the pending queue is empty
    pub fn is_empty(&self) -> bool {
        self.pending_queue.is_empty()
    }

    /// Get the current pending queue size
    pub fn pending_size(&self) -> usize {
        self.pending_queue.len()
    }
}

/// Enhanced Execution Dispatcher with Atomic Nonce Resolution
#[derive(Debug)]
pub struct AtomicExecutionDispatcher {
    /// Base execution dispatcher
    base_dispatcher: ExecutionDispatcher,
    /// Atomic nonce resolver
    nonce_resolver: AtomicNonceResolver,
    /// Storage for account state
    storage: Arc<Mutex<Storage>>,
}

impl AtomicExecutionDispatcher {
    /// Create a new atomic execution dispatcher
    pub fn new(
        dispatcher_config: DispatcherConfig,
        pending_config: PendingQueueConfig,
        storage: Arc<Mutex<Storage>>,
    ) -> Result<Self> {
        let base_dispatcher = ExecutionDispatcher::new(dispatcher_config);
        let nonce_resolver = AtomicNonceResolver::with_config(pending_config);

        Ok(Self {
            base_dispatcher,
            nonce_resolver,
            storage,
        })
    }

    /// Execute transactions with atomic nonce resolution
    pub async fn execute_with_atomic_resolution(
        &mut self,
        mempool_txs: Vec<MempoolTx>,
        signed_txs: Vec<SignedTx>,
    ) -> Result<(Vec<MempoolTx>, Vec<SignedTx>)> {
        // First, attempt to execute transactions normally
        let (mut executable_txs, mut executable_signed) = self.base_dispatcher
            .schedule_transactions(mempool_txs.clone(), signed_txs.clone());

        // Check for nonce gaps and park transactions that cannot be executed
        let mut parked_count = 0;
        for (i, (mempool_tx, signed_tx)) in mempool_txs.iter().zip(signed_txs.iter()).enumerate() {
            // Get current account nonce from storage
            let current_nonce = {
                let storage = self.storage.lock().unwrap();
                if let Ok(Some(bytes)) = storage.get_account(&mempool_tx.sender_id.to_le_bytes().to_vec()) {
                    savitri_core::Account::decode(&bytes).map(|acc| acc.nonce).unwrap_or(0)
                } else {
                    0 // Default nonce for new accounts
                }
            };

            // If this transaction's nonce is higher than expected, park it
            if mempool_tx.nonce > current_nonce {
                // Check if this transaction was not already selected for execution
                if !executable_txs.iter().any(|tx| tx.tx_handle == mempool_tx.tx_handle) {
                    self.nonce_resolver.park_transaction(mempool_tx.clone(), signed_tx.clone(), mempool_tx.nonce)?;
                    parked_count += 1;
                }
            }
        }

        println!("📊 Execution phase: {} executed, {} parked", executable_txs.len(), parked_count);

        // Now execute the ready transactions
        if !executable_txs.is_empty() {
            // Simulate execution and update account state
            self.simulate_execution(&executable_txs).await?;

            // For each executed transaction, attempt to resolve pending transactions
            for tx in &executable_txs {
                // Get the sender address and new nonce after execution
                let sender_address = tx.sender_id.to_le_bytes().to_vec();
                let new_nonce = tx.nonce + 1; // Simplified: nonce increments by 1

                // Attempt recursive resolution
                let resolved = self.nonce_resolver.resolve_pending_transactions(&sender_address, new_nonce)?;
                
                if !resolved.is_empty() {
                    println!("🔄 Recursive resolution triggered: {} additional transactions", resolved.len());
                    
                    // Execute the resolved transactions immediately in the same atomic unit
                    let (resolved_mempool, resolved_signed) = resolved.into_iter().unzip();
                    
                    // Add to executable set
                    executable_txs.extend(resolved_mempool);
                    executable_signed.extend(resolved_signed);
                    
                    // Simulate execution of resolved transactions
                    self.simulate_execution(&executable_txs[executable_txs.len() - resolved.len()..]).await?;
                }
            }
        }

        // Clean up expired transactions
        let expired_count = self.nonce_resolver.cleanup_expired();
        if expired_count > 0 {
            println!("🧹 Cleaned up {} expired pending transactions", expired_count);
        }

        Ok((executable_txs, executable_signed))
    }

    /// Simulate transaction execution and update account state
    async fn simulate_execution(&mut self, txs: &[MempoolTx]) -> Result<()> {
        let storage = self.storage.lock().unwrap();
        
        for tx in txs {
            // This is a simplified execution simulation
            // In production, you'd execute the actual transaction logic
            
            println!("⚡ Executing transaction: nonce={}, sender={:x?}", 
                    tx.nonce, &tx.sender_id.to_le_bytes()[..8]);
            
            // Update account nonce (simplified)
            // In production, this would be part of the transaction execution
        }
        
        Ok(())
    }

    /// Get nonce resolver statistics
    pub fn get_resolver_stats(&self) -> &AtomicNonceResolverStats {
        self.nonce_resolver.get_stats()
    }

    /// Check if all pending transactions are resolved
    pub fn is_fully_resolved(&self) -> bool {
        self.nonce_resolver.is_empty()
    }
}

