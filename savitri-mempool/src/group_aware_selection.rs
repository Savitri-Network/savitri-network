//! Group-Aware Transaction Selection
//!
//! Implements transaction selection and prioritization for group-based

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use savitri_core::Transaction;

/// Group-aware selection configuration
#[derive(Debug, Clone)]
pub struct GroupAwareSelectionConfig {
    /// Enable group-based prioritization
    pub enable_group_prioritization: bool,
    /// Enable proposer priority queue
    pub enable_proposer_priority: bool,
    /// Maximum transactions per proposer
    pub max_transactions_per_proposer: usize,
    pub group_validation_timeout_secs: u64,
    /// Enable fee-based selection within groups
    pub enable_fee_based_selection: bool,
    /// Minimum fee multiplier for group members
    pub min_fee_multiplier: f64,
    /// Enable transaction sharding by group
    pub enable_transaction_sharding: bool,
}

impl Default for GroupAwareSelectionConfig {
    fn default() -> Self {
        Self {
            enable_group_prioritization: true,
            enable_proposer_priority: true,
            max_transactions_per_proposer: 100,
            group_validation_timeout_secs: 30,
            enable_fee_based_selection: true,
            min_fee_multiplier: 1.0,
            enable_transaction_sharding: false,
        }
    }
}

/// Group transaction pool
#[derive(Debug)]
pub struct GroupTransactionPool {
    group_id: String,
    proposer_id: String,
    transactions: VecDeque<PrioritizedTransaction>,
    max_size: usize,
    total_fees: u64,
    last_updated: u64,
}

/// Prioritized transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrioritizedTransaction {
    pub transaction: Transaction,
    pub priority: TransactionPriority,
    pub group_score: f64,
    pub proposer_bonus: f64,
    pub timestamp: u64,
}

/// Transaction priority levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransactionPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Selection statistics
#[derive(Debug, Clone, Default)]
pub struct SelectionStats {
    pub total_transactions_processed: u64,
    pub group_prioritized_transactions: u64,
    pub proposer_priority_transactions: u64,
    pub fee_based_selections: u64,
    pub average_selection_time_ms: f64,
    pub groups_served: usize,
    pub proposers_served: usize,
    pub total_fees_collected: u64,
}

/// Group-Aware Transaction Selector
pub struct GroupAwareTransactionSelector {
    config: GroupAwareSelectionConfig,
    group_pools: Arc<RwLock<HashMap<String, GroupTransactionPool>>>,
    proposer_pools: Arc<RwLock<HashMap<String, VecDeque<PrioritizedTransaction>>>>,
    global_pool: Arc<RwLock<VecDeque<PrioritizedTransaction>>>,
    stats: Arc<RwLock<SelectionStats>>,
}

impl GroupAwareTransactionSelector {
    pub fn new(config: GroupAwareSelectionConfig) -> Self {
        Self {
            config,
            group_pools: Arc::new(RwLock::new(HashMap::new())),
            proposer_pools: Arc::new(RwLock::new(HashMap::new())),
            global_pool: Arc::new(RwLock::new(VecDeque::new())),
            stats: Arc::new(RwLock::new(SelectionStats::default())),
        }
    }

    /// Add transaction to pool
    pub async fn add_transaction(
        &self,
        transaction: Transaction,
        group_id: Option<String>,
        proposer_id: Option<String>,
    ) -> Result<()> {
        let prioritized_tx = self
            .prioritize_transaction(transaction, group_id.clone(), proposer_id.clone())
            .await?;

        // Add to appropriate pool
        if let (Some(group), Some(proposer)) = (group_id.clone(), proposer_id.clone()) {
            self.add_to_group_pool(group, proposer, prioritized_tx)
                .await?;
        } else if let Some(proposer) = proposer_id {
            self.add_to_proposer_pool(proposer, prioritized_tx).await?;
        } else {
            self.add_to_global_pool(prioritized_tx).await?;
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_transactions_processed += 1;

        Ok(())
    }

    /// Prioritize transaction based on group and proposer context
    async fn prioritize_transaction(
        &self,
        transaction: Transaction,
        group_id: Option<String>,
        proposer_id: Option<String>,
    ) -> Result<PrioritizedTransaction> {
        let mut priority = TransactionPriority::Normal;
        let mut group_score = 1.0;
        let mut proposer_bonus = 1.0;

        // Group-based prioritization
        if self.config.enable_group_prioritization {
            if let Some(ref group) = group_id {
                // Calculate group score based on group health and activity
                group_score = self.calculate_group_score(group).await;

                // Higher priority for active groups
                if group_score > 0.8 {
                    priority = TransactionPriority::High;
                } else if group_score > 0.5 {
                    priority = TransactionPriority::Normal;
                } else {
                    priority = TransactionPriority::Low;
                }
            }
        }

        // Proposer-based prioritization
        if self.config.enable_proposer_priority {
            if let Some(ref proposer) = proposer_id {
                // Calculate proposer bonus based on proposer reputation
                proposer_bonus = self.calculate_proposer_bonus(proposer).await;

                // Boost priority for reputable proposers
                if proposer_bonus > 0.9 {
                    priority = std::cmp::max(priority, TransactionPriority::High);
                }
            }
        }

        // Fee-based adjustment
        if self.config.enable_fee_based_selection {
            let fee_multiplier = transaction.fee as f64 / 1000.0; // Base fee in wei
            if fee_multiplier > self.config.min_fee_multiplier {
                priority = std::cmp::max(priority, TransactionPriority::High);
            }
        }

        Ok(PrioritizedTransaction {
            transaction,
            priority,
            group_score,
            proposer_bonus,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        })
    }

    /// Calculate group score (simplified)
    async fn calculate_group_score(&self, group_id: &str) -> f64 {
        // Simplified group score calculation
        // In real implementation would query group manager
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        group_id.hash(&mut hasher);
        let hash = hasher.finish();

        (hash % 100) as f64 / 100.0
    }

    /// Calculate proposer bonus (simplified)
    async fn calculate_proposer_bonus(&self, proposer_id: &str) -> f64 {
        // Simplified proposer bonus calculation
        // In real implementation would query proposer reputation system
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        proposer_id.hash(&mut hasher);
        let hash = hasher.finish();

        0.5 + ((hash % 50) as f64 / 100.0)
    }

    /// Add transaction to group pool
    async fn add_to_group_pool(
        &self,
        group_id: String,
        proposer_id: String,
        transaction: PrioritizedTransaction,
    ) -> Result<()> {
        let mut pools = self.group_pools.write().await;

        let pool = pools
            .entry(group_id.clone())
            .or_insert_with(|| GroupTransactionPool {
                group_id: group_id.clone(),
                proposer_id: proposer_id.clone(),
                transactions: VecDeque::new(),
                max_size: self.config.max_transactions_per_proposer,
                total_fees: 0,
                last_updated: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            });

        // Add transaction if pool not full
        if pool.transactions.len() < pool.max_size {
            pool.transactions.push_back(transaction.clone());
            pool.total_fees += transaction.transaction.fee;
            pool.last_updated = transaction.timestamp;

            // Update stats
            let mut stats = self.stats.write().await;
            stats.group_prioritized_transactions += 1;
            stats.total_fees_collected += transaction.transaction.fee;

            debug!(
                group_id = %group_id,
                proposer_id = %proposer_id,
                tx_hash = %hex::encode(&transaction.transaction.hash()),
                "Added transaction to group pool"
            );
        } else {
            warn!(
                group_id = %group_id,
                proposer_id = %proposer_id,
                "Group pool full, dropping transaction"
            );
        }

        Ok(())
    }

    /// Add transaction to proposer pool
    async fn add_to_proposer_pool(
        &self,
        proposer_id: String,
        transaction: PrioritizedTransaction,
    ) -> Result<()> {
        let mut pools = self.proposer_pools.write().await;

        let pool = pools
            .entry(proposer_id.clone())
            .or_insert_with(VecDeque::new);

        // Limit pool size
        if pool.len() < self.config.max_transactions_per_proposer {
            pool.push_back(transaction.clone());

            // Update stats
            let mut stats = self.stats.write().await;
            stats.proposer_priority_transactions += 1;
            stats.total_fees_collected += transaction.transaction.fee;

            debug!(
                proposer_id = %proposer_id,
                tx_hash = %hex::encode(&transaction.transaction.hash()),
                "Added transaction to proposer pool"
            );
        } else {
            warn!(
                proposer_id = %proposer_id,
                "Proposer pool full, dropping transaction"
            );
        }

        Ok(())
    }

    /// Add transaction to global pool
    async fn add_to_global_pool(&self, transaction: PrioritizedTransaction) -> Result<()> {
        let mut pool = self.global_pool.write().await;

        // Limit global pool size
        if pool.len() < self.config.max_transactions_per_proposer * 10 {
            let tx_hash = transaction.transaction.hash();
            pool.push_back(transaction);

            debug!(
                tx_hash = %hex::encode(&tx_hash),
                "Added transaction to global pool"
            );
        } else {
            warn!("Global pool full, dropping transaction");
        }

        Ok(())
    }

    /// Select transactions for block proposal
    pub async fn select_transactions_for_block(
        &self,
        group_id: Option<String>,
        proposer_id: Option<String>,
        max_transactions: usize,
    ) -> Result<Vec<Transaction>> {
        let start_time = std::time::SystemTime::now();

        let selected_transactions: Vec<Transaction> =
            if let (Some(group), Some(proposer)) = (group_id.clone(), proposer_id.clone()) {
                self.select_from_group_pool(&group, &proposer, max_transactions)
                    .await?
            } else if let Some(proposer) = proposer_id {
                self.select_from_proposer_pool(&proposer, max_transactions)
                    .await?
            } else {
                self.select_from_global_pool(max_transactions).await?
            };

        // Update stats
        let duration = start_time.elapsed().unwrap().as_millis() as f64;
        let mut stats = self.stats.write().await;
        stats.average_selection_time_ms = (stats.average_selection_time_ms
            * (stats.total_transactions_processed as f64 - 1.0)
            + duration)
            / stats.total_transactions_processed as f64;

        info!(
            transactions_selected = selected_transactions.len(),
            selection_time_ms = duration,
            "Selected transactions for block"
        );

        Ok(selected_transactions)
    }

    /// Select from group pool
    async fn select_from_group_pool(
        &self,
        group_id: &str,
        proposer_id: &str,
        max_transactions: usize,
    ) -> Result<Vec<Transaction>> {
        let pools = self.group_pools.read().await;

        if let Some(pool) = pools.get(group_id) {
            let transactions: Vec<Transaction> = pool
                .transactions
                .iter()
                .take(max_transactions)
                .map(|pt| pt.transaction.clone())
                .collect();

            // Update stats
            let mut stats = self.stats.write().await;
            stats.groups_served = stats.groups_served.max(1);
            stats.proposers_served += 1;

            Ok(transactions)
        } else {
            warn!(group_id = %group_id, "Group pool not found");
            Ok(vec![])
        }
    }

    /// Select from proposer pool
    async fn select_from_proposer_pool(
        &self,
        proposer_id: &str,
        max_transactions: usize,
    ) -> Result<Vec<Transaction>> {
        let pools = self.proposer_pools.read().await;

        if let Some(pool) = pools.get(proposer_id) {
            let transactions: Vec<Transaction> = pool
                .iter()
                .take(max_transactions)
                .map(|pt| pt.transaction.clone())
                .collect();

            // Update stats
            let mut stats = self.stats.write().await;
            stats.proposers_served += 1;

            Ok(transactions)
        } else {
            warn!(proposer_id = %proposer_id, "Proposer pool not found");
            Ok(vec![])
        }
    }

    /// Select from global pool
    async fn select_from_global_pool(&self, max_transactions: usize) -> Result<Vec<Transaction>> {
        let pool = self.global_pool.read().await;

        let transactions: Vec<Transaction> = pool
            .iter()
            .take(max_transactions)
            .map(|pt| pt.transaction.clone())
            .collect();

        Ok(transactions)
    }

    /// Remove processed transactions
    pub async fn remove_processed_transactions(&self, transactions: &[Transaction]) -> Result<()> {
        let tx_hashes: Vec<[u8; 32]> = transactions.iter().map(|tx| tx.hash()).collect();

        // Remove from group pools
        {
            let mut pools = self.group_pools.write().await;
            for pool in pools.values_mut() {
                pool.transactions
                    .retain(|pt| !tx_hashes.contains(&pt.transaction.hash()));
                pool.total_fees = pool.transactions.iter().map(|pt| pt.transaction.fee).sum();
            }
        }

        // Remove from proposer pools
        {
            let mut pools = self.proposer_pools.write().await;
            for pool in pools.values_mut() {
                pool.retain(|pt| !tx_hashes.contains(&pt.transaction.hash()));
            }
        }

        // Remove from global pool
        {
            let mut pool = self.global_pool.write().await;
            pool.retain(|pt| !tx_hashes.contains(&pt.transaction.hash()));
        }

        debug!(
            transactions_count = transactions.len(),
            "Removed processed transactions from pools"
        );

        Ok(())
    }

    /// Get selection statistics
    pub async fn get_stats(&self) -> SelectionStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Get pool sizes
    pub async fn get_pool_sizes(&self) -> (usize, usize, usize) {
        let group_pools = self.group_pools.read().await;
        let proposer_pools = self.proposer_pools.read().await;
        let global_pool = self.global_pool.read().await;

        let group_size: usize = group_pools.values().map(|p| p.transactions.len()).sum();
        let proposer_size: usize = proposer_pools.values().map(|p| p.len()).sum();
        let global_size = global_pool.len();

        (group_size, proposer_size, global_size)
    }

    /// Cleanup old transactions
    pub async fn cleanup_old_transactions(&self, timeout_secs: u64) -> Result<usize> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut removed_count = 0;

        // Cleanup group pools
        {
            let mut pools = self.group_pools.write().await;
            for pool in pools.values_mut() {
                let original_len = pool.transactions.len();
                pool.transactions
                    .retain(|pt| (current_time - pt.timestamp) < timeout_secs);
                removed_count += original_len - pool.transactions.len();
            }
        }

        // Cleanup proposer pools
        {
            let mut pools = self.proposer_pools.write().await;
            for pool in pools.values_mut() {
                let original_len = pool.len();
                pool.retain(|pt| (current_time - pt.timestamp) < timeout_secs);
                removed_count += original_len - pool.len();
            }
        }

        // Cleanup global pool
        {
            let mut pool = self.global_pool.write().await;
            let original_len = pool.len();
            pool.retain(|pt| (current_time - pt.timestamp) < timeout_secs);
            removed_count += original_len - pool.len();
        }

        if removed_count > 0 {
            info!(removed_count = removed_count, "Cleaned up old transactions");
        }

        Ok(removed_count)
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting group-aware transaction selector");

        // Start cleanup task
        let selector = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;
                if let Err(e) = selector.cleanup_old_transactions(300).await {
                    error!("Failed to cleanup old transactions: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Stop the selector
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping group-aware transaction selector");
        Ok(())
    }
}

impl Clone for GroupAwareTransactionSelector {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            group_pools: self.group_pools.clone(),
            proposer_pools: self.proposer_pools.clone(),
            global_pool: self.global_pool.clone(),
            stats: self.stats.clone(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════
//
// Routes transactions from the same sender to the same group, minimizing
// cross-group AccountNonce conflicts in the DAG conflict set model.
//
// Approach: consistent hashing by sender address → deterministic group
// assignment that survives group membership changes gracefully.

use std::hash::{Hash, Hasher};

/// Sender affinity router for conflict-aware group assignment.
///
/// When multiple groups execute blocks in parallel, transactions from the
/// same sender should be routed to the same group to avoid AccountNonce
/// conflicts. This router uses consistent hashing to deterministically
/// map sender addresses to groups.
///
/// # Conflict Reduction
///
/// Without affinity: 5 groups × 250 senders = every sender's TXs can
/// appear in any group → high AccountNonce conflict rate.
///
/// With affinity: sender X always goes to group G → AccountNonce(X, N)
/// conflict only possible within group G → near-zero cross-group conflicts.
#[derive(Debug, Clone)]
pub struct SenderAffinityRouter {
    /// Known active group IDs, sorted for deterministic assignment
    active_groups: Vec<String>,
    /// Sticky sender→group overrides (for hot-account lanes)
    sticky_assignments: HashMap<[u8; 32], String>,
    /// Whether affinity routing is enabled
    enabled: bool,
}

impl SenderAffinityRouter {
    pub fn new() -> Self {
        Self {
            active_groups: Vec::new(),
            sticky_assignments: HashMap::new(),
            enabled: true,
        }
    }

    /// Update the set of active groups (called when group membership changes).
    pub fn update_groups(&mut self, group_ids: Vec<String>) {
        self.active_groups = group_ids;
        self.active_groups.sort(); // deterministic ordering
        info!(
            groups = self.active_groups.len(),
            "Sender affinity router: updated active groups"
        );
    }

    /// Route a transaction sender to a preferred group using consistent hashing.
    ///
    /// Returns `None` if no groups are active or affinity is disabled.
    pub fn route_sender(&self, sender_address: &[u8]) -> Option<String> {
        if !self.enabled || self.active_groups.is_empty() {
            return None;
        }

        // Check sticky assignment first (hot-account lanes)
        if sender_address.len() >= 32 {
            let mut key = [0u8; 32];
            key.copy_from_slice(&sender_address[..32]);
            if let Some(group) = self.sticky_assignments.get(&key) {
                if self.active_groups.contains(group) {
                    return Some(group.clone());
                }
            }
        }

        // Consistent hash: FNV-1a of sender address → group index
        let mut hash: u64 = 14695981039346656037; // FNV offset basis
        for byte in sender_address {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211); // FNV prime
        }
        let idx = (hash as usize) % self.active_groups.len();
        Some(self.active_groups[idx].clone())
    }

    /// Assign a sender to a specific group (sticky override for hot accounts).
    pub fn set_sticky_assignment(&mut self, sender: [u8; 32], group_id: String) {
        info!(
            sender = %hex::encode(&sender[..8]),
            group = %group_id,
            "Set sticky sender affinity"
        );
        self.sticky_assignments.insert(sender, group_id);
    }

    /// Remove a sticky assignment.
    pub fn remove_sticky_assignment(&mut self, sender: &[u8; 32]) {
        self.sticky_assignments.remove(sender);
    }

    /// Check if a sender has affinity to a specific group.
    pub fn has_affinity(&self, sender_address: &[u8], group_id: &str) -> bool {
        self.route_sender(sender_address)
            .map(|g| g == group_id)
            .unwrap_or(false)
    }

    /// Get statistics about routing distribution.
    pub fn get_distribution(&self) -> HashMap<String, usize> {
        let mut dist = HashMap::new();
        for group in &self.active_groups {
            dist.insert(group.clone(), 0);
        }
        for group in self.sticky_assignments.values() {
            *dist.entry(group.clone()).or_insert(0) += 1;
        }
        dist
    }

    /// Enable or disable affinity routing.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }
}

impl Default for SenderAffinityRouter {
    fn default() -> Self {
        Self::new()
    }
}
