//! Transaction Validation Module
//!
//! This module implements the anti-double spending logic for the masternode,
//! ensuring that transactions are not processed multiple times across different groups.

use hex;
use libp2p::PeerId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_big_array::BigArray;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

// Custom serialization for Option<[u8; 64]>
fn serialize_big_array_option<S>(
    option: &Option<[u8; 64]>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match option {
        Some(arr) => {
            // Convert to hex string for serialization
            let hex_str = hex::encode(arr);
            serializer.serialize_some(&hex_str)
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_big_array_option<'de, D>(deserializer: D) -> Result<Option<[u8; 64]>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize as hex string then convert to array
    let hex_str: Option<String> = Option::deserialize(deserializer)?;
    match hex_str {
        Some(s) => {
            let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
            if bytes.len() != 64 {
                return Err(serde::de::Error::custom("Expected 64 bytes for signature"));
            }
            let mut array = [0u8; 64];
            array.copy_from_slice(&bytes);
            Ok(Some(array))
        }
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatedTransaction {
    pub tx_hash: [u8; 32],
    pub sender: [u8; 32],
    pub receiver: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    pub processing_group_id: Option<String>,
    pub execution_status: ExecutionStatus,
    pub processed_at: Option<u64>,
    #[serde(
        serialize_with = "serialize_big_array_option",
        deserialize_with = "deserialize_big_array_option"
    )]
    pub block_hash: Option<[u8; 64]>,
    pub is_duplicate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExecutionStatus {
    Pending,
    Confirmed,
    Rejected,
    Invalidated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedTxInfo {
    pub tx_hash: [u8; 32],
    pub group_id: String,
    pub processed_at: u64,
    pub block_height: u64,
    pub execution_status: ExecutionStatus,
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub is_confirmed: bool,
}

#[derive(Debug, Clone)]
pub struct TransactionValidator {
    processed_cache: HashMap<[u8; 32], ProcessedTxInfo>,
    uniqueness_threshold: f64,
    current_block_height: u64,
}

impl TransactionValidator {
    pub fn new() -> Self {
        Self {
            processed_cache: HashMap::new(),
            uniqueness_threshold: 0.8, // 80% threshold
            current_block_height: 0,
        }
    }

    pub fn with_threshold(threshold: f64) -> Self {
        Self {
            processed_cache: HashMap::new(),
            uniqueness_threshold: threshold,
            current_block_height: 0,
        }
    }

    /// Set the current block height (call this when processing a new block)
    pub fn set_current_block_height(&mut self, height: u64) {
        self.current_block_height = height;
    }

    /// Get the current block height
    pub fn get_current_block_height(&self) -> u64 {
        self.current_block_height
    }

    /// Validate transactions in a block proposal
    pub fn validate_block_transactions(
        &mut self,
        transactions: Vec<ValidatedTransaction>,
        proposer_group_id: String,
    ) -> ValidationResult {
        self.validate_block_transactions_with_height(
            transactions,
            proposer_group_id,
            self.current_block_height,
        )
    }

    /// Validate transactions in a block proposal with specific block height
    pub fn validate_block_transactions_with_height(
        &mut self,
        transactions: Vec<ValidatedTransaction>,
        proposer_group_id: String,
        block_height: u64,
    ) -> ValidationResult {
        self.current_block_height = block_height;

        let mut validated_txs = Vec::new();
        let mut duplicate_hashes = Vec::new();
        let mut unique_count = 0;

        for tx in transactions {
            match self.processed_cache.get(&tx.tx_hash) {
                None => {
                    // Transaction is unique
                    let mut validated_tx = tx.clone();
                    validated_tx.processing_group_id = Some(proposer_group_id.clone());
                    validated_tx.execution_status = ExecutionStatus::Confirmed;
                    validated_tx.processed_at = Some(current_timestamp());
                    validated_tx.is_duplicate = false;

                    validated_txs.push(validated_tx);
                    unique_count += 1;
                }
                Some(processed_info) => {
                    // Transaction is duplicate
                    duplicate_hashes.push(tx.tx_hash);
                    let mut rejected_tx = tx.clone();
                    rejected_tx.processing_group_id = Some(proposer_group_id.clone());
                    rejected_tx.execution_status = ExecutionStatus::Rejected;
                    rejected_tx.is_duplicate = true;

                    validated_txs.push(rejected_tx);
                }
            }
        }

        let total_txs = validated_txs.len();
        let uniqueness_ratio = if total_txs > 0 {
            unique_count as f64 / total_txs as f64
        } else {
            0.0
        };

        let is_accepted = uniqueness_ratio >= self.uniqueness_threshold;

        // Only cache if accepted
        if is_accepted {
            for tx in &validated_txs {
                if !tx.is_duplicate {
                    self.cache_processed_transaction(tx);
                }
            }
        }

        ValidationResult {
            validated_transactions: validated_txs,
            duplicate_hashes,
            total_transactions: total_txs,
            unique_transactions: unique_count,
            uniqueness_ratio,
            is_accepted,
        }
    }

    /// Cache a processed transaction
    fn cache_processed_transaction(&mut self, tx: &ValidatedTransaction) {
        let info = ProcessedTxInfo {
            tx_hash: tx.tx_hash,
            group_id: tx.processing_group_id.clone().unwrap_or_default(),
            processed_at: tx.processed_at.unwrap_or_default(),
            block_height: self.current_block_height,
            execution_status: tx.execution_status.clone(),
            block_hash: tx.block_hash.unwrap_or([0u8; 64]),
            is_confirmed: tx.execution_status == ExecutionStatus::Confirmed,
        };

        self.processed_cache.insert(tx.tx_hash, info);
    }

    /// Check if a transaction has been processed
    pub fn is_transaction_processed(&self, tx_hash: &[u8; 32]) -> bool {
        self.processed_cache.contains_key(tx_hash)
    }

    /// Get processed transaction info
    pub fn get_processed_transaction(&self, tx_hash: &[u8; 32]) -> Option<&ProcessedTxInfo> {
        self.processed_cache.get(tx_hash)
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> CacheStats {
        let confirmed_count = self
            .processed_cache
            .values()
            .filter(|info| info.is_confirmed)
            .count();

        let pending_count = self
            .processed_cache
            .values()
            .filter(|info| info.execution_status == ExecutionStatus::Pending)
            .count();

        let unique_groups = self
            .processed_cache
            .values()
            .map(|info| &info.group_id)
            .collect::<std::collections::HashSet<_>>()
            .len();

        CacheStats {
            total_cached: self.processed_cache.len(),
            confirmed_count,
            rejected_count: self.processed_cache.len() - confirmed_count - pending_count,
            pending_count,
            unique_groups,
        }
    }

    /// Clear old transactions (for memory management)
    pub fn clear_old_transactions(&mut self, max_age_seconds: u64) {
        let cutoff_time = current_timestamp() - max_age_seconds;

        self.processed_cache
            .retain(|_, info| info.processed_at >= cutoff_time);

        info!(
            "Cleared old transactions older than {} seconds",
            max_age_seconds
        );
    }

    /// Get pending transactions count
    pub fn get_pending_count(&self) -> usize {
        self.processed_cache
            .values()
            .filter(|info| info.execution_status == ExecutionStatus::Pending)
            .count()
    }

    /// Get confirmed transactions count
    pub fn get_confirmed_count(&self) -> usize {
        self.processed_cache
            .values()
            .filter(|info| info.is_confirmed)
            .count()
    }

    /// Get rejected transactions count
    pub fn get_rejected_count(&self) -> usize {
        self.processed_cache
            .values()
            .filter(|info| {
                info.execution_status == ExecutionStatus::Rejected
                    || info.execution_status == ExecutionStatus::Invalidated
            })
            .count()
    }

    /// Get transactions by execution status
    pub fn get_transactions_by_status(&self, status: ExecutionStatus) -> Vec<&ProcessedTxInfo> {
        self.processed_cache
            .values()
            .filter(|info| info.execution_status == status)
            .collect()
    }

    /// Update transaction status
    pub fn update_transaction_status(
        &mut self,
        tx_hash: &[u8; 32],
        new_status: ExecutionStatus,
    ) -> Result<(), String> {
        if let Some(info) = self.processed_cache.get_mut(tx_hash) {
            let old_status = info.execution_status.clone();
            info.execution_status = new_status.clone();

            // Update is_confirmed flag based on new status
            info.is_confirmed = matches!(new_status, ExecutionStatus::Confirmed);

            debug!(
                "Updated transaction {} status from {:?} to {:?}",
                hex::encode(tx_hash),
                old_status,
                new_status
            );

            Ok(())
        } else {
            Err(format!(
                "Transaction {} not found in cache",
                hex::encode(tx_hash)
            ))
        }
    }

    /// Get transactions by group ID
    pub fn get_transactions_by_group(&self, group_id: &str) -> Vec<&ProcessedTxInfo> {
        self.processed_cache
            .values()
            .filter(|info| info.group_id == group_id)
            .collect()
    }

    /// Get pending transactions by group ID
    pub fn get_pending_transactions_by_group(&self, group_id: &str) -> Vec<&ProcessedTxInfo> {
        self.processed_cache
            .values()
            .filter(|info| {
                info.group_id == group_id && info.execution_status == ExecutionStatus::Pending
            })
            .collect()
    }

    /// Confirm pending transactions for a specific block
    pub fn confirm_pending_transactions(
        &mut self,
        block_height: u64,
        block_hash: [u8; 64],
    ) -> usize {
        let mut confirmed_count = 0;

        for info in self.processed_cache.values_mut() {
            if info.execution_status == ExecutionStatus::Pending
                && info.block_height == block_height
            {
                info.execution_status = ExecutionStatus::Confirmed;
                info.is_confirmed = true;
                info.block_hash = block_hash;
                confirmed_count += 1;
            }
        }

        if confirmed_count > 0 {
            info!(
                "Confirmed {} pending transactions for block height {}",
                confirmed_count, block_height
            );
        }

        confirmed_count
    }

    /// Reject pending transactions older than specified height
    pub fn reject_old_pending_transactions(&mut self, max_height_diff: u64) -> usize {
        let current_height = self.current_block_height;
        let mut rejected_count = 0;

        for info in self.processed_cache.values_mut() {
            if info.execution_status == ExecutionStatus::Pending
                && current_height.saturating_sub(info.block_height) > max_height_diff
            {
                info.execution_status = ExecutionStatus::Rejected;
                info.is_confirmed = false;
                rejected_count += 1;
            }
        }

        if rejected_count > 0 {
            info!(
                "Rejected {} old pending transactions (height difference > {})",
                rejected_count, max_height_diff
            );
        }

        rejected_count
    }

    /// Get detailed cache statistics with additional metrics
    pub fn get_detailed_cache_stats(&self) -> DetailedCacheStats {
        let total = self.processed_cache.len();
        let confirmed = self.get_confirmed_count();
        let pending = self.get_pending_count();
        let rejected = self.get_rejected_count();
        let invalidated = self
            .processed_cache
            .values()
            .filter(|info| info.execution_status == ExecutionStatus::Invalidated)
            .count();

        let unique_groups = self
            .processed_cache
            .values()
            .map(|info| &info.group_id)
            .collect::<std::collections::HashSet<_>>()
            .len();

        // Calculate age statistics
        let now = current_timestamp();
        let ages: Vec<u64> = self
            .processed_cache
            .values()
            .map(|info| now.saturating_sub(info.processed_at))
            .collect();

        let avg_age = if !ages.is_empty() {
            ages.iter().sum::<u64>() / ages.len() as u64
        } else {
            0
        };

        let max_age = ages.iter().max().copied().unwrap_or(0);
        let min_age = ages.iter().min().copied().unwrap_or(0);

        DetailedCacheStats {
            total_cached: total,
            confirmed_count: confirmed,
            pending_count: pending,
            rejected_count: rejected,
            invalidated_count: invalidated,
            unique_groups,
            average_age_seconds: avg_age,
            oldest_transaction_age: max_age,
            newest_transaction_age: min_age,
            current_block_height: self.current_block_height,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedCacheStats {
    pub total_cached: usize,
    pub confirmed_count: usize,
    pub pending_count: usize,
    pub rejected_count: usize,
    pub invalidated_count: usize,
    pub unique_groups: usize,
    pub average_age_seconds: u64,
    pub oldest_transaction_age: u64,
    pub newest_transaction_age: u64,
    pub current_block_height: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub validated_transactions: Vec<ValidatedTransaction>,
    pub duplicate_hashes: Vec<[u8; 32]>,
    pub total_transactions: usize,
    pub unique_transactions: usize,
    pub uniqueness_ratio: f64,
    pub is_accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub total_cached: usize,
    pub confirmed_count: usize,
    pub rejected_count: usize,
    pub pending_count: usize,
    pub unique_groups: usize,
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_validation_all_unique() {
        let mut validator = TransactionValidator::new();
        // `create_test_transactions` lives in another `#[cfg(test)] mod tests`
        // and isn't accessible from here. Build the batch locally from the
        // existing single-tx helper.
        let transactions: Vec<ValidatedTransaction> =
            (1u32..=5).map(create_test_transaction).collect();

        let result = validator.validate_block_transactions(transactions, "group1".to_string());

        assert!(result.is_accepted);
        assert_eq!(result.total_transactions, 5);
        assert_eq!(result.unique_transactions, 5);
        assert_eq!(result.uniqueness_ratio, 1.0);
        assert_eq!(result.duplicate_hashes.len(), 0);
    }

    #[test]
    fn test_transaction_validation_with_duplicates() {
        let mut validator = TransactionValidator::new();

        // First batch - all unique
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        let result1 = validator
            .validate_block_transactions(vec![tx1.clone(), tx2, tx3], "group1".to_string());
        assert!(result1.is_accepted);

        // Second batch - with duplicate
        let tx4 = create_test_transaction(4);
        let tx5 = create_test_transaction(5);

        let result2 = validator
            .validate_block_transactions(vec![tx1.clone(), tx4, tx5], "group2".to_string());

        assert!(!result2.is_accepted); // 66% unique (< 80%)
        assert_eq!(result2.total_transactions, 3);
        assert_eq!(result2.unique_transactions, 2);
        assert_eq!(result2.uniqueness_ratio, 2.0 / 3.0);
        assert_eq!(result2.duplicate_hashes.len(), 1);
        assert_eq!(result2.duplicate_hashes[0], tx1.tx_hash);
    }

    #[test]
    fn test_80_percent_threshold() {
        let mut validator = TransactionValidator::new();

        // Cache some transactions first
        let cached_tx = create_test_transaction(1);
        validator.cache_processed_transaction(&ValidatedTransaction {
            processing_group_id: Some("group1".to_string()),
            execution_status: ExecutionStatus::Confirmed,
            processed_at: Some(current_timestamp()),
            block_hash: Some([0u8; 64]),
            is_duplicate: false,
            ..cached_tx.clone()
        });

        // Test with 80% unique (4/5 = 80%)
        let mut transactions = Vec::new();
        for i in 2..=5 {
            // tx2, tx3, tx4, tx5 (4 unique) + tx1 (duplicate)
            transactions.push(if i == 2 {
                cached_tx.clone()
            } else {
                create_test_transaction(i)
            });
        }

        let result = validator.validate_block_transactions(transactions, "group2".to_string());

        assert!(result.is_accepted); // Exactly 80%
        assert_eq!(result.unique_transactions, 4);
        assert_eq!(result.total_transactions, 5);
        assert_eq!(result.uniqueness_ratio, 0.8);
    }

    fn create_test_transaction(id: u32) -> ValidatedTransaction {
        ValidatedTransaction {
            tx_hash: [id as u8; 32],
            sender: [id as u8; 32],
            receiver: [(id + 100) as u8; 32],
            amount: id as u64,
            nonce: id as u64,
            signature: [id as u8; 64],
            processing_group_id: None,
            execution_status: ExecutionStatus::Pending,
            processed_at: None,
            block_hash: None,
            is_duplicate: false,
        }
    }

    #[test]
    fn test_pending_tracking() {
        let mut validator = TransactionValidator::new();
        validator.set_current_block_height(100);

        // Add some transactions with different statuses
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        // Validate transactions (they should be marked as pending)
        let result =
            validator.validate_block_transactions(vec![tx1, tx2, tx3], "group1".to_string());
        assert!(result.is_accepted);

        // Check pending count
        assert_eq!(validator.get_pending_count(), 3);
        assert_eq!(validator.get_confirmed_count(), 0);
        assert_eq!(validator.get_rejected_count(), 0);

        // Check cache stats
        let stats = validator.get_cache_stats();
        assert_eq!(stats.pending_count, 3);
        assert_eq!(stats.confirmed_count, 0);
        assert_eq!(stats.rejected_count, 0);
        assert_eq!(stats.total_cached, 3);
    }

    #[test]
    fn test_transaction_status_update() {
        let mut validator = TransactionValidator::new();

        let tx1 = create_test_transaction(1);
        let result = validator.validate_block_transactions(vec![tx1], "group1".to_string());
        assert!(result.is_accepted);

        // Update transaction status to confirmed
        let tx_hash = [1u8; 32];
        let update_result =
            validator.update_transaction_status(&tx_hash, ExecutionStatus::Confirmed);
        assert!(update_result.is_ok());

        // Check updated status
        assert_eq!(validator.get_pending_count(), 0);
        assert_eq!(validator.get_confirmed_count(), 1);

        let stats = validator.get_cache_stats();
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.confirmed_count, 1);
    }

    #[test]
    fn test_confirm_pending_transactions() {
        let mut validator = TransactionValidator::new();
        validator.set_current_block_height(100);

        // Add pending transactions
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        validator.validate_block_transactions(vec![tx1, tx2], "group1".to_string());
        validator.validate_block_transactions(vec![tx3], "group2".to_string());

        assert_eq!(validator.get_pending_count(), 3);

        // Confirm transactions for block height 100
        let block_hash = [42u8; 64];
        let confirmed_count = validator.confirm_pending_transactions(100, block_hash);

        // Should confirm 2 transactions (from group1 at height 100)
        assert_eq!(confirmed_count, 2);
        assert_eq!(validator.get_pending_count(), 1);
        assert_eq!(validator.get_confirmed_count(), 2);
    }

    #[test]
    fn test_reject_old_pending_transactions() {
        let mut validator = TransactionValidator::new();
        validator.set_current_block_height(200);

        // Add pending transactions at different heights
        let mut tx1 = create_test_transaction(1);
        let mut tx2 = create_test_transaction(2);
        let mut tx3 = create_test_transaction(3);

        // Manually add transactions to cache with different heights
        validator.processed_cache.insert(
            [1u8; 32],
            ProcessedTxInfo {
                tx_hash: [1u8; 32],
                group_id: "group1".to_string(),
                processed_at: current_timestamp(),
                block_height: 150, // Old transaction
                execution_status: ExecutionStatus::Pending,
                block_hash: [0u8; 64],
                is_confirmed: false,
            },
        );

        validator.processed_cache.insert(
            [2u8; 32],
            ProcessedTxInfo {
                tx_hash: [2u8; 32],
                group_id: "group2".to_string(),
                processed_at: current_timestamp(),
                block_height: 190, // Recent transaction
                execution_status: ExecutionStatus::Pending,
                block_hash: [0u8; 64],
                is_confirmed: false,
            },
        );

        validator.processed_cache.insert(
            [3u8; 32],
            ProcessedTxInfo {
                tx_hash: [3u8; 32],
                group_id: "group3".to_string(),
                processed_at: current_timestamp(),
                block_height: 195, // Recent transaction
                execution_status: ExecutionStatus::Pending,
                block_hash: [0u8; 64],
                is_confirmed: false,
            },
        );

        assert_eq!(validator.get_pending_count(), 3);

        // Reject old pending transactions (older than 50 blocks)
        let rejected_count = validator.reject_old_pending_transactions(50);

        // Should reject 1 transaction (height 150, difference = 50)
        assert_eq!(rejected_count, 1);
        assert_eq!(validator.get_pending_count(), 2);
        assert_eq!(validator.get_rejected_count(), 1);
    }

    #[test]
    fn test_get_transactions_by_status() {
        let mut validator = TransactionValidator::new();

        // Add transactions with different statuses
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        validator.validate_block_transactions(vec![tx1, tx2], "group1".to_string());

        // Update one to confirmed
        validator
            .update_transaction_status(&[1u8; 32], ExecutionStatus::Confirmed)
            .unwrap();

        // Update one to rejected
        validator
            .update_transaction_status(&[2u8; 32], ExecutionStatus::Rejected)
            .unwrap();

        // Check transactions by status
        let pending_txs = validator.get_transactions_by_status(ExecutionStatus::Pending);
        let confirmed_txs = validator.get_transactions_by_status(ExecutionStatus::Confirmed);
        let rejected_txs = validator.get_transactions_by_status(ExecutionStatus::Rejected);

        assert_eq!(pending_txs.len(), 1);
        assert_eq!(confirmed_txs.len(), 1);
        assert_eq!(rejected_txs.len(), 1);

        assert_eq!(pending_txs[0].tx_hash, [3u8; 32]);
        assert_eq!(confirmed_txs[0].tx_hash, [1u8; 32]);
        assert_eq!(rejected_txs[0].tx_hash, [2u8; 32]);
    }

    #[test]
    fn test_get_transactions_by_group() {
        let mut validator = TransactionValidator::new();

        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        validator.validate_block_transactions(vec![tx1, tx2], "group1".to_string());
        validator.validate_block_transactions(vec![tx3], "group2".to_string());

        // Get transactions by group
        let group1_txs = validator.get_transactions_by_group("group1");
        let group2_txs = validator.get_transactions_by_group("group2");

        assert_eq!(group1_txs.len(), 2);
        assert_eq!(group2_txs.len(), 1);

        // Check pending transactions by group
        let group1_pending = validator.get_pending_transactions_by_group("group1");
        assert_eq!(group1_pending.len(), 2);
    }

    #[test]
    fn test_detailed_cache_stats() {
        let mut validator = TransactionValidator::new();
        validator.set_current_block_height(100);

        // Add transactions with different statuses
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        validator.validate_block_transactions(vec![tx1, tx2], "group1".to_string());
        validator.validate_block_transactions(vec![tx3], "group2".to_string());

        // Update some statuses
        validator
            .update_transaction_status(&[1u8; 32], ExecutionStatus::Confirmed)
            .unwrap();
        validator
            .update_transaction_status(&[2u8; 32], ExecutionStatus::Rejected)
            .unwrap();

        // Get detailed stats
        let stats = validator.get_detailed_cache_stats();

        assert_eq!(stats.total_cached, 3);
        assert_eq!(stats.confirmed_count, 1);
        assert_eq!(stats.pending_count, 1);
        assert_eq!(stats.rejected_count, 1);
        assert_eq!(stats.invalidated_count, 0);
        assert_eq!(stats.unique_groups, 2);
        assert_eq!(stats.current_block_height, 100);

        // Check age statistics
        assert!(stats.average_age_seconds >= 0);
        assert!(stats.oldest_transaction_age >= stats.newest_transaction_age);
    }

    #[test]
    fn test_cache_stats_accuracy() {
        let mut validator = TransactionValidator::new();

        // Add transactions with different statuses
        let tx1 = create_test_transaction(1);
        let tx2 = create_test_transaction(2);
        let tx3 = create_test_transaction(3);

        validator.validate_block_transactions(vec![tx1, tx2, tx3], "group1".to_string());

        // Update some statuses
        validator
            .update_transaction_status(&[1u8; 32], ExecutionStatus::Confirmed)
            .unwrap();
        validator
            .update_transaction_status(&[2u8; 32], ExecutionStatus::Rejected)
            .unwrap();

        let stats = validator.get_cache_stats();
        let detailed_stats = validator.get_detailed_cache_stats();

        // Verify consistency between basic and detailed stats
        assert_eq!(stats.total_cached, detailed_stats.total_cached);
        assert_eq!(stats.confirmed_count, detailed_stats.confirmed_count);
        assert_eq!(stats.pending_count, detailed_stats.pending_count);
        assert_eq!(stats.unique_groups, detailed_stats.unique_groups);

        // Verify math: total = confirmed + pending + rejected
        assert_eq!(
            stats.total_cached,
            stats.confirmed_count + stats.pending_count + stats.rejected_count
        );
    }
}
