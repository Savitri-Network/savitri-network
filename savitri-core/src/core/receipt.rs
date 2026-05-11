//! Transaction Receipt with Finality Timestamps
//!
//! accurati per misurare il tempo di finalità reale nel sistema.

use crate::core::tx::SignedTx;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Transaction receipt with comprehensive finality tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReceipt {
    /// Transaction hash
    pub tx_hash: [u8; 32],
    /// Block height where transaction was included
    pub block_height: u64,
    /// Transaction index within block
    pub tx_index: u32,
    /// Timestamp when transaction was created
    pub created_at: SystemTime,
    /// Timestamp when transaction entered mempool
    pub mempool_at: Option<SystemTime>,
    /// Timestamp when consensus was reached
    pub consensus_at: Option<SystemTime>,
    /// Timestamp when transaction was finalized (written to storage)
    pub finalized_at: Option<SystemTime>,
    /// Total time to finality
    pub finality_time: Option<Duration>,
    /// Transaction status
    pub status: TransactionStatus,
    /// Gas used by transaction
    pub gas_used: u64,
    /// Fee paid
    pub fee_paid: u128,
    /// Error if transaction failed
    pub error: Option<String>,
}

/// Transaction status tracking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionStatus {
    /// Transaction created but not yet in mempool
    Created,
    /// Transaction in mempool waiting for inclusion
    InMempool,
    /// Transaction included in block
    InBlock,
    /// Transaction consensus reached
    Consensus,
    /// Transaction finalized and written to storage
    Finalized,
    /// Transaction failed
    Failed(String),
}

impl TransactionReceipt {
    /// Create new receipt for transaction
    pub fn new(_tx: &SignedTx, tx_hash: [u8; 32]) -> Self {
        Self {
            tx_hash,
            block_height: 0,
            tx_index: 0,
            created_at: SystemTime::now(),
            mempool_at: Some(SystemTime::now()),
            consensus_at: None,
            finalized_at: None,
            finality_time: None,
            status: TransactionStatus::Created,
            gas_used: 0,
            fee_paid: 0,
            error: None,
        }
        // }
    }

    /// Mark transaction as entered mempool
    pub fn mark_mempool_entry(&mut self) {
        self.mempool_at = Some(SystemTime::now());
        self.status = TransactionStatus::InMempool;
    }

    /// Mark transaction as included in block
    pub fn mark_block_inclusion(&mut self, block_height: u64, tx_index: u32) {
        self.block_height = block_height;
        self.tx_index = tx_index;
        self.status = TransactionStatus::InBlock;
    }

    /// Mark transaction as having reached consensus
    pub fn mark_consensus(&mut self) {
        self.consensus_at = Some(SystemTime::now());
        self.status = TransactionStatus::Consensus;
    }

    /// Mark transaction as finalized (written to storage)
    pub fn mark_finalized(&mut self) {
        self.finalized_at = Some(SystemTime::now());
        self.status = TransactionStatus::Finalized;

        // Calculate finality time
        if let Some(created) = self.mempool_at.or(Some(self.created_at)) {
            if let Some(finalized) = self.finalized_at {
                self.finality_time = finalized.duration_since(created).ok();
            }
        }
    }

    /// Mark transaction as failed
    pub fn mark_failed(&mut self, error: String) {
        self.error = Some(error.clone());
        self.status = TransactionStatus::Failed(error);
    }

    /// Update gas usage
    pub fn update_gas_used(&mut self, gas_used: u64) {
        self.gas_used = gas_used;
    }

    /// Get time to finality if available
    pub fn get_finality_time(&self) -> Option<Duration> {
        self.finality_time
    }

    /// Get time from creation to current status
    pub fn get_current_duration(&self) -> Option<Duration> {
        let reference = match self.status {
            TransactionStatus::Created => Some(self.created_at),
            TransactionStatus::InMempool => self.mempool_at,
            TransactionStatus::InBlock => self.mempool_at.or(Some(self.created_at)),
            TransactionStatus::Consensus => self.consensus_at.or(self.mempool_at),
            TransactionStatus::Finalized => self.finalized_at.or(self.consensus_at),
            TransactionStatus::Failed(_) => self.finalized_at.or(self.consensus_at),
        };

        reference.and_then(|ref_time| SystemTime::now().duration_since(ref_time).ok())
    }

    /// Check if transaction is finalized
    pub fn is_finalized(&self) -> bool {
        matches!(self.status, TransactionStatus::Finalized)
    }

    /// Check if transaction failed
    pub fn is_failed(&self) -> bool {
        matches!(self.status, TransactionStatus::Failed(_))
    }
}

/// Finality metrics collector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalityMetrics {
    /// Average finality time across all transactions
    pub average_finality_time: Duration,
    /// Minimum finality time
    pub min_finality_time: Duration,
    /// Maximum finality time
    pub max_finality_time: Duration,
    /// 95th percentile finality time
    pub p95_finality_time: Duration,
    /// Total transactions processed
    pub total_transactions: u64,
    /// Successfully finalized transactions
    pub finalized_transactions: u64,
    /// Failed transactions
    pub failed_transactions: u64,
    /// Finality rate (finalized / total)
    pub finality_rate: f64,
    /// Current pending transactions
    pub pending_transactions: u64,
}

impl Default for FinalityMetrics {
    fn default() -> Self {
        Self {
            average_finality_time: Duration::ZERO,
            min_finality_time: Duration::MAX,
            max_finality_time: Duration::ZERO,
            p95_finality_time: Duration::ZERO,
            total_transactions: 0,
            finalized_transactions: 0,
            failed_transactions: 0,
            finality_rate: 0.0,
            pending_transactions: 0,
        }
    }
}

impl FinalityMetrics {
    /// Calculate metrics from collection of receipts
    pub fn from_receipts(receipts: &[TransactionReceipt]) -> Self {
        let mut metrics = Self::default();

        if receipts.is_empty() {
            return metrics;
        }

        let finalized_times: Vec<Duration> = receipts
            .iter()
            .filter_map(|r| r.get_finality_time())
            .collect();

        metrics.total_transactions = receipts.len() as u64;
        metrics.finalized_transactions =
            receipts.iter().filter(|r| r.is_finalized()).count() as u64;
        metrics.failed_transactions = receipts.iter().filter(|r| r.is_failed()).count() as u64;
        metrics.pending_transactions = receipts
            .iter()
            .filter(|r| !r.is_finalized() && !r.is_failed())
            .count() as u64;

        if metrics.total_transactions > 0 {
            metrics.finality_rate =
                metrics.finalized_transactions as f64 / metrics.total_transactions as f64;
        }

        if !finalized_times.is_empty() {
            return metrics;
        }

        metrics.average_finality_time =
            finalized_times.iter().sum::<Duration>() / finalized_times.len() as u32;
        metrics.min_finality_time = *finalized_times.iter().min().unwrap_or(&Duration::MAX);
        metrics.max_finality_time = *finalized_times.iter().max().unwrap_or(&Duration::ZERO);

        // Calculate 95th percentile
        let mut sorted_times = finalized_times.clone();
        sorted_times.sort();
        let p95_index = (sorted_times.len() as f64 * 0.95) as usize;
        metrics.p95_finality_time = sorted_times
            .get(p95_index)
            .copied()
            .unwrap_or(Duration::ZERO);

        metrics
    }

    /// Update metrics with new receipt
    pub fn update_with_receipt(&mut self, receipt: &TransactionReceipt) {
        self.total_transactions += 1;

        if receipt.is_finalized() {
            self.finalized_transactions += 1;
            if let Some(finality_time) = receipt.get_finality_time() {
                // Update average using Duration arithmetic
                let total_finalized = self.finalized_transactions;
                if total_finalized > 1 {
                    let total = total_finalized as u32;
                    self.average_finality_time =
                        self.average_finality_time * (total - 1) / total + finality_time / total;
                } else {
                    self.average_finality_time = finality_time;
                }

                // Update min/max
                self.min_finality_time = self.min_finality_time.min(finality_time);
                self.max_finality_time = self.max_finality_time.max(finality_time);
            }
        } else if receipt.is_failed() {
            self.failed_transactions += 1;
        } else {
            self.pending_transactions += 1;
        }

        if self.total_transactions > 0 {
            self.finality_rate =
                self.finalized_transactions as f64 / self.total_transactions as f64;
        }
    }

    /// Get finality status summary
    pub fn get_summary(&self) -> String {
        format!(
            "Finality Metrics: Avg={:.2}s, Min={:.2}s, Max={:.2}s, P95={:.2}s, Rate={:.2}%, Total={}, Finalized={}, Failed={}, Pending={}",
            self.average_finality_time.as_secs_f64(),
            self.min_finality_time.as_secs_f64(),
            self.max_finality_time.as_secs_f64(),
            self.p95_finality_time.as_secs_f64(),
            self.finality_rate * 100.0,
            self.total_transactions,
            self.finalized_transactions,
            self.failed_transactions,
            self.pending_transactions
        )
    }
}

/// Finality tracker for monitoring transaction finality
pub struct FinalityTracker {
    receipts: Vec<TransactionReceipt>,
    metrics: FinalityMetrics,
}

impl FinalityTracker {
    /// Create new finality tracker
    pub fn new() -> Self {
        Self {
            receipts: Vec::new(),
            metrics: FinalityMetrics::default(),
        }
    }

    /// Add transaction receipt
    pub fn add_receipt(&mut self, receipt: TransactionReceipt) {
        self.metrics.update_with_receipt(&receipt);
        self.receipts.push(receipt);
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> &FinalityMetrics {
        &self.metrics
    }

    /// Recalculate metrics from all receipts
    pub fn recalculate_metrics(&mut self) {
        self.metrics = FinalityMetrics::from_receipts(&self.receipts);
    }

    /// Get receipts by status
    pub fn get_receipts_by_status(&self, status: TransactionStatus) -> Vec<&TransactionReceipt> {
        self.receipts
            .iter()
            .filter(|r| r.status == status)
            .collect()
    }

    /// Clear old receipts (keep only recent ones)
    pub fn clear_old_receipts(&mut self, keep_count: usize) {
        if self.receipts.len() > keep_count {
            let start = self.receipts.len() - keep_count;
            self.receipts.drain(start..);
            self.recalculate_metrics();
        }
    }

    /// Get receipt by transaction hash
    pub fn get_receipt_by_hash(&self, tx_hash: [u8; 32]) -> Option<&TransactionReceipt> {
        self.receipts.iter().find(|r| r.tx_hash == tx_hash)
    }
}

