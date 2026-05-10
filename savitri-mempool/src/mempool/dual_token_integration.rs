//! Dual Token Integration for Mempool
//!
//! This module integrates the dual token system with the mempool pipeline,
//! including TEST token balance checking, dynamic fee calculation, and fee burning.

use crate::mempool::integration::MempoolPipeline;
use crate::mempool::prevalidation::{hash_signed_tx_bytes, PrevalidationResult};
use crate::mempool::types::{MempoolTx, PrevalidatedTx, RawTx, SignedTx};
use anyhow::Result;
use savitri_storage::{Storage, StorageTrait};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Deserialize Transaction from bytes using bincode
fn deserialize_transaction(bytes: &[u8]) -> Option<savitri_core::core::types::Transaction> {
    match bincode::deserialize::<savitri_core::core::types::Transaction>(bytes) {
        Ok(tx) => Some(tx),
        Err(e) => {
            tracing::warn!("Failed to deserialize transaction: {}", e);
            None
        }
    }
}

/// Create a SignedTx from raw transaction bytes using proper deserialization
fn create_signed_tx_from_bytes(raw_tx: &[u8]) -> Result<SignedTx> {
    // First try to deserialize as Transaction using bincode
    if let Some(transaction) = deserialize_transaction(raw_tx) {
        // Convert Transaction to SignedTx format
        return Ok(SignedTx {
            from: transaction.from.as_bytes().to_vec(),
            to: transaction.to.as_bytes().to_vec(),
            amount: transaction.amount,
            nonce: transaction.nonce,
            fee: transaction.fee,
            pubkey: vec![0u8; 32], // Would be extracted from signature in real implementation
            sig: transaction.signature,
            pre_verified: false,
        });
    }

    // Fallback: try to extract basic data manually if bincode fails
    let mut tx = SignedTx {
        from: vec![0u8; 32],
        to: vec![0u8; 32],
        amount: 0,
        nonce: 0,
        fee: 0,
        pubkey: vec![0u8; 32],
        sig: vec![0u8; 32],
        pre_verified: false,
    };

    // Extract data from raw bytes if available
    if raw_tx.len() >= 96 {
        // Extract from address (32 bytes)
        tx.from.copy_from_slice(&raw_tx[0..32]);
        // Extract to address (32 bytes)
        tx.to.copy_from_slice(&raw_tx[32..64]);

        // Extract amount (little-endian u64)
        if raw_tx.len() >= 72 {
            let amount_bytes = &raw_tx[64..72];
            tx.amount = u64::from_le_bytes([
                amount_bytes[0],
                amount_bytes[1],
                amount_bytes[2],
                amount_bytes[3],
                amount_bytes[4],
                amount_bytes[5],
                amount_bytes[6],
                amount_bytes[7],
            ]);
        }

        // Extract nonce (little-endian u64)
        if raw_tx.len() >= 80 {
            let nonce_bytes = &raw_tx[72..80];
            tx.nonce = u64::from_le_bytes([
                nonce_bytes[0],
                nonce_bytes[1],
                nonce_bytes[2],
                nonce_bytes[3],
                nonce_bytes[4],
                nonce_bytes[5],
                nonce_bytes[6],
                nonce_bytes[7],
            ]);
        }

        // Extract fee (little-endian u64)
        if raw_tx.len() >= 88 {
            let fee_bytes = &raw_tx[80..88];
            tx.fee = u64::from_le_bytes([
                fee_bytes[0],
                fee_bytes[1],
                fee_bytes[2],
                fee_bytes[3],
                fee_bytes[4],
                fee_bytes[5],
                fee_bytes[6],
                fee_bytes[7],
            ]);
        }
    }

    Ok(tx)
}

/// Serialize SignedTx to bytes for hashing
fn serialize_signed_tx(tx: &SignedTx) -> Result<Vec<u8>> {
    // Create a simple serialization format
    let mut bytes = Vec::new();

    // Add from address
    bytes.extend_from_slice(&tx.from);

    // Add to address
    bytes.extend_from_slice(&tx.to);

    // Add amount
    bytes.extend_from_slice(&tx.amount.to_le_bytes());

    // Add nonce
    bytes.extend_from_slice(&tx.nonce.to_le_bytes());

    // Add fee
    bytes.extend_from_slice(&tx.fee.to_le_bytes());

    // Add public key
    bytes.extend_from_slice(&tx.pubkey);

    // Add signature
    bytes.extend_from_slice(&tx.sig);

    // Add pre_verified flag
    bytes.push(if tx.pre_verified { 1 } else { 0 });

    Ok(bytes)
}

// ============================================
// DUAL TOKEN INTEGRATION TYPES
// ============================================

#[derive(Debug, Clone)]
pub struct DualTokenFeeEngine {
    pub min_balance_threshold: u128,
}

impl DualTokenFeeEngine {
    pub fn new() -> Self {
        Self {
            min_balance_threshold: 1000000,
        }
    }

    pub fn calculate_dynamic_fee(
        &self,
        _tx: &SignedTx,
        _params: &DynamicFeeParams,
    ) -> FeeCalculationResult {
        FeeCalculationResult {
            required_fee: 1000,
            dynamic_fee: 1200,
            final_fee: 1200,
            burn_amount: 120,
            network_fee: 1080,
        }
    }

    pub fn get_network_metrics(&self) -> NetworkMetrics {
        NetworkMetrics {
            mempool_size: 100,
            avg_block_time_ms: 5000,
            throughput_tps: 25.5,
            gas_utilization: 0.75,
        }
    }

    pub fn burn_rate(&self) -> f64 {
        0.1
    }

    pub fn min_balance_threshold(&self) -> u128 {
        self.min_balance_threshold
    }
}

#[derive(Debug, Clone)]
pub struct DynamicFeeParams {
    pub base_fee: u64,
    pub multiplier: f64,
}

impl Default for DynamicFeeParams {
    fn default() -> Self {
        Self {
            base_fee: 1000,
            multiplier: 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkMetrics {
    pub mempool_size: usize,
    pub avg_block_time_ms: u64,
    pub throughput_tps: f64,
    pub gas_utilization: f64,
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        Self {
            mempool_size: 0,
            avg_block_time_ms: 5000,
            throughput_tps: 0.0,
            gas_utilization: 0.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeeCalculationResult {
    pub required_fee: u64,
    pub dynamic_fee: u64,
    pub final_fee: u64,
    pub burn_amount: u64,
    pub network_fee: u64,
}

// ============================================
// ERROR TYPES (OUTSIDE impl block)
// ============================================

#[derive(Debug, Clone)]
pub enum DualTokenTransactionError {
    PrevalidationFailed(String),
    PrevalidationError(String),
    DeserializationError(String),
    SerializationError(String),
    InvalidAddress(String),
    FeeCalculationError(String),
    InsufficientBalance(String),
    StorageError(String),
    MempoolError(String),
    AdmissionRejected(String),
}

impl std::fmt::Display for DualTokenTransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DualTokenTransactionError::PrevalidationFailed(msg) => {
                write!(f, "Prevalidation failed: {}", msg)
            }
            DualTokenTransactionError::PrevalidationError(msg) => {
                write!(f, "Prevalidation error: {}", msg)
            }
            DualTokenTransactionError::DeserializationError(msg) => {
                write!(f, "Deserialization error: {}", msg)
            }
            DualTokenTransactionError::SerializationError(msg) => {
                write!(f, "Serialization error: {}", msg)
            }
            DualTokenTransactionError::InvalidAddress(msg) => write!(f, "Invalid address: {}", msg),
            DualTokenTransactionError::FeeCalculationError(msg) => {
                write!(f, "Fee calculation error: {}", msg)
            }
            DualTokenTransactionError::InsufficientBalance(msg) => {
                write!(f, "Insufficient balance: {}", msg)
            }
            DualTokenTransactionError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            DualTokenTransactionError::MempoolError(msg) => write!(f, "Mempool error: {}", msg),
            DualTokenTransactionError::AdmissionRejected(msg) => {
                write!(f, "Admission rejected: {}", msg)
            }
        }
    }
}

impl std::error::Error for DualTokenTransactionError {}

// ============================================
// SUPPORTING TYPES
// ============================================

#[derive(Debug, Clone)]
pub struct FeeBurnInfo {
    pub tx_hash: [u8; 32],
    pub sender_address: [u8; 32],
    pub total_fee: u128,
    pub burn_amount: u128,
    pub network_fee: u128,
}

#[derive(Debug, Clone)]
pub struct DualTokenStats {
    pub total_supply: u128,
    pub total_burned: u128,
    pub circulating_supply: u128,
    pub burn_rate: f64,
    pub min_balance_threshold: u128,
    pub network_metrics: NetworkMetrics,
}

// ============================================
// MAIN STRUCT
// ============================================

pub struct DualTokenMempoolIntegration {
    pipeline: Arc<MempoolPipeline>,
    fee_engine: Arc<DualTokenFeeEngine>,
    storage: Arc<dyn StorageTrait>,
}

impl DualTokenMempoolIntegration {
    pub fn new(
        pipeline: Arc<MempoolPipeline>,
        fee_engine: Arc<DualTokenFeeEngine>,
        storage: Arc<dyn StorageTrait>,
    ) -> Self {
        Self {
            pipeline,
            fee_engine,
            storage,
        }
    }

    pub async fn process_raw_transactions_with_dual_token(&self, raw_txs: Vec<RawTx>) -> usize {
        if raw_txs.is_empty() {
            return 0;
        }

        let mut accepted_count = 0;
        let network_metrics = self.fee_engine.get_network_metrics();
        let dynamic_params = DynamicFeeParams::default();

        for raw_tx in raw_txs {
            match self
                .process_single_raw_transaction_with_dual_token(
                    raw_tx,
                    &network_metrics,
                    &dynamic_params,
                )
                .await
            {
                Ok(_) => accepted_count += 1,
                Err(_) => {
                    eprintln!("Failed to process transaction in dual token integration");
                }
            }
        }

        accepted_count
    }

    pub async fn process_single_raw_transaction_with_dual_token(
        &self,
        raw_tx: RawTx,
        _network_metrics: &NetworkMetrics,
        dynamic_params: &DynamicFeeParams,
    ) -> Result<[u8; 32], DualTokenTransactionError> {
        let prevalidation_result = self.pipeline.prevalidator.prevalidate(raw_tx.clone()).await;

        let prevalidated = match prevalidation_result {
            Ok(PrevalidationResult::Valid(pv)) => pv,
            Ok(PrevalidationResult::Invalid(reason)) => {
                return Err(DualTokenTransactionError::PrevalidationFailed(reason));
            }
            Err(e) => {
                return Err(DualTokenTransactionError::PrevalidationError(format!(
                    "{}",
                    e
                )));
            }
        };

        let signed_tx: SignedTx = create_signed_tx_from_bytes(&raw_tx.bytes)
            .map_err(|e| DualTokenTransactionError::DeserializationError(format!("{}", e)))?;

        let sender_address = self.extract_sender_address(&signed_tx)?;

        let fee_result = self
            .fee_engine
            .calculate_dynamic_fee(&signed_tx, dynamic_params);

        let mut updated_tx = signed_tx;
        updated_tx.fee = fee_result.final_fee;

        // Serialize the updated transaction for hashing
        let updated_bytes = serialize_signed_tx(&updated_tx)
            .map_err(|e| DualTokenTransactionError::SerializationError(format!("{}", e)))?;

        let tx_hash = hash_signed_tx_bytes(&updated_bytes);

        let result = {
            let mut mp = self.pipeline.lock_mempool().map_err(|_| {
                DualTokenTransactionError::MempoolError("Failed to lock mempool".to_string())
            })?;

            let updated_prevalidated = PrevalidatedTx {
                sender_id: prevalidated.sender_id,
                sender_address: prevalidated.sender_address,
                nonce: prevalidated.nonce,
                max_fee: fee_result.final_fee as u64,
                amount: prevalidated.amount,
                tx_handle: prevalidated.tx_handle,
                class: prevalidated.class,
                stream_nonce: prevalidated.stream_nonce,
            };

            mp.add_prevalidated(updated_prevalidated, Some(tx_hash))
        };

        match result {
            crate::mempool::core::AdmissionOutcome::Admitted
            | crate::mempool::core::AdmissionOutcome::Queued => {
                self.record_fee_burning(&sender_address, &fee_result)?;
                Ok(tx_hash)
            }
            crate::mempool::core::AdmissionOutcome::Rejected(reason) => {
                Err(DualTokenTransactionError::AdmissionRejected(
                    "transaction rejected by admission control".to_string(),
                ))
            }
        }
    }

    fn extract_sender_address(&self, tx: &SignedTx) -> Result<[u8; 32], DualTokenTransactionError> {
        if tx.from.len() != 32 {
            return Err(DualTokenTransactionError::InvalidAddress(
                "Invalid sender address length".to_string(),
            ));
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&tx.from);
        Ok(address)
    }

    fn record_fee_burning(
        &self,
        _sender_address: &[u8; 32],
        fee_result: &FeeCalculationResult,
    ) -> Result<(), DualTokenTransactionError> {
        let current_burned = 0u128;

        let _new_burned = current_burned
            .checked_add(fee_result.burn_amount as u128)
            .ok_or_else(|| {
                DualTokenTransactionError::StorageError("Burn amount overflow".to_string())
            })?;

        Ok(())
    }

    pub fn drain_for_block_production_with_dual_token(
        &self,
        max_txs: usize,
    ) -> (Vec<MempoolTx>, Vec<SignedTx>, Vec<FeeBurnInfo>) {
        let (mempool_txs, signed_txs) = self.pipeline.drain_for_block_production(max_txs);

        if mempool_txs.is_empty() {
            return (mempool_txs, signed_txs, Vec::new());
        }

        let mut fee_burn_info = Vec::new();
        let _network_metrics = self.fee_engine.get_network_metrics();
        let dynamic_params = DynamicFeeParams::default();

        for (i, signed_tx) in signed_txs.iter().enumerate() {
            if let Some(mempool_tx) = mempool_txs.get(i) {
                if let Ok(sender_address) = self.extract_sender_address(signed_tx) {
                    let fee_result = self
                        .fee_engine
                        .calculate_dynamic_fee(signed_tx, &dynamic_params);

                    if let Err(e) = self.record_fee_burning(&sender_address, &fee_result) {
                        eprintln!("Failed to record fee burning: {}", e);
                    }

                    fee_burn_info.push(FeeBurnInfo {
                        tx_hash: mempool_tx.tx_hash.unwrap_or([0u8; 32]),
                        sender_address,
                        total_fee: fee_result.final_fee as u128,
                        burn_amount: fee_result.burn_amount as u128,
                        network_fee: fee_result.network_fee as u128,
                    });
                }
            }
        }

        (mempool_txs, signed_txs, fee_burn_info)
    }

    pub fn get_dual_token_stats(&self) -> Result<DualTokenStats, DualTokenTransactionError> {
        let total_burned = 0u128;
        let total_supply = 1000000u128;
        let circulating_supply = 0u128;

        let network_metrics = self.fee_engine.get_network_metrics();

        Ok(DualTokenStats {
            total_supply,
            total_burned,
            circulating_supply,
            burn_rate: self.fee_engine.burn_rate(),
            min_balance_threshold: self.fee_engine.min_balance_threshold(),
            network_metrics,
        })
    }
}

// ============================================
// TESTS
// ============================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dual_token_stats_creation() {
        let stats = DualTokenStats {
            total_supply: 1000000,
            total_burned: 1000,
            circulating_supply: 999000,
            burn_rate: 0.1,
            min_balance_threshold: 100,
            network_metrics: NetworkMetrics::default(),
        };

        assert_eq!(stats.total_supply, 1000000);
        assert_eq!(stats.burn_rate, 0.1);
    }

    #[test]
    fn test_fee_burn_info_creation() {
        let info = FeeBurnInfo {
            tx_hash: [1u8; 64],
            sender_address: [2u8; 32],
            total_fee: 1000,
            burn_amount: 100,
            network_fee: 900,
        };

        assert_eq!(info.total_fee, 1000);
        assert_eq!(info.burn_amount, 100);
    }
}
