//! 
//! transactions, and proposals.

use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait SignatureValidator: Send + Sync {
    /// Validate block signatures
    async fn validate_block_signatures(&self, block: &Block) -> Result<(), ValidationError>;
    
    /// Validate transaction signature
    async fn validate_transaction_signature(&self, tx: &Transaction) -> Result<(), ValidationError>;
    
    /// Validate proposal signature
    async fn validate_proposal_signature(&self, proposal: &dyn Proposal) -> Result<(), ValidationError>;
    
    async fn validate_validator_signature(&self, message: &[u8], signature: &[u8], public_key: &[u8]) -> Result<bool>;
    
    /// Sign message
    async fn sign_message(&self, message: &[u8], private_key: &[u8]) -> Result<Vec<u8>>;
}

pub struct DefaultSignatureValidator {
    config: SignatureValidationConfig,
    stats: Arc<tokio::sync::RwLock<SignatureValidationStats>>,
}

#[derive(Debug, Clone)]
pub struct SignatureValidationConfig {
    pub enable_validation: bool,
    pub enable_batch_validation: bool,
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
}

impl Default for SignatureValidationConfig {
    fn default() -> Self {
        Self {
            enable_validation: true,
            enable_batch_validation: true,
            enable_caching: true,
            cache_ttl_secs: 300, // 5 minutes
            max_batch_size: 100,
            timeout_ms: 5000,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignatureValidationStats {
    pub total_signatures: u64,
    /// Valid signatures
    pub valid_signatures: u64,
    /// Invalid signatures
    pub invalid_signatures: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    pub avg_validation_time_us: f64,
    pub batch_validations: u64,
}

impl DefaultSignatureValidator {
    pub fn new(config: SignatureValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(tokio::sync::RwLock::new(SignatureValidationStats::default())),
        }
    }
}

#[async_trait]
impl SignatureValidator for DefaultSignatureValidator {
    async fn validate_block_signatures(&self, block: &Block) -> Result<(), ValidationError> {
        if !self.config.enable_validation {
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        
        // Validate proposer signature
        if let Err(error) = self.validate_proposer_signature(&block.header.proposer, &block.header, &block.consensus_data.proposer_info.public_key).await {
            self.record_invalid_signature(start_time.elapsed().as_micros() as f64).await;
            return Err(error);
        }
        
        for signature in &block.signatures {
            if let Err(error) = self.validate_validator_signature(&block.hash(), &signature.signature, &signature.public_key).await {
                self.record_invalid_signature(start_time.elapsed().as_micros() as f64).await;
                return Err(error);
            }
        }
        
        self.record_valid_signature(start_time.elapsed().as_micros() as f64).await;
        Ok(())
    }
    
    async fn validate_transaction_signature(&self, tx: &Transaction) -> Result<(), ValidationError> {
        if !self.config.enable_validation {
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        
        // Validate transaction signature
        let tx_data = self.serialize_transaction(tx)?;
        if let Err(error) = self.validate_validator_signature(&tx_data, &tx.signature, &tx.from).await {
            self.record_invalid_signature(start_time.elapsed().as_micros() as f64).await;
            return Err(error);
        }
        
        self.record_valid_signature(start_time.elapsed().as_micros() as f64).await;
        Ok(())
    }
    
    async fn validate_proposal_signature(&self, proposal: &dyn Proposal) -> Result<(), ValidationError> {
        if !self.config.enable_validation {
            return Ok(());
        }
        
        let start_time = std::time::Instant::now();
        
        let proposal_data = self.serialize_proposal(proposal)?;
        let proposer_info = proposal.proposer_info();
        
        if let Err(error) = self.validate_validator_signature(&proposal_data, &proposal.signature(), &proposer_info.public_key).await {
            self.record_invalid_signature(start_time.elapsed().as_micros() as f64).await;
            return Err(error);
        }
        
        self.record_valid_signature(start_time.elapsed().as_micros() as f64).await;
        Ok(())
    }
    
    async fn validate_validator_signature(&self, message: &[u8], signature: &[u8], public_key: &[u8]) -> Result<bool> {
        if !self.config.enable_validation {
            return Ok(true);
        }
        
        // In a real implementation, this would use actual cryptographic verification
        
        // Check if signature is empty
        if signature.iter().all(|&b| b == 0) {
            return Ok(false);
        }
        
        // Check if public key is empty
        if public_key.iter().all(|&b| b == 0) {
            return Ok(false);
        }
        
        // Simulate verification time
        tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        
        Ok(true)
    }
    
    async fn sign_message(&self, message: &[u8], _private_key: &[u8]) -> Result<Vec<u8>> {
        // In a real implementation, this would use actual cryptographic signing
        // For now, return a mock signature
        let mut signature = vec![0u8; 64];
        signature[0] = 1; // Non-zero signature
        Ok(signature)
    }
}

impl DefaultSignatureValidator {
    fn serialize_transaction(&self, tx: &Transaction) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        data.extend_from_slice(&tx.hash);
        data.extend_from_slice(&tx.from);
        data.extend_from_slice(&tx.to);
        data.extend_from_slice(&tx.amount.to_le_bytes());
        data.extend_from_slice(&tx.nonce.to_le_bytes());
        data.extend_from_slice(&tx.fee.to_le_bytes());
        data.extend_from_slice(&tx.data);
        data.extend_from_slice(&tx.timestamp.to_le_bytes());
        Ok(data)
    }
    
    fn serialize_proposal(&self, proposal: &dyn Proposal) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        data.extend_from_slice(&proposal.hash());
        data.extend_from_slice(&proposal.proposer_info().public_key);
        data.extend_from_slice(proposal.round_id().to_le_bytes().as_ref());
        data.extend_from_slice(proposal.height().to_le_bytes().as_ref());
        data.extend_from_slice(proposal.timestamp().to_le_bytes().as_ref());
        Ok(data)
    }
    
    /// Validate proposer signature
    async fn validate_proposer_signature(&self, proposer_pubkey: &[u8], header: &BlockHeader, expected_pubkey: &[u8]) -> Result<(), ValidationError> {
        // Check if proposer public key matches expected
        if proposer_pubkey != expected_pubkey {
            return Err(ValidationError::InvalidSignature);
        }
        
        // For now, we'll just check that the proposer public key is not empty
        if proposer_pubkey.iter().all(|&b| b == 0) {
            return Err(ValidationError::InvalidSignature);
        }
        
        Ok(())
    }
    
    async fn record_valid_signature(&self, duration_us: f64) {
        let mut stats = self.stats.write().await;
        stats.total_signatures += 1;
        stats.valid_signatures += 1;
        
        stats.avg_validation_time_us = if stats.total_signatures == 1 {
            duration_us
        } else {
            (stats.avg_validation_time_us * (stats.total_signatures - 1) as f64 + duration_us) / stats.total_signatures as f64
        };
    }
    
    async fn record_invalid_signature(&self, duration_us: f64) {
        let mut stats = self.stats.write().await;
        stats.total_signatures += 1;
        stats.invalid_signatures += 1;
        
        stats.avg_validation_time_us = if stats.total_signatures == 1 {
            duration_us
        } else {
            (stats.avg_validation_time_us * (stats.total_signatures - 1) as f64 + duration_us) / stats.total_signatures as f64
        };
    }
    
    pub async fn get_stats(&self) -> SignatureValidationStats {
        self.stats.read().await.clone()
    }
    
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = SignatureValidationStats::default();
    }
}

pub struct BatchSignatureValidator {
    validator: Arc<dyn SignatureValidator>,
    config: SignatureValidationConfig,
    stats: Arc<tokio::sync::RwLock<BatchValidationStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct BatchValidationStats {
    /// Total batches processed
    pub total_batches: u64,
    /// Total signatures in batches
    pub total_signatures: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average batch time in milliseconds
    pub avg_batch_time_ms: f64,
    /// Successful batches
    pub successful_batches: u64,
    /// Failed batches
    pub failed_batches: u64,
}

impl BatchSignatureValidator {
    pub fn new(validator: Arc<dyn SignatureValidator>, config: SignatureValidationConfig) -> Self {
        Self {
            validator,
            config,
            stats: Arc::new(tokio::sync::RwLock::new(BatchValidationStats::default())),
        }
    }
    
    /// Validate multiple signatures in batch
    pub async fn validate_signatures_batch(&self, signatures: &[SignatureData]) -> Result<Vec<bool>> {
        if !self.config.enable_batch_validation || signatures.is_empty() {
            return Ok(signatures.iter().map(|_| true).collect());
        }
        
        let start_time = std::time::Instant::now();
        let batch_size = signatures.len().min(self.config.max_batch_size);
        
        // Process signatures in parallel
        let futures: Vec<_> = signatures.iter()
            .take(batch_size)
            .map(|sig| self.validator.validate_validator_signature(&sig.message, &sig.signature, &sig.public_key))
            .collect();
        
        let results: Vec<bool> = futures::future::join_all(futures).await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|_| vec![false; batch_size]);
        
        // Record statistics
        let duration = start_time.elapsed().as_millis() as f64;
        self.record_batch_stats(batch_size, duration, &results).await;
        
        Ok(results)
    }
    
    /// Validate multiple transactions in batch
    pub async fn validate_transactions_batch(&self, transactions: &[Transaction]) -> Result<Vec<ValidationResult>> {
        if !self.config.enable_batch_validation || transactions.is_empty() {
            return Ok(transactions.iter().map(|_| ValidationResult::Valid).collect());
        }
        
        let start_time = std::time::Instant::now();
        let batch_size = transactions.len().min(self.config.max_batch_size);
        
        // Process transactions in parallel
        let futures: Vec<_> = transactions.iter()
            .take(batch_size)
            .map(|tx| self.validator.validate_transaction_signature(tx))
            .collect();
        
        let results: Vec<ValidationResult> = futures::future::join_all(futures).await
            .into_iter()
            .map(|result| {
                match result {
                    Ok(()) => ValidationResult::Valid,
                    Err(error) => ValidationResult::Invalid(error),
                }
            })
            .collect();
        
        // Record statistics
        let duration = start_time.elapsed().as_millis() as f64;
        let valid_count = results.iter().filter(|r| r.is_valid()).count();
        self.record_batch_stats(batch_size, duration, &vec![true; valid_count]).await;
        
        Ok(results)
    }
    
    async fn record_batch_stats(&self, batch_size: usize, duration_ms: f64, results: &[bool]) {
        let mut stats = self.stats.write().await;
        stats.total_batches += 1;
        stats.total_signatures += batch_size as u64;
        
        stats.avg_batch_size = if stats.total_batches == 1 {
            batch_size as f64
        } else {
            (stats.avg_batch_size * (stats.total_batches - 1) as f64 + batch_size as f64) / stats.total_batches as f64
        };
        
        stats.avg_batch_time_ms = if stats.total_batches == 1 {
            duration_ms
        } else {
            (stats.avg_batch_time_ms * (stats.total_batches - 1) as f64 + duration_ms) / stats.total_batches as f64
        };
        
        if results.iter().all(|&r| r) {
            stats.successful_batches += 1;
        } else {
            stats.failed_batches += 1;
        }
    }
    
    pub async fn get_stats(&self) -> BatchValidationStats {
        self.stats.read().await.clone()
    }
    
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = BatchValidationStats::default();
    }
}

#[derive(Debug, Clone)]
pub struct SignatureData {
    pub message: Vec<u8>,
    pub signature: Vec<u8>,
    pub public_key: Vec<u8>,
}

pub struct MockSignatureValidator;

#[async_trait]
impl SignatureValidator for MockSignatureValidator {
    async fn validate_block_signatures(&self, _block: &Block) -> Result<(), ValidationError> {
        Ok(())
    }
    
    async fn validate_transaction_signature(&self, _tx: &Transaction) -> Result<(), ValidationError> {
        Ok(())
    }
    
    async fn validate_proposal_signature(&self, _proposal: &dyn Proposal) -> Result<(), ValidationError> {
        Ok(())
    }
    
    async fn validate_validator_signature(&self, _message: &[u8], _signature: &[u8], _public_key: &[u8]) -> Result<bool> {
        Ok(true)
    }
    
    async fn sign_message(&self, _message: &[u8], _private_key: &[u8]) -> Result<Vec<u8>> {
        Ok(vec![1u8; 64]) // Mock signature
    }
}

pub struct SignatureValidationUtils;

impl SignatureValidationUtils {
    /// Verify Ed25519 signature
    pub fn verify_ed25519_signature(message: &[u8], signature: &[u8], public_key: &[u8]) -> Result<bool> {
        use ed25519_dalek::{VerifyingKey, Signature};
        
        if signature.len() != 64 || public_key.len() != 32 {
            return Ok(false);
        }
        
        let pk = VerifyingKey::from_bytes(public_key.try_into().map_err(|_| ValidationError::InvalidSignature)?);
        let sig = Signature::from_bytes(signature.try_into().map_err(|_| ValidationError::InvalidSignature)?);
        
        Ok(pk.verify(message, &sig).is_ok())
    }
    
    /// Create Ed25519 signature
    pub fn create_ed25519_signature(message: &[u8], private_key: &[u8]) -> Result<Vec<u8>> {
        use ed25519_dalek::{SigningKey, Signer};
        
        let sk = SigningKey::from_bytes(private_key.try_into().map_err(|_| ValidationError::InvalidSignature)?);
        let signature = sk.sign(message);
        Ok(signature.to_bytes().to_vec())
    }
    
    /// Validate signature format
    pub fn validate_signature_format(signature: &[u8]) -> bool {
        signature.len() == 64 && !signature.iter().all(|&b| b == 0)
    }
    
    /// Validate public key format
    pub fn validate_public_key_format(public_key: &[u8]) -> bool {
        public_key.len() == 32 && !public_key.iter().all(|&b| b == 0)
    }
    
    /// Hash message for signing
    pub fn hash_message(message: &[u8]) -> Vec<u8> {
        blake3::hash(message).as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_default_signature_validator() {
        let config = SignatureValidationConfig::default();
        let validator = DefaultSignatureValidator::new(config);
        
        let stats = validator.get_stats().await;
        assert_eq!(stats.total_signatures, 0);
    }
    
    #[tokio::test]
    async fn test_batch_signature_validator() {
        let config = SignatureValidationConfig::default();
        let mock_validator = Arc::new(MockSignatureValidator);
        let batch_validator = BatchSignatureValidator::new(mock_validator, config);
        
        let signatures = vec![
            SignatureData {
                message: vec![1, 2, 3],
                signature: vec![1u8; 64],
                public_key: vec![2u8; 32],
            },
            SignatureData {
                message: vec![4, 5, 6],
                signature: vec![3u8; 64],
                public_key: vec![4u8; 32],
            },
        ];
        
        let results = batch_validator.validate_signatures_batch(&signatures).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|&r| r));
    }
    
    #[test]
    fn test_signature_validation_utils() {
        assert!(SignatureValidationUtils::validate_signature_format(&[1u8; 64]));
        assert!(!SignatureValidationUtils::validate_signature_format(&[0u8; 64]));
        assert!(!SignatureValidationUtils::validate_signature_format(&[1u8; 63]));
        
        assert!(SignatureValidationUtils::validate_public_key_format(&[1u8; 32]));
        assert!(!SignatureValidationUtils::validate_public_key_format(&[0u8; 32]));
        assert!(!SignatureValidationUtils::validate_public_key_format(&[1u8; 31]));
    }
}
