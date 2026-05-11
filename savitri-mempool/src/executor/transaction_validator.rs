//! Transaction Validator - Security and Consensus Validation Engine
//!
//! 1. Replay prevention
//! 3. Balance sufficiency check

use anyhow::Result;
use ed25519_dalek::{Signature as DalekSignature, Verifier, VerifyingKey as DalekPublicKey};
use lru::LruCache;
use savitri_core::Transaction as SignedTx;
use savitri_storage::{Storage, StorageTrait};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tracing::warn;

/// Wrapper for DalekPublicKey to provide compatibility
pub struct DalekPublicKeyWrapper {
    pub key: DalekPublicKey,
}

impl DalekPublicKeyWrapper {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 32 {
            return Err("Invalid public key length");
        }
        let bytes_array: [u8; 32] = bytes.try_into().map_err(|_| "Invalid public key length")?;
        match DalekPublicKey::from_bytes(&bytes_array) {
            Ok(key) => Ok(DalekPublicKeyWrapper { key }),
            Err(_) => Err("Invalid public key format"),
        }
    }

    pub fn verify_strict(
        &self,
        message: &[u8],
        signature: &DalekSignature,
    ) -> Result<(), &'static str> {
        match self.key.verify(message, signature) {
            Ok(()) => Ok(()),
            Err(_) => Err("Signature verification failed"),
        }
    }
}

/// Wrapper for DalekSignature to provide compatibility
pub struct DalekSignatureWrapper {
    pub signature: DalekSignature,
}

impl DalekSignatureWrapper {
    pub fn as_bytes(&self) -> [u8; 64] {
        self.signature.to_bytes()
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 64 {
            return Err("Invalid signature length");
        }
        let bytes_array: [u8; 64] = bytes.try_into().map_err(|_| "Invalid signature length")?;
        Ok(DalekSignatureWrapper {
            signature: DalekSignature::from_bytes(&bytes_array),
        })
    }
}

impl From<&Vec<u8>> for DalekSignatureWrapper {
    fn from(bytes: &Vec<u8>) -> Self {
        if bytes.len() == 64 {
            let bytes_array: [u8; 64] = bytes.as_slice().try_into().unwrap_or([0u8; 64]);
            DalekSignatureWrapper {
                signature: DalekSignature::from_bytes(&bytes_array),
            }
        } else {
            // Create a dummy signature for error cases
            DalekSignatureWrapper {
                signature: DalekSignature::from_bytes(&[0u8; 64]),
            }
        }
    }
}

/// Extension trait for SignedTx to provide message serialization
pub trait TransactionMessage {
    fn message(&self) -> Vec<u8>;
}

impl TransactionMessage for SignedTx {
    fn message(&self) -> Vec<u8> {
        // Create message for signature verification
        let mut message = Vec::new();

        // Add transaction fields to message (from/to are String types)
        message.extend_from_slice(self.from.as_bytes());
        message.extend_from_slice(self.to.as_bytes());
        message.extend_from_slice(&self.amount.to_le_bytes());
        message.extend_from_slice(&self.nonce.to_le_bytes());
        message.extend_from_slice(&self.fee.to_le_bytes());

        // Hash the message for consistency
        let hash = Sha256::digest(&message);
        hash.to_vec()
    }
}

impl From<&str> for ValidationError {
    fn from(msg: &str) -> Self {
        ValidationError::SignatureError(msg.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    Valid,
    /// Nonce non valido (troppo basso o già used)
    InvalidNonce {
        expected: u64,
        actual: u64,
    },
    InvalidSignature(String),
    /// Balance insufficient per la transazione
    InsufficientBalance {
        required: u128,
        available: u128,
    },
    /// Transazione già eseguita (replay attack)
    ReplayAttack {
        tx_hash: [u8; 32],
        block_height: u64,
    },
    /// Fee troppo bassa rispetto ai limiti
    FeeTooLow {
        required: u64,
        actual: u64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Signature validation failed: {0}")]
    SignatureError(String),

    #[error("Nonce validation failed: {0}")]
    NonceError(String),

    #[error("Balance validation failed: {0}")]
    BalanceError(String),

    #[error("Replay detection failed: {0}")]
    ReplayError(String),

    #[error("Fee validation failed: {0}")]
    FeeError(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] anyhow::Error),
}

/// Configurazione per TransactionValidator
#[derive(Debug, Clone)]
pub struct ValidatorConfig {
    /// Dimensione cache per replay prevention (default: 100_000)
    pub replay_cache_size: usize,
    /// Fee minima accettata (default: 100_000_000_000_000)
    pub min_fee: u64,
    /// Massimo nonce gap consentito (default: 1000)
    pub max_nonce_gap: u64,
    /// Abilita strict mode per testing (default: false)
    pub strict_mode: bool,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            replay_cache_size: 100_000,
            min_fee: 50_000_000_000_000, // 0.00005 token (più basso per test)
            max_nonce_gap: 3000,
            strict_mode: false,
        }
    }
}

///
/// - Replay prevention tramite cache
/// - Balance sufficiency check
pub struct TransactionValidator {
    /// Storage per accesso account state
    storage: Arc<Storage>,
    /// Cache per replay prevention (tx_hash -> block_height)
    replay_cache: Arc<std::sync::Mutex<LruCache<[u8; 32], u64>>>,
    /// Cache nonce per sender per performance (sender_address -> last_nonce)
    nonce_cache: Arc<std::sync::Mutex<HashMap<Vec<u8>, u64>>>,
    config: ValidatorConfig,
}

impl TransactionValidator {
    pub fn new(storage: Arc<Storage>, config: ValidatorConfig) -> Self {
        Self {
            storage,
            replay_cache: Arc::new(std::sync::Mutex::new(LruCache::new(
                NonZeroUsize::new(config.replay_cache_size).unwrap(),
            ))),
            nonce_cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Creates un TransactionValidator con configurazione default
    pub fn new_default(storage: Arc<Storage>) -> Self {
        Self::new(storage, ValidatorConfig::default())
    }

    pub fn validate_transaction(
        &self,
        tx: &SignedTx,
        current_block_height: u64,
    ) -> Result<ValidationResult, ValidationError> {
        // Step 1: Validazione firma
        self.validate_signature(tx)?;

        // Step 2: Validazione fee
        self.validate_fee(tx)?;

        // Step 3: Replay prevention
        self.check_replay_attack(tx, current_block_height)?;

        // Step 4: Validazione nonce sequence
        let nonce_validation = self.validate_nonce_sequence(tx, current_block_height)?;
        if let ValidationResult::InvalidNonce { .. } = nonce_validation {
            return Ok(nonce_validation);
        }

        // Step 5: Validazione balance
        self.validate_balance(tx)?;

        Ok(ValidationResult::Valid)
    }

    pub fn validate_transaction_batch(
        &self,
        txs: &[SignedTx],
        current_block_height: u64,
    ) -> Result<Vec<ValidationResult>, ValidationError> {
        let mut results = Vec::with_capacity(txs.len());
        let mut valid_txs = Vec::new();

        // Step 1: Validazione firme in batch (parallela se possibile)
        for tx in txs {
            match self.validate_signature(tx) {
                Ok(()) => valid_txs.push(tx),
                Err(e) => {
                    results.push(ValidationResult::InvalidSignature(e.to_string()));
                    continue;
                }
            }
        }

        // Step 2: Validazione fee in batch
        for tx in &valid_txs {
            match self.validate_fee(tx) {
                Ok(()) => (),
                Err(_e) => {
                    results.push(ValidationResult::FeeTooLow {
                        required: self.config.min_fee,
                        actual: tx.fee,
                    });
                }
            }
        }

        for tx in &valid_txs {
            match self.validate_transaction(tx, current_block_height) {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Convert error to appropriate ValidationResult
                    if e.to_string().contains("signature") {
                        results.push(ValidationResult::InvalidSignature(e.to_string()));
                    } else if e.to_string().contains("nonce") {
                        results.push(ValidationResult::InvalidNonce {
                            expected: 0,
                            actual: 0,
                        });
                    } else if e.to_string().contains("balance") {
                        results.push(ValidationResult::InsufficientBalance {
                            required: 0,
                            available: 0,
                        });
                    } else if e.to_string().contains("replay") {
                        results.push(ValidationResult::ReplayAttack {
                            tx_hash: [0; 32],
                            block_height: 0,
                        });
                    } else if e.to_string().contains("fee") {
                        results.push(ValidationResult::FeeTooLow {
                            required: 0,
                            actual: 0,
                        });
                    }
                }
            }
        }

        Ok(results)
    }

    fn validate_signature(&self, tx: &SignedTx) -> Result<(), ValidationError> {
        if tx.pre_verified {
            return Ok(());
        }

        // Converti pubkey e sig ai tipi corretti - handle Vec<u8> to [u8; N] conversion
        let pubkey_array: [u8; 32] = tx.pubkey.as_slice().try_into().map_err(|_| {
            ValidationError::SignatureError("Invalid public key length".to_string())
        })?;
        let pubkey = DalekPublicKey::from_bytes(&pubkey_array)
            .map_err(|e| ValidationError::SignatureError(format!("Invalid public key: {}", e)))?;

        let sig_array: [u8; 64] =
            tx.sig.as_slice().try_into().map_err(|_| {
                ValidationError::SignatureError("Invalid signature length".to_string())
            })?;
        let sig = DalekSignature::from_bytes(&sig_array);

        if pubkey.verify_strict(&tx.message(), &sig).is_ok() {
            Ok(())
        } else {
            Err(ValidationError::SignatureError(
                "Invalid signature".to_string(),
            ))
        }
    }

    fn validate_fee(&self, tx: &SignedTx) -> Result<(), ValidationError> {
        let fee = tx.fee;
        if fee < self.config.min_fee {
            return Err(ValidationError::FeeError(format!(
                "Fee too low: {} < {}",
                fee, self.config.min_fee
            )));
        }
        Ok(())
    }

    fn check_replay_attack(
        &self,
        tx: &SignedTx,
        _current_block_height: u64,
    ) -> Result<(), ValidationError> {
        let tx_hash = self.compute_tx_hash(tx);

        if let Ok(mut cache) = self.replay_cache.lock() {
            if let Some(&executed_height) = cache.get(&tx_hash) {
                return Err(ValidationError::ReplayError(format!(
                    "Transaction already executed at block height {}",
                    executed_height
                )));
            }
        }

        Ok(())
    }

    fn validate_nonce_sequence(
        &self,
        tx: &SignedTx,
        _current_block_height: u64,
    ) -> Result<ValidationResult, ValidationError> {
        let sender_address = tx.from.as_bytes();
        let expected_nonce = self.get_expected_nonce(sender_address)?;

        // In una blockchain corretta, solo nonce == expected_nonce è valido
        if tx.nonce != expected_nonce {
            return Ok(ValidationResult::InvalidNonce {
                expected: expected_nonce,
                actual: tx.nonce,
            });
        }

        Ok(ValidationResult::Valid)
    }

    fn validate_balance(&self, tx: &SignedTx) -> Result<(), ValidationError> {
        let sender_address = tx.from.as_bytes();
        let account_bytes = self.storage.get_account(sender_address)?;

        let account: savitri_core::Account = if let Some(bytes) = account_bytes {
            savitri_core::Account::decode(&bytes)?
        } else {
            return Err(ValidationError::BalanceError(
                "Account does not exist".to_string(),
            ));
        };

        let total_required = tx.amount + tx.fee;
        if account.balance < total_required as u128 {
            return Err(ValidationError::BalanceError(format!(
                "Insufficient balance: required {}, available {}",
                total_required, account.balance
            )));
        }

        Ok(())
    }

    /// Ottiene il nonce atteso per un indirizzo sender
    fn get_expected_nonce(&self, sender_address: &[u8]) -> Result<u64, ValidationError> {
        // Prima controlla cache nonce
        if let Ok(cache) = self.nonce_cache.lock() {
            if let Some(&nonce) = cache.get(sender_address) {
                return Ok(nonce);
            }
        }

        // Se non in cache, leggi da storage
        let account_bytes = self.storage.get_account(sender_address)?;

        let account: savitri_core::Account = if let Some(bytes) = account_bytes {
            savitri_core::Account::decode(&bytes)?
        } else {
            // Account doesn't exist - return 0 as default nonce
            savitri_core::Account {
                balance: 0,
                nonce: 0,
            }
        };

        if let Ok(mut cache) = self.nonce_cache.lock() {
            cache.insert(sender_address.to_vec(), account.nonce);
        }

        Ok(account.nonce)
    }

    fn compute_tx_hash(&self, tx: &SignedTx) -> [u8; 32] {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();

        tx.from.hash(&mut hasher);
        tx.to.hash(&mut hasher);
        tx.amount.hash(&mut hasher);
        tx.nonce.hash(&mut hasher);
        tx.fee.hash(&mut hasher);

        let hash = hasher.finish();

        // Converti in [u8; 32] per consistenza
        let mut result = [0u8; 32];
        let hash_bytes = hash.to_le_bytes();
        result[..hash_bytes.len()].copy_from_slice(&hash_bytes);

        result
    }

    /// Registra una transazione come eseguita (per replay prevention)
    pub fn mark_transaction_executed(
        &self,
        tx: &SignedTx,
        block_height: u64,
    ) -> Result<(), ValidationError> {
        let tx_hash = self.compute_tx_hash(tx);

        if let Ok(mut cache) = self.replay_cache.lock() {
            cache.put(tx_hash, block_height);
        }

        let sender_address = tx.from.as_bytes();
        if let Ok(mut cache) = self.nonce_cache.lock() {
            cache.insert(sender_address.to_vec(), tx.nonce + 1);
        }

        Ok(())
    }

    /// Registra un batch di transazioni come eseguite
    pub fn mark_transactions_executed(
        &self,
        txs: &[SignedTx],
        block_height: u64,
    ) -> Result<(), ValidationError> {
        for tx in txs {
            self.mark_transaction_executed(tx, block_height)?;
        }
        Ok(())
    }

    /// Pulisce vecchie entry dalle cache
    pub fn cleanup(&self, max_block_age: u64) -> Result<(), ValidationError> {
        // Pulisce replay cache
        if let Ok(mut cache) = self.replay_cache.lock() {
            let current_height = self.get_current_block_height()?;
            let cutoff_height = current_height.saturating_sub(max_block_age);

            // Rimuovi entry più vecchie di max_block_age
            let mut new_cache = LruCache::new(NonZeroUsize::new(cache.cap().get()).unwrap());
            for (tx_hash, executed_height) in cache.iter() {
                if *executed_height >= cutoff_height {
                    new_cache.put(*tx_hash, *executed_height);
                }
            }
            *cache = new_cache;
        }

        Ok(())
    }

    /// Ottiene l'altezza of the blocco corrente
    fn get_current_block_height(&self) -> Result<u64, ValidationError> {
        // Try to get current block height from chain head
        // This is a simplified implementation - in production this would
        // read from the blockchain state directly
        if let Ok(Some(head_bytes)) = self.storage.get_chain_head() {
            // Parse height from chain head data (first 8 bytes as u64)
            if head_bytes.len() >= 8 {
                let height = u64::from_le_bytes([
                    head_bytes[0],
                    head_bytes[1],
                    head_bytes[2],
                    head_bytes[3],
                    head_bytes[4],
                    head_bytes[5],
                    head_bytes[6],
                    head_bytes[7],
                ]);
                return Ok(height);
            }
        }
        // Fallback to 0 if chain head not available
        Ok(0)
    }

    pub fn get_stats(&self) -> ValidatorStats {
        let replay_cache_size = self
            .replay_cache
            .lock()
            .map(|cache| cache.len())
            .unwrap_or(0);

        let nonce_cache_size = self
            .nonce_cache
            .lock()
            .map(|cache| cache.len())
            .unwrap_or(0);

        ValidatorStats {
            replay_cache_size,
            nonce_cache_size,
        }
    }

    /// Resetta le cache (utile per testing)
    pub fn reset(&self) -> Result<(), ValidationError> {
        if let Ok(mut cache) = self.replay_cache.lock() {
            cache.clear();
        }

        if let Ok(mut cache) = self.nonce_cache.lock() {
            cache.clear();
        }

        Ok(())
    }
}

/// Statistiche of the TransactionValidator
#[derive(Debug, Clone)]
pub struct ValidatorStats {
    pub replay_cache_size: usize,
    pub nonce_cache_size: usize,
}

impl Default for ValidatorStats {
    fn default() -> Self {
        Self {
            replay_cache_size: 0,
            nonce_cache_size: 0,
        }
    }
}

