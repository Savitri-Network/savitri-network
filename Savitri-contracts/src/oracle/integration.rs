//! Oracle Integration: Helper per integrazione nel core blockchain
//!

use crate::oracle::feed::Feed;
use crate::oracle::proof::{hash_feed_data, ProofVerifier};
use crate::oracle::schema::{Schema, SchemaRegistry};
use crate::oracle::types::{OracleConfig, OracleError};
use crate::p2p::messages::ConsensusCertificate;
use crate::storage::{oracle::OracleAnchor, Storage};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleFeedTx {
    pub feed: Feed,
    /// Caller address (32 bytes) - chi sta submittendo il feed
    pub caller: Vec<u8>,
}

impl OracleFeedTx {
    pub fn new(feed: Feed, caller: Vec<u8>) -> Self {
        Self { feed, caller }
    }

    /// Serializza in bytes per trasmissione/storage
    pub fn to_bytes(&self) -> Result<Vec<u8>, OracleError> {
        bincode::serialize(self)
            .map_err(|e| OracleError::NonCanonicalEncoding(format!("Serialization error: {}", e)))
    }

    /// Maximum allowed size for OracleFeedTx deserialization (1 MB).
    const MAX_DESERIALIZE_SIZE: usize = 1 * 1024 * 1024;

    /// Deserializza da bytes with size limit.
    ///
    /// SECURITY (AUDIT-020): Rejects payloads larger than 1 MB.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, OracleError> {
        if bytes.len() > Self::MAX_DESERIALIZE_SIZE {
            return Err(OracleError::NonCanonicalEncoding(format!(
                "Data too large for deserialization: {} bytes (max {})",
                bytes.len(),
                Self::MAX_DESERIALIZE_SIZE
            )));
        }
        bincode::deserialize(bytes)
            .map_err(|e| OracleError::NonCanonicalEncoding(format!("Deserialization error: {}", e)))
    }
}

#[derive(Debug, Clone)]
pub enum OracleValidationResult {
    /// Validazione passata
    Valid,
    /// Validazione fallita con errore
    Invalid(OracleError),
    NotOracle,
}

pub struct OracleValidator {
    /// Schema registry (thread-safe)
    schema_registry: Arc<RwLock<SchemaRegistry>>,
    /// Proof verifier con anti-replay (thread-safe)
    proof_verifier: Arc<RwLock<ProofVerifier>>,
    /// Configurazione Oracle
    config: OracleConfig,
}

impl OracleValidator {
    pub fn new(config: OracleConfig) -> Self {
        Self {
            schema_registry: Arc::new(RwLock::new(SchemaRegistry::new())),
            proof_verifier: Arc::new(RwLock::new(ProofVerifier::new())),
            config,
        }
    }

    /// Creates un OracleValidator con configurazione di default
    pub fn default() -> Self {
        Self::new(OracleConfig::default())
    }

    /// Ottiene il timestamp corrente in secondi Unix
    fn current_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    ///
    /// 1. Validazione schema
    /// 3. Validazione TTL (non scaduto)
    /// 4. Validazione timestamp futuro (entro tolleranza)
    /// 5. Check anti-replay (sequence number)
    /// 6. Check canonical encoding
    ///
    /// # Arguments
    /// * `storage` - Storage per lookup schema persistiti
    ///
    /// # Returns
    pub fn validate_oracle_tx(
        &self,
        tx: &OracleFeedTx,
        storage: Option<&Storage>,
    ) -> OracleValidationResult {
        let current_time = Self::current_time();

        // 1. Risolvi schema (da storage o registry predefinito)
        let schema = match self.resolve_schema(&tx.feed.schema_id, storage) {
            Ok(s) => s,
            Err(e) => return OracleValidationResult::Invalid(e),
        };

        if let Err(e) = tx.feed.validate(&schema, &self.config, current_time) {
            return OracleValidationResult::Invalid(e);
        }

        // 3. Check anti-replay con proof verifier
        let data_hash = hash_feed_data(&tx.feed.data);
        {
            let mut verifier = match self.proof_verifier.write() {
                Ok(v) => v,
                Err(_) => {
                    return OracleValidationResult::Invalid(OracleError::InvalidProof(
                        "Proof verifier lock poisoned".to_string(),
                    ))
                }
            };

            if let Err(e) = verifier.verify_proof(
                &tx.feed.proof,
                &tx.feed.feed_id,
                &tx.feed.schema_id,
                &data_hash,
                current_time,
                self.config.future_timestamp_tolerance_seconds,
            ) {
                return OracleValidationResult::Invalid(e);
            }
        }

        OracleValidationResult::Valid
    }

    /// Risolve uno schema da storage o registry predefinito
    fn resolve_schema(
        &self,
        schema_id: &[u8; 32],
        storage: Option<&Storage>,
    ) -> Result<Schema, OracleError> {
        // Prima prova storage (se disponibile)
        if let Some(s) = storage {
            if let Ok(Some(schema_bytes)) = s.get_oracle_schema(schema_id) {
                const MAX_SCHEMA_SIZE: usize = 1 * 1024 * 1024;
                if schema_bytes.len() > MAX_SCHEMA_SIZE {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: hex::encode(schema_id),
                        reason: format!(
                            "Schema data too large: {} bytes (max {})",
                            schema_bytes.len(),
                            MAX_SCHEMA_SIZE
                        ),
                    });
                }
                let schema: Schema = bincode::deserialize(&schema_bytes).map_err(|e| {
                    OracleError::SchemaValidationFailed {
                        schema_id: hex::encode(schema_id),
                        reason: format!("Schema decode error: {}", e),
                    }
                })?;
                return Ok(schema);
            }
        }

        // Fallback a registry predefinito
        let registry = self
            .schema_registry
            .read()
            .map_err(|_| OracleError::SchemaNotFound("Registry lock poisoned".to_string()))?;

        registry
            .get(schema_id)
            .cloned()
            .ok_or_else(|| OracleError::SchemaNotFound(hex::encode(schema_id)))
    }

    ///
    /// Runs controlli veloci without accesso a storage:
    /// - Formato valido (deserializzazione)
    /// - TTL non ovviamente scaduto
    /// - Timestamp non nel futuro lontano
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn prevalidate_oracle_tx(&self, tx_bytes: &[u8]) -> Result<OracleFeedTx, OracleError> {
        // 1. Deserializza
        let tx = OracleFeedTx::from_bytes(tx_bytes)?;

        // 2. Controlli veloci
        let current_time = Self::current_time();

        // TTL check rapido (without schema lookup)
        let ttl = if tx.feed.ttl_seconds == 0 {
            self.config.default_ttl_seconds
        } else {
            tx.feed.ttl_seconds
        };

        let expires_at = tx
            .feed
            .created_at
            .checked_add(ttl)
            .ok_or_else(|| OracleError::InvalidConfig("TTL overflow".to_string()))?;

        if current_time > expires_at {
            return Err(OracleError::ExpiredData {
                feed_id: hex::encode(tx.feed.feed_id),
                expired_at: expires_at,
                current_time,
            });
        }

        // Future timestamp check
        if tx.feed.proof.timestamp
            > current_time.saturating_add(self.config.future_timestamp_tolerance_seconds)
        {
            return Err(OracleError::FutureTimestamp {
                timestamp: tx.feed.proof.timestamp,
                current_time,
                tolerance: self.config.future_timestamp_tolerance_seconds,
            });
        }

        // Canonical encoding check
        tx.feed.ensure_canonical_encoding()?;

        Ok(tx)
    }
}

/// Helper per verificare se dei bytes rappresentano una OracleFeedTx
pub fn is_oracle_feed_tx(tx_bytes: &[u8]) -> bool {
    // Prova a deserializzare come OracleFeedTx
    OracleFeedTx::from_bytes(tx_bytes).is_ok()
}

/// Compute hash deterministico per cross-platform consistency
/// Usa SHA-512 con domain separation
pub fn compute_deterministic_hash(data: &[u8]) -> [u8; 64] {
    use sha2::{Digest, Sha512};

    let mut hasher = Sha512::new();
    hasher.update(b"SAVITRI_ORACLE_DETERMINISTIC_V1");
    hasher.update(data);

    let result = hasher.finalize();
    let mut hash = [0u8; 64];
    hash.copy_from_slice(&result);
    hash
}

/// Ancora opzionale: persiste un ConsensusCertificate come anchor per un feed
pub fn anchor_feed_with_certificate(
    storage: &Storage,
    feed_id: &[u8; 32],
    certificate: &ConsensusCertificate,
) -> Result<(), OracleError> {
    validate_certificate_anchor(certificate)?;
    let block_hash: [u8; 64] = certificate
        .block_hash
        .as_slice()
        .try_into()
        .map_err(|_| OracleError::InvalidAnchor("block_hash must be 64 bytes".to_string()))?;
    let anchor_hash = compute_anchor_hash(&block_hash, certificate);
    let cert_bytes = bincode::serialize(certificate)
        .map_err(|e| OracleError::InvalidAnchor(format!("certificate serialize error: {}", e)))?;
    let anchor = OracleAnchor {
        feed_id: feed_id.to_vec(),
        block_height: certificate.height,
        block_hash: certificate.block_hash.clone(),
        timestamp: certificate.timestamp,
        data_hash: block_hash[..32].to_vec(),
        certificate: cert_bytes,
        anchor_hash: anchor_hash.to_vec(),
    };
    let anchor_bytes = bincode::serialize(&anchor)
        .map_err(|e| OracleError::InvalidAnchor(format!("anchor serialize error: {}", e)))?;
    storage
        .put_oracle_anchor(feed_id, &anchor_bytes)
        .map_err(|e| OracleError::InvalidAnchor(format!("{}", e)))
}

fn validate_certificate_anchor(cert: &ConsensusCertificate) -> Result<(), OracleError> {
    if cert.version != ConsensusCertificate::VERSION {
        return Err(OracleError::InvalidAnchor(format!(
            "unsupported certificate version {}",
            cert.version
        )));
    }
    if cert.voters.is_empty() {
        return Err(OracleError::InvalidAnchor(
            "certificate voters cannot be empty".to_string(),
        ));
    }
    if cert.aggregated_signature.is_empty() {
        return Err(OracleError::InvalidAnchor(
            "certificate aggregated_signature cannot be empty".to_string(),
        ));
    }
    Ok(())
}

/// Compute l'anchor hash per batch oracle: H("ORACLE_ANCHOR_V1" || block_hash || cert_bytes)
pub fn compute_anchor_hash(block_hash: &[u8; 64], cert: &ConsensusCertificate) -> [u8; 32] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(b"ORACLE_ANCHOR_V1");
    hasher.update(block_hash);
    if let Ok(bytes) = bincode::serialize(cert) {
        hasher.update(bytes);
    }
    let out = hasher.finalize();
    let mut h = [0u8; 32];
    h.copy_from_slice(&out[..32]);
    h
}

/// Check che un valore non contenga float/double
pub fn ensure_no_float_encoding(value: &[u8]) -> Result<(), OracleError> {
    // Float IEEE 754 patterns detection (heuristic)
    // Single precision (4 bytes): exponent in bits 23-30
    // Double precision (8 bytes): exponent in bits 52-62
    //

    if value.len() < 4 {
        return Ok(());
    }

    // Controlla pattern per single precision (4 bytes)
    if value.len() >= 4 {
        let bytes = &value[0..4];
        let bits = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Estrai exponent (bits 23-30)
        let exponent = (bits >> 23) & 0xFF;

        // Controlla se è un pattern float valido
        // Exponent 0xFF = NaN/Inf, 0x00 = subnormal/zero, altri = normali
        if exponent != 0x00 {
            let _mantissa = bits & 0x7FFFFF;

            // Per ora, consideriamo sospetti solo i pattern ovvi di NaN/Inf
            if exponent == 0xFF {
                return Err(OracleError::NonCanonicalEncoding(
                    "Float encoding detected (NaN/Inf pattern)".to_string(),
                ));
            }
        }
    }

    // Controlla pattern per double precision (8 bytes)
    if value.len() >= 8 {
        let bytes = &value[0..8];
        let bits = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        // Estrai exponent (bits 52-62)
        let exponent = (bits >> 52) & 0x7FF;

        // Controlla se è un pattern double valido
        // Exponent 0x7FF = NaN/Inf, 0x000 = subnormal/zero, altri = normali
        if exponent != 0x000 {
            let _mantissa = bits & 0xFFFFFFFFFFFFF;

            // Consideriamo sospetti i pattern NaN/Inf ovvi
            if exponent == 0x7FF {
                return Err(OracleError::NonCanonicalEncoding(
                    "Double encoding detected (NaN/Inf pattern)".to_string(),
                ));
            }
        }
    }

    // Controlla pattern comuni di stringhe float (es. "1.23", "4.56e-7")
    if let Ok(value_str) = std::str::from_utf8(value) {
        // Pattern per decimali con punto
        if value_str.contains('.') {
            // Check se sembra un numero decimale valido
            let parts: Vec<&str> = value_str.split('.').collect();
            if parts.len() == 2 {
                if parts[0].chars().all(|c| c.is_ascii_digit() || c == '-')
                    && parts[1].chars().all(|c| c.is_ascii_digit())
                {
                    return Err(OracleError::NonCanonicalEncoding(
                        "Decimal float encoding detected".to_string(),
                    ));
                }
            }
        }

        // Pattern per notazione scientifica
        if value_str.to_lowercase().contains('e') {
            return Err(OracleError::NonCanonicalEncoding(
                "Scientific notation encoding detected".to_string(),
            ));
        }

        // Pattern per valori float comuni
        let float_indicators = ["inf", "infinity", "nan", "-inf", "+inf"];
        let lower_str = value_str.to_lowercase();
        for indicator in &float_indicators {
            if lower_str.contains(indicator) {
                return Err(OracleError::NonCanonicalEncoding(
                    "Special float value encoding detected".to_string(),
                ));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oracle::proof::OracleProof;
    use crate::oracle::schema::Schema;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;
    use std::collections::BTreeMap;

    fn create_test_feed() -> (Feed, SigningKey) {
        let keypair = SigningKey::generate(&mut OsRng);
        let schema = Schema::price_feed();
        let feed_id = [1u8; 32];
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut data = BTreeMap::new();
        data.insert("asset".to_string(), b"BTC/USD".to_vec());
        data.insert("value".to_string(), 50000000000u64.to_le_bytes().to_vec());
        data.insert("timestamp".to_string(), current_time.to_le_bytes().to_vec());

        let data_hash = hash_feed_data(&data);
        let message =
            OracleProof::create_message(&feed_id, &schema.id, &data_hash, 1, current_time);
        let signature = keypair.sign(&message);

        let proof = OracleProof {
            producer_pubkey: keypair.verifying_key().to_bytes(),
            signature: signature.to_bytes(),
            sequence: 1,
            timestamp: current_time,
        };

        let feed = Feed::new(
            feed_id,
            schema.id,
            schema.version,
            data,
            proof,
            3600,
            current_time,
        );

        (feed, keypair)
    }

    fn create_feed_with_sequence(sequence: u64) -> (Feed, SigningKey) {
        let keypair = SigningKey::generate(&mut OsRng);
        let schema = Schema::price_feed();
        let feed_id = [2u8; 32];
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut data = BTreeMap::new();
        data.insert("asset".to_string(), b"ETH/USD".to_vec());
        data.insert("value".to_string(), 3000000000u64.to_le_bytes().to_vec());
        data.insert("timestamp".to_string(), current_time.to_le_bytes().to_vec());

        let data_hash = hash_feed_data(&data);
        let message =
            OracleProof::create_message(&feed_id, &schema.id, &data_hash, sequence, current_time);
        let signature = keypair.sign(&message);

        let proof = OracleProof {
            producer_pubkey: keypair.verifying_key().to_bytes(),
            signature: signature.to_bytes(),
            sequence,
            timestamp: current_time,
        };

        let feed = Feed::new(
            feed_id,
            schema.id,
            schema.version,
            data,
            proof,
            3600,
            current_time,
        );

        (feed, keypair)
    }

    #[test]
    fn test_oracle_feed_tx_serialization() {
        let (feed, _) = create_test_feed();
        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);

        let bytes = tx.to_bytes().unwrap();
        let decoded = OracleFeedTx::from_bytes(&bytes).unwrap();

        assert_eq!(tx.feed.feed_id, decoded.feed.feed_id);
        assert_eq!(tx.caller, decoded.caller);
    }

    #[test]
    fn test_oracle_validator_valid_tx() {
        let (feed, _) = create_test_feed();
        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);

        let validator = OracleValidator::default();
        let result = validator.validate_oracle_tx(&tx, None);

        match result {
            OracleValidationResult::Valid => {}
            OracleValidationResult::Invalid(e) => panic!("Expected valid, got: {:?}", e),
            OracleValidationResult::NotOracle => panic!("Expected valid, got NotOracle"),
        }
    }

    #[test]
    fn test_oracle_validator_expired_ttl() {
        let keypair = SigningKey::generate(&mut OsRng);
        let schema = Schema::price_feed();
        let feed_id = [1u8; 32];

        // Create feed with timestamp in the past (expired)
        let past_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 7200; // 2 hours ago

        let mut data = BTreeMap::new();
        data.insert("asset".to_string(), b"BTC/USD".to_vec());
        data.insert("value".to_string(), 50000000000u64.to_le_bytes().to_vec());
        data.insert("timestamp".to_string(), past_time.to_le_bytes().to_vec());

        let data_hash = hash_feed_data(&data);
        let message = OracleProof::create_message(&feed_id, &schema.id, &data_hash, 1, past_time);
        let signature = keypair.sign(&message);

        let proof = OracleProof {
            producer_pubkey: keypair.verifying_key().to_bytes(),
            signature: signature.to_bytes(),
            sequence: 1,
            timestamp: past_time,
        };

        let feed = Feed::new(
            feed_id,
            schema.id,
            schema.version,
            data,
            proof,
            3600, // 1 hour TTL
            past_time,
        );

        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);
        let validator = OracleValidator::default();
        let result = validator.validate_oracle_tx(&tx, None);

        match result {
            OracleValidationResult::Invalid(OracleError::ExpiredData { .. }) => {}
            other => panic!("Expected ExpiredData error, got: {:?}", other),
        }
    }

    #[test]
    fn test_oracle_validator_future_timestamp() {
        let keypair = SigningKey::generate(&mut OsRng);
        let schema = Schema::price_feed();
        let feed_id = [1u8; 32];

        // Create feed with timestamp far in the future
        let future_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600; // 1 hour in the future

        let mut data = BTreeMap::new();
        data.insert("asset".to_string(), b"BTC/USD".to_vec());
        data.insert("value".to_string(), 50000000000u64.to_le_bytes().to_vec());
        data.insert("timestamp".to_string(), future_time.to_le_bytes().to_vec());

        let data_hash = hash_feed_data(&data);
        let message = OracleProof::create_message(&feed_id, &schema.id, &data_hash, 1, future_time);
        let signature = keypair.sign(&message);

        let proof = OracleProof {
            producer_pubkey: keypair.verifying_key().to_bytes(),
            signature: signature.to_bytes(),
            sequence: 1,
            timestamp: future_time,
        };

        let feed = Feed::new(
            feed_id,
            schema.id,
            schema.version,
            data,
            proof,
            3600,
            future_time,
        );

        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);
        let validator = OracleValidator::default();
        let result = validator.validate_oracle_tx(&tx, None);

        match result {
            OracleValidationResult::Invalid(OracleError::FutureTimestamp { .. }) => {}
            other => panic!("Expected FutureTimestamp error, got: {:?}", other),
        }
    }

    #[test]
    fn test_prevalidate_oracle_tx() {
        let (feed, _) = create_test_feed();
        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);
        let bytes = tx.to_bytes().unwrap();

        let validator = OracleValidator::default();
        let result = validator.prevalidate_oracle_tx(&bytes);

        assert!(result.is_ok());
    }

    #[test]
    fn test_is_oracle_feed_tx() {
        let (feed, _) = create_test_feed();
        let tx = OracleFeedTx::new(feed, vec![1u8; 32]);
        let bytes = tx.to_bytes().unwrap();

        assert!(is_oracle_feed_tx(&bytes));
        assert!(!is_oracle_feed_tx(b"not an oracle tx"));
    }

    #[test]
    fn test_compute_deterministic_hash() {
        let data = b"test data for hashing";
        let hash1 = compute_deterministic_hash(data);
        let hash2 = compute_deterministic_hash(data);

        // Same input should produce same hash
        assert_eq!(hash1, hash2);

        // Different input should produce different hash
        let hash3 = compute_deterministic_hash(b"different data");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_oracle_validator_replay_attack_same_sequence() {
        let (feed, _) = create_feed_with_sequence(1);
        let tx = OracleFeedTx::new(feed.clone(), vec![1u8; 32]);
        let validator = OracleValidator::default();

        let first = validator.validate_oracle_tx(&tx, None);
        assert!(matches!(first, OracleValidationResult::Valid));

        let second = validator.validate_oracle_tx(&tx, None);
        match second {
            OracleValidationResult::Invalid(OracleError::ReplayAttack { .. }) => {}
            other => panic!("expected replay attack, got {:?}", other),
        }
    }

    #[test]
    fn test_oracle_validator_invalid_signature_rejected() {
        let (mut feed, _) = create_feed_with_sequence(5);

        // Corrupt signature
        feed.proof.signature[0] ^= 0xFF;

        let tx = OracleFeedTx::new(feed, vec![2u8; 32]);
        let validator = OracleValidator::default();
        let result = validator.validate_oracle_tx(&tx, None);
        match result {
            OracleValidationResult::Invalid(OracleError::InvalidProof(_)) => {}
            other => panic!("expected invalid proof, got {:?}", other),
        }
    }
}
