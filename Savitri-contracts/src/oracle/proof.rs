//! Oracle Proof: Sistema di proof firmate con domain separation e anti-replay

use crate::oracle::types::OracleError;
use ed25519_dalek::{Signature, Verifier, VerifyingKey as PublicKey};
use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};

/// Domain separation prefix per Oracle proof
const ORACLE_PROOF_DOMAIN: &[u8] = b"SAVITRI_ORACLE_PROOF_V1";

/// Proof per un feed Oracle
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleProof {
    /// Public key of the producer (32 bytes)
    pub producer_pubkey: [u8; 32],
    #[serde(with = "serde_big_array::BigArray")]
    pub signature: [u8; 64],
    /// Sequence/nonce per anti-replay (u64)
    pub sequence: u64,
    pub timestamp: u64,
}

impl OracleProof {
    /// Creates il messaggio da firmare per la proof
    ///
    /// Format deterministico con domain separation:
    /// ORACLE_PROOF_DOMAIN || feed_id || schema_id || data_hash || sequence || timestamp
    pub fn create_message(
        feed_id: &[u8],
        schema_id: &[u8; 32],
        data_hash: &[u8; 32],
        sequence: u64,
        timestamp: u64,
    ) -> Vec<u8> {
        let mut msg =
            Vec::with_capacity(ORACLE_PROOF_DOMAIN.len() + feed_id.len() + 32 + 32 + 8 + 8);
        msg.extend_from_slice(ORACLE_PROOF_DOMAIN);
        msg.extend_from_slice(feed_id);
        msg.extend_from_slice(schema_id);
        msg.extend_from_slice(data_hash);
        msg.extend_from_slice(&sequence.to_le_bytes());
        msg.extend_from_slice(&timestamp.to_le_bytes());
        msg
    }

    /// Check la proof
    pub fn verify(
        &self,
        feed_id: &[u8],
        schema_id: &[u8; 32],
        data_hash: &[u8; 32],
    ) -> Result<(), OracleError> {
        // Check formato pubkey
        let pubkey = PublicKey::from_bytes(&self.producer_pubkey)
            .map_err(|e| OracleError::InvalidProof(format!("Invalid public key: {}", e)))?;

        // Check formato signature
        let sig = Signature::try_from(&self.signature)
            .map_err(|e| OracleError::InvalidProof(format!("Invalid signature: {}", e)))?;

        // Creates messaggio
        let message =
            Self::create_message(feed_id, schema_id, data_hash, self.sequence, self.timestamp);

        // Check firma
        if pubkey.verify(&message, &sig).is_ok() {
        } else {
            return Err(OracleError::InvalidProof(
                "Signature verification failed".to_string(),
            ));
        }

        Ok(())
    }
}

///
/// SECURITY FIX: Le sequence are ora persistite in RocksDB tramite
/// `Storage::put_oracle_max_sequence()` / `get_oracle_max_sequence()`.
/// Without persistenza, un restart of the nodo annullava tutta la protezione
/// anti-replay, permettendo di re-submittar proof già processate.
pub struct ProofVerifier {
    cache: std::collections::BTreeMap<Vec<u8>, u64>,
}

impl ProofVerifier {
    pub fn new() -> Self {
        Self {
            cache: std::collections::BTreeMap::new(),
        }
    }

    /// Check una proof e controlla anti-replay (versione con storage-backed persistence)
    pub fn verify_proof_with_storage(
        &mut self,
        proof: &OracleProof,
        feed_id: &[u8],
        schema_id: &[u8; 32],
        data_hash: &[u8; 32],
        current_time: u64,
        future_tolerance: u64,
        storage: &savitri_storage::storage::Storage,
    ) -> Result<(), OracleError> {
        // Check firma
        proof.verify(feed_id, schema_id, data_hash)?;

        // Check timestamp (non nel futuro oltre tolleranza)
        if proof.timestamp > current_time.saturating_add(future_tolerance) {
            return Err(OracleError::FutureTimestamp {
                timestamp: proof.timestamp,
                current_time,
                tolerance: future_tolerance,
            });
        }

        // Prima controlla cache in-memory, poi fallback su storage
        let max_seen = match self.cache.get(feed_id) {
            Some(&cached) => cached,
            None => {
                // Carica da storage persistente
                let stored = storage
                    .get_oracle_max_sequence(feed_id)
                    .map_err(|e| {
                        OracleError::StorageError(format!(
                            "Failed to read max sequence for feed {}: {}",
                            hex::encode(feed_id),
                            e
                        ))
                    })?
                    .unwrap_or(0);
                self.cache.insert(feed_id.to_vec(), stored);
                stored
            }
        };

        if proof.sequence <= max_seen {
            return Err(OracleError::ReplayAttack {
                feed_id: hex::encode(feed_id),
                sequence: proof.sequence,
            });
        }

        self.cache.insert(feed_id.to_vec(), proof.sequence);
        storage
            .put_oracle_max_sequence(feed_id, proof.sequence)
            .map_err(|e| {
                OracleError::StorageError(format!(
                    "Failed to persist max sequence for feed {}: {}",
                    hex::encode(feed_id),
                    e
                ))
            })?;

        Ok(())
    }

    pub fn verify_proof(
        &mut self,
        proof: &OracleProof,
        feed_id: &[u8],
        schema_id: &[u8; 32],
        data_hash: &[u8; 32],
        current_time: u64,
        future_tolerance: u64,
    ) -> Result<(), OracleError> {
        // Check firma
        proof.verify(feed_id, schema_id, data_hash)?;

        // Check timestamp
        if proof.timestamp > current_time.saturating_add(future_tolerance) {
            return Err(OracleError::FutureTimestamp {
                timestamp: proof.timestamp,
                current_time,
                tolerance: future_tolerance,
            });
        }

        // Check anti-replay (solo in-memory)
        let max_seen = self.cache.get(feed_id).copied().unwrap_or(0);
        if proof.sequence <= max_seen {
            return Err(OracleError::ReplayAttack {
                feed_id: hex::encode(feed_id),
                sequence: proof.sequence,
            });
        }

        self.cache.insert(feed_id.to_vec(), proof.sequence);
        Ok(())
    }

    /// Resetta il tracking (per test)
    #[cfg(test)]
    pub fn reset(&mut self) {
        self.cache.clear();
    }
}

impl Default for ProofVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute hash deterministico dei dati of the feed
pub fn hash_feed_data(data: &std::collections::BTreeMap<String, Vec<u8>>) -> [u8; 32] {
    let mut hasher = Sha512::new();

    for (key, value) in data {
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value);
    }

    let hash = hasher.finalize();
    let mut result = [0u8; 32];
    result.copy_from_slice(&hash[..32]);
    result
}
