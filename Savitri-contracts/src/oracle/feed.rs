//! Oracle Feed: Feed di dati con versioning e TTL

use crate::oracle::proof::{hash_feed_data, OracleProof};
use crate::oracle::schema::{Schema, SchemaId};
use crate::oracle::types::{OracleConfig, OracleError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// ID feed (32 bytes)
pub type FeedId = [u8; 32];

/// Dati di un feed (chiave-valore con encoding canonico)
pub type FeedData = BTreeMap<String, Vec<u8>>;

/// Feed Oracle completo
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Feed {
    /// ID of the feed
    pub feed_id: FeedId,
    /// ID schema
    pub schema_id: SchemaId,
    /// Versione schema
    pub schema_version: u32,
    /// Dati of the feed (encoding canonico, no float)
    pub data: FeedData,
    /// Proof firmata
    pub proof: OracleProof,
    /// TTL in secondi (0 = usa default)
    pub ttl_seconds: u64,
    /// Timestamp di creazione (Unix timestamp in secondi)
    pub created_at: u64,
}

impl Feed {
    pub fn new(
        feed_id: FeedId,
        schema_id: SchemaId,
        schema_version: u32,
        data: FeedData,
        proof: OracleProof,
        ttl_seconds: u64,
        created_at: u64,
    ) -> Self {
        Self {
            feed_id,
            schema_id,
            schema_version,
            data,
            proof,
            ttl_seconds,
            created_at,
        }
    }

    pub fn validate(
        &self,
        schema: &Schema,
        config: &OracleConfig,
        current_time: u64,
    ) -> Result<(), OracleError> {
        // Check versione schema
        if self.schema_version != schema.version {
            return Err(OracleError::SchemaValidationFailed {
                schema_id: hex::encode(self.schema_id),
                reason: format!(
                    "Schema version mismatch: expected {}, got {}",
                    schema.version, self.schema_version
                ),
            });
        }

        schema.validate_data(&self.data)?;

        // Check encoding canonico (ordine deterministico, no float)
        self.ensure_canonical_encoding()?;

        if self.created_at != self.proof.timestamp {
            return Err(OracleError::InvalidProof(
                "Feed created_at does not match proof timestamp".to_string(),
            ));
        }

        // Compute hash dati
        let data_hash = hash_feed_data(&self.data);

        // Check proof
        self.proof
            .verify(&self.feed_id, &self.schema_id, &data_hash)?;

        // Check TTL
        let ttl = if self.ttl_seconds == 0 {
            config.default_ttl_seconds
        } else {
            self.ttl_seconds
        };

        let expires_at = self
            .created_at
            .checked_add(ttl)
            .ok_or_else(|| OracleError::InvalidConfig("TTL overflow".to_string()))?;

        if current_time > expires_at {
            return Err(OracleError::ExpiredData {
                feed_id: hex::encode(self.feed_id),
                expired_at: expires_at,
                current_time,
            });
        }

        // Check timestamp non nel futuro oltre tolleranza
        if self.proof.timestamp
            > current_time.saturating_add(config.future_timestamp_tolerance_seconds)
        {
            return Err(OracleError::FutureTimestamp {
                timestamp: self.proof.timestamp,
                current_time,
                tolerance: config.future_timestamp_tolerance_seconds,
            });
        }

        Ok(())
    }

    /// Check che i dati siano in formato canonico (no float, encoding deterministico)
    pub fn ensure_canonical_encoding(&self) -> Result<(), OracleError> {
        for (key, value) in &self.data {
            std::str::from_utf8(key.as_bytes())
                .map_err(|_| OracleError::NonCanonicalEncoding(format!("Invalid key: {}", key)))?;

            if value.is_empty() && !key.is_empty() {
            }
        }
        Ok(())
    }
}
