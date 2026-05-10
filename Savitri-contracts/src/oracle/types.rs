//! Oracle Types: Tipi base per il framework Oracle

use hex;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Errori of the framework Oracle
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleError {
    /// Feed non trovato
    FeedNotFound(String),
    /// Schema non trovato
    SchemaNotFound(String),
    InvalidProof(String),
    /// Ancora/certificato non valido
    InvalidAnchor(String),
    /// Dati scaduti (TTL expired)
    ExpiredData {
        feed_id: String,
        expired_at: u64,
        current_time: u64,
    },
    /// Timestamp nel futuro oltre tolleranza
    FutureTimestamp {
        timestamp: u64,
        current_time: u64,
        tolerance: u64,
    },
    /// Sequence/nonce già utilizzato (replay attack)
    ReplayAttack {
        feed_id: String,
        sequence: u64,
    },
    /// Permesso negato (ACL)
    PermissionDenied {
        address: Vec<u8>,
        role: OracleRole,
        action: String,
    },
    SchemaValidationFailed {
        schema_id: String,
        reason: String,
    },
    /// Encoding non canonico
    NonCanonicalEncoding(String),
    InvalidConfig(String),
    /// Connector non whitelisted
    ConnectorNotWhitelisted(String),
    /// Rate limit exceeded
    RateLimitExceeded {
        connector_id: String,
        limit: u64,
        current: u64,
    },
    /// Size limit exceeded
    SizeLimitExceeded {
        connector_id: String,
        limit: usize,
        actual: usize,
    },
    /// Invalid signature
    InvalidSignature(String),
    /// Storage I/O error (anti-replay persistence)
    StorageError(String),
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleError::FeedNotFound(id) => write!(f, "Feed not found: {}", id),
            OracleError::SchemaNotFound(id) => write!(f, "Schema not found: {}", id),
            OracleError::InvalidProof(msg) => write!(f, "Invalid proof: {}", msg),
            OracleError::InvalidAnchor(msg) => write!(f, "Invalid anchor: {}", msg),
            OracleError::ExpiredData {
                feed_id,
                expired_at,
                current_time,
            } => {
                write!(
                    f,
                    "Feed {} expired at {} (current: {})",
                    feed_id, expired_at, current_time
                )
            }
            OracleError::FutureTimestamp {
                timestamp,
                current_time,
                tolerance,
            } => {
                write!(
                    f,
                    "Timestamp {} is {} seconds in the future (tolerance: {})",
                    timestamp,
                    timestamp.saturating_sub(*current_time),
                    tolerance
                )
            }
            OracleError::ReplayAttack { feed_id, sequence } => {
                write!(
                    f,
                    "Replay attack detected: feed {} sequence {} already used",
                    feed_id, sequence
                )
            }
            OracleError::PermissionDenied {
                address,
                role,
                action,
            } => {
                write!(
                    f,
                    "Permission denied: address {:?} with role {:?} cannot {}",
                    hex::encode(address),
                    role,
                    action
                )
            }
            OracleError::SchemaValidationFailed { schema_id, reason } => {
                write!(f, "Schema validation failed for {}: {}", schema_id, reason)
            }
            OracleError::NonCanonicalEncoding(msg) => {
                write!(f, "Non-canonical encoding: {}", msg)
            }
            OracleError::InvalidConfig(msg) => {
                write!(f, "Invalid config: {}", msg)
            }
            OracleError::ConnectorNotWhitelisted(id) => {
                write!(f, "Connector not whitelisted: {}", id)
            }
            OracleError::RateLimitExceeded {
                connector_id,
                limit,
                current,
            } => {
                write!(
                    f,
                    "Rate limit exceeded for connector {}: {} >= {}",
                    connector_id, current, limit
                )
            }
            OracleError::SizeLimitExceeded {
                connector_id,
                limit,
                actual,
            } => {
                write!(
                    f,
                    "Size limit exceeded for connector {}: {} > {}",
                    connector_id, actual, limit
                )
            }
            OracleError::InvalidSignature(msg) => {
                write!(f, "Invalid signature: {}", msg)
            }
            OracleError::StorageError(msg) => {
                write!(f, "Storage error: {}", msg)
            }
        }
    }
}

impl std::error::Error for OracleError {}

/// Ruoli per ACL Oracle
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum OracleRole {
    /// Può scrivere feed
    Writer,
    /// Può leggere feed
    Reader,
    /// Può auditare feed e ACL
    Auditor,
}

impl fmt::Display for OracleRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleRole::Writer => write!(f, "Writer"),
            OracleRole::Reader => write!(f, "Reader"),
            OracleRole::Auditor => write!(f, "Auditor"),
        }
    }
}

/// Configurazione Oracle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleConfig {
    /// TTL di default per i feed (in secondi)
    pub default_ttl_seconds: u64,
    /// Tolleranza per timestamp futuri (in secondi)
    pub future_timestamp_tolerance_seconds: u64,
    pub strict_schema_validation: bool,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            default_ttl_seconds: 3600,              // 1 ora
            future_timestamp_tolerance_seconds: 60, // 60 secondi
            strict_schema_validation: true,
        }
    }
}

impl OracleConfig {
    pub fn validate(&self) -> Result<(), OracleError> {
        if self.default_ttl_seconds == 0 {
            return Err(OracleError::InvalidConfig(
                "default_ttl_seconds cannot be 0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configurazione per connector IoT/ERP
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorConfig {
    /// Rate limit: max ingest per minuto (default: 100)
    pub max_ingest_per_minute: u64,
    /// Size limit: max payload size in bytes (default: 64KB)
    pub max_payload_size: usize,
    /// Window size per rate limiting in secondi (default: 60)
    pub rate_limit_window_secs: u64,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            max_ingest_per_minute: 100,
            max_payload_size: 64 * 1024, // 64KB
            rate_limit_window_secs: 60,
        }
    }
}

impl ConnectorConfig {
    pub fn validate(&self) -> Result<(), OracleError> {
        if self.max_ingest_per_minute == 0 {
            return Err(OracleError::InvalidConfig(
                "max_ingest_per_minute cannot be 0".to_string(),
            ));
        }
        if self.max_payload_size == 0 {
            return Err(OracleError::InvalidConfig(
                "max_payload_size cannot be 0".to_string(),
            ));
        }
        if self.rate_limit_window_secs == 0 {
            return Err(OracleError::InvalidConfig(
                "rate_limit_window_secs cannot be 0".to_string(),
            ));
        }
        Ok(())
    }
}
