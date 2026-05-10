//! Error types for consensus operations

use crate::types::validation::ValidationError;
use thiserror::Error;

/// Main consensus error type
#[derive(Debug, Error)]
pub enum ConsensusError {
    /// Initialization error
    #[error("Initialization error: {0}")]
    Initialization(String),

    /// Validation failed
    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(ValidationError),

    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    /// Timeout error
    #[error("Timeout error: {0}")]
    TimeoutError(String),

    /// Cryptographic error
    #[error("Cryptography error: {0}")]
    CryptoError(String),

    /// Invalid signature error
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// Invalid state error
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// Not found error
    #[error("Not found: {0}")]
    NotFound(String),

    /// Permission denied error
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Rate limited error
    #[error("Rate limited: {0}")]
    RateLimited(String),

    /// Custom error
    #[error("{0}")]
    Custom(String),

    /// Invalid message (malformed or oversized)
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    /// Validator already permanently slashed; further misbehavior reports are ignored.
    #[error("Validator already slashed: {0}")]
    AlreadySlashed(String),

    /// Validator is currently jailed; misbehavior reports are rejected until unjailing.
    #[error("Validator jailed: {0}")]
    ValidatorJailed(String),

    #[error("Slash cooldown active: {0}")]
    SlashCooldown(String),

    /// ZKP verification error
    #[error("ZKP error: {0}")]
    ZkpError(String),

    /// Version incompatibility
    #[error("Version incompatibility: {0}")]
    VersionIncompatibility(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    /// Anyhow error
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

/// Result type for consensus operations
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Validation result type
pub type ValidationResult<T> = std::result::Result<T, ValidationError>;

/// Validation error result type (no generics)
pub type ValidationErrorResult = std::result::Result<(), ValidationError>;
