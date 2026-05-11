//! Savitri Storage Layer
//!
//! This crate provides the storage layer for the Savitri blockchain, supporting:
//! - RocksDB storage with optimized column families
//! - FlatFile storage for monoliths
//! - In-memory storage for testing
//! - Thread-safe concurrent access
//! - Efficient caching and batch operations
//! - Backup and restore functionality
//!
//! # Features
//!
//! - `rocksdb`: RocksDB-based persistent storage
//! - `memory`: In-memory storage for testing
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use savitri_storage::Storage;
//!
//! // Create new storage instance
//! let mut storage = Storage::new("path/to/database")?;
//!
//! // Store and retrieve data
//! storage.put(b"key", b"value")?;
//! let value = storage.get(b"key")?;
//!
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod fl;
pub mod storage;
pub mod traits;

#[cfg(feature = "rocksdb")]
pub use storage::{Storage, StorageConfig, StorageSnapshot};

#[cfg(not(feature = "rocksdb"))]
pub use storage::{Storage, StorageConfig, StorageSnapshot};

#[cfg(feature = "memory")]
pub use storage::Storage as MemoryStorage;

pub use fl::{FlRetentionConfig, FlRetentionOutcome, FlStorage};
pub use traits::StorageTrait;

// Re-export governance and vesting types
pub use storage::{
    Proposal, ProposalAction, ProposalStatus, VestingSchedule, VestingType, VoteType,
};

// Re-export common types
pub use anyhow::{Context, Result};
pub use std::path::{Path, PathBuf};

/// Storage error types
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[cfg(feature = "rocksdb")]
    #[error("Database error: {0}")]
    Database(#[from] rocksdb::Error),

    #[cfg(not(feature = "rocksdb"))]
    #[error("Database error: {0}")]
    Database(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Backup error: {0}")]
    Backup(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

// ─── Safe bincode deserialization ────────────────────────────────────────

/// Maximum size for bincode deserialization (16 MB).
///
/// SECURITY: Prevents denial-of-service attacks via maliciously crafted
/// payloads that would allocate excessive memory during deserialization.
pub const MAX_BINCODE_SIZE: u64 = 16 * 1024 * 1024;

/// Safe bincode deserialization with a 16 MB size limit.
///
/// Drop-in replacement for `bincode::deserialize()` that enforces a
/// maximum decoded size to prevent memory exhaustion from untrusted input.
///
/// Uses the same encoding settings as the standard `bincode::serialize()`.
pub fn safe_deserialize<'de, T: serde::Deserialize<'de>>(
    bytes: &'de [u8],
) -> std::result::Result<T, bincode::Error> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .with_limit(MAX_BINCODE_SIZE)
        .deserialize(bytes)
}
