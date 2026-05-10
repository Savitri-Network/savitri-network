use std::fmt;

/// Minimal storage error type for external API exposure.
/// Not yet wired through the storage code, but available for adopters.
#[derive(Debug)]
pub enum StorageError {
    SchemaVersionMismatch { db: u32, expected: u32 },
    InvalidMetaEncoding,
    EmptyAccountPersisted,
    AccountEncodingRoundTripMismatch,
    RocksDb(String),
    Other(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::SchemaVersionMismatch { db, expected } => {
                write!(
                    f,
                    "schema version mismatch: db={}, expected={}",
                    db, expected
                )
            }
            StorageError::InvalidMetaEncoding => write!(f, "invalid meta encoding"),
            StorageError::EmptyAccountPersisted => write!(f, "found persisted empty account"),
            StorageError::AccountEncodingRoundTripMismatch => {
                write!(f, "account encoding round-trip mismatch")
            }
            StorageError::RocksDb(e) => write!(f, "rocksdb error: {}", e),
            StorageError::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<rocksdb::Error> for StorageError {
    fn from(e: rocksdb::Error) -> Self {
        StorageError::RocksDb(e.to_string())
    }
}
