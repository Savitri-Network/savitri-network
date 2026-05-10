//! Database helpers for RocksDB operations
//!
//! This module provides low-level RocksDB helpers for column family management
//! and schema versioning.

use anyhow::Result;

/// Column family name for metadata
pub const CF_META: &str = "meta";

/// Key for schema version in metadata
pub const META_SCHEMA_VERSION_KEY: &str = "schema_version";

/// Current schema version
pub const SCHEMA_VERSION: u32 = 1;

/// Database helper trait for RocksDB operations
#[cfg(feature = "rocksdb")]
pub struct DbHelper {
    db: std::sync::Arc<rocksdb::DB>,
}

#[cfg(feature = "rocksdb")]
impl DbHelper {
    /// Create a new DbHelper wrapping a RocksDB instance
    pub fn new(db: std::sync::Arc<rocksdb::DB>) -> Self {
        Self { db }
    }

    /// Get a column family handle by name
    pub fn cf(
        &self,
        name: &str,
    ) -> Result<std::sync::Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow::anyhow!("missing column family: {name}"))
    }

    /// Ensure the schema version matches the expected version
    pub fn ensure_schema_version(&self) -> Result<()> {
        let cf = match self.db.cf_handle(CF_META) {
            Some(cf) => cf,
            None => {
                // CF_META doesn't exist yet, this is a fresh DB
                return Ok(());
            }
        };
        
        let existing = self.db.get_cf(&cf, META_SCHEMA_VERSION_KEY.as_bytes())?;
        match existing {
            Some(bytes) => {
                if bytes.len() != 4 {
                    anyhow::bail!("invalid schema_version encoding")
                }
                let v = u32::from_le_bytes(bytes[..4].try_into().unwrap());
                if v != SCHEMA_VERSION {
                    anyhow::bail!(
                        "schema version mismatch: db={}, expected={}",
                        v,
                        SCHEMA_VERSION
                    );
                }
            }
            None => {
                // Initialize schema version on fresh DB
                self.db.put_cf(
                    &cf,
                    META_SCHEMA_VERSION_KEY.as_bytes(),
                    &SCHEMA_VERSION.to_le_bytes(),
                )?;
            }
        }
        Ok(())
    }

    /// Put a value in a column family
    pub fn put_cf<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        cf_name: &str,
        key: K,
        value: V,
    ) -> Result<()> {
        let cf = self.cf(cf_name)?;
        Ok(self.db.put_cf(&cf, key.as_ref(), value.as_ref())?)
    }

    /// Get a value from a column family
    pub fn get_cf<K: AsRef<[u8]>>(&self, cf_name: &str, key: K) -> Result<Option<Vec<u8>>> {
        let cf = self.cf(cf_name)?;
        Ok(self.db.get_cf(&cf, key.as_ref())?)
    }

    /// Delete a value from a column family
    pub fn delete_cf<K: AsRef<[u8]>>(&self, cf_name: &str, key: K) -> Result<()> {
        let cf = self.cf(cf_name)?;
        Ok(self.db.delete_cf(&cf, key.as_ref())?)
    }

    /// Get an iterator over a column family
    pub fn iterator_cf<'a>(
        &'a self,
        cf: &'a std::sync::Arc<rocksdb::BoundColumnFamily<'a>>,
    ) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a {
        self.db.iterator_cf(cf, rocksdb::IteratorMode::Start)
    }

    /// Get an iterator with a specific prefix
    pub fn prefix_iterator_cf<'a>(
        &'a self,
        cf: &'a std::sync::Arc<rocksdb::BoundColumnFamily<'a>>,
        prefix: &[u8],
    ) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> + 'a {
        self.db.prefix_iterator_cf(cf, prefix)
    }
}

#[cfg(not(feature = "rocksdb"))]
pub struct DbHelper;

#[cfg(not(feature = "rocksdb"))]
impl DbHelper {
    pub fn new() -> Self {
        Self
    }
    
    pub fn ensure_schema_version(&self) -> Result<()> {
        Ok(())
    }
}
