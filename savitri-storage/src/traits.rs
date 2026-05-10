//! Storage trait for dynamic dispatch
//!
//! This module defines the StorageTrait that allows using Storage
//! as a trait object (dyn StorageTrait) for dynamic dispatch.

use anyhow::Result;

/// Storage trait for dynamic dispatch
/// This trait defines the common interface for all storage implementations
pub trait StorageTrait: Send + Sync + std::fmt::Debug {
    /// Put data in default column family
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Get data from default column family
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Delete data
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Put data in specific column family
    fn put_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Get data from specific column family
    fn get_cf(&self, cf_name: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterate all entries in a specific column family.
    fn iterator_cf(
        &self,
        cf_name: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>>>;

    /// Scan a prefix from a specific column family with a bounded result set.
    fn scan_cf_prefix(
        &self,
        cf_name: &str,
        prefix: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Check if storage is healthy
    fn is_healthy(&self) -> bool;

    /// Get account data
    fn get_account(&self, address: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put account data
    fn put_account(&self, address: &[u8], account_data: &[u8]) -> Result<()>;
}
