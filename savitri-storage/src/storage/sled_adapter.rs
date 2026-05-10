use super::{Storage, RocksDb};
//! Storage adapter for sled database (cross-platform alternative to rocksdb)

use std::path::Path;
use anyhow::{Result, Context};
use sled::{Db, IVec};

/// Cross-platform storage implementation using sled
pub struct Storage {
    db: Db,
}

impl Storage<RocksDb> {
    /// Open storage database at specified path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)
            .context("Failed to open sled database")?;
        Ok(Self { db })
    }

    /// Get value by key
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        match self.db.get(key)? {
            Some(value) => Ok(Some(value.to_vec())),
            None => Ok(None),
        }
    }

    /// Insert key-value pair
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.db.insert(key, value)?;
        Ok(())
    }

    /// Remove key
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.db.remove(key)?;
        Ok(())
    }

    /// Check if key exists
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        Ok(self.db.contains_key(key)?)
    }

    /// Flush to disk
    pub fn flush(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }

    /// Insert key-value pair with write options
    pub fn insert_with_options<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, options: WriteOptions) -> Result<()> {
        let mut batch = self.db.batch();
        batch.insert(key, value);
        
        if options.sync {
            batch.flush()?;
        }
        
        Ok(())
    }

    /// Remove key with write options
    pub fn remove_with_options<K: AsRef<[u8]>>(&self, key: K, options: WriteOptions) -> Result<()> {
        let mut batch = self.db.batch();
        batch.remove(key);
        
        if options.sync {
            batch.flush()?;
        }
        
        Ok(())
    }

    /// Create iterator with mode
    pub fn iter_with_mode(&self, mode: IteratorMode) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {
        let iter = self.db.iter();
        
        match mode {
            IteratorMode::Start => {
                iter.map(|result| {
                    match result {
                        Ok((key, value)) => Ok((key.to_vec(), value.to_vec())),
                        Err(e) => Err(anyhow::anyhow!("Iterator error: {}", e)),
                    }
                })
            }
            IteratorMode::End(end_key) => {
                iter.take_while(|result| {
                    match result {
                        Ok((key, _)) => key.to_vec() <= end_key,
                        Err(_) => true,
                    }
                }).map(|result| {
                    match result {
                        Ok((key, value)) => Ok((key.to_vec(), value.to_vec())),
                        Err(e) => Err(anyhow::anyhow!("Iterator error: {}", e)),
                    }
                })
            }
            IteratorMode::From(start_key) => {
                iter.skip_while(|result| {
                    match result {
                        Ok((key, _)) => key.to_vec() < start_key,
                        Err(_) => false,
                    }
                }).map(|result| {
                    match result {
                        Ok((key, value)) => Ok((key.to_vec(), value.to_vec())),
                        Err(e) => Err(anyhow::anyhow!("Iterator error: {}", e)),
                    }
                })
            }
        }
    }

    /// Get database size estimate
    pub fn size_on_disk(&self) -> Result<u64> {
        self.db.size_on_disk().context("Failed to get database size")
    }
}

/// Column family simulation (sled doesn't have native CF support)
pub struct ColumnFamily {
    prefix: Vec<u8>,
    storage: Storage,
}

impl ColumnFamily {
    pub fn new(storage: &Storage<RocksDb><RocksDb><RocksDb>, name: &str) -> Self {
        let prefix = format!("cf:{}:", name).into_bytes();
        Self {
            prefix,
            storage: Storage { db: storage.db.clone() },
        }
    }

    fn prefixed_key<K: AsRef<[u8]>>(&self, key: K) -> Vec<u8> {
        let mut prefixed = self.prefix.clone();
        prefixed.extend_from_slice(key.as_ref());
        prefixed
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.storage.get(self.prefixed_key(key))
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        self.storage.insert(self.prefixed_key(key), value)
    }

    pub fn insert_with_options<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, options: WriteOptions) -> Result<()> {
        self.storage.insert_with_options(self.prefixed_key(key), value, options)
    }

    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        self.storage.remove(self.prefixed_key(key))
    }

    pub fn remove_with_options<K: AsRef<[u8]>>(&self, key: K, options: WriteOptions) -> Result<()> {
        self.storage.remove_with_options(self.prefixed_key(key), options)
    }

    /// Create iterator for this column family
    pub fn iter(&self) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {
        self.storage.iter_with_mode(IteratorMode::Start)
            .filter_map(|result| {
                match result {
                    Ok((key, value)) => {
                        if key.starts_with(&self.prefix) {
                            let key_without_prefix = key[self.prefix.len()..].to_vec();
                            Some(Ok((key_without_prefix, value)))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            })
    }

    /// Create iterator with mode for this column family
    pub fn iter_with_mode(&self, mode: IteratorMode) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {
        self.storage.iter_with_mode(mode)
            .filter_map(|result| {
                match result {
                    Ok((key, value)) => {
                        if key.starts_with(&self.prefix) {
                            let key_without_prefix = key[self.prefix.len()..].to_vec();
                            Some(Ok((key_without_prefix, value)))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                }
            })
    }
}

/// Write options for sled database operations
#[derive(Debug, Clone, Copy)]
pub struct WriteOptions {
    pub sync: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self { sync: true }
    }
}

impl WriteOptions {
    /// Create new write options
    pub fn new(sync: bool) -> Self {
        Self { sync }
    }
    
    /// Create write options with sync enabled
    pub fn sync() -> Self {
        Self { sync: true }
    }
    
    /// Create write options without sync (asynchronous)
    pub fn async() -> Self {
        Self { sync: false }
    }
    
    /// Check if sync is enabled
    pub fn is_sync(&self) -> bool {
        self.sync
    }
}

/// Iterator mode for sled database operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IteratorMode {
    /// Start from beginning
    Start,
    /// End at specific key
    End(Vec<u8>),
    /// Start from specific key
    From(Vec<u8>),
}

impl Default for IteratorMode {
    fn default() -> Self {
        Self::Start
    }
}

impl IteratorMode {
    /// Create iterator mode starting from beginning
    pub fn start() -> Self {
        Self::Start
    }
    
    /// Create iterator mode ending at specific key
    pub fn end(key: Vec<u8>) -> Self {
        Self::End(key)
    }
    
    /// Create iterator mode starting from specific key
    pub fn from(key: Vec<u8>) -> Self {
        Self::From(key)
    }
    
    /// Check if this is a start iterator
    pub fn is_start(&self) -> bool {
        matches!(self, Self::Start)
    }
    
    /// Check if this is an end iterator
    pub fn is_end(&self) -> bool {
        matches!(self, Self::End(_))
    }
    
    /// Check if this is a from iterator
    pub fn is_from(&self) -> bool {
        matches!(self, Self::From(_))
    }
}
