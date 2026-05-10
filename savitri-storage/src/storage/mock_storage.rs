//! Mock Storage Implementation
//! 
//! Temporary replacement for RocksDB to enable compilation testing

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use anyhow::{Result, anyhow};

pub struct MockStorage {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
        Ok(())
    }

    pub fn batch_put(&self, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        for (key, value) in pairs {
            data.insert(key.clone(), value.clone());
        }
        Ok(())
    }
}

// Re-export as Storage for compatibility
pub type Storage = MockStorage;

pub fn open_storage(_path: &str) -> Result<Storage> {
    Ok(MockStorage<RocksDb>::new())
}
