use super::{Storage, RocksDb};
use super::{Storage, CF_MISSING, CF_ORPHANS, RocksDb};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrphanRec {
    #[serde(with = "BigArray")]
    pub parent_exec_hash: [u8; 64],
    #[serde(with = "opt_hash64")]
    pub parent_ref_hash: Option<[u8; 64]>,
    pub first_seen: u64,
    pub tries: u32,
    pub next_try: u64,
    pub block_bytes: Vec<u8>,
}

mod opt_hash64 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<[u8; 64]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(ref bytes) => serializer.serialize_some(bytes.as_slice()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<[u8; 64]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<Vec<u8>> = Option::deserialize(deserializer)?;
        match opt {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                if bytes.len() != 64 {
                    return Err(serde::de::Error::custom(format!(
                        "expected 64-byte hash, got {}",
                        bytes.len()
                    )));
                }
                let mut arr = [0u8; 64];
                arr.copy_from_slice(&bytes);
                Ok(Some(arr))
            }
            None => Ok(None),
        }
    }
}

impl Storage<RocksDb> {
    pub fn put_orphan(&self, child_hash: &[u8; 64], rec: &OrphanRec) -> Result<()> {
        let key = orphan_key(child_hash);
        let value = bincode::serialize(rec)?;
        self.put_cf(CF_ORPHANS, key, value)
    }

    pub fn get_orphan(&self, child_hash: &[u8; 64]) -> Result<Option<OrphanRec>> {
        let key = orphan_key(child_hash);
        match self.get_cf(CF_ORPHANS, key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)),
            None => Ok(None),
        }
    }

    pub fn delete_orphan(&self, child_hash: &[u8; 64]) -> Result<()> {
        let key = orphan_key(child_hash);
        self.delete_cf(CF_ORPHANS, key)
    }

    pub fn add_missing_index(&self, parent_exec: &[u8; 64], child_hash: &[u8; 64]) -> Result<()> {
        let key = missing_key(parent_exec, child_hash);
        self.put_cf(CF_MISSING, key, [])
    }

    pub fn remove_missing_index(
        &self,
        parent_exec: &[u8; 64],
        child_hash: &[u8; 64],
    ) -> Result<()> {
        let key = missing_key(parent_exec, child_hash);
        self.delete_cf(CF_MISSING, key)
    }

    pub fn list_missing_children(&self, parent_exec: &[u8; 64]) -> Result<Vec<[u8; 64]>> {
        let prefix = missing_prefix(parent_exec);
        let cf = self.cf(CF_MISSING)?;
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let mut out = Vec::new();
        for item in iter {
            let (key, _value): (Box<[u8]>, Box<[u8]>) = item?;
            let slice = key.as_ref();
            if slice.len() != prefix.len() + 64 {
                return Err(anyhow!("malformed missing-index key len={}", slice.len()));
            }
            let mut arr = [0u8; 64];
            arr.copy_from_slice(&slice[prefix.len()..]);
            out.push(arr);
        }
        Ok(out)
    }
}

fn orphan_key(child_hash: &[u8; 64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 64);
    key.extend_from_slice(b"o|");
    key.extend_from_slice(child_hash);
    key
}

fn missing_key(parent_exec: &[u8; 64], child_hash: &[u8; 64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 64 + 1 + 64);
    key.extend_from_slice(b"m|");
    key.extend_from_slice(parent_exec);
    key.push(b'|');
    key.extend_from_slice(child_hash);
    key
}

fn missing_prefix(parent_exec: &[u8; 64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + 64 + 1);
    key.extend_from_slice(b"m|");
    key.extend_from_slice(parent_exec);
    key.push(b'|');
    key
}
