//! Custom serialization utilities for BlockHeader

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::vec::Vec;

/// Custom serialization for Vec<Vec<u8>> that handles empty vectors properly
pub mod optional_parent_hashes {
    use super::*;

    pub fn serialize<S>(data: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if data.is_empty() {
            // Serialize as None for empty vectors
            serializer.serialize_none()
        } else {
            // Serialize as Some(Vec<Vec<u8>>) for non-empty vectors
            serializer.serialize_some(&data)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize Option<Vec<Vec<u8>>> and convert to Vec<Vec<u8>>
        let opt: Option<Vec<Vec<u8>>> = Option::deserialize(deserializer)?;
        Ok(opt.unwrap_or_default())
    }
}
