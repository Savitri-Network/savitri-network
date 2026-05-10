//! Account structures and types

use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub balance: u128,
    pub nonce: u64,
    pub address: Vec<u8>,
    pub last_updated: Instant,
}

impl Account {
    pub fn new(address: Vec<u8>, balance: u128, nonce: u64) -> Self {
        Self {
            balance,
            nonce,
            address,
            last_updated: Instant::now(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        // Simple serialization
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.balance.to_le_bytes());
        bytes.extend_from_slice(&self.nonce.to_le_bytes());
        bytes.extend_from_slice(&(self.address.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.address);
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 24 {
            return None;
        }

        let balance = u128::from_le_bytes(bytes[0..16].try_into().ok()?);
        let nonce = u64::from_le_bytes(bytes[16..24].try_into().ok()?);
        
        if bytes.len() < 28 {
            return None;
        }
        
        let addr_len = u32::from_le_bytes(bytes[24..28].try_into().ok()?) as usize;
        
        if bytes.len() < 28 + addr_len {
            return None;
        }
        
        let address = bytes[28..28 + addr_len].to_vec();

        Some(Self {
            balance,
            nonce,
            address,
        })
    }
}

impl Default for Account {
    fn default() -> Self {
        Self {
            balance: 0,
            nonce: 0,
            address: vec![0u8; 32],
            last_updated: Instant::now(),
        }
    }
}
