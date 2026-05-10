//! Transaction types and utilities

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTx {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub amount: u128,
    pub nonce: u64,
    pub fee: Option<u128>,
    #[serde(with = "serde_bytes")]
    pub pubkey: [u8; 32],
    #[serde(with = "serde_big_array::BigArray")]
    pub sig: [u8; 64],
    pub pre_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallTransaction {
    pub contract_address: Vec<u8>,
    pub function_selector: Vec<u8>,
    pub calldata: Vec<u8>,
    pub caller: Vec<u8>,
    pub nonce: u64,
    pub fee: Option<u128>,
    #[serde(with = "serde_bytes")]
    pub pubkey: [u8; 32],
    #[serde(with = "serde_big_array::BigArray")]
    pub sig: [u8; 64],
    pub pre_verified: bool,
}

impl CallTransaction {
    pub fn verify(&self) -> Result<(), String> {
        // Simple verification - in real implementation would verify signature
        if self.sig.iter().all(|&x| x == 0) {
            return Err("Empty signature".to_string());
        }
        Ok(())
    }
}

impl SignedTx {
    pub fn verify(&self) -> Result<(), String> {
        // Simple verification - in real implementation would verify signature
        if self.sig.iter().all(|&x| x == 0) {
            return Err("Empty signature".to_string());
        }
        Ok(())
    }

    pub fn message(&self) -> Vec<u8> {
        // Create message for signature verification
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&self.from);
        hasher.update(&self.to);
        hasher.update(self.amount.to_le_bytes());
        hasher.update(self.nonce.to_le_bytes());
        hasher.finalize().to_vec()
    }
}

pub fn hash_signed_tx_bytes(tx: &[u8]) -> Vec<u8> {
    // Simple hash implementation
    use sha2::{Sha256, Digest};
    let mut hasher = Sha256::new();
    hasher.update(tx);
    hasher.finalize().to_vec()
}

pub fn deserialize_signed_tx(data: &[u8]) -> Result<SignedTx, Box<dyn std::error::Error>> {
    bincode::deserialize(data).map_err(|e| e.into())
}

pub fn deserialize_call_tx(data: &[u8]) -> Result<CallTransaction, Box<dyn std::error::Error>> {
    bincode::deserialize(data).map_err(|e| e.into())
}
