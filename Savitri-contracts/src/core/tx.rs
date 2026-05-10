//! Transaction types for savitri-contracts
//!
//! Provides transaction-related types for contract execution

use serde::{Deserialize, Serialize};

/// Signed transaction wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTx {
    pub tx: Transaction,
    pub signature: Vec<u8>,
    pub signer: Vec<u8>,
    pub from: Vec<u8>,
    pub to: Option<Vec<u8>>,
}

impl SignedTx {
    pub fn new(tx: Transaction, signature: Vec<u8>, signer: Vec<u8>) -> Self {
        let from = tx.from.clone();
        let to = tx.to.clone();
        Self {
            tx,
            signature,
            signer,
            from,
            to,
        }
    }

    pub fn hash(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&bincode::serialize(&self.tx).unwrap_or_default());
        hasher.finalize().to_vec()
    }
}

/// Transaction for contract execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub from: Vec<u8>,
    pub to: Option<Vec<u8>>,
    pub value: u128,
    pub data: Vec<u8>,
    pub nonce: u64,
    pub gas_limit: u64,
    pub gas_price: u128,
}

impl Transaction {
    pub fn new(from: Vec<u8>, to: Option<Vec<u8>>, value: u128, data: Vec<u8>) -> Self {
        Self {
            from,
            to,
            value,
            data,
            nonce: 0,
            gas_limit: 21000,
            gas_price: 1,
        }
    }

    pub fn is_contract_creation(&self) -> bool {
        self.to.is_none()
    }
}

/// Transaction class for scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TxClass {
    Transfer,
    ContractCall,
    ContractDeploy,
    Governance,
    Oracle,
    System,
}

impl Default for TxClass {
    fn default() -> Self {
        TxClass::Transfer
    }
}

/// Call transaction for contract execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallTransaction {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub value: u128,
    pub data: Vec<u8>,
    pub gas_limit: u64,
    pub gas_price: u128,
    pub nonce: u64,
    pub caller: Vec<u8>,
    pub contract_address: Vec<u8>,
}

impl CallTransaction {
    pub fn new(from: Vec<u8>, to: Vec<u8>, value: u128, data: Vec<u8>) -> Self {
        Self {
            from: from.clone(),
            to: to.clone(),
            value,
            data,
            gas_limit: 21000,
            gas_price: 1,
            nonce: 0,
            caller: from.clone(),
            contract_address: to,
        }
    }
}

/// Deploy transaction for contract deployment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployTransaction {
    pub from: Vec<u8>,
    pub value: u128,
    pub code: Vec<u8>,
    pub gas_limit: u64,
    pub gas_price: u128,
    pub nonce: u64,
}

impl DeployTransaction {
    pub fn new(from: Vec<u8>, value: u128, code: Vec<u8>) -> Self {
        Self {
            from,
            value,
            code,
            gas_limit: 1000000,
            gas_price: 1,
            nonce: 0,
        }
    }
}
