//! Core types for savitri-contracts
//!
//! Provides Account and other core types

use serde::{Deserialize, Serialize};

/// Account representation for contract execution
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Account {
    pub address: Vec<u8>,
    pub balance: u128,
    pub nonce: u64,
    pub code_hash: Option<Vec<u8>>,
    pub storage_root: Option<Vec<u8>>,
}

impl Account {
    pub fn new(address: Vec<u8>) -> Self {
        Self {
            address,
            balance: 0,
            nonce: 0,
            code_hash: None,
            storage_root: None,
        }
    }

    pub fn debit(&mut self, amount: u128) -> anyhow::Result<()> {
        if self.balance < amount {
            anyhow::bail!("Insufficient balance");
        }
        self.balance -= amount;
        Ok(())
    }

    pub fn credit(&mut self, amount: u128) -> anyhow::Result<()> {
        self.balance = self
            .balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;
        Ok(())
    }

    pub fn with_balance(mut self, balance: u128) -> Self {
        self.balance = balance;
        self
    }

    pub fn with_nonce(mut self, nonce: u64) -> Self {
        self.nonce = nonce;
        self
    }

    pub fn is_contract(&self) -> bool {
        self.code_hash.is_some()
    }

    pub fn increment_nonce(&mut self) {
        self.nonce = self.nonce.saturating_add(1);
    }

    pub fn add_balance(&mut self, amount: u128) {
        self.balance = self.balance.saturating_add(amount);
    }

    pub fn sub_balance(&mut self, amount: u128) -> bool {
        if self.balance >= amount {
            self.balance = self.balance.saturating_sub(amount);
            true
        } else {
            false
        }
    }
}

/// Transaction type for contract calls
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
