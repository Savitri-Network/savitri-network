//! Core types

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub timestamp: u64,
    pub gas_limit: u64,
    pub gas_price: u64,
}

// Re-export Account from account module
pub use crate::core::account::Account;
