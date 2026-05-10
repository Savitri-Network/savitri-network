//! Compatibility module for savitri-mempool
//!
//! This module provides compatibility shims for the light node build.

// Re-export types from light node modules (intentionally unused - API compatibility)
#[allow(unused_imports)]
pub use crate::core::tx::{MempoolTx, TxHandle};
#[allow(unused_imports)]
pub use crate::tx::{
    deserialize_call_tx, deserialize_signed_tx, hash_signed_tx_bytes, serialize_signed_tx,
    CallTransaction, SignedTx, TransactionExt,
};

// Compatibility shims for missing modules
#[allow(unused_imports)]
pub mod core {
    pub use crate::tx::*;
}
