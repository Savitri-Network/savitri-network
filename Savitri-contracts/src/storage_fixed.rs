//! Storage re-export module
//!
//! Re-exports savitri_storage types for use within savitri-contracts

// Re-export storage from savitri-storage
pub mod storage;

// Re-export specific items from savitri_storage to avoid conflicts
pub use savitri_storage::storage::contracts::*;
pub use savitri_storage::storage::fl::*;
pub use savitri_storage::storage::treasury::*;
pub use savitri_storage::storage::vote_tokens::*;
pub use savitri_storage::storage::oracle::*;
pub use savitri_storage::storage::bonds::*;
pub use savitri_storage::storage::governance::*;
