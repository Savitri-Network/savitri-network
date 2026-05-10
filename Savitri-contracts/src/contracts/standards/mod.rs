//! Token Standards: Standard tokens for the platform
//!
//! This module implements token standards:
//! - SFT1 (SAVITRI Fungible Token): Fungible token
//! - SNT1 (SAVITRI Non Fungible Token): NFT
//! - SMA (SAVITRI Multi Asset): Multi-asset token

pub mod savitri1155;
pub mod savitri20;
pub mod savitri721;
pub mod savnft;

pub use savitri1155::SAVITRI1155;
pub use savitri20::SAVITRI20;
pub use savitri721::SAVITRI721;
pub use savnft::SAVNFT;
