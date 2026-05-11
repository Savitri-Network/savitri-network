//! Savitri Network SDK
//!
//! A standalone SDK for interacting with the Savitri Network via its
//! JSON-RPC 2.0 interface.
//!
//! # Modules
//!
//! - [`client`] -- RPC client, light client, wallet, and contract helpers
//! - [`types`] -- Request/response types matching `savitri-rpc`
//!
//! # Quick start
//!
//! ```no_run
//! use savitri_sdk::{RpcClient, Wallet, TransactionBuilder};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = RpcClient::from_url("http://localhost:8545")?;
//!
//!     // Health check
//!     let health = client.health().await?;
//!     println!("Node mode: {}", health.mode);
//!
//!     // Block height
//!     let height = client.get_block_number().await?;
//!     println!("Block height: {}", height);
//!
//!     // Account info
//!     let wallet = Wallet::new();
//!     let account = client.get_account(wallet.address()).await?;
//!     println!("Balance: {}", account.balance);
//!
//!     Ok(())
//! }
//! ```

#![deny(missing_docs)]
#![warn(clippy::all)]

pub mod client;
pub mod types;
pub mod utils;

// Re-exports for convenience
pub use client::{
    ContractClient, GovernanceClient, LightClient, OracleClient, ProposalStatus, RpcClient, Wallet,
};
pub use types::*;
pub use utils::{AddressUtils, GovernanceAction, TransactionBuilder};

/// SDK version (from Cargo.toml).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
