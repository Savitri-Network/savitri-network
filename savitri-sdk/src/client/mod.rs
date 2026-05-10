//! Client modules for Savitri Network SDK
//!
//! - [`RpcClient`]: JSON-RPC 2.0 client for the Savitri Network
//! - [`LightClient`]: Lightweight convenience wrapper
//! - [`Wallet`]: Key management and transaction signing
//! - [`ContractClient`]: High-level contract, oracle, and governance helpers

pub mod contract_client;
pub mod light_client;
pub mod rpc_client;
pub mod wallet;

pub use contract_client::{ContractClient, GovernanceClient, OracleClient, ProposalStatus};
pub use light_client::LightClient;
pub use rpc_client::RpcClient;
pub use wallet::Wallet;
