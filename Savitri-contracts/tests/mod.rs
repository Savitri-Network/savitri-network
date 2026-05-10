//! Contract Testing Framework
//!
//! Framework completo per testing smart contracts con:
//! - Deployment helpers
//! - Mock contracts (SFT1, SNT1)
//! - Transaction helpers
//! - Snapshot/restore per test isolation
//! - Fuzzing per security testing

pub mod framework;
pub mod helpers;
pub mod mocks;

#[cfg(test)]
mod integration_test;

#[cfg(test)]
mod framework_test;

pub use framework::*;
pub use helpers::*;
pub use mocks::*;
