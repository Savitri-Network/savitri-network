//! Core consensus traits and interfaces
//!
//! This module defines the fundamental traits that all consensus implementations
//! must follow, ensuring compatibility and interoperability between different node types.

pub mod consensus;
pub mod proposer;
pub mod storage;
pub mod validator;

// Re-export all traits
pub use consensus::*;
pub use proposer::*;
pub use storage::*;
pub use validator::*;
