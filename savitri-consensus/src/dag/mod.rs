//! DAG Management Module
//!
//! This module provides non-invasive DAG (Directed Acyclic Graph) management
//! capabilities for the Savitri consensus system. It works with existing
//! BlockHeader structures without requiring modifications to core consensus code.

pub mod conflict_detector;
pub mod manager;
pub mod types;

// Re-export main types for convenience
pub use conflict_detector::ConflictDetector;
pub use manager::DAGManager;
pub use types::*;
