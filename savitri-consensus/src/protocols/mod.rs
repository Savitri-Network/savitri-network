//! Protocol implementations for different consensus mechanisms
//!
//! This module contains the specific consensus protocol implementations
//! for masternode and lightnode consensus mechanisms.

pub mod bft;
pub mod group_aware;
pub mod hybrid;
pub mod partition;
pub mod pou_based;

// Re-export all protocols
pub use bft::*;
pub use group_aware::*;
pub use hybrid::*;
pub use partition::*;
pub use pou_based::*;
