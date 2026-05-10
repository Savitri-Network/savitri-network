// SPDX-License-Identifier: MIT
// 2026 Savitri Network

//! Core functionality for Savitri Network
//!
//! This module contains fundamental data structures and algorithms
//! that form the foundation of the Savitri blockchain ecosystem.

pub mod epoch;
pub mod monolith;
pub mod slot_scheduler;
pub mod types;

// Re-export commonly used types
pub use types::{Transaction, Account, FeeLimits};
pub use slot_scheduler::{SlotScheduler, SlotSchedulerConfig, SlotInfo, SlotRole};
pub use monolith::{MonolithHeader, MonolithPolicy, generate_monolith, compute_monolith_id};
pub use epoch::{EpochConfig, EpochManager, EpochStatus, calculate_epoch_id};
