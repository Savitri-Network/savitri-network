//! Executor: Transaction execution and scheduling
//!
//! This module provides:
//! - Transaction execution engine
//! - Execution dispatcher with adaptive scheduling
//! - Score cache system
//! - Nonce conflict resolution

pub mod dispatcher;
pub mod nonce_conflict_resolver;
pub mod score_cache;
pub mod transaction_validator;

// Re-export commonly used types
pub use dispatcher::{
    AdaptiveWeightsConfig, DispatcherConfig, DispatcherMetrics, ExecutionDispatcher,
    SchedulingError,
};
pub use nonce_conflict_resolver::{
    ConflictAnalysis, ConflictResolutionStrategy, NonceConflictResolver,
};
pub use score_cache::ScoreCache;
pub use transaction_validator::{
    TransactionValidator, ValidationError, ValidationResult, ValidatorConfig, ValidatorStats,
};
