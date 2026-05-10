//! Byzantine-robust FL scoring re-exports.
//!
//! The implementation lives in `savitri_core::fl_robust` so the mempool
//! aggregator (which depends on core but not consensus) can call it
//! directly. Consensus consumers continue to access the same types via
//! `crate::scoring::fl_robust::*`.

pub use savitri_core::fl_robust::{
    coordinate_wise_median, score_gradients_vs_median, ScoredGradient, MALICIOUS_GRADIENT_STREAK,
    MALICIOUS_GRADIENT_THRESHOLD_PERMILLE, NORM_CLIP_THRESHOLD,
};
