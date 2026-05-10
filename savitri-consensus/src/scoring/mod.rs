//! Runtime observation state that feeds real values into PoU scoring.
//!
//! `types::score` defines the pure calculators (stateless, deterministic).
//! This module provides the stateful input side: a per-peer rolling store
//! of measurements captured by the P2P and consensus layers, consumed by
//! `PouCalculator` in `protocols::pou_based` to replace hardcoded stubs.
//!
//! Federated Learning contribution scoring (see `fl_robust`).

pub mod fl_robust;
mod observations;
pub mod streak_daemon;

pub use fl_robust::{
    coordinate_wise_median, score_gradients_vs_median, ScoredGradient, MALICIOUS_GRADIENT_STREAK,
    MALICIOUS_GRADIENT_THRESHOLD_PERMILLE, NORM_CLIP_THRESHOLD,
};
pub use observations::{
    FlContributionSample, LatencySample, ObservationStore, PeerObservations, SlashEvent,
    SlashReason, ValidationSample, DEFAULT_WINDOW_SECS, MAX_SAMPLES_PER_METRIC,
};
