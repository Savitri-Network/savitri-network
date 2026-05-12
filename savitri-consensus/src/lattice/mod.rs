//! Lattice ordering runtime — DAG-based parallel block production.
//!
//! Part of Savitri V0.2 Phase 2. The types defining the wire format
//! (`LatticeCell`, `CellAttestation`, `CellCertificate`, `Cycle`) live in
//! `crate::types::lattice` and were shipped with the Phase 2 spike as
//! part of issue #31 closeout. This module owns the *runtime*: cell
//! aggregation, attestation quorum collection, lineage commit decision,
//! and cycle pivot election.
//!
//! ## Layout
//!
//! - [`aggregator`] — collects raw cells and attestations from gossip,
//!   verifies signatures, emits `CellCertificate`s when quorum is met.
//! - [`commit`] — walks the certified-cell DAG to decide cycle commits
//!   and emit the deterministic ordering of committed cells (this is the
//!   "lineage commit" rule from the Bullshark-family family).
//! - [`pivot`] — thin wrapper around the existing PoU-weighted RR
//!   schedule helper from Phase 1, mapping cycle index → elected
//!   pivot author.
//!
//! ## Status
//!
//! Phase 2.1.1: skeleton + state types. Subsequent commits implement the
//! gossip wiring, quorum collection, and the lineage commit walker.
//!
//! ## Compatibility
//!
//! This module is self-contained. Until the migration gate is flipped
//! (`SAVITRI_CONSENSUS_VERSION=v2`), the V0.1 single-proposer
//! `BlockCertificate` path remains authoritative. The Lattice runtime
//! can be enabled in observation-only mode for pre-activation testing.

pub mod aggregator;
pub mod commit;
pub mod pivot;

// Re-export the canonical aggregator + commit types so downstream crates
// can refer to them as `savitri_consensus::lattice::LatticeAggregator`.
pub use aggregator::{AggregatorConfig, AggregatorError, AttestationOutcome, LatticeAggregator};
pub use commit::{CommitDecision, LineageCommit};
pub use pivot::pivot_for_cycle;
