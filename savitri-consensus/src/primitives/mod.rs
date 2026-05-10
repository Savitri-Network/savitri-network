//! Canonical consensus primitives.
//!
//! This module is the single source of truth for the small, frequently
//! duplicated primitives that the workspace needs at every layer (epoch
//! arithmetic, block hashing, quorum thresholds). Historically each call
//! site re-implemented these inline, drifting in formula and field
//! ordering over time — see `memory/architectural_debt.md` Tier 1.
//!
//! Every implementation in `savitri-{lightnode,masternode,core}` and in
//! the `traits/` default impls of this crate must delegate here. New
//! primitives added in the future MUST live here, not be duplicated at
//! call sites.
//!
//! See `memory/refactor_plan_2026-04-28.md` for the consolidation plan.

pub mod epoch;
pub mod group_id;
pub mod hashing;
pub mod quorum;
pub mod timeouts;
