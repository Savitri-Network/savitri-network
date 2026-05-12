//! Proposer state machine (Tier 6 refactor — Phase 1: shadow-mode introduction).
//!
//! This module introduces a typed state machine that captures the full lifecycle
//! of the local node as an intra-group block proposer. It is intentionally
//! introduced ALONGSIDE the four legacy boolean flags described in
//! `memory/proposer_state_audit_2026-04-28.md`:
//!
//! - `block_loop_running` (`AtomicBool`)
//! - `is_intragroup_proposer` (`Option<RwLock<bool>>`)
//! - `is_in_intra_group` (`Option<RwLock<bool>>`)
//! - `proposer_state.is_active` (`bool`)
//!
//! Phase 1 only creates the structure; no caller is migrated yet. Subsequent
//! phases will (a) mirror the four flags onto this state machine, (b) migrate
//! readers, (c) migrate writers, and (d) delete the legacy flags.
//!
//! # Allowed transitions
//!
//! ```text
//! Idle ──try_elect──> Elected
//! Elected ──try_start_producing──> Producing
//! Elected ──try_step_down──> SteppingDown
//! Producing ──try_step_down──> SteppingDown
//! SteppingDown ──(future auto-transition)──> Idle
//! ```
//!
//! All other transitions return `TransitionError::InvalidTransition`.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{Mutex, RwLock};

/// Local numeric group identifier used by the state machine. Distinct from the
/// `String` group id used by `group_manager` so the enum can remain `Copy`.
pub type GroupId = u64;

/// Maximum number of transitions retained in the diagnostic log.
const TRANSITION_LOG_CAPACITY: usize = 100;

/// Reason recorded when the proposer steps down from `Elected` or `Producing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepDownReason {
    /// Current epoch ended; rotate out cleanly.
    EpochEnd,
    /// Another node was elected for the next round; we yield.
    NewElectionElsewhere,
    /// Operator-initiated step-down (RPC or graceful shutdown).
    ManualStepDown,
    /// Crash detected by external watchdog (block loop died, deadlock, etc.).
    Crash,
}

/// Typed state of the local node as an intra-group proposer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposerState {
    /// Not part of an active election; default state at startup.
    Idle,
    /// Elected as proposer for `(group, round)` but block loop has not started.
    Elected {
        group: GroupId,
        round: u32,
        since: Instant,
    },
    /// Block production loop is actively running for `(group, round)`.
    Producing {
        group: GroupId,
        round: u32,
        height: u64,
        count: u64,
        since: Instant,
    },
    /// Voluntary or forced exit; loop draining, awaiting return to `Idle`.
    SteppingDown {
        reason: StepDownReason,
        since: Instant,
    },
}

impl ProposerState {
    /// Short tag for log messages and diagnostics.
    pub fn variant_name(&self) -> &'static str {
        match self {
            ProposerState::Idle => "Idle",
            ProposerState::Elected { .. } => "Elected",
            ProposerState::Producing { .. } => "Producing",
            ProposerState::SteppingDown { .. } => "SteppingDown",
        }
    }
}

/// Error returned when an invalid transition is attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionError {
    /// Attempted transition is not allowed from the current state.
    InvalidTransition {
        from: ProposerState,
        attempted: &'static str,
    },
}

impl fmt::Display for TransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionError::InvalidTransition { from, attempted } => write!(
                f,
                "invalid proposer-state transition: cannot '{}' from {:?}",
                attempted, from
            ),
        }
    }
}

impl std::error::Error for TransitionError {}

/// Single recorded transition: `(timestamp, from, to)`.
pub type TransitionRecord = (Instant, ProposerState, ProposerState);

/// Async-safe state machine wrapping `ProposerState` with a bounded transition
/// log for diagnostics.
#[derive(Clone)]
pub struct ProposerStateMachine {
    state: Arc<RwLock<ProposerState>>,
    transition_log: Arc<Mutex<VecDeque<TransitionRecord>>>,
}

impl ProposerStateMachine {
    /// Create a new state machine in `Idle` state with an empty transition log.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ProposerState::Idle)),
            transition_log: Arc::new(Mutex::new(VecDeque::with_capacity(TRANSITION_LOG_CAPACITY))),
        }
    }

    /// Snapshot of the current state.
    pub async fn current(&self) -> ProposerState {
        *self.state.read().await
    }

    /// Attempt to transition `Idle -> Elected{group, round}`.
    pub async fn try_elect(
        &self,
        group: GroupId,
        round: u32,
    ) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::Idle => {
                let next = ProposerState::Elected {
                    group,
                    round,
                    since: Instant::now(),
                };
                self.record_transition(*guard, next).await;
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "try_elect",
            }),
        }
    }

    /// Attempt to transition `Elected -> Producing{height, count: 0}`.
    pub async fn try_start_producing(&self, height: u64) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::Elected { group, round, .. } => {
                let next = ProposerState::Producing {
                    group,
                    round,
                    height,
                    count: 0,
                    since: Instant::now(),
                };
                self.record_transition(*guard, next).await;
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "try_start_producing",
            }),
        }
    }

    /// Attempt to transition `Elected|Producing -> SteppingDown{reason}`.
    pub async fn try_step_down(
        &self,
        reason: StepDownReason,
    ) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::Elected { .. } | ProposerState::Producing { .. } => {
                let next = ProposerState::SteppingDown {
                    reason,
                    since: Instant::now(),
                };
                self.record_transition(*guard, next).await;
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "try_step_down",
            }),
        }
    }

    /// Attempt to transition `SteppingDown -> Idle`. Will be invoked by an
    /// auto-transition timer in a later phase; exposed now for symmetry and
    /// tests.
    pub async fn try_finish_step_down(&self) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::SteppingDown { .. } => {
                let next = ProposerState::Idle;
                self.record_transition(*guard, next).await;
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "try_finish_step_down",
            }),
        }
    }

    /// Snapshot of the transition log (oldest first).
    pub async fn transition_log_snapshot(&self) -> Vec<TransitionRecord> {
        let guard = self.transition_log.lock().await;
        guard.iter().copied().collect()
    }

    async fn record_transition(&self, from: ProposerState, to: ProposerState) {
        let mut log = self.transition_log.lock().await;
        if log.len() == TRANSITION_LOG_CAPACITY {
            log.pop_front();
        }
        log.push_back((Instant::now(), from, to));
    }

    // drift detector compares the SM-derived value against the flag-derived
    // helpers become the single source of truth.

    /// Returns `true` iff the SM is in `Elected` or `Producing`.
    /// Replaces reads of `is_intragroup_proposer` flag.
    pub async fn is_proposer_role(&self) -> bool {
        matches!(
            self.current().await,
            ProposerState::Elected { .. } | ProposerState::Producing { .. }
        )
    }

    /// Returns `true` iff the SM is in `Producing` (block loop is alive).
    /// Replaces reads of `block_loop_running` flag.
    pub async fn is_loop_active(&self) -> bool {
        matches!(self.current().await, ProposerState::Producing { .. })
    }

    /// Increment the block-produced counter while staying in `Producing`.
    /// Returns `InvalidTransition` if not in `Producing`.
    /// Replaces writes to `proposer_state.block_proposal_count`.
    pub async fn record_block_produced(&self) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::Producing {
                group,
                round,
                height,
                count,
                since,
            } => {
                let next = ProposerState::Producing {
                    group,
                    round,
                    height,
                    count: count.saturating_add(1),
                    since,
                };
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "record_block_produced",
            }),
        }
    }

    /// Update `last_block_height` while staying in `Producing`.
    /// Replaces writes to `proposer_state.last_block_height`.
    pub async fn record_height(&self, h: u64) -> Result<ProposerState, TransitionError> {
        let mut guard = self.state.write().await;
        match *guard {
            ProposerState::Producing {
                group,
                round,
                count,
                since,
                ..
            } => {
                let next = ProposerState::Producing {
                    group,
                    round,
                    height: h,
                    count,
                    since,
                };
                *guard = next;
                Ok(next)
            }
            other => Err(TransitionError::InvalidTransition {
                from: other,
                attempted: "record_height",
            }),
        }
    }
}

impl Default for ProposerStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
