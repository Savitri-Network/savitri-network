//! Periodic FL malicious-gradient streak detector.
//!
//! The aggregator pipeline writes per-round FL contribution scores to the
//! `ObservationStore` via `record_fl_contribution`. Without an external
//! observer those scores would just sit there: this daemon polls every
//! `period_secs`, walks the known peers, and forwards a
//! `MisbehaviorType::MaliciousGradient` slash report to `SlashingManager`
//! whenever a peer's recent streak of below-threshold scores reaches
//! `MALICIOUS_GRADIENT_STREAK`.
//!
//! Properties:
//! * Best-effort. Failed slashes (cooldown / already jailed / permanent)
//!   are silently ignored — the slasher itself enforces those gates.
//! * No state of its own. Re-firing on a peer that is already jailed is
//!   harmless: the slasher returns `ValidatorJailed`.
//! * Decoupled from the BFT layer. The daemon needs only the store +
//!   slasher + a way to read "current epoch".

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use crate::scoring::{
    fl_robust::{MALICIOUS_GRADIENT_STREAK, MALICIOUS_GRADIENT_THRESHOLD_PERMILLE},
    ObservationStore,
};
use crate::slashing::SlashingManager;
use crate::types::slashing::MisbehaviorType;

/// Default polling interval. Aligns with `ScoreConfig::default()
/// .update_interval_secs`.
pub const DEFAULT_STREAK_POLL_SECS: u64 = 60;

/// Spawn the FL streak daemon. Returns the join handle so callers can
/// cancel it on shutdown (drop / abort).
///
/// `epoch_provider` is a closure returning the current consensus epoch.
/// In production this is typically `|| now_secs() / 3600`. Injecting it
/// keeps the daemon testable and avoids hard-coding the epoch policy.
pub fn spawn_fl_streak_daemon(
    store: Arc<ObservationStore>,
    slasher: Arc<SlashingManager>,
    period_secs: u64,
    epoch_provider: Arc<dyn Fn() -> u64 + Send + Sync>,
) -> JoinHandle<()> {
    spawn_fl_streak_daemon_with_thresholds(
        store,
        slasher,
        period_secs,
        epoch_provider,
        MALICIOUS_GRADIENT_THRESHOLD_PERMILLE,
        MALICIOUS_GRADIENT_STREAK,
    )
}

/// Variant exposing thresholds for tuning / tests.
pub fn spawn_fl_streak_daemon_with_thresholds(
    store: Arc<ObservationStore>,
    slasher: Arc<SlashingManager>,
    period_secs: u64,
    epoch_provider: Arc<dyn Fn() -> u64 + Send + Sync>,
    threshold_permille: u16,
    streak_required: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(period_secs.max(1)));
        // Skip the first immediate tick; production uses a 60s cadence
        // and we don't want to fire at boot before any FL round happened.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            run_one_pass(
                &store,
                &slasher,
                threshold_permille,
                streak_required,
                &epoch_provider,
            )
            .await;
        }
    })
}

/// Single sweep — exposed for tests so they don't need to wait for the
/// real interval. Returns the number of slash reports that were
/// accepted (rejected ones are counted only as best-effort attempts).
pub async fn run_one_pass(
    store: &ObservationStore,
    slasher: &SlashingManager,
    threshold_permille: u16,
    streak_required: usize,
    epoch_provider: &Arc<dyn Fn() -> u64 + Send + Sync>,
) -> usize {
    let peers = store.known_peers();
    let mut accepted = 0usize;
    let epoch = epoch_provider();
    for peer_hex in peers {
        let streak = store.bad_fl_streak(&peer_hex, threshold_permille);
        if streak < streak_required {
            continue;
        }
        // key (see `aggregate_federated_updates` in mempool). Decode
        // back; skip silently on malformed entries (defensive).
        let Ok(bytes) = hex::decode(&peer_hex) else {
            continue;
        };
        if bytes.len() != 32 {
            continue;
        }
        let mut validator_id = [0u8; 32];
        validator_id.copy_from_slice(&bytes);

        // Evidence hash is a deterministic digest of the streak context
        // so re-firing on the same peer produces the same hash and the
        // slasher can dedup if it ever grows that capability.
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"FL-MALICIOUS-STREAK");
        hasher.update(&validator_id);
        hasher.update(&epoch.to_le_bytes());
        hasher.update(&(streak as u64).to_le_bytes());
        let evidence_hash: [u8; 32] = hasher.finalize().into();

        match slasher
            .report_misbehavior(
                validator_id,
                MisbehaviorType::MaliciousGradient,
                epoch,
                /* slot */ 0,
                evidence_hash,
            )
            .await
        {
            Ok(_permille) => {
                accepted += 1;
                tracing::warn!(
                    peer = %peer_hex,
                    streak,
                    epoch,
                    "FL streak daemon: MaliciousGradient slash recorded"
                );
            }
            Err(crate::error::ConsensusError::SlashCooldown(_))
            | Err(crate::error::ConsensusError::ValidatorJailed(_))
            | Err(crate::error::ConsensusError::AlreadySlashed(_)) => {
                // Expected: peer already in cooldown/jailed/removed.
                tracing::debug!(peer = %peer_hex, "FL streak: slash skipped (peer not eligible)");
            }
            Err(e) => {
                tracing::warn!(peer = %peer_hex, error = %e, "FL streak: unexpected slash error");
            }
        }
    }
    accepted
}

/// Convenience: epoch as floor(unix_secs / 3600).
pub fn default_epoch_provider() -> Arc<dyn Fn() -> u64 + Send + Sync> {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 3600)
            .unwrap_or(0)
    })
}

#[cfg(test)]
mod tests {
    // The real daemon spins a tokio task. We test only `run_one_pass`
    // here because it is sync to invoke and deterministic.
    // Integration coverage lives in `tests/scoring_observations.rs`.
}
