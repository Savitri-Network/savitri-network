//! Slashing manager for consensus enforcement
//!
//! applies score penalties using u32 permille arithmetic, manages jail transitions,
//! and handles permanent slashing for repeat offenders.
//!
//! SECURITY: All penalty calculations use integer permille (parts per 1000) to
//! avoid f64 non-determinism in consensus-critical paths.

use crate::error::ConsensusError;
use crate::scoring::{ObservationStore, SlashReason};
use crate::types::slashing::{
    MisbehaviorType, SlashingConfig, SlashingRecord, ValidatorSlashingState,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Map the consensus misbehavior taxonomy to the scoring slash reason so
/// observation-store consumers see a consistent vocabulary.
fn map_misbehavior_to_slash_reason(m: &MisbehaviorType) -> SlashReason {
    match m {
        MisbehaviorType::DoubleVote => SlashReason::DoubleVote,
        MisbehaviorType::Equivocation => SlashReason::Equivocation,
        MisbehaviorType::InvalidProposal => SlashReason::InvalidBlock,
        MisbehaviorType::MaliciousGradient => SlashReason::MaliciousGradient,
        MisbehaviorType::Downtime => SlashReason::Other,
    }
}

pub struct SlashingManager {
    /// Slashing configuration
    config: SlashingConfig,
    states: Arc<RwLock<HashMap<[u8; 32], ValidatorSlashingState>>>,
    /// Optional PoU observation store. When set, every accepted slash is
    /// mirrored into the store so `build_integrity_measurement` can apply
    /// the 100-permille-per-slash penalty to the integrity score.
    observations: Option<Arc<ObservationStore>>,
}

impl SlashingManager {
    /// Create a new slashing manager with the given configuration
    pub fn new(config: SlashingConfig) -> Self {
        Self {
            config,
            states: Arc::new(RwLock::new(HashMap::new())),
            observations: None,
        }
    }

    /// Create a new slashing manager with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SlashingConfig::default())
    }

    /// Attach a shared observation store so accepted slashes feed into PoU
    /// integrity scoring. Idempotent.
    pub fn set_observations(&mut self, store: Arc<ObservationStore>) {
        self.observations = Some(store);
    }

    /// Get a reference to the slashing configuration
    pub fn config(&self) -> &SlashingConfig {
        &self.config
    }

    ///
    /// This method:
    /// 1. Checks cooldown enforcement (rejects if too recent)
    /// 2. Records the misbehavior with an audit trail
    /// 3. Calculates and returns the score penalty in permille
    /// 4. Transitions to Jailed if cumulative slashes reach the threshold
    /// 5. Transitions to permanently Slashed if max_cumulative_slashes reached
    ///
    /// # Arguments
    /// * `misbehavior` - Type of misbehavior detected
    /// * `current_epoch` - Current consensus epoch
    /// * `current_slot` - Current consensus slot
    /// * `evidence_hash` - Blake3 hash of the evidence
    ///
    /// # Returns
    pub async fn report_misbehavior(
        &self,
        validator_id: [u8; 32],
        misbehavior: MisbehaviorType,
        current_epoch: u64,
        current_slot: u64,
        evidence_hash: [u8; 32],
    ) -> Result<u32, ConsensusError> {
        let mut states = self.states.write().await;
        let state = states
            .entry(validator_id)
            .or_insert_with(ValidatorSlashingState::default);

        // Check if already permanently slashed
        if state.is_permanently_slashed {
            tracing::warn!(
                validator = hex::encode(validator_id),
                misbehavior = %misbehavior,
                "Misbehavior report for permanently slashed validator — ignoring"
            );
            return Err(ConsensusError::AlreadySlashed(format!(
                "Validator {} is permanently slashed",
                hex::encode(validator_id)
            )));
        }

        // Check if currently jailed
        if state.is_jailed_at(current_epoch) {
            tracing::warn!(
                validator = hex::encode(validator_id),
                misbehavior = %misbehavior,
                jailed_until = state.jailed_until_epoch,
                "Misbehavior report for jailed validator — ignoring"
            );
            return Err(ConsensusError::ValidatorJailed(format!(
                "Validator {} is jailed until epoch {}",
                hex::encode(validator_id),
                state.jailed_until_epoch
            )));
        }

        // Enforce cooldown: reject if last slash was too recent
        if state.last_slash_epoch > 0
            && current_epoch.saturating_sub(state.last_slash_epoch) < self.config.cooldown_epochs
        {
            tracing::warn!(
                validator = hex::encode(validator_id),
                misbehavior = %misbehavior,
                last_slash_epoch = state.last_slash_epoch,
                cooldown = self.config.cooldown_epochs,
                "Slash cooldown active — rejecting report"
            );
            return Err(ConsensusError::SlashCooldown(format!(
                "Validator {} is in cooldown until epoch {} (last slashed at epoch {})",
                hex::encode(validator_id),
                state.last_slash_epoch + self.config.cooldown_epochs,
                state.last_slash_epoch
            )));
        }

        // Determine slash amount
        let slash_permille = self.config.slash_permille_for(&misbehavior);

        // Record the misbehavior
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let record = SlashingRecord {
            validator_id,
            misbehavior_type: misbehavior.clone(),
            epoch: current_epoch,
            slot: current_slot,
            evidence_hash,
            slash_amount_permille: slash_permille,
            timestamp,
        };

        state.records.push(record);
        state.cumulative_slashes += 1;
        state.last_slash_epoch = current_epoch;

        // Mirror the slash into the PoU observation store (if wired) so the
        // scorer's integrity component reflects it. Done before jail/permanent
        // transitions below so every accepted slash counts, regardless of
        if let Some(store) = &self.observations {
            store.record_slash(
                &hex::encode(validator_id),
                map_misbehavior_to_slash_reason(&misbehavior),
            );
        }

        tracing::warn!(
            validator = hex::encode(validator_id),
            misbehavior = %misbehavior,
            slash_permille = slash_permille,
            cumulative_slashes = state.cumulative_slashes,
            epoch = current_epoch,
            slot = current_slot,
            "SLASHING: Validator penalized for misbehavior"
        );

        // Check if max cumulative slashes reached -> permanent slash
        if state.cumulative_slashes >= self.config.max_cumulative_slashes {
            state.is_permanently_slashed = true;
            tracing::warn!(
                validator = hex::encode(validator_id),
                cumulative_slashes = state.cumulative_slashes,
                "SLASHING: Validator permanently slashed — max cumulative slashes reached"
            );
            return Ok(slash_permille);
        }

        // Check if cumulative slash amount exceeds jail threshold
        let total_slash_permille: u32 = state.records.iter().map(|r| r.slash_amount_permille).sum();

        // Normalize: total_slash_permille is cumulative across all events.
        // Compare cumulative count against jail threshold (as permille of max).
        // Jail if cumulative slashes * 1000 / max_cumulative_slashes >= jail_threshold_permille.
        // This provides a proportional jailing mechanism.
        let cumulative_ratio_permille = if self.config.max_cumulative_slashes > 0 {
            (state.cumulative_slashes as u64 * 1000) / (self.config.max_cumulative_slashes as u64)
        } else {
            1000
        };

        if cumulative_ratio_permille >= self.config.jail_threshold_permille as u64 {
            state.jailed_until_epoch = current_epoch + self.config.jail_duration_epochs;
            tracing::warn!(
                validator = hex::encode(validator_id),
                cumulative_ratio_permille = cumulative_ratio_permille,
                jail_threshold = self.config.jail_threshold_permille,
                jailed_until = state.jailed_until_epoch,
                "SLASHING: Validator jailed — cumulative threshold exceeded"
            );
        }

        Ok(slash_permille)
    }

    ///
    pub async fn process_jail_expiry(&self, current_epoch: u64) -> Vec<[u8; 32]> {
        let states = self.states.read().await;
        let mut unjailed = Vec::new();

        for (&validator_id, state) in states.iter() {
            if !state.is_permanently_slashed
                && state.jailed_until_epoch > 0
                && state.jailed_until_epoch <= current_epoch
            {
                unjailed.push(validator_id);
                tracing::info!(
                    validator = hex::encode(validator_id),
                    epoch = current_epoch,
                    "Validator jail term expired — eligible for reactivation"
                );
            }
        }

        unjailed
    }

    ///
    /// This should be called after `process_jail_expiry` to actually update state.
    pub async fn unjail_validator(
        &self,
        validator_id: &[u8; 32],
        current_epoch: u64,
    ) -> Result<(), ConsensusError> {
        let mut states = self.states.write().await;
        if let Some(state) = states.get_mut(validator_id) {
            if state.is_permanently_slashed {
                return Err(ConsensusError::AlreadySlashed(format!(
                    "Validator {} is permanently slashed and cannot be unjailed",
                    hex::encode(validator_id)
                )));
            }
            if state.jailed_until_epoch <= current_epoch {
                state.jailed_until_epoch = 0;
                tracing::info!(
                    validator = hex::encode(validator_id),
                    epoch = current_epoch,
                    "Validator unjailed successfully"
                );
                Ok(())
            } else {
                Err(ConsensusError::ValidatorJailed(format!(
                    "Validator {} is still jailed until epoch {}",
                    hex::encode(validator_id),
                    state.jailed_until_epoch
                )))
            }
        } else {
            Ok(()) // No state means never slashed, nothing to unjail
        }
    }

    pub async fn is_validator_active(&self, validator_id: &[u8; 32], current_epoch: u64) -> bool {
        let states = self.states.read().await;
        match states.get(validator_id) {
            Some(state) => state.is_active_at(current_epoch),
            None => true, // No slashing state means the validator is active
        }
    }

    pub async fn get_validator_state(
        &self,
        validator_id: &[u8; 32],
    ) -> Option<ValidatorSlashingState> {
        let states = self.states.read().await;
        states.get(validator_id).cloned()
    }

    pub async fn get_validator_records(&self, validator_id: &[u8; 32]) -> Vec<SlashingRecord> {
        let states = self.states.read().await;
        states
            .get(validator_id)
            .map(|s| s.records.clone())
            .unwrap_or_default()
    }

    /// Apply the score penalty using permille arithmetic.
    ///
    /// Formula: `new_score = current_score - (current_score * slash_permille) / 1000`
    ///
    /// This is a pure helper function — it does not mutate any state.
    pub fn apply_score_penalty(current_score: u32, slash_permille: u32) -> u32 {
        let penalty = (current_score as u64 * slash_permille as u64) / 1000;
        current_score.saturating_sub(penalty as u32)
    }
}
