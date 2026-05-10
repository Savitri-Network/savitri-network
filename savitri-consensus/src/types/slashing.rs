//! Slashing types for consensus enforcement
//!
//! This module defines the data structures for tracking and enforcing
//!
//! SECURITY: All slashing amounts use u32 permille (parts per 1000)
//! arithmetic to avoid f64 non-determinism in consensus-critical paths.

use serde::{Deserialize, Serialize};

/// Types of misbehavior that can trigger slashing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MisbehaviorType {
    /// Validator voted twice in the same round
    DoubleVote,
    /// Validator proposed conflicting blocks for the same slot
    Equivocation,
    /// Validator was offline for too many consecutive epochs
    Downtime,
    /// Validator submitted a malformed or invalid proposal
    InvalidProposal,
    /// Federated Learning client produced a sustained streak of gradient
    /// norm-clip gate). See `scoring::fl_robust`.
    MaliciousGradient,
}

impl std::fmt::Display for MisbehaviorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MisbehaviorType::DoubleVote => write!(f, "DoubleVote"),
            MisbehaviorType::Equivocation => write!(f, "Equivocation"),
            MisbehaviorType::Downtime => write!(f, "Downtime"),
            MisbehaviorType::InvalidProposal => write!(f, "InvalidProposal"),
            MisbehaviorType::MaliciousGradient => write!(f, "MaliciousGradient"),
        }
    }
}

/// Slashing configuration
///
/// All penalty amounts are expressed in permille (parts per 1000).
/// For example, `double_vote_slash_permille = 500` means a 50% score penalty.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashingConfig {
    /// Score penalty for double voting (permille, 0-1000)
    pub double_vote_slash_permille: u32,
    /// Score penalty for equivocation (permille, 0-1000)
    pub equivocation_slash_permille: u32,
    /// Score penalty for downtime (permille, 0-1000)
    pub downtime_slash_permille: u32,
    /// Score penalty for invalid proposals (permille, 0-1000)
    pub invalid_proposal_slash_permille: u32,
    /// Score penalty for sustained malicious FL gradients (permille, 0-1000)
    pub malicious_gradient_slash_permille: u32,
    /// Cumulative slash threshold to trigger jailing (permille, 0-1000)
    pub jail_threshold_permille: u32,
    pub jail_duration_epochs: u64,
    pub cooldown_epochs: u64,
    /// Maximum cumulative slashes before permanent removal
    pub max_cumulative_slashes: u32,
}

impl Default for SlashingConfig {
    fn default() -> Self {
        Self {
            double_vote_slash_permille: 500,
            equivocation_slash_permille: 500,
            downtime_slash_permille: 100,
            invalid_proposal_slash_permille: 250,
            // A sustained malicious-gradient streak already cost the peer
            // most of its FL contribution score; the slash adds a stronger
            // systemic penalty that also counts toward jailing.
            malicious_gradient_slash_permille: 300,
            // 600 permille of max_cumulative_slashes triggers jail. With the
            // default `max_cumulative_slashes = 5`, this means jail after the
            // third accepted slash (3/5 = 600 permille). A lower threshold
            // would jail on the very first infraction (1/5 = 200 permille),
            // leaving no room for the cooldown window to protect against
            // repeat offenders spamming reports.
            jail_threshold_permille: 600,
            jail_duration_epochs: 10,
            cooldown_epochs: 5,
            max_cumulative_slashes: 5,
        }
    }
}

impl SlashingConfig {
    /// Get the slash amount in permille for a given misbehavior type
    pub fn slash_permille_for(&self, misbehavior: &MisbehaviorType) -> u32 {
        match misbehavior {
            MisbehaviorType::DoubleVote => self.double_vote_slash_permille,
            MisbehaviorType::Equivocation => self.equivocation_slash_permille,
            MisbehaviorType::Downtime => self.downtime_slash_permille,
            MisbehaviorType::InvalidProposal => self.invalid_proposal_slash_permille,
            MisbehaviorType::MaliciousGradient => self.malicious_gradient_slash_permille,
        }
    }
}

/// A single slashing record for audit trail
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashingRecord {
    /// Validator that was slashed (32-byte public key)
    pub validator_id: [u8; 32],
    /// Type of misbehavior
    pub misbehavior_type: MisbehaviorType,
    /// Epoch when the misbehavior occurred
    pub epoch: u64,
    /// Slot when the misbehavior occurred
    pub slot: u64,
    /// Blake3 hash of the evidence (e.g., conflicting votes)
    pub evidence_hash: [u8; 32],
    /// Penalty applied in permille
    pub slash_amount_permille: u32,
    /// Unix timestamp when the slash was recorded
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorSlashingState {
    /// Total number of cumulative slashes
    pub cumulative_slashes: u32,
    /// Epoch of the most recent slash (0 if never slashed)
    pub last_slash_epoch: u64,
    pub jailed_until_epoch: u64,
    pub is_permanently_slashed: bool,
    /// Full history of slashing records
    pub records: Vec<SlashingRecord>,
}

impl Default for ValidatorSlashingState {
    fn default() -> Self {
        Self {
            cumulative_slashes: 0,
            last_slash_epoch: 0,
            jailed_until_epoch: 0,
            is_permanently_slashed: false,
            records: Vec::new(),
        }
    }
}

impl ValidatorSlashingState {
    pub fn is_jailed_at(&self, current_epoch: u64) -> bool {
        self.jailed_until_epoch > current_epoch
    }

    pub fn is_active_at(&self, current_epoch: u64) -> bool {
        !self.is_permanently_slashed && !self.is_jailed_at(current_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_slashing_config() {
        let config = SlashingConfig::default();
        assert_eq!(config.double_vote_slash_permille, 500);
        assert_eq!(config.equivocation_slash_permille, 500);
        assert_eq!(config.downtime_slash_permille, 100);
        assert_eq!(config.invalid_proposal_slash_permille, 250);
        assert_eq!(config.malicious_gradient_slash_permille, 300);
        assert_eq!(config.jail_threshold_permille, 600);
        assert_eq!(config.jail_duration_epochs, 10);
        assert_eq!(config.cooldown_epochs, 5);
        assert_eq!(config.max_cumulative_slashes, 5);
    }

    #[test]
    fn test_slash_permille_for_misbehavior() {
        let config = SlashingConfig::default();
        assert_eq!(config.slash_permille_for(&MisbehaviorType::DoubleVote), 500);
        assert_eq!(
            config.slash_permille_for(&MisbehaviorType::Equivocation),
            500
        );
        assert_eq!(config.slash_permille_for(&MisbehaviorType::Downtime), 100);
        assert_eq!(
            config.slash_permille_for(&MisbehaviorType::InvalidProposal),
            250
        );
    }

    #[test]
    fn test_validator_slashing_state_default() {
        let state = ValidatorSlashingState::default();
        assert_eq!(state.cumulative_slashes, 0);
        assert_eq!(state.last_slash_epoch, 0);
        assert_eq!(state.jailed_until_epoch, 0);
        assert!(!state.is_permanently_slashed);
        assert!(state.records.is_empty());
    }

    #[test]
    fn test_is_jailed_at() {
        let mut state = ValidatorSlashingState::default();
        state.jailed_until_epoch = 100;
        assert!(state.is_jailed_at(50));
        assert!(state.is_jailed_at(99));
        assert!(!state.is_jailed_at(100));
        assert!(!state.is_jailed_at(200));
    }

    #[test]
    fn test_is_active_at() {
        let mut state = ValidatorSlashingState::default();
        assert!(state.is_active_at(0));

        state.jailed_until_epoch = 100;
        assert!(!state.is_active_at(50));
        assert!(state.is_active_at(100));

        state.jailed_until_epoch = 0;
        state.is_permanently_slashed = true;
        assert!(!state.is_active_at(0));
        assert!(!state.is_active_at(1000));
    }

    #[test]
    fn test_misbehavior_type_display() {
        assert_eq!(MisbehaviorType::DoubleVote.to_string(), "DoubleVote");
        assert_eq!(MisbehaviorType::Equivocation.to_string(), "Equivocation");
        assert_eq!(MisbehaviorType::Downtime.to_string(), "Downtime");
        assert_eq!(
            MisbehaviorType::InvalidProposal.to_string(),
            "InvalidProposal"
        );
    }
}
