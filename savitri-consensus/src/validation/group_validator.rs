//!
//! group membership, leader election, and group state integrity.

use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct GroupValidationConfig {
    /// Minimum group size
    pub min_group_size: usize,
    /// Maximum group size
    pub max_group_size: usize,
    /// Minimum active members for consensus
    pub min_active_members: usize,
    pub enable_leader_validation: bool,
    pub enable_membership_validation: bool,
    /// Maximum group age before refresh (seconds)
    pub max_group_age_secs: u64,
    /// Quorum percentage (0.0-1.0)
    pub quorum_percentage: f64,
}

impl Default for GroupValidationConfig {
    fn default() -> Self {
        Self {
            min_group_size: 4,
            max_group_size: 100,
            min_active_members: 3,
            enable_leader_validation: true,
            enable_membership_validation: true,
            max_group_age_secs: 3600,
            quorum_percentage: 0.67,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupValidationStats {
    pub groups_validated: u64,
    pub groups_accepted: u64,
    pub groups_rejected: u64,
    pub membership_failures: u64,
    pub leader_failures: u64,
    pub quorum_failures: u64,
}

/// Group state
#[derive(Debug, Clone)]
pub struct GroupState {
    pub group_id: u64,
    pub members: HashSet<[u8; 32]>,
    pub leader: Option<[u8; 32]>,
    pub epoch: u64,
    pub created_at: u64,
    pub last_activity: u64,
    pub active_members: HashSet<[u8; 32]>,
}

impl GroupState {
    /// Create new group state
    pub fn new(group_id: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            group_id,
            members: HashSet::new(),
            leader: None,
            epoch: 0,
            created_at: now,
            last_activity: now,
            active_members: HashSet::new(),
        }
    }

    /// Add member to group
    pub fn add_member(&mut self, member: [u8; 32]) {
        self.members.insert(member);
        self.last_activity = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Remove member from group
    pub fn remove_member(&mut self, member: &[u8; 32]) {
        self.members.remove(member);
        self.active_members.remove(member);
        if self.leader.as_ref() == Some(member) {
            self.leader = None;
        }
        self.last_activity = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Mark member as active
    pub fn mark_active(&mut self, member: &[u8; 32]) {
        if self.members.contains(member) {
            self.active_members.insert(*member);
        }
    }

    /// Mark member as inactive
    pub fn mark_inactive(&mut self, member: &[u8; 32]) {
        self.active_members.remove(member);
    }

    /// Set group leader
    pub fn set_leader(&mut self, leader: [u8; 32]) -> bool {
        if self.members.contains(&leader) {
            self.leader = Some(leader);
            self.epoch += 1;
            true
        } else {
            false
        }
    }

    /// Check if group has quorum
    pub fn has_quorum(&self, quorum_percentage: f64) -> bool {
        if self.members.is_empty() {
            return false;
        }
        let required = (self.members.len() as f64 * quorum_percentage).ceil() as usize;
        self.active_members.len() >= required
    }
}

pub struct GroupValidator {
    config: GroupValidationConfig,
    stats: Arc<RwLock<GroupValidationStats>>,
    groups: Arc<RwLock<HashMap<u64, GroupState>>>,
}

impl GroupValidator {
    pub fn new(config: GroupValidationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(GroupValidationStats::default())),
            groups: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Validate a group
    pub async fn validate_group(&self, group_id: u64) -> Result<ValidationResult> {
        let groups = self.groups.read().await;

        let group = match groups.get(&group_id) {
            Some(g) => g,
            None => {
                self.record_rejection(GroupRejectionReason::NotFound).await;
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::GroupNotFound,
                ));
            }
        };

        if group.members.len() < self.config.min_group_size {
            self.record_rejection(GroupRejectionReason::TooSmall).await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Group too small: {} < {}",
                    group.members.len(),
                    self.config.min_group_size
                )),
            ));
        }

        if group.members.len() > self.config.max_group_size {
            self.record_rejection(GroupRejectionReason::TooLarge).await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Group too large: {} > {}",
                    group.members.len(),
                    self.config.max_group_size
                )),
            ));
        }

        if group.active_members.len() < self.config.min_active_members {
            self.record_rejection(GroupRejectionReason::InsufficientActive)
                .await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(format!(
                    "Insufficient active members: {} < {}",
                    group.active_members.len(),
                    self.config.min_active_members
                )),
            ));
        }

        if !group.has_quorum(self.config.quorum_percentage) {
            self.record_rejection(GroupRejectionReason::NoQuorum).await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::InsufficientMembers,
            ));
        }

        if self.config.enable_leader_validation {
            if let Some(leader) = &group.leader {
                if !group.members.contains(leader) {
                    self.record_rejection(GroupRejectionReason::InvalidLeader)
                        .await;
                    return Ok(ValidationResult::Invalid(
                        crate::types::validation::ValidationError::ProposerNotInGroup,
                    ));
                }
                if !group.active_members.contains(leader) {
                    self.record_rejection(GroupRejectionReason::InactiveLeader)
                        .await;
                    return Ok(ValidationResult::Invalid(
                        crate::types::validation::ValidationError::GroupInactive,
                    ));
                }
            }
        }

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if current_time - group.created_at > self.config.max_group_age_secs {
            self.record_rejection(GroupRejectionReason::Expired).await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::ValidationTimeout,
            ));
        }

        self.record_acceptance().await;
        Ok(ValidationResult::Valid)
    }

    /// Validate member is in group
    pub async fn validate_membership(
        &self,
        group_id: u64,
        member: &[u8; 32],
    ) -> Result<ValidationResult> {
        let groups = self.groups.read().await;

        let group = match groups.get(&group_id) {
            Some(g) => g,
            None => {
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::GroupNotFound,
                ));
            }
        };

        if !group.members.contains(member) {
            self.record_membership_failure().await;
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::ProposerNotInGroup,
            ));
        }

        Ok(ValidationResult::Valid)
    }

    /// Validate node is the group leader
    pub async fn validate_leader(
        &self,
        group_id: u64,
        node: &[u8; 32],
    ) -> Result<ValidationResult> {
        let groups = self.groups.read().await;

        let group = match groups.get(&group_id) {
            Some(g) => g,
            None => {
                return Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::GroupNotFound,
                ));
            }
        };

        match &group.leader {
            Some(leader) if leader == node => Ok(ValidationResult::Valid),
            Some(_) => {
                self.record_leader_failure().await;
                Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom(
                        "Node is not the leader".to_string(),
                    ),
                ))
            }
            None => {
                self.record_leader_failure().await;
                Ok(ValidationResult::Invalid(
                    crate::types::validation::ValidationError::Custom(
                        "Group has no leader".to_string(),
                    ),
                ))
            }
        }
    }

    /// Create a new group
    pub async fn create_group(&self, group_id: u64, initial_members: Vec<[u8; 32]>) -> Result<()> {
        let mut groups = self.groups.write().await;

        let mut group = GroupState::new(group_id);
        for member in initial_members {
            group.add_member(member);
            group.mark_active(&member);
        }

        groups.insert(group_id, group);
        Ok(())
    }

    /// Add member to group
    pub async fn add_member(&self, group_id: u64, member: [u8; 32]) -> Result<()> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            group.add_member(member);
        }

        Ok(())
    }

    /// Remove member from group
    pub async fn remove_member(&self, group_id: u64, member: &[u8; 32]) -> Result<()> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            group.remove_member(member);
        }

        Ok(())
    }

    /// Set group leader
    pub async fn set_leader(&self, group_id: u64, leader: [u8; 32]) -> Result<bool> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            Ok(group.set_leader(leader))
        } else {
            Ok(false)
        }
    }

    /// Mark member as active
    pub async fn mark_active(&self, group_id: u64, member: &[u8; 32]) {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(&group_id) {
            group.mark_active(member);
        }
    }

    /// Mark member as inactive
    pub async fn mark_inactive(&self, group_id: u64, member: &[u8; 32]) {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(&group_id) {
            group.mark_inactive(member);
        }
    }

    /// Get group state
    pub async fn get_group(&self, group_id: u64) -> Option<GroupState> {
        let groups = self.groups.read().await;
        groups.get(&group_id).cloned()
    }

    /// Get all groups
    pub async fn get_all_groups(&self) -> Vec<GroupState> {
        let groups = self.groups.read().await;
        groups.values().cloned().collect()
    }

    /// Delete group
    pub async fn delete_group(&self, group_id: u64) {
        let mut groups = self.groups.write().await;
        groups.remove(&group_id);
    }

    pub async fn stats(&self) -> GroupValidationStats {
        self.stats.read().await.clone()
    }

    // Internal stat recording methods
    async fn record_rejection(&self, _reason: GroupRejectionReason) {
        let mut stats = self.stats.write().await;
        stats.groups_validated += 1;
        stats.groups_rejected += 1;
    }

    async fn record_acceptance(&self) {
        let mut stats = self.stats.write().await;
        stats.groups_validated += 1;
        stats.groups_accepted += 1;
    }

    async fn record_membership_failure(&self) {
        let mut stats = self.stats.write().await;
        stats.membership_failures += 1;
    }

    async fn record_leader_failure(&self) {
        let mut stats = self.stats.write().await;
        stats.leader_failures += 1;
    }
}

/// Group rejection reasons
#[derive(Debug, Clone)]
pub enum GroupRejectionReason {
    NotFound,
    TooSmall,
    TooLarge,
    InsufficientActive,
    NoQuorum,
    InvalidLeader,
    InactiveLeader,
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_group_validator_creation() {
        let config = GroupValidationConfig::default();
        let validator = GroupValidator::new(config);
        let stats = validator.stats().await;
        assert_eq!(stats.groups_validated, 0);
    }

    #[tokio::test]
    async fn test_create_and_validate_group() {
        let validator = GroupValidator::new(GroupValidationConfig::default());

        let members: Vec<[u8; 32]> = (0..5).map(|i| [i as u8; 32]).collect();
        validator.create_group(1, members).await.unwrap();

        let result = validator.validate_group(1).await.unwrap();
        assert!(matches!(result, ValidationResult::Valid));
    }

    #[tokio::test]
    async fn test_small_group_rejection() {
        let validator = GroupValidator::new(GroupValidationConfig::default());

        let members: Vec<[u8; 32]> = vec![[1u8; 32], [2u8; 32]]; // Only 2 members
        validator.create_group(1, members).await.unwrap();

        let result = validator.validate_group(1).await.unwrap();
        assert!(matches!(result, ValidationResult::Invalid(_)));
    }

    #[tokio::test]
    async fn test_leader_validation() {
        let validator = GroupValidator::new(GroupValidationConfig::default());

        let members: Vec<[u8; 32]> = (0..5).map(|i| [i as u8; 32]).collect();
        validator.create_group(1, members.clone()).await.unwrap();
        validator.set_leader(1, members[0]).await.unwrap();

        let result = validator.validate_leader(1, &members[0]).await.unwrap();
        assert!(matches!(result, ValidationResult::Valid));

        let result = validator.validate_leader(1, &members[1]).await.unwrap();
        assert!(matches!(result, ValidationResult::Invalid(_)));
    }
}
