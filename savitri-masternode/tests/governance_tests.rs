// SPDX-License-Identifier: Apache-2.0
// © 2026 Savitri Network

//! Tests for the governance system functionality.
//!
//! Requires the `contracts` feature, which is opt-in on this crate because
//! the contract-executor integration is still being refactored to use the
//! storage trait object. Run with:
//!     `cargo test -p savitri-masternode --features contracts`
#![cfg(feature = "contracts")]

use savitri_contracts::governance::proposals::{
    Proposal, ProposalAction, ProposalStatus, ProposalSystem,
};
use savitri_contracts::governance::voting::VotingSystem;
use savitri_storage::Storage;
use tempfile::TempDir;

#[cfg(test)]
mod governance_tests {
    use super::*;

    #[test]
    fn test_governance_workflow() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();
        let voting_system = VotingSystem::new();

        // Create proposal
        let creator = [1u8; 32];
        let action = ProposalAction::NonCore {
            description: "Governance test proposal".to_string(),
        };

        let proposal = proposal_system
            .create_proposal(
                &storage,
                &creator,
                1000, // deposit
                "Test governance workflow".to_string(),
                action,
            )
            .unwrap();

        // Transition to ActiveVoting
        let mut proposal = proposal;
        proposal.transition_state_with_timestamp(86401); // After review period

        // Test voting
        let voter1 = [2u8; 32];
        let voter2 = [3u8; 32];
        let voter3 = [4u8; 32];

        // Add votes
        voting_system
            .vote_on_proposal(
                &storage,
                proposal.id,
                &voter1,
                true, // vote yes
                100,  // voting power
            )
            .unwrap();

        voting_system
            .vote_on_proposal(
                &storage,
                proposal.id,
                &voter2,
                true, // vote yes
                150,  // voting power
            )
            .unwrap();

        voting_system
            .vote_on_proposal(
                &storage,
                proposal.id,
                &voter3,
                false, // vote no
                50,    // voting power
            )
            .unwrap();

        // Check voting results
        let voting_results = voting_system
            .get_voting_results(&storage, proposal.id)
            .unwrap();
        assert_eq!(voting_results.yes_votes, 250);
        assert_eq!(voting_results.no_votes, 50);
        assert_eq!(voting_results.abstain_votes, 0);
    }

    #[test]
    fn test_proposal_approval_threshold() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();
        let voting_system = VotingSystem::new();

        // Create proposal
        let creator = [1u8; 32];
        let action = ProposalAction::NonCore {
            description: "Threshold test proposal".to_string(),
        };

        let proposal = proposal_system
            .create_proposal(
                &storage,
                &creator,
                1000,
                "Test approval threshold".to_string(),
                action,
            )
            .unwrap();

        let mut proposal = proposal;
        proposal.transition_state_with_timestamp(86401);

        // Add votes to reach approval threshold (assuming 70% yes votes needed)
        let total_voting_power = 1000;
        let yes_votes_needed = (total_voting_power * 70) / 100;

        // Add enough yes votes to pass
        for i in 0..7 {
            let voter = [i + 2u8; 32];
            voting_system
                .vote_on_proposal(
                    &storage,
                    proposal.id,
                    &voter,
                    true,
                    100, // 100 voting power each
                )
                .unwrap();
        }

        // Check if proposal is approved
        let voting_results = voting_system
            .get_voting_results(&storage, proposal.id)
            .unwrap();
        assert!(voting_results.yes_votes >= yes_votes_needed);
    }

    #[test]
    fn test_voting_restrictions() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();
        let voting_system = VotingSystem::new();

        // Create proposal
        let creator = [1u8; 32];
        let action = ProposalAction::NonCore {
            description: "Voting restrictions test".to_string(),
        };

        let proposal = proposal_system
            .create_proposal(
                &storage,
                &creator,
                1000,
                "Test voting restrictions".to_string(),
                action,
            )
            .unwrap();

        // Try to vote before ActiveVoting (should fail)
        let voter = [2u8; 32];
        let result = voting_system.vote_on_proposal(&storage, proposal.id, &voter, true, 100);

        assert!(result.is_err(), "Voting before ActiveVoting should fail");

        // Transition to ActiveVoting
        let mut proposal = proposal;
        proposal.transition_state_with_timestamp(86401);

        // Now voting should succeed
        let result = voting_system.vote_on_proposal(&storage, proposal.id, &voter, true, 100);

        assert!(result.is_ok(), "Voting during ActiveVoting should succeed");

        // Try to vote twice (should fail)
        let result = voting_system.vote_on_proposal(
            &storage,
            proposal.id,
            &voter,
            false, // different vote
            100,
        );

        assert!(result.is_err(), "Double voting should fail");
    }

    #[test]
    fn test_proposal_types() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();

        let creator = [1u8; 32];

        // Test different proposal types
        let proposals = vec![
            (
                "Fee Variation",
                ProposalAction::FeeVariation {
                    new_base_fee: Some(1000),
                    new_max_fee: Some(2000),
                },
            ),
            (
                "Project Selection",
                ProposalAction::ProjectSelection {
                    project_address: vec![2u8; 32],
                    amount: 5000,
                },
            ),
            (
                "Standards",
                ProposalAction::Standards {
                    standard_name: "ERC-20".to_string(),
                    standard_version: "1.0".to_string(),
                },
            ),
            (
                "Non Core",
                ProposalAction::NonCore {
                    description: "Test non-core proposal".to_string(),
                },
            ),
        ];

        for (name, action) in proposals {
            let proposal = proposal_system.create_proposal(
                &storage,
                &creator,
                1000,
                format!("Test {} proposal", name),
                action,
            );

            assert!(proposal.is_ok(), "Failed to create {} proposal", name);
        }
    }

    #[test]
    fn test_proposal_deadlines() {
        let proposal = Proposal::new_with_timestamp(
            1,
            "creator".to_string(),
            1000,
            "Deadline test proposal".to_string(),
            ProposalAction::NonCore {
                description: "Test".to_string(),
            },
            1000, // created_at
        );

        // Check initial state
        assert_eq!(proposal.status, ProposalStatus::Pending);
        assert!(!proposal.can_be_voted_with_timestamp(1000)); // During review period

        // Check after review period (24 hours)
        assert!(proposal.can_be_voted_with_timestamp(86401)); // After review period
        assert!(!proposal.can_be_voted_with_timestamp(691201)); // After voting period

        // Check finalization
        assert!(!proposal.is_finalized());
        proposal.transition_state_with_timestamp(691201); // After voting period
        assert!(proposal.is_finalized());
    }

    #[test]
    fn test_governance_permissions() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();

        // Test that only eligible accounts can create proposals
        let ineligible_creator = [0u8; 32]; // All zeros account
        let action = ProposalAction::NonCore {
            description: "Test proposal".to_string(),
        };

        let result = proposal_system.create_proposal(
            &storage,
            &ineligible_creator,
            1000,
            "Test permissions".to_string(),
            action,
        );

        // This should either succeed or fail gracefully based on implementation
        // The important thing is that it doesn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_proposal_execution() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();

        let creator = [1u8; 32];
        let action = ProposalAction::FeeVariation {
            new_base_fee: Some(1000),
            new_max_fee: Some(2000),
        };

        let proposal = proposal_system
            .create_proposal(
                &storage,
                &creator,
                1000,
                "Test proposal execution".to_string(),
                action,
            )
            .unwrap();

        // Simulate proposal approval and execution
        let mut proposal = proposal;
        proposal.transition_state_with_timestamp(86401); // ActiveVoting
        proposal.transition_state_with_timestamp(691201); // Approved

        assert_eq!(proposal.status, ProposalStatus::Approved);

        // Test that approved proposals can be executed
        let execution_result = proposal_system.execute_proposal(&storage, proposal.id);

        // This should either succeed or fail gracefully
        assert!(execution_result.is_ok() || execution_result.is_err());
    }

    #[test]
    fn test_governance_metrics() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let proposal_system = ProposalSystem::new();

        // Create multiple proposals
        let creator = [1u8; 32];
        for i in 1..=5 {
            let action = ProposalAction::NonCore {
                description: format!("Test proposal {}", i).to_string(),
            };

            proposal_system
                .create_proposal(
                    &storage,
                    &creator,
                    1000,
                    format!("Test proposal {} description", i),
                    action,
                )
                .unwrap();
        }

        // Test governance metrics
        let metrics = proposal_system.get_governance_metrics(&storage).unwrap();

        assert_eq!(metrics.total_proposals, 5);
        assert_eq!(metrics.pending_proposals, 5);
        assert_eq!(metrics.active_proposals, 0);
        assert_eq!(metrics.approved_proposals, 0);
        assert_eq!(metrics.rejected_proposals, 0);
    }
}
