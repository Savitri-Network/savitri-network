// SPDX-License-Identifier: Apache-2.0
// © 2026 Savitri Network

//! Tests for the proposal system functionality.
//!
//! Requires the `contracts` feature (see `governance_tests.rs` for the same
//! gating rationale). Run with:
//!     `cargo test -p savitri-masternode --features contracts`
#![cfg(feature = "contracts")]

use savitri_contracts::governance::proposals::{
    Proposal, ProposalAction, ProposalStatus, ProposalSystem,
};
use savitri_storage::Storage;
use tempfile::TempDir;

#[cfg(test)]
mod proposal_tests {
    use super::*;

    #[test]
    fn test_proposal_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let system = ProposalSystem::new();

        let creator = [0u8; 32];
        let action = ProposalAction::NonCore {
            description: "Test proposal".to_string(),
        };

        let proposal = system.create_proposal(
            &storage,
            &creator,
            100, // deposit
            "Test proposal description".to_string(),
            action,
        );

        assert!(proposal.is_ok(), "Proposal creation should succeed");
        let proposal = proposal.unwrap();
        assert_eq!(proposal.id, 1);
        assert_eq!(proposal.creator, hex::encode(&creator));
        assert_eq!(proposal.deposit, 100);
        assert_eq!(proposal.description, "Test proposal description");
        assert_eq!(proposal.status, ProposalStatus::Pending);
    }

    #[test]
    fn test_proposal_state_transitions() {
        let mut proposal = Proposal::new_with_timestamp(
            1,
            "creator".to_string(),
            100,
            "Test proposal".to_string(),
            ProposalAction::NonCore {
                description: "Test".to_string(),
            },
            1000, // created_at
        );

        // Initial state should be Pending
        assert_eq!(proposal.status, ProposalStatus::Pending);
        assert!(!proposal.is_finalized());

        // Transition to ActiveVoting after review period (24h)
        let changed = proposal.transition_state_with_timestamp(86401);
        assert!(changed);
        assert_eq!(proposal.status, ProposalStatus::ActiveVoting);

        // Should be able to vote during ActiveVoting
        assert!(proposal.can_be_voted_with_timestamp(86401));

        // Transition to final state after voting period (7 days)
        let changed = proposal.transition_state_with_timestamp(691201);
        assert!(changed);
        assert!(proposal.is_finalized());
    }

    #[test]
    fn test_proposal_voting() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let system = ProposalSystem::new();

        let creator = [1u8; 32];
        let voter = [2u8; 32];
        let action = ProposalAction::NonCore {
            description: "Voting test proposal".to_string(),
        };

        // Create proposal
        let proposal = system
            .create_proposal(
                &storage,
                &creator,
                100,
                "Test voting proposal".to_string(),
                action,
            )
            .unwrap();

        // Transition to ActiveVoting
        let mut proposal = proposal;
        proposal.transition_state_with_timestamp(86401);

        // Test voting
        let result = system.vote_on_proposal(
            &storage,
            proposal.id,
            &voter,
            true, // vote yes
            50,   // voting power
        );

        assert!(result.is_ok(), "Voting should succeed");
    }

    #[test]
    fn test_proposal_actions() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let system = ProposalSystem::new();

        let creator = [3u8; 32];

        // Test FeeVariation action
        let fee_action = ProposalAction::FeeVariation {
            new_base_fee: Some(1000),
            new_max_fee: Some(2000),
        };

        let proposal = system.create_proposal(
            &storage,
            &creator,
            100,
            "Fee variation proposal".to_string(),
            fee_action,
        );

        assert!(proposal.is_ok());

        // Test ProjectSelection action
        let project_action = ProposalAction::ProjectSelection {
            project_address: vec![4u8; 32],
            amount: 5000,
        };

        let proposal = system.create_proposal(
            &storage,
            &creator,
            100,
            "Project selection proposal".to_string(),
            project_action,
        );

        assert!(proposal.is_ok());
    }

    #[test]
    fn test_proposal_validation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");
        let system = ProposalSystem::new();

        let creator = [5u8; 32];

        // Test proposal with empty description (should fail)
        let result = system.create_proposal(
            &storage,
            &creator,
            100,
            "".to_string(), // empty description
            ProposalAction::NonCore {
                description: "Test".to_string(),
            },
        );

        assert!(result.is_err(), "Empty description should fail validation");

        // Test proposal with zero deposit (should fail)
        let result = system.create_proposal(
            &storage,
            &creator,
            0, // zero deposit
            "Valid description".to_string(),
            ProposalAction::NonCore {
                description: "Test".to_string(),
            },
        );

        assert!(result.is_err(), "Zero deposit should fail validation");
    }

    #[test]
    fn test_proposal_serialization() {
        let proposal = Proposal::new_with_timestamp(
            1,
            "creator".to_string(),
            100,
            "Test proposal".to_string(),
            ProposalAction::NonCore {
                description: "Test".to_string(),
            },
            1000,
        );

        // Test that proposal can be serialized and deserialized
        let serialized = serde_json::to_string(&proposal);
        assert!(serialized.is_ok());

        let deserialized: Result<Proposal, _> = serde_json::from_str(&serialized.unwrap());
        assert!(deserialized.is_ok());

        let deserialized_proposal = deserialized.unwrap();
        assert_eq!(deserialized_proposal.id, proposal.id);
        assert_eq!(deserialized_proposal.creator, proposal.creator);
        assert_eq!(deserialized_proposal.description, proposal.description);
    }
}
