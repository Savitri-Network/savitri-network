//! Governance Storage Module
//!
//! Complete storage implementation for governance system including:
//! - Proposal lifecycle management
//! - Voting and tallying
//! - Governance parameter storage
//! - Vote token management
//! - Quorum and approval calculations

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Column family for governance data
pub const CF_GOVERNANCE: &str = "governance";

/// Stato di una proposta
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposalStatus {
    Pending,
    ActiveVoting,
    Approved,
    Rejected,
    Executed,
}

/// Tipo di voto
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VoteType {
    Yes,
    No,
    Abstain,
}

/// Struttura proposta governance
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proposal {
    pub id: u64,
    pub creator: Vec<u8>,
    pub deposit: u128,
    pub description: String,
    pub action: ProposalAction,
    pub status: ProposalStatus,
    pub created_at: u64,
    pub review_end: u64,
    pub voting_end: u64,
    pub yes_votes: u128,
    pub no_votes: u128,
    pub abstain_votes: u128,
}

/// Azione associata a una proposta
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposalAction {
    FeeVariation {
        new_base_fee: Option<u128>,
        new_max_fee: Option<u128>,
    },
    ProjectSelection {
        project_address: Vec<u8>,
        amount: u128,
    },
    Standards {
        standard_name: String,
        standard_version: String,
    },
    ContractUpgrade {
        contract_address: Vec<u8>,
        new_code_hash: Vec<u8>,
        description: String,
    },
    NonCore {
        description: String,
    },
    SetFlPolicy {
        fee_treasury_bps: u16,
        max_models: u32,
        whitelist_aggregators: Vec<Vec<u8>>,
    },
    ApproveFlModel {
        model_id: Vec<u8>,
    },
    AbortFlRound {
        model_id: Vec<u8>,
        round_id: u64,
    },
    SetBondParams {
        min_bond: Option<u128>,
        max_bond: Option<u128>,
    },
    SetSlashParams {
        equivocation_pct: Option<u16>,
        double_vote_pct: Option<u16>,
        invalid_attestation_pct: Option<u16>,
    },
    AddConnector {
        connector_id: String,
        pubkey: Vec<u8>,
        config: Vec<u8>,
    },
    RemoveConnector {
        connector_id: String,
    },
    SlashingParamsUpdate {
        new_min_bond_amount: Option<u128>,
        new_slash_pct_equivocation: Option<u16>,
        new_slash_pct_double_vote: Option<u16>,
        new_slash_pct_invalid_attestation: Option<u16>,
    },
}

/// Voto su una proposta
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Vote {
    pub proposal_id: u64,
    pub voter: Vec<u8>,
    pub vote_type: VoteType,
    pub vote_amount: u128,
    pub timestamp: u64,
}

/// Governance parameters
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GovernanceParams {
    pub quorum_threshold_bps: u16, // Basis points for quorum (e.g., 1000 = 10%)
    pub approval_threshold_bps: u16, // Basis points for approval (e.g., 5000 = 50%)
    pub voting_period_blocks: u64, // Voting period in blocks
    pub review_period_blocks: u64, // Review period before voting
    pub min_proposal_deposit: u128, // Minimum deposit to create proposal
    pub max_proposals_per_block: u32, // Max proposals that can be executed per block
}

impl Default for GovernanceParams {
    fn default() -> Self {
        Self {
            quorum_threshold_bps: 1000,    // 10%
            approval_threshold_bps: 5000,  // 50%
            voting_period_blocks: 10080,   // ~1 week assuming 6s blocks
            review_period_blocks: 1440,    // ~1 day
            min_proposal_deposit: 1000000, // 1 SAV tokens
            max_proposals_per_block: 5,
        }
    }
}

/// Governance storage interface
pub struct GovernanceStorage {
    // This would typically be a reference to the main storage
    // For now, we'll implement standalone functions that work with storage
}

impl GovernanceStorage {
    /// Create a new proposal
    pub fn create_proposal(
        storage: &crate::Storage,
        creator: Vec<u8>,
        deposit: u128,
        description: String,
        action: ProposalAction,
        current_block: u64,
        params: &GovernanceParams,
    ) -> Result<u64> {
        // Validate deposit
        if deposit < params.min_proposal_deposit {
            return Err(anyhow!("Deposit below minimum required"));
        }

        // Get next proposal ID
        let proposal_id = storage.next_proposal_id()?;

        // Calculate timestamps
        let created_at = current_block;
        let review_end = created_at + params.review_period_blocks;
        let voting_end = review_end + params.voting_period_blocks;

        // Create proposal
        let proposal = Proposal {
            id: proposal_id,
            creator,
            deposit,
            description,
            action,
            status: ProposalStatus::Pending,
            created_at,
            review_end,
            voting_end,
            yes_votes: 0,
            no_votes: 0,
            abstain_votes: 0,
        };

        // Serialize and store proposal
        let proposal_data = bincode::serialize(&proposal)?;
        storage.put_proposal(proposal_id, &proposal_data)?;

        // Update next proposal ID
        storage.put(b"proposals:next_id", &(proposal_id + 1).to_le_bytes())?;

        Ok(proposal_id)
    }

    /// Get proposal by ID
    pub fn get_proposal(storage: &crate::Storage, proposal_id: u64) -> Result<Option<Proposal>> {
        match storage.get_proposal(proposal_id)? {
            Some(data) => {
                let proposal = crate::safe_deserialize(&data)?;
                Ok(Some(proposal))
            }
            None => Ok(None),
        }
    }

    /// Update proposal status
    pub fn update_proposal_status(
        storage: &crate::Storage,
        proposal_id: u64,
        new_status: ProposalStatus,
    ) -> Result<()> {
        let mut proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Err(anyhow!("Proposal not found")),
        };

        proposal.status = new_status;
        let proposal_data = bincode::serialize(&proposal)?;
        storage.put_proposal(proposal_id, &proposal_data)?;

        Ok(())
    }

    /// Cast a vote on a proposal
    pub fn cast_vote(
        storage: &crate::Storage,
        voter: Vec<u8>,
        proposal_id: u64,
        vote_type: VoteType,
        vote_amount: u128,
        current_block: u64,
    ) -> Result<()> {
        // Check if proposal exists and is in voting period
        let mut proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Err(anyhow!("Proposal not found")),
        };

        // Check if proposal is in voting period
        if current_block < proposal.review_end {
            return Err(anyhow!("Voting period not started"));
        }
        if current_block > proposal.voting_end {
            return Err(anyhow!("Voting period ended"));
        }

        // Check if voter has already voted
        if storage.get_vote(&voter, proposal_id)?.is_some() {
            return Err(anyhow!("Already voted"));
        }

        // Check if voter has sufficient vote tokens
        let available_tokens = storage.get_available_vote_tokens(&voter)?;
        if available_tokens < vote_amount {
            return Err(anyhow!("Insufficient vote tokens"));
        }

        // Create vote
        let vote = Vote {
            proposal_id,
            voter: voter.clone(),
            vote_type,
            vote_amount,
            timestamp: current_block,
        };

        // Store vote
        let vote_data = bincode::serialize(&vote)?;
        storage.put_vote(&voter, proposal_id, &vote_data)?;

        // Update proposal vote counts
        match vote_type {
            VoteType::Yes => proposal.yes_votes += vote_amount,
            VoteType::No => proposal.no_votes += vote_amount,
            VoteType::Abstain => proposal.abstain_votes += vote_amount,
        }

        // Update proposal
        let proposal_data = bincode::serialize(&proposal)?;
        storage.put_proposal(proposal_id, &proposal_data)?;

        // Lock vote tokens
        let locked_tokens = storage.get_locked_vote_tokens(&voter)?;
        let new_locked = locked_tokens.saturating_add(vote_amount);
        let key = format!("locked_tokens:{}", hex::encode(&voter));
        storage.put(key.as_bytes(), &new_locked.to_le_bytes())?;

        Ok(())
    }

    /// Get all votes for a proposal
    pub fn get_proposal_votes(storage: &crate::Storage, proposal_id: u64) -> Result<Vec<Vote>> {
        let vote_data_list = storage.get_proposal_votes(proposal_id)?;
        let mut votes = Vec::new();

        for vote_data in &vote_data_list {
            let vote: Vote = crate::safe_deserialize(vote_data)?;
            votes.push(vote);
        }

        Ok(votes)
    }

    /// Get all proposals
    pub fn get_all_proposals(storage: &crate::Storage) -> Result<Vec<Proposal>> {
        let proposal_ids = storage.get_all_proposals()?;
        let mut proposals = Vec::new();

        for proposal_id in proposal_ids {
            if let Some(proposal) = Self::get_proposal(storage, proposal_id)? {
                proposals.push(proposal);
            }
        }

        Ok(proposals)
    }

    /// Check if proposal has reached quorum
    pub fn check_quorum(
        storage: &crate::Storage,
        proposal_id: u64,
        params: &GovernanceParams,
    ) -> Result<bool> {
        let proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Ok(false),
        };

        let total_vote_tokens = storage.get_total_vote_tokens()?;
        if total_vote_tokens == 0 {
            return Ok(false);
        }

        let total_votes = proposal
            .yes_votes
            .saturating_add(proposal.no_votes)
            .saturating_add(proposal.abstain_votes);

        let quorum_threshold = total_vote_tokens
            .saturating_mul(params.quorum_threshold_bps as u128)
            .saturating_div(10000);

        Ok(total_votes >= quorum_threshold)
    }

    /// Check if proposal is approved
    pub fn check_approval(
        storage: &crate::Storage,
        proposal_id: u64,
        params: &GovernanceParams,
    ) -> Result<bool> {
        // First check quorum
        if !Self::check_quorum(storage, proposal_id, params)? {
            return Ok(false);
        }

        let proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Ok(false),
        };

        let total_votes = proposal
            .yes_votes
            .saturating_add(proposal.no_votes)
            .saturating_add(proposal.abstain_votes);

        if total_votes == 0 {
            return Ok(false);
        }

        // Approval threshold: yes votes must be >= threshold percentage of total votes
        let approval_threshold = total_votes
            .saturating_mul(params.approval_threshold_bps as u128)
            .saturating_div(10000);

        Ok(proposal.yes_votes >= approval_threshold)
    }

    /// Execute a proposal (if approved)
    pub fn execute_proposal(
        storage: &crate::Storage,
        proposal_id: u64,
        _params: &GovernanceParams,
    ) -> Result<()> {
        let proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Err(anyhow!("Proposal not found")),
        };

        if proposal.status != ProposalStatus::Approved {
            return Err(anyhow!("Proposal not approved"));
        }

        // Execute the proposal action
        match &proposal.action {
            ProposalAction::FeeVariation {
                new_base_fee,
                new_max_fee,
            } => {
                if let Some(base_fee) = new_base_fee {
                    storage.set_fee_base(*base_fee as u64)?;
                }
                if let Some(max_fee) = new_max_fee {
                    storage.set_fee_max(*max_fee as u64)?;
                }
            }
            ProposalAction::Standards {
                standard_name,
                standard_version,
            } => {
                let standard_id = format!("{}:{}", standard_name, standard_version);
                storage.put_approved_standard(standard_id.as_bytes(), &proposal.creator)?;
            }
            ProposalAction::ContractUpgrade {
                contract_address,
                new_code_hash,
                ..
            } => {
                storage.update_contract_code(contract_address, new_code_hash)?;
            }
            ProposalAction::SetFlPolicy { .. } => {
                // FL policy data would be serialized and stored
                let policy_data = bincode::serialize(&proposal.action)?;
                storage.set_fl_policy(&policy_data)?;
            }
            ProposalAction::ApproveFlModel { model_id } => {
                storage.approve_fl_model(model_id)?;
            }
            ProposalAction::AbortFlRound {
                model_id: _,
                round_id,
            } => {
                storage.abort_fl_round(*round_id)?;
                // Additional model-specific cleanup could be added here
            }
            ProposalAction::AddConnector {
                connector_id,
                pubkey,
                config,
            } => {
                let connector_data = format!(
                    "{}:{}:{}",
                    connector_id,
                    hex::encode(pubkey),
                    hex::encode(config)
                );
                storage.put_connector_info(connector_id.as_bytes(), connector_data.as_bytes())?;
            }
            ProposalAction::RemoveConnector { connector_id } => {
                storage.delete_connector_info(connector_id.as_bytes())?;
            }
            // Other actions would be implemented similarly
            _ => {
                return Err(anyhow!("Proposal action not implemented"));
            }
        }

        // Update proposal status to executed
        Self::update_proposal_status(storage, proposal_id, ProposalStatus::Executed)?;

        Ok(())
    }

    /// Get governance parameters
    pub fn get_governance_params(storage: &crate::Storage) -> Result<GovernanceParams> {
        match storage.get(b"governance:params")? {
            Some(data) => {
                let params = crate::safe_deserialize(&data)?;
                Ok(params)
            }
            None => Ok(GovernanceParams::default()),
        }
    }

    /// Set governance parameters
    pub fn set_governance_params(
        storage: &crate::Storage,
        params: &GovernanceParams,
    ) -> Result<()> {
        let params_data = bincode::serialize(params)?;
        storage.put(b"governance:params", &params_data)?;
        Ok(())
    }

    /// Process proposal status updates (should be called each block)
    pub fn process_proposals(storage: &crate::Storage, current_block: u64) -> Result<Vec<u64>> {
        let mut executed_proposals = Vec::new();
        let params = Self::get_governance_params(storage)?;
        let proposals = Self::get_all_proposals(storage)?;

        for proposal in proposals {
            match proposal.status {
                ProposalStatus::Pending => {
                    if current_block >= proposal.review_end {
                        Self::update_proposal_status(
                            storage,
                            proposal.id,
                            ProposalStatus::ActiveVoting,
                        )?;
                    }
                }
                ProposalStatus::ActiveVoting => {
                    if current_block > proposal.voting_end {
                        // Voting period ended, check results
                        if Self::check_approval(storage, proposal.id, &params)? {
                            Self::update_proposal_status(
                                storage,
                                proposal.id,
                                ProposalStatus::Approved,
                            )?;
                        } else {
                            Self::update_proposal_status(
                                storage,
                                proposal.id,
                                ProposalStatus::Rejected,
                            )?;
                        }
                    }
                }
                ProposalStatus::Approved => {
                    // Execute approved proposals (respecting max per block limit)
                    if executed_proposals.len() < params.max_proposals_per_block as usize {
                        if let Ok(()) = Self::execute_proposal(storage, proposal.id, &params) {
                            executed_proposals.push(proposal.id);
                        }
                    }
                }
                _ => {} // No action needed for other statuses
            }
        }

        Ok(executed_proposals)
    }

    /// Get voting power for an address
    pub fn get_voting_power(storage: &crate::Storage, address: &[u8]) -> Result<u128> {
        storage.get_available_vote_tokens(address)
    }

    /// Get proposal statistics
    pub fn get_proposal_stats(
        storage: &crate::Storage,
        proposal_id: u64,
    ) -> Result<Option<ProposalStats>> {
        let proposal = match Self::get_proposal(storage, proposal_id)? {
            Some(p) => p,
            None => return Ok(None),
        };

        let votes = Self::get_proposal_votes(storage, proposal_id)?;
        let total_votes = proposal
            .yes_votes
            .saturating_add(proposal.no_votes)
            .saturating_add(proposal.abstain_votes);

        let stats = ProposalStats {
            proposal_id,
            status: proposal.status,
            total_votes,
            yes_votes: proposal.yes_votes,
            no_votes: proposal.no_votes,
            abstain_votes: proposal.abstain_votes,
            unique_voters: votes.len() as u64,
            created_at: proposal.created_at,
            voting_end: proposal.voting_end,
        };

        Ok(Some(stats))
    }
}

/// Proposal statistics for reporting
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProposalStats {
    pub proposal_id: u64,
    pub status: ProposalStatus,
    pub total_votes: u128,
    pub yes_votes: u128,
    pub no_votes: u128,
    pub abstain_votes: u128,
    pub unique_voters: u64,
    pub created_at: u64,
    pub voting_end: u64,
}
