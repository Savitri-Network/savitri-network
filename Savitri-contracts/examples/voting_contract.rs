//! Esempio: Contratto di Voto
//!
//! il sistema di governance di Savitri.

use anyhow::Result;
use savitri_contracts::{
    governance::{Proposal, ProposalAction, ProposalStatus, Vote, VotingSystem},
    BaseContract, ContractStorage, Runtime,
};
use savitri_storage::Storage;

/// Contratto di Voto Semplice
///
/// - Creare proposte
/// - Votare sulle proposte
/// - Eseguire proposte approvate
pub struct SimpleVotingContract;

impl SimpleVotingContract {
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &[u8; 32],
    ) -> Result<()> {
        // Inizializza BaseContract
        BaseContract::initialize(contract_storage, storage, owner, None)?;

        println!("✓ Voting contract initialized");
        Ok(())
    }

    pub fn create_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8; 32],
        proposal_id: u64,
        action: ProposalAction,
        deposit_amount: u128,
    ) -> Result<Proposal> {
        if BaseContract::is_paused(contract_storage, storage, None)? {
            anyhow::bail!("Contract is paused");
        }

        // Creates proposta
        let proposal = Proposal::new(proposal_id, *caller, action, deposit_amount)?;

        println!(
            "✓ Proposal {} created by {:?}",
            proposal_id,
            hex::encode(caller)
        );

        Ok(proposal)
    }

    /// Vota su una proposta
    pub fn vote_on_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        voter: &[u8; 32],
        proposal_id: u64,
        vote: Vote,
        vote_token_amount: u128,
    ) -> Result<()> {
        let voting_system = VotingSystem::new();

        // Vota
        voting_system.vote(storage, proposal_id, *voter, vote, vote_token_amount)?;

        println!(
            "✓ Vote cast: {:?} voted {:?} on proposal {} with {} tokens",
            hex::encode(voter),
            vote,
            proposal_id,
            vote_token_amount
        );

        Ok(())
    }

    /// Compute risultati di una proposta
    pub fn calculate_results(storage: &Storage, proposal_id: u64) -> Result<(bool, bool)> {
        let voting_system = VotingSystem::new();

        // Carica proposta
        let proposal = storage.get_proposal(proposal_id)?;

        // Compute risultati
        let results = voting_system.calculate_results(storage, proposal_id)?;

        let quorum_reached = results.total_votes * 10 >= results.total_supply;
        let approval_reached =
            results.yes_votes * 100 >= (results.yes_votes + results.no_votes) * 65;

        println!(
            "✓ Proposal {} results: quorum={} approval={}",
            proposal_id,
            if quorum_reached {
                "reached"
            } else {
                "not reached"
            },
            if approval_reached {
                "reached"
            } else {
                "not reached"
            }
        );

        Ok((quorum_reached, approval_reached))
    }

    /// Runs una proposta approvata
    pub fn execute_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        proposal_id: u64,
    ) -> Result<()> {
        // Carica proposta
        let proposal = storage.get_proposal(proposal_id)?;

        // Check che sia approvata
        if proposal.status != ProposalStatus::Approved {
            anyhow::bail!("Proposal is not approved");
        }

        // Compute risultati
        let (quorum_reached, approval_reached) = Self::calculate_results(storage, proposal_id)?;

        if !quorum_reached || !approval_reached {
            anyhow::bail!("Proposal does not meet requirements");
        }

        // Esegui proposta
        let executor = ProposalExecutor::new();
        executor.execute(storage, proposal_id)?;

        println!("✓ Proposal {} executed", proposal_id);

        Ok(())
    }
}

fn main() -> Result<()> {
    println!("=== Simple Voting Contract Example ===\n");

    // Setup
    let tmp = tempfile::TempDir::new()?;
    let storage = Storage::new(tmp.path())?;
    let owner = [1u8; 32];
    let mut contract_storage = ContractStorage::new([2u8; 32])?;

    // Initialize contract
    SimpleVotingContract::initialize(&mut contract_storage, &storage, &owner)?;

    // Creates proposta
    let proposal_id = 1;
    let proposal_action = ProposalAction::TreasurySpend {
        recipient: [3u8; 32],
        amount: 1000,
    };
    let deposit = 5000; // 5% of the vote token supply

    let _proposal = SimpleVotingContract::create_proposal(
        &mut contract_storage,
        &storage,
        &owner,
        proposal_id,
        proposal_action,
        deposit,
    )?;

    // Vota
    let voter1 = [4u8; 32];
    let voter2 = [5u8; 32];

    SimpleVotingContract::vote_on_proposal(
        &mut contract_storage,
        &storage,
        &voter1,
        proposal_id,
        Vote::Yes,
        6000, // 6% of the supply
    )?;

    SimpleVotingContract::vote_on_proposal(
        &mut contract_storage,
        &storage,
        &voter2,
        proposal_id,
        Vote::Yes,
        5000, // 5% of the supply
    )?;

    // Compute risultati
    let (quorum_reached, approval_reached) =
        SimpleVotingContract::calculate_results(&storage, proposal_id)?;

    println!("\n=== Results ===");
    println!("Quorum reached: {}", quorum_reached);
    println!("Approval reached: {}", approval_reached);

    // Esegui se approvata
    if quorum_reached && approval_reached {
        SimpleVotingContract::execute_proposal(&mut contract_storage, &storage, proposal_id)?;
    }

    println!("\n✓ Example completed successfully");

    Ok(())
}
