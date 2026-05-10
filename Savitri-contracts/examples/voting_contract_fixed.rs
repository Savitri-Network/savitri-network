//! Esempio: Contratto di Voto
//!
//! il sistema di governance di Savitri.

use anyhow::Result;
use hex;
use savitri_contracts::{
    governance::{Proposal, ProposalAction, ProposalStatus, Vote, VotingSystem},
    BaseContract, ContractStorage,
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

        // Creates proposta (versione semplificata)
        let proposal = Proposal {
            id: proposal_id,
            proposer: *caller,
            action,
            status: ProposalStatus::Active,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            expires_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
                + 86400, // 24 hours
            execution_at: 0,
            votes_for: 0,
            votes_against: 0,
            total_weight: 0,
            deposit_amount,
            metadata: Default::default(),
        };

        println!(
            "✓ Proposal {} created by {}",
            proposal_id,
            hex::encode(caller)
        );

        Ok(proposal)
    }

    /// Vota su una proposta
    pub fn vote(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8; 32],
        proposal_id: u64,
        vote: Vote,
        weight: u64,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, None)? {
            anyhow::bail!("Contract is paused");
        }

        // - Che la proposta esista
        // - Che il votante sia autorizzato
        // - Che il voto sia nel periodo valido

        println!(
            "✓ Vote cast by {} on proposal {}: {:?} (weight: {})",
            hex::encode(caller),
            proposal_id,
            vote,
            weight
        );

        Ok(())
    }

    /// Runs una proposta approvata
    pub fn execute_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8; 32],
        proposal_id: u64,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, None)? {
            anyhow::bail!("Contract is paused");
        }

        // In un'implementazione reale, questo:
        // - Verificherebbe che la proposta sia approvata
        // - Eseguirebbe l'azione specificata
        // - Aggiornerebbe lo stato

        println!(
            "✓ Proposal {} executed by {}",
            proposal_id,
            hex::encode(caller)
        );

        Ok(())
    }

    /// Ottiene lo stato di una proposta
    pub fn get_proposal_status(
        contract_storage: &ContractStorage,
        storage: &Storage,
        proposal_id: u64,
    ) -> Result<ProposalStatus> {
        println!("✓ Getting status for proposal {}", proposal_id);

        // Mock status
        Ok(ProposalStatus::Active)
    }

    /// Compute i risultati of the voto
    pub fn calculate_vote_results(
        contract_storage: &ContractStorage,
        storage: &Storage,
        proposal_id: u64,
    ) -> Result<(u64, u64)> {
        println!("✓ Calculating results for proposal {}", proposal_id);

        // Mock results
        Ok((10, 5)) // 10 for, 5 against
    }
}

/// Esempio di utilizzo completo
pub fn run_voting_example() -> Result<()> {
    println!("🚀 Starting Voting Contract Example");

    // Creates storage temporaneo
    let (storage, _temp_dir) = create_test_storage("voting_example")?;
    let mut contract_storage = ContractStorage::new([100u8; 32].to_vec())?;

    // Initialize contract
    let owner = [1u8; 32];
    SimpleVotingContract::initialize(&mut contract_storage, &storage, &owner)?;

    // Creates proposta
    let proposal_id = 1;
    let action = ProposalAction::Transfer {
        to: [2u8; 32],
        amount: 1000,
    };

    let proposal = SimpleVotingContract::create_proposal(
        &mut contract_storage,
        &storage,
        &owner,
        proposal_id,
        action,
        100, // deposit
    )?;

    println!("📋 Proposal created: {:?}", proposal);

    // Vota
    let voter = [3u8; 32];
    SimpleVotingContract::vote(
        &mut contract_storage,
        &storage,
        &voter,
        proposal_id,
        Vote::For,
        100, // weight
    )?;

    // Compute risultati
    let (votes_for, votes_against) =
        SimpleVotingContract::calculate_vote_results(&contract_storage, &storage, proposal_id)?;

    println!(
        "📊 Vote results: {} for, {} against",
        votes_for, votes_against
    );

    // Esegui proposta se approvata
    if votes_for > votes_against {
        SimpleVotingContract::execute_proposal(
            &mut contract_storage,
            &storage,
            &owner,
            proposal_id,
        )?;
    }

    println!("✅ Voting Contract Example completed successfully!");

    Ok(())
}

fn create_test_storage(prefix: &str) -> Result<(Storage, std::path::PathBuf)> {
    use tempfile::TempDir;

    let tmp_dir = TempDir::new()?;
    let path = tmp_dir.path().join(prefix);
    std::fs::create_dir_all(&path)?;

    let storage = Storage::new(path.clone())?;

    // Keep temp dir alive
    let path_buf = path.to_path_buf();
    std::mem::forget(tmp_dir);

    Ok((storage, path_buf))
}

fn main() -> Result<()> {
    run_voting_example()
}
