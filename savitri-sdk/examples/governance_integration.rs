//! Example: Governance Integration
//!
//! Shows how to create proposals, vote, and execute governance actions
//! using both the GovernanceClient and the TransactionBuilder.

use savitri_sdk::{ContractClient, GovernanceAction, TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Governance Integration");

    // Setup
    let wallet = Wallet::new();
    let contract_client =
        ContractClient::from_url_and_wallet("http://localhost:8545", wallet.clone())?;
    let governance_address = "3".repeat(64);
    let governance = contract_client.governance();

    println!("Wallet address: {}", wallet.address());

    // 1. Create a proposal
    println!("\n1. Creating FL proposal...");
    match governance
        .create_proposal(
            &governance_address,
            "Improve Network Throughput",
            "Optimise block production to increase TPS",
            7 * 24 * 60 * 60, // 7 days
        )
        .await
    {
        Ok(tx_hash) => println!("   Proposal tx: {}", tx_hash),
        Err(e) => println!("   Error: {}", e),
    }

    // 2. Vote
    println!("\n2. Voting on proposal...");
    match governance.vote(&governance_address, 1, true).await {
        Ok(tx_hash) => println!("   Vote tx: {}", tx_hash),
        Err(e) => println!("   Error: {}", e),
    }

    // 3. Execute
    println!("\n3. Executing proposal...");
    match governance.execute(&governance_address, 1).await {
        Ok(tx_hash) => println!("   Execute tx: {}", tx_hash),
        Err(e) => println!("   Error: {}", e),
    }

    // 4. Using TransactionBuilder directly
    println!("\n4. TransactionBuilder governance calls...");

    let proposal_tx = TransactionBuilder::new()
        .from(wallet.address())
        .create_fl_proposal(
            &governance_address,
            "New Feature",
            "Add support for new transaction types",
            14 * 24 * 60 * 60,
        )
        .build_and_sign(&wallet)?;
    println!(
        "   Proposal tx data: {} bytes",
        proposal_tx
            .transaction
            .data
            .as_ref()
            .map(|d| d.len())
            .unwrap_or(0)
    );

    let vote_tx = TransactionBuilder::new()
        .from(wallet.address())
        .governance_call(&governance_address, 1, GovernanceAction::Vote(true))
        .build_and_sign(&wallet)?;
    println!(
        "   Vote tx data: {} bytes",
        vote_tx
            .transaction
            .data
            .as_ref()
            .map(|d| d.len())
            .unwrap_or(0)
    );

    let execute_tx = TransactionBuilder::new()
        .from(wallet.address())
        .governance_call(&governance_address, 1, GovernanceAction::Execute)
        .build_and_sign(&wallet)?;
    println!(
        "   Execute tx data: {} bytes",
        execute_tx
            .transaction
            .data
            .as_ref()
            .map(|d| d.len())
            .unwrap_or(0)
    );

    Ok(())
}
