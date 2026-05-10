//! Example: Oracle Integration
//!
//! Shows how to interact with the Oracle system using both the OracleClient
//! and the TransactionBuilder.

use savitri_sdk::{ContractClient, TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Oracle Integration");

    let wallet = Wallet::new();
    let contract_client =
        ContractClient::from_url_and_wallet("http://localhost:8545", wallet.clone())?;
    let oracle_address = "4".repeat(64);
    let oracle = contract_client.oracle();

    println!("Wallet address: {}", wallet.address());

    // 1. Request data
    println!("\n1. Requesting oracle data...");
    match oracle
        .request_data(&oracle_address, "price", b"BTC/USD")
        .await
    {
        Ok(tx_hash) => println!("   Request tx: {}", tx_hash),
        Err(e) => println!("   Error: {}", e),
    }

    // 2. Submit response (oracle provider)
    println!("\n2. Submitting oracle response...");
    match oracle
        .submit_response(&oracle_address, 12345, b"50000.00")
        .await
    {
        Ok(tx_hash) => println!("   Response tx: {}", tx_hash),
        Err(e) => println!("   Error: {}", e),
    }

    // 3. Verify data
    println!("\n3. Verifying oracle data...");
    match oracle
        .verify_data(&oracle_address, b"verified_payload")
        .await
    {
        Ok(valid) => println!("   Data valid: {}", valid),
        Err(e) => println!("   Error: {}", e),
    }

    // 4. TransactionBuilder oracle call
    println!("\n4. TransactionBuilder oracle call...");
    let oracle_tx = TransactionBuilder::new()
        .from(wallet.address())
        .oracle_call(&oracle_address, "request_data", b"ETH/USD")
        .build_and_sign(&wallet)?;

    println!(
        "   Oracle tx data: {} bytes",
        oracle_tx
            .transaction
            .data
            .as_ref()
            .map(|d| d.len())
            .unwrap_or(0)
    );

    Ok(())
}
