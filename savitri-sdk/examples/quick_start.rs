//! Quick Start Example
//!
//! Demonstrates connecting to a Savitri node, querying chain state, and
//! building a transaction -- all via JSON-RPC 2.0.

use savitri_sdk::{RpcClient, TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Quick Start");

    // 1. Create a wallet
    println!("\n1. Creating wallet...");
    let wallet = Wallet::new();
    println!("   Wallet address: {}", wallet.address());

    // 2. Connect to the RPC node (JSON-RPC 2.0)
    println!("\n2. Connecting to RPC node...");
    let client = RpcClient::from_url("http://localhost:8545")?;

    let is_connected = client.ping().await?;
    println!("   RPC connected: {}", is_connected);

    if is_connected {
        // 3. Health check
        let health = client.health().await?;
        println!("   Node mode: {}", health.mode);

        // 4. Query block number (savitri_blockNumber)
        let block_number = client.get_block_number().await?;
        println!("   Current block: {}", block_number);

        // 5. Query account (savitri_getAccount)
        match client.get_account(wallet.address()).await {
            Ok(acc) => println!("   Balance: {}, Nonce: {}", acc.balance, acc.nonce),
            Err(e) => println!("   Account not found (expected for new wallet): {}", e),
        }
    }

    // 6. Build a transaction (offline)
    println!("\n3. Building transaction...");
    let tx = TransactionBuilder::new()
        .to("1".repeat(64))
        .value(1000)
        .nonce(0)
        .build_and_sign(&wallet)?;

    println!("   Transaction created:");
    println!("     From:  {}", tx.transaction.from);
    println!("     To:    {}", tx.transaction.to.as_ref().unwrap());
    println!("     Value: {}", tx.transaction.value);

    println!("\nQuick start completed.");

    Ok(())
}
