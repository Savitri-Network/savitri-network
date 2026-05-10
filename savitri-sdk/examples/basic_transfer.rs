//! Example: Basic Transfer
//!
//! Shows how to build, sign, and submit a simple transfer transaction
//! using the Savitri JSON-RPC 2.0 interface.

use savitri_sdk::{TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Basic Transfer Example");

    // Create a wallet connected to the RPC node
    let wallet = Wallet::with_rpc("http://localhost:8545")?;
    println!("Wallet address: {}", wallet.address());

    // Check balance via savitri_getAccount
    match wallet.get_balance().await {
        Ok(balance) => println!("Balance: {}", balance),
        Err(e) => println!("Could not fetch balance: {}", e),
    }

    // Build and sign the transfer
    let to_address = "1".repeat(64);
    let tx = TransactionBuilder::new()
        .to(&to_address)
        .value(1000)
        .nonce(0)
        .fee(1_000_000_000_000_000_000) // 1 SAVT fee
        .build_and_sign(&wallet)?;

    println!(
        "Transaction created: from {} to {}",
        tx.transaction.from,
        tx.transaction.to.as_ref().unwrap()
    );

    // Submit via savitri_sendRawTransaction
    // let client = RpcClient::from_url("http://localhost:8545")?;
    // let result = client.send_raw_transaction(&hex::encode(&serialized_tx)).await?;
    // println!("Transaction hash: {}", result.tx_hash);

    Ok(())
}
