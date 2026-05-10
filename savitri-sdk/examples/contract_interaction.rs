//! Example: Contract Interaction
//!
//! Shows how to build a contract-call transaction using TransactionBuilder.

use savitri_sdk::{TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Contract Interaction");

    let wallet = Wallet::new();
    let contract_address = "2".repeat(64);

    // Build call data (function selector + arguments)
    let function_selector = b"transfer";
    let args: Vec<u8> = vec![];

    let mut call_data = function_selector.to_vec();
    call_data.extend_from_slice(&args);

    // Create the transaction
    let tx = TransactionBuilder::new()
        .from(wallet.address())
        .to(&contract_address)
        .value(0)
        .data(call_data)
        .build_and_sign(&wallet)?;

    println!("Contract call transaction created");
    println!("  To:   {}", tx.transaction.to.as_ref().unwrap());
    println!(
        "  Data: {} bytes",
        tx.transaction.data.as_ref().map(|d| d.len()).unwrap_or(0)
    );

    // To submit:
    // let client = RpcClient::from_url("http://localhost:8545")?;
    // let result = client.send_raw_transaction(&hex_encoded_tx).await?;

    Ok(())
}
