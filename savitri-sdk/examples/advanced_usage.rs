//! Example: Advanced Usage
//!
//! multiple RPC queries, and transaction building with custom data.

use savitri_sdk::{AddressUtils, RpcClient, TransactionBuilder, Wallet};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Advanced Usage");

    // 1. Wallet management
    println!("\n1. Wallet Management");
    let wallet = Wallet::new();
    println!("   Address: {}", wallet.address());

    // Save and reload from private key
    let private_key = wallet.private_key();
    let loaded = Wallet::from_private_key(&private_key)?;
    assert_eq!(wallet.address(), loaded.address());
    println!("   Wallet round-tripped via private key");

    println!("\n2. Address Validation");
    let addr = wallet.address();
    AddressUtils::validate(addr)?;
    println!("   Address valid: {}...", &addr[..16]);

    // 3. RPC operations (all via JSON-RPC 2.0)
    println!("\n3. JSON-RPC 2.0 Operations");
    let rpc = RpcClient::from_url("http://localhost:8545")?;

    if rpc.ping().await? {
        // savitri_health
        let health = rpc.health().await?;
        println!("   Mode: {}", health.mode);

        // savitri_blockNumber
        let block_number = rpc.get_block_number().await?;
        println!("   Block: {}", block_number);

        // savitri_getBlockByHeight (block 0 / genesis)
        match rpc.get_block_by_height(0).await {
            Ok(block) => println!("   Genesis hash: {}...", &block.hash[..16]),
            Err(e) => println!("   Genesis block: {}", e),
        }

        // savitri_getBlockHash
        match rpc.get_block_hash(0).await {
            Ok(hash) => println!("   Block 0 hash: {}", hash),
            Err(e) => println!("   Block 0 hash: {}", e),
        }

        // savitri_getAccount
        match rpc.get_account(wallet.address()).await {
            Ok(acc) => println!("   Account nonce: {}", acc.nonce),
            Err(e) => println!("   Account: {}", e),
        }

        // savitri_pouLocal
        match rpc.pou_local().await {
            Ok(pou) => println!(
                "   PoU: score={:?}, leader={}, epoch={:?}",
                pou.local_score, pou.local_is_leader, pou.epoch
            ),
            Err(e) => println!("   PoU: {}", e),
        }

        // Batch request
        println!("\n   Batch request:");
        let results = rpc
            .batch(vec![
                ("savitri_health", serde_json::json!([])),
                ("savitri_blockNumber", serde_json::json!([])),
            ])
            .await?;
        for (i, r) in results.iter().enumerate() {
            println!("     [{}] {}", i, r);
        }
    } else {
        println!("   Node not reachable (skipping RPC examples)");
    }

    // 4. Advanced transaction building
    println!("\n4. Advanced Transaction Building");
    let tx = TransactionBuilder::new()
        .from(wallet.address())
        .to("1".repeat(64))
        .value(1000)
        .nonce(1)
        .fee(100)
        .data(b"custom_data".to_vec())
        .build()?;

    println!("   Transaction built:");
    println!("     Value:    {}", tx.value);
    println!("     Nonce:    {}", tx.nonce);
    println!("     Has data: {}", tx.data.is_some());

    println!("\nAdvanced usage examples completed.");

    Ok(())
}
