//! Example: Light Node Setup
//!
//! Shows how to use the LightClient for basic node interaction.

use savitri_sdk::LightClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    println!("Savitri SDK - Light Node Setup");

    let client = LightClient::new("http://localhost:8545")?;

    let connected = client.is_connected().await;
    println!("Connected: {}", connected);

    if connected {
        // savitri_health
        let health = client.health().await?;
        println!("Node mode: {}", health.mode);

        // savitri_blockNumber
        let block = client.get_block_number().await?;
        println!("Current block: {}", block);

        // savitri_getAccount
        let address = "0".repeat(64);
        match client.get_balance(&address).await {
            Ok(balance) => println!("Balance for {}...: {}", &address[..8], balance),
            Err(e) => println!("Account query: {}", e),
        }

        // savitri_pouLocal
        match client.pou_local().await {
            Ok(pou) => println!("PoU local score: {:?}", pou.local_score),
            Err(e) => println!("PoU: {}", e),
        }
    }

    Ok(())
}
