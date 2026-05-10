//! Tool: Network Monitor
//!
//! Continuously polls a Savitri node via JSON-RPC 2.0 and prints status.

use savitri_sdk::RpcClient;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Savitri Network Monitor\n");

    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:8545".to_string());

    println!("Connecting to: {}\n", url);

    let client = RpcClient::from_url(&url).map_err(|e| anyhow::anyhow!("{}", e))?;

    loop {
        let now = {
            let dur = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = dur.as_secs() % 86400;
            format!(
                "{:02}:{:02}:{:02}",
                secs / 3600,
                (secs % 3600) / 60,
                secs % 60
            )
        };

        match client.health().await {
            Ok(health) => {
                let block = client.get_block_number().await.unwrap_or(0);
                println!("[{}] mode={} block={}", now, health.mode, block);
            }
            Err(e) => {
                println!("[{}] Connection error: {}", now, e);
            }
        }

        sleep(Duration::from_secs(5)).await;
    }
}
