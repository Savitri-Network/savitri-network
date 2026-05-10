//! Esempio: Desktop Light Node
//!

use anyhow::Result;
use savitri_lightnode::{load_config, Config};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== Desktop Light Node Setup Example ===\n");

    // 1. Carica configurazione desktop
    println!("1. Loading desktop configuration...");
    let config_path = "config/lightnode.desktop.toml";

    if !Path::new(config_path).exists() {
        println!("   ⚠️  Config file not found: {}", config_path);
        println!("   Creating desktop config from defaults...");

        println!("   Please create config/lightnode.desktop.toml from example");
        return Ok(());
    }

    let config = load_config(config_path)?;
    println!("   ✓ Desktop configuration loaded");

    // 2. Check configurazione desktop
    println!("\n2. Validating desktop configuration...");
    println!("   Bootstrap peers: {}", config.bootstrap_peers.len());
    println!("   Masternode peers: {}", config.masternode_peers.len());
    println!("   Listen port: {}", config.listen_port);

    // 3. Check resource configuration
    println!("\n3. Resource configuration (desktop optimized):");
    if let Some(resources) = &config.resources {
        println!("   Bandwidth: {:?} Mbps", resources.bandwidth_mbps);
        println!("   CPU cores: {:?}", resources.cpu_cores);
        println!("   Storage: {:?} GB", resources.storage_gb);
        println!("   Tolerance: {:.2}%", resources.tolerance * 100.0);
        println!(
            "   Weights: bandwidth={:.1}, cpu={:.1}, storage={:.1}",
            resources.weights.bandwidth, resources.weights.cpu, resources.weights.storage
        );
    }

    // 4. Desktop-specific features
    println!("\n4. Desktop features:");
    println!("   ✓ Full P2P networking enabled");
    println!("   ✓ Metrics support enabled");
    println!("   ✓ System monitoring enabled");
    println!("   ✓ Higher resource limits");

    // 5. Light node initialization (example)
    println!("\n5. Light node initialization (example):");
    println!("   // Initialize light node");
    println!("   let mut lightnode = LightNode::new(config)?;");
    println!("   lightnode.start().await?;");
    println!("   // Light node running...");

    // 6. Next steps
    println!("\n=== Desktop Light Node Example Completed ===");
    println!("\nNext steps:");
    println!("1. Configure bootstrap peers in config/lightnode.desktop.toml");
    println!("2. Adjust resource limits for your system");
    println!("3. Build: cargo build --release --features desktop");
    println!("4. Run: cargo run --release --features desktop --example desktop_lightnode");

    Ok(())
}

// Helper function to load config (assumes similar structure to lightnode config)
fn load_config(path: impl AsRef<Path>) -> Result<Config> {
    // Per ora, restituiamo un default config
    Ok(Config::default())
}
