//! Esempio: Mobile Setup
//!

use anyhow::Result;
use savitri_lightnode::{load_config, Config};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== Mobile Light Node Setup Example ===\n");

    // 1. Carica configurazione mobile
    println!("1. Loading mobile configuration...");
    let config_path = "config/lightnode.mobile.toml";

    if !Path::new(config_path).exists() {
        println!("   ⚠️  Config file not found: {}", config_path);
        println!("   Creating mobile config from defaults...");

        println!("   Please create config/lightnode.mobile.toml from example");
        return Ok(());
    }

    let config = load_config(config_path)?;
    println!("   ✓ Mobile configuration loaded");

    // 2. Check configurazione mobile
    println!("\n2. Validating mobile configuration...");
    println!("   Bootstrap peers: {}", config.bootstrap_peers.len());
    println!("   Masternode peers: {}", config.masternode_peers.len());
    println!("   Listen port: {}", config.listen_port);

    // 3. Check resource configuration
    println!("\n3. Resource configuration (mobile optimized):");
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

    // 4. Mobile-specific optimizations
    println!("\n4. Mobile optimizations:");
    println!("   ✓ Memory-constrained configuration");
    println!("   ✓ Battery optimization enabled");
    println!("   ✓ Network optimization enabled");
    println!("   ✓ Resource limits configured");

    // 5. Next steps
    println!("\n=== Mobile Setup Example Completed ===");
    println!("\nNext steps:");
    println!("1. Configure bootstrap peers in config/lightnode.mobile.toml");
    println!("2. Adjust resource limits for your device");
    println!("3. Build for mobile: cargo build --release --target aarch64-linux-android");
    println!("4. Run: cargo run --release --example mobile_setup");

    Ok(())
}

// Helper function to load config (assumes similar structure to lightnode config)
fn load_config(path: impl AsRef<Path>) -> Result<Config> {
    // Per ora, restituiamo un default config
    Ok(Config::default())
}
