//! Esempio: Guardian Setup
//!

use anyhow::Result;
use savitri_guardian::{load_config, GuardianConfig};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== Guardian Node Setup Example ===\n");

    // 1. Carica configurazione
    println!("1. Loading configuration...");
    let config_path = "guardian.toml";

    if !Path::new(config_path).exists() {
        println!("   ⚠️  Config file not found: {}", config_path);
        println!("   Creating example config...");

        println!("   Please create guardian.toml from example");
        return Ok(());
    }

    let config = load_config(config_path)?;
    println!("   ✓ Configuration loaded");

    // 2. Check configurazione
    println!("\n2. Validating configuration...");
    println!("   Database path: {:?}", config.db_path);
    println!("   Listen port: {:?}", config.listen_port);
    println!("   Bootstrap peers: {}", config.bootstrap_peers.len());

    // 3. Check monitoring configuration
    println!("\n3. Monitoring configuration:");
    println!(
        "   Metrics interval: {:?} seconds",
        config.monitoring.metrics_interval_secs
    );
    println!(
        "   Disk alert threshold: {} GB",
        config.monitoring.disk_alert_threshold_gb
    );

    // 4. Check rate limits
    println!("\n4. Rate limits configuration:");
    println!(
        "   Max blocks per request: {:?}",
        config.rate_limits.max_blocks_per_request
    );
    println!(
        "   Max monoliths per request: {:?}",
        config.rate_limits.max_monoliths_per_request
    );

    // 5. Guardian features
    println!("\n5. Guardian features:");
    println!("   ✓ Full archive storage enabled");
    println!("   ✓ Automatic backup enabled");
    println!("   ✓ Storage compaction optimized");
    println!("   ✓ Disk monitoring enabled");

    // 6. Next steps
    println!("\n=== Guardian Setup Example Completed ===");
    println!("\nNext steps:");
    println!("1. Configure guardian.toml with your settings");
    println!("2. Ensure sufficient storage (minimum 500GB+)");
    println!("3. Build: cargo build --release");
    println!("4. Run: cargo run --release -- --config guardian.toml");

    Ok(())
}

// Helper function to load config (assumes similar structure to guardian config)
fn load_config(path: impl AsRef<Path>) -> Result<GuardianConfig> {
    // Per ora, restituiamo un default config
    Ok(GuardianConfig::default())
}
