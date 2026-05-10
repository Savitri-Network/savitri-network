//! Esempio: Basic Setup
//!

use anyhow::Result;
use savitri_masternode::{load_config, MasternodeConfig};
use std::path::Path;

fn main() -> Result<()> {
    println!("=== Basic Masternode Setup Example ===\n");

    // 1. Carica configurazione
    println!("1. Loading configuration...");
    let config_path = "config/masternode.example.toml";

    if !Path::new(config_path).exists() {
        println!("   ⚠️  Config file not found: {}", config_path);
        println!("   Creating example config...");

        println!("   Please create config/masternode.toml from example");
        return Ok(());
    }

    let config = load_config(config_path)?;
    println!("   ✓ Configuration loaded");

    // 2. Check configurazione
    println!("\n2. Validating configuration...");
    config.validate()?;
    println!("   ✓ Configuration valid");

    // 3. Display configuration
    println!("\n3. Configuration details:");
    println!("   Role: {}", config.role);
    println!("   P2P Port: {}", config.p2p_port);
    println!("   Slot Duration: {:?}", config.slot_duration);
    println!("   Bootstrap Peers: {}", config.bootstrap_peers.len());
    println!("   Validators: {}", config.validators.len());

    // 4. Check chiavi
    println!("\n4. Checking keys...");
    if config.network_key_path.exists() {
        println!("   ✓ Network key found: {:?}", config.network_key_path);
    } else {
        println!(
            "   ⚠️  Network key not found: {:?}",
            config.network_key_path
        );
        println!("      (Will be generated automatically on first run)");
    }

    if config.masternode_key_path.exists() {
        println!(
            "   ✓ Masternode key found: {:?}",
            config.masternode_key_path
        );
    } else {
        println!(
            "   ⚠️  Masternode key not found: {:?}",
            config.masternode_key_path
        );
        println!("      (Please generate manually)");
    }

    println!("\n=== Setup Example Completed ===");
    println!("\nNext steps:");
    println!("1. Generate or copy your keys");
    println!("2. Update config/masternode.toml with your settings");
    println!("3. Add your Peer ID to validators list");
    println!("4. Run: cargo run --release");

    Ok(())
}
