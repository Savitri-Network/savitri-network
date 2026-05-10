//! Esempio: Validator Configuration
//!

use anyhow::Result;
use libp2p::{identity::Keypair, PeerId};
use savitri_masternode::{load_config, MasternodeConfig};

fn main() -> Result<()> {
    println!("=== Validator Configuration Example ===\n");

    // 1. Load or generate network identity
    println!("1. Network identity...");
    let network_key_path = "identity.key";

    let identity = if std::path::Path::new(network_key_path).exists() {
        let bytes = std::fs::read(network_key_path)?;
        Keypair::from_protobuf_encoding(&bytes)?
    } else {
        println!("   Generating new network identity...");
        let kp = Keypair::generate_ed25519();
        let encoded = kp.to_protobuf_encoding()?;
        std::fs::write(network_key_path, encoded)?;
        kp
    };

    let peer_id = PeerId::from(identity.public());
    println!("   ✓ Peer ID: {}", peer_id);

    // 2. Load configuration
    println!("\n2. Loading configuration...");
    let config = load_config("config/masternode.toml")?;
    println!("   ✓ Configuration loaded");

    println!("\n3. Validator setup verification...");
    let peer_id_str = peer_id.to_string();

    if config.validators.contains(&peer_id_str) {
        println!("   ✓ Peer ID found in validators list");
        println!("   ✓ Node is configured as validator");
    } else {
        println!("   ⚠️  Peer ID NOT in validators list");
        println!("      Peer ID: {}", peer_id_str);
        println!("      Validators in config: {:?}", config.validators);
        println!("\n   To add as validator:");
        println!("   1. Add '{}' to validators list in config", peer_id_str);
        println!("   2. Ensure all validators have same list (ordered)");
        println!("   3. Restart masternode");
    }

    println!("\n4. Validator configuration:");
    println!("   Validators count: {}", config.validators.len());
    println!("   Slot duration: {:?}", config.slot_duration);
    println!("   Slot base (ms): {}", config.slot_base_ms);

    // Calculate leader rotation
    if let Some(position) = config.validators.iter().position(|v| v == &peer_id_str) {
        println!("\n   Leader rotation:");
        println!("   Your position: {} (0-indexed)", position);
        println!("   Total validators: {}", config.validators.len());
        println!("   Rotation cycle: {} slots", config.validators.len());
    }

    println!("\n=== Validator Configuration Example Completed ===");

    Ok(())
}
