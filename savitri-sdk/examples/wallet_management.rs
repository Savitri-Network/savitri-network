//! Example: Wallet Management
//!
//! Shows how to create wallets, export/import keys, and sign messages.

use savitri_sdk::Wallet;

fn main() -> anyhow::Result<()> {
    println!("Savitri SDK - Wallet Management");

    // Create a new wallet
    let wallet = Wallet::new();
    println!("\nNew wallet created:");
    println!("  Address:     {}", wallet.address());
    println!(
        "  Private key: {} (DO NOT SHARE)",
        hex::encode(wallet.private_key())
    );

    // Sign and verify a message
    let message = b"Test message";
    let signature = wallet.sign_message(message);
    println!("\nSign message:");
    println!("  Message:   {:?}", std::str::from_utf8(message).unwrap());
    println!("  Signature: {}", hex::encode(signature));

    wallet.verify_signature(message, &signature)?;
    println!("  Signature verified");

    // Round-trip via private key bytes
    println!("\nLoad wallet from private key:");
    let private_key = wallet.private_key();
    let loaded = Wallet::from_private_key(&private_key)?;
    println!("  Original address: {}", wallet.address());
    println!("  Loaded address:   {}", loaded.address());
    assert_eq!(wallet.address(), loaded.address());
    println!("  Addresses match");

    // Round-trip via hex string
    let hex_key = hex::encode(wallet.private_key());
    let loaded2 = Wallet::from_private_key_hex(&hex_key)?;
    assert_eq!(wallet.address(), loaded2.address());
    println!("  Hex import matches");

    Ok(())
}
