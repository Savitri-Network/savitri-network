//! Tool: Key Generator
//!
//! Generates a new Ed25519 keypair for a Savitri Network wallet.

use savitri_sdk::Wallet;
use std::io::{self, Write};

fn main() -> anyhow::Result<()> {
    println!("Savitri Network Key Generator\n");

    let wallet = Wallet::new();

    println!("New keypair generated:\n");
    println!("Address (public key hex):");
    println!("  {}", wallet.address());

    println!("\nPrivate key (32 bytes hex):");
    println!("  {}", hex::encode(wallet.private_key()));

    println!("\nIMPORTANT: Keep the private key secret.");
    println!("Never share it with anyone.\n");

    print!("Save private key to file? (y/n): ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    if input.trim().eq_ignore_ascii_case("y") {
        let filename = format!("savitri_key_{}.txt", &wallet.address()[..8]);
        let content = format!(
            "Savitri Network Private Key\n\
             ============================\n\n\
             Address: {}\n\
             Private Key: {}\n\n\
             DO NOT SHARE THIS KEY.\n",
            wallet.address(),
            hex::encode(wallet.private_key())
        );

        std::fs::write(&filename, content)?;
        println!("Key saved to: {}", filename);
    }

    Ok(())
}
