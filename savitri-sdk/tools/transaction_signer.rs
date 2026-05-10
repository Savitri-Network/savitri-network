//! Tool: Transaction Signer
//!
//! Offline transaction signing utility.

use savitri_sdk::{TransactionBuilder, Wallet};
use std::io::{self, Write};

fn main() -> anyhow::Result<()> {
    println!("Savitri Network Transaction Signer\n");

    // Input private key
    print!("Private key (hex, 64 characters): ");
    io::stdout().flush()?;
    let mut private_key_hex = String::new();
    io::stdin().read_line(&mut private_key_hex)?;
    let private_key_hex = private_key_hex.trim();

    let wallet = Wallet::from_private_key_hex(private_key_hex)?;
    println!("Wallet address: {}\n", wallet.address());

    // Input transaction fields
    print!("Recipient address: ");
    io::stdout().flush()?;
    let mut to = String::new();
    io::stdin().read_line(&mut to)?;
    let to = to.trim().to_string();

    print!("Value: ");
    io::stdout().flush()?;
    let mut value_str = String::new();
    io::stdin().read_line(&mut value_str)?;
    let value: u128 = value_str.trim().parse()?;

    print!("Nonce: ");
    io::stdout().flush()?;
    let mut nonce_str = String::new();
    io::stdin().read_line(&mut nonce_str)?;
    let nonce: u64 = nonce_str.trim().parse().unwrap_or(0);

    print!("Fee (0 for default): ");
    io::stdout().flush()?;
    let mut fee_str = String::new();
    io::stdin().read_line(&mut fee_str)?;
    let fee: u128 = fee_str.trim().parse().unwrap_or(0);

    // Build and sign
    let mut builder = TransactionBuilder::new().to(to).value(value).nonce(nonce);
    if fee > 0 {
        builder = builder.fee(fee);
    }
    let tx = builder.build_and_sign(&wallet)?;

    println!("\nSigned transaction:\n");
    println!("From:      {}", tx.transaction.from);
    println!("To:        {}", tx.transaction.to.as_ref().unwrap());
    println!("Value:     {}", tx.transaction.value);
    println!("Nonce:     {}", tx.transaction.nonce);
    println!("Signature: {}", hex::encode(&tx.signature));

    let serialized = serde_json::to_string_pretty(&tx)?;
    println!("\nSerialized (JSON):\n{}", serialized);

    Ok(())
}
