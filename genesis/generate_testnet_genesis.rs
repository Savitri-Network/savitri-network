//! Genesis generator for the Savitri testnet.
//!
//! Produces a testnet genesis with 10 faucet wallets of 10M SAVT each, no
//! vesting. Intended for tests and development, not mainnet.
//!
//! Note: amounts use 8 decimals in the JSON output (10M SAVT = 10^15) so the
//! values fit in a `u64`. `savitri-core` rescales to 18 decimals at load time.
//!
//! Output files (written to the current working directory):
//!   - `genesis_testnet.json`        — the genesis JSON loaded by nodes
//!   - `private_keys_testnet.md`     — Markdown listing of faucet keys
//!   - `faucet_keys_testnet.json`    — JSON listing of faucet private keys
//!
//! ⚠ The output files contain private keys for testnet wallets. They are
//! written to the *current working directory* and **should never be checked
//! into version control**. The repository `.gitignore` excludes the three
//! filenames above.

use savitri_core::core::crypto::generate_keypair;
use std::fs;

/// 1 SAVT = 10^8 units in the genesis (rescaled to 10^18 by savitri-core). 10M SAVT = 10^15.
const ONE_SAVT: u64 = 100_000_000;

fn main() {
    println!("Genesis Testnet — Savitri Network\n");
    println!("10 faucet wallets, 10M SAVT each. For tests only.\n");

    let mut wallets = Vec::new();
    const AMOUNT: u64 = 10_000_000 * ONE_SAVT;
    let config = [
        ("Faucet 1 (10M SAVT)", AMOUNT),
        ("Faucet 2 (10M SAVT)", AMOUNT),
        ("Faucet 3 (10M SAVT)", AMOUNT),
        ("Faucet 4 (10M SAVT)", AMOUNT),
        ("Faucet 5 (10M SAVT)", AMOUNT),
        ("Faucet 6 (10M SAVT)", AMOUNT),
        ("Faucet 7 (10M SAVT)", AMOUNT),
        ("Faucet 8 (10M SAVT)", AMOUNT),
        ("Faucet 9 (10M SAVT)", AMOUNT),
        ("Faucet 10 (10M SAVT)", AMOUNT),
    ];

    for (desc, amount) in config {
        let keypair = generate_keypair();
        let address = format!("0x{}", hex::encode(keypair.verifying_key().as_bytes()));
        let secret = hex::encode(keypair.to_bytes());
        println!("  {}: {}", desc, &address);
        wallets.push((desc, address, secret, amount));
    }

    let total: u64 = config.iter().map(|(_, a)| a).sum();

    let mut tx_entries = Vec::new();
    for wallet in wallets.iter() {
        tx_entries.push(format!(
            r#"    {{ "from": "0x0000000000000000000000000000000000000000", "to": "{}", "amount": {} }}"#,
            wallet.1, wallet.3
        ));
    }
    let transactions_json = tx_entries.join(",\n");

    let genesis_json = format!(
        r#"{{
  "version": 1,
  "timestamp": 1700000000,
  "proposer": "{}",
  "signature": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  "state_root": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  "parent_exec_hash": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  "parent_ref_hash": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  "transactions": [
{}
  ]
}}"#,
        wallets[0].1, transactions_json
    );

    let out_path = "genesis_testnet.json";
    fs::write(out_path, &genesis_json).expect("failed to write genesis_testnet.json");

    let mut keys = String::from("# Testnet keys — FOR TEST USE ONLY\n\n");
    keys.push_str(
        "⚠ These are TEST keys. Do not use them on mainnet, do not fund them with real value, \
         and do not commit this file to version control.\n\n",
    );
    for (desc, address, secret, amount) in &wallets {
        keys.push_str(&format!("## {}\n", desc));
        keys.push_str(&format!("Address: `{}`\n", address));
        keys.push_str(&format!("Private Key: `{}`\n", secret));
        keys.push_str(&format!("Amount: {} SAVT\n\n", amount / ONE_SAVT));
    }
    keys.push_str("Total testnet supply: 100M SAVT across 10 faucet wallets, no vesting.\n");
    fs::write("private_keys_testnet.md", keys).expect("failed to write private_keys_testnet.md");

    let secret_hex_list: Vec<String> = wallets.iter().map(|(_, _, s, _)| s.clone()).collect();
    let faucet_json = serde_json::json!({ "private_keys": secret_hex_list });
    fs::write("faucet_keys_testnet.json", faucet_json.to_string())
        .expect("failed to write faucet_keys_testnet.json");

    println!("\nFiles written to the current working directory:");
    println!("   genesis_testnet.json");
    println!("   private_keys_testnet.md");
    println!("   faucet_keys_testnet.json");
    println!(
        "\nTestnet: {} SAVT total across 10 faucet wallets.",
        total / ONE_SAVT
    );
    println!(
        "\n⚠  The two key files contain private keys. Keep them out of git and any public location."
    );
}
