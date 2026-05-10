use anyhow::{Context, Result};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;
use serde::Serialize;
use sha2::Digest;
use std::env;

#[derive(Serialize)]
struct TxWire {
    from: String,
    to: String,
    amount: u64,
    nonce: u64,
    fee: Option<u128>,
    data: Option<Vec<u8>>,
    pubkey: Vec<u8>,
    #[serde(with = "serde_big_array::BigArray")]
    sig: [u8; 64],
    pre_verified: bool,
}

fn build_raw_tx_hex(
    private_key_hex: &str,
    to: &str,
    amount: u64,
    nonce: u64,
    fee: u128,
) -> Result<(String, String)> {
    let key_bytes =
        hex::decode(private_key_hex.trim_start_matches("0x")).context("invalid private key hex")?;
    let key_bytes: [u8; 32] = key_bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("private key must be 32 bytes"))?;
    let signing_key = SigningKey::from_bytes(&key_bytes);
    let from = hex::encode(signing_key.verifying_key().to_bytes());

    let mut message = Vec::new();
    message.extend_from_slice(from.as_bytes());
    message.extend_from_slice(to.as_bytes());
    message.extend_from_slice(&amount.to_le_bytes());
    message.extend_from_slice(&nonce.to_le_bytes());
    message.extend_from_slice(&fee.to_le_bytes());
    let message_hash = sha2::Sha256::digest(&message);
    let sig = signing_key.sign(message_hash.as_slice()).to_bytes();

    let tx = TxWire {
        from: from.clone(),
        to: to.to_string(),
        amount,
        nonce,
        fee: Some(fee),
        data: None,
        pubkey: signing_key.verifying_key().to_bytes().to_vec(),
        sig,
        pre_verified: false,
    };
    let bytes = bincode::serialize(&tx).context("failed to serialize tx")?;
    Ok((from, hex::encode(bytes)))
}

fn generate_wallets(count: usize) {
    for _ in 0..count {
        let signing_key = SigningKey::generate(&mut OsRng);
        println!("private_key={}", hex::encode(signing_key.to_bytes()));
        println!(
            "address={}",
            hex::encode(signing_key.verifying_key().to_bytes())
        );
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("generate") => {
            let count = args
                .get(2)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(1);
            generate_wallets(count);
        }
        Some("build") => {
            let private_key = args.get(2).context("missing private key")?;
            let to = args.get(3).context("missing destination address")?;
            let amount = args
                .get(4)
                .context("missing amount")?
                .parse::<u64>()
                .context("invalid amount")?;
            let nonce = args
                .get(5)
                .context("missing nonce")?
                .parse::<u64>()
                .context("invalid nonce")?;
            let fee = args
                .get(6)
                .map(|s| s.parse::<u128>())
                .transpose()
                .context("invalid fee")?
                .unwrap_or(1);

            let (from, raw_tx_hex) = build_raw_tx_hex(private_key, to, amount, nonce, fee)?;
            println!("from={}", from);
            println!("raw_tx_hex={}", raw_tx_hex);
        }
        _ => {
            eprintln!("usage:");
            eprintln!("  raw_tx_builder generate [count]");
            eprintln!(
                "  raw_tx_builder build <private_key_hex> <to_address> <amount> <nonce> [fee]"
            );
            std::process::exit(2);
        }
    }
    Ok(())
}
