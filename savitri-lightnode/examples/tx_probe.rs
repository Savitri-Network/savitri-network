use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use savitri_lightnode::build_and_sign_transaction_ext;
use savitri_lightnode::tx::serialize_signed_tx;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Generate {
        #[arg(long, default_value_t = 1)]
        count: usize,
    },
    Build {
        #[arg(long)]
        private_key: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        amount: u64,
        #[arg(long)]
        nonce: u64,
        #[arg(long, default_value_t = 1)]
        fee: u128,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Generate { count } => {
            for _ in 0..count {
                let signing_key = SigningKey::generate(&mut OsRng);
                println!("private_key={}", hex::encode(signing_key.to_bytes()));
                println!(
                    "address={}",
                    hex::encode(signing_key.verifying_key().to_bytes())
                );
            }
        }
        Command::Build {
            private_key,
            to,
            amount,
            nonce,
            fee,
        } => {
            let pk_bytes = hex::decode(private_key.trim_start_matches("0x"))
                .context("invalid private key hex")?;
            let pk_bytes: [u8; 32] = pk_bytes
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("private key must be 32 bytes"))?;
            let signing_key = SigningKey::from_bytes(&pk_bytes);
            let from = hex::encode(signing_key.verifying_key().to_bytes());
            let tx = build_and_sign_transaction_ext(
                &signing_key,
                from.clone(),
                to,
                amount,
                nonce,
                Some(fee),
                None,
            );
            let raw = serialize_signed_tx(&tx)?;
            println!("from={}", from);
            println!("raw_tx_hex={}", hex::encode(raw));
        }
    }
    Ok(())
}
