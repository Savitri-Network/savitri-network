//! Tiny helper: reads a 32-byte ed25519 seed file, prints the hex public key.
//! Used by test scripts to build genesis_accounts lists.
//!
//! Additional mode: `--tx-gen-keys <count>` prints deterministic pubkeys
//! used by the tx generator's multi-sender pool (same derivation as
//! `tx::generate_tx_gen_sender_keys`).

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Mode: generate tx-gen sender pubkeys deterministically
    if args.len() >= 3 && args[1] == "--tx-gen-keys" {
        let count: usize = args[2].parse().unwrap_or_else(|_| {
            eprintln!("invalid count: {}", args[2]);
            std::process::exit(1);
        });
        for i in 0..count {
            let seed: [u8; 32] = {
                use sha2::Digest;
                sha2::Sha256::new()
                    .chain_update(b"savitri-tx-gen-sender-")
                    .chain_update((i as u32).to_le_bytes())
                    .finalize()
                    .into()
            };
            let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
            let pk = sk.verifying_key().to_bytes();
            println!("{}", hex::encode(pk));
        }
        return;
    }

    // Mode: derive pubkey from key file
    let path = match args.get(1) {
        Some(p) => p,
        None => {
            eprintln!("usage: derive-pubkey <key_file>");
            eprintln!("       derive-pubkey --tx-gen-keys <count>");
            std::process::exit(1);
        }
    };
    let bytes = std::fs::read(path).unwrap_or_else(|e| {
        eprintln!("failed to read {path}: {e}");
        std::process::exit(1);
    });
    let seed: [u8; 32] = bytes[..32].try_into().unwrap_or_else(|_| {
        eprintln!("key file must be at least 32 bytes");
        std::process::exit(1);
    });
    let sk = ed25519_dalek::SigningKey::from_bytes(&seed);
    let pk = sk.verifying_key().to_bytes();
    println!("{}", hex::encode(pk));
}
