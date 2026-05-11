// ==============================
// 🔐 Crypto utilities for Savitri Node
// ==============================

use crate::core::types::Transaction;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use sha2::{Digest, Sha512};
use std::{fs, path::Path};

// Type alias for compatibility - in ed25519_dalek 2.x, Keypair is now SigningKey
pub type Keypair = SigningKey;
pub type PublicKey = VerifyingKey;

// ------------------------------
// 📦 Block Hashing
// ------------------------------

/// Compute deterministic tx root over the transaction list
///
/// Le transazioni are ordinate lessicograficamente in base alla loro serializzazione
pub fn compute_tx_root(txs: &[Transaction]) -> [u8; 64] {
    // Sort the transactions deterministically by their bincode encoding.
    // Invariant: every `Transaction` is bincode-serialisable by construction;
    // a serialisation failure here would mean the type evolved in a way that
    // breaks the on-wire contract and is a programmer error.
    let mut sorted_encodings: Vec<Vec<u8>> = txs
        .iter()
        .map(|tx| {
            bincode::serialize(tx)
                .expect("invariant: Transaction is bincode-serialisable by construction")
        })
        .collect();

    // Ordinamento lessicografico deterministico
    sorted_encodings.sort();

    let seed = Sha512::digest(b"TXv1");
    let mut acc = Sha512::new();
    acc.update(seed);
    for enc in sorted_encodings {
        let mut leaf = Sha512::new();
        leaf.update(b"TXv1");
        leaf.update(&enc);
        let l = leaf.finalize();
        acc.update(l);
    }
    let out = acc.finalize();
    let mut root = [0u8; 64];
    root.copy_from_slice(&out);
    root
}

// ------------------------------
// ✍️ Digital Signatures (ed25519-dalek)
// ------------------------------

/// Generates an ed25519 keypair (for signing TXs, blocks, etc.)
pub fn generate_keypair() -> Keypair {
    let mut csprng = OsRng {};
    Keypair::generate(&mut csprng)
}

/// Signs arbitrary data
pub fn sign_data(keypair: &Keypair, data: &[u8]) -> Signature {
    keypair.sign(data)
}

/// Verifies a signature
pub fn verify_signature(pubkey: &PublicKey, data: &[u8], sig: &Signature) -> bool {
    pubkey.verify(data, sig).is_ok()
}

/// Convert address bytes to PublicKey for verification
pub fn address_to_publickey(address: &[u8; 32]) -> anyhow::Result<PublicKey> {
    PublicKey::from_bytes(address).map_err(|e| anyhow::anyhow!("Invalid public key: {}", e))
}

// ------------------------------
// 🆔 Identity Persistence (libp2p compatible)
// ------------------------------

const KEY_FILE: &str = "identity.key";

/// libp2p-compatible identity keypair wrapper
/// This provides a compatible interface that can be easily extended
/// when libp2p dependency is added to the project
#[derive(Debug, Clone)]
pub struct IdentityKeypair {
    /// The underlying ed25519 signing key
    pub signing_key: SigningKey,
    /// The peer ID derived from the public key
    pub peer_id: String,
}

// SECURITY (HIGH-03): ed25519_dalek::SigningKey implements ZeroizeOnDrop in 2.x.
// The previous manual Drop called `to_bytes()` (a copy) and then zeroized the copy —
// the original signing key was never wiped. Removed in favour of the built-in impl.

impl IdentityKeypair {
    /// Create a new identity keypair
    pub fn new() -> Self {
        let signing_key = SigningKey::generate(&mut OsRng {});
        let peer_id = Self::derive_peer_id(&signing_key.verifying_key());

        Self {
            signing_key,
            peer_id,
        }
    }

    /// Create from existing signing key
    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let peer_id = Self::derive_peer_id(&signing_key.verifying_key());

        Self {
            signing_key,
            peer_id,
        }
    }

    /// Get the public key
    pub fn public_key(&self) -> PublicKey {
        self.signing_key.verifying_key()
    }

    /// Get the peer ID (libp2p compatible format)
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Get the signing key
    pub fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    /// Sign data with the identity key
    pub fn sign(&self, data: &[u8]) -> Signature {
        self.signing_key.sign(data)
    }

    /// Derive peer ID from public key (libp2p compatible format)
    fn derive_peer_id(public_key: &PublicKey) -> String {
        // libp2p peer IDs are derived from the public key using SHA-256
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(public_key.as_bytes());
        format!("12D3KooW{}", hex::encode(&hash[..20]))
    }

    /// Convert to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        self.signing_key.as_bytes().to_vec()
    }

    /// Convert from bytes
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "Invalid key length: expected 32 bytes, got {}",
                bytes.len()
            ));
        }

        let key_array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Failed to convert bytes to array"))?;

        let signing_key = SigningKey::from_bytes(&key_array);
        Ok(Self::from_signing_key(signing_key))
    }
}

/// Loads or generates a persistent identity keypair
/// This function provides libp2p-compatible identity management
/// without requiring the full libp2p dependency
pub fn load_or_generate_identity() -> IdentityKeypair {
    let path = Path::new(KEY_FILE);

    if path.exists() {
        match fs::read(path) {
            Ok(bytes) => match IdentityKeypair::from_bytes(&bytes) {
                Ok(kp) => {
                    println!("🔑 Loaded existing identity from {}", KEY_FILE);
                    println!("🆔 Peer ID: {}", kp.peer_id());
                    return kp;
                }
                Err(e) => {
                    eprintln!("⚠️ Failed to parse identity key: {}", e);
                    eprintln!("⚠️ Key file length: {} bytes", bytes.len());
                    eprintln!("⚠️ Regenerating new identity key...");
                }
            },
            Err(e) => {
                eprintln!("⚠️ Failed to read identity key: {}", e);
            }
        }
    }

    let kp = IdentityKeypair::new();
    let encoded = kp.to_bytes();

    if let Err(e) = fs::write(path, &encoded) {
        eprintln!("⚠️ Failed to write identity key: {}", e);
    } else {
        println!("📝 New identity key saved to {}", KEY_FILE);
        println!("🆔 Generated Peer ID: {}", kp.peer_id());
    }

    kp
}

/// Load identity keypair from specific path
pub fn load_identity_from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<IdentityKeypair> {
    let bytes = fs::read(path)?;
    IdentityKeypair::from_bytes(&bytes)
}

/// Save identity keypair to specific path
pub fn save_identity_to_path<P: AsRef<Path>>(kp: &IdentityKeypair, path: P) -> anyhow::Result<()> {
    let encoded = kp.to_bytes();
    fs::write(path, &encoded)?;
    Ok(())
}

/// Generate a new identity keypair without persistence
pub fn generate_identity() -> IdentityKeypair {
    let kp = IdentityKeypair::new();
    println!("🆔 Generated Peer ID: {}", kp.peer_id());
    kp
}

/// Validate identity keypair format
pub fn validate_identity_keypair(kp: &IdentityKeypair) -> Result<(), String> {
    // Check if peer ID is valid format
    if !kp.peer_id().starts_with("12D3KooW") {
        return Err("Invalid peer ID format".to_string());
    }

    if kp.peer_id().len() != 53 {
        // "12D3KooW" + 20 hex chars
        return Err("Invalid peer ID length".to_string());
    }

    // Verify peer ID matches public key
    let expected_peer_id = IdentityKeypair::derive_peer_id(&kp.public_key());
    if kp.peer_id() != expected_peer_id {
        return Err("Peer ID does not match public key".to_string());
    }

    Ok(())
}

/// Convert identity to libp2p Keypair (when libp2p is available)
/// This function provides libp2p integration when the dependency is available
#[cfg(feature = "libp2p")]
pub fn identity_to_libp2p_keypair(
    identity: IdentityKeypair,
) -> anyhow::Result<libp2p::identity::Keypair> {
    use libp2p::identity::Keypair;

    // libp2p 0.55+ removed the `Keypair::Ed25519(...)` constructor. The
    // canonical entry point is `Keypair::ed25519_from_bytes`, which takes
    // ownership of the 32-byte secret and zeroises the buffer once used.
    let mut secret_bytes = identity.signing_key().to_bytes();
    Keypair::ed25519_from_bytes(&mut secret_bytes)
        .map_err(|e| anyhow::anyhow!("libp2p ed25519_from_bytes: {}", e))
}

/// Convert libp2p keypair to identity (when libp2p is available).
/// This function provides libp2p integration when the dependency is available.
#[cfg(feature = "libp2p")]
pub fn libp2p_keypair_to_identity(keypair: libp2p::identity::Keypair) -> IdentityKeypair {
    // libp2p 0.55+ replaced `match Keypair { Ed25519(..) }` with the
    // fallible accessor `try_into_ed25519`. Non-Ed25519 variants fall back
    // to a fresh local identity, matching the prior behaviour.
    match keypair.try_into_ed25519() {
        Ok(ed25519_keypair) => {
            // Bind the SecretKey to a `let` so its lifetime spans the
            // `as_ref().try_into()` chain. libp2p-identity 0.2 returns
            // the secret by value, so without the binding the
            // temporary is dropped before we can borrow its bytes.
            let secret = ed25519_keypair.secret();
            // Invariant: ed25519 SecretKey is always 32 bytes.
            let secret_bytes: [u8; 32] = secret
                .as_ref()
                .try_into()
                .expect("invariant: ed25519 secret key is 32 bytes");
            let signing_key = ed25519_dalek::SigningKey::from_bytes(&secret_bytes);
            IdentityKeypair::from_signing_key(signing_key)
        }
        Err(_) => IdentityKeypair::new(),
    }
}

/// Derive peer ID from public key bytes
pub fn peer_id_from_public_key_bytes(pubkey_bytes: &[u8]) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(pubkey_bytes);
    Ok(format!("12D3KooW{}", hex::encode(&hash[..20])))
}
