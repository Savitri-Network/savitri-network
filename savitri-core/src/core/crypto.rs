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
    // Ordina le transazioni in modo deterministico basato on the serializzazione bincode
    let mut sorted_encodings: Vec<Vec<u8>> = txs
        .iter()
        .map(|tx| bincode::serialize(tx).expect("tx encode"))
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
    use libp2p::identity::ed25519::Keypair as Ed25519Keypair;
    use libp2p::identity::Keypair;

    // Convert our signing key to libp2p format
    let ed25519_keypair = Ed25519Keypair::from(identity.signing_key());
    let libp2p_keypair = Keypair::Ed25519(ed25519_keypair);

    Ok(libp2p_keypair)
}

/// Convert libp2p keypair to identity (when libp2p is available)
/// This function provides libp2p integration when the dependency is available
#[cfg(feature = "libp2p")]
pub fn libp2p_keypair_to_identity(keypair: libp2p::identity::Keypair) -> IdentityKeypair {
    use libp2p::identity::ed25519::Keypair as Ed25519Keypair;
    use libp2p::identity::Keypair;

    match keypair {
        Keypair::Ed25519(ed25519_keypair) => {
            let signing_key = ed25519_keypair.into();
            IdentityKeypair::from_signing_key(signing_key)
        }
        _ => {
            // For other keypair types, generate a new identity
            IdentityKeypair::new()
        }
    }
}

/// Derive peer ID from public key bytes
pub fn peer_id_from_public_key_bytes(pubkey_bytes: &[u8]) -> anyhow::Result<String> {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(pubkey_bytes);
    Ok(format!("12D3KooW{}", hex::encode(&hash[..20])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Block;

    #[test]
    fn block_hash_changes_when_parents_change() {
        // Construct a minimal block with deterministic fields
        let txs: Vec<Transaction> = vec![Transaction {
            from: "a".into(),
            to: "b".into(),
            amount: 1,
            ..Default::default()
        }];
        let proposer = [3u8; 32];
        let mut block = Block {
            version: 1,
            hash: [0u8; 64],
            transactions: txs,
            proposer,
            signature: [0u8; 64],
            state_root: [9u8; 64],
            parent_exec_hash: [1u8; 64],
            parent_ref_hash: [2u8; 64],
            height: 7,
            timestamp: 42,
            tx_root: [8u8; 64],
        };

        let h0 = block.header_hash();

        // Change only parent_exec_hash
        block.parent_exec_hash[0] ^= 0xFF;
        let h1 = block.header_hash();
        assert_ne!(h0, h1, "changing parent_exec_hash must flip block hash");

        // Revert and change only parent_ref_hash
        block.parent_exec_hash[0] ^= 0xFF; // revert
        block.parent_ref_hash[0] ^= 0xAA;
        let h2 = block.header_hash();
        assert_ne!(h0, h2, "changing parent_ref_hash must flip block hash");
    }

    #[test]
    fn tx_root_deterministic_ordering() {
        // Test che lo stesso set di transazioni in ordine diverso produca lo stesso hash
        let tx1 = Transaction {
            from: "alice".into(),
            to: "bob".into(),
            amount: 100,
            ..Default::default()
        };
        let tx2 = Transaction {
            from: "charlie".into(),
            to: "dave".into(),
            amount: 200,
            ..Default::default()
        };
        let tx3 = Transaction {
            from: "eve".into(),
            to: "frank".into(),
            amount: 300,
            ..Default::default()
        };

        // Ordine 1: tx1, tx2, tx3
        let txs1 = vec![tx1.clone(), tx2.clone(), tx3.clone()];
        let root1 = compute_tx_root(&txs1);

        // Ordine 2: tx3, tx1, tx2 (ordine diverso)
        let txs2 = vec![tx3.clone(), tx1.clone(), tx2.clone()];
        let root2 = compute_tx_root(&txs2);

        // Ordine 3: tx2, tx3, tx1 (altro ordine)
        let txs3 = vec![tx2.clone(), tx3.clone(), tx1.clone()];
        let root3 = compute_tx_root(&txs3);

        assert_eq!(
            root1, root2,
            "tx_root deve essere deterministico indipendentemente dall'ordine"
        );
        assert_eq!(
            root2, root3,
            "tx_root deve essere deterministico indipendentemente dall'ordine"
        );
        assert_eq!(
            root1, root3,
            "tx_root deve essere deterministico indipendentemente dall'ordine"
        );
    }

    #[test]
    fn tx_root_different_transactions_different_hash() {
        // Test che transazioni diverse producano hash diversi
        let tx1 = Transaction {
            from: "alice".into(),
            to: "bob".into(),
            amount: 100,
            ..Default::default()
        };
        let tx2 = Transaction {
            from: "alice".into(),
            to: "bob".into(),
            amount: 200, // amount diverso
            ..Default::default()
        };

        let root1 = compute_tx_root(&[tx1.clone()]);
        let root2 = compute_tx_root(&[tx2.clone()]);
        let root_both = compute_tx_root(&[tx1, tx2]);

        assert_ne!(
            root1, root2,
            "transazioni diverse devono produrre hash diversi"
        );
        assert_ne!(root1, root_both, "set diversi devono produrre hash diversi");
        assert_ne!(root2, root_both, "set diversi devono produrre hash diversi");
    }

    // Identity tests
    #[test]
    fn test_identity_keypair_creation() {
        let identity = IdentityKeypair::new();

        // Check peer ID format
        assert!(identity.peer_id().starts_with("12D3KooW"));
        assert_eq!(identity.peer_id().len(), 53); // "12D3KooW" + 20 hex chars

        // Check that peer ID is derived from public key
        let expected_peer_id = IdentityKeypair::derive_peer_id(&identity.public_key());
        assert_eq!(identity.peer_id(), expected_peer_id);
    }

    #[test]
    fn test_identity_from_signing_key() {
        let signing_key = SigningKey::generate(&mut OsRng {});
        let identity = IdentityKeypair::from_signing_key(signing_key);

        assert!(identity.peer_id().starts_with("12D3KooW"));
    }

    #[test]
    fn test_identity_serialization() {
        let identity = IdentityKeypair::new();

        // Test to_bytes and from_bytes
        let bytes = identity.to_bytes();
        assert_eq!(bytes.len(), 32);

        let restored = IdentityKeypair::from_bytes(&bytes).unwrap();
        assert_eq!(identity.peer_id(), restored.peer_id());
        assert_eq!(identity.public_key(), restored.public_key());
    }

    #[test]
    fn test_identity_signing() {
        let identity = IdentityKeypair::new();
        let message = b"Hello, Savitri!";

        let signature = identity.sign(message);
        assert!(verify_signature(
            &identity.public_key(),
            message,
            &signature
        ));
    }

    #[test]
    fn test_identity_validation() {
        let identity = IdentityKeypair::new();

        assert!(validate_identity_keypair(&identity).is_ok());

        // Test with invalid peer ID
        let mut invalid_identity = identity.clone();
        invalid_identity.peer_id = "invalid_peer_id".to_string();
        assert!(validate_identity_keypair(&invalid_identity).is_err());
    }

    #[test]
    fn test_generate_identity() {
        let identity = generate_identity();

        assert!(identity.peer_id().starts_with("12D3KooW"));
        assert_eq!(identity.peer_id().len(), 53);
    }

    #[test]
    fn test_peer_id_from_public_key_bytes() {
        let identity = IdentityKeypair::new();
        let public_key_bytes = identity.public_key().as_bytes();

        let peer_id = peer_id_from_public_key_bytes(public_key_bytes).unwrap();
        assert_eq!(peer_id, identity.peer_id());

        // Test with invalid length
        let invalid_bytes = vec![1u8; 31];
        assert!(peer_id_from_public_key_bytes(&invalid_bytes).is_err());
    }

    #[test]
    fn test_identity_from_invalid_bytes() {
        let invalid_bytes = vec![1u8; 31]; // Wrong length
        let result = IdentityKeypair::from_bytes(&invalid_bytes);
        assert!(result.is_err());

        let invalid_bytes = vec![1u8; 33]; // Wrong length
        let result = IdentityKeypair::from_bytes(&invalid_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_identities_different() {
        let identity1 = IdentityKeypair::new();
        let identity2 = IdentityKeypair::new();

        // Different identities should have different peer IDs
        assert_ne!(identity1.peer_id(), identity2.peer_id());
        assert_ne!(identity1.public_key(), identity2.public_key());
    }

    #[test]
    fn test_identity_consistent_peer_id() {
        let signing_key = SigningKey::generate(&mut OsRng {});

        // Create multiple identities from the same signing key
        let identity1 = IdentityKeypair::from_signing_key(signing_key.clone());
        let identity2 = IdentityKeypair::from_signing_key(signing_key);

        // Should have the same peer ID
        assert_eq!(identity1.peer_id(), identity2.peer_id());
        assert_eq!(identity1.public_key(), identity2.public_key());
    }
}

/// Compute hash over transaction
pub fn hash_tx(tx: &super::types::Transaction) -> [u8; 64] {
    let serialized = bincode::serialize(tx).expect("tx serialization failed");
    let hash = Sha512::digest(&serialized);
    let mut result = [0u8; 64];
    result.copy_from_slice(&hash);
    result
}
