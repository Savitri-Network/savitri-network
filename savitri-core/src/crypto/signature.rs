//! Digital signatures

use ed25519_dalek::{
    Signature, Signer, SigningKey as Keypair, Verifier, VerifyingKey as PublicKey,
};

/// Sign a message using the provided Ed25519 keypair
///
/// # Arguments
/// * `message` - The message to sign
/// * `keypair` - The Ed25519 signing keypair
///
/// # Returns
/// The digital signature
pub fn sign(message: &[u8], keypair: &Keypair) -> Signature {
    keypair.sign(message)
}

/// Verify a digital signature using the public key
///
/// # Arguments
/// * `message` - The original message
/// * `signature` - The signature to verify
/// * `public_key` - The Ed25519 public key
///
/// # Returns
/// True if the signature is valid, false otherwise
pub fn verify(message: &[u8], signature: &Signature, public_key: &PublicKey) -> bool {
    public_key.verify(message, signature).is_ok()
}

// ─── Canonical TX signature (v1) ─────────────────────────────────────────
//
// Single source of truth for the SHA-256-then-ed25519 signature format used
// (savitri-lightnode/src/tx.rs::verify_transaction_signature_ext). Before
// this module existed the same logic was reimplemented in two places and
//
// Wire format `from`/`to` are 64-char hex STRINGS (not raw 32-byte
// addresses). The signable bytes concatenate the hex string bytes verbatim
// — no separators, no extra hashing of the hex layer — to keep parity with
// the legacy `TransactionExt` clients (rpc-loadtest, build_and_sign_transaction_ext).
//
// A separate `Path B` exists in savitri-lightnode/src/core/tx.rs that signs
// `format!("hex:hex:amount:nonce:fee")` directly via ed25519. That format
// is incompatible with this canonical one and is currently kept for legacy
// gossip handlers only — see architectural_debt.md "verify-signature-fork".

use sha2::{Digest as _, Sha256};

/// Build the canonical signable bytes (v1) for a Savitri transaction.
///
/// Format: `from_hex_bytes || to_hex_bytes || amount_u64_le || nonce_u64_le || fee_u128_le`
///
/// MUST be kept in sync with:
/// * `tools/rpc-loadtest/src/main.rs::sign_tx` (client signer)
/// * `savitri-lightnode/src/tx.rs::build_and_sign_transaction_ext` (LN/SDK signer)
/// * `savitri-lightnode/src/tx.rs::verify_transaction_signature_ext` (gossip RX verifier)
pub fn build_tx_signable_v1(
    from_hex_bytes: &[u8],
    to_hex_bytes: &[u8],
    amount: u64,
    nonce: u64,
    fee: u128,
) -> Vec<u8> {
    let mut msg = Vec::with_capacity(from_hex_bytes.len() + to_hex_bytes.len() + 8 + 8 + 16);
    msg.extend_from_slice(from_hex_bytes);
    msg.extend_from_slice(to_hex_bytes);
    msg.extend_from_slice(&amount.to_le_bytes());
    msg.extend_from_slice(&nonce.to_le_bytes());
    msg.extend_from_slice(&fee.to_le_bytes());
    msg
}

/// Verify a canonical TX signature (v1).
///
/// Hashes `signable` with SHA-256 then runs ed25519 `verify(digest, sig)`.
/// Returns false on any failure (bad pubkey/sig encoding or signature mismatch).
///
/// Pair with [`build_tx_signable_v1`] to construct `signable`.
pub fn verify_tx_signature_v1(
    signable: &[u8],
    signature_bytes: &[u8; 64],
    public_key_bytes: &[u8; 32],
) -> bool {
    let digest = Sha256::digest(signable);
    let pk = match PublicKey::from_bytes(public_key_bytes) {
        Ok(k) => k,
        Err(_) => return false,
    };
    let sig = Signature::from_bytes(signature_bytes);
    pk.verify(&digest, &sig).is_ok()
}

// ─── Canonical TX signature (v2) ─────────────────────────────────────────
//
// any future schema change (gas_limit, tip vs base fee, contract-call data
// hash) requires a flag day — and a Savitri TX signed for one chain replays
// on another deployment that adopts the same code. v2 prepends:
//
//   * 1-byte version tag = 0x02 (domain separation from v1, which has no
//     leading version byte; v1 starts with the 64 hex chars of `from`)
//   * 4-byte chain_id (big-endian u32)
//
// Then the v1 payload (from_hex || to_hex || amount_le || nonce_le || fee_le).
//
// Migration plan (NOT yet flipped on; v1 remains the wire default):
//   1. Land v2 helpers (this commit) — additive, no client impact.
//   2. Rev SDK + rpc-loadtest to emit v2 (versioned signable).
//   3. Mempool/lightnode verify accepts BOTH v1 and v2 during a sunset window.
//   4. Flag-day commit deletes v1 helpers and the legacy branches in
//      `savitri-lightnode/src/tx.rs` (data=Some / fee=None contract-call /
//      deprecated-builder paths flagged in audit §1.2).
//

/// Version tag byte for canonical signable v2.
pub const TX_SIGNABLE_V2_TAG: u8 = 0x02;

/// Build the canonical signable bytes (v2) for a Savitri transaction.
///
/// Layout:
/// ```text
///   [0]      version tag = 0x02
///   [1..5]   chain_id (u32 big-endian)
///   [5..]    v1 payload: from_hex || to_hex || amount_le || nonce_le || fee_le
/// ```
///
/// Domain separation: the leading 0x02 cannot collide with v1, where byte 0
/// is the first hex character of `from` (a printable ASCII '0'..='f' = 0x30..=0x66).
pub fn build_tx_signable_v2(
    chain_id: u32,
    from_hex_bytes: &[u8],
    to_hex_bytes: &[u8],
    amount: u64,
    nonce: u64,
    fee: u128,
) -> Vec<u8> {
    let mut msg =
        Vec::with_capacity(1 + 4 + from_hex_bytes.len() + to_hex_bytes.len() + 8 + 8 + 16);
    msg.push(TX_SIGNABLE_V2_TAG);
    msg.extend_from_slice(&chain_id.to_be_bytes());
    msg.extend_from_slice(from_hex_bytes);
    msg.extend_from_slice(to_hex_bytes);
    msg.extend_from_slice(&amount.to_le_bytes());
    msg.extend_from_slice(&nonce.to_le_bytes());
    msg.extend_from_slice(&fee.to_le_bytes());
    msg
}

/// Verify a canonical TX signature (v2). Same hash-then-ed25519 envelope as v1.
pub fn verify_tx_signature_v2(
    signable: &[u8],
    signature_bytes: &[u8; 64],
    public_key_bytes: &[u8; 32],
) -> bool {
    // Reject signables that don't carry the v2 tag — protects callers that
    // accidentally feed a v1 buffer to the v2 verifier (or vice versa via
    // length sniffing).
    if signable.first().copied() != Some(TX_SIGNABLE_V2_TAG) {
        return false;
    }
    verify_tx_signature_v1(signable, signature_bytes, public_key_bytes)
}

#[cfg(test)]
mod tests_canonical_v2 {
    use super::*;
    use ed25519_dalek::Signer;

    #[test]
    fn canonical_v2_roundtrip() {
        let kp = Keypair::from_bytes(&[3u8; 32]);
        let from_hex = "11".repeat(32);
        let to_hex = "22".repeat(32);
        let signable = build_tx_signable_v2(
            0xCAFEBABE,
            from_hex.as_bytes(),
            to_hex.as_bytes(),
            10u64,
            5u64,
            1000u128,
        );
        // domain separation: v2 must start with 0x02
        assert_eq!(signable[0], TX_SIGNABLE_V2_TAG);
        // chain_id round-trips big-endian
        assert_eq!(&signable[1..5], &0xCAFEBABEu32.to_be_bytes());
        let digest = Sha256::digest(&signable);
        let sig = kp.sign(digest.as_slice());
        let pk_bytes: [u8; 32] = kp.verifying_key().to_bytes();
        assert!(verify_tx_signature_v2(
            &signable,
            &sig.to_bytes(),
            &pk_bytes
        ));
    }

    #[test]
    fn v2_verifier_rejects_v1_signable() {
        // Verifier is strict on the tag byte — feeding v1 bytes (start with
        // a hex char, not 0x02) must fail.
        let kp = Keypair::from_bytes(&[5u8; 32]);
        let from_hex = "aa".repeat(32);
        let to_hex = "bb".repeat(32);
        let v1 = build_tx_signable_v1(from_hex.as_bytes(), to_hex.as_bytes(), 1u64, 0u64, 1u128);
        let digest = Sha256::digest(&v1);
        let sig = kp.sign(digest.as_slice());
        let pk_bytes: [u8; 32] = kp.verifying_key().to_bytes();
        assert!(!verify_tx_signature_v2(&v1, &sig.to_bytes(), &pk_bytes));
    }

    #[test]
    fn v2_chain_id_is_part_of_digest() {
        // Same payload, different chain_id → different digest → signature
        // from chain A must NOT verify against chain B.
        let kp = Keypair::from_bytes(&[7u8; 32]);
        let from_hex = "cc".repeat(32);
        let to_hex = "dd".repeat(32);
        let s_a =
            build_tx_signable_v2(1, from_hex.as_bytes(), to_hex.as_bytes(), 1u64, 0u64, 1u128);
        let s_b =
            build_tx_signable_v2(2, from_hex.as_bytes(), to_hex.as_bytes(), 1u64, 0u64, 1u128);
        let sig_a = kp.sign(Sha256::digest(&s_a).as_slice());
        let pk_bytes: [u8; 32] = kp.verifying_key().to_bytes();
        assert!(verify_tx_signature_v2(&s_a, &sig_a.to_bytes(), &pk_bytes));
        assert!(!verify_tx_signature_v2(&s_b, &sig_a.to_bytes(), &pk_bytes));
    }
}

#[cfg(test)]
mod tests_canonical_v1 {
    use super::*;
    use ed25519_dalek::Signer;

    #[test]
    fn canonical_v1_roundtrip() {
        let kp = Keypair::from_bytes(&[7u8; 32]);
        let from_hex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let to_hex = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";
        let signable = build_tx_signable_v1(
            from_hex.as_bytes(),
            to_hex.as_bytes(),
            42u64,
            7u64,
            1000u128,
        );
        let digest = Sha256::digest(&signable);
        let sig = kp.sign(digest.as_slice());
        let pk_bytes: [u8; 32] = kp.verifying_key().to_bytes();
        assert!(verify_tx_signature_v1(
            &signable,
            &sig.to_bytes(),
            &pk_bytes
        ));
    }

    #[test]
    fn canonical_v1_rejects_tampered_amount() {
        let kp = Keypair::from_bytes(&[9u8; 32]);
        let from_hex = "aa".repeat(32);
        let to_hex = "bb".repeat(32);
        let signable_orig =
            build_tx_signable_v1(from_hex.as_bytes(), to_hex.as_bytes(), 100u64, 1u64, 5u128);
        let digest = Sha256::digest(&signable_orig);
        let sig = kp.sign(digest.as_slice());
        let pk_bytes: [u8; 32] = kp.verifying_key().to_bytes();

        let signable_tampered =
            build_tx_signable_v1(from_hex.as_bytes(), to_hex.as_bytes(), 101u64, 1u64, 5u128);
        assert!(!verify_tx_signature_v1(
            &signable_tampered,
            &sig.to_bytes(),
            &pk_bytes
        ));
    }
}
