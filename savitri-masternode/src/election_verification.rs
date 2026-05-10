//! Shared verification logic for election certificates
//! This module replicates the exact logic from lightnode to ensure consistent verification

use ed25519_dalek::{Signature, VerifyingKey};

/// Reconstruct signable bytes for ProposerElectionResult
/// This MUST match exactly with lightnode's ProposerElectionResult::signable_bytes()
pub fn election_result_signable_bytes(
    round: u64,
    elected_proposer: &str,
    proposer_pou_score: u32,
    sender: &str,
    group_id: &str,
    timestamp: u64,
    candidates: &[(String, u32, f64)],
) -> Result<Vec<u8>, serde_json::Error> {
    #[derive(serde::Serialize)]
    struct Signable<'a> {
        round: u64,
        elected_proposer: &'a str,
        proposer_pou_score: u32,
        sender: &'a str,
        group_id: &'a str,
        timestamp: u64,
        candidates: &'a Vec<(String, u32, f64)>,
    }
    
    let candidates_vec: Vec<(String, u32, f64)> = candidates.to_vec();
    serde_json::to_vec(&Signable {
        round,
        elected_proposer,
        proposer_pou_score,
        sender,
        group_id,
        timestamp,
        candidates: &candidates_vec,
    })
}

/// Create intragroup signing payload
/// This MUST match exactly with lightnode's intragroup_signing_payload()
/// Format: "savitri-intragroup-v1|<msg_type>|<group_id>|<signable>"
pub fn intragroup_signing_payload(group_id: &str, msg_type: &str, signable: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(b"savitri-intragroup-v1|");
    data.extend_from_slice(msg_type.as_bytes());
    data.extend_from_slice(b"|");
    data.extend_from_slice(group_id.as_bytes());
    data.extend_from_slice(b"|");
    data.extend_from_slice(signable);
    data
}

/// Verify an intragroup message signature
/// This MUST match exactly with lightnode's verify_intragroup_message()
pub fn verify_intragroup_message(
    sender_pubkey: &[u8; 32],
    group_id: &str,
    msg_type: &str,
    signable: &[u8],
    signature: &[u8; 64],
) -> bool {
    let verifying_key = match VerifyingKey::from_bytes(sender_pubkey) {
        Ok(key) => key,
        Err(_) => {
            return false;
        }
    };

    let payload = intragroup_signing_payload(group_id, msg_type, signable);
    let signature = Signature::from_bytes(signature);
    verifying_key.verify_strict(&payload, &signature).is_ok()
}
