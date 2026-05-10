//! Federated Learning Governance Proposals
//!
//!
//! ## Tipi di Proposte FL
//!
//!    - fee_treasury_bps: Percentuale fee per treasury (basis points)
//!    - max_models: Numero massimo di modelli FL consentiti
//!    - whitelist_aggregators: List aggregatori autorizzati
//!
//!
//! 3. **AbortFlRound**: Abortisce un round FL in corso (emergenza)
//!    - model_id: Identificatore of the modello
//!    - round_id: Identificatore of the round da abortire
//!
//! ## Validazioni Policy FL
//!
//! - I parametri policy siano entro range validi
//! - Gli aggregatori in the whitelist siano validi (32 bytes addresses)
//! - I model_id e round_id siano validi
//! - Le policy siano rispettate nei contract calls FL

use crate::governance::proposals::ProposalAction;
use crate::storage::{FlPolicy, Storage};
use anyhow::{Context, Result};
use hex;

/// Maximum allowed size for deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads from storage.
const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

///
/// Check che:
/// - fee_treasury_bps sia <= 10000 (100%)
/// - max_models sia > 0
///
/// # Argomenti
/// - `fee_treasury_bps`: Percentuale fee treasury in basis points
/// - `max_models`: Numero massimo di modelli FL
/// - `whitelist_aggregators`: List aggregatori autorizzati (hex strings)
///
/// # Ritorna
pub fn validate_set_fl_policy(
    fee_treasury_bps: u16,
    max_models: u32,
    whitelist_aggregators: &[String],
) -> Result<()> {
    if fee_treasury_bps > 10_000 {
        anyhow::bail!(
            "fee_treasury_bps deve essere <= 10000 (100%), fornito: {}",
            fee_treasury_bps
        );
    }

    if max_models == 0 {
        anyhow::bail!("max_models deve essere > 0");
    }

    for (idx, addr) in whitelist_aggregators.iter().enumerate() {
        let bytes = hex::decode(addr)
            .with_context(|| format!("Invalid aggregator address at index {}: {}", idx, addr))?;
        if bytes.len() != 32 {
            anyhow::bail!(
                "Aggregator address at index {} deve essere 32 bytes, trovato {} bytes",
                idx,
                bytes.len()
            );
        }
    }

    Ok(())
}

///
/// Check che model_id sia un identificatore valido (32 bytes hex).
///
/// # Argomenti
///
/// # Ritorna
pub fn validate_approve_fl_model(model_id: &str) -> Result<()> {
    let bytes =
        hex::decode(model_id).with_context(|| format!("Invalid model_id hex: {}", model_id))?;
    if bytes.len() != 32 {
        anyhow::bail!(
            "model_id deve essere 32 bytes, trovato {} bytes",
            bytes.len()
        );
    }
    Ok(())
}

///
/// Check che:
/// - model_id sia valido (32 bytes hex)
/// - round_id sia > 0
///
/// # Argomenti
/// - `round_id`: Identificatore of the round
///
/// # Ritorna
pub fn validate_abort_fl_round(model_id: &str, round_id: u64) -> Result<()> {
    validate_approve_fl_model(model_id)?;
    if round_id == 0 {
        anyhow::bail!("round_id deve essere > 0");
    }
    Ok(())
}

///
///
/// # Argomenti
///
/// # Ritorna
pub fn validate_fl_proposal_action(action: &ProposalAction) -> Result<()> {
    match action {
        ProposalAction::SetFlPolicy {
            fee_treasury_bps,
            max_models,
            whitelist_aggregators,
        } => validate_set_fl_policy(*fee_treasury_bps, *max_models, whitelist_aggregators),
        ProposalAction::ApproveFlModel { model_id } => validate_approve_fl_model(model_id),
        ProposalAction::AbortFlRound { model_id, round_id } => {
            validate_abort_fl_round(model_id, *round_id)
        }
        _ => anyhow::bail!("Azione non è una proposta FL"),
    }
}

///
/// # Argomenti
/// - `storage`: Storage per accedere alla policy FL
/// - `aggregator`: Indirizzo aggregator (32 bytes)
///
/// # Ritorna
pub fn is_aggregator_whitelisted(storage: &Storage, aggregator: &[u8]) -> Result<bool> {
    if aggregator.len() != 32 {
        return Ok(false);
    }

    let policy = match storage.get_fl_policy()? {
        Some(bytes) => {
            if bytes.len() > MAX_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "FL policy data too large: {} bytes (max {})",
                    bytes.len(),
                    MAX_DESERIALIZE_SIZE
                );
            }
            bincode::deserialize::<FlPolicy>(&bytes)?
        }
        None => return Ok(true), // Nessuna policy = tutti consentiti
    };

    if policy.whitelist_aggregators.is_empty() {
        return Ok(true);
    }

    // Check se aggregator è in the whitelist
    Ok(policy
        .whitelist_aggregators
        .iter()
        .any(|addr| addr == aggregator))
}

///
/// # Argomenti
/// - `storage`: Storage per verificare approvazione modello
///
/// # Ritorna
pub fn is_fl_model_approved(storage: &Storage, model_id: &[u8]) -> Result<bool> {
    if model_id.len() != 32 {
        return Ok(false);
    }
    storage.is_fl_model_approved(model_id)
}

/// Check se un round FL è stato abortito dalla governance
///
/// # Argomenti
/// - `storage`: Storage per verificare abort round
/// - `round_id`: Identificatore of the round
///
/// # Ritorna
pub fn is_fl_round_aborted(storage: &Storage, _model_id: &[u8], round_id: u64) -> Result<bool> {
    storage.is_fl_round_aborted(round_id)
}

/// Ottiene la policy FL corrente
///
/// # Argomenti
/// - `storage`: Storage per recuperare la policy
///
/// # Ritorna
/// `Ok(Some(policy))` se esiste una policy, `Ok(None)` se non esiste
pub fn get_fl_policy(storage: &Storage) -> Result<Option<FlPolicy>> {
    match storage.get_fl_policy()? {
        Some(bytes) => {
            if bytes.len() > MAX_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "FL policy data too large: {} bytes (max {})",
                    bytes.len(),
                    MAX_DESERIALIZE_SIZE
                );
            }
            Ok(Some(bincode::deserialize::<FlPolicy>(&bytes)?))
        }
        None => Ok(None),
    }
}

///
///
/// # Argomenti
/// - `storage`: Storage per recuperare la policy
/// - `min_trainers`: Numero minimo di trainers richiesti
/// - `max_trainers`: Numero massimo di trainers consentiti
///
/// # Ritorna
pub fn validate_trainer_limits(
    storage: &Storage,
    min_trainers: u32,
    max_trainers: u32,
) -> Result<()> {
    // Validazioni base
    if min_trainers == 0 {
        anyhow::bail!("min_trainers deve essere > 0");
    }
    if max_trainers < min_trainers {
        anyhow::bail!(
            "max_trainers ({}) deve essere >= min_trainers ({})",
            max_trainers,
            min_trainers
        );
    }

    // Check policy FL se esiste
    if let Some(_policy) = storage.get_fl_policy()? {
        // La policy FL non ha limiti espliciti su min/max trainers,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Create a temporary Storage for tests (replaces missing crate::test_utils)
    fn create_test_storage(prefix: &str) -> anyhow::Result<(Storage, PathBuf)> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("savitri-{}-test-{}", prefix, nanos));
        std::fs::create_dir_all(&p)?;
        let storage = Storage::new(&p)?;
        Ok((storage, p))
    }

    #[test]
    fn test_validate_set_fl_policy() {
        // Test valido
        assert!(validate_set_fl_policy(250, 5, &[]).is_ok());
        assert!(validate_set_fl_policy(10000, 1, &["a".repeat(64), "b".repeat(64)]).is_ok());

        // Test invalidi
        assert!(validate_set_fl_policy(10001, 5, &[]).is_err()); // fee troppo alto
        assert!(validate_set_fl_policy(250, 0, &[]).is_err()); // max_models = 0
        assert!(validate_set_fl_policy(250, 5, &["invalid".to_string()]).is_err());
        // hex invalido
    }

    #[test]
    fn test_validate_approve_fl_model() {
        // Test valido
        assert!(validate_approve_fl_model(&"a".repeat(64)).is_ok());

        // Test invalidi
        assert!(validate_approve_fl_model("invalid").is_err());
        assert!(validate_approve_fl_model(&"a".repeat(32)).is_err()); // troppo corto
    }

    #[test]
    fn test_validate_abort_fl_round() {
        // Test valido
        assert!(validate_abort_fl_round(&"a".repeat(64), 1).is_ok());

        // Test invalidi
        assert!(validate_abort_fl_round(&"a".repeat(64), 0).is_err()); // round_id = 0
        assert!(validate_abort_fl_round("invalid", 1).is_err()); // model_id invalido
    }

    #[test]
    fn test_validate_fl_proposal_action() {
        // Test SetFlPolicy
        let action = ProposalAction::SetFlPolicy {
            fee_treasury_bps: 250,
            max_models: 5,
            whitelist_aggregators: vec![],
        };
        assert!(validate_fl_proposal_action(&action).is_ok());

        // Test ApproveFlModel
        let action = ProposalAction::ApproveFlModel {
            model_id: "a".repeat(64),
        };
        assert!(validate_fl_proposal_action(&action).is_ok());

        // Test AbortFlRound
        let action = ProposalAction::AbortFlRound {
            model_id: "a".repeat(64),
            round_id: 1,
        };
        assert!(validate_fl_proposal_action(&action).is_ok());
    }

    #[test]
    fn test_is_aggregator_whitelisted() {
        let (storage, _tmp) = create_test_storage("fl-whitelist").unwrap();

        assert!(is_aggregator_whitelisted(&storage, &[1u8; 32]).unwrap());

        // Creates policy con whitelist
        let policy = FlPolicy {
            fee_treasury_bps: 250,
            max_models: 5,
            whitelist_aggregators: vec![vec![1u8; 32], vec![2u8; 32]],
            min_contributions_per_round: FlPolicy::default().min_contributions_per_round,
            max_contributions_per_round: FlPolicy::default().max_contributions_per_round,
            reward_per_contribution: FlPolicy::default().reward_per_contribution,
            round_duration_blocks: FlPolicy::default().round_duration_blocks,
            model_approval_required: FlPolicy::default().model_approval_required,
        };
        let policy_bytes = bincode::serialize(&policy).unwrap();
        storage.set_fl_policy(&policy_bytes).unwrap();

        // Aggregator in the whitelist
        assert!(is_aggregator_whitelisted(&storage, &[1u8; 32]).unwrap());
        assert!(is_aggregator_whitelisted(&storage, &[2u8; 32]).unwrap());

        // Aggregator non in the whitelist
        assert!(!is_aggregator_whitelisted(&storage, &[3u8; 32]).unwrap());
    }

    #[test]
    fn test_validate_trainer_limits() {
        let (storage, _tmp) = create_test_storage("fl-trainers").unwrap();

        // Test validi
        assert!(validate_trainer_limits(&storage, 1, 10).is_ok());
        assert!(validate_trainer_limits(&storage, 5, 5).is_ok());

        // Test invalidi
        assert!(validate_trainer_limits(&storage, 0, 10).is_err()); // min = 0
        assert!(validate_trainer_limits(&storage, 10, 5).is_err()); // max < min
    }
}
