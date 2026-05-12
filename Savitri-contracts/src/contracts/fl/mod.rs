//! Federated Learning AI Manager (FLAIManager)
//!
//! - `model_registry.rs`: registrazione modelli, versioning, access control
//! - `job_lifecycle.rs`: gestione round FL, update submissions, rewards
//!
//! Lo storage utilizza slot 100+ e segue la convenzione `keccak256(key || base_slot)` -> u64
//! (little-endian sui primi 8 byte) per i mapping annidati.
//!
//! Integrazione governance hooks: ApproveFlModel, SetFlPolicy, AbortFlRound

pub mod job_lifecycle;
pub mod model_registry;

use crate::contracts::base::BaseContract;
use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use crate::governance::proposals::ProposalAction;
use anyhow::{Context, Result};
use savitri_storage::storage::Storage;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

pub use job_lifecycle::JobLifecycle;
pub use model_registry::ModelRegistry;

fn decode_model_id_hex_to_u64(model_id: &str) -> Result<u64> {
    let bytes =
        hex::decode(model_id).with_context(|| format!("Invalid model_id hex: {model_id}"))?;
    if bytes.len() != 32 {
        anyhow::bail!("model_id deve essere 32 bytes, trovato {}", bytes.len());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    Ok(u64::from_le_bytes(arr))
}

/// Slot di storage riservati al modulo FL (100+)
pub const SLOT_STORAGE_VERSION: u64 = 100;
pub const SLOT_TREASURY_ADDRESS: u64 = 101;
pub const SLOT_TREASURY_FEE_BPS: u64 = 102;
pub const SLOT_CONTRACT_REGISTRY_HASH: u64 = 103;
pub const SLOT_MODEL_COUNT: u64 = 110;

pub const SLOT_MODELS_BASE: u64 = 200;
pub const SLOT_VERSIONS_BASE: u64 = 300;
pub const SLOT_ROUNDS_BASE: u64 = 400;
pub const SLOT_UPDATES_BASE: u64 = 500;
pub const SLOT_ROLES_BASE: u64 = 600;
pub const SLOT_ALLOWLIST_BASE: u64 = 700;
pub const SLOT_DENYLIST_BASE: u64 = 800;
pub const SLOT_NONCES_BASE: u64 = 900;
pub const SLOT_CLAIM_BASE: u64 = 1000;
pub const SLOT_POOL_BASE: u64 = 1100; // accounting interna per fund/claim

/// Offsets per Model (slot base calcolato per model_id)
const MODEL_OFFSET_CREATOR: u64 = 0;
const MODEL_OFFSET_NFT_CONTRACT: u64 = 1;
const MODEL_OFFSET_TOKEN_CLASS: u64 = 2;
const MODEL_OFFSET_CURRENT_VERSION: u64 = 3;
const MODEL_OFFSET_METADATA: u64 = 4;
const MODEL_OFFSET_LICENSE: u64 = 5;
const MODEL_OFFSET_ACCESS_POLICY: u64 = 6;
const MODEL_OFFSET_REWARD_POLICY: u64 = 7;
const MODEL_OFFSET_NEXT_ROUND: u64 = 8;

/// Offsets per Version
const VERSION_OFFSET_PARENT: u64 = 0;
const VERSION_OFFSET_WEIGHTS_COMMIT: u64 = 1;
const VERSION_OFFSET_URI_HASH: u64 = 2;
const VERSION_OFFSET_ROUND_ID: u64 = 3;
const VERSION_OFFSET_AGGREGATOR: u64 = 4;
const VERSION_OFFSET_TIMESTAMP: u64 = 5;

/// Offsets per Round
const ROUND_OFFSET_STATUS: u64 = 0;
const ROUND_OFFSET_BASE_VERSION: u64 = 1;
const ROUND_OFFSET_OPEN_FROM: u64 = 2;
const ROUND_OFFSET_OPEN_UNTIL: u64 = 3;
const ROUND_OFFSET_MIN_UPDATES: u64 = 4;
const ROUND_OFFSET_MAX_UPDATES: u64 = 5;
const ROUND_OFFSET_UPDATE_COUNT: u64 = 6;
const ROUND_OFFSET_AGGREGATED_COMMIT: u64 = 7;
const ROUND_OFFSET_ACCEPTED_ROOT: u64 = 8;
const ROUND_OFFSET_SCORE_ROOT: u64 = 9;
const ROUND_OFFSET_CHECKPOINT_URI: u64 = 10;

/// Offsets per Update
const UPDATE_OFFSET_COMMIT: u64 = 0;
const UPDATE_OFFSET_URI_HASH: u64 = 1;
const UPDATE_OFFSET_NONCE: u64 = 2;
const UPDATE_OFFSET_TIMESTAMP: u64 = 3;

/// Ruoli RBAC
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    Creator = 0,
    Viewer = 1,
    Trainer = 2,
    Aggregator = 3,
}

/// Stato round
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RoundStatus {
    Planned = 0,
    Open = 1,
    Sealed = 2,
    Finalized = 3,
    Aborted = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelView {
    pub creator: [u8; 32],
    pub nft_contract: [u8; 32],
    pub token_class: u128,
    pub current_version: u64,
    pub metadata_hash: [u8; 32],
    pub license_hash: [u8; 32],
    pub access_policy_hash: [u8; 32],
    pub reward_policy_hash: [u8; 32],
    pub next_round_id: u64,
}

/// Struttura versione (view)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionView {
    pub parent_version: u64,
    pub weights_commit: [u8; 32],
    pub uri_hash: [u8; 32],
    pub round_id: u64,
    pub aggregator: [u8; 32],
    pub timestamp: u64,
}

/// Struttura round (view)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundView {
    pub status: RoundStatus,
    pub base_version: u64,
    pub open_from: u64,
    pub open_until: u64,
    pub min_updates: u32,
    pub max_updates: u32,
    pub update_count: u32,
    pub aggregated_commit: [u8; 32],
    pub accepted_root: [u8; 32],
    pub score_root: [u8; 32],
    pub checkpoint_uri_hash: [u8; 32],
}

/// Struttura update (view)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateView {
    pub commit: [u8; 32],
    pub uri_hash: [u8; 32],
    pub nonce: u64,
    pub timestamp: u64,
}

/// Manager principale
pub struct FlAiManager;

impl FlAiManager {
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner_address: &[u8],
        treasury_address: &[u8],
        fee_bps: u16,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        BaseContract::initialize(
            contract_storage,
            storage,
            owner_address,
            gas_meter.as_deref_mut(),
        )?;

        ensure_address32(owner_address)?;
        ensure_address32(treasury_address)?;
        validate_fee_bps(fee_bps)?;

        write_u64(
            contract_storage,
            storage,
            SLOT_STORAGE_VERSION,
            1,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            SLOT_TREASURY_ADDRESS,
            treasury_address,
            gas_meter.as_deref_mut(),
        )?;
        write_u16(
            contract_storage,
            storage,
            SLOT_TREASURY_FEE_BPS,
            fee_bps,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(contract_storage, storage, SLOT_MODEL_COUNT, 0, gas_meter)?;
        Ok(())
    }

    /// Returns treasury e fee bps.
    pub fn get_treasury(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<([u8; 32], u16)> {
        let treasury = read_bytes32(
            contract_storage,
            storage,
            SLOT_TREASURY_ADDRESS,
            gas_meter.as_deref_mut(),
        )?;
        let fee = read_u16(contract_storage, storage, SLOT_TREASURY_FEE_BPS, gas_meter)?;
        Ok((treasury, fee))
    }

    /// Governance hook: gestisce proposal types FL (ApproveFlModel, SetFlPolicy, AbortFlRound).
    ///
    /// una proposta governance viene approvata e il governance hook è abilitato.
    pub fn on_governance_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        action: &ProposalAction,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        match action {
            ProposalAction::ApproveFlModel { model_id } => {
                let model_id_u64 = decode_model_id_hex_to_u64(model_id)?;
                ModelRegistry::on_governance_approve_model(
                    contract_storage,
                    storage,
                    model_id_u64,
                    gas_meter,
                )?;
            }
            ProposalAction::SetFlPolicy { .. } => {
                // L'implementazione è gestita in governance/execution.rs
                // Qui possiamo aggiungere logica aggiuntiva se necessario
            }
            ProposalAction::AbortFlRound { model_id, round_id } => {
                let model_id_u64 = decode_model_id_hex_to_u64(model_id)?;
                JobLifecycle::on_governance_abort_round(
                    contract_storage,
                    storage,
                    model_id_u64,
                    *round_id,
                    gas_meter,
                )?;
            }
            _ => {
                // Ignora altri tipi di proposal
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn register_model(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        nft_contract: &[u8],
        token_class: u128,
        metadata_hash: [u8; 32],
        license_hash: [u8; 32],
        initial_weights_commit: [u8; 32],
        initial_checkpoint_uri_hash: [u8; 32],
        timestamp: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        ensure_address32(caller)?;
        ensure_address32(nft_contract)?;

        let model_id = read_u64(
            contract_storage,
            storage,
            SLOT_MODEL_COUNT,
            gas_meter.as_deref_mut(),
        )?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("model_id overflow"))?;
        write_u64(
            contract_storage,
            storage,
            SLOT_MODEL_COUNT,
            model_id,
            gas_meter.as_deref_mut(),
        )?;

        let base = mapping_slot_single(&model_id.to_le_bytes(), SLOT_MODELS_BASE);
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_CREATOR,
            caller,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_NFT_CONTRACT,
            nft_contract,
            gas_meter.as_deref_mut(),
        )?;
        write_u128(
            contract_storage,
            storage,
            base + MODEL_OFFSET_TOKEN_CLASS,
            token_class,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            base + MODEL_OFFSET_CURRENT_VERSION,
            0,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_METADATA,
            &metadata_hash,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_LICENSE,
            &license_hash,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_ACCESS_POLICY,
            &[0u8; 32],
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_REWARD_POLICY,
            &[0u8; 32],
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            base + MODEL_OFFSET_NEXT_ROUND,
            0,
            gas_meter.as_deref_mut(),
        )?;

        // Set ruolo Creator al caller
        let role_slot = mapping_slot_triple(
            &model_id.to_le_bytes(),
            caller,
            &(Role::Creator as u8).to_le_bytes(),
            SLOT_ROLES_BASE,
        );
        write_bool(
            contract_storage,
            storage,
            role_slot,
            true,
            gas_meter.as_deref_mut(),
        )?;

        // Creates versione 0 (genesis)
        create_version_internal(
            contract_storage,
            storage,
            model_id,
            0,
            initial_weights_commit,
            initial_checkpoint_uri_hash,
            0,
            caller,
            timestamp,
            gas_meter,
        )?;

        Ok(model_id)
    }

    pub fn set_model_metadata(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        metadata_hash: [u8; 32],
        license_hash: [u8; 32],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let base = model_slot(model_id);
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_METADATA,
            &metadata_hash,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_LICENSE,
            &license_hash,
            gas_meter,
        )?;
        Ok(())
    }

    pub fn set_access_policy_hash(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        policy_hash: [u8; 32],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let base = model_slot(model_id);
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_ACCESS_POLICY,
            &policy_hash,
            gas_meter,
        )?;
        Ok(())
    }

    pub fn set_reward_policy_hash(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        policy_hash: [u8; 32],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let base = model_slot(model_id);
        write_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_REWARD_POLICY,
            &policy_hash,
            gas_meter,
        )?;
        Ok(())
    }

    /// Concede ruolo.
    pub fn grant_role(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        account: &[u8],
        role: Role,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        ensure_address32(account)?;
        let slot = role_slot(model_id, account, role);
        write_bool(contract_storage, storage, slot, true, gas_meter)?;
        Ok(())
    }

    /// Revoke ruolo.
    pub fn revoke_role(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        account: &[u8],
        role: Role,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        ensure_address32(account)?;
        let slot = role_slot(model_id, account, role);
        write_bool(contract_storage, storage, slot, false, gas_meter)?;
        Ok(())
    }

    /// Set allowlist.
    pub fn set_allowlisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        account: &[u8],
        allowed: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        ensure_address32(account)?;
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, SLOT_ALLOWLIST_BASE);
        write_bool(contract_storage, storage, slot, allowed, gas_meter)?;
        Ok(())
    }

    /// Set denylist.
    pub fn set_denylisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        account: &[u8],
        denied: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        ensure_address32(account)?;
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, SLOT_DENYLIST_BASE);
        write_bool(contract_storage, storage, slot, denied, gas_meter)?;
        Ok(())
    }

    /// Creates round in stato Planned, incrementando round_id interno.
    #[allow(clippy::too_many_arguments)]
    pub fn create_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        base_version: u64,
        open_from: u64,
        open_until: u64,
        min_updates: u32,
        max_updates: u32,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        ensure_creator_or_aggregator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let model = Self::get_model(
            contract_storage,
            storage,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        if base_version > model.current_version {
            anyhow::bail!("ERR_INVALID_ROUND");
        }

        // round id = next_round_id
        let base_slot = model_slot(model_id);
        let round_id = read_u64(
            contract_storage,
            storage,
            base_slot + MODEL_OFFSET_NEXT_ROUND,
            gas_meter.as_deref_mut(),
        )?;
        let next_round = round_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("round id overflow"))?;
        write_u64(
            contract_storage,
            storage,
            base_slot + MODEL_OFFSET_NEXT_ROUND,
            next_round,
            gas_meter.as_deref_mut(),
        )?;

        let round_slot = round_slot(model_id, round_id);
        write_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            RoundStatus::Planned as u8,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_BASE_VERSION,
            base_version,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_OPEN_FROM,
            open_from,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_OPEN_UNTIL,
            open_until,
            gas_meter.as_deref_mut(),
        )?;
        write_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_MIN_UPDATES,
            min_updates,
            gas_meter.as_deref_mut(),
        )?;
        write_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_MAX_UPDATES,
            max_updates,
            gas_meter.as_deref_mut(),
        )?;
        write_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_UPDATE_COUNT,
            0,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_AGGREGATED_COMMIT,
            &[0u8; 32],
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_ACCEPTED_ROOT,
            &[0u8; 32],
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_SCORE_ROOT,
            &[0u8; 32],
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_CHECKPOINT_URI,
            &[0u8; 32],
            gas_meter,
        )?;

        emit_event(
            storage,
            EventSystem::new(),
            CustomEvent {
                contract_address: String::new(),
                event_name: "RoundCreated".into(),
                topics: vec![],
                data: vec![],
            },
            None,
        );

        Ok(round_id)
    }

    /// Apre un round (Planned -> Open).
    pub fn open_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator_or_aggregator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let round_slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        if status != RoundStatus::Planned as u8 {
            anyhow::bail!("ERR_ROUND_NOT_OPEN");
        }
        write_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            RoundStatus::Open as u8,
            gas_meter,
        )?;
        Ok(())
    }

    /// Sigilla round (Open -> Sealed).
    pub fn seal_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator_or_aggregator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let round_slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        if status != RoundStatus::Open as u8 {
            anyhow::bail!("ERR_ROUND_NOT_OPEN");
        }
        write_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            RoundStatus::Sealed as u8,
            gas_meter,
        )?;
        Ok(())
    }

    /// Aborta round (qualsiasi stato non Finalized).
    pub fn abort_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let round_slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        if status == RoundStatus::Finalized as u8 {
            anyhow::bail!("ERR_ROUND_ALREADY_FINALIZED");
        }
        write_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            RoundStatus::Aborted as u8,
            gas_meter,
        )?;
        Ok(())
    }

    /// Finalize round: set roots, checkpoint and create a new version.
    pub fn finalize_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        aggregated_commit: [u8; 32],
        checkpoint_uri_hash: [u8; 32],
        accepted_root: [u8; 32],
        score_root: [u8; 32],
        timestamp: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        ensure_creator_or_aggregator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let round_slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        if status != RoundStatus::Sealed as u8 {
            anyhow::bail!("ERR_ROUND_NOT_SEALED");
        }

        // min updates check
        let min_updates = read_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_MIN_UPDATES,
            gas_meter.as_deref_mut(),
        )?;
        let update_count = read_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_UPDATE_COUNT,
            gas_meter.as_deref_mut(),
        )?;
        if update_count < min_updates {
            anyhow::bail!("ERR_MIN_UPDATES_NOT_REACHED");
        }

        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_AGGREGATED_COMMIT,
            &aggregated_commit,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_ACCEPTED_ROOT,
            &accepted_root,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_SCORE_ROOT,
            &score_root,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_CHECKPOINT_URI,
            &checkpoint_uri_hash,
            gas_meter.as_deref_mut(),
        )?;

        write_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            RoundStatus::Finalized as u8,
            gas_meter.as_deref_mut(),
        )?;

        // create new version
        let model_base = model_slot(model_id);
        let current_version = read_u64(
            contract_storage,
            storage,
            model_base + MODEL_OFFSET_CURRENT_VERSION,
            gas_meter.as_deref_mut(),
        )?;
        let new_version = current_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("version overflow"))?;
        write_u64(
            contract_storage,
            storage,
            model_base + MODEL_OFFSET_CURRENT_VERSION,
            new_version,
            gas_meter.as_deref_mut(),
        )?;

        create_version_internal(
            contract_storage,
            storage,
            model_id,
            current_version,
            aggregated_commit,
            checkpoint_uri_hash,
            round_id,
            caller,
            timestamp,
            gas_meter,
        )?;

        Ok(new_version)
    }

    pub fn submit_update(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        update_commit: [u8; 32],
        update_uri_hash: [u8; 32],
        nonce: u64,
        signature: &[u8],
        timestamp: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_address32(caller)?;
        ensure_trainer_allowed(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        if signature.len() != 64 {
            anyhow::bail!("ERR_SIGNATURE_INVALID");
        }

        let round_slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        if status != RoundStatus::Open as u8 {
            anyhow::bail!("ERR_ROUND_NOT_OPEN");
        }

        let update_slot = update_slot(model_id, round_id, caller);
        let existing_commit = read_bytes32(
            contract_storage,
            storage,
            update_slot + UPDATE_OFFSET_COMMIT,
            gas_meter.as_deref_mut(),
        )?;
        if existing_commit != [0u8; 32] {
            anyhow::bail!("ERR_ALREADY_SUBMITTED");
        }

        let nonce_slot = mapping_slot_double(&model_id.to_le_bytes(), caller, SLOT_NONCES_BASE);
        let expected_nonce = read_u64(
            contract_storage,
            storage,
            nonce_slot,
            gas_meter.as_deref_mut(),
        )?;
        if nonce != expected_nonce {
            anyhow::bail!("ERR_NONCE_MISMATCH");
        }

        write_bytes32(
            contract_storage,
            storage,
            update_slot + UPDATE_OFFSET_COMMIT,
            &update_commit,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            update_slot + UPDATE_OFFSET_URI_HASH,
            &update_uri_hash,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            update_slot + UPDATE_OFFSET_NONCE,
            nonce,
            gas_meter.as_deref_mut(),
        )?;
        write_u64(
            contract_storage,
            storage,
            update_slot + UPDATE_OFFSET_TIMESTAMP,
            timestamp,
            gas_meter.as_deref_mut(),
        )?;

        // incrementa nonce globale
        write_u64(
            contract_storage,
            storage,
            nonce_slot,
            nonce
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("nonce overflow"))?,
            gas_meter.as_deref_mut(),
        )?;

        // incrementa update_count
        let update_count = read_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_UPDATE_COUNT,
            gas_meter.as_deref_mut(),
        )?;
        let max_updates = read_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_MAX_UPDATES,
            gas_meter.as_deref_mut(),
        )?;
        if update_count >= max_updates {
            anyhow::bail!("ERR_MAX_UPDATES_REACHED");
        }
        write_u32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_UPDATE_COUNT,
            update_count + 1,
            gas_meter,
        )?;

        Ok(())
    }

    /// Set accepted_root e score_root per un round.
    pub fn set_roots(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        accepted_root: [u8; 32],
        score_root: [u8; 32],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator_or_aggregator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let round_slot = round_slot(model_id, round_id);
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_ACCEPTED_ROOT,
            &accepted_root,
            gas_meter.as_deref_mut(),
        )?;
        write_bytes32(
            contract_storage,
            storage,
            round_slot + ROUND_OFFSET_SCORE_ROOT,
            &score_root,
            gas_meter,
        )?;
        Ok(())
    }

    /// Marca claimed per contributor/asset.
    pub fn mark_claimed(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        round_id: u64,
        asset_id: [u8; 32],
        contributor: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let slot = claim_slot(model_id, round_id, &asset_id, contributor);
        write_bool(contract_storage, storage, slot, true, gas_meter)
    }

    /// Fund pool interna (accounting locale).
    pub fn fund_pool(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        asset_id: [u8; 32],
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        ensure_creator(
            contract_storage,
            storage,
            caller,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        let slot = pool_slot(model_id, &asset_id);
        let current = read_u128(contract_storage, storage, slot, gas_meter.as_deref_mut())?;
        let new_amount = current
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("pool overflow"))?;
        write_u128(contract_storage, storage, slot, new_amount, gas_meter)?;
        Ok(())
    }

    /// Claim reward con check Merkle semplificata e fee treasury.
    #[allow(clippy::too_many_arguments)]
    pub fn claim_reward(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        caller: &[u8],
        model_id: u64,
        round_id: u64,
        asset_id: [u8; 32],
        gross_amount: u128,
        score: u128,
        accepted_proof: Vec<u8>,
        score_proof: Vec<u8>,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<(u128, u128)> {
        ensure_address32(caller)?;
        let round = Self::get_round(
            contract_storage,
            storage,
            model_id,
            round_id,
            gas_meter.as_deref_mut(),
        )?;
        if round.status != RoundStatus::Finalized {
            anyhow::bail!("ERR_ROUND_NOT_SEALED");
        }

        // check claimed
        let claim_slot = claim_slot(model_id, round_id, &asset_id, caller);
        let already_claimed = read_bool(
            contract_storage,
            storage,
            claim_slot,
            gas_meter.as_deref_mut(),
        )?;
        if already_claimed {
            anyhow::bail!("ERR_ALREADY_CLAIMED");
        }

        // verify accepted proof (leaf = keccak(model|round|caller|commit))
        let update = Self::get_update(
            contract_storage,
            storage,
            model_id,
            round_id,
            caller,
            gas_meter.as_deref_mut(),
        )?
        .ok_or_else(|| anyhow::anyhow!("ERR_INVALID_ROOT_PROOF"))?;
        let accepted_leaf = keccak_concat(&[
            &model_id.to_le_bytes(),
            &round_id.to_le_bytes(),
            caller,
            &update.commit,
        ]);
        let accepted_ok = verify_merkle_proof(
            &accepted_leaf,
            &round.accepted_root,
            bytes_to_nodes(&accepted_proof)?,
        );
        if !accepted_ok {
            anyhow::bail!("ERR_INVALID_ROOT_PROOF");
        }

        // verify score proof (leaf = keccak(model|round|caller|score|gross))
        let mut score_bytes = [0u8; 16];
        score_bytes.copy_from_slice(&score.to_le_bytes());
        let mut gross_bytes = [0u8; 16];
        gross_bytes.copy_from_slice(&gross_amount.to_le_bytes());
        let score_leaf = keccak_concat(&[
            &model_id.to_le_bytes(),
            &round_id.to_le_bytes(),
            caller,
            &score_bytes,
            &gross_bytes,
        ]);
        let score_ok = verify_merkle_proof(
            &score_leaf,
            &round.score_root,
            bytes_to_nodes(&score_proof)?,
        );
        if !score_ok {
            anyhow::bail!("ERR_INVALID_ROOT_PROOF");
        }

        let (treasury, fee_bps) =
            Self::get_treasury(contract_storage, storage, gas_meter.as_deref_mut())?;

        let fee = (gross_amount)
            .checked_mul(fee_bps as u128)
            .ok_or_else(|| anyhow::anyhow!("fee overflow"))?
            / 10_000u128;
        let net = gross_amount
            .checked_sub(fee)
            .ok_or_else(|| anyhow::anyhow!("fee exceeds gross"))?;

        let pool_slot = pool_slot(model_id, &asset_id);
        let balance = read_u128(
            contract_storage,
            storage,
            pool_slot,
            gas_meter.as_deref_mut(),
        )?;
        if balance < gross_amount {
            anyhow::bail!("ERR_POOL_INSUFFICIENT_FUNDS");
        }
        write_u128(
            contract_storage,
            storage,
            pool_slot,
            balance - gross_amount,
            gas_meter.as_deref_mut(),
        )?;

        // mark claimed
        write_bool(
            contract_storage,
            storage,
            claim_slot,
            true,
            gas_meter.as_deref_mut(),
        )?;

        // emette evento custom per payout
        emit_event(
            storage,
            EventSystem::new(),
            CustomEvent {
                contract_address: hex::encode(contract_storage.contract_address()),
                event_name: "RewardClaimed".into(),
                topics: vec![],
                data: Vec::new(),
            },
            gas_meter,
        );

        // (trasferimenti effettivi via adapter sarebbero gestiti altrove; qui contabilizziamo)
        let _ = treasury; // placate lint; trasferimento off-chain/non implementato

        Ok((net, fee))
    }

    pub fn get_model(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<ModelView> {
        let base = model_slot(model_id);
        let creator = read_bytes32(
            contract_storage,
            storage,
            base + MODEL_OFFSET_CREATOR,
            gas_meter.as_deref_mut(),
        )?;
        if creator == [0u8; 32] {
            anyhow::bail!("ERR_INVALID_MODEL");
        }
        Ok(ModelView {
            creator,
            nft_contract: read_bytes32(
                contract_storage,
                storage,
                base + MODEL_OFFSET_NFT_CONTRACT,
                gas_meter.as_deref_mut(),
            )?,
            token_class: read_u128(
                contract_storage,
                storage,
                base + MODEL_OFFSET_TOKEN_CLASS,
                gas_meter.as_deref_mut(),
            )?,
            current_version: read_u64(
                contract_storage,
                storage,
                base + MODEL_OFFSET_CURRENT_VERSION,
                gas_meter.as_deref_mut(),
            )?,
            metadata_hash: read_bytes32(
                contract_storage,
                storage,
                base + MODEL_OFFSET_METADATA,
                gas_meter.as_deref_mut(),
            )?,
            license_hash: read_bytes32(
                contract_storage,
                storage,
                base + MODEL_OFFSET_LICENSE,
                gas_meter.as_deref_mut(),
            )?,
            access_policy_hash: read_bytes32(
                contract_storage,
                storage,
                base + MODEL_OFFSET_ACCESS_POLICY,
                gas_meter.as_deref_mut(),
            )?,
            reward_policy_hash: read_bytes32(
                contract_storage,
                storage,
                base + MODEL_OFFSET_REWARD_POLICY,
                gas_meter.as_deref_mut(),
            )?,
            next_round_id: read_u64(
                contract_storage,
                storage,
                base + MODEL_OFFSET_NEXT_ROUND,
                gas_meter.as_deref_mut(),
            )?,
        })
    }

    /// Version view.
    pub fn get_version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        version_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<VersionView> {
        let slot = version_slot(model_id, version_id);
        let parent = read_u64(
            contract_storage,
            storage,
            slot + VERSION_OFFSET_PARENT,
            gas_meter.as_deref_mut(),
        )?;
        let weights_commit = read_bytes32(
            contract_storage,
            storage,
            slot + VERSION_OFFSET_WEIGHTS_COMMIT,
            gas_meter.as_deref_mut(),
        )?;
        // Version exists if weights_commit is non-zero (version 0 is genesis, version 1+ have parent 0 which is valid)
        if weights_commit == [0u8; 32] {
            anyhow::bail!("ERR_INVALID_MODEL");
        }
        Ok(VersionView {
            parent_version: parent,
            weights_commit: read_bytes32(
                contract_storage,
                storage,
                slot + VERSION_OFFSET_WEIGHTS_COMMIT,
                gas_meter.as_deref_mut(),
            )?,
            uri_hash: read_bytes32(
                contract_storage,
                storage,
                slot + VERSION_OFFSET_URI_HASH,
                gas_meter.as_deref_mut(),
            )?,
            round_id: read_u64(
                contract_storage,
                storage,
                slot + VERSION_OFFSET_ROUND_ID,
                gas_meter.as_deref_mut(),
            )?,
            aggregator: read_bytes32(
                contract_storage,
                storage,
                slot + VERSION_OFFSET_AGGREGATOR,
                gas_meter.as_deref_mut(),
            )?,
            timestamp: read_u64(
                contract_storage,
                storage,
                slot + VERSION_OFFSET_TIMESTAMP,
                gas_meter.as_deref_mut(),
            )?,
        })
    }

    /// Round view.
    pub fn get_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        round_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<RoundView> {
        let slot = round_slot(model_id, round_id);
        let status = read_u8(
            contract_storage,
            storage,
            slot + ROUND_OFFSET_STATUS,
            gas_meter.as_deref_mut(),
        )?;
        // Check if round exists by verifying open_until is set (must be > 0 for valid rounds)
        let open_until = read_u64(
            contract_storage,
            storage,
            slot + ROUND_OFFSET_OPEN_UNTIL,
            gas_meter.as_deref_mut(),
        )?;
        if open_until == 0 && round_id != 0 {
            anyhow::bail!("ERR_INVALID_ROUND");
        }
        Ok(RoundView {
            status: match status {
                0 => RoundStatus::Planned,
                1 => RoundStatus::Open,
                2 => RoundStatus::Sealed,
                3 => RoundStatus::Finalized,
                4 => RoundStatus::Aborted,
                _ => RoundStatus::Planned,
            },
            base_version: read_u64(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_BASE_VERSION,
                gas_meter.as_deref_mut(),
            )?,
            open_from: read_u64(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_OPEN_FROM,
                gas_meter.as_deref_mut(),
            )?,
            open_until: read_u64(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_OPEN_UNTIL,
                gas_meter.as_deref_mut(),
            )?,
            min_updates: read_u32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_MIN_UPDATES,
                gas_meter.as_deref_mut(),
            )?,
            max_updates: read_u32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_MAX_UPDATES,
                gas_meter.as_deref_mut(),
            )?,
            update_count: read_u32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_UPDATE_COUNT,
                gas_meter.as_deref_mut(),
            )?,
            aggregated_commit: read_bytes32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_AGGREGATED_COMMIT,
                gas_meter.as_deref_mut(),
            )?,
            accepted_root: read_bytes32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_ACCEPTED_ROOT,
                gas_meter.as_deref_mut(),
            )?,
            score_root: read_bytes32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_SCORE_ROOT,
                gas_meter.as_deref_mut(),
            )?,
            checkpoint_uri_hash: read_bytes32(
                contract_storage,
                storage,
                slot + ROUND_OFFSET_CHECKPOINT_URI,
                gas_meter,
            )?,
        })
    }

    /// Update view (opzionale).
    pub fn get_update(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        round_id: u64,
        contributor: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<UpdateView>> {
        ensure_address32(contributor)?;
        let slot = update_slot(model_id, round_id, contributor);
        let commit = read_bytes32(
            contract_storage,
            storage,
            slot + UPDATE_OFFSET_COMMIT,
            gas_meter.as_deref_mut(),
        )?;
        if commit == [0u8; 32] {
            return Ok(None);
        }
        Ok(Some(UpdateView {
            commit,
            uri_hash: read_bytes32(
                contract_storage,
                storage,
                slot + UPDATE_OFFSET_URI_HASH,
                gas_meter.as_deref_mut(),
            )?,
            nonce: read_u64(
                contract_storage,
                storage,
                slot + UPDATE_OFFSET_NONCE,
                gas_meter.as_deref_mut(),
            )?,
            timestamp: read_u64(
                contract_storage,
                storage,
                slot + UPDATE_OFFSET_TIMESTAMP,
                gas_meter,
            )?,
        }))
    }

    pub fn get_nonce(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        contributor: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let slot = mapping_slot_double(&model_id.to_le_bytes(), contributor, SLOT_NONCES_BASE);
        read_u64(contract_storage, storage, slot, gas_meter)
    }

    /// Check ruolo.
    pub fn has_role(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        account: &[u8],
        role: Role,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(account)?;
        let slot = role_slot(model_id, account, role);
        read_bool(contract_storage, storage, slot, gas_meter)
    }

    /// Check allowlist.
    pub fn is_allowlisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        account: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(account)?;
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, SLOT_ALLOWLIST_BASE);
        read_bool(contract_storage, storage, slot, gas_meter)
    }

    /// Check denylist.
    pub fn is_denylisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        account: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(account)?;
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, SLOT_DENYLIST_BASE);
        read_bool(contract_storage, storage, slot, gas_meter)
    }

    /// Check claimed.
    pub fn is_claimed(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        round_id: u64,
        asset_id: [u8; 32],
        contributor: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(contributor)?;
        let slot = claim_slot(model_id, round_id, &asset_id, contributor);
        read_bool(contract_storage, storage, slot, gas_meter)
    }
}

// =========================
// Helpers (esportati per uso nei moduli)
// =========================

/// Compute slot per mapping singolo: keccak256(key || base_slot)[0..8] -> u64
pub fn mapping_slot_single(key: &[u8], base_slot: u64) -> u64 {
    let mut h = Keccak256::new();
    h.update(key);
    h.update(&base_slot.to_le_bytes());
    let out = h.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&out[..8]);
    u64::from_le_bytes(bytes)
}

/// Compute slot per mapping doppio
pub fn mapping_slot_double(k1: &[u8], k2: &[u8], base: u64) -> u64 {
    let intermediate = mapping_slot_single(k1, base);
    mapping_slot_single(k2, intermediate)
}

/// Compute slot per mapping triplo
pub fn mapping_slot_triple(k1: &[u8], k2: &[u8], k3: &[u8], base: u64) -> u64 {
    let first = mapping_slot_single(k1, base);
    let second = mapping_slot_single(k2, first);
    mapping_slot_single(k3, second)
}

/// Compute slot base per modello
pub fn model_slot(model_id: u64) -> u64 {
    mapping_slot_single(&model_id.to_le_bytes(), SLOT_MODELS_BASE)
}

/// Compute slot per versione
pub fn version_slot(model_id: u64, version_id: u64) -> u64 {
    mapping_slot_double(
        &model_id.to_le_bytes(),
        &version_id.to_le_bytes(),
        SLOT_VERSIONS_BASE,
    )
}

/// Compute slot per round
pub fn round_slot(model_id: u64, round_id: u64) -> u64 {
    mapping_slot_double(
        &model_id.to_le_bytes(),
        &round_id.to_le_bytes(),
        SLOT_ROUNDS_BASE,
    )
}

/// Compute slot per update
pub fn update_slot(model_id: u64, round_id: u64, contributor: &[u8]) -> u64 {
    mapping_slot_triple(
        &model_id.to_le_bytes(),
        &round_id.to_le_bytes(),
        contributor,
        SLOT_UPDATES_BASE,
    )
}

fn role_slot(model_id: u64, account: &[u8], role: Role) -> u64 {
    mapping_slot_triple(
        &model_id.to_le_bytes(),
        account,
        &(role as u8).to_le_bytes(),
        SLOT_ROLES_BASE,
    )
}

fn claim_slot(model_id: u64, round_id: u64, asset_id: &[u8; 32], contributor: &[u8]) -> u64 {
    let first = mapping_slot_triple(
        &model_id.to_le_bytes(),
        &round_id.to_le_bytes(),
        asset_id,
        SLOT_CLAIM_BASE,
    );
    mapping_slot_single(contributor, first)
}

fn pool_slot(model_id: u64, asset_id: &[u8; 32]) -> u64 {
    mapping_slot_double(&model_id.to_le_bytes(), asset_id, SLOT_POOL_BASE)
}

/// Check che l'address sia 32 bytes
pub fn ensure_address32(addr: &[u8]) -> Result<()> {
    if addr.len() != 32 {
        anyhow::bail!("address must be 32 bytes");
    }
    Ok(())
}

fn ensure_creator(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    caller: &[u8],
    model_id: u64,
    mut gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let model = FlAiManager::get_model(
        contract_storage,
        storage,
        model_id,
        gas_meter.as_deref_mut(),
    )?;
    if caller != model.creator {
        anyhow::bail!("ERR_UNAUTHORIZED");
    }
    Ok(())
}

fn ensure_creator_or_aggregator(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    caller: &[u8],
    model_id: u64,
    mut gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    if ensure_creator(
        contract_storage,
        storage,
        caller,
        model_id,
        gas_meter.as_deref_mut(),
    )
    .is_ok()
    {
        return Ok(());
    }
    let has_role = FlAiManager::has_role(
        contract_storage,
        storage,
        model_id,
        caller,
        Role::Aggregator,
        gas_meter,
    )?;
    if !has_role {
        anyhow::bail!("ERR_UNAUTHORIZED");
    }
    Ok(())
}

fn ensure_trainer_allowed(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    caller: &[u8],
    model_id: u64,
    mut gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let deny = FlAiManager::is_denylisted(
        contract_storage,
        storage,
        model_id,
        caller,
        gas_meter.as_deref_mut(),
    )?;
    if deny {
        anyhow::bail!("ERR_UNAUTHORIZED");
    }
    let allow = FlAiManager::is_allowlisted(
        contract_storage,
        storage,
        model_id,
        caller,
        gas_meter.as_deref_mut(),
    )?;
    if !allow {
        let has_role = FlAiManager::has_role(
            contract_storage,
            storage,
            model_id,
            caller,
            Role::Trainer,
            gas_meter,
        )?;
        if !has_role {
            anyhow::bail!("ERR_UNAUTHORIZED");
        }
    }
    Ok(())
}

fn create_version_internal(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    model_id: u64,
    parent_version: u64,
    weights_commit: [u8; 32],
    uri_hash: [u8; 32],
    round_id: u64,
    aggregator: &[u8],
    timestamp: u64,
    mut gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let model_slot = model_slot(model_id);
    let current_version = read_u64(
        contract_storage,
        storage,
        model_slot + MODEL_OFFSET_CURRENT_VERSION,
        gas_meter.as_deref_mut(),
    )?;
    let slot = version_slot(model_id, current_version);
    write_u64(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_PARENT,
        parent_version,
        gas_meter.as_deref_mut(),
    )?;
    write_bytes32(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_WEIGHTS_COMMIT,
        &weights_commit,
        gas_meter.as_deref_mut(),
    )?;
    write_bytes32(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_URI_HASH,
        &uri_hash,
        gas_meter.as_deref_mut(),
    )?;
    write_u64(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_ROUND_ID,
        round_id,
        gas_meter.as_deref_mut(),
    )?;
    write_bytes32(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_AGGREGATOR,
        aggregator,
        gas_meter.as_deref_mut(),
    )?;
    write_u64(
        contract_storage,
        storage,
        slot + VERSION_OFFSET_TIMESTAMP,
        timestamp,
        gas_meter,
    )?;
    Ok(())
}

fn validate_fee_bps(fee_bps: u16) -> Result<()> {
    if fee_bps > 1000 {
        anyhow::bail!("ERR_FEE_CONFIG_INVALID");
    }
    Ok(())
}

fn keccak_concat(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Keccak256::new();
    for p in parts {
        h.update(p);
    }
    let out = h.finalize();
    let mut r = [0u8; 32];
    r.copy_from_slice(&out[..32]);
    r
}

fn bytes_to_nodes(proof: &[u8]) -> Result<Vec<[u8; 32]>> {
    if proof.is_empty() {
        return Ok(vec![]);
    }
    if proof.len() % 32 != 0 {
        anyhow::bail!("proof length invalid");
    }
    Ok(proof
        .chunks(32)
        .map(|c| {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(c);
            arr
        })
        .collect())
}

fn verify_merkle_proof(leaf: &[u8; 32], root: &[u8; 32], proof: Vec<[u8; 32]>) -> bool {
    if proof.is_empty() {
        return leaf == root;
    }
    let mut hash = *leaf;
    for sibling in proof {
        let mut data = Vec::with_capacity(64);
        if hash <= sibling {
            data.extend_from_slice(&hash);
            data.extend_from_slice(&sibling);
        } else {
            data.extend_from_slice(&sibling);
            data.extend_from_slice(&hash);
        }
        hash = keccak_concat(&[&data]);
    }
    &hash == root
}

/// Legge bytes32 dallo storage
pub fn read_bytes32(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<[u8; 32]> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    if v.len() != 32 {
        anyhow::bail!("invalid storage length");
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&v);
    Ok(arr)
}

/// Scrive bytes32 in the storage
pub fn write_bytes32(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: &[u8],
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    if value.len() != 32 {
        anyhow::bail!("value must be 32 bytes");
    }
    contract_storage.sstore(storage, slot, value.to_vec(), gas_meter)
}

/// Legge u64 dallo storage
pub fn read_u64(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<u64> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    let mut b = [0u8; 8];
    b.copy_from_slice(&v[..8]);
    Ok(u64::from_le_bytes(b))
}

/// Scrive u64 in the storage
pub fn write_u64(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let mut b = vec![0u8; 32];
    b[..8].copy_from_slice(&value.to_le_bytes());
    contract_storage.sstore(storage, slot, b, gas_meter)
}

/// Legge u32 dallo storage
pub fn read_u32(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<u32> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    let mut b = [0u8; 4];
    b.copy_from_slice(&v[..4]);
    Ok(u32::from_le_bytes(b))
}

/// Scrive u32 in the storage
pub fn write_u32(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: u32,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let mut b = vec![0u8; 32];
    b[..4].copy_from_slice(&value.to_le_bytes());
    contract_storage.sstore(storage, slot, b, gas_meter)
}

/// Legge u16 dallo storage
pub fn read_u16(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<u16> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    let mut b = [0u8; 2];
    b.copy_from_slice(&v[..2]);
    Ok(u16::from_le_bytes(b))
}

/// Scrive u16 in the storage
pub fn write_u16(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: u16,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let mut b = vec![0u8; 32];
    b[..2].copy_from_slice(&value.to_le_bytes());
    contract_storage.sstore(storage, slot, b, gas_meter)
}

/// Legge u8 dallo storage
pub fn read_u8(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<u8> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    Ok(v[0])
}

/// Scrive u8 in the storage
pub fn write_u8(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: u8,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let mut b = vec![0u8; 32];
    b[0] = value;
    contract_storage.sstore(storage, slot, b, gas_meter)
}

/// Legge bool dallo storage
pub fn read_bool(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<bool> {
    Ok(read_u8(contract_storage, storage, slot, gas_meter)? != 0)
}

/// Scrive bool in the storage
pub fn write_bool(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: bool,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    write_u8(
        contract_storage,
        storage,
        slot,
        if value { 1 } else { 0 },
        gas_meter,
    )
}

/// Legge u128 dallo storage
pub fn read_u128(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    gas_meter: Option<&mut GasMeter>,
) -> Result<u128> {
    let v = contract_storage.sload(storage, slot, gas_meter)?;
    let mut b = [0u8; 16];
    b.copy_from_slice(&v[..16]);
    Ok(u128::from_le_bytes(b))
}

/// Scrive u128 in the storage
pub fn write_u128(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    slot: u64,
    value: u128,
    gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let mut b = vec![0u8; 32];
    b[..16].copy_from_slice(&value.to_le_bytes());
    contract_storage.sstore(storage, slot, b, gas_meter)
}

/// Emette evento custom
pub fn emit_event(
    storage: &Storage,
    event_system: EventSystem,
    event: CustomEvent,
    gas: Option<&mut GasMeter>,
) {
    let _ = storage;
    event_system.emit_custom_event(event, gas);
}

// =========================
// Tests
// =========================
