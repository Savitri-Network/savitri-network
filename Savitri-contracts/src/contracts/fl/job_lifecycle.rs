//! Job Lifecycle: Contratto per gestione round FL (open, submit updates, finalize, claim rewards)
//!
//! Implementa:
//! - Round lifecycle (create, open, seal, finalize, abort)
//! - Update submissions con replay protection (nonce)
//! - Reward claims con Merkle proof verification
//! - Governance hooks integration (AbortFlRound)

use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use anyhow::Result;
use hex;
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};

use super::{
    ensure_address32, mapping_slot_double, model_registry::ModelRegistry, model_slot, read_bool,
    read_bytes32, read_u128, read_u32, read_u64, read_u8, round_slot, update_slot, version_slot,
    write_bool, write_bytes32, write_u128, write_u32, write_u64, write_u8, Role, RoundStatus,
    RoundView, UpdateView, MODEL_OFFSET_CURRENT_VERSION, MODEL_OFFSET_NEXT_ROUND,
    ROUND_OFFSET_ACCEPTED_ROOT, ROUND_OFFSET_AGGREGATED_COMMIT, ROUND_OFFSET_BASE_VERSION,
    ROUND_OFFSET_CHECKPOINT_URI, ROUND_OFFSET_MAX_UPDATES, ROUND_OFFSET_MIN_UPDATES,
    ROUND_OFFSET_OPEN_FROM, ROUND_OFFSET_OPEN_UNTIL, ROUND_OFFSET_SCORE_ROOT, ROUND_OFFSET_STATUS,
    ROUND_OFFSET_UPDATE_COUNT, SLOT_CLAIM_BASE, SLOT_NONCES_BASE, SLOT_POOL_BASE,
    UPDATE_OFFSET_COMMIT, UPDATE_OFFSET_NONCE, UPDATE_OFFSET_TIMESTAMP, UPDATE_OFFSET_URI_HASH,
    VERSION_OFFSET_AGGREGATOR, VERSION_OFFSET_PARENT, VERSION_OFFSET_ROUND_ID,
    VERSION_OFFSET_TIMESTAMP, VERSION_OFFSET_URI_HASH, VERSION_OFFSET_WEIGHTS_COMMIT,
};

/// Job Lifecycle Contract
pub struct JobLifecycle;

impl JobLifecycle {
    /// Creates round in stato Planned, incrementando round_id interno.
    ///
    /// # ABI
    /// `createRound(uint64 modelId, uint64 baseVersion, uint64 openFrom, uint64 openUntil, uint32 minUpdates, uint32 maxUpdates) -> (uint64 roundId)`
    ///
    /// # Auth
    /// Creator o aggregator autorizzato.
    ///
    /// # Effetti
    /// - Creates round in stato `Planned`
    /// - Incrementa `next_round_id` of the modello
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
        let model = ModelRegistry::get_model(
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

        super::emit_event(
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
    ///
    /// # ABI
    /// `openRound(uint64 modelId, uint64 roundId) -> ()`
    ///
    /// # Auth
    /// Creator/aggregator; oppure auto-open se `now >= openFrom`.
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
    ///
    /// # ABI
    /// `sealRound(uint64 modelId, uint64 roundId) -> ()`
    ///
    /// # Auth
    /// Creator/aggregator; oppure auto-seal se `now > openUntil` o `updateCount >= maxUpdates`.
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
    ///
    /// # ABI
    /// `abortRound(uint64 modelId, uint64 roundId, bytes32 reasonHash) -> ()`
    ///
    /// # Auth
    /// Creator o governance (via AbortFlRound).
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
    ///
    /// # ABI
    /// `finalizeRound(uint64 modelId, uint64 roundId, bytes32 aggregatedCommit, bytes32 checkpointUriHash, bytes32 acceptedRoot, bytes32 scoreRoot) -> (uint64 newVersionId)`
    ///
    /// # Auth
    /// Aggregator (role) o creator.
    ///
    /// # Precond
    /// Round `Sealed`.
    ///
    /// # Effetti
    /// - Creates nuova version
    /// - Set roots
    /// - Status `Finalized`
    #[allow(clippy::too_many_arguments)]
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

    ///
    /// # ABI
    /// `submitUpdate(uint64 modelId, uint64 roundId, bytes32 updateCommit, bytes32 updateUriHash, uint64 nonce, bytes signature) -> ()`
    ///
    /// # Auth
    /// Trainer autorizzato.
    ///
    /// # Precond
    /// - Round `Open`
    /// - `nonce == getNonce(modelId, caller)`
    ///
    /// # Effetti
    /// - Salva commit e incrementa nonce
    /// - Incrementa `updateCount`
    #[allow(clippy::too_many_arguments)]
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

    /// Claim reward con check Merkle semplificata e fee treasury.
    ///
    /// # ABI
    /// `claimReward(uint64 modelId, uint64 roundId, bytes32 assetId, uint128 grossAmount, uint128 score, bytes acceptedProof, bytes scoreProof) -> (uint128 netAmount, uint128 feeAmount)`
    ///
    /// # Auth
    /// Contributor.
    ///
    /// # Precond
    /// - Round `Finalized`
    /// - `acceptedProof` dimostra che il contributor è nell'accepted set
    /// - `scoreProof` dimostra lo score
    /// - Non già claimed
    ///
    /// # Effetti
    /// - Transfers `net` al contributor, fee a treasury
    /// - Marca claimed
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
            super::FlAiManager::get_treasury(contract_storage, storage, gas_meter.as_deref_mut())?;

        // Fixed-point arithmetic: fee = (gross * fee_bps) / 10000
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
        super::emit_event(
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

    /// Round view.
    ///
    /// # ABI
    /// `getRound(uint64 modelId, uint64 roundId) -> (uint8 status, uint64 baseVersion, uint64 openFrom, uint64 openUntil, uint32 minUpdates, uint32 maxUpdates, uint32 updateCount, bytes32 aggregatedCommit, bytes32 acceptedRoot, bytes32 scoreRoot, bytes32 checkpointUriHash)`
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
    ///
    /// # ABI
    /// `getUpdate(uint64 modelId, uint64 roundId, address contributor) -> (bytes32 commit, bytes32 uriHash, uint64 nonce, uint64 timestamp)`
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

    ///
    /// # ABI
    /// `getNonce(uint64 modelId, address contributor) -> (uint64 nextNonce)`
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

    /// Check claimed.
    ///
    /// # ABI
    /// `isClaimed(uint64 modelId, uint64 roundId, bytes32 assetId, address contributor) -> (bool)`
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

    /// Fund pool interna (accounting locale).
    ///
    /// # ABI
    /// `fundPool(uint64 modelId, bytes32 assetId, uint128 amount) -> ()`
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

    /// Gestisce governance hook per AbortFlRound.
    ///
    /// per abortire il round specificato.
    pub fn on_governance_abort_round(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        round_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Usa un address fittizio per il caller (governance)
        let governance_caller = [0xFFu8; 32];
        Self::abort_round(
            contract_storage,
            storage,
            &governance_caller,
            model_id,
            round_id,
            gas_meter,
        )
    }
}

// =========================
// Helpers interni
// =========================

fn ensure_creator(
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    caller: &[u8],
    model_id: u64,
    mut gas_meter: Option<&mut GasMeter>,
) -> Result<()> {
    let model = ModelRegistry::get_model(
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
    let has_role = ModelRegistry::has_role(
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
    let deny = ModelRegistry::is_denylisted(
        contract_storage,
        storage,
        model_id,
        caller,
        gas_meter.as_deref_mut(),
    )?;
    if deny {
        anyhow::bail!("ERR_UNAUTHORIZED");
    }
    let allow = ModelRegistry::is_allowlisted(
        contract_storage,
        storage,
        model_id,
        caller,
        gas_meter.as_deref_mut(),
    )?;
    if !allow {
        let has_role = ModelRegistry::has_role(
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

fn claim_slot(model_id: u64, round_id: u64, asset_id: &[u8; 32], contributor: &[u8]) -> u64 {
    let first = super::mapping_slot_triple(
        &model_id.to_le_bytes(),
        &round_id.to_le_bytes(),
        asset_id,
        SLOT_CLAIM_BASE,
    );
    super::mapping_slot_single(contributor, first)
}

fn pool_slot(model_id: u64, asset_id: &[u8; 32]) -> u64 {
    mapping_slot_double(&model_id.to_le_bytes(), asset_id, SLOT_POOL_BASE)
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
