//! Model Registry: Contratto per registrazione modelli, versioning e metadata
//!
//! Implementa:
//! - Registrazione modelli con metadata (metadata_uri, license_uri, weights_uri, owner)
//! - Versioning immutabile (catena versioni per modello)
//! - Access control (RBAC: Creator, Viewer, Trainer, Aggregator)
//! - Policy management (access_policy_hash, reward_policy_hash)
//! - Governance hooks integration (ApproveFlModel, SetFlPolicy)

use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::Result;

use super::{
    ensure_address32, mapping_slot_double, mapping_slot_single, mapping_slot_triple, model_slot,
    read_bool, read_bytes32, read_u128, read_u64, role_slot, version_slot, write_bool,
    write_bytes32, write_u128, write_u64, ModelView, Role, VersionView, SLOT_MODELS_BASE,
    SLOT_MODEL_COUNT, SLOT_ROLES_BASE, VERSION_OFFSET_AGGREGATOR, VERSION_OFFSET_PARENT,
    VERSION_OFFSET_ROUND_ID, VERSION_OFFSET_TIMESTAMP, VERSION_OFFSET_URI_HASH,
    VERSION_OFFSET_WEIGHTS_COMMIT,
};

/// Offsets per Model (slot base calcolato per model_id)
pub const MODEL_OFFSET_CREATOR: u64 = 0;
pub const MODEL_OFFSET_NFT_CONTRACT: u64 = 1;
pub const MODEL_OFFSET_TOKEN_CLASS: u64 = 2;
pub const MODEL_OFFSET_CURRENT_VERSION: u64 = 3;
pub const MODEL_OFFSET_METADATA: u64 = 4;
pub const MODEL_OFFSET_LICENSE: u64 = 5;
pub const MODEL_OFFSET_ACCESS_POLICY: u64 = 6;
pub const MODEL_OFFSET_REWARD_POLICY: u64 = 7;
pub const MODEL_OFFSET_NEXT_ROUND: u64 = 8;

/// Model Registry Contract
pub struct ModelRegistry;

impl ModelRegistry {
    ///
    /// # ABI
    /// `registerModel(address nftContract, uint128 tokenClass, bytes32 metadataHash, bytes32 licenseHash, bytes32 initialWeightsCommit, bytes32 initialCheckpointUriHash) -> (uint64 modelId)`
    ///
    /// # Auth
    /// Caller diventa creator automaticamente.
    ///
    /// # Effetti
    /// - Incrementa `model_count`
    /// - Creates `version 0` (genesis)
    /// - Set ruolo Creator al caller
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

    ///
    /// # ABI
    /// `setModelMetadata(uint64 modelId, bytes32 metadataHash, bytes32 licenseHash) -> ()`
    ///
    /// # Auth
    /// Creator o governance (via ApproveFlModel).
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

    ///
    /// # ABI
    /// `setAccessPolicyHash(uint64 modelId, bytes32 policyHash) -> ()`
    ///
    /// # Auth
    /// Creator o governance (via SetFlPolicy).
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

    ///
    /// # ABI
    /// `setRewardPolicyHash(uint64 modelId, bytes32 rewardPolicyHash) -> ()`
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

    /// Concede ruolo a un account.
    ///
    /// # ABI
    /// `grantRole(uint64 modelId, address account, uint8 role) -> ()`
    ///
    /// # Auth
    /// Creator.
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

    /// Revoke ruolo a un account.
    ///
    /// # ABI
    /// `revokeRole(uint64 modelId, address account, uint8 role) -> ()`
    ///
    /// # Auth
    /// Creator.
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

    /// Set allowlist per account.
    ///
    /// # ABI
    /// `setAllowlisted(uint64 modelId, address account, bool allowed) -> ()`
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
        let slot =
            mapping_slot_double(&model_id.to_le_bytes(), account, super::SLOT_ALLOWLIST_BASE);
        write_bool(contract_storage, storage, slot, allowed, gas_meter)?;
        Ok(())
    }

    /// Set denylist per account.
    ///
    /// # ABI
    /// `setDenylisted(uint64 modelId, address account, bool denied) -> ()`
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
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, super::SLOT_DENYLIST_BASE);
        write_bool(contract_storage, storage, slot, denied, gas_meter)?;
        Ok(())
    }

    ///
    /// # ABI
    /// `getModel(uint64 modelId) -> (address creator, address nftContract, uint128 tokenClass, uint64 currentVersion, bytes32 metadataHash, bytes32 licenseHash)`
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
                gas_meter,
            )?,
        })
    }

    /// Version view.
    ///
    /// # ABI
    /// `getVersion(uint64 modelId, uint64 versionId) -> (uint64 parentVersion, bytes32 weightsCommit, bytes32 uriHash, uint64 roundId, address aggregator, uint64 timestamp)`
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
                gas_meter,
            )?,
        })
    }

    /// Check ruolo.
    ///
    /// # ABI
    /// `hasRole(uint64 modelId, address account, uint8 role) -> (bool)`
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
    ///
    /// # ABI
    /// `isAllowlisted(uint64 modelId, address account) -> (bool)`
    pub fn is_allowlisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        account: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(account)?;
        let slot =
            mapping_slot_double(&model_id.to_le_bytes(), account, super::SLOT_ALLOWLIST_BASE);
        read_bool(contract_storage, storage, slot, gas_meter)
    }

    /// Check denylist.
    ///
    /// # ABI
    /// `isDenylisted(uint64 modelId, address account) -> (bool)`
    pub fn is_denylisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        account: &[u8],
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        ensure_address32(account)?;
        let slot = mapping_slot_double(&model_id.to_le_bytes(), account, super::SLOT_DENYLIST_BASE);
        read_bool(contract_storage, storage, slot, gas_meter)
    }

    /// Gestisce governance hook per ApproveFlModel.
    ///
    pub fn on_governance_approve_model(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        model_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let _model = Self::get_model(
            contract_storage,
            storage,
            model_id,
            gas_meter.as_deref_mut(),
        )?;
        // Qui si potrebbe aggiungere un flag "approved" o aggiornare policy
        // Per ora, l'approvazione è implicita in the presenza of the modello
        Ok(())
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
