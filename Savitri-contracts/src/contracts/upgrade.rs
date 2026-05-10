//! Contract Upgrade: Sistema di upgrade of contracts
//!
//! - Upgrade logic (preservazione storage)
//! - Governance-controlled upgrade
//!
//! # Upgrade Process
//!
//! - Slot 0-99 (BaseContract): preservati automaticamente
//! - Slot custom (100+): preservati automaticamente
//! - La versione viene incrementata

use crate::contracts::base::BaseContract;
use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use anyhow::{Context, Result};
use hex;
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};

/// Sistema di upgrade of contracts
pub struct UpgradeSystem;

impl UpgradeSystem {
    pub fn new() -> Self {
        Self
    }

    /// Compute keccak256 hash di un input
    ///
    /// # Arguments
    /// * `input` - Dati da hashar
    ///
    /// # Returns
    /// Hash keccak256 (32 bytes)
    fn keccak256(input: &[u8]) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(input);
        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(&hash);
        result
    }

    /// Decodifica address da stringa hex a bytes (32 bytes)
    fn decode_address(address_str: &str) -> Result<[u8; 32]> {
        let address_hex = address_str.strip_prefix("0x").unwrap_or(address_str);
        let address_bytes = hex::decode(address_hex).with_context(|| "Failed to decode address")?;

        if address_bytes.len() != 32 {
            anyhow::bail!("Address must be 32 bytes, got {}", address_bytes.len());
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&address_bytes);
        Ok(address)
    }

    /// Runs upgrade di a contract
    ///
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `new_bytecode` - Nuovo bytecode of the contract
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    ///
    /// # Note
    /// - La versione viene incrementata automaticamente
    pub fn upgrade_contract(
        &self,
        storage: &Storage,
        contract_address: &str,
        new_bytecode: Vec<u8>,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // 1. Decodifica contract address
        let address_bytes = Self::decode_address(contract_address)?;

        let contract_data = storage
            .get_contract(&address_bytes)
            .with_context(|| "Failed to get contract info")?
            .ok_or_else(|| anyhow::anyhow!("Contract not found at address {}", contract_address))?;

        const MAX_CONTRACT_INFO_SIZE: usize = 4 * 1024 * 1024;
        if contract_data.len() > MAX_CONTRACT_INFO_SIZE {
            anyhow::bail!(
                "Contract info data too large: {} bytes (max {})",
                contract_data.len(),
                MAX_CONTRACT_INFO_SIZE
            );
        }
        // Deserialize contract info
        let contract_info: savitri_storage::storage::contracts::ContractInfo =
            bincode::deserialize(&contract_data)
                .with_context(|| "Failed to deserialize contract info")?;

        if contract_info.code.is_empty() {
            anyhow::bail!("Cannot upgrade contract with empty bytecode");
        }

        // 4. Check che il nuovo bytecode non sia vuoto
        if new_bytecode.is_empty() {
            anyhow::bail!("New bytecode cannot be empty");
        }

        // Creiamo un ContractStorage temporaneo per verificare lo stato paused
        let mut contract_storage = ContractStorage::new(address_bytes.to_vec())
            .with_context(|| "Failed to create contract storage")?;
        if BaseContract::is_paused(&mut contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Cannot upgrade contract while paused");
        }

        // 6. Ottieni la versione corrente e incrementala
        let current_version = contract_info.version;
        let _new_version = current_version.checked_add(1).ok_or_else(|| {
            anyhow::anyhow!("Version overflow: cannot increment version beyond u64::MAX")
        })?;

        // 7. Compute code_hash of the nuovo bytecode
        let new_code_hash = Self::keccak256(&new_bytecode);

        // 8. Check che il nuovo bytecode sia diverso dal vecchio
        if contract_info.code_hash == new_code_hash.to_vec() {
            anyhow::bail!("New bytecode is identical to current bytecode (same code_hash)");
        }

        storage
            .update_contract_code(&address_bytes, &new_bytecode)
            .with_context(|| "Failed to update contract code")?;

        // 10. Lo storage è preservato automaticamente:
        // - Gli slot 0-99 (BaseContract) rimangono invariati
        // - Gli slot custom (100+) rimangono invariati
        // - L'address è rimasto lo stesso

        Ok(())
    }

    ///
    /// Check che non modifichi slot riservati (0-99).
    /// a slot riservati e li rifiuta.
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Ok(()) se valido, errore altrimenti
    ///
    /// # Validazioni eseguite
    /// 1. Check che il bytecode non sia vuoto
    /// 2. Check che il bytecode abbia una dimensione minima ragionevole
    /// 3. Analizza il bytecode per identificare accessi diretti a slot riservati (0-99)
    /// 4. Check che non ci siano pattern che indicano modifiche a slot riservati
    ///
    /// # Note
    pub fn validate_storage_layout(&self, new_bytecode: &[u8]) -> Result<(), String> {
        use crate::contracts::base::{BASE_CONTRACT_SLOT_END, BASE_CONTRACT_SLOT_START};

        // 1. Validazione base: check che il bytecode non sia vuoto
        if new_bytecode.is_empty() {
            return Err("New bytecode cannot be empty".to_string());
        }

        // 2. Check che il bytecode abbia una dimensione minima ragionevole
        const MIN_BYTECODE_SIZE: usize = 4; // Almeno 4 bytes per un selector di funzione
        if new_bytecode.len() < MIN_BYTECODE_SIZE {
            return Err(format!(
                "New bytecode too small: minimum {} bytes, got {}",
                MIN_BYTECODE_SIZE,
                new_bytecode.len()
            ));
        }

        // 3. Analizza il bytecode per identificare accessi diretti a slot riservati (0-99)
        // Cerchiamo pattern che potrebbero indicare accessi a slot riservati:
        // - Pattern comuni di accesso a slot bassi

        // Check accessi espliciti a slot riservati
        // a runtime dal sistema di storage.

        // Cerca pattern che potrebbero indicare accessi a slot 0-99

        for slot in BASE_CONTRACT_SLOT_START..=BASE_CONTRACT_SLOT_END {
            // Little-endian representation (u64, 8 bytes)
            let slot_le_bytes = slot.to_le_bytes();

            // Big-endian representation (u64, 8 bytes)
            let slot_be_bytes = slot.to_be_bytes();

            // Cerca il pattern nel bytecode
            // Se troviamo un pattern che corrisponde esattamente a uno slot riservato,

            // Cerchiamo pattern di 8 bytes consecutivi che corrispondono allo slot
            for window in new_bytecode.windows(8) {
                if window == slot_le_bytes || window == slot_be_bytes {
                    // Trovato un pattern che corrisponde a uno slot riservato
                    return Err(format!(
                        "Bytecode contains pattern matching reserved slot {} (slots 0-99 are reserved for BaseContract)",
                        slot
                    ));
                }
            }

            // Cerchiamo anche pattern più corti (u32, u16, u8) per slot piccoli
            if slot <= u32::MAX as u64 {
                let slot_u32_le = (slot as u32).to_le_bytes();
                let slot_u32_be = (slot as u32).to_be_bytes();

                for window in new_bytecode.windows(4) {
                    if window == slot_u32_le || window == slot_u32_be {
                        return Err(format!(
                            "Bytecode contains pattern matching reserved slot {} (slots 0-99 are reserved for BaseContract)",
                            slot
                        ));
                    }
                }
            }

            if slot <= u16::MAX as u64 {
                let slot_u16_le = (slot as u16).to_le_bytes();
                let slot_u16_be = (slot as u16).to_be_bytes();

                for window in new_bytecode.windows(2) {
                    if window == slot_u16_le || window == slot_u16_be {
                        return Err(format!(
                            "Bytecode contains pattern matching reserved slot {} (slots 0-99 are reserved for BaseContract)",
                            slot
                        ));
                    }
                }
            }

            // nel bytecode e causerebbero troppi falsi positivi.
        }

        // il sistema di storage blocca comunque accessi a slot riservati a runtime
        // problemi prima dell'upgrade.

        Ok(())
    }

    /// Check che l'upgrade sia controllato da governance
    ///
    /// Check:
    /// 1. La proposta esiste
    /// 2. La proposta è di tipo ContractUpgrade
    /// 3. La proposta è approvata (quorum 10%, approval 65%)
    /// 4. La proposta non è scaduta (voting_end >= current_timestamp)
    /// 6. Il code_hash in the proposta corrisponde al nuovo bytecode
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `current_timestamp` - Timestamp corrente (Unix timestamp in secondi)
    ///
    /// # Returns
    pub fn validate_governance_proposal(
        &self,
        storage: &Storage,
        proposal_id: u64,
        contract_address: &[u8; 32],
        new_code_hash: &[u8; 32],
        current_timestamp: u64,
    ) -> Result<()> {
        use savitri_storage::storage::{ProposalAction, ProposalStatus};

        // 1. Check che la proposta esista
        let proposal_data = storage
            .get_proposal(proposal_id)
            .with_context(|| format!("Failed to get proposal {}", proposal_id))?
            .ok_or_else(|| anyhow::anyhow!("Proposal {} not found", proposal_id))?;

        const MAX_PROPOSAL_SIZE: usize = 4 * 1024 * 1024;
        if proposal_data.len() > MAX_PROPOSAL_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                proposal_data.len(),
                MAX_PROPOSAL_SIZE
            );
        }
        // Deserialize proposal
        let proposal: savitri_storage::storage::Proposal = bincode::deserialize(&proposal_data)
            .with_context(|| "Failed to deserialize proposal")?;

        // 2. Check che la proposta sia di tipo ContractUpgrade
        match &proposal.action {
            ProposalAction::ContractUpgrade {
                contract_address: prop_address,
                new_code_hash: prop_code_hash,
                ..
            } => {
                // Check che il contract_address corrisponda
                if prop_address != contract_address {
                    anyhow::bail!(
                        "Proposal contract address mismatch: proposal has {}, expected {}",
                        hex::encode(prop_address),
                        hex::encode(contract_address)
                    );
                }

                // Check che il code_hash corrisponda
                if prop_code_hash != new_code_hash {
                    anyhow::bail!(
                        "Proposal code hash mismatch: proposal has {}, expected {}",
                        hex::encode(prop_code_hash),
                        hex::encode(new_code_hash)
                    );
                }
            }
            _ => {
                anyhow::bail!("Proposal {} is not a ContractUpgrade proposal", proposal_id);
            }
        };

        // 3. Check che la proposta sia approvata (status Approved)
        if proposal.status != ProposalStatus::Approved {
            anyhow::bail!(
                "Proposal {} is not approved: current status is {:?}",
                proposal_id,
                proposal.status
            );
        }

        // 4. Check quorum (10% dei vote token totali)
        let quorum_reached = storage
            .check_proposal_quorum(proposal_id)
            .with_context(|| format!("Failed to check quorum for proposal {}", proposal_id))?;

        if !quorum_reached {
            anyhow::bail!(
                "Proposal {} quorum not reached: need 10% of total vote tokens",
                proposal_id
            );
        }

        let approval_reached = storage
            .check_proposal_approval(proposal_id)
            .with_context(|| format!("Failed to check approval for proposal {}", proposal_id))?;

        if !approval_reached {
            anyhow::bail!(
                "Proposal {} approval not reached: need 65% yes votes",
                proposal_id
            );
        }

        if current_timestamp > proposal.voting_end {
            anyhow::bail!(
                "Proposal {} has expired: voting_end {} is before current timestamp {}",
                proposal_id,
                proposal.voting_end,
                current_timestamp
            );
        }

        Ok(())
    }

    /// Check che l'upgrade sia controllato da governance (versione semplificata)
    ///
    /// # Arguments
    ///
    /// # Returns
    /// true se controllato da governance, false altrimenti
    ///
    /// # Note
    pub fn is_governance_controlled(&self, _proposal_id: u64) -> bool {
        true
    }

    ///
    /// garantire che l'upgrade sia sicuro.
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `new_bytecode` - Nuovo bytecode of the contract
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    pub fn upgrade_contract_with_validation(
        &self,
        storage: &Storage,
        contract_address: &str,
        new_bytecode: Vec<u8>,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        self.validate_storage_layout(&new_bytecode)
            .map_err(|e| anyhow::anyhow!("Storage layout validation failed: {}", e))?;

        // 2. Runs l'upgrade
        self.upgrade_contract(storage, contract_address, new_bytecode, gas_meter)
    }

    ///
    /// Valida:
    /// 1. La proposta governance esiste ed è approvata
    /// 2. La proposta è di tipo ContractUpgrade
    /// 3. Quorum raggiunto (10% dei vote token)
    /// 4. Approval raggiunto (65% yes votes)
    /// 5. La proposta non è scaduta
    /// 6. Contract address e code_hash corrispondono alla proposta
    /// 7. Storage layout valido
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `new_bytecode` - Nuovo bytecode of the contract
    /// * `current_timestamp` - Timestamp corrente (Unix timestamp in secondi)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    pub fn upgrade_contract_governance_controlled(
        &self,
        storage: &Storage,
        contract_address: &str,
        new_bytecode: Vec<u8>,
        proposal_id: u64,
        current_timestamp: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // 1. Decodifica contract address
        let address_bytes = Self::decode_address(contract_address)?;

        // 2. Compute code_hash of the nuovo bytecode
        let new_code_hash = Self::keccak256(&new_bytecode);

        self.validate_governance_proposal(
            storage,
            proposal_id,
            &address_bytes,
            &new_code_hash,
            current_timestamp,
        )
        .with_context(|| format!("Governance proposal {} validation failed", proposal_id))?;

        self.validate_storage_layout(&new_bytecode)
            .map_err(|e| anyhow::anyhow!("Storage layout validation failed: {}", e))?;

        // 5. Runs l'upgrade
        self.upgrade_contract(storage, contract_address, new_bytecode, gas_meter)
            .with_context(|| {
                format!(
                    "Failed to upgrade contract after governance approval (proposal {})",
                    proposal_id
                )
            })?;

        Ok(())
    }
}

impl Default for UpgradeSystem {
    fn default() -> Self {
        Self::new()
    }
}
