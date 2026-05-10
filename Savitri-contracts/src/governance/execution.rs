//! Proposal Execution: Esecuzione di proposte approvate
//!
//! - Esecuzione azioni proposte
//! - Treasury spending (max 5% per proposta)
//! - Burn deposit se proposta negata
//!
//! ## Tipi di Proposte Supportate
//!
//! Il sistema supporta quattro tipi di proposte:
//!
//! 1. **FeeVariation**: Modifica dei parametri fee (base_fee e max_fee)
//!
//! 2. **ProjectSelection**: Selezione di progetti da finanziare dal treasury
//!    - Transfers fondi dal treasury a un progetto specificato
//!    - L'importo non può superare il 5% of the treasury totale
//!
//! 3. **Standards**: Approvazione di nuovi standard per smart contract
//!    - Registra nuovi standard approvati dalla governance
//!
//! 4. **NonCore**: Modifiche non-core alla blockchain
//!    - Registra modifiche informative approvate dalla governance
//!    - Principalmente per audit e trasparenza

use crate::fee::Treasury;
use crate::governance::proposals::{Proposal, ProposalAction, ProposalStatus};
use hex;
use savitri_storage::storage::Storage;

/// Esecutore di proposte
pub struct ProposalExecutor {
    #[allow(dead_code)] // Conservato per uso futuro (es. logging, validazioni)
    treasury: Treasury,
}

impl ProposalExecutor {
    pub fn new(treasury: Treasury) -> Self {
        Self { treasury }
    }

    /// Runs una proposta approvata
    ///
    /// Check che la proposta sia in stato Approved prima di eseguire.
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da eseguire
    ///
    /// # Ritorna
    /// `Ok(())` se l'esecuzione è riuscita, `Err` in caso di errore
    ///
    /// # Errori
    /// - `Proposta non approvata`: La proposta non è in stato Approved
    pub fn execute_proposal(
        &self,
        storage: &mut Storage,
        proposal: &Proposal,
    ) -> anyhow::Result<()> {
        if proposal.status != ProposalStatus::Approved {
            anyhow::bail!(
                "Proposta non approvata: stato corrente è {:?}",
                proposal.status
            );
        }

        match &proposal.action {
            ProposalAction::FeeVariation {
                new_base_fee,
                new_max_fee,
            } => self.execute_fee_variation(storage, new_base_fee, new_max_fee),
            ProposalAction::ProjectSelection {
                project_address,
                amount,
            } => self.execute_project_selection(storage, project_address, *amount),
            ProposalAction::Standards {
                standard_name,
                standard_version,
            } => self.execute_standard_approval(storage, standard_name, standard_version),
            ProposalAction::NonCore { description } => {
                self.execute_non_core_change(storage, description)
            }
            ProposalAction::ContractUpgrade {
                contract_address: _,
                new_code_hash: _,
                description: _,
            } => {
                // L'upgrade viene eseguito tramite la funzione upgrade() of the contract
                Ok(())
            }
            ProposalAction::SlashingParamsUpdate {
                new_min_bond_amount,
                new_slash_pct_equivocation,
                new_slash_pct_double_vote,
                new_slash_pct_invalid_attestation,
            } => self.execute_slashing_params_update(
                storage,
                *new_min_bond_amount,
                *new_slash_pct_equivocation,
                *new_slash_pct_double_vote,
                *new_slash_pct_invalid_attestation,
            ),
            ProposalAction::SetFlPolicy {
                fee_treasury_bps,
                max_models,
                whitelist_aggregators,
            } => self.execute_set_fl_policy(
                storage,
                *fee_treasury_bps,
                *max_models,
                whitelist_aggregators,
            ),
            ProposalAction::ApproveFlModel { model_id } => {
                self.execute_approve_fl_model(storage, model_id)
            }
            ProposalAction::AbortFlRound { model_id, round_id } => {
                self.execute_abort_fl_round(storage, model_id, *round_id)
            }
            ProposalAction::AddConnector {
                connector_id,
                pubkey,
                config,
            } => self.execute_add_connector(storage, connector_id, pubkey, config),
            ProposalAction::RemoveConnector { connector_id } => {
                self.execute_remove_connector(storage, connector_id)
            }
        }
    }

    ///
    /// Check che almeno uno dei parametri fee sia specificato e che i valori siano validi.
    ///
    /// # Argomenti
    /// - `new_base_fee`: Nuovo fee base (opzionale)
    /// - `new_max_fee`: Nuovo fee massimo (opzionale)
    ///
    /// # Ritorna
    pub fn validate_fee_variation(
        new_base_fee: &Option<u128>,
        new_max_fee: &Option<u128>,
    ) -> anyhow::Result<()> {
        // Check che almeno uno dei parametri sia specificato
        if new_base_fee.is_none() && new_max_fee.is_none() {
            anyhow::bail!("Almeno uno tra new_base_fee e new_max_fee deve essere specificato");
        }

        if let Some(base_fee) = new_base_fee {
            if *base_fee < 100_000_000_000_000 {
                anyhow::bail!("Fee base troppo basso: minimo 0.0001 token");
            }
            if *base_fee > 1_000_000_000_000_000_000_000 {
                anyhow::bail!("Fee base troppo alto: massimo 1.0 token");
            }
        }

        if let Some(max_fee) = new_max_fee {
            if *max_fee < 1_000_000_000_000_000 {
                anyhow::bail!("Fee max troppo basso: minimo 0.001 token");
            }
            if *max_fee > 10_000_000_000_000_000_000_000 {
                anyhow::bail!("Fee max troppo alto: massimo 10.0 token");
            }
        }

        if let (Some(base_fee), Some(max_fee)) = (new_base_fee, new_max_fee) {
            if base_fee > max_fee {
                anyhow::bail!(
                    "Fee base ({}) non può essere maggiore di fee max ({})",
                    base_fee,
                    max_fee
                );
            }
        }

        Ok(())
    }

    /// Runs variazione parametri fee
    ///
    /// I nuovi valori are salvati in the storage e utilizzati per il calcolo dei fee.
    ///
    /// # Argomenti
    /// - `storage`: Storage per salvare i nuovi parametri fee
    /// - `new_base_fee`: Nuovo fee base (opzionale)
    /// - `new_max_fee`: Nuovo fee massimo (opzionale)
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `Fee base troppo basso/alto`: Il fee base non è nel range valido
    /// - `Fee max troppo basso/alto`: Il fee max non è nel range valido
    /// - `Fee base > fee max`: Il fee base è maggiore of the fee max
    fn execute_fee_variation(
        &self,
        storage: &Storage,
        new_base_fee: &Option<u128>,
        new_max_fee: &Option<u128>,
    ) -> anyhow::Result<()> {
        Self::validate_fee_variation(new_base_fee, new_max_fee)?;

        if let Some(base_fee) = new_base_fee {
            let fee: u64 = (*base_fee)
                .try_into()
                .map_err(|_| anyhow::anyhow!("new_base_fee does not fit into u64"))?;
            storage.set_fee_base(fee)?;
        }

        if let Some(max_fee) = new_max_fee {
            let fee: u64 = (*max_fee)
                .try_into()
                .map_err(|_| anyhow::anyhow!("new_max_fee does not fit into u64"))?;
            storage.set_fee_max(fee)?;
        }

        Ok(())
    }

    ///
    /// Check che l'address of the progetto sia valido e che l'importo sia positivo.
    ///
    /// # Argomenti
    /// - `amount`: Importo da trasferire
    ///
    /// # Ritorna
    pub fn validate_project_selection(project_address: &str, amount: u128) -> anyhow::Result<()> {
        // Check che l'address non sia vuoto
        if project_address.is_empty() {
            anyhow::bail!("Project address non può essere vuoto");
        }

        // Check che l'address sia un hex valido e di 32 bytes (64 caratteri hex)
        let address_bytes = hex::decode(project_address)
            .map_err(|e| anyhow::anyhow!("Invalid project address hex: {}", e))?;

        if address_bytes.len() != 32 {
            anyhow::bail!(
                "Project address deve essere di 32 bytes (64 caratteri hex), trovati {} bytes",
                address_bytes.len()
            );
        }

        // Check che l'importo sia positivo
        if amount == 0 {
            anyhow::bail!("Amount deve essere maggiore di zero");
        }

        Ok(())
    }

    /// Runs selezione progetto e treasury spending
    ///
    /// Transfers fondi dal treasury al progetto specificato, verificando che:
    /// - Il treasury abbia fondi sufficienti
    /// - L'importo non superi il 5% of the treasury totale
    ///
    ///
    /// # Argomenti
    /// - `amount`: Importo da trasferire
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `Invalid project address`: L'address of the progetto non è valido
    /// - `Insufficient treasury balance`: Il treasury non ha fondi sufficienti
    /// - `Amount exceeds limit`: L'importo supera il 5% of the treasury
    fn execute_project_selection(
        &self,
        storage: &Storage,
        project_address: &str,
        amount: u128,
    ) -> anyhow::Result<()> {
        Self::validate_project_selection(project_address, amount)?;

        let mut treasury = self.treasury.clone();
        treasury.spend(storage, amount, project_address)
    }

    ///
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    pub fn validate_standard_approval(
        standard_name: &str,
        standard_version: &str,
    ) -> anyhow::Result<()> {
        if standard_name.is_empty() {
            anyhow::bail!("Standard name non può essere vuoto");
        }
        if standard_version.is_empty() {
            anyhow::bail!("Standard version non può essere vuoto");
        }

        if standard_name.len() > 100 {
            anyhow::bail!("Standard name troppo lungo: massimo 100 caratteri");
        }
        if standard_version.len() > 50 {
            anyhow::bail!("Standard version troppo lunga: massimo 50 caratteri");
        }

        // Formato accettato: "1.0.0", "1.0", "1", "v1.0.0", etc.
        if !standard_version.chars().any(|c| c.is_alphanumeric()) {
            anyhow::bail!("Standard version deve contenere almeno un carattere alfanumerico");
        }

        Ok(())
    }

    /// Runs approvazione standard
    ///
    ///
    /// # Argomenti
    /// - `storage`: Storage per salvare lo standard approvato
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `Standard name/version troppo lungo`: Il nome o la versione superano i limiti
    fn execute_standard_approval(
        &self,
        storage: &Storage,
        standard_name: &str,
        standard_version: &str,
    ) -> anyhow::Result<()> {
        Self::validate_standard_approval(standard_name, standard_version)?;

        // Registra lo standard approvato in the storage
        storage.put_approved_standard(standard_name.as_bytes(), standard_version.as_bytes())?;

        Ok(())
    }

    ///
    /// Check che contract_address e new_code_hash siano validi.
    ///
    /// # Argomenti
    /// - `new_code_hash`: Code hash of the nuovo bytecode (32 bytes)
    ///
    /// # Ritorna
    pub fn validate_contract_upgrade(
        contract_address: &[u8],
        new_code_hash: &[u8],
    ) -> anyhow::Result<()> {
        // Check che contract_address sia di 32 bytes
        if contract_address.len() != 32 {
            anyhow::bail!(
                "Contract address deve essere di 32 bytes, trovati {} bytes",
                contract_address.len()
            );
        }

        // Check che new_code_hash sia di 32 bytes
        if new_code_hash.len() != 32 {
            anyhow::bail!(
                "New code hash deve essere di 32 bytes, trovati {} bytes",
                new_code_hash.len()
            );
        }

        // Check che contract_address non sia tutto zero (indirizzo nullo)
        if contract_address.iter().all(|&b| b == 0) {
            anyhow::bail!("Contract address non può essere l'indirizzo nullo");
        }

        // Check che new_code_hash non sia tutto zero
        if new_code_hash.iter().all(|&b| b == 0) {
            anyhow::bail!("New code hash non può essere zero");
        }

        Ok(())
    }

    ///
    /// Check che la descrizione non sia vuota e non superi i limiti.
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    pub fn validate_non_core_change(description: &str) -> anyhow::Result<()> {
        if description.is_empty() {
            anyhow::bail!("Description non può essere vuota");
        }

        if description.len() > 10000 {
            anyhow::bail!("Description troppo lunga: massimo 10000 caratteri");
        }

        Ok(())
    }

    /// Runs modifiche non-core
    ///
    /// Registra le modifiche non-core approvate dalla governance.
    /// per audit e trasparenza.
    ///
    /// # Argomenti
    /// - `storage`: Storage per registrare le modifiche
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `Description troppo lunga`: La descrizione supera i limiti
    fn execute_non_core_change(&self, storage: &Storage, description: &str) -> anyhow::Result<()> {
        Self::validate_non_core_change(description)?;

        // Registra la modifica non-core in the storage
        // Usa un timestamp per identificare univocamente la modifica
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let key = format!("non_core_change:{}", timestamp);
        storage.put_non_core_change(key.as_bytes(), description.as_bytes())?;

        Ok(())
    }

    ///
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::execution::ProposalExecutor;
    /// use crate::governance::proposals::ProposalAction;
    /// use crate::fee::Treasury;
    ///
    /// let executor = ProposalExecutor::new(Treasury::new());
    /// let action = ProposalAction::FeeVariation {
    ///     new_base_fee: Some(100_000_000_000_000),
    ///     new_max_fee: Some(1_000_000_000_000_000),
    /// };
    /// ```
    pub fn validate_action(&self, action: &ProposalAction) -> anyhow::Result<()> {
        match action {
            ProposalAction::FeeVariation {
                new_base_fee,
                new_max_fee,
            } => Self::validate_fee_variation(new_base_fee, new_max_fee),
            ProposalAction::ProjectSelection {
                project_address,
                amount,
            } => Self::validate_project_selection(project_address, *amount),
            ProposalAction::Standards {
                standard_name,
                standard_version,
            } => Self::validate_standard_approval(standard_name, standard_version),
            ProposalAction::NonCore { description } => Self::validate_non_core_change(description),
            ProposalAction::ContractUpgrade {
                contract_address,
                new_code_hash,
                description: _,
            } => Self::validate_contract_upgrade(contract_address, new_code_hash),
            ProposalAction::SlashingParamsUpdate {
                new_min_bond_amount,
                new_slash_pct_equivocation,
                new_slash_pct_double_vote,
                new_slash_pct_invalid_attestation,
            } => Self::validate_slashing_params_update(
                new_min_bond_amount,
                new_slash_pct_equivocation,
                new_slash_pct_double_vote,
                new_slash_pct_invalid_attestation,
            ),
            ProposalAction::SetFlPolicy {
                fee_treasury_bps,
                max_models,
                whitelist_aggregators,
            } => {
                Self::validate_set_fl_policy(*fee_treasury_bps, *max_models, whitelist_aggregators)
            }
            ProposalAction::ApproveFlModel { model_id } => Self::validate_model_id(model_id),
            ProposalAction::AbortFlRound { model_id, round_id } => {
                Self::validate_abort_round(model_id, *round_id)
            }
            ProposalAction::AddConnector {
                connector_id,
                pubkey,
                config,
            } => Self::validate_add_connector(connector_id, pubkey, config),
            ProposalAction::RemoveConnector { connector_id } => {
                Self::validate_remove_connector(connector_id)
            }
        }
    }

    ///
    /// Quando una proposta è negata (non raggiunge quorum o approval), il deposit viene bruciato
    ///
    /// Il burn è effettivamente già avvenuto quando la proposta è stata creata:
    ///
    /// # Argomenti
    /// - `proposal`: La proposta negata
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - La proposta non è in the stato Rejected
    /// - Altri errori di storage
    ///
    /// # Note
    /// `DepositManager::burn_deposit()` per gestire il burn. Il burn è già implicito
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::execution::ProposalExecutor;
    /// use crate::governance::proposals::{Proposal, ProposalStatus};
    /// use crate::fee::Treasury;
    /// use crate::storage::Storage;
    ///
    /// let executor = ProposalExecutor::new(Treasury::new());
    /// let storage = Storage::new("db")?;
    /// // ... dopo che una proposta è stata rifiutata ...
    /// executor.burn_deposit_if_rejected(&storage, &rejected_proposal)?;
    /// ```
    pub fn burn_deposit_if_rejected(
        &self,
        storage: &Storage,
        proposal: &Proposal,
    ) -> anyhow::Result<()> {
        // Check che la proposta sia in the stato Rejected
        if proposal.status != ProposalStatus::Rejected {
            return Ok(());
        }

        // Check che il deposit sia valido
        if proposal.deposit == 0 {
            // Zero deposit, no burn required
            return Ok(());
        }

        // Usa DepositManager per gestire il burn
        use crate::governance::deposit::DepositManager;
        let deposit_manager = DepositManager::new();
        deposit_manager.burn_deposit(storage, proposal)?;

        Ok(())
    }

    /// Gestisce il risultato finale di una proposta (esecuzione o burn)
    ///
    /// - Burn of the deposit se negata
    ///
    /// # Argomenti
    /// - `proposal`: La proposta finalizzata
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - Errori durante il burn of the deposit se negata
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::execution::ProposalExecutor;
    /// use crate::governance::proposals::Proposal;
    /// use crate::fee::Treasury;
    /// use crate::storage::Storage;
    ///
    /// let executor = ProposalExecutor::new(Treasury::new());
    /// let storage = Storage::new("db")?;
    /// // ... dopo che una proposta è stata finalizzata ...
    /// executor.handle_finalized_proposal(&storage, &proposal)?;
    /// ```
    pub fn handle_finalized_proposal(
        &self,
        storage: &mut Storage,
        proposal: &Proposal,
    ) -> anyhow::Result<()> {
        match proposal.status {
            ProposalStatus::Approved => {
                // Runs la proposta approvata
                self.execute_proposal(storage, proposal)?;
            }
            ProposalStatus::Rejected => {
                self.burn_deposit_if_rejected(storage, proposal)?;
            }
            _ => {
                anyhow::bail!(
                    "Proposta non finalizzata: stato attuale {:?}. Solo proposte Approved o Rejected possono essere gestite.",
                    proposal.status
                );
            }
        }
        Ok(())
    }

    fn execute_slashing_params_update(
        &self,
        _storage: &mut Storage,
        new_min_bond_amount: Option<u128>,
        new_slash_pct_equivocation: Option<u16>,
        new_slash_pct_double_vote: Option<u16>,
        new_slash_pct_invalid_attestation: Option<u16>,
    ) -> anyhow::Result<()> {
        use savitri_storage::storage::bonds::{BondManager, SlashingParams};
        let bond_manager = BondManager::new();
        let current_params = bond_manager.get_slashing_params();

        // Creates nuovi parametri aggiornando solo i valori specificati
        let new_params = SlashingParams {
            equivocation_pct: new_slash_pct_equivocation.unwrap_or(current_params.equivocation_pct),
            double_vote_pct: new_slash_pct_double_vote.unwrap_or(current_params.double_vote_pct),
            invalid_attestation_pct: new_slash_pct_invalid_attestation
                .unwrap_or(current_params.invalid_attestation_pct),
            min_bond_amount: new_min_bond_amount.unwrap_or(current_params.min_bond_amount),
            slash_pct_equivocation: new_slash_pct_equivocation
                .unwrap_or(current_params.slash_pct_equivocation),
            slash_pct_double_vote: new_slash_pct_double_vote
                .unwrap_or(current_params.slash_pct_double_vote),
            slash_pct_invalid_attestation: new_slash_pct_invalid_attestation
                .unwrap_or(current_params.slash_pct_invalid_attestation),
            updated_at: current_params.updated_at,
        };

        bond_manager.update_slashing_params(new_params)?;

        Ok(())
    }

    fn validate_set_fl_policy(
        fee_treasury_bps: u16,
        max_models: u32,
        whitelist_aggregators: &[String],
    ) -> anyhow::Result<()> {
        if fee_treasury_bps > 10_000 {
            anyhow::bail!("fee_treasury_bps deve essere <= 10000 (bps)");
        }
        if max_models == 0 {
            anyhow::bail!("max_models deve essere > 0");
        }
        for addr in whitelist_aggregators {
            let bytes = hex::decode(addr)
                .map_err(|e| anyhow::anyhow!("Invalid aggregator address: {}", e))?;
            if bytes.len() != 32 {
                anyhow::bail!(
                    "Aggregator address deve essere 32 bytes, trovato {}",
                    bytes.len()
                );
            }
        }
        Ok(())
    }

    fn validate_model_id(model_id: &str) -> anyhow::Result<()> {
        let bytes =
            hex::decode(model_id).map_err(|e| anyhow::anyhow!("Invalid model_id: {}", e))?;
        if bytes.len() != 32 {
            anyhow::bail!("model_id deve essere 32 bytes, trovato {}", bytes.len());
        }
        Ok(())
    }

    fn validate_abort_round(model_id: &str, round_id: u64) -> anyhow::Result<()> {
        Self::validate_model_id(model_id)?;
        if round_id == 0 {
            anyhow::bail!("round_id deve essere > 0");
        }
        Ok(())
    }

    /// Runs set policy FL
    fn execute_set_fl_policy(
        &self,
        storage: &mut Storage,
        fee_treasury_bps: u16,
        max_models: u32,
        whitelist_aggregators: &Vec<String>,
    ) -> anyhow::Result<()> {
        Self::validate_set_fl_policy(fee_treasury_bps, max_models, whitelist_aggregators)?;

        // SECURITY (HIGH-06): Validate all aggregator addresses before building the
        // policy struct. `.unwrap()` on user-controlled hex strings would panic on
        // invalid input; use `?` propagation instead.
        let decoded_aggregators: anyhow::Result<Vec<Vec<u8>>> = whitelist_aggregators
            .iter()
            .map(|a| {
                hex::decode(a).map_err(|e| anyhow::anyhow!("Invalid aggregator hex '{}': {}", a, e))
            })
            .collect();
        let decoded_aggregators = decoded_aggregators?;

        let policy = crate::storage::FlPolicy {
            fee_treasury_bps,
            max_models,
            whitelist_aggregators: decoded_aggregators,
            min_contributions_per_round: crate::storage::FlPolicy::default()
                .min_contributions_per_round,
            max_contributions_per_round: crate::storage::FlPolicy::default()
                .max_contributions_per_round,
            reward_per_contribution: crate::storage::FlPolicy::default().reward_per_contribution,
            round_duration_blocks: crate::storage::FlPolicy::default().round_duration_blocks,
            model_approval_required: crate::storage::FlPolicy::default().model_approval_required,
        };
        let policy_bytes = bincode::serialize(&policy)?;
        storage.set_fl_policy(&policy_bytes)
    }

    fn execute_approve_fl_model(
        &self,
        storage: &mut Storage,
        model_id: &str,
    ) -> anyhow::Result<()> {
        Self::validate_model_id(model_id)?;
        // SECURITY (HIGH-06): Propagate hex decode error instead of panicking.
        let bytes = hex::decode(model_id)
            .map_err(|e| anyhow::anyhow!("Invalid model_id hex '{}': {}", model_id, e))?;
        storage.approve_fl_model(&bytes)
    }

    /// Runs abort round FL
    fn execute_abort_fl_round(
        &self,
        storage: &mut Storage,
        model_id: &str,
        round_id: u64,
    ) -> anyhow::Result<()> {
        Self::validate_abort_round(model_id, round_id)?;
        storage.abort_fl_round(round_id)
    }

    fn validate_slashing_params_update(
        new_min_bond_amount: &Option<u128>,
        new_slash_pct_equivocation: &Option<u16>,
        new_slash_pct_double_vote: &Option<u16>,
        new_slash_pct_invalid_attestation: &Option<u16>,
    ) -> anyhow::Result<()> {
        use savitri_storage::storage::bonds::{MAX_SLASH_PCT, MIN_SLASH_PCT};
        use savitri_storage::storage::{MAX_BOND_AMOUNT, MIN_BOND_AMOUNT};

        if let Some(amount) = new_min_bond_amount {
            if *amount < MIN_BOND_AMOUNT || *amount > MAX_BOND_AMOUNT {
                anyhow::bail!(
                    "Invalid min_bond_amount: must be between {} and {}",
                    MIN_BOND_AMOUNT,
                    MAX_BOND_AMOUNT
                );
            }
        }

        for (name, pct) in [
            ("slash_pct_equivocation", new_slash_pct_equivocation),
            ("slash_pct_double_vote", new_slash_pct_double_vote),
            (
                "slash_pct_invalid_attestation",
                new_slash_pct_invalid_attestation,
            ),
        ] {
            if let Some(percentage) = pct {
                if *percentage < MIN_SLASH_PCT || *percentage > MAX_SLASH_PCT {
                    anyhow::bail!(
                        "Invalid {}: must be between {} and {}",
                        name,
                        MIN_SLASH_PCT,
                        MAX_SLASH_PCT
                    );
                }
            }
        }

        // Check che almeno un parametro sia specificato
        if new_min_bond_amount.is_none()
            && new_slash_pct_equivocation.is_none()
            && new_slash_pct_double_vote.is_none()
            && new_slash_pct_invalid_attestation.is_none()
        {
            anyhow::bail!("At least one parameter must be specified for SlashingParamsUpdate");
        }

        Ok(())
    }

    fn validate_add_connector(
        connector_id: &str,
        pubkey: &[u8],
        config: &crate::oracle::types::ConnectorConfig,
    ) -> anyhow::Result<()> {
        if connector_id.is_empty() {
            anyhow::bail!("connector_id cannot be empty");
        }
        if connector_id.len() > 256 {
            anyhow::bail!("connector_id too long: max 256 characters");
        }
        if pubkey.len() != 32 {
            anyhow::bail!("pubkey must be 32 bytes, found {}", pubkey.len());
        }
        config
            .validate()
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        Ok(())
    }

    fn validate_remove_connector(connector_id: &str) -> anyhow::Result<()> {
        if connector_id.is_empty() {
            anyhow::bail!("connector_id cannot be empty");
        }
        Ok(())
    }

    /// Runs aggiunta connector alla whitelist
    fn execute_add_connector(
        &self,
        storage: &mut Storage,
        connector_id: &str,
        pubkey: &[u8],
        config: &crate::oracle::types::ConnectorConfig,
    ) -> anyhow::Result<()> {
        Self::validate_add_connector(connector_id, pubkey, config)?;

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Salva connector info in the storage
        let payload = bincode::serialize(&(pubkey.to_vec(), config, current_time))
            .map_err(|e| anyhow::anyhow!("Failed to serialize connector payload: {}", e))?;
        storage.put_connector_info(connector_id.as_bytes(), &payload)?;

        Ok(())
    }

    /// Runs rimozione connector dalla whitelist
    fn execute_remove_connector(
        &self,
        storage: &mut Storage,
        connector_id: &str,
    ) -> anyhow::Result<()> {
        Self::validate_remove_connector(connector_id)?;

        // Check che connector esista
        if !storage.connector_exists(connector_id.as_bytes())? {
            anyhow::bail!("Connector {} not found", connector_id);
        }

        // Rimuovi connector
        storage.delete_connector_info(connector_id.as_bytes())?;

        Ok(())
    }
}
