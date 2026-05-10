//!
//! - Creazione con deposit
//! - Stati: Pending (24h review), Active Voting (7 giorni), Approved, Rejected
//!
//! ## Review Period
//!
//! - Gli utenti possono esaminare la proposta
//! - Dopo 24h, la proposta passa automaticamente allo stato "ActiveVoting"
//!
//! le proposte prima che inizino le votazioni.

use crate::storage::Storage;
use metrics::counter;
use savitri_storage::storage::ProposalAction as StorageProposalAction;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum allowed size for deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads from storage.
const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

/// Durata of the review period in secondi (24 ore)
///
pub const REVIEW_PERIOD_SECONDS: u64 = 24 * 60 * 60; // 24 ore

/// Durata of the voting period in secondi (7 giorni)
///
pub const VOTING_PERIOD_SECONDS: u64 = 7 * 24 * 60 * 60; // 7 giorni

/// Stato di una proposta
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalStatus {
    /// Proposta in review (24h)
    Pending,
    /// Proposta in votazione attiva (7 giorni)
    ActiveVoting,
    /// Proposta approvata
    Approved,
    /// Proposta negata
    Rejected,
}

/// Proposta di governance
///
/// - action: Tipo di azione che la proposta intende eseguire
/// - created_at: Timestamp di creazione
/// - review_end: Timestamp fine periodo di review (24h)
/// - voting_end: Timestamp fine periodo di votazione (7 giorni)
/// - yes_votes: Totale vote token votati "Yes"
/// - no_votes: Totale vote token votati "No"
/// - abstain_votes: Totale vote token votati "Abstain"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub id: u64,
    pub creator: String,
    pub deposit: u128,
    pub description: String,
    pub action: ProposalAction,
    pub status: ProposalStatus,
    pub created_at: u64,
    pub review_end: u64,
    pub voting_end: u64,
    pub yes_votes: u128,
    pub no_votes: u128,
    pub abstain_votes: u128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalAction {
    /// Variazione parametri fee
    FeeVariation {
        /// Nuovo fee base (opzionale)
        new_base_fee: Option<u128>,
        /// Nuovo fee massimo (opzionale)
        new_max_fee: Option<u128>,
    },
    /// Selezione progetto da finanziare
    ProjectSelection {
        project_address: String,
        /// Importo da trasferire
        amount: u128,
    },
    /// Approvazione nuovo standard
    Standards {
        standard_name: String,
        standard_version: String,
    },
    /// Modifiche non-core
    NonCore {
        description: String,
    },
    ContractUpgrade {
        contract_address: Vec<u8>,
        /// Code hash of the nuovo bytecode (32 bytes)
        new_code_hash: Vec<u8>,
        /// Descrizione dell'upgrade
        description: String,
    },
    SlashingParamsUpdate {
        /// Nuovo bond amount minimo
        new_min_bond_amount: Option<u128>,
        /// Nuova percentuale slash per equivocation
        new_slash_pct_equivocation: Option<u16>,
        /// Nuova percentuale slash per double vote
        new_slash_pct_double_vote: Option<u16>,
        /// Nuova percentuale slash per invalid attestation
        new_slash_pct_invalid_attestation: Option<u16>,
    },
    /// Federated Learning: set policy (governance-controlled)
    SetFlPolicy {
        fee_treasury_bps: u16,
        max_models: u32,
        whitelist_aggregators: Vec<String>, // hex-encoded addresses
    },
    /// Federated Learning: approve model for deployment
    ApproveFlModel {
        model_id: String, // hex-encoded 32 bytes
    },
    /// Federated Learning: abort training round (emergency)
    AbortFlRound {
        model_id: String, // hex-encoded 32 bytes
        round_id: u64,
    },
    /// Connector: add connector to the whitelist
    AddConnector {
        connector_id: String,
        pubkey: Vec<u8>, // 32 bytes
        config: crate::oracle::types::ConnectorConfig,
    },
    /// Connector: rimuovi connector dalla whitelist
    RemoveConnector { connector_id: String },
}

impl Proposal {
    ///
    /// # Argomenti
    /// - `action`: Tipo di azione che la proposta intende eseguire
    ///
    /// # Note
    /// La proposta viene creata con:
    /// - Status: `Pending` (review period di 24h)
    /// - Voti inizializzati a 0
    /// - Timestamp calcolati automaticamente
    pub fn new(
        id: u64,
        creator: String,
        deposit: u128,
        description: String,
        action: ProposalAction,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            creator,
            deposit,
            description,
            action,
            status: ProposalStatus::Pending,
            created_at: now,
            review_end: now + REVIEW_PERIOD_SECONDS,
            voting_end: now + REVIEW_PERIOD_SECONDS + VOTING_PERIOD_SECONDS,
            yes_votes: 0,
            no_votes: 0,
            abstain_votes: 0,
        }
    }

    /// Compute il totale dei voti (yes + no + abstain)
    pub fn total_votes(&self) -> u128 {
        self.yes_votes
            .checked_add(self.no_votes)
            .and_then(|sum| sum.checked_add(self.abstain_votes))
            .unwrap_or(u128::MAX) // Overflow protection
    }

    /// Compute il totale dei voti validi per l'approval (yes + no, escludendo abstain)
    pub fn total_valid_votes(&self) -> u128 {
        self.yes_votes
            .checked_add(self.no_votes)
            .unwrap_or(u128::MAX) // Overflow protection
    }

    /// Adds un voto "Yes" alla proposta
    ///
    /// # Argomenti
    /// - `amount`: Quantità di vote token used per il voto
    ///
    /// # Ritorna
    pub fn add_yes_vote(&mut self, amount: u128) -> Result<(), String> {
        self.yes_votes = self
            .yes_votes
            .checked_add(amount)
            .ok_or_else(|| "yes_votes overflow".to_string())?;
        Ok(())
    }

    /// Adds un voto "No" alla proposta
    ///
    /// # Argomenti
    /// - `amount`: Quantità di vote token used per il voto
    ///
    /// # Ritorna
    pub fn add_no_vote(&mut self, amount: u128) -> Result<(), String> {
        self.no_votes = self
            .no_votes
            .checked_add(amount)
            .ok_or_else(|| "no_votes overflow".to_string())?;
        Ok(())
    }

    /// Adds un voto "Abstain" alla proposta
    ///
    /// # Argomenti
    /// - `amount`: Quantità di vote token used per il voto
    ///
    /// # Ritorna
    pub fn add_abstain_vote(&mut self, amount: u128) -> Result<(), String> {
        self.abstain_votes = self
            .abstain_votes
            .checked_add(amount)
            .ok_or_else(|| "abstain_votes overflow".to_string())?;
        Ok(())
    }

    ///
    /// Durante il review period (24h dopo la creazione), la proposta è in stato "Pending"
    /// prima che inizi il periodo di votazione.
    ///
    ///
    /// # Ritorna
    pub fn is_in_review(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.is_in_review_with_timestamp(now)
    }

    ///
    /// Durante il review period (24h dopo la creazione), la proposta è in stato "Pending"
    /// prima che inizi il periodo di votazione.
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `false` altrimenti (anche se lo stato è Pending ma il review period è scaduto)
    pub fn is_in_review_with_timestamp(&self, current_timestamp: u64) -> bool {
        // La proposta è in review period solo se:
        // 1. È in stato Pending (non ancora passata a ActiveVoting)
        // 2. Il review_end non è ancora passato
        self.status == ProposalStatus::Pending && current_timestamp < self.review_end
    }

    ///
    pub fn is_in_voting(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.is_in_voting_with_timestamp(now)
    }

    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    pub fn is_in_voting_with_timestamp(&self, current_timestamp: u64) -> bool {
        self.status == ProposalStatus::ActiveVoting
            && current_timestamp >= self.review_end
            && current_timestamp < self.voting_end
    }

    /// Transizione da Pending a ActiveVoting
    ///
    pub fn start_voting(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.transition_state_with_timestamp(now);
    }

    ///
    /// # Argomenti
    /// - `deposit`: Deposit in vote token
    /// - `action`: Tipo di azione che la proposta intende eseguire
    /// - `created_at`: Timestamp di creazione personalizzato
    ///
    /// # Note
    pub fn new_with_timestamp(
        id: u64,
        creator: String,
        deposit: u128,
        description: String,
        action: ProposalAction,
        created_at: u64,
    ) -> Self {
        Self {
            id,
            creator,
            deposit,
            description,
            action,
            status: ProposalStatus::Pending,
            created_at,
            review_end: created_at + REVIEW_PERIOD_SECONDS,
            voting_end: created_at + REVIEW_PERIOD_SECONDS + VOTING_PERIOD_SECONDS,
            yes_votes: 0,
            no_votes: 0,
            abstain_votes: 0,
        }
    }

    ///
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.is_expired_with_timestamp(now)
    }

    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    pub fn is_expired_with_timestamp(&self, current_timestamp: u64) -> bool {
        current_timestamp >= self.voting_end
    }

    ///
    /// - La proposta non sia in review period
    /// - La proposta sia in stato ActiveVoting
    /// - Il voting period non sia scaduto
    ///
    ///
    /// # Ritorna
    pub fn can_be_voted(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.can_be_voted_with_timestamp(now)
    }

    /// Compute il timestamp di fine review period
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    /// Il timestamp di fine review period (created_at + 24h)
    pub fn calculate_review_end(created_at: u64) -> u64 {
        created_at + REVIEW_PERIOD_SECONDS
    }

    /// Compute il timestamp di fine voting period
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    /// Il timestamp di fine voting period (created_at + 24h + 7 giorni)
    pub fn calculate_voting_end(created_at: u64) -> u64 {
        created_at + REVIEW_PERIOD_SECONDS + VOTING_PERIOD_SECONDS
    }

    pub fn approve(&mut self) {
        self.status = ProposalStatus::Approved;
        counter!("governance_proposals_approved_total").increment(1);
    }

    pub fn reject(&mut self) {
        self.status = ProposalStatus::Rejected;
        counter!("governance_proposals_rejected_total").increment(1);
    }

    pub fn is_finalized(&self) -> bool {
        matches!(
            self.status,
            ProposalStatus::Approved | ProposalStatus::Rejected
        )
    }

    ///
    /// - Pending -> ActiveVoting: quando `review_end` è passato
    /// - ActiveVoting -> Rejected: quando `voting_end` è passato (se non è già approvata)
    ///
    /// Le transizioni verso Approved o Rejected basate sui risultati di votazione devono
    /// quorum e approval.
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `true` se lo stato è stato modificato, `false` altrimenti
    ///
    /// # Note
    /// Le proposte finalizzate (Approved/Rejected) non are modificate.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::proposals::Proposal;
    /// let mut proposal = Proposal::new_with_timestamp(
    ///     1, "creator".to_string(), 100, "test".to_string(),
    ///     ProposalAction::NonCore { description: "test".to_string() },
    ///     1000 // created_at
    /// );
    /// // review_end = 1000 + 24*60*60 = 87400
    /// let changed = proposal.transition_state_with_timestamp(87401); // Dopo review_end
    /// assert!(changed);
    /// assert_eq!(proposal.status, ProposalStatus::ActiveVoting);
    /// ```
    pub fn transition_state_with_timestamp(&mut self, current_timestamp: u64) -> bool {
        // Le proposte finalizzate non cambiano stato
        if self.is_finalized() {
            return false;
        }

        match self.status {
            ProposalStatus::Pending => {
                // Transizione Pending -> ActiveVoting quando review_end è passato
                if current_timestamp >= self.review_end {
                    self.status = ProposalStatus::ActiveVoting;
                    return true;
                }
            }
            ProposalStatus::ActiveVoting => {
                // Transizione ActiveVoting -> Rejected quando voting_end è passato
                // (se non è già stata approvata manualmente)
                if current_timestamp >= self.voting_end {
                    // Se la proposta è scaduta e non è stata approvata, viene automaticamente rifiutata
                    self.status = ProposalStatus::Rejected;
                    return true;
                }
            }
            ProposalStatus::Approved | ProposalStatus::Rejected => {
                // Stati finali, non cambiano
                return false;
            }
        }

        false
    }

    ///
    ///
    /// # Ritorna
    /// `true` se lo stato è stato modificato, `false` altrimenti
    pub fn transition_state(&mut self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.transition_state_with_timestamp(now)
    }

    ///
    /// Utile per verificare se `transition_state()` modificherebbe lo stato.
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `true` se una transizione è necessaria, `false` altrimenti
    pub fn should_transition(&self, current_timestamp: u64) -> bool {
        if self.is_finalized() {
            return false;
        }

        match self.status {
            ProposalStatus::Pending => {
                // Deve transizionare se review_end è passato
                current_timestamp >= self.review_end
            }
            ProposalStatus::ActiveVoting => {
                // Deve transizionare se voting_end è passato
                current_timestamp >= self.voting_end
            }
            ProposalStatus::Approved | ProposalStatus::Rejected => {
                // Stati finali, non transizionano
                false
            }
        }
    }

    ///
    /// Le transizioni valide sono:
    /// - Pending -> ActiveVoting (quando review_end è passato)
    /// - ActiveVoting -> Approved (quando approvata manualmente)
    /// - ActiveVoting -> Rejected (quando rifiutata manualmente o voting_end è passato)
    /// - Approved -> (no transition, terminal state)
    /// - Rejected -> (no transition, terminal state)
    ///
    /// # Argomenti
    /// - `new_status`: Il nuovo stato desiderato
    ///
    /// # Ritorna
    pub fn is_valid_transition(&self, new_status: ProposalStatus) -> bool {
        // Non si può transizionare se già finalizzata
        if self.is_finalized() {
            return false;
        }

        match self.status {
            ProposalStatus::Pending => {
                // Da Pending si può solo andare a ActiveVoting
                new_status == ProposalStatus::ActiveVoting
            }
            ProposalStatus::ActiveVoting => {
                // Da ActiveVoting si può andare a Approved o Rejected
                matches!(
                    new_status,
                    ProposalStatus::Approved | ProposalStatus::Rejected
                )
            }
            ProposalStatus::Approved | ProposalStatus::Rejected => {
                // Stati finali, non si può transizionare
                false
            }
        }
    }

    ///
    /// Usa `transition_state_with_timestamp()` per transizioni automatiche basate su timestamp,
    ///
    /// # Argomenti
    /// - `new_status`: Il nuovo stato desiderato (per transizioni manuali)
    /// - `current_timestamp`: Timestamp corrente per transizioni automatiche
    ///
    /// # Ritorna
    pub fn try_transition_to(
        &mut self,
        new_status: ProposalStatus,
        current_timestamp: u64,
    ) -> Result<bool, String> {
        if self.status == new_status {
            return Ok(false);
        }

        if !self.is_valid_transition(new_status) {
            return Err(format!(
                "Transizione invalida: da {:?} a {:?}",
                self.status, new_status
            ));
        }

        // Applica la transizione
        match new_status {
            ProposalStatus::ActiveVoting => {
                // Transizione automatica basata su timestamp
                Ok(self.transition_state_with_timestamp(current_timestamp))
            }
            ProposalStatus::Approved => {
                self.approve();
                Ok(true)
            }
            ProposalStatus::Rejected => {
                self.reject();
                Ok(true)
            }
            ProposalStatus::Pending => {
                // Non si può tornare a Pending
                Err("Non è possibile tornare allo stato Pending".to_string())
            }
        }
    }

    ///
    /// - È in stato ActiveVoting (la transizione Pending -> ActiveVoting è avvenuta)
    /// - Il voting_end non è ancora passato (il periodo di votazione non è scaduto)
    ///
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::proposals::{Proposal, ProposalAction};
    /// let proposal = Proposal::new_with_timestamp(
    ///     1, "creator".to_string(), 100, "test".to_string(),
    ///     ProposalAction::NonCore { description: "test".to_string() },
    ///     1000 // created_at
    /// );
    /// // review_end = 1000 + 24*60*60 = 87400
    /// // voting_end = 87400 + 7*24*60*60 = 691600
    ///
    /// // Durante review period (prima di review_end), non si può votare
    /// assert!(!proposal.can_be_voted_with_timestamp(50000));
    ///
    /// // Dopo review period, si può votare (se lo stato è ActiveVoting)
    /// ```
    pub fn can_be_voted_with_timestamp(&self, current_timestamp: u64) -> bool {
        // Check che la proposta non sia in review period
        if self.is_in_review_with_timestamp(current_timestamp) {
            return false;
        }

        // Check che la proposta sia in stato ActiveVoting
        if self.status != ProposalStatus::ActiveVoting {
            return false;
        }

        // Check che il voting period non sia scaduto
        current_timestamp < self.voting_end
    }
}

pub struct ProposalSystem;

impl ProposalSystem {
    pub fn new() -> Self {
        Self
    }

    /// Compute il minimo deposit richiesto (5% of the vote token supply totale)
    ///
    /// Il deposit minimo è calcolato come: `total_vote_tokens * 5 / 100`
    ///
    /// # Argomenti
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    /// Il minimo deposit richiesto in vote token.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::proposals::ProposalSystem;
    /// let system = ProposalSystem::new();
    /// // Con 1000 vote token totali, il minimo deposit è 50
    /// let min_deposit = system.calculate_min_deposit(1000);
    /// assert_eq!(min_deposit, 50); // 5% di 1000
    /// ```
    pub fn calculate_min_deposit(&self, total_vote_tokens: u128) -> u128 {
        // Calcolo preciso: min_deposit = total_vote_tokens * 5 / 100
        total_vote_tokens
            .checked_mul(5)
            .and_then(|v| v.checked_div(100))
            .unwrap_or(u128::MAX) // Overflow protection
    }

    ///
    /// # Argomenti
    /// - `deposit`: Il deposit fornito
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::proposals::ProposalSystem;
    /// let system = ProposalSystem::new();
    /// // Con 1000 vote token totali, il minimo deposit è 50
    /// assert!(system.is_deposit_sufficient(50, 1000)); // Deposit sufficiente
    /// assert!(!system.is_deposit_sufficient(49, 1000)); // Deposit insufficiente
    /// ```
    pub fn is_deposit_sufficient(&self, deposit: u128, total_vote_tokens: u128) -> bool {
        if total_vote_tokens == 0 {
            return false; // Nessun vote token disponibile
        }

        let min_deposit = self.calculate_min_deposit(total_vote_tokens);
        deposit >= min_deposit
    }

    ///
    /// 2. Check che il creator abbia abbastanza vote token
    /// 3. Locka i vote token of the deposit
    /// 5. Creates la proposta con stato Pending
    /// 6. Salva la proposta in the storage
    ///
    /// # Argomenti
    /// - `deposit`: Deposit in vote token da lockare
    /// - `action`: Tipo di azione che la proposta intende eseguire
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `InsufficientDeposit`: Il deposit è inferiore al minimo richiesto (5%)
    /// - `InsufficientBalance`: Il creator non ha abbastanza vote token
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::proposals::{ProposalSystem, ProposalAction};
    /// use crate::storage::Storage;
    /// let system = ProposalSystem::new();
    /// let storage = Storage::new("db")?;
    /// let creator = [0u8; 32];
    /// let action = ProposalAction::NonCore { description: "Test".to_string() };
    /// let proposal = system.create_proposal(
    ///     &storage,
    ///     &creator,
    ///     100, // deposit
    ///     "Test proposal".to_string(),
    ///     action,
    /// )?;
    /// ```
    pub fn create_proposal(
        &self,
        storage: &Storage,
        creator: &[u8; 32],
        deposit: u128,
        description: String,
        action: ProposalAction,
    ) -> anyhow::Result<Proposal> {
        // 1. Ottieni il totale dei vote token
        let total_vote_tokens = storage.get_total_vote_tokens()?;

        if !self.is_deposit_sufficient(deposit, total_vote_tokens) {
            let min_deposit = self.calculate_min_deposit(total_vote_tokens);
            anyhow::bail!(
                "Deposit insufficiente: richiesto almeno {} vote token (5% di {}), forniti {}",
                min_deposit,
                total_vote_tokens,
                deposit
            );
        }

        // 3. Check che il creator abbia abbastanza vote token
        let creator_balance = storage.get_vote_token_balance(creator)?;
        if creator_balance < deposit {
            anyhow::bail!(
                "Balance insufficiente: creator ha {} vote token, richiesti {}",
                creator_balance,
                deposit
            );
        }

        // 4. Locka i vote token of the deposit
        storage.decrement_vote_token_balance(creator, deposit)?;

        let proposal_id = storage.next_proposal_id()?;

        // 6. Creates la proposta con stato Pending
        let proposal = Proposal::new(
            proposal_id,
            hex::encode(creator), // Converti address a string per compatibilità
            deposit,
            description,
            action,
        );

        let storage_proposal = Self::governance_proposal_to_storage_proposal(&proposal)?;
        let encoded = bincode::serialize(&storage_proposal)?;
        storage.put_proposal(storage_proposal.id, &encoded)?;

        counter!("governance_proposals_total").increment(1);

        Ok(proposal)
    }

    ///
    /// il totale dei vote token dallo storage.
    ///
    /// # Argomenti
    /// - `deposit`: Deposit in vote token da lockare
    /// - `action`: Tipo di azione che la proposta intende eseguire
    ///
    /// # Ritorna
    pub fn create_proposal_with_validation(
        &self,
        storage: &Storage,
        creator: &[u8; 32],
        deposit: u128,
        description: String,
        action: ProposalAction,
    ) -> anyhow::Result<Proposal> {
        self.create_proposal(storage, creator, deposit, description, action)
    }

    ///
    /// basandosi sui timestamp, e lo salva di nuovo in the storage.
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `Ok(true)` se lo stato è stato modificato, `Ok(false)` se non era necessario, `Err` in caso di errore
    pub fn update_proposal_state(
        &self,
        storage: &Storage,
        proposal_id: u64,
        current_timestamp: u64,
    ) -> anyhow::Result<bool> {
        // Carica la proposta dallo storage
        let storage_proposal = storage
            .get_proposal(proposal_id)?
            .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
        if storage_proposal.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                storage_proposal.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let storage_proposal: savitri_storage::storage::Proposal =
            bincode::deserialize(&storage_proposal)?;

        let mut proposal =
            ProposalSystem::storage_proposal_to_governance_proposal(&storage_proposal)?;

        let changed = proposal.transition_state_with_timestamp(current_timestamp);

        if changed {
            // Salva la proposta aggiornata in the storage
            let updated_storage_proposal =
                Self::governance_proposal_to_storage_proposal(&proposal)?;
            let encoded = bincode::serialize(&updated_storage_proposal)?;
            storage.put_proposal(updated_storage_proposal.id, &encoded)?;
        }

        Ok(changed)
    }

    /// Converti una proposta dallo storage al formato governance
    pub fn storage_proposal_to_governance_proposal(
        storage_proposal: &savitri_storage::storage::Proposal,
    ) -> anyhow::Result<Proposal> {
        Ok(Proposal {
            id: storage_proposal.id,
            creator: hex::encode(&storage_proposal.creator),
            deposit: storage_proposal.deposit,
            description: storage_proposal.description.clone(),
            action: match &storage_proposal.action {
                StorageProposalAction::FeeVariation {
                    new_base_fee,
                    new_max_fee,
                } => ProposalAction::FeeVariation {
                    new_base_fee: Some(new_base_fee.unwrap_or(0)),
                    new_max_fee: Some(new_max_fee.unwrap_or(0)),
                },
                StorageProposalAction::ProjectSelection {
                    project_address,
                    amount,
                } => ProposalAction::ProjectSelection {
                    project_address: hex::encode(project_address),
                    amount: *amount,
                },
                StorageProposalAction::Standards {
                    standard_name,
                    standard_version,
                } => ProposalAction::Standards {
                    standard_name: standard_name.clone(),
                    standard_version: standard_version.clone(),
                },
                StorageProposalAction::NonCore { description } => ProposalAction::NonCore {
                    description: description.clone(),
                },
                StorageProposalAction::ContractUpgrade {
                    contract_address,
                    new_code_hash,
                    description,
                } => ProposalAction::ContractUpgrade {
                    contract_address: hex::decode(contract_address)
                        .map_err(|e| anyhow::anyhow!("Invalid contract address: {}", e))?,
                    new_code_hash: hex::decode(new_code_hash)
                        .map_err(|e| anyhow::anyhow!("Invalid new_code_hash: {}", e))?,
                    description: description.clone(),
                },
                StorageProposalAction::SlashingParamsUpdate {
                    new_min_bond_amount,
                    new_slash_pct_equivocation,
                    new_slash_pct_double_vote,
                    new_slash_pct_invalid_attestation,
                } => ProposalAction::SlashingParamsUpdate {
                    new_min_bond_amount: Some(new_min_bond_amount.unwrap_or(0)),
                    new_slash_pct_equivocation: Some(new_slash_pct_equivocation.unwrap_or(0)),
                    new_slash_pct_double_vote: Some(new_slash_pct_double_vote.unwrap_or(0)),
                    new_slash_pct_invalid_attestation: Some(
                        new_slash_pct_invalid_attestation.unwrap_or(0),
                    ),
                },
                StorageProposalAction::SetFlPolicy {
                    fee_treasury_bps,
                    max_models,
                    whitelist_aggregators,
                } => ProposalAction::SetFlPolicy {
                    fee_treasury_bps: *fee_treasury_bps,
                    max_models: *max_models,
                    whitelist_aggregators: whitelist_aggregators
                        .iter()
                        .map(|addr| hex::encode(addr))
                        .collect(),
                },
                StorageProposalAction::ApproveFlModel { model_id } => {
                    ProposalAction::ApproveFlModel {
                        model_id: hex::encode(model_id),
                    }
                }
                StorageProposalAction::AbortFlRound { model_id, round_id } => {
                    ProposalAction::AbortFlRound {
                        model_id: hex::encode(model_id),
                        round_id: *round_id,
                    }
                }
                StorageProposalAction::AddConnector {
                    connector_id,
                    pubkey,
                    config,
                } => {
                    if config.len() > MAX_DESERIALIZE_SIZE {
                        anyhow::bail!(
                            "Connector config data too large: {} bytes (max {})",
                            config.len(),
                            MAX_DESERIALIZE_SIZE
                        );
                    }
                    ProposalAction::AddConnector {
                        connector_id: connector_id.clone(),
                        pubkey: pubkey.clone(),
                        config: bincode::deserialize(&config)
                            .map_err(|e| anyhow::anyhow!("Failed to deserialize config: {}", e))?,
                    }
                }
                StorageProposalAction::RemoveConnector { connector_id } => {
                    ProposalAction::RemoveConnector {
                        connector_id: connector_id.clone(),
                    }
                }
                _ => ProposalAction::NonCore {
                    description: "Unknown action type".to_string(),
                },
            },
            status: match storage_proposal.status {
                savitri_storage::storage::ProposalStatus::Pending => ProposalStatus::Pending,
                savitri_storage::storage::ProposalStatus::ActiveVoting => {
                    ProposalStatus::ActiveVoting
                }
                savitri_storage::storage::ProposalStatus::Approved => ProposalStatus::Approved,
                savitri_storage::storage::ProposalStatus::Rejected => ProposalStatus::Rejected,
                // Local governance model has no distinct Executed state; treat it as Approved.
                savitri_storage::storage::ProposalStatus::Executed => ProposalStatus::Approved,
            },
            created_at: storage_proposal.created_at,
            review_end: storage_proposal.review_end,
            voting_end: storage_proposal.voting_end,
            yes_votes: storage_proposal.yes_votes,
            no_votes: storage_proposal.no_votes,
            abstain_votes: storage_proposal.abstain_votes,
        })
    }

    fn governance_proposal_to_storage_proposal(
        proposal: &Proposal,
    ) -> anyhow::Result<savitri_storage::storage::Proposal> {
        Ok(savitri_storage::storage::Proposal {
            id: proposal.id,
            creator: hex::decode(&proposal.creator)
                .map_err(|e| anyhow::anyhow!("Invalid creator: {}", e))?,
            deposit: proposal.deposit,
            description: proposal.description.clone(),
            action: match &proposal.action {
                ProposalAction::FeeVariation {
                    new_base_fee,
                    new_max_fee,
                } => StorageProposalAction::FeeVariation {
                    new_base_fee: Some(new_base_fee.unwrap_or(0)),
                    new_max_fee: Some(new_max_fee.unwrap_or(0)),
                },
                ProposalAction::ProjectSelection {
                    project_address,
                    amount,
                } => StorageProposalAction::ProjectSelection {
                    project_address: hex::decode(project_address)
                        .map_err(|e| anyhow::anyhow!("Invalid project address: {}", e))?,
                    amount: *amount,
                },
                ProposalAction::Standards {
                    standard_name,
                    standard_version,
                } => StorageProposalAction::Standards {
                    standard_name: standard_name.clone(),
                    standard_version: standard_version.clone(),
                },
                ProposalAction::NonCore { description } => StorageProposalAction::NonCore {
                    description: description.clone(),
                },
                ProposalAction::ContractUpgrade {
                    contract_address,
                    new_code_hash,
                    description,
                } => StorageProposalAction::ContractUpgrade {
                    contract_address: hex::decode(contract_address)
                        .map_err(|e| anyhow::anyhow!("Invalid contract address: {}", e))?,
                    new_code_hash: hex::decode(new_code_hash)
                        .map_err(|e| anyhow::anyhow!("Invalid new_code_hash: {}", e))?,
                    description: description.clone(),
                },
                ProposalAction::SlashingParamsUpdate {
                    new_min_bond_amount,
                    new_slash_pct_equivocation,
                    new_slash_pct_double_vote,
                    new_slash_pct_invalid_attestation,
                } => StorageProposalAction::SlashingParamsUpdate {
                    new_min_bond_amount: Some(new_min_bond_amount.unwrap_or(0)),
                    new_slash_pct_equivocation: Some(new_slash_pct_equivocation.unwrap_or(0)),
                    new_slash_pct_double_vote: Some(new_slash_pct_double_vote.unwrap_or(0)),
                    new_slash_pct_invalid_attestation: Some(
                        new_slash_pct_invalid_attestation.unwrap_or(0),
                    ),
                },
                ProposalAction::SetFlPolicy {
                    fee_treasury_bps,
                    max_models,
                    whitelist_aggregators,
                } => StorageProposalAction::SetFlPolicy {
                    fee_treasury_bps: *fee_treasury_bps,
                    max_models: *max_models,
                    whitelist_aggregators: whitelist_aggregators
                        .iter()
                        .map(|addr| hex::decode(addr).unwrap_or_default())
                        .collect(),
                },
                ProposalAction::ApproveFlModel { model_id } => {
                    StorageProposalAction::ApproveFlModel {
                        model_id: hex::decode(model_id)
                            .map_err(|e| anyhow::anyhow!("Invalid model_id: {}", e))?,
                    }
                }
                ProposalAction::AbortFlRound { model_id, round_id } => {
                    StorageProposalAction::AbortFlRound {
                        model_id: hex::decode(model_id)
                            .map_err(|e| anyhow::anyhow!("Invalid model_id: {}", e))?,
                        round_id: *round_id,
                    }
                }
                ProposalAction::AddConnector {
                    connector_id,
                    pubkey,
                    config,
                } => StorageProposalAction::AddConnector {
                    connector_id: connector_id.clone(),
                    pubkey: pubkey.clone(),
                    config: bincode::serialize(&config)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize config: {}", e))?,
                },
                ProposalAction::RemoveConnector { connector_id } => {
                    StorageProposalAction::RemoveConnector {
                        connector_id: connector_id.clone(),
                    }
                }
            },
            status: match proposal.status {
                ProposalStatus::Pending => savitri_storage::storage::ProposalStatus::Pending,
                ProposalStatus::ActiveVoting => {
                    savitri_storage::storage::ProposalStatus::ActiveVoting
                }
                ProposalStatus::Approved => savitri_storage::storage::ProposalStatus::Approved,
                ProposalStatus::Rejected => savitri_storage::storage::ProposalStatus::Rejected,
            },
            created_at: proposal.created_at,
            review_end: proposal.review_end,
            voting_end: proposal.voting_end,
            yes_votes: proposal.yes_votes,
            no_votes: proposal.no_votes,
            abstain_votes: proposal.abstain_votes,
        })
    }
}

impl Default for ProposalSystem {
    fn default() -> Self {
        Self::new()
    }
}
