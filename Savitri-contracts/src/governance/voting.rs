//! Voting System: Sistema di votazione con vote token
//!
//! - Votazione Yes/No/Abstain
//! - Calcolo quorum (20% dei vote token totali — raised from 10% per LOW-04)
//! - Calcolo approval (65% dei voti Yes)
//! - Lock dei vote token durante la votazione

use crate::governance::proposals::{Proposal, ProposalStatus};
use metrics::{counter, gauge};
use savitri_storage::storage::{ProposalAction as StorageProposalAction, Storage};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum allowed size for deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads from storage.
const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;
/// Tipo di voto
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteType {
    Yes,
    No,
    Abstain,
}

/// Risultato dettagliato di una votazione
///
/// il risultato di una votazione.
#[derive(Debug, Clone)]
pub struct VotingResult {
    pub result: ProposalStatus,
    pub quorum_reached: bool,
    /// Indica se l'approval è stato raggiunto
    pub approval_reached: bool,
    /// Threshold minimo per il quorum (20% dei vote token totali)
    pub quorum_threshold: u128,
    /// Totale dei vote token used per votare
    pub total_votes: u128,
    /// Totale dei vote token in circolazione
    pub total_vote_tokens: u128,
    /// Totale dei voti "Yes"
    pub yes_votes: u128,
    /// Totale dei voti "No"
    pub no_votes: u128,
    /// Totale dei voti "Abstain"
    pub abstain_votes: u128,
    /// Threshold minimo per l'approval (65% dei voti validi)
    pub approval_threshold: u128,
    /// Percentuale di approval raggiunta
    pub approval_percentage: f64,
}

/// Voto su una proposta
pub struct Vote {
    pub proposal_id: u64,
    pub voter: String,
    pub vote_type: VoteType,
    pub vote_amount: u128, // Quantità di vote token used
}

/// Sistema di votazione
pub struct VotingSystem;

impl VotingSystem {
    pub fn new() -> Self {
        Self
    }

    /// Registra un voto su una proposta
    ///
    /// 1. Check che la proposta sia in ActiveVoting
    /// 2. Check che il voter abbia abbastanza vote token
    /// 3. Prevent doppio voto (un voter può votare solo una volta per proposta)
    /// 4. Lock i vote token used per votare
    /// 5. Registra il voto in the storage
    ///
    /// # Argomenti
    /// - `vote_type`: Tipo di voto (Yes, No, Abstain)
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `ProposalNotFound`: La proposta non esiste
    /// - `ProposalNotActive`: La proposta non è in ActiveVoting
    /// - `InsufficientBalance`: Il voter non ha abbastanza vote token
    /// - `InvalidVoteAmount`: Il vote_amount è zero o troppo grande
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::{VotingSystem, VoteType};
    /// use crate::storage::Storage;
    /// let voting = VotingSystem::new();
    /// let storage = Storage::new("db")?;
    /// let voter = [0u8; 32];
    /// voting.vote(&storage, 1, &voter, VoteType::Yes, 100)?;
    /// ```
    pub fn vote(
        &self,
        storage: &Storage,
        proposal_id: u64,
        voter: &[u8; 32],
        vote_type: VoteType,
        vote_amount: u128,
    ) -> anyhow::Result<()> {
        if vote_amount == 0 {
            anyhow::bail!("Vote amount deve essere maggiore di zero");
        }

        let proposal_data = storage
            .get_proposal(proposal_id)?
            .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
        if proposal_data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                proposal_data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let storage_proposal: savitri_storage::storage::Proposal =
            bincode::deserialize(&proposal_data)?;

        // 3. Check che la proposta sia in ActiveVoting
        use savitri_storage::storage::ProposalStatus as StorageProposalStatus;
        if storage_proposal.status != StorageProposalStatus::ActiveVoting {
            anyhow::bail!(
                "Proposta non in votazione attiva: stato corrente è {:?}",
                storage_proposal.status
            );
        }

        // 4. Check che la proposta non sia scaduta
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        if now >= storage_proposal.voting_end {
            anyhow::bail!("Proposta scaduta: voting period terminato");
        }

        if storage.get_vote(voter, proposal_id)?.is_some() {
            anyhow::bail!("Voter ha già votato su questa proposta");
        }

        // 6. Check che il voter abbia abbastanza vote token disponibili
        let available_tokens = storage.get_available_vote_tokens(voter)?;
        if available_tokens < vote_amount {
            let locked = storage.get_locked_vote_tokens(voter)?;
            let total_balance = storage.get_vote_token_balance(voter)?;
            anyhow::bail!(
                "Vote token disponibili insufficienti: voter ha {} vote token totali ({} locked in altre proposte), disponibili {}, richiesti {}",
                total_balance,
                locked,
                available_tokens,
                vote_amount
            );
        }

        // 7. Lock i vote token (decrementa il balance)
        storage.decrement_vote_token_balance(voter, vote_amount)?;

        // 8. Creates il voto per lo storage
        use crate::storage::{Vote as StorageVote, VoteType as StorageVoteType};
        let storage_vote = StorageVote {
            proposal_id,
            voter: voter.to_vec(),
            vote_type: match vote_type {
                VoteType::Yes => StorageVoteType::Yes,
                VoteType::No => StorageVoteType::No,
                VoteType::Abstain => StorageVoteType::Abstain,
            },
            vote_amount,
            timestamp: now,
        };

        // 9. Registra il voto in the storage
        let vote_data = bincode::serialize(&storage_vote)?;
        storage.put_vote(voter, proposal_id, &vote_data)?;

        let mut updated_proposal = storage_proposal.clone();
        match vote_type {
            VoteType::Yes => {
                updated_proposal.yes_votes = updated_proposal
                    .yes_votes
                    .checked_add(vote_amount)
                    .ok_or_else(|| anyhow::anyhow!("yes_votes overflow"))?;
            }
            VoteType::No => {
                updated_proposal.no_votes = updated_proposal
                    .no_votes
                    .checked_add(vote_amount)
                    .ok_or_else(|| anyhow::anyhow!("no_votes overflow"))?;
            }
            VoteType::Abstain => {
                updated_proposal.abstain_votes = updated_proposal
                    .abstain_votes
                    .checked_add(vote_amount)
                    .ok_or_else(|| anyhow::anyhow!("abstain_votes overflow"))?;
            }
        }

        // 11. Salva la proposta aggiornata
        let proposal_data = bincode::serialize(&updated_proposal)?;
        storage.put_proposal(proposal_id, &proposal_data)?;

        // 12. Update governance metrics
        counter!("governance_votes_total").increment(1);
        gauge!("governance_vote_tokens_locked").set(vote_amount as f64);

        Ok(())
    }

    /// Unlock i vote token quando una proposta è finalizzata
    ///
    /// Quando una proposta è finalizzata (Approved o Rejected), i vote token
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    /// `Ok(())` se l'unlock è riuscito, `Err` in caso di errore.
    ///
    /// # Note
    /// (approvata o negata) per sbloccare i vote token used per votare.
    /// I vote token are restituiti al balance of the voter.
    pub fn unlock_vote_tokens(&self, storage: &Storage, proposal_id: u64) -> anyhow::Result<()> {
        use savitri_storage::storage::ProposalStatus as StorageProposalStatus;

        let proposal_data = storage
            .get_proposal(proposal_id)?
            .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
        if proposal_data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                proposal_data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let proposal: savitri_storage::storage::Proposal = bincode::deserialize(&proposal_data)?;

        // 2. Check che la proposta sia finalizzata
        if !matches!(
            proposal.status,
            StorageProposalStatus::Approved | StorageProposalStatus::Rejected
        ) {
            anyhow::bail!(
                "Proposta non finalizzata: stato corrente è {:?}",
                proposal.status
            );
        }

        let votes_data = storage.get_proposal_votes(proposal_id)?;
        let mut votes: Vec<savitri_storage::storage::governance::Vote> = Vec::new();
        for vote_bytes in votes_data {
            if vote_bytes.len() > MAX_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Vote data too large: {} bytes (max {})",
                    vote_bytes.len(),
                    MAX_DESERIALIZE_SIZE
                );
            }
            votes.push(bincode::deserialize(&vote_bytes)?);
        }

        for vote in votes {
            // Restituisci i vote token al balance of the voter
            storage.increment_vote_token_balance(&vote.voter, vote.vote_amount)?;
        }

        Ok(())
    }

    ///
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    pub fn get_locked_vote_tokens(
        &self,
        storage: &Storage,
        voter: &[u8; 32],
    ) -> anyhow::Result<u128> {
        storage.get_locked_vote_tokens(voter)
    }

    ///
    /// su nuove proposte (balance totale - vote token locked).
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    pub fn get_available_vote_tokens(
        &self,
        storage: &Storage,
        voter: &[u8; 32],
    ) -> anyhow::Result<u128> {
        storage.get_available_vote_tokens(voter)
    }

    ///
    ///
    /// # Argomenti
    /// - `vote`: Struttura Vote contenente i dati of the voto
    ///
    /// # Ritorna
    #[deprecated(note = "Use vote() instead")]
    pub fn vote_with_struct(&self, storage: &Storage, vote: Vote) -> anyhow::Result<()> {
        // Converti voter da String a [u8; 32]
        let voter_bytes = hex::decode(&vote.voter)
            .map_err(|e| anyhow::anyhow!("Invalid voter address: {}", e))?;
        if voter_bytes.len() != 32 {
            anyhow::bail!("Voter address deve essere 32 bytes");
        }
        let mut voter = [0u8; 32];
        voter.copy_from_slice(&voter_bytes);

        self.vote(
            storage,
            vote.proposal_id,
            &voter,
            vote.vote_type,
            vote.vote_amount,
        )
    }

    /// Compute il quorum minimo richiesto per una proposta
    ///
    /// Il quorum è definito come almeno il 20% dei vote token totali in circolazione.
    ///
    /// # Security (LOW-04)
    /// Raised from 10% to 20% to mitigate governance-capture attacks: a 10% quorum
    /// allows a well-resourced minority to pass proposals when most holders are inactive.
    /// 20% requires broader participation for legitimate governance outcomes.
    ///
    /// # Argomenti
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    /// Il numero minimo di vote token che devono votare per raggiungere il quorum.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// let quorum = voting.calculate_quorum(1000);
    /// assert_eq!(quorum, 200); // 20% di 1000
    /// ```
    pub fn calculate_quorum(&self, total_vote_tokens: u128) -> u128 {
        // Calcolo preciso: quorum = total_vote_tokens * 20 / 100 = total / 5
        total_vote_tokens.checked_div(5).unwrap_or(u128::MAX) // Overflow protection
    }

    ///
    /// è almeno il 20% dei vote token totali in circolazione.
    ///
    /// # Argomenti
    /// - `total_votes`: Il totale dei vote token used per votare (yes + no + abstain)
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 1000 vote token totali, il quorum è 200
    /// assert!(voting.is_quorum_reached(200, 1000)); // Quorum raggiunto
    /// assert!(!voting.is_quorum_reached(199, 1000)); // Quorum non raggiunto
    /// ```
    pub fn is_quorum_reached(&self, total_votes: u128, total_vote_tokens: u128) -> bool {
        if total_vote_tokens == 0 {
            return false;
        }

        let quorum_threshold = self.calculate_quorum(total_vote_tokens);
        total_votes >= quorum_threshold
    }

    ///
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da verificare
    /// - `storage`: Storage per ottenere il totale dei vote token
    ///
    /// # Ritorna
    ///
    /// # Note
    /// dalla proposta e ottiene il totale dei vote token dallo storage.
    pub fn check_proposal_quorum(
        &self,
        proposal: &Proposal,
        storage: &Storage,
    ) -> anyhow::Result<bool> {
        let total_votes = proposal.total_votes();
        let total_vote_tokens = storage.get_total_vote_tokens()?;
        Ok(self.is_quorum_reached(total_votes, total_vote_tokens))
    }

    ///
    /// e compute il quorum minimo richiesto.
    ///
    /// # Argomenti
    /// - `storage`: Storage per ottenere il totale dei vote token
    ///
    /// # Ritorna
    pub fn calculate_quorum_from_storage(&self, storage: &Storage) -> anyhow::Result<u128> {
        let total_vote_tokens = storage.get_total_vote_tokens()?;
        Ok(self.calculate_quorum(total_vote_tokens))
    }

    /// Compute la percentuale di partecipazione per una proposta
    ///
    /// La percentuale di partecipazione è calcolata come:
    /// `(total_votes / total_vote_tokens) * 100`
    ///
    /// # Argomenti
    /// - `total_votes`: Il totale dei vote token used per votare
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 100 voti su 1000 vote token totali, la partecipazione è 10% (sotto quorum of the 20%)
    /// let participation = voting.calculate_participation_percentage(100, 1000);
    /// assert_eq!(participation, 10.0);
    /// ```
    pub fn calculate_participation_percentage(
        &self,
        total_votes: u128,
        total_vote_tokens: u128,
    ) -> f64 {
        if total_vote_tokens == 0 {
            return 0.0;
        }

        // Compute percentuale: (total_votes / total_vote_tokens) * 100
        (total_votes as f64 / total_vote_tokens as f64) * 100.0
    }

    /// Compute quanto manca per raggiungere il quorum
    ///
    /// per raggiungere il quorum minimo.
    ///
    /// # Argomenti
    /// - `total_votes`: Il totale dei vote token used per votare
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    /// Il numero di vote token mancanti per raggiungere il quorum.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 1000 vote token totali, il quorum è 100
    /// let missing = voting.calculate_missing_for_quorum(80, 1000);
    /// assert_eq!(missing, 20);
    /// ```
    pub fn calculate_missing_for_quorum(&self, total_votes: u128, total_vote_tokens: u128) -> u128 {
        if total_vote_tokens == 0 {
            return 0; // Nessun vote token disponibile
        }

        let quorum_threshold = self.calculate_quorum(total_vote_tokens);

        if total_votes >= quorum_threshold {
            0 // Quorum già raggiunto
        } else {
            quorum_threshold.checked_sub(total_votes).unwrap_or(0) // Overflow protection
        }
    }

    /// Compute l'approval come percentuale (65% dei voti Yes)
    ///
    /// Il calcolo considera solo voti validi (Yes + No), escludendo Abstain.
    /// La percentuale è calcolata come: `(yes_votes / total_valid_votes) * 100`
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 65 voti Yes e 35 voti No, l'approval è 65%
    /// let approval = voting.calculate_approval_percentage(65, 35);
    /// assert_eq!(approval, 65.0);
    /// ```
    pub fn calculate_approval_percentage(&self, yes_votes: u128, no_votes: u128) -> f64 {
        let total_valid_votes = yes_votes.checked_add(no_votes).unwrap_or(u128::MAX);

        if total_valid_votes == 0 {
            return 0.0;
        }

        // Compute percentuale: (yes_votes / total_valid_votes) * 100
        (yes_votes as f64 / total_valid_votes as f64) * 100.0
    }

    /// Compute l'approval threshold minimo richiesto (65% dei voti validi)
    ///
    /// il 65% di approval, dato un certo numero di voti validi totali.
    ///
    /// # Argomenti
    /// - `total_valid_votes`: Il totale dei voti validi (Yes + No, escludendo Abstain)
    ///
    /// # Ritorna
    /// Il numero minimo di voti "Yes" necessari per raggiungere il 65% di approval.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 100 voti validi totali, servono almeno 65 voti Yes per approval
    /// let threshold = voting.calculate_approval_threshold(100);
    /// assert_eq!(threshold, 65);
    /// ```
    pub fn calculate_approval_threshold(&self, total_valid_votes: u128) -> u128 {
        // Calcolo preciso: approval_threshold = (total_valid_votes * 65) / 100
        total_valid_votes
            .checked_mul(65)
            .and_then(|v| v.checked_div(100))
            .unwrap_or(u128::MAX) // Overflow protection
    }

    ///
    /// Il calcolo considera solo voti validi (Yes + No), escludendo Abstain.
    /// L'approval è raggiunto quando: `yes_votes >= (total_valid_votes * 65) / 100`
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    ///
    /// # Ritorna
    /// `true` se l'approval è raggiunto (>= 65%), `false` altrimenti.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 65 voti Yes e 35 voti No, l'approval è raggiunto
    /// assert!(voting.is_approval_reached(65, 35)); // 65% >= 65%
    /// assert!(!voting.is_approval_reached(64, 36)); // 64% < 65%
    /// ```
    pub fn is_approval_reached(&self, yes_votes: u128, no_votes: u128) -> bool {
        let total_valid_votes = yes_votes.checked_add(no_votes).unwrap_or(u128::MAX);

        if total_valid_votes == 0 {
            return false; // Nessun voto valido, approval non raggiunto
        }

        let approval_threshold = self.calculate_approval_threshold(total_valid_votes);
        yes_votes >= approval_threshold
    }

    ///
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    ///
    /// # Ritorna
    /// Il rapporto di approval (0.0 - 1.0), dove 1.0 = 100%
    #[deprecated(note = "Use calculate_approval_percentage instead")]
    pub fn calculate_approval(&self, yes_votes: u128, no_votes: u128) -> f64 {
        self.calculate_approval_percentage(yes_votes, no_votes) / 100.0
    }

    /// Check se una proposta è approvata (quorum + approval)
    ///
    /// Una proposta è approvata se:
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    /// - `total_votes`: Il totale dei vote token used per votare (yes + no + abstain)
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Quorum raggiunto (100/1000) e approval raggiunto (65/100)
    /// assert!(voting.is_approved(65, 35, 100, 1000));
    /// ```
    pub fn is_approved(
        &self,
        yes_votes: u128,
        no_votes: u128,
        total_votes: u128,
        total_vote_tokens: u128,
    ) -> bool {
        // Check quorum
        if !self.is_quorum_reached(total_votes, total_vote_tokens) {
            return false;
        }

        self.is_approval_reached(yes_votes, no_votes)
    }

    ///
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da verificare
    ///
    /// # Ritorna
    /// `true` se l'approval è raggiunto (>= 65%), `false` altrimenti.
    ///
    /// # Note
    pub fn check_proposal_approval(&self, proposal: &Proposal) -> bool {
        self.is_approval_reached(proposal.yes_votes, proposal.no_votes)
    }

    ///
    /// e il totale dei vote token ottenuto dallo storage.
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da verificare
    /// - `storage`: Storage per ottenere il totale dei vote token
    ///
    /// # Ritorna
    ///
    /// # Note
    /// dalla proposta e ottiene il totale dei vote token dallo storage.
    pub fn check_proposal_approved(
        &self,
        proposal: &Proposal,
        storage: &Storage,
    ) -> anyhow::Result<bool> {
        let total_votes = proposal.total_votes();
        let total_vote_tokens = storage.get_total_vote_tokens()?;
        Ok(self.is_approved(
            proposal.yes_votes,
            proposal.no_votes,
            total_votes,
            total_vote_tokens,
        ))
    }

    /// Compute quanto manca per raggiungere l'approval
    ///
    /// per raggiungere il 65% di approval.
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    ///
    /// # Ritorna
    /// Il numero di voti "Yes" mancanti per raggiungere l'approval.
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// let voting = VotingSystem::new();
    /// // Con 100 voti validi totali, servono 65 voti Yes per approval
    /// let missing = voting.calculate_missing_for_approval(60, 40);
    /// assert_eq!(missing, 5);
    /// ```
    pub fn calculate_missing_for_approval(&self, yes_votes: u128, no_votes: u128) -> u128 {
        let total_valid_votes = yes_votes.checked_add(no_votes).unwrap_or(u128::MAX);

        if total_valid_votes == 0 {
            return 0; // Nessun voto valido
        }

        let approval_threshold = self.calculate_approval_threshold(total_valid_votes);

        if yes_votes >= approval_threshold {
            0 // Approval già raggiunto
        } else {
            approval_threshold.checked_sub(yes_votes).unwrap_or(0) // Overflow protection
        }
    }

    /// Compute il risultato finale di una proposta (deterministico)
    ///
    /// 1. **Check quorum**: Almeno 20% dei vote token totali devono aver votato
    ///
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da valutare
    /// - `storage`: Storage per ottenere il totale dei vote token
    ///
    /// # Ritorna
    /// `Ok(ProposalStatus::Approved)` se approvata, `Ok(ProposalStatus::Rejected)` se negata,
    /// `Err` in caso di errore
    ///
    /// # Note
    /// Usa aritmetica intera per le verifiche critiche (quorum e approval threshold).
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// use crate::governance::proposals::Proposal;
    /// use crate::storage::Storage;
    /// let voting = VotingSystem::new();
    /// let storage = Storage::new("db")?;
    /// let proposal = storage.get_proposal(1)?;
    /// let result = voting.calculate_proposal_result(&proposal, &storage)?;
    /// ```
    pub fn calculate_proposal_result(
        &self,
        proposal: &Proposal,
        storage: &Storage,
    ) -> anyhow::Result<ProposalStatus> {
        let total_votes = proposal.total_votes();
        let total_vote_tokens = storage.get_total_vote_tokens()?;
        let yes_votes = proposal.yes_votes;
        let no_votes = proposal.no_votes;

        // 1. Check quorum (20% dei vote token totali)
        let quorum_reached = self.is_quorum_reached(total_votes, total_vote_tokens);

        if !quorum_reached {
            // Quorum non raggiunto → Rejected
            return Ok(ProposalStatus::Rejected);
        }

        let approval_reached = self.is_approval_reached(yes_votes, no_votes);

        let result = if quorum_reached && approval_reached {
            ProposalStatus::Approved
        } else {
            ProposalStatus::Rejected
        };

        Ok(result)
    }

    ///
    ///
    /// # Argomenti
    /// - `storage`: Storage per ottenere la proposta e il totale dei vote token
    ///
    /// # Ritorna
    /// `Ok(ProposalStatus::Approved)` se approvata, `Ok(ProposalStatus::Rejected)` se negata,
    /// `Err` in caso di errore (proposta non trovata, etc.)
    ///
    /// # Note
    /// e of the modulo governance.
    pub fn calculate_proposal_result_from_storage(
        &self,
        storage: &Storage,
        proposal_id: u64,
    ) -> anyhow::Result<ProposalStatus> {
        let proposal_data = storage
            .get_proposal(proposal_id)?
            .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
        if proposal_data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                proposal_data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let storage_proposal: savitri_storage::storage::Proposal =
            bincode::deserialize(&proposal_data)?;

        let total_votes = storage_proposal
            .yes_votes
            .checked_add(storage_proposal.no_votes)
            .and_then(|sum| sum.checked_add(storage_proposal.abstain_votes))
            .ok_or_else(|| anyhow::anyhow!("total votes overflow"))?;

        let total_vote_tokens = storage.get_total_vote_tokens()?;
        let yes_votes = storage_proposal.yes_votes;
        let no_votes = storage_proposal.no_votes;

        // 3. Check quorum (20% dei vote token totali)
        let quorum_reached = self.is_quorum_reached(total_votes, total_vote_tokens);

        if !quorum_reached {
            // Quorum non raggiunto → Rejected
            return Ok(ProposalStatus::Rejected);
        }

        let approval_reached = self.is_approval_reached(yes_votes, no_votes);

        Ok(if approval_reached {
            ProposalStatus::Approved
        } else {
            ProposalStatus::Rejected
        })
    }

    ///
    /// su quorum, approval, e altre metriche utili per reporting e debugging.
    ///
    /// # Argomenti
    /// - `proposal`: La proposta da valutare
    /// - `storage`: Storage per ottenere il totale dei vote token
    ///
    /// # Ritorna
    /// Una struttura `VotingResult` contenente:
    /// - Risultato finale (Approved/Rejected)
    /// - Quorum raggiunto (true/false)
    /// - Approval raggiunto (true/false)
    /// - Metriche dettagliate (quorum threshold, approval threshold, etc.)
    pub fn calculate_detailed_result(
        &self,
        proposal: &Proposal,
        storage: &Storage,
    ) -> anyhow::Result<VotingResult> {
        let total_votes = proposal.total_votes();
        let total_vote_tokens = storage.get_total_vote_tokens()?;
        let yes_votes = proposal.yes_votes;
        let no_votes = proposal.no_votes;
        let abstain_votes = proposal.abstain_votes;

        // Compute quorum
        let quorum_threshold = self.calculate_quorum(total_vote_tokens);
        let quorum_reached = self.is_quorum_reached(total_votes, total_vote_tokens);

        // Compute approval
        let total_valid_votes = yes_votes.checked_add(no_votes).unwrap_or(u128::MAX);
        let approval_threshold = self.calculate_approval_threshold(total_valid_votes);
        let approval_reached = self.is_approval_reached(yes_votes, no_votes);
        let approval_percentage = self.calculate_approval_percentage(yes_votes, no_votes);

        let result = if quorum_reached && approval_reached {
            ProposalStatus::Approved
        } else {
            ProposalStatus::Rejected
        };

        Ok(VotingResult {
            result,
            quorum_reached,
            approval_reached,
            quorum_threshold,
            total_votes,
            total_vote_tokens,
            yes_votes,
            no_votes,
            abstain_votes,
            approval_threshold,
            approval_percentage,
        })
    }

    /// Processa le proposte scadute e compute i risultati automaticamente
    ///
    /// 1. Transizione automatica da Pending a ActiveVoting dopo il review period (24h)
    /// 2. Calcolo automatico of the risultato dopo il voting period (7 giorni)
    ///
    /// # Argomenti
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `Ok(processed_count)` dove `processed_count` è il numero di proposte processate,
    /// `Err` in caso di errore
    ///
    /// # Note
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::voting::VotingSystem;
    /// use crate::storage::Storage;
    /// use std::time::{SystemTime, UNIX_EPOCH};
    /// let voting = VotingSystem::new();
    /// let storage = Storage::new("db")?;
    /// let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    /// let processed = voting.process_expired_proposals(&storage, now)?;
    /// ```
    pub fn process_expired_proposals(
        &self,
        storage: &Storage,
        current_timestamp: u64,
    ) -> anyhow::Result<usize> {
        use savitri_storage::storage::ProposalStatus as StorageProposalStatus;

        // SECURITY FIX: Limit batch size to prevent DoS from unbounded iteration
        // over thousands of proposals. Process at most 100 per call.
        const MAX_PROPOSALS_PER_BATCH: usize = 100;

        let all_proposals = storage.get_all_proposals()?;
        let mut processed_count = 0;

        for proposal_id in all_proposals.into_iter().take(MAX_PROPOSALS_PER_BATCH) {
            let proposal_data = storage
                .get_proposal(proposal_id)?
                .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
            if proposal_data.len() > MAX_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Proposal data too large: {} bytes (max {})",
                    proposal_data.len(),
                    MAX_DESERIALIZE_SIZE
                );
            }
            let mut proposal: savitri_storage::storage::Proposal =
                bincode::deserialize(&proposal_data)?;
            let mut updated = false;

            // 2. Gestisci transizione Pending -> ActiveVoting
            if proposal.status == StorageProposalStatus::Pending {
                if current_timestamp >= proposal.review_end {
                    proposal.status = StorageProposalStatus::ActiveVoting;
                    updated = true;
                }
            }

            // 3. Gestisci calcolo risultato per proposte ActiveVoting scadute
            if proposal.status == StorageProposalStatus::ActiveVoting {
                if current_timestamp >= proposal.voting_end {
                    let result =
                        self.calculate_proposal_result_from_storage(storage, proposal.id)?;

                    proposal.status = match result {
                        ProposalStatus::Approved => StorageProposalStatus::Approved,
                        ProposalStatus::Rejected => StorageProposalStatus::Rejected,
                        _ => {
                            StorageProposalStatus::Rejected
                        }
                    };
                    updated = true;
                    processed_count += 1;

                    // 4. Gestisci deposit in base al risultato
                    use crate::governance::proposals::{
                        Proposal as GovernanceProposal, ProposalAction,
                    };

                    let gov_proposal = GovernanceProposal::new_with_timestamp(
                        proposal.id,
                        hex::encode(&proposal.creator),
                        proposal.deposit,
                        proposal.description.clone(),
                        match &proposal.action {
                            StorageProposalAction::NonCore { description } => {
                                ProposalAction::NonCore {
                                    description: description.clone(),
                                }
                            }
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
                            StorageProposalAction::ContractUpgrade {
                                contract_address,
                                new_code_hash,
                                description,
                            } => ProposalAction::ContractUpgrade {
                                contract_address: contract_address.clone(),
                                new_code_hash: new_code_hash.clone(),
                                description: description.clone(),
                            },
                            StorageProposalAction::SlashingParamsUpdate {
                                new_min_bond_amount,
                                new_slash_pct_equivocation,
                                new_slash_pct_double_vote,
                                new_slash_pct_invalid_attestation,
                            } => ProposalAction::SlashingParamsUpdate {
                                new_min_bond_amount: Some(new_min_bond_amount.unwrap_or(0)),
                                new_slash_pct_equivocation: Some(
                                    new_slash_pct_equivocation.unwrap_or(0),
                                ),
                                new_slash_pct_double_vote: Some(
                                    new_slash_pct_double_vote.unwrap_or(0),
                                ),
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
                                    .map(|a| hex::encode(a))
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
                                    config: bincode::deserialize(&config).map_err(|e| {
                                        anyhow::anyhow!("Failed to deserialize config: {}", e)
                                    })?,
                                }
                            }
                            StorageProposalAction::RemoveConnector { connector_id } => {
                                ProposalAction::RemoveConnector {
                                    connector_id: connector_id.clone(),
                                }
                            }
                            StorageProposalAction::SetBondParams { min_bond, max_bond } => {
                                ProposalAction::SlashingParamsUpdate {
                                    new_min_bond_amount: Some(min_bond.or(*max_bond).unwrap_or(0)),
                                    new_slash_pct_equivocation: None,
                                    new_slash_pct_double_vote: None,
                                    new_slash_pct_invalid_attestation: None,
                                }
                            }
                            StorageProposalAction::SetSlashParams {
                                equivocation_pct,
                                double_vote_pct,
                                invalid_attestation_pct,
                            } => ProposalAction::SlashingParamsUpdate {
                                new_min_bond_amount: None,
                                new_slash_pct_equivocation: Some(equivocation_pct.unwrap_or(0)),
                                new_slash_pct_double_vote: Some(double_vote_pct.unwrap_or(0)),
                                new_slash_pct_invalid_attestation: Some(
                                    invalid_attestation_pct.unwrap_or(0),
                                ),
                            },
                        },
                        proposal.created_at,
                    );

                    let mut gov_proposal = gov_proposal;
                    gov_proposal.status = match result {
                        ProposalStatus::Approved => ProposalStatus::Approved,
                        ProposalStatus::Rejected => ProposalStatus::Rejected,
                        // If the status is Pending or ActiveVoting, keep the current status
                        ProposalStatus::Pending | ProposalStatus::ActiveVoting => {
                            gov_proposal.status
                        }
                    };

                    // Gestisci deposit e vote token in base al risultato
                    match result {
                        ProposalStatus::Approved => {
                            // Unlock deposit if approved
                            use crate::governance::deposit::DepositManager;
                            let deposit_manager = DepositManager::new();
                            deposit_manager.unlock_deposit(storage, &gov_proposal)?;

                            // SECURITY FIX: Unlock vote token used per votare
                            // Without questo, i vote token restano lockati permanentemente
                            // degradando progressivamente la capacità di governance.
                            self.unlock_vote_tokens(storage, proposal.id)?;
                        }
                        ProposalStatus::Rejected => {
                            // Burn deposit if rejected
                            use crate::governance::deposit::DepositManager;
                            let deposit_manager = DepositManager::new();
                            deposit_manager.burn_deposit(storage, &gov_proposal)?;

                            // Unlock i vote token used per votare quando la proposta è finalizzata
                            self.unlock_vote_tokens(storage, proposal.id)?;
                        }
                        _ => {
                            // Nessuna azione per altri stati
                        }
                    }
                } // Chiude if current_timestamp >= proposal.voting_end
            } // Chiude if proposal.status == StorageProposalStatus::ActiveVoting

            // Salva la proposta aggiornata se è stata modificata
            if updated {
                let proposal_data = bincode::serialize(&proposal)?;
                storage.put_proposal(proposal.id, &proposal_data)?;
            }
        }

        Ok(processed_count)
    }

    ///
    ///
    /// # Argomenti
    ///
    /// # Ritorna
    /// `Ok(processed_count)` dove `processed_count` è il numero di proposte processate,
    /// `Err` in caso di errore
    pub fn process_expired_proposals_now(&self, storage: &Storage) -> anyhow::Result<usize> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.process_expired_proposals(storage, now)
    }

    /// Check se una proposta è nel voting period (ActiveVoting e non scaduta)
    ///
    /// Una proposta è nel voting period se:
    /// - È in stato ActiveVoting
    /// - Il review_end è passato
    /// - Il voting_end non è ancora passato
    ///
    /// # Argomenti
    /// - `storage`: Storage per ottenere la proposta
    /// - `current_timestamp`: Timestamp corrente (in secondi dall'epoch Unix)
    ///
    /// # Ritorna
    /// `Err` in caso di errore
    pub fn is_in_voting_period(
        &self,
        storage: &Storage,
        proposal_id: u64,
        current_timestamp: u64,
    ) -> anyhow::Result<bool> {
        use savitri_storage::storage::ProposalStatus as StorageProposalStatus;

        let proposal_data = storage
            .get_proposal(proposal_id)?
            .ok_or_else(|| anyhow::anyhow!("Proposta {} non trovata", proposal_id))?;
        if proposal_data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Proposal data too large: {} bytes (max {})",
                proposal_data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        let proposal: savitri_storage::storage::Proposal = bincode::deserialize(&proposal_data)?;

        Ok(proposal.status == StorageProposalStatus::ActiveVoting
            && current_timestamp >= proposal.review_end
            && current_timestamp < proposal.voting_end)
    }

    ///
    ///
    /// # Argomenti
    /// - `storage`: Storage per ottenere la proposta
    ///
    /// # Ritorna
    /// `Err` in caso di errore
    pub fn is_in_voting_period_now(
        &self,
        storage: &Storage,
        proposal_id: u64,
    ) -> anyhow::Result<bool> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.is_in_voting_period(storage, proposal_id, now)
    }

    ///
    ///
    /// # Argomenti
    /// - `yes_votes`: Il totale dei vote token votati "Yes"
    /// - `no_votes`: Il totale dei vote token votati "No"
    /// - `total_votes`: Il totale dei vote token used per votare
    /// - `total_vote_tokens`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    /// `ProposalStatus::Approved` se approvata, `ProposalStatus::Rejected` se negata
    #[deprecated(note = "Use calculate_proposal_result instead")]
    pub fn calculate_result(
        &self,
        _proposal: &Proposal,
        yes_votes: u128,
        no_votes: u128,
        total_votes: u128,
        total_vote_tokens: u128,
    ) -> ProposalStatus {
        if self.is_approved(yes_votes, no_votes, total_votes, total_vote_tokens) {
            ProposalStatus::Approved
        } else {
            ProposalStatus::Rejected
        }
    }
}

impl Default for VotingSystem {
    fn default() -> Self {
        Self::new()
    }
}
