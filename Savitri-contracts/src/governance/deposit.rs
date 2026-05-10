//!
//! - Lock dei vote token durante la proposta
//! - Unlock se approvata (returns i vote token al creator)
//! - Burn se negata (i vote token are bruciati, già sottratti durante il lock)

use crate::governance::proposals::{Proposal, ProposalStatus};
use anyhow::Context;
use savitri_storage::storage::Storage;

/// Manager dei deposit
pub struct DepositManager;

impl DepositManager {
    pub fn new() -> Self {
        Self
    }

    /// Lock dei vote token per il deposit
    ///
    /// # Argomenti
    /// - `amount`: Quantità di vote token da lockare
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - `InsufficientBalance`: Il creator non ha abbastanza vote token
    /// - Altri errori di storage
    ///
    /// # Note
    pub fn lock_deposit(
        &self,
        storage: &Storage,
        creator: &[u8; 32],
        amount: u128,
    ) -> anyhow::Result<()> {
        if amount == 0 {
            anyhow::bail!("Deposit amount must be greater than zero");
        }

        // Check che il creator abbia abbastanza vote token
        let creator_balance = storage.get_vote_token_balance(creator)?;
        if creator_balance < amount {
            anyhow::bail!(
                "Balance insufficiente: creator ha {} vote token, richiesti {}",
                creator_balance,
                amount
            );
        }

        // Lock i vote token (decrementa il balance)
        storage
            .decrement_vote_token_balance(creator, amount)
            .context("Errore durante il lock of the deposit")?;

        Ok(())
    }

    /// Unlock dei vote token se proposta approvata
    ///
    /// # Argomenti
    /// - `proposal`: La proposta per cui sbloccare il deposit
    ///
    /// # Ritorna
    /// `Ok(())` se l'unlock è riuscito, `Err` in caso di errore
    ///
    /// # Errori
    /// - La proposta non è in the stato Approved
    /// - Altri errori di storage
    ///
    /// # Note
    pub fn unlock_deposit(&self, storage: &Storage, proposal: &Proposal) -> anyhow::Result<()> {
        // Check che la proposta sia approvata
        if proposal.status != ProposalStatus::Approved {
            anyhow::bail!(
                "Non è possibile sbloccare il deposit: proposta non è in the stato Approved (stato attuale: {:?})",
                proposal.status
            );
        }

        // Check che il deposit sia valido
        if proposal.deposit == 0 {
            // Zero deposit, no unlock required
            return Ok(());
        }

        let creator_address = hex::decode(&proposal.creator)
            .context("Errore nel parsing of the address of the creator (non è un hex valido)")?;

        if creator_address.len() != 32 {
            anyhow::bail!(
                "Invalid creator address: length {} instead of 32 bytes",
                creator_address.len()
            );
        }

        let creator: [u8; 32] = creator_address
            .try_into()
            .map_err(|_| anyhow::anyhow!("Errore in the conversione of the address"))?;

        storage
            .increment_vote_token_balance(&creator, proposal.deposit)
            .context("Errore durante l'unlock of the deposit")?;

        Ok(())
    }

    /// Burn dei vote token se proposta negata
    ///
    /// # Argomenti
    /// - `proposal`: La proposta per cui bruciare il deposit
    ///
    /// # Ritorna
    ///
    /// # Errori
    /// - La proposta non è in the stato Rejected
    /// - Altri errori di storage
    ///
    /// # Note
    ///
    /// e non are restituiti al creator. La supply totale dei vote token viene automaticamente
    pub fn burn_deposit(&self, _storage: &Storage, proposal: &Proposal) -> anyhow::Result<()> {
        // Check che la proposta sia negata
        if proposal.status != ProposalStatus::Rejected {
            anyhow::bail!(
                "Non è possibile bruciare il deposit: proposta non è in the stato Rejected (stato attuale: {:?})",
                proposal.status
            );
        }

        // Check che il deposit sia valido
        if proposal.deposit == 0 {
            // Zero deposit, no burn required
            return Ok(());
        }

        // Il burn è già avvenuto implicitamente:
        // - Ora che la proposta è negata, i vote token non are restituiti
        // - La supply totale viene automaticamente aggiornata quando si compute get_total_vote_tokens()

        Ok(())
    }

    ///
    /// # Argomenti
    /// - `proposal`: La proposta finalizzata
    ///
    /// # Ritorna
    ///
    /// # Note
    ///
    /// # Esempio
    /// ```
    /// use crate::governance::deposit::DepositManager;
    /// use crate::governance::proposals::{Proposal, ProposalStatus};
    /// use crate::storage::Storage;
    ///
    /// let manager = DepositManager::new();
    /// let storage = Storage::new("db")?;
    /// // ... dopo che una proposta è stata finalizzata ...
    /// manager.handle_finalized_proposal(&storage, &proposal)?;
    /// ```
    pub fn handle_finalized_proposal(
        &self,
        storage: &Storage,
        proposal: &Proposal,
    ) -> anyhow::Result<()> {
        match proposal.status {
            ProposalStatus::Approved => {
                // Returns i vote token al creator
                self.unlock_deposit(storage, proposal)
            }
            ProposalStatus::Rejected => {
                self.burn_deposit(storage, proposal)
            }
            _ => {
                anyhow::bail!(
                    "Proposta non finalizzata: stato attuale {:?}. Solo proposte Approved o Rejected possono essere gestite.",
                    proposal.status
                );
            }
        }
    }

    ///
    /// # Argomenti
    /// - `deposit`: Il deposit da verificare
    /// - `total_vote_token_supply`: Il totale dei vote token in circolazione
    ///
    /// # Ritorna
    ///
    /// # Note
    /// Formula: `deposit >= (total_vote_token_supply * 5) / 100`
    pub fn is_deposit_sufficient(&self, deposit: u128, total_vote_token_supply: u128) -> bool {
        if total_vote_token_supply == 0 {
            return false; // Nessun vote token disponibile
        }

        let min_deposit = total_vote_token_supply
            .checked_mul(5)
            .and_then(|v| v.checked_div(100))
            .unwrap_or(u128::MAX); // Overflow protection

        deposit >= min_deposit
    }
}

impl Default for DepositManager {
    fn default() -> Self {
        Self::new()
    }
}
