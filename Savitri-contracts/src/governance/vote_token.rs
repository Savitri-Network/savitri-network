//! Vote Token: Sistema di vote token non-transferable
//!
//! - Mint (0.1% dei fee totali)
//! - Distribuzione proporzionale
//! - Non-transferability
//! - Query balance

use savitri_storage::storage::Storage;

/// Sistema di vote token
pub struct VoteToken;

impl VoteToken {
    pub fn new() -> Self {
        Self
    }

    /// Mint vote token (0.1% dei fee totali)
    ///
    /// Compute la quantità di vote token da mintare basandosi sui fee totali pagati.
    /// Formula: total_fees * 0.001 (0.1%)
    pub fn mint_from_fees(&self, total_fees: u128) -> u128 {
        // 0.1% = 0.001
        // Usa integer arithmetic per evitare problemi di precisione
        (total_fees / 1000).max(0) // Assicura che non sia negativo
    }

    /// Distribuisce vote token proporzionalmente ai partecipanti basandosi sulle loro fee ricevute
    ///
    /// # Parametri
    /// - `storage`: Storage per aggiornare i bilanci dei vote token
    /// - `total_vote_tokens`: Quantità totale di vote token da distribuire
    ///   - `address`: Indirizzo of the partecipante (32 bytes)
    ///
    /// # Note
    /// I vote token are distribuiti proporzionalmente alle fee ricevute.
    ///
    /// # Ritorna
    pub fn distribute_proportional(
        &self,
        storage: &Storage,
        total_vote_tokens: u128,
        participants: Vec<([u8; 32], u128)>, // (address, fee_received)
    ) -> anyhow::Result<()> {
        if total_vote_tokens == 0 || participants.is_empty() {
            return Ok(()); // Nessun vote token da distribuire
        }

        let total_fees_received: u128 =
            participants
                .iter()
                .map(|(_, fee)| fee)
                .try_fold(0u128, |acc, fee| {
                    acc.checked_add(*fee)
                        .ok_or_else(|| anyhow::anyhow!("total fees overflow"))
                })?;

        if total_fees_received == 0 {
            return Ok(()); // No fee received, no vote tokens to distribute
        }

        // Distribuisce i vote token proporzionalmente alle fee ricevute
        let mut distributed_total = 0u128;

        for (idx, (address, fee_received)) in participants.iter().enumerate() {
            let is_last = idx == participants.len() - 1;

            let vote_tokens = if is_last {
                // Assegna il resto all'ultimo partecipante per evitare perdite di precisione
                total_vote_tokens - distributed_total
            } else {
                // Compute amount proporzionale: total_vote_tokens * (fee_received / total_fees_received)
                (total_vote_tokens as u128)
                    .checked_mul(*fee_received)
                    .and_then(|x| x.checked_div(total_fees_received))
                    .unwrap_or(0)
            };

            if vote_tokens > 0 {
                storage.increment_vote_token_balance(address, vote_tokens)?;
                distributed_total = distributed_total
                    .checked_add(vote_tokens)
                    .ok_or_else(|| anyhow::anyhow!("distributed total overflow"))?;
            }
        }

        Ok(())
    }

    /// Distribuisce vote token a un singolo address basandosi sulle fee pagate
    ///
    /// che riceve vote token proporzionali alle fee che ha pagato (non alle fee ricevute).
    ///
    /// # Parametri
    /// - `storage`: Storage per aggiornare il balance dei vote token
    /// - `address`: Indirizzo of the sender (32 bytes)
    /// - `fee_paid`: Fee pagate dal sender
    ///
    /// # Note
    pub fn mint_to_sender(
        &self,
        storage: &Storage,
        address: &[u8; 32],
        fee_paid: u128,
    ) -> anyhow::Result<()> {
        if fee_paid == 0 {
            return Ok(()); // No fee paid, no vote tokens
        }

        let vote_tokens = self.mint_from_fees(fee_paid);

        if vote_tokens > 0 {
            storage.increment_vote_token_balance(address, vote_tokens)?;
        }

        Ok(())
    }

    /// Query of the balance di vote token di un address
    ///
    /// # Parametri
    /// - `storage`: Storage per leggere il balance
    /// - `address`: Indirizzo dell'account (32 bytes)
    ///
    /// # Ritorna
    /// Balance dei vote token per l'address specificato, o 0 se l'address non ha vote token
    pub fn balance_of(&self, storage: &Storage, address: &[u8; 32]) -> anyhow::Result<u128> {
        storage.get_vote_token_balance(address)
    }

    ///
    pub fn is_transferable(&self) -> bool {
        false
    }
}

impl Default for VoteToken {
    fn default() -> Self {
        Self::new()
    }
}
