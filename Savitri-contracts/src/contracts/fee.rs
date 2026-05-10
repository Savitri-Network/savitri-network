//! Contract Fee: Integrazione fee per contract calls
//!
//! - Calcolo fee basato su gas consumato
//! - Integrazione con fee distribution
//! - Vote token mint da contract calls

use crate::contracts::gas::GasMeter;
use crate::fee::distribution::DistributionAmounts;
use crate::fee::{BurnEngine, FeeDistribution, FeeEngine, HalvingEngine};
use crate::governance::VoteToken;
use anyhow::Result;
use hex;
use savitri_core::core::types::Account;
use savitri_storage::storage::Storage;
use std::collections::BTreeMap;

/// Sistema di fee per contratti
pub struct ContractFee {
    #[allow(dead_code)]
    fee_engine: FeeEngine,
    fee_distribution: FeeDistribution,
    /// Gas price in token per unità di gas
    /// Default: 1 token per 1_000_000 gas (0.000001 token per gas)
    gas_price: u128,
}

impl ContractFee {
    pub fn new() -> Self {
        Self {
            fee_engine: FeeEngine::default(),
            fee_distribution: FeeDistribution::default(),
            gas_price: 1_000_000, // 1 token per 1M gas = 0.000001 token per gas
        }
    }

    ///
    /// # Arguments
    /// * `gas_price` - Prezzo of the gas in token per unità di gas
    ///                 Esempio: 1_000_000 significa 1 token per 1M gas
    pub fn with_gas_price(gas_price: u128) -> Self {
        Self {
            fee_engine: FeeEngine::default(),
            fee_distribution: FeeDistribution::default(),
            gas_price,
        }
    }

    /// Compute il fee basato sul gas consumato
    ///
    /// Formula: `fee = gas_used * gas_price`
    /// Usa checked arithmetic per prevent overflow.
    ///
    /// # Arguments
    /// * `gas_used` - Gas consumato durante l'esecuzione
    /// * `gas_price` - Prezzo of the gas (opzionale, usa il default se None)
    ///
    /// # Returns
    /// Fee calcolato in token, o errore se c'è overflow
    ///
    /// # Example
    /// ```
    /// use crate::contracts::fee::ContractFee;
    ///
    /// let fee_system = ContractFee::new();
    /// let gas_used = 100_000;
    /// let fee = fee_system.calculate_fee_from_gas(gas_used, None).unwrap();
    /// // fee = 100_000 * 1_000_000 = 0.1 token
    /// ```
    pub fn calculate_fee_from_gas(
        &self,
        gas_used: u64,
        gas_price: Option<u128>,
    ) -> Result<u128, String> {
        let price = gas_price.unwrap_or(self.gas_price);

        // Usa checked arithmetic per prevent overflow
        let fee = (gas_used as u128).checked_mul(price).ok_or_else(|| {
            format!(
                "Fee calculation overflow: gas_used {} * gas_price {} exceeds u128::MAX",
                gas_used, price
            )
        })?;

        Ok(fee)
    }

    /// Compute il fee basato sul gas consumato da un GasMeter
    ///
    /// di una contract call. Legge il gas_used dal gas meter e compute il fee.
    ///
    /// # Arguments
    /// * `gas_meter` - Gas meter che ha tracciato il consumo durante l'esecuzione
    /// * `gas_price` - Prezzo of the gas (opzionale, usa il default se None)
    ///
    /// # Returns
    /// Fee calcolato in token, o errore se c'è overflow
    pub fn calculate_fee_from_gas_meter(
        &self,
        gas_meter: &GasMeter,
        gas_price: Option<u128>,
    ) -> Result<u128, String> {
        let gas_used = gas_meter.gas_used();
        self.calculate_fee_from_gas(gas_used, gas_price)
    }

    ///
    /// # Deprecated
    #[deprecated(note = "Use calculate_fee_from_gas instead")]
    pub fn calculate_fee(&self, gas_used: u64, gas_price: u128) -> u128 {
        // Usa il nuovo metodo sicuro con checked_mul e gestisce il Result
        match self.calculate_fee_from_gas(gas_used, Some(gas_price)) {
            Ok(fee) => fee,
            Err(_) => {
                // In caso di overflow, usa il massimo valore possibile (comportamento legacy)
                u128::MAX
            }
        }
    }

    /// Distribuisce i fee raccolti dai contract calls secondo il PRD
    ///
    /// - Treasury: 10% (non soggetto ad halving)
    /// - Masternode: 10% (soggetto ad halving)
    /// - Proposer + P2P: 80% (soggetto ad halving)
    ///   - Proposer: 85% of the proposer+P2P (68% of the net_fee)
    ///   - P2P group: 15% of the proposer+P2P (12% of the net_fee), distribuito proporzionalmente al PoU
    ///
    /// La sequenza è: burn → distribuzione → treasury transfer
    ///
    /// # Arguments
    /// * `total_fees` - Fee totali raccolti dai contract calls
    /// * `overlay` - Overlay state per aggiornare i balance degli account
    /// * `current_timestamp` - Timestamp corrente of the blocco
    /// * `proposer_address` - Indirizzo of the proposer (32 bytes)
    /// * `p2p_nodes` - List opzionale di (account_address, pou_score) per i nodi P2P.
    ///   Se `None`, tutto il proposer_p2p_reward viene assegnato al proposer (comportamento legacy).
    ///
    /// # Returns
    /// * `Ok(DistributionAmounts)` con gli amount distribuiti (dopo halving per rewards)
    /// * `Err` se c'è un errore durante la distribuzione
    ///
    /// # Note
    /// - I fee are prima sottoposti al burn dinamico basato sul volume 24h
    /// - L'halving viene applicato automaticamente ai rewards (masternode e proposer+P2P)
    /// - Il treasury non è soggetto ad halving
    /// - La distribuzione è consistente con quella dei fee normali
    pub fn distribute_fees(
        &self,
        total_fees: u128,
        storage: &Storage,
        overlay: &mut BTreeMap<Vec<u8>, Account>,
        current_timestamp: u64,
        masternode_address: &[u8; 32],
        proposer_address: &[u8; 32],
        p2p_nodes: Option<Vec<([u8; 32], u64)>>,
    ) -> Result<DistributionAmounts> {
        if total_fees == 0 {
            return Ok(DistributionAmounts {
                burn_amount: 0,
                treasury_amount: 0,
                validator_amount: 0,
                proposer_amount: 0,
                treasury: 0,
                masternode: 0,
                proposer_p2p: 0,
            });
        }

        let mut burn_engine = BurnEngine::default();
        burn_engine.update_volume(total_fees).map_err(|e| {
            anyhow::anyhow!("Failed to update volume for contract call fees: {}", e)
        })?;

        // 2. Compute l'amount da bruciare basandosi sul volume 24h
        let burn_amount = burn_engine
            .calculate_burn_amount_from_storage(storage)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to calculate burn amount for contract call fees: {}",
                    e
                )
            })?;

        // 3. Esegui il burn (anche se amount = 0, per aggiornare le metriche)
        burn_engine
            .execute_burn(burn_amount)
            .map_err(|e| anyhow::anyhow!("Failed to execute burn for contract call fees: {}", e))?;

        // 4. Compute net fee dopo il burn
        let net_fee = total_fees
            .checked_sub(burn_amount)
            .ok_or_else(|| anyhow::anyhow!("Burn amount exceeds total fees for contract calls"))?;

        // 5. Creates HalvingEngine per applicare l'halving ai rewards
        let halving_engine = HalvingEngine::from_storage(storage)
            .map_err(|e| anyhow::anyhow!("Failed to create HalvingEngine: {}", e))?;

        // 6. Distribuisce i fee: treasury 10%, masternode 10%, proposer+P2P 80%
        // L'halving viene applicato automaticamente ai rewards (masternode e proposer+P2P)
        // Il proposer riceve 85% of the proposer+P2P, il resto (15%) viene distribuito ai nodi P2P
        // proporzionalmente al PoU score (escludendo il nodo con PoU più basso)
        let distribution_result = self
            .fee_distribution
            .distribute_fees_after_burn(
                net_fee,
                storage,
                overlay,
                &halving_engine,
                current_timestamp,
                masternode_address,
                proposer_address,
                p2p_nodes.clone(),
            )
            .map_err(|e| anyhow::anyhow!("Failed to distribute contract call fees: {}", e))?;

        // 7. Mint vote token: 0.1% dei fee totali viene convertito in vote token
        // Sequenza: burn → distribuzione → treasury transfer → vote token mint
        let vote_token_engine = VoteToken::default();
        let total_vote_tokens = vote_token_engine.mint_from_fees(total_fees);

        if total_vote_tokens > 0 {
            // Distribuisce i vote token proporzionalmente alle fee ricevute dai partecipanti
            // I partecipanti sono: masternode, proposer, e nodi P2P
            self.distribute_vote_tokens(
                storage,
                total_vote_tokens,
                &distribution_result,
                masternode_address,
                proposer_address,
                p2p_nodes,
                net_fee,
            )
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to distribute vote tokens from contract call fees: {}",
                    e
                )
            })?;
        }

        Ok(distribution_result)
    }

    /// Distribuisce i vote token proporzionalmente alle fee ricevute dai partecipanti
    ///
    /// I vote token are distribuiti proporzionalmente alle fee ricevute da:
    /// - Masternode: riceve vote token proporzionali alla sua parte di fee (10% dei net fee)
    /// - Proposer: riceve vote token proporzionali alla sua parte di fee (85% of the proposer+P2P)
    /// - P2P nodes: ricevono vote token proporzionali alle loro parti di fee (15% of the proposer+P2P, distribuito proporzionalmente al PoU)
    ///
    /// # Arguments
    /// * `storage` - Storage per aggiornare i bilanci dei vote token
    /// * `total_vote_tokens` - Quantità totale di vote token da distribuire
    /// * `proposer_address` - Indirizzo of the proposer (32 bytes)
    /// * `p2p_nodes` - List opzionale di (account_address, pou_score) per i nodi P2P
    /// * `net_fee` - Fee netti dopo il burn
    ///
    /// # Returns
    /// * `Err` se c'è un errore durante la distribuzione
    fn distribute_vote_tokens(
        &self,
        storage: &Storage,
        total_vote_tokens: u128,
        distribution_result: &DistributionAmounts,
        masternode_address: &[u8; 32],
        proposer_address: &[u8; 32],
        p2p_nodes: Option<Vec<([u8; 32], u64)>>,
        net_fee: u128,
    ) -> Result<()> {
        if total_vote_tokens == 0 || net_fee == 0 {
            return Ok(()); // Nessun vote token da distribuire
        }

        // Masternode riceve: distribution_result.masternode
        // Proposer riceve: proposer_amount = distribution_result.proposer_p2p * 85% (8500 BPS)
        // P2P nodes ricevono: p2p_amount = distribution_result.proposer_p2p * 15% (1500 BPS) (distribuito proporzionalmente)

        let masternode_fee = distribution_result.masternode;
        let proposer_p2p_fee = distribution_result.proposer_p2p;

        const PROPOSER_BPS: u128 = 8_500; // 85%
        const BPS_DENOM: u128 = 10_000;
        let proposer_fee = proposer_p2p_fee
            .checked_mul(PROPOSER_BPS)
            .and_then(|x: u128| x.checked_div(BPS_DENOM))
            .unwrap_or(0);
        let p2p_fee = proposer_p2p_fee.checked_sub(proposer_fee).unwrap_or(0);

        let total_fees_received = masternode_fee
            .checked_add(proposer_p2p_fee)
            .ok_or_else(|| anyhow::anyhow!("fee overflow"))?;

        if total_fees_received == 0 {
            return Ok(()); // No fee received, no vote tokens to distribute
        }

        // Distribuisce i vote token proporzionalmente alle fee ricevute
        if masternode_address == proposer_address {
            // Stesso indirizzo: somma le parti masternode e proposer
            let combined_fee = masternode_fee
                .checked_add(proposer_fee)
                .ok_or_else(|| anyhow::anyhow!("fee overflow"))?;

            if combined_fee > 0 {
                let combined_vote_tokens = (total_vote_tokens as u128)
                    .checked_mul(combined_fee)
                    .and_then(|x| x.checked_div(total_fees_received))
                    .unwrap_or(0);

                if combined_vote_tokens > 0 {
                    storage
                        .increment_vote_token_balance(masternode_address, combined_vote_tokens)?;
                }
            }
        } else {
            // Indirizzi diversi: distribuisci separatamente
            // Masternode
            if masternode_fee > 0 {
                let masternode_vote_tokens = (total_vote_tokens as u128)
                    .checked_mul(masternode_fee)
                    .and_then(|x| x.checked_div(total_fees_received))
                    .unwrap_or(0);

                if masternode_vote_tokens > 0 {
                    storage
                        .increment_vote_token_balance(masternode_address, masternode_vote_tokens)?;
                }
            }

            // Proposer
            if proposer_fee > 0 {
                let proposer_vote_tokens = (total_vote_tokens as u128)
                    .checked_mul(proposer_fee)
                    .and_then(|x| x.checked_div(total_fees_received))
                    .unwrap_or(0);

                if proposer_vote_tokens > 0 {
                    storage.increment_vote_token_balance(proposer_address, proposer_vote_tokens)?;
                }
            }
        }

        // P2P nodes: distribuisci proporzionalmente al PoU score (escludendo lowest PoU)
        if let Some(p2p_nodes_list) = p2p_nodes {
            if p2p_fee > 0 && !p2p_nodes_list.is_empty() {
                // Trova il nodo con PoU score più basso
                let lowest_pou_node = p2p_nodes_list
                    .iter()
                    .min_by_key(|(_, score)| score)
                    .map(|(account, score)| (*account, *score));

                // Filtra i nodi escludendo quello con PoU più basso
                let mut eligible_nodes: Vec<([u8; 32], u64)> = Vec::new();
                if let Some((lowest_account, lowest_score)) = lowest_pou_node {
                    let mut lowest_excluded = false;
                    for (account, score) in p2p_nodes_list {
                        if !lowest_excluded && account == lowest_account && score == lowest_score {
                            lowest_excluded = true;
                            continue; // Escludi il primo nodo con PoU più basso
                        }
                        eligible_nodes.push((account, score));
                    }
                } else {
                    eligible_nodes = p2p_nodes_list;
                }

                if !eligible_nodes.is_empty() {
                    // Compute la somma totale dei PoU scores dei nodi eleggibili
                    let total_pou_sum: u64 = eligible_nodes.iter().map(|(_, score)| score).sum();

                    if total_pou_sum > 0 {
                        // Distribuisci proporzionalmente ai PoU scores
                        let mut distributed_total = 0u128;

                        for (idx, (account, score)) in eligible_nodes.iter().enumerate() {
                            let is_last = idx == eligible_nodes.len() - 1;

                            let vote_tokens = if is_last {
                                // Assegna il resto all'ultimo nodo per evitare perdite di precisione
                                let p2p_vote_tokens = (total_vote_tokens as u128)
                                    .checked_mul(p2p_fee)
                                    .and_then(|x| x.checked_div(total_fees_received))
                                    .unwrap_or(0);
                                p2p_vote_tokens - distributed_total
                            } else {
                                // Compute amount proporzionale: p2p_vote_tokens * (score / total_pou_sum)
                                let p2p_vote_tokens = (total_vote_tokens as u128)
                                    .checked_mul(p2p_fee)
                                    .and_then(|x| x.checked_div(total_fees_received))
                                    .unwrap_or(0);

                                (p2p_vote_tokens as u128)
                                    .checked_mul(*score as u128)
                                    .and_then(|x| x.checked_div(total_pou_sum as u128))
                                    .unwrap_or(0)
                            };

                            if vote_tokens > 0 {
                                storage.increment_vote_token_balance(account, vote_tokens)?;
                                distributed_total = distributed_total
                                    .checked_add(vote_tokens)
                                    .unwrap_or(distributed_total);
                            }
                        }
                    } else {
                        let p2p_vote_tokens = (total_vote_tokens as u128)
                            .checked_mul(p2p_fee)
                            .and_then(|x| x.checked_div(total_fees_received))
                            .unwrap_or(0);

                        let amount_per_node = p2p_vote_tokens / eligible_nodes.len() as u128;
                        let remainder = p2p_vote_tokens % eligible_nodes.len() as u128;

                        for (idx, (account, _)) in eligible_nodes.iter().enumerate() {
                            let vote_tokens = if idx == 0 {
                                amount_per_node + remainder // Assegna il resto al primo nodo
                            } else {
                                amount_per_node
                            };

                            if vote_tokens > 0 {
                                storage.increment_vote_token_balance(account, vote_tokens)?;
                            }
                        }
                    }
                }
            }
        } else {
            if p2p_fee > 0 {
                let p2p_vote_tokens = (total_vote_tokens as u128)
                    .checked_mul(p2p_fee)
                    .and_then(|x| x.checked_div(total_fees_received))
                    .unwrap_or(0);

                if p2p_vote_tokens > 0 {
                    if masternode_address == proposer_address {
                        storage.increment_vote_token_balance(proposer_address, p2p_vote_tokens)?;
                    } else {
                        storage.increment_vote_token_balance(proposer_address, p2p_vote_tokens)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Compute la quantità di vote token da mintare dai fee (0.1% dei fee totali)
    ///
    /// di vote token da mintare, garantendo consistenza con il sistema normale.
    ///
    /// # Arguments
    /// * `total_fees` - Fee totali raccolti dai contract calls
    ///
    /// # Returns
    /// Quantità di vote token da mintare (0.1% dei fee totali)
    ///
    /// # Note
    /// Usa integer arithmetic per evitare problemi di precisione con floating point.
    pub fn mint_vote_tokens(&self, total_fees: u128) -> u128 {
        let vote_token_engine = VoteToken::default();
        vote_token_engine.mint_from_fees(total_fees)
    }

    /// Compute e deduce il fee dal caller basato sul gas consumato
    ///
    /// per calcolare il fee basato sul gas effettivamente consumato e dedurlo
    /// dal balance of the caller.
    ///
    /// # Arguments
    /// * `gas_meter` - Gas meter che ha tracciato il consumo durante l'esecuzione
    /// * `caller_key` - Chiave dell'account of the caller (Vec<u8>)
    /// * `storage` - Storage layer per leggere lo stato corrente
    /// * `overlay` - Overlay state dove applicare le modifiche
    /// * `gas_price` - Prezzo of the gas (opzionale, usa il default se None)
    ///
    /// # Returns
    /// * `Err` se c'è overflow nel calcolo o balance insufficiente
    ///
    /// # Behavior
    /// - Compute il fee basato su `gas_meter.gas_used()`
    /// - Check che il caller abbia balance sufficiente
    /// - Deduce il fee dal balance of the caller nell'overlay
    ///
    /// # Note
    /// Il fee viene dedotto anche se l'esecuzione è fallita (prevenzione DoS).
    pub fn calculate_and_deduct_fee_from_caller(
        &self,
        gas_meter: &GasMeter,
        caller_key: &[u8],
        storage: &Storage,
        overlay: &mut BTreeMap<Vec<u8>, Account>,
        gas_price: Option<u128>,
    ) -> Result<u128, String> {
        let gas_used = gas_meter.gas_used();
        self.calculate_and_deduct_fee_from_caller_with_gas(
            gas_used, caller_key, storage, overlay, gas_price,
        )
    }

    /// Compute e deduce il fee dal caller basato sul gas consumato (variante con gas_used diretto)
    ///
    /// già calcolato il gas consumato per una chiamata specifica.
    ///
    /// # Arguments
    /// * `gas_used` - Gas consumato durante l'esecuzione
    /// * `caller_key` - Chiave dell'account of the caller (Vec<u8>)
    /// * `storage` - Storage layer per leggere lo stato corrente
    /// * `overlay` - Overlay state dove applicare le modifiche
    /// * `gas_price` - Prezzo of the gas (opzionale, usa il default se None)
    ///
    /// # Returns
    /// * `Err` se c'è overflow nel calcolo o balance insufficiente
    ///
    /// # Behavior
    /// - Compute il fee basato su `gas_used`
    /// - Check che il caller abbia balance sufficiente
    /// - Deduce il fee dal balance of the caller nell'overlay
    ///
    /// # Note
    /// Il fee viene dedotto anche se l'esecuzione è fallita (prevenzione DoS).
    pub fn calculate_and_deduct_fee_from_caller_with_gas(
        &self,
        gas_used: u64,
        caller_key: &[u8],
        storage: &Storage,
        overlay: &mut BTreeMap<Vec<u8>, Account>,
        gas_price: Option<u128>,
    ) -> Result<u128, String> {
        // 1. Compute il fee basato sul gas consumato
        let fee_amount = self
            .calculate_fee_from_gas(gas_used, gas_price)
            .map_err(|e| format!("Failed to calculate fee from gas: {}", e))?;

        if fee_amount == 0 {
            return Ok(0);
        }

        // 3. Leggi l'account of the caller (read-through overlay)
        const MAX_ACCOUNT_SIZE: usize = 1 * 1024 * 1024;
        let caller_account = overlay.get(caller_key).cloned().unwrap_or_else(|| {
            if let Ok(Some(account_data)) = storage.get_account(caller_key) {
                if account_data.len() > MAX_ACCOUNT_SIZE {
                    return Account::default();
                }
                bincode::deserialize::<Account>(&account_data)
                    .unwrap_or_else(|_| Account::default())
            } else {
                Account::default()
            }
        });

        // 4. Check che il caller abbia balance sufficiente
        if caller_account.balance < fee_amount {
            let shortfall = fee_amount - caller_account.balance;
            return Err(format!(
                "Insufficient balance for contract call fee: caller=0x{}, balance={}, required={}, shortfall={}",
                hex::encode(caller_key),
                caller_account.balance,
                fee_amount,
                shortfall
            ));
        }

        // 5. Deduce il fee dal balance of the caller
        let mut updated_account = caller_account;
        updated_account
            .debit(fee_amount)
            .map_err(|e| format!("Failed to debit fee from caller: {}", e))?;

        overlay.insert(caller_key.to_vec(), updated_account);

        Ok(fee_amount)
    }
}

impl Default for ContractFee {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir() -> anyhow::Result<PathBuf> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("savitri-fee-test-{}", nanos));
        fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn setup_test_storage() -> anyhow::Result<Storage> {
        let tmp = unique_tmp_dir()?;
        Storage::new(&tmp)
    }

    fn create_test_account(_address: &[u8; 32], balance: u128) -> Account {
        let mut account = Account::default();
        account.credit(balance).unwrap();
        account
    }

    /// Helper to convert Result<T, String> to anyhow::Result<T>
    fn map_str_err<T>(r: std::result::Result<T, String>) -> anyhow::Result<T> {
        r.map_err(|e| anyhow::anyhow!("{}", e))
    }

    #[test]
    fn test_contract_fee_new() {
        let fee_system = ContractFee::new();

        // Should have default gas price
        assert_eq!(fee_system.gas_price, 1_000_000);
    }

    #[test]
    fn test_contract_fee_with_gas_price() {
        let custom_gas_price = 2_000_000;
        let fee_system = ContractFee::with_gas_price(custom_gas_price);

        assert_eq!(fee_system.gas_price, custom_gas_price);
    }

    #[test]
    fn test_calculate_fee_from_gas() -> anyhow::Result<()> {
        let fee_system = ContractFee::new();

        // Test basic calculation
        let gas_used = 100_000;
        let fee = map_str_err(fee_system.calculate_fee_from_gas(gas_used, None))?;

        // fee = 100_000 * 1_000_000 = 0.1 token
        assert_eq!(fee, 100_000_000_000);

        // Test with custom gas price
        let custom_gas_price = 2_000_000;
        let fee_custom =
            map_str_err(fee_system.calculate_fee_from_gas(gas_used, Some(custom_gas_price)))?;

        // fee = 100_000 * 2_000_000 = 0.2 token
        assert_eq!(fee_custom, 200_000_000_000);

        Ok(())
    }

    #[test]
    fn test_calculate_fee_from_gas_overflow() {
        let fee_system = ContractFee::new();

        // Test overflow case
        let gas_used = u64::MAX;
        let gas_price = u128::MAX;

        let result = fee_system.calculate_fee_from_gas(gas_used, Some(gas_price));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("overflow"));
    }

    #[test]
    fn test_calculate_fee_from_gas_meter() -> anyhow::Result<()> {
        let fee_system = ContractFee::new();

        // Create gas meter with some usage
        let mut gas_meter = GasMeter::new(1_000_000);
        gas_meter.consume(50_000).unwrap();

        let fee = map_str_err(fee_system.calculate_fee_from_gas_meter(&gas_meter, None))?;

        // fee = 50_000 * 1_000_000 = 0.05 token
        assert_eq!(fee, 50_000_000_000);

        Ok(())
    }

    #[test]
    fn test_calculate_fee_deprecated() {
        let fee_system = ContractFee::new();

        // Test deprecated method (should still work)
        let gas_used = 100_000;
        let gas_price = 1_000_000;
        let fee = fee_system.calculate_fee(gas_used, gas_price);

        // fee = 100_000 * 1_000_000 = 0.1 token
        assert_eq!(fee, 100_000_000_000);

        // Test overflow case (should return u128::MAX)
        let overflow_gas = u64::MAX;
        let overflow_price = u128::MAX;
        let overflow_fee = fee_system.calculate_fee(overflow_gas, overflow_price);

        assert_eq!(overflow_fee, u128::MAX);
    }

    #[test]
    fn test_calculate_and_deduct_fee_from_caller() -> anyhow::Result<()> {
        let storage = setup_test_storage()?;
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        // Create test account with sufficient balance
        let caller_address = [0x01; 32];
        let initial_balance = 1_000_000_000_000; // 1000 tokens
        let caller_account = create_test_account(&caller_address, initial_balance);
        overlay.insert(caller_address.to_vec(), caller_account);

        // Create gas meter with usage
        let mut gas_meter = GasMeter::new(1_000_000);
        gas_meter.consume(100_000).unwrap(); // 0.1 token fee

        // Calculate and deduct fee
        let fee_amount = map_str_err(fee_system.calculate_and_deduct_fee_from_caller(
            &gas_meter,
            &caller_address,
            &storage,
            &mut overlay,
            None,
        ))?;

        // Verify fee amount
        assert_eq!(fee_amount, 100_000_000_000); // 0.1 token

        // Verify balance was deducted
        let updated_account = overlay.get(caller_address.as_slice()).unwrap();
        assert_eq!(updated_account.balance, initial_balance - fee_amount);

        Ok(())
    }

    #[test]
    fn test_calculate_and_deduct_fee_insufficient_balance() {
        let storage = setup_test_storage().unwrap();
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        // Create test account with insufficient balance
        let caller_address = [0x01; 32];
        let insufficient_balance = 50_000_000_000; // 0.05 tokens
        let caller_account = create_test_account(&caller_address, insufficient_balance);
        overlay.insert(caller_address.to_vec(), caller_account);

        // Create gas meter with usage that requires more fee
        let mut gas_meter = GasMeter::new(1_000_000);
        gas_meter.consume(100_000).unwrap(); // 0.1 token fee required

        // Try to calculate and deduct fee (should fail)
        let result = fee_system.calculate_and_deduct_fee_from_caller(
            &gas_meter,
            &caller_address,
            &storage,
            &mut overlay,
            None,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Insufficient balance"));
    }

    #[test]
    fn test_calculate_and_deduct_fee_zero_fee() -> anyhow::Result<()> {
        let storage = setup_test_storage()?;
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        // Create test account
        let caller_address = [0x01; 32];
        let initial_balance = 1_000_000_000_000;
        let caller_account = create_test_account(&caller_address, initial_balance);
        overlay.insert(caller_address.to_vec(), caller_account);

        // Create gas meter with zero usage
        let gas_meter = GasMeter::new(1_000_000); // No gas consumed

        // Calculate and deduct fee
        let fee_amount = map_str_err(fee_system.calculate_and_deduct_fee_from_caller(
            &gas_meter,
            &caller_address,
            &storage,
            &mut overlay,
            None,
        ))?;

        // Should be zero fee
        assert_eq!(fee_amount, 0);

        // Balance should be unchanged
        let updated_account = overlay.get(caller_address.as_slice()).unwrap();
        assert_eq!(updated_account.balance, initial_balance);

        Ok(())
    }

    #[test]
    fn test_calculate_and_deduct_fee_with_gas_direct() -> anyhow::Result<()> {
        let storage = setup_test_storage()?;
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        // Create test account
        let caller_address = [0x01; 32];
        let initial_balance = 1_000_000_000_000;
        let caller_account = create_test_account(&caller_address, initial_balance);
        overlay.insert(caller_address.to_vec(), caller_account);

        // Calculate and deduct fee with direct gas amount
        let gas_used = 50_000;
        let fee_amount = map_str_err(fee_system.calculate_and_deduct_fee_from_caller_with_gas(
            gas_used,
            &caller_address,
            &storage,
            &mut overlay,
            None,
        ))?;

        // Verify fee amount
        assert_eq!(fee_amount, 50_000_000_000); // 0.05 token

        // Verify balance was deducted
        let updated_account = overlay.get(caller_address.as_slice()).unwrap();
        assert_eq!(updated_account.balance, initial_balance - fee_amount);

        Ok(())
    }

    #[test]
    fn test_mint_vote_tokens() {
        let fee_system = ContractFee::new();

        // Test vote token minting (0.1% of fees)
        let total_fees = 1_000_000_000_000; // 1000 tokens
        let vote_tokens = fee_system.mint_vote_tokens(total_fees);

        // 0.1% of 1000 tokens = 1 token
        assert_eq!(vote_tokens, 1_000_000_000);

        // Test with zero fees
        let zero_fees = 0;
        let zero_tokens = fee_system.mint_vote_tokens(zero_fees);
        assert_eq!(zero_tokens, 0);
    }

    #[test]
    fn test_distribute_fees_zero_fees() -> anyhow::Result<()> {
        let storage = setup_test_storage()?;
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        let masternode_address = [0x01; 32];
        let proposer_address = [0x02; 32];
        let current_timestamp = 1234567890;

        // Test with zero fees
        let result = fee_system.distribute_fees(
            0,
            &storage,
            &mut overlay,
            current_timestamp,
            &masternode_address,
            &proposer_address,
            None,
        )?;

        // Should return zero distribution
        assert_eq!(result.treasury, 0);
        assert_eq!(result.masternode, 0);
        assert_eq!(result.proposer_p2p, 0);

        Ok(())
    }

    #[test]
    fn test_fee_calculation_edge_cases() -> anyhow::Result<()> {
        let fee_system = ContractFee::new();

        // Test with minimum gas
        let min_gas = 1;
        let min_fee = map_str_err(fee_system.calculate_fee_from_gas(min_gas, None))?;
        assert_eq!(min_fee, 1_000_000);

        // Test with maximum gas that doesn't overflow
        let max_safe_gas = u64::MAX / 1_000_000;
        let max_safe_fee = map_str_err(fee_system.calculate_fee_from_gas(max_safe_gas, None))?;
        assert_eq!(max_safe_fee, (max_safe_gas as u128) * 1_000_000);

        // Test with zero gas price
        let zero_price_fee = map_str_err(fee_system.calculate_fee_from_gas(100_000, Some(0)))?;
        assert_eq!(zero_price_fee, 0);

        Ok(())
    }

    #[test]
    fn test_deprecated_method_compatibility() {
        let fee_system = ContractFee::new();

        // Test that deprecated method gives same result as new method
        let gas_used = 123_456;
        let gas_price = 2_345_678;

        // New method
        let new_result = fee_system.calculate_fee_from_gas(gas_used, Some(gas_price));

        // Deprecated method
        let deprecated_result = fee_system.calculate_fee(gas_used, gas_price);

        // Should match when no overflow
        if let Ok(new_fee) = new_result {
            assert_eq!(deprecated_result, new_fee);
        } else {
            // If new method overflows, deprecated should return u128::MAX
            assert_eq!(deprecated_result, u128::MAX);
        }
    }

    #[test]
    fn test_gas_price_variants() -> anyhow::Result<()> {
        let custom_fee_system = ContractFee::with_gas_price(500_000); // 0.5 token per 1M gas

        // Test with default gas price
        let gas_used = 200_000;
        let fee_with_default =
            map_str_err(custom_fee_system.calculate_fee_from_gas(gas_used, None))?;
        assert_eq!(fee_with_default, 100_000_000_000); // 200k * 500k = 0.1 token

        // Test with custom gas price override
        let fee_with_override =
            map_str_err(custom_fee_system.calculate_fee_from_gas(gas_used, Some(1_000_000)))?;
        assert_eq!(fee_with_override, 200_000_000_000); // 200k * 1M = 0.2 token

        Ok(())
    }

    #[test]
    fn test_account_read_through_overlay() -> anyhow::Result<()> {
        let storage = setup_test_storage()?;
        let fee_system = ContractFee::new();
        let mut overlay = BTreeMap::new();

        let caller_address = [0x01; 32];
        let initial_balance = 500_000_000_000; // 0.5 tokens

        // Create account in storage (not in overlay)
        let storage_account = create_test_account(&caller_address, initial_balance);
        let account_bytes = bincode::serialize(&storage_account)?;
        storage.put_account(&caller_address, &account_bytes)?;

        // Calculate and deduct fee (should read from storage)
        let mut gas_meter = GasMeter::new(1_000_000);
        gas_meter.consume(100_000).unwrap(); // 0.1 token fee

        let fee_amount = map_str_err(fee_system.calculate_and_deduct_fee_from_caller(
            &gas_meter,
            &caller_address,
            &storage,
            &mut overlay,
            None,
        ))?;

        // Verify fee was deducted and account is now in overlay
        assert_eq!(fee_amount, 100_000_000_000);
        assert!(overlay.contains_key(caller_address.as_slice()));

        let updated_account = overlay.get(caller_address.as_slice()).unwrap();
        assert_eq!(updated_account.balance, initial_balance - fee_amount);

        Ok(())
    }
}
