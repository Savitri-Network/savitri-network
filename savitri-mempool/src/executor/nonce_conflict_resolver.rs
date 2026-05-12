//! Nonce Conflict Resolver (FASE 5)
//!
//! Modulo specializzato per la risoluzione dei conflitti nonce con fee-based priority.

use crate::mempool::types::{MempoolTx, TxClass};
use std::collections::HashMap;

/// Strategie di risoluzione conflitti
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictResolutionStrategy {
    /// Seleziona transazione con fee più alta
    FeePriority,
    /// Seleziona transazione ricevuta prima (temporal priority)
    TemporalPriority,
    /// Combinazione di fee e tempo (weighted score)
    WeightedScore { fee_weight: f64, time_weight: f64 },
    /// Priorità basata su classe di transazione
    ClassPriority,
}

/// Risolutore di conflitti nonce per transaction ordering
pub struct NonceConflictResolver {
    /// Strategia di risoluzione conflitti
    strategy: ConflictResolutionStrategy,
    /// Cache per memoizzare i risultati di risoluzione
    resolution_cache: HashMap<(Vec<u8>, u64), usize>, // (sender_address, nonce) -> winning_index
    /// Statistiche sulle risoluzioni
    stats: ConflictResolutionStats,
}

/// Statistiche sulle risoluzioni dei conflitti
#[derive(Debug, Clone, Default)]
pub struct ConflictResolutionStats {
    /// Numero totale di conflitti risolti
    pub total_conflicts_resolved: u64,
    pub fee_priority_wins: u64,
    pub temporal_priority_wins: u64,
    pub weighted_score_wins: u64,
    pub class_priority_wins: u64,
    pub conflicts_per_sender: HashMap<Vec<u8>, u64>,
}

impl NonceConflictResolver {
    pub fn new() -> Self {
        Self::with_strategy(ConflictResolutionStrategy::FeePriority)
    }

    /// Creates un risolutore con strategia specifica
    pub fn with_strategy(strategy: ConflictResolutionStrategy) -> Self {
        Self {
            strategy,
            resolution_cache: HashMap::new(),
            stats: ConflictResolutionStats::default(),
        }
    }

    /// Risolve i conflitti nonce in un gruppo di transazioni
    ///
    /// # Arguments
    /// * `transactions` - Vettore di transazioni con possibili conflitti
    ///
    /// # Returns
    pub fn resolve_conflicts(&mut self, transactions: &[MempoolTx]) -> Vec<usize> {
        let mut conflicts: HashMap<(Vec<u8>, u64), Vec<usize>> = HashMap::new();

        // Raggruppa transazioni per conflitti (sender_address + nonce)
        for (idx, tx) in transactions.iter().enumerate() {
            let conflict_key = (tx.sender_address.clone(), tx.nonce);
            conflicts
                .entry(conflict_key)
                .or_insert_with(Vec::new)
                .push(idx);
        }

        let mut winners = Vec::new();

        for ((sender_address, nonce), indices) in conflicts {
            if indices.len() == 1 {
                // No conflict, append directly
                winners.push(indices[0]);
            } else {
                // Conflitto detected: risolvi con strategia
                let sender_address_clone = sender_address.clone();
                let winner_idx =
                    self.resolve_single_conflict(transactions, &indices, sender_address, nonce);
                winners.push(winner_idx);

                self.stats.total_conflicts_resolved += 1;
                *self
                    .stats
                    .conflicts_per_sender
                    .entry(sender_address_clone)
                    .or_insert(0) += 1;
            }
        }

        winners
    }

    /// Risolve un singolo conflitto tra più transazioni
    fn resolve_single_conflict(
        &mut self,
        transactions: &[MempoolTx],
        conflicting_indices: &[usize],
        sender_address: Vec<u8>,
        nonce: u64,
    ) -> usize {
        // Check cache prima di ricalcolare
        let cache_key = (sender_address, nonce);
        if let Some(&cached_winner) = self.resolution_cache.get(&cache_key) {
            return cached_winner;
        }

        let winner_idx = match &self.strategy {
            ConflictResolutionStrategy::FeePriority => {
                self.resolve_by_fee_priority(transactions, conflicting_indices)
            }
            ConflictResolutionStrategy::TemporalPriority => {
                self.resolve_by_temporal_priority(transactions, conflicting_indices)
            }
            ConflictResolutionStrategy::WeightedScore {
                fee_weight,
                time_weight,
            } => self.resolve_by_weighted_score(
                transactions,
                conflicting_indices,
                *fee_weight,
                *time_weight,
            ),
            ConflictResolutionStrategy::ClassPriority => {
                self.resolve_by_class_priority(transactions, conflicting_indices)
            }
        };

        // Memorizza in cache
        self.resolution_cache.insert(cache_key, winner_idx);
        winner_idx
    }

    /// Risoluzione basata su fee più alta
    fn resolve_by_fee_priority(&mut self, transactions: &[MempoolTx], indices: &[usize]) -> usize {
        let mut best_idx = indices[0];
        let mut best_fee = transactions[best_idx].fee;

        for &idx in indices.iter().skip(1) {
            let current_fee = transactions[idx].fee;
            if current_fee > best_fee {
                best_fee = current_fee;
                best_idx = idx;
            } else if current_fee == best_fee {
                // Tie-breaker: temporal priority
                if transactions[idx].received_at < transactions[best_idx].received_at {
                    best_idx = idx;
                }
            }
        }

        self.stats.fee_priority_wins += 1;
        best_idx
    }

    /// Risoluzione basata su tempo (prima ricevuta)
    fn resolve_by_temporal_priority(
        &mut self,
        transactions: &[MempoolTx],
        indices: &[usize],
    ) -> usize {
        let mut best_idx = indices[0];
        let mut best_time = transactions[best_idx].received_at;

        for &idx in indices.iter().skip(1) {
            let current_time = transactions[idx].received_at;
            if current_time < best_time {
                best_time = current_time;
                best_idx = idx;
            }
        }

        self.stats.temporal_priority_wins += 1;
        best_idx
    }

    /// Risoluzione basata su score pesato (fee + tempo)
    fn resolve_by_weighted_score(
        &mut self,
        transactions: &[MempoolTx],
        indices: &[usize],
        fee_weight: f64,
        time_weight: f64,
    ) -> usize {
        let mut best_idx = indices[0];
        let mut best_score =
            self.calculate_weighted_score(&transactions[best_idx], fee_weight, time_weight);

        for &idx in indices.iter().skip(1) {
            let current_score =
                self.calculate_weighted_score(&transactions[idx], fee_weight, time_weight);
            if current_score > best_score {
                best_score = current_score;
                best_idx = idx;
            }
        }

        self.stats.weighted_score_wins += 1;
        best_idx
    }

    /// Risoluzione basata su priorità classe
    fn resolve_by_class_priority(
        &mut self,
        transactions: &[MempoolTx],
        indices: &[usize],
    ) -> usize {
        let mut best_idx = indices[0];
        let mut best_priority = self.get_class_priority(&transactions[best_idx].class);

        for &idx in indices.iter().skip(1) {
            let current_priority = self.get_class_priority(&transactions[idx].class);
            if current_priority > best_priority {
                best_priority = current_priority;
                best_idx = idx;
            } else if current_priority == best_priority {
                // Tie-breaker: fee priority
                if transactions[idx].fee > transactions[best_idx].fee {
                    best_idx = idx;
                }
            }
        }

        self.stats.class_priority_wins += 1;
        best_idx
    }

    /// Compute score pesato per transazione
    fn calculate_weighted_score(&self, tx: &MempoolTx, fee_weight: f64, time_weight: f64) -> f64 {
        let fee_score = tx.fee as f64 / 1_000_000_000_000_000.0; // Normalizza fee
        let time_score = match std::time::Instant::now().checked_duration_since(tx.received_at) {
            Some(duration) => duration.as_secs_f64() * 0.001, // Converti a ms e normalizza
            None => 0.0,                                      // Clock went backwards, use 0
        };

        fee_score * fee_weight + time_score * time_weight
    }

    /// Ottiene priorità numerica per classe di transazione
    fn get_class_priority(&self, class: &TxClass) -> u8 {
        match class {
            TxClass::System => 4,          // Priorità più alta
            TxClass::Financial => 3,       // Alta priorità
            TxClass::FederatedUpdate => 2, // Media priorità
            TxClass::IoTData => 1,         // Bassa priorità
        }
    }

    ///
    /// # Arguments
    /// * `transactions` - Vettore di transazioni da filtrare
    ///
    /// # Returns
    /// Vettore filtrato con solo le transazioni vincenti
    pub fn filter_conflicts(&mut self, transactions: Vec<MempoolTx>) -> Vec<MempoolTx> {
        if transactions.is_empty() {
            return transactions;
        }

        let winner_indices = self.resolve_conflicts(&transactions);
        winner_indices
            .into_iter()
            .map(|idx| transactions[idx].clone())
            .collect()
    }

    /// Ottiene le statistiche di risoluzione conflitti
    pub fn get_stats(&self) -> &ConflictResolutionStats {
        &self.stats
    }

    /// Resetta le statistiche
    pub fn reset_stats(&mut self) {
        self.stats = ConflictResolutionStats::default();
    }

    /// Svuota la cache di risoluzione
    pub fn clear_cache(&mut self) {
        self.resolution_cache.clear();
    }

    pub fn set_strategy(&mut self, strategy: ConflictResolutionStrategy) {
        self.strategy = strategy;
        self.clear_cache(); // Svuota cache perché strategia è cambiata
    }

    /// Analizza i conflitti in un set di transazioni
    ///
    /// # Returns
    pub fn analyze_conflicts(&self, transactions: &[MempoolTx]) -> ConflictAnalysis {
        let mut conflicts: HashMap<(Vec<u8>, u64), usize> = HashMap::new();
        let mut total_conflicts = 0;
        let mut max_conflict_size = 0;
        let mut conflict_sizes = Vec::new();

        // Raggruppa per sender + nonce
        for tx in transactions {
            let conflict_key = (tx.sender_address.clone(), tx.nonce);
            let count = conflicts.entry(conflict_key).or_insert(0);
            *count += 1;
        }

        // Analizza i gruppi di conflitti
        for &count in conflicts.values() {
            if count > 1 {
                total_conflicts += 1;
                max_conflict_size = max_conflict_size.max(count);
                conflict_sizes.push(count);
            }
        }

        let avg_conflict_size = if !conflict_sizes.is_empty() {
            conflict_sizes.iter().sum::<usize>() as f64 / conflict_sizes.len() as f64
        } else {
            0.0
        };

        ConflictAnalysis {
            total_transactions: transactions.len(),
            total_conflicts,
            max_conflict_size,
            avg_conflict_size,
            conflict_sizes,
        }
    }
}

/// Analisi dei conflitti in un set di transazioni
#[derive(Debug, Clone)]
pub struct ConflictAnalysis {
    /// Numero totale di transazioni analizzate
    pub total_transactions: usize,
    /// Numero totale di conflitti trovati
    pub total_conflicts: usize,
    /// Dimensione massima di un conflitto
    pub max_conflict_size: usize,
    pub avg_conflict_size: f64,
    /// Dimensioni dei singoli conflitti
    pub conflict_sizes: Vec<usize>,
}

impl ConflictAnalysis {
    /// Percentuale di transazioni in conflitto
    pub fn conflict_rate(&self) -> f64 {
        if self.total_transactions == 0 {
            0.0
        } else {
            (self.total_conflicts as f64 / self.total_transactions as f64) * 100.0
        }
    }

    pub fn has_significant_conflicts(&self) -> bool {
        self.total_conflicts > 0 && (self.max_conflict_size > 2 || self.avg_conflict_size > 1.5)
    }
}
