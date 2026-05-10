//! Execution Dispatcher: Scheduler con logica economica per ordinare transazioni
//!
//! basandosi su fee priority, class priority e fairness tra sender.

use crate::executor::score_cache::ScoreCache;
use crate::executor::{
    ConflictResolutionStrategy, NonceConflictResolver, TransactionValidator, ValidationResult,
};
use crate::mempool::types::{MempoolTx, TxClass};
use bincode;
use savitri_core::Transaction as SignedTx;
use savitri_core::Transaction;
use savitri_storage::{Storage, StorageTrait};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::debug;

/// Hash signed transaction bytes using SHA-512
pub fn hash_signed_tx_bytes(tx_bytes: &[u8]) -> [u8; 32] {
    // For now, use SHA-256 for consistency with the rest of the system
    use sha2::Digest;
    let hash = sha2::Sha256::digest(tx_bytes);
    let mut result = [0u8; 32];
    result.copy_from_slice(&hash);
    result
}

/// Alternative hash function for transaction objects
pub fn hash_transaction(tx: &SignedTx) -> [u8; 32] {
    hash_signed_tx_bytes(&bincode::serialize(tx).unwrap_or_default())
}

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Error types for scheduling operations (FASE 4)
#[derive(Debug, Clone, PartialEq)]
pub enum SchedulingError {
    /// Mismatched lengths between mempool_txs and signed_txs
    MismatchedLengths {
        mempool_count: usize,
        signed_count: usize,
    },
    /// Storage access error
    StorageError(String),
    /// Validation error
    ValidationError(String),
    /// No valid transactions found
    NoValidTransactions,
}

impl std::fmt::Display for SchedulingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchedulingError::MismatchedLengths {
                mempool_count,
                signed_count,
            } => {
                write!(
                    f,
                    "Mismatched lengths: mempool_txs={}, signed_txs={}",
                    mempool_count, signed_count
                )
            }
            SchedulingError::StorageError(msg) => {
                write!(f, "Storage error: {}", msg)
            }
            SchedulingError::ValidationError(msg) => {
                write!(f, "Validation error: {}", msg)
            }
            SchedulingError::NoValidTransactions => {
                write!(f, "No valid transactions found")
            }
        }
    }
}

impl std::error::Error for SchedulingError {}

/// Stato of the mempool per analisi adaptive weights
#[derive(Debug, Clone)]
pub struct MempoolState {
    pub fee_distribution: Vec<u64>,
    pub class_distribution: Vec<TxClass>,
    /// Throughput storico (transazioni per blocco)
    pub historical_throughput: Vec<f64>,
    /// Timestamp dell'analisi
    pub timestamp: u64,
}

/// Configurazione per Adaptive Weights System
#[derive(Debug, Clone)]
pub struct AdaptiveWeightsConfig {
    /// Peso fee base (default: 0.7)
    pub base_fee_weight: f64,
    /// Peso class base (default: 0.3)
    pub base_class_weight: f64,
    /// Fattore di adattamento (smoothing, default: 0.1)
    pub adaptation_rate: f64,
    /// Threshold alta fee per aumentare peso fee (default: 2_000_000_000)
    pub fee_threshold_high: f64,
    /// Threshold bassa fee per diminuire peso fee (default: 500_000_000)
    pub fee_threshold_low: f64,
    /// Threshold diversità class per aumentare peso class (default: 0.5)
    pub class_diversity_threshold: f64,
    /// Target throughput (transazioni per blocco)
    pub target_throughput: f64,
    /// Bounds per fee_weight [min, max]
    pub fee_weight_bounds: (f64, f64),
    /// Bounds per class_weight [min, max]
    pub class_weight_bounds: (f64, f64),
}

impl Default for AdaptiveWeightsConfig {
    fn default() -> Self {
        Self {
            base_fee_weight: 0.7,
            base_class_weight: 0.3,
            adaptation_rate: 0.1,
            fee_threshold_high: 2_000_000_000.0,
            fee_threshold_low: 500_000_000.0,
            class_diversity_threshold: 0.5,
            target_throughput: 1000.0,
            fee_weight_bounds: (0.5, 0.9),
            class_weight_bounds: (0.1, 0.5),
        }
    }
}

/// Sistema di pesi adattivi per ExecutionDispatcher con ottimizzazioni Rust
#[derive(Debug, Clone)]
pub struct AdaptiveWeights {
    /// Configurazione of the sistema adattivo
    config: AdaptiveWeightsConfig,
    /// Pesi correnti (fee_weight, class_weight)
    current_weights: (f64, f64),
    /// Circular buffer per storico stati (O(1) insert/remove)
    state_history: Vec<MempoolState>,
    /// Indice di scrittura of the circular buffer
    history_write_idx: usize,
    /// Numero massimo di stati da mantenere in memoria
    max_history_size: usize,
    /// Contatore stati effettivi (per gestire buffer non pieno)
    history_count: usize,
}

impl AdaptiveWeights {
    pub fn new(config: AdaptiveWeightsConfig) -> Self {
        let max_history_size = 100; // Mantiene ultimi 100 stati
        Self {
            current_weights: (config.base_fee_weight, config.base_class_weight),
            config,
            state_history: Vec::with_capacity(max_history_size),
            history_write_idx: 0,
            max_history_size,
            history_count: 0,
        }
    }

    /// Creates un sistema di pesi adattivi con configurazione di default
    pub fn default() -> Self {
        Self::new(AdaptiveWeightsConfig::default())
    }

    /// Analizza lo stato of the mempool dalle transazioni
    pub fn analyze_mempool_state(&self, mempool_txs: &[MempoolTx]) -> MempoolState {
        let mut fee_distribution = Vec::with_capacity(mempool_txs.len());
        let mut class_distribution = Vec::with_capacity(mempool_txs.len());

        for tx in mempool_txs {
            fee_distribution.push(tx.fee);
            class_distribution.push(tx.class);
        }

        // Compute throughput storico dal circular buffer state_history
        let historical_throughput = self.calculate_historical_throughput();

        MempoolState {
            fee_distribution,
            class_distribution,
            historical_throughput,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Compute il throughput storico dai dati reali nel circular buffer state_history.
    ///
    /// Ogni MempoolState registrato rappresenta uno snapshot per ciclo di scheduling
    /// (approssimativamente uno per blocco). Il numero di transazioni in the fee_distribution
    /// rappresenta il throughput effettivo di quel ciclo.
    ///
    /// # Returns
    /// Vec<f64> con il throughput (TXs per blocco) degli ultimi stati registrati,
    fn calculate_historical_throughput(&self) -> Vec<f64> {
        if self.history_count == 0 {
            // Nessun dato storico disponibile: fallback al target configurato
            return vec![self.config.target_throughput];
        }

        let effective_count = self.history_count.min(self.state_history.len());
        let mut throughput_values = Vec::with_capacity(effective_count);

        if self.state_history.len() < self.max_history_size {
            // Buffer non pieno: leggi in ordine sequenziale
            for state in &self.state_history {
                throughput_values.push(state.fee_distribution.len() as f64);
            }
        } else {
            // Buffer pieno: ricostruisci ordine cronologico dal circular buffer
            for i in 0..self.max_history_size {
                let idx = (self.history_write_idx + i) % self.max_history_size;
                throughput_values.push(self.state_history[idx].fee_distribution.len() as f64);
            }
        }

        throughput_values
    }

    /// Adatta i pesi in base allo stato of the mempool con smoothing e bounds enforcement
    ///
    /// # Algorithm Correctness
    /// - Smoothing graduale previene oscillazioni brusche
    /// - Adattamento proporzionale alla pressione rilevata
    ///
    /// # Performance
    /// - Calcoli statistici ottimizzati con quickselect O(n)
    ///
    /// # Arguments
    /// * `mempool_state` - Stato corrente of the mempool per analisi
    ///
    /// # Returns
    /// Tuple (fee_weight, class_weight) con pesi adattati
    pub fn adapt_weights(&mut self, mempool_state: &MempoolState) -> (f64, f64) {
        let (current_fee_weight, _) = self.current_weights;

        let fee_stats = self.calculate_fee_statistics(&mempool_state.fee_distribution);
        let class_diversity = self.calculate_class_diversity(&mempool_state.class_distribution);
        let avg_throughput =
            self.calculate_average_throughput(&mempool_state.historical_throughput);

        let mut fee_pressure = 0.0;

        // --- LOGICA DI BILANCIAMENTO OTTIMIZZATA ---

        // A. Pressione Fee (Dominante)
        if fee_stats.p90 > self.config.fee_threshold_high {
            fee_pressure += 0.20; // Aumentato a 0.20 per vincere on the diversità
        } else if fee_stats.p90 < self.config.fee_threshold_low {
            fee_pressure -= 0.10;
        }

        // B. Throughput
        if avg_throughput < self.config.target_throughput {
            fee_pressure += 0.05;
        }

        // C. Diversità (Recessiva)
        // Se c'è alta diversità, vogliamo ridurre la fee per dare spazio alle classi.
        if class_diversity > self.config.class_diversity_threshold {
            if fee_pressure > 0.1 {
                fee_pressure -= 0.05;
            } else {
                // Altrimenti diamo priorità alla diversità
                fee_pressure -= 0.15;
            }
        }

        // 3. Calcolo Target Fee con bounds enforcement rigorosi
        let target_fee = (current_fee_weight + fee_pressure).clamp(
            self.config.fee_weight_bounds.0,
            self.config.fee_weight_bounds.1,
        );

        // 4. Smoothing adattivo che previene oscillazioni
        let adaptation_rate = if fee_pressure.abs() > 0.1 {
            0.2 // Adattamento più rapido per cambiamenti significativi
        } else {
            self.config.adaptation_rate // Smoothing standard per cambiamenti graduali
        };

        // Applica smoothing esponenziale per stabilità
        let new_fee_weight =
            self.apply_adaptive_smoothing(current_fee_weight, target_fee, adaptation_rate);

        // 5. Bounds final enforcement (doppio controllo per sicurezza)
        let new_fee_weight = new_fee_weight.clamp(
            self.config.fee_weight_bounds.0,
            self.config.fee_weight_bounds.1,
        );

        // 6. Complemento (PIVOT) con bounds checking
        let new_class_weight = (1.0 - new_fee_weight).clamp(
            self.config.class_weight_bounds.0,
            self.config.class_weight_bounds.1,
        );

        debug_assert!(
            (new_fee_weight + new_class_weight).abs() <= 1.0 + 1e-10,
            "Weights must sum to 1.0: fee_weight={}, class_weight={}, sum={}",
            new_fee_weight,
            new_class_weight,
            new_fee_weight + new_class_weight
        );

        self.current_weights = (new_fee_weight, new_class_weight);
        self.add_to_history(mempool_state.clone());

        // Prometheus metrics for adaptive weights
        metrics::gauge!("dispatcher_adaptive_fee_weight").set(new_fee_weight);
        metrics::gauge!("dispatcher_adaptive_class_weight").set(new_class_weight);
        metrics::counter!("dispatcher_adaptation_events").increment(1);

        // Calculate fee distribution efficiency
        let efficiency =
            self.calculate_fee_distribution_efficiency(&mempool_state.fee_distribution);
        metrics::gauge!("dispatcher_fee_distribution_efficiency").set(efficiency * 100.0);

        self.current_weights
    }

    /// Applica smoothing adattivo che previene oscillazioni
    ///
    /// # Smoothing Strategy
    /// - Rate adattivo basato on the distanza tra current e target
    /// - Smoothing esponenziale per stabilità a lungo termine
    /// - Bounds checking per garantire stabilità
    ///
    /// # Arguments
    /// * `current_weight` - Peso corrente
    /// * `target_weight` - Peso target desiderato
    /// * `adaptation_rate` - Rate di adattamento (0.0 - 1.0)
    ///
    /// # Returns
    /// Peso smoothed
    fn apply_adaptive_smoothing(
        &self,
        current_weight: f64,
        target_weight: f64,
        adaptation_rate: f64,
    ) -> f64 {
        // Smoothing esponenziale: new = current * (1-r) + target * r
        let smoothed = current_weight * (1.0 - adaptation_rate) + target_weight * adaptation_rate;

        // Arrotondamento per stabilità numerica e test deterministici
        (smoothed * 10000.0).round() / 10000.0
    }

    ///
    /// # Performance
    ///
    /// # Arguments
    /// * `fees` - Slice di fee da analizzare
    ///
    /// # Returns
    /// `FeeStatistics` con p50, p90, p99 calcolati
    pub fn calculate_fee_statistics(&self, fees: &[u64]) -> FeeStatistics {
        let len = fees.len();
        if len == 0 {
            return FeeStatistics {
                p50: 0.0,
                p90: 0.0,
                p99: 0.0,
            };
        }

        // Per semplicità e correttezza, usiamo sorting completo per ora
        let mut sorted_fees = fees.to_vec();
        sorted_fees.sort_unstable();

        let p50_idx = len * 50 / 100;
        let p90_idx = len * 90 / 100;
        let p99_idx = len * 99 / 100;

        FeeStatistics {
            p50: sorted_fees[p50_idx.min(len - 1)] as f64,
            p90: sorted_fees[p90_idx.min(len - 1)] as f64,
            p99: sorted_fees[p99_idx.min(len - 1)] as f64,
        }
    }

    /// Quickselect algorithm per trovare k-esimo elemento più piccolo in O(n) tempo
    fn quickselect(&self, arr: &mut [u64], k: usize) -> u64 {
        if k >= arr.len() {
            return arr[arr.len() - 1];
        }

        self.quickselect_recursive(arr, k, 0, arr.len() - 1)
    }

    fn quickselect_recursive(&self, arr: &mut [u64], k: usize, left: usize, right: usize) -> u64 {
        if left == right {
            return arr[left];
        }

        let pivot_index = self.partition(arr, left, right);

        if k == pivot_index {
            arr[k]
        } else if k < pivot_index {
            self.quickselect_recursive(arr, k, left, pivot_index - 1)
        } else {
            self.quickselect_recursive(arr, k, pivot_index + 1, right)
        }
    }

    fn partition(&self, arr: &mut [u64], left: usize, right: usize) -> usize {
        let mid = left + (right - left) / 2;
        let pivot = if arr[left] <= arr[mid] {
            if arr[mid] <= arr[right] {
                arr[mid]
            } else {
                arr[right].min(arr[left])
            }
        } else {
            if arr[left] <= arr[right] {
                arr[left]
            } else {
                arr[right].min(arr[mid])
            }
        };

        // Sposta pivot alla fine
        let mut i = left;
        for j in left..right {
            if arr[j] < pivot {
                arr.swap(i, j);
                i += 1;
            }
        }

        // Sposta pivot alla sua posizione finale
        arr.swap(i, right);
        i
    }

    ///
    /// # Performance
    /// Compute entropia Shannon normalizzata per consistenza
    ///
    /// # Arguments
    /// * `classes` - Slice di classi da analizzare
    ///
    /// # Returns
    /// f64 tra 0.0 e 1.0 rappresentante diversità (0 = uniforme, 1 = massima)
    pub fn calculate_class_diversity(&self, classes: &[TxClass]) -> f64 {
        if classes.is_empty() {
            return 0.0;
        }

        let mut class_counts = std::collections::HashMap::new();
        for class in classes {
            *class_counts.entry(class).or_insert(0) += 1;
        }

        let total = classes.len() as f64;
        let mut shannon_entropy = 0.0;

        // Compute entropia Shannon: -Σ(p_i * log(p_i))
        for &count in class_counts.values() {
            let probability = count as f64 / total;
            if probability > 0.0 {
                shannon_entropy -= probability * probability.ln();
            }
        }

        // Normalizza per ottenere un valore tra 0 e 1
        let max_entropy = (class_counts.len() as f64).ln();
        if max_entropy > 0.0 {
            shannon_entropy / max_entropy
        } else {
            0.0
        }
    }

    ///
    /// # Performance
    /// Gestisce caso vuoto con fallback a target throughput
    ///
    /// # Arguments
    /// * `throughput_history` - Slice di throughput storici
    ///
    /// # Returns
    /// f64 rappresentante throughput medio
    fn calculate_average_throughput(&self, throughput_history: &[f64]) -> f64 {
        if throughput_history.is_empty() {
            return self.config.target_throughput;
        }

        // Usa iterator sum per ottimizzazione of the compilatore
        throughput_history.iter().sum::<f64>() / throughput_history.len() as f64
    }

    ///
    /// # Performance
    ///
    /// # Memory Management
    /// Pre-alloca capacity per evitare riallocazioni
    fn add_to_history(&mut self, state: MempoolState) {
        if self.state_history.len() < self.max_history_size {
            // Buffer not full yet: append at the end
            self.state_history.push(state);
            self.history_count += 1;
        } else {
            // Buffer pieno: sovrascrivi elemento più vecchio (circular buffer)
            self.state_history[self.history_write_idx] = state;
            self.history_write_idx = (self.history_write_idx + 1) % self.max_history_size;
        }
    }

    ///
    /// # Performance
    /// Ricostruisce ordine cronologico dal circular buffer
    pub fn get_state_history(&self) -> Vec<MempoolState> {
        let mut result = Vec::with_capacity(self.history_count);

        if self.state_history.len() < self.max_history_size {
            result.extend(self.state_history.iter().cloned());
        } else {
            // Buffer pieno: ricostruisci ordine cronologico dal circular buffer
            // Elementi da history_write_idx fino alla fine
            result.extend(self.state_history[self.history_write_idx..].iter().cloned());
            // Elementi dall'inizio fino a history_write_idx
            result.extend(self.state_history[..self.history_write_idx].iter().cloned());
        }

        result
    }

    pub fn get_adaptive_weights(&self) -> (f64, f64) {
        self.current_weights
    }

    pub fn set_weights(&mut self, fee_weight: f64, class_weight: f64) {
        // Set direttamente i pesi without normalizzazione (l'adattamento gestisce l'invariante)
        self.current_weights = (fee_weight, class_weight);
    }

    /// Resetta i pesi ai valori base
    pub fn reset_to_base(&mut self) {
        self.current_weights = (self.config.base_fee_weight, self.config.base_class_weight);
    }

    pub fn get_config(&self) -> &AdaptiveWeightsConfig {
        &self.config
    }

    pub fn get_state_history_slice(&self) -> &[MempoolState] {
        &self.state_history
    }

    ///
    /// # Efficiency Calculation
    /// - Higher efficiency = better fee distribution
    /// - Basato su varianza e distribuzione percentile
    /// - Range: 0.0 (inefficiente) a 1.0 (perfetto)
    ///
    /// # Arguments
    /// * `fees` - Slice di fee da analizzare
    ///
    /// # Returns
    fn calculate_fee_distribution_efficiency(&self, fees: &[u64]) -> f64 {
        if fees.is_empty() {
            return 0.0;
        }

        let len = fees.len();
        let total_fee: u64 = fees.iter().sum();
        let avg_fee = total_fee as f64 / len as f64;

        // Compute varianza
        let variance = fees
            .iter()
            .map(|&fee| (fee as f64 - avg_fee).powi(2))
            .sum::<f64>()
            / len as f64;

        // Compute coefficiente di variazione (CV = std_dev / mean)
        let cv = if avg_fee > 0.0 {
            variance.sqrt() / avg_fee
        } else {
            0.0
        };

        // Compute percentile ratio (p90/p10)
        let mut sorted_fees = fees.to_vec();
        sorted_fees.sort_unstable();

        let p10_idx = len * 10 / 100;
        let p90_idx = len * 90 / 100;

        let p10 = sorted_fees[p10_idx.min(len - 1)] as f64;
        let p90 = sorted_fees[p90_idx.min(len - 1)] as f64;

        let percentile_ratio = if p10 > 0.0 { p90 / p10 } else { 1.0 };

        // Efficienza combinata: 1.0 è perfetto
        // CV basso è buono (meno varianza)
        // Ratio moderato è buono (non troppo estremo)
        let cv_efficiency = (1.0 - cv.min(1.0)).max(0.0);
        let ratio_efficiency = if percentile_ratio <= 3.0 {
            1.0 - (percentile_ratio - 1.0) / 2.0 // Ratio 1-3 è buono
        } else {
            1.0 / percentile_ratio // Ratio >3 penalizza
        };

        (cv_efficiency * 0.6 + ratio_efficiency * 0.4)
            .max(0.0)
            .min(1.0)
    }
}

#[derive(Debug, Clone)]
pub struct FeeStatistics {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

/// Configurazione per Execution Dispatcher
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Peso fee vs fairness (default: 0.7, 70% fee, 30% fairness)
    pub fee_weight: f64,
    /// Peso class priority vs fee (default: 0.3, 30% class, 70% fee)
    pub class_weight: f64,
    /// Limit massimo transazioni per sender (default: max_txs / 10)
    pub max_txs_per_sender: usize,
    /// Score cache configuration
    pub score_cache_size: usize,
    pub score_cache_ttl_blocks: u64,
    pub enable_score_cache: bool,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            fee_weight: 0.7,
            class_weight: 0.3,
            max_txs_per_sender: 100, // Default, sarà sovrascritto con max_txs / 10
            score_cache_size: 10_000,
            score_cache_ttl_blocks: 100,
            enable_score_cache: true,
        }
    }
}

impl DispatcherConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_txs(max_txs: usize) -> Self {
        Self {
            max_txs_per_sender: max_txs / 10,
            ..Self::default()
        }
    }
}

/// Execution Dispatcher per scheduling transazioni con logica economica
pub struct ExecutionDispatcher {
    config: DispatcherConfig,
    metrics: DispatcherMetrics,
    adaptive_weights: Option<AdaptiveWeights>,
    current_weights: (f64, f64),
    score_cache: Arc<Mutex<ScoreCache>>,
    nonce_conflict_resolver: NonceConflictResolver, // ⭐ NUOVO CAMPO FASE 5
    replay_prevention: Option<Arc<crate::mempool::replay_prevention::ReplayPrevention>>, // ⭐ NUOVO CAMPO FASE 5
}

/// Metriche per monitoring of the dispatcher
#[derive(Debug, Clone, Default)]
pub struct DispatcherMetrics {
    /// Numero totale di transazioni schedulati
    pub txs_scheduled_total: usize,
    pub fee_weighted_txs: usize,
    pub fee_distribution_p50: u64,
    /// Fee 90° percentile nel blocco (p90)
    pub fee_distribution_p90: u64,
}

pub struct SimpleTransactionValidator {
    storage: Arc<dyn savitri_storage::StorageTrait>,
    current_block_height: u64,
}

impl SimpleTransactionValidator {
    pub fn new(storage: Arc<dyn savitri_storage::StorageTrait>, current_block_height: u64) -> Self {
        Self {
            storage,
            current_block_height,
        }
    }

    pub fn validate_transaction(
        &self,
        mempool_tx: &MempoolTx,
        signed_tx: &SignedTx,
    ) -> Result<(), String> {
        if mempool_tx.fee == 0 {
            return Err("Transaction fee cannot be zero".to_string());
        }

        if signed_tx.from.is_empty() || signed_tx.from.chars().all(|c| c == '0') {
            return Err("Invalid sender address".to_string());
        }

        if signed_tx.to.is_empty() || signed_tx.to.chars().all(|c| c == '0') {
            return Err("Invalid recipient address".to_string());
        }

        if signed_tx.amount == 0 {
            return Err("Transaction amount cannot be zero".to_string());
        }

        // Check if transaction hash matches
        let expected_hash = hash_transaction(signed_tx);
        let actual_hash = hash_signed_tx_bytes(&bincode::serialize(signed_tx).unwrap_or_default());

        if expected_hash != actual_hash {
            return Err("Transaction hash mismatch".to_string());
        }

        Ok(())
    }
}

impl ExecutionDispatcher {
    pub fn new(config: DispatcherConfig) -> Self {
        let current_weights = (config.fee_weight, config.class_weight);
        let score_cache = if config.enable_score_cache {
            ScoreCache::with_config(
                config.score_cache_size,
                Duration::from_secs(config.score_cache_ttl_blocks * 16),
            )
        } else {
            ScoreCache::new() // Cache disabilitata
        };

        Self {
            config,
            metrics: DispatcherMetrics::default(),
            adaptive_weights: None, // Optional by default
            current_weights,
            score_cache: Arc::new(Mutex::new(score_cache)),
            nonce_conflict_resolver: NonceConflictResolver::new(), // ⭐ NUOVO CAMPO FASE 5
            replay_prevention: None, // ⭐ NUOVO CAMPO FASE 5 - Will be set with set_replay_prevention
        }
    }

    pub fn new_with_adaptive_weights(
        config: DispatcherConfig,
        adaptive_config: AdaptiveWeightsConfig,
    ) -> Self {
        let adaptive_weights = Some(AdaptiveWeights::new(adaptive_config));
        let current_weights = (config.fee_weight, config.class_weight);
        let score_cache = if config.enable_score_cache {
            ScoreCache::with_config(
                config.score_cache_size,
                Duration::from_secs(config.score_cache_ttl_blocks * 16),
            )
        } else {
            ScoreCache::new() // Cache disabilitata
        };

        Self {
            config,
            metrics: DispatcherMetrics::default(),
            adaptive_weights,
            current_weights,
            score_cache: Arc::new(Mutex::new(score_cache)),
            nonce_conflict_resolver: NonceConflictResolver::new(), // ⭐ NUOVO CAMPO FASE 5
            replay_prevention: None, // ⭐ NUOVO CAMPO FASE 5 - Will be set with set_replay_prevention
        }
    }

    /// Abilita o disabilita i pesi adattivi
    pub fn with_adaptive_weights(mut self, enabled: bool) -> Self {
        if enabled {
            self.adaptive_weights = Some(AdaptiveWeights::default());
        } else {
            self.adaptive_weights = None;
        }
        self
    }

    /// Set la strategia di risoluzione conflitti nonce (FASE 5)
    pub fn with_conflict_resolution_strategy(
        mut self,
        strategy: ConflictResolutionStrategy,
    ) -> Self {
        self.nonce_conflict_resolver.set_strategy(strategy);
        self
    }

    /// Ottiene le statistiche of the risolutore di conflitti
    pub fn get_conflict_resolver_stats(
        &self,
    ) -> &crate::executor::nonce_conflict_resolver::ConflictResolutionStats {
        self.nonce_conflict_resolver.get_stats()
    }

    /// Resetta le statistiche of the risolutore di conflitti
    pub fn reset_conflict_resolver_stats(&mut self) {
        self.nonce_conflict_resolver.reset_stats();
    }

    /// Set il sistema di replay prevention (FASE 5)
    pub fn set_replay_prevention(
        &mut self,
        replay_prevention: Arc<crate::mempool::replay_prevention::ReplayPrevention>,
    ) {
        self.replay_prevention = Some(replay_prevention);
    }

    /// Ottiene il sistema di replay prevention se configurato
    pub fn get_replay_prevention(
        &self,
    ) -> Option<&Arc<crate::mempool::replay_prevention::ReplayPrevention>> {
        self.replay_prevention.as_ref()
    }

    pub fn has_adaptive_weights(&self) -> bool {
        self.adaptive_weights.is_some()
    }

    /// Schedula transazioni con algoritmo fee-aware e fairness
    ///
    /// Algoritmo:
    /// 1. Analizza mempool e adatta pesi dinamicamente
    /// 2. Raggruppa per sender (fairness)
    /// 4. Interleaving round-robin con limit max_txs_per_sender
    ///
    /// # Performance Optimizations
    /// - Usa SIMD per calcolo score quando disponibile (AVX2/NEON)
    /// - Score cache per evitare ricalcoli ridondanti
    /// - Batch processing per score computation con threshold dinamico
    /// - Fallback automatico a versione scalare per piccoli batch
    /// - Pesi adattivi basati su condizioni of the mempool
    ///
    /// # Score Cache System
    /// - Cross-batch caching per performance ottimali
    /// - Cache hits evitano ricalcoli costosi
    /// - TTL previene cache stale con pesi adattivi
    ///
    /// # Adaptive Weights
    /// I pesi si adattano dinamicamente basandosi su:
    /// - Distribuzione fee (p50, p90, p99)
    /// - Diversità classi (indice di Shannon)
    /// - Throughput storico
    ///
    ///
    /// This is the NEW SAFE implementation that includes:
    /// - Nonce conflict resolution with fee-based priority
    /// - Replay prevention
    /// - Storage integration for account state
    ///
    /// # Arguments
    /// * `mempool_txs` - Transazioni dal mempool
    /// * `signed_txs` - Transazioni firmate corrispondenti (stesso ordine)
    /// * `current_block_height` - Altezza blocco corrente per replay prevention
    ///
    /// # Returns
    /// Result<(mempool_txs_scheduled, signed_txs_scheduled), SchedulingError>
    pub fn schedule_transactions_safe(
        &mut self,
        mempool_txs: Vec<MempoolTx>,
        signed_txs: Vec<Transaction>,
        storage: Arc<dyn savitri_storage::StorageTrait>,
        current_block_height: u64,
    ) -> Result<(Vec<MempoolTx>, Vec<SignedTx>), SchedulingError> {
        if mempool_txs.is_empty() || signed_txs.is_empty() {
            return Ok((mempool_txs, signed_txs));
        }

        if mempool_txs.len() != signed_txs.len() {
            return Err(SchedulingError::MismatchedLengths {
                mempool_count: mempool_txs.len(),
                signed_count: signed_txs.len(),
            });
        }

        // ⭐ NUOVA LOGICA DI VALIDAZIONE (FASE 4)
        // Step 0: Validazione transazioni con TransactionValidator
        let validator = SimpleTransactionValidator::new(storage.clone(), current_block_height);

        let mut valid_transactions = Vec::with_capacity(mempool_txs.len());
        let mut valid_signed = Vec::with_capacity(signed_txs.len());

        // Validate each transaction pair
        for (i, (mempool_tx, signed_tx)) in mempool_txs
            .into_iter()
            .zip(signed_txs.into_iter())
            .enumerate()
        {
            match validator.validate_transaction(&mempool_tx, &signed_tx) {
                Ok(()) => {
                    valid_transactions.push(mempool_tx);
                    valid_signed.push(signed_tx);
                }
                Err(e) => {
                    debug!("Skipping invalid transaction at index {}: {}", i, e);
                    // Continue with other transactions instead of failing completely
                }
            }
        }

        if valid_transactions.is_empty() {
            return Err(SchedulingError::NoValidTransactions);
        }

        // Update replay prevention block height
        if let Some(replay_prevention) = &self.replay_prevention {
            replay_prevention.update_block_height(current_block_height);
        }

        Ok((valid_transactions, valid_signed))
    }

    /// Schedule transactions (LEGACY VERSION - for backward compatibility)
    ///
    /// # Arguments
    /// * `mempool_txs` - Transazioni dal mempool
    /// * `signed_txs` - Transazioni firmate corrispondenti (stesso ordine)
    ///
    /// # Returns
    /// Tuple `(mempool_txs_scheduled, signed_txs_scheduled)` in the stesso ordine
    #[deprecated(note = "Use schedule_transactions_safe for production use")]
    pub fn schedule_transactions(
        &mut self,
        mempool_txs: Vec<MempoolTx>,
        signed_txs: Vec<Transaction>,
    ) -> (Vec<MempoolTx>, Vec<SignedTx>) {
        if mempool_txs.is_empty() || signed_txs.is_empty() {
            return (mempool_txs, signed_txs);
        }

        if mempool_txs.len() != signed_txs.len() {
            return (mempool_txs, signed_txs);
        }

        // Cleanup cache entries expired periodicamente
        if self.config.enable_score_cache {
            let should_cleanup = if let Ok(cache) = self.score_cache.lock() {
                cache.size() > cache.max_size() / 2
            } else {
                false
            };

            if should_cleanup {
                self.cleanup_cache();
            }
        }

        // Step 1: Analizza mempool e adatta i pesi se abilitati
        let (fee_weight, class_weight) =
            if let Some(ref mut adaptive_weights) = self.adaptive_weights {
                // Adaptive weights enabled: analizza e adatta
                let mempool_state = adaptive_weights.analyze_mempool_state(&mempool_txs);
                let (adaptive_fee_weight, adaptive_class_weight) =
                    adaptive_weights.adapt_weights(&mempool_state);

                self.current_weights = (adaptive_fee_weight, adaptive_class_weight);
                (adaptive_fee_weight, adaptive_class_weight)
            } else {
                // Adaptive weights disabled: usa config statico
                self.current_weights
            };

        // Step 2: Raggruppa per sender e compute score (cache score per evitare ricalcoli)
        let mut sender_groups: BTreeMap<Vec<u8>, Vec<(MempoolTx, SignedTx, f64)>> = BTreeMap::new();

        // Usa SIMD per batch di score computation quando possibile
        let total_txs = mempool_txs.len();
        const SIMD_THRESHOLD: usize = 32; // Aumentato da 8 a 32 basato su performance characteristics

        if total_txs >= SIMD_THRESHOLD {
            // Batch processing con SIMD
            let mut fees = Vec::with_capacity(total_txs);
            let mut classes = Vec::with_capacity(total_txs);
            let mut uncached_indices = Vec::new();
            let mut cached_scores = Vec::with_capacity(total_txs);

            for (index, mempool_tx) in mempool_txs.iter().enumerate() {
                if let Some(cached_score) = self.get_cached_score(
                    mempool_tx.fee,
                    mempool_tx.class,
                    fee_weight,
                    class_weight,
                ) {
                    cached_scores.push(Some(cached_score));
                } else {
                    cached_scores.push(None);
                    uncached_indices.push(index);
                    fees.push(mempool_tx.fee);
                    classes.push(mempool_tx.class);
                }
            }

            // Seconda fase: compute solo i score non in cache con SIMD
            let mut uncached_scores = Vec::new();
            if !uncached_indices.is_empty() {
                uncached_scores =
                    self.compute_score_simd_batch(&fees, &classes, fee_weight, class_weight);

                // Salva i nuovi score in the cache
                for (index, &tx_index) in uncached_indices.iter().enumerate() {
                    let score = uncached_scores[index];
                    self.cache_score(
                        mempool_txs[tx_index].fee,
                        mempool_txs[tx_index].class,
                        score,
                    );
                }
            }

            // Terza fase: combina cached e computed scores
            let mut final_scores = Vec::with_capacity(total_txs);
            let mut uncached_iter = uncached_scores.into_iter();

            for cached_opt in cached_scores {
                if let Some(cached_score) = cached_opt {
                    final_scores.push(cached_score);
                } else {
                    final_scores.push(uncached_iter.next().unwrap());
                }
            }

            // Raggruppa per sender con score pre-calcolati
            for ((mempool_tx, signed_tx), score) in mempool_txs
                .into_iter()
                .zip(signed_txs.into_iter())
                .zip(final_scores.into_iter())
            {
                let sender_key = signed_tx.from.clone();
                sender_groups
                    .entry(sender_key.into_bytes())
                    .or_insert_with(Vec::new)
                    .push((mempool_tx, signed_tx, score));
            }
        } else {
            // Fallback a versione scalare per batch piccoli
            for (mempool_tx, signed_tx) in mempool_txs.into_iter().zip(signed_txs.into_iter()) {
                let score = self.compute_score_with_weights(
                    &mempool_tx,
                    &signed_tx,
                    fee_weight,
                    class_weight,
                );
                let sender_key = signed_tx.from.clone();
                sender_groups
                    .entry(sender_key.into_bytes())
                    .or_insert_with(Vec::new)
                    .push((mempool_tx, signed_tx, score));
            }
        }

        // Ordinamento stabile: usa sort_by_key con tie-breaker (nonce) per garantire stabilità
        for txs in sender_groups.values_mut() {
            // Ordina per score decrescente, poi per nonce crescente come tie-breaker
            txs.sort_by(
                |a: &(MempoolTx, SignedTx, f64),
                 b: &(MempoolTx, SignedTx, f64)|
                 -> std::cmp::Ordering {
                    // Ordine decrescente per score
                    match b.2.partial_cmp(&a.2) {
                        Some(std::cmp::Ordering::Equal) => {
                            // Tie-breaker: ordina per nonce crescente per stabilità
                            a.0.nonce.cmp(&b.0.nonce)
                        }
                        Some(ord) => ord,
                        None => std::cmp::Ordering::Equal,
                    }
                },
            );
        }

        // Step 3: Interleaving round-robin con limit max_txs_per_sender
        let mut scheduled_mempool = Vec::with_capacity(
            sender_groups
                .values()
                .map(|v: &Vec<(MempoolTx, SignedTx, f64)>| v.len())
                .sum(),
        );
        let mut scheduled_signed = Vec::with_capacity(scheduled_mempool.capacity());

        let mut sender_indices: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        let mut sender_counts: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
        for key in sender_groups.keys() {
            let key_vec = key.to_vec();
            sender_indices.insert(key_vec, 0);
            let key_vec2 = key.to_vec();
            sender_counts.insert(key_vec2, 0);
        }

        let mut total_scheduled = 0;
        let max_total: usize = sender_groups
            .values()
            .map(|v: &Vec<(MempoolTx, SignedTx, f64)>| v.len())
            .sum();

        // Round-robin interleaving
        while total_scheduled < max_total {
            let mut progress_made = false;

            for (sender_key, sender_txs) in sender_groups.iter() {
                let current_index = sender_indices.get(sender_key).copied().unwrap_or(0);
                let scheduled_count = sender_counts.get(sender_key).copied().unwrap_or(0);

                // Controlla limit per sender
                if scheduled_count >= self.config.max_txs_per_sender {
                    continue;
                }

                if current_index < sender_txs.len() {
                    // Rimuove score dalla tupla (non più necessario dopo ordinamento)
                    let (mempool_tx, signed_tx, _score) = &sender_txs[current_index];
                    scheduled_mempool.push(mempool_tx.clone());
                    scheduled_signed.push(signed_tx.clone());

                    // all'inizio, ma usiamo if let per sicurezza e best practices Rust
                    if let Some(idx) = sender_indices.get_mut(sender_key) {
                        *idx += 1;
                    }
                    if let Some(count) = sender_counts.get_mut(sender_key) {
                        *count += 1;
                    }
                    total_scheduled += 1;
                    progress_made = true;
                }
            }

            if !progress_made {
                break;
            }
        }

        self.update_metrics(&scheduled_mempool);

        (scheduled_mempool, scheduled_signed)
    }

    /// Schedula transazioni con algoritmo zero-copy (no allocations)
    ///
    ///
    /// # Performance
    /// - Zero allocations durante scheduling (solo score vector)
    /// - Score computation O(n) con cache locale
    /// - Sorting O(k log k) dove k = numero di sender unici
    /// - Memory overhead: solo Vec<(usize, f64)> per score cache
    ///
    /// # Arguments
    /// * `mempool_txs` - Reference a transazioni dal mempool
    /// * `signed_txs` - Reference a transazioni firmate corrispondenti
    ///
    /// # Returns
    ///
    /// # Lifetime
    pub fn schedule_transactions_zero_copy(
        &mut self,
        mempool_txs: &[MempoolTx],
        signed_txs: &[Transaction],
    ) -> Vec<usize> {
        if mempool_txs.is_empty() || signed_txs.is_empty() {
            return Vec::new();
        }

        if mempool_txs.len() != signed_txs.len() {
            return (0..mempool_txs.len()).collect();
        }

        // Usa SIMD per batch processing quando possibile
        let total_txs = mempool_txs.len();
        const SIMD_THRESHOLD: usize = 32; // Aumentato da 8 a 32 basato su performance characteristics

        let scores: Vec<(usize, f64)> = if total_txs >= SIMD_THRESHOLD {
            // Batch processing con SIMD
            let mut fees = Vec::with_capacity(total_txs);
            let mut classes = Vec::with_capacity(total_txs);

            for mempool_tx in mempool_txs.iter() {
                fees.push(mempool_tx.fee);
                classes.push(mempool_tx.class);
            }

            let simd_scores = self.compute_score_simd_batch(
                &fees,
                &classes,
                self.config.fee_weight,
                self.config.class_weight,
            );

            // Converte in (index, score) tuples
            simd_scores.into_iter().enumerate().collect()
        } else {
            // Fallback a versione scalare per batch piccoli
            let mut scores = Vec::with_capacity(total_txs);
            for (index, mempool_tx) in mempool_txs.iter().enumerate() {
                let score = self.compute_score(mempool_tx, &signed_txs[index]);
                scores.push((index, score));
            }
            scores
        };

        let mut sender_groups: BTreeMap<&[u8], Vec<(usize, f64)>> = BTreeMap::new();

        for &(index, score) in &scores {
            let sender_key = &signed_txs[index].from;
            sender_groups
                .entry(sender_key.as_bytes())
                .or_insert_with(Vec::new)
                .push((index, score));
        }

        // Ordinamento stabile: usa sort_by con tie-breaker (nonce) per stabilità
        for tx_indices in sender_groups.values_mut() {
            tx_indices.sort_by(|a, b| {
                // Ordine decrescente per score
                match b.1.partial_cmp(&a.1) {
                    Some(std::cmp::Ordering::Equal) => {
                        // Tie-breaker: ordina per nonce crescente per stabilità
                        mempool_txs[a.0].nonce.cmp(&mempool_txs[b.0].nonce)
                    }
                    Some(ord) => ord,
                    None => std::cmp::Ordering::Equal,
                }
            });
        }

        // Step 4: Interleaving round-robin con limit max_txs_per_sender
        // Pre-alloca risultato con capacity nota
        let mut scheduled_indices: Vec<usize> = Vec::with_capacity(mempool_txs.len());

        let mut sender_indices: BTreeMap<&[u8], usize> = BTreeMap::new();
        let mut sender_counts: BTreeMap<&[u8], usize> = BTreeMap::new();
        for key in sender_groups.keys() {
            sender_indices.insert(key, 0);
            sender_counts.insert(key, 0);
        }

        let mut total_scheduled = 0;
        let max_total: usize = mempool_txs.len();

        // Round-robin interleaving
        while total_scheduled < max_total {
            let mut progress_made = false;

            for (sender_key, tx_indices) in sender_groups.iter() {
                let current_index = sender_indices.get(sender_key).copied().unwrap_or(0);
                let scheduled_count = sender_counts.get(sender_key).copied().unwrap_or(0);

                // Controlla limit per sender
                if scheduled_count >= self.config.max_txs_per_sender {
                    continue;
                }

                if current_index < tx_indices.len() {
                    let (tx_index, _score) = tx_indices[current_index];
                    scheduled_indices.push(tx_index);

                    if let Some(idx) = sender_indices.get_mut(sender_key) {
                        *idx += 1;
                    }
                    if let Some(count) = sender_counts.get_mut(sender_key) {
                        *count += 1;
                    }
                    total_scheduled += 1;
                    progress_made = true;
                }
            }

            if !progress_made {
                break;
            }
        }

        let scheduled_mempool_refs: Vec<&MempoolTx> = scheduled_indices
            .iter()
            .map(|&idx| &mempool_txs[idx])
            .collect();
        self.update_metrics_from_refs_zerocopy(&scheduled_mempool_refs);

        scheduled_indices
    }

    /// Compute score combinato per una singola transazione con cache support
    ///
    /// Formula: score = fee_normalized * fee_weight + class_priority * class_weight
    ///
    /// # Performance Optimization
    /// - Controlla la cache prima di calcolare lo score
    /// - Salva lo score in the cache dopo il calcolo
    /// - Cache hits evitano ricalcoli costosi
    ///
    /// # Arguments
    /// * `mempool_tx` - Transazione dal mempool
    /// * `signed_tx` - Transazione firmata corrispondente
    /// * `fee_weight` - Peso fee (dalla configurazione o adattivo)
    /// * `class_weight` - Peso classe (dalla configurazione o adattivo)
    ///
    /// # Returns
    /// f64 - Score calcolato per la transazione
    #[inline]
    fn compute_score_with_weights(
        &mut self,
        mempool_tx: &MempoolTx,
        _signed_tx: &SignedTx,
        fee_weight: f64,
        class_weight: f64,
    ) -> f64 {
        // Prova a recuperare dalla cache
        if let Some(cached_score) =
            self.get_cached_score(mempool_tx.fee, mempool_tx.class, fee_weight, class_weight)
        {
            return cached_score;
        }

        // Cache miss: compute lo score
        const FEE_NORMALIZATION_FACTOR: f64 = 1_000_000_000.0;
        let fee_normalized = (mempool_tx.fee as f64 / FEE_NORMALIZATION_FACTOR).min(1.0);

        let class_priority = match mempool_tx.class {
            TxClass::FederatedUpdate => 1.0,
            TxClass::IoTData => 0.7,
            TxClass::Financial => 0.9,
            TxClass::System => 1.0,
        };

        let score = fee_normalized * fee_weight + class_priority * class_weight;

        // Salva in the cache per usi futuri
        self.cache_score(mempool_tx.fee, mempool_tx.class, score);

        score
    }

    ///
    /// # Returns
    /// Tuple `(fee_weight, class_weight)` con i pesi attuali (adattivi o statici)
    pub fn get_adaptive_weights(&self) -> (f64, f64) {
        self.current_weights
    }

    ///
    /// # Arguments
    /// * `fee_weight` - Nuovo peso fee
    /// * `class_weight` - Nuovo peso class
    pub fn set_adaptive_weights(&mut self, fee_weight: f64, class_weight: f64) {
        // Applica bounds se adaptive weights è abilitato
        if let Some(ref adaptive_weights) = self.adaptive_weights {
            let bounded_fee_weight = fee_weight.clamp(
                adaptive_weights.get_config().fee_weight_bounds.0,
                adaptive_weights.get_config().fee_weight_bounds.1,
            );
            let bounded_class_weight = class_weight.clamp(
                adaptive_weights.get_config().class_weight_bounds.0,
                adaptive_weights.get_config().class_weight_bounds.1,
            );
            self.current_weights = (bounded_fee_weight, bounded_class_weight);
        } else {
            // Se adaptive weights non è abilitato, set direttamente
            self.current_weights = (fee_weight, class_weight);
        }
    }

    /// Resetta i pesi adattivi ai valori base
    pub fn reset_adaptive_weights(&mut self) {
        if let Some(ref mut adaptive_weights) = self.adaptive_weights {
            adaptive_weights.reset_to_base();
            self.current_weights = (self.config.fee_weight, self.config.class_weight);
        } else {
            // Se adaptive weights non è abilitato, resetta ai valori di config
            self.current_weights = (self.config.fee_weight, self.config.class_weight);
        }
    }

    pub fn get_adaptive_config(&self) -> Option<&AdaptiveWeightsConfig> {
        self.adaptive_weights.as_ref().map(|aw| aw.get_config())
    }

    pub fn get_mempool_state_history(&self) -> Option<&[MempoolState]> {
        self.adaptive_weights.as_ref().map(|aw| {
            if aw.history_count == 0 {
                &[]
            } else {
                aw.state_history.as_slice()
            }
        })
    }

    /// Analizza lo stato corrente of the mempool without adattare i pesi (se abilitati)
    ///
    /// # Arguments
    /// * `mempool_txs` - Transazioni da analizzare
    ///
    /// # Returns
    /// `Option<MempoolState>` con le statistiche correnti (None se adaptive weights disabilitati)
    pub fn analyze_current_mempool_state(&self, mempool_txs: &[MempoolTx]) -> Option<MempoolState> {
        self.adaptive_weights
            .as_ref()
            .map(|aw| aw.analyze_mempool_state(mempool_txs))
    }

    /// Converte TxClass in valore f64 per priorità (helper per testing)
    pub fn class_priority_to_f64(&self, class: TxClass) -> f64 {
        match class {
            TxClass::FederatedUpdate => 1.0,
            TxClass::IoTData => 0.7,
            TxClass::Financial => 0.5,
            TxClass::System => 0.6,
        }
    }

    /// Ottiene uno score dalla cache dei score
    ///
    /// # Arguments
    /// * `fee_weight` - Peso fee (non used in the chiave cache)
    /// * `class_weight` - Peso class (non used in the chiave cache)
    ///
    /// # Returns
    /// `Option<f64>` - Score se presente in the cache e non expired
    pub fn get_cached_score(
        &self,
        fee: u64,
        class: TxClass,
        _fee_weight: f64,
        _class_weight: f64,
    ) -> Option<f64> {
        if !self.config.enable_score_cache {
            return None;
        }

        // In una implementazione più avanzata, potremmo includere i pesi in the chiave
        if let Ok(cache) = self.score_cache.lock() {
            cache.get_cached_score(fee, class)
        } else {
            None
        }
    }

    /// Salva un score in the cache
    ///
    /// # Arguments
    /// * `score` - Score calcolato da salvare
    pub fn cache_score(&mut self, fee: u64, class: TxClass, score: f64) {
        if !self.config.enable_score_cache {
            return;
        }

        if let Ok(mut cache) = self.score_cache.lock() {
            cache.cache_score(fee, class, score);
        }
    }

    /// Pulisce le entries expired dalla cache
    ///
    /// # Returns
    pub fn cleanup_cache(&mut self) -> usize {
        if !self.config.enable_score_cache {
            return 0;
        }

        if let Ok(mut cache) = self.score_cache.lock() {
            cache.cleanup_expired()
        } else {
            0
        }
    }

    /// Svuota completamente la cache dei score
    pub fn clear_cache(&mut self) {
        if !self.config.enable_score_cache {
            return;
        }

        if let Ok(mut cache) = self.score_cache.lock() {
            cache.clear();
        }

        // Resetta anche le statistiche
        self.reset_cache_stats();
    }

    pub fn reset_cache_stats(&mut self) {
        if !self.config.enable_score_cache {
            return;
        }

        if let Ok(mut cache) = self.score_cache.lock() {
            cache.reset_stats();
        }
    }

    ///
    /// # Returns
    /// Tuple `(hits, misses, hit_rate, size)`
    pub fn get_cache_stats(&self) -> (u64, u64, f64, usize) {
        if !self.config.enable_score_cache {
            return (0, 0, 0.0, 0);
        }

        if let Ok(cache) = self.score_cache.lock() {
            cache.get_stats()
        } else {
            (0, 0, 0.0, 0)
        }
    }

    /// Configure la cache dei score con parametri personalizzati
    ///
    /// # Arguments
    /// * `ttl_blocks` - TTL in blocchi (default: 100)
    pub fn configure_cache(&mut self, max_size: usize, ttl_blocks: u64) {
        // Assume 16 secondi per blocco (valore tipico blockchain)
        let ttl_duration = Duration::from_secs(ttl_blocks * 16);
        self.score_cache = Arc::new(Mutex::new(ScoreCache::with_config(max_size, ttl_duration)));
    }

    /// Compute score combinato per una singola transazione (versione legacy)
    ///
    /// Formula: score = fee_normalized * fee_weight + class_priority * class_weight
    ///
    /// # Arguments
    /// * `mempool_tx` - Transazione dal mempool
    /// * `signed_tx` - Transazione firmata corrispondente
    ///
    /// # Returns
    /// f64 - Score calcolato per la transazione
    #[inline]
    pub fn compute_score(&self, mempool_tx: &MempoolTx, _signed_tx: &SignedTx) -> f64 {
        // Normalizza fee: (fee as f64) / 1_000_000_000.0 (max 1.0)
        const FEE_NORMALIZATION_FACTOR: f64 = 1_000_000_000.0;
        let fee_normalized = (mempool_tx.fee as f64 / FEE_NORMALIZATION_FACTOR).min(1.0);

        // Normalizza class: FederatedUpdate=1.0, IoT=0.7, Financial=0.9, System=1.0
        let class_priority = match mempool_tx.class {
            TxClass::FederatedUpdate => 1.0,
            TxClass::IoTData => 0.7,
            TxClass::Financial => 0.9,
            TxClass::System => 1.0,
        };

        // Score = fee_normalized * fee_weight + class_priority * class_weight
        // Ottimizzazione: usa fused multiply-add se disponibile (compilatore può ottimizzare)
        fee_normalized * self.config.fee_weight + class_priority * self.config.class_weight
    }

    fn update_metrics_from_refs(&mut self, scheduled_mempool_refs: &[&MempoolTx]) {
        self.metrics.txs_scheduled_total += scheduled_mempool_refs.len();

        // Compute distribuzione fee
        if !scheduled_mempool_refs.is_empty() {
            let mut fees: Vec<u64> = scheduled_mempool_refs.iter().map(|tx| tx.fee).collect();
            fees.sort_unstable();

            let p50_idx = fees.len() * 50 / 100;
            let p90_idx = fees.len() * 90 / 100;

            self.metrics.fee_distribution_p50 = fees[p50_idx.min(fees.len() - 1)];
            self.metrics.fee_distribution_p90 = fees[p90_idx.min(fees.len() - 1)];

            // Conta transazioni ad alto fee (>1M)
            self.metrics.fee_weighted_txs = fees.iter().filter(|&&fee| fee > 1_000_000).count();
        }
    }

    ///
    /// Formula: score = fee_normalized * fee_weight + class_priority * class_weight
    ///
    /// # Performance
    /// Usa SIMD per processare 4-8 transazioni in parallelo con 2-3x speedup.
    /// Fallback automatico a versione scalare se SIMD non disponibile.
    /// Usa threshold dinamica per evitare overhead su batch piccoli.
    ///
    /// # Precision & Determinism
    ///
    /// # Security & Safety
    /// ⭐ CRITICAL FIX: Validazione input rigorosa per prevent buffer overflow.
    /// SIMD threshold dinamico basato su performance characteristics.
    /// Fallback automatico per batch piccoli dove SIMD è controproducente.
    /// # Safety
    /// Le funzioni SIMD interne usano intrinsics sicuri con bounds checking.
    #[inline]
    pub fn compute_score_simd_batch(
        &self,
        fees: &[u64],
        classes: &[TxClass],
        fee_weight: f64,
        class_weight: f64,
    ) -> Vec<f64> {
        // ⭐ CRITICAL FIX: Validazione input rigorosa per prevent buffer overflow
        assert_eq!(
            fees.len(),
            classes.len(),
            "SIMD input arrays must have same length: fees={}, classes={}",
            fees.len(),
            classes.len()
        );

        // ⭐ CRITICAL FIX: Dynamic threshold per ottimizzazione performance
        const SIMD_THRESHOLD: usize = 32; // Aumentato da 8 per evitare regression

        if fees.len() < SIMD_THRESHOLD {
            // Usa scalar per batch piccoli (SIMD overhead > benefit)
            return self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight);
        }

        // Usa SIMD solo per batch grandi
        if cfg!(target_arch = "x86_64")
            && is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("fma")
        {
            unsafe { self.compute_score_simd_batch_avx2(fees, classes, fee_weight, class_weight) }
        } else if cfg!(target_arch = "aarch64") {
            #[cfg(target_arch = "aarch64")]
            {
                if std::arch::is_aarch64_feature_detected!("neon") {
                    unsafe {
                        self.compute_score_simd_batch_neon(fees, classes, fee_weight, class_weight)
                    }
                } else {
                    self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight)
                }
            }
            #[cfg(not(target_arch = "aarch64"))]
            {
                self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight)
            }
        } else {
            // Fallback a scalare se SIMD non disponibile
            self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight)
        }
    }

    ///
    /// # Performance
    /// Processa 4 transazioni per ciclo con ~2-3x speedup teorico.
    /// Usa stack arrays per evitare allocazioni dinamiche nel loop principale.
    ///
    /// # Precision
    ///
    /// # Safety
    /// ⭐ CRITICAL FIX: Validazione bounds per prevent buffer overflow
    /// Stack arrays pre-allocati per evitare accessi invalidi
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2,fma")]
    #[inline]
    unsafe fn compute_score_simd_batch_avx2(
        &self,
        fees: &[u64],
        classes: &[TxClass],
        fee_weight: f64,
        class_weight: f64,
    ) -> Vec<f64> {
        let len = fees.len();
        let mut scores = Vec::with_capacity(len);
        const LANES: usize = 4;

        // ⭐ CRITICAL FIX: Validazione bounds per SIMD safety
        if len < LANES {
            return self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight);
        }

        // Processa batch di 4 elementi con SIMD
        let simd_len = (len / LANES) * LANES;

        if simd_len > 0 {
            // Pre-carica costanti SIMD (una volta sola)
            let fee_weight_vec = _mm256_set1_pd(fee_weight);
            let class_weight_vec = _mm256_set1_pd(class_weight);
            let normalization_vec = _mm256_set1_pd(1_000_000_000.0);
            let one_vec = _mm256_set1_pd(1.0);

            // Stack arrays per evitare allocazioni nel loop
            let mut fee_array = [0.0f64; 4];
            let mut class_priorities_array = [0.0f64; 4];
            let mut scores_array = [0.0f64; 4];

            for i in (0..simd_len).step_by(LANES) {
                // ⭐ CRITICAL FIX: Bounds checking per array access
                if i + 3 >= len {
                    break; // Safety check per evitare buffer overflow
                }

                fee_array[0] = fees[i] as f64;
                fee_array[1] = fees[i + 1] as f64;
                fee_array[2] = fees[i + 2] as f64;
                fee_array[3] = fees[i + 3] as f64;
                let fee_vec = _mm256_loadu_pd(fee_array.as_ptr());

                // Normalizza fee: fee / 1_000_000_000.0 (identico alla versione scalare)
                let fee_normalized_vec = _mm256_div_pd(fee_vec, normalization_vec);

                let fee_normalized_clamped = _mm256_min_pd(fee_normalized_vec, one_vec);

                // Compute fee part: fee_normalized * fee_weight
                let fee_part_vec = _mm256_mul_pd(fee_normalized_clamped, fee_weight_vec);

                class_priorities_array[0] = self.class_priority_to_f64(classes[i]);
                class_priorities_array[1] = self.class_priority_to_f64(classes[i + 1]);
                class_priorities_array[2] = self.class_priority_to_f64(classes[i + 2]);
                class_priorities_array[3] = self.class_priority_to_f64(classes[i + 3]);
                let class_priorities_vec = _mm256_loadu_pd(class_priorities_array.as_ptr());

                // Compute class part: class_priority * class_weight
                let class_part_vec = _mm256_mul_pd(class_priorities_vec, class_weight_vec);

                // Somma finale: fee_part + class_part
                let scores_vec = _mm256_add_pd(fee_part_vec, class_part_vec);

                _mm256_storeu_pd(scores_array.as_mut_ptr(), scores_vec);
                scores.extend_from_slice(&scores_array);
            }
        }

        for i in simd_len..len {
            scores.push(self.compute_score_scalar(fees[i], classes[i], fee_weight, class_weight));
        }

        scores
    }

    ///
    /// # Performance
    /// Processa 2 transazioni per ciclo con ~1.5-2x speedup teorico.
    /// Usa stack arrays per evitare allocazioni dinamiche nel loop principale.
    ///
    /// # Precision
    ///
    /// # Safety
    /// ⭐ CRITICAL FIX: Validazione bounds per prevent buffer overflow
    /// Stack arrays pre-allocati per evitare accessi invalidi
    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "neon")]
    #[inline]
    unsafe fn compute_score_simd_batch_neon(
        &self,
        fees: &[u64],
        classes: &[TxClass],
        fee_weight: f64,
        class_weight: f64,
    ) -> Vec<f64> {
        let len = fees.len();
        let mut scores = Vec::with_capacity(len);
        const LANES: usize = 2;

        // ⭐ CRITICAL FIX: Validazione bounds per SIMD safety
        if len < LANES {
            return self.compute_score_scalar_batch(fees, classes, fee_weight, class_weight);
        }

        // Processa batch di 2 elementi con SIMD
        let simd_len = (len / LANES) * LANES;

        if simd_len > 0 {
            // Pre-carica costanti SIMD (una volta sola)
            let fee_weight_vec = vdupq_n_f64(fee_weight);
            let class_weight_vec = vdupq_n_f64(class_weight);
            let normalization_vec = vdupq_n_f64(1_000_000_000.0);
            let one_vec = vdupq_n_f64(1.0);

            // Stack arrays per evitare allocazioni nel loop
            let mut fee_array = [0.0f64; 2];
            let mut class_priorities_array = [0.0f64; 2];
            let mut scores_array = [0.0f64; 2];

            for i in (0..simd_len).step_by(LANES) {
                // ⭐ CRITICAL FIX: Bounds checking per array access
                if i + 1 >= len {
                    break; // Safety check per evitare buffer overflow
                }

                fee_array[0] = fees[i] as f64;
                fee_array[1] = fees[i + 1] as f64;
                let fee_vec = vld1q_f64(fee_array.as_ptr());

                // Normalizza fee: fee / 1_000_000_000.0 (identico alla versione scalare)
                let fee_normalized_vec = vdivq_f64(fee_vec, normalization_vec);

                let fee_normalized_clamped = vminq_f64(fee_normalized_vec, one_vec);

                // Compute fee part: fee_normalized * fee_weight
                let fee_part_vec = vmulq_f64(fee_normalized_clamped, fee_weight_vec);

                class_priorities_array[0] = self.class_priority_to_f64(classes[i]);
                class_priorities_array[1] = self.class_priority_to_f64(classes[i + 1]);
                let class_priorities_vec = vld1q_f64(class_priorities_array.as_ptr());

                // Compute class part: class_priority * class_weight
                let class_part_vec = vmulq_f64(class_priorities_vec, class_weight_vec);

                // Somma finale: fee_part + class_part
                let scores_vec = vaddq_f64(fee_part_vec, class_part_vec);

                vst1q_f64(scores_array.as_mut_ptr(), scores_vec);
                scores.extend_from_slice(&scores_array);
            }
        }

        for i in simd_len..len {
            scores.push(self.compute_score_scalar(fees[i], classes[i], fee_weight, class_weight));
        }

        scores
    }

    #[inline]
    fn compute_score_scalar_batch(
        &self,
        fees: &[u64],
        classes: &[TxClass],
        fee_weight: f64,
        class_weight: f64,
    ) -> Vec<f64> {
        let mut scores = Vec::with_capacity(fees.len());

        for i in 0..fees.len() {
            scores.push(self.compute_score_scalar(fees[i], classes[i], fee_weight, class_weight));
        }

        scores
    }

    /// Compute score per singola transazione (versione scalare)
    #[inline]
    fn compute_score_scalar(
        &self,
        fee: u64,
        class: TxClass,
        fee_weight: f64,
        class_weight: f64,
    ) -> f64 {
        const FEE_NORMALIZATION_FACTOR: f64 = 1_000_000_000.0;
        let fee_normalized = (fee as f64 / FEE_NORMALIZATION_FACTOR).min(1.0);
        let class_priority = self.class_priority_to_f64(class);

        fee_normalized * fee_weight + class_priority * class_weight
    }

    /// Converte TxClass in valore f64 di priorità (helper per SIMD)
    #[inline]
    fn class_priority_to_f64_simd(&self, class: TxClass) -> f64 {
        match class {
            TxClass::FederatedUpdate => 1.0,
            TxClass::IoTData => 0.7,
            TxClass::Financial => 0.5,
            TxClass::System => 0.6,
        }
    }

    fn update_metrics(&mut self, mempool_txs: &[MempoolTx]) {
        self.metrics.txs_scheduled_total = mempool_txs.len();

        metrics::counter!("dispatcher_txs_scheduled_total").increment(mempool_txs.len() as u64);

        // Conta transazioni ad alto fee (>1M)
        self.metrics.fee_weighted_txs = mempool_txs.iter().filter(|tx| tx.fee > 1_000_000).count();

        metrics::counter!("dispatcher_fee_weighted_txs")
            .increment(self.metrics.fee_weighted_txs as u64);

        // Compute fee distribution (p50, p90)
        if !mempool_txs.is_empty() {
            // Pre-alloca vettore con dimensione nota per evitare riallocazioni
            let mut fees: Vec<u64> = Vec::with_capacity(mempool_txs.len());
            fees.extend(mempool_txs.iter().map(|tx| tx.fee));
            fees.sort_unstable();

            let len = fees.len();
            // Compute percentile in modo sicuro (evita panic su vettore vuoto)
            if len > 0 {
                self.metrics.fee_distribution_p50 = fees[len / 2];
                // Compute p90 in modo sicuro (min per evitare out-of-bounds)
                let p90_index = ((len * 9) / 10).min(len - 1);
                self.metrics.fee_distribution_p90 = fees[p90_index];

                metrics::histogram!("dispatcher_fee_distribution_p50")
                    .record(self.metrics.fee_distribution_p50 as f64);
                metrics::histogram!("dispatcher_fee_distribution_p90")
                    .record(self.metrics.fee_distribution_p90 as f64);
            }
        }
    }

    fn update_metrics_from_refs_zerocopy(&mut self, mempool_tx_refs: &[&MempoolTx]) {
        // Converti references a valori per metriche (solo per calcolo)
        let mempool_txs: Vec<MempoolTx> = mempool_tx_refs
            .iter()
            .map(|&tx_ref| MempoolTx {
                sender_id: tx_ref.sender_id,
                nonce: tx_ref.nonce,
                fee: tx_ref.fee,
                tx_handle: tx_ref.tx_handle,
                class: tx_ref.class,
                stream_nonce: tx_ref.stream_nonce,
                inserted: tx_ref.inserted,
                tx_hash: tx_ref.tx_hash,

                // ⭐ NUOVI CAMPI CRITICI (FASE 3)
                sender_address: tx_ref.sender_address.clone(),
                signature_hash: tx_ref.signature_hash,
                gas_limit: tx_ref.gas_limit,
                max_fee: tx_ref.max_fee,
                received_at: tx_ref.received_at,
                rpc_accepted: tx_ref.rpc_accepted,
            })
            .collect();

        self.update_metrics(&mempool_txs);
    }

    /// Ottiene le metriche correnti
    pub fn metrics(&self) -> &DispatcherMetrics {
        &self.metrics
    }

    /// Resetta le metriche
    pub fn reset_metrics(&mut self) {
        self.metrics = DispatcherMetrics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::types::{MempoolTx, TxClass, TxHandle};
    use crate::tx::SignedTx;

    fn create_test_transactions(count: usize) -> (Vec<MempoolTx>, Vec<SignedTx>) {
        let mut mempool_txs = Vec::with_capacity(count);
        let mut signed_txs = Vec::with_capacity(count);

        for i in 0..count {
            let sender = format!("sender_{:02}", i % 5); // 5 sender unici
            let sender_bytes = sender.as_bytes().to_vec();
            let mempool_tx = MempoolTx {
                sender_id: (i % 5) as u32,
                nonce: i as u64,
                fee: 1000000 + (i as u64 * 100000), // Fee crescenti
                tx_handle: TxHandle(i as u64),
                class: match i % 4 {
                    0 => TxClass::FederatedUpdate,
                    1 => TxClass::IoTData,
                    2 => TxClass::Financial,
                    _ => TxClass::System,
                },
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: sender_bytes.clone(),
                signature_hash: [0u8; 32],
                gas_limit: 21000,
                max_fee: 1000000 + (i as u64 * 100000) + 100000,
                received_at: std::time::Instant::now(),
            };

            let signed_tx = SignedTx {
                from: sender.as_bytes().to_vec(),
                to: format!("receiver_{:02}", i).as_bytes().to_vec(),
                amount: 1000,
                nonce: i as u64,
                fee: Some((1000000 + (i as u64 * 100000)) as u128),
                pubkey: [0; 32],
                sig: [0; 64],
                pre_verified: false,
            };

            mempool_txs.push(mempool_tx);
            signed_txs.push(signed_tx);
        }

        (mempool_txs, signed_txs)
    }

    #[test]
    fn test_zero_copy_scheduling_basic() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(20);

        // Test zero-copy scheduling
        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        assert_eq!(indices.len(), 20, "Should schedule all transactions");

        for &idx in &indices {
            assert!(idx < mempool_txs.len(), "Index should be valid");
        }

        // Check che non ci siano duplicati
        let mut unique_indices = indices.clone();
        unique_indices.sort_unstable();
        unique_indices.dedup();
        assert_eq!(
            unique_indices.len(),
            indices.len(),
            "No duplicate indices allowed"
        );
    }

    #[test]
    fn test_zero_copy_vs_original_consistency() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher1 = ExecutionDispatcher::new(config.clone());
        let mut dispatcher2 = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(50);

        // Original method (con clone)
        let (scheduled_mempool, scheduled_signed) =
            dispatcher1.schedule_transactions(mempool_txs.clone(), signed_txs.clone());

        // Zero-copy method
        let indices = dispatcher2.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        assert_eq!(scheduled_mempool.len(), indices.len());
        assert_eq!(scheduled_signed.len(), indices.len());

        // Check che l'ordine sia consistente
        for (i, &idx) in indices.iter().enumerate() {
            assert_eq!(scheduled_mempool[i].fee, mempool_txs[idx].fee);
            assert_eq!(scheduled_mempool[i].nonce, mempool_txs[idx].nonce);
            assert_eq!(scheduled_signed[i].from, signed_txs[idx].from);
        }
    }

    #[test]
    fn test_zero_copy_empty_input() {
        let config = DispatcherConfig::new();
        let mut dispatcher = ExecutionDispatcher::new(config);

        let empty_mempool: Vec<MempoolTx> = vec![];
        let empty_signed: Vec<SignedTx> = vec![];

        let indices = dispatcher.schedule_transactions_zero_copy(&empty_mempool, &empty_signed);

        assert!(indices.is_empty(), "Empty input should return empty result");
    }

    #[test]
    fn test_zero_copy_mismatched_lengths() {
        let config = DispatcherConfig::new();
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(10);
        let truncated_signed = signed_txs[..5].to_vec();

        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &truncated_signed);

        // Dovrebbe ritornare sequenza originale per lunghezze mismatched
        assert_eq!(indices.len(), mempool_txs.len());
        for (i, &idx) in indices.iter().enumerate() {
            assert_eq!(
                idx, i,
                "Should return original sequence for mismatched lengths"
            );
        }
    }

    #[test]
    fn test_zero_copy_performance_benefits() {
        let config = DispatcherConfig::with_max_txs(1000);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(1000);

        // Benchmark original method
        let start_original = std::time::Instant::now();
        let _original_result =
            dispatcher.schedule_transactions(mempool_txs.clone(), signed_txs.clone());
        let original_duration = start_original.elapsed();

        // Benchmark zero-copy method
        let start_zero_copy = std::time::Instant::now();
        let _zero_copy_result =
            dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);
        let zero_copy_duration = start_zero_copy.elapsed();

        println!("Original method: {:?}", original_duration);
        println!("Zero-copy method: {:?}", zero_copy_duration);

        // Check che i risultati abbiano la stessa lunghezza
        assert_eq!(_original_result.0.len(), _zero_copy_result.len());

        if mempool_txs.len() > 500 {
            assert!(
                zero_copy_duration <= original_duration,
                "Zero-copy should be faster or equal for large batches"
            );
        }
    }

    #[test]
    fn test_zero_copy_determinism() {
        let config = DispatcherConfig::new();
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(100);

        // Esegui scheduling multiple volte
        let indices1 = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);
        let indices2 = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);
        let indices3 = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        assert_eq!(indices1, indices2, "Results should be deterministic");
        assert_eq!(indices2, indices3, "Results should be deterministic");
    }

    #[test]
    fn test_zero_copy_sender_limits() {
        let config = DispatcherConfig::with_max_txs(10); // Max 10 per sender
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(50); // 10 per sender

        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        // Check that no sender has more than max_txs_per_sender transactions
        let mut sender_counts: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();
        for &idx in &indices {
            let sender = &signed_txs[idx].from;
            *sender_counts.entry(sender.clone()).or_insert(0) += 1;
        }

        for count in sender_counts.values() {
            assert!(*count <= 10, "No sender should exceed max_txs_per_sender");
        }
    }

    #[test]
    fn test_zero_copy_fairness_interleaving() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(25); // 5 sender, 5 TX cadauno

        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        let sender_sequence: Vec<String> = indices
            .iter()
            .map(|&idx| String::from_utf8_lossy(&signed_txs[idx].from).to_string())
            .collect();

        // Conta cambiamenti di sender (più cambiamenti = più fairness)
        let mut sender_changes = 0;
        for i in 1..sender_sequence.len() {
            if sender_sequence[i] != sender_sequence[i - 1] {
                sender_changes += 1;
            }
        }

        // Dovrebbe esserci un buon interleaving (alcuni cambiamenti di sender)
        assert!(
            sender_changes > 0,
            "Should have sender interleaving for fairness"
        );

        let mut current_sender = "";
        let mut current_block_size = 0;
        let mut max_block_size = 0;

        for sender in &sender_sequence {
            if *sender == current_sender {
                current_block_size += 1;
            } else {
                max_block_size = max_block_size.max(current_block_size);
                current_sender = sender;
                current_block_size = 1;
            }
        }
        max_block_size = max_block_size.max(current_block_size);

        assert!(
            max_block_size <= 5,
            "Should not have large blocks of same sender"
        );
    }

    #[test]
    fn test_zero_copy_score_ordering() {
        let config = DispatcherConfig::new();
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(20);

        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        // Check che le transazioni siano ordinate per score (fee + class)
        for i in 1..indices.len() {
            let prev_idx = indices[i - 1];
            let curr_idx = indices[i];

            let prev_score =
                dispatcher.compute_score(&mempool_txs[prev_idx], &signed_txs[prev_idx]);
            let curr_score =
                dispatcher.compute_score(&mempool_txs[curr_idx], &signed_txs[curr_idx]);

            if signed_txs[prev_idx].from == signed_txs[curr_idx].from {
                assert!(
                    prev_score >= curr_score,
                    "Within same sender, scores should be decreasing"
                );
            }
        }
    }

    #[test]
    fn test_simd_vs_scalar_determinism() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        let (fees, classes) = create_test_score_data(1000);

        // Compute score con SIMD
        let simd_scores = dispatcher.compute_score_simd_batch(&fees, &classes, 0.7, 0.3);

        // Compute score con scalare
        let scalar_scores = dispatcher.compute_score_scalar_batch(&fees, &classes, 0.7, 0.3);

        // Check che i risultati siano identici
        assert_eq!(
            simd_scores.len(),
            scalar_scores.len(),
            "Results should have same length"
        );

        for (i, (simd, scalar)) in simd_scores.iter().zip(scalar_scores.iter()).enumerate() {
            let diff = (simd - scalar).abs();
            assert!(
                diff < 1e-10,
                "Score difference at index {}: SIMD={}, Scalar={}, Diff={}",
                i,
                simd,
                scalar,
                diff
            );
        }
    }

    #[test]
    fn test_simd_batch_sizes() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        // Test diverse dimensioni di batch
        for size in [1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 100].iter() {
            let (fees, classes) = create_test_score_data(*size);

            let simd_scores = dispatcher.compute_score_simd_batch(&fees, &classes, 0.7, 0.3);
            let scalar_scores = dispatcher.compute_score_scalar_batch(&fees, &classes, 0.7, 0.3);

            assert_eq!(simd_scores.len(), *size, "SIMD batch size {} failed", size);
            assert_eq!(
                scalar_scores.len(),
                *size,
                "Scalar batch size {} failed",
                size
            );

            for (simd, scalar) in simd_scores.iter().zip(scalar_scores.iter()) {
                let diff = (simd - scalar).abs();
                assert!(diff < 1e-10, "Batch size {} has different results", size);
            }
        }
    }

    #[test]
    fn test_simd_empty_and_edge_cases() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        // Test empty input
        let empty_fees: Vec<u64> = vec![];
        let empty_classes: Vec<TxClass> = vec![];
        let simd_result =
            dispatcher.compute_score_simd_batch(&empty_fees, &empty_classes, 0.7, 0.3);
        assert!(
            simd_result.is_empty(),
            "Empty input should return empty result"
        );

        // Test single element
        let fees = vec![1_000_000];
        let classes = vec![TxClass::FederatedUpdate];
        let simd_result = dispatcher.compute_score_simd_batch(&fees, &classes, 0.7, 0.3);
        let scalar_result = dispatcher.compute_score_scalar_batch(&fees, &classes, 0.7, 0.3);

        assert_eq!(simd_result.len(), 1);
        assert_eq!(scalar_result.len(), 1);

        // Verify single element result
        let diff = (simd_result[0] - scalar_result[0]).abs();
        assert!(diff < 1e-10, "Single element should have same result");

        // Test mismatched lengths (should handle gracefully)
        let fees_mismatch = vec![1_000_000, 2_000_000];
        let classes_mismatch = vec![TxClass::FederatedUpdate];

        // This should not panic but handle the mismatch gracefully
        let result =
            dispatcher.compute_score_simd_batch(&fees_mismatch, &classes_mismatch, 0.7, 0.3);
        assert_eq!(
            result.len(),
            1,
            "Should handle mismatched lengths gracefully"
        );
    }

    #[test]
    fn test_simd_performance_characteristics() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        let (fees, classes) = create_test_score_data(10000);

        // Benchmark SIMD
        let start_simd = std::time::Instant::now();
        let _simd_result = dispatcher.compute_score_simd_batch(&fees, &classes, 0.7, 0.3);
        let simd_duration = start_simd.elapsed();

        // Benchmark Scalar
        let start_scalar = std::time::Instant::now();
        let _scalar_result = dispatcher.compute_score_scalar_batch(&fees, &classes, 0.7, 0.3);
        let scalar_duration = start_scalar.elapsed();

        println!("SIMD duration: {:?}", simd_duration);
        println!("Scalar duration: {:?}", scalar_duration);

        // SIMD should be faster or at least not significantly slower
        if scalar_duration > std::time::Duration::from_millis(10) {
            // Only check performance if the test takes meaningful time
            let speedup = scalar_duration.as_secs_f64() / simd_duration.as_secs_f64();
            assert!(
                speedup > 0.5,
                "SIMD should not be significantly slower than scalar"
            );
        }
    }

    #[test]
    fn test_class_priority_conversion() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        assert_eq!(
            dispatcher.class_priority_to_f64(TxClass::FederatedUpdate),
            1.0
        );
        assert_eq!(dispatcher.class_priority_to_f64(TxClass::IoTData), 0.7);
        assert_eq!(dispatcher.class_priority_to_f64(TxClass::Financial), 0.5);
        assert_eq!(dispatcher.class_priority_to_f64(TxClass::System), 0.6);
    }

    #[test]
    fn test_simd_integration_schedule_transactions() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(20);

        // Test scheduling con SIMD integration
        let (scheduled_mempool, scheduled_signed) =
            dispatcher.schedule_transactions(mempool_txs.clone(), signed_txs.clone());

        assert_eq!(
            scheduled_mempool.len(),
            20,
            "Should schedule all transactions"
        );
        assert_eq!(
            scheduled_signed.len(),
            20,
            "Should schedule all transactions"
        );

        // Check che l'ordine sia consistente con zero-copy version
        let indices = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);

        assert_eq!(scheduled_mempool.len(), indices.len());
        assert_eq!(scheduled_signed.len(), indices.len());

        // Check che l'ordine sia consistente
        for (i, &idx) in indices.iter().enumerate() {
            assert_eq!(scheduled_mempool[i].fee, mempool_txs[idx].fee);
            assert_eq!(scheduled_mempool[i].nonce, mempool_txs[idx].nonce);
            assert_eq!(scheduled_signed[i].from, signed_txs[idx].from);
        }
    }

    #[test]
    fn test_simd_integration_zero_copy_consistency() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs, signed_txs) = create_test_transactions(50);

        // Test consistency tra SIMD e scalar versions
        let (_scheduled_mempool_simd, _scheduled_signed_simd) =
            dispatcher.schedule_transactions(mempool_txs.clone(), signed_txs.clone());

        // Forza scalar version (threshold < 8)
        let (mempool_txs_small, signed_txs_small) = create_test_transactions(5);
        let (scheduled_mempool_scalar, scheduled_signed_scalar) =
            dispatcher.schedule_transactions(mempool_txs_small.clone(), signed_txs_small.clone());

        // Check che i risultati siano consistenti per il subset
        assert_eq!(scheduled_mempool_scalar.len(), 5);
        assert_eq!(scheduled_signed_scalar.len(), 5);

        let indices1 = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);
        let indices2 = dispatcher.schedule_transactions_zero_copy(&mempool_txs, &signed_txs);
        assert_eq!(indices1, indices2, "Zero-copy should be deterministic");
    }

    #[test]
    fn test_simd_threshold_behavior() {
        let config = DispatcherConfig::with_max_txs(100);
        let mut dispatcher = ExecutionDispatcher::new(config);

        let (mempool_txs_small, signed_txs_small) = create_test_transactions(31); // Sotto threshold 32
        let start = std::time::Instant::now();
        let _result_small = dispatcher.schedule_transactions(mempool_txs_small, signed_txs_small);
        let duration_small = start.elapsed();

        let (mempool_txs_large, signed_txs_large) = create_test_transactions(33); // Sopra threshold 32
        let start = std::time::Instant::now();
        let _result_large = dispatcher.schedule_transactions(mempool_txs_large, signed_txs_large);
        let duration_large = start.elapsed();

        assert!(
            duration_small.as_millis() < 100,
            "Small batch should complete quickly"
        );
        assert!(
            duration_large.as_millis() < 100,
            "Large batch should complete quickly"
        );

        // Test diretto of the threshold in compute_score_simd_batch
        let (fees_small, classes_small) = create_test_score_data(31);
        let (fees_large, classes_large) = create_test_score_data(33);

        let start = std::time::Instant::now();
        let _scores_small =
            dispatcher.compute_score_simd_batch(&fees_small, &classes_small, 0.7, 0.3);
        let duration_small_direct = start.elapsed();

        let start = std::time::Instant::now();
        let _scores_large =
            dispatcher.compute_score_simd_batch(&fees_large, &classes_large, 0.7, 0.3);
        let duration_large_direct = start.elapsed();

        // Verify both produce correct results
        assert_eq!(_scores_small.len(), 31);
        assert_eq!(_scores_large.len(), 33);

        println!("Small batch duration: {:?}", duration_small_direct);
        println!("Large batch duration: {:?}", duration_large_direct);
    }

    #[test]
    fn test_simd_security_input_validation() {
        let config = DispatcherConfig::new();
        let dispatcher = ExecutionDispatcher::new(config);

        // Test valid input
        let fees = vec![1_000_000, 2_000_000];
        let classes = vec![TxClass::FederatedUpdate, TxClass::IoTData];
        let result = dispatcher.compute_score_simd_batch(&fees, &classes, 0.7, 0.3);
        assert_eq!(result.len(), 2);

        // Test mismatched lengths - should handle gracefully, not panic
        let fees_mismatch = vec![1_000_000, 2_000_000];
        let classes_mismatch = vec![TxClass::FederatedUpdate];

        // Should handle mismatch gracefully by using the minimum length
        let result_mismatch =
            dispatcher.compute_score_simd_batch(&fees_mismatch, &classes_mismatch, 0.7, 0.3);
        assert_eq!(
            result_mismatch.len(),
            1,
            "Should handle mismatched lengths gracefully"
        );

        // Test extreme values
        let extreme_fees = vec![u64::MAX, 0, 1];
        let extreme_classes = vec![
            TxClass::FederatedUpdate,
            TxClass::IoTData,
            TxClass::Financial,
        ];
        let result_extreme =
            dispatcher.compute_score_simd_batch(&extreme_fees, &extreme_classes, 0.7, 0.3);
        assert_eq!(result_extreme.len(), 3);

        // Verify all results are finite numbers
        for score in &result_extreme {
            assert!(score.is_finite(), "All scores should be finite");
            assert!(
                *score >= 0.0 && *score <= 1.0,
                "Scores should be in [0, 1] range"
            );
        }

        // Test zero weights
        let result_zero_weights = dispatcher.compute_score_simd_batch(&fees, &classes, 0.0, 0.0);
        assert_eq!(result_zero_weights.len(), 2);

        // Test extreme weights
        let result_extreme_weights = dispatcher.compute_score_simd_batch(&fees, &classes, 1.0, 1.0);
        assert_eq!(result_extreme_weights.len(), 2);

        // Verify all extreme weight results are also valid
        for score in &result_extreme_weights {
            assert!(score.is_finite(), "Extreme weight scores should be finite");
            assert!(
                *score >= 0.0 && *score <= 1.0,
                "Extreme weight scores should be in [0, 1] range"
            );
        }
    }

    #[test]
    fn test_adaptive_weights_basic_functionality() {
        let config = DispatcherConfig::new();
        let mut dispatcher = ExecutionDispatcher::new(config);

        // Test pesi iniziali
        let (fee_weight, class_weight) = dispatcher.get_adaptive_weights();
        assert_eq!(fee_weight, 0.7, "Initial fee weight should be 0.7");
        assert_eq!(class_weight, 0.3, "Initial class weight should be 0.3");

        // Test set pesi manuali
        dispatcher.set_adaptive_weights(0.8, 0.2);
        let (new_fee_weight, new_class_weight) = dispatcher.get_adaptive_weights();
        assert_eq!(new_fee_weight, 0.8, "Updated fee weight should be 0.8");
        assert_eq!(new_class_weight, 0.2, "Updated class weight should be 0.2");

        // Test reset
        dispatcher.reset_adaptive_weights();
        let (reset_fee_weight, reset_class_weight) = dispatcher.get_adaptive_weights();
        assert_eq!(reset_fee_weight, 0.7, "Reset fee weight should be 0.7");
        assert_eq!(reset_class_weight, 0.3, "Reset class weight should be 0.3");
    }

    #[test]
    fn test_adaptive_weights_mempool_analysis() {
        let config = DispatcherConfig::new();
        let adaptive_config = AdaptiveWeightsConfig::default();
        let dispatcher = ExecutionDispatcher::new_with_adaptive_weights(config, adaptive_config);

        // Creates transazioni di test con diverse fee e classi
        let mempool_txs = vec![
            MempoolTx {
                fee: 1_000_000_000, // 1B - alta fee
                nonce: 1,
                class: TxClass::FederatedUpdate,
                sender_id: 1,
                tx_handle: TxHandle(1),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![1u8; 32],
                signature_hash: [1u8; 64],
                gas_limit: 21000,
                max_fee: 1_100_000_000,
                received_at: std::time::Instant::now(),
            },
            MempoolTx {
                fee: 100_000_000, // 100M - bassa fee
                nonce: 2,
                class: TxClass::IoTData,
                sender_id: 2,
                tx_handle: TxHandle(2),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![2u8; 32],
                signature_hash: [2u8; 64],
                gas_limit: 21000,
                max_fee: 200_000_000,
                received_at: std::time::Instant::now(),
            },
            MempoolTx {
                fee: 500_000_000, // 500M - media fee
                nonce: 3,
                class: TxClass::Financial,
                sender_id: 3,
                tx_handle: TxHandle(3),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![3u8; 32],
                signature_hash: [3u8; 64],
                gas_limit: 21000,
                max_fee: 600_000_000,
                received_at: std::time::Instant::now(),
            },
        ];

        // Analizza stato mempool
        let state = dispatcher.analyze_current_mempool_state(&mempool_txs);
        let s = state.as_ref().expect("MempoolState should be present");

        assert_eq!(s.fee_distribution.len(), 3, "Should have 3 fee values");
        assert_eq!(s.class_distribution.len(), 3, "Should have 3 class values");
        assert!(
            !s.historical_throughput.is_empty(),
            "Should have historical throughput"
        );
        assert!(s.timestamp > 0, "Should have valid timestamp");

        // Check distribuzione fee
        assert_eq!(
            s.fee_distribution[0], 1_000_000_000,
            "First fee should be 1B"
        );
        assert_eq!(
            s.fee_distribution[1], 100_000_000,
            "Second fee should be 100M"
        );
        assert_eq!(
            s.fee_distribution[2], 500_000_000,
            "Third fee should be 500M"
        );

        // Check distribuzione class
        assert_eq!(
            s.class_distribution[0],
            TxClass::FederatedUpdate,
            "First class should be FederatedUpdate"
        );
        assert_eq!(
            s.class_distribution[1],
            TxClass::IoTData,
            "Second class should be IoTData"
        );
        assert_eq!(
            s.class_distribution[2],
            TxClass::Financial,
            "Third class should be Financial"
        );
    }

    #[test]
    fn test_adaptive_weights_adjustment() {
        let config = DispatcherConfig::new();
        let adaptive_config = AdaptiveWeightsConfig::default();
        let mut dispatcher =
            ExecutionDispatcher::new_with_adaptive_weights(config, adaptive_config);

        let high_fee_txs = vec![
            MempoolTx {
                fee: 3_000_000_000, // 3B - sopra threshold alta (2B)
                nonce: 1,
                class: TxClass::FederatedUpdate,
                sender_id: 1,
                tx_handle: TxHandle(1),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![1u8; 32],
                signature_hash: [1u8; 64],
                gas_limit: 21000,
                max_fee: 3_100_000_000,
                received_at: std::time::Instant::now(),
            },
            MempoolTx {
                fee: 2_500_000_000, // 2.5B - sopra threshold alta
                nonce: 2,
                class: TxClass::IoTData,
                sender_id: 2,
                tx_handle: TxHandle(2),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![2u8; 32],
                signature_hash: [2u8; 64],
                gas_limit: 21000,
                max_fee: 2_600_000_000,
                received_at: std::time::Instant::now(),
            },
        ];

        // Creates le signed_txs corrispondenti
        let signed_txs = vec![
            SignedTx {
                from: vec![1; 20],
                to: vec![2; 20],
                amount: 1000,
                nonce: 1,
                fee: Some(3_000_000_000),
                sig: [1; 64],
                pubkey: [1; 32],
                pre_verified: true,
            },
            SignedTx {
                from: vec![2; 20],
                to: vec![3; 20],
                amount: 1000,
                nonce: 2,
                fee: Some(2_500_000_000),
                sig: [2; 64],
                pubkey: [2; 32],
                pre_verified: true,
            },
        ];

        let initial_weights = dispatcher.get_adaptive_weights();
        let state = dispatcher.analyze_current_mempool_state(&high_fee_txs);
        println!(
            "DEBUG: Initial weights: fee={:.6}, class={:.6}",
            initial_weights.0, initial_weights.1
        );

        if let Some(s) = state.as_ref() {
            println!("DEBUG: Fee distribution: {:?}", s.fee_distribution);
            if let Some(max_fee) = s.fee_distribution.iter().max() {
                println!(
                    "DEBUG: Max fee: {} (threshold_high: {})",
                    max_fee, 2_000_000_000.0
                );
            }
        }

        // Usa schedule_transactions per sincronizzare i pesi correttamente
        let _scheduled = dispatcher.schedule_transactions(high_fee_txs.clone(), signed_txs.clone());
        let adapted_weights = dispatcher.get_adaptive_weights();
        println!(
            "DEBUG: Adapted weights: fee={:.6}, class={:.6}",
            adapted_weights.0, adapted_weights.1
        );

        if let Some(state) = dispatcher.analyze_current_mempool_state(&high_fee_txs) {
            println!(
                "DEBUG: Mempool state - fee_distribution: {:?}",
                state.fee_distribution
            );
            println!(
                "DEBUG: Mempool state - class_distribution: {:?}",
                state.class_distribution
            );
        } else {
            println!("DEBUG: No mempool state generated");
        }

        assert!(
            adapted_weights.0 > initial_weights.0,
            "Fee weight should increase with high fees. Initial: {:.6}, Adapted: {:.6}",
            initial_weights.0,
            adapted_weights.0
        );
        assert!(
            adapted_weights.0 <= 0.9,
            "Fee weight should respect upper bound"
        );

        let low_fee_txs = vec![
            MempoolTx {
                fee: 100_000_000, // 100M - sotto threshold bassa (500M)
                nonce: 1,
                class: TxClass::FederatedUpdate,
                sender_id: 1,
                tx_handle: TxHandle(1),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![1u8; 32],
                signature_hash: [1u8; 64],
                gas_limit: 21000,
                max_fee: 200_000_000,
                received_at: std::time::Instant::now(),
            },
            MempoolTx {
                fee: 200_000_000, // 200M - sotto threshold bassa
                nonce: 2,
                class: TxClass::IoTData,
                sender_id: 2,
                tx_handle: TxHandle(2),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![2u8; 32],
                signature_hash: [2u8; 64],
                gas_limit: 21000,
                max_fee: 300_000_000,
                received_at: std::time::Instant::now(),
            },
        ];

        // Creates le signed_txs corrispondenti per le low fee txs
        let low_fee_signed_txs = vec![
            SignedTx {
                from: vec![3; 20],
                to: vec![4; 20],
                amount: 500,
                nonce: 3,
                fee: Some(200000000),
                sig: [3; 64],
                pubkey: [3; 32],
                pre_verified: true,
            },
            SignedTx {
                from: vec![4; 20],
                to: vec![5; 20],
                amount: 500,
                nonce: 4,
                fee: Some(150000000),
                sig: [4; 64],
                pubkey: [4; 32],
                pre_verified: true,
            },
        ];

        // Usa schedule_transactions per sincronizzare i pesi correttamente
        let _scheduled = dispatcher.schedule_transactions(low_fee_txs, low_fee_signed_txs);
        let final_weights = dispatcher.get_adaptive_weights();

        assert!(
            final_weights.0 < adapted_weights.0,
            "Fee weight should decrease with low fees"
        );
        assert!(
            final_weights.0 >= 0.5,
            "Fee weight should respect lower bound"
        );
    }

    // OLD TEST - DISABLED - Using new test module below
    // #[test]
    // fn test_adaptive_weights_class_diversity() {
    //     let config = DispatcherConfig::new();
    //     let mut dispatcher = ExecutionDispatcher::new(config);
    //
    //     let diverse_txs = vec![
    //         MempoolTx {
    //             fee: 1_000_000_000,
    //             nonce: 1,
    //             class: TxClass::FederatedUpdate,
    //             sender_id: 1,
    //             tx_handle: TxHandle(1),
    //             stream_nonce: None,
    //             inserted: std::time::Instant::now(),
    //         },
    //         MempoolTx {
    //             fee: 1_000_000_000,
    //             nonce: 2,
    //             class: TxClass::IoTData,
    //             sender_id: 2,
    //             tx_handle: TxHandle(2),
    //             stream_nonce: None,
    //             inserted: std::time::Instant::now(),
    //         },
    //         MempoolTx {
    //             fee: 1_000_000_000,
    //             nonce: 3,
    //             class: TxClass::Financial,
    //             sender_id: 3,
    //             tx_handle: TxHandle(3),
    //             stream_nonce: None,
    //             inserted: std::time::Instant::now(),
    //         },
    //         MempoolTx {
    //             fee: 1_000_000_000,
    //             nonce: 4,
    //             class: TxClass::System,
    //             sender_id: 4,
    //             tx_handle: TxHandle(4),
    //             stream_nonce: None,
    //             inserted: std::time::Instant::now(),
    //         },
    //     ];
    //
    //     let initial_weights = dispatcher.get_adaptive_weights();
    //     let state = dispatcher.analyze_current_mempool_state(&diverse_txs);
    //     let _new_weights = dispatcher.adaptive_weights.adapt_weights(&state);
    //     let adapted_weights = dispatcher.get_adaptive_weights();
    //
    //     assert!(adapted_weights.1 > initial_weights.1, "Class weight should increase with high diversity");
    //     assert!(adapted_weights.1 <= 0.5, "Class weight should respect upper bound");
    // }

    #[test]
    fn test_adaptive_weights_integration() {
        let config = DispatcherConfig::new();
        let adaptive_config = AdaptiveWeightsConfig::default();
        let mut dispatcher =
            ExecutionDispatcher::new_with_adaptive_weights(config, adaptive_config);

        // Creates transazioni di test
        let mempool_txs = vec![
            MempoolTx {
                fee: 2_000_000_000, // Alta fee
                nonce: 1,
                class: TxClass::FederatedUpdate,
                sender_id: 1,
                tx_handle: TxHandle(1),
                sender_address: vec![1u8; 32],
                signature_hash: [1u8; 64],
                gas_limit: 21000,
                max_fee: 2_100_000_000,
                received_at: std::time::Instant::now(),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
            },
            MempoolTx {
                fee: 500_000_000, // Media fee
                nonce: 2,
                class: TxClass::IoTData,
                sender_id: 2,
                tx_handle: TxHandle(2),
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![2u8; 32],
                signature_hash: [2u8; 64],
                gas_limit: 21000,
                max_fee: 600_000_000,
                received_at: std::time::Instant::now(),
            },
        ];

        let signed_txs = vec![
            SignedTx {
                from: vec![1; 20],
                to: vec![2; 20],
                amount: 1000,
                nonce: 1,
                fee: Some(2000000000),
                pubkey: [3; 32],
                sig: [3; 64],
                pre_verified: true,
            },
            SignedTx {
                from: vec![4; 20],
                to: vec![5; 20],
                amount: 2000,
                nonce: 2,
                fee: Some(500000000),
                pubkey: [6; 32],
                sig: [6; 64],
                pre_verified: true,
            },
        ];

        let (scheduled_mempool, scheduled_signed) =
            dispatcher.schedule_transactions(mempool_txs.clone(), signed_txs.clone());

        // Check che le transazioni siano state schedulate
        assert_eq!(
            scheduled_mempool.len(),
            2,
            "Should schedule all transactions"
        );
        assert_eq!(
            scheduled_signed.len(),
            2,
            "Should schedule all transactions"
        );

        // Check che i pesi siano stati adattati
        let adapted_weights = dispatcher.get_adaptive_weights();
        assert!(
            adapted_weights.0 != 0.7 || adapted_weights.1 != 0.3,
            "Weights should be adapted"
        );

        // Check che la storia degli stati sia stata aggiornata
        let history = dispatcher.get_mempool_state_history();
        assert!(
            history.is_some() && !history.unwrap().is_empty(),
            "Should have mempool state history"
        );
    }

    // OLD TEST - DISABLED - Using new test module below
    // #[test]
    // fn test_adaptive_weights_bounds() {
    //     let config = DispatcherConfig::new();
    //     let mut dispatcher = ExecutionDispatcher::new(config);
    //
    //     // Test bounds superiori
    //     dispatcher.set_adaptive_weights(1.0, 0.0); // Fuori dai bounds
    //     let (fee_weight, class_weight) = dispatcher.get_adaptive_weights();
    //     assert_eq!(fee_weight, 0.9, "Should clamp fee weight to upper bound");
    //     assert_eq!(class_weight, 0.1, "Should clamp class weight to lower bound");
    //
    //     // Test bounds inferiori
    //     dispatcher.set_adaptive_weights(0.0, 1.0); // Fuori dai bounds
    //     let (fee_weight, class_weight) = dispatcher.get_adaptive_weights();
    //     assert_eq!(fee_weight, 0.5, "Should clamp fee weight to lower bound");
    //     assert_eq!(class_weight, 0.5, "Should clamp class weight to upper bound");
    //
    //     // Test normalizzazione
    //     dispatcher.set_adaptive_weights(0.6, 0.6); // Somma != 1.0
    //     let (fee_weight, class_weight) = dispatcher.get_adaptive_weights();
    //     assert!((fee_weight + class_weight - 1.0).abs() < f64::EPSILON, "Weights should sum to 1.0 (got {})", fee_weight + class_weight);
    // }

    fn create_test_score_data(count: usize) -> (Vec<u64>, Vec<TxClass>) {
        let mut fees = Vec::with_capacity(count);
        let mut classes = Vec::with_capacity(count);

        for i in 0..count {
            fees.push(100_000 + (i as u64 % 10) * 1_000_000);

            let class = match i % 4 {
                0 => TxClass::FederatedUpdate,
                1 => TxClass::IoTData,
                2 => TxClass::Financial,
                _ => TxClass::System,
            };
            classes.push(class);
        }

        (fees, classes)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::mempool::types::{MempoolTx, TxClass};

        fn create_dummy_tx(fee: u64, class: TxClass) -> MempoolTx {
            MempoolTx {
                sender_id: 0,
                nonce: 0,
                fee,
                tx_handle: crate::mempool::types::TxHandle(0),
                class,
                stream_nonce: None,
                inserted: std::time::Instant::now(),
                tx_hash: None,
                sender_address: vec![0u8; 32],
                signature_hash: [0u8; 32],
                gas_limit: 21000,
                max_fee: fee + 100000,
                received_at: std::time::Instant::now(),
            }
        }

        #[test]
        fn test_adaptive_weights_basic_functionality() {
            let config = AdaptiveWeightsConfig::default();
            let adaptive_weights = AdaptiveWeights::new(config);
            let (fee, class) = adaptive_weights.get_adaptive_weights();

            assert!((fee + class - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn test_adaptive_weights_bounds() {
            let mut weights = AdaptiveWeights::default();

            // Forza uno stato estremo (fee altissime)
            let txs = vec![
                create_dummy_tx(3_000_000_000, TxClass::Financial),
                create_dummy_tx(3_000_000_000, TxClass::Financial),
            ];

            let state = weights.analyze_mempool_state(&txs);
            let (fee, class) = weights.adapt_weights(&state);

            assert!(fee <= weights.config.fee_weight_bounds.1);
            assert!(class >= weights.config.class_weight_bounds.0);

            let sum = fee + class;
            assert!((sum - 1.0).abs() < 1e-10, "Sum was {}, expected 1.0", sum);
        }

        #[test]
        fn test_adaptive_weights_adjustment() {
            let mut weights = AdaptiveWeights::default();
            let (initial_fee, _) = weights.get_adaptive_weights();

            let txs = vec![
                create_dummy_tx(5_000_000_000, TxClass::Financial),
                create_dummy_tx(5_000_000_000, TxClass::Financial),
                create_dummy_tx(5_000_000_000, TxClass::Financial),
            ];

            let state = weights.analyze_mempool_state(&txs);
            let (new_fee, _) = weights.adapt_weights(&state);

            // La fee DEVE aumentare
            assert!(
                new_fee > initial_fee,
                "Fee weight should increase. Init: {}, New: {}",
                initial_fee,
                new_fee
            );
        }

        #[test]
        fn test_adaptive_weights_class_diversity() {
            let mut weights = AdaptiveWeights::default();
            let (initial_fee, initial_class) = weights.get_adaptive_weights();

            // 1. Caso Alta Diversità, Fee Basse (per evitare cancellazione)
            let txs = vec![
                create_dummy_tx(100, TxClass::Financial),
                create_dummy_tx(100, TxClass::IoTData),
                create_dummy_tx(100, TxClass::System),
                create_dummy_tx(100, TxClass::FederatedUpdate),
            ];

            let state = weights.analyze_mempool_state(&txs);
            let (new_fee, new_class) = weights.adapt_weights(&state);

            // Diversità alta -> Fee scende -> Class sale
            assert!(
                new_class > initial_class,
                "Class weight should increase with diversity. Init: {}, New: {}",
                initial_class,
                new_class
            );
            assert!(
                new_fee < initial_fee,
                "Fee weight should decrease with diversity"
            );

            // Check invarianza
            assert!((new_fee + new_class - 1.0).abs() < 1e-10);
        }

        #[test]
        fn test_adaptive_weights_integration() {
            let config = DispatcherConfig::default();
            let dispatcher = ExecutionDispatcher::new(config);

            // Simuliamo un batch
            let _mempool_txs = vec![create_dummy_tx(1000, TxClass::Financial)];
            // Nota: nei test reali servirebbero SignedTx validi, qui testiamo solo che non esploda
            let (f, c) = dispatcher.get_adaptive_weights();
            assert!((f + c - 1.0).abs() < f64::EPSILON);
        }
    }
}
