//! Parallel Execution: Esecuzione parallela di contratti
//!
//! - Dependency detection (storage overlap, call overlap)
//! - Dependency graph construction
//! - Topological sort
//! - Parallel execution con rayon
//! - Memory monitoring

use crate::contracts::memory_monitor::MemoryMonitor;
use rayon::prelude::*;
use savitri_core::core::tx::{CallTransaction, SignedTx};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Traccia gli accessi di una transazione per dependency detection
///
/// e sulle chiamate effettuate da una transazione.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionAccess {
    pub called_contracts: BTreeSet<Vec<u8>>,
    /// Contratti il cui storage è stato letto (contract_address -> set di slot letti)
    pub storage_reads: BTreeMap<Vec<u8>, BTreeSet<u64>>,
    /// Contratti il cui storage è stato scritto (contract_address -> set di slot scritti)
    pub storage_writes: BTreeMap<Vec<u8>, BTreeSet<u64>>,
    pub caller: Vec<u8>,
}

impl TransactionAccess {
    pub fn new(caller: Vec<u8>) -> Self {
        Self {
            called_contracts: BTreeSet::new(),
            storage_reads: BTreeMap::new(),
            storage_writes: BTreeMap::new(),
            caller,
        }
    }

    pub fn add_called_contract(&mut self, contract_address: Vec<u8>) {
        self.called_contracts.insert(contract_address);
    }

    /// Adds una lettura storage per a contract
    pub fn add_storage_read(&mut self, contract_address: Vec<u8>, slot: u64) {
        self.storage_reads
            .entry(contract_address)
            .or_insert_with(BTreeSet::new)
            .insert(slot);
    }

    /// Adds una scrittura storage per a contract
    pub fn add_storage_write(&mut self, contract_address: Vec<u8>, slot: u64) {
        self.storage_writes
            .entry(contract_address)
            .or_insert_with(BTreeSet::new)
            .insert(slot);
    }

    ///
    /// - Una scrive e l'altra legge/scrive lo stesso slot
    pub fn has_storage_overlap(&self, other: &TransactionAccess) -> bool {
        // Check overlap tra scritture
        for (contract, slots) in &self.storage_writes {
            if let Some(other_slots) = other.storage_writes.get(contract) {
                if !slots.is_disjoint(other_slots) {
                    return true;
                }
            }
            // Scrittura vs lettura: se scrivo e l'altro legge, c'è overlap
            if let Some(other_read_slots) = other.storage_reads.get(contract) {
                if !slots.is_disjoint(other_read_slots) {
                    return true;
                }
            }
        }

        // Check overlap tra letture e scritture (l'altra scrive, io leggo)
        for (contract, slots) in &self.storage_reads {
            if let Some(other_write_slots) = other.storage_writes.get(contract) {
                if !slots.is_disjoint(other_write_slots) {
                    return true;
                }
            }
        }

        false
    }

    ///
    pub fn has_call_overlap(&self, other: &TransactionAccess) -> bool {
        // Check se chiamano gli stessi contratti
        if !self.called_contracts.is_disjoint(&other.called_contracts) {
            return true;
        }

        for called_contract in &self.called_contracts {
            if other.storage_writes.contains_key(called_contract) {
                return true;
            }
        }

        for called_contract in &other.called_contracts {
            if self.storage_writes.contains_key(called_contract) {
                return true;
            }
        }

        false
    }
}

/// Trait per estrarre informazioni di accesso da una transazione
///
/// di analizzare le transazioni e rilevare dipendenze.
pub trait TransactionAccessExtractor {
    /// Estrae gli accessi di una transazione
    ///
    /// che descrive gli accessi allo storage e le chiamate effettuate.
    ///
    /// # Note
    /// - Per un'analisi più precisa, richiede esecuzione dry-run o analisi bytecode
    fn extract_access(&self) -> TransactionAccess;
}

/// Cache key per dependency graph
#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyCacheKey {
    access_hash: u64,
    num_txs: usize,
}

impl Hash for DependencyCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_hash.hash(state);
        self.num_txs.hash(state);
    }
}

/// Cache entry per dependency graph
#[derive(Debug, Clone)]
struct DependencyCacheEntry {
    /// Dependency graph cached
    #[allow(dead_code)]
    graph: Vec<Vec<usize>>,
    /// Dipendenze rilevate
    #[allow(dead_code)]
    dependencies: Vec<(usize, usize)>,
    /// Timestamp di creazione (per TTL)
    created_at: std::time::Instant,
}

/// Cache per dependency graph con TTL
struct DependencyGraphCache {
    /// Cache entries (key -> entry)
    entries: HashMap<DependencyCacheKey, DependencyCacheEntry>,
    /// TTL per le entries (default: 5 minuti)
    ttl: std::time::Duration,
    /// Max entries in the cache (default: 1000)
    max_entries: usize,
}

impl DependencyGraphCache {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            ttl: std::time::Duration::from_secs(5 * 60), // 5 minuti
            max_entries: 1000,
        }
    }

    /// Pulisce entries scadute
    fn cleanup_expired(&mut self) {
        let now = std::time::Instant::now();
        self.entries
            .retain(|_, entry| now.duration_since(entry.created_at) < self.ttl);
    }

    fn evict_if_full(&mut self) {
        if self.entries.len() >= self.max_entries {
            // Rimuovi le entry più vecchie (FIFO semplice)
            let mut oldest_key: Option<DependencyCacheKey> = None;
            let mut oldest_time = std::time::Instant::now();

            for (key, entry) in &self.entries {
                if entry.created_at < oldest_time {
                    oldest_time = entry.created_at;
                    oldest_key = Some(key.clone());
                }
            }

            if let Some(key) = oldest_key {
                self.entries.remove(&key);
            }
        }
    }
}

/// Rileva dipendenze tra transazioni
///
/// Analizza storage overlap e call overlap per determinare
///
/// Supporta cache per ottimizzare rilevamento dipendenze per batch simili.
pub struct DependencyDetector {
    cache: Option<Arc<Mutex<DependencyGraphCache>>>,
    /// Abilita/disabilita cache
    cache_enabled: bool,
}

impl DependencyDetector {
    pub fn new() -> Self {
        Self {
            cache: None,
            cache_enabled: false,
        }
    }

    pub fn with_cache() -> Self {
        Self {
            cache: Some(Arc::new(Mutex::new(DependencyGraphCache::new()))),
            cache_enabled: true,
        }
    }

    /// Compute hash degli accessi per una cache key
    fn compute_access_hash<T>(transactions: &[T]) -> u64
    where
        T: TransactionAccessExtractor,
    {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();

        // Hash combinato degli accessi
        for tx in transactions {
            let access = tx.extract_access();
            // Hash caller
            access.caller.hash(&mut hasher);
            // Hash contratti chiamati
            for contract in &access.called_contracts {
                contract.hash(&mut hasher);
            }
            // Hash storage writes (solo contract, non slot per performance)
            for contract in access.storage_writes.keys() {
                contract.hash(&mut hasher);
            }
            // Hash storage reads (solo contract)
            for contract in access.storage_reads.keys() {
                contract.hash(&mut hasher);
            }
        }

        hasher.finish()
    }

    ///
    ///
    /// # Arguments
    /// * `transactions` - Slice di transazioni che implementano TransactionAccessExtractor
    ///
    /// # Returns
    /// List di dipendenze (from_index, to_index) dove from_index < to_index
    ///
    /// # Algorithm
    /// 1. Costruisce indici hash-based per restringere le comparazioni
    /// 2. Check overlap solo per candidati che condividono risorse
    /// 3. Complessità: O(n + d) dove d è il numero di dipendenze (d << n²)
    ///
    /// # Note
    /// Per backward compatibility, mantiene la stessa interfaccia ma usa
    pub fn detect_dependencies<T>(&self, transactions: &[T]) -> Vec<(usize, usize)>
    where
        T: TransactionAccessExtractor,
    {
        self.detect_dependencies_optimized(transactions)
    }

    ///
    /// Costruisce indici di accesso O(n) e check solo le transazioni che
    /// condividono risorse, riducendo la complessità a circa O(n + d) dove
    /// d è il numero di coppie effettivamente dipendenti (d << n²).
    ///
    /// # Arguments
    /// * `transactions` - Slice di transazioni che implementano TransactionAccessExtractor
    ///
    /// # Returns
    /// List di dipendenze (from_index, to_index) dove from_index < to_index
    ///
    /// # Complexity
    /// O(n + d) dove n è il numero di transazioni e d è il numero di dipendenze.
    /// La costruzione degli indici è O(n), la ricerca candidati è O(n * avg_candidates),
    /// e la check overlap è O(d). Con d << n², la complessità è approssimativamente O(n).
    ///
    /// # Ottimizzazioni
    /// - Pre-alloca HashMap con capacity hints per ridurre reallocazioni
    ///
    /// # Note
    /// Mantiene detect_dependencies() per backward compatibility.
    pub fn detect_dependencies_optimized<T>(&self, transactions: &[T]) -> Vec<(usize, usize)>
    where
        T: TransactionAccessExtractor,
    {
        let num_txs = transactions.len();
        if num_txs <= 1 {
            return Vec::new();
        }

        let accesses: Vec<TransactionAccess> =
            transactions.iter().map(|tx| tx.extract_access()).collect();

        // Pre-alloca indici con capacity hints per ridurre reallocazioni
        // Stima: ~num_txs/8 contratti unici, ~num_txs slot unici
        let estimated_contracts = (num_txs / 8).max(16);
        let estimated_slots = num_txs.max(16);

        // Indici hash-based per restringere le comparazioni
        // Usa capacity hints per ridurre reallocazioni durante l'inserimento
        let mut call_index: HashMap<Vec<u8>, Vec<usize>> =
            HashMap::with_capacity(estimated_contracts);
        let mut write_index: HashMap<(Vec<u8>, u64), Vec<usize>> =
            HashMap::with_capacity(estimated_slots);
        let mut read_index: HashMap<(Vec<u8>, u64), Vec<usize>> =
            HashMap::with_capacity(estimated_slots);
        let mut writes_by_contract: HashMap<Vec<u8>, Vec<usize>> =
            HashMap::with_capacity(estimated_contracts);

        // Costruisci indici (O(n) dove n è numero di transazioni)
        for (idx, access) in accesses.iter().enumerate() {
            for contract in &access.called_contracts {
                call_index
                    .entry(contract.clone())
                    .or_insert_with(|| Vec::with_capacity(8))
                    .push(idx);
            }

            for (contract, slots) in &access.storage_writes {
                writes_by_contract
                    .entry(contract.clone())
                    .or_insert_with(|| Vec::with_capacity(8))
                    .push(idx);
                for &slot in slots {
                    write_index
                        .entry((contract.clone(), slot))
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(idx);
                }
            }

            for (contract, slots) in &access.storage_reads {
                for &slot in slots {
                    read_index
                        .entry((contract.clone(), slot))
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(idx);
                }
            }
        }

        // Pre-alloca dependencies con stima conservativa
        // Stima: ~num_txs/4 dipendenze (caso moderato overlap)
        let mut dependencies = Vec::with_capacity(num_txs / 4);

        // Complessità: O(n * avg_candidates) dove avg_candidates << n
        for i in 0..num_txs {
            let access_i = &accesses[i];
            let mut candidates = HashSet::with_capacity(16); // Stima iniziale per candidati

            // Candidati che chiamano gli stessi contratti o scrivono su contratti chiamati
            for contract in &access_i.called_contracts {
                if let Some(list) = call_index.get(contract) {
                    candidates.extend(list.iter().copied());
                }
                if let Some(list) = writes_by_contract.get(contract) {
                    candidates.extend(list.iter().copied());
                }
            }

            // Candidati che condividono slot di scrittura/lettura
            for (contract, slots) in &access_i.storage_writes {
                for &slot in slots {
                    // Creates tupla per lookup (clone necessario per chiave HashMap)
                    let key = (contract.clone(), slot);
                    if let Some(list) = write_index.get(&key) {
                        candidates.extend(list.iter().copied());
                    }
                    if let Some(list) = read_index.get(&key) {
                        candidates.extend(list.iter().copied());
                    }
                }
            }

            for (contract, slots) in &access_i.storage_reads {
                for &slot in slots {
                    let key = (contract.clone(), slot);
                    if let Some(list) = write_index.get(&key) {
                        candidates.extend(list.iter().copied());
                    }
                }
            }

            let mut candidate_vec: Vec<usize> = candidates.into_iter().filter(|&j| j > i).collect();
            candidate_vec.sort_unstable(); // Determinismo: ordina per garantire ordine consistente

            // Check overlap solo per candidati validi (j > i)
            // Complessità: O(d_i) dove d_i è numero di candidati per transazione i
            for j in candidate_vec {
                let access_j = &accesses[j];
                if access_i.has_storage_overlap(access_j) || access_i.has_call_overlap(access_j) {
                    dependencies.push((i, j));
                }
            }
        }

        // Salva in cache se abilitata
        if self.cache_enabled {
            if let Some(cache) = &self.cache {
                let access_hash = Self::compute_access_hash(transactions);
                let cache_key = DependencyCacheKey {
                    access_hash,
                    num_txs,
                };

                if let Ok(mut cache_guard) = cache.lock() {
                    cache_guard.cleanup_expired();
                    cache_guard.evict_if_full();

                    let graph = self.build_graph(dependencies.clone(), num_txs);
                    let entry = DependencyCacheEntry {
                        graph,
                        dependencies: dependencies.clone(),
                        created_at: std::time::Instant::now(),
                    };

                    cache_guard.entries.insert(cache_key, entry);
                }
            }
        }

        dependencies
    }

    /// Costruisce il dependency graph
    ///
    /// Costruisce un grafo diretto dove:
    /// - Gli archi rappresentano dipendenze (from -> to)
    ///
    /// # Arguments
    /// * `dependencies` - List di dipendenze (from_index, to_index)
    ///
    /// # Returns
    /// Grafo rappresentato come list di adiacenza: graph[i] contiene gli indici
    ///
    /// # Complexity
    /// O(d) dove d è il numero di dipendenze
    ///
    /// # Note
    /// Per grafi molto grandi, si potrebbe considerare una rappresentazione più compatta.
    pub fn build_graph(
        &self,
        dependencies: Vec<(usize, usize)>,
        num_nodes: usize,
    ) -> Vec<Vec<usize>> {
        // Inizializza grafo con liste di adiacenza vuote
        let mut graph = vec![Vec::new(); num_nodes];

        for (from, to) in dependencies {
            if from >= num_nodes || to >= num_nodes {
                continue; // Skip dipendenze invalide
            }

            // Aggiungi arco from -> to
            // Usa un HashSet per evitare duplicati (se build_graph viene chiamato più volte)
            if !graph[from].contains(&to) {
                graph[from].push(to);
            }
        }

        for adj_list in &mut graph {
            adj_list.sort_unstable();
        }

        graph
    }

    /// Runs topological sort of the dependency graph
    ///
    /// Se la transazione i dipende dalla transazione j, allora j apparirà prima di i
    /// nell'ordine risultante.
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Vettore di indici ordinati topologicamente. Se il grafo contiene cicli,
    ///
    /// # Algorithm
    /// Usa l'algoritmo di Kahn (BFS-based):
    /// 2. Push nodes with in-degree 0 onto the queue
    /// 4. Push new nodes with in-degree 0 onto the queue
    ///
    /// # Complexity
    /// O(V + E) dove V è il numero di nodi e E è il numero di archi
    ///
    /// # Determinismo
    /// L'ordine è deterministico: i nodi are processati in ordine crescente
    pub fn topological_sort(&self, graph: &[Vec<usize>]) -> Result<Vec<usize>, String> {
        let num_nodes = graph.len();

        // in_degree[i] = numero di archi che puntano al nodo i
        let mut in_degree = vec![0; num_nodes];
        for adj_list in graph {
            for &dependent in adj_list {
                if dependent < num_nodes {
                    in_degree[dependent] += 1;
                }
            }
        }

        let mut queue = VecDeque::new();
        for (node, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                queue.push_back(node);
            }
        }

        let mut sorted_queue: Vec<usize> = queue.iter().copied().collect();
        sorted_queue.sort_unstable();
        queue.clear();
        for node in sorted_queue {
            queue.push_back(node);
        }

        let mut result = Vec::new();
        let mut processed = 0;

        while let Some(node) = queue.pop_front() {
            result.push(node);
            processed += 1;

            // Decrementa in-degree dei nodi dipendenti
            for &dependent in &graph[node] {
                if dependent < num_nodes {
                    in_degree[dependent] -= 1;
                    // If the dependent node now has in-degree 0, push it onto the queue
                    if in_degree[dependent] == 0 {
                        queue.push_back(dependent);
                    }
                }
            }

            // (riordina quando aggiungiamo nuovi nodi)
            if !queue.is_empty() {
                let mut sorted: Vec<usize> = queue.iter().copied().collect();
                sorted.sort_unstable();
                queue.clear();
                for node in sorted {
                    queue.push_back(node);
                }
            }
        }

        // Se non lo sono, c'è un ciclo nel grafo
        if processed != num_nodes {
            return Err(format!(
                "Topological sort failed: cycle detected. Processed {}/{} nodes",
                processed, num_nodes
            ));
        }

        Ok(result)
    }

    /// Runs transazioni in parallelo rispettando le dipendenze (con memory monitoring)
    ///
    /// Runs le transazioni in modo che:
    /// - Le dipendenze siano rispettate (una transazione viene eseguita solo dopo
    /// - Le transazioni indipendenti (allo stesso livello) siano eseguite in parallelo
    /// - L'ordine di esecuzione sia deterministico
    /// - I memory bounds siano rispettati per prevent DoS
    ///
    /// # Arguments
    /// * `transactions` - Vettore di transazioni da eseguire
    /// * `memory_monitor` - Memory monitor per bounds checking
    ///
    /// # Returns
    /// Ogni risultato indica se l'esecuzione è riuscita o ha fallito.
    pub fn execute_parallel_with_memory<T, F>(
        &self,
        transactions: Vec<T>,
        graph: &[Vec<usize>],
        execute_fn: F,
        memory_monitor: &MemoryMonitor,
    ) -> Result<Vec<Result<(), String>>, String>
    where
        T: Send + Sync + Clone,
        F: Fn(T) -> Result<(), String> + Sync + Send,
    {
        let num_txs = transactions.len();

        // Check memory bounds per batch size
        memory_monitor
            .check_batch_size(num_txs)
            .map_err(|e| format!("Memory limit exceeded: {}", e))?;

        // Check che il grafo abbia la dimensione corretta
        if graph.len() != num_txs {
            return Err(format!(
                "Graph size mismatch: graph has {} nodes, but {} transactions provided",
                graph.len(),
                num_txs
            ));
        }

        let _sorted_indices = self.topological_sort(graph)?;

        // Costruisce grafo inverso per trovare i livelli
        let mut reverse_graph = vec![Vec::new(); num_txs];
        for (from, adj_list) in graph.iter().enumerate() {
            for &to in adj_list {
                if to < num_txs {
                    reverse_graph[to].push(from);
                }
            }
        }

        let mut levels = Vec::new();
        let mut in_degree = vec![0; num_txs];
        for adj_list in graph {
            for &dependent in adj_list {
                if dependent < num_txs {
                    in_degree[dependent] += 1;
                }
            }
        }

        let mut current_level = Vec::new();
        for (idx, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                current_level.push(idx);
            }
        }
        current_level.sort_unstable(); // Determinismo

        while !current_level.is_empty() {
            levels.push(current_level.clone());
            let mut next_level = Vec::new();

            for &node in &current_level {
                for &dependent in &graph[node] {
                    if dependent < num_txs {
                        in_degree[dependent] -= 1;
                        if in_degree[dependent] == 0 {
                            next_level.push(dependent);
                        }
                    }
                }
            }

            next_level.sort_unstable(); // Determinismo
            current_level = next_level;
        }

        // Inizializza risultati (None = non ancora eseguita)
        let mut results: Vec<Option<Result<(), String>>> = vec![None; num_txs];

        // Runs livelli sequenzialmente, transazioni in the stesso livello in parallelo
        for (level_idx, level) in levels.iter().enumerate() {
            let level_memory = level.len() * 1024; // Stima 1KB per transazione
            if let Err(e) = memory_monitor.check_tx_memory(level_memory as u64) {
                log::warn!("Memory limit exceeded for level {}: {}", level_idx, e);
            }

            let level_results: Vec<(usize, Result<(), String>)> = level
                .par_iter()
                .map(|&idx| {
                    let tx = transactions[idx].clone();
                    let result = execute_fn(tx);
                    (idx, result)
                })
                .collect();

            // Salva risultati nell'ordine originale
            for (idx, result) in level_results {
                results[idx] = Some(result);
            }
        }

        // Converti risultati in formato finale
        let final_results: Vec<Result<(), String>> = results
            .into_iter()
            .enumerate()
            .map(|(idx, opt_result)| {
                opt_result.unwrap_or_else(|| Err(format!("Transaction {} was not executed", idx)))
            })
            .collect();

        Ok(final_results)
    }
}

impl Default for DependencyDetector {
    fn default() -> Self {
        Self::new()
    }
}

///
///   o analizzare il bytecode per determinare gli slot specifici
impl TransactionAccessExtractor for CallTransaction {
    fn extract_access(&self) -> TransactionAccess {
        let mut access = TransactionAccess::new(self.from.clone());

        // Nota: contract_address è Vec<u8>, lo aggiungiamo direttamente
        if self.contract_address.len() == 32 {
            access.add_called_contract(self.contract_address.clone());
        }

        // Per un'analisi statica conservativa, non possiamo determinare
        //
        // Per storage overlap più preciso, si potrebbe:
        // 2. Analizzare il bytecode staticamente (se disponibile)
        // 3. Usare informazioni di tipo dal calldata (se disponibili)

        access
    }
}

/// Helper per verificare se una SignedTx è una batch transaction IoT
///
/// - Header: numero di entries (u32, little-endian) nei primi 4 bytes
fn is_iot_batch_transaction(tx: &SignedTx) -> bool {
    if tx.to.len() < 4 {
        return false;
    }

    // Leggi header: numero di entries (u32, little-endian)
    let num_entries = u32::from_le_bytes([tx.to[0], tx.to[1], tx.to[2], tx.to[3]]);

    // Check che il numero di entries sia ragionevole (1-100)
    // e che la dimensione totale corrisponda al formato batch
    if num_entries == 0 || num_entries > 100 {
        return false;
    }

    let mut offset = 4usize;
    for _ in 0..num_entries {
        // Deve esserci spazio per length (u32)
        if offset + 4 > tx.to.len() {
            return false;
        }

        let entry_len = u32::from_le_bytes([
            tx.to[offset],
            tx.to[offset + 1],
            tx.to[offset + 2],
            tx.to[offset + 3],
        ]) as usize;

        offset += 4;

        // Deve esserci spazio per i dati dell'entry
        if offset + entry_len > tx.to.len() {
            return false;
        }

        offset += entry_len;
    }

    offset == tx.to.len()
}

///
/// - Transazioni normali: estrae informazioni conservative dagli accessi
/// - CallTransaction embedded: se SignedTx contiene dati di CallTransaction
impl TransactionAccessExtractor for SignedTx {
    fn extract_access(&self) -> TransactionAccess {
        let mut access = TransactionAccess::new(self.from.clone());

        // Check se è una batch transaction IoT
        if is_iot_batch_transaction(self) {
            // Non ha storage overlap con altri batch (ogni device ha il suo batch)
            return access;
        }

        // Transazione normale: check se è una CallTransaction
        // Per ora, assumiamo che SignedTx normale possa chiamare contratti
        if self.to.len() == 32 {
            // Aggiungilo come called contract (conservativo)
            access.add_called_contract(self.to.clone());
        }

        // Per un'analisi statica conservativa, non possiamo determinare
        //
        // Per storage overlap più preciso, si potrebbe:
        // 2. Analizzare il bytecode staticamente (se disponibile)
        // 3. Usare informazioni di tipo dal calldata (se disponibili)

        access
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MockTx {
        access: TransactionAccess,
    }

    impl TransactionAccessExtractor for MockTx {
        fn extract_access(&self) -> TransactionAccess {
            self.access.clone()
        }
    }

    fn addr(byte: u8) -> Vec<u8> {
        vec![byte; 32]
    }

    #[test]
    fn optimized_matches_original() {
        let mut access0 = TransactionAccess::new(vec![]);
        access0.add_called_contract(addr(1));

        let mut access1 = TransactionAccess::new(vec![]);
        access1.add_called_contract(addr(1));

        // tx2 scrive, tx3 legge lo stesso slot -> dipendenza storage overlap
        let mut access2 = TransactionAccess::new(vec![]);
        access2.add_storage_write(addr(2), 10);

        let mut access3 = TransactionAccess::new(vec![]);
        access3.add_storage_read(addr(2), 10);

        // tx4 e tx5 scrivono on the stesso slot -> dipendenza storage overlap
        let mut access4 = TransactionAccess::new(vec![]);
        access4.add_storage_write(addr(3), 7);

        let mut access5 = TransactionAccess::new(vec![]);
        access5.add_storage_write(addr(3), 7);

        let txs = vec![
            MockTx { access: access0 },
            MockTx { access: access1 },
            MockTx { access: access2 },
            MockTx { access: access3 },
            MockTx { access: access4 },
            MockTx { access: access5 },
        ];

        let detector = DependencyDetector::new();
        let mut baseline = detector.detect_dependencies(&txs);
        let mut optimized = detector.detect_dependencies_optimized(&txs);

        baseline.sort_unstable();
        optimized.sort_unstable();

        assert_eq!(baseline, optimized);
    }
}

/// Metriche per monitoring DAG execution
///
#[derive(Debug, Clone, Default)]
pub struct DAGMetrics {
    /// Numero totale di esecuzioni parallele
    pub total_executions: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    /// Tempo totale di dependency detection (microsecondi)
    pub total_dependency_detection_time_us: u64,
    /// Tempo totale di esecuzione (microsecondi)
    pub total_execution_time_us: u64,
    /// Numero totale di transazioni processate
    pub total_transactions: u64,
    /// Numero totale di dipendenze rilevate
    pub total_dependencies: u64,
}

impl DAGMetrics {
    pub fn new() -> Self {
        Self {
            total_executions: 0,
            cache_hits: 0,
            cache_misses: 0,
            total_dependency_detection_time_us: 0,
            total_execution_time_us: 0,
            total_transactions: 0,
            total_dependencies: 0,
        }
    }

    /// Compute cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let total_cache_ops = self.cache_hits + self.cache_misses;
        if total_cache_ops == 0 {
            return 0.0;
        }
        self.cache_hits as f64 / total_cache_ops as f64
    }

    pub fn avg_dependency_detection_time_us(&self) -> f64 {
        if self.total_executions == 0 {
            return 0.0;
        }
        self.total_dependency_detection_time_us as f64 / self.total_executions as f64
    }

    pub fn avg_execution_time_us(&self) -> f64 {
        if self.total_transactions == 0 {
            return 0.0;
        }
        self.total_execution_time_us as f64 / self.total_transactions as f64
    }
}
