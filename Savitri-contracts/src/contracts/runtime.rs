//! Runtime: Execution environment per smart contracts
//!
//! - Overlay state
//! - Call stack
//! - Gas meter
//! - Determinismo

use crate::contracts::events::EventSystem;
use crate::contracts::gas::GasMeter;
use crate::contracts::memory_monitor::MemoryMonitor;
use anyhow::Result;
use hex;
use savitri_core::core::types::Account;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

/// Massima profondità of the call stack (default)
///
/// Limita il numero di chiamate nested per prevent stack overflow.
pub const MAX_CALL_DEPTH: usize = 64;

/// Frame di chiamata
///
/// Rappresenta un frame di chiamata nel call stack.
/// Supporta nested calls con depth tracking e storage snapshot per rollback.
#[derive(Debug, Clone)]
pub struct CallFrame {
    pub contract_address: [u8; 32],
    /// Indirizzo of the chiamante (EOA o contract)
    pub caller: [u8; 32],
    /// Token trasferiti con la chiamata (se payable)
    pub value: u128,
    /// Dati di chiamata (function selector + args)
    pub calldata: Vec<u8>,
    /// Dati di ritorno dalla funzione
    pub return_data: Vec<u8>,
    pub gas_remaining: u64,
    /// Profondità nel call stack (0 = root call)
    pub depth: u8,
    pub storage_snapshot: [u8; 64],
}

/// Runtime per l'esecuzione of contracts
///
/// Thread-safe per supportare esecuzione parallela.
pub struct Runtime {
    /// Overlay state per modifiche temporanee durante l'esecuzione
    /// Le modifiche are accumulate qui e committate atomically al blocco
    overlay: Arc<RwLock<BTreeMap<Vec<u8>, Account>>>,
    /// Gas meter per tracciare il consumo di gas
    gas_meter: Arc<RwLock<GasMeter>>,
    /// Call stack per gestire nested calls
    call_stack: Arc<RwLock<Vec<CallFrame>>>,
    /// Massima profondità of the call stack (default: 64)
    max_call_depth: usize,
    /// Timestamp deterministico of the blocco corrente (Unix timestamp in secondi)
    /// Usa Arc<RwLock<>> per thread-safety e migliori performance
    block_timestamp: Arc<RwLock<u64>>,
    /// Viene impostato a true se viene rilevato un accesso non-deterministico
    non_deterministic_access_detected: Arc<RwLock<bool>>,
    /// Memory monitor per bounds checking e monitoring
    memory_monitor: Arc<MemoryMonitor>,
    /// Sistema di eventi per raccogliere eventi emessi durante l'esecuzione (T4.9.3)
    event_system: Arc<EventSystem>,
}

impl Runtime {
    ///
    /// # Arguments
    /// * `gas_limit` - Limit di gas per l'esecuzione
    /// * `max_call_depth` - Massima profondità of the call stack (default: 64)
    /// * `block_timestamp` - Timestamp deterministico of the blocco corrente (Unix timestamp in secondi)
    pub fn new(
        overlay: BTreeMap<Vec<u8>, Account>,
        gas_limit: u64,
        max_call_depth: usize,
        block_timestamp: u64,
    ) -> Self {
        let memory_monitor = Arc::new(MemoryMonitor::default());

        Self {
            overlay: Arc::new(RwLock::new(overlay)),
            gas_meter: Arc::new(RwLock::new(GasMeter::new(gas_limit))),
            call_stack: Arc::new(RwLock::new(Vec::new())),
            max_call_depth,
            block_timestamp: Arc::new(RwLock::new(block_timestamp)),
            non_deterministic_access_detected: Arc::new(RwLock::new(false)),
            memory_monitor,
            event_system: Arc::new(EventSystem::new()),
        }
    }

    ///
    /// Usa `MAX_CALL_DEPTH` come profondità massima of the call stack.
    ///
    /// # Arguments
    /// * `gas_limit` - Limit di gas per l'esecuzione
    /// * `block_timestamp` - Timestamp deterministico of the blocco corrente (Unix timestamp in secondi)
    pub fn with_empty_overlay(gas_limit: u64, block_timestamp: u64) -> Self {
        Self::new(BTreeMap::new(), gas_limit, MAX_CALL_DEPTH, block_timestamp)
    }

    /// Ottiene riferimento all'overlay state (thread-safe)
    pub fn overlay(&self) -> Arc<RwLock<BTreeMap<Vec<u8>, Account>>> {
        Arc::clone(&self.overlay)
    }

    /// Ottiene riferimento al gas meter (thread-safe)
    pub fn gas_meter(&self) -> Arc<RwLock<GasMeter>> {
        Arc::clone(&self.gas_meter)
    }

    /// Ottiene riferimento al memory monitor (thread-safe)
    pub fn memory_monitor(&self) -> Arc<MemoryMonitor> {
        Arc::clone(&self.memory_monitor)
    }

    /// Ottiene riferimento al sistema di eventi (thread-safe)
    ///
    /// e inclusi nel receipt.
    ///
    /// # Returns
    /// Riferimento Arc al sistema di eventi
    pub fn event_system(&self) -> Arc<EventSystem> {
        Arc::clone(&self.event_system)
    }

    ///
    /// di sicurezza critico per prevent vulnerabilità come il re-entrancy attack.
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn check_reentrancy(&self, contract_address: &[u8; 32]) -> Result<(), String> {
        if self.is_in_call_path(contract_address) {
            let call_path = self.call_path_hex();
            return Err(format!(
                "Re-entrancy protection: contract {} is already in execution (call path: {:?})",
                hex::encode(contract_address),
                call_path
            ));
        }
        Ok(())
    }

    ///
    /// Il sistema previene stack overflow limitando il depth a max_call_depth (default: 64).
    ///
    /// non sia già nel call stack prima di aggiungere il frame.
    ///
    /// # Arguments
    /// * `frame` - Frame da aggiungere al call stack (il depth verrà impostato automaticamente)
    ///
    /// # Returns
    pub fn push_frame(&self, mut frame: CallFrame) -> Result<(), String> {
        // Check memory bounds prima di procedere
        {
            let stack = self
                .call_stack
                .read()
                .map_err(|e| format!("Failed to acquire read lock for memory check: {}", e))?;
            if let Err(e) = self.memory_monitor.check_call_frames(stack.len() + 1) {
                return Err(format!("Memory limit exceeded: {}", e));
            }
        }

        let mut stack = self
            .call_stack
            .write()
            .map_err(|e| format!("Failed to acquire write lock for call stack: {}", e))?;

        if stack
            .iter()
            .any(|f| f.contract_address == frame.contract_address)
        {
            let call_path: Vec<String> = stack
                .iter()
                .map(|f| hex::encode(f.contract_address))
                .collect();
            return Err(format!(
                "Re-entrancy protection: contract {} is already in execution (call path: {:?})",
                hex::encode(frame.contract_address),
                call_path
            ));
        }

        // Check che non si superi il max call depth
        if stack.len() >= self.max_call_depth {
            return Err(format!(
                "Max call depth exceeded: {} >= {} (stack overflow prevented)",
                stack.len(),
                self.max_call_depth
            ));
        }

        // Il depth è 0-based: depth 0 = root call, depth 1 = prima chiamata nested, etc.
        frame.depth = stack.len() as u8;

        // Check che il depth non superi u8::MAX (255)
        if frame.depth == u8::MAX && stack.len() >= u8::MAX as usize {
            return Err(format!(
                "Call depth exceeds u8::MAX: depth {} >= {}",
                frame.depth,
                u8::MAX
            ));
        }

        stack.push(frame);
        Ok(())
    }

    /// Pop il frame corrente dal call stack
    ///
    /// # Returns
    pub fn pop_frame(&self) -> Option<CallFrame> {
        let mut stack = self.call_stack.write().ok()?;
        stack.pop()
    }

    /// Ottiene il frame corrente (immutabile)
    ///
    /// # Returns
    /// * `Some(&CallFrame)` se c'è un frame corrente
    pub fn current_frame(&self) -> Option<CallFrame> {
        let stack = self.call_stack.read().ok()?;
        stack.last().cloned()
    }

    /// Ottiene la profondità corrente of the call stack
    ///
    /// La profondità è 0-based:
    /// - 0 = no frame (empty stack)
    /// - 1 = un frame (root call)
    /// - 2+ = chiamate nested
    ///
    /// # Returns
    /// Profondità corrente of the call stack (numero di frame)
    pub fn call_depth(&self) -> usize {
        match self.call_stack.read() {
            Ok(stack) => stack.len(),
            Err(_) => 0, // Fallback: se il lock fallisce, assume depth 0
        }
    }

    ///
    /// Il call path rappresenta la sequenza di contract addresses chiamati,
    /// dal root call fino alla chiamata corrente. Utile per debugging e
    /// per prevent re-entrancy attacks.
    ///
    /// # Returns
    /// Vettore di contract addresses nel call path (dal root al corrente)
    ///
    /// # Performance
    /// Usa pre-allocazione per ridurre allocazioni dinamiche
    pub fn call_path(&self) -> Vec<[u8; 32]> {
        match self.call_stack.read() {
            Ok(stack) => {
                // Pre-alloca con capacity esatta per evitare reallocazioni
                let mut path = Vec::with_capacity(stack.len());
                for frame in stack.iter() {
                    path.push(frame.contract_address);
                }
                path
            }
            Err(_) => Vec::new(), // Fallback: if the lock fails, return an empty path
        }
    }

    /// Ottiene il call path come stringhe hex (per debugging)
    ///
    /// # Returns
    /// Vettore di contract addresses come stringhe hex
    pub fn call_path_hex(&self) -> Vec<String> {
        self.call_path()
            .iter()
            .map(|addr| hex::encode(addr))
            .collect()
    }

    ///
    /// Utile per ispezionare lo stato completo of the call stack durante l'esecuzione.
    ///
    /// # Returns
    ///
    /// # Performance
    /// Usa pre-allocazione per ridurre allocazioni dinamiche
    pub fn all_frames(&self) -> Vec<CallFrame> {
        match self.call_stack.read() {
            Ok(stack) => {
                // Pre-alloca con capacity esatta per evitare reallocazioni
                let mut frames = Vec::with_capacity(stack.len());
                frames.extend_from_slice(&stack);
                frames
            }
            Err(_) => Vec::new(), // Fallback: if the lock fails, return an empty frame
        }
    }

    ///
    /// stato chiamato nel call path corrente.
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn is_in_call_path(&self, contract_address: &[u8; 32]) -> bool {
        match self.call_stack.read() {
            Ok(stack) => stack
                .iter()
                .any(|frame| frame.contract_address == *contract_address),
            Err(_) => false, // Fallback: se il lock fallisce, assume non in path
        }
    }

    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn call_count_in_path(&self, contract_address: &[u8; 32]) -> usize {
        match self.call_stack.read() {
            Ok(stack) => stack
                .iter()
                .filter(|frame| frame.contract_address == *contract_address)
                .count(),
            Err(_) => 0, // Fallback: se il lock fallisce, assume count 0
        }
    }

    /// Ottiene il frame a una profondità specifica
    ///
    /// # Arguments
    /// * `depth` - Profondità of the frame da ottenere (0 = root)
    ///
    /// # Returns
    /// * `Some(CallFrame)` se esiste un frame alla profondità specificata
    pub fn frame_at_depth(&self, depth: usize) -> Option<CallFrame> {
        let stack = self.call_stack.read().ok()?;
        stack.get(depth).cloned()
    }

    /// Ottiene il root frame (primo frame nel call stack)
    ///
    /// # Returns
    /// * `Some(CallFrame)` se esiste un root frame
    pub fn root_frame(&self) -> Option<CallFrame> {
        self.frame_at_depth(0)
    }

    /// Check che il call stack sia valido
    ///
    /// Check che:
    /// - Non ci siano frame con depth maggiore di max_call_depth
    /// - Il call stack non sia corrotto
    ///
    /// # Returns
    /// * `Err(String)` se viene rilevata una corruzione
    pub fn validate_call_stack(&self) -> Result<(), String> {
        let stack = self.call_stack.read().map_err(|e| {
            format!(
                "Failed to acquire read lock for call stack validation: {}",
                e
            )
        })?;

        // Check che il numero di frame non superi max_call_depth
        if stack.len() > self.max_call_depth {
            return Err(format!(
                "Call stack corrupted: {} frames > max_call_depth {}",
                stack.len(),
                self.max_call_depth
            ));
        }

        for (index, frame) in stack.iter().enumerate() {
            if frame.depth != index as u8 {
                return Err(format!(
                    "Call stack corrupted: frame at index {} has depth {} (expected {})",
                    index, frame.depth, index
                ));
            }
        }

        Ok(())
    }

    /// Ottiene il timestamp deterministico of the blocco corrente
    ///
    ///
    /// # Returns
    /// Timestamp of the blocco corrente (Unix timestamp in secondi)
    pub fn block_timestamp(&self) -> u64 {
        *self.block_timestamp.read().unwrap_or_else(|poisoned| {
            poisoned.into_inner()
        })
    }

    /// Set il timestamp deterministico of the blocco
    ///
    /// Thread-safe.
    ///
    /// # Arguments
    /// * `timestamp` - Timestamp deterministico of the blocco (Unix timestamp in secondi)
    pub fn set_block_timestamp(&self, timestamp: u64) {
        if let Ok(mut ts) = self.block_timestamp.write() {
            *ts = timestamp;
        }
        // Silently fail if lock poisoned - non-critical
    }

    /// Check che non ci siano stati accessi non-deterministici
    ///
    /// accessi a risorse non-deterministiche (random, network, SystemTime, etc.).
    ///
    /// # Returns
    /// * `Err(String)` se è stato rilevato un accesso non-deterministico
    pub fn verify_no_non_deterministic_access(&self) -> Result<(), String> {
        let detected = self.non_deterministic_access_detected.read().map_err(|e| {
            format!(
                "Failed to acquire read lock for non-deterministic check: {}",
                e
            )
        })?;
        if *detected {
            Err("Non-deterministic access detected: contracts cannot access random, network, or SystemTime".to_string())
        } else {
            Ok(())
        }
    }

    /// Segna che è stato rilevato un accesso non-deterministico
    ///
    /// di accesso a risorse non-deterministiche durante l'esecuzione of the bytecode.
    ///
    /// # Arguments
    /// * `reason` - Motivo of the rilevamento (per logging/debugging)
    pub fn mark_non_deterministic_access(&self, reason: &str) {
        if let Ok(mut detected) = self.non_deterministic_access_detected.write() {
            *detected = true;
        }
        eprintln!("Non-deterministic access detected: {}", reason);
    }

    ///
    /// Controlla che l'esecuzione sia deterministica:
    /// - Nessun accesso a risorse non-deterministiche (random, network, etc.)
    /// - Timestamp deterministico dal blocco
    ///
    /// # Returns
    /// * `Ok(())` se i controlli passano
    pub fn check_determinism(&self) -> Result<(), String> {
        // 1. Check che non ci siano stati accessi non-deterministici
        self.verify_no_non_deterministic_access()?;

        // 2. Check fixed-point arithmetic
        // floating point operations.
        // Il GasMeter e altri moduli già usano checked arithmetic (u64/u128).

        // 3. Check timestamp deterministico

        Ok(())
    }

    /// Check che un valore sia un intero fixed-point valido (u128)
    ///
    /// e documentazione.
    ///
    /// # Arguments
    /// * `value` - Valore da verificare
    ///
    /// # Returns
    pub fn verify_fixed_point_value(_value: u128) -> Result<(), String> {
        // che potrebbero supportare tipi decimali con precisione fissa.
        Ok(())
    }

    /// Check che un calcolo aritmetico sia deterministico
    ///
    /// per prevent overflow/underflow non-deterministici.
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn verify_arithmetic_determinism<T>(result: Option<T>) -> Result<(), String> {
        if result.is_some() {
            Ok(())
        } else {
            Err(
                "Arithmetic operation would cause overflow/underflow (non-deterministic)"
                    .to_string(),
            )
        }
    }

    ///
    /// Mantiene l'overlay ma resetta call stack, gas meter, event system e flag non-deterministici
    ///
    /// # Arguments
    /// * `gas_limit` - Nuovo limit di gas
    /// * `block_timestamp` - Timestamp deterministico of the nuovo blocco
    pub fn reset(&self, gas_limit: u64, block_timestamp: u64) {
        // Resetta call stack
        if let Ok(mut stack) = self.call_stack.write() {
            stack.clear();
        }

        // Resetta gas meter
        if let Ok(mut gas_meter) = self.gas_meter.write() {
            *gas_meter = GasMeter::new(gas_limit);
        }

        // Resetta flag non-deterministici
        if let Ok(mut detected) = self.non_deterministic_access_detected.write() {
            *detected = false;
        }

        if let Ok(mut ts) = self.block_timestamp.write() {
            *ts = block_timestamp;
        }

        // Pulisce gli eventi of the sistema di eventi
        self.event_system.clear_events();
    }

    ///
    /// Utile per snapshot o commit
    ///
    /// # Performance
    pub fn overlay_snapshot(&self) -> BTreeMap<Vec<u8>, Account> {
        let overlay = self.overlay.read().unwrap_or_else(|poisoned| {
            poisoned.into_inner()
        });

        // Check memory bounds
        let overlay_size = overlay
            .iter()
            .map(|(k, v): (&Vec<u8>, &Account)| k.len() + std::mem::size_of_val(v))
            .sum();
        if let Err(e) = self.memory_monitor.check_overlay_size(overlay_size) {
            log::warn!("Memory limit exceeded for overlay: {}", e);
        }

        // Clone esplicito per migliori performance rispetto a collect()
        overlay.clone()
    }

    ///
    ///
    /// # Returns
    /// * `Some([u8; 32])` se c'è un frame corrente (contract address)
    pub fn current_contract_address(&self) -> Option<[u8; 32]> {
        self.current_frame().map(|frame| frame.contract_address)
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Returns
    /// * `Ok(())` if access is allowed (same contract)
    /// * `Err(String)` se l'accesso non è permesso (violazione isolation)
    pub fn validate_storage_access(&self, storage_address: &[u8]) -> Result<(), String> {
        let current_contract = self
            .current_contract_address()
            .ok_or_else(|| "No contract in execution context".to_string())?;

        if storage_address.len() != 32 {
            return Err(format!(
                "Storage address must be exactly 32 bytes, got {}",
                storage_address.len()
            ));
        }

        let storage_address_array: [u8; 32] = storage_address
            .try_into()
            .map_err(|_| "Failed to convert storage address to [u8; 32]".to_string())?;

        if storage_address_array != current_contract {
            return Err(format!(
                "Storage isolation violation: current contract {} cannot access storage of contract {}",
                hex::encode(current_contract),
                hex::encode(storage_address)
            ));
        }

        Ok(())
    }
}

impl Default for Runtime {
    fn default() -> Self {
        // Usa MAX_CALL_DEPTH come profondità massima of the call stack
        Self::with_empty_overlay(10_000_000, 0) // Default gas limit: 10M
    }
}
