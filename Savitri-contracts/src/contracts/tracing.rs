//! Smart Contract Tracing System
//!
//! - Execution trace per contract calls
//! - Storage access logging
//! - Event emission tracing
//! - Gas usage breakdown
//! - Debug mode per development

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Livelli di log per il debug mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Formato di export per le trace
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExportFormat {
    Json,
    PrettyJson,
    Csv,
}

/// Accesso allo storage (lettura/scrittura)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageAccess {
    pub key: Vec<u8>,
    /// Valore letto (per letture)
    pub value_read: Option<Vec<u8>>,
    /// Valore scritto (per scritture)
    pub value_written: Option<Vec<u8>>,
    /// Tipo di accesso
    pub access_type: StorageAccessType,
    /// Gas consumato per l'accesso
    pub gas_used: u64,
    /// Timestamp dell'accesso
    pub timestamp: Duration,
}

/// Tipo di accesso allo storage
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StorageAccessType {
    Read,
    Write,
    Delete,
}

/// Traccia di un evento emesso
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventTrace {
    pub contract_address: [u8; 32],
    /// Topic dell'evento
    pub topics: Vec<[u8; 32]>,
    /// Dati dell'evento
    pub data: Vec<u8>,
    /// Gas consumato per l'emissione
    pub gas_used: u64,
    /// Timestamp dell'emissione
    pub timestamp: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GasBreakdown {
    /// Gas totale consumato
    pub total_gas: u64,
    /// Gas per esecuzione bytecode
    pub execution_gas: u64,
    /// Gas per accessi storage
    pub storage_gas: u64,
    /// Gas per chiamate esterne
    pub call_gas: u64,
    /// Gas per eventi
    pub event_gas: u64,
    pub operation_breakdown: HashMap<String, u64>,
    /// Gas usage per linea di codice (se disponibile)
    pub line_breakdown: HashMap<u32, u64>,
}

/// Traccia completa di esecuzione di un contract
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContractExecutionTrace {
    pub trace_id: String,
    pub contract_address: [u8; 32],
    /// Indirizzo of the chiamante
    pub caller: [u8; 32],
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    /// Gas utilizzato
    pub gas_used: u64,
    /// Breakdown dettagliato of the gas
    pub gas_breakdown: GasBreakdown,
    /// Accessi allo storage
    pub storage_accesses: Vec<StorageAccess>,
    /// Eventi emessi
    pub events_emitted: Vec<EventTrace>,
    /// Sottocallate (per nested calls)
    pub sub_calls: Vec<ContractExecutionTrace>,
    /// Tempo di esecuzione
    pub execution_time: Duration,
    /// Successo o fallimento
    pub success: bool,
    /// Messaggio di errore (se fallito)
    pub error_message: Option<String>,
    pub depth: u8,
    /// Timestamp di inizio
    pub start_time: Duration,
}

/// Configurazione of the sistema di tracing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Abilita/disabilita il tracing
    pub enabled: bool,
    /// Livello di log
    pub log_level: LogLevel,
    /// Formato di export
    pub export_format: ExportFormat,
    /// Traccia accessi storage
    pub trace_storage_access: bool,
    /// Traccia gas breakdown
    pub trace_gas_breakdown: bool,
    /// Traccia eventi
    pub trace_events: bool,
    /// Limit massimo di subcall da tracciare
    pub max_sub_calls: usize,
    /// Limit massimo di storage access da tracciare
    pub max_storage_accesses: usize,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            log_level: LogLevel::Info,
            export_format: ExportFormat::Json,
            trace_storage_access: true,
            trace_gas_breakdown: true,
            trace_events: true,
            max_sub_calls: 100,
            max_storage_accesses: 1000,
        }
    }
}

/// Tracer per l'esecuzione dei contract
#[derive(Clone)]
pub struct ContractTracer {
    /// Configurazione of the tracer
    config: TracingConfig,
    trace_stack: VecDeque<ContractExecutionTrace>,
    /// Trace completate
    completed_traces: Vec<ContractExecutionTrace>,
    /// Contatore per ID unici
    trace_counter: u64,
    /// Tempo di inizio assoluto
    start_time: Instant,
}

impl ContractTracer {
    pub fn new(config: TracingConfig) -> Self {
        Self {
            config,
            trace_stack: VecDeque::new(),
            completed_traces: Vec::new(),
            trace_counter: 0,
            start_time: Instant::now(),
        }
    }

    pub fn start_trace(
        &mut self,
        contract_address: [u8; 32],
        caller: [u8; 32],
        input: Vec<u8>,
        depth: u8,
    ) -> Result<String> {
        if !self.config.enabled {
            return Ok(String::new());
        }

        let trace_id = format!("trace_{}", self.trace_counter);
        self.trace_counter += 1;

        let trace = ContractExecutionTrace {
            trace_id: trace_id.clone(),
            contract_address,
            caller,
            input,
            output: Vec::new(),
            gas_used: 0,
            gas_breakdown: GasBreakdown {
                total_gas: 0,
                execution_gas: 0,
                storage_gas: 0,
                call_gas: 0,
                event_gas: 0,
                operation_breakdown: HashMap::new(),
                line_breakdown: HashMap::new(),
            },
            storage_accesses: Vec::new(),
            events_emitted: Vec::new(),
            sub_calls: Vec::new(),
            execution_time: Duration::ZERO,
            success: true,
            error_message: None,
            depth,
            start_time: self.start_time.elapsed(),
        };

        self.trace_stack.push_back(trace);
        Ok(trace_id)
    }

    /// Registra un accesso allo storage
    pub fn trace_storage_access(
        &mut self,
        key: Vec<u8>,
        value_read: Option<Vec<u8>>,
        value_written: Option<Vec<u8>>,
        access_type: StorageAccessType,
        gas_used: u64,
    ) -> Result<()> {
        if !self.config.enabled || !self.config.trace_storage_access {
            return Ok(());
        }

        if let Some(current_trace) = self.trace_stack.back_mut() {
            if current_trace.storage_accesses.len() >= self.config.max_storage_accesses {
                return Ok(());
            }

            let access = StorageAccess {
                key,
                value_read,
                value_written,
                access_type,
                gas_used,
                timestamp: self.start_time.elapsed(),
            };

            current_trace.storage_accesses.push(access);
            current_trace.gas_breakdown.storage_gas += gas_used;
        }

        Ok(())
    }

    /// Registra un evento emesso
    pub fn trace_event(
        &mut self,
        contract_address: [u8; 32],
        topics: Vec<[u8; 32]>,
        data: Vec<u8>,
        gas_used: u64,
    ) -> Result<()> {
        if !self.config.enabled || !self.config.trace_events {
            return Ok(());
        }

        if let Some(current_trace) = self.trace_stack.back_mut() {
            let event = EventTrace {
                contract_address,
                topics,
                data,
                gas_used,
                timestamp: self.start_time.elapsed(),
            };

            current_trace.events_emitted.push(event);
            current_trace.gas_breakdown.event_gas += gas_used;
        }

        Ok(())
    }

    pub fn trace_gas_operation(&mut self, operation: &str, gas_used: u64) -> Result<()> {
        if !self.config.enabled || !self.config.trace_gas_breakdown {
            return Ok(());
        }

        if let Some(current_trace) = self.trace_stack.back_mut() {
            *current_trace
                .gas_breakdown
                .operation_breakdown
                .entry(operation.to_string())
                .or_insert(0) += gas_used;
            current_trace.gas_breakdown.total_gas += gas_used;
        }

        Ok(())
    }

    /// Registra l'uso di gas per linea di codice
    pub fn trace_gas_line(&mut self, line: u32, gas_used: u64) -> Result<()> {
        if !self.config.enabled || !self.config.trace_gas_breakdown {
            return Ok(());
        }

        if let Some(current_trace) = self.trace_stack.back_mut() {
            *current_trace
                .gas_breakdown
                .line_breakdown
                .entry(line)
                .or_insert(0) += gas_used;
        }

        Ok(())
    }

    /// Adds una subcall alla trace corrente
    pub fn add_sub_call(&mut self, sub_trace: ContractExecutionTrace) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if let Some(current_trace) = self.trace_stack.back_mut() {
            if current_trace.sub_calls.len() >= self.config.max_sub_calls {
                return Ok(());
            }

            current_trace.sub_calls.push(sub_trace);
        }

        Ok(())
    }

    /// Completa la trace corrente
    pub fn complete_trace(
        &mut self,
        output: Vec<u8>,
        success: bool,
        error_message: Option<String>,
    ) -> Result<ContractExecutionTrace> {
        if !self.config.enabled {
            // Return empty trace for consistency
            return Ok(ContractExecutionTrace {
                trace_id: String::new(),
                contract_address: [0u8; 32],
                caller: [0u8; 32],
                input: Vec::new(),
                output,
                gas_used: 0,
                gas_breakdown: GasBreakdown {
                    total_gas: 0,
                    execution_gas: 0,
                    storage_gas: 0,
                    call_gas: 0,
                    event_gas: 0,
                    operation_breakdown: HashMap::new(),
                    line_breakdown: HashMap::new(),
                },
                storage_accesses: Vec::new(),
                events_emitted: Vec::new(),
                sub_calls: Vec::new(),
                execution_time: Duration::ZERO,
                success,
                error_message,
                depth: 0,
                start_time: Duration::ZERO,
            });
        }

        let mut trace = self
            .trace_stack
            .pop_back()
            .ok_or_else(|| anyhow::anyhow!("No active trace to complete"))?;

        trace.output = output;
        trace.success = success;
        trace.error_message = error_message;
        trace.execution_time = self.start_time.elapsed() - trace.start_time;

        // Compute il gas totale
        trace.gas_used = trace.gas_breakdown.total_gas;

        let completed_trace = trace.clone();
        self.completed_traces.push(trace);

        Ok(completed_trace)
    }

    /// Ottiene la trace corrente
    pub fn get_current_trace(&self) -> Option<&ContractExecutionTrace> {
        self.trace_stack.back()
    }

    pub fn get_completed_traces(&self) -> &[ContractExecutionTrace] {
        &self.completed_traces
    }

    /// Esporta le trace in formato JSON
    pub fn export_traces(&self) -> Result<String> {
        if !self.config.enabled {
            return Ok(String::new());
        }

        match self.config.export_format {
            ExportFormat::Json => serde_json::to_string(&self.completed_traces).map_err(Into::into),
            ExportFormat::PrettyJson => {
                serde_json::to_string_pretty(&self.completed_traces).map_err(Into::into)
            }
            ExportFormat::Csv => self.export_csv(),
        }
    }

    /// Esporta le trace in formato CSV
    fn export_csv(&self) -> Result<String> {
        if !self.config.enabled {
            return Ok(String::new());
        }

        let mut csv = String::new();
        csv.push_str("trace_id,contract_address,caller,gas_used,execution_time,success,storage_accesses,events_emitted,sub_calls\n");

        for trace in &self.completed_traces {
            csv.push_str(&format!(
                "{},{},{},{},{},{},{},{},{}\n",
                trace.trace_id,
                hex::encode(trace.contract_address),
                hex::encode(trace.caller),
                trace.gas_used,
                trace.execution_time.as_millis(),
                trace.success,
                trace.storage_accesses.len(),
                trace.events_emitted.len(),
                trace.sub_calls.len()
            ));
        }

        Ok(csv)
    }

    pub fn clear_traces(&mut self) {
        self.trace_stack.clear();
        self.completed_traces.clear();
    }

    pub fn update_config(&mut self, config: TracingConfig) {
        self.config = config;
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Ottiene statistiche sulle trace
    pub fn get_trace_stats(&self) -> TraceStats {
        TraceStats {
            total_traces: self.completed_traces.len(),
            active_traces: self.trace_stack.len(),
            total_storage_accesses: self
                .completed_traces
                .iter()
                .map(|t| t.storage_accesses.len())
                .sum(),
            total_events: self
                .completed_traces
                .iter()
                .map(|t| t.events_emitted.len())
                .sum(),
            total_gas_used: self.completed_traces.iter().map(|t| t.gas_used).sum(),
            average_execution_time: if self.completed_traces.is_empty() {
                Duration::ZERO
            } else {
                let total: Duration = self.completed_traces.iter().map(|t| t.execution_time).sum();
                total / self.completed_traces.len() as u32
            },
        }
    }
}

/// Statistiche sulle trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceStats {
    /// Numero totale di trace completate
    pub total_traces: usize,
    pub active_traces: usize,
    /// Numero totale di accessi storage
    pub total_storage_accesses: usize,
    /// Numero totale di eventi
    pub total_events: usize,
    /// Gas totale utilizzato
    pub total_gas_used: u64,
    /// Tempo medio di esecuzione
    pub average_execution_time: Duration,
}

/// Tracer globale per uso in tutto il sistema - THREAD-SAFE
pub static GLOBAL_TRACER: std::sync::Mutex<Option<ContractTracer>> = std::sync::Mutex::new(None);
static TRACER_INIT: std::sync::Once = std::sync::Once::new();

/// Initializes the tracer globale
pub fn init_global_tracer(config: TracingConfig) {
    TRACER_INIT.call_once(|| {
        let tracer = ContractTracer::new(config);
        *GLOBAL_TRACER.lock().unwrap() = Some(tracer);
    });
}

/// Ottiene il tracer globale - THREAD-SAFE
pub fn get_global_tracer() -> Option<ContractTracer> {
    GLOBAL_TRACER.lock().unwrap().as_ref().cloned()
}

#[macro_export]
macro_rules! trace_storage_read {
    ($key:expr, $value:expr, $gas:expr) => {
        if let Some(tracer) = $crate::contracts::tracing::get_global_tracer() {
            let _ = tracer.trace_storage_access(
                $key.to_vec(),
                Some($value.to_vec()),
                None,
                $crate::contracts::tracing::StorageAccessType::Read,
                $gas,
            );
        }
    };
}

#[macro_export]
macro_rules! trace_storage_write {
    ($key:expr, $old_value:expr, $new_value:expr, $gas:expr) => {
        if let Some(tracer) = $crate::contracts::tracing::get_global_tracer() {
            let _ = tracer.trace_storage_access(
                $key.to_vec(),
                $old_value.map(|v| v.to_vec()),
                Some($new_value.to_vec()),
                $crate::contracts::tracing::StorageAccessType::Write,
                $gas,
            );
        }
    };
}

#[macro_export]
macro_rules! trace_gas_operation {
    ($operation:expr, $gas:expr) => {
        if let Some(tracer) = $crate::contracts::tracing::get_global_tracer() {
            let _ = tracer.trace_gas_operation($operation, $gas);
        }
    };
}
