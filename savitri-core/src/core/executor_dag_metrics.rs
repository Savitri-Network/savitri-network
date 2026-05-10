//! Metriche e monitoring per DAG Execution
//!

use std::sync::{Arc, Mutex};

/// Metriche per monitoring DAG execution
///
/// Definizione copiata da Savitri-contracts/src/contracts/parallel.rs per evitare dipendenza circolare
#[derive(Debug, Clone, Default)]
pub struct ParallelDAGMetrics {
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

impl ParallelDAGMetrics {
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

/// Metriche per una singola esecuzione DAG
#[derive(Debug, Clone, Default)]
pub struct DAGExecutionMetrics {
    pub total_transactions: usize,
    pub dependencies_count: usize,
    pub dependency_detection_time_us: u64,
    pub execution_time_us: u64,
}

/// Metriche globali per DAG execution (thread-safe)
#[derive(Clone)]
pub struct GlobalDAGMetrics {
    inner: Arc<Mutex<ParallelDAGMetrics>>,
}

impl GlobalDAGMetrics {
    /// Creates nuove metriche globali
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ParallelDAGMetrics::new())),
        }
    }

    /// Registra una esecuzione DAG
    pub fn record_execution(&self, metrics: &DAGExecutionMetrics) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.total_executions += 1;
            guard.total_transactions += metrics.total_transactions as u64;
            guard.total_dependencies += metrics.dependencies_count as u64;
            guard.total_dependency_detection_time_us += metrics.dependency_detection_time_us;
            guard.total_execution_time_us += metrics.execution_time_us;
        }
    }

    /// Registra un cache hit
    pub fn record_cache_hit(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.cache_hits += 1;
        }
    }

    /// Registra un cache miss
    pub fn record_cache_miss(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.cache_misses += 1;
        }
    }

    pub fn snapshot(&self) -> ParallelDAGMetrics {
        self.inner
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| ParallelDAGMetrics::new())
    }

    /// Log metriche in formato strutturato
    pub fn log_metrics(&self, level: LogLevel) {
        let metrics = self.snapshot();

        if metrics.total_executions == 0 {
            return; // Nessuna metrica da loggare
        }

        let cache_hit_rate = metrics.cache_hit_rate();
        let avg_dep_time = metrics.avg_dependency_detection_time_us();
        let avg_exec_time = metrics.avg_execution_time_us();

        let message = format!(
            "DAG Execution Metrics: {} executions, {} TX, {} deps, cache_hit_rate={:.2}%, avg_dep_time={:.2}µs, avg_exec_time={:.2}µs",
            metrics.total_executions,
            metrics.total_transactions,
            metrics.total_dependencies,
            cache_hit_rate * 100.0,
            avg_dep_time,
            avg_exec_time
        );

        match level {
            LogLevel::Info => println!("[INFO] {}", message),
            LogLevel::Debug => println!("[DEBUG] {}", message),
            LogLevel::Trace => println!("[TRACE] {}", message),
        }
    }
}

impl Default for GlobalDAGMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Livello di logging per metriche
#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Debug,
    Trace,
}

/// Helper per esportare metriche in formato Prometheus-compatibile
pub fn export_prometheus_metrics(metrics: &ParallelDAGMetrics) -> String {
    format!(
        r#"# HELP dag_executions_total Total number of DAG executions
# TYPE dag_executions_total counter
dag_executions_total {} {}

# HELP dag_transactions_total Total number of transactions processed
# TYPE dag_transactions_total counter
dag_transactions_total {} {}

# HELP dag_dependencies_total Total number of dependencies detected
# TYPE dag_dependencies_total counter
dag_dependencies_total {} {}

# HELP dag_cache_hits_total Total number of cache hits
# TYPE dag_cache_hits_total counter
dag_cache_hits_total {} {}

# HELP dag_cache_misses_total Total number of cache misses
# TYPE dag_cache_misses_total counter
dag_cache_misses_total {} {}

# HELP dag_cache_hit_rate Cache hit rate (0.0-1.0)
# TYPE dag_cache_hit_rate gauge
dag_cache_hit_rate {} {}

# HELP dag_avg_dependency_detection_time_us Average dependency detection time in microseconds
# TYPE dag_avg_dependency_detection_time_us gauge
dag_avg_dependency_detection_time_us {} {}

# HELP dag_avg_execution_time_us Average execution time per transaction in microseconds
# TYPE dag_avg_execution_time_us gauge
dag_avg_execution_time_us {} {}
"#,
        metrics.total_executions,
        metrics.total_executions as f64,
        metrics.total_transactions,
        metrics.total_transactions as f64,
        metrics.total_dependencies,
        metrics.total_dependencies as f64,
        metrics.cache_hits,
        metrics.cache_hits as f64,
        metrics.cache_misses,
        metrics.cache_misses as f64,
        metrics.cache_hit_rate(),
        metrics.cache_hit_rate(),
        metrics.avg_dependency_detection_time_us(),
        metrics.avg_dependency_detection_time_us(),
        metrics.avg_execution_time_us(),
        metrics.avg_execution_time_us(),
    )
}
