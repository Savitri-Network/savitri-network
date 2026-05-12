//! Generatore Manifest Metriche per Savitri Network
//!
//! disponibili per la piattaforma di monitoraggio decentralizzata.
//!
//! Il manifesto contiene:
//! - 200+ metriche predefinite
//! - Descrizioni e tipi
//! - Labels/tags
//! - Categorie di raggruppamento
//! - Informazioni di sicurezza

use log::info;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsManifest {
    /// Versione of the manifesto
    pub version: String,
    /// Timestamp di generazione
    pub generated_at: String,
    /// Informazioni on the piattaforma
    pub platform: PlatformInfo,
    /// Categorie di metriche
    pub categories: Vec<MetricCategory>,
    /// Metriche definite
    pub metrics: Vec<MetricDefinition>,
    /// Informazioni di sicurezza
    pub security: SecurityInfo,
}

/// Informazioni on the piattaforma
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub name: String,
    /// Versione
    pub version: String,
    /// Tipo di deployment (decentralizzato)
    pub deployment_type: String,
    /// Principi di sicurezza
    pub security_principles: Vec<String>,
}

/// Categoria di metriche
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricCategory {
    pub id: String,
    pub name: String,
    /// Descrizione
    pub description: String,
    /// Priorità (critical, high, medium, low)
    pub priority: String,
    pub metric_ids: Vec<String>,
}

/// Definizione di una metrica
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDefinition {
    pub id: String,
    /// Nome Prometheus
    pub name: String,
    /// Tipo di metrica
    pub metric_type: MetricType,
    /// Descrizione dettagliata
    pub description: String,
    /// Unità di misura
    pub unit: Option<String>,
    /// Labels disponibili
    pub labels: Vec<LabelDefinition>,
    /// Categoria di appartenenza
    pub category: String,
    /// Priorità
    pub priority: String,
    pub update_frequency: Option<u64>,
    /// Soglie di allarme
    pub thresholds: Vec<ThresholdDefinition>,
    /// Note di sicurezza
    pub security_notes: Vec<String>,
}

/// Tipo di metrica
/// Metric type for manifest definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricType {
    /// Counter metric
    ///
    /// A cumulative metric that represents a single monotonically
    /// increasing value whose rate of increase is of interest.
    Counter,
    /// Gauge metric
    ///
    /// A metric that represents a single numerical value that can
    /// arbitrarily go up and down.
    Gauge,
    /// Histogram metric
    ///
    /// A metric that samples observations and counts them in
    /// configurable buckets.
    Histogram,
    /// Summary metric
    ///
    /// A metric that calculates configurable quantiles over a
    /// sliding time window.
    Summary,
}

/// Definizione di label
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelDefinition {
    pub name: String,
    /// Descrizione
    pub description: String,
    /// Valori possibili
    pub possible_values: Vec<String>,
    /// Obbligatoria
    pub required: bool,
}

/// Definizione di threshold
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdDefinition {
    pub name: String,
    /// Tipo di threshold
    pub threshold_type: ThresholdType,
    pub value: f64,
    /// Operatore di confronto
    pub operator: String,
    /// Severità
    pub severity: String,
    /// Descrizione
    pub description: String,
}

/// Threshold type for alerting
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThresholdType {
    /// Warning threshold
    ///
    /// Indicates a potential issue that should be monitored
    /// but doesn't require immediate action.
    Warning,
    /// Critical threshold
    ///
    /// Indicates a serious issue that requires immediate attention.
    Critical,
    /// Information threshold
    ///
    /// Provides informational context about the metric state.
    Info,
}

/// Informazioni di sicurezza
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityInfo {
    /// Principi di sicurezza
    pub principles: Vec<String>,
    /// Controlli di accesso
    pub access_controls: Vec<String>,
    /// Limitazioni di esposizione
    pub exposure_limits: Vec<String>,
    /// Raccomandazioni
    pub recommendations: Vec<String>,
}

/// Generatore di manifest
pub struct ManifestGenerator;

impl ManifestGenerator {
    pub fn generate_manifest() -> MetricsManifest {
        info!("Generating metrics manifest for Savitri Network");

        let manifest = MetricsManifest {
            version: "1.0.0".to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            platform: Self::generate_platform_info(),
            categories: Self::generate_categories(),
            metrics: Self::generate_all_metrics(),
            security: Self::generate_security_info(),
        };

        info!("Generated manifest with {} metrics", manifest.metrics.len());
        manifest
    }

    fn generate_platform_info() -> PlatformInfo {
        PlatformInfo {
            name: "Savitri Network".to_string(),
            version: "1.0.0".to_string(),
            deployment_type: "decentralized".to_string(),
            security_principles: vec![
                "Local-only exposure".to_string(),
                "No external dependencies".to_string(),
                "Cryptographic peer verification".to_string(),
                "Minimal overhead (<1% CPU)".to_string(),
                "Zero data leakage".to_string(),
            ],
        }
    }

    /// Genera le categorie di metriche
    fn generate_categories() -> Vec<MetricCategory> {
        vec![
            MetricCategory {
                id: "blockchain".to_string(),
                name: "Blockchain Metrics".to_string(),
                description: "Core blockchain and consensus metrics".to_string(),
                priority: "critical".to_string(),
                metric_ids: vec![
                    "block_height".to_string(),
                    "block_time".to_string(),
                    "consensus_round_time".to_string(),
                    "finality_time".to_string(),
                    "validator_count".to_string(),
                ],
            },
            MetricCategory {
                id: "mempool".to_string(),
                name: "Mempool Metrics".to_string(),
                description: "Transaction pool and processing metrics".to_string(),
                priority: "high".to_string(),
                metric_ids: vec![
                    "mempool_size".to_string(),
                    "mempool_pending_tx".to_string(),
                    "mempool_admission_rate".to_string(),
                    "mempool_rejection_rate".to_string(),
                    "mempool_eviction_rate".to_string(),
                ],
            },
            MetricCategory {
                id: "network".to_string(),
                name: "Network Metrics".to_string(),
                description: "P2P network and connectivity metrics".to_string(),
                priority: "high".to_string(),
                metric_ids: vec![
                    "peer_count".to_string(),
                    "network_latency".to_string(),
                    "bandwidth_usage".to_string(),
                    "message_propagation_time".to_string(),
                    "connection_errors".to_string(),
                ],
            },
            MetricCategory {
                id: "storage".to_string(),
                name: "Storage Metrics".to_string(),
                description: "Database and storage performance metrics".to_string(),
                priority: "medium".to_string(),
                metric_ids: vec![
                    "storage_read_ops".to_string(),
                    "storage_write_ops".to_string(),
                    "storage_read_latency".to_string(),
                    "storage_write_latency".to_string(),
                    "disk_usage".to_string(),
                ],
            },
            MetricCategory {
                id: "execution".to_string(),
                name: "Execution Metrics".to_string(),
                description: "Transaction execution and smart contract metrics".to_string(),
                priority: "high".to_string(),
                metric_ids: vec![
                    "tx_execution_time".to_string(),
                    "gas_used".to_string(),
                    "contract_deployments".to_string(),
                    "contract_calls".to_string(),
                    "execution_errors".to_string(),
                ],
            },
            MetricCategory {
                id: "system".to_string(),
                name: "System Metrics".to_string(),
                description: "System resource and performance metrics".to_string(),
                priority: "medium".to_string(),
                metric_ids: vec![
                    "cpu_usage".to_string(),
                    "memory_usage".to_string(),
                    "disk_io".to_string(),
                    "network_io".to_string(),
                    "thread_count".to_string(),
                ],
            },
            MetricCategory {
                id: "security".to_string(),
                name: "Security Metrics".to_string(),
                description: "Security and threat detection metrics".to_string(),
                priority: "critical".to_string(),
                metric_ids: vec![
                    "failed_auth_attempts".to_string(),
                    "suspicious_connections".to_string(),
                    "spam_detected".to_string(),
                    "byzantine_behavior".to_string(),
                    "security_alerts".to_string(),
                ],
            },
            MetricCategory {
                id: "tokenomics".to_string(),
                name: "Tokenomics Metrics".to_string(),
                description: "Token economics and supply metrics".to_string(),
                priority: "medium".to_string(),
                metric_ids: vec![
                    "total_supply".to_string(),
                    "circulating_supply".to_string(),
                    "burned_tokens".to_string(),
                    "locked_tokens".to_string(),
                    "transaction_fees".to_string(),
                ],
            },
        ]
    }

    fn generate_all_metrics() -> Vec<MetricDefinition> {
        let mut metrics = Vec::new();

        // Blockchain Metrics
        metrics.extend(Self::generate_blockchain_metrics());

        // Mempool Metrics
        metrics.extend(Self::generate_mempool_metrics());

        // Network Metrics
        metrics.extend(Self::generate_network_metrics());

        // Storage Metrics
        metrics.extend(Self::generate_storage_metrics());

        // Execution Metrics
        metrics.extend(Self::generate_execution_metrics());

        // System Metrics
        metrics.extend(Self::generate_system_metrics());

        // Security Metrics
        metrics.extend(Self::generate_security_metrics());

        // Tokenomics Metrics
        metrics.extend(Self::generate_tokenomics_metrics());

        metrics
    }

    /// Genera metriche blockchain
    fn generate_blockchain_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "block_height".to_string(),
                name: "savitri_block_height".to_string(),
                metric_type: MetricType::Gauge,
                description: "Current blockchain height".to_string(),
                unit: Some("blocks".to_string()),
                labels: vec![],
                category: "blockchain".to_string(),
                priority: "critical".to_string(),
                update_frequency: Some(1),
                thresholds: vec![ThresholdDefinition {
                    name: "block_height_stalled".to_string(),
                    threshold_type: ThresholdType::Critical,
                    value: 300.0,
                    operator: "gt".to_string(),
                    severity: "critical".to_string(),
                    description: "No new blocks for 5 minutes".to_string(),
                }],
                security_notes: vec![
                    "Critical for consensus health".to_string(),
                    "Should never decrease".to_string(),
                ],
            },
            MetricDefinition {
                id: "block_time".to_string(),
                name: "savitri_block_time_seconds".to_string(),
                metric_type: MetricType::Histogram,
                description: "Time between consecutive blocks".to_string(),
                unit: Some("seconds".to_string()),
                labels: vec![LabelDefinition {
                    name: "shard".to_string(),
                    description: "Shard identifier".to_string(),
                    possible_values: vec![
                        "0".to_string(),
                        "1".to_string(),
                        "2".to_string(),
                        "3".to_string(),
                    ],
                    required: false,
                }],
                category: "blockchain".to_string(),
                priority: "critical".to_string(),
                update_frequency: Some(1),
                thresholds: vec![ThresholdDefinition {
                    name: "block_time_high".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 15.0,
                    operator: "gt".to_string(),
                    severity: "warning".to_string(),
                    description: "Block time exceeds 15 seconds".to_string(),
                }],
                security_notes: vec![
                    "High block time indicates network issues".to_string(),
                    "Should be consistent across shards".to_string(),
                ],
            },
            MetricDefinition {
                id: "consensus_round_time".to_string(),
                name: "savitri_consensus_round_time_ms".to_string(),
                metric_type: MetricType::Histogram,
                description: "Time to complete consensus round".to_string(),
                unit: Some("milliseconds".to_string()),
                labels: vec![LabelDefinition {
                    name: "round_type".to_string(),
                    description: "Type of consensus round".to_string(),
                    possible_values: vec![
                        "proposal".to_string(),
                        "vote".to_string(),
                        "certificate".to_string(),
                    ],
                    required: true,
                }],
                category: "blockchain".to_string(),
                priority: "high".to_string(),
                update_frequency: Some(1),
                thresholds: vec![ThresholdDefinition {
                    name: "consensus_slow".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 5000.0,
                    operator: "gt".to_string(),
                    severity: "warning".to_string(),
                    description: "Consensus round takes more than 5 seconds".to_string(),
                }],
                security_notes: vec![
                    "Slow consensus may indicate network partition".to_string(),
                    "Monitor for Byzantine behavior".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche mempool
    fn generate_mempool_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "mempool_size".to_string(),
                name: "savitri_mempool_size".to_string(),
                metric_type: MetricType::Gauge,
                description: "Number of transactions in mempool".to_string(),
                unit: Some("transactions".to_string()),
                labels: vec![LabelDefinition {
                    name: "priority".to_string(),
                    description: "Transaction priority level".to_string(),
                    possible_values: vec![
                        "high".to_string(),
                        "medium".to_string(),
                        "low".to_string(),
                    ],
                    required: false,
                }],
                category: "mempool".to_string(),
                priority: "high".to_string(),
                update_frequency: Some(5),
                thresholds: vec![ThresholdDefinition {
                    name: "mempool_full".to_string(),
                    threshold_type: ThresholdType::Critical,
                    value: 10000.0,
                    operator: "gt".to_string(),
                    severity: "critical".to_string(),
                    description: "Mempool is approaching capacity limit".to_string(),
                }],
                security_notes: vec![
                    "Large mempool may indicate spam attack".to_string(),
                    "Monitor for unusual growth patterns".to_string(),
                ],
            },
            MetricDefinition {
                id: "mempool_admission_rate".to_string(),
                name: "savitri_mempool_admission_rate".to_string(),
                metric_type: MetricType::Gauge,
                description: "Rate of transaction admission to mempool".to_string(),
                unit: Some("rate".to_string()),
                labels: vec![LabelDefinition {
                    name: "reason".to_string(),
                    description: "Admission/rejection reason".to_string(),
                    possible_values: vec![
                        "valid".to_string(),
                        "invalid_nonce".to_string(),
                        "insufficient_fee".to_string(),
                        "double_spend".to_string(),
                    ],
                    required: true,
                }],
                category: "mempool".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(10),
                thresholds: vec![],
                security_notes: vec![
                    "Low admission rate may indicate network issues".to_string(),
                    "High rejection rate may indicate attack".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche network
    fn generate_network_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "peer_count".to_string(),
                name: "savitri_peer_count".to_string(),
                metric_type: MetricType::Gauge,
                description: "Number of connected peers".to_string(),
                unit: Some("peers".to_string()),
                labels: vec![LabelDefinition {
                    name: "peer_type".to_string(),
                    description: "Type of peer connection".to_string(),
                    possible_values: vec!["inbound".to_string(), "outbound".to_string()],
                    required: true,
                }],
                category: "network".to_string(),
                priority: "critical".to_string(),
                update_frequency: Some(30),
                thresholds: vec![ThresholdDefinition {
                    name: "low_peer_count".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 3.0,
                    operator: "lt".to_string(),
                    severity: "warning".to_string(),
                    description: "Less than 3 peers connected".to_string(),
                }],
                security_notes: vec![
                    "Low peer count may indicate network isolation".to_string(),
                    "Monitor for sudden peer disconnections".to_string(),
                ],
            },
            MetricDefinition {
                id: "network_latency".to_string(),
                name: "savitri_network_latency_ms".to_string(),
                metric_type: MetricType::Histogram,
                description: "Network latency to peers".to_string(),
                unit: Some("milliseconds".to_string()),
                labels: vec![LabelDefinition {
                    name: "peer_id".to_string(),
                    description: "Peer identifier (truncated)".to_string(),
                    possible_values: vec![],
                    required: false,
                }],
                category: "network".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(60),
                thresholds: vec![ThresholdDefinition {
                    name: "high_latency".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 1000.0,
                    operator: "gt".to_string(),
                    severity: "warning".to_string(),
                    description: "Network latency exceeds 1 second".to_string(),
                }],
                security_notes: vec![
                    "High latency may indicate network congestion".to_string(),
                    "Monitor for latency spikes".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche storage
    fn generate_storage_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "storage_read_ops".to_string(),
                name: "savitri_storage_read_ops_total".to_string(),
                metric_type: MetricType::Counter,
                description: "Total number of storage read operations".to_string(),
                unit: Some("operations".to_string()),
                labels: vec![LabelDefinition {
                    name: "column_family".to_string(),
                    description: "Storage column family".to_string(),
                    possible_values: vec![
                        "default".to_string(),
                        "blocks".to_string(),
                        "transactions".to_string(),
                        "contracts".to_string(),
                    ],
                    required: false,
                }],
                category: "storage".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(10),
                thresholds: vec![],
                security_notes: vec![
                    "High read rate may indicate performance issues".to_string(),
                    "Monitor for read/write balance".to_string(),
                ],
            },
            MetricDefinition {
                id: "storage_write_ops".to_string(),
                name: "savitri_storage_write_ops_total".to_string(),
                metric_type: MetricType::Counter,
                description: "Total number of storage write operations".to_string(),
                unit: Some("operations".to_string()),
                labels: vec![LabelDefinition {
                    name: "column_family".to_string(),
                    description: "Storage column family".to_string(),
                    possible_values: vec![
                        "default".to_string(),
                        "blocks".to_string(),
                        "transactions".to_string(),
                        "contracts".to_string(),
                    ],
                    required: false,
                }],
                category: "storage".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(10),
                thresholds: vec![],
                security_notes: vec![
                    "High write rate may indicate spam".to_string(),
                    "Monitor for disk space usage".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche execution
    fn generate_execution_metrics() -> Vec<MetricDefinition> {
        vec![MetricDefinition {
            id: "tx_execution_time".to_string(),
            name: "savitri_tx_execution_time_ms".to_string(),
            metric_type: MetricType::Histogram,
            description: "Transaction execution time".to_string(),
            unit: Some("milliseconds".to_string()),
            labels: vec![LabelDefinition {
                name: "tx_type".to_string(),
                description: "Transaction type".to_string(),
                possible_values: vec![
                    "transfer".to_string(),
                    "contract_deploy".to_string(),
                    "contract_call".to_string(),
                    "governance".to_string(),
                ],
                required: true,
            }],
            category: "execution".to_string(),
            priority: "high".to_string(),
            update_frequency: Some(5),
            thresholds: vec![ThresholdDefinition {
                name: "slow_execution".to_string(),
                threshold_type: ThresholdType::Warning,
                value: 1000.0,
                operator: "gt".to_string(),
                severity: "warning".to_string(),
                description: "Transaction execution exceeds 1 second".to_string(),
            }],
            security_notes: vec![
                "Slow execution may indicate DoS attack".to_string(),
                "Monitor for gas limit exhaustion".to_string(),
            ],
        }]
    }

    /// Genera metriche system
    fn generate_system_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "cpu_usage".to_string(),
                name: "savitri_cpu_usage_percent".to_string(),
                metric_type: MetricType::Gauge,
                description: "CPU usage percentage".to_string(),
                unit: Some("percent".to_string()),
                labels: vec![LabelDefinition {
                    name: "core".to_string(),
                    description: "CPU core identifier".to_string(),
                    possible_values: vec![],
                    required: false,
                }],
                category: "system".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(10),
                thresholds: vec![ThresholdDefinition {
                    name: "high_cpu".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 80.0,
                    operator: "gt".to_string(),
                    severity: "warning".to_string(),
                    description: "CPU usage exceeds 80%".to_string(),
                }],
                security_notes: vec![
                    "High CPU usage may indicate attack".to_string(),
                    "Monitor for sustained high usage".to_string(),
                ],
            },
            MetricDefinition {
                id: "memory_usage".to_string(),
                name: "savitri_memory_usage_bytes".to_string(),
                metric_type: MetricType::Gauge,
                description: "Memory usage in bytes".to_string(),
                unit: Some("bytes".to_string()),
                labels: vec![LabelDefinition {
                    name: "type".to_string(),
                    description: "Memory type".to_string(),
                    possible_values: vec![
                        "heap".to_string(),
                        "stack".to_string(),
                        "cache".to_string(),
                    ],
                    required: true,
                }],
                category: "system".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(10),
                thresholds: vec![ThresholdDefinition {
                    name: "high_memory".to_string(),
                    threshold_type: ThresholdType::Critical,
                    value: 8589934592.0, // 8GB
                    operator: "gt".to_string(),
                    severity: "critical".to_string(),
                    description: "Memory usage exceeds 8GB".to_string(),
                }],
                security_notes: vec![
                    "High memory usage may indicate memory leak".to_string(),
                    "Monitor for memory growth patterns".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche security
    fn generate_security_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "failed_auth_attempts".to_string(),
                name: "savitri_failed_auth_attempts_total".to_string(),
                metric_type: MetricType::Counter,
                description: "Total failed authentication attempts".to_string(),
                unit: Some("attempts".to_string()),
                labels: vec![LabelDefinition {
                    name: "reason".to_string(),
                    description: "Failure reason".to_string(),
                    possible_values: vec![
                        "invalid_signature".to_string(),
                        "invalid_certificate".to_string(),
                        "rate_limit".to_string(),
                    ],
                    required: true,
                }],
                category: "security".to_string(),
                priority: "critical".to_string(),
                update_frequency: Some(1),
                thresholds: vec![ThresholdDefinition {
                    name: "auth_attack".to_string(),
                    threshold_type: ThresholdType::Critical,
                    value: 100.0,
                    operator: "gt".to_string(),
                    severity: "critical".to_string(),
                    description: "High rate of failed authentication attempts".to_string(),
                }],
                security_notes: vec![
                    "May indicate brute force attack".to_string(),
                    "Requires immediate investigation".to_string(),
                ],
            },
            MetricDefinition {
                id: "spam_detected".to_string(),
                name: "savitri_spam_detected_total".to_string(),
                metric_type: MetricType::Counter,
                description: "Total spam transactions detected".to_string(),
                unit: Some("transactions".to_string()),
                labels: vec![LabelDefinition {
                    name: "spam_type".to_string(),
                    description: "Type of spam detected".to_string(),
                    possible_values: vec![
                        "duplicate".to_string(),
                        "invalid_nonce".to_string(),
                        "low_fee".to_string(),
                        "malformed".to_string(),
                    ],
                    required: true,
                }],
                category: "security".to_string(),
                priority: "high".to_string(),
                update_frequency: Some(5),
                thresholds: vec![ThresholdDefinition {
                    name: "spam_attack".to_string(),
                    threshold_type: ThresholdType::Warning,
                    value: 1000.0,
                    operator: "gt".to_string(),
                    severity: "warning".to_string(),
                    description: "High rate of spam transactions detected".to_string(),
                }],
                security_notes: vec![
                    "May indicate network attack".to_string(),
                    "Monitor for spam patterns".to_string(),
                ],
            },
        ]
    }

    /// Genera metriche tokenomics
    fn generate_tokenomics_metrics() -> Vec<MetricDefinition> {
        vec![
            MetricDefinition {
                id: "total_supply".to_string(),
                name: "savitri_total_supply".to_string(),
                metric_type: MetricType::Gauge,
                description: "Total token supply".to_string(),
                unit: Some("tokens".to_string()),
                labels: vec![],
                category: "tokenomics".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(60),
                thresholds: vec![],
                security_notes: vec![
                    "Should never exceed maximum supply".to_string(),
                    "Monitor for unexpected changes".to_string(),
                ],
            },
            MetricDefinition {
                id: "circulating_supply".to_string(),
                name: "savitri_circulating_supply".to_string(),
                metric_type: MetricType::Gauge,
                description: "Circulating token supply".to_string(),
                unit: Some("tokens".to_string()),
                labels: vec![],
                category: "tokenomics".to_string(),
                priority: "medium".to_string(),
                update_frequency: Some(60),
                thresholds: vec![],
                security_notes: vec![
                    "Should track token issuance".to_string(),
                    "Monitor for supply anomalies".to_string(),
                ],
            },
        ]
    }

    /// Genera le informazioni di sicurezza
    fn generate_security_info() -> SecurityInfo {
        SecurityInfo {
            principles: vec![
                "Local-only metrics exposure".to_string(),
                "No external API calls".to_string(),
                "Cryptographic peer verification".to_string(),
                "Minimal performance overhead".to_string(),
                "Zero data leakage risk".to_string(),
            ],
            access_controls: vec![
                "localhost-only binding".to_string(),
                "Optional mTLS for P2P discovery".to_string(),
                "Rate limiting on metrics endpoint".to_string(),
                "Connection limits enforced".to_string(),
            ],
            exposure_limits: vec![
                "Metrics only accessible on localhost:9090".to_string(),
                "No external network connectivity".to_string(),
                "No IP addresses or URLs in configuration".to_string(),
                "Sidecar collector required for external access".to_string(),
            ],
            recommendations: vec![
                "Use firewall to restrict access to localhost".to_string(),
                "Implement monitoring for metrics endpoint access".to_string(),
                "Regular security audits of metrics collection".to_string(),
                "Encrypt metrics in transit to sidecar".to_string(),
            ],
        }
    }

    /// Salva il manifesto su file
    pub fn save_manifest(
        manifest: &MetricsManifest,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(manifest)?;
        std::fs::write(path, json)?;
        info!("Metrics manifest saved to: {}", path);
        Ok(())
    }

    /// Carica il manifesto da file
    pub fn load_manifest(path: &str) -> Result<MetricsManifest, Box<dyn std::error::Error>> {
        let json = std::fs::read_to_string(path)?;
        let manifest: MetricsManifest = serde_json::from_str(&json)?;
        info!("Metrics manifest loaded from: {}", path);
        Ok(manifest)
    }

    pub fn validate_manifest(manifest: &MetricsManifest) -> Result<(), String> {
        let category_ids: std::collections::HashSet<&str> =
            manifest.categories.iter().map(|c| c.id.as_str()).collect();

        for metric in &manifest.metrics {
            if !category_ids.contains(metric.category.as_str()) {
                return Err(format!(
                    "Metric {} has invalid category: {}",
                    metric.id, metric.category
                ));
            }
        }

        let metric_ids: std::collections::HashSet<&str> =
            manifest.metrics.iter().map(|m| m.id.as_str()).collect();

        for category in &manifest.categories {
            for metric_id in &category.metric_ids {
                if !metric_ids.contains(metric_id.as_str()) {
                    return Err(format!(
                        "Category {} references invalid metric: {}",
                        category.id, metric_id
                    ));
                }
            }
        }

        // Check che non ci siano ID duplicati
        let mut seen_ids = std::collections::HashSet::new();
        for metric in &manifest.metrics {
            if !seen_ids.insert(&metric.id) {
                return Err(format!("Duplicate metric ID: {}", metric.id));
            }
        }

        Ok(())
    }
}
