//! Macro System per Metriche Savitri - Generazione Automatica
//!
//! con labels dinamiche, riducendo il codice boilerplate e garantendo
//!
//! Caratteristiche:
//! - Macro per generazione metriche con labels
//! - Labels dinamici per moltiplicare punti dati
//! - Validazione automatica tipi

#[macro_export]
macro_rules! create_gauge_with_labels {
    (
        $name:expr, $help:expr, $($label_name:ident),*
    ) => {
        {
            let opts = prometheus::Opts::new($name, $help);
            let gauge = prometheus::Gauge::with_opts(opts);
            gauge
        }
    };
}

#[macro_export]
macro_rules! create_counter_with_labels {
    (
        $name:expr, $help:expr, $($label_name:ident),*
    ) => {
        {
            let opts = prometheus::Opts::new($name, $help);
            let counter = prometheus::Counter::with_opts(opts);
            counter
        }
    };
}

#[macro_export]
macro_rules! create_histogram_with_labels {
    (
        $name:expr, $help:expr, $buckets:expr, $($label_name:ident),*
    ) => {
        {
            let opts = prometheus::HistogramOpts::new($name, $help)
                .buckets($buckets.clone());
            let histogram = prometheus::Histogram::with_opts(opts);
            histogram
        }
    };
}

/// Macro per registrare metriche con labels dinamiche
#[macro_export]
macro_rules! register_labeled_metric {
    (
        $registry:expr, $metric:ident, $($label_name:ident = $label_value:expr),*
    ) => {
        {
            let labeled_metric = $metric.with_label_values(&[
                $(stringify!($label_name).to_string(), $label_value.to_string()),*
            ]);
            // Registration only fails on duplicate metric name, which is a
            // programmer error in the calling crate.
            $registry
                .register(Box::new(labeled_metric))
                .expect(concat!(
                    "metric registration failed: duplicate labeled metric `",
                    stringify!($metric),
                    "`"
                ));
        }
    };
}

/// Macro per aggiornare metriche con labels dinamiche
#[macro_export]
macro_rules! update_labeled_gauge {
    (
        $metric:ident, $value:expr, $($label_name:ident = $label_value:expr),*
    ) => {
        {
            let labeled_metric = $metric.with_label_values(&[
                $(stringify!($label_name).to_string(), $label_value.to_string()),*
            ]);
            labeled_metric.set($value);
        }
    };
}

/// Macro per incrementare metriche con labels dinamiche
#[macro_export]
macro_rules! increment_labeled_counter {
    (
        $metric:ident, $($label_name:ident = $label_value:expr),*
    ) => {
        {
            let labeled_metric = $metric.with_label_values(&[
                $(stringify!($label_name).to_string(), $label_value.to_string()),*
            ]);
            labeled_metric.inc();
        }
    };
}

/// Macro per osservare metriche con labels dinamiche
#[macro_export]
macro_rules! observe_labeled_histogram {
    (
        $metric:ident, $value:expr, $($label_name:ident = $label_value:expr),*
    ) => {
        {
            let labeled_metric = $metric.with_label_values(&[
                $(stringify!($label_name).to_string(), $label_value.to_string()),*
            ]);
            labeled_metric.observe($value);
        }
    };
}

#[macro_export]
macro_rules! generate_category_metrics {
    (
        $category_name:ident, {
            $(
                $metric_name:ident: $metric_type:ident {
                    name: $metric_name_str:expr,
                    help: $metric_help:expr,
                    buckets: $metric_buckets:expr,
                    labels: [$($label_name:ident),*]
                }
            ),* $(,)?
        }
    ) => {
        pub struct $category_name {
            $(
                pub $metric_name: prometheus::$metric_type,
            )*
        }

        impl $category_name {
            pub fn new(registry: &prometheus::Registry) -> Self {
                Self {
                    $(
                        $metric_name: generate_metric!($metric_type, $metric_name_str, $metric_help, $metric_buckets),
                    )*
                }
            }

            $(
                pub fn $metric_name(&self) -> &prometheus::$metric_type {
                    &$self.$metric_name
                }
            )*
        }
    };
}

/// Macro helper per generare metriche in base al tipo
#[macro_export]
macro_rules! generate_metric {
    (Gauge, $name:expr, $help:expr, $buckets:expr) => {
        prometheus::Gauge::with_opts(prometheus::Opts::new($name, $help))
    };
    (Counter, $name:expr, $help:expr, $buckets:expr) => {
        prometheus::Counter::with_opts(prometheus::Opts::new($name, $help))
    };
    (Histogram, $name:expr, $help:expr, $buckets:expr) => {
        prometheus::Histogram::with_opts(
            prometheus::HistogramOpts::new($name, $help).buckets($buckets.clone())
        )
    };
}

/// Macro per generare metriche blockchain con labels
#[macro_export]
macro_rules! generate_blockchain_metrics {
    () => {
        generate_category_metrics!(BlockchainMetrics, {
            block_height: Gauge {
                name: "savitri_block_height",
                help: "Current blockchain height",
                buckets: vec![],
                labels: []
            },
            finality_time_seconds: Histogram {
                name: "savitri_finality_time_seconds",
                help: "Time to finality in seconds",
                buckets: vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
                labels: []
            },
            consensus_rounds_total: Counter {
                name: "savitri_consensus_rounds_total",
                help: "Total consensus rounds",
                buckets: vec![],
                labels: [round_type, success]
            },
            active_validators: Gauge {
                name: "savitri_active_validators",
                help: "Number of active validators",
                buckets: vec![],
                labels: [validator_type]
            },
            temporary_forks_total: Counter {
                name: "savitri_temporary_forks_total",
                help: "Total temporary forks",
                buckets: vec![],
                labels: [fork_reason]
            },
            block_time_seconds: Histogram {
                name: "savitri_block_time_seconds",
                help: "Block time in seconds",
                buckets: vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
                labels: [shard_id, block_type]
            },
            block_proposals_total: Counter {
                name: "savitri_block_proposals_total",
                help: "Total block proposals",
                buckets: vec![],
                labels: [proposer_id, shard_id]
            },
            votes_received_total: Counter {
                name: "savitri_votes_received_total",
                help: "Total votes received",
                buckets: vec![],
                labels: [vote_type, validator_id]
            },
            certificates_generated_total: Counter {
                name: "savitri_certificates_generated_total",
                help: "Total certificates generated",
                buckets: vec![],
                labels: [round_number, shard_id]
            },
            byzantine_behavior_score: Gauge {
                name: "savitri_byzantine_behavior_score",
                help: "Byzantine behavior score",
                buckets: vec![],
                labels: [peer_id, behavior_type]
            }
        });
    };
}

/// Macro per generare metriche mempool con labels
#[macro_export]
macro_rules! generate_mempool_metrics {
    () => {
        generate_category_metrics!(MempoolMetrics, {
            size: Gauge {
                name: "savitri_mempool_size",
                help: "Mempool size (number of transactions)",
                buckets: vec![],
                labels: [priority, shard_id]
            },
            size_bytes: Gauge {
                name: "savitri_mempool_size_bytes",
                help: "Mempool size in bytes",
                buckets: vec![],
                labels: [priority, shard_id]
            },
            admission_rate: Gauge {
                name: "savitri_mempool_admission_rate",
                help: "Mempool admission rate",
                buckets: vec![],
                labels: [priority, shard_id]
            },
            rejection_rate: Gauge {
                name: "savitri_mempool_rejection_rate",
                help: "Mempool rejection rate",
                buckets: vec![],
                labels: [rejection_reason, shard_id]
            },
            avg_queue_time_seconds: Gauge {
                name: "savitri_mempool_avg_queue_time_seconds",
                help: "Average queue time in seconds",
                buckets: vec![],
                labels: [priority, shard_id]
            },
            eviction_rate: Gauge {
                name: "savitri_mempool_eviction_rate",
                help: "Mempool eviction rate",
                buckets: vec![],
                labels: [eviction_reason, shard_id]
            },
            transactions_added_total: Counter {
                name: "savitri_mempool_transactions_added_total",
                help: "Total transactions added to mempool",
                buckets: vec![],
                labels: [tx_type, priority, shard_id]
            },
            transactions_removed_total: Counter {
                name: "savitri_mempool_transactions_removed_total",
                help: "Total transactions removed from mempool",
                buckets: vec![],
                labels: [removal_reason, shard_id]
            },
            transactions_expired_total: Counter {
                name: "savitri_mempool_transactions_expired_total",
                help: "Total expired transactions",
                buckets: vec![],
                labels: [shard_id]
            },
            duplicate_rejections_total: Counter {
                name: "savitri_mempool_duplicate_rejections_total",
                help: "Total duplicate rejections",
                buckets: vec![],
                labels: [shard_id]
            },
            fee_distribution: Histogram {
                name: "savitri_mempool_fee_distribution",
                help: "Transaction fee distribution",
                buckets: vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0, 100.0],
                labels: [tx_type, priority, shard_id]
            },
            wait_time_seconds: Histogram {
                name: "savitri_mempool_wait_time_seconds",
                help: "Transaction wait time in mempool",
                buckets: vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0],
                labels: [tx_type, priority, shard_id]
            }
        });
    };
}

/// Macro per generare metriche network con labels
#[macro_export]
macro_rules! generate_network_metrics {
    () => {
        generate_category_metrics!(NetworkMetrics, {
            connected_peers: Gauge {
                name: "savitri_connected_peers",
                help: "Number of connected peers",
                buckets: vec![],
                labels: [peer_type, connection_state]
            },
            peer_latency_ms: Histogram {
                name: "savitri_peer_latency_ms",
                help: "Peer latency in milliseconds",
                buckets: vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
                labels: [peer_id, peer_type, protocol]
            },
            gossip_propagation_time_ms: Histogram {
                name: "savitri_gossip_propagation_time_ms",
                help: "Gossip propagation time in milliseconds",
                buckets: vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
                labels: [message_type, shard_id]
            },
            protocol_bandwidth_bytes_per_sec: Gauge {
                name: "savitri_protocol_bandwidth_bytes_per_sec",
                help: "Protocol bandwidth in bytes per second",
                buckets: vec![],
                labels: [protocol, direction]
            },
            connection_errors_total: Counter {
                name: "savitri_connection_errors_total",
                help: "Total connection errors",
                buckets: vec![],
                labels: [error_type, peer_id, protocol]
            },
            discovery_rate: Gauge {
                name: "savitri_discovery_rate",
                help: "Peer discovery rate",
                buckets: vec![],
                labels: [discovery_method, peer_type]
            },
            gossip_messages_sent_total: Counter {
                name: "savitri_gossip_messages_sent_total",
                help: "Total gossip messages sent",
                buckets: vec![],
                labels: [message_type, shard_id]
            },
            gossip_messages_received_total: Counter {
                name: "savitri_gossip_messages_received_total",
                help: "Total gossip messages received",
                buckets: vec![],
                labels: [message_type, shard_id]
            },
            p2p_bytes_sent_total: Counter {
                name: "savitri_p2p_bytes_sent_total",
                help: "Total P2P bytes sent",
                buckets: vec![],
                labels: [protocol, peer_id]
            },
            p2p_bytes_received_total: Counter {
                name: "savitri_p2p_bytes_received_total",
                help: "Total P2P bytes received",
                buckets: vec![],
                labels: [protocol, peer_id]
            },
            handshakes_completed_total: Counter {
                name: "savitri_handshakes_completed_total",
                help: "Total handshakes completed",
                buckets: vec![],
                labels: [handshake_type, peer_type]
            },
            handshakes_failed_total: Counter {
                name: "savitri_handshakes_failed_total",
                help: "Total handshakes failed",
                buckets: vec![],
                labels: [handshake_type, failure_reason, peer_type]
            }
        });
    };
}

/// Macro per generare metriche execution con labels
#[macro_export]
macro_rules! generate_execution_metrics {
    () => {
        generate_category_metrics!(ExecutionMetrics, {
            tx_execution_time_ms: Histogram {
                name: "savitri_tx_execution_time_ms",
                help: "Transaction execution time in milliseconds",
                buckets: vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
                labels: [tx_type, shard_id, complexity]
            },
            gas_used: Histogram {
                name: "savitri_gas_used",
                help: "Gas used",
                buckets: vec![1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0],
                labels: [tx_type, shard_id]
            },
            contract_deployments_total: Counter {
                name: "savitri_contract_deployments_total",
                help: "Total contract deployments",
                buckets: vec![],
                labels: [shard_id, contract_type]
            },
            contract_calls_total: Counter {
                name: "savitri_contract_calls_total",
                help: "Total contract calls",
                buckets: vec![],
                labels: [shard_id, contract_address, function_name]
            },
            dag_dependency_depth: Histogram {
                name: "savitri_dag_dependency_depth",
                help: "DAG dependency depth",
                buckets: vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0],
                labels: [shard_id, transaction_type]
            },
            execution_concurrency: Gauge {
                name: "savitri_execution_concurrency",
                help: "Execution concurrency",
                buckets: vec![],
                labels: [shard_id, execution_type]
            },
            transactions_per_second: Gauge {
                name: "savitri_transactions_per_second",
                help: "Transactions per second",
                buckets: vec![],
                labels: [shard_id, tx_type]
            },
            execution_errors_total: Counter {
                name: "savitri_execution_errors_total",
                help: "Total execution errors",
                buckets: vec![],
                labels: [error_type, shard_id, tx_type]
            },
            dag_throughput_ops_per_sec: Gauge {
                name: "savitri_dag_throughput_ops_per_sec",
                help: "DAG throughput operations per second",
                buckets: vec![],
                labels: [shard_id, operation_type]
            },
            parallel_execution_utilization: Gauge {
                name: "savitri_parallel_execution_utilization",
                help: "Parallel execution utilization",
                buckets: vec![],
                labels: [shard_id, worker_type]
            }
        });
    };
}

/// Macro per generare metriche system con labels
#[macro_export]
macro_rules! generate_system_metrics {
    () => {
        generate_category_metrics!(SystemMetrics, {
            disk_read_iops: Gauge {
                name: "savitri_disk_read_iops",
                help: "Disk read IOPS",
                buckets: vec![],
                labels: [storage_type, device]
            },
            disk_write_iops: Gauge {
                name: "savitri_disk_write_iops",
                help: "Disk write IOPS",
                buckets: vec![],
                labels: [storage_type, device]
            },
            storage_read_latency_ms: Histogram {
                name: "savitri_storage_read_latency_ms",
                help: "Storage read latency in milliseconds",
                buckets: vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0],
                labels: [storage_type, operation_type]
            },
            storage_write_latency_ms: Histogram {
                name: "savitri_storage_write_latency_ms",
                help: "Storage write latency in milliseconds",
                buckets: vec![0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0],
                labels: [storage_type, operation_type]
            },
            cpu_usage_percent: Gauge {
                name: "savitri_cpu_usage_percent",
                help: "CPU usage percentage",
                buckets: vec![],
                labels: [core_id, process_type]
            },
            memory_usage_mb: Gauge {
                name: "savitri_memory_usage_mb",
                help: "Memory usage in MB",
                buckets: vec![],
                labels: [memory_type, process_type]
            },
            active_threads: Gauge {
                name: "savitri_active_threads",
                help: "Active threads",
                buckets: vec![],
                labels: [thread_type, process_type]
            },
            file_descriptors_used: Gauge {
                name: "savitri_file_descriptors_used",
                help: "File descriptors used",
                buckets: vec![],
                labels: [process_type, file_type]
            },
            system_load_average: Gauge {
                name: "savitri_system_load_average",
                help: "System load average",
                buckets: vec![],
                labels: [load_period]
            },
            swap_usage_mb: Gauge {
                name: "savitri_swap_usage_mb",
                help: "Swap usage in MB",
                buckets: vec![],
                labels: [swap_type]
            },
            network_io_bytes_per_sec: Gauge {
                name: "savitri_network_io_bytes_per_sec",
                help: "Network I/O bytes per second",
                buckets: vec![],
                labels: [interface, direction]
            }
        });
    };
}

/// Macro per generare metriche security con labels
#[macro_export]
macro_rules! generate_security_metrics {
    () => {
        generate_category_metrics!(SecurityMetrics, {
            failed_auth_attempts_total: Counter {
                name: "savitri_failed_auth_attempts_total",
                help: "Total failed authentication attempts",
                buckets: vec![],
                labels: [auth_method, failure_reason, peer_id]
            },
            suspicious_connections_total: Counter {
                name: "savitri_suspicious_connections_total",
                help: "Total suspicious connections",
                buckets: vec![],
                labels: [suspicion_type, peer_id, protocol]
            },
            spam_detected_total: Counter {
                name: "savitri_spam_detected_total",
                help: "Total spam detected",
                buckets: vec![],
                labels: [spam_type, source, severity]
            },
            byzantine_behavior_score: Gauge {
                name: "savitri_byzantine_behavior_score",
                help: "Byzantine behavior score",
                buckets: vec![],
                labels: [peer_id, behavior_type, detection_method]
            },
            invalid_signatures_total: Counter {
                name: "savitri_invalid_signatures_total",
                help: "Total invalid signatures",
                buckets: vec![],
                labels: [signature_type, peer_id, context]
            },
            revoked_certificates_total: Counter {
                name: "savitri_revoked_certificates_total",
                help: "Total revoked certificates",
                buckets: vec![],
                labels: [certificate_type, peer_id, revocation_reason]
            },
            rate_limiting_active: Gauge {
                name: "savitri_rate_limiting_active",
                help: "Active rate limiting",
                buckets: vec![],
                labels: [limit_type, protocol, peer_id]
            },
            firewall_blocks_total: Counter {
                name: "savitri_firewall_blocks_total",
                help: "Total firewall blocks",
                buckets: vec![],
                labels: [block_reason, source_ip, protocol]
            },
            security_alerts_total: Counter {
                name: "savitri_security_alerts_total",
                help: "Total security alerts",
                buckets: vec![],
                labels: [alert_type, severity, source]
            },
            ddos_detection_score: Gauge {
                name: "savitri_ddos_detection_score",
                help: "DDoS detection score",
                buckets: vec![],
                labels: [attack_vector, source, severity]
            }
        });
    };
}

/// Macro per generare metriche tokenomics con labels
#[macro_export]
macro_rules! generate_tokenomics_metrics {
    () => {
        generate_category_metrics!(TokenomicsMetrics, {
            dynamic_supply: Gauge {
                name: "savitri_dynamic_supply",
                help: "Dynamic token supply",
                buckets: vec![],
                labels: [token_type, supply_source]
            },
            burn_rate_per_hour: Gauge {
                name: "savitri_burn_rate_per_hour",
                help: "Token burn rate per hour",
                buckets: vec![],
                labels: [burn_mechanism, token_type]
            },
            accumulated_fees: Gauge {
                name: "savitri_accumulated_fees",
                help: "Accumulated fees",
                buckets: vec![],
                labels: [fee_type, token_type]
            },
            staking_ratio: Gauge {
                name: "savitri_staking_ratio",
                help: "Staking ratio",
                buckets: vec![],
                labels: [staking_pool, token_type]
            },
            circulating_supply: Gauge {
                name: "savitri_circulating_supply",
                help: "Circulating supply",
                buckets: vec![],
                labels: [token_type, distribution_type]
            },
            burned_tokens: Gauge {
                name: "savitri_burned_tokens",
                help: "Burned tokens",
                buckets: vec![],
                labels: [burn_mechanism, token_type]
            },
            locked_tokens: Gauge {
                name: "savitri_locked_tokens",
                help: "Locked tokens",
                buckets: vec![],
                labels: [lock_type, token_type]
            },
            annual_inflation_rate: Gauge {
                name: "savitri_annual_inflation_rate",
                help: "Annual inflation rate",
                buckets: vec![],
                labels: [inflation_source, token_type]
            },
            market_cap: Gauge {
                name: "savitri_market_cap",
                help: "Market cap",
                buckets: vec![],
                labels: [currency, market_type]
            },
            transaction_volume_total: Counter {
                name: "savitri_transaction_volume_total",
                help: "Total transaction volume",
                buckets: vec![],
                labels: [transaction_type, token_type]
            }
        });
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Registry;

    #[test]
    fn test_macro_gauge_creation() {
        let gauge = create_gauge_with_labels!("test_gauge", "Test gauge", label1, label2);
        assert_eq!(gauge.name(), "test_gauge");
    }

    #[test]
    fn test_macro_counter_creation() {
        let counter = create_counter_with_labels!("test_counter", "Test counter", label1);
        counter.inc();
        assert_eq!(counter.get(), 1.0);
    }

    #[test]
    fn test_macro_histogram_creation() {
        let histogram = create_histogram_with_labels!(
            "test_histogram",
            "Test histogram",
            vec![1.0, 5.0, 10.0],
            label1
        );
        histogram.observe(5.0);
        // Non possiamo testare direttamente il valore dell'istogramma
    }

    #[test]
    fn test_generate_blockchain_metrics() {
        let registry = Registry::new();
        let metrics = generate_blockchain_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.block_height.name(), "savitri_block_height");
        assert_eq!(metrics.finality_time_seconds.name(), "savitri_finality_time_seconds");
    }

    #[test]
    fn test_generate_mempool_metrics() {
        let registry = Registry::new();
        let metrics = generate_mempool_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.size.name(), "savitri_mempool_size");
        assert_eq!(metrics.fee_distribution.name(), "savitri_mempool_fee_distribution");
    }

    #[test]
    fn test_generate_network_metrics() {
        let registry = Registry::new();
        let metrics = generate_network_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.connected_peers.name(), "savitri_connected_peers");
        assert_eq!(metrics.peer_latency_ms.name(), "savitri_peer_latency_ms");
    }

    #[test]
    fn test_generate_execution_metrics() {
        let registry = Registry::new();
        let metrics = generate_execution_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.tx_execution_time_ms.name(), "savitri_tx_execution_time_ms");
        assert_eq!(metrics.gas_used.name(), "savitri_gas_used");
    }

    #[test]
    fn test_generate_system_metrics() {
        let registry = Registry::new();
        let metrics = generate_system_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.cpu_usage_percent.name(), "savitri_cpu_usage_percent");
        assert_eq!(metrics.memory_usage_mb.name(), "savitri_memory_usage_mb");
    }

    #[test]
    fn test_generate_security_metrics() {
        let registry = Registry::new();
        let metrics = generate_security_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.failed_auth_attempts_total.name(), "savitri_failed_auth_attempts_total");
        assert_eq!(metrics.byzantine_behavior_score.name(), "savitri_byzantine_behavior_score");
    }

    #[test]
    fn test_generate_tokenomics_metrics() {
        let registry = Registry::new();
        let metrics = generate_tokenomics_metrics!();
        
        // Check che le metriche siano state create
        assert_eq!(metrics.dynamic_supply.name(), "savitri_dynamic_supply");
        assert_eq!(metrics.burn_rate_per_hour.name(), "savitri_burn_rate_per_hour");
    }
}
