//! Telemetry Module — Real Prometheus metrics endpoint on 0.0.0.0:9898
//!
//! Registers all 133 Savitri metrics at startup so they appear in every scrape.
//! System metrics (CPU, memory, disk, network, process) are refreshed every 5 s
//! via sysinfo.  All other metrics are updated in-place by the subsystems that
//! own them (P2P, mempool, consensus, …) using `metrics::gauge!` / `counter!`.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;
use sysinfo::{Disks, Networks, Pid, System};
use tracing::info;
#[cfg(target_os = "windows")]
use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
};

static PROMETHEUS_HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();
pub fn metrics_listen_addr() -> String {
    let port = std::env::var("METRICS_PORT").unwrap_or_else(|_| "9898".to_string());
    format!("0.0.0.0:{}", port)
}

// ---------------------------------------------------------------------------
// Public API (called from main.rs)
// ---------------------------------------------------------------------------

/// Installs the Prometheus recorder + HTTP listener (sync — no .await).
pub fn init_metrics() -> Result<(), anyhow::Error> {
    let addr: SocketAddr = metrics_listen_addr()
        .parse()
        .context("invalid metrics listen address")?;
    let builder = PrometheusBuilder::new().with_http_listener(addr);
    let (recorder, exporter) = builder
        .build()
        .context("failed to build Prometheus metrics recorder/exporter")?;
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder)
        .context("failed to install Prometheus metrics recorder")?;
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| anyhow!("metrics recorder already initialized"))?;

    tokio::spawn(exporter);

    register_all_metrics();

    tracing::info!(listen = %addr, "metrics endpoint ready");
    Ok(())
}

/// Optional accessor for the Prometheus handle.
pub fn metrics_handle() -> Option<&'static PrometheusHandle> {
    PROMETHEUS_HANDLE.get()
}

/// Spawns a background task that refreshes the 30 SYSTEM metrics every 5 s.
/// Returns a watch sender — send `true` to request graceful shutdown.
pub async fn update_system_metrics_periodically(
    startup_time: Instant,
) -> tokio::sync::watch::Sender<bool> {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let mut system = System::new();

    // Initial refresh
    system.refresh_cpu_usage();
    system.refresh_memory();

    let pid = Pid::from(std::process::id() as usize);
    system.refresh_processes();

    // Short pause so sysinfo has a delta for CPU usage
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(5));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    update_system_metrics(&mut system, pid, startup_time);
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("System metrics task shutting down gracefully");
                        break;
                    }
                }
            }
        }
    });

    shutdown_tx
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------

fn register_all_metrics() {
    // ── 2.1 SYSTEM (30) ──────────────────────────────────────────────
    // CPU (6)
    gauge!("system_cpu_usage_percent").set(0.0);
    gauge!("system_cpu_cores").set(0.0);
    gauge!("system_cpu_frequency_mhz").set(0.0);
    gauge!("system_cpu_load_avg_1m").set(0.0);
    gauge!("system_cpu_load_avg_5m").set(0.0);
    gauge!("system_cpu_load_avg_15m").set(0.0);
    // Memory (5)
    gauge!("system_memory_total_bytes").set(0.0);
    gauge!("system_memory_used_bytes").set(0.0);
    gauge!("system_memory_free_bytes").set(0.0);
    gauge!("system_memory_available_bytes").set(0.0);
    gauge!("system_memory_usage_percent").set(0.0);
    // Disk (8)
    gauge!("system_disk_total_bytes").set(0.0);
    gauge!("system_disk_used_bytes").set(0.0);
    gauge!("system_disk_free_bytes").set(0.0);
    gauge!("system_disk_usage_percent").set(0.0);
    counter!("system_disk_read_bytes_total").absolute(0);
    counter!("system_disk_write_bytes_total").absolute(0);
    counter!("system_disk_read_ops_total").absolute(0);
    counter!("system_disk_write_ops_total").absolute(0);
    // Network OS-level (5)
    counter!("system_network_bytes_sent_total").absolute(0);
    counter!("system_network_bytes_received_total").absolute(0);
    counter!("system_network_packets_sent_total").absolute(0);
    counter!("system_network_packets_received_total").absolute(0);
    counter!("system_network_errors_total").absolute(0);
    // Process (6 — 5 + fds)
    gauge!("process_uptime_seconds").set(0.0);
    gauge!("process_threads").set(0.0);
    gauge!("process_memory_rss_bytes").set(0.0);
    gauge!("process_memory_vms_bytes").set(0.0);
    gauge!("process_fds").set(0.0);

    // ── 2.2 NETWORK P2P (19) ────────────────────────────────────────
    gauge!("savitri_peers_connected").set(0.0);
    gauge!("savitri_active_peers").set(0.0);
    gauge!("savitri_total_peers").set(0.0);
    gauge!("p2p_peers_connected").set(0.0);
    gauge!("p2p_active_peers").set(0.0);
    counter!("p2p_peers_disconnected_total").absolute(0);
    counter!("p2p_connection_attempts_total").absolute(0);
    counter!("p2p_messages_sent_total").absolute(0);
    counter!("p2p_messages_received_total").absolute(0);
    counter!("p2p_bytes_sent_total").absolute(0);
    counter!("p2p_bytes_received_total").absolute(0);
    gauge!("p2p_latency_ms").set(0.0);
    // Gossip (4)
    counter!("gossip_messages_sent_total").absolute(0);
    counter!("gossip_messages_received_total").absolute(0);
    counter!("gossip_duplicate_messages_total").absolute(0);
    counter!("gossip_validation_failures_total").absolute(0);
    // Broadcast (3)
    counter!("broadcast_transactions_total").absolute(0);
    counter!("broadcast_blocks_total").absolute(0);
    counter!("broadcast_heartbeats_total").absolute(0);
    // DHT (4 — bonus, part of P2P)
    gauge!("dht_routing_table_size").set(0.0);
    counter!("dht_queries_total").absolute(0);
    counter!("dht_query_successes_total").absolute(0);
    counter!("dht_query_failures_total").absolute(0);
    counter!("peer_server_register_success_total").absolute(0);
    counter!("peer_server_register_failure_total").absolute(0);
    counter!("peer_server_heartbeat_success_total").absolute(0);
    counter!("peer_server_heartbeat_failure_total").absolute(0);
    counter!("peer_server_fetch_success_total").absolute(0);
    counter!("peer_server_fetch_failure_total").absolute(0);
    counter!("peer_server_peers_received_total").absolute(0);
    counter!("peer_server_dials_attempted_total").absolute(0);

    // ── 2.3 TRANSACTION (6) ─────────────────────────────────────────
    gauge!("transactions_pending").set(0.0);
    counter!("transactions_confirmed_total").absolute(0);
    counter!("transactions_failed_total").absolute(0);
    counter!("transactions_size_bytes_total").absolute(0);
    gauge!("transaction_throughput_tps").set(0.0);
    gauge!("transaction_latency_ms").set(0.0);

    // ── 2.4 BLOCK (7) ───────────────────────────────────────────────
    gauge!("block_height").set(0.0);
    gauge!("block_time_seconds").set(0.0);
    counter!("blocks_produced_total").absolute(0);
    counter!("blocks_received_total").absolute(0);
    gauge!("block_size_bytes_avg").set(0.0);
    gauge!("block_transactions_count_avg").set(0.0);
    gauge!("chain_state_size_bytes").set(0.0);

    // ── 2.5 MEMPOOL (5) ─────────────────────────────────────────────
    gauge!("mempool_size").set(0.0);
    gauge!("mempool_size_bytes").set(0.0);
    counter!("mempool_transactions_added_total").absolute(0);
    counter!("mempool_transactions_removed_total").absolute(0);
    counter!("mempool_transactions_evicted_total").absolute(0);

    // ── 2.6 CONSENSUS (9) ───────────────────────────────────────────
    counter!("consensus_rounds_total").absolute(0);
    counter!("consensus_proposals_total").absolute(0);
    counter!("consensus_votes_total").absolute(0);
    counter!("consensus_quorum_achieved_total").absolute(0);
    gauge!("consensus_finality_time_ms").set(0.0);
    gauge!("consensus_participation_rate").set(0.0);
    gauge!("consensus_validators_count").set(0.0);
    gauge!("consensus_active_validators_count").set(0.0);
    gauge!("byzantine_score").set(0.0);

    // ── 2.7 EXECUTION DAG (11) ──────────────────────────────────────
    gauge!("dag_nodes_total").set(0.0);
    gauge!("dag_edges_total").set(0.0);
    gauge!("dag_parallelism_degree").set(0.0);
    gauge!("dag_critical_path_length").set(0.0);
    gauge!("dag_dependency_depth_avg").set(0.0);
    gauge!("dag_dependency_depth_max").set(0.0);
    gauge!("dag_execution_stages").set(0.0);
    gauge!("dag_nodes_count").set(0.0);
    gauge!("dag_edges_count").set(0.0);
    gauge!("execution_time_ms").set(0.0);
    gauge!("gas_efficiency").set(0.0);

    // ── 2.8 STORAGE / RocksDB (10) ──────────────────────────────────
    counter!("rocksdb_reads_total").absolute(0);
    counter!("rocksdb_writes_total").absolute(0);
    counter!("rocksdb_compactions_total").absolute(0);
    gauge!("rocksdb_compaction_time_ms").set(0.0);
    counter!("rocksdb_block_cache_hits").absolute(0);
    counter!("rocksdb_block_cache_misses").absolute(0);
    gauge!("state_tree_size_bytes").set(0.0);
    gauge!("accounts_count").set(0.0);
    gauge!("contracts_count").set(0.0);
    gauge!("storage_used_bytes").set(0.0);

    // ── 2.9 SECURITY (16) ───────────────────────────────────────────
    gauge!("byzantine_behavior_score").set(0.0);
    counter!("integrity_checks_total").absolute(0);
    counter!("integrity_checks_failed_total").absolute(0);
    counter!("proof_of_uptime_validations_total").absolute(0);
    counter!("proof_of_uptime_validations_failed_total").absolute(0);
    counter!("security_events_total").absolute(0);
    counter!("security_critical_events_total").absolute(0);
    counter!("signature_verifications_total").absolute(0);
    counter!("signature_verifications_failed_total").absolute(0);
    counter!("hash_operations_total").absolute(0);
    counter!("connection_attempts_total").absolute(0);
    counter!("connection_rejections_total").absolute(0);
    counter!("ddos_attacks_detected_total").absolute(0);
    counter!("fork_detection_events_total").absolute(0);
    counter!("double_spend_attempts_total").absolute(0);
    // consensus_participation_rate already registered in CONSENSUS

    // ── 2.10 FEE (4) ────────────────────────────────────────────────
    counter!("fee_collected_total").absolute(0);
    counter!("fee_distributed_total").absolute(0);
    counter!("fee_burned_total").absolute(0);
    gauge!("fee_per_transaction_avg").set(0.0);

    // ── 2.11 GOVERNANCE — Not active in testnet V0.1.0.
    // Governance metrics will be enabled when Savitri-contracts/governance
    // is integrated into the lightnode runtime (planned for mainnet).
    // #[cfg(feature = "governance")]
    // {
    //     counter!("governance_votes_total").absolute(0);
    //     ... (11 metrics)
    // }

    // ── 2.12 TOKENOMICS (5) ─────────────────────────────────────────
    gauge!("token_supply_total").set(0.0);
    gauge!("token_supply_circulating").set(0.0);
    counter!("token_halving_events_total").absolute(0);
    gauge!("token_distribution_holders_count").set(0.0);
    gauge!("token_inflation_rate").set(0.0);

    counter!("routing_route_entry_total").absolute(0);
    counter!("routing_decisions_total", "decision" => "local").absolute(0);
    counter!("routing_decisions_total", "decision" => "forward").absolute(0);
    counter!("routing_cross_group_rx_total").absolute(0);
    metrics::histogram!("routing_cross_group_rx_bytes");

    counter!("consensus_cert_match_total", "outcome" => "match").absolute(0);
    counter!("consensus_cert_match_total", "outcome" => "miss").absolute(0);
    counter!("consensus_cert_lock_busy_total").absolute(0);
    counter!("consensus_cert_spawn_total").absolute(0);
    counter!("consensus_speculative_exec_total", "result" => "ok").absolute(0);
    counter!("consensus_speculative_exec_total", "result" => "fail").absolute(0);
    counter!("consensus_commit_scheduler_admit_total").absolute(0);
    counter!("consensus_commit_scheduler_drain_total", "result" => "empty").absolute(0);
    counter!("consensus_commit_scheduler_drain_total", "result" => "hit").absolute(0);
    metrics::histogram!("consensus_commit_scheduler_ready_size");

    // ── 2.15 MEMPOOL DRAIN PIPELINE (Tier 8 — was DRAIN/FILTER/NOFILTER_CTR) ─
    metrics::histogram!("mempool_drain_request");
    metrics::histogram!("mempool_drain_total_before");
    metrics::histogram!("mempool_drain_yield");
    counter!("mempool_drain_kept_total", "via" => "rpc_accepted").absolute(0);
    counter!("mempool_drain_kept_total", "via" => "is_local").absolute(0);
    counter!("mempool_drain_dropped_remote_total").absolute(0);
    counter!("mempool_drain_no_filter_total").absolute(0);

    // ── 2.16 RPC CONSUMER (Tier 8 — was REJECT_CTR in savitri-rpc/lib.rs) ──
    counter!("rpc_tx_consumer_rejected_total", "reason" => "_init").absolute(0);
    counter!("rpc_tx_consumer_accepted_total").absolute(0);

    counter!("proposer_state_drift_total").absolute(0);
    counter!("proposer_state_match_total").absolute(0);
}

// ---------------------------------------------------------------------------
// Periodic sysinfo refresh (30 SYSTEM metrics)
// ---------------------------------------------------------------------------

fn update_system_metrics(system: &mut System, pid: Pid, startup_time: Instant) {
    system.refresh_cpu_usage();
    system.refresh_memory();
    system.refresh_processes();

    // ── CPU (6) ──────────────────────────────────────────────────────
    let cpu = system.global_cpu_info();
    gauge!("system_cpu_usage_percent").set(cpu.cpu_usage() as f64);
    gauge!("system_cpu_cores").set(system.cpus().len() as f64);
    gauge!("system_cpu_frequency_mhz").set(cpu.frequency() as f64);

    // Load averages (Linux/macOS; returns 0.0 on Windows)
    let load = System::load_average();
    gauge!("system_cpu_load_avg_1m").set(load.one);
    gauge!("system_cpu_load_avg_5m").set(load.five);
    gauge!("system_cpu_load_avg_15m").set(load.fifteen);

    // ── Memory (5) ───────────────────────────────────────────────────
    gauge!("system_memory_total_bytes").set(system.total_memory() as f64);
    gauge!("system_memory_used_bytes").set(system.used_memory() as f64);
    gauge!("system_memory_free_bytes").set(system.free_memory() as f64);
    gauge!("system_memory_available_bytes").set(system.available_memory() as f64);

    let mem_pct = if system.total_memory() > 0 {
        (system.used_memory() as f64 / system.total_memory() as f64) * 100.0
    } else {
        0.0
    };
    gauge!("system_memory_usage_percent").set(mem_pct);

    // ── Disk (8) ─────────────────────────────────────────────────────
    let disks = Disks::new_with_refreshed_list();
    if let Some(disk) = disks.list().first() {
        let total = disk.total_space();
        let avail = disk.available_space();
        let used = total.saturating_sub(avail);
        let pct = if total > 0 {
            (used as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        gauge!("system_disk_total_bytes").set(total as f64);
        gauge!("system_disk_used_bytes").set(used as f64);
        gauge!("system_disk_free_bytes").set(avail as f64);
        gauge!("system_disk_usage_percent").set(pct);
    }
    // Per-process disk I/O (read/write bytes as proxy for ops)
    if let Some(proc) = system.process(pid) {
        let du = proc.disk_usage();
        counter!("system_disk_read_bytes_total").absolute(du.total_read_bytes);
        counter!("system_disk_write_bytes_total").absolute(du.total_written_bytes);
        // Use bytes as proxy for ops — sysinfo doesn't expose op count
        counter!("system_disk_read_ops_total").absolute(du.total_read_bytes / 4096);
        counter!("system_disk_write_ops_total").absolute(du.total_written_bytes / 4096);
    }

    // ── Network OS-level (5) ─────────────────────────────────────────
    let networks = Networks::new_with_refreshed_list();
    let mut bytes_sent = 0u64;
    let mut bytes_recv = 0u64;
    let mut pkts_sent = 0u64;
    let mut pkts_recv = 0u64;
    let mut errors = 0u64;
    for (_name, iface) in networks.list() {
        bytes_sent += iface.total_transmitted();
        bytes_recv += iface.total_received();
        pkts_sent += iface.total_packets_transmitted();
        pkts_recv += iface.total_packets_received();
        errors += iface.total_errors_on_received() + iface.total_errors_on_transmitted();
    }
    counter!("system_network_bytes_sent_total").absolute(bytes_sent);
    counter!("system_network_bytes_received_total").absolute(bytes_recv);
    counter!("system_network_packets_sent_total").absolute(pkts_sent);
    counter!("system_network_packets_received_total").absolute(pkts_recv);
    counter!("system_network_errors_total").absolute(errors);

    // ── Process (5) ──────────────────────────────────────────────────
    gauge!("process_uptime_seconds").set(startup_time.elapsed().as_secs() as f64);
    if let Some(proc) = system.process(pid) {
        let thread_count = current_process_thread_count().unwrap_or(0);
        gauge!("process_threads").set(thread_count as f64);
        gauge!("process_memory_rss_bytes").set(proc.memory() as f64);
        gauge!("process_memory_vms_bytes").set(proc.virtual_memory() as f64);
        // FDs not directly available on Windows — 0.0 fallback
        gauge!("process_fds").set(0.0);
    }
}

#[cfg(target_os = "windows")]
fn current_process_thread_count() -> Option<usize> {
    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            return None;
        }

        let mut entry: THREADENTRY32 = std::mem::zeroed();
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;

        let target_pid = std::process::id();
        let mut count = 0usize;

        if Thread32First(snapshot, &mut entry) == 0 {
            CloseHandle(snapshot);
            return None;
        }

        loop {
            if entry.th32OwnerProcessID == target_pid {
                count += 1;
            }

            if Thread32Next(snapshot, &mut entry) == 0 {
                break;
            }
        }

        CloseHandle(snapshot);
        Some(count)
    }
}

#[cfg(not(target_os = "windows"))]
fn current_process_thread_count() -> Option<usize> {
    None
}
