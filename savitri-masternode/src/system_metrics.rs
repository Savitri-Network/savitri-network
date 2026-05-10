//! Sistema di raccolta metriche di sistema per il masternode.
//!

use metrics::{counter, gauge};
use std::time::{Duration, Instant};
use sysinfo::{System, Pid};

/// Handle globale per il sistema
static SYSTEM: once_cell::sync::OnceCell<System> = once_cell::sync::OnceCell::new();

/// Timestamp di avvio of the processo per calcolare l'uptime
static START_TIME: once_cell::sync::Lazy<Instant> = once_cell::sync::Lazy::new(Instant::now);

/// Initializes the sistema di raccolta metriche
pub fn init() {
    let mut system = System::new();
    system.refresh_all();
    if let Err(_) = SYSTEM.set(system) {
        tracing::warn!("System metrics already initialized, using existing instance");
    }
}

pub fn update_metrics() {
    let system = match SYSTEM.get() {
        Some(system) => system,
        None => {
            tracing::warn!("System metrics not initialized, attempting to initialize");
            init();
            return match SYSTEM.get() {
                Some(system) => {
                    tracing::info!("System metrics initialized successfully");
                    system
                }
                None => {
                    tracing::error!("Failed to initialize system metrics, skipping update");
                    return;
                }
            };
        }
    };
    let mut system = system.clone();
    
    // Refresh of the sistema per ottenere dati aggiornati
    system.refresh_all();
    
    let cpu_usage = system.global_cpu_info().cpu_usage() as f64;
    let cpu_count = system.cpus().len() as u32;
    
    gauge!("system_cpu_usage_percent").set(cpu_usage);
    gauge!("system_cpu_cores").set(cpu_count as f64);

    let total_memory = system.total_memory();
    let used_memory = system.used_memory();
    let free_memory = system.free_memory();
    let available_memory = system.available_memory();
    let memory_usage_percent = if total_memory > 0 {
        (used_memory as f64 / total_memory as f64) * 100.0
    } else {
        0.0
    };

    gauge!("system_memory_total_bytes").set(total_memory as f64);
    gauge!("system_memory_used_bytes").set(used_memory as f64);
    gauge!("system_memory_free_bytes").set(free_memory as f64);
    gauge!("system_memory_available_bytes").set(available_memory as f64);
    gauge!("system_memory_usage_percent").set(memory_usage_percent);

    if let Some(disk) = system.disks().first() {
        let total_disk = disk.total_space();
        let available_disk = disk.available_space();
        let used_disk = total_disk - available_disk;
        let disk_usage_percent = if total_disk > 0 {
            (used_disk as f64 / total_disk as f64) * 100.0
        } else {
            0.0
        };

        gauge!("system_disk_total_bytes").set(total_disk as f64);
        gauge!("system_disk_used_bytes").set(used_disk as f64);
        gauge!("system_disk_free_bytes").set(available_disk as f64);
        gauge!("system_disk_usage_percent").set(disk_usage_percent);
    }

    let pid = Pid::from(std::process::id());
    if let Some(process) = system.process(pid) {
        let uptime_seconds = START_TIME.elapsed().as_secs();
        let threads = process.num_threads().unwrap_or(0);
        let memory_rss = process.memory(); // sysinfo 0.29 returns già bytes
        let memory_vms = process.virtual_memory(); // sysinfo 0.29 returns già bytes

        gauge!("process_uptime_seconds").set(uptime_seconds as f64);
        gauge!("process_threads").set(threads as f64);
        gauge!("process_memory_rss_bytes").set(memory_rss as f64);
        gauge!("process_memory_vms_bytes").set(memory_vms as f64);
    }

    let mut total_bytes_sent = 0u64;
    let mut total_bytes_received = 0u64;

    for (_, interface) in system.networks() {
        total_bytes_sent += interface.transmitted();
        total_bytes_received += interface.received();
    }

    counter!("system_network_bytes_sent").absolute(total_bytes_sent);
    counter!("system_network_bytes_received").absolute(total_bytes_received);
}

pub async fn start_metrics_collector_task(interval_secs: u64) {
    let interval = Duration::from_secs(interval_secs);
    
    loop {
        tokio::time::sleep(interval).await;
        update_metrics();
    }
}
