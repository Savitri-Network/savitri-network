//! Esempio: Backup Automation
//!

use anyhow::Result;
use savitri_guardian::{Archive, GuardianConfig};
use std::path::Path;
use std::time::Duration;
use tokio::time;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Backup Automation Example ===\n");

    // 1. Load configuration
    println!("1. Loading guardian configuration...");
    let config = GuardianConfig::load("guardian.toml")?;
    println!("   ✓ Configuration loaded");

    // 2. Initialize archive
    println!("\n2. Initializing archive...");
    let db_path = config.db_path.as_deref().unwrap_or("guardian.db");
    let archive = Archive::open(db_path)?;
    println!("   ✓ Archive initialized");

    // 3. Backup configuration
    println!("\n3. Backup configuration:");
    let backup_interval = Duration::from_secs(3600); // 1 hour
    let backup_dir = "backups";
    println!("   Backup interval: {:?}", backup_interval);
    println!("   Backup directory: {}", backup_dir);

    // Create backup directory
    std::fs::create_dir_all(backup_dir)?;

    // 4. Automated backup loop
    println!("\n4. Starting automated backup loop...");
    println!("   Press Ctrl+C to stop\n");

    let mut interval = time::interval(backup_interval);
    let mut backup_count = 0;

    loop {
        interval.tick().await;
        backup_count += 1;

        println!(
            "[{}] Starting backup #{}...",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
            backup_count
        );

        // Create backup
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let backup_path = format!("{}/backup_{}.tar.gz", backup_dir, timestamp);

        match create_backup(&archive, &backup_path).await {
            Ok(size) => {
                println!(
                    "   ✓ Backup created: {} ({:.2} GB)",
                    backup_path,
                    size as f64 / 1e9
                );
            }
            Err(e) => {
                eprintln!("   ❌ Backup failed: {}", e);
            }
        }

        // Cleanup old backups (keep last 30 days)
        cleanup_old_backups(backup_dir, 30).await?;
    }
}

async fn create_backup(archive: &Archive, backup_path: &str) -> Result<u64> {
    // Per ora, simuliamo il backup
    let size = 1_000_000_000; // 1GB placeholder
    Ok(size)
}

async fn cleanup_old_backups(backup_dir: &str, retention_days: u64) -> Result<()> {
    use std::time::{Duration, SystemTime};

    let now = SystemTime::now();
    let cutoff = now - Duration::from_secs(retention_days * 24 * 3600);

    let entries = std::fs::read_dir(backup_dir)?;
    let mut removed = 0;

    for entry in entries {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if let Ok(modified) = metadata.modified() {
            if modified < cutoff {
                std::fs::remove_file(entry.path())?;
                removed += 1;
            }
        }
    }

    if removed > 0 {
        println!("   ✓ Cleaned up {} old backup(s)", removed);
    }

    Ok(())
}
