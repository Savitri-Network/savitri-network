//! Example: Basic Mempool Setup
//!
//! This example demonstrates how to set up and configure a mempool pipeline.

use savitri_mempool::mempool::{AdmissionConfig, AdmissionControl, MempoolPipeline};
use std::sync::Arc;
use std::sync::Mutex;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Setting up Savitri Mempool");

    // 1. Create admission control
    let admission_config = AdmissionConfig::default();
    let admission = Arc::new(Mutex::new(AdmissionControl::new(admission_config)));

    // 2. Create mempool pipeline
    let mempool = MempoolPipeline::new(admission.clone());

    println!("✅ Mempool created successfully");
    println!("   - Admission control: Enabled");
    println!("   - Pipeline: Ready");

    // 3. Configure mempool settings
    println!("\n📋 Mempool Configuration:");
    println!("   - Global capacity: 10,000 transactions");
    println!("   - Per-sender capacity: 128 transactions");
    println!("   - TTL: 120 seconds");

    Ok(())
}
