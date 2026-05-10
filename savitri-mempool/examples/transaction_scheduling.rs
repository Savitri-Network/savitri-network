//! Example: Transaction Scheduling
//!
//! This example demonstrates transaction scheduling with the ExecutionDispatcher.

use savitri_mempool::executor::{AdaptiveWeightsConfig, DispatcherConfig, ExecutionDispatcher};
use savitri_mempool::mempool::{MempoolTx, TxClass, TxHandle};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Transaction Scheduling Example");

    // 1. Create dispatcher with default configuration
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);

    // 2. Create sample transactions
    let mut mempool_txs = Vec::new();
    for i in 0..10 {
        mempool_txs.push(MempoolTx {
            sender_id: (i % 3) as u32,
            nonce: i as u64,
            fee: 1_000_000 + (i * 100_000) as u64,
            tx_handle: TxHandle(i),
            class: if i % 2 == 0 {
                TxClass::Financial
            } else {
                TxClass::IoTData
            },
            stream_nonce: None,
            inserted: Instant::now(),
            tx_hash: None,
            sender_address: vec![(i % 256) as u8; 32],
            signature_hash: [0u8; 64],
            gas_limit: 1_000_000,
            max_fee: 2_000_000,
            received_at: Instant::now(),
        });
    }

    println!("✅ Created {} transactions", mempool_txs.len());

    // 3. Demonstrate scheduling
    println!("\n📋 Transaction Ordering:");
    println!("   - Fee-based prioritization");
    println!("   - Class-aware scheduling");
    println!("   - Sender fairness");

    // 4. Show adaptive weights configuration
    println!("\n⚙️ Adaptive Weights Configuration:");
    let adaptive_config = AdaptiveWeightsConfig::default();
    println!("   - Base fee weight: {}", adaptive_config.base_fee_weight);
    println!(
        "   - Base class weight: {}",
        adaptive_config.base_class_weight
    );
    println!("   - Adaptation rate: {}", adaptive_config.adaptation_rate);

    Ok(())
}
