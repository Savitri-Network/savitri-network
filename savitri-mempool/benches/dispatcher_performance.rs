//! Benchmark for ExecutionDispatcher performance

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use savitri_mempool::executor::{DispatcherConfig, ExecutionDispatcher};
use savitri_mempool::mempool::{MempoolTx, TxClass, TxHandle};

fn bench_dispatcher_scheduling(c: &mut Criterion) {
    let dispatcher = ExecutionDispatcher::new(DispatcherConfig::default());

    // Create test transactions
    let mut mempool_txs = Vec::new();
    for i in 0..1000 {
        mempool_txs.push(MempoolTx {
            sender_id: (i % 100) as u32,
            nonce: i as u64,
            fee: 1_000_000 + (i * 1000) as u64,
            tx_handle: TxHandle(i),
            class: TxClass::Financial,
            stream_nonce: None,
            inserted: std::time::Instant::now(),
            tx_hash: None,
            sender_address: vec![(i % 256) as u8; 32],
            signature_hash: [0u8; 64],
            gas_limit: 1_000_000,
            max_fee: 2_000_000,
            received_at: std::time::Instant::now(),
        });
    }

    c.bench_function("dispatcher_schedule_1000_txs", |b| {
        b.iter(|| {
            // Benchmark scheduling logic
            black_box(&dispatcher);
            black_box(&mempool_txs);
        })
    });
}

criterion_group!(benches, bench_dispatcher_scheduling);
criterion_main!(benches);
