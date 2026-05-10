//! Benchmark for ScoreCache efficiency

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use savitri_mempool::executor::ScoreCache;
use savitri_mempool::mempool::TxClass;

fn bench_cache_hit_rate(c: &mut Criterion) {
    let mut cache = ScoreCache::new();

    // Pre-populate cache
    for i in 0..1000 {
        cache.cache_score((i * 1000) as u64, TxClass::Financial, (i as f64) * 0.1);
    }

    c.bench_function("cache_get_1000_hits", |b| {
        b.iter(|| {
            for i in 0..1000 {
                black_box(cache.get_cached_score((i * 1000) as u64, TxClass::Financial));
            }
        })
    });
}

fn bench_cache_misses(c: &mut Criterion) {
    let cache = ScoreCache::new();

    c.bench_function("cache_get_1000_misses", |b| {
        b.iter(|| {
            for i in 0..1000 {
                black_box(cache.get_cached_score((i * 1000) as u64, TxClass::Financial));
            }
        })
    });
}

criterion_group!(benches, bench_cache_hit_rate, bench_cache_misses);
criterion_main!(benches);
