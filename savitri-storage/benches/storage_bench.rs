//! Storage Benchmarks for Savitri Storage Layer
//!
//! This module contains comprehensive benchmarks for testing the performance
//! of the Savitri storage layer under various conditions.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use savitri_storage::{Storage, StorageConfig};
use std::time::Duration;
use tempfile::TempDir;

fn bench_storage_basic_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = Storage::new(temp_dir.path()).unwrap();

    c.bench_function("put", |b| {
        b.iter(|| {
            let key = format!("key_{}", rand::random::<u32>());
            let value = vec![1u8; 1024];
            storage.put(black_box(&key), black_box(&value)).unwrap();
        });
    });

    c.bench_function("get", |b| {
        // Pre-populate with data
        for i in 0..1000 {
            let key = format!("bench_key_{}", i);
            let value = vec![i as u8; 1024];
            storage.put(&key, value).unwrap();
        }

        b.iter(|| {
            let key = format!("bench_key_{}", rand::random::<u32>() % 1000);
            storage.get(black_box(&key)).unwrap();
        });
    });

    c.bench_function("delete", |b| {
        b.iter(|| {
            let key = format!("del_key_{}", rand::random::<u32>());
            storage.put(&key, vec![1, 2, 3]).unwrap();
            storage.delete(black_box(&key)).unwrap();
        });
    });
}

fn bench_storage_batch_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = Storage::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("batch_operations");

    for batch_size in [10, 100, 1000, 10000].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch_put", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let mut batch = storage.batch();
                    for i in 0..batch_size {
                        let key = format!("batch_key_{}", i);
                        let value = vec![i as u8; 256];
                        batch.put(&key, value);
                    }
                    batch.commit().unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_storage_compression(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = Storage::new(temp_dir.path()).unwrap();

    let mut group = c.benchmark_group("compression");

    for data_size in [1024, 10240, 102400, 1024000].iter() {
        group.bench_with_input(
            BenchmarkId::new("compress_put", data_size),
            data_size,
            |b, &data_size| {
                let data = vec![42u8; data_size];
                b.iter(|| {
                    let key = format!("compress_key_{}", rand::random::<u32>());
                    storage
                        .put_compressed(black_box(&key), black_box(&data))
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let storage = Storage::new(temp_dir.path()).unwrap();

    c.bench_function("concurrent_writes", |b| {
        b.iter(|| {
            let storage = &storage;
            let handles: Vec<_> = (0..8)
                .map(|thread_id| {
                    std::thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("thread_{}_key_{}", thread_id, i);
                            let value = vec![thread_id as u8; 512];
                            storage.put(&key, value).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    c.bench_function("concurrent_reads", |b| {
        // Pre-populate data
        for thread_id in 0..8 {
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = vec![thread_id as u8; 512];
                storage.put(&key, value).unwrap();
            }
        }

        b.iter(|| {
            let storage = &storage;
            let handles: Vec<_> = (0..8)
                .map(|thread_id| {
                    std::thread::spawn(move || {
                        for i in 0..100 {
                            let key = format!("thread_{}_key_{}", thread_id, i);
                            storage.get(&key).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });
}

criterion_group!(
    benches,
    bench_storage_basic_operations,
    bench_storage_batch_operations,
    bench_storage_compression,
    bench_concurrent_operations
);
criterion_main!(benches);
