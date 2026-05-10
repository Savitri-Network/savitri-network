# Savitri Mempool

High-performance mempool and transaction execution engine for the Savitri blockchain network.

## Overview

Savitri Mempool provides a production-ready transaction pool and execution engine with:

- **SIMD Optimizations**: Vectorized transaction processing for maximum throughput
- **Adaptive Scheduling**: Dynamic transaction ordering based on fee, priority, and dependencies
- **Atomic Nonce Resolution**: Thread-safe nonce conflict resolution for parallel execution
- **Score Cache System**: O(1) score caching to avoid redundant calculations
- **Performance Monitoring**: Comprehensive metrics and benchmarking tools

## Architecture

### Core Components

- **Mempool**: Transaction pool with class-aware architecture (pool, scheduler, dispatcher, scoring, prevalidation)
- **Executor**: Transaction execution engine with parallel processing capabilities
- **Score Cache**: High-performance caching system for transaction scores
- **Dispatcher**: Adaptive transaction scheduling with configurable weights

## Features

- `simd`: Enable SIMD optimizations for vectorized processing
- `cache`: Enable score cache system
- `adaptive_weights`: Enable adaptive weight calculation for transaction scheduling

## Quick Start

```rust
use savitri_mempool::{MempoolPipeline, ExecutionDispatcher, DispatcherConfig};

// Create mempool pipeline
let mempool = MempoolPipeline::new(config);

// Create execution dispatcher
let dispatcher = ExecutionDispatcher::new(DispatcherConfig::default());
```

## Documentation

- [Transaction Ordering](docs/TRANSACTION_ORDERING.md) - How transactions are ordered and scheduled
- [Score Cache](docs/SCORE_CACHE.md) - Score caching system architecture
- [Adaptive Weights](docs/ADAPTIVE_WEIGHTS.md) - Adaptive weight calculation for scheduling
- [Performance Tuning](docs/PERFORMANCE_TUNING.md) - Optimization guide
- [Atomic Nonce](docs/ATOMIC_NONCE.md) - Atomic nonce resolution system

## Examples

- [Mempool Setup](examples/mempool_setup.rs) - Basic mempool configuration
- [Transaction Scheduling](examples/transaction_scheduling.rs) - Transaction scheduling examples

## Benchmarks

Run benchmarks with:

```bash
cargo bench
```

Available benchmarks:
- `dispatcher_performance`: Execution dispatcher performance
- `cache_efficiency`: Score cache hit/miss rates

## Testing

Run tests with:

```bash
cargo test
```

Test coverage includes:
- Transaction ordering tests
- Atomic nonce resolution tests
- Performance scheduling tests
- Score cache tests (23 tests)

## License

MIT
