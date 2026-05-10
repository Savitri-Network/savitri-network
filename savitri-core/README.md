# Savitri Core Library

The foundational library for the Savitri Network blockchain ecosystem.

## Overview

Savitri Core is a lightweight, fast-compiling Rust library that provides the essential building blocks for the Savitri Network blockchain. It focuses on core types, cryptographic primitives, utilities, and metrics without external dependencies on networking, storage, or consensus layers.

## Features

- **🚀 Fast Compilation**: Sub-30 second build time
- **🔒 Minimal Dependencies**: Only essential crypto and serialization libraries
- **🛡️ Secure**: Extensive input validation and error handling
- **📊 Portable**: Cross-platform compatibility
- **📚 Well-Documented**: Comprehensive documentation and examples
- **🧪 Test Coverage**: >90% test coverage with comprehensive test suite

## Architecture

The library is organized into four main modules:

### Core (`src/core/`)
- **Types**: Basic blockchain data structures (Account, Transaction, FeeLimits)
- **Slot Scheduler**: Deterministic slot scheduling and leader rotation
- **Monolith**: Basic monolith data structures and utilities

### Crypto (`src/crypto/`)
- **Signatures**: Ed25519 digital signatures with security-level awareness
- **Hashing**: SHA-256, SHA-512, BLAKE3 hash functions
- **Keys**: Key generation, storage, and management
- **Encryption**: Symmetric encryption and password-based key derivation

### Utils (`src/utils/`)
- **Convert**: Type conversion utilities
- **Time**: Time handling and formatting
- **Math**: Mathematical functions and fixed-point arithmetic
- **Bincode**: Unified serialization configuration

### Metrics (`src/metrics/`)
- **Provider**: Lightweight metrics collection system
- **Exporter**: Prometheus format export
- **Manifest**: Metrics manifest generation

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
savitri-core = "0.1.0"
```

### Basic Usage

```rust
use savitri_core::*;

fn main() {
    // Create an account
    let mut account = Account::default();
    account.credit(1000).unwrap();
    
    // Generate a keypair
    let keypair = generate_keypair();
    
    // Sign and verify a message
    let message = b"Hello, Savitri!";
    let signature = sign(message, &keypair);
    let public_key = keypair.verifying_key();
    
    assert!(verify(message, &signature, &public_key));
    
    // Use slot scheduler
    let config = SlotSchedulerConfig {
        slot_duration: std::time::Duration::from_millis(1000),
        validators: vec!["validator1".to_string(), "validator2".to_string()],
        local_id: "validator1".to_string(),
        slot_base_ms: Some(1000000),
    };
    
    let scheduler = SlotScheduler::new(config).unwrap();
    let slot_info = scheduler.current_slot_info().unwrap();
    
    println!("Current slot: {}", slot_info.slot);
    println!("Role: {:?}", slot_info.role);
}
```

## API Reference

### Core Types

#### Account
```rust
let mut account = Account::default();
account.credit(1000).unwrap();
account.debit(500).unwrap();
account.increment_nonce().unwrap();
```

#### Transaction
```rust
let tx = Transaction {
    from: "alice".to_string(),
    to: "bob".to_string(),
    amount: 100,
};
```

#### FeeLimits
```rust
let limits = FeeLimits::default();
let custom = FeeLimits::new(1000, 10000);
```

### Cryptography

#### Signatures
```rust
let keypair = generate_keypair();
let signature = sign(message, &keypair);
let verified = verify(message, &signature, &keypair.verifying_key());
```

#### Hashing
```rust
let hash = sha256(data);
let merkle_root = merkle_root(&hashes);
let domain_hash = hash_with_domain("DOMAIN", data);
```

#### Key Management
```rust
let keypair = KeyPair::new();
let mut manager = KeyManager::new(MemoryKeyStorage::new());
let key_id = manager.generate_key(None).unwrap();
```

### Slot Scheduler

#### Configuration
```rust
let config = SlotSchedulerConfig {
    slot_duration: Duration::from_millis(1000),
    validators: validators,
    local_id: local_id,
    slot_base_ms: Some(1000000),
};
```

#### Usage
```rust
let scheduler = SlotScheduler::new(config)?;
let slot_info = scheduler.current_slot_info()?;
```

### Monolith

#### Policy
```rust
let policy = MonolithPolicy::new(1000)
    .with_epoch_length(Some(100))
    .with_retention(30)
    .with_max_size_bytes(500_000_000);
```

#### Header Creation
```rust
let header = MonolithHeader::new(
    prev_monolith_id,
    headers_commit,
    state_commit,
    proof_commit,
    exec_height,
    window_start,
    epoch_id,
    producer,
);
```

### Utilities

#### Type Conversions
```rust
let hex = bytes_to_hex(&bytes);
let bytes = hex_to_bytes(&hex)?;
let timestamp = str_to_u64("123456")?;
```

#### Time Functions
```rust
let now = now_timestamp();
let formatted = format_duration(duration);
let slot = slot::current_slot(1000, 1000000);
```

#### Math Functions
```rust
let fp = fixed_point::from_f64(1.5);
let mean = stats::mean(&values);
let is_prime = crypto::is_prime(17);
```

### Metrics

#### Provider
```rust
let mut provider = MetricsProvider::new(config);
provider.register_metric("counter".to_string(), 42.0, MetricType::Counter);
provider.increment_counter("counter".to_string(), 8.0);
```

#### Exporter
```rust
let exporter = PrometheusExporter::new(config);
let prometheus_format = exporter.export_metrics(&metrics);
```

## Configuration

### Environment Variables

- `SAVITRI_METRICS_ENABLED`: Enable/disable metrics (default: false)
- `SAVITRI_METRICS_PORT`: Metrics port (default: 9090)
- `SAVITRI_METRICS_MAX`: Maximum metrics count (default: 1000)
- `SAVITRI_SECURITY_LEVEL`: Security level for crypto (development/testing/staging/production)

### Constants

- `DEFAULT_SLOT_DURATION_MS`: 1000ms
- `DEFAULT_BLOCK_TIME_MS`: 500ms
- `DEFAULT_CONSENSUS_TIMEOUT_MS`: 5000ms
- `DEFAULT_NETWORK_TIMEOUT_MS`: 30000ms
- `TOKEN_DECIMALS`: 18 (like Ethereum)
- `WEI_PER_TOKEN`: 10^18

## Testing

Run tests with:

```bash
cargo test --lib
cargo test --tests
```

The library includes comprehensive tests covering:
- Unit tests for all modules
- Integration tests for cross-module functionality
- Performance benchmarks for critical operations
- Security tests for cryptographic functions

## Development

### Building from Source

```bash
git clone https://github.com/savitri-network/savitri-core
cd savitri-core
cargo build --release
```

### Running Examples

```bash
cargo run --example basic_usage
cargo run --example slot_scheduler
cargo run --example cryptography
```

### Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_basic_types

# Run benchmarks
cargo bench
```

## Dependencies

### Core Dependencies
- `serde`: Serialization framework
- `serde_json`: JSON serialization
- `sha2`, `sha3`: Cryptographic hash functions
- `rand`: Random number generation
- `ed25519-dalek`: Ed25519 digital signatures
- `blake3`: Modern hash function
- `chrono`: Time handling
- `hex`: Hex encoding/decoding
- `anyhow`: Error handling

### Dev Dependencies
- `tokio`: Async runtime (for tests only)
- `tempfile`: Temporary file creation (for tests only)
- `criterion`: Benchmarking (for benchmarks only)

## Security

### Security Features

- **Ed25519 Digital Signatures**: Industry-standard cryptographic signatures
- **Secure Random Generation**: Cryptographically secure random number generation
- **Input Validation**: Extensive validation for all inputs
- **Memory Safety**: No unsafe code except in well-audited cryptographic operations
- **Error Handling**: Comprehensive error handling with detailed messages

### Security Levels

The library supports different security levels via the `SAVITRI_SECURITY_LEVEL` environment variable:

- `development`: Fast mock verification for development
- `testing`: Real crypto with relaxed logging
- `staging`: Real crypto with full logging
- `production`: Real crypto with security-focused logging (default)

## Performance

### Benchmarks

The library includes comprehensive benchmarks for:

- **Cryptographic Operations**: Signature generation and verification
- **Hash Functions**: SHA-256, SHA-512, BLAKE3 performance
- **Slot Scheduling**: Leader rotation and slot calculation
- **Serialization**: Fast binary serialization
- **Math Operations**: Fixed-point arithmetic and statistics

### Performance Characteristics

- **Compilation Time**: < 30 seconds on typical development machine
- **Binary Size**: ~2MB when optimized
- **Memory Usage**: Minimal memory footprint
- **CPU Usage**: Low CPU overhead for most operations

## Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Code Style

- Follow Rust standard style guidelines
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting
- Document all public APIs
- Write comprehensive tests

### Testing Requirements

- All new features must include tests
- Test coverage should be >90%
- Performance-critical code should have benchmarks
- Security-critical code should have security tests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.

## Support

- **Documentation**: [docs.savitri.network](https://docs.savitrinetwork.com)
- **Issues**: [GitHub Issues](https://github.com/savitri-network/savitri-core/issues)
- **Discussions**: [GitHub Discussions](https://github.com/savitri-network/savitri-core/discussions)

## Related Projects

- [savitri-node](https://github.com/savitri-network/savitri-node): Full node implementation
- [savitri-lightnode](https://github.com/savitri-network/savitri-lightnode): Light node implementation
- [savitri-masternode](https://github.com/savitri-network/savitri-masternode): Master node implementation
- [savitri-testnet](https://github.com/savitri-network/savitri-testnet): Test network configuration

---

**Built with ❤️ for the Savitri Network community**
