# savitri-guardian

Archive and backup node for Savitri Network, providing full blockchain data archival, storage management, and network monitoring without consensus participation.

## Overview

`savitri-guardian` is a non-validating node that connects to the Savitri Network to archive all blocks, transactions, and monoliths. It operates in observer mode: it subscribes to gossipsub topics and receives finalized data from masternodes and lightnodes, but does not participate in consensus, elections, or block production.

The guardian node is intended for infrastructure operators who need a complete historical record of the blockchain. It uses RocksDB for persistent storage with configurable compaction, and provides rate-limited data serving to other nodes requesting historical blocks or proofs.

A built-in metrics collector tracks uptime, storage usage, and request counts. Disk usage monitoring can trigger alerts when available space drops below a configurable threshold.

## Features

- **Full Archive Storage**: Persists all blocks, transactions, receipts, and monoliths to RocksDB
- **P2P Observer Mode**: Connects to the gossipsub mesh to receive finalized blocks without consensus participation
- **Storage Compaction**: RocksDB with snappy, lz4, and zlib compression support
- **Disk Monitoring**: Configurable disk space alerts via `sysinfo`
- **Rate-Limited Serving**: Configurable limits on history block requests, proof bytes, and requests per minute
- **Metrics Collection**: Internal counters and gauges for operational monitoring
- **Prometheus Export**: Optional metrics exposure via Prometheus HTTP endpoint
- **Mempool Tracking**: Maintains a local mempool (up to 10,000 transactions) for observing pending transactions
- **TOML Configuration**: File-based configuration with sensible defaults

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `archive` | Yes | RocksDB persistent storage |
| `monitoring` | Yes | Prometheus metrics, system monitoring via sysinfo |

## Usage

### CLI

```bash
# Start with default configuration
savitri-guardian

# Start with config file
savitri-guardian --config guardian.toml

# Specify database path and port
savitri-guardian --db-path /data/guardian.db --listen-port 4101
```

### Configuration (TOML)

```toml
db_path = "guardian.db"
listen_port = 4101
network_key_path = "guardian-identity.key"
bootstrap_peers = [
    "12D3KooW...@/ip4/1.2.3.4/tcp/4021"
]

[rate_limits]
max_history_blocks = 1000
max_history_span = 86400
max_history_reply_bytes = 10485760
max_proof_bytes = 1048576
requests_per_minute = 60

[monitoring]
disk_alert_threshold_gb = 100.0
metrics_interval_secs = 60
```

If no config file is provided, the guardian starts with default settings.

## Building

```bash
# Default (archive + monitoring)
cargo build --release -p savitri-guardian

# Without monitoring (smaller binary)
cargo build --release -p savitri-guardian --no-default-features --features archive
```

Requires MSVC build tools on Windows (for RocksDB).

## Testing

```bash
cargo test -p savitri-guardian
```

## Architecture

```
src/
  main.rs       -- CLI parsing (clap), config loading, P2P setup, main event loop
  config.rs     -- GuardianConfig, RateLimitConfig, MonitoringConfig (TOML deserialization)
  archive.rs    -- Archive storage operations, block/monolith persistence
  serve.rs      -- ArchiveConfig, data serving with rate limiting
  telemetry.rs  -- Metrics collection, Prometheus export, system monitoring
```

### Main Event Loop

The `main.rs` file contains the full node implementation:

1. Parses CLI arguments and loads TOML configuration
2. Generates or loads a libp2p Ed25519 identity keypair
3. Constructs a libp2p swarm with TCP + Noise + Yamux transport and gossipsub
4. Subscribes to block and transaction gossipsub topics
5. Connects to bootstrap peers
6. Runs the event loop, persisting received blocks and transactions to RocksDB

### Key Components

- `GuardianConfig`: TOML-based configuration with rate limits and monitoring settings
- `MetricsCollector`: Tracks counters (blocks received, requests served) and gauges (disk usage, peer count)
- `Mempool`: Observer-mode mempool capped at 10,000 entries for tracking pending transactions
- `ArchiveConfig`: Controls data serving behavior (max blocks per request, proof size limits)

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `db_path` | `guardian.db` | RocksDB database path |
| `listen_port` | 4101 | P2P TCP listen port |
| `network_key_path` | (auto-generated) | Ed25519 identity key file |
| `rate_limits.max_history_blocks` | 1000 | Max blocks per history request |
| `rate_limits.requests_per_minute` | 60 | Rate limit for incoming requests |
| `monitoring.disk_alert_threshold_gb` | 100 | Disk space alert threshold in GB |
| `monitoring.metrics_interval_secs` | 60 | Metrics collection interval |

## System Requirements

- Rust >= 1.82
- Disk: 500GB minimum, 1TB+ recommended for full archive
- The guardian stores all historical data, so storage requirements grow linearly with chain height

## Dependencies

- `savitri-core` for types and crypto
- `savitri-p2p` for networking
- `savitri-storage` for storage abstractions
- `libp2p` 0.55 (gossipsub, TCP, Noise, Yamux, DNS)
- `rocksdb` (optional, via `archive` feature) with snappy/lz4/zlib compression
- `clap` for CLI argument parsing
- `sysinfo` (optional, via `monitoring` feature) for system monitoring
- `metrics` + `metrics-exporter-prometheus` (optional) for metrics export

## License

Apache-2.0
