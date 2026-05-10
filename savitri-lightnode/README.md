# savitri-lightnode

Light node implementation for Savitri Network, optimized for both desktop and mobile platforms with full P2P consensus participation, block production, and transaction processing.

## Overview

`savitri-lightnode` is the primary block-producing node in the Savitri Network. Despite its name, it is a full participant in the consensus protocol: it joins dynamically formed groups, runs Proof-of-Unity (PoU) scoring, participates in elections, proposes blocks, and relays transactions via gossipsub. Masternodes coordinate groups and finalize blocks via BFT, but lightnodes do the actual block production.

The node supports block pipelining, allowing up to 8 blocks to be proposed ahead of the finalized height without waiting for masternode BFT certification. This achieves approximately 15 TPS on an 18-node localhost test network. The transaction pipeline handles nonce tracking, mempool draining with fair batching, and stale transaction purging after block commits.

Two platform targets are supported: desktop (full P2P, system metrics, Prometheus export) and mobile (lightweight P2P, reduced dependencies). Storage can be either persistent (RocksDB) or in-memory, controlled by configuration.

## Features

- **Block Production**: Proposer election via PoU scoring, block assembly from mempool, pipelining up to `MAX_PIPELINE_DEPTH=8` blocks ahead
- **PoU Consensus**: Intra-group election, latency probing, availability tracking, integrity scoring, reputation management
- **P2P Networking**: libp2p 0.55 with gossipsub, Kademlia DHT, request-response protocol, NAT traversal (QUIC, Relay, DCUtR, AutoNAT)
- **Transaction Pipeline**: SIMD-optimized mempool (AVX2/FMA on x86_64, NEON on ARM), fair batch draining, nonce gap handling
- **Proposer Rotation**: Automatic step-down after 50 blocks to prevent single-node monopoly
- **Fee Distribution**: Transaction fee processing and distribution
- **Adaptive Latency**: Real-time latency measurement and adaptive scoring
- **Sharding Support**: Module structure for future sharding
- **Optional RPC**: JSON-RPC 2.0 API via `savitri-rpc` (enabled with `rpc` feature)
- **Telemetry**: Prometheus metrics export and system resource monitoring

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `desktop` | Yes | Full P2P, metrics, sysinfo, Prometheus export |
| `rocksdb` | Yes | Persistent storage via RocksDB |
| `mobile` | No | Lightweight P2P, reduced dependencies |
| `rpc` | No | Enable JSON-RPC 2.0 API server |
| `lightweight-p2p` | No | Minimal P2P for constrained environments |
| `full-p2p` | No | Full P2P with metrics and system monitoring |
| `test_simulated_latency` | No | Simulated latency for localhost testing |

## Usage

### CLI

```bash
# Start with CLI arguments
savitri-lightnode \
  --listen-port 4001 \
  --tx-interval-secs 2 \
  --block-interval-secs 10 \
  --max-block-txs 32 \
  --bootstrap <PEER_ID>@/ip4/127.0.0.1/tcp/4002

# Start with config file
savitri-lightnode --config config/lightnode.toml

# Second node for local mesh
savitri-lightnode \
  --db lightnode2.db \
  --network-key-path lightnode-network-2.key \
  --producer-key-path lightnode-producer-2.key \
  --listen-port 4002 \
  --bootstrap <PEER_ID>@/ip4/127.0.0.1/tcp/4001
```

### Configuration (TOML)

Storage persistence is controlled by the config file:
- `memory_only` (default `true`): If `true`, uses in-memory storage. If `false`, uses RocksDB.
- `db_path` (optional): Database directory path when `memory_only = false`. Defaults to a path derived from the listen port.

### Binaries

| Binary | Description |
|--------|-------------|
| `savitri-lightnode` | Main lightnode binary |
| `derive-pubkey` | Utility to derive public key from private key file |

## Building

```bash
# Default (desktop + RocksDB)
cargo build --release -p savitri-lightnode

# Without RocksDB (in-memory only)
cargo build --release -p savitri-lightnode --no-default-features --features desktop

# Mobile build
cargo build --release -p savitri-lightnode --no-default-features --features mobile

# With RPC server
cargo build --release -p savitri-lightnode --features rpc
```

RocksDB requires MSVC (cl.exe) on Windows. Use `--no-default-features` to build without it.

## Testing

```bash
cargo test -p savitri-lightnode
```

For network integration tests, use the scripts in `local_test/`:
```bash
# PowerShell: 5 masternodes + 10 lightnodes
./local_test/run_5mn_10ln.ps1
```

## Architecture

```
src/
  main.rs                -- CLI parsing (clap), node startup, module wiring
  config.rs              -- TOML configuration parsing
  logging.rs             -- Structured logging with flag-based message categories
  lib.rs                 -- Library exports for masternode integration
  signer.rs              -- Ed25519 key loading/generation
  storage.rs             -- Storage trait abstraction (RocksDB or in-memory)
  telemetry.rs           -- Prometheus metrics and system monitoring
  resource.rs            -- Resource usage tracking
  tx.rs                  -- Transaction creation, signing, nonce management
  proposer.rs            -- Block proposer logic, pipelining
  mempool_integration.rs -- Mempool draining and fair batch assembly
  availability.rs        -- Peer availability tracking
  integrity.rs           -- Node integrity scoring
  adaptive_latency.rs    -- Adaptive latency measurement
  latency_service.rs     -- Latency probe service
  compatibility.rs       -- Version compatibility checks
  core/
    mod.rs, tx.rs        -- Core transaction types
  fee/                   -- Fee calculation and distribution
  sharding/              -- Sharding module structure
  p2p/
    mod.rs               -- P2P module re-exports
    network.rs           -- Main event loop (swarm events, command processing)
    network_tasks.rs     -- Background network tasks
    transport.rs         -- libp2p transport configuration (TCP, QUIC, Noise, Yamux)
    bootstrap.rs         -- Bootstrap peer connection
    block.rs             -- Block processing and mempool pipeline
    block_sync.rs        -- Block synchronization
    broadcast.rs         -- Message broadcasting
    certificate.rs       -- Block acceptance certificate handling
    consensus_protocol.rs -- Consensus protocol messages
    dag.rs               -- DAG block management
    fee_distribution.rs  -- Fee distribution P2P protocol
    group_manager.rs     -- Group membership management
    intra_group.rs       -- Intra-group election, PoU scoring, block production
    periodic_tasks.rs    -- Periodic PoU updates, elections, latency probes
    pou.rs               -- PoU score calculation and sharing
    receipts.rs          -- Transaction receipt handling
    swarm_commands.rs    -- Swarm command interface
    sync.rs              -- Chain sync protocol
    types.rs             -- P2P message types
    helpers.rs           -- Utility functions
    aux_protocol.rs      -- Auxiliary protocol (heartbeats, direct messages)
```

## Configuration

Key configuration parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `listen_port` | 4001 | P2P TCP listen port |
| `block_interval_secs` | 10 (2 in test) | Block production interval |
| `max_block_txs` | 2000 | Maximum transactions per block |
| `tx_interval_secs` | 2 | Transaction generation interval (0 = max speed) |
| `memory_only` | true | Use in-memory storage |
| `db_path` | (derived) | RocksDB path when persistent |

Network ports: P2P on configurable TCP port (default 4001), RPC on 8545 (when enabled).

## Dependencies

- `savitri-core` (testnet features) for types and crypto
- `savitri-consensus` (pou-based, lightweight) for consensus
- `savitri-p2p` (gossipsub, kademlia) for networking
- `savitri-mempool` for transaction management
- `savitri-storage` for chain state
- `savitri-zkp` (arkworks) for proof verification
- `savitri-rpc` (optional) for JSON-RPC server
- `libp2p` 0.55 for P2P transport
- `clap` for CLI argument parsing

## License

Apache-2.0
