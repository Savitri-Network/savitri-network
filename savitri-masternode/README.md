# savitri-masternode

Full validator node for Savitri Network, responsible for BFT consensus, group formation, block finalization, and network coordination.

## Overview

`savitri-masternode` is the coordination layer of the Savitri Network. Masternodes form consensus groups of lightnodes, verify block proposals, aggregate BFT votes, issue Block Acceptance Certificates, and maintain the canonical chain state in RocksDB. They run a full libp2p networking stack with gossipsub for message propagation and Kademlia for peer discovery.

The masternode uses adaptive batch collection for BFT vote aggregation, with configurable timeouts (base 50ms, peak multiplier 1.5x) to balance finalization latency against network conditions. Gossipsub is configured with 4MB max transmit size to accommodate blocks with up to 2000 transactions. The backup certification timeout is set to 300ms for fast failover.

Masternodes also serve as relay nodes for NAT traversal, enabling lightnodes behind NATs to participate in the network via QUIC transport, relay circuits, and DCUtR hole-punching.

## Features

- **BFT Consensus**: Byzantine Fault Tolerant voting with 2f+1 quorum. Adaptive batch collection for vote aggregation.
- **Group Formation**: Dynamic group assignment of lightnodes with deterministic group IDs (epoch-based). Configurable overlap threshold (25%).
- **Block Finalization**: Block Acceptance Certificate issuance after BFT verification. Backup certification with 300ms timeout.
- **Proposal Validation**: Signature verification, PoU score validation, and transaction root verification for proposed blocks.
- **P2P Networking**: libp2p 0.55 with gossipsub (mesh_n=8), Kademlia DHT, request-response, NAT traversal (QUIC, Relay server, DCUtR, AutoNAT).
- **Monolith Management**: Monolith (checkpoint) production, storage, and P2P distribution.
- **Contract Execution**: Smart contract runtime via `savitri-contracts` (governance, oracle, token standards).
- **ZKP Verification**: Zero-knowledge proof verification via `savitri-zkp` (Arkworks/Groth16).
- **Prometheus Metrics**: System metrics, consensus state, peer counts, block heights exposed on port 9090.
- **Telemetry**: CPU, memory, disk, and network monitoring via `sysinfo`.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `full` | Yes | Enables storage + consensus + p2p + contracts |
| `storage` | Via `full` | RocksDB persistent storage |
| `consensus` | Via `full` | BFT + group-aware consensus |
| `p2p` | Via `full` | P2P networking |
| `contracts` | Via `full` | Smart contract execution |
| `rpc` | No | JSON-RPC 2.0 API server |
| `zkp-plonk` | No | PLONK ZKP backend |
| `zkp-arkworks` | No | Arkworks ZKP backend (always linked via savitri-zkp) |
| `zkp-all` | No | All ZKP backends |
| `minimal` | No | Core + P2P only (no storage/consensus/contracts) |

## Usage

### CLI

```bash
# Start with TOML config file
savitri-masternode config/masternode.toml

# Example with pre-configured testnet configs
savitri-masternode exucutables/configs/masternode/mn-1.toml
```

### Configuration (TOML)

```toml
role = "Masternode"
network_key_path = "identity.key"
masternode_key_path = "masternode.key"
p2p_port = 4021
slot_duration = "1s"
slot_base_ms = 0
monolith_interval_secs = 30
monolith_max_blocks = 10000
group_node_timeout_secs = 600
bootstrap_peers = []
validators = []

[adaptive_batch_collector]
base_timeout_ms = 50
peak_timeout_multiplier = 1.5
```

### Docker

```bash
docker build -f docker/Dockerfile.masternode -t savitri-masternode .
docker run -d -p 4021:4021 -p 9090:9090 savitri-masternode
```

## Building

```bash
# Default (full features)
cargo build --release -p savitri-masternode

# With RPC server
cargo build --release -p savitri-masternode --features rpc

# Minimal build (no storage/consensus/contracts)
cargo build --release -p savitri-masternode --no-default-features --features minimal
```

Requires MSVC build tools on Windows (for RocksDB).

## Testing

```bash
cargo test -p savitri-masternode
```

For multi-node network tests:
```bash
# 5 masternodes + 10 lightnodes (PowerShell)
./local_test/run_5mn_10ln.ps1

# Batch scripts
./scripts/run_5mn_10ln_test.bat
```

## Architecture

```
src/
  main.rs                    -- CLI startup, config loading, module wiring
  lib.rs                     -- Library crate exports
  config.rs                  -- TOML configuration parsing
  config/                    -- Config module directory
  node.rs                    -- Masternode state management
  libp2p_network.rs          -- Main P2P event loop, gossipsub, peer management
  masternode_p2p.rs          -- Masternode-specific P2P messages
  gossipsub_ext.rs           -- Gossipsub extensions and helpers
  bootstrap.rs               -- Bootstrap peer connection and discovery
  group_formation.rs         -- Dynamic group assignment, deterministic group IDs
  group_consensus.rs         -- BFT voting within groups, cert issuance
  proposal_validator.rs      -- Block proposal verification (BlockAcceptanceCertificate)
  election_verification.rs   -- Election result verification
  consensus_integration.rs   -- Consensus module integration
  consensus_protocol.rs      -- Consensus protocol message handling
  consensus_storage_adapter.rs -- Adapter between consensus and storage
  adaptive_batch_collector.rs -- Adaptive timeout BFT vote collection
  batch_collector.rs         -- Basic vote batch collection
  vote_aggregator.rs         -- BFT vote aggregation
  block_messages.rs          -- Block-related P2P messages
  p2p_block_receiver.rs      -- Block reception and processing
  bridge.rs                  -- Bridge between subsystems
  mempool_manager.rs         -- Mempool coordination
  transaction_validator.rs   -- Transaction validation
  signature_verifier.rs      -- Ed25519 signature verification
  contract_executor.rs       -- Smart contract execution
  monolith_producer.rs       -- Monolith (checkpoint) creation
  monolith_storage.rs        -- Monolith persistence
  monolith_p2p.rs            -- Monolith P2P distribution
  monolith_p2p_fixed.rs      -- Fixed monolith P2P distribution
  monolith_benchmark.rs      -- Monolith performance benchmarks
  rewards.rs                 -- Block reward calculation
  rpc.rs                     -- RPC server integration
  telemetry.rs               -- Prometheus metrics and system monitoring
  system_metrics.rs          -- CPU/memory/disk monitoring
  performance.rs             -- Performance tracking
  error_handling.rs          -- Error type definitions
  retry_manager.rs           -- Retry logic for failed operations
  p2p.rs                     -- P2P module re-exports
  zkp_integration.rs         -- ZKP verification integration
  zkp_tests.rs               -- ZKP test suite
  integration_tests.rs       -- Integration test helpers
```

## Configuration

Masternode TOML configs are stored in `exucutables/configs/masternode/mn-*.toml` for the 5-MN test setup.

Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `p2p_port` | 4021 | P2P TCP listen port |
| `slot_duration` | 1s | Slot duration |
| `monolith_interval_secs` | 30 | Monolith production interval |
| `group_node_timeout_secs` | 600 | Timeout before removing inactive nodes |
| `connection_handler_queue_len` | 50000 | libp2p connection handler queue size |

Network ports: P2P on configured port (default 4021-4025 for test), Prometheus on 9090.

## Dependencies

- `savitri-core` (testnet features) for types and crypto
- `savitri-consensus` (group-aware, bft) for consensus engine
- `savitri-storage` (rocksdb) for persistent state
- `savitri-p2p` (kademlia) for networking
- `savitri-contracts` for smart contract execution
- `savitri-zkp` (arkworks) for proof verification
- `savitri-rpc` (optional) for JSON-RPC server
- `libp2p` 0.55 for full P2P stack
- `rocksdb` for direct database access
- `prometheus` for metrics

## License

Apache-2.0
