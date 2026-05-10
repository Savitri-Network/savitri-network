# Savitri Network

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.82%2B-orange.svg?logo=rust)](https://www.rust-lang.org/)
[![Edition](https://img.shields.io/badge/edition-2021-orange.svg?logo=rust)](https://doc.rust-lang.org/edition-guide/rust-2021/index.html)
[![Workspace](https://img.shields.io/badge/cargo--workspace-14%20crates-informational.svg?logo=rust)](Cargo.toml)
[![libp2p](https://img.shields.io/badge/libp2p-0.55-2C3E50?logo=ipfs&logoColor=white)](https://libp2p.io/)
[![RocksDB](https://img.shields.io/badge/storage-RocksDB-2E7D32?logo=database&logoColor=white)](https://rocksdb.org/)
[![Tokio](https://img.shields.io/badge/runtime-tokio-blue.svg)](https://tokio.rs/)
[![JSON-RPC](https://img.shields.io/badge/api-JSON--RPC%202.0-orange.svg)](https://www.jsonrpc.org/specification)
[![Consensus](https://img.shields.io/badge/consensus-PoU%20%2B%20BFT-brightgreen.svg)]()
[![ZKP](https://img.shields.io/badge/ZK-arkworks%20%7C%20halo2-9C27B0.svg)]()
[![Status](https://img.shields.io/badge/status-pre--release-yellow.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

Savitri Network is a high-performance blockchain protocol implementing
**Proof-of-Unity (PoU)** consensus with Byzantine Fault Tolerant (BFT)
voting, written in Rust.

The codebase is organized as a Cargo workspace of focused crates so each
layer of the stack — networking, storage, mempool, consensus,
cryptography, application contracts — can evolve, be reviewed, and be
embedded independently.

---

## Architecture overview

Savitri separates *block production* from *block finalization*:

| Role | Responsibility | Consensus role |
|---|---|---|
| **Lightnode** | Produce blocks, participate in PoU election, gossip transactions | PoU election + intra-group voting |
| **Masternode** | Finalize blocks, coordinate BFT, lead per-group consensus | Full BFT (≥ 2f+1 quorum) + PoU scoring |
| **Guardian** | Archival / observation node | None (read-only) |

Block production happens inside *groups*: a deterministic partitioning of
lightnodes into committees. Each group elects a proposer using the PoU
score (a moving average of availability, latency, integrity, reputation
and participation), and emits blocks that the masternodes finalize via
classic PBFT-style voting (`2f+1` quorum). A block-acceptance certificate
makes finalization observable to the rest of the network.

---

## Workspace crates

| Crate | Purpose |
|---|---|
| [`savitri-core`](savitri-core) | Foundation types, Ed25519 signing, BLAKE3 / SHA-256 hashing, slot scheduler, common primitives |
| [`savitri-storage`](savitri-storage) | RocksDB-backed persistence with column families (and an in-memory backend for tests) |
| [`savitri-mempool`](savitri-mempool) | Transaction admission, prevalidation, sharding, class-aware ordering, replay-prevention |
| [`savitri-consensus`](savitri-consensus) | BFT voting, PoU scoring engine, dynamic group formation, ZKP integration glue |
| [`savitri-p2p`](savitri-p2p) | libp2p 0.55: GossipSub, Kademlia DHT, Noise + Yamux, NAT traversal |
| [`savitri-rpc`](savitri-rpc) | JSON-RPC 2.0 HTTP API (axum), method namespaces, optional faucet |
| [`Savitri-contracts`](Savitri-contracts) | Smart contracts, governance / DAO, oracle framework, token standards |
| [`savitri-zkp`](savitri-zkp) | Pluggable zero-knowledge backends (mock for tests, arkworks Groth16/BN254, halo2 plonk) |
| [`savitri-sdk`](savitri-sdk) | Client library: RPC client, wallet, transaction builders |
| [`savitri-masternode`](savitri-masternode) | Masternode binary: BFT coordinator + finalizer |
| [`savitri-lightnode`](savitri-lightnode) | Lightnode binary: block producer + PoU participant |
| [`savitri-guardian`](savitri-guardian) | Guardian binary: archival / observation only |
| [`genesis`](genesis) | Genesis-state generation utilities |
| [`tools/rpc-loadtest`](tools/rpc-loadtest) | RPC load-testing utility for development |

### Stack at a glance

| Layer | Technology |
|---|---|
| Language | Rust 2021 edition, MSRV 1.82 |
| Async runtime | [Tokio](https://tokio.rs/) |
| Networking | [libp2p 0.55](https://libp2p.io/) (GossipSub, Kademlia DHT, Noise, Yamux) |
| Storage | [RocksDB](https://rocksdb.org/) with column families, in-memory backend for tests |
| Cryptography | [`ed25519-dalek`](https://docs.rs/ed25519-dalek/), [`blake3`](https://docs.rs/blake3/), `sha2` |
| ZK | [`arkworks`](https://github.com/arkworks-rs) (Groth16/BN254), [`halo2`](https://github.com/zcash/halo2) (PLONK), mock for tests |
| RPC | [`axum`](https://docs.rs/axum/) (HTTP), JSON-RPC 2.0 |
| Serialization | `serde` + `bincode` (canonical fixint) for wire, `serde_json` for RPC |

---

## Requirements

- Rust stable ≥ 1.82
- A C / C++ toolchain (MSVC on Windows, build-essentials on Linux,
  Xcode CLT on macOS) — RocksDB is built from source.
- `cmake`, `clang`, `protobuf-compiler` are typically required by
  transitive dependencies.

---

## Build

Workspace check / build:

```bash
cargo check --workspace
cargo build --workspace --release
```

Build a specific node:

```bash
cargo build --release -p savitri-masternode --features rpc
cargo build --release -p savitri-lightnode  --features rpc
cargo build --release -p savitri-guardian
```

Lightnode without persistent storage (in-memory backend):

```bash
cargo build --release -p savitri-lightnode --no-default-features --features desktop
```

---

## Test

```bash
cargo test --workspace
cargo test -p savitri-consensus
```

---

## Contributing

Contributions are welcome. Please open an issue first for any non-trivial
change, and follow the existing code style. PRs that include tests are
strongly preferred.

---

## License

Apache-2.0. See [`LICENSE`](LICENSE).

---

## Status

This is an open-source release of the Savitri Network reference
implementation. The codebase is under active development; APIs and wire
formats may change between minor versions until a stable 1.0 release.
