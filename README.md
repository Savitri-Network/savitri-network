# Savitri Network

[![CI](https://github.com/Savitri-Network/savitri-network/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Savitri-Network/savitri-network/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/Savitri-Network/savitri-network?include_prereleases&sort=semver&color=blue)](https://github.com/Savitri-Network/savitri-network/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.90%2B-orange.svg?logo=rust)](https://www.rust-lang.org/)
[![Edition](https://img.shields.io/badge/edition-2021-orange.svg?logo=rust)](https://doc.rust-lang.org/edition-guide/rust-2021/index.html)
[![Workspace](https://img.shields.io/badge/cargo--workspace-14%20crates-informational.svg?logo=rust)](Cargo.toml)
[![libp2p](https://img.shields.io/badge/libp2p-0.55-2C3E50?logo=ipfs&logoColor=white)](https://libp2p.io/)
[![RocksDB](https://img.shields.io/badge/storage-RocksDB-2E7D32?logo=database&logoColor=white)](https://rocksdb.org/)
[![Tokio](https://img.shields.io/badge/runtime-tokio-blue.svg)](https://tokio.rs/)
[![JSON-RPC](https://img.shields.io/badge/api-JSON--RPC%202.0-orange.svg)](https://www.jsonrpc.org/specification)
[![Consensus](https://img.shields.io/badge/consensus-PoU%20%2B%20BFT%20%2B%20Lattice-brightgreen.svg)](docs/CONSENSUS_V0.2_DESIGN.md)
[![ZKP](https://img.shields.io/badge/ZK-arkworks%20%7C%20halo2-9C27B0.svg)]()
[![Status](https://img.shields.io/badge/status-pre--release-yellow.svg)]()
[![Changelog](https://img.shields.io/badge/changelog-keepachangelog-orange.svg)](CHANGELOG.md)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)
[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Savitri-Network/savitri-network?quickstart=1)
Savitri Network is a high-performance blockchain protocol implementing
**Proof-of-Unity (PoU)** consensus with Byzantine Fault Tolerant (BFT)
voting, written in Rust.

The codebase is organized as a Cargo workspace of focused crates so each
layer of the stack — networking, storage, mempool, consensus,
cryptography, application contracts — can evolve, be reviewed, and be
embedded independently.

> **V0.2 Lattice runtime** — A DAG-BFT consensus runtime derived from
> the Bullshark / Narwhal family ships in observation-only mode
> alongside the V0.1 BFT path. Three concrete deviations from the
> reference designs: wall-clock-bucketed rounds, PoU-weighted pivot
> selection via deterministic shuffle, and an environment-variable-
> gated migration pattern. Empirical evidence on a 4-VM 6-MN 15-LN
> cluster: **0/277 mismatches** on canonical state digests under
> sustained load (Phase 1 D3+D4 validation). See
> [`docs/CONSENSUS_V0.2_DESIGN.md`](docs/CONSENSUS_V0.2_DESIGN.md)
> for the full design and the open
> [`Phase 2.6` security-hardening milestone](https://github.com/Savitri-Network/savitri-network/milestone/4)
> for the work in flight.

---

## Architecture overview

Savitri separates *block production* from *block finalization*:

| Role | Responsibility | V0.1 consensus role | V0.2 Lattice role |
|---|---|---|---|
| **Lightnode** | Produce blocks/cells, gossip transactions | PoU election + intra-group voting | Cell + attestation publisher, pivot in cycle commits |
| **Masternode** | Coordinate consensus, distribute group composition | Full BFT (≥ 2f+1 quorum) + PoU scoring | Group formation only — does not attest individual cells |
| **Guardian** | Archival / observation node | None (read-only) | None (read-only) |

Block production happens inside *groups*: a deterministic partitioning of
lightnodes into committees. Each group elects a proposer using the
five-component PoU score (a moving average of availability, latency,
integrity, reputation and participation, weighted 25/20/20/20/15).

- **V0.1** (chain finality today): the elected proposer emits a block;
  masternodes finalize via PBFT-style `2f+1` voting, issuing a
  block-acceptance certificate. This is the path that is
  authoritative on chain finality.
- **V0.2 Lattice** (observation-only today, flag-day cutover planned):
  every lightnode in a group publishes one *cell* per wall-clock
  second on a gossipsub topic, peers attest received cells, and the
  `LineageCommit::try_commit` walker collapses certified cells into
  ordered *cycle* commits using the Bullshark anchor + 2f+1 follower
  rule. The cycle pivot is elected by a deterministic
  `blake3`-seeded Fisher-Yates shuffle weighted by PoU score.

The V0.2 runtime is gated by `SAVITRI_CONSENSUS_VERSION=v2`; when unset
(default), V0.2 publishes and certifies but does not push commits to
chain storage — V0.1 BFT remains authoritative. The flag-day cutover
is conditioned on the empirical pre-criterion
`lattice_commit_matches_v1 = 1` over a window of at least 10⁵ blocks
(see the [Phase 2.6 milestone](https://github.com/Savitri-Network/savitri-network/milestone/4)).

---

## Workspace crates

| Crate | Purpose |
|---|---|
| [`savitri-core`](savitri-core) | Foundation types, Ed25519 signing, BLAKE3 / SHA-256 hashing, slot scheduler, common primitives |
| [`savitri-storage`](savitri-storage) | RocksDB-backed persistence with column families (and an in-memory backend for tests) |
| [`savitri-mempool`](savitri-mempool) | Transaction admission, prevalidation, sharding, class-aware ordering, replay-prevention |
| [`savitri-consensus`](savitri-consensus) | BFT voting, PoU scoring engine, dynamic group formation, ZKP integration glue, **V0.2 Lattice modules** (`lattice/aggregator.rs`, `lattice/commit.rs`, `lattice/pivot.rs`) — Bullshark/Narwhal-family DAG-BFT |
| [`savitri-p2p`](savitri-p2p) | libp2p 0.55: GossipSub, Kademlia DHT, Noise + Yamux, NAT traversal |
| [`savitri-rpc`](savitri-rpc) | JSON-RPC 2.0 HTTP API (axum), method namespaces, optional faucet |
| [`Savitri-contracts`](Savitri-contracts) | Smart contracts, governance / DAO, oracle framework, token standards |
| [`savitri-zkp`](savitri-zkp) | Pluggable zero-knowledge backends (mock for tests, arkworks Groth16/BN254, halo2 plonk) |
| [`savitri-sdk`](savitri-sdk) | Client library: RPC client, wallet, transaction builders |
| [`savitri-masternode`](savitri-masternode) | Masternode binary: BFT coordinator + finalizer |
| [`savitri-lightnode`](savitri-lightnode) | Lightnode binary: block producer + PoU participant + V0.2 Lattice runtime (`lattice_runtime.rs`) |
| [`savitri-guardian`](savitri-guardian) | Guardian binary: archival / observation only |
| [`genesis`](genesis) | Genesis-state generation utilities |
| [`tools/rpc-loadtest`](tools/rpc-loadtest) | RPC load-testing utility for development |

### Stack at a glance

| Layer | Technology |
|---|---|
| Language | Rust 2021 edition, MSRV 1.90 |
| Async runtime | [Tokio](https://tokio.rs/) |
| Networking | [libp2p 0.55](https://libp2p.io/) (GossipSub, Kademlia DHT, Noise, Yamux) |
| Storage | [RocksDB](https://rocksdb.org/) with column families, in-memory backend for tests |
| Cryptography | [`ed25519-dalek`](https://docs.rs/ed25519-dalek/), [`blake3`](https://docs.rs/blake3/), `sha2` |
| ZK | [`arkworks`](https://github.com/arkworks-rs) (Groth16/BN254), [`halo2`](https://github.com/zcash/halo2) (PLONK), mock for tests |
| RPC | [`axum`](https://docs.rs/axum/) (HTTP), JSON-RPC 2.0 |
| Serialization | `serde` + `bincode` (canonical fixint) for wire, `serde_json` for RPC |

---

## Documentation

- 🌐 **Project website**: [savitrinetwork.com](https://savitrinetwork.com)
- 📚 **Full documentation**: [docs.savitrinetwork.com](https://docs.savitrinetwork.com)
- 🗺️ **Roadmap**: [`ROADMAP.md`](ROADMAP.md) and the
  [open milestones](https://github.com/Savitri-Network/savitri-network/milestones)
- 🤝 **How to contribute**: [`CONTRIBUTING.md`](CONTRIBUTING.md)
- 🛡️ **Security policy**: [`SECURITY.md`](SECURITY.md)
- 🤲 **Code of conduct**: [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- 📜 **Changelog**: [`CHANGELOG.md`](CHANGELOG.md)

### In-tree protocol guides

- [`docs/getting-started.md`](docs/getting-started.md) — build a node
  and submit your first transaction
- [`docs/consensus.md`](docs/consensus.md) — Proof-of-Unity + BFT
  finalisation model (V0.1 baseline)
- [`docs/CONSENSUS_V0.2_DESIGN.md`](docs/CONSENSUS_V0.2_DESIGN.md) —
  V0.2 design specification: Latency Canon, Lattice ordering, migration
  gate, wire formats
- [`docs/group-formation.md`](docs/group-formation.md) — group
  lifecycle and proposer election
- [`docs/transactions.md`](docs/transactions.md) — wire format,
  signing, fees

### V0.2 Lattice — academic provenance

The V0.2 Lattice runtime is derived from the Bullshark / Narwhal
DAG-BFT family. The full academic references and Savitri-specific
deviations are documented inline in the source headers of
[`savitri-consensus/src/lattice/`](savitri-consensus/src/lattice/):

- **`aggregator.rs`** — follows Narwhal's primary-worker DAG mempool
  pattern (Danezis et al., EuroSys 2022,
  [arXiv:2105.11827](https://arxiv.org/abs/2105.11827)).
- **`commit.rs`** — implements the Bullshark commit rule
  (Spiegelman et al., CCS 2022,
  [arXiv:2201.05677](https://arxiv.org/abs/2201.05677)).
- **`pivot.rs`** — substitutes Algorand's VRF-based committee
  selection (Gilad et al., SOSP 2017,
  [doi:10.1145/3132747.3132757](https://doi.org/10.1145/3132747.3132757))
  with a deterministic blake3-seeded Fisher-Yates shuffle weighted
  by the PoU score.

A 44-page preprint covering the full design, empirical evaluation, and
honest security analysis is available in the companion testnet
repository (see `docs/paper_accademico_savitri.pdf` in
[Savitri-Testnet-V0.1.0](https://github.com/Savitri-Network/Savitri-Testnet-V0.1.0)).
An arXiv preprint submission is planned.

---

## Requirements

- Rust stable ≥ 1.90
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
cargo build --release -p savitri-lightnode \
    --no-default-features --features desktop,rpc
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

### Open work — good places to start

Two milestones are currently active and explicitly designed to welcome
external contributors:

#### 🛡️ [Phase 2.6 — Security hardening](https://github.com/Savitri-Network/savitri-network/milestone/4)

NTP-resilient TimeOracle for residential / mobile / IoT validators,
plus PoU floor admission gate, equivocation slashing, and cross-shard
watchdog committee. The Lattice runtime depends on NTP synchronisation
for the wall-clock-bucket rounds; this milestone tracks the work to
make that dependence safe on heterogeneous validator infrastructure.

Recommended entry point:
[#35 — TimeOracle scaffolding](https://github.com/Savitri-Network/savitri-network/issues/35)
(~3 days, no upstream dependencies).

#### 🔌 [Phase 2.6-RPC — RPC migration](https://github.com/Savitri-Network/savitri-network/milestone/5)

JSON-RPC migration plan for the V0.1 → V0.2 semantic shift (block
height → cycle index). Four sub-issues covering Lattice introspection
endpoints, `tx_getStatus` extension, dual-mode chain reads, and V0.1
endpoint retirement.

Recommended entry point:
[#43 — Lattice introspection endpoints](https://github.com/Savitri-Network/savitri-network/issues/43)
(~2-3 weeks, purely additive, no SDK coordination required).

Additional follow-on work — VRF-based group assignment,
`lattice_commit_matches_v1` divergence counter for the empirical
pre-activation criterion — will be filed as the in-flight items
land.

---

## License

Apache-2.0. See [`LICENSE`](LICENSE).

---

## Status

This is an open-source release of the Savitri Network reference
implementation. The codebase is under active development; APIs and wire
formats may change between minor versions until a stable 1.0 release.

- **V0.1 chain finality**: production-grade, authoritative.
- **V0.2 Lattice runtime**: shipped in observation-only mode (default).
  Authoritative-mode activation is gated on the empirical pre-criterion
  documented in
  [`docs/CONSENSUS_V0.2_DESIGN.md`](docs/CONSENSUS_V0.2_DESIGN.md) §5
  and the security-hardening items tracked under
  [milestone #4](https://github.com/Savitri-Network/savitri-network/milestone/4).
- **Wire formats**: V0.1 wire formats are stable; V0.2 attestation
  envelope evolves to v2 with the `signer_unix_secs_at_sign` field
  (tracked under
  [#38](https://github.com/Savitri-Network/savitri-network/issues/38)).
  Migration is staged with both v1 and v2 topics co-existing during
  the transition window.
