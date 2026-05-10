# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until the project reaches a stable `1.0` release, the public API may
change between minor versions. Pin to an exact tag in production.

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.1.0] — 2026-05-10

### Added

- Initial open-source release of the Savitri Network reference
  implementation, organised as a Cargo workspace of 14 crates.
- `savitri-core`: foundation types, Ed25519 signing, BLAKE3 / SHA-256
  hashing, slot scheduler, common primitives.
- `savitri-storage`: RocksDB-backed persistence with column families
  and an in-memory backend for tests.
- `savitri-mempool`: transaction admission, prevalidation, sharded
  pipeline, class-aware ordering, replay protection.
- `savitri-consensus`: BFT voting, PoU scoring engine, dynamic group
  formation, ZKP integration glue.
- `savitri-p2p`: libp2p 0.55 stack with GossipSub, Kademlia DHT, Noise
  + Yamux transport, NAT traversal.
- `savitri-rpc`: JSON-RPC 2.0 HTTP API built on `axum`.
- `Savitri-contracts`: smart-contract framework, governance / DAO
  module, oracle framework, token standards.
- `savitri-zkp`: pluggable zero-knowledge backends (mock, arkworks
  Groth16/BN254, halo2 PLONK).
- `savitri-sdk`: client library with RPC client, wallet, and
  transaction builders.
- `savitri-masternode`, `savitri-lightnode`, `savitri-guardian`: the
  three node binaries.
- `genesis`: genesis-state generation utilities for testnet.
- `tools/rpc-loadtest`: RPC load-testing utility for development.
- Public roadmap with three milestones (`Phase 1 — Foundations & DX`,
  `Phase 2 — Performance & Reliability`, `Phase 3 — Research &
  Scaling`) and 15 tracked issues.
- Project documentation: `README.md`, `ROADMAP.md`, per-crate
  `README.md` files, Apache-2.0 license.

[Unreleased]: https://github.com/Savitri-Network/savitri-network/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Savitri-Network/savitri-network/releases/tag/v0.1.0
