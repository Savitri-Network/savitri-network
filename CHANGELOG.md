# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until the project reaches a stable `1.0` release, the public API may
change between minor versions. Pin to an exact tag in production.

Lattice design overview: see [`docs/CONSENSUS_V0.2_DESIGN.md`](docs/CONSENSUS_V0.2_DESIGN.md).

## [Unreleased]

### Added

- feat(lightnode): `LatticeBlock` construction — for every committed `Cycle`,
  builds a `LatticeBlock` with `cycle_index`, `group_id`, SHA-256
  `parent_block_hash` chain, deterministic `tx_root` (Merkle-flat SHA-256),
  `pivot`, `timestamp_ms`, and `signed_tx_bytes`; `hash()` domain-separated with
  `"savitri-lattice-block-v1"`. Per-group parent-hash chain starts at `[0; 32]`
  (genesis) and chains deterministically across cycles. Opt-in gossipsub
  broadcast on `/savitri/group/<gid>/lattice/block/1` gated by
  `SAVITRI_LATTICE_BLOCK_BROADCAST` env var (default OFF).
  Shadow-only — V0.1 BFT chain is unaffected. (P2.6-C.2 Phase B.2, commit `10a5a30`)
- feat(lightnode): wire real PoU scores from `LatencyCanonState` into
  `LatticeRuntime` `group_provider` — replaces constant-1000 placeholders;
  subscribes to `/savitri/group/<gid>/latency_canon/1` per group rotation,
  deserializes + Ed25519-verifies `LatencyReport` gossip messages, ingests into
  shared `LatencyCanonState`, and rebuilds the canonical latency table per
  wall-clock bucket at every `group_provider` call; spawns periodic 10 s
  `latency_canon_publisher` task; `lookup_score` returns neutral 1000 during
  bootstrap window. `LatencyCanonState` exposed via `NetworkComponents`.
  (P2.6 A.6b, commit `cea3dc3`)

### In progress

- Phase 2.5 self-attestation fix validation cluster-wide (113 cert events
  observed in `group_555_0_555_ad8e05b7` across 3 co-grouped LNs).
- Code-level academic citations added to
  `savitri-consensus/src/lattice/{aggregator,commit,pivot}.rs` (Bullshark
  CCS '22, Narwhal EuroSys '22, DAG-Rider PODC '21, Algorand SOSP '17).

### Planned

- Phase 2.6+ — security hardening (PoU floor, equivocation slashing,
  cross-shard watchdog committee).
- Chain hook for authoritative mode (`SAVITRI_CONSENSUS_VERSION=v2`).
- arXiv preprint paper #1: *Savitri Lattice: Wall-Clock-Bucketed DAG-BFT
  with PoU-Weighted Pivot Selection*.

## [0.2.5] — 2026-05-11

V0.2 Phase 2.4.2 — Lattice runtime observability.

### Added

- `DIAG[lattice]` aggregator state snapshot logging
  (`pending` / `certified` / `last_cycle`, periodic 1 Hz per group).
- `DIAG[lattice-cert]` event emitted when a cell reaches BFT 2f+1 quorum.
- `DIAG[lattice-commit]` event emitted when `LineageCommit::try_commit`
  produces a `Cycle`.
- Per-cycle observability counters in `lattice_runtime.rs` (+133 LOC).
- Migration gate `SAVITRI_CONSENSUS_VERSION` env var (default unset =
  observation-only, `v2` = authoritative).
- `scripts/observe_lattice.sh` — poll Phase 2.4.2 counters cluster-wide.

### Notes

- Chain hook in `commit_poller_loop` is intentionally a `debug!` TODO
  pending security hardening (PoU floor, equivocation slashing,
  cross-shard watchdog committee).
- Observation-only mode default ensures V0.1 BFT MN-centric path remains
  authoritative until soak validation completes.

### References

- Commits `cc70571`, `23240bc`
- Docs: `docs/CONSENSUS_V0.2_DESIGN.md`

## [0.2.4] — 2026-05-10

V0.2 Phase 2.1 + 2.2 + 2.3 — Lattice runtime (aggregator + commit + pivot).

### Added

- `savitri-consensus/src/lattice/aggregator.rs` — DAG cell + attestation
  aggregator. Follows the Narwhal primary-worker pattern simplified to
  single-tier (Danezis et al., EuroSys 2022,
  <https://arxiv.org/abs/2105.11827>).
- `savitri-consensus/src/lattice/commit.rs` — Bullshark commit rule:
  anchor + 2f+1 follower votes, with deterministic round-major +
  author-lex topological ordering (Spiegelman et al., CCS 2022,
  <https://arxiv.org/abs/2201.05677>).
- `savitri-consensus/src/lattice/pivot.rs` — PoU-weighted pivot election
  via blake3-seeded deterministic Fisher-Yates shuffle (Algorand-family
  weighted committee selection, VRF substituted; Gilad et al.,
  SOSP 2017, <https://doi.org/10.1145/3132747.3132757>).
- `savitri-lightnode/src/lattice_runtime.rs` — `LatticeRuntime` with
  `publisher_loop` (1 Hz) + `commit_poller_loop` (0.5 Hz).
- Gossipsub topics
  `/savitri/group/<gid>/lattice/cell/1` and
  `/savitri/group/<gid>/lattice/attestation/1`.
- 5 wiring patch points in `savitri-lightnode/src/p2p/network/mod.rs`.

### Changed

- `savitri-core/src/types/lattice.rs` — `LatticeCell`,
  `CellAttestation`, `CellCertificate`, `Cycle`, `BatchRoot`, `CellId`
  derive `PartialEq + Eq` for canonical comparison.

### References

- Commit `126389d`
- Docs: `docs/CONSENSUS_V0.2_DESIGN.md`

## [0.2.3] — 2026-05-09

V0.2 Phase 2 spike — wall-clock latency convergence + Lattice ordering.

### Added

- Wall-clock-bucketed rounds for both LatencyCanon publisher and Lattice
  runtime: `round = unix_secs / N` (10 s for LatencyCanon, 1 s for
  Lattice). Eliminates DAG-depth-derived observer divergence.
- Phase 2 spike: Lattice ordering wire types in `savitri-core`.

### Notes

- Trade-off documented: the wall-clock approach requires NTP
  synchronisation across nodes. In exchange it provides cluster-wide
  byte-identical canonical state without coordination.

### Validation

- 44,739 TX accept under sustained load.
- 0 signature failures on the canonical wire format.
- Phase 1 D3+D4 HARD PASS: 0/277 mismatches on `ranked_hash_stable`,
  `tenure_start_height`, `group_members_hash`, `ranked_hash_full`.

### References

- Commit `b499259`
- Docs: `docs/CONSENSUS_V0.2_DESIGN.md` §4

## [0.2.2] — 2026-05-07

V0.2 Phase 1.5 completion — election candidates in signable bytes.

### Fixed

- Election candidate set + `proposer_pou_score` re-included in
  `signable_bytes()` for the canonical election certificate signature.
  Resolves a revert-of-revert tightening the signed payload after the
  Phase 1.4c port from Savitri-Testnet-V0.1.0.

### References

- Commit `a96cc36`

## [0.2.1] — 2026-05-05

V0.2 Phase 1.5 — u32 permille wire format.

### Changed

- Wire-format migration from `f64` to `u32` permille (per-mille
  fixed-point) for canonical PoU score serialisation. Eliminates
  floating-point divergence across observers in serialised election
  results.

### References

- Commit `cea8106`
- Docs: `docs/CONSENSUS_V0.2_DESIGN.md` §3.4

## [0.2.0] — 2026-05-03

V0.2 Phase 1 — Score Canonicity (LatencyCanonState integration).

### Added

- `LatencyCanonState` integration into the consensus path (port from
  Savitri-Testnet-V0.1.0).
- Score canonicity verification across observers via deterministic
  latency table convergence.
- `savitri-lightnode/src/latency_canon_publisher.rs` — periodic
  publisher of canonical `LatencyReport` on the intra-group gossip
  topic `/savitri/group/<gid>/latency_canon/1`.

### References

- Commit `716ae1b`
- Docs: `docs/CONSENSUS_V0.2_DESIGN.md` §3.3

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

[Unreleased]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.5...HEAD
[0.2.5]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Savitri-Network/savitri-network/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Savitri-Network/savitri-network/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/Savitri-Network/savitri-network/releases/tag/v0.1.0
