# Roadmap

The roadmap is organized in three phases. Every issue is tagged with a
`phase: …` label and a `priority: P0/P1/P2/P3` label. Each phase has a
matching [milestone](https://github.com/Savitri-Network/savitri-network/milestones).

| Priority | Meaning |
|---|---|
| **P0** | Critical for project hygiene; blocks contributors |
| **P1** | High — clear unlock or significant DX improvement |
| **P2** | Medium — useful but not blocking |
| **P3** | Future — long-term direction |

---

## Phase 1 — Foundations & DX

Project hygiene, developer experience, and observability. Items here are
well-scoped and contributor-friendly: they make the codebase easier to
build, test, document, and operate.

| # | Issue | Priority |
|---|---|---|
| [#12](../../issues/12) | Add GitHub Actions CI: `cargo fmt + clippy + check + test` | **P0** |
| [#13](../../issues/13) | Doc-comments for every public API in `savitri-core` and `savitri-sdk` | **P1** |
| [#21](../../issues/21) | Structured JSON logging via `tracing-subscriber` | **P1** |
| [#22](../../issues/22) | JSON schema validation for masternode/lightnode config TOML | **P1** |
| [#19](../../issues/19) | Criterion benchmark suite for the mempool admission path | **P2** |

> Good entry points for first-time contributors: **#12, #13, #22**.

---

## Phase 2 — Performance & Reliability

Throughput and resilience improvements that build on the Phase 1
foundations: tunable mempool, RPC ergonomics, fuzz-driven regression
discovery, property-based correctness checks.

| # | Issue | Priority |
|---|---|---|
| [#9](../../issues/9) | Configurable `MIN_TX_PER_BLOCK` env knob to increase block density | **P1** |
| [#16](../../issues/16) | Prometheus histograms for block-density distribution | **P1** |
| [#17](../../issues/17) | Cursor-based pagination for `chain_getBlocks` | **P2** |
| [#18](../../issues/18) | `cargo-fuzz` harness for the transaction parser | **P2** |
| [#20](../../issues/20) | Property-based tests for block-hash determinism with `proptest` | **P2** |

---

## Phase 3 — Research & Scaling

Architectural research towards higher throughput and horizontal scaling.
These items are explicitly long-term: they involve design tradeoffs,
breaking changes to wire formats, and cross-crate refactors.

| # | Issue | Priority |
|---|---|---|
| [#4](../../issues/4) | DAG-based mempool with availability certificates | **P2** |
| [#5](../../issues/5) | Optimistic parallel transaction execution with conflict detection | **P3** |
| [#6](../../issues/6) | Erasure-coded block shredding for low-latency propagation | **P3** |
| [#14](../../issues/14) | Aggregated BLS signatures for block-acceptance certificates | **P3** |
| [#15](../../issues/15) | Sharded execution per `assigned_shards` | **P3** |

---

## How to read the labels

Every issue carries:

- a `phase: …` label tying it to a phase milestone,
- a `priority: P0..P3` label,
- one or more topic labels (`consensus`, `mempool`, `p2p`, `rpc`,
  `crypto`, `architecture`, `testing`, `observability`, `dx`,
  `documentation`, `ci`, …).

Use the GitHub filters to find work that matches your interests. For
example:

- [`good first issue`](https://github.com/Savitri-Network/savitri-network/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
- [`phase: foundations`](https://github.com/Savitri-Network/savitri-network/issues?q=is%3Aissue+is%3Aopen+label%3A%22phase%3A+foundations%22)
- [`priority: P1`](https://github.com/Savitri-Network/savitri-network/issues?q=is%3Aissue+is%3Aopen+label%3A%22priority%3A+P1%22)

---

## Versioning

The roadmap targets a stable `1.0` release once Phase 1 + Phase 2 are
complete and at least the foundational items in Phase 3 are in place.
Until then APIs and wire formats may change between minor versions.
