# Savitri V0.2 Consensus Design

Version: 0.2-draft-1
Status: Proposed
Scope: Phase 1 (Score Canonicity) — full design. Phase 2 (Lattice ordering) — architectural sketch only.

---

## 1. Overview

Savitri V0.1 consensus relies on a single elected proposer per slot, with a synchronous leader-mediated commit path. Test sessions have identified three converging blockers inherent to this model:

1. **Pivot concentration** — a small subset of lightnodes wins proposer election repeatedly; the rest idle.
2. **Election certificate under-quorum** — non-deterministic PoU scoring causes attestation signature mismatch; certificates fail BFT quorum under load.
3. **Cert broadcast cross-process fragility** — commit notification depends on a multi-hop gossipsub broadcast (MN→LN) that silently fails when the mesh is under-populated.

V0.2 addresses all three with two coordinated changes, designed to ship in sequence:

- **Phase 1 — Score Canonicity**: make every component of the PoU score reproducible offline by any observer. Standardizes the latency measurement, eliminates per-observer f64 jitter, makes election certificates signable identically across all observers. Resolves blocker (2). Independent shippable.

- **Phase 2 — Lattice ordering**: replace single-proposer-per-slot with parallel batch publication on a DAG (the **Lattice**), committed in two-round **cycles** anchored by a PoU-elected **cycle pivot**. The pivot does not collect transactions; it only provides a deterministic commit anchor. Resolves blockers (1) and (3). Builds on Phase 1, planned as a follow-up.

This document specifies Phase 1 in full and provides an architectural sketch of Phase 2 sufficient to verify Phase 1 design decisions are compatible.

---

## 2. Nomenclature

The following terms are defined for Savitri V0.2 and used consistently throughout the codebase and documentation:

| Term | Definition |
|---|---|
| **Score canonicity** | The property of the PoU score being reproducible offline by any observer given only on-chain state. |
| **Latency canon** | The protocol by which lightnodes report measured peer RTTs to produce a canonical observation. |
| **Latency report** | A signed message from one LN containing its observed RTT to each peer in its group, bucketed. |
| **Latency table** | A chain-state structure mapping `(group_id, peer_id) → canonical_rtt_bucket`, updated each round from latency reports. |
| **Lattice** | A directed acyclic graph (DAG) of certified cells, one instance per group. The parallel data layer for Phase 2. |
| **Lattice cell** | A vertex in the lattice. One batch of transactions from one LN at one lattice round. |
| **Cell certificate** | The set of 2f+1 signatures attesting that a lattice cell is committed-by-availability at the data layer. |
| **Lattice round** | One time step of the lattice. All cells published at the same round share a logical timestamp. |
| **Cycle** | A commit unit consisting of two consecutive lattice rounds plus a commit decision. |
| **Cycle pivot** | The LN elected to anchor a cycle's commit, chosen via PoU-weighted round-robin. |
| **Lineage commit** | The rule by which all causal ancestors of the cycle pivot's cell commit deterministically once the pivot has 2f+1 followers in the next round. |

Carry-overs from V0.1 (unchanged):

| Term | Definition |
|---|---|
| **Group** | A logical partition of lightnodes that runs consensus together. Identified by `group_<epoch>_<idx>`. |
| **Epoch** | A time window across which group membership is stable. |
| **Tenure window** | Number of consecutive scheduling slots before the PoU-weighted RR schedule is regenerated. In V0.1 a slot = 1 block; in V0.2 a slot = 1 cycle. |
| **PoU score** | Composite score on [0, 1000] computed from five components (availability, latency, integrity, reputation, participation). |

---

## 3. Phase 1 — Score Canonicity

### 3.1 Goal

Eliminate observer-dependent f64 values from the inputs used to compute the PoU score, so that:

- Every node in a group computes the same `latency_score(peer)` for the same peer at the same round.
- The election certificate `signable_bytes` can include the candidates field (currently excluded due to f64 variance).
- Attestation signatures match across signers; the BFT 2f+1 quorum check on election certificates succeeds at full strength.

### 3.2 Architecture

```
  Phase 1 — Score Canonicity pipeline
  
  1. Local probe                  2. Periodic report                  3. Aggregation
  ┌─────────────────────┐      ┌──────────────────────────┐       ┌─────────────────────┐
  │ LN-X pings each     │      │ LN-X publishes           │       │ Each observer       │
  │ peer in group every │  →   │ LatencyReport every 10s  │   →   │ collects reports    │
  │ 1s. Maintains       │      │ on intra-group topic     │       │ across last 30s.    │
  │ sliding-window      │      │ /savitri/group/<gid>/    │       │ Computes median per │
  │ median over 10s.    │      │ latency_canon/1          │       │ peer.               │
  └─────────────────────┘      └──────────────────────────┘       └──────────┬──────────┘
                                                                              ▼
                                                                  4. LatencyTable update
                                                                  ┌─────────────────────┐
                                                                  │ LatencyTable[group] │
                                                                  │ [peer_id] =         │
                                                                  │   median_rtt_bucket │
                                                                  └──────────┬──────────┘
                                                                              ▼
                                                                  5. PoU score lookup
                                                                  ┌─────────────────────┐
                                                                  │ latency_score(X)    │
                                                                  │   = max(0, 100      │
                                                                  │       - lookup(X)/5)│
                                                                  │ (integer arithmetic)│
                                                                  └─────────────────────┘
```

### 3.3 Wire format

`LatencyReport` is defined in a new module `savitri-consensus/src/types/latency_canon.rs`:

```rust
/// A lightnode's observation of peer round-trip times in its group.
/// Bucketed at 5ms granularity; range 0..255 covers 0..1275ms.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyReport {
    /// Lattice round (Phase 2) or block height (Phase 1 fallback).
    pub round: u64,
    /// Reporter's group_id.
    pub group_id: String,
    /// Reporter peer_id.
    pub reporter: String,
    /// Observations of each peer in the group (excluding self).
    pub observations: Vec<PeerLatencyObservation>,
    /// Ed25519 signature over (round, group_id, reporter, observations).
    pub signature: [u8; 64],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerLatencyObservation {
    pub peer_id: String,
    /// RTT bucket: rtt_ms / 5. Saturating cast at 255.
    pub rtt_ms_bucket: u8,
    /// Number of probe samples in the reporter's sliding window.
    /// Used as a confidence weight by the aggregator.
    pub samples: u8,
}
```

Floating point is explicitly absent. All values are integer; serialization is canonical (no f64 → no signature reproducibility issue).

### 3.4 Aggregation rules

For a given `(group_id, peer_id)` at observation window ending at round R:

1. Collect all `LatencyReport`s published in rounds `[R-3, R]` (configurable window) whose `group_id` matches and whose `observations` contain an entry for `peer_id`.
2. Verify each report's signature against the reporter's known public key.
3. Drop reports where `samples < MIN_SAMPLES` (default 3). Insufficient confidence.
4. Extract the `rtt_ms_bucket` value from each remaining report.
5. The canonical bucket for `(group_id, peer_id)` is the **median** of the extracted values. If even count, lower median is used (deterministic tiebreak).

Pseudocode:

```rust
fn compute_canonical_rtt(
    reports: &[LatencyReport],
    target: &str,
    group_id: &str,
    window_end: u64,
    window_size: u64,
) -> Option<u8> {
    let mut samples: Vec<u8> = reports
        .iter()
        .filter(|r| r.round >= window_end.saturating_sub(window_size))
        .filter(|r| r.round <= window_end)
        .filter(|r| r.group_id == group_id)
        .filter(|r| verify_signature(r))
        .flat_map(|r| {
            r.observations
                .iter()
                .filter(|o| o.peer_id == target && o.samples >= MIN_SAMPLES)
                .map(|o| o.rtt_ms_bucket)
        })
        .collect();

    if samples.len() < MIN_REPORTERS {
        return None;  // Insufficient observations; latency_score defaults to 100.
    }

    samples.sort();
    // Lower-median for deterministic tiebreak on even count.
    Some(samples[(samples.len() - 1) / 2])
}
```

Constants:
- `MIN_SAMPLES = 3` — drop reporters whose sliding-window is sparse.
- `MIN_REPORTERS = 2` — require at least 2 distinct reporters for the canonical bucket to be valid. Otherwise default to 100 (neutral).
- `WINDOW_SIZE = 3` rounds — observation window for median computation.

### 3.5 LatencyTable representation

Chain state stores a per-group table, persisted alongside the existing group metadata:

```rust
pub struct LatencyTable {
    /// (group_id, peer_id) → canonical bucket. None means "no canonical value yet".
    pub entries: HashMap<(String, String), Option<u8>>,
    /// Last round at which this table was updated.
    pub last_update_round: u64,
}
```

Update rule: at the end of each round, each masternode that participates in the group's BFT recomputes the table from the gossip buffer of `LatencyReport`s received since the previous update. The resulting table is published as part of the next block's metadata. All other observers reconstruct the same table from the same inputs.

### 3.6 PoU score integration

The existing `latency_score(peer)` function in the PoU module is replaced with a lookup:

```rust
// BEFORE: per-observer measurement
fn latency_score(&self, peer: &PeerId) -> u32 {
    let rtt = self.local_rtt_measurements.get(peer).unwrap_or(default_rtt);
    score_from_rtt(rtt as f64)  // f64 → per-observer variance
}

// AFTER: chain-state lookup
fn latency_score(&self, peer: &PeerId, group: &str) -> u32 {
    let bucket = self.latency_table
        .lookup(group, peer)
        .unwrap_or(0);  // None → max score (neutral bootstrap)
    score_from_bucket(bucket)  // integer arithmetic
}

#[inline]
fn score_from_bucket(bucket: u8) -> u32 {
    100u32.saturating_sub(bucket as u32 / 5)
}
```

The other four PoU components (availability, integrity, reputation, participation) are already canonical-by-construction (heartbeat counts, signature observations, on-chain state) and require no changes for Phase 1.

### 3.7 Election certificate signable bytes

Currently `signable_bytes` excludes the candidates field due to f64 variance. With Phase 1 deployed, the field becomes deterministic and is re-included:

```rust
// BEFORE
#[derive(Serialize)]
struct Signable<'a> {
    round: u64,
    elected_proposer: &'a str,
    sender: &'a str,
    group_id: &'a str,
    // candidates: EXCLUDED (per-node f64 latency variance)
    // proposer_pou_score: EXCLUDED (per-node f64 variance)
}

// AFTER (Phase 1 deployed)
#[derive(Serialize)]
struct Signable<'a> {
    round: u64,
    elected_proposer: &'a str,
    sender: &'a str,
    group_id: &'a str,
    candidates: &'a [(String, u32)],          // canonical via LatencyTable lookup
    proposer_pou_score: u32,                   // canonical via LatencyTable lookup
}
```

Attestation signatures now match across signers. The masternode's BFT 2f+1 quorum check on election cert attestations can be re-enabled at full strength; the env override added as a temporary mitigation (`SAVITRI_FALLA2_DISABLE`) is removed.

### 3.8 Anti-gaming

| Attack | Defense |
|---|---|
| LN-X under-reports its own RTT to appear faster | LN-X does not report its own RTT; only peers report RTT-to-X. |
| LN-X over-reports RTT to a competitor LN-Y | Median across all reporters trims outliers. With f Byzantine LN out of n, at most f outliers can move the median by `(f + 1)`-th order statistic. With n=5 and BFT f=1, a single outlier cannot move the median across honest values. |
| f LN collude to inflate RTT to honest LN-Y | Under BFT assumption f < n/3, honest majority of reports preserves the canonical value. Beyond this threshold the chain has already lost safety; latency canonicity is no weaker than the BFT layer. |
| LN drops out / fails to publish reports | After `MIN_REPORTERS` is no longer met, peer falls back to neutral score (100). Equivalent to "not penalized for absence". Score updates resume when reports come back. |
| Replay of old `LatencyReport` from previous round | Round number in the signature payload prevents replay; observers reject reports outside the current window. |

### 3.9 Bootstrap

For rounds 0..N (where N = `WINDOW_SIZE` = 3), no canonical RTT exists for any peer. The lookup returns `None` and `latency_score` defaults to 100 (neutral).

This is equivalent to "during the first ~30s of a new group's existence, all peers are treated as equally fast". Election proceeds normally; PoU-weighted RR falls back to the other four score components, which are non-zero from epoch start (availability defaults to 1000, integrity defaults to 1000, etc.).

After round N the canonical RTT for each pair starts to populate. By round N + 3 the full table is stable. No bootstrapping flag day is required; the system self-converges.

### 3.10 Test strategy

| Test class | What it proves |
|---|---|
| `signable_bytes_canonical_across_observers` | Two observers with different gossip buffers but the same chain state produce byte-identical `signable_bytes` for the same election. |
| `byzantine_minority_cannot_shift_median` | With n=5 and f=1 Byzantine reporter publishing extreme RTT values, the canonical bucket remains within the honest cluster. |
| `bootstrap_neutral_scores` | In rounds 0..3 of a fresh group, `latency_score` returns 100 for all peers, and election proceeds without error. |
| `falla2_quorum_at_full_strength` | After Phase 1 deployment, election cert acceptance rate ≥ 99% over a 60s window with 5 MN + 10 LN under representative load. |
| `latency_table_reproducibility` | Two masternodes receiving the same gossip buffer produce the same `LatencyTable` at end-of-round. |

Cluster validation: 5 MN + 10 LN, 5min run, 200 senders, 50 concurrency. Acceptance criterion: zero `Falla 2: attestations below BFT quorum` log lines.

---

## 4. Phase 2 — Lattice ordering (architectural sketch)

This section is **not part of Phase 1 deliverable** but specifies enough of Phase 2 to verify Phase 1 compatibility.

### 4.1 Goal

Replace single-proposer-per-slot with parallel batch publication on a per-group DAG (the Lattice), committed in cycles. Resolves blockers (1) and (3).

### 4.2 Lattice structure

Each group `G` runs an independent Lattice instance. At each lattice round `r`, every LN in `G` publishes one cell:

```
cell {
    group_id: G,
    round: r,
    author: peer_id,
    batch: Vec<SignedTx>,
    parents: Vec<CellId>,          // references to round (r-1) cells
    author_signature: [u8; 64],
}
```

A cell becomes part of the Lattice once 2f+1 LN in `G` sign its header (a **cell certificate**). The cell is then "certified". Cells without quorum at round-end are dropped — the corresponding TX return to mempool and may be included in a future cell.

### 4.3 Cycles and cycle pivot

A **cycle** spans two lattice rounds: anchor round `2k` and follow round `2k+1`. The **cycle pivot** for cycle `k` is the LN at slot `(k mod TENURE_BLOCKS)` of the PoU-weighted RR schedule computed in Phase 1 (same helper function, unchanged).

**Lineage commit rule**: at the end of follow round `2k+1`, the cycle commits if and only if 2f+1 certified cells in round `2k+1` carry the pivot's cell at round `2k` in their `parents` set. If so, the pivot's entire causal history (all certified ancestors transitively reachable via `parents`) commits in deterministic topological order (round-major, then author-id lexicographic tiebreak).

If the pivot's round-`2k` cell does not reach the 2f+1 follower threshold by round-`2k+1` end, the cycle skips and waits for the next cycle's pivot. The skipped cycle's cells are subsumed into a later cycle's lineage when the next successful pivot's causal history reaches them.

### 4.4 Multi-group composition

Each group's Lattice produces a stream of committed cells. Masternodes consume these streams across all groups and produce the global block height ordering, using the existing cross-group aggregation layer (unchanged from V0.1).

The cycle commit replaces the per-block `BlockCertificate` of V0.1. Instead of one MN vote per block, MN votes once per cycle commit per group. Voting frequency drops by `block_per_cycle × group_count` ≈ 10–50×.

### 4.5 Compatibility with Phase 1

The PoU-weighted RR helper (`build_weighted_proposer_schedule`) introduced in earlier work is reused unchanged. The only difference is the slot lookup unit:

```rust
// Phase 1 (block-granular)
let slot = (finalized_height % TENURE_BLOCKS) as usize;

// Phase 2 (cycle-granular)
let slot = (cycle_number % TENURE_BLOCKS) as usize;
```

The `LatencyTable` and `latency_score` introduced in Phase 1 are reused at cell-author selection time. No Phase 1 work is invalidated.

### 4.6 Cert broadcast resolution

Under Phase 2, there is no `BlockCertificate` broadcast on a gossipsub topic between MN and LN. The cycle commit happens at the masternode aggregating cell certificates it already has in hand; the LN learns the commit through the next cycle's cell observations (which reference the committed lineage). Blocker (3) is structurally eliminated.

### 4.7 Spike implementation (in tree)

A type-level spike of the Lattice primitives ships alongside this design document. The module `savitri-consensus/src/types/lattice.rs` defines the wire-format types and basic helpers:

- `LatencyRound` / `CycleIndex` / `CellId` / `BatchRoot` — stable identifier types.
- `LatticeCell { round, group_id, author, author_pubkey, parents, batch_root, author_signature }` — one vertex in the DAG. The constructor `with_sorted_parents` enforces canonical parent ordering so `signable_bytes()` is byte-identical across observers.
- `LatticeCell::signable_bytes()` — domain-separated `b"savitri-lattice-cell-v1|..."` payload. Verifies via `verify_author_signature()`.
- `LatticeCell::cell_id()` — blake3 over `signable_bytes()` for stable referencing in parent sets.
- `CellAttestation { signer, signer_pubkey, signature }` — one BFT attestation on a cell. Future work can swap this for a BLS aggregate signature without breaking the API surface.
- `CellCertificate { cell, attestations }` — a cell with `2f+1` attestations attached. Admissible as a parent of subsequent-round cells.
- `Cycle { index, group_id, pivot, pivot_cell, committed_cells }` — one cycle commit decision. Helpers: `anchor_round()`, `follow_round()`, `did_commit()`.
- `lattice_quorum(group_size)` — canonical PBFT-style `2f+1` threshold: `f = (n - 1) / 3`, `quorum = 2f + 1`. Aligned with the existing `savitri_consensus::primitives::quorum::quorum_for_voters`.

Test coverage shipped with the spike:

- `quorum_classic_pbft_values` — sanity check on the PBFT formula at n ∈ {0, 1, 3, 4, 5, 6, 7, 10}.
- `parents_sorted_after_with_sorted_parents` — canonical parent ordering enforced.
- `signable_bytes_observer_independent` — same input → same bytes.
- `cell_id_deterministic` — same cell → same id across observers.
- `signable_bytes_change_with_any_field` — sensitivity to round, group_id, batch_root.
- `signature_round_trip` — Ed25519 sign/verify, plus tamper detection.
- `cycle_helpers` — anchor_round / follow_round / did_commit return correct derived values.

Aggregation rules (cell certificate quorum collection, lineage commit, cycle pivot election from the PoU-weighted RR schedule) live alongside the existing consensus protocols and will be wired in a follow-up issue. This spike module compiles standalone — no existing code path is altered.

### 4.8 Convergence prerequisite — Phase 2 latency table

Phase 1.5's `candidates` field exclusion from `signable_bytes` had a single root cause: per-observer `last_certified_height` divergence drove the `LatencyCanonState` window filter to admit different report sets on each LN, producing different canonical tables.

Phase 2 lands a fix orthogonal to the Lattice spike: the `round` field on `LatencyReport` is now derived from a wall-clock-aligned 10-second bucket (`unix_secs / 10`), not from the local chain head. The publisher and the aggregator share the same `current_wall_clock_bucket()` helper. As long as participating LNs run with their clocks loosely synchronized via NTP — a standard testnet operational assumption — the bucket index is identical cluster-wide.

With the table now byte-canonical, re-inclusion of `candidates` and `proposer_pou_score` in `signable_bytes` becomes a follow-up cleanup commit. The wire format type change (`Vec<(String, u32, u32)>`) introduced in Phase 1.5 already accommodates the integer-only payload.

---

## 5. Migration plan

### 5.1 Phase 1 rollout

Phase 1 ships as a backward-compatible addition:

1. Deploy MN binary with `LatencyTable` aggregation but with the score lookup gated behind `SAVITRI_LATENCY_CANON=1`. Default off.
2. Deploy LN binary with `LatencyProbe` + report publication, gated behind `SAVITRI_LATENCY_CANON=1`. Default off.
3. Observe for 1 week that the canonical table converges as expected and matches local measurements within tolerance.
4. Flip the env var to default on. Election cert `signable_bytes` start including canonical fields.
5. Re-enable full BFT 2f+1 quorum check on election cert (`SAVITRI_FALLA2_DISABLE` removed).

Rollback path: setting `SAVITRI_LATENCY_CANON=0` reverts to local-measurement scoring + excluded-fields signable. The canonical table continues to be computed and stored but is not consulted.

### 5.2 Phase 2 rollout

Phase 2 requires a coordinated upgrade — V0.1 single-proposer blocks and V0.2 cycle commits cannot interleave on the same chain. The migration is a flag day:

1. Build V0.2 binaries with Lattice support and `SAVITRI_CONSENSUS_VERSION=v2` env var.
2. Coordinate cluster-wide restart at a pre-agreed epoch boundary.
3. From the chosen epoch onward, all groups run Lattice. The old `BlockCertificate` path is retired.

A separate issue and design document will cover Phase 2 migration in detail when implementation begins.

---

## 6. Operational notes

### 6.1 Observability

Phase 1 adds the following counters to the existing observability layer:

```
savitri_latency_canon_reports_sent_total{group_id}
savitri_latency_canon_reports_received_total{group_id, reporter}
savitri_latency_canon_reports_dropped_total{group_id, reason}
savitri_latency_canon_table_size{group_id}
savitri_latency_canon_lookups_total{group_id}
savitri_latency_canon_lookups_default_total{group_id}  # MIN_REPORTERS not met
```

A periodic 10s DIAG log line summarizes per-group table coverage:

```
DIAG[latency-canon] group=group_42_0 peers_with_canon=4/5 last_update_round=137 reports_in_window=18
```

### 6.2 Tuning knobs

| Constant | Default | Effect of increase | Effect of decrease |
|---|---|---|---|
| `WINDOW_SIZE` | 3 rounds | Smoother (less responsive to RTT changes); more reports needed for confidence | More reactive; fewer reports per peer; risk of insufficient median samples |
| `MIN_SAMPLES` | 3 | Stricter confidence per report; fewer reports survive filter | Looser; noisy reports survive; median quality degrades |
| `MIN_REPORTERS` | 2 | Stricter quorum on canon; more cells return None default 100 | Looser; smaller cliques can dictate canonical value |
| Probe interval | 1s | Less probe traffic; sparser sliding window | More probe traffic; tighter sliding window |
| Report interval | 10s | Less gossip traffic; slower canonical updates | More gossip traffic; faster updates |

### 6.3 Capacity impact

Phase 1 adds:

- One gossip topic per group with one message per LN every 10s, payload ≈ 50 bytes per peer in group → typically ≤ 500 bytes per LN per 10s.
- One `LatencyTable` field in chain state per group, size ≤ `(n × 16) + 24` bytes per group.

Negligible relative to the existing transaction gossip volume.

---

## 7. Open questions

1. **Window size dynamic vs fixed?** A dynamic window that grows under packet loss could improve canonical quality, at the cost of stale canonicity. Phase 1 ships with fixed `WINDOW_SIZE = 3`; reconsider after one week of canary data.
2. **Treatment of cross-group RTT?** Phase 1 only canonicalizes intra-group RTT. Inter-group communication (LN→MN, MN→MN, cross-group LN→LN) is not standardized. This is sufficient for PoU score (which is computed within a group) but may need extension for future routing optimizations.
3. **Signature scheme upgrade?** Current Ed25519 + 5ms buckets gives canonical signable bytes. If we later move to BLS aggregate signatures (single aggregated signature over all attestations), the LatencyTable lookup is still valid; only the signature aggregation layer changes. Phase 1 is BLS-compatible by design.

---

## 8. Acceptance criteria for Phase 1

This issue is considered complete when, in a 5 MN + 10 LN cluster under representative load for 30 minutes:

1. Every observer in any group computes the same `latency_score(peer)` for any peer in that group at any block height. Verified via debug-log emission of computed scores and post-run diff.
2. Election certificate acceptance rate at the masternode is ≥ 99% (measured as `accepted / total_seen`).
3. No `Falla 2: attestations below BFT quorum` log lines in any masternode over the 30-minute window.
4. The env override `SAVITRI_FALLA2_DISABLE` is removed from the codebase.
5. Test suite includes the five test classes listed in §3.10, all passing in CI.
6. This design document is updated to reflect any deviations from the spec discovered during implementation.
