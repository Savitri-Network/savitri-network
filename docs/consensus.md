# Consensus — Proof-of-Unity (PoU) + BFT finalisation

This document describes how Savitri Network reaches agreement on the
chain. It is intended for protocol developers, integrators, and
auditors who want a high-level model of the moving parts before
diving into the source code.

> Source of truth for behaviour is the code in
> [`savitri-consensus`](../savitri-consensus) and the BFT messaging
> in [`savitri-masternode`](../savitri-masternode). This document
> tracks the design at the level of "what" and "why"; for the "how"
> please follow the inline `///` doc-comments and tests.

---

## 1. Core idea

Savitri separates **block production** from **block finalisation**:

- *Lightnodes* produce blocks. Multiple lightnodes are partitioned
  into deterministic committees called **groups**, and within each
  group exactly one lightnode is the elected *proposer* at any given
  height.
- *Masternodes* finalise blocks. Every masternode runs a classic
  PBFT-style vote on each proposal and emits a
  **block-acceptance certificate** when a quorum of `2f+1` votes
  is reached.

This split lets the network produce many blocks in parallel — one
per group — while keeping a single, verifiable BFT finalisation
gate. It also lets the two roles scale independently: increasing the
number of groups raises throughput, while increasing the masternode
count raises Byzantine fault tolerance.

```
┌─────────────────────────────────────────────────────────────────┐
│                          Group A                               │
│   ┌────┐  ┌────┐  ┌────┐                                       │
│   │ LN │  │ LN │  │ LN │   ←  proposer elected by PoU score    │
│   └────┘  └────┘  └────┘                                       │
│       │      │      │                                          │
│       └──┬───┴──┬───┘                                          │
│          ▼      ▼                                              │
│       block   votes                                            │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
   ┌────────────┐    ┌────────────┐    ┌────────────┐
   │ Masternode │ …  │ Masternode │ …  │ Masternode │
   └────────────┘    └────────────┘    └────────────┘
                          │
                          ▼
              Block-acceptance certificate
                  (≥ 2f+1 signatures)
                          │
                          ▼
                    Finalised block
```

---

## 2. Roles

| Role | Identity | Joins via | Responsibilities |
|---|---|---|---|
| **Lightnode** | Ed25519 keypair | Registration with the masternode set | Maintain mempool, submit transactions, participate in PoU election, propose blocks when elected |
| **Masternode** | Ed25519 keypair, listed in the genesis validator set | Static validator set | Finalise blocks via BFT voting, coordinate group formation, emit block-acceptance certificates |
| **Guardian** | Ed25519 keypair (read-only) | Free-form, no consensus role | Observe and archive the chain |

The masternode set is **static and consensus-critical**: it is fixed
at genesis (or rotated through governance). Lightnodes, by contrast,
can join and leave dynamically; their membership in groups is
recomputed every epoch.

---

## 3. PoU score

Every lightnode carries a per-epoch score in the range `[0, 1000]`
called the **Proof-of-Unity score**. The score is a moving average of
five components:

| Component | Weight | What it measures |
|---|---|---|
| `availability` | 25% | Uptime / reachability over the last `N` heartbeats |
| `latency` | 20% | Median round-trip time to other group members |
| `integrity` | 20% | Past behaviour: missed votes, malformed blocks, conflicts |
| `reputation` | 20% | Long-running average across many epochs |
| `participation` | 15% | Fraction of expected votes / proposals actually delivered |

The new score is computed at every epoch boundary as

```
S_i(t) = a · S_i(t-1) + (1 - a) · components(t)
```

where `a` is a smoothing factor that prevents single-epoch spikes
from dominating the long-term ranking. Both the weights and `a` are
configurable parameters of the consensus crate.

The score is used for:

- **Proposer election** within a group: the next proposer is selected
  in PoU-ranked order, with deterministic tie-breaking by peer id.
- **Group composition**: when groups are reformed, lightnodes are
  partitioned roughly evenly across groups while keeping high-PoU
  nodes spread out.
- **Reward distribution**: per-epoch rewards are weighted by the
  closing score.

A lightnode whose score drops below a threshold is excluded from
proposer rotation until it recovers.

---

## 4. Block production lifecycle

A block goes through five stages:

1. **Election**. Inside a group, a proposer is selected using the PoU
   ranking. The election is observable by every group member (and by
   the masternodes) so the next-proposer is always known one tenure
   ahead.
2. **Build**. The proposer drains transactions from its local mempool
   shard, validates them, packs them into a block of at most
   `MAX_TX_PER_BLOCK` transactions and `MAX_BLOCK_BYTES` bytes, and
   computes the block hash.
3. **Propose**. The proposer broadcasts the block to its group on
   GossipSub; group members forward it to the masternode set.
4. **Finalise (BFT)**. Each masternode validates the block and
   votes. When `2f+1` votes have been collected, a *block-acceptance
   certificate* is built (today this is a vector of Ed25519
   signatures; an aggregated BLS variant is on the roadmap).
5. **Commit**. The certificate is gossiped back to the lightnodes;
   each receiver verifies the quorum and persists the block.

Block production within a group is pipelined: the proposer can
already be building block `N+1` while masternodes are voting on
block `N`. The depth of the pipeline is bounded by an environment
knob to keep the rollback cost limited.

---

## 5. BFT finalisation

The BFT layer is a classic PBFT-style two-phase vote with a
`2f+1` quorum, where `n ≥ 3f + 1` masternodes tolerate at most `f`
faulty participants:

| n | max f | quorum |
|---|---|---|
| 4 | 1 | 3 |
| 5 | 1 | 3 |
| 7 | 2 | 5 |
| 10 | 3 | 7 |

Masternodes vote `Approve` only after they have:

- Verified the block's structural integrity (parent reference, hash,
  signatures, transaction count and size limits).
- Verified the proposer's election certificate for the relevant
  group and tenure.
- Re-played the transactions against the local view of the state to
  confirm consistency.

When `2f+1` `Approve` votes are observed, a designated *aggregator*
masternode emits the block-acceptance certificate. A *backup*
masternode is selected deterministically per-group to publish the
certificate if the aggregator is offline, with a short timeout.

The certificate carries:

- The block hash.
- The list of voter public keys with their signatures (or, in the
  future, a single aggregated BLS signature).
- The election certificate that authorised the proposer.

Any node — masternode, lightnode, or guardian — can verify a
certificate independently and use it as a finality proof.

---

## 6. Safety and liveness

### Safety

- **One block per height per group**: at most one block-acceptance
  certificate can ever be produced for a given `(group_id, height)`,
  because it requires `2f+1` votes from a fixed validator set.
- **Cross-group consistency**: groups produce blocks in parallel but
  every block carries a parent reference into the masternode-shared
  finality DAG, so divergent histories cannot be hidden.
- **Replay protection**: certificates are bound to the issuing
  proposer's tenure window (see
  [`docs/group-formation.md`](group-formation.md)), preventing reuse
  of an old certificate to mint blocks at unrelated heights.

### Liveness

- A group continues producing blocks as long as at least one
  PoU-ranked lightnode is reachable.
- The BFT layer makes progress as long as at least `2f+1`
  masternodes are reachable and synchronised.
- A late-joining masternode requests a synchronisation snapshot
  (`GroupSyncRequest`) from any peer with a fresher view; the rest
  of the cluster keeps making progress in the meantime.

---

## 7. What is *not* in this document

- **Wire formats** — see the relevant crate's `README.md` and
  inline doc-comments.
- **Configuration knobs** — see each binary's TOML config schema.
- **Group formation algorithm** — see
  [`docs/group-formation.md`](group-formation.md).
- **Transaction model and signing** — see
  [`docs/transactions.md`](transactions.md).

The roadmap items that touch consensus directly are tracked under
the `phase: research` label and the `Phase 3 — Research & Scaling`
milestone (see [`ROADMAP.md`](../ROADMAP.md)).
