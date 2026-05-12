# Group formation and proposer election

This document describes how lightnodes are partitioned into committees
("groups") and how a single proposer is chosen inside each group at
any given height. It is the companion to
[`docs/consensus.md`](consensus.md), which gives the high-level model.

> Source of truth: the `GroupFormationManager` in
> [`savitri-masternode`](../savitri-masternode) and the
> `IntraGroupCommunication` module in
> [`savitri-lightnode`](../savitri-lightnode).

---

## 1. Why groups?

Producing one block at a time across the whole network would cap
throughput at the slowest validator's block time. Savitri instead
partitions the lightnode set into **groups**, each of which produces
its own stream of blocks in parallel. The masternode BFT layer
finalises blocks from every group through the same gate, so the
network's overall finality stays single-threaded but the *production*
side scales horizontally.

Each group has:

- A **stable identity** (`group_id`) that survives small membership
  perturbations.
- A **deterministic membership** agreed by the masternode set.
- A **single proposer at a time**, rotating among members in
  PoU-ranked order.
- An on-chain **leader masternode** and **backup masternode** that
  publish block-acceptance certificates.

---

## 2. The epoch

Time is sliced into fixed-size **epochs** (`SLOTS_PER_EPOCH` heartbeats
per epoch). All membership changes happen on epoch boundaries:

- Lightnodes that registered during the previous epoch are added to
  the active pool.
- Lightnodes that have been silent past the inactivity threshold are
  evicted.
- The masternode set runs the group formation protocol once per epoch.

Inside an epoch, group membership is **immutable**. This avoids the
chaotic case where a lightnode is in different groups on different
masternodes' views.

---

## 3. Group formation protocol

The protocol runs once per epoch and is driven by the masternode set.
The high-level flow:

1. **Coordinator selection**. Every masternode computes the same
   coordinator deterministically as `validators[epoch % len]`, where
   `validators` is the static, sorted list of masternode IDs from
   genesis. All masternodes therefore agree on who proposes the new
   group layout for the upcoming epoch.

2. **Proposal**. The coordinator drains the *available lightnodes*
   pool and partitions it into groups of size between
   `min_group_size` and `max_group_size`. Each group is assigned a
   stable `group_id`, a leader masternode, and a backup masternode.
   The coordinator wraps the layout in a `GroupProposal` and
   broadcasts it on the dedicated gossip topic.

3. **Vote**. Every other masternode validates the proposal:
   - Lightnodes referenced exist in the registry.
   - Group sizes are within bounds.
   - There is no excessive overlap (≥ 80% novelty) with the previous
     epoch's groups.
   - Proposer-as-leader assignment is consistent with the masternode
     ordering rule.

   On success, each masternode emits a `GroupVote::Approve`.

4. **Certificate**. When a `2f+1` quorum of approvals is reached, a
   `GroupApprovalCertificate` is built and broadcast. From that
   moment forward every node treats the proposed groups as the
   active layout for the new epoch.

5. **Distribution**. Lightnodes receive the certificate, learn their
   group membership, and start gossiping on the per-group topics.

If the coordinator is unreachable for a configurable timeout, the
masternodes fall back to a leader-election dance over a separate
gossip topic. The fallback path is designed to converge to the same
result as the deterministic coordinator path — it just takes longer.

---

## 4. Proposer election inside a group

Once a group is active, its members elect the **proposer** for the
next *tenure* (a window of `PROPOSER_TENURE_BLOCKS` blocks).

The election is local to the group:

1. Every member emits a signed `ProposerElectionResult` carrying:
   - The group id.
   - The PoU ranking of the candidates.
   - The current finalised height (`tenure_start_height`).
2. After enough results have been observed, each member builds an
   `ElectionCertificate` aggregating the attestations. The candidate
   with the highest PoU score wins, with deterministic tie-breaking.
3. The winner becomes the proposer for the next `PROPOSER_TENURE_BLOCKS`.
4. Block proposals carry a copy of the election certificate so that
   masternodes can verify the proposer's authority before voting on
   their blocks.

The election is **deterministic given the same input**: every member
that observes the same attestation set elects the same proposer.

### Tenure boundaries

A proposer's tenure ends after `PROPOSER_TENURE_BLOCKS` finalised
blocks. The election runs at the end of the previous tenure so that
the next proposer can start producing blocks immediately, with no
gap. If the elected proposer fails to produce, the group falls back
to the next-ranked candidate after a short timeout.

---

## 5. Election certificate

The `ElectionCertificate` is a small structure that any node can
verify against a group's current membership:

```text
ElectionCertificate {
    group_id:                 String,
    election_round:           u64,
    elected_proposer_peer_id: String,
    elected_proposer_pubkey:  [u8; 32],
    proposer_pou_score:       u32,
    timestamp:                u64,
    candidates:               Vec<(peer_id, pou_score, combined_score)>,
    attestations:             Vec<ElectionAttestation>,
    tenure_start_height:      u64,
}
```

`attestations` carries one Ed25519 signature per attesting group
member. The `tenure_start_height` field binds the certificate to a
specific window of finalised heights, so a leaked certificate cannot
be reused outside its tenure.

The **block-acceptance** path verifies that:

- The proposed block's `proposer_group_id` matches the certificate's
  group id.
- The proposed block is signed by the elected proposer's pubkey.
- The block height falls inside the tenure window
  `[tenure_start_height, tenure_start_height + PROPOSER_TENURE_BLOCKS)`.
- The certificate carries at least `2f+1` valid attestations from
  members of the group's roster.

Any failure here causes the masternode to drop the proposal silently
without voting on it.

---

## 6. Membership churn

The protocol is designed to keep churn low. Specifically:

- Lightnodes that come online late are deferred to the next epoch
  rather than being slotted into the current one.
- A small number of inactive members in a group are tolerated; only
  when the active majority drops below a threshold does the group
  transition to a *Dissolving* state and its members are returned
  to the available pool.
- Group identifiers are stable across small perturbations — adding or
  removing a single member rarely changes the `group_id`.

---

## 7. Failure modes and recovery

| Symptom | Likely cause | Recovery |
|---|---|---|
| New masternode reports zero `Active` groups | Missed certificate broadcasts during boot | The masternode automatically issues a `GroupSyncRequest` to peers; certificates are replayed in epoch order |
| Group has no proposer | Elected proposer offline or below PoU threshold | After a short timeout the group falls back to the next-ranked PoU candidate |
| BFT quorum not reached on a `GroupProposal` | Network partition or insufficient masternodes online | The proposal expires; the next coordinator (next epoch) tries again |
| Groups contain stale members | Late lightnode departures | Detected at the next epoch's `cleanup_inactive` pass; the affected groups are reformed |

The intent is that the steady state is "boring" — groups are stable,
proposers rotate quietly through their tenures, certificates flow
back into the masternode set, and the chain advances at the
configured cadence.

---

## Related crates

- [`savitri-consensus`](../savitri-consensus/README.md) - group
  membership, election, and certificate data structures.
- [`savitri-lightnode`](../savitri-lightnode/README.md) - lightnode
  participation in group formation and proposer rotation.
