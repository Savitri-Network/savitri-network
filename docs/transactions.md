# Transactions

This document describes the on-the-wire transaction format used by
Savitri Network, how transactions are signed and verified, and the
fee model. It targets developers who integrate the chain (wallets,
SDK consumers, custodians, exchanges) and contributors who work on
the mempool / RPC layers.

> Source of truth: the canonical `TransactionExt` type in
> [`savitri-core`](../savitri-core), the admission and prevalidation
> pipeline in [`savitri-mempool`](../savitri-mempool), and the
> JSON-RPC handlers in [`savitri-rpc`](../savitri-rpc).

---

## 1. Lifecycle overview

A transaction goes through six stages:

1. **Build** — the client (SDK or wallet) constructs an unsigned
   `TransactionExt`.
2. **Sign** — the client signs the canonical bytes with the sender's
   Ed25519 secret key.
3. **Submit** — the signed transaction is sent to a node via
   JSON-RPC (`tx_sendRawTransaction`) or via direct gossip on the
   transaction topic.
4. **Admit** — the node prevalidates the transaction (signature,
   nonce, balance, fee floor) and stores it in the local mempool
   shard.
5. **Include** — the next group proposer drains a batch of
   transactions from the mempool and packs them into a block.
6. **Finalise** — once the block is approved by the BFT layer
   (`2f+1` quorum), the transactions become canonical: their state
   updates are persisted and the block-acceptance certificate is
   propagated.

After step 6 the transaction is *final*; there is no probabilistic
reorg window.

---

## 2. Transaction format

The canonical wire type is `TransactionExt`. Its fields, in
serialisation order:

| Field | Type | Description |
|---|---|---|
| `from` | `String` (32-byte address, hex) | Sender address |
| `to` | `String` (32-byte address, hex) | Recipient address (or contract) |
| `amount` | `u64` | Native-token amount in base units (1 SAVI = 10^18 wei) |
| `nonce` | `u64` | Sender-local counter, monotonically increasing |
| `fee` | `u64` | Optional fee in base units; `0` is permitted only when the network is configured for a fee-free testnet |
| `data` | `Vec<u8>` | Optional payload (for contract calls / arbitrary data) |
| `signature` | `[u8; 64]` | Ed25519 signature over the canonical signable bytes |

Encoding:

- Wire format: `bincode` with **fixed-int** integer encoding and
  `serde_big_array` for the 64-byte signature. This produces a
  deterministic byte string that does not depend on integer values.
- JSON-RPC format: standard `serde_json` with hex-encoded byte fields
  (`from`, `to`, `signature`).

Two wire encodings exist intentionally: the binary form is what
nodes exchange via gossip and what the mempool stores; the JSON form
is what wallets and dApps see when they call the RPC. The conversion
between the two is deterministic and lossless.

---

## 3. Signing

The signature covers a canonical *signable bytes* prefix that
includes a domain-separation tag and the chain identifier. In
pseudocode:

```text
signable = "savitri-tx-v1" ‖ chain_id ‖ from ‖ to ‖ amount
           ‖ nonce ‖ fee ‖ data
signature = ed25519_sign(secret_key, signable)
```

Implementation notes:

- The version tag (`savitri-tx-v1`) provides domain separation
  against future signable formats. A `v2` tag is on the roadmap to
  introduce additional fields without breaking older signers.
- Including `chain_id` prevents replay across networks (mainnet vs.
  testnet vs. devnet).
- The signing key MUST match the `from` field: nodes reject any
  transaction where `verify(from_pubkey, signable, signature)`
  fails.

The reference implementation lives in
[`savitri-sdk`](../savitri-sdk); third-party libraries should
mirror it byte-for-byte.

---

## 4. Nonce model

The nonce is a per-sender counter that prevents replay and orders a
sender's transactions deterministically. Rules:

- A fresh account starts at `nonce = 0`.
- Each accepted transaction increments the sender's stored nonce
  by 1.
- Submitted transactions with `nonce <= committed_nonce` are
  rejected as replays.
- A submitted transaction with `nonce > committed_nonce + GAP` is
  rejected with a "nonce gap too large" error; the gap bound is a
  configurable parameter (`ADMISSION_MAX_MAIN_POOL_NONCE_GAP`).
- A transaction with `committed_nonce < nonce <= committed_nonce + GAP`
  is admitted into the *queued pool* and promoted to the main pool
  as soon as the nonces of preceding transactions are observed.

Wallets are expected to track their own next-nonce locally and submit
in order. The RPC endpoint `account_getNonce` returns the current
committed nonce for an address.

---

## 5. Fee model

Savitri uses a simple flat-fee model in this release. Each transaction
carries an explicit `fee` in base units. Nodes enforce two checks at
admission time:

- `balance(from) >= amount + fee` — the sender must be able to pay
  both the transfer and the fee.
- `fee >= MIN_FEE` — a configurable network parameter. On testnet
  this floor is typically zero; on mainnet it is set by governance.

The fee is paid to the proposer that includes the transaction in a
block, with a configurable share burned to control inflation. The
detailed fee distribution is documented in the per-crate `README.md`
of [`Savitri-contracts`](../Savitri-contracts).

A more sophisticated fee market (priority fee + base fee, EIP-1559
style) is on the roadmap and will be introduced behind a feature
flag.

---

## 6. Submitting a transaction

### Via SDK

```rust
use savitri_sdk::client::SavitriClient;
use savitri_sdk::wallet::Wallet;

let wallet  = Wallet::from_secret_hex(&secret_hex)?;
let client  = SavitriClient::new("http://localhost:8545")?;
let chain   = client.chain_id().await?;
let nonce   = client.get_nonce(&wallet.address()).await?;

let tx = wallet
    .build_transfer(&recipient, amount)
    .with_nonce(nonce)
    .with_fee(min_fee)
    .with_chain_id(chain)
    .sign();

let hash = client.send_raw_transaction(&tx.to_bytes()).await?;
```

### Via JSON-RPC

```bash
curl -s -X POST http://localhost:8545 \
    -H 'Content-Type: application/json' \
    -d '{
      "jsonrpc": "2.0",
      "method": "tx_sendRawTransaction",
      "params": ["0x<bincode-serialised hex>"],
      "id": 1
    }'
```

Successful submission returns the transaction hash. Use
`tx_getReceipt` to poll for inclusion. The transaction is final once
the receipt's enclosing block carries a verified block-acceptance
certificate.

---

## 7. Admission errors

The mempool may reject a transaction during prevalidation. The most
common error codes:

| Code | Meaning |
|---|---|
| `InvalidSignature` | Signature does not verify against the sender's public key |
| `NonceTooLow` | Nonce is at or below the sender's committed nonce |
| `NonceGapTooLarge` | Nonce is too far ahead of the committed nonce |
| `InsufficientBalance` | `balance(from) < amount + fee` |
| `FeeBelowFloor` | `fee < MIN_FEE` for the current network |
| `Duplicate` | A transaction with the same hash is already in the pool |
| `MempoolFull` | The mempool shard for this sender is full |
| `MalformedTransaction` | Wire decoding failed |

Error responses are stable across releases; SDKs should treat unknown
codes as transient and retry with backoff.

---

## 8. Determinism

The on-chain effect of a transaction is fully determined by:

1. The transaction's canonical signable bytes.
2. The signer's public key (recovered from `from`).
3. The pre-state of the chain at the height of inclusion.

Nodes that disagree on the resulting state diverge and are detected
at the next state-root commitment, which is part of every block
header. Operators can compare state roots across nodes to confirm a
healthy cluster.

---

## 9. Where to go next

- [`docs/getting-started.md`](getting-started.md) — running a local
  node and submitting your first transaction end-to-end.
- [`docs/consensus.md`](consensus.md) — how transactions become final.
- [`Savitri-contracts/README.md`](../Savitri-contracts/README.md) —
  smart-contract execution and the on-chain fee distribution.
- [`savitri-sdk/README.md`](../savitri-sdk/README.md) — client-side
  API reference and code examples.
