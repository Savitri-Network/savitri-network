# Getting started

This guide walks you from a freshly cloned repository to a running
local node that produces and finalises blocks.

> **Audience**: Rust developers who want to evaluate the protocol
> locally or contribute to the codebase. Operators planning to run a
> public node should read [`docs/consensus.md`](consensus.md) and
> [`docs/group-formation.md`](group-formation.md) first.

---

## 1. Prerequisites

- **Rust** stable ≥ 1.85. The repository ships a `rust-toolchain.toml`
  so `rustup` will install the right version automatically when you
  enter the directory.
- A C / C++ toolchain:
  - Linux: `build-essential` (Debian/Ubuntu) or the equivalent on your
    distribution.
  - macOS: Xcode Command Line Tools (`xcode-select --install`).
  - Windows: MSVC (Visual Studio Build Tools 2022 or newer).
- Additional system libraries used by transitive dependencies:
  `cmake`, `clang`, `protobuf-compiler`. On Debian/Ubuntu:

  ```bash
  sudo apt-get install -y build-essential pkg-config clang cmake \
    ninja-build libssl-dev zlib1g-dev liblz4-dev libzstd-dev \
    protobuf-compiler git
  ```

---

## 2. Clone and build

```bash
git clone https://github.com/Savitri-Network/savitri-network.git
cd savitri-network

# Type-check the whole workspace — quick sanity test (~3-5 min cold).
cargo check --workspace

# Release build of the three node binaries (slower, ~15-20 min cold).
cargo build --release -p savitri-masternode --features rpc
cargo build --release -p savitri-lightnode  --features rpc
cargo build --release -p savitri-guardian
```

The compiled binaries will be in `target/release/`.

If you only want to play with the lightnode and you don't care about
persistent storage, you can build with the in-memory backend:

```bash
cargo build --release -p savitri-lightnode --no-default-features --features desktop
```

---

## 3. Generate a genesis

The repository ships a placeholder testnet genesis under
`savitri-core/src/core/genesis/genesis_testnet.json` that uses dummy
addresses (`0x000…0001`, …). To produce a genesis with real Ed25519
addresses for your own faucet wallets:

```bash
cd genesis
cargo run --release --bin generate_testnet_genesis
```

This writes three files to the current working directory:

| File | Purpose |
|---|---|
| `genesis_testnet.json` | The genesis block consumed by every node |
| `private_keys_testnet.md` | Human-readable list of faucet keys |
| `faucet_keys_testnet.json` | Machine-readable list for the faucet RPC |

> ⚠ The two key files contain private keys. They are listed in
> `.gitignore` so they will never be committed by accident — but you
> should **never** publish them, fund those wallets with real value,
> or reuse them across environments.

Copy `genesis_testnet.json` somewhere your nodes can read it (for
example `configs/genesis_testnet.json`). To use the genesis built into
the binaries instead, build the dependent crates with
`--features testnet`.

---

## 4. Run a single node (smoke test)

The fastest way to confirm everything builds and runs is to launch a
single lightnode pointing at the bundled testnet genesis. From the
repository root:

```bash
cargo run --release -p savitri-lightnode --features rpc -- \
    --listen-port 4001 \
    --tx-interval-secs 2 \
    --block-interval-secs 10
```

In another terminal, hit the JSON-RPC endpoint:

```bash
curl -s -X POST http://127.0.0.1:8545 \
    -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","method":"chain_getBlockHeight","params":[],"id":1}' \
  | jq
```

You should see a JSON response with a numeric `result` that grows over
time as the node produces blocks.

---

## 5. Run a multi-node local network

A real Savitri deployment splits the work between **masternodes**
(BFT finalisation) and **lightnodes** (block production). The minimum
viable cluster is 4 masternodes plus 4 lightnodes; the recommended
testnet topology is 5 masternodes plus 10–20 lightnodes.

The repository does not ship Docker Compose files for the cluster
yet; the simplest approach is:

1. Generate a shared genesis once (Section 3).
2. Configure each node's TOML with a unique `node_id`, listen port,
   and storage path.
3. Start the masternodes first and wait roughly 60 seconds for the
   BFT mesh to stabilise.
4. Start the lightnodes; they will register with the masternodes,
   the masternodes will form groups, and block production will begin.

The order matters: lightnodes that try to register before the
masternode mesh is up will sit idle until the next group-formation
cycle.

---

## 6. Sending a transaction

Once the cluster is producing blocks, submit a transaction via the
SDK or directly via RPC:

```bash
# Pseudocode — the exact RPC method names ship in savitri-rpc.
curl -s -X POST http://127.0.0.1:8545 \
    -H 'Content-Type: application/json' \
    -d '{
      "jsonrpc": "2.0",
      "method": "tx_sendRawTransaction",
      "params": ["0x<hex-encoded signed tx>"],
      "id": 1
    }'
```

Read [`docs/transactions.md`](transactions.md) for the wire format,
signing rules, and fee model.

---

## 7. Where to go next

- [`docs/consensus.md`](consensus.md) — how PoU + BFT work end-to-end.
- [`docs/group-formation.md`](group-formation.md) — how lightnodes are
  partitioned into groups and how proposers are elected.
- [`docs/transactions.md`](transactions.md) — transaction format,
  signing, fees.
- [`ROADMAP.md`](../ROADMAP.md) — three-phase development plan and
  open contribution opportunities.
- [`CONTRIBUTING.md`](../CONTRIBUTING.md) — branching, commit style,
  review process.
