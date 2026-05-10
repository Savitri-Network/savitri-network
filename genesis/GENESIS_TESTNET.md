# Testnet genesis — Savitri Network

A simplified genesis for **testnet** use: no mainnet tokenomics (no vesting,
reduced supply).

## Differences vs. mainnet

| Aspect    | Mainnet                       | Testnet                  |
|-----------|-------------------------------|--------------------------|
| Supply    | 230M SAVI                     | 20 SAVI (configurable)   |
| Wallets   | 7 (4 vesting + 3 liquid)      | 3 (all liquid)           |
| Vesting   | Yes (team, investors, …)      | No                       |
| Use       | Production                    | Tests, dev, faucet       |

## Files and generation

- **`genesis_testnet.json`** (under `savitri-core/src/core/genesis/`): default
  genesis with placeholder addresses `0x00…01`, `0x00…02`, `0x00…03` and a
  10 + 5 + 5 SAVI distribution. Used when the crate is built with the
  `testnet` feature.
- **Generator**: to produce real Ed25519 addresses and a genesis with your
  own keys:

  ```bash
  cd genesis
  cargo run --bin generate_testnet_genesis
  ```

  The generator writes (to the current working directory):

  - `genesis_testnet.json`         — the genesis JSON
  - `private_keys_testnet.md`      — Markdown listing of faucet keys
  - `faucet_keys_testnet.json`     — JSON listing of faucet private keys

  ⚠ The two key files contain private keys. Keep them out of git and any
  public location. The repository `.gitignore` excludes them by default.

You can copy `genesis_testnet.json` into
`savitri-core/src/core/genesis/genesis_testnet.json` if you want to use your
own addresses instead of the placeholders.

## Using the testnet genesis from code

### Build with the testnet genesis

In any crate that depends on `savitri-core` (a node or guardian, for
example), enable the feature:

```toml
# Cargo.toml
savitri-core = { path = "../savitri-core", features = ["testnet"] }
```

or from the command line:

```bash
cargo build --features testnet
```

When the `testnet` feature is enabled:

- `genesis_testnet.json` is bundled instead of `genesis.json`;
- `ensure_genesis_block` / `initialize_genesis_mint` skip vesting setup and
  use the sum of the genesis transactions as the total supply.

### Deploy a testnet (script / package layout)

For deployment scripts, use the generated genesis as the shared genesis:

1. Generate: `cargo run --bin generate_testnet_genesis` from the workspace
   root (or from `genesis/`).
2. Copy `genesis_testnet.json` to your package's shared genesis path
   (for example `shared/genesis.json`), or configure each node to load the
   file via a CLI flag such as `--genesis configs/genesis_testnet.json`.

## Testnet genesis JSON layout

- **version**: `1`
- **timestamp**: `1700000000` (or any chosen value)
- **proposer**: 32-byte hex address of the genesis proposer
- **transactions**: a list of mint transactions from `0x00…00` to N wallets;
  the `amount` is expressed in the smallest unit (1 SAVI = 10¹⁸) and must
  fit in a `u64` (so at most ~18 SAVI per transaction when using 18 decimal
  places).

Example transactions (3 wallets, 20 SAVI total):

- Treasury / Proposer: 10 SAVI
- Dev: 5 SAVI
- Faucet: 5 SAVI

No vesting: everything is spendable immediately.

## Summary

1. **Tests only**: enable the `testnet` feature and rely on the built-in
   `genesis_testnet.json` shipped with `savitri-core` (placeholder
   addresses).
2. **Testnet with your own keys**: run `generate_testnet_genesis`, replace
   `savitri-core/src/core/genesis/genesis_testnet.json` if needed, and
   build with `--features testnet`.
3. **Package / script**: point every node to the same generated
   `genesis_testnet.json` (or a copy of it placed at e.g.
   `shared/genesis.json`) so the whole testnet shares one genesis.
