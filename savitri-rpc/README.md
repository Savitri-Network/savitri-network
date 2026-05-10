# savitri-rpc

JSON-RPC 2.0 HTTP API for Savitri Network lightnodes and masternodes, built on axum.

## Overview

`savitri-rpc` implements the public-facing RPC interface for Savitri blockchain nodes. All communication uses the JSON-RPC 2.0 protocol over HTTP POST, with a single endpoint at `/` (or `/rpc`). The server provides access to chain state, transaction submission, account queries, mempool status, PoU consensus data, and token information.

The crate is designed to be embedded into both lightnode and masternode binaries. It accepts shared references to storage and mempool backends through the `RpcState` struct, which supports builder-pattern configuration for optional features like PoU readers and testnet faucet wallets.

The server does not terminate TLS. Production deployments should place a TLS-terminating reverse proxy (nginx, Caddy, HAProxy) in front of the RPC endpoint.

## Features

- **JSON-RPC 2.0**: Full protocol compliance with standard error codes
- **7 Method Namespaces**: `chain_*`, `tx_*`, `account_*`, `net_*`, `mempool_*`, `pou_*`, `token_*`, plus `savitri_*` protocol utilities
- **Global Rate Limiting**: Sliding-window rate limiter with configurable max requests per window
- **CORS Support**: Configurable cross-origin resource sharing via `tower-http`
- **Faucet**: Built-in testnet faucet with 10 round-robin wallets, Ed25519 key management, and claim serialization to prevent race conditions
- **Error Sanitization**: Internal error details are stripped before returning to clients to prevent information leakage

## RPC Methods

### Chain (`chain_*`)

| Method | Description |
|--------|-------------|
| `chain_getBlock` | Get block by height |
| `chain_getBlockByNumber` | Get block by number |
| `chain_getBlockByHash` | Get block by hash |
| `chain_getLatestBlock` | Get the latest block |
| `chain_getBlockHeight` | Get current block height |
| `chain_getChainInfo` | Get chain metadata |

### Transaction (`tx_*`)

| Method | Description |
|--------|-------------|
| `tx_sendTransaction` | Submit a signed transaction |
| `tx_getTransaction` | Get transaction by hash |
| `tx_getTransactionReceipt` | Get transaction receipt |
| `tx_getTransactionsByBlock` | List transactions in a block |
| `tx_getPendingTransactions` | List pending mempool transactions |

### Account (`account_*`)

| Method | Description |
|--------|-------------|
| `account_getBalance` | Get account balance |
| `account_getNonce` | Get account nonce |
| `account_getAccount` | Get full account state |
| `account_getTokenBalance` | Get token balance for an account |

### Network (`net_*`)

| Method | Description |
|--------|-------------|
| `net_version` | Network version |
| `net_peerCount` | Connected peer count |
| `net_listening` | Whether node is listening |
| `net_peers` | Peer list |
| `net_nodeInfo` | Node information |

### Mempool (`mempool_*`)

| Method | Description |
|--------|-------------|
| `mempool_getSize` | Current mempool size |
| `mempool_getPendingTransactions` | Pending transactions |
| `mempool_getTransactionStatus` | Transaction status in mempool |

### PoU Consensus (`pou_*` / `savitri_pou*`)

| Method | Description |
|--------|-------------|
| `pou_getValidators` | Active validator set |
| `pou_getStakeInfo` | Staking information |
| `pou_getEpochInfo` | Current epoch details |
| `pou_getConsensusState` | Local PoU consensus state |
| `savitri_pouPeers` | PoU scores for connected peers |
| `savitri_pouGroups` | Active consensus groups |
| `savitri_pouMasternodes` | Masternode PoU data |
| `savitri_pouGroupNodes` | Nodes in a specific group |

### Token (`token_*`)

| Method | Description |
|--------|-------------|
| `token_getTokenInfo` | Token metadata |
| `token_getTokenBalance` | Token balance |
| `token_getTokenTransfers` | Token transfer history |

### Protocol (`savitri_*`)

| Method | Description |
|--------|-------------|
| `savitri_protocolVersion` | Protocol version |
| `savitri_syncing` | Sync status |
| `savitri_gasPrice` | Current gas price |
| `savitri_estimateGas` | Gas estimation |
| `savitri_health` | Node health check |
| `savitri_faucetClaim` | Testnet faucet claim |
| `savitri_getRewards` | Account reward summary, including raw group-check reward totals |
| `savitri_getRewardHistory` | Paginated per-block reward history |
| `savitri_getMonolithHead` | Latest monolith head |
| `savitri_getMonolithsForRange` | Monoliths in a height range |
| `savitri_getMonolith` | Get specific monolith |

#### Reward RPCs

`savitri_getRewards` returns account balance data plus aggregate rewards:

```json
{
  "jsonrpc": "2.0",
  "method": "savitri_getRewards",
  "params": ["YOUR_ADDRESS_HEX"],
  "id": 1
}
```

Response fields:

| Field | Description |
|-------|-------------|
| `reward_balance` | Current reward balance in raw units |
| `total_rewards` | Total reward ledger amount in raw units |
| `group_check_rewards` | Total raw units earned from block-checking rewards |

`savitri_getRewardHistory` returns paginated per-block reward entries:

```json
{
  "jsonrpc": "2.0",
  "method": "savitri_getRewardHistory",
  "params": {
    "address": "YOUR_ADDRESS_HEX",
    "offset": 0,
    "limit": 10
  },
  "id": 1
}
```

Entries with `"reward_type": "group_check"` are block-checking rewards credited to non-proposer group members when a certified block is committed. Amounts are raw units. With 18 decimals, `1 SAVT = 1_000_000_000_000_000_000` raw units, so each `group_check` reward is `1 SAVT`.

## Usage

```rust
use std::sync::Arc;
use tokio::sync::Mutex;
use savitri_rpc::{RpcState, RpcConfig, run_server};

// Create RPC state for a lightnode
let state = RpcState::new(storage, mempool);

// Or for a masternode
let state = RpcState::for_masternode(pou_reader, Some(storage));

// Configuration
let config = RpcConfig {
    enabled: true,
    bind_addr: "127.0.0.1".into(),
    port: 8545,
};
```

## Building

```bash
cargo build -p savitri-rpc
```

## Testing

```bash
cargo test -p savitri-rpc
```

## Architecture

```
src/
  lib.rs       -- RpcState, RpcConfig, FaucetConfig, public re-exports
  server.rs    -- Axum router setup, global rate limiter middleware, CORS, TLS notes
  jsonrpc.rs   -- JSON-RPC 2.0 protocol types, method dispatcher, error mapping
  handlers.rs  -- Internal data-access functions (storage/mempool queries)
  types.rs     -- Wire format types for serialization (BlockWire, TransactionWire, AccountWire)
  pou.rs       -- PouReader and MasternodePouReader traits, group info types
```

## Configuration

Default bind address is `127.0.0.1:8545`. The server listens for JSON-RPC 2.0 POST requests only. Rate limiting is applied globally across all endpoints.

Key configuration fields in `RpcConfig`:
- `enabled`: Whether to start the RPC server
- `bind_addr`: IP address to bind to
- `port`: TCP port (default 8545)

## Dependencies

- `axum` 0.7 with JSON support
- `tower-http` for CORS and tracing
- `savitri-storage` for chain state queries
- `savitri-mempool` for transaction submission and mempool queries
- `savitri-core` for core types
- `ed25519-dalek` for faucet wallet signing
- `dashmap` for concurrent state

## License

Apache-2.0
