# Savitri Contracts

Complete framework for smart contracts, governance and Oracle on the Savitri blockchain.

## Overview

Savitri Contracts provides:

- **Smart Contracts Platform**: Runtime environment for contract execution, storage model with Merkle tree, gas metering, parallel execution
- **Governance System**: Decentralized governance system (DAO) with proposals, voting and automatic execution
- **Oracle Framework**: System for external data feeds with signed proofs, TTL, and schema registry
- **Token Standards**: Standard token implementations (SAVITRI-20, SAVITRI-721, SAVITRI-1155)
- **Federated Learning**: Contracts for federated AI model management

## Structure

```
savitri-contracts/
├── src/
│   ├── contracts/          # Smart contracts platform
│   │   ├── base.rs         # BaseContract with standard functions
│   │   ├── call.rs         # Contract calls
│   │   ├── deploy.rs       # Contract deployment
│   │   ├── runtime.rs      # Runtime environment
│   │   ├── storage.rs      # Contract storage (Merkle tree)
│   │   ├── gas.rs          # Gas metering
│   │   ├── standards/      # Token standards (FL, SAVITRI-20)
│   │   └── fl/             # Federated Learning contracts
│   ├── governance/         # Governance system
│   │   ├── proposals.rs    # Proposal management
│   │   ├── voting.rs       # Voting system
│   │   ├── execution.rs    # Proposal execution
│   │   ├── deposit.rs      # Deposit management
│   │   └── vote_token.rs   # Vote token
│   └── oracle/             # Oracle framework
│       ├── feed.rs         # Data feeds (price_feed)
│       ├── schema.rs       # Schema registry
│       ├── proof.rs        # ed25519 proof
│       └── types.rs        # Data sources
├── examples/               # Contract examples
├── tests/                  # Contract tests
└── docs/                   # Documentation
```

## Features

### Smart Contracts

- **BaseContract**: Base contract with standard functions (owner, version, pause/unpause, governance hooks)
- **Runtime**: Execution environment with call stack (max depth 64), gas meter
- **Storage**: Storage model with Merkle tree, SLOAD/SSTORE operations
- **Gas Metering**: Gas calculation for operations (SLOAD, SSTORE, CALL, CREATE)
- **Parallel Execution**: Parallel execution based on dependency graph

### Governance

- **Proposals**: DAO proposal creation and management
- **Voting**: Voting system with vote token (Yes/No/Abstain)
- **Execution**: Automatic execution of approved proposals
- **Deposit**: Deposit management (lock during proposal, unlock if approved, burn if rejected)
- **Quorum**: Minimum 10% quorum of vote tokens
- **Approval**: Minimum 65% approval of Yes votes

### Oracle

- **Feed System**: Typed and versioned feeds (feed_id, schema_id/version)
- **Proof System**: ed25519 proof with domain separation and anti-replay
- **TTL**: Time-to-live with rejection of expired or future data
- **Schema Registry**: Registry to define schemas for feed types
- **Canonical Encoding**: Canonical encoding for determinism

### Token Standards

- **SAVITRI-20**: Fungible token standard (similar to ERC20)
- **SAVITRI-721**: Non-fungible token standard (similar to ERC721)
- **SAVITRI-1155**: Multi-asset token standard (similar to ERC1155)
- **FL Standards**: Federated Learning specific standards

## Usage

### Add to your Cargo.toml

```toml
[dependencies]
savitri-contracts = { version = "0.1.0", path = "../savitri-contracts" }
```

### Enable Features

```toml
[dependencies]
savitri-contracts = { 
    version = "0.1.0", 
    path = "../savitri-contracts",
    features = ["governance", "oracle", "standards"] 
}
```

Available features:
- `governance`: Enable governance system
- `oracle`: Enable Oracle framework
- `standards`: Enable token standards (includes `fl`, `savitri20`)
- `fl`: Enable Federated Learning contracts
- `savitri20`: Enable SAVITRI-20/ERC20 standard

## Examples

See `examples/` for usage examples:

- `examples/voting_contract.rs`: Voting contract
- `examples/oracle_integration.rs`: Oracle integration

## Documentation

Complete documentation available in `docs/`:

- [SMART_CONTRACTS.md](docs/SMART_CONTRACTS.md): Smart contracts guide
- [GOVERNANCE_SYSTEM.md](docs/GOVERNANCE_SYSTEM.md): Governance system
- [VOTING_MECHANISM.md](docs/VOTING_MECHANISM.md): Voting mechanism
- [ORACLE_SYSTEM.md](docs/ORACLE_SYSTEM.md): Oracle system
- [CONTRACT_STANDARDS.md](docs/CONTRACT_STANDARDS.md): Contract standards (FL, SAVITRI-20)
- [DEVELOPMENT_GUIDE.md](docs/DEVELOPMENT_GUIDE.md): Development guide

## License

MIT

## Repositories

- **savitri-contracts**: Smart contracts, governance, Oracle (this repository)
- **savitri-core**: Core library (types, crypto, utilities)
- **savitri-storage**: Storage layer (RocksDB, Merkle tree)
