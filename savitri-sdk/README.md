# Savitri Network SDK

Complete SDK for Savitri Network developers - RPC client, wallet, transaction building, and utilities for interacting with the network.

## Overview

The Savitri SDK provides all the necessary functionality to interact with the Savitri Network:

- **RPC Client**: Communication with Savitri nodes via JSON-RPC
- **Light Client**: Optimized client for light nodes
- **Wallet**: Key management, transaction signing, and account management
- **Transaction Builder**: Transaction construction and signing
- **Contract Client**: High-level smart contract interactions
- **Oracle Client**: Oracle system for external data
- **Governance Client**: Governance system for FL proposals
- **Address Utils**: Address validation and conversion
- **Types**: Public types for development use

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
savitri-sdk = { path = "../savitri-sdk", version = "0.1.0" }
```

Or from git (when available):

```toml
[dependencies]
savitri-sdk = { git = "https://github.com/savitri-network/savitri-sdk", version = "0.1.0" }
```

### Basic Example

```rust
use savitri_sdk::{Wallet, RpcClient, TransactionBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create wallet
    let wallet = Wallet::new();
    println!("Address: {}", wallet.address());

    // Connect to RPC
    let rpc = RpcClient::from_url("http://localhost:8545")?;
    let balance = rpc.get_balance(&wallet.address()).await?;
    println!("Balance: {}", balance);

    // Create and sign transaction
    let tx = TransactionBuilder::new()
        .to("destinatario_address")
        .value(1000)
        .build_and_sign(&wallet)?;

    Ok(())
}
```

## Documentation

- **[Getting Started](docs/GETTING_STARTED.md)** - Complete introductory guide
- **[API Reference](docs/API_REFERENCE.md)** - Full API documentation
- **[Connection Guide](docs/CONNECTION_GUIDE.md)** - Network setup and configuration
- **[Contract Interaction](docs/CONTRACT_INTERACTION.md)** - Smart contract integration
- **[Vote Token Governance](docs/VOTE_TOKEN_GOVERNANCE_GUIDE.md)** - Governance system
- **[Wallet Guide](docs/WALLET_GUIDE.md)** - Wallet management guide
- **[Transaction Building](docs/TRANSACTION_BUILDING.md)** - Transaction construction
- **[Client Types](docs/CLIENT_TYPES.md)** - Available client types
- **[Migration Guide](docs/MIGRATION_GUIDE.md)** - Migration guide
- **[Best Practices](docs/BEST_PRACTICES.md)** - Development best practices

## Features

### RPC Client

```rust
use savitri_sdk::RpcClient;

let client = RpcClient::from_url("http://localhost:8545")?;
let block_number = client.get_block_number().await?;
let balance = client.get_balance(&address).await?;
```

### Wallet

```rust
use savitri_sdk::Wallet;

let wallet = Wallet::new();
let signature = wallet.sign_message(b"message");
wallet.verify_signature(b"message", &signature)?;
```

### Transaction Builder

```rust
use savitri_sdk::{Wallet, TransactionBuilder};

let wallet = Wallet::new();
let tx = TransactionBuilder::new()
    .to("destinatario")
    .value(1000)
    .nonce(1)
    .build_and_sign(&wallet)?;
```

### Contract Client

```rust
use savitri_sdk::{Wallet, ContractClient};

let wallet = Wallet::with_rpc("http://localhost:8545")?;
let contract_client = ContractClient::from_url_and_wallet("http://localhost:8545", wallet)?;

// Oracle system
let oracle = contract_client.oracle();
let tx_hash = oracle.request_data(&oracle_address, "price", b"BTC/USD").await?;

// Governance system
let governance = contract_client.governance();
let tx_hash = governance.create_proposal(&gov_address, "Title", "Description", 604800).await?;
```

## Structure

```
savitri-sdk/
├── src/
│   ├── client/          # RPC, Light Client, Wallet, Contract Client
│   ├── types/           # Public types
│   └── utils/           # Address utils, Transaction builder
├── examples/            # Esempi completi
├── tools/               # Tool sviluppatori
├── docs/                # Documentazione
└── README.md
```

## 🛠️ Tool Sviluppatori

Il SDK include tool CLI:

### Key Generator

```bash
cargo run --bin key_generator
```

Genera nuove chiavi per wallet.

### Transaction Signer

```bash
cargo run --bin transaction_signer
```

Firma transazioni offline.

## 📖 Esempi

Vedi la directory `examples/` per esempi completi:

- `basic_transfer.rs` - Trasferimento base
- `wallet_management.rs` - Gestione wallet
- `quick_start.rs` - Quick start completo
- `contract_interaction.rs` - Interazione contratti generici
- `oracle_integration.rs` - Sistema Oracle completo
- `governance_integration.rs` - Sistema Governance completo
- `light_node_setup.rs` - Setup light node
- `advanced_usage.rs` - Uso avanzato

Esegui esempi:

```bash
cargo run --example basic_transfer
```

## 🧪 Testing

```bash
# Esegui tutti i test
cargo test

# Test specifici
cargo test --lib client::wallet
```

## 📄 Licenza

MIT License - vedi [LICENSE](../LICENSE) per dettagli.

## 🤝 Contribuire

Contributi sono benvenuti! Per contribuire:

1. Fork il repository
2. Creates un branch per la feature
3. Commit le modifiche
4. Push al branch
5. Apri una Pull Request

## 🆘 Supporto

- **Documentazione**: Vedi `docs/`
- **Issues**: https://github.com/savitri-network/savitri-sdk/issues
- **Email**: support@savitrinetwork.com

## 🔗 Links

- **Website**: https://savitrinetwork.com
- **Documentation**: https://docs.savitrinetwork.com
- **GitHub**: https://github.com/savitri-network/savitri-sdk
