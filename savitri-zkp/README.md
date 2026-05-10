# savitri-zkp

Zero-knowledge proof library for the Savitri Network, supporting multiple backends: mock (testing), Arkworks/Groth16 (production), and PLONK/halo2.

## Overview

`savitri-zkp` provides a unified interface for zero-knowledge proof generation and verification across the Savitri blockchain. The crate abstracts over multiple ZKP backends through the `ZkVerifier` trait, allowing nodes to select the appropriate backend based on their deployment context.

The crate is intentionally independent of `savitri-core` to avoid circular dependencies in the crate graph. It sits at the leaf of the dependency hierarchy and is consumed by `savitri-consensus` and the node binaries.

A key safety property: when a production backend (Plonk or Arkworks) is requested but its feature flag is not enabled, the `create_verifier()` function panics rather than silently falling back to MockVerifier. This prevents accidentally running production nodes with mock proof verification.

## Features

- **Mock Backend**: Accepts all proofs unconditionally. Used for development and testing only.
- **Arkworks Backend**: Production-grade Groth16 proofs using the `ark-bn254` curve (BN254/alt_bn128). Supports batch verification.
- **PLONK Backend**: Alternative production backend using halo2 with BN256 curves.
- **Monolith Proofs**: ZKP circuits for monolith (checkpoint) verification.
- **Configurable Limits**: Maximum proof size and verification timeout settings per environment.
- **Batch Verification**: All backends implement `batch_verify()` for processing multiple proofs efficiently.

## Feature Flags

| Feature | Description |
|---------|-------------|
| `mock` | Mock verifier (accepts all proofs) |
| `arkworks` | Groth16 backend via ark-bn254, ark-groth16 |
| `plonk` | PLONK backend via halo2_proofs, halo2curves |
| `circom` | Circom circuit compatibility (requires `arkworks`) |
| `production` | Alias for `arkworks` |
| `advanced` | Enables both `arkworks` and `plonk` |
| `all_backends` | Enables both `arkworks` and `plonk` |

No default features are enabled. Consumers must opt in explicitly.

## Usage

```rust
use savitri_zkp::{ZkpConfig, ZkpBackend, create_verifier};
use savitri_zkp::{ZkVerifier, ZkProof, Statement};

// Development: mock backend (accepts all proofs)
let config = ZkpConfig::development();
let verifier = create_verifier(config);

// Production: Arkworks/Groth16 backend
let config = ZkpConfig::production();
let verifier = create_verifier(config);

// Verify a proof
let result = verifier.verify(&statement, &proof)?;

// Batch verify
let results = verifier.batch_verify(&statements, &proofs)?;
```

### Configuration Presets

```rust
// Development (mock, 1MB max proof, 5s timeout)
let config = ZkpConfig::development();

// Production (Arkworks, 4MB max proof, 15s timeout)
let config = ZkpConfig::production();

// Testing (mock, 512KB max proof, 1s timeout)
let config = ZkpConfig::testing();
```

## Building

```bash
# Build with mock backend (testing)
cargo build -p savitri-zkp --features mock

# Build with Arkworks/Groth16 (production)
cargo build -p savitri-zkp --features arkworks

# Build with PLONK/halo2
cargo build -p savitri-zkp --features plonk

# Build with all backends
cargo build -p savitri-zkp --features all_backends
```

## Testing

```bash
cargo test -p savitri-zkp
cargo test -p savitri-zkp --features arkworks
cargo test -p savitri-zkp --features plonk
```

## Architecture

```
src/
  lib.rs                  -- ZkpBackend enum, ZkpConfig, create_verifier() factory
  zkp.rs                  -- ZkProof and Statement types
  verifier.rs             -- ZkVerifier trait, MockVerifier, PlonkVerifier, ArkworksVerifier
  prover.rs               -- Proof generation logic
  keys.rs                 -- Proving and verification key management
  monolith.rs             -- Monolith-specific ZKP operations
  tests.rs                -- Unit tests
  circuits/
    mod.rs                -- Circuit module re-exports
    monolith_circuit.rs   -- Monolith verification circuit
    plonk_circuit.rs      -- PLONK-specific circuit definitions
```

### Key Types

- `ZkVerifier` (trait): `verify(&Statement, &ZkProof) -> Result<bool>` and `batch_verify()`
- `ZkpBackend` (enum): `Mock`, `Plonk`, `Arkworks`
- `ZkpConfig`: Backend selection, max proof size, verification timeout
- `ZkProof`: Serializable proof container
- `Statement`: Public inputs for proof verification
- `MockVerifier`: Always returns true (testing only)
- `PlonkVerifier`: halo2-based verifier (requires `plonk` feature)
- `ArkworksVerifier`: Groth16/BN254 verifier (requires `arkworks` feature)

## Dependencies

Core:
- `sha2`, `sha3`, `blake3` for hashing
- `serde`, `bincode` for serialization
- `rand` for randomness

Arkworks backend (`arkworks` feature):
- `ark-bn254`, `ark-ec`, `ark-ff`, `ark-serialize`, `ark-relations`, `ark-groth16`

PLONK backend (`plonk` feature):
- `halo2curves`, `halo2_proofs`

## License

Apache-2.0
