# savitri-consensus

Shared consensus library for Savitri blockchain nodes, implementing BFT voting, Proof-of-Unity (PoU) scoring, group-aware consensus, and DAG-based block management.

## Overview

`savitri-consensus` provides the consensus engine used by both masternodes and lightnodes in the Savitri Network. It defines the core types, validation logic, and protocol implementations that govern how blocks are proposed, voted on, and finalized across the network.

The crate supports multiple consensus modes through feature flags, allowing nodes to operate with different levels of participation. Masternodes use the full BFT + group-aware consensus path, while lightnodes use the lightweight PoU-based protocol for resource-constrained environments.

The library integrates with `savitri-zkp` for zero-knowledge proof verification during block validation, using the Arkworks backend by default.

## Features

- **BFT Consensus**: Byzantine Fault Tolerant voting with 2f+1 quorum (67% threshold)
- **Proof-of-Unity (PoU) Scoring**: Exponentially weighted scoring across availability, latency, integrity, reputation, and participation components. Score formula: `S_i(t) = a * S_i(t-1) + (1-a) * (weighted components)`, scaled to 0-1000
- **Group-Aware Consensus**: Proposer selection within dynamically formed node groups
- **DAG Support**: Multi-parent block headers with conflict detection via `DAGManager`
- **Block Validation**: Parallel signature verification, score validation, and adaptive validation
- **Slashing**: Configurable slashing conditions for misbehaving validators
- **Merkle Proofs**: Cryptographic hash chains and Merkle tree construction for state verification
- **ZKP Integration**: Optional zero-knowledge proof verification via `savitri-zkp`

## Feature Flags

| Feature | Description |
|---------|-------------|
| `std` (default) | Standard library support |
| `zkp` (default) | ZKP verification integration |
| `group-aware` | Group-based consensus with proposer selection |
| `pou-based` | PoU scoring and lightweight consensus |
| `bft` | Byzantine Fault Tolerant voting protocol |
| `lightweight` | Reduced-resource consensus for mobile/light nodes |
| `full` | Enables `group-aware` + `pou-based` + `bft` |

## Usage

```rust
use savitri_consensus::{PouScore, POU_SCORE_MAX, ValidationResult};
use savitri_consensus::protocols::group_aware::GroupAwareConsensus;

// PoU scores are u16 values in the range 0-1000
let score: PouScore = 850;
assert!(score <= POU_SCORE_MAX);

// Validation results
let result = ValidationResult::Valid;
assert!(result.is_valid());
```

## Building

```bash
# Build with default features (std + zkp)
cargo build -p savitri-consensus

# Build with full consensus features
cargo build -p savitri-consensus --features full

# Build lightweight mode for lightnodes
cargo build -p savitri-consensus --features "pou-based,lightweight"
```

## Testing

```bash
cargo test -p savitri-consensus
```

## Architecture

### Module Structure

```
src/
  lib.rs                 -- Core types (PouScore, ValidationResult, BlockHeader), re-exports
  serialization.rs       -- Binary serialization helpers
  slashing.rs            -- Slashing condition definitions
  error.rs               -- ConsensusError type and Result alias
  crypto/
    mod.rs               -- Cryptographic primitives
    hashes.rs            -- Hash functions (blake3, sha2)
    merkle.rs            -- Merkle tree construction and verification
    signatures.rs        -- Ed25519 signature operations
  dag/                   -- DAG block management
    (DAGManager, ConflictDetector, DAGConfig, BranchInfo, Conflict)
  types/
    mod.rs               -- Re-exports
    block.rs             -- Block-level types
    consensus.rs         -- Consensus state types
    proposal.rs          -- Block proposal types
    score.rs             -- PoU score types and calculations
    slashing.rs          -- Slashing event types
    validation.rs        -- Validation result types
  validation/
    mod.rs               -- Re-exports
    block_validator.rs   -- Block structure and hash validation
    signature_validator.rs -- Ed25519 signature verification
    score_validator.rs   -- PoU score range and consistency checks
    group_validator.rs   -- Group membership validation
    proposal_validator.rs -- Block proposal validation
    adaptive_validator.rs -- Adaptive validation thresholds
    parallel_validator.rs -- Parallel validation via rayon
  protocols/
    mod.rs               -- Re-exports
    group_aware.rs       -- GroupAwareConsensus, GroupProposerSelector
    pou_based.rs         -- PoU-based lightweight consensus
    bft.rs               -- BFT voting protocol
    hybrid.rs            -- Hybrid consensus combining BFT + PoU
    partition.rs         -- Network partition handling
  traits/                -- Trait definitions for consensus interfaces
  utils/                 -- Utility functions
```

### Key Types

- `PouScore` (`u16`): PoU score in the range 0-1000
- `BlockHeader`: Block header with multi-parent DAG support
- `ValidationResult`: Enum of Valid, Invalid(reason), Pending
- `ConsensusError`: Error type covering validation, crypto, and protocol failures
- `DAGManager`: Manages the directed acyclic graph of blocks with conflict detection

## Dependencies

- `savitri-zkp` (Arkworks backend) for zero-knowledge proof verification
- `ed25519-dalek` for signature operations
- `blake3`, `sha2` for hashing
- `rayon` for parallel validation
- `tokio` for async runtime support

## License

Apache-2.0
