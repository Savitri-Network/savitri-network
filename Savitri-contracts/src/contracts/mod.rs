//! Smart Contract Platform: Piattaforma per smart contracts
//!
//! - Runtime environment
//! - Storage model (Merkle tree)
//! - BaseContract con funzioni standard
//! - Deployment e calls
//! - Gas metering
//! - Parallel execution
//! - Events e upgradeability

pub mod base;
pub mod call;
pub mod callbacks;
pub mod deploy;
pub mod events;
pub mod evm_interpreter;
pub mod fee;
pub mod fl;
pub mod gas;
pub mod memory_monitor;
pub mod oracle_registry;
pub mod parallel;
pub mod runtime;
pub mod storage;
pub mod tracing;
pub mod upgrade;

pub mod standards;

pub use base::BaseContract;
pub use call::CallTransaction;
pub use deploy::DeployTransaction;
pub use fee::ContractFee;
pub use gas::GasMeter;
pub use runtime::Runtime;
pub use storage::ContractStorage;
pub use tracing::{
    get_global_tracer, init_global_tracer, ContractTracer, TraceStats, TracingConfig,
};
