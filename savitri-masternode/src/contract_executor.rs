//! Compila solo con feature "contracts". Chiamare da dove si applicano le transazioni di blocco.

#![cfg(feature = "contracts")]

use anyhow::Result;
use std::collections::BTreeMap;

pub fn execute_deploy(
    storage: &savitri_storage::Storage,
    deployer: &[u8],
    bytecode: Vec<u8>,
    constructor_args: Vec<u8>,
    nonce: u64,
    block_timestamp: u64,
    gas_limit: u64,
) -> Result<[u8; 32]> {
    let deployer_hex = hex::encode(deployer);
    let tx =
        savitri_contracts::DeployTransaction::new(deployer_hex, bytecode, constructor_args, nonce);
    let overlay = BTreeMap::new();
    let runtime = savitri_contracts::Runtime::new(overlay, gas_limit, 64, block_timestamp);
    let gas_meter_guard = runtime.gas_meter();
    let mut gas_meter = gas_meter_guard
        .write()
        .map_err(|e| anyhow::anyhow!("gas_meter lock: {}", e))?;
    tx.execute_constructor(storage, &runtime, &mut gas_meter)
}

/// Da invocare nel flusso di applicazione transazioni.
pub fn execute_call(
    storage: &savitri_storage::Storage,
    contract_address: &[u8],
    function_selector: [u8; 4],
    calldata: Vec<u8>,
    caller: &[u8],
    value: u128,
    block_timestamp: u64,
    gas_limit: u64,
) -> Result<Vec<u8>> {
    let contract_hex = hex::encode(contract_address);
    let caller_hex = hex::encode(caller);
    let tx = savitri_contracts::CallTransaction {
        contract_address: contract_hex,
        function_selector,
        calldata,
        caller: caller_hex,
        value,
    };
    let overlay = BTreeMap::new();
    let runtime = savitri_contracts::Runtime::new(overlay, gas_limit, 64, block_timestamp);
    let gas_meter_guard = runtime.gas_meter();
    let mut gas_meter = gas_meter_guard
        .write()
        .map_err(|e| anyhow::anyhow!("gas_meter lock: {}", e))?;
    tx.execute(storage, &runtime, &mut gas_meter)
        .map_err(|e| anyhow::anyhow!("{}", e))
}
