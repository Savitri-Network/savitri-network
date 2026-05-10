use anyhow::{Context, Result};
use savitri_contracts::{
    contracts::{base::BaseContract, gas::GasMeter, storage::ContractStorage},
    DeployTransaction, Runtime,
};
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<()> {
    let (storage, _tmp_dir) = create_test_storage("deploy-minimal-contract")?;
    let runtime = Runtime::new(
        std::collections::BTreeMap::new(),
        10_000_000,
        64,
        1_710_000_000,
    );
    let mut gas_meter = GasMeter::new(10_000_000);
    let bytecode = create_minimal_contract_bytecode();

    let deployer = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let deploy_tx = DeployTransaction::new(deployer.to_string(), bytecode.clone(), Vec::new(), 0);

    let contract_address = deploy_tx
        .execute_constructor(&storage, &runtime, &mut gas_meter)
        .context("failed to deploy minimal contract")?;

    let mut contract_storage =
        ContractStorage::new(contract_address.to_vec()).context("failed to open storage")?;
    let owner = BaseContract::owner(&mut contract_storage, &storage, None)?;
    let version = BaseContract::version(&mut contract_storage, &storage, None)?;

    println!("contract_address=0x{}", hex::encode(contract_address));
    println!("owner=0x{}", hex::encode(owner));
    println!("version={version}");
    println!("gas_used={}", gas_meter.gas_used());
    println!("bytecode_hex=0x{}", hex::encode(bytecode));

    Ok(())
}

fn create_test_storage(prefix: &str) -> Result<(Storage, PathBuf)> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut tmp_dir = std::env::temp_dir();
    tmp_dir.push(format!("{prefix}-{nanos}"));
    fs::create_dir_all(&tmp_dir)?;

    let storage = Storage::new(&tmp_dir)?;
    Ok((storage, tmp_dir))
}

fn create_minimal_contract_bytecode() -> Vec<u8> {
    let selectors = [
        selector("owner()"),
        selector("version()"),
        selector("transfer_ownership(address)"),
        selector("pause()"),
        selector("unpause()"),
    ];

    let mut bytecode = Vec::new();
    for selector in selectors {
        bytecode.push(0x63);
        bytecode.extend_from_slice(&selector);
    }

    bytecode.extend_from_slice(&[0x56, 0x57, 0x58, 0x59]);
    while bytecode.len() < 50 {
        bytecode.push(0x00);
    }

    bytecode
}

fn selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let hash = hasher.finalize();

    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[..4]);
    selector
}
