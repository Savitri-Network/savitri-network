use anyhow::{Context, Result};
use savitri_contracts::{
    contracts::{gas::GasMeter, standards::SAVITRI20, storage::ContractStorage},
    DeployTransaction, Runtime,
};
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<()> {
    let (storage, _tmp_dir) = create_test_storage("deploy-token-contract")?;
    let runtime = Runtime::new(
        std::collections::BTreeMap::new(),
        10_000_000,
        64,
        1_710_000_000,
    );
    let mut gas_meter = GasMeter::new(10_000_000);

    let deployer = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let token_name = "Demo Token";
    let token_symbol = "DMT";
    let initial_supply = 1_000_000u128;

    let bytecode = create_token_bytecode();
    let constructor_args = encode_token_constructor_args(token_name, token_symbol, initial_supply);

    let deploy_tx = DeployTransaction::new(
        deployer.to_string(),
        bytecode.clone(),
        constructor_args.clone(),
        0,
    );

    let contract_address = deploy_tx
        .execute_constructor(&storage, &runtime, &mut gas_meter)
        .context("failed to deploy token contract")?;

    let mut contract_storage =
        ContractStorage::new(contract_address.to_vec()).context("failed to open storage")?;
    let total_supply = SAVITRI20::total_supply(&mut contract_storage, &storage, None)?;
    let deployer_balance = SAVITRI20::balance_of(&mut contract_storage, &storage, deployer, None)?;

    println!("contract_address=0x{}", hex::encode(contract_address));
    println!("deployer={deployer}");
    println!("gas_used={}", gas_meter.gas_used());
    println!("bytecode_hex=0x{}", hex::encode(&bytecode));
    println!("constructor_args_hex=0x{}", hex::encode(&constructor_args));
    println!("local_total_supply={total_supply}");
    println!("local_deployer_balance={deployer_balance}");
    println!("note=constructor_args_are_applied_for_savitri20_deployments");
    println!();
    println!("rpc_deploy_example=curl -sS -X POST http://127.0.0.1:8545/rpc -H 'content-type: application/json' -d '{{\"jsonrpc\":\"2.0\",\"method\":\"savitri_deployContract\",\"params\":{{\"deployer\":\"{deployer}\",\"bytecode_hex\":\"0x{}\",\"constructor_args_hex\":\"0x{}\",\"nonce\":0,\"gas_limit\":10000000}},\"id\":1}}'", hex::encode(&bytecode), hex::encode(&constructor_args));

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

fn create_token_bytecode() -> Vec<u8> {
    let selectors = [
        selector("totalSupply()"),
        selector("balanceOf(address)"),
        selector("transfer(address,uint256)"),
        selector("approve(address,uint256)"),
        selector("transferFrom(address,address,uint256)"),
        selector("allowance(address,address)"),
        selector("mint(address,uint256)"),
        selector("burn(uint256)"),
        selector("owner()"),
        selector("pause()"),
        selector("unpause()"),
    ];

    let mut bytecode = Vec::new();
    for selector in selectors {
        bytecode.push(0x63);
        bytecode.extend_from_slice(&selector);
    }

    bytecode.extend_from_slice(&[0x56, 0x57, 0x58, 0x59]);
    while bytecode.len() < 64 {
        bytecode.push(0x00);
    }

    bytecode
}

fn encode_token_constructor_args(name: &str, symbol: &str, initial_supply: u128) -> Vec<u8> {
    let mut args = Vec::new();
    args.extend_from_slice(&(name.len() as u32).to_be_bytes());
    args.extend_from_slice(name.as_bytes());
    args.extend_from_slice(&(symbol.len() as u32).to_be_bytes());
    args.extend_from_slice(symbol.as_bytes());
    args.extend_from_slice(&initial_supply.to_be_bytes());
    args
}

fn selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let hash = hasher.finalize();

    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[..4]);
    selector
}
