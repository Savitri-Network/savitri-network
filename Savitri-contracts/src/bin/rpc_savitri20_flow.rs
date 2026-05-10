use anyhow::{anyhow, bail, Context, Result};
use serde_json::{json, Value};
use sha3::{Digest, Keccak256};
use std::env;
use std::process::Command;

#[derive(Debug, Clone)]
struct Config {
    rpc_url: String,
    deployer: String,
    recipient: String,
    token_name: String,
    token_symbol: String,
    initial_supply: u128,
    transfer_amount: u128,
    nonce: u64,
    deploy_gas: u64,
    call_gas: u64,
    block_timestamp: Option<u64>,
}

fn main() -> Result<()> {
    let cfg = parse_args()?;
    validate_address(&cfg.deployer)?;
    validate_address(&cfg.recipient)?;

    println!("RPC URL: {}", cfg.rpc_url);
    println!("Deployer: {}", cfg.deployer);
    println!("Recipient: {}", cfg.recipient);
    println!(
        "Token: {} ({}) | initial_supply={} | transfer_amount={}",
        cfg.token_name, cfg.token_symbol, cfg.initial_supply, cfg.transfer_amount
    );

    let bytecode = create_token_bytecode();
    let constructor_args =
        encode_token_constructor_args(&cfg.token_name, &cfg.token_symbol, cfg.initial_supply);

    let deploy_params = json!({
        "deployer": cfg.deployer,
        "bytecode_hex": format!("0x{}", hex::encode(&bytecode)),
        "constructor_args_hex": format!("0x{}", hex::encode(&constructor_args)),
        "nonce": cfg.nonce,
        "gas_limit": cfg.deploy_gas,
        "block_timestamp": cfg.block_timestamp,
    });

    let deploy_result = rpc_call(&cfg.rpc_url, "savitri_deployContract", deploy_params)
        .context("deploy call failed")?;
    let contract_address = deploy_result
        .get("contract_address")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing contract_address in deploy result"))?
        .to_string();
    validate_address(&contract_address)?;

    println!("Deployed contract: {}", contract_address);

    let deployer_before =
        balance_of(&cfg, &contract_address, &cfg.deployer).context("balanceOf deployer before")?;
    let recipient_before = balance_of(&cfg, &contract_address, &cfg.recipient)
        .context("balanceOf recipient before")?;

    println!("Before transfer:");
    println!("  deployer_balance = {}", deployer_before);
    println!("  recipient_balance = {}", recipient_before);

    let transfer_calldata = format!(
        "0x{}{}",
        encode_address_abi(&cfg.recipient)?,
        encode_u256_abi(cfg.transfer_amount)
    );

    let transfer_params = json!({
        "contract_address": contract_address,
        "function_signature": "transfer(address,uint256)",
        "calldata_hex": transfer_calldata,
        "caller": cfg.deployer,
        "value": "0",
        "gas_limit": cfg.call_gas,
        "block_timestamp": cfg.block_timestamp,
    });

    let transfer_result = rpc_call(&cfg.rpc_url, "savitri_callContract", transfer_params)
        .context("transfer call failed")?;
    let transfer_return = transfer_result
        .get("return_data_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing return_data_hex in transfer result"))?;
    let transfer_ok = decode_bool32(transfer_return)?;
    if !transfer_ok {
        bail!("transfer returned false");
    }

    let deployer_after =
        balance_of(&cfg, &contract_address, &cfg.deployer).context("balanceOf deployer after")?;
    let recipient_after =
        balance_of(&cfg, &contract_address, &cfg.recipient).context("balanceOf recipient after")?;

    println!("After transfer:");
    println!("  deployer_balance = {}", deployer_after);
    println!("  recipient_balance = {}", recipient_after);

    let recipient_delta = recipient_after.saturating_sub(recipient_before);
    if recipient_delta != cfg.transfer_amount {
        bail!(
            "recipient delta mismatch: expected {}, got {}",
            cfg.transfer_amount,
            recipient_delta
        );
    }

    let deployer_delta = deployer_before.saturating_sub(deployer_after);
    if deployer_delta != cfg.transfer_amount {
        bail!(
            "deployer delta mismatch: expected {}, got {}",
            cfg.transfer_amount,
            deployer_delta
        );
    }

    println!("SUCCESS: transfer verified end-to-end.");
    println!(
        "Summary: contract={} amount={} recipient_received={}",
        contract_address, cfg.transfer_amount, recipient_delta
    );

    Ok(())
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        rpc_url: env::var("RPC_URL").unwrap_or_else(|_| "http://127.0.0.1:8545/rpc".to_string()),
        deployer: env::var("DEPLOYER").unwrap_or_else(|_| {
            "0x1111111111111111111111111111111111111111111111111111111111111111".to_string()
        }),
        recipient: env::var("RECIPIENT").unwrap_or_else(|_| {
            "0x2222222222222222222222222222222222222222222222222222222222222222".to_string()
        }),
        token_name: env::var("TOKEN_NAME").unwrap_or_else(|_| "Demo Token".to_string()),
        token_symbol: env::var("TOKEN_SYMBOL").unwrap_or_else(|_| "DMT".to_string()),
        initial_supply: env::var("INITIAL_SUPPLY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000),
        transfer_amount: env::var("TRANSFER_AMOUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(250),
        nonce: env::var("NONCE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        deploy_gas: env::var("DEPLOY_GAS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000_000),
        call_gas: env::var("CALL_GAS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1_000_000),
        block_timestamp: env::var("BLOCK_TIMESTAMP")
            .ok()
            .and_then(|v| v.parse().ok()),
    };

    let args: Vec<String> = env::args().collect();
    let mut i = 1usize;
    while i < args.len() {
        let key = &args[i];
        let val = args.get(i + 1).cloned();
        match (key.as_str(), val) {
            ("--rpc", Some(v)) => cfg.rpc_url = v,
            ("--deployer", Some(v)) => cfg.deployer = v,
            ("--recipient", Some(v)) => cfg.recipient = v,
            ("--name", Some(v)) => cfg.token_name = v,
            ("--symbol", Some(v)) => cfg.token_symbol = v,
            ("--initial-supply", Some(v)) => cfg.initial_supply = v.parse()?,
            ("--amount", Some(v)) => cfg.transfer_amount = v.parse()?,
            ("--nonce", Some(v)) => cfg.nonce = v.parse()?,
            ("--deploy-gas", Some(v)) => cfg.deploy_gas = v.parse()?,
            ("--call-gas", Some(v)) => cfg.call_gas = v.parse()?,
            ("--block-timestamp", Some(v)) => cfg.block_timestamp = Some(v.parse()?),
            ("--help", _) | ("-h", _) => {
                print_help();
                std::process::exit(0);
            }
            _ => bail!("unknown or incomplete argument: {}", key),
        }
        i += 2;
    }

    Ok(cfg)
}

fn print_help() {
    println!("rpc_savitri20_flow usage:");
    println!("  --rpc <url>                 (default: http://127.0.0.1:8545/rpc)");
    println!("  --deployer <0x..64hex>");
    println!("  --recipient <0x..64hex>");
    println!("  --name <token name>");
    println!("  --symbol <token symbol>");
    println!("  --initial-supply <u128>");
    println!("  --amount <u128>");
    println!("  --nonce <u64>");
    println!("  --deploy-gas <u64>");
    println!("  --call-gas <u64>");
    println!("  --block-timestamp <u64>");
}

fn rpc_call(rpc_url: &str, method: &str, params: Value) -> Result<Value> {
    let payload = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    })
    .to_string();

    let output = Command::new("curl")
        .args([
            "-sS",
            "-X",
            "POST",
            rpc_url,
            "-H",
            "content-type: application/json",
            "-d",
            &payload,
        ])
        .output()
        .context("failed to execute curl")?;

    if !output.status.success() {
        bail!(
            "curl failed (status={}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let body = String::from_utf8(output.stdout).context("rpc response is not utf-8")?;
    let parsed: Value = serde_json::from_str(&body)
        .with_context(|| format!("invalid json-rpc response: {}", body))?;

    if let Some(err) = parsed.get("error") {
        bail!("rpc error for {}: {}", method, err);
    }

    parsed
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("missing result for method {}", method))
}

fn balance_of(cfg: &Config, contract_address: &str, owner: &str) -> Result<u128> {
    let params = json!({
        "contract_address": contract_address,
        "function_signature": "balanceOf(address)",
        "calldata_hex": format!("0x{}", encode_address_abi(owner)?),
        "caller": cfg.deployer,
        "value": "0",
        "gas_limit": cfg.call_gas,
        "block_timestamp": cfg.block_timestamp,
    });
    let result = rpc_call(&cfg.rpc_url, "savitri_callContract", params)?;
    let ret = result
        .get("return_data_hex")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("balanceOf missing return_data_hex"))?;
    decode_u256_from_hex(ret)
}

fn validate_address(input: &str) -> Result<()> {
    let clean = input.trim_start_matches("0x");
    if clean.len() != 64 {
        bail!(
            "address must be 32 bytes hex (64 chars), got {}",
            clean.len()
        );
    }
    hex::decode(clean).context("invalid hex address")?;
    Ok(())
}

fn encode_address_abi(input: &str) -> Result<String> {
    validate_address(input)?;
    Ok(input.trim_start_matches("0x").to_string())
}

fn encode_u256_abi(value: u128) -> String {
    let mut bytes = [0u8; 32];
    bytes[16..].copy_from_slice(&value.to_be_bytes());
    hex::encode(bytes)
}

fn decode_u256_from_hex(ret_hex: &str) -> Result<u128> {
    let clean = ret_hex.trim_start_matches("0x");
    let data = hex::decode(clean).context("invalid return_data_hex")?;
    if data.len() < 32 {
        bail!("return data too short for uint256: {}", data.len());
    }
    let mut n = [0u8; 16];
    n.copy_from_slice(&data[data.len() - 16..]);
    Ok(u128::from_be_bytes(n))
}

fn decode_bool32(ret_hex: &str) -> Result<bool> {
    let clean = ret_hex.trim_start_matches("0x");
    let data = hex::decode(clean).context("invalid bool return_data_hex")?;
    if data.len() < 32 {
        bail!("return data too short for bool: {}", data.len());
    }
    Ok(data[data.len() - 1] == 1)
}

fn selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let hash = hasher.finalize();

    let mut s = [0u8; 4];
    s.copy_from_slice(&hash[..4]);
    s
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
    for s in selectors {
        bytecode.push(0x63);
        bytecode.extend_from_slice(&s);
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
