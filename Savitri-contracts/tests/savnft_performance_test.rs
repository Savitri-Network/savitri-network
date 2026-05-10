//! SAVNFT Performance Tests
//!
//! Comprehensive performance testing for SAVNFT contract:
//! - Gas cost measurement for all functions
//! - Throughput testing
//! - Latency measurement
//! - Storage efficiency analysis
//! - Benchmarking
//! - Load testing

use anyhow::{Context, Result};
use hex;
use savitri_contracts::contracts::{
    base::BaseContract,
    gas::GasMeter,
    runtime::{CallFrame, Runtime},
    standards::savnft::SAVNFT,
    storage::ContractStorage,
};
use savitri_contracts::storage::Storage;
use std::time::{Duration, Instant};

fn create_test_storage(prefix: &str) -> Result<(Storage, std::path::PathBuf)> {
    use tempfile::TempDir;

    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let path = tmp_dir.path().join(prefix);
    std::fs::create_dir_all(&path).context("Failed to create test directory")?;

    let storage = Storage::new(path.clone()).context("Failed to create storage")?;

    let path_buf = path.to_path_buf();
    std::mem::forget(tmp_dir);

    Ok((storage, path_buf))
}

/// Performance test environment
struct SAVNFTPerformanceEnv {
    storage: Storage,
    contract_storage: ContractStorage,
    runtime: Runtime,
    gas_meter: GasMeter,
    owner: [u8; 32],
    user1: [u8; 32],
    user2: [u8; 32],
    contract_address: [u8; 32],
}

impl SAVNFTPerformanceEnv {
    fn new() -> Result<Self> {
        let (storage, _tmp_dir) = create_test_storage("savnft_performance_test")
            .context("Failed to create test storage")?;

        let owner = [1u8; 32];
        let user1 = [2u8; 32];
        let user2 = [3u8; 32];
        let contract_address = [100u8; 32];

        let contract_storage = ContractStorage::new(contract_address.to_vec())
            .context("Failed to create contract storage")?;

        let runtime = Runtime::new(std::collections::BTreeMap::new(), 50_000_000, 64, 0);
        let gas_meter = GasMeter::new(50_000_000);

        let initial_frame = CallFrame {
            contract_address,
            caller: owner,
            value: 0,
            calldata: Vec::new(),
            return_data: Vec::new(),
            gas_remaining: 50_000_000,
            depth: 0,
            storage_snapshot: [0u8; 64],
        };

        runtime
            .push_frame(initial_frame)
            .map_err(|e| anyhow::anyhow!("Failed to push initial frame: {}", e))?;

        Ok(Self {
            storage,
            contract_storage,
            runtime,
            gas_meter,
            owner,
            user1,
            user2,
            contract_address,
        })
    }

    fn set_caller(&self, caller: [u8; 32]) -> Result<()> {
        if let Some(mut frame) = self.runtime.current_frame() {
            self.runtime.pop_frame();
            frame.caller = caller;
            self.runtime
                .push_frame(frame)
                .map_err(|e| anyhow::anyhow!("Failed to push frame: {}", e))?;
            Ok(())
        } else {
            let new_frame = CallFrame {
                contract_address: self.contract_address,
                caller,
                value: 0,
                calldata: Vec::new(),
                return_data: Vec::new(),
                gas_remaining: 50_000_000,
                depth: 0,
                storage_snapshot: [0u8; 64],
            };
            self.runtime
                .push_frame(new_frame)
                .map_err(|e| anyhow::anyhow!("Failed to push frame: {}", e))?;
            Ok(())
        }
    }

    fn initialize_contract(
        &mut self,
        name: Option<&str>,
        symbol: Option<&str>,
        enable_enumeration: Option<bool>,
        enable_burn: Option<bool>,
    ) -> Result<()> {
        BaseContract::initialize(
            &mut self.contract_storage,
            &self.storage,
            &self.owner,
            Some(&mut self.gas_meter),
        )?;

        SAVNFT::initialize(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &self.owner,
            name,
            symbol,
            enable_enumeration,
            enable_burn,
            Some(&mut self.gas_meter),
        )?;

        Ok(())
    }

    fn get_gas_used(&self) -> u64 {
        self.gas_meter.gas_used()
    }

    fn reset_gas_meter(&mut self) {
        // Reset gas meter for accurate measurements
        self.gas_meter = GasMeter::new(50_000_000);
    }
}

/// Gas cost measurement results
#[derive(Debug, Clone)]
struct GasCostResult {
    function_name: String,
    gas_used: u64,
    target_gas: Option<u64>,
    passed: bool,
    notes: String,
}

/// Performance measurement results
#[derive(Debug, Clone)]
struct PerformanceResult {
    operation: String,
    gas_cost: u64,
    execution_time: Duration,
    throughput: Option<f64>, // operations per second
}

// ============================================
// Gas Cost Measurement Tests
// ============================================

#[test]
fn test_gas_cost_view_functions() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Mint a token first
    env.set_caller(env.owner)?;
    SAVNFT::mint(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        1,
        Some("https://example.com/token1"),
        Some(&mut env.gas_meter),
    )?;

    let mut results = Vec::new();

    // Test balanceOf
    let start_gas = env.get_gas_used();
    let _balance = SAVNFT::balance_of(
        &mut env.contract_storage,
        &env.storage,
        &hex::encode(env.user1),
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "balanceOf".to_string(),
        gas_used,
        target_gas: Some(300),
        passed: gas_used < 300,
        notes: format!("Gas used: {}, Target: < 300", gas_used),
    });

    // Test ownerOf
    let start_gas = env.get_gas_used();
    let _owner = SAVNFT::owner_of(
        &mut env.contract_storage,
        &env.storage,
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "ownerOf".to_string(),
        gas_used,
        target_gas: Some(300),
        passed: gas_used < 300,
        notes: format!("Gas used: {}, Target: < 300", gas_used),
    });

    // Test getApproved
    let start_gas = env.get_gas_used();
    let _approved = SAVNFT::get_approved(
        &mut env.contract_storage,
        &env.storage,
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "getApproved".to_string(),
        gas_used,
        target_gas: Some(300),
        passed: gas_used < 300,
        notes: format!("Gas used: {}, Target: < 300", gas_used),
    });

    // Test tokenURI
    let start_gas = env.get_gas_used();
    let _uri = SAVNFT::token_uri(
        &mut env.contract_storage,
        &env.storage,
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "tokenURI".to_string(),
        gas_used,
        target_gas: Some(500),
        passed: gas_used < 500,
        notes: format!("Gas used: {}, Target: < 500", gas_used),
    });

    // Print results
    println!("\n=== View Functions Gas Costs ===");
    for result in &results {
        println!(
            "{}: {} gas (Target: {:?}, Passed: {}) - {}",
            result.function_name, result.gas_used, result.target_gas, result.passed, result.notes
        );
    }

    // Verify all passed
    for result in &results {
        assert!(
            result.passed,
            "{} exceeded gas target",
            result.function_name
        );
    }

    Ok(())
}

#[test]
fn test_gas_cost_state_changing_functions() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, Some(false), Some(true))?;

    let mut results = Vec::new();

    // Test mint (without enumeration)
    env.set_caller(env.owner)?;
    let start_gas = env.get_gas_used();
    SAVNFT::mint(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        1,
        Some("https://example.com/token1"),
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "mint (no enumeration)".to_string(),
        gas_used,
        target_gas: Some(105_000),
        passed: gas_used < 105_000,
        notes: format!("Gas used: {}, Target: < 105,000", gas_used),
    });

    // Test transferFrom (without enumeration)
    env.set_caller(env.user1)?;
    let start_gas = env.get_gas_used();
    SAVNFT::transfer_from(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        &hex::encode(env.user2),
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "transferFrom (no enumeration)".to_string(),
        gas_used,
        target_gas: Some(70_000),
        passed: gas_used < 70_000,
        notes: format!("Gas used: {}, Target: < 70,000", gas_used),
    });

    // Test approve
    env.set_caller(env.user2)?;
    let start_gas = env.get_gas_used();
    SAVNFT::approve(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "approve".to_string(),
        gas_used,
        target_gas: Some(25_000),
        passed: gas_used < 25_000,
        notes: format!("Gas used: {}, Target: < 25,000", gas_used),
    });

    // Test burn (without enumeration)
    env.set_caller(env.user2)?;
    let start_gas = env.get_gas_used();
    SAVNFT::burn(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        1,
        Some(&mut env.gas_meter),
    )?;
    let end_gas = env.get_gas_used();
    let gas_used = end_gas - start_gas;
    results.push(GasCostResult {
        function_name: "burn (no enumeration)".to_string(),
        gas_used,
        target_gas: Some(50_000),
        passed: gas_used < 50_000,
        notes: format!("Gas used: {}, Target: < 50,000", gas_used),
    });

    // Print results
    println!("\n=== State-Changing Functions Gas Costs ===");
    for result in &results {
        println!(
            "{}: {} gas (Target: {:?}, Passed: {}) - {}",
            result.function_name, result.gas_used, result.target_gas, result.passed, result.notes
        );
    }

    // Verify all passed
    for result in &results {
        assert!(
            result.passed,
            "{} exceeded gas target",
            result.function_name
        );
    }

    Ok(())
}

// ============================================
// Throughput Testing
// ============================================

#[test]
fn test_throughput_minting() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, Some(false), None)?;

    env.set_caller(env.owner)?;

    let start_time = Instant::now();
    let num_tokens = 100;

    for i in 1..=num_tokens {
        SAVNFT::mint(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            i,
            None,
            Some(&mut env.gas_meter),
        )?;
    }

    let elapsed = start_time.elapsed();
    let throughput = num_tokens as f64 / elapsed.as_secs_f64();

    println!("\n=== Minting Throughput ===");
    println!("Tokens minted: {}", num_tokens);
    println!("Time elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} tokens/second", throughput);

    // Verify all tokens minted
    let balance = SAVNFT::balance_of(
        &mut env.contract_storage,
        &env.storage,
        &hex::encode(env.user1),
        Some(&mut env.gas_meter),
    )?;
    assert_eq!(balance, num_tokens);

    Ok(())
}

#[test]
fn test_throughput_transfers() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, Some(false), None)?;

    // Mint tokens first
    env.set_caller(env.owner)?;
    let num_tokens = 50;
    for i in 1..=num_tokens {
        SAVNFT::mint(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            i,
            None,
            Some(&mut env.gas_meter),
        )?;
    }

    // Measure transfer throughput
    env.set_caller(env.user1)?;
    let start_time = Instant::now();

    for i in 1..=num_tokens {
        SAVNFT::transfer_from(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            &hex::encode(env.user2),
            i,
            Some(&mut env.gas_meter),
        )?;
    }

    let elapsed = start_time.elapsed();
    let throughput = num_tokens as f64 / elapsed.as_secs_f64();

    println!("\n=== Transfer Throughput ===");
    println!("Transfers executed: {}", num_tokens);
    println!("Time elapsed: {:?}", elapsed);
    println!("Throughput: {:.2} transfers/second", throughput);

    // Verify transfers
    let balance = SAVNFT::balance_of(
        &mut env.contract_storage,
        &env.storage,
        &hex::encode(env.user2),
        Some(&mut env.gas_meter),
    )?;
    assert_eq!(balance, num_tokens);

    Ok(())
}

// ============================================
// Latency Measurement
// ============================================

#[test]
fn test_latency_view_functions() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Mint a token
    env.set_caller(env.owner)?;
    SAVNFT::mint(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        1,
        Some("https://example.com/token1"),
        Some(&mut env.gas_meter),
    )?;

    let mut results = Vec::new();

    // Measure balanceOf latency
    let start = Instant::now();
    let _balance = SAVNFT::balance_of(
        &mut env.contract_storage,
        &env.storage,
        &hex::encode(env.user1),
        Some(&mut env.gas_meter),
    )?;
    let latency = start.elapsed();
    results.push(PerformanceResult {
        operation: "balanceOf".to_string(),
        gas_cost: 0, // Not measured here
        execution_time: latency,
        throughput: Some(1.0 / latency.as_secs_f64()),
    });

    // Measure ownerOf latency
    let start = Instant::now();
    let _owner = SAVNFT::owner_of(
        &mut env.contract_storage,
        &env.storage,
        1,
        Some(&mut env.gas_meter),
    )?;
    let latency = start.elapsed();
    results.push(PerformanceResult {
        operation: "ownerOf".to_string(),
        gas_cost: 0,
        execution_time: latency,
        throughput: Some(1.0 / latency.as_secs_f64()),
    });

    println!("\n=== View Functions Latency ===");
    for result in &results {
        println!(
            "{}: {:?} ({:.2} ops/sec)",
            result.operation,
            result.execution_time,
            result.throughput.unwrap_or(0.0)
        );
    }

    Ok(())
}

// ============================================
// Storage Efficiency Testing
// ============================================

#[test]
fn test_storage_efficiency_uri() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    env.set_caller(env.owner)?;

    // Test short URI (single slot)
    let short_uri = "https://example.com/1";
    SAVNFT::mint(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        1,
        Some(short_uri),
        Some(&mut env.gas_meter),
    )?;

    // Test long URI (multi-slot)
    let long_uri = "https://example.com/very/long/uri/path/that/exceeds/twenty/four/bytes/limit/and/requires/multi/slot/storage/for/efficient/encoding/and/optimization";
    SAVNFT::mint(
        &mut env.contract_storage,
        &env.storage,
        &env.runtime,
        &hex::encode(env.user1),
        2,
        Some(long_uri),
        Some(&mut env.gas_meter),
    )?;

    // Verify URIs stored correctly
    let uri1 = SAVNFT::token_uri(
        &mut env.contract_storage,
        &env.storage,
        1,
        Some(&mut env.gas_meter),
    )?;
    assert_eq!(uri1, short_uri);

    let uri2 = SAVNFT::token_uri(
        &mut env.contract_storage,
        &env.storage,
        2,
        Some(&mut env.gas_meter),
    )?;
    assert_eq!(uri2, long_uri);

    println!("\n=== Storage Efficiency (URI) ===");
    println!("Short URI ({} bytes): Single slot storage", short_uri.len());
    println!("Long URI ({} bytes): Multi-slot storage", long_uri.len());
    println!("Storage optimization: Efficient multi-slot pattern");

    Ok(())
}

// ============================================
// Load Testing
// ============================================

#[test]
fn test_load_high_volume_minting() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, Some(false), None)?;

    env.set_caller(env.owner)?;

    let num_tokens = 1000;
    let start_time = Instant::now();
    let mut total_gas = 0;

    for i in 1..=num_tokens {
        env.reset_gas_meter(); // Reset gas meter per operation
        SAVNFT::mint(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            i,
            None,
            Some(&mut env.gas_meter),
        )?;
        total_gas += env.get_gas_used();
    }

    let elapsed = start_time.elapsed();
    let avg_gas_per_mint = total_gas / num_tokens;

    println!("\n=== High-Volume Minting Load Test ===");
    println!("Tokens minted: {}", num_tokens);
    println!("Total time: {:?}", elapsed);
    println!("Total gas: {}", total_gas);
    println!("Average gas per mint: {}", avg_gas_per_mint);
    println!(
        "Throughput: {:.2} tokens/second",
        num_tokens as f64 / elapsed.as_secs_f64()
    );

    // Verify all tokens minted
    let balance = SAVNFT::balance_of(
        &mut env.contract_storage,
        &env.storage,
        &hex::encode(env.user1),
        Some(&mut env.gas_meter),
    )?;
    assert_eq!(balance, num_tokens);

    // Verify gas efficiency
    assert!(
        avg_gas_per_mint < 100_000,
        "Average gas per mint should be < 100,000"
    );

    Ok(())
}

#[test]
fn test_load_stress_transfers() -> Result<()> {
    let mut env = SAVNFTPerformanceEnv::new()?;
    env.initialize_contract(None, None, Some(false), None)?;

    // Mint tokens
    env.set_caller(env.owner)?;
    let num_tokens = 500;
    for i in 1..=num_tokens {
        SAVNFT::mint(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            i,
            None,
            Some(&mut env.gas_meter),
        )?;
    }

    // Stress test transfers
    env.set_caller(env.user1)?;
    let start_time = Instant::now();
    let mut total_gas = 0;

    for i in 1..=num_tokens {
        env.reset_gas_meter(); // Reset gas meter per operation
        let to = if i % 2 == 0 { env.user2 } else { env.user1 };
        SAVNFT::transfer_from(
            &mut env.contract_storage,
            &env.storage,
            &env.runtime,
            &hex::encode(env.user1),
            &hex::encode(to),
            i,
            Some(&mut env.gas_meter),
        )?;
        total_gas += env.get_gas_used();
    }

    let elapsed = start_time.elapsed();
    let avg_gas_per_transfer = total_gas / num_tokens;

    println!("\n=== Stress Transfer Load Test ===");
    println!("Transfers executed: {}", num_tokens);
    println!("Total time: {:?}", elapsed);
    println!("Total gas: {}", total_gas);
    println!("Average gas per transfer: {}", avg_gas_per_transfer);
    println!(
        "Throughput: {:.2} transfers/second",
        num_tokens as f64 / elapsed.as_secs_f64()
    );

    // Verify gas efficiency
    assert!(
        avg_gas_per_transfer < 70_000,
        "Average gas per transfer should be < 70,000"
    );

    Ok(())
}
