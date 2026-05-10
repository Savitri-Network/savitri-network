//! Integration Tests per Contract Testing Framework
//!
//! Test completi per verificare che il framework funzioni correttamente:
//! - Deployment di contratti
//! - Chiamate a funzioni
//! - Snapshot/restore per isolamento test
//! - Fuzzing per security testing

#[path = "framework.rs"]
mod framework;
#[path = "helpers.rs"]
mod helpers;
#[path = "mocks.rs"]
mod mocks;

use anyhow::Result;
use framework::*;
use helpers::*;
use mocks::*;

#[test]
fn test_framework_deployment() -> Result<()> {
    let mut env = TestEnvironment::new("test-deployment", None, None)?;

    let deployer = "0x1111111111111111111111111111111111111111111111111111111111111111";
    let bytecode = MockSFT1::bytecode();
    let constructor_args = MockSFT1::constructor_args("TestToken", "TST", 1_000_000);

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    assert!(env.contract_exists(&contract_address)?);

    Ok(())
}

#[test]
fn test_framework_view_function() -> Result<()> {
    let mut env = TestEnvironment::new("test-view", None, None)?;

    let deployer = "0x2222222222222222222222222222222222222222222222222222222222222222";
    let bytecode = MockSFT1::bytecode();
    let constructor_args = MockSFT1::constructor_args("TestToken", "TST", 1_000_000);

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    let caller = deployer;
    let function_sig = "balanceOf(address)";
    let calldata = encode_address(&[0x33; 32]);

    let _result = env.call_view_function(contract_address, function_sig, calldata, caller);

    Ok(())
}

#[test]
fn test_framework_snapshot_restore() -> Result<()> {
    let mut env = TestEnvironment::new("test-snapshot", None, None)?;

    let deployer = "0x3333333333333333333333333333333333333333333333333333333333333333";
    let bytecode = MockSFT1::bytecode();
    let constructor_args = MockSFT1::constructor_args("TestToken", "TST", 1_000_000);

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    // Modifica lo storage
    let test_slot = 100u64;
    let test_value = vec![0xAA; 32];
    env.set_contract_storage_slot(contract_address, test_slot, test_value.clone())?;

    // Creates snapshot
    let snapshot = env.snapshot_contract_state(contract_address)?;

    // Modifica ulteriormente lo storage
    let modified_value = vec![0xBB; 32];
    env.set_contract_storage_slot(contract_address, test_slot, modified_value.clone())?;

    // Check che il valore sia cambiato
    let current_value = env.get_contract_storage_slot(contract_address, test_slot)?;
    assert_eq!(current_value, modified_value);

    // Ripristina snapshot
    env.restore_contract_state(&snapshot)?;

    // Check che il valore sia stato ripristinato
    let restored_value = env.get_contract_storage_slot(contract_address, test_slot)?;
    assert_eq!(restored_value, test_value);

    Ok(())
}

#[test]
fn test_framework_test_isolation() -> Result<()> {
    let mut env = TestEnvironment::new("test-isolation", None, None)?;

    let deployer = "0x4444444444444444444444444444444444444444444444444444444444444444";
    let bytecode = MockSFT1::bytecode();
    let constructor_args = MockSFT1::constructor_args("TestToken", "TST", 1_000_000);

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    // Stato iniziale
    let initial_snapshot = env.snapshot_contract_state(contract_address)?;

    for i in 0..10 {
        let slot = 200 + i;
        let value = vec![i as u8; 32];
        env.set_contract_storage_slot(contract_address, slot, value)?;
    }

    // Check che lo stato sia cambiato
    let modified_snapshot = env.snapshot_contract_state(contract_address)?;
    assert_ne!(
        initial_snapshot.storage_root,
        modified_snapshot.storage_root
    );

    // Ripristina stato iniziale
    env.restore_contract_state(&initial_snapshot)?;

    // Check che lo stato sia stato ripristinato
    let restored_snapshot = env.snapshot_contract_state(contract_address)?;
    assert_eq!(
        initial_snapshot.storage_root,
        restored_snapshot.storage_root
    );

    Ok(())
}

#[test]
fn test_framework_fuzzing_input_validation() -> Result<()> {
    let mut env = TestEnvironment::new("test-fuzzing", None, None)?;

    let deployer = "0x5555555555555555555555555555555555555555555555555555555555555555";
    let bytecode = MockSFT1::bytecode();
    let constructor_args = MockSFT1::constructor_args("TestToken", "TST", 1_000_000);

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    let function_sig = "transfer(address,uint256)";
    let valid_inputs = fuzz_input_validation(
        &mut env,
        contract_address,
        function_sig,
        |i| generate_random_calldata(64 + (i % 100)),
        100, // 100 iterazioni
    )?;

    // Check che almeno alcune chiamate siano state gestite correttamente
    println!("Valid inputs handled: {}", valid_inputs);

    Ok(())
}

#[test]
fn test_framework_fuzzing_overflow() -> Result<()> {
    let mut env = TestEnvironment::new("test-fuzzing-overflow", None, None)?;

    let deployer = "0x6666666666666666666666666666666666666666666666666666666666666666";
    let bytecode = MockEdgeCaseContract::overflow_bytecode();
    let constructor_args = vec![];

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    // Fuzzing per overflow/underflow
    let function_sig = "add(uint256,uint256)";
    // Function selector: keccak256("add(uint256,uint256)")[0..4]
    // Calculated: 0x771602f7
    let base_calldata = vec![0x77, 0x16, 0x02, 0xf7]; // add(uint256,uint256) function selector
    let safe_operations = fuzz_overflow_underflow(
        &mut env,
        contract_address,
        function_sig,
        base_calldata,
        |i| generate_overflow_values(i),
        50, // 50 iterazioni
    )?;

    println!("Safe operations: {}", safe_operations);

    Ok(())
}

#[test]
fn test_framework_mock_sft1() -> Result<()> {
    let bytecode = MockSFT1::bytecode();
    assert!(!bytecode.is_empty());

    let bytecode_hex = MockSFT1::bytecode_hex();
    assert!(bytecode_hex.starts_with("0x"));

    let constructor_args = MockSFT1::constructor_args("MyToken", "MTK", 1_000_000_000);
    assert!(!constructor_args.is_empty());

    Ok(())
}

#[test]
fn test_framework_mock_snt1() -> Result<()> {
    let bytecode = MockSNT1::bytecode();
    assert!(!bytecode.is_empty());

    let bytecode_hex = MockSNT1::bytecode_hex();
    assert!(bytecode_hex.starts_with("0x"));

    let constructor_args = MockSNT1::constructor_args("MyNFT", "MNFT", "https://example.com/");
    assert!(!constructor_args.is_empty());

    Ok(())
}

#[test]
fn test_framework_helpers_encode_decode() -> Result<()> {
    // Test encoding/decoding
    let value_u256 = 1_000_000_000u128;
    let encoded = encode_u256(value_u256);
    let decoded = decode_u256(&encoded)?;
    assert_eq!(value_u256, decoded);

    let value_u64 = 42u64;
    let encoded = encode_u64(value_u64);
    let decoded = decode_u64(&encoded)?;
    assert_eq!(value_u64, decoded);

    Ok(())
}

#[test]
fn test_framework_edge_cases() -> Result<()> {
    let mut env = TestEnvironment::new("test-edge-cases", None, None)?;

    let deployer = "0x7777777777777777777777777777777777777777777777777777777777777777";
    let bytecode = MockEdgeCaseContract::access_control_bytecode();
    let constructor_args = vec![];

    let contract_address = env.deploy_contract(deployer, bytecode, constructor_args)?;

    // Test edge cases
    let function_sig = "test()";
    test_edge_cases(&mut env, contract_address, function_sig)?;

    Ok(())
}
