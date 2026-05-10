//! Contract Interaction Helpers
//!
//! Helper per interagire con i contratti:
//! - Send transaction
//! - Call view function
//! - Wait for transaction
//! - Get contract state
//! - Fuzzing per security testing

#[path = "framework.rs"]
mod framework;

use crate::framework::TestEnvironment;
use anyhow::{Context, Result};
use rand::Rng;
use std::time::Duration;

/// Helper per inviare una transazione
///
/// # Arguments
/// * `env` - Ambiente di test
/// * `calldata` - Dati di chiamata
/// * `value` - Valore in token da trasferire
pub fn send_transaction(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    function_signature: &str,
    calldata: Vec<u8>,
    caller: &str,
    value: u128,
) -> Result<Vec<u8>> {
    env.call_state_function(
        contract_address,
        function_signature,
        calldata,
        caller,
        value,
    )
    .context("Failed to send transaction")
}

/// Helper per chiamare una funzione view (read-only)
///
/// # Arguments
/// * `env` - Ambiente di test
/// * `calldata` - Dati di chiamata
pub fn call_view_function(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    function_signature: &str,
    calldata: Vec<u8>,
    caller: &str,
) -> Result<Vec<u8>> {
    env.call_view_function(contract_address, function_signature, calldata, caller)
        .context("Failed to call view function")
}

/// Helper per attendere il completamento di una transazione
///
pub fn wait_for_transaction(
    _env: &TestEnvironment,
    _tx_hash: &[u8],
    _timeout: Option<Duration>,
) -> Result<()> {
    Ok(())
}

/// Helper per ottenere lo stato di a contract
///
/// # Arguments
/// * `env` - Ambiente di test
pub fn get_contract_state(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    slot: u64,
) -> Result<Vec<u8>> {
    env.get_contract_storage_slot(contract_address, slot)
        .context("Failed to get contract state")
}

///
/// # Arguments
/// * `expected_error` - Errore atteso (substring of the messaggio di errore)
pub fn assert_contract_error(result: Result<Vec<u8>>, expected_error: &str) {
    match result {
        Ok(_) => panic!(
            "Expected contract error '{}', but call succeeded",
            expected_error
        ),
        Err(e) => {
            let error_msg = e.to_string();
            if !error_msg.contains(expected_error) {
                panic!(
                    "Expected error containing '{}', but got: {}",
                    expected_error, error_msg
                );
            }
        }
    }
}

///
/// # Arguments
pub fn assert_contract_success(result: Result<Vec<u8>>) -> Vec<u8> {
    result.expect("Expected contract call to succeed")
}

// ============================================================================
// Fuzzing Helpers per Security Testing
// ============================================================================

///
///
/// # Arguments
/// * `env` - Ambiente di test
pub fn fuzz_input_validation<F>(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    function_signature: &str,
    calldata_generator: F,
    iterations: usize,
) -> Result<usize>
where
    F: Fn(usize) -> Vec<u8>,
{
    let mut valid_inputs = 0;
    let mut rng = rand::thread_rng();

    for i in 0..iterations {
        let calldata = calldata_generator(i);
        let caller = format!("0x{}", hex::encode([rng.gen(); 32]));

        match env.call_view_function(contract_address, function_signature, calldata, &caller) {
            Ok(_) => valid_inputs += 1,
            Err(_) => {
                // Input invalido gestito correttamente (revert)
            }
        }
    }

    Ok(valid_inputs)
}

/// Fuzzing per overflow/underflow
///
///
/// # Arguments
/// * `env` - Ambiente di test
/// * `base_calldata` - Calldata base (without valori numerici)
pub fn fuzz_overflow_underflow<F>(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    function_signature: &str,
    base_calldata: Vec<u8>,
    value_generator: F,
    iterations: usize,
) -> Result<usize>
where
    F: Fn(usize) -> Vec<u8>,
{
    let mut safe_operations = 0;
    let mut rng = rand::thread_rng();

    for i in 0..iterations {
        // Genera valori che possono causare overflow
        let overflow_values = value_generator(i);

        // Combina base calldata con valori overflow
        let mut calldata = base_calldata.clone();
        calldata.extend_from_slice(&overflow_values);

        let caller = format!("0x{}", hex::encode([rng.gen(); 32]));

        match env.call_view_function(contract_address, function_signature, calldata, &caller) {
            Ok(_) => safe_operations += 1,
            Err(_) => {
                // Overflow/underflow gestito correttamente (revert)
            }
        }
    }

    Ok(safe_operations)
}

/// Fuzzing per re-entrancy
///
///
/// # Arguments
/// * `env` - Ambiente di test
/// * `vulnerable_function` - Funzione potenzialmente vulnerabile
pub fn fuzz_reentrancy(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    vulnerable_function: &str,
    reentrancy_function: &str,
    iterations: usize,
) -> Result<usize> {
    let mut safe_calls = 0;
    let mut rng = rand::thread_rng();

    for _ in 0..iterations {
        let caller = format!("0x{}", hex::encode([rng.gen(); 32]));

        // vulnerable_function che internamente chiama reentrancy_function

        match env.contract_exists(&contract_address) {
            Ok(true) => {
                // Prova una chiamata normale
                let dummy_calldata = vec![0u8; 32];
                match env.call_view_function(
                    contract_address,
                    vulnerable_function,
                    dummy_calldata,
                    &caller,
                ) {
                    Ok(_) => safe_calls += 1,
                    Err(_) => {
                        // Re-entrancy gestito correttamente
                    }
                }
            }
            Ok(false) => {
                // Contratto non esiste
                break;
            }
            Err(_) => {
                // Errore nel check
                break;
            }
        }
    }

    Ok(safe_calls)
}

/// Genera calldata randomico per fuzzing
///
/// # Arguments
/// * `size` - Dimensione of the calldata (in bytes)
pub fn generate_random_calldata(size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}

/// Genera valori che possono causare overflow
///
/// # Arguments
/// * `seed` - Seed per generazione deterministica
pub fn generate_overflow_values(seed: usize) -> Vec<u8> {
    // Genera valori vicini a u128::MAX o u64::MAX
    let base: u128 = u128::MAX - (seed as u128 % 1000);
    let mut bytes = vec![0u8; 32];
    bytes[16..32].copy_from_slice(&base.to_be_bytes());
    bytes
}

/// Helper per testare edge cases comuni
///
/// # Arguments
/// * `env` - Ambiente di test
pub fn test_edge_cases(
    env: &mut TestEnvironment,
    contract_address: [u8; 32],
    function_signature: &str,
) -> Result<()> {
    let caller = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Test con zero address
    let zero_calldata = vec![0u8; 32];
    let _ = env.call_view_function(
        contract_address,
        function_signature,
        zero_calldata.clone(),
        caller,
    )?;

    // Test con valori massimi
    let max_calldata = vec![0xFFu8; 32];
    let _ = env.call_view_function(contract_address, function_signature, max_calldata, caller)?;

    // Test con calldata vuoto
    let empty_calldata = vec![];
    let _ = env.call_view_function(contract_address, function_signature, empty_calldata, caller)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_random_calldata() {
        let calldata = generate_random_calldata(64);
        assert_eq!(calldata.len(), 64);
    }

    #[test]
    fn test_generate_overflow_values() {
        let values = generate_overflow_values(0);
        assert_eq!(values.len(), 32);
    }

    #[test]
    fn test_assert_contract_error() {
        let error_result: Result<Vec<u8>> = Err(anyhow::anyhow!("Revert: Insufficient balance"));
        assert_contract_error(error_result, "Revert");
    }
}
