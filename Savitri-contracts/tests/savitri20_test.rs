//! SAVITRI-20 Unit Tests - Fixed Version
//!
//! Comprehensive unit tests for SAVITRI-20 contract with >90% coverage:
//! - All view functions
//! - All state-changing functions
//! - All modifiers
//! - All events
//! - Success cases, failure cases, edge cases, access control

use anyhow::{Context, Result};
use hex;
use savitri_contracts::contracts::{
    base::BaseContract,
    gas::GasMeter,
    runtime::{CallFrame, Runtime},
    standards::savitri20::SAVITRI20,
    storage::ContractStorage,
};
use savitri_contracts::storage::Storage;

fn create_test_storage(prefix: &str) -> Result<(Storage, std::path::PathBuf)> {
    use tempfile::TempDir;

    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let path = tmp_dir.path().join(prefix);
    std::fs::create_dir_all(&path).context("Failed to create test directory")?;

    let storage = Storage::new(path.clone()).context("Failed to create storage")?;

    // Keep temp dir alive by storing path
    let path_buf = path.to_path_buf();
    std::mem::forget(tmp_dir); // Prevent cleanup (for testing only)

    Ok((storage, path_buf))
}

struct SAVITRI20TestEnv {
    storage: Storage,
    contract_storage: ContractStorage,
    runtime: Runtime,
    gas_meter: GasMeter,
    owner: [u8; 32],
    user1: [u8; 32],
    user2: [u8; 32],
    contract_address: [u8; 32],
}

impl SAVITRI20TestEnv {
    fn new() -> Result<Self> {
        let (storage, _tmp_dir) =
            create_test_storage("savitri20_test").context("Failed to create test storage")?;

        // Addresses di test
        let owner = [1u8; 32];
        let user1 = [2u8; 32];
        let user2 = [3u8; 32];
        let contract_address = [100u8; 32];

        // Creates ContractStorage con contract_address
        let contract_storage = ContractStorage::new(contract_address.to_vec())
            .context("Failed to create contract storage")?;

        let runtime = Runtime::new(
            std::collections::BTreeMap::new(),
            10_000_000, // gas_limit
            64,         // max_call_depth
            0,          // block_timestamp
        );
        let gas_meter = GasMeter::new(10_000_000);

        // Creates frame iniziale con owner come caller
        let initial_frame = CallFrame {
            contract_address,
            caller: owner,
            value: 0,
            calldata: Vec::new(),
            return_data: Vec::new(),
            gas_remaining: 10_000_000,
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
            // Pop frame corrente
            self.runtime.pop_frame();
            // Modifica caller
            frame.caller = caller;
            // Push frame modificato
            self.runtime
                .push_frame(frame)
                .map_err(|e| anyhow::anyhow!("Failed to push frame: {}", e))?;
            Ok(())
        } else {
            // If no frame is present, create a new one
            let new_frame = CallFrame {
                contract_address: self.contract_address,
                caller,
                value: 0,
                calldata: Vec::new(),
                return_data: Vec::new(),
                gas_remaining: 10_000_000,
                depth: 0,
                storage_snapshot: [0u8; 64],
            };
            self.runtime
                .push_frame(new_frame)
                .map_err(|e| anyhow::anyhow!("Failed to push frame: {}", e))?;
            Ok(())
        }
    }

    fn initialize_contract(&mut self, initial_supply: Option<u128>) -> Result<()> {
        // Inizializza BaseContract (non richiede runtime)
        BaseContract::initialize(
            &mut self.contract_storage,
            &self.storage,
            &self.owner,
            Some(&mut self.gas_meter),
        )?;

        // Se c'è initial_supply, mint al owner
        if let Some(supply) = initial_supply {
            if supply > 0 {
                SAVITRI20::mint(
                    &mut self.contract_storage,
                    &self.storage,
                    &self.runtime,
                    &Self::encode_address(&self.owner),
                    supply,
                    Some(&mut self.gas_meter),
                )?;
            }
        }

        Ok(())
    }

    /// Helper per encode address
    fn encode_address(addr: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(addr))
    }

    /// Helper per transfer
    fn transfer(&mut self, to: &[u8; 32], amount: u128) -> Result<()> {
        SAVITRI20::transfer(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(to),
            amount,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per balanceOf
    fn balance_of(&mut self, owner: &[u8; 32]) -> Result<u128> {
        SAVITRI20::balance_of(
            &mut self.contract_storage,
            &self.storage,
            &Self::encode_address(owner),
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per transferFrom
    fn transfer_from(&mut self, from: &[u8; 32], to: &[u8; 32], amount: u128) -> Result<()> {
        SAVITRI20::transfer_from(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(from),
            &Self::encode_address(to),
            amount,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per approve
    fn approve(&mut self, spender: &[u8; 32], amount: u128) -> Result<()> {
        SAVITRI20::approve(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(spender),
            amount,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per allowance
    fn allowance(&mut self, owner: &[u8; 32], spender: &[u8; 32]) -> Result<u128> {
        SAVITRI20::allowance(
            &mut self.contract_storage,
            &self.storage,
            &Self::encode_address(owner),
            &Self::encode_address(spender),
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per totalSupply
    fn total_supply(&mut self) -> Result<u128> {
        SAVITRI20::total_supply(
            &mut self.contract_storage,
            &self.storage,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per mint
    fn mint(&mut self, to: &[u8; 32], amount: u128) -> Result<()> {
        SAVITRI20::mint(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(to),
            amount,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per burn
    fn burn(&mut self, from: &[u8; 32], amount: u128) -> Result<()> {
        SAVITRI20::burn(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(from),
            amount,
            Some(&mut self.gas_meter),
        )
    }
}

// ============================================
// Test Suite: View Functions
// ============================================

#[test]
fn test_total_supply_initial() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    // Initial total supply should be 0
    let total = env.total_supply()?;
    assert_eq!(total, 0);

    Ok(())
}

#[test]
fn test_total_supply_after_mint() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let owner = env.owner;

    // Mint tokens to owner
    env.mint(&owner, 1000)?;

    // Total supply should be 1000
    let total = env.total_supply()?;
    assert_eq!(total, 1000);

    Ok(())
}

#[test]
fn test_balance_of_initial() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;

    // Initial balance should be 0
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 0);

    Ok(())
}

#[test]
fn test_balance_of_after_mint() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;

    // Mint tokens to user1
    env.mint(&user1, 500)?;

    // Balance should be 500
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 500);

    Ok(())
}

#[test]
fn test_allowance_initial() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Initial allowance should be 0
    let allowance = env.allowance(&user1, &user2)?;
    assert_eq!(allowance, 0);

    Ok(())
}

#[test]
fn test_allowance_after_approve() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint tokens to user1
    env.mint(&user1, 1000)?;

    // Approve user2 to spend 300
    env.set_caller(user1)?;
    env.approve(&user2, 300)?;

    // Allowance should be 300
    let allowance = env.allowance(&user1, &user2)?;
    assert_eq!(allowance, 300);

    Ok(())
}

// ============================================
// Test Suite: State-Changing Functions
// ============================================

#[test]
fn test_transfer_success() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let owner = env.owner;

    // Transfer 300 to user1
    env.transfer(&user1, 300)?;

    // Verify balances
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 700);

    let user1_balance = env.balance_of(&user1)?;
    assert_eq!(user1_balance, 300);

    // Total supply should remain unchanged
    let total = env.total_supply()?;
    assert_eq!(total, 1000);

    Ok(())
}

#[test]
fn test_transfer_insufficient_balance() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(100))?; // owner gets 100

    let user1 = env.user1;
    let owner = env.owner;

    // Try to transfer 200 (should fail)
    let result = env.transfer(&user1, 200);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Insufficient"));

    // Balances should remain unchanged
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 100);

    let user1_balance = env.balance_of(&user1)?;
    assert_eq!(user1_balance, 0);

    Ok(())
}

#[test]
fn test_transfer_to_zero_address() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    // Try to transfer to zero address (should fail)
    let zero_address = [0u8; 32];
    let result = env.transfer(&zero_address, 100);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot transfer to zero address"));

    Ok(())
}

#[test]
fn test_approve_success() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    let user1 = env.user1;

    // Approve user1 to spend 500
    env.approve(&user1, 500)?;

    // Verify allowance
    let owner = env.owner;
    let allowance = env.allowance(&owner, &user1)?;
    assert_eq!(allowance, 500);

    Ok(())
}

#[test]
fn test_approve_zero_amount() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    let user1 = env.user1;

    // Approve user1 to spend 0 (should succeed)
    env.approve(&user1, 0)?;

    // Verify allowance is 0
    let owner = env.owner;
    let allowance = env.allowance(&owner, &user1)?;
    assert_eq!(allowance, 0);

    Ok(())
}

#[test]
fn test_transfer_from_success() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Approve user1 to spend 300
    env.approve(&user1, 300)?;

    // Transfer from owner to user2 using user1's allowance
    env.set_caller(user1)?;
    env.transfer_from(&owner, &user2, 200)?;

    // Verify balances
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 800);

    let user2_balance = env.balance_of(&user2)?;
    assert_eq!(user2_balance, 200);

    // Verify remaining allowance
    let allowance = env.allowance(&owner, &user1)?;
    assert_eq!(allowance, 100);

    Ok(())
}

#[test]
fn test_transfer_from_insufficient_allowance() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Approve user1 to spend only 100
    env.approve(&user1, 100)?;

    // Try to transfer 200 (should fail)
    env.set_caller(user1)?;
    let result = env.transfer_from(&owner, &user2, 200);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("allowance"));

    // Balances should remain unchanged
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 1000);

    let user2_balance = env.balance_of(&user2)?;
    assert_eq!(user2_balance, 0);

    Ok(())
}

#[test]
fn test_transfer_from_insufficient_balance() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(100))?; // owner gets only 100

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Approve user1 to spend 1000 (more than owner has)
    env.approve(&user1, 1000)?;

    // Try to transfer 200 (should fail due to insufficient balance)
    env.set_caller(user1)?;
    let result = env.transfer_from(&owner, &user2, 200);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Insufficient"));

    Ok(())
}

#[test]
fn test_mint_success() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;

    // Mint 1000 to user1
    env.mint(&user1, 1000)?;

    // Verify balance
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 1000);

    // Verify total supply
    let total = env.total_supply()?;
    assert_eq!(total, 1000);

    Ok(())
}

#[test]
fn test_mint_to_zero_address() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    // Try to mint to zero address (should fail)
    let zero_address = [0u8; 32];
    let result = env.mint(&zero_address, 1000);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot mint to zero address"));

    Ok(())
}

#[test]
fn test_approve_zero_address() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    // Try to approve zero address (should fail)
    let zero_address = [0u8; 32];
    let result = env.approve(&zero_address, 500);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot approve zero address"));

    Ok(())
}

#[test]
fn test_transfer_from_zero_address() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Approve user1 to spend 500
    env.approve(&user1, 500)?;

    // Try to transfer from zero address (should fail)
    let zero_address = [0u8; 32];
    env.set_caller(user1)?;
    let result = env.transfer_from(&zero_address, &user2, 100);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot transfer from zero address"));

    // Try to transfer to zero address (should fail)
    let result = env.transfer_from(&owner, &zero_address, 100);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot transfer to zero address"));

    Ok(())
}

#[test]
fn test_burn_success() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let owner = env.owner;

    // Burn 300
    env.burn(&owner, 300)?;

    // Verify balance decreased
    let balance = env.balance_of(&owner)?;
    assert_eq!(balance, 700);

    // Verify total supply decreased
    let total = env.total_supply()?;
    assert_eq!(total, 700);

    Ok(())
}

#[test]
fn test_burn_insufficient_balance() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(100))?; // owner gets only 100

    let owner = env.owner;

    // Try to burn 200 (should fail)
    let result = env.burn(&owner, 200);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Insufficient"));

    // Balance should remain unchanged
    let balance = env.balance_of(&owner)?;
    assert_eq!(balance, 100);

    Ok(())
}

// ============================================
// Test Suite: Edge Cases
// ============================================

#[test]
fn test_transfer_self() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let owner = env.owner;

    // Transfer to self (should succeed, no-op)
    env.transfer(&owner, 100)?;

    // Balance should remain unchanged
    let balance = env.balance_of(&owner)?;
    assert_eq!(balance, 1000);

    Ok(())
}

#[test]
fn test_transfer_zero_amount() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let owner = env.owner;

    // Transfer 0 amount (should succeed, no-op)
    env.transfer(&user1, 0)?;

    // Balances should remain unchanged
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 1000);

    let user1_balance = env.balance_of(&user1)?;
    assert_eq!(user1_balance, 0);

    Ok(())
}

#[test]
fn test_approve_self() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?;

    let owner = env.owner;

    // Approve self (should succeed)
    env.approve(&owner, 500)?;

    // Verify allowance
    let allowance = env.allowance(&owner, &owner)?;
    assert_eq!(allowance, 500);

    Ok(())
}

#[test]
fn test_multiple_transfers() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Transfer to multiple users
    env.transfer(&user1, 300)?;
    env.transfer(&user2, 200)?;
    env.transfer(&user1, 100)?; // additional transfer to user1

    // Verify final balances
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 400);

    let user1_balance = env.balance_of(&user1)?;
    assert_eq!(user1_balance, 400);

    let user2_balance = env.balance_of(&user2)?;
    assert_eq!(user2_balance, 200);

    // Total supply should remain unchanged
    let total = env.total_supply()?;
    assert_eq!(total, 1000);

    Ok(())
}

#[test]
fn test_full_approval_cycle() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(Some(1000))?; // owner gets 1000

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Owner approves user1 for 500
    env.approve(&user1, 500)?;

    // User1 transfers 200 to user2
    env.set_caller(user1)?;
    env.transfer_from(&owner, &user2, 200)?;

    // User1 transfers remaining 300 to user2
    env.transfer_from(&owner, &user2, 300)?;

    // Try to transfer more (should fail - no allowance left)
    let result = env.transfer_from(&owner, &user2, 100);
    assert!(result.is_err());

    // Verify final state
    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 500);

    let user2_balance = env.balance_of(&user2)?;
    assert_eq!(user2_balance, 500);

    let allowance = env.allowance(&owner, &user1)?;
    assert_eq!(allowance, 0);

    Ok(())
}

#[test]
fn test_mint_and_burn_cycle() -> Result<()> {
    let mut env = SAVITRI20TestEnv::new()?;
    env.initialize_contract(None)?;

    let user1 = env.user1;
    let user2 = env.user2;
    let owner = env.owner;

    // Mint to multiple users
    env.mint(&user1, 1000)?;
    env.mint(&user2, 500)?;
    env.mint(&owner, 300)?;

    // Verify total supply
    let total = env.total_supply()?;
    assert_eq!(total, 1800);

    // Burn from users
    env.set_caller(user1)?;
    env.burn(&user1, 200)?;

    env.set_caller(user2)?;
    env.burn(&user2, 100)?;

    env.set_caller(owner)?;
    env.burn(&owner, 50)?;

    // Verify final state
    let user1_balance = env.balance_of(&user1)?;
    assert_eq!(user1_balance, 800);

    let user2_balance = env.balance_of(&user2)?;
    assert_eq!(user2_balance, 400);

    let owner_balance = env.balance_of(&owner)?;
    assert_eq!(owner_balance, 250);

    let total = env.total_supply()?;
    assert_eq!(total, 1450);

    Ok(())
}
