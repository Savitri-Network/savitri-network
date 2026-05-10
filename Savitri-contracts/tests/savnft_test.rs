//! SAVNFT Unit Tests
//!
//! Comprehensive unit tests for SAVNFT contract with >90% coverage:
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
    standards::savnft::SAVNFT,
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

struct SAVNFTTestEnv {
    storage: Storage,
    contract_storage: ContractStorage,
    runtime: Runtime,
    gas_meter: GasMeter,
    owner: [u8; 32],
    user1: [u8; 32],
    user2: [u8; 32],
    contract_address: [u8; 32],
}

impl SAVNFTTestEnv {
    fn new() -> Result<Self> {
        let (storage, _tmp_dir) =
            create_test_storage("savnft_test").context("Failed to create test storage")?;

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

    fn initialize_contract(
        &mut self,
        name: Option<&str>,
        symbol: Option<&str>,
        enable_enumeration: Option<bool>,
        enable_burn: Option<bool>,
    ) -> Result<()> {
        // Inizializza BaseContract (non richiede runtime)
        BaseContract::initialize(
            &mut self.contract_storage,
            &self.storage,
            &self.owner,
            Some(&mut self.gas_meter),
        )?;

        // Inizializza SAVNFT (richiede owner_address come &[u8])
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

    /// Helper per encode address
    fn encode_address(addr: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(addr))
    }

    /// Helper per mint un token
    fn mint(&mut self, to: &[u8; 32], token_id: u64, uri: Option<&str>) -> Result<()> {
        SAVNFT::mint(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(to),
            token_id,
            uri,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per balanceOf
    fn balance_of(&mut self, owner: &[u8; 32]) -> Result<u64> {
        SAVNFT::balance_of(
            &mut self.contract_storage,
            &self.storage,
            &Self::encode_address(owner),
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per ownerOf
    fn owner_of(&mut self, token_id: u64) -> Result<String> {
        SAVNFT::owner_of(
            &mut self.contract_storage,
            &self.storage,
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per transferFrom
    fn transfer_from(&mut self, from: &[u8; 32], to: &[u8; 32], token_id: u64) -> Result<()> {
        SAVNFT::transfer_from(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(from),
            &Self::encode_address(to),
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per approve
    fn approve(&mut self, approved: &[u8; 32], token_id: u64) -> Result<()> {
        SAVNFT::approve(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(approved),
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per getApproved
    fn get_approved(&mut self, token_id: u64) -> Result<Option<String>> {
        SAVNFT::get_approved(
            &mut self.contract_storage,
            &self.storage,
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per setApprovalForAll
    fn set_approval_for_all(&mut self, operator: &[u8; 32], approved: bool) -> Result<()> {
        SAVNFT::set_approval_for_all(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(operator),
            approved,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per isApprovedForAll
    fn is_approved_for_all(&mut self, owner: &[u8; 32], operator: &[u8; 32]) -> Result<bool> {
        SAVNFT::is_approved_for_all(
            &mut self.contract_storage,
            &self.storage,
            &Self::encode_address(owner),
            &Self::encode_address(operator),
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per safeTransferFrom
    fn safe_transfer_from(&mut self, from: &[u8; 32], to: &[u8; 32], token_id: u64) -> Result<()> {
        self.set_caller(*from)?;
        SAVNFT::safe_transfer_from(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            &Self::encode_address(from),
            &Self::encode_address(to),
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per tokenURI
    fn token_uri(&mut self, token_id: u64) -> Result<String> {
        SAVNFT::token_uri(
            &mut self.contract_storage,
            &self.storage,
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per setTokenURI
    fn set_token_uri(&mut self, token_id: u64, uri: &str) -> Result<()> {
        SAVNFT::set_token_uri(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            token_id,
            uri,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per name
    fn name(&mut self) -> Result<String> {
        SAVNFT::name(
            &mut self.contract_storage,
            &self.storage,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per symbol
    fn symbol(&mut self) -> Result<String> {
        SAVNFT::symbol(
            &mut self.contract_storage,
            &self.storage,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per totalSupply
    fn total_supply(&mut self) -> Result<u64> {
        SAVNFT::total_supply(
            &mut self.contract_storage,
            &self.storage,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per burn
    fn burn(&mut self, token_id: u64) -> Result<()> {
        SAVNFT::burn(
            &mut self.contract_storage,
            &self.storage,
            &self.runtime,
            token_id,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per tokenByIndex
    fn token_by_index(&mut self, index: u64) -> Result<u64> {
        SAVNFT::token_by_index(
            &mut self.contract_storage,
            &self.storage,
            index,
            Some(&mut self.gas_meter),
        )
    }

    /// Helper per tokenOfOwnerByIndex
    fn token_of_owner_by_index(&mut self, owner: &[u8; 32], index: u64) -> Result<u64> {
        SAVNFT::token_of_owner_by_index(
            &mut self.contract_storage,
            &self.storage,
            &Self::encode_address(owner),
            index,
            Some(&mut self.gas_meter),
        )
    }
}

// ============================================
// Test Suite: View Functions
// ============================================

#[test]
fn test_balance_of_initial() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Initial balance should be 0
    let user1 = env.user1;
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 0);

    Ok(())
}

#[test]
fn test_balance_of_after_mint() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Balance should be 1
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 1);

    // Mint another token
    env.mint(&user1, 2, None)?;

    // Balance should be 2
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 2);

    Ok(())
}

#[test]
fn test_balance_of_after_transfer() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Transfer to user2 (user1 transfers as token owner)
    env.set_caller(user1)?;
    env.transfer_from(&user1, &user2, 1)?;

    // user1 balance should be 0
    let balance1 = env.balance_of(&user1)?;
    assert_eq!(balance1, 0);

    // user2 balance should be 1
    let balance2 = env.balance_of(&user2)?;
    assert_eq!(balance2, 1);

    Ok(())
}

#[test]
fn test_owner_of_after_mint() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Owner should be user1
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user1));

    Ok(())
}

#[test]
fn test_owner_of_nonexistent_token() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Non-existent token should fail
    let result = env.owner_of(999);
    assert!(result.is_err());

    Ok(())
}

#[test]
fn test_get_approved_none() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // No approval should return None
    let approved = env.get_approved(1)?;
    assert_eq!(approved, None);

    Ok(())
}

#[test]
fn test_get_approved_after_approve() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Approve user2
    env.set_caller(user1)?;
    env.approve(&user2, 1)?;

    // Get approved should return user2
    let approved = env.get_approved(1)?;
    assert_eq!(approved, Some(SAVNFTTestEnv::encode_address(&user2)));

    Ok(())
}

#[test]
fn test_is_approved_for_all_false() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // No approval should return false
    let approved = env.is_approved_for_all(&user1, &user2)?;
    assert_eq!(approved, false);

    Ok(())
}

#[test]
fn test_is_approved_for_all_true() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let owner = env.owner;
    let user2 = env.user2;

    // Set approval for all
    env.set_approval_for_all(&user2, true)?;

    // Should return true
    let approved = env.is_approved_for_all(&owner, &user2)?;
    assert_eq!(approved, true);

    Ok(())
}

#[test]
fn test_name_default() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Default name should be "SAVNFT"
    let name = env.name()?;
    assert_eq!(name, "SAVNFT");

    Ok(())
}

#[test]
fn test_name_custom() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(Some("MyNFT"), None, None, None)?;

    // Custom name should be set
    let name = env.name()?;
    assert_eq!(name, "MyNFT");

    Ok(())
}

#[test]
fn test_symbol_default() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Default symbol should be "SAVNFT"
    let symbol = env.symbol()?;
    assert_eq!(symbol, "SAVNFT");

    Ok(())
}

#[test]
fn test_symbol_custom() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, Some("MNFT"), None, None)?;

    // Custom symbol should be set
    let symbol = env.symbol()?;
    assert_eq!(symbol, "MNFT");

    Ok(())
}

#[test]
fn test_total_supply_initial() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Initial total supply should be 0
    let total = env.total_supply()?;
    assert_eq!(total, 0);

    Ok(())
}

#[test]
fn test_total_supply_after_mint() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint tokens
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;
    env.mint(&user2, 3, None)?;

    // Total supply should be 3
    let total = env.total_supply()?;
    assert_eq!(total, 3);

    Ok(())
}

#[test]
fn test_token_uri_empty() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token without URI
    env.mint(&user1, 1, None)?;

    // URI should be empty
    let uri = env.token_uri(1)?;
    assert_eq!(uri, "");

    Ok(())
}

#[test]
fn test_token_uri_after_mint() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token with URI
    env.mint(&user1, 1, Some("https://example.com/token/1"))?;

    // URI should be set
    let uri = env.token_uri(1)?;
    assert_eq!(uri, "https://example.com/token/1");

    Ok(())
}

// ============================================
// Test Suite: State-Changing Functions
// ============================================

#[test]
fn test_mint_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Verify ownership
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user1));

    // Verify balance
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 1);

    // Verify total supply
    let total = env.total_supply()?;
    assert_eq!(total, 1);

    Ok(())
}

#[test]
fn test_mint_duplicate_token_id() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to mint same token ID again (should fail)
    let result = env.mint(&user1, 1, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already minted"));

    Ok(())
}

#[test]
fn test_mint_to_zero_address() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    // Try to mint to zero address (should fail)
    let zero_address = [0u8; 32];
    let result = env.mint(&zero_address, 1, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("zero"));

    Ok(())
}

#[test]
fn test_mint_not_owner() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Try to mint as non-owner (should fail)
    env.set_caller(user1)?;
    let result = env.mint(&user1, 1, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("owner"));

    Ok(())
}

#[test]
fn test_transfer_from_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Transfer to user2 (user1 transfers as token owner)
    env.set_caller(user1)?;
    env.transfer_from(&user1, &user2, 1)?;

    // Verify ownership changed
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user2));

    // Verify balances
    let balance1 = env.balance_of(&user1)?;
    assert_eq!(balance1, 0);

    let balance2 = env.balance_of(&user2)?;
    assert_eq!(balance2, 1);

    Ok(())
}

#[test]
fn test_transfer_from_not_owner() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Try to transfer as non-owner (should fail)
    env.set_caller(user2)?;
    let result = env.transfer_from(&user1, &user2, 1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("authorized"));

    Ok(())
}

#[test]
fn test_transfer_from_with_approval() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Approve user2
    env.set_caller(user1)?;
    env.approve(&user2, 1)?;

    // Transfer as approved user
    env.set_caller(user2)?;
    env.transfer_from(&user1, &user2, 1)?;

    // Verify ownership changed
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user2));

    Ok(())
}

#[test]
fn test_transfer_from_with_operator_approval() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Set approval for all (user1 approves user2 as operator)
    env.set_caller(user1)?;
    env.set_approval_for_all(&user2, true)?;

    // Transfer as operator
    env.set_caller(user2)?;
    env.transfer_from(&user1, &user2, 1)?;

    // Verify ownership changed
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user2));

    Ok(())
}

#[test]
fn test_transfer_from_self() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Transfer to self (user1 transfers as token owner, should succeed, no-op but emits event)
    env.set_caller(user1)?;
    env.transfer_from(&user1, &user1, 1)?;

    // Verify ownership unchanged
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user1));

    Ok(())
}

#[test]
fn test_approve_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Approve user2
    env.set_caller(user1)?;
    env.approve(&user2, 1)?;

    // Verify approval
    let approved = env.get_approved(1)?;
    assert_eq!(approved, Some(SAVNFTTestEnv::encode_address(&user2)));

    Ok(())
}

#[test]
fn test_approve_not_owner() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Try to approve as non-owner (should fail)
    env.set_caller(user2)?;
    let result = env.approve(&user2, 1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("authorized"));

    Ok(())
}

#[test]
fn test_approve_self() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    // Try to approve self (should fail)
    env.set_caller(user1)?;
    let result = env.approve(&user1, 1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("approve owner"));

    Ok(())
}

#[test]
fn test_set_approval_for_all_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // user1 authorizes user2 as operator for user1's tokens
    env.set_caller(user1)?;
    env.set_approval_for_all(&user2, true)?;

    // Verify approval
    let approved = env.is_approved_for_all(&user1, &user2)?;
    assert_eq!(approved, true);

    Ok(())
}

#[test]
fn test_set_approval_for_all_self_rejected() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Self-approval should fail
    env.set_caller(user1)?;
    let result = env.set_approval_for_all(&user1, true);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Cannot approve owner"));

    Ok(())
}

#[test]
fn test_mint_emits_runtime_event() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    env.mint(&user1, 1, None)?;

    let events = env.runtime.event_system().get_custom_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_name, "Transfer");

    Ok(())
}

#[test]
fn test_safe_transfer_from_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token to user1
    env.mint(&user1, 1, None)?;

    env.safe_transfer_from(&user1, &user2, 1)?;

    // Verify ownership changed
    let owner = env.owner_of(1)?;
    assert_eq!(owner, SAVNFTTestEnv::encode_address(&user2));

    Ok(())
}

#[test]
fn test_set_token_uri_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Set URI as contract owner
    env.set_token_uri(1, "https://example.com/new-uri")?;

    // Verify URI
    let uri = env.token_uri(1)?;
    assert_eq!(uri, "https://example.com/new-uri");

    Ok(())
}

#[test]
fn test_set_token_uri_as_token_owner() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Set URI as token owner
    env.set_caller(user1)?;
    env.set_token_uri(1, "https://example.com/owner-uri")?;

    // Verify URI
    let uri = env.token_uri(1)?;
    assert_eq!(uri, "https://example.com/owner-uri");

    Ok(())
}

#[test]
fn test_set_token_uri_not_authorized() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to set URI as non-authorized user (should fail)
    env.set_caller(user2)?;
    let result = env.set_token_uri(1, "https://example.com/unauthorized");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("authorized"));

    Ok(())
}

// ============================================
// Test Suite: Burn Function
// ============================================

#[test]
fn test_burn_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, Some(true))?; // enable_burn = true

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Burn as token owner
    env.set_caller(user1)?;
    env.burn(1)?;

    // Verify token no longer exists
    let result = env.owner_of(1);
    assert!(result.is_err());

    // Verify balance decreased
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 0);

    // Verify total supply decreased
    let total = env.total_supply()?;
    assert_eq!(total, 0);

    Ok(())
}

#[test]
fn test_burn_not_enabled() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, Some(false))?; // enable_burn = false

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to burn (should fail)
    env.set_caller(user1)?;
    let result = env.burn(1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not enabled"));

    Ok(())
}

#[test]
fn test_burn_not_authorized() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, Some(true))?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to burn as non-owner (should fail)
    env.set_caller(user2)?;
    let result = env.burn(1);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("authorized"));

    Ok(())
}

#[test]
fn test_burn_as_contract_owner() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, Some(true))?;

    let user1 = env.user1;
    let owner = env.owner;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Burn as contract owner
    env.set_caller(owner)?;
    env.burn(1)?;

    // Verify token no longer exists
    let result = env.owner_of(1);
    assert!(result.is_err());

    Ok(())
}

// ============================================
// Test Suite: Enumeration Functions
// ============================================

#[test]
fn test_token_by_index_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(true), None)?; // enable_enumeration = true

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint tokens
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;
    env.mint(&user2, 3, None)?;

    // Get token by index
    let token0 = env.token_by_index(0)?;
    let token1 = env.token_by_index(1)?;
    let token2 = env.token_by_index(2)?;

    // Verify tokens (order may vary, but all should exist)
    assert!(token0 == 1 || token0 == 2 || token0 == 3);
    assert!(token1 == 1 || token1 == 2 || token1 == 3);
    assert!(token2 == 1 || token2 == 2 || token2 == 3);
    assert_ne!(token0, token1);
    assert_ne!(token1, token2);
    assert_ne!(token0, token2);

    Ok(())
}

#[test]
fn test_token_by_index_not_enabled() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(false), None)?; // enable_enumeration = false

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to get token by index (should fail)
    let result = env.token_by_index(0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not enabled"));

    Ok(())
}

#[test]
fn test_token_by_index_out_of_bounds() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(true), None)?;

    let user1 = env.user1;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Try to get token by invalid index (should fail)
    let result = env.token_by_index(10);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("out of bounds"));

    Ok(())
}

#[test]
fn test_token_of_owner_by_index_success() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(true), None)?;

    let user1 = env.user1;

    // Mint tokens to user1
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;
    env.mint(&user1, 3, None)?;

    // Get tokens by owner index
    let token0 = env.token_of_owner_by_index(&user1, 0)?;
    let token1 = env.token_of_owner_by_index(&user1, 1)?;
    let token2 = env.token_of_owner_by_index(&user1, 2)?;

    // Verify tokens (order may vary, but all should exist)
    assert!(token0 == 1 || token0 == 2 || token0 == 3);
    assert!(token1 == 1 || token1 == 2 || token1 == 3);
    assert!(token2 == 1 || token2 == 2 || token2 == 3);
    assert_ne!(token0, token1);
    assert_ne!(token1, token2);
    assert_ne!(token0, token2);

    Ok(())
}

#[test]
fn test_token_of_owner_by_index_after_transfer() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(true), None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint tokens to user1
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;

    // Transfer token 1 to user2 (user1 transfers as token owner)
    env.set_caller(user1)?;
    env.transfer_from(&user1, &user2, 1)?;

    // user1 should have only token 2
    let token = env.token_of_owner_by_index(&user1, 0)?;
    assert_eq!(token, 2);

    // user2 should have token 1
    let token = env.token_of_owner_by_index(&user2, 0)?;
    assert_eq!(token, 1);

    Ok(())
}

// ============================================
// Test Suite: Edge Cases
// ============================================

#[test]
fn test_multiple_mints() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Mint multiple tokens
    for i in 1..=10 {
        env.mint(&user1, i, None)?;
    }

    // Verify balances
    let balance = env.balance_of(&user1)?;
    assert_eq!(balance, 10);

    let total = env.total_supply()?;
    assert_eq!(total, 10);

    Ok(())
}

#[test]
fn test_long_uri() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;

    // Create long URI (> 24 bytes)
    let long_uri = "https://example.com/very/long/uri/path/that/exceeds/twenty/four/bytes/limit/and/requires/multi/slot/storage";

    // Mint with long URI
    env.mint(&user1, 1, Some(long_uri))?;

    // Verify URI
    let uri = env.token_uri(1)?;
    assert_eq!(uri, long_uri);

    Ok(())
}

#[test]
fn test_approval_cleared_on_transfer() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, None)?;

    let user1 = env.user1;
    let user2 = env.user2;

    // Mint token
    env.mint(&user1, 1, None)?;

    // Approve user2
    env.set_caller(user1)?;
    env.approve(&user2, 1)?;

    // Verify approval
    let approved = env.get_approved(1)?;
    assert_eq!(approved, Some(SAVNFTTestEnv::encode_address(&user2)));

    // Transfer token
    env.transfer_from(&user1, &user2, 1)?;

    // Approval should be cleared
    let approved = env.get_approved(1)?;
    assert_eq!(approved, None);

    Ok(())
}

#[test]
fn test_burn_with_enumeration() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, Some(true), Some(true))?; // both enabled

    let user1 = env.user1;

    // Mint tokens
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;
    env.mint(&user1, 3, None)?;

    // Burn token 2
    env.set_caller(user1)?;
    env.burn(2)?;

    // Verify token 2 no longer exists
    let result = env.owner_of(2);
    assert!(result.is_err());

    // Verify enumeration arrays updated
    let token0 = env.token_of_owner_by_index(&user1, 0)?;
    let token1 = env.token_of_owner_by_index(&user1, 1)?;

    // Should have tokens 1 and 3 (order may vary)
    assert!((token0 == 1 && token1 == 3) || (token0 == 3 && token1 == 1));

    Ok(())
}

#[test]
fn test_total_supply_after_burn() -> Result<()> {
    let mut env = SAVNFTTestEnv::new()?;
    env.initialize_contract(None, None, None, Some(true))?;

    let user1 = env.user1;

    // Mint tokens
    env.mint(&user1, 1, None)?;
    env.mint(&user1, 2, None)?;
    env.mint(&user1, 3, None)?;

    // Verify total supply
    let total = env.total_supply()?;
    assert_eq!(total, 3);

    // Burn token
    env.set_caller(user1)?;
    env.burn(2)?;

    // Verify total supply decreased
    let total = env.total_supply()?;
    assert_eq!(total, 2);

    Ok(())
}
