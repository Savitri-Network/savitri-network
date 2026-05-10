//! SAVNFT: Savitri NFT Standard Implementation
//!
//! Complete implementation of the SAVITRI-721 standard for NFTs with:
//! - Optimized storage layout (slots 0-99 BaseContract, 100+ SAVNFT)
//! - Gas optimization for all operations
//! - Multi-slot storage for long URIs
//! - Complete integration with BaseContract
//!
//! # Storage Layout
//!
//! ## BaseContract Slots (0-99)
//! Slots 0-99 are reserved for BaseContract and managed automatically.
//!
//! ## SAVNFT Slots (100+)
//! - Token ownership mapping (tokenId => owner): keccak256(tokenId . SLOT_TOKEN_OWNERS_BASE)
//! - Balances mapping (owner => balance): keccak256(owner . SLOT_TOKEN_BALANCES_BASE)
//! - Token approvals mapping (tokenId => approved address): keccak256(tokenId . SLOT_TOKEN_APPROVALS_BASE)
//! - Token URIs base slots: SLOT_TOKEN_URIS_BASE + tokenId + offset (multi-slot for URI > 24 bytes)
//! - Operator approvals nested mapping (owner => operator => bool): keccak256(owner . operator . SLOT_OPERATOR_APPROVALS_BASE)
//!
//! **Optimization**: All mappings use keccak256 for uniform slot distribution
//!
//! # Gas Optimization
//!
//! This implementation includes comprehensive gas optimizations:
//!
//! ## Storage Access Optimizations
//! - **Minimize SLOAD operations**: Caching values read frequently (enumeration flags, owner checks)
//! - **Conditional writes**: Only write to storage if value changes (approval clearing, URI updates)
//! - **Early returns**: Exit early for invalid inputs (zero addresses, non-existent tokens)
//! - **Batch reads**: Group related storage reads together when possible
//!
//! ## Slot Calculation Optimizations
//! - **Keccak256 hashing**: Uniform slot distribution for all mappings (prevents collisions)
//! - **Cached slot calculations**: ContractStorage caches slot calculations automatically
//! - **Efficient nested mappings**: Single hash for nested mappings (owner => operator)
//!
//! ## Storage Layout Optimizations
//! - **Packing**: 24 bytes URI + 8 bytes length in first slot (single slot for short URIs)
//! - **Multi-slot storage**: Efficient chunk size (32 bytes) for long URIs
//! - **Minimal slots**: Only necessary slots are read/written
//!
//! ## Code Optimizations
//! - **Conditional operator checks**: Skip operator approval check if caller is already owner
//! - **Cached flags**: Cache enumeration/burn flags to avoid repeated reads
//! - **Efficient conditionals**: Use short-circuit evaluation where possible
//! - **Swap-and-pop pattern**: O(1) array removals for enumeration
//!
//! ## Gas Cost Targets
//! - `balanceOf`: < 300 gas (single SLOAD)
//! - `ownerOf`: < 300 gas (single SLOAD)
//! - `transferFrom`: < 70,000 gas (without enumeration)
//! - `mint`: < 100,000 gas (without enumeration)
//! - `safeTransferFrom`: < 100,000 gas (EOA), < 100,000 gas (contract)

#![allow(dead_code)]
#![allow(clippy::needless_option_as_deref)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::needless_borrows_for_generic_args)]

use crate::contracts::base::BaseContract;
use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::{Context, Result};
use hex;
use sha3::{Digest, Keccak256};

/// Main SAVNFT struct
///
/// Implements the SAVITRI-721 standard for NFTs with:
/// - Optimized storage layout
/// - Gas optimization
/// - Multi-slot URI storage
/// - BaseContract integration
pub struct SAVNFT;

// ============================================
// Base Slot Constants
// ============================================

/// Base slot for token ownership mapping
/// Range: calculated dynamically (slot = keccak256(tokenId . SLOT_TOKEN_OWNERS_BASE))
/// Optimization: use of keccak256 uniformly distributes slots and prevents conflicts
/// Validation: slot calculated dynamically, cannot conflict with BaseContract
const SLOT_TOKEN_OWNERS_BASE: u64 = 100;

/// Base slot for balances mapping
/// Range: 200+ (slot = keccak256(owner . SLOT_TOKEN_BALANCES_BASE))
/// Validation: slot calculated dynamically, cannot conflict with BaseContract
const SLOT_TOKEN_BALANCES_BASE: u64 = 200;

/// Base slot for token approvals mapping
/// Range: calculated dynamically (slot = keccak256(tokenId . SLOT_TOKEN_APPROVALS_BASE))
/// Optimization: use of keccak256 uniformly distributes slots and prevents conflicts
/// Validation: slot calculated dynamically, cannot conflict with BaseContract
const SLOT_TOKEN_APPROVALS_BASE: u64 = 300;

/// Base slot for token URIs
/// Range: 400+ (slot = SLOT_TOKEN_URIS_BASE + tokenId + offset for multi-slot)
/// Validation: slot must be >= 400
const SLOT_TOKEN_URIS_BASE: u64 = 400;

/// Base slot for operator approvals nested mapping
/// Range: 500+ (slot = keccak256(owner . operator . SLOT_OPERATOR_APPROVALS_BASE))
/// Validation: slot calculated dynamically, cannot conflict with BaseContract
const SLOT_OPERATOR_APPROVALS_BASE: u64 = 500;

/// Maximum URI size in a single slot (24 bytes after length)
const MAX_URI_SINGLE_SLOT: usize = 24;

/// Chunk size for multi-slot URI (32 bytes per slot)
const URI_CHUNK_SIZE: usize = 32;

/// Base slot for contract name storage
/// Range: 600+ (multi-slot for long names)
const SLOT_NAME_BASE: u64 = 600;

/// Base slot for contract symbol storage
/// Range: 700+ (multi-slot for long symbols)
const SLOT_SYMBOL_BASE: u64 = 700;

/// Base slot for total supply storage
/// Range: 800 (single slot for u64)
const SLOT_TOTAL_SUPPLY: u64 = 800;

/// Base slot for enumeration enabled flag
/// Range: 900 (single slot for bool)
const SLOT_ENUMERATION_ENABLED: u64 = 900;

/// Base slot for allTokens array (global token list)
/// Range: 1000+ (array length at base slot, elements at keccak256(base + index))
const SLOT_ALL_TOKENS_BASE: u64 = 1000;

/// Base slot for ownerTokens mapping (owner => array of tokenIds)
/// Range: 2000+ (mapping base slot, array length at keccak256(owner + base), elements at keccak256(keccak256(owner + base) + index))
const SLOT_OWNER_TOKENS_BASE: u64 = 2000;

/// Base slot for burn enabled flag
/// Range: 3000 (single slot for bool)
const SLOT_BURN_ENABLED: u64 = 3000;

impl SAVNFT {
    // ============================================
    // Helper Functions for Value Conversion
    // ============================================

    /// Converts a u64 to a storage value (32 bytes, little-endian)
    ///
    /// # Arguments
    /// * `value` - u64 value to convert
    ///
    /// # Returns
    /// Storage value (32 bytes) with u64 in little-endian in the first 8 bytes
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converts a storage value (32 bytes) to u64 (little-endian)
    ///
    /// # Arguments
    /// * `value` - Storage value (32 bytes)
    ///
    /// # Returns
    /// u64 value or error if the value is invalid
    fn storage_value_to_u64(value: &[u8]) -> Result<u64> {
        if value.len() < 8 {
            anyhow::bail!(
                "Storage value too short for u64: expected at least 8 bytes, got {}",
                value.len()
            );
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[0..8]);
        Ok(u64::from_le_bytes(bytes))
    }

    /// Converts a bool to a storage value (32 bytes)
    ///
    /// # Arguments
    /// * `value` - bool value to convert
    ///
    /// # Returns
    /// Storage value (32 bytes): [1, 0, ...] for true, [0, 0, ...] for false
    fn bool_to_storage_value(value: bool) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        if value {
            bytes[0] = 1;
        }
        bytes
    }

    /// Converts a storage value (32 bytes) to bool
    ///
    /// # Arguments
    /// * `value` - Storage value (32 bytes)
    ///
    /// # Returns
    /// `true` if the first byte is non-zero, `false` otherwise
    fn storage_value_to_bool(value: &[u8]) -> Result<bool> {
        if value.is_empty() {
            return Ok(false);
        }
        Ok(value[0] != 0)
    }

    /// Decodes an address from hex string
    ///
    /// # Arguments
    /// * `address_str` - Address as hex string (with or without "0x" prefix)
    ///
    /// # Returns
    /// Address as [u8; 32] or error if invalid
    fn decode_address(address_str: &str) -> Result<[u8; 32]> {
        let address_hex = address_str.strip_prefix("0x").unwrap_or(address_str);
        let address_bytes = hex::decode(address_hex)
            .with_context(|| format!("Failed to decode address: {}", address_str))?;
        if address_bytes.len() != 32 {
            anyhow::bail!("Address must be 32 bytes, got {}", address_bytes.len());
        }
        let mut address = [0u8; 32];
        address.copy_from_slice(&address_bytes);
        Ok(address)
    }

    /// Encodes an address to hex string
    ///
    /// # Arguments
    /// * `address` - Address as [u8; 32]
    ///
    /// # Returns
    /// Address as hex string with "0x" prefix
    fn encode_address(address: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(address))
    }

    /// Converts a storage value to address
    ///
    /// # Arguments
    /// * `value` - Storage value (32 bytes)
    ///
    /// # Returns
    /// Address as [u8; 32] or error if invalid
    fn storage_value_to_address(value: &[u8]) -> Result<[u8; 32]> {
        if value.len() != 32 {
            anyhow::bail!(
                "Invalid address value length: expected 32, got {}",
                value.len()
            );
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(value);
        Ok(out)
    }

    // ============================================
    // Helper Functions for Slot Calculation
    // ============================================

    /// Calculates the slot for token ownership using keccak256
    ///
    /// **Storage Optimization**: Use of keccak256 instead of direct addition
    /// - Uniformly distributes slots in available space
    /// - Prevents collisions between different token IDs
    /// - Avoids overflow issues with very large token IDs
    /// - Standard pattern for mappings in smart contracts
    ///
    /// Slot = keccak256(tokenId . SLOT_TOKEN_OWNERS_BASE)
    ///
    /// # Arguments
    /// * `token_id` - Token ID (u64)
    ///
    /// # Returns
    /// Slot for token ownership or error if invalid
    ///
    /// # Validation
    /// Validates that the calculated slot is not reserved for BaseContract (0-99)
    ///
    /// # Gas Optimization
    /// - Efficient hash calculation (keccak256 is optimized)
    /// - No overflow risk (hash always produces 32 bytes)
    /// - Uniform distribution reduces collisions and improves performance
    fn token_owner_slot(token_id: u64) -> Result<u64> {
        // Optimization: use keccak256 for uniform slot distribution
        let mut hasher = Keccak256::new();
        hasher.update(&token_id.to_le_bytes());
        hasher.update(&SLOT_TOKEN_OWNERS_BASE.to_le_bytes());
        let hash = hasher.finalize();

        // Take first 8 bytes of hash as slot (little-endian)
        // Optimization: use only 8 bytes for slot (u64) instead of 32 bytes
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        let slot = u64::from_le_bytes(slot_bytes);

        // Validation: slot must not be reserved for BaseContract
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Token owner slot {} conflicts with BaseContract reserved slots",
                slot
            )
        })?;

        Ok(slot)
    }

    /// Calculates the slot for an owner's balance using keccak256
    ///
    /// **Storage Optimization**: keccak256 pattern for efficient mapping
    /// - Uniformly distributes slots for different owners
    /// - Prevents collisions between different addresses
    /// - Standard pattern for address => value mapping in smart contracts
    ///
    /// Slot = keccak256(owner . SLOT_TOKEN_BALANCES_BASE)
    ///
    /// # Arguments
    /// * `owner` - Owner address (32 bytes)
    ///
    /// # Returns
    /// Slot for owner's balance
    ///
    /// # Validation
    /// Validates that the calculated slot is not reserved for BaseContract
    ///
    /// # Gas Optimization
    /// - Efficient hash calculation (keccak256 is optimized)
    /// - Uniform distribution reduces collisions
    /// - Optimized access pattern for frequent reads
    fn owner_balance_slot(owner: &[u8; 32]) -> Result<u64> {
        // Optimization: use keccak256 for uniform slot distribution
        let mut hasher = Keccak256::new();
        hasher.update(owner);
        hasher.update(&SLOT_TOKEN_BALANCES_BASE.to_le_bytes());
        let hash = hasher.finalize();

        // Take first 8 bytes of hash as slot (little-endian)
        // Optimization: use only 8 bytes for slot (u64) instead of 32 bytes
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        let slot = u64::from_le_bytes(slot_bytes);

        // Validation: slot must not be reserved for BaseContract
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Owner balance slot {} conflicts with BaseContract reserved slots",
                slot
            )
        })?;

        Ok(slot)
    }

    /// Calculates the slot for token approval using keccak256
    ///
    /// **Storage Optimization**: Use of keccak256 instead of direct addition
    /// - Uniformly distributes slots in available space
    /// - Prevents collisions between different token IDs
    /// - Avoids overflow issues with very large token IDs
    /// - Standard pattern for mappings in smart contracts
    ///
    /// Slot = keccak256(tokenId . SLOT_TOKEN_APPROVALS_BASE)
    ///
    /// # Arguments
    /// * `token_id` - Token ID (u64)
    ///
    /// # Returns
    /// Slot for token approval or error if invalid
    ///
    /// # Validation
    /// Validates that the calculated slot is not reserved for BaseContract
    ///
    /// # Gas Optimization
    /// - Efficient hash calculation (keccak256 is optimized)
    /// - No overflow risk (hash always produces 32 bytes)
    /// - Uniform distribution reduces collisions and improves performance
    fn token_approval_slot(token_id: u64) -> Result<u64> {
        // Optimization: use keccak256 for uniform slot distribution
        let mut hasher = Keccak256::new();
        hasher.update(&token_id.to_le_bytes());
        hasher.update(&SLOT_TOKEN_APPROVALS_BASE.to_le_bytes());
        let hash = hasher.finalize();

        // Take first 8 bytes of hash as slot (little-endian)
        // Optimization: use only 8 bytes for slot (u64) instead of 32 bytes
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        let slot = u64::from_le_bytes(slot_bytes);

        // Validation: slot must not be reserved for BaseContract
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Token approval slot {} conflicts with BaseContract reserved slots",
                slot
            )
        })?;

        Ok(slot)
    }

    /// Calculates the base slot for token URI
    ///
    /// Base slot = SLOT_TOKEN_URIS_BASE + tokenId
    /// For long URIs, additional slots are used: base+1, base+2, ...
    ///
    /// # Arguments
    /// * `token_id` - Token ID (u64)
    ///
    /// # Returns
    /// Base slot for token URI or error if overflow
    ///
    /// # Validation
    /// Validates that the base slot is not reserved for BaseContract
    fn token_uri_base_slot(token_id: u64) -> Result<u64> {
        let slot = SLOT_TOKEN_URIS_BASE
            .checked_add(token_id)
            .ok_or_else(|| anyhow::anyhow!("Token ID overflow for URI base slot calculation"))?;

        // Validation: slot must not be reserved for BaseContract
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Token URI base slot {} conflicts with BaseContract reserved slots",
                slot
            )
        })?;

        Ok(slot)
    }

    /// Calculates the slot for operator approval (nested mapping) using keccak256
    ///
    /// **Storage Optimization**: keccak256 pattern for efficient nested mapping
    /// - Implements mapping(owner => mapping(operator => bool)) with a single hash
    /// - Uniformly distributes slots for different owner/operator combinations
    /// - Prevents collisions between different owner/operator pairs
    /// - Standard pattern for nested mappings in smart contracts
    ///
    /// Slot = keccak256(owner . operator . SLOT_OPERATOR_APPROVALS_BASE)
    ///
    /// # Arguments
    /// * `owner` - Owner address (32 bytes)
    /// * `operator` - Operator address (32 bytes)
    ///
    /// # Returns
    /// Slot for operator approval or error if invalid
    ///
    /// # Validation
    /// Validates that the calculated slot is not reserved for BaseContract
    ///
    /// # Gas Optimization
    /// - Efficient hash calculation (keccak256 is optimized)
    /// - Single hash instead of double lookup (nested mapping)
    /// - Uniform distribution reduces collisions
    fn operator_approval_slot(owner: &[u8; 32], operator: &[u8; 32]) -> Result<u64> {
        // Optimization: use keccak256 for efficient nested mapping
        // Pattern: hash(owner . operator . base) instead of double lookup
        let mut hasher = Keccak256::new();
        hasher.update(owner);
        hasher.update(operator);
        hasher.update(&SLOT_OPERATOR_APPROVALS_BASE.to_le_bytes());
        let hash = hasher.finalize();

        // Take first 8 bytes of hash as slot (little-endian)
        // Optimization: use only 8 bytes for slot (u64) instead of 32 bytes
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        let slot = u64::from_le_bytes(slot_bytes);

        // Validation: slot must not be reserved for BaseContract
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Operator approval slot {} conflicts with BaseContract reserved slots",
                slot
            )
        })?;

        Ok(slot)
    }

    // ============================================
    // Event Emission Functions
    // ============================================

    /// Emits Transfer event
    ///
    /// Event signature: Transfer(address,address,uint256)
    /// Topics: [keccak256("Transfer(address,address,uint256)"), from, to]
    /// Data: tokenId
    ///
    /// # Arguments
    /// * `event_system` - Event system
    /// * `contract_address` - Contract address (32 bytes)
    /// * `from` - Sender address (32 bytes)
    /// * `to` - Recipient address (32 bytes)
    /// * `token_id` - Token ID (u64)
    /// * `gas_meter` - Optional gas meter
    fn emit_transfer_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        from: &[u8; 32],
        to: &[u8; 32],
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Calculate topic0: keccak256("Transfer(address,address,uint256)")
        let transfer_signature = b"Transfer(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(transfer_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic1: from address
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(from);

        // Topic2: to address
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(to);

        // Data: tokenId (32 bytes, little-endian in first 8 bytes)
        let mut data = vec![0u8; 32];
        data[0..8].copy_from_slice(&token_id.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Transfer".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };
        event_system.emit_custom_event(event, gas_meter);
    }

    /// Emits Approval event
    ///
    /// Event signature: Approval(address,address,uint256)
    /// Topics: [keccak256("Approval(address,address,uint256)"), owner, approved]
    /// Data: tokenId
    ///
    /// # Arguments
    /// * `event_system` - Event system
    /// * `contract_address` - Contract address (32 bytes)
    /// * `owner` - Owner address (32 bytes)
    /// * `approved` - Approved address (32 bytes)
    /// * `token_id` - Token ID (u64)
    /// * `gas_meter` - Optional gas meter
    fn emit_approval_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        owner: &[u8; 32],
        approved: &[u8; 32],
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Calculate topic0: keccak256("Approval(address,address,uint256)")
        let approval_signature = b"Approval(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(approval_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic1: owner address
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(owner);

        // Topic2: approved address
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(approved);

        // Data: tokenId (32 bytes, little-endian in first 8 bytes)
        let mut data = vec![0u8; 32];
        data[0..8].copy_from_slice(&token_id.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Approval".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };
        event_system.emit_custom_event(event, gas_meter);
    }

    /// Emits ApprovalForAll event
    ///
    /// Event signature: ApprovalForAll(address,address,bool)
    /// Topics: [keccak256("ApprovalForAll(address,address,bool)"), owner, operator]
    /// Data: approved (bool as 32 bytes)
    ///
    /// # Arguments
    /// * `event_system` - Event system
    /// * `contract_address` - Contract address (32 bytes)
    /// * `owner` - Owner address (32 bytes)
    /// * `operator` - Operator address (32 bytes)
    /// * `approved` - Whether approval is enabled (bool)
    /// * `gas_meter` - Optional gas meter
    fn emit_approval_for_all_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        owner: &[u8; 32],
        operator: &[u8; 32],
        approved: bool,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Calculate topic0: keccak256("ApprovalForAll(address,address,bool)")
        let approval_for_all_signature = b"ApprovalForAll(address,address,bool)";
        let mut hasher = Keccak256::new();
        hasher.update(approval_for_all_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic1: owner address
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(owner);

        // Topic2: operator address
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(operator);

        // Data: approved (bool as 32 bytes)
        let data = Self::bool_to_storage_value(approved);

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "ApprovalForAll".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };
        event_system.emit_custom_event(event, gas_meter);
    }

    // ============================================
    // Multi-Slot URI Storage Functions
    // ============================================

    /// Writes a URI to storage using multi-slot if necessary
    ///
    /// **Multi-Slot Storage Optimization for Long URIs**:
    /// - Efficient schema to divide long URIs into multiple slots
    /// - Optimized encoding: length indicator in first slot
    /// - Minimize SLOAD operations: reads only necessary slots
    /// - Efficient packing: 24 bytes in first slot + length (8 bytes)
    ///
    /// Storage schema:
    /// - Base slot: [length (8 bytes, little-endian) | first 24 bytes of URI]
    /// - Base+1, base+2, ... slots: [next 32 bytes chunks] if URI > 24 bytes
    ///
    /// **Applied Optimizations**:
    /// 1. **Packing**: Length (8 bytes) + first 24 bytes URI in first slot (32 bytes total)
    /// 2. **Chunk Size**: 32 bytes per additional slot (maximum storage efficiency)
    /// 3. **Minimize SLOAD**: Does not read existing slots before writing (assumes overwrite)
    /// 4. **Early Return**: If URI <= 24 bytes, uses only 1 slot (minimum gas cost)
    /// 5. **Slot Validation**: Verifies that each chunk slot is not reserved for BaseContract
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `base_slot` - Base slot for URI (calculated by token_uri_base_slot)
    /// * `uri_bytes` - URI as bytes
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if write fails
    ///
    /// # Gas Optimization
    /// - Minimizes SLOAD operations (does not read existing slots)
    /// - Writes only necessary slots (early return for short URIs)
    /// - Efficient packing (24 bytes + length in first slot)
    /// - Optimized chunk size (32 bytes per slot)
    fn write_uri_to_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        uri_bytes: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let uri_len = uri_bytes.len();

        // Base slot: [length (8 bytes) | first 24 bytes]
        let mut first_slot = vec![0u8; 32];
        first_slot[0..8].copy_from_slice(&uri_len.to_le_bytes());

        if uri_len <= MAX_URI_SINGLE_SLOT {
            // Complete URI in first slot
            first_slot[8..8 + uri_len].copy_from_slice(uri_bytes);
            contract_storage
                .sstore(storage, base_slot, first_slot, gas_meter.as_deref_mut())
                .with_context(|| "Failed to write URI first slot")?;
        } else {
            // Partial URI in first slot + additional slots
            first_slot[8..32].copy_from_slice(&uri_bytes[0..MAX_URI_SINGLE_SLOT]);
            contract_storage
                .sstore(storage, base_slot, first_slot, gas_meter.as_deref_mut())
                .with_context(|| "Failed to write URI first slot")?;

            // Write remaining chunks (32 bytes per slot)
            let mut offset = MAX_URI_SINGLE_SLOT;
            let mut slot_offset = 1u64;
            while offset < uri_len {
                let chunk_end = std::cmp::min(offset + URI_CHUNK_SIZE, uri_len);
                let chunk_size = chunk_end - offset;

                let mut chunk = vec![0u8; 32];
                chunk[0..chunk_size].copy_from_slice(&uri_bytes[offset..chunk_end]);

                let chunk_slot = base_slot
                    .checked_add(slot_offset)
                    .ok_or_else(|| anyhow::anyhow!("Slot overflow for URI storage"))?;

                // Validation: slot must not be reserved
                BaseContract::validate_slot_not_reserved(chunk_slot).with_context(|| {
                    format!("URI chunk slot {} conflicts with BaseContract", chunk_slot)
                })?;

                contract_storage
                    .sstore(storage, chunk_slot, chunk, gas_meter.as_deref_mut())
                    .with_context(|| format!("Failed to write URI chunk at slot {}", chunk_slot))?;

                offset = chunk_end;
                slot_offset = slot_offset
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Too many slots for URI"))?;
            }
        }

        Ok(())
    }

    /// Reads a URI from storage (supports multi-slot)
    ///
    /// **Multi-Slot Read Optimization**:
    /// - Efficient decoding: reads length from first slot
    /// - Minimize SLOAD: reads only necessary slots
    /// - Early return: if URI <= 24 bytes, reads only 1 slot
    /// - Efficient reconstruction: uses Vec::with_capacity to avoid reallocations
    ///
    /// **Applied Optimizations**:
    /// 1. **Single SLOAD for Short URIs**: If length <= 24, reads only first slot
    /// 2. **Pre-allocation**: Vec::with_capacity(uri_len) avoids multiple reallocations
    /// 3. **Chunk Reading**: Reads 32 bytes chunks per additional slot
    /// 4. **Early Return**: If length == 0, returns immediately without additional reads
    /// 5. **Minimize SLOAD**: Does not read unnecessary slots (uses length to determine how many slots to read)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `base_slot` - Base slot for URI
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// URI as string or error if read fails
    ///
    /// # Gas Optimization
    /// - Reads only necessary slots (determined by length in first slot)
    /// - Cache for repeated reads (managed by ContractStorage)
    /// - Memory pre-allocation (avoids reallocations)
    /// - Early return for empty or short URIs
    fn read_uri_from_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        // Read first slot: [length | first 24 bytes]
        let first_slot_value = contract_storage
            .sload(storage, base_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read URI first slot")?;

        // Extract length (first 8 bytes)
        if first_slot_value.len() < 8 {
            anyhow::bail!("Invalid URI storage: first slot too short");
        }
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&first_slot_value[0..8]);
        let uri_len = u64::from_le_bytes(len_bytes);

        if uri_len == 0 {
            return Ok(String::new());
        }

        // Build URI
        let mut uri_bytes = Vec::with_capacity(uri_len as usize);

        if uri_len <= MAX_URI_SINGLE_SLOT as u64 {
            // Complete URI in first slot
            uri_bytes.extend_from_slice(&first_slot_value[8..8 + uri_len as usize]);
        } else {
            // Partial URI in first slot + additional slots
            uri_bytes.extend_from_slice(&first_slot_value[8..32]);

            // Read remaining chunks
            let mut offset = MAX_URI_SINGLE_SLOT;
            let mut slot_offset = 1u64;
            while offset < uri_len as usize {
                let chunk_slot = base_slot
                    .checked_add(slot_offset)
                    .ok_or_else(|| anyhow::anyhow!("Slot overflow reading URI"))?;

                let chunk_value = contract_storage
                    .sload(storage, chunk_slot, gas_meter.as_deref_mut())
                    .with_context(|| format!("Failed to read URI chunk at slot {}", chunk_slot))?;

                let remaining = uri_len as usize - offset;
                let chunk_size = std::cmp::min(URI_CHUNK_SIZE, remaining);
                uri_bytes.extend_from_slice(&chunk_value[0..chunk_size]);

                offset += chunk_size;
                slot_offset = slot_offset
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Too many slots reading URI"))?;
            }
        }

        // Convert to UTF-8 string
        let uri = std::str::from_utf8(&uri_bytes).with_context(|| "Invalid UTF-8 in token URI")?;
        Ok(uri.to_string())
    }

    // ============================================
    // BaseContract Integration
    // ============================================

    /// Initializes SAVNFT contract with BaseContract integration
    ///
    /// This function must be called during contract deployment to:
    /// - Initialize BaseContract with owner address
    /// - Set contract name and symbol (if provided)
    /// - Validate all input parameters
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `owner_address` - Owner address (32 bytes)
    /// * `name` - Contract name (optional, default "SAVNFT")
    /// * `symbol` - Contract symbol (optional, default "SAVNFT")
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if initialization fails
    ///
    /// # Errors
    /// - If owner_address is not 32 bytes
    /// - If name or symbol are invalid
    /// - If BaseContract initialization fails
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        _runtime: &Runtime,
        owner_address: &[u8],
        name: Option<&str>,
        symbol: Option<&str>,
        enable_enumeration: Option<bool>,
        enable_burn: Option<bool>,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Validate owner address
        if owner_address.len() != 32 {
            anyhow::bail!(
                "owner_address must be exactly 32 bytes, got {}",
                owner_address.len()
            );
        }

        // Initialize BaseContract
        BaseContract::initialize(
            contract_storage,
            storage,
            owner_address,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to initialize BaseContract")?;

        // Set contract name (default "SAVNFT" if not provided)
        let name_str = name.unwrap_or("SAVNFT");
        Self::set_name(
            contract_storage,
            storage,
            name_str,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to set contract name")?;

        // Set contract symbol (default "SAVNFT" if not provided)
        let symbol_str = symbol.unwrap_or("SAVNFT");
        Self::set_symbol(
            contract_storage,
            storage,
            symbol_str,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to set contract symbol")?;

        // Initialize total supply to 0
        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u64_to_storage_value(0),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize total supply")?;

        // Initialize enumeration flag (default: false for gas savings)
        let enumeration_enabled = enable_enumeration.unwrap_or(false);
        contract_storage
            .sstore(
                storage,
                SLOT_ENUMERATION_ENABLED,
                Self::bool_to_storage_value(enumeration_enabled),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize enumeration flag")?;

        // Initialize allTokens array length to 0 (if enumeration enabled)
        if enumeration_enabled {
            contract_storage
                .sstore(
                    storage,
                    SLOT_ALL_TOKENS_BASE,
                    Self::u64_to_storage_value(0),
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| "Failed to initialize allTokens array")?;
        }

        // Initialize burn flag (default: false for non-burnable collections)
        let burn_enabled = enable_burn.unwrap_or(false);
        contract_storage
            .sstore(
                storage,
                SLOT_BURN_ENABLED,
                Self::bool_to_storage_value(burn_enabled),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize burn flag")?;

        Ok(())
    }

    /// Sets the contract name
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `name` - Contract name string
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if write fails
    fn set_name(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        name: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let name_bytes = name.as_bytes();
        Self::write_uri_to_storage(
            contract_storage,
            storage,
            SLOT_NAME_BASE,
            name_bytes,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to write contract name")
    }

    /// Gets the contract name
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Contract name as string or error if read fails
    pub fn name(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        Self::read_uri_from_storage(
            contract_storage,
            storage,
            SLOT_NAME_BASE,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to read contract name")
    }

    /// Sets the contract symbol
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `symbol` - Contract symbol string
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if write fails
    fn set_symbol(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        symbol: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let symbol_bytes = symbol.as_bytes();
        Self::write_uri_to_storage(
            contract_storage,
            storage,
            SLOT_SYMBOL_BASE,
            symbol_bytes,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to write contract symbol")
    }

    /// Gets the contract symbol
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Contract symbol as string or error if read fails
    pub fn symbol(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        Self::read_uri_from_storage(
            contract_storage,
            storage,
            SLOT_SYMBOL_BASE,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to read contract symbol")
    }

    /// Gets the total supply of tokens minted
    ///
    /// **Gas Optimization**: Efficient storage read
    /// - Single SLOAD operation
    /// - Direct u64 return (no conversion overhead)
    /// - Gas cost target: < 300
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Total supply (u64) or error if read fails
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation (minimal gas cost)
    /// - Direct u64 return (no string conversion)
    /// - Efficient storage layout (single slot)
    pub fn total_supply(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        // Read total supply from storage (single SLOAD)
        let total_supply_value = contract_storage
            .sload(storage, SLOT_TOTAL_SUPPLY, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read total supply from storage")?;

        // Convert storage value to u64
        Self::storage_value_to_u64(&total_supply_value)
            .with_context(|| "Failed to convert total supply value to u64")
    }

    // ============================================
    // Access Control Functions
    // ============================================

    /// Checks if the caller is the contract owner (onlyOwner modifier)
    ///
    /// This function implements the onlyOwner access control pattern.
    /// It should be called at the beginning of admin functions.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime to get current caller
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if caller is not the owner
    ///
    /// # Errors
    /// - "Only owner can call this function" if caller is not owner
    pub fn only_owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read contract owner")?;

        if caller != owner {
            anyhow::bail!(
                "Only owner can call this function. Owner: {}, Caller: {}",
                hex::encode(owner),
                hex::encode(caller)
            );
        }

        Ok(())
    }

    /// Checks if the contract is not paused (whenNotPaused modifier)
    ///
    /// This function implements the whenNotPaused access control pattern.
    /// It should be called at the beginning of user functions.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if contract is paused
    ///
    /// # Errors
    /// - "Contract is paused" if contract is paused
    pub fn when_not_paused(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let is_paused =
            BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read pause state")?;

        if is_paused {
            anyhow::bail!("Contract is paused");
        }

        Ok(())
    }

    /// Helper function to check if contract is paused
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `true` if paused, `false` otherwise, or error if read fails
    pub fn is_paused(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read pause state")
    }

    /// Checks if burn is enabled (onlyBurnable modifier)
    ///
    /// This function implements the onlyBurnable access control pattern.
    /// It should be called at the beginning of burn functions.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if burn is not enabled
    ///
    /// # Errors
    /// - "Burn is not enabled" if burn functionality is disabled
    fn only_burnable(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let burn_enabled_value = contract_storage
            .sload(storage, SLOT_BURN_ENABLED, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read burn enabled flag")?;

        let burn_enabled = Self::storage_value_to_bool(&burn_enabled_value)
            .with_context(|| "Failed to convert burn flag to bool")?;

        if !burn_enabled {
            anyhow::bail!("Burn is not enabled for this collection");
        }

        Ok(())
    }

    // ============================================
    // BaseContract API Access Functions
    // ============================================

    /// Gets the contract owner address
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Owner address (32 bytes) or error if read fails
    pub fn owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<[u8; 32]> {
        BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read contract owner")
    }

    /// Transfers contract ownership to a new owner
    ///
    /// This function implements the transferOwnership functionality.
    /// Only the current owner can call this function.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime to get current caller
    /// * `new_owner` - New owner address (32 bytes)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `Ok(true)` if transfer succeeds, error otherwise
    ///
    /// # Errors
    /// - "Only owner can transfer ownership" if caller is not owner
    /// - "new_owner cannot be address zero" if new_owner is zero address
    pub fn transfer_ownership(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        new_owner: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Validate that caller is owner
        Self::only_owner(contract_storage, storage, runtime, gas_meter.as_deref_mut())
            .with_context(|| "Only owner can transfer ownership")?;

        // Transfer ownership using BaseContract
        BaseContract::transfer_ownership(
            contract_storage,
            storage,
            runtime,
            new_owner,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to transfer ownership")
    }

    /// Pauses the contract
    ///
    /// Only the contract owner can pause the contract.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime to get current caller
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `Ok(true)` if pause succeeds, error otherwise
    ///
    /// # Errors
    /// - "Only owner can pause contract" if caller is not owner
    /// - "Contract is already paused" if contract is already paused
    pub fn pause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        BaseContract::pause(contract_storage, storage, runtime, gas_meter.as_deref_mut())
            .with_context(|| "Failed to pause contract")
    }

    /// Unpauses the contract
    ///
    /// Only the contract owner can unpause the contract.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime to get current caller
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `Ok(true)` if unpause succeeds, error otherwise
    ///
    /// # Errors
    /// - "Only owner can unpause contract" if caller is not owner
    /// - "Contract is not paused" if contract is not paused
    pub fn unpause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        BaseContract::unpause(contract_storage, storage, runtime, gas_meter.as_deref_mut())
            .with_context(|| "Failed to unpause contract")
    }

    /// Gets the contract version
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Contract version (u64) or error if read fails
    pub fn version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        BaseContract::get_version(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read contract version")
    }

    // ============================================
    // Core View Functions (SAVITRI-721)
    // ============================================

    /// Gets the balance of tokens owned by an address
    ///
    /// **Gas Optimization**: Efficient storage read from balances mapping
    /// - Single SLOAD operation
    /// - Gas cost target: < 300
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address as hex string (with or without "0x" prefix)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Balance (u64) or error if invalid
    ///
    /// # Errors
    /// - "Address cannot be zero" if owner is zero address
    /// - "Failed to decode address" if address format is invalid
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation (minimal gas cost)
    /// - Efficient slot calculation (keccak256 cached by ContractStorage)
    pub fn balance_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        // Decode owner address
        let owner_bytes = Self::decode_address(owner)
            .with_context(|| format!("Failed to decode owner address: {}", owner))?;

        // Validate: address cannot be zero
        let zero_address = [0u8; 32];
        if owner_bytes == zero_address {
            anyhow::bail!("Address cannot be zero");
        }

        // Calculate balance slot using keccak256
        let balance_slot = Self::owner_balance_slot(&owner_bytes)
            .with_context(|| "Failed to calculate balance slot")?;

        // Read balance from storage (single SLOAD)
        let balance_value = contract_storage
            .sload(storage, balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read balance from storage")?;

        // Convert storage value to u64
        Self::storage_value_to_u64(&balance_value)
            .with_context(|| "Failed to convert balance value to u64")
    }

    /// Gets the owner of a token
    ///
    /// **Gas Optimization**: Efficient storage read from ownership mapping
    /// - Single SLOAD operation
    /// - Early return for non-existent tokens
    /// - Gas cost target: < 300
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Owner address as hex string or error if token does not exist
    ///
    /// # Errors
    /// - "Token does not exist" if token has not been minted
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation (minimal gas cost)
    /// - Efficient slot calculation (keccak256 cached by ContractStorage)
    pub fn owner_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        // Calculate owner slot using keccak256
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        // Read owner from storage (single SLOAD)
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        // Validate: token must exist (owner cannot be zero address)
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        // Convert storage value to address
        let owner = Self::storage_value_to_address(&owner_value)
            .with_context(|| "Failed to convert owner value to address")?;

        // Encode address to hex string
        Ok(Self::encode_address(&owner))
    }

    /// Gets the approved address for a token
    ///
    /// **Gas Optimization**: Efficient storage read from approvals mapping
    /// - Two SLOAD operations (owner check + approval read)
    /// - Early return if token does not exist
    /// - Returns None if no approval (saves gas)
    /// - Gas cost target: < 300
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Option<address> - Some(address) if approved, None if no approval, or error if token does not exist
    ///
    /// # Errors
    /// - "Token does not exist" if token has not been minted
    ///
    /// # Gas Optimization
    /// - Returns None for zero approval (no conversion needed)
    /// - Efficient slot calculation (keccak256 cached)
    pub fn get_approved(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<String>> {
        // First, verify token exists by checking owner
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        // Early return: token must exist
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        // Calculate approval slot using keccak256
        let approval_slot = Self::token_approval_slot(token_id)
            .with_context(|| format!("Failed to calculate approval slot for token {}", token_id))?;

        // Read approval from storage (single SLOAD)
        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token approval for token {}", token_id))?;

        // Early return: if approval is zero, return None (no conversion needed)
        if approval_value.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        // Convert storage value to address
        let approved = Self::storage_value_to_address(&approval_value)
            .with_context(|| "Failed to convert approval value to address")?;

        // Encode address to hex string
        Ok(Some(Self::encode_address(&approved)))
    }

    /// Checks if an operator is approved for all tokens of an owner
    ///
    /// **Gas Optimization**: Efficient nested mapping lookup
    /// - Single SLOAD operation (nested mapping uses single hash)
    /// - Direct boolean return (no conversion needed)
    /// - Gas cost target: < 300
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address as hex string
    /// * `operator` - Operator address as hex string
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `true` if operator is approved, `false` otherwise, or error if invalid
    ///
    /// # Errors
    /// - "Failed to decode address" if address format is invalid
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation (nested mapping uses single hash)
    /// - Direct boolean return (no Option wrapping)
    /// - Efficient slot calculation (keccak256 cached)
    pub fn is_approved_for_all(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        operator: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Decode owner address
        let owner_bytes = Self::decode_address(owner)
            .with_context(|| format!("Failed to decode owner address: {}", owner))?;

        // Decode operator address
        let operator_bytes = Self::decode_address(operator)
            .with_context(|| format!("Failed to decode operator address: {}", operator))?;

        // Calculate operator approval slot using keccak256 (nested mapping)
        let operator_approval_slot = Self::operator_approval_slot(&owner_bytes, &operator_bytes)
            .with_context(|| "Failed to calculate operator approval slot")?;

        // Read operator approval from storage (single SLOAD)
        let approval_value = contract_storage
            .sload(storage, operator_approval_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read operator approval from storage")?;

        // Convert storage value to boolean (true if first byte is non-zero)
        Self::storage_value_to_bool(&approval_value)
            .with_context(|| "Failed to convert approval value to boolean")
    }

    // ============================================
    // State-Changing Functions
    // ============================================

    /// Mints a new token to the specified address
    ///
    /// **Gas Optimization**: Target < 100,000 gas
    /// - Batch storage updates where possible
    /// - Efficient URI storage (multi-slot if needed)
    /// - Single event emission
    ///
    /// **Pattern**: Checks-Effects-Interactions
    /// 1. Checks: Validate all inputs and conditions
    /// 2. Effects: Update storage state
    /// 3. Interactions: Emit events
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `to` - Recipient address as hex string
    /// * `token_id` - Token ID to mint (u64)
    /// * `uri` - Token URI (optional, can be empty string)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if minting fails
    ///
    /// # Errors
    /// - "Only owner can mint tokens" if caller is not owner
    /// - "Address cannot be zero" if to is zero address
    /// - "Token already minted" if tokenId already exists
    /// - "Contract is paused" if contract is paused
    ///
    /// # Gas Optimization
    /// - Batch storage updates (ownership, balance, URI, approval)
    /// - Efficient URI storage (multi-slot if needed)
    /// - Single event emission
    /// - Target: < 100,000 gas
    pub fn mint(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        to: &str,
        token_id: u64,
        uri: Option<&str>,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and conditions
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Caller must be owner (onlyOwner)
        Self::only_owner(contract_storage, storage, runtime, gas_meter.as_deref_mut())
            .with_context(|| "Only owner can mint tokens")?;

        let to_bytes = Self::decode_address(to)
            .with_context(|| format!("Failed to decode recipient address: {}", to))?;

        // Check 4: Recipient cannot be zero address
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Address cannot be zero");
        }

        // Check 5: Token must not already exist
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let current_owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if !current_owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token already minted");
        }

        // Get contract address for event emission
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // ============================================
        // EFFECTS: Update storage state (atomic updates)
        // ============================================

        // Effect 1: Set token ownership
        contract_storage
            .sstore(
                storage,
                owner_slot,
                to_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to write token owner for token {}", token_id))?;

        // Effect 2: Update recipient balance
        let balance_slot = Self::owner_balance_slot(&to_bytes)
            .with_context(|| "Failed to calculate balance slot")?;

        let balance_value = contract_storage
            .sload(storage, balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read recipient balance")?;

        let balance = Self::storage_value_to_u64(&balance_value)
            .with_context(|| "Failed to convert balance value to u64")?;

        let new_balance = balance
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

        contract_storage
            .sstore(
                storage,
                balance_slot,
                Self::u64_to_storage_value(new_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update recipient balance")?;

        // Effect 3: Store URI (multi-slot if necessary)
        if let Some(uri_str) = uri {
            if !uri_str.is_empty() {
                let uri_base_slot = Self::token_uri_base_slot(token_id).with_context(|| {
                    format!("Failed to calculate URI base slot for token {}", token_id)
                })?;

                let uri_bytes = uri_str.as_bytes();
                Self::write_uri_to_storage(
                    contract_storage,
                    storage,
                    uri_base_slot,
                    uri_bytes,
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| format!("Failed to write URI for token {}", token_id))?;
            }
        }

        // Effect 4: Clear any existing approval (set to zero)
        let approval_slot = Self::token_approval_slot(token_id)
            .with_context(|| format!("Failed to calculate approval slot for token {}", token_id))?;

        // Check if approval exists before clearing (optimization: only write if needed)
        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read approval for token {}", token_id))?;

        if !approval_value.iter().all(|&b| b == 0) {
            // Clear approval by setting to zero
            contract_storage
                .sstore(
                    storage,
                    approval_slot,
                    vec![0u8; 32],
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| format!("Failed to clear approval for token {}", token_id))?;
        }

        // Effect 5: Increment total supply
        let total_supply_value = contract_storage
            .sload(storage, SLOT_TOTAL_SUPPLY, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read total supply")?;

        let current_total_supply = Self::storage_value_to_u64(&total_supply_value)
            .with_context(|| "Failed to convert total supply value to u64")?;

        let new_total_supply = current_total_supply
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Total supply overflow"))?;

        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u64_to_storage_value(new_total_supply),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update total supply")?;

        // Effect 6: Update enumeration arrays (if enumeration is enabled)
        // Optimization: Cache enumeration flag to avoid multiple reads
        let enumeration_enabled =
            Self::is_enumeration_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .unwrap_or(false);

        if enumeration_enabled {
            // Add tokenId to allTokens array
            Self::add_to_all_tokens(
                contract_storage,
                storage,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to add token to allTokens array")?;

            // Add tokenId to ownerTokens array
            Self::add_to_owner_tokens(
                contract_storage,
                storage,
                &to_bytes,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to add token to ownerTokens array")?;
        }

        // ============================================
        // INTERACTIONS: Emit events
        // ============================================

        // Emit Transfer event: Transfer(address(0), to, tokenId)
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &zero_address,
            &to_bytes,
            token_id,
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    /// Transfers a token from one address to another
    ///
    /// **Gas Optimization**: Target < 70,000 gas
    /// - Efficient storage updates
    /// - Minimal SLOAD operations
    /// - Batch state updates
    ///
    /// **Pattern**: Checks-Effects-Interactions
    /// 1. Checks: Validate all inputs and authorization
    /// 2. Effects: Update storage state atomically
    /// 3. Interactions: Emit events
    ///
    /// **Security Patterns**:
    /// - Checks-effects-interactions pattern (prevents reentrancy)
    /// - Overflow protection for balances (checked arithmetic)
    /// - Authorization checks (owner, approved, or operator)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `from` - Sender address as hex string
    /// * `to` - Recipient address as hex string
    /// * `token_id` - Token ID to transfer (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if transfer fails
    ///
    /// # Errors
    /// - "Contract is paused" if contract is paused
    /// - "Token does not exist" if tokenId does not exist
    /// - "From is not token owner" if from is not the current owner
    /// - "Caller is not authorized" if caller is not owner, approved, or operator
    /// - "Address cannot be zero" if to is zero address
    /// - "Balance underflow" if from balance would underflow
    /// - "Balance overflow" if to balance would overflow
    ///
    /// # Gas Optimization
    /// - Efficient storage updates (batch operations)
    /// - Minimal SLOAD operations (cache values when possible)
    /// - Conditional writes (only clear approval if exists)
    /// - Target: < 70,000 gas
    pub fn transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and authorization
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Get caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        let from_bytes = Self::decode_address(from)
            .with_context(|| format!("Failed to decode from address: {}", from))?;

        let to_bytes = Self::decode_address(to)
            .with_context(|| format!("Failed to decode to address: {}", to))?;

        // Check 4: Recipient cannot be zero address
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Address cannot be zero");
        }

        // Check 5: Token must exist and from must be owner
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let token_owner = Self::storage_value_to_address(&owner_value)
            .with_context(|| "Failed to convert owner value to address")?;

        if token_owner != from_bytes {
            anyhow::bail!("From is not token owner");
        }

        // Check 6: Caller must be authorized (owner OR approved OR operator)
        let caller_is_owner = caller == token_owner;

        // Check if caller is approved for this token
        let approval_slot = Self::token_approval_slot(token_id)
            .with_context(|| format!("Failed to calculate approval slot for token {}", token_id))?;

        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token approval for token {}", token_id))?;

        let caller_is_approved = if approval_value.iter().all(|&b| b == 0) {
            false
        } else {
            let approved = Self::storage_value_to_address(&approval_value)
                .with_context(|| "Failed to convert approval value to address")?;
            caller == approved
        };

        // Check if caller is an approved operator for all tokens
        let caller_is_operator = Self::is_approved_for_all(
            contract_storage,
            storage,
            &Self::encode_address(&token_owner),
            &Self::encode_address(&caller),
            gas_meter.as_deref_mut(),
        )
        .unwrap_or(false);

        // Authorization check: caller must be owner, approved, or operator
        if !caller_is_owner && !caller_is_approved && !caller_is_operator {
            anyhow::bail!(
                "Caller is not authorized. Caller: {}, Owner: {}, Approved: {}, Operator: {}",
                hex::encode(caller),
                hex::encode(token_owner),
                if caller_is_approved { "yes" } else { "no" },
                if caller_is_operator { "yes" } else { "no" }
            );
        }

        // Check 7: Early return if from == to (no-op transfer)
        if from_bytes == to_bytes {
            // Still emit event for consistency (some implementations do this)
            let contract_address = runtime
                .current_contract_address()
                .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

            let event_system = runtime.event_system();
            Self::emit_transfer_event(
                &event_system,
                &contract_address,
                &from_bytes,
                &to_bytes,
                token_id,
                gas_meter.as_deref_mut(),
            );
            return Ok(());
        }

        // Get contract address for event emission
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // ============================================
        // EFFECTS: Update storage state (atomic updates)
        // ============================================

        // Effect 1: Update token ownership (from => to)
        contract_storage
            .sstore(
                storage,
                owner_slot,
                to_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to update token owner for token {}", token_id))?;

        // Effect 2: Clear token approval (if exists)
        if !approval_value.iter().all(|&b| b == 0) {
            contract_storage
                .sstore(
                    storage,
                    approval_slot,
                    vec![0u8; 32],
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| format!("Failed to clear approval for token {}", token_id))?;
        }

        // Effect 3: Decrement from balance
        let from_balance_slot = Self::owner_balance_slot(&from_bytes)
            .with_context(|| "Failed to calculate from balance slot")?;

        let from_balance_value = contract_storage
            .sload(storage, from_balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read from balance")?;

        let from_balance = Self::storage_value_to_u64(&from_balance_value)
            .with_context(|| "Failed to convert from balance value to u64")?;

        let new_from_balance = from_balance
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;

        contract_storage
            .sstore(
                storage,
                from_balance_slot,
                Self::u64_to_storage_value(new_from_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update from balance")?;

        // Effect 4: Increment to balance
        let to_balance_slot = Self::owner_balance_slot(&to_bytes)
            .with_context(|| "Failed to calculate to balance slot")?;

        let to_balance_value = contract_storage
            .sload(storage, to_balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read to balance")?;

        let to_balance = Self::storage_value_to_u64(&to_balance_value)
            .with_context(|| "Failed to convert to balance value to u64")?;

        let new_to_balance = to_balance
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

        contract_storage
            .sstore(
                storage,
                to_balance_slot,
                Self::u64_to_storage_value(new_to_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update to balance")?;

        // Note: Operator approvals are preserved (not cleared on transfer)
        // This is standard SAVITRI-721 behavior

        // Effect 5: Update enumeration arrays (if enumeration is enabled)
        // Optimization: Cache enumeration flag to avoid multiple reads
        let enumeration_enabled =
            Self::is_enumeration_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .unwrap_or(false);

        if enumeration_enabled {
            // Remove tokenId from from's ownerTokens array
            Self::remove_from_owner_tokens(
                contract_storage,
                storage,
                &from_bytes,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to remove token from from's ownerTokens array")?;

            // Add tokenId to to's ownerTokens array
            Self::add_to_owner_tokens(
                contract_storage,
                storage,
                &to_bytes,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to add token to to's ownerTokens array")?;

            // Note: allTokens array is not modified on transfer (token still exists)
        }

        // ============================================
        // INTERACTIONS: Emit events
        // ============================================

        // Emit Transfer event: Transfer(from, to, tokenId)
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &from_bytes,
            &to_bytes,
            token_id,
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    /// Approves an address to transfer a specific token
    ///
    /// **Gas Optimization**: Target < 25,000 gas
    /// - Efficient storage write
    /// - Single event emission
    ///
    /// **Security Considerations**:
    /// - Prevents self-approval (approved != owner)
    /// - Clear approval semantics
    /// - Authorization check (owner OR operator)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `approved` - Address to approve (hex string)
    /// * `token_id` - Token ID to approve (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if approval fails
    ///
    /// # Errors
    /// - "Contract is paused" if contract is paused
    /// - "Token does not exist" if tokenId does not exist
    /// - "Caller is not authorized" if caller is not owner or operator
    /// - "Cannot approve owner" if approved address is the token owner
    ///
    /// # Gas Optimization
    /// - Efficient storage write (single SSTORE)
    /// - Single event emission
    /// - Target: < 25,000 gas
    pub fn approve(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        approved: &str,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and authorization
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Get caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Check 3: Token must exist
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let token_owner = Self::storage_value_to_address(&owner_value)
            .with_context(|| "Failed to convert owner value to address")?;

        // Check 4: Caller must be authorized (owner OR operator)
        let caller_is_owner = caller == token_owner;

        // Optimization: Only check operator approval if caller is not owner (saves gas)
        let caller_is_operator = if caller_is_owner {
            false // Skip operator check if already owner
        } else {
            Self::is_approved_for_all(
                contract_storage,
                storage,
                &Self::encode_address(&token_owner),
                &Self::encode_address(&caller),
                gas_meter.as_deref_mut(),
            )
            .unwrap_or(false)
        };

        if !caller_is_owner && !caller_is_operator {
            anyhow::bail!(
                "Caller is not authorized. Caller: {}, Owner: {}, Operator: {}",
                hex::encode(caller),
                hex::encode(token_owner),
                if caller_is_operator { "yes" } else { "no" }
            );
        }

        let approved_bytes = Self::decode_address(approved)
            .with_context(|| format!("Failed to decode approved address: {}", approved))?;

        // Check 6: Approved cannot be owner (self-approval prevention)
        if approved_bytes == token_owner {
            anyhow::bail!("Cannot approve owner");
        }

        // Get contract address for event emission
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // ============================================
        // EFFECTS: Update storage state
        // ============================================

        // Update single token approval
        let approval_slot = Self::token_approval_slot(token_id)
            .with_context(|| format!("Failed to calculate approval slot for token {}", token_id))?;

        contract_storage
            .sstore(
                storage,
                approval_slot,
                approved_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to write token approval for token {}", token_id))?;

        // ============================================
        // INTERACTIONS: Emit events
        // ============================================

        // Emit Approval event: Approval(owner, approved, tokenId)
        let event_system = runtime.event_system();
        Self::emit_approval_event(
            &event_system,
            &contract_address,
            &token_owner,
            &approved_bytes,
            token_id,
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    /// Sets or revokes approval for an operator to manage all tokens
    ///
    /// **Gas Optimization**: Target < 25,000 gas
    /// - Efficient storage write
    /// - Single event emission
    ///
    /// **Security Considerations**:
    /// - Prevents self-approval (operator != owner)
    /// - Clear approval semantics
    /// - Owner-only authorization
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `operator` - Operator address (hex string)
    /// * `approved` - Whether to approve (true) or revoke (false)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if approval fails
    ///
    /// # Errors
    /// - "Contract is paused" if contract is paused
    /// - "Caller is not owner" if caller is not the contract owner
    /// - "Cannot approve owner" if operator address is the owner
    ///
    /// # Gas Optimization
    /// - Efficient storage write (single SSTORE)
    /// - Single event emission
    /// - Target: < 25,000 gas
    pub fn set_approval_for_all(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        operator: &str,
        approved: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and authorization
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Get caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Check 3: Caller is the token owner context for operator approvals.
        // Operator approvals are per-wallet, not restricted to the contract owner.
        let owner = caller;

        let operator_bytes = Self::decode_address(operator)
            .with_context(|| format!("Failed to decode operator address: {}", operator))?;

        // Check 5: Operator cannot be owner (self-approval prevention)
        if operator_bytes == owner {
            anyhow::bail!("Cannot approve owner as operator");
        }

        // Get contract address for event emission
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // ============================================
        // EFFECTS: Update storage state
        // ============================================

        // Update nested mapping operator approvals
        let operator_approval_slot = Self::operator_approval_slot(&owner, &operator_bytes)
            .with_context(|| "Failed to calculate operator approval slot")?;

        // Write approval value (bool to storage value)
        let approval_value = Self::bool_to_storage_value(approved);

        contract_storage
            .sstore(
                storage,
                operator_approval_slot,
                approval_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write operator approval")?;

        // ============================================
        // INTERACTIONS: Emit events
        // ============================================

        // Emit ApprovalForAll event: ApprovalForAll(owner, operator, approved)
        let event_system = runtime.event_system();
        Self::emit_approval_for_all_event(
            &event_system,
            &contract_address,
            &owner,
            &operator_bytes,
            approved,
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    // ============================================
    // Safe Transfer Functions
    // ============================================

    /// Calculates the magic value for onSNT1Received
    ///
    /// This is the value that must be returned by onSNT1Received to indicate
    /// that the receiver accepts the token.
    ///
    /// **Magic Value**: `bytes4(keccak256("onSNT1Received(address,address,uint256,bytes)"))`
    ///
    /// # Returns
    /// Magic value as 4-byte array
    fn on_snt1_received_magic_value() -> [u8; 4] {
        use crate::contracts::call::CallTransaction;
        CallTransaction::calculate_selector("onSNT1Received(address,address,uint256,bytes)")
    }

    /// Calculates the function selector for onSNT1Received
    ///
    /// This is the same as the magic value that must be returned.
    ///
    /// # Returns
    /// Function selector as 4-byte array
    fn on_snt1_received_selector() -> [u8; 4] {
        Self::on_snt1_received_magic_value()
    }

    /// Validates the receiver for safeTransferFrom
    ///
    /// **Complete Receiver Contract Validation**:
    /// - Contract existence check using storage layer (gas-efficient)
    /// - onSNT1Received interface call for contracts
    /// - Magic value verification (0x150b7a02)
    /// - Proper ABI encoding for all parameters
    /// - Gas limit enforcement (50,000 gas)
    /// - Malicious contract handling (revert on failure)
    ///
    /// **Security Patterns**:
    /// - Validation happens BEFORE transfer (prevents transfers to non-compatible contracts)
    /// - Gas limit enforcement prevents gas exhaustion attacks
    /// - Magic value verification ensures interface implementation
    /// - Error handling for malicious contracts (revert, gas exhaustion)
    ///
    /// **Contract Detection**:
    /// - Uses `storage.contract_exists()` for efficient detection
    /// - Handles edge cases (contracts in deployment)
    /// - No extcodesize check needed (handled by storage layer)
    ///
    /// **onSNT1Received Interface**:
    /// - Function selector: `bytes4(keccak256("onSNT1Received(address,address,uint256,bytes)"))`
    /// - Parameters: operator, from, tokenId, data
    /// - Return value: bytes4 magic value (must match selector)
    /// - Gas limit: 50,000 (standard SNT1)
    ///
    /// **ABI Encoding**:
    /// - Proper parameter encoding (32 bytes per parameter)
    /// - Type correctness (address, uint256, bytes)
    /// - Data parameter handling (dynamic array with offset)
    /// - Padding to 32-byte boundaries
    ///
    /// If the receiver is a contract, calls onSNT1Received and verifies that
    /// it returns the magic value. If the receiver is not a contract (EOA),
    ///
    /// # Arguments
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `receiver` - Receiver address (32 bytes)
    /// * `operator` - Operator address (32 bytes)
    /// * `from` - Sender address (32 bytes)
    /// * `token_id` - Token ID (u64)
    /// * `data` - Additional data (bytes)
    /// * `gas_meter` - Optional gas meter (required if receiver is contract)
    ///
    /// # Returns
    ///
    /// # Errors
    /// - "Failed to check if receiver is a contract" if contract check fails
    /// - "Failed to call onSNT1Received" if contract call fails (revert, gas exhaustion, etc.)
    /// - "onSNT1Received returned invalid data" if return data is invalid
    /// - "onSNT1Received returned invalid magic value" if magic value doesn't match
    ///
    /// # Security Considerations
    /// - Gas limit enforcement: prevents gas exhaustion attacks
    /// - Malicious contract handling: reverts on any failure
    /// - Interface verification: ensures correct implementation
    fn validate_receiver(
        storage: &Storage,
        runtime: &Runtime,
        receiver: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        token_id: u64,
        data: &[u8],
        gas_meter: &mut Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // Contract Existence Check
        // ============================================

        // Check if receiver is a contract using storage layer
        // This is gas-efficient and handles edge cases (contracts in deployment)
        // The storage layer handles extcodesize checks internally
        let is_contract = storage
            .contract_exists(receiver)
            .with_context(|| "Failed to check if receiver is a contract")?;

        if !is_contract {
            return Ok(());
        }

        // ============================================
        // Contract Interface Validation
        // ============================================

        // This prevents transfers to contracts that don't handle tokens correctly

        // Gas meter is required for contract calls
        let gas_meter_mut = gas_meter
            .as_deref_mut()
            .ok_or_else(|| anyhow::anyhow!("Gas meter required for receiver validation"))?;

        // ============================================
        // ABI Encoding for onSNT1Received
        // ============================================

        // Prepare calldata for onSNT1Received(address operator, address from, uint256 tokenId, bytes data)
        // ABI encoding format:
        // - operator (32 bytes, padded)
        // - from (32 bytes, padded)
        // - tokenId (32 bytes, padded, big-endian)
        // - data offset (32 bytes) - 0x80 (128) for 4 parameters
        // - data length (32 bytes)
        // - data (padded to multiple of 32 bytes)
        let mut calldata = Vec::new();

        // operator (32 bytes, padded)
        calldata.extend_from_slice(operator);

        // from (32 bytes, padded)
        calldata.extend_from_slice(from);

        // tokenId (32 bytes, padded) - ABI encoding uses big-endian
        let mut token_id_bytes = vec![0u8; 32];
        token_id_bytes[24..32].copy_from_slice(&token_id.to_be_bytes());
        calldata.extend_from_slice(&token_id_bytes);

        // data offset (32 bytes) - offset is 0x80 (128) because we have 4 parameters of 32 bytes each
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&128u32.to_be_bytes());

        // data length (32 bytes)
        let data_len = data.len() as u32;
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&data_len.to_be_bytes());

        // data (padded to multiple of 32 bytes)
        if !data.is_empty() {
            calldata.extend_from_slice(data);
            let padding = (32 - (data.len() % 32)) % 32;
            calldata.extend(vec![0u8; padding]);
        }

        // ============================================
        // Contract Call Implementation
        // ============================================

        // Call onSNT1Received on the receiver contract
        // Gas limit is enforced to prevent gas exhaustion attacks
        use crate::contracts::call::CallTransaction;
        let selector = Self::on_snt1_received_selector();

        // Call contract with gas limit enforcement
        // Gas limit: 50,000 (standard SNT1)
        // This prevents malicious contracts from exhausting gas
        let return_data = CallTransaction::call_contract(
            *receiver,
            selector,
            calldata,
            Some(50000), // Gas limit for onSNT1Received (standard SNT1)
            storage,
            runtime,
            gas_meter_mut,
        )
        .map_err(|e| {
            // Error handling for call failures:
            // - Contract revert (doesn't implement interface)
            // - Gas exhaustion (malicious contract)
            // - Call failure (contract error)
            anyhow::anyhow!(
                "Failed to call onSNT1Received on receiver contract: {}. This may indicate that the contract does not implement the onSNT1Received interface or encountered an error.",
                e
            )
        })?;

        // ============================================
        // ============================================

        // Verify that return data is exactly the magic value (4 bytes)
        // The magic value must match the function selector
        // This ensures the contract correctly implements the interface
        if return_data.len() < 4 {
            anyhow::bail!(
                "onSNT1Received returned invalid data: expected at least 4 bytes, got {}. The contract may not implement the interface correctly.",
                return_data.len()
            );
        }

        // Extract first 4 bytes as magic value
        let returned_magic = [
            return_data[0],
            return_data[1],
            return_data[2],
            return_data[3],
        ];

        // Compare with expected magic value
        // Expected: bytes4(keccak256("onSNT1Received(address,address,uint256,bytes)"))
        let expected_magic = Self::on_snt1_received_magic_value();
        if returned_magic != expected_magic {
            anyhow::bail!(
                "onSNT1Received returned invalid magic value: expected {:?}, got {:?}. The contract does not implement the interface correctly.",
                hex::encode(expected_magic),
                hex::encode(returned_magic)
            );
        }

        // Validation passed: contract implements onSNT1Received correctly

        Ok(())
    }

    /// Safely transfers a token from one address to another
    ///
    /// is a contract, it must implement the onSNT1Received interface.
    ///
    /// **Gas Optimization**: Target < 100,000 gas
    /// - Efficient contract detection
    /// - Minimal overhead for EOA transfers
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `from` - Sender address as hex string
    /// * `to` - Recipient address as hex string
    /// * `token_id` - Token ID to transfer (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if transfer fails
    ///
    /// # Errors
    /// - All errors from transferFrom
    /// - "Cannot transfer to zero address" if to is zero address
    /// - "Failed to call onSNT1Received" if receiver contract call fails
    /// - "onSNT1Received returned invalid magic value" if receiver doesn't implement interface
    ///
    /// # Gas Optimization
    /// - Efficient contract detection
    /// - Minimal overhead for EOA transfers
    /// - Target: < 100,000 gas
    pub fn safe_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        Self::safe_transfer_from_with_data(
            contract_storage,
            storage,
            runtime,
            from,
            to,
            token_id,
            &[],
            gas_meter,
        )
    }

    /// Safely transfers a token from one address to another with additional data
    ///
    /// is a contract, it must implement the onSNT1Received interface and the
    /// additional data will be passed to it.
    ///
    /// **Gas Optimization**: Target < 100,000 gas
    /// - Efficient contract detection
    /// - Minimal overhead for EOA transfers
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `from` - Sender address as hex string
    /// * `to` - Recipient address as hex string
    /// * `token_id` - Token ID to transfer (u64)
    /// * `data` - Additional data to pass to receiver (bytes)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if transfer fails
    ///
    /// # Errors
    /// - All errors from transferFrom
    /// - "Cannot transfer to zero address" if to is zero address
    /// - "Failed to call onSNT1Received" if receiver contract call fails
    /// - "onSNT1Received returned invalid magic value" if receiver doesn't implement interface
    ///
    /// # Gas Optimization
    /// - Efficient contract detection
    /// - Minimal overhead for EOA transfers
    /// - Target: < 100,000 gas
    pub fn safe_transfer_from_with_data(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        data: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Decode addresses
        let from_bytes = Self::decode_address(from)
            .with_context(|| format!("Failed to decode from address: {}", from))?;

        let to_bytes = Self::decode_address(to)
            .with_context(|| format!("Failed to decode to address: {}", to))?;

        // Verify that receiver is not zero address
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Cannot transfer to zero address");
        }

        // Get caller (operator) from runtime
        let operator = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Validate receiver (calls onSNT1Received if it's a contract)
        Self::validate_receiver(
            storage,
            runtime,
            &to_bytes,
            &operator,
            &from_bytes,
            token_id,
            data,
            &mut gas_meter,
        )?;

        Self::transfer_from(
            contract_storage,
            storage,
            runtime,
            from,
            to,
            token_id,
            gas_meter,
        )
    }

    // ============================================
    // URI and Metadata Management
    // ============================================

    /// Sets the URI for a token
    ///
    /// **Authorization**: Only token owner OR contract owner can set token URI
    /// **Storage Optimization**: Uses multi-slot storage for URIs > 24 bytes
    /// **Support**: URIs up to 1000+ characters
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `token_id` - Token ID (u64)
    /// * `uri` - URI string (can be IPFS, HTTP, or data URI)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if setting URI fails
    ///
    /// # Errors
    /// - "Contract is paused" if contract is paused
    /// - "Token does not exist" if tokenId does not exist
    /// - "Caller is not authorized" if caller is not token owner or contract owner
    /// - "Failed to write URI" if URI storage fails
    ///
    /// # Storage Optimization
    /// - Single slot for URIs <= 24 bytes
    /// - Multi-slot for URIs > 24 bytes (32 bytes per additional slot)
    /// - Efficient UTF-8 encoding
    /// - Minimal SLOAD operations
    pub fn set_token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        token_id: u64,
        uri: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and authorization
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Get caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Check 3: Token must exist
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let token_owner = Self::storage_value_to_address(&owner_value)
            .with_context(|| "Failed to convert owner value to address")?;

        // Check 4: Get contract owner
        let contract_owner =
            BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read contract owner")?;

        // Check 5: Caller must be authorized (token owner OR contract owner)
        let caller_is_token_owner = caller == token_owner;
        let caller_is_contract_owner = caller == contract_owner;

        if !caller_is_token_owner && !caller_is_contract_owner {
            anyhow::bail!(
                "Caller is not authorized. Caller: {}, Token Owner: {}, Contract Owner: {}",
                hex::encode(caller),
                hex::encode(token_owner),
                hex::encode(contract_owner)
            );
        }

        // ============================================
        // EFFECTS: Update storage state
        // ============================================

        // Calculate URI base slot
        let uri_base_slot = Self::token_uri_base_slot(token_id)
            .with_context(|| format!("Failed to calculate URI base slot for token {}", token_id))?;

        // Convert URI to bytes (UTF-8 encoding)
        let uri_bytes = uri.as_bytes();

        // Write URI to storage (multi-slot if necessary)
        Self::write_uri_to_storage(
            contract_storage,
            storage,
            uri_base_slot,
            uri_bytes,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| format!("Failed to write URI for token {}", token_id))?;

        Ok(())
    }

    /// Gets the URI for a token
    ///
    /// **Storage Optimization**: Efficient multi-slot reconstruction for long URIs
    /// **Support**: Reads URIs up to 1000+ characters
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// URI as string, or empty string if not set, or error if read fails
    ///
    /// # Errors
    /// - "Token does not exist" if tokenId does not exist
    /// - "Failed to read URI" if URI read fails
    /// - "Invalid UTF-8 in token URI" if URI contains invalid UTF-8
    ///
    /// # Storage Optimization
    /// - Single SLOAD for URIs <= 24 bytes
    /// - Minimal SLOAD operations for long URIs (only necessary slots)
    /// - Efficient UTF-8 reconstruction
    /// - Early return for empty URIs
    pub fn token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        // ============================================
        // CHECKS: Validate token exists
        // ============================================

        // Check: Token must exist
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        // ============================================
        // EFFECTS: Read URI from storage
        // ============================================

        // Calculate URI base slot
        let uri_base_slot = Self::token_uri_base_slot(token_id)
            .with_context(|| format!("Failed to calculate URI base slot for token {}", token_id))?;

        // Read URI from storage (multi-slot if necessary)
        // Returns empty string if URI is not set (length == 0)
        Self::read_uri_from_storage(
            contract_storage,
            storage,
            uri_base_slot,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| format!("Failed to read URI for token {}", token_id))
    }

    // ============================================
    // Enumeration Functions (Optional)
    // ============================================

    /// Checks if enumeration is enabled
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `true` if enumeration is enabled, `false` otherwise
    fn is_enumeration_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let enabled_value = contract_storage
            .sload(storage, SLOT_ENUMERATION_ENABLED, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read enumeration enabled flag")?;
        Self::storage_value_to_bool(&enabled_value)
            .with_context(|| "Failed to convert enumeration flag to bool")
    }

    /// Calculates the slot for an array element at a given index
    ///
    /// Pattern: keccak256(array_base_slot) + index
    ///
    /// # Arguments
    /// * `array_base_slot` - Base slot of the array (contains length)
    /// * `index` - Index of the element
    ///
    /// # Returns
    /// Slot number for the element
    fn array_element_slot(array_base_slot: u64, index: u64) -> Result<u64> {
        // Calculate keccak256(array_base_slot)
        let mut hasher = Keccak256::new();
        hasher.update(&array_base_slot.to_le_bytes());
        let hash = hasher.finalize();

        // Convert first 8 bytes to u64
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        let base_hash_slot = u64::from_le_bytes(slot_bytes);

        // Add index to get element slot
        base_hash_slot
            .checked_add(index)
            .ok_or_else(|| anyhow::anyhow!("Array element slot overflow"))
    }

    /// Gets the length of an array stored at a base slot
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `array_base_slot` - Base slot of the array
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Array length (u64) or error
    fn get_array_length(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        array_base_slot: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let length_value = contract_storage
            .sload(storage, array_base_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read array length")?;
        Self::storage_value_to_u64(&length_value)
            .with_context(|| "Failed to convert array length to u64")
    }

    /// Sets the length of an array stored at a base slot
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `array_base_slot` - Base slot of the array
    /// * `length` - New array length
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if write fails
    fn set_array_length(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        array_base_slot: u64,
        length: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        contract_storage
            .sstore(
                storage,
                array_base_slot,
                Self::u64_to_storage_value(length),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write array length")
    }

    /// Gets an element from an array at a given index
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `array_base_slot` - Base slot of the array
    /// * `index` - Index of the element
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Element value (u64) or error
    fn get_array_element(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        array_base_slot: u64,
        index: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let element_slot = Self::array_element_slot(array_base_slot, index)
            .with_context(|| format!("Failed to calculate element slot for index {}", index))?;

        let element_value = contract_storage
            .sload(storage, element_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read array element at index {}", index))?;

        Self::storage_value_to_u64(&element_value)
            .with_context(|| format!("Failed to convert array element to u64 at index {}", index))
    }

    /// Sets an element in an array at a given index
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `array_base_slot` - Base slot of the array
    /// * `index` - Index of the element
    /// * `value` - Value to set (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if write fails
    fn set_array_element(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        array_base_slot: u64,
        index: u64,
        value: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let element_slot = Self::array_element_slot(array_base_slot, index)
            .with_context(|| format!("Failed to calculate element slot for index {}", index))?;

        contract_storage
            .sstore(
                storage,
                element_slot,
                Self::u64_to_storage_value(value),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to write array element at index {}", index))
    }

    /// Adds a tokenId to the allTokens array
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID to add
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if operation fails
    fn add_to_all_tokens(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let length = Self::get_array_length(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            gas_meter.as_deref_mut(),
        )?;

        // Add tokenId at the end of the array
        Self::set_array_element(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            length,
            token_id,
            gas_meter.as_deref_mut(),
        )?;

        // Increment array length
        Self::set_array_length(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            length + 1,
            gas_meter.as_deref_mut(),
        )
    }

    /// Removes a tokenId from the allTokens array
    ///
    /// Uses swap-and-pop pattern for gas efficiency.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID to remove
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if operation fails
    fn remove_from_all_tokens(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let length = Self::get_array_length(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            gas_meter.as_deref_mut(),
        )?;

        if length == 0 {
            return Ok(()); // Array is empty, nothing to remove
        }

        // Find the index of tokenId
        let mut found_index = None;
        for i in 0..length {
            let element = Self::get_array_element(
                contract_storage,
                storage,
                SLOT_ALL_TOKENS_BASE,
                i,
                gas_meter.as_deref_mut(),
            )?;
            if element == token_id {
                found_index = Some(i);
                break;
            }
        }

        if let Some(index) = found_index {
            // Swap-and-pop: move last element to index, then decrement length
            if index < length - 1 {
                let last_element = Self::get_array_element(
                    contract_storage,
                    storage,
                    SLOT_ALL_TOKENS_BASE,
                    length - 1,
                    gas_meter.as_deref_mut(),
                )?;

                Self::set_array_element(
                    contract_storage,
                    storage,
                    SLOT_ALL_TOKENS_BASE,
                    index,
                    last_element,
                    gas_meter.as_deref_mut(),
                )?;
            }

            // Decrement array length
            Self::set_array_length(
                contract_storage,
                storage,
                SLOT_ALL_TOKENS_BASE,
                length - 1,
                gas_meter.as_deref_mut(),
            )?;
        }

        Ok(())
    }

    /// Calculates the base slot for ownerTokens array
    ///
    /// Pattern: keccak256(owner + SLOT_OWNER_TOKENS_BASE)
    ///
    /// # Arguments
    /// * `owner` - Owner address (32 bytes)
    ///
    /// # Returns
    /// Base slot for owner's token array
    fn owner_tokens_base_slot(owner: &[u8; 32]) -> Result<u64> {
        let mut hasher = Keccak256::new();
        hasher.update(owner);
        hasher.update(&SLOT_OWNER_TOKENS_BASE.to_le_bytes());
        let hash = hasher.finalize();

        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        Ok(u64::from_le_bytes(slot_bytes))
    }

    /// Adds a tokenId to ownerTokens array
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address
    /// * `token_id` - Token ID to add
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if operation fails
    fn add_to_owner_tokens(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &[u8; 32],
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let base_slot = Self::owner_tokens_base_slot(owner)
            .with_context(|| "Failed to calculate owner tokens base slot")?;

        let length = Self::get_array_length(
            contract_storage,
            storage,
            base_slot,
            gas_meter.as_deref_mut(),
        )?;

        // Add tokenId at the end of the array
        Self::set_array_element(
            contract_storage,
            storage,
            base_slot,
            length,
            token_id,
            gas_meter.as_deref_mut(),
        )?;

        // Increment array length
        Self::set_array_length(
            contract_storage,
            storage,
            base_slot,
            length + 1,
            gas_meter.as_deref_mut(),
        )
    }

    /// Removes a tokenId from ownerTokens array
    ///
    /// Uses swap-and-pop pattern for gas efficiency.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address
    /// * `token_id` - Token ID to remove
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if operation fails
    fn remove_from_owner_tokens(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &[u8; 32],
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let base_slot = Self::owner_tokens_base_slot(owner)
            .with_context(|| "Failed to calculate owner tokens base slot")?;

        let length = Self::get_array_length(
            contract_storage,
            storage,
            base_slot,
            gas_meter.as_deref_mut(),
        )?;

        if length == 0 {
            return Ok(()); // Array is empty, nothing to remove
        }

        // Find the index of tokenId
        let mut found_index = None;
        for i in 0..length {
            let element = Self::get_array_element(
                contract_storage,
                storage,
                base_slot,
                i,
                gas_meter.as_deref_mut(),
            )?;
            if element == token_id {
                found_index = Some(i);
                break;
            }
        }

        if let Some(index) = found_index {
            // Swap-and-pop: move last element to index, then decrement length
            if index < length - 1 {
                let last_element = Self::get_array_element(
                    contract_storage,
                    storage,
                    base_slot,
                    length - 1,
                    gas_meter.as_deref_mut(),
                )?;

                Self::set_array_element(
                    contract_storage,
                    storage,
                    base_slot,
                    index,
                    last_element,
                    gas_meter.as_deref_mut(),
                )?;
            }

            // Decrement array length
            Self::set_array_length(
                contract_storage,
                storage,
                base_slot,
                length - 1,
                gas_meter.as_deref_mut(),
            )?;
        }

        Ok(())
    }

    /// Gets a token by global index
    ///
    /// **Gas Optimization**: Efficient array access
    /// - Single SLOAD for array length
    /// - Single SLOAD for element
    /// - Gas cost target: < 500 gas
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `index` - Global index (0-based)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Token ID (u64) or error if index is out of bounds
    ///
    /// # Errors
    /// - "Enumeration is not enabled" if enumeration is disabled
    /// - "Index out of bounds" if index >= totalSupply
    ///
    /// # Gas Optimization
    /// - Minimal SLOAD operations (length + element)
    /// - Efficient slot calculation
    pub fn token_by_index(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        index: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        // Check if enumeration is enabled
        let enabled =
            Self::is_enumeration_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to check enumeration status")?;

        if !enabled {
            anyhow::bail!("Enumeration is not enabled");
        }

        // Get array length
        let length = Self::get_array_length(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            gas_meter.as_deref_mut(),
        )?;

        // Validate index bounds
        if index >= length {
            anyhow::bail!("Index out of bounds: index {} >= length {}", index, length);
        }

        // Get element at index
        Self::get_array_element(
            contract_storage,
            storage,
            SLOT_ALL_TOKENS_BASE,
            index,
            gas_meter.as_deref_mut(),
        )
    }

    /// Gets a token by owner index
    ///
    /// **Gas Optimization**: Efficient nested array access
    /// - Single SLOAD for array length
    /// - Single SLOAD for element
    /// - Gas cost target: < 500 gas (for single access)
    ///
    /// **⚠️ Gas Cost Warning for Large Arrays**:
    /// - This function has constant gas cost per call (< 500 gas)
    /// - However, iterating through large owner token arrays (e.g., 1,000+ tokens per owner)
    ///   will require multiple calls, each consuming gas
    /// - For owners with many tokens, consider:
    ///   - Using off-chain indexing for enumeration
    ///   - Implementing pagination in your application
    ///   - Disabling enumeration if not needed (saves gas on mint/transfer/burn)
    /// - Gas cost scales linearly with number of tokens: O(1) per call, O(n) for full enumeration
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address as hex string
    /// * `index` - Owner index (0-based)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Token ID (u64) or error if index is out of bounds
    ///
    /// # Errors
    /// - "Enumeration is not enabled" if enumeration is disabled
    /// - "Address cannot be zero" if owner is zero address
    /// - "Index out of bounds" if index >= balanceOf(owner)
    ///
    /// # Gas Optimization
    /// - Minimal SLOAD operations (length + element)
    /// - Efficient slot calculation
    pub fn token_of_owner_by_index(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        index: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        // Check if enumeration is enabled
        let enabled =
            Self::is_enumeration_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to check enumeration status")?;

        if !enabled {
            anyhow::bail!("Enumeration is not enabled");
        }

        // Decode owner address
        let owner_bytes = Self::decode_address(owner)
            .with_context(|| format!("Failed to decode owner address: {}", owner))?;

        // Validate: address cannot be zero
        let zero_address = [0u8; 32];
        if owner_bytes == zero_address {
            anyhow::bail!("Address cannot be zero");
        }

        // Calculate owner tokens base slot
        let base_slot = Self::owner_tokens_base_slot(&owner_bytes)
            .with_context(|| "Failed to calculate owner tokens base slot")?;

        // Get array length
        let length = Self::get_array_length(
            contract_storage,
            storage,
            base_slot,
            gas_meter.as_deref_mut(),
        )?;

        // Validate index bounds
        if index >= length {
            anyhow::bail!("Index out of bounds: index {} >= length {}", index, length);
        }

        // Get element at index
        Self::get_array_element(
            contract_storage,
            storage,
            base_slot,
            index,
            gas_meter.as_deref_mut(),
        )
    }

    // ============================================
    // Burn Function (Optional)
    // ============================================

    /// Burns a token, removing it from circulation
    ///
    /// **Authorization**: Only token owner OR contract owner can burn tokens
    /// **Configuration**: Burn must be enabled (constructor flag)
    /// **Gas Optimization**: Target < 50,000 gas
    /// - Efficient storage cleanup
    /// - Minimal operations
    /// - Conditional enumeration updates
    ///
    /// **Pattern**: Checks-Effects-Interactions
    /// 1. Checks: Validate all inputs and authorization
    /// 2. Effects: Update storage state (cleanup)
    /// 3. Interactions: Emit events
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime for execution context
    /// * `token_id` - Token ID to burn (u64)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Error if burning fails
    ///
    /// # Errors
    /// - "Burn is not enabled" if burn functionality is disabled
    /// - "Contract is paused" if contract is paused
    /// - "Token does not exist" if tokenId does not exist
    /// - "Caller is not authorized" if caller is not token owner or contract owner
    ///
    /// # Gas Optimization
    /// - Efficient storage cleanup (batch operations)
    /// - Conditional writes (only clear if exists)
    /// - Minimal SLOAD operations
    /// - Target: < 50,000 gas
    pub fn burn(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // ============================================
        // CHECKS: Validate all inputs and authorization
        // ============================================

        // Check 1: Contract must not be paused
        Self::when_not_paused(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Contract is paused")?;

        // Check 2: Burn must be enabled
        Self::only_burnable(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Burn is not enabled")?;

        // Check 3: Get caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Check 4: Token must exist
        let owner_slot = Self::token_owner_slot(token_id)
            .with_context(|| format!("Failed to calculate owner slot for token {}", token_id))?;

        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read token owner for token {}", token_id))?;

        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let token_owner = Self::storage_value_to_address(&owner_value)
            .with_context(|| "Failed to convert owner value to address")?;

        // Check 5: Get contract owner
        let contract_owner =
            BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read contract owner")?;

        // Check 6: Caller must be authorized (token owner OR contract owner)
        let caller_is_token_owner = caller == token_owner;
        let caller_is_contract_owner = caller == contract_owner;

        if !caller_is_token_owner && !caller_is_contract_owner {
            anyhow::bail!(
                "Caller is not authorized. Caller: {}, Token Owner: {}, Contract Owner: {}",
                hex::encode(caller),
                hex::encode(token_owner),
                hex::encode(contract_owner)
            );
        }

        // Get contract address for event emission
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        let zero_address = [0u8; 32];

        // ============================================
        // EFFECTS: Update storage state (cleanup)
        // ============================================

        // Effect 1: Reset token ownership to zero address
        contract_storage
            .sstore(
                storage,
                owner_slot,
                zero_address.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to reset token owner for token {}", token_id))?;

        // Effect 2: Decrement owner balance
        let balance_slot = Self::owner_balance_slot(&token_owner)
            .with_context(|| "Failed to calculate balance slot")?;

        let balance_value = contract_storage
            .sload(storage, balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner balance")?;

        let balance = Self::storage_value_to_u64(&balance_value)
            .with_context(|| "Failed to convert balance value to u64")?;

        let new_balance = balance
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;

        contract_storage
            .sstore(
                storage,
                balance_slot,
                Self::u64_to_storage_value(new_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update owner balance")?;

        // Effect 3: Clear token approval (if exists)
        let approval_slot = Self::token_approval_slot(token_id)
            .with_context(|| format!("Failed to calculate approval slot for token {}", token_id))?;

        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read approval for token {}", token_id))?;

        if !approval_value.iter().all(|&b| b == 0) {
            contract_storage
                .sstore(
                    storage,
                    approval_slot,
                    vec![0u8; 32],
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| format!("Failed to clear approval for token {}", token_id))?;
        }

        // Effect 4: Clear URI (optional - set to empty)
        let uri_base_slot = Self::token_uri_base_slot(token_id)
            .with_context(|| format!("Failed to calculate URI base slot for token {}", token_id))?;

        // Check if URI exists before clearing (optimization)
        let uri_length = Self::read_uri_from_storage(
            contract_storage,
            storage,
            uri_base_slot,
            gas_meter.as_deref_mut(),
        )
        .map(|uri| uri.len())
        .unwrap_or(0);

        if uri_length > 0 {
            // Clear URI by writing empty string
            Self::write_uri_to_storage(
                contract_storage,
                storage,
                uri_base_slot,
                &[],
                gas_meter.as_deref_mut(),
            )
            .with_context(|| format!("Failed to clear URI for token {}", token_id))?;
        }

        // Effect 5: Decrement total supply
        let total_supply_value = contract_storage
            .sload(storage, SLOT_TOTAL_SUPPLY, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read total supply")?;

        let current_total_supply = Self::storage_value_to_u64(&total_supply_value)
            .with_context(|| "Failed to convert total supply value to u64")?;

        let new_total_supply = current_total_supply
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("Total supply underflow"))?;

        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u64_to_storage_value(new_total_supply),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update total supply")?;

        // Effect 6: Update enumeration arrays (if enumeration is enabled)
        // Optimization: Cache enumeration flag to avoid multiple reads
        let enumeration_enabled =
            Self::is_enumeration_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .unwrap_or(false);

        if enumeration_enabled {
            // Remove tokenId from allTokens array
            Self::remove_from_all_tokens(
                contract_storage,
                storage,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to remove token from allTokens array")?;

            // Remove tokenId from ownerTokens array
            Self::remove_from_owner_tokens(
                contract_storage,
                storage,
                &token_owner,
                token_id,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to remove token from ownerTokens array")?;
        }

        // ============================================
        // INTERACTIONS: Emit events
        // ============================================

        // Emit Transfer event: Transfer(owner, address(0), tokenId)
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &token_owner,
            &zero_address,
            token_id,
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    // ============================================
    // Additional Utility Functions
    // ============================================
    // Additional utility functions for enhanced NFT functionality

    /// Check if a token exists (has been minted)
    ///
    /// This is a utility function that checks if a token ID has been minted
    /// by verifying that it has a valid owner (not the zero address).
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID to check
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `true` if token exists, `false` otherwise
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation
    /// - Zero address check is efficient
    /// - Early return for non-existent tokens
    pub fn exists(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let owner_slot = Self::token_owner_slot(token_id)?;
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner for exists check")?;

        // Token exists if owner is not zero address
        let owner_address = Self::storage_value_to_address(&owner_value)?;
        Ok(owner_address != [0u8; 32])
    }

    /// Get the total number of tokens owned by an address
    ///
    /// This is a utility function that provides the balance as a u64
    /// instead of a string, which can be more efficient for calculations.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address (hex string)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Balance as u64
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation
    /// - Direct u64 conversion (no string parsing)
    /// - Efficient storage layout
    pub fn balance_of_u64(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let owner_address = Self::decode_address(owner)?;
        let balance_slot = Self::owner_balance_slot(&owner_address)?;
        let balance_value = contract_storage
            .sload(storage, balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read balance for balance_of_u64")?;

        Self::storage_value_to_u64(&balance_value)
    }

    /// Check if an address is approved for all tokens of an owner
    ///
    /// This is a utility function that provides a boolean result
    /// instead of a string, which can be more efficient for checks.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `owner` - Owner address (hex string)
    /// * `operator` - Operator address (hex string)
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// `true` if approved for all, `false` otherwise
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation (nested mapping)
    /// - Direct boolean return
    /// - Efficient slot calculation
    pub fn is_approved_for_all_bool(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        operator: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let owner_address = Self::decode_address(owner)?;
        let operator_address = Self::decode_address(operator)?;
        let approval_slot = Self::operator_approval_slot(&owner_address, &operator_address)?;
        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read operator approval for is_approved_for_all_bool")?;

        Self::storage_value_to_bool(&approval_value)
    }

    /// Get the approved address for a token (as hex string)
    ///
    /// This is a utility function that returns the approved address
    /// as a hex string instead of raw bytes, which can be more convenient
    /// for display purposes.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Approved address as hex string, or empty string if no approval
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation
    /// - Early return for zero approval
    /// - Efficient encoding
    pub fn get_approved_hex(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        let approved_address = Self::get_approved(
            contract_storage,
            storage,
            token_id,
            gas_meter.as_deref_mut(),
        )?;
        Ok(approved_address.unwrap_or_default())
    }

    /// Get the owner of a token (as hex string)
    ///
    /// This is a utility function that returns the owner address
    /// as a hex string instead of raw bytes, which can be more convenient
    /// for display purposes.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Owner address as hex string
    ///
    /// # Gas Optimization
    /// - Single SLOAD operation
    /// - Efficient encoding
    pub fn owner_of_hex(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        let owner_slot = Self::token_owner_slot(token_id)?;
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner for owner_of_hex")?;

        let owner_address = Self::storage_value_to_address(&owner_value)?;
        Ok(Self::encode_address(&owner_address))
    }

    /// Batch check if multiple tokens exist
    ///
    /// This utility function checks if multiple tokens exist in a single call,
    /// which can be more efficient than checking them individually.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_ids` - Vector of token IDs to check
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Vector of booleans indicating if each token exists
    ///
    /// # Gas Optimization
    /// - Batch processing reduces function call overhead
    /// - Efficient slot calculation caching
    /// - Early returns for invalid tokens
    pub fn batch_exists(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_ids: &[u64],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Vec<bool>> {
        let mut results = Vec::with_capacity(token_ids.len());

        for &token_id in token_ids {
            let exists = Self::exists(
                contract_storage,
                storage,
                token_id,
                gas_meter.as_deref_mut(),
            )?;
            results.push(exists);
        }

        Ok(results)
    }

    /// Example usage of helper functions for slot calculation
    ///
    /// This function demonstrates how to use the helper functions to
    /// calculate slots efficiently and safely.
    ///
    /// # Arguments
    /// * `token_id` - Example token ID
    /// * `owner` - Example owner address
    /// * `operator` - Example operator address
    ///
    /// # Returns
    /// Tuple with all calculated slots or error if invalid
    #[allow(unused)]
    pub fn example_slot_calculation(
        token_id: u64,
        owner: &[u8; 32],
        operator: &[u8; 32],
    ) -> Result<(u64, u64, u64, u64, u64)> {
        let owner_slot = Self::token_owner_slot(token_id)?;
        let balance_slot = Self::owner_balance_slot(owner)?;
        let approval_slot = Self::token_approval_slot(token_id)?;
        let uri_base_slot = Self::token_uri_base_slot(token_id)?;
        let operator_approval_slot = Self::operator_approval_slot(owner, operator)?;

        Ok((
            owner_slot,
            balance_slot,
            approval_slot,
            uri_base_slot,
            operator_approval_slot,
        ))
    }

    /// Get comprehensive token information
    ///
    /// This utility function returns multiple pieces of information
    /// about a token in a single call, which can be more efficient
    /// than calling multiple individual functions.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `token_id` - Token ID
    /// * `gas_meter` - Optional gas meter
    ///
    /// # Returns
    /// Token information including owner, URI, approval, and existence
    ///
    /// # Gas Optimization
    /// - Single call for multiple data points
    /// - Efficient slot calculation caching
    /// - Early returns for non-existent tokens
    pub fn get_token_info(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<TokenInfo> {
        let exists = Self::exists(
            contract_storage,
            storage,
            token_id,
            gas_meter.as_deref_mut(),
        )?;

        if !exists {
            return Ok(TokenInfo {
                exists: false,
                owner: String::new(),
                uri: String::new(),
                approved: String::new(),
            });
        }

        let owner = Self::owner_of_hex(
            contract_storage,
            storage,
            token_id,
            gas_meter.as_deref_mut(),
        )?;
        let uri = Self::token_uri(
            contract_storage,
            storage,
            token_id,
            gas_meter.as_deref_mut(),
        )?;
        let approved = Self::get_approved_hex(
            contract_storage,
            storage,
            token_id,
            gas_meter.as_deref_mut(),
        )?;

        Ok(TokenInfo {
            exists: true,
            owner,
            uri,
            approved,
        })
    }
}

/// Token information structure
#[derive(Debug, Clone)]
pub struct TokenInfo {
    /// Whether the token exists
    pub exists: bool,
    /// Owner address (hex string)
    pub owner: String,
    /// Token URI
    pub uri: String,
    /// Approved address (hex string, empty if none)
    pub approved: String,
}
