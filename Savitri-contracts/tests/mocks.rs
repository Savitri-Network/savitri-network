//! Mock Contracts per Testing
//!
//! Implementazioni semplificate di contratti per uso nei test:
//! - Mock SFT1 (Savitri Fungible Token)
//! - Mock SNT1 (Savitri Non Fungible Token)
//! - Mock contracts per edge cases

use anyhow::{Context, Result};
use hex;
use sha3::{Digest, Keccak256};

/// Bytecode mock per SFT1-like token
///
/// Contiene bytecode semplificato per un token fungible
/// con funzioni: balanceOf, transfer, mint, burn
pub struct MockSFT1;

impl MockSFT1 {
    /// Genera bytecode mock per un token SFT1
    ///
    /// Include implementazioni base per le funzioni standard ERC20-like.
    pub fn bytecode() -> Vec<u8> {
        // Bytecode mock semplificato con funzioni base
        let mut bytecode = vec![
            0x60, 0x60, 0x60, 0x40, // PUSH1 0x60, PUSH1 0x40
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20 (return size)
            0x60, 0x00, // PUSH1 0x00 (return offset)
        ];

        // Aggiungi funzioni mock per testing
        // balanceOf(address) selector: 0x70a08231
        bytecode.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);

        // transfer(address,uint256) selector: 0xa9059cbb
        bytecode.extend_from_slice(&[0xa9, 0x05, 0x9c, 0xbb]);

        // mint(address,uint256) selector: 0x40110f4f
        bytecode.extend_from_slice(&[0x40, 0x11, 0x0f, 0x4f]);

        // burn(uint256) selector: 0x42966c68
        bytecode.extend_from_slice(&[0x42, 0x96, 0x6c, 0x68]);

        bytecode.extend_from_slice(&[
            0x56, // JUMP (salta a dispatch table)
            0x5b, // JUMPDEST
            0x60, 0x01, // PUSH1 0x01 (return true per semplicità)
            0x60, 0x00, // PUSH1 0x00
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20
            0x60, 0x20, // PUSH1 0x20
            0xf3, // RETURN
        ]);

        // Aggiungi metadata per renderlo valido EVM bytecode
        bytecode
    }

    /// Genera bytecode hex per un token SFT1
    pub fn bytecode_hex() -> String {
        format!("0x{}", hex::encode(Self::bytecode()))
    }

    /// Genera constructor args per deploy
    ///
    /// # Arguments
    /// * `name` - Nome of the token
    /// * `symbol` - Simbolo of the token
    /// * `initial_supply` - Supply iniziale
    pub fn constructor_args(name: &str, symbol: &str, initial_supply: u128) -> Vec<u8> {
        // Encode constructor arguments
        // Format: name (string) + symbol (string) + initial_supply (uint128)
        let mut args = Vec::new();

        // Name length + name bytes
        args.extend_from_slice(&(name.len() as u32).to_be_bytes());
        args.extend_from_slice(name.as_bytes());

        // Symbol length + symbol bytes
        args.extend_from_slice(&(symbol.len() as u32).to_be_bytes());
        args.extend_from_slice(symbol.as_bytes());

        // Initial supply (128 bits = 16 bytes)
        args.extend_from_slice(&initial_supply.to_be_bytes());

        args
    }
}

/// Bytecode mock per SNT1-like NFT
///
/// Contiene bytecode semplificato per un token non-fungible
/// con funzioni: balanceOf, ownerOf, transferFrom, mint
pub struct MockSNT1;

impl MockSNT1 {
    /// Genera bytecode mock per un token SNT1
    ///
    /// Include implementazioni base per le funzioni standard ERC721-like.
    pub fn bytecode() -> Vec<u8> {
        // Bytecode mock semplificato con funzioni NFT base
        let mut bytecode = vec![
            0x60, 0x60, 0x60, 0x40, // PUSH1 0x60, PUSH1 0x40
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20 (return size)
            0x60, 0x00, // PUSH1 0x00 (return offset)
        ];

        // Aggiungi funzioni mock NFT per testing
        // balanceOf(address) selector: 0x70a08231
        bytecode.extend_from_slice(&[0x70, 0xa0, 0x82, 0x31]);

        // ownerOf(uint256) selector: 0x6352211e
        bytecode.extend_from_slice(&[0x63, 0x52, 0x21, 0x1e]);

        // transferFrom(address,address,uint256) selector: 0x23b872dd
        bytecode.extend_from_slice(&[0x23, 0xb8, 0x72, 0xdd]);

        // mint(address,uint256) selector: 0x40110f4f
        bytecode.extend_from_slice(&[0x40, 0x11, 0x0f, 0x4f]);

        // approve(address,uint256) selector: 0x095ea7b3
        bytecode.extend_from_slice(&[0x09, 0x5e, 0xa7, 0xb3]);

        // tokenURI(uint256) selector: 0xc87b56dd
        bytecode.extend_from_slice(&[0xc8, 0x7b, 0x56, 0xdd]);

        bytecode.extend_from_slice(&[
            0x56, // JUMP (salta a dispatch table)
            0x5b, // JUMPDEST
            0x60, 0x01, // PUSH1 0x01 (return true per semplicità)
            0x60, 0x00, // PUSH1 0x00
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20
            0x60, 0x20, // PUSH1 0x20
            0xf3, // RETURN
        ]);

        // Aggiungi metadata per renderlo valido EVM bytecode
        bytecode
    }

    /// Genera bytecode hex per un token SNT1
    pub fn bytecode_hex() -> String {
        format!("0x{}", hex::encode(Self::bytecode()))
    }

    /// Genera constructor args per deploy
    ///
    /// # Arguments
    /// * `name` - Nome of the NFT
    /// * `symbol` - Simbolo of the NFT
    /// * `base_uri` - Base URI per metadata
    pub fn constructor_args(name: &str, symbol: &str, base_uri: &str) -> Vec<u8> {
        let mut args = Vec::new();

        // Name
        args.extend_from_slice(&(name.len() as u32).to_be_bytes());
        args.extend_from_slice(name.as_bytes());

        // Symbol
        args.extend_from_slice(&(symbol.len() as u32).to_be_bytes());
        args.extend_from_slice(symbol.as_bytes());

        // Base URI
        args.extend_from_slice(&(base_uri.len() as u32).to_be_bytes());
        args.extend_from_slice(base_uri.as_bytes());

        args
    }
}

/// Mock contract per edge cases testing
///
/// Contiene bytecode per testare edge cases come:
/// - Re-entrancy
/// - Overflow/underflow
/// - Access control
pub struct MockEdgeCaseContract;

impl MockEdgeCaseContract {
    ///
    /// per testare i meccanismi di protezione of the framework.
    pub fn reentrancy_bytecode() -> Vec<u8> {
        let mut bytecode = vec![
            0x60, 0x60, 0x60, 0x40, // PUSH1 0x60, PUSH1 0x40
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20 (return size)
            0x60, 0x00, // PUSH1 0x00 (return offset)
        ];

        // withdraw() selector: 0x3ccfd60b
        bytecode.extend_from_slice(&[0x3c, 0xcf, 0xd6, 0x0b]);

        // deposit() selector: 0xd0e30db0
        bytecode.extend_from_slice(&[0xd0, 0xe3, 0x0d, 0xb0]);

        bytecode.extend_from_slice(&[
            0x56, // JUMP
            0x5b, // JUMPDEST
            0x60, 0x01, // PUSH1 0x01 (simulate success)
            0x60, 0x00, // PUSH1 0x00
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20
            0x60, 0x20, // PUSH1 0x20
            0xf3, // RETURN
        ]);

        bytecode
    }

    ///
    pub fn overflow_bytecode() -> Vec<u8> {
        // Bytecode mock per testare overflow/underflow
        let mut bytecode = vec![
            0x60, 0x60, 0x60, 0x40, // PUSH1 0x60, PUSH1 0x40
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20 (return size)
            0x60, 0x00, // PUSH1 0x00 (return offset)
        ];

        // add(uint256,uint256) selector: 0x771602f7
        bytecode.extend_from_slice(&[0x77, 0x16, 0x02, 0xf7]);

        // sub(uint256,uint256) selector: 0x23b872dd
        bytecode.extend_from_slice(&[0x23, 0xb8, 0x72, 0xdd]);

        // mul(uint256,uint256) selector: 0x095ea7b3
        bytecode.extend_from_slice(&[0x09, 0x5e, 0xa7, 0xb3]);

        // div(uint256,uint256) selector: 0x10f13d8c
        bytecode.extend_from_slice(&[0x10, 0xf1, 0x3d, 0x8c]);

        bytecode.extend_from_slice(&[
            0x56, // JUMP
            0x5b, // JUMPDEST
            0x60, 0x01, // PUSH1 0x01 (simulate result)
            0x60, 0x00, // PUSH1 0x00
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20
            0x60, 0x20, // PUSH1 0x20
            0xf3, // RETURN
        ]);

        bytecode
    }

    /// Genera bytecode per un contract con access control
    ///
    /// per testare i meccanismi di autorizzazione of the framework.
    pub fn access_control_bytecode() -> Vec<u8> {
        // Bytecode mock per testare access control
        let mut bytecode = vec![
            0x60, 0x60, 0x60, 0x40, // PUSH1 0x60, PUSH1 0x40
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20 (return size)
            0x60, 0x00, // PUSH1 0x00 (return offset)
        ];

        // onlyOwner() modifier selector: 0x8da5cb5b
        bytecode.extend_from_slice(&[0x8d, 0xa5, 0xcb, 0x5b]);

        // transferOwnership(address) selector: 0xf2fde38b
        bytecode.extend_from_slice(&[0xf2, 0xfd, 0xe3, 0x8b]);

        // pause() selector: 0x8456cb59
        bytecode.extend_from_slice(&[0x84, 0x56, 0xcb, 0x59]);

        // unpause() selector: 0x3f4ba83a
        bytecode.extend_from_slice(&[0x3f, 0x4b, 0xa8, 0x3a]);

        bytecode.extend_from_slice(&[
            0x56, // JUMP
            0x5b, // JUMPDEST
            0x60, 0x01, // PUSH1 0x01 (simulate access allowed)
            0x60, 0x00, // PUSH1 0x00
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 0x20
            0x60, 0x20, // PUSH1 0x20
            0xf3, // RETURN
        ]);

        bytecode
    }
}

///
/// Compute il function selector per una signature di funzione
pub fn calculate_function_selector(signature: &str) -> [u8; 4] {
    let mut hasher = Keccak256::new();
    hasher.update(signature.as_bytes());
    let hash = hasher.finalize();
    let mut selector = [0u8; 4];
    selector.copy_from_slice(&hash[0..4]);
    selector
}

/// Helper per encode calldata per una funzione
///
/// # Arguments
pub fn encode_calldata(function_signature: &str, args: &[u8]) -> Vec<u8> {
    let selector = calculate_function_selector(function_signature);
    let mut calldata = Vec::new();
    calldata.extend_from_slice(&selector);
    calldata.extend_from_slice(args);
    calldata
}

/// Helper per encode un address come calldata
pub fn encode_address(address: &[u8; 32]) -> Vec<u8> {
    address.to_vec()
}

/// Helper per encode un u256 come calldata
pub fn encode_u256(value: u128) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    bytes[16..32].copy_from_slice(&value.to_be_bytes());
    bytes
}

/// Helper per encode un u64 come calldata (per token ID)
pub fn encode_u64(value: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 32];
    bytes[24..32].copy_from_slice(&value.to_be_bytes());
    bytes
}

/// Helper per decode un u256 da return data
pub fn decode_u256(data: &[u8]) -> Result<u128> {
    if data.len() < 32 {
        anyhow::bail!("Return data too short for u256");
    }
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&data[16..32]);
    Ok(u128::from_be_bytes(bytes))
}

/// Helper per decode un u64 da return data
pub fn decode_u64(data: &[u8]) -> Result<u64> {
    if data.len() < 32 {
        anyhow::bail!("Return data too short for u64");
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[24..32]);
    Ok(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_sft1_bytecode() {
        let bytecode = MockSFT1::bytecode();
        assert!(!bytecode.is_empty());
    }

    #[test]
    fn test_mock_snt1_bytecode() {
        let bytecode = MockSNT1::bytecode();
        assert!(!bytecode.is_empty());
    }

    #[test]
    fn test_calculate_function_selector() {
        let selector = calculate_function_selector("transfer(address,uint256)");
        assert_eq!(selector.len(), 4);

        // Check che lo stesso signature produca lo stesso selector
        let selector2 = calculate_function_selector("transfer(address,uint256)");
        assert_eq!(selector, selector2);
    }

    #[test]
    fn test_encode_decode_u256() -> Result<()> {
        let value = 1_000_000u128;
        let encoded = encode_u256(value);
        let decoded = decode_u256(&encoded)?;
        assert_eq!(value, decoded);
        Ok(())
    }

    #[test]
    fn test_encode_decode_u64() -> Result<()> {
        let value = 42u64;
        let encoded = encode_u64(value);
        let decoded = decode_u64(&encoded)?;
        assert_eq!(value, decoded);
        Ok(())
    }
}
