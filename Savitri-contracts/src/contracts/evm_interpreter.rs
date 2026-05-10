use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use primitive_types::U256;
use savitri_storage::storage::contracts::ContractInfo;
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};

pub fn execute(
    contract_info: &ContractInfo,
    contract_storage: &mut ContractStorage,
    storage: &Storage,
    runtime: &Runtime,
    gas_meter: &mut GasMeter,
    caller: [u8; 32],
    value: u128,
    calldata: &[u8],
) -> Result<Vec<u8>, String> {
    let contract_address: [u8; 32] = contract_info
        .address
        .as_slice()
        .try_into()
        .map_err(|_| "contract address must be 32 bytes".to_string())?;

    let mut vm = Vm {
        code: &contract_info.code,
        pc: 0,
        stack: Vec::new(),
        memory: Vec::new(),
        contract_storage,
        storage,
        runtime,
        gas_meter,
        contract_address,
        caller,
        value,
        calldata,
    };

    vm.run()
}

struct Vm<'a> {
    code: &'a [u8],
    pc: usize,
    stack: Vec<U256>,
    memory: Vec<u8>,
    contract_storage: &'a mut ContractStorage,
    storage: &'a Storage,
    runtime: &'a Runtime,
    gas_meter: &'a mut GasMeter,
    contract_address: [u8; 32],
    caller: [u8; 32],
    value: u128,
    calldata: &'a [u8],
}

impl<'a> Vm<'a> {
    fn run(&mut self) -> Result<Vec<u8>, String> {
        while self.pc < self.code.len() {
            self.gas_meter
                .consume(1)
                .map_err(|e| format!("gas exhausted: {}", e))?;

            let op = self.code[self.pc];
            self.pc += 1;

            match op {
                0x00 => return Ok(Vec::new()), // STOP
                0x01 => self.binop(|a, b| a.overflowing_add(b).0)?,
                0x02 => self.binop(|a, b| a.overflowing_mul(b).0)?,
                0x03 => self.binop(|a, b| a.overflowing_sub(b).0)?,
                0x04 => self.binop(|a, b| if b.is_zero() { U256::zero() } else { a / b })?,
                0x06 => self.binop(|a, b| if b.is_zero() { U256::zero() } else { a % b })?,
                0x10 => self.cmpop(|a, b| a < b)?,
                0x11 => self.cmpop(|a, b| a > b)?,
                0x12 => self.cmpop(|a, b| signed_lt(a, b))?,
                0x13 => self.cmpop(|a, b| signed_gt(a, b))?,
                0x14 => self.cmpop(|a, b| a == b)?,
                0x15 => {
                    let v = self.pop()?;
                    self.push(if v.is_zero() {
                        U256::one()
                    } else {
                        U256::zero()
                    });
                }
                0x16 => self.binop(|a, b| a & b)?,
                0x17 => self.binop(|a, b| a | b)?,
                0x18 => self.binop(|a, b| a ^ b)?,
                0x19 => {
                    let v = self.pop()?;
                    self.push(!v);
                }
                0x1a => {
                    let idx = self.pop()?;
                    let word = self.pop()?;
                    let idx_usize = u256_to_usize(&idx)?;
                    if idx_usize >= 32 {
                        self.push(U256::zero());
                    } else {
                        let shift = (31 - idx_usize) * 8;
                        let byte = ((word >> shift) & U256::from(0xffu8)).as_u32() as u8;
                        self.push(U256::from(byte));
                    }
                }
                0x1b => self.shift_left()?,
                0x1c => self.shift_right()?,
                0x20 => self.sha3()?,
                0x30 => self.push(bytes32_to_u256(&self.contract_address)),
                0x33 => self.push(bytes32_to_u256(&self.caller)),
                0x34 => self.push(U256::from(self.value)),
                0x35 => self.calldataload()?,
                0x36 => self.push(U256::from(self.calldata.len())),
                0x37 => self.calldatacopy()?,
                0x38 => self.push(U256::from(self.code.len())),
                0x39 => self.codecopy()?,
                0x42 => self.push(U256::from(self.runtime.block_timestamp())),
                0x50 => {
                    let _ = self.pop()?;
                }
                0x51 => self.mload()?,
                0x52 => self.mstore()?,
                0x53 => self.mstore8()?,
                0x54 => self.sload()?,
                0x55 => self.sstore()?,
                0x56 => self.jump()?,
                0x57 => self.jumpi()?,
                0x58 => self.push(U256::from(self.pc.saturating_sub(1))),
                0x59 => self.push(U256::from(self.memory.len())),
                0x5b => {} // JUMPDEST
                0xf3 => return self.return_data(),
                0xfd => {
                    let data = self.return_data()?;
                    return Err(format!("EVM revert: 0x{}", hex::encode(data)));
                }
                0xfe => return Err("EVM invalid opcode".to_string()),
                0x60..=0x7f => self.pushn(op)?,
                0x80..=0x8f => self.dup(op)?,
                0x90..=0x9f => self.swap(op)?,
                _ => return Err(format!("EVM opcode 0x{:02x} not yet supported", op)),
            }
        }

        Ok(Vec::new())
    }

    fn push(&mut self, value: U256) {
        self.stack.push(value);
    }

    fn pop(&mut self) -> Result<U256, String> {
        self.stack
            .pop()
            .ok_or_else(|| "EVM stack underflow".to_string())
    }

    fn peek(&self, idx_from_top: usize) -> Result<U256, String> {
        let idx = self
            .stack
            .len()
            .checked_sub(1 + idx_from_top)
            .ok_or_else(|| "EVM stack underflow".to_string())?;
        Ok(self.stack[idx])
    }

    fn binop<F>(&mut self, f: F) -> Result<(), String>
    where
        F: Fn(U256, U256) -> U256,
    {
        let b = self.pop()?;
        let a = self.pop()?;
        self.push(f(a, b));
        Ok(())
    }

    fn cmpop<F>(&mut self, f: F) -> Result<(), String>
    where
        F: Fn(U256, U256) -> bool,
    {
        let b = self.pop()?;
        let a = self.pop()?;
        self.push(if f(a, b) { U256::one() } else { U256::zero() });
        Ok(())
    }

    fn pushn(&mut self, op: u8) -> Result<(), String> {
        let n = (op - 0x5f) as usize;
        if self.pc + n > self.code.len() {
            return Err("PUSH exceeds bytecode length".to_string());
        }
        let mut buf = [0u8; 32];
        buf[32 - n..].copy_from_slice(&self.code[self.pc..self.pc + n]);
        self.pc += n;
        self.push(U256::from_big_endian(&buf));
        Ok(())
    }

    fn dup(&mut self, op: u8) -> Result<(), String> {
        let depth = (op - 0x7f) as usize;
        let v = self.peek(depth - 1)?;
        self.push(v);
        Ok(())
    }

    fn swap(&mut self, op: u8) -> Result<(), String> {
        let depth = (op - 0x8f) as usize;
        if self.stack.len() <= depth {
            return Err("EVM stack underflow".to_string());
        }
        let top = self.stack.len() - 1;
        let other = self.stack.len() - 1 - depth;
        self.stack.swap(top, other);
        Ok(())
    }

    fn shift_left(&mut self) -> Result<(), String> {
        let shift = self.pop()?;
        let value = self.pop()?;
        let s = if shift > U256::from(255u64) {
            256
        } else {
            shift.as_usize()
        };
        self.push(if s >= 256 { U256::zero() } else { value << s });
        Ok(())
    }

    fn shift_right(&mut self) -> Result<(), String> {
        let shift = self.pop()?;
        let value = self.pop()?;
        let s = if shift > U256::from(255u64) {
            256
        } else {
            shift.as_usize()
        };
        self.push(if s >= 256 { U256::zero() } else { value >> s });
        Ok(())
    }

    fn ensure_memory(&mut self, end: usize) {
        if self.memory.len() < end {
            self.memory.resize(end, 0);
        }
    }

    fn mload(&mut self) -> Result<(), String> {
        let offset = u256_to_usize(&self.pop()?)?;
        self.ensure_memory(offset.saturating_add(32));
        let mut word = [0u8; 32];
        word.copy_from_slice(&self.memory[offset..offset + 32]);
        self.push(U256::from_big_endian(&word));
        Ok(())
    }

    fn mstore(&mut self) -> Result<(), String> {
        let offset = u256_to_usize(&self.pop()?)?;
        let value = self.pop()?;
        self.ensure_memory(offset.saturating_add(32));
        let mut word = [0u8; 32];
        value.to_big_endian(&mut word);
        self.memory[offset..offset + 32].copy_from_slice(&word);
        Ok(())
    }

    fn mstore8(&mut self) -> Result<(), String> {
        let offset = u256_to_usize(&self.pop()?)?;
        let value = self.pop()?;
        self.ensure_memory(offset.saturating_add(1));
        self.memory[offset] = (value & U256::from(0xffu8)).as_u32() as u8;
        Ok(())
    }

    fn calldataload(&mut self) -> Result<(), String> {
        let offset = u256_to_usize(&self.pop()?)?;
        let mut out = [0u8; 32];
        if offset < self.calldata.len() {
            let copy_len = (self.calldata.len() - offset).min(32);
            out[..copy_len].copy_from_slice(&self.calldata[offset..offset + copy_len]);
        }
        self.push(U256::from_big_endian(&out));
        Ok(())
    }

    fn calldatacopy(&mut self) -> Result<(), String> {
        let mem_offset = u256_to_usize(&self.pop()?)?;
        let data_offset = u256_to_usize(&self.pop()?)?;
        let len = u256_to_usize(&self.pop()?)?;
        self.ensure_memory(mem_offset.saturating_add(len));

        for i in 0..len {
            let src = data_offset + i;
            self.memory[mem_offset + i] = if src < self.calldata.len() {
                self.calldata[src]
            } else {
                0
            };
        }
        Ok(())
    }

    fn codecopy(&mut self) -> Result<(), String> {
        let mem_offset = u256_to_usize(&self.pop()?)?;
        let code_offset = u256_to_usize(&self.pop()?)?;
        let len = u256_to_usize(&self.pop()?)?;
        self.ensure_memory(mem_offset.saturating_add(len));

        for i in 0..len {
            let src = code_offset + i;
            self.memory[mem_offset + i] = if src < self.code.len() {
                self.code[src]
            } else {
                0
            };
        }
        Ok(())
    }

    fn sha3(&mut self) -> Result<(), String> {
        let offset = u256_to_usize(&self.pop()?)?;
        let len = u256_to_usize(&self.pop()?)?;
        self.ensure_memory(offset.saturating_add(len));
        let hash = Keccak256::digest(&self.memory[offset..offset + len]);
        self.push(U256::from_big_endian(&hash));
        Ok(())
    }

    fn sload(&mut self) -> Result<(), String> {
        let slot = self.pop()?;
        let slot_u64 = slot_to_u64(slot);
        let value = self
            .contract_storage
            .sload(self.storage, slot_u64, Some(self.gas_meter))
            .map_err(|e| format!("sload failed: {}", e))?;
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&value[..32]);
        self.push(U256::from_big_endian(&bytes));
        Ok(())
    }

    fn sstore(&mut self) -> Result<(), String> {
        let slot = self.pop()?;
        let value = self.pop()?;
        let slot_u64 = slot_to_u64(slot);
        let mut value_bytes = [0u8; 32];
        value.to_big_endian(&mut value_bytes);

        // Keep generic EVM path writable for low slots too.
        // Current ContractStorage reserves 0..99 for BaseContract API, which is not EVM-compatible.
        self.gas_meter
            .consume_sstore(false)
            .map_err(|e| format!("sstore gas failed: {}", e))?;
        self.contract_storage
            .overlay_mut()
            .insert(slot_u64, value_bytes.to_vec());
        Ok(())
    }

    fn jump(&mut self) -> Result<(), String> {
        let dest = u256_to_usize(&self.pop()?)?;
        self.validate_jumpdest(dest)?;
        self.pc = dest;
        Ok(())
    }

    fn jumpi(&mut self) -> Result<(), String> {
        let dest = u256_to_usize(&self.pop()?)?;
        let cond = self.pop()?;
        if !cond.is_zero() {
            self.validate_jumpdest(dest)?;
            self.pc = dest;
        }
        Ok(())
    }

    fn validate_jumpdest(&self, dest: usize) -> Result<(), String> {
        if dest >= self.code.len() {
            return Err("jump destination out of bounds".to_string());
        }
        if self.code[dest] != 0x5b {
            return Err("jump destination is not JUMPDEST".to_string());
        }
        Ok(())
    }

    fn return_data(&mut self) -> Result<Vec<u8>, String> {
        let offset = u256_to_usize(&self.pop()?)?;
        let size = u256_to_usize(&self.pop()?)?;
        self.ensure_memory(offset.saturating_add(size));
        Ok(self.memory[offset..offset + size].to_vec())
    }
}

fn u256_to_usize(v: &U256) -> Result<usize, String> {
    if *v > U256::from(usize::MAX) {
        return Err("value too large for usize".to_string());
    }
    Ok(v.as_usize())
}

fn bytes32_to_u256(bytes: &[u8; 32]) -> U256 {
    U256::from_big_endian(bytes)
}

fn slot_to_u64(slot: U256) -> u64 {
    // Storage backend currently supports u64 slots.
    // Use low 64 bits deterministically for now.
    (slot & U256::from(u64::MAX)).as_u64()
}

fn signed_lt(a: U256, b: U256) -> bool {
    let sign_bit = U256::one() << 255;
    let a_neg = (a & sign_bit) != U256::zero();
    let b_neg = (b & sign_bit) != U256::zero();
    if a_neg != b_neg {
        return a_neg;
    }
    a < b
}

fn signed_gt(a: U256, b: U256) -> bool {
    let sign_bit = U256::one() << 255;
    let a_neg = (a & sign_bit) != U256::zero();
    let b_neg = (b & sign_bit) != U256::zero();
    if a_neg != b_neg {
        return b_neg;
    }
    a > b
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::runtime::Runtime;
    use crate::contracts::storage::ContractStorage;
    use savitri_storage::storage::contracts::ContractInfo;
    use savitri_storage::storage::Storage;

    fn setup() -> (Storage, Runtime, ContractStorage, ContractInfo, GasMeter) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let storage = Storage::new(tmp.path()).expect("storage");
        let runtime = Runtime::with_empty_overlay(10_000_000, 1_700_000_000);
        let address = vec![7u8; 32];
        let contract_storage = ContractStorage::new(address.clone()).expect("contract storage");
        let info = ContractInfo::new(
            address,
            vec![],
            vec![0u8; 32],
            vec![0u8; 32],
            vec![1u8; 32],
            1,
            1_700_000_000,
        );
        let gas = GasMeter::new(10_000_000);
        (storage, runtime, contract_storage, info, gas)
    }

    #[test]
    fn executes_simple_arithmetic_return() {
        let (storage, runtime, mut cs, mut info, mut gas) = setup();
        info.code = vec![
            0x60, 0x02, // PUSH1 2
            0x60, 0x03, // PUSH1 3
            0x01, // ADD
            0x60, 0x00, // PUSH1 0
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 32
            0x60, 0x00, // PUSH1 0
            0xf3, // RETURN
        ];

        let out = execute(
            &info,
            &mut cs,
            &storage,
            &runtime,
            &mut gas,
            [1u8; 32],
            0,
            &[],
        )
        .expect("vm run");
        assert_eq!(out.len(), 32);
        assert_eq!(out[31], 5);
    }

    #[test]
    fn executes_sstore_and_sload() {
        let (storage, runtime, mut cs, mut info, mut gas) = setup();
        info.code = vec![
            0x60, 0x2a, // PUSH1 42
            0x60, 0x01, // PUSH1 1
            0x55, // SSTORE
            0x60, 0x01, // PUSH1 1
            0x54, // SLOAD
            0x60, 0x00, // PUSH1 0
            0x52, // MSTORE
            0x60, 0x20, // PUSH1 32
            0x60, 0x00, // PUSH1 0
            0xf3, // RETURN
        ];

        let out = execute(
            &info,
            &mut cs,
            &storage,
            &runtime,
            &mut gas,
            [1u8; 32],
            0,
            &[],
        )
        .expect("vm run");
        assert_eq!(out[31], 42);
    }
}
