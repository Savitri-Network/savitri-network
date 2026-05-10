// 
// PROBLEMA: Gli account dei destinatari non are persistiti correttamente
// CAUSA: L'overlay viene sovrascritto o non committato correttamente

use savitri_node::storage::Storage;
use savitri_node::types::Account;
use std::collections::BTreeMap;

/// Funzione patch che applica correttamente il transfer
pub fn apply_transfer_patch(
    storage: &mut Storage,
    from_addr: &[u8],
    to_addr: &[u8],
    amount: u128,
) -> anyhow::Result<()> {
    println!("APPLYING TRANSFER PATCH: from={:?} to={:?} amount={}", from_addr, to_addr, amount);
    
    // 1. Leggi account sender
    let mut from_account = storage.get_account(from_addr)?.unwrap_or_default();
    println!("SENDER balance before: {}", from_account.balance);
    
    // 2. Leggi account receiver (crealo se non esiste)
    let mut to_account = storage.get_account(to_addr)?.unwrap_or_default();
    println!("RECEIVER balance before: {}", to_account.balance);
    
    // 3. Applica debit al sender
    from_account.debit(amount)?;
    println!("SENDER balance after debit: {}", from_account.balance);
    
    // 4. Applica credit al receiver
    to_account.credit(amount)?;
    println!("RECEIVER balance after credit: {}", to_account.balance);
    
    // 5. Salva entrambi gli account
    storage.put_account(from_addr, &from_account)?;
    storage.put_account(to_addr, &to_account)?;
    
    // 6. Check finale
    let final_from = storage.get_account(from_addr)?.unwrap_or_default();
    let final_to = storage.get_account(to_addr)?.unwrap_or_default();
    
    println!("FINAL SENDER balance: {}", final_from.balance);
    println!("FINAL RECEIVER balance: {}", final_to.balance);
    
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use savitri_node::common::storage::{create_test_storage, initialize_total_minted};
    use savitri_node::common::crypto::generate_test_keypair;
    
    #[test]
    fn test_transfer_patch_works() -> anyhow::Result<()> {
        let (mut storage, _temp_dir) = create_test_storage("test_patch")?;
        initialize_total_minted(&storage, Some(1_000_000_000_000_000_000_000u128))?;
        
        let kp = generate_test_keypair();
        let from_addr = kp.public.to_bytes();
        let to_addr = vec![9, 9, 9];
        
        // Setup account sender
        let from_account = Account { balance: 1_000_000_000_000_000, nonce: 0 };
        storage.put_account(&from_addr, &from_account)?;
        
        // Applica patch
        apply_transfer_patch(&mut storage, &from_addr, &to_addr, 100)?;
        
        // Check
        let final_from = storage.get_account(&from_addr)?.unwrap_or_default();
        let final_to = storage.get_account(&to_addr)?.unwrap_or_default();
        
        assert_eq!(final_from.balance, 1_000_000_000_000_000 - 100);
        assert_eq!(final_to.balance, 100);
        
        Ok(())
    }
}
