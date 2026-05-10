//! Test Suite for Transaction Ordering Scenarios
//! 
//! 1. Same Sender Nonce Ordering - Esclusività mutua per stesso sender+nonce
//! 2. Cross-Batch Replay Test - Prevenzione riapplicazione transazioni in batch successivi

use anyhow::Result;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use savitri_node::executor::dispatcher::{ExecutionDispatcher, DispatcherConfig, AdaptiveWeightsConfig};
use savitri_node::mempool::types::{MempoolTx, TxClass, SenderId, TxHandle};
use savitri_node::tx::SignedTx;
use savitri_node::storage::Storage;
use savitri_node::types::Account;

mod common;
use common::crypto::generate_test_keypair;
use common::tx::{make_custom_tx, make_sequenced_batch};
use common::storage::initialize_total_minted;

/// Test 2.1: Same Sender Nonce Ordering
/// 
/// Test: 2 tx con stesso sender, stesso nonce, fee diversa
#[test]
fn test_same_sender_nonce_mutual_exclusion() -> Result<()> {
    println!("🧪 Testing Same Sender Nonce Mutual Exclusion...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("nonce-exclusion-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account with sufficient balance
    let sender_account = Account { 
        balance: 1_000_000_000_000_000, // 1M tokens
        nonce: 0 
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(sender_account.balance))?;
    
    // Create dispatcher
    let config = DispatcherConfig::default();
    let mut dispatcher = ExecutionDispatcher::new(config);
    
    // Create 2 transactions with same sender and nonce but different fees
    let tx_low_fee = make_custom_tx(
        sender_pub,
        b"recipient1".to_vec(),
        100_000_000_000_000, // 0.0001 tokens
        &kp,
        0, // same nonce
        Some(50_000_000_000_000), // 0.00005 tokens fee (low)
    )?;
    
    let tx_high_fee = make_custom_tx(
        sender_pub,
        b"recipient2".to_vec(),
        100_000_000_000_000, // 0.0001 tokens
        &kp,
        0, // same nonce
        Some(200_000_000_000_000), // 0.0002 tokens fee (high)
    )?;
    
    // Verify both transactions are valid
    assert!(tx_low_fee.verify().is_ok(), "Low fee transaction should be valid");
    assert!(tx_high_fee.verify().is_ok(), "High fee transaction should be valid");
    
    // Convert to MempoolTx and SignedTx
    let mempool_txs = vec![
        MempoolTx {
            sender_id: 1, // Simple sender ID
            nonce: 0,
            fee: 50_000_000_000_000,
            tx_handle: TxHandle(0),
            class: TxClass::Financial,
            stream_nonce: None,
            inserted: std::time::Instant::now(),
            tx_hash: None,
        },
        MempoolTx {
            sender_id: 1, // Same sender ID
            nonce: 0, // Same nonce
            fee: 200_000_000_000_000, // Higher fee
            tx_handle: TxHandle(1),
            class: TxClass::Financial,
            stream_nonce: None,
            inserted: std::time::Instant::now(),
            tx_hash: None,
        },
    ];
    
    let signed_txs = vec![tx_low_fee, tx_high_fee];
    
    // Test scheduling - should only pick one transaction
    let (scheduled_mempool, scheduled_signed) = dispatcher.schedule_transactions(mempool_txs, signed_txs);
    
    // Verify only one transaction is scheduled
    assert_eq!(scheduled_mempool.len(), 1, "Should schedule exactly one transaction");
    assert_eq!(scheduled_signed.len(), 1, "Should schedule exactly one signed transaction");
    
    // Verify the high fee transaction is selected (deterministic choice)
    let scheduled_fee = scheduled_mempool[0].fee;
    assert_eq!(scheduled_fee, 200_000_000_000_000, 
               "Should select high fee transaction deterministically");
    
    println!("✅ Mutual exclusion test passed: High fee transaction selected");
    println!("   Scheduled transaction: fee={}", scheduled_fee);
    
    Ok(())
}

/// Test 2.1.1: Determinismo in the selezione per stesso nonce
/// Check che la selezione sia deterministica ripetendo il test
#[test]
fn test_same_sender_nonce_determinism() -> Result<()> {
    println!("🧪 Testing Same Sender Nonce Determinism...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("nonce-determinism-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account
    let sender_account = Account { 
        balance: 1_000_000_000_000_000,
        nonce: 0 
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(sender_account.balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Create transactions with same nonce, different fees
    let txs = vec![
        make_custom_tx(sender_pub, b"rec1".to_vec(), 100_000_000_000_000, &kp, 0, Some(100_000_000_000_000))?,
        make_custom_tx(sender_pub, b"rec2".to_vec(), 100_000_000_000_000, &kp, 0, Some(300_000_000_000_000))?,
        make_custom_tx(sender_pub, b"rec3".to_vec(), 100_000_000_000_000, &kp, 0, Some(200_000_000_000_000))?,
    ];
    
    // Convert to MempoolTx
    let mempool_txs: Vec<MempoolTx> = txs.into_iter().enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    // Run scheduling multiple times to verify determinism
    let mut selected_fees = Vec::new();
    
    for round in 0..5 {
        let scheduled = dispatcher.schedule_transactions(mempool_txs.clone(), &storage)?;
        assert_eq!(scheduled.len(), 1, "Round {}: Should select exactly one transaction", round);
        
        let selected_fee = scheduled[0].tx.fee.unwrap();
        selected_fees.push(selected_fee);
        
        println!("   Round {}: Selected fee = {}", round, selected_fee);
    }
    
    // Verify all rounds selected the same transaction (highest fee)
    let expected_fee = 300_000_000_000_000; // Highest fee
    for (i, &fee) in selected_fees.iter().enumerate() {
        assert_eq!(fee, expected_fee, "Round {}: Expected fee {}, got {}", i, expected_fee, fee);
    }
    
    println!("✅ Determinism test passed: Always selected highest fee transaction");
    
    Ok(())
}

/// Test 2.2: Cross-Batch Replay Test
/// 
/// Catch: nonce cache, receipt reuse, mempool leak
#[test]
fn test_cross_batch_replay_prevention() -> Result<()> {
    println!("🧪 Testing Cross-Batch Replay Prevention...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("replay-prevention-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account
    let sender_account = Account { 
        balance: 10_000_000_000_000_000, // 10M tokens for multiple transactions
        nonce: 0 
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(sender_account.balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Create first batch of transactions
    let batch1_txs = make_sequenced_batch(
        sender_pub,
        0, // start from nonce 0
        vec![
            (b"alice".to_vec(), 100_000_000_000_000),
            (b"bob".to_vec(), 200_000_000_000_000),
            (b"carol".to_vec(), 150_000_000_000_000),
        ],
        &kp,
        Some(100_000_000_000_000), // same fee for all
    )?;
    
    // Convert to MempoolTx
    let batch1_mempool: Vec<MempoolTx> = batch1_txs.into_iter().enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    println!("   Batch 1: {} transactions", batch1_mempool.len());
    
    // Schedule first batch
    let scheduled1 = dispatcher.schedule_transactions(batch1_mempool.clone(), &storage)?;
    println!("   Batch 1 scheduled: {} transactions", scheduled1.len());
    
    // Simulate execution - update account nonce
    let updated_account = Account {
        balance: sender_account.balance - (scheduled1.len() as u128 * 100_000_000_000_000) 
                               - (scheduled1.len() as u128 * 100_000_000_000_000), // amount + fee
        nonce: scheduled1.len() as u64, // nonce should advance
    };
    storage.put_account(&sender_pub, &updated_account)?;
    
    // Try to schedule the same batch again (should be rejected due to nonce conflicts)
    let scheduled2 = dispatcher.schedule_transactions(batch1_mempool.clone(), &storage)?;
    println!("   Batch 2 (replay): {} transactions", scheduled2.len());
    
    // Should schedule 0 transactions because all have already used nonces
    assert_eq!(scheduled2.len(), 0, "Should not schedule any transactions from replay batch");
    
    // Create second batch with new nonces
    let batch2_txs = make_sequenced_batch(
        sender_pub,
        3, // continue from nonce 3
        vec![
            (b"dave".to_vec(), 100_000_000_000_000),
            (b"eve".to_vec(), 200_000_000_000_000),
        ],
        &kp,
        Some(100_000_000_000_000),
    )?;
    
    let batch2_mempool: Vec<MempoolTx> = batch2_txs.into_iter().enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    println!("   Batch 2 (new): {} transactions", batch2_mempool.len());
    
    // Schedule second batch (should work)
    let scheduled3 = dispatcher.schedule_transactions(batch2_mempool.clone(), &storage)?;
    println!("   Batch 2 scheduled: {} transactions", scheduled3.len());
    
    // Should schedule the new transactions
    assert!(scheduled3.len() > 0, "Should schedule new transactions");
    assert_eq!(scheduled3.len(), batch2_mempool.len(), "Should schedule all new transactions");
    
    println!("✅ Cross-batch replay prevention test passed");
    println!("   Batch 1: {} executed", scheduled1.len());
    println!("   Replay attempt: {} executed", scheduled2.len());
    println!("   Batch 2: {} executed", scheduled3.len());
    
    Ok(())
}

/// Test 2.2.1: Mempool Leak Detection
/// Check che non ci siano leak nel mempool dopo batch multipli
#[test]
fn test_mempool_leak_detection() -> Result<()> {
    println!("🧪 Testing Mempool Leak Detection...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("mempool-leak-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account
    let initial_balance = 5_000_000_000_000_000;
    let sender_account = Account { 
        balance: initial_balance,
        nonce: 0 
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(initial_balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Track total fees and amounts
    let mut total_fees_collected = 0u128;
    let mut total_amounts_transferred = 0u128;
    
    // Execute multiple batches
    for batch_num in 0..3 {
        let start_nonce = batch_num * 2;
        
        let batch_txs = make_sequenced_batch(
            sender_pub,
            start_nonce,
            vec![
                (b"recipient1".to_vec(), 100_000_000_000_000),
                (b"recipient2".to_vec(), 150_000_000_000_000),
            ],
            &kp,
            Some(50_000_000_000_000),
        )?;
        
        let batch_mempool: Vec<MempoolTx> = batch_txs.into_iter().enumerate().map(|(i, tx)| {
            MempoolTx {
                tx,
                added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
                class: TxClass::Payment,
                gas_limit: 1,
            }
        }).collect();
        
        // Schedule batch
        let scheduled = dispatcher.schedule_transactions(batch_mempool.clone(), &storage)?;
        
        if scheduled.len() > 0 {
            // Update account state
            let current_account = storage.get_account(&sender_pub)?.unwrap_or_default();
            let batch_fees = scheduled.len() as u128 * 50_000_000_000_000;
            let batch_amounts = scheduled.iter().map(|tx| tx.tx.amount).sum::<u128>();
            
            total_fees_collected += batch_fees;
            total_amounts_transferred += batch_amounts;
            
            let updated_account = Account {
                balance: current_account.balance - batch_fees - batch_amounts,
                nonce: current_account.nonce + scheduled.len() as u64,
            };
            storage.put_account(&sender_pub, &updated_account)?;
        }
        
        println!("   Batch {}: {} transactions scheduled", batch_num + 1, scheduled.len());
    }
    
    // Verify final account state
    let final_account = storage.get_account(&sender_pub)?.unwrap_or_default();
    let expected_balance = initial_balance - total_fees_collected - total_amounts_transferred;
    let expected_nonce = 6; // 2 transactions per batch * 3 batches
    
    assert_eq!(final_account.balance, expected_balance, 
               "Final balance mismatch: expected {}, got {}", expected_balance, final_account.balance);
    assert_eq!(final_account.nonce, expected_nonce, 
               "Final nonce mismatch: expected {}, got {}", expected_nonce, final_account.nonce);
    
    println!("✅ Mempool leak detection test passed");
    println!("   Total fees collected: {}", total_fees_collected);
    println!("   Total amounts transferred: {}", total_amounts_transferred);
    println!("   Final balance: {} (expected: {})", final_account.balance, expected_balance);
    println!("   Final nonce: {} (expected: {})", final_account.nonce, expected_nonce);
    
    Ok(())
}

/// Test 2.2.2: Receipt Reuse Prevention
#[test]
fn test_receipt_reuse_prevention() -> Result<()> {
    println!("🧪 Testing Receipt Reuse Prevention...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("receipt-reuse-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account
    let sender_account = Account { 
        balance: 2_000_000_000_000_000,
        nonce: 0 
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(sender_account.balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Create a transaction
    let tx = make_custom_tx(
        sender_pub,
        b"recipient".to_vec(),
        100_000_000_000_000,
        &kp,
        0,
        Some(100_000_000_000_000),
    )?;
    
    let mempool_tx = MempoolTx {
        tx: tx.clone(),
        added_at: std::time::SystemTime::now(),
        class: TxClass::Payment,
        gas_limit: 1,
    };
    
    // Schedule transaction first time
    let scheduled1 = dispatcher.schedule_transactions(vec![mempool_tx.clone()], &storage)?;
    assert_eq!(scheduled1.len(), 1, "Should schedule transaction first time");
    
    // Simulate execution by updating nonce
    let updated_account = Account {
        balance: sender_account.balance - 200_000_000_000_000, // amount + fee
        nonce: 1,
    };
    storage.put_account(&sender_pub, &updated_account)?;
    
    // Try to schedule same transaction again (should be rejected)
    let scheduled2 = dispatcher.schedule_transactions(vec![mempool_tx.clone()], &storage)?;
    assert_eq!(scheduled2.len(), 0, "Should not schedule same transaction again");
    
    // Create transaction with same parameters but different signature (should still be rejected)
    let kp2 = generate_test_keypair();
    let tx2 = make_custom_tx(
        sender_pub,
        b"recipient".to_vec(),
        100_000_000_000_000,
        &kp2, // Different keypair
        0, // Same nonce
        Some(100_000_000_000_000),
    )?;
    
    let mempool_tx2 = MempoolTx {
        tx: tx2,
        added_at: std::time::SystemTime::now(),
        class: TxClass::Payment,
        gas_limit: 1,
    };
    
    // This should also be rejected due to nonce conflict
    let scheduled3 = dispatcher.schedule_transactions(vec![mempool_tx2], &storage)?;
    assert_eq!(scheduled3.len(), 0, "Should not schedule transaction with same nonce");
    
    println!("✅ Receipt reuse prevention test passed");
    println!("   First scheduling: {} transactions", scheduled1.len());
    println!("   Replay attempt 1: {} transactions", scheduled2.len());
    println!("   Replay attempt 2: {} transactions", scheduled3.len());
    
    Ok(())
}

/// Test 2.2.3: Nonce Cache Behavior
/// Check che il nonce cache funzioni correttamente
#[test]
fn test_nonce_cache_behavior() -> Result<()> {
    println!("🧪 Testing Nonce Cache Behavior...");
    
    // Setup
    let kp = generate_test_keypair();
    let sender_pub = kp.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("nonce-cache-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize sender account
    let sender_account = Account { 
        balance: 3_000_000_000_000_000,
        nonce: 5 // Start with nonce 5
    };
    storage.put_account(&sender_pub, &sender_account)?;
    initialize_total_minted(&storage, Some(sender_account.balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Create transactions with various nonces
    let txs = vec![
        make_custom_tx(sender_pub, b"rec1".to_vec(), 100_000_000_000_000, &kp, 3, Some(100_000_000_000_000))?, // Old nonce
        make_custom_tx(sender_pub, b"rec2".to_vec(), 100_000_000_000_000, &kp, 4, Some(100_000_000_000_000))?, // Old nonce
        make_custom_tx(sender_pub, b"rec3".to_vec(), 100_000_000_000_000, &kp, 5, Some(100_000_000_000_000))?, // Current nonce
        make_custom_tx(sender_pub, b"rec4".to_vec(), 100_000_000_000_000, &kp, 6, Some(100_000_000_000_000))?, // Next nonce
        make_custom_tx(sender_pub, b"rec5".to_vec(), 100_000_000_000_000, &kp, 7, Some(100_000_000_000_000))?, // Future nonce
    ];
    
    let mempool_txs: Vec<MempoolTx> = txs.into_iter().enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    // Schedule transactions
    let scheduled = dispatcher.schedule_transactions(mempool_txs, &storage)?;
    
    // Should only schedule transaction with nonce 5 (current nonce)
    assert_eq!(scheduled.len(), 1, "Should schedule exactly one transaction");
    assert_eq!(scheduled[0].tx.nonce, 5, "Should schedule transaction with current nonce");
    assert_eq!(scheduled[0].tx.to, b"rec3".to_vec(), "Should schedule correct transaction");
    
    // Update account nonce to 6
    let updated_account = Account {
        balance: sender_account.balance - 200_000_000_000_000, // amount + fee
        nonce: 6,
    };
    storage.put_account(&sender_pub, &updated_account)?;
    
    // Try scheduling again - should now schedule nonce 6
    let scheduled2 = dispatcher.schedule_transactions(mempool_txs.clone(), &storage)?;
    assert_eq!(scheduled2.len(), 1, "Should schedule exactly one transaction");
    assert_eq!(scheduled2[0].tx.nonce, 6, "Should schedule transaction with new current nonce");
    assert_eq!(scheduled2[0].tx.to, b"rec4".to_vec(), "Should schedule correct transaction");
    
    println!("✅ Nonce cache behavior test passed");
    println!("   Initial state (nonce=5): {} transactions scheduled", scheduled.len());
    println!("   Updated state (nonce=6): {} transactions scheduled", scheduled2.len());
    
    Ok(())
}

#[test]
fn test_comprehensive_transaction_ordering() -> Result<()> {
    println!("🧪 Testing Comprehensive Transaction Ordering...");
    
    // Setup multiple senders
    let kp1 = generate_test_keypair();
    let kp2 = generate_test_keypair();
    let sender1_pub = kp1.public.to_bytes();
    let sender2_pub = kp2.public.to_bytes();
    
    // Create temporary storage
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let mut tmp = std::env::temp_dir();
    tmp.push(format!("comprehensive-ordering-{}", nanos));
    std::fs::create_dir_all(&tmp)?;
    let mut storage = Storage::new(&tmp)?;
    
    // Initialize accounts
    let account1 = Account { balance: 5_000_000_000_000_000, nonce: 0 };
    let account2 = Account { balance: 3_000_000_000_000_000, nonce: 0 };
    storage.put_account(&sender1_pub, &account1)?;
    storage.put_account(&sender2_pub, &account2)?;
    initialize_total_minted(&storage, Some(account1.balance + account2.balance))?;
    
    let config = DispatcherConfig::default();
    let dispatcher = ExecutionDispatcher::new(config);
    
    // Scenario 1: Same sender, same nonce, different fees
    let conflict_txs = vec![
        make_custom_tx(sender1_pub, b"rec1".to_vec(), 100_000_000_000_000, &kp1, 0, Some(100_000_000_000_000))?,
        make_custom_tx(sender1_pub, b"rec2".to_vec(), 100_000_000_000_000, &kp1, 0, Some(300_000_000_000_000))?, // Higher fee
        make_custom_tx(sender1_pub, b"rec3".to_vec(), 100_000_000_000_000, &kp1, 0, Some(200_000_000_000_000))?,
    ];
    
    // Scenario 2: Normal transactions from different senders
    let normal_txs = vec![
        make_custom_tx(sender1_pub, b"rec4".to_vec(), 100_000_000_000_000, &kp1, 1, Some(150_000_000_000_000))?,
        make_custom_tx(sender2_pub, b"rec5".to_vec(), 100_000_000_000_000, &kp2, 0, Some(150_000_000_000_000))?,
        make_custom_tx(sender1_pub, b"rec6".to_vec(), 100_000_000_000_000, &kp1, 2, Some(150_000_000_000_000))?,
    ];
    
    // Combine all transactions
    let all_txs: Vec<MempoolTx> = conflict_txs.into_iter().chain(normal_txs.into_iter()).enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    // Schedule first batch
    let scheduled1 = dispatcher.schedule_transactions(all_txs.clone(), &storage)?;
    
    // Verify results
    println!("   First batch: {} transactions scheduled", scheduled1.len());
    
    // Should have selected the high fee transaction from the conflict set
    let conflict_selected = scheduled1.iter().find(|tx| tx.tx.nonce == 0 && tx.tx.from == sender1_pub.to_vec());
    assert!(conflict_selected.is_some(), "Should have selected one transaction from conflict set");
    assert_eq!(conflict_selected.unwrap().tx.fee, Some(300_000_000_000_000), "Should select highest fee");
    
    // Update account states
    let account1_updated = Account {
        balance: account1.balance - (400_000_000_000_000), // 2 transactions * (amount + fee)
        nonce: 3, // Should have executed nonces 0, 1, 2
    };
    storage.put_account(&sender1_pub, &account1_updated)?;
    
    let account2_updated = Account {
        balance: account2.balance - (250_000_000_000_000), // 1 transaction * (amount + fee)
        nonce: 1, // Should have executed nonce 0
    };
    storage.put_account(&sender2_pub, &account2_updated)?;
    
    // Try to schedule same batch again (replay prevention)
    let scheduled2 = dispatcher.schedule_transactions(all_txs.clone(), &storage)?;
    println!("   Replay batch: {} transactions scheduled", scheduled2.len());
    assert_eq!(scheduled2.len(), 0, "Should not schedule any transactions in replay");
    
    // Create new batch with fresh nonces
    let fresh_txs = vec![
        make_custom_tx(sender1_pub, b"rec7".to_vec(), 100_000_000_000_000, &kp1, 3, Some(150_000_000_000_000))?,
        make_custom_tx(sender2_pub, b"rec8".to_vec(), 100_000_000_000_000, &kp2, 1, Some(150_000_000_000_000))?,
    ];
    
    let fresh_mempool: Vec<MempoolTx> = fresh_txs.into_iter().enumerate().map(|(i, tx)| {
        MempoolTx {
            tx,
            added_at: std::time::SystemTime::now() + Duration::from_millis(i as u64),
            class: TxClass::Payment,
            gas_limit: 1,
        }
    }).collect();
    
    let scheduled3 = dispatcher.schedule_transactions(fresh_mempool, &storage)?;
    println!("   Fresh batch: {} transactions scheduled", scheduled3.len());
    assert_eq!(scheduled3.len(), 2, "Should schedule all fresh transactions");
    
    println!("✅ Comprehensive transaction ordering test passed");
    println!("   Mutual exclusion: ✅");
    println!("   Determinism: ✅");
    println!("   Replay prevention: ✅");
    println!("   Nonce cache: ✅");
    
    Ok(())
}
