//! Savitri P2P Message Routing Example
//! 
//! This example demonstrates how to use the Savitri P2P message routing system
//! to publish, subscribe, and route messages between peers.

## 📋 Example Overview

This example showcases:
- Topic subscription and message publishing
- Message handlers for different message types
- Direct messaging between peers
- Priority-based message routing
- Performance monitoring and statistics

## 🚀 Quick Start

```bash
# Run the example
cargo run --example message_routing --release

# Run with debug output
RUST_LOG=debug cargo run --example message_routing --release
```

## 📊 Expected Output

```
📨 Savitri P2P Message Routing Example
=====================================
🚀 Starting P2P node...
✅ P2P node started successfully!
📡 Local peer ID: 12D3KooW...
📬 Subscribing to topics...
   ✅ Subscribed to /savitri/tx/1
   ✅ Subscribed to /savitri/block/1
   ✅ Subscribed to /savitri/consensus/proposal/1
   ...
🔧 Setting up message handlers...
   🔧 Transaction message handler started
   🔧 Block message handler started
   🔧 Consensus message handler started
⏳ Waiting for peer connections...
📤 Starting message publishing demo...
   📤 Publishing transaction messages...
   ✅ Published 20 transactions
   🧱 Published block #1 with 5 transactions
   ...
📊 Monitoring message routing for 60 seconds...
   📊 Routing Stats [5s]:
     Messages sent: 45
     Messages received: 38
     Duplicate rate: 2.35%
     Mesh size: 8
     ...
🎯 Demonstrating direct messaging...
   🎯 Setting up direct messaging demo...
   🎯 Sending direct message to peer: 12D3KooW...
   ✅ Direct message sent successfully
   🚀 Demonstrating priority-based message routing...
   ✅ Messages sent with different priorities
🛑 Shutting down P2P node...
✅ P2P node stopped successfully!
```

use anyhow::Result;
use savitri_p2p::{P2PManager, P2PConfig, GossipConfig, NetworkConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::time::{sleep, interval};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransactionMessage {
    id: String,
    sender: String,
    receiver: String,
    amount: u64,
    fee: u64,
    timestamp: u64,
    signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BlockMessage {
    height: u64,
    hash: String,
    previous_hash: String,
    transactions: Vec<String>,
    timestamp: u64,
    proposer: String,
    signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConsensusMessage {
    round: u64,
    proposal: Option<BlockMessage>,
    votes: Vec<String>,
    certificate: Option<String>,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("📨 Savitri P2P Message Routing Example");
    println!("=====================================");

    // Create network configuration
    let network_config = NetworkConfig {
        port: 8333,
        max_connections: 50,
        connection_timeout: Duration::from_secs(30),
        enable_ipv6: true,
        ..Default::default()
    };

    // Create gossip configuration
    let gossip_config = GossipConfig {
        mesh_n: 8,
        mesh_n_low: 5,
        mesh_n_high: 12,
        heartbeat_interval: Duration::from_millis(1000),
        history_length: 5,
        history_gossip: 3,
        duplicate_cache_time: Duration::from_secs(10),
        validation_mode: savitri_p2p::gossip::ValidationMode::Strict,
        max_transmit_size: 10485760, // 10MB
    };

    // Create P2P configuration
    let mut config = P2PConfig::default();
    config.network = network_config;
    config.gossip = gossip_config;

    // Create and start P2P manager
    println!("🚀 Starting P2P node...");
    let mut manager = P2PManager::new(config)?;
    manager.start().await?;

    println!("✅ P2P node started successfully!");
    println!("📡 Local peer ID: {}", manager.get_local_peer_id());

    // Subscribe to various topics
    println!("\n📬 Subscribing to topics...");
    
    let topics = vec![
        "/savitri/tx/1",
        "/savitri/block/1", 
        "/savitri/consensus/proposal/1",
        "/savitri/consensus/vote/1",
        "/savitri/consensus/cert/1",
        "/savitri/monolith/announce/1",
    ];

    for topic in &topics {
        manager.subscribe(topic).await?;
        println!("   ✅ Subscribed to {}", topic);
    }

    // Set up message handlers
    println!("\n🔧 Setting up message handlers...");
    
    // Transaction handler
    let tx_manager = manager.clone();
    tokio::spawn(async move {
        handle_transaction_messages(tx_manager).await;
    });

    // Block handler
    let block_manager = manager.clone();
    tokio::spawn(async move {
        handle_block_messages(block_manager).await;
    });

    // Consensus handler
    let consensus_manager = manager.clone();
    tokio::spawn(async move {
        handle_consensus_messages(consensus_manager).await;
    });

    // Wait for connections
    println!("\n⏳ Waiting for peer connections...");
    sleep(Duration::from_secs(10)).await;

    // Start message publishing demo
    println!("\n📤 Starting message publishing demo...");
    
    // Publish transactions
    publish_transaction_messages(&manager).await?;
    
    // Publish blocks
    publish_block_messages(&manager).await?;
    
    // Publish consensus messages
    publish_consensus_messages(&manager).await?;

    // Monitor message routing
    println!("\n📊 Monitoring message routing for 60 seconds...");
    monitor_message_routing(&manager, Duration::from_secs(60)).await;

    // Demonstrate direct messaging
    println!("\n🎯 Demonstrating direct messaging...");
    demonstrate_direct_messaging(&manager).await?;

    // Cleanup and shutdown
    println!("\n🛑 Shutting down P2P node...");
    manager.stop().await?;
    println!("✅ P2P node stopped successfully!");

    Ok(())
}

/// Handle transaction messages
async fn handle_transaction_messages(mut manager: P2PManager) {
    println!("   🔧 Transaction message handler started");
    
    let mut message_count = 0;
    let mut total_amount = 0u64;
    
    loop {
        // In a real implementation, this would receive messages from the gossip layer
        // For this example, we'll simulate receiving messages
        sleep(Duration::from_millis(500)).await;
        
        // Simulate receiving a transaction
        if message_count < 100 {
            let tx = create_sample_transaction(message_count);
            message_count += 1;
            total_amount += tx.amount;
            
            if message_count % 10 == 0 {
                println!("   📋 Processed {} transactions, total amount: {} SAV", 
                         message_count, total_amount);
            }
        }
    }
}

/// Handle block messages
async fn handle_block_messages(mut manager: P2PManager) {
    println!("   🔧 Block message handler started");
    
    let mut block_count = 0;
    let mut last_height = 0u64;
    
    loop {
        sleep(Duration::from_millis(1000)).await;
        
        // Simulate receiving a block
        if block_count < 10 {
            let block = create_sample_block(last_height + 1);
            block_count += 1;
            last_height = block.height;
            
            println!("   🧱 Received block #{} with {} transactions", 
                     block.height, block.transactions.len());
        }
    }
}

/// Handle consensus messages
async fn handle_consensus_messages(mut manager: P2PManager) {
    println!("   🔧 Consensus message handler started");
    
    let mut consensus_round = 0u64;
    
    loop {
        sleep(Duration::from_millis(2000)).await;
        
        // Simulate consensus rounds
        consensus_round += 1;
        
        if consensus_round % 5 == 0 {
            println!("   ⚙  Consensus round #{} completed", consensus_round);
        }
    }
}

/// Publish transaction messages
async fn publish_transaction_messages(manager: &P2PManager) -> Result<()> {
    println!("   📤 Publishing transaction messages...");
    
    for i in 0..20 {
        let tx = create_sample_transaction(i);
        let tx_data = serde_json::to_vec(&tx)?;
        
        manager.broadcast_message("/savitri/tx/1", tx_data).await?;
        
        if i % 5 == 0 {
            println!("   📤 Published {} transactions", i + 1);
        }
        
        sleep(Duration::from_millis(200)).await;
    }
    
    println!("   ✅ Published 20 transactions");
    Ok(())
}

/// Publish block messages
async fn publish_block_messages(manager: &P2PManager) -> Result<()> {
    println!("   📤 Publishing block messages...");
    
    for height in 1..=5 {
        let block = create_sample_block(height);
        let block_data = serde_json::to_vec(&block)?;
        
        manager.broadcast_message("/savitri/block/1", block_data).await?;
        
        println!("   🧱 Published block #{} with {} transactions", 
                 height, block.transactions.len());
        
        sleep(Duration::from_millis(1000)).await;
    }
    
    println!("   ✅ Published 5 blocks");
    Ok(())
}

/// Publish consensus messages
async fn publish_consensus_messages(manager: &P2PManager) -> Result<()> {
    println!("   📤 Publishing consensus messages...");
    
    for round in 1..=3 {
        let consensus = create_sample_consensus(round);
        let consensus_data = serde_json::to_vec(&consensus)?;
        
        // Publish to different consensus topics
        if let Some(ref proposal) = consensus.proposal {
            let proposal_data = serde_json::to_vec(proposal)?;
            manager.broadcast_message("/savitri/consensus/proposal/1", proposal_data).await?;
        }
        
        for vote in &consensus.votes {
            manager.broadcast_message("/savitri/consensus/vote/1", vote.as_bytes().to_vec()).await?;
        }
        
        if let Some(ref cert) = consensus.certificate {
            manager.broadcast_message("/savitri/consensus/cert/1", cert.as_bytes().to_vec()).await?;
        }
        
        println!("   ⚙  Published consensus round #{}", round);
        sleep(Duration::from_millis(1500)).await;
    }
    
    println!("   ✅ Published 3 consensus rounds");
    Ok(())
}

/// Monitor message routing statistics
async fn monitor_message_routing(manager: &P2PManager, duration: Duration) {
    let mut interval = interval(Duration::from_secs(5));
    let start_time = SystemTime::now();
    
    loop {
        interval.tick().await;
        
        let elapsed = SystemTime::now().duration_since(start_time).unwrap();
        if elapsed >= duration {
            break;
        }
        
        let gossip_stats = manager.get_gossip_stats();
        let network_stats = manager.get_network_stats();
        
        println!("   📊 Routing Stats [{}s]:", elapsed.as_secs());
        println!("     Messages sent: {}", gossip_stats.messages_sent);
        println!("     Messages received: {}", gossip_stats.messages_received);
        println!("     Duplicate rate: {:.2}%", gossip_stats.duplicate_rate * 100.0);
        println!("     Mesh size: {}", gossip_stats.mesh_size);
        println!("     Active connections: {}", network_stats.active_connections);
        println!("     Bytes sent: {}", network_stats.bytes_sent);
        println!("     Bytes received: {}", network_stats.bytes_received);
    }
}

/// Demonstrate direct messaging between peers
async fn demonstrate_direct_messaging(manager: &P2PManager) -> Result<()> {
    println!("   🎯 Setting up direct messaging demo...");
    
    // Get connected peers
    let peers = manager.get_connected_peers();
    
    if peers.is_empty() {
        println!("   ⚠️ No connected peers for direct messaging demo");
        return Ok(());
    }
    
    let target_peer = &peers[0].id;
    println!("   🎯 Sending direct message to peer: {}", target_peer);
    
    // Create a direct message
    let direct_message = format!("Hello from {}! Time: {}", 
                                manager.get_local_peer_id(),
                                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
    
    // Send direct message
    match manager.send_direct_message(target_peer, direct_message.as_bytes().to_vec()).await {
        Ok(()) => {
            println!("   ✅ Direct message sent successfully");
        }
        Err(e) => {
            println!("   ❌ Failed to send direct message: {}", e);
        }
    }
    
    // Demonstrate message routing with priorities
    println!("   🚀 Demonstrating priority-based message routing...");
    
    let high_priority_msg = b"High priority consensus message";
    let normal_priority_msg = b"Normal priority transaction";
    let low_priority_msg = b"Low priority analytics data";
    
    // Send messages with different priorities
    manager.broadcast_with_priority("/savitri/consensus/vote/1", high_priority_msg.to_vec(), 
                                  savitri_p2p::gossip::MessagePriority::High).await?;
    
    manager.broadcast_with_priority("/savitri/tx/1", normal_priority_msg.to_vec(),
                                  savitri_p2p::gossip::MessagePriority::Normal).await?;
    
    manager.broadcast_with_priority("/savitri/analytics/1", low_priority_msg.to_vec(),
                                  savitri_p2p::gossip::MessagePriority::Low).await?;
    
    println!("   ✅ Messages sent with different priorities");
    
    Ok(())
}

/// Create a sample transaction
fn create_sample_transaction(index: u64) -> TransactionMessage {
    TransactionMessage {
        id: format!("tx_{}", index),
        sender: format!("sender_{}", index % 10),
        receiver: format!("receiver_{}", index % 10),
        amount: (index + 1) * 1000,
        fee: 10,
        timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
        signature: vec![0u8; 64], // Mock signature
    }
}

/// Create a sample block
fn create_sample_block(height: u64) -> BlockMessage {
    let mut transactions = Vec::new();
    for i in 0..5 {
        transactions.push(format!("tx_{}_{}", height, i));
    }
    
    BlockMessage {
        height,
        hash: format!("block_hash_{}", height),
        previous_hash: format!("block_hash_{}", height.saturating_sub(1)),
        transactions,
        timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
        proposer: format!("proposer_{}", height % 3),
        signature: vec![0u8; 64], // Mock signature
    }
}

/// Create a sample consensus message
fn create_sample_consensus(round: u64) -> ConsensusMessage {
    let proposal = if round % 2 == 0 {
        Some(create_sample_block(round * 10))
    } else {
        None
    };
    
    let mut votes = Vec::new();
    for i in 0..3 {
        votes.push(format!("vote_{}_{}", round, i));
    }
    
    let certificate = if round == 3 {
        Some(format!("cert_{}", round))
    } else {
        None
    };
    
    ConsensusMessage {
        round,
        proposal,
        votes,
        certificate,
        timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let tx = create_sample_transaction(1);
        assert_eq!(tx.id, "tx_1");
        assert_eq!(tx.amount, 2000);
        assert_eq!(tx.fee, 10);
    }

    #[test]
    fn test_block_creation() {
        let block = create_sample_block(5);
        assert_eq!(block.height, 5);
        assert_eq!(block.previous_hash, "block_hash_4");
        assert_eq!(block.transactions.len(), 5);
    }

    #[test]
    fn test_consensus_creation() {
        let consensus = create_sample_consensus(2);
        assert_eq!(consensus.round, 2);
        assert!(consensus.proposal.is_some());
        assert_eq!(consensus.votes.len(), 3);
    }

    #[tokio::test]
    async fn test_message_serialization() {
        let tx = create_sample_transaction(1);
        let serialized = serde_json::to_vec(&tx).unwrap();
        let deserialized: TransactionMessage = serde_json::from_slice(&serialized).unwrap();
        
        assert_eq!(tx.id, deserialized.id);
        assert_eq!(tx.amount, deserialized.amount);
    }
}
