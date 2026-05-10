//! Savitri P2P Node Discovery Example
//! 
//! This example demonstrates how to use the Savitri P2P discovery system
//! to find and connect to peers in the network.

## 📋 Example Overview

This example showcases:
- Peer discovery with mDNS and DNS bootstrap
- Reputation system monitoring
- Geographic peer distribution analysis
- Manual peer connection management

## 🚀 Quick Start

```bash
# Run the example
cargo run --example node_discovery --release

# Run with debug output
RUST_LOG=debug cargo run --example node_discovery --release
```

## 📊 Expected Output

```
🔍 Savitri P2P Node Discovery Example
========================================
🚀 Starting P2P node...
✅ P2P node started successfully!
📡 Local peer ID: 12D3KooW...
🔍 Starting peer discovery...
📊 Discovery Results:
   Total peers discovered: 15
   1. 12D3KooW... - /dns/ec2-18-196-97-198.eu-central-1.compute.amazonaws.com/tcp/4001 (Score: 85.50)
   2. 12D3KooW... - /dns/ec2-18-196-23-60.eu-central-1.compute.amazonaws.com/tcp/4001 (Score: 92.25)
   ...
📈 Monitoring discovery progress for 60 seconds...
   [10] Discovered: 15, Connected: 12
   ...
🌍 Analyzing geographic peer distribution...
   Geographic Distribution:
     EU Central: 8 peers
     US East: 4 peers
     Local: 3 peers
🛑 Shutting down P2P node...
✅ P2P node stopped successfully!
```

use anyhow::Result;
use savitri_p2p::{P2PManager, P2PConfig, DiscoveryConfig, NetworkConfig};
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🔍 Savitri P2P Node Discovery Example");
    println!("========================================");

    // Create discovery configuration
    let discovery_config = DiscoveryConfig {
        enable_mdns: true,
        enable_dns: true,
        bootstrap_nodes: vec![
            "12D3KooWBx4xa8JPMhMLLVAKyhtUiGaovzreTngTWBtbKNYVLMsE@/dns/ec2-18-196-97-198.eu-central-1.compute.amazonaws.com/tcp/4001".to_string(),
            "12D3KooWRuL61awWrJX4aiU9oqkn8qETG1gHoiniSnynZp93GB53@/dns/ec2-18-196-23-60.eu-central-1.compute.amazonaws.com/tcp/4001".to_string(),
        ],
        discovery_timeout: Duration::from_secs(10),
        max_discovered_peers: 100,
        heartbeat_interval: Duration::from_secs(30),
        ..Default::default()
    };

    // Create network configuration
    let network_config = NetworkConfig {
        port: 8333,
        max_connections: 50,
        connection_timeout: Duration::from_secs(30),
        enable_ipv6: true,
        ..Default::default()
    };

    // Create P2P configuration
    let mut config = P2PConfig::default();
    config.discovery = discovery_config;
    config.network = network_config;

    // Create and start P2P manager
    println!("🚀 Starting P2P node...");
    let mut manager = P2PManager::new(config)?;
    manager.start().await?;

    println!("✅ P2P node started successfully!");
    println!("📡 Local peer ID: {}", manager.get_local_peer_id());

    // Start discovery process
    println!("\n🔍 Starting peer discovery...");
    let discovered_peers = manager.discover_peers().await?;
    
    println!("📊 Discovery Results:");
    println!("   Total peers discovered: {}", discovered_peers.len());
    
    for (i, peer) in discovered_peers.iter().enumerate() {
        println!("   {}. {} - {} (Score: {:.2})", 
                 i + 1, 
                 peer.id, 
                 peer.addresses.first().unwrap_or(&"/ip4/0.0.0.0/tcp/0".to_string()),
                 peer.reputation_score);
    }

    // Monitor discovery progress
    println!("\n📈 Monitoring discovery progress for 60 seconds...");
    let mut discovery_stats = manager.get_discovery_stats();
    
    for i in 0..60 {
        sleep(Duration::from_secs(1)).await;
        
        let current_stats = manager.get_discovery_stats();
        
        if current_stats.total_discovered != discovery_stats.total_discovered ||
           current_stats.connected_peers != discovery_stats.connected_peers {
            println!("   [{}] Discovered: {}, Connected: {}", 
                     i + 1, 
                     current_stats.total_discovered,
                     current_stats.connected_peers);
            discovery_stats = current_stats;
        }
        
        // Print detailed stats every 10 seconds
        if (i + 1) % 10 == 0 {
            print_detailed_stats(&manager).await;
        }
    }

    // Demonstrate manual peer connection
    println!("\n🔗 Demonstrating manual peer connection...");
    
    if let Some(peer) = discovered_peers.first() {
        println!("   Attempting to connect to {}...", peer.id);
        
        match timeout(Duration::from_secs(10), manager.connect_to_peer(&peer.id)).await {
            Ok(Ok(())) => {
                println!("   ✅ Successfully connected to {}", peer.id);
            }
            Ok(Err(e)) => {
                println!("   ❌ Failed to connect to {}: {}", peer.id, e);
            }
            Err(_) => {
                println!("   ⏰ Connection attempt timed out");
            }
        }
    }

    // Demonstrate peer reputation monitoring
    println!("\n⭐ Monitoring peer reputation for 30 seconds...");
    
    for i in 0..30 {
        sleep(Duration::from_secs(1)).await;
        
        let peers = manager.get_connected_peers();
        if let Some((best_peer, score)) = peers.iter()
            .map(|p| (p.id, p.reputation_score))
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap()) {
            if i % 5 == 0 {
                println!("   [{}] Best peer: {} (Score: {:.2})", i + 1, best_peer, score);
            }
        }
    }

    // Demonstrate geographic peer distribution
    println!("\n🌍 Analyzing geographic peer distribution...");
    
    let peers = manager.get_connected_peers();
    let mut regions = std::collections::HashMap::new();
    
    for peer in &peers {
        let region = extract_region_from_addresses(&peer.addresses);
        *regions.entry(region).or_insert(0) += 1;
    }
    
    println!("   Geographic Distribution:");
    for (region, count) in regions {
        println!("     {}: {} peers", region, count);
    }

    // Cleanup and shutdown
    println!("\n🛑 Shutting down P2P node...");
    manager.stop().await?;
    println!("✅ P2P node stopped successfully!");

    Ok(())
}

/// Print detailed discovery statistics
async fn print_detailed_stats(manager: &P2PManager) {
    let stats = manager.get_discovery_stats();
    let network_stats = manager.get_network_stats();
    
    println!("   📊 Detailed Statistics:");
    println!("     Discovery:");
    println!("       Total discovered: {}", stats.total_discovered);
    println!("       Connected: {}", stats.connected_peers);
    println!("       Failed connections: {}", stats.failed_connections);
    println!("       Average latency: {:.2}ms", stats.average_latency);
    println!("     Network:");
    println!("       Active connections: {}", network_stats.active_connections);
    println!("       Pending connections: {}", network_stats.pending_connections);
    println!("       Total bytes sent: {}", network_stats.bytes_sent);
    println!("       Total bytes received: {}", network_stats.bytes_received);
}

/// Extract geographic region from peer addresses
fn extract_region_from_addresses(addresses: &[String]) -> String {
    for address in addresses {
        if address.contains("eu-central") {
            return "EU Central".to_string();
        } else if address.contains("us-east") {
            return "US East".to_string();
        } else if address.contains("us-west") {
            return "US West".to_string();
        } else if address.contains("asia") {
            return "Asia".to_string();
        } else if address.starts_with("/ip4/192.168.") || address.starts_with("/ip4/10.") {
            return "Local".to_string();
        }
    }
    "Unknown".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discovery_configuration() {
        let config = DiscoveryConfig {
            enable_mdns: true,
            enable_dns: true,
            bootstrap_nodes: vec![],
            discovery_timeout: Duration::from_secs(5),
            max_discovered_peers: 10,
            ..Default::default()
        };

        assert!(config.enable_mdns);
        assert!(config.enable_dns);
        assert_eq!(config.max_discovered_peers, 10);
    }

    #[tokio::test]
    async fn test_region_extraction() {
        let eu_addresses = vec![
            "/dns/ec2-18-196-97-198.eu-central-1.compute.amazonaws.com/tcp/4001".to_string()
        ];
        assert_eq!(extract_region_from_addresses(&eu_addresses), "EU Central");

        let local_addresses = vec![
            "/ip4/192.168.1.100/tcp/8333".to_string()
        ];
        assert_eq!(extract_region_from_addresses(&local_addresses), "Local");

        let unknown_addresses = vec![
            "/ip4/203.0.113.1/tcp/8333".to_string()
        ];
        assert_eq!(extract_region_from_addresses(&unknown_addresses), "Unknown");
    }
}
