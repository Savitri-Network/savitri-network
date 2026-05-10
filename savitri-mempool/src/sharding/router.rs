//! Shard routing logic

#[derive(Debug, Clone)]
pub struct ShardRouter {
    num_shards: usize,
}

#[derive(Debug, Clone)]
pub struct RoutingResult {
    pub shard_id: usize,
    pub assignment: ShardAssignment,
    pub prevalidated: crate::mempool::types::PrevalidatedTx,
    pub tx_hash: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ShardAssignment {
    pub is_cross_shard: bool,
    pub target_shard: usize,
}

impl ShardAssignment {
    pub fn is_cross_shard(&self) -> bool {
        self.is_cross_shard
    }
    
    pub fn target_shard(&self) -> usize {
        self.target_shard
    }
}

impl ShardRouter {
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }
    
    pub fn new_with_config(config: &ShardingConfig) -> Self {
        Self {
            num_shards: config.num_shards,
        }
    }
    
    pub fn route_to_shard(&self, address: &[u8]) -> usize {
        // guarantee parity with `ShardFilter::is_local` and the lightnode
        // tx_router's `shard_for_sender`. All three previously inlined the
        // same DefaultHasher recipe; this single source of truth removes
        // the silent-drift risk (TX routed to A, filtered out in B → 0 tx
        // in blocks).
        savitri_core::sharding::shard_for_sender(address, self.num_shards as u32) as usize
    }
    
    pub fn route_prevalidated(&self, tx: &crate::mempool::types::PrevalidatedTx) -> RoutingResult {
        // Simple routing based on sender address
        let shard_id = self.route_to_shard(&tx.sender_address);
        let is_cross_shard = shard_id != 0; // Simple logic - shard 0 is "home", others are cross-shard
        
        // Generate simple tx hash for routing
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        tx.sender_address.hash(&mut hasher);
        hasher.write_u64(tx.nonce);
        let tx_hash = hasher.finish().to_le_bytes().to_vec();
        
        RoutingResult {
            shard_id,
            assignment: ShardAssignment { 
                is_cross_shard,
                target_shard: shard_id,
            },
            prevalidated: tx.clone(),
            tx_hash,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShardingConfig {
    pub num_shards: usize,
    pub replication_factor: usize,
}

impl ShardingConfig {
    pub fn new(num_shards: usize) -> Self {
        Self {
            num_shards,
            replication_factor: 1,
        }
    }
    
    pub fn with_num_shards(num_shards: usize, replication_factor: usize) -> Self {
        Self {
            num_shards,
            replication_factor,
        }
    }
}
