//! Group Routing Table
//!
//! Implements efficient routing table for group-based message routing
//! with path optimization and load balancing.

use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Routing configuration
#[derive(Debug, Clone)]
pub struct RoutingConfig {
    /// Enable route caching
    pub enable_route_caching: bool,
    /// Maximum routes per destination
    pub max_routes_per_destination: usize,
    /// Route timeout in seconds
    pub route_timeout_secs: u64,
    /// Enable load balancing
    pub enable_load_balancing: bool,
    /// Enable path optimization
    pub enable_path_optimization: bool,
    /// Maximum path length
    pub max_path_length: usize,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            enable_route_caching: true,
            max_routes_per_destination: 5,
            route_timeout_secs: 300,
            enable_load_balancing: true,
            enable_path_optimization: true,
            max_path_length: 10,
        }
    }
}

/// Group route information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupRoute {
    pub route_id: String,
    pub destination: PeerId,
    pub path: Vec<PeerId>,
    pub hop_count: usize,
    pub latency_ms: u64,
    pub reliability: f64,
    pub last_used: u64,
    pub route_type: RouteType,
}

/// Route types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RouteType {
    /// Direct connection
    Direct,
    /// Through group members
    GroupRelay,
    /// Multi-hop relay
    MultiHop,
    /// Optimized path
    Optimized,
}

/// Routing metrics
#[derive(Debug, Clone, Default)]
pub struct RoutingMetrics {
    pub total_routes: usize,
    pub active_routes: usize,
    pub average_hop_count: f64,
    pub average_latency_ms: f64,
    pub routes_by_type: HashMap<RouteType, usize>,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// Group Routing Table
pub struct GroupRoutingTable {
    config: RoutingConfig,
    local_peer_id: PeerId,
    routes: Arc<RwLock<HashMap<String, GroupRoute>>>,
    destination_routes: Arc<RwLock<HashMap<PeerId, Vec<String>>>>,
    metrics: Arc<RwLock<RoutingMetrics>>,
}

impl GroupRoutingTable {
    pub fn new(config: RoutingConfig, local_peer_id: PeerId) -> Self {
        Self {
            config,
            local_peer_id,
            routes: Arc::new(RwLock::new(HashMap::new())),
            destination_routes: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(RoutingMetrics::default())),
        }
    }

    /// Add route to routing table
    pub async fn add_route(&self, route: GroupRoute) -> Result<()> {
        let route_id = route.route_id.clone();
        let destination = route.destination;

        // Validate route
        if self.validate_route(&route).await? {
            // Add to routes table
            let mut routes = self.routes.write().await;
            routes.insert(route_id.clone(), route.clone());

            // Add to destination mapping
            let mut dest_routes = self.destination_routes.write().await;
            dest_routes
                .entry(destination)
                .or_insert_with(Vec::new)
                .push(route_id.clone());

            // Limit routes per destination
            if let Some(route_ids) = dest_routes.get_mut(&destination) {
                if route_ids.len() > self.config.max_routes_per_destination {
                    if let Some(oldest_route_id) = route_ids.first() {
                        routes.remove(oldest_route_id);
                        route_ids.remove(0);
                    }
                }
            }

            // Update metrics
            self.update_metrics().await;

            info!(
                route_id = %route_id,
                destination = %destination,
                hop_count = route.hop_count,
                "Added route to routing table"
            );
        } else {
            warn!(route_id = %route_id, "Invalid route, not adding");
        }

        Ok(())
    }

    /// Validate route
    async fn validate_route(&self, route: &GroupRoute) -> Result<bool> {
        // Check if path is valid
        if route.path.is_empty() {
            return Ok(false);
        }

        // Check if path length is within limits
        if route.path.len() > self.config.max_path_length {
            return Ok(false);
        }

        // Check if destination is reachable
        if route.path.last() != Some(&route.destination) {
            return Ok(false);
        }

        // Check for loops
        let mut seen_peers = std::collections::HashSet::new();
        for peer in &route.path {
            if !seen_peers.insert(peer) {
                return Ok(false); // Loop detected
            }
        }

        Ok(true)
    }

    /// Get best route to destination
    pub async fn get_best_route(&self, destination: PeerId) -> Option<GroupRoute> {
        let dest_routes = self.destination_routes.read().await;
        let routes = self.routes.read().await;

        if let Some(route_ids) = dest_routes.get(&destination) {
            let mut best_route = None;
            let mut best_score = 0.0;

            for route_id in route_ids {
                if let Some(route) = routes.get(route_id) {
                    let score = self.calculate_route_score(route);
                    if score > best_score {
                        best_score = score;
                        best_route = Some(route.clone());
                    }
                }
            }

            if best_route.is_some() {
                let mut metrics = self.metrics.write().await;
                metrics.cache_hits += 1;
            } else {
                let mut metrics = self.metrics.write().await;
                metrics.cache_misses += 1;
            }

            best_route
        } else {
            let mut metrics = self.metrics.write().await;
            metrics.cache_misses += 1;
            None
        }
    }

    /// Calculate route score for selection
    fn calculate_route_score(&self, route: &GroupRoute) -> f64 {
        let latency_score = if route.latency_ms == 0 {
            1.0
        } else {
            1.0 / (1.0 + route.latency_ms as f64 / 1000.0)
        };

        let reliability_score = route.reliability;
        let hop_score = 1.0 / (1.0 + route.hop_count as f64);

        // Weighted score
        (latency_score * 0.4) + (reliability_score * 0.4) + (hop_score * 0.2)
    }

    /// Create direct route
    pub async fn create_direct_route(&self, destination: PeerId, latency_ms: u64) -> GroupRoute {
        let route_id = format!("direct_{}_{}", self.local_peer_id, destination);

        GroupRoute {
            route_id: route_id.clone(),
            destination,
            path: vec![destination],
            hop_count: 1,
            latency_ms,
            reliability: 0.9,
            last_used: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            route_type: RouteType::Direct,
        }
    }

    /// Create group relay route
    pub async fn create_group_relay_route(
        &self,
        destination: PeerId,
        relay_peers: Vec<PeerId>,
        latency_ms: u64,
    ) -> GroupRoute {
        let route_id = format!("group_relay_{}_{}", self.local_peer_id, destination);
        let mut path = relay_peers;
        path.push(destination);

        GroupRoute {
            route_id: route_id.clone(),
            destination,
            path: path.clone(),
            hop_count: path.len(),
            latency_ms,
            reliability: 0.8,
            last_used: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            route_type: RouteType::GroupRelay,
        }
    }

    /// Update route statistics
    pub async fn update_route_stats(
        &self,
        route_id: &str,
        latency_ms: u64,
        success: bool,
    ) -> Result<()> {
        let mut routes = self.routes.write().await;
        if let Some(route) = routes.get_mut(route_id) {
            route.last_used = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // Update latency (exponential moving average)
            if route.latency_ms == 0 {
                route.latency_ms = latency_ms;
            } else {
                route.latency_ms = (route.latency_ms * 3 + latency_ms) / 4;
            }

            // Update reliability
            if success {
                route.reliability = (route.reliability * 0.9) + 0.1;
            } else {
                route.reliability *= 0.8;
            }

            debug!(
                route_id = %route_id,
                latency_ms = latency_ms,
                success = success,
                new_reliability = route.reliability,
                "Updated route statistics"
            );
        }

        Ok(())
    }

    /// Remove expired routes
    pub async fn cleanup_expired_routes(&self) -> Result<usize> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut removed_count = 0;

        // Remove expired routes
        {
            let mut routes = self.routes.write().await;
            let mut dest_routes = self.destination_routes.write().await;

            routes.retain(|route_id, route| {
                let is_valid = (current_time - route.last_used) < self.config.route_timeout_secs;
                if !is_valid {
                    // Remove from destination mapping
                    if let Some(route_ids) = dest_routes.get_mut(&route.destination) {
                        route_ids.retain(|id| id != route_id);
                        if route_ids.is_empty() {
                            dest_routes.remove(&route.destination);
                        }
                    }
                    removed_count += 1;
                }
                is_valid
            });
        }

        // Update metrics
        self.update_metrics().await;

        if removed_count > 0 {
            info!(removed_count = removed_count, "Cleaned up expired routes");
        }

        Ok(removed_count)
    }

    /// Update routing metrics
    async fn update_metrics(&self) {
        let routes = self.routes.read().await;
        let mut metrics = self.metrics.write().await;

        metrics.total_routes = routes.len();
        metrics.active_routes = routes
            .values()
            .filter(|r| {
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                (current_time - r.last_used) < self.config.route_timeout_secs
            })
            .count();

        if !routes.is_empty() {
            let total_hop_count: usize = routes.values().map(|r| r.hop_count).sum();
            metrics.average_hop_count = total_hop_count as f64 / routes.len() as f64;

            let total_latency: u64 = routes.values().map(|r| r.latency_ms).sum();
            metrics.average_latency_ms = total_latency as f64 / routes.len() as f64;
        }

        // Count routes by type
        metrics.routes_by_type.clear();
        for route in routes.values() {
            *metrics
                .routes_by_type
                .entry(route.route_type.clone())
                .or_insert(0) += 1;
        }
    }

    /// Get all routes
    pub async fn get_all_routes(&self) -> Vec<GroupRoute> {
        let routes = self.routes.read().await;
        routes.values().cloned().collect()
    }

    /// Get routes by type
    pub async fn get_routes_by_type(&self, route_type: RouteType) -> Vec<GroupRoute> {
        let routes = self.routes.read().await;
        routes
            .values()
            .filter(|r| r.route_type == route_type)
            .cloned()
            .collect()
    }

    /// Get routing metrics
    pub async fn get_metrics(&self) -> RoutingMetrics {
        self.update_metrics().await;
        let metrics = self.metrics.read().await;
        metrics.clone()
    }

    /// Start background tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting group routing table");

        // Start cleanup task
        let routing_table = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;
                if let Err(e) = routing_table.cleanup_expired_routes().await {
                    error!("Failed to cleanup expired routes: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Stop the routing table
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping group routing table");
        Ok(())
    }
}

impl Clone for GroupRoutingTable {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_peer_id: self.local_peer_id,
            routes: self.routes.clone(),
            destination_routes: self.destination_routes.clone(),
            metrics: self.metrics.clone(),
        }
    }
}
