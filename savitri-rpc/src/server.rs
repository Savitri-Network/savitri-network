//! RPC HTTP server with Axum
//!
//! JSON-RPC 2.0 only. All requests go through POST / or POST /rpc.
//! No REST endpoints.
//!
//! SECURITY (PT-L06): This server does NOT terminate TLS.
//! For production deployments, place a TLS-terminating reverse proxy
//! (e.g., nginx, Caddy, or HAProxy) in front of the RPC endpoint.

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use dashmap::DashMap;
use http::{Method, Request};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::jsonrpc::handle_jsonrpc;
use crate::RpcState;

/// Global rate limiter state.
/// Tracks total request count with a sliding window.
struct GlobalRateLimiter {
    /// (request_count, window_start)
    state: std::sync::Mutex<(u32, Instant)>,
    /// Max requests per window
    max_requests: u32,
    /// Window duration
    window: std::time::Duration,
}

impl GlobalRateLimiter {
    fn new(max_requests: u32, window: std::time::Duration) -> Self {
        Self {
            state: std::sync::Mutex::new((0, Instant::now())),
            max_requests,
            window,
        }
    }

    /// Check if a request is allowed globally. Returns true if allowed.
    fn check(&self) -> bool {
        let now = Instant::now();
        // LOW-03: recover from mutex poisoning — if another thread panicked while
        // holding this lock the rate-limiter state may be inconsistent, but allowing
        // the request (and resetting the window) is safer than crashing the server.
        let mut guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let (count, window_start) = &mut *guard;

        if now.duration_since(*window_start) >= self.window {
            *count = 0;
            *window_start = now;
        }

        if *count >= self.max_requests {
            return false;
        }
        *count += 1;
        true
    }
}

/// Axum middleware for global rate limiting.
async fn global_rate_limit(
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    let limiter = req.extensions().get::<Arc<GlobalRateLimiter>>().cloned();

    if let Some(limiter) = limiter {
        if !limiter.check() {
            tracing::warn!("Global rate limit exceeded");
            return Err(
                (StatusCode::TOO_MANY_REQUESTS, "Global rate limit exceeded").into_response(),
            );
        }
    }

    Ok(next.run(req).await)
}

/// Per-IP rate limiter state.
/// Tracks request counts per IP with a sliding window.
struct PerIpRateLimiter {
    /// Map from IP → (request_count, window_start)
    requests: DashMap<IpAddr, (u32, Instant)>,
    /// Max requests per IP per window
    max_per_ip: u32,
    /// Window duration
    window: std::time::Duration,
}

impl PerIpRateLimiter {
    fn new(max_per_ip: u32, window: std::time::Duration) -> Self {
        Self {
            requests: DashMap::new(),
            max_per_ip,
            window,
        }
    }

    /// Check if request from this IP is allowed. Returns true if allowed.
    fn check(&self, ip: IpAddr) -> bool {
        let now = Instant::now();
        let mut entry = self.requests.entry(ip).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // Reset window if expired
        if now.duration_since(*window_start) >= self.window {
            *count = 0;
            *window_start = now;
        }

        if *count >= self.max_per_ip {
            return false;
        }
        *count += 1;
        true
    }
}

/// Axum middleware for per-IP rate limiting.
async fn per_ip_rate_limit(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, Response> {
    let limiter = req.extensions().get::<Arc<PerIpRateLimiter>>().cloned();

    if let Some(limiter) = limiter {
        if !limiter.check(addr.ip()) {
            tracing::warn!(ip = %addr.ip(), "Per-IP rate limit exceeded");
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Rate limit exceeded for your IP",
            )
                .into_response());
        }
    }

    Ok(next.run(req).await)
}

/// Log the RPC state configuration at startup.
fn log_rpc_state(state: &RpcState) {
    let has_storage = state.storage.is_some();
    let has_mempool = state.mempool.is_some();
    let has_pou_reader = state.pou_reader.is_some();
    let has_masternode_pou = state.masternode_pou_reader.is_some();

    let mode = if has_masternode_pou {
        "masternode"
    } else if has_storage && has_mempool {
        "lightnode"
    } else {
        "unknown"
    };

    tracing::info!(
        mode = mode,
        storage = has_storage,
        mempool = has_mempool,
        pou_reader = has_pou_reader,
        masternode_pou = has_masternode_pou,
        "RPC state configured (JSON-RPC 2.0 only)"
    );
}

fn start_mempool_stats_sampler(state: Arc<RpcState>) {
    if state.mempool.is_none() {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            let Some(mempool) = state.mempool.as_ref() else {
                continue;
            };

            let snapshot = mempool.stats_snapshot();

            let mut tracker = state.mempool_stats_tracker.lock().await;
            tracker.observe(&snapshot);
        }
    });
}

pub async fn run_server(state: RpcState, addr: SocketAddr) -> anyhow::Result<()> {
    tracing::info!(addr = %addr, "Savitri-RPC: starting JSON-RPC 2.0 server...");

    let state = Arc::new(state);
    log_rpc_state(state.as_ref());
    start_mempool_stats_sampler(state.clone());

    // SECURITY: Restrict CORS to localhost origins only.
    // For production deployments behind a reverse proxy, configure allowed
    // origins via the RPC config or environment variable.
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(|origin, _| {
            let origin_str = origin.as_bytes();
            // Allow localhost origins (any port) and 127.0.0.1
            origin_str.starts_with(b"http://localhost")
                || origin_str.starts_with(b"http://127.0.0.1")
                || origin_str.starts_with(b"https://localhost")
                || origin_str.starts_with(b"https://127.0.0.1")
        }))
        .allow_methods([Method::POST])
        .allow_headers([http::header::CONTENT_TYPE]);

    let trace_layer = TraceLayer::new_for_http();

    // SECURITY (PT-M06): Global rate limit to 200 requests/second
    let global_limiter = Arc::new(GlobalRateLimiter::new(
        200,
        std::time::Duration::from_secs(1),
    ));

    // SECURITY: Per-IP rate limit to 50 requests/second
    let per_ip_limiter = Arc::new(PerIpRateLimiter::new(50, std::time::Duration::from_secs(1)));

    let app = Router::new()
        // JSON-RPC 2.0 endpoints (single POST endpoint)
        .route("/", post(handle_jsonrpc))
        .route("/rpc", post(handle_jsonrpc))
        .layer(trace_layer)
        .layer(cors)
        // SECURITY: Limit request body to 1MB to prevent memory exhaustion DoS
        .layer(DefaultBodyLimit::max(1024 * 1024))
        // SECURITY (PT-M06): Global rate limiting
        .layer(axum::Extension(global_limiter))
        .layer(middleware::from_fn(global_rate_limit))
        // SECURITY: Per-IP rate limiting (50 req/s per IP)
        .layer(axum::Extension(per_ip_limiter))
        .layer(middleware::from_fn(per_ip_rate_limit))
        .with_state(state);

    tracing::info!(addr = %addr, "Savitri-RPC: binding TCP...");

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        tracing::error!(addr = %addr, error = %e, "Savitri-RPC: failed to bind to port");
        e
    })?;

    let local_addr = listener.local_addr().map_err(|e| {
        tracing::error!(error = %e, "Savitri-RPC: failed to get local address");
        e
    })?;

    tracing::info!(
        addr = %local_addr,
        "Savitri-RPC: JSON-RPC 2.0 server listening - ready to accept requests"
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "Savitri-RPC: server stopped unexpectedly");
        e
    })?;

    tracing::warn!("Savitri-RPC: server terminated (normal shutdown)");
    Ok(())
}
