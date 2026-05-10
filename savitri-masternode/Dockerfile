# Dockerfile for Savitri Masternode

# Build stage
FROM rust:1.75-slim AS builder

WORKDIR /build

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace dependencies
COPY savitri-core/Cargo.toml savitri-core/
COPY savitri-storage/Cargo.toml savitri-storage/
COPY savitri-consensus/Cargo.toml savitri-consensus/
COPY savitri-p2p/Cargo.toml savitri-p2p/
COPY savitri-zkp/Cargo.toml savitri-zkp/
COPY savitri-contracts/Cargo.toml savitri-contracts/

# Copy build files
COPY Cargo.toml Cargo.lock ./
COPY savitri-masternode/Cargo.toml savitri-masternode/

# Build dependencies (cache layer)
RUN mkdir -p savitri-masternode/src && \
    echo "fn main() {}" > savitri-masternode/src/main.rs && \
    cargo build --release --bin savitri-masternode && \
    rm -rf savitri-masternode/src

# Copy source code
COPY savitri-masternode/src savitri-masternode/src/
COPY savitri-core/src savitri-core/src/
COPY savitri-storage/src savitri-storage/src/
COPY savitri-consensus/src savitri-consensus/src/
COPY savitri-p2p/src savitri-p2p/src/
COPY savitri-zkp/src savitri-zkp/src/
COPY savitri-contracts/src savitri-contracts/src/

# Build application
RUN cargo build --release --bin savitri-masternode

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash savitri

# Copy binary
COPY --from=builder /build/target/release/savitri-masternode /usr/local/bin/savitri-masternode

# Copy config
COPY savitri-masternode/config /app/config

# Create data directory and set ownership
RUN mkdir -p /app/data && chown -R savitri:savitri /app

# Switch to non-root user
USER savitri

# Expose ports
EXPOSE 4021 9090

# Set environment variables
ENV RUST_LOG=info
ENV METRICS_PORT=9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/metrics || exit 1

# Run masternode
CMD ["savitri-masternode", "config/masternode.toml"]
