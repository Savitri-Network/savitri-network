#!/usr/bin/env bash
# Savitri Network — Codespaces / Dev Container post-create setup.
#
# Installs the C/C++ toolchain pieces that RocksDB bindgen and a few
# native dependencies need on Debian Bookworm, then warms the cargo
# registry so the first `cargo build` doesn't pay the full network
# round-trip for every crate in the workspace.
#
# Idempotent: safe to re-run.

set -euo pipefail

log() { echo -e "\033[1;34m[savitri-setup]\033[0m $*"; }
warn() { echo -e "\033[1;33m[savitri-setup]\033[0m $*"; }

log "Updating apt index"
sudo apt-get update -qq

log "Installing system dependencies (RocksDB bindgen, native build tools)"
sudo apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    libclang-dev \
    clang \
    llvm-dev \
    protobuf-compiler \
    jq \
    curl

log "Verifying Rust toolchain (rust-toolchain.toml will be honoured by rustup)"
rustup show active-toolchain || true
rustup component add rustfmt clippy rust-src 2>/dev/null || true

log "Versions"
rustc --version
cargo --version
clang --version | head -n1
protoc --version
docker --version || warn "docker not yet available in this shell — Docker-in-Docker feature provides it after container fully starts"

log "Pre-fetching cargo dependencies (this may take a few minutes the first time)"
if cargo fetch --locked 2>/dev/null; then
    log "cargo fetch --locked succeeded"
else
    warn "cargo fetch --locked failed (Cargo.lock may be out of sync); falling back to cargo fetch"
    cargo fetch
fi

log "Setup complete."
cat <<'EOF'

  Quick start
  -----------
    # Build the masternode binary (release, takes ~5-10 min cold):
    cargo build --release -p savitri-masternode

    # Build the lightnode without RocksDB (fast, in-memory only):
    cargo build --release -p savitri-lightnode --no-default-features --features desktop

    # Run the workspace tests:
    cargo test --workspace

    # Spin up a 2 MN + 2 LN testnet with Prometheus + Grafana:
    cd docker && docker compose up -d

  Forwarded ports
  ---------------
    JSON-RPC      : 8545-8549
    Lightnode P2P : 5001-5010
    Masternode P2P: 5021-5025
    Prometheus    : 9090
    Grafana       : 3000   (login admin / admin)

  Tip: rust-analyzer's first index pass on this workspace is heavy
  (13 crates). Let it finish before kicking off a large cargo build,
  or you'll pay the cost twice.

EOF
