# Savitri Network — Dev Container

This directory configures a ready-to-use development environment for the
Savitri Network workspace. You can use it locally with the VS Code
**Dev Containers** extension, or in the cloud with **GitHub Codespaces**.

## Why use this

Savitri builds against RocksDB (via bindgen → libclang), libp2p with
protobuf code generation, and BLAKE3/Ed25519 SIMD paths. On a clean
machine the toolchain setup typically takes 20–40 minutes the first
time, and on Windows the MSVC + libclang interplay is a recurring
source of friction.

This dev container removes all of that. A cold Codespace boots in
about 2 minutes, after which `cargo build` and `cargo test` work
without further configuration.

## Open in Codespaces

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/Savitri-Network/savitri-network?quickstart=1)

The recommended machine size is **4-core / 8 GB RAM / 32 GB storage**
or larger. The free tier (60 hours / month at 2-core) will work for
small experiments but the workspace build saturates 2 cores.

## Open locally (VS Code Dev Containers)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/)
   and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
2. Clone the repo: `git clone https://github.com/Savitri-Network/savitri-network`
3. Open the folder in VS Code.
4. When prompted, choose **Reopen in Container**. The post-create
   script installs system deps and warms the cargo cache; allow
   ~5–10 minutes the first time.

## What's installed

- **Rust toolchain** — pinned by `rust-toolchain.toml` (currently
  `1.90.0`), with `rustfmt`, `clippy`, and `rust-src`.
- **System libraries** — `build-essential`, `cmake`, `pkg-config`,
  `libssl-dev`, `libclang-dev`, `clang`, `llvm-dev`,
  `protobuf-compiler`, `jq`.
- **Docker-in-Docker** — for spinning up the local testnet
  (`cd docker && docker compose up -d`).
- **GitHub CLI** (`gh`) — for working with issues, PRs, releases.
- **VS Code extensions** — `rust-analyzer`, `vscode-lldb` (debugger),
  `even-better-toml`, `dependi` (Cargo.toml dependency lens),
  `errorlens`, `docker`, GitHub PR/Actions/YAML.

## Persistent caches

The container declares three named Docker volumes:

| Volume                       | Mounted at                                  | Purpose                                   |
|------------------------------|---------------------------------------------|-------------------------------------------|
| `savitri-cargo-registry`     | `/usr/local/cargo/registry`                 | Downloaded crate sources                  |
| `savitri-cargo-git`          | `/usr/local/cargo/git`                      | Git-source crates                         |
| `savitri-target`             | `${workspaceFolder}/target`                 | Compiled artifacts (build cache)          |

These survive container rebuilds, so subsequent builds are
incremental even after recreating the dev container. The `target`
volume in particular saves you the ~5–10 minute cold build cost.

If you need to start from scratch (e.g. after a `Cargo.lock` change
that invalidates many crates), remove the volumes:

```bash
docker volume rm savitri-cargo-registry savitri-cargo-git savitri-target
```

## Forwarded ports

| Port range  | Service              |
|-------------|----------------------|
| 8545–8549   | JSON-RPC (lightnode) |
| 5001–5010   | Lightnode P2P        |
| 5021–5025   | Masternode P2P       |
| 9090        | Prometheus           |
| 3000        | Grafana (admin/admin)|

In Codespaces these are exposed as preview URLs in the **Ports** tab.

## Troubleshooting

**`cargo build` fails with `bindgen` or `clang-sys` errors**

The post-create script installs `libclang-dev`, but if you reopened the
container without re-running it (e.g. after editing `devcontainer.json`),
trigger it manually:

```bash
bash .devcontainer/post-create.sh
```

**Out of memory during link step**

The masternode link step needs ~6 GB of RAM at peak. If you're on the
free 2-core Codespace tier and OOM-killed, switch to 4-core/8 GB via
the **Codespaces machine type** menu, or build crates individually:

```bash
cargo build --release -p savitri-core
cargo build --release -p savitri-storage
cargo build --release -p savitri-masternode
```

**`docker compose up` fails with "Cannot connect to the Docker daemon"**

Docker-in-Docker can take ~30 seconds to start after the container
boots. Wait a moment and retry. If it persists, restart the
container.

## Contributing

If you improve this configuration (faster cold build, smaller image,
additional VS Code extensions worth shipping by default), please open
a PR. See [`CONTRIBUTING.md`](../CONTRIBUTING.md) in the repo root.
