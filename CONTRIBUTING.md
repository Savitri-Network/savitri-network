# Contributing to Savitri Network

Thanks for your interest in contributing! This document describes how the
project is developed, what we expect from a pull request, and how to
move from an idea to a merged change.

The short version: **open an issue first for non-trivial work**, write
small focused commits, and keep tests passing.

---

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Reporting bugs and requesting features](#reporting-bugs-and-requesting-features)
- [Development setup](#development-setup)
- [Branching model](#branching-model)
- [Commit style](#commit-style)
- [Pull requests](#pull-requests)
- [Code style](#code-style)
- [Testing](#testing)
- [Documentation](#documentation)
- [Security](#security)
- [License](#license)

---

## Code of conduct

This project adheres to the
[Contributor Covenant 2.1](CODE_OF_CONDUCT.md). By participating you
agree to honor it. Unacceptable behavior may be reported to the
maintainers via the address listed in [`SECURITY.md`](SECURITY.md).

---

## Reporting bugs and requesting features

- **Bugs** — open an issue using the *Bug report* template. Include the
  exact crate version, your `rustc -V`, OS, and a minimal reproduction.
- **Features** — open an issue using the *Feature request* template.
  For non-trivial features, please discuss the design before sending a
  PR; this saves everyone time.
- **Security vulnerabilities** — do **not** open a public issue. Follow
  the disclosure process in [`SECURITY.md`](SECURITY.md).

Before opening an issue, please search both open and closed issues to
avoid duplicates.

---

## Development setup

You need:

- **Rust** stable ≥ 1.82 (a `rust-toolchain.toml` is provided so
  `rustup` will pick this up automatically).
- A C / C++ toolchain (MSVC on Windows, `build-essential` on Linux,
  Xcode CLT on macOS).
- `cmake`, `clang`, `protobuf-compiler` for transitive dependencies
  (RocksDB, libp2p, BLS support).

Quick check:

```bash
git clone https://github.com/Savitri-Network/savitri-network.git
cd savitri-network
cargo check --workspace
```

---

## Branching model

- The default branch is **`main`**.
- All work happens on feature branches forked from `main`. We do not
  use long-lived develop / release branches; integration happens
  directly via PRs against `main`.
- Recommended branch name: `kind/short-topic`, where `kind` is one of
  `feat`, `fix`, `chore`, `docs`, `refactor`, `perf`, `test`. Examples:
  - `feat/dag-mempool`
  - `fix/lightnode-config-parse`
  - `docs/consensus-spec`

---

## Commit style

We follow **Conventional Commits**. The subject line should fit in
~72 columns and use the imperative mood (*"add",* not *"adds"* /
*"added"*).

```
<type>(<scope>): <subject>

<body — wrap at 72 cols, explain what and why, not how>

<optional footers>
```

Types we use most: `feat`, `fix`, `chore`, `docs`, `refactor`, `perf`,
`test`, `ci`. Common scopes mirror the crate names (`mempool`,
`consensus`, `p2p`, `rpc`, `masternode`, `lightnode`, …).

Examples:

```
feat(mempool): add cursor-based pagination for chain_getBlocks
fix(p2p): handle GossipSub PRUNE on stale topic
docs(consensus): describe PoU score components
```

---

## Pull requests

A PR should:

1. **Reference an issue** (or open one first if the change is not
   trivial). Use `Closes #N` or `Refs #N` in the description.
2. **Be focused** — one logical change per PR. Split unrelated
   refactors into separate PRs.
3. **Pass CI** — `cargo fmt`, `cargo check`, and `cargo test` must be
   green. New code should come with tests when feasible.
4. **Include documentation** — public APIs ship with `///` doc-comments;
   user-facing changes update the relevant `docs/` page or `README.md`.
5. **Update `CHANGELOG.md`** under the `[Unreleased]` section, in the
   format described in the file.

When you open the PR, the *Pull request template* will guide you
through the checklist.

### Review and merge

- A maintainer will review your PR. We aim to give a first response
  within a few business days.
- Once approved and CI green, the PR is **squash-merged** so `main`
  has one commit per logical change.
- Force-pushing to `main` is reserved for maintainer hygiene
  operations and requires a clear reason in the commit message.

---

## Code style

- Run `cargo fmt --all` before committing. The configuration lives in
  `rustfmt.toml`. CI rejects unformatted code.
- Run `cargo clippy --workspace --all-targets` and address warnings
  introduced by your change. Clippy is currently advisory in CI but
  will become gating once the existing baseline is cleaned up.
- Prefer `?` over `.unwrap()` / `.expect()` outside of test code.
- Write idiomatic, defensive Rust:
  - No `panic!`/`unwrap` in code that runs on the network path.
  - Explicit error types via `thiserror` for libraries; `anyhow` for
    binaries.
  - Bound-checked arithmetic on token amounts (`checked_*` /
    `saturating_*`).
- Logs use the `tracing` crate. Emit structured fields, not
  string-formatted ones, so the JSON exporter can flatten them.

---

## Testing

- **Unit tests** live next to the code in a `#[cfg(test)] mod tests`
  block.
- **Integration tests** go under `tests/` per crate and are run by
  `cargo test --workspace`.
- For consensus / mempool changes, please add at least one
  property-based test (`proptest`) or a fuzz target (`cargo-fuzz`)
  when feasible.
- Tests should be deterministic. If you need randomness, seed it
  explicitly so failures are reproducible.

Run the full suite:

```bash
cargo test --workspace --no-fail-fast
```

---

## Documentation

- **Public APIs**: every `pub` item in a library crate should have a
  `///` doc-comment. Where possible, include a small example so it
  shows up on docs.rs.
- **User docs**: guides live under [`docs/`](docs/). When you change a
  user-visible behavior, update the matching guide in the same PR.
- **Roadmap**: high-level direction is tracked in [`ROADMAP.md`](ROADMAP.md)
  and as GitHub milestones (`Phase 1`, `Phase 2`, `Phase 3`).

---

## Security

If you believe you have found a security vulnerability, please follow
the responsible-disclosure process described in
[`SECURITY.md`](SECURITY.md). Do **not** open a public issue with
exploit details.

---

## License

By contributing you agree that your contributions will be licensed
under the [Apache-2.0 license](LICENSE) of this repository.
