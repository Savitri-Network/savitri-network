<!--
Thanks for sending a pull request!

Before submitting, please confirm:
  - You have run `cargo fmt --all` locally.
  - You have run `cargo check --workspace` locally.
  - You have added tests for new behavior, or explained why none are needed.
  - The PR title follows conventional commits: `type(scope): summary`
    (e.g. `fix(masternode): correct quorum calculation`).
-->

## Summary

<!-- 1-3 sentences describing the change. -->

## Motivation

<!-- Why this change? Link related issue(s) below. -->

Closes #

## Changes

<!-- Bullet list of the most important changes. -->

-

## Testing

<!-- How did you verify? Unit tests, manual reproduction, loadtest, etc. -->

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo check --workspace`
- [ ] `cargo test --workspace --no-fail-fast`
- [ ] Added/updated tests for the new behavior (or N/A — explain)

## Compatibility

<!--
Does this change affect:
  - the on-wire format (gossipsub topics, message structs, signatures)?
  - the storage format (RocksDB column families, key encoding)?
  - the public API of any published crate?

If yes, describe the migration path.
-->

- [ ] No breaking change
- [ ] Breaking change — described above

## Checklist

- [ ] My commits follow conventional-commits style
- [ ] I have updated `CHANGELOG.md` under the Unreleased section if user-visible
- [ ] I have updated relevant doc-comments and per-crate READMEs
