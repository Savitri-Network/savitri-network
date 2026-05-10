# Security policy

The Savitri Network team takes security seriously. This document
describes the supported versions, the responsible-disclosure process
for vulnerabilities, the scope of what we treat as in-scope, and the
expectations for both reporters and maintainers.

## Supported versions

Until the project reaches a stable `1.0` release, only the latest
tagged release of `main` receives security fixes. Older `0.x` releases
are **not** maintained.

| Version | Status |
|---|---|
| `main` (HEAD) | ✅ supported |
| latest tagged `0.x` | ✅ supported |
| earlier `0.x` | ❌ not supported — please upgrade |

## Reporting a vulnerability

**Please do not open a public GitHub issue for a suspected
vulnerability.**

Instead, contact the maintainers by one of the following channels:

- **Email**: `security@savitrinetwork.com` (preferred). PGP key on
  request.
- **GitHub private advisory**: open a draft security advisory at
  https://github.com/Savitri-Network/savitri-network/security/advisories/new
  — only the project maintainers will see it.

Please include:

1. A clear description of the issue and the affected component
   (crate, file, function).
2. A proof-of-concept or minimal reproduction (link to a private gist
   is fine).
3. The impact you believe the vulnerability has — what an attacker
   could achieve.
4. The version, commit SHA, and configuration where you observed it.
5. Any suggested mitigation, if you have one.

## Disclosure timeline

We follow a standard 90-day coordinated-disclosure model:

| Step | Target |
|---|---|
| Acknowledge receipt of the report | within **2 business days** |
| Initial triage (severity, scope, reproduction confirmed) | within **7 days** |
| Fix design and patch development | best effort, typically **30–60 days** |
| Coordinated public disclosure | up to **90 days** from initial report |

We will keep you informed throughout the process and credit you in the
release notes (unless you prefer to remain anonymous).

If a vulnerability is being actively exploited in the wild, we may
shorten the timeline and publish an emergency advisory.

## Scope

**In-scope** (we want to know):

- Cryptographic correctness of any function in `savitri-core`,
  `savitri-zkp`, or the consensus / certificate paths.
- Authentication / authorization issues in the JSON-RPC API
  (`savitri-rpc`).
- Memory-safety bugs (use-after-free, buffer overruns, double-free).
- Denial-of-service vectors that affect a fully patched node — for
  example crashes triggered by malformed peer messages, mempool
  spam patterns that bypass admission control, certificate validation
  bypasses.
- Logic errors in token accounting (mint, burn, vesting, transfers).
- Privilege escalation, sandbox escape, or any path from a
  non-validator role to a validator role.

**Out-of-scope** (please do not report):

- Issues that require physical access to the validator host.
- Theoretical attacks against the underlying cryptographic primitives
  (Ed25519, BLAKE3, SHA-256, RocksDB) — please report those upstream.
- Bugs in *test* or *example* code that is not part of any binary
  shipped to operators.
- Findings that depend on the operator running with non-default
  configuration explicitly documented as insecure.
- Social-engineering attacks against the maintainers or community.
- Findings from automated scanners without a manual reproduction.

## Bug bounty programme

A formal bug bounty programme is **in preparation** and will be
announced in a dedicated repository post and on the project website.
Until that announcement, valid security reports are still acknowledged
publicly (with the reporter's consent) through the channels listed in
the *Hall of fame* section below; reporters who would have qualified
under the bounty rules will be retroactively eligible once the
programme launches.

## Hall of fame

Once a vulnerability is responsibly disclosed and fixed, we will list
the reporter in `SECURITY-CREDITS.md` (with their permission), in the
release notes that ship the fix, and — if applicable — in the
public-facing security advisory.

## A note on testnet keys

Faucet keys, validator keys, or any private key material that ships
inside this repository for testing purposes is, by definition, public.
Please do not report it as a leak. Production deployments are expected
to generate their own keys from scratch.

---

## License

This security policy is published alongside the source code under the
[Apache-2.0 license](LICENSE).
