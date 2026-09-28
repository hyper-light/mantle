# Mantle: rules for every change

Mantle is a Tectonic-style distributed filesystem and object store with an S3 API. One
process on a laptop and a fleet holding exabytes run the same code path.

These rules are absolute. Production code is everything compiled outside a `#[cfg(test)]`
module, `tests/`, `benches/` and `examples/`.

## 1. Production code never panics

Every failure is a typed error returned to a caller that handles it: checked arithmetic
(`checked_*`; `saturating_*` only where saturation is the stated meaning), `try_from` for
narrowing, `.get(..)` for indexing, and a poisoned lock, closed channel or ended task is an
error to return. The workspace lints (`Cargo.toml`, `clippy.toml`) encode this and
`scripts/check-production.sh` is its gate; production code never carries an `#[allow]` for
them. Test code opts out at the crate root with
`#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::indexing_slicing, clippy::arithmetic_side_effects, clippy::disallowed_macros))]`.
A dependency that can panic is called behind an unwind boundary, and the path that reaches
the panic is closed at its cause as well.

## 2. Every resource has a bound

Every queue, cache, map, buffer, retry loop and wait has a stated bound. Reaching it is a
typed refusal or an eviction by a stated rule. A map keyed by a value a peer or client
chooses is bounded and pruned. A loop ends on a counted budget or a deadline.

## 3. Root causes

Fix the cause. A failing test is never quieted by a longer timeout, a retry or a raised
limit. A test waits on the fact it needs, never on a wall-clock guess.

## 4. Decisions cite evidence

A design decision cites peer-reviewed literature or primary-source ground truth (kernel
docs, man pages, vendor API references, RFCs, the AWS S3 API reference) in `docs/design/`,
with the source notes in `docs/research/`. A tuning constant is either measured on the
running hardware or cited, and its comment says which.

## 5. Hardware is detected, then measured

Mantle assumes no device class. Each data device is identified from the OS (Linux sysfs,
macOS IOKit, Windows storage IOCTLs), the identification is checked by measuring the
device, and I/O size, queue depth, direct-I/O use and flush strategy derive from both. A
device the OS cannot describe gets measurement and conservative defaults.

## 6. Durability is exact

A write is acknowledged only once it is durable on every replica the protocol requires:
the platform's full flush (`fdatasync` on Linux, `F_FULLFSYNC` on macOS,
`FlushFileBuffers` on Windows), and the parent directory flushed after a create or rename.
Every on-disk record and every network payload carries a checksum verified on read; a
mismatch is a typed corruption error that feeds repair.

## 7. Portable by construction

Linux, macOS and Windows on x86_64 and aarch64. Platform code sits behind `cfg` with a
portable path beside it. `unsafe` lives only in the OS-interface files that
`scripts/check-contracts.py` lists, every block with a `// SAFETY:` comment stating the
invariant. `scripts/check-targets.sh` lints all six targets from one machine and
`scripts/linux-test.sh` runs the tests on Linux in a container.

## 8. Tests are real

End-to-end first: real processes, real disks, real sockets, real S3 clients. Consensus
and replication also run under deterministic simulation with injected faults. Every hot
path has a benchmark with a recorded baseline, and a performance claim names the run
that measured it.

## 9. Lean code

Code states what the system does now: no speculative abstraction, no dead or commented-out
code. Comments explain why.

## Gates

Every commit passes these on its final tree (`bash scripts/gates.sh` runs them in order and
stops at the first failure); CI runs them on Linux, macOS and Windows, x86_64 and aarch64.

```
python3 scripts/check-contracts.py
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh
cargo deny check advisories bans licenses sources
cargo test --workspace --locked
```

Work lands on `dev`, committed and pushed after each gated step.

## Where things are

- `docs/design/` — architecture and decision records; start at `ARCHITECTURE.md`.
- `docs/research/` — the literature and ground-truth notes decisions cite.
