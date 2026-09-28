#!/usr/bin/env bash
# Lints every supported target from one machine (CLAUDE.md §7). Type-checking only: tests for
# a target run on that target's CI runner.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
targets=(
  aarch64-apple-darwin
  x86_64-apple-darwin
  aarch64-unknown-linux-gnu
  x86_64-unknown-linux-gnu
  aarch64-pc-windows-msvc
  x86_64-pc-windows-msvc
)
for target in "${targets[@]}"; do
  echo "== $target"
  cargo clippy --workspace --all-targets --all-features --locked --target "$target" -- -D warnings
  bash scripts/check-production.sh --target "$target"
done
