#!/usr/bin/env bash
# Every gate of CLAUDE.md in order; exits non-zero on the first failure. Run before each commit.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
python3 scripts/check-contracts.py
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
bash scripts/check-production.sh
cargo deny check advisories bans licenses sources 2>/dev/null
cargo test --workspace --locked
