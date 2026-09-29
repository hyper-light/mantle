#!/usr/bin/env bash
# The no-panic law (CLAUDE.md §1) on production code alone: libraries and binaries built
# without cfg(test), so the test-only allowances at each crate root do not apply.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
# The clippy driver: `cargo clippy`, or the cross-building one check-targets.sh passes.
read -r -a clippy <<<"${CLIPPY:-cargo clippy}"
"${clippy[@]}" --workspace --lib --bins --all-features --locked "$@" -- \
  -D warnings \
  -D clippy::panic \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::unreachable \
  -D clippy::todo \
  -D clippy::unimplemented \
  -D clippy::indexing_slicing \
  -D clippy::arithmetic_side_effects \
  -D clippy::string_slice \
  -D clippy::cast_possible_truncation \
  -D clippy::cast_sign_loss \
  -D clippy::cast_possible_wrap \
  -D clippy::disallowed_macros \
  -D clippy::disallowed_methods \
  -D clippy::disallowed_types \
  -D clippy::dbg_macro \
  -D clippy::exit
