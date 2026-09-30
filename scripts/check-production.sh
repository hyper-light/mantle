#!/usr/bin/env bash
# The no-panic law (CLAUDE.md §1) on production code alone: libraries and binaries built
# without cfg(test), so the test-only allowances at each crate root do not apply.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
# The clippy driver: `cargo clippy`, or the cross-building one check-targets.sh passes.
read -r -a clippy <<<"${CLIPPY:-cargo clippy}"
lints=(
  -D warnings
  -D clippy::panic
  -D clippy::unwrap_used
  -D clippy::expect_used
  -D clippy::unreachable
  -D clippy::todo
  -D clippy::unimplemented
  -D clippy::indexing_slicing
  -D clippy::arithmetic_side_effects
  -D clippy::string_slice
  -D clippy::cast_possible_truncation
  -D clippy::cast_sign_loss
  -D clippy::cast_possible_wrap
  -D clippy::disallowed_macros
  -D clippy::disallowed_methods
  -D clippy::disallowed_types
  -D clippy::dbg_macro
  -D clippy::exit
)
"${clippy[@]}" --workspace --lib --bins --all-features --locked "$@" -- "${lints[@]}"

# The second pass: methods that panic inside an ordinary call (scripts/panic-paths.toml,
# audit S10a), over the crates whose call sites are fixed. Its configuration is clippy.toml
# with that list appended to disallowed-methods, checked without --all-targets because tests
# may panic. Each crate joins PANIC_PATH_CRATES once its call sites are fixed, and the list
# moves into clippy.toml once every crate has.
PANIC_PATH_CRATES=(mantle-codec mantle-crc mantle-ec mantle-gateway mantle-meta)
conf=target/panic-paths
mkdir -p "$conf"
awk '
  FNR == NR {
    if ($0 ~ /^disallowed-methods = \[/) { inside = 1; next }
    if (inside && $0 ~ /^\]/) { inside = 0; next }
    if (inside) extra = extra $0 "\n"
    next
  }
  /^disallowed-methods = \[/ { open = 1 }
  open && /^\]/ { printf "%s", extra; open = 0; merged = 1 }
  { print }
  END { if (!merged || extra == "") exit 1 }
' scripts/panic-paths.toml clippy.toml >"$conf/clippy.toml.new" || {
  echo "check-production: could not merge scripts/panic-paths.toml into clippy.toml" >&2
  exit 1
}
# Rewritten only on a change, so clippy's cached results stay valid between runs.
if cmp -s "$conf/clippy.toml.new" "$conf/clippy.toml"; then
  rm "$conf/clippy.toml.new"
else
  mv "$conf/clippy.toml.new" "$conf/clippy.toml"
fi
packages=()
for crate in "${PANIC_PATH_CRATES[@]}"; do packages+=(-p "$crate"); done
CLIPPY_CONF_DIR="$PWD/$conf" "${clippy[@]}" "${packages[@]}" --lib --bins --all-features --locked "$@" -- --no-deps "${lints[@]}"
