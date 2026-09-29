#!/usr/bin/env bash
# Lints every supported target from one machine (CLAUDE.md §7). Type-checking only: tests for
# a target run on that target's CI runner.
#
# aws-lc-sys compiles AWS-LC's C and assembly for the target even to type-check, so each target
# needs a C toolchain for it. The host's own builds its targets; Linux and Apple targets from
# another OS build through cargo-zigbuild (zig's clang and libc headers) and Windows targets
# through cargo-xwin (clang-cl and the Windows SDK), the two drivers aws-lc-rs cross-builds
# with in its own CI. x86_64 Windows also needs NASM for AWS-LC's assembly.
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
host=$(rustc -vV | sed -n 's/^host: //p')

# The clippy driver that can build the target's C on this host.
driver() {
  case "$1" in
    "$host") echo "cargo clippy" ;;
    *-apple-darwin) [[ "$host" == *-apple-darwin ]] && echo "cargo clippy" || echo "cargo-zigbuild clippy" ;;
    *-linux-gnu) echo "cargo-zigbuild clippy" ;;
    *-windows-msvc) [[ "$host" == *-windows-msvc ]] && echo "cargo clippy" || echo "cargo xwin clippy" ;;
    *) echo "no C toolchain known for $1" >&2; return 1 ;;
  esac
}

for target in "${targets[@]}"; do
  clippy=$(driver "$target")
  case "$clippy" in
    cargo-zigbuild*) tools="cargo-zigbuild zig" ;;
    "cargo xwin"*) tools="cargo-xwin" ;;
    *) tools="" ;;
  esac
  if [[ "$target" == x86_64-pc-windows-msvc ]]; then
    tools="$tools nasm"
  fi
  for tool in $tools; do
    if ! command -v "$tool" >/dev/null; then
      echo "$target needs $tool to build AWS-LC's C for it (see the header of $0)" >&2
      exit 1
    fi
  done
  echo "== $target ($clippy)"
  read -r -a command <<<"$clippy"
  "${command[@]}" --workspace --all-targets --all-features --locked --target "$target" -- -D warnings
  CLIPPY="$clippy" bash scripts/check-production.sh --target "$target"
done
