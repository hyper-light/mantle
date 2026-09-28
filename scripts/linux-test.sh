#!/usr/bin/env bash
# Runs the workspace tests on Linux in a container (the host's architecture by default; pass
# --platform linux/amd64 for x86_64 under emulation). TMPDIR is a bind-mounted host
# directory or a tmpfs, so tests see a real file system rather than the image's overlay.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
platform="${PLATFORM:-}"
tmpfs_mode="${TMPFS:-0}"
args=(run --rm -v "$PWD":/work -w /work
  -v mantle-cargo-registry:/usr/local/cargo/registry
  -v "mantle-target-${platform//\//-}":/work/target
  -e CARGO_TERM_COLOR=never)
[ -n "$platform" ] && args+=(--platform "$platform")
if [ "$tmpfs_mode" = "1" ]; then
  args+=(--tmpfs /scratch:rw,exec,size=2g -e TMPDIR=/scratch)
else
  args+=(-v mantle-scratch:/scratch -e TMPDIR=/scratch)
fi
docker "${args[@]}" rust:1.98.0 bash -c "rustup component add clippy rustfmt >/dev/null 2>&1; cargo test --workspace --locked $*"
