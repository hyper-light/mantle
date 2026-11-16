#!/usr/bin/env bash
# The previous wire profile's `mantle` (docs/research/91), which tests/upgrade.rs runs a cell's
# other release on: built from the commit scripts/previous-profile names, in a worktree of its own
# under the repository's target, with its own target, once; a build already there is used again.
# Prints the binary's path, which the suite takes as MANTLE_PREVIOUS.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
commit="$(grep -v '^#' "$root/scripts/previous-profile" | head -n 1)"
tree="$root/target/previous/$commit"
binary="$tree/target/debug/mantle"
if [ ! -x "$binary" ]; then
  if ! git -C "$root" cat-file -e "$commit^{commit}" 2> /dev/null; then
    # A shallow checkout, as CI's, holds no older commit: fetched by its id.
    git -C "$root" fetch --quiet --depth 1 origin "$commit"
  fi
  if [ ! -d "$tree" ]; then
    git -C "$root" worktree add --quiet --detach "$tree" "$commit"
  fi
  cargo build --quiet --locked -p mantle --bin mantle \
    --manifest-path "$tree/Cargo.toml" --target-dir "$tree/target" >&2
fi
echo "$binary"
