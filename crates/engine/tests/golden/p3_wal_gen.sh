#!/bin/bash
# Golden WALs for mantle-engine P3's differential (docs/research/24 §3.1 P2, P3): RocksDB 11.8.1's
# own `ldb` (~/Projects/rocksdb at abeebd963, built from src.mk with -O2 -DNDEBUG on aarch64
# macOS) writes each scenario's WAL into a fresh DB and dumps it with
# `ldb dump_wal --header --print_value`. Each WAL is saved as p3_wal/<scenario>.log and its dump's
# header and batch lines as p3_wal/<scenario>.dump; tests/write_batch_oracle_wal.rs reads every
# WAL with the port's log reader and must print the dump line for line.
#
# Run outside the repository's tests, as the golden programs are:
#   bash p3_wal_gen.sh <path to ldb>
set -euo pipefail
LDB=${1:?the oracle ldb}
OUT=$(cd "$(dirname "$0")" && pwd)/p3_wal
mkdir -p "$OUT"

# One scenario: a fresh DB, a seed put, then the commands (each "args|stdin-file" or "args|").
scenario() {
  local name=$1; shift
  local dir; dir=$(mktemp -d)
  "$LDB" --db="$dir" --create_if_missing put seed 0 > /dev/null
  for cmd in "$@"; do
    local args=${cmd%%|*} stdin=${cmd#*|}
    if [ -n "$stdin" ]; then
      # shellcheck disable=SC2086
      "$LDB" --db="$dir" $args < "$stdin" > /dev/null
    else
      # shellcheck disable=SC2086
      "$LDB" --db="$dir" $args > /dev/null
    fi
  done
  local n=0
  for wal in $(ls "$dir"/*.log | sort); do
    cp "$wal" "$OUT/$name-$n.log"
    "$LDB" dump_wal --walfile="$wal" --header --print_value \
      | grep -E '^(Sequence,|[0-9])' > "$OUT/$name-$n.dump"
    n=$((n + 1))
  done
  rm -rf "$dir"
}

# Many single-record batches in one WAL, through ldb's REPL.
session=$(mktemp)
for i in $(seq 0 299); do
  if [ $((i % 7)) -eq 3 ]; then
    printf 'delete key%04d\n' $((i - 3)) >> "$session"
  else
    xs=$(printf '%*s' $((i % 50)) '' | tr ' ' x)
    printf 'put key%04d value-%s\n' "$i" "$xs" >> "$session"
  fi
done
scenario query "query|$session"
rm -f "$session"

# A batch of many puts.
bp="batchput"
for i in $(seq 0 63); do bp="$bp $(printf 'bk%03d' "$i") bv$((i * i))"; done
scenario batchput "$bp|"

# One of each other record the tool writes.
scenario singledelete "singledelete sd|"
scenario deleterange "deleterange a m|"
scenario put_entity "put_entity ent c1:v1 c2:v2 :dflt|"

# A value spanning several 32 KiB log blocks, so the record is fragmented.
scenario fragmented "put big $(head -c 100000 /dev/zero | tr '\0' 'v')|"
