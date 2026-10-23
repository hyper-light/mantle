#!/usr/bin/env bash
# Runs node.rs's whole suite pinned to CPUS (default 0-3), RUNS times, keeping each run's output
# and counting the transport's stall judgements (diag-stall lines) and failures.
set -u
runs=${1:-3}; cpus=${2:-0-3}
out="$HOME/diag-suite"; rm -rf "$out"; mkdir -p "$out"
bin=$(ls -t target/debug/deps/node-* | grep -v '\.d$' | head -1)
for i in $(seq 1 "$runs"); do
  start=$(date +%s)
  taskset -c "$cpus" "$bin" > "$out/run$i.log" 2>&1; rc=$?
  echo "run $i rc $rc $(( $(date +%s) - start )) s: $(grep -c '^diag-stall unread' "$out/run$i.log") unread, $(grep -c '^diag-stall judge' "$out/run$i.log") judge, $(grep -c 'FAILED' "$out/run$i.log") FAILED lines"
  grep -E '^test .*FAILED|panicked at' "$out/run$i.log" | head -4
done
