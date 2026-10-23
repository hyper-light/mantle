#!/usr/bin/env bash
# Loops node.rs's objects test on a runner, two CPUs each with two busy loops on them, keeping each
# failing run's output. Usage: diag-objects-loop.sh SLOT RUNS (SLOT picks CPUs 2*SLOT, 2*SLOT+1).
set -u
slot=$1; runs=$2
cpus="$((2*slot)),$((2*slot+1))"
out="$HOME/diag-objects/slot$slot"; mkdir -p "$out"
bin=$(ls -t target/debug/deps/node-* 2>/dev/null | grep -v '\.d$' | head -1)
[ -x "$bin" ] || { echo "no test binary"; exit 1; }
for c in $(echo "$cpus" | tr ',' ' '); do taskset -c "$c" bash -c 'while :; do :; done' & done
busy=$(jobs -p)
pass=0; fail=0
for i in $(seq 1 "$runs"); do
  start=$(date +%s)
  if taskset -c "$cpus" "$bin" --exact objects_round_trip_through_any_node_and_read_back_after_one_is_killed --test-threads 1 > "$out/run$i.log" 2>&1; then
    pass=$((pass+1)); rm -f "$out/run$i.log"
  else
    fail=$((fail+1)); echo "slot $slot run $i FAILED after $(( $(date +%s) - start )) s: $(grep -m1 'panicked\|failed:' "$out/run$i.log" | head -c 300)"
  fi
done
kill $busy 2>/dev/null
echo "slot $slot: $pass passed, $fail failed"
