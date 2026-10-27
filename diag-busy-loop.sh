#!/usr/bin/env bash
# Scratch (never committed): the native suite, its tests side by side, on CPUs 0-1 beside two
# busy loops on the same CPUs, as CI's linux-x86_64-2cpu-busy job runs it, round after round until
# one fails or the rounds run out. A failing round's log is kept whole; each round's result is a line
# of rounds.txt.
set -uo pipefail
rounds="${ROUNDS:-10}"
out="${OUT:-/tmp/diag-busy}"
mkdir -p "$out"
: > "$out/rounds.txt"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/diag-busy-target}"
built=$(cargo test --locked -p mantle --test native --no-run 2>&1) || { echo "$built" | tail -30; exit 1; }
bin=$(echo "$built" | sed -n 's/.*Executable tests\/native.rs (\(.*\))/\1/p' | tail -1)
[ -x "$bin" ] || { echo "no test binary: $built" | tail -5; exit 1; }
echo "binary $bin" | tee -a "$out/rounds.txt"
for round in $(seq 1 "$rounds"); do
  hogs=()
  for _ in 1 2; do
    taskset -c 0-1 sh -c 'while :; do :; done' &
    hogs+=("$!")
  done
  log="$out/round-$round.log"
  start=$(date +%s)
  # The whole suite, its tests side by side, as CI's job runs it.
  taskset -c 0-1 "$bin" --nocapture > "$log" 2>&1
  code=$?
  for pid in "${hogs[@]}"; do
    kill "$pid" || echo "kill $pid failed" >> "$out/rounds.txt"
  done
  wait "${hogs[@]}" 2>/dev/null
  end=$(date +%s)
  echo "round $round exit $code seconds $((end - start))" | tee -a "$out/rounds.txt"
  if [ "$code" != 0 ]; then
    echo "kept $log" | tee -a "$out/rounds.txt"
    exit 0
  fi
  rm -f "$log"
done
echo "no failure in $rounds rounds" | tee -a "$out/rounds.txt"
