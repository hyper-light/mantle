#!/usr/bin/env bash
# Scratch (never committed): the serve suite, its tests side by side, on CPUs 0-3 as CI's
# linux-x86_64 job's four vCPUs run it, round after round until one fails or the rounds run out. A
# failing round's log is kept whole; each round's result is a line of rounds.txt.
set -uo pipefail
rounds="${ROUNDS:-8}"
out="${OUT:-/tmp/diag-roll}"
mkdir -p "$out"
: > "$out/rounds.txt"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/diag-roll-target}"
built=$(cargo test --locked -p mantle --test serve --no-run 2>&1) || { echo "$built" | tail -30; exit 1; }
bin=$(echo "$built" | sed -n 's/.*Executable tests\/serve.rs (\(.*\))/\1/p' | tail -1)
[ -x "$bin" ] || { echo "no test binary: $built" | tail -5; exit 1; }
echo "binary $bin" | tee -a "$out/rounds.txt"
for round in $(seq 1 "$rounds"); do
  log="$out/round-$round.log"
  start=$(date +%s)
  # The fill test is skipped: this tree predates its fix (it asks again on SlowDown now).
  taskset -c 0-3 "$bin" --nocapture --skip a_cell_whose_volumes_fill > "$log" 2>&1
  code=$?
  end=$(date +%s)
  echo "round $round exit $code seconds $((end - start)) $(grep -a 'test result' "$log" | tail -1)" | tee -a "$out/rounds.txt"
  if [ "$code" != 0 ]; then
    grep -a "FAILED\|panicked" "$log" | head -10 | tee -a "$out/rounds.txt"
    echo "kept $log" | tee -a "$out/rounds.txt"
    exit 0
  fi
  rm -f "$log"
done
echo "no failure in $rounds rounds" | tee -a "$out/rounds.txt"
