#!/usr/bin/env bash
# DIAG (ci-health, never merged): runs one test binary's one test ROUNDS times beside HOGS busy
# loops, each round's whole output kept for a failure, outcomes counted by kind.
set -uo pipefail
bin="$1"; test_name="$2"; rounds="$3"; hogs="$4"; label="$5"
mkdir -p "diag-out/$label"
pids=()
for _ in $(seq "$hogs"); do (while :; do :; done) & pids+=($!); done
pass=0; quiet=0; wake=0; other=0
for round in $(seq "$rounds"); do
  out="diag-out/$label/round-$round.log"
  started=$(date +%s)
  timeout 1200 "$bin" --exact "$test_name" --test-threads=1 --nocapture > "$out" 2>&1
  rc=$?
  took=$(( $(date +%s) - started ))
  if [ "$rc" -eq 0 ]; then
    pass=$((pass + 1)); kind=pass; rm -f "$out"
  elif grep -q "MeasurementTimeout" "$out"; then
    wake=$((wake + 1)); kind=wake
  elif grep -q "quiet for" "$out"; then
    quiet=$((quiet + 1)); kind=quiet
  else
    other=$((other + 1)); kind="other rc=$rc"
  fi
  echo "DIAGLOOP $label round $round: $kind in ${took}s"
done
kill "${pids[@]}" 2>/dev/null || true
echo "DIAGLOOP SUMMARY $label: rounds=$rounds hogs=$hogs pass=$pass quiet=$quiet wake=$wake other=$other"
for f in diag-out/"$label"/round-*.log; do
  [ -e "$f" ] || continue
  echo "===== $f"
  grep -E "quiet for|MeasurementTimeout|panicked|DIAGWAKE|refused to start|error" "$f" | head -40 | cut -c1-3000
done
