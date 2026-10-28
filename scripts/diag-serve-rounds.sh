#!/usr/bin/env bash
# Scratch (diag/roll-stop-bound, never merged): the serve suite in rounds, as CI runs it, each
# round's whole output kept under rounds/. At most 40 rounds; no round begins unless the longest
# so far, half again, still fits within the step's 290 minutes (275 used, the rest the upload's).
set -u
target="$1"
mkdir -p rounds
start=$(date +%s)
budget=$((275 * 60))
longest=0
failed=0
ran=0
for round in $(seq 1 40); do
  now=$(date +%s)
  elapsed=$((now - start))
  if [ "$round" -gt 1 ] && [ $((elapsed + longest * 3 / 2)) -gt "$budget" ]; then
    echo "=== no round $round: ${elapsed}s spent, the longest round ${longest}s"
    break
  fi
  log="rounds/round-$(printf '%02d' "$round").log"
  echo "=== round $round at $(date -u +%H:%M:%S)"
  began=$(date +%s)
  if ! cargo test --locked -p mantle --test serve --target "$target" > "$log" 2>&1; then
    failed=$((failed + 1))
    echo "=== round $round FAILED"
  fi
  took=$(( $(date +%s) - began ))
  if [ "$took" -gt "$longest" ]; then longest=$took; fi
  ran=$round
  echo "=== round $round took ${took}s"
  grep -aE "^test .* (ok|FAILED)|test result|BOUND-PASSED|stopped at once|panicked|DIAG-ROLL" "$log" | cut -c1-2000 || true
done
echo "=== $failed of $ran rounds failed"
test "$failed" -eq 0
