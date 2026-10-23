#!/usr/bin/env bash
# Runs node.rs's suite RUNS times as the gates do (its tests at once), printing each run's stall
# judgements and, for a failing run, its failures and the traces around them.
set -u
runs=$1
bin=$(ls -t target/aarch64-pc-windows-msvc/debug/deps/node-*.exe | head -1)
fails=0
for i in $(seq 1 "$runs"); do
  start=$(date +%s)
  "$bin" > run.log 2>&1; rc=$?
  echo "run $i rc $rc $(( $(date +%s) - start )) s: $(grep -c '^diag-stall unread' run.log) unread, $(grep -c '^diag-stall judge' run.log) judge, $(grep -c '^diag-client .* object lost' run.log) objects lost"
  grep -E '^diag-stall' run.log | cut -c1-900 | head -40
  if [ "$rc" -ne 0 ]; then
    fails=$((fails + 1))
    grep -E '^diag-test|^diag-client .*(error|lost)|^diag-client .* read_body|^diag-node .*chunk lost|panicked|FAILED|failed:' run.log | cut -c1-900 | head -200
    cp run.log "fail$i.log"
  fi
done
echo "$fails of $runs runs failed"
