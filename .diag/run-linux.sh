#!/bin/bash
# Diagnostic run (never committed): repair tests pinned to CPUs with taskset on Linux.
# Args: LABEL ROUNDS GROUPS HOGS FILTER
#   GROUPS: CPU lists separated by ';', one instance of the test binary pinned to each.
#   HOGS: busy loops pinned to each group beside its instance.
set -u
LABEL="$1"
ROUNDS="$2"
GROUPS="$3"
HOGS="$4"
FILTER="${5:-}"
OUT="$HOME/repair-ci-$LABEL.out"
: >"$OUT"
exec 9>"$HOME/heavy-run.lock"
echo "waiting for heavy-run lock $(date +%T)" >>"$OUT"
flock 9
echo "host $(hostname) nproc $(nproc) pwd $PWD lock taken $(date +%T)" >>"$OUT"
cargo test -j 16 -p mantle --test repair --locked --no-run >"$OUT.build" 2>&1 || {
	echo "BUILD FAILED" >>"$OUT"
	tail -40 "$OUT.build" >>"$OUT"
	exit 1
}
BIN=$(grep -o 'target/debug/deps/repair-[0-9a-f]*' "$OUT.build" | tail -1)
echo "binary $BIN" >>"$OUT"
pids=()
IFS=';' read -r -a groups <<<"$GROUPS"
for g in "${groups[@]}"; do
	for _ in $(seq "$HOGS"); do
		taskset -c "$g" sh -c 'while :; do :; done' &
		pids+=($!)
	done
done
cleanup() {
	kill "${pids[@]}" 2>/dev/null
	pkill -f "$PWD/target/debug/mantle" 2>/dev/null
}
trap cleanup EXIT
for r in $(seq "$ROUNDS"); do
	echo "ROUND $r begins $(date +%T)" >>"$OUT"
	runs=()
	i=0
	for g in "${groups[@]}"; do
		i=$((i + 1))
		(
			# shellcheck disable=SC2086
			taskset -c "$g" "$BIN" --nocapture $FILTER 2>&1 |
				grep --line-buffered -E '^(UNTIL|STEP|REPAIRDBG|ENLISTDBG|test |thread |failures)|panicked|still double|quiet for|test result' |
				sed -u "s/^/[r$r i$i] /" >>"$OUT"
			echo "[r$r i$i] INSTANCE ends rc=${PIPESTATUS[0]} $(date +%T)" >>"$OUT"
		) &
		runs+=($!)
	done
	wait "${runs[@]}"
	echo "ROUND $r ends $(date +%T)" >>"$OUT"
done
echo "ALL DONE" >>"$OUT"
