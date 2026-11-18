#!/bin/bash
# Diagnostic run (never committed): INSTANCES copies of a repair test at once beside CPU hogs on
# a Mac, ROUNDS rounds.
# Args: LABEL ROUNDS HOGS_PER_CPU INSTANCES FILTER
set -u
LABEL="$1"
ROUNDS="$2"
PER="$3"
INSTANCES="$4"
FILTER="$5"
OUT="$HOME/repair-ci-$LABEL.out"
: >"$OUT"
echo "host $(hostname) ncpu $(sysctl -n hw.ncpu) mem $(sysctl -n hw.memsize) pwd $PWD" >>"$OUT"
cargo test -p mantle --test repair --locked --no-run >"$OUT.build" 2>&1 || {
	echo "BUILD FAILED" >>"$OUT"
	tail -40 "$OUT.build" >>"$OUT"
	exit 1
}
BIN=$(grep -o 'target/debug/deps/repair-[0-9a-f]*' "$OUT.build" | tail -1)
echo "binary $BIN" >>"$OUT"
NCPU=$(sysctl -n hw.ncpu)
HOGS=$((NCPU * PER))
pids=()
for _ in $(seq "$HOGS"); do
	(while :; do :; done) &
	pids+=($!)
done
echo "hogs $HOGS" >>"$OUT"
cleanup() {
	kill "${pids[@]}" 2>/dev/null
	pkill -f "$PWD/target/debug/mantle" 2>/dev/null
}
trap cleanup EXIT
for r in $(seq "$ROUNDS"); do
	echo "ROUND $r begins $(date +%T)" >>"$OUT"
	runs=()
	for i in $(seq "$INSTANCES"); do
		(
			"$BIN" --nocapture --exact "$FILTER" 2>&1 |
				grep --line-buffered -E '^(UNTIL|STEP|REPAIRDBG|ENLISTDBG|test |thread |failures)|panicked|still double|quiet for|test result' |
				sed -l "s/^/[r$r i$i] /" >>"$OUT"
			echo "[r$r i$i] INSTANCE ends rc=${PIPESTATUS[0]} $(date +%T)" >>"$OUT"
		) &
		runs+=($!)
	done
	wait "${runs[@]}"
	echo "ROUND $r ends $(date +%T)" >>"$OUT"
	pkill -f "$PWD/target/debug/mantle" 2>/dev/null
done
echo "ALL DONE" >>"$OUT"
