#!/usr/bin/env bash
# Check the models of docs/models with TLC. The checker is fetched once, by its digest, into
# the directory named by TLA_TOOLS (default: target/tla), and runs on the local Java or, where
# there is none, in a Java container.
#
#   scripts/check-model.sh             every model as mantle builds it
#   scripts/check-model.sh unfenced    splits without generation checks: the check passes
#                                      when the checker finds the write it loses
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
tools="${TLA_TOOLS:-$root/target/tla}"
jar="$tools/tla2tools.jar"
digest=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
mkdir -p "$tools"
sum() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}
if [ ! -f "$jar" ] || [ "$(sum "$jar")" != "$digest" ]; then
  curl -sSfL -o "$jar" https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
  if [ "$(sum "$jar")" != "$digest" ]; then
    echo "tla2tools.jar is not the one this script names" >&2
    exit 1
  fi
fi
tlc() { # MODEL CONFIG
  local work="$tools/run-${2%.cfg}"
  rm -rf "$work"
  mkdir -p "$work"
  cp "$root/docs/models/$1.tla" "$root/docs/models/$2" "$work/"
  local status=0
  if java -version >/dev/null 2>&1; then
    (cd "$work" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock \
      -config "$2" "$1.tla" >out.log 2>&1) || status=$?
  else
    docker run --rm -v "$work":/work -v "$jar":/tla2tools.jar:ro -w /work \
      eclipse-temurin:21-jre java -XX:+UseParallelGC -cp /tla2tools.jar tlc2.TLC \
      -workers auto -deadlock -config "$2" "$1.tla" >"$work/out.log" 2>&1 || status=$?
  fi
  grep -E "states generated|Invariant .* is violated|^Error|Finished in" "$work/out.log" | tail -5
  rm -rf "$work/states"
  return "$status"
}
case "${1:-}" in
  "") tlc RangeSplit RangeSplit.cfg ;;
  unfenced)
    status=0
    tlc RangeSplit RangeSplitUnfenced.cfg || status=$?
    # 12: an invariant was violated.
    if [ "$status" -eq 12 ] && grep -q "Invariant NoLostWrite is violated" \
        "$tools/run-RangeSplitUnfenced/out.log"; then
      echo "the checker finds the write lost without generation checks"
      exit 0
    fi
    echo "the checker did not find the lost write (exit $status)" >&2
    exit 1
    ;;
  *) echo "unknown model: $1" >&2; exit 2 ;;
esac
