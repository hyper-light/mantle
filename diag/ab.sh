#!/usr/bin/env bash
# DIAGNOSTIC (diag-nfc2): interleaved trials of `mantle bench doors` arms (bash 3.2 and later).
# ab.sh TOP ORDER ARM=BIN... -- BENCH-FLAGS
#   TOP   a folder made for the trials (must not exist); each trial runs in TOP/<n>-<arm>
#   ORDER the arms in the order they run, e.g. ABBAABBA
#   ARM=BIN each arm's letter and binary
# Each trial's line: its arm, its window, the ladder's best per door, the most any phase's
# process was paused, and kiln's headroom events in its window (Linux). Volumes go after each.
set -u -o pipefail
top=$1; order=$2; shift 2
arms=""
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  a=${1%%=*}; b=${1#*=}
  case "$a" in [A-Z]) ;; *) echo "an arm is one capital letter: $a"; exit 1;; esac
  eval "bin_$a=\$b"; arms="$arms $a"; shift
done
[ "${1:-}" = "--" ] && shift
if [ -e "$top" ]; then echo "$top exists"; exit 1; fi
mkdir -p "$top"
for a in $arms; do eval "b=\$bin_$a"; echo "arm $a $b md5 $( (md5sum < "$b" || md5 -q "$b") 2>/dev/null | cut -c1-12)" | tee -a "$top/arms"; done
n=0
i=0
while [ $i -lt ${#order} ]; do
  a=${order:$i:1}; i=$((i+1)); n=$((n+1)); dir=$top/$n-$a
  eval "b=\$bin_$a"
  began=$(date '+%F %T')
  echo "== trial $n arm $a began $began"
  "$b" bench doors "$dir" "$@" > "$top/$n-$a.out" 2>&1
  rc=$?
  ended=$(date '+%F %T')
  echo "   exit $rc ended $ended"
  grep -E ' ladder best ' "$top/$n-$a.out" | grep -v ' set2 ' | sed 's/^/   /'
  grep -E ' step clients ' "$top/$n-$a.out" | awk '{for(i=1;i<=NF;i++){if($i=="paused_ms")p=$(i+1); if($i=="longest_pause_ms")l=$(i+1)} if(p>mp)mp=p; if(l>ml)ml=l} END {printf "   paused_ms most %d longest %d\n", mp, ml}'
  journalctl --user -u 'kiln-agent*' --since "$began" --until "$ended" --no-pager 2>/dev/null | grep -i 'headroom event=kiln' | sed 's/^.*event=/   pacer: /' | cut -c1-140
  grep -E 'verified' "$top/$n-$a.out" | sed 's/^/   /'
  for v in "$dir"/volume-*; do [ -e "$v" ] && rm -rf -- "$v"; done
done
echo "== end $(date '+%F %T')"
