#!/usr/bin/env bash
# DIAGNOSTIC (diag-nfc2): a closed ladder of 1-byte PUTs through both doors, traced.
# repro.sh BIN NAME [extra bench flags]: runs in ~/nfc2/runs/NAME (which must not exist).
set -u -o pipefail
bin=$1; name=$2; shift 2
top=$HOME/nfc2/runs
dir=$top/$name
mkdir -p "$top"
if [ -e "$dir" ]; then echo "$dir exists"; exit 1; fi
mkdir -p "$dir/trace"
echo "== $name $(date -u +%FT%TZ) load $(cut -d' ' -f1-3 /proc/loadavg) bin $(md5sum < "$bin" | cut -c1-12)"
MANTLE_DIAG_TRACE=$dir/trace "$bin" bench doors "$dir/run" --load closed --sizes 1 --kinds PUT --ops 1000 --clients 1024 --objects 2 --volume 256M "$@" > "$dir/out.log" 2>&1
echo "exit $?"
grep -E '^doors (native|http) PUT 1B ' "$dir/out.log" | awk '{for(i=1;i<=NF;i++){if($i=="clients")c=$(i+1); if($i=="elapsed_ms")e=$(i+1); if($i=="ops")o=$(i+1); if($i=="p50")p=$(i+1); if($i=="max")m=$(i+1); if($i=="again")a=$(i+1); if($i=="major_faults/op")mf=$(i+1)} if(e>0) printf "   %s clients %s ops %s elapsed_ms %s rate %.1f/s p50_us %s max_us %s again %s majf %s\n", $2, c, o, e, o*1000/e, p, m, a, mf}'
grep -E 'verified|clients at most' "$dir/out.log"
# the volumes go; the cell's logs and stores and the trace stay
for v in "$dir"/run/volume-*; do [ -e "$v" ] && rm -rf -- "$v"; done
echo "== end $(date -u +%FT%TZ)"
