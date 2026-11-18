#!/usr/bin/env bash
# SeaweedFS 4.52, single node `weed server` (master + volume + filer + S3), fsync on for every path.
# Usage: start.sh <data-dir> <s3-port>
# Ports used (127.0.0.1): S3 <port>; master <port>+1, volume <port>+2, filer <port>+3;
# gRPC <port>+11..+14. Iceberg/Lance listeners disabled. -volume.minFreeSpace defaults to 4GiB
# (env SEAWEED_MIN_FREE) because the stock 1% threshold marks volumes read-only on a 99%-full disk.
#
# Durability: a filer.conf storage rule `fs.configure -locationPrefix=/ -fsync=true` makes the filer's
# AssignVolume answer Fsync=true, so the S3 gateway uploads each chunk with ?fsync=true; the volume
# server then fsyncs the .dat (group-committed) and the .idx before replying, and the filer's
# leveldb2 metadata write uses WriteOptions{Sync:true}. All before the S3 200.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <data-dir> <port>" >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
W=$HERE/bin/weed
DATA=$(mkdir -p "$1" && cd "$1" && pwd)
P=$2
mkdir -p "$DATA/store"
cat > "$DATA/s3.json" <<'EOF'
{"identities":[{"name":"bench","credentials":[{"accessKey":"AKIAIOSFODNN7EXAMPLE","secretKey":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
EOF

"$W" server -ip=127.0.0.1 -ip.bind=127.0.0.1 -dir="$DATA/store" \
  -master.port=$((P + 1)) -master.port.grpc=$((P + 11)) \
  -volume.port=$((P + 2)) -volume.port.grpc=$((P + 12)) \
  -filer -filer.port=$((P + 3)) -filer.port.grpc=$((P + 13)) \
  -s3 -s3.port=$P -s3.port.grpc=$((P + 14)) -s3.config="$DATA/s3.json" \
  -s3.port.iceberg=0 -s3.port.lance=0 -volume.minFreeSpace="${SEAWEED_MIN_FREE:-4GiB}" -volume.preStopSeconds=0 -master.telemetry=false &
PID=$!
trap 'kill -TERM $PID 2>/dev/null; wait $PID' INT TERM

# Apply the fsync storage rule once the filer answers (persisted in the filer, so idempotent).
for _ in $(seq 1 300); do curl -sf -o /dev/null "http://127.0.0.1:$((P + 3))/" && break; sleep 0.2; done
echo 'fs.configure -locationPrefix=/ -fsync=true -apply' |
  "$W" shell -master=127.0.0.1:$((P + 1)).$((P + 11)) -filer=127.0.0.1:$((P + 3)).$((P + 13)) >&2
echo "seaweedfs ready on 127.0.0.1:$P (fsync rule applied)" >&2
wait $PID
