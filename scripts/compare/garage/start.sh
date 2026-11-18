#!/usr/bin/env bash
# Garage v2.5.0, single node, data_fsync + metadata_fsync on (LMDB engine).
# Usage: start.sh <data-dir> <s3-port>
# Uses <port>+1 for RPC and <port>+2 for the admin API (both 127.0.0.1).
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <data-dir> <port>" >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=$HERE/bin/garage
DATA=$(mkdir -p "$1" && cd "$1" && pwd)
PORT=$2
CONF=$DATA/garage.toml
mkdir -p "$DATA/meta" "$DATA/data"
[ -f "$DATA/rpc_secret" ] || head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$DATA/rpc_secret"
cat > "$CONF" <<EOF
metadata_dir = "$DATA/meta"
data_dir = "$DATA/data"
db_engine = "lmdb"
metadata_fsync = true
data_fsync = true
replication_factor = 1
rpc_bind_addr = "127.0.0.1:$((PORT + 1))"
rpc_public_addr = "127.0.0.1:$((PORT + 1))"
rpc_secret = "$(cat "$DATA/rpc_secret")"

[s3_api]
s3_region = "us-east-1"
api_bind_addr = "127.0.0.1:$PORT"
root_domain = ".s3.localhost"

[admin]
api_bind_addr = "127.0.0.1:$((PORT + 2))"
EOF

export RUST_LOG=${RUST_LOG:-garage=info}
"$BIN" -c "$CONF" server &
PID=$!
trap 'kill -TERM $PID 2>/dev/null; wait $PID' INT TERM

# One-time cluster setup: layout with this node, then the fixed benchmark key.
for _ in $(seq 1 100); do "$BIN" -c "$CONF" status >/dev/null 2>&1 && break; sleep 0.1; done
if ! "$BIN" -c "$CONF" key info AKIAIOSFODNN7EXAMPLE >/dev/null 2>&1; then
  NODE=$("$BIN" -c "$CONF" node id -q | cut -c1-16)
  "$BIN" -c "$CONF" layout assign -z dc1 -c 100G "$NODE" >&2
  "$BIN" -c "$CONF" layout apply --version 1 >&2
  "$BIN" -c "$CONF" key import --yes -n bench AKIAIOSFODNN7EXAMPLE 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY' >&2
  "$BIN" -c "$CONF" key allow --create-bucket AKIAIOSFODNN7EXAMPLE >&2
fi
echo "garage ready on 127.0.0.1:$PORT" >&2
wait $PID
