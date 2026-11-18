#!/usr/bin/env bash
# RustFS 1.0.4, single node single drive, durability mode strict (the default, set explicitly).
# Usage: start.sh <data-dir> <s3-port>
#
# RUSTFS_DURABILITY_MODE=strict: shard/part files are fdatasynced and xl.meta contents, rollback
# backups and commit-rename directory entries are fsynced before rename_data returns (i.e. before
# the PUT is acknowledged). RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit keeps new buckets on it.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <data-dir> <port>" >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=$(mkdir -p "$1" && cd "$1" && pwd)
PORT=$2
mkdir -p "$DATA/disk" "$DATA/logs"
export RUSTFS_DURABILITY_MODE=strict
export RUSTFS_NEW_BUCKET_DURABILITY_MODE=inherit
export RUSTFS_DRIVE_SYNC_ENABLE=true
export RUSTFS_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export RUSTFS_SECRET_KEY='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
export RUSTFS_REGION=us-east-1
export RUSTFS_ADDRESS=127.0.0.1:$PORT
export RUSTFS_CONSOLE_ENABLE=false
export RUSTFS_OBS_LOG_DIRECTORY=$DATA/logs
echo "rustfs ready on 127.0.0.1:$PORT (starting; S3 answers once initialised)" >&2
exec "$HERE/bin/rustfs" server "$DATA/disk"
