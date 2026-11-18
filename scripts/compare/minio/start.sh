#!/usr/bin/env bash
# MinIO RELEASE.2025-10-15T17-29-55Z (built from source), single node single drive (xl-single erasure backend).
# Usage: start.sh <data-dir> <s3-port>      (console listens on <port>+1)
#
# Durability as shipped (no option needed): erasure shard files are written (O_DIRECT where possible)
# then fdatasync'd (cmd/xl-storage.go writeAllDirect), and xl.meta is written O_DSYNC or O_DIRECT +
# fdatasync (writeAllInternal) before RenameData commits. The commit rename's directory entry is NOT
# fsynced by default. MINIO_FS_OSYNC=on adds a deferred syscall.Sync() at the end of every RenameData
# (cmd/xl-storage.go:2576), i.e. a whole-machine sync(2) before the PUT is acknowledged, which also makes
# the rename durable. It is on by default here; it flushes every filesystem on the host on each PUT,
# so on a shared box set MINIO_FS_OSYNC=off to keep only MinIO's per-file fdatasync/O_DSYNC.
set -euo pipefail
[ $# -eq 2 ] || { echo "usage: $0 <data-dir> <port>" >&2; exit 2; }
HERE=$(cd "$(dirname "$0")" && pwd)
DATA=$(mkdir -p "$1" && cd "$1" && pwd)
PORT=$2
mkdir -p "$DATA/disk"
export MINIO_FS_OSYNC=${MINIO_FS_OSYNC:-on}
export MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE
export MINIO_ROOT_PASSWORD='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
export MINIO_SITE_REGION=us-east-1
export MINIO_BROWSER=off
export MINIO_UPDATE=off
echo "minio ready on 127.0.0.1:$PORT (starting; MINIO_FS_OSYNC=$MINIO_FS_OSYNC)" >&2
exec "$HERE/bin/minio" server --quiet --address "127.0.0.1:$PORT" --console-address "127.0.0.1:$((PORT + 1))" "$DATA/disk"
