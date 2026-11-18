#!/usr/bin/env bash
# secnetperf (msquic v2.6.3, quictls) with the bundled libmsquic. Server: secnetperf.sh -bind:127.0.0.1:4433
# Client: secnetperf.sh -target:127.0.0.1 -port:4433 -exec:maxtput -upload:3s -ptput:1
HERE=$(cd "$(dirname "$0")" && pwd)
export LD_LIBRARY_PATH=$HERE/bin${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
exec "$HERE/bin/secnetperf" "$@"
