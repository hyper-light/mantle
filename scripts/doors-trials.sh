#!/usr/bin/env bash
# The box's trial runner for `mantle bench doors` (docs/design/measurement.md §12; research/79):
# every case a fresh process pinned to CPUSET 8-15 inside a systemd memory scope, its arms in a
# Williams order in whole cycles, each case's rusage and the host's load kept, and every expected
# case accounted for as completed, failed with its reason, or excluded with its reason.
#
#   OUT=DIR scripts/doors-trials.sh plan            write DIR/plan.tsv, the batch's cases in order
#   OUT=DIR scripts/doors-trials.sh run             run each planned case not yet run
#   OUT=DIR scripts/doors-trials.sh case SEQ        run the planned case SEQ alone
#   python3 scripts/doors-tables.py DIR             regenerate the tables and the trial report
#
# DIR/bin holds the arms: mantle-A1 and mantle-A2 (one timed release build, copied twice, the A/A),
# mantle-C (the `count-allocs` build of the same commit) and hyper-quic-geo_open_loop. The
# comparators are COMPARE's (its COMPARATORS.md): garage, seaweedfs and rustfs through the HTTP door
# with mantle-A1 as the harness, secnetperf and quinn-perf beside bare hyper-quic for QUIC.
# Raw outputs go to DIR/raw, named box_CONDITION_ARM_rROUND; each case's data goes under WORK and
# is removed after the case. Environment: CYCLES (Williams cycles, 1), FULL_CONDITIONS (the
# conditions that run every size and the set), MID_CONDITIONS (64 KiB, 8 MiB and the
# set; the rest run LEAN_*), and the matrices below.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:?OUT=DIR: the batch directory, holding bin/}
BIN=$OUT/bin
RAW=$OUT/raw
COMPARE=${COMPARE:-/home/adalundhe/Projects/mantle-compare}
# Each case's data: a literal root, so no removal ever names a directory a variable left empty.
WORK=/home/adalundhe/.cache/claude-scratch/doors/work
CYCLES=${CYCLES:-1}

CONDITIONS=(quiet busy1x busy2x cpu1 cpu2 memfloor)
FULL_CONDITIONS=${FULL_CONDITIONS:-"quiet"}
MID_CONDITIONS=${MID_CONDITIONS:-"busy2x"}
S3_ARMS=(A1 A2 C garage seaweedfs rustfs)
QUIC_ARMS=(secnetperf quinn hyperquic)

# The node's memory floor, summed over the cell's three nodes: each node's `memory.floor.*` basis
# lines as `mantle serve` at 4711a46a says them on this box (objects 83513198, volumes 147172608,
# engines 822800, ranges 267841530, log 225853540, transport 69725483, threads 27262976: 822192135
# bytes a node; crates/node/src/derive.rs `floor`, `say_floor`), read from a founder of a cell made
# as `mantle bench doors` makes one (DIR/floor-probe-serve.out).
FLOOR=$((3 * 822192135))
# The scope every other case runs in: the cell's three nodes and the harness, or a comparator and
# the harness, well within the box's memory; QUIC's two endpoints.
S3_MEMORY=24G
QUIC_MEMORY=4G

# The S3 matrix: the full one (every size and the 10,000 set) under FULL_CONDITIONS, the mid one
# under MID_CONDITIONS, the lean one under the rest. The 100,000 set is cut from the box's ten
# hours: its puts and deletes run at the doors' write capacity here, about 20 a second through the
# native door, over two hours a mantle case (the dimensioning pass, 2026-11-04). A sized phase asks OPS operations at most (a p99's 368, measurement
# §5a) and STEP bytes; a set's phases ask every object of a door's half, 5,000 and 50,000.
FULL_ARGS=${FULL_ARGS:-"--objects 10000 --ops 368"}
MID_ARGS=${MID_ARGS:-"--sizes 65536,8M --objects 10000 --ops 368"}
LEAN_ARGS=${LEAN_ARGS:-"--sizes 65536,8M --objects 0 --ops 368"}
LOAD_ARGS=${LOAD_ARGS:-"--load open:500 --clients 16"}
# A case past its deadline is stopped (SIGINT, then SIGKILL) and reported failed.
FULL_DEADLINE=${FULL_DEADLINE:-3600}
MID_DEADLINE=${MID_DEADLINE:-2400}
LEAN_DEADLINE=${LEAN_DEADLINE:-900}
QUIC_DEADLINE=${QUIC_DEADLINE:-600}
PORT=39500
KEY=AKIAIOSFODNN7EXAMPLE
SECRET='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
# A case needs a cell's three 12 GiB volumes and its objects: never started with less free.
FREE_FLOOR_KIB=$((80 * 1024 * 1024))

# A Williams design's rows for n arms: row r is the first row plus r, modulo n, the first row
# 0, 1, n-1, 2, n-2, ...; each arm follows each other once in n rows for an even n, and with the
# rows reversed beside them (2n rows) for an odd n.
williams() {
  local n=$1 first=() lo=1 hi=$(($1 - 1)) r i
  first+=(0)
  while [ ${#first[@]} -lt "$n" ]; do
    first+=("$lo"); lo=$((lo + 1))
    [ ${#first[@]} -lt "$n" ] && { first+=("$hi"); hi=$((hi - 1)); }
  done
  for ((r = 0; r < n; r++)); do
    local row=()
    for i in "${first[@]}"; do row+=($(((i + r) % n))); done
    echo "${row[*]}"
  done
  if [ $((n % 2)) -eq 1 ]; then
    for ((r = 0; r < n; r++)); do
      local row=()
      for i in "${first[@]}"; do row+=($(((i + r) % n))); done
      local rev=()
      for ((i = n - 1; i >= 0; i--)); do rev+=("${row[$i]}"); done
      echo "${rev[*]}"
    done
  fi
}

# The plan: each cycle gives every condition one Williams row of the S3 arms and one of QUIC's,
# the conditions' order turned a place each cycle, so over the cycles each condition meets
# different rows. The S3 square's six rows are one cycle; QUIC's three arms need six rows too.
plan() {
  mkdir -p "$OUT" "$RAW"
  local s3_rows quic_rows seq=0 cycle c i cond row a matrix
  mapfile -t s3_rows < <(williams ${#S3_ARMS[@]})
  mapfile -t quic_rows < <(williams ${#QUIC_ARMS[@]})
  {
    printf 'seq\tkind\tcondition\tarm\tround\tmatrix\tnote\n'
    for ((cycle = 1; cycle <= CYCLES; cycle++)); do
      for ((i = 0; i < ${#CONDITIONS[@]}; i++)); do
        c=$(((i + cycle - 1) % ${#CONDITIONS[@]}))
        cond=${CONDITIONS[$c]}
        matrix=lean
        [[ " $MID_CONDITIONS " == *" $cond "* ]] && matrix=mid
        [[ " $FULL_CONDITIONS " == *" $cond "* ]] && matrix=full
        row=${s3_rows[$(((i + (cycle - 1) * ${#CONDITIONS[@]}) % ${#s3_rows[@]}))]}
        for a in $row; do
          seq=$((seq + 1))
          printf '%s\ts3\t%s\t%s\t%s\t%s\t-\n' "$seq" "$cond" "${S3_ARMS[$a]}" "$cycle" "$matrix"
        done
        row=${quic_rows[$(((i + (cycle - 1) * ${#CONDITIONS[@]}) % ${#quic_rows[@]}))]}
        for a in $row; do
          seq=$((seq + 1))
          printf '%s\tquic\t%s\t%s\t%s\t-\t-\n' "$seq" "$cond" "${QUIC_ARMS[$a]}" "$cycle"
        done
        seq=$((seq + 1))
        printf '%s\texcluded\t%s\tminio\t%s\t-\t%s\n' "$seq" "$cond" "$cycle" \
          "MinIO runs on the Mac: its full-sync mode (MINIO_FS_OSYNC, a whole-machine sync(2) a PUT) flushes the shared box (owner's decision, 2026-11-04)"
        for a in A1 A2 C; do
          seq=$((seq + 1))
          printf '%s\texcluded\t%s\t%s-native-relay500\t%s\t-\t%s\n' "$seq" "$cond" "$a" "$cycle" \
            "the 500 ms one-way relay with loss and reordering between client and nodes is not in the harness: geo_open_loop's relay is an example's threads, not a library the cell's clients can be pointed through"
        done
      done
    done
  } > "$OUT/plan.tsv"
  echo "planned $seq cases in $OUT/plan.tsv"
}

cpus_of() {
  case $1 in cpu1) echo 8 ;; cpu2) echo 8-9 ;; *) echo 8-15 ;; esac
}

busy_of() {
  case $1 in busy1x) echo 8 ;; busy2x) echo 16 ;; *) echo 0 ;; esac
}

# The host as the case starts or ends: time, load averages, memory and swap free, pressure.
host() {
  local avail swap io cpu mem
  avail=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  swap=$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)
  io=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/io)
  cpu=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/cpu)
  mem=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/memory)
  echo "$1 epoch $(date +%s) loadavg $(cut -d' ' -f1-3 /proc/loadavg) memavailable_kib $avail swapfree_kib $swap psi_io_some10 $io psi_cpu_some10 $cpu psi_mem_some10 $mem"
}

# Runs a command and writes the rusage of its process tree (RUSAGE_CHILDREN: every descendant
# waited for) to FILE as JSON; exits with the command's status.
RUSAGE='import json, resource, subprocess, sys, time
began = time.monotonic()
rc = subprocess.call(sys.argv[2:])
r = resource.getrusage(resource.RUSAGE_CHILDREN)
with open(sys.argv[1], "w") as f:
    json.dump({"rc": rc, "wall_s": round(time.monotonic() - began, 3), "user_s": r.ru_utime, "system_s": r.ru_stime,
               "maxrss_kib": r.ru_maxrss, "minflt": r.ru_minflt, "majflt": r.ru_majflt, "nvcsw": r.ru_nvcsw,
               "nivcsw": r.ru_nivcsw, "inblock": r.ru_inblock, "oublock": r.ru_oublock}, f)
    f.write("\n")
sys.exit(rc if rc >= 0 else 128 - rc)'

# One S3 case through a comparator, run inside its scope: the server started fresh on a fresh data
# directory, the harness against it, the server stopped.
endpoint_case() {
  local name=$1 cond=$2 arm=$3 matrix=$4 data server pid code i rc
  data=$WORK/$name/data
  mkdir -p "$data"
  "$COMPARE/$arm/start.sh" "$data" "$PORT" > "$RAW/$name.server.out" 2> "$RAW/$name.server.err" &
  server=$!
  # Ready once it says so and S3 answers other than 503 (RustFS answers 503 while it starts):
  # at most 1,200 looks a tenth of a second apart.
  for ((i = 0; i < 1200; i++)); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/" || true)
    if grep -q 'ready on' "$RAW/$name.server.err" && [ "$code" != 000 ] && [ "$code" != 503 ]; then
      break
    fi
    sleep 0.1
  done
  case $arm in
    garage) pid=$(pgrep -P "$server" -x garage || true) ;;
    seaweedfs) pid=$(pgrep -P "$server" -x weed || true) ;;
    rustfs) pid=$server ;;
  esac
  if [ -z "$pid" ] || [ "$i" -ge 1200 ]; then
    echo "doors-trials: $arm did not become ready (last answer $code)" >&2
    kill -TERM "$server" 2> /dev/null || true
    wait "$server" || true
    return 3
  fi
  rc=0
  # shellcheck disable=SC2086
  "$BIN/mantle-A1" bench doors "$WORK/$name/run" --endpoint "http://127.0.0.1:$PORT" --arm "$arm" \
    --key "$KEY" --secret "$SECRET" --region us-east-1 --server-pid "$pid" --condition "$cond" --perf \
    $(matrix_args "$matrix") $LOAD_ARGS || rc=$?
  kill -TERM "$server" 2> /dev/null || true
  wait "$server" || true
  return "$rc"
}

matrix_args() {
  case $1 in full) echo "$FULL_ARGS" ;; mid) echo "$MID_ARGS" ;; *) echo "$LEAN_ARGS" ;; esac
}

# One QUIC comparator case, inside its scope: the server and its client on loopback, each size.
quic_case() {
  local name=$1 cond=$2 arm=$3 server i label
  case $arm in
    secnetperf)
      "$COMPARE/msquic/secnetperf.sh" "-bind:127.0.0.1:$PORT" > "$RAW/$name.server.out" 2>&1 &
      server=$!
      ;;
    quinn)
      "$COMPARE/quinn/bin/quinn-perf" server --listen "127.0.0.1:$PORT" > "$RAW/$name.server.out" 2>&1 &
      server=$!
      ;;
    hyperquic)
      "$BIN/hyper-quic-geo_open_loop" --one-way-ms 0 --rate 200 --requests 4000 > "$RAW/$name.rr100.out" 2>&1 || true
      if [ "$cond" = quiet ]; then
        "$BIN/hyper-quic-geo_open_loop" --one-way-ms 500 --rate 200 --requests 4000 > "$RAW/$name.relay500.out" 2>&1 || true
      fi
      return 0
      ;;
  esac
  # Listening once its UDP port is bound: at most 300 looks a tenth of a second apart.
  for ((i = 0; i < 300; i++)); do
    ss -Huln "sport = :$PORT" | grep -q . && break
    sleep 0.1
  done
  for label in 64KiB 8MiB 64MiB rr100; do
    case $arm:$label in
      secnetperf:rr100) set -- -upload:100 -download:100 ;;
      secnetperf:64KiB) set -- -upload:0 -download:65536 ;;
      secnetperf:8MiB) set -- -upload:0 -download:8388608 ;;
      secnetperf:64MiB) set -- -upload:0 -download:67108864 ;;
      quinn:rr100) set -- --upload-size 100 --download-size 100 ;;
      quinn:64KiB) set -- --upload-size 0 --download-size 65536 ;;
      quinn:8MiB) set -- --upload-size 0 --download-size 8M ;;
      quinn:64MiB) set -- --upload-size 0 --download-size 64M ;;
    esac
    if [ "$arm" = secnetperf ]; then
      timeout -s INT -k 30 60 "$COMPARE/msquic/secnetperf.sh" -target:127.0.0.1 "-port:$PORT" -exec:lowlat \
        -rstream:1 -runtime:5s -platency:1 -ptput:1 "$@" > "$RAW/$name.$label.out" 2>&1 || true
    else
      timeout -s INT -k 30 60 "$COMPARE/quinn/bin/quinn-perf" client --ip 127.0.0.1 --duration 5 "$@" \
        "localhost:$PORT" > "$RAW/$name.$label.out" 2>&1 || true
    fi
  done
  kill -TERM "$server" 2> /dev/null || true
  wait "$server" || true
}

# Runs planned case SEQ: its busy loops, its scope, its status.
run_case() {
  local line seq kind cond arm round matrix note name cpus busy memory deadline began rc reason
  line=$(awk -F'\t' -v s="$1" '$1 == s' "$OUT/plan.tsv")
  IFS=$'\t' read -r seq kind cond arm round matrix note <<< "$line"
  case $kind in
    s3) name=box_${cond}_${arm}_r${round} ;;
    quic) name=box_${cond}_quic-${arm}_r${round} ;;
    excluded) name=box_${cond}_${arm}_r${round} ;;
    *) echo "no case $1" >&2; return 1 ;;
  esac
  [[ $name =~ ^box_[a-z0-9]+_[A-Za-z0-9-]+_r[0-9]+$ ]] || { echo "a case's name: $name" >&2; return 1; }
  [ -e "$RAW/$name.status" ] && return 0
  if [ "$kind" = excluded ]; then
    echo "excluded 0 $note" > "$RAW/$name.status"
    return 0
  fi
  cpus=$(cpus_of "$cond")
  busy=$(busy_of "$cond")
  memory=$S3_MEMORY
  [ "$kind" = quic ] && memory=$QUIC_MEMORY
  [ "$cond" = memfloor ] && memory=$FLOOR
  deadline=$QUIC_DEADLINE
  if [ "$kind" = s3 ]; then
    case $matrix in full) deadline=$FULL_DEADLINE ;; mid) deadline=$MID_DEADLINE ;; *) deadline=$LEAN_DEADLINE ;; esac
  fi
  local free
  free=$(df -Pk /home | awk 'NR == 2 {print $4}')
  if [ "$kind" = s3 ] && [ "$free" -lt "$FREE_FLOOR_KIB" ]; then
    echo "failed 0 not started: /home had $((free / 1024 / 1024)) GiB free, under the 80 GiB a case needs" > "$RAW/$name.status"
    return 0
  fi
  echo "case $seq $name: cpus $cpus busy $busy MemoryMax $memory deadline ${deadline}s" > "$RAW/$name.host"
  local pids=() k
  for ((k = 0; k < busy; k++)); do
    taskset -c 8-15 bash -c 'while :; do :; done' &
    pids+=($!)
  done
  host start >> "$RAW/$name.host"
  began=$(date +%s)
  rc=0
  local wrap=(systemd-run --user --scope -q "--unit=doors-$name" -p "MemoryMax=$memory" -p MemorySwapMax=0
    python3 -I -c "$RUSAGE" "$RAW/$name.rusage" timeout -s INT -k 120 "$deadline" taskset -c "$cpus")
  case $arm in
    A1 | A2 | C)
      # shellcheck disable=SC2046,SC2086
      "${wrap[@]}" "$BIN/mantle-$arm" bench doors "$WORK/$name" --condition "$cond" --perf \
        $(matrix_args "$matrix") $LOAD_ARGS > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
    garage | seaweedfs | rustfs)
      "${wrap[@]}" bash "$0" endpoint-case "$name" "$cond" "$arm" "$matrix" > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
    *)
      "${wrap[@]}" bash "$0" quic-case "$name" "$cond" "$arm" > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
  esac
  systemctl --user stop "doors-$name.scope" 2> /dev/null || true
  host end >> "$RAW/$name.host"
  for k in "${pids[@]}"; do kill "$k" 2> /dev/null || true; done
  wait "${pids[@]}" 2> /dev/null || true
  rm -rf /home/adalundhe/.cache/claude-scratch/doors/work/"${name:?}"
  if [ "$rc" -eq 0 ]; then
    echo "completed $(($(date +%s) - began))" > "$RAW/$name.status"
  else
    case $rc in
      124 | 137) reason="the ${deadline}s deadline passed" ;;
      *) reason=$(grep -v '^\s*$' "$RAW/$name.err" | tail -1 | cut -c1-300) ;;
    esac
    echo "failed $(($(date +%s) - began)) exit $rc: ${reason:-no message}" > "$RAW/$name.status"
  fi
}

case ${1:-} in
  plan) plan ;;
  run)
    [ -e "$OUT/plan.tsv" ] || plan
    for seq in $(awk -F'\t' 'NR > 1 {print $1}' "$OUT/plan.tsv"); do
      run_case "$seq"
      echo "$(date -u +%FT%TZ) $seq" >> "$OUT/progress"
    done
    python3 -I "$ROOT/scripts/doors-tables.py" "$OUT"
    ;;
  case) run_case "$2" ;;
  endpoint-case) shift; endpoint_case "$@" ;;
  quic-case) shift; quic_case "$@" ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
