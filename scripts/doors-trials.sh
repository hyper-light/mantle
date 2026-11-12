#!/usr/bin/env bash
# The trial runner for `mantle bench doors` (docs/design/measurement.md §12; research/70) on the
# box, the Mac and the Mac's Linux VM lane: every case a fresh process tree, its arms in a Williams
# order in whole cycles, each case's rusage and the host's load kept, and every expected case
# accounted for as completed, failed with its reason, or excluded with its reason.
#
#   OUT=DIR MACHINE=M scripts/doors-trials.sh plan       write DIR/plan.tsv, the batch's cases in order
#   OUT=DIR MACHINE=M scripts/doors-trials.sh run        run each planned case not yet run
#   OUT=DIR MACHINE=M scripts/doors-trials.sh case SEQ   run the planned case SEQ alone
#   python3 scripts/doors-tables.py DIR                  regenerate the tables and the trial report
#
# MACHINE is box (Linux x86_64: CPUSET 8-15, each case in a systemd memory scope), mac (macOS
# arm64: no CPU binding and no memory limit, so cpu1, cpu2 and memfloor are excluded) or vm (the
# Mac's Docker Desktop Linux VM: each case in a container of the image VM_IMAGE, its CPUs and memory
# the container's). A batch is one machine's; DIR/plan.tsv names the machine on every row.
#
# DIR/bin holds the arms: mantle-A1 and mantle-A2 (one timed release build, copied twice, the A/A),
# mantle-C (the `count-allocs` build of the same commit), hyper-quic-geo_open_loop, and COMMIT. The
# comparators are COMPARE's (its COMPARATORS.md), each machine's own builds: S3 servers through the
# HTTP door with mantle-A1 as the harness, secnetperf and quinn-perf beside bare hyper-quic for
# QUIC. Raw outputs go to DIR/raw, named MACHINE_CONDITION_ARM_rROUND (a tail case adds -tail to
# its arm); each case's data goes under the machine's WORK and is removed after the case.
#
# Environment: CYCLES (Williams cycles, 1), FULL_CONDITIONS (every size and the set),
# MID_CONDITIONS (64 KiB, 8 MiB and the set; the rest LEAN), TAIL_CONDITIONS (the conditions that
# also run the tail case: TAIL_ARGS, sizes at the 3,688 operations a p99.9 needs, measurement.md
# §5a), VOLUME (a founder's volume, passed as --volume when set), and the matrices below.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:?OUT=DIR: the batch directory, holding bin/}
MACHINE=${MACHINE:-box}
BIN=$OUT/bin
RAW=$OUT/raw
CYCLES=${CYCLES:-1}

# Each case's data: a literal root a machine, so no removal ever names a directory a variable left
# empty. The Mac's paths are its home's.
case $MACHINE in
  box)
    WORK=/home/adalundhe/.cache/claude-scratch/doors/work
    COMPARE=${COMPARE:-/home/adalundhe/Projects/mantle-compare}
    ;;
  mac | vm)
    WORK=/Users/adalundhe/.cache/claude-scratch/doors/work
    COMPARE=${COMPARE:-/Users/adalundhe/mantle-compare}
    ;;
  *) echo "MACHINE is box, mac or vm, not $MACHINE" >&2; exit 2 ;;
esac
VM_IMAGE=${VM_IMAGE:-mantle-bench:1.98.1}

CONDITIONS=(quiet busy1x busy2x cpu1 cpu2 memfloor)
FULL_CONDITIONS=${FULL_CONDITIONS:-"quiet"}
MID_CONDITIONS=${MID_CONDITIONS:-"busy2x"}
TAIL_CONDITIONS=${TAIL_CONDITIONS:-"quiet busy2x"}
MANTLE_ARMS=(A1 A2 C)
# The S3 comparators each machine has builds of (COMPARATORS.md); MinIO only where its full-sync
# mode, a whole-machine sync(2) a PUT, flushes no one else's work (owner's decision, 2026-11-04).
case $MACHINE in
  box) S3_COMPARATORS=${S3_COMPARATORS:-"garage seaweedfs rustfs"} ;;
  *) S3_COMPARATORS=${S3_COMPARATORS:-"minio garage seaweedfs rustfs"} ;;
esac
QUIC_ARMS=(secnetperf quinn hyperquic)
# Why the native door's 500 ms relay is not run, until the harness carries one (the baseline's
# "Needs a decision", 4).
DEFAULT_RELAY_EXCLUDED="the 500 ms one-way relay with loss and reordering between client and nodes is not in the harness: geo_open_loop's relay is an example's threads, not a library the cell's clients can be pointed through"
RELAY_EXCLUDED=${RELAY_EXCLUDED:-$DEFAULT_RELAY_EXCLUDED}

# The node's memory floor, summed over the cell's three nodes: each node's `memory.floor.*` basis
# lines as `mantle serve` of the batch's product says them on the machine, read from a founder of a
# cell made as `mantle bench doors` makes one (DIR/floor-probe-serve.out). On the box at 4711a46a:
# objects 83513198, volumes 147172608, engines 822800, ranges 267841530, log 225853540, transport
# 69725483, threads 27262976, 822192135 bytes a node (crates/node/src/derive.rs `floor`,
# `say_floor`). FLOOR_NODE overrides it for another machine or product, read the same way.
FLOOR=$((3 * ${FLOOR_NODE:-822192135}))
# The memory every other case runs in: the cell's three nodes and the harness, or a comparator and
# the harness, well within the machine's memory; QUIC's two endpoints.
S3_MEMORY=24G
QUIC_MEMORY=4G

# The S3 matrices. A sized phase asks OPS operations at most (a p99's 368, measurement.md §5a) and
# STEP bytes; a set's phases ask every object of a door's half, 5,000 and 50,000. The 100,000 set is
# cut from the box's ten hours: its puts and deletes run at the doors' write capacity here, about 20
# a second through the native door, over two hours a mantle case (the dimensioning pass,
# 2026-11-04); FULL_ARGS and MID_ARGS take it back. The tail matrix asks 3,688 operations a sized
# phase, a p99.9's, at the small sizes step 10 judges tails at, with the bytes a step moves raised to
# hold them (3,688 of 128 KiB is 483,393,536 bytes, under 512 MiB).
FULL_ARGS=${FULL_ARGS:-"--objects 10000 --ops 368"}
MID_ARGS=${MID_ARGS:-"--sizes 65536,8M --objects 10000 --ops 368"}
LEAN_ARGS=${LEAN_ARGS:-"--sizes 65536,8M --objects 0 --ops 368"}
TAIL_ARGS=${TAIL_ARGS:-"--sizes 1,65536,128K --objects 0 --ops 3688 --bytes 512M"}
LOAD_ARGS=${LOAD_ARGS:-"--load open:500 --clients 16"}
# A case past its deadline is stopped (SIGINT, then SIGKILL) and reported failed.
FULL_DEADLINE=${FULL_DEADLINE:-3600}
MID_DEADLINE=${MID_DEADLINE:-2400}
LEAN_DEADLINE=${LEAN_DEADLINE:-900}
TAIL_DEADLINE=${TAIL_DEADLINE:-7200}
QUIC_DEADLINE=${QUIC_DEADLINE:-600}
PORT=39500
KEY=AKIAIOSFODNN7EXAMPLE
SECRET='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
# A case needs a cell's three volumes and its objects: never started with less free.
FREE_FLOOR_KIB=$((80 * 1024 * 1024))

# Why a condition is not run on a machine, or nothing where it is.
excluded_on() {
  case $MACHINE:$1 in
    mac:cpu1 | mac:cpu2)
      echo "macOS binds no thread to a CPU: THREAD_AFFINITY_POLICY is a hint to group threads on a cache, not a binding (xnu osfmk/kern/thread_policy.c), so one and two CPUs run on the VM lane and the box" ;;
    mac:memfloor)
      echo "macOS has no cgroups: no kernel limit on a process tree's memory as Linux's memory.max is, so the memory floor runs on the VM lane and the box" ;;
    *) ;;
  esac
}

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

# The plan: each cycle gives every condition one Williams row of the S3 arms (mantle's and the
# comparators') and one of QUIC's, and under TAIL_CONDITIONS one more row of the S3 arms on the tail
# matrix; the conditions' order turns a place each cycle, so over the cycles each condition meets
# different rows. Each row is accounted: run, or excluded with its reason.
plan() {
  mkdir -p "$OUT" "$RAW"
  local s3_arms=("${MANTLE_ARMS[@]}") s3_rows quic_rows seq=0 cycle c i cond row a matrix why k
  # shellcheck disable=SC2206
  s3_arms+=($S3_COMPARATORS)
  mapfile -t s3_rows < <(williams ${#s3_arms[@]})
  mapfile -t quic_rows < <(williams ${#QUIC_ARMS[@]})
  # The row a condition takes in a cycle, a tail case the row after it.
  row_of() { local rows=("${!1}"); echo "${rows[$(((i + ${2:-0} + (cycle - 1) * ${#CONDITIONS[@]}) % ${#rows[@]}))]}"; }
  {
    printf 'seq\tmachine\tkind\tcondition\tarm\tround\tmatrix\tnote\n'
    for ((cycle = 1; cycle <= CYCLES; cycle++)); do
      for ((i = 0; i < ${#CONDITIONS[@]}; i++)); do
        c=$(((i + cycle - 1) % ${#CONDITIONS[@]}))
        cond=${CONDITIONS[$c]}
        why=$(excluded_on "$cond")
        matrix=lean
        [[ " $MID_CONDITIONS " == *" $cond "* ]] && matrix=mid
        [[ " $FULL_CONDITIONS " == *" $cond "* ]] && matrix=full
        local kinds=(s3)
        [[ " $TAIL_CONDITIONS " == *" $cond "* ]] && kinds+=(tail)
        for k in "${kinds[@]}"; do
          local m=$matrix
          [ "$k" = tail ] && m=tail
          for a in $(row_of s3_rows[@] "$([ "$k" = tail ] && echo 1 || echo 0)"); do
            seq=$((seq + 1))
            if [ -n "$why" ]; then
              printf '%s\t%s\texcluded\t%s\t%s\t%s\t%s\t%s\n' "$seq" "$MACHINE" "$cond" "${s3_arms[$a]}$([ "$k" = tail ] && echo -tail)" "$cycle" "$m" "$why"
            else
              printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t-\n' "$seq" "$MACHINE" "$k" "$cond" "${s3_arms[$a]}" "$cycle" "$m"
            fi
          done
        done
        for a in $(row_of quic_rows[@]); do
          seq=$((seq + 1))
          if [ -n "$why" ]; then
            printf '%s\t%s\texcluded\t%s\tquic-%s\t%s\t-\t%s\n' "$seq" "$MACHINE" "$cond" "${QUIC_ARMS[$a]}" "$cycle" "$why"
          else
            printf '%s\t%s\tquic\t%s\t%s\t%s\t-\t-\n' "$seq" "$MACHINE" "$cond" "${QUIC_ARMS[$a]}" "$cycle"
          fi
        done
        if [ "$MACHINE" = box ]; then
          seq=$((seq + 1))
          printf '%s\t%s\texcluded\t%s\tminio\t%s\t-\t%s\n' "$seq" "$MACHINE" "$cond" "$cycle" \
            "MinIO runs on the Mac and its VM lane: its full-sync mode (MINIO_FS_OSYNC, a whole-machine sync(2) a PUT) flushes the shared box (owner's decision, 2026-11-04)"
        fi
        for a in "${MANTLE_ARMS[@]}"; do
          seq=$((seq + 1))
          printf '%s\t%s\texcluded\t%s\t%s-native-relay500\t%s\t-\t%s\n' "$seq" "$MACHINE" "$cond" "$a" "$cycle" \
            "$RELAY_EXCLUDED"
        done
      done
    done
  } > "$OUT/plan.tsv"
  echo "planned $seq cases on $MACHINE in $OUT/plan.tsv"
}

# The CPUs a case's processes take, and how many busy loops share them.
cpus_of() {
  case $MACHINE:$1 in
    box:cpu1) echo 8 ;; box:cpu2) echo 8-9 ;; box:*) echo 8-15 ;;
    vm:cpu1) echo 0 ;; vm:cpu2) echo 0-1 ;; vm:*) echo "0-$(($(vm_cpus) - 1))" ;;
    *) echo all ;;
  esac
}

busy_of() {
  local cores
  case $MACHINE in box) cores=8 ;; vm) cores=$(vm_cpus) ;; *) cores=$(sysctl -n hw.ncpu) ;; esac
  case $1 in busy1x) echo "$cores" ;; busy2x) echo $((2 * cores)) ;; *) echo 0 ;; esac
}

# The VM lane's CPUs, as Docker Desktop's VM has them.
vm_cpus() { docker info --format '{{.NCPU}}'; }

# The host as the case starts or ends: time, load averages, memory free, pressure where the OS
# says it.
host() {
  case $MACHINE in
    mac)
      echo "$1 epoch $(date +%s) loadavg $(sysctl -n vm.loadavg | tr -d '{}' | awk '{print $1, $2, $3}') pages_free $(vm_stat | awk '/Pages free/ {gsub("\\.", "", $3); print $3}') swapusage $(sysctl -n vm.swapusage | awk '{print $6}')"
      ;;
    *)
      local avail swap io cpu mem
      avail=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
      swap=$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)
      io=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/io 2> /dev/null || echo -)
      cpu=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/cpu 2> /dev/null || echo -)
      mem=$(awk '/^some/ {sub("avg10=", "", $2); print $2}' /proc/pressure/memory 2> /dev/null || echo -)
      echo "$1 epoch $(date +%s) loadavg $(cut -d' ' -f1-3 /proc/loadavg) memavailable_kib $avail swapfree_kib $swap psi_io_some10 $io psi_cpu_some10 $cpu psi_mem_some10 $mem"
      ;;
  esac
}

# Runs a command and writes the rusage of its process tree (RUSAGE_CHILDREN: every descendant
# waited for) to FILE as JSON; exits with the command's status. ru_maxrss is KiB on Linux and bytes
# on macOS; the file says which.
RUSAGE='import json, resource, subprocess, sys, time
began = time.monotonic()
rc = subprocess.call(sys.argv[2:])
r = resource.getrusage(resource.RUSAGE_CHILDREN)
with open(sys.argv[1], "w") as f:
    json.dump({"rc": rc, "wall_s": round(time.monotonic() - began, 3), "user_s": r.ru_utime, "system_s": r.ru_stime,
               "maxrss": r.ru_maxrss, "maxrss_unit": "B" if sys.platform == "darwin" else "KiB",
               "minflt": r.ru_minflt, "majflt": r.ru_majflt, "nvcsw": r.ru_nvcsw,
               "nivcsw": r.ru_nivcsw, "inblock": r.ru_inblock, "oublock": r.ru_oublock}, f)
    f.write("\n")
sys.exit(rc if rc >= 0 else 128 - rc)'

# A command stopped at its deadline: timeout(1) where the OS has it, Perl's alarm on macOS, which
# has none (SIGALRM ends it, exit 142).
deadline() {
  local seconds=$1
  shift
  if [ "$MACHINE" = mac ]; then
    perl -e 'alarm shift; exec @ARGV or exit 127' "$seconds" "$@"
  else
    timeout -s INT -k 120 "$seconds" "$@"
  fi
}

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
    rustfs | minio) pid=$server ;;
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
    --key "$KEY" --secret "$SECRET" --region us-east-1 --server-pid "$pid" --condition "$cond" \
    $(perf_arg) $(matrix_args "$matrix") $LOAD_ARGS || rc=$?
  kill -TERM "$server" 2> /dev/null || true
  wait "$server" || true
  return "$rc"
}

matrix_args() {
  local volume=
  [ -n "${VOLUME:-}" ] && volume="--volume $VOLUME"
  case $1 in
    full) echo "$FULL_ARGS $volume" ;; mid) echo "$MID_ARGS $volume" ;;
    tail) echo "$TAIL_ARGS $volume" ;; *) echo "$LEAN_ARGS $volume" ;;
  esac
}

# Instructions and cycles by `perf stat` where the machine has it (Linux); none on macOS.
perf_arg() {
  if [ "$MACHINE" != mac ] && command -v perf > /dev/null; then echo --perf; fi
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
    if [ "$MACHINE" = mac ]; then
      lsof -nP -iUDP:"$PORT" > /dev/null 2>&1 && break
    else
      ss -Huln "sport = :$PORT" | grep -q . && break
    fi
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
      deadline 60 "$COMPARE/msquic/secnetperf.sh" -target:127.0.0.1 "-port:$PORT" -exec:lowlat \
        -rstream:1 -runtime:5s -platency:1 -ptput:1 "$@" > "$RAW/$name.$label.out" 2>&1 || true
    else
      deadline 60 "$COMPARE/quinn/bin/quinn-perf" client --ip 127.0.0.1 --duration 5 "$@" \
        "localhost:$PORT" > "$RAW/$name.$label.out" 2>&1 || true
    fi
  done
  kill -TERM "$server" 2> /dev/null || true
  wait "$server" || true
}

# A case's command, pinned and limited as the machine does it: the box's systemd scope and taskset;
# the VM lane's container (the batch directory mounted where it is, the image's Linux running this
# script inside); the Mac's bare process.
wrapped() {
  local name=$1 cpus=$2 memory=$3 seconds=$4
  shift 4
  case $MACHINE in
    box)
      systemd-run --user --scope -q "--unit=doors-$name" -p "MemoryMax=$memory" -p MemorySwapMax=0 \
        python3 -I -c "$RUSAGE" "$RAW/$name.rusage" timeout -s INT -k 120 "$seconds" taskset -c "$cpus" "$@"
      ;;
    vm)
      docker run --rm --name "doors-$name" --network host --cpuset-cpus "$cpus" \
        --memory "$memory" --memory-swap "$memory" -v "$OUT:$OUT" -v "$WORK:$WORK" -v "$ROOT:$ROOT:ro" \
        -v "$COMPARE:$COMPARE:ro" -e MACHINE=vm -e OUT="$OUT" -e COMPARE="$COMPARE" "$VM_IMAGE" \
        python3 -I -c "$RUSAGE" "$RAW/$name.rusage" timeout -s INT -k 120 "$seconds" "$@"
      ;;
    mac)
      python3 -I -c "$RUSAGE" "$RAW/$name.rusage" perl -e 'alarm shift; exec @ARGV or exit 127' "$seconds" "$@"
      ;;
  esac
}

# Busy loops on a case's CPUs, one a process; their pids on stdout. On the VM lane they run in a
# container of their own on the case's CPUs.
start_busy() {
  local n=$1 cpus=$2 name=$3 k
  [ "$n" -gt 0 ] || return 0
  case $MACHINE in
    box)
      for ((k = 0; k < n; k++)); do
        taskset -c "$cpus" bash -c 'while :; do :; done' > /dev/null 2>&1 &
        echo $!
      done
      ;;
    vm)
      docker run -d --rm --name "doors-busy-$name" --cpuset-cpus "$cpus" "$VM_IMAGE" \
        bash -c "for k in \$(seq $n); do (while :; do :; done) & done; wait" > /dev/null
      ;;
    mac)
      for ((k = 0; k < n; k++)); do
        bash -c 'while :; do :; done' > /dev/null 2>&1 &
        echo $!
      done
      ;;
  esac
}

stop_busy() {
  local name=$1
  shift
  [ "$MACHINE" = vm ] && docker rm -f "doors-busy-$name" > /dev/null 2>&1 || true
  local k
  for k in "$@"; do kill "$k" 2> /dev/null || true; done
  wait "$@" 2> /dev/null || true
}

# The free space the case's data would take, in KiB.
free_kib() { df -Pk "$WORK" | awk 'NR == 2 {print $4}'; }

# Runs planned case SEQ: its busy loops, its scope, its status.
run_case() {
  local line seq machine kind cond arm round matrix note name cpus busy memory seconds began rc reason
  line=$(awk -F'\t' -v s="$1" '$1 == s' "$OUT/plan.tsv")
  IFS=$'\t' read -r seq machine kind cond arm round matrix note <<< "$line"
  [ "$machine" = "$MACHINE" ] || { echo "case $1 is $machine's, not $MACHINE's" >&2; return 1; }
  case $kind in
    s3 | excluded) name=${machine}_${cond}_${arm}_r${round} ;;
    tail) name=${machine}_${cond}_${arm}-tail_r${round} ;;
    quic) name=${machine}_${cond}_quic-${arm}_r${round} ;;
    *) echo "no case $1" >&2; return 1 ;;
  esac
  [[ $name =~ ^(box|mac|vm)_[a-z0-9]+_[A-Za-z0-9-]+_r[0-9]+$ ]] || { echo "a case's name: $name" >&2; return 1; }
  [ -e "$RAW/$name.status" ] && return 0
  if [ "$kind" = excluded ]; then
    echo "excluded 0 $note" > "$RAW/$name.status"
    return 0
  fi
  mkdir -p "$WORK"
  cpus=$(cpus_of "$cond")
  busy=$(busy_of "$cond")
  memory=$S3_MEMORY
  [ "$kind" = quic ] && memory=$QUIC_MEMORY
  [ "$cond" = memfloor ] && memory=$FLOOR
  seconds=$QUIC_DEADLINE
  case $kind:$matrix in
    tail:*) seconds=$TAIL_DEADLINE ;; s3:full) seconds=$FULL_DEADLINE ;;
    s3:mid) seconds=$MID_DEADLINE ;; s3:*) seconds=$LEAN_DEADLINE ;;
  esac
  local free
  free=$(free_kib)
  if [ "$kind" != quic ] && [ "$free" -lt "$FREE_FLOOR_KIB" ]; then
    echo "failed 0 not started: $WORK had $((free / 1024 / 1024)) GiB free, under the 80 GiB a case needs" > "$RAW/$name.status"
    return 0
  fi
  echo "case $seq $name: machine $MACHINE cpus $cpus busy $busy memory $memory deadline ${seconds}s" > "$RAW/$name.host"
  local pids
  mapfile -t pids < <(start_busy "$busy" "$cpus" "$name")
  host start >> "$RAW/$name.host"
  began=$(date +%s)
  rc=0
  case $kind:$arm in
    s3:A1 | s3:A2 | s3:C | tail:A1 | tail:A2 | tail:C)
      # shellcheck disable=SC2046,SC2086
      wrapped "$name" "$cpus" "$memory" "$seconds" "$BIN/mantle-$arm" bench doors "$WORK/$name" \
        --condition "$cond" $(perf_arg) $(matrix_args "$matrix") $LOAD_ARGS > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
    s3:* | tail:*)
      wrapped "$name" "$cpus" "$memory" "$seconds" bash "$0" endpoint-case "$name" "$cond" "$arm" "$matrix" \
        > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
    *)
      wrapped "$name" "$cpus" "$memory" "$seconds" bash "$0" quic-case "$name" "$cond" "$arm" \
        > "$RAW/$name.out" 2> "$RAW/$name.err" || rc=$?
      ;;
  esac
  [ "$MACHINE" = box ] && { systemctl --user stop "doors-$name.scope" 2> /dev/null || true; }
  host end >> "$RAW/$name.host"
  stop_busy "$name" "${pids[@]}"
  remove_work "$name"
  if [ "$rc" -eq 0 ]; then
    echo "completed $(($(date +%s) - began))" > "$RAW/$name.status"
  else
    case $rc in
      124 | 137 | 142) reason="the ${seconds}s deadline passed" ;;
      *) reason=$(grep -v '^\s*$' "$RAW/$name.err" | tail -1 | cut -c1-300) ;;
    esac
    echo "failed $(($(date +%s) - began)) exit $rc: ${reason:-no message}" > "$RAW/$name.status"
  fi
}

# A case's data removed: under its machine's literal root alone.
remove_work() {
  case $MACHINE in
    box) rm -rf /home/adalundhe/.cache/claude-scratch/doors/work/"${1:?}" ;;
    *) rm -rf /Users/adalundhe/.cache/claude-scratch/doors/work/"${1:?}" ;;
  esac
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
  *) sed -n '2,28p' "$0"; exit 2 ;;
esac
