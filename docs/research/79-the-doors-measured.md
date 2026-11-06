# 79. The doors measured: what each operation of the native and S3 doors costs, and how it is counted

Status: research note, 2026-11-04. For `mantle bench doors` (crates/mantle/src/bench_doors.rs) and
measurement.md §12. It states how both of mantle's doors, the native protocol over QUIC
(gateway.md §2, research/73) and the S3 HTTP/1.1 listener (node.md §4, research/70, research/72),
are measured before any of their costs is cut: the operations and sizes, the measures, the
instruments, the load, the trials, the conditions, the machines and the comparators. Each number
below is cited or measured; the parameters a run takes have their rows in docs/design/constants.md.

## The question

Steps 1–3 of the owner's goals cut the engine's and the replication's costs by counting them per
operation, attributing each to a site with tools and removing the unneeded work at its cause
[1][2][3]. The doors this goal optimizes had no such measurement: `mantle bench http` and
`mantle bench s3` time the codec and the listener alone against a stub host, and
`mantle bench transfer` checks exactness and throughput through a cell but counts no allocation,
fault or instruction per operation (research/76). The question is what to measure on each door so
that a cost is counted before it is cut, its cut is shown by the same count, and no correctness is
traded for it.

## Sources

[1] hyper-raft CLAUDE.md §1a, the law for the shared crates: "Allocations, reallocations and page
faults are measured and driven down on every hot path." "Counts come from a counting allocator and
the OS (`getrusage` minor and major faults, and" the Mach VM layer's on macOS). "A crate replaces a
project's implementation only where it is at least as fast and allocates no" more.

[2] crates/engine/benches/ranges.rs (lines 154–340): "The allocation count is a diagnostic:
compiled in only with the `count-allocs` feature"; its phase lines `elapsed … allocs/op …
reallocs/op … bytes/op … faults/op`, `process_cpu_ns/op … user_ns/op … system_ns/op …
instructions/op … cycles/op … switches/op` and `latency_us p50 … p99 … p99.9 … p99.99 … max`.

[3] hyper-raft docs/benchmarks.md, "What is measured, and how": "One measurement is two fresh
processes of the same seed, the cores interleaved run by run: a timed run, whose allocator reads
one word per call and counts nothing; a counting run, whose counts are exact: a workload's
allocations are fixed by its seed." Page faults "minor and major from `getrusage(RUSAGE_SELF)`, and
every fault the Mach VM layer" counts on macOS.

[4] vendor/hyper-measure: `alloc.rs` (a global allocator that counts allocations, reallocations,
those that moved, frees, bytes asked, live and peak, per thread or across the process, and sets the
caller's own work aside); `faults.rs` (`getrusage` on Linux and macOS, Mach `task_info` on macOS,
`GetProcessMemoryInfo`'s `PageFaultCount` on Windows); `usage.rs` (a process's CPU time,
instructions, cycles and footprint, "a process other than this one is read by its pid").

[5] getrusage(2), Linux man-pages: `ru_minflt` "The number of page faults serviced without any I/O
activity"; `ru_majflt` "The number of page faults serviced that required I/O activity."
perf_event_open(2): "PERF_COUNT_SW_PAGE_FAULTS_MIN This counts the number of minor page faults."

[6] Schroeder, Wierman, Harchol-Balter, "Open Versus Closed: A Cautionary Tale", NSDI 2006: "for a
fixed load, the mean response time for an open system model can exceed that for a closed system
model by an order of magnitude". wrk2's README (G. Tene): latency measured "from the time the
transmission *should* have occurred"; a generator that waits on responses ends up "coordinating
with the server". MinIO warp's README: "By default, 20 operations will run concurrently", so its
latencies are closed loop.

[7] measurement.md §5a: a round's transfers give a quantile its 95% order-statistic interval (Le
Boudec, Theorem 2.1): "6 samples for a median, 368 for a p99, 3,688 for a p99.9".

[8] measurement.md §10: a client is a record, not a thread; open loop with Poisson arrivals and
latency from each request's intended start; the generator reports its own CPU and lateness;
benchmarks report resident memory over client count at two counts, the slope `S` per idle and per
active connection.

[9] The engine's trial runner (benchmark-results/mantle-splinterdb-20261013/inside7.sh): "each
round's order a Williams design, so each arm runs right after" each other arm equally often, each
case's `minflt majflt nvcsw nivcsw rc` kept. Mytkowicz, Diwan, Hauswirth, Sweeney, "Producing Wrong
Data Without Doing Anything Obviously Wrong!", ASPLOS 2009: "measurement bias is significant and
commonplace in computer system evaluation." The owner's rule from the engine's A/As: an effect under
about 5% needs an A/A beside it.

[10] Amazon S3 User Guide and API Reference: "With a single PUT operation, you can upload a single
object up to 5 GB in size" (Uploading objects); parts "5 MiB to 5 GiB", at most 10,000 (multipart
upload limits, research/05 §4.1); ListObjectsV2 "returns up to 1,000 key names"; "The CRC64NVME
checksum algorithm is the default" (Checking object integrity). AWS CLI s3-config:
`multipart_threshold` and `multipart_chunksize` default `8MB` (research/73; bench_s3.rs's
`OBJECT`).

[11] Amazon S3 User Guide, optimizing performance: "consistent small object latencies (and
first-byte-out latencies for larger objects) of roughly 100–200 milliseconds"; "at least 3,500
PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix". S3
Express One Zone: "consistent, single-digit millisecond data access".

[12] draft-banks-quic-performance-00: "Standardizing the performance protocol allows for easy
comparisons across different QUIC implementations"; msquic's secnetperf implements it; quinn ships
`perf`. vendor/hyper-quic/examples/geo_open_loop.rs: overhead "is a latency less the round trip the
relay imposes", the relay holding each datagram 500 ms one way.

[13] The S3 servers' state (GitHub API, 2026-11-03): MinIO's repository is archived ("THIS
REPOSITORY IS NO LONGER MAINTAINED"; last release RELEASE.2025-10-15T17-29-55Z, AGPL-3.0); Garage
v2.5.0 (AGPL-3.0, 2026-10-24), SeaweedFS 4.52 (Apache-2.0, 2026-11-02) and RustFS 1.0.4 (Apache-2.0,
2026-10-25) are maintained. Garage's configuration reference: `data_fsync` "is disabled (`false`) by
default", and `metadata_fsync` likewise, so a same-host comparison sets each comparator to flush
before it acknowledges, as mantle does (CLAUDE.md §6).

[14] layout.rs: an object is sealed in 64 KiB segments (`seal::SEGMENT`), each stored with its tag;
65,535, 65,536 and 65,537 bytes are one short of a segment, a segment, and one past.

## Decisions

**Operations: everything a client asks of an object, through both doors.** PUT whole and streamed
(the native upload's runs; aws-chunked on HTTP), GET whole and ranged, HEAD, DELETE, a
ListObjectsV2 page, and a multipart upload's create, upload part, complete, abort and list parts.
The workload is agentic (small objects and metadata beside very large streams), so neither door is
judged at one size.

**Sizes: S3's own lines and the layout's.** 0 B and 1 B; 65,535, 65,536 and 65,537 bytes, the
segment's boundaries [14]; 128 KiB, the small-object line; 8 MiB, the stock clients' multipart
threshold and part [10]; 64 MiB; 5 GiB, the single PUT's limit [10]; and a 10 GB stream. Sets of
1,000, 10,000 and 100,000 small objects, 100,000 the default with 10,000 beside it, as 100M and 10M
are for the engine. A listing page holds 1,000 keys and a completion 10,000 parts, S3's most [10].

**Measures, per operation.** Allocations, reallocations, reallocations that moved, bytes asked;
peak live heap per request and per connection; minor and major faults (Mach faults on macOS) [5];
user and system CPU; instructions and cycles where the OS counts them; context switches; wire bytes
and, on the native door, datagrams; and latency at p50, p99, p99.9, p99.99 and max from each
request's intended start [2][6]. Per connection, the memory slope between two connection counts and
the handshake's counts [8].

**Instruments: the counting allocator in a diagnostic build only.** `mantle serve` and the harness
built with `count-allocs` install hyper-measure's counting allocator; timed builds run on the
system allocator and count nothing [2][3]. The servers' faults, CPU and footprint are read by pid
[4]; `perf stat` counts instructions on the box; aws-lc's own heap, which the counting allocator
cannot see, shows in the footprint (node.md §1.4). The harness's own work is set aside from the
doors' counts.

**Load: open loop by default, a closed ladder beside it.** Poisson arrivals with latency from each
request's intended start, since a closed generator understates response time by up to an order of
magnitude and omits the server's slow periods [6]; a closed ladder of logical clients doubling
while throughput grows, up to the node's connection budget, for the throughput knee. An HTTP
connection carries one request at a time, as HTTP/1.1 answers in order (research/70), so
concurrency is connections. Logical clients are records on hyper-rt; the generator reports its CPU
and lateness [8].

**Rounds sized for the quantiles reported.** At least 3,688 transfers behind a p99.9, 368 behind a
p99 [7]; a quantile with fewer is shown as a dash.

**Trials: two fresh processes a point, in a Williams order.** Each point is a timed process and a
counting process of the same seed [3]; arms follow each other in a Williams order in whole cycles,
each case's resource use kept, an A/A beside any effect under about 5%, every expected case
accounted as completed, failed with its reason, or excluded with its reason [9].

**Conditions.** Quiet; busy loops at one and two times the cores; one and two CPUs; the node's
memory floor as a cgroup limit; and, for the native door, the 500 ms one-way relay with loss and
reordering, its overhead above physics reported [12]. A busy host is the norm and never blamed.

**Machines.** The box (Linux x86_64, CPUSET 8-15, one die without SMT siblings, under a systemd
memory scope), the Mac's Linux VM lane (arm64), and the Mac (macOS arm64). Windows runs the
correctness suites; its performance belongs to the next goal.

**Comparators: same host, equal durability, pinned.** The native door against bare hyper-quic,
upstream quinn at hyper-quic's base revision through quinn's `perf`, msquic through secnetperf
[12], and the HTTP door on the same node. The HTTP door against Garage v2.5.0, SeaweedFS 4.52,
RustFS 1.0.4 and MinIO RELEASE.2025-10-15T17-29-55Z, each configured to flush before it
acknowledges [13], built and run in a comparison-only workspace outside the repository (the
owner's choice, 2026-11-03), each one's revision, license and durability setting recorded beside
its numbers. S3's published latencies and rates are reference bars, not same-host comparators
[11].

## Open

- Whether SeaweedFS and RustFS can be made to flush before acknowledging is a property of their
  source at the pinned revisions; where one cannot, its numbers are reported with what it does.
- The 5 GiB and 10 GB cases run where free space allows a cell of three to hold them; the run
  records where they ran.
