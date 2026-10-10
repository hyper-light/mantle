# Engine comparison

Build `shard_db` with `cargo bench -p mantle-engine --bench shard_db --no-run` and
keep the executable that Cargo prints. Build RocksDB 11.8.1's `db_bench` separately.
The comparison does not build or change either engine. It copies the supplied executables
into a new results directory, hashes them, and records the declared source revisions,
dirty contents, commands, host state, all raw output, process resources and parsed tails.

```sh
python3 scripts/check-benchmark-workload.py --rocksdb-source /path/to/rocksdb
python3 -m unittest discover -s scripts/tests -p test_benchmark_comparison.py
python3 scripts/compare-engine-benchmarks.py \
  --mantle-binary mantle=/path/to/shard_db \
  --mantle-source mantle=/path/to/mantle-checkout \
  --rocksdb-binary /path/to/db_bench --rocksdb-source /path/to/rocksdb \
  --output /path/to/new-results-directory
```

For an engine change, copy the **same** `benches/shard_db.rs` and
`benches/support/rocks_workload.rs` into both checkouts, build and save both executables,
and use the same runner command. This keeps the harness outside the production change.
Two Mantle configurations can be compared without RocksDB:

```sh
python3 scripts/compare-engine-benchmarks.py --single-configuration \
  --mantle-binary before=/path/to/before-shard-db \
  --mantle-source before=/path/to/before-checkout \
  --mantle-binary after=/path/to/after-shard-db \
  --mantle-source after=/path/to/after-checkout \
  --output /path/to/new-before-after-results
```

Defaults reproduce the recorded workload: three rounds, 10 million puts, 1 million
gets and 1 million scans of 10 rows; 16-byte keys, 100-byte values, buffered I/O,
64 MiB per memtable, 256 MiB configured data cache, WAL and compression disabled.
Mantle's issued-write budget is separately 64 MiB, with requested issuer depth and
runs in flight both 16. RocksDB has one benchmark thread, two write buffers and two
background jobs. Round order reverses each time; each pair uses the same seed, starting
at 301 and advancing one per round. All valid slow rounds remain in the result.

`--rocks-seed=N` makes Mantle use RocksDB's `mt19937_64` streams, including per-phase
seed offsets and the database-selection draws in fill/read. Keys use the same big-endian
numeric prefix and ASCII `'0'` padding. The C++ oracle checks 9,000 operation/key outputs
against RocksDB's own `Random64`, across several state twists and seed boundaries. This
matches one database, one benchmark thread and batch size 1 in RocksDB 11.8.1; changing
those settings requires updating the oracle and stream schedule. Mantle's unmatched
post-fill scan probes are omitted in this mode. Values still differ: Mantle writes repeated
`v`, while RocksDB generates its values; compression is disabled.

`--bloom-bits=24` is an explicit experiment setting rounded from Mantle's measured
23.96–25.88 filter bits per input key in the October 8 three-round run. The CLI can
replace it. It enables RocksDB's Bloom filters; it does **not** establish equal filter
memory, because RocksDB sizes filters per SST entry and Mantle keeps filters across
branches. Mantle prints actual filter bytes and bits per input key on every run.

Cache settings do **not** establish equal total memory or CPU budgets. Mantle prints
its owner, issuer and actual device worker counts; RocksDB's driver and background jobs
are recorded in the command. The kernel's `wait4`/`getrusage` account measures whole-process
CPU time and peak RSS, including every process thread. The CSV samples approximate process-tree RSS
and thread counts every 250 ms; short peaks can be missed and denied OS access is labelled
unavailable. Mantle additionally reports
memtable/cache/index/filter memory and the OS physical footprint where supported.
Its latency vector holds one sample per put and remains allocated during reads.
The shared OS page cache is neither bounded by the configured data cache nor cleared.

Mantle also reads `hyper_measure::usage` at each phase's boundaries, outside the API
timers. It prints whole-process user/system/total CPU nanoseconds, instructions and cycles
per operation, with unsupported counters explicitly unavailable (`null` in parsed JSON).
The issuer and device workers are included; this is not isolated foreground CPU. It also
includes the phase's RNG/bookkeeping and any maintenance running concurrently. Maintenance
and landing are reported per input put, so fill and drain CPU can be summed with the same
denominator. No per-operation OS calls are added. Boundary account calls can themselves
add a small cost, and the kernel's supported counters differ by OS; RocksDB has no equivalent
phase accounts in this workflow. These accounts help separate CPU work from elapsed time,
but do not alone distinguish code work from a different compaction/tree/cache layout.

Ordinary Mantle latency samples time only `put`, `get` and `scan`; RNG and bookkeeping
remain outside them. RocksDB's histogram measures intervals between operation completions,
including intervening harness work, with microsecond buckets. Throughput includes each
driver's loop. `--attribute` is a separate experiment: its extra clocks, OS accounts and
slow-event recording alter performance and must not replace ordinary acceptance runs.

In `--maintenance=drain` mode, Mantle separately measures maintenance plus landing
issued writes before reads. RocksDB runs `waitforcompaction` before reads; stock 11.8.1
unconditionally sleeps **five seconds** before that call and does not print its elapsed
drain time. The runner labels that duration unavailable. Neither operation flushes the
active memtable. Never subtract the sleep and call the remaining process wall time a
drain measurement, or infer drained fill throughput from it. `--maintenance=deferred`
starts reads immediately and moves each engine's drain after scans. Background overlap,
iterator construction, value copies, total memory and CPU still differ; these runs measure
the stated configurations and do not alone isolate a production root cause.

Each `round-N-LABEL/` contains `raw.txt`, `resources.csv` and `run.json`.
`metadata.json` records binaries, sources and limitations; `results.json` retains every
completed run. A failing child keeps its raw output and resource/exit evidence and stops
the comparison. Temporary database directories are removed only by their owning run.
