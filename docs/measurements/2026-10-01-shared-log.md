# mantle on hyper-log and hyper-block

**Question.** mantle's range replicas, `mantle bench log` and the chunk store moved from
`crates/log` and mantle-disk's block layer onto the shared crates hyper-raft took them into:
`hyper-log` (one owner thread that answers every call by ticket, L-2) and `hyper-block` (each
file, pool and buffer with one owner) (research/32 §5.2 L-1, L-2; vendor/UPSTREAM.md). Does the
range replay the same, do the threads and allocations hold, and what does the move cost?

**Machine.** macOS 26.4.1 on an Apple M5 Max (18 cores, 128 GiB), APFS on the internal SSD,
`F_FULLFSYNC`. Other sessions built and tested on the machine throughout: load averages were
25 to 43, given with each run. The device's durable write moved between 8.65 and 22–30 ms
from one run to the next of the same build (below).

**Builds.** Before: `ci-six` at `a2021df` (`crates/log`, mantle-disk). After: this change,
hyper-raft `50a711d` snapshots. Release builds for every timing, debug for the simulation.

## The simulation replays the same

`crates/range/tests/sim.rs` was run for seeds 1 to 48 on both builds with a temporary hook,
not kept, that appended each seed's `Ran` to a file: its counters and every gateway operation,
key, input, output, and the steps it was called and answered at:

```
MANTLE_SIM_SEEDS=48 MANTLE_SIM_RECORD=<file> cargo test -p mantle-range --test sim
```

The two files are byte-identical (MD5 `5583f0469593de8cd73c982d52789e49`): 2,880 operations
over 144,205 steps, with 111 members lost and replaced, 60 readies stalled for room, 7,916
readies left flushing past a step, 33 frames damaged at rest, 5 members marked and repaired,
10 rebuilt under a new identity and 5 devices replaced. Every invariant of the file's header
held in both. The simulation's devices are the log's own now, reached through
`Log::with_file` while a member runs and handed back by `Log::close` when it stops.

## A read of the log is a round trip

hyper-log answers `view`, `term` and `entries` from its owner thread, so each is a message and
a reply on the caller's port. Measured in release on a log of one group, 20,000 calls each:
`view` 32.7 µs, `term` 28.1 µs a call, at a load average of 34. `crates/log` answered under a
read lock.

The core asks for its group's bounds on most calls: the simulation's first seed asked the log
about 29,000 times, 25,400 of them `first_index` or `last_index`, and in its first four seeds
87–93% of the `term` calls were for the last entry. The replica's store now keeps the group's start, last entry and that
entry's term between its own writes, which alone move them, and asks the log while one is out,
as a write's records reach readers before its answer (`crates/range/src/store.rs`). The 48
seeds replay the same with it. The simulation of 48 seeds took 6.2 s before (load 25), 114 s
after the move without the kept bounds and 13.0 s with them (load 38–42).

## The replica's path, allocations and time

A group of three replicas on simulated devices in one process, driven to commit one
registration at a time until no message is left, 50 entries to warm and then 300 measured,
counting every allocation in the process with a counting global allocator (a temporary test,
not kept). Five runs of each build, alternated, load 38–40:

| | Before | After |
|---|---|---|
| allocations a committed entry | 262.2 | 127.2 |
| reallocations | 25.0 | 4.0 |
| bytes allocated | 282,345 | 145,367 |
| wall time a committed entry, median of 5 (range) | 159 µs (153–518) | 828 µs (612–984) |

The counts were the same in every run. The wall time is the price of the round trips that
remain: per committed entry the three members submit 6 writes, each answered after the owner
hands the frame to the log's device thread and hears back (four thread switches where
`crates/log` took two), ask the log for their bounds 6 times after those writes, for a term
other than the last 9 times, and fetch entries 3 times. On a device whose flush takes
milliseconds the switches are a small part; in a process that drives its replicas faster than
its device flushes, they are most of it.

## `mantle bench log`

```
mantle bench log <dir> --skip-device --sizes 128,16384 --replicas 1,256
```

Six runs of each build, alternated, load 37.8–43.6. Medians of the six:

| Point | Throughput before | after | p50 before | after | Threads before | after |
|---|---|---|---|---|---|---|
| 128 B, 1 replica | 13.8 kB/s | 14.6 kB/s | 8.78 ms | 8.65 ms | 3 | 4 |
| 128 B, 256 replicas | 2.65 MB/s | 3.01 MB/s | 9.70 ms | 10.7 ms | 20 | 21 |
| 16 KiB, 1 replica | 1.71 MB/s | 1.75 MB/s | 8.65 ms | 8.65 ms | 3 | 4 |
| 16 KiB, 256 replicas | 314 MB/s | 301 MB/s | 12.8 ms | 13.0 ms | 20 | 21 |

The process ran 1 thread idle before every point in both. A log runs two threads, its owner
and its device thread, where `crates/log` ran one writer, so every point runs one more, the
same at 1 and 256 replicas: no thread a replica (`crates/mantle/tests/clients.rs` now bounds
the point's threads at a driver a core and two). The p50 at 256 appends of 128 B was higher
after in five of six pairs, 10.5–10.7 ms against 9.70; the rest are within the runs' spread. Two full default
runs of each, alternated, moved by more than that from run to run of one build: the same point
at 256 replicas and 128 B ran 26.6K and 5.61K appends/s before, 9.71K and 17.3K after, as the
device's p50 moved from 8.65 to 22–30 ms.

## The chunk store's issuer

`cargo test -p mantle-chunk --test issuer -- --nocapture`, before once and after three times
(load 25 before, 35–36 after): 9 threads idle and 9 while writing, at most 32 writes between
flushes and at most 4 in flight at depth 4, in every run. The volume's tests share one
simulated device between the volume and the issuer's duplicate handle through a lock
(`crates/chunk/tests/common/device.rs`), since hyper-block's simulated file has one owner.
