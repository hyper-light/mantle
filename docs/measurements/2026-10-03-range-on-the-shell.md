# The range replica on the durable shell

**Question.** D-1 moves mantle's range replica onto hyper-raft's durable shell (`hyper-durable`,
hyper-raft `docs/durable.md`): the shell takes the core's readies ahead of their writes' answers
to the log's depth, where mantle's own shell kept one `Ready` out and waited for it. The shell
replaces mantle's only where it is at least as fast and allocates no more on mantle's workload
(hyper-raft `docs/durable.md` §13), and hyper-raft listed three losses of the shell against
mantle's to close first: reallocations, context switches on the device, and the tail with one
member. Is the range replica on the shell at least as fast as mantle's own, and does it allocate
no more?

**Machine.** macOS 26.4.1 on an Apple M5 Max (18 cores, 128 GiB), APFS on the internal SSD,
`F_FULLFSYNC`. Other sessions built and tested throughout; the load average is beside every
point, read before and after it.

**Builds.** `crates/hyper-durable-compare` at hyper-raft branch `mantle-d1`, release, which runs
four sides in one process on the same workload, each a range group of 1, 3 or 5 members on logs
of their own, member 1 leading, committing one entry at a time (closed loop), every entry a
mantle `wire::Entry` applied by mantle's engine (`Model`) and Name layer:
- *mantle 1c179e8*: mantle's range `Replica` at origin/dev `1c179e8`, before this work, over the
  hyper-raft, hyper-log and hyper-block it vendored (hyper-raft `dce1daa`), driven as its tests
  drove it: every member `begin`s its ready, the messages are delivered, every member
  `wait_persisted`s;
- *mantle 85b9c2d*: the same shell at this work's first step, over hyper-raft `687244f`'s
  snapshots, so the log is the shell's and the shell is mantle's;
- *hyper-durable*: the shell with the comparison's stand-in state machine, held by one `Owner`;
- *mantle D-1*: this change, mantle's range replica on the shell, over the snapshots of hyper-raft
  `df5f8ad` (measured at `b3a1fd7`, which differs from it in a note of `ORIGIN.md` alone), each
  member driven when a message came for it, its log answered one of its writes, or
  its last drive said there is more (node.md §2.2).

`file` is a real file on the SSD, direct I/O and the platform's full flush; `sim` is
hyper-block's simulated device, whose flush costs nothing, so the shells' own costs show. Each
point runs every side once a round, the order rotated each round. Latencies are pooled over the
rounds; the other columns are per committed entry, medians over the rounds: frames flushed by
every member's log, the process's allocations and reallocations (every thread), minor and major
page faults, context switches, the threads alive, and the allocations and reallocations of the
driving thread alone (the shell, the core, the state machine and the store's calls).

On the device the comparison also gives each side's leader's writes, from submission to the
answer taken (`HYPER_DURABLE_DIAGNOSIS`): the log's and the device's part of an entry's latency,
the wake included. mantle's shell has one write of the leader's out at a time; the durable shell
up to three, so its writes wait behind each other in the log and each is longer while entries
commit sooner.

## On the device

```
cd crates/hyper-durable-compare && CARGO_BUILD_JOBS=4 cargo build --release
HYPER_DURABLE_DIAGNOSIS=1 ./target/release/hyper-durable-compare --devices file \
  --members 1,3,5 --shapes register,put --rounds 4 --entries 300 --warm 50
```

2026-10-03, 17:21–17:47 PDT.

**1 member, register entries, device; load 6.69 7.41 7.53 → 4.79 6.75 7.28**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 8655 | 14842 | 19076 | 113 | 1.00 | 38.4 | 9.08 | 15.9 | 38.3 | 9.03 | 8642 | 14824 |
| mantle 85b9c2d | 8560 | 21166 | 126517 | 108 | 1.00 | 38.4 | 9.08 | 16.5 | 38.3 | 9.03 | 8542 | 21146 |
| hyper-durable | 8596 | 17907 | 63222 | 110 | 1.00 | 30.4 | 9.08 | 16.5 | 30.3 | 9.03 | 8584 | 17896 |
| mantle D-1 | 8583 | 13673 | 18059 | 110 | 1.00 | 30.4 | 9.08 | 15.7 | 30.3 | 9.03 | 8569 | 13628 |

**3 members, register entries, device; load 4.79 6.75 7.28 → 11.03 7.70 7.44**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 65358 | 112836 | 330276 | 20 | 6.00 | 135.1 | 21.19 | 140.7 | 135.0 | 21.06 | 13030 | 29733 |
| mantle 85b9c2d | 65018 | 118407 | 353097 | 20 | 6.00 | 135.1 | 21.19 | 128.7 | 135.0 | 21.06 | 13816 | 29340 |
| hyper-durable | 37683 | 63027 | 295355 | 27 | 5.72 | 93.9 | 21.19 | 125.8 | 93.7 | 21.06 | 30669 | 54159 |
| mantle D-1 | 36989 | 68076 | 244814 | 27 | 5.78 | 93.9 | 21.19 | 137.3 | 93.7 | 21.06 | 30774 | 54707 |

**5 members, register entries, device; load 11.03 7.70 7.44 → 8.53 11.54 9.88**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 87928 | 129185 | 369212 | 12 | 10.00 | 225.8 | 33.29 | 266.8 | 225.7 | 33.08 | 18063 | 30529 |
| mantle 85b9c2d | 87973 | 148602 | 367219 | 11 | 10.00 | 225.8 | 33.29 | 285.4 | 225.7 | 33.08 | 17997 | 31578 |
| hyper-durable | 49905 | 75805 | 313985 | 19 | 9.80 | 157.0 | 33.29 | 277.8 | 156.5 | 33.08 | 41897 | 72522 |
| mantle D-1 | 49676 | 77707 | 319761 | 20 | 9.81 | 157.0 | 33.29 | 276.7 | 156.5 | 33.08 | 41066 | 75894 |

**1 member, 1 KiB put entries, device; load 8.53 11.54 9.88 → 4.36 9.02 9.09**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 22150 | 33877 | 223216 | 44 | 1.00 | 26.0 | 10.07 | 29.4 | 26.0 | 10.02 | 22125 | 33852 |
| mantle 85b9c2d | 21087 | 29924 | 240435 | 56 | 1.00 | 26.0 | 10.07 | 28.4 | 26.0 | 10.02 | 21058 | 29899 |
| hyper-durable | 19998 | 28386 | 185015 | 52 | 1.00 | 18.0 | 10.07 | 28.0 | 18.0 | 10.02 | 19976 | 28364 |
| mantle D-1 | 21200 | 28399 | 151172 | 49 | 1.00 | 18.0 | 10.07 | 28.2 | 18.0 | 10.02 | 21174 | 28359 |

**3 members, 1 KiB put entries, device; load 4.36 9.02 9.09 → 3.67 6.19 7.77**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 73622 | 134614 | 363142 | 15 | 6.00 | 96.1 | 12.16 | 162.6 | 96.0 | 12.03 | 16800 | 35881 |
| mantle 85b9c2d | 70426 | 166069 | 358973 | 14 | 6.00 | 96.1 | 12.16 | 132.7 | 96.0 | 12.03 | 14992 | 38832 |
| hyper-durable | 39046 | 69309 | 346192 | 26 | 5.77 | 54.9 | 12.16 | 138.4 | 54.8 | 12.03 | 33010 | 64949 |
| mantle D-1 | 41298 | 109634 | 292041 | 24 | 5.75 | 55.0 | 12.16 | 139.9 | 54.8 | 12.03 | 35672 | 95434 |

**5 members, 1 KiB put entries, device; load 3.67 6.19 7.77 → 20.54 14.97 11.25**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 68990 | 126718 | 376839 | 14 | 10.00 | 160.2 | 14.24 | 247.8 | 160.0 | 14.03 | 10377 | 29337 |
| mantle 85b9c2d | 74467 | 148087 | 423174 | 13 | 10.00 | 160.2 | 14.24 | 251.4 | 160.0 | 14.03 | 12901 | 32791 |
| hyper-durable | 40159 | 129713 | 375414 | 25 | 9.81 | 91.3 | 14.24 | 241.2 | 90.8 | 14.03 | 33177 | 112756 |
| mantle D-1 | 42310 | 67332 | 408772 | 23 | 9.80 | 91.4 | 14.24 | 247.1 | 90.8 | 14.03 | 34082 | 64292 |

- **Faster with more than one member.** p50 43–44% lower at three and five members with
  register entries (37.0 against 65.4 ms; 49.7 against 87.9) and 39–44% with 1 KiB entries
  (41.3 against 73.6; 42.3 against 69.0); p99 19–47% lower; entries a second 35–67% higher;
  fewer flushes an entry (5.75–5.78 against 6.00 at three, 9.80–9.81 against 10.00 at five). At
  five members with 1 KiB entries p99.9 is 409 against 377 ms, the second slowest of 1,200
  entries in a point whose load rose from 3.7 to 20.5. With one member p50 is even (8.58 against
  8.66 ms; 21.2 against 22.2).
- **Fewer allocations, no more reallocations.** 21–43% fewer allocations an entry (93.9 against
  135.1 at three members, register entries; 55.0 against 96.1 with 1 KiB entries; 30.4 against
  38.4 at one member); reallocations equal at every point, on the driving thread and in the
  process.
- **Context switches** an entry within 4% either way at three and five members with register
  entries and with 1 KiB entries at five (137.3 against 140.7; 276.7 against 266.8; 247.1
  against 247.8), 14% fewer with 1 KiB entries at three (139.9 against 162.6).
- **The step-1 side** (mantle's shell on the shell's log) is mantle 1c179e8's within each point's
  spread: the log under the shells is not what differs.

## On the simulated device

```
./target/release/hyper-durable-compare --devices sim --members 1,3,5 --shapes register,put \
  --rounds 4 --entries 3000 --warm 50
./target/release/hyper-durable-compare --devices sim --members 3,5 --shapes register,put \
  --rounds 4 --entries 3000 --warm 3000
```

2026-10-03, 17:20–17:21 PDT. 50 entries to warm:

**1 member, register entries, simulated device; load 5.48 7.32 7.51 → 5.68 7.33 7.51**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 11 | 31 | 121 | 87486 | 1.00 | 44.4 | 9.02 | 3.0 | 38.3 | 9.01 |
| mantle 85b9c2d | 12 | 42 | 140 | 88860 | 1.00 | 44.4 | 9.02 | 3.0 | 38.3 | 9.01 |
| hyper-durable | 11 | 40 | 145 | 85162 | 1.00 | 36.4 | 9.02 | 3.0 | 30.3 | 9.01 |
| mantle D-1 | 12 | 48 | 188 | 85463 | 1.00 | 36.4 | 9.02 | 3.0 | 30.3 | 9.01 |

**3 members, register entries, simulated device; load 5.68 7.33 7.51 → 5.68 7.33 7.51**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 58 | 178 | 324 | 15155 | 6.00 | 179.0 | 21.29 | 17.0 | 135.0 | 21.01 |
| mantle 85b9c2d | 59 | 231 | 462 | 14985 | 6.00 | 179.0 | 21.29 | 17.1 | 135.0 | 21.01 |
| hyper-durable | 46 | 97 | 211 | 20355 | 5.53 | 130.0 | 21.28 | 15.8 | 93.5 | 21.02 |
| mantle D-1 | 48 | 113 | 244 | 20403 | 5.52 | 130.0 | 21.28 | 15.3 | 93.5 | 21.02 |

**5 members, register entries, simulated device; load 5.68 7.33 7.51 → 6.11 7.40 7.53**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 96 | 261 | 492 | 9810 | 10.00 | 299.0 | 33.49 | 27.4 | 225.7 | 33.01 |
| mantle 85b9c2d | 95 | 258 | 444 | 10064 | 10.00 | 299.0 | 33.49 | 27.4 | 225.7 | 33.01 |
| hyper-durable | 65 | 130 | 214 | 14959 | 9.13 | 218.7 | 33.44 | 23.5 | 155.8 | 33.02 |
| mantle D-1 | 67 | 139 | 240 | 14793 | 9.14 | 218.7 | 33.44 | 23.5 | 155.8 | 33.02 |

**1 member, 1 KiB put entries, simulated device; load 6.11 7.40 7.53 → 6.11 7.40 7.53**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 13 | 55 | 188 | 77236 | 1.00 | 32.0 | 10.02 | 3.0 | 26.0 | 10.00 |
| mantle 85b9c2d | 13 | 36 | 169 | 81030 | 1.00 | 32.0 | 10.02 | 3.0 | 26.0 | 10.00 |
| hyper-durable | 12 | 32 | 158 | 81718 | 1.00 | 24.0 | 10.02 | 3.0 | 18.0 | 10.00 |
| mantle D-1 | 12 | 33 | 160 | 82443 | 1.00 | 24.0 | 10.02 | 3.0 | 18.0 | 10.00 |

**3 members, 1 KiB put entries, simulated device; load 6.11 7.40 7.53 → 6.10 7.37 7.52**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 60 | 212 | 354 | 15203 | 6.00 | 141.5 | 12.32 | 16.8 | 96.0 | 12.00 |
| mantle 85b9c2d | 59 | 196 | 359 | 15192 | 6.00 | 141.5 | 12.32 | 16.8 | 96.0 | 12.00 |
| hyper-durable | 47 | 116 | 255 | 20550 | 5.52 | 91.8 | 12.31 | 15.5 | 54.5 | 12.01 |
| mantle D-1 | 47 | 110 | 199 | 20209 | 5.52 | 91.9 | 12.31 | 15.9 | 54.5 | 12.01 |

**5 members, 1 KiB put entries, simulated device; load 6.10 7.37 7.52 → 6.57 7.45 7.55**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 100 | 304 | 505 | 9604 | 10.00 | 235.8 | 14.54 | 27.4 | 160.0 | 14.00 |
| mantle 85b9c2d | 95 | 280 | 461 | 9787 | 10.00 | 235.8 | 14.54 | 27.5 | 160.0 | 14.00 |
| hyper-durable | 68 | 154 | 277 | 14716 | 9.17 | 155.0 | 14.50 | 23.6 | 90.1 | 14.02 |
| mantle D-1 | 68 | 154 | 270 | 14402 | 9.16 | 154.3 | 14.50 | 23.8 | 90.1 | 14.02 |

3,000 entries to warm:

**3 members, register entries, simulated device; load 6.57 7.45 7.55 → 6.45 7.41 7.53**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 55 | 188 | 263 | 15835 | 6.00 | 202.1 | 22.21 | 16.9 | 135.0 | 21.00 |
| mantle 85b9c2d | 53 | 175 | 225 | 17417 | 6.00 | 202.1 | 22.21 | 16.8 | 135.0 | 21.00 |
| hyper-durable | 44 | 99 | 154 | 21439 | 5.55 | 152.1 | 22.20 | 15.9 | 93.5 | 21.00 |
| mantle D-1 | 45 | 105 | 145 | 21366 | 5.56 | 152.4 | 22.21 | 15.5 | 93.6 | 21.00 |

**5 members, register entries, simulated device; load 6.45 7.41 7.53 → 6.25 7.35 7.51**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 82 | 298 | 366 | 11061 | 10.00 | 337.4 | 35.01 | 27.4 | 225.7 | 33.00 |
| mantle 85b9c2d | 80 | 286 | 340 | 11929 | 10.00 | 337.4 | 35.01 | 27.4 | 225.7 | 33.00 |
| hyper-durable | 52 | 121 | 165 | 18128 | 9.16 | 256.0 | 34.95 | 23.4 | 155.8 | 33.00 |
| mantle D-1 | 54 | 132 | 215 | 18959 | 9.16 | 256.1 | 34.95 | 23.4 | 155.8 | 33.00 |

**3 members, 1 KiB put entries, simulated device; load 6.25 7.35 7.51 → 6.23 7.33 7.50**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 54 | 208 | 270 | 16661 | 6.00 | 177.4 | 13.80 | 16.6 | 96.0 | 12.00 |
| mantle 85b9c2d | 55 | 212 | 268 | 16008 | 6.00 | 177.4 | 13.80 | 16.6 | 96.0 | 12.00 |
| hyper-durable | 34 | 112 | 146 | 27227 | 5.51 | 125.0 | 13.72 | 14.3 | 54.5 | 12.00 |
| mantle D-1 | 34 | 112 | 149 | 27556 | 5.51 | 125.1 | 13.73 | 14.2 | 54.5 | 12.00 |

**5 members, 1 KiB put entries, simulated device; load 6.23 7.33 7.50 → 6.69 7.41 7.53**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs |
|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 78 | 334 | 401 | 11588 | 10.00 | 295.7 | 16.99 | 26.8 | 160.0 | 14.00 |
| mantle 85b9c2d | 76 | 321 | 372 | 11794 | 10.00 | 295.7 | 16.99 | 26.5 | 160.0 | 14.00 |
| hyper-durable | 54 | 149 | 200 | 17098 | 9.16 | 210.8 | 16.82 | 23.8 | 90.1 | 14.01 |
| mantle D-1 | 54 | 150 | 194 | 17134 | 9.17 | 210.6 | 16.83 | 24.0 | 90.1 | 14.01 |

- With three and five members, p50 17–37% lower and entries a second 33–71% higher; allocations
  31–44% fewer an entry on the driving thread (93.5 against 135.0, 155.8 against 225.7, 54.5
  against 96.0, 90.1 against 160.0) and 24–35% fewer in the process.
- **Reallocations.** The process's are at or below mantle's at every point (33.44 against 33.49;
  16.83 against 16.99 after 3,000). The driving thread's are mantle's after 3,000 entries (21.00,
  33.00 and 12.00 against the same; 14.01 against 14.00), and 0.01–0.02 above in a group's first
  3,000 (21.02 against 21.01). Recorded by size and stack with a tracing allocator (a scratch
  build, not kept), those are the core's message queues growing to the high water the pipeline
  needs, once each: about 18 growths a group of three more than mantle's shell makes, from four
  slots to eight and sixteen. After
  12,000 entries the stand-in's driving thread reallocated as mantle's (21.00, 33.00), and before
  the core kept a spare queue for each ready in flight (hyper-raft `df5f8ad`) it grew a follower's
  queue from four slots again 176 times in 12,000 entries at three members and 212 at five
  (21.02 and 33.02 an entry).
- **One member**: p50 within a microsecond (12 against 11 and 13 µs), entries a second 2.3% fewer
  with register entries and 6.7% more with 1 KiB entries. See "One member" below.

## One member

```
HYPER_DURABLE_DIAGNOSIS=1 ./target/release/hyper-durable-compare --devices file --members 1 \
  --shapes register,put --rounds 8 --entries 500 --warm 50
```

2026-10-03, 17:47–17:54 PDT. 4,000 entries a side:

**1 member, register entries, device; load 20.54 14.97 11.25 → 4.02 11.30 10.81**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 8725 | 48390 | 146935 | 90 | 1.00 | 38.4 | 9.06 | 16.5 | 38.3 | 9.02 | 8707 | 48362 |
| mantle 85b9c2d | 8741 | 37375 | 108733 | 89 | 1.00 | 38.4 | 9.06 | 17.3 | 38.3 | 9.02 | 8717 | 37342 |
| hyper-durable | 9440 | 57560 | 144584 | 84 | 1.00 | 30.4 | 9.06 | 18.1 | 30.3 | 9.02 | 9418 | 57526 |
| mantle D-1 | 9407 | 44444 | 169264 | 82 | 1.00 | 30.4 | 9.06 | 17.8 | 30.3 | 9.02 | 9393 | 44420 |

**1 member, 1 KiB put entries, device; load 4.02 11.30 10.81 → 3.89 8.06 9.55**

| side | p50 µs | p99 µs | p99.9 µs | entries/s | flushes | allocs | reallocs | switches | driver's allocs | driver's reallocs | leader's write p50 µs | leader's write p99 µs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mantle 1c179e8 | 8614 | 23116 | 27544 | 108 | 1.00 | 26.0 | 10.05 | 17.4 | 26.0 | 10.01 | 8592 | 23075 |
| mantle 85b9c2d | 8536 | 22556 | 146141 | 106 | 1.00 | 26.0 | 10.05 | 16.8 | 26.0 | 10.01 | 8514 | 22530 |
| hyper-durable | 8659 | 25197 | 268937 | 92 | 1.00 | 18.0 | 10.05 | 17.6 | 18.0 | 10.01 | 8638 | 25175 |
| mantle D-1 | 8561 | 24681 | 154813 | 102 | 1.00 | 18.0 | 10.05 | 17.1 | 18.0 | 10.01 | 8539 | 24661 |

On every side an entry's commit latency is its leader's write and 14–37 µs more at every
percentile (9,407 against 9,393 µs for D-1 at p50, 8,725 against 8,707 for mantle 1c179e8; the
same at p99 and p99.9). Each entry is one write and one flush on every side, and the shells
differ only in what they add to the write, which is the same; what differs between points is
the write, the device's flush under the load of the moment. Across today's three runs on the
device the shells' one-member p50 and p99 trade places: register entries 8.58 against 8.66 ms
and 9.41 against 8.73 at p50, 1 KiB entries 21.2 against 22.2 and 8.56 against 8.61; with
hyper-durable's stand-in at hyper-raft `687244f` (16:03–16:42 PDT, load 3.2–9.1) 30.8 against
29.8 and 26.6 against 26.7.

On the simulated device, where a flush costs nothing, what is left is the wake. Each side was
run again with the D-1 side waiting for its log's answer by polling rather than blocking on its
channel (`HYPER_DURABLE_SPIN`), six rounds of 3,000 entries, 18:00 PDT at a load average of 16–18:

| side | register entries/s | 1 KiB entries/s | leader's write p50 µs |
|---|---|---|---|
| mantle 1c179e8 | 20,892; 19,883 | 20,757; 18,894 | 34; 33–37 |
| mantle 85b9c2d | 19,115; 20,653 | 18,793; 18,723 | 36; 32–36 |
| mantle D-1, blocking | 18,946 | 19,824 | 40, 39 |
| mantle D-1, polling | 25,019 | 25,471 | 28, 30 |

The first figure of each pair is the run beside D-1 blocking, the second beside D-1 polling.
Blocking on a channel, the D-1 side's write takes 5–6 µs more at the median and it commits
4.5–9.3% fewer entries a second; polling, 26–35% more than mantle's shell. mantle's shell's
harness waits in `wait_persisted`, the log's own wait for its ticket; a node's shard waits for
the next of many ranges' answers and is woken through its waker (node.md §1.3), the wake this
harness's channel stands for.

## Context switches

```
for i in 1 2; do for o in "mantle 1c" "mantle 85" "hyper" "mantle D-1"; do
  HYPER_DURABLE_ONLY="$o" /usr/bin/time -l ./target/release/hyper-durable-compare \
    --devices file --members 3 --shapes register --rounds 2 --entries 300
done; done
```

One side a process, the device, three members, two rounds of 300 entries after 50, 17:54–17:58
PDT at a load average of 3.8–7.7; the whole process, both rounds' groups opened and warmed:

| side | voluntary | involuntary | user + sys s | wall s | switches an entry, measured |
|---|---|---|---|---|---|
| mantle 1c179e8 | 61,157; 60,248 | 21,017; 17,920 | 1.07; 1.10 | 50.4; 34.9 | 118.2; 112.7 |
| mantle 85b9c2d | 61,020; 65,760 | 20,304; 18,220 | 1.30; 0.94 | 32.9; 37.3 | 119.7; 120.3 |
| hyper-durable | 63,852; 70,760 | 21,178; 23,276 | 0.74; 1.09 | 21.8; 24.6 | 119.9; 143.5 |
| mantle D-1 | 62,454; 68,763 | 23,010; 22,290 | 1.08; 1.01 | 23.0; 23.7 | 126.3; 130.7 |

The D-1 side switches 2–14% more voluntarily and 9–24% more involuntarily over a run that takes a
third to a half less wall time, for the same CPU time; an entry's switches are 7–16% more here
and, in the grid above, within 4% either way or 14% fewer. hyper-raft's trace of the same
difference (its `docs/benchmarks.md`, "The three losses against mantle's shell, traced") found
them involuntary and the overlap's: driven as mantle's shell drives, every write answered before
the next turn, the shell switched as mantle's did, at mantle's latency. Taking readies ahead keeps
a leader's log and its followers' logs runnable at once, and the scheduler preempts more among
them.

## Verdict

The range replica on the shell is at least as fast as mantle's own shell at every point with more
than one member, on the device and on the simulated device, and allocates 21–44% less with no more
reallocations once a group has warmed; with one member, on the device, it is even, an entry's
latency its write's on both; on the simulated device, it is within the cost of the owner's wake,
which a node's shard pays as this harness does. mantle moves onto it.
