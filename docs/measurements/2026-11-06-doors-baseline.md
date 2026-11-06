# The doors' baseline: every operation through both doors, before any cut

**Question.** What does each operation cost through mantle's two doors, the native protocol and the
S3 listener, on the box, before step 4 cuts anything: latency from each request's intended start,
throughput, the servers' allocations, faults, CPU and instructions per operation, beside Garage,
SeaweedFS and RustFS through the same HTTP load, and beside msquic, quinn and bare hyper-quic for
QUIC (measurement.md §12; research/79)?

**The run.** `mantle bench doors` at 4711a46a (rebased on main cc720c05) with the trial runner
`scripts/doors-trials.sh` at 1878a7e0, one Williams cycle, as the systemd user service
`doors-baseline`, 2026-11-06 09:20 to 13:32 UTC (4 h 12 min), then six short control and slope
cases (`doors-baseline-controls`, 13:33 to 14:00). Arms: A1 and A2, one timed release build copied
twice (the A/A pair, sha256 7af9aaf9...); C, the `count-allocs` build of the same commit
(84e202d7...); Garage v2.5.0, SeaweedFS 4.52 and RustFS 1.0.4 through `--endpoint`, A1 as their
harness; secnetperf (msquic v2.6.3), quinn-perf (quinn-proto 0.11.18) and hyper-quic's
`geo_open_loop` (vendor/hyper-quic at this commit, bf3bc98f...). Every case a fresh process tree in
its own `systemd-run --user --scope` with `MemorySwapMax=0` (24 GiB for S3 cases, 4 GiB for QUIC,
the floor below for memfloor) under `taskset -c 8-15`, its rusage kept, the host's load averages,
MemAvailable and pressure recorded at its start and end, its cell or data directory fresh and removed
after it. Load: open loop, Poisson arrivals at 500 a second, 16 clients (the native door holds 6,
the cell's admission), latency from each operation's intended start; sized phases 368 operations at
most and 256 MiB (so 32 at 8 MiB, 4 at 64 MiB); the set's phases every object of a door's half,
5,000. Every server's instructions and cycles counted per phase with `perf stat -e
instructions:u,cycles:u` (`--perf`); faults from the counts line (mantle) or /proc/PID/stat
(comparators). The host ran at a one-minute load of 17 to 81 (production and other sessions; step
4's cells on CPUs 0-7).

**Conditions and matrices.** quiet runs every size (0 B, 1 B, 65,535, 65,536, 65,537 B, 128 KiB,
8 MiB, 64 MiB) and the 10,000 set; busy2x (16 busy loops on 8-15) 64 KiB, 8 MiB and the set; busy1x
(8 loops), cpu1 (`taskset -c 8`), cpu2 (`8-9`) and memfloor 64 KiB and 8 MiB. memfloor's
`MemoryMax` is the cell's memory floor, 2,466,576,405 bytes: three times the 822,192,135 bytes a
node's `memory.floor.*` basis lines sum to as `mantle serve` says them at this commit (objects
83,513,198; volumes 147,172,608; engines 822,800; ranges 267,841,530; log 225,853,540; transport
69,725,483; threads 27,262,976; crates/node/src/derive.rs `floor`), read from a founder of a cell
made as the harness makes one (`floor-probe-serve.out` in the batch directory). Comparators ran under the same limit.

## What was cut, and why

- **The 100,000 set.** The dimensioning pass (2026-11-04, its outputs kept in
  baseline-2026-11-04/dimension) measured the native door's small writes at about 20 a second on
  this box; the 100,000 set's 50,000 puts and 50,000 deletes a door would take over two hours a
  mantle case, six to eighteen hours for the three arms. The 10,000 set (5,000 a door, past the
  3,688 a p99.9 needs) runs instead.
- **Sizes under busy1x, cpu1, cpu2, memfloor**: 64 KiB and 8 MiB only; busy2x adds the set. One
  Williams cycle: one case an arm a condition, so each cell's quantiles are one case's and the A/A
  band is one pair a cell.
- **5 GiB and the 10 GB stream** were not asked (§12's sizes on request).

## Headline: per door and operation, quiet and busy2x

Latency in milliseconds from each operation's intended start; a dash where the case's samples do
not resolve the quantile at 95% (368 for a p99, 3,688 for a p99.9). Mantle's rows are A1's;
allocs/op are C's, the servers' allocator counts per operation summed over the cell's three nodes;
faults/op are minor faults; instr/op are `perf stat`'s user-space instructions over the servers'
processes (three nodes for mantle, one process for a comparator). ops/s is the phase's operations
over its elapsed time.

| cond | size | op | door (arm) | n | ops/s | p50 | p99 | p99.9 | allocs/op (C) | faults/op | instr/op |
|---|---|---|---|---|---|---|---|---|---|---|---|
| quiet | 64KiB | PUT | native | 368 | 27 | 6,577 | 12,737 | - | 2143.9 | 8.0 | 34108882.9 |
| quiet | 64KiB | PUT | http | 368 | 36 | 4,295 | 9,440 | - | 1850.1 | 2.5 | 46224328.3 |
| quiet | 64KiB | PUT | http@garage | 368 | 12 | 10,737 | 29,528 | - | - | 170.3 | 2859763.7 |
| quiet | 64KiB | PUT | http@seaweedfs | 368 | 297 | 319 | 503 | - | - | 0.6 | 5161580.9 |
| quiet | 64KiB | PUT | http@rustfs | 368 | 475 | 38.80 | 77.59 | - | - | 0.1 | 3027142.7 |
| quiet | 64KiB | GET | native | 368 | 552 | 1.70 | 10.22 | - | 462.9 | 6.6 | 2939591.3 |
| quiet | 64KiB | GET | http | 368 | 533 | 0.97 | 7.21 | - | 481.9 | 0.3 | 2446722.2 |
| quiet | 64KiB | GET | http@garage | 368 | 553 | 1.18 | 5.64 | - | - | 137.1 | 590609.0 |
| quiet | 64KiB | GET | http@seaweedfs | 368 | 553 | 0.61 | 1.31 | - | - | 0.2 | 1553985.1 |
| quiet | 64KiB | GET | http@rustfs | 368 | 553 | 0.43 | 1.11 | - | - | 1.4 | 630100.6 |
| quiet | 64KiB | HEAD | native | 368 | 499 | 0.40 | 1.15 | - | 102.4 | 0.0 | 527906.8 |
| quiet | 64KiB | HEAD | http | 368 | 524 | 0.34 | 0.66 | - | 148.6 | 0.0 | 544570.8 |
| quiet | 64KiB | HEAD | http@garage | 368 | 499 | 0.29 | 2.00 | - | - | 0.9 | 222881.1 |
| quiet | 64KiB | HEAD | http@seaweedfs | 368 | 499 | 0.41 | 0.54 | - | - | 0.0 | 811631.2 |
| quiet | 64KiB | HEAD | http@rustfs | 368 | 499 | 0.29 | 0.52 | - | - | 0.0 | 463025.7 |
| quiet | 64KiB | DELETE | native | 368 | 35 | 5,906 | 9,776 | - | 460.6 | 0.0 | 6813939.7 |
| quiet | 64KiB | DELETE | http | 368 | 47 | 3,691 | 7,248 | - | 504.7 | 0.0 | 16867264.6 |
| quiet | 64KiB | DELETE | http@garage | 368 | 50 | 3,892 | 6,690 | - | - | 1.7 | 392064.2 |
| quiet | 64KiB | DELETE | http@seaweedfs | 368 | 507 | 6.29 | 16.52 | - | - | 1.1 | 1138557.2 |
| quiet | 64KiB | DELETE | http@rustfs | 368 | 103 | 1,342 | 2,853 | - | - | 7.4 | 8378815.7 |
| quiet | 8MiB | PUT | native (2 failed) | 32 | 4 | 4,832 | - | - | 69278.6 | 16796.0 | 942485756.0 |
| quiet | 8MiB | PUT | http (11 failed) | 32 | 5 | 4,027 | - | - | 56500.4 | 18716.8 | 821756834.4 |
| quiet | 8MiB | PUT | http@garage | 32 | 2 | 10,469 | - | - | - | 10525.3 | 215687290.1 |
| quiet | 8MiB | PUT | http@seaweedfs | 32 | 65 | 302 | - | - | - | 7460.4 | 389470862.3 |
| quiet | 8MiB | PUT | http@rustfs | 32 | 97 | 218 | - | - | - | 19.8 | 95124653.0 |
| quiet | 8MiB | GET | native (2 failed) | 32 | 59 | 285 | - | - | 11048.2 | 8233.2 | 135231519.0 |
| quiet | 8MiB | GET | http (11 failed) | 32 | 94 | 143 | - | - | 2724.5 | 5960.4 | 82130177.3 |
| quiet | 8MiB | GET | http@garage | 32 | 274 | 42.99 | - | - | - | 6924.7 | 22768716.1 |
| quiet | 8MiB | GET | http@seaweedfs | 32 | 203 | 48.23 | - | - | - | 0.1 | 4578414.6 |
| quiet | 8MiB | GET | http@rustfs | 32 | 250 | 19.40 | - | - | - | 1.3 | 4965545.1 |
| quiet | 8MiB | HEAD | native (2 failed) | 32 | 432 | 0.27 | - | - | 108.9 | 0.0 | 469409.5 |
| quiet | 8MiB | HEAD | http (11 failed) | 32 | 571 | 0.33 | - | - | 139.1 | 0.0 | 487779.3 |
| quiet | 8MiB | HEAD | http@garage | 32 | 432 | 0.23 | - | - | - | 11.2 | 206135.7 |
| quiet | 8MiB | HEAD | http@seaweedfs | 32 | 427 | 0.44 | - | - | - | 0.0 | 833727.6 |
| quiet | 8MiB | HEAD | http@rustfs | 32 | 427 | 0.37 | - | - | - | 0.0 | 306826.2 |
| quiet | 8MiB | DELETE | native | 32 | 48 | 344 | - | - | 486.0 | 0.1 | 44256803.4 |
| quiet | 8MiB | DELETE | http | 32 | 15 | 738 | - | - | 491.1 | 0.1 | 42821971.8 |
| quiet | 8MiB | DELETE | http@garage | 32 | 41 | 453 | - | - | - | 5.2 | 406395.2 |
| quiet | 8MiB | DELETE | http@seaweedfs | 32 | 457 | 24.12 | - | - | - | 0.3 | 1005187.4 |
| quiet | 8MiB | DELETE | http@rustfs | 32 | 42 | 495 | - | - | - | 16.3 | 7067612.4 |
| quiet | set10000 | PUT | native | 5000 | 28 | 73,014 | 165,673 | 165,673 | 2178.6 | 11.1 | 113247354.2 |
| quiet | set10000 | PUT | http | 5000 | 27 | 92,342 | 173,643 | 173,643 | 1885.6 | 0.2 | 153765437.5 |
| quiet | set10000 | PUT | http@garage | 5000 | 8 | 403,727 | 627,539 | 627,539 | - | 151.7 | 2950058.6 |
| quiet | set10000 | PUT | http@seaweedfs | 5000 | 411 | 554 | 2,248 | 2,248 | - | 12.7 | 5142040.9 |
| quiet | set10000 | PUT | http@rustfs | 5000 | 501 | 55.57 | 419 | 428 | - | 0.8 | 3010541.9 |
| quiet | set10000 | GET | native | 5000 | 499 | 1.11 | 4.00 | 19.40 | 455.5 | 0.0 | 2673767.7 |
| quiet | set10000 | GET | http | 5000 | 482 | 1.08 | 4.19 | 6.68 | 465.6 | 0.0 | 2075287.7 |
| quiet | set10000 | GET | http@garage | 5000 | 455 | 621 | 990 | 1,007 | - | 122.7 | 569945.3 |
| quiet | set10000 | GET | http@seaweedfs | 5000 | 499 | 0.47 | 1.05 | 2.36 | - | 0.1 | 1297067.9 |
| quiet | set10000 | GET | http@rustfs | 5000 | 499 | 0.66 | 2.36 | 4.72 | - | 0.7 | 652805.7 |
| quiet | set10000 | DELETE | native | 5000 | 38 | 75,162 | 120,259 | 120,821 | 485.6 | 0.0 | 34173830.1 |
| quiet | set10000 | DELETE | http | 5000 | 42 | 47,245 | 109,522 | 110,122 | 504.8 | 0.0 | 67625907.3 |
| quiet | set10000 | DELETE | http@garage | 5000 | 44 | 49,392 | 103,079 | 103,171 | - | 2.3 | 409983.8 |
| quiet | set10000 | DELETE | http@seaweedfs | 5000 | 494 | 6.55 | 2,079 | 2,079 | - | 1.2 | 1249594.8 |
| quiet | set10000 | DELETE | http@rustfs | 5000 | 62 | 36,507 | 70,867 | 71,071 | - | 13.1 | 9700043.4 |
| quiet | set10000 | LIST | native (1 failed) | 11 | 33 | 155 | - | - | 16238.5 | 4.8 | 154603623.2 |
| quiet | set10000 | LIST | http | 11 | 38 | 168 | - | - | 18836.6 | 0.5 | 160927945.4 |
| quiet | set10000 | LIST | http@garage | 11 | 15 | 702 | - | - | - | 455.5 | 38160393.9 |
| quiet | set10000 | LIST | http@seaweedfs | 11 | 306 | 9.70 | - | - | - | 7.7 | 47666485.7 |
| quiet | set10000 | LIST | http@rustfs | 11 | 4 | 2,590 | - | - | - | 262.5 | 1293966958.8 |
| busy2x | 64KiB | PUT | native | 368 | 28 | 6,174 | 12,699 | - | 2383.7 | 35.1 | 11648373.4 |
| busy2x | 64KiB | PUT | http | 368 | 24 | 6,979 | 14,458 | - | 1995.4 | 14.8 | 24542042.8 |
| busy2x | 64KiB | PUT | http@garage | 368 | 5 | 39,728 | 68,638 | - | - | 172.1 | 2895456.4 |
| busy2x | 64KiB | PUT | http@seaweedfs | 368 | 42 | 7,516 | 8,049 | - | - | 32.4 | 5366842.8 |
| busy2x | 64KiB | PUT | http@rustfs | 368 | 231 | 310 | 906 | - | - | 4.9 | 3000386.3 |
| busy2x | 64KiB | GET | native | 368 | 419 | 147 | 214 | - | 448.6 | 16.8 | 2321416.2 |
| busy2x | 64KiB | GET | http | 368 | 524 | 11.01 | 53.48 | - | 461.4 | 2.1 | 2002073.3 |
| busy2x | 64KiB | GET | http@garage | 368 | 484 | 10.75 | 36.70 | - | - | 162.8 | 548003.7 |
| busy2x | 64KiB | GET | http@seaweedfs | 368 | 501 | 0.97 | 9.96 | - | - | 26.1 | 1471232.5 |
| busy2x | 64KiB | GET | http@rustfs | 368 | 503 | 1.67 | 23.59 | - | - | 0.6 | 584143.9 |
| busy2x | 64KiB | HEAD | native | 368 | 493 | 1.00 | 9.18 | - | 109.8 | 0.0 | 441318.3 |
| busy2x | 64KiB | HEAD | http | 368 | 467 | 0.66 | 8.65 | - | 151.2 | 0.0 | 486447.4 |
| busy2x | 64KiB | HEAD | http@garage | 368 | 496 | 0.27 | 3.60 | - | - | 11.3 | 207893.2 |
| busy2x | 64KiB | HEAD | http@seaweedfs | 368 | 495 | 0.38 | 3.21 | - | - | 0.4 | 758939.2 |
| busy2x | 64KiB | HEAD | http@rustfs | 368 | 495 | 0.33 | 2.16 | - | - | 0.0 | 277395.6 |
| busy2x | 64KiB | DELETE | native | 368 | 45 | 3,355 | 7,497 | - | 468.4 | 2.0 | 13836913.0 |
| busy2x | 64KiB | DELETE | http | 368 | 44 | 3,959 | 7,516 | - | 498.3 | 0.5 | 4497042.3 |
| busy2x | 64KiB | DELETE | http@garage | 368 | 21 | 9,127 | 16,784 | - | - | 4.1 | 464515.4 |
| busy2x | 64KiB | DELETE | http@seaweedfs | 368 | 528 | 17.83 | 36.70 | - | - | 0.4 | 998021.9 |
| busy2x | 64KiB | DELETE | http@rustfs | 368 | 65 | 2,886 | 4,943 | - | - | 10.6 | 8406495.8 |
| busy2x | 8MiB | PUT | native (7 failed) | 32 | 2 | 5,906 | - | - | 97129.2 | 8517.8 | 920640860.5 |
| busy2x | 8MiB | PUT | http (22 failed) | 32 | 4 | 4,295 | - | - | 34900.0 | 12963.7 | 613967655.9 |
| busy2x | 8MiB | PUT | http@garage | 32 | 2 | 11,543 | - | - | - | 10536.8 | 216031113.7 |
| busy2x | 8MiB | PUT | http@seaweedfs | 32 | 31 | 789 | - | - | - | 7150.7 | 386436446.7 |
| busy2x | 8MiB | PUT | http@rustfs | 32 | 52 | 352 | - | - | - | 10.7 | 95039944.0 |
| busy2x | 8MiB | GET | native (7 failed) | 32 | 17 | 1,023 | - | - | 7553.4 | 4494.5 | 118408084.0 |
| busy2x | 8MiB | GET | http (22 failed) | 32 | 69 | 37.75 | - | - | 2425.4 | 2239.3 | 25012013.2 |
| busy2x | 8MiB | GET | http@garage | 32 | 26 | 822 | - | - | - | 7039.2 | 22774102.1 |
| busy2x | 8MiB | GET | http@seaweedfs | 32 | 125 | 138 | - | - | - | 80.7 | 4447548.5 |
| busy2x | 8MiB | GET | http@rustfs | 32 | 116 | 147 | - | - | - | 0.3 | 5093027.9 |
| busy2x | 8MiB | HEAD | native (7 failed) | 32 | 640 | 0.57 | - | - | 87.9 | 0.0 | 403593.1 |
| busy2x | 8MiB | HEAD | http (22 failed) | 32 | 508 | 0.41 | - | - | 129.5 | 0.0 | 400771.4 |
| busy2x | 8MiB | HEAD | http@garage | 32 | 640 | 0.34 | - | - | - | 11.2 | 208753.1 |
| busy2x | 8MiB | HEAD | http@seaweedfs | 32 | 640 | 0.39 | - | - | - | 0.0 | 773999.6 |
| busy2x | 8MiB | HEAD | http@rustfs | 32 | 640 | 0.32 | - | - | - | 0.0 | 270816.7 |
| busy2x | 8MiB | DELETE | native | 32 | 14 | 1,342 | - | - | 438.6 | 0.1 | 5365445.9 |
| busy2x | 8MiB | DELETE | http | 32 | 39 | 419 | - | - | 439.9 | 0.0 | 5143027.9 |
| busy2x | 8MiB | DELETE | http@garage | 32 | 23 | 755 | - | - | - | 6.2 | 390628.3 |
| busy2x | 8MiB | DELETE | http@seaweedfs | 32 | 356 | 21.50 | - | - | - | 0.3 | 1001277.5 |
| busy2x | 8MiB | DELETE | http@rustfs | 32 | 38 | 419 | - | - | - | 11.5 | 6968653.7 |
| busy2x | set10000 | PUT | native (1 failed) | 5000 | 25 | 98,784 | 191,354 | 191,354 | 2000.2 | 26.4 | 43711858.9 |
| busy2x | set10000 | PUT | http | 5000 | 28 | 88,047 | 170,300 | 170,300 | 1929.6 | 33.6 | 59104826.7 |
| busy2x | set10000 | PUT | http@garage | 5000 | 8 | 283,468 | 600,750 | 600,750 | - | 152.0 | 2858510.4 |
| busy2x | set10000 | PUT | http@seaweedfs | 5000 | 200 | 6,979 | 14,725 | 14,725 | - | 4.9 | 5136659.1 |
| busy2x | set10000 | PUT | http@rustfs | 5000 | 345 | 2,416 | 4,228 | 4,270 | - | 0.6 | 2962917.5 |
| busy2x | set10000 | GET | native | 5000 | 477 | 24.12 | 201 | 222 | 451.5 | 16.7 | 2610927.0 |
| busy2x | set10000 | GET | http | 5000 | 505 | 88.08 | 528 | 554 | 472.7 | 14.8 | 2082889.8 |
| busy2x | set10000 | GET | http@garage | 5000 | 469 | 428 | 738 | 753 | - | 133.0 | 552594.4 |
| busy2x | set10000 | GET | http@seaweedfs | 5000 | 503 | 0.67 | 10.22 | 19.92 | - | 0.3 | 1220216.2 |
| busy2x | set10000 | GET | http@rustfs | 5000 | 503 | 1.05 | 7.21 | 14.94 | - | 0.5 | 628429.0 |
| busy2x | set10000 | DELETE | native | 5000 | 40 | 54,761 | 115,724 | 115,724 | 486.4 | 0.1 | 41785372.8 |
| busy2x | set10000 | DELETE | http | 5000 | 35 | 61,203 | 130,997 | 132,109 | 511.8 | 0.1 | 23193198.8 |
| busy2x | set10000 | DELETE | http@garage | 5000 | 25 | 81,604 | 187,350 | 187,350 | - | 2.0 | 427537.2 |
| busy2x | set10000 | DELETE | http@seaweedfs | 5000 | 462 | 24.64 | 1,242 | 1,258 | - | 0.1 | 1010115.1 |
| busy2x | set10000 | DELETE | http@rustfs | 5000 | 47 | 49,392 | 96,307 | 96,307 | - | 6.7 | 11090998.9 |
| busy2x | set10000 | LIST | native (1 failed) | 11 | 19 | 214 | - | - | 16254.7 | 21.3 | 124451012.2 |
| busy2x | set10000 | LIST | http | 11 | 21 | 277 | - | - | 20790.1 | 60.6 | 132795860.4 |
| busy2x | set10000 | LIST | http@garage | 11 | 24 | 453 | - | - | - | 337.2 | 36207374.1 |
| busy2x | set10000 | LIST | http@seaweedfs | 11 | 100 | 92.27 | - | - | - | 28.7 | 46688901.4 |
| busy2x | set10000 | LIST | http@rustfs | 11 | 2 | 4,607 | - | - | - | 313.5 | 1183585492.4 |

What the tables say:

- **Writes run at the doors' capacity, and their latency is the queue's.** Mantle takes 24 to 36
  small puts and 35 to 47 deletes a second through either door at quiet, against the open loop's
  500: a 64 KiB put's p50 is 4.3 s (HTTP) and 6.6 s (native), the set's 73 to 92 s, and the
  lateness column of tables.md says the same. RustFS takes 475 to 501 puts a second (p50 39 to
  56 ms), SeaweedFS 297 to 411, Garage 8 to 12. Reads are under capacity for every server and are
  comparable: a 64 KiB GET's p50 is 1.70 ms native, 0.97 ms HTTP, 0.43 to 1.18 ms for the
  comparators; HEAD 0.29 to 0.40 ms everywhere.
- **Mantle refuses 8 MiB writes with SlowDown.** In every mantle case, 2 to 30 of the 32 8 MiB
  puts, parts and HTTP streamed puts were refused 503 SlowDown three times (botocore's retries) and
  failed; the reads of those keys then found no such key. The native door's streamed put was never
  refused. The comparators took every 8 MiB write (RustFS 97 a second, Garage 2).
- **A small operation's cost.** A 64 KiB put: about 2,100 allocations and 34 to 46 million
  instructions across the cell's three nodes (each write replicated three times), against 3 to 5
  million for a comparator's single process; a 64 KiB GET 460 to 480 allocations and 2.4 to 2.9
  million instructions, against 0.6 to 1.6 million. The 8 MiB put: 57,000 to 69,000 allocations,
  about 900 million instructions and 17,000 to 19,000 faults an operation.
- **busy2x** doubles mantle's small-read p50 to tens or hundreds of ms on the native door
  (64 KiB GET 147 ms native, 11 ms HTTP), where the comparators stay at 1 to 11 ms; mantle's write
  rates barely move (they were bound already).

## A/A band (A2 against A1, the same binary)

Per cell, |A2 − A1| / A1, over the cells where neither arm failed; median and 90th percentile:

| condition | reads p50 | reads p99 | writes p50 | writes p99 | instructions/op (all) |
|---|---|---|---|---|---|
| quiet | 17.7% / 125% | 48.6% / 580% | 32.4% / 73% | 27.4% / 61% | 5.6–5.7% / 58–65% |
| busy1x | 17.2% / 151% | 31.9% / 960% | 78.5% / 526% | 56.8% / 228% | 4.4–4.5% / 9–26% |
| busy2x | 27.1% / 822% | 40.0% / 108% | 20.8% / 64% | 12.6% / 31% | 2.5–4.1% / 6–14% |
| cpu1 | 16.7% / 58% | 16.9% / 40% | 15.8% / 78% | 30.7% / 180% | 2.1–4.1% / 8% |
| cpu2 | 23.9% / 47% | 51.1% / 284% | 39.3% / 68% | 47.3% / 71% | 1.6–3.5% / 5–9% |
| memfloor | 12.9% / 37% | 31.9% / 238% | 22.8% / 52% | 8.2% / 43% | 1.2–2.2% / 4% |

With one case a cell on a host at load 17 to 81, the latency band is 13 to 80% at the median: no
latency effect under about 30% is resolved by this batch. Instructions per operation hold to 1 to
6% at the median, the measure step 4's cuts can be judged by at one round; latency needs more rounds
(CYCLES) or cells measured beside each other.

## Per connection and handshakes

The main batch's mantle cases all ended at the controls (below), before the slope; six short cases
(`--sizes 1 --objects 0 --ops 8`, quiet and busy2x, A1, A2, C) measured it, five completing (raw
in controls/). From the counting arm, quiet: an HTTP connection holds 68,566 live bytes on the
servers and 131,222 in the harness, its handshake (connect and a first HEAD) 157 allocations and
98,891 bytes on the servers, p50 0.43 ms; a native connection 141,807 live bytes on the servers and
344,052 in the client, its handshake 3,598 allocations and 866,817 bytes on the servers, 511 and
425,181 in the client, at most 12.7 ms (three handshakes, so no median). The comparators' HTTP
handshakes: p50 0.09 to 0.26 ms; their footprint slope 0 to 12,743 bytes a connection (RSS, no
allocator counts).

## QUIC

Loopback, server and client pinned together to 8-15. Latency per request; quinn-perf's per-stream
download duration; hyper-quic's over one RTT through geo_open_loop's relay.

| | quiet | busy2x |
|---|---|---|
| secnetperf 100 B request/response | 46,845/s, p50 19 µs, p99 49 µs, p99.9 219 µs | 45,552/s, p50 20 µs, p99 42 µs |
| secnetperf 64 KiB | 15,344/s, p50 59 µs, p99 124 µs | 10,867/s, p50 88 µs, p99 140 µs |
| secnetperf 8 MiB | 241/s, p50 3.7 ms, p99 10.3 ms | 188/s, p50 5.2 ms, p99 9.7 ms |
| secnetperf 64 MiB | 36/s, p50 25.8 ms, p99 48.3 ms | 24/s, p50 40.8 ms |
| quinn-perf 100 B | 61,799/s, upload p50 12 µs | 15,324/s, p50 12 µs |
| quinn-perf 64 KiB / 8 MiB / 64 MiB | 16,533/s 58 µs; 178/s 4.8 ms; 24/s 46.4 ms | 4,151/s 108 µs; 49/s 16.5 ms; 7/s 160 ms |
| hyper-quic 100 B at 200/s, 0 ms relay | p50 1.44 ms [1.437–1.450], p99 2.45, p99.9 3.79 | p50 2.93 ms, p99 8.56 |
| hyper-quic 100 B at 200/s, 500 ms one way | over one RTT: p50 4.51 ms, p99 688 ms, p99.9 754 ms | not run |

hyper-quic's figures carry its relay's two threads even at 0 ms, so they are not secnetperf's
like for like; hyper-quic has no transfer example, so its 64 KiB to 64 MiB rows are excluded.

## Accounting

78 cases planned (raw/../trial-report.md): **36 completed, 18 failed, 24 excluded.**

- **Failed, 18: every mantle case (A1, A2, C in all six conditions)**, each after every phase was
  measured, at the controls: "a control's object was not put: slowed or unanswered". The control's
  put of a 65,537-byte object, asked right after the 8 MiB and set phases, was refused SlowDown (or
  went unanswered) three times. The phases' lines are complete; the controls, slope and handshake
  lines are not. Of the six short control cases, five caught all six controls and quiet A1 failed
  the same way at eight operations a phase.
- Within completed and failed cases, phases with failures: the 8 MiB (and quiet's 64 MiB) writes
  above, and one native LIST page a mantle case "slowed or unanswered" (the set's first page or
  set0's one).
- **Excluded, 24**: MinIO in each condition (6), run on the Mac later (owner's decision: its full-
  sync mode is a whole-machine sync(2) a PUT); the native door's 500 ms relay for A1, A2 and C in
  each condition (18), not in the harness (below). Within Garage's completed cases its server
  refused, so the harness excluded: every PUT-STREAM ("Chunk format error: Tag", below) and the
  0 B MPU-PART ("Empty body") with the list-parts and completion that needed it.

## Comparators and their durability

Garage v2.5.0 (586726ed, AGPL-3.0) with `data_fsync = true`, `metadata_fsync = true`, LMDB;
SeaweedFS 4.52 (984eac6a, Apache-2.0) with the filer rule `fs.configure -locationPrefix=/
-fsync=true` (the volume server fsyncs .dat and .idx, group-committed, and leveldb2 writes sync)
and `-volume.minFreeSpace=4GiB`; RustFS 1.0.4 (46337942, Apache-2.0) with
`RUSTFS_DURABILITY_MODE=strict` (data and metadata fsynced and commit renames' directories synced
before the PUT returns); all single node, single drive, one copy. Mantle's cell is three nodes on
the same NVMe, three copies, each fdatasynced before the acknowledgement (CLAUDE.md §6): a write
does three times a comparator's durable work. msquic secnetperf v2.6.3 (16ca4d8d, quictls),
quinn-perf at quinn-proto 0.11.18 (eaec0db4). Sources, checksums and the source lines behind each
durability setting: mantle-compare/COMPARATORS.md (copied into tables.md).

## S3's published bars, as references

Amazon S3: "consistent small object latencies ... of roughly 100–200 milliseconds" and "at least
3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix";
S3 Express One Zone "consistent, single-digit millisecond data access" (research/79 [11]). Mantle's
small reads (about 1 ms at quiet) sit inside S3 Express's bar; its small writes, 24 to 47 a second
at seconds of queue under a 500-a-second load, are two orders under S3's per-prefix write rate on
this box.

## Needs a decision

1. **The open loop's rate.** 500 a second is past every server's write capacity but RustFS's, so
   write latency is the queue's; a rate under mantle's write capacity (about 20 a second) would
   make a set's phase take minutes. A per-operation rate, or the closed ladder for writes, is the
   choice.
2. **The 8 MiB SlowDown** in every mantle case, and the controls failing on it after the phases:
   whether the harness's three attempts with botocore's backoff are the bar, or the controls should
   wait for the door to admit again.
3. **Garage's PUT-STREAM refusal**: mantle's codec writes the `aws-chunked` trailer as
   `x-amz-checksum-crc32: VALUE` (RFC 9112's optional space); botocore writes it with none, and
   Garage's parser (src/api/common/signature/streaming.rs, `parse_content`) takes none. The harness
   could write botocore's form, or mantle_http::write::last could.
4. **The 500 ms relay for the native door.** It would take a UDP relay per node in the harness
   (geo_open_loop's two threads a direction, a bounded delay queue, loss and reordering by seed),
   the clients' `Settings.nodes` pointed at the relays, flags for delay, loss and reordering with
   constants.md rows, about 150 to 200 lines; and a matrix trimmed to a few sizes, each operation
   then taking at least a second.
5. **More rounds** (CYCLES=2 or more) for latency effects under 30%.

## Raw outputs

`~/.cache/claude-scratch/doors/baseline-2026-11-06/` on the box: `raw/` (each case's
`box_CONDITION_ARM_r1.out/.err/.host/.rusage/.status`, comparators' server logs, QUIC runs by
size), `controls/raw/`, `plan.tsv`, `progress`, `bin/` (the arms, SHA256SUMS, COMMIT),
`scripts/` (the runner and generator as run), `floor-probe-serve.out`. `python3
scripts/doors-tables.py DIR` regenerates `tables.md` (every cell: n, ops/s, lateness, p50 with its
95% interval, p99 to max, CPU, instructions, cycles, faults, failures; the counting arm's counts;
the A/A band; slope and handshakes; QUIC; host per case; comparator configurations) and
`trial-report.md`.

## The Mac and its Linux VM lane

To come: the same runner on the Mac (macOS arm64) and its Linux VM lane, with MinIO there.
