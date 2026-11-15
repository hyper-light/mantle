# One descriptor a segment: the store's handles share its open files

2026-11-14 and 15. mantle branch `shared-fds` over lean-doors `40a4f84e`, every hyper-raft crate
vendored from branch `shared-segment-fds-doors` (lean-doors' `294678c` and the commits of hyper-raft
branch `shared-segment-fds`; vendor/UPSTREAM.md). Design and sources: research/91.

**Question.** A range's store, kept as segments, held about 188 descriptors a segment on a 32-core
host, each maintenance worker's handle and each of the issuer's duplicates of it opening every
segment for itself, so a 100M fill ran out of descriptors at macOS's default limit of 10,240 and in
a Linux container's 2,048 (2026-11-06-targets.md, 2026-11-12-targets.md). With every handle
transferring through one open file a segment: do descriptors scale with segments alone, does a
100M fill complete at both limits, and does anything get slower? And what cost the earlier patch
(`2026-11-06-targets-hyper-block-fd.patch`) its 12% on fills at 10M?

**Answer.** Descriptors are one a segment and the process's few others, whatever the workers: a
100M fill holds 47-49 on macOS where lean-doors holds 10,240 and stops, and completes in a Linux
container at its default 2,048. Nothing measured got slower past the A/A: fills, gets and seeks
over lean-doors at 1M and 10M on macOS and Linux sit within the A/A's band, and the engine's lead
over RocksDB is unchanged. At 100M lean-doors cannot run at macOS's limit at all; on Linux, where
the runner's own limit lets it, four rounds on a host whose load ran from 15 to 174 resolve nothing
the A/A's spread does not hold, and the fill's CPU a put is lean-doors' (1.01x; the A/A 0.97x).
The earlier patch's 12% did not come back, for it or for this change: in ten interleaved rounds at
10M on Linux the earlier patch filled at 1.006x lean-doors (A/A 0.936x). Sharing an open file
description among threads does cost positioned reads (5-13% at 4 to 32 threads in a
microbenchmark of nothing but cached reads, research/91), but this change shares one among more
threads than the earlier patch did and its fills cost no more CPU a put than lean-doors' (0.97x on
both platforms at 10M), so that is not what a fill pays.

## How it ran

Arms interleaved in a Williams order, each case its own process, database removed after, every
binary a release build of `benches/ranges.rs` with the bench's mantle arguments
(`DB NUM 1 1000000 256 16 1000000 10 --rocks-seed=301 --shard-client --segments
--machine-memory`): `m_base` lean-doors `40a4f84e`, `m_fix` this branch, `m_aa` `m_base`'s binary
again (the A/A), `m_old` lean-doors with the 2026-11-06 patch, `rocks` RocksDB 11.8.1's `db_bench`
(`abeebd963`) with inside7.sh's flags and a 256 MiB block cache. A case's descriptors are counted
once a second while it runs, from the OS (`/proc/PID/fd`; `proc_pidinfo(PROC_PIDLISTFDS)`), and
its rusage kept. A busy host is the norm; no case was dropped for load. Ratios are the median of
each round's own ratio, with its range and n.

| Lane | Machine | Open-file limit | Binaries |
|---|---|---|---|
| macOS aarch64 | adas-mac-studio.local-2 (Adas-Mac-Studio.local): 32 cores, 96 GB, APFS | the kiln shell's default, soft 10,240 | `m_fix` `39debbd5`, `m_base`/`m_aa` `80f53480`, db_bench `4d9ba097` |
| Linux x86_64 | adacachy2: 32 threads, 122 GiB, btrfs | the runner's own, soft 1,048,576 (not raised for these runs) | `m_fix` md5 `2626d8e3`, `m_base`/`m_aa` `8c584da6`, `m_old` `de6f5093`, db_bench `5a6613b9` |
| Linux x86_64, container | adacachy2, the fds-bench image (rust:1.98.1, inside7.sh), no `--ulimit` | Docker's default here, soft 2,048 | `m_fix` as above |

The binaries were built from hyper-raft `07c03e7`; the vendored head, `8a4d855`, adds discovery's
guard for a segment being added (`Open::add`, `Open::discover`) and its document, which no transfer
runs: a store asks a segment's end and its length only as it opens.

## Descriptors

The engine's own test of it, `tests/shard_db_descriptors.rs` (a shard on maintenance workers and an
issuer with a worker for each core, descriptors counted after each checkpoint), on adacachy in a
container at its default soft limit of 2,048:

| puts | this branch: segments | descriptors | lean-doors' hyper-block: segment files | descriptors |
|---|---|---|---|---|
| 1,500 | 16 | 16 | 16 | 217 |
| 3,000 | 30 | 30 | 24 | 452 |
| 4,500 | 30 | 30 | 30 | 646 |
| 6,000 | 36 | 36 | 42 | 1,066 |
| 7,500 | 36 | 36 | 42 | 1,158 |
| 9,000 | 36 | 36 | 42 | 1,204 |
| 10,500 | 40 | 40 | 42 | 1,237 |
| 12,000 | 40 | 40 | 42 | 1,297 |

Five workers each time. On lean-doors' hyper-block the descriptors kept growing with no new
segment as more handles reached more segments; the test fails there at its first count (263 for one
segment the owner's handle knew). `ranges_room`, `ranges_growth_asker` and `store_segments` pass
there at that limit on this branch.

The bench, peak descriptors per case:

| | 1M | 10M | 100M |
|---|---|---|---|
| macOS, m_fix | at most 45 | 25-27 | 47-49, 40-63 segments |
| macOS, m_base | 3,220 | 3,904-4,280 | 10,240: `Too many open files` 11 s into the fill at 34-41 segment files, both rounds |
| Linux, m_fix | 12, 6 segments | 23-29, 17-23 segments | 45-48, 39-42 segments |
| Linux, m_base | 1,748-1,755 | 3,689-4,809 | 19,819-23,919 (at the runner's soft limit, 1,048,576) |
| Linux, m_old | 109-112 | 224-334 | |
| Linux container at 2,048, m_fix | | | completed: 43 segments, recovery exact |

macOS's 1M counts and its 10M and 100M counts of the 1-8 rounds are the descriptor table's size,
which bounds the open descriptors from above (a null buffer asks it); the 10M and 100M rows for
rounds r301 and r302 count open descriptors exactly.

## 100M at the default limits

| | fills/s | gets/s | seeks/s | segments | recovery |
|---|---|---|---|---|---|
| macOS at 10,240, m_fix, n=4 | 6,815,468 (6,335,525-6,892,511) | 2,219,982 (2,160,966-2,248,564) | 615,685 (591,931-631,710) | 40-63 | exact, 63,209,014 live keys |
| macOS at 10,240, m_base, n=3 | `Too many open files (os error 24)` in the fill | | | | |
| Linux container at 2,048, m_fix, n=1 | 2,067,243 | 2,570,089 | 548,386 | 43 | exact, 63,209,014 live keys |

## Against lean-doors, the A/A and RocksDB

### macOS

| 1M, n=8 | fills | gets | seeks |
|---|---|---|---|
| m_fix over m_base | 0.995 (0.957-1.058) | 1.008 (0.860-1.040) | 1.002 (0.972-1.024) |
| m_aa over m_base | 1.002 (0.926-1.034) | 0.988 (0.854-1.036) | 1.010 (0.931-1.073) |
| m_fix over rocks | 6.21x (5.94-6.32) | 7.21x (6.79-7.61) | 5.10x (4.73-5.53) |
| m_base over rocks | 6.28x (5.62-6.50) | 7.20x (6.64-8.85) | 5.10x (4.71-5.57) |

| 10M, n=8 | fills | gets | seeks |
|---|---|---|---|
| m_fix over m_base | 1.090 (0.779-1.327) | 1.004 (0.923-1.191) | 1.010 (0.962-1.155) |
| m_aa over m_base | 1.046 (0.783-1.354) | 1.018 (0.941-1.202) | 1.022 (0.952-1.130) |
| m_fix over rocks | 5.68x (4.64-6.67) | 10.81x (10.21-12.52) | 7.05x (6.69-8.13) |
| m_base over rocks | 5.04x (4.70-7.48) | 10.71x (10.29-11.56) | 6.95x (6.75-7.65) |

| 100M, n=4 | fills | gets | seeks |
|---|---|---|---|
| m_fix over rocks | 6.11x (5.66-6.25) | 8.41x (8.29-8.63) | 9.50x (8.25-11.44) |

CPU a put through the fill and its flush (`paid_fill` process CPU): m_fix over m_base 1.003
(0.967-1.032) at 1M, 0.968 (0.935-1.006, n=10) at 10M; m_aa 0.994 at both. Instructions a put, the
same span: 5,424-5,470 against 5,457-5,495 at 1M, 8,255-8,313 against 8,368-8,430 at 10M.

### Linux

| 1M, n=10 | fills | gets | seeks |
|---|---|---|---|
| m_fix over m_base | 1.023 (0.855-1.187) | 1.038 (0.968-1.094) | 1.005 (0.988-1.042) |
| m_aa over m_base | 1.001 (0.875-1.143) | 0.993 (0.944-1.074) | 1.003 (0.981-1.078) |
| m_old over m_base | 0.990 (0.878-1.094) | 1.004 (0.961-1.077) | 0.973 (0.961-1.023) |
| m_fix over rocks | 3.94x (3.56-4.56) | 4.85x (4.66-5.44) | 4.86x (4.81-4.95) |
| m_base over rocks | 3.92x (3.34-4.24) | 4.73x (4.45-5.03) | 4.86x (4.65-4.91) |

| 10M, n=10 | fills | gets | seeks |
|---|---|---|---|
| m_fix over m_base | 1.001 (0.584-1.710) | 0.980 (0.653-1.820) | 1.008 (0.547-2.083) |
| m_aa over m_base | 0.936 (0.685-1.911) | 1.022 (0.803-1.584) | 0.966 (0.758-1.616) |
| m_old over m_base | 1.006 (0.508-1.656) | 1.010 (0.575-2.658) | 0.979 (0.550-3.149) |
| m_fix over rocks | 3.24x (1.68-6.98) | 7.81x (5.30-15.92) | 6.52x (3.25-10.80) |
| m_base over rocks | 3.24x (2.73-6.40) | 8.43x (6.05-11.52) | 6.89x (3.79-9.59) |

The host's other work took all 32 threads through rounds 6-10 of the 10M batch, which widened every
arm's range; each ratio pairs arms of one round. CPU a put through the fill and its flush: m_fix
over m_base 1.009 (1M), 0.967 (10M); m_old 1.018 and 1.045 (0.575-1.370); m_aa 0.992 and 1.002.

| 100M, n=4 | fills | gets | seeks |
|---|---|---|---|
| m_fix over m_base | 0.719 (0.596-1.209) | 3.471 (0.312-5.608) | 2.039 (0.245-5.594) |
| m_aa over m_base | 0.857 (0.410-1.370) | 2.635 (0.722-5.198) | 2.003 (1.109-3.940) |
| m_fix over rocks | 2.38x (1.35-2.98) | 4.96x (1.42-12.47) | 7.08x (1.74-13.18) |
| m_base over rocks | 2.60x (2.03-4.43) | 1.90x (1.24-4.56) | 3.29x (2.36-7.11) |

At 100M the host's load at a case's start ran from 15 to 174, and one binary's own rounds spread
fills from 0.56 to 1.89 million a second (m_aa) and gets from 0.22 to 2.24 million: four rounds
resolve no difference the A/A's 0.41-1.37 does not hold. CPU a put through the fill and its flush,
which a frozen or descheduled process does not accrue: m_fix 2,902 ns (2,760-4,241), m_base 3,542
(2,339-3,559), m_aa 2,997 (2,765-3,920); m_fix over m_base 1.012 (0.780-1.217), m_aa 0.968. Peak
descriptors at 100M: m_fix 45-48 at 39-42 segments, m_base 19,819-23,919.

## The final build

The comparison ran builds of hyper-raft `07c03e7`; the vendored head `8a4d855` changes only a
segmented file's add and discovery, which a store runs as it grows a segment and as it opens. Its
build against `07c03e7`'s (`m_fix07`), interleaved:

| macOS | fills/s | gets/s | seeks/s | CPU a put, paid fill |
|---|---|---|---|---|
| 10M, n=4: m_fix (8a4d855) | 6,720,066 (5,723,264-7,082,526) | 3,378,521 (3,266,693-3,439,944) | 828,854 (822,531-840,897) | 867 ns (858-874) |
| 10M, n=4: m_fix07 | 5,621,042 (5,425,115-6,420,878) | 3,391,940 (2,962,367-3,425,118) | 805,917 (704,826-827,025) | 869 ns (864-878) |
| 10M, n=4: m_base | 5,573,412 (5,507,157-7,139,629) | 3,371,278 (3,331,289-3,415,770) | 828,598 (823,789-847,787) | 892 ns (877-902) |
| 100M, n=6: m_fix (8a4d855) | 6,522,415 (6,284,451-6,672,492) | 617,804 (466,050-2,258,785) | 399,156 (300,089-553,451) | 1,268 ns (1,185-1,310) |
| 100M, n=6: m_fix07 | 6,813,840 (6,108,536-6,928,959) | 1,852,841 (587,820-2,343,551) | 530,462 (370,828-601,621) | 1,203 ns (1,175-1,257) |

macOS's 100M gets follow the memory a case finds free as it starts (`--machine-memory`): another
program held memory through these rounds, and each round's gets went with its peak resident set,
both binaries alike (m_fix 534K/s at 30.4 GB and 2.26M/s at 36.5 GB; m_fix07 588K/s at 33.1 GB and
2.34M/s at 37.3 GB). On Linux the same check ran on a host at load 24 to 174 with two CI jobs, its
10M rounds (n=4) a third as fast as an hour before for every arm, so it measured the host. Its 100M
round, a case a binary starting at loads 266 and 123, completed both with recovery exact
(63,209,014 live keys), m_fix at 51 descriptors and 45 segments, m_fix07 at 59 and 53, and was
stopped there to give the host to hyper-raft's gates.

## The earlier 12%

The 2026-11-06 patch kept one open file a segment for a handle and its duplicates, a family for
each maintenance worker's file. It measured 0.88x lean-doors' fills at 10M (0.73-1.05, 6 rounds,
on adacachy in a container with the limit raised; its A/A 1.03, 0.98-1.12), cause untraced.

- The mechanism a shared open file has: every positioned read takes and gives back the open file
  description's reference and writes its read-ahead state, from every thread that shares it. In
  nothing but cached reads that costs 5-13% at 4 to 32 threads on Linux, and nothing on macOS
  (research/91). A duplicate shares the description too: the issuer's duplicates always did.
- That patch here, ten rounds at 10M on Linux: fills 1.006x lean-doors (A/A 0.936x), gets 1.010x,
  seeks 0.979x; CPU a put 1.045x (0.575-1.370), within what the A/A's spread allows.
- This change shares each segment's description among every handle of the file, more threads than
  the patch shared one among, and fills cost no more CPU a put than lean-doors' (0.967x Linux,
  0.968x macOS at 10M) nor ran slower past the A/A. So a shared description is not what a fill pays.

The 12% was not reproduced on adacachy2. adacachy, where it was measured, was held by another
session's heavy run (`~/heavy-run.lock`) throughout this work, and its comparison raised the
container's open-file limit, which this work does not do; so the patch was not re-run there.
