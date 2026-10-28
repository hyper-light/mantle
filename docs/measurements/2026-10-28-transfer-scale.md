# Transfer at scale: this Mac and the box, small sets, then large objects until a cell refuses one

2026-10-27 and 2026-10-28, branch `transfer`. Plan step 10 (research/76's harness, `mantle bench
transfer`).

**Question.** Plan step 10: on the owner's machines, does `mantle bench transfer` (research/76) hold
every check through both doors, from 1k small objects to 10 GB and 100 GB objects put until the
cell refuses one as out of space: every byte read back the generator's, every block's chunks on
distinct nodes, every acknowledged object read back after each founder is killed and started again,
and each large case ending at the typed refusal with the count held beside the volumes' stated
capacity?

**The machines.** adacachy is the bare-metal box (the owner was told on 2026-10-27 that they are one
machine): AMD Ryzen 9 9950X3D2, 16 cores and 32 threads, 122 GiB, `/home` on btrfs on a Samsung
9100 PRO NVMe, Linux 7.2.9-2-cachyos, a kiln runner that other agents' CI shares ("the default is a
noisy machine", the owner, 2026-10-27). This Mac: Apple M5 Max, 128 GiB, APFS on the internal SSD,
macOS 26.4.1, the owner's working machine with its other agents.

**Method.** Each case is a fresh cell of three that `mantle init` makes on loopback, its founders in
three failure domains, the small sets at seed 1 on volumes stated to hold the set (a volume is
written whole at format where the file system's first writes cost more, so a volume sized from free
space would write some 0.9 TB on the box before its first object), the large cases on volumes
stated from the machine's free space less the owner's reserve: about 30 GB on the Mac, 1 TB on the
box (`--reserve 1T`, 1.1 TB). On the box every case runs under a `systemd-run --user --scope` of
`MemoryMax=64G` (the owner: 64 GB first, then 32 and 16 GB as the runs do well, each halving
focused further on throughput, latency, allocations, page faults and thrashing); every five seconds
the scope's memory (current, peak, anon, file, swap, PSI, events) and each of its processes' peak
resident set, threads and faults are sampled. On the Mac the harness's resident set and threads
are sampled from `ps`.

**Measured: the small sets.** Seed 1, every object through its own door and read back through
both, each founder then killed and started again with every object read through the others and
through it. Latencies are each door's own objects: its puts, then their reads natively and over
HTTP.

| Machine (binary) | Case | Volume | Objects | Put p50 native/http | Put p99 native/http | Native read p50/p99 (native objects) | HTTP read p50/p99 (native objects) | Reads after kills | Asked again | Wrong / missing | Seconds |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Mac (75e9e9d6\*) | small 1000 | 256 MiB | 1,000 | 107.0 / 104.9 ms | 230.7 / 180.4 ms | 4.2 / 24.6 ms | 5.0 / 25.2 ms | 6,000 | 0 | 0 / 0 | 255 |
| Mac (75e9e9d6\*) | small 10000 | 2 GiB | 10,000 | 104.9 / 104.9 ms | 419.4 / 243.3 ms | 4.6 / 25.2 ms | 5.5 / 47.2 ms | 60,000 | 0 | 0 / 0 | 2,485 |
| Mac (75e9e9d6\*) | small 100000 | 16 GiB | 100,000 | 102.8 / 107.0 ms | 394.3 / 327.2 ms | 4.7 / 25.2 ms | 4.0 / 24.6 ms | 600,000 | 1 | 0 / 0 | 14,375 |
| box, 64 GB scope (30b68641, 219c4d5d) | small 1000 | 256 MiB | 1,000 | 132.1 / 134.2 ms | 570.4 / 453.0 ms | 2.2 / 8.9 ms | 3.5 / 11.8 ms | 6,000 | 0 | 0 / 0 | 237 |
| box, 64 GB scope (30b68641, 219c4d5d) | small 10000 | 2 GiB | 10,000 | 111.1 / 107.0 ms | 687.9 / 494.9 ms | 1.1 / 3.9 ms | 3.3 / 18.9 ms | 60,000 | 0 | 0 / 0 | 1,665 |
| box, 64 GB scope (30b68641, 219c4d5d) | small 100000 | 16 GiB | 100,000 | 81.8 / 86.0 ms | 285.2 / 360.7 ms | 1.0 / 4.0 ms | 1.8 / 5.5 ms | 600,000 | 1 | 0 / 0 | 11,167 |

\* The Mac's small sets ran on a release build of branch `transfer` on 2026-10-27 whose commit was
not recorded beside its md5; the box's on 219c4d5d.

Every case: each block's chunks on distinct nodes (999, 9,999 and 99,998 blocks), each node's
chunk bytes exactly what it received over QUIC and put itself, and the three controls caught
(a byte inverted, a chunk moved onto another's node, a key never put).

Memory and threads. On the Mac the harness peaked at 17.1, 17.3 and 15.3 MiB resident with 4
threads. On the box, sampled every five seconds inside the scope:

| Case | Scope peak | PSI some avg10, most | Load | Node VmHWM, most | Node threads | Harness VmHWM / threads | boto3 VmHWM | Node major faults, most | OOM events |
|---|---|---|---|---|---|---|---|---|---|
| small 1000 | 107.8 MiB | 0.00 | 56-90 | 45.5 MiB | 43 | 15.4 MiB / 4 | 54.5 MiB | 2,080 | 0 |
| small 10000 | 139.5 MiB | 1.44 | 31-103 | 45.1 MiB | 43 | 14.3 MiB / 4 | 54.6 MiB | 28,112 | 0 |
| small 100000 | 1,613.7 MiB | 0.12 | 8-515 | 410.1 MiB | 43 | 13.9 MiB / 4 | 54.7 MiB | 913,633 | 0 |

**Measured: large objects.** Each case a fresh cell, its volumes stated from the free space at the
case's start less the reserve, the ways in turn (the native door's single upload in runs, the AWS
CLI's streamed single upload, the native door's explicit parts, boto3's parts) until each door
refuses an object as out of space.

This Mac (binary 1c7af6eb, 8729d338; launchd job at the shell's descriptor limit, 1,048,576):

| Case | Volumes (each) | Held | Put / native read / HTTP read | Refused | Chunks held a node |
|---|---|---|---|---|---|
| 10G | 17,763,591,509 B (85.5 GB free less 32.2 GB) | 1 (native) | 33 / 199 / 369 MiB/s | CLI and native parts `InsufficientStorage`; none otherwise | 10,949,740,528 B, 1 free segment |
| 100G | 17,730,831,701 B | 0: one 100 GiB object needs 107 GB a node | | both doors at their first object, typed | 15.8 GB of the refused object's chunks, kept by none of its records |

Both exact: the 10G case's 2,048 blocks on distinct nodes, its object read back after each founder's
kill and restart (6 reads, 0 wrong or missing), its controls caught; the 100G case's refused objects
unreadable and unlisted, the key never put read missing. At today's free space a volume holds one
10 GB object after the three copies, whatever the packing, which is what plan step 10 expected.

**Where a full cell's room goes.** A cell is full when one block's chunks find no node with room:
every block needs a chunk on each of the three nodes, so the fullest node decides. A node's
volume refuses a chunk `Full` for one of two things (chunk-store.md §5, §8): no free segment past
the cleaner's reserve, or its index's fragments at their budget. 8729d338 has each node say both.
A probe of 2 GiB objects on stated 8 GiB volumes (this Mac, four runs of the scratch build
66a87312/a9639fc1, never merged, which also printed every refused chunk) ended exact each time,
refused typed on both doors, the only refused chunks the volumes' `Full`:

| Run | Held a node (GB) | Fragments of budget | Free segment bytes (fullest node) |
|---|---|---|---|
| 1 | 6.12 / 6.13 / 6.13 | 1,358 of 398,012 | 8,404,992 |
| 2 | 6.02 / 6.00 / 6.00 | 1,330 of 398,012 | 8,404,992 |
| 3 | 5.92 / 5.93 / 5.92 | 1,310 of 399,531 | 8,404,992 |
| 4 | 5.85 / 5.86 / 5.86 | 1,293 of 399,608 | 8,404,992 |

The index was never the limit (under 0.4% of its budget); the segments were, and every free figure
is a whole multiple of 8,404,992 bytes, the segment `derive` makes: "the least segment holding a
record of the largest chunk whole", the largest chunk being the gateway's `layout::CHUNK`, 8 MiB.
A record never spans segments, and a native upload's runs are S3's least part, 5 MiB, for any
object under about 50 GB (`name::run_length`: "a run is the smallest these bounds allow"; each
run closes a block). A 5 MiB chunk's record (5,244,160 bytes) leaves room for no second in its
segment, so each takes a segment alone: 5,244,160 / 8,404,992 = 62.4% of the segments hold bytes.
That is where this Mac's 10G cells were refused in three runs on three binaries (62.1, 62.4 and
62.6% of each volume held), while a 100G native object, cut into runs of 10.75 MiB (an 8 MiB block
that fills a segment and a 2.75 MiB block that packs), filled 89 to 94%. S3 parts of 8 MiB (the
AWS CLI's default) cut into a 127-segment block that fills a segment and a 64 KiB block that packs.

research/11 §7.4 bounds this waste: a segment at least `max_record / δ`, so that "a record that
does not fit wastes ≤ δ of a segment" (its illustration: 8 MiB chunks in 256 MiB segments, δ ≈ 3%).
`derive` keeps the record bound and drops δ, so the waste is bounded only by the record itself.

**Native runs in whole blocks.** A run ends its file's last block, so `name::run_length` now cuts
runs in whole blocks of the layout new blocks take (127 segments, 8,323,072 bytes, under three
copies), or in whole segments where the client's buffer holds no block. The same probe on that build
(binary 979f1bfe):

| Build | Objects held | Held a node (GB) of 8.59 | Fragments | Free segment bytes |
|---|---|---|---|---|
| runs of 5 MiB (four runs above) | 2 | 5.85 to 6.13 | 1,291 to 1,358 | 8,404,992 (fullest) |
| runs of whole blocks | 3 | 8.21 / 8.16 / 8.21 (95.6%) | 1,691 to 1,697 | 151,289,856 / 201,719,808 / 159,694,848 |

The cell held a third object more and its nodes 2.1 to 2.3 GB more each, every block's chunks on
distinct nodes and every object read back exact after each founder's kill and restart. The case was
not exact: its three refusals (l3 by boto3, l4 natively in runs, l5 by the CLI) were `503 SlowDown`,
not the typed `InsufficientStorage`, each node keeping 18 to 24 free segments at the end, past the
one segment kept from client writes (`CLEANER_RESERVE`). Which path answered `SlowDown` (the
gateway's admission, a node at its job bound, or a block whose chunk was refused for other than
`Full`, `PutError::Unplaced`) is being traced; one such refusal had come before in five runs of the
5 MiB build (a `SlowDown` on l2, native parts).

**Where a small put's time goes.** A diagnostic build (never merged) timed each put on the node
that served it, on kiln-a094a12d (300 objects, 170 traced puts): the body is in at 0.4 ms; the
three chunk copies go in parallel and the slowest is durable at 5.9 ms (each 5.0 ms at the median);
then the Block, File and Name commands commit one after another, 11.4, 11.5 and 11.4 ms at the
median (p90 about 14 ms each), and the reply leaves 0.03 ms after the last. The median put took
40.2 ms, 34 of them in the three commits. A commit costs about two durable writes where a chunk's
costs one.

Why a commit costs two durable writes: the log answers an update only once a later durable record
confirms its frame's flush, the next frame's or a confirmation written on its own when no frame
follows at once, so that recovery can tell a torn tail from damage (raft-log.md §6; AGL+18 §3.3.3;
docs/measurements/2026-09-29-log-confirmation.md). On kiln-a094a12d `mantle bench log` measured
one durable write (4 KiB, then a full flush) at 2.88 ms and a closed-loop 1 KiB append at 5.77 ms
p50 (8.65 ms p99), four replicas sharing each flush at the same latency. A small put asks its
Block, File and Name commands one after another, each answered before the next is asked, so no
frame ever follows another at once and each commit pays both flushes: six flushes and the chunks'
one, in turn, about 40 ms on that machine (83 ms on adas-mac-studio-2, where `F_FULLFSYNC` makes a
flush about 10 ms).
