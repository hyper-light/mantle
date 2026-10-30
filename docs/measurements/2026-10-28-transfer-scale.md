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

A case at this Mac's free space holds one 10 GB object, through its first way alone, so each way
ran a case of its own (`--ways`, binary 57146c75, 28277261, native runs of 5 MiB):

| Way | Volumes (each) | Held | Put / native read / HTTP read (MiB/s) | Refused | Chunks held a node | Exact |
|---|---|---|---|---|---|---|
| native (runs) | 17,842,323,456 B | 1 | 33 / 202 / 299 | `InsufficientStorage` (l1) | 10.98 GB, 61.5%, 1 free segment | yes |
| cli | 17,849,536,512 B | 1 | 62 / 140 / 364 | l1, l2 `SlowDown` | 17.28 GB, 96.8% | no |
| native-parts | 17,818,162,517 B | 1 | 35 / 167 / 427 | l1, l2 `SlowDown` | 17.06 GB | no |
| boto-parts | 17,815,586,133 B | 1 | 35 / 136 / 293 | l1, l2 `SlowDown` | 17.39 GB | no |

Every way's object read back exact through both doors and after each founder's kill and restart (6
reads each, 0 wrong or missing), its blocks' chunks on distinct nodes (2,048 blocks natively in
runs, 2,560 in 8 MiB parts) and its controls caught. The three ways in 8 MiB parts ended at
`503 SlowDown`, not the typed refusal: below, where a full cell's room goes.

The box (binary ddaeea3f, dfe7399b, its native runs still S3's least part; under the 64 GB scope):

| Case | Volumes (each) | Held | Put / native read / HTTP read (MiB/s) | Chunks held a node | Seconds |
|---|---|---|---|---|---|
| 10G | 295,415,316,480 B (1.986 TB free less 1.1 TB) | 21: native 6, CLI 5, native parts 5, boto3 parts 5 | native 40 / 293 / 565; CLI 65 / 199 / 542; native parts 40 / 218 / 600; boto3 46 / 226 / 593 | 226.3 GB, 76.6% | 11,265 |

Exact: 50,688 blocks, each block's chunks on distinct nodes; every node's chunk bytes what it
received and put; 126 reads after each founder's kill and restart, 0 wrong or missing; the controls
caught; refused typed on both doors (native l22, the listener l21), none otherwise; one native part
asked again. The scope peaked at 54.1 GB, the page cache of the reads (360 MB anonymous), its memory
PSI at 0 and no OOM event; a node at most 199 MiB resident and 43 threads, the harness 13.6 MiB and
4. 76.6% is the native objects' 5 MiB chunks at 62.4% of their segments beside the rest's at 99%.

The box under a 32 GB scope, the owner's first halving: the small sets on 913abd1a (binary
bd631dac), the 10G case on the planned pass, fb57d055 (binary 63b26d90), the case on 913abd1a
superseded by it part way through its fill.

| Case | Volume | Objects | Put p50 native/http | Put p99 native/http | Native read p50/p99 | HTTP read p50/p99 | Reads after kills | Asked again | Seconds |
|---|---|---|---|---|---|---|---|---|---|
| small 1000 | 256 MiB | 1,000 | 60.8 / 64.0 ms | 146.8 / 111.1 ms | 0.57 / 1.97 ms | 0.87 / 2.16 ms | 6,000 | 0 | 125 |
| small 10000 | 2 GiB | 10,000 | 66.1 / 69.2 ms | 146.8 / 167.8 ms | 0.57 / 1.31 ms | 0.82 / 2.23 ms | 60,000 | 0 | 878 |
| small 100000 | 16 GiB | 100,000 | 69.2 / 71.3 ms | 146.8 / 155.2 ms | 0.56 / 1.25 ms | 0.79 / 1.51 ms | 600,000 | 1 | 7,837 |

| Case | Scope peak | Node VmHWM, most | Node threads | Node major faults, most | Harness VmHWM / threads | OOM events |
|---|---|---|---|---|---|---|
| small 1000 | 102.4 MiB | 39.1 MiB | 43 | 948 | 13.6 MiB / 4 | 0 |
| small 10000 | 222.7 MiB | 41.8 MiB | 33 | 3,177 | 14.2 MiB / 4 | 0 |
| small 100000 | 1,921 MiB | 470.3 MiB | 43 | 240,110 | 13.0 MiB / 4 | 0 |

The 10G case, its volumes 278,787,130,709 bytes each (1.936 TB free less 1.1 TB):

| Way | Held | Put / native read / HTTP read (MiB/s) | Refused |
|---|---|---|---|
| native (whole blocks) | 7 | 66 / 347 / 592 | |
| cli | 6 | 91 / 235 / 539 | the listener l25, typed |
| native-parts | 6 | 50 / 231 / 579 | native l26, typed |
| boto-parts | 6 | 52 / 229 / 574 | |

Exact in 11,953 s: 25 objects, 55,117 blocks each on distinct nodes; each node holding
275,263,860,736 bytes of 278,787,130,709 (98.7%) with one segment free, every byte what it received
and put; 150 reads after each founder's kill and restart, none wrong, missing or asked again; the
controls caught; refused typed on both doors and none otherwise. The scope peaked at 1.02 GiB, its
swap at 405 MB (zram) and no OOM event; a node at most 274 MB resident with 33 threads and 92,568
major faults, the harness 14 MB and 4 threads, boto3 and the AWS CLI 172 MB. The 64 GB pass's
10G case, before whole-block runs, the whole-record stream and the planned pass, held 21 objects
in 76.6% of its volumes.

The 100G case on the same build, its volumes 230,959,255,552 bytes each (1.792 TB free less
1.1 TB):

| Way | Held | Put / native read / HTTP read (MiB/s) | Refused |
|---|---|---|---|
| native (whole blocks) | 1 | 67 / 413 / 665 | |
| cli | 1 | 90 / 317 / 656 | |
| native-parts | 0 | | native l2, typed |
| boto-parts | 0 | | the listener l3, typed |

Exact in 9,027 s: 32,101 blocks each on distinct nodes; each node holding 227.65 GB of 230.96 GB
(98.6%) with one segment free, every byte what it received and put; 12 reads after each founder's
kill and restart, none wrong, missing or asked again; the controls caught; refused typed on both
doors and none otherwise. The scope peaked at 7.8 GB, its swap at 314 MB, no OOM event; a node at
most 153 MB resident with 43 threads and 21,698 major faults, the harness 13.6 MB. The 64 GB pass's
100G case held its two objects in 93.3% of 287.9 GB volumes. The 16 GB pass on 750a1525 held its 1k set
exact and failed its 10k set in the kills (`Budget`, the client change below, reverted), and ran
again on d5eadd39 (binary 5415eb0a), every case under the same scope:

| Case | Volume | Objects | Put p50 native/http | Put p99 native/http | Native read p50/p99 | HTTP read p50/p99 | Reads after kills | Asked again | Seconds |
|---|---|---|---|---|---|---|---|---|---|
| small 1000 | 256 MiB | 1,000 | 66.1 / 54.5 ms | 115.3 / 83.9 ms | 0.54 / 1.34 ms | 0.84 / 1.41 ms | 6,000 | 0 | 120 |
| small 10000 | 2 GiB | 10,000 | 73.4 / 77.6 ms | 159.4 / 213.9 ms | 0.72 / 2.62 ms | 1.25 / 2.62 ms | 60,000 | 2 | 1,059 |
| small 100000 | 16 GiB | 95,295 of 100,000 | 69.2 / 113.2 ms | 838.9 / 671.1 ms | 0.90 / 5.51 ms | 1.67 / 9.96 ms | 571,770 | 2 | 16,341 |

| Case | Scope peak | PSI some avg10, most | Load | Node VmHWM, most | Node threads | Node major faults, most | OOM events |
|---|---|---|---|---|---|---|---|
| small 1000 | 119.3 MiB | 0.00 | 8-20 | 45.0 MiB | 33 | 2 | 0 |
| small 10000 | 234.3 MiB | 0.00 | 5-25 | 51.1 MiB | 43 | 16,962 | 0 |
| small 100000 | 1,846.4 MiB | 8.30 | 7-423 | 466.3 MiB | 43 | 577,113 | 0 |

The 1k and 10k sets were exact. The 100k set was not: the S3 listener refused 4,705 of its 50,000
puts, and the harness counted them without their status (a small case now says each status with
its count, f89bcd60). Each node held 293 to 307 MB of chunks no file keeps, the refused puts' bytes
at the set's 65.6 KB mean (309 MB): their chunks were written and the puts refused after. Every
acknowledged object read back exact through both doors and after each founder's kill, each block's
chunks on distinct nodes. The run met memory pressure the passes before it did not: PSI some at
most 8.30 where the 64 GB pass's 100k set saw 0.12, a node's major faults at 577,113 with the
scope's swap at 1.08 GB, load to 423 from the box's other work. Starved of CPU alone, a cell
refused no put: the 10k set on adas-mac-studio-2 beside 28 and 70 busy loops (load 33 and 76 on 14
CPUs) was exact both times, and on GitHub's 4-CPU runners (branch `diag/refusal-ci`, never merged,
run 40989666794) beside 16, 32 and 64 busy loops, load 19 to 70, five runs were exact, none asked
again, the puts' p99 90 to 248 ms. Nor did a memory limit reproduce it (run 41002528646, the cell
in one systemd scope with swap on), and it showed two things of its own. Three founders in one
scope each took the whole limit as their allowance (node.md's allowance: a stated ceiling, else the
OS's limit), so under 128 and 96 MiB each refused to start, its floor of 825 to 1,382 MB past the
limit; the harness states no founder its share. And under 1 GiB the floor left each volume's index
about 33,000 chunks: past them every put was refused `InsufficientStorage` with most of the disk
free, 16,555 of the native door's 50,000 and all of the listener's, the 33,444 held exact.

The 10G case, its volumes 141,446,945,450 bytes each (1.524 TB free less 1.1 TB, the box's free
space falling with its other work):

| Way | Held | Put / native read / HTTP read (MiB/s) | Refused |
|---|---|---|---|
| native (whole blocks) | 4 | 31 / 138 / 228 | |
| cli | 3 | 43 / 105 / 248 | the listener l13, typed |
| native-parts | 3 | 25 / 98 / 261 | native l14, typed |
| boto-parts | 3 | 27 / 121 / 293 | |

Exact in 11,551 s: 13 objects, 28,204 blocks each on distinct nodes; each node holding
139,821,891,584 bytes of 141,446,945,450 (98.85%) with one segment free; 78 reads after each
founder's kill and restart, none wrong, missing or asked again; the controls caught; refused typed on
both doors and none otherwise. The scope peaked at 1.40 GiB, PSI some at most 0.51; a node at most
129.7 MiB resident with 43 threads and 254,393 major faults, the AWS CLI 175 MiB. The 100G case's
volumes were 92,559,448,746 bytes each (1.377 TB free less 1.1 TB), less than one 100 GiB object
takes a node: none held, both doors refused their first object typed (native l0, the listener l1),
in 1,855 s, the scope at 505 MiB. By 2026-10-29 the box's free space was 737 GB, under the 1 TB the
owner keeps: no large case runs there until space frees.

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
one segment kept from client writes (`CLEANER_RESERVE`). A scratch build (never merged; binaries
fe237607 and 09737e00) printed every refused chunk, every `SlowDown`'s path, each put a volume held
for its cleaner's verdict and each verdict:

| Probe run | `SlowDown`s by path | Chunk refusals | Puts held | Cleaner passes |
|---|---|---|---|---|
| fe237607 | 16 `Unplaced`, 0 admission | 14 `Busy`, 2 `Lost(Stalled)`, 0 `Full` | (not printed) | (not printed) |
| 09737e00 | 12 `Unplaced`, 0 admission | 9 `Busy`, 7 `Lost(Budget)`, 2 `Lost(Stalled)`, 0 `Full` | 7,508 | each gained 1 to 45 segments, relocating 2 records a segment, in 45 ms to 2.9 s; no dead bytes |

The cell was not full when it answered `SlowDown`: its cleaners kept freeing segments. An S3 part of
8 MiB is cut into a 127-segment block and a 64 KiB block; a segment holding a few 64 KiB records
cannot take the next 8.3 MB record, so it is sealed nearly empty, and near full each pass frees
such segments by moving their small records, gaining space, so the volume never comes to the
verdict `Full` and holds each put for the next pass (research/75). The writer holds them with the
room they take of its queue: the chunks past that room are refused `Busy`, those past the
transport's chunk budget `Lost(Budget)`, and those held past the transfer's progress period
`Lost(Stalled)`; the put's driver takes each for a cause that may pass, and with no other volume
for the block's chunk fails it `Unplaced`, answered `503 SlowDown` (node.md §4.4). The 5 MiB build
met it once in five runs: its native records, one to a segment, leave nothing a pass can pack.

**A record longer than half a segment in a stream of its own.** Such a record shares a segment with
no record of its size, so the writer now gives it a stream of its own, and small records keep the
segment they began. The same probe (binary 7563025f, 36f8912e):

| Build | Objects held | Held a node (GB) of 8.59 | Free segments | Refused |
|---|---|---|---|---|
| runs of 5 MiB | 2 | 5.85 to 6.13 | 1 (fullest) | typed |
| runs of whole blocks | 3 | 8.16 to 8.21 | 18 to 24 | 3 `SlowDown` |
| whole blocks and the whole-record stream | 3 | 8.36 on each node (97.3%) | 1 | typed on both doors (native l4, the listener l3), none otherwise |

Exact: 1,283 blocks, each block's chunks on distinct nodes, 18 reads after each founder's kill and
restart with none wrong or missing, the controls caught.

Each way's own 10G case on that build (binary 7563025f, 5e94d09a, this Mac's free space less 30 GB):

| Way | Volumes (each) | Held | Put / native read / HTTP read (MiB/s) | Refused | Chunks held a node |
|---|---|---|---|---|---|
| native (whole blocks) | 16,377,375,402 B | 1 | 29 / 142 / 242 | `InsufficientStorage` (l1), exact | 15.98 GB, 97.6%, 1 free segment |
| cli | 12,679,972,181 B | 1 | 29 / 125 / 370 | l1, l2 `SlowDown` | 12.36 GB, 97.5% |
| native-parts | 12,625,889,962 B | 1 | 26 / 150 / 286 | l1, l2 `SlowDown` | 12.31 GB, 97.5% |
| boto-parts | 14,719,268,181 B | | | (hung in the kills, below) | 14.36 GB, 97.5% |

A node holds more than its one object: an upload refused part way keeps what it stored for the
upload's resumption. Every way's object read back exact after each founder's kill and restart, and
the parts' ways still ended at `SlowDown`. A scratch build (binary c26d8e6e on 5e94d09a, never
merged), 512 MiB objects by the CLI on stated 2 GiB volumes, printed why:

| Cleaner passes (one a node) | Puts held for a verdict | Chunks refused | Answer |
|---|---|---|---|
| each gained 0 segments, relocated 357 records, took 9.5 / 9.9 / 10.0 s, dead bytes 0 | about 1,530, every one before the first verdict (`futile_at` unset) | 8 `Busy`, 2 `Lost(Budget)`, 2 `Lost(Stalled)`, 2 `Full` | l3 `SlowDown` (9 blocks `Unplaced`), l4 `InsufficientStorage` |

A fresh volume holds puts until its cleaner's first verdict (chunk-store.md §8). A part's 8.3 MB
record sits alone in its segment with 77 KiB beside it, which the victims' estimate counted as dead
space, so the first pass near full moved such records one after another, each into a segment of its
own, for ten seconds before it found nothing gained; the puts it held filled the writer's queue and
the transport's budget, and those past them were refused as causes that may pass. A pass now lays
out its relocations from the index's record lengths before it moves one, the writer's own placement
rule, and cleans only victims whose records take fewer segments than they free (chunk-store.md §8).
In the chunk store's tests a volume filled with records longer than half a segment, each with room
beside it, is cleaned by no pass, background or asked for, and every pass that moves a record gains
a segment.

The probe again on the planned pass (binary 6118bc8e, fb57d055), 512 MiB objects on stated 2 GiB
volumes, each way a case of its own, beside the two diagnostic builds and the repro loops below on
the same machine (load 12 to 23):

| Way | Objects held | Held a node of 2.15 GB | Free segment bytes | Refused | Exact | Seconds |
|---|---|---|---|---|---|---|
| cli | 3 | 2,013,757,440 B on each | 8,404,992 | the listener l3 typed, none otherwise | yes | 320 |
| native-parts | 3 | 2,013,757,440 B on each | 8,404,992 | native l3 typed, none otherwise | yes | 488 |
| boto-parts | 3 | 2,013,691,888 B on each | 8,404,992 | the listener l3 typed, none otherwise | yes | 516 |

Each held what the morning's probe held, to the segment, and ended at the typed refusal where the
morning's ended `SlowDown`; every block's chunks on distinct nodes, 18 reads after each founder's
kill and restart with none wrong, missing or asked again, the controls caught.

**A read that waited two hours.** The boto-parts case on the stream build filled its volumes
(14.36 GB held a node, 97.5%) and then stopped in its kills: the second founder killed and started
again at 08:22:39, the harness's native read of l0 through it, its first, was still waiting at 10:22.
The node was idle (1,875 of 1,893 samples parked in its runtime's `kevent`); a fresh client's `head`
of l0 through it answered at once and its whole read came back in about 40 s. The harness had opened
a fresh client and asked it with no deadline, and with nothing measured the client gave its first
exchange no progress period either: a node that dies is found by its connection's idle timeout, but
one alive that never answers keeps its connection. And an exchange a node took and lost was asked
again of the same node. Giving every exchange at least the node's serve period, asking a lost one
of the next node and a get cut before its bytes came again (750a1525, 2289f3e2; a test of a node
that holds a get showed each half) did not hold under load: a node serving a get while a peer is
dead asks that peer for its chunks first, 0.5 to 2 s an ask, so it began its answer after two
periods, the cuts were asked again elsewhere, and the client's transport was refused `Budget` (CI
40794052095's linux-aarch64-1cpu, the filled cell of `tests/transfer.rs`; adacachy's 16 GB pass,
small 10000 in its kills), where the builds before passed. Both are reverted (b9b2a5b8). The wait
stays open: a client cannot tell a node working toward its answer from a stuck one while the node
sends nothing as it works and its own asks have no bound; it needs the node to state its progress
(STATUS item 8) and to stop asking a dead peer for chunks.

Why the node never answered is not found. Loops of the case (diagnostic builds of branch
`diag/kills-hang`, never merged: every slow or unplain ask, every ask held 2 s, each connection's
events and each read's replies, a trace file a process) on this Mac before 11:51, under load from
the gates beside them (load 18 to 23), found a founder started again whose first range reads waited
12 to 25 s with their range's leader known: its exchanges to the leader could not open, and one
read's three attempts all went to it, the third cut `Stalled` after 40 s, so the harness's read
failed. On adas-mac-studio-2, about 51 rounds of one 256 MiB object and 7 of 1 GiB objects filled
to refusal, quiet and beside twelve busy loops (load about 15), met no such wait. The loops after
11:51 on this Mac are void: its lid closed then ('Clamshell Sleep') and it slept most of every
quarter hour since ('Dark Wake Thermal Emergency', `pmset -g log`), every process frozen together,
which a first reading of their traces took for a runtime that lost its wakeups.

**A read racing the cleaner.** A chunk store read finds its fragments in the index and reads them
after its turn at the device; a record the cleaner moved meanwhile, its segment freed and written
again, failed its check where the read found it and was answered `Corrupt` with the chunk whole
elsewhere on the volume (shown in-crate: found at segment 0, moved by a pass to segment 3, segment 0
written again). A failed check is now weighed as the scrubber weighs one, and the read made again
where the chunk lies (d5eadd39, chunk-store.md §7).

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
