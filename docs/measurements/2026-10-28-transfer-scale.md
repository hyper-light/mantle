# Transfer at scale: this Mac and the box, small sets, then large objects until a cell refuses one

2026-10-27 and 2026-10-28, branch `transfer`. Plan step 10 (research/76's harness, `mantle bench
transfer`).

**Question.** Plan step 10: on the owner's machines, does `mantle bench transfer` (research/76) hold
every check through both doors, from 1k small objects to 10 GB and 100 GB objects put until the
cell refuses one as out of space: every byte read back the generator's, every block's chunks on
distinct nodes, every acknowledged object read back after each founder is killed and started again,
and each large case ending at the typed refusal with the count held beside the volumes' stated
capacity?

**Checkpoint, 2026-11-02.** The reader fix is on main at `a57eaea7`. Eleven of the 22 host/case
slots have complete readback evidence with their build attribution below; all six small sets
and all four Box 10 GiB upload ways passed. Five 100 GiB cases have clean capacity-only results:
all four Box ways and the new Mac S3 multipart case. Six Mac slots remain unresolved. The
earlier Mac generic refusal and Box CLI failure remain recorded and unexplained; their first
queues stopped. The fresh Box CLI case passed with the verified CLI acknowledgment check.
The Mac 10 GiB native case stopped at its existing disk guard without final integrity totals.
Capacity-only zero-object results receive no whole-object readback credit.
The full Box workspace gate still has its recorded engine
file-limit failure; the client, vendor and scale results retain their separate scopes.
All four Box 10 GiB upload ways now have complete results from separate cells.

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
free, 16,555 of the native door's 50,000 and all of the listener's, the 33,444 held exact. The
floor each founder said on adas-mac-studio-3 (one 2 GiB volume each, no limit):

| Floor class | One founder | The other two | Its basis |
|---|---|---|---|
| objects | 83.5 MB | 83.5 MB | two operations of the largest blocks |
| volumes | 147.1 MB | 147.1 MB | each volume's buffers (`layout::most_bytes`) |
| engines | 0.8 MB | 0.8 MB | memtables and store buffers |
| ranges | 279.3 MB | 268.5 MB | each range's member at hyper-raft's limits |
| log | 225.9 MB | 225.9 MB | the node's shared log |
| transport | 88.2 MB | 77.0 MB | the transport's budget and what it holds beside it |
| threads | 549.5 MB | 52.4 MB | each thread's 2 MiB stack, counted reserved whole |
| total | 1,374 MB | 855 MB | |

The floor is what a node may hold at its bounds, not what it holds: a node's resident peak on the
box under 16 GB was 466 MB at 100k objects. One founder started 262 threads where the others
started 25, its device's issuer sized by the depth its own calibration measured of the same SSD.

The 100k set again on 9cbe191a (step 1's bound on a node's waits, node.md §5.4; the harness saying
each refusal's status), 2026-10-30, under the same 16 GB scope, in 21,656 s, the scope's peak
1,653.3 MiB:

| Door | Objects | Refused | Asked again | Wrong | Missing |
|---|---|---|---|---|---|
| native | 50,000 | 11, `SlowDown` | 4 | 0 | 0 |
| S3 listener | 50,000 | 4,703, `InsufficientStorage` | 0 | 0 | 0 |

Every acknowledged object read back exact through both doors and through each founder killed and
started again (571,716 reads, 1 asked again), each of 95,284 blocks' chunks on distinct nodes, the
controls caught. The listener's refusals are the index's: each volume held 95,297 fragments of the
95,297 it may, with 10.58 GB of free segments, and 100,000 less 95,297 is 4,703. A founder of the
same cell started under the same scope on the box says why in its basis lines: of the 16.3 GB its
16 GiB allowance leaves past the floor, each of the five ranges' engines took 3.25 GB for resident
metadata (`engine.metadata_bytes`), its share of the node directory's whole file system, 8.0 TB
(`dir.bytes`), and the volume's index 33.6 MB, 95,358 fragments (`volume.1.index_bytes`,
`max_fragments`). `derive::shares` gives each the same part of its full-size want, the engines' want
the file system's, which the cell's metadata never fills, and the index's its volume's, which the
objects fill: the index fills while the engines' 16.3 GB of room holds a few megabytes. The
volume's log, sized at format for that index, holds it there for the volume's life
(`derive::held`). The native door's 11 refusals were `503 SlowDown`, which the harness's native
door returned where boto3 and the AWS CLI ask again (research/75). Their path was not traced:
step 1's end of a range step that waited its budget without a leader to serve it, or a block
`Unplaced` once a chunk was cut `Lost(Stalled)`, which a diagnostic build found under load on
2026-10-31 (below).

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

The 100G case again below that reserve, as the owner chose on 2026-10-31, on 3e88d710 (binary
e75c393e), under the same scope, its volumes 161,714,373,973 bytes each (622.6 GB free less
128 GiB): one object fits a node, so the case asked three (l0 natively in runs, l1 by the CLI, l2 in
native parts) and boto3 had no turn. None was acknowledged: each was answered `503 SlowDown` part
way, the nodes keeping 13.4 GB of chunks each that no record keeps, in 1,148 s at load 45 to 95. A
diagnostic build (branch `diag/large-slowdown`, 5639a8c4 on 1ac2509d, binary d782703c, never
merged: each `SlowDown`'s path and each refused chunk on a node's standard error, every line a node
says kept by the harness with the wall clock) put 40 GiB objects on stated 50 GiB volumes beside it:

| Seconds into l0 | What the node serving it said |
|---|---|
| 32.5 | a chunk to a peer refused `Lost(Stalled)` |
| 32.7 | `SlowDown`, block 64e1cbbd `Unplaced` |
| 116.5 | a chunk to a peer refused `Lost(Stalled)` |
| 119.0 | `SlowDown`, block aec62ba2 `Unplaced`; the client's third attempt ended `Closed` |

No link changed its judgment in either window. All three nodes were waiting on their own devices
(`held` turning on and off every 0.1 to 0.6 s), one node's log had been flushing for 5.92 s and the
longest commit waits were 4.83 to 5.37 s; the box's load was 112 to 189, later 315, from its other
work. Two small cases run at the same time (10,000 and 30,000 objects) ended at the native door's
first exchange `Closed`. A node holding a chunk while its device stalls sends nothing, so its
sender's transport cuts the transfer at its progress period, and a cell of three with three copies
has no other volume for the chunk: the block is `Unplaced`, answered `SlowDown` (node.md §4.4). It
is the wait of "a node cut while it works" (2026-10-29) on the write path, and the fix is the
owner's step 2 of 2026-10-30: a node working on an exchange says so each period (research/77),
designed in hyper-transport, which waits on that design and the owner's word. The client's
`SlowDown` retry (1ac2509d) asks again within its three attempts, and each attempt met another
stall. The 100G case's own refusals were not traced; the same answer at the same place, under the
same load, is read as the same cause.

The same traced case at half size on the progress notes (branch `progress-notes`, e81c9c65, with
the diagnostic lines of 5639a8c4 and each range step's budget spent and each commit of 250 ms or
more a node waited on, never merged; binary 2e562ce7): 20 GiB objects on stated 25 GiB volumes, the
box's load 80 to 346, 266 to 319 while the puts ran. Each way was answered `SlowDown` part way and
boto3 had no turn; the case ended inexact in 1,142 s with no object acknowledged, nothing read
wrong or missing, each node holding 641 to 658 MB of the runs and parts acknowledged before the
refusals, the scope at most 367 MiB:

| Way | Answered | What the node serving it said |
|---|---|---|
| native (whole blocks) | `SlowDown` at 59.2 s | a chunk to a peer cut `Lost(Stalled)` at 44.5 s, 49.9 s and 58.6 s, the client's three attempts, each block `Unplaced` |
| cli | `SlowDown` at 32.4 s | at 24.7 s three chunks cut `Lost(Stalled)`, then within 0.9 s four refused `Lost(Budget)` by its transport and seven `Busy` by its own volume, each block `Unplaced` |
| native-parts | `SlowDown` at 75.4 s | a chunk cut `Lost(Stalled)` at 38.4 s, `Unplaced`; the client asked three times more |

A node vouches for a peer's chunk each time the volume it waits on answers a put, and a device that
stalls past the period answers none, so the notes keep a chunk only while its receiver's device
moves. Behind a stalled receiver the serving node's own volume filled its writer queue and its
transport its budget for that peer, and the chunks after were refused at once. No request's range
step ran out its budget, though before the first put the young cell's leader waited up to 4.1 s on
commits of its own, and its own steps three times outlasted the 6.1 s an unjudged step is given. The large
cases here wait on how a node waits on a peer whose device stalls, which research/77 D2's carried
deadline is to bound.

The same case on the carried deadlines (branch `carried-deadlines`, f7788c31, with the diagnostic
lines of 5639a8c4, never merged; binaries bf676bba and 88b49be7, the second naming each `SlowDown`'s
cause): 20 GiB objects on stated 25 GiB volumes under the 16 GB scope, the box's load 39 to 74.
Each run acknowledged the native way's object with no `SlowDown` and refused the next two typed,
the volumes holding one object's three copies:

| Run | Native put | CLI | Native parts | Each node's chunks | Scope at most | Ended |
|---|---|---|---|---|---|---|
| 1 (bf676bba) | `Done` in 1,272 s, 16.1 MiB/s | `InsufficientStorage` at 203 s | `InsufficientStorage` at 0.3 s | 25.05 GB of 26.84 GB, 3,431 fragments | 1.76 GB | in its reads after the kills, `the native door: the transport: UnknownExchange` |
| 2 (88b49be7) | `Done` in 2,268 s, 9.0 MiB/s | `InsufficientStorage` at 241 s | `InsufficientStorage` at 1.0 s | 25.05 GB of 26.84 GB, 3,429 fragments | 1.31 GB | stopped at 2.5 h in its reads after the kills, a get moving nothing |

Exact as far as each ran: 2,581 blocks, each block's chunks on distinct nodes, every node's chunk
bytes what it received over QUIC and put. The first run's reads ended on a reply body its transport
had just ended, which the client took for a transport failure (fixed, 412d1676). In the second the
nodes answered one request `SlowDown` past its patience 347 s after the last put, and the founder
started again a range step it found leaderless (`Read(Deadline)`) 908 s later; then nothing moved
for 1.4 hours. The harness reads after each kill through a client it opens, asking with no
deadline, and a client that has measured nothing gave its first exchange, the one object's get,
the time to the deadline as its period: none. It now gives at most the stock S3 clients' 60 s
(research/77 D2), judged once a period as the transport judges every exchange; a test holding a
put's body three such periods sees it cut `Stalled` at the end of the first period that moved
nothing (`crates/client/tests/progress.rs`).

**This Mac's 10G cases, one way at a time** (the owner's choice of 2026-10-31), on 1ac2509d
(binary 4858a0d7), each a fresh cell under launchd whose volumes are this Mac's free space less
30 GiB, a third each, deleted once its case ends:

| Way | Volumes (each) | Held | Put / native read / HTTP read (MiB/s) | Refused | Chunks held a node | Seconds | Load |
|---|---|---|---|---|---|---|---|
| native (whole blocks) | 36,511,648,426 B | 3 | 29 / 70 / 173 | `InsufficientStorage` (l3), none otherwise | 35.72 GB, 97.8%, one segment free | 4,138 | 14 to 25 |
| cli (2026-11-01) | 15,889,685,162 B | 1 | 54 / 133 / 238 | `InsufficientStorage` at the listener (l1), none otherwise | 15.51 GB, 97.6%, one segment free | 1,306 | 7 to 41 |
| native-parts (2026-11-01) | 148,829,888,512 B | 13 | 23 / 54 / 124 | `InsufficientStorage` (l13), none otherwise | 145.73 GB, 97.9%, one segment free | 18,897 | 9.3 at start |

Exact: 3,873 blocks, each block's chunks on distinct nodes; every node's chunk bytes what it
received and put; 18 reads after each founder's kill and restart, none wrong or missing, one asked
again; the controls caught; the refused object unreadable and unlisted. The CLI's way ran once this
Mac's disk was cleaned (its free space had fallen to 15 GiB with other work), on volumes a third the
native way's, so it held one object: exact, 2,560 blocks each on distinct nodes, 6 reads after the
kills, none asked again, wrong or missing, the controls caught. The native parts' way then ran
after more disk space was freed: 139,586,437,120 bytes in 13 objects, 33,280 blocks, each block's
chunks on distinct nodes; 78 reads after the kills, none asked again, wrong or missing. All three
controls caught; the refused object unreadable and unlisted and all acknowledged objects listed.
Each node held 145,728,913,408 chunk bytes and had 8,404,992 bytes free. Run
`mac-10G-native-parts-20261101-154857`, exit 0; the volumes were removed after the result was kept,
leaving 442 GiB free. The boto3 parts' way ran on the later `17eb8032` build (binary
`dcfa6de03a12b98542ce34515fdafafd`), run `mac-10G-boto-parts-20261101-210926`, with
147,296,316,074-byte volumes. It was stopped for shared-filesystem capacity after 6,182 s,
exit 143: free space had fallen to 5.8 GiB, and retiring its volumes returned it to 417 GiB.
Twelve completed objects were listed by a separate probe; this is not the harness's acknowledgment
or exactness tally. No placement or restart totals were published, so the case is incomplete and
must run again. The nodes' `write(stdout)` refusals after the stop are their closed harness pipes.

The first 100 GiB native multipart case, `mac-100G-native-parts-20261101-230335`, used
111,669,149,696-byte (104 GiB) volumes. It ended after 3,230 s with a typed full refusal at `l0`
and **zero whole objects**: no placement blocks or restart reads were verified. Each node held
100,999,364,372 chunk bytes. This was an undersized test volume, not a successful 100 GiB readback
or proof that the Mac cannot hold one. The estimate based on a preceding build's 2.08% non-payload
space did not account for this format's log reservation and multipart packing.

The automatically started boto3 case, `mac-100G-boto-parts-20261101-235915`, was canceled after
all three volume headers, with valid CRC-32C, showed only 105,398,599,680 data-region bytes
(98.16 GiB): less than the plaintext object alone. Each volume reserved 6,231,248,896 bytes for
the log. Its geometry and logs were retained and its volumes removed; it has no completed-object
or exactness result.

Run `mac-100G-native-parts-20261102-003931` completed on `17eb8032` with
120,259,084,288-byte (112 GiB) volumes: one 107,374,182,400-byte object, exit 0 after 8,446 s.
Put/native/S3 read rates were 39/123/257 MiB/s. All 20,000 blocks placed their chunks on distinct
nodes. Six native reads after the founder kills and restarts were exact, with none asked again,
wrong or missing; the initial reads covered both APIs. All three controls caught. Each node held
108,775,147,904 chunk bytes, with 8,325,104 bytes kept by none from the refused upload, exactly
accounting for the received and locally put bytes; 8,404,992 bytes remained free. The next object,
`l1`, was refused `InsufficientStorage`, unreadable and unlisted, and all acknowledged objects
were listed. There were no other refusals. At the start, 389 GiB free less the 32 GiB reserve
could not hold two whole objects in three copies (600 GiB before overhead), so one is the maximum
whole-object count. Retiring the volumes left 366 GiB free. Three 112 GiB volumes plus the reserve
require 368 GiB, so the remaining 10k/100k small sets were scheduled before the remaining large
ways. A subsequent calculation using `Geometry::plan`, `log_bytes`, `checkpoint_bytes` and
`derive::volume` matched both recorded 104 and 112 GiB headers exactly. With the same segment
and block sizes, 110 GiB per node provides 111,483,813,888 data-region bytes, 2,700,340,880 bytes
more than the verified case's chunk bytes including the refused partial upload. The remaining
fixed-volume cases use that budget, require 362 GiB free including the unchanged reserve, and
must still complete one whole object and all checks. Capacity waits start no data job; the
calculation alone is not a readback result. Later large ways recalculate capacity; if two whole
objects might fit, they use free-sized volumes. The runner stops an incomplete case if filesystem free
space falls below 30 GiB and reserves 32 GiB, including 2 GiB for metadata outside the volumes
(about 0.7 GiB measured, with room for a checkpoint being replaced). The earlier finding that
100 GiB did not fit was specific to the free space then.

**Deadline-build acceptance.** These baseline cases used `17eb8032`. Its complete matrix has
22 slots across the two physical hosts (adacachy is the box): three small sets and four large
upload ways at each of 10 and 100 GiB per host. Historical passes above remain historical;
missing current-build slots stay pending. The Mac's completed small sets are:

| Host / build | Case | Acknowledged native / S3 | Distinct-node blocks | Native reads after kills | Again / refused / wrong / missing | Controls caught | Seconds |
|---|---|---|---|---|---|---|---|
| Mac / `17eb8032` | small 1000 | 500 / 500 | 999 | 6,000 | 0 / 0 / 0 / 0 | 3 / 3 | 335 |
| Mac / `17eb8032` | small 10000 | 5,000 / 5,000 | 9,999 | 60,000 | 1 / 0 / 0 / 0 | 3 / 3 | 2,153 |
| Mac / `17eb8032` | small 100000 | 50,000 / 50,000 | 99,998 | 600,000 | 0 / 0 / 0 / 0 | 3 / 3 | 16,006 |

Run `mac-small-1000-20261102-011230`, exit 0, empty harness stderr: 65,477,038 object bytes,
every object read exactly through both APIs before the kills, and natively after each founder's
kill and restart. The replication pass does not exercise S3 reads after a kill.
Each node held 65,500,862 chunk bytes, exactly what it received and put, with no unkept bytes.
The case used 256 MiB per node while the 100 GiB native multipart case ran on the same filesystem
(load 10.62, 10.12, 11.20 at start). The 10k case's first preflight refused allocation before any
data job started as shared free space fell. Run `mac-small-10000-20261102-013310` then started
after headroom returned, but was stopped after 316 s, exit 143, when free space reached
45,067,243,520 bytes, below its 42 GiB floor. It published no readback or restart totals and is
incomplete. Its nodes' `write(stdout)` refusals followed the closed harness pipes. That
overlapping attempt required 50 GiB free, including a 44 GiB reserve. After the 100 GiB native
multipart case released its volumes, the 10k and 100k sets were scheduled sequentially before
the remaining large ways. The fresh 10k run, `mac-small-10000-20261102-034102`, ended exit 0 after
2,153 s with empty stderr: 654,185,067 logical bytes, all objects exact through both APIs
initially, 9,999 distinct-node blocks, and 60,000 exact native reads after kills with one asked
again. Each node held 654,424,107 chunk bytes, exactly its received and locally put bytes,
with none unkept. All controls caught.

The 100k run, `mac-small-100000-20261102-041703`, ended exit 0 after 16,006 s with empty stderr.
It acknowledged all 100,000 objects, totaling 6,548,797,886 bytes, and verified 99,998 blocks on
distinct nodes. All 600,000 native reads after founder kills and restarts were exact, with no
re-asks, refusals, wrong or missing objects; all three controls caught their faults. Each node
held 6,551,196,718 chunk bytes, exactly accounted with none unkept, on its 16 GiB volume.
The volumes were released after success, and the remaining large-case queue started its
100 GiB S3 multipart case using the fixed reader. The replication pass reads natively;
S3-after-kill readback is not part of these measured totals.

The box's early 1k run on `17eb8032`, `box-16G-20261102-015302`, failed after 469 s during the
replication pass with `the native door: the transport: Budget`. Placement and chunk accounting
passed; no completed kill/readback totals were emitted. Its scope recorded no max/OOM events
and peaked at 115,802,112 bytes, below the 16 GiB limit. The old runner removed the failed cell;
future failures now retain their cells. A repeat at the original 1k size,
`box-16G-20261102-031247`, passed in 271 s with 999 distinct-node blocks, 6,000 exact native restart
reads, no retries/refusals/wrong/missing, and all controls caught. The earlier intermittent refusal
is still unresolved; the repeat does not close it. Smaller diagnostic sets of 2, 128 and 512
objects passed in 65, 118 and 177 s and receive no credit for the required case sizes. A separate
diagnostic build records the local refusal's origin; no product fix or larger limit is asserted.
Three traced 1k trials also passed in 271, 191 and 176 s. The failed 1k slot stays open while
diagnosis continues. The independent current-build 10k case, `box-16G-20261102-040748/small-10000`,
also ended with the native transport `Budget` refusal after 1,882 s. Its 9,999 blocks of 10,000
acknowledged objects were on distinct nodes; chunk accounting matched, but no completed restart
summary was emitted. Its scope peaked at 145,256,448 bytes with no max/OOM events. The failed
cell was retained. Its 86 files were copied, compared byte for byte and flushed before a replay
reopened it. `budget-replay-10k-17eb8032.log` records all 10,000 initial native reads exact,
placement exact again, and 60,000 exact native reads across the original three-founder restart
sequence, with no re-asks; exit 0 in 258.37 s. This replay neither reproduces nor fixes the
original refusal and receives no credit as a completed fresh-cell case.

The box's fresh native 10 GiB case, `box-16G-20261102-044701/large-10G`, passed on `17eb8032`,
exit 0 after 1,129 s with empty stderr: two objects (21,474,836,480 logical bytes), 2,582
distinct-node blocks, 12 exact native restart reads with no re-asks or wrong/missing objects,
all controls caught, and native `l2` refused full, unreadable and unlisted. All acknowledged
objects were listed; no other refusal occurred. Each 23,344,063,829-byte volume held
21,771,458,000 chunk bytes plus 8,325,104 unkept bytes, exactly accounted, and 8,404,992 free
bytes. Initial native and S3 reads were exact; put/native/S3 rates were 46/256/457 MiB/s.
The 16 GiB scope peaked at 4,690,051,072 bytes with no max/OOM events.

The next CLI attempt, `box-16G-20261102-050550`, saw only 139,682,914,304 bytes free, leaving
747,986,944 bytes per node after the 128 GiB reserve. It held no whole object and is not a 10 GiB
readback pass. The next minute, the native-parts case saw 207,058,972,672 bytes free and used
23,206,673,066 bytes per node. It put two whole objects and passed placement over 5,120 blocks,
then ended with the native transport `Budget` refusal after 1,441 s during restart checks.
Its scope peaked at 5,617,119,232 bytes, with no max/OOM events. The failed cell is retained and
the queue stopped. The CLI slot requires a fresh run with enough room; the multipart slot stays
open pending diagnosis. A zero-object 10 GiB attempt cannot count as completed readback evidence.
The Mac also queues reruns
of its other three
10 GiB ways after those small sets; its canceled boto3 case is already in the large-case sequence.
The box's final-build runs use the 16 GiB memory scope. A queue is not a pass: every slot requires
its terminal result, full acknowledgment/readback and placement totals, restart checks and
applicable controls. A capacity refusal that holds no whole object is never a successful
large-object readback; an undersized explicit volume requires a corrected run before drawing a
conclusion about the host's capacity.

**Older Box 100k completion, 2026-11-02.** The run begun on `0100ddd8`,
`box-16G-20261101-163809/small-100000`, ended exit 0 after 40,804 s with empty harness stderr:

| Build | Native / S3 objects | Logical bytes | Distinct-node blocks | Exact native reads after kills | Again / refused / wrong / missing | Controls caught |
|---|---|---|---|---|---|---|
| `0100ddd8` | 50,000 / 50,000 | 6,548,797,886 | 99,998 | 600,000 | 0 / 0 / 0 / 0 | 3 / 3 |

Each node held 6,551,373,832 chunk bytes, exactly its received and locally put bytes, with none
kept by no object. Each 16 GiB volume had 100,001 fragments and 9,363,161,088 free bytes.
The 16 GiB memory scope peaked at 1,769,697,280 bytes and recorded no max/OOM events. The same
older job's combined 10 GiB capacity case on 40 GiB per node then ended exit 0 in 2,887 s:

| Build | Native / CLI / native parts / boto3 parts objects | Logical bytes | Distinct-node blocks | Exact native reads after kills | Again / wrong / missing | Controls caught |
|---|---|---|---|---|---|---|
| `0100ddd8` | 1 / 1 / 1 / 0 | 32,212,254,720 | 6,411 | 18 | 0 / 0 / 0 | 3 / 3 |

Native/CLI/native-parts put rates were 37/51/26 MiB/s, native read rates 206/163/159 MiB/s and
S3 read rates 338/328/320 MiB/s. Native `l4` and HTTP `l3` were refused full; the refused objects
were unreadable and unlisted, all acknowledged objects listed, and there were no other refusals.
Each node held 40,098,945,024 chunk bytes plus 16,650,208 unkept bytes, exactly accounted, with
8,404,992 free bytes. Boto3 multipart held no whole object because the ways shared one cell.
The final build's queue therefore runs every way on its own fresh cell. These older-build results
do not fill missing `17eb8032` slots.

Its three 40 GiB volumes were fully allocated before the next small-case and restart probes
started. Their temporary 128 GiB headroom guard had stopped the first repeated-restart probe
after five complete rounds, without reproducing `Budget`. Once that allocation was accounted
for, about 96 GiB remained free; the independent current-build 10k case and the tiny restart
probe use the established 20,000,000,000-byte Box floor and unchanged 16 GiB memory scopes.
The earlier guard stop remains incomplete, not a pass or a `Budget` reproduction. No product
quota, deadline or retry count changed.
The repeated-restart diagnostic, `box-16G-20261102-040748/small-1000`, then completed 30 kills
and restarts with 60,000 exact native reads and no retries or wrong/missing objects. The isolated
probe `budget-reopen-17eb8032.log` created and dropped 10,000 clients on one runtime in 0.29 s
without a refusal. Neither probe reproduced the original intermittent `Budget`.

**Reply-buffer failure isolated, 2026-11-02.** The backtrace-only build of `17eb8032`,
SHA-256 `ed74f12a1bcbd8496e26af14f47502f4a727ffe8ea4ffcc8b07aa80f785f05f4`, reproduced
that refusal in `box-16G-20261102-052226/small-10000`, exit 1 after 1,251 s. Placement had
passed for 9,999 blocks of 10,000 acknowledged objects. The backtrace ends at
`Client::read_reply` reserving `PIECE` from the local transport after a founder restart.
This is the reader's scratch-buffer allocation, rather than a peer refusal or an OOM event.
The client already owns a `PIECE` charged separately in `Client::memory`.

The direct regression uses a live connection and reserves other transport memory until less
than `PIECE` remains, then reads a real GET into the exact-byte checker. Old code returns
`Transport(Budget)`; reusing the existing client buffer passes at the same transport capacity.
Source uploads retain their unsent bytes in that buffer and use their existing reply path.
The owned buffer is restored before awaiting the sink, including when that wait is cancelled;
a second regression cancels at the sink and verifies the next upload's checksum and answer.
Neither the client's stated memory nor its retries, deadlines or transport budget is increased.
The clean candidate passed all 17 client tests: five unit, two heap, five progress and five
resume tests. The heap test's peaks were 349,040 and 348,855 bytes against the unchanged
3,823,855-byte bound. Workspace all-targets/all-features clippy, production checks, contracts,
formatting and the dependency audit passed. Jupiter's Windows x86_64 run (`r4`, Rust 1.98.1)
passed all five unit tests and package all-targets/all-features clippy. The Mac also passed all
17 client tests: its heap peaks were 342,578 and 343,234 bytes against the unchanged 3,821,135-byte
bound. Fresh Box 1k/10k runs and
the retained 10k cell's replay use the candidate binary with SHA-256
`b69e581448d8aac3eb32184096245170e3379e4624b242a50e85b0053036e166`. The vendor suite
passed. The full workspace run stopped at the same three unchanged engine `ranges_room` tests:
`Too many open files (os error 24)`, with soft/hard limits still 1,024/1,048,576. Earlier
native, S3, serve, stock-client and transfer-harness suites passed; packages after the failure
were not reached. The failed gate remains recorded, with no descriptor limit adjustment. The fresh Box 1k
case, `box-16G-20261102-061338/small-1000`, then passed in 211 s: 500 native and 500 S3
objects, 65,477,038 bytes, 999 blocks on distinct nodes, 6,000 exact native restart reads, no
re-asks/refusals/wrong/missing, all three controls and empty stderr. Its 16 GiB scope peaked
at 115,527,680 bytes with no max/OOM events. The fresh 10k successor passed in 1,225 s:
5,000 native and 5,000 S3 objects, 654,185,067 bytes, 9,999 blocks on distinct nodes, 60,000 exact
native restart reads, no re-asks/refusals/wrong/missing, all three controls and empty stderr.
Each node's 654,424,107 chunk bytes were accounted exactly with none unkept. Its 16 GiB scope
peaked at 263,569,408 bytes with no max/OOM events.
This result is attributed to the reply-buffer candidate, rather than retroactively changing the
failed `17eb8032` attempts. The Mac small-set and 100 GiB native multipart passes and the
Box native 10 GiB pass retain their `17eb8032` attribution.

The retained 10k cell then passed on the fixed reader in 397.74 s: all 10,000 initial native
reads, placement of 9,999 blocks, and 60,000 native restart reads exact, with no re-asks or
wrong/missing objects. This recovered-cell replay is additional evidence for the fix, separate
from the fresh 10k run's full acceptance result.

The retained failed 10 GiB multipart cell also passed on the fixed reader in 799.21 s: both
10,737,418,240-byte objects read exactly initially, all 5,120 blocks on distinct nodes, and
12 exact native restart reads with no re-asks/wrong/missing. Its 78 metadata files were archived,
compared byte for byte and flushed before replay. After the successful replay and verification
that no node still served the cell, its three chunk volumes were removed; the archive, raw
failure, replay log and verified result remain. This recovers the acknowledged objects and does
not relabel the failed fresh multipart case as passed. A fresh Box 100k case then started on
16 GiB/node volumes in the unchanged 16 GiB memory scope.

After `stat` verified all three 16 GiB files fully allocated, the remaining large-case queue
resumed on fresh cells with a 32 GiB reserve and the unchanged 20,000,000,000-byte disk floor.
The reserve leaves room for the active small case's metadata; free space is read for every large
case. Each 10 GiB way still requires a whole object and full terminal checks. A 100 GiB case
that cannot hold one is capacity-only evidence. The earlier verified native 10 GiB result keeps
its `17eb8032` attribution.

**Fresh CLI 10 GiB case on the fixed reader.** `box-16G-20261102-064320/large-10G`
completed in 1,956 s, exit 0 with empty stderr. Its binary is the `a57eaea7` reader source
identified above. The case acknowledged one 10,737,418,240-byte object, read it exactly through
native and S3 initially, placed its 2,560 blocks on distinct nodes, and completed all six native
restart reads with no re-asks/wrong/missing objects. All three controls caught their faults.
The next key, HTTP `l1`, was refused full, unreadable and unlisted; the acknowledged key was
listed, with no other refusal. Put/native/S3 rates were 24/162/338 MiB/s.

It began with 100,388,159,488 bytes free and a 34,359,738,368-byte reserve, giving
22,009,473,706 bytes per node. Each held 20,515,153,920 chunk bytes plus 8,325,104 unkept bytes,
exactly accounted, with 8,404,992 bytes free. Its 16 GiB scope peaked at 3,415,420,928 bytes
with no max/OOM events. During this case, 27.1 GiB of this session's unused debug artifacts
were reclaimed after confirming no compiler used the cache; the case's existing volume sizes
were unchanged. Later cases read the new free-space value. The large-result verifier checked
all terminal counts and rejected a copied result reporting one incorrect object.
The next native multipart case started in `box-16G-20261102-071744` alongside the active 100k set.

For the remaining Mac 100 GiB ways, the queue uses 110 GiB/node only when the measured
one-object budget fits the current free space. Otherwise it free-sizes the volumes after the
same 32 GiB reserve and measures the outcome. A fixed 110 GiB case still requires one whole
object; a free-sized zero-object result is capacity-only. All Mac 10 GiB ways require a whole
object, clean typed-capacity/listing results and empty stderr before the next case starts.
This removes an indefinite wait for a preferred size while retaining the full result checks.

**Controlled native multipart stop.** The next attempt, `box-16G-20261102-071744`, was
incomplete: the existing disk guard stopped it when shared free space reached 19,958,525,952
bytes, below the unchanged 20,000,000,000-byte floor. The three runtime `write(stdout)` errors
came during that shutdown. The guard cleaned its disposable cell; raw output, errors and scope
samples remain. No acknowledged-object or readback totals are inferred from this attempt.

Three later checks found about 144.7 GB available, and the immediate pre-admission check found
145,440,178,176 bytes. The queue resumed in a fresh cell, `box-16G-20261102-075358`, with the
same binary, 16 GiB memory scope, 32 GiB reserve, disk floor and per-case bound. Its volumes are
37,026,813,269 bytes each. The verified CLI case was skipped, and the queue now checks the
complete large-result verification before advancing.

**Fresh native multipart 10 GiB result.** That rerun ended exit 0 after 3,144 s with empty
stderr, acknowledging three whole objects (32,212,254,720 bytes). Initial reads through both
APIs were exact, all 7,680 blocks had chunks on distinct nodes, and all 18 native reads after
kills and restarts were exact with no re-asks, wrong or missing objects. All three controls
caught their faults. Native `l3` was refused full, unreadable and unlisted; acknowledged keys
were listed, with no other refusal. Put/native/S3 rates were 26/159/369 MiB/s.

Each node held 34,552,721,408 chunk bytes plus 8,325,104 unkept bytes, exactly accounted, with
8,404,992 bytes free. Its 16 GiB scope peaked at 283,058,176 bytes with no max/OOM events.
The complete result verifier passed before the queue admitted the fresh S3 multipart case.
This adds one case to acceptance; the earlier guard-stopped attempt remains incomplete.

**Fresh S3 multipart 10 GiB result.** `box-16G-20261102-084821/large-10G` ended exit 0 after
6,785 s with empty stderr on the same fixed reader binary. It acknowledged five whole objects
(53,687,091,200 bytes), read them exactly through native and S3 initially, placed all 12,800
blocks on distinct nodes, and completed 30 exact native reads after founder kills and restarts.
There was one re-ask during restart readback, zero wrong or missing objects, and all three
controls caught their faults. HTTP `l5` was refused full, unreadable and unlisted; acknowledged
keys were listed, with no other refusal. Put/native/S3 rates were 16/169/306 MiB/s.

The case began with 223,134,056,448 bytes free and the same 34,359,738,368-byte reserve, giving
62,924,772,693 bytes per node. Each node held 59,112,171,520 chunk bytes plus 8,325,104 unkept
bytes, exactly accounted, with 8,404,992 bytes free. The 16 GiB memory scope reported a peak
of 17,180,639,232 bytes and 10,791 `max` pressure events, with zero OOM/OOM-kill events; the
recorded peak is retained as reported by the kernel. The complete verifier passed before
the queue admitted its first fresh 100 GiB case. These four Box 10 GiB ways used different
free-space budgets; their object counts are recorded per case rather than combined.

**Fresh Box 100k result on the fixed reader.** `box-16G-20261102-063855/small-100000` ended
exit 0 after 19,110 s with empty stderr. It acknowledged 50,000 native and 50,000 S3 objects,
6,548,797,886 bytes total, read them exactly through both APIs initially, and verified all
99,998 blocks on distinct nodes. All 600,000 native reads after founder kills and restarts
were exact, with no re-asks, refusals, wrong or missing objects. All three controls caught
their faults. Each 16 GiB volume held 6,551,196,718 chunk bytes, exactly accounted with none
unkept. The 16 GiB scope peaked at 1,671,155,712 bytes with no max/OOM events. The independent
small-result verifier passed; all three small sets now have complete results on each host.

**Box 100 GiB native capacity result.** `box-16G-20261102-104441/large-100G` ended exit 0
after 2,845 s, empty stderr, on the same fixed binary. It began with 342,674,227,200 bytes free
less 34,359,738,368 kept, giving 102,771,496,277 bytes per node, below one 100 GiB payload even
before format overhead. It held no whole object and refused native `l0` full; the refused key
was unreadable and unlisted, with no other refusal. The missing-key control caught. Each node
accounted exactly 98,602,531,776 chunk bytes plus 8,325,104 unkept bytes with 8,404,992 free.
The scope peaked at 266,424,320 bytes with no max/OOM events. The complete verifier passed.
This resolves that slot as capacity-only; there were no whole-object or restart reads to credit.

**Unresolved 100 GiB S3 failures.** The Mac's fresh free-sized S3 multipart case,
`mac-100G-boto-parts-20261102-084422`, ended exit 1 after 9,804 s. Its volumes were
107,047,936,000 bytes each, from 355,503,546,368 bytes free less the same 32 GiB reserve.
No whole object was acknowledged: placement and acknowledged bytes were both zero. The table's
one object counts the non-full refusal, `l0 Failed`, before HTTP `l1` was refused full.
The harness maps unrecognized S3 codes to `Failed`, so this output does not identify the
original code. The remaining Mac queues stopped.

An APFS diagnostic clone had all 84 metadata files (563,339,504 bytes) compared byte for byte;
the three volume files were cloned with their sizes and three 4 KiB windows checked, without
claiming full-volume hashes. The first 8 MiB diagnostic part returned the correct HTTP 507
`InsufficientStorage`. The original metadata hashes and volume sizes/mtimes remained unchanged.
A separate fresh 128 MiB/node case kept the same 100 GiB multipart geometry and recorded the
SDK's raw error: `UploadPart`, HTTP 507, `InsufficientStorage`, zero SDK retries. It ended
exit 0 in 62.95 s and did not reproduce the original failure. These are diagnostic probes,
not host-capacity or whole-object acceptance results; no descriptor limit was changed.

When shared Mac free space later fell below 30 GiB, the failed case's 84 metadata files, all
three complete volume-index regions and the first two blocks of every segment were archived
and verified against their decompressed copies. The archive totals 276,999,229 compressed bytes;
it preserves index and header evidence, not the chunk payloads. The case had acknowledged no
whole object and verified both refused keys unreadable and unlisted. Its aborted chunk data
was reclaimed only after the corresponding archive verification, while the metadata and raw
failure logs remain. The Box CLI failure's chunk data remains retained.

The Box's following free-sized CLI case, `box-16G-20261102-113613/large-100G`, ended exit 1
after 2,769 s with `does not read: l0's current version`. It began with 345,174,036,480 bytes
free less 34,359,738,368 kept, giving 103,604,766,037 bytes per node. There are no completed
readback or placement totals. Its full cell is retained; all 84 metadata files (500,265,068
bytes) were archived, flushed and byte-compared before preparing a metadata-only diagnostic
copy. The original chunk volumes remain untouched. Whether the CLI's apparent success and
the unread current version reflect an acknowledgment failure is unresolved. The metadata-only
recovery copy returned HEAD 404 and an empty listing through all three nodes. A fresh 128 MiB/node
CLI probe captured AWS CLI exit 1 and `InsufficientStorage` from `UploadPart` and did not reproduce
the original failure. The queue stopped, and the case receives no acceptance credit.

A read-only decoder then checked the retained log frames and their CRCs on all three Box
nodes. Each has the same contiguous Name-group history, indexes 1–5,931, with no undecoded
nonempty normal entry. `l0` has 2,012 part commands followed by Abort at index 2,018; `l1` has
3,911 followed by Abort at 5,931. Neither has a Complete or Put command in that history.
This agrees with the recovered empty listing; it does not establish that a committed object
disappeared. The latest retained Name hard state records commit 5,930, so the decoder does not
claim the last abort's commit fence. All original log SHA-256 sums matched before and after
the scan. The raw CLI exit and error for the original upload were overwritten by later CLI
calls, leaving the apparent acknowledgment unresolved. Ten short probes of the installed
AWS CLI against a loopback S3 fixture all returned exit 1 after an injected `UploadPart`
storage refusal and Abort; none reproduced a false success. A full 100 GiB stream with refusal
at part 2,013 also returned exit 1 correctly in 32.68 s, after feeding the entire input, with
one Abort and no Complete. These are diagnostic results, with no Scale acceptance credit.
Three more full streams using the harness's explicit `classic` client profile gave the same
correct exit 1 in 28.52–29.08 s each. The archived Mac logs were also decoded without opening
the original cell. Their retained histories have gaps after log reuse, so they cannot establish
the first failed upload's complete command history; no absence claim is drawn from those gaps.

After the Mac archive and reclamation, a new full-sized S3 multipart case started at
`mac-100G-boto-parts-20261102-135528`, using the same immutable `a57eaea7` binary and unchanged
SDK defaults, 32 GiB reserve, 30 GiB guard and 36,000 s bound. Its per-node volumes are
106,918,025,898 bytes. A diagnostic wrapper records the original SDK operation, error code,
HTTP status and retry count before the harness maps them to a native status. It remains
finished exit 0 in 2,540 s. The raw record is `UploadPart`, `InsufficientStorage`, HTTP 507,
zero SDK retries. No whole object fit; `l0` was unreadable and unlisted through both doors,
chunk accounting was exact and the missing-key control was caught. Each node held
96,700,932,233 bytes plus 2,414,939 kept by none, with 8,404,992 bytes free. This is a second
capacity-only result, bringing the matrix to 11 readback cases, two capacity-only results and
nine unresolved slots. The earlier Mac generic refusal remains unexplained.

**CLI skip regression.** The same real-CLI fixture given `PreconditionFailed` instead of the
storage refusal produced exit 0, empty stderr, one Abort and no Complete in 1.35 s. The native
harness regression with a 9 MiB body failed on the old code with `Ok(Done)` in 5.44 s and passed
after the uploaded-object check in 1.51 s. A successful CLI copy now has to pass `HeadObject`
at the stated length before the acknowledgment oracle is set (research/76). The original Box
error was overwritten, so this identifies and fixes a harness assumption without claiming
that the original refusal was 412 or that a storage acknowledgment was lost.
All 41 command-line unit tests passed in 1.89 s, the real-process filling test covering all four
upload ways in 52.27 s, all-target/all-feature Clippy in 10.02 s and the production gate passed.

**Box 100 GiB native multipart capacity result.** The fresh
`box-100G-native-parts-20261102-143316` case used the same immutable `b69e5814` binary in a
16 GiB scope, with unchanged descriptor limits of 1,024 soft and 1,048,576 hard. Its
81,931,218,944 starting free bytes less the 34,359,738,368-byte reserve gave 15,857,160,192
bytes per node. It ended exit 0 in 1,064 s: typed full for native `l0`, no whole object,
unreadable and unlisted, exact chunk accounting and the missing-key control caught. Each node
held 14,294,997,233 bytes plus 8,325,104 kept by none, with 8,404,992 bytes free. Scope peak was
258,879,488 bytes, with no max/OOM events. This adds a third capacity-only result; the current
matrix is 11 readback cases, three capacity-only results and eight unresolved slots.

**Box 100 GiB S3 multipart capacity result.** `box-100G-boto-parts-20261102-150906` ended
exit 0 in 1,321 s on the immutable `b69e5814` binary. Its 121,286,602,752 starting free bytes
less 34,359,738,368 kept gave 28,975,621,461 bytes per node. No whole object fit. The raw SDK
record is `UploadPart`, `InsufficientStorage`, HTTP 507, zero retries; `l0` was unreadable and
unlisted, accounting was exact and the missing-key control was caught. Each node held
26,171,069,852 bytes plus 2,414,939 kept by none, with 8,404,992 free. Scope peak was
294,195,200 bytes, with no max/OOM events. All 79 metadata files were archived and verified
against their decompressed copies (12,290,409 compressed bytes) before releasing only this
successful case's chunk volumes. The original failed CLI cell remains intact. The matrix now
has 11 readback cases, four capacity-only results and seven unresolved slots.

**CLI fix landed; Mac guard stop retained.** The CLI acknowledgment check landed on main as
`a9446d9d` after CI `41542855742` passed all nine jobs, including busy Linux. A fresh Box CLI
100 GiB case uses its immutable binary SHA-256
`1e0f92734a99abd5c50f5350b9c3d0a88da375a9eb5a41dae19bfc26b30652c0`, with raw CLI receipts
and the existing 16 GiB memory limit, 32 GiB reserve and descriptor baseline.
The Mac native 10 GiB case `mac-10G-native-20261102-145005` stopped after 7,529 s when shared
free space reached 32,187,895,808 bytes, below the existing 32,212,254,720-byte guard. It has no
final readback, placement, restart or capacity totals and receives no acceptance credit. An
earlier bounded S3 listing saw five 10 GiB keys; that is visibility evidence only. All three
chunk volumes remain retained. Its post-stop files outside the volumes occupy 355,725,312
bytes, so this observation does not establish that its metadata budget needs increasing.
Recovery is being prepared after preserving its metadata; both the reserve and guard stay
unchanged. Older verified case metadata is being archived and byte-compared before removing
those raw metadata copies, while their proof logs and archives remain.

**Box 100 GiB CLI capacity result on the fixed harness.**
`box-100G-cli-20261102-171326` ended exit 0 in 856 s. Its immutable binary SHA-256 is
`1e0f92734a99abd5c50f5350b9c3d0a88da375a9eb5a41dae19bfc26b30652c0`; the production sources
match `a9446d9d`. Its 106,038,980,608 starting free bytes less 34,359,738,368 kept gave
23,893,080,746 bytes per node. The raw CLI receipt records upload exit 1 with
`InsufficientStorage` from `UploadPart`, download exit 1 with HEAD 404, and an empty successful
listing. No whole object fit. Accounting was exact, the missing-key control was caught, and
each node held 22,285,582,336 bytes plus 8,325,104 kept by none, with 8,404,992 free. Scope peak
was 520,282,112 bytes, with no max/OOM events. This is capacity evidence, not a reproduction or
explanation of the original CLI failure. All eleven Box slots now have separate results:
seven readback cases and four capacity-only cases. Across both hosts the matrix is eleven
readback cases, five capacity-only results and six unresolved Mac slots.

**Guarded Mac recovery, still uncredited.** The initial isolated launcher inherited launchd's
256-descriptor limit and could not duplicate the volume handles. That diagnostic is retained.
The next diagnostic used the existing execution context's unchanged 1,048,575 limit; it read
144,957,927 bytes correctly before native `cp` ended `Closed`. The script had omitted the
root-enlistment publication barrier that `Cell::restart` requires. A corrected replay waits
for all three exact `enlisted` lines before reading and compares the actual native and S3 CLI
streams against the public seed-1 AES-256-CTR generator using LibreSSL. Its first full 10 GiB
native read passed. No complete recovery, placement, all-founder kill replay or capacity result
is claimed yet, and every payload remains retained. Both failed diagnostics and the metadata
snapshots taken before replay remain available. No descriptor limit, reserve or guard was
changed.

**What an object takes of a range's store.** For the engines' share of a node's memory, sized by
what the cell's volumes hold (the owner's choice of 2026-10-31, node.md §1.4): the harness's small
sets on this Mac (binary 4858a0d7, launchd at the shell's 1,048,576 descriptors), each object a
block through one of the two doors, every node's five range stores at the case's end, their files'
lengths (a store grows its file an extent past its end when none is free, so the length is the
most it held at once). Every node's stores came to the same bytes:

| Objects | Root, bucket | Name | File | Block | Seconds |
|---|---|---|---|---|---|
| 10,000 | 73,728 each | 7,766,016 | 11,747,328 | 14,696,448 | 1,902 |
| 30,000 | 221,184 each | 17,375,232 | 21,479,424 | 51,511,296 | 6,611 |

Both cases were exact: every block's chunks on distinct nodes, 60,000 and 180,000 reads through
each founder killed and started again with none wrong, missing or asked again, the controls caught.
The empty store is 73,728 bytes. The Block range's store takes the most, its seven rows a block of
three copies: past the empty store, 1,462 bytes an object at 10,000 objects, 1,715 at 30,000, and
1,841 an object between the two. The figure grew with the objects, as a store whose most at once
holds compaction's room with what it compacts would, so the node takes the slope, the most of the
three (`derive::STORE_BYTES_AN_OBJECT`). The same set at 100,000 on the box ended at its first native
exchange `Closed` under load 216, so the figure stands measured to 30,000 objects. The same cases show
the old split on this Mac too: each 2 GiB volume's index held at most 99,497 fragments of its
524,288 blocks, 19% of its want, since the engines wanted room across the Mac's 7.3 TB file
system and what memory remained past the floor went in proportion to the wants.

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
