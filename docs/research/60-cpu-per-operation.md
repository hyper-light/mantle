# 60. CPU an operation

The owner's target (2026-10-17): half the CPU of every operation, with correctness, throughput and
latency kept. This note records where each operation's CPU goes, what each change measured, and
what was measured and dropped. Runs are in `benchmark-results/mantle-splinterdb-20261013/runs`
(the Mac's Linux VM, run7.sh's balanced order); native A/Bs and callgrind counts are in
`benchmark-results/native-ab`.

CPU is the process's user and system time over a phase, divided by its operations. A get's and a
seek's is the shard thread's alone, so their CPU is their time. A put's is the shard's, the
workers' and the I/O threads' together.

## 1. The baseline

Main 5969b07, the VM, the whole host, the median of 6 rounds (`runs/n*-r4001..r4026`).

| operation | 10M CPU | 100M CPU | 10M rate | 100M rate |
|---|---|---|---|---|
| put | 902 ns | 1,309 ns | 3.64M/s | 3.21M/s |
| get | 440 ns | 655 ns | 2.25M/s | 1.52M/s |
| seek and 10 nexts | 2,045 ns | 3,225 ns | 487K/s | 309K/s |

The bench's own loop costs a share of each (`--harness-floor` runs each phase with the fill's
oracle answering in the engine's place): natively, 36-44 ns a get at 1M and 10M and 48-84 ns at
100M (its oracle a bitmap of the key space), 75-150 ns a seek, 24-31 ns a put.

## 2. A get's floor

A get the kept memtables answer reads the kept index's slot, then the entry the slot names: two
reads, the second's address in the first's answer. A key absent from them all reads the slot
alone. `floor.c` times that chain with nothing else: each operation reads a random 16-byte slot of
an index the size of the kept index's, then the 128-byte-aligned entry it names in an arena the
size of the kept memtables', the next slot depending on the entry read.

At 100M the kept memtables hold the whole fill: 178 memtables, 19.5 GB of arenas, a 2 GiB index.

| sizes (index, arena) | where | slot alone | slot, then entry | entry over two lines |
|---|---|---|---|---|
| 100M (2 GiB, 19.5 GiB) | native | 132-141 ns | 360-379 ns | 368-376 ns |
| 100M | VM | 141-143 ns | 382-384 ns | 402-409 ns |
| 10M (256 MiB, 1.75 GiB) | native | 102-106 ns | 216 ns | 262-269 ns |
| 10M | VM | 120-122 ns | 251-253 ns | 289-290 ns |

The SwissTable's control word, a line apart from the slot, costs nothing measured: read beside the
slot it overlaps it (VM 100M: 148-151 ns against 143-145 ns alone).

The bench's gets find 63% of their keys (a fill of N draws over N keys holds 1 − 1/e of them). At
100M in the VM the floor at that mix is about 0.63 × 405 + 0.37 × 142 ≈ 308 ns, and with the
harness about 360-390 ns, against 550-580 ns measured; at 10M about 270 ns against 400-410 ns.

## 3. Why fewer instructions did not make gets faster

A get ran 1,632 instructions (callgrind, 100K keys, 1.39M gets; natively 1,649 at 1M), the harness
about 400 of them. Removing the per-get bookkeeping outright (a diagnostic, `diag-both`: no owed
check, no `count_get`, 274 instructions fewer) changed a get's CPU by 1.00× at 10M and 0.93-1.00×
at 100M (`r4481..r4496`).

`mlp.c` explains it. Each operation reads a slot drawn from a counter (independent of earlier
answers, as the bench's keys come from its generator), then the entry it names, then does F rounds
of arithmetic independent of the reads:

| instructions between one op's reads and the next's (VM, 100M sizes) | ns an op |
|---|---|
| ~0 | 15 |
| ~80 | 62 |
| ~200 | 134 |
| ~400 | 205 |
| ~800 | 402 (no overlap left) |
| ~1,600 | 578 |

The core reaches the next get's reads while this one's are out only when the work between fits its
window. At 1,600 instructions a get, consecutive gets' misses never overlap, so cutting a few
hundred changes nothing; a get of a few hundred instructions would overlap them and beat the serial
floor of §2. Natively on macOS the effect is weaker: at 16 KiB pages its table walks bound the
overlap (146 ns an op with no work between).

## 4. Where the instructions go

Callgrind (valgrind 3.24 in a container; rustix built with `use-libc-auxv`, since valgrind maps no
vDSO). Self instructions, inlined code charged to the function it is inlined into.

A get, 1,632 instructions (100K keys):

| part | instructions |
|---|---|
| the kept index's probe (`kept_get`) | 204 |
| the kept memtables' crowding check, run twice | 170 |
| the empty active memtable's probe | 161 |
| whether maintenance is owed (`owed_with` and what it calls) | 152 |
| the client layer (`get_async`, `Inline::get`, the lending's drop) | 241 |
| the entry's read (`read_at`, `parse`, `get_at`) | 91 |
| the harness (clock, draw, loop, value check) | ~400 |

A seek and 10 nexts, 14,666 instructions (152,752 seeks):

| part | instructions |
|---|---|
| the scan's own loop (`scan_inner`) | 2,184 |
| a leaf entry's decode | 1,578 |
| copies (`memcpy`) | 1,233 |
| the leaf index's trie (`step`, `floor`, `select1`) | 1,278 |
| the merge (`entry`, `pick`, `next`) | 1,594 |
| rows out (`Rows::push`) | 720 |
| a cached value copied into the cursor (`load_in`) | 663 |
| seek rent (`Trunk::charge`, `rents`) | 466 |

A compaction's merge and build (`benches/compaction.rs`, 1M entries, its consolidate and pivot
cases): 1,194 instructions an entry read, about 1,290 an entry written.

| part | instructions an entry |
|---|---|
| page building (`add_inner`, `Page::push`, `insert`, `encode_into`) | ~352 |
| the merge's picking (`pick`, `alone`, its loop) | ~235 |
| the range filter LIST uses (`push_key`, `Level::push`, `Packed::push`, `cut`, `level`) | ~218 |
| the input cursors (`decode`, `load`, `next`, `land`) | ~116 |
| copies (`memcpy`) | 110 |
| the point filter (`insert`, `xxh3`) | 71 |
| the checksum | 29 |

## 5. Changes

Each against its base in the same rounds, 6 rounds a size; a cell is the median of the same-round
ratios and the rounds the change won.

**The idle counts** (idle-wr fd363e4, landed on main c2fe0fb). The trunk counts the nodes whose
in-flight list holds a bundle and those wanting a change, where they change; `is_idle` and
`maintaining` read the counts, `check_references` compares them with a look at every node.

| idle-wr against main | CPU | rate |
|---|---|---|
| 10M puts | 0.91×, 5/6 | 1.04× |
| 100M gets, settled | 0.84×, 5/6 | 1.20×, 5/6 |
| 100M seeks, settled | 0.92×, 4/6 | 1.09×, 4/6 |

**The calm get and the kept index read alone** (getq-wr 0d6ee7a, cd51325, 935428c). A get the
memtables and kept memtables answer, with nothing of the kept memtables out or to tend, no region
trimming and the tuning cycle not ending, changes nothing the memory budget or maintenance looks
at: its looks after it are skipped, an empty memtable is not probed, and an index covering every
kept memtable is read alone, its group and tag computed once. 1,657 → 1,183 instructions a get
natively (0.71×).

| against idle-wr (`r4601..r4616`) | 10M gets | 10M settled | 100M gets | 100M settled |
|---|---|---|---|---|
| the calm get | 0.97×, 4/6 | 0.96×, 4/6 | 0.97×, 4/6 | 0.97×, 4/6 |
| with the index read alone | 0.95×, 4/6 | 1.02×, 2/6 | 0.99×, 3/6 | 0.98×, 3/6 |

Puts and seeks unchanged. §3 is why 29% fewer instructions bought 3-5%.

**The put path** (put-path 6ce3152, 52373fd, e4e1963). A suffix's head is one load when the suffix
has a head's bytes (it was copied a byte at a time, 35 instructions an entry); a leaf entry the
leaf being filled has room for is staged there without the walk up the levels a written page
starts; each filter probe's remix is the hash's high 32 bits by the multiplier's power, so the
seven probes wait on no other's multiply (the bits, and the filter's bytes, the same: the golden
bytes test passes). Compaction: 1,194 → 1,118 instructions an entry; natively 0.89× CPU an entry
on the consolidate case, 0.99× on the pivot case.

| against idle-wr (`r4741..r4756`) | 10M | 100M |
|---|---|---|
| fill, CPU a put | 0.97×, 4/6 | 0.95×, 6/6 |
| gets | 0.97×, 3/6 | 0.94×, 6/6 |
| gets, settled | 0.99×, 3/6 | 0.97×, 4/6 |
| seeks | 0.97×, 3/6 | 0.95×, 4/6 |
| seeks, settled | 1.02×, 1/6 | 0.99×, 4/6 |

## 6. Measured and dropped

| change | measured | why dropped |
|---|---|---|
| `IncMap::touch` reads both cache lines of a group (touch-wr 470de8e) | 10M gets 1.10× CPU (2/6); 100M 1.00× | the second line rarely holds the entry |
| a compaction picks from a tree of losers (loser-wr ef8531c; Knuth, TAOCP vol. 3 §5.4.1) | VM: puts 1.00-1.03×, seeks 1.01-1.04× (`r4441..r4456`); natively 1.12× CPU and 1.07× instructions an entry on the two-input merge, 0.99× and 0.98× on the four-input one | a compaction's inputs are few; the linear pick with its stay-alone shortcut is cheaper |
| a page view reads its page's lines 1-3 with the header (land-wr 9252868) | settled seeks 1.18× CPU at 10M (2/6), 1.16× at 100M (2/6) (`r4461..r4476`) | a read whose value is used is not a prefetch: the core waits for each line |
| no per-get bookkeeping at all (diagnostic) | gets 1.00× at 10M, 0.93-1.00× at 100M | §3: instructions under a get's misses cost nothing |
| a scan row only the trunk holds pushed from where the merge holds it (seek-rows 2022344) | 0.91× instructions a seek natively; VM seeks 1.03× (1/6) at 10M, 0.94× and 1.12× at 100M (4 rounds) | no gain the VM shows: a seek's rows wait on their pages' lines |
| a synchronous scan reads a cached leaf's values in place (seek-rows 183ace1) | CI: `shard_db_paced`'s evicting-cache tests fail | a scan's own reads give a small cache's slots away mid-scan |
| inserting a kept list's keys in group order | natively 0.87× cycles but 2.2× instructions a key, the sort not counted | the sort it needs costs more than it saves |

## 7. What half needs

- Gets: the work between consecutive gets' reads down to a few hundred instructions, or fewer
  lines a get. The floor and its harness are about 360-390 ns at 100M in the VM.
- Seeks: fewer dependent lines a seek (the trie's levels, the leaf's header and search, the
  entry), since their instructions are not what they wait on.
- Puts: workers are compute-bound, so their instructions are CPU; each put is written about four
  times (packing and about three compactions at 100M), each written entry about 1,100
  instructions spread over a dozen parts. Half needs fewer entries written a put, or the page
  builder copying each entry once, as well as every part trimmed.
