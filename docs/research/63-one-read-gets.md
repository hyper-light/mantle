# 63. One read for a kept record

A 10M get takes 322 ns in the Linux VM (main e52730a, `runs/n10000000-r4941..r4946`), against
SplinterDB's 1,003-1,057 ns (`r4801..r4806`, `r5221..r5226`): 3.0-3.3×. Four times needs 264 ns
or less. This note measures what a get's time is made of, finds the part a layout change removes,
and states the design that removes it.

## 1. What a get's time is made of

The bench reads the clock once an operation, as db_bench and `splinter_bench.c` do. On the VM's
arm64 Linux the vDSO reads the counter after an `isb` (research/60 §3), so consecutive gets do not
overlap: a get is its dependent reads and the work on its path between them.

**The floor.** `native-ab/gets/mlp3.c` and `mlp4.c` time the reads alone, with a clock read an
operation, in the VM, on memory the kernel backs with huge pages (a live 10M run's resident set is
90% huge pages: 3.80 of 4.24 GB, `AnonHugePages`, `gets/probe-main61.out`):

| floor, a clock read an op | 10M, 63% hits | 10M, all hits | 100M, 63% hits | 100M, all hits |
|---|---|---|---|---|
| a 16-byte index slot, then the entry it names (today) | 222-229 ns | 271-284 ns | 271-284 ns | 339-362 ns |
| the same on 4 KiB pages | 375 ns | 470 ns | 418 ns | 533 ns |
| the record at its hash's slot, one read | 142 ns | 145-157 ns | 169-170 ns | 171-173 ns |
| two candidate slots read together | 145-146 ns | 148-149 ns | 193-212 ns | 197-198 ns |

The bench's gets find 63% of their keys (a fill of N draws over N keys holds 1 - 1/e of them).
Sizes are the kept index's and kept memtables' at 10M (256 MiB, 1.75 GiB) and 100M (2 GiB,
19.5 GiB; research/60 §2).

**Not the entry's later lines.** An entry of a 16-byte key and a 100-byte value spans two or three
lines, and its value's place is known only once its header is read. Read that way (`mlp3`, `DEP 1`),
the later lines cost what they cost read at once: 281-291 ns against 282-291 ns at 10M, all hits.
The hardware brings them with the first.

**Not the instructions.** A get runs 1,089 instructions (callgrind, 1M keys, 391,182 gets,
`native-ab/cg/cgget-ranges-getscg.*`). Removing 274 of them changed a 10M get by 1.00× (research/60
§3): work off the reads' dependent path runs in their shadow.

So of a 10M get's 322 ns, 222-229 ns are its two dependent reads, and the rest is the work on
their path. Four times SplinterDB leaves 35-42 ns above today's floor: no trimming of the path gets
there. One read instead of two takes the floor to 142-146 ns, and the get, with today's path, to
about 240 ns.

## 2. The design

**What is kept.** Today's kept memtables hold every entry written to them, each key's older
versions too, behind two indexes that take turns (one serving, one out on a worker taking the
memtables kept since): at 10M, 1.75 GiB of arenas and 2 × 256 MiB of index for 6.3M keys; at 100M,
19.5 GiB and 2 × 2.2 GiB for 63.2M. A get needs only each key's newest record. The kept table
holds that alone, one a slot where the key's hash points (`crate::kept_table`):

- **Slots.** Linear probing with Robin Hood displacement (Celis, 1986), each slot one record, at
  most 2/3 full: a successful search then reads 1/2 (1 + 1/(1 - a)) = 2 slots expected, the
  second the next line pair (Knuth, TAOCP vol. 3, §6.4). A slot's header is 10 bytes: the
  memtable's number's low 16 bits, the displacement, the flags, the low half of the key's filter
  hash (its home slot: a rebuild places the record without hashing the key again) and the key's
  length; an inline value's length follows. A 16-byte key and a 100-byte value fill a 128-byte
  slot.
- **Records larger than a slot.** The slot keeps the key's head and names the rest in its part's
  spill: one read for what fits, two for what does not.
- **Parts.** The table splits by the hash's top bits into parts, each its own table. A kept
  memtable's records reach the table one part at a time: the shard lends the part to a worker
  with the records bound for it, and takes it back merged. A key of a part out on a worker is
  answered by the trunk; with no merge out, as in a phase of reads alone, every get reads once.
- **Order.** Memtables merge oldest first, so a key's newer record replaces its older one. The
  memtables not yet merged into every part are probed through their own indexes first, newest
  first, as the memtables the index does not yet cover are today.
- **Give-back.** Giving back the oldest kept memtable advances the oldest number a record may
  have; a record older answers unknown, and the trunk answers the get. A part drops such records
  when it next rebuilds, and before its oldest record's number is 2^15 behind, so the 16-bit
  numbers never alias.

The memory at a load of 2/3 and 128-byte slots: 6.3M × 1.5 × 128 = 1.21 GB at 10M against
1.75 + 0.5 GiB today, and 12.1 GB at 100M against 19.5 + 4.4 GiB.

## 3. Giving memory back

Today a kept memtable given back frees its arena at once, 64 MiB at a time, and its index
entries, 16 bytes each, wait as stale until more than half an index is stale (`KeptIndex::extend`
with `sweep`): a give-back costs nothing it does not free. In a table of records, a memtable's
records are spread over every part; dropping them moves no bytes back until a part is rebuilt,
which costs the part's size. Three ways, and what each costs a record given back:

- **Wait for half stale, as the index does.** One rebuild for each half-table given back: two
  moves a record. But the stale half is memory the budget no longer allows: under pressure the
  regions and kept memory together would hold up to half a table more than the budget.
- **Delete each given-back record in place** (its memtable's hashes kept, 4 bytes a record): the
  slots free at once, but the table's bytes do not, so a budget counted in bytes keeps finding
  the memory held and gives back more.
- **Give back in batches** a fixed fraction `f` of the table at once, then rebuild the parts
  smaller: each record given back is moved `1/f` times amortized, and the kept set swings between
  `1 - f` and all of what the budget allows, `1 - f/2` on average.

None of the three both holds the budget exactly and frees memory the moment it gives back, as
an arena does. A table of records can, if what it gives back together is stored together:

**Generations.** The kept records are split by age, not by hash: a generation is one table of the
records of a run of consecutive kept memtables, each key's newest among them. Only the newest
generation takes new memtables, extended by two copies that take turns on a worker as today's two
indexes do; when it holds a fixed share of what the budget lets the kept memtables hold, it is
closed and never written again, its spare copy dropped. Giving back drops the oldest generation's
table whole: the memory is free at once, and no record goes stale. A get probes every generation,
newest first, their home slots computed and their first lines read together, so their misses
overlap: one read's latency (the two candidate slots read together, `mlp4` mode 2: 145-146 ns at
10M against 142 ns for one) while the lines read together stay within what the core overlaps.

**The share.** A generation's share `f` of the kept memory sets both costs: a get probes `1/f`
generations, and a kept set giving back steadily holds `1 - f/2` of what the budget allows, the
rest of its gets going to the trunk. The share is the one that minimizes a get's expected time
from the host's own measures: the time of `k` generations probed together (`mlp4`, two lines a
generation, for each `k`) and the time of a get the trunk answers. With memory to spare, as at 10M
and 100M here, there is one generation and nothing is given back.

**Growth inside the budget.** A generation's table grows by half again as memtables merge into
it, on the worker that holds its spare copy: for that trip the worker holds the old copy and the
grown one, beside the copy serving gets. The shard therefore decides the growth before the copy
goes, and counts the grown size as out (today's index grows on its worker unannounced, which at
16 bytes an entry the budget's slack absorbs; at 128 bytes a record it cannot): a growth the budget
cannot take closes the generation instead, and the next memtable starts a new one.

## 4. Measured: one read, on huge pages

**A diagnostic table** (`diag-rt`, never merged): after a checkpoint, the kept memtables' newest
records built into one table on the shard, calm gets answered from it. Against main in the same
rounds, 10M, the VM:

| build | gets/s, main | gets/s, the table | table's huge pages | runs |
|---|---|---|---|---|
| table on `Vec` memory | 2.91M | 2.97M (1.00×, 3/6) | – | `r5251..r5256` |
| table advised `MADV_HUGEPAGE` | 3.10M, 2.99M | 2.98M, 3.01M | 0 kB | `r5281..r5282` |
| advised and populated for writing | 2.92M, 2.87M | 3.71M, 3.54M (1.27×, 1.24×) | 1,183,744 kB of 1,213,282 kB | `r5293..r5294` |

The table answered 999,999 of each million gets. Advised alone it held no huge page: its first
touch of a page is a probe's read, which maps the shared zero page, and the insert's write after it
copies 4 KiB pages out of it; the kernel counted 74 huge-page faults over the build, and no
fallback, for a 1.2 GB table. Faulted in for writing while advised (`MADV_POPULATE_WRITE`), it held
huge pages, and a get gained the second read's 70 ns: 342-349 ns to 270-282 ns.

**Where the rest of a get goes** (`gettime`, never merged: a clock read at each boundary, summed
over a gets phase of 1M, 10M; each read costs about 20 ns itself, the floor of the small segments):

| segment | ns a get | less its clock read |
|---|---|---|
| the kept lookup: index slot, entry, value copy | 268-277 | ~250 |
| the bench's clock read and latency record | 47-50 | ~28 |
| the harness between operations | 40-45 | ~22 |
| the client to the engine | 29-43 | ~10-23 |
| the calm checks and the key's hash | 22-23 | ~2 |
| the return through the lend | 21-22 | ~1 |
| back to the bench | 22-23 | ~2 |

The kept lookup is four fifths of a get; the rest of the path is 60-80 ns. At 1M keys, much of the
kept data in cache, gets ran 288-418 ns on the same build (`r3941..r3946`): a get is its path's
latency, not its instructions, which research/60 §3 removed 274 of for nothing.
