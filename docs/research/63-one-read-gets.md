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

## 5. Keeping pace with the writes

**The index fell behind.** In the bench's first gets phase, a fill of 10M just ended, not one get
of a million took the calm path (`gettime4`, cand4 `c4a385f`, `r5381..r5382`): an index was out on
its worker for the whole phase, its lists waiting, and each get went the full path, 454-484 ns.
The phase after the seeks, the index home, read the record in 219-221 ns, against 268-277 ns for
main's slot and entry. Against main in the same rounds (`r5361..r5376`):

| cand4 over main | 10M | 100M |
|---|---|---|
| gets after the fill | 0.95× | 0.32× |
| gets after the seeks | 1.41× | 0.56× |

**Why: the trips' recurrence.** The two indexes take turns: a trip takes every list kept since its
index last went, which is two trips' worth, the other index's and its own's. With `x` ns to merge
a record and `r` records a second, the time between departures obeys `d_k = x r (d_{k-1} +
d_{k-2})`, whose characteristic root stays at most 1, the trips bounded, only while `x r <= 1/2`.
At the bench's 5M records a second, `x` must be at most 100 ns.

**What a merge costs.** `benches/kept_merge.rs` merges 19 memtable lists of the bench's records,
9.7M in all, into one table, as a worker extends an index copy:

| | ns a record |
|---|---|
| the box, lists in key order (as packings make them) | 178-317 |
| the box, lists in place order | 151-250 |
| the M5, natively, key order / place order | 130-189 / 84-138 |

Counting the work: a placement probes 0.44-0.47 slots past its home and moves 0.20-0.22 records,
so a record costs its placements' fixed work and their misses, but there are two placements a
record: the growth rebuilds re-place 9.4M records beside the 9.7M merged, and each new table is
zeroed or faulted in (on macOS 290K minor faults, 0.25 s of a run's system time). A profile
(`sample`, natively) puts 86% of the merge in `place`, at the loads of a slot.

Kept, each short of the bound alone: placing a run's records along with one move rather than a
swap a slot (about 10%, and a refused placement now leaves the table unchanged, where the swapping
version could drop the record it carried); placing records sixteen at a time, a group's homes read
back to back first (Chen, Ailamaki, Gibbons and Mowry, ICDE 2004): 118-153 ns a record against
154-168 ns one at a time, natively, a sweep finding 16-32 best; a rebuild copying each slot once
rather than twice (no change measured: the time is the slots' misses). Measured and not kept:
lists in place order (10-20%, and a sort on the packing worker); reading the homes of the records
ahead one at a time (real loads, which retire in order: 15% at best); reading more of each run
(nothing). A compact index of locators small enough to
cache does not stand in for the table either: with the arena's random reads beside it, an 8 MiB
index costs a get what a 256 MiB one does (`mlp3` with an `isb` an operation, natively: 149-166
ns against 151-169 ns; 86-95 ns for 1 MiB).

**Parts.** No single worker's merge reaches 100 ns a record with margin, so the merge is split:
the index is `2^b` parts, a key's part chosen by its place's top `b` bits and its home in the part
by the bits below, `2^b` the host's workers rounded down to a power of two. A packing splits its
records by part as it walks them; a trip sends each part with its records to a worker of its own,
as many at once as the pool admits, and the index comes home once every part is back. A get
reads one slot of one part, as before; `x` falls to the slowest part's time, about `1/2^b`.

On the box, pinned to one die (8 CPUs, 4 parts), against cand4 at 10M (`r201..r204`): gets after
the fill 1.20× (4/4), their CPU an operation 1,060 to 420 ns; the fill 0.92×, its parts' merges
beside the packings on 8 CPUs.

## 6. One copy

**Two copies do not fit 100M.** With the index in parts (§5), the 10M gets after the fill ran
1.41× main and 4.66× SplinterDB (`r5431..r5436`), but at 100M they ran 0.35× main (`r5441..`).
The two copies held a record twice in 128-byte slots at a load near 0.55, about 466 bytes a record,
where main holds an entry in its arena and a 16-byte slot, near 265 bytes a key. At 100M the kept
memory reached 32 GB covering 36 of 178 memtables (main: 19.5 GB covering all 178); the machine
readings gave the rest back, and 72% of the hits went to the trunk. Peaks ran 43-46 GB against
main's 35-37, and before the index kept pace, 46-60 GB, two runs killed at 62-63 GB of 67.

**The design.** The index is one copy, as §2 first stated it. Each part has its own table and its
own coverage, the newest memtable whose records it holds. A part goes out alone to a worker with
its records of the memtables kept since it last went and comes back to serve again, its records'
lists merged and freed. While it is out, a key of that part is answered by the memtables newer
than the part holds and then by the trunk. A memtable's list goes, and its arena, once every part
holds its records. Each record is merged once, where two copies merged it twice.

**Pacing.** A record costs `x` ns to merge and the writes bring `r` records a second, so `x r`
workers are busy merging; the parts out at once are twice that, so a part's records never wait a
fill for a worker, at least one and at most every part. `x` is the last part's measured time a
record, `r` a memtable's records over its measured fill.

**Generations.** A generation closes only with every part home. Its parts may hold different
memtables; giving it back gives back the memtables through the newest any part holds, so no key
is answered from a newer part whose older record went with it.

## 7. Resizing in place

**The populate held the mmap lock.** A table's mapping is faulted in for writing when it is made
(§4). One `madvise(MADV_POPULATE_WRITE)` over gigabytes held the process's mmap lock for reading
for the whole call: `madvise_populate` (Linux v6.12 mm/madvise.c) drops it only for a fault that
retries. A huge page's fault in a `MADV_HUGEPAGE` range may also reclaim and compact memory before
it maps; that is the `defrag` setting's `madvise` and `defer+madvise` modes
(Documentation/admin-guide/mm/transhuge.rst). For that whole time every mapping change in the
process waited, including the shard's next memtable, whose arena it maps. On the box at 100M
(pinned to 8 CPUs, the host's page cache full), every shard step of 100 ms or more lay inside a
worker's populate of the same length. Populated a huge page at a time, the lock is held for one
huge page's fault:

| box, 100M, land2 with stamps | worst put, 4 rounds | shard steps of 100 ms or more | runs |
|---|---|---|---|
| one populate a table | 423, 341, 311, 1,752 ms | 6 a run, each inside a populate | `r351..r354` |
| a huge page at a time | 45, 48, 881, 6,893 ms | none | `r351..r354` |

The two long worst puts left were not single steps: they were rotations that waited many steps on
workers, whose populates took twice as long once the shard ran beside them (5.1-10.7 s of
populate a run, against 2.7-5.2 s). Each growth built a new table and populated it. A 100M fill's
four parts went through ten steps of 1.5x each, from 0.04 to 3.3 GB, and populated 37.9 GB to
hold 13.3 GB; a part's last growth held 2.2 GB beside 3.3 GB.

**In place.** A table's slots hold its records in their homes' order, but for the run at its
start that wrapped around its end. A home scales with the table (Lemire's reduction of the place:
`home = place * slots >> 32`), so in a larger table no record's home is before the one it had,
and in a smaller one none is after. Take that run out (the records from slot 0 to the first slot
that is empty or holds a record at its home): what is left is each record at its home or just
after the one before it. A record's slot in the resized table is then its new home or just after
the last record's, so:

- in a larger table, no record's slot is before the one it lies in, and the records move back to
  front once the mapping has grown;
- in a smaller or swept one, none is after, and they move front to back before it shrinks.

Each record moves once, and the run is placed again last, by the insert. This holds only if the
records of one home are also in their places' order. Robin Hood keeps a run in its homes' order
but leaves ties in the order the records came. Grown, two records of one old home can have new
homes the other way round, and moved in their old order they left a gap a probe stopped at, a key
then found twice. The insert now puts a record before the first one of its home with a later
place.

**From a huge page's boundary.** On Linux the mapping resizes with mremap(2): its pages move with it
and nothing is copied. Moved where the kernel chose, though, the tables lost their huge pages. The
kernel moves a huge page whole only between two addresses both on its boundary, and splits it into
base pages otherwise (Linux v6.12 mm/mremap.c, `move_page_tables`). A 10M run held 75% of its
resident bytes on huge pages, against 94% for tables built new (box, `r471`, `r461`), and at 100M
the gets ran 0.81x (`r391..r396`). The mapping now starts on a huge page's boundary. It grows in
place where the address space after it is free, nothing moving; otherwise it moves onto a
reservation from a boundary, so each huge page moves whole. Its length is whole huge pages where a
huge page is at most an eighth of it, and pages where one is larger: on a 64 KiB-page kernel a huge
page is 512 MiB. Rounded to pages alone, each table's last huge page stayed on base pages, and the
10M settled gets ran 0.96x those of tables built new (VM, `r5701..r5706`, 0/6); whole, the tables
are all on huge pages (box, `r631`, `r641`) and the settled gets ran 1.07x and the first 1.05x those
rounded to pages (6/6 each). Elsewhere a new mapping takes a copy, and the growth a trip reserves
counts it.

| box, 100M | populated a run | populate a run | the tables on huge pages | gets, vs a new table | runs |
|---|---|---|---|---|---|
| a new table each growth | 37.9 GB | 2.7-10.7 s | 100% | | `r351..r354`, `r561` |
| in place, moved where the kernel chose | 13.3 GB | 0.95-1.3 s | (70% of all memory) | 0.81x settled, 0.91x first | `r391..r412`, `r491` |
| in place, from a huge page's boundary | as above | as above | 97% | 1.01x settled, 1.01x first | `r521..r526`, `r571` |

The tables' share is from each mapping's `AnonHugePages` (`/proc/PID/smaps`) as a 100M fill ends.
The rest of the process's memory, memtables and the page cache, held 70-71% of its bytes on huge
pages for both builds. That share moved from run to run with the host's free memory, and an
earlier whole-process reading put it at 76% against 95%.

## 8. The first gets after a fill

A put forgets the cache's pages of the extents compactions freed, in shares over the room the
trunk's work frees them in. The last compactions free theirs after the last put. A flush finished
the packing and the trunk's work but left those pages, and the idle slices forgot them during the
first gets after a fill: about 287K pages at 10M, and those gets ran 0.79-0.89 of the settled
ones in most rounds. The flush now forgets them, a slice at a time, as part of the maintenance
the writes owe:

| land3 vs the flush forgetting | first gets | their CPU | idle units, first phase | flush | runs |
|---|---|---|---|---|---|
| 10M | 1.08x (6/6) | 0.82x (6/6) | 287K -> 0 | +30-60 ms | `r5611..r5616` |
| 100M | 1.24x (4/6) | 0.77x (4/6) | 0.19-0.74M -> 0 | | `r5621..r5626` |

At 100M the first gets still ran 0.54-0.89 of the settled ones: the parts out merging the last
memtables, which a get of their keys skips for the trunk (§6).

## 9. The memory look after every read

The shard looks at what its kept memtables and their indexes hold after every get, scan and
trunk charge (`kept_crowded`), so that after any operation they and the caches stay within the
budget. With the index in parts, the look summed every part's table of every generation each time.
Under callgrind over a 10M seek phase (200K seeks of 10 rows after 1M gets; `native-ab/cg/
cgphase-seekrandom-*`, normalized by `Trie::floor` calls, 742 instructions each in both builds),
the sum was 201 instructions a look and a seek made 2.6 looks: 523 of a seek's 10,959
instructions, 4.8%, where main 63098c7's look summed three fields. The 10M seeks ran 0.95x main
(`r5651..r5656`, 0/6) once the index came in.

An index now keeps its home tables' bytes in a field: a part taken out subtracts its table's, a
part put back adds its own, and an extension, made only without workers, counts again once, its
merges all made or one refused. A table home changes size only through these.

| 10M, `r5761..r5766` | seeks | seeks, settled |
|---|---|---|
| the field over the sum (land8 over landg) | 1.06x (4/6) | 1.03x (3/6) |
| over main 63098c7 | 1.00x (3/6) | 0.99x (3/6) |

The look at whether maintenance is owed (`owed_with`), made after an inline operation that is not
a calm get while the range is not already marked owed, is the rest: 145 instructions a look, and
in the same profiles 157K looks in the phase against main's 8K, 1.3% of a seek. With the flush
now paying the writes' debts (§8), nothing is owed during a seek phase, so each look runs every
clause and finds none. A scan can make work owed (its rents, the cache's slots), so the look
stays.

## 10. A put's own probe

A put probes its memtable's index at random: a control word and a slot. Measured, the memory it
probes mattered and the instructions around the probe did not.

**Its pages.** Each process's smaps a second through a fill (`native-ab/dirty/smaps-watch.sh`,
`r5871` at 100M, `r5881` at 10M): the memtables' 64 MiB arenas lay 95-99% on huge pages, but the
4-16 MiB mappings, the index's control words and slots and the entries' tail, held none or one
2 MiB page of about 10 MB resident, at both sizes. The VM's kernel backs memory with huge pages
everywhere but compacts for them only where advised (`enabled` always, `defrag` madvise,
Documentation/admin-guide/mm/transhuge.rst): a region not advised takes a huge page at its fault
only if one is free. The index now lies on the record index's allocation (`HugeZeroed`: mapped
from a huge page's boundary, advised, faulted in a huge page at a time).

**Its size.** A table made for a fill was the size a migration starts, at most half full, which a
migration needs for the writes it spans (`util::incmap`); a table made for a fill spans none. For a
64 MiB memtable of 0.56M entries the power of two above twice them is 2^21 buckets, 10 MiB of
control words and slots. 2^20 holds them under the 7/8 rebuild threshold, and at 54% a group of
eight is full one time in 140, so a probe still reads one group. A fill streams about 140 MB
through the shard's core a memtable (its arena, the worker's copy of it, the tail), and an index
line touched every 11 ms or so is evicted between touches; half the index stays cached:

| over land8 da4f3a8 | 10M fill | 100M fill | runs |
|---|---|---|---|
| index on huge pages (as a diagnostic build) | 1.06x (6/6), 1.04x (11/12) | 1.04x (6/6) | `r5891..r5906`, `r5931..r5942` |
| and its instructions trimmed, extents counted (memhuge3) | 1.05x (11/12) | 1.04x (6/6) | `r6001..r6026` |
| and a fill's table under the threshold (memhalf2) | 1.17x (12/12) | 1.14x (6/6) | `r6001..r6026` |
| the threshold's table over memhuge3's | 1.11x (12/12) | 1.11x (12/12) | `r6001..r6012`, `r6041..r6052` |

**Its instructions.** Under callgrind with the runtime's quantum scaled to valgrind's pace (turns
as often in puts as natively, `cg/put-inner2.sh`), a 10M put ran 1,446 instructions, and three
were work for nothing: a look through every frozen memtable for a failed packing (55, now the
terminal error's one field), the range a key falls in when there is one (78, now none), and a
future made for each operation inside the client's turn (66, now a count). Without them puts ran
as fast (1.00x, `r5911..r5916`, `r5931..r5942`): a put waits on its misses, and work off their
path runs in their shadow (§1).

Over memhuge3 the smaller table's 100M gets ran 0.97-0.98x (`r6041..r6052`, 3-5 of 12 rounds
faster) though no get reads a memtable's index once the fill is flushed: every function of the
get path lies 28 bytes later in its binary, the size of the change before it, and an A/A of
layout alone moves a 100M phase 3-4% (research/61 §4).

## 11. A get in the steady state a fill leaves

A 1M get, exactly (fairq 7dc6ee63 under callgrind, a Debian container on the box, collection over
the gets phase and its untimed replay, 7.9M gets; `bench/cgget/cg.1341`): about 1,069
instructions a get with the bench's own work, its engine path about 741 of them: the client's
`get_async` 177, the shard's `Inline::get` (its calm checks) 119, `kept_get` 145, the slot's probe
139 with `holds` 32 and a `memcmp` 23, the key's hash 28. On the box the get took 926 cycles (177
ns) at 1.18 instructions a cycle, with 1.9 fills from memory: the slot's line pair, read once
(§1). In the VM it took 216 ns, and SplinterDB's 675: four times SplinterDB is 169 ns.

Once a fill is flushed, every get finds what `kept_get` was written for gone: no memtable holds an
entry, the index is one generation, home, and its part for the key covers the newest kept
memtable. `kept_get` still builds what the general case needs (the part's covered number through
an `Option` chain over the generations, a walk of the kept memtables from the newest that stops
at once, an iterator chaining the open generation before the closed ones) before its one probe.
`ShardDb::get_steady` checks those conditions, with `get_calm`'s, in loads of fields, then makes
the probe; any condition failing leaves the get to `get_calm` and `lookup` as before, and the key's
hash, computed once by `get_now`, goes with it. Where a closed generation exists, `kept_get` asks
the open one and then the closed ones newest first with no iterator built over them.

| 1M gets, exact (callgrind) | fairq | the closed ones' loop | and the steady get |
|---|---|---|---|
| `kept_get`, a get | 145 | 110 | 0 |
| `Inline::get` (with the steady get) | 119 | 119 | 104 |
| `get_async` | 177 | 169 | 168 |
| the probe, `holds`, `memcmp` | 195 | 192 | 191 |

The probe itself was tried with its header read once into a fixed array and keys compared a word
at a time (`util::keys::equal`): 185 instructions against 183, the fields it no longer checked one
by one paid for in the struct it built (probe f4591e07, reverted). The time of a get with these is
measured below (§12).

## 12. Four times SplinterDB at 1M is at the floor

The gate (VM, `runs/n1000000-r6451..r6460`, ten rounds each, run7's balanced order, medians):

| arm | gets a second | ns a get | vs SplinterDB |
|---|---|---|---|
| SplinterDB | 1.80M | 556 | 1.00 |
| fairq 7dc6ee63 | 4.78M | 209 | 2.66 |
| faa aa861c53 (fairq, layout shifted: an A/A) | 4.89M | 205 | 2.72 |
| cand 43cd9c15 (the steady get) | 4.98M | 201 | 2.77 |

The steady get's 145 instructions fewer (§11) are 1.04× over fairq and 1.02× over the A/A of
layout. SplinterDB's own 1M gets, batch by batch since `r5801`, have medians of 1.66M, 1.77M,
1.89M, 1.48M and 1.80M a second (602, 563, 530, 675 and 556 ns): four times SplinterDB is 132-151
ns a get in four of the five batches, 169 ns in the slowest. §1's floor, the record's slot read
and the bench's clock read with nothing else, is 142 ns at 10M; at 1M the table holds 632K records in
128-byte slots at most 2/3 full, 121 MB or more, past the last-level cache, and the slot is
still a read from memory. Four times SplinterDB at 1M asks a
get to cost what its one read and its clock read cost alone: no path reaches it. The work a get
has above the floor, about 60 ns (§4), halved would leave 171 ns: 3.3× against this batch's
SplinterDB, 3.9× against the slowest.
