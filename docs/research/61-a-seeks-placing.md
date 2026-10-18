# 61. A seek's placing

The owner's bar (2026-10-18): four to five times SplinterDB along every dimension. At 10M a seek and
ten nexts ran 3.13× SplinterDB's on main 8ccd739 (research/60's three-engine runs, `r4801..r4816`),
about 701K a second against 227K; four times needs about 908K, a seek and its rows in 1.1 µs. This
note records where a 10M seek's time goes and what each change to it measured. Runs are in
`benchmark-results/mantle-splinterdb-20261013/runs` and `diag-runs` (the Mac's Linux VM, run7.sh's
balanced order), each A/B six rounds a size.

## 1. Placing against rows

Seeks of 1, 10 and 20 nexts, the same build (`ranges-z0m8`, main e52730a's code), 10M, two runs
each:

| nexts | a seek |
|---|---|
| 1 | 1.13-1.16 µs |
| 10 | 1.53 µs |
| 20 | 1.90-2.06 µs |

A row past the first costs about 43 ns. Placing the cursor costs the rest, about 1.10 µs: 72% of
the bench's seek of ten nexts. Cutting a seek's instructions in its rows alone (research/60 §6,
seek-rows: 9% fewer natively) left nothing the VM could measure.

## 2. Where placing goes

Each step of a placing timed on the shard's scans alone (`seekdiag` 3eba86e, never merged; counts
10-18 of the trace; `diag-runs/n10000000-r5071..r5072`, 2.63M placings a run; the timers add about
0.2 µs a seek):

| step | a placing |
|---|---|
| the trunk's segment (`Trunk::segment_at`) | 51 ns |
| the leaf the key falls in (`Branch::leaf_of`, the trie's `floor`) | 218 ns |
| landing on it (`RunCursor::land`) | 445 ns |
| its entry table's search (`page_lower_bound`) | 103 ns |
| its entry read (`load_in`) | 48 ns |

Landing was a chain of four reads, each waiting for the one before: the cache index's control word,
the bucket it names, the slot, and then the page's first line. At about 100 ns a miss in this VM
that is the 445 ns. The look also probed the index twice, `contains` and then `lookup`, and the
landing loaded entry 0 before the table's search picked the entry wanted.

The trie's floor, the next step, walks a node a key byte and, where the key passes every stored
separator, back down the rightmost path: about 1,800 instructions a seek at 1M (callgrind,
`cgseek-ranges-cgz0`) and two or three misses.

## 3. Changes

**Landing's misses in pairs** (land2). A scan's landing reads the index's control word and first
bucket together (`IncMap::get_touched`), and once the slot is known starts its page's first line
while the slot itself is read, the page's place taken from the layout every chunk keeps (its slots
in a row from its first, a page apart); one probe in place of `contains` and `lookup`. Against main
e52730a (`runs/n*-r4881..r4896`, RocksDB beside):

| | 10M | 100M |
|---|---|---|
| seeks | 1.15×, 6/6 | 1.14×, 6/6 |
| seeks, settled | 1.14×, 6/6 | 1.16×, 6/6 |
| CPU a seek | 0.87× | 0.88× |

**No entry loaded on landing** (land3). A seek lands on its leaf without loading entry 0, then
loads the entry its table search finds. Over land2 (`r4901..r4916`): 10M seeks 1.03× (6/6), settled
1.04× (4/6); 100M 0.99× and 1.03× (4/6).

**One source's rows in a run** (rows). Where the trunk's segment holds one source and no memtable
holds a key before the source's next, its rows go out in a run (`ScanMerge::push_run`): a
deletion passed over, each row pushed as the cursor holds it, until the row budget, the least
memtable key, the segment's end or the source's end, with none of the shard's comparison of
sources a row. A run moves the source at least once, so the scan's walk goes round after any run
and opens the next segment where a run of deletions ended this one; a merge of several sources is
reported unrun and takes the general step. Over land3 (`r4921..r4936`):

| | 10M | 100M |
|---|---|---|
| seeks | 1.06×, 6/6 | 1.12×, 6/6 |
| seeks, settled | 1.09×, 5/6 | 1.05×, 6/6 |

The three against main e52730a: seeks 1.20× at 10M (6/6) and 1.25× at 100M (6/6), settled 1.17×
(5/6) and 1.21× (6/6). A 10M seek of ten nexts in 1,245 ns, about 803K a second: 3.5× SplinterDB's
227K.

## 4. Gets, and layout

With each change 100M gets read 2-8% fewer a second than main's in most rounds (land2 `r4891..r4896`
settled 0.95×; rows `r4931..r4936` settled 0.92×), with no mechanism in the get path: the same
instructions a get natively (1,131 and 1,134 at 1M, 1,274 and 1,225 at 10M), the same kept
memtables and kept-index hits at every round, and gets that never reach a landing. An A/A of main's
code, the layout alone moved by one function added off every hot path (`aa-wr` 8d35dbf, never
merged; `runs/n*-r4941..r4956`), moved 100M seeks 0.96× (1/6) and settled seeks 0.97× (0/6), and
settled gets 1.03× (4/6): layout alone moves a 100M phase 3-4%, one way in every round (Mytkowicz et
al., ASPLOS 2009; Curtsinger and Berger, ASPLOS 2013, Stabilizer). The hot get functions sat at
other offsets in the two builds (`nm`: `kept_get` 52 and 40 bytes into a 64-byte line). An effect
under about 5% at 100M is not the change's without an A/A beside it; the seeks' 13-25% in every
round are.

## 5. What four times needs

A 10M seek of ten nexts in 1.1 µs, about 150 ns under the three: the trie's floor (218 ns, two or
three misses, its rightmost descent after a miss included) and the entry table's search (103 ns)
are the placing's steps left, and the rows' 43 ns each before the run.
