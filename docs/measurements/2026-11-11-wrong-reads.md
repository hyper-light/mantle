# Wrong answers at 100M on macOS: a loan adopted by a replaced cache

2026-11-11, mantle `87ab09e1` (main) and this branch's fix. The open case of
[2026-11-06-targets.md](2026-11-06-targets.md) § "Open: wrong answers at 100M under `sample` on
macOS": four 100M cases on a 36 GB Mac answered acknowledged keys "not found" or met
`Corruption { "a branch page", .. }`, the pages decoded as leaves holding trunk-image headers and
runs of integers, with the Mac short of memory.

**Answer.** A page cache took pages into its slots from a loan another cache had lent. When the
machine's free memory leaves the page cache's share under one page, the tuner sizes it to 0
(`ShardDb::grant` → `Store::resize_cache(0)`, `shard_db.rs`), which drops the cache, and gives a new
one when memory comes back. A worker's job may hold a loan of the old cache's memory across that
(`Cache::lend`, research/57 §7). The loan names the old cache's chunks by number. The new cache's
`adopt` put each kept page in the slots its own tables give that number; for a number past its own
chunks, `starts.get(c).unwrap_or(0)` put them in its first slots. The index still named those slots
for the addresses they had held, and a cache hit is not verified again (a device read is,
`store/page.rs`), so a get could read another address's page as its leaf. Fixed: each cache has an
identity, each loan carries its lender's, and a cache adopts or takes back only its own loans
(`store/cache.rs`).

## The defect, by test

Each test fails with the identity check disabled and passes with it.

| Test | Without the check |
|---|---|
| `store::cache` `a_cache_takes_no_page_from_another_cache_s_loan`: a chunk lent again, adopted by another cache | `page 100 was dropped` (the probe before it read page 13's bytes for address 104) |
| `store::cache` `a_loan_naming_a_chunk_past_the_cache_s_own_takes_no_slot`: the number past the adopter's chunks, the form below | `page 100 was dropped` |
| `shard_db_workers` `a_loan_of_a_replaced_cache_puts_no_page_in_the_new_one`: real workers; the cache replaced each time a loan of its chunks is out (`Memory::relent`) | key 10 read `v875…` where `v1240…` was written |

## The defect in the 100M runs

adas-mac-studio-2.local: Apple M4 Max, 14 cores, 36 GB, APFS. The ranges bench at 100M keys, its
product path (`--shard-client --segments --machine-memory`), `sample` attached from the start of the
gets, the repro loop of 2026-11-07 (`MODE=machine`). A diagnostic build, never landed, adds to main:
a line when the cache is replaced with loans out; a line on a hit whose slot holds another address;
and (A2) a line when `adopt` is given a chunk this cache did not lend (one not `Lent` or `Refilled`).
A further diagnostic takes `DIAG_HOLD_GIB` GiB from the free-memory reading for the first half of
each `DIAG_PERIOD_MS`, as another process's memory comes and goes; the 11-06 failures came with
another session's load test on the Mac and about 9 GB held by the compressor. Every round filled
100M (1.87M/s in the round read), found 631,247 on its gets and passed its oracle and reopen checks.

| Pressure | Build | Rounds | Replaced with loans out | Foreign chunks adopted | Wrong answers |
|---|---|---|---|---|---|
| none | main + lines | 6 | 0 | not checked | 0 |
| a 22 GiB process (compressible: it took little) | main + lines | 4 | 2 | not checked | 0 |
| 28 GiB, period 4 s | main + lines | 4 | 5-22 a round | not checked | 0 |
| 28 GiB, period 4 s | the fix + lines | 4 | 8-15 a round | none, by construction | 0 |
| 28 GiB, period 120 s (held through most of each round) | A2 | 6 | 0 | 0 | 0 |
| 28 GiB, periods 10, 20, 40 s | A2 | 6 | 8 | 0 | 0 |
| 28 GiB, period drawn from 4-40 s each round | A2 | 30 | 80 | **10,194, in 2 rounds** | 0 |

The two rounds: 1,633 chunks numbered from about 14,334 given to a cache of 651 chunks, and 8,561
numbered from about 24,119 given to a cache of 4,710: every one past the adopter's own, so its pages
went into the adopter's first slots, which its index named for other addresses. 2 rounds of 30 is
the rate of the 11-06 failures (2 of 24). No get in these 30 read through such a slot before its
entries left, so the lines saw no mismatched hit. A cache short of a page with a loan out is
reached only under memory pressure, as the 11-06 failures were; what `sample` added to it (its own
memory, or its suspensions lengthening a job's loan) was not measured.

Whether anything durable was wrong on 11-06 was not known. The defect writes nothing: the cache
holds copies of pages already written (`store/cache.rs` has no write to the file), so it misled
reads and left the file as it was.

## Not done here

- The fixed build under the same random-period pressure for as many rounds: its `adopt` returns
  before any chunk is touched, so the foreign-chunk line cannot fire; the tests above are its
  evidence.
- Linux: the same loans and replacement run there (the tuner reads `MemAvailable`), not measured.
