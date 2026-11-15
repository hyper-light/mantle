# A full log that answered `Full` for good once its groups compacted

**What was open.** docs/measurements/2026-11-07-full-log-restart.md left one case: on a file of
three segments, hyper-log's least, the node simulation's full-log test stalled a leader for good,
its log compacted to one entry and every write refused `Full`, with no fact left to wait for. The
test ran four segments instead, and kiln's Shards session named the stall as blocking: a node
whose log refuses every write holds its ranges still, and its cell serves around it or not at all.

**Reproduced.** `crates/node/tests/sim.rs`'s full-log test at three segments (`tight_log`), its
seeds from `MANTLE_NODE_SIM_SEEDS` and `MANTLE_NODE_SIM_SEED`, one process a seed, eight at once
(six for the fix's), on adas-mac-studio.local (macOS, Apple silicon, shared with other sessions'
builds; debug build):

| Three segments | lean-doors `40a4f84e` (hyper-raft `294678c`) | hyper-raft `6423713` (`277dd17` whole) | hyper-raft log-full (this fix) |
|---|---|---|---|
| Seeds run | 1-100 | 1-100 | 1-200 |
| Passed | 59 | 100 | 200 |
| The cell's quiet watch failing, a node stalled | 41 | 0 | 0 |

The fix's 200 ran on `d029835`, whose source is `97b2a21`'s but for the third commit's fallback,
which only 4-block segments reached (below); the test's default seeds are now 1, 2, 7, 10 and 11,
of which 7, 10 and 11 wedged on lean-doors.

Branch `log-full-red` (mantle `c8fd3231`) is lean-doors with the test alone, the evidence for the
first column.

**Traced.** A diagnostic build (branch `log-full-diag`, never landed) dumped the log's segments at
each `Full`: each live segment's pieces and bytes, the free ones and when each may be reused, the
head's rest, whether the tail is sweepable, and each group's pieces by segment. Seeds 7, 11, 22
and 27 ended alike, seed 11's leader (node 3) at its last refusal:

```
usable 0, head slot 1 (incarnation 65), 57,344 bytes left (14 blocks)
live: slot 0 (inc 63) 2 pieces 66 bytes, used 4,096 | slot 2 (inc 64) 1 piece 174 bytes, used 61,440 | slot 1 (inc 65) 1 piece 33 bytes
sweepable false
range: start 286, last 287; hard state (commit 285) and released mark in slot 0, entry 287 in slot 2, start in slot 1
```

273 bytes live in a file of three 64 KiB segments, and no write taken. The tail held one block's
frame, its two live pieces a hard state and a released mark, left there when the next frame was too
large for the tail's rest and opened a segment of its own. The sweep weighed a tail against the
bytes its frames took since it opened: one block's copy was no smaller than one block's frames, so
the tail was never swept, and the segments behind it, each holding one small live piece, never
reached the tail. With no segment usable, the head's room was kept for frames that make room, and
an update frame was refused `Full`; the range's compaction had nothing left to free (start 286,
everything applied), so it wrote nothing, and nothing the node waited for could move.

**Generated histories, to find the rest.** hyper-log's own explorer (hyper-raft
`crates/hyper-log/tests/full.rs`): groups write updates of entries of every size up to a frame's
room, compact part of their logs and write hard states, on files of 3, 4, 6 and 10 segments of 4,
8 and 16 blocks, 1 to 3 groups; whenever the log answers `Full`, every group compacts through its
last entry, each on its own or all in one frame beside the write refused, as a node's turn submits
them, and then the write. A log that refuses a compaction, or the write once all are taken, has
wedged. 32 seeds a shape and way, 2,304 histories each, release builds on adas-mac-studio.local:

| Histories | hyper-raft `294678c` | `6423713` (`277dd17`'s sweep and retention) | `8ac0ca5` |
|---|---|---|---|
| Wedged | 927 | 598 | 0 |
| Filled their log and went on | 983 | 1,220 | 1,818 |

A soak of seeds 33-288 at `8ac0ca5` (18,432 histories) wedged 12, every one 4-block segments with
the groups compacting one at a time (the third cause below); at `97b2a21`, seeds 1-512 (36,864
histories, eight processes of 64 seeds) wedged none, 28,932 filling their log and going on.

The same dump, per frame, traced the wedges `277dd17` leaves:

- *A compaction that freed no segment took the kept room as one that does.* 3 segments of 4
  blocks, 3 groups, step 13: with no segment usable and one block left in the head, a group's
  compaction (a frame that only frees what its group held, which the log let take that room) took
  it, killing pieces but emptying no segment, and the next group's compaction, with the sweep of
  the tail it made due, two blocks, found no room at all. Every write after, compactions too, was
  refused.
- *A frame that frees a segment was refused whole for its updates.* 3 segments of 4 blocks, 1
  group, step 696: after the group compacted, the oldest segment held one hard state and the next
  segment had emptied, reusable only once a frame naming a later tail was durable. The sweep's
  frame, laid beside the refused entry of 9,796 bytes, took three blocks where the head had two,
  and nothing else could open a segment.
- *A compaction laid beside another group's entries was refused with them*, as a node's turn
  submits its ranges' compactions and their writes together.
- *A sweep that did not fit even alone was refused, where a frame of nothing would have freed a
  segment.* 10 segments of 4 blocks, 2 groups, seed 283, step 76: one group's compaction emptied the
  tail and took the last usable segment; the emptied segment could be reused only once a frame
  named a later tail. The next frame swept the new tail, due because the segment after it held
  nothing live, and its copies took three blocks where the head had two.

**The fix**, in hyper-raft branch `log-full` (`294678c` and three commits; vendored at `97b2a21`):

1. `22f6bbb`, the sweep half of `277dd17` (branch `log-retention-counts-frames`, the hyper-raft
   agent's): the sweep weighs a tail against the slot it frees, a whole segment's room however few
   frames it held, so a tail of one small frame is swept.
2. `8ac0ca5`: the room kept for making room goes to frames that free a segment (they name a later
   tail); a frame that only frees what its groups held takes it only where what it leaves still
   holds the frame that then frees a segment (one block where the frame's updates leave the tail
   empty, else the tail's sweep less what they kill); a frame no segment takes is laid again with
   what may take that room, the rest waiting for the next frame, and a frame that frees a segment
   goes with its sweep's copies alone if need be.
3. `97b2a21`: where a sweep does not fit even alone, it waits, and a frame of nothing names the
   tail past the segments emptied since the last frame, which frees them.

Every durability rule stands: nothing is answered before its frame and confirmation are durable,
a segment is reused only after a durable frame names a tail past it, and the on-disk format and
recovery are unchanged (a frame of copies alone, or of compactions alone, is one the writer wrote
before). A write that waits for the next frame is held, not dropped, and a group's updates stay in
the order submitted (raft-log.md §3).

**Real processes.** `crates/mantle/tests/node.rs`,
`a_cell_whose_logs_fill_under_sustained_writes_goes_on_and_reads_back_every_acknowledged_object`:
a cell of three `mantle node` processes hosting objects, each on the least file (three segments,
its groups bound by nothing the file does not fill first), puts through each node in turn until
every node has compacted three times for its log's refusals (the new `room` line,
`Status::compactions`), every node applying what the others did after each round, every
acknowledged object read back through another node, and one node killed and started again on its
full log, serving every object. Debug builds on adas-mac-studio.local, at load 8 to 20 from other
sessions, the two arms interleaved, each its own tree and target, the binaries' md5s recorded
beside the runs (`~/log-full-runs/e2e-ab4/binaries.txt`): log-full at `97b2a21`, and branch
`log-full-red-e2e`, the same test on lean-doors' hyper-log:

| Ten runs a arm, interleaved | lean-doors' hyper-log (`294678c`) | log-full (`97b2a21`) |
|---|---|---|
| Passed | 6 | 10 |
| Failed at the cell's watch, a node behind | 3 (at 45, 117 and 43 s, the nodes at one or two rounds of compaction) | 0 |
| Past the 1,500 s bound | 1 | 0 |
| Puts acknowledged in a passing run | 270-276 | 273-291 |
| Puts refused | 0 | 0 |
| A passing run's time | 26-63 s | 26-309 s |

An earlier batch of eight pairs, the test then killing each node in turn after the writes, had
lean-doors' arm pass 3 and fail 5 (4 at the watch, 1 past the bound) and the fix's pass 8, its
binaries clean as these. Together, 9 of 18 red runs failed and none of 18 green runs (Fisher's
exact test, one-sided, p = 0.0005).

A red run fails at the cell's watch once a node's log wedges and that node cannot apply what the
others did, or runs on past the runs' 1,500 s bound, its cell serving around the wedged node while
something the cell says keeps the watch from going quiet (not traced: it is the wedge's, and the
fix arm never reached it). The first batch, whose red binary was a stale build of the diagnostic
patch (its sources' restored mtimes older than the build: the test binary was rebuilt, the node
binary not) and so is left out of the counts above, printed the log's segments at each `Full`,
changing nothing else, and caught the wedge in each red run that failed: one node's log at `usable 0`,
its head with 10 blocks free, its tail one block's frame holding one live piece of 25 bytes (a
released mark), 495 bytes live in all and `sweepable false`, every write refused from then on, as
the simulation's. A batch before the test restarted only one node had one fix run fail: the
second restart's wait for the node started again to judge its links went past the cell's quiet
period (20 s, its patience grown with flushes of 9.7 s at load 20), every log answering and every
range converged; the test now restarts one node.

**Left.** `277dd17`'s retention half, each group's bounds held to what the file holds of it as
stored, so a group is refused `Backlog` before its file is `Full` (the KIND founder's cause (3) of
2026-11-07), is on hyper-raft branch `log-retention-counts-frames` and not vendored: mantle's
`derive::log` takes the least file's segment as a quarter of the least file, which that half
changes, and would need `Config::disk_for` instead. hyper-raft branch `log-full` is general (no
default or behaviour of mantle's alone) and lands on hyper-raft main once focal's and slates'
suites pass on it, as mantle's do here. The sweep's count of its copies leaves out each entry's
tag in a sealed log, as before; mantle's log is not sealed. A compaction that frees no segment is
weighed on its own frame: groups compacting one frame each into a file whose segments hold
barely more blocks than there are groups could still need two compactions in one frame to make a
sweep fit, which none of the 36,864 histories at `97b2a21` reached; a node's turn submits its
ranges' compactions together.
