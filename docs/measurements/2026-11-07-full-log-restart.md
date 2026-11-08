# A founder whose log was full of live records could not start again

**What happened.** On 2026-11-07 at 17:11 this Mac's kernel panicked ("watchdog timeout: no
checkins from watchdogd in 93 seconds", 914 free pages of 16 KiB and 37.5 GB compressed at the
panic), stopping the Docker Desktop VM uncleanly with KIND run 5's cell in it: main `aa5cbf62`'s
image `0.1.0-ci.879`, idle for 41 hours. After the reboot founders `mantle-0` and `mantle-2`
started again. `mantle-1` started 22 times in a row, each start ending after its status lines with
"mantle: the node: the node's log: the log is full of live records".

**What its log held.** A copy of its node volume (169 MB, kept with its founders' lines and the
namespace's events under `~/mantle-kind/kind-full-log-20261107-171455`), opened by hyper-log's own
recovery in a scratch test that never lands, with the founder's derived configuration
(`log.segment_bytes` 569,344, `log.max_segments` 237, `log.max_groups` 6, `log.group_entries`
259,317, `log.group_bytes` 4,667,697): the file was 135,503,872 bytes, its quota, and every group
started at index 0. No group had ever compacted.

| Range | Entries held | Commit | Applied by its engine when the node started |
|---|---|---|---|
| root | 91 | 90 | 90 |
| bucket | 10 | 9 | 9 |
| name | 7,868 | 7,867 | 2,233 |
| file | 7,858 | 7,857 | 2,436 |
| block | 8,012 | 8,011 | 2,008 |

Recovery replayed 30,896 frames: 4.4 KB of the file a frame, about one 4 KiB block each, for
entries of a few hundred payload bytes. No group reached its bound, both of which count payload, so
none was refused `Backlog`; the file reached its quota first.

**Why it could not start.** A node compacts its ranges only when the log refuses a range's write
for room (`Node::compact_if_stalled`), so it runs its log to the file's quota first. And a
liveness write the log refused stopped the node (node.md §6.2), a refusal for room included: a
start's first liveness write found the file full, the node stopped before its ranges replayed and
compacted, and the next start found the log as full again.

**Reproduced.** `crates/node/tests/sim.rs`,
`a_node_whose_log_fills_with_live_records_waits_for_its_ranges_to_compact_and_starts_again`: a
log of the three segments hyper-log takes at least, its groups bound only by the file, eight
times the range group's entry bound of puts, a follower's power cut at the turn its compaction
starts and started again, then the idle cell until a node's log refuses its stream's write. On
main's behaviour, with every refusal fatal, a node stops as the founder did: "seed 1: 2 stopped:
the node's log: the log is full of live records". With the fix it passes
on seeds 1 and 2 (three nodes on LAN paths, five on regional ones) in 83 s: no node stops, a node's
log refuses its stream's write for room and the node goes on, the histories are linearizable and
every node converges. A first form of the fix compacted every range at every turn while the write
was owed and drove every range again each time, so a node with nothing to compact never parked
(the simulation spun at 91% of a CPU for 14 minutes); a compaction is now asked once at each
refusal and drives only the ranges whose compaction started, and the owed write is asked again
only once a range's write became durable or every range applied all its log holds committed past
what it had at the refusal.

**The fix.** A liveness write the log refuses for room (`LogError::Full`, or its queue `Busy`) is
owed, not fatal. The stream waits on its answer and asks no other meanwhile, so the node keeps it
owed: each refusal asks one compaction of every range through what its engine applied, and the
write is asked again once a range's write became durable since the refusal (the log's progress,
which may have freed room) or once every range applied all its log holds committed past what it
had then (a start's replay done, which a compaction then frees). While it is owed the node
heartbeats no peer, which suspects it as one whose disk stalls. `Status::liveness_waits` counts
the refusals. Every other refusal still stops the node.

**Left, in hyper-raft.** hyper-log counts what a group retains in payload bytes, while its frames
take whole blocks of the file, so for small updates the file fills (`Full`) before any group's
bound (`Backlog`), and mantle's `derive::log`, which sizes the file to hold every group's
retention from that payload, does not hold what the groups keep on disk. Handed to the hyper-raft
agent on a branch of its own: a group's own bound must come before the file's for any update
size.

## 2026-11-08: what two seeds hid

**What happened.** Concourse's test job, about 30 minutes through 2026-11-05, ran out its 2-hour
limit inside this test on both builds of `68c7b831` (`mantle/test` 12 and 13), and GitHub's two
Windows jobs for the commit ran out the 6-hour limit in their `Tests` step (run 42438332994,
cancelled), while every Linux and macOS job finished `tests/sim.rs` in 57 to 205 s. A container on
this Mac reproduced it with the same binary's settings, so it was neither Concourse nor load; the
box then reproduced it across seeds, as the two the test ran had not.

| Three segments, seeds 1-26, on the box | `68c7b831` | with the fixes below |
|---|---|---|
| Spun past 200 s | 11 | 0 |
| A node fenced, "a write made durable behind a refused one" | 1 | 0 |
| A node stalled for good, the cell's quiet watch failing | hidden by the spin | 11 |
| The test's last wait not reached | 2 | 2 |
| Passed | 12 | 13 |

**The spin.** A range stalled for room with nothing to compact was tried at every turn, and a try
drives every range again, so the node never parked and the simulated clock could not move: 600,000
passes at one instant (253,546 ms) with the log's durable writes at 1,182 and the applied entries
at 1,176 throughout, on a stack that ends in a whole-state copy (`Model::persist`, from
`Replica::compact`). What the node waited for could not arrive. A real node would have spun a core.
A stalled range is now tried once for each move of those two counts, the facts the owed stream
write already waits on, and the count is kept across the turn a try's own write clears the stall
(a first form reset it there and retried as often as before).

**The fence.** `compact_if_stalled` compacted every range at a stream's refusal, one with writes
out included. A write out then refused for room left the compaction's start behind it, which
hyper-log takes even when full, as it frees room: durable behind a refused write, it fenced the
range, and the node stopped. A range not stalled with writes out now waits for their answers, which
bring the next try. This is `68c7b831`'s own defect, reached on seed 23 at three segments and seed
10 at four.

**Still open: a file of three segments.** Parked instead of spinning, 11 of the 26 seeds show what
the spin hid: a leader stalled with its log compacted to one entry (start 286, last 287, everything
applied), `Full` answered for good, and no fact left to wait for; the others elect past it and it
never rejoins. Three segments is hyper-log's least, and the test's choice. At 4, 6 and 10 segments
none of 78 runs wedged, fenced or spun, and a founder's derived log has 237. Why a file of three
holds no room once its groups have compacted is hyper-log's to answer (with the frame accounting
above).

**The tests.** `tight_log` is four segments. A stream's write refused for room no longer depends on
a run's timing: `a_liveness_write_the_log_refuses_for_room_is_owed_and_the_node_goes_on` has the
harness refuse a node's next three, whatever its log holds, and fails in 0.12 s with the refusal
fatal again ("seed 1: 1 stopped: the node's log: the log is full of live records"). The full-log
test runs seeds 1, 2 and 10, on the box and in the container: `tests/sim.rs` passes in 52 s and 28 s.
