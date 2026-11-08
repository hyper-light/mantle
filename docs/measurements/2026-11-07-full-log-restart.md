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
