# The Raft log: one log per metadata device, shared by every range on it

Status: design, 2026-09-28. Sources: docs/research/06 (consensus, cited as "06 §x"), 07
(focal's consensus stack), 03 and 11 (I/O and the operating-parameter models), 01
(Tectonic, Ceph BlueStore as [AWK+19]); docs/design/chunk-store.md, whose techniques this
log reuses, and docs/design/metadata.md §3, which places it.

A range replica persists its Raft state here: its entries, its hard state, the point its
log starts after, and the entries it approved for Fast Raft's fast track. Every replica on
a device shares the device's one log, so one flush commits every group's writes, as
Bigtable's single commit log per tablet server does. Per-tablet logs "reduce the
effectiveness of the group commit optimization, since groups would tend to be smaller"
(06 §A4.1, §C.c). The design follows the requirements of 06 §C.c and takes tikv's
raft-engine as its reference without depending on it (06 §B5, §C.c.1).

## 1. Where it lives

The log sits beside the metadata engine's files: one log file on each device that holds
range replicas, in the same directory as those replicas' engines. Ceph's BlueStore writes
object data to the raw device with direct I/O and keeps all metadata in RocksDB, which runs
on BlueFS, a minimal file system of its own [AWK+19 §4.1; research/03 AWK+19-F5]. A mantle
node likewise keeps its chunk stores on the data devices (chunk-store.md §1) and its
metadata, the engines and this log, on a file system. On a laptop that is the one SSD.
Placing ranges on devices places their logs, so a device's failure fences only the replicas
on it (06 §C.c, item 6). BlueStore changed RocksDB to "reuse WAL files as a circular
buffer" so that a metadata write costs one flush [AWK+19 §4.1; research/03 AWK+19-F6]. This
log's segments are reused the same way (§2, §5).

## 2. Layout

The log is one file of fixed-size segments, preallocated a segment at a time and grown up to
a quota, as a file-backed chunk volume grows (chunk-store.md §2). Every offset and length is
a multiple of the file's alignment `B`: the larger of 4 KiB and the device's logical and
physical block sizes, as for the chunk store.

- **Segments.** A segment's first block is its header: magic, format, the log's 128-bit
  ID, the segment's incarnation, and a random nonce drawn when it opens. The log holds
  segments in incarnation order, not by position, so a free segment anywhere in the file
  can be taken next. A frame belongs to a segment only if it names the segment's
  incarnation and nonce. A record left in a reused slot by its previous segment is
  therefore never read as the new one's. The same holds for a frame left by an opening
  whose header never became durable, even when a later opening takes its incarnation again.
- **Frames.** One group-commit batch is one frame. It starts on a block boundary and is
  padded to `B`, since a partial page is padded and never rewritten [HKA17-F7]. The frame
  header carries the log ID, the segment's incarnation and nonce, a sequence number
  consecutive over the log's life, the incarnation of the oldest live segment (the tail),
  the payload's length and record count, and a CRC-32C over the header and payload.
  Because a batch is written only after the one before it is flushed, a valid frame with a
  later sequence proves that every earlier frame was acknowledged (§6).
- **Records.** Each record names one group, the range replica's 128-bit ID:
  - `Entries { first, entries }`: entries from index `first` on, each with its term and
    bytes. They replace any the group held at or after `first`, which is how a follower
    drops a conflicting suffix, since truncating back to front is safe (06 §C.c, item 3).
  - `Relocated { first, entries }`: copies of entries the group still holds, written by
    reclamation (§5). They replace nothing past them.
  - `HardState { term, vote, commit }`. The latest wins.
  - `Start { index, term }`: the group's log now starts after `index`, whose term is
    `term`, which the log keeps so that focal-raft can ask the term of the entry before
    its first. The replica's engine made the entries before it durable, or the replica
    installed a snapshot there; a snapshot that discards what follows it comes with
    `Entries` of none from `index + 1`.
  - `Proposal { index, term, bytes }`: an entry the replica approved by itself on the fast
    track, held beside its log until the log reaches that index, even if a later conflict
    shortens the log again (07 §1.4).
  - `Removed`: the replica left this device, and all of its records are dead.

A replica's `Ready` becomes one submission of all its records, and they go in one frame. The
frame's single CRC makes them durable together, which meets etcd's rule that entries and
hard state are persisted before the messages that depend on them (06 §C.c, item 1). The log
stores entries and hard states as the replica gives them. It knows indexes and terms and
nothing of Raft's message formats, so it depends on no Raft crate.

## 3. Writing

One writer thread per log runs the chunk store's group-commit loop (chunk-store.md §4). A
bounded queue admits two batches' worth of submissions, by count and bytes, and refuses
past that with `Busy` [research/11 §4]. The loop takes everything that arrived while the
last batch was being made durable, encodes one frame, writes it, flushes the file once
with the platform's full flush, and only then publishes the records to readers and answers
every submitter. Replicas submit in a closed loop, so the writer waits for the replicas it
just answered as long as that is expected to lower total latency, the wait the chunk store's
writer derived (`mantle_disk::commit`). Without it, a few replicas alternate between batches
and each update waits for two flushes
(docs/measurements/2026-09-28-raft-log-benchmark.md, finding 2).

A replica cannot have its update refused: once the core has handed over a `Ready`, it takes
no other call until the `Ready` is made durable (07 §1.2). So a replica submits by waiting
for room in the queue instead of taking `Busy`. The writer frees room with every batch it
takes, and a fence wakes every waiter, so the wait lasts no longer than the writer's
progress.

A replica that leads may send its appends to followers before its own flush completes;
followers answer only after theirs (06 §C.c, item 2). The log lets both happen: a
submission returns at once with a handle, and the replica waits on it only for what must
follow durability.

A failed write or flush fences the log, and no later submission is taken. A failed flush
is never retried, because the kernel may already have marked the pages clean [RPA+20 §3].
The log must be reopened, and recovery trusts only what verifies.

## 4. What a group holds in memory

Each group has its hard state and where it was written, the point its log starts after,
its proposals, and one slot per retained entry: its term, segment, offset and length, and
its bytes while it is recent, about 40 bytes besides them. The recent bytes serve the
leader's appends and the replica's applies. Both are bounded:

- A group retains at most a configured number of entries and bytes, refusing with
  `Backlog` a submission that would pass either, and the replica compacts first. It
  retains entries only until its engine has made them durable, plus a window for lagging
  followers. Past the window a follower is sent a snapshot instead, which makes chronic
  laggards snapshot recipients rather than blockers of reclamation (06 §C.c, item 4; Raft
  dissertation ch. 5).
- The log admits at most a configured number of groups, and each keeps at most a
  configured number of bytes of its latest entries in memory, dropping its oldest first.

An entry no longer in memory is read from the file: a positional read of the blocks that
hold it, verified by the entry's own CRC over its group, index, term and bytes, so a read
of the wrong place is caught as surely as damage. Such reads serve only a follower
catching up, or a replica applying after a restart.

## 5. Reclaiming segments

Records die as groups move on. A new start kills the entries before it, a new hard state
kills the old one, a suffix replaced by `Entries` kills what it replaced, and
`Removed` kills everything the group wrote. Each segment keeps a count of its live bytes.
When the oldest live segment's count reaches zero, the tail moves past it, and the next
frame records the new tail, after which the segment is free. A segment is reused only after
a flushed frame records a tail past it, so recovery never takes a reused segment for a live
one.

When updates are waiting and fewer than two segments are free, the writer sweeps the
oldest segment, provided some of it is dead. It reads the segment and, for each piece the
in-memory state still points at, writes a `Relocated`, `HardState`, `Start` or `Proposal`
copy at the front of the next frame. That frame names the segment after it as the tail.
This is the cleaning of a log-structured file system applied to the end of a log [RO92],
and raft-engine's rewrite of lagging groups (06 §B5). Segments are freed oldest first, so the
live ones stay a run of incarnations that a single tail number names. The chunk store's
separate streams for relocated data, which group data by age [RO92 §3.6], would break that
run, so the log does without them.

A sweep always completes in one frame. A group's live entries run unbroken from its start
to its last, and an entry dies either from the front of the log, by a start, or from its
back, by a replacement. So the live part of any record is a single run, and its copy is no
larger than the record. The copies of a whole segment therefore fit in one frame. A freed
segment is reused only once a durable frame names a tail past it. The last free segment is
kept for a frame that names a later tail, whose durability frees a segment in turn, so the
log never runs out of room to make room. When every segment is live and the sweep would
free nothing, updates are refused with `Full` and the groups compact. Sweeping only while
updates wait keeps an idle or full log from reading its tail again and again.

The file therefore holds each group's live log plus the segments being reclaimed. By
Little's law, the live bytes are the node's append rate times the time an entry waits for
its engine to make it durable [research/11 §4]. The log measures both and grows the file,
up to its quota, to hold twice that, the same headroom the chunk store's cleaning keeps.

## 6. Recovery

1. Read every segment header. The one with the highest incarnation holds the head.
2. Scan the head segment's frames in order to the last valid one. That frame gives the
   tail, and the live segments are those from the tail to the head, in incarnation order.
3. Replay every live segment's frames in sequence order, rebuilding each group's state. A
   later record wins, and `Entries` replace any entries at or after their first index.
4. An invalid frame is a torn tail if no valid frame with a later sequence follows it,
   anywhere after it in its segment or in a later one: it was never acknowledged, so the
   log is cut there. If a later frame does follow, the acknowledged state was damaged, as
   protocol-aware recovery tells a crash from corruption (06 §A3; chunk-store.md §3.2).
   The log reports the damage and its replicas recover from their peers. It never
   truncates silently.
5. The block where the next frame goes is overwritten with zeros and flushed before any
   write. A frame there was never acknowledged, but it may be partly durable: its header
   whole while its last sectors, or the file's end, are not. Left alone, a later crash
   could complete it with the zeros of a newer frame's padding and bring back an update the
   replica had already been told was lost, such as a vote.

Recovery reads the live log once, sequentially. Reclamation bounds the live log (§5), so
the time to recover is bounded by the live bytes over the device's measured sequential
read rate.

## 7. What a replica reads

A replica wraps its group's view in focal-raft's `Storage` trait: `initial_state` from the
hard state, the engine's configuration and the held proposals; `entries`, `term`,
`first_index` and `last_index` from the group's slots; and `snapshot` from the engine
(07 §1.2). The log serves the view, and the replica updates it through the log only after a
submission is durable, since `Storage` is what is durable, as the core reads it.

## 8. Testing

The log is written against `mantle-disk`'s `BlockFile`, so the same code runs on a real file
and on the simulated device. That device loses unflushed sectors at a crash, tears
multi-sector writes, fails flushes after marking pages clean, and flips bits on read
(mantle-disk sim.rs). The tests cover these properties:

- every acknowledged submission survives any crash;
- a torn tail is cut and corruption is reported;
- reclamation never loses a live record;
- a group's state after recovery equals its state before the crash, less what was never
  acknowledged.

These tests run over generated histories of groups appending, conflicting, compacting,
installing snapshots and leaving. A benchmark measures submissions per second and their
latency against the device's measured flush rate.

## 9. Open

- When to sweep beyond the need for room, by LFS's cost-benefit [RO92 §3.6] once the
  workload is measured. A sweep reads a whole segment in one batch, which that batch's
  submitters wait for.

- The window of entries a leader keeps for lagging followers before it sends a snapshot,
  as a function of the snapshot's cost and the follower's measured lag.
- Whether hard states also get a periodically written second copy, as protocol-aware
  recovery keeps its metainformation twice (06 §C.c, item 5), or whether recovering a
  damaged hard state from peers suffices.
