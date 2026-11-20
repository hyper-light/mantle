# The Raft log: one log per metadata device, shared by every range on it

Status: design, 2026-09-28; waking, alignment and power 2026-09-30. Sources: docs/research/06
(consensus, cited as "06 §x"), 07 (focal's consensus stack), 03 and 11 (I/O and the
operating-parameter models), 26, 28 and 29 (concurrency, power, device classes), 01
(Tectonic, Ceph BlueStore as [AWK+19]); docs/design/chunk-store.md, whose techniques this
log reuses, and docs/design/metadata.md §3, which places it.

Code: `hyper-log`, vendored from hyper-raft (`vendor/hyper-log`, `vendor/UPSTREAM.md`), which
took mantle's `crates/log` with its history (step L-1 of research/32 §5.2) and holds its state
in one owner thread answering by ticket (L-2); the format and every rule here are unchanged.

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
a multiple of the file's alignment `B`: the largest of 4 KiB, the device's logical and
physical block sizes and its write unit, as for the chunk store (chunk-store.md §2). A log of
small frames is where a unit above the physical block costs most: each frame padded to 4 KiB on
a drive whose unit is 16 or 64 KiB made the drive rewrite its unit once per frame (research/29
§3.3), so the padding goes to the unit. Before, `B` took the logical and physical sizes alone.

- **The persist area.** The file's first segment's length holds two persist slots, and the
  segments follow it. Each frame's flush also writes a persist record into the slot of its
  sequence's parity: for each group the frame carried, the hard state and start it wrote,
  where its entries began, how many there were and the last one's term, whether it removed
  the group, and whether it held proposals. A record also says the last frame known flushed
  when it was written: the frame before it, which it was written only after. It is
  checksummed whole. Protocol-aware recovery keeps such identifiers apart from the entries,
  "at least a few megabytes physically apart", because a misdirected write can corrupt an
  item and an identifier stored beside it (AGL+18 §3.3.4); a segment's length away from every
  frame, at the sizes a device's geometry gives (16 MiB), is past the 10 MB within which
  latent sector errors cluster [BGPS07 §5]. Each slot holds a record of the log's most groups.

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
    `term`, which the log keeps so that the Raft core can ask the term of the entry before
    its first. The replica's engine made the entries before it durable, or the replica
    installed a snapshot there; a snapshot that discards what follows it comes with
    `Entries` of none from `index + 1`.
  - `Proposal { index, term, bytes }`: an entry the replica approved by itself on the fast
    track, held beside its log until the log reaches that index, even if a later conflict
    shortens the log again (07 §1.4).
  - `Removed`: the replica left this device, and all of its records are dead.
  - `Uncertain { index, term }`: the group's log may lack entries through `index`, of terms up
    to `term`, that a frame no longer readable held (§6). It ends once the log reaches
    `index` again, or holds an entry of a later term, and until then it is a live piece,
    copied by reclamation like a hard state.

A replica's `Ready` becomes one submission of all its records, and they go in one frame
when they fit one. A larger update goes in parts that each fit, entries first and hard state
last, as etcd's raft writes a `Ready` when its store cannot write atomically (06 §A10.3;
replica.md §3); the replica's `Ready` is done only once every part is durable. The
frame's single CRC makes a frame's records durable together, which meets etcd's rule that entries and
hard state are persisted before the messages that depend on them (06 §C.c, item 1). The log
stores entries and hard states as the replica gives them. It knows indexes and terms and
nothing of Raft's message formats, so it depends on no Raft crate.

## 3. Writing

One writer per log runs the chunk store's group-commit loop (chunk-store.md §4), as a state
machine on its device's issuer (node.md §1.2), which is one thread for everything on the device. A
bounded queue admits submissions by count and by bytes and refuses past either with `Busy`
[research/11 §4] (§3.1). A submission holds its room until it is answered, so the bound
covers updates waiting to be taken, those held for a later frame, those being written and
those flushed and awaiting their confirmation, and each group holds two at most, the two
batches' worth of a group that has one update in each frame; a hot group waits for its own
room and never takes the others'. When more waits than a frame holds, the writer chooses
what goes by class and by fair share of bytes (§3.2). The loop takes everything that arrived while the
last batch was being made durable, encodes one frame, writes it, flushes the file once
with the platform's full flush, and publishes the records to readers. It answers the frame's
submitters once a later durable record confirms that flush (§6): the next frame's persist
record, when work is queued, or a confirmation written on its own at once. Replicas submit in a closed loop, so the writer waits for the replicas it
just answered as long as that is expected to lower total latency, the wait the chunk store's
writer derived (`hyper_block::commit`). Without it, a few replicas alternate between batches
and each update waits for two flushes
(docs/measurements/2026-09-28-raft-log-benchmark.md, finding 2).

A frame is laid out once: its header, whose checksum covers the payload where it lies, and
then the payload, go straight into the aligned buffer the last frame used, kept while it is
large enough, where before the payload was copied into a frame and the frame into a fresh
zeroed buffer each time (audit §12.2). That is one whole-frame copy and one allocation fewer
a frame; on this machine a flush's variance hides it in throughput, and peak memory of a
16 KiB-entry benchmark fell from 88.6 and 82.1 MB to 80.9 and 71.0 MB in two alternating
pairs of runs.

That wait runs on the clock, and ends once the batch holds a frame's worth of charged bytes
(§3.1): what came next would go in a later frame whatever the wait. `Config::waits` chooses: `Waits::Measured` is what a node
runs; under `Waits::Never` a batch is what is queued when the writer looks, for the replica
simulation, whose members have one update out at a time and so never return within a wait.

A group's updates become durable in the order submitted. An update that waits for a frame
with room holds its group's later updates behind it for that frame, so a newer hard state
never lands before an older one.

A replica cannot have its update refused: once the core has handed over a `Ready`, it takes
no other call until the `Ready` is made durable (07 §1.2). So a replica submits by waiting
for room in the queue instead of taking `Busy`. Waiters wait in arrival order, each in a slot of
its own; room the writer frees is handed to the waiters it fits, in order, and only those are
woken, one wake each, and a fence completes every waiter's slot with its answer once (node.md
§1.3). The waiting list is bounded by construction: a group waits with at most its two
submissions, and the log holds at most `max_groups`. The wait lasts no longer than the writer's
progress. Before, room was a condition variable the writer broadcast to every waiter on every
answered submission; on macOS that broadcast walks every waiter inside one kernel spinlock, and a
benchmark of thousands of replicas, each a thread, held it until the kernel panicked
(research/26 §1.3–§1.4).

The writer's wait follows the chunk store's rules for power (chunk-store.md §4): on battery or
in saver mode it waits for its returning replicas up to the measured batch service time, which
loses no throughput and at most doubles a lone update's latency (research/28 §4.5, D14), and an
update is answered when it is durable whatever the power. The log does not overlap frames even
on a device whose flush measured free (measurement.md §9, C2), where the chunk store does
(chunk-store.md §4): a frame's persist record states that the frame before it was flushed, and
recovery's rule that a valid later frame proves every earlier one acknowledged (§2, §6) rests on
each frame being written only after the one before is flushed. A log of depth past one keeps
several frames in flight and narrows that rule to the frames outside each frame's window (§3.3).

A replica that leads may send its appends to followers before its own flush completes;
followers answer only after theirs (06 §C.c, item 2). The log lets both happen: a
submission returns at once with a handle, and the replica waits on it only for what must
follow durability.

A failed write or flush fences the log, and no later submission is taken. So does a failed
read of the tail being swept, and every update the writer holds is answered `Fenced`
wherever its commit failed: those laid into the frame, those of the batch not yet reached,
those held for a later frame, and those of the frame before, whose confirmation will not
come. A commit that failed before it wrote its frame once left the last two unanswered, and
their submitters waited as long as the log lived (audit review R08). A failed flush
is never retried, because the kernel may already have marked the pages clean [RPA+20 §3].
The log must be reopened, and recovery trusts only what verifies.

### 3.1 The queue's byte bound

A submission is charged what it holds: the bytes its records take in a frame's payload,
record and entry headers included, and its group's row in the frame's persist record, 97
bytes (§2). An entry of no bytes costs its 29-byte record header and 16-byte entry header besides
its row, and an update of no records its row. The queue once counted entries' and proposals'
payload bytes alone, which charged such updates nothing, so a flood of them was bounded only
by count (audit S03). A submission whose records take more than a frame holds is refused
`TooLarge` when it is sent, before it holds any room, since no frame could take it.

The bound is three frames of the largest charge, `B = 3·(F + 97)`, where `F` is the payload
a frame holds, a segment less its header block and the frame header. It is derived, not
configured. Three frames are what the writer can have in hand at once: the frame flushed and
awaiting the persist record that confirms it (§6), the frame being written, whose record
does, and the frame gathering while that flush runs. With two, the next frame could gather
only once a flush had ended and would go out short; bytes past the third cannot be written
before the third flush from now, so by Little's law they add only waiting [research/11 §4,
§5.2]. A group's two submissions hold at most two thirds of `B`, so one group never holds
the last frame's worth that the others need.

### 3.2 Sharing a full frame

A frame takes at most one update of a group, as before. When the updates waiting do not all
fit, the writer orders them and walks the order once, laying each that fits and passing over
each that does not. The order is by tier, and within a tier by start tag:

1. updates an earlier frame passed over for room, those passed over first ahead, and those
   of one frame by class;
2. latency-class updates, then normal, then background.

The node sets the class with `Log::submit_in` and `submit_waiting_in`; `submit` and
`submit_waiting` are normal. The classes are Tectonic's TrafficClasses, "Gold, Silver or
Bronze, corresponding to latency-sensitive, normal and background applications", ordered on
Tectonic's storage nodes by three protections: a lower-class request may cede its turn to a
higher class "if the request will have enough time to complete after the higher-TrafficClass
request", non-Gold traffic is held back while Gold waits, and "scheduling of non-Gold
requests to a disk stops once a Gold request has been pending there for a threshold time"
[research/01 §1.11, TEC §4.1]. Here a lower class cedes its place in a frame to a higher one,
and its threshold is one frame: once passed over it goes before every class, since the
writer decides once a frame and cannot wait less than one. A group's update takes the later
of its own place and that of its update submitted before it, as Tectonic gives traffic that
borrows another TrafficGroup's resources "the minimum TrafficClass of the two", so a group's
updates stay in the order submitted (§3) whatever their classes.

Within a tier, groups share the frame by start-time fair queueing in charged bytes [SFQ96].
A submission taken by the writer is stamped with the start tag `S = max(v, F_prev)`, the
larger of the virtual time and its group's last finish tag, and the finish tag `S + charge`
[SFQ96 §2, eqs. 4–5]; `v` is the start tag of the last update laid into a frame, and when the
backlog empties it is set to the largest finish tag, as SFQ sets it at the end of a busy
period. For any interval in which two groups are backlogged, the difference in the bytes
they are served, over their weights, is at most their largest charges over their weights
[SFQ96 Theorem 1], and the proof makes no assumption about the server's rate: "Theorem 1
holds regardless of the characteristics of the server". A frame's service time varies with
the device's flush and with the frame's size (research/11 §5.3), so fairness that does not
depend on a known rate is what the log needs, and weighted fair queueing, which computes its
virtual time from the link's capacity, does not give it [SFQ96 §1]. The delay bound depends on
the other flows' largest packets rather than on a flow's share [SFQ96 Theorem 4], which gives
"low average as well as maximum delay for low-throughput applications" [SFQ96 §1]: a group
with small updates goes ahead of one that has just had a large one written. SFQ also reports
"considerably better fairness properties and smaller maximum delay than DRR" [SFQ96 §1],
which node.md §2.3 uses for a shard's turns; the log's server is the frame, whose capacity
varies, where the shard's is a pass. The finish tags are kept for the groups the log holds or
that have an update waiting, and only while ahead of the virtual time, so they are bounded by
`max_groups` and `queue_submissions`.

Every group weighs the same. Tectonic's class shares and thresholds are not published
(research/01 §1.11), and no caller yet has a deadline from which a class's rate could be
derived (research/11 §4.5); the classes order tiers instead of weighting one group's bytes
over another's. Tectonic's weighted round robin "provisionally skips a TrafficGroup's turn if
it will exceed its resource quota" (research/01 §1.11). The start tag is that quota: a group
that has had more than its share has a start tag ahead of the others and is walked after
them. Skipping it outright would leave room empty that it could fill, where SFQ serves it
whenever the others leave room.

**Bounds.** A latency-class update is laid in the first frame that walks it unless the
updates passed over before it and the latency updates ahead of it fill that frame. Any update
is walked in the first frame after the writer takes it, or the next if its group's update
before it is in that one. Once passed over it is laid within `2⌈B/F⌉` frames, eight at these
sizes: every update ahead of it in the first tier was taken before it and waits with it, so
together they hold at most `B`; in any two consecutive frames before it is laid, the first
passed-over update not laid in the first frame is laid first in the second, and what the
first frame laid ahead of it and it together exceed `F`. Frames that sweep the tail (§5) are
not counted. `latency_traffic_does_not_starve_background_work` and
`hot_groups_do_not_starve_a_cold_one` step the writer a frame at a time and find such an
update written in the frame after the one that passed it over.

**Measured** (`crates/log/tests/fairness.rs`, now hyper-log's `tests/fairness.rs`, the simulated device stepped a flush at a time,
400 frames a mix, so each run is the same run). A hot group keeps two updates of a share of a
frame outstanding, cold groups one each, all closed loops. Waits are frames from sending to
being written; two is the least, the frame flushing when the update is sent and the next.

| Mix (share of a frame) | Arrival order (before) | Passed over first, then arrival | SFQ with class tiers |
|---|---|---|---|
| hot 0.9, 8 cold 0.05 | cold wait mean 2.403, max 4; hot 0.665/frame, cold 3.31/frame | 2.402, max 3; 0.662, 3.32 | **2.004, max 3; 0.497, 3.98** |
| hot 0.6, 16 cold 0.05 | 2.003, max 3; 0.665, 7.96 | 2.003, max 3; 0.665, 7.96 | 2.003, max 3; 0.665, 7.96 |
| hot 0.9, 4 cold 0.3 | 4.003, max 5; 0.500, 0.99 | 2.008, max 3; 0.662, 1.00 | 2.008, max 3; 0.662, 1.00 |
| hot 0.9 background, 8 cold 0.05 latency | 2.403, max 4; 0.665, 3.31 | 2.402, max 3; 0.662, 3.32 | **2.000, max 2; 0.497, 3.98** |

SFQ with class tiers is kept. Where the frames are full it gives the cold groups every update
they ask for, at the least wait, and the latency class never waits past the least. The hot
group's rate falls from 0.665 to 0.497 updates a frame in the first mix: it asked for 0.9 of
every frame while the cold groups asked for 0.2, and max-min fairness serves the smaller
demand whole [SFQ96 Theorem 1]; an update of 0.9 of a frame fits only beside less than 0.1
of others, so the frames the cold groups share carry less in all. Arrival order let the hot
group take that room and the cold groups waited for it, and passing over for room first
bounded the waits of large cold updates (the third mix) but not the share.

[SFQ96]: P. Goyal, H. M. Vin, H. Cheng. "Start-time Fair Queuing: A Scheduling Algorithm for
Integrated Services Packet Switching Networks." SIGCOMM '96, pp. 157–168.
http://conferences.sigcomm.org/sigcomm/1996/papers/goyal.pdf

### 3.3 Frames in flight past one

One frame's flush on the device at a time leaves the device's flush concurrency unused. On the
gate machine, an Apple M3 Ultra whose full flush is `F_FULLFSYNC`, 4 KiB writes each flushed on a
file of its own ran 207 flushes a second from one flusher, 278 from 4, 441 from 8 and 503 from 16,
at a median of 4.3, 13.6, 14.1 and 18.6 ms (the owner's measurement, 2026-11-20); btrfs ran 124 a
second from one writer and 466–478 from 6 to 16 (research/71; node.md §2.9). A writer that keeps
the writes and flushes of up to `K` frames on the device at once, the log's depth, commits up to
`K` frames' groups in the time of about one flush. This section states what that can leave on the
device after a crash and the rules that keep every answered write and take no unanswered one.
Depth 1 is the log as §3 and §6 describe it, and is the setting's default.

**What a crash can leave.** A flush makes durable the writes that completed before it was issued,
not those still in flight (research/02 [M14]: an `IORING_OP_FSYNC` "does not wait for earlier
writes"), and a drive may keep later writes and lose earlier ones (research/02 §3.8, quoting
fsync(2) on macOS: "later writes may be present, while earlier writes are not"). With two frames
in flight, frame `N+1`'s write can complete before `N`'s and its flush end while `N`'s write is
still going, and flushes can end in either order. A crash can then leave `N+1` whole and `N` lost
or torn. At depth 1 that cannot happen: `N+1` is written only after `N`'s flush, so a valid `N+1`
proves `N` was durable (§6, step 4).

**The window.** The writer lays frames in sequence order and lays frame `M` only while fewer than
`K` frames are unapplied. It applies completions in sequence order: a frame whose flush ends before
an earlier frame's waits for it, and its place in the window is freed when it is applied, not when
its flush ends. So when `M` is laid, every frame through `M−K` has been flushed and applied, and
the frames that can share the device with `M` are `M−K+1` to `M−1`. `M`'s header records the depth
it was laid at, in the first of the header's three padding bytes, as `K−1`: zero, which every frame
written before depth existed holds, is depth 1. Recovery reads the window from each frame, never
from the setting of the binary that opens the log, which may have changed since.

**Answers.** A frame is answered only once it and every frame before it are flushed, applied in
order, and a durable persist record says so. A record's `confirms` is the highest frame applied
when the record was laid, so it rises with the sequence, and a frame is answered once a record
whose `confirms` reaches it is durable: the record of a frame laid after it was applied, or its own
confirmation written on its own (§6, step 5). At depth 1 this is the rule of §3 and §6: frame
`N+1`'s record confirms `N`. No frame is answered earlier than at depth 1. The owner's decision of
2026-11-20, that a frame on a write-back device is answered only once confirmed
(`AtFlush::DECIDED`, `WriteBackFrames::Confirmed`; durability.md §7), holds at every depth. A log
that answers frames at their own flush (durability.md §7.5) keeps depth 1: answered at its flush,
a frame of a lost frame's window was answered if the lost frame had been applied, and nothing on
the device tells whether it had, so recovery takes such a record past the gap for damage.

**Records.** A frame's record goes in a slot before its flush (§6, step 5). At depth `K` the area
holds `K+1` slots, the record of `M` in slot `M mod (K+1)`, which writes over the record of
`M−K−1`. Every frame through `M−K` was applied when `M` was laid, so the record of `M−K` is
durable, and it and each later record confirm at least what the overwritten one did. So at every
moment some slot holds a durable record that confirms every answered frame. Two slots do not
suffice past depth 1: the records of two frames in flight can both tear, taking the only
confirmation of the frames answered before them, and an answered frame damaged at rest, followed
by one of its window, would then be cut as a crash. At depth 1, `K+1` is today's two slots.

**Recovery: a frame past a gap is never taken.** Recovery walks the frames to the first that does
not read, `G`, the frame after the last valid one. For each valid frame `M` past it in its
segment:

- `M ≥ G + depth(M)`: every frame through `G` was applied before `M` was laid, so `G` was durable
  and the acknowledged state was damaged. The log reports it, as §6, step 4 says.
- `G < M < G + depth(M)`: `M` is in `G`'s window and may have shared the device with it. Answering
  `M` needed `G` applied and a record confirming it, so `M` was answered only if `G` was durable.
  The records tell: a readable record whose `confirms` reaches `G` proves `G` was applied, and its
  loss is damage, reported. With none, `G` was never confirmed (records, above), so neither `G` nor
  any frame after it was answered, and the crash cut the log at `G`. Recovery replays through
  `G−1`, takes no frame of the window, and writes zeros over each window frame's first block before
  any write, flushed with step 6's erasure. Left whole, a window frame keeps its segment's identity
  and a checksum that holds, and the walk would take it once a later frame ended where it begins.
  So does each frame of the segment past `G` whose header reads and whose checksum fails: a torn
  frame of the window, which a later write torn over it could complete, as step 6 says of the
  block at `G`.
- Any other valid frame past the stop, one of `G`'s own sequence elsewhere in the segment, is
  damage.

At depth 1 the window is empty and every valid frame past the stop is damage, which is §6, step 4.
An answered frame `F` survives: every frame through `F` was applied and a record confirming `F` was
durable when `F` was answered, and that record or a later one still reads, so a lost frame at or
before `F` is reported rather than cut, and with none lost `F` is replayed. An unanswered frame is
never taken: recovery takes no frame past `G`, and cuts at `G` only once the records show `G` was
never answered, so no frame after it was either. This is protocol-aware recovery's rule, that a
crash is told from a corruption by what the protocol could have acknowledged (AGL+18 §3.3.3;
research/97 §2), with the window in place of the one frame before.

**Segments and sweeps.** A segment's header is flushed with its first frame, and recovery's search
for frames of a header that no longer reads rests on that (§6, step 4). So the frame that opens a
segment is laid alone: the writer lets the window drain before it and lays nothing after it until
it is applied, and every window lies within one segment. A sweep, which reads the tail and moves
what is live, also runs alone. A confirmation written on its own takes a place in the window as a
frame does, and writes only its own frame's slot.

**Choosing K.** `K` is the device's knee: the depth past which more flushes in flight add latency
faster than they add flushes a second. On the gate machine two flushers ran no more flushes a
second than one (210 against 207) at twice the latency, eight ran 2.1 times as many (441), their
median flat at 13–14 ms from three to eight, and sixteen ran 14% more than eight (503) at 32% more
latency (18.6 ms against 14.1). Those flushers each flushed a file of their own; a log flushes one,
and whether one file's flushes overlap on the device, on APFS, ext4, XFS and NTFS, is for the
calibration to measure (§5, mantle CLAUDE.md §5). The knee is the device's, shared by every log on
it: where a cell runs three nodes on one SSD, as on the gate machine, three flushes are in flight
at depth 1, and each log's share of the knee is what its depth may take. Until hyper-block's calibration measures concurrent flushes of
one file, `K` is a stated setting, default 1, and at most 16, the most either measurement ran;
neither says what lies past it. Each frame in flight holds its room (§3.1), so the queue's byte
bound becomes `(K+2)·(F+97)`: the `K` frames in flight, the one gathering while they run, and the
one flushed and awaiting its confirmation, today's three at `K = 1`.

Steps, each built and tested before the next:

1. This section, and recovery's rule that a frame past a gap is never taken: a frame's header reads
   its overlap; a frame past the gap by more than its overlap is damage, reported; one within it
   is cut and erased, unless a record confirms the gap or says a frame from the gap on was
   answered at its flush, which is damage (hyper-log `recover.rs`, `tests/log.rs`). Every frame
   written so far is depth 1, so every log opens as before.
2. The persist area's `K+1` slots, laid again on open when the depth changes, as the move to pieces
   lays it (durability.md §7.7); the open reads every slot.
3. The setting, the writer's window and the device's `K` jobs, completions applied in order, the
   opening and the sweep alone; the queue's bound at `K`.
4. The depth from calibration, measured on one file, where hyper-block measures it.

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
oldest segment, provided sweeping it makes room (below). It reads the segment and, for each piece the
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
segment is reused only once a durable frame names a tail past it.

**Room.** A sweep makes room only if its frame takes less of the file than freeing the tail
gives back. Freed, the tail's slot holds a whole segment's frames again, however few it held:
frames go only at the head, so what a segment left unwritten when a larger frame opened the next
is room only once its slot is freed. The frame takes its own bytes at the head, and the head's
unwritten rest besides where it does not fit there and opens a segment. The copies take at most
the tail's live bytes and a relocated record's header for each live piece, as if no two entries
ran together. The tail is swept only when those copies fit one frame, and either that frame takes
less than a segment's room or the segment after the tail, unless it is the head, holds nothing
live, since the frame naming the later tail frees both (tails are freed oldest first, so a dead
segment waits for the tail to pass it). A tail of one large live entry is not swept, since its
copy would take a segment's room. Before, the sweep weighed the tail against its frames, the bytes
they took padded since the segment opened: a tail of one block's frame, holding a hard state and a
released mark (66 bytes), left behind when the next frame opened a segment of its own, copied to
one block, no smaller than its frames, and was never swept. On a file of three segments the next
held one entry and the head the group's start, 273 bytes live in all with 14 blocks free in the
head, and the log answered `Full` for good: the node's simulation stalled a leader so on 41 of 100
seeds (docs/measurements/2026-11-14-log-full-wedge.md; hyper-raft `277dd17`, found by the KIND
founder's full log of 2026-11-07). Before that, a group's hard state left alone in the oldest
segment, in a frame as small as its copy, was never swept, and the writer once swept any tail whose
live bytes were below a frame's room: a tail with nothing to free was copied into a segment of its
own, freeing one and taking one, and the writer swept segment after segment without ever answering
the update waiting. A log whose segments were all live never answered `Full`.

The last usable segment, and while none is usable the head's remaining room, are kept for making
room. A frame that names a later tail, its sweep's or the one past segments emptied since the last
frame, frees a segment as it becomes durable, and takes that room. A frame whose every update only
frees what its group holds (a compaction or snapshot's start with no entry or proposal, a removal,
or a damage fence) takes it only where what it leaves still holds the frame that then frees a
segment: one block where its updates leave the tail holding nothing live, or else the tail's sweep,
its copies counted as above less what the updates kill there. A frame that does neither never
takes it. So the frame a full log waits for always has somewhere to go: while no segment is usable
the head holds it, and once it is durable a segment is. Before, a compaction that freed no
segment took that room as one that does. Compactions a little at a time, a block each, filled
it; the tail's sweep and the frame naming the tail past what they emptied no longer fitted, and
the log answered `Full` for good to every write, the groups' compactions too: hyper-log's
generated histories (below, §8) wedged so on 927 of 2,304 at hyper-raft `294678c`. Before that, an
update frame could fill the head, and the compaction that would free the log found no room.

A frame no segment takes is laid again with what may take the room kept: the sweep's copies
and the updates that only free what their groups held, the rest waiting for the next frame. A
frame that frees a segment and still does not fit goes with its copies alone, its updates waiting
for the room it frees. Before, a frame was refused whole: a sweep that made room, or the frame
naming the tail past segments a compaction emptied, did not fit beside the update waiting while
no segment was usable, and every write after was refused; and a compaction laid beside another
group's entries was refused with them. When even that finds no room, its updates are refused
`Full` and the groups compact. If the frame was only a sweep, the updates it was making room for
are refused too, since no room can come. The writer also counts frames in a row that swept and carried no update while
updates waited. Each such sweep reclaims the tail's dead pieces and padding, and its copies
are packed and live. So after `max_segments` of them every segment has been swept once,
and the updates still waiting are refused `Full`. This is the loop's counted budget
(CLAUDE.md §2), derived from the log's size rather than chosen. Sweeping only while updates
wait keeps an idle or full log from reading its tail again and again.

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
4. Where a segment's frames stop, whatever no longer reads there, its checksum or its
   magic, format or identity, the stop is a torn tail only if no valid frame with a later
   sequence follows it, anywhere after it in its segment or in a later one: it was never
   acknowledged, so the log is cut there. If a later frame does follow, the acknowledged
   state was damaged, as protocol-aware recovery tells a crash from corruption (06 §A3;
   chunk-store.md §3.2); in a log of depth past one, a later frame within its own window of the
   stop proves nothing of it, and is cut unless a record confirms the stop (§3.3). The same
   holds one level up: a segment's header is flushed with
   its first frame, so a slot whose header no longer reads but which holds this log's
   frames past the first, from a segment newer than any whose header reads, lost a header
   that was durable. The log reports the damage, writes nothing, and its replicas recover
   from their peers. It never truncates silently. For the same reason every live segment
   before the last frame's holds a frame at least: sequences are checked from the first frame
   replayed on, so a tail segment whose frames no longer began where they should once
   dropped out of the replay whole, and the groups whose only records lay there vanished
   without a mark (audit review R17).
5. The last frame has no later frame to vouch for it, which protocol-aware recovery leaves
   ambiguous: "if e_i is the last entry, then we cannot determine whether it was a crash or
   a corruption" (AGL+18 §3.3.3). Its persist record answers what it held, and a confirmation
   answers whether its flush completed: the next frame's record, written only after that
   flush, or, when no frame follows at once, the frame's own record written again, in its
   own slot, saying the frame was flushed. A frame's updates are answered only once its
   confirmation is durable, so an
   unconfirmed frame was never acknowledged, and the ambiguity AGL+18 leaves to the
   replication protocol does not arise.
   - A confirmed frame that no longer reads was acknowledged and damaged since. Each of its
     groups gets back the start and hard state the frame left, its entries cut back to where
     the frame's began, and an `Uncertain` mark through the frame's last entry; its commit is
     cut to the entries left, and the leader tells the replica its commit again. A group whose
     frame held proposals, which the record does not carry, is damaged: the log serves it to
     no one, since a replica opening it afresh would vote again in a term it voted in, and
     takes only its removal, after which it is rebuilt from its peers. The replica keeps out
     of elections while it is marked (replica.md §4).
   - A group found damaged, by a record missing or by a lost frame's proposals, is fenced by
     a `Damaged` record written before the log serves anyone, which replaces whatever the
     group held and lives, copied by sweeps, until the group's removal. What showed the
     damage does not last: the next frame overwrites the lost one, and a sweep may reclaim
     the segments around a missing record, and without the fence the next open found the
     group as its older records left it (audit S16). The fence's persist record carries it
     too, and recovery keeps a fence whether or not its frame was confirmed, since a power
     cut can tear the frame that wrote it after the one that showed the damage was erased,
     and marking a group damaged is always safe.
   - An unconfirmed frame was never acknowledged, and its persist record can reach the disk
     while the frame tears. It is the torn tail, but for its term and vote, which are kept:
     raising a term or keeping a vote is always safe. Its commit is not kept: it may name an
     entry only the frame held.

   Before, a frame was answered at its own flush, and one damaged before its confirmation
   was durable could not be told from a torn one: the audit (S01, round three) cut power
   after the flush and before the confirmation, damaged the frame, and recovery dropped an
   acknowledged entry without a mark. Marking every unconfirmed frame instead would have
   closed that at a cost the simulation measured: a crash that tore a frame after its record
   landed marked entries no one had acknowledged, and a group with one member lost and that
   one marked could elect no one; a power loss that tore the frame on a leader and a follower
   alike would leave them both marked. Answering after the confirmation costs one more flush
   of latency where no frame follows at once
   (docs/measurements/2026-09-29-log-confirmation.md).

   durability.md §7 answers a frame at its own flush again, and has the group decide what the
   device cannot (research/97): there an unconfirmed frame whose record reads is undetermined,
   restored and marked as a confirmed one is, and resolved as CTRL resolves a faulty entry
   (AGL+18 §3.4), with the confirmation kept where the others could not resolve it without the
   member. The open reports which of the three it found (`Recovery::tail`: `Torn`,
   `Undetermined`, `Lost`). Until that rule lands, an unconfirmed frame is the torn tail, as above.

   Where each record goes is what keeps these answers. A record is written into its slot
   before its frame's flush, and a record larger than what the device writes atomically, a
   sector, can reach the disk part old and part new (research/02, `physical_block_size` and
   `RWF_ATOMIC`); a record of five groups spans two 512-byte sectors. So a slot is written
   over only when what it holds is no longer needed, or tells of a frame never answered. The
   next frame's record takes the slot of the frame before the last, which the last frame,
   durable, has superseded. A confirmation written on its own is the last frame's record
   again, in the frame's own slot, where the record it replaces is of a frame not yet
   answered, which recovery may drop as it drops a torn tail. It was an empty record in the
   other slot: the next frame's record, written there before that frame's flush, took it
   away when it tore, and a frame answered and then damaged at rest was cut as a torn tail,
   its entries dropped without a mark (audit review R07). Format 3 records this.

   The restore is written through the writer, marks included, before the log serves anyone,
   so it survives the next restart. It is the writer's first batch and goes in one frame, or
   the open fails and writes nothing: sent one at a time, a restore could be taken in
   several frames, and a crash between them would leave the rest restored nowhere once the
   first had written over the lost frame's record. That frame takes the lost frame's
   sequence, and so its slot, and its record, written there before its flush, could tear and
   take the lost frame's record with it while the restore never became durable: the next
   open found no record, and restored nothing, marked nothing and fenced nothing (audit
   review R09). The open therefore copies the lost frame's record, saying whether the
   frame was confirmed, into the other slot, and flushes the copy with the erasure of step
   6 before the restore is written. The other slot then held the record of the frame before,
   which reads whole, or of a frame after, which never became durable, so was never
   answered, and whose confirmation of the lost frame the copy carries. Recovery reads a
   frame's record from its own slot, and from the copy where that no longer reads. A
   restore's record that landed beside a restore frame that tore is read before the copy,
   and restores the same state, being the restore of the lost frame's record.

   A persist record says what a frame's updates made durable, not what its sweep moved. A
   lost last frame that swept the tail freed that segment, and the next frame may already
   have opened in its slot. Where that opening tore after writing over the segment's header
   or first frames, the pieces the sweep moved were in the lost frame and nowhere else, and
   the frame before it, which recovery falls back to, names the overwritten segment as its
   tail. That is damage to acknowledged records no durable copy holds, as in an interior
   frame, and the log reports it and writes nothing (R17). Where the opening wrote nothing
   there, the segment replays and the lost frame is restored.
6. The block where the next frame goes is overwritten with zeros and flushed before any
   write. A frame there was never acknowledged, but it may be partly durable: its header
   whole while its last sectors, or the file's end, are not. Left alone, a later crash
   could complete it with the zeros of a newer frame's padding and bring back an update the
   replica had already been told was lost, such as a vote. Where the head is a segment newer
   than the last frame's, an opening whose frame never became durable, the block after the
   last frame is erased too. A frame there was flushed before the opening was written, and no
   longer reads: it is the lost frame, whose restore goes in the head. Left in its segment,
   which is then the tail, it was taken by the restore frame's sweep for a live frame damaged,
   the sweep fenced the log, and the open failed where the frame was to be restored (R18).

Recovery reads the live log through a window of one segment, the most any frame takes. A
frame the window holds is verified there, and the window moves to the start of one it does
not hold, which then lies wholly within it, so each segment is read in one read, or two where
a walk begins part way, and the searches past the last frame and through slots whose header
does not read examine every block from memory. The file's length is taken once. Before, each
frame took a read of its first block, another of the whole frame and a look at the file's
length, and each search read a block at a time: reopening a log after five seconds of
appends took 98–365 ms on this machine's SSD, and takes 8–108 ms (audit P07;
[measurements](../measurements/2026-09-29-recovery-reads.md)). Reclamation bounds the live log
(§5), so the time to recover is bounded by the live bytes over the device's sequential read
rate. The writer's sweep reads the tail segment through the same window.

## 7. What a replica reads

A replica's core reads its group through hyper-durable's group handle, `GroupStore`, which
the shell serves as the core's `Storage` trait (hyper-raft `docs/durable.md` §8):
`initial_state` from the hard state the log held when the member opened, the configuration the
range's engine holds and the held proposals; `entries`, `term`, `first_index` and
`last_index` from the group's slots; `any_entry` by walking the slots a segment's bytes at a
time without taking the entries; and `snapshot` from the image the shell last prepared
(07 §1.2). The handle's view also carries the group's uncertainty mark, if any (§6). The
log moves the view only as it answers a write, since `Storage` is what is durable, as the
core reads it. The handle writes entries as the replica's store did before it (an entry's
kind, its context's length, its context and its data), so a log written before the shell
opens under it.

Entries no longer in memory are read from the file, a run of them whose blocks touch in one
read, as an update's entries lie together in its frame; each is verified as its group's entry
of its term, and one moved since its place was taken is looked up again. Reading back one
replica's 86–116 kept entries, one to a one-block frame, took 5–6 ms and takes 0.4–1.1 ms. Entries of many groups interleaved in larger frames lie apart and are still read
one at a time (audit P07).

## 8. Testing

The log is written against `hyper-block`'s `BlockFile`, so the same code runs on a real file
and on the simulated device. That device loses unflushed sectors at a crash, tears
multi-sector writes, fails flushes after marking pages clean, and flips bits on read
(hyper-block sim.rs). The tests cover these properties:

- every acknowledged submission survives any crash;
- a torn tail is cut and corruption is reported, damage to any field of a frame or of a
  segment header included, and slots reused by a newer segment prove nothing about it;
- a group's updates held behind a frame's room become durable in the order submitted, whatever
  their classes;
- the queue refuses past its byte bound with `Busy`, charging empty entries their records;
- a full frame takes updates by class and by fair share, and an update passed over for room
  is written in the next frame under hot traffic of any class;
- a log whose segments are all live answers `Full` within one frame and one sweep's reads,
  takes the compaction it waits for even after writes that followed a small compaction,
  and goes on once groups compact; a reopened log sweeps the segments it recovered; once
  its groups compact, a log that answered `Full` takes entries of any size up to a frame's
  room, though a small live record sits in its oldest segment;
- a log driven to `Full` by generated histories, entries of every size up to a frame's room on
  files of three to ten segments of four to sixteen blocks, one to three groups compacting part
  of their logs as they go, takes every group's compaction once it answers `Full`, whether the
  groups compact one at a time or together beside the write refused, and then that write
  (hyper-log `tests/full.rs`);
- reclamation never loses a live record, and a lost frame that swept the tail loses
  nothing unreported when the next frame's opening tears over the segment it freed;
- a group's state after recovery equals its state before the crash, less what was never
  acknowledged.

These tests run over generated histories of groups appending, conflicting, compacting,
installing snapshots and leaving. A benchmark measures submissions per second and their
latency against the device's measured flush rate.

## 9. Open

- When to sweep beyond the need for room, by LFS's cost-benefit [RO92 §3.6] once the
  workload is measured. A sweep reads a whole segment in one batch, which that batch's
  submitters wait for.

- Weights for the classes, once callers carry deadlines from which a class's share of a full
  frame can be derived (research/11 §4.5); until then the classes are tiers of equal weight.
- The window of entries a leader keeps for lagging followers before it sends a snapshot,
  as a function of the snapshot's cost and the follower's measured lag.
- Whether a segment a sweep freed waits to be reused until a frame after the sweep's is
  durable. The sweep's frame would then no longer be the only copy of what it moved while it
  is the last frame, and a torn opening in the freed slot would leave the log to open and
  restore it (§6, R17). It costs room: the frame after the sweep needs a place other than the
  freed segment, so the log would keep a second segment free where it keeps one (§5).
- Whether hard states also get a periodically written second copy, as protocol-aware
  recovery keeps its metainformation twice (06 §C.c, item 5). The persist record holds the
  last frame's hard states a second time; a hard state in an interior frame that is damaged
  still makes the log refuse to open.
