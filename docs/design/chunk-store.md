# Chunk store: how mantle writes bytes to a device

Status: design, 2026-09-28. Sources: docs/research/03 (cited by its keys, e.g. [RO92]),
docs/research/01 (Tectonic, Haystack, Ambry), docs/research/11 (models for the operating
parameters, cited as "research/11 §x"), docs/measurements.

A chunk is the unit one device stores: one replica or one erasure-coded shard of a
block. The chunk store owns a device and keeps chunks durably, verifies them on every
read, reclaims the space of deleted ones, and scrubs the rest.

## 1. Why a log-structured volume, not files per chunk

Ceph's FileStore/NewStore paid four device flushes to create an object on a journaling
file system; BlueStore on a raw device paid two and gained 50–100% write throughput with
an order of magnitude lower tail latency [AWK+19 §3.1.3, §6.1]. Haystack and Ambry keep
small objects in large append-only containers with an in-memory index so a read is one
I/O [HAY §3.4; AMB §2.2]. LFS shows how to reclaim such a log [RO92]. SSDs reward large,
aligned, sequential writes grouped by death time [HKA17 §3]; zoned devices require them
[BAH+21 §2.3]. So a chunk store is **one volume per device** — a raw block device or one
large file — laid out as a log of fixed-size segments, opened with direct I/O, with an
in-memory index. The same layout runs on a raw device and in a file, so a laptop and a
storage server run the same code.

## 2. Volume layout

All offsets and lengths are multiples of the volume's block size `B`: the larger of 4 KiB
[HL23 §2.2] and the device's logical and physical block sizes (`mantle-disk` identity).

| Region | Offset | Size | Contents |
|---|---|---|---|
| Superblock A | 0 | `B` | volume identity, geometry, checkpoint pointer, identifier reservations, sequence, CRC-32C |
| Superblock B | 16 MiB | `B` | same; written alternately with A |
| Index log | 32 MiB | `L` | circular log of index frames |
| Segments | after the log, segment-aligned | `n × S` | data records, written sequentially |

The two superblocks sit 16 MiB apart because latent sector errors cluster within 10 MB
[BGPS07 §5]; the current one is the valid copy with the higher sequence [RO92, checkpoint
regions]. `S` defaults to 256 MiB, the zone size of host-managed SMR drives and in the
range of ZNS zone capacities [BAH+21 §2.3; AD15], and is a volume parameter fixed at
format. A segment the size of a zone is what a zoned backend would map one to one, but the
volume has none: its superblocks, circular index log and reused segments rewrite earlier
offsets, which a host-managed zoned device refuses. Such a device node, and a zonefs file,
is therefore refused when it is opened, before any write; a file system mounted over a
zoned device places its own writes (audit B10; research/02 §6.4a). `L` is fixed at format
so that three index checkpoints of the volume's chunk budget fit (§5).

A volume on a raw device takes the device's capacity as its length and the device's own
full flush as its flush: `lseek` to the end and `fdatasync` on Linux, the disk ioctls on
macOS (where `F_FULLFSYNC` fails on a device node), `IOCTL_DISK_GET_LENGTH_INFO` and
`FlushFileBuffers` on Windows (research/02 §6.4a). Read as a file, a node's length is zero,
and recovery would find no superblock (audit §6.2).

A volume is laid out at its full size when it is formatted. Where the file system journals
the conversion of an extent on its first write, a durable write into preallocated space
that was never written pays for it at the flush: 5.7x the median of an overwrite on ext4,
nothing on APFS (docs/measurements/2026-09-28-flush-cost-by-extent-state.md). Calibration
measures it on the device at hand: small durable writes into never-written space, then the
same writes over the space they wrote, a penalty when the first writes' throughput interval
lies wholly below the overwrites' (research/11 §13.3, from GBE07 §3.3). Where it finds one, format writes the whole
volume once with zeros before its superblocks, so every append overwrites written blocks
[RO92; AWK+19-F6 reuses WAL files for the same reason]; where it finds none, format writes
only the superblocks. It finds 6.3x on ext4 and none on APFS
(docs/measurements/2026-09-29-first-write-calibration.md). Raw block devices have no extent
state.

## 3. Records

### 3.1 Data records

A group-commit batch lays records back to back from a block-aligned position in an open
segment, 8-byte aligned, and pads the batch to `B` [HKA17-F7: pad the partial page, never
rewrite it]. Small chunks therefore cost their size, not a block each [AWK+19-F7].

```
offset size field
0      4    magic "MNRC"
4      1    kind: 1 = payload
5      1    flags: bit 0 = final (the chunk is sealed at this record's end)
6      1    checksum-block shift k (checksum blocks of 2^k bytes; 16 = 64 KiB)
7      1    reserved (0)
8      16   volume id
24     4    segment number
28     8    segment incarnation (volume-wide, never reused)
36     8    sequence (volume-wide, never reused)
44     16   block id
60     4    epoch (layout generation of the block)
64     2    chunk index
66     2    reserved (0)
68     8    chunk offset of the payload
76     4    payload length
80     4    number of checksum blocks
84     8    write time (Unix ns)
92     4    CRC-32C of bytes [0,92) and the checksum table
96     4n   checksum table: CRC-32C of each 2^k-byte block of the payload
..     len  payload
```

The header identifies the chunk, its place in the chunk, and the segment incarnation it
was written in [BGS+08 §2.2: identity with the data catches misdirected writes; RO92:
segment summaries]. The payload's checksum blocks default to 64 KiB [GGL03 §5.2]; CRC-32C
is the Castagnoli polynomial [CBH93; Koo02]. A read verifies the header CRC, the identity
against what the index expects, the incarnation against the segment's, and the CRC of
every checksum block it returns.

### 3.2 Index frames

The index log holds a second copy of every record's identity and location, written in
separate I/Os from the data [BGS+08-F7; AGL+18-F4]: a lost write leaves a stale but
self-consistent record whose identity or incarnation no longer matches the index. A frame
is one group-commit batch of index updates:

```
frame header: magic "MNIX", version, lsn (u64), record count, payload length,
              CRC-32C over header and payload
records:      Put { key, segment, incarnation, offset, header+payload length, chunk
                    offset, payload length, sequence, whole-payload CRC-32C, flags }
              Delete { key, sequence }
              SegmentOpened { segment, incarnation }
              SegmentFreed { segment, incarnation }
              CheckpointBegin / CheckpointChunk { entries } / CheckpointEnd { count, crc }
padding to B
```

**Open question: the frame's cost.** A separate 4 KiB frame written before each flush
costs a quarter of durable bandwidth on the development machine's SSD
(docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 8), and on a disk it costs a
seek per batch. The separate copy is what turns a lost data write into a detected one; a
frame written in the same I/O as its data, or lazily after it (Haystack writes its index
asynchronously and rebuilds the tail from needles [HAY §3.5]), would give that up for
acknowledged writes the metadata service alone would then have to catch. Deciding needs the
field rate of lost writes [BGS+08] against the measured cost per device class.

Frames carry consecutive LSNs. Recovery tells a torn tail from corruption by what follows
an invalid frame [AGL+18 §3.3.3; GAA17 §4.2]: nothing with the next LSN means a crash
(the frame was never acknowledged, discard it); a valid frame with a later LSN means
corruption of acknowledged state, which is reported and recovered from the data records
(§6), never truncated.

## 4. Writing

One group-commit loop per volume [DKO+84 §5.2; research/03 G1]. Callers submit put,
append and delete requests to a bounded queue. It admits two batches of client requests,
by count and by payload bytes, and beyond that refuses with `Busy` before the payload is
copied: the loop takes at most a batch at a time, so by Little's law a longer queue adds
only waiting, about two batches' worth at this bound [research/11 §4]. A payload larger
than the byte bound is admitted into a queue holding no other payload, and one no record
can hold is refused as `TooLarge` before it is queued. The cleaner's relocations, one batch
at a time, pass beside the bound. The loop takes every request that arrived while the
previous batch was being made durable,
lays the data records into the open segment for their stream, encodes one index frame,
issues all of the batch's writes at once, then **one** flush of the volume (`sync_data`:
`fdatasync`, `F_FULLFSYNC`, `FlushFileBuffers`), and only then acknowledges every request
in the batch and publishes the new index entries to readers. Until the flush, a batch's
writes reach the device in any order however they are issued. Issuing the frame after the
records made a 32 MiB batch a fifth slower, and issuing it beside them costs nothing
measurable (docs/measurements/2026-09-29-frame-overlap.md). There is no artificial delay [research/03
G6]: under light load a batch is one request, under heavy load it is everything queued.
On the development machine a durable flush costs ~4.7 ms whatever its size
(docs/measurements), so this loop is what turns 245 flushes/s into tens of thousands of
chunk writes/s.

The writer keeps the segment table with what batches, the cleaner and the scrubber ask of
it: the free segments in order, the segments it can free without copying, the count in each
state and the live bytes, each kept as a segment changes. A batch changes only the segments
it writes into, seals, opens or frees, and hands readers only those, so its bookkeeping
costs what it did rather than a pass over the table; a 20 TB device of 256 MiB segments has
about 75,000. Before, every batch walked the table several times, built a map of every
segment in use, sorted every free one and copied the whole table for readers, and every
1 MiB scrub step summed every segment's live bytes (audit P06;
[measurements](../measurements/2026-09-29-segment-table.md)).

A volume runs three threads: the writer, the cleaner (§8) and, when scrubbing is on, the
scrubber (§9). The volume owns each from the moment it starts, so a start the operating
system refuses part way stops and joins those already running before the error returns,
and nothing is left holding the device (audit S12). Detached, the writer would keep the
cleaner's wake channel open and the cleaner the writer's queue, each waiting on the other
for good.

A failed write or flush fences the volume: the batch fails, no later request is accepted,
and the volume must be reopened and recovered. A failed flush is never retried, because
the kernel may already have marked the pages clean [RPA+20 §3]; recovery trusts only what
the device returns, verified by checksum.

Record sequences and segment incarnations are never reused over the volume's life. A
freed segment keeps its old records until new ones overwrite them, and a batch whose flush
failed may leave records the log never names, so numbers derived from the log could
repeat theirs; a repeated incarnation would let roll-forward (§6) take a stale record for
a new one. The superblock therefore records a reservation for each, raised with a
superblock write and flush before any record carries a number past it (2^24 sequences and
2^16 incarnations at a time), and recovery resumes above it (docs/bugs/2026-09-28).

A retry is told apart by its bytes. A write of a fragment already there with the same length
and CRC-32C, which different payloads can share, reads the stored copy and compares: the
same bytes answer it done, different ones refuse it, and a stored copy that no longer
verifies is written again from the retry, which carries the bytes acknowledged (audits B05,
S05). Only a retry pays the read; a write of the same fragment within one batch compares
with the bytes queued.

Streams: client writes and cleaner relocations append to different open segments, so
data is grouped by age [RO92 §3.6; HKA17 rule 4]. Appends to one chunk must be contiguous
(the offset equals the chunk's current length) or an exact repeat of a durable fragment
(a retry), and a sealed chunk takes no more appends.

## 5. The index and its checkpoints

The index maps a chunk key `(block id, epoch, chunk index)` to its fragments (one for a
whole chunk; a bounded list for an appended one) and is held in memory, as Haystack holds
its needle index [HAY §3.4]. Its size is bounded by the volume's chunk budget; a put past
the budget is refused with `Full`, never an allocation failure.

Recovery must not scan the device [HAY §3.5: the index file]. The loop writes the whole
index into the log as a checkpoint, then records it in the superblock (alternating A/B);
log space before the checkpoint is then free. It checkpoints when the log could not
otherwise hold more: before a batch, if the live log, the batch's frame, a wrap and a
checkpoint of the index as the batch may leave it would not fit in `L`.

Every frame, of a batch or a checkpoint, is at most `MAX_FRAME_BYTES` (4 MiB), the most
memory reading or writing one takes, and recovery reads no larger frame: it would take one
for the log's end and lose what it held and everything after (audit S13). So a batch's
frame is bounded before any I/O. A request's worst is a record placed with a seal of the
full segment before it and an open of the next, and one segment the batch frees; at most
one request in a batch is a relocation, since one cleaning pass runs at a time and waits
for each relocation it sends, and its moves are placed as puts are. Settings whose largest
batch frame passes the bound are refused at format and open. A checkpoint packs its
records, each segment's state and then each fragment, into frames in order, streaming the
fragments from the index; recovery replays any frame's records alike, so a checkpoint of
any size reads back whole. The writer refuses to write a frame past the bound, failing
its batch, so a fault in this accounting loses nothing. Segments recovery finds open
beyond the writer's two streams are sealed when the volume starts and recorded by a
checkpoint then.

`L` holds three checkpoints of the full budget `C_max`, one batch frame, and the wraps
before two checkpoints, the one being written and the one before it, whose skipped tail
stays live until the next (a wrap skips less than the largest frame). Settings are checked
against `L` again when a volume opens, since the log was sized for those given at format. So a checkpoint of
size `C` follows at least `3·C_max − 2·C` bytes of other frames: checkpoints take at most
half the log's writes, at the full budget, and far less below it, which is Raft's rule of
snapshotting at a log size well above the snapshot's [research/11 §9.2: RAFTX §7]; and
replay reads at most one log. The trigger as first built, a third of `L` counting the
checkpoint itself, rewrote a full 320 MB index after every ~2.8 MB of frames
[research/11 §9.1]. A bound on recovery time, checkpointing once replay would exceed a
stated budget as Oracle's Fast-Start does [research/11 §9.2: LAH01 §3.1], waits for an
availability target to set the budget from.

## 6. Recovery

1. Read both superblocks; take the valid one with the higher sequence. Sequences and
   incarnations resume above its reservations (§4).
2. Load the checkpoint it names and replay index frames in LSN order to the end of the
   log (§3.2 torn-versus-corrupt rule).
3. Check the last batch. Its flush may not have completed, and a record whose data does not
   verify may be torn; it may as well be an acknowledged record damaged since, or read wrong,
   and nothing after the batch tells the two apart, as a later frame would for an earlier
   one [AGL+18 §3.3.3]. So no record is dropped on that evidence. A relocation whose new
   copy does not verify goes back to the copy it moved, intact either way, since a segment
   is freed only after the relocation that empties it is durable. Any other record stays
   and is reported damaged: reads of it answer that its bytes do not verify, repair
   restores it from the block's other chunks, and a chunk no block names, never
   acknowledged, goes when the volume is reconciled with the Block layer. Misclassifying a
   crash as damage costs repair; the reverse loses data (audit S05).
4. Roll forward [RO92 §4.2]: from each open segment's last known write position, scan
   data records; each whose header, identity, incarnation and checksums verify is added
   to the index. This recovers records whose frame was lost with a torn index write.
5. Rebuild the segment usage table (live bytes and youngest record time per segment).

If the index log is unreadable, step 3 over every segment rebuilds the index from the
data records alone: slow, but no acknowledged chunk depends on the log.

Steps 2 and the search behind the torn-versus-corrupt rule read the log through a window
of one largest frame, 4 MiB: a frame the window holds is verified there, and the window
moves to the start of one it does not hold, which then lies wholly within it. The replay and
the search, which examines every block of the log for a later flush group's frame, read the
log in reads of 4 MiB, each byte at most twice, and verify in full only the frames whose
header names a later group. Before, each frame took a read of its first block and another
of the whole frame, and the search read the log one block at a time, 41,000 reads for a log
of 168 MB: an open took about a second on this machine's SSD, and now takes 50–80 ms (audit
P07; [measurements](../measurements/2026-09-29-recovery-reads.md)).

## 7. Reading

A read looks up the fragments covering the requested range and, for each, reads the
fragment's header with its checksum table and the checksum blocks the range touches, then
verifies them (§3.1). A checksum, identity or incarnation mismatch, or `EIO`, is one
condition: a typed `Corrupt` error naming the chunk, returned to the caller (which reads
another replica or fragment) and reported for repair [GAA17 §4; research/03 X1]. The node
never crashes and never returns unverified bytes.

The header and the blocks are one read when the payload between them is no larger than the
volume's read gap, and two reads otherwise. A read costs the device an access and then its
bytes at the transfer rate (Gray and Graefe 1997, §2), so passing over the gap costs what a
second access would. The volume reads at the depth where the device saturates, so both are
taken there: the access is the device time a small random read takes at saturation less its
transfer, and the gap is what the device transfers in that time
(`Calibration::read_gap`; research/11 §8.4). On this machine that is 51.2 kB, so a range
beyond its fragment's first checksum block is read apart; a device whose access is longer,
as a disk's seek is, reads through proportionally more. A device not measured has a gap of
zero and reads only what it verifies. Before this rule a read spanned from the header through its last
block, so 4 KiB at the end of an 8 MiB fragment read all 8 MiB before it (audit P01). Ranges
of 8 MiB chunks now read 16 to 27 times as fast with 16 in flight, and a gap taken at idle,
about 1 MB here, lost to this one at every point under load
([measurements](../measurements/2026-09-29-range-reads.md)).

A volume holds at most `Reads::depth` client reads at the device: the shallowest depth at
which calibration finds throughput stops growing, its interval overlapping the fastest
point's (Georges et al., OOPSLA 2007, §3.3). Past it each read added only waits, so holding
the device there costs no throughput, and it leaves the device's queue to the writer's
flushes rather than to however many reads arrive. The volume lets as many more wait, in the
order they came, and refuses the rest with `Busy`, after which the caller reads another
copy, which every chunk has. By Little's law a read let wait then waits about as long as a
read takes at the depth. A wait lasts while the reads ahead of it take, each bounded by the
operating system's I/O timeout, and a turn given back wakes only the read it passes to. A
device not measured reads one at a time. Before this gate nothing bounded the reads a volume
took at once: every caller's thread read at the device. The scrubber's paced steps (§9) and
the cleaner's relocations (§8) keep their own pacing outside the gate.

The depth of greatest power, Kleinrock's optimum (research/11 §13.3), is the wrong bound
here: on this machine it is 16, where 4 KiB reads still gain 30% by 64 in flight, and
holding the device at 16 with the rest waiting halved throughput at 32 callers and more than
doubled their median latency against the same reads through the file layer
([measurements](../measurements/2026-09-29-read-depth.md)). Power trades throughput for
latency, which pays only where reads refused go to another copy with room; reads made to
wait in software rather than at the device lose throughput and gain nothing.

## 8. Deleting and cleaning

A delete appends `Delete` to the index log in the next batch and removes the entry; the
record's bytes become dead in its segment's usage, and cleaning may reclaim them at once.
The net under deleting data by mistake is lazy deletion one layer up: the metadata service
deletes a block's chunks only after the block has gone unreferenced for a grace period, as
GFS keeps a deleted file for three days before reclaiming its chunks [GGL03 §4.4] and
Tectonic deletes lazily between its metadata layers [TEC §3.5]. Undoing a mistake there is
a metadata change. Holding deleted chunks here instead would either keep them in the index
for the cleaner to copy, or keep freed segments from being discarded, and dead data an SSD
must still treat as live raises its internal garbage collection [HKA17 Obs. #8, #21].

Cleaning starts when free segments fall below a runway: enough to take writes at the
fastest rate seen, `W`, while the cleaner reacts (a batch: the writer wakes it after each)
and cleans one victim (the longest one has taken), plus a batch arriving between wakes,
and the client's one-segment reserve [research/11 §10.3]. Before a victim has been timed,
cleaning one counts as two segments of writes, reading and rewriting a wholly live victim at
the rate writes arrive. LFS set its thresholds without study and found performance
insensitive to them [RO92 §3.4], and a fraction of the volume holds a 20 TB disk's 1.25 TB
idle for a runway of a few segments [research/11 §10.3]. A pass cleans until free segments
are one past the runway. It picks sealed segments by cost-benefit, `(1−u)·age / (1+u)` with
`u` the live fraction and `age` the youngest record's age [RO92 §3.6]; the scores move with
the clock, so each victim is chosen over the whole table, one pass against the victim's
relocation, which reads and writes what is live in it. It copies their live
records to the cleaner's stream (sorted by age), and frees the segment once the copies are
durable and indexed. Victims of live fraction `u` free `1 − u` segments each [RO92 §3.4],
so a net gain is due once they have held a segment's worth of dead space beyond what
packing their live data costs; a pass that gains nothing by then stops, and cleaning waits
until more data is deleted. A segment whose dead bytes do not pay for packing its live data
is no victim at all: cleaning it only moves its data, and a volume filled in chunks of
nearly a segment each, whose only dead bytes are padding, was cleaned victim after victim
for nothing. A pass that finds no victim is futile as one that gains nothing is (audit P06).
One pass runs at a time.

A client write with no free segment to go to is `Busy`, to be retried, while cleaning may
still free one, and `Full` once cleaning has been tried on the data deleted so far and
gained nothing: only trying tells how tightly relocated records pack. A relocation frees
no space (an equal copy replaces the old), so only deletes count as new dead data. A relocated record keeps its sequence; the index accepts it only if
the chunk still points at the old location, so a concurrent delete wins.

The cleaner and the scrubber find a segment's records through the index's record places:
the `(segment, offset)` of every live record, ordered, kept beside the key map. It is LFS's
segment summary [RO92 §3.3] held in memory, and it replaces a walk of the whole map, under
its lock, for every segment cleaned or scrubbed. It costs about 20 bytes a fragment,
measured with a counting allocator: 19.6 for 4M records appended in order, 15.9 once a
third are deleted at random, against roughly 150 for the map's entry. Each record
is read where it lies and verified from its own header, then counted only if the index
still names it; a record too damaged to name itself is attributed by a search of the map,
which only damage pays for.

## 9. Scrubbing

Every sealed segment is read and verified at least every 14 days, targeting 7, at the
lowest I/O priority [BGPS07 §6; SDG10 §5; AWK+19 §5.1]; seven days is practice rather than
a derived period [research/11 §12.2]. The order is staggered: SDG10 reads 128 MiB regions in
1 MiB steps, one step of every region before the next, which shortens the mean time to
detect an error by 10–20% at 7–14-day periods [research/11 §12.3: SDG10 §5.2.3]. The
scrubber does the same over the data area, the segments laid end to end: each step
verifies the live records that start in its 1 MiB (§8's record places), and each round
reads 1/128 of the volume spread over all of it. A read error at a live record is damage:
it is how a latent sector error appears. A
bad block triggers an immediate scan of its ±10 MiB neighbourhood and marks the device at
risk for 30 days [BGPS07 §5; SLM16 §5]. Damaged chunks are listed for repair up to 4,096;
more than 80% of disks with latent errors had fewer than 50 [BGPS07-F2], so a volume past
that is failing and is drained whole [research/11 §12.4].

## 10. Testing

The store is written against a `BlockFile` trait with two implementations: the real
`DeviceFile`, and a simulated file with power-loss semantics — writes not yet flushed are
lost, kept, or torn at sector granularity when the simulator crashes it, and reads, writes
and flushes can fail or return flipped bits on command [PCA+14; GAA17 §3; RPA+20
recommends block-level fault injection]. Every test that crashes checks the same
invariants: every acknowledged put reads back exactly; no read returns bytes that do not
verify; recovery never refuses a volume whose only damage is a torn tail, and reports damaged
only records of the last batch, each a write that was in flight; a flush failure fences the
volume. A read that fails while the volume recovers, or bytes damaged on the device, leave
an acknowledged chunk kept and reported, never dropped, and a retry of the same bytes writes
it again; a payload sharing a retry's length and CRC-32C is refused, not taken for it.
