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
regions]. `S` defaults to 256 MiB — the zone size of host-managed SMR drives and in the
range of ZNS zone capacities, so segments map 1:1 onto zones [BAH+21 §2.3; AD15] — and is
a volume parameter fixed at format. `L` is fixed at format so that three index
checkpoints of the volume's chunk budget fit (§5).

A file-backed volume grows by whole segments up to its quota. Where calibration measures
a first-write flush penalty (ext4: 5.7x; docs/measurements/2026-09-28), a new region is
written once with zeros before first use, so steady-state appends overwrite written
blocks [RO92; AWK+19-F6 reuses WAL files for the same reason]; where it measures none
(APFS), it is not.

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
append and delete requests to a bounded queue (a full queue refuses with `Busy`). The
loop takes every request that arrived while the previous batch was being made durable,
lays the data records into the open segment for their stream, appends one index frame,
issues the writes, then **one** flush of the volume (`sync_data`: `fdatasync`,
`F_FULLFSYNC`, `FlushFileBuffers`), and only then acknowledges every request in the batch
and publishes the new index entries to readers. There is no artificial delay [research/03
G6]: under light load a batch is one request, under heavy load it is everything queued.
On the development machine a durable flush costs ~4.7 ms whatever its size
(docs/measurements), so this loop is what turns 245 flushes/s into tens of thousands of
chunk writes/s.

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

`L` holds three checkpoints of the full budget `C_max`, one batch frame, and the wraps
before two checkpoints, the one being written and the one before it, whose skipped tail
stays live until the next (a wrap skips less than the largest frame). So a checkpoint of
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
3. Roll forward [RO92 §4.2]: from each open segment's last known write position, scan
   data records; each whose header, identity, incarnation and checksums verify is added
   to the index. This recovers records whose frame was lost with a torn index write.
4. Rebuild the segment usage table (live bytes and youngest record time per segment).

If the index log is unreadable, step 3 over every segment rebuilds the index from the
data records alone: slow, but no acknowledged chunk depends on the log.

## 7. Reading

A read looks up the fragments covering the requested range, reads the block-aligned span
holding each fragment's header and the needed payload, and verifies it (§3.1). A
checksum, identity or incarnation mismatch, or `EIO`, is one condition: a typed
`Corrupt` error naming the chunk, returned to the caller (which reads another replica or
fragment) and reported for repair [GAA17 §4; research/03 X1]. The node never crashes and
never returns unverified bytes.

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

Cleaning starts when free segments fall below a low watermark and stops at a high one
[RO92 §3.6]. It picks sealed segments by cost-benefit, `(1−u)·age / (1+u)` with `u` the
live fraction and `age` the youngest record's age [RO92 §3.6], copies their live records
to the cleaner's stream (sorted by age), and frees the segment once the copies are
durable and indexed. A relocated record keeps its sequence; the index accepts it only if
the chunk still points at the old location, so a concurrent delete wins.

## 9. Scrubbing

Every sealed segment is read and verified at least every 14 days, targeting 7, at the
lowest I/O priority [BGPS07 §6; SDG10 §5; AWK+19 §5.1]; seven days is practice rather than
a derived period [research/11 §12.2]. The order is staggered: SDG10 reads 128 MiB regions in
1 MiB steps, one step of every region before the next, which shortens the mean time to
detect an error by 10–20% at 7–14-day periods [research/11 §12.3: SDG10 §5.2.3]. Here the
step is a segment, so a region is 128 segments and each round reads 1/128 of the volume
spread over all of it; steps within a segment await a per-segment map of its fragments. A
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
verify; recovery never refuses a volume whose only damage is a torn tail; a
flush failure fences the volume.
