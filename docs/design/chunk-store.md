# Chunk store: how mantle writes bytes to a device

Status: design, 2026-09-28; device classes, power, budgets and carried checksums 2026-09-30.
Sources: docs/research/03 (cited by its keys, e.g. [RO92]), docs/research/01 (Tectonic,
Haystack, Ambry), docs/research/11 (models for the operating parameters, cited as
"research/11 §x"), docs/research/26–31 (concurrency, upload scheduling, storage classes and
power, device classes, resilient transfer, caching and integrity; cited as "research/29 §x"),
docs/measurements.

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

All offsets and lengths are multiples of the volume's block size `B`: the largest of 4 KiB
[HL23 §2.2], the device's logical and physical block sizes, and its write unit (`mantle-disk`
identity). The write unit is the smallest write the device takes without a read-modify-write:
on NVMe the namespace's preferred write granularity, NPWG, "the smallest recommended write
granularity", which the host "should minimally construct writes" to meet (research/29 §3.3).
Linux reports it as `queue/minimum_io_size`, and reports `physical_block_size` as the *smaller*
of NPWG and the atomic-write unit, because "Linux filesystems assume writing a single physical
block is an atomic operation"; so a QLC drive with a 16 KiB or 64 KiB indirection unit and 4 KiB
atomic writes reads 4096 there and its unit only in `minimum_io_size` (research/29 §3.3, finding
1). For random aligned writes of `s` below the unit `IU` the device programs whole units, a
write amplification of `IU/s` from that cause alone, and a log that appends small batches into
one unit rewrites it once per batch. Where the device does not report a unit (SATA, USB, SD,
eMMC, cloud volumes, NVMe without NPWG, and macOS, which exposes none found), calibration
measures it at format: durable aligned random writes of 4 to 64 KiB, the unit being the smallest
size above which throughput per byte stops rising (measurement.md §9, C3). `B` is fixed at
format and recorded in the device's profile (node.md §1.6). Before, `B` took the logical and
physical sizes alone, and on such a drive every batch padded to 4 KiB paid the device a
read-modify-write of its unit.

### 2.1 The device's plan by class

The plan a volume runs on its device follows from the device's identity, confirmed by
measurement (CLAUDE.md §5), never from a class assumed. Each row is research/29 §8.1–§8.7;
measurement.md §9 lists the probes.

| Input | NVMe flash | SATA flash | Disk (CMR) | USB, SD, eMMC | Cloud block volume |
|---|---|---|---|---|---|
| `B` | max(4 KiB, logical, physical, NPWG) | measured unit | physical sector | measured unit | measured unit |
| Write size the writer issues | multiples of NOWS where reported, else the calibrated knee | the knee | at least `s = 9·R·t_p` per positioning, `R` and `t_p` calibrated: 90% of a positioning's time transferring (research/29 §5.1) | at most `max_hw_sectors_kb` | 256 KiB on an SSD volume, 1 MiB on an HDD volume, the sizes EBS counts as one I/O (research/29 §6.4) |
| Depth | the measured knee, reads and writes apart | ≤ 32 (NCQ) | the knee, small | 1 under Bulk-Only Transport; the knee under UAS | provisioned IOPS / 1,000, AWS's stated depth, and the knee where it is lower |
| Flush | per batch; free where the drive reports no volatile cache (§4) | per batch, a whole-cache FLUSH CACHE EXT, Linux sending no FUA to SATA by default | per batch | per batch; recorded flush-unverified (§4) | per batch; recorded flush-unverified where host caching is ReadWrite |
| Placement handles | one per stream where the operator provisioned FDP (§4) | none | none | none | none |
| Discard | whole freed segments (§8) | the same | none | the same | none on instance store, which arrives trimmed |

A parity RAID or md device under a volume is planned as one device with the stack's
`minimum_io_size` (the stripe chunk) and `optimal_io_size` (the stripe width); mantle codes
across failure domains itself, so the array's write hole and small-write penalty buy redundancy
mantle does not use, and deployment guidance is to give mantle the member disks (research/29
§6.5). Drive-managed SMR takes the disk row with writes of at least 8 MiB, never interleaved,
and idle time for its own cleaning (research/03 W3); host-managed zoned devices stay refused
until a zone backend exists: the index log as a chain of zones, superblocks in zones of their
own, a segment per zone, open segments within the device's open-zone limit, and frames issued
after their records under Zone Append, which returns the address it wrote only on completion
(research/29 §4.1, §4.5). FDP gives most of the device-side benefit with none of these changes,
so it comes first. Instance-store volumes vanish on any stop
(research/29 §6.4), which placement treats as a failure domain's loss, not a planner input.

**Host over-provisioning is derived, not set.** A volume uses its whole device and discards
freed segments (§8). It leaves space unallocated only where the device's Endurance Group log
shows device write amplification rising with fullness on that device; FDP25 measured 1.3 at
half full and 3.5 full without segregation, and about 1.03 at any fullness with it (research/29
§3.4).

**The index log of a volume on a disk lives on flash where the node has flash.** Each group
commit writes its records into the open segment and its frame into the index log at a fixed
offset, two places on the platter: at 7,200 rpm rotational latency alone averages 4.17 ms, so
an idle-to-busy commit costs two positionings and a flush, and light-load commits on a disk run
at tens a second however small the requests (research/29 §8.4, finding 7). Where the node has a
flash device, the volume's index log is a file there (a metadata device's file system,
raft-log.md §1), named by the volume's superblocks with its own identity and sequence; the frame
keeps its separate I/O and so its power to reveal a lost data write [BGS+08-F7], and the commit
pays one positioning. A volume whose log device is lost rebuilds its index from the data records
alone (§6). A disk-only node keeps the log in the volume, at the low LBAs that sit on the fast
outer tracks (research/29 §5.1), and pays the second positioning, which calibration measures and
`mantle status` reports (measurement.md §9, C7). Folding the frame into the segment beside its
batch would save that positioning at the price of the separate copy, turning a lost write into
an undetected one; mantle keeps the copy (§3.2).

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

**The checksum table is carried, not recomputed.** A chunk write carries the CRC-32C of each
64 KiB block of its payload as the gateway computed them over the bytes it coded, and the volume
verifies each block against its value and stores those values in the record's table. It never
writes a table of its own recomputation over bytes in its memory, since a checksum made after a
corruption protects the corruption: ZFS found that "any corruption to blocks that are dirty ...
is written to disk permanently on a flush" for exactly that reason (research/31 §5.5, boundary
B5). The chunk's whole CRC-32C is the combination of the table's, which needs no second pass
over the bytes (research/31 §5.3). A read returns the stored values for the blocks it returns,
so the gateway verifies what left the node's memory (B8).

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
field rate of lost writes [BGS+08] against the measured cost per device class. On a disk the
cost is a positioning per batch, which §2 removes where the node has flash by moving the log
there; on flash it is the bandwidth above, which C2's split of a flush's cost by dirty bytes
(measurement.md §9) attributes per device.

Frames carry consecutive LSNs. Recovery tells a torn tail from corruption by what follows
an invalid frame [AGL+18 §3.3.3; GAA17 §4.2]: nothing with the next LSN means a crash
(the frame was never acknowledged, discard it); a valid frame of a later flush group means
corruption of acknowledged state, which is reported and recovered from the data records
(§6), never truncated. The frames of a checkpoint share one flush group, so none of them
proves another durable; the superblock, written only once the whole checkpoint is, records
the LSN after its last frame, and a replay that stops short of it met damage. Before, damage
to a checkpoint's first frame passed for a torn tail and the volume opened empty.

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
`fdatasync`, `F_FULLFSYNC`, `FlushFileBuffers`), and only then acknowledges the batch's
writes and publishes the new index entries to readers. Some of a batch's requests it
acknowledges only once a later frame is durable too: the next batch's, a checkpoint's, or an
empty frame written and flushed at once, when no request is queued or the next batch writes
no frame of its own. A write's record describes itself in its segment, and roll-forward
finds it when its frame is the torn tail recovery discards (§6), but only in a segment the
log already left open: a write in a segment its own batch opens is found only through that
frame's record of the opening. A delete lives only in its frame. So the frame of an
acknowledged delete, or of a write in a segment its batch opened, must never be the last,
where damage after the flush is indistinguishable from a torn write and discarding it lost
the write or brought the deleted chunk back (audit S15). Damage to a frame a later one
follows is corruption, reported and never truncated (§3.2). An answer decided without
writing rests on what it was decided on, and waits for it: a second delete of a chunk an
unconfirmed delete removed, a retry of a write not yet confirmed, a refusal of bytes that
differ from them. So while any request is held, every answer decided without writing is held
behind it, and one decided on a request earlier in its own batch stands only if that request
is written, `Busy` otherwise. A batch that writes no frame confirms at once, so no answer
waits longer than a batch while requests that write nothing keep coming. Deletes come from the reclaimer, a layer
below any client's request, so the second flush they wait for costs no client latency. Until the flush, a batch's
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

**How a batch reaches the device.** A batch's writes are issued in pieces of the plan's write
size (§2.1) and never larger than the device dispatcher's unit `u`, the smallest transfer at
which the device reaches its bandwidth (node.md §2.7): at 200 MB/s one 8 MiB write is 42 ms in
which a small read behind it waits (research/27 §6.4). Cutting a record into several device
writes weakens nothing: durability is decided at the flush, and recovery already takes a batch's
writes as having landed in any order (§6). Research note 27 left this open (§11 item 5); the
flush's semantics close it.

**The flush, by what the device declares and what it measures.** A drive that reports no
volatile write cache (NVMe's VWC clear; Linux's `queue/write_cache` "write through" with `fua`
off) is one where a Flush "shall have no effect" (research/29 §3.8), as drives with power-loss
capacitors report. Calibration measures the flush twice, after dirty writes and on an empty
cache (measurement.md §9, C2): where both are the cost of the command alone, the batch's
service time is its write time, and the writer issues the next batch's writes as soon as the
last batch's writes have completed, without waiting for its flush call to return (research/11
§5), answering each batch in order once its own flush is done. A later batch's frame is thus
still written only after the earlier batch's writes completed, which on such a device is when
they are durable; if the drive's claim is false, recovery finds an earlier batch damaged beneath
a later frame and reports it, the direction that costs repair and never loses an acknowledged
write (§6). Where the flush costs the dirty data's
programming, as on consumer NVMe, every SATA device and every disk, the flush is the cost the
group commit amortizes and batches go one at a time. A declaration is the drive's claim:
thirteen of fifteen SSDs, "enterprise-class" ones among them, lost data their kernel had flushed
when Zheng et al. cut their power (research/29 §3.8, finding 4). So a device whose flush mantle
cannot verify, every USB-attached device (whose bridge may never see a flush, research/29
§6.2), a cloud volume under ReadWrite host caching, and a drive whose SMART log reports its
volatile-memory backup failed, is recorded flush-unverified, and placement counts a chunk there
as a weaker copy (durability.md §5).

**The wait, by class and by power.** The writer's wait for returning submitters is the derived
wait (`mantle_disk::commit`; research/11 §2) with three changes. A batch holding an
EXPRESS_ONEZONE write does not wait: a lone request is flushed at once (research/11 §2.4;
storage-classes.md §5). On battery or under the OS's saver mode, the writer waits until every
outstanding submitter has returned, the batch limits are reached, or the measured batch service
time `S` has passed, whichever comes first: each request that joins a batch saves a flush's
energy, and within `S` the wait costs no throughput, so the longest wait the bound allows is the
energy-minimizing one, and a lone request's latency at most doubles, `S` to `2S`
(research/28 §4.5; research/11 §2.3). Under thermal state Serious or worse every class, Express
included, takes that rule (node.md §1.8). Acknowledgement never changes: a write is answered when
it is durable, on battery as on mains.

**Placement handles.** On a drive the operator provisioned with Flexible Data Placement, each
stream writes under its own placement handle: client writes, each of the cleaner's three age
streams, the index log, the superblocks, and the long-lived stream of storage classes with a
minimum duration (§8) while handles last, so data of one death time fills the device's reclaim
units together (research/29 §4.2, §4.5). With segregation FDP25 held device write amplification
at about 1.03 at any fullness, against 1.3 to 3.5 without (research/29 §3.3). A reclaim unit
(gigabytes) is far larger than a segment, so the claim that the cleaner frees a stream's
segments nearly in write order, which keeps the device's amplification near one under initially
isolated handles, is checked on each device from its Endurance Group log (measurement.md §9,
C10), never assumed. Enabling FDP deletes every namespace of the endurance group (research/29
§4.2), so it is an operator's step at drive intake; mantle never does it. Linux carries the
handle as io_uring's per-write `write_stream`; macOS and Windows expose no interface the research
found, and there the streams write as today.

**One issuer per physical device.** The writer, the cleaner and the scrubber of every volume on
a device, and the device's Raft log writer where the device holds metadata, are state machines
run by that device's issuer thread, which submits their I/O through the platform's asynchronous
interface or the device's bounded pool of blocking workers (node.md §1.2). Each was a thread of
its own, three per volume, 300 at 100 volumes (audit §14.1), and `write_together` started a
scoped thread for each region of a batch after the first: a thread start on the write path,
whose count grew with load (research/26 §4.7, recommendation 7). The issuer owns its volumes from
the moment it starts them, so a start the operating system refuses part way stops what it
started, in reverse, before the error returns, and nothing is left holding the device (audit
S12).

*As built (2026-10-01; STATUS item 4).* Every write and flush of every volume on a device goes
through the device's issuer (`mantle_disk::issuer`): one thread and a pool of blocking workers,
all started when the device opens, `min(the device's reported queue, the depth calibration
measured throughput to stop growing at, the process's thread budget)` of them (node.md §1.2).
A volume attaches a second handle to its file and hands the issuer a batch's regions and frame
together; the issuer issues them as deep as its workers go, and once all have completed, and
only if all succeeded, the batch's one flush, then answers. A failed region fails its batch
with no flush issued, and the writer fences the volume. Index frames written alone, checkpoints,
superblocks and the pre-write at format go the same way. No thread starts for a batch or a
region: at four workers, batches of 32 regions ran with the process's thread count unchanged
(`tests/issuer.rs`). The writer, cleaner and scrubber are still threads of each volume, two or
three per volume, not yet state machines on the issuer's thread; the cleaner's relocations
reach the device through the writer's batches and the scrubber writes nothing. A refused start
is still unwound by the volume itself (`Volume::start`).

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
index into the log as a checkpoint, then records it in the superblock (alternating A/B),
with the LSNs of its first frame and of the frame after its last; log space before the
checkpoint is then free. It checkpoints when the log could not
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
   incarnations resume above its reservations (§4). When the other copy does not read, the
   one taken may be a write behind the damaged one and its reservations one reservation
   short, so they are raised past the most one batch can issue and one reservation more:
   fewer sequences than a frame holds bytes, and no more incarnations than there are
   segments. The next superblock written rewrites the damaged copy.
2. Load the checkpoint it names and replay index frames in LSN order to the end of the
   log (§3.2 torn-versus-corrupt rule). A replay that ends inside the checkpoint met damage
   and the volume refuses to open.
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

The gate is the device dispatcher's (node.md §2.7): the reads that wait are taken in start-tag
order across tenants and principals rather than in arrival order, and each is charged its cost
in the device's measured units, so one principal's flood of reads waits behind its own share.
On a disk, the reads the dispatcher releases together are issued sorted by address, ascending,
so the drive's queue can reorder them (C-LOOK "achieves the highest performance among
seek-reducing algorithms", research/29 §5.2); one at a time forfeits it. A latency sample taken
on a device records the idle time before its request, so the exit from a low-power state (up to
6 ms on a consumer NVMe drive after 100 ms idle under Linux's defaults, research/29 §3.9) is a
known second state of the device and not a fault or noise (research/29 §9 item 14; research/10
R5).

**A chunk cache in front of the device, where it pays.** The volume reads with direct I/O and
the page cache is bypassed (§1). A node may hold verified chunk bytes in memory, and on flash in
front of disks, keyed by chunk key, which is never reused and names bytes that never change, so
the cache needs no invalidation (research/31 §3.1). It is filled only with bytes that verified
and served only after they verify again against their stored checksums (research/31 §5.8). Its
size is the node's division of memory by measured miss-ratio curves (node.md §2.5), which may
give it nothing, the right answer on a laptop where the gateway's caches filter the stream
first. A flash tier's admissions are paced so its writes stay within the flash device's rated
endurance (research/31 §3.3).

The depth of greatest power, Kleinrock's optimum (research/11 §13.3), is the wrong bound
here: on this machine it is 16, where 4 KiB reads still gain 30% by 64 in flight, and
holding the device at 16 with the rest waiting halved throughput at 32 callers and more than
doubled their median latency against the same reads through the file layer
([measurements](../measurements/2026-09-29-read-depth.md)). Power trades throughput for
latency, which pays only where reads refused go to another copy with room; reads made to
wait in software rather than at the device lose throughput and gain nothing.

A read is let through the gate for everything it holds at once, counted together against
`Reads::bytes`: the buffers the device reads into, the output it fills, and the fragment it
reads (audit S07). The output used to go uncounted, and a read of a chunk appended in many
fragments gathered all of them first, so the gate bounded the device's bytes in flight but
not the memory reads held. A buffered read (`Volume::read`) holds its whole range, so it
takes at most the larger of what one record holds, the most a put writes, and the gate's
bytes; a larger range is `TooLarge`, and is read with `Volume::stream`, which reads it a
piece at a time into the caller's buffer, each piece verified like any read and let through
the gate on its own, from one fragment looked up as it is reached. A chunk of any size is
then read holding one piece's buffers and output: a test streams a 2.4 MB chunk of 24
fragments through a 64 KiB gate from four readers at once and never holds more. A stream
reads the chunk it began with: the chunk's first fragment's length and checksum, which
relocation keeps, are checked at each piece, and a chunk deleted and written again under its
key reads as not found rather than as a mix of the two. Range reads, cold and warm, alone and
beside writes, are measured in
[measurements](../measurements/2026-09-30-chunk-range-reads.md).

## 8. Deleting and cleaning

A delete appends `Delete` to the index log in the next batch and removes the entry; the
record's bytes become dead in its segment's usage, and cleaning may reclaim them at once. It
is acknowledged once a later frame is durable (§4).
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
and the reserve kept from client writes, a segment for each of the cleaner's three streams
[research/11 §10.3]. Before a victim has been timed,
cleaning one counts as two segments of writes, reading and rewriting a wholly live victim at
the rate writes arrive. LFS set its thresholds without study and found performance
insensitive to them [RO92 §3.4], and a fraction of the volume holds a 20 TB disk's 1.25 TB
idle for a runway of a few segments [research/11 §10.3]. A pass cleans until free segments
are one past the runway. It picks sealed segments by cost-benefit, `(1−u)·age / u` with
`u` the live fraction and `age` the time since the segment was opened. That is LFS's ratio
[RO92 §3.6] with RAMCloud's two corrections [RKO14 §9]: the cleaner reads and rewrites only
the live data, so a victim costs `u` each way, not LFS's `1 + u` for reading it whole; and the
age is the segment's, reset when its data is cleaned into another, not its youngest
record's. A relocated record keeps the time its chunk was written, so by the youngest
record's age the segments the cleaner writes look as old as their data, and the policy
cleans them again at high utilization; RAMCloud found the same, and 70% more write
throughput at 90% utilization from the change. Each segment's opening time is in its
segment record, so recovery restores it.

The scores move with the clock, and a pass over the table for each victim held the table's
lock against the writer's publication for the pass (audit P06). The table keeps the
candidates in a kinetic tournament instead [BGH99 §4]: a score is a line in time,
`(a/b)·(t − o)` with `a = S − L`, `b = L`, so each match of a tournament tree over the
segments holds its winner at the tree's time with the time its loser's line passes the
winner's, found exactly, and the earliest such time below it. Moving the clock replays only
the matches whose certificates have failed, a segment's change replays its path to the
root, and a victim skipped in the pass is passed over best-first, a subtree at a time.
Scores compare exactly, in 256-bit products with ties to the lower segment, so the
tournament names the segment a scan of the table names, which a property test checks over
random tables, changes and times, forward and back (a clock stepped back replays every
match). At 62,403 segments a choice took 262 µs at the median by the pass and 19.5 µs by
the tournament with eight segments changed since the last
([measurements](../measurements/2026-09-30-chunk-victims.md)). The victim is cleaned only in
the incarnation it was chosen in.

The cleaner moves a victim's live records into one of three streams of its own by the age of
their data, the time since the chunk was written: below 4ℓ, below 16ℓ, and older, where ℓ is
the mean time client segments lived before they were cleaned, taken over each 16 cleaned.
These are SepBIT's classes of rewritten data [WLL+22 §3.4], deployed at Alibaba Cloud;
data written once and deleted later, as chunks are, has only its age to predict when it will
die, and grouping data of like age makes segments that die together. The client's writes
keep their own stream, as LFS kept cleaned data apart [RO92 §3.6]. Until ℓ is known, all go
to the youngest's stream. Each segment's stream is in its segment record, so recovery
continues each stream's newest open segment. A model of the volume's cleaning measured the
choices against each other, uniform, hot-and-cold and heavy-tailed lifetimes at 75–90% live
([measurements](../measurements/2026-09-30-chunk-cleaning.md)): the old policy, youngest
record's age and one cleaner stream, wrote 2.38–6.17 times what clients wrote under
uniform deletes, 1.65–3.04 under hot-and-cold and 1.79–2.52 under the heavy tail; this one
writes 6–10%, 7–19% and 9–12% less. Greedy, the emptiest segment first, which is optimal
when deletes are uniform [Des14], is 1–3% better there and 20–95% worse under skew. The
copies are freed once they are durable and indexed. Victims of live fraction `u` free `1 − u` segments each [RO92 §3.4],
so a net gain is due once they have held a segment's worth of dead space beyond what
packing their live data costs; a pass that gains nothing by then stops, and cleaning waits
until more data is deleted. A pass's gain is the victims it frees less the segments its
relocations open, counted by the writer; the change in free segments over the pass counts
client writes as well, and a pass that freed a segment while a client opened one read as
futile, which answers writes `Full` while space could be reclaimed. The cleaner has the
writer free its victims with a request of its own; it used to delete a key it took no
client to use, and deleted a client's chunk stored under it. A segment whose dead bytes do not pay for packing its live data
is no victim at all: cleaning it only moves its data, and a volume filled in chunks of
nearly a segment each, whose only dead bytes are padding, was cleaned victim after victim
for nothing. A pass that finds no victim is futile as one that gains nothing is (audit P06).
One pass runs at a time.

**A storage class is a second predictor of death.** A write whose object declares a class with
a minimum duration (STANDARD_IA, ONEZONE_IA and GLACIER_IR for 30 or 90 days; archived classes
moving to a cold pool) is written straight into the oldest cleaner stream rather than the
client's: "data written once and deleted later ... has only its age to predict when it will
die", and the class is the client's own statement that it will live months (research/28 §3.4,
D6). The cleaner then need not copy it there. The cleaning model of
measurements/2026-09-30-chunk-cleaning.md, run on a class-labelled trace, measures the write
amplification saved.

**Freed segments are discarded.** When the cleaner frees a segment, the volume discards its
range, aligned to the device's `discard_granularity` and split at `discard_max_bytes`, since "some
devices exhibit large latencies when large discards are issued" (research/29 §3.5); dead data the
drive must keep as live raises its garbage collection [HKA17 Obs. #8, #21]. A discarded range
reads back as zeros, ones or its old bytes as the drive reports (DLFEAT), which recovery already
treats as no record, since roll-forward takes nothing without its checksum and a sequence above
the superblock's reservation (§6). On a file volume a discard is a hole, and a hole is
never-written space: where calibration found the first-write penalty (§2), discarding brings the
penalty back on the segment's next pass, so there the volume weighs the device's measured
garbage-collection cost against its own measured penalty and discards only where the first is
larger (research/29 §3.5, D12). A device that arrives trimmed, as instance store does, is not
discarded whole at format.

**Reads apart from write bursts.** On flash, random reads beside sequential writes cost the
writes a factor of 4.5 in Chen et al.'s measurement, where either alone was "largely independent
of access patterns" (research/29 §3.7). The cleaner's victim reads and the scrubber's steps are
dispatched in the gaps between the writer's bursts on that device, not interleaved with them;
client reads, which cannot wait, are what hedged reads across replicas are for (gateway.md §3).

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

References for this section: [RKO14] S. M. Rumble, A. Kejriwal, J. Ousterhout, "Log-structured
Memory for DRAM-based Storage", FAST 2014. [WLL+22] Q. Wang, J. Li, P. P. C. Lee, T. Ouyang,
C. Shi, L. Huang, "Separating Data via Block Invalidation Time Inference for Write
Amplification Reduction in Log-Structured Storage", FAST 2022. [Des14] P. Desnoyers,
"Analytic Models of SSD Write Performance", ACM TOS 10(2), 2014. [BGH99] J. Basch,
L. J. Guibas, J. Hershberger, "Data Structures for Mobile Data", J. Algorithms 31(1), 1999.

## 9. Scrubbing

### 9.1 The period

**Decision: the scrub period is derived per device from three bounds, and is the longest period
at which the blocks the device holds still meet their durability target.** It was "at least
every 14 days, targeting 7", which is practice, not a derivation [BGPS07 §6; SDG10 §5; AWK+19
§5.1; research/11 §12.2], and on a large disk that practice breaks the drive's rating: a disk's
rated workload counts reads, "(Lifetime Writes + Lifetime Reads) * (8760 / Lifetime Power On
Hours)", and current 24–28 TB disks are rated for 550 TB a year, so scrubbing a full 24 TB disk
every 7 days reads 1,251 TB a year, 2.3 times the rating, and every 14 days 626 TB; an 8 TB
desktop disk rated 55 TB a year is overrun 7.6 times by a weekly scrub (research/28 §5.2, D11;
research/29 §5.5, finding 6). Vendors derate the drive's failure rate above the rating; Pelican
stopped scrubbing for this reason (research/28 §5.2).

For a device holding `V` bytes of live records across its volumes, with rated annual workload
`L`, measured annual client traffic `W_c` and cleaner traffic `W_g` (reads and rewrites), and the
background share of its bandwidth `B_scrub` (node.md §2.6), the period `T` satisfies:

- **The workload bound, on disks:** `T ≥ V / (L − W_c − W_g)`, in years. For a full 24 TB disk
  with no other traffic that is 15.9 days; with 300 TB a year of client and cleaner traffic, 35
  days (research/29 §5.5). It counts every volume and log on the physical device, since the
  rating is the device's.
- **The bandwidth bound:** `T ≥ V / B_scrub`, research/11 §12.4's, so scrubbing stays within the
  share that keeps foreground latency within its bound.
- **The durability bound:** `T` is at most the period at which the scheme of every block the
  device holds still meets the target, with detection in the repair rate: a latent error found
  on average half a period after it occurs makes the repair rate at most `1/(T/2 + T_rebuild)`
  (durability.md §2; research/28 §3.3, D4).

The scrubber runs at the longest period the durability bound allows and no shorter than the
other two: a shorter period than durability needs spends the device's rated workload, its energy
and its read bandwidth on margin nobody asked for. Where the lower bounds exceed the durability
bound, the conflict is resolved by parity, never by exceeding the rating: the device's pool is
reported as needing a wider code, and blocks placed there take one (durability.md §4). The
rating comes from the device where it reports one, from the operator otherwise, and in the
absence of both from the most conservative datasheet value of the device's class, 55 TB a year
for a disk (research/28 §5.3), as durability.md §5 takes failure rates from the conservative end
of their sources. Flash carries no rated read workload, so on flash the workload bound is absent.
`mantle status` prints each device's period and the bound that set it.

### 9.2 How the scrubber reads

The scrubber reads at the lowest I/O priority, and beneath mantle's own pacing, which works
whether or not the operating system honours the priority (research/28 §4.5). The order is
staggered: SDG10 reads 128 MiB regions in
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

How the steps are timed depends on the device and its power:

- **On a disk**, a step's reads are issued sorted by address (§7), and steps are batched into
  bursts spaced either below the drive's head-unload timer or far above it, never just over it.
  Disks rate 600,000 load/unload cycles, 329 a day over a five-year warranty; background I/O
  that arrives just after each unload spends a cycle each time, up to 1,440 a day at a
  one-minute spacing past a short timer (research/29 §5.6, D8). The timers are read from the
  drive's power-condition pages where mantle has the privilege, and otherwise from the first-I/O
  latency after idle gaps (measurement.md §9, C5), which shows an unload as the time to reload
  the heads.
- **On flash**, steps run in the gaps between the writer's bursts (§8).
- **On battery or under the OS's saver mode**, the period's work is scheduled into mains time
  first, and runs on battery only when the period would otherwise be missed; when it runs, it
  runs in dense bursts at the device's measured knee and then leaves the device idle long
  enough for its low-power state to pay, rather than trickling at the constant rate `V/T`
  research/11 §12.6 paces at (research/28 §4.5; research/29 §7.5, D15). Under thermal state Fair
  or worse it is deferred as background work is (node.md §1.8).

### 9.3 Device budgets

Every device carries a budget for each rating it has, and every class of work on it spends from
it (research/28 §5.3, D12):

| Budget | Disk | Flash |
|---|---|---|
| Bytes | read plus written, against the rated annual workload | media bytes written, against the drive's Endurance Estimate, over the planned service life |
| Cycles | load/unload; start/stop where spun down (storage-classes.md §3) | — |
| Hours | rated power-on hours a year (2,400 on a desktop disk, 24×7 on a data-center one) | — |

Each rating comes from the device where it reports one (NVMe's Endurance Estimate and Percentage
Used), from the operator otherwise, and else from the most conservative cited datasheet value
for the class (55 TB a year and 50,000 cycles for disks). Spending is counted from mantle's own
I/O, always available, and checked against the drive's counters where readable (measurement.md
§9, C10–C11). Device write amplification multiplies into a flash budget: device bytes are client
bytes times the cleaner's amplification, measured exactly, times the drive's own, Δ media units
over Δ data units where reported (research/28 §5.1). Repair at a margin of one chunk or less is
never refused for budget; the scrub period takes what is left (§9.1); placement prefers, among
feasible devices, the one whose projected exhaustion is latest, keeping a stripe's members from
converging on one date (research/10 R9; node.md §7); and a device whose budget will run out
before its planned retirement is drained early enough to finish under the background budget,
PACEMAKER's rule (research/10 R6). Running past a rating is evidence for a higher failure rate,
which enters the device's failure probability as a covariate (research/10 R3), not a hard stop.

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

The simulated file also misdirects and loses writes, a write landing at another record's offset
or reported done and not done, as Krioukov et al. enumerate them (research/31 §8, T4), and
discards ranges returning zeros, ones or stale bytes; the invariants are the same, with no
acknowledged chunk unreadable while its scheme's tolerance holds. A flip injected into the
volume's receive buffer between a chunk's arrival and its write is caught by the carried table
before the write (§3.1). Tests on the device plan check that `B` takes `minimum_io_size` where
Linux reports a unit above the physical block, that batches pipeline only where the flush
measured free, and that the derived scrub period never passes a device's workload bound.
