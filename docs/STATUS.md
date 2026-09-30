# Status

Updated 2026-09-29. A component is complete when its tests pass on all six CI targets
(Linux, macOS and Windows on x86_64 and arm64) and the evidence listed for it has been
recorded.

## Implemented

`mantle disk probe` reports what the operating system knows about the device that holds a
directory and, with `--measure`, benchmarks the device. It is built on:

- **Device identification** on Linux (sysfs, including device-mapper and md members),
  macOS (the I/O Registry, including APFS containers and disk images) and Windows (the
  volume management functions and storage IOCTLs). Tested on physical disks, a mounted
  disk image and a virtual disk.
- **The file layer**: direct I/O where the file system supports it, aligned positional
  reads and writes, and each platform's full flush. Tested on APFS, ext4, tmpfs and NTFS.
- **Calibration**: reads, writes and flush latency measured through the file layer. Each
  point runs until the 95% confidence interval of its throughput is within 5% of its mean
  (three to six rounds), latency quantiles are reported only when enough transfers ran to
  estimate them, and the random reads go on to deeper queues until throughput stops growing:
  that depth is what a chunk volume holds at the device, and the depth of greatest power
  (Kleinrock) is reported beside it ([measurements](measurements/2026-09-29-read-depth.md)).
  The scratch file is removed whether the run succeeds or fails.
- **Checksums**: CRC-32C for stored and transmitted data, and CRC-64/NVME for S3
  checksums, both verified against published test vectors.
- **Cryptography** ([design](design/crypto.md)): AWS-LC through aws-lc-rs, vendored with the
  changes vendor/UPSTREAM.md lists. Processes seed from the operating system rather than CPU
  jitter entropy, 17.6 ms sooner, and a CPU random-number instruction that keeps failing gives
  way to the operating system instead of aborting the process, tested with generators that
  fail on demand. A TLS 1.3 record seals from several slices in one AES-GCM invocation, byte
  for byte as RFC 8448 traces it (aws/aws-lc-rs#1241). JWE's AES-CBC-HMAC and 192-bit key wrap
  match the vectors of RFCs 7518, 3394, 5649 and 7516 (aws/aws-lc-rs#617). Both crates' own
  suites run on every target, and every target lints from one machine with cross C toolchains.
- **A simulated device** for crash testing: writes not yet flushed are lost, kept or torn
  at sector granularity when it crashes, a failed flush leaves their durability unknown,
  and reads and writes can be made to fail or return corrupted bytes.
- **Erasure coding** (`mantle-ec`): systematic Reed–Solomon over GF(2^16) with contiguous
  data chunks, from `reed-solomon-simd` behind an unwind boundary. Every combination of lost
  chunks within the tolerance of RS(2,1) through RS(9,6) is rebuilt byte for byte in the
  tests, and `mantle bench ec` measures encoding and rebuilding per code
  ([measurements](measurements/2026-09-28-erasure-coding.md)). A block's scheme is chosen
  from the failure domains available and a durability target, S3's eleven nines by
  default ([design](design/durability.md)). The choice rests on a Markov model of permanent
  loss: chunks lost one at a time, whole domains at a time, and in events across domains,
  with repair one chunk at a time. The model is solved without subtraction and matches
  Ford et al.'s closed form to 10⁻¹² where elimination loses every digit. `mantle
  durability` runs it for a deployment's rates.

## In progress

**Chunk store** ([design](design/chunk-store.md)). Implemented: the volume layout with
two superblocks, self-describing data records with a CRC-32C per checksum block, the
index log with a second copy of every record's identity, the group-commit writer that
flushes once per batch and fences the volume on a failed write or flush, reads that
verify every byte they return, checkpoints and log wrap-around, reuse of segments that
empty out, cleaning of partly dead segments chosen by cost-benefit (relocations batched
into the cleaner's own stream, a reserve segment kept for it, and passes that stop when
they cannot gain space), and crash recovery (replay, verification of the last batch,
roll-forward, and a checkpoint that makes recovery's corrections durable). Its operating
parameters are calculated from models and measurements rather than chosen
(docs/research/11): checkpoints when the log needs the room, at most half its writes;
a write queue of two batches that refuses with `Busy` beyond them; cleaning on a runway
set by the measured write rate and cleaning time, with writes `Busy` while cleaning can
still make room and `Full` once it cannot; and an index of where each live record lies,
so cleaning and scrubbing never walk the whole index. Where calibration finds a durable
write into never-written space slower than one over written space, as ext4's extent
journaling makes it, format writes the volume once before use. Tested with
randomized workloads, cleaning included, cut by power loss at every point on the
simulated device (20,000 runs per soak: no acknowledged write lost, no unverified byte
returned) and with bit flips, read errors, damaged superblocks, damaged log frames and
failed writes and flushes. On real file systems (APFS, ext4 and tmpfs), a writing process
killed with `kill -9` at random points loses no write it acknowledged.

The scrubber verifies every live record in the background, in 1 MiB steps staggered over
the volume, paced so a pass takes the configured period (7 days by default) and
continuous once damage is found. Damaged chunks, including those whose records cannot be
read at all, are listed for repair, and a volume with more than 4,096 is marked failing, to
be drained whole.

Reads are held at the device's measured depth: a volume keeps at most the depth of greatest
power calibration finds at the device, lets as many more wait in the order they came, and
refuses the rest with `Busy`, where before every caller's thread read at the device
unbounded.

Remaining before it is done:

- `mantle bench chunk` measures puts and reads next to the same reads through the file
  layer, each point in ten to thirty rounds judged as the [measurement
  design](design/measurement.md) sets out: rounds ordered in time found by the lag-1
  autocorrelation and an exact runs test, two states found by Hartigan's dip test (its
  statistic matching R's `diptest` and a linear program from its definition on 5,000
  samples), and each state's median with its order-statistic interval. On this machine reads
  fall in two states, because the drive stalls every read for about a second in every nine
  under a stream of full flushes ([measurements](measurements/2026-09-29-chunk-store-states.md)).
  Remaining: where states change in time, and repetition across volumes and processes
  (design §7). Also remaining: large puts brought closer to the device's durable bandwidth.
  8 MiB puts reach about 69% of it now that a batch's frame is written beside its records
  ([measurements](measurements/2026-09-29-frame-overlap.md)); the flush is most of the
  rest. Writing the next batch during the last one's flush gains 10% at 32 MiB batches and
  nothing at 8 MiB here, where `F_FULLFSYNC` holds writes issued during it: not worth a
  pipelined writer's complexity until a device measures more.
- The group-commit wait for submitters slower than half a batch, from the measured
  distribution of their return times (docs/research/11 §2.6), once real clients supply it.
- Device health in how writes are placed and when a device is drained (docs/research/10).

**S3 protocol** (`mantle-s3`, [design](design/s3-protocol.md)). Signature Version 4 in the
`Authorization` header and in presigned URLs, and `aws-chunked` bodies with signed chunks and
signed or unsigned trailing checksums, verified against every worked example in AWS's S3
developer guide: the canonical requests, the signatures, and the chunked bodies byte for
byte. The ten checksum algorithms S3 accepts, full-object CRCs combined from parts without
the data, composite values and ETags, verified against AWS's multipart tutorial and ceph
s3-tests' vectors. Routing by virtual-hosted and path-style addressing, conditional requests
in RFC 9110's order, byte ranges, and paging of keys, versions and multipart uploads, each
tested against the s3-tests cases for it. A page passes at most 1,000 keys that list nothing
and resumes past them without dropping a common prefix, over the Name layer's scans. An XML reader for request bodies that refuses entity declarations and does
work linear in the body, checked against roxmltree on generated and mutated documents, with
the CompleteMultipartUpload, DeleteObjects, CreateBucket, PutBucketVersioning, Tagging,
AccessControlPolicy and OwnershipControls documents read against their schemas under size
limits computed from S3's own. Tags held to S3's limits and characters in documents and in
the `x-amz-tagging` header. ACLs disabled on every bucket, as S3's default Object Ownership
has them since 2023: canned, header and document ACLs read, and only the bucket owner's full
control accepted, as s3-tests expects. Lifecycle configuration: rules checked as S3 was
recorded checking them, in both of its forms, transitions refused once the rest is valid,
and when each expiration, noncurrent expiration, delete marker removal and upload abort falls
due, with the `x-amz-expiration` and abort headers, checked against the user guide's worked
examples and s3-tests, and every configuration read back as it was set, by property test.
CORS: rules checked as S3 was recorded checking them, preflights answered and refused as S3
answers them, and the headers an actual cross-origin request's response carries, checked
against s3-tests' origin tables and LocalStack's recordings of S3. Bucket policies: read by
a strict JSON reader checked against JSONTestSuite, checked as S3 was recorded checking them
against the Service Authorization Reference's actions and keys, and requests judged against
them as IAM documents, with its conditions, wildcards and variables, for accounts and the
anonymous requester; whether a policy is public, as S3 judges it, and Block Public Access, on
for every new bucket as in S3. Object Lock's documents and headers, checked as S3 was recorded
checking them, with the integrity, signature and bypass rules for writes that carry locks.
Browser uploads: `multipart/form-data` bodies decoded as they stream, strictly as RFC 7578 and
RFC 2046 define them, the fields before the file bounded as S3 bounds them, and the form's
policy, its signature and its conditions checked as S3 was recorded checking them, against
AWS's signed example and botocore's presigned POST byte for byte. A subresource routes to its
own operation or to 405, never to the bucket or object itself, nor across from one to the
other; `OPTIONS` is a preflight, and a `POST` to a bucket a browser upload. Server-side
encryption: its headers and a bucket's configuration checked as S3 was recorded checking them,
SSE-C blocked on new buckets as S3's are since April 2026, and the sealing every stored byte
takes ([design](design/encryption.md)): a random key per file wrapped with AES-256 key wrap,
checked against RFC 3394's vector, and AES-256-GCM segments at counted nonces, at about 8 GB/s
on one core. The documents every response carries, from listings and
multipart results to batch deletes, errors, bucket settings, tags, ACLs, lifecycle and CORS
rules, each holding what AWS's sample response holds for the same content when both are read
by roxmltree. Its SHA-1, SHA-256, SHA-512, MD5 and HMAC-SHA256 come from AWS-LC, every
call fallible and behind an unwind boundary. `mantle bench hash` measures each checksum
algorithm, signature verification, signed-chunk and form decoding, a form's policy check, and
sealing and opening at rest on one core.

Remaining before it is done:

- Encryption at rest: the root key's file and its generations, and the pass that rewraps
  files' keys under a new one (design/encryption.md). Each file's key is wrapped in its
  header row, and the gateway seals every object and part under it.

- The worker that takes lifecycle actions as they fall due, over the Name layer's versions
  and uploads, once the gateway and the metadata layer hold configurations (metadata.md §6).

**Metadata service** (`mantle-meta`, [design](design/metadata.md)). Row keys whose byte
order is the order the design needs, checked by property tests against the components they
encode; row values with a format byte and a CRC-32C checked on read, every flipped bit and
truncation refused; and the state machines of three layers over an engine interface, run
on an in-memory engine that loses what a crash would. The Name layer: versioning enabled,
suspended and never enabled, conditional writes judged at commit, delete markers, the null
version, multipart uploads whose parts are checked at completion, listings that stop
within a budget and resume, and Object Lock: each version's retention and legal hold, the
removals and changes they refuse at the step that would make them, and a bucket's default
retention, with versioning held enabled while Object Lock is on. Every file a removal stops
referencing, and every file a refused write carried, is released in the same transaction
into a queue the collector takes oldest first; a property test over random histories checks
that each file handed to a range is held in exactly one place after every step. A reclaimer
removes a released file's chunks, blocks and rows bottom up, resumable from the queue after
stopping at any step. The File layer: files written once as extents, found from any
offset with one seek. The Block layer: where each chunk lives, with a reverse row per chunk
that a property test keeps in step with the chunks through writes, moves and deletes. The
Bucket layer: owners' quotas and listings, and the steps of creating and deleting a bucket
across ranges, fenced by attempt, with the gates each Name range admits writes through, run
by a coordinator that names each range read and command and moves on with its answer. A
simulation of one bucket across a Bucket range and two Name ranges checks after every step
that no acknowledged write is lost to a delete, under 2,000 generated schedules of
concurrent creates and deletes, coordinators taken over, gateways that stop for good, and
writers with stale views; once each schedule's faults stop, the collector acts on its
schedule and every create and delete must end. The collector's schedule: a released file
comes due a grace after release, three days by default, and the collector waits on the
queue's own times; a create or delete is taken over once it has gone its patience without
progress, which its driver stamps on the bucket's row, read from an index of the attempts in
progress; and a deleted bucket's cleanup resumes from any step. Files a stopped gateway made
and never handed over: every file waits in its File range's queue of unsettled files until a
sweep settles it, a Name range takes a file only by the deadline the file was written with,
and the sweep asks the file's Name range, which releases a file it never took once the
deadline has passed at its own time, so no later handover can take it. Blocks a gateway made
for a file it never wrote are found one layer down the same way, from the file itself. A
gateway renews the blocks of a body still streaming in, and the sweep releases a block only
at the deadline the File range judged, a released block renewing no more, so a renewal and a
release are ordered by the Block range's log. A simulation of 2,000 schedules, with gateways
that renew, stop at any stage or come late, sweeps that stop between any two steps, and
leaders whose clocks run behind, finds no file both referenced and released, no named block
taken apart, and nothing left unsettled or unnamed; with either of the release's two checks
removed, it loses a named block's chunk.

Name ranges split and merge ([design §3](design/metadata.md#3-ranges)), as the TLA+ model
of splits and merges under a create and a delete lays out: each range records its span and a
generation, the child takes the keys past the cut with their rows and marks, the gates of the
buckets it can hold, the gate floor and the clock, and a command for a key outside the span,
or a coordinator's step routed by another generation, is answered with where the span went
and takes nothing. A merge freezes the higher range and lets the lower range decide once, in
its own log, moving its generation on either way, then ends or thaws the frozen range; its
driver (`mantle_meta::merge`) resumes from either range. The coordinator routes by
descriptors and starts its phase again when a range has moved on. Both simulations split and
merge ranges while buckets are created and deleted and files are handed over and swept, with
writers and sweeps that learn of both late and merge drivers that stop, resume, give up and
send late, and each rule removed on purpose fails one of them or a test of its own.

**Gateway object path** (`mantle-gateway`, [design](design/gateway.md)). A PUT or an upload
part as a state machine that names each request and does no I/O: the body sealed in 64 KiB
segments as it streams, cut into blocks of whole segments sized to the scheme's 8 MiB chunks,
each block coded as copies or Reed–Solomon and its chunks written at once to distinct volumes
with their CRC-32C, a refused chunk sent to the next volume offered, the block recorded once
every chunk is durable and renewed while the body streams on, the file written once every
block is, and the version or part committed last. It holds two blocks at most, the body
waiting while both are full. An empty object is its version alone; an empty part has a file.
Tested end to end against volumes and Block, File and Name ranges in memory: every object and
part reads back from its chunks under its key, coded blocks rebuild from any `data` chunks,
the body is held to its length and digests, and generated schedules of bodies, client pace
and refusing volumes commit an object whole or nothing, which fails with renewals removed.

Remaining before it is done:

- The GET path's lookahead and flight window, from measurements of the path. The path reads
  a range from the chunk bytes that hold it, reads another copy or decodes a block around a
  chunk that fails, reads objects of parts by their parts' plaintext, and holds two blocks.
- The server around it: HTTP, the transport to storage nodes and ranges, routing by
  descriptors, placement across failure domains, and each PUT's memory admitted against the
  gateway's.

The Raft log (`mantle-log`, [design](design/raft-log.md)), which every range replica on a
metadata device shares: group commit across ranges with one flush a batch, frames whose
sequences tell a torn tail from damage to acknowledged state, segments reclaimed oldest
first by sweeping their live records forward in one frame, and reads of entries no longer
in memory verified by each entry's own checksum. A property test runs generated histories
of appends, conflicts, compactions, snapshots, proposals and removals, cutting power at
random points on the simulated device, and checks after every reopen that each
acknowledged update survived and the one in flight landed whole or not at all; a soak of
200,000 histories over segment sizes and quotas passed. `mantle bench log` measures
appends against the device: one flush commits every replica's append, from 2 replicas to 256,
at about one durable write of latency ([measurements](measurements/2026-09-28-raft-log-benchmark.md)).

Range replicas (`mantle-range`, [design](design/replica.md)) run focal-raft's core over the
log and an engine: an entry carries a batch of gateway commands applied as one engine batch,
client sessions make each command take effect once however often it is retried, and members
that lag are caught up by snapshot, and reads are confirmed by ReadIndex. A member lost for
good is replaced by one under a new identity: added as a learner, caught up, swapped in by
one joint change, and done once every voter knows the new configuration committed. A
deterministic simulation of three members and three concurrent gateways, under crashes,
failed writes and flushes, partitions, dropped and reordered messages, compaction, and one
or two members lost for good in every run, checks after every run that each index was
applied the same everywhere, that every operation completes once faults stop, that every
put exists exactly once, that the members agree, that each key's history is linearizable,
and that every member's configuration names the live members. A soak of 20,000 seeds passed,
replacing 39,948 members lost for good, and the simulation catches stale reads and repeated
commands applied twice when either is introduced on purpose.

Remaining before it is done:

- Chunks a stopped gateway wrote for a block it never recorded, reconciled per volume once
  storage nodes run, and the collector's pacing against foreground latency (design §2, §6;
  docs/research/22 §10).
- The production engine, once its binding is chosen (design §4).
- The replica side of splits and merges: the child's group made on the parent's replicas
  from the parent's engine files as they stood at the split's entry, which carries the split
  alone; a merge's replica sets aligned first, its decision proposed once every replica of
  the frozen range has applied the freeze, and the ended range's group kept until every
  replica of the lower range has taken its rows; the directory the ranges publish their
  descriptors to; which ranges to split and merge, from measured size and load; and a read
  by key answered only by the range whose span holds it, when the gateway's read path is
  built.
- The fast track under simulation, and the transport: QUIC for bulk transfers and
  snapshots, and a UDP transport for consensus messages.
- Linearizability checked with real processes on the production engine.
- Done when a linearizability checker accepts histories recorded under network partitions,
  process crashes and disk faults, both in deterministic simulation and with real
  processes.

## Planned, in order

1. **S3 gateway.** Request signing (including presigned URLs and chunked uploads with
   trailing checksums), buckets, PUT, GET with byte ranges, HEAD, DELETE and batch
   delete, copy, ListObjects and ListObjectsV2, multipart uploads including resuming an
   interrupted upload, versioning, conditional requests, checksums, tags, and ACLs as S3's
   bucket owner enforced default answers them. Done when
   end-to-end suites using the AWS CLI, boto3 and the AWS SDK for Rust pass against a
   running mantle, and ceph's s3-tests pass for every supported feature.
2. **Multi-machine operation.** Placement across racks and zones, repair ordered by
   remaining redundancy, rebalancing, and retiring disks that start to fail. Done when
   tests that fail disks, machines and racks during writes show that every acknowledged
   write can still be read back.
3. **Cells.** A replicated map of which cell owns each key range, routing from cached
   copies of it with redirects after a range moves, moving a range between cells while it
   is read and written, and adding and retiring cells. Done when ranges move between cells
   under a mixed workload with no lost write and no stale read, and failing or upgrading
   one cell leaves the requests of every other cell unaffected.
