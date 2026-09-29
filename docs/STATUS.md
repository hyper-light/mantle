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
  estimate them, and the read depth reported is the one of greatest power (Kleinrock). The
  scratch file is removed whether the run succeeds or fails.
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

Remaining before it is done:

- An I/O path that keeps the measured number of reads in flight.
- `mantle bench chunk` measures puts and reads next to the same reads through the file
  layer ([measurements](measurements/2026-09-28-chunk-store-benchmark.md)). Remaining: each
  point repeated until its result is statistically stable, and large puts brought closer
  to the device's durable bandwidth.
- The group-commit wait for submitters slower than half a batch, from the measured
  distribution of their return times (docs/research/11 §2.6), once real clients supply it.
- Device health in how writes are placed and when a device is drained (docs/research/10).

**Erasure coding** (`mantle-ec`). Systematic Reed–Solomon over GF(2^16) with contiguous
data chunks, from `reed-solomon-simd` behind an unwind boundary. Every combination of lost
chunks within the tolerance of RS(2,1) through RS(9,6) is rebuilt byte for byte in the
tests, and `mantle bench ec` measures encoding and rebuilding per code
([measurements](measurements/2026-09-28-erasure-coding.md)).

Remaining before it is done:

- Choosing the code for a block from the failure domains available and a durability
  target, computed from failure and repair rates under correlated failures
  (docs/research/04 §A5) rather than from a table.

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
control accepted, as s3-tests expects. A subresource routes to its own operation or to 405,
never to the bucket or object itself. The documents every response carries, from listings
and multipart results to batch deletes, errors, bucket settings, tags and ACLs, each holding
what AWS's sample response holds for the same content when both are read by roxmltree. Its SHA-1, SHA-256, SHA-512, MD5 and HMAC-SHA256 come from AWS-LC, every
call fallible and behind an unwind boundary. `mantle bench hash` measures each checksum
algorithm, signature verification and signed-chunk decoding on one core.

Remaining before it is done:

- The rest of 05 §16.1's later surface: bucket policies, the access S3 grants beyond the
  owner once ACLs are disabled; CORS; lifecycle; server-side encryption headers; Object
  Lock; and browser POST uploads.

**Metadata service** (`mantle-meta`, [design](design/metadata.md)). Row keys whose byte
order is the order the design needs, checked by property tests against the components they
encode; row values with a format byte and a CRC-32C checked on read, every flipped bit and
truncation refused; and the state machines of three layers over an engine interface, run
on an in-memory engine that loses what a crash would. The Name layer: versioning enabled,
suspended and never enabled, conditional writes judged at commit, delete markers, the null
version, multipart uploads whose parts are checked at completion, and listings that stop
within a budget and resume. The File layer: files written once as extents, found from any
offset with one seek. The Block layer: where each chunk lives, with a reverse row per chunk
that a property test keeps in step with the chunks through writes, moves and deletes. The
Bucket layer: owners' quotas and listings, and the steps of creating and deleting a bucket
across ranges, fenced by attempt, with the gates each Name range admits writes through. A
simulation of one bucket across a Bucket range and two Name ranges checks after every step
that no acknowledged write is lost to a delete, under 2,000 generated schedules of
concurrent creates and deletes, coordinators taken over, and writers with stale views.

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

- The coordinator that runs a bucket's create and delete across the ranges, and the
  collector that finishes what a failed one leaves and removes unreferenced files and
  blocks after a grace period.
- The production engine, once its binding is chosen (design §4).
- Splits and the fast track under simulation, and the transport:
  QUIC for bulk transfers and snapshots, and a UDP transport for consensus messages.
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
