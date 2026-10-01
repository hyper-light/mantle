# Status

Updated 2026-09-30. A component is complete when its tests pass on all six CI targets
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
  The scratch file is removed whether the run succeeds or fails. Depth is kept by one pool of
  blocking workers started once a calibration and reused across its points, growing only as
  deep as its deepest step, no deeper than the device's reported queue (`nr_requests`,
  `IOCommandPoolSize`, or SATA NCQ's 32 where the OS cannot say) and the process thread
  budget; each point reports the depth it achieved beside the depth asked.
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
roll-forward, and a checkpoint that makes recovery's corrections durable). A delete, and a
put in a segment its batch opened, are answered once a later frame confirms their batch's,
since recovery finds neither without its frame, and an answer decided without writing waits
for whatever unconfirmed request it rests on. Damage to any frame of the checkpoint the
superblock names is refused, and a superblock read without its twin resumes past what the
twin may have reserved. Its operating
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

Reads are held at the device's measured depth: a volume keeps at most the shallowest depth at
which calibration finds throughput stops growing (the depth of greatest power, which this
section once named, is reported beside it and is not the bound; design §7), lets as many more
wait in the order they came, and refuses the rest with `Busy`, where before every caller's
thread read at the device unbounded.

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
block is, and the version or part committed last. It holds the block filling and as many full
blocks going down at once as its caller admits, the body waiting while that many go. A GET
seeks by the byte it wants, reads only the chunk bytes that hold its range, reads another copy
or decodes a block around a chunk that fails, reads an object of parts by its parts'
plaintext, and holds as many blocks as its caller admits. Completing an upload derives the
object's size, ETag and checksum, full-object or composite, from its parts' rows, and the Name
range checks them against its own. An empty object is its version alone; an empty part has a
file. Tested end to end against volumes and Block, File and Name ranges in memory: every object
and part reads back through the GET, in any range and with chunks lost, coded blocks rebuild
from any `data` chunks, the body is held to its length and digests, and generated schedules of
bodies, client pace, windows and refusing volumes commit an object whole or nothing, which
fails with renewals removed. `mantle bench gateway` measures both paths on one core and counts
the round trips each waits through.

Remaining before it is done:

- The windows a gateway admits, from its memory and measured latency; the rates of the paths
  measured on an idle machine.
- The server around it: mantle's protocol over QUIC for its client and its nodes, the HTTP/1.1
  listener for stock S3 clients, the transport to storage nodes and ranges, routing by
  descriptors, placement across failure domains, and each request's memory admitted against
  the gateway's.

The Raft log (`mantle-log`, [design](design/raft-log.md)), which every range replica on a
metadata device shares: group commit across ranges with one flush a batch, frames whose
sequences tell a torn tail from damage to acknowledged state, updates answered only once a
later durable record confirms their frame's flush, segments reclaimed oldest first by
sweeping their live records forward in one frame, and reads of entries no longer in memory
verified by each entry's own checksum. A confirmation rewrites its frame's own record, a
commit that fails answers every update it holds, an open restoring a lost frame keeps that
frame's record until the restore is durable, and a live segment that yields no frame is
reported as damage. A property test runs generated histories of appends,
conflicts, compactions, snapshots, proposals and removals, cutting power at random points on
the simulated device, and checks after every reopen that each acknowledged update survived
and the one in flight landed whole or not at all; a soak of 200,000 histories over segment
sizes and quotas passed. A second property test damages the last frame after power loss and
checks every acknowledged group is kept or reported: 20,000 cases a run. `mantle bench log` measures appends against the device: one flush
commits every replica's append, from 2 replicas to 256; an append waits for two flushes, its
frame's and its confirmation's, where none follows at once
([measurements](measurements/2026-09-29-log-confirmation.md)).

Range replicas (`mantle-range`, [design](design/replica.md)) run focal-raft's core over the
log and an engine: an entry carries a batch of gateway commands applied as one engine batch,
client sessions make each command take effect once however often it is retried, a write
delivered again in a new session is recognised by its file, or for a write with no file by
the ID the gateway drew for its request, and answered as its first delivery was, and members
that lag are caught up by snapshot, and reads are confirmed by ReadIndex. A member lost for
good is replaced by one under a new identity: added as a learner, caught up, swapped in by
one joint change, and done once every voter knows the new configuration committed. While a
`Ready` flushes, a replica holds the messages and ticks it is given, within a window of
appends per member and one election timeout, and takes them in order after; refusing them
left a leader under steady load committing nothing. A
deterministic simulation of three members and three concurrent gateways, under crashes,
failed writes and flushes, partitions, dropped and reordered messages, compaction, and one
or two members lost for good in every run, checks after every run that each index was
applied the same everywhere, that every operation completes once faults stop, that every
put exists exactly once, that the members agree, that each key's history is linearizable,
and that every member's configuration names the live members. A soak of 20,000 seeds passed, holding 4.4 million messages and 3.3 million ticks
through flushes and replacing 39,898 members lost for good, and the simulation catches stale reads and repeated
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

The work the designs of 2026-09-30 create (research notes 26–31, folded into node.md,
gateway.md, metadata.md, chunk-store.md, measurement.md, durability.md, raft-log.md,
replica.md, engine.md, encryption.md, crypto.md, s3-protocol.md, architecture.md and the new
storage-classes.md), in the audit's stage order (audit §17). Each item is done when its test
passes on all six CI targets.

### First: stop the demonstrated safety failures

1. **No broadcast wakes.** The log's `room`, `workers.rs`'s latch and every `Barrier` or
   `notify_all` over a pool or population replaced by per-waiter slots woken one to one in
   arrival order, with a bounded waiting list, and a fence completing each slot once (node.md
   §1.3; raft-log.md §3). Done when an instrumented test with `W` waiters and `K` completions
   counts at most `K` plus the waiters admitted, a fence wakes each exactly once, and `bench log`
   at thousands of logical replicas runs on macOS without a kernel spinlock timeout.
   *Done on macOS (2026-09-30)*: `mantle_log`'s `Room` hands freed room to waiters in arrival
   order, each woken by its own `unpark`, its list bounded at `max_groups` times a group's two;
   `room::tests::each_answer_wakes_only_the_waiter_it_admits_and_a_fence_wakes_each_once`
   counts 16 wakes for 16 answers among 48 waiters and 48 in all after the fence;
   `workers.rs` is gone. A submission can carry a `Waker`, woken exactly once
   (`a_waker_is_woken_once_for_each_answer`). `bench log` ran 16,384 logical replicas on 20
   threads on the development Mac. No production `notify_all` or `Barrier` remains; the tests'
   simulated files keep theirs, waited on by a few test threads.
2. **Depth without a thread per transfer.** Calibration's depth kept by io_uring on Linux, an
   overlapped completion port on Windows, and a reusable pool of exactly `depth` workers with
   per-worker slots on macOS and where io_uring is unusable, inside a process thread budget read
   from `kern.wq_max_threads` on macOS, refused before any thread starts; achieved depth sampled
   and reported (measurement.md §8). Done when a measurement at a device's full reported depth
   never exceeds the pool bound in the OS's own thread count, a request past the budget starts
   no thread, and the achieved depth reaches the depth asked or reports its shortfall.
   *The portable pool is done on macOS (2026-09-30)*: `mantle_disk::threads` reads the budget's
   ceiling from `kern.wq_max_threads` on macOS, the smaller of `kernel.threads-max` and the soft
   `RLIMIT_NPROC` on Linux, and Microsoft's stated 500 pool threads on Windows, and counts the
   process's threads from the OS (`proc_pidinfo`, `/proc/self/status`, a ToolHelp snapshot);
   `measure::Pool` draws from it before any thread starts and refuses with
   `DiskError::Threads`. `tests/depth.rs` measured at the NVMe controller's 253: 2 threads
   before, a peak of 256 (the pool and the test's sampler), a mean of 228.7 in flight and
   253 at most; a depth past the budget started none. *Remaining*: io_uring on Linux and the
   completion port on Windows, which the pool stands in for on every OS until they land; the
   Linux and Windows thread counters have been type-checked for their targets but not yet run
   there; macOS reads its queue from `IOCommandPoolSize`, Windows reports none yet.
3. **Logical clients as records.** `bench log`, `bench` and `bench gateway` multiplex clients on
   at most the granted cores' driver threads through `submit` and a `Waker`; the replica ladder
   ends at the stated replica bound; generators report their CPU and lateness (measurement.md
   §10). Done when the same benchmark at `R` and `10R` clients shows the same peak thread count.
   *Done on macOS (2026-09-30)*: replicas and clients are records on at most
   `available_parallelism` drivers, answered through `Log::submit_waking` and the chunk store's
   new `Volume::put_waking`/`delete_waking`; `bench chunk --rate` and `bench gateway --clients
   --rate` generate open loop from a Poisson process with latency from intended start. Rows
   report the OS's thread count, the drivers' CPU and the generator's lateness.
   `tests/clients.rs` ran `mantle bench log` at 18, 180 and 1,800 replicas on 20 threads each;
   `bench::tests::clients_cost_no_threads` ran puts at 18 and 180 clients closed and 180 open
   on 22 threads each. No ladder doubles until a spawn fails. *Remaining*: chunk reads still
   block, so a read is a thread, as many as the store's measured read bound, until the
   device's dispatcher (item 5) takes them; open loop needs a stated `--rate`; the replica
   bound is the list the run states until node.md §11 derives the replicas a node hosts; the
   resident memory per client (measurement.md §10) is not yet reported.

### Second: make overload and retry behaviour dependable

4. **One issuer per physical device**, running every volume's writer, cleaner and scrubber and the
   device's log writer as state machines, `write_together` without threads per region, startup
   rollback per issuer (node.md §1.2; chunk-store.md §4). Done when 100 volumes on one device run
   on one issuer and its pool, the process thread count matches `3C + D + Σ p_i`, and the
   chunk store's crash soak passes unchanged.
   *Writes through the issuer done on macOS (2026-10-01)*: `mantle_disk::issuer` is one thread
   and a pool of `min(device queue, measured depth, thread budget left)` blocking workers per
   device, all started when the device opens (`issuer::depth`; one worker on a device
   calibration has not measured). Every write and flush of every volume goes through it: a
   batch's regions and frame together, its one flush only once all have completed and only if
   all succeeded; index frames written alone, checkpoints, superblocks and the format's
   pre-write likewise. `write_together` and its thread per region are gone. Files are
   duplicated into an arena the issuer's thread owns and its scoped workers borrow; no `Arc`.
   `tests/issuer.rs` ran batches of up to 21 writes on a depth-4 issuer at 9 threads idle and 9
   while writing, at most 4 in flight; a failed region fails its batch with no flush;
   `bench::tests::clients_cost_no_threads` ran 18 and 180 clients closed and 180 open at 27
   threads each. Batch latency before and after: measurements/2026-10-01-device-issuer.md.
   *Remaining*: each volume's writer, cleaner and scrubber are still threads of their own, not
   state machines on the issuer's thread, so the count is `2V` or `3V` plus `D + Σ p_i`, not yet
   `3C + D + Σ p_i`; the Raft log's writer does not go through the issuer; startup rollback is
   still each volume's own; io_uring on Linux and the completion port on Windows are not built,
   the pool runs there (node.md §1.2); the 100-volume test is not written; reads go through the
   issuer with item 5.
5. **The device dispatcher**: start-tag order across tenants and principals, in-flight budget
   `D_b`, `D_n` and dispatch unit `u` from calibration, background shares, the log, engines and
   chunks of a laptop's one device under one authority (node.md §2.7; chunk-store.md §4, §7).
   Done when a small read's p99 beside a saturating large write stays within `(D_b + n·u)/C` and
   throughput stays within measurement of the calibrated plateau.
6. **Charges and fair queues**: dominant-share charging from calibrated per-device cost models,
   hierarchical SFQ (tenant, principal, request) with state bounded by admitted work, contract
   classes with mClock reservations and an Aequitas admit probability, a Space-Saving heavy-hitter
   table, and per-range tenant SFQ with the range's admission level on answers (node.md §2.3,
   §2.7). Done when the owner's scenario (node.md §9.1) holds at laptop scale: the large upload's
   goodput during the spike within measurement of its weighted share, the small PUTs within the
   §2.7 bound of their latency alone, recovery within one chain latency, and every table within
   its bound under a new principal per request.
7. **Overload as sojourn, shed by hashed principal**: the shared authority for both listeners,
   CoDel's sojourn test, `503 SlowDown` before `100 Continue` on HTTP, retries shed first by the
   attempt header, a deterministic subset of principals shed at the measured excess, the retry
   ratio held below `C/λ − 1` (node.md §4.5). Done when steps to 1.5× and 10× capacity, open loop
   and corrected for coordinated omission, leave overload once the trigger stops, with retries
   shed before first attempts and completed multi-request steps per second reported.
8. **Fair-share windows**: a PUT's window from the entitled rate, a GET's from the latency at the
   hedge quantile (node.md §2.6; gateway.md §2–§3). Done when a lone upload's goodput reaches
   `min(client rate, device plateau)` and its window shrinks and regrows across a spike.
9. **Operation identity**: client-drawn identities, `amz-sdk-invocation-id` on the listener,
   operation rows in the Name range with the first answer, the horizon capped by the operation
   table's budget, removal by the collector, carried by splits (gateway.md §2.1; metadata.md §2).
   Done when the orphan-sweep simulation extended with every attempt of every identity at any
   step, across gateways, splits and lagging clocks, holds the five obligations, each broken
   variant fails it, and a gateway killed between commit and answer in a versioned bucket leaves
   one version after a retry through another gateway.
10. **The device plan by class**: `B` from `minimum_io_size` or C3, batches padded to it, write
    size from NOWS, depth caps for SATA, USB Bulk-Only and EBS, flush measured twice and batches
    overlapped only where it is free, flush-unverified devices recorded (chunk-store.md §2.1, §4;
    raft-log.md §2; measurement.md §9). Done when tests on reported and simulated geometries
    take the right `B` and plan, and C1–C3 run on each OS's devices.
11. **The derived scrub period** from the workload, bandwidth and durability bounds, per device,
    with `mantle status` naming the binding bound (chunk-store.md §9.1; durability.md §5). Done
    when a simulated 24 TB disk at 550 TB/yr scrubs no more often than its workload bound and the
    durability model counts latent errors detected at the chosen period.

### Third: establish a real laptop serving baseline

12. **The native protocol and the client library and CLI**: S3's operations natively, credits,
    typed refusals and the admission level, the server's upload plan, per-block GET streams with
    CRC-64/NVME, migration on interface change, TLS session resumption without early data, the
    TLS-over-TCP route, and the journal with its flush rules (node.md §3.9, §4.1; gateway.md §3,
    §6). Done when the client killed and its storage cut at every journal write and protocol step
    ends every operation answered, resumed or provably restarted, and a Wi-Fi to cellular switch
    mid-upload continues or resumes with no committed run sent again.
13. **Resumable native uploads** in server-cut runs with progress frames, the checkpoint interval
    of gateway.md §2.1, MD5 and block-hash state carried across runs, and digest-state export
    and import in the vendored aws-lc-rs (gateway.md §2.1; metadata.md §2; crypto.md §10). Done
    when the six obligations hold under interruption at every frame, a changed source ends in
    `BadDigest`, and the ETag equals S3's for objects at and above 5 GiB.
14. **The HTTP/1.1 listener's resilience**: exact `ListParts`, `AbortIncompleteMultipartUpload`,
    open-upload bytes reported, completion answered directly unless near clients' read
    timeouts, pinned resumed downloads (gateway.md §2.1). Done when the AWS CLI's and boto3's
    multipart uploads with the connection cut at every part and at completion finish or resume,
    and s3-tests' multipart suite passes.
15. **The gateway's caches**: sealed-block, File and Block row caches; Name rows and absences
    validated by ReadIndex taken after arrival; coalesced fetches and next-round validations;
    policy by scaled-down simulation and size by SHARDS curves within the node's division
    (gateway.md §5; node.md §2.5; metadata.md §2). Done when the linearizability checker accepts
    every history with every cache on and catches the four broken variants (node.md §9.1).
16. **Integrity across the path**: per-segment plaintext CRCs and seals opened on another core,
    the parity's random linear check, carried CRC tables to and from storage nodes, command CRCs
    checked at apply, the engine's per-key-value protection, CRCs kept with cached rows, and
    attribution per node and core (node.md §5.6; chunk-store.md §3.1; replica.md §2; engine.md
    §3). Done when the flips and the faulty core of node.md §9.1 are caught at their boundaries
    before acknowledgement or service, and the core is attributed; and CRC-64/NVME's Hamming
    distances at 64 KiB and 8 MiB are computed by a program that reproduces Koopman's CRC-32C and
    "Jones" figures (node.md §5.6).
17. **Storage classes on one device**: the class stored, validated and answered on every
    operation, archived reads refused, RestoreObject, lifecycle transitions, Intelligent-Tiering
    with day-resolution tracking, Reduced Redundancy, directory buckets with `CreateSession`, the
    class's writer rule and stream, and the achieved bound reported (storage-classes.md §2, §5,
    §8; s3-protocol.md §4, §7). Done when S3's recorded answers replay, s3-tests'
    `storage_class`, `lifecycle_transition` and `restore` markers pass, and the controlled-clock
    restore and tiering tests pass.
18. **Power, thermal state and background work**: the OS's inputs by notification, the battery
    wait rule in the chunk store's and log's writers, background deferral by deadline, Apple's
    thermal responses, background I/O priority, sleep reconciliation (node.md §1.8). Done when
    toggled inputs on each OS defer exactly the work the table names, no acknowledgement precedes
    durability across a switch, and energy per acknowledged byte on battery falls under the rule
    by measurement.md §9's differential method with the `2S` latency bound holding.
19. **Device probes and energy**: C1–C14 where each applies, idle-gap tagging of latency samples,
    and calibration refused on battery unless forced (measurement.md §9; chunk-store.md §7).
    Done when each probe's result is recorded for the development machines' devices with its
    device cost.

The S3 gateway profile listed before stays this stage's frame: request signing, buckets, PUT,
GET with ranges, HEAD, DELETE and batch delete, copy, listings, multipart uploads with resume,
versioning, conditional requests, checksums, tags and ACLs; done when end-to-end suites with the
AWS CLI, boto3 and the AWS SDK for Rust, and the native client library, pass against a running
mantle, and ceph's s3-tests pass for every supported feature.

### Fourth: prove heterogeneous regional operation

20. **The QUIC layer on a vendored quinn-proto**: the pacing quantum and operating packet size,
    adaptive reordering, the PMTU raise rule, Copa with focal's half stride, idle timeout from
    the probe timeout, keep-alive from the measured NAT lifetime, stream windows under the
    2,048-frame ceiling, the connection credit reserve, key update before AES-GCM's limit (node.md
    §3.3, §3.8–§3.9). Done when control-message p99 beside saturating bulk at 8, 16, 64 and 256
    kbit/s beats stock quinn with goodput reported, a fuzzed alternate-frame loss at the window
    ceiling never closes a connection, and the congestion qualification over the audit §13.6
    matrix is recorded with its selection rule and rows.
21. **Brownouts and reconnects**: the four clocks, equal-jitter backoff from the probe timeout,
    replacement by certificate, election timing from durable-ack tails, the bulk queue bound
    (node.md §3.8). Done when a 64 kbit/s upload with 5–120 s outages completes, returning its
    memory within one run commit of each stall, and elections per hour during a rate collapse
    with saturating bulk stay at the level of the run without bulk.
22. **Cell-wide shares**: node credits on responses with dmClock counters, tenant contracts
    divided among gateways by measured demand, hedged reads at the derived quantile and
    reservation-hedged writes (node.md §3.3, §4.5, §5.4). Done when a tenant whose load lands on
    few nodes keeps its cell-wide share, and its rate across gateways stays within its contract as
    its load moves.
23. **Placement by credits and budgets**: two random choices on credits, devices chosen by latest
    projected budget exhaustion, device budgets for workload, cycles, hours and flash wear, early
    drains, flush-unverified copies weighed (node.md §7; chunk-store.md §9.3; durability.md §5).
    Done when per-device load spread and small-request p99 beat random and least-loaded placement
    in the same run, and endurance accounting tracks the drives' own counters.
24. **Media pools, moves and restores**: pools from measurement, class eligibility, copysets per
    pool, cold pools with spin-down groups and start/stop budgets, staging writes for archived
    classes, copy-switch-release moves spread under the background cap, bounded restore queues
    per tier (storage-classes.md §3–§7). Done when crash tests at every step of a move leave each
    object at one class with every block referenced once, a simulated day of billions of due
    moves stays under the cap with no deadline missed, and restore completion times meet their
    tiers' documented times under load.
25. **Device-specific placement**: FDP handles per stream on provisioned drives with DLWA checked
    from the Endurance Group log; disk volumes' index logs on flash; disk reads sorted; scrub and
    cleaner reads in the writer's gaps on flash; head-load-aware background bursts; discard by
    measured cost (chunk-store.md §2, §4, §7–§9). Done when DLWA, commit latency (C7) and
    load/unload counts are recorded against their baselines on real drives.
26. **Fleet-scale integrity**: replica digests compared at a leader-named index, core quarantine
    and node fencing by comparison with the cell's background rate, correctable-memory counters
    as health, repair that verifies every source and its rebuilt chunk (replica.md §2; node.md
    §5.6). Done when an injected divergent member is found and rebuilt, an injected faulty core
    is quarantined, a week without faults measures the false-positive rate that sets the
    thresholds, and repair with one corrupt source and one lost chunk rebuilds from a verified
    subset or refuses, never writing a chunk that fails the block's CRC.

Multi-machine operation keeps its frame: placement across racks and zones, repair ordered by
remaining redundancy, rebalancing, and retiring disks that start to fail; done when tests that
fail disks, machines and racks during writes show every acknowledged write can still be read
back.

### Fifth: make fleet growth and geography safe

27. **Regional and fleet shares**: contracts divided among cells and regions by demand on longer
    intervals, principal limits across cells by count-min sketches merged by addition
    (architecture §8). Done when simulation shows the regional share error and the sketches'
    overestimate within their stated bounds against exact counts.
28. **Zonal storage classes**: one-zone and multi-zone classes physically distinct with zone
    exposure reported, directory buckets placed near compute, lifecycle due dates spread across
    cells' budgets, restore rates per principal (storage-classes.md §9). Done when a zone's loss
    in simulation loses no multi-zone object and reports one-zone exposure as computed.

Cells keep their frame: a replicated map of which cell owns each key range, routing from cached
copies with redirects after a range moves, moving a range between cells while it is read and
written, and adding and retiring cells; done when ranges move between cells under a mixed
workload with no lost write and no stale read, and failing or upgrading one cell leaves every
other cell's requests unaffected.
