# Status

Updated 2026-10-01. A component is complete when its tests pass on all six CI targets
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
write past never-written space slower than one over written space, as ext4's extent
journaling makes it, and on NTFS, whose zeros up to a write past the valid data length are
documented, format writes the volume once before use
(measurements/2026-10-24-first-writes-past-unwritten-space.md). Tested with
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

A node's storage role (`mantle_node::storage`, [design](design/node.md) §5.2) opens the volumes
its configuration names before its runtime starts, one issuer per device the OS names and each
device directory locked against a second process, and answers chunk puts and gets from nodes and
gateways as exchanges of their own class over QUIC: a put's bytes checked against the CRC-32C its
sender computed before the volume writes them, answered once durable, and refused unread past
what the volumes' writer queues hold; a get read at the device through its issuer from the
node's shard (`mantle_chunk::Reader`) and verified before it is sent; each refusal typed. Tested
in one process (`crates/node/tests/cluster.rs`) and with `mantle node` processes
(`crates/mantle/tests/node.rs`): a gateway puts chunks round three nodes and one is killed with
`SIGKILL` mid-put. Every chunk acknowledged reads back whole after the node starts again on its
volume. A lost put either landed whole or not at all, and is stored when retried. This holds with
a busy loop on every core.

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

The cell's root range keeps the volume table ([node design §6.6](design/node.md)): each node's
failure domain and each volume's node and bytes, enlisted by one command that records what the
table does not hold, refuses a node or volume held otherwise as a conflict that changes nothing,
and refuses past the cell's stated bounds; read a page at a time, as many volumes as a reply
carries. A cell of three `mantle node` processes hosts it beside a range of each layer. A node
asks commands of ranges itself, as a gateway does, within a session of its own, in-process where
it leads and over QUIC where another node does, each applied once through a leader's stop
([node design §6.6](design/node.md)); the leader stamps every entry it proposes with its wall
clock. Each node with volumes enlists itself and them as it starts, a node in another domain
than the table holds refused; placement offers a block's volumes by a capacity-weighted highest
random weight, one to a failure domain first ([research 63](research/63-placement-order.md)).
Remaining: the table published to nodes that host no root range (architecture §4's constant-work
loop), two random choices on credits once nodes advertise them, and copysets once a cell has the
domains for them (node design §7).

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

A node hosts the drivers ([node design §5.1](design/node.md)): a client asks it over mantle's
protocol on QUIC for S3's bucket and object operations (CreateBucket, HeadBucket, ListBuckets,
DeleteBucket, Put and GetBucketVersioning; PutObject with its bytes on the exchange, GetObject
with them on the reply, HeadObject, DeleteObject, ListObjectsV2) and its multipart uploads
(CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts,
ListMultipartUploads), and the node runs the bucket's coordinator, the `Put`, `Get` and
`Completion` drivers or the ranges' commands and reads each needs, placing
from its replica of the volume table, writing chunks to its own volume in-process and to its
peers' over the transfer class, and asking the ranges' leaders through its own sessions. A put's
`If-Match` and `If-None-Match` are judged as its version commits, and its checksum and
`Content-MD5` before anything commits; a get's or head's preconditions in RFC 9110's order and its
range against the version it reads, by the S3 layer's own judges; a page of ListObjectsV2 is one
read of the Name range, ended early at its reply's room and resumed exactly. A cell of three
`mantle node` processes takes objects from empty to three blocks through any node, each block's
chunks on distinct nodes, and reads every one back whole and in part with each node in turn
killed by `SIGKILL`, and through that node once started again; it passes with a busy loop on every
core. Through each node of a cell of three `mantle serve` processes every operation answers as
S3 does for each case research/05 records, and 1,050 keys list in pages of 1,000 and of 97,
through the three nodes in turn, every key once and in order; a multipart upload whose parts go
through different nodes, one node killed and started again between them, completes through
another into an object whose bytes, multipart ETag and full-object checksum are S3's, the same
when completed again, each block's chunks on distinct nodes (`crates/mantle/tests/serve.rs`).
The operation identity every mutation carries is not yet recorded by the Name range (item 9).
Stock S3 clients reach the same operations through each node's HTTP/1.1 listener, mantle's own
codec on hyper-rt's TCP, plain on loopback and TLS 1.3 and 1.2 with the endpoint's P-256 chain
elsewhere, each request routed, its SigV4 signature checked against the cell's access keys, and
its operation handed to the node's object host in-process through a bounded door, with a put's
body read only as its driver wants it, `aws-chunked` decoded with each chunk's signature and its
trailing checksum checked before commit, and a burst past the host's operations waiting at the
door, its bodies unread, rather than refused (node design §4.1–§4.5; research/72). Through each listener of a cell of three: buckets,
puts read back whole and by range and head through the others, ListObjectsV2, conditional writes,
deletes and a multipart upload spread over the listeners, each read byte-identical; objects put
through one listener alone held by the other two nodes exactly as received over QUIC, and on every
node the chunk bytes its volumes hold exactly those its peers sent over QUIC and those it wrote
itself; as many puts at once as the listener takes connections, past the host's two operations,
each committed; a request whose framing cannot be trusted answered 400 and closed, a put refused before
`100 Continue` sending no body, a connection past the budget closed as it comes, and a bad
signature `403 SignatureDoesNotMatch` (`crates/mantle/tests/s3.rs`; TLS 1.3 and 1.2 handshakes in
`crates/node/tests/listener.rs`). The AWS CLI v2 and boto3 pass against a cell of three on every CI
target: the CLI's bucket, object, listing and multipart commands, its multipart `cp` from standard
input and to standard output, and boto3's object calls and managed transfers with the SDK's default
checksums, every object read back byte-identical (`crates/mantle/tests/stock_clients.rs`; CI
installs boto3 pinned with every hash). The CLI's CRT transfer client, `aws s3`'s default from CLI
2.38.0, runs where the image's CLI takes an http endpoint for it, 2.38.0 onward: on the Windows
images now (2.38.3 and 2.38.4 on 2026-10-26), on Linux (2.37.13) and macOS (2.36.40) as their
images move past it. `mantle bench s3` measures the listener's own cost
(measurements/2026-10-25-s3-listener.md). A cell out of space says so on both doors: an object whose
block every volume refused as full is answered `507 Insufficient Storage`, natively
`InsufficientStorage`, which stock clients do not retry, where it was `503 SlowDown`, and nothing of
it commits; against a cell of three whose stated volumes fill through both doors, the next object is
refused so on each and every object acknowledged before reads back whole (node design §4.4,
research/75; `crates/mantle/tests/serve.rs`).

Clients of mantle's own protocol reach the same operations over QUIC through the native client
library, `mantle-client`, and `mantle`'s object commands, its command line (gateway design §6). The
native protocol's messages, classes and transport bounds are a crate of their own,
`mantle-protocol`, which the node and the client share; the client depends on none of the engine,
the ranges or the chunk store (`cargo tree -p mantle-client`). A client is made by `mantle init
client NAME --cell OUT`: a certificate under the cell's authority for the client role, `client-ID`
acting for the cell's account with `extKeyUsage` clientAuth alone, and an owner-only key; nodes admit
it by that certificate and pin no client, its operations acting for the account the cell's access
keys act for, a node that hosts objects admitting `gateway.clients` of them at once beside its pinned
peers and refusing one past them as an identity past its transport's bound, and a client sending
objects alone, never a range's command (node design §3.6; research/73). A put streams from a source
and a get into a sink a piece at a time within the client's stated memory (a put and a get of 1,024
windows peaked at 408,996 bytes of the 3,821,063 stated, `crates/client/tests/heap.rs`); every
mutation carries its identity, a request is asked again only where no node can have taken it, at
most three attempts, and a put states its CRC-64/NVME in a trailer the node checks before commit,
a whole get held to the full-object checksum kept. Through the library and the command line,
against a cell of three `mantle init` made whose nodes pin each other alone: buckets made, headed,
listed, versioned and removed; objects put from memory, files and standard input (in 8 MiB parts
past one), got whole, by range, to files and to standard output, headed, listed with the account as
their owner and removed; multipart uploads created, their parts put from files and standard input,
listed, completed into one full-object checksum and aborted, each byte read back identical; and a
client certificate another authority issued, or the cell's for a node presented as a client's,
refused at its handshake (`crates/mantle/tests/native.rs`).

A copy into the cell, from a file or standard input, is a native upload of any length in runs the
node cuts (gateway design §2.1): the run length fixed at its opening from S3's part sizes, the
client's buffer and the 10,000 runs a stated length must fit; each run its own exchange, committed
only at the upload's durable offset as its next number and answered with its progress frame; the
MD5 across the runs kept in the upload's row through a digest state the vendored aws-lc-rs exports
and resumes (crypto design §10), the object's ETag at most 5 GiB and the multipart ETag past it;
and the object committed at the end only if the client's length and full-object CRC are the runs',
`BadDigest` with nothing committed otherwise. The library resumes from the node's offset where a
run's exchange ends unanswered; the command line keeps a journal of its open uploads and resumes a
copy run again after a kill, re-reading and hashing what is committed and sending none of it
(research/74). Against a cell of three: a copy SIGKILLed at a run boundary and one SIGKILLed mid-run
each resume at the cell's offset, no committed run sent again, the object whole; a source changed
between attempts ends in `BadDigest` with nothing committed; and a copy whose node is SIGKILLed
mid-run ends `SlowDown`, the cell writing no block while one node of three is gone, and resumes at
the cell's offset once the node is back (`crates/mantle/tests/native.rs`).

`mantle bench transfer` puts objects through a cell of three it makes with `mantle init` and runs
as `mantle serve` children, a fresh cell each case under a directory its operator names: each
object's bytes AES-256-CTR's keystream from the run's seed and the object's name, so a read is
checked by making them again and no copy is kept; a small set's sizes the layout's boundaries and
then drawn below S3's 128 KiB small-object line; each object put through the native door and read
back through a client that asks another node first, every byte exact or counted wrong; latency in
fixed-size histograms (research/76; `crates/mantle/tests/transfer.rs`). The HTTP door, placement
and replication checks, the full-cell case and the controls are the step's next pieces.

Remaining before it is done:

- The windows a gateway admits, from its memory and measured latency; the rates of the paths
  measured on an idle machine.
- The native upload's checkpoint interval from its measured terms, two runs in flight, the
  operation identity's record (item 9), and the journal in the library with its power-cut tests
  (gateway design §2.1, §6); the rest of planned items 12 and 13.
- The HTTP/1.1 listener's remaining parts: the operations beyond the node's own set (copies, tags, ACLs, CORS, lifecycle, policies, object
  lock, encryption settings, DeleteObjects, ListObjects v1 and ListObjectVersions answer `501`
  there), an object's stored headers (`Content-Type`, `x-amz-meta-*`), bucket policies judged on
  each request, the connection budget growing past its floor of ten with the node's memory (the
  AWS CLI's CRT transfers open at least ten and twenty-five at their 10 Gb/s target, research/72)
  and reading the descriptor limit, the reclaim of the slowest connection when the node needs the
  room, TLS session resumption, and the endpoint certificate's issue again before 825 days
  (node design §1.5, §4.1–§4.5); deadlines and
  hedging on the node's asks (node design §5.4); routing by descriptors once ranges split; and
  each request's memory admitted against the gateway's.

The Raft log (`hyper-log`, vendored from hyper-raft with the block layer under it,
`hyper-block`; [design](design/raft-log.md), vendor/UPSTREAM.md), which every range replica on
a metadata device shares: group commit across ranges with one flush a batch, frames whose
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
([measurements](measurements/2026-09-29-log-confirmation.md)). The log was mantle's
`crates/log` until hyper-raft took it with its history (research/32 §5.2, L-1) and gave its
state to one owner thread that answers every call by ticket, waking only its caller (L-2);
mantle runs on that crate since 2026-10-01 and `crates/log` is gone (§5.3). The range
simulation's recorded seeds 1 to 48 replayed to the same histories, step for step, on it as on
`crates/log`, before the replica moved onto the shell. A replica writes and reads its group through the shell's handle on the group
(hyper-durable's `GroupStore` over hyper-log's `GroupLog`, at hyper-raft `df5f8ad`), which
answers the core's reads from what the replica's own answered writes left and submits each
write without waiting, the log's answer waking the replica's owner; the member holds no handle
on the log itself. Before the shell, a committed entry of a three-member group, driven in one
process, allocated 127.2 times and reallocated 4.0 against `crates/log`'s 262.2 and 25.0, and
took a median 143 µs against 151 µs at a load average of 34–35, faster in 27 of 31 paired
rounds; on the move to hyper-log before the handle it took 0.83 ms
([measurements](measurements/2026-10-01-group-log.md),
[before](measurements/2026-10-01-shared-log.md)). `mantle bench log` matches `crates/log`
with one replica and runs more appends a second at 256 replicas: 29.3K against 27.1K at
128 B, in 12 of 12 paired rounds, and 23.1K against 21.1K at 16 KiB, in 11 of 12.

Range replicas (`mantle-range`, [design](design/replica.md)) are hyper-raft's durable shell
over the log and an engine since 2026-10-03 (`hyper-durable`, vendored with the core at hyper-raft
`df5f8ad`; D-1): the range's engine and layer are the shell's state machine, an entry carries a
batch of gateway commands applied as one engine batch, client sessions make each command take
effect once however often it is retried, a write delivered again in a new session is recognised
by its file, or for a write with no file by the ID the gateway drew for its request, and answered
as its first delivery was, members that lag are caught up by snapshot, and reads are confirmed by
ReadIndex. A member lost for good is replaced by one under a new identity: added as a learner,
caught up, swapped in by one joint change, and done once every voter's durable commit covers the
new configuration. The shell takes the core's readies ahead of their writes' answers to the log's
three pipeline frames, taking messages and ticks while writes are out; applies a change of
configuration only once the member's durable commit covers it (the commit fence, focal's F17),
the larger of its log's commit and its engine's durable index, whose term the engine keeps beside
it; states that durable commit in the member's answers (core step R-6), so a replacement ends only
on commits every voter's restart keeps; repairs a member whose log lost a last frame by its lost
entries (R-5) and elects it on its log where the others can be a quorum (R-7); and fences a
member whose log failed a write. Ranges elect on their owner's ticks with focal's counts until the
node carries the node-pair liveness stream. The move found two faults in the core, fixed there: a
member whose writes stayed out through a hundred election timeouts sent 234 vote requests at once
when its device went on, where a campaign now supersedes the requests still waiting; and a
follower's queue of messages grew from four slots again whenever its writes held more queues
than the one spare it kept, where it now keeps one for each write that may be out. Directed power
cuts at every write and flush of a change check for a leader, a follower, a sole voter and a
group shrinking to one; a replacement's leader, a follower and the joining member, each killed
inside the window between a change's commit and the commit its durable state states (at every
write and flush in the simulation, and at every window as real processes with `SIGKILL`), leave
a group whose members reopen in the final configuration once the leader says every voter knows
it, and which elects with any one of them lost; counting a voter that only holds the change fails
them, on mantle's shell before the move and on the durable one. A deterministic simulation of
three or five members and three concurrent gateways, under crashes, failed writes and flushes,
partitions, dropped, duplicated and reordered messages, clock steps, compaction, damage at rest,
and one or two members lost for good in every run, checks after every run that each index was
applied the same everywhere, that every operation completes once faults stop, that every put
exists exactly once, that the members agree, that each key's history is linearizable, and that
every member's configuration names the live members. A soak of 6,000 seeds passed on the shell,
replacing 14,864 members lost for good and repairing all 643 members its damage marked, and the
simulation catches stale reads when they are introduced on purpose.
Against mantle's own shell at `1c179e8` on its workload (hyper-raft's `hyper-durable-compare`,
four sides in one process, the order rotated each round, load 3.7–20.5), a range group of three
or five on real files commits at a median 39–44% lower and 35–67% more entries a second, with
30–43% fewer allocations an entry and the same reallocations; with one member its latency is
even, an entry's latency its write's on either shell, with 21–31% fewer allocations
([measurements](measurements/2026-10-03-range-on-the-shell.md)).

Remaining before it is done:

- Elections by suspicion on hyper-raft's node-pair liveness stream (`hyper-liveness`, L-3): a node
  (`crates/node`) carries the stream for the ranges it hosts, which elect by suspicion over it,
  with the owner that holds its replicas, the datagram plane with kernel receive stamps, a durable
  run, liveness writes on its log, and shard timers that measure their own lateness, its detectors
  starting from the cell's history: each node's time run and crashes in the root range, an orderly
  stop no crash (node.md §2.4 item 7; before it a restarted node's MTBF was twice the seconds it had
  run, [measurements](measurements/2026-10-25-roll-on-a-busy-host.md)). Placement's map of members
  to nodes is left ([design](design/node.md) §2.4), and a node that enlists nothing keeps no history
  of its own. A range hosted by no node elects on ticks.
- Chunks a stopped gateway wrote for a block it never recorded, reconciled per volume once
  storage nodes run, and the collector's pacing against foreground latency (design §2, §6;
  docs/research/22 §10).
- The production engine: a node keeps its ranges' rows in mantle's engine
  (`mantle_range::Stored`, `ShardDb` checkpointed at the applied index, the Raft log its only
  log) on the shard that runs the node, whose owner permits it to wait on the device. Moving the
  replicas to metadata shards apart from the network's (node.md §1.2) is left.
- The replica side of splits and merges: the child's group made on the parent's replicas
  from the parent's engine files as they stood at the split's entry, which carries the split
  alone; a merge's replica sets aligned first, its decision proposed once every replica of
  the frozen range has applied the freeze, and the ended range's group kept until every
  replica of the lower range has taken its rows; the directory the ranges publish their
  descriptors to; which ranges to split and merge, from measured size and load; and a read
  by key answered only by the range whose span holds it, when the gateway's read path is
  built.
- The fast track under simulation. The transport is built on hyper-raft's shared stack
  (node.md §3): a range's Raft messages on QUIC lanes, snapshots on bulk exchanges, clients'
  commands and ReadIndex reads on the request class, and consensus control on the sealed
  datagram plane, and chunk puts and gets on a transfer class between the requests someone waits
  on and bulk (node.md §3.1, §5.2); SWIM and the TCP fallback are left.
- Linearizability with real processes. `crates/node/tests/cluster.rs` checks the histories of
  three clients putting and reading through a leader stopped or cut off by a relay, and through
  a follower lost for good and replaced by a spare, on three and five voters and on both
  engines, with real sockets in one process. `crates/mantle/tests/node.rs` runs `mantle node`
  processes: the leader killed (SIGKILL) mid-write, every answered write applied once and the
  restarted node caught up; the clients' histories through a leader killed and started again
  and through a leader cut off, on three voters and five, and through a leader whose plane goes
  deaf (it hears no peer's plane while they hear its), on three; and a follower whose engine
  store is damaged at rest, which refuses it with a typed corruption error, is replaced by a
  spare, and then as many original voters are killed as leave a bare quorum, the spare in it;
  and a cell hosting a Bucket, a Name, a File and a Block range on every node, voters one to a
  node, each range serving on through the kill of the node leading the most of them.
  Each key's history is checked linearizable (`crates/node/tests/support/history.rs`). Left:
  damage to a node's log (a group the log finds damaged is rebuilt under a new identity,
  `Replica::rebuild`, which needs placement's map of members to nodes).
- The node under deterministic simulation. `crates/node/tests/sim.rs` runs cells of three and
  five nodes and their clients on hyper-rt's simulation runtime. The production node and client
  speak QUIC and the sealed plane on its fabric, and each node's log is on hyper-block's
  simulated device. Each run puts a leader or a follower through a cut, a power cut, or a device
  failing a write, on one of three path profiles, with 2% loss on some seeds. Each key's history
  must be linearizable, and each seed must replay to the same histories (node.md §9.1). A leader
  whose plane goes deaf, before or after it judged its links, steps down and the others elect.
- Done when a linearizability checker accepts histories recorded under network partitions,
  process crashes and disk faults, both in deterministic simulation and with real
  processes.

**Derived settings** ([design](design/node.md) §1.4). `mantle serve CONFIG` runs a node from its
operator's statements alone: its identity and directory, where it listens, its peers, the cell's
founding ranges and voters, its volumes' directories and bytes, its failure domain and the cell's
bounds, its root key's generations, and, where stated, a memory ceiling and the durability
target; any other key is refused at its line. It locks each volume's device directory, measures
the host's memory, the OS's limit, cores and page, the node directory's device, and each data
device, whose profile it keeps in `mantle/profile` and takes again while the device still matches
it; derives every value `mantle node`'s configuration names (`derive::derive`): the cell's settings
and rules, the log, the objects' floor and asks, each volume's layout, each engine's page, extents
and memtable, the transport's floor, the floor class by class within the allowance, and what
remains shared by each volume's index and each engine's resident metadata; and says each value
with its basis before it runs as `mantle node` does. A node started again takes its stores' and
volumes' shapes as they were made. Tested by unit tests of every model and by a cell of three
`mantle serve` processes that enlists, serves objects, and says the same values after a node is
killed and started again (`crates/mantle/tests/serve.rs`). `mantle init` makes a cell from its
founders' statements, given as `--node` flags, a machines file or both: the authority, the root key,
the founding ranges' groups and each founder's identity with every other founder's certificate
pinned, each founder's directory with its stated configuration (paths relative to it, so it moves
to its host whole) and owner-only keys (mode 0600; a protected DACL on Windows), and for a cell of one
its calibration and device profiles measured; a cell of one and a cell of three serve objects from
what it writes. A `peer` line may name where the peer listens, by address or by a name the node
resolves as it starts to its own socket's family. The node directory's `LOCK` is held while the
process runs; the start runs node.md §1.7's steps in order, a failure naming its step (`mantle: the
start's locks step: …`). SIGTERM or SIGINT (Ctrl-C or Ctrl-Break on Windows), or a `stop` line,
stops the node in order within `stop_ms`: new objects answered `SlowDown`, those running ended, each
led range handed to a caught-up member that then leads (handed again where its core gives a
transfer up), connections closed and left to drain, exit 0; a second request ends it at once
(`crates/mantle/tests/serve.rs`). A start refuses, at its identity step and naming the file, a key
readable by others than its owner (the node's TLS key and each root key, on every platform: no
group or other mode bit on Unix; on Windows no DACL entry for a principal but the owner and
LocalSystem, read from the handle the key is read through) and a certificate the stated authority
did not issue (the node's own and each pinned peer's: RFC 5280 §6.1.3 (a)'s issuer, signature and
validity); and refuses no configuration, and a volume another process holds, at their steps, and a
plane or QUIC port another socket holds at its network step (hyper-rt-transport's listener binds its
port before it shares it). A node whose peers have never answered heartbeats each every 3 s at most,
not at its stream's floor with a flush each (hyper-liveness `f647365`). A cell
of three made by `mantle init` rolls one node at a time, each handing its ranges over, announcing its
end to its peers' liveness streams and started again from its directories, with no other member
saying a range has no leader, and every object read back through every node after the roll, on two
CPUs beside two or four busy loops and on one CPU: on CI's busy runners 0 of 48 rounds failed of the
serve suite and 0 of 48 of tests/node.rs (diag run 40170267351), where the roll failed 2 of 16 and the
objects round trip 4 of 19. Their causes: a false suspicion between the two voters left in a range
cost its leader, which hyper-raft's core no longer pays where acting on it cannot elect anyone
(`d97153c`, model-checked); restarted nodes' detectors assumed an MTBF of their own seconds (§2.4
item 7, the cell's history, now kept); and the transport refused a stream its reader took at its
own pace as unread (hyper-raft `1912194`, ported)
([measurements](measurements/2026-10-25-roll-on-a-busy-host.md)). Two later CI runs failed the roll
once each, on linux-aarch64-1cpu and macos-aarch64 (40161728764, 40223736182): a node that asked its
statement of its history of a leader whose stop then closed its connections took the reply, told
after the transport had ended the exchange, as an empty head, refused its own statement `Malformed`
and ended; a request or reply whose head is gone is now its exchange's end, as the refusal queued
behind it says ([measurements](measurements/2026-10-25-a-reply-past-its-connection.md)).
Remaining: `mantle init client`;
Windows' console close and shutdown taken as requests to stop, which hyper-rt's handler now holds
for the owner's stop; a replicated range refusing puts at its engine's metadata share where its
leader admits a command, since a refusal at apply would let members diverge; and hyper-raft's
`cc5a65b` (a stop suspected as its announcement arrives, and answered, so a stop may announce before
it hands over): taken by mantle, the roll failed both macOS CI runs (a member naming no leader for
every range after a stop; branches `cell-land-cc5a65b`, `cell-land-announce-first`), to be traced
before it lands. On macOS x86_64, which no CI target supports, tests/node.rs's replaced node of five
fenced once on a snapshot its engine refused (`the engine refused a snapshot`), the engine's error
not kept by the fence (`crates/range/src/stored.rs`).

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
   *Done on macOS (2026-09-30)*: the log's `Room` (now hyper-log's, held by its owner thread)
   hands freed room to waiters in arrival order, each woken by its own `unpark`, its list bounded at `max_groups` times a group's two;
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
   *The portable pool is done on macOS (2026-09-30)*: the process thread budget (now
   `hyper_block::threads`, which mantle-disk's measurement and the issuer both draw on) reads its
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
   *Writes through the issuer done on macOS (2026-10-01)*: the issuer (now `hyper_block::issuer`) is one thread
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
   *Open*: a replica's chunk put waits one idle timeout (3.07 s) for its answer once the peer
   has the request, whatever the peer's device takes; a durable write slower than that ends the
   put as stalled, and a cell with no spare volume fails the object (on windows-11-arm a
   volume's first write took up to 8.0 s before calibration saw NTFS's zeros;
   measurements/2026-10-24-first-writes-past-unwritten-space.md). The answer's period belongs
   with the peer's measured write time, as a command ask's comes from its slowest answer.
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

12. **The native protocol and the client library and CLI**, of which S3's operations natively,
    streamed within the client's memory, and the client admitted by its certificate are built
    (above); the rest: credits,
    typed refusals and the admission level, the server's upload plan, per-block GET streams with
    CRC-64/NVME, migration on interface change, TLS session resumption without early data, the
    TLS-over-TCP route, and the journal with its flush rules (node.md §3.9, §4.1; gateway.md §3,
    §6). Done when the client killed and its storage cut at every journal write and protocol step
    ends every operation answered, resumed or provably restarted, and a Wi-Fi to cellular switch
    mid-upload continues or resumes with no committed run sent again.
13. **Resumable native uploads** in server-cut runs with progress frames, the checkpoint interval
    of gateway.md §2.1, MD5 and block-hash state carried across runs, and digest-state export
    and import in the vendored aws-lc-rs (gateway.md §2.1; metadata.md §2; crypto.md §10), of
    which the runs, their frames and the client's resume from the node's offset, the MD5 carried
    across runs and its state's export and import, the run length from the bounds, and the kills
    at a run boundary and mid-run of the client and of a node are built (above); the rest: the
    interval's measured terms, two runs in flight, block hashes besides MD5. Done when the six
    obligations hold under interruption at every frame, a changed source ends in `BadDigest`, and
    the ETag equals S3's for objects at and above 5 GiB.
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
