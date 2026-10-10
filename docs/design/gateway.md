# The gateway's object path: from a PUT's body to chunks, and back

Status: design, 2026-09-29; operation identity, resumable uploads, caches, integrity and the
client 2026-09-30. Sources: docs/research/01 (Tectonic and Meta's storage, cited as
"01 §x"), 04 (erasure coding and placement), 05 (S3 semantics), 27 (upload scheduling), 28
(storage classes), 30 (resilient transfer), 31 (caching, ordering and integrity);
docs/design/metadata.md (the Name, File and Block layers), encryption.md (sealing),
durability.md (the scheme a block is stored in), chunk-store.md (volumes), node.md (admission,
transport and integrity), storage-classes.md.

The gateway turns a PUT's body into chunks on storage nodes and rows in the metadata
ranges, and a GET's range back into bytes. It writes chunks directly, as Tectonic's client
library does: it "replicates or RS-encodes data and writes chunks directly to the Chunk
Store", and "reads and reconstructs chunks" (01 §1.7). `mantle-s3` holds the protocol; this
record holds where the bytes go. The path is a state machine that names each read, write and
command and moves on with its answer, doing no I/O, as the bucket coordinator does
(metadata.md §2), so one gateway, many, or a simulation drive it alike.

## 1. Layout

An object's bytes pass through three shapes on the way down.

- **Sealed.** The plaintext is sealed in 64 KiB segments, each growing by a 16-byte tag, under
  the file's own data key (encryption.md §3). A file's stored bytes are the sealed stream, and a
  plaintext offset maps to a stored one by arithmetic alone.
- **Blocks.** The sealed stream is cut into blocks, each holding whole segments, so a segment
  lies in one block and opening it reads one block. A block holds as many segments as fit in
  the scheme's data chunks at the chunk size (below); the last block holds the rest.
- **Chunks.** A block is stored in its scheme (durability.md §4): as copies, each chunk the
  whole block, or Reed–Solomon coded into data chunks that hold the block's bytes in order and
  parity chunks (`mantle-ec`). Each chunk is at most the chunk size. The data chunks are
  slices of the block, which they share, and the parity is computed from them where they
  lie, so coding a block adds only its parity and a last data chunk the block does not fill,
  padded; each data chunk was a copy, and a PUT held the block twice while coding it (audit
  P08; [measurements](../measurements/2026-09-29-erasure-copies.md)).

**Decision: chunks of up to 8 MiB.** Tectonic divides blocks "into smaller chunks (typically
8 MiB)", 72 MiB blocks being RS(9,6)'s nine data chunks (01 §1.14), with "block sizes large
enough that disk bandwidth, not IOPS, is the bottleneck for full-block writes" (01 §1.8). The
chunk size is the one parameter of the layout, and a chunk store's largest record holds it.
Measuring where a device's transfers reach its bandwidth, as calibration's plan proposes
(research/11 §13.4), is what would replace the cited value. What a small request waits behind at
a device is not the chunk but the device dispatcher's unit `u`, the smallest transfer that
reaches the device's bandwidth, in which a chunk goes down (node.md §2.7; chunk-store.md §4), so
the chunk size bounds the block, not the head-of-line delay.

The File row's extents are the blocks in order, each its stored length. The File header holds
the stored length, and the data key wrapped (encryption.md §2): an upload's part is a file
with a key of its own, and a completed upload is a file of part files, whose bytes each open
under their own part's key. A file of parts is addressed by plaintext: its extents are its
parts' plaintext lengths, and its header's length the object's size. Each part seals its own
last segment, so no arithmetic maps the object's plaintext to its parts' stored bytes; keyed
by plaintext, the part holding any byte is one seek away, where stored lengths, each part a
tag or more longer, would place the tail of a 10,000-part object only after reading every
part before it (audit §16.7). An empty object is its version alone, with no file, as the
version row allows; a part row names a file, so an empty part has one, of one block holding
its one empty segment, sealed. An empty object's write still carries the file ID the PUT
drew, which no file takes, as its own ID (§2).

## 2. Writing an object

A PUT writes bottom up, as metadata.md §2 lays out, and its Name commit is its linearization
point:

1. **Identities.** The gateway draws a file ID and a data key from the operating system, and a
   block ID for each block as it begins it. Each is random and 128 or 256 bits, never reused:
   a retry writes a new file (metadata.md §2). The request's operation identity, which the
   client drew (§2.1), is carried through to the Name commit.
2. **The body streams.** Each 64 KiB of plaintext is sealed as it arrives, the last segment
   marked at the end, and any checksum the request asks for runs over the plaintext
   (`mantle-s3` checksum.rs). A CRC-32C of each plaintext segment is taken as it arrives and
   combined into the request's checksum, and once the segment is sealed it is opened on a
   different core and its plaintext's CRC compared, so a flip in the buffer between the client's
   check and the seal, or a core that seals wrongly, is caught before anything is stored
   (node.md §5.6). The ETag is the MD5 of the plaintext under SSE-S3, and of the
   segments as sealed under SSE-C (encryption.md §4); the plaintext's MD5 runs under SSE-C
   only to check a `Content-MD5` the request sent, a header the PUT is told of when it is
   made. Sealed segments fill the current block.
3. **A full block goes down.** It is coded into its chunks, the parity checked against the data
   by a random linear combination on another core (node.md §5.6), and each chunk goes to its own
   volume with its CRC-32C table per 64 KiB, which the storage node verifies and stores
   (chunk-store.md §3.1; CLAUDE.md §6). The block's chunks are written at once, on distinct
   volumes chosen by two random choices on credits among placement's candidates (node.md §7),
   in the pool and the stream the object's storage class plans (storage-classes.md §5).
4. **The block is recorded** in the Block range once every chunk is durable, with the file it
   was made for and a handover deadline (metadata.md §2, "Blocks never named"). A write is
   acknowledged only once durable on every copy its scheme stores (CLAUDE.md §6): the gateway
   waits for every chunk, where Tectonic acknowledges at a quorum and repairs the last chunk
   offline (01 §1.8), which needs repair to run.
5. **The file is written** in the File range once every block is recorded, naming the blocks,
   the key wrapped, the object key it is for, and a deadline no later than its blocks'
   earliest.
6. **The version is committed** in the Name range, carrying the file and its deadline.
   Preconditions, versioning and Object Lock are judged there (metadata.md §2). An empty
   object has no file and no File range to give it a deadline: its write carries the file ID
   drawn in step 1 as its own ID, and a deadline of the same handover time from the gateway's
   clock when it commits, so the Name range recognises a copy of it as it would a file's
   (metadata.md §2, "The mark").

**A chunk refused** by its volume, busy, full, failed or unreachable, goes to another volume the
placement offers that holds none of the block's chunks. When none is left, the PUT fails with
`503 SlowDown`, and what it wrote is left to the sweeps: blocks and files never handed over
are found and released (metadata.md §2), and chunks no block names are reconciled per volume
(STATUS).

**Admission.** A PUT declares its length, and one longer than a single request carries,
5 GiB (research/05 §4.1), is refused as `EntityTooLarge` when the PUT is made, before a chunk
is written (audit B08). Within that length every layout's blocks fit a file's 10,000 extents:
the layout with the smallest blocks, one copy, holds 127 segments a block, so the longest body
takes 646 blocks. The File range therefore never refuses a body after all its chunks were
written, and the blocks a PUT keeps for renewal, with its file's manifest, are bounded by its
admission.

**Blocks in flight.** A PUT holds the block filling and at most `window` full blocks going
down at once, a number its caller admits from the memory it gives the PUT; the body waits
while that many go down. Each block's write is a chain of round trips, placement, its chunks
and its Block row, so with one block going down a body's round trips add up block by block:
a 64 MiB object in three copies, eight blocks, took 29, and a single part's rate is bounded
by a block's bytes each chain (audit §16.3). With every block of it admitted at once the
same object takes 5, as a one-block object does (`mantle bench gateway`, "all out"; round
trips do not depend on the machine). By Little's law a window of `w` sustains `w` blocks'
bytes each chain's latency, and the memory it takes is `w` blocks and their parity; what `w`
a gateway admits follows from its memory and the latency it measures, which the node sets. The
rate in that law is the rate the upload is entitled to, the lesser of what its client sends and
its principal's fair share over the authorities it uses (node.md §2.6–§2.7), recomputed at each
block: the window that makes an upload fast when it is alone is the window that makes it yield
when others arrive, its share falling, its window shrinking and the memory it held returning,
and regrowing to `⌈R·T/B⌉` at the next admission once they leave (research/27 §6.1, §6.4, D7).

**Deadlines** pass up the layers: each write answers with the deadline by which the layer
above must take what it wrote, and the gateway's handover time is the per-step deadline times
its retry budget plus the clock offset between ranges, measured once gateways run
(metadata.md §2, "The deadline's length"). A block recorded while the body still streams in
would outlive that deadline before its file is written, so the gateway renews each block it
recorded once a quarter of the handover time has passed since it asked for the write or
renewal that set the block's deadline, as Centrifuge's owners renew 60 s leases every 15 s, so
that three renewals in a row must be lost before a lease lapses (research/09 §7.2.2). Its timer
starts when it asks, before the range stamps the deadline, as Centrifuge's owner's timer starts
before its manager's. A renewal the Block range refuses, the sweep having released the block,
fails the PUT. Once the body has ended and every block is recorded, the blocks due are renewed
one last time and the file is written when they answer, so its write starts with every block
held as long as a one-block PUT's is; those renewals are not repeated while they answer, since
one slower than a quarter of the handover would find the next already due. The blocks wait for
renewal in order of when each is due, so a turn takes only the due ones and touches no other,
and at most every recorded block is due at once, which admission bounds. Renewals of blocks
that one Block range holds go as separate commands; whether they should go as one, the
batching of §16.5 of the audit, waits for the gateway's driver, which routes them.

**Checked** (`crates/gateway/tests/object_path.rs`) against volumes that check each chunk's
CRC-32C and Block, File and Name ranges in memory, their entries stamped by a clock the test
moves: objects of three blocks and coded objects read back from their chunks under their keys,
refused chunks land elsewhere, released blocks fail the PUT, empty objects and parts take the
shapes above, and generated schedules of bodies, client pace and refusing volumes commit each
object whole or nothing; without renewals, a slow client's PUT fails.

**Completing an upload** (`complete.rs`) reads the upload's row and its parts from the Name
range, a page at a time as many as the parts listed past the last read, and checks the listed
parts against them: each uploaded, with the ETag the request sent, and each but the last at
least 5 MiB (05 §4.4). The object's size, ETag and checksum are derived from those rows and
never taken from the request: the size the parts' sum, the ETag the MD5 of their MD5s with
their number (05 §4.5), and the checksum, in the algorithm the upload named, the full-object
CRC combined from each part's value and length, or the composite hash of the parts' values
(05 §3.4). Composite checksums need parts numbered from 1 without a gap, which S3 answers
with a 500 and mantle refuses as `InvalidPart` (05 §3.3), and an upload whose row names a
combination its algorithm lacks is refused. The object's file of parts is written with each part's plaintext length, an empty
part left out, and the Name range's Complete checks the parts again against its own rows,
refusing a size that is not their sum or an ETag that does not name their number
(`Miscombined`), and commits (audit §16.5). The pages and the listed parts both ascend by
number, so the completion matches them in one walk along each, not a search of the list for
each row.

A completion whose upload is gone asks the Name range for the version a completion of the
same parts made, and gets it or `NoSuchUpload`, the error AWS gives for an upload that "might
have been ... completed" (05 §4.4). S3 re-completes an identical part list, returning the same
ETag and checksum, as s3-tests checks and SDK retries rely on (05 §4.4), so a retry is matched
as strictly as a first completion: each Complete carries the SHA-256 of the parts listed, their
numbers and their ETags as sent (FIPS 180-4), the version keeps it, and a retry is answered
only if its list has the same digest and the version the same ETag. Before, a retry was matched
by the multipart ETag alone, which names neither the parts' numbers nor their ETags' text, so
a list the first completion would refuse as `InvalidPart` was answered as the object. The
answer to a retry carries the version's size and checksum, which the gateway, holding no part
rows, reports as the first completion did.

A Name command whose session expired before it was answered has an unknown outcome
(replica.md §1, 06 §A1.8): the gateway registers anew and re-sends it unchanged, with the
same file and the same ID. The range recognises the file it already took, or for a write
with no file, an empty object's or a completion of empty parts, the ID, and answers as it
answered the first delivery, a PUT or completion with its version's ID and a part with
`PartWritten`, or `Expired` when the first was refused and its file released, after which
the gateway writes a new file and makes the request again as a new attempt. A copy never
takes or releases the file, so no re-send can give a file a second referrer, release one a
version holds, or make a second version of one request. A write with no file that was
refused changed nothing, so its copy is judged afresh. A copy that comes after its write's
deadline, once the range has let the mark go, is refused `Expired` like any late write
(metadata.md §2). Every Completion carries its file's ID as its own and a deadline a
handover from the gateway's time, which the File range's deadline replaces when it writes
the file of parts.

### 2.1 Operation identity, and native uploads that resume

**What a client knows after a fault.** A write passes seven points: the transport's
acknowledgement, the bytes read, a segment sealed, a chunk stored, a block recorded, the file
written, the version committed; and then the answer delivered (research/30 §2). Nothing is
committed before the body's end, length and checksums have checked (node.md §4.3), so a client
that had not sent its last byte knows its write did not commit and may start again; only a
client that sent its last byte, or its completion list, and got no answer cannot tell. Within
one gateway's attempt the machinery above already answers a copy as the first (`SessionExpired`,
the file's mark). What was missing is identity across attempts: a retry after the answer was
lost, through another gateway or after a restart, drew a new file and was a new write. In a
versioned bucket it made a second version; under `If-None-Match: *` it was refused `412`
against its own success; and a slow first attempt could land after its retry and replace a part
whose ETag the client already held (research/30 §3.3, G1). S3 has these hazards; mantle need not.

**Decision: every mutation carries an operation identity** (research/30 §3.4, D2). It is 128
random bits the client draws before its first attempt, journaled and flushed before its first
byte is sent (§6), and carried unchanged by every attempt, through any gateway, after any
restart. Native requests must carry one. On the HTTP/1.1 listener the identity is
`amz-sdk-invocation-id`, which aws-sdk-go-v2 sets "to a fresh UUID per operation" and botocore
copies onto every attempt (research/30 §3.4); a request without it is handled as S3 handles it,
a retry being a new write. The value is the client's, so it is scoped to what authentication
established: the principal, the bucket and the key, and for a part the upload and part number. A
forged or reused value can then collide only with its own principal's operations on that key.
The Name range records it with what the first delivery made and answers every later attempt from
the record (metadata.md §2, "Operations"): the same version ID, ETag and checksum, nothing
applied, the attempt's own file released as a refused write's is. A refused first attempt (a
precondition, a lock) records nothing, since it changed nothing, and a later attempt is judged
afresh. Its proof obligations are five: at most one effect per identity within its horizon, the
same answer to every attempt, no stale attempt replacing a later write, every attempt's file
released exactly once, and the record carried by a split with its key.

**Decision: a native upload resumes at a durable offset the server reports, in runs the server
cuts** (research/30 §3.5, D3). The IETF's resumable-upload draft and tus give the semantics,
which mantle carries on its own protocol rather than HTTP's syntax: the client reads the upload's
offset and appends to it, "the offset MUST NOT decrease", an append at the wrong offset is
refused with the correct offset, and a server that loses any of the upload's state deactivates
it. A native upload is a multipart upload whose part boundaries the server chooses:

1. *Create.* The client opens the upload with its operation identity, the object's length when
   known, the checksum algorithms it will verify, its source's identity (§6) and its resume
   horizon. The Name range writes the upload's row, as CreateMultipartUpload does, and the
   identity's.
2. *Runs.* The gateway streams the body as a PUT does into a run: a file with its own data key.
   At a checkpoint it ends the run, its last segment sealed as last, its blocks recorded, its
   file written, and hands the file to the Name range as the upload's next run, a part whose
   number the server assigns. From that commit the run is owned like any part, and no renewal
   holds it.
3. *Progress.* After each run commits, the client is sent a progress frame: the durable offset,
   the sum of the committed runs' plaintext lengths, with the run's number and CRC.
4. *Resume.* On reconnect the client asks for the upload's state and appends from the offset it
   gets; an append at any other offset is refused naming the right one. The server's offset is
   authoritative both ways: below the client's belief, the tail was never durable; above it, a
   run committed whose progress frame was lost.
5. *Complete.* The client sends its full-object checksum, the gateway combines the runs' CRCs
   and commits the object only on a match, with the identity's record; a mismatch is
   `BadDigest` and commits nothing.

Runs rather than an offset into one file, because a file has one data key and seals segment `i`
under nonce `i` (encryption.md §3): resuming inside a file would seal again, after a crash,
segments whose earlier sealing may have reached a chunk store, and a resent plaintext that
differed by one byte, a changed source or a client's bug, would reuse an AES-GCM nonce with
different plaintext, forfeiting confidentiality and authenticity. A run ends at every
checkpoint, and every interruption starts a new run under a new key, so nonce reuse is impossible
by construction (research/30 §3.5; audit §16.4). An interrupted run's unrecorded tail goes to the
sweeps like any abandoned body. Durable progress then needs no gateway: a gateway's power cut
costs one run of replay per open upload (research/30 §5.2).

**The ETag S3 would give.** An object over 5 GiB is in S3's terms a multipart object, its runs
its parts, and its ETag the MD5 of the runs' MD5s with their count; its runs keep S3's part
sizes, 5 MiB to 5 GiB, all but the last. An object of at most 5 GiB is in S3's terms one PUT,
whose ETag under SSE-S3 is its plaintext's MD5: the gateway keeps one MD5 running across runs
and records its state in the upload's row with each run's commit. A checkpoint on a 64 KiB
boundary leaves MD5's 64-byte buffer empty, so the state is four 32-bit words and a length,
committed in the run's transaction, and cannot drift from the data; the other block hashes the
request named are kept alike, and CRCs need no state, since they combine (research/30 §3.5). An
SSE-C upload needs the customer's key at every resume; the server never stores it, nor anything
derived from it alone (encryption.md §2), so the client sends it again and the gateway checks it
by unwrapping a committed run's data key with it, which fails under any other key. Research note
30 proposed checking the key's MD5 against the upload's row; that would store a value derived from
the customer's key, which encryption.md §2 refuses, and the unwrap checks the same thing.

**The checkpoint interval** (research/30 §3.5, §4.6, D4) is the largest of: the smallest interval
the principal's metadata share admits, `g/r_meta` for goodput `g` and admitted run commits a
second `r_meta`, where the next run streams while the last commits, so a checkpoint costs
metadata and no time; Young's `g·sqrt(2h/λ)` for checkpoint cost `h` and interruption rate `λ`,
where a commit stalls the stream, as on a client whose source cannot read ahead; `S/10,000` for a
known size `S`, so the runs fit a file's extents; and 5 MiB where the multipart ETag applies. It is
at most the client's buffer when its source cannot be read again, since the client holds every
byte past the durable offset until its run commits, and it falls on 64 KiB boundaries. `g` counts
durable bytes, never transport acknowledgements; `λ` counts events that restart work, never
packet loss, which QUIC repairs within the stream, and is estimated as `k/T` with its exact
Poisson interval, the upper bound alone guiding the first runs; `h` is measured per run. A new
interval applies to runs not yet begun, never to bytes sealed, and changes only when the
interval of the predicted saving excludes zero and exceeds the measured cost of switching, each
change recorded with its inputs (audit §16.2).

**Stock clients keep S3's own resume** (research/30 §3.6, D17). Through the HTTP/1.1 listener a
client resumes only by multipart: a committed part is a Name row nothing but abort, completion,
replacement or lifecycle removes; `ListParts` lists only committed parts, at most 1,000 a page;
with `amz-sdk-invocation-id`, a part's original attempt landing after its retry no longer replaces
it; a completion is answered when its result is ready, and switches to `200` followed by
whitespace only if its measured latency comes near the shortest read timeout of the clients
served; and open uploads are listed by `ListMultipartUploads`, removed by the bucket's
`AbortIncompleteMultipartUpload` rule, which mantle serves from the first release because stock
tools leave uploads behind, and their bytes reported per bucket and principal. mantle invents no
default expiry for data a client may still mean to finish. A ranged GET carrying `versionId` or a
strong `If-Match` is answered only from that version, so a resumed download is never stitched
from a newer one (audit §16.4).

## 3. Reading an object

A GET reads top down (`get.rs`), from the version the Name range read, whose preconditions,
lock and range the S3 layer has judged:

1. The version's File header. A file of blocks has its data key, which is unwrapped; a file of
   parts names, by plaintext, the part holding each byte, and each part is read as a file of
   blocks under its own key.
2. The next wanted segment's stored offset, the segment's index times a sealed segment, finds
   the block holding it by one seek of the file's extents, and the block's row gives its
   chunks' volumes. Addressing by the extents rather than by the layout's arithmetic reads a
   file whatever block size it was written with.
3. Only the chunk bytes holding the wanted segments are read: a copy's, or each data chunk's
   part of them. A chunk read that fails or does not verify, the chunk store's `Corrupt`
   (chunk-store.md §7), is read from another copy, or its block decoded from any `data` whole
   chunks, checked against the block's CRC-32C.
4. Each segment is opened, its tag checked under the file's key and its index, the last marked
   as sealed, and the range cut exactly.

A GET holds at most `window` blocks, being read or read and waiting to be taken, a number
its caller admits from the memory it gives the GET: with two, one is read while the one
before it waits for the caller. It asks for extents a page at a time, as many as the window
has room for, reads that many blocks' rows and chunks at once, and gives out blocks in order
whichever finishes first. A 64 MiB object in three copies, eight blocks, takes 16 round trips
with two blocks held, where reading one block's extent, row and chunks after another took 28,
and 4 with every block out, as a one-block object does (`mantle bench gateway`; round trips do
not depend on the machine). Parts are read one after
another, each found by one seek, so a read of the last byte of an object of 10,000 parts asks
for one part's header and one block.

**Where order is restored** (research/31 §4.3, D9). A QUIC stream is delivered "as an ordered
byte stream", so one GET on one stream waits behind its slowest block, and a lost packet holds
every byte after it. On the native protocol a GET therefore carries a control stream (its
headers, version, the checksum to expect, errors) and a stream for each block in flight, each
framed with its offset, length and the CRC-64/NVME of its plaintext, which the client verifies.
A slow block holds only its own stream; the blocks behind it are delivered and held at the
client, within the window the client was granted, or written straight to their positions by a
client writing to a file. Gateway memory per slow client falls to the blocks in flight, and
stream credit keeps a stalled client from making the gateway hold more. On the HTTP/1.1 listener,
which must answer "in the same order that the requests were received" (RFC 9112 §9.3.2), bytes go
out in order from the window, pipelined requests are served one at a time, and the response
checksum is the stored value sent in headers before the body (research/31 §4.4, D12).

**Head-of-line is bounded by hedging** (node.md §5.4). A block outstanding past the measured
hedge quantile is read from another copy or decoded, and the window is sized from the chain
latency at that quantile, `w = ⌈R·T_q/B⌉`, so a block at the hedge point does not starve the
client (research/31 §4.8). The chunks a GET reads arrive with their stored CRCs, which it
verifies before decoding or opening (node.md §5.6).

**Checked** (`crates/gateway/tests/object_path.rs`): objects under copies and codes read back
whole and in ranges across segment and block boundaries, with requests answered in either
order, and any range of generated objects; a chunk a volume cannot read is read from another
copy or decoded, and a GET with more lost than its scheme tolerates fails naming the block;
bytes that are not the block's fail by a segment's tag, and by the block's CRC-32C once
decoded; an object of a part of 5 MiB and one byte and a part of one byte reads by its parts'
plaintext; and a GET whose caller takes nothing reads as many blocks as its window and no
more, a wider window taking fewer round trips, blocks finished out of order given out in
order.

## 4. Costs and open questions

- Memory: each PUT holds up to two blocks' bytes and one block's parity, 192 MiB under
  RS(9,6), and while it codes a block the coder's work space, which the coding library sizes
  to the code: 128 MiB to encode 8 MiB chunks under RS(9,6), and a read that decodes one,
  256 MiB (research: the library's `work_count`, measurements/2026-09-29-erasure-copies.md).
  The gateway admits PUTs and reads against the memory it has, as every other queue is
  bounded, and decides whether to keep coders between blocks, which saves 6–10% of coding
  time at 1–8 MiB chunks for memory held between them.
- Hedged writes: Tectonic reserves more storage nodes than a block needs and writes to the
  first that answer (01 §1.8); mantle does so at the cell step, `Δ` from the measured tail
  (node.md §5.4).
- Small objects: a coded block of a few kilobytes costs as many writes as its code is wide,
  where Tectonic appends small blobs replicated and re-encodes them sealed (01 §1.9). The
  per-request cost is measured and reported (node.md §2.7); packing waits for its ownership
  rules (audit §14.4).
- Placement across failure domains is the placement driver's (STATUS); the path takes the
  volumes it is offered, choosing among candidates by two random choices on credits.
- Digest state: the ETag of a resumed native upload needs MD5's and the other block hashes'
  state exported and restored at a 64 KiB boundary, which aws-lc-rs does not expose
  (research/30 §11; crypto.md §10).
- The client's horizon: how long agents and laptops take to come back, which sizes the
  operation table and the open-upload storage (research/30 §11).
- Stock clients' read timeouts, which decide whether a completion ever needs the
  200-then-whitespace form, are to be recorded against botocore and the SDKs (research/30 §11).
- A block decoded around a lost chunk pays a cost fixed per decode, whatever its size: the
  coding library evaluates the erasure locator with two Walsh–Hadamard transforms over all
  65,536 elements of GF(2^16) (reed-solomon-simd 3.1, `rate_low.rs`/`rate_high.rs` calling
  `eval_poly` with `GF_ORDER`), of which only the positions of the code's chunks are used.
  Small blocks, and their repair, are read far slower degraded than whole. The locator
  depends only on which chunks are missing, so it could be kept per loss pattern, or
  evaluated at those positions alone: a change to the library, to be made in a vendored copy
  with its tests and offered upstream, and measured on an idle machine.

## 5. Caches

**Decision: data caches are keyed by identities that are never reused, so they need no
invalidation, and the whole consistency obligation sits in one read: which version a key names**
(research/31 §3.1, D1). A file ID, block ID and chunk key are random and never reused, and a
file's extents never change once written, so a cache keyed by them can be stale only by holding
what nothing names any more, which costs memory and never correctness. Block rows' chunk
locations do change, by repair and moves under compare-and-swap, so a cached location is a hint:
a chunk store answers a stale one with not-found or with the same bytes, verified by the record's
own identity, never another chunk's (research/31 §3.1). What a key maps to, whether it exists, a
listing and a bucket's configuration do change, and those caches carry the obligation.

What the gateway keeps, each within the share of memory the node's division gives it (node.md
§2.5):

| Cache | Keyed by | Consistency |
|---|---|---|
| Sealed block bytes | block ID | none needed; served only after their tag verifies at open |
| File headers and extents | file ID | none needed; never change |
| Block rows | block ID | hints; a stale location is refused at the chunk store |
| Name rows, versions and absences | `(bucket, key)` | validated before every use (below) |
| Bucket rows | bucket | bounded staleness, refreshed by a constant-work loop; the gate refuses a stale incarnation (metadata.md §1–§2) |

**Name rows are validated, not trusted and not invalidated** (research/31 §3.6, D3). A gateway
holding `(bucket, key) → (version, file, commit index)` serves a GET from it only after the Name
range's leader confirms, at a read index from a ReadIndex round that began after the GET arrived,
that the key's first row is still that version; the leader answers from one point read, a block
cache hit when the key is hot, and returns the current row when it is not. Conditional reads are
judged against the validated row, and `304` is answered from it without reading chunks. This is
the shape S3 chose, keeping its caches and adding a witness that "acts like a read barrier", and
TAO's, whose followers send the version they hold and get no data back when it is current.
Invalidation was refused: it needs, for every key, the set of gateways holding it, a map keyed by
what clients choose, and holds every write on the slowest holder, Chubby's node "uncachable while
cache invalidations remain unacknowledged". Leases wait for a stated clock bound (replica.md §7).
Validation costs what a GET pays today for its Name read, and saves the row's bytes and every
File, Block and chunk read after it.

**No negative cache that is not validated** (research/31 §3.7, D5). An absence is a row,
`(bucket, key) → none as of index i`, cached and validated exactly as a version is. A negative
answer served on a timer is what made S3 eventually consistent before December 2020: "if you make
a HEAD or GET request to the key name ... before creating the object, Amazon S3 provides eventual
consistency for read-after-write" (research/31 §3.7). That form is forbidden at every layer, the
client library's included.

**Swarms coalesce, on one condition** (research/31 §3.8, D4). Concurrent GETs of one block at a
gateway wait on one fetch, keyed by block ID, unconditionally. Concurrent GETs of one key share a
validation only if the ReadIndex round they share began after each arrived: a request joining a
round already out could miss a write acknowledged between the round's start and its arrival,
which is exactly S3's guarantee to reads "initiated following the receipt of a successful PUT
response". So a joining request waits for the next round, and rounds are taken back to back while
requests wait: one quorum round a heartbeat serves every reader of every key at that leader. Hot
blocks may be held at every gateway of the tenant's shuffle shard, since immutable blocks need no
invalidation to replicate; a front-end cache of `O(n log n)` entries for `n` back ends balances
load "regardless of the query distribution", so the hottest `O(n log n)` blocks across a cell's
gateways, `n` its volumes, keep any one volume from being hot (research/31 §3.8). Concurrent
identical LIST pages share a scan on the same next-round rule. LIST pages are not cached: that
would need each range to keep the last write index of each span, and waits for a measurement
showing listings of unchanged spans dominate a workload (research/31 §3.6).

**Sealed bytes, never plaintext** (research/31 §3.9, D8). A plaintext cache would serve an SSE-C
object to a request that never presented its key, which S3 requires on every request, and serve
bytes no tag checks again. The block cache holds sealed segments; every serve is an AES-GCM open,
about 8.4 GB/s a core (encryption.md §3), which verifies the tag, so a flipped bit in the cache is
caught on serve. Unsealed bytes, where a deployment stores some, carry their CRC tables and are
checked on serve the same way. Unwrapped data keys are not cached across requests; unwrapping
costs 1.8 µs. A cached row keeps the CRC it was read with and is checked on every use, a CRC over
tens of bytes (research/31 §5.4, B14). A cache is filled only from bytes that verified on the way
in, and is never a source for repair or a write (research/31 §5.8).

**Policy and size by measurement on the node's own references** (research/31 §3.4–§3.5, D6–D7).
Every cache is one lock-free-on-hits implementation with a pluggable eviction policy; the node
runs scaled-down simulations of FIFO, CLOCK, S3-FIFO, W-TinyLFU and ARC over a spatially hashed
sample of its own references, SHARDS's method, which built miss-ratio curves "in a bounded 1 MB
footprint" with errors "averaging less than 0.01", and ARC's at a sampling rate of 0.001 with a
mean absolute error of 0.01. The policy whose simulated miss ratio at the size the division gives
is lowest is used, and the choice changes only when the difference exceeds the simulations'
measured error. The published comparisons disagree by workload, S3-FIFO winning on 10 of 14 trace
sets and losing to objects read exactly twice far apart, a plausible agent pattern, and CacheLib
finding that "storage is not Zipfian"; no fixed choice is supported. A policy that needs a lock
on hits is used only where its measured contention costs less than its miss-ratio gain saves. The
sampling hash is keyed per node from the OS's random source, so a client cannot choose keys that
are sampled or never sampled. Whether a PUT's blocks enter the cache as they go down is the same
decision, run with and without (research/31 §3.2).

**Nothing is acknowledged from a staging buffer** (research/31 §3.2, D2). A PUT's window exists so
a body arriving at rate `R` is not stalled by a chain of latency `T`; it never answers a client,
and writing bodies to a gateway's own flash before placement buys no latency, since the answer
waits for the chunks anyway, and adds a write, a read and a copy the client cannot see. Flow
credit may be returned once a byte is in a reserved buffer, since credit promises buffer, not
durability.

## 6. The client library and CLI

mantle's client library and its CLI are part of the system: they speak the native protocol
(node.md §4.1), carry S3's semantics, and hold the half of resilience a server cannot hold for
them. The CLI is the library's command line; agents link the library.

**What the library does on the wire.**

- **One connection per gateway carries all of a client's transfers**, so they share one
  congestion controller and take no more of a bottleneck by opening more flows (research/30
  §4.6). Concurrency inside it serves three purposes and no other: covering a stream's window
  limit (node.md §3.3), covering a block chain's latency, and spreading an upload over gateways
  and disks.
- **It sends against credit and sheds against the admission level** (node.md §4.5). It sends a
  body only against the gateway's credit, so its concurrency follows the server's grant; it holds
  a retry budget, carries the attempt number on every retry, and drops locally what the
  piggybacked admission level says the gateway would refuse. A typed refusal's wait is honoured;
  retries back off from the measured probe timeout with equal jitter (node.md §3.8).
- **It takes the server's plan.** Upload part size, run interval and window come from the gateway
  (§2.1), not from a fixed transfer-manager setting; a stock S3 client chooses its own part
  boundaries and the server can only recommend (research/27 §7.1).
- **It adapts to the path.** Goodput, interruption rate and checkpoint cost are measured per
  upload and the plan changes only by §2.1's rule; receive windows come from the client's own
  memory budget and the path's measured BDP, so a laptop on Wi-Fi holds no more than its window
  (research/30 §4.5–§4.6).
- **It moves with the laptop.** It watches the OS's interface changes, the Network framework's
  path monitor on macOS, rtnetlink address events on Linux and IP interface notifications on
  Windows, and rebinds its socket so the connection migrates (node.md §3.9); where the new
  network blocks UDP it continues over TLS on TCP with the same operations. Keep-alives follow the
  NAT lifetime it measures (node.md §3.8).
- **It restores order for its caller.** A GET's per-block streams are reordered within the window
  the client was granted, or written straight to their positions in a file (§3).
- **Its caches are validated.** A cached object or version row is used only after a conditional
  read carrying the version is answered "unchanged", with no bytes (research/31 §3.3); there is no
  negative cache on a timer. Whether an application holding an object open across a version
  change sees a snapshot of one version or the latest on each read is an API the library states
  per call (research/31 §9).

**The journal** (research/30 §5.3, D16). The library keeps a small log of its own under CLAUDE.md
§6's rules: the platform's full flush, the parent directory flushed after a create or rename, a
checksum on every record verified on read, a mismatch a typed error. It is bounded: one record
set per open operation, removed when the operation is answered or abandoned, and the operations a
client may hold open are bounded by its configuration.

| Record | Written when | Flushed before | Why |
|---|---|---|---|
| Operation: identity, principal, bucket, key, kind, resume horizon | the operation begins | its first byte is sent | without it a restarted client draws a new identity, and §2.1's guarantee is gone |
| Source: path, length, modification time, the OS's file identity (inode and device, or the Windows file ID), each committed run's CRC | the upload is created; each CRC when its progress frame arrives | the create (identity); lazily (CRCs) | a resume must detect a changed source, and the CRCs let the client check a run it doubts |
| Upload: upload ID, checksum algorithms, whether SSE-C applies | the create is answered | the first append | the upload is found again after a restart |
| Progress: durable offset, run number | a progress frame arrives | never required | the server is authoritative for the offset; losing it costs one state query |
| Download: version, ETag, verified prefix, temporary file | the first range is answered | the prefix, after the data it covers is flushed | a resume continues only the same version, from bytes known good |

**After a restart or a power loss.** The library reads its journal and asks the server for each
open operation's state. A committed operation's record answers it; an upload's state gives its
durable offset and runs. The source is checked: same length, same modification time and file
identity, and the CRCs of runs it re-reads where it doubts them. A changed source is a new
operation, never a resume, and the old upload is aborted; the full-object checksum at completion
catches what those checks miss (§2.1). A download writes to a temporary file named in the
journal, verifies each block against its CRC as it arrives, journals the verified prefix only
after the data is flushed, and finishes by flushing the file, renaming it and flushing the
directory; a resume asks for the rest of the same version, which the server refuses rather than
substitute another. Bytes past the durable offset are replayed, at most one run.

**After sleep.** A clock that stops while the machine is suspended says little time has passed
when much has; the server's deadlines run on its ranges' agreed time and never paused. So after a
wake the library treats every session, connection and open run as possibly gone and reconciles
with the server, never trusting a local timer across a suspend (research/30 §5.3).

**By step.** On a laptop the CLI reaches its own node over loopback QUIC, and stock tools reach
the same node through the HTTP/1.1 listener; the journal, operation identity and resumable
uploads are there from the first step, since battery death and roaming are a laptop's faults. At
a node serving remote clients the library's path adaptation, keep-alives from the measured NAT
lifetime and migration do their work. Across cells and regions nothing in the library changes:
routing redirects it (architecture §5), and its shares are the gateways' business.

**Testing.** The library is killed, and its storage cut by a power-loss emulator, at every
journal write and every protocol step; on restart every operation is answered from the server's
record, resumed with no committed run sent again, or restarted under a new identity only where
the server shows the first had no effect (research/30 §9, D16). A changed source between
attempts ends in `BadDigest` or a new operation, never a committed wrong object. A Wi-Fi to
cellular switch emulated with two interfaces mid-upload continues on the same connection, or
resumes from the durable offset with no committed run sent again (research/30 D14).
