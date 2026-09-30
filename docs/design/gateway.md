# The gateway's object path: from a PUT's body to chunks, and back

Status: design, 2026-09-29. Sources: docs/research/01 (Tectonic and Meta's storage, cited as
"01 §x"), 04 (erasure coding and placement), 05 (S3 semantics); docs/design/metadata.md (the
Name, File and Block layers), encryption.md (sealing), durability.md (the scheme a block is
stored in), chunk-store.md (volumes).

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
(research/11 §13.4), is what would replace the cited value.

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
its one empty segment, sealed.

## 2. Writing an object

A PUT writes bottom up, as metadata.md §2 lays out, and its Name commit is its linearization
point:

1. **Identities.** The gateway draws a file ID and a data key from the operating system, and a
   block ID for each block as it begins it. Each is random and 128 or 256 bits, never reused:
   a retry writes a new file (metadata.md §2).
2. **The body streams.** Each 64 KiB of plaintext is sealed as it arrives, the last segment
   marked at the end, and any checksum the request asks for runs over the plaintext
   (`mantle-s3` checksum.rs). The ETag is the MD5 of the plaintext under SSE-S3, and of the
   segments as sealed under SSE-C (encryption.md §4); the plaintext's MD5 runs under SSE-C
   only to check a `Content-MD5` the request sent, a header the PUT is told of when it is
   made. Sealed segments fill the current block.
3. **A full block goes down.** It is coded into its chunks, and each chunk goes to its own
   volume with its CRC-32C, which the storage node checks before it writes (chunk-store.md §4;
   CLAUDE.md §6). The block's chunks are written at once, on distinct volumes (metadata.md §1).
4. **The block is recorded** in the Block range once every chunk is durable, with the file it
   was made for and a handover deadline (metadata.md §2, "Blocks never named"). A write is
   acknowledged only once durable on every copy its scheme stores (CLAUDE.md §6): the gateway
   waits for every chunk, where Tectonic acknowledges at a quorum and repairs the last chunk
   offline (01 §1.8), which needs repair to run.
5. **The file is written** in the File range once every block is recorded, naming the blocks,
   the key wrapped, the object key it is for, and a deadline no later than its blocks'
   earliest.
6. **The version is committed** in the Name range, carrying the file and its deadline.
   Preconditions, versioning and Object Lock are judged there (metadata.md §2).

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
a gateway admits follows from its memory and the latency it measures, which the node sets.

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
(`Miscombined`), and commits (audit §16.5). A completion whose upload is gone asks the Name
range for the version a completion of the same parts made, and gets it or `NoSuchUpload`.

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

**Checked** (`crates/gateway/tests/object_path.rs`): objects under copies and codes read back
whole and in ranges across segment and block boundaries, with requests answered in either
order, and any range of generated objects; a chunk a volume cannot read is read from another
copy or decoded, and a GET with more lost than its scheme tolerates fails naming the block;
bytes that are not the block's fail by a segment's tag, and by the block's CRC-32C once
decoded; an object of a part of 5 MiB and one byte and a part of one byte reads by its parts'
plaintext; and a GET whose caller takes nothing reads as many blocks as its window and no
more, a wider window taking fewer round trips, blocks finished out of order given out in
order.

## 4. Open

- Memory: each PUT holds up to two blocks' bytes and one block's parity, 192 MiB under
  RS(9,6), and while it codes a block the coder's work space, which the coding library sizes
  to the code: 128 MiB to encode 8 MiB chunks under RS(9,6), and a read that decodes one,
  256 MiB (research: the library's `work_count`, measurements/2026-09-29-erasure-copies.md).
  The gateway admits PUTs and reads against the memory it has, as every other queue is
  bounded, and decides whether to keep coders between blocks, which saves 6–10% of coding
  time at 1–8 MiB chunks for memory held between them.
- Hedged writes: Tectonic reserves more storage nodes than a block needs and writes to the
  first that answer (01 §1.8); mantle writes to the volumes placement offers first.
- Small objects: a coded block of a few kilobytes costs as many writes as its code is wide,
  where Tectonic appends small blobs replicated and re-encodes them sealed (01 §1.9).
- Placement across failure domains is the placement driver's (STATUS, planned 2); the path
  takes the volumes it is offered.
