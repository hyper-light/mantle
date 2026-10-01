# 31 — Caching, ordering and integrity: what each layer may keep, in what order bytes move, and what a wrong bit costs

Research note for three questions the owner asked together: what mantle may cache at each
layer without weakening S3's strong consistency, in what order bytes, parts and operations
must move and where keeping that order costs throughput, and how a corrupted bit is found,
contained and repaired from the client's buffer to the platter and back. The three meet at one
place: a cache is a copy, ordering is a property of copies in flight, and corruption is a copy
that differs. Each section ends in recommendations; §7 gathers them into a numbered proposal,
§8 is the test plan, and §9 what remains unknown.

The note does not repeat what earlier notes established. Tectonic's caching of sealed metadata
and its in-memory checksum rule are note 01's; the storage-stack corruption studies
(Bairavasundaram et al., Ganesan et al., Alagappan et al.) are note 03's; leases, ReadIndex
and the Raft dissertation's read rules are note 06's; S3's witness and DynamoDB's
constant-work cache are note 09's; RocksDB's block cache, HyperClockCache and secondary caches
are notes 12 and 23's; the transport's credits, QUIC windows and the datagram plane are note 25
and node.md's. Upload scheduling is note 27's; storage classes and power, device classes, and
resilient transfer are notes 28, 29 and 30, and this note points to them where a question is
theirs.

The transport assumption is the owner's of 2026-09-30, as architecture.md §5 and node.md §4.1
now state it: every hop runs over QUIC, nodes and mantle's client library speak mantle's own
protocol over QUIC (as slates carries its own, note 08), and stock S3 tools reach a cell
through an HTTP/1.1 listener that speaks the S3 wire protocol and is on by default. No other
HTTP version is offered. HTTP/2 appears below only as history.

Compiled 2026-09-30. This is research input, not a decision record.

---

## 0. How to read this note

**Citation tags.**

- Papers: `[KEY §section]`, with the printed page where the text layer carries it
  (`[KEY §section, p. N]`). Section numbers are the paper's own; where a paper's sections are
  unnumbered, the heading is quoted.
- RFCs: `[RFCnnnn §x]`.
- Web pages and source: `[KEY, "heading"]` or `[KEY path:line]`.
- Earlier notes: "note 03 §x", and design records: "gateway.md §x".

**Quotes** are verbatim from the source's text layer, ligatures normalized, hyphenated words
rejoined, "..." for an elision.

**Evidence labels.** As in notes 12 and 23:

- *(no label)*: stated in the cited peer-reviewed source and checked against its text.
- **PREPRINT**: arXiv only (Meta's two silent-data-corruption papers).
- **NON-PEER-REVIEWED**: vendor documentation, blogs, RFC-adjacent web pages, source code,
  Koopman's CRC catalogue pages, slates' own benchmark records. RFCs are standards, labelled
  by their number.
- **DERIVED**: arithmetic or interpretation by this note.
- **UNVERIFIED**: not found in any primary source consulted.
- **INFERENCE / Recommendation**: reasoning for mantle, citing what it rests on.

**Method.** PDFs were fetched from usenix.org, sigops.org, arxiv.org, the authors' and
universities' sites, and converted with `pdftotext`; the passages quoted were read in context.
ACM Digital Library PDFs refuse a non-browser client (HTTP 403), so author copies were used:
their pagination is the proceedings' where the copy prints it, and otherwise the citation gives
the section only. RFC texts came from rfc-editor.org. Koopman's CRC tables were read from his
CMU pages, and the polynomials they list were checked against mantle's own `mantle-crc`
constants by arithmetic (§5.3). AWS's 2019 statement of S3's consistency model was read from
the Internet Archive's capture of the S3 developer guide (`Introduction.html`, June 2019).
slates' transport was read from its source at `/Users/adalundhe/Projects/slates`
(`crates/transport/src`) and its design draft `docs/wip/fleet-transport.md`.

---

## Sources

### Peer-reviewed

| Key | Citation | Obtained |
|---|---|---|
| **S3FIFO** | Juncheng Yang, Yazhuo Zhang, Ziyue Qiu, Yao Yue, K. V. Rashmi. "FIFO Queues are All You Need for Cache Eviction." *SOSP '23*, pp. 130–149. | https://jasony.me/publication/sosp23-s3fifo.pdf |
| **TINYLFU** | Gil Einziger, Roy Friedman, Ben Manes. "TinyLFU: A Highly Efficient Cache Admission Policy." *ACM Trans. Storage* 13(4), 2017. Read from the arXiv version 1512.00727. | arxiv.org |
| **ARC** | Nimrod Megiddo, Dharmendra S. Modha. "ARC: A Self-Tuning, Low Overhead Replacement Cache." *FAST '03*. | usenix.org (legacy) |
| **CACHELIB** | Benjamin Berg et al. "The CacheLib Caching Engine: Design and Experiences at Scale." *OSDI '20*. Same key as note 23. | usenix.org |
| **KANGAROO** | Sara McAllister et al. "Kangaroo: Caching Billions of Tiny Objects on Flash." *SOSP '21*. Same key as note 23. | pdl.cmu.edu |
| **SHARDS** | Carl A. Waldspurger, Nohhyun Park, Alexander Garthwaite, Irfan Ahmad. "Efficient MRC Construction with SHARDS." *FAST '15*, pp. 95–110. | usenix.org |
| **MEMCACHE** | Rajesh Nishtala et al. "Scaling Memcache at Facebook." *NSDI '13*, pp. 385–398. | usenix.org |
| **TAO** | Nathan Bronson et al. "TAO: Facebook's Distributed Data Store for the Social Graph." *USENIX ATC '13*, pp. 49–60. | usenix.org |
| **CHUBBY** | Mike Burrows. "The Chubby lock service for loosely-coupled distributed systems." *OSDI '06*. | research.google.com |
| **QLEASE** | Iulian Moraru, David G. Andersen, Michael Kaminsky. "Paxos Quorum Leases: Fast Reads Without Sacrificing Writes." *SoCC '14*. | cs.cmu.edu |
| **FAN11** | Bin Fan, Hyeontaek Lim, David G. Andersen, Michael Kaminsky. "Small Cache, Big Effect: Provable Load Balancing for Randomly Partitioned Cluster Services." *SoCC '11*. | pdl.cmu.edu |
| **PHOTO** | Qi Huang, Ken Birman, Robbert van Renesse, Wyatt Lloyd, Sanjeev Kumar, Harry C. Li. "An Analysis of Facebook Photo Caching." *SOSP '13*. | cs.cornell.edu |
| **E2E** | J. H. Saltzer, D. P. Reed, D. D. Clark. "End-to-End Arguments in System Design." *ACM TOCS* 2(4), 1984, pp. 277–288. Read from the authors' MIT copy; cited by section heading. | web.mit.edu |
| **KOOP02** | Philip Koopman. "32-Bit Cyclic Redundancy Codes for Internet Applications." *DSN 2002*. | users.ece.cmu.edu |
| **STONE** | Jonathan Stone, Craig Partridge. "When The CRC and TCP Checksum Disagree." *SIGCOMM 2000*. | conferences.sigcomm.org |
| **KRIO08** | Andrew Krioukov, Lakshmi N. Bairavasundaram, Garth R. Goodson, Kiran Srinivasan, Randy Thelen, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. "Parity Lost and Parity Regained." *FAST '08*. | usenix.org |
| **DRAM09** | Bianca Schroeder, Eduardo Pinheiro, Wolf-Dietrich Weber. "DRAM Errors in the Wild: A Large-Scale Field Study." *SIGMETRICS '09*. | cs.toronto.edu |
| **CORES** | Peter H. Hochschild, Paul Turner, Jeffrey C. Mogul, Rama Govindaraju, Parthasarathy Ranganathan, David E. Culler, Amin Vahdat. "Cores that don't count." *HotOS '21*. | sigops.org |
| **ZFS** | Yupu Zhang, Abhishek Rajimwale, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. "End-to-end Data Integrity for File Systems: A ZFS Case Study." *FAST '10*. | usenix.org |
| **TAIL** | Jeffrey Dean, Luiz André Barroso. "The Tail at Scale." *Communications of the ACM* 56(2), 2013, pp. 74–80 (refereed "contributed article"). | author copy |

### Preprints

| Key | Citation | Obtained |
|---|---|---|
| **SDC21** | Harish Dattatraya Dixit, Sneha Pendharkar, Matt Beadon, Chris Mason, Tejasvi Chakravarthy, Bharath Muthiah, Sriram Sankar. "Silent Data Corruptions at Scale." arXiv:2102.11245, 2021. **PREPRINT.** | arxiv.org |
| **SDC22** | Harish Dattatraya Dixit, Laura Boyle, Gautham Vunnam, Sneha Pendharkar, Matt Beadon, Sriram Sankar. "Detecting silent data corruptions in the wild." arXiv:2203.08989, 2022. **PREPRINT.** | arxiv.org |

### Standards

| Key | Title |
|---|---|
| **RFC9000** | QUIC: A UDP-Based Multiplexed and Secure Transport (2021) |
| **RFC9001** | Using TLS to Secure QUIC (2021) |
| **RFC9112** | HTTP/1.1 (2022) |
| **RFC9113** | HTTP/2 (2022), for its deprecation of RFC 7540's priorities only |
| **RFC9218** | Extensible Prioritization Scheme for HTTP (2022), as a design reference only |
| **RFC2308** | Negative Caching of DNS Queries (DNS NCACHE) (1998) |
| **RFC3385** | Internet Protocol Small Computer System Interface (iSCSI) Cyclic Redundancy Check (CRC)/Checksum Considerations (2002) |

### NON-PEER-REVIEWED

| Key | What it is |
|---|---|
| **KOOP-ZOO** | Philip Koopman, "Best CRC Polynomials", the CRC catalogue at users.ece.cmu.edu/~koopman/crc/ (32-bit and 64-bit pages, the page last updated 3/9/2024), fetched 2026-09-30. Data computed by its author; not refereed. |
| **S3-2019** | Amazon S3 Developer Guide, "Introduction to Amazon S3", "Amazon S3 Data Consistency Model", Internet Archive capture of June 2019. |
| **SLATES-SRC** | slates source: `crates/transport/src/streams.rs`, `flow.rs`, `stream.rs`, `session.rs`, `connection.rs`, `reorder.rs`; `docs/wip/fleet-transport.md`; `docs/wip/BENCHMARKS.md` (scheduler grid). |
| **MANTLE-SRC** | mantle's `crates/crc/src/lib.rs` (polynomial constants), `crates/s3/src/checksum.rs`. |

### Cited through earlier notes (their keys and readings)

TEC (Tectonic, note 01 §1.5, §1.13); BGS+08, BGPS07, GAA17, AGL+18, AWK+19 (note 03 §6–§10);
GC89 (Gray and Cheriton, leases), Raft dissertation §6.4, Spanner, CockroachDB (note 06 §A1.5,
§A2, §A7); DDB, VOGELS21, PARIS86, PHY (note 09 §1.8, §3.5, §6.6); DONG21 (note 12 §1.12);
RDB-PR #10626, #11738, #12141 (note 23 §2.2); RFC 8085 and quinn's documentation (note 25 §4,
§6).

### Not read

- Mattson, Gecsei, Slutz, Traiger, "Evaluation techniques for storage hierarchies", *IBM
  Systems Journal* 9(2), 1970. Not freely available; its stack algorithm is described here from
  SHARDS's account of it.
- Jiang and Zhang, "LIRS", *SIGMETRICS '02*. The copy fetched has an unusable text layer; LIRS
  is described from S3FIFO's and TINYLFU's accounts and labelled so.
- Freivalds, "Probabilistic machines can use less running time", *IFIP Congress* 1977, for
  randomized verification of a linear map (§5.6). Not obtained; the use here is DERIVED.
- Sridharan et al., "Memory Errors in Modern Systems", *ASPLOS '15*; and Wang et al.,
  "Understanding Silent Data Corruptions in a Large Production CPU Population", *SOSP '23*. Not
  obtained (ACM 403; no author copy found). The owner's list names "Sridharan"; DRAM09 stands
  for DRAM field data here.
- Waldspurger et al., "Cache Modeling and Optimization using Miniature Simulations", *USENIX
  ATC '17*; Qureshi and Patt, "Utility-Based Cache Partitioning", *MICRO '06*. Not read; the
  scaled-down simulation used below is SHARDS §4.6's, and the allocation rule of §3.5 is
  DERIVED.
- The NVM Express NVM Command Set specification, which defines the 64-bit guard CRC that S3
  calls CRC-64/NVME. The polynomial used below is mantle's, which reproduces S3's published
  test vectors (note 05 §3.4).

---

## 1. Decision-relevant summary

1. **Object bytes in mantle are immutable under identities never reused, so a data cache
   needs no invalidation.** A block ID, file ID and chunk key are random and never reused
   (gateway.md §2, metadata.md §1), and a file's extents never change once written. A cache
   keyed by them can be stale only by holding something nobody can name any more. Every
   consistency obligation therefore sits in one place: the Name-layer read that says which
   version, and so which file, a request means (§3.1). This is Tectonic's sealed-metadata rule
   (note 01 §1.5) applied to bytes.
2. **Nothing may be acknowledged from a staging buffer.** A PUT's bytes held in memory or on
   local flash before every chunk is durable are not durable on every copy the scheme stores
   (CLAUDE.md §6). Staging exists to decouple a client's arrival rate from the block chain's
   latency, sized by Little's law (gateway.md §2, note 27); it never answers a client (§3.2).
3. **The Name read is the only cache that must be linearizable, and it should be validated,
   not trusted or invalidated.** S3 itself kept its caches and added a witness that "acts like
   a read barrier during read operations allowing the cache to learn if its view of an object
   is stale" (note 09 §6.6). TAO's clients send the version they hold and get no data back when
   it is current [TAO §5.3]. mantle's equivalent is the Name range's ReadIndex: a gateway may
   keep a version row but must confirm it at a read index taken after the request arrived
   (§3.6). Invalidation (Chubby [CHUBBY §2.7]) blocks every write on the slowest cache holder
   and needs a per-key holder list, a map keyed by what clients choose (CLAUDE.md §2); leases
   need a clock bound mantle does not yet state (replica.md §7).
4. **Negative caching is exactly what made S3 eventually consistent before 2020.** S3's 2019
   documentation: read-after-write held for new objects "with one caveat ... if you make a HEAD
   or GET request to the key name (to find if the object exists) before creating the object,
   Amazon S3 provides eventual consistency for read-after-write" [S3-2019]. An absence is a
   row like any other and is served only once validated (§3.7).
5. **Chunk and block caches should be chosen by measurement among a small set of policies,
   because the published comparisons disagree by workload.** S3-FIFO has the lowest mean miss
   ratio on 10 of 14 trace datasets and is lock-free [S3FIFO §1, §5.2–§5.3], but its own
   authors name its adversary, objects "accessed only twice" with the second request falling
   out of the small queue [S3FIFO §5.2], and CacheLib reports that "Storage is not Zipfian"
   [CACHELIB §3]. TinyLFU is worse than FIFO on close to half the traces at small sizes
   [S3FIFO §5.2]. The node runs scaled-down simulations of each candidate on a hashed sample
   of its own reference stream, as SHARDS did for ARC at a sampling rate of 0.001 with a mean
   absolute error of 0.01 [SHARDS §4.6], and uses the policy and size the curves choose
   (§3.4–§3.5).
6. **Cache sizes come from miss-ratio curves measured online, not from a fraction of
   memory.** SHARDS builds an LRU miss-ratio curve "in a bounded 1 MB footprint" with errors
   "averaging less than 0.01" [SHARDS abstract]; scaled-down simulation extends it to policies
   that are not stack algorithms [SHARDS §4.6]. Memory is divided among the node's caches
   (engine block cache, Name and File row caches, chunk cache) by equal marginal saving per
   byte on those curves, inside the node's memory budget tree (node.md §2.5) (§3.5).
7. **Swarms on one object are a load-balancing problem a small cache solves provably, and a
   correctness problem only at the Name read.** A front-end cache of O(n log n) entries, n the
   back-end nodes, balances load "regardless of the query distribution" [FAN11 §1]. Immutable
   blocks make that cache free of invalidation. At the Name read, concurrent requests may share
   one ReadIndex round only if the round began after each request arrived (§3.8).
8. **Cache sealed bytes, not plaintext.** A gateway that caches plaintext would serve an SSE-C
   object without the customer's key, which S3 requires on every request (note 20), and would
   serve bytes no tag re-verifies. Caching ciphertext makes every serve an AES-GCM open, which
   verifies the tag at about 8.4 GB/s a core (encryption.md §3), so a cache bit flip is caught
   on serve for free (§3.9).
9. **QUIC removes head-of-line blocking between requests; it does not remove it within one.**
   "When a packet loss occurs, only streams with data in that packet are blocked" [RFC9000
   §13], but each stream is delivered "as an ordered byte stream" [RFC9000 §2.2]. One GET on one
   stream still waits behind its slowest block; mantle bounds that wait by its block window and
   by hedged or degraded reads after a measured quantile (§4.3, §4.8).
10. **The HTTP/1.1 listener is the strictest ordering case and is on by default.** A server
    "MUST send the corresponding responses in the same order that the requests were received"
    [RFC9112 §9.3.2], so one connection carries one response at a time in byte order; stock
    clients get parallelism from connections and ranged GETs. The listener's behaviour is a
    first-class default path and is tested as such (§4.4, §8).
11. **Priorities belong to the native protocol, and the evidence favours few strict classes.**
    RFC 7540's dependency tree "proved to be complex, and it was not uniformly implemented"
    and "was not successful" [RFC9113 §5.3.1]; RFC 9218 replaced it with an urgency of 0–7 and
    an incremental flag [RFC9218 §4.1–§4.2]. slates measured strict priority among three classes
    best for its control tail and rejected deficit round-robin, which "starved control and
    metadata" (NON-PEER-REVIEWED, SLATES-SRC BENCHMARKS). node.md §3.1–§3.3 already has strict
    classes with a floor for repair; within the request class, fairness is note 27's (§4.2).
12. **S3 orders bytes within a GET, parts by number at completion, and operations on one key by
    commit; it orders nothing else.** Parts may arrive in any order and any number of times,
    the last upload of a number replacing earlier ones (note 05 §4.3); the order of an object's
    bytes is in its File row, not in arrival (§4.5–§4.6).
13. **Corruption originates in CPUs and memory as well as disks, and replication copies it.**
    RocksDB corruption measured at Meta, CPU and memory faults included, ran "roughly once every
    three months for each 100PB", and "in 40% of those cases, the corruption had already
    propagated to other replicas" (note 12 §1.12). Meta found silent data corruption at "one in
    thousand silicon devices" [SDC22 §1] (PREPRINT); Google "a few mercurial cores per several
    thousand machines" [CORES §1]; about a third of Google's machines see a correctable DRAM error
    a year and 1.3% an uncorrectable one [DRAM09 §3.1]. A checksum computed after the corruption
    protects the corruption: the gateway's buffers, its sealing and its erasure coding happen
    once, before the fan-out to every copy (§5.2, §5.4).
14. **Every transform is verified by its inverse on a different core before acknowledgement,
    and every checksum is carried, never regenerated.** Tectonic verifies a transform D' = F(D)
    by computing its inverse and comparing checksums, "an acceptable cost" (note 01 §1.13);
    Google saw "a deterministic AES mis-computation, which was 'self-inverting': encrypting and
    decrypting on the same core yielded the identity function, but decryption elsewhere yielded
    gibberish" [CORES §2], so the inverse must run elsewhere. ZFS shows that a checksum made at
    flush time writes in-memory corruption to disk "permanently" [ZFS §5.3, Obs. 3]. mantle
    carries the client's CRC down to the segment, the segment's CRC to the chunk record, and the
    chunk's per-block CRCs to the device (§5.5–§5.6).
15. **CRC-32C keeps Hamming distance 4 over mantle's 64 KiB checksum blocks; CRC-64/NVME's
    distance profile is unpublished.** Koopman's table gives CRC-32C HD=4 up to 2,147,483,615
    bits and HD=6 only to 5,243 bits (KOOP-ZOO), so every 1-, 2- and 3-bit error in a 64 KiB block
    is detected and wider bursts and random errors pass with probability about 2^-32. The 64-bit
    polynomial Koopman lists as "Jones" (0xAD93D23594C935A9) is not CRC-64/NVME's
    (0xAD93D23594C93659, MANTLE-SRC); NVME's distances must be computed before any claim rests
    on them (§5.3).

---

## 2. Ground truth: where mantle holds copies today, and what checks each

| Where | What it holds | How long | Check on use | Source |
|---|---|---|---|---|
| Client's buffer | object bytes | until the response | SDK CRC32 or CRC-64/NVME, sent as header or trailer, by default since Dec 2024 | note 05 §3.6 |
| Wire (native) | requests and bytes | in flight | QUIC packet protection: an AEAD over every packet [RFC9001 §5] | node.md §3 |
| Wire (HTTP/1.1) | requests and bytes | in flight | TLS 1.3 records (AEAD); plain HTTP only on a laptop's loopback, SigV4 still checked | node.md §4.1 |
| Gateway, PUT | the segment being sealed, `window` full blocks and their parity | until every chunk is durable | the request's checksum over plaintext, checked at the end before any Name commit | gateway.md §2, node.md §4.3 |
| Gateway, GET | `window` blocks read or waiting | until the caller takes them | segment tag (AES-256-GCM), the block's CRC-32C when decoded | gateway.md §3, encryption.md §3 |
| Storage node | the chunk being written | until the batch is durable | chunk CRC-32C checked before write; per-64 KiB CRC table, header CRC, identity and incarnation on every read | chunk-store.md §3.1, §4, §7 |
| Chunk index | every record's identity and location | the volume's life | frames CRC-32C, checkpoints, separate copy from data records | chunk-store.md §3.2, §5 |
| Raft log | every range's entries | until compacted | frame CRC-32C; torn tail told from corruption | raft-log.md §2, §6 |
| Engine | rows | the range's life | RocksDB-format block checksums verified on read; per-key-value protection off by default | note 12 §1.12 |
| Engine block cache | uncompressed blocks | until evicted | **none on a hit** | note 23 §2.2 |
| Route caches | cell map, range descriptors | refreshed on a loop | owners refuse stale epochs and generations | architecture.md §4–§5 |
| Bucket row cache | bucket rows | staleness bound open | the Name range's gate refuses a stale incarnation | metadata.md §1, §2 |

**What has no cache yet.** The gateway keeps no Name, File, Block or chunk cache; every GET
reads the version, header, block rows and chunks (gateway.md §3). Storage nodes read with
direct I/O and keep no chunk cache; the page cache is bypassed (chunk-store.md §1). This note
designs what to add and where.

**DERIVED: the gaps the table shows.** Integrity has three holes, each the end-to-end
argument's "transmitted data was unprotected while stored in each gateway" [E2E, "A too-real
example"]:

1. between the client's checksum being verified over the plaintext and the plaintext being
   sealed, the plaintext sits in gateway memory with nothing that would notice a flip: the
   seal would authenticate the flipped bytes;
2. sealing and erasure coding run once, on one core, before the fan-out, so a mis-computation
   there is copied to every chunk and passes every later check except the inverse;
3. an engine block-cache hit returns bytes no checksum re-verifies, which is ZFS's page-cache
   finding [ZFS §5.1.2; §5.3, Obs. 1–2] in mantle's metadata.

§5.4 lists every boundary and closes each.

---

## 3. Caching

### 3.1 The property that makes data caching safe

S3's consistency model requires that "any read (GET or LIST request) that is initiated
following the receipt of a successful PUT response will return the data written by the PUT
request", and that a concurrent reader gets "either the old data or the new data, but never
partial or corrupt data" (note 05 §13). Strong consistency is a property of which version a
read returns, not of the bytes of a version.

mantle names bytes so that the two separate cleanly. A PUT writes a new file with a random
128-bit ID and new blocks with random IDs, never reused, and a retry writes a new file
(gateway.md §2); a completed upload is a file of part files (gateway.md §1); a chunk is keyed
by `(block id, epoch, chunk index)` (chunk-store.md §5). Nothing is overwritten in place. Its
version row names its file; the file's header and extents are written once.

**DERIVED.** Therefore:

- a cache keyed by file ID (File header and extents), block ID (Block header and the block's
  bytes) or chunk key is never stale in content; it can only hold an entry nothing references
  any more, which costs memory, not correctness;
- the Block layer's chunk *locations* do change, by repair and moves under compare-and-swap
  (architecture.md §6); a cached location is a hint, as Tectonic treats it: "A stale Block layer
  cache can be detected during reads, triggering a cache refresh" (note 01 §1.5). A chunk store
  answers a stale location with not-found or with the same bytes, verified by the record's own
  identity (chunk-store.md §3.1), never with another chunk's bytes;
- the version a key maps to, the existence of a key, a listing, and a bucket's configuration
  change; caches of these carry the whole consistency obligation.

One residue is reclamation: a version deleted, its file released, its blocks reclaimed after the
grace period (metadata.md §2; chunk-store.md §8). A GET that resolved the version before the
delete may still be reading. The grace period's floor is "the longest read's deadline plus clock
offset" (metadata.md §6), and a cached block of a reclaimed file is served only to a request
whose own Name read named it, so the cache adds nothing to that window.

### 3.2 Upload-side buffering: what staging may and may not do

CLAUDE.md §6: "A write is acknowledged only once it is durable on every replica the protocol
requires." gateway.md §2 waits for every chunk, where Tectonic acknowledges at a quorum and
repairs the last chunk offline (01 §1.8).

- **What staging is for.** The PUT pipeline holds the segment being sealed and up to `window`
  full blocks going down, `w = ⌈R·T/B⌉` by Little's law, capped by memory (node.md §2.6). That
  buffer exists so a body arriving at rate `R` is not stalled by a chain of latency `T`. A
  buffer larger than `R·T` adds only memory; one smaller stalls the client.
- **Local flash as staging.** Writing an arriving body to the gateway's own flash before
  placement would let the gateway accept bytes faster than the cell takes them. It buys nothing
  for the client's latency, since the acknowledgement still waits for the chunks, and it adds a
  device write and read per byte and a copy whose loss the client cannot see. A single device is
  not durable "on every replica" under any scheme mantle stores beyond one copy. **INFERENCE:**
  no write-back staging. A multipart upload is already the protocol's own resumable staging:
  each part is durable when acknowledged and survives the client's interruption (note 30 owns
  resumption).
- **What staging may acknowledge.** Nothing to the client. Internally, a flow-control credit
  (the native protocol's `MaxStreamData`, TCP's window on the listener) may be returned as soon
  as a byte is in a reserved buffer, because credit is a promise of buffer, not of durability
  (node.md §3.3; SLATES-SRC `flow.rs`).
- **Admission.** Staging memory is a reservation held until the answer is delivered (node.md
  §2.5); how uploads share it is note 27's.
- **Write-allocate.** A PUT's sealed blocks pass through gateway memory once. Whether to insert
  them into the gateway's block cache as they go down (write-allocate) depends on whether agents
  read back what they wrote soon enough to hit. It is safe (immutable IDs) and is decided as any
  other admission is, by the curves of §3.5 run with and without it.

### 3.3 Where read caches go

Facebook's photo stack measured how much each layer of caching shelters: of requests, "65.5%
browser cache, 20.0% Edge Cache, 4.6% Origin Cache, and 9.9% Backend storage" [PHOTO
abstract], the Edge's hit ratio 58.0% and the Origin's 31.8% [PHOTO §4]. The layer nearest the
reader shelters most; each layer below sees a stream already filtered of its most popular
items, so its hit ratio falls. Tectonic puts a flash hot-chunk cache in its storage nodes in
front of disks (note 01 §1.3), and CacheLib's storage-backend cache exists because "some
blocks remain popular enough to exceed the target IOPS of the disks" [CACHELIB §2].

| Layer | What it caches | Keyed by | Consistency | Evidence it pays |
|---|---|---|---|---|
| Client library (native) | object bytes and version rows the client read | `(bucket, key)` → `(version, ETag)`, bytes by file | each use validated by a conditional read carrying the version, answered "unchanged" without bytes | PHOTO's 65.5%; TAO §5.3's version-carrying client cache |
| Gateway | sealed block bytes; File headers and extents; Block rows (as hints); Name rows (validated); bucket rows; routes | file, block, chunk IDs; `(bucket, key)` | §3.6 | FAN11; S3's own caches (note 09 §6.6) |
| Storage node | chunk bytes in DRAM, and on flash in front of disks | chunk key | none needed (immutable) | TEC hot-chunk cache; CACHELIB §2; KANGAROO |
| Range replica | engine blocks (HyperClockCache) | engine block | inside the engine | note 23 §2.2 |

**The HTTP/1.1 listener's clients.** Stock S3 SDKs keep no object cache; a client that wants
one uses `If-None-Match` with the ETag on GET (note 05 §2.4), which the listener answers `304`
from a validated Name read without reading chunks. That conditional GET is the same validation
the native client library does, so the listener needs no separate cache design.

**Storage nodes with direct I/O.** The chunk store bypasses the page cache by design
(chunk-store.md §1). A node-local chunk cache pays only where the device read costs more than a
memory copy and the curve shows reuse at the node, after the gateways' caches have filtered the
stream. On disks it is Tectonic's and CacheLib's case. On NVMe it must be measured: the cached
read saves a device access of tens of microseconds at the measured read depth
(chunk-store.md §7) against memory the engines and gateways also want. The rule is §3.5's, not a
default.

**Flash caches.** Where a node has flash in front of disks, the flash tier's writes are limited
by endurance; CacheLib admits to flash "with a fixed probability p" by default to control the
write rate [CACHELIB §4.2], and Kangaroo's log-plus-set design reduced misses by 29% at a
budget of three device-writes per day and 16 GB of DRAM [KANGAROO Fig. 1]. Mantle's chunks are
up to 8 MiB, not Kangaroo's 100-byte objects, so the large-object design (one object per region,
CacheLib's LOC for items of 2 KB and more [CACHELIB §4.2]) is the relevant one; the admission
rate follows from the device's rated writes, which device classes (note 29) supply.

### 3.4 Admission and eviction

| Policy | Mechanism | Concurrency | Evidence for | Evidence against |
|---|---|---|---|---|
| LRU | recency list | a lock on every hit to move the item | baseline | "each cache hit requires promoting the requested object to the head of the queue guarded by locking" [S3FIFO §1] |
| CLOCK / HyperClockCache | a reference bit or counter per slot, a sweeping hand | one atomic per lookup in HCC (note 23 §2.2) | HCC 5.2× LRU's throughput with the database cached at 48 threads (NON-PEER-REVIEWED, note 23 §2.2) | LRU beat AutoHCC at about 50% hit rate and 10 threads (note 23 §2.2) |
| ARC | two LRU lists and two ghost lists, adapting their split | LRU lists, so LRU's locking | "scan-resistant: it allows one-time sequential requests to pass through without polluting the cache" [ARC abstract] | worse than S3-FIFO "most of the time" [S3FIFO §6.3] |
| LIRS | inter-reference recency, a 1% queue for new objects | complex | "the highest efficiency" on some traces [S3FIFO §1] (not read; §Sources) | "requires a more complex implementation" [S3FIFO §5.2] |
| W-TinyLFU | a 1% LRU window, then a count-min sketch admits a candidate only if more frequent than the victim; counters halved every W additions [TINYLFU, "reset" and W-TinyLFU] | "LRU-based eviction algorithms, such as LRU, 2Q, and TinyLFU, require locking on both cache hits and cache misses" [S3FIFO §5.3] | "the only scheme to obtain such good results on all traces" of its paper [TINYLFU abstract] | worse than FIFO on close to 50% of traces at the small cache size [S3FIFO §5.2] |
| S3-FIFO | a small FIFO (10%), a main FIFO (90%) with two-bit counters and reinsertion, a ghost FIFO | "only FIFO queues without locking on either read or write" [S3FIFO §5.3] | lowest mean miss ratio on 10 of 14 datasets, 6,594 traces; more than 6× LRU's throughput at 16 threads [S3FIFO §1, §5] | objects accessed twice with the second access beyond the small queue [S3FIFO §5.2] |

The finding the policies share is quick demotion: in a cache of size C, "most objects will be
one-hit wonders (no request after insertion) when evicted"; the median one-hit-wonder ratio
rises from 26% over a whole trace to 72% over sequences holding 10% of its objects [S3FIFO §1].
A new object should prove itself in a small space before it takes a large one. S3-FIFO's
small queue, W-TinyLFU's window and LIRS's 1% queue all do this.

Two properties of mantle's streams bear on the choice:

- **Chunk reads are sequential within an object and reused across requests.** A GET reads an
  object's blocks in order (gateway.md §3); a scan of a large object is ARC's "one-time
  sequential requests". A policy that admits every block of a scan into its main space pollutes
  it. S3-FIFO's small queue and W-TinyLFU's window both keep scans out of the main space.
- **The workload is unknown and will shift.** Agent workloads are not CDN or social-graph
  workloads, and CacheLib's own storage-backend trace is "not Zipfian" [CACHELIB §3]. S3-FIFO's
  adversary, objects read exactly twice far apart, is a plausible agent pattern: write a
  checkpoint, read it once at restart.

**Recommendation.** One cache implementation per layer with a pluggable eviction policy, and a
default chosen per node by measurement: the node runs scaled-down simulations of FIFO, CLOCK,
S3-FIFO, W-TinyLFU and ARC on a spatially hashed sample of its own references [SHARDS §2, §4.6],
and selects the policy whose simulated miss ratio at the size §3.5 gives is lowest, with
hysteresis so the choice changes only when the difference exceeds the simulations' measured
error. The live cache must be lock-free on hits, which CLOCK and S3-FIFO are and LRU and ARC are
not; a policy that wins the simulation but needs a lock is used only where the measured hit
rate's contention, as note 23 §2.2 measured for LRU against HCC, does not cost more than its
miss-ratio gain saves.

### 3.5 Sizing from miss-ratio curves

Mattson et al.'s insight, as SHARDS states it: "many replacement policies have an inclusion
property: given a cache C of size z, C(z) ⊆ C(z + 1). Such policies, referred to as stack
algorithms, include LRU, LFU, and MRU", and so one pass over a trace gives the miss ratio at
every size [SHARDS §5]. The exact algorithm takes "O(NM) time and O(M) space" [SHARDS §5], too
much online.

SHARDS samples references whose location hashes below a threshold, `hash(L) mod P < T`, so a
sampled location's every reference is kept, and scales reuse distances by the sampling rate. "R
= 0.001 yields very accurate MRCs" for typical workloads [SHARDS §2.2]; a fixed-size variant
lowers T adaptively and runs in constant space: "MRCs constructed in a bounded 1 MB footprint,
with effective sampling rates significantly lower than 1%, exhibit approximate miss ratio errors
averaging less than 0.01" [SHARDS abstract]. For policies that are not stack algorithms, "A series
of separate simulations is run, each using a different cache size, which is also scaled down by
R"; for ARC at R = 0.001 "the simulated cache is only 0.1% of the desired cache size ... with an
MAE of 0.01" [SHARDS §4.6].

**Recommendation (DERIVED).**

1. Every cache in the node (engine block cache, Name and File row caches, block or chunk cache,
   the client library's) samples its references by spatial hash and keeps an LRU curve
   (fixed-size SHARDS) and scaled-down simulations of the candidate policies, each in a bounded
   footprint stated in the node's memory budget.
2. Sampling hashes keyed per node from the operating system's random source, so a client cannot
   choose keys that are sampled or never sampled.
3. Memory for caches is the node's budget less what admitted work reserves (node.md §1.4,
   §2.5). It is divided among the caches by equal marginal saving: each cache's curve times the
   cost of one of its misses (a device read, a range read, a QUIC round trip, measured) gives
   the saving per byte at each size, and the division moves memory to the cache whose next
   byte saves most until no move gains more than the curves' measured error. Miss-ratio curves
   need not be convex; the division works on each curve's convex hull, which a cache attains by
   splitting its space in proportion (DERIVED; the hull rule is standard and its source here is
   not read, §Sources).
4. A cache whose curve is flat at its smallest size gets none: the laptop's chunk cache may well
   be zero, and that is the measurement's answer, not a default.

### 3.6 Metadata caches and linearizable reads

**Routes and descriptors.** Already designed: "stale routing costs liveness, never
correctness" (architecture.md §5), and routers receive the full cell map "on a fixed loop rather
than on change", DynamoDB's constant-work fix for a cache whose 99.75% hit rate made metadata
load bimodal (note 09 §3.5).

**Bucket rows.** Bucket configuration is eventually consistent in S3: a versioning change "may
take up to 15 minutes" (note 05 §13). A gateway's bucket cache keeps rows a bounded time;
correctness of writes is the gate's (metadata.md §2). The bound is open (metadata.md §6). This
note adds only that refresh should be constant-work, by the same loop, so the Bucket range's load
does not depend on the caches' hit rate.

**File and Block rows.** File rows never change; Block rows change only in their chunk
locations. Both are cached at gateways without validation (§3.1).

**Name rows: the three ways to serve a cached read linearizably.**

| Way | How | Cost per read | Cost per write | Clock assumption | Source |
|---|---|---|---|---|---|
| Invalidation | the owner tracks which caches hold a key and blocks a write until each acknowledges an invalidation or its cache lease lapses | none while valid | one round to every holder, waiting on the slowest | the cache lease's bound | CHUBBY §2.7: "The modification proceeds only after the server knows that each client has invalidated its cache, either because the client acknowledged the invalidation, or because the client allowed its cache lease to expire" |
| Leases | the leader, or a quorum of replicas, serves reads locally for a lease term after a quorum round | a local read | none; leases expire before a leader change | drift bound, "start + election timeout / clock drift bound" | GC89 (note 06 §A7): "a contract that gives its holder specified rights over property for a limited period of time", whose "correct functioning ... requires only that clocks have a known bounded drift"; note 06 §A1.5; QLEASE abstract: "a majority of replicas to perform strongly consistent local reads" |
| Validation | the reader sends the version it holds; the owner confirms at a linearizable point and returns no row when unchanged | one ReadIndex round (batched) and one engine read at the leader | none | none | S3's witness (note 09 §6.6); TAO §5.3: "By including the version number in subsequent queries, the follower can omit the data in replies if the data has not changed since the previous version" |

**Why validation for mantle (INFERENCE).**

- Invalidation needs, per key, the set of gateways that may hold it. Under billions of agent
  principals and machine-speed bursts that set is a map keyed by values clients choose, which
  CLAUDE.md §2 requires to be bounded and pruned; and every write waits on the slowest holder.
  Chubby makes a node "uncachable while cache invalidations remain unacknowledged" [CHUBBY
  §2.7], which turns a slow gateway into a slow writer.
- Leases are kept out of replica.md until a deployment states a clock-drift bound (replica.md
  §7; note 06 §A1.5), and a lease read with a violated bound "could return arbitrarily stale
  information" (note 06 §A1.5).
- Validation needs no clock and no holder list, and its per-read cost is what a GET pays today
  without a cache: a ReadIndex round, which "batch[es] many reads per heartbeat round" (note 06, "Implications for mantle (A1)"). What it saves is everything after: the row's bytes, and the File and Block reads
  the gateway now takes from its own cache. S3 chose the same shape: keep the cache, put a
  read barrier in front of it (note 09 §6.6).

**The validation rule, stated exactly.** A gateway holding `(bucket, key) → (version order,
file, commit index i)` serves a GET from it only after the Name range's leader answers, at a
read index `r` obtained by a ReadIndex round that began after the GET arrived, that the key's
first row is still that version. The leader answers from its engine at `r`, which is one point
read (a block-cache hit when the key is hot). A delete marker, a newer version, or a missing
row returns the current row. Conditional reads (`If-Match`, `If-None-Match`,
`If-Modified-Since`) are judged against the validated row, never against the cache alone.

**Leases, when a deployment states a clock bound.** The leader's read lease of the Raft
dissertation lets the leader answer validations without the quorum round; quorum leases
[QLEASE] let a majority of replicas answer them locally, which matters for regional reads
(§6). Either is an optimization of the validation's cost, not a change to the rule above, and
each enters with the bound it depends on, measured as note 06 §A1.5 requires.

**LIST.** A listing is a scan of the Name range at a read index (metadata.md §2). Caching pages
would need the range to answer "nothing in `[a, b)` changed since index i", which it can only do
by tracking the last write index per span. **INFERENCE:** no list cache until a measurement shows
listings of unchanged spans dominate some workload; then a per-range "last write index" (one
number, one span) validates a cached page of a range nobody wrote, which is cheap to keep and
exact.

### 3.7 Negative caching

**The hazard, in S3's own history.** Before 2020 S3 guaranteed read-after-write for new objects
"with one caveat. The caveat is that if you make a HEAD or GET request to the key name (to find
if the object exists) before creating the object, Amazon S3 provides eventual consistency for
read-after-write" [S3-2019]. A negative answer had been cached. Since December 2020 the caveat is
gone (note 05 §13), and mantle must not reintroduce it.

**Its value.** DNS made negative caching mandatory because it "reduces the response time for
negative answers" and the messages sent [RFC2308 §1]. In Facebook's social graph "55.6% of
requests are for keys that do not exist", and CacheLib supports negative caching in compact
fixed-size entries; 4 of its 10 largest deployments use it, its storage cache does not
[CACHELIB §3.5, §4.3]. Agents probing for a key before writing it (`If-None-Match: *` is the
correct way, note 05 §2.2) will make absence a common answer.

**Rule (INFERENCE).** An absence is a row: `(bucket, key) → none, as of index i`. It is cached and
validated exactly as §3.6 validates a version, and never served on its own. What negative
caching saves under strong consistency is the same as positive caching: the bytes and any reads
after the Name read, not the validation. A TTL-only negative cache in the RFC 2308 manner is
forbidden at every layer, including the client library. TAO's high-degree objects show the
other hazard: an absence is cheap to prove only where the cache holds the whole set; TAO's
"empty result" queries "will always go to the database" when the queried edge "could be in the
uncached tail" [TAO §5.4]. A mantle LIST cache (§3.6) is the only place that applies.

### 3.8 Hot objects and swarms

**Many readers of one object.** The bytes are immutable, so the question is load.

- *Coalescing (single-flight).* Concurrent GETs of the same block at one gateway wait on one
  fetch, keyed by block ID. Safe without condition, since the block's content cannot change.
- *Coalescing the Name read, with the one condition that keeps it linearizable.* Concurrent GETs
  of the same key may share a validation only if the ReadIndex round they share began after each
  of them arrived. A request that joins a round already in flight could miss a write that
  completed between the round's start and the request's arrival, and the S3 guarantee is
  exactly about reads "initiated following the receipt of a successful PUT response" (note 05
  §13). So a joining request waits for the next round, and rounds are taken back to back while
  requests wait: one quorum round a heartbeat serves every reader of every key at that leader.
  This is ReadIndex's own batching made explicit for the swarm.
- *memcache's leases* solve the herd that follows an invalidation in a look-aside cache: a
  server "regulates the rate at which it returns tokens", once every 10 seconds per key by
  default, and other clients wait briefly; a week of herd-prone keys peaked at 17K database
  queries/s without leases and 1.3K/s with them [MEMCACHE §3.2.1]. The same lease token also
  stops stale sets, a fill racing a delete [MEMCACHE §3.2.1]. **DERIVED:** mantle's caches are
  filled by the reader that missed, from a linearizable read, keyed by immutable IDs, so a stale
  set cannot occur for bytes; for Name rows the validated read is the fill. The herd is the
  coalescing above; the 10-second token rate is memcache's choice and has no role here.
- *How large a hot-item cache must be.* Fan et al. prove that a front-end cache holding O(n log n)
  entries, n the number of back-end nodes, balances load whatever the distribution, "a key-value
  storage system with 100 nodes using 1 KiB entries can be serviced using 4 megabytes" [FAN11 §1].
  For mantle the back ends are storage volumes and the entries are blocks of up to 72 MiB; the
  bound counts entries, so the hottest O(n log n) blocks across a cell's gateways suffice to stop
  any one volume from being hot. **DERIVED** sizing: the entry count from the cell's volume count,
  each entry the sampled hot block's size, checked against §3.5's curves.
- *Replicating hot items.* memcache replicates a key set "when (1) the application routinely
  fetches many keys simultaneously, (2) the entire data set fits in one or two memcached servers
  and (3) the request rate is much higher than what a single server can manage", at the price
  that "This approach requires delivering invalidations to all replicas" [MEMCACHE §3.2.3]; TAO
  clones hot shards across followers and caches the hottest items in clients with their version
  [TAO §5.3]. For mantle's blocks no invalidation is needed, so hot blocks can be held at every
  gateway serving the swarm; the gateways are shuffle-sharded per tenant (architecture.md §8), so
  the replication is bounded by the tenant's shard.
- *Hedging.* Dean and Barroso defer a second request "until the first request has been
  outstanding for more than the 95th-percentile expected latency", which "limits the additional
  load to approximately 5% while substantially shortening the latency tail"; reading 1,000 keys
  from 100 servers, a hedge after 10 ms cut the 99.9th percentile from 1,800 ms to 74 ms for 2%
  more requests [TAIL, "Hedged requests"]. **DERIVED:** the quantile is not a constant: hedging
  after the q-quantile adds about (1 − q) of reads, so q follows from the read amplification the
  node's devices can absorb. A coded block's hedge is a degraded read costing `data` chunk reads,
  and Tectonic restricts reconstructed reads "to 10% of all reads" against reconstruction storms
  (note 01 §1.13); mantle sets its cap from measured headroom, with Tectonic's figure as the cited
  upper reference until measured.

**Many writers of one key.** S3 gives last-writer-wins and compare-and-swap on one key (note 05
§2.2, §13). All writes to a key go through one range and serialize there. The range's admission
and `503 SlowDown` (architecture.md §8) are the bound; per-tenant and per-key fairness under a
swarm of writers is note 27's.

**Many listers of one prefix.** Concurrent identical LIST pages at a gateway may coalesce on the
same rule as the Name read: share a scan only if it began after each request arrived.

### 3.9 Caches and server-side encryption

- **Cache ciphertext.** The gateway opens segments after decoding (encryption.md §3). A block
  cache of sealed bytes leaves the key check in the request path: an SSE-C GET supplies its key
  on every request (note 20), and the open fails under a wrong key. A plaintext cache would answer
  an SSE-C request without the key ever being presented.
- **Every serve verifies.** An AES-256-GCM open checks the tag; at about 8.4 GB/s a core
  (encryption.md §3) a cached segment's integrity is checked at every serve for less than a
  memory copy's multiple. ZFS's finding that a page-cache hit is returned "without verifying the
  checksum" and that the "window of vulnerability of blocks in the page cache is unbounded" [ZFS
  §5.1.2; §5.3, Obs. 1–2] does not apply to a sealed cache.
- **Unencrypted objects.** If a deployment stores some objects unsealed, their cached blocks carry
  their per-64 KiB CRC-32C tables and are verified on serve the same way (§5.8).
- **Wrapped keys.** A cached File header holds the data key wrapped (gateway.md §1). Unwrapped
  keys are not cached across requests; unwrapping costs 1.8 µs (encryption.md §3), less than any
  saving.

---

## 4. Stream ordering

### 4.1 What S3 requires to be ordered

| What | Requirement | Where mantle keeps it |
|---|---|---|
| Bytes of a GET | in object order; a range "bytes=a-b" returns exactly those bytes in order (note 05 §12.1) | the GET driver gives out blocks in order whichever finishes first (gateway.md §3) |
| Bytes of a PUT body | arrive in order on one request; sealed segment by segment | the PUT driver; the File row's extents |
| Parts of a multipart upload | any arrival order, any concurrency; "If you upload a new part using the same part number ... the previously uploaded part is overwritten"; completion concatenates "in ascending order by part number"; a list out of order is `InvalidPartOrder` (note 05 §4.3–§4.4) | the Name range's part rows by number; the completion walks both lists ascending (gateway.md §2) |
| Operations on one key | linearizable: a read after an acknowledged write sees it; concurrent PUTs, "the request with the latest timestamp wins"; conditional writes "the first write operation to finish succeeds" (note 05 §2.2, §13) | the Name range's log order; versions ordered by commit time, a completed upload by initiation time (metadata.md §1) |
| Operations on different keys | none: "There is no way to make atomic updates across keys" (note 05 §13) | none needed; each range linearizes its own keys, and per-key linearizability composes (note 06 §A6) |

Nothing else is ordered: not a client's requests to different keys, not chunks within a block on
the wire, not blocks of a PUT in their journey to volumes, not parts of an upload.

### 4.2 Transport: QUIC streams, priorities and their history

**QUIC.** "Endpoints MUST be able to deliver stream data to an application as an ordered byte
stream. Delivering an ordered byte stream requires that an endpoint buffer any data that is
received out of order, up to the advertised flow control limit" [RFC9000 §2.2]. "QUIC makes no
specific allowances for delivery of stream data out of order. However, implementations MAY choose
to offer the ability to deliver data out of order to a receiving application" [RFC9000 §2.2].
Across streams: "One of the benefits of QUIC is avoidance of head-of-line blocking across
multiple streams. When a packet loss occurs, only streams with data in that packet are blocked
waiting for a retransmission to be received, while other streams can continue making progress.
Note that when data from multiple streams is included in a single QUIC packet, loss of that packet
blocks all those streams from making progress" [RFC9000 §13]. Data at an offset is immutable on
the wire: "The data at a given offset MUST NOT change if it is sent multiple times" [RFC9000
§2.2]. TCP, under the HTTP/1.1 listener, has one byte stream per connection, so a lost segment
holds back everything after it.

**Priorities.** "QUIC does not provide a mechanism for exchanging prioritization information.
Instead, it relies on receiving priority information from the application" [RFC9000 §2.3]. The
history argues for a simple scheme:

- RFC 7540 gave HTTP/2 a dependency tree with weights. RFC 9113 records that it "proved to be
  complex, and it was not uniformly implemented ... Many server deployments ignored client signals
  ... In short, the prioritization signaling in RFC 7540 was not successful", and "This revision of
  HTTP/2 deprecates the priority signaling scheme from RFC 7540" [RFC9113 §1, §5.3.1]. (History
  only; mantle offers no HTTP/2.)
- RFC 9218 replaced it with two parameters: urgency, "between 0 and 7 inclusive, in descending
  order of priority. The default is 3", and incremental, whether a response is useful in pieces
  and so may share bandwidth with others of its urgency [RFC9218 §4.1–§4.2]. It is defined for
  HTTP; its model, a handful of strict levels and within a level either one-at-a-time or
  round-robin, is a reference for mantle's own protocol, not a dependency.
- slates (NON-PEER-REVIEWED, SLATES-SRC): three classes, Control, Metadata and Bulk, carried in
  the stream ID's two class bits; "the sender fills each packet from the most urgent class that
  has data"; over a 13-scenario grid strict priority was selected (control p99 within 1.023× of
  the best, geometric mean), round-robin rejected because "the control tail was 4.8× worse", and
  deficit round-robin rejected because it "starved control and metadata". Connection credit
  carries a reserve per class above, so "a bulk transfer never spends the credit a control
  exchange needs" (`flow.rs`; a 2026-09-30 bug record measured a control ping waiting 68 ms of a
  40 ms path before the reserve).

node.md §3.1–§3.3 already orders control above replication above request above bulk, with
quinn's priorities ("Higher priority streams always take precedence over lower priority
streams", note 25 §4) and a repair floor so strict priority never starves repair. **INFERENCE:**
the client protocol needs only two levels inside the request class: the small exchanges whose
latency a caller waits on (Name reads, validations, HEAD, small PUTs and GETs, completions) above
the bulk of large bodies, the incremental case, shared round-robin within the level and fairly
across tenants by note 27's scheduler. A client cannot raise its own class: the class is decided
by what a message is (node.md §3.1).

### 4.3 The GET on the native protocol: where ordering is restored

gateway.md §3 holds at most `window` blocks for a GET and "gives out blocks in order whichever
finishes first". The reorder buffer is the window: a block finished early waits for the ones
before it. The cost is head-of-line blocking at the gateway: one slow block (a busy volume, a
degraded decode) holds every later block that finished, and the client's stream idles.

Two places can restore order:

1. **At the gateway, one stream.** The gateway emits in order on the request's one stream; the
   client reads an ordered stream. The reorder buffer is the gateway's window. A lost packet on
   the stream also blocks the bytes behind it [RFC9000 §2.2]. This is the only form the HTTP/1.1
   listener can have (§4.4).
2. **At the client, a stream per block.** The gateway opens one stream per block in flight, each
   carrying the block's opened bytes with its offset and its checksum, and the client library
   places them; the client's reorder buffer is the window. A slow block holds only its own
   stream; blocks behind it are delivered and held at the client, not the gateway, and a packet
   lost on one block's stream blocks only that block [RFC9000 §13]. The client may also hand
   blocks to its application out of order when the application writes them to positions (a
   download to a file), which removes the reorder buffer.

**DERIVED.** Both bound the same memory, `window` blocks, on one side or the other; they differ in
which side holds it and in loss blocking. With per-block streams, a gateway's memory per slow
client drops to the blocks in flight, and a client that wants order pays its own window, which is
its memory to give. Stream credit bounds both (node.md §3.3), so a stalled client cannot make the
gateway buffer more: the stream's `MaxStreamData` stays put, and the GET reads no more blocks
until it moves (gateway.md §3: a GET "whose caller takes nothing reads as many blocks as its
window and no more"). slates' receiver already delivers "contiguously, in order, once each"
under "a bounded receive window" with an offer past it "a typed refusal, never unbounded growth"
(SLATES-SRC `stream.rs`).

**Recommendation.** The native GET carries a control stream (headers, the version, the checksum
to expect, errors) and one stream per block, each block stream framed as `(offset, length,
CRC-64/NVME of the plaintext)` so the client verifies each block it receives (§5.4) and can place
it. The client library restores order by default within the window it was granted; an API that
writes to positions takes blocks as they come. The stream count per GET is the window, which the
node already derives (node.md §2.6).

### 4.4 The HTTP/1.1 listener: one ordered response at a time

HTTP/1.1 carries one response at a time per connection; pipelined requests are answered in order
("it MUST send the corresponding responses in the same order that the requests were received",
[RFC9112 §9.3.2]). The listener is on by default (architecture.md §5), so its behaviour is a
default path, not a fallback:

- **Within a response,** bytes go out in order from the GET driver's window, as §4.3's first form.
- **Head-of-line within a response** is bounded by the window and by hedging: a block outstanding
  past the measured quantile (§3.8) is read from another copy or decoded, so the response stalls
  at most until the hedge returns.
- **Across requests,** stock clients parallelize with connections: the AWS CLI and SDK transfer
  managers split large GETs into ranged GETs on separate connections and large PUTs into parts
  (note 30 owns their behaviour under interruption). Each connection is admitted against the
  connection budget (node.md §4.2).
- **Pipelining.** A server "MAY process a sequence of pipelined requests in parallel if they all
  have safe methods" [RFC9112 §9.3.2]. **INFERENCE:** the listener serves pipelined requests one at
  a time; processing ahead would hold responses for requests whose client may never read them,
  against the rule that work holds its reservation until delivered (node.md §2.5).
- **The response checksum** travels in headers before the body (`x-amz-checksum-*` with
  `x-amz-checksum-mode: ENABLED`, note 05 §3.5), so the gateway must know it before the first byte:
  it is the object's stored value, never computed while streaming. A ranged GET that does not align
  to a part carries no checksum header (note 05 §3.5), so the native protocol's per-block checksums
  (§4.3) have no listener equivalent; the listener's end-to-end check of a ranged read is the TLS
  record's AEAD and the client's own retry.

### 4.5 PUT and multipart: arrival order versus object order

- **A single PUT.** The body arrives in order on its stream. Its blocks go down out of order
  within the window and are recorded in the Block range in any order; the File row lists them by
  offset (gateway.md §2). Order lives in the File row; arrival is free.
- **Parts.** Parts arrive in any order, concurrently, from any number of connections, and any
  part number may be uploaded again, replacing the earlier part (note 05 §4.3). The Name range
  holds part rows by number; the completion walks the listed parts and the rows ascending
  (gateway.md §2), and a list out of order is `InvalidPartOrder` (note 05 §4.4).
- **A part replaced while the completion runs.** S3 does not say which upload of a number a
  completion takes when one races it. The completion checks each listed part's ETag against its
  row (gateway.md §2), so it takes the part the client named by ETag, or refuses. That is the
  strictest ordering available and needs nothing more.
- **Composite checksums** need parts numbered from 1 without gaps, and the full-object CRC is
  combined from each part's value and length in part order (gateway.md §2; note 05 §3.3–§3.4):
  ordering is applied to checksums, not bytes.

### 4.6 Ordering of operations on one key

A PUT's linearization point is its Name commit (gateway.md §2). Two concurrent PUTs to one key
both commit, in the range's log order, and the later commit is the current version: that is
S3's "latest timestamp wins" with the commit as the timestamp, which is the only timestamp all
replicas agree on. The exception S3 states is kept: a completed multipart upload takes its
initiation time for version order (metadata.md §1; note 05 §4.4). Conditional writes are judged
at commit, atomically, against the current row (note 05 §2.2 "Implication"). A cache never takes
part in ordering: a validated read returns the row at its read index, which is after every
acknowledged write.

### 4.7 The Raft log and sessions

A range's log totally orders its commands. Within a gateway's session, commands carry serials
but "Commands reach the log in any order, a gateway's retries and the fast track's re-proposals
among them" (replica.md §1); serials give exactly-once, not FIFO. That suffices for S3, which
orders only requests whose invoker waited for the earlier response (note 05 §13). A client that
pipelines two PUTs to one key without waiting for the first has asked for no order and gets the
log's. **INFERENCE:** neither the native protocol nor the listener should promise per-connection
FIFO across requests; promising it would oblige the gateway to serialize independent requests,
adding head-of-line blocking that S3 does not require.

### 4.8 Where ordering costs throughput, and the bounds

| Where | Cost | Bound |
|---|---|---|
| GET reorder buffer | finished blocks wait for a slow one | `window` blocks of memory; the wait by the hedge quantile (§3.8) |
| Single QUIC stream | a lost packet holds later bytes of that stream | per-block streams on the native protocol (§4.3) |
| HTTP/1.1 connection | one response at a time; a lost TCP segment holds the rest | client parallelism by connections; the hedge |
| Name range | one key's writes serialize | per-key and per-tenant admission (note 27), `503 SlowDown` |
| Completion | parts walked in order | one pass each over the listed parts and their rows (gateway.md §2) |
| Log | every command of a range in one order | batching and group commit (replica.md §1; raft-log.md §3) |

**DERIVED:** the reorder buffer a GET needs is the window that sustains its rate, `w = ⌈R·T/B⌉`
(node.md §2.6); a larger one only helps when block latencies vary, by the spread between the
median and the hedge quantile. The window should therefore be derived from the measured latency
distribution's quantile at the hedge point, not its mean, so that a block at the hedge point does
not starve the client: `w = ⌈R·T_q/B⌉` with `T_q` the chain latency at the hedge quantile.

---

## 5. Corrupt data, end to end

### 5.1 The end-to-end argument

"The function in question can completely and correctly be implemented only with the knowledge
and help of the application standing at the end points of the communication system ...
(Sometimes an incomplete version of the function provided by the communication system may be
useful as a performance enhancement.)" [E2E, Introduction]. The paper's own example is a gateway: a network
"used a packet checksum on each hop", and "the transmitted data was unprotected while stored in
each gateway. One gateway computer developed a transient error in which while copying data from
an input to an output buffer a byte pair was interchanged, with a frequency of about one such
interchange in every million bytes passed" [E2E, "A too-real example"].

For mantle the ends are the client and the bytes on the device, and in between every hop's check
is the "incomplete version". The S3 checksum the client computes is the end-to-end check on
writes and, when the client asks for it on reads (note 05 §3.5–§3.6), on reads; every internal
CRC and tag is a performance enhancement that localizes the fault so it can be repaired before
the client sees it.

### 5.2 Where corruption comes from

| Source | Evidence | What it does | Detected by |
|---|---|---|---|
| Disk media | latent sector errors cluster within 10 MB and in time (note 03 §10, BGPS07) | unreadable or wrong sectors | read errors, record CRCs, scrubbing |
| Disk and controller firmware | 0.86% of nearline disks had checksum mismatches over 41 months; lost or misdirected writes "in a total 365 disks out of the 1.53 million" (note 03 §10.2) | right-looking wrong data; writes reported done and not done; writes landing elsewhere | identity in the record and a separate copy of it (chunk-store.md §3) |
| RAID-style reconstruction | "parity pollution ... corrupt data in one block of a stripe spreads to other blocks through various parity calculations", and "data scrubbing ... tends to be one of the main causes" [KRIO08 §1] | repair writes corruption into new chunks | verifying every source before it is used (§5.8) |
| DRAM | "About a third of all machines in the fleet experience at least one memory error per year"; "1.3% of machines are affected by uncorrectable errors per year"; "more than 8% of DIMMs affected by errors per year"; errors "dominated by hard errors"; a correctable error raises the chance of an uncorrectable one "by factors between 9–400" [DRAM09 abstract, §3.1, Conclusions] | flips in buffers, caches, indexes | ECC (corrects most); checksums over buffers; caches verified on serve |
| CPUs | "the SDC occurrence rate of one in thousand silicon devices" [SDC22 §1] (PREPRINT); "a few mercurial cores per several thousand machines" [CORES §1]; a Scala `pow` returned 0 for a file of known size on a defective CPU, so files went missing from a database [SDC21 §4] (PREPRINT); "These errors do not leave any record or trace in system logs" [SDC22 abstract] | wrong results with no error: wrong checksums, wrong ciphertext, wrong parity, lock violations, corrupted indexes, "Corruption affecting garbage collection, in a storage system, causing live data to be lost" [CORES §2] | redundant computation on another core; inverse transforms; screening |
| Network and NICs | "between 1 packet in 1,100 and 1 packet in 32,000 fails the TCP checksum", and "the checksum will fail to detect errors for roughly 1 in 16 million to 10 billion packets"; errors are "highly non-random", so "some applications should employ application-level checksums" [STONE abstract]; at Meta a storage bug produced "roughly 17 checksum mismatches for every petabyte of physical data transferred" (note 12 §1.12) | wrong bytes past the transport's check | QUIC and TLS AEAD on the wire; payload CRCs inside |
| Software | RocksDB-level corruption, CPU and memory faults included, "roughly once every three months for each 100PB", "in 40% of those cases, the corruption had already propagated to other replicas" (note 12 §1.12); protocols that spread damage (note 03, GAA17-F7) | corruption copied by replication or repair | replica comparison; protocol-aware recovery (AGL+18) |

**DERIVED: why the CPU and memory row matters most for mantle's write path.** A PUT's bytes are
checked by the client's checksum, then sealed, then coded into `data + parity` chunks, each sent
with its CRC to a separate volume. Every chunk derives from one sealing and one coding on one
gateway core. A flip in the plaintext after the client checksum was verified, or a wrong seal, or
wrong parity, is copied to every chunk, and every chunk's CRC is then computed over the wrong
bytes. Replication protects against independent faults; this one is common-mode. The client's
checksum still catches it on a later read only if the client asks for checksums on reads, and by
then the PUT was acknowledged and the original bytes are gone.

### 5.3 What each check detects

| Check | Width | What it guarantees | Against adversaries | Source |
|---|---|---|---|---|
| TCP checksum | 16-bit ones' complement | little; "1 in 16 million to 10 billion packets" undetected in the field | none | STONE |
| CRC-32C (Castagnoli) | 32 | HD=4 (all 1–3-bit errors) up to 2,147,483,615 bits; HD=6 only to 5,243 bits; all bursts up to 32 bits; random errors pass with probability about 2^-32 | none: linear, forgeable | KOOP-ZOO; KOOP02 Table 1 (HD=6 to 5,243 bits, HD=4 from 5,244 to beyond 131,072); RFC3385 §5.1 (Informational) |
| CRC-64/NVME | 64 | all bursts up to 64 bits; random errors pass with probability about 2^-64; **Hamming-distance profile not published** | none | MANTLE-SRC; §below |
| MD5 (ETag) | 128 | not a security property; collisions are constructible | broken | note 14 |
| SHA-256 | 256 | collision resistance at 2^128 work | yes | FIPS 180-4 (note 14) |
| AES-256-GCM tag | 128 | authenticity of segment, index and file under the key | yes, with the key secret | encryption.md §3 |
| QUIC packet protection | AEAD tag | every packet's integrity between endpoints | yes | RFC9001 §5 |

**CRC-32C over mantle's records.** mantle checksums payloads in 64 KiB blocks (chunk-store.md
§3.1), 524,288 bits, well inside CRC-32C's HD=4 range and far beyond its HD=6 range. So within a
block every error of three bits or fewer is caught; a four-bit error can pass. Koopman's 2002
choice of polynomial for iSCSI explains the trade: Castagnoli's polynomial yields "5 bit error
detection (HD=6) for MTU-size payloads and 3 bit error detection (HD=4) to 114K bits" [KOOP02
abstract], and RFC 3385, an Informational memo, gives the undetected-error estimates that informed iSCSI's
choice of CRC-32C [RFC3385 §4–§5, §9]. Real corruption is
not independent bit errors (STONE; BGS+08's clustered blocks), and against bursts and random garbage
the guarantee that matters is the 2^-32 escape rate per checked block.

**DERIVED: the escape rate at fleet scale.** A checked block whose content is random garbage
passes CRC-32C with probability 2^-32 ≈ 2.3×10^-10. A fleet reading an exabyte a day reads
10^18 / 65,536 ≈ 1.5×10^13 blocks of 64 KiB a day. If one in a million of them were damaged (an
assumed rate for illustration, not a measurement), 1.5×10^7 damaged blocks a day would reach the
check, and 1.5×10^7 × 2.3×10^-10 ≈ 3.5×10^-3 would pass it: about one a year. The figure scales
linearly with the damage rate, which mantle will measure (§5.6). It argues for a second,
independent check at the end, which mantle has: the segment's GCM tag on every encrypted read,
and the client's checksum.

**CRC-64/NVME.** S3's default full-object checksum (note 05 §3.1). mantle's implementation uses
the reflected polynomial 0x9A6C9329AC4BC9B5, normal form 0xAD93D23594C93659 (MANTLE-SRC
`crates/crc/src/lib.rs:101–104`), and reproduces S3's test vectors (note 05 §3.4). Koopman's 64-bit
table lists a polynomial annotated "Jones", 0xD6C9E91ACA649AD4 in his implicit-+1 notation, which
is 0xAD93D23594C935A9 in normal form, with HD=4 to 12,296,205,748 bits and HD=5 to 1,626,758 bits
(KOOP-ZOO). **The two polynomials differ in their low bits** (…935A9 against …93659, checked by
arithmetic), so Koopman's figures do not describe CRC-64/NVME, and no source consulted states its
Hamming distances. They are computable offline for the lengths mantle uses (64 KiB blocks, 8 MiB
chunks), and nothing in this note's proposal depends on them until computed.

**Combining CRCs.** CRC-32C and CRC-64/NVME combine: `crc(a ‖ b)` follows from `crc(a)`, `crc(b)`
and `|b|` (MANTLE-SRC; note 05 §3.4). This is what lets a checksum be carried rather than
regenerated (§5.5): a block's CRC is combined from its 64 KiB blocks' CRCs, a chunk's from its
records', an object's full CRC from its parts', without touching the bytes again.

### 5.4 Every boundary, and what checks it

| # | Boundary | Check today | Gap | Close it with |
|---|---|---|---|---|
| B1 | client → gateway (wire) | QUIC AEAD or TLS; the S3 checksum over plaintext, verified at end of body | none on the wire | — |
| B2 | gateway memory: plaintext after its checksum is computed, before sealing | none | a flip is sealed and authenticated | per-segment CRC of plaintext computed as it arrives and combined into the request's checksum; after sealing, the segment is opened on a different core and its plaintext CRC compared (§5.6) |
| B3 | sealing | none | wrong ciphertext on a defective core; self-inverting AES [CORES §2] | the open in B2 runs on a different core than the seal |
| B4 | erasure coding | the block's CRC-32C, over data chunks | wrong parity is found only when a decode later uses it | a random linear check of parity against data on a different core (§5.6); or a full re-encode where measured cheap enough |
| B5 | gateway → storage node | QUIC AEAD; chunk CRC-32C checked before write | the storage node then computes the per-64 KiB table itself from bytes in its memory | the gateway sends the per-64 KiB CRC table it computed over the chunk; the node verifies each block against it and stores those values, never its own recomputation (§5.5) |
| B6 | storage node → device | record header CRC, per-block CRCs, identity, separate index copy | — | — |
| B7 | device → storage node (read) | every read verified; `Corrupt` typed | — | — |
| B8 | storage node → gateway | QUIC AEAD | the bytes leave the node unchecked after verification | the node sends the stored per-block CRCs with the bytes; the gateway verifies (cheap, and catches the node's memory) |
| B9 | gateway decode | block CRC-32C after decode | — | — |
| B10 | gateway open | GCM tag per segment | — (detects B2/B3 errors at read time, too late to repair) | B2/B3 at write time |
| B11 | gateway → client | AEAD on the wire; S3 checksum headers when asked; per-block CRC on the native protocol (§4.3) | listener ranged reads without a checksum header | none available in S3's protocol; TLS's AEAD |
| B12 | Raft entries | frame CRC on disk | an entry is encoded in memory, then replicated: a flip before encoding goes to every replica | the command's own CRC computed where it is built (the gateway) and checked at apply on every replica; mismatch fences the command, not the replica |
| B13 | engine | block checksums on read | block-cache hits unverified; memtable unprotected | the port's per-key-value protection on (note 12 §1.12: "per-key-value checksums in memtables and write batches", off by default); block-cache entries verified on serve at a sampled rate whose cost is measured |
| B14 | metadata caches at the gateway | none | a flipped File extent or Block row in a gateway's memory points a read at the wrong place | each cached row keeps the CRC it was read with and is checked on use; a mismatch drops the entry (rows are small: the cost is a CRC over tens of bytes) |
| B15 | chunk/block caches | none | ZFS's page-cache window [ZFS §5.3] | sealed bytes verified by GCM on serve (§3.9); unsealed by the stored CRC table |

### 5.5 Carry checksums; do not regenerate them

ZFS: "Since checksums are created when blocks are written to disk, any corruption to blocks that
are dirty (or will be dirtied) is written to disk permanently on a flush" [ZFS §5.3, Obs. 3].
Tectonic: "All API boundaries involving moving, copying, or transforming data had to be
retrofitted to include checksum information" (note 01 §1.13).

**Rule (INFERENCE).** A layer that receives bytes with a checksum verifies them against it and
passes that checksum on; it computes a new one only over bytes it produced by a transform, and
then verifies the transform (§5.6). CRC combination (§5.3) makes this cheap where the unit
changes: the gateway's per-64 KiB CRCs over a chunk combine into the chunk's CRC; the storage
node's record table is the gateway's values; a decoded block's CRC is checked against the Block
row's, which the gateway combined from its chunks' when it wrote them.

### 5.6 Finding a corrupting CPU or memory

**Inverse transforms, elsewhere.** Tectonic computes the inverse G of a transform and compares
checksums, "an acceptable cost" (note 01 §1.13). A self-inverting mis-computation on one core
defeats the check when it runs on the same core [CORES §2], so the inverse runs on a different
physical core, chosen by the coding pool (node.md §1.2), not the thread that sealed.

| Transform | Inverse | Cost (one core) | Proposal |
|---|---|---|---|
| Seal (AES-256-GCM) | open, compare the plaintext's CRC | about 8.4 GB/s (encryption.md §3): roughly doubles sealing's CPU | every segment, before its block goes down |
| RS encode | Freivalds-style check: for a fixed random vector r over the field, `r·parity = (r·P)·data`, where `r·P` is computed once per code (DERIVED; Freivalds 1977 not read) | one parity chunk's worth of field multiplies per block, about 1/m of encode, m the parity count | every block, on a core other than the encoder's |
| RS decode | the block CRC-32C | combined CRC | already done (gateway.md §3) |
| CRC combine | none needed: verified when the bytes are next read | — | — |
| Compression (if added) | decompress and compare | measured | Meta's Spark loss was in this path [SDC21 §4] |

The random vector's check fails with probability at most 1/|F| per block for a wrong parity
(DERIVED from the Schwartz–Zippel bound over GF(2^16), the field of mantle's coder, gateway.md
§4), so 1.5×10^-5 per corrupted block per check; a second independent vector squares it. A full
re-encode costs as much as the encode; whether it is worth the certainty is a measurement of the
coding pool's headroom (`mantle bench ec`).

**Screening cores.** Google: "a simple RPC service that allows an application to report a suspect
core or CPU. Reports that are evenly spread across cores probably are not CEEs; reports from
multiple applications that appear to be concentrated on a few cores might well be CEEs, and become
grounds for quarantining those cores" [CORES §5]. Meta runs out-of-production screening
(Fleetscanner, "93 percent coverage among all detected" silent errors but "six months" for full
coverage) and in-production screening (Ripple, "70 percent ... within 15 days", 7% unique) [SDC22
§5–§6] (PREPRINT). **INFERENCE for mantle:** every inverse-check failure, every decoded block whose
CRC fails while each source chunk verified, and every GCM failure on bytes whose chunk CRCs
verified is attributed to the core and node that produced the bytes, and recorded with the core
ID. A node's counts per core are compared against the cell's: concentration on one core
quarantines that core from the coding pool and the sealing path (affinity), concentration on one
node fences the node from writing until screened. No threshold is set here; the attribution and
the comparison are, and the threshold follows from the false-positive rate the cell measures
when no core is bad.

**Memory.** DRAM errors are mostly hard and repeat [DRAM09 abstract]; a correctable error raises
the chance of an uncorrectable one 9–400 times [DRAM09, Conclusions]. The node reads the platform's
correctable-error counters where the OS exposes them (Linux EDAC; the Windows and macOS
equivalents are note 29's or 10's to find) and treats a rising count as device health is treated
(note 10): the node's caches are shrunk toward zero and its in-memory indexes re-verified, and the
node is drained when the rate crosses what the cell's history says precedes uncorrectable errors.

**In-memory structures.** Long-lived structures a flip would corrupt silently: the chunk index
(identity checked against each record on read, chunk-store.md §7, so a wrong index entry yields
`Corrupt`, not wrong bytes), the segment table, cached metadata rows (B14), the engine's
memtables (B13), session tables in ranges (in the engine). A background verifier, paced like the
scrubber, recomputes each cached row's CRC and each index checkpoint against the live index, and
feeds the same attribution.

### 5.7 What to do on detection, layer by layer

| Detected at | First action | Then | Never |
|---|---|---|---|
| storage node read (`Corrupt`, EIO) | return `Corrupt` naming the chunk | report for repair; scan ±10 MiB; mark device at risk (chunk-store.md §9) | return the bytes; crash the node |
| gateway, chunk | read another copy or decode from `data` others (gateway.md §3) | report the chunk; if the other copies verify, repair restores it | serve unverified bytes |
| gateway, decoded block CRC fails though sources verified | try another subset of chunks | attribute to the gateway core (§5.6); report | repair from that decode |
| gateway, GCM open fails | as above | if every subset fails, the write was wrong at the source: the version is reported as damaged, never silently served | return plaintext that failed its tag |
| gateway, write-time inverse check fails | redo the transform on another core and check again | attribute; fail the PUT with `500 InternalError` only if the second attempt fails too | acknowledge |
| range replica, entry or engine | fence the replica (replica.md §2) | repair from peers (replica.md §4, §6; AGL+18) | truncate past corruption (note 03, AGL+18-F2) |
| cache, entry fails on serve | drop the entry, refill from source | attribute to the node's memory | serve it |
| client, checksum mismatch on GET | the client library retries from another gateway | reports the gateway | — |

The disposition column of protocol-aware recovery holds here too: "if there exists at least one
correct copy of a committed data item, it will be recovered or the system will wait for that item
to be fixed" (note 03, AGL+18), and a crash is never confused with corruption (chunk-store.md
§3.2, §6).

### 5.8 Keeping corruption out of caches and out of repair

- **Verify on fill.** A cache is filled only with bytes that verified on the way in: a block from
  chunks whose CRCs verified and whose decode matched the Block row's CRC; a row from an engine
  read whose block checksum verified.
- **Verify on serve.** Sealed bytes by their tag, unsealed by their stored CRC table, rows by
  their kept CRC (B14, B15). ZFS proposes exactly this for its page cache, "Block-level checksums
  in the page cache ... verify the checksums on reads", noting its overhead [ZFS §9].
- **Never write back from a cache.** A cache is never a source for repair or for a write: repair
  reads chunks from volumes and verifies them.
- **Repair verifies every source and its own output.** Krioukov et al.'s parity pollution is a
  scrub or reconstruction that computes new redundancy from a corrupt input [KRIO08 §1]. mantle's
  chunks are never updated in place, so RAID's read-modify-write path does not exist, but repair
  computes a lost chunk from `data` others: each source's CRC is verified, the rebuilt chunk is
  checked by decoding the block it belongs to against the Block row's CRC, and only then written,
  on a core other than the one that computed it (§5.6).
- **Replica comparison.** Meta lists comparing replicas efficiently as open (note 12 §1.12), and
  40% of its RocksDB corruptions had propagated. Range replicas apply the same log
  deterministically (replica.md §2), so a digest of a range's rows at an applied index is
  comparable across members; a mismatch is the AGL+18 case of a member whose state is wrong while
  its log is right.

---

## 6. Stepped complexity

Each step adds only what a limit it meets requires; the mechanisms of the earlier steps stay.

**Laptop** (one node, one device, a few GB of memory; the HTTP/1.1 listener and the native
protocol both on loopback):

- Caches: the engine's block cache; a gateway File/Block row cache and a Name row cache with
  validation (here a ReadIndex round is a local step, so validation is nearly free); no chunk
  cache unless the curve says so (§3.5 item 4). SHARDS sampling runs from the start, since it is
  what decides.
- Ordering: the GET window; per-block streams on the native protocol; in-order emission on the
  listener.
- Integrity: every check of §5.4, including the inverse checks on another core. One machine has no
  other copy to cross-check a CPU against; the inverse check is the only defence, and the laptop
  pays it.
- No hedging (one copy, one device), no hot-item replication, no leases.

**Node** (a server: many cores, many devices, memory for real caches):

- Chunk cache in DRAM, and on flash where disks sit behind it, sized and policed by §3.4–§3.5.
- The memory division among caches by marginal saving.
- Per-core attribution of integrity failures and core quarantine within the node (§5.6).
- Correctable-error counters feed the node's cache sizes and health (§5.6).

**Cell** (many nodes, gateways separate from storage, ranges replicated):

- Validation crosses the network: a ReadIndex round per batch at each Name leader; coalescing by
  the next-round rule (§3.8).
- Hedged and degraded reads at the measured quantile, capped by headroom (§3.8).
- Hot-block replication across a tenant's shuffle shard of gateways; the O(n log n) entry count
  from the cell's volume count [FAN11].
- Cross-node attribution: a node whose cores' failure counts stand out is fenced from writes.
- Replica digests for ranges (§5.8).

**Region** (many cells, a router):

- Clients far from a range's leader pay a round trip per validation. A deployment that states a
  clock-drift bound may enable leader read leases, or quorum leases so a majority can validate
  locally [QLEASE] (note 06 §A1.5); without a stated bound, validation stays at the leader.
- The cell map's constant-work distribution (already designed).

**Fleet** (many regions):

- No cross-region cache coherence: S3's consistency is per region, and buckets live in one region
  (note 05 §13); caches never span regions.
- Fleet-wide screening of CPUs and memory, out of and in production, with the attribution data
  from every cell (SDC22's funnel, PREPRINT).

---

## 7. Design proposal

Each item names its sources and the measurement that confirms it. No constant is chosen here.

1. **Key every data cache by immutable identity, and keep the consistency obligation only in the
   Name read.** Sources: gateway.md §1–§2; metadata.md §1; note 01 §1.5 (TEC §3.3). Confirm: the
   linearizability checker over histories with every cache on (§8 T6) accepts them; a test that
   disables validation must fail it.
2. **No write-back staging; nothing acknowledged before every chunk is durable.** Sources:
   CLAUDE.md §6; gateway.md §2. Confirm: the crash tests of §8 T4 never find an acknowledged PUT
   unreadable.
3. **Validate cached Name rows by ReadIndex taken after the request arrived; serve conditional
   GETs (`304`) from the validated row.** Sources: VOGELS21 (note 09 §6.6); TAO §5.3; note 06
   §A1. Confirm: `mantle bench gateway` round trips and bytes per GET with and without the cache;
   T6.
4. **Coalesce Name validations only onto rounds that began after the joiner arrived; coalesce block
   fetches unconditionally.** Sources: note 05 §13; MEMCACHE §3.2.1. Confirm: T6's swarm histories,
   and a test that lets a request join an in-flight round must fail linearizability.
5. **Treat absence as a row: validated negative entries, no TTL negative cache anywhere.** Sources:
   S3-2019; RFC2308 §1; CACHELIB §3.5. Confirm: T6 with create-after-probe histories (the 2019
   caveat as a test).
6. **One cache implementation per layer, lock-free on hits, with the eviction policy chosen per
   node by scaled-down simulation among FIFO, CLOCK, S3-FIFO, W-TinyLFU and ARC.** Sources: S3FIFO;
   TINYLFU; ARC; SHARDS §4.6; note 23 §2.2. Confirm: simulated against exact miss ratios on recorded
   mantle traces (the simulation's MAE must be measured below the margin by which a policy is
   chosen), and throughput at the node's thread count.
7. **Size every cache from SHARDS curves measured online, and divide memory among caches by equal
   marginal saving.** Sources: SHARDS; node.md §2.5. Confirm: the curves' error against exact
   curves on recorded traces; the division's total miss cost against fixed splits.
8. **Cache sealed bytes at gateways; never cache plaintext or unwrapped keys across requests.**
   Sources: encryption.md §3; note 20. Confirm: an SSE-C GET without its key against a warm cache is
   refused (T7); the cost of opening on every hit (`mantle bench hash`).
9. **Native GETs carry a stream per block in flight with each block's offset and CRC-64/NVME; the
   client library restores order within its window or writes to positions.** Sources: RFC9000 §2.2,
   §13; SLATES-SRC `stream.rs`; gateway.md §3. Confirm: GET latency and gateway memory per slow
   client, one stream against per-block streams, under injected loss and one slow volume (T9).
10. **Bound head-of-line blocking by hedging at a quantile derived from the read amplification the
    devices can absorb, and derive the GET window from the latency at that quantile.** Sources:
    TAIL; note 01 §1.13; node.md §2.6. Confirm: p99 and p99.9 of GETs with one slow volume, and the
    extra reads hedging caused, against the headroom measured.
11. **Two levels inside the request class on the native protocol (waited-on exchanges above
    incremental bulk); classes decided by message kind, never by the client.** Sources: RFC9113
    §5.3.1; RFC9218 §4; SLATES-SRC BENCHMARKS; node.md §3.1. Confirm: small-request latency beside
    saturating bulk on one connection, with and without the split.
12. **The HTTP/1.1 listener serves pipelined requests one at a time, streams GETs in order from the
    window, and returns stored checksums in headers.** Sources: RFC9112 §9.3.2; note 05 §3.5; node.md
    §2.5. Confirm: s3-tests and the AWS CLI's transfer manager against the listener (T10).
13. **Compute a CRC per plaintext segment as the body arrives, combine it into the request's checksum,
    and verify every sealed segment by opening it on a different core before its block goes down.**
    Sources: E2E; note 01 §1.13; CORES §2. Confirm: injected flips in the plaintext buffer and an
    injected self-inverting sealer are caught before acknowledgement (T1, T2); the CPU cost from
    `mantle bench hash`.
14. **Check parity against data with a random linear combination on another core, and decide by
    measurement whether a full re-encode is affordable.** Sources: KRIO08; note 01 §1.13; DERIVED
    (Freivalds). Confirm: injected parity errors caught (T2); coding pool headroom (`mantle bench
    ec`).
15. **Carry checksums: the gateway sends each chunk's per-64 KiB CRC table; the storage node verifies
    and stores it; nodes send stored CRCs with every read; caches keep the CRC of what they hold.**
    Sources: ZFS §5.3; note 01 §1.13; chunk-store.md §3.1. Confirm: flips injected in the storage
    node's buffer between receipt and write are caught (T1).
16. **Give every range command a CRC computed where it is built and checked at apply on every
    replica; turn on the engine's per-key-value protection.** Sources: note 12 §1.12; replica.md §2.
    Confirm: flips injected into encoded commands and memtables are caught (T1); the engine's cost
    against RocksDB with protection on (note 23's matrix).
17. **Attribute every integrity failure to the node and core that produced the bytes; quarantine cores
    and fence nodes by comparison with the cell's own background rate.** Sources: CORES §5; SDC22.
    Confirm: an injected faulty core is quarantined and a healthy cell's false-positive rate is
    measured (T3).
18. **Read correctable-memory-error counters as device health; shrink caches and drain on a rising
    rate.** Sources: DRAM09, Conclusions; note 10. Confirm: what each OS exposes (§9).
19. **Repair verifies every source chunk and its rebuilt chunk against the block's CRC before
    writing.** Sources: KRIO08; AGL+18 (note 03). Confirm: repair under injected source corruption
    never writes a chunk that fails the block's CRC (T5).
20. **Compute CRC-64/NVME's Hamming distances at 64 KiB and 8 MiB before relying on them.** Sources:
    KOOP-ZOO's method; MANTLE-SRC. Confirm: the computation, checked by reproducing Koopman's
    published CRC-32C and "Jones" figures with the same program.
21. **Replica digests at an applied index for every range, compared across members on a schedule
    paced like the scrubber.** Sources: note 12 §1.12; replica.md §2. Confirm: an injected divergent
    member is found (T5).

---

## 8. Test plan

Every test runs both client paths: mantle's client library over the native protocol, and stock S3
clients (the AWS CLI, boto3, s3-tests) over the HTTP/1.1 listener, which is on by default.

**T1. Bit flips in memory.** A fault-injecting allocator and buffer hooks flip chosen and random
bits in: the PUT's plaintext segment after its CRC is taken; a sealed segment; a data chunk after
coding; parity; the storage node's receive buffer; an encoded range command; a memtable entry; a
cached Name, File or Block row; a cached sealed block. Each flip must be detected at the boundary
§5.4 names, before acknowledgement for writes, before serving for reads, and attributed (§5.6).
ZFS's method, random flips at a rate with a predefined workload whose reads are all checked [ZFS
§6.1], gives the second form: random flips over the gateway's and storage node's heaps while a
workload with known content runs; no read may return wrong bytes, and every crash or refusal is
counted.

**T2. A defective core.** The sealer and the coder accept an injected fault that makes one named
core produce wrong output deterministically, including the self-inverting case (wrong seal, matching
wrong open on the same core) [CORES §2]. No PUT using that core may be acknowledged with wrong
bytes; the core must be quarantined.

**T3. Attribution's false positives.** A cell with no injected fault runs a week of the benchmark
workload; the attribution's counts per core and node are recorded, and the quarantine rule must not
fire. Then one faulty core is added and must be found within a stated number of its failures.

**T4. On disk.** The chunk store's simulated file already flips bits, tears writes and fails flushes
(chunk-store.md §10); add misdirected and lost writes at the volume layer (data lands at another
record's offset, or is reported written and not written), as Krioukov et al.'s model enumerates
[KRIO08 §3]. Invariants as chunk-store.md §10, plus: no acknowledged object is unreadable while its
scheme's tolerance holds.

**T5. Repair under corruption.** Corrupt one source chunk of a block whose other chunk is lost; repair
must refuse to rebuild from the corrupt source and must rebuild from a verified subset when one exists.
Inject a divergent range member; the digest comparison must find it, and protocol-aware recovery
repair it (replica.md §4–§6).

**T6. Cache consistency and linearizability.** With every cache on at every layer, generated histories
of concurrent PUT, GET, HEAD, DELETE, conditional writes, completions and LISTs over few keys, with
gateways that crash and restart with warm or cold caches, leaders that change, and network delays; the
WGL checker of note 06 §A6.8 must accept every history (per-key, plus LIST's visibility rule from note
05 §13). Required failing variants: a cache served without validation; a Name validation joining an
in-flight round; a negative entry served with a TTL; a LIST page served from cache without its range's
last-write check. Each must be caught within the first seeds, as replica.md §5 requires of its broken
variants.

**T7. Encryption and caches.** A warm cache must not serve an SSE-C object to a request without its
key, nor one with a wrong key; a flipped bit in a cached sealed block must fail its tag and be refilled.

**T8. Swarms and hot keys.** Open-loop load (note 26 §3) of many logical clients on one object, one
prefix, and one key under concurrent writes: the number of chunk reads per object must stay near one per
gateway per block; Name validations per heartbeat at the leader must stay at one round; p99 latency and
the back-end load must match FAN11's prediction for the cell's volume count; writers on one key must get
`503 SlowDown` before any queue passes its bound.

**T9. Ordering.** GETs with one slow volume, one lost-packet path, and a stalled client: emitted bytes are
in order and exact; the gateway never holds more than its window per GET; per-block streams deliver blocks
behind a slow one; hedges stay within the read amplification cap. Multipart: parts uploaded in random order,
concurrently and repeatedly, with completions racing part re-uploads; the completed object's bytes are the
listed parts' in number order or the completion is refused.

**T10. The HTTP/1.1 listener.** s3-tests' suites, the AWS CLI's multipart upload and ranged download, and
boto3 with default checksum settings (CRC32 or CRC-64/NVME trailers, note 05 §3.6), against the listener;
pipelined requests on one connection get responses in order; `304` for a matching `If-None-Match`; checksum
headers present on full-object and part-aligned reads and absent otherwise.

**T11. Cache sizing.** Recorded mantle traces (agent workloads from the benchmark states of note 21) replayed
against exact LRU stacks and full simulations of each policy; SHARDS and scaled-down simulation must match
within their measured error, and the chosen policy and size must be the exact optimum's within that error.

**Benchmarks with baselines** (CLAUDE.md §8): cache hit throughput by policy and thread count; GET with cache
hit, miss and validation; sealing with and without the inverse open; coding with and without the parity
check; per-block streams against one stream.

---

## 9. What remains unknown

- **The workload.** No published trace describes agents at machine speed against an object store; every
  policy comparison above is on other workloads. The curves of §3.5 and the simulations of §3.4 are how
  mantle finds out, from its own traffic.
- **CRC-64/NVME's Hamming distances** at mantle's lengths (§5.3). Not in Koopman's tables; the polynomial he
  lists as "Jones" is a different one.
- **The cost of the inverse checks** on each platform: AES-GCM opens at 8.4 GB/s on this machine
  (encryption.md §3), but its share of a PUT's CPU at full rate, and whether a full parity re-encode is
  affordable, are unmeasured.
- **The background rate of integrity failures** that attribution compares against, and so the quarantine
  thresholds (§5.6). SDC22's one-in-a-thousand devices and CORES's few-per-several-thousand machines are other
  fleets' figures, the first a preprint.
- **What each OS exposes of memory errors.** Linux has EDAC counters; what macOS and Windows expose to an
  unprivileged process was not researched here.
- **The validation's cost at a hot leader** under billions of principals: one engine point read per validated
  key per round. Whether follower reads with a leader-issued read index (note 06 §A1) are needed before leases
  to spread that cost is a measurement at the cell step.
- **Leases' clock bound.** Unchanged from replica.md §7: no lease until a deployment states and mantle measures
  a drift bound.
- **Freivalds' check** is applied here from its well-known form without reading the 1977 source, and the field
  bound is DERIVED; both should be checked before the parity check is built.
- **Not obtained:** Sridharan et al. (ASPLOS '15) and Alibaba's SOSP '23 study of silent data corruption in
  production CPUs, which would add a second peer-reviewed fleet rate for CPU defects; Mattson et al. (1970) in the
  original; LIRS's own paper in a readable copy.
- **Listing caches.** Whether any workload makes a validated LIST cache worth its per-span bookkeeping (§3.6).
- **Client-side caching on the native protocol** under an application that holds an object open across a version
  change: the library revalidates on each read, but what an application API should promise (snapshot of a version
  or latest per read) is a design choice for the client library, not settled here.
