# 30 — Resilient transfer: interrupted uploads, congestion, poor and variable bandwidth, power loss

**Status:** research input for the gateway (`docs/design/gateway.md`), the metadata layers
(`metadata.md`), the node's transport and front end (`node.md` §3–§5), the client library and CLI,
and the HTTP/1.1 S3 compatibility listener. This is not a decision record; §9 proposes decisions
for the design records to take or refuse.
**Compiled:** 2026-09-30.
**The question, verbatim from the owner:** "have we researched how to handle interrupted uploads?
Network congestion? Variable bandwidth? Poor bandwidth? We have SOME implementation in ../slates,
however it really starts to matter in mantle (slates focuses on QUIC, which we'll need to as well,
so it's worth looking at the implementation). What about sudden power loss? Etc.?" and "No
shortcuts. Maximally correct, robust, performant, and efficient."

**The client-facing protocols, as the owner decided them on 2026-09-30.** Mantle speaks its own
protocol over QUIC, natively and end to end: between nodes, and to clients through mantle's own
client library and CLI, carrying S3 semantics. There is no HTTP/2 and no HTTP/3 anywhere. The only
HTTP is an HTTP/1.1 S3 compatibility listener, **on by default**, that translates stock S3 requests
(AWS CLI, boto3, the SDKs, rclone) onto the native protocol; its resilience is a first-class,
default-on path, not a fallback. The evidence that HTTP/1.1 is the whole of what stock clients
need: on 2026-09-30 the coordinator probed live Amazon S3 endpoints (`s3.amazonaws.com`,
`s3.us-east-1`, `s3.us-west-2`, `s3.dualstack.us-east-1`, `s3express-control.us-east-1`); offered
`h2` by ALPN each answered `http/1.1`, none advertised HTTP/3 (no `Alt-Svc`), and AWS documents
HTTP/3 only for CloudFront (**primary**: a live probe, reported by the coordinator; not re-run by
this note). `node.md` §4.1 still says "HTTP/1.1 and HTTP/2 through hyper", and note 25 §2 treats
HTTP/2 flow control and resets; both predate the decision and must change (§9 D1).

**Scope.** (1) Interrupted uploads end to end: client disconnects, gateway and node crashes and
sudden power loss at every step of a PUT, a part and a completion; what S3 guarantees and what S3
clients do; what mantle's existing machinery covers and what is missing, each with its protocol and
proof obligations. (2) Congestion and poor or variable bandwidth: on the native QUIC protocol and on
the HTTP/1.1 listener's TCP; congestion control per environment, pacing, loss recovery, path MTU,
windows from BDP and memory, brownouts, adapting part size and concurrency online, deadlines from
measurement and progress rather than the wall clock, connection migration, and Raft under
brownouts. (3) Sudden power loss on clients, gateways, storage nodes and laptops. (4) slates' and
focal's transport mechanisms, one verdict each. (5) What each step of scale newly needs.

**What this note does not repeat.** Admission, fairness between uploads and the block window
`w = ⌈R·T/B⌉` are note 27 (§6.1, §7.1); storage classes and power-aware writes are note 28; device
classes and whether a device honours a flush are note 29; HTTP/1.1 server behaviour, S3's retry
contract, QUIC flow-control memory, 0-RTT replay, RFC 8085 and RFC 9221 are note 25 §2–§6; the
concurrency model is note 26; the audit's derivations of part size, BDP windows and progress
deadlines are audit §13 and §16, which this note builds on and cites rather than restates.

---

## 0. How to read this note

**Citation tags.** RFCs as `[RFC9000 §9.4]`; drafts as `[RESUME-12 §4.1]`; sibling source as a path
and line range at a named commit; earlier notes as "note 25 §x"; the audit as "audit §x"
(`docs/audit/2026-09-29_audit.md`, read for its argument).

**Evidence labels**, as in notes 12, 23, 25 and 27:

- *(no label)*: stated in a **peer-reviewed** source and checked against its text or abstract.
- **primary**: a standard (RFC), an IETF draft (labelled as a draft), vendor documentation, or a
  library's source, read directly.
- **NON-PEER-REVIEWED**: blog posts, magazine articles, project records.
- **DERIVED**: arithmetic or interpretation made by this note.
- **UNVERIFIED**: not confirmed against a primary text this note read.
- **INFERENCE / Recommendation**: reasoning for mantle, citing the facts it rests on.

**Method.** RFCs 6298, 8085, 8470, 8899, 9000, 9001, 9002, 9114, 9221, 9308 and 9438 were
downloaded as text from rfc-editor.org on 2026-09-30 and every quoted sentence was matched by
machine against that text with whitespace collapsed. draft-ietf-httpbis-resumable-upload-12 was
downloaded from the IETF archive and checked the same way. AWS pages and paper abstracts were read
through a fetch that summarizes; where a sentence below is in quotation marks from those, it is the
fetched text, and where it is not it is a paraphrase. Sibling source was read at slates
`fdaa51d2fd373f81231b1ad54d446497cb1d8b4a` and focal `a8e95f71496461ebc8984926a65666746e946473`
(both 2026-09-30, working trees included), and quinn-proto 0.11.18, the version focal resolves,
from the local cargo registry. Line numbers are at those revisions; the audit read slates at
`8ab25deb` and focal at `801ba65b`, so some of its line references have moved.

## Sources

| Key | Source | Label |
|---|---|---|
| RFC9000 | QUIC: A UDP-Based Multiplexed and Secure Transport, §8, §9, §10.1, §14 | primary |
| RFC9001 | Using TLS to Secure QUIC, §6.6, §9.2 | primary |
| RFC9002 | QUIC Loss Detection and Congestion Control, §5–§7 | primary |
| RFC8899 | Packetization Layer Path MTU Discovery for Datagram Transports | primary |
| RFC9308 | Applicability of the QUIC Transport Protocol, §3.2 | primary |
| RFC6298 | Computing TCP's Retransmission Timer | primary |
| RFC8470 | Using Early Data in HTTP | primary |
| RFC9438 | CUBIC for Fast and Long-Distance Networks | primary |
| RFC9114 | HTTP/3, read only to record why it is not used | primary |
| RESUME-12 | draft-ietf-httpbis-resumable-upload-12, 2026-07-06, Internet-Draft, intended Proposed Standard | primary, **draft** |
| TUS | tus resumable upload protocol 1.0.0 (tus.io) | NON-PEER-REVIEWED (community specification) |
| BBR16 | N. Cardwell, Y. Cheng, C. S. Gunn, S. H. Yeganeh, V. Jacobson. "BBR: Congestion-Based Congestion Control." ACM Queue 14(5), 2016 | NON-PEER-REVIEWED (magazine), cited through slates' constrained-links note |
| BBRv3 | draft-ietf-ccwg-bbr-06, 2026-07-06, Experimental | primary, **draft** |
| COPA | V. Arun, H. Balakrishnan. "Copa: Practical Delay-Based Congestion Control for the Internet." NSDI '18 | peer-reviewed (abstract read) |
| VIVACE | M. Dong et al. "PCC Vivace: Online-Learning Congestion Control." NSDI '18 | peer-reviewed (abstract read) |
| SWIFT | G. Kumar et al. "Swift: Delay is Simple and Effective for Congestion Control in the Datacenter." SIGCOMM '20 | peer-reviewed, **UNVERIFIED** here (not re-read) |
| HOMA | B. Montazeri et al. "Homa: A Receiver-Driven Low-Latency Transport Protocol Using Network Priorities." SIGCOMM '18 | peer-reviewed, through note 27 §5.5 |
| MATHIS97 | M. Mathis, J. Semke, J. Mahdavi, T. Ott. "The Macroscopic Behavior of the TCP Congestion Avoidance Algorithm." CCR 27(3), 1997 | peer-reviewed, through slates' constrained-links note |
| KAKHKI17 | A. M. Kakhki et al. "Taking a Long Look at QUIC." IMC '17 | peer-reviewed, through slates' constrained-links note |
| ZHANG24 | X. Zhang et al. "QUIC is not Quick Enough over Fast Internet." WWW '24 | peer-reviewed, through slates' constrained-links note |
| WARE19 | R. Ware, M. Mukerjee, S. Seshan, J. Sherry. "Modeling BBR's Interactions with Loss-Based Congestion Control." IMC '19 | peer-reviewed, through slates' constrained-links note |
| YOUNG74 | J. W. Young. "A First Order Approximation to the Optimum Checkpoint Interval." CACM 17(9), 1974 | peer-reviewed, **UNVERIFIED** here (formula stated from the literature, paper not re-read) |
| DALY06 | J. T. Daly. "A Higher Order Estimate of the Optimum Checkpoint Interval for Restart Dumps." FGCS 22(3), 2006 | peer-reviewed, **UNVERIFIED** here |
| RAFT14 | D. Ongaro, J. Ousterhout. "In Search of an Understandable Consensus Algorithm." USENIX ATC '14, §5.6 | peer-reviewed, through note 06 |
| AWS-CLI | AWS CLI "S3 configuration" topic (`s3-config`) | primary |
| AWS-JTM | AWS SDK for Java 2.x developer guide, "Transfer files and directories with the Amazon S3 Transfer Manager" | primary |
| AWS-MPU | S3 multipart limits, ListParts, CompleteMultipartUpload, AbortMultipartUpload | primary, through note 05 §4 |
| SDK-RETRY, BOTOCORE, SDK-GO | the SDKs' retry modes and `amz-sdk-invocation-id` | primary, through note 25 §3, §13 |
| SLATES | slates `crates/transport` (session plane, Copa, pacer, RTT, reorder, PMTUD, flow, keys, demux), `crates/cluster/src/timing.rs`, `docs/wip/BENCHMARKS.md` §§ "Session-plane congestion control and scheduling bake-offs", "path MTU discovery", "adaptive reordering tolerance", `docs/wip/research/nfs-transport-constrained-links.md` | primary (source); its records NON-PEER-REVIEWED |
| FOCAL | focal `crates/focal-wire/src/{transport,congestion,peers}.rs`, focal-timing through note 07 §5.1 | primary (source) |
| QUINN | quinn-proto 0.11.18: `config/transport.rs`, `config/mod.rs`, `connection/{mod,pacing,assembler}.rs`, `connection/streams/recv.rs`, `congestion/bbr/mod.rs` | primary (source) |
| S3-PROBE | live ALPN and `Alt-Svc` probes of Amazon S3 endpoints, 2026-09-30, by the coordinator | primary, reported |

---

## 1. Decision-relevant summary

1. **The client knows the outcome of every interrupted write but one.** A Name commit needs the
   body's end, its length and its checksums (`node.md` §4.3), so a PUT or part whose client had not
   finished sending cannot have committed, and its client may start again as a new attempt. Only a
   request whose client sent its last byte (or its completion list) and got no answer has an
   unknown outcome. That one case is what a retry identity must cover (§2, DERIVED).
2. **Mantle's machinery already reclaims every abandoned intermediate state and answers a copy of
   a committed write as it answered the first, within one gateway's attempt.** Blocks never named,
   files never handed over, refused writes, losing completions and aborted uploads are each swept
   or reclaimed by a stated rule (`metadata.md` §2), and a re-sent Name command is recognised by
   its file's mark (`gateway.md` §2). What is missing is identity *across* attempts: a client's
   retry through another gateway, or after the gateway restarted, draws a new file and is a new
   write. In a versioned bucket it makes a second version; under `If-None-Match: *` it answers
   `412` to its own success; and a slow first attempt can land after its retry and replace a part
   whose ETag the client already holds (§3.3, G1).
3. **Proposed: a client operation identity, journaled before the first send, carried by every
   attempt and recorded with what it made.** The native protocol requires it; the HTTP/1.1 listener
   takes the SDKs' `amz-sdk-invocation-id`, which "two SDKs send ... on every attempt" (note 25
   §13), scoped to the authenticated principal so a forged value can only collide with the forger's
   own operations. The Name range answers a later attempt from the record and applies nothing
   (§3.4).
4. **Proposed: native uploads resume at a server-reported durable offset, as the IETF resumable
   upload draft does, built from the representation mantle already has.** The upload is cut, by the
   server, into runs at checkpoints; each run is a file with its own data key, handed to the Name
   range like a part, so durable progress needs no renewal, survives gateway loss and power loss,
   and a resume never re-seals a segment index under a key that sealed it before. The checkpoint
   interval is Young's optimum, `c* ≈ g·sqrt(2h/λ)`, the same law as the audit's part size (§3.5).
   Stock clients keep S3's own resume, multipart with `ListParts`, which the listener serves
   exactly (§3.6).
5. **"Durable" is one point on a ladder of seven, and the protocol names which.** Transport ACK,
   bytes received, segment sealed, chunk stored, block recorded, file written, version committed:
   the draft says the same of HTTP, "Data may have been delivered and acknowledged at the transport
   layer without yet being reflected in the offset" [RESUME-12 §4.1.1]. Mantle's resumable offset is
   the end of the last checkpointed run, never a transport or chunk offset (§2).
6. **On congestion control the evidence picks a family, not a winner.** Copa is "robust to
   non-congestive loss and large bottleneck buffers" and "outperforms other schemes on long-RTT
   paths" [COPA, abstract]; both siblings chose it on their own grids (slates 57 scenarios, focal
   30 paths). Its measured cost is thin links: 678 ms ping p99 against NewReno's 289 ms at
   64 kbit/s (slates BENCHMARKS). Loss-based laws are bounded by `MSS/(RTT·√p)` under random loss
   [MATHIS97]. Mantle starts with Copa at δ = ½ behind a controller interface and qualifies it on
   its own traffic mix (§4.1); the thin-link cost is attacked with the operating packet size and the
   pacing quantum, not δ (§4.2).
7. **quinn's pacer bursts at least ten datagrams.** `MIN_BURST_SIZE = 10` in quinn-proto 0.11.18
   (`connection/pacing.rs:147-148`): 12 KB, which is 1.5 s at 64 kbit/s and 12 s at 8 kbit/s, ahead
   of any control frame. slates' quantum is a millisecond of the pacing rate with a two-datagram
   floor (`congestion/mod.rs:119-132`, after draft-ietf-ccwg-bbr §5.6.3). Mantle needs the latter
   on thin paths, which means a patched, vendored quinn-proto or its own transport (§4.2, §6).
8. **A single quinn stream's window has a ceiling that is not memory.** quinn closes a connection
   whose stream buffer holds more than 1,024 non-contiguous spans (`assembler.rs:361`,
   `streams/recv.rs:81-83`, "too many gaps in stream buffer"), which focal hit at a 10 MiB window and
   avoided with a 1 MiB ceiling (`transport.rs:49-62`). A per-stream window of at most about
   2,048 datagrams cannot reach it (DERIVED, §4.5); a path whose BDP exceeds that uses several
   streams or a patched assembler.
9. **Deadlines are of progress, and memory is reclaimed faster than data.** A stalled upload's
   memory, coder and renewals are released when it stops making progress for a time derived from
   its path (`max_idle ≥ 3·PTO`, [RFC9000 §10.1]); its durable runs stay until the client's
   declared resume horizon or the bucket's lifecycle. A multi-day upload on a slow link completes
   because each checkpoint is durable and owned, not because something waits for it (§4.7).
10. **A laptop changing networks keeps its connection if QUIC migration works, and its upload if it
    does not.** "Only clients are able to migrate in this version of QUIC" [RFC9000 §9]; on a new
    address the congestion controller and RTT estimator reset "unless the only change in the
    peer's address is its port number" [RFC9000 §9.4]. quinn implements migration and keeps state
    for IPv4 port-only changes (`connection/mod.rs:3066-3087`); slates' dialect has no path
    validation or migration. Where UDP is blocked on the new network, the native client needs a
    route that is not QUIC (§4.9; open question in §11).
11. **0-RTT stays off for anything that mutates.** RFC 8470: clients "MUST NOT send unsafe methods
    (or methods whose safety is not known) in early data". The native protocol keeps 0-RTT off at
    first and takes TLS session resumption without early data, which saves the certificate chain's
    bytes on a thin link without any replay exposure (§4.9).
12. **Power loss is already exact on the server side; the client is where it is not yet
    specified.** Chunks and log frames are acknowledged only after the platform's full flush, and
    recovery never drops an acknowledged record without a mark (`chunk-store.md` §6,
    `raft-log.md` §6). A client must persist its operation identity before the first attempt and
    may persist progress lazily, since the server is authoritative for the durable offset (§5).
13. **Raft stays stable through a brownout by keeping control off the congested path and timing
    from measured tails.** Control rides the datagram plane (`node.md` §3.4); election timeouts are
    ten measured tail round trips, the order of magnitude Raft asks for (RAFT14 §5.6; slates
    `timing.rs:52, 175-213`); the bulk queue at the sender holds no more than one tail round trip of
    the path's rate (`node.md` §3.8) (§4.8).
14. **Of slates' and focal's transport mechanisms, mantle adopts the laws and the measurements and
    rejects their constants.** Copa, the pacer quantum, adaptive reordering, DPLPMTUD's recheck,
    absolute credits with a class reserve, ACK-of-ACK bounds, progress-charged waits and equal
    jitter are adopted or adapted; fixed 10 s idle, 5 s calls, 10 ms retry backoff, the 1 Gbit/s ×
    100 ms reference path, the PTO cap, a 1 ms first handshake retransmit and a class a peer
    chooses through its stream ID are rejected (§6).

---

## 2. Progress points, and what each party knows after a fault

A write passes seven points, and a fault leaves each party knowing a different one.

| # | Point | Who knows it | Durable? |
|---|---|---|---|
| T | Transport acknowledgement of the bytes | client's QUIC (or TCP) stack | no: in the gateway's memory at most |
| R | Bytes read by the `Put` driver | gateway | no |
| S | Segment sealed under the file's data key | gateway | no |
| C | Chunk `Stored` on a volume | gateway, volume | yes, one copy; nothing names it yet |
| B | Block recorded in the Block range, with a deadline | gateway, Block range | yes; reclaimed if never named |
| F | File written in the File range, with a deadline | gateway, File range | yes; released if never handed over |
| N | Version (or part) committed in the Name range | gateway, Name range | yes: the linearization point |
| A | The answer delivered to the client | client | — |

The audit lists the same distinctions: "A TCP/QUIC ACK, bytes written, a completed chunk, a
committed part and a committed object are different progress points; expose useful durable
progress without telling a client its object exists early" (audit §16.4). The resumable-upload
draft draws the same line for HTTP: the offset "reflects application-level processing for the
upload. Data may have been delivered and acknowledged at the transport layer without yet being
reflected in the offset" [RESUME-12 §4.1.1].

**The only unknown outcome (DERIVED).** `node.md` §4.3 commits nothing in the Name range "until
the body's length, digests, chunk signatures and trailer have all checked", and the `Put` driver
writes the file and commits the version only after `end` (`gateway.md` §2). So N cannot precede the
client's last byte. A client that knows it had not sent its last byte (its stream's FIN on the
native protocol, its final chunk or the end of `Content-Length` on HTTP/1.1) knows the write did
not commit, whatever the server did; it may start a new attempt with a new identity. A client that
sent the last byte and holds no answer cannot tell N from not-N. A completion is the same: its
outcome is unknown once its list was sent. Every retry identity in §3 exists for that one case.

---

## 3. Interrupted uploads, end to end

### 3.1 What S3 guarantees and what S3 clients do

- **A PUT is all or nothing.** A successful single PUT makes the object; an interrupted one leaves
  nothing visible. A single PUT carries at most 5 GiB (note 05 §4.1). Its integrity is
  `Content-MD5` or an `x-amz-checksum-*` header or trailer, checked before the object is stored,
  `BadDigest` on mismatch (note 05 §3.2). Current SDKs compute CRC32 (boto3, the Rust SDK and most
  others) or CRC64NVME (CLI v2) on every upload by default, and botocore sends uploads as unsigned
  `aws-chunked` bodies with a trailing checksum (note 05 §3.6).
- **A retried PUT is a new PUT.** S3 offers no request identity: the SDK resends the whole body
  under the standard retry mode, three attempts, with backoff below 1 s then 2 s for throttling
  (note 25 §3). A non-seekable source cannot be retried; the AWS CLI's CRT client says an upload
  "whose source cannot be rewound or if any of its data was already transferred" fails rather than
  being redirected [AWS-CLI, `preferred_transfer_client`].
- **Multipart is S3's resume.** Parts of 5 MiB to 5 GiB, up to 10,000, retried independently;
  "If you upload a new part using the same part number that was used with a previous part, the
  previously uploaded part is overwritten"; `ListParts` pages of at most 1,000 reconcile progress,
  but "Do not use the result of this listing when sending a *complete multipart upload* request";
  a re-sent identical `CompleteMultipartUpload` returns the same ETag and checksum, which s3-tests
  checks and SDK retries rely on; a completion can answer `200 OK` and then an error in its body;
  an abort "might be necessary ... multiple times in order to completely free all storage" because
  in-flight parts "might or might not succeed" (note 05 §4.3–§4.8).
- **What the transfer managers choose.** The AWS CLI defaults to `multipart_threshold` 8 MB,
  `multipart_chunksize` 8 MB and `max_concurrent_requests` 10, with a queue of 1,000 tasks; its CRT
  client targets the detected network bandwidth, or 4 Gbit/s when it cannot detect it, "The lower
  fallback limits the memory that the `crt` transfer client reserves for buffering parts"
  [AWS-CLI]. The Java Transfer Manager can "pause the transfer for later execution" [AWS-JTM]; what
  its resume token records and how it detects a changed source were not on the page read
  (**UNVERIFIED**). These are a vendor's chosen constants, recorded here as what stock clients will
  send, not as values for mantle.

**INFERENCE.** Stock clients will retry whole PUTs and whole parts, will not carry an identity the
server can rely on, and will leave abandoned uploads behind. The listener must make each of those
safe and bounded; it cannot change them.

### 3.2 The fault matrix

Rows are the step a fault interrupts; the columns say what is left, what removes it, and what the
client sees. "Sweep" is the File or Block sweep of `metadata.md` §2, "reconcile" the per-volume
reconciliation of unnamed chunks against the Block layer's reverse rows (`node.md` §5.2, to be
built), "collector" the reclaimer of released files.

| Step interrupted | Fault | What is left | What removes it | What the client sees and does |
|---|---|---|---|---|
| Admission, before the body | any | a reservation | released with the request | connection loss or `503 SlowDown` before `100 Continue`; retry is a new attempt (§2) |
| Body streaming, no block recorded yet (R, S, C) | client disconnect | sealed segments in memory; chunks stored, unnamed | memory with the driver; chunks by reconcile | it had not sent its last byte: new attempt |
| | gateway crash or power loss | chunks stored, unnamed | reconcile | same |
| | storage node power loss before `Stored` | a torn or unconfirmed record | recovery keeps it, reports it damaged; reconcile removes it, never acknowledged (`chunk-store.md` §6 step 3) | nothing: the gateway takes the chunk to another volume (`gateway.md` §2) |
| | storage node power loss after `Stored` | a durable chunk | it is part of the block | nothing |
| Body streaming, blocks recorded (B) | client disconnect or stall | recorded blocks, renewed while the driver lives | the driver ends; renewals stop; the block sweep releases each block after its deadline and takes its chunks apart at once (`metadata.md` §2, "Blocks never named") | new attempt, the whole body again (S3); a resume from the last checkpoint (native, §3.5) |
| | gateway crash | recorded blocks, no longer renewed | block sweep after the deadline | same |
| Body ended, checksums verified, file write in flight (F) | gateway crash | a written or unwritten file | the file sweep releases a file never handed over (`metadata.md` §2, "Files never handed over") | **unknown outcome**: it sent its last byte |
| Name commit in flight (N) | gateway crash; range leader change | a committed or uncommitted version | if committed: nothing to remove. If not: file sweep | **unknown outcome** |
| Committed, answer in flight (A) | client disconnect; gateway crash after commit | the version | nothing | **unknown outcome**; today a retry through any gateway is a new write (G1) |
| Completion list sent | any | a committed object or an open upload | a losing composite is reclaimed to its own rows (`metadata.md` §2, "Adoption") | **unknown outcome**; a re-sent identical list is answered as the first (`gateway.md` §2) |
| Abort | in-flight parts | parts that land after the abort | released by the Name range as an aborted upload's parts | S3 says abort again; mantle releases them on arrival (to confirm, §10) |

Where the gateway itself retries inside one attempt, the machinery is exact: a Name command whose
session expired is re-sent unchanged and "The range recognises the file it already took ... and
answers as it answered the first delivery" (`gateway.md` §2), a chunk re-sent with the same bytes is
"answered as done, and a retry with different bytes is refused" (`node.md` §5.2), and "A retry
never draws a new identity for a mutation whose outcome is uncertain" (`node.md` §5.4). The
remaining gaps are all across attempts, across gateways, or across time.

### 3.3 What is missing

- **G1. Identity across attempts.** The file ID, data key and the ID of a write with no file are
  drawn by the gateway (`gateway.md` §2 step 1). A client's retry after the A row of §3.2 reaches
  some gateway, which draws new ones. In an unversioned bucket the retry overwrites the object with
  identical bytes, a harmless second commit unless another writer came between, in which case the
  stale retry overwrites the newer object. In a versioned bucket it makes a second version. Under
  `If-None-Match: *` it is refused `412` though its own first attempt succeeded. A part's retry
  that commits first can be replaced by the original attempt landing later, and the client's
  completion then lists an ETag the part row no longer has (`InvalidPart`); under SSE-C, where the
  ETag is the MD5 of the sealed segments under a fresh data key (`gateway.md` §2 step 2), the two
  attempts' ETags differ even for the same bytes. S3 has the same hazards; mantle need not.
- **G2. Resume inside one request.** Standard S3 cannot resume within a PUT or a part: "it does not
  make an arbitrary offset within UploadPart a resumable standard request" (audit §16.4). On a slow
  or unstable path the replay cost of a part is the part. A 256 MiB part takes 9.32 hours at
  64 kbit/s (audit §16.2).
- **G3. Durable progress that needs no gateway.** The blocks of an uncommitted part are held only
  by renewals from the gateway that wrote them (`gateway.md` §2, "Deadlines"); a long outage or a
  gateway restart loses them. Committed parts need no renewal (audit §16.1). There is no durable
  state between "renewed blocks" and "a committed part".
- **G4. Client-side persistence rules.** Nothing yet states what mantle's client library journals,
  when it flushes, how it detects a changed source, and how it pins a download's version (audit
  §16.4).
- **G5. Long completions on the listener.** The 200-then-whitespace form exists so a completion
  that takes minutes does not time out (note 05 §4.4); whether mantle ever needs it depends on its
  measured completion latency (§3.6).

### 3.4 Operation identity (closes G1)

**The rule.** Every mutating request carries an operation identity: 128 random bits drawn by the
client before its first attempt, written to the client's journal and flushed before the first byte
is sent (§5.4), and carried unchanged by every attempt, through any gateway, after any restart.
Native requests must carry it. On the HTTP/1.1 listener the operation identity is the
`amz-sdk-invocation-id` header when the request carries one, which aws-sdk-go-v2 sets "to a fresh
UUID per operation" and botocore copies onto every attempt (note 25 §13); without it the request is
handled as S3 does today, a retry being a new write.

**Scope.** An identity is meaningful only with the authenticated principal, the bucket and the key
(and, for a part, the upload and part number). A client chooses the value, so it is untrusted: by
scoping it to what authentication established (`node.md` §4.2 step 3), a forged or reused value can
match only an operation of the same principal on the same key, which is that principal's own
business.

**What the Name range records.** When it commits a version, a part or a completion carrying an
identity, it writes an operation row keyed by (key, principal, identity) naming what it made: the
version's ID and order, or the part's number and file, with the answer the first delivery got. A
later attempt with the same identity finds the row, applies nothing, releases its own file as a
refused write's is released (`metadata.md` §2, "Releasing"), and is answered from the row. A refused
first attempt (a precondition failed, a lock held) records nothing: it changed nothing, and a later
attempt is judged afresh, as a refused write with no file is today (`metadata.md` §2, "Writes with
no file").

**How long a row lives.** A row must outlive every attempt the client may still make. The client
states its resume horizon in the request (the longest it will keep retrying), the server caps it by
a configured maximum, and the row is removed by the collector once the range's time passes the
commit time plus that horizon, judged by the time agreed through the log as marks are (`metadata.md`
§2, "No mark goes"). An attempt arriving after its row is gone is past the horizon the client itself
declared, and is refused with a typed `Expired` on the native protocol; on the listener it is a new
write, as in S3. Rows are bounded: one per committed mutation within the horizon, so by the admitted
mutation rate times the horizon, a quantity admission controls per principal (note 27 §7.1). The
server maximum is set from the bytes the operator gives the operation table, divided by that rate;
it is configuration derived from a budget, not a picked number.

**Proof obligations.**
1. *At most one effect per identity:* no two commits in a range carry the same (key, principal,
   identity) within the horizon. The row and the commit are one range transaction, so the Name
   range's log orders any two attempts.
2. *Same answer:* every attempt that finds the row is answered with the first delivery's answer,
   including its version ID, ETag and checksum.
3. *No stale overwrite:* an attempt that finds its row never replaces what a later write made.
   This is the property S3 lacks.
4. *Ownership:* an attempt's file is released exactly once whether it is refused by the row, by a
   precondition or by the deadline (the existing property test in `metadata.md` §2 extends to it).
5. *Split safety:* the row is keyed by the object key, so a split carries it with the key's rows,
   as marks are carried.
A simulation in the shape of `orphan_sweep.rs` delivers every attempt of every identity at any
step, across gateways, splits and leader clocks that run behind, and checks 1–5 after each step.

### 3.5 Resumable native uploads (closes G2 and G3)

**The model from the IETF draft.** An upload resource whose offset the client reads and to which it
appends: "The server is responsible for persisting the state of the upload resource ... and
updating it as the upload progresses"; "Representation data processed by the upload resource cannot
be removed again and, therefore, the offset MUST NOT decrease. If the server loses any part of the
state, it MUST deactivate the upload resource and reject further interaction with it"; "Using HEAD is
RECOMMENDED, since response content is not required for resumption"; and an append at the wrong
offset is refused, "the server MUST reject the request with a 409 (Conflict) status code ... The
response MUST include the correct offset" [RESUME-12 §4, §4.1.1, §4.3.1, §4.4.2]. tus 1.0 has the same core
(`HEAD` for the offset, `PATCH` with `application/offset+octet-stream`, `409` on a mismatch) and
extensions for creation, checksums, expiration, termination and concatenation [TUS]. Both are HTTP;
mantle takes the semantics, not the syntax, onto its own protocol.

**Mantle's shape.** A native upload is a multipart upload whose part boundaries the server chooses.

1. *Create.* The client opens an upload with its operation identity, the object's length when known,
   the checksum algorithms it will verify, its source's identity (§5.4) and its resume horizon. The
   Name range writes an upload row, exactly as `CreateMultipartUpload` does, plus the identity's row.
2. *Runs.* The gateway streams the body as a PUT does, into a run: a file with its own data key
   (`gateway.md` §1: "an upload's part is a file with a key of its own"). At a checkpoint it ends
   the run: the run's last segment is sealed as last, its remaining blocks recorded, its file
   written, and the file handed to the Name range as the next run of the upload, a part whose number
   the server assigns. From that commit the run is owned like any part: no renewal holds it.
3. *Progress.* After each run commits, the server sends the client a progress frame: the durable
   offset (the sum of the committed runs' plaintext lengths), the run's number and its CRC. This is
   the draft's progress report, carried as a frame where HTTP uses `104` interim responses.
4. *Resume.* On reconnect the client asks for the upload's state; the answer is the durable offset
   and the runs. It appends from that offset, and an append at any other offset is refused with the
   correct offset, as the draft does. The client trusts the server's offset in both directions: a
   server offset below the client's belief means the tail was never durable; above it means a run
   committed whose progress frame was lost.
5. *Complete.* At the end the client sends its full-object checksum; the gateway combines the runs'
   CRCs (`gateway.md` §2, the CRC algebra) and commits the object only on a match, with the
   operation identity's row. A mismatch is `BadDigest` and commits nothing.

**Why runs and not a byte offset into one file.** A file has one data key and seals segment `i`
with nonce `i` (`gateway.md` §1; audit §16.4, "the nonce/index/last-segment binding"). Resuming
inside a file would re-seal, after a crash, segments whose earlier sealing reached a chunk store
under the same key and index. If the resent plaintext differed by a single byte, a changed source or
a client bug, AES-GCM's nonce would be reused with different plaintext, which forfeits its
confidentiality and authenticity. A run that ends at every checkpoint, and a new run with a new key
after every interruption, makes nonce reuse impossible by construction rather than by trusting the
source. The audit states the requirement: "Each changed/replaced part needs a fresh file/data-key
incarnation; resumed ciphertext must be identical to its recorded attempt" (audit §16.4). The
interrupted run's unrecorded tail is left to the sweeps like any abandoned body.

**The checkpoint interval.** Checkpointing a long computation against failures has a classical
optimum: Young's first-order interval `sqrt(2·δ·M)` for checkpoint cost `δ` and mean time between
failures `M` [YOUNG74; DALY06 gives the higher-order form] (**UNVERIFIED** here: the formula is
stated from the literature, the papers were not re-read). With goodput `g`, checkpoint cost `h` in
seconds and interruption rate `λ`, the interval in bytes is `c* ≈ g·sqrt(2h/λ)`, which is the
audit's part-size candidate `p* ≈ g·sqrt(2h/λ)` arrived at independently (audit §16.2). The audit's
caveats hold: evaluate the exact cost at the feasible boundaries, and fit `λ` from interruptions
that restart work, not from packet loss, which QUIC repairs within the stream. Hard constraints
bound it:

- `c ≥ S/10,000` for a known size `S`, so the object's runs fit a file's 10,000 extents
  (`gateway.md` §1; audit §16.1);
- `5 MiB ≤ c ≤ 5 GiB` except the last run, when the object is to carry S3's multipart ETag (below);
- `c ≤` the client's buffer when its source cannot be re-read (a pipe): the client must hold every
  byte past the durable offset until the run commits;
- a run is whole segments: checkpoints fall on 64 KiB plaintext boundaries.

`g`, `h` and `λ` are measured per upload (§4.6); with none measured yet (a first upload on a new
path), the first checkpoint is the smallest legal one and the interval grows as estimates arrive,
since a run too short costs only metadata and a run too long risks replay.

**The ETag.** S3 semantics are carried, so the ETag must be the one S3 would give. An object larger
than 5 GiB is in S3 terms a multipart object, and the runs are its parts: the multipart ETag, the MD5
of the runs' MD5s with their count (`gateway.md` §2; note 05 §4.5), with the run-size constraints
above. An object of at most 5 GiB is in S3 terms a single PUT, whose ETag is the MD5 of the
plaintext under SSE-S3. Then the gateway keeps one MD5 running across runs and records its state at
each checkpoint in the upload row: a checkpoint on a 64 KiB boundary leaves MD5's 64-byte block
buffer empty, so the state is exactly its four 32-bit words and the length, and it is committed in
the same transaction as the run, so it cannot drift from the data (DERIVED). SHA-1, SHA-256 and the
other block hashes the request named are kept the same way; CRCs need no state, since they combine.
This needs digest implementations that expose their state, which aws-lc's and ring's interfaces do
not (**UNVERIFIED**; an implementation obligation, §11). SSE-C uploads need the customer's key at
every resume; the server never stores it, so the client resends it and the gateway checks its MD5
against the upload row.

**Proof obligations.**
1. *Monotone offset:* the durable offset never decreases (the draft's rule) — runs are only added.
2. *Ownership:* every byte below the durable offset is in exactly one committed run the upload row
   references; an aborted, expired or completed upload releases or adopts each run exactly once (the
   existing adoption and release rules, `metadata.md` §2).
3. *No nonce reuse:* no (data key, segment index) seals two plaintexts — a key belongs to one run,
   and a run is written by one attempt.
4. *End-to-end integrity:* the object commits only if its length and full-object checksum match the
   client's, so a wrong resume offset, a changed source or a journal bug produces `BadDigest`, never
   a wrong object.
5. *Bounded state:* runs per upload ≤ 10,000; the row's hash states are a fixed size; open uploads
   per principal are admitted (note 27).
6. *Progress without a gateway:* a run's durability depends on no renewal after its commit.

### 3.6 The HTTP/1.1 listener: S3's resume, served exactly

Stock clients resume only through multipart, so the listener's duty is to make S3's mechanism
exact and its leftovers bounded:

- **Parts survive everything a part survives in S3.** A committed part is a Name row naming its file;
  nothing but abort, completion, replacement or lifecycle removes it (`metadata.md` §2).
- **`ListParts` is exact and paged.** At most 1,000 per page with `NextPartNumberMarker`, listing
  only committed parts, as S3 does ("the returned list of parts doesn't include parts that haven't
  finished uploading", note 05 §4.6).
- **A part's retry is identified where the SDK identifies it.** With `amz-sdk-invocation-id`, the
  original attempt landing after its retry no longer replaces the part (§3.4).
- **Completion answers promptly.** The listener answers a completion with its status when the
  result is ready. It switches to `200` followed by whitespace only when the completion has not
  answered within the shortest read timeout of the clients it serves; whether that ever happens is a
  measurement of completion latency at 10,000 parts on a loaded cell (audit §16.5 sizes the command
  at about 540 KB), and stock clients' read timeouts are their own (botocore's default is
  **UNVERIFIED** here).
- **Abandoned uploads are visible and bounded, not silently removed.** `ListMultipartUploads` shows
  them, and the bucket's `AbortIncompleteMultipartUpload` lifecycle rule removes them (note 05 §4.8
  marks it optional for v1; with the listener on by default and stock tools leaving uploads behind,
  this note recommends it for v1). Mantle does not invent a default expiry for data a client may
  still mean to finish; the space held by open uploads is reported per bucket and principal.
- **A stalled body holds only its reservation**, and is ended with `RequestTimeout` only when the
  node needs the room, choosing the request that has made the least progress for longest
  (`node.md` §4.3). On TCP the server's other levers are the kernel's: `TCP_NOTSENT_LOWAT` bounds
  unsent bytes in a GET's socket so a slow reader holds little kernel memory, and Linux's
  `TCP_USER_TIMEOUT` (RFC 5482) ends a connection whose data stays unacknowledged, a dead peer,
  after a stated time (both **UNVERIFIED** here as to per-OS availability; to be checked against the
  man pages when the listener is built).
- **Download resume is pinned.** A ranged GET carrying `versionId` or a strong `If-Match` is answered
  only from that version; the listener never stitches a retried range from a newer version (audit
  §16.4).

---

## 4. Congestion, variable and poor bandwidth

Two transports carry client bytes. The native protocol runs over QUIC, where mantle chooses the
congestion controller, the pacer, the loss thresholds and the packet size on both ends of every
connection it makes, clients included. The HTTP/1.1 listener runs over the operating system's TCP,
where the client's kernel controls an upload's sending and the server's kernel a download's. Node to
node traffic is native. Everything in §4.1–§4.9 is about the native protocol; §4.10 is the listener.

### 4.1 Which congestion controller, by environment

| Environment | What dominates | Evidence |
|---|---|---|
| Datacenter, within a cell | microsecond RTTs, shallow switch buffers, incast from erasure-coded fan-in | Swift needs NIC timestamps to split fabric from host delay, and Homa needs in-network priority queues and receiver grants (SWIFT, **UNVERIFIED** here; HOMA through note 27 §5.5). Neither is available to a user-space UDP transport on commodity hosts; their lesson that delay, not loss, is the signal carries over |
| WAN between cells and regions | 20–300 ms RTT, random non-congestive loss, deep or shallow buffers | a loss-based flow is bounded by roughly `MSS/(RTT·√p)` [MATHIS97], about 1.2 Mbit/s at 1% loss and 100 ms whatever the link; slates' NewReno and CUBIC "collapsed on lossy high-BDP paths (stalls at 100 Mbit/s with 1 % loss)" (`congestion/mod.rs:10-12`); Copa is "robust to non-congestive loss and large bottleneck buffers" [COPA] |
| Intercontinental, satellite | 300–700 ms RTT (GEO), varying RTT and capacity on LEO | Copa "outperforms other schemes on long-RTT paths" [COPA]; slates' BBRv3 "stalled at 100 Mbit/s, 300 ms, 5 %" (`congestion/mod.rs:12`); LEO-specific behaviour is **UNVERIFIED** in this note (no measurement study was read) |
| Cellular | capacity that varies over hundreds of milliseconds, deep per-user buffers | the bufferbloat case: a buffer-filling sender makes every packet wait behind its standing queue (BBR16 via slates' constrained-links note §2.1); Copa targets a standing queue of a few packets; the Copa paper's cellular results were not read here (**UNVERIFIED**) |
| Laptop on Wi-Fi | link-layer aggregation, bursty ACKs, competing household traffic, address changes | Copa's competitive mode exists for a buffer-filling competitor: it "detects buffer-fillers" and "responding with additive-increase/multiplicative decrease on the δ parameter" [COPA abstract, as fetched]; address changes are §4.9 |
| Thin links, 8–256 kbit/s | serialization: 1,200 bytes take 150 ms at 64 kbit/s and 1.2 s at 8 kbit/s (audit §13.1) | Copa's cost is here: 678 ms ping p99 against NewReno's 289 ms at 64 kbit/s with no loss, "because Copa holds a small standing queue by design (about 1/δ packets, each 146 ms at that rate)" (slates BENCHMARKS, bake-off) |

**What the siblings measured.** slates built five laws to their specifications and ran 57 scenarios
(rate 64 kbit/s–100 Mbit/s × RTT 20–300 ms × loss 0–5 %, buffers, reordering, burst loss, a
bandwidth step, fairness, coexistence with CUBIC), three seeds each, with "the selection rule fixed
before any run"; Copa at δ = ½ was "the only law that never stalled and stayed RTT-fair", with a
goodput shortfall geomean of 1.068 against NewReno's 15.8 and CUBIC's 14.0, and Meta's δ = 0.04
"failed RTT fairness (Jain 0.840)" (`congestion/mod.rs:5-13`; BENCHMARKS). focal, over quinn,
chose Copa on its own 30-path grid: geomean p99 1.130× best and 0.987 of best throughput, against
CUBIC's 1.193× and 0.455 with five stalls (note 07 §4.3). focal found that Copa's velocity, capped
at `cwnd·δ` packets as the paper and slates have it, moves the window by all of itself in a round
trip and overshoots, and halves the stride: "at 100 Mbit/s and 100 ms 95% of the path where the
whole carries 67%" (`focal-wire/src/congestion.rs:52-61`). The audit's caution stands: these are
evidence for those implementations on those grids; the BBR, Reno and CUBIC stalls may be
implementation defects, and "A geomean winner can lose on exactly the links the user requires"
(audit §13.1–§13.2). BBRv3 is still an Experimental draft (draft-ietf-ccwg-bbr-06, 2026-07-06), and
quinn's own BBR is marked "Experimental! Use at your own risk." (`congestion/bbr/mod.rs:19`).
PCC Vivace reports gains over TCP variants and BBR [VIVACE abstract] but was not in either grid.

**Recommendation.** One law, Copa at δ = ½ with focal's half stride, behind quinn's
`ControllerFactory` interface, so the choice is a measurement and not a fork. Mantle runs its own
bake-off on its own mix before the choice is recorded as a decision: Raft appends and votes on the
datagram plane beside chunk writes, repair and snapshots, over the qualification matrix (audit
§13.6; §10 below), with the selection rule written before the first run as slates did. CUBIC
[RFC9438] and BBRv3 run in that grid as candidates, not as fallbacks. A per-path choice of law is
not proposed: two laws on one bottleneck are each other's competitor, and Copa's competitive mode
already covers a buffer-filling neighbour.

### 4.2 Pacing, the burst and the operating packet size

RFC 9002 paces at `rate = N * congestion_window / smoothed_rtt` with "a value for "N" that is small,
but at least 1 (for example, 1.25)" [RFC9002 §7.7]. Copa paces at `2·cwnd/RTTstanding` (slates
`copa.rs:136-143`); under quinn, focal's Copa sets only the window and quinn paces it at five
quarters of the window per smoothed RTT (`focal-wire/src/congestion.rs:25-29`).

What matters on a thin link is the burst, because a burst is what a control frame waits behind.
quinn's pacer bucket holds `window × 2 ms / srtt` bytes clamped between `MIN_BURST_SIZE = 10` and
`MAX_BURST_SIZE = 256` datagrams (`connection/pacing.rs:129-151`). Ten 1,200-byte datagrams are
12,000 bytes: 1.5 s at 64 kbit/s and 12 s at 8 kbit/s (DERIVED). slates' send quantum is one
millisecond of the pacing rate, at least two datagrams and at most 64 KiB, after
draft-ietf-ccwg-bbr §5.6.3 (`congestion/mod.rs:119-132`), and its token bucket refills to one
quantum after idle so a returning sender never bursts an idle period's worth (`pacer.rs:31-41`).

The second lever is the packet size. Strict priority "cannot preempt a bulk packet already
queued/transmitting" (audit §13.1), and Copa holds a standing queue of a few packets at the
bottleneck. So a control frame's wait on a thin path is about `(q + 1)·8s/R` for `q` packets queued
ahead, packet size `s` and rate `R`. Path MTU is the ceiling on `s`, not its target (audit §13.1).
QUIC requires 1,200-byte support of the path and padded Initial datagrams, not 1,200-byte
established packets [RFC9000 §14] (audit §13.1). The operating size is therefore derived (DERIVED):

- each packet carries `s − o_q` bytes of payload for QUIC overhead `o_q` (short header, connection
  ID, packet number, 16-byte AEAD tag, frame header) and costs `s + o_ip` on the wire for UDP/IP
  headers `o_ip` (28 bytes on IPv4, 48 on IPv6);
- the path must carry the admitted bulk rate `b`: `R·(s − o_q)/(s + o_ip) ≥ b`, which gives the
  smallest feasible `s_min = (b·o_ip + R·o_q)/(R − b)` for `b < R`;
- a control frame's wait falls with the bulk packet size, so bulk packets are sent at
  `s_op = min(s_min, PMTU)`, the smallest size that still carries the admitted load, while a control
  message's packet is sized to the message.

Every input is measured (`R` from delivery-rate samples, `b` from admission) or a protocol constant
(`o_q`, `o_ip`). On a fast path `s_min` exceeds the PMTU and the operating size is the PMTU, where
fewer packets also mean fewer per-packet costs, the loopback result of slates' 1,625 → 7,840 Mbit/s
at 9,209-byte datagrams (BENCHMARKS, path MTU discovery). When `b ≥ R` nothing is feasible: the node
reports that the path cannot carry the admitted load and admission lowers `b` (audit §13.4).

**Recommendation.** Adopt slates' quantum rule and the derived operating size. quinn's pacer
constants and its choice of packet size are internal, so this needs a vendored quinn-proto with the
pacer's bounds taken from the controller, upstreamed if accepted (CLAUDE.md §7 keeps vendored crates
gated by `cargo test --manifest-path vendor/Cargo.toml`).

### 4.3 Loss recovery and its timers

- **Estimator and probe timeout.** RFC 9002's `PTO = smoothed_rtt + max(4*rttvar, kGranularity) +
  max_ack_delay` [RFC9002 §6.2.1], with Karn's rule unnecessary because QUIC never reuses a packet
  number. Both siblings implement it exactly (slates `rtt.rs:61-108`; quinn).
- **Backoff, uncapped.** "When a PTO timer expires, the PTO backoff MUST be increased, resulting in
  the PTO period being set to twice its current value" [RFC9002 §6.2.1]. slates holds the backed-off
  timeout "under the larger of the PTO and the initial PTO" (`connection.rs:1491-1514`): once the PTO
  exceeds 666 ms, as it does on any path slower than a few tens of kbit/s, there is no backoff at
  all, and a dead peer is probed once a PTO for as long as the connection lives. Mantle keeps the
  RFC's doubling; the idle timeout, not a cap, bounds how long probing lasts.
- **Idle timeout from the PTO.** "endpoints MUST increase the idle timeout period to be at least
  three times the current Probe Timeout (PTO)" [RFC9000 §10.1]. On a thin or long path that is
  seconds to tens of seconds, which is why focal's fixed 10 s (`transport.rs:48`) is wrong for
  mantle. RFC 9308 notes that "timeouts shorter than 30 seconds can make it harder to handle
  transient network interruptions, such as Virtual Machine (VM) migration or coverage loss during
  mobility" [RFC9308 §3.2].
- **Handshake retransmission.** Before any sample the PTO is `2 × kInitialRtt`, 666 ms
  [RFC9002 §6.2.2]. slates' handshake retransmits from 1 ms, doubling (`endpoint.rs:873, 920`,
  capped at the initial PTO, `:1445-1455`): on a 64 kbit/s path a 1,200-byte Initial is still
  serializing when the first three copies are queued behind it. Reject; take the RFC's.
- **Thresholds that learn reordering.** RFC 9002 declares loss three packets or 9/8 of an RTT past
  a later acknowledged packet [RFC9002 §6.1]. slates widens both from observed spurious losses, after
  RACK's reordering window (RFC 8985), forgets them after 16 quiet recoveries, and bounds its memory
  by count and age (`reorder.rs:1-118`): a reordering path's capacity share went 0.254 → 0.540 with
  no measurable cost elsewhere (BENCHMARKS, adaptive reordering). quinn has fixed thresholds
  (`packet_threshold: 3`, `time_threshold: 9.0 / 8.0`, `config/transport.rs:381-382`) settable per
  connection but not adaptive. Adopt the mechanism, in the vendored quinn-proto.
- **Persistent congestion.** Losses spanning `(smoothed_rtt + max(4*rttvar, kGranularity) +
  max_ack_delay) * kPersistentCongestionThreshold` collapse the window, threshold 3
  [RFC9002 §7.6]. Both siblings keep it, and Copa ignores other losses in its default mode
  (`copa.rs:275-288`).
- **What is charged.** Bytes in flight are packets as sent, header and tag included
  [RFC9002 §B.2, through audit §11.8]. quinn charges the finished packet; slates charges only stream
  payload (`conn.rs:215-222`, `connection.rs:851`), so a one-byte request's 23-byte frame and its
  header and tag are invisible to its controller and pacer (audit §11.8). Mantle charges packets.

### 4.4 Path MTU

Datagram PLPMTUD [RFC8899] as RFC 9000 §14.3 applies it: start at 1,200, probe upward with padded
ack-eliciting packets, `MAX_PROBES` lost probes of a size bound it, and "This timer has a period of
600 seconds" before a new search [RFC8899 §5.1.1, §5.1.2]. Two details from slates' measurements
are worth taking (`pmtud.rs:1-21`; BENCHMARKS): a completed search's raise rechecks only the smallest
size that failed, since restarting from the peer's limit cost 36 lost probes each raise and raised
the 64 kbit/s ping p99 from 483 to 587 ms; and a probe refused locally (`EMSGSIZE`) gives back its
packet number, since the gap it left cost burst-loss tails. Probe losses are never congestion
signals [RFC9000 §14.4]; quinn already excludes them (audit §11.8). quinn's default search ceiling
is 1,452 bytes (`MtuDiscoveryConfig::upper_bound`, `config/transport.rs:745-753`); mantle sets it
from the interface MTU the OS reports (jumbo frames in a cell, macOS's 9,216-byte UDP cap on
loopback, which slates found) and lets the search confirm it.

### 4.5 Windows: bandwidth-delay product, memory, and quinn's gap limit

`node.md` §3.3 already sizes a peer's receive window as `min(BDP, share)` and each class's as
`min(stream budget, connection share, node remaining budget, admitted sink capacity)` (audit §13.4),
and reads received bytes only into budgeted stages. Two additions:

- **The per-stream ceiling quinn imposes.** quinn's stream assembler keeps what arrives out of order
  as separate chunks and fails the connection with "too many gaps in stream buffer" when more than
  `MAX_CHUNKS = 1024` remain after defragmentation (`connection/assembler.rs:361`,
  `connection/streams/recv.rs:81-83`). With a window of `W` bytes and frames of at least `d` bytes,
  the most disjoint chunks a loss pattern can leave is about `W/(2d)` (every other frame lost), so
  `W ≤ 2,048·d` cannot reach the limit (DERIVED; a bound to confirm under a fuzzed loss pattern,
  since frames of one stream can be smaller than a datagram when streams share packets). That is
  about 2.4 MB at 1,200-byte frames and 18 MB at 9,000. focal reached the failure with a 10 MiB
  window and settled on 1 MiB (`transport.rs:49-62`). A path whose BDP exceeds the ceiling carries
  one transfer on several streams, or the vendored assembler is changed; parallel streams do not add
  bandwidth (audit §13.4), they only remove a per-stream window limit.
- **Clients size their windows the same way.** The client library advertises receive windows from
  its own memory budget and the path's measured BDP, so a laptop downloading over Wi-Fi holds no
  more than its window, and a GET's server side holds no more than the client's credit plus its own
  admitted window (`gateway.md` §3).

### 4.6 Adapting part size, checkpoints and concurrency online

The audit derives the part-size candidate and states what an optimizer must measure (audit §16.2);
note 27 §6.1 sets the block window from the upload's entitled rate. What this note adds is how each
input is estimated and when a plan may change.

- **Goodput `g`**: durable plaintext bytes per second, counted at the B and N points of §2, never at
  the transport ACK, over the last several checkpoints.
- **Interruption rate `λ`**: events that restart work (a connection lost, a migration that failed, a
  gateway that answered `Expired`) per second of exposure. Interruptions are counted events over an
  exposure time, so the maximum-likelihood rate is `k/T` and its exact confidence interval is the
  Poisson (Garwood) interval from the chi-square distribution; with no event yet, the upper bound
  alone is informative and drives the plan toward short runs (DERIVED). Packet loss is not `λ`: QUIC
  repairs it within the stream (audit §16.2).
- **Checkpoint cost `h`**: measured per run as the time and the metadata work a run commit takes.
  Whether `h` is a cost in time depends on the pipeline. If the next run streams while the last one
  commits, which the gateway should arrange as it overlaps block chains (`gateway.md` §2), a
  checkpoint costs metadata work, charged to the principal's share (note 27), and no wall time; the
  time-optimal interval is then the smallest the principal's metadata share admits, `c ≥ g/r_meta`
  for an admitted run-commit rate `r_meta`. Young's interval of §3.5 applies where a commit stalls the
  stream, as on a client whose source cannot read ahead (DERIVED). Both are evaluated, and the larger
  constraint binds.
- **When a plan changes.** A new interval or window applies to runs and blocks not yet begun, never
  to bytes already sealed (audit §16.2: "Live adaptation must never reinterpret committed bytes").
  It changes when the confidence interval of the predicted saving excludes zero and exceeds the
  measured cost of switching; the confidence level is a stated policy, not a picked multiplier (audit
  §16.2 asks that hysteresis be derived). Each change records its inputs and reason.
- **Concurrency.** One connection per client and gateway carries all of a client's transfers, so
  they share one congestion controller and do not take a bottleneck from other users by opening
  more flows. Concurrency inside it serves three purposes and no other: covering the per-stream
  window limit of §4.5, covering a block chain's latency (note 27 §6.1), and spreading an upload over
  gateways and disks (note 27 §6.2). For stock clients over HTTP/1.1, concurrency is the client's
  (`max_concurrent_requests` 10 by default in the CLI, [AWS-CLI]), and each TCP connection has its
  own controller; admission (note 27) is what keeps that fair.

### 4.7 Deadlines from progress, not the wall clock

The rule `node.md` §3.8 states between nodes, "bytes remaining over the measured delivery rate plus
the path's tail delay and the remote side's measured service time", applies to every wait in the
transfer path, and focal's `carried` is a worked form of it: a wait is charged, each period, what
the connection sent and did not lose, and gives up only after a period in which less than a datagram
moved, or after the peer had the whole request and a period to answer it (`transport.rs:150-197`).
For transfers this gives four distinct clocks, each with its own reclaim:

1. **Transport liveness.** The connection is alive while ACKs arrive; QUIC's idle timeout, at least
   three PTOs [RFC9000 §10.1], ends a dead one. Keep-alive PINGs run more often than the NAT mapping
   lifetime the client measures (a rebinding the server sees as a new port is the signal), starting
   from RFC 9308's guidance that "30 seconds might be a suitable value for the public Internet when a
   NAT is on path" and that sending more often wastes "unacceptable power usage for power-constrained
   (mobile) devices" [RFC9308 §3.2].
2. **Request progress.** An exchange waits while its bytes move, as `carried` does, with a ceiling
   from the caller's budget.
3. **Memory and work held by a stalled upload.** When an upload has made no byte progress for the
   connection's idle timeout, or the node needs its reservation (`node.md` §4.3, least progress for
   longest first), the gateway closes the current run at its last whole segment, commits it as a
   checkpoint, and releases the upload's buffers, coder and renewals. Its memory comes back within one
   run commit; its data stays.
4. **Durable progress held by an abandoned upload.** Committed runs and parts stay until the client's
   declared resume horizon passes, the client aborts, or the bucket's lifecycle rule removes the
   upload. This is a policy over storage, which the operator sees and bounds, not a timeout.

A multi-day upload on a slow link therefore completes as long as each run commits, and a stalled one
gives back its memory in one run commit (DERIVED). The cost of interruption is bounded by one run's
replay, the quantity §3.5 and §4.6 optimize.

**Retry pacing.** Reconnects and retried exchanges use exponential backoff from the measured PTO,
capped by the idle timeout, with focal's equal jitter: "drawn uniformly between half of `delay` and
the whole of it", where "Half the pause is kept whole because the pause has a meaning of its own"
(`peers.rs:1197-1212`). focal's defaults, a 10 ms retry backoff, 5 s calls and 2 s cooldowns
(`peers.rs:52-61`), are not adopted; the audit's arithmetic shows a 5 s call failing a healthy 128 KiB
append at 64 kbit/s, which needs 16.384 s (audit §13.5).

### 4.8 Brownouts, reconnects and Raft

A brownout is the path's rate falling far below what was admitted, for seconds to minutes. What keeps
consensus stable through it:

- **Control is not behind bulk.** Votes, heartbeats and their answers ride the datagram plane, sealed
  under exporter keys and rate-limited per RFC 8085 (`node.md` §3.4), so a bulk transfer in loss
  recovery cannot delay them in its congestion window, as RFC 9221 datagrams would (note 25 §6).
- **Bulk at the sender is bounded by the path.** The bulk class keeps no more queued than one tail
  round trip of the path's measured rate (`node.md` §3.8). When the rate collapses, that bound
  collapses with it and the admitted bulk credit shrinks; durable work already admitted is finished
  or checkpointed, not abandoned (audit §13.4).
- **Election timing from measured tails.** slates derives the election base as
  `ELECTION_MARGIN × max(tail over the voter paths, heartbeat)` with `ELECTION_MARGIN = 10`
  (`cluster/src/timing.rs:52, 175-213`), focal-timing does the same with a tick stretched from the
  slowest voter path, never the heartbeat (note 07 §5.1). Ten is Raft's "order of magnitude" between
  broadcast time and election timeout [RAFT14 §5.6]. The tail must include the durable-ack time, not
  only the network RTT (audit §11.7).
- **Reconnects do not storm.** Reconnection backs off with equal jitter (§4.7); a re-dialing peer's
  new connection replaces its old one under the same certificate, as slates' demultiplexer does
  (`demux.rs:12-19`); epochs and certificates are revalidated on reconnect (`node.md` §3.8).
- **What cannot be promised.** A path that cannot carry the admitted append rate cannot sustain it;
  "Admission and deployment qualification must say so" (audit §13.5).

### 4.9 Migration, blocked UDP, 0-RTT and resumption

- **Migration.** QUIC connections are named by connection IDs, and "Only clients are able to migrate
  in this version of QUIC" [RFC9000 §9]. On confirming a new client address the server "MUST
  immediately reset the congestion controller and round-trip time estimator for the new path to
  initial values ... unless the only change in the peer's address is its port number", and until the
  address is validated it sends no more than three times what it received [RFC9000 §9.4, §8.1].
  quinn allows client migration by default (`ServerConfig::migration`, "Improves behavior for clients
  that move between different internet connections or suffer NAT rebinding. Enabled by default.",
  `config/mod.rs:288-295`) and keeps the old path's state only for an IPv4 address whose IP is
  unchanged (`connection/mod.rs:3066-3087`), stricter than the RFC allows on IPv6 port-only changes.
  slates' dialect has no path validation and no migration; its constrained-links note lists them as
  "missing" (§5.4 of that note). The client library watches the OS's interface changes and rebinds its
  socket so the connection migrates; the operating-system interfaces for that notification (the
  Network framework's path monitor on macOS, rtnetlink address events on Linux, IP interface change
  notifications on Windows) are named here without their documentation read (**UNVERIFIED**).
- **When migration is not enough.** A laptop that moves to a network that blocks UDP loses QUIC
  entirely. How often mantle's clients meet such networks is not known (§11). The candidates are the
  native protocol's frames over TLS on TCP, the same application protocol with head-of-line blocking
  accepted, or the HTTP/1.1 listener with S3's semantics, which loses the native resume. The
  operation identity and the upload row (§3.4–§3.5) are server state, so either route can finish an
  upload begun on the other if both carry them.
- **0-RTT.** RFC 8470's rule for HTTP is the right one for mantle's protocol: clients "MAY send
  requests with safe HTTP methods ... in early data when it is available and MUST NOT send unsafe
  methods (or methods whose safety is not known) in early data", and a server that will not risk it
  answers so that the client retries after the handshake [RFC8470 §4, §5.2]. RFC 9001 calls disabling
  0-RTT "the most effective defense against replay attack" (note 25 §5). focal disables it on both
  sides (`transport.rs:265, 286`), and `node.md` §3.2 does for nodes. The saving is one RTT on a
  reconnect. The cost of allowing it for reads is that a replayed read still costs the server its
  work and charges the principal's share. Off at first; reconsidered only with a measurement of
  reconnect frequency and a per-stream signal of which requests arrived in early data (quinn exposes
  `accepted_0rtt`, `connection/mod.rs:1368`).
- **Resumption without early data.** TLS session resumption skips the certificate chain and its
  verification. On an 8 kbit/s path a few kilobytes of chain are several seconds, and slates found
  operators' chains exceed one datagram (`flight.rs:3-10`). Resumption tickets are on for clients;
  they carry no replay exposure because no application data rides them.

### 4.10 The HTTP/1.1 listener's TCP

On the listener the kernel's TCP controls the bytes. An upload's congestion control is the client's
kernel's, a download's the server's. On Linux the server may choose a controller per socket, but an
unprivileged process may choose only from `tcp_allowed_congestion_control` (slates' constrained-links
note §2.2, citing the kernel's ip-sysctl documentation). What the server controls is admission and
backpressure: a body is read only into reserved slots, so TCP's receive window closes on a client
the node cannot yet take (`node.md` §4.3), and `503 SlowDown` before `100 Continue` refuses a body
that was never going to be stored (`node.md` §4.5). TCP has no migration: a laptop changing networks
loses every listener connection, and a stock client restarts its in-flight requests; only multipart
keeps its committed parts. That is S3's contract, and the reason the native client exists.

---

## 5. Sudden power loss

### 5.1 Storage nodes: what an acknowledgement means

A chunk is answered `Stored` only after its batch's records and index frame are written and the
volume is flushed with the platform's full flush (`fdatasync`, `F_FULLFSYNC`, `FlushFileBuffers`),
and some answers wait for a later frame's flush as well, so that the frame of an acknowledged delete
or of a write into a newly opened segment is never the last one (`chunk-store.md` §4). After a power
cut, recovery takes the newer valid superblock, replays index frames to the end of the log, tells a
torn tail from damage by whether a later frame follows, and does not drop the last batch's records
on the evidence of a failed checksum: "Misclassifying a crash as damage costs repair; the reverse
loses data" (`chunk-store.md` §6). The Raft log answers a frame only once its confirmation is
durable, so an unconfirmed frame was never acknowledged, and a confirmed frame that no longer reads
marks its groups `Uncertain` or `Damaged` and repairs them from peers, never truncating silently
(`raft-log.md` §6).

What a client sees: a chunk write cut by power loss before `Stored` is a refused write, and the
gateway takes the chunk to another volume (`gateway.md` §2); one cut after `Stored` is durable. A
range whose leader lost power elects another, and a command whose outcome the gateway could not
learn is re-sent in a new session and answered as the first was (`gateway.md` §2). No acknowledged
byte or command is lost to a power cut on any number of nodes, provided every replica the scheme
requires flushed before the answer (CLAUDE.md §6) and each device honours its flush, which is note
29's subject (device classes, flush verification) and note 28's (power-aware writes, devices with
power-loss protection).

**Whole-cell power loss.** Because the answer waits for every required replica's flush, a power cut
to every node at once loses nothing acknowledged; it costs the in-flight requests, whose clients see
the unknown-outcome case of §2, and recovery time, which is bounded by each log's live bytes over its
device's sequential read rate (`raft-log.md` §6) and each volume's checkpoint plus one log
(`chunk-store.md` §5). Deadlines and marks are judged by the range's agreed time, recorded in the log
(`metadata.md` §2), so a restart with clocks that jumped does not release a referenced file: it can
only make an honest handover late.

### 5.2 Gateways

A gateway holds no durable state of its own. A power cut is a crash: its in-flight requests fall into
the rows of §3.2, its sessions expire by the log's time (`node.md` §5.3), its renewals stop and the
sweeps reclaim what it had not handed over. With native resumable uploads (§3.5), committed runs
survive; the open run's recorded blocks are lost to the block sweep and replayed from the last
checkpoint, which bounds a gateway's power cut to one run of replay per open upload.

### 5.3 Clients

The client library's journal is a small log of its own, written under CLAUDE.md §6's rules: the
platform's full flush, the parent directory flushed after a create or rename, a checksum on every
record verified on read, a mismatch a typed error.

| Record | Written when | Flushed before | Why |
|---|---|---|---|
| Operation: identity, principal, bucket, key, kind, resume horizon | the operation is begun | its first byte is sent | without it a restarted client draws a new identity and §3.4's guarantee is gone |
| Source: path, length, modification time, the OS's file identity (inode and device, or the Windows file ID), and for each committed run its CRC | the upload is created; a run's CRC when its progress frame arrives | the upload's create request (identity part); lazily (CRCs) | a resume must detect a changed source; the CRCs let a client verify any run it doubts |
| Upload: upload ID, checksum algorithms, SSE-C key fingerprint | the create is answered | the first append | the upload is found again after a restart |
| Progress: durable offset, run number | a progress frame arrives | never required | the server is authoritative for the offset; losing it costs one state query on resume |
| Download: object version, ETag, verified prefix length, destination temporary file | the first range is answered | the prefix length after the data it covers is flushed | a resume continues only the same version, from bytes known good |

**Resume after a client reboot.** Read the journal; for each open operation, ask the server for its
state. A committed operation's record answers it; an upload's state gives its durable offset and
runs. Check the source: same length, same modification time and file identity, and, if the client
doubts it, the CRC of the runs it re-reads. A changed source is a new operation, never a resume; the
old upload is aborted. The full-object checksum at completion catches what these checks miss
(§3.5, obligation 4).

**Downloads.** Bytes go to a temporary file named in the journal; each range is verified against the
server's per-block CRC (native) or the object's checksum when complete; the verified prefix is
journaled only after the data is flushed; the finished file is flushed and renamed, and the directory
flushed. A resume asks for the rest of the same version, and the server refuses rather than
substituting another (audit §16.4).

**Clocks across sleep.** A client's local deadlines do not advance while the machine is suspended if
they are measured by a clock that stops during suspend. Linux's `CLOCK_MONOTONIC` does not count
suspended time and `CLOCK_BOOTTIME` does (clock_gettime(2); **UNVERIFIED** here which one Rust's
`Instant` uses on each platform). Whatever the clock, the server's deadlines run by the range's
agreed time and do not pause for the client. After a wake the client therefore treats every session,
connection and open run as possibly gone and reconciles with the server, never trusting a local
timer that says little time has passed.

### 5.4 Laptops

A laptop is a client, and in the first step of scale it is also the whole cell.

- **Battery death as a node.** Every acknowledgement waited for `F_FULLFSYNC` (macOS) or the
  platform's full flush, so a dead battery loses nothing acknowledged and the next start recovers as
  §5.1 describes. The cost is the flush: about 4.7 ms on the development machine whatever its size,
  which the volume's group commit amortizes over a batch (`chunk-store.md` §4).
- **Battery death as a client.** The journal rules of §5.3. Bytes past the durable offset are
  replayed, at most one run.
- **Low battery and power state.** The OS's power notifications can make the node stop admitting new
  writes and drain what it holds while it still has power. How mantle reads them and what it does is
  note 28's (power-aware writes); this note only requires that a drain end each open upload at a
  checkpoint, so the replay after power returns is the drained state's, not a run's.
- **Sleep as a node.** A one-node cell asleep for hours wakes with its range time far ahead: sessions
  expire, handover deadlines of in-flight PUTs pass, and those PUTs' blocks are swept. Committed runs
  are not affected. Raft timers measured on a clock that stops in sleep do not fire spuriously, and a
  one-voter range elects itself regardless.

---

## 6. slates and focal: what mantle adopts, adapts or rejects

### 6.1 Mechanism by mechanism

| Mechanism | Where | Verdict | Why |
|---|---|---|---|
| Packet protection with `rustls::quic`, header protection, AES-256-GCM | slates `flight.rs`, `keys.rs`; quinn | **adopt through quinn** | quinn implements RFC 9001 protection; slates' own dialect is not Quinn-compatible (audit §11.8) |
| 1-RTT key update at the AEAD's confidentiality limit, integrity budget across keys, at most three key sets | slates `keys.rs:1-30` | **adopt the requirement** | RFC 9001 §6.6: "For AEAD_AES_128_GCM and AEAD_AES_256_GCM, the confidentiality limit is 2^23 encrypted packets"; a multi-day upload sends far more; verify quinn updates keys before the limit (a test, §10) |
| Handshake flights fragmented to the path floor, Handshake level sealed | slates `flight.rs:1-50` | **adopt through quinn** | quinn's CRYPTO frames do this; slates sealed the Handshake level only on 2026-09-30 after certificates crossed in clear (`flight.rs` module doc) |
| Handshake retransmit starting at 1 ms, doubling to the initial PTO | slates `endpoint.rs:873, 920, 1445-1455` | **reject** | RFC 9002 §6.2.2 starts at `2 × kInitialRtt`; on a thin path early copies queue behind the first (§4.3) |
| Connection IDs from the TLS exporter; one socket per plane; a re-dial replaces the session under the same certificate | slates `demux.rs:1-40` | **adapt** | the replacement-by-certificate rule and per-certificate quotas are right for reconnect storms; quinn provides connection IDs and routing |
| Streams that carry the request kind and the priority class in the stream ID | slates `streams.rs:5-10, 43-55` | **reject the class-in-ID** | a peer must not choose a privileged class by setting a stream ID (audit §13.3); mantle decides class by message kind and the sender's role (`node.md` §3.1, §3.6) |
| Stream concurrency credited, closed streams implicit by sequence | slates `streams.rs:12-38` | **adopt through quinn** | quinn's stream limits are RFC 9000 §4.6; slates' record of an idle-peer RTT inflation (6.6 s PTO on 100 ms) is a test case to keep |
| Absolute-offset credits, a window kept ahead of what the application consumed, autotuned when a window is consumed within two RTTs, up to a ceiling from memory | slates `flow.rs:1-45` | **adapt** | the law is Chromium's and sound; mantle's ceiling is the node's budget share (`node.md` §3.3) and the quinn gap limit (§4.5) |
| A connection credit reserve for the classes above a stream's | slates `flow.rs:8-13`, `connection.rs:60-75` | **adopt** | a bulk stream spent the last connection credit and a control ping waited 68 ms of a 40 ms path for `MaxData` (slates bug record cited in `flow.rs`); quinn has one connection window, so the reserve becomes the node's own accounting on top |
| Whole-exchange retention: the request copied whole, replies accumulated whole | slates `connection.rs:497-520`; audit §11.8 | **reject** | an 8 KiB receive ceiling accepted a 32 MiB request (audit §11.8); mantle streams bulk through reservations (`node.md` §3.2) |
| Multi-range ACKs bounded by a frame budget; received set bounded by ACK-of-ACK and a range cap | slates `conn.rs:36-130` | **adopt through quinn** | RFC 9000 §13.2.4; slates measured an unbounded receiver set (353 entries with a 40 bound) before the cap; check quinn's bound under a pure-receiver workload (§10) |
| RTT estimator and PTO, RFC 9002 | slates `rtt.rs:61-120`; quinn | **adopt** | protocol constants |
| PTO backoff held under `max(PTO, initial PTO)` | slates `connection.rs:1491-1514` | **reject** | RFC 9002 §6.2.1 doubles without a cap; the idle timeout bounds probing (§4.3) |
| Loss detection by packet and time thresholds; persistent congestion | slates `conn.rs:364-414`, `connection.rs:1530-1545`; quinn | **adopt** | RFC 9002 §6.1, §7.6 |
| Adaptive reordering tolerance, RACK-style, bounded memory | slates `reorder.rs` | **adopt (vendored quinn-proto)** | 0.254 → 0.540 capacity share on a reordering path, neutral elsewhere (BENCHMARKS) |
| Copa δ = ½, integer arithmetic, RFC 9002 §7.8 bounding only growth | slates `congestion/copa.rs`; focal `congestion.rs` | **adopt focal's port** | the law plugs into quinn; focal's half stride measured better; mantle's own bake-off decides (§4.1) |
| Copa's loss signal: persistent congestion only; competitive mode's 1/δ AIMD | slates `copa.rs:246-288` | **adopt** | as the paper and mvfst specify (module doc `copa.rs:1-24`) |
| Pacer quantum: 1 ms of the rate, floor two datagrams, cap 64 KiB; refill to one quantum after idle | slates `congestion/mod.rs:119-132`, `pacer.rs` | **adopt (vendored quinn-proto)** | quinn's floor of ten datagrams is seconds on a thin link (§4.2) |
| Bytes in flight counting only stream payload | slates `conn.rs:215-222`, `connection.rs:851` | **reject** | RFC 9002 §B.2 counts "the QUIC header and Authenticated Encryption with Associated Data" overhead; quinn does |
| DPLPMTUD with a raise that rechecks only the last failed size, refused probes returning their number, probes giving no RTT sample, black-hole fallback after three large losses | slates `pmtud.rs:1-44`; BENCHMARKS | **adopt the refinements**; quinn's search otherwise | each is a measured fix (§4.4) |
| Fixed idle timeout 10 s, keep-alive idle/4 | focal `transport.rs:48, 223-225` | **reject the constants** | idle ≥ 3 PTO [RFC9000 §10.1]; keep-alive from the NAT lifetime (§4.7) |
| Per-stream window ceiling of 1 MiB | focal `transport.rs:49-62` | **adapt** | the cause is quinn's 1,024-chunk limit; derive the ceiling from it (§4.5) |
| A 1 Gbit/s × 100 ms reference path sizing the content lane | focal `transport.rs:63-75` | **reject** | windows come from each path's measured BDP and the node's budget (`node.md` §3.3) |
| Progress-charged waits (`carried`) | focal `transport.rs:150-197` | **adopt** | the shape of every transfer deadline (§4.7) |
| Exchange-time estimator that doubles per abandoned exchange, at most six times | focal `peers.rs:229-245` | **adapt** | the doubling follows RFC 9002's backoff; the cap of six is not derived; bound it by the caller's budget instead |
| Equal-jitter pauses | focal `peers.rs:1197-1212` | **adopt** | prevents peers that lost one node from re-dialing it in step |
| Pool defaults: 5 s timeout, 10 ms retry backoff, 2 s cooldown, two exchanges per peer | focal `peers.rs:30-62` | **reject** | fixed values a slow path fails (audit §13.5), and two exchanges per peer throttles thousands of ranges (note 07 §4.6) |
| 0-RTT disabled both sides; TLS 1.3 only; mutual TLS on AWS-LC | focal `transport.rs:236-290` | **adopt** | `node.md` §3.2, §3.6 |
| Admission of handshakes and connections per identity | focal `admission.rs` (note 07 §4.1) | **adapt** | bounds from the node's budget, not 128 connections |
| Election base and span from measured voter-path tails, `ELECTION_MARGIN = 10`; pipelining window from measured tail over heartbeat | slates `cluster/src/timing.rs:52-57, 175-235` | **adopt the law** | RAFT14 §5.6; include durable-ack time in the tail (audit §11.4, §11.7) |
| Datagram seal with a counter-zero sealer per key | slates `seal.rs:8-24`, `enrollment.rs` | **adapt** | mantle keys each epoch from a fresh connection's exporter, so a restart never reuses a nonce (`node.md` §3.4; audit §11.8) |
| Migration and path validation | absent in slates (`session.rs`, constrained-links note §5.4); quinn has them | **requires quinn** | the laptop case (§4.9) |

### 6.2 The audit's blockers, restated with their status

Audit §11.8 lists what blocks adopting either sibling's transport as it stands:

1. *slates retains whole exchanges.* Unresolved in slates; avoided in mantle by the streaming
   reservations of `node.md` §3.2–§3.3.
2. *slates' congestion accounting counts payload only.* Still so at `fdaa51d2` (`conn.rs:217`); not
   shared by quinn.
3. *focal's credit multiplication*, about 144 MiB of window per connection and 18 GiB over 128
   connections outside any budget. Avoided by `node.md` §3.3's windows from memory.
4. *focal allocates a whole frame before admitting its handler.* Avoided by reading a frame's
   header into a fixed buffer and reserving before the body (`node.md` §3.2).
5. *Restart and key authority*: a counter-zero sealer reused under an unchanged key would reuse
   nonces. Avoided by exporter keys per connection epoch (`node.md` §3.4).

Two more come from this note: quinn's ten-datagram pacing floor (§4.2) and its 1,024-chunk stream
limit (§4.5). Both are inside quinn-proto, which is why §9 proposes a vendored copy.

### 6.3 Which QUIC beneath mantle's own protocol

The owner's decision is a protocol of mantle's own over QUIC, "as ../slates does". It has two
layers, and the evidence treats them differently.

- **The application protocol** is mantle's own in either case: the frames, the exchanges, the
  classes and who may use them, the credits and reserves, the operation identity, the upload and
  progress frames of §3.4–§3.5, S3's semantics. slates' application layer is the model: one
  connection per peer, bidirectional exchanges a request and a reply each, priority classes with a
  credit reserve for the classes above, absolute credits, typed refusals. `node.md` §3 already takes
  that shape, minus the class-in-stream-ID.
- **The QUIC beneath it** can be the RFC 9000 wire through quinn, as focal does, or a private
  dialect, as slates does. quinn brings migration and path validation (§4.9), key update, idle
  timeout and stateless reset, connection IDs, ECN, segmentation offload, an interoperable wire that
  standard tools can decode, and a congestion-controller interface Copa already plugs into. Its
  costs are the pacing floor and the chunk limit, both small changes to a vendored copy. slates'
  dialect has every refinement this note wants to adopt, measured, and lacks migration, path
  validation, packet-byte accounting and streaming exchanges; focal assessed it as missing "idle
  timeout/close/reset/migration/key update" at its earlier cut (note 07 §4.7), of which key update has
  since been built (`keys.rs`).

**Recommendation.** Mantle's own application protocol, under its own ALPN, over RFC 9000 QUIC
through a vendored quinn-proto carrying slates' measured refinements (pacing quantum, adaptive
reordering, the PMTU raise) as patches offered upstream. This is a protocol of mantle's own end to
end, as the owner asked, while the laptop's migration and the wire's tooling come from the standard.
What would overturn it is a measurement: if mantle's own grid (§10) finds quinn's loss recovery or
pacing, patched, losing to slates' dialect on the paths mantle must serve, the dialect's missing
pieces become the cheaper work.

---

## 7. Stepped complexity: what each step newly needs

| Step | What is new on the path | What it newly needs | What it does not yet need |
|---|---|---|---|
| **1. One laptop, local clients** (loopback, or the LAN over Wi-Fi) | the client and node on one machine or one LAN hop; battery and sleep; Wi-Fi to cellular changes for LAN clients that roam | the journal (§5.3); operation identity (§3.4); resumable uploads with server-chosen runs (§3.5); the HTTP/1.1 listener on by default (§3.6); QUIC migration for roaming clients (§4.9); full flush on every answer (§5.4) | congestion control beyond the defaults' safety on loopback (the in-process path skips the socket, `node.md` §5.5); PMTU search beyond loopback's cap; the datagram plane (one voter) |
| **2. A node serving remote clients** | WAN or cellular clients; thin and lossy last miles; NATs | Copa with the derived pacing quantum and operating packet size (§4.1–§4.2); PTO-derived idle and keep-alive from the measured NAT lifetime (§4.7); windows from the client's BDP and the node's memory (§4.5); progress deadlines and memory reclaim at checkpoints (§4.7); online plan from `g`, `λ`, `h` (§4.6) | node-to-node transport |
| **3. A cell** (nodes on one datacenter network) | node-to-node chunk fan-out and incast; Raft replication; jumbo frames | the datagram plane for control (`node.md` §3.4); PMTU search to the interface MTU (§4.4); per-stream ceilings from quinn's chunk limit (§4.5); election timing from measured durable-ack tails (§4.8); reconnect with equal jitter (§4.7) | path migration between nodes (servers do not migrate, [RFC9000 §9]) |
| **4. A region over WAN** (cells or zones 20–100 ms apart) | random non-congestive loss; deep and shallow buffers; brownouts | adaptive reordering (§4.3); the bulk queue bounded by one tail round trip of a collapsed rate and shrinking admitted credit (§4.8); several streams per transfer where BDP exceeds a stream's ceiling (§4.5) | anything cross-region in the write path |
| **5. The global fleet** (intercontinental, satellite, lossy) | 150–700 ms RTT; 8–256 kbit/s links; long outages | the full qualification matrix (§10); thin-link operating sizes and honest infeasibility reports (§4.2, audit §16.2's 8.7 years at 64 kbit/s for 2 TiB); election timing that stretches with tails and stays safe (audit §13.5) | — |

Each step adds mechanism only where its path adds a cause. A laptop serving itself never runs a
congestion experiment; a cell never configures a satellite's operating size; the laws are the same
code and the values come from what each step measures.

---

## 8. Design records this note bears on

- `node.md` §4.1: replace "HTTP/1.1 and HTTP/2 through hyper" with the HTTP/1.1 listener, on by
  default, and the native protocol for clients; remove the HTTP/2 settings; record S3-PROBE as the
  evidence.
- `node.md` §3: name the vendored quinn-proto and its patches; the operating packet size; the
  per-stream ceiling from quinn's chunk limit; idle timeout ≥ 3 PTO.
- `gateway.md` §2: the operation identity on every write; runs and checkpoints for native uploads.
- `metadata.md` §2: the operation row, its horizon and its removal; the upload's runs and hash
  states.
- Note 25 §2: its HTTP/2 material no longer applies to any path mantle serves.

---

## 9. Design proposal

Each decision names its sources and the measurement or test that confirms it. No value below is
picked: each is a protocol constant, a measurement, or derived from them.

**D1. Client-facing protocols.** The native protocol over QUIC for mantle's client library and CLI;
the HTTP/1.1 S3 listener on by default; no HTTP/2 or HTTP/3. *Sources:* the owner's decision;
S3-PROBE (S3 negotiates only HTTP/1.1 and advertises no HTTP/3). *Confirms:* s3-tests and the AWS
CLI, boto3 and rclone against the listener with default settings (note 05 §15).

**D2. Operation identity.** Every mutation carries a client-drawn 128-bit identity, journaled and
flushed before the first attempt; the Name range records (key, principal, identity) with the first
answer and answers later attempts from it; the listener uses `amz-sdk-invocation-id`. The row lives
for the client's declared horizon capped by the server's maximum, which is the operation table's
byte budget over the admitted mutation rate. *Sources:* §3.4; `gateway.md` §2; `metadata.md` §2;
note 25 §13. *Confirms:* the five obligations of §3.4 checked after every step of a generated
simulation in the shape of `orphan_sweep.rs`, with removing the row, its scope or its horizon check
each made to fail it; an end-to-end test that kills the gateway between N and A (§2) and retries
through another gateway in a versioned bucket, expecting one version.

**D3. Resumable native uploads as server-cut runs.** §3.5's protocol: create, runs each a file with
its own key handed to the Name range at a checkpoint, progress frames with the durable offset,
resume at the server's offset with `409`-style refusal of any other, completion on the full-object
checksum. *Sources:* RESUME-12 §4–§4.4; TUS; `gateway.md` §1; audit §16.1, §16.4. *Confirms:* the six
obligations of §3.5 under simulation with interruptions at every frame; a fault test that changes
the source between attempts and expects `BadDigest`, never a committed object; a test that the ETag
equals S3's for both a ≤ 5 GiB object (MD5 across runs) and a larger one (multipart form).

**D4. Checkpoint interval.** `c` is the larger of: the smallest interval the principal's
metadata share admits, `g/r_meta`, when commits overlap the stream; Young's `g·sqrt(2h/λ)` when a
commit stalls it; `S/10,000`; S3's 5 MiB part floor where the multipart ETag applies; and at most the
client's buffer for non-rereadable sources. `g`, `h`, `λ` measured per upload, `λ` with its Poisson
interval. *Sources:* YOUNG74, DALY06 (**UNVERIFIED** here), audit §16.2, note 27 §6.1.
*Confirms:* the audit's §16.9 experiment, adaptive against fixed and trace-optimal intervals over
recorded interruption traces at 8/64/256 kbit/s and 100 Mbit/s, reporting replay bytes, metadata
commits and completion time.

**D5. Congestion control.** Copa, δ = ½, focal's half stride, behind quinn's controller interface,
pending mantle's own bake-off with CUBIC [RFC9438] and BBRv3 (draft) as candidates and a selection
rule written before the first run. *Sources:* COPA; slates and focal bake-offs (§4.1); MATHIS97.
*Confirms:* the bake-off over §10's matrix with mantle's traffic mix; the result recorded with its
raw rows.

**D6. Pacing and packet size.** The send quantum is a millisecond of the pacing rate, floor two
datagrams, cap 64 KiB; bulk packets are sent at `min(s_min, PMTU)` with
`s_min = (b·o_ip + R·o_q)/(R − b)`, control packets at their message's size; `b ≥ R` is reported as
infeasible.
Requires a vendored quinn-proto. *Sources:* draft-ietf-ccwg-bbr §5.6.3 via slates
`congestion/mod.rs:119-132`; quinn `pacing.rs:129-151`; audit §13.1. *Confirms:* control-message
p99 beside a saturating bulk transfer at 8, 16, 64 and 256 kbit/s, against stock quinn, with
goodput reported; the "airtime/fresh-control test" audit §13.1 asks for.

**D7. Loss recovery.** RFC 9002 estimator, thresholds and persistent congestion; PTO backoff
uncapped; handshake retransmit from the RFC's initial PTO; adaptive reordering with bounded memory;
bytes in flight as packets sent. *Sources:* RFC9002 §5–§7, §B.2; RFC 8985 via slates `reorder.rs`.
*Confirms:* slates' reorder-jitter scenario reproduced on mantle's transport (capacity share with
and without); a blackout test showing probes back off to the idle timeout and stop.

**D8. Idle timeout and keep-alive.** Idle ≥ 3 PTO, also bounded by the caller's progress budget;
keep-alive below the measured NAT mapping lifetime, starting from RFC 9308's 30 s for the public
Internet and lengthened where no NAT rebinding is observed. *Sources:* RFC9000 §10.1; RFC9308 §3.2.
*Confirms:* a NAT emulator with a short UDP mapping timeout; a client idle across it keeps its
connection (or migrates), and a mobile-power measurement of keep-alive traffic per hour.

**D9. Path MTU.** RFC 8899 through quinn with slates' raise recheck and refused-probe rules; the
search ceiling from the interface MTU. *Sources:* RFC8899; slates `pmtud.rs`; BENCHMARKS.
*Confirms:* loopback and jumbo-frame throughput with and without discovery; a black-hole test (MTU
shrinks mid-transfer) recovering at the floor; probe losses absent from congestion events.

**D10. Windows.** `node.md` §3.3's windows, plus a per-stream ceiling of `2,048·d` for frame size
`d` from quinn's chunk limit, or a patched assembler; transfers whose BDP exceeds it use several
streams on one connection. *Sources:* quinn `assembler.rs:361`; focal `transport.rs:49-62`; audit
§13.4. *Confirms:* a fuzzed loss pattern (alternate-frame loss, burst loss) at the ceiling never
closing a connection; throughput at 1 Gbit/s × 100 ms with one and several streams.

**D11. Progress deadlines and reclaim.** Four clocks of §4.7: transport liveness, request progress
(`carried`), memory reclaim by checkpoint after the idle timeout or under memory pressure, durable
progress by the client's horizon or the bucket's lifecycle. *Sources:* focal `transport.rs:150-197`;
`node.md` §3.8, §4.3; audit §13.5. *Confirms:* a 64 kbit/s upload of a manageable size with
injected 5–120 s outages completing (audit §16.9's row), its memory returning within one run
commit of each stall, and its durable runs surviving a gateway restart.

**D12. Retries and reconnects.** Exponential backoff from the measured PTO, capped by the idle
timeout, equal jitter; the same operation identity on every retry; a re-dial replaces the session
under its certificate. *Sources:* focal `peers.rs:1197-1212`; slates `demux.rs:12-19`; audit
§13.5. *Confirms:* a cell-wide partition healed at once: reconnect and election counts per second
stay below the membership's measured round trips' worth.

**D13. Raft under brownouts.** Control on the datagram plane; election base and span from
`ELECTION_MARGIN = 10` measured tails including durable-ack time; the bulk class bounded at the
sender by one tail round trip of the path's rate. *Sources:* RAFT14 §5.6; slates `timing.rs`;
`node.md` §3.4, §3.8; audit §11.7, §13.5. *Confirms:* elections and leader changes per hour
during a 5–120 s rate collapse with saturating bulk traffic, against a run without the bulk bound.

**D14. Migration.** quinn migration on; the client library rebinds on an OS interface change; the
server resets path state per RFC 9000 §9.4. *Sources:* RFC9000 §8.1, §9, §9.4; quinn
`config/mod.rs:288-295`, `connection/mod.rs:3066-3087`. *Confirms:* a Wi-Fi to cellular switch
emulated with two interfaces and a route change mid-upload: the upload continues on the same
connection, or on failure resumes from its durable offset with no re-sent committed run.

**D15. 0-RTT and resumption.** 0-RTT off everywhere; TLS session resumption without early data on
for clients. *Sources:* RFC8470 §4; RFC9001 §9.2 (note 25 §5). *Confirms:* handshake bytes and time
at 8 and 64 kbit/s with and without resumption.

**D16. Client journal.** §5.3's records, flushed by CLAUDE.md §6's rules, operation identity before
the first byte. *Sources:* CLAUDE.md §6; audit §16.4. *Confirms:* a client killed, and its storage
cut by a power-loss emulator, at every journal write and every protocol step; on restart every
operation is either answered from the server's record, resumed, or restarted as a new identity only
when the server shows it had no effect.

**D17. Listener resilience.** Exact `ListParts`; identity from `amz-sdk-invocation-id`;
`AbortIncompleteMultipartUpload` in v1; open-upload bytes reported per bucket and principal;
completion answered directly unless its measured latency approaches the clients' read timeouts.
*Sources:* note 05 §4; §3.6. *Confirms:* AWS CLI and boto3 multipart uploads with the connection
cut at every part and at completion; s3-tests' multipart suite.

**D18. Vendored quinn-proto.** A vendored quinn-proto carrying D6, D7's adaptive reordering, D9's
raise rule and D10's assembler change if chosen, each patch with its tests and offered upstream.
*Sources:* §6.3. *Confirms:* the vendor gate (`cargo test --manifest-path vendor/Cargo.toml`), and
the bake-off of D5 run on the vendored stack.

---

## 10. Test plan

**Network emulation.**
- *Linux:* `tc` with `netem` for delay, jitter, loss (random and correlated), duplication and
  reordering, and a rate limit with a stated buffer (`tbf`, or `netem rate` with a queue limit) for
  the bottleneck; network namespaces for multi-node topologies; two interfaces with route changes for
  migration. The container route of `scripts/linux-test.sh` runs these.
- *macOS:* Network Link Conditioner for the developer's laptop cases; `dnctl`/`pfctl` (dummynet) for
  scripted rate, delay and loss.
- *Windows:* clumsy (WinDivert) for loss, delay, duplication and reordering.
- *Deterministic simulation* for the transport and the drivers, in the manner of slates' simulated
  network (a bottleneck with a drop-tail queue of one BDP, random and burst loss, reordering) and
  focal-sim's real code in virtual time (note 07 §5.2), so every grid is reproducible by seed.

**Fault injection at every protocol step.** A proxy on the native protocol and on the listener cuts
the connection after each frame kind and at every byte offset class (before the first byte, mid
segment, at a segment, block and run boundary, after the last byte, after the completion list, after
the answer's first byte). The gateway is killed at each of §2's points R through A; storage nodes
lose power before and after `Stored`, with the chunk store's existing crash harness
(`crates/chunk/tests/crash.rs`) discarding unflushed writes; range leaders lose power between commit
and answer. Each run checks that the client's final state is one of: committed once with the first
answer, resumed with no committed run re-sent, or refused with no effect; and that, once faults stop,
nothing is left unsettled (the property `orphan_sweep.rs` checks today).

**The qualification matrix** (audit §13.6), cell by cell:

| Axis | Cases | Observables |
|---|---|---|
| Rate | 8, 16, 64, 256 kbit/s; 1, 10, 100 Mbit/s; 1, 10, 100 Gbit/s where hardware allows; asymmetric up and down | durable goodput, operating packet size, control p99 beside bulk |
| Delay and queues | LAN; 20, 100, 300, 1,000+ ms; jitter; tiny and deep buffers; incast; many competing flows | p50/p99/p999 of control and small requests; standing queue |
| Loss and reorder | 0–10 % random; correlated bursts; duplication; reordering; MTU shrink and black holes | spurious-loss count, retransmitted bytes, time to recover |
| Instability | capacity steps and collapse; 5–120 s outages; one-way failures; NAT rebinding; interface changes; certificate rotation; reconnect storms | elections per hour; reconnects per second; replay bytes per interruption; memory returned per stall |
| Application mix | cold handshake; tiny durable PUT and GET; Raft appends and votes; large object streaming; snapshots; repair; stalled disk; slow readers | request dispositions (committed once, resumed, refused); first-byte latency; tail stalls |
| Scale and authority | one laptop; a node with remote clients; a cell; a region; many cells | per-step envelope (§7); stale-epoch callbacks refused |
| Power | client, gateway, storage node, range leader, whole cell, laptop battery | nothing acknowledged lost; recovery time; journal outcomes (D16) |

Every result records the build, the hardware, the seed, the selection rule where one applies, and
the raw rows, as CLAUDE.md §8 asks of a performance claim.

---

## 11. What remains unknown

- **Copa on mantle's traffic.** Both siblings' grids are their own workloads; whether Copa holds up
  beside erasure-coded fan-in, Raft on the datagram plane and repair floors is D5's experiment.
  Copa's own cellular results and any LEO satellite measurement were not read for this note.
- **Whether quinn, patched, matches slates' dialect** on thin and lossy paths. §6.3's recommendation
  rests on migration and tooling; a measurement could reverse it.
- **How often mantle's clients meet networks that block UDP**, and so whether the native protocol
  needs a TCP binding (§4.9). No population of mantle clients exists to measure yet.
- **The digest state a resumable upload needs.** Whether the chosen hash implementations expose
  MD5, SHA-1, SHA-256, SHA-512 and XXHash states at a block boundary, or mantle must carry its own
  (§3.5).
- **Checkpoint optimality.** Young's interval was stated from the literature, not re-read; its
  higher-order correction (DALY06) and the overlapped-commit case of §4.6 need checking against
  recorded interruption traces.
- **The client horizon's distribution.** How long agents and laptops actually take to come back,
  which sets the operation table's size (§3.4) and the open-upload storage.
- **Stock clients' read timeouts and resume details.** botocore's default read timeout, the Java
  Transfer Manager's resume token and its source-change test, and the AWS CLI's behaviour after a
  process restart were not confirmed (§3.1, §3.6).
- **Kernel facilities per OS.** `TCP_NOTSENT_LOWAT` and `TCP_USER_TIMEOUT` availability on macOS and
  Windows; which clock Rust's `Instant` uses on each platform across suspend (§3.6, §5.3); the OS
  network-change notification interfaces (§4.9).
- **Device flush honesty.** That every device acknowledges a flush only when durable is note 29's
  question; this note assumes it.
- **8 and 16 kbit/s.** Neither sibling's grid starts below 64 kbit/s (audit §13.6); the thin-link
  operating sizes of §4.2 are derived, not measured.
