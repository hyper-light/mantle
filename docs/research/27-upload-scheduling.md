# 27 — Upload scheduling: no upload dominates, every upload as fast as it can be

**Status:** research input for the gateway's admission (`docs/design/gateway.md`), the node's
schedulers (`docs/design/node.md` §2–§4) and the cell's control plane (`architecture.md` §8).
This is not a decision record; §9 proposes decisions for the design records to take or refuse.
**Compiled:** 2026-09-30.
**The question, verbatim from the owner:** "One of the problem with uploads is we might have a
VERY large multi-part upload and get a sudden spike of smaller uploads (or other similar
scenarios). So the question is - how can we design a system that facilitates these uploads such
that while no one upload dominates/blocks, any given upload is as fast as it can be."
**The workload:** agentic. Principals and sessions orders of magnitude beyond human counts,
machine-speed correlated bursts (a swarm fanning out the same step, retry storms on timeout,
runaway loops), a request mix dominated by tiny objects and metadata operations beside
multi-terabyte datasets and 100 GB videos. **The deployment:** one binary and one code path from
one laptop to a global fleet, with each step of scale adding only the mechanism and the
configuration that step needs. **The client protocols:** mantle's own QUIC protocol, spoken by its
client library and CLI and between nodes, carries S3 semantics natively; an HTTP/1.1 S3
compatibility listener, on by default, translates stock S3 requests onto it. Both are first-class
and share one admission authority (§7.1).

**Scope.** What the literature says about (1) work-conserving fairness over several resources,
(2) storage QoS that bounds small requests' delay beside large streams, (3) overload and
admission from client to device, including size-based priority and its starvation problem, (4)
making one large upload fast without letting it dominate, and (5) where each mechanism belongs in
mantle's hybrid of cells, Tectonic and ZippyDB, with how a principal's share is enforced across a
fleet without a global bottleneck and without state that grows with the number of principals.

**What this note does not repeat.** Start-time fair queueing, hierarchical SFQ, H-WF²Q+, 2DFQ,
Retro, Pisces, Breakwater, Fail at Scale, CoDel, retry budgets and the S3 SDKs' retry behavior
are in note 25 §3, §11–§13, quoted from their sources; this note cites them there. Tectonic's
multitenancy is note 01 §1.11; DynamoDB's global admission control, S3's partitioning and
SlowDown, shuffle sharding and Shard Manager are note 09 §3.6–§3.8, §6.2–§6.3, §7.3.5, §8. Part
size, block windows and resume are audit §16.2–§16.4 and `gateway.md` §2. How the node issues
concurrent I/O and drives many clients without a thread per unit is note 26 (concurrency model,
being written alongside this note); this note says what is scheduled and in what order, not how
the waiting is implemented.

---

## 0. How to read this note

**Citation tags.** `[KEY §section, p. N]`. Where a PDF carries printed proceedings pages they are
used (GPS93, P2C, PARDA, GIMBAL); otherwise "PDF p. N" is the page of the PDF that was read.
AWS pages are cited by heading. Earlier notes are "note 25 §x"; the audit is "audit §x"
(`docs/audit/2026-09-29_audit.md`, read for its argument, not quoted as evidence of fact).

**Quotes** are verbatim from each source's text layer, read with `pdftotext` on 2026-09-30:
ligatures restored, words hyphenated across lines joined, "..." for an elision. Equations whose
symbols the text layer loses are marked *transcription* and written in plain notation.

**Evidence labels**, as in notes 12, 23 and 25:

- *(no label)*: stated in a **peer-reviewed** source and checked against its text.
- **primary**: vendor documentation or a standard (RFC), read directly.
- **NON-PEER-REVIEWED**: magazine articles, vendor books, blogs, conference slides.
- **DERIVED**: arithmetic or interpretation made by this note.
- **UNVERIFIED**: not found in a primary source that was read.
- **INFERENCE / Recommendation**: reasoning for mantle, citing the facts it rests on.

**Method.** PDFs were fetched from usenix.org, sigcomm.org, arxiv.org, nsf.gov and the authors'
or courses' hosted copies; each was converted to text and the passages quoted here were found by
search and read in context. Two papers named in the brief could not be read: Swift (SIGCOMM '20;
the ACM PDF refused a scripted client and no author copy was found), and the published
SIGMETRICS '01 version of Bansal and Harchol-Balter, read instead as the authors' extended CMU
version. The brief places Libra at ATC 2014; it appeared at EuroSys 2014.

---

## Sources

### Read for this note

| Key | Source | Label |
|---|---|---|
| WFQ89 | A. Demers, S. Keshav, S. Shenker. "Analysis and Simulation of a Fair Queueing Algorithm." SIGCOMM '89, pp. 1–12. https://web.stanford.edu/class/cs244/papers/demers-queueing.pdf | peer-reviewed |
| GPS93 | A. K. Parekh, R. G. Gallager. "A Generalized Processor Sharing Approach to Flow Control in Integrated Services Networks: The Single-Node Case." IEEE/ACM ToN 1(3):344–357, 1993. https://www.cs.utexas.edu/~lam/396m/papers/PG1993.pdf | peer-reviewed |
| CSFQ | I. Stoica, S. Shenker, H. Zhang. "Core-Stateless Fair Queueing: Achieving Approximately Fair Bandwidth Allocations in High Speed Networks." SIGCOMM '98. https://conferences.sigcomm.org/sigcomm/1998/tp/paper10.pdf | peer-reviewed |
| DRF | A. Ghodsi, M. Zaharia, B. Hindman, A. Konwinski, S. Shenker, I. Stoica. "Dominant Resource Fairness: Fair Allocation of Multiple Resource Types." NSDI '11. https://www.usenix.org/legacy/event/nsdi11/tech/full_papers/Ghodsi.pdf | peer-reviewed |
| DRFQ | A. Ghodsi, V. Sekar, M. Zaharia, I. Stoica. "Multi-Resource Fair Queueing for Packet Processing." SIGCOMM '12. https://people.eecs.berkeley.edu/~alig/papers/drfq.pdf | peer-reviewed |
| MCLOCK | A. Gulati, A. Merchant, P. J. Varman. "mClock: Handling Throughput Variability for Hypervisor IO Scheduling." OSDI '10. https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Gulati.pdf | peer-reviewed |
| PARDA | A. Gulati, I. Ahmad, C. A. Waldspurger. "PARDA: Proportional Allocation of Resources for Distributed Storage Access." FAST '09, pp. 85–98. https://www.usenix.org/legacy/event/fast09/tech/full_papers/gulati/gulati.pdf | peer-reviewed |
| SFQD | W. Jin, J. S. Chase, J. Kaur. "Interposed Proportional Sharing for a Storage Service Utility." SIGMETRICS '04; the authors' revised version. https://www.cs.unc.edu/~jasleen/papers/sigmetrics04.pdf | peer-reviewed |
| LIBRA | D. Shue, M. J. Freedman. "From Application Requests to Virtual IOPs: Provisioned Key-Value Storage with Libra." EuroSys '14. https://www.cs.princeton.edu/~mfreed/docs/libra-eurosys14.pdf | peer-reviewed |
| IOFLOW | E. Thereska et al. "IOFlow: A Software-Defined Storage Architecture." SOSP '13. https://pages.cs.wisc.edu/~remzi/Classes/739/Fall2016/Papers/ioflow-sosp13.pdf | peer-reviewed |
| REFLEX | A. Klimovic, H. Litz, C. Kozyrakis. "ReFlex: Remote Flash ≈ Local Flash." ASPLOS '17. https://courses.physics.illinois.edu/ece598ms/fa2019/papers/paper193.pdf | peer-reviewed |
| GIMBAL | J. Min, M. Liu, T. Chugh, C. Zhao, A. Wei, I. H. Doh, A. Krishnamurthy. "Gimbal: Enabling Multi-tenant Storage Disaggregation on SmartNIC JBOFs." SIGCOMM '21, pp. 106–122. https://conferences.sigcomm.org/sigcomm/2021/files/papers/3452296.3472940.pdf | peer-reviewed |
| DAGOR | H. Zhou, M. Chen, Q. Lin, Y. Wang, X. She, S. Liu, R. Gu, B. C. Ooi, J. Yang. "Overload Control for Scaling WeChat Microservices." SoCC '18. Text from arXiv:1806.04075. | peer-reviewed |
| AEQUITAS | Y. Zhang, G. Kumar, N. Dukkipati, X. Wu, P. Jha, M. Chowdhury, A. Vahdat. "Aequitas: Admission Control for Performance-Critical RPCs in Datacenters." SIGCOMM '22. https://par.nsf.gov/servlets/purl/10412424 | peer-reviewed |
| HOMA | B. Montazeri, Y. Li, M. Alizadeh, J. Ousterhout. "Homa: A Receiver-Driven Low-Latency Transport Protocol Using Network Priorities." SIGCOMM '18. Text from arXiv:1803.09615. | peer-reviewed |
| SRPT01 | N. Bansal, M. Harchol-Balter. "Analysis of SRPT Scheduling: Investigating Unfairness." SIGMETRICS '01; the authors' extended version. https://www.cs.cmu.edu/~harchol/Papers/Sigmetrics01.pdf | peer-reviewed |
| TAIL | J. Dean, L. A. Barroso. "The Tail at Scale." CACM 56(2), 2013. https://www.barroso.org/publications/TheTailAtScale.pdf | peer-reviewed (magazine, refereed) |
| P2C | M. Mitzenmacher. "The Power of Two Choices in Randomized Load Balancing." IEEE TPDS 12(10):1094–1104, 2001. https://www.eecs.harvard.edu/~michaelm/postscripts/tpds2001.pdf | peer-reviewed |
| CM05 | G. Cormode, S. Muthukrishnan. "An Improved Data Stream Summary: The Count-Min Sketch and its Applications." J. Algorithms 55(1):58–75, 2005; preprint. https://www.cs.tufts.edu/~nr/cs257/archive/graham-cormode/count-min.pdf | peer-reviewed |
| SPACESAVING | A. Metwally, D. Agrawal, A. El Abbadi. "Efficient Computation of Frequent and Top-k Elements in Data Streams." ICDT '05; extended version, UCSB TR 2005-23. https://www.cs.ucsb.edu/sites/default/files/documents/2005-23.pdf | peer-reviewed |
| DRL | B. Raghavan, K. Vishwanath, S. Ramabhadran, K. Yocum, A. C. Snoeren. "Cloud Control with Distributed Rate Limiting." SIGCOMM '07. https://cseweb.ucsd.edu/~snoeren/papers/drl-sigcomm07.pdf | peer-reviewed |
| META-HOTOS | N. Bronson, A. Aghayev, A. Charapko, T. Zhu. "Metastable Failures in Distributed Systems." HotOS '21. | peer-reviewed (workshop) |
| META-OSDI | L. Huang et al. "Metastable Failures in the Wild." OSDI '22. https://www.usenix.org/system/files/osdi22-huang-lexiang.pdf | peer-reviewed |
| RFC9218 | K. Oku, L. Pardue. "Extensible Prioritization Scheme for HTTP." RFC 9218, §4.1, §10, §13 | primary |
| S3-GUIDE | Amazon S3 User Guide, "Performance guidelines for Amazon S3", fetched 2026-09-30 as markdown | primary |
| S3-PATTERNS | Amazon S3 User Guide, "Performance design patterns for Amazon S3", fetched 2026-09-30 | primary |

### Cited through earlier notes (their keys)

SFQ96, HSFQ96, HPFQ96, 2DFQ, RETRO, PISCES, BREAKWATER, FAILSCALE, RFC8289, SRE-CASCADE,
SRE-OVERLOAD, SDK-RETRY, SDK-GO, BOTOCORE, QUINN, RFC9000, S3-REDIRECT, S3-ERR, S3-PERF (note 25);
TEC, ZDB-BLOG (note 01); DDB, S3UG-PERF, S3UG-PATTERNS, STG314-23, S3-BLOG12, SM, INFIMA
(note 09).

### Not read

| Source | Why | Effect here |
|---|---|---|
| Swift (Kumar et al., SIGCOMM '20) | ACM PDF refused a scripted client; no author copy found | Its delay-target congestion control is **UNVERIFIED**; Aequitas, which was read, runs over it (§5.4). |
| Jain, Chiu, Hawe, DEC-TR-301 (1984), the fairness index | not fetched | The index is used in §10 as a definition only; the cited fairness measure is the min-max ratio Pisces and Libra report. |
| Mitzenmacher, "How Useful Is Old Information?" (TPDS 2000) | not fetched | Herding on stale load is cited through TAIL (§6.2). |

---

## 1. Decision-relevant summary

1. **The two kinds of upload are bottlenecked on different resources.** A small PUT costs a
   fixed set of metadata commands and device operations whatever its size; a multipart part
   costs bandwidth, coding CPU and memory in proportion to its bytes (§2.1, DERIVED from
   `gateway.md` §2). Fairness counted in one unit, bytes or requests, either lets small requests
   flood the metadata path or lets large ones flood the devices. Dominant Resource Fairness is
   the generalization of max-min fairness to this case, and DRFQ is its form for a queue [DRF §1;
   DRFQ §1] (§3.3).
2. **Fair queueing already gives small requests low delay without size priority.** SFQ "does
   not couple bandwidth and delay allocation", so a flow that sends little gets a small maximum
   delay (note 25 §11); WFQ was introduced with "lower delay for sources using less than their
   full share of bandwidth" [WFQ89 Abstract]. What makes a small request wait behind a large
   upload is the unit of work dispatched and the depth of the device queue, both of which the
   scheduler controls: SFQ(D) bounds lag "as a function of D" [SFQD §3], and Gimbal and Tectonic
   cap a stream's outstanding I/O at a device (§4).
3. **Strict size priority across principals starves the large upload exactly when the spike
   comes.** SRPT is near-optimal for mean response time and, below load ½, better than processor
   sharing for every job [SRPT01 Theorem 3], but Homa measures "99th-percentile slowdowns of 100x
   or more" for its largest messages and proposes "dedicating a small fraction of downlink
   bandwidth to the oldest message" [HOMA, PDF p. 9]; under overload SRPT's guarantees cover "all
   but the largest 1% of the jobs" [SRPT01 §1]. An agent swarm is overload made of small jobs,
   and SRPT is not strategy-proof against splitting. Size priority therefore belongs inside a
   principal's share, never across principals (§5.6).
4. **A high-priority class is only fast while its admitted share is bounded.** Aequitas shows
   that with weighted fair queues, "at a certain share of QoSh, we observe priority inversion
   where delay in QoSh exceeds that of QoSl", and it admits each RPC to its class with a
   probability driven by measured latency, downgrading the rest [AEQUITAS §4.1–§5.1] (§5.4).
5. **The large upload yields within one device queue's worth of time and reclaims within one
   block chain.** A work-conserving fair scheduler gives a newly backlogged flow service at its
   next dispatch; what the large upload has already dispatched drains in at most the device's
   in-flight bytes over its rate. That bound is set by the depth calibration already measures
   (§6.4, DERIVED).
6. **Per-principal state can be bounded without losing fairness.** SFQ needs state only for
   flows whose last finish tag is ahead of virtual time, so a fair queue's table is bounded by
   admitted work, not by principals (§8.1, DERIVED from SFQ96's tags). Rate accounting over a
   window, where principals are unbounded, uses Space-Saving, whose counts are exact while the
   distinct principals fit its table and whose error is at most N/m beyond [SPACESAVING Lemma
   2–3]; and count-min sketches, which are linear and so merge across gateways, cells and
   regions by addition [CM05 §1, Theorem 1] (§8).
7. **Shed load by principal, deterministically, so a swarm's admitted members finish.** DAGOR
   hashes the user ID into 128 priority levels, rotates the hash hourly, and moves an admission
   cursor over a histogram of those levels; it rejected session-keyed priority because users
   learned to log out and in to escape shedding [DAGOR §4.2.2–§4.2.3]. Agents with short-lived
   sessions are that behavior automated: shed by principal, never by session (§5.2).
8. **Retries sustain the failure after the trigger is gone.** Retry policy sustained more than
   half of 22 studied metastable incidents; "a policy with at most two retries will not amplify
   the work more than three times, while the policy with no cap effectively leaves the system with
   no stable region" [META-OSDI §3, §5]. S3 SDKs send `amz-sdk-request: attempt=N` (note 25 §13),
   so the gateway can see retries and shed them first (§5.7).
9. **A principal's share is enforced globally by local decisions on exchanged summaries.**
   Tectonic's client library uses "near-realtime distributed counters" (note 01 §1.11);
   DynamoDB's routers hold tokens vended by a stateless admission service (note 09 §3.6); DRL
   proves that "a distributed limiter cannot be simultaneously perfectly accurate and responsive"
   and divides a global limit in proportion to local demand [DRL §1, §3.2]; dmClock piggybacks
   two integers per request so storage servers honor a client's global share "without any
   synchronization among the storage servers" [MCLOCK §3.2.1]. Each degenerates to a local
   bucket with one participant (§7.6).
10. **The metadata ranges are where a swarm of tiny requests binds first.** Tectonic's Name
    shards cap at 10 KQPS and about 1% hit it with "very hot directories" (note 01 §1.12); S3
    answers 503 SlowDown while a prefix's partition splits (note 09 §6.2–§6.3). Splitting for
    observed heat, not for a single key or sequential keys, and per-range fair queues by tenant
    are as critical to the owner's scenario as device bandwidth (§7.3).
11. **Two front doors, one authority.** The native QUIC protocol can carry what HTTP cannot:
    credits before data, a typed refusal with a derived wait, the admission level for the client
    to shed locally, the server's upload plan, and server-assigned stream classes. The HTTP/1.1
    listener maps the same decision onto S3's contract: `503 SlowDown` before `100 Continue`, TCP
    flow control on admitted bodies. A principal's share is the same whichever door it uses (§7.1,
    §9 D5).

---

## 2. The problem in resources, and the scale it must hold

### 2.1 What one upload consumes (DERIVED from `gateway.md` §1–§2 and `node.md` §2.5)

| Resource | Small PUT of `s` bytes (`s` ≤ one block) | One part of a multipart upload, `p` bytes |
|---|---|---|
| Client → gateway path | `s` bytes, one request head | `p` bytes, held as a window of blocks |
| Gateway CPU | MD5, AEAD over `⌈s/64 KiB⌉` segments, coding of one block | the same per byte, `⌈p/B⌉` blocks |
| Gateway memory | one block and its parity | `w` blocks and parity, `w = ⌈R·T/B⌉` (`gateway.md` §2) |
| Placement | one request | one per block |
| Chunk writes | `k + m` (coded) or `n` (copies) writes, each a durable flush | the same per block |
| Device time | `k + m` small writes, IOPS-bound | `⌈p/B⌉·(k+m)` chunk writes of up to 8 MiB, bandwidth-bound |
| Metadata commands | Block row, File row, Name commit: three Raft commands in up to three ranges | one Block row per block, one File row and one part row per part, renewals (audit §16.5) |
| Sessions | none per principal: sessions are per gateway incarnation and range (`node.md` §5.3) | the same |

Two consequences follow. First, a burst of small PUTs is bounded by device operations and
metadata commands, and a large part by bandwidth, coding and memory: they have different
**dominant resources** in DRF's sense (§3.3). Second, the per-request overheads (three metadata
commands, `k + m` flushed writes) are what an agentic request mix multiplies; per-request cost is
itself a capacity limit (§9, D10).

### 2.2 The scale assumptions, stated

No source gives agentic request rates. The design therefore states each scale quantity as a
symbol, names where a value comes from, and checks every per-principal structure against the
largest value the owner names (billions of principals).

| Symbol | Meaning | Where its value comes from |
|---|---|---|
| `P` | principals known to a tenant or the fleet | owner: 10⁶–10⁹ ("millions to billions of agents") |
| `P_a(Δ)` | principals active in a window `Δ` | measured per gateway; bounded above by requests in `Δ` |
| `λ` | aggregate request rate at a gateway, a cell, a region | measured; the published anchor is S3's index, "350 trillion objects, 100+ million requests per second" (note 09 §6.2.4, NON-PEER-REVIEWED slides) |
| `λ_range` | sustainable commands per second of one metadata range | measured per engine and device; Tectonic's cited order is 10 KQPS per shard (note 01 §1.12), S3's per-prefix floor 3,500 writes / 5,500 reads (note 09 §6.2.1) |
| `Q` | requests a gateway admits at once | Little's law, `Q = λ·d` for admitted delay `d` (`node.md` §2.6) |
| `r` | attempts per request a client makes | SDK standard mode: 3 (note 25 §3); amplification across layers multiplies (SRE: "64 attempts (4^3)", note 25 §13) |

**DERIVED.** An exact per-principal table at a gateway costs `P × e` bytes for `e` bytes per
entry: at `P = 10⁹` and `e = 48` it is 48 GB, more than a gateway's memory and far more than a
laptop's. Every per-principal structure must therefore be bounded by something the gateway
controls (admitted work, a configured table size) and pruned by a stated rule, which §8 does.

### 2.3 The steps of scale

The owner's governing principle is stepped complexity. The steps used throughout are:

| Step | What it is | What appears there that the step before did not have |
|---|---|---|
| 1. Laptop | one process, one node, one cell, one device shared by the log, engines and chunks, a few cores, a few GB | everything shares one device and one memory; agents on the laptop can still be many |
| 2. Node | one server: many devices, many cores, many clients | a choice among devices; heterogeneous devices; many range shards |
| 3. Cell | several nodes, several gateways | remote devices and links; a tenant's requests spread over gateways and nodes; repair |
| 4. Region | a set of cells behind a router | a tenant or bucket spread over cells; hot prefixes that outgrow a cell |
| 5. Fleet | many regions | shares across regions; coordination latency of continents |

---

## 3. Fairness that is work-conserving across several resources

### 3.1 Max-min fairness, WFQ and GPS

- The definition: "an allocation is fair if (1) no user receives more than its request, (2) no
  other allocation scheme satisfying condition 1 has a higher minimum allocation, and (3)
  condition 2 remains recursively true as we remove the minimal user and reduce the total
  resource accordingly" [WFQ89 §2]. "Note that implicit in the max-min definition of fairness is
  the assumption that the users have equal rights to the resource." [WFQ89 §2]
- What fair queueing gives: "fair allocation of bandwidth, lower delay for sources using less
  than their full share of bandwidth, and protection from ill-behaved sources" [WFQ89 Abstract].
- GPS with admission: "the use of Generalized Processor Sharing (GPS), when combined with Leaky
  Bucket admission control, allows the network to make a wide range of worst-case performance
  guarantees on throughput and delay. The scheme is flexible in that different users may be
  given widely different performance guarantees, and is efficient in that each of the servers is
  work conserving." [GPS93 Abstract, p. 344]
- The packet approximation: PGPS ("first proposed by Demers, Shenker, and Keshav under the name
  of Weighted Fair Queueing") finishes every packet no later than GPS would plus one largest
  packet's transmission time: *transcription* of Theorem 1, `F̂_p − F_p ≤ L_max / r` [GPS93 §III].

**For mantle (INFERENCE).** Two facts carry through the rest of this note. The guarantee is
work-conserving: an idle share is used by whoever is backlogged, so a large upload alone on a
device gets the whole device. And the delay a packetized scheduler adds over the fluid ideal is
one largest unit of work at the server's rate: the size of the largest thing dispatched sets the
delay a small request sees, which is the handle §4 and §6.4 use.

### 3.2 Start-time fair queueing and hierarchies

Note 25 §11 quotes SFQ96, HSFQ96 and HPFQ96. The properties used here: SFQ's fairness bound
"holds regardless of the characteristics of the server", which matters because a device's and a
peer path's rates vary; SFQ "does not couple bandwidth and delay allocation"; a request's cost
need not be known when it starts, only when it finishes; a hierarchy is SFQ applied recursively,
and an idle class's share goes to its siblings by weight. H-WF²Q+ gives tighter delay bounds in a
deep hierarchy at O(log N) cost per decision, where H-WFQ's worst-case fairness index grows with
the number of queues.

### 3.3 Dominant Resource Fairness, and its queueing form

- "DRF seeks to maximize the minimum dominant share across all users. For example, if user A
  runs CPU-heavy tasks and user B runs memory-heavy tasks, DRF attempts to equalize user A's
  share of CPUs with user B's share of memory. In the single resource case, DRF reduces to
  max-min fairness for that resource." [DRF §1, PDF p. 1]
- Its four properties: "DRF incentivizes users to share resources, by ensuring that no user is
  better off if resources are equally partitioned among them. Second, DRF is strategy-proof, as a
  user cannot increase her allocation by lying about her requirements. Third, DRF is envy-free
  ... Finally, DRF allocations are Pareto efficient" [DRF Abstract].
- Cost: "The above algorithm can be implemented using a binary heap that stores each user's
  dominant share. Each scheduling decision then takes O(log n) time for n users." [DRF §4.2,
  PDF p. 5]
- Weighted DRF: "the definition of a dominant share for user i changes to s_i = max_j {u_i,j /
  w_i,j}" [DRF §4.3].
- Limit: DRF does not satisfy resource monotonicity, and the authors prove "no policy can provide
  resource monotonicity without violating either sharing incentive or Pareto efficiency" [DRF §6,
  PDF p. 7].
- In time rather than across servers: DRFQ "generalize[s] the concept of virtual time in
  classical fair queuing to multi-resource settings" [DRFQ Abstract]. It must be memoryless,
  "a flow should not be penalized for having had a high resource share in the past when fewer
  flows were active ... This memoryless property is key to guaranteeing that flows cannot be
  starved in a work-conserving system." [DRFQ §1, PDF p. 2] Memorylessness and "dove-tailing"
  (crediting a flow's under-use of one resource against over-use of another) "cannot both be fully
  achieved at the same time" [DRFQ §3, PDF p. 6]. Memoryless DRFQ charges each packet, in SFQ's
  tags, its largest per-resource processing time (*transcription*: the finish tag adds
  `max_j s_{k,j} / w_i`) [DRFQ §5.4].

**For mantle (INFERENCE).** A small PUT's dominant resource is device operations or metadata
commands; a large part's is device bandwidth, coding CPU or memory. Charging each request its
dominant share — its largest cost over the authorities it reserves from (`node.md` §2.5), each
cost normalized by that authority's measured capacity — makes a principal that floods the
metadata path and one that floods the disks pay in the same currency. Memoryless charging is the
property the owner's scenario needs: when the spike arrives, the large upload is not "owed"
anything for having had the device to itself, and the newcomers are not penalized for it either.
Strategy-proofness matters because agents will be tuned, by their authors or by themselves, to
whatever the scheduler rewards.

### 3.4 Requests of unknown and widely varying cost

2DFQ and Retro are quoted in note 25 §12. The points used: request costs "vary by at least 4
orders of magnitude"; WFQ and WF²Q at high concurrency produce "bursty schedules, where large
requests block small ones for long periods of time"; 2DFQ separates requests by size across
workers and prices unknown requests pessimistically, per tenant and API, with a decaying maximum;
charging is retroactive by measured cost; Retro attributes resource use across processes by a
propagated workflow ID and treats a resource as saturated by its measured slowdown, since "it is
not possible to query the capacity of a resource".

---

## 4. Storage QoS: small requests beside large streams, work-conserving

### 4.1 mClock and dmClock: reservations, limits, shares

- Controls: "mClock, supports proportional-share fairness subject to minimum reservations and
  maximum limits on the IO allocations for VMs." [MCLOCK Abstract]
- Mechanism: "logically interleave a constraint-based scheduler and a weight-based scheduler in a
  fine-grained manner. The constraint-based scheduler ensures that VMs receive at least their
  minimum reserved service and no more than the upper limit in a time interval, while the
  weight-based scheduler allocates the remaining throughput to achieve proportional sharing."
  [MCLOCK §3, PDF p. 5] Each request carries three tags; the reservation tag is
  `R_i^r = max{R_i^{r−1} + 1/r_i, t}`, and "idle VMs do not gain any idle credit for future
  service" [MCLOCK §3].
- Bounded idle credit, when wanted: the share tag becomes `max{P_i^{r−1} + 1/w_i, t − σ_i/w_i}`,
  where "σ_i ... determines the maximum amount of credit that can be gained by becoming idle"
  [MCLOCK §3.1].
- Large I/Os are charged as more than one: "a single request of IO size S is treated as
  equivalent to: (1 + S/(T_m × B_peak)) IO requests", with T_m the mechanical delay and B_peak
  the peak transfer rate [MCLOCK §3.1, PDF p. 6].
- Distributed: "dmClock runs a modified version of mClock at each server", the host
  "piggybacking two integers ρ_i and δ_i with each request", the service the VM received at other
  servers (all, and the part done under reservation). Tags advance by `ρ_i/r_i` and `δ_i/w_i`
  instead of `1/r_i` and `1/w_i`, so "the new request may receive a tag further into the future,
  to reflect the fact that v_i has received additional service at other servers ... this does not
  require any synchronization among the storage servers. ... The values of ρ and δ may, in the
  worst case, be inaccurate by up to 1 request at each of the other servers." [MCLOCK §3.2.1,
  PDF p. 7]

### 4.2 PARDA: a FAST-TCP window on storage latency

- Each host runs, independently, `w(t+1) = (1 − γ)·w(t) + γ·(L/L(t)·w(t) + β)` on its issue
  queue, where `L(t)` is an EWMA of measured latency, `L` "the system-wide latency threshold", and
  "β is a per-host parameter that reflects its IO shares allocation" [PARDA §3.2, pp. 87–88].
- "at equilibrium, the throughput of host i is proportional to β_i/q_i", and the window is
  bounded by `[w_min, w_max]`: "The lower bound w_min prevents starvation for hosts with very few
  IO shares. The upper bound w_max avoids very long queues at the array, limiting the latency
  seen by hosts that start issuing requests after a period of inactivity." [PARDA §3.2, p. 88]
- On choosing `L`: "increasing the array queue length beyond a certain value doesn't lead to
  increased throughput. Thus, L can be set to a value which is high enough to ensure that a
  sufficiently large number of requests can always be pending at the array." [PARDA §3.2, p. 88]

### 4.3 SFQ(D): depth against fairness

- "SFQ(D) and FSFQ(D) dispatch at most D outstanding requests to the server at any given time;
  the depth parameter D is a tradeoff between tight resource control and server resource
  utilization." [SFQD §1] "a larger D may allow better multiplexing of server resources, but it
  may impose a higher waiting time on an incoming client request. The policy to configure the D
  parameter or adapt it dynamically is outside the scope of this paper." [SFQD §3, PDF p. 4]
- Theorem 2 bounds the difference in completed work between two backlogged flows under SFQ(D)
  by a term that grows with `D` times each flow's largest request over its weight; the
  inequality's symbols are lost in the text layer and are not transcribed here.
- FSFQ(D) "can reduce the lag bound modestly by giving newly active flows preference for their
  fair share of the D request slots" [SFQD §3].

### 4.4 Pisces and Libra: costs in the device's own units

Pisces (note 25 §12) composes placement, weight allocation, replica selection and DWRR at four
time scales to reach "0.99 Min-Max Ratio" with "<3%" overhead. Libra adds the device cost model:

- The problems: "non-uniform IO amplification, unpredictable IO interference, and non-linear IO
  performance" [LIBRA Abstract]; "IO cost varies non-linearly with operation size ... shifting
  bottlenecks from the controller (IOP) to the data channel (bandwidth) as op sizes increase"
  [LIBRA §1].
- Tracking: "Libra marks tenant IO operations by their associated application request across all
  foreground and background operations in the storage stack" [LIBRA §1], so a 1 KB PUT's log
  write and table write are charged to it.
- Cost unit: "virtual IO operations (VOP) using an IO cost model that captures the non-linear
  performance characteristics of the underlying SSD media" [LIBRA §1].
- Work conservation: "Although up to half the IO resources may be left unprovisioned, Libra still
  preserves high utilization by allowing tenants to share any excess IO throughput in a
  work-conserving manner. Under most realistic workloads ... this provisionable resource gap drops
  to 16% or less." Accuracy: "0.98 min-max ratio (MMR) across equal-allocation tenants—compared to
  other extant IO cost models (< 0.84 MMR)" [LIBRA §1, PDF p. 2].

### 4.5 IOFlow: tokens priced by end-to-end cost, depth held below the uncontrolled layer

- "at sender stages, instead of releasing tokens based on bytes in the request, they need to be
  released based on the end-to-end cost of the operation." The controller "benchmarks the storage
  devices to measure the cost of IO requests as a function of their type and size", repeated
  "periodically since the cost of IO requests varies with varying aggregate workload" [IOFLOW,
  "Storage request peculiarities", PDF p. 5].
- Where a layer cannot be scheduled: "IOFlow does not have control over requests once they enter
  an SSD. The discovery component runs benchmarks to measure the tradeoff between the token rate,
  number of outstanding IO requests, and latency for the device. As an example, to keep the SSD
  ... 95% utilized, 90 outstanding requests could be sufficient ... Thus, IOFlow could control the
  token rate to maintain 90 requests at the device and the rest in IOFlow's data-plane queues.
  Priority treatment can then be applied to those data-plane queues." [IOFLOW, PDF p. 8]

### 4.6 ReFlex: a calibrated token model with latency and best-effort tenants

- Cost: `I/O cost = ⌈I/O size / 4KB⌉ × C(I/O type, r)`, where `r` is the device's read ratio;
  "one token represents the cost of a 4KB random read request"; "write operations are 10 to 20
  times more expensive than read operations, depending on the device"; "We calibrate the cost
  model for each type of Flash device ... we conservatively use random write patterns to trigger
  the worst case" [REFLEX §3.2.1, PDF p. 4].
- Supply: "The ReFlex scheduler generates tokens at a rate equal to the maximum weighted IOPS the
  Flash device can support at a given tail latency SLO. ReFlex enforces the strictest (lowest)
  latency SLO among all LC tenants that share a Flash device." Latency-critical tenants are
  guaranteed their tokens; "Tokens generated by the scheduler but not allocated to LC tenants are
  distributed fairly among BE tenants." [REFLEX §3.2.2, PDF p. 5]
- Bursts are bounded both ways: a deficit limit rate-limits a tenant "to limit the number of
  expensive write requests in a burst", and an accumulation limit donates unused tokens to a
  global bucket [REFLEX §3.2.2]. The limits are set empirically ("−50 tokens").

### 4.7 Gimbal: congestion control per SSD, normalized slots, credits end to end

- Mechanisms: "a delay-based SSD congestion control algorithm, dynamic estimation of SSD write
  costs, a fair scheduler that operates at the granularity of a virtual slot, and an end-to-end
  credit-based flow control channel" [GIMBAL Abstract, p. 106].
- Virtual slots bound a stream's outstanding I/O in normalized units: a slot holds "up to 128KB in
  total (e.g., it might contain up to 1 × 128KB or 32 × 4KB IO commands)"; "Each tenant always has
  the same number of virtual slots. If a tenant runs out of its virtual slots, the IO scheduler
  defers following IOs until one of its virtual slots completes"; this "provides an upper bound on
  the submission rate and guarantees that any sized IO pattern obtains a fair portion of the SSD
  internal resource"; "Gimbal sets the threshold for the number of virtual slots in a single
  tenant to the minimum number to reach the device's maximum bandwidth if there is only one active
  tenant. Virtual slots are equally distributed when more active tenants contend for the storage."
  [GIMBAL §3, p. 111]
- The latency threshold adapts rather than being fixed: "We find that 2ms fixed threshold is only
  effective for large ... Therefore, we propose a dynamic latency threshold scaling method" [GIMBAL
  §3]; write cost is updated additive-decrease, multiplicative-increase from measured write
  latency [GIMBAL §3].
- Credits are piggybacked, not separate messages: "we piggyback the allocated credits into the
  NVMe-oF completion response" [GIMBAL §3, p. 112]; and a client chooses among replicas by them:
  "RocksDB will issue a read request to the copy whose remote SSD has the least load. We simply
  rely on the number of allocated credits to decide the loading status on the target." [GIMBAL
  §4, p. 113]

### 4.8 Tectonic's node-local enforcement

Note 01 §1.11 quotes TEC §4.1: a weighted round-robin scheduler "provisionally skips a
TrafficGroup's turn if it will exceed its resource quota", and storage nodes protect Gold traffic
by letting lower classes cede a turn when the request "will have enough time to complete after
the higher-TrafficClass request", by a per-disk limit on in-flight non-Gold I/Os, and by stopping
non-Gold dispatch to a disk once a Gold request has waited a threshold time. Disk time is the
accounted resource, because "neither IOPS nor bandwidth can fairly account for disk IO usage"
(note 01 §1.12). The thresholds are not published.

### 4.9 How each bounds a small request's delay beside a large stream (DERIVED)

| Mechanism | What bounds the small request's wait | Work-conserving? |
|---|---|---|
| SFQ / WFQ at the device | one largest dispatched unit per other backlogged flow, at the device's rate | yes |
| SFQ(D), IOFlow's depth | plus the `D` requests already inside the device | yes, for `D` large enough to keep it busy |
| Gimbal virtual slots, Tectonic's in-flight cap | a large stream holds at most its share of normalized slots | yes: one tenant alone gets the slots that reach full bandwidth |
| mClock reservation, ReFlex LC tokens | a reserved rate in device-cost units, honored first | yes: unreserved capacity goes to shares / best effort |
| Tectonic Gold protections | a class-level stop on lower-class dispatch while Gold waits | yes, between Gold arrivals |
| Libra / ReFlex / IOFlow / mClock cost models | charges in measured device cost, so a large write is not one "request" | — |

Every row has the same shape: the delay a small request sees is the device's in-flight work plus
one largest unit per competitor, divided by the device's rate, and the large stream keeps full
bandwidth when nobody else is waiting. None of them needs to know which upload is "large".

---

## 5. Overload and admission, end to end

### 5.1 Credits and queueing delay

Breakwater (note 25 §13): clients send only with server-issued credits; the signal is queueing
delay ("when RPC service times have high dispersion, queue length is a poor indicator"); the
credit pool moves AIMD each RTT; unused credits are revoked with negative credits; the server
overcommits on speculated demand; spikes to 1.4× capacity converge "in less than 20 ms". Fail at
Scale and CoDel (note 25 §13) bound queue sojourn and switch to LIFO under a standing queue.

### 5.2 DAGOR: priority by business, then by hashed user

- Signal: "the average waiting time of requests in the pending queue (or queuing time for short)
  to profile the load status of a server"; WeChat refreshes it "every second or every 2000
  requests" with an overload threshold of 20 ms, "empirical configurations" [DAGOR §4.1, PDF p. 5].
- Business priority is set at the entry service and inherited along the call path; the table
  holds "only a few tens of entries" [DAGOR §4.2.1].
- User priority: "dynamically generated by the entry service through a hash function that takes
  the user ID as an argument. Each entry service changes its hash function every hour. As a
  consequence, requests from the same user are likely to be assigned to the same user priority
  within one hour, but different user priorities across hours." It gives "a relatively consistent
  quality of service for a long period of time" while "high priorities are granted to different
  users over hours of the day" [DAGOR §4.2.2, PDF p. 6].
- Why not sessions: "WeChat users often prefer to logout and immediately login again whenever
  they encounter service unavailability ... Through the logout and immediate login, user obtains
  a refreshed session ID. As a consequence, the session-oriented admission control assigns the
  user a new session priority ... it would introduce extra user requests due to the misleading
  logout and login, further deteriorating the overload situation" [DAGOR §4.2.2, PDF p. 7].
- Adaptive level: a compound level `(B, U)` with 128 user levels per business level; per period,
  a histogram of arrivals by level, and on overload the expected admitted count is cut by `α`,
  otherwise raised by `β` of incoming; "Empirically, we set α = 5% and β = 1%"; the cursor is set
  from the histogram's prefix sums in one step [DAGOR §4.2.3, PDF p. 8].
- Collaborative: "a server piggybacks its current admission level (B, U) to each response message
  ... whenever the upstream server intends to send request to the downstream server, it performs
  a local admission control on the request according to the stored admission level" [DAGOR §4.2.4,
  PDF p. 8].

### 5.3 Fail at Scale and CoDel

Quoted in note 25 §13: a queue that has not emptied in the last `N` ms limits sojourn to `M` ms;
adaptive LIFO under a standing queue; client-side caps on outstanding requests per service.
Their constants (5 ms / 100 ms) are cited for Facebook's services and the Internet's RTTs, not
measured for a storage node.

### 5.4 Aequitas: admit to a class with a probability, downgrade the rest

- "Aequitas, a distributed sender-driven admission control scheme that uses commodity
  Weighted-Fair Queuing (WFQ) to guarantee RPC-level SLOs. In the presence of network overloads,
  it enforces cluster-wide RPC latency SLOs by limiting the amount of traffic admitted into any
  given QoS and downgrading the rest." [AEQUITAS Abstract]
- Why the admitted share must be bounded: "at a certain share of QoSh, we observe priority
  inversion where delay in QoSh exceeds that of QoSl" [AEQUITAS §4.1, PDF p. 6].
- Mechanism: each channel keeps "an admit probability ... on a per-(src-host, dst-host, QoS)
  basis", raised while measured RPC network latency is within target and lowered otherwise, AIMD;
  "Aequitas downgrades the unadmitted RPCs and issues them at the lowest QoS level", and the
  downgrade "is explicitly notified to the application via an additional field in RPC metadata"
  [AEQUITAS §5.1, PDF p. 7].
- Result: SLO-compliant at the 99.9th percentile "even when network demand spikes 10× beyond
  provisioned capacity"; "10% average reduction in 99th-p RNL across fifty clusters" in production
  [AEQUITAS §1].

### 5.5 Homa: receiver grants and SRPT

- "SRPT provides near-optimal average message latency, and as shown in prior work, it also
  provides very good tail latency for short messages. Homa implements an approximation of SRPT"
  [HOMA §2].
- Receiver-driven: a message's first `RTTbytes` go unscheduled; the rest only against "GRANT
  packets" from the receiver [HOMA §3.1].
- Overcommitment: a receiver that grants one sender at a time wastes its downlink when that sender
  is busy elsewhere (pHost supports only "58% and 73%" load), so "Homa's receivers intentionally
  overcommit their downlinks by granting simultaneously to a small number of senders" [HOMA §2,
  PDF p. 4].
- The cost to large messages: "For the very largest messages, Homa produces 99th-percentile
  slowdowns of 100x or more. This is because of the SRPT policy. We speculate that the performance
  of these outliers could be improved by dedicating a small fraction of downlink bandwidth to the
  oldest message; we leave a full analysis of this alternative to future work." [HOMA §5, PDF p.
  9]

### 5.6 Size-based priority against fairness

- The theory (M/G/1, one server, independent jobs of known size): "if the load is less than half,
  then for every job size distribution, each job has a lower expected response time and slowdown
  under SRPT than under PS" [SRPT01 §1, Theorem 3]; for heavy-tailed sizes, "at least 99% of the
  jobs have a lower expected response time under SRPT than under PS"; "the expected response time
  under SRPT for any job is never more than 3 times that under PS, when the load is ≤ 0.8, and
  never more than 5.5 times ... when the load is ≤ 0.9" [SRPT01 §1, PDF p. 3]. Under overload,
  for load 1.5, "99% of jobs will experience a mean slowdown of only 4 under SRPT" — the bound
  covers "all but the largest 1% of the jobs" [SRPT01 §1].

**INFERENCE.** The owner's scenario breaks every assumption the favorable results need:

1. **It is overload**, and under overload SRPT's guarantee leaves out the largest jobs; the
   multi-terabyte upload is the largest job.
2. **Jobs are not independent.** A multipart upload is thousands of parts and tens of thousands
   of blocks; SRPT by request sees each block as mid-sized, by upload as the largest. Neither is
   the M/G/1 job.
3. **Size is chosen by the client.** SRPT is not strategy-proof: a principal that splits its
   work into small requests gains priority. DRF's strategy-proofness (§3.3) is the property that
   resists this, and agents are optimizers.
4. **Homa measured the outcome** for the largest messages: 100× slowdown, and its own proposed
   remedy is a floor for the oldest.

The resolution the sources support: fairness across principals (SFQ/DRFQ, weights from contracts),
and within a principal's share, any order that serves its own small requests first. The small
uploads of the spike get low delay from fair queueing's decoupling of delay from rate (§3.1–§3.2)
and from bounded dispatch units (§4.9), not from outranking the large upload; and the large
upload keeps at least its fair share, which is Homa's "fraction to the oldest message" made exact
by weights.

### 5.7 Retries and metastable failure

- Definition: "Metastable failures occur in open systems with an uncontrolled source of load where
  a trigger causes the system to enter a bad state that persists even when the trigger is removed
  ... there is a sustaining effect—often involving work amplification or decreased overall
  efficiency—that prevents the system from leaving the bad state." [META-HOTOS §1]
- The retry example: a database that serves under 100 ms below 300 QPS, an application that
  retries after 1 s, a 10 s outage, and afterwards "client queries will continue at 560 QPS due to
  retries. This will prevent the database from recovering." [META-HOTOS §2, PDF p. 2]
- In the wild: "By far, the most common sustaining effect is due to the retry policy, affecting
  more than 50% of the studied incidents" [META-OSDI §3, PDF p. 4]; "a policy with at most two
  retries will not amplify the work more than three times, while the policy with no cap
  effectively leaves the system with no stable region" [META-OSDI §5, PDF p. 9].
- The client side is fixed by the SDKs: standard mode caps attempts at three with full jitter and
  a retry quota; adaptive mode rate-limits per client instance (note 25 §3, §13). S3's own
  guidance for latency-sensitive callers is to retry, aggressively: "When you make large variably
  sized requests (for example, more than 128 MB), we advise tracking the throughput being achieved
  and retrying the slowest 5 percent of the requests. When you make smaller requests (for example,
  less than 512 KB) ... a good guideline is to retry a GET or PUT operation after 2 seconds."
  [S3-PATTERNS, "Timeouts and retries for latency-sensitive applications"]

**DERIVED.** Offered load with retries is `λ·(1 + ρ)` for retry ratio `ρ`; the system stays in its
stable region only while `λ·(1 + ρ) ≤ C`. The retry ratio the gateway can afford is therefore
`ρ_max = C/λ − 1`, computed from measured capacity and measured first-attempt load, not a fixed
percentage. The `amz-sdk-request` attempt number (note 25 §13) tells the gateway which requests
are retries.

---

## 6. Making one large upload as fast as it can be without dominating

### 6.1 Parallelism from rate, latency and failure cost

`gateway.md` §2 and audit §16.2–§16.3 derive the pieces; they are recalled here only to connect
them to fairness:

- A PUT's block window `w = ⌈R·T/B⌉`: body rate `R` times one block chain's latency `T` over a
  block's bytes `B`, capped by memory (`node.md` §2.6). By Little's law it sustains `R`.
- The part size `p* ≈ g·sqrt(2h/λ)` under the audit's Poisson-interruption model, checked against
  the 10,000-part ceiling (audit §16.2); the server recommends, the S3 client decides.
- AWS's guidance on concurrency is a measurement procedure, not a number: "We recommend starting
  with a single request at a time. Measure the network bandwidth being achieved and the use of
  other resources ... You can then identify the bottleneck resource (that is, the resource with
  the highest usage), and hence the number of requests that are likely to be useful. For example,
  if processing one request at a time leads to a CPU usage of 25 percent, it suggests that up to
  four concurrent requests can be accommodated." [S3-PATTERNS, "Horizontal scaling and request
  parallelization"]

**INFERENCE.** The `R` in `w = ⌈R·T/B⌉` is the rate the upload is *entitled* to, not the rate
its client can push: `R = min(client body rate, the principal's fair share across the authorities
the upload uses)`. With that one substitution the window that makes the upload fast when alone is
the window that makes it yield when others arrive: its fair share falls, `R` falls, `w` falls, and
the memory it held returns to the authority.

### 6.2 Spreading one upload's chunks

- Tectonic's placement hands the client a copyset per block from about a hundred shuffles of all
  disks, and writes reserved ahead of the data go "to the first nodes to accept the reservation",
  avoiding nodes where "the requester exceeded its resource share on that node" (note 01 §1.8,
  §1.10).
- Two random choices: "Having d = 2 choices leads to exponential improvements in the expected time
  a customer spends in the system over d = 1, whereas having d = 3 choices is only a constant factor
  better than d = 2." [P2C Abstract, p. 1094]
- The herd warning: probing and sending to the least-loaded server "can be beneficial but is less
  effective than submitting work to two queues simultaneously for three main reasons: load levels
  can change between probe and request time; request service times can be difficult to estimate
  ...; and clients can create temporary hot spots by all clients picking the same (least-loaded)
  server at the same time." [TAIL, "Tied requests", PDF p. 5]
- Gimbal chooses the replica with more credits, credits being normalized load (§4.7).

**INFERENCE.** A large upload spreads by drawing each block's chunks from placement's candidate
set with two random choices weighted by advertised credits, never from a global least-loaded
ranking, which every gateway would read alike in an agent herd. Spreading is also what makes it
yield cheaply: its load on any one device is a small fraction of the device, so the newcomers find
most devices only partly occupied by it.

### 6.3 Hedged and tied requests

- "defer sending a secondary request until the first request has been outstanding for more than
  the 95th-percentile expected latency for this class of requests. This approach limits the
  additional load to approximately 5% while substantially shortening the latency tail." In a
  BigTable benchmark, "sending a hedging request after a 10ms delay reduces the 99.9th-percentile
  latency for retrieving all 1,000 values from 1,800ms to 74ms while sending just 2% more
  requests. The overhead of hedged requests can be further reduced by tagging them as lower
  priority than the primary requests." [TAIL, "Hedged requests", PDF p. 4]
- Tied requests enqueue on two servers that cancel each other when one starts; the client waits
  "two times the average network message delay" first; disk overhead "is less than 1%" [TAIL,
  "Tied requests"].
- Tectonic's reservation-hedged writes are tied requests for writes: reservations to 19 nodes,
  data to the first 15, "~20% improvement in 99th percentile latency ... in a test cluster with 80%
  throughput utilization" (note 01 §1.8).

**DERIVED.** Hedging after the `q`-quantile adds at most `1 − q` of extra load by construction;
the quantile is the knob, and its value follows from the load the cell can spare, which the
device authorities measure. Unbounded hedging under congestion makes congestion worse (audit
§13.5), so hedges are charged to the same principal share as primaries.

### 6.4 Yielding instantly, reclaiming quickly (DERIVED)

Let a device have measured rate `C` (bytes/s of its cost unit), an in-flight budget `D_b` (bytes,
from calibration: the smallest in-flight at which throughput stops growing, `chunk-store.md` §7),
and a largest dispatch unit `u`.

- **Yield.** When the spike's first small request arrives at a device the large upload has to
  itself, it is next in start-tag order at the next dispatch. It waits for at most the in-flight
  bytes to drain and one unit to finish: `t_yield ≤ (D_b + u)/C`. With `n` backlogged principals,
  each holding at most its share of slots, the bound is `(D_b + n·u)/C` in the worst arrival order
  (§3.1, §4.3), and with Gimbal-style slots the large upload holds at most `D_b/n` of the in-flight
  bytes once `n` tenants contend.
- **Upstream of the device**, the gateway's window for the upload shrinks to its new fair share at
  its next block (§6.1); bytes already in QUIC send buffers drain within the bulk class's one tail
  RTT of queued bytes (`node.md` §3.8). Toward the client, a native client's credit for further
  parts shrinks and its stream window stops advancing; a stock client behind the HTTP listener
  sees its TCP window close as the listener stops reading the body.
- **Reclaim.** When the spike ends, the device is work-conserving: the large upload's next dispatch
  gets the whole device. The gateway's window regrows to `⌈R·T/B⌉` at the next admission, and the
  blocks it then sends take one chain latency `T` to reach the devices, so goodput recovers in
  about one `T` plus whatever the client's own congestion window needs. Both are measured in §10.
- **What must not happen.** A large part that is refused, rather than slowed, restarts from its
  first byte: at 64 kbit/s a 256 MiB part is 9.32 hours (audit §16.2). Admitted bodies are slowed
  by flow control — QUIC's `MAX_STREAM_DATA` on the native protocol, TCP's window behind the HTTP
  listener — and refusals happen only at admission, before the body: a typed refusal or withheld
  credit natively, `503 SlowDown` before `100 Continue` on HTTP (§7.1; `node.md` §4.2, §4.5).

At the large end `u` must not be a whole 8 MiB chunk on a slow device: at 200 MB/s one 8 MiB unit
is 42 ms of head-of-line delay for every small request behind it (DERIVED). The dispatch unit is
therefore the smallest transfer that reaches the device's bandwidth, which calibration already
measures for the chunk size (`gateway.md` §1: "Measuring where a device's transfers reach its
bandwidth ... is what would replace the cited value").

---

## 7. Where each mechanism lives in the hybrid architecture

### 7.1 Clients and the gateway: two front doors, one admission authority

The owner's decision (2026-09-30) is that clients reach mantle by two paths, both on by default
and both first-class: mantle's own QUIC protocol, spoken by mantle's client library and CLI, which
carries S3 semantics natively (as slates' runtime speaks its own protocol, note 08); and an
HTTP/1.1 S3 compatibility listener that translates stock S3 requests onto the same operations.
Nodes talk to each other over the native protocol only (`node.md` §3). The two paths differ in
what the client can be told and what it will do, so the admission authority is one and the
signalling is two.

**What both paths share (INFERENCE).** One admission decision per request, made by the same
authorities (`node.md` §2.5) with the same charge (§9 D1), the same fair-queue tree (D3) and the
same shedding order (D6). A principal's share does not depend on which door its requests use, and
a principal cannot gain share by splitting traffic between them; the compatibility listener is a
translator into the native operation, not a second scheduler.

**What the native protocol can carry that HTTP cannot.**

- **Credits before data.** The gateway can grant a client library credits per class, Breakwater's
  form (note 25 §13), and revoke them; a client sends a part's body only against credit, so a
  refused part costs no bytes and the client's own concurrency follows the server's grant rather
  than a fixed transfer-manager setting. This is Tectonic's argument for throttling in the client
  library, "before they make a potentially wasted request" (note 01 §1.11).
- **The admission level, piggybacked.** Responses carry the gateway's current admission level and
  the client library sheds locally against it, DAGOR's collaborative admission extended one hop
  outward [DAGOR §4.2.4].
- **A typed refusal with a stated wait.** A refusal names its cause (the principal's share, a
  range splitting, a cell out of space) and, where the cause is a queue, a wait derived from its
  measured drain time. S3 offers only `SlowDown` and an `x-amz-retry-after` header whose use by S3
  is unverified (note 25 §3).
- **The upload plan from the server.** The client library can take the part size and window the
  gateway computes (audit §16.2; D7) instead of choosing its own; a stock S3 client "chooses its
  UploadPart boundaries" and the server can only recommend (audit §16.2).
- **Retry identity and budget in the library.** Attempts carry the operation's identity and
  attempt number by construction, and the library holds a retry budget, so the gateway does not
  depend on SDK headers to see retries.
- **Server-assigned stream classes.** The class of a stream is decided by the gateway from the
  principal's contract, as between nodes (`node.md` §3.1); a client cannot raise its own.

**What the HTTP/1.1 listener maps onto (primary sources for the client side).**

- S3 documents a per-prefix floor ("at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD
  requests per second per partitioned Amazon S3 prefix"), gradual scaling with `503 Slow Down`
  meanwhile, and asks clients to ramp up and spread prefixes (note 09 §6.2–§6.3; S3-PATTERNS,
  "Optimizing for high-request rate workloads"). Clients are told to "Scale storage connections
  horizontally" [S3-GUIDE], so the listener sees concurrency it did not choose.
- A refusal maps to `503 SlowDown` and is given before the body under `Expect: 100-continue` (note
  25 §3; `node.md` §4.2); an admitted body is slowed by TCP flow control, never refused for load.
- The listener knows the principal and tenant from SigV4, the operation, the declared length, the
  upload ID and part number, and, from the AWS SDKs, the `amz-sdk-request` attempt number and
  `amz-sdk-invocation-id` (note 25 §13).
- Behind the listener, many HTTP connections from many clients are coalesced onto the native
  operations, which is RFC 9218's coalescing intermediary: "the asymmetry between the priority
  declared by multiple clients might cause all responses going to one user agent to be delayed
  until all responses going to another user agent have been sent. In order to mitigate this
  fairness problem, a server could ... distribute bandwidth (for example, in a round-robin
  manner)" [RFC9218 §13.1]. Priorities a client declares are inputs, never authority (audit §13.3).

**Where tenant fairness is decided.** At the gateway on either path, the only place that sees all
of a request's resource needs before any is spent, which is why `node.md` §2.3 puts it there.

### 7.2 Cell routing

Cells isolate failures and deployments; they are large, and a bucket usually maps to one cell
(`architecture.md` §2, §4). The router is thin and does no admission (`architecture.md` §5; note
09 §0 item 4). Load across cells is balanced by placing new buckets and, rarely, moving or
splitting a bucket's ranges between cells (`architecture.md` §6.1, §7). For the owner's scenario
the cell is the unit that has the capacity; spreading a hot bucket across cells is a region-step
remedy for a bucket that outgrows one cell, not a per-request mechanism.

### 7.3 Metadata ranges

- **Capacity per range is finite and measured.** Tectonic: "each shard can serve a maximum of 10
  KQPS", enforced by "the isolation mechanism on metadata nodes"; about 1% of Name shards hit it
  for hot directories, and refused requests "are retried after a backoff" (note 01 §1.12).
- **Split for observed heat, not for one key or a sequence.** DynamoDB chooses the split point
  "based on key distribution the partition has observed" and avoids splitting for "high traffic to
  a single item" or sequential access (note 09 §3.8); S3 splits for "sustained high request rates",
  sometimes into many children at once, and answers SlowDown meanwhile (note 09 §6.2).
- **Per-partition limits dilute on split; per-tenant admission does not.** DynamoDB moved from
  per-partition allocations to global admission control because splitting "can result in the hot
  portion of the partition having less available performance than it did before the split", and
  kept partition caps "for defense-in-depth" (note 09 §3.6).
- **Avoid creating the herd.** Tectonic's list API returns file IDs with names so workers open
  files "without querying the directory shard again", avoiding the orchestrator-lists-then-workers-
  open anti-pattern (note 01 §1.12). An agent swarm that lists a prefix and then HEADs every key is
  the same pattern; LIST answers that already carry what the next step needs remove a herd.
- **Shard Manager balances by solver under move budgets**, emergency mode for unavailability and a
  periodic mode that never worsens soft goals (note 09 §7.3.5).
- **Within a range, fairness today is among ranges, not tenants** (`node.md` §2.3): the shard's
  deficit round robin keeps one hot range from starving another's heartbeats; a range's own
  command queue is FIFO in gateway admission order and refuses `Busy` past its Little's-law bound.

### 7.4 Storage nodes

`node.md` §2.5 already has one admission authority per resolved bottleneck, with per-device
shares for foreground, log, engine flush, cleaner, scrubber, repair and snapshots, and holds a
reservation from admission to answer. What §4 adds to it: costs in the device's measured units
(Libra, ReFlex, IOFlow, mClock), a dispatch depth from calibration (SFQ(D), IOFlow), per-principal
slots that a lone stream fills and contenders split (Gimbal), and class protections (Tectonic's
Gold rules, mClock reservations).

### 7.5 The network

- quinn's priorities are strict between levels and round-robin within one (note 25 §4); `node.md`
  §3.1 orders control, replication, request, bulk.
- RFC 9218's urgency is 0–7, "The default is 3", with 7 "reserved for background tasks"; servers
  are asked to avoid starving incremental responses behind large non-incremental ones: "It is
  RECOMMENDED that servers avoid such starvation where possible" [RFC9218 §4.1, §10].
- The native client protocol is QUIC end to end, so the same machinery serves clients: a stream
  per request with its class set by the gateway, connection and stream windows from the gateway's
  memory authority (`node.md` §3.3's `min(BDP, share)`), and quinn's `set_receive_window` and
  `set_max_concurrent_bi_streams` to follow a client's share on a live connection (note 25 §4).
  RFC 9218 applies only behind the HTTP listener, where stock clients may send it.
- Receiver-driven credit (Homa's grants, Breakwater's credits, Gimbal's piggybacked credits) is
  how a storage node keeps gateways, and the gateway keeps native clients, from sending more than
  the authorities have reserved;
  `node.md` §3.3 already sizes receive windows as `min(BDP, share)` and reads bytes only into
  budgeted stages.

### 7.6 A principal's share across the fleet without a global bottleneck

| System | What is exchanged | Who decides | Staleness, error |
|---|---|---|---|
| Tectonic (note 01 §1.11) | "near-realtime distributed counters" of demand per tenant and TrafficGroup in "the last small time window" | each client library, leaky bucket, own group first, then surplus by class | not published |
| DynamoDB GAC (note 09 §3.6) | routers replenish "a local token bucket" from GAC "at regular intervals (in the order of few seconds)"; GAC state is ephemeral and restartable | each request router | a few seconds |
| DRL [DRL §3] | each limiter's measured arrival rate, by gossip; "a full mesh is also extremely bandwidth-intensive (requiring O(N²) update messages per estimate interval)" | each limiter: FPS sets the local limit in proportion to local demand weights so allocations are max-min fair; GTB "is highly sensitive to stale observations" | "cannot be simultaneously perfectly accurate and responsive" [DRL §1, PDF p. 2] |
| dmClock [MCLOCK §3.2.1] | two integers on each request: service received elsewhere | each storage server, in its tags | "inaccurate by up to 1 request at each of the other servers" |
| CSFQ [CSFQ Abstract] | a rate label on each packet, set at the edge | core routers, which "maintain no per flow state" | the edge's rate estimate |
| DAGOR [DAGOR §4.2.4] | the downstream admission level, on each response | the upstream, locally | one response old |

**INFERENCE.** All six keep the decision local and move a summary. Three of them (dmClock, CSFQ,
DAGOR) piggyback the summary on traffic that flows anyway, so they cost no messages and no
central service, and with one participant they reduce exactly to a local scheduler: dmClock with
no other servers sends ρ = δ = 0 extra; CSFQ with the edge and core on one host is fair queueing;
DAGOR with no upstream is local admission. The two that need a service or gossip (GAC, DRL) are
the ones to add only when a tenant's requests arrive through more than one gateway.

---

## 8. Agentic scale: bounded per-principal state and resistance to herds

### 8.1 A fair queue's state is bounded by admitted work (DERIVED)

SFQ's tags are `S = max{v(A), F_prev}`, `F = S + l/r` (note 25 §11). For a principal with nothing
queued, only `F_prev` is remembered, and once virtual time `v` has passed it, `S = max{v, F_prev} =
v` for any later arrival: forgetting `F_prev` changes no future tag. A table therefore needs an
entry only for principals that are backlogged or whose last finish tag is ahead of `v`, and when
the table is full, evicting the entry with the smallest `F_prev − v` misstates that principal's
next start tag by at most one request's `l/r`, the same slack SFQ's Theorem 1 already allows.
Backlogged principals are at most the admitted requests `Q = λ·d` (§2.2), so the table's size is
bounded by the gateway's admission, not by `P`. This is the same memorylessness DRFQ requires
for starvation-freedom (§3.3).

### 8.2 Rate over a window: Space-Saving and count-min

- **Space-Saving** keeps `m` counters. "Among all counters, the minimum counter value, min, is no
  greater than N/m", "0 ≤ ε_i ≤ min, i.e., f_i ≤ (f_i + ε_i) = count_i ≤ f_i + min", and "An
  element E_i with F_i > min, must exist in Stream-Summary" [SPACESAVING Lemma 2–3, Theorem 1, PDF
  p. 6]. When the distinct elements do not exceed `m`, "all counts are exact, and the problem is
  trivial" [SPACESAVING §3]. Every principal above `N/m` of the window's `N` requests is found and
  counted within `N/m`.
- **Count-min** with width `w = ⌈e/ε⌉` and depth `d = ⌈ln(1/δ)⌉` answers a point query with
  `a_i ≤ â_i` and, "with probability at least 1 − δ, â_i ≤ a_i + ε‖a‖₁" [CM05 §3–§4, Theorem 1].
  Sketches are "linear functions of their input ... it is easy to compute certain functions on data
  that is distributed over sites, by casting them as computations on their sketches" [CM05 §1]: two
  gateways' count-min arrays with the same hashes add cell-wise into the cell's.
- **The errors point one way.** Both overestimate. A rate limit enforced on an overestimate
  throttles an innocent principal by at most the error, so the error must be small against the
  smallest limit enforced: `N/m ≪ limit` or `ε‖a‖₁ ≪ limit` (DERIVED).

### 8.3 State cost per structure (DERIVED; entry sizes are estimates for a layout, to be measured)

| Structure | Keyed by | Entries | Bytes per entry | Bound | Example |
|---|---|---|---|---|---|
| Fair-queue table (§8.1) | principal, tenant | backlogged + recently served | ~48 | `Q = λ·d` | `λ = 10⁵/s`, `d = 10 ms`: 1,000 entries, ~48 KB |
| Tenant contracts (mClock R/L/P tags) | tenant | configured tenants | ~64 | the cell's tenant count, set by the control plane | 10⁵ tenants: 6.4 MB |
| Heavy-hitter table (Space-Saving) | principal | `m` | ~48 | `m` from memory share and target error `N/m` | `m = 10⁴`: 480 KB; finds any principal above 0.01% of the window |
| Count-min (only where principal limits span gateways) | principal | `w·d` | 4–8 | `ε`, `δ` | `ε = 10⁻⁵`, `δ = 10⁻³`: `w = 271,829`, `d = 7`, 7.6–15 MB |
| DAGOR levels | hash of principal | levels × classes | 8 | a configured number of levels | 128 × a few classes: a few KB |
| dmClock counters at a gateway | (tenant, node) pairs with traffic | active pairs | ~16 | tenants × nodes in the cell, active only | — |
| Metadata sessions | gateway incarnation × range | `node.md` §5.3 | — | independent of principals | — |

Nothing in the table grows with `P`. The persistent per-principal rows that do exist are S3's
own: in-progress multipart uploads and their parts. Agents that crash in a loop leave abandoned
uploads; the S3 mechanism the earlier notes record is the lifecycle rule
`AbortIncompleteMultipartUpload` (note 05 §4.8), and whether S3 caps in-progress uploads per bucket
is **UNVERIFIED**. mantle needs its own stated bound (§11 item 7).

### 8.4 Herds, storms and loops

- **Correlated bursts.** A swarm fanning out one step sends many principals' requests at once to
  one prefix. Fair queueing divides the range's or device's capacity among them; it does not
  reduce the offered load. What keeps goodput up is admitting a consistent subset so admitted
  members finish their step (DAGOR's hashed principal priority, §5.2), and telling the rest early
  and cheaply: withheld credit and a typed refusal with a derived wait to native clients, which
  the client library honors, and `503 SlowDown` before the body to stock clients, which SDKs back
  off with jitter.
- **Retry storms.** Shed retries before first attempts under overload, by the attempt header; hold
  the retry ratio below `ρ_max = C/λ − 1` (§5.7).
- **Runaway loops.** One principal re-issuing the same request at machine speed is a heavy hitter;
  Space-Saving finds it in bounded memory (§8.2) and the fair queue caps it at its share of its
  tenant's share. Its tenant's other principals are untouched.
- **Session churn.** Every per-principal key is the authenticated principal (access key or role),
  never a session token, which agents rotate (§5.2's re-login finding).

---

## 9. Proposed design for mantle

Each decision gives what it is, the sources, the measurement that would confirm it, where its
numbers come from, its form at the smallest step and its fixed cost there, and how it scales.
Decisions are grouped by the step that first needs them; nothing from a later group is built,
configured or paid for at an earlier step, though it is the same binary and code path: a
mechanism of a later step is either absent from the running configuration (no peers, no second
gateway, no second cell) or is a generalization whose one-participant form is the earlier step's
mechanism.

### Step 1 — one laptop: one process, one node, one device, a few cores

What appears: one device carries the log, the engines, the chunks, the cleaner and the scrubber;
memory is a few GB; a laptop can host an agent swarm of its own, so many principals and the
owner's scenario both occur here; and both front doors are open, the CLI on the native protocol
over loopback and stock S3 tools on the HTTP listener.

**D1. Every request is charged its dominant share, in measured units.** Each authority
(`node.md` §2.5: memory, the device, the coding pool, the metadata shard) prices a request in its
own cost unit from a calibrated model — device cost as a function of operation, size and read/write
mix (ReFlex's form, Libra's VOP, IOFlow's measured token price), metadata cost as commands and
encoded bytes. The request's charge in the fair queue is its largest cost over the authorities it
reserves from, each over that authority's measured capacity (DRF's dominant share in DRFQ's
memoryless form). Unknown costs (LIST, a completion) are priced by 2DFQ's decaying per-tenant,
per-operation maximum and corrected after completion.
*Sources:* DRF §1–§4; DRFQ §3, §5.4; LIBRA §1; REFLEX §3.2.1; IOFLOW; MCLOCK §3.1; 2DFQ §5 (note 25
§12). *Confirm:* per-principal min-max ratio of dominant shares under the §10 mixes, against the
same run charged in bytes and in requests. *Numbers:* cost coefficients from calibration of the
running device, re-measured as its state changes (`node.md` §2.6); no coefficient is a constant.
*Laptop:* one device and one shard, so the vector has three or four entries; the cost model is a
table per operation type from calibration, already run at format. *Scales:* the vector grows with
the authorities a request touches; at a cell the remote device's cost arrives with its credits
(D9).

**D2. One dispatcher per physical device, with a calibrated in-flight budget and bounded dispatch
unit.** The device authority dispatches to the device in start-tag order, keeping at most `D_b`
bytes and `D_n` operations in flight, where `D_b`, `D_n` are the smallest in-flight values at which
calibrated throughput stops growing (SFQ(D)'s trade-off resolved by IOFlow's measurement); requests
beyond it wait in the authority's queues, where order can still be chosen. A large write is
dispatched in units no larger than the smallest transfer that reaches the device's bandwidth (the
same measurement that would replace the 8 MiB chunk size, `gateway.md` §1). Control and durable
completions keep `node.md` §2.3's precedence.
*Sources:* SFQD §1, §3; IOFLOW (90 outstanding for 95% utilization, measured); PARDA §3.2 (window
bounded by measured latency); `chunk-store.md` §7. *Confirm:* small-request p99 against `(D_b +
n·u)/C` (§6.4) while one large stream runs; throughput loss against the device's calibrated
plateau. *Numbers:* calibration, per device, re-run on the conditions `node.md` §2.6 names.
*Laptop:* one dispatcher; the log's flushes and the engine's compactions take shares of the same
authority, which is the shared-device case audit §14.1 requires. *Scales:* one dispatcher per
device; nothing is shared between devices.

**D3. Hierarchical start-time fair queueing: tenant, then principal, then the principal's own
requests, with state only for active principals.** At the gateway's admission and at each device
dispatcher, SFQ runs recursively (HSFQ96): tenants by contract weight, principals equally within a
tenant unless the tenant sets weights, and within a principal, its requests smallest-charge first
(size priority confined to the principal's own share, §5.6). The principal level keeps entries per
§8.1, bounded by admitted work; the key is the authenticated principal, never a session.
*Sources:* SFQ96, HSFQ96 (note 25 §11); WFQ89; GPS93; SRPT01; HOMA §5; DAGOR §4.2.2. *Confirm:* in
the owner's scenario, the large upload's goodput during the spike is within measurement of its
weighted fair share, and small uploads' latency is within measurement of the same small uploads
run alone plus the §6.4 bound. *Numbers:* tenant weights are contract (operator configuration);
principal weights equal by default; the table bound is `Q` from Little's law. *Laptop:* one tenant
unless configured otherwise, so the tree has one interior node and as many principal leaves as are
active; its memory is tens of KB (§8.3). *Scales:* depth stays three; H-WF²Q+ replaces SFQ at a
level only if measured delay bounds at that level need its tighter worst-case index (HPFQ96).

**D4. Classes by contract, not by size, with reserved latency, a bulk floor and a bounded
high-class share.** A tenant's traffic group carries a class (Tectonic's Gold/Silver/Bronze form);
Gold gets an mClock reservation in device-cost units and Tectonic's protections (in-flight cap on
lower classes while Gold waits); background work (repair, cleaning, scrubbing) has the floors
`node.md` §2.6 and §3.3 already derive. The admitted share of each upper class is controlled by
Aequitas's admit probability on measured latency against the class's target, downgrading the
excess rather than refusing it.
*Sources:* TEC §4.1 (note 01 §1.11); MCLOCK §3; REFLEX §3.2.2; AEQUITAS §4.1–§5.1; audit §13.3.
*Confirm:* Gold small-request p99 with and without a concurrent bulk upload; absence of priority
inversion as the Gold share is swept. *Numbers:* targets are the tenant's contract; the admit
probability is a controlled variable, not a setting; floors are derived. *Laptop:* with no contract
configured every request is one class and the class layer is a single node of the tree; repair has
nothing to repair. *Scales:* classes are a handful per cell, as Tectonic's ~50 TrafficGroups per
cluster show cardinality must be kept low (note 01 §1.11).

**D5. Overload is measured as sojourn; one admission decision, signalled per path.** The
gateway's admission authority, shared by the native listener and the HTTP/1.1 compatibility
listener, refuses when its queue's minimum sojourn over an interval exceeds a target derived from
measured service time (CoDel's test, `node.md` §2.6). On the native protocol the refusal is
expressed first as credit: the gateway grants each client connection credits per class from the
authority's free reservation, revokes unused credit when sojourn rises and grants again when it
falls (Breakwater's form), and answers a request sent without credit with a typed refusal naming
its cause and a wait derived from the queue's measured drain time. On the HTTP listener the same
decision is a `503 SlowDown` before `100 Continue`. On both, admitted bodies are never refused for
load; they are slowed by flow control (§6.4).
*Sources:* RFC8289, FAILSCALE, BREAKWATER (note 25 §13); S3-REDIRECT, S3-ERR, SDK-RETRY (note 25
§3); TEC §4.1 (note 01 §1.11); audit §16.2. *Confirm:* goodput, refusal counts and credit waste
under a step to 1.5× and 10× measured capacity, open loop, corrected for coordinated omission
(audit §15.3), with native and stock S3 clients mixed in the same run and their per-principal
shares compared. *Numbers:* derived per `node.md` §2.6; credit pool from reservations; the
grant-and-revoke step from measured RTT and sojourn. *Laptop:* one queue; the CLI's connection
gets credits over loopback QUIC, and the HTTP listener's refusals come from the same queue.
*Scales:* one authority per gateway; nothing shared between gateways (D13 divides tenants'
contracts among them).

**D6. Shed by hashed principal, retries first, at a measured rate.** Under overload, the admission
level moves over a histogram of `(class, retry?, h(principal, epoch))`, DAGOR's compound level with
the attempt number as an added key: retries are shed before first attempts, and among first
attempts a deterministic subset of principals is shed so admitted ones complete their steps. The
cut per interval is the measured excess (`1 − C/λ_offered`), not DAGOR's empirical 5% and 1%; the
epoch rotates over a period no shorter than the measured 99th-percentile length of a principal's
burst, so one step is not split across epochs.
The attempt number comes from the client library on the native protocol and from the
`amz-sdk-request` header behind the HTTP listener; an HTTP request without it counts as a first
attempt. Native responses carry the admission level so the library sheds locally (DAGOR §4.2.4).
*Sources:* DAGOR §4.2.2–§4.2.4; META-OSDI §3, §5; SRE-OVERLOAD, SDK-GO, BOTOCORE (note 25 §13).
*Confirm:* the herd and retry-storm runs of §10: goodput of completed multi-request steps, time to
leave overload after the trigger is removed. *Numbers:* histogram levels sized so the finest step
of the cursor is at or below the measured resolution of the overload signal; period from measured
burst lengths. *Laptop:* a histogram of a few hundred counters, one hash per request. *Scales:*
per gateway, independent.

**D7. A large upload's window is its fair share times its chain latency.** `w = ⌈R·T/B⌉` with `R`
the upload's entitled rate (§6.1), recomputed at each block; the memory authority's completion
lane (`node.md` §2.5) keeps what admitted blocks need to finish. A native client library takes
the part size from audit §16.2's model and its part concurrency from its credits, so both follow
the server; behind the HTTP listener the stock client keeps its own part size and concurrency and
the window acts on how fast the listener reads each body.
*Sources:* `gateway.md` §2; audit §16.2–§16.3; S3-PATTERNS (concurrency from the bottleneck's
use). *Confirm:* single-upload goodput against `min(client rate, device plateau)` when alone;
window size and goodput during and after the spike; recovery time against one chain latency.
*Numbers:* all measured. *Laptop:* `T` is local and short, so `w` is small; the CLI uploads with
the plan the gateway gives it. *Scales:* `w` grows
with path latency and rate (75 blocks at 100 Gbit/s and 300 ms, audit §16.3).

**D8. Heavy-hitter accounting in bounded memory, exact when small.** Each gateway keeps a
Space-Saving table of `m` principals over a window, `m` from the gateway's memory share and the
error `N/m` against the smallest enforced per-principal limit. It finds runaway principals and any
principal above a tenant-configured cap; when the active principals fit in `m`, counts are exact.
*Sources:* SPACESAVING Lemma 2–3, Theorem 1. *Confirm:* detection delay and count error for a
runaway principal among 10⁶ churning principals in the §10 run. *Numbers:* `m` derived; the cap is
tenant configuration. *Laptop:* `m` small and usually not full, so exact. *Scales:* per gateway;
merged views are D11's.

**D9. Metadata ranges queue by tenant and split for sustained, spread heat.** A range's command
queue (`node.md` §2.2) becomes SFQ over tenants with D3's bounded table, bounded in bytes as now;
`Busy` maps to SlowDown. A range splits at an observed key when its measured sustained load
exceeds its measured capacity and the load is spread over keys, never for one key or a sequence
(DynamoDB, S3). Each answer carries the range's admission level so gateways shed locally first
(DAGOR §4.2.4).
*Sources:* note 09 §3.6, §3.8, §6.2; note 01 §1.12; DAGOR §4.2.4; `architecture.md` §6, §11.
*Confirm:* a herd on one prefix: refusals before and after the split, time to split, and other
tenants' latency on the same range. *Numbers:* range capacity measured per engine and device (the
cited 10 KQPS is an order, not a value); "sustained" is the split's own duration times a margin
derived from the split's measured cost. *Laptop:* splitting helps only when another shard has idle
cores; with one core it never triggers. *Scales:* unchanged per range; Shard Manager-style
balancing moves children at the cell step.

**D10. Per-request cost is a capacity limit, measured and reduced.** Report CPU, allocations and
metadata commands per small PUT; batch commands bound for one range into one entry per turn
(`node.md` §2.2); keep small-object packing (Tectonic's log-structured blob files, note 01 §1.9) as
an evaluated option, not a default (audit §14.4's ownership rules first).
*Sources:* note 01 §1.9; `gateway.md` §4. *Confirm:* small PUTs per core and per metadata range,
before and after batching. *Laptop:* the same measurements. *Scales:* determines `λ_range` and
gateway cores per request rate.

### Step 2 — one node: many devices, many cores

What appears: a choice of device for every chunk; devices that differ; many range shards.

**D11a. Choose devices by two random choices on credits.** Each block's chunks come from
placement's candidates, two drawn at random per chunk, the one with more advertised credit taken
(Gimbal's credit-as-load, Mitzenmacher's d = 2). Never a global least-loaded order (TAIL's herd).
*Sources:* P2C; TAIL; GIMBAL §4; note 01 §1.10. *Confirm:* per-device load spread and small-request
p99 against random placement and least-loaded placement under the same run. *Numbers:* none.
*Laptop:* one candidate device: the choice is trivial and costs nothing. *Scales:* candidates from
the cell's placement at step 3.

**D11b. Device costs per device, not per class of device.** Each device keeps its own calibrated
cost model (Libra, ReFlex calibrate per device type; Gimbal adapts write cost online) and its own
`D_b`. *Laptop:* one model. *Scales:* linearly in devices, a table each.

### Step 3 — one cell: several nodes and gateways

What appears: remote devices; a tenant's requests arriving at several gateways and touching
several nodes; links that congest; repair.

**D12. Storage nodes grant credits, gateways piggyback service received.** A node's authorities
grant per-gateway, per-class credits on responses (Gimbal's piggybacked credits, Breakwater's
grant-and-revoke with overcommit, Homa's receiver-driven grants), and revoke unused credit when
their queues' delay rises. Each chunk write and command carries dmClock's two counters for its
tenant: service the tenant received at other nodes since its last request here, so each node's
tags reflect the tenant's cell-wide share with no synchronization.
*Sources:* MCLOCK §3.2.1; GIMBAL §3; BREAKWATER (note 25 §13); HOMA §2–§3; `node.md` §3.3.
*Confirm:* tenant shares across nodes when one tenant's load lands on few nodes; credit waste and
convergence after a spike. *Numbers:* credit pool from the authority's reservations; AIMD steps
from measured RTT and delay. *Laptop:* the loopback peer's credit is the reservation itself
(`node.md` §5.5) and the counters are zero: no messages, no state. *Scales:* state per active
(tenant, node) pair at each gateway.

**D13. Tenant tokens across gateways by demand-proportional division.** A cell admission service,
stateless and restartable like DynamoDB's GAC, divides each tenant's contract among the gateways
serving it in proportion to their measured demand (DRL's FPS), refreshed on an interval whose
length trades accuracy for responsiveness as DRL states; gateways keep admitting from their last
grant if it is unreachable (`architecture.md` §9).
*Sources:* note 09 §3.6; DRL §3; note 01 §1.11. *Confirm:* a tenant's cell-wide rate against its
contract when its load moves between gateways; error against the interval. *Numbers:* the
interval from the measured rate of demand change and the error the contract tolerates. *Laptop:*
one gateway: its grant is the whole contract and no message is sent. *Scales:* per cell, keyed by
tenant only.

**D14. Hedge chunk writes only where measured tails justify it, within the principal's share.**
Reservation-ahead writes to `n + Δ` volumes (Tectonic), or hedges after the `q`-quantile (TAIL),
with `Δ` and `q` from the cell's measured tail and spare capacity, charged to the principal.
*Sources:* note 01 §1.8; TAIL; `node.md` §5.4. *Confirm:* p99 of block chains with and without, and
the added load. *Laptop:* one device: nothing to hedge to, structurally absent. *Scales:* per cell.

### Step 4 — a region of cells

What appears: a tenant or bucket spread over cells; a bucket that outgrows its cell; a router
that sees a region's traffic.

**D15. A tenant's regional share is divided among cells by demand,** D13's method one level up, on
a longer interval, and principal limits that span cells use count-min sketches merged by addition
from cells to region, with `ε` set against the smallest regional limit. Hot buckets that outgrow a
cell split across cells by `architecture.md` §6.1. The router stays thin.
*Sources:* DRL §3; CM05 §1, Theorem 1; `architecture.md` §6.1. *Confirm:* regional share error and
sketch overestimate against exact counts in simulation. *Laptop:* one cell: absent. *Scales:* one
sketch per cell per window.

### Step 5 — the fleet

**D16. Regional shares by the same division, slower again; no per-request global coordination.**
Each region enforces its share; the global loop re-divides by demand with a stated staleness, which
DRL proves is the price of not putting a global limiter on the request path.
*Sources:* DRL §1, §3. *Laptop:* absent. *Scales:* one summary per region per interval.

### Summary by step

| Step | New mechanisms | Values from |
|---|---|---|
| 1 Laptop | D1 dominant-share charging; D2 device dispatcher with calibrated depth and unit; D3 hierarchical SFQ with bounded principal state; D4 classes, reservations, floors, bounded high-class share; D5 one admission authority for both listeners, native credits and typed refusals, HTTP SlowDown before body; D6 hashed-principal, retries-first shedding; D7 fair-share windows; D8 Space-Saving; D9 per-range tenant SFQ and heat splits; D10 per-request cost | calibration, Little's law, contracts, measured service times |
| 2 Node | D11a two-choice device selection; D11b per-device cost models | calibration per device |
| 3 Cell | D12 node credits and dmClock counters; D13 tenant tokens across gateways; D14 measured hedging | measured RTT, delay, demand |
| 4 Region | D15 cell shares, merged sketches, cross-cell bucket splits | measured demand, error targets |
| 5 Fleet | D16 regional shares | measured demand, stated staleness |

---

## 10. Benchmark and simulation plan

**The scenario.** One large multipart upload runs at its steady rate; at a known instant a spike
of `N` small PUTs from `P_s` principals arrives open-loop, lasts a known duration, and stops. The
large upload's size is chosen per column so that it is still running well after the spike ends
(at least the spike plus the expected recovery, both measured in a pilot run), since a
multi-terabyte object cannot be written on a laptop's disk.

**Variants.** (a) the spike to a spread of prefixes; (b) the spike to one prefix (a herd); (c)
clients with timeouts shorter than the loaded service time (a retry storm), with and without the
attempt header; (d) one principal looping; (e) a new principal ID per request (session churn) to
test that state stays bounded; (f) the large upload over a slow path (audit §13.6's rates); (g)
with repair running; (h) a Gold tenant among the small writers.

**Clients.** Every column runs two client populations in the same run: mantle's client library
on the native protocol, and stock S3 SDKs (standard retry mode) through the HTTP/1.1 listener,
with the large upload and the spike each tried from both. Per-principal shares are compared
across the two populations: a difference beyond measurement is a defect of D5.

**Baselines.** FIFO admission; per-request round robin without the hierarchy; strict SRPT by
request size; the §9 design. Each is the same binary with the scheduler policy changed.

**Metrics.**

| Metric | Definition |
|---|---|
| Small-upload latency tail | p50, p99, p99.9 of the spike's PUTs, open loop, coordinated omission corrected (audit §15.3), and as slowdown against the same PUTs run alone |
| Large-upload goodput loss | durable plaintext bytes/s during the spike against its weighted fair share and against its rate alone |
| Recovery time | from the spike's end until the large upload's goodput is back within measurement resolution of its pre-spike rate |
| Fairness | min-max ratio of per-principal dominant shares (PISCES, LIBRA report MMR); Jain's index as a second view (definition only, see "Not read") |
| Utilization | each bottleneck authority's busy fraction (device time, NIC, coding cores), the large upload's and the spike's shares of it |
| Overload behavior | refusals by kind, retries seen, completed multi-request steps per second, time to leave overload after the trigger |
| State | bytes in each §8.3 structure against its bound; allocations per request |

**Columns.**

| | Laptop | Node | Cell | Region | Fleet |
|---|---|---|---|---|---|
| How | real process, real disk, real S3 clients | real process, several devices | deterministic simulation with injected faults (`node.md` §9.1), then real processes | simulation | simulation |
| Large upload | sized to the free disk, laptop's own client | tens of GB to TB on the devices | TB, simulated time where needed | as cell, across cells | summary model |
| Spike `N`, `P_s` | swept to past measured capacity; `P_s` up to the table bounds and beyond (variant e) | same, more | swept to 10× measured capacity (AEQUITAS's spike) | spike in one cell, tenant spanning cells | one region overloaded |
| Shared device | log, engines, chunks on one device | separate and shared layouts | per node | — | — |
| Mechanisms exercised | D1–D10 | + D11 | + D12–D14 | + D15 | + D16 |

**What each run must report**: the inputs measured (calibration, service times), the derived
values used (`D_b`, `u`, `w`, `m`, targets), the hardware and persistence identity, every failure,
and the baselines' numbers beside the design's (audit §15.3). A performance claim names the run.

---

## 11. What remains unknown

1. **Agentic request rates.** No source publishes them; every scale number here is a symbol with a
   stated origin. The distribution of a principal's burst lengths, which D6's epoch needs, must be
   measured from real agent traffic.
2. **Swift's delay target** and whether a delay-based controller suits mantle's QUIC paths:
   unread (Not read), and node.md leaves the congestion controller to the network matrix.
3. **2DFQ on an async runtime.** 2DFQ partitions fixed worker threads by size; whether its
   separation carries to async tasks is not stated in the paper (note 25 §12), and note 26 decides
   how work is issued.
4. **Device cost models on mantle's own write path.** ReFlex's linear model fit three NVMe
   devices; Libra's VOP model one SSD with LevelDB; neither covers HDDs, shared laptop SSDs or
   mantle's log-structured volumes with full flushes. The model's form and error per device class
   are to be measured.
5. **The dispatch unit below a chunk.** Whether the volume can dispatch a chunk's record in
   smaller device writes without weakening its record checksum and recovery (chunk-store.md §3)
   is a design question this note raises and does not answer.
6. **Tectonic's thresholds** (counter windows, in-flight limits, Gold wait threshold) are
   unpublished (note 01 §1.11).
7. **S3's limit on in-progress multipart uploads** per bucket or account is unverified; mantle's
   bound on abandoned uploads per principal and tenant is undecided.
8. **Whether priority inversion appears in mantle's classes** at the shares tenants will buy is
   Aequitas's analysis for switch WFQ; mantle's device and gateway queues must be measured.
9. **SFQ(D)'s exact lag bound** was not transcribed (§4.3); the design relies on the measured bound
   in §10 instead.
10. **DRL's accuracy at agentic cardinality.** DRL's evaluation rate-limits "thousands of flows";
    D13 and D15 key by tenant, not principal, to stay within what was evaluated, and the cost of
    keying by principal across gateways is not established.
11. **The interaction of fair-share windows with stock S3 clients' fixed concurrency.** Behind
    the HTTP listener a transfer manager that opens a fixed number of part uploads sees only
    slowed bodies, not a smaller window or fewer credits; whether that wastes client memory or
    listener connections at scale is to be measured with real SDKs.
12. **The native protocol's admission messages** (credit grants and revocations, typed refusals,
    the admission level, the upload plan) are not yet specified; their encoding, bounds and
    authentication belong to the protocol's design, and their cost per request to D10's
    measurements.
