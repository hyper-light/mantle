# 11 — Operating-parameter models: computing mantle's constants from measurements and cited results

**Status:** research input for replacing mantle's hand-picked constants with calculated ones. This is not a decision record; decisions it supports belong in `docs/design/`.
**Compiled:** 2026-09-28.
**Scope:** every operating parameter the chunk store (`crates/chunk`), the disk layer (`crates/disk`) and the chunk benchmark (`crates/mantle/src/bench.rs`) carry today. For each one: what mantle does now, the models in the literature that compute it and their assumptions, the inputs mantle must measure and how, the calculation written out, and what remains uncertain.

---

## 0. How to read this document

**Citation tags.** `[KEY §section, p. N]` gives the printed page of the copy that was read. "PDF p. N" means the copy prints no page number and N is the page index in the PDF. Three sources use their own pagination: RO92 is the 1991 preprint of the TOCS paper, HSL+87 is Tandem Technical Report 88.1 ("TR p."), and GK85 and HEL85 use the page numbers of the bulletin issue. RFCs are cited by section. Web documentation is cited by section or parameter name. Earlier notes are cited as "note 03 §x".

**Quotes.** Quotes in "double quotes" are verbatim from the text layer of the source. Ligatures are normalized, hyphenation at line breaks is rejoined, bracketed reference numbers are kept as printed, and "..." marks an elision. **[OCR]** marks quotes from scanned pages (KLE79, NEU67, SBMS93, YOUNG74, ARIES92) that were recognized with the macOS Vision framework and checked against the page images. GK85, HEL85, DKO+84 and HSL+87 are scans with a publisher text layer; their quotes were checked against that layer.

**Evidence labels.**
- *(no label)*: stated in the cited peer-reviewed source and checked against its text.
- **NON-PEER-REVIEWED**: standards (RFCs, SNIA), product documentation (PostgreSQL, MySQL), READMEs, technical reports, bulletins with invited articles, magazine articles and preprints.
- **DERIVED**: arithmetic or a model built in this note from stated facts. The sources do not state it.
- **INFERENCE / Recommendation**: design reasoning for mantle, citing the facts it rests on.
- **UNVERIFIED**: not confirmed against a primary text.
- *abstract only*: the full text was not accessible. Only the authors' abstract was read, and nothing beyond it is claimed.

**Method.**
1. PDFs were obtained from publishers, authors' sites, USENIX and university pages. KJ13 came from the Internet Archive's copy of an institutional repository, and DKO+84 from a course site. Each was identified from its first page before it was cited.
2. Text was extracted with `pdftotext`, in both layout and reading order. Scans without a usable text layer were rendered with `pdftoppm` and recognized with Vision; formulas on scanned pages were read from the page images.
3. Abstracts of inaccessible papers came from the publisher's page (Cambridge Core) or from Crossref and Semantic Scholar metadata. No contact identifier was sent with any request.
4. Every quoted fragment of four or more words was checked by machine against the saved texts, with both sides reduced to lowercase letters and digits. Mismatches were reviewed by hand.
5. No secondary summary (blogs about papers, lecture notes, other authors' descriptions) is used as evidence. Where a paper describes another paper that was not read, the description is attributed to the paper that makes it.

**Notation used throughout.**

| Symbol | Meaning | Measured by |
|---|---|---|
| `S` | service time of one writer batch: its writes plus one flush | the writer times every batch (`service_ns`) |
| `F0` | fixed cost of one durable flush | calibration's durable 4 KiB write; the service time of batches that carry little data |
| `BW` | durable write bandwidth for large batches | calibration's durable batch; regression of `S` on batch bytes |
| `n`, `m` | requests in the batch being formed; submitters answered by the previous batch that have not yet sent again | writer counters |
| `Z` | a submitter's return delay (think time): from its answer to its next submission | arrival timestamps relative to the previous batch's answer time |
| `W` | client write rate into the volume, bytes/s | EWMA over batches |
| `μ` | request completion rate, requests/s | EWMA over batches |
| `u` | live fraction of a segment | segment usage table |
| `ρ` | recovery replay rate, bytes/s | timed at every open |
| `X(N)`, `R(N)` | throughput and mean latency with `N` requests in flight | calibration |
| `λ_s` | latent-error rate per byte of stored data per unit time | scrubber findings |

---

## Sources

The "Label" column is the evidence label that applies to every fact drawn from that source.

### Group commit, batching and waiting

| Key | Citation | Label | What was read |
|---|---|---|---|
| **DKO+84** | D. J. DeWitt, R. H. Katz, F. Olken, L. D. Shapiro, M. R. Stonebraker, D. Wood. "Implementation Techniques for Main Memory Database Systems." SIGMOD '84, pp. 1–8. doi:10.1145/602259.602261 (also SIGMOD Record 14(2), doi:10.1145/971697.602261). Same key as note 03. | peer-reviewed | scan with text layer, https://15721.courses.cs.cmu.edu/spring2016/papers/p1-dewitt.pdf |
| **GK85** | D. Gawlick, D. Kinkade. "Varieties of Concurrency Control in IMS/VS Fast Path." *IEEE Database Engineering* 8(2): 3–10, June 1985. | **NON-PEER-REVIEWED** (invited bulletin article) | scan of the whole bulletin issue |
| **HEL85** | P. Helland. "The Transaction Monitoring Facility (TMF)." *IEEE Database Engineering* 8(2): 11–18, June 1985 (same issue as GK85; HSL+87 cites it as its reference [1]). | **NON-PEER-REVIEWED** (invited bulletin article) | same scan |
| **HSL+87** | P. Helland, H. Sammer, J. Lyon, R. Carr, P. Garrett, A. Reuter. "Group Commit Timers and High-Volume Transaction Systems." Tandem Computers Technical Report 88.1, March 1988 (Part No. 12522). The workshop version is HPTS 1987, LNCS 359: 301–329, 1989, doi:10.1007/3-540-51085-0_52. | **NON-PEER-REVIEWED** (technical report). The LNCS text was not read. The TR says the HPTS proceedings printed Figures 4 and 5 swapped. | full TR, 36 pages |
| **BAI54** | N. T. J. Bailey. "On Queueing Processes with Bulk Service." *JRSS B* 16(1): 80–87, 1954. doi:10.1111/j.2517-6161.1954.tb00149.x | peer-reviewed; *abstract only* | author's summary (Crossref record) |
| **NEU67** | M. F. Neuts. "A General Class of Bulk Queues with Poisson Input." *Ann. Math. Statist.* 38(3): 759–770, 1967. doi:10.1214/aoms/1177698869 | peer-reviewed | JSTOR scan, OCR |
| **DS73** | R. K. Deb, R. F. Serfozo. "Optimal Control of Batch Service Queues." *Adv. Appl. Prob.* 5(2): 340–361, 1973. doi:10.2307/1426040 | peer-reviewed; *abstract only* | Cambridge Core abstract |
| **DEB76** | R. K. Deb. "Optimal Control of Batch Service Queues with Switching Costs." *Adv. Appl. Prob.* 8(1): 177–194, 1976. doi:10.2307/1426028 | peer-reviewed; *abstract only* | Cambridge Core abstract |
| **ZX17** | Y. Zeng, C. H. Xia. "Optimal Bulking Threshold of Batch Service Queues." *J. Appl. Prob.* 54(2): 409–423, 2017. doi:10.1017/jpr.2017.8 | peer-reviewed; *abstract only* | Cambridge Core abstract |
| **AETHER10** | R. Johnson, I. Pandis, R. Stoica, M. Athanassoulis, A. Ailamaki. "Aether: A Scalable Approach to Logging." PVLDB 3(1–2): 681–692, 2010. doi:10.14778/1920841.1920928 | peer-reviewed | PDF without printed page numbers |
| **SILOR14** | W. Zheng, S. Tu, E. Kohler, B. Liskov. "Fast Databases with Fast Durability and Recovery Through Multicore Parallelism." OSDI '14, pp. 465–477. | peer-reviewed | USENIX PDF |
| **ID01** | S. Iyer, P. Druschel. "Anticipatory Scheduling: A Disk Scheduling Framework to Overcome Deceptive Idleness in Synchronous I/O." SOSP '01, pp. 117–130. doi:10.1145/502034.502046 | peer-reviewed | ACM PDF and the authors' preprint (same text; the preprint's text layer was used where ligatures broke the ACM one) |
| **TSSD09** | K. Tsakalozos, V. Stoumpos, K. Saidis, A. Delis. "Adaptive Disk Scheduling with Workload-Dependent Anticipation Intervals." *J. Systems and Software* 82(2): 274–291, 2009. doi:10.1016/j.jss.2008.06.025 | peer-reviewed | publisher PDF |
| **SS13** | N. Santos, A. Schiper. "Optimizing Paxos with Batching and Pipelining." *Theoretical Computer Science* 496: 170–183, 2013. doi:10.1016/j.tcs.2012.10.002 | peer-reviewed | publisher PDF |
| **CLIPPER17** | D. Crankshaw, X. Wang, G. Zhou, M. J. Franklin, J. E. Gonzalez, I. Stoica. "Clipper: A Low-Latency Online Prediction Serving System." NSDI '17, pp. 613–627. | peer-reviewed | USENIX PDF |
| **PG18** | PostgreSQL 18 documentation: §19.5 "Write Ahead Log" (`commit_delay`, `commit_siblings`); §28.5 "WAL Configuration"; "CREATE SEQUENCE"; §9.17 "Sequence Manipulation Functions". Read 2026-09-28 from https://www.postgresql.org/docs/current/ | **NON-PEER-REVIEWED** | web pages |
| **MYSQL80** | MySQL 8.0 Reference Manual §19.1.6.4 "Binary Logging Options and Variables" (`binlog_group_commit_sync_delay`, `binlog_group_commit_sync_no_delay_count`). Read 2026-09-28. | **NON-PEER-REVIEWED** | web page |
| **GCS26** | M. Mandarapu, S. Kunkunuru. "Group Commit Self-Clocks: Why Tuning Is Unnecessary Above a Device-Set Load Threshold." arXiv:2606.18187v1, 16 June 2026. | **NON-PEER-REVIEWED** (preprint) | arXiv PDF, 5 pages |

### Estimation, queues and admission

| Key | Citation | Label | What was read |
|---|---|---|---|
| **JK88** | V. Jacobson, M. J. Karels. "Congestion Avoidance and Control." Revised version, November 1988. This is the version RFC 6298 cites as [JK88]. The original is V. Jacobson, SIGCOMM '88, pp. 314–329, doi:10.1145/52324.52356. | peer-reviewed paper; the text read is the revision, not compared with the SIGCOMM printing | report PDF, 25 pages |
| **RFC6298** | V. Paxson, M. Allman, J. Chu, M. Sargent. "Computing TCP's Retransmission Timer." RFC 6298, June 2011. | **NON-PEER-REVIEWED** (IETF Standards Track) | RFC text |
| **LIT61** | J. D. C. Little. "A Proof for the Queuing Formula: L = λW." *Operations Research* 9(3): 383–387, 1961. doi:10.1287/opre.9.3.383 | peer-reviewed; *abstract only* | abstract (Crossref) |
| **RFC8289** | K. Nichols, V. Jacobson, A. McGregor, J. Iyengar (eds.). "Controlled Delay Active Queue Management." RFC 8289 (Experimental), January 2018. | **NON-PEER-REVIEWED** | RFC text |
| **BREAKWATER20** | I. Cho, A. Saeed, J. Fried, S. J. Park, M. Alizadeh, A. Belay. "Overload Control for µs-Scale RPCs with Breakwater." OSDI '20, pp. 299–314. | peer-reviewed | USENIX PDF |
| **DAGOR18** | H. Zhou, M. Chen, Q. Lin, Y. Wang, X. She, S. Liu, R. Gu, B. C. Ooi, J. Zhou. "Overload Control for Scaling WeChat Microservices." SoCC '18, pp. 149–161. doi:10.1145/3267809.3267823 | peer-reviewed venue; the text read is arXiv:1806.04075v3 ("Updated on 18 December 2018") | arXiv PDF |
| **WC03** | M. Welsh, D. Culler. "Adaptive Overload Control for Busy Internet Servers." USITS '03. | peer-reviewed | USENIX PDF |
| **KLE79** | L. Kleinrock. "Power and Deterministic Rules of Thumb for Probabilistic Problems in Computer Communications." Proc. ICC '79, pp. 43.1.1–43.1.10, 1979. | peer-reviewed | scan, OCR and page images |
| **BBR16** | N. Cardwell, Y. Cheng, C. S. Gunn, S. Hassas Yeganeh, V. Jacobson. "BBR: Congestion-Based Congestion Control." *ACM Queue* 14(5), 2016 (reprinted in CACM 60(2): 58–66, 2017). | **NON-PEER-REVIEWED** (practitioner magazine) | web page |
| **KNEEDLE11** | V. Satopää, J. Albrecht, D. Irwin, B. Raghavan. "Finding a 'Kneedle' in a Haystack: Detecting Knee Points in System Behavior." ICDCS Workshops 2011, pp. 166–171. doi:10.1109/ICDCSW.2011.20 | peer-reviewed | PDF without printed page numbers |

### Layout, cleaning, checkpoints and identifiers

| Key | Citation | Label | What was read |
|---|---|---|---|
| **RO92** | M. Rosenblum, J. K. Ousterhout. "The Design and Implementation of a Log-Structured File System." *ACM TOCS* 10(1): 26–52, 1992. doi:10.1145/146941.146943. Same key as note 03. | peer-reviewed | preprint dated July 24, 1991 (page numbers are the preprint's) |
| **SBMS93** | M. Seltzer, K. Bostic, M. K. McKusick, C. Staelin. "An Implementation of a Log-Structured File System for UNIX." USENIX Winter 1993, pp. 307–326. | peer-reviewed | scan, OCR |
| **BHS95** | T. Blackwell, J. Harris, M. Seltzer. "Heuristic Cleaning Algorithms in Log-Structured File Systems." USENIX 1995 Technical Conference. | peer-reviewed | PDF without printed page numbers |
| **MRC97** | J. N. Matthews, D. Roselli, A. M. Costello, R. Y. Wang, T. E. Anderson. "Improving the Performance of Log-Structured File Systems with Adaptive Methods." SOSP '97, pp. 238–251. doi:10.1145/268998.266700 | peer-reviewed | scan with text layer |
| **F2FS15** | C. Lee, D. Sim, J.-Y. Hwang, S. Cho. "F2FS: A New File System for Flash Storage." FAST '15, pp. 273–286. | peer-reviewed | USENIX PDF |
| **DES12** | P. Desnoyers. "Analytic Modeling of SSD Write Performance." SYSTOR '12. doi:10.1145/2367589.2367603 | peer-reviewed | PDF without printed page numbers |
| **AD15**, **BAH+21**, **AWK+19**, **GGL03**, **HL23** | as in note 03 (Skylight, FAST '15; ZNS, ATC '21; Ceph, SOSP '19; GFS, SOSP '03; NVMe, PVLDB 16(9)) | peer-reviewed | publisher/USENIX PDFs; the passages cited here were re-read |
| **YOUNG74** | J. W. Young. "A First Order Approximation to the Optimum Checkpoint Interval." *CACM* 17(9): 530–531, 1974. doi:10.1145/361147.361115 | peer-reviewed | scan, OCR and page images |
| **DALY06** | J. T. Daly. "A Higher Order Estimate of the Optimum Checkpoint Interval for Restart Dumps." *FGCS* 22(3): 303–312, 2006. doi:10.1016/j.future.2004.11.016 | **UNVERIFIED** | not accessible (publisher returned 403); nothing from it is used |
| **ARIES92** | C. Mohan, D. Haderle, B. Lindsay, H. Pirahesh, P. Schwarz. "ARIES." *ACM TODS* 17(1): 94–162, 1992. doi:10.1145/128765.128770 | peer-reviewed | scan; p. 121 OCR |
| **LAH01** | T. Lahiri, A. Ganesh, R. Weiss, A. Joshi. "Fast-Start: Quick Fault Recovery in Oracle." SIGMOD '01, pp. 593–598. doi:10.1145/375663.375751 | peer-reviewed | ACM PDF |
| **RAFTX** | D. Ongaro, J. Ousterhout. "In Search of an Understandable Consensus Algorithm (Extended Version)." https://raft.github.io/raft.pdf. The ATC '14 paper (pp. 305–319) defers snapshotting to this version. | **NON-PEER-REVIEWED** (extended version) | PDF, 18 pages |
| **PERC10** | D. Peng, F. Dabek. "Large-scale Incremental Processing Using Distributed Transactions and Notifications." OSDI '10. | peer-reviewed | author/USENIX PDF (pages numbered 1–14) |

### Scrubbing and idle time

| Key | Citation | Label | What was read |
|---|---|---|---|
| **BGPS07**, **SDG10** | as in note 03 (latent sector errors, SIGMETRICS '07; FAST '10) | peer-reviewed | PDFs; SDG10 copy numbered 1–14 |
| **OJ10** | A. Oprea, A. Juels. "A Clean-Slate Look at Disk Scrubbing." FAST '10. | peer-reviewed | PDF numbered 1–14 |
| **AOS12** | G. Amvrosiadis, A. Oprea, B. Schroeder. "Practical Scrubbing: Getting to the Bad Sector at the Right Time." DSN 2012, pp. 1–12. doi:10.1109/DSN.2012.6263919 | peer-reviewed | author PDF, http://www.cs.toronto.edu/~gamvrosi/assets/scrubbing_dsn12.pdf |
| **GBS95** | R. Golding, P. Bosch, C. Staelin, T. Sullivan, J. Wilkes. "Idleness is not sloth." USENIX Winter 1995. | peer-reviewed | PDF without printed page numbers |
| **MRZ+09** | N. Mi, A. Riska, Q. Zhang, E. Smirni, E. Riedel. "Efficient Management of Idleness in Storage Systems." *ACM TOS* 5(2), Article 4, 2009. doi:10.1145/1534912.1534913 | peer-reviewed | author PDF, https://www.cs.wm.edu/~esmirni/docs/acm_TOS.pdf |

### Measurement, histograms and memory

| Key | Citation | Label | What was read |
|---|---|---|---|
| **GBE07** | A. Georges, D. Buytaert, L. Eeckhout. "Statistically Rigorous Java Performance Evaluation." OOPSLA '07, pp. 57–76. doi:10.1145/1297027.1297033 | peer-reviewed | PDF without printed page numbers |
| **KJ13** | T. Kalibera, R. Jones. "Rigorous Benchmarking in Reasonable Time." ISMM '13, pp. 63–74. doi:10.1145/2464157.2464160 | peer-reviewed | author's accepted manuscript from the Kent Academic Repository, via the Internet Archive's copy; PDF p. 1 is the repository cover |
| **SNIA-PTS** | SNIA. *Solid State Storage (SSS) Performance Test Specification (PTS)*, Version 2.0.2. | **NON-PEER-REVIEWED** (industry specification) | PDF |
| **DDS19** | C. Masson, J. E. Rim, H. K. Lee. "DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees." PVLDB 12(12): 2195–2205, 2019. doi:10.14778/3352063.3352135 | peer-reviewed | PVLDB PDF |
| **HDR** | HdrHistogram README, https://github.com/HdrHistogram/HdrHistogram (master branch), read 2026-09-28. | **NON-PEER-REVIEWED** | raw README |
| **WJNB95** | P. R. Wilson, M. S. Johnstone, M. Neely, D. Boles. "Dynamic Storage Allocation: A Survey and Critical Review." IWMM '95, LNCS 986: 1–116. doi:10.1007/3-540-60368-9_19 | peer-reviewed | the authors' revised PostScript version, which states it differs from the LNCS text "in several very minor respects"; cited by its PostScript page ("PS p.") |
| **BA01** | J. Bonwick, J. Adams. "Magazines and Vmem: Extending the Slab Allocator to Many CPUs and Arbitrary Resources." USENIX ATC 2001. | peer-reviewed | USENIX PDF without printed page numbers |
| **KNUTH** | D. E. Knuth. *The Art of Computer Programming*, Vol. 1, §2.5 (buddy systems). | **UNVERIFIED** | not read; nothing is attributed to it |

---

## 1. Findings that change decisions

1. **The writer's wait rule is correct algebra under assumptions that fail in practice (§2.3).** `dw < p·S/(n+p)` is the break-even of `n·dw` of added latency against `p·(S − dw)` saved for one newcomer. But `p` is treated as a constant although it is the probability of a return within `dw`. It is learned from the writer's own waits. The rule counts one newcomer when `m` may be outstanding, and it ignores the delay the wait imposes on the next batch. The result is concrete: a submitter whose think time exceeds the first step, at most `S/2`, is never waited for, even when waiting would lower total latency (a worked case is in §2.3). The closest peer-reviewed model is anticipatory disk scheduling [ID01 §3.3]. It waits for the process whose request just completed, using learned think-time statistics. **Recommendation (§2.6):** measure the return-delay distribution, choose the wait that maximizes the expected saving `Δ(t)`, and keep the total wait below `S`.
2. **The first step of mantle's rule equals PostgreSQL's recommended `commit_delay`.** With `n = 1` and `p = 1` the bound is `S/2`. PostgreSQL recommends half the flush time of a single 8 kB write as the starting value [PG18 §28.5, NON-PEER-REVIEWED]. PostgreSQL gives no model for this value; the agreement is DERIVED.
3. **Helland et al. can now be cited from the text (Tandem TR 88.1; §2.2 of this note).** In their model, timers pay only when each commit write consumes a shared, queued resource (CPU). The zero-timer group size under Poisson arrivals is computed. The optimal timer is computed at run time from measured quantities, and forced to zero when it would be shorter than a log write. Their conclusion is that "the system should dynamically calculate an appropriate timer value based on the system load" [HSL+87 §12, TR p. 19, NON-PEER-REVIEWED]. This replaces note 03's UNVERIFIED entry, with the caveat that the text read is the TR.
4. **DeWitt et al. do not define the batch the way `writer.rs` cites them for.** The module takes "every request that arrived while the previous batch was being made durable" and cites DeWitt et al. §5.2. Their commit group is the transactions whose commit records share a log page, sized by how many fit in one log write [DKO+84 §5.2, p. 7]. Batching whatever queued during the previous log write is HSL+87's zero-timer rule [HSL+87 §2, TR p. 2, NON-PEER-REVIEWED], and PostgreSQL describes the same behaviour for `commit_delay = 0` [PG18 §28.5, NON-PEER-REVIEWED]. `writer.rs` and design §4 should cite those for it.
5. **For open (Poisson) arrivals the optimal dispatch rule is a threshold on the number waiting, not a timer** [DS73; DEB76; ZX17, abstracts only]. For closed-loop submitters the deciding quantity is the think-time distribution (item 1).
6. **The 1/8 gain is right only for a particular noise level (§3).** Jacobson ties the gain to signal-to-noise and gives the time constant `1/g` [JK88 App. A.1, p. 18]. His statement that the estimate's standard deviation is `g·sdev(m)` understates the steady-state spread. For independent samples it is `√(g/(2−g))·sd(m)`, which is `0.26·sd(m)` at `g = 1/8` (DERIVED). Given a measured coefficient of variation `CV` and a target relative error `ε`, the gain is `g = 2ε²/(CV² + ε²)`. The value 1/8 corresponds to `CV ≈ 0.2` at `ε = 5%`.
7. **The writer queue is bounded in requests, not bytes or time, and a full queue blocks (§4).** 4,096 queued requests are 1.1 s of work at 3.71K puts/s, and 32 GiB of payload at 8 MiB each. Oversized payloads are copied and queued before the writer refuses them. Design §4 says a full queue refuses with `Busy`, but `Volume::submit` uses a blocking `SyncSender::send` and no `Busy` error exists. Little's law gives `Q = μ·d` requests and `BW·d` bytes for a queueing-delay budget `d`. CoDel's sojourn-time control is the adaptive form.
8. **256 MiB segments are supported for host-managed SMR by AWK+19, not by AD15 or BAH+21 (§7).** AD15 measured drive-managed SMR bands of 15–40 MiB. BAH+21's one ZNS device has 2,048 MiB zones with 1,077 MiB of writable capacity and 14 active zones. On zoned devices the segment must be the device's zone capacity. On conventional devices MRC97's trade-off between transfer efficiency and cleaning efficiency is the model.
9. **GFS's 64 KB checksum block is verified as GFS's choice, but GFS gives no model for it (§8).** For random, aligned reads of `r` bytes, read amplification is `1 + (k − a)/r` (DERIVED): 16× for 4 KiB reads at 64 KiB blocks. The block should be chosen per chunk class from its read-size distribution. The record format already allows a per-record block size.
10. **The cleaner's watermarks have no basis in RO92, which says it did not study its thresholds methodically (§10).** A fraction of capacity scales with the disk, not the write rate. On a 20 TB volume `segments/16` keeps about 1.25 TB free before cleaning starts; on the 4 GiB benchmark volume it is two segments. The rule that keeps writes from being refused starts cleaning when the free-space runway falls below the time to reclaim the first victim plus the writes that arrive between cleaning opportunities, which BHS95 measured on its traces. When the cleaning rate `G = (1 − u)·S_seg/t_seg` falls below the write rate, the rule throttles client writes.
11. **`FUTILE_AFTER = 4` equals `⌈1/(1 − u)⌉` for victims at `u = 0.75`,** equivalently a cleaning write-amplification budget of 3 (DERIVED, §10). It should be computed from the victims' live fraction, or from a stated cleaning budget.
12. **The checkpoint trigger degenerates as the index fills, and it has no time bound (§9).** The writer checkpoints when the log since the start of the last checkpoint, the checkpoint included, exceeds a third of the log. With a full 2^22-fragment index the checkpoint is about 320 MB, so the writer rewrites it after every ~2.8 MB of new log: about 99% of log bandwidth goes to checkpoints (DERIVED from `layout.rs` and `writer.rs`). Design §5's rule, checkpoint once the log since the last checkpoint exceeds the checkpoint's own size, caps that share at one half and fits the same log. Raft sets the trigger so that snapshot bandwidth stays small [RAFTX §7, PDF p. 13]. For the time bound: Oracle's Fast-Start sets checkpointing from a roll-forward-time target and the I/O rate [LAH01 §3.1, p. 595], and SiloR measured recovery time proportional to the data read [SILOR14 §6.4, p. 474]. The trigger should be the earliest of the space bound and `ρ·(T_budget − T_load)`, subject to a bandwidth floor. Young's `√(2·T_s·T_f)` adapts once the lost work is replaced by replay time.
13. **Identifier reservations cost one flush every 75 minutes at today's rates (§11).** The size rule is `R ≥ r_max·c/ε`. Raising the reservations at every checkpoint's superblock write removes the extra flush entirely.
14. **The 7-day scrub period is practice, not derivation (§12).** BGPS07's systems scrubbed "at least once every two weeks" and found over 60% of latent errors that way [BGPS07 §6.2, PDF p. 11]. SDG10 calls one or two weeks common. Ceph scrubs data weekly [AWK+19 §5.1]. OJ10's MLET is the objective, and a first-order model gives `T ≤ 2·p*/λ_chunk` (DERIVED), with `λ` learned from the scrubber's own findings. Two code gaps: the scrubber visits segments in order, not in the staggered order design §9 specifies, which SDG10 found shortens detection time by 10–20% at 7–14-day periods. And `MAX_DAMAGED`'s comment says the oldest entries are dropped, while the code drops new ones.
15. **Calibration statistics are weaker than they look (§13).** The minimum and maximum of three rounds bracket the population median only 75% of the time. Sixty-four durable writes yield a "p99" that is the sample maximum, which exceeds the true p99 with probability 0.47 (DERIVED). The 90%-of-best knee is a heuristic. Kleinrock's power gives `N* ≈ X_max·R_min`, about 16 for this machine's 4 KiB reads (15K/s at depth 1, about 235K/s at depth 64).
16. **Histogram accuracy follows from the estimator (§14).** Reporting bucket upper bounds gives a one-sided error below `2^−SUB_BITS`, 12.5% at 3 bits. Reporting the bucket's harmonic midpoint `2ab/(a+b)`, as DDSketch does, cuts the bound to `1/17 ≈ 5.9%` with the same memory (DERIVED).
17. **Benchmark repetitions follow from measured variance (§16).** They need `n ≥ (t·CV/ε)²`. With `CV ≈ 0.3`, a ±10% interval at 95% confidence needs about 37 independent repetitions per point. KJ13's dimensioning experiment allocates repetitions across levels. SNIA's steady-state test and GBE07's CoV window detect warm-up.

---

## 2. Group commit: when the writer dispatches a batch (`writer.rs`)

### 2.1 What mantle does now

A batch is the first request that arrives plus everything already queued, drained with `try_recv`. Draining stops at 1,024 requests or once the payload reaches 32 MiB. The byte test runs before each addition, so one request may carry a batch past 32 MiB. If the previous batch answered `answered` submitters, `gather` then waits for them to return. It waits one step at a time, each step bounded by `dw = p·S/(n + p)`, and stops when every answered submitter has returned, a step times out, or the batch limits are reached. `S` is a moving average, with gain 1/8, of the measured time to write and flush a batch. `p` is a moving average, with gain 1/8, of each batch's fraction of answered submitters that returned during the wait. It starts at 1/2. The module documentation derives the bound from "`n·dw` of latency" against saving "the newcomer about `S − dw`", and states that the model "assumes a batch's service time does not grow with one more request".

Measured effect (docs/measurements/2026-09-28-chunk-store-benchmark.md, finding 5): with 4 writers of 4 KiB chunks and no wait, batches alternated between 1 and 3 requests and each put took two flushes (472 puts/s). With the wait, 4 KiB puts ran at 238/s with 1 writer, 885/s with 4 and 3.71K/s with 16.

### 2.2 What the literature says

**Group commit's origin and the batch it defines.**
- DeWitt et al. define the group by co-residence on a log page: "The transactions with commit records on the same log page are committed as a group, and are called the commit group. A single log I/O is incurred to commit all transactions within the group. The size of a commit group depends on how many transactions can fit their logs within a unit of log write (i.e., a log buffer page)." They also state the durability rule: "The transaction is delayed from committing until its commit record actually appears on disk." [DKO+84 §5.2, p. 7]. They do not say when a partly filled page is written.
- IMS Fast Path, as described by its implementers, forces the journal by size or time: "The system journal records will eventually be forced out by a buffer ... becoming full, a time-limit expiring, or standard IMS/VS processing." (The sentence runs across the page break; the elision is the page footer.) Then: "When a system journal buffer is physically written out, it is likely to contain commit records for more than one transaction." [GK85 §6, pp. 9–10, NON-PEER-REVIEWED].
- Helland's description of Tandem TMF gives the arithmetic that makes per-processor buffers fail. "Using 16 buffers with 100 transactions per second would mean an average arrival rate of 6.25 commit records per second into each buffer. To wait for two or three records in the buffer would impose an unacceptable delay in response time. This means that the buffering (box cars) is ineffective." The fix: "By directing all commit records to one buffer, it becomes reasonable to write an average of 10 records every .1 second." [HEL85, p. 15, NON-PEER-REVIEWED]. *DERIVED:* a group forms only when (arrival rate into the buffer) × (wait) is several requests. At 6.25/s, gathering 2–3 records takes 0.3–0.5 s.

**Timers: Helland, Sammer, Lyon, Carr, Garrett and Reuter** (Tandem TR 88.1, NON-PEER-REVIEWED).
- Definition: "Group Commit refers to the technique used in high volume transaction systems where many transactions are committed with a single disk I/O to the log." [HSL+87 §2, TR p. 1]
- The zero-timer rule, which is mantle's no-wait rule: "If a transaction needs to write a commit record and there is no log I/O outstanding, then the commit record is immediately written to the log." When a log I/O completes, everything that queued meanwhile is written in one I/O [HSL+87 §2, TR p. 2]. "When the Group Commit Timer is zero, the bus driver will leave when the first passenger arrives." [HSL+87 §2, TR p. 2]
- The model's scope: "Fundamentally, we are assuming that Group Commit Timers do not affect the I/O component of the response time." [HSL+87 §3, TR p. 2] Transactions cost `A` seconds of CPU plus a share of `B`, the CPU cost of one commit write. CPU response time is taken from M/M/1, arrivals are Poisson at rate `T`, and a log write takes `L` [HSL+87 §§5–6, TR pp. 4–8].
- Why timers can help: "If the savings in CPU queuing exceeds the average wait time for the Group Commit Timer, then Timers improve the transaction's response time." [HSL+87 §4, TR p. 3]
- Zero-timer group size: `C₀ = LT + e^(−LT)`. "When the timer is zero, the number of transactions in a Group Commit buffer is a function of the transaction rate and the time a write to the log takes." [HSL+87 §5.1, TR p. 6]. With timer `D`, the group is `C_d = TD/(1 − e^(−TD))` [HSL+87 §6.1, TR p. 9, formula read from the page].
- The optimal `D` minimizes `R_d = (A + B)/(1 − U_d) + D/2`. It is found by Newton–Raphson from the starting guess `D₀ = (B + √(2B(A + B)))/(1 − TA)` [HSL+87 §§7.1–7.2, TR pp. 10–11]. The inputs are measured: "B is a constant (for a given release of the operating system). T is measurable from the system." `A` is computed from measured utilization and write frequency [HSL+87 §8, TR p. 12].
- Guard: when the computed optimum is shorter than a log write, they "restrict the system to behave as if the timer value was zero when the calculated optimal timer value was smaller than L" [HSL+87 §11.2, TR p. 18].
- Result: a 32-processor DebitCredit benchmark with a 2-second response-time requirement ran at 165 transactions per second with zero timers. It reached 208 once the group commit timer was set by "back-of-the-envelope" calculation and an audit flush timer was added [HSL+87 §10, TR p. 16].
- Conclusion: "The arbitrary selection of a specific value for the timer could harm some transaction mixes. ... To expect a system manager to wisely select an appropriate timer value is also untenable. For these reasons, it seems clear that the system should dynamically calculate an appropriate timer value based on the system load." [HSL+87 §12, TR p. 19]

**Queueing theory of batch service.**
- Bailey studied "a simple queueing process in which customers arrive at random, form a single queue in order of arrival, and are served in batches, the size of each batch having a fixed maximum" [BAI54, abstract only].
- Neuts's general bulk-service rule: below `L` waiting the server waits for `L`; "If there are L or more, but less than K(K ≥ L) customers waiting, all are served together." "The service times of successive groups are assumed to be conditionally independent given the bulk sizes, but may depend on their magnitude." [NEU67 §1, p. 759, OCR]. "For L = 1, we obtain the case in which the server is operating as soon as one customer is present." [NEU67 §1, p. 761, OCR]. He notes a timeout variant: "We may want to serve a group of less than L customers if its waiting time exceeds a given value." [NEU67 §1, p. 760, OCR]. *DERIVED:* mantle's writer is Neuts's rule with `L = 1` and `K` = the batch limits, plus the anticipation wait.
- Optimal control, from an MDP with serving and holding costs, both discounted and average cost: optimal policies "are of the form: at a review point when x customers are waiting, serve min { x, Q } customers ( Q being the, possibly infinite, service capacity) if and only if x exceeds a certain optimal level M" [DS73, abstract only]. With switching costs the optimum becomes two thresholds: "leave the server off until the number of customers x reaches an optimal level M , then turn the server on and serve min ( x, Q ) customers", continuing until the queue falls below `m ≤ M` [DEB76, abstract only]. For M/G[a,b]/1/N, "We then establish a necessary and sufficient condition on the optimal bulking threshold that minimizes the expected waiting time." [ZX17, abstract only].

**Engines and practice.**
- Aether: "A daemon thread triggers log flushes using policies similar to those used in group commit (e.g. “flush every X transactions, L bytes logged, or T time elapsed, whichever comes first”)." [AETHER10 §4.1, PDF p. 4]. With one I/O thread, "the group commit policy ensures that requests become larger rather than more frequent" [AETHER10 §4.2, PDF p. 5]. Aether's flush pipelining lets threads "detach from transactions during log flush in order to execute other work, resuming the transaction once the flush is completed" [AETHER10 §4.1, PDF p. 4].
- SiloR fixes the interval: "A designated thread advances it periodically (every 40 ms)." "Epochs allow for a form of group commit: SiloR persists and recovers in units of epochs." [SILOR14 §2, p. 466]. The cost: "Since the epoch advances every 40 ms, average latency cannot be less than 20 ms." [SILOR14 §6.2, p. 472].
- PostgreSQL (NON-PEER-REVIEWED) waits only when others are likely to commit: "Because the delay is just wasted if no other transactions become ready to commit, a delay is only performed if at least commit_siblings other transactions are active when a flush is about to be initiated." [PG18 §19.5]. Its starting value: "A value of half of the average time the program reports it takes to flush after a single 8kB write operation is often the most effective setting for commit_delay, so this value is recommended as the starting point to use when optimizing for a particular workload." With zero delay, "each group will consist only of sessions that reach the point where they need to flush their commit records during the window in which the previous flush operation (if any) is occurring." And: "Setting commit_delay can only help when (1) there are some concurrently committing transactions, and (2) throughput is limited to some degree by commit rate" [PG18 §28.5].
- MySQL (NON-PEER-REVIEWED): `binlog_group_commit_sync_delay` "Controls how many microseconds the binary log commit waits before synchronizing the binary log file to disk", with default 0. `binlog_group_commit_sync_no_delay_count` is "The maximum number of transactions to wait for before aborting the current delay". The manual's advice: "Typically, the benefits of setting a delay outweigh the drawbacks, but tuning should always be carried out to determine the optimal setting." [MYSQL80 §19.1.6.4]

**Waiting for the submitter that was just served (anticipation).** Anticipatory scheduling addresses the pattern of finding 5: a synchronous submitter sends its next request right after its previous one completes. The scheduler "sometimes introduces a short, controlled delay period, during which the disk scheduler waits for additional requests to arrive from the process that issued the last serviced request" [ID01 §1, PDF p. 1]. Think time is "the interval between completion of the previous request issued by the process and issue of a new request" [ID01 §3.3 footnote, PDF p. 5]. The decision weighs the expected gain against `cost = max(0, expected median thinktime − elapsed)`. "The waiting period is chosen as the expected 95-percentile thinktime, within which there is a 95% probability that a request will arrive." "Expected median and 95%ile thinktimes are estimated by maintaining a decayed frequency table of request thinktimes for each process." [ID01 §3.3, PDF p. 5]. A later design "determines the length of every anticipation period in an on-line fashion in order to reduce penalties" per process [TSSD09, abstract, p. 274].

**A preprint on closed loops** (GCS26, NON-PEER-REVIEWED). It argues: "Real commit arrivals are closed-loop: a client issues its next transaction only after its last commits, so the arrival rate is induced by the policy’s own latency." [GCS26 abstract, p. 1]. It claims "the parameter-free greedy-pipelined policy (flush the instant the device is free) self-clocks to a computable fixed point", within about 0.1% of the best timer, and it quotes the open-loop "EOQ square-root rule" timer `√(2F0/λ)` [GCS26 abstract and §2, pp. 1–2]. Its self-clocking rests on pipelining: "Flush pipelining [Johnson et al., 2010] is the mechanism that makes self-clocking possible" [GCS26 §1, p. 2], and "The closed-loop competitive bound is conjectured, not proven" [GCS26 §5, p. 4]. *INFERENCE:* mantle's writer is not pipelined. It writes and flushes a batch before forming the next one, and finding 5 measured greedy dispatch at half the throughput of waiting for 4 closed-loop writers. The preprint's claim does not carry over to mantle's current writer.

### 2.3 Checking mantle's derivation (DERIVED)

**The algebra holds.** Waiting `dw` costs the `n` batched requests `n·dw`. A newcomer that arrives within the wait, with probability `p`, saves at least `S − dw`, because otherwise it would wait for this batch and then its own. Waiting lowers the expected total while `n·dw < p·(S − dw)`, which is `dw < p·S/(n + p)`.

**The measurements fit the underlying closed-loop picture.** With one writer, 238 puts/s means `S ≈ 1/238 s = 4.20 ms`. With `N` closed-loop writers and no wait, the submitters split into two groups that alternate, so each put waits about two service times: `X ≈ N/(2S)` gives 476/s for `N = 4`, against 472/s measured. With all writers in every batch, `X ≈ N/(S + W)`, where `W` is the wait per batch. The measured 885/s implies `S + W ≈ 4.52 ms`, so `W ≈ 0.3 ms` at most. Waiting beats alternation exactly when `W < S`. **Any wait rule for closed-loop submitters should keep the total wait per batch below `S`.**

**Where the rule departs from a correct model.**
1. *`p` depends on `dw`.* The probability that a submitter returns within `dw` is `G(dw) = P(Z ≤ e + dw | Z > e)`, where `e` is the time already elapsed since the answer. The code uses one learned constant.
2. *The learning is circular.* `p` is estimated from returns during the writer's own waits. If think times exceed the first step, no one returns during it, `p` falls, and the steps shrink further. Worked case: `S = 4.7 ms`, three answered submitters that return 3 ms after their answers, one other request already in the batch. Even at `p = 1` the first step is `S/2 = 2.35 ms`, so the writer never waits for them. Yet waiting 3 ms lowers the total latency of the four requests from 23.9 ms to 21.8 ms: `3 × ((4.7 − 3) + 4.7) + 4.7` against `(3 + 4.7) + 3 × 4.7`.
3. *One newcomer at a time.* With `m` outstanding submitters, the benefit of waiting scales with the number expected to arrive, not with one.
4. *The next batch is ignored.* Dispatching later also delays requests that arrive after the dispatch but before the batch would have finished.
5. *Sample weighting.* `p` is averaged per batch as `returned/answered`. A batch with one answered submitter contributes a 0-or-1 sample with the same weight as one with 64. The ratio of running sums, `Σreturned / Σanswered`, weights each submitter equally.

**The expected saving of waiting until `t`, derived directly.** Measure `t` from the previous batch's answers, with elapsed time `e` now. Let `K(t)` be the number of answered submitters that arrive in `(e, t]`. Let `J(t)` be the requests that arrive in `(t, t + S)`, answered or not. Waiting delays each of them by at most `t − e`, because the batch they join, or wait behind, starts that much later. Each returner included by waiting saves `S − (t − e)`, whatever its arrival time. Each of the `n` batched requests loses `t − e`. So, conservatively,

```
Δ(t) = E[K(t)]·(S − (t − e)) − (n + E[J(t)])·(t − e),       e ≤ t < e + S
E[K(t)] = m·G(t),   G(t) = P(Z ≤ t | Z > e)
```

and the best wait is `t* = argmax Δ(t)`, dispatching at once when `max Δ ≤ 0`. Near `t = e`, and ignoring `J`, the marginal form is: keep waiting while `(m − k)·h(t)·(S − (t − e)) > n + k`, where `h` is the hazard rate of `Z` and `k` returners have arrived. When all think times are much shorter than `S`, `G` jumps to 1 almost at once and the rule reduces to waiting for everyone, which takes almost no time. That is why mantle's rule worked in finding 5. When think times are memoryless with mean `z̄`, the hazard is the constant `1/z̄`, and early in the wait waiting pays while `(m − k)·S/z̄ > n + k`. When they are heavy-tailed, the hazard falls and the rule stops early.

### 2.4 When each rule is optimal

| Rule | Optimal when | Source |
|---|---|---|
| No wait: batch = everything queued (HSL+87's zero timer; PostgreSQL `commit_delay = 0`) | no submitter is likely to arrive within `S` (`G(e + S)` small), or the writer is pipelined, so a latecomer's alternative is the next flush rather than a full `S` after this one | HSL+87 TR p. 18 (zero when optimum < L); AETHER10 §4.1; GCS26 (NON-PEER-REVIEWED, pipelined writers only) |
| Fixed timer `D` | open Poisson arrivals, each batch's overhead consumes a queued resource (HSL+87: CPU per commit write), and the I/O time is unaffected by the timer; `D` computed from measured `A`, `B`, `T` | HSL+87 §§5–8 |
| Threshold on the number waiting (control limit `M`) | Poisson arrivals, costs for serving and holding, discounted or average cost; the optimal policy class for the stated MDP | DS73, DEB76, ZX17 (abstracts only) |
| Size-or-time trigger ("flush every X transactions, L bytes logged, or T time elapsed") | practice; no optimality claim | GK85; AETHER10 |
| Fixed epoch (40 ms) | practice; buys group commit and epoch-granular recovery at a mean latency of at least 20 ms | SILOR14 |
| Anticipation (wait for the just-served submitter) | closed-loop submitters with short, predictable think times relative to the time saved; a cost-benefit heuristic, not a proven optimum | ID01; TSSD09 |
| Mantle's `dw < p·S/(n + p)` | think times much shorter than `S` (so `p → 1`), one newcomer at a time, `S` independent of batch size | §2.3 (DERIVED) |

### 2.5 Inputs to measure online

- `S`: already measured per batch. Also keep its deviation (§3) and its dependence on bytes and requests (§5).
- `Z`, the return-delay distribution. After each batch is answered, timestamp the arrivals that follow. The first `answered` arrivals after the backlog are the returners, which is the identification the code already uses. Record their delays in a log-linear histogram (`histogram.rs`) with decay (ID01 keeps a decayed frequency table). Delays of returners that arrive after the next dispatch must also be recorded, or the estimate is censored by the policy that is being tuned.
- `n`, `m`, and the arrival rate of other requests (for `J(t)`).
- Whether the writer is pipelined, and at what depth (§5): pipelining shrinks the saving `S − (t − e)`.

### 2.6 The calculation (Recommendation)

```
at dispatch time, with n requests in the batch and m answered submitters outstanding:
  if m = 0: dispatch
  G(t)   = empirical conditional CDF of Z given Z > e       (from the histogram)
  K(t)   = m·G(t)
  J(t)   = m·(G(t+S) − G(t)) + λ_other·S                  (arrivals in (t, t+S); a conservative count)
  Δ(t)   = K(t)·(S − (t − e)) − (n + J(t))·(t − e)
  t*     = argmax over t in (e, e + S) of Δ(t)   (evaluate at the histogram's bucket bounds)
  wait until t* or until all m return, whichever comes first; dispatch at once if Δ(t*) ≤ 0
```

The ID01-style simplification is to wait until `G` reaches its 95th percentile when `Δ` at that point is positive. Both versions need nothing from the developer's machine. Every input is measured by the writer itself.

### 2.7 What remains uncertain

- The writer cannot identify submitters, because each request carries its own reply channel. Treating the first `answered` arrivals as the returners is an approximation. It fails when other clients' requests interleave, and a submitter identifier on requests would remove it.
- Think times of real clients (S3 front ends, replication peers) are unknown until measured. The model assumes they are stable over the histogram's decay window.
- For large batches `S` grows with batch bytes. `Δ` then needs `S(n + 1)` rather than `S`, which is §5's regression.
- A pipelined writer (finding 9: two flushes in parallel reached 5.99 GB/s against 5.12 GB/s for one) changes the whole question: the saving shrinks, and the pipeline depth becomes the parameter (§5).

---

## 3. Moving averages: the gain of 1/8 (`writer.rs`)

### 3.1 Now

`smooth(a, x) = a − a/8 + x/8` in integer arithmetic. It averages the batch service time `S`, in nanoseconds, and the return fraction `p`, in units of 1/65,536. The module cites Jacobson and RFC 6298 §2. `S` starts at its first sample and `p` at 1/2.

### 3.2 Models

- Jacobson: the update `a ← a + g(m − a)` is a stochastic-gradient estimator. `g` "should be related to the signal-to-noise ratio (or, equivalently, variance) of m". Further: "it’s almost always better to use a gain that’s too small rather than one that’s too large. Typical gain choices are 0.1–0.2 (though it’s a good idea to take long look at your raw data before picking a gain)". The estimate "converges to the true average exponentially with time constant 1/g" [JK88 App. A.1, p. 18]. For variation he estimates the mean deviation with a second, larger gain: "it’s a good idea to give v a larger gain" [JK88 App. A.2, p. 19], and "Using a gain of .25 on the deviation and computing the retransmit timer, rto, as a + 4v" [JK88 App. A.2, p. 20].
- RFC 6298 (NON-PEER-REVIEWED) standardizes the pair: `RTTVAR ← (1 − β)·RTTVAR + β·|SRTT − R'|`, `SRTT ← (1 − α)·SRTT + α·R'`, and "The above SHOULD be computed using alpha=1/8 and beta=1/4 (as suggested in [JK88])." [RFC6298 §2 (2.3)]

### 3.3 What the gain does (DERIVED)

- **Time constant.** After a step change, the estimate closes `1 − (1 − g)^k` of the gap in `k` samples: 66% in 8 samples and 95% in 23 at `g = 1/8`. At `S ≈ 4.2 ms` per batch, 8 samples is about 34 ms.
- **Noise.** For independent samples of standard deviation `σ`, the stationary variance of the EWMA is `g/(2 − g)·σ²`, so its standard deviation is `√(1/15)·σ = 0.258·σ` at `g = 1/8`. Jacobson writes that it "will be g sdev(m)" [JK88 p. 18], which is `0.125·σ`, about half the true spread. That sentence describes the size of each step, not the stationary spread.
- **Choosing `g` from a target.** To keep the estimate within relative error `ε`, one standard deviation, when the samples have coefficient of variation `CV`: `√(g/(2 − g))·CV ≤ ε`, so `g ≤ 2ε²/(CV² + ε²)`. For `CV = 0.2` and `ε = 0.05` this gives `g ≤ 0.118`, which is about 1/8. To follow changes within `τ` seconds at `r` samples per second: `g ≥ 1/(r·τ)`. When the two bounds conflict, the measurement is too noisy to track changes that fast, and the conflict itself should be reported.
- **Bernoulli samples.** `p`'s per-batch sample is a fraction with variance `p(1 − p)/answered`. The ratio of two EWMAs, of returned and of answered counts, weights submitters rather than batches (§2.3, item 5).
- **Rounding.** Integer truncation biases the fixed point of `a − a/8 + x/8` by at most 7 units. That is 7 ns for `S`, and 7/65,536 ≈ 0.01% for `p`: negligible here. Jacobson keeps the scaled sum `8a` to avoid it [JK88 App. A.2, p. 19].

### 3.4 Inputs and calculation

- Inputs: per-batch samples of `S` and of the return counts; the batch rate `r`; a running deviation (`RTTVAR`-style, β = 1/4 per RFC 6298) giving `CV ≈ dev/mean`, using Jacobson's approximation that mean deviation is close to standard deviation.
- Calculation: `g = clamp(2ε²/(CV² + ε²), 1/(r·τ), 1/2)`, rounded to a power of two if shifts are wanted. `ε` and `τ` come from what consumes the estimate: the wait rule (§2) needs `S` within a few percent, and the queue bound (§4) needs `S + 4·dev` as a tail estimate.

### 3.5 Uncertain

`S` is not stationary: it depends on batch size and on device state, and finding 2 shows millisecond stalls. An EWMA of the mean says nothing about tails. Where a quantile is needed, the histogram (§14) is the right tool, not the EWMA.

---

## 4. Writer queue bound (`Limits::queue = 4096`)

### 4.1 Now

`sync_channel(4096)`. A submitter blocks in `send` when the queue is full, and blocks in `recv` until answered. Both waits are unbounded in time. Each queued request holds a copy of its payload. Payloads larger than a record can hold (about one segment, 256 MiB) are refused only after the writer dequeues them (`validate` returns `TooLarge`), so nothing bounds a queued payload's size except the caller. Design §4 says "a full queue refuses with `Busy`", but no such error exists in `crates/chunk`, so the design text and the code disagree.

### 4.2 Models

- Little: "It is shown that, if the three means are finite and the corresponding stochastic processes strictly stationary, and, if the arrival process is metrically transitive with nonzero mean, then L = λW." [LIT61, abstract only]
- CoDel (NON-PEER-REVIEWED): "It is not the queue length that should be controlled but the amount of excess delay packets experience due to a persistent or standing queue, which means that the packet sojourn time in the buffer is exactly what we want to track." [RFC8289 §3.1]. "Instead of averages, we recommend tracking the minimum sojourn time; then, if there is one packet that has a zero sojourn time, there is no persistent queue." [RFC8289 §3.1]. The setpoint comes from Kleinrock's power: "the ideal range for the permitted standing queue, or the target setpoint, is between 5% and 10% of the TCP connection's RTT" [RFC8289 §3.2], and "The calculations of Section 3.2 show that the best TARGET value is 5-10% of the RTT, with the low end of 5% preferred." [RFC8289 §4.3]
- Breakwater: "A more reliable signal is queuing delay, as it is accurate even under RPC service time variability. Furthermore, it is intuitive to map a target SLO to a target queueing delay at the server." [BREAKWATER20 §3.1, p. 303]. The target delay is one "which is set based on the SLO of the RPC" [BREAKWATER20 §3.2.1, p. 304]. In their evaluation they set "dt to 40% of SLO" [BREAKWATER20 §5.1, p. 306], a tuned value rather than a derived one.
- DAGOR: "given the default timeout of each service task being 500 ms in WeChat, the threshold of the average request queuing time to indicate server overload is set to 20 ms" [DAGOR18 §4.1, PDF p. 5]. The paper calls this an empirical configuration.
- SEDA's controller: "an adaptive admission control mechanism that attempts to bound the 90th-percentile response time of requests flowing through the service" [WC03, abstract, PDF p. 2].

### 4.3 Inputs

- `μ`, completed requests per second, and `BW`, bytes per second, both measured by the writer.
- Each request's sojourn: time from `submit` to the dispatch of the batch that carries it. The submitter can stamp the request at `send`.
- `D`, the deadline of whoever submits: the S3 request timeout, or the replication protocol's timeout. These are policy inputs from other layers and are not yet defined.

### 4.4 Calculation (DERIVED)

A request's queueing delay is at least the residual of the batch in progress, anywhere in `[0, S)`, so the budget must satisfy `d ≥ S`. By Little's law, holding the queue's mean delay to `d` holds its mean length to `μ·d` requests and `BW·d` bytes:

```
d        = f·D                  f: the share of the caller's deadline D given to this queue
require    d ≥ S + 4·dev(S)     otherwise the deadline cannot be met on this device: report it
Q_req    = ⌈μ·d⌉
Q_bytes  = BW·d
```

Admission refuses with a typed `Busy`, as the design intended, when either bound is exceeded, or, CoDel-style, when the minimum sojourn over the last interval exceeds `d`. The literature fixes `f` empirically: 5–10% for CoDel's target relative to its interval, 4% in DAGOR, 40% of the SLO in Breakwater. `f` is a policy choice that must be stated with the deadline it divides.

Worked example with this machine's rates and an illustrative `d = 50 ms`: `μ = 3.71K/s` for 4 KiB puts gives 186 requests. For 8 MiB puts at 3.0 GB/s, `BW·d = 150 MB`, about 18 requests. Today's 4,096 requests correspond to 1.1 s of queued 4 KiB work, or 32 GiB of queued 8 MiB payloads.

### 4.5 Uncertain

The caller deadlines `D` do not exist yet. Until they do, `d` has only its lower bound `S`, which is measured. Sojourn-based admission needs submit timestamps. A byte bound also needs the largest single request to fit within it, or such requests need their own path.

---

## 5. Batch limits (`batch_requests = 1024`, `batch_bytes = 32 MiB`) and writer pipelining

### 5.1 Now

The comment on both limits says "Each bounds memory, not throughput" (`layout.rs`). The calibration's durable batch is 32 MiB because it is "The chunk store's largest group commit" (`calibrate.rs`), so the calibration inherits the limit instead of informing it. `batch_requests` also sizes the log headroom through `batch_frame_bytes`.

### 5.2 Models

- **Fixed cost amortized over a batch.** Clipper states it directly: "the gain in efficiency is a result of the ratio of the fixed cost for sending a batch to the variable cost of increasing the size of a batch" [CLIPPER17 §4.3.2, p. 619]. Its batch-size rule: "We define the optimal batch size as the batch size that maximizes throughput subject to the constraint that the batch evaluation latency is under the target SLO." [CLIPPER17 §4.3.1, p. 618]. Found online by AIMD: "we additively increase the batch size by a fixed amount until the latency to process a batch exceeds the latency objective. At this point, we perform a small multiplicative backoff, reducing the batch size by 10%." [CLIPPER17 §4.3.1, p. 619]
- **Kleinrock's power**, throughput divided by delay: "power will be maximized at that value of throughput where a ray out of the origin" of the delay–throughput plane is tangent to the delay curve [KLE79 §2, p. 43.1.2, OCR]. For M/M/1 the optimum is at twice the minimum delay and half the maximum throughput [KLE79 §2, p. 43.1.2].
- **Knees.** Kneedle defines a knee through curvature and approximates it as the point farthest from the chord of the normalized curve. Its sensitivity `S` "is a measure of how many “flat” points we expect to see in the unmodified data curve before declaring a knee" [KNEEDLE11 §III, PDF p. 4]. It warns that "there exists neither an accepted definition of a knee nor a general systematic approach for detecting one" [KNEEDLE11 §I, PDF p. 1] and that "knee detection is an inherently heuristic process" [KNEEDLE11 §II, PDF p. 2].
- **Pipelining depth.** For Paxos, "From these, we can compute the maximum number of parallel instances that the system can sustain as: w = ⌈min(wcpu , wnet )⌉", where each `w_R` is the instance's wall time divided by resource `R`'s busy time per instance [SS13 §4.3, Eq. 9, p. 176]. *DERIVED mapping:* the writer's depth is `⌈T_batch/φ_device⌉`, with `T_batch` the wall time of write plus flush and `φ_device` the device busy time per batch. Finding 9 measured the answer on this machine: two concurrent write-and-flush streams reached 5.99 GB/s, one reached 5.12 GB/s and four reached 5.55 GB/s.

### 5.3 The calculation (DERIVED)

Model a batch's service time as `T(b, n) = F0 + n·c_r + b/BW`, with `b` bytes, `n` requests and per-request cost `c_r`. Then:

```
throughput X(b)   = b / T(b)
efficiency e(b)   = X(b)/BW = b/(b + F0·BW)            (bytes term only)
power X/T         = b/T(b)²   is maximal at   b* = F0·BW     (e = 1/2, T = 2·F0)
bytes for a target efficiency e:   b_e = F0·BW·e/(1 − e)
latency-bounded maximum (Clipper): b_max = BW·(d_batch − F0)
request-count analogue:           n* = F0/c_r
```

`batch_bytes` should be `min(b_max, memory bound)`, where `d_batch` is the latency the batch may add, from §4's budget. `batch_requests` should be `n*` or its latency-bounded analogue. The power point `b* = F0·BW` is Kleinrock's rule in deterministic form: twice the minimum delay, half the throughput. It is the natural *target* size when no latency budget is given.

**Worked example, and why it must be measured.** On this machine `F0 ≈ 4.7 ms` (4 KiB write plus `F_FULLFSYNC`), and 32 MiB written and flushed runs at 5.3 GB/s, so `T(32 MiB) ≈ 6.3 ms`. An affine fit through these two points implies a marginal rate of 33.5 MB / 1.6 ms ≈ 21 GB/s, four times the rate the device sustains for 32 MiB batches. Write and flush overlap inside the device, so the affine model does not describe it past small batches. The calibration must measure `T(b)` on a ladder (4 KiB, 1 MiB, 8 MiB, 32 MiB, 128 MiB, …) and at pipeline depths 1, 2 and 4, then fit or read the knee from those points rather than from assumed costs.

### 5.4 Inputs

Per-batch triples (`b`, `n`, `S`) from the writer give an online least-squares fit of `F0`, `c_r` and `1/BW`, updated with the same gains as §3. The calibration ladder gives the starting values and the device's saturated `BW` at each depth.

### 5.5 Uncertain

Device write caches make `T(b)` nonlinear. Reads that share the device raise `F0` (note 03, HL23-F12). The latency budget `d_batch` comes from §4's policy input.

---

## 6. Index budget (`max_fragments = 2^22`) and fragments per chunk (`fragments_per_chunk = 4096`)

### 6.1 Now

`max_fragments` bounds index memory and sizes the log at format: three checkpoints of the full budget. A checkpoint record is 76 bytes (`PUT_LEN`), so 2^22 fragments make a checkpoint of about 320 MB and a log of about 0.97 GB. The benchmark sets its own budget, `volume/smallest × 2/3`. `fragments_per_chunk` caps the fragments of an appended chunk before it must be sealed.

### 6.2 Models

No paper reviewed here gives these limits. They follow from capacity, the workload's chunk sizes and memory (DERIVED):

```
max_fragments = min( C_volume / E[chunk size] · (1 + margin),  M_index / m_entry )
```

`E[chunk size]` is a workload property, learned from the size histogram of stored chunks. `m_entry` is the measured memory per index entry. `M_index` is the per-volume share of host memory, detected. Examples: 20 TB of 8 MiB erasure-coded shards is 2.4M fragments, which fits in 2^22. 4 TB of 64 KiB chunks is 61M fragments, which does not. At that size the index is a memory question that capacity planning must answer at format time, because the log size depends on it.

For `fragments_per_chunk`, a read of a chunk stored as `f` fragments costs about `f·t_frag + size/BW`, where `t_frag` is the per-fragment fixed cost of I/O plus header verification. Bounding the read amplification to `1 + ε` gives `f ≤ ε·size/(BW·t_frag)`. An 8 MiB chunk at 3 GB/s takes 2.8 ms; if `t_frag` were 20 µs, `ε = 1` would allow about 140 fragments. That `t_frag` is illustrative, not measured. The alternative to a hard limit is to rewrite a chunk contiguously when it is sealed. For context, S3 caps a multipart upload at 10,000 parts (note 05 §6.1), but a part is not a chunk fragment.

### 6.3 Inputs, calculation, uncertainty

Inputs: the chunk-size histogram, `m_entry` (from allocator statistics at a known entry count), the memory budget, and `t_frag`, timed on reads of appended chunks. The calculation is the two formulas above. Uncertain: the production chunk-size distribution is unknown before deployment, and both limits are fixed at format.

---

## 7. Segment size (`segment_size = 256 MiB`)

### 7.1 Now

256 MiB, fixed at format. The comment in `layout.rs` says it "matches host-managed SMR zones and is in the range of ZNS zone capacities", and design §2 cites BAH+21 §2.3 and AD15 for the same claim.

### 7.2 Verification

- **AD15 does not support it.** Skylight examined drive-managed SMR drives: "the examined drives have a small band size of 15–40 MiB" [AD15 §1, p. 136], and the Seagate drive's bands were 30 MiB [AD15 §4.7, p. 144].
- **BAH+21 does not support it.** Its one ZNS SSD has a 2,048 MiB zone size, 1,077 MiB zone capacity and 14 maximum active zones [BAH+21 §5, Table 3, p. 696]. The paper explains the split: zone capacity "enables the zone size of ZNS SSDs to align with the power-of-two zone size industry norm introduced with SMR HDDs" [BAH+21 §2.3, p. 691]. It gives no 256 MiB figure.
- **AWK+19 does.** The host-managed SMR zone interface "manages the disk as a sequence of 256 MiB regions that must be written sequentially" [AWK+19 §3.3, p. 358].

So 256 MiB is right for host-managed SMR, wrong as a 1:1 match for the one ZNS device in the evidence, and unrelated to drive-managed SMR bands.

### 7.3 Models

- **Seek amortization.** "The segment size is chosen large enough that the transfer time to read or write a whole segment is much greater than the cost of a seek to the beginning of the segment." Sprite used 512 KB or 1 MB [RO92 §3.2, preprint p. 4].
- **Transfer versus cleaning.** "we show how to choose the LFS segment size by trading transfer efficiency against cleaning efficiency" [MRC97 §1, p. 239]. Smaller segments help cleaning because they are "more likely to empty completely before cleaning" [MRC97 §4.1, p. 241].
- **Uniform random traffic is the exception.** For LRU cleaning under uniform traffic, "We note that Equations 3 and 4 are independent of the block size Np." [DES12 §3.1, PDF p. 3]. Segment size affects cleaning cost only when the workload has locality. Mantle's workload does: chunks die in groups by write time.

### 7.4 Calculation (DERIVED)

```
zoned device:        S_seg = zone capacity (device-reported), open segments ≤ active-zone limit
rotating (CMR) disk: S_seg ≥ k · t_access · BW_seq      (overhead ≤ 1/k per segment transfer)
flash:               S_seg ≥ the sequential-write size at which bandwidth stops rising (calibrated)
all:                 S_seg ≥ max_record / δ            (a record that does not fit wastes ≤ δ of a segment)
then:                the smallest S_seg meeting these bounds, since smaller segments clean better (MRC97)
```

Illustration with assumed inputs, not measurements: a disk with 8 ms access and 200 MB/s at `k = 20` needs 32 MB. With 8 MiB chunks, 256 MiB segments give `δ ≈ 3%`.

### 7.5 Uncertain

Flash erase-block and superblock sizes are not exposed (note 03, HKA17). The cleaning-efficiency side of MRC97's trade-off needs traces of the real workload's death times. Segment size is fixed at format, so it is a capacity-planning computation.

---

## 8. Checksum block (`checksum_shift = 16`, 64 KiB)

### 8.1 Now and verification

64 KiB, citing GFS. GFS states the fact but not a reason: "A chunk is broken up into 64 KB blocks. Each has a corresponding 32 bit checksum." [GGL03 §5.2, PDF p. 10]. Note 03 §15.8 C2 records the other constraint: CRC-32C's guaranteed Hamming distances are verified only up to 16 KiB blocks. Each record's header carries its own shift, so the block can differ per chunk.

### 8.2 Model (DERIVED)

A read of `r` bytes at an `a`-aligned random offset, with blocks of `k = K·a` bytes, verifies on average `r + k − a` bytes. The read touches `⌈(o + r)/k⌉` blocks, with `o` uniform over the `K` aligned positions. The read amplification is therefore

```
A(k, r) = 1 + (k − a)/r
```

Checksum metadata costs `4/k` of the payload. At 4 KiB reads, 64 KiB blocks give `A = 16` and 4 KiB blocks give `A = 1`. At 1 MiB reads, 64 KiB blocks give `A = 1.06`.

**Calculation:** per chunk class, choose the largest `k` with `E_r[(k − a)/r] ≤ ε` over the class's read-size distribution. Where a guaranteed error-detection distance matters, cap `k` at the length up to which CRC-32C's distance is verified: 16 KiB per note 03 §15.8 C2. Beyond it only the roughly 2^−32 chance of missing random corruption applies.

### 8.3 Inputs and uncertainty

Inputs: a histogram of read sizes per chunk class, kept by the read path. Uncertain: a chunk's future read pattern is not known when it is written. Classes, such as small objects against erasure-coded shards read whole, carry the prediction.

---

## 9. Index log size and checkpoint trigger

### 9.1 Now

The log holds three checkpoints of the full budget plus headroom: about 0.97 GB for 2^22 fragments. A checkpoint is written, with two flushes, inside the writer loop when `cursor.used > log_size/3`. `cursor.used` counts from the start of the last checkpoint, so it includes the checkpoint itself. The trigger therefore fires once the checkpoint plus the frames written after it exceed about one full-budget checkpoint (323 MB). With a small index that is hundreds of megabytes of frames. With a full index, whose checkpoint is about 320 MB, it is about 2.8 MB. Every 2.8 MB of new log then costs a 320 MB checkpoint and a writer stall of roughly 70 ms on this machine (DERIVED from `layout.rs`, `writer.rs` and the measured 5 GB/s). Design §5 states a different rule: checkpoint "once the log since the last checkpoint exceeds the checkpoint's own size".

### 9.2 Models

- **Young.** Checkpointing "is a very practical question that does not appear to have been addressed satisfactorily in the literature, and in practice a variety of rules of thumb with no substantial justification appear to be in use" [YOUNG74, p. 530, OCR]. The model: failures are Poisson with mean interval `T_f`, and a checkpoint takes `T_s`. The first-order optimum is `T_c = √(2·T_s·T_f)`, valid for `T_s ≪ T_f` [YOUNG74, p. 531, formula read from the page image]. Young notes that "In practice the occurrences of failures tend to cluster, because you think you have the cause of the failure fixed, but you don't", and that "the value of the optimum checkpoint interval is to be taken as a design goal which will not always be attained exactly" [YOUNG74, p. 531, OCR].
- **Daly's higher-order estimate** [DALY06] is UNVERIFIED and not used.
- **ARIES** states the purpose: "Periodically, checkpoints are taken to reduce the amount of work that needs to be performed during restart recovery. The work may relate to the extent of the log that needs to be examined" [ARIES92 §5.4, p. 121, OCR].
- **DeWitt et al.'s scale example:** "Consider the case of 1000 transactions per second, two dirty pages per transaction, and 30 seconds between checkpoints. In the worst case, 60,000 pages would need to be written at the checkpoint!" [DKO+84 §5.3, p. 7]
- **Oracle Fast-Start** replaces a frequency with a recovery bound: "Instead of requiring administrators to specify a frequency for issuing (conventional) checkpoints, the fast-start mechanism provides the following dynamic configuration parameters for adaptively adjusting the rate of checkpointing to impose predictable bounds on roll-forward time". The worked example: "If an administrator believes that the I/O subsystem can perform approximately 1000 random IOs per second, she should set “FAST_START_IO_TARGET = 50000” to limit roll-forward time to approximately 50 seconds." And: "Since the time it takes to generate a certain amount of redo is approximately the same as the time it takes to apply that redo, LOG_CHECKPOINT_TIMEOUT may be interpreted as an upper bound on recovery time." [LAH01 §3.1, p. 595]
- **SiloR measured recovery cost:** "The smaller the distance between checkpoints, the less log data needs to be replayed, and we found the size of the log to be the major recovery expense." [SILOR14 §4.1, p. 468]. "Recovery takes 211 s, or about 1.08 s/GB of recovery data." [SILOR14 §6.4, p. 474]. "Thus, recovery time is proportional to the amount of data that must be read to recover, and log replay is the limiting factor in recovery, justifying our decision to checkpoint frequently." [SILOR14 §6.4, p. 474]. Their interval is fixed: "The next checkpoint is begun roughly 10 seconds after the previous checkpoint completed." They add: "In future work, we would like to investigate a more flexible scheme that, for example, could delay a checkpoint if the log isn't growing too fast." [SILOR14 §4.3, p. 470]
- **Raft's rule** (NON-PEER-REVIEWED extended version): "If a server snapshots too often, it wastes disk bandwidth and energy; if it snapshots too infrequently, it risks exhausting its storage capacity, and it increases the time required to replay the log during restarts. One simple strategy is to take a snapshot when the log reaches a fixed size in bytes. If this size is set to be significantly larger than the expected size of a snapshot, then the disk bandwidth overhead for snapshotting will be small." [RAFTX §7, PDF p. 13]

### 9.3 Calculation (DERIVED)

Let `C` be the current checkpoint size, `L_trig` the log bytes written since the last checkpoint when the next one starts, `ρ` the replay rate, `ρ_c` the checkpoint load rate, `T_rf` the roll-forward time over open segments, `w` the log growth rate, `T_s` the checkpoint's write-plus-flush time, and `T_f` the mean interval between restarts. Three constraints apply:

```
space (today's invariant):   C_prev + L_trig + C_new + headroom ≤ L      ⇒ L_trig ≤ L − 2·C_max − headroom
recovery time:               C/ρ_c + L_trig/ρ + T_rf ≤ T_budget          ⇒ L_trig ≤ ρ·(T_budget − C/ρ_c − T_rf)
bandwidth (Raft):            C/(C + L_trig) ≤ x                           ⇒ L_trig ≥ C·(1 − x)/x
```

Young's model, with the lost work replaced by replay time: a crash at time `τ` into an interval replays `w·τ/ρ` seconds of log. The expected overhead per unit time is `T_s/T_c + (w·T_c/2)/(ρ·T_f)`, minimized at

```
T_c* = √(2·T_s·T_f·ρ/w)
```

**Trigger:** checkpoint when `L_trig` reaches the smaller of the space and recovery-time bounds, but not before the bandwidth floor. If the floor exceeds the recovery bound, the index is too large for the recovery budget on this device, and that should be reported rather than silently accepted.

**Log size.** The bandwidth floor sets the log size. At checkpoint bandwidth share `x`, the log must hold `C_prev + C·(1 − x)/x + C_new`, so `L ≈ (2 + (1 − x)/x)·C_max`. Today's `L ≈ 3·C_max` supports `x = 1/2`, which is design §5's rule; `x = 10%` needs `11·C_max`. The trigger must measure `L_trig` from the end of the last checkpoint, not from its start.

**Worked example.** For 4 KiB puts at 3.71K/s, the log grows about 0.94 MB/s: 230 frames per second, each padded to 4 KiB. The full-budget checkpoint is 320 MB. At ~5 GB/s it takes about 64 ms plus two ~4.7 ms flushes, so `T_s ≈ 0.07 s`. With `ρ = 1 GB/s` (an assumed value, to be measured) and a restart every 30 days, `T_c* ≈ 5.6 h`, far longer than the space bound allows, so space binds. Under design §5's rule (`x = 1/2`), a full index checkpoints every `320 MB / 0.94 MB/s ≈ 340 s`. That costs about 0.02% of writer time and bounds replay to about 0.3 s at 1 GB/s. Under today's trigger the same index checkpoints every ~3 s: about 2.3% of writer time stalled in checkpoints, and ~110 MB/s of checkpoint writes. On a disk replaying at 150 MB/s, 320 MB of log takes about 2 s to replay plus about 2 s to load the checkpoint.

### 9.4 Inputs, uncertainty

Inputs: `ρ` and `ρ_c`, timed at every open (`RecoveryReport` already counts frames); `T_s`, timed per checkpoint; `w`, from the log cursor; `T_f`, the restart history. `T_budget` is a policy input, an availability target, that must be stated and cited. Uncertain: `ρ` varies by device and cache state; roll-forward cost depends on the size of open segments.

---

## 10. Cleaner: watermarks, `FUTILE_AFTER`, the 5 s wake, `CLEANER_RESERVE`

### 10.1 Now

- Cleaning starts below `max(2, segments/16)` free segments and stops at `max(segments/8, low + 1)` (`volume.rs`).
- The writer pokes the cleaner after each batch while free segments are below `low`. Otherwise the cleaner wakes every 5 s (`IDLE`).
- A pass stops after `FUTILE_AFTER = 4` victims with no net gain, and stays idle until more data dies.
- Client writes leave `CLEANER_RESERVE = 1` free segment to the cleaner. When they cannot open a segment, a put fails with `Full` even if partly dead segments hold reclaimable space.

### 10.2 Models

- **RO92.** "In our work so far we have not methodically addressed the first two of the above policies. Sprite LFS starts cleaning segments when the number of clean segments drops below a threshold value (typically a few tens of segments). It cleans a few tens of segments at a time until the number of clean segments surpasses another threshold value (typically 50-100 clean segments). The overall performance of Sprite LFS does not seem to be very sensitive to the exact choice of the threshold values." [RO92 §3.4, preprint p. 6]. The first two policies are when to clean and how much. Steady state: "the cleaner must generate one clean segment for every segment of new data written", which gives write cost `2/(1 − u)` [RO92 §3.4, preprint p. 6]. Design §8 cites §3.6 for the watermarks; the passage is in §3.4.
- **BSD-LFS.** "To ensure that the cleaner can always run and eventually generate more free space, normal writing is suspended when the number of clean segments drops to two." Also: "There are degenerative cases where cleaning a segment can actually consume more space than it frees" [SBMS93 §3.5, p. 314, OCR].
- **Idle-time cleaning and the runway.** BHS95 measured how much data arrives between cleaning opportunities, because "This determines the maximum disk utilization that should be employed to avoid cleaner interference with normal disk activity" [BHS95 §5, PDF p. 5]. On their traces, "We never observed more than 350 MB (4.5% of Maytag’s disk space) written before cleaning occurred" (NFS-async) and 420 MB (NFS-sync) [BHS95 §5.2, PDF p. 8]. Their trigger: "a long interval (greater than 2 seconds) is a good predictor of an even longer interval (greater than 4 seconds)" [BHS95 §5.1, PDF p. 5]. "With a simple heuristic of cleaning whenever the disk has been idle for two seconds, we can virtually eliminate any user-perceived cleaning latency." They add that some workloads, such as OLTP, "may not demonstrate the idle-gap distribution on which this heuristic depends" [BHS95 Conclusions, PDF p. 11].
- **F2FS.** "Foreground cleaning is triggered only when there are not enough free sections, while a kernel thread wakes up periodically to conduct cleaning in background." It "reserves a small unused capacity (5% of the storage space by default)". And "Background cleaning does not kick in when normal I/O or foreground cleaning is in progress." [F2FS15 §2.5, pp. 276–277]. These are defaults, not derivations.
- **MRC97's simulator** cleaned only when out of space: "The cleaner runs when there are no more empty segments available for new data." [MRC97 §3.1, p. 240]
- **Write amplification against spare space** (DES12). Their model keeps a low watermark on the free list and cleans until it "has reached w again" [DES12 §2, PDF p. 2]. It gives a closed form for LRU cleaning under uniform traffic. At spare factors of 0.03, 0.07, 0.11, 0.17 and 0.23, write amplification is 16.8, 7.3, 4.7, 3.1 and 2.4 [DES12 §3.1, Table 1, PDF p. 3]. This is the worst case for random overwrites. It is the price of running a volume nearly full.

### 10.3 Calculation (DERIVED)

Let `F` be free bytes above the reserve, `W` the client write rate, `W_peak` its high quantile over the reaction window, `u_v` the live fraction of chosen victims, `t_seg(u)` the measured time to clean one victim, `G = (1 − u_v)·S_seg/t_seg` the cleaner's net space generation, and `D_q` a high quantile of bytes written between cleaning opportunities (BHS95's measurement, taken online).

```
start cleaning when   F ≤ F_start = W_peak·(t_react + t_seg(u_v)) + D_q
sustainable ingest    W ≤ G;  when W > G, admit client writes at rate G (throttle) instead of failing with Full
stop cleaning at      F_stop = F_start + H,  with H large enough to amortize a pass's fixed cost
                      (RO92 cleaned "a few tens of segments at a time"); measure the per-pass setup cost
FUTILE_AFTER          = ⌈1/(1 − ū)⌉, where ū is the mean live fraction of this pass's victims: k victims free
                        k segments while their live data fills up to ⌈k·ū⌉ new ones, so the first net gain comes
                        at k = ⌈1/(1 − ū)⌉
victim cap            relocation cost per byte freed is u/(1 − u); for a cleaning budget B_c (bytes copied per
                        byte freed), clean only victims with u ≤ B_c/(1 + B_c).  B_c = 3 gives u ≤ 0.75,
                        which is the case where FUTILE_AFTER = 4 is right
CLEANER_RESERVE       one segment is enough while the cleaner relocates one victim at a time and relocation
                        rewrites no more than the victim's live bytes plus one block of padding per writer batch
                        (a victim of 256 MiB of 4 KiB records relocated in ~63 batches pads at most ~250 KiB, so this
                        holds for u < 0.999); relocating k victims at once needs k
t_react               bounded by the writer's poke (≈ one batch). Every path that lowers free space runs a writer
                        batch, which pokes the cleaner, so the 5 s wake is a backstop. If kept, its period must be at
                        most (F_start − reserve)/W_peak − t_seg (INFERENCE)
```

**Worked examples.** On a 20 TB volume, 74,506 segments of 256 MiB, today's rule starts cleaning below 4,656 free segments (1.25 TB) and stops at 9,313 (2.5 TB). Now take illustrative inputs, not measurements: a disk writing 200 MB/s, victims at `u = 0.5` (about 1.3 s to read and rewrite 134 MB), and BHS95's worst observed 420 MB as `D_q`. That gives `F_start ≈ 200 MB/s × 1.3 s + 420 MB ≈ 0.7 GB`, three segments. The fraction rule holds about 1,800 times more space out of use. At the other end, the 4 GiB benchmark volume has about 15 segments, so the rule gives `low = 2` (512 MiB). At 3 GB/s that is at most a 0.18 s runway, comparable to one victim's cleaning time, so puts can be refused with `Full` while dead space exists.

### 10.4 Inputs and uncertainty

Inputs: `W`, and its quantile over windows of `t_react + t_seg`; per-victim `t_seg` and `u`; free and dead bytes (both tracked); the idle-interval distribution (§12) if cleaning should use idle time. Uncertain: the production death-time distribution, which sets `u_v`; how much cleaning I/O slows foreground reads (note 03, HL23-F12), which may argue for idle-time cleaning (BHS95, GBS95) on top of the runway rule.

---

## 11. Identifier reservations (`SEQUENCE_RESERVE = 2^24`, `INCARNATION_RESERVE = 2^16`)

### 11.1 Now

When a batch would issue a sequence or incarnation beyond the superblock's limit, the writer raises the limit by the reserve and writes and flushes the superblock inside that batch's commit. Recovery resumes above the limits.

### 11.2 Models

- **Percolator's timestamp oracle** is the same mechanism: "The oracle periodically allocates a range of timestamps by writing the highest allocated timestamp to stable storage; given an allocated range of timestamps, the oracle can satisfy future requests strictly from memory. If the oracle restarts, the timestamps will jump forward to the maximum allocated timestamp (but will never go backwards)." [PERC10 §2.3, PDF p. 6]. The paper gives no range size. It reports "around 2 million timestamps per second from a single machine", with clients batching requests: "As the oracle becomes more loaded, the batching naturally increases to compensate." [PERC10 §2.3, PDF p. 6]
- **PostgreSQL sequences** (NON-PEER-REVIEWED). `CACHE` "specifies how many sequence numbers are to be preallocated and stored in memory for faster access" ["CREATE SEQUENCE"]. Numbers can be skipped: "transaction aborts or database crashes can result in gaps in the sequence of assigned values" [PG18 §9.17]. No sizing rule is given.

### 11.3 Calculation (DERIVED)

A reservation costs one superblock write and flush, `c_sb`: 4.7 ms on this machine, the durable 4 KiB write. With issue rate `r` and reservation `R`, the cost is `c_sb` per `R/r` seconds, a fraction `c_sb·r/R` of writer time. To keep that fraction at or below `ε`:

```
R ≥ r_max · c_sb / ε
```

Sequences at `r = 3.71K/s` with `R = 2^24` cost one flush every 4,522 s (75 min), a fraction of about 10⁻⁶. At `ε = 10⁻³`, 2^24 covers up to `2^24·ε/c_sb ≈ 3.6M` puts/s. Incarnations: at 3 GB/s into 256 MiB segments, `r ≈ 11` per second, so `R ≥ 53` at `ε = 10⁻³`. 2^16 is about 1,000 times more than needed and lasts 97 minutes at that rate. Waste per crash is at most `R`, which is immaterial against a 2^64 space: `2^64/2^24 ≈ 10^12` restarts.

**Removing the separate flush.** The checkpoint already writes and flushes the superblock. If each checkpoint raises both limits by `r_max·T_ckpt,max`, where `T_ckpt,max` is the longest interval between checkpoints (§9), the reservation piggybacks on writes that happen anyway. The in-batch raise remains as a fallback.

### 11.4 Inputs and uncertainty

Inputs: EWMAs of sequences and incarnations issued per second, `c_sb` timed at each raise, the checkpoint interval. The choice of `ε` is policy, but any value between 10⁻⁶ and 10⁻³ gives reservations far below 2^64 exhaustion. Nothing material is uncertain.

---

## 12. Scrubbing: period, pacing, `MAX_DAMAGED`, at-risk mode (`scrub.rs`)

### 12.1 Now

- Every live fragment is verified once per `scrub_period`, 7 days by default. After each segment, the scrubber sleeps that segment's share of the period: `bytes/volume_bytes × period`.
- Segments are visited in index order.
- Once any damage is found, the volume is "at risk" and scrubbed continuously until reopened.
- At most 65,536 damaged chunks are remembered. When the list is full the code drops the newly found key, while the doc comment on `MAX_DAMAGED` says "the oldest are dropped".
- The module cites BGPS07 §6 and SDG10 §5 for "target 7 days, never more than 14".

### 12.2 Verification

- BGPS07 supports scrubbing, and a two-week period as the one its systems used: "Disk scrubbing is very useful for proactively detecting latent sector errors. More than 60% of these errors are discovered through scrubbing." [BGPS07 Table 1, PDF p. 2]. Also: "over 60% of all latent sector errors are discovered by the media scrubbing process, which scans the entire surface of the media at least once every two weeks" [BGPS07 §6.2, PDF p. 11]. It frames the trade-off without resolving it: "An overly aggressive rate of such background operations may negatively impact performance [3], while a low rate may adversely affect MTTDL." [BGPS07 §1, PDF p. 1]
- SDG10 describes practice: "Common scrub intervals are one or two weeks." It defines the standard scrubber's rate: "for a scrubbing interval s and drive capacity c, a drive is being scrubbed at a rate of c/s" [SDG10 §5, PDF p. 9]. It does not set a maximum of 14 days.
- Ceph: "Ceph scrubs metadata every day and data every week." [AWK+19 §5.1, p. 361]

Seven days is therefore practice from NetApp (two weeks) and Ceph (one week), not a derived value.

### 12.3 Models

- **OJ10** defines the objective: "Thus we define a new metric for hard drives called MLET (“Mean Latent Error Time”). MLET captures the percentage of time in which the disk is susceptible to data loss due to an LSE". Its search over strategies shows "how optimal scrubbing strategies depend on disk characteristics (e.g., the BER rate), as well as disk workloads" [OJ10 abstract, §1, PDF pp. 1–2]. One of its principles: "Scrubbing is not free: limit scrubbing rate to avoid collateral LSEs" [OJ10 Table 1, PDF p. 3].
- **SDG10** on order and bursts: "staggered scrubbing [13], can improve the mean time to error detection by up to 40%" [SDG10 §6, PDF p. 13], and "For commonly used intervals in the 7-14 day range, improvements in MTTED for these policies range from 30 to 70 hours, corresponding to an improvement of 10–20%" [SDG10 §5.2.3, PDF p. 12]. After a first error, "the probability of seeing additional errors drops off exponentially ... dropping close to 1% after only 10 weeks and below 0.1% after 30 weeks" [SDG10 §3, PDF p. 4].
- **AOS12, scheduling around foreground load.** "The decreasing hazard rates in our traces imply that after the system has been idle for a while, it will likely remain idle." Their Waiting policy issues scrub requests only after a period of idleness [AOS12 §V.B, PDF p. 9]. The request size is chosen as "the request size that will lead to a slowdown within the prescribed limit, while maximizing scrub throughput" [AOS12 §V.C, PDF p. 10].
- **MRZ+09:** "if idle times have low variability, then idle waiting is not necessary. Only if idle times are highly variable does idle waiting become necessary to minimize the impact of background activity on foreground performance." [MRZ+09 abstract, p. 4:1]
- **GBS95:** an idleness detector starts idle work "When the detector believes the system will be idle enough for long enough"; for timer-based detection, "The timer period can be fixed, variable, or adaptive." [GBS95 §1, §3.1.1, PDF pp. 1, 6]

### 12.4 Calculation (DERIVED)

**Period.** With a periodic scrubber, an error that appears at a random moment waits on average `T/2` to be found. A chunk region of `b` bytes hit by latent errors at rate `λ_s·b` is undetected-bad for a fraction `λ_s·b·T/2` of the time, to first order. If durability requires that a repair read, made after another replica is lost, meets an undetected error with probability at most `p*`:

```
T ≤ 2·p* / (λ_s · b)                       (reliability bound)
T ≥ V / B_scrub                            (bandwidth bound: V live bytes, B_scrub the bandwidth scrubbing may use)
```

`λ_s` is learned online from the scrubber's own findings, damaged fragments per byte verified per unit time, with field data as the prior (BGPS07, SDG10; their per-GB rates are in figures not read off here). For scale, 20 TB in 7 days needs 33 MB/s of sustained reads.

**At-risk window.** SDG10 found the probability of a further error in a two-week period falling to about 1% ten weeks after the first error. A principled window ends when that conditional probability, learned from mantle's own devices, falls to the rate of devices that have had no error. Today the volume instead scrubs continuously until it is reopened.

**Order.** Staggered order, as design §9 specifies, instead of segment order.

**Damaged list.** By Little's law the list holds on average (damage discovery rate) × (time to repair) entries. The bound should be a high quantile of that product, and overflow should mark the volume failed and trigger draining rather than silently dropping entries. BGPS07 found that "A large fraction of disks with latent sector errors develop fewer than 50 errors" [BGPS07 Observation 7, PDF p. 6], so a list of thousands already signals a failing device.

**Pacing.** Measure the idle-interval distribution and its CV. If the CV is low, pace at the constant rate `V/T` (MRZ+09). If it is high, use AOS12's Waiting policy, with the wait threshold and request size chosen to meet a foreground slowdown bound, while enforcing the period's lower bound on rate.

### 12.5 Uncertain

Latent errors cluster in space and time (BGPS07, SDG10), so the independent-arrival model is only first order. OJ10 needed simulation. The field data are old HDD populations (note 03 §17). `p*` is a durability policy input.

---

## 13. Calibration (`calibrate.rs`)

### 13.1 Now

`Plan::standard`: a 256 MiB span, capped at a tenth of free space, filled once. Then three rounds per point, with the round of median throughput reported, including its p50 and p99. 500 ms steps. Random 4 KiB reads at depths {1, 4, 16, 64}. Sequential 1 MiB transfers at depths {1, 4}. 64 durable 4 KiB writes (budget 2 s). Durable 32 MiB batches (budget 2 s). The read "knee" is the smallest depth reaching 90% of the best throughput: "the 10% is the margin for run-to-run noise".

### 13.2 Models

- **Kleinrock's power.** An M/M/1 queue "has maximum power when on the average there is only one message in the system". For M/M/m, "one should have a number of messages in the system such that each channel has on the average one message in transmission and no other messages waiting" [KLE79 §2, §3, pp. 43.1.2, 43.1.5, OCR]. The general statement is the tangent ray from the origin (§5.2).
- **BBR** (NON-PEER-REVIEWED) applies the same operating point to a network path: "A connection runs with the highest throughput and lowest delay when (rate balance) the bottleneck packet arrival rate equals BtlBw and (full pipe) the total data in flight is equal to the BDP (= BtlBw × RTprop)." The two quantities "obey an uncertainty principle: whenever one can be measured, the other cannot" [BBR16]. The minimum delay is measured at low load and the maximum rate at high load.
- **Knees** (KNEEDLE11, §5.2): a heuristic, defined through curvature.
- **Statistics.** GBE07 distinguishes large samples ("typically, n ≥ 30") from small ones, which need Student's t [GBE07 §3.2, PDF p. 6]. KJ13: "a given benchmark on a given platform is typically prone to much less non-determinism than the common worst-case of published corner-case studies" and "repetition is most needed where most uncertainty arises" [KJ13 abstract, PDF p. 2].
- **Device state.** SNIA-PTS (NON-PEER-REVIEWED) runs each test point "for 1 minute" per round [SNIA-PTS §7, PDF p. 27]. It calls a device steady when "Range(y) is less than 20% of Ave(y)" and "Slope(y) is less than 10%" over a five-round measurement window [SNIA-PTS §2.1.24, §2.1.13, PDF pp. 16–17]. It preconditions with "128KiB SEQ W for 2X (twice) the user capacity" [SNIA-PTS §7.1, PDF p. 26].
- **Small transfers.** HL23 found the 4 KB sweet spot "For data center grade SSDs" [HL23 §2.2, p. 2092]. The claim is verified, but for that device class only.

### 13.3 What the current choices give (DERIVED)

- **Median of three.** The population median lies between the minimum and maximum of `n` independent rounds with probability `1 − 2·2^(−n)`: 75% for `n = 3`. For 95% coverage by min and max, `n ≥ 6`. With `n = 9`, the 2nd and 8th order statistics give 96%.
- **p99 from 64 durable writes** is the sample maximum, since `⌈0.99·64⌉ = 64`. The maximum exceeds the true p99 with probability `1 − 0.99^64 = 0.47`, so the reported "p99" is below the true one more often than not. Estimating a quantile `q` within rank error `δ` at 95% needs `n ≥ 1.96²·q(1 − q)/δ²`: 1,522 samples for p99 ± 0.005, which is 7.1 s of durable writes at 4.7 ms each; 15,351 for p99.9 ± 0.0005.
- **The knee.** Kleinrock's rule and Little's law give the depth at which the device's parallel units are just full:

```
N* = X_max · R_min            (max throughput × minimum latency, both measured)
equivalently: N* = argmax_N X(N)²/N      (power X/R with R = N/X in a closed loop)
```

On this machine, 4 KiB reads at 15K/s at depth 1 (`R_min ≈ 66.7 µs`) and about 235K/s at depth 64 give `N* ≈ 15.7`, so 16. The 90% threshold should be replaced by this, with noise handled statistically: among ladder points, take the smallest depth whose throughput confidence interval overlaps the best one's (GBE07 §3.3, PDF p. 7).

Measured (2026-09-29): as a bound on reads held at the device, `N*` is too shallow. This machine's 4 KiB reads still gained 30% from 16 in flight to 64, and fell from 64 to 256; a chunk volume held at 16 with the rest waiting lost more than half its throughput at 32 callers, while one held at 64, the smallest depth whose interval overlaps the best one's, matched the file layer ([measurements/2026-09-29-read-depth.md](../measurements/2026-09-29-read-depth.md)). The ideal device the formula assumes keeps its latency flat until its parallel units fill; a real one's latency climbs before that. Power is the right optimum only where reads past it are refused to another copy with room.

### 13.4 Inputs and calculation (Recommendation)

- Ladders: geometric in depth, refined by bisection around the maximum of `X²/N`. Transfer size swept to find where IOPS × size reaches the bandwidth plateau, which becomes the large transfer instead of a fixed 1 MiB. `T(b)` measured for durable batches (§5).
- Rounds: run until each point's 95% interval is within ε of its mean, or the budget ends, reporting whichever happened (GBE07's JavaStats stops the same way).
- Steps: long enough for the reported quantiles (the sample counts above), and inside a window SNIA's steady-state test accepts.

### 13.5 Uncertain

The budget: all of this must fit in tens of seconds at startup. The trade-off between precision and time is KJ13's subject (§16). Short steps cannot see slow device transients such as write caches filling and garbage collection. Those need the benchmark's longer runs, or background measurement during operation.

---

## 14. Latency histogram (`histogram.rs`)

### 14.1 Now

Log-linear buckets: row `r ≥ 1` covers `[2^e, 2^(e+1))` with `e = r − 1 + 3`, split into 8 linear sub-buckets. Rows exist up to exponent 40, so values below 2^41 ns (36.7 minutes) have rows; larger values go to an overflow bucket. 313 buckets take 2.5 KB. A quantile is reported as the upper bound of its bucket, clamped to the maximum seen, so the error is one-sided and below 12.5%.

### 14.2 Models

- **DDSketch.** "x̃q is an α-accurate q-quantile if |x̃q −xq | ≤ αxq" [DDS19 §1, p. 2196]. With "γ := (1+α)/(1−α)", bucket `i` holds `γ^(i−1) < x ≤ γ^i` and the estimate returned is `2γ^i/(γ + 1)` [DDS19 §2.1, p. 2198]. The motivation: "Given the inadequacy of rank accuracy for tracking the higher order quantiles for distributions with heavy tails, we turn instead to relative accuracy." [DDS19 §1, p. 2196]
- **HdrHistogram** (NON-PEER-REVIEWED) sets precision in significant digits: with 3 digits, "Value quantization within the range will thus be no larger than 1/1,000th (or 0.1%) of any value." [HDR README, introduction]. Its structure is the one mantle uses: "exponentially increasing bucket value ranges ... with each bucket containing a fixed number (per bucket) set of linear sub-buckets" [HDR README, "Histogram variants and internal representation"].

### 14.3 Calculation (DERIVED)

A sub-bucket `j` of row `e` spans `[a, b)` with `(b − a)/a = 1/(8 + j) ≤ 2^(−SUB_BITS)`.

```
upper-bound estimate (today):     relative error < 2^(−SUB_BITS)                 3 bits: 12.5%
harmonic-midpoint estimate 2ab/(a+b), DDSketch's form:
                                  relative error ≤ 1/(2^(SUB_BITS+1) + 1)       3 bits: 5.9%; 5 bits: 1.5%
buckets:                          (MAX_EXP − SUB_BITS + 2)·2^SUB_BITS + 1         3 bits: 313; 5 bits: 1,185
```

So `SUB_BITS = ⌈log2(1/α)⌉` for upper-bound reporting, or `⌈log2(1/α − 1)⌉ − 1` for midpoints, where `α` is the accuracy the consumers of the quantiles need. For a p99 compared against a target with margin `ε`, `α ≤ ε/2` keeps the estimation error to half the margin. The top of the range is the longest latency any consumer acts on, such as a request deadline; beyond it the overflow count is enough.

### 14.4 Uncertain

Which decisions will consume which quantiles is not yet defined, and they set `α`. The upper-bound estimate has one virtue: it never under-reports, which may be wanted for SLO alarms. The choice between the estimators is itself a policy.

---

## 15. Buffer pool (`buf.rs`)

### 15.1 Now

The read pool keeps at most one batch, 32 MiB, of free buffers, none larger than 32 MiB. The writer pool keeps at most 64 MiB, none larger than 64 MiB. A request takes the smallest free buffer between the needed size and twice it; otherwise it allocates exactly what it needs. Finding 3 motivated the pool: zeroed allocation cost 12.6% of a reader's time.

### 15.2 Models

- **Little's law** (LIT61): bytes in flight = throughput × latency, for example depth × transfer size.
- **Working set of free buffers.** "The variation in memory consumption over a fixed period of time defines a form of working set [Denning68]; specifically, it defines how many magazines the depot must have on hand to keep the allocator working mostly out of its high−performance magazine layer. For example, if the depot’s full magazine list varies between 37 and 47 magazines over a given period, then the working set is 10 magazines; the other 37 are eligible for reclaiming." [BA01 §3.7, PDF p. 7]. Sizes are tuned, not chosen: "Rather than picking some “magic value,” we designed the magazine layer to tune itself dynamically", and "We enforce a maximum magazine size to ensure that this feedback loop can’t get out of control" [BA01 §3.4, PDF p. 7].
- **Size classes.** "In the worst case, memory usage is proportional to the product of the maximum amount of live data (plus worst-case internal fragmentation due to the rounding up of sizes) and the number of size classes." "(In practice, very coarse size classes generally lose more memory to internal fragmentation than they save in external fragmentation.)" Power-of-two classes are common, but "closer size class spacings have also been used, and are usually preferable" [WJNB95 §3.6, PS p. 36]. Knuth's buddy-system analysis was not read (UNVERIFIED).

### 15.3 Calculation (DERIVED)

```
reuse factor r:     a buffer up to r times the request wastes at most 1 − 1/r of it
                    (r = 2: 50%; r = 1.25: 20%, i.e. four linear classes per power of two)
free-buffer limit:  the working set, (max − min) of free bytes over a window, as BA01 does,
                    capped by a memory budget; hard upper bound = in-flight bytes by Little's law
                    = Σ over streams of (depth × transfer size)
```

Example: 64 readers of 1 MiB chunks hold up to 64 MiB in flight. If their number in flight swings by more than 32, a 32 MiB free limit frees and reallocates buffers, which is the cost finding 3 removed. The writer pool's limit of two batches has no stated model. With a pipelined writer it would be `depth × batch_bytes` (§5).

### 15.4 Inputs and uncertainty

Inputs: the free-bytes time series (min and max per window), allocation misses, in-flight bytes, and the memory budget. Uncertain: allocator costs differ by platform (finding 3 names macOS `xzm` and glibc's mmap threshold), so the benefit of a larger pool must be measured per platform.

---

## 16. Benchmark parameters (`bench.rs`)

### 16.1 Now

A 4 GiB scratch volume, or a tenth of free space if smaller, filled once. Chunk sizes of 4 KiB, 64 KiB, 1 MiB and 8 MiB. Closed-loop workers at {1, 4, 16, 64}. 1 s per put and read point. One pass per point. Finding 7 shows that one pass measures the drive's recent history as much as the store: points just after the fill ran at half rate. STATUS.md lists "each point repeated until its result is statistically stable" as remaining work.

### 16.2 Models

- **Independence and warm-up.** GBE07's steady-state method takes iterations once "the coefficient of variation (CoV)" of the last `k` "falls below a preset threshold, say 0.01 or 0.02", and computes intervals across invocations because "the various iterations within a single VM invocation are not independent" [GBE07 §4.2, PDF p. 10]. "To reach independence, we discard the first VM invocation for each benchmark from our measurements and only retain the subsequent measurements" [GBE07 §4.1, PDF p. 9]. "JavaStats stops the measurements and reports the confidence interval as soon as the desired confidence interval width is achieved or the maximum number of VM invocations and benchmark iterations is reached." [GBE07 §6.3, PDF p. 19]
- **Levels of repetition.** "We call a state independent if the execution times of the benchmark iterations are (statistically) independent and identically distributed." [KJ13 §6, PDF p. 5]. The optimal count at level `i` is `r_i = ⌈√((c_{i+1}/c_i)·(T_i²/T_{i+1}²))⌉`, where `c` are the costs of repeating at each level and `T²` the variances [KJ13 §9.3, Eq. 3, PDF p. 10]. "RECOMMENDATION: For each benchmark/VM/platform, conduct a dimensioning experiment to establish the optimal repetition counts (equation 3) for each but the top level of the real experiment." [KJ13 §9.3, PDF p. 10]
- **Device state** (SNIA-PTS, NON-PEER-REVIEWED): preconditioning, the steady-state test, one-minute points (§13.2).
- **Closed-loop latency** (HDR, NON-PEER-REVIEWED): when responses stall, a closed-loop recorder misses the requests that would have been sent. A 100-second pause in their example leaves "~99.99% of results at 1 msec or below" where the true share is about 50%. Correction "will tend to produce data sets that would much more accurately reflect the response time distribution that a random, uncoordinated request would have experienced." [HDR README, "Corrected vs. Raw value recording calls"]. Finding 2's stalls are exactly this case.
- **Worker counts** follow from §13's `N*`: include depth 1 (the latency floor), `N*`, and a point past saturation.

### 16.3 Calculation (DERIVED)

```
repetitions per point:  n ≥ (t_{0.975, n−1} · CV / ε)²
    CV = 0.3 (runs alternating between full and half rate have CV ≈ 0.33):  ε = 10% → n ≈ 37;  ε = 5% → n ≈ 140
    CV = 0.1 (a quiet point):                                                ε = 5% → n ≈ 18
bimodal results:        report the states separately (share of runs in each and each state's interval);
                        a mean and CV describe neither state
step length:            ≥ the samples the reported quantiles need (§13.3), and inside SNIA's steady-state window
scratch size:           ≥ several times the device's write-cache size; measure that size as AD15 does, by writing
                        until throughput drops; "a tenth of the free space" is a safety cap for the user's disk,
                        not a performance parameter, and should be labeled as such
chunk sizes:            the design's chunk-size distribution (small objects; erasure-coded shard size = block/k),
                        not a fixed ladder
```

The repetition formula above holds for normally distributed rounds (HB15 §4.2.2, as note 21 §6.4 finds). Note 21 gives cited methods for the rest of this section: independence checks, the dip test for states, and intervals of each state's median (note 21 §9).

### 16.4 Inputs and uncertainty

Inputs: per-point `CV` from a dimensioning run, levels and costs (step, process, fresh volume), the device's cache size. Uncertain: machine noise (finding 2) inflates `CV` and may make ±5% unaffordable. KJ13's method then says where to spend the budget, and the report should state the achieved interval rather than claim precision it lacks.

---

## 17. Parameter, model, inputs

| Parameter | Now | Model (sources) | Measured inputs | What it needs |
|---|---|---|---|---|
| Writer wait (`gather`) | `dw < p·S/(n+p)`, `p` learned | expected saving `Δ(t)` over the think-time distribution; ID01 anticipation; cap below `S` (§2) | `S`, return-delay histogram, `n`, `m`, other arrivals | writer changes only |
| Batch dispatch rule | queue drained, `L = 1` | NEU67; HSL+87 zero timer; DS73 threshold for open arrivals | arrival process type | keep `L = 1` unless measured open-loop load justifies `M > 1` |
| EWMA gain | 1/8 | JK88; RFC6298; variance `g/(2−g)` (§3) | `CV` of samples, sample rate | `ε`, `τ` from consumers |
| `Limits::queue` | 4,096 requests, blocking | Little; CoDel sojourn; Breakwater, DAGOR (§4) | `μ`, `BW`, sojourn times | caller deadline `D` and share `f` (policy) |
| `batch_bytes` | 32 MiB | fixed-cost amortization; Kleinrock power `b* = F0·BW`; Clipper `b_max` (§5) | `T(b)` ladder; online `F0`, `BW` | `d_batch` (policy) |
| `batch_requests` | 1,024 | `n* = F0/c_r` (§5) | per-request cost `c_r` | — |
| Writer pipeline depth | 1 (serial) | SS13 `w = ⌈T/φ⌉` (§5) | `T(b)` at depths 1, 2, 4 | writer redesign (finding 9) |
| `max_fragments` | 2^22 | capacity ÷ mean chunk size, memory ÷ entry size (§6) | chunk-size histogram, `m_entry`, RAM | fixed at format |
| `fragments_per_chunk` | 4,096 | read amplification `f·t_frag` bound (§6) | `t_frag` | — |
| `segment_size` | 256 MiB | zone capacity; RO92 seek amortization; MRC97 (§7) | zone report, access time, plateau size | fixed at format |
| Checksum block | 64 KiB | read amplification `1 + (k − a)/r` (§8) | read-size histogram per class | classes |
| Log size | 3 × full checkpoint | space invariant; sets the checkpoint bandwidth share `x`: `L ≈ (2 + (1 − x)/x)·C_max` (§9) | `C_max` | `x` (policy); 3× gives `x = 1/2` |
| Checkpoint trigger | log used, checkpoint included, > 1/3 (≈2.8 MB of new log at a full index) | frames since the checkpoint ≥ `C·(1 − x)/x`; min(space, `ρ·(T_budget − …)`); Young with replay (§9) | `ρ`, `ρ_c`, `T_s`, `w`, `C` | `T_budget`, `x` (policy) |
| Cleaner start / stop | segs/16, segs/8 | runway `W_peak·(t_react + t_seg) + D_q`; throttle at `W > G` (§10) | `W`, `t_seg(u)`, `u`, burst quantile | — |
| `FUTILE_AFTER` | 4 | `⌈1/(1 − ū)⌉`, or a cleaning budget `B_c` (§10) | victims' `u` | `B_c` (policy) if budgeted |
| Cleaner wake | 5 s | bounded by the runway; poke-driven (§10) | `W_peak`, `F_start` | — |
| `CLEANER_RESERVE` | 1 | one per concurrent victim (§10) | — | sound for one victim at a time |
| `SEQUENCE_RESERVE` / `INCARNATION_RESERVE` | 2^24 / 2^16 | `R ≥ r_max·c_sb/ε`; piggyback on checkpoints (§11) | issue rates, `c_sb` | `ε` (any small value) |
| Scrub period | 7 days | `2·p*/(λ_s·b) ≥ T ≥ V/B_scrub`; MLET (OJ10) (§12) | `λ_s` from findings, idle bandwidth | `p*` (durability policy) |
| Scrub pacing and order | constant rate, segment order | staggered order (SDG10); Waiting policy (AOS12) if idle CV is high (MRZ+09) | idle-interval distribution | — |
| At-risk mode | continuous until reopen | revert after the conditional error rate decays (SDG10) | time since last error | — |
| `MAX_DAMAGED` | 65,536, new keys dropped | Little's law on discovery × repair time; overflow ⇒ fail the volume (§12) | discovery rate, repair latency | — |
| Calibration rounds | 3, median | order statistics; run until the CI is within ε (§13) | per-round results | `ε`, budget |
| Calibration step | 500 ms | quantile sample size; SNIA steady state (§13) | op rate | — |
| Read depth knee | first ≥ 90% of best | `N* = X_max·R_min`, `argmax X²/N`; CI overlap (§13) | `X(N)`, `R(N)` | — |
| Small / large transfer | 4 KiB / 1 MiB | HL23 (DC SSDs); IOPS–bandwidth plateau sweep (§13) | size sweep | — |
| Durable batch | 32 MiB | `T(b)` ladder (§5, §13) | `T(b)` | — |
| Histogram `SUB_BITS`, range | 3, 2^41 ns | DDSketch `α`; midpoint estimator (§14) | — | `α` from consumers |
| Pool limits, reuse factor | 1 or 2 batches; 2× | working set (BA01); Little; size classes (WJNB95) (§15) | free-bytes series, in-flight bytes | memory budget |
| Bench repetitions | 1 per point | `n ≥ (t·CV/ε)²`; KJ13 levels; GBE07 (§16) | `CV` per point and level | `ε`, time budget |
| Bench step, scratch size, sizes, workers | 1 s, 4 GiB, 4 KiB–8 MiB, {1, 4, 16, 64} | quantile counts; cache size (AD15-style); design sizes; `N*` (§16) | cache size, `N*` | — |

---

## 18. Parameters with no principled model yet, and the measurement that would learn each

1. **`fragments_per_chunk`.** No source gives it. Learn `t_frag` by timing reads of chunks built from `f` appended fragments against the same chunk written whole. Then set `f` from a read-amplification budget, or rewrite chunks contiguously at seal.
2. **`max_fragments` for small-chunk workloads.** Learn the chunk-size distribution from the running store and the index's bytes per entry from allocator statistics. Capacity planning then decides whether a volume's index fits in memory.
3. **Queueing-delay share `f` and the caller deadline `D`.** Storage cannot derive them. They come from the S3 and replication protocols' timeouts. Until those exist, measure the sojourn-time distribution and report it, and bound only by `S`.
4. **Recovery-time budget `T_budget`** is an availability target. Measure `ρ` and `ρ_c` at every open, so the trigger can be computed once the target is set.
5. **Durability target `p*` for scrubbing.** Measure `λ_s` from the scrubber's findings, and the repair latency from the repair path, so the period can be computed once `p*` is set.
6. **Cleaner stop hysteresis `H`.** Measure the fixed cost of a cleaning pass (victim selection, index scan, the flush) against its per-segment cost. Choose `H` so the fixed cost is a stated fraction of a pass.
7. **Cleaning budget `B_c`** is the share of device bandwidth the cleaner may use. Measure the foreground latency impact of cleaning I/O on each device class (note 03, HL23-F12), then set `B_c` from a foreground slowdown bound, as AOS12 does for scrubbing.
8. **Segment size on conventional flash.** Measure the sequential-write plateau size, and record death-time traces from a production-like workload to evaluate MRC97's trade-off offline.
9. **Checksum block per chunk class.** Record read-size histograms per class.
10. **Histogram accuracy `α`.** Enumerate the decisions that read quantiles: SLO alarms, the calibration knee, the benchmark report. Take the strictest.
11. **Benchmark scratch size.** Detect the device's write-cache size by writing until throughput drops.
12. **Return-delay distribution of real submitters** (for §2). Record it in production. The benchmark's workers have near-zero think time and cannot stand in for S3 clients.

Out of scope: limits that exist for safety rather than performance. These include `MAX_BUFFER`, `MAX_ALIGNMENT`, `MAX_FRAME_BYTES`, `measure::MAX_DEPTH` and the superblock offsets (the 16 MiB spacing follows note 03's BGPS07 clustering result). They bound memory or corrupt inputs, and none is tuned.

---

## 19. Unverified items and corrections to other documents

**Not accessed or partly accessed.**
- DALY06: not accessible; unused.
- KNUTH: not read; unused.
- BAI54, LIT61, DS73, DEB76, ZX17: abstracts only. Their conditions, such as the cost structure of DS73's MDP, are stated only as far as the abstracts state them.
- HSL+87: the Tandem TR, not the LNCS text.
- JK88: the November 1988 revision, not the SIGCOMM '88 printing.
- DAGOR18: arXiv v3, not the ACM text.
- KJ13: the author's manuscript.
- WJNB95: the authors' revised version.
- RO92: the 1991 preprint.

**Corrections to repository documents** (INFERENCE, for their owners):
- `writer.rs` module docs and design §4 cite DeWitt et al. §5.2 for "every request that arrived while the previous batch was being made durable". DeWitt's group is defined by the log page. The zero-timer rule is HSL+87's [HSL+87 §2, TR p. 2]; PostgreSQL's `commit_delay = 0` [PG18 §28.5] describes the same behaviour (NON-PEER-REVIEWED).
- Design §4 says a full queue refuses with `Busy`. `Volume::submit` blocks on `SyncSender::send`, and no `Busy` error exists.
- Design §2 cites BAH+21 §2.3 and AD15 for 256 MiB segments, and `layout.rs` repeats the claim. The 256 MiB figure is AWK+19 §3.3's (host-managed SMR). BAH+21's ZNS device has 2,048 MiB zones and 1,077 MiB capacity, and AD15's drive-managed bands are 15–40 MiB.
- Design §5 says a checkpoint is taken "once the log since the last checkpoint exceeds the checkpoint's own size". The code's trigger, `cursor.used > log_size/3`, counts the checkpoint itself and fires after ~2.8 MB of new log when the index is full (§9.1).
- Design §8 cites RO92 §3.6 for low and high watermarks. The passage is RO92 §3.4, and it says the thresholds were "not methodically addressed".
- `scrub.rs` cites SDG10 §5 for "never more than 14" days. SDG10 describes one or two weeks as common and sets no maximum. The 14-day figure is NetApp's practice [BGPS07 §6.2].
- Design §9 specifies a staggered scrub order; `scrub.rs` visits segments in index order.
- `MAX_DAMAGED`'s comment says the oldest entries are dropped when full; `Findings::record` drops the new key.
- Note 03 §13.2, G6 and item 1 of §17 mark HSL+87 UNVERIFIED. Its content is now available from the TR (§2.2 here). G6's advice, zero delay unless measurements show the flush limits throughput, agrees with HSL+87's zero-when-shorter-than-a-log-write guard. The mechanism HSL+87 analyzes, CPU cost per commit write, is not mantle's bottleneck.
