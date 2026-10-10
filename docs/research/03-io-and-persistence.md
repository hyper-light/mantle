# 03 — I/O and Persistence: What the Literature Says About How Bytes Should Hit Disk

**Status:** research input for mantle's chunk store and for the persistence path of the metadata service's replicated log. This is not a design decision record; it is the evidence base those decisions should cite.
**Compiled:** 2026-09-28.
**Scope:** local persistence on storage nodes (NVMe SSD, HDD, SMR HDD, ZNS SSD), the I/O interface, durability/ordering, checksumming, scrubbing, corruption handling, space reclamation, and zone-friendly layout.

---

## 0. How to read this document

- **Citation keys** such as `[HL23 §2.3, p. 2092]` give the section and the printed proceedings/journal page. When the printed page is not in the PDF we had, we write `PDF p. N` (page index of the PDF we read) or cite only the section.
- **Quotes** in "double quotes" are verbatim, copied from text extracted from the PDFs with `pdftotext`. We only repaired hyphenation at line breaks. The DeWitt et al. 1984 PDF is an OCR scan, so quotes from it are marked **[OCR-corrected]**, meaning obvious OCR character errors were fixed. `[sic]` marks a typo that is in the original. Subscripts are written with an underscore (e.g. `e_i`), and superscripts inline (e.g. 10³).
- **Inference:** marks our own reasoning about what a finding means for mantle. The paper does not say it.
- **UNVERIFIED** marks anything we could not check against the primary text (for example, paywalled papers where we saw only the abstract). **NOT ADDRESSED** means we read the paper and it does not cover the question.
- **Method.** We downloaded every paper's PDF from the publisher, the author, or a university site (vldb.org, usenix.org, ACM/SIGCOMM, university pages), extracted the text, and located each quoted claim in it programmatically. Numbers come from the running text or tables. We did not read values off plots unless we say so. Two papers were only partly accessible: Castagnoli et al. 1993 (IEEE, paywalled; we verified the abstract and metadata, and took the details from Koopman 2002, which re-derives them) and Helland et al. 1987 (Springer LNCS, paywalled; we verified the metadata only). The per-paper findings below are grouped by topic rather than restating each paper's own findings list.
- Two **supplementary** peer-reviewed papers were added because the requested ones leave gaps that affect correctness: Koopman, DSN 2002 (the CRC-32C error-detection numbers) and Rebello et al., USENIX ATC 2020 (what `fsync` failure actually means on Linux).

### Citation keys

| Key | Paper |
|---|---|
| HL23 | Haas & Leis, *What Modern NVMe Storage Can Do, And How To Exploit It*, PVLDB 16(9), 2023 |
| DPI+22 | Didona, Pfefferle, Ioannou, Metzler, Trivedi, *Understanding Modern Storage APIs*, SYSTOR 2022 |
| HKA17 | He, Kannan, Arpaci-Dusseau, Arpaci-Dusseau, *The Unwritten Contract of Solid State Drives*, EuroSys 2017 |
| AWK+19 | Aghayev, Weil, Kuchnik, Nelson, Ganger, Amvrosiadis, *File Systems Unfit as Distributed Storage Backends*, SOSP 2019 |
| PCA+14 | Pillai et al., *All File Systems Are Not Created Equal*, OSDI 2014 |
| GAA17 | Ganesan, Alagappan, Arpaci-Dusseau, Arpaci-Dusseau, *Redundancy Does Not Imply Fault Tolerance*, FAST 2017 |
| AGL+18 | Alagappan et al., *Protocol-Aware Recovery for Consensus-Based Storage*, FAST 2018 |
| RO92 | Rosenblum & Ousterhout, *The Design and Implementation of a Log-Structured File System*, ACM TOCS 10(1), 1992 |
| BGPS07 | Bairavasundaram, Goodson, Pasupathy, Schindler, *An Analysis of Latent Sector Errors in Disk Drives*, SIGMETRICS 2007 |
| BGS+08 | Bairavasundaram, Goodson, Schroeder, Arpaci-Dusseau, Arpaci-Dusseau, *An Analysis of Data Corruption in the Storage Stack*, FAST 2008 |
| SDG10 | Schroeder, Damouras, Gill, *Understanding Latent Sector Errors and How to Protect Against Them*, FAST 2010 |
| SLM16 | Schroeder, Lagisetty, Merchant, *Flash Reliability in Production: The Expected and the Unexpected*, FAST 2016 |
| SP00 | Stone & Partridge, *When the CRC and TCP Checksum Disagree*, SIGCOMM 2000 |
| SRC84 | Saltzer, Reed, Clark, *End-to-End Arguments in System Design*, ACM TOCS 2(4), 1984 |
| BAH+21 | Bjørling, Aghayev, Holmberg, Ramesh, Le Moal, Ganger, Amvrosiadis, *ZNS: Avoiding the Block Interface Tax for Flash-based SSDs*, USENIX ATC 2021 |
| AD15 | Aghayev & Desnoyers, *Skylight—A Window on Shingled Disk Operation*, FAST 2015 |
| ATGD17 | Aghayev, Ts'o, Gibson, Desnoyers, *Evolving Ext4 for Shingled Disks*, FAST 2017 |
| DKO+84 | DeWitt, Katz, Olken, Shapiro, Stonebraker, Wood, *Implementation Techniques for Main Memory Database Systems*, SIGMOD 1984 |
| HSL+87 | Helland, Sammer, Lyon, Carr, Garrett, Reuter, *Group Commit Timers and High Volume Transaction Systems*, HPTS 1987 (LNCS 359, 1989) |
| CBH93 | Castagnoli, Bräuer, Herrmann, *Optimization of Cyclic Redundancy-Check Codes with 24 and 32 Parity Bits*, IEEE Trans. Commun. 41(6), 1993 |
| GGL03 | Ghemawat, Gobioff, Leung, *The Google File System*, SOSP 2003 |
| Koo02 | Koopman, *32-Bit Cyclic Redundancy Codes for Internet Applications*, DSN 2002 (supplementary) |
| RPA+20 | Rebello, Patel, Alagappan, Arpaci-Dusseau, Arpaci-Dusseau, *Can Applications Recover from fsync Failures?*, USENIX ATC 2020 (supplementary) |

Full bibliographic entries with DOIs are in §16.

---

## 1. Executive summary: the decisions the evidence supports

Each item points to the detailed findings below. Items marked *(inference)* are our synthesis, not a single paper's claim.

1. **mantle, not the kernel, should own the data path: a raw block device, or a few large preallocated files, opened with `O_DIRECT`, plus mantle's own cache.** Ceph moved off file systems to BlueStore on raw devices. Steady-state write throughput rose 50–100% and tail latency fell by an order of magnitude [AWK+19 §6.1, p. 362]. Every engine in HL23 ran with `O_DIRECT`, and reaching the array's limit required "disabl[ing] most operating system features, such as the file system, RAID, and the OS page cache" [HL23 §2.5, p. 2093]. Buffered reads cap how many requests reach the device [HKA17 Obs. #5]. OS writeback makes latency unpredictable [AWK+19 §3.4].
2. **On NVMe, 4 KiB is the minimum I/O size and the alignment unit.** It gives the best trade-off between IOPS, latency, and amplification. Going below 4 KiB hurts [HL23 §2.2, p. 2092].
3. **Keep many I/Os in flight.** NVMe needs "around 1000 concurrent I/O requests, i.e., more than 100 per device … to get decent performance, and 3000 to fully saturate" an 8-SSD array [HL23 §2.3, p. 2092]. Blocking I/O would need more than 1000 threads, which oversubscribes the CPU. Use asynchronous I/O driven by lightweight per-core tasks [HL23 §3.2]. SATA's queue is limited to 32 requests [HKA17 §3.1].
4. **I/O interface: io_uring in completion-polling mode (IOPOLL) on thread-per-core workers, with one ring per core.** It was "the only kernel interface that could achieve the full bandwidth" of the array [HL23 §6, p. 2101]. Avoid SQPOLL unless each device has about two dedicated cores: it collapses without them [DPI+22 §3.2–3.3], and it gave no efficiency gain in HL23 [HL23 §3.4, §4.5]. SPDK is the most CPU-efficient option but needs root and exclusive ownership of the device [HL23 §6].
5. **Make writes large, aligned, and grouped by when the data will be deleted.** SSDs reward large or concurrent requests, locality, aligned sequential writes, grouping by *death time*, and uniform data lifetimes [HKA17 §3]. Separating hot from cold data is "inaccurate and misleading" advice [HKA17 §3.4].
6. **Group commit:** one device flush should cover every record that became ready since the last flush. Clients are acknowledged only after the record is durable. This is where the 10× gain comes from (100 → 1000 tx/s) [DKO+84 §5.2]. GFS batches its operation-log records the same way [GGL03 §2.6.3].
7. **Never stack a journal on a journaling file system.** Creating an object in NewStore (RocksDB plus files on XFS) cost four device-cache flushes. Ceph's raw-disk emulation needed two, and object creation was 70–80% faster [AWK+19 §3.1.3, p. 357–358].
8. **A failed `fsync` is fatal for the affected state.** Do not retry it. Linux marks the dirty pages clean after the failure, so a retry "succeeds" without writing anything. Crash, then recover from the log and from replicas [RPA+20 §1, §3, §5].
9. **Where a file system is used anyway** (metadata DB files, bootstrap config): fsync the parent directory after create or rename. Do not assume appends are atomic. Persist data before renaming it [PCA+14 §2, §4.4].
10. **Checksum end to end with CRC-32C.** The client computes it before sending, the storage node verifies it on ingest, and every read verifies it again [SRC84; SP00 §5; GGL03 §5.2; AWK+19 §5.1]. CRC-32C is the Castagnoli {1,31} polynomial [CBH93; Koo02].
11. **Every block should identify itself.** Store its identity (chunk id, block index, generation) with the data, and keep a second, physically separate copy of identifiers and checksums. This catches misdirected and lost writes that checksums alone miss [BGS+08 §2.2; AGL+18 §3.3.4; GGL03 §5.2] *(the combination is inference)*.
12. **Checksum granularity:** GFS uses 64 KB blocks with 32-bit checksums [GGL03 §5.2]. Checksumming 4 KiB blocks costs 10 GiB of checksum metadata per 10 TiB of data [AWK+19 §5.1]. Read-only data can use larger blocks (BlueStore uses 128 KiB for S3-style objects) [AWK+19 §5.1].
13. **Scrub every device fully at least every 1–2 weeks, in a staggered order** (128 MB regions, 1 MB segments), at low priority [BGPS07 §6; BGS+08 §2.2; SDG10 §5]. Ceph scrubs metadata daily and data weekly [AWK+19 §5.1]. Also verify cold data during idle time [GGL03 §5.2].
14. **After a first error, check the neighbouring blocks immediately and treat the device as higher-risk.** Latent sector errors cluster within 10 MB and arrive within minutes of each other [BGPS07 §5]. Corruptions cluster in consecutive blocks [BGS+08 §4]. An SSD uncorrectable error predicts more of them: about 30% chance of another the next month, against 2% at baseline [SLM16 §5].
15. **Never crash-loop, truncate, or silently return data on corruption.** Tell a torn tail apart from real corruption, and repair from peers [GAA17 §4.2; AGL+18 §3.3.3].
16. **The metadata Raft log should adopt CTRL:** per-entry checksums, identifiers stored apart from the entries, two copies of term/vote, and leader recovery that determines commitment before discarding entries [AGL+18 §3–4].
17. **Compaction (garbage collection):** pick victims by cost-benefit, `(1−u)·age/(1+u)`, sort the surviving data by age, start below a low watermark and stop at a high one, and aim for a bimodal distribution of segment utilization [RO92 §3.4–3.6].
18. **Place data by expected deletion group, and issue discards promptly but batched.** F2FS's delayed discards leave "ghost data" behind and increase the SSD's internal garbage collection [HKA17 Obs. #8, #21].
19. **Design for zones from day one.** Segments are written sequentially only, sized to the zone capacity, and the number of open zones stays within the device's active-zone limit (8–32 expected; 14 on the evaluated SSD). The host does its own garbage collection. Metadata lives in a log, not in fixed locations that are updated in place [BAH+21 §2.3, §3.1, §4.2; AWK+19 §3.3; ATGD17 §6; AD15 §6].
20. **On drive-managed SMR, keep the disk strictly sequential.** Sequential writes of at least 8 MiB bypass the drive's persistent cache. A single 4 KiB random write interrupts that streaming [ATGD17 §4.3.1]. Random-write bursts beyond about 16 GB or 180,000 operations fill the cache and throughput collapses [AD15 §6].

---

## 2. Haas & Leis, PVLDB 2023: NVMe arrays, queue depth, I/O interfaces (request item 1)

**Citation.** Gabriel Haas and Viktor Leis. *What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance I/O for High-Performance Storage Engines.* PVLDB 16(9): 2090–2102, 2023. doi:10.14778/3598581.3598584.

**Setup** [HL23 §4.1, p. 2097]. Linux 5.19; AMD EPYC 7713 (64 cores/128 threads); 512 GB DRAM; 8 × Samsung PM1733 3.84 TB PCIe 4.0 SSDs. The IOMMU was disabled (`amd_iommu=off`) because the SSDs could not reach full performance with it on. SSDs were erased with `blkdiscard` before every run. Caveat: §2 describes the server as "64-core AMD Zen 4", but EPYC 7713 (Milan) is Zen 3. The paper is inconsistent here; we report both statements as written. Workloads: random 4 KB reads, TPC-C, and key-value lookups in LeanStore; RocksDB and WiredTiger as baselines.

### Findings

- **HL23-F1: What the hardware can do.** Eight SSDs deliver "12.5 M IOPS or 1.56 M IOPS per SSD" of random 4 KB reads [§2.1, p. 2091]. Random writes on an *empty* SSD reached 4.7 M IOPS in total, but "the data sheet specifies the worst-case, per-drive random write throughput at 135 k IOPS". Mixed loads: "with 10% (25%) writes we measured 8.9 M (7.0 M) IOPS" [§2.1, p. 2091]. Later, "mixed: 6.7M IOPS vs. read-only: 12.5M IOPS, flash writes are around 10× more expensive than reads" [§4.5, p. 2099].
- **HL23-F2: Full or long-running SSDs are slower.** "performance degrades on full SSDs and with prolonged writing due to internal write amplification. This is caused by the SSD's flash translation layer (FTL) performing garbage collection" [§4.1, p. 2097].
- **HL23-F3: Queue depth.** "around 1000 concurrent I/O requests, i.e., more than 100 per device, are necessary to get decent performance, and 3000 to fully saturate the system" (Fig. 4, SPDK, 8 SSDs, 4 KB random reads) [§2.3, p. 2092]. Flash read latency is about 100 µs, so a synchronous request stream per device "would result in a meager 10k IOPS (or 40 MB/s)" [§2.3, p. 2092].
- **HL23-F4: Page size.** "For data center grade SSDs, we found that the sweet spot for the page size is 4 KB". Random 4 KB reads reach about 6 GB/s: "This is only 8% slower than the maximum bandwidth of 6.5 GB/s that can be achieved with larger pages (or sequential access)" (Fig. 3; the axis scale suggests this is per SSD). "using smaller pages than 4 KB significantly decreases performance" because of FTL overhead and larger internal physical registers. Amplification cost of larger pages: "With 16 KB pages, for example, reading or writing 100 Byte records results in an I/O amplification of 160×" [§2.2, p. 2092]. Conclusion Q3: "The best trade-off between random IOPS, throughput, latency, and I/O amplification is achieved with 4 KB pages" [§6, p. 2101].
- **HL23-F5: CPU budget per I/O.** "a CPU budget of 13k cycles per I/O operation (2.5 GHz × 64 cores / 12 M IOPS)". Unless SPDK is used, "around half of the available CPU cores are consumed by the OS just for submitting and reaping I/O requests", which leaves about 6,500 cycles per I/O for everything else [§2.5, p. 2093].
- **HL23-F6: Interfaces in a microbenchmark** (Fig. 6, no batching). With libaio and interrupt-driven io_uring "the full bandwidth cannot be reached. With 32 threads, the maximum throughput is 10 M IOPS, with most of the time being spent in the kernel". io_uring in poll mode comes close to full throughput with 16 threads. SPDK reaches "the full bandwidth with only three threads". To get these numbers "we had to disable most operating system features, such as the file system, RAID, and the OS page cache" [§2.5, p. 2093].
- **HL23-F7: Blocking I/O does not scale.** With synchronous `pread`, "there must be more than 1000 threads running simultaneously" to keep the SSDs busy. "After starting 500-1000 threads, all CPU cores are running at 100% load, with most time spent in the kernel" [§2.6, §3.2, p. 2094]. Replacing kernel threads with user-space tasks (Boost `fcontext`) means "a task switch costs only around ~20 CPU cycles, instead of several thousand for a kernel context switch" [§3.2, p. 2095]. Removing the oversubscription raised TPC-C throughput by 16% and read-only throughput by 25% [§4.3, p. 2099].
- **HL23-F8: Who performs the I/O.** A single dedicated I/O thread "can only achieve 630k (libaio, io_uring) to 820k (io_uring poll) IOPS". Using io_uring SQPOLL kernel threads as dedicated I/O threads "actually decreased performance and efficiency" because they take cores away from workers. Assigning SSDs to specific threads performed about the same as the all-to-all model (Fig. 11). HL23 adopted all-to-all, where "Every thread can have its own queue pair to every SSD" [§3.4, p. 2096]. Conclusion Q6: "I/O should be performed directly by worker threads" [§6].
- **HL23-F9: Software RAID is a bottleneck.** "the Linux md raid seems to have a hard limit at around 15 GB/s". A custom RAID-0 inside the engine gave almost 2× on read-only lookups [§4.3, p. 2098].
- **HL23-F10: Interfaces in the full engine** [§4.5, p. 2100]. io_uring with default settings (no polling) is "surprisingly slightly slower (≈2% on average) than libaio". With I/O polling it is 5–8% faster than libaio. With few cores, SPDK is 60–80% faster than the kernel interfaces on read-only work, narrowing to about 50% with more cores. Polled I/O needs the NVMe driver to allocate polling queues ("by setting nvme.poll_queue"; the Linux module parameter is spelled `poll_queues`, so the paper's spelling is probably a typo). SQPOLL, including `ATTACH_WQ` and `SQ_AFF`: "it was not possible to get better efficiency in terms of cycles per I/O operation". "A SPDK polling call only takes about 80ns (200 cycles)".
- **HL23-F11: Where the CPU goes.** In TPC-C, I/O submission takes "≈2% vs. 16-19%" of CPU for SPDK versus the kernel interfaces. The authors attribute this mainly to "the inefficient implementation of the I/O path in the Linux kernel (v5.19.0-26)". Submission batching gained 3% and `mitigations=off` gained 4% for the kernel interfaces [§4.6, p. 2100].
- **HL23-F12: Writes hurt read tails.** TPC-C averages 3.5 synchronous reads and 2.8 page writes per transaction; "they could be stalled by write operations, which take significantly longer on flash" [§4.7, p. 2100].
- **HL23-F13: Direct I/O and fsync batching.** "All storage engines are run without kernel page cache (O_DIRECT)" [§4.1, p. 2097]. Even the baseline LeanStore already used "file system bypassing using O_DIRECT, and fsync batching" [§1, p. 2091].
- **HL23-F14: Trade-offs of each interface** [§6, p. 2101]. "Kernel-bypassing is not essential to achieve full bandwidth even with small pages. However, it is more efficient in CPU usage." "io_uring with I/O polling enabled was the only kernel interface that could achieve the full bandwidth of our NVMe array." SPDK "requires root privileges and exclusive access to the whole drive." PCIe 5.0 "might make kernel bypassing more relevant".
- **NOT ADDRESSED:** io_uring registered buffers or fixed files, write-path queue depths (all queue-depth data is for reads), HDD behaviour, and any durability or crash-consistency mechanism (logging was *disabled* in the benchmarks [§4.1]).

### Implications for mantle

- **R1.1 NVMe data path.** Use `O_DIRECT` on raw block devices (or on large preallocated files), 4 KiB-aligned buffers and offsets, and 4 KiB as the smallest I/O unit (HL23-F4, F6, F13). *Inference:* any sub-4 KiB record must be packed into 4 KiB-aligned pages instead of being written on its own.
- **R1.2 Outstanding I/Os per NVMe device.** Aim for at least 128 in flight per device under load, and 256–512 on arrays expected to run at their limit (HL23-F3: more than 100 per device for "decent", about 375 per device (3000 / 8) to saturate). Queue depth should be a per-device-class runtime setting, not a constant.
- **R1.3 Concurrency model.** Thread-per-core workers, each running many cooperative async tasks. Each core owns its own submission path to every device (all-to-all, one io_uring per core; no dedicated I/O threads, no cross-thread message passing) (HL23-F7, F8). *Inference:* in Rust this means a thread-per-core executor whose run loop alternates between running tasks, submitting I/O, and reaping completions. Do not run blocking `pread`/`pwrite` in a thread pool on the NVMe path.
- **R1.4 Interface choice.** Default: io_uring with IOPOLL on dedicated NVMe poll queues. Fallback: io_uring interrupt mode, which is about equal to libaio. SQPOLL is off by default (HL23-F8, F10, F14). Keep an SPDK/vfio backend as a later option for nodes that are short on CPU; it needs exclusive device ownership (HL23-F14).
- **R1.5 No md RAID.** Stripe across devices inside mantle, and let placement and erasure coding handle redundancy (HL23-F9).
- **R1.6 CPU budget.** At about 1–1.5 M IOPS per device, the whole per-I/O path (checksumming, indexing, networking) has only a few thousand cycles. Checksumming must be SIMD- or hardware-accelerated, and allocation-free on the hot path (HL23-F5, F11).
- **R1.7 Separate reads from writes.** Writes inflate read tail latency (HL23-F12). *Inference:* limit or schedule background writes (compaction, re-replication) per device against a read-latency target.
- **R1.8 Benchmark honestly.** Precondition SSDs to steady state. HL23 erased drives before each run and warns that full, long-running drives are slower (HL23-F2). Treat HL23's write numbers as upper bounds.

---

## 3. Didona et al., SYSTOR 2022: libaio vs io_uring vs SPDK (request item 2)

**Citation.** Diego Didona, Jonas Pfefferle, Nikolas Ioannou, Bernard Metzler, Animesh Trivedi. *Understanding Modern Storage APIs: A Systematic Study of libaio, SPDK, and io_uring.* SYSTOR '22, Haifa, 2022. doi:10.1145/3534056.3534945. The printed page range is not in the PDF, so page references are to the PDF.

**Setup** [Table 1, PDF p. 2]. 2 × Intel Xeon E5-2630 (10 cores per socket, 2.2 GHz); 20 × Intel DC P3600 400 GB NVMe (rated 320K IOPS random read, 30K random write); Linux 5.13; fio 3.28. Workload: "random data reads at the granularity of 4KiB using unbuffered I/O", queue depth (QD) 1–128, on raw block devices. The three io_uring modes studied are:
- **iou**: `io_uring_enter` with interrupts;
- **iou+p**: completion polling (IOPOLL, fio `hipri`);
- **iou+k**: kernel submission-queue poller thread plus completion polling (SQPOLL, fio `sqthread_poll`), which makes no system calls per I/O.

### Findings

- **DPI+22-F1: One drive with one or two cores** [§3.1, PDF p. 4]. "With just one core, SPDK achieves 305 KIOPS versus the 171 KIOPS and 145 KIOPS of the best io_uring alternative and libaio". With two cores, "SPDK achieves 313 KIOPS, vs the 260 KIOPS and 150 KIOPS of iou+k and libaio". "SPDK is the only library capable of saturating the bandwidth of the drive, while all other approaches are CPU-bound."
- **DPI+22-F2: SQPOLL on a single core collapses.** iou+k "suffers a catastrophic performance loss delivering only 13 KIOPS". The kernel poller uses about 50% of the CPU and "The median latency of iou+k is 8 msec" [§3.1, PDF p. 4]. With a second core it recovers to within 18% of SPDK's peak.
- **DPI+22-F3: Polling matters at high load.** iou+p "achieves performance that is comparable with SPDK for low to medium throughput values (up to ≈ 150 KIOPS)"; above that, system-call overhead becomes the bottleneck. Up to QD 16, iou and libaio are close ("79 KIOPS and 72 KIOPS"; median latency 185 vs 190 µs). Beyond that iou peaks higher: "182 KIOPS of peak throughput versus 151 KIOPS on two cores". iou+p shows "a cache miss rate of 5% versus 0.6% for SPDK" (QD 16, two cores) [§3.1, PDF p. 4]. System calls per I/O converge to about 1 for both iou and iou+p at high QD (Fig. 3).
- **DPI+22-F4: SQPOLL needs about two cores per device** [§3.2, PDF p. 5]. "iou+k, in particular, needs twice as many CPUs as drives to achieve the highest throughput." With five jobs, one per drive, iou+k with a dedicated core per poller is about 15% below SPDK, about 45% above iou/iou+p, and about 80% above libaio. With only two extra cores it is about 20% *below* libaio, and with no extra cores "iou+k's throughput [plummets] to less than half the throughput of libaio".
- **DPI+22-F5: Scaling to 20 drives** [§3.3, PDF p. 6] (C = 2J cores, capped at the 20 physical cores). For J ≤ 10 jobs (one per drive), iou+k is 9–16% below SPDK, 27–45% above iou/iou+p, and "between 50% and 76% higher than libaio". Once cores are oversubscribed, "With J = 14, iou+k becomes the worst performing library". At J = 20 it reaches under a third of SPDK. From J = 14 to 20, iou and iou+p are 33% below SPDK, and "libaio achieves throughput that is only 10% lower than iou+p and iou".
- **DPI+22-F6: The paper's lessons** [§4, PDF p. 6]. Lesson 1, "Not all polling methods are created equal." Lesson 2: "Our results recommend using twice as many CPU cores as the number of drives" (for iou+k). Lesson 3: "In our largest experiment (20 drives), SPDK outperforms the second best approach (iou+p) in throughput by as much as 50%"; iou+k "can deliver performance within 90% of SPDK, but it utilizes twice as many cores (20 vs 10)". Also: "libaio only supports unbuffered accesses (i.e., with O_DIRECT)" [§2, PDF p. 2], while io_uring supports both direct and buffered I/O.
- **When is a thread pool doing blocking I/O competitive? NOT ADDRESSED** by DPI+22, which did not benchmark synchronous `pread`/`pwrite`. The only peer-reviewed evidence in this review is HL23-F7, which says a thread pool is *not* competitive at NVMe-array scale. *Inference:* a small blocking thread pool may be enough when a device needs only a few outstanding requests, such as an HDD at NCQ depth ≤ 32 [HKA17 §3.1]. That remains **UNVERIFIED** in this literature set.
- **Caveats.** fio on raw devices, reads only, older CPUs, kernel 5.13, and 2015-era P3600 drives that one SPDK core can saturate. The absolute numbers will not carry over; the qualitative lessons should (they agree with HL23-F8 and F10).

### Implications for mantle

- **R2.1** Do not enable SQPOLL unless the node can dedicate about one extra core per device. Choose it per node from the core-to-device ratio, not as a global default (DPI+22-F2, F4, F5).
- **R2.2** Enable completion polling (IOPOLL) for NVMe. Its benefit shows up at medium and high load (DPI+22-F3; HL23-F10).
- **R2.3** On nodes where cores are scarce relative to drives (dense JBOF-style nodes), use kernel paths with polling. Revisit SPDK only if profiling shows submission overhead dominating (DPI+22-F5, F6; HL23-F11).
- **R2.4** Microbenchmark each hardware SKU at QD 1–128 before fixing per-device queue depths; the "hook" curves show latency rising past saturation (DPI+22 Fig. 2).

---

## 4. He, Kannan, Arpaci-Dusseau & Arpaci-Dusseau, EuroSys 2017: the unwritten contract of SSDs (request item 3)

**Citation.** Jun He, Sudarsun Kannan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. *The Unwritten Contract of Solid State Drives.* EuroSys '17, pp. 127–144, 2017. doi:10.1145/3064176.3064187.

**Setup** [§4]. Block traces from LevelDB 1.18, RocksDB 4.11.2, SQLite 3.8.2 (rollback-journal and WAL modes), and Varmail, running on ext4, XFS and F2FS (Linux 4.5.4) with discard enabled. Traces were collected on a SATA SSD with NCQ depth 32 ("We use a 1-GB partition of the 480 GB available space") and replayed in WiscSim, the authors' discrete-event SSD simulator (supporting NCQ, page-level/DFTL and hybrid FTLs, GC and wear-levelling). **Caveat:** most results are *simulated internal metrics*: miss-ratio curves, unaligned ratios, "zombie curves" (the sorted valid-page ratios of flash blocks), and write counts. They are not end-to-end throughput.

### The five rules (§3, pp. 129–131)

| Rule | Statement (verbatim) | SSD-internal reason | Affects |
|---|---|---|---|
| Request Scale | "SSD clients should issue large data requests or multiple concurrent requests" [§3.1] | internal parallelism across channels and dies; NCQ ("a typical maximum queue depth of modern SATA SSDs is 32 requests") | immediate and sustained performance |
| Locality | "SSD clients should access with locality" [§3.2] | the mapping-table cache in on-demand FTLs ("With a page size of 2 KB, a 512-GB SSD would require 2 GB of RAM" for a full page map) | immediate and sustained |
| Aligned Sequentiality | clients "should start writing at the aligned beginning of a block boundary and write sequentially" [§3.3] | hybrid FTLs cheaply convert aligned, sequential page mappings to block mappings; unaligned data needs a read–reorder–rewrite "merge" | sustained only |
| Grouping by Death Time | "SSD clients should group writes by the likely death time of data" [§1]; done "by order" (write together) or "by space" (logical segments) [§3.4] | garbage collection must copy the live data out of "zombie" blocks, those holding both live and dead pages | sustained only |
| Uniform Data Lifetime | "clients of SSDs should create data with similar lifetimes" [§3.5] | static wear-levelling copies long-lived data; P/E endurance "is on the order of 10³" | sustained only |

Background facts: flash blocks are "typically hundreds of KBs (e.g., 128 KB), or much larger (e.g., 4 MB)" and pages are 2–16 KB [§2]. The size of the penalty for breaking these rules comes from prior literature summarized in Table 2 [p. 131]: request scale up to 7.2×/18× in read bandwidth and 10×/4× in write bandwidth; locality 1.6–2.2× in response time; aligned sequentiality 2.5× in execution time and 2.4× in erase count; death-time grouping 4.8× in write bandwidth, 1.6× in throughput, 1.8× in erase count; lifetime uniformity 1.6× in write latency. §3.4: "The advice of separating hot and cold data is inaccurate and misleading", because two pieces of data can have the same lifetime but die at very different times. §3.6: "Generally, a client should not choose an SSD with rules that the client violates."

### Findings (the paper's numbered observations that matter for mantle)

- **HKA17-F1 (Obs. #1): log structure makes writes large.** "Log structure increases the scale of write size for applications" [§5.1, p. 133].
- **HKA17-F2 (Obs. #2): reads stay small.** "The scale of read requests is often low", because dependent reads (index before data) cannot be batched [§5.1].
- **HKA17-F3 (Obs. #4 and #6): barriers limit request scale.** "Frequent data barriers in applications limit request scale". Even with about 2 MB written between barriers in LevelDB/RocksDB, "the write and read barriers frequently drain the NCQ depth to 0" [§5.1, p. 134]. File-system journaling and F2FS checkpoints (triggered by directory fsyncs) add barriers of their own [Obs. #6].
- **HKA17-F4 (Obs. #5): buffered I/O limits request scale.** Buffered `read()` issues requests of `read_ahead_kb` "(default: 128) KB" one at a time, and a single request to the block layer is capped at "a hard-coded size (2 MB)". "In contrast to buffered reads, direct I/O produces much larger request scale" [§5.1, p. 134]. The block layer split requests larger than "1280 KB in our case" [Fig. 3 caption].
- **HKA17-F5 (Obs. #8, #21): discards should not be deferred indefinitely.** ext4 and XFS discard immediately; "F2FS attempts to delay and merge discard operations". That helps immediate performance but produces ghost data, so "F2FS sacrifices too much sustainable performance for immediate performance" [§5.1, §5.4, pp. 135, 138].
- **HKA17-F6 (Obs. #10): reusing freed space quickly helps locality.** "XFS achieves the best locality on all workloads" because it aggressively reuses space from deleted files [§5.2, p. 135].
- **HKA17-F7 (Obs. #12, #16): small flushes make log-structured systems overwrite in place.** F2FS switches to in-place updates when "flush size must be less than 32 KB" and data is being overwritten. Appending to a partially filled 4 KB sector counts as an overwrite ("partial sector use"). Result: "sequential + sequential ≠ sequential" [§5.2–5.3, pp. 136–137].
- **HKA17-F8 (Obs. #14, #15): log structure does not guarantee alignment.** "Application log structuring does not guarantee alignment", because file systems reuse freed space partially. "Log-structured file systems may not be as sequential as commonly expected" [§5.3, p. 136].
- **HKA17-F9 (Obs. #17): log structure does not reduce GC by itself.** "Application log structuring does not reduce garbage collection". LSM files die at unpredictable times, and each 2 MB file is striped across channels: "Our 128-KB block in the simulation may mix data from two files", and larger modern blocks would mix more [§5.4, pp. 137–138].
- **HKA17-F10 (Obs. #18–20): file systems undo the application's separation.** "Applications often separate data of different death time and file systems mix them". Segmented FTLs help ("suggesting that FTLs should always be segmented"). "All file systems fail to group data from different directories" [§5.4, p. 138].
- **HKA17-F11 (Obs. #22–24): journals and superblocks are written far more often than data.** "Application and file system data lifetimes differ significantly". For example, a journal superblock "is written 2600 times more than average Varmail data". "In-place-update file systems preserve data lifetime of applications" [§5.5, p. 139].
- **HKA17-F12: the terms "random" and "sequential" mislead.** "We advocate an optimistic view for random writes": random writes are fine if the data dies together. "We advocate dropping the terms 'random write' and 'sequential write'" and using death time and zombie curves instead [§5.6, p. 140]. Also: "Being friendly to one rule is not enough" [§5.6, p. 139].

### Implications for mantle

- **R3.1 Write in large units that die together.** Allocate chunk data in large extents written sequentially from an aligned start. *Inference:* size each extent at many multiples of (flash block size × channel striping), so that deleting a whole extent frees whole erase blocks. HKA17-F9 shows that 2 MB files already mix inside 128 KB blocks.
- **R3.2 Group by death time, not by "hotness".** Put data that will be deleted together in the same extent, stream, or zone. *Inference:* candidates for mantle are the same bucket-lifecycle/TTL class, the same tenant, the same write epoch, and the same EC stripe or chunk generation. Never interleave short-lived data (journals, temporary replicas, re-replication staging) with long-lived chunk data in one extent (HKA17 §3.4, F10, F11).
- **R3.3 Keep journals and superblocks apart from bulk data.** Put them on dedicated extents, preferably a separate logical region per device, so their very high write counts do not skew wear or mix with bulk data (HKA17-F11).
- **R3.4 Fewer barriers, more concurrency.** Keep many independent write streams in flight and use group commit, so that `fsync` barriers do not drain the device queue (HKA17-F3; §13 of this document).
- **R3.5 Direct I/O for reads as well** (HKA17-F4; HL23).
- **R3.6 Discard freed extents promptly, but in extent-sized batches.** This avoids both deferred discards that leave ghost data and a flood of tiny discards (HKA17-F5). *Inference:* issue discards when a whole extent or zone is reclaimed.
- **R3.7 Never let small appends rewrite partial pages.** Pad each record group to a 4 KiB boundary rather than rewriting a partial page (HKA17-F7).

---

## 5. Aghayev et al., SOSP 2019: ten years of Ceph and why file systems were a poor backend (request item 4)

**Citation.** Abutalib Aghayev, Sage Weil, Michael Kuchnik, Mark Nelson, Gregory R. Ganger, George Amvrosiadis. *File Systems Unfit as Distributed Storage Backends: Lessons from 10 Years of Ceph Evolution.* SOSP '19, pp. 353–369, 2019. doi:10.1145/3341301.3359656.

**Setup** [§6, p. 362]. A 16-node cluster. Each node: 16-core Xeon E5-2698Bv3, 64 GiB RAM, 400 GB Intel P3600 NVMe, 4 TB 7200 RPM Seagate ST4000NM0023 HDD, 40 GbE; Linux 4.15; Ceph Luminous v12.2.11 with default configuration. BlueStore "is adopted by 70% of users in production" [abstract].

### Why file systems failed as a backend

- **AWK+19-F1: transactions are hard to build on a file system** [§3.1].
  - Btrfs's user-visible transactions had no rollback: a crash committed half a transaction [§3.1.1].
  - A user-space WAL on XFS causes three problems [§3.1.2]. (a) Slow read-modify-write: every such operation "incurred the full latency of the WAL commit"; the full-data journal "capped the speed of read-modify-write workloads" to the WAL's write speed. (b) Replaying non-idempotent operations after a crash corrupts data; FileStore added sequence-number guards, but the code "ended up fragile and hard-to-maintain". (c) Double writes, "halving the disk bandwidth". Also, `sync` "is too expensive because it synchronizes all file systems on all drives", which led to the `syncfs` system call being added [§3.1.2].
  - RocksDB as the WAL on top of a journaling file system (NewStore): "each fsync issues two flush commands", so an object creation "results in four expensive flush commands to disk". Emulating a raw-disk design (2 flushes) made object creation "80% higher on a raw HDD" and 70% higher on a raw NVMe SSD (Fig. 3) [§3.1.3, pp. 357–358].
- **AWK+19-F2: metadata performance at scale** [§3.2]. Enumerating large directories is slow and unordered. FileStore splits directories to keep them at a few hundred entries, and when many OSDs split at once it "kills the throughput for 7 minutes on an all-SSD and 120 minutes on an all-HDD cluster" (Fig. 4, 4 KiB objects, QD 128) [p. 358].
- **AWK+19-F3: new hardware is slow to arrive in file systems** [§3.3]. The host-managed SMR zone interface presents "a sequence of 256 MiB regions that must be written sequentially", which forces a log-structured, copy-on-write design. Attempts to make XFS/ext4 zone-capable "have so far been unsuccessful". ZNS SSDs remove the FTL and cut over-provisioning and DRAM [pp. 358–359].
- **AWK+19-F4: the page cache makes latency unpredictable** [§3.4]. Writeback on busy systems is triggered by complex policies at arbitrary times. "Even with a periodic use of fsync, FileStore has been unable to bound the amount of deferred inode metadata write-back" [p. 359].

### BlueStore's design

- **AWK+19-F5: layout** [§4, Fig. 5]. BlueStore runs on raw disks. An allocator places data, "which is asynchronously written to disk using direct I/O". All metadata, including allocation metadata, lives in RocksDB, which runs on BlueFS, a minimal user-space file system. In BlueFS, "The journal has the only copy of all file system metadata", and the journal has no fixed location [§4.1, p. 360].
- **AWK+19-F6: one flush each for data and metadata.** Data goes straight to the raw disk, "resulting in one cache flush for data write", and RocksDB was changed to "reuse WAL files as a circular buffer", giving one flush per metadata write [§4.1, p. 360]. The emulated setup in F1 used "a preallocated pool of WAL files" [§3.1.3].
- **AWK+19-F7: copy-on-write for large writes, deferred writes for small ones** [§4.2, p. 360]. Writes larger than the "minimum allocation size (64 KiB for HDDs, 16 KiB for SSDs)" go to a newly allocated extent, and the metadata is committed afterwards. That gives cheap clones and no journal double-write. Smaller writes put data and metadata into RocksDB as "promises of future I/O" and are applied later: "new data writes require two I/O operations whereas an insert to RocksDB requires one". Overwrites of 64 KiB or less are applied in place on HDD, while "in-place overwrites only happen for I/O sizes less than 16 KiB on SSDs".
- **AWK+19-F8: allocation** [§4.2, pp. 360–361]. The free list is a bitmap in RocksDB, updated with RocksDB's merge operator so updates need no ordering. The allocator is a hierarchy of indexes over one bit per block, with "a fixed memory usage of 35 MiB per terabyte of capacity". An earlier power-of-two bin allocator fragmented as the disk filled.
- **AWK+19-F9: checksums** [§5.1, p. 361]. "Ceph scrubs metadata every day and data every week." "checksums are indispensable for distributed storage systems … where bit flips are almost certain to occur." 32-bit checksums over 4 KiB blocks "results in 10 GiB of checksum metadata" per 10 TiB, which is hard to cache. "BlueStore computes a checksum for every write and verifies the checksum on every read." "crc32c is used by default because it is well-optimized on both x86 and ARM architectures, and it is sufficient for detecting random bit errors." The checksum block size is chosen from I/O hints: for read-only RGW (S3) objects "the checksum can be computed over 128 KiB blocks"; for compressed objects it is computed after compression. **The default checksum block size is not stated in the paper: UNVERIFIED.**
- **AWK+19-F10: its own cache, no readahead.** BlueStore "implements its own write-through cache in user space, using the scan resistant 2Q algorithm", sharded like the OSD [§4.2, p. 361]. "BlueStore does not implement read-ahead on purpose" [§6.1, p. 362]. The cache "is a fixed configuration parameter that requires manual tuning"; that is an open problem [§7.1, p. 364].
- **AWK+19-F11: erasure-coded overwrites** [§5.2, §6.3]. Overwrites of EC data use two-phase commit to avoid the RAID write hole. Copy-on-write makes the rollback copy cheap: "6× more IOPS on EC4-2, and 8× more IOPS on EC5-1" than FileStore [p. 363].
- **AWK+19-F12: results** [§6]. Steady-state RADOS writes are "50-100% greater than FileStore", with "an order of magnitude lower tail latency" (Figs. 7–8, all-HDD). Metadata-heavy 4 KiB object writes run 2× faster on SSD and 3× faster on HDD, with no directory-split collapse, although RocksDB compaction still wears throughput down (Fig. 9). For RBD with I/O larger than 512 KiB, sequential and random writes are 1.7× and 2× faster; below 64 KiB (deferred writes) BlueStore is 20% faster [§6.2, p. 363].
- **AWK+19-F13: problems that remain** [§7]. "RocksDB's compaction and high write amplification have been the primary performance limiters when using NVMe SSDs", along with serialization CPU cost and RocksDB's own threading model [§7.2]. On-disk metadata was shrunk with "delta and variable-integer encoding" [§7.3]. RocksDB/BlueFS were ported to host-managed SMR [§5.4, p. 362].

### Implications for mantle

- **R4.1** Store data on raw devices, and store per-device metadata (extent map, free space, chunk index) in an embedded ordered KV/log that is itself on the raw device or on a preallocated region. Avoid what the paper calls the "journaling of journal" problem (AWK+19-F1, F5, F6).
- **R4.2 Flush budget.** A new chunk write should cost exactly one data flush plus one metadata-log flush that is amortized by group commit. Write data first and commit the reference second. Never run a WAL on top of a journaling file system (AWK+19-F1, F6).
- **R4.3 Small-write threshold.** Writes below a per-device-class threshold go into the log (deferred) and are applied later. Larger writes go to fresh extents. Start with BlueStore's values as defaults, 64 KiB for HDD and 16 KiB for SSD, and validate them (AWK+19-F7). *Inference:* in an append-mostly object store, small objects should be packed into larger log-structured extents rather than deferred and applied in place.
- **R4.4 Crash replay must be idempotent by construction.** Resolve every read dependency when a transaction is prepared, and have the log describe the *result*, not the operation. FileStore's guard-based replay "ended up fragile" (AWK+19-F1).
- **R4.5 Keep allocator metadata bounded and compact:** a bitmap plus a summary hierarchy, with memory fixed per TB (AWK+19-F8).
- **R4.6 Checksums:** CRC-32C on every write, verified on every read; block size depends on the data class, with larger blocks for immutable objects (AWK+19-F9). See §14 for sizing.
- **R4.7 Scrub cadence floor:** metadata daily, data weekly, as in AWK+19-F9 (and see §10).
- **R4.8 Budget for the metadata store's compaction.** Its write amplification will dominate on NVMe (AWK+19-F13). *Inference:* keep per-chunk metadata small (varint/delta encoded) and avoid a per-4 KiB metadata record for large objects.

---

## 6. Pillai et al., OSDI 2014: crash consistency on real file systems (request item 5), plus Rebello et al., ATC 2020 (supplementary)

**Citation.** Thanumalayan Sankaranarayana Pillai, Vijay Chidambaram, Ramnatthan Alagappan, Samer Al-Kiswany, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. *All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications.* OSDI '14, pp. 433–448, 2014.

**Setup.** Two tools:
- **BOB (Block Order Breaker)** records the block I/O a file system issues, reorders it while respecting barriers, and checks which persistence properties break across 16 configurations of ext2, ext3, ext4, btrfs, xfs and reiserfs [§2.2].
- **ALICE** turns an application's system-call trace into micro-operations under an abstract persistence model and explores the resulting crash states [§3]. It was run on 11 applications: LevelDB (1.10 and 1.15), GDBM, LMDB, SQLite, PostgreSQL, HSQLDB, Git, Mercurial, HDFS, ZooKeeper and VMWare Player [§4].

### What file systems actually guarantee (BOB, §2.2, Table 1, p. 436)

- **PCA+14-F1: atomicity.**
  - "all tested file systems seemingly provide atomic single-sector overwrites", but only because the disk writes sectors atomically. Byte-atomic media such as PCM would break this.
  - Atomic appends (inode size and data updated together) are "not provided by ext2 or writeback configurations of ext3, ext4, and reiserfs".
  - "Current file systems do not provide atomic multi-block appends"; at best "some prefix" of the data is appended atomically.
  - Directory operations (rename, link) "are seemingly atomic on all file systems that use techniques like journaling or copy-on-write for consistency".
  - The introduction adds that "new storage technology may be atomic only at a smaller granularity than 512-bytes" [§1, p. 434].
- **PCA+14-F2: ordering.** Only ext3/ext4/reiserfs in data-journaling mode and ext2 in sync mode "persist all tested operations in order". With delayed allocation, an append can be persisted *after* later operations. File systems special-case the append-then-rename idiom and `O_TRUNC` appends. Successive appends to the same file stay in order. "Linux ext2 and btrfs freely reorder directory operations" [§2.2.2, p. 436].

### What applications get wrong (ALICE, §4, pp. 440–445)

- **PCA+14-F3: overall count.** ALICE found "a total of 60 vulnerabilities" (156 dynamic); applications failed in "more than 4000 crash states". The consequences: "5 resulting in silent failures, 12 in loss of durability, 25 leading to inaccessible applications, and 17 returning errors" [§4.3, pp. 442–443]. "7 of the 11 tested applications have trouble properly recovering" when writes are reordered, and "10 of the 11 applications expect atomicity of filesystem updates" [§1, p. 434]. About half of the vulnerabilities show up on the file systems in use at the time [§1]. Counts per file system (Table 3c): ext3-writeback 16, ext3-ordered 12, ext3-data-journal 10, ext4-ordered 17, btrfs 31 [p. 443].
- **PCA+14-F4: appends are assumed to be atomic.** "A crash can result in the appended portion of the file containing garbage", and LevelDB's recovery did not handle it [§4.2.1, p. 442]. "three applications require appends to be content-atomic". "Filling the appended portion with zeros instead of garbage still causes failure" [§4.4.2, p. 444]. LMDB, PostgreSQL and ZooKeeper "require small writes (< 200 bytes) to be atomic" [§4.4.2].
- **PCA+14-F5: operations are assumed to reach disk in order.** "we find 27 vulnerabilities" of this kind [§4.4.3, p. 444].
- **PCA+14-F6: directory fsync is often missing.** "An fsync() on a file does not guarantee that the file's directory entry is also persisted" [§4.4.3]. "Six applications require fsync() calls on directories" [§4.4.4]. Of the five vulnerabilities that developers acted on, "three relate to not explicitly issuing an fsync() on the parent directory" [§4.3, p. 443]. For example, "LevelDB does not explicitly persist the directory entries of ldb files" (the files vanish after a crash) and ZooKeeper does not persist the directory entries of its log files [§4.2, p. 442].
- **PCA+14-F7: a "safe rename" heuristic is not enough.** File systems that persist a file's data before a later rename of that file "only matches (and thus fixes) three discovered vulnerabilities" [§4.4.3, p. 444].
- **PCA+14-F8: fsync on one file can pay for another.** On ext3-ordered, after writing 250 MB to file A, appending one byte to file B and calling `fsync(B)` "takes about four seconds". With A and B on different partitions of the same disk, "the fsync() takes only 40 ms" [§4.6, p. 445].
- **PCA+14-F9: recovery code is rarely exercised.** Recovery code is "infrequently executed and insufficiently tested" [§4.7]. Developers are also "suspicious that the underlying storage stack might not respect fsync()" [§4.7, p. 445].

### Supplementary: Rebello et al., USENIX ATC 2020 (fsync *failure*)

**Citation.** Anthony Rebello, Yuvraj Patel, Ramnatthan Alagappan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. *Can Applications Recover from fsync Failures?* USENIX ATC '20, pp. 753–767, 2020.

- **RPA+20-F1: failed pages are marked clean.** On ext4, XFS and Btrfs, "all three file systems mark pages clean after fsync fails", which "render[s] techniques such as application-level retry ineffective" [§1, p. 753]. On ext4, "future calls to fsync never retry previous data writes that may have failed" [§3, p. 757].
- **RPA+20-F2: some failures take down the whole file system.** "Failed updates to some structures (e.g., journal blocks) during fsync reliably lead to file-system unavailability" [§1, p. 753].
- **RPA+20-F3: PostgreSQL's experience.** "PostgreSQL had been using fsync incorrectly for 20 years". It now "respond[s] to the fsync error by crashing and restarting without retrying the fsync" and rebuilds from its WAL [§2, p. 754].
- **RPA+20-F4: the paper's lessons.** "Ext4 data mode provides a false sense of durability". "Copy-on-Write file systems such as Btrfs handle fsync failures better than existing journaling file systems like ext4 and XFS". "the approach currently taken by PostgreSQL to use direct IO may best handle fsync failures" [§5, p. 763]. Even with `O_DIRECT`, "Calls to fsync are still required since data may be cached within the underlying storage media" [§2, p. 755].

### Implications for mantle

- **R5.1 Keep the durability logic inside mantle.** On the chunk-store data path, do not depend on file-system persistence properties at all. Use raw devices or preallocated files, with checksummed, self-delimiting records (PCA+14-F1–F6; AWK+19).
- **R5.2 Assume appends are neither atomic nor ordered.** Every record carries a length, a sequence number or LSN, and a CRC-32C. Recovery scans forward and stops at the first record that fails validation, and only if that record is in the *unacknowledged tail* (see §8 on telling crash from corruption). Recovery must handle garbage and zeros after the valid tail (PCA+14-F4).
- **R5.3 Where files must be used** (bootstrap config, the metadata DB's files if it runs on a file system), use this protocol. *Inference, built from PCA+14-F6/F7 and matching the protocol AGL+18 §3.3.3 relies on for snapshots:* write a temporary file → `fsync(temp)` → `rename(temp, final)` → `fsync(parent directory)`. For new log segments: create → `fsync(file)` → `fsync(parent directory)` before the first acknowledgement that depends on that segment.
- **R5.4 A failed fsync is fatal for the affected device or volume.** Stop acknowledging, fail the in-flight group commit, restart the store process for that device, recover from mantle's own log, and repair from replicas. Never retry `fsync` (RPA+20-F1, F3). Report the error to the metadata service so it can re-replicate.
- **R5.5 Isolate flush domains.** Give each device its own log and its own group-commit loop, so one device's flush never waits on unrelated data (PCA+14-F8; AWK+19 §3.1.2 on `sync` vs `syncfs`).
- **R5.6 Test crash consistency.** Build an ALICE/BOB-style harness into CI: record the I/O stream, generate crash states by prefixes and reorderings between barriers, and check recovery invariants (PCA+14-F9).

---

## 7. Ganesan et al., FAST 2017: how distributed stores react to a single local fault (request item 6)

**Citation.** Aishwarya Ganesan, Ramnatthan Alagappan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. *Redundancy Does Not Imply Fault Tolerance: Analysis of Distributed Storage Reactions to Single Errors and Corruptions.* FAST '17, pp. 149–166, 2017.

**Fault model** [§3.1, p. 151]. "exactly a single fault to a single file-system block in a single node at a time". Faults are injected only into application-level on-disk structures, "not file-system metadata". They are: corruption on read (block replaced with zeros or junk), `EIO` on read, `EIO` on write, and `ENOSPC`/`EDQUOT` on writes that allocate. Injection used errfs, a FUSE file system that is part of the CORDS framework [§3.2]. Systems tested: Redis 3.0.4, ZooKeeper 3.4.8, Cassandra 3.7, Kafka 0.9, RethinkDB 2.3.4, MongoDB 3.2.0, LogCabin 1.0 and CockroachDB beta-20160714. All ran as 3 nodes with replication factor 3, "we enabled checksums, synchronous replication, and synchronous disk writes" [§4, p. 153].

### Findings

- **GAA17-F1: the headline.** "a single file-system fault can induce catastrophic outcomes in most modern distributed storage systems" [§1, p. 149].
- **GAA17-F2: examples by system** [§4.1].
  - Redis: "does not use checksums for aof user data", and resynchronization spreads a corrupted leader's data to its followers.
  - Cassandra: checksums are verified only when compression is on. Read repair picks the lexically greater value, so "the corrupted value is returned to the user and the corruption is propagated to other intact replicas" [p. 155].
  - Kafka: a corrupted leader log entry makes the leader drop it and all later entries; the followers then "hit a fatal assertion and simply crash", which loses data and blocks writes [p. 155].
  - RethinkDB: "RethinkDB silently returns corrupted data" [p. 156].
  - ZooKeeper: a write error during log initialization partially crashes the leader, leaving the cluster write-unavailable.
  - MongoDB and LogCabin: they recover some log corruptions by discarding the entry *and everything after it* and re-fetching from the leader.
- **GAA17-F3 (Obs. #1): integrity strategies vary widely.** Some systems checksum everything and others trust the lower layers. "ZooKeeper uses Adler32", and the authors produced checksum collisions that made ZooKeeper serve corrupted data [§4.2, p. 157].
- **GAA17-F4 (Obs. #2): the usual reaction is to crash.** Faults are often not detected, and when they are, "crashing is the most common local reaction". A sticky fault then crashes the node again on every restart. "We observe that failed operations are rarely retried" [§4.2, pp. 157–158].
- **GAA17-F5 (Obs. #3): replicas are rarely used for repair.** "Redundancy is underutilized: A single fault can have disastrous cluster-wide effects". A small fault can make a large amount of data unreachable, such as a whole Redis dataset, a whole Cassandra table, or a whole Kafka log (Table 3) [§4.2, p. 158].
- **GAA17-F6 (Obs. #4): crash recovery and corruption recovery are mixed up.** "Crash and corruption handling are entangled". Systems treat a checksum mismatch as a torn write. RethinkDB "rolls back the committed and already-acknowledged transaction, leading to a data loss", and Kafka truncates everything after the bad entry [§4.2, pp. 158–159]. LogCabin's developers said "it is hard to distinguish a partial write from corruption in open segments" [§4.4, p. 160].
- **GAA17-F7 (Obs. #5): replication protocols can spread damage.** "Nuances in commonly used distributed protocols can spread corruption or data loss": Kafka's leader election considers only the in-sync replica set, Cassandra's read repair, and Redis's resynchronization [§4.2, p. 159].
- **GAA17-F8: checksumming file systems change the failure.** On btrfs and ZFS a corrupted block is returned as an error instead, but "applications crash more often due to errors than corruptions" [§4.3, p. 159].
- **GAA17-F9: the paper's recommendations** [§4.4, pp. 159–160].
  - On-disk structures must be designed so that corrupted or unreadable parts can be identified.
  - "corruption recovery has to be decoupled from crash recovery".
  - When no intact replica is reachable, "the outcome should be defined by design rather than left as an implementation detail".
  - "future distributed systems need to rigorously test failure recovery code using fault injection frameworks".

### Implications for mantle

- **R6.1 Handle EIO and corruption the same way.** Treat `EIO` and a checksum mismatch as *the same event*: this block is bad. Mark it, serve the read from another replica or EC fragment, rebuild the local copy, and never crash the node on a data-block fault (GAA17-F4, F8). *Inference:* reserve process crashes for violated invariants in mantle's own metadata that cannot be recovered locally, and even then fence only the affected device, not the whole node.
- **R6.2 Repair precisely, not by truncation.** Identify the exact bad blocks and re-fetch only those; do not truncate everything that follows. This requires self-identifying records and an out-of-band index (GAA17-F5, F6, F9; §8 and §10 below).
- **R6.3 Never propagate an unverified replica.** Repair, re-replication and EC reconstruction read only checksum-verified sources. A replica that fails verification is never a donor (GAA17-F7). *Inference:* resolve conflicts between replicas by (chunk id, generation, checksum), never by a heuristic such as latest-timestamp-wins or lexically-greater-value-wins.
- **R6.4 Use strong checksums**, CRC-32C or better (GAA17-F3 rules out Adler-32 for small records; see §14).
- **R6.5 Handle ENOSPC deliberately.** Treat it as a planned state (stop accepting new chunks, keep serving reads), and never retry it in a loop (GAA17-F4: ZooKeeper's blind retry on a space error ran out of file descriptors).
- **R6.6 Fault-injection testing.** Include a CORDS-style harness that corrupts, errors, and fills each on-disk structure (chunk data, chunk index, journal, superblock) on a leader, a follower and each EC member, and checks the global outcome (GAA17-F9).

---

## 8. Alagappan et al., FAST 2018: protocol-aware recovery for replicated state machines (request item 7)

**Citation.** Ramnatthan Alagappan, Aishwarya Ganesan, Eric Lee, Aws Albarghouthi, Vijay Chidambaram, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. *Protocol-Aware Recovery for Consensus-Based Storage.* FAST '18, pp. 15–31, 2018.

**Fault model** [§3.1, Table 2, p. 19]. The usual crash and network faults, *plus* corruption or unreadability of any on-disk structure (log, snapshots, "metainfo" such as term and vote), on any number of nodes at once. Faults can hit several contiguous blocks or a few bytes. File-system metadata faults can show up as missing or unopenable files, wrong sizes, a read-only file system, or an unmountable one. **Guarantee** [§3.2]: "if there exists at least one correct copy of a committed data item, it will be recovered or the system will wait for that item to be fixed". If every copy is faulty, the system stays unavailable rather than losing data.

### Why common approaches are unsafe or unavailable (§2.3, Table 1, pp. 16–18)

- **AGL+18-F1: a taxonomy.**
  - *NoDetection*: unsafe.
  - *Crash*: safe, but "suffers from severe unavailability"; a persistent fault crashes the node again on every restart.
  - *Truncate*: unsafe. A node truncates its log after a corrupted entry, forms a majority with lagging nodes, and committed entries are silently lost: "We find this safety violation in ZooKeeper and LogCabin".
  - *DeleteRebuild* (wipe the node and restart it): unsafe for the same reason.
  - *MarkNonVoting* (Google's Paxos): can lose promises and is unavailable when only a bare majority is alive.
  - *Reconfigure*: needs a majority to commit the configuration change.
  - *BFT*: "can achieve only half the throughput" and needs 3f+1 nodes.

### CTRL: local storage layer (CLSTORE, §3.3)

- **AGL+18-F2: granularity and detection.**
  - Faults are detected per log entry and per 4 KB snapshot chunk.
  - The metainfo is small and node-specific, so it cannot be recovered from peers; CLSTORE "maintains two copies of the metainfo locally" [§3.3.1, p. 19].
  - Each log entry, snapshot chunk and metainfo copy carries a CRC32. `EIO` is converted into a zero-filled buffer so that it becomes a checksum mismatch [§4.1, p. 24].
  - Log and metainfo files "are preallocated and are of a fixed size", which makes truncation and extension faults detectable [§3.3.2, p. 20].
- **AGL+18-F3: telling a crash from a corruption (§3.3.3, p. 20).** Each entry e_i is followed by a small persist record p_i.
  - The strict order would be "write(e_i), fsync(), write(p_i), fsync()". To avoid the extra fsync, CLSTORE writes e_i then p_i and calls fsync once; it "does not order e_i before p_i".
  - If e_i fails its checksum and p_i is absent, the node crashed mid-write, and the entry is safely discarded (it was never acknowledged).
  - If p_i is present, or e_(i+1) or p_(i+1) is present, the entry is corrupted and must be recovered, not discarded.
  - The one ambiguous case is the last entry: "if e_i is the last entry, then we cannot determine whether it was a crash or a corruption". It is marked corrupted and left to the distributed protocol.
  - Snapshots and metainfo avoid the ambiguity because "These files are first written to a temporary file and then atomically renamed".
- **AGL+18-F4: identifiers stored apart from the data (§3.3.4, p. 21).**
  - An entry's identifier is (epoch, index, offset, checksum). It is stored at the head of the log file, away from the entry, because storing the two together is less useful: "a misdirected write can corrupt both the item and its identifier". The identifier also serves as the persist record.
  - The cost is a "nominal storage overhead (32 bytes for log entries and 12 bytes for snapshots)".
  - In the implementation, "CLSTORE ensures that a log entry and its identifier are at least a few megabytes physically apart" [§4.1, p. 24].

### CTRL: distributed recovery (§3.4–3.5, pp. 21–24)

- **AGL+18-F5: fixing a follower.** The leader holds every committed entry. A follower reports its faulty entries as (epoch, index). If the leader does not have that exact (epoch, index), the entry was never committed and the follower truncates it.
- **AGL+18-F6: fixing the leader, by finding out whether the entry was committed.** A node with faulty entries may be elected leader, but it must repair them before it accepts new commands. It asks the followers about each faulty entry and gets one of three answers:
  - *have*: fix the entry from that follower;
  - *dontHave*: "if it gets a dontHave response from a majority of followers, it confirms that the entry is uncommitted" and discards it together with every later entry;
  - *haveFaulty*: wait for one of the two cases above.

  If no majority answers, the leader waits. "In the unfortunate and unlikely case where all copies of an entry are faulty, the system will remain unavailable". In LogCabin, "After a configurable recovery timeout, the leader steps down" [§4.2, p. 25].
- **AGL+18-F7: identical snapshots, recovered chunk by chunk** [§3.5]. The leader inserts a `snap` marker into the log. Every node snapshots when that marker is applied, so snapshots are taken at the same index and are byte-identical. The log is garbage-collected (a `gc` marker) only after a majority has the snapshot. Recovery then fetches individual faulty chunks, or the full snapshot when needed.

### Evaluation (§5, pp. 25–27)

- **AGL+18-F8: correctness.**
  - Targeted corruptions: of the cases that are recoverable, "the original systems recover only in 46/2401 cases", while CTRL recovers all 2401.
  - Random block corruptions: original LogCabin is unsafe or unavailable in about 30% of 5000 cases. On block *errors* the originals are unavailable in about 50% of cases. CTRL recovers them all.
  - Model checking covered "over 2.5M log states".
  - File-system metadata faults: the originals violated safety in 36 cases (LogCabin) and 192 cases (ZooKeeper). CTRL crashes the node deliberately and stays safe.
- **AGL+18-F9: cost.** Write throughput drops by 8–10% on HDD at 32 clients, because identifier and entry are placed apart and the head must seek; on SSD the cost is "very minimal … (4% in the worst case)". Recovering one corrupted entry out of 30K took "1.24 seconds (32MB transferred) in the original system, while CTRL takes only 1.2 ms (7KB transferred)" [§5.2, pp. 26–27]. The implementation is about 1500 lines of code per system [§4, p. 24].

### Implications for mantle

- **R7.1 Metadata-service log (Raft).** Adopt CTRL as specified:
  - a CRC for each entry, and (term, index, offset, crc) identifiers kept in a separate region several MB away from the entries;
  - two checksummed copies of term and vote;
  - leader recovery using have / dontHave / haveFaulty, with a majority of dontHave required before discarding;
  - snapshots at the same index on every node, triggered by a snap marker in the log and recoverable by chunk;
  - a recovery timeout after which the leader steps down (AGL+18-F2–F7).
- **R7.1a What mantle's log does (docs/design/raft-log.md §2, §6).** Persist records are
  written in each frame's flush, a segment's length apart from every frame, and carry each
  group's hard state, start and entry positions, so a damaged last frame is restored, not
  cut. Two differences from CLSTORE, both measured against the replica simulation:
  - *Confirmation.* An unordered persist record can land while its frame tears, and CLSTORE
    then treats the entry as corrupt and asks its peers. In mantle's simulation, where
    members are lost for good, that turned crashes into groups that could elect no one: a
    member marked for an entry no one acknowledged, beside a lost member. So a record counts
    as proof of acknowledgement only once confirmed, by the next frame's record or a
    confirmation the writer flushes when the log falls idle or closes; an unconfirmed one
    keeps only its term and vote. The window left is a crash within about one flush of an
    acknowledgement followed by damage to that frame before the restart.
  - *Hard state in the record.* CLSTORE's identifiers name entries; mantle's also carry the
    hard state, so a damaged last frame never regresses a term or vote, confirmed or not.
  - The replica judges vote requests against the last entry it acknowledged while its log
    lacks it, rather than CLSTORE's per-entry have/dontHave exchange: once a leader's entry
    of a later term arrives, Raft's log matching puts every committed entry the mark covers
    in the log.
- **R7.2 Chunk-store journal: same crash/corruption rule.** Apply the same disentanglement to the storage node's local journal. A bad tail with no persist record is a crash and is discarded. A bad record that has a persist record, or a valid record after it, is corruption and must be repaired. Never let a corrupted record trigger truncation of later records (AGL+18-F3; GAA17-F6). *Inference:* when a chunk-store record cannot be repaired locally, the metadata service decides the outcome: the chunk is re-replicated from peers, or the write is declared failed if it was never acknowledged.
- **R7.3 Preallocate log segments at a fixed size.** This makes size changes after a crash detectable, and appends overwrite preallocated space instead of changing file metadata (AGL+18-F2; AWK+19-F6).
- **R7.4 Never apply DeleteRebuild (wipe the data and restart) to a Raft node while it is a voter.** Wiping its data is unsafe (AGL+18-F1). Rebuild a replica only as a learner, or only after the protocol has confirmed commitment.
- **R7.5 Budget the cost.** Expect roughly 4% write overhead on SSD and 8–10% on HDD for separating identifiers (AGL+18-F9). The metadata log should live on SSD.

---

## 9. Rosenblum & Ousterhout, TOCS 1992: log-structured storage and segment cleaning (request item 8)

**Citation.** Mendel Rosenblum and John K. Ousterhout. *The Design and Implementation of a Log-Structured File System.* ACM Transactions on Computer Systems 10(1): 26–52, February 1992. doi:10.1145/146941.146943. (We read the authors' preprint, whose pagination differs from TOCS, so we cite sections only.)

### Findings

- **RO92-F1: headline** [abstract]. Sprite LFS "can use 70% of the disk bandwidth for writing, whereas Unix file systems typically can use only 5-10%".
- **RO92-F2: segment size** [§3.2]. The disk is divided into "large fixed-size extents called segments", and each segment is "always written sequentially from its beginning to its end". The size is chosen "large enough that the transfer time to read or write a whole segment is much greater than the cost of a seek to the beginning of the segment". "Sprite LFS currently uses segment sizes of either 512 kilobytes or one megabyte".
- **RO92-F3: telling live blocks from dead ones** [§3.3]. Segment summary blocks record each block's owner. A block is live if the file's inode still points to it. A per-file version number, incremented on delete or truncate, forms "an unique identifier (uid)" together with the inode number, which gives a fast dead-block check.
- **RO92-F4: write cost** [§3.4, formula (1)]. Write cost is the "total bytes read and written" divided by "new data written". Cleaning reads N segments and writes back N·u segments of live data, freeing N(1−u) segments. So write cost = (N + N·u + N·(1−u)) / (N·(1−u)) = **2/(1−u)**, where u is the utilization of the segments being cleaned. If u = 0 the segment need not be read and the cost is 1.0. To beat Unix FFS on small files, "the segments cleaned must have a utilization of less than .8".
- **RO92-F5: when to clean** [§3.4]. Cleaning starts when the number of clean segments "drops below a threshold value (typically a few tens of segments)", cleans "a few tens of segments at a time", and stops above "another threshold value (typically 50-100 clean segments)". Performance "does not seem to be very sensitive to the exact choice of the threshold values". The critical choices are *which* segments to clean and *how* to group the live data.
- **RO92-F6: greedy cleaning fails under locality** [§3.5]. With a uniform workload at 75% disk utilization, the segments cleaned average only 55% utilization. But with a hot/cold workload (10% of files get 90% of writes), greedy selection plus age sorting was *worse*: "performance got worse and worse as the locality increased". Cold segments tie up free space for a long time.
- **RO92-F7: the cost-benefit policy** [§3.5]. "The amount of free space is just 1−u". The age used is the most recent modification time of any block in the segment, i.e. the age of the youngest block. "The cost of cleaning the segment is 1+u (one unit of cost to read the segment, u to write back the live data)". The policy picks the segments with the highest

  > benefit / cost = (free space generated × age of data) / cost = **(1−u) × age / (1+u)**

  Combined with sorting live blocks by age, it produced the desired bimodal distribution: it "cleans cold segments at about 75% utilization but waits until hot segments reach a utilization of about 15%", and "reduces the write cost by as much as 50% over the greedy policy" [§3.5].
- **RO92-F8: segment usage table** [§3.6]. It records, per segment, "the number of live bytes in the segment and the most recent modified time of any block in the segment". A segment whose live count reaches zero is reused without cleaning.
- **RO92-F9: crash recovery** [§4.1–4.2]. A checkpoint writes all modified metadata to the log, then writes a checkpoint region at a fixed location. "there are actually two checkpoint regions, and checkpoint operations alternate between them", and the one with the newest timestamp wins. The checkpoint interval was "thirty seconds, which is probably much too short". The authors suggest checkpointing by amount of data written rather than by time. Roll-forward replays the segment summaries written after the checkpoint. A "directory operation log" is written before the directory and inode blocks it describes, which also "made it easy to provide an atomic rename operation".
- **RO92-F10: production numbers** [§5.2, Table 2]. Over four months on five file systems at 11–75% disk utilization, "more than half of the segments cleaned were totally empty". "The overall write costs ranged from 1.2 to 1.6", compared with 2.5–3 in simulation, which "limits the long-term write performance to about 70% of the maximum sequential write" bandwidth. Real files are longer than simulated ones and are deleted whole, and truly cold data sits in segments that are never cleaned. Recovery-time estimate [§5.3]: "maximum recovery time would grow by one second for every 70 seconds of checkpoint interval length" at the highest observed write rate of 150 MB/hour.
- **Caveat:** 1991-era disks and workloads. The *principles* carry over; the specific segment sizes do not (see §12 for modern zone and band sizes).

### Implications for mantle

- **R8.1 Chunk store as a log.** Treat each device as an append-only log of large segments. *Inference:* size segments per device class. The 1992 rule, transfer time ≫ seek time, gives roughly tens of MiB on today's HDDs. SMR segments should equal the band or zone size. ZNS segments should equal the zone capacity (§12). SSD segments should be a large multiple of the erase-block × channel stripe (§4, R3.1).
- **R8.2 Choose cleaning victims by cost-benefit.** Maintain a segment usage table (live bytes, youngest-data age) and select victims by `(1−u)·age/(1+u)`. Sort surviving blocks by age, or better by expected death time (HKA17), when rewriting them (RO92-F7, F8).
- **R8.3 Watermark-driven cleaning.** Start compaction below a low watermark of free segments and stop above a high watermark, cleaning in batches. *Inference:* set the watermarks per device from segment size and ingest rate, and add a separate reserve so repair and re-replication can always write (RO92-F5).
- **R8.4 Budget write amplification with 2/(1−u).** Cleaning at u = 0.5 triples device writes relative to user writes (cost 4.0 against 1.0 for a free segment). *Inference:* choose target utilization per device class accordingly, and prefer whole-segment deletion (whole chunks dying together) so that cleaning is free (RO92-F4, F10).
- **R8.5 Checkpointing.** Use two alternating checkpoint regions with a trailing timestamp or sequence number and a checksum. Checkpoint by *bytes written* so recovery time is bounded. Replay forward from the checkpoint using self-describing segment summaries (RO92-F9).
- **R8.6 Per-extent generation numbers.** Maintain (chunk id, generation) as a unique identifier, as LFS does with (inode, version), so the cleaner can tell dead blocks from live ones without a full index lookup, and scrub and repair can recognize stale copies (RO92-F3; GGL03 §4.5).

---

## 10. Field reliability studies: latent sector errors, silent corruption, scrubbing, flash (request item 9)

### 10.1 Bairavasundaram, Goodson, Pasupathy, Schindler, SIGMETRICS 2007: latent sector errors (LSEs)

**Citation.** *An Analysis of Latent Sector Errors in Disk Drives.* SIGMETRICS '07; ACM SIGMETRICS Performance Evaluation Review 35(1): 289–300, 2007. doi:10.1145/1269899.1254917.
**Data.** NetApp field logs covering "1.53 million disks" over 32 months, both nearline (SATA) and enterprise (FC) drives. Errors were detected by reads, writes, and SCSI VERIFY-based media scrubs, which "typically complete within 2 weeks" [§3.3, p. 291].

- **BGPS07-F1: how common.** "A total of 3.45% of 1.53 million disks developed latent sector errors over a period of 32 months" [§1 Table 1, p. 290]. "about 8.5% of all nearline disks are affected … while only 1.9% of all enterprise class disks" [§5.2 Obs. 1, p. 293]. Within 12 months of shipping, 3.15% of nearline and 1.46% of enterprise disks develop at least one LSE [§5.2, p. 293]. The fraction affected grows with age (linearly for enterprise disks, super-linearly for nearline) and "increases as disk capacity increases" [Table 1, p. 290].
- **BGPS07-F2: counts per disk.** For most models, "more than 80% of disks with latent sector errors have fewer than 50 errors". Errors are not independent: a disk that already has one is more likely to develop more [Table 1; §5.4.1].
- **BGPS07-F3: spatial locality** [§5.4.2 Obs. 10, p. 296]. "the probability of other latent sector errors within a 10 MB radius of an existing error is 0.5", and more than 0.6 for many models. On average there is more than one other error within 10 MB, and up to 2.5 for some models.
- **BGPS07-F4: temporal locality** [§5.4.3 Obs. 11–12, p. 296]. "between 40%-80% of errors arrive within one minute of the previous error". For "54.8% of nearline error disks and 62.0% of enterprise class error disks, at least one additional error is developed within one month". The probability of 50 more errors within a month is 0.05 (nearline) and 0.10 (enterprise).
- **BGPS07-F5: scrubbing finds most errors** [§5.5 Obs. 13, p. 298; §6.2, p. 299]. Verify (scrub) operations discovered "86.6% of all latent sector errors in nearline disks and 61.5% … in enterprise class disks", or 77.4% on average across models. In §6.2, "over 60% of all latent sector errors are discovered by the media scrubbing process, which scans the entire surface of the media at least once every two weeks", and "a low priority background scrubbing process is sufficient".
- **BGPS07-F6: correlation with other errors** [Table 1]. Enterprise disks show strong correlation between recovered errors and LSEs; nearline disks between not-ready conditions and LSEs.
- **BGPS07-F7: speed up repair when risk is high** [§6.3, p. 299]. Keep each disk's age, error count, and last-error time. If the surviving disks in a RAID group are older than 1 year, or any had an error in the last 1000 minutes, "the repair process should proceed at an accelerated pace".

### 10.2 Bairavasundaram, Goodson, Schroeder, Arpaci-Dusseau, Arpaci-Dusseau, FAST 2008: silent data corruption

**Citation.** *An Analysis of Data Corruption in the Storage Stack.* FAST '08, pp. 223–238, 2008.
**Detection mechanism** [§2.2, p. 225]. Each 4 KB block is written with "a 64-byte data integrity segment". On enterprise drives it sits in 520-byte sectors; on nearline drives it goes in a ninth 512-byte sector. It holds a checksum of the block, the block's *identity* within the file system ("this block belongs to inode 5 at offset 100"), and a checksum of the integrity segment itself [§2.2.1]. Data scrubs read and verify every block, and "an entire RAID group is scrubbed approximately once every two weeks on an average" [§2.2.2].

- **BGS+08-F1: how common.** There were about 400,000 checksum mismatches over 41 months, on "3088 of the 358,000 nearline disks (0.86%) and 767 of the 1.17 million enterprise class disks (0.065%)" [§4.1, p. 227]. Among affected disks, the mean was 104 mismatches and "the median is 3" [§4.1]. Nearline disks and their adapters corrupt "an order of magnitude more often" [§1, p. 224].
- **BGS+08-F2: corruption clusters in physically adjacent blocks** [§4.3.3 Obs. 10, p. 231]. On disks with 2–10 mismatches, for more than 50% (nearline) and more than 40% (enterprise) of corrupt blocks, "the immediate neighboring block also has a checksum mismatch". A run of consecutive bad blocks averages 3.4 blocks. 3% of affected drives have runs of 100 blocks, and 0.7% have runs of 1000.
- **BGS+08-F3: corruptions cluster in time** [§4.3.4 Obs. 11, p. 232]. "Most checksum mismatches are detected within one minute of a previous detection of a mismatch".
- **BGS+08-F4: disks in the same system are not independent** [§4.3.2 Obs. 9, p. 230]. One nearline system had 92 corrupt disks, with probability below 1e−12 if the disks were independent. A shared component, such as a shelf controller or adapter, is the likely cause [§6.1.2].
- **BGS+08-F5: how corruption is found** [§4.5 Obs. 16–17, p. 233]. Data scrubbing finds about 49% (nearline) and 73% (enterprise) of mismatches. About 8% are found only during RAID reconstruction, which is when they can cause data loss.
- **BGS+08-F6: identity checks catch what checksums miss** [§5.1–5.2, p. 235]. Lost or misdirected writes that pass the checksum but fail the identity check were found "in a total 365 disks out of the 1.53 million disks", and "the system recommends replacement of the disk once the first identity discrepancy is detected". Parity inconsistencies affect 3.5–4.4× fewer disks than checksum mismatches.
- **BGS+08-F7: drives can acknowledge a cache flush without persisting** [§6.1.3, p. 236]. "Upon reception of a cache flush command, the disk drive sometimes returned success without committing the data to stable storage". After a power cycle the data was lost. Block identity protection plus RAID prevented any user-visible loss.
- **BGS+08-F8: firmware bugs hit particular LBAs** [§6.1.1, p. 235]. For one disk model, particular block numbers were much more likely to be corrupted, probably from a firmware bug.
- **BGS+08-F9: the paper's lessons** [§6.2, p. 236]:
  - checksums *and* block identity are "well-worth the extra space";
  - "More aggressive scrubbing can speed the detection of errors";
  - replace an enterprise drive at its first corruption, because many more are likely to follow;
  - "use staggered stripes such that the blocks that form the stripe are not stored at the same or nearby block number";
  - "redundant data structures should be stored distant from each other" and "should be written as part of different write requests spaced over time";
  - use locality to "trigger a scrub before it's next scheduled time" in areas likely to be affected.

### 10.3 Schroeder, Damouras, Gill, FAST 2010: how to scrub

**Citation.** *Understanding Latent Sector Errors and How to Protect Against Them.* 8th USENIX FAST, 2010 (page range not printed in our PDF). An extended version appeared in ACM TOS 6(3), 2010, doi:10.1145/1837915.1837917. Data: a subset of the BGPS07 NetApp data (LSE timestamps and logical block numbers).

- **SDG10-F1: statistics** [§3; §6]. Burst lengths, gaps between bursts, and LSEs per time period follow power laws; "a Pareto distribution fits the data very well". Poisson and geometric models are poor fits. There is "no significant difference … in nearline drives versus enterprise class drives". "nearly all drives with LSEs, experience all LSEs in their lifetime within the same 2-week period", i.e. one event (such as a scratch) rather than gradual wear [§6, PDF p. 13].
- **SDG10-F2: scrub policies compared** [§5, PDF pp. 9–13]. The metric is mean time to error detection (MTTED). Standard sequential scrubbing reads at rate c/s for capacity c and interval s; "Common scrub intervals are one or two weeks" [§5].
  - *Local* scrubbing (re-scan r sectors after an error) was "disappointing", about equal to standard.
  - *Accelerated* scrubbing (scan the rest of the disk faster after an error) gave no substantial gain either.
  - *Staggered* scrubbing wins. The disk is split into r regions, each divided into segments; each pass reads segment 1 of every region, then segment 2, and so on. With "a region size of 128MB and a segment size of 1MB", staggered scrubbing and accelerated-staggered scrubbing improve MTTED by "30 to 70 hours, corresponding to an improvement of 10–20%" at 7–14-day intervals, with larger gains at longer intervals [§5.2.3, PDF p. 12]. Overall, staggered scrubbing "can improve the mean time to error detection by up to 40%" without changing the scrub rate [§6, PDF p. 13].
- **SDG10-F3: segment size barely matters.** With a 128 MB region, effectiveness is "identical for segment sizes ranging from 1KB to 32MB". At 64 MB segments the gain over standard scrubbing "drops by 50%". Rule of thumb: keep segments below ¼–½ of the region size. Citing Oprea et al., segments of 1 MB or more cost about the same I/O as standard scrubbing [§5.2.3, PDF p. 12].
- **SDG10-F4: redundancy inside a single disk** [§4; §6]. Simple single-parity schemes "still leave a significant fraction of drives (50% for some models)" with unrecoverable errors. Stronger codes on the error-prone bottom 10% of the LBA space cut unrecoverable drives by 30% versus single parity. Interleaved parity is "significantly weaker than … MDS" codes.

### 10.4 Schroeder, Lagisetty, Merchant, FAST 2016: flash in production

**Citation.** *Flash Reliability in Production: The Expected and the Unexpected.* FAST '16, pp. 67–80, 2016. Data: six years of Google production, "many millions of drive days", ten models (MLC, eMLC, SLC), 24–50 nm lithography [§1].

- **SLM16-F1: uncorrectable errors are common** [§3, p. 68; §10, p. 79]. "between 20-63% of drives experience at least one such error and between 2-6 out of 1,000 drive days are affected". Uncorrectable errors are the most common non-transparent error.
- **SLM16-F2: RBER and UBER are poor metrics** [§5, p. 75; §10, p. 79]. "RBER is a poor predictor of UEs". UBER "is not very meaningful" because uncorrectable errors do not correlate with the number of reads. RBER grows with P/E cycles "following a linear rather than exponential rate", and age matters independently of use.
- **SLM16-F3: bad blocks and chips** [§6, pp. 76–77]. Depending on model, "30-80% of drives develop bad blocks in the field", and "around 2-7% of drives develop bad chips during the first four years". After a second bad block, the median total jumps to about 200 ("50% of those drives that develop two bad blocks will develop close to 200 or more"). "most bad blocks are discovered in a non-transparent way", i.e. by a failed read that the user sees. Vendors "guarantee that no more than 2% of blocks on a chip will go bad" within the P/E limit; two-thirds of the chips declared bad exceeded 5%.
- **SLM16-F4: one error predicts the next** [§5, p. 76]. "the chance of experiencing an uncorrectable error in a month following another uncorrectable error is nearly 30%, compared to only a 2% chance" in a random month.
- **SLM16-F5: flash compared with HDD** [§8, p. 78]. Flash drives are replaced less often (4–10% over 4 years, against a previously reported 2–9% *annual* rate for HDDs), but they have "significantly higher rates of uncorrectable errors". "We see no evidence that higher-end SLC drives are more reliable than MLC drives" [abstract].
- **Caveat:** MLC/eMLC/SLC drives from 2010–2015. Modern TLC/QLC drives were not studied (**UNVERIFIED** whether they are better or worse).

### Implications for mantle (all of §10)

- **R9.1 Scrub cadence.** Every device completes a full *data* scrub (read plus checksum verification, not just SCSI VERIFY) at least every 14 days. Target 7 days for data and 1 day for metadata, following Ceph (AWK+19-F9; BGPS07-F5; BGS+08 §2.2.2; SDG10-F2). Run it at low priority, throttled against the foreground latency target (BGPS07-F5).
- **R9.2 Scrub order.** Scrub in a staggered order, 128 MiB regions by 1 MiB segments, rather than one sequential pass (SDG10-F2, F3). *Inference:* on SMR or ZNS, stagger by zone and read each segment sequentially within its zone.
- **R9.3 Why data scrubbing is required.** Silent corruption is invisible to the drive. Only reading and checking mantle's own checksums and identities finds it; SCSI VERIFY only finds LSEs (BGS+08-F1, F6).
- **R9.4 Escalate on the first error.** When an LSE or checksum mismatch is found:
  - (a) immediately verify the neighbourhood: at least ±10 MiB for LSEs, and consecutive blocks for corruption (BGPS07-F3; BGS+08-F2);
  - (b) repair from replicas or EC right away;
  - (c) mark the device "at risk" for about a month, raise its scrub priority, and speed up repair of chunks whose *other* copies sit on at-risk devices (BGPS07-F4, F7; BGS+08-F3; SLM16-F4);
  - (d) retire or drain the device after an identity discrepancy (a lost or misdirected write) (BGS+08-F6), after a first corruption on an enterprise-class drive (BGS+08-F9), or after an SSD's bad-block count passes a small threshold (SLM16-F3).
- **R9.5 Placement.** *Inference, from BGS+08-F4 and F8:* avoid putting all replicas or EC fragments of a stripe on drives of the same model behind the same controller or shelf. Avoid identical physical offsets for a stripe's fragments across devices ("staggered stripes"). Keep a device's redundant local structures (superblock copies, checkpoint regions, the identifier index) far apart and written in separate I/Os (BGS+08-F9).
- **R9.6 Do not trust a single device's flush.** Durability is claimed only when the record is on k devices in independent failure domains. Identity and generation checks detect writes the drive claimed but did not persist (BGS+08-F7). *Inference:* this is why replication must be synchronous before a write is acknowledged.
- **R9.7 SSD health signals.** Track uncorrectable-error counts, final read errors, and bad-block counts per SSD, not RBER or UBER, and use them to drive proactive draining (SLM16-F2–F4).
- **R9.8 Intra-disk parity is not a substitute** for cross-node redundancy (SDG10-F4). *Inference:* mantle should rely on replicas and EC for repair, and use checksums and identity for detection.

---

## 11. Why checksums must be end to end: Stone & Partridge, SIGCOMM 2000; Saltzer, Reed & Clark, TOCS 1984 (request item 10)

### 11.1 Stone & Partridge

**Citation.** Jonathan Stone and Craig Partridge. *When the CRC and TCP Checksum Disagree.* SIGCOMM 2000, pp. 309–319. doi:10.1145/347059.347561.

- **SP00-F1: TCP checksum failures are frequent** [abstract, p. 309]. "between 1 packet in 1,100 and 1 packet in 32,000 fails the TCP checksum, even on links where link-level CRCs should catch all but 1 in 4 billion errors". One hour-long test saw 1 in 400. The authors collected "nearly 500,000 packets which failed the TCP or UDP or IP checksum".
- **SP00-F2: some errors get through both checks** [abstract; §6, p. 318]. The checksum "will fail to detect errors for roughly 1 in 16 million to 10 billion packets". "the expected time until a corrupted data is accepted could be as low as a few minutes" for flows through the worst "bad-apple" hosts or paths.
- **SP00-F3: where the errors come from** [§4.3, p. 313]. "Errors fall into four broad groups: errors in end-host hardware, errors in end-host software, errors in router memory; and errors at the link level or in network-interface hardware". Examples include DMA errors, a TCP bug (the ACK-of-FIN bug), and memory errors in routers. "the networking hardware is often trashing the packets which are entrusted to it" [§5.1, p. 317].
- **SP00-F4: the Internet checksum is weak** [§2, p. 310; §4.4, p. 317]. It detects all bursts up to 15 bits and "all 16-bit burst errors except two: substitutions of 0x0000 for 0xFFFF and vice-versa"; other errors are caught only probabilistically, depending on the data. In earlier ATM work it detected cell erasures at only about 1 in 2^10. CRC-32 "will detect all errors that span less than 32 contiguous bits and all 2-bit errors less than 2048 bits apart", with about a 1 in 2^32 miss rate for other errors [§1, p. 309].
- **SP00-F5: compute the checksum early and at the application** [§5.1–5.4, pp. 317–318]. "the safest thing to do is checksum the data as early as possible in the transmission path: before the data can suffer DMA errors or data path errors in the network interface". The author's earlier advice to checksum during DMA or in the NIC was "wrong because it leaves data too exposed to hardware errors". "Rather the application must add the checksum before handing its data to TCP". "vital applications should strongly consider augmenting the TCP checksum with an application[-level checksum]"; "the application should add a stronger application-level checksum".

### 11.2 Saltzer, Reed & Clark

**Citation.** J. H. Saltzer, D. P. Reed, D. D. Clark. *End-to-End Arguments in System Design.* ACM TOCS 2(4): 277–288, November 1984. doi:10.1145/357401.357402. (Cited by the paper's named subsections.)

- **SRC84-F1: the argument** [Introduction]. "The function in question can completely and correctly be implemented only with the knowledge and help of the application standing at the end points of the communication system. Therefore, providing that questioned function as a feature of the communication system itself is not possible. (Sometimes an incomplete version of the function provided by the communication system may be useful as a performance enhancement.)"
- **SRC84-F2: careful file transfer** ["Careful file transfer"]. The threats listed include: the file on disk "may contain incorrect data, perhaps because of hardware faults in the disk storage system"; software mistakes in buffering and copying; transient processor or memory errors; the network dropping, altering or duplicating packets; and hosts crashing partway through. The fix is an end-to-end checksum stored with the file. The receiver *reads the copy back from its disk*, "recalculates the checksum, and sends this value back to host A", and the transfer commits only if the two match. Reliability inside the network only reduces how often retries are needed.
- **SRC84-F3: a real incident** ["A too-real example"]. A gateway swapped byte pairs "with a frequency of about one such interchange in every million bytes passed". Per-hop checksums did not help because the data was unprotected while it sat inside the gateway.
- **SRC84-F4: acknowledgements must also be end to end** ["Delivery guarantees"]. "The acknowledgement that is really desired is an end-to-end one", meaning "I did it", from the application that performed the action.
- **SRC84-F5: duplicate suppression belongs at the endpoints** ["Transaction management"]. In SWALLOW, "the object identifier plus the version information suffices to detect duplicate writes", so the transport does not need to suppress duplicates.

### Implications for mantle

- **R10.1 Checksum at the client.** The client library computes CRC-32C per checksum block *before* handing data to the network stack. The storage node verifies the data before acknowledging and stores those same checksums. Do not recompute a "fresh" checksum on the server over data that may already have been damaged in transit (SP00-F5; SRC84-F1, F2).
- **R10.2 Verify across layers.** On every read the storage node verifies against the stored checksum, and the client verifies again end to end. *Inference:* the metadata service should also hold a whole-object digest per chunk, so readers can detect *lost* or *stale* chunks that are internally consistent (SRC84-F2; BGS+08-F6).
- **R10.3 Acknowledge only after durability.** A write acknowledgement means the data is durable on the required number of replicas or EC fragments. It never means merely "received" (SRC84-F4; GGL03 §2.6.3).
- **R10.4 Idempotent writes.** Write requests carry (chunk id, generation/version, offset), so retried or duplicate writes are detected at the storage node without transport-level deduplication (SRC84-F5; GGL03 §4.5).
- **R10.5 Do not rely on the transport or NIC.** TCP checksums and NIC checksum offload are insufficient for storage integrity (SP00-F1–F4).

---

## 12. Zoned storage: ZNS SSDs (Bjørling et al., ATC 2021) and SMR disks (Aghayev & Desnoyers, FAST 2015; Aghayev et al., FAST 2017) (request item 11)

### 12.1 Bjørling et al., ZNS

**Citation.** Matias Bjørling, Abutalib Aghayev, Hans Holmberg, Aravind Ramesh, Damien Le Moal, Gregory R. Ganger, George Amvrosiadis. *ZNS: Avoiding the Block Interface Tax for Flash-based SSDs.* USENIX ATC '21, pp. 689–703, 2021.
**Setup** [§5, Table 3, p. 696]. One production SSD platform that can be formatted either as a block device (7% or 28% over-provisioning, with stream support) or as a ZNS device (0% OP). 2 TiB media; ZNS max active zones 14; zone size 2048 MiB; zone capacity 1077 MiB. Host: AMD EPYC 7302P, Linux 5.9, RocksDB 6.12.

- **BAH+21-F1: the block-interface tax** [§2.1, p. 690; §3.1, p. 692]. SSDs need media "over-provisioned by up to 28% of the total capacity" for garbage collection, and a fully associative mapping table "often requires 1GB of mappings per 1TB of media capacity". Garbage collection brings write amplification, throughput limits, and unpredictable latency.
- **BAH+21-F2: the zone model** [§2.3, p. 691].
  - Each zone is read randomly but written sequentially, and must be reset before it is rewritten.
  - Zone states are EMPTY, OPEN, CLOSED, FULL. Writes that "do not begin at the write pointer, or (2) write to a zone in the FULL state will fail to execute".
  - ZNS adds *zone capacity*, which "allows a zone to have a writeable capacity smaller than the zone size".
  - ZNS also adds an *active zone limit*, a hard cap on zones in OPEN or CLOSED state, which is necessary because of flash properties such as program disturb.
  - The write pointer the device maintains lets the host find where writing stopped after a crash.
- **BAH+21-F3: the constraints on hardware** [§3.1, p. 692]. A zone's size follows the erase-block stripe across 16–128 dies, "hundreds of megabytes to low single-digit gigabytes". The authors "argue for the smallest zone size possible, where die-level protection is still provided". Each active zone ties up buffers, XOR engines and power-loss capacitors, so "ZNS SSDs are expected to have 8-32 active zones".
- **BAH+21-F4: integrate zones with the application** [§1, p. 690; §3.2]. "Shifting FTL responsibilities to the host is less effective than integrating with the data mapping and placement logic of storage software". The authors name LSM stores, CacheLib and Ceph SeaStore as natural fits.
- **BAH+21-F5: ZenFS, a RocksDB backend for zones** [§4.2, pp. 695–696].
  - Files map to extents, and "An extent is a variable-sized, block-aligned, contiguous region that is written sequentially to a data zone"; "extents do not span zones".
  - A zone is reset when every file with extents in it has been deleted.
  - Two journal zones hold the superblock and the extent map, and recovery replays them up to the write pointer.
  - Zone selection uses lifetime hints: "A match is only valid if the lifetime of the file is less than the oldest data stored in the zone".
  - A zone is finished (closed for writing) when its remaining capacity falls below a configurable limit, for example "setting the finish limit to 5%". Space amplification is "kept at around 10%".
  - ZenFS needs "a minimum of three active zones" (journal, WAL, compaction). It "can work with as few as 6 active zones with restricted write performance, while more than 12 active zones does not add" further benefit.
  - It "performs direct I/O writes for SST files, bypassing the kernel page cache". The WAL is buffered, and on flush "the buffer is padded to the next block boundary", a small write amplification.
- **BAH+21-F6: f2fs on zones** [§4.1, p. 694]. f2fs "requires its metadata to be stored on a conventional block device". Its open segments are capped at 6 to fit the active-zone limit, and its random-write feature (slack space recycling) is disabled on zoned devices.
- **BAH+21-F7: results** [§5, pp. 696–698].
  - Sustained writes: the block SSDs reach 370 MiB/s (7% OP) and 590 MiB/s (28% OP), while "The ZNS SSD with 0% OP, however, reaches 1010MiB/s", which is 1.7–2.7× faster with 7–28% more usable capacity. (The text also says the block SSDs "sustain target writes up to 300MiB/s (0% OP)"; "0% OP" there looks like a typo for 7%.)
  - Idle 4 KiB read latency on the block SSDs is 85 µs. While writing at 300 MiB/s, ZNS read latency is "64% and 27% lower" than the 7% and 28% OP block SSDs.
  - RocksDB overwrite: "ZenFS is 183% faster than XFS". Write amplification is 2.0× for XFS and 2.4× for f2fs on the block SSD, against about 1.0× on ZNS. p99.9 read latency is 2–4× lower.
  - Against a stream-capable SSD: "up to 44% higher throughput on ZNS and up to half the tail latency".
- **Zone Append: NOT ADDRESSED.** The ZNS Zone Append command (the device picks the write offset, which allows many writes in flight to one zone) does not appear in this paper. Any mantle design that relies on Zone Append semantics is **UNVERIFIED** with respect to the peer-reviewed literature in this review, and must be grounded in the NVMe ZNS specification or other sources.

### 12.2 Aghayev & Desnoyers, Skylight (drive-managed SMR internals)

**Citation.** Abutalib Aghayev and Peter Desnoyers. *Skylight—A Window on Shingled Disk Operation.* FAST '15, pp. 135–149, 2015. Studied: Seagate 5 TB and 8 TB drive-managed SMR disks, plus emulated shingle translation layers.

- **AD15-F1: what the drives do internally** [§1, p. 136].
  - "The drives use a persistent disk cache of 20 GiB and 25 GiB on the 5 TB and 8 TB drives", written as a journal. Random writes are fast until that cache is full.
  - "Non-cached data is statically mapped" (fixed LBA-to-physical placement).
  - "the examined drives have a small band size of 15–40 MiB".
  - Cleaning runs aggressively during idle time, and "cleaning duration is 0.6–1.6 s per modified band".
- **AD15-F2: conclusions for users** [§6, p. 147].
  - With the volatile write cache disabled, sequential throughput is 3× or more below a conventional drive; "achieving full sequential throughput requires enabling volatile cache".
  - Random write throughput (with cache or high QD) is "15× that of the equivalent CMR drive", until "Throughput may degrade precipitously when the cache fills after many writes".
  - "Background cleaning begins after ≈1 second of idle time", taking 0.6–1.6 s per band.
  - "Sequential reads of randomly-written data will result in random-like read performance until cleaning completes".
  - The drives perform well only if writes touch few bands at a time and non-sequential writes "occur in bursts of less than 16 GB or 180,000 operations", with long idle periods. The drive "may perform poorly on server workloads".

### 12.3 Aghayev, Ts'o, Gibson, Desnoyers, ext4-lazy (FAST 2017)

**Citation.** *Evolving Ext4 for Shingled Disks.* FAST '17, pp. 105–120, 2017.

- **ATGD17-F1: in-place metadata updates are what hurts.** ext4 writes each metadata block twice: once to the journal, then again at its fixed home location, which is a random write. ext4-lazy writes metadata only to a large journal (10 GiB in the evaluation) and keeps an in-memory map (jmap) from home locations to journal locations [§1–3, pp. 105–107]. Results: "1.7-5.4× improvement over ext4 on a metadata-light file server benchmark" and "2-13× improvement over ext4 on drive-managed SMR disks as well as on conventional disks" for metadata-heavy work [abstract, p. 105].
- **ATGD17-F2: what triggers the drive's fast sequential path** [§4.3.1, p. 113]. On the Seagate ST8000AS0002, "sequential writes of at least 8 MiB in size are streamed" directly to their bands, bypassing the persistent cache. Also, "a single 4 KiB random write in the middle of a sequential write disrupted streaming". Background [§2, p. 107]: an average band of 30 MiB, more than 260,000 bands, a persistent cache of about 25 GiB, and cleaning a band "typically takes 1-2 seconds" and up to 45 s.
- **ATGD17-F3: costs** [§4.4, p. 115]. Each jmap entry takes 66 bytes of memory (1 M entries "requires 63 MiB"). The first noticeable slowdown came at a 1.4 GiB journal, about 70% live metadata.
- **ATGD17-F4: the paper's takeaways** [§6, p. 116]. "file systems should work to eliminate structures that induce small isolated writes". Random writes are costlier than random reads, and when they are unavoidable they should be confined "to the smallest perimeter possible". "putting metadata at the center of the disk and managing it as a log looks like a better choice".

### Implications for mantle (all of §12)

- **R11.1 One sequential segment abstraction for every device type.** The segment is sequentially written, reset or freed as a unit, and carries a device-reported or recovered write pointer. Map it to a ZNS zone (at zone capacity), a host-managed SMR zone (256 MiB, AWK+19-F3), a CMR HDD region, or an SSD region (BAH+21-F2; AWK+19-F3).
- **R11.2 No in-place updates on data devices.** Per-device metadata (extent map, chunk index) lives in a log and is checkpointed, never updated at fixed locations. Superblocks alternate between two slots (RO92-F9; ATGD17-F4; BAH+21-F5 journal zones).
- **R11.3 Open-zone budget.** The number of simultaneously open segments per device must stay within the device's active-zone limit: *expected 8–32; 14 on the evaluated device*. *Inference:* budget one metadata-journal zone, a small number of lifetime-class streams, one compaction/GC destination, and one repair/re-replication destination. ZenFS's experience (3 minimum, 6 workable, more than 12 no gain) suggests about 6–12 is adequate (BAH+21-F3, F5).
- **R11.4 Size chunks and extents to zone capacity.** Extents do not span zones. Finish a zone when its remaining capacity is below a threshold (e.g. 5%) instead of splitting a chunk across zones. Expect about 10% space amplification (BAH+21-F5). *Inference:* a mantle chunk's maximum size should divide the zone capacity (1077 MiB on the evaluated drive, which is not a power of two), or chunks should be packed into zone-sized segments.
- **R11.5 Place data by lifetime.** Put a new chunk into an open zone only if its expected lifetime is shorter than the oldest data already in that zone; otherwise open a new zone (BAH+21-F5; HKA17). Reset zones when their live count reaches zero (RO92-F8). Otherwise garbage-collect them by cost-benefit (RO92-F7).
- **R11.6 Concurrency within a zone.** Without Zone Append (NOT ADDRESSED; UNVERIFIED), writes to one zone must be issued at the write pointer. *Inference:* serialize writes per zone and get parallelism from *several* open zones and devices. Treat Zone Append as a later optimization, pending a separate evidence review.
- **R11.7 Drive-managed SMR.** Write only large sequential extents of at least 8 MiB (the streaming threshold measured on one model, ATGD17-F2). Never interleave small random writes, including metadata; send those to a separate NVMe device. Enable the volatile write cache and rely on explicit flushes, and leave idle time for the drive's cleaning (AD15-F2; ATGD17-F2, F4). *Inference:* prefer host-managed SMR over drive-managed, because drive-managed SMR "may perform poorly on server workloads" (AD15-F2).
- **R11.8 Keep a conventional (non-zoned) region or device** for the small, frequently rewritten structures (superblocks, the metadata journal), as f2fs does (BAH+21-F6), or keep them in dedicated journal zones as ZenFS does.

---

## 13. Group commit: DeWitt et al., SIGMOD 1984; Helland et al., HPTS 1987 (request item 12)

### 13.1 DeWitt, Katz, Olken, Shapiro, Stonebraker, Wood

**Citation.** *Implementation Techniques for Main Memory Database Systems.* SIGMOD '84, pp. 1–8. Also in ACM SIGMOD Record 14(2): 1–8, 1984, doi:10.1145/971697.602261 (DOI from the ACM DL listing). Our copy is an OCR scan.

- **DKO+84-F1: the bottleneck** [§5.1–5.2, pp. 6–7]. A "typical" transaction writes 400 bytes of log, "which takes 10 ms (time to write one 4096 byte page without a disk seek)". Because commit requires the commit record on stable storage, "Assuming a single log device, the system could commit at most 100 transactions per second" [OCR-corrected].
- **DKO+84-F2: pre-commit** [§5.2, p. 7]. When a transaction finishes, its commit record goes into the log buffer and "The transaction releases all locks without waiting for the commit record to be written to disk. The transaction is delayed from committing until its commit record actually appears on disk. The 'user' is not notified that the transaction has committed until this event has occurred." [OCR-corrected] Transactions that read a pre-committed transaction's data are safe, because log pages are written in order.
- **DKO+84-F3: group commit** [§5.2, p. 7]. "The transactions with commit records on the same log page are committed as a group, and are called the commit group. A single log I/O is incurred to commit all transactions within the group." With about 10 records per 4 KB page, throughput rises "by another order of magnitude, to 1000 transactions per second" [OCR-corrected]. Throughput can be pushed further "by partitioning the log across several devices". Groups then need a topological (dependency) order so that a dependent transaction's commit record is never on disk before its predecessor's.
- **DKO+84-F4: where the idea came from** [§5.2, footnote 3, p. 7]. "The notion of group commits appears to be part of the unwritten database folklore. The System-R implementors claim to have implemented it. To our knowledge, neither the idea nor the implementation details have yet appeared in print." [OCR-corrected]
- **DKO+84-F5: stable memory** [§5.4, p. 8]. Battery-backed memory lets transactions commit once their record is in the in-memory log, but "Stable memory does not seem to gain much over the group commit mechanism". Its main benefit is compressing the log (dropping old values of committed transactions).

### 13.2 Helland, Sammer, Lyon, Carr, Garrett, Reuter (HPTS 1987)

**Citation.** *Group Commit Timers and High Volume Transaction Systems.* In *High Performance Transaction Systems* (2nd Int'l Workshop, 1987), LNCS 359, pp. 301–329, Springer, 1989. doi:10.1007/3-540-51085-0_52 (metadata verified via Crossref).
**Content: UNVERIFIED.** The full text is paywalled and the abstract could not be retrieved from the publisher. We therefore make **no** claims about this paper's timer policies or measurements. In particular, we cannot confirm specific group-commit timer values or the throughput effects reported in it.

### Implications for mantle

- **R12.1 One group-commit loop per device log.** Any number of writers append records to an in-memory log buffer. The loop issues one write-and-flush per batch and completes every waiter in the batch together. Acknowledge only after the flush, and for replicated writes, after the required replicas acknowledge too (DKO+84-F2, F3; GGL03 §2.6.3, "The master batches several log records together before flushing").
- **R12.2 Batch size comes from the device, not a timer.** The batch is whatever has accumulated while the previous flush was in flight, so the group grows naturally with load (DKO+84-F3). *Inference:* an explicit delay window (a "timer") trades latency for batch size under light load, and HSL+87 is the classic reference on this, but its conclusions are UNVERIFIED here. Start with no artificial delay, and add an adaptive delay only if device flush rate limits throughput.
- **R12.3 Pre-commit semantics are allowed internally, never externally.** A storage node may make a write visible to later internal operations before it is durable (like DeWitt's pre-commit). Clients must never observe acknowledged-but-not-durable data, and the node must never ack a client before the group flush (DKO+84-F2; SRC84-F4).
- **R12.4 Keep the log small relative to the data.** Separate log devices or partitions increase commit throughput but require ordering across them (DKO+84-F3). *Inference:* one log per device avoids cross-log ordering because chunks on different devices are independent; the metadata service's Raft log is separate.
- **R12.5 Measure the flush cost per device class.** DeWitt's gain (10 records/page → 10×) assumed 10 ms per log write. On NVMe with power-loss protection the ratio is smaller, but the principle (amortize one flush over many commits) is what matters (HL23-F13 notes fsync batching). *Inference:* re-measure on each device SKU.

---

## 14. CRC-32C and GFS-style block checksums: Castagnoli et al., IEEE Trans. Commun. 1993; Koopman, DSN 2002 (supplementary); Ghemawat et al., SOSP 2003 (request item 13)

### 14.1 Castagnoli, Bräuer, Herrmann

**Citation.** Guy Castagnoli, Stefan Bräuer, Martin Herrmann. *Optimization of Cyclic Redundancy-Check Codes with 24 and 32 Parity Bits.* IEEE Transactions on Communications 41(6): 883–892, June 1993. doi:10.1109/26.231911.
**Access:** paywalled. We verified the metadata (Crossref) and the abstract (via OpenAlex, which mirrors IEEE's). **Everything beyond the abstract is UNVERIFIED from the primary text** and is taken from Koopman 2002 (§14.2), which re-derives and checks Castagnoli's results.

- **CBH93-F1: what the paper did** (abstract). The authors extend Fujiwara et al.'s method for computing minimum distance "to treat arbitrary shortened cyclic codes", run it on "a high-speed special-purpose processor", and investigate "several classes of cyclic redundancy-check (CRC) codes with 24 and 32 parity bits". For each class they identify the code whose minimum distance exceeds the class's guaranteed d_min "up to the largest block length", and compare the resulting d_min profiles with "the widely used 32 parity-bit standard code recommended in IEEE-802".

### 14.2 Koopman, DSN 2002 (supplementary; provides the verifiable numbers)

**Citation.** Philip Koopman. *32-Bit Cyclic Redundancy Codes for Internet Applications.* Proc. International Conference on Dependable Systems and Networks (DSN 2002), pp. 459–468. doi:10.1109/DSN.2002.1028931. We read the author's preprint.

- **Koo02-F1: an exhaustive search.** "The entire set of 1,073,774,592 distinct polynomials has been evaluated" [abstract/§1, PDF p. 1]. Standard CRC-32 (IEEE 802.3) achieves "a Hamming Distance (HD) of only 4 for maximum-length Ethernet messages, whereas HD=6 is possible" [abstract].
- **Koo02-F2: Castagnoli's polynomial was chosen for iSCSI** [§4, PDF p. 6]. A study during iSCSI's definition "recommends adoption of Castagnoli's {1,31} polynomial 0x8F6E37A0" (Koopman's notation). This is CRC-32C.
- **Koo02-F3: detection strength by message length** [Table 1, PDF p. 4; computed only up to 131,072 bits, i.e. 16 KiB].

  | Polynomial | HD=6 | HD=5 | HD=4 | HD=3 |
  |---|---|---|---|---|
  | IEEE 802.3 CRC-32 (0x82608EDB) | 172–268 bits | 269–2,974 | 2,975–91,607 | 91,608 up to the 131,072 computed |
  | CRC-32C / Castagnoli iSCSI (0x8F6E37A0) | 178–5,243 bits | — | 5,244 up to the 131,072 computed | — |

  So up to 16 KiB, CRC-32C guarantees detection of all 1–3-bit errors (HD=4) at every length. IEEE CRC-32 drops to HD=3 beyond 91,607 bits (about 11.2 KiB). **HD of CRC-32C at 32 KiB–64 KiB blocks: UNVERIFIED** here, because Table 1 stops at 131,072 bits.
- **Koo02-F4: a transcription error in the original** [§3, PDF p. 5]. One polynomial in Castagnoli's Table XI "is incorrectly given as 1F6ACFB13, but should have been 1F4ACFB13". That is a warning to copy polynomial constants from validated implementations, not by hand. (It does not affect CRC-32C.)
- **General CRC properties** (SP00 §1): any 32-bit CRC catches all bursts under 32 bits, and other errors slip through with probability about 2^−32. Ceph's reasons for CRC-32C are that it "is well-optimized on both x86 and ARM architectures, and it is sufficient for detecting random bit errors" [AWK+19 §5.1].

### 14.3 Ghemawat, Gobioff, Leung, The Google File System

**Citation.** Sanjay Ghemawat, Howard Gobioff, Shun-Tak Leung. *The Google File System.* SOSP '03, pp. 29–43, 2003. doi:10.1145/945445.945450.

- **GGL03-F1: why checksums** [§7, p. 42]. Disks "claimed to the Linux driver that they supported a range of IDE protocol versions but in fact responded reliably only to the more recent ones … This would corrupt data silently due to problems in the kernel. This problem motivated our use of checksums". Also, in Linux 2.2 the cost of fsync was "proportional to the size of the file rather than the size of the modified portion".
- **GGL03-F2: granularity and where checksums are stored** [§5.2, p. 38]. "A chunk is broken up into 64 KB blocks. Each has a corresponding 32 bit checksum." "checksums are kept in memory and stored persistently with logging, separate from user data." Each chunkserver checks its own copy, because comparing replicas is impractical and replicas can legitimately differ: "each chunkserver must independently verify the integrity of its own copy by maintaining checksums".
- **GGL03-F3: every read is verified** [§5.2]. The chunkserver verifies every block overlapping a read "before returning any data to the requester, whether a client or another chunkserver", so corruption does not spread. On a mismatch it returns an error and reports to the master. The reader goes to another replica, and the master clones a good replica and then deletes the bad one. "GFS client code further reduces this overhead by trying to align reads at checksum block boundaries."
- **GGL03-F4: checksums for appends and overwrites** [§5.2]. For appends, "We just incrementally update the checksum for the last partial checksum block" and compute fresh checksums for new blocks; a corrupt partial block will still fail on its next read. For overwrites, GFS "must read and verify the first and last blocks of the range being overwritten, then perform the write, and finally compute and record the new checksums". Otherwise the new checksums could hide existing corruption in the parts of those blocks not being overwritten.
- **GGL03-F5: idle scanning** [§5.2]. "During idle periods, chunkservers can scan and verify the contents of inactive chunks". This stops "an inactive but corrupted chunk replica from fooling the master into thinking that it has enough valid replicas of a chunk".
- **GGL03-F6: related design choices.**
  - 64 MB chunks; "Lazy space allocation avoids wasting space due to internal fragmentation" [§2.5, p. 31].
  - Operation log: respond "only after flushing the corresponding log record to disk both locally and remotely"; "The master batches several log records together before flushing" [§2.6.3, p. 32].
  - Record append is at-least-once, pads the chunk, and "is restricted to be at most one-fourth of the maximum chunk size" [§3.3, p. 35].
  - Deleted files are removed lazily, "if they have existed for more than three days" [§4.4, p. 36].
  - "the master maintains a chunk version number to distinguish between up-to-date and stale replicas" [§4.5, p. 37].
  - Re-replication priority favours chunks that have lost more replicas and chunks that block clients [§4.3].

### Implications for mantle

- **R13.1 CRC-32C everywhere a checksum is needed:** client-side, stored, and verified on read (CBH93; Koo02-F2, F3; AWK+19-F9). Use hardware instructions (SSE4.2/ARMv8 CRC32C are the reason Ceph gives for "well-optimized on both x86 and ARM", AWK+19-F9). *Inference:* CRC-32C is not collision-resistant against an adversary. Content addressing or deduplication keyed on it would need a cryptographic hash, which is outside the scope of this literature.
- **R13.2 Checksum block size.** Default 64 KiB for immutable chunk data (GGL03-F2), with 4 KiB alignment for I/O. Use smaller checksum blocks (4–16 KiB) where small random reads dominate, to avoid reading more than requested, and larger ones (128 KiB) only for write-once, large-read data (AWK+19-F9). Clients align reads to checksum-block boundaries (GGL03-F3). *Inference:* at 64 KiB blocks, a 32-bit checksum costs 64 MiB of checksum metadata per TiB of data, versus 1 GiB per TiB at 4 KiB (the same arithmetic as AWK+19's figure of 10 GiB of checksums for 10 TiB of data).
- **R13.3 Where checksums live.** Keep them *separately* from data, in the per-device metadata log with a memory cache (GGL03-F2), *and* inline in a self-identifying block trailer (chunk id, generation, block index, CRC-32C) (BGS+08 §2.2.1; AGL+18-F4). The out-of-band copy catches misdirected writes that corrupt data and inline checksum together. The inline copy lets scrubbing and recovery work without the index.
- **R13.4 Appends and overwrites.** Use GFS's incremental rule for the partial last block. If a chunk is ever modified in place, verify the first and last blocks of the range before writing and recording new checksums (GGL03-F4). *Inference:* prefer immutable, sealed chunks, which make this case rare.
- **R13.5 Repair workflow.** On a mismatch: return an error, report to the metadata service, read from another replica, re-replicate from a verified source, then delete or quarantine the bad copy (GGL03-F3; GAA17-F7). Verify cold chunks during idle time (GGL03-F5) in addition to the §10 cadence.
- **R13.6 Stale replicas.** Give every chunk a version or generation number that is bumped on each new write lease or append epoch. Storage nodes report their (chunk, generation) pairs, and stale copies are garbage-collected (GGL03-F6; RO92-F3).
- **R13.7 Lazy allocation and delayed reclamation.** Allocate chunk space lazily (GGL03-F6). *Inference:* on raw devices, extend within the current segment. Keep deleted data for a grace period before reclaiming it; GFS used 3 days by default (GGL03-F6). That gives a safety net against accidental deletion, at a known capacity cost.

---

## 15. Consolidated rules for mantle's chunk store

These rules combine the per-paper implications above. Each one names the findings it depends on. **Paper-stated** means a paper states it directly or measured it. **Inference** means it is our synthesis. Numbers marked *starting value* are defaults to validate on real hardware, not settled facts.

### 15.1 On-disk record layout

- **L1. The device is a log of segments** (paper-stated principle: RO92-F2, BAH+21-F2, AWK+19-F3; the mantle mapping is inference). Device layout:
  - two alternating, checksummed superblocks/checkpoint regions, each with a trailing sequence number (RO92-F9), placed far apart (BGS+08-F9);
  - a separate, preallocated region for the metadata/index log (AGL+18-F2, F4; AWK+19-F6);
  - everything else is data segments that are written sequentially and freed or reset as a unit.
- **L2. Every segment describes itself.** A header records segment id, device id, segment generation, lifetime class and open time. Periodic *summary* records list which chunk and generation owns each block, so cleaning, scrubbing and roll-forward work without the index (RO92-F3, F8, F9).
- **L3. Every data block describes itself.** Each checksum block (sized per C2) carries a trailer: `{chunk_id, chunk_generation, block_index, payload_len, flags, crc32c}`. The CRC covers the payload *and* the identity fields. It catches bit rot, torn writes, and misdirected writes (the identity will not match the location) (BGS+08 §2.2.1, F6; GGL03-F2; SRC84-F5).
- **L4. A second copy of the identity, stored elsewhere.** The per-device index log holds `(chunk_id, generation) → [(segment, offset, len, crc32c)]` in a different region, written in different I/Os. This detects a *lost* write, where a stale but self-consistent block is left in place, and a misdirected write that damaged both data and trailer (AGL+18-F4: "a misdirected write can corrupt both the item and its identifier"; BGS+08-F7, F9). The metadata service keeps a whole-chunk digest for an end-to-end check (SRC84-F2) *(inference)*.
- **L5. Journal records are self-delimiting**: `{lsn, type, len, payload, crc32c}`, 4 KiB-padded per group-commit batch, followed by a persist/commit record. That makes it possible to tell a crashed tail from corruption (AGL+18-F3; PCA+14-F4; HKA17-F7).
- **L6. Metadata is compact.** Varint/delta encoding, no per-4 KiB metadata rows for large chunks, and allocator memory fixed per TB (AWK+19-F8, F13).

### 15.2 Alignment

- **A1.** All I/O offsets and lengths on SSDs are multiples of 4 KiB. Nothing smaller is ever issued (HL23-F4, paper-stated).
- **A2.** Segment boundaries are aligned to zone/band boundaries on zoned devices (BAH+21-F2; AD15-F1), and to large power-of-two boundaries on conventional SSDs so that writes are aligned and sequential (HKA17 rule 3) *(inference: at least 64 MiB, see L1/R3.1)*.
- **A3.** Chunk data inside a segment starts at checksum-block boundaries, so readers can align reads to checksum blocks (GGL03-F3).

### 15.3 Write sizes

- **W1.** Stream data appends in large I/Os and keep many in flight (HKA17 rule 1; HL23-F3). *Starting value (inference):* 256 KiB–1 MiB per write I/O on NVMe and 1–8 MiB on HDD.
- **W2.** Writes below 16 KiB (SSD) or 64 KiB (HDD) are *not* written in place. Pack them into the log or into larger extents (AWK+19-F7 thresholds; paper-stated for BlueStore).
- **W3.** On drive-managed SMR, keep every write stream sequential in pieces of at least 8 MiB, and never interleave random writes with it (ATGD17-F2; paper-stated for one drive model).
- **W4.** Pad the partial last page of a group-commit batch rather than rewriting a partial page later (HKA17-F7; BAH+21-F5).

### 15.4 Queue depths per device class

| Device class | Target outstanding I/Os | Evidence / status |
|---|---|---|
| NVMe SSD (PCIe 4) | ≥ 128 per device under load; 256–512 per device to saturate | HL23-F3 (more than 100 per device "decent", 3000 per 8 devices to saturate); paper-stated for reads, **write QD not studied** |
| NVMe, per core | one io_uring per core, IOPOLL, all-to-all to devices | HL23-F8, F10; DPI+22-F3 |
| SATA SSD | ≤ 32 (NCQ limit) | HKA17 §3.1 |
| CMR HDD | **NOT ADDRESSED** by the reviewed literature; *inference:* up to the NCQ limit of 32, tuned by measurement | UNVERIFIED |
| DM-SMR HDD | use a high QD (Skylight's QD 31 behaved like cache-enabled) *or* enable the volatile cache | AD15-F2 and §4.6 footnote |
| ZNS SSD | writes at the write pointer. *Inference:* one outstanding write per open zone without Zone Append; parallelism comes from several open zones | BAH+21-F2; Zone Append **UNVERIFIED** |

### 15.5 Direct vs buffered I/O

- **D1.** Use direct I/O (`O_DIRECT` or raw block device) for all chunk data, journals and index logs, with mantle's own scan-resistant cache (2Q-like) and no kernel readahead; clients do their own readahead (HL23-F13; AWK+19-F4, F5, F10; HKA17-F4; paper-stated).
- **D2.** Direct I/O still needs explicit device flushes: "Calls to fsync are still required since data may be cached within the underlying storage media" (RPA+20-F4). *Inference:* on raw devices, issue a flush/FUA per group commit, for example `fdatasync` on the block device, `RWF_DSYNC`, or an io_uring fsync. Validate the exact mechanism per kernel and device.
- **D3.** Buffered I/O is acceptable only off the durability path (logs for debugging, configuration read at startup) (RPA+20-F1, F3).

### 15.6 fsync / group-commit strategy

- **G1.** One group-commit loop per device. The loop's batch is the set of records that accumulated while the previous flush was in flight. Acknowledge only after the flush completes *and* the required replicas have acknowledged (DKO+84-F2, F3; GGL03-F6; SRC84-F4).
- **G2.** At most one data flush plus one index-log flush per batch; never a journal inside a journaling file system (AWK+19-F1, F6).
- **G3.** Flushes are isolated per device, so one device's flush never waits for unrelated data (PCA+14-F8; AWK+19-F1 on `sync` vs `syncfs`).
- **G4.** A flush error is fatal for that device's in-memory state: fail the batch, fence the device, restart its store instance, recover from mantle's own log, and repair from peers. **Never retry the fsync** (RPA+20-F1–F4).
- **G5.** Do not trust a single device's flush acknowledgement for durability. Rely on synchronous replication across failure domains, plus identity and generation checks to detect lost writes (BGS+08-F7).
- **G6.** Delay timers: *starting value:* 0 (no artificial wait). HSL+87's analysis of timers is UNVERIFIED here. Add an adaptive delay only if measurements show the device's flush rate is the bottleneck (DKO+84-F3).

### 15.7 Preallocation

- **P1.** Preallocate fixed-size journal and index-log regions, and reuse them circularly (AGL+18-F2; AWK+19-F6 "reuse WAL files as a circular buffer" / "a preallocated pool of WAL files"). A fixed size also makes size faults detectable (AGL+18-F2).
- **P2.** Allocate data space for chunks lazily, extending within the current open segment. Do not reserve a full maximum-size chunk up front (GGL03-F6).
- **P3.** *Inference:* if mantle must run on files instead of raw devices, preallocate large files and write every byte once up front, so steady-state appends overwrite already-allocated blocks. PCA+14-F1 found single-sector overwrites atomic on all file systems tested, while appends needed extra machinery. **The exact behaviour of `fallocate` unwritten extents under `O_DIRECT` is NOT ADDRESSED by the reviewed literature (UNVERIFIED).**

### 15.8 Checksum granularity

- **C1.** Algorithm: CRC-32C (CBH93; Koo02-F2, F3; AWK+19-F9). Never Adler-32 or other weak sums (GAA17-F3; SP00-F4).
- **C2.** Checksum blocks: *starting value* 64 KiB for sealed, immutable chunk data (GGL03-F2). 4–16 KiB for classes dominated by small random reads. Up to 128 KiB for write-once, large-read data (AWK+19-F9). HD guarantees for CRC-32C are verified up to 16 KiB (HD=4); at 64 KiB they are UNVERIFIED in this literature, and the general CRC miss probability of about 2^−32 applies (SP00-F4; Koo02-F3).
- **C3.** Also checksum every journal record, index-log record, superblock copy, and 4 KiB metadata-snapshot chunk (AGL+18-F2, F7).
- **C4.** The client computes checksums before sending, the storage node verifies them before acknowledging, the node verifies them again on every read, and the client verifies end to end (SP00-F5; SRC84-F2; GGL03-F3; AWK+19-F9).

### 15.9 Scrubbing cadence

- **S1.** Full data scrub (read plus CRC plus identity check) at least every 14 days (hard limit), target 7 days. Metadata is scrubbed daily (BGPS07-F5; BGS+08 §2.2.2; AWK+19-F9; SDG10-F2).
- **S2.** Use a staggered order with 128 MiB regions and 1 MiB segments. Segments must stay below ¼–½ of the region size (SDG10-F2, F3).
- **S3.** Run at low priority, throttled against the p99 read-latency target. Also verify cold chunks during idle time (BGPS07-F5; GGL03-F5).
- **S4.** After any error, scan the neighbourhood (±10 MiB and adjacent blocks) immediately, and scrub the whole device ahead of schedule (BGPS07-F3, F4; BGS+08-F2, F3, F9).

### 15.10 Corruption handling

- **X1.** `EIO` and a CRC or identity mismatch are handled the same way: the block is bad. Serve the read from another replica or EC fragment, rebuild from a *verified* source, and never crash-loop the node (GAA17-F4, F7, F8; GGL03-F3).
- **X2.** Journal recovery tells crash from corruption. A bad tail with no persist record means a crash, and the tail is discarded. A bad record that has a persist record, or a valid record after it, is corruption: mark it and repair it. Never truncate valid later records (AGL+18-F3; GAA17-F6).
- **X3.** Metadata Raft: follow CTRL (AGL+18-F2–F7; R7.1).
- **X4.** Device risk escalation. Mark a device "at risk" after its first error, speed up repair of chunks whose other copies are on at-risk devices, and drain or retire a device after:
  - any identity discrepancy (a lost or misdirected write);
  - a first corruption on an enterprise-class drive;
  - repeated uncorrectable errors on an SSD, or more than two bad blocks.

  Evidence: BGPS07-F7; BGS+08-F6, F9; SLM16-F3, F4.
- **X5.** Placement diversity. Keep replicas and EC fragments off the same drive model behind the same controller or shelf where possible, and at different physical offsets (BGS+08-F4, F8, F9) *(inference for mantle)*.
- **X6.** Test with fault injection covering every on-disk structure and every role, and with crash-state exploration (GAA17-F9; PCA+14-F9; AGL+18-F8).

### 15.11 Compaction and space-reclamation policy

- **K1.** Prefer data that dies whole: group chunks by expected death time (lifecycle/TTL class, tenant, write epoch) into the same segment or zone, so segments empty out and are reclaimed without copying (HKA17 rule 4, F9, F12; RO92-F10; BAH+21-F5).
- **K2.** Pick victims by cost-benefit, `(1−u)·age/(1+u)`, using a segment usage table (live bytes, youngest-data age). Sort surviving data by age or death-time class when rewriting (RO92-F7, F8).
- **K3.** Watermarks: start below a low number of free segments, clean in batches, stop at a high watermark. Keep a separate reserve for repair and re-replication writes (RO92-F5) *(reserve is inference)*.
- **K4.** Account for write amplification of 2/(1−u) when setting per-device target utilization (RO92-F4).
- **K5.** Issue discards or zone resets as soon as a segment is fully dead, batched per segment (HKA17-F5; BAH+21-F5).
- **K6.** Throttle compaction and re-replication writes against the read-latency target (HL23-F12).
- **K7.** Keep deleted chunks for a grace period (a safety net) before reclaiming them. GFS used 3 days (GGL03-F6).

### 15.12 SMR and ZNS friendliness

- **Z1.** Use the same sequential segment abstraction for every device type. Map it to ZNS zones (at zone capacity), HM-SMR zones (256 MiB), and regions on CMR/SSD (BAH+21-F2; AWK+19-F3).
- **Z2.** No in-place updates on zoned devices. Per-device metadata lives in log zones, or on a small conventional region or device (ATGD17-F4; BAH+21-F5, F6).
- **Z3.** Open-zone budget: at or below the device's active-zone limit (8–32 expected, 14 on the evaluated SSD). *Starting value:* 6–12 open zones per ZNS device (BAH+21-F3, F5).
- **Z4.** Extents never span zones. Finish a zone below a remaining-capacity threshold (e.g. 5%). Expect about 10% space amplification (BAH+21-F5).
- **Z5.** Place chunks into zones by lifetime ("lifetime of the file is less than the oldest data stored in the zone") (BAH+21-F5).
- **Z6.** Drive-managed SMR: only large sequential writes (at least 8 MiB), no interleaved random writes, keep idle time for the drive's cleaning, volatile cache enabled plus explicit flushes. Prefer HM-SMR (AD15-F2; ATGD17-F2).
- **Z7.** Zone Append–based concurrency is out of scope until separately evidenced (UNVERIFIED).

### 15.13 Starting parameters

| Parameter | Starting value | Basis |
|---|---|---|
| Minimum I/O and alignment (SSD) | 4 KiB | HL23-F4 |
| NVMe outstanding I/Os per device | 128 (load), up to 512 (saturation) | HL23-F3 |
| io_uring mode | IOPOLL, per-core ring; SQPOLL off | HL23-F10; DPI+22-F4 |
| Small-write deferral threshold | 16 KiB SSD / 64 KiB HDD | AWK+19-F7 |
| DM-SMR minimum sequential write | 8 MiB | ATGD17-F2 (one model) |
| Checksum algorithm | CRC-32C | CBH93; Koo02; AWK+19-F9 |
| Data checksum block | 64 KiB (4–128 KiB per class) | GGL03-F2; AWK+19-F9 |
| Scrub interval (data / metadata) | 7 days / 1 day (hard limit 14 days for data) | AWK+19-F9; BGPS07-F5; SDG10 |
| Scrub order | staggered, 128 MiB regions × 1 MiB segments | SDG10-F2, F3 |
| Post-error neighbourhood scan | ±10 MiB + adjacent blocks, immediately | BGPS07-F3; BGS+08-F2 |
| "At-risk" device window after an error | 30 days | BGPS07-F4 (most further errors within a month); SLM16-F4 |
| ZNS open zones | 6–12 (≤ device limit) | BAH+21-F5 |
| ZNS zone finish threshold | 5% remaining | BAH+21-F5 |
| Cleaning victim score | (1−u)·age/(1+u) | RO92-F7 |
| Group-commit delay timer | 0 (adaptive later) | DKO+84-F3; HSL+87 UNVERIFIED |
| Deleted-chunk grace period | 3 days (configurable) | GGL03-F6 |

---

## 16. Bibliography

Page ranges and DOIs are as printed in the PDFs or as returned by Crossref, which we cross-checked.

1. **[HL23]** Gabriel Haas, Viktor Leis. What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance I/O for High-Performance Storage Engines. *PVLDB* 16(9): 2090–2102, 2023. doi:10.14778/3598581.3598584. PDF: https://www.vldb.org/pvldb/vol16/p2090-haas.pdf
2. **[DPI+22]** Diego Didona, Jonas Pfefferle, Nikolas Ioannou, Bernard Metzler, Animesh Trivedi. Understanding Modern Storage APIs: A Systematic Study of libaio, SPDK, and io_uring. *SYSTOR '22*, Haifa, 2022. doi:10.1145/3534056.3534945. PDF: https://atlarge-research.com/pdfs/2022-systor-apis.pdf
3. **[HKA17]** Jun He, Sudarsun Kannan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. The Unwritten Contract of Solid State Drives. *EuroSys '17*, pp. 127–144, 2017. doi:10.1145/3064176.3064187. PDF: https://research.cs.wisc.edu/adsl/Publications/eurosys17-he.pdf
4. **[AWK+19]** Abutalib Aghayev, Sage Weil, Michael Kuchnik, Mark Nelson, Gregory R. Ganger, George Amvrosiadis. File Systems Unfit as Distributed Storage Backends: Lessons from 10 Years of Ceph Evolution. *SOSP '19*, pp. 353–369, 2019. doi:10.1145/3341301.3359656. PDF: https://www.pdl.cmu.edu/PDL-FTP/Storage/ceph-exp-sosp19.pdf
5. **[PCA+14]** Thanumalayan Sankaranarayana Pillai, Vijay Chidambaram, Ramnatthan Alagappan, Samer Al-Kiswany, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. All File Systems Are Not Created Equal: On the Complexity of Crafting Crash-Consistent Applications. *OSDI '14*, pp. 433–448, 2014. PDF: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf
6. **[GAA17]** Aishwarya Ganesan, Ramnatthan Alagappan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. Redundancy Does Not Imply Fault Tolerance: Analysis of Distributed Storage Reactions to Single Errors and Corruptions. *FAST '17*, pp. 149–166, 2017. PDF: https://www.usenix.org/system/files/conference/fast17/fast17-ganesan.pdf
7. **[AGL+18]** Ramnatthan Alagappan, Aishwarya Ganesan, Eric Lee, Aws Albarghouthi, Vijay Chidambaram, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. Protocol-Aware Recovery for Consensus-Based Storage. *FAST '18*, pp. 15–31, 2018. PDF: https://www.usenix.org/system/files/conference/fast18/fast18-alagappan.pdf
8. **[RO92]** Mendel Rosenblum, John K. Ousterhout. The Design and Implementation of a Log-Structured File System. *ACM TOCS* 10(1): 26–52, 1992. doi:10.1145/146941.146943. Preprint read: https://people.eecs.berkeley.edu/~brewer/cs262/LFS.pdf
9. **[BGPS07]** Lakshmi N. Bairavasundaram, Garth R. Goodson, Shankar Pasupathy, Jiri Schindler. An Analysis of Latent Sector Errors in Disk Drives. *SIGMETRICS '07*; *ACM SIGMETRICS PER* 35(1): 289–300, 2007. doi:10.1145/1269899.1254917. PDF: https://research.cs.wisc.edu/adsl/Publications/latent-sigmetrics07.pdf
10. **[BGS+08]** Lakshmi N. Bairavasundaram, Garth R. Goodson, Bianca Schroeder, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. An Analysis of Data Corruption in the Storage Stack. *FAST '08*, pp. 223–238, 2008. PDF: https://www.usenix.org/legacy/event/fast08/tech/full_papers/bairavasundaram/bairavasundaram.pdf
11. **[SDG10]** Bianca Schroeder, Sotirios Damouras, Phillipa Gill. Understanding Latent Sector Errors and How to Protect Against Them. *FAST '10*, 2010 (page range UNVERIFIED). Extended version: *ACM TOS* 6(3), 2010, doi:10.1145/1837915.1837917. PDF: https://www.usenix.org/legacy/event/fast10/tech/full_papers/schroeder.pdf
12. **[SLM16]** Bianca Schroeder, Raghav Lagisetty, Arif Merchant. Flash Reliability in Production: The Expected and the Unexpected. *FAST '16*, pp. 67–80, 2016. PDF: https://www.usenix.org/system/files/conference/fast16/fast16-papers-schroeder.pdf
13. **[SP00]** Jonathan Stone, Craig Partridge. When the CRC and TCP Checksum Disagree. *SIGCOMM 2000*, pp. 309–319. doi:10.1145/347059.347561. PDF: https://conferences.sigcomm.org/sigcomm/2000/conf/paper/sigcomm2000-9-1.pdf
14. **[SRC84]** J. H. Saltzer, D. P. Reed, D. D. Clark. End-to-End Arguments in System Design. *ACM TOCS* 2(4): 277–288, 1984. doi:10.1145/357401.357402. PDF: https://web.mit.edu/Saltzer/www/publications/endtoend/endtoend.pdf
15. **[BAH+21]** Matias Bjørling, Abutalib Aghayev, Hans Holmberg, Aravind Ramesh, Damien Le Moal, Gregory R. Ganger, George Amvrosiadis. ZNS: Avoiding the Block Interface Tax for Flash-based SSDs. *USENIX ATC '21*, pp. 689–703, 2021. PDF: https://www.usenix.org/system/files/atc21-bjorling.pdf
16. **[AD15]** Abutalib Aghayev, Peter Desnoyers. Skylight—A Window on Shingled Disk Operation. *FAST '15*, pp. 135–149, 2015. PDF: https://www.usenix.org/system/files/conference/fast15/fast15-paper-aghayev.pdf
17. **[ATGD17]** Abutalib Aghayev, Theodore Ts'o, Garth Gibson, Peter Desnoyers. Evolving Ext4 for Shingled Disks. *FAST '17*, pp. 105–120, 2017. PDF: https://www.usenix.org/system/files/conference/fast17/fast17-aghayev.pdf
18. **[DKO+84]** David J. DeWitt, Randy H. Katz, Frank Olken, Leonard D. Shapiro, Michael R. Stonebraker, David Wood. Implementation Techniques for Main Memory Database Systems. *SIGMOD '84*, pp. 1–8 (also *SIGMOD Record* 14(2)), 1984. doi:10.1145/971697.602261. Scan read: https://15721.courses.cs.cmu.edu/spring2016/papers/p1-dewitt.pdf
19. **[HSL+87]** Pat Helland, Harald Sammer, Jim Lyon, Richard Carr, Phil Garrett, Andreas Reuter. Group Commit Timers and High Volume Transaction Systems. *HPTS 1987*; LNCS 359, pp. 301–329, Springer, 1989. doi:10.1007/3-540-51085-0_52. **Content not accessed (paywalled).**
20. **[CBH93]** Guy Castagnoli, Stefan Bräuer, Martin Herrmann. Optimization of Cyclic Redundancy-Check Codes with 24 and 32 Parity Bits. *IEEE Transactions on Communications* 41(6): 883–892, 1993. doi:10.1109/26.231911. **Abstract only accessed.**
21. **[GGL03]** Sanjay Ghemawat, Howard Gobioff, Shun-Tak Leung. The Google File System. *SOSP '03*, pp. 29–43, 2003. doi:10.1145/945445.945450. PDF: https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf
22. **[Koo02]** (supplementary) Philip Koopman. 32-Bit Cyclic Redundancy Codes for Internet Applications. *DSN 2002*, pp. 459–468. doi:10.1109/DSN.2002.1028931. Preprint read: https://users.ece.cmu.edu/~koopman/networks/dsn02/dsn02_koopman.pdf
23. **[RPA+20]** (supplementary) Anthony Rebello, Yuvraj Patel, Ramnatthan Alagappan, Andrea C. Arpaci-Dusseau, Remzi H. Arpaci-Dusseau. Can Applications Recover from fsync Failures? *USENIX ATC '20*, pp. 753–767, 2020. PDF: https://www.usenix.org/system/files/atc20-rebello.pdf

---

## 17. What is UNVERIFIED, NOT ADDRESSED, or open

1. **Helland et al. 1987 (group-commit timers):** content not accessed (paywalled). No claims about timer policy or its measured effect are made. Group-commit throughput claims rest solely on DeWitt et al. 1984, which we verified.
2. **Castagnoli et al. 1993:** only the abstract was verified. The Hamming-distance figures for CRC-32C come from Koopman 2002, Table 1, which stops at 131,072 bits (16 KiB). **CRC-32C's HD at 32–64 KiB checksum blocks is UNVERIFIED** in this literature set.
3. **NVMe ZNS Zone Append:** not covered by Bjørling et al. 2021 or any other paper here. Designs that depend on it need their own evidence (spec plus measurement).
4. **Write-path queue depths and HDD (CMR) queue depths:** not measured by the papers reviewed. HL23 and DPI+22 queue-depth data are for random reads.
5. **Blocking thread pool vs async I/O at HDD scale:** not addressed (DPI+22 did not test synchronous I/O; HL23 shows it fails at NVMe-array scale).
6. **BlueStore's default checksum block size:** not stated in AWK+19.
7. **Linux flush semantics on raw block devices under `O_DIRECT` (FUA vs flush, io_uring fsync), and `fallocate` unwritten-extent behaviour:** not addressed by the reviewed literature. RPA+20 only establishes that fsync is still needed with direct I/O and that retrying a failed fsync is unsafe.
8. **Modern TLC/QLC SSD field reliability:** SLM16 covers MLC/eMLC/SLC from 2010–2015. Numbers may differ for current drives.
9. **Field reliability data sets are old:** NetApp 2004–2007-era HDDs (BGPS07, BGS+08, SDG10). The *patterns* (spatial and temporal clustering, value of scrubbing, identity checks) are the durable lessons; the absolute rates are not.
10. **HL23's CPU description is inconsistent:** "64-core AMD Zen 4" in §2 versus "AMD EPYC 7713 Milan" (Zen 3) in §4.1. HL23 also spells the NVMe module parameter "nvme.poll_queue"; the Linux parameter is `poll_queues`. Confirm this against kernel documentation.
11. **ZNS paper typo:** the block SSDs "sustain target writes up to 300MiB/s (0% OP)" (§5.1) conflicts with the 7%/28% OP configurations described elsewhere. We treat it as 7% OP.
12. **All starting values in §15.13 are inferences**, to be validated on mantle's target hardware with steady-state (preconditioned) benchmarks.
