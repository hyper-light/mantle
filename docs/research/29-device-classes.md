# 29 — Device classes: how each kind of storage wants to be written and read, what it costs in power, and what wears it

**Status:** research input for `crates/disk` (identity and calibration), the chunk store's writer,
cleaner and scrubber (`docs/design/chunk-store.md`), and the write planner. This is not a
decision record; §9 proposes decisions for `docs/design/`.
**Compiled:** 2026-09-30.
**Scope:** for each class of device mantle will meet — flash SSDs on NVMe and SATA, zoned and
placement-aware flash, CMR and SMR disks, laptop and phone flash, USB and Thunderbolt attachments,
SD/eMMC/UFS, cloud block devices, RAID and volume managers, persistent memory, network file
systems and tape — how it prefers to be written and read, what it costs in power, what wears it,
which of these the operating system can report and which mantle must measure, and how to measure
them without wearing the device or draining a laptop's battery.

This note is the device-physics foundation under three sibling notes written at the same time:
note 26 (concurrency: how many I/Os are in flight and on how many threads), note 27 (upload
scheduling) and note 28 (storage classes, power-aware writes, longevity). It cites them and does
not repeat them. It also extends, and does not repeat, these earlier notes:

| Earlier note | What it already covers, which this note only cites |
|---|---|
| 02 | Finding the device under a path on Linux, macOS and Windows; every OS query and its privilege; the durable-write protocols (`fdatasync`, `F_FULLFSYNC`, `FlushFileBuffers`, `RWF_DSYNC`, FUA); device nodes; the calibration probes of §6.3 |
| 03 | HKA17's five rules and observations; HL23 queue depths and io_uring; ZNS (BAH+21) and SMR (Skylight, ext4-lazy); group commit; checksums; the consolidated chunk-store rules R3.x, R11.x, W1–W4, G1–G6 |
| 10 | Field failure studies of disks and SSDs (PWB07, SG07, BGPS07, SLM16, MWKM15, NWJ+16, MMES20/22, XZQ+19, LXZ+22); fail-slow; fullness and write amplification (Des12, DIKS20); GC tail latency (TTFlash, Rails, ReFlex, Gimbal, IODA); NVMe SMART, thermal telemetry, Endurance Group log; rules R1–R12 |
| 11 | Models for every chunk-store constant: group-commit wait, batch limits, segment size, checksum block, cleaner watermarks, scrub period and pacing, calibration rounds |
| 21 | Benchmark rounds that form more than one state (dip test, changepoints) |
| 26 | Device I/O in flight on each OS; the bounded worker pool; the process thread budget; stepped complexity of threads |

---

## 0. How to read this note

**Citation tags.** `[KEY §section, p. N]` for papers, with `p.` the printed page where the copy
has one and "PDF p. N" otherwise. Specifications are cited by section, figure and printed page.
Source files are cited as `[LINUX path:line]` at the revision in the Sources table. Earlier
notes are "note 03 §x".

**Quotes.** Verbatim from the text layer of the fetched copy (`pdftotext`), with ligatures
restored, line-break hyphenation joined, and "..." for an elision.

**Evidence labels.**
- *(no label)*: a peer-reviewed paper, checked against its text.
- **primary**: a specification, kernel source, man page, or a vendor's own reference
  documentation (AWS, Microsoft, Google), read directly.
- **NON-PEER-REVIEWED**: a vendor datasheet, white paper, blog or trade-press report.
- **MEASURED**: read on the development machine on 2026-09-30 with read-only queries (`ioreg`,
  `sysctl`, `man`, `powermetrics -h`). No mantle code was built or run for this note.
- **DERIVED**: arithmetic on stated facts, shown.
- **INFERENCE**: reasoning in this note that the sources do not state.
- **UNVERIFIED**: not confirmed against a primary text.

**Method.** Every source was fetched on 2026-09-30: papers as PDFs from USENIX, arXiv, the
authors' sites, IBM Research and the IEEE S&P poster site; NVMe specifications from
nvmexpress.org; Linux files from `raw.githubusercontent.com/torvalds/linux/master` at commit
`551c722f40809618230001baccf219193e22fc5a` (committed 2026-09-30T01:54:21Z); AWS, Microsoft and
Google documentation from their documentation sites. Where only an abstract, slides or a summary
could be read, the Sources table says so and every claim from it is labelled.

**Era.** Device numbers age fast. Each number below carries the device generation it was
measured on. A number from 2008 hardware (Seo08, Agr08) is used for its *shape* (what grows with
what), never as a value for a drive mantle will meet; values come from mantle's own measurement
(§10) or from the device's own report.

---

## Sources

### Flash: devices, FTLs, placement

| Key | Source | Label | Where read |
|---|---|---|---|
| **Agr08** | Nitin Agrawal, Vijayan Prabhakaran, Ted Wobber, John D. Davis, Mark Manasse, Rina Panigrahy. "Design Tradeoffs for SSD Performance." *USENIX ATC '08*, pp. 57–70. | peer-reviewed | usenix.org legacy PDF |
| **Cai17** | Yu Cai, Saugata Ghose, Erich F. Haratsch, Yixin Luo, Onur Mutlu. "Error Characterization, Mitigation, and Recovery in Flash-Memory-Based Solid-State Drives." *Proceedings of the IEEE* 105(9), September 2017. | peer-reviewed | arXiv:1706.08642v3 (the authors' copy; no printed pages; cited by section) |
| **Chen11** | Feng Chen, Rubao Lee, Xiaodong Zhang. "Essential Roles of Exploiting Internal Parallelism of Flash Memory based Solid State Drives in High-Speed Data Processing." *HPCA 2011*, pp. 266–277. | peer-reviewed | author copy, homes.luddy.indiana.edu (no printed pages; cited by section) |
| **Hu09** | Xiao-Yu Hu, Evangelos Eleftheriou, Robert Haas, Ilias Iliadis, Roman Pletka. "Write Amplification Analysis in Flash-Based Solid State Drives." *SYSTOR 2009*. | peer-reviewed; **only the authors' conference slides were read** (systor.org/2009/papers/2_2_2.pdf); the paper's text was not | slides |
| **Zheng13** | Mai Zheng, Joseph Tucek, Feng Qin, Mark Lillibridge. "Understanding the Robustness of SSDs under Power Fault." *FAST '13*, pp. 271–284. | peer-reviewed | usenix.org PDF |
| **Kang14** | Jeong-Uk Kang, Jeeseok Hyun, Hyunjoo Maeng, Sangyeun Cho. "The Multi-streamed Solid-State Drive." *HotStorage '14*. | peer-reviewed workshop | usenix.org PDF |
| **LNVM17** | Matias Bjørling, Javier González, Philippe Bonnet. "LightNVM: The Linux Open-Channel SSD Subsystem." *FAST '17*, pp. 359–374. | peer-reviewed | usenix.org PDF |
| **FDP25** | Michael Allison, Arun George, Javier Gonzalez, Dan Helmick, Vikash Kumar, Roshan R Nair, Vivek Shah. "Towards Efficient Flash Caches with Emerging NVMe Flexible Data Placement SSDs." *EuroSys '25*. doi:10.1145/3689031.3696091 | peer-reviewed | arXiv:2503.11665v1 (no printed pages; cited by section) |
| **QLC-IU** | AnandTech, "Solidigm Announces D5-P5336" (anandtech.com/show/18967) and architecting.it on the same drive: the D5-P5336 has a 16 KB indirection unit, its predecessor D5-P5316 64 KB. | **NON-PEER-REVIEWED** (trade press reporting a vendor statement); the vendor's own document was not found | search results; values used only as an example of what NPWG may report |
| **JESD218** | JEDEC JESD218, *Solid-State Drive (SSD) Requirements and Endurance Test Method*. | **UNVERIFIED**: the standard's text was not read (JEDEC requires an account); the class retention conditions below come from secondary summaries | — |

### Energy

| Key | Source | Label | Where read |
|---|---|---|---|
| **Seo08** | Euiseong Seo, Seon Yeong Park, Bhuvan Urgaonkar. "Empirical Analysis on Energy Efficiency of Flash-based SSDs." *HotPower '08*. | peer-reviewed workshop | usenix.org legacy PDF (no printed pages) |
| **HA20** | Bryan Harris, Nihat Altiparmak. "Ultra-Low Latency SSDs' Impact on Overall Energy Efficiency." *HotStorage '20*. | peer-reviewed workshop | usenix.org PDF |
| **Zed03** | John Zedlewski, Sumeet Sobti, Nitin Garg, Fengzhou Zheng, Arvind Krishnamurthy, Randolph Wang. "Modeling Hard-Disk Power Consumption." *FAST '03*. | peer-reviewed | usenix.org legacy PDF |
| **Nar08** | Dushyanth Narayanan, Austin Donnelly, Antony Rowstron. "Write Off-Loading: Practical Power Management for Enterprise Storage." *FAST '08*, pp. 253–267. | peer-reviewed | usenix.org legacy PDF |

### Disks, tape, other media

| Key | Source | Label | Where read |
|---|---|---|---|
| **RW94** | Chris Ruemmler, John Wilkes. "An Introduction to Disk Drive Modeling." *IEEE Computer* 27(3): 17–28, March 1994. | peer-reviewed | CMU course copy of the HP Labs preprint (cited by preprint page) |
| **WGP94** | Bruce L. Worthington, Gregory R. Ganger, Yale N. Patt. "Scheduling Algorithms for Modern Disk Drives." *SIGMETRICS '94*, pp. 241–251. | peer-reviewed; **abstract only** (users.ece.cmu.edu/~ganger/papers/sigmetrics94_abs.html) | abstract |
| **Blue18** | Connor Bolton, Sara Rampazzi, Chaohao Li, Andrew Kwong, Wenyuan Xu, Kevin Fu. "Blue Note: How Intentional Acoustic Interference Damages Availability and Integrity in Hard Disk Drives and Operating Systems." IEEE S&P 2018. | **poster abstract only** (ieee-security.org/TC/SP2018/poster-abstracts/oakland2018-paper46-poster-abstract.pdf) | poster abstract |
| **Kim12** | Hyojun Kim, Nitin Agrawal, Cristian Ungureanu. "Revisiting Storage for Smartphones." *FAST '12*. | peer-reviewed | usenix.org PDF (cited by section) |
| **Yang20** | Jian Yang, Juno Kim, Morteza Hoseinzadeh, Joseph Izraelevitz, Steven Swanson. "An Empirical Guide to the Behavior and Use of Scalable Persistent Memory." *FAST '20*, pp. 169–182. | peer-reviewed | usenix.org PDF |
| **T10-EPC** | Gerry Houlder (Seagate). T10/08-184r3, "SPC-4 SBC-3 Adding more low power options," 2008 (the proposal that defined the SCSI idle_a/b/c and standby_y/z conditions). | **primary** (standards proposal) | t10.org |
| **TP608** | Seagate, "PowerChoice Technology Provides Unprecedented Hard Drive Power Savings and Flexibility," technology paper TP608 (Traditional Chinese edition; the tables are in English). | **NON-PEER-REVIEWED** | seagate.com/files/docs/pdf/zh-TW/whitepaper/tp608-powerchoice-tech-provides-tw.pdf |
| **SEA-AWR** | Seagate knowledge base, "Annualized Workload Rate" (005902en). | **primary** (vendor definition) | seagate.com |
| **SEA-X24** | Seagate Exos X24 data sheet DS2080-2307 (French edition). | **NON-PEER-REVIEWED** | seagate.com |
| **WD-HC580**, **WD-HC680** | Western Digital Ultrastar DC HC580 (24 TB CMR) and DC HC680 (28 TB host-managed SMR) data sheets. | **NON-PEER-REVIEWED** | documents.westerndigital.com |
| **BB25** | Backblaze, "Backblaze Drive Stats for 2025" (blog, February 2026). | **NON-PEER-REVIEWED**; read through search summaries only | — |
| **SPEC-LTO10** | Spectra Logic, "LTO-10 Tape Drive" data sheet. | **NON-PEER-REVIEWED** | spectralogic.com |
| **LTO-GEN** | LTO Program, "LTO Generation Information" (lto.org/lto-generation-compatibility). | **primary** (consortium) | fetched page |
| **LTFS25** | SNIA, *Linear Tape File System (LTFS) Format Specification*, version 2.5, Technical Position. | **primary** | snia.org |
| **IBM-RAO** | Shawn Brume, IBM community blog, "Random file access on tape: RAO accelerates data retrieval," 2023-01-05. | **NON-PEER-REVIEWED** | community.ibm.com |
| **SDA-A2** | SD Association, "Delivering Advanced Mobile Application Performance with A1" and the Application Performance Class pages. | **primary** (consortium), read through a fetch summary | sdcard.org |
| **INTEL-10Q** | Intel Corporation Form 10-Q for the quarter ended 2022-07-02 (the Optane wind-down and its $559 million inventory impairment). | **primary** (SEC filing), read through search summaries | sec.gov |

### Specifications, kernel and platform documentation

| Key | Source | Label |
|---|---|---|
| **NVMe24** | NVM Express Base Specification, Revision 2.4, ratified 2026-07-31 (the same revision note 10 cites). | primary |
| **NVMCS13** | NVM Express NVM Command Set Specification, Revision 1.3, ratified 2026-07-31. | primary |
| **ZNS15** | NVM Express Zoned Namespace Command Set Specification, Revision 1.5, ratified 2026-07-31. | primary |
| **LINUX** | Linux at commit `551c722f4080`: `drivers/nvme/host/core.c`, `drivers/scsi/sd.c`, `drivers/usb/storage/scsiglue.c`, `drivers/usb/storage/uas.c`, `drivers/ata/libata-core.c`, `drivers/ata/libata-sata.c`, `drivers/mmc/core/sd.c`, `drivers/mmc/core/block.c`, `include/uapi/linux/io_uring.h`, `Documentation/ABI/stable/sysfs-block`, `Documentation/ABI/testing/sysfs-driver-ufs`, `Documentation/driver-api/md/raid5-ppl.rst`, `Documentation/driver-api/md/raid5-cache.rst`, `Documentation/power/powercap/powercap.rst`. `drivers/lightnvm/core.c` returns 404 at this commit. | primary |
| **AWS-EBS-IO** | AWS, "Amazon EBS I/O characteristics and monitoring." | primary |
| **AWS-EBS-GP** | AWS, "Amazon EBS General Purpose SSD volumes." | primary |
| **AWS-IS** | AWS, "SSD instance store volumes for EC2 instances" and "Data persistence for Amazon EC2 instance store volumes." | primary |
| **AZ-PERF** | Microsoft Learn, "Azure premium storage: design for high performance" (updated 2026-09-19, git commit `5b2b6881`). | primary |
| **GCP-PD** | Google Cloud, "Persistent Disk performance" (docs.cloud.google.com/compute/docs/disks/performance), read through a fetch summary. | primary |
| **APPLE-PM** | macOS `powermetrics(1)` man page and `powermetrics -h` on macOS 26.4.1. | primary, MEASURED |

---

## 1. Findings that change decisions

1. **The flash write unit the planner must respect is the indirection unit, and on NVMe the drive
   can report it.** NVMe's Namespace Preferred Write Granularity (NPWG), Preferred Write Alignment
   (NPWA), Optimal Write Size (NOWS) and Preferred Deallocate Granularity (NPDG) are the drive's
   own statement of its write and trim units [NVMCS13 Fig. 123, p. 90; §5.2.2, pp. 122–123]. Linux
   puts NPWG in `queue/minimum_io_size` and NOWS in `queue/optimal_io_size`, but sets
   `queue/physical_block_size` to the *smaller* of NPWG and the atomic-write unit
   [LINUX nvme/host/core.c:2120–2141]. A volume block size `B` taken from the physical block size
   (chunk-store design §2) therefore misses a 16 KiB or 64 KiB unit on QLC drives. §3.3.
2. **On NVMe, `write_cache` = "write through" means the controller reports no volatile write
   cache, and then a Flush "shall have no effect"** [NVMe24 §7.2, p. 567; LINUX
   nvme/host/core.c:2476–2479]. Power-loss-protected drives report this way. The group-commit
   flush is then free and the batch's service time is its write time. §3.8.
3. **On SATA, Linux disables FUA by default** (`libata.fua=0`) [LINUX ata/libata-core.c:125–127],
   so every durable write on a SATA disk or SSD with a volatile cache is a write plus a whole-cache
   FLUSH CACHE EXT. §3.8, §5.3.
4. **Consumer flash loses flushed data on power loss.** 13 of 15 SSDs, including
   "enterprise-class" ones, lost data that the kernel had flushed; the two most expensive SLC drives
   lost none [Zheng13 Abstract, p. 271; §5.4, p. 280]. A flush acknowledgement from a device without
   power-loss protection is a hint, not a fact; mantle's durability rests on replicas (note 03 G5).
5. **A USB disk may get no flushes at all.** `usb-storage` never reads the caching mode page
   directly, and when the all-pages fallback fails the SCSI disk driver assumes "write through"
   and sends no cache flushes [LINUX usb/storage/scsiglue.c:192, 276–290; scsi/sd.c:3129–3150,
   3268–3277]. Its queue depth is 1 [scsiglue.c:632]. §6.2.
6. **A disk's rated workload counts reads.** Seagate defines the annualized workload rate as
   "(Lifetime Writes + Lifetime Reads) * (8760 / Lifetime Power On Hours)" [SEA-AWR]; current 24–28
   TB drives are rated at 550 TB/year [WD-HC580; WD-HC680]. A full scrub of a 24 TB disk every 14
   days alone reads 626 TB/year (DERIVED, §5.5). The scrub period on a disk is bounded below by its
   workload rate, which neither note 03's 1–2 weeks nor note 11 §12's formula includes.
7. **On a disk, each group commit pays at least two positioning delays**, because the batch's
   data goes to the open segment and its index frame to the index log at a fixed offset near the
   start of the volume (chunk-store design §2, §4). At 7200 rpm average rotational latency alone is
   4.17 ms (DERIVED, §5.1), so an idle-to-busy commit costs tens of milliseconds where NVMe costs
   one flush. §8.4.
8. **Head unloads are a counted wear budget.** Disks rate 600,000 load/unload cycles [WD-HC580;
   WD-HC680]; over a 5-year warranty that is 329 a day (DERIVED). The EPC idle_b condition unloads
   the heads [TP608], so background work that wakes an idle disk every few minutes spends this
   budget. §5.6.
9. **Linux puts an idle NVMe drive into a non-operational state after 100 ms by default** when
   the state's entry plus exit latency is within 15 ms, and after 2 s when within 100 ms
   [LINUX nvme/host/core.c:85–102, 2898–2916]. On a laptop drive this adds up to milliseconds to
   the first I/O after a pause, a second latency state for calibration and benchmarks (note 21).
   §3.9.
10. **Drives can now report their own power.** NVMe 2.4 defines an Interval Power Measurement and
    an Operational Lifetime Energy Consumed field in the SMART log and an optional Power
    Measurement log with averages, maxima and a histogram [NVMe24 §8.1.20, pp. 677–679]. Where
    present, it is the only per-device energy meter mantle can read without external hardware.
    §7.6.
11. **Flexible Data Placement gives mantle death-time grouping without changing its layout.** FDP
    tags each write with a placement handle; it needs no new commands, is backward compatible, and
    reached a device write amplification of about 1 in CacheLib, against 1.3 at 50% and 3.5 at
    100% utilization without segregation [FDP25 §3.1–3.2, §6]. Linux exposes it as io_uring write
    streams [LINUX include/uapi/linux/io_uring.h:100; sysfs-block:550–564]. Enabling FDP requires
    deleting every namespace in the endurance group [NVMe24 §8.1.12, p. 648], so it is an operator
    provisioning step, never something mantle does at run time. §4.2.
12. **Zone Append returns the address it wrote** [ZNS15 §3.4.1, p. 24]. That is what lets several
    writes be in flight to one zone, and it means a batch's index frame cannot name its records'
    locations until their writes complete — the ordering whose cost mantle has already measured
    (one fifth slower for a 32 MiB batch, `docs/measurements/2026-09-29-frame-overlap.md`). §4.1.
13. **Reads and writes interfere on flash far more than either interferes with itself.**
    Sequential writes fell from 61.4 to 13.4 MB/s, a factor of 4.5, when random reads ran beside
    them, while write performance alone was "largely independent of access patterns"
    [Chen11 Abstract; §5.3]. Scrub and cleaner reads should not share a window with large write
    bursts on the same device. §3.7.
14. **Cloud block devices count I/Os, not bytes, up to a cap.** EBS counts an SSD-volume I/O up to
    256 KiB and an HDD-volume I/O up to 1 MiB, merges sequential small I/Os and splits large ones,
    and asks for an average queue depth of one per 1,000 provisioned IOPS [AWS-EBS-IO]. Instance
    store loses its data on any stop, hibernation, OS-initiated shutdown or instance-type change
    [AWS-IS]. §6.4.
15. **The energy of storage is dominated by idle power on disks and by request count on flash.**
    A disk draws 5.5–6.3 W idle and 7–9.4 W busy [SEA-X24; WD-HC580; WD-HC680], so its energy is
    almost flat in load; flash is energy-proportional, and "bytes per joule increases as request
    size increases" at every depth [HA20 Obs. 6]. Large requests are the energy rule on every
    class; on disks, so is spinning down, whose break-even is seconds of energy but 10–15 s of
    latency [Nar08 Table 3]. §7.

---

## 2. What every class is asked

A device class is described here by the same eight properties, because those are what the
planner acts on:

1. **Program unit**: the smallest write the medium accepts without a read-modify-write.
2. **Reclaim unit**: the unit the device frees at once (flash erase block or superblock, SMR band
   or zone, tape partition).
3. **Positioning cost**: the time before the first byte moves (seek and rotation, tape locate,
   nearly zero on flash).
4. **Internal parallelism**: how many independent units serve requests at once (flash channels,
   dies and planes; one head stack on a disk; one head on tape).
5. **Volatile cache and its protection**: whether an acknowledged write can be lost on power loss,
   and what a flush costs.
6. **Power states**: what the device draws busy, idle and in each low-power state, and the latency
   and energy of leaving each.
7. **Wear**: which operations consume a finite budget (program/erase cycles, head loads, start/stop
   cycles, rated workload, tape passes).
8. **What the OS reports**: which of 1–7 a process can read, with what privilege, on which OS.

| Class | Program unit | Reclaim unit | Positioning | Parallelism | Volatile cache | Wear |
|---|---|---|---|---|---|---|
| NVMe/SATA flash | indirection unit, 4–64 KiB (§3.3) | erase block × planes × dies; FDP reclaim unit of GBs [FDP25 §3.2.1] | none (µs) | channels × dies × planes [Agr08 §2.2; Chen11 §3] | DRAM; protected only on PLP drives (§3.8) | P/E cycles (§3.11) |
| ZNS flash | zone write granularity | zone (hundreds of MiB to GiB) [note 03 §12.1] | none | across open zones | as above | P/E, host-controlled |
| CMR disk | 4 KiB physical sector | none (in place) | ms: settle, seek, rotation [RW94] | one actuator | DRAM; NAND-backed on some [WD-HC580] | head loads, start/stop, workload rate (§5.5–5.6) |
| SMR disk | sector, but band rewrite on random writes | band 15–40 MiB (DM) or zone 256 MiB (HM) [note 03 §12.2] | as CMR | one actuator | as CMR, plus persistent media cache | as CMR |
| SD/eMMC/UFS | flash page; cards' allocation unit | erase block | none | small | optional cache [LINUX mmc/core/sd.c:1570] | P/E, small spare |
| Tape | data set | partition / cartridge | tens of seconds (UNVERIFIED, §6.8) | one head | drive buffer | passes, generation read-back limit |

The rest of the note fills in the rows and turns them into planner rules.

---

## 3. Flash SSDs (NVMe and SATA)

### 3.1 Cells, endurance and timing

- **Endurance per cell type.** Cai17 gives the planar figures: "5x-nm ... MLC NAND flash could
  endure ~10,000 P/E cycles per block before being worn out, modern 1x-nm (i.e., 15–19 nm) MLC and
  TLC NAND flash can endure only ~3,000 and ~1,000 P/E cycles per block, respectively" [Cai17 §3.4].
  3D NAND moved back to larger cells: "Contemporary 3D NAND flash can contain 48–64 layers,
  allowing manufacturers to use larger feature sizes (e.g., 50–54 nm)", and "the endurance ... of
  the flash cells has increased as well, by over an order of magnitude" [Cai17 §7]. The same
  section warns that "3D NAND flash cells tend to leak more rapidly, especially soon after being
  programmed", so retention, not endurance, is 3D NAND's weaker axis. SLC-era parts were rated at
  100K cycles [Agr08 Table 1, p. 58].
- **QLC and PLC.** No peer-reviewed source read for this note gives P/E or timing for QLC
  (4 bits per cell) or PLC (5 bits). Each added bit halves the voltage margin between states
  (INFERENCE from the cell model in Cai17 §3), so endurance and program time worsen with bits per
  cell. **UNVERIFIED** for any number; mantle reads the rated endurance from the drive
  (§3.11) and does not assume one.
- **Timing shape.** On 2008 SLC parts a page read took 25 µs, a program 200 µs and an erase 1.5 ms,
  and the 100 µs serial transfer per 4 KB page, not the cells, limited one package to 32–40 MB/s of
  reads [Agr08 §2.1–2.2, pp. 58–59]. The values are obsolete; the shape is not: a program is about
  an order of magnitude slower than a read and an erase another order slower, and a single die is
  slow, so throughput comes from parallelism (§3.6).
- **Read cost grows with wear and age.** When raw errors exceed what the first decode corrects,
  the controller retries at shifted read voltages; "a five-level soft decoding step requires up to
  480 µs" [Cai17 §6.2], which is how worn or retention-aged data shows up as read latency (note 10
  §5.6 covers the field evidence).

### 3.2 pSLC write caches and the cliff

Note 10 §5.4 and R11 cover the mechanism (YS20) and the absence of a peer-reviewed measurement of
cache size or post-cliff rate in current drives, and prescribe passive detection. This note adds
three facts that bound the model:

- **The cache costs capacity at the bits-per-cell ratio.** Linux's UFS ABI documents the
  WriteBooster buffer, UFS's standardized pSLC cache, and its "total user-space decrease in shared
  buffer mode": "The value of this parameter is 3 for TLC NAND when SLC mode is used as
  WriteBooster Buffer. 2 for MLC NAND" [LINUX sysfs-driver-ufs:1302–1311]. Each byte of SLC cache
  holds three bytes of TLC capacity. *INFERENCE:* a dynamic cache built from free blocks shrinks as
  the drive fills, at a rate of bits-per-cell bytes of cache lost per byte of free space consumed,
  so the cliff moves earlier as a volume fills, and a measurement of the cliff is valid only at the
  fullness it was taken.
- **The cliff is followed by background folding that costs idle power.** On a 2008 MLC drive with
  no write buffer, 4 MB random writes ran at sequential speed for about 330 MB and then fell, and
  "even after handling all requests, the power consumption of SSD3 periodically hit the double of its
  idle power ... the SSD prepares free or log blocks larger than 300MB during the idle period and it
  takes a few minutes" [Seo08 §3.2]. A cache that absorbs a burst is repaid later in idle-time work
  and energy.
- **The cache has its own wear meter on UFS.** `wb_life_time_est` reports the WriteBooster
  buffer's consumed life in 10% steps [LINUX sysfs-driver-ufs:1436–1447]; the SLC region wears
  separately from the main array.

**Model (DERIVED from the above and note 10 R11).** A burst of `b` bytes written at the cached
rate `R_c` completes in `b/R_c` while `b ≤ C_slc(f)`, where `C_slc(f)` is the cache available at
fullness `f`; beyond it, at the sustained rate `R_s`, and the device later spends idle time
`≈ C_slc/R_fold` and energy folding. The planner needs `R_s` (for sustained ingest, drain and
repair rates) and `C_slc(f)` (for burst admission). Both are learned passively from the writer's
batch throughput against bytes written per burst (note 10 R11); a burst's throughput then falls
into two states, which note 21's dip test separates.

### 3.3 Mapping, the indirection unit, garbage collection and write amplification

- **What the drive tells the host.** NPWG is "the smallest recommended write granularity in
  logical blocks for this namespace", NPWA "the recommended write alignment", NOWS "the size in
  logical blocks for optimal write performance", and NPDG/NPDA the granularity and alignment for
  deallocation; each "may change if the namespace is reformatted" [NVMCS13 Fig. 123, p. 90]. The
  host "should minimally construct writes that meet the recommendations of NPWG and NPWA, but may
  achieve optimal write performance by constructing writes that meet the recommendation of NOWS"
  [NVMCS13 §5.2.2, p. 123]. All are optional (the OPTPERF field says which are present).
- **Where Linux puts them.** `io_min = NPWG`, `io_opt = NOWS`, `discard_granularity =
  max(NPDG, NPDA)` (or their long forms), and `physical_block_size = min(NPWG, atomic write unit)`
  because "Linux filesystems assume writing a single physical block is an atomic operation"
  [LINUX nvme/host/core.c:2120–2141, 2172–2184]. So on a drive whose NPWG is 16 KiB but whose
  atomic unit is 4 KiB, `physical_block_size` reads 4096 and only `minimum_io_size` reads 16384.
  sysfs documents `minimum_io_size` as "the smallest request the device can perform without
  incurring a performance penalty" and `optimal_io_size` as "the device's preferred unit for
  sustained I/O" [LINUX sysfs-block:574–584, 666–676].
- **What a large unit costs.** Trade press reports QLC drives with a 64 KB indirection unit and
  their successors with 16 KB [QLC-IU, NON-PEER-REVIEWED]. A larger unit shrinks the mapping table
  (one entry per unit) at the price of a read-modify-write for any smaller write.
  **DERIVED:** for random aligned writes of `s < IU` bytes, each write programs a whole unit, so
  the device write amplification from this cause alone is `IU/s` (16 for 4 KiB writes on a 64 KiB
  unit), multiplied with GC amplification. A log that appends small group-commit batches to the
  same unit rewrites that unit once per batch: a run of `n` batches of `s` bytes that fill one unit
  writes `n·IU` bytes of NAND for `IU` bytes of data.
- **Garbage-collection amplification.** Hu09's slides state the result: "write amplification
  decreases as over-provisioning increases" and "Separating static and dynamic data reduces write
  amplification" [Hu09 slide 13]; Des12's closed forms and values are in note 10 §5.1 and note 11
  §10.2. FDP25 measured the same mechanism on a current drive: without data segregation device
  write amplification "increases from 1.3 at 50% utilization to 3.5 at 100% utilization", and with
  segregation "remains unchanged at ~1.03 across utilizations" [FDP25 §6.3]. And "A DLWA of 2 causes
  the SSD to fail twice as fast compared to DLWA of 1" [FDP25 §2.1].
- **Write order is the physical layout.** "The physical data layout in SSDs is dynamically
  determined by the order in which logical blocks are written", and "an ill-mapped data layout can
  cause up to 4.2 times higher latency for parallel" reads [Chen11 §1, §7]. *INFERENCE:* data
  written as one large sequential stream is striped across all dies and reads back at full
  parallelism; data written interleaved with other streams in small pieces can land on one die and
  read back serialized. mantle's segments, written in large batches, are the favourable case.

**What follows for mantle (DERIVED).** The volume block `B` should be the largest of: 4 KiB
[HL23], the logical block, the physical block, and `minimum_io_size` (NPWG) where reported; and a
group-commit batch should be padded to `B`, extending note 03 R3.7 (pad to 4 KiB) to the drive's
own unit. Padding costs mantle the tail of a unit per batch in segment space; not padding costs
the same NAND bytes plus a device read-modify-write and its latency on every following batch. Where
NOWS is reported, the writer's preferred I/O size is a multiple of it.

### 3.4 Over-provisioning

- Hu09 and Des12 give the dependence on spare space under random overwrites (note 10 §5.1). AWS's
  guidance for its instance-store SSDs states the same in operator terms: they "do not have any space
  reserved for over-provisioning", and "we recommend that you leave 10 percent of the volume
  unpartitioned so that the SSD controller can use it for over-provisioning" [AWS-IS].
- In NetApp's log-structured fleet over-provisioning and fullness barely mattered (MMES22, note 10
  §5.1), and FDP25's segregated writes held DLWA at 1.03 at 100% utilization. *INFERENCE,
  consistent with note 10's reconciliation:* a host that writes large units and deallocates whole
  units needs no host-side over-provisioning; one that cannot (shared devices, small objects
  overwritten in place) does.
- **Rule for mantle (DERIVED):** host over-provisioning is a function of mantle's own measured
  device write amplification (note 10 R11), not a fixed fraction: leave unallocated space only
  when the Endurance Group log shows DLWA rising with fullness on that device; otherwise discard
  freed segments (§3.5) and use the whole device.

### 3.5 TRIM, discard and deallocate

- **Semantics.** NVMe reports what a deallocated block reads back as: "The read behavior is not
  reported", "all bytes cleared to 0h", or "all bytes set to FFh" (the DRB field of DLFEAT)
  [NVMCS13 Fig. 123, p. 88]. When it is not reported, the old data may come back. *INFERENCE:*
  mantle's recovery already treats any bytes that do not verify as "no record" (chunk-store design
  §6); a discarded segment must be indistinguishable from an unwritten one to roll-forward, which
  holds as long as roll-forward never trusts content without its checksum and its sequence above
  the superblock's reservation, as today.
- **Granularity and latency.** `discard_granularity` is "the size of the internal allocation unit
  in bytes if reported by the device"; `discard_max_bytes` exists because "Some devices exhibit
  large latencies when large discards are issued" [LINUX sysfs-block:320–341]. HKA17 (note 03 R3.6)
  says discard promptly but in large units.
- **On a file volume, a discard is a hole punch, and a hole is unwritten space.** mantle measured a
  5.7× first-write penalty for durable writes into never-written extents on ext4 and formats the
  volume with zeros where calibration finds one (chunk-store design §2). *DERIVED:* punching a freed
  segment's range returns it to that state, so the next pass of writes through the segment pays the
  penalty again. On a file volume over ext4, discarding freed segments trades the SSD's GC cost
  against mantle's first-write cost; on APFS, where no penalty was found, and on raw devices, which
  have no extent state, it does not. The choice per volume follows from calibration's existing
  first-write measurement.
- **Pre-trimmed devices.** AWS instance-store volumes "are fully trimmed before they are allocated
  to your instance" [AWS-IS], so a format-time discard of the whole device is wasted time there;
  elsewhere it tells the drive the whole space is free (INFERENCE).

### 3.6 Request size, alignment and depth

- Note 03 §2 (HL23) gives NVMe's depth needs and the 4 KiB minimum; note 03 §4 (HKA17) the request
  scale rule; note 26 §2 how depth is realised on each OS.
- **Internal parallelism is where the throughput is.** Exploiting it improved bandwidth "e.g.
  7.2x", and with it "SSD performance is no longer highly sensitive to access patterns" [Chen11
  Abstract]. Chen11 also gives a black-box method to find a drive's striping chunk and
  interleaving degree from the latency of paired writes [Chen11 §4], the basis of the optional
  probe in §10.
- **SATA is capped at 32 outstanding commands** by NCQ (note 03 §15.4). NOWS, where reported, is
  the drive's own optimal write size (§3.3).

### 3.7 Read/write interference and tail latency

- Note 10 §5.2 and §8 cover GC-induced tails and the host-side responses (TTFlash, Rails, ReFlex
  token costs, Gimbal, IODA, NVMe Predictable Latency Mode).
- **Interference between classes of request.** Chen11 co-ran pairs of 4 KB workloads: sequential
  reads with sequential writes gained in aggregate, but "when running random reads and writes
  together, we see a strong negative impact. For example, sequential writes can achieve a bandwidth
  of 61.4MB/sec when running individually, however when running with random reads, the bandwidth
  drops by a factor of 4.5 to 13.4MB/sec" [Chen11 §5.2]. Their rule: "schedule random reads together
  and separate random reads and writes whenever possible" [Chen11 §7].
- **For mantle (INFERENCE):** the writer's batches are large sequential writes; client reads,
  scrub reads and the cleaner's victim reads are random. The scrubber and cleaner should take
  their reads in windows when the writer is not mid-burst on that device, not interleave with it;
  client reads cannot wait, which is what hedged reads across replicas are for (note 04).

### 3.8 Power-loss protection, flush and FUA

- **What the drive declares.** NVMe's Identify Controller VWC field "indicates attributes related to
  the presence of a volatile write cache in the controller" [NVMe24 Fig. 338, p. 374], and a Flush
  commits "data and metadata associated with the specified namespace(s) to non-volatile storage
  media" — but "If a volatile write cache is not present or not enabled, then Flush commands shall
  have no effect" [NVMe24 §7.2, p. 567]. An FDP namespace can declare no volatile cache even when
  the controller has one (VWCNP) [same]. Linux enables both write-back caching and FUA for a
  namespace exactly when VWC is present and VWCNP is not set [LINUX nvme/host/core.c:2476–2479], so
  `queue/write_cache` and `queue/fua` read without privileges report it.
- **What a declaration is worth.** Drives with power-loss capacitors normally declare no volatile
  cache; consumer drives declare one. *INFERENCE:* the declaration is the drive's claim, and only a
  power cut tests it (note 02 §6.2). Zheng13 cut power to 15 SSDs over more than three thousand
  cycles; "13 out of the 15 devices, including the supposedly 'enterprise-class' devices, exhibit
  failure behavior contrary to our expectations", and "two of the fifteen devices became massively
  corrupted" [Zheng13 §1, p. 271]. Writes were synchronous and "the kernel is made to send commands
  to the device being tested to flush its write cache at the end of each write request", yet devices
  showed hundreds of serialization errors per fault, suggesting they "ignore the flush requests";
  "the most expensive SLC drives ... did not exhibit any serialization errors" [Zheng13 §5.4,
  p. 280]. One of two hard disks tested also lost flushed writes [Zheng13 §5.7, p. 282]. NVMe's SMART
  Critical Warning bit 4 reports a failed volatile-memory backup (the capacitors), and LXZ+22 found
  bad capacitors in a third of returned fail-slow drives (note 10 §6.1, §3.7).
- **What a flush costs, by class.** On a PLP drive, nothing beyond the command (the spec says it has
  no effect). On a drive with a volatile cache, the cache's contents must be programmed, so a flush
  costs the program time of the dirty data plus, *INFERENCE*, the padding of any partly filled NAND
  page, which is write amplification charged per flush. mantle measured about 4.7 ms per durable
  flush on the development machine's internal SSD regardless of size (chunk-store design §4).
- **FUA.** On NVMe with a volatile cache Linux uses FUA writes; on SATA it does not by default
  ("FUA support (0=off [default], 1=on)") [LINUX ata/libata-core.c:125–127], so a durable SATA write
  is a write plus FLUSH CACHE EXT, which drains every dirty byte in the drive, not just mantle's.
  Note 02 §2.10 gives the exact conditions for the kernel's FUA path.
- **Group-commit consequence (DERIVED, extends note 11 §2).** The batch service time `S` that sizes
  the group-commit wait is `t_write(batch) + t_flush`. Where `t_flush ≈ 0` (no volatile cache) the
  wait rule's gain shrinks and batches can be issued back to back with several in flight (note 11
  §5 pipelining); where `t_flush` dominates (consumer NVMe, SATA, disks), one flush per batch is the
  whole optimization. Calibration's commit-cost probe (note 02 §6.3, item 4) should therefore
  measure the flush twice — with dirty data and with an empty cache — because the difference is the
  device's evidence about its cache (§10, C2).

### 3.9 NVMe power states, APST and their latencies

- **The table the drive publishes.** Up to 32 power states, each with a Maximum Power, an Entry
  Latency and an Exit Latency in microseconds, a Non-Operational State bit, relative read/write
  latency and throughput ranks, and optionally Idle Power ("typical power ... over 30 seconds ...
  when idle", measured after 10 s idle) and Active Power ("the largest average power ... over a 10
  second period ... with the workload indicated") [NVMe24 Fig. 340, pp. 385–386; §8.1.19,
  pp. 666–667]. "The maximum amount of time to transition between any two power states is equal to
  the sum of the old state's exit latency and the new state's entry latency" [§8.1.19, p. 667].
- **Non-operational states.** "No I/O commands are processed by the controller while in a
  non-operational power state"; the controller leaves it when an I/O arrives. Controller-initiated
  background operations may exceed the state's power only "if the Non-Operational Power State
  Permissive Mode is supported and enabled" [NVMe24 §8.1.19.1, p. 667]. *INFERENCE:* a drive that
  drops quickly into a non-operational state without permissive mode does its garbage collection
  only while operational, so aggressive power saving can defer GC into the next burst.
- **An example table.** A Samsung 960 (consumer NVMe, 2016) reports five states: 6.04, 5.09 and
  4.08 W operational with zero latencies, then 40 mW (enter 210 µs, exit 1.5 ms) and 5 mW (enter
  2.2 ms, exit 6.0 ms) non-operational [HA20 Table 3]. The Optane SSD in the same study had one
  state and no APST, which HA20 gives as a cause of its higher idle power [HA20 §3.4].
- **Linux's policy.** With the default module parameters, Linux builds the APST table so that the
  drive enters a non-operational state after 100 ms idle if that state's entry plus exit latency is
  at most 15,000 µs, and after 2,000 ms if at most 100,000 µs; it never enters a state whose exit
  latency exceeds `ps_max_latency_us` (default 100,000 µs, changeable per device through PM QoS)
  [LINUX nvme/host/core.c:76–102, 2898–2916, 2940–3036]. The defaults "were selected based on the
  values used by Microsoft's and Intel's NVMe drivers" as "a compromise between values used by
  Microsoft for AC and battery scenarios"; without them the older rule applies, entering the next
  lower state after "50 * (enlat + exlat) microseconds", "at most 2% of the time transitioning"
  [core.c:2924–2937]. HA20 describes the older rule [HA20 §3.4].
- **DERIVED for the Samsung 960 under today's defaults:** the 5 mW state's total latency 8.2 ms is
  under 15 ms, so after 100 ms idle the drive drops to 5 mW and the next I/O waits up to 6.0 ms.
  Any mantle latency measured on such a drive after a pause longer than 100 ms carries that exit;
  calibration rounds with idle gaps between them measure two states (note 21).
- **macOS and Windows.** Apple's internal SSD manages its own power through IOKit, and what states
  it has and when it enters them is not documented in any source read here (**UNVERIFIED**).
  Windows's NVMe driver behaviour is cited only through Linux's comment above (**UNVERIFIED**
  directly).

### 3.10 Thermal state

Note 10 §5.3 and R10 cover the telemetry (Composite Temperature, WCTEMP/CCTEMP, host-controlled
thermal management) and the absence of a peer-reviewed measurement of throttling's cost. Two
additions:
- **Power states are the host's lever.** Static power management lets the host cap the drive by
  "setting the NVM Express power state to one that consumes this amount of power or less" [NVMe24
  §8.1.19, p. 666]. *INFERENCE:* in a chassis with a power or thermal budget, a cell can cap drives
  instead of letting them throttle unpredictably, trading a known lower rate for stable latency.
- **The controller adapts retention work to temperature.** "a state-of-the-art SSD controller
  adapts the rate at which it triggers refresh. The SSD contains sensors that monitor the current
  environmental temperature every few milliseconds. The controller then uses the Arrhenius equation
  to estimate the rate at which retention errors accumulate" [Cai17 §5.3]. A hot drive spends more
  of its own background bandwidth and endurance on refresh.

### 3.11 Endurance and retention

- **Ratings.** JESD218 defines an endurance rating as terabytes written by the host (TBW) such that
  the drive keeps its capacity, its uncorrectable error rate, its functional failure requirement,
  and its power-off retention for its class; the class retention conditions are reported as one
  year at 30 °C for client drives and three months at 40 °C for enterprise drives
  (**UNVERIFIED** against the standard's text). DWPD is the same budget per day:
  **DERIVED:** `DWPD = TBW / (C · 365 · Y)` for capacity `C` in TB and warranty `Y` years.
- **What mantle spends.** **DERIVED:** NAND bytes = client bytes × mantle's cleaner write
  amplification `A_m` (note 11 §10) × the device's DLWA (§3.3). Life consumed is read from the drive
  (Percentage Used, or Media Units Written over the Endurance Estimate, note 10 R9), not computed
  from TBW, because DLWA is not known in advance.
- **Retention is temperature- and wear-dependent.** Retention errors dominate raw flash errors
  (note 10 §3.9); controllers refresh by Arrhenius-scaled rates (§3.10); 3D NAND leaks faster soon
  after programming (§3.1). *INFERENCE for note 28:* a powered-off flash drive is not an archive;
  its retention guarantee is months to a year at room temperature at the end of its rated life, and
  a powered-on but idle drive retains data only because its controller refreshes it.
- **Field reliability.** Note 10 §3 covers SLM16, MWKM15, NWJ+16, MMES20/22, XZQ+19 and LXZ+22, and
  draws the conclusions (errors and their trends predict failure; wear rarely binds; temperature
  matters most for drives that do not throttle). Not repeated.

### 3.12 NVMe and SATA flash compared

| | NVMe | SATA |
|---|---|---|
| Queue | many deep queues; the useful depth comes from calibration (note 03 §2) | NCQ 32 |
| Write unit reported | NPWG/NPWA/NOWS → `minimum_io_size`/`optimal_io_size` | logical and physical sector (note 02 §2.4); no equivalent of NPWG was found in the sources read |
| Volatile cache reported | VWC (and VWCNP for FDP) → `write_cache`, `fua` | WCE from IDENTIFY/mode page → `write_cache`; FUA off by default in Linux |
| Power states | up to 32 with latencies; APST | link power management only (`link_power_management_policy`: `max_performance` … `min_power`) [LINUX ata/libata-sata.c:896–901]; device idle/standby (ATA) |
| Placement | Streams, FDP, ZNS | none |
| Own power meter | optional IPM/OLEC/Power Measurement log (§7.6) | none in the sources read |

---

## 4. Zoned, placement-aware and open-channel flash

### 4.1 ZNS: what the specification adds to note 03 §12.1

- **Zone Append.** "The controller assigns the data and metadata ... to a set of logical blocks
  within the zone. The lowest LBA of the set of logical blocks written is returned in the
  completion queue entry"; "Write ordering in the case of multiple outstanding Zone Append commands
  to a zone is undefined and left to the controller" [ZNS15 §3.4.1, p. 24]. This closes note 03's
  open item (Zone Append "NOT ADDRESSED" by BAH+21): parallelism within a zone is real, and the
  price is that the host learns each record's address only on completion.
- **ZRWA.** A Zone Random Write Area is "an area of non-volatile medium with a sliding set of
  assigned LBAs which start at the write pointer", "analogous to a type of non-volatile cache";
  writes within it may be in any order, and Zone Append is refused on a zone that has one
  [ZNS15 §1.4.4.10, §5.7, §3.4.1].
- **Resource limits.** Maximum Active Resources and Maximum Open Resources bound zones in the
  active and open states; writes beyond them fail with "Too Many Active Zones" or "Too Many Open
  Zones" [ZNS15 Identify Namespace, MAR/MOR; status codes BDh, BEh].
- **What this means for mantle's writer (DERIVED from chunk-store design §4).** Today a batch's
  index frame records each record's offset and is issued beside the records; measured, issuing the
  frame after the records made a 32 MiB batch a fifth slower. Under Zone Append the offsets are
  known only after the records complete, so the frame must follow them, and that measured cost
  becomes structural. The alternatives are a ZRWA (ordinary writes at chosen offsets inside the
  window), or one in-flight write per zone with parallelism across zones (note 03 R11.6).

### 4.2 Flexible Data Placement (NVMe TP4146, now in the base specification)

- **Model.** The device groups media into reclaim units (RU), "one or more erase blocks but no
  guarantees are made", sized by the manufacturer; reclaim units form reclaim groups; reclaim unit
  handles (RUH) point at the RU currently being filled. A write names a placement handle in its
  directive field; writes without one use handle 0 [FDP25 §3.2.1–3.2.2; NVMe24 §8.1.12,
  pp. 648–649]. "FDP is backward compatible" and "does not introduce any new command sets"; reads
  are unchanged; deallocation is ordinary TRIM; "If all the data in a RU is invalidated, then the RU
  is erased for future writes and no logical blocks have to copied" [sic] [FDP25 §3.2.2].
- **Two handle types.** "Initially Isolated": data from different handles starts apart but GC may
  intermix it, "the cheapest to implement". "Persistently Isolated": GC keeps each handle's data
  apart, "expensive to implement on the controller" [FDP25 §3.2.1]. The device the paper used had
  "a single FDP configuration of 8 Initially Isolated RUHs, 1RG and RU size of 6GB" [same].
- **Provisioning.** "The host is required to delete all namespaces associated with the specified
  Endurance Group before modifying the value of the Flexible Data Placement feature" [NVMe24
  §8.1.12, p. 648]. FDP is chosen when a drive is provisioned, not by mantle at run time.
- **Linux.** Write streams map to placement handles: a bio's write stream selects
  `plids[write_stream - 1]` [LINUX nvme/host/core.c:1032–1038]; io_uring carries a per-SQE
  `write_stream` [io_uring.h:100]; sysfs reports `max_write_streams` and `write_stream_granularity`,
  "the size that should be discarded or overwritten together to avoid write amplification in the
  device" [sysfs-block:550–564], which Linux fills from the RU nominal size (`runs`)
  [core.c:2257–2326]. Linux refuses FDP configurations with more than one reclaim group
  ("FDP NRG > 1 not supported") [core.c:2321]. On macOS and Windows no write-stream interface was
  found in the sources read (**UNVERIFIED** that none exists).
- **Results.** DLWA about 1 across utilizations with segregation, against 1.3–3.5 without; "∼3.6x
  fewer GC events for the same amount of host writes" [FDP25 §6.3, §6.6].
- **Its condition.** DLWA of 1 holds when the host deallocates all of an RU's data together: "By
  careful deallocation of all the data in a previously written RU, the host can achieve a DLWA of
  ~1" [FDP25 §3.2.2].

### 4.3 Streams and multi-streamed SSDs

The Streams directive predates FDP: a stream has a Stream Write Size and a Stream Granularity
Size, and writes "aligned to and in multiples of the Stream Write Size (SWS) provides optimal
performance" [NVMe24 §8.1.9.3]. Kang14's prototype improved Cassandra's worst-case update
throughput "by nearly 56%" and its 99.9th-percentile latency by 54% [Kang14 Abstract; §3]. FDP25
describes it as an interface that "did not really take off due to a lack of industry and academic
interest" and that FDP borrows from [FDP25 §3.1]. *INFERENCE:* mantle targets FDP, not Streams.

### 4.4 Open-channel SSDs

LightNVM moved flash management to the host [LNVM17 Abstract]. `drivers/lightnvm/core.c` no longer
exists in Linux at the revision read (it returns 404), and FDP25 positions FDP as avoiding "low-level
media control of Open-Channel SSD proposals" [FDP25 §3.1]. Not a target for mantle.

### 4.5 Should mantle's segments map to zones or placement handles?

**FDP: map streams to handles, not segments to reclaim units (DERIVED).**
- mantle already writes in streams: client data and cleaner relocations append to different open
  segments (chunk-store design §4), and the index log and superblocks are separate regions. These
  have different death times, which is exactly what placement handles separate. With four or more
  handles (FDP25's device had eight), give each stream its own handle: client writes, cleaner
  writes, index log, superblocks (and, if note 28 adds lifetime classes, one per class while handles
  last).
- A segment (256 MiB, note 11 §7) is far smaller than the reported RU (6 GB), so a segment cannot
  own an RU. Each RU therefore holds about 22 segments of one stream (DERIVED: 6 GB / 256 MiB), freed
  by the cleaner at different times. With Initially Isolated handles the device may then mix
  survivors across streams during its GC; DLWA stays near 1 only if the cleaner tends to free a
  stream's segments in write order. *INFERENCE:* the cleaner's cost-benefit policy with age
  (chunk-store design §8) already prefers old, mostly dead segments, which are those written
  earliest, so RU-level order is roughly preserved; this is a measurable claim (Endurance Group log,
  note 10 §5.1), not an assumption.
- `write_stream_granularity` is the RU size; if a deployment wants exact RU ownership, the segment
  size can be set to it at format (it is a format-time parameter already). The cost is cleaning
  granularity: one victim is then a whole RU. Whether that pays is a measurement of mantle's own
  write amplification `A_m` against DLWA, not a default.

**ZNS: a separate backend, later.** The chunk store refuses host-managed zoned devices today because
its superblocks, circular index log and reused segments rewrite earlier offsets (chunk-store design
§2). A zone backend needs the index log as a chain of zones and superblocks in dedicated zones
(note 03 R11.8, ZenFS's journal zones), segment = zone capacity, open segments ≤ MOR, and frames
ordered after records under Zone Append (§4.1). FDP gives most of the device-WA benefit with none of
these changes, so FDP comes first.

---

## 5. Hard disk drives

### 5.1 The service-time model

- **Seek.** "Very short seeks (less than, say, two to four cylinders) are dominated by the settle
  time (1–3 milliseconds) ... Short seeks (less than 200–400 cylinders) spend almost all of their
  time in the constant-acceleration phase, and their time is proportional to the square root of the
  seek distance plus the settle time. Long seeks spend most of their time moving at a constant
  speed, taking time that is proportional to distance plus a constant overhead" [RW94 preprint
  p. 3]. A seek time linear in distance is a poor model, and a calibrated model cut the error of a
  first-order one fourteenfold [RW94 p. 1].
- **Rotation.** Average rotational latency is half a revolution: **DERIVED** 60/7200/2 = 4.17 ms at
  7200 rpm, which is the 4.16 ms the HC580 and HC680 data sheets give as "Latency average"
  [WD-HC580; WD-HC680].
- **Transfer and zones.** Outer zones have more sectors per track and higher transfer rates [RW94
  p. 5]; note 10 §5.5 and R12 cover the zone model (VM97) and its calibration. Western Digital's
  footnote locates the maximum sustained rate "at approximately 10% into the capacity of the HDD"
  [WD-HC580, note 4], i.e. near the low LBAs, which answers note 10's open question 8 for at least
  this model: low LBAs are on the fast outer tracks.
- **Current numbers (NON-PEER-REVIEWED, vendor).** 24 TB CMR, 7200 rpm: sustained 298 MB/s, random
  4 KB reads 212 IOPS at QD32, random writes 565 IOPS at QD32 with cache enabled or disabled
  [WD-HC580]; Seagate's 24 TB: 285 MB/s, 168/550 IOPS read/write at QD16 [SEA-X24]. 28 TB
  host-managed SMR: 265 MB/s [WD-HC680].
- **Request size for sequential efficiency (DERIVED).** With positioning time `t_p` and media rate
  `R`, a request of `s` bytes spends the fraction `(s/R)/(t_p + s/R)` transferring. For 90% of the
  sequential rate, `s = 9·R·t_p`. With `R` = 298 MB/s: `t_p` = 4.17 ms (rotation only, a track-to-track
  move) gives `s` ≈ 11 MB; `t_p` = 12 ms (rotation plus an average seek, the seek **UNVERIFIED** for
  current drives) gives `s` ≈ 32 MB. A disk wants each positioning event to be followed by
  roughly 10–30 MB of transfer; the calibrated values replace both inputs.

### 5.2 Queueing and reordering

- "Disk subsystem performance can be dramatically improved by dynamically ordering, or scheduling,
  pending requests"; with a prefetching cache, "The cyclical scan algorithm (C-LOOK), which always
  schedules requests in ascending logical order, achieves the highest performance among
  seek-reducing algorithms", and algorithms that reduce total positioning delay do best when they
  "recognize and exploit a prefetching cache" [WGP94 Abstract]. Drives do this themselves over the
  NCQ/TCQ queue (32 deep on SATA).
- **DERIVED from the data sheet:** at QD32 the HC580 serves 212 random reads/s, 4.7 ms each; a QD1
  random read costs at least rotation plus seek, so depth roughly doubles a disk's random-read rate,
  where it multiplies a flash drive's by its parallelism. The calibrated depth ladder (calibrate.rs)
  finds the knee; on a disk it is a modest depth.
- **For mantle (INFERENCE):** a batch of reads to one disk (a scrub step, a cleaner victim's live
  records, several clients' range reads) is issued sorted by LBA ascending and all at once, so the
  drive's queue can reorder; one read at a time forfeits it.

### 5.3 Write cache and flush

- Disks hold writes in volatile DRAM; RW94 describes the hazard and the fix ("Volatile write-cache
  problems go away if the disk's cache memory can be made nonvolatile") [RW94 p. 8]. Western
  Digital's ArmorCache now does this with on-board flash: it "ensures that all the data in the DRAM
  cache is safely written to the onboard NVM device, should a sudden power loss event occur", so
  that "host flush cache commands are no longer necessary to protect data" [WD-HC580,
  NON-PEER-REVIEWED]. Whether such a drive reports its cache as non-volatile to the host was not
  found (**UNVERIFIED**); if it reports write-back, Linux still flushes, which is correct and costs
  only the flush command.
- Drive-managed SMR needs its volatile cache enabled to reach sequential speed (note 03 §12.2).
- A low-end disk ignored flushes under power fault [Zheng13 §5.7, p. 282].
- **Flush cost on a disk (INFERENCE):** a flush writes every dirty cached sector, with the
  positioning that requires; it is larger when the cache holds scattered writes and smallest after a
  purely sequential batch. mantle's batches are sequential within a segment plus one frame in the
  index log (§8.4), so a disk flush costs about one extra positioning. Measured per device (§10, C2).

### 5.4 CMR and SMR

Note 03 §12.2–12.3 covers drive-managed SMR (Skylight: persistent cache, band cleaning, the 8 MiB
streaming threshold) and ext4-lazy. Additions:
- **Host-managed SMR is now the high-capacity product.** The 28 TB HC680 is host-managed; its data
  sheet describes HM-SMR as overlapping the physical tracks, which requires the host to write each
  zone sequentially [WD-HC680].
  mantle refuses host-managed zoned devices until it has a zone backend (§4.5, chunk-store design
  §2); a deployment on HM-SMR therefore waits for that backend.
- **Detection.** Host-managed and host-aware drives report `queue/zoned` (note 02 §2.4);
  drive-managed SMR usually reports nothing, and note 02 §6.3 prescribes the explicit, expensive
  cliff test.

### 5.5 The rated workload, and what it does to scrubbing

- **Definition.** "Annualized Workload Rate = (Lifetime Writes + Lifetime Reads) * (8760 /
  Lifetime Power On Hours)"; beyond the Workload Rate Limit "the reliability of the drive will begin
  to decline" [SEA-AWR]. Western Digital rates its 24–28 TB drives "for workloads up to 550TB per
  year" and derates MTBF and AFR above "550TB/year and 60°C" [WD-HC580; WD-HC680].
- **DERIVED: the scrub period has a lower bound on disks.** A full scrub of capacity `C` every `P`
  days reads `C·365/P` per year. With client reads and writes `W_c` and cleaner traffic `W_g` (reads
  plus rewrites) per year, staying within the limit `L` requires
  `P ≥ C·365 / (L − W_c − W_g)`. For a 24 TB disk with no other traffic, `P ≥ 15.9` days; with
  300 TB/year of client and cleaner traffic, `P ≥ 35` days. Note 03's "every 1–2 weeks" (from
  BGPS07) and note 11 §12's `T ≥ V/B_scrub` both ignore this; the binding bound on a large disk is
  the workload rate, and note 11 §12's formula gains the term `T ≥ V/(L − W_c − W_g)`.
- **What to read.** Drives report lifetime reads, writes and power-on hours (ATA device statistics
  log; SCSI log pages; NVMe Data Units Read/Written in the SMART log, note 10 §6.1), so the rate is
  computable on the device; the accesses and privileges are those of note 10 §6.4–6.7.

### 5.6 Head loads, start/stop and idle states

- **The budget.** "Load/Unload cycles (at 40°C) 600,000" [WD-HC580; WD-HC680]. **DERIVED:** over a
  5-year warranty, 600,000 / (5·365) = 329 per day, one per 4.4 minutes on average. Start/stop
  (spin-down) cycle ratings were not in the data sheets read (**UNVERIFIED**).
- **What unloads the heads.** The SCSI power conditions T10-EPC added: idle_a, idle_b, idle_c and
  standby_y alongside standby_z, each with its own timer and recovery time [T10-EPC §5.9]. Seagate's
  implementation, for one 2.5-inch drive: idle_a 2.82 W, 0 s recovery, default timer 1 s; idle_b
  2.18 W (−23%), heads unloaded, 0.5 s recovery, default timer 10 min; idle_c 1.82 W (−35%), heads
  unloaded and reduced rpm, 1 s, 30 min; standby_z 1.29 W (−54%), spindle stopped, 8 s, 60 min
  [TP608 table]. Current data sheets give idle power "based on use of Idle_A" [WD-HC580].
- **DERIVED: how background work spends the budget.** If a disk's idle_b timer is `T_b` and
  mantle's background activity on an otherwise idle disk arrives every `g > T_b`, each arrival costs
  one load/unload cycle, i.e. `86400/g` cycles a day. With `T_b` = 10 min and `g` just over it, that
  is up to 144 a day, within budget; with a short vendor timer (some desktop drives unload after
  seconds, **UNVERIFIED**) and `g` of a minute, 1,440 a day, four times the budget. The cleaner's
  5 s idle wake (note 11 §10.1) only does I/O when there is work, so it does not by itself wake the
  disk; the scrubber's pacing does (note 11 §12.6). *Rule:* on disks, background I/O is batched into
  bursts separated either by less than the idle_b timer (keep heads loaded) or by much more (spend
  few cycles), never by just over it. The timers are readable (SCSI Power Condition mode page;
  ATA EPC log) where mantle has the privilege (**UNVERIFIED** per OS).

### 5.7 Spinning down

- An enterprise 15K disk "consumes 12 W even when idle"; spinning up took 10 s (36 GB) or 15 s
  (146 GB), cost 20 J, and the disk drew 2.6 W spun down [Nar08 §1, p. 253; Table 3, p. 259]. Just
  spinning disks down when idle saved 28–36% of energy on a week of enterprise traces, and
  redirecting writes to other disks while spun down ("write off-loading") raised it to 45–60%; the
  first read to a spun-down disk waits the full spin-up [Nar08 Abstract; §4].
- **DERIVED:** the energy break-even of a spin-down is 20 J / (12 W − 2.6 W) ≈ 2.1 s of idleness,
  so energy is never the binding constraint; latency (10–15 s) and the start/stop budget are.
- **For mantle (INFERENCE):** spin-down belongs to cold storage classes whose readers tolerate
  seconds of first-byte latency (note 28), and write off-loading is what mantle's replication
  already provides: a write can go to replicas on spinning disks while the cold copy's disk sleeps,
  and the cold copy is filled in a later burst.

### 5.8 Vibration and shock

- Data sheets rate operating rotational vibration (12.5 rad/s² at 20–1,500 Hz) and shock (40 G,
  2 ms) [SEA-X24; WD-HC580]. Drives use shock sensors to "detect such movement and safely park the
  read/write head", and "loud audible sounds, such as shouting or fire alarms, can cause drive
  components to vibrate, disturbing throughput"; intentional acoustic interference made drives
  unresponsive and corrupted file systems [Blue18 §I]. *INFERENCE:* vibration shows up to mantle as
  a fail-slow disk or a slow node whose disks all slow together; note 10 §4 and R5 (fail-slow, peer
  comparison, node-level events) already detect it.

### 5.9 Field reliability

Note 10 §2 covers PWB07, SG07, BGPS07, RAIDShield and their use as priors. Backblaze reports a 2025
annualized failure rate of 1.36% over 344,196 drives and a lifetime rate of 1.30% [BB25,
NON-PEER-REVIEWED, read through summaries], against the 0.35% AFR current data sheets rate
[WD-HC580; SEA-X24]. As in note 10, data sheet rates are not used as priors.

---

## 6. Other classes mantle will meet

### 6.1 Laptop and phone internal NVMe (Apple)

- Identified by IOKit as interconnect "Apple Fabric", 4 KiB logical and physical blocks, `Unmap`,
  `Priority` and `Barrier` features and no "Force Unit Access" key; the VM page is 16 KiB (note 02
  §3.4, MEASURED there).
- Durability requires `F_FULLFSYNC`; `F_BARRIERFSYNC` orders without durability (note 02 §3.7–3.8).
  The full flush costs about 4.7 ms on the development machine whatever the batch size (chunk-store
  design §4), the case where group commit matters most.
- Power state behaviour of Apple's controller is not documented in the sources read
  (**UNVERIFIED**, §3.9); the battery's discharge rate is the only power observable (§7.6).
- iPhones and iPads run the same controller family (**UNVERIFIED**); mantle's step of scale there,
  if any, is a client, not a storage node.

### 6.2 USB- and Thunderbolt-attached drives

- **Two protocols.** USB Mass Storage Bulk-Only Transport (BOT, `usb-storage`) runs one command at
  a time: the Linux host template sets `.can_queue = 1` [LINUX usb/storage/scsiglue.c:632]. USB
  Attached SCSI (UAS) queues: 32 commands on high-speed (USB 2) links and as many as the device's
  streams allow on SuperSpeed [uas.c:982–991].
- **Transfer size.** `usb-storage` limits requests to 240 sectors (120 KiB) by default and to 2,048
  sectors (1 MiB) on USB 3 devices; the comment records that "Windows 7 limiting transfers to 128
  sectors for both USB2 and USB3 and Apple Mac OS X 10.11 limiting transfers to 256 sectors for USB2
  and 2048 for USB3 devices" [scsiglue.c:92–125, 660–667].
- **Cache detection and flushes.** `usb-storage` always skips the caching mode page ("A number of
  devices have problems with MODE SENSE for page x08") and the SCSI disk driver then asks for all
  pages; if that is skipped too, or for devices flagged `US_FL_ALWAYS_SYNC` or `US_FL_WRITE_CACHE`,
  it falls back to a default: "Assuming drive cache: write back" only when the quirk says so,
  otherwise "Assuming drive cache: write through" [scsiglue.c:188–193, 276–290; sd.c:3129–3150,
  3268–3277]. A write-through view means the kernel sends no cache flushes, so `fdatasync` on such a
  device reaches the bridge but not the disk's cache. Some bridges "don't understand FUA"
  (`US_FL_BROKEN_FUA`) [scsiglue.c:280–282; uas.c:858–860]. On macOS, Apple documents that "Certain
  FireWire drives have also been known to ignore the request to flush their buffered data" (note 02
  §3.7); nothing equivalent was found for USB (**UNVERIFIED**).
- **Thunderbolt.** A Thunderbolt-attached NVMe enclosure presents an NVMe controller over tunnelled
  PCIe and appears to the OS as NVMe (*INFERENCE*; note 02 identifies it by its interconnect
  properties).
- **For mantle:** a USB volume is durable only as far as replicas elsewhere make it; mantle records
  `write_cache` as it reads it, marks flush-honouring as unverified for every USB-attached device,
  and runs BOT devices at depth 1 with transfers no larger than `max_hw_sectors_kb`.

### 6.3 SD cards, eMMC and UFS

- **Random writes were the weakness.** On 2012 phones, "most if not all [SD cards] exhibit abysmal
  performance (0.02 MB/s or less!)" for random writes, "even when sequential write performance
  quadruples", and speed class did not predict it [Kim12 §3.1]. Application performance varied 100–300%
  with the card alone, due to "random I/O from application databases, and heavy-handed use of
  synchronous writes" [Kim12 Abstract].
- **Application Performance Classes.** A1 requires at least 1,500 random read and 500 random write
  IOPS; A2 4,000 and 2,000 [SDA-A2]; A2's command queuing and volatile cache work only with a host
  that supports them ("A2 performance is available only the combination of A2 supported host and A2
  supported card") [SDA-A2, application class page].
- **The SD cache is volatile and Linux flushes it.** Linux enables the card's cache when supported
  and implements flush by setting the Flush Cache bit and polling until "The card shall reset it, to
  confirm that it's has completed the flushing of the cache" [sic] [LINUX mmc/core/sd.c:1323–1401, 1570].
  On eMMC, Linux implements FUA with Reliable Write: "Reliable writes are used to implement Forced
  Unit Access" [mmc/core/block.c:1397–1402].
- **UFS WriteBooster** is a pSLC cache with exported size, availability and life (§3.2).
- **For mantle (INFERENCE):** these devices are the laptop-and-edge step's removable or embedded
  storage; they get the flash rules with small `B` multiples, depth from calibration (often 1–2),
  and a write budget from their small endurance.

### 6.4 Cloud block devices

- **EBS accounting.** "I/O size is capped at 256 KiB for SSD volumes and 1,024 KiB for HDD volumes";
  physically sequential small I/Os are merged up to the cap and larger ones split; random I/Os count
  one each [AWS-EBS-IO]. **DERIVED:** on an SSD volume, writes of 256 KiB use the provisioned IOPS
  and throughput together most efficiently; larger writes buy nothing, and 4 KiB random writes spend
  an IOPS each.
- **Depth.** "For maximum consistency, a volume must maintain an average queue depth (rounded to the
  nearest whole number) of one for every 1,000 provisioned IOPS"; "Consistently driving more IOPS to
  a volume than it has available can cause increased I/O latency" [AWS-EBS-IO]. **DERIVED:** the
  depth for a gp3 volume at its 3,000 IOPS baseline is 3, not the device's knee; calibration's ladder
  will find the throttle, and the planner should stop there.
- **gp3 and gp2.** gp3: 3,000 IOPS and 125 MiB/s included, up to 80,000 IOPS (500 per GiB) and 2,000
  MiB/s (0.25 MiB/s per IOPS), no bursting, "single-digit millisecond latency", AFR "no higher than
  0.2 percent" [AWS-EBS-GP]. gp2: 3 IOPS per GiB, bursting to 3,000 on a credit bucket of 5.4 million
  I/O credits refilled at 3 per GiB per second [AWS-EBS-GP]. *INFERENCE:* a credit-bucket volume
  measures fast at calibration and slows when the bucket empties; the planner's sustained rate on gp2
  is the baseline, and the `BurstBalance` metric, not calibration, says which state it is in.
- **io2 Block Express** targets "an average latency of under 500 microseconds for 16KiB I/O
  operations" [AWS-EBS-GP].
- **Instance store.** Local NVMe, encrypted with keys destroyed on stop; data "persists even if the
  instance is rebooted" but not if it is "stopped, hibernated, or terminated", including "A shutdown
  is initiated" from the OS, an instance-type change, and the underlying disk failing; it survives
  a power failure "upon reboot" [AWS-IS]. Volumes have no over-provisioning, AWS recommends leaving
  10% unpartitioned, and TRIM-capable volumes arrive fully trimmed [AWS-IS].
- **Azure.** Host caching ReadOnly or None is safe for durable writes; with ReadWrite "you must have a
  proper way to write the data from cache to persistent disks", log disks should use None, and
  "if caching is set to ReadWrite, barriers should remain enabled to ensure write durability"
  [AZ-PERF].
- **Google Cloud.** Persistent Disk performance scales with size and with the instance's vCPU count,
  has per-instance ceilings, and write throughput is subject to network egress caps [GCP-PD, read
  through a summary].
- **Flush semantics on EBS and Persistent Disk** were not found in the documents read
  (**UNVERIFIED**). mantle reads the guest's `write_cache` and measures the flush (§10, C2).

### 6.5 RAID, LVM and md

- **The write hole.** "after a dirty shutdown, parity of a particular stripe may become inconsistent
  with data on other member disks. If the array is also in degraded state, there is no way to
  recalculate parity ... This can lead to silent data corruption"; md's Partial Parity Log closes it
  at a cost: "Write performance is reduced by up to 30%-40%" [LINUX md/raid5-ppl.rst]. md's journal
  device closes it by writing all data to the journal first [md/raid5-cache.rst].
- **What the stack reports.** For RAID arrays `minimum_io_size` "is often the stripe chunk size" and
  `optimal_io_size` "is usually the stripe width" [LINUX sysfs-block:574–584, 666–676]; note 02 §2.1
  and §6.1 walk stacked devices to their leaves and give the FUA rule for stacks.
- **For mantle (INFERENCE):** mantle replicates and erasure-codes across failure domains itself
  (note 04), so a parity RAID under it pays the write hole's cost and a small-write penalty for
  redundancy mantle does not need. The planner treats an md/dm device as one device with the stack's
  `io_min`/`io_opt`, and the deployment guidance is to give mantle the member disks.

### 6.6 Persistent memory

Optane DC persistent memory accessed media in 256-byte lines, and its guidance was "Avoid random
accesses smaller than 256 B", "Use non-temporal stores when possible for large transfers", "Limit
the number of concurrent threads accessing an Optane DIMM", "Avoid NUMA accesses" [Yang20 §5, p. 174]. Intel
announced the wind-down of Optane in July 2022 with a $559 million inventory impairment
[INTEL-10Q]. mantle's identity classes DAX/pmem as `Medium::Memory` (`identity.rs`); no planner work
is warranted for a discontinued product.

### 6.7 Network file systems

Note 02 §6.5 already says: warn, and refuse them for the log by default, because their direct-I/O
and stable-storage semantics differ and servers "may also be configured to lie" about stable
storage. Nothing to add.

### 6.8 Tape (archive class, for note 28)

- **Capacity, rate, power.** LTO-10: 30 TB native (40 TB on a later cartridge), 400 MB/s native,
  40 W reading or writing and 27 W idle per drive [SPEC-LTO10, NON-PEER-REVIEWED].
- **Generation read-back.** LTO-9 drives read and write "LTO-8 and LTO-9 media only", and LTO-10 drives
  "can only read and write to LTO-10 media" [LTO-GEN, read through a fetch summary]. *INFERENCE for note 28:* a tape archive must
  migrate before the last drive that reads its generation is retired; the medium's longevity is
  bounded by drive availability, not only by the tape.
- **Format.** LTFS splits a cartridge into an index partition and an append-only data partition;
  indexes carry a generation number, and "the Index with the highest generation number on the volume
  represents the current state of the entire volume" [LTFS25 §5.4.1]. The structure is mantle's own
  (segments plus index frames with sequences) in sequential-only form.
- **Positioning.** Seeking on tape takes tens of seconds (**UNVERIFIED**: no primary figure was
  found); IBM reports that Recommended Access Order, in which the drive orders a batch of reads,
  "improves random access time to data segments on tape by as much as 86%" [IBM-RAO,
  NON-PEER-REVIEWED]. *INFERENCE:* tape reads are batched and handed to the drive to order, the disk
  rule (§5.2) at a larger scale.

---

## 7. Energy

### 7.1 What each class draws

| Class and example | Busy | Idle | Low-power | Source |
|---|---|---|---|---|
| Consumer NVMe (Samsung 960, 2016) | ≤ 6.04 W (PS0 max) | — | 40 mW / 5 mW (non-operational) | HA20 Table 3 |
| SATA SSDs (2008) | random writes at device maximum "regardless to the request size" | 0.52–1.08 W | — | Seo08 Table 2, §3.2 |
| 3.5" 7200 rpm disk, 24 TB | 8.4–8.9 W (4K random, QD4–16) | 5.5–6.3 W (idle_a) | idle_b/c, standby_z (§5.6) | WD-HC580; SEA-X24 |
| 28 TB HM-SMR disk | 9.4 W (4K random read, QD8) | 5.5 W | as above | WD-HC680 |
| 15K rpm enterprise disk (2008) | — | 12 W | 2.6 W spun down; spin-up 20 J | Nar08 Table 3 |
| 2.5" 7200 rpm disk (2012) | — | 2.82 W (idle_a) | 2.18 / 1.82 / 1.29 W | TP608 |
| LTO-10 drive | 40 W | 27 W | cartridge on shelf: none | SPEC-LTO10 |
| Optane SSD (900P) | highest of the devices tested | system 34 W vs 31 W for flash | no APST | HA20 §3.4 |

### 7.2 Energy per byte and per request

- **Flash is energy-proportional; disks are not.** "the HDD is clearly not energy proportional. Its
  power usage is flat"; flash SSDs "are more energy proportional as they show an increase in IO
  performance with greater power usage" [HA20 §3.6].
- **Size.** "For the same IO depth, energy efficiency as bytes per joule increases as request size
  increases"; "the energy cost of transferring additional data in one request is less significant
  than the cost of managing the request itself and the pressure put on system software" [HA20 Obs. 6].
- **Depth.** "For the same request size, energy efficiency as IOs per joule is coupled to internal
  device parallelism": on the Optane drive, power kept rising past the depth where throughput
  saturated, so efficiency fell; on the flash NVMe drive, "increased power consumption corresponds
  to continuously increased IO performance" [HA20 Obs. 7]. *INFERENCE:* the energy-optimal depth is
  the throughput knee calibration already finds; depth beyond it costs energy for nothing.
- **Pattern.** On 2008 SSDs, random writes drew near maximum power at any size and were the least
  efficient operation, worse than a fast disk's; "The energy efficiency of sequential access was
  similarly good with all the devices and was under 1 Joule/MB in most cases" [Seo08 §3.2].
- **DERIVED examples from current data sheets** (power from the data sheet, rate from the same
  sheet; system power excluded):
  - 24 TB CMR disk, 4 KiB random at QD4: 8.4 W / 220 IOPS = 38 mJ per I/O, 9.3 µJ per byte.
  - The same disk sequential: about 8–9 W / 298 MB/s ≈ 0.03 J/MB, some 300× less per byte.
  - LTO-10 streaming: 40 W / 400 MB/s = 0.1 J/MB, then nothing for a cartridge on a shelf.
  - Disk idle: 5.5 W × 8,760 h = 48 kWh a year per drive, the 0.23 W/TB the data sheet states
    [WD-HC580].
- **Attribution (INFERENCE).** "Energy per byte" has two meanings: the *marginal* energy of an I/O
  (power above idle times its duration) and the *average* (all power divided by work). On flash the
  two are close at low load; on a disk the marginal is small and the average is dominated by idle
  power. mantle's planner uses marginal energy to choose request size and depth, and average energy
  (idle power × time) to decide whether a device should exist in a class at all (note 28).

### 7.3 The energy of a flush

No source read measures a flush's energy. *INFERENCE from §3.8 and §5.3:* on a PLP drive a flush
costs nothing extra (it "shall have no effect"); on a drive with a volatile cache it costs the
programming of dirty data that would have been programmed later anyway, plus padding of partly
filled pages; on a disk it costs a cache drain with positioning. The extra energy of flushing is
therefore the extra NAND programs (write amplification) and disk positionings that flushing more
often causes, which is what group commit already minimises. Measurement: differential energy of the
same bytes written with flushes every `n` batches for two values of `n` (§7.6, §10).

### 7.4 The energy and latency of waking

- **NVMe:** leaving a non-operational state costs its exit latency (up to milliseconds, §3.9); Linux
  bounds transitions so that at most 2% of time is spent transitioning under its fallback rule.
- **Disks:** idle_b to active 0.5 s, idle_c 1 s, standby_z 8 s on the 2.5-inch drive [TP608]; 10–15 s
  and 20 J for 3.5-inch enterprise drives [Nar08]. **DERIVED:** energy break-even ≈ 2 s; latency and
  the load/unload and start/stop budgets bind (§5.6–5.7).
- **Background work after bursts.** A flash drive may spend minutes after a write burst preparing
  free blocks at double its idle power [Seo08 §3.2], and GC events track device write amplification
  [FDP25 §6.6]; operational energy of an SSD is modelled as proportional to GC work [FDP25 §4.2].
  *INFERENCE:* keeping DLWA near 1 (§3.3, §4.5) is also the energy rule for flash writes.

### 7.5 Battery-powered nodes

*INFERENCE, consistent with §7.2–7.4:* on a laptop on battery, the energy-efficient schedule is
the race-to-idle one: do background I/O (scrub, cleaning, repair) in large, dense bursts at the
device's knee, then leave the device idle long enough for its low-power state to pay off (100 ms
to 2 s on Linux NVMe defaults, §3.9), instead of trickling it at a constant rate. note 11 §12's
pacing rule (constant rate `V/T` when idle intervals vary little) needs a battery-mode variant that
issues the same volume per period in bursts. Whether to scrub on battery at all is a policy for
note 28; the device physics says that if it is done, it is done in bursts.

### 7.6 Measuring power

| Method | What it measures | Privilege | Availability |
|---|---|---|---|
| NVMe Interval Power Measurement (IPM) and Operational Lifetime Energy Consumed (OLEC) in the SMART log; Power Measurement log (average, maximum, histogram) | the drive's own measured power, 1 s intervals, "based on measured power" [NVMe24 §8.1.20, pp. 677–679] | log page access (note 10 §6.4–6.6) | optional; IPMSR ≠ 0 says it is present; how many drives implement it is **UNVERIFIED** |
| NVMe power state descriptors (MP, IDLP, ACTP) | bounds and typical values per state | Identify Controller | mandatory MP; IDLP/ACTP optional |
| macOS `powermetrics` | estimated CPU, GPU, ANE power; the `disk` sampler reports usage, not power; the `battery` sampler reports discharge rates | root to run (**UNVERIFIED**: the man page does not state it) | "Average power values reported by powermetrics are estimated and may be inaccurate - hence they should not be used for any comparison between devices" [APPLE-PM] |
| macOS `AppleSmartBattery` registry entry | `Voltage`, `InstantAmperage`, `PackCurrentAccumulator` | none (read with `ioreg`) [MEASURED: keys present; `InstantAmperage` = 0 on AC power] | battery-powered Macs, on battery only; field semantics undocumented (**UNVERIFIED**) |
| Linux powercap/RAPL | CPU package, core, uncore (and DRAM, platform on some parts) energy counters [LINUX powercap.rst] | read access to `energy_uj` (root on current kernels, **UNVERIFIED** per distribution) | not storage devices |
| Linux `power_supply` class | battery power or current × voltage | none | laptops |
| External meter on the drive's supply rails | device power directly; Seo08 sampled the +5 V and +12 V SATA lines once per millisecond [Seo08 §3.1]; HA20 used a wall meter for whole-system power [HA20 §3.1] | hardware | lab and fleet qualification |
| Differential traces | the power of one activity as `(E2 − E1)/(T2 − T1)` for two runs that differ only in that activity [Zed03 §3.3.1, p. 222] | as the meter used | the only way to isolate storage from system power with a system-level meter |

*INFERENCE:* the only storage-specific meter a process can read is NVMe IPM/OLEC where present.
Everywhere else, mantle can attribute energy to its own I/O only by the differential method on a
system meter (battery or RAPL plus platform), with the same rounds-and-intervals discipline as
throughput (note 21), and only in an operator-requested measurement mode.

---

## 8. What the write planner should do per class

### 8.1 Inputs, and where each comes from

| Input | NVMe | SATA SSD | Disk | USB/SD/eMMC | Cloud block | Source of truth |
|---|---|---|---|---|---|---|
| Logical/physical block | OS | OS | OS | OS (bridges lie, note 02) | OS | report (must obey) |
| Write unit `IU` | `minimum_io_size` (NPWG) | measure (C3) | physical sector | measure | measure | report, else measure |
| Optimal write size | `optimal_io_size` (NOWS) | measure | measure (§5.1 `s`) | `max_hw_sectors_kb` cap | 256 KiB / 1 MiB (EBS, cited) | report, else measure or cite |
| Discard unit | `discard_granularity`, `discard_max_bytes` | same | n/a | same | same | report |
| Volatile cache | `write_cache`, `fua` (VWC) | `write_cache` | `write_cache` | unverified (§6.2) | `write_cache`; provider docs | report as claim; flush cost measured (C2) |
| Queue depth | calibrated knee ≤ OS queue | ≤ 32 | calibrated (small) | 1 (BOT) or calibrated (UAS) | provisioned IOPS / 1000 (EBS) | measure, capped by report and cite |
| Placement handles | `max_write_streams`, `write_stream_granularity` | none | none | none | none | report |
| Zoned | `zoned` | — | `zoned` (HA/HM) | — | — | report |
| Power states | Identify (privileged) | link policy | EPC timers (privileged) | — | — | report where readable |
| Rated endurance | Endurance Estimate / Percentage Used | SMART | workload rate (data sheet), counters | UFS life estimate | n/a | report |
| Sustained vs burst rate | passive (§3.2) | passive | n/a | passive | gp2 credits (cited) | measure passively |
| Device WA | Endurance Group log | vendor SMART | n/a | n/a | n/a | report where readable |

### 8.2 NVMe flash

- **Request size and alignment:** `B = max(4 KiB, logical, physical, IU)`; batches padded to `B`;
  writes issued in multiples of NOWS where reported, else at the calibrated knee size; never larger
  than the maximum transfer (note 02).
- **Depth:** the calibrated knee (note 26 §2.6), separately for reads and writes.
- **Flush cadence:** one per group commit (note 03 G1); on a drive with no volatile cache the flush
  is free and batches can be pipelined several deep (note 11 §5), the group-commit wait shrinking
  with it (§3.8).
- **Sequentiality:** segments written front to back; streams separated (chunk-store design §4); with
  FDP, one placement handle per stream (§4.5).
- **Discard:** whole freed segments, aligned to `discard_granularity`, split at
  `discard_max_bytes`, issued when the cleaner frees them (note 03 R3.6); on ext4 file volumes,
  weighed against the first-write penalty (§3.5).
- **Background work:** scrub and cleaner reads in windows apart from write bursts (§3.7); on battery,
  in bursts (§7.5).
- **Wear spreading:** by projected wear-out date (note 10 R9).

### 8.3 SATA flash

As NVMe, with depth ≤ 32, `IU` measured (C3) because ATA does not report it, and each durable write
costing a full cache flush (§3.8), which makes the batch size the main lever.

### 8.4 CMR disks

- **Request size:** each positioning followed by `s = 9·R·t_p` bytes or more (§5.1), from calibrated
  `R` and `t_p`; at today's figures, 10–30 MB.
- **Positionings per commit:** a batch writes into the open segment and its frame into the index log
  (chunk-store design §2, §4), two places on the platter. **DERIVED:** an idle-to-busy commit costs
  about two positionings plus a flush, tens of milliseconds, so light-load commit throughput on a
  disk is tens per second however small the requests. Two remedies, to be decided by measurement:
  put the volume's index log on a flash device where the node has one (note 03 R11.7 already sends
  small, frequently rewritten structures to flash for SMR), or keep the frame next to its batch in
  the segment and rebuild the index from segment summaries on recovery (chunk-store design §6 already
  rolls forward from self-describing records).
- **Reads:** sorted ascending and issued together (§5.2).
- **Depth:** calibrated knee; modest.
- **Flush:** per group commit.
- **Workload budget:** the scrubber's period satisfies `T ≥ V/(L − W_c − W_g)` (§5.5); the drive's
  annualized rate is tracked from its counters.
- **Idle:** background bursts spaced below the idle_b timer or well above it (§5.6).
- **Zones:** hot metadata (index log) at low LBAs, the fast outer tracks (§5.1).

### 8.5 SMR disks

Drive-managed: as CMR plus note 03 W3 (≥ 8 MiB sequential, never interleaved) and idle time for its
cleaning. Host-managed: refused until the zone backend (§4.5).

### 8.6 USB, SD, eMMC, UFS

BOT at depth 1 and transfers ≤ `max_hw_sectors_kb`; UAS calibrated. Durability recorded as unverified
(§6.2). Small endurance makes the write budget (note 10 R9) bind sooner: wear projection runs from
first use.

### 8.7 Cloud block devices

On EBS SSD volumes, write in 256 KiB I/Os (the accounting cap) and run at the depth the provisioned
IOPS imply; on HDD volumes, 1 MiB. gp2's sustained rate is its baseline. Instance store is flash with
no over-provisioning and a lifetime of the instance: placement treats it as a device that vanishes on
any stop (§6.4), a fact for note 28 and the cell layer, not the planner.

### 8.8 By step of scale

**Laptop** (one node, one internal NVMe or Apple SSD, often on battery):
- Identity and calibration as today, plus the unprivileged reads of §8.1. No APST or EPC changes:
  they need privileges and belong to the OS.
- Group commit dominated by the flush (4.7 ms here); batches are the lever.
- Background work in bursts on battery (§7.5); measured latencies tagged with the idle gap before
  them so that power-state exits are a known second state (§3.9, note 21).
- External USB disks: depth 1 or calibrated, durability unverified (§6.2).

**Server** (many local devices, mains power):
- One planner state per device; class from identity confirmed by calibration.
- Disks: workload-rate-bounded scrub (§5.5), sorted batched reads (§5.2), index log on flash where
  present (§8.4), idle bursts aligned to EPC timers (§5.6).
- Flash: IU-aligned batches, FDP handles where provisioned (§4.2), pipelined batches on PLP drives
  (§3.8).
- Energy: NVMe IPM/OLEC recorded where present; nothing else per device.

**Cell and fleet:**
- FDP provisioning becomes an operator step at drive intake (§4.2), and the Endurance Group log
  verifies DLWA per model key (note 10 R11).
- Placement across classes and wear-out dates (note 10 R7, R9; note 28).
- Disk spin-down only for cold classes with seconds-tolerant readers and replicas absorbing writes
  (§5.7, note 28).
- Fleet energy accounting from IPM/OLEC and rack meters (§7.6).

---

## 9. Design implications for mantle

1. **Identity reads the drive's write and trim units.** Add `minimum_io_size` (NPWG),
   `discard_granularity`, `discard_max_bytes`, `max_write_streams` and `write_stream_granularity` to
   the Linux probe, and the volume block `B` takes `minimum_io_size` into its maximum
   [NVMCS13 §5.2.2; LINUX nvme/host/core.c:2120–2184; sysfs-block]. (§3.3)
2. **Batches are padded to `B`, so to the indirection unit**, extending note 03 R3.7 from 4 KiB to
   the drive's NPWG [NVMCS13 Fig. 123; QLC-IU]. (§3.3)
3. **The flush is measured twice at calibration** — after dirty writes and on an empty cache — and
   the difference, with `write_cache`/`fua`, classes the device as power-loss protected or not, which
   sets whether batches pipeline [NVMe24 §7.2; Zheng13 §5.4]. (§3.8)
4. **No device's flush acknowledgement is durability by itself**: the class "flush-unverified"
   (USB, cloud volumes with ReadWrite host cache, devices whose VMBF bit is set) is recorded and the
   placement layer counts such a copy as weaker [Zheng13; LINUX sd.c:3268–3277; AZ-PERF; note 10
   §6.1]. (§3.8, §6.2, §6.4)
5. **FDP: one placement handle per write stream**, enabled only on drives the operator provisioned
   with FDP; verify DLWA from the Endurance Group log before relying on it [FDP25; NVMe24 §8.1.12;
   LINUX io_uring.h:100]. (§4.2, §4.5)
6. **ZNS remains a separate backend**, with frames ordered after records under Zone Append or a
   ZRWA, and open segments ≤ MOR [ZNS15 §3.4.1, §5.7]. (§4.1)
7. **Scrub period on disks gains the workload-rate bound** `T ≥ V/(L − W_c − W_g)`, and each disk's
   annualized workload is tracked from its own counters [SEA-AWR; WD-HC580]. Note 11 §12 should carry
   the term. (§5.5)
8. **Disk background I/O respects the head-load budget**: bursts spaced below the idle_b timer or far
   above it, and load/unload counts tracked against 600,000 [WD-HC580; TP608]. (§5.6)
9. **On disks, the index log moves to flash where a node has it, or frames stay with their batch**;
   chosen by measuring commit latency both ways [RW94; chunk-store design §4]. (§8.4)
10. **Disk reads are sorted ascending and issued together** [WGP94]. (§5.2)
11. **Scrub and cleaner reads take windows apart from write bursts on flash** [Chen11 §5.2, §7].
    (§3.7)
12. **Discard on free, but on ext4 file volumes weigh it against the measured first-write penalty**
    (chunk-store design §2; [LINUX sysfs-block:320–341]). (§3.5)
13. **Host over-provisioning is derived from measured DLWA against fullness**, not set as a fraction
    [Hu09; FDP25 §6.3; AWS-IS; note 10 R11]. (§3.4)
14. **Latency samples carry the idle gap before them**, so power-state exits are recognised as a
    state, not a fault (note 10 R5) or noise [LINUX nvme/host/core.c:2898–2916; HA20 §3.4]. (§3.9)
15. **On battery, background work runs in bursts** at the calibrated knee and then lets the device
    idle [HA20 Obs. 6–7; Seo08 §3.2]; note 11 §12 pacing gains a battery variant. (§7.5)
16. **Energy is recorded where the device measures it** (NVMe IPM/OLEC) and otherwise only in an
    operator-requested differential mode [NVMe24 §8.1.20; Zed03]. (§7.6)
17. **USB BOT devices run at depth 1 with transfers ≤ `max_hw_sectors_kb`** [LINUX
    scsiglue.c:92–125, 632]. (§6.2)
18. **EBS volumes are planned in 256 KiB (SSD) or 1 MiB (HDD) I/Os at depth = provisioned IOPS /
    1000**, and instance store is a device that vanishes on stop [AWS-EBS-IO; AWS-IS]. (§6.4)
19. **Parity RAID under mantle is discouraged; the stack's `io_min`/`io_opt` are used when present**
    [LINUX md/raid5-ppl.rst; sysfs-block]. (§6.5)
20. **Tape, if note 28 adopts it, is append-only with generation-numbered indexes and batched,
    drive-ordered reads, and migrates before its generation's drives retire** [LTFS25; LTO-GEN;
    IBM-RAO]. (§6.8)

---

## 10. Measurements calibration must add

Every probe below runs on calibration's scratch file or, for raw devices, its scratch region, uses
the bounded worker pool and depth bounds of note 26 §2.5–2.6 (depth ≤ min(OS queue, measured knee,
process thread budget); no thread per I/O), and is measured in rounds to the precision note 11 §20
and note 21 define. Each states its device cost.

| # | Measurement | Method | Device cost | When |
|---|---|---|---|---|
| C1 | Reported units and features | read `minimum_io_size`, `optimal_io_size`, `discard_*`, `max_write_streams`, `write_stream_granularity`, `zoned`, `write_cache`, `fua`, `max_hw_sectors_kb` (Linux); the IOKit and Windows equivalents of note 02 | none | every open |
| C2 | Flush with and without dirty data | durable write of `B` then flush, against a flush with nothing written since the last; rounds until the intervals separate or overlap | `B` per round | format and calibrate |
| C3 | Write unit where not reported | durable aligned random writes of 4, 8, 16, 32, 64 KiB at depth 1 within a region the size of a few units; the unit is the smallest size above which throughput per byte stops rising | a few MiB | format, SATA/USB/SD and NVMe without NPWG |
| C4 | Read/write interference | the read ladder's knee point repeated while a sequential write stream runs at the writer's batch size | one batch's bytes per round | calibrate |
| C5 | Idle-exit latency | one read after idle gaps of 50 ms, 200 ms, 1 s, 3 s; a jump marks a power-state exit (§3.9). On disks, at most a few gaps longer than the drive's idle_b timer, counted against the load/unload budget (§5.6) | reads only | calibrate, on mains power |
| C6 | Disk positioning model | depth-1 random reads at controlled LBA distances; fit settle + √d for short and a + b·d for long seeks [RW94]; with note 10 R12's zone profile | reads only | calibrate, rotational devices |
| C7 | Commit placement cost (disks) | commit latency with the frame in the index log against the frame adjacent to the batch (§8.4) | a few batches | calibrate, rotational devices |
| C8 | Discard cost | time to discard one freed segment and the read latency of the next reads (raw devices); on file volumes, the first-write penalty already measured | one segment's discard | calibrate |
| C9 | Burst vs sustained write rate | passive change-point on the writer's batch throughput per burst (note 10 R11), tagged with fullness (§3.2) | none extra | always |
| C10 | Device write amplification | Δ Media Units Written / Δ Data Units Written from the Endurance Group log (note 10 R11) | none | periodic, where readable |
| C11 | Workload and wear counters | lifetime reads and writes and power-on hours (disks); load/unload count; Percentage Used (flash) | none | periodic, where readable |
| C12 | Device power | NVMe IPM/OLEC where IPMSR ≠ 0 | none | periodic, where readable |
| C13 | Differential energy | the same work with and without a component (flush cadence, request size), against battery discharge or RAPL plus platform power, by Zed03's method | the workload's bytes | operator-requested only; never on battery below a set charge |
| C14 | Striping chunk and interleave (optional) | Chen11's paired-write latency probe [Chen11 §4] | writes, small | operator-requested only |

Probes that write until a cache is exhausted (pSLC cliff, drive-managed SMR) stay in the explicit
calibrate mode note 02 §6.3 and note 10 R11 already restrict them to; on battery none of C2–C8 runs
unless the operator asks.

---

## 11. What remains unknown

**Device behaviour with no source read**

1. Current QLC and PLC endurance, program and read times, and pSLC cache sizes against fullness;
   only vendor and trade-press figures exist (§3.1–3.2; note 10 §5.4).
2. How many drives mantle will meet report NPWG/NOWS/NPDG, and whether drives that report them
   report the true indirection unit (§3.3).
3. Whether power-loss-protected drives in practice report no volatile write cache, and whether any
   drive that reports none loses data on power loss; only a power-cut rig answers this (§3.8).
4. How many drives support FDP, with which handle types, reclaim unit sizes and handle counts; and
   whether mantle's cleaner frees segments close enough to write order for DLWA ≈ 1 under Initially
   Isolated handles (§4.2, §4.5).
5. The energy of a flush and of a power-state exit on any current drive (§7.3–7.4).
6. Apple's SSD power-state behaviour and whether it is observable from user space (§3.9, §6.1).
7. Start/stop cycle ratings and EPC default timers on current 3.5-inch drives; and whether
   NAND-backed disk caches (ArmorCache) report a non-volatile cache to the host (§5.3, §5.6).
8. Average seek time of current high-capacity disks, which sets the disk request size (§5.1).
9. Tape locate times for LTO-9 and LTO-10 (§6.8).
10. Flush semantics of EBS, Persistent Disk and Azure disks with None or ReadOnly caching, i.e.
    whether their virtual devices report a volatile cache and what a flush costs (§6.4).

**Access**

11. Whether unprivileged processes can read NVMe Identify (power state table, DLFEAT, VWC) on each OS;
    on Linux the character device is root-only by default (note 10 §6.4), and only the sysfs values
    of §8.1 are certain.
12. Whether macOS exposes NPWG-like write units or write streams for internal or external NVMe (§3.3,
    §4.2).
13. The semantics of the `AppleSmartBattery` keys used for differential energy (§7.6).

**Policy left to sibling notes**

14. Whether a laptop node scrubs on battery, and at what charge it stops (note 28).
15. Whether cold classes spin disks down and how replicas absorb writes meanwhile (note 28, §5.7).
16. How lifetime classes map onto placement handles when classes outnumber handles (note 28, §4.5).
17. How upload scheduling shapes batch sizes per device class (note 27).
