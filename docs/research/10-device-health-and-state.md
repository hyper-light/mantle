# 10 — Device health and state: what predicts failure and slowness, what a process can read, and how mantle decides from measurements

Research note for the chunk store (`docs/design/chunk-store.md`) and for the placement layer (STATUS.md, planned item 4: "retiring disks that start to fail"). It covers:

- which device signals predict whole-device failure, partial failure (latent sector errors, uncorrectable reads) and fail-slow behavior, how strongly and how early, and which commonly assumed signals do not;
- device state that changes performance: fullness and internal write amplification, garbage-collection interference, thermal throttling, SLC write caches, zoned bit recording on disks, and flash aging;
- the health telemetry devices expose (NVMe log pages, ATA SMART attributes) and what a process can read on Linux, macOS and Windows, with and without privileges;
- what production systems do with these signals;
- how I/O can adapt to device variability at run time;
- for each signal, how mantle can compute a decision from measurements, and why that computation is right.

Compiled 2026-09-28. This is research input, not a decision record. §9 proposes rules for `docs/design/`.

The owner's constraint shapes every recommendation here: a threshold or number mantle uses is either learned at run time from measurements, derived from measured quantities and a stated objective, or backed by a cited result whose population is named. None is hand-picked, and none is tuned to the development machine.

---

## How to read this document

**Citation tags.**

- Papers are cited as `[KEY §section, p. N]`, where `p.` is the printed proceedings or journal page. When the copy we read has no printed page numbers, the tag says `PDF p. N`.
- Specifications, documentation and source code are cited by section, figure, file and line, or page title.
- Keys are defined once, in the Sources section below.
- Other research notes are cited as note 02 §x (OS storage APIs and device identification), note 03 §x (I/O and persistence, including the field reliability studies BGS+08 and SDG10 and the SSD rules of HKA17), note 04 §x (erasure coding, placement, hedged reads), and note 11 §x (models for the chunk store's operating parameters, including the scrub period and calibration).

**Quotes.** Quotes are verbatim from each source's text layer:

- ligatures are normalized and words hyphenated across line breaks are rejoined;
- bracketed reference numbers are dropped, and "..." marks an elision;
- a Greek letter the text layer rendered as another glyph (for example λ in Little's law) is written as the letter.

**Evidence labels.**

- *(no label)*: stated in the cited peer-reviewed source and checked against its text.
- **NON-PEER-REVIEWED**: specifications (NVM Express, OCP), vendor and OS documentation (Microsoft Learn, Apple headers, kernel documentation), source code (Linux, smartmontools, nvme-cli, Ceph), and Ceph's documentation.
- **DERIVED**: arithmetic, or a formula obtained from stated facts or definitions. The source does not state it.
- **INFERENCE / Recommendation**: design reasoning for mantle, citing the facts it rests on.
- **UNVERIFIED**: not confirmed in a primary source we could read. Do not rely on it without new evidence.
- **[obs]**: observed on the research machine (macOS 26.4.1 build 25E253, Apple Silicon, internal APPLE SSD AP8192Z, the same machine as note 02 §0). It shows how that machine behaves and nothing more.

**Method.**

1. PDFs were downloaded from USENIX, JMLR, the VLDB Endowment, the KDD conference site, NVM Express, and the authors' or their institutions' pages, converted with `pdftotext`, and the relevant sections read in full. The ACM Digital Library served a browser challenge instead of PDFs, so papers ACM published were read from the copies named in the Sources rows. Page numbers were located by searching each page's text, not read off a table of contents. (The NVMe 2.4 table of contents disagrees with its own page footers by up to four pages; the footers are cited.)
2. Linux sources were read at tag `v7.3-rc5`, the revision note 02 pins. Microsoft Learn, Apple SDK headers, smartmontools, nvme-cli and Ceph were read on 2026-09-28.
3. Every quoted fragment of four or more words was checked by machine against the saved texts, with both sides reduced to lowercase letters and digits. Fragments that did not match were corrected by hand.
4. No secondary summaries were used as evidence. Where a paper reports another work's result, the note says so.

## Sources

The "Peer-reviewed" column is the evidence label for every fact drawn from that source.

### Field studies of disk failure and its predictors

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **PWB07** | Eduardo Pinheiro, Wolf-Dietrich Weber, Luiz André Barroso (Google). "Failure Trends in a Large Disk Drive Population." *5th USENIX Conference on File and Storage Technologies (FAST '07)*, 2007, pp. 17–29. | Yes | https://www.usenix.org/conference/fast-07/failure-trends-large-disk-drive-population |
| **SG07** | Bianca Schroeder, Garth A. Gibson. "Disk failures in the real world: What does an MTTF of 1,000,000 hours mean to you?" *FAST '07*, 2007, pp. 1–16. | Yes | https://www.usenix.org/conference/fast-07/disk-failures-real-world-what-does-mttf-1000000-hours-mean-you |
| **BGPS07** | Lakshmi N. Bairavasundaram, Garth R. Goodson, Shankar Pasupathy, Jiri Schindler. "An Analysis of Latent Sector Errors in Disk Drives." *SIGMETRICS '07*; ACM SIGMETRICS Performance Evaluation Review 35(1): 289–300, 2007. DOI 10.1145/1269899.1254917. Read from the authors' group copy. Also summarized in note 03 §10.1. | Yes | https://doi.org/10.1145/1269899.1254917 (copy: https://research.cs.wisc.edu/adsl/Publications/latent-sigmetrics07.pdf) |
| **RAIDShield** | Ao Ma, Fred Douglis, Guanlin Lu, Darren Sawyer, Surendar Chandra, Windsor Hsu (EMC, Datrium). "RAIDShield: Characterizing, Monitoring, and Proactively Protecting Against Disk Failures." *FAST '15*, 2015, pp. 241–256. | Yes | https://www.usenix.org/conference/fast15/technical-sessions/presentation/ma |
| **MHK05** | Joseph F. Murray, Gordon F. Hughes, Kenneth Kreutz-Delgado. "Machine Learning Methods for Predicting Failures in Hard Drives: A Multiple-Instance Application." *Journal of Machine Learning Research* 6: 783–816, 2005. | Yes | https://www.jmlr.org/papers/v6/murray05a.html |
| **BGBW16** | Mirela Botezatu, Ioana Giurgiu, Jasmina Bogojeska, Dorothea Wiesmann (IBM Research). "Predicting Disk Replacement towards Reliable Data Centers." *KDD '16*, 2016. DOI 10.1145/2939672.2939699. The copy read has no printed page numbers. | Yes | https://www.kdd.org/kdd2016/papers/files/adf0849-botezatuA.pdf |
| **MSS17** | Farzaneh Mahdisoltani, Ioan Stefanovici, Bianca Schroeder. "Proactive error prediction to improve storage system reliability." *2017 USENIX Annual Technical Conference (ATC '17)*, pp. 391–402. | Yes | https://www.usenix.org/conference/atc17/technical-sessions/presentation/mahdisoltani |
| **LLP+20** | Sidi Lu, Bing Luo, Tirthak Patel, Yongtao Yao, Devesh Tiwari, Weisong Shi. "Making Disk Failure Predictions SMARTer!" *FAST '20*, pp. 151–167. | Yes | https://www.usenix.org/conference/fast20/presentation/lu |
| **XWL+18** | Yong Xu, Kaixin Sui, Randolph Yao, Hongyu Zhang, Qingwei Lin, Yingnong Dang, Peng Li, Keceng Jiang, Wenchi Zhang, Jian-Guang Lou, Murali Chintalapati, Dongmei Zhang (Microsoft and others). "Improving Service Availability of Cloud Systems by Predicting Disk Error." *ATC '18*, pp. 481–494. | Yes | https://www.usenix.org/conference/atc18/presentation/xu-yong |
| **KRG19** | Saurabh Kadekodi, K. V. Rashmi, Gregory R. Ganger. "Cluster storage systems gotta have HeART: improving storage efficiency by exploiting disk-reliability heterogeneity." *FAST '19*, pp. 345–358. | Yes | https://www.usenix.org/conference/fast19/presentation/kadekodi |
| **KMS+20** | Saurabh Kadekodi, Francisco Maturana, Suhas Jayaram Subramanya, Juncheng Yang, K. V. Rashmi, Gregory R. Ganger. "PACEMAKER: Avoiding HeART attacks in storage clusters with disk-adaptive redundancy." *OSDI '20*, pp. 369–385. | Yes | https://www.usenix.org/conference/osdi20/presentation/kadekodi |
| **JHZK08** | Weihang Jiang, Chongfeng Hu, Yuanyuan Zhou, Arkady Kanevsky. "Are Disks the Dominant Contributor for Storage Failures? A Comprehensive Study of Storage Subsystem Failure Characteristics." *FAST '08*, pp. 111–125. | Yes | https://www.usenix.org/legacy/event/fast08/tech/full_papers/jiang/jiang.pdf |
| **ESA+12** | Nosayba El-Sayed, Ioan Stefanovici, George Amvrosiadis, Andy A. Hwang, Bianca Schroeder. "Temperature Management in Data Centers: Why Some (Might) Like It Hot." *SIGMETRICS '12*; Performance Evaluation Review 40(1): 163–174, 2012. DOI 10.1145/2318857.2254778 (Crossref). Read from the author's copy, which has no printed page numbers. | Yes | https://doi.org/10.1145/2318857.2254778 (copy: https://www.cs.toronto.edu/~bianca/papers/temperature_cam.pdf) |
| **MSM+16** | Ioannis Manousakis, Sriram Sankar, Gregg McKnight, Thu D. Nguyen, Ricardo Bianchini. "Environmental Conditions and Disk Reliability in Free-Cooled Datacenters." *FAST '16*. Read from the authors' copy, paginated 1–13. | Yes | https://www.usenix.org/conference/fast16/technical-sessions/presentation/manousakis (copy: https://www.microsoft.com/en-us/research/wp-content/uploads/2014/11/Reliability-FAST16.pdf) |

### Field studies of SSD failure, and flash characterization

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **SLM16** | Bianca Schroeder, Raghav Lagisetty, Arif Merchant. "Flash Reliability in Production: The Expected and the Unexpected." *FAST '16*, pp. 67–80. Also summarized in note 03 §10.4. | Yes | https://www.usenix.org/conference/fast16/technical-sessions/presentation/schroeder |
| **MWKM15** | Justin Meza, Qiang Wu, Sanjeev Kumar, Onur Mutlu. "A Large-Scale Study of Flash Memory Failures in the Field." *SIGMETRICS '15*, pp. 177–190. DOI 10.1145/2745844.2745848. Read from the author's copy, which has no printed page numbers. | Yes | https://doi.org/10.1145/2745844.2745848 (copy: https://users.ece.cmu.edu/~omutlu/pub/flash-memory-failures-in-the-field-at-facebook_sigmetrics15.pdf) |
| **NWJ+16** | Iyswarya Narayanan, Di Wang, Myeongjae Jeon, Bikash Sharma, Laura Caulfield, Anand Sivasubramaniam, Ben Cutler, Jie Liu, Badriddine Khessib, Kushagra Vaid. "SSD Failures in Datacenters: What? When? and Why?" *SYSTOR '16*. DOI 10.1145/2928275.2928278. Read from the authors' employer's copy, which has no printed page numbers. | Yes | https://www.microsoft.com/en-us/research/wp-content/uploads/2016/08/a7-narayanan.pdf |
| **MMES20** | Stathis Maneas, Kaveh Mahdaviani, Tim Emami, Bianca Schroeder. "A Study of SSD Reliability in Large Scale Enterprise Storage Deployments." *FAST '20*, pp. 137–149. | Yes | https://www.usenix.org/conference/fast20/presentation/maneas |
| **MMES22** | Stathis Maneas, Kaveh Mahdaviani, Tim Emami, Bianca Schroeder. "Operational Characteristics of SSDs in Enterprise Storage Systems: A Large-Scale Field Study." *FAST '22*, pp. 165–180. | Yes | https://www.usenix.org/conference/fast22/presentation/maneas |
| **XZQ+19** | Erci Xu, Mai Zheng, Feng Qin, Yikang Xu, Jiesheng Wu. "Lessons and Actions: What We Learned from 10K SSD-Related Storage System Failures." *ATC '19*, pp. 961–975. | Yes | https://www.usenix.org/conference/atc19/presentation/xu |
| **LXZ+22** | Ruiming Lu, Erci Xu, Yiming Zhang, Zhaosheng Zhu, Mengtian Wang, Zongpeng Zhu, Guangtao Xue, Minglu Li, Jiesheng Wu. "NVMe SSD Failures in the Field: the Fail-Stop and the Fail-Slow." *ATC '22*, pp. 1005–1019. | Yes | https://www.usenix.org/conference/atc22/presentation/lu |
| **CL20** | Chandranil Chakraborttii, Heiner Litz. "Improving the Accuracy, Adaptability, and Interpretability of SSD Failure Prediction Models." *SoCC '20*. DOI 10.1145/3419111.3421300. Read from the author's copy, which has no printed page numbers. | Yes | https://doi.org/10.1145/3419111.3421300 (copy: https://people.ucsc.edu/~hlitz/papers/SOCC_2020ca.pdf) |
| **Diff-RAID** | Mahesh Balakrishnan, Asim Kadav, Vijayan Prabhakaran, Dahlia Malkhi. "Differential RAID: Rethinking RAID for SSD Reliability." *EuroSys '10*, pp. 15–26. DOI 10.1145/1755913.1755916 (Crossref). Read from the author's copy, which has no printed page numbers. | Yes | https://doi.org/10.1145/1755913.1755916 (copy: https://www.cs.yale.edu/homes/mahesh/papers/eurosys10-diffraid.pdf) |
| **CLHMM15** | Yu Cai, Yixin Luo, Erich F. Haratsch, Ken Mai, Onur Mutlu. "Data Retention in MLC NAND Flash Memory: Characterization, Optimization, and Recovery." *HPCA 2015*, pp. 551–563. DOI 10.1109/HPCA.2015.7056062 (Crossref). Read from the authors' copy. | Yes | https://people.inf.ethz.ch/omutlu/pub/flash-memory-data-retention_hpca15.pdf |
| **CLGHMM15** | Yu Cai, Yixin Luo, Saugata Ghose, Erich F. Haratsch, Ken Mai, Onur Mutlu. "Read Disturb Errors in MLC NAND Flash Memory: Characterization, Mitigation, and Recovery." *DSN 2015*, pp. 438–449. DOI 10.1109/DSN.2015.49 (Crossref). Read from the authors' copy. | Yes | https://people.inf.ethz.ch/omutlu/pub/flash-read-disturb-errors_dsn15.pdf |
| **YS20** | Sangjin Yoo, Dongkun Shin. "Reinforcement Learning-Based SLC Cache Technique for Enhancing SSD Write Performance." *HotStorage '20* (peer-reviewed workshop). | Yes | https://www.usenix.org/conference/hotstorage20/presentation/yoo |

### Fail-slow hardware

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **GSA+18** | Haryadi S. Gunawi, Riza O. Suminto, Russell Sears, Casey Golliher, Swaminathan Sundararaman, Xing Lin, Tim Emami, Weiguang Sheng, Nematollah Bidokhti, Caitie McCaffrey, Gary Grider, Parks M. Fields, Kevin Harms, Robert B. Ross, Andree Jacobson, Robert Ricci, Kirk Webb, Peter Alvaro, H. Birali Runesha, Mingzhe Hao, Huaicheng Li. "Fail-Slow at Scale: Evidence of Hardware Performance Faults in Large Production Systems." *FAST '18*, pp. 1–14. | Yes | https://www.usenix.org/conference/fast18/presentation/gunawi |
| **HSK16** | Mingzhe Hao, Gokul Soundararajan, Deepak Kenchammana-Hosekote, Andrew A. Chien, Haryadi S. Gunawi. "The Tail at Store: A Revelation from Millions of Hours of Disk and SSD Deployments." *FAST '16*, pp. 263–276. | Yes | https://www.usenix.org/conference/fast16/technical-sessions/presentation/hao |
| **IASO** | Biswaranjan Panda, Deepthi Srinivasan, Huan Ke, Karan Gupta, Vinayak Khot, Haryadi S. Gunawi. "IASO: A Fail-Slow Detection and Mitigation Framework for Distributed Storage Services." *ATC '19*, pp. 47–61. | Yes | https://www.usenix.org/conference/atc19/presentation/panda |
| **Perseus** | Ruiming Lu, Erci Xu, Yiming Zhang, Fengyi Zhu, Zhaosheng Zhu, Mengtian Wang, Zongpeng Zhu, Guangtao Xue, Jiwu Shu, Minglu Li, Jiesheng Wu. "Perseus: A Fail-Slow Detection Framework for Cloud Storage Systems." *FAST '23*, pp. 49–63. | Yes | https://www.usenix.org/conference/fast23/presentation/lu |
| **ADR** | Ruiming Lu, Yunchi Lu, Yuxuan Jiang, Guangtao Xue, Peng Huang. "One-Size-Fits-None: Understanding and Enhancing Slow-Fault Tolerance in Modern Distributed Systems." *NSDI '25*, pp. 359–378. ADR is the library the paper proposes. | Yes | https://www.usenix.org/conference/nsdi25/presentation/lu |
| **Limplock** | Thanh Do, Mingzhe Hao, Tanakorn Leesatapornwongsa, Tiratat Patana-anake, Haryadi S. Gunawi. "Limplock: Understanding the Impact of Limpware on Scale-Out Cloud Systems." *SoCC '13*. DOI 10.1145/2523616.2523627. Read from the authors' group copy, paginated from 1. | Yes | https://doi.org/10.1145/2523616.2523627 (copy: https://ucare.cs.uchicago.edu/pdf/socc13-limplock.pdf) |

### Device state and adaptive I/O

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **TTFlash** | Shiqin Yan, Huaicheng Li, Mingzhe Hao, Michael Hao Tong, Swaminathan Sundararaman, Andrew A. Chien, Haryadi S. Gunawi. "Tiny-Tail Flash: Near-Perfect Elimination of Garbage Collection Tail Latencies in NAND SSDs." *FAST '17*, pp. 15–28. | Yes | https://www.usenix.org/conference/fast17/technical-sessions/presentation/yan |
| **Des12** | Peter Desnoyers. "Analytic Modeling of SSD Write Performance." *SYSTOR '12*, article 12, pp. 1–10. DOI 10.1145/2367589.2367603 (Crossref). Read from the author's copy. The TOS'14 extension was not read. | Yes | https://www.ccs.neu.edu/~pjd/papers/pjd-systor12.pdf |
| **DIKS20** | Diego Didona, Nikolas Ioannou, Radu Stoica, Kornilios Kourtis. "Toward a Better Understanding and Evaluation of Tree Structures on Flash SSDs." *PVLDB* 14(3): 364–377, 2020. DOI 10.14778/3430915.3430926. | Yes | https://doi.org/10.14778/3430915.3430926 |
| **OJ10** | Alina Oprea, Ari Juels. "A Clean-Slate Look at Disk Scrubbing." *FAST '10*. Read from the USENIX copy, which is paginated from 1. | Yes | https://www.usenix.org/conference/fast-10/clean-slate-look-disk-scrubbing |
| **VM97** | Rodney Van Meter. "Observing the Effects of Multi-Zone Disks." *USENIX Annual Technical Conference 1997*, pp. 19–30. Read as the USENIX HTML full text, which has no page numbers; cited by section. | Yes | https://www.usenix.org/legacy/publications/library/proceedings/ana97/full_papers/vanmeter/vanmeter/zcav.html |
| **MittOS** | Mingzhe Hao, Huaicheng Li, Michael Hao Tong, Chrisma Pakha, Riza O. Suminto, Cesar A. Stuardo, Andrew A. Chien, Haryadi S. Gunawi. "MittOS: Supporting Millisecond Tail Tolerance with Fast Rejecting SLO-Aware OS Interface." *SOSP '17*. DOI 10.1145/3132747.3132774. Read from the authors' group copy, paginated from 1. | Yes | https://doi.org/10.1145/3132747.3132774 (copy: https://ucare.cs.uchicago.edu/pdf/sosp17-mittos.pdf) |
| **LinnOS** | Mingzhe Hao, Levent Toksoz, Nanqinqin Li, Edward Edberg Halim, Henry Hoffmann, Haryadi S. Gunawi. "LinnOS: Predictability on Unpredictable Flash Storage with a Light Neural Network." *OSDI '20*, pp. 173–190. | Yes | https://www.usenix.org/conference/osdi20/presentation/hao |
| **Heimdall** | Daniar H. Kurniawan, Rani Ayu Putri, Peiran Qin, Kahfi S. Zulkifli, Ray A. O. Sinurat, Janki Bhimani, Sandeep Madireddy, Achmad Imam Kistijantoro, Haryadi S. Gunawi. "Heimdall: Optimizing Storage I/O Admission with Extensive Machine Learning Pipeline." *EuroSys '25*. DOI 10.1145/3689031.3717496. Read from the authors' group copy, which has no printed page numbers. | Yes | https://ucare.cs.uchicago.edu/pdf/eurosys25-heimdall.pdf |
| **IODA** | Huaicheng Li, Martin L. Putra, Ronald Shi, Xing Lin, Gregory R. Ganger, Haryadi S. Gunawi. "IODA: A Host/Device Co-Design for Strong Predictability Contract on Modern Flash Storage." *SOSP '21*. DOI 10.1145/3477132.3483573. Read from the authors' group copy, which has no printed page numbers. | Yes | https://doi.org/10.1145/3477132.3483573 (copy: https://ucare.cs.uchicago.edu/pdf/sosp21-ioda.pdf) |
| **Gimbal** | Jaehong Min, Ming Liu, Tapan Chugh, Chenxingyu Zhao, Andrew Wei, In-Hwan Doh, Arvind Krishnamurthy. "Gimbal: Enabling Multi-tenant Storage Disaggregation on SmartNIC JBOFs." *SIGCOMM '21*. DOI 10.1145/3452296.3472940. Read from the authors' copy. | Yes | https://homes.cs.washington.edu/~arvind/papers/gimbal.pdf |
| **ReFlex** | Ana Klimovic, Heiner Litz, Christos Kozyrakis. "ReFlex: Remote Flash ≈ Local Flash." *ASPLOS '17*. DOI 10.1145/3037697.3037732. Read from the authors' copy. | Yes | https://hlitz.github.io/papers/reflex.pdf |
| **Rails** | Dimitris Skourtis, Dimitris Achlioptas, Noah Watkins, Carlos Maltzahn, Scott Brandt. "Flash on Rails: Consistent Flash Performance through Redundancy." *ATC '14*, pp. 463–474. | Yes | https://www.usenix.org/conference/atc14/technical-sessions/presentation/skourtis |
| **TAIL** | Jeffrey Dean, Luiz André Barroso. "The Tail at Scale." *Communications of the ACM* 56(2): 74–80, February 2013. DOI 10.1145/2408776.2408794 (Crossref). Read from the author's copy of the published article. Also used in note 04 §A9. | Yes | https://doi.org/10.1145/2408776.2408794 (copy: https://www.barroso.org/publications/TheTailAtScale.pdf) |
| **Little11** | John D. C. Little. "Little's Law as Viewed on Its 50th Anniversary." *Operations Research* 59(3): 536–549, 2011. DOI 10.1287/opre.1110.0940. Read from the published article as posted on a University of Massachusetts course page. | Yes | https://doi.org/10.1287/opre.1110.0940 (copy: https://people.cs.umass.edu/~emery/classes/cmpsci691st/readings/OS/Littles-Law-50-Years-Later.pdf) |

### Specifications, operating-system sources and documentation

| Key | Citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **NVMe24** | NVM Express, Inc. *NVM Express Base Specification, Revision 2.4*, ratified July 31, 2026. © 2008 to 2026 NVM Express, Inc. ALL RIGHTS RESERVED. Cited by section, figure and printed page. | **No (NON-PEER-REVIEWED; specification)** | https://nvmexpress.org/wp-content/uploads/NVM-Express-Base-Specification-Revision-2.4-Ratified-2026.07.31.pdf |
| **OCP-C0** | The OCP "SMART / Health Information Extended" log (log identifier C0h) as implemented by nvme-cli `plugins/ocp/ocp-smart-extended-log.h` (commit `0f5ad220a71c`, 2026-09-26). The OCP Datacenter NVMe SSD Specification itself could not be fetched (opencompute.org served a browser challenge; the staging mirror's TLS certificate had expired), so field semantics are **UNVERIFIED** against the specification. | **No (NON-PEER-REVIEWED; source code)** | https://github.com/linux-nvme/nvme-cli/blob/master/plugins/ocp/ocp-smart-extended-log.h |
| **LNX** | Linux kernel, tag `v7.3-rc5` (commit `72d3fcf802c4`): `drivers/nvme/host/{ioctl.c, hwmon.c, core.c, sysfs.c}`, `include/linux/nvme.h`, `drivers/scsi/{scsi_ioctl.c, scsi_sysfs.c, sg.c}`, `drivers/ata/libata-scsi.c`, `drivers/hwmon/{drivetemp.c, hwmon.c}`, `drivers/base/devtmpfs.c`, `block/genhd.c`, `lib/kobject_uevent.c`, `Documentation/block/stat.rst`, `Documentation/ABI/stable/sysfs-nvme`, `Documentation/hwmon/drivetemp.rst`. | **No (NON-PEER-REVIEWED; source and documentation)** | https://github.com/torvalds/linux/tree/v7.3-rc5 |
| **APPLE** | macOS SDK 26.4 (Command Line Tools) headers `IOKit.framework/Headers/storage/nvme/NVMeSMARTLibExternal.h` and `IOKit.framework/Headers/storage/ata/ATASMARTLib.h` (© Apple Inc.), and the [obs] probe described in §6.5. | **No (NON-PEER-REVIEWED; headers)** | Local SDK files; the Apple open-source equivalents are listed in note 02 §8 |
| **SMT** | smartmontools `smartmontools/os_darwin.cpp` (commit `d9179f1553d4`, 2025-06-01). | **No (NON-PEER-REVIEWED; source code)** | https://github.com/smartmontools/smartmontools/blob/master/smartmontools/os_darwin.cpp |
| **MSL** | Microsoft Learn, retrieved 2026-09-28: "Working with NVMe drives"; `STORAGE_PROPERTY_ID`; `STORAGE_TEMPERATURE_DATA_DESCRIPTOR`; `IOCTL_STORAGE_PREDICT_FAILURE` and `STORAGE_PREDICT_FAILURE` (WDK); `IOCTL_STORAGE_QUERY_PROPERTY` (WDK); `MSFT_StorageReliabilityCounter`; `Get-StorageReliabilityCounter`. IOCTL codes are read from `windows-sys` 0.61.2, Microsoft's metadata-generated bindings, as in note 02 ([ms-meta]). | **No (NON-PEER-REVIEWED; vendor documentation)** | https://learn.microsoft.com/en-us/windows/win32/fileio/working-with-nvme-devices ; https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ne-winioctl-storage_property_id ; https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-storage_temperature_data_descriptor ; https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ni-ntddstor-ioctl_storage_predict_failure ; https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ns-ntddstor-_storage_predict_failure ; https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntddstor/ni-ntddstor-ioctl_storage_query_property ; https://learn.microsoft.com/en-us/windows-hardware/drivers/storage/msft-storagereliabilitycounter ; https://learn.microsoft.com/en-us/powershell/module/storage/get-storagereliabilitycounter |
| **CEPH** | Ceph documentation, "Device Management" (development branch), and `src/pybind/mgr/devicehealth/module.py` (commit `4aa9e246f056`, 2025-12-17) and `src/pybind/mgr/diskprediction_local/{module.py, predictor.py}`. | **No (NON-PEER-REVIEWED; documentation and source)** | https://docs.ceph.com/en/latest/rados/operations/devices/ ; https://github.com/ceph/ceph/tree/main/src/pybind/mgr |

**Named but not reviewed.** Anything said about these works is another source's description of them:

- Sankar, Shaw, Vaid, Gurumurthi, *Datacenter Scale Evaluation of the Impact of Temperature on Hard Disk Drive Failures*, ACM TOS 9(2), 2013: the author copy returned HTTP 403 and the ACM copy was not reachable.
- Hughes, Murray, Kreutz-Delgado, Elkan, *Improved Disk-Drive Failure Warnings*, IEEE Transactions on Reliability, 2002: paywalled. Its results are described in MHK05 §1.1 and PWB07 §4.
- Alter, Xue, Dimnaku, Smirni, *SSD Failures in the Field: Symptoms, Causes, and Prediction Models*, SC '19: only the presentation slides could be retrieved, so the paper is not cited.
- Desnoyers, *Analytic Models of SSD Write Performance*, ACM TOS 10(2), 2014: not obtained; the SYSTOR '12 paper (Des12) is cited instead.
- JEDEC JESD218B-02 (SSD endurance and retention test method): referenced by NVMe24 for Percentage Used, not obtained.
- The OCP Datacenter NVMe SSD Specification (see OCP-C0).
- Backblaze's published SMART statistics: only KRG19's footnote describing which attributes Backblaze uses is cited.

---

## 1. Executive summary: the findings that decide design

Each item points to the detailed sections. Items marked *(inference)* or *(derived)* are this note's reasoning, not a single source's claim.

1. **Error counters predict failure; the drive's own health flag, wear and temperature mostly do not.**
   - On 100,000+ consumer disks at Google, the first scan error, reallocation, offline reallocation or probational (pending) sector raised the 60-day failure probability 39, 14, 21 and 16 times over drives without one [PWB07 §3.5, pp. 23–25].
   - In EMC's fleet of about one million SATA disks, disks of one model without reallocated sectors failed at 1.7%; past 40 reallocated sectors more than 50% failed, and at 500–600 nearly 95% [RAIDShield §3.4, p. 248].
   - On Google SSDs, a month after an uncorrectable error had a nearly 30% chance of another, against 2% for a random month [SLM16 §5.6, p. 76]. After 2–4 bad blocks there is a 50% chance that hundreds follow [SLM16 §6.1.2, p. 77].
   - Manufacturers' built-in SMART thresholds catch an estimated 3–10% of failures at a 0.1%-per-year false-alarm rate [MHK05 §1, p. 784].
   - Raw bit error rate is a poor predictor of uncorrectable errors, and uncorrectable errors do not track read volume [SLM16 §10, p. 79].

   (§2, §3)

2. **Counters miss most failures, and they miss fail-slow devices completely.** Over 56% of Google's failed disks had no count in any of the four strong SMART signals, and over 36% had no SMART signal at all [PWB07 §3.5.6, p. 26]. On more than a million NVMe SSDs at Alibaba, fail-slow drives showed only negligible correlation with SMART attributes [LXZ+22 §5.3.3, p. 1015]. Performance metrics predicted disk failure 10 days ahead far better than SMART: MCC 0.94 for a model on performance metrics alone against at most 0.54 on SMART alone [LLP+20 Fig. 8, p. 160]. (§2.7, §4)

3. **Thresholds must be relative: to the device's model, its peers and its own history.** SMART attribute definitions and reporting differ between models and manufacturers [MSS17 §2.1, p. 392]. Seek error rate is meant for model-specific thresholds [PWB07 §3.5.5, p. 25]. Replacement can happen before a normalized SMART value moves at all [BGBW16 §4, PDF p. 5]. SSDs above their model's 95th percentile of factory bad blocks develop more new bad blocks and write errors [SLM16 §6.1.2, p. 77]. Failure rates of disk models in one fleet differ by more than 3.5× [KRG19 §1, p. 345]. A confident per-model failure rate needs thousands of disks [KMS+20 §1, p. 370]. (§2.10)

4. **Slowness is common, and it is told apart from busyness by comparing a device with its peers under the same load, never by a fixed latency threshold.**
   - Disks were more than 2× slower than their RAID-group peers 0.2% of the time and SSDs 0.6%, and more than 95% of those slowdowns could not be attributed to I/O rate or size imbalance [HSK16 Abstract, p. 263; §4.2, p. 269].
   - 1.41% of NVMe SSDs turned fail-slow within four months of monitoring, 6.05× the rate for disks [LXZ+22 §1, p. 1006].
   - Fixed latency thresholds failed in production because latency depends on load [Perseus §3.2, p. 51]. A per-node regression of latency on throughput, with its prediction upper bound as the adaptive threshold, reached precision 0.99 and recall 1.00 [Perseus §1, p. 50].

   (§4)

5. **Within normal ranges, temperature and utilization are weak predictors of disk failure.** Google found lower average temperatures associated with *higher* failure rates, and only a weak link with utilization [PWB07 §3.3–3.4, p. 21]. Below 50 °C, latent sector errors rise slowly and linearly with temperature, and temperature *variability* matters more than its average [ESA+12 Obs. 1–2, PDF p. 3]. For SSDs, heat raises failure rates mostly where the drive does not throttle [MWKM15 §8, PDF p. 13], and passive heating of idle SSDs raised raw bit errors 57% over 128 hours, an effect periodic reads cut to 1% [XZQ+19 §4.3, p. 968]. (§2.9, §5.3)

6. **Flash wear-out is rarely what ends an SSD in the field. Infant mortality and firmware matter more.** 99% of NetApp systems use at most 15% of their drives' rated life [MMES20 §8, p. 147]. Uncorrectable errors grow linearly, not exponentially, with program/erase cycles, with no spike past the vendor's limit [SLM16 §10, p. 79]. NetApp saw infant mortality lasting more than a year at 2–3× the later rate, and firmware versions correlated with replacement rates [MMES20 Findings 2 and 6, pp. 142, 145]. Diff-RAID's concern, correlated wear-out of drives written at the same rate, matters only when wear projections actually converge *(inference)*. (§3, §7.4)

7. **Fullness is not a reliable proxy for SSD write amplification in the field, but mantle can measure write amplification directly on devices that report it.** Under uniform random overwrites, write amplification grows steeply as spare space shrinks: greedy cleaning at 10% spare factor gives 4.82 [Des12 §5, PDF p. 7], and larger datasets raise it in the lab [DIKS20 §4.4, p. 371]. In NetApp's fleet, however, fullness and over-provisioning had little impact, firmware dominated, and write amplification ranged from 2 at the 10th percentile to 480 at the 99th [MMES22 §3.2, p. 170; Table 4, p. 177]. NVMe's Endurance Group Information log reports both host bytes written and media bytes written, so the ratio can be measured [NVMe24 Fig. 225, pp. 238–239]. (§5.1)

8. **Garbage collection dominates flash tail latency, and the host's remedy is to route reads away from a device that is writing or collecting.** GC slowed the 99th–99.99th percentiles 5–138× in a baseline SSD design [TTFlash Abstract, p. 15]. SSDs block reads behind writes [Rails Abstract, p. 463]. Separating reads from writes across replicas [Rails], fast-failing reads to reconstruct them from redundancy [IODA], per-I/O fast/slow prediction [LinnOS; Heimdall], and hedged or tied requests [TAIL] all exploit redundancy mantle already has. (§5.2, §8)

9. **What a process can read without privileges differs by platform.**
   - **macOS [obs]:** an unprivileged, ad-hoc-signed binary read the full NVMe SMART / Health log through IOKit's NVMeSMARTLib.
   - **Linux:** the kernel requires `CAP_SYS_ADMIN` for a Get Log Page passthrough [LNX `ioctl.c`]. Without privileges a process can still read NVMe temperatures and the temperature alarm (hwmon, mode 0444), NVMe command error and retry counters (Linux 7.2+), block-layer I/O statistics, and SCSI error counters.
   - **Windows:** the health queries (`IOCTL_STORAGE_QUERY_PROPERTY`, `IOCTL_STORAGE_PREDICT_FAILURE`) are `FILE_ANY_ACCESS` [ms-meta], but whether an unprivileged process can obtain a handle to the disk is **UNVERIFIED** (note 02 §4.3).

   (§6)

10. **Production systems act on health predictions, and the actions need rate limits and lead time.**
    - Replacing disks proactively on a reallocated-sector threshold eliminated about 88% of the recovery incidents caused by triple-disk failures, about 70% of all disk-related incidents [RAIDShield §4.3, p. 250].
    - A third of NetApp's SSD replacements are preventative [MMES20 Finding 1, p. 141].
    - Doubling the scrub rate whenever a sector error is predicted detects errors 1.7–1.8× sooner on disks while spending 2% of the time accelerated [MSS17 §4.3, p. 399].
    - Unthrottled data movement driven by failure-rate changes consumed 100% of cluster I/O for weeks. PACEMAKER never needed more than 5% because it started transitions early and capped their rate [KMS+20 Abstract, p. 369].

    (§7)

11. **Every number above belongs to its population.** §9 turns each into a rule that computes the decision from mantle's own measurements: peer-relative latency, latency-throughput models, conditional failure probabilities learned from the fleet, a drain threshold derived from the durability objective, and budgets derived from measured drain rates. The literature's numbers serve as priors until the fleet's own estimate is precise enough to decide, and are then replaced by it *(inference)*.

---

## 2. What predicts disk failure: field studies

### 2.1 PWB07: SMART counters on 100,000+ consumer disks at Google

**Population.** More than one hundred thousand serial and parallel ATA consumer drives, 5400–7200 rpm, 80–400 GB, at least nine models, monitored December 2005 to August 2006 [PWB07 §2.2, p. 19]. A failure is a replacement in the repair process [PWB07 §2.3, p. 19].

- **PWB07-F1: failure rates.** Annualized failure rates range "from 1.7%, for drives that were in their first year of operation, to over 8.6%, observed in the 3-year old" population [§3.1, p. 20].
- **PWB07-F2: the method, a "critical threshold".** For each SMART parameter the authors looked "for thresholds that increased the probability of failure in the next 60 days by at least a factor of 10 with respect to drives that have zero counts for that parameter" at more than 95% confidence [§3.5, p. 22]. The comparison group is drives of the same population without the event, so the threshold is relative by construction.
- **PWB07-F3: four strong signals, all with a critical threshold of one event.**

  | Signal (PWB07's name) | Drives with a non-zero count | Failure within 60 days after the first event, relative to drives without | Source |
  |---|---|---|---|
  | Scan errors | fewer than 2% | "39 times more likely" | §3.5.1, pp. 22–23 |
  | Reallocation count | about 9% | "over 14 times more likely"; annual failure rate 3–6× higher | §3.5.2, p. 23 |
  | Offline reallocations (found by background scans) | about 4% | "over 21 times higher chances" | §3.5.3, p. 24 |
  | Probational counts (sectors "on probation") | about 2% | "16 times more likely" | §3.5.4, p. 25 |

  After the first scan error "A little over 70% of the drives survive the first 8 months" [§3.5.1, p. 22], and about 85% survive 8 months past the first reallocation [§3.5.2, p. 23]. Offline-reallocation trends should be interpreted "within specific models", because models classify reallocations differently [§3.5.3, p. 24].
- **PWB07-F4: weak signals.** Seek errors were widespread for one manufacturer only, and the attribute is "meant to be used in combination with model-specific thresholds" [§3.5.5, p. 25]. CRC errors "are less indicative of drive failures than that of cables and connectors" [§3.5.5, p. 26].
- **PWB07-F5: the ceiling on SMART-only prediction.** "Out of all failed drives, over 56% of them have no count in any of the four strong SMART signals". Adding every other SMART parameter except temperature, "over 36% of all failed drives had zero counts on all variables" [§3.5.6, p. 26]. The authors conclude "it is unlikely that SMART data alone can be effectively used to build models that predict failures of individual drives", and suggest that "performance anomalies and other application or operating system signals could be useful in conjunction with SMART data" [§3.5.6, p. 26].
- **PWB07-F6: temperature and utilization.** Utilization shows "a much weaker correlation between utilization levels and failures than previous work has suggested" [§3.3, p. 21]. For temperature, "failures do not increase when the average temperature increases. In fact, there is a clear trend showing that lower temperatures are associated with higher failure rates"; higher temperatures matter only at the high end and for older drives [§3.4, pp. 21–22].

### 2.2 SG07: replacement rates, age and correlation on about 100,000 disks

**Population.** Seven data sets from HPC and Internet-service sites, about 100,000 SCSI, FC and SATA disks, some followed for five years; datasheet MTTFs of 1,000,000–1,500,000 hours imply "a nominal annual failure rate of at most 0.88%" [SG07 Abstract, p. 1].

- **SG07-F1:** "annual disk replacement rates typically exceed 1%, with 2-4% common and up to 13% observed on some systems" [Abstract, p. 1]. For drives under five years old, field replacement rates were 2–10× the datasheet rate [§7, p. 14].
- **SG07-F2:** there is little difference between SCSI, FC and SATA replacement rates, and "rather than a significant infant mortality effect, we see a significant early onset of wear-out degradation" [Abstract, p. 1].
- **SG07-F3: failures are not independent.** The number of replacements in a week varies by a factor of 9 depending on the previous week [§5.2, p. 11], with "strong autocorrelation even for large lags in the range of 100 weeks" and a Hurst exponent of 0.6–0.8 [§5.2, p. 12]. Time between replacements fits a Weibull distribution with shape 0.7–0.8, meaning decreasing hazard rates [§7, p. 15]. The consequence for repair: "the probability of seeing two drives in the cluster fail within one hour is four times larger under the real data, compared to the exponential distribution" [§5.3, p. 12].

### 2.3 BGPS07: latent sector errors cluster in space and time

Covered in note 03 §10.1 (1.53 million NetApp disks, 32 months). The findings used here:

- "the probability of other latent sector errors within a 10 MB radius of an existing error is 0.5" [BGPS07 §5.4.2, p. 296], and for 54.8% of nearline and 62.0% of enterprise disks with errors, "at least one additional error is developed within one month" [§5.4.3, p. 296].
- Media scrubs find most errors, and "a low priority background scrubbing process is sufficient" [§6.2, p. 299].
- A repair-pacing rule [§6.3, p. 299]: a storage system keeps each disk's age, error count and time of last error; if the surviving disks of a RAID group are over a year old, or one had an error within the last 1000 minutes, "the repair process should proceed at an accelerated pace". The authors add: "Our definition of normal and accelerated pace is subjective." The rule's shape (condition repair urgency on the survivors' age and recent errors) is evidence-based; its constants were chosen from their data and are not portable *(inference)*.

### 2.4 RAIDShield: reallocated sectors, and choosing a threshold from lead time and false positives

**Population.** "about one million SATA disks from 6 disk models for periods up to 5 years" in EMC Data Domain systems [RAIDShield Abstract, p. 241].

- **RS-F1: failures cluster by age.** 63% of failed A-1 drives, 66% of A-2 and 64% of B-1 failed in their fourth year [§3.2, p. 245]. Among disks with sector errors, the count grew in the second year by 25% (C-2) to about 300% (A-2) [§3.2, p. 246].
- **RS-F2: reallocated sectors (RS) discriminate; read media errors do not.** Media errors show "only moderate discrimination" [Fig. 7, p. 247]. Pending and uncorrectable sectors appeared only on failing disks: "No working disks show these two types of sector errors" [§3.3.2, p. 248].
- **RS-F3: failure probability and time to failure versus RS count (model A-2).** "the failure rate of disks without any RS is merely 1.7%, while more than 50% of disks fail after this count exceeds 40. If the count grows to the range of 500 and 600, the failure rate increases to nearly 95%" [§3.4, p. 248]. Past 40 RS, half of the failing disks still had more than seven days; past 200, "50% of those disks that will soon fail are found to fail within just two days", though "the 90th percentile of failures is measured in weeks rather than days" [§3.4, p. 248]. Disks with 100–200 RS gained about 100 more in a month, against about 6 for disks under 100 [§3.4, p. 248].
- **RS-F4: the estimator.** The failure probability given a count is computed from the population as P(fail | N_RS) = (number of failed disks with at least N_RS) / (number of all disks with at least N_RS) [Fig. 14, p. 251].
- **RS-F5: how the threshold was chosen.** Below 200 RS the predictor "captures nearly 52–70% impending whole-disk failures, with 0.8–4.5% false positive rates" [§4.2, p. 250]. The deployed threshold of 200 came from two measured facts: replacing a disk "may take up to 3 days in the worst case", and "the median time to failure drops to less than 3 days when the count of RS grows beyond 200"; together with a false-positive target "less than 1%" [§4.3, p. 250].
- **RS-F6: results.** The single-disk policy (PLATE) eliminated about 88% of recovery incidents caused by triple-disk failures, "equivalent to about 70% of all disk-related incidents" [§4.3, p. 250]. The group-level policy (ARMOR) computes the probability that two or more disks of a RAID-6 group fail from each disk's P(fail | RS_i), multiplying per-disk probabilities as if independent given their counts [Fig. 15, p. 252], and in simulation raised coverage to 98% of triple failures [Abstract, p. 241].
- **RS-F7: scrubbing was extended** "to periodically check even unused disk sectors", because reallocations found anywhere indicate the drive's condition [§4.1, p. 249].

### 2.5 MHK05 and BGBW16: testing a drive against its own model's population

- **MHK05-F1: manufacturer thresholds.** Drives raise their SMART failure flag when any single attribute exceeds a vendor threshold, set for "an acceptable false alarm rate on the order of 0.1% per year"; manufacturers estimate the detection rate at 3–10% [MHK05 §1, p. 784].
- **MHK05-F2: nonparametric comparison with a reference population.** Many SMART attributes "are nonparametrically distributed", so the authors compared each test drive's recent samples with samples from known-good drives using the Wilcoxon rank-sum test [§1.1, p. 785]. With four attributes, the rank-sum test predicted 28.1% of failures with no measured false alarms, and "the rank-sum detection rate is 52.8% with 0.7% false alarms" [p. 806]. A radial-kernel SVM reached "50.6% detection and no measured false alarms", at about 100 times the training cost [§5.1, p. 801; p. 806]. It was hard to reach false-alarm rates low enough "compared with the low 0.3-1.0% annual failure rate of hard drives" [§5.1, p. 801].
- **BGBW16-F1: change points select the attributes.** On more than 30,000 disks from two manufacturers over 17 months, the pipeline keeps attributes whose time series shows a shift before replacement that is "permanent and unrecoverable" [BGBW16 §2, PDF p. 2], and predicts replacement "even 10-15 days in advance" with up to 98% accuracy [Abstract, PDF p. 1].
- **BGBW16-F2: which attributes, and raw beats normalized.** For Seagate drives, "63% of the replaced drives correlate with an increase in SMART 193 raw", with 19–26% on attributes 7, 1, 240, 197, 198, 187 and 5. For Hitachi, 196, 194, 5 and 197 led. Raw values carry the signal, since "the normalized values are computed based on generous thresholds, where a replacement can also occur before the normalized value changes at all" [§4, PDF p. 5].

### 2.6 MSS17: predicting sector errors a week ahead, and scrubbing faster when they are predicted

**Population.** Backblaze's public SMART data for disks (Table 1 lists seven models with 2,719–36,368 drives each) and about 30,000 Google MLC SSDs [MSS17 §2, pp. 392–393].

- **MSS17-F1:** SMART 5, 187, 196 and 197 are sector-error related, but "the exact definition and reporting of these parameters varies between drive models and manufacturers, and that not all parameters are reported by all drive models" [§2.1, p. 392].
- **MSS17-F2: features.** The latest raw value of each attribute, plus each cumulative counter's increase over the past week [§3.1.2, p. 394].
- **MSS17-F3: accuracy, one week ahead.** Disks: at a 10% false-positive rate "we can correctly predict 90% and 95% of all errors for Hitachi and Seagate", and at 2% "70-90% of the errors" [§3.2.1, p. 395]. SSD uncorrectable errors: "At a false positive rate of 10% the random forest classifier catches 50-70% of errors. At a false positive rate of 2% the classifier can still catch 50-60% of errors for two of the three models" [§3.2.2, p. 396]. Training on 10% of the data barely changed quality, and a model trained on one drive model still predicted well on another [§3.3, p. 397].
- **MSS17-F4: prediction-driven scrubbing.** The baseline scrubs once a week (the paper calls one or two weeks "A common rule of thumb" [§4.1, p. 398]) and scrubs X times faster while an error is predicted [§4.2, pp. 398–399]. With accelerated mode limited to 2% of the time and X = 2, "we detect errors on average 1.7-1.8X faster than a fixed rate scrubber" on disks, and 1.4–1.5× on two SSD models [§4.3, p. 399].
- **MSS17-F5:** scrubbing is read-only, and cited field studies found "no correlation between the number of reads and the number uncorrectable errors" [§4.4, p. 399].

### 2.7 LLP+20 and XWL+18: performance and system signals, peer comparison, and honest evaluation

- **LLP+20-F1: population.** 380,000 disks from five manufacturers across 64 sites and 10,000 racks, about 70 days; annual failure rate "≈1.36%" [LLP+20 §1–2, pp. 151–155].
- **LLP+20-F2: SMART moves late.** SMART attributes "do not always have the strong predictive capability of making disk failure predictions at longer prediction horizon windows", because "the change is often noticeable only a few hours before the actual failure" [§1, p. 151].
- **LLP+20-F3: peer comparison on the same server.** The authors compare each failed disk's metrics with the average of the healthy disks in the same server over the 240 hours before failure; failed disks drift or show repeated sharp impulses [§3, pp. 156–157].
- **LLP+20-F4: threshold selection by Youden's J.** For each normalized feature they scan thresholds and keep the one maximizing J = TPR + TNR − 1 [§3.1, p. 155]. Disk-level I/O queue size scored J = 0.45, above every SMART attribute, and "Contrary to SMART attributes, performance metrics tend to have a higher true positive rate and a lower true negative rate" [§3.1, p. 156].
- **LLP+20-F5: results at a 10-day horizon** [Fig. 8, p. 160]. SMART alone: MCC at most 0.54 (F-measure 0.58). Performance metrics alone: MCC 0.94 with gradient-boosted trees. SMART + performance + location: MCC 0.95. Location helps only together with performance features, by less than 10% MCC [§5, p. 159]. MCC fell to 0.89 at a 15-day horizon [§5, p. 162].
- **XWL+18-F1: disk errors are gray failures that precede failure.** Microsoft Azure predicts disk *errors* (latency errors, timeouts, sector errors) from SMART plus system signals such as file-system errors, device resets, telemetry loss and dropped-request events; "only about 300 out 1,000,000 disks could become faulty every day" [XWL+18 §1–3, pp. 481–485].
- **XWL+18-F2: evaluate over time, not by cross-validation.** Random splits leak future and environment-specific information; features that change drastically over time or with the environment look good in cross-validation and fail online [§2.2, p. 483]. Useful derived features are the change over a window, x(t) − x(t−w), and the variance within a window [§3.1, pp. 484–485].
- **XWL+18-F3: choose the cut by cost.** Disks are ranked, and the top r are flagged, with r minimizing Cost1·FP_r + Cost2·FN_r estimated from historical false-positive and false-negative ratios. "The ratio between Cost1 and Cost2 is set to 3:1 by the domain experts" [§3.2, p. 486].
- **XWL+18-F4: deployment.** CDEF "saved around 63k minutes of VM downtime per month"; it retrains daily on a moving 90-day window, because the error distribution drifts with firmware, drivers and workload [§5, p. 489].

### 2.8 KRG19 and KMS+20: learning each model's failure rate online, and the cost of acting on it

- **KRG19-F1: models differ.** Among the six make/model groups making up over 90% of Backblaze's 100,000+ disks, "The highest failure rate is over 3.5" times the lowest [KRG19 §1, p. 345]. Grouping by capacity alone hides these differences; grouping is by make/model [§2.2, p. 347].
- **KRG19-F2: how much data a rate needs.** KRG19 defines a sizeable population, one large enough for statistical confidence in a group's AFR, as "approximately 10,000 or more disks" [§2.2, p. 347]. AFR on day d is computed from the past d days as failures divided by the summed daily count of operating disks, times 365 [§2.1, p. 346].
- **KRG19-F3: online phase detection.** HeART exempts the first quarter from infancy detection, runs change-point detection on AFR over a 30-day sliding window (monthly, "because AFRs at a lower granularity than a month are jittery" [§4.1, p. 352]), and filters one-time bulk failures with an anomaly detector: "In the absence of anomaly detection, HeART would have incorrectly concluded that the disk group's wearout stage began as early as point A" [§3.3–3.4, pp. 350–351].
- **KRG19-F4 (footnote 2, p. 347):** Backblaze uses SMART 5, 187, 188, 197 and 198 as indicators of impending failure. This is KRG19's description of Backblaze's practice, **NON-PEER-REVIEWED** in origin.
- **KMS+20-F1: acting on rate changes overwhelms clusters unless it is planned.** Analyzing 5.3 million disks from Google, NetApp and Backblaze, redundancy transitions triggered by observed failure-rate changes "can consume 100% cluster IO continuously for several weeks"; PACEMAKER needs "never ... more than 5% cluster IO bandwidth (0.2–0.4% on average)" [KMS+20 Abstract, p. 369].
- **KMS+20-F2:** "A statistically confident AFR observation requires thousands of disks" [§1, p. 370]; for trickle deployments the first C disks act as canaries, and "C in low thousands (e.g., 3000) is sufficient" [§5.1.2, p. 375].
- **KMS+20-F3: failure rates rise gradually.** "none of the over 60 makes/models from Google, Backblaze and NetApp displayed sudden onset of wearout" [§3.2, p. 373].
- **KMS+20-F4: start early, under a cap.** A transition is started so it "can be completed before the AFR crosses the tolerated-AFR" while respecting a peak-IO cap [§5.1.2, p. 375]. With a 5% cap, a disk that would take one day at full bandwidth "would now take at least 20 days"; the caps "can be configured based on how busy the cluster is" [§4, p. 374].

### 2.9 Environment and components: ESA+12, MSM+16, JHZK08

- **ESA+12** (Google, three disk models in seven data centers, January 2007 to May 2009 [§2.1, PDF p. 2]):
  - Obs. 1: below 50 °C the prevalence of latent sector errors "increases much more slowly with temperature, than reliability models suggest"; half of the model/data-center pairs show no increase, and the rest a linear one [PDF p. 3].
  - Obs. 2: "The variability in temperature tends to have a more pronounced and consistent effect on LSE rates than mere average temperature" [PDF p. 3].
  - Obs. 3 and 5: temperature does not raise the number of errors once a drive has them, and "High utilization does not increase LSE rates under temperatures" [PDF p. 4].
  - The same model's error rate differed by more than 2× between data centers [PDF p. 4].
- **MSM+16** (nine hyperscale, partly free-cooled data centers): "relative humidity seems to have a dominant impact on component failures", and "disk failures increase significantly when operating at high relative humidity, due to controller/adaptor malfunction" [Abstract, PDF p. 1].
- **JHZK08** (about 39,000 NetApp systems, 1,800,000 disks, 155,000 shelves, 44 months): "In addition to disk failures that contribute to 20-55% of storage subsystem failures, other components such as physical interconnects and protocol stacks also account for significant percentages" [Abstract, p. 111]. Failures show strong self-correlation and burstiness, redundant interconnects lower subsystem failure rates by 30–40%, and some failures of other components "are treated as disk faults and lead to unnecessary disk replacements" [§1.1, p. 112].

### 2.10 What the disk evidence says about absolute versus relative thresholds

- **Every predictor that worked was defined relative to a reference population.** PWB07's critical thresholds compare against drives with zero counts [§3.5, p. 22]. MHK05 tests a drive against good drives of its model [§1.1, p. 785]. RAIDShield's P(fail | N) is estimated from the same model's population [Fig. 14, p. 251]. LLP+20 compares a disk with healthy disks in its server [§3, p. 156].
- **Vendor thresholds are absolute and set for a vendor's objective** (few warranty returns), which is why they catch only 3–10% of failures [MHK05 §1, p. 784].
- **Attribute semantics are model-specific** [MSS17 §2.1, p. 392; PWB07 §3.5.3, p. 24], and normalized values are unreliable [BGBW16 §4, PDF p. 5]. Linux's own SMART temperature driver notes that "SMART attributes are not well defined" [LNX `drivers/hwmon/drivetemp.c`, header comment; **NON-PEER-REVIEWED**].
- **The first event matters more than the level.** Four signals had critical threshold one [PWB07 §3.5, pp. 23–25], and RAIDShield's failure probability grows steadily with the count [§3.4, p. 248]. A count's *growth* over a window predicts better than its lifetime total [MSS17 §3.1.2, p. 394; XWL+18 §3.1, p. 484].
- **Confident per-model rates need thousands of devices** [KMS+20 §1, p. 370; KRG19 §2.2, p. 347]. A small mantle deployment therefore cannot learn its own conditional failure rates and must start from the literature's, labeled with their populations (§9.5) *(inference)*.

---

## 3. What predicts SSD failure: field studies

### 3.1 SLM16: six years of Google flash

Summarized in note 03 §10.4 (ten MLC, eMLC and SLC models, six years). The findings that bear on prediction:

- **SLM16-F1: raw bit error rate predicts itself, not failures.** Last month's RBER predicts this month's (correlation above 0.8), but "there is no significant correlation between uncorrectable errors and RBER" [§4.2.5, p. 72].
- **SLM16-F2: prior errors predict uncorrectable errors (UEs).** "the chance of experiencing an uncorrectable error in a month following another uncorrectable error is nearly 30%, compared to only a 2% chance of seeing an uncorrectable error in a random month". Final write errors, meta errors and erase errors "increase the UE probability by more than 5X", and "prior errors, in particular prior uncorrectable errors, increase the chances of later uncorrectable errors by more than an order of magnitude" [§5.6, p. 76].
- **SLM16-F3: bad blocks come in bursts, and factory bad blocks are a per-model percentile signal.** 30–80% of drives develop a bad block in the field; "after only 2-4 bad blocks on a drive, there is a 50% chance that hundreds of bad blocks will follow". Drives that "have above the 95%ile of factory bad blocks have a higher fraction of developing new bad blocks in the field and final write errors, compared to an average drive of the same model" [§6.1.2, p. 77].
- **SLM16-F4: wear and age** [§10, p. 79]. RBER and UEs grow with program/erase (PE) cycles "following a linear rather than exponential rate, and there are no sudden spikes once a drive exceeds the vendor’s PE cycle limit". Separately, "independently of usage the age of a drive, i.e. the time spent in the field, affects reliability".
- **SLM16-F5: reads.** "We see no correlation between UEs and number of reads" [§10, p. 79]. Read disturb is not harmless, though: "while read disturb does not create uncorrectable errors, read disturb errors happen at a rate that is significant enough to affect RBER in the field" [§9, p. 79].
- **SLM16-F6: flash against disks.** Flash replacement rates were 4–10% over four years, against previously reported annual disk replacement rates of 2–9%, but flash has far more uncorrectable errors [§8, p. 78].

### 3.2 MWKM15: Facebook's SSDs, failure periods and temperature

**Population.** "a majority of flash-based solid state drives at Facebook data centers over nearly four years and many millions of operational hours" [MWKM15 Abstract, PDF p. 1]. The five observations of its summary and conclusions:

1. Failure rate does not rise monotonically with data written; drives pass through "early detection, early failure, usable life, and wearout" periods [§8, PDF p. 13].
2. "the effect of read disturbance errors is not a predominant source of errors in the SSDs we examine" [§8, PDF p. 13].
3. Sparse logical data layout, measured by the controller DRAM needed to hold its mapping, raises failure rates [§8, PDF p. 13].
4. "Higher temperatures lead to increased failure rates, but do so most noticeably for SSDs that do not employ throttling techniques" [§8, PDF p. 13]. Throttled SSDs had lower failure rates, although "Such throttling could potentially reduce performance, though we are not able to examine this effect" [§5.1, PDF p. 10]. Temperature correlates with PCIe bus power, which "can potentially be used as a proxy for temperature" [§8, PDF p. 13].
5. "The amount of data reported to be written by the system software can overstate the amount of data actually written to flash chips" [§8, PDF p. 13].

### 3.3 NWJ+16: half a million Microsoft SSDs

**Population.** "over half a million SSDs that span multiple generations spread across several datacenters" over "nearly 3 years" [NWJ+16 Abstract, PDF p. 1].

- **NWJ+16-F1:** observed AFR for some models is "significantly higher (as much as 70%) than that quoted in SSD specifications" [§1, PDF p. 2].
- **NWJ+16-F2:** "Four symptoms - Data Errors (Uncorrectable and CRC), Sector Reallocations, Program/Erase Failures and SATA Downshift" are the most important SMART-reported symptoms, in that order. Symptoms precede failures, yet "these symptoms are not a sufficient indicator for diagnosing failures" [§1, PDF p. 2].
- **NWJ+16-F3: intensity, relative to the population.** "80% of healthy devices have fewer than 2 reallocated sectors", while "20% of failed devices have over 1000 sectors reallocated". The authors define high risk as above "the 80th percentile of each symptom’s CDF for all devices" and find such devices fail much faster [§3.3, PDF p. 5].
- **NWJ+16-F4:** a model using tens of factors and thresholds identified failed devices with 87% precision and 71% recall, and "Devices are more likely to fail in less than a month after their symptoms match failure signatures" [§1, PDF p. 2].
- **NWJ+16-F5: write amplification, both ways.** "devices with either very high or very low write amplification have higher failure rates" [§3, PDF p. 7]. (Low values can come from on-device compression.)

### 3.4 MMES20: 1.4 million NetApp enterprise SSDs

**Population.** About 1.4 million SSDs, three manufacturers, 18 models, SLC, cMLC, eMLC and 3D-TLC, 30 months [MMES20 §1, p. 137].

- **Finding 1:** a third of replacements come with severe SCSI errors, and "one third of drive replacements are merely preventative based on predictions" [§4, p. 141].
- **Finding 2:** "a very drawn-out period of infant mortality, which can last more than a year and see failure rates 2-3X larger than later in life" [§5.1, p. 142].
- **Finding 6:** "Earlier firmware versions can be correlated with significantly higher replacement rates" [§5.5, p. 145]; yet "70% of drives in our study remain at the same firmware version" [§8, p. 147].
- **Findings 7 and 8:** "SSDs with a non-empty defect list have a higher chance of getting replaced", and "SSDs that make greater use of their overprovisioned space are quite likely to be replaced in the future" [§5.8–5.9, p. 145].
- **Finding 9:** larger RAID groups see more replacements, but the rate of multiple failures per group does not grow with group size [§6, p. 146]; failures within a group are nonetheless correlated in time [§8, p. 147].
- **Wear** [§8, p. 147]: "99% of systems use at most 15% of the rated life of their drives", and "correlated failures due to infant mortality are likely to be a bigger threat" than correlated wear-out.

### 3.5 MMES22: operational state of 2 million NetApp SSDs

- **MMES22-F1: field write amplification varies over orders of magnitude.** 98.8% and 96% of drives exceed a write amplification factor (WAF) of 1.3 and 1.5; "While the 10th percentile is only 2, the 99th percentile is 480". Within one drive family, "The 95th percentile of a drive family’s WAF is often 9× larger than the corresponding median" [§3.2, p. 170].
- **MMES22-F2: firmware, not garbage collection, drove the worst cases.** Families with median WAF around 100 did background work whenever idle, "due to aggressive rewriting of blocks to avoid retention problems" [§3.2, p. 170].
- **MMES22-F3: fullness and over-provisioning barely matter in this fleet.** Comparing drives above and below 80% full, "we observe no significant differences in the WAF"; drives with 28% over-provisioning had *higher* WAF than drives with 7% [§4.3, p. 174; §4.4, p. 175]. Summary: "over-provisioning and fullness have little impact on WAF in practice, unlike commonly assumed" [Table 4, p. 177].
- **MMES22-F4: wear leveling is imperfect.** "5% of all SSDs report an erase ratio above 6" (the most-erased block wears six times as fast as the average), and "those blocks are more likely to experience errors and error correction contributes to tail latencies" [Table 4, p. 177].
- **MMES22-F5:** all-flash systems are 43% full on average, and 94% of workloads are read-dominant, with a median read/write ratio of 3.62 [Table 4, p. 177].

Caveat *(inference)*: NetApp's WAFL writes in large, log-structured units. That these drives' WAF did not depend on fullness says nothing about random-overwrite workloads, for which the analytic and lab results in §5.1 apply.

### 3.6 XZQ+19: 10,000 SSD-related failures at Alibaba

**Population.** Around 450,000 SSDs in seven data centers over three years; of more than 150,000 failure tickets, 5.6% (about 10,000) were reported as SSD-related (RASR) [XZQ+19 §1, p. 962; §2, p. 963].

- **XZQ+19-F1:** "a significant number (34.4%) of RASR failures are not caused by the SSD device"; drives plugged into the wrong slot alone caused 20.1% [§1, p. 962].
- **XZQ+19-F2: passive heating.** Poor rack layout "can increase the temperature of idle SSDs by up to 28" °C, "resulting in 57% more device errors after 128 hours of passive heating" [§1, p. 962]. The errors were raw bit errors, "most likely due to a retention issue" [§4.3, p. 968].
- **XZQ+19-F3: reading triggers the drive's own refresh.** Scanning idle SSDs to trigger the controller's read refresh, every 4 hours during 128 hours at 55 °C, left "only ... 1% more Raw Bit Errors, which is in stark contrast to 57% more Raw Bit Errors without scanning"; more frequent scanning did not help much [§4.3, p. 968]. The caveats: the "SMART logs are pulled on a daily basis", which is too coarse to see passive heating, and "the scanning might introduce more read disturb errors" [§4.3, p. 968].
- **XZQ+19-F4: uneven allocation wears some drives out.** A direct-mapped block service overused 15–20% of SSDs, causing up to 77.3% more device errors and up to 18.7% higher failure rates, until a shared append-only log evened the load [§1, p. 962].
- **XZQ+19-F5:** accumulating Ultra-DMA CRC errors indicate a faulty interconnect rather than a failing drive, and an indicator built on them improved repair [§1, p. 962].

### 3.7 LXZ+22: more than a million NVMe SSDs, fail-stop and fail-slow

**Population.** SMART logs, iostat traces and tickets for over one million enterprise NVMe SSDs at Alibaba [LXZ+22 Abstract; §1, p. 1005].

- **LXZ+22-F1:** infant mortality is not notable in NVMe SSDs [Finding 1, §1, p. 1005].
- **LXZ+22-F2:** high WAF no longer correlates closely with failure, and "NVMe SSD with low WAF (WAF≤1) exhibits 2.19× higher ARR than high-WAF ones" [§1, p. 1005].
- **LXZ+22-F3:** failures within a node or rack are more temporally correlated than for SATA SSDs (up to 14.69× and 1.78×), but over one day to one month rather than within minutes [§1, p. 1006; Finding 3].
- **LXZ+22-F4: fail-slow is common and independent of SMART.** "On average, 1.41% of NVMe SSDs are infected within four-month monitoring, which is 6.05× that of HDD" [§1, p. 1006]. "SMART attributes only exhibit negligible correlation with fail-slow metrics" [§5.3.3, p. 1015], and fail-slow drives rarely go on to fail-stop within five months [Finding 10, p. 1015]. The detection method is in §4.2.
- **LXZ+22-F5: severity and causes.** Fail-slow NVMe drives typically degrade to SATA-SSD latency (about 160 µs per event on average), and in several models the slowest 1% of events average about 22 ms [§5.2.2, p. 1013]. Of the 100 slowest drives returned to vendors, "33 of them have bad capacitors" and 46 had bad chips [§5.2.2, p. 1013].

### 3.8 CL20: learning "normal" when failures are rare

On more than 30,000 Google SSDs over six years, one-class models (isolation forest, autoencoder) trained only on healthy drives predicted "previously unseen SSD failure types with high accuracy", and "ignoring the minority class for training can improve the performance by up to 9.5% and if adaptability to dynamic environments is required, by up to 13%" [CL20 Abstract, PDF p. 1].

### 3.9 Flash characterization: retention and read disturb

These are chip-level laboratory studies of 2y-nm planar MLC, not field studies:

- "Retention errors, caused by charge leakage over time, are the dominant source of flash memory errors" [CLHMM15 Abstract, PDF p. 1]. The best read reference voltage drifts with retention age, which is why controllers retry reads at shifted voltages (GSA+18-F2 in §4.1 below).
- The probability of read disturb errors "increases with both higher wear-out and higher pass-through voltage levels" [CLGHMM15 Abstract, PDF p. 1]. CLGHMM15 reports, citing other work, that single-level-cell flash was expected to show read disturb errors after about a million reads per block, first-generation MLC after 100,000, and that "some modern MLC flash devices are now prone to read disturb errors after as few as 20,000 reads" [§1, PDF p. 1]. It also describes mitigations proposed elsewhere, such as rewriting a block once its page reads reach a fixed count, "(e.g., 50,000 reads for an MLC chip)" [§2.4, PDF p. 3].
- The field sees read disturb as a contributor to RBER but not to uncorrectable errors [SLM16 §9, p. 79; MWKM15 §8, PDF p. 13].

### 3.10 What the SSD evidence says

- **The signals that predict failure are errors and error trends**, not wear: prior UEs, bad-block growth, reallocations, write/erase/meta errors, and link downshift [SLM16 §5.6, §6.1.2; NWJ+16 §1]. Consumption of the spare area also predicts replacement [MMES20 Finding 8].
- **Wear matters mostly as a planning quantity.** Most drives use a small fraction of their rated life [MMES20 §8], and error growth with PE cycles is gradual [SLM16 §10].
- **Thresholds are again population-relative:** a per-model 95th percentile of factory bad blocks [SLM16 §6.1.2] and an 80th percentile of each symptom over all devices [NWJ+16 §3.3].
- **Fail-slow SSDs need their own detector**, because SMART does not see them [LXZ+22 §5.3.3].
- **Firmware version is a failure covariate** [MMES20 Finding 6] and a WAF covariate [MMES22 §3.2], so it belongs in the model key alongside vendor and model *(inference)*.

---

## 4. Fail-slow hardware: how common, how harmful, and how to tell slow from busy

### 4.1 Evidence

- **GSA+18-F1: every component class can fail slow.** From 101 reports at 12 institutions: "disk throughput can drop by three orders of magnitude to 100 KB/s due to vibration, SSD operations can stall for seconds due to firmware bugs" [GSA+18 §1, p. 1]. Faults convert between forms; slowdowns can be permanent, transient, partial or intermittent stops; and "It can take hours to months to pinpoint and isolate a fail-slow hardware" [Table 1, p. 2]. 39% of root causes are external (environment, configuration, power) [Table 1, p. 2]. The study excludes known slowdowns such as occasional garbage collection [§2, p. 2].
- **GSA+18-F2: SSD-internal causes** [§4.1, pp. 5–6]:
  - firmware that throttled I/Os "by exactly multiples of 250µs, as high as 23ms" [p. 5];
  - read retries at shifted voltages as flash wears ("We observed as high as 4 retries in the field") [p. 5];
  - RAIN (in-drive parity) reconstruction, which "occurs frequently in devices nearing end of life" [p. 5];
  - dying chips shrinking the over-provisioned area and triggering more garbage collection [p. 5];
  - heat causing repeated erases [p. 6];
  - write amplification of "5× for model “A”, 600× for model “B”" [p. 6].

  SSD engineers found that the number of bit flips is "a complex function of the time since the last write, the number of reads since the last write, the temperature of the flash, and the amount of wear on the flash" [§4.1, p. 5].
- **HSK16-F1: disk and SSD slowdowns relative to RAID peers** (458,482 disks and 4,069 SSDs, 87 days). With slowdown defined as a drive's hourly latency over the median of its RAID group, "we define “slow” (unstable) drive hour when Si ≥ 2" [HSK16 §1, p. 263]. Disks were slow 0.22% and SSDs 0.58% of drive hours, and RAIDs had at least one slow drive 1.5% and 2.2% of the time [Abstract, p. 263; §1, p. 264].
- **HSK16-F2: slowdowns persist, recur and are silent.** 40% of slow disks and 35% of slow SSDs stay slow for more than an hour; 90% and 85% of recurrences happen the same day as the previous one; 26% and 29% of drives slowed at least once [§1, p. 264]. Slowdowns are "not accompanied by observable drive events" [§1, p. 264], and "history-based tail mitigation strategies can be a fitting solution" [Finding 5, p. 268].
- **HSK16-F3: slowdowns are not load imbalance.** Only 5% of slow drive hours coincided with the drive serving twice its peers' I/O rate, and "only 1% and 5% of rate-imbalanced disk and SSD hours experience slowdowns"; "Slowdowns are independent of I/O rate and size imbalance" [§4.2, p. 269].
- **LXZ+22-F6: NVMe makes slowness visible.** A SATA SSD's roughly 100 µs latency masks faults that an NVMe drive's roughly 10 µs latency exposes [§1, p. 1005]; the prevalence and severity are in §3.7.
- **IASO-F1:** across 39,000 nodes, "the fail-slow annual failure rate in our field is 1.02%" [IASO Abstract, p. 47]. Before IASO the deployment had more than 25 full outages caused by cascading fail-slow incidents; afterwards, 2 [§1, p. 47].
- **Perseus-F1:** "annual fail-slow occurrences can be as frequent as annual fail-stop events (1%∼2%)" [Perseus §1, p. 49]. Isolating the 304 fail-slow drives found among 248,000 over ten months cut node-level 99.99th-percentile write latency by 48% [Abstract, p. 49; §1, p. 50].
- **Limplock:** degraded hardware (limpware) can drive a system into limplock, "a situation where a system progresses slowly due to the presence of limpware and is not capable of failing over to healthy components", and five cloud systems were not limpware-tolerant [Limplock Abstract, PDF p. 1].
- **ADR-F1: static timeouts miss most slow faults.** Existing slow-fault handling in distributed systems is "mostly controlled by static thresholds" [ADR Abstract, p. 359]. Performance can collapse within a narrow *danger zone* of fault severity, and "a milder slow fault can cause more harm than a severe one" [§1, p. 359]. "Tail latency is not always reliable in capturing slow faults", because healthy-period samples dilute it [Finding 9, p. 364].

### 4.2 Detection models, from simplest to most load-aware

| Model | Statistic | What separates slow from busy | Constants in the published version | Source |
|---|---|---|---|---|
| Peer ratio | drive's hourly mean latency ÷ median of its RAID group | peers receive the same (balanced) load, so load cancels out | slow if the ratio is ≥ 2 (also 1.5) | HSK16 §1, p. 263 |
| Fence + peer ratio | 3-hour median latency against the cluster's Q3 + 2·IQR; then per-15 s latency ÷ median of the 11 other drives in the node | a drive whose IOPS or throughput also exceeds Q3 + 2·IQR is excluded as heavy traffic | ratio ≥ 2 held for 5, 15, 30 or 60 minutes | LXZ+22 §5.1, pp. 1012–1013 |
| Timeout ratio | per 5 s epoch, timeouts ÷ responses seen from each peer; additive-decrease, multiplicative-increase score; 30th percentile of peers' scores over 10 minutes; DBSCAN outliers | normalizing timeouts by responses makes it load-aware | ratio threshold 0.1 "In our experience", 2-minute recovery, 10-minute window, at most one quarantine at a time | IASO §2.2, p. 50 |
| Latency-vs-throughput regression | per node and day: PCA + DBSCAN removes outlier ⟨latency, throughput⟩ points; polynomial regression of latency on throughput; 95% and 99.9% prediction upper bounds; slowdown ratio = latency ÷ bound | throughput is the load variable; "latency is more closely related to throughput than IOPS" | event if the median ratio exceeds 1 over half of a 5-minute window; daily risk score over durations and ratio bands | Perseus §3.5, p. 53; §4.2–4.5, pp. 54–56 |
| Percentile + rate cross-check | 99th percentile of a sliding window of the traced latency, plus the update frequency (responses per second) with mean ± standard-deviation bounds | rising frequency means more load (reset the windows); falling frequency with sustained high values means a slow fault | window sizes | ADR §5, pp. 367–368 |
| Per-I/O classifier | light neural network labels each I/O fast or slow at an "inflection point" of the device's latency distribution | features are recent queue and latency history; the device-specific boundary is learned | retrain when the inflection point moves by five percentiles | LinnOS §4.2, §4.4, pp. 176–180 |
| Period labeling | latency high *and* throughput low in the same period marks internal contention | "We should only be suspicious of device busyness when latency is high and throughput is low at the same time" | latency and throughput thresholds per trace | Heimdall §3, PDF p. 4 |

**What failed in production, and why:**

- **Fixed latency thresholds.** "the accuracy of threshold-identified fail-slow is low, as the latency is highly influenced by the workloads"; Alibaba keeps one only as a fail-safe, like a timeout [Perseus §3.2, p. 51].
- **Peer evaluation with hand-tuned constants.** "it took on-site engineers two hours to fine-tune a cluster with around 300 nodes, and this set of parameters fails to work on another cluster", even with identical drive models and service [Perseus §3.3, p. 52].
- **Cluster-wide models.** Latency-throughput distributions differed between clusters and even between nodes of one cluster, while "drives from the same node follow a similar LvT distribution" [Perseus §3.5, p. 53].
- **Timeout-based detection adapted to devices** reached a precision of only 0.48 [Perseus §3.4, p. 52].
- **Binary per-event labels.** Labeling every drive with one slow event as fail-slow gives "6K such “fail-slow” cases on a bad day" [Perseus §4.5, p. 56]. Hence Perseus's graded risk score and HSK16's persistence and recurrence statistics.
- **SMART.** It does not see fail-slow drives [LXZ+22 §5.3.3, p. 1015].

**Mitigations reported:**

- IASO reboots the slow service instance and removes its leadership, never quarantining more than one instance so the cluster stays within its fault tolerance [IASO §2.3, p. 51].
- Alibaba is experimenting with "three strikes" for NVMe fail-slow: wipe and redeploy; then zero-fill, reformat and redeploy; then retire. It has no results yet [LXZ+22 §5.2.2, p. 1013].
- GSA+18 recommends converting fail-slow to fail-stop and taking recurring *flip-flop* devices offline, with explicit challenges: shutdowns must not be triggered by false positives, some devices cannot be dropped without excessive re-replication, removing machines can cost availability ("some machines exhibited 10-20% performance degradation but if they were taken out, availability would be reduced"), and when the cause is external, "the solution is to isolate the external factors, not to shutdown the slow device" [§6.3, p. 11].

### 4.3 Separating slow from busy with measured quantities (derived)

The published detectors share one idea: compare latency against what the *same load* produces on a healthy device. mantle is the issuer of its own I/O, so it can measure load directly and exactly, rather than inferring it from sampled `iostat` averages as Alibaba must [LXZ+22 §5.1, p. 1012; Perseus §4.1, p. 54].

**Little's law.** "the average number of items in a queuing system, denoted L, equals the average arrival rate of items to the system, λ, multiplied by the average waiting time of an item in the system, W" [Little11 §1, p. 536]. It holds over a finite measurement window [§2, p. 538] "independent of queue discipline" and "under nonstationary conditions" [§2, p. 538].

**DERIVED consequences for a device over a window:**

1. mantle counts in-flight I/Os (L), completions per second (λ) and per-I/O latency (W) for each device. The identity W = L / λ checks the bookkeeping.
2. Calibration measures the device's healthy completion capacity at each concurrency, λ_cal(L) (`crates/disk/src/calibrate.rs` measures random-read throughput at queue depths 1, 4, 16 and 64 by default; §9.2 extends it to the operation mixes mantle issues, and note 11 §13 derives where the curve's knee lies).
3. At concurrency L:
   - A device that is **busy but healthy** completes near its calibrated rate: λ ≈ λ_cal(L), so W ≈ L / λ_cal(L). Its latency is high only because L is high, and admission control or placement fixes that.
   - A device that is **slow** completes fewer operations at the same concurrency: λ < λ_cal(L), so W > L / λ_cal(L).
4. The ratio λ_cal(L) / λ is a slowdown ratio that needs no peers. It needs the calibration curve and the offered concurrency, both of which mantle has.
5. This is Heimdall's condition (high latency *with* low throughput) [Heimdall §3, PDF p. 4] and ADR's (falling completion frequency with sustained high latency) [ADR §5, p. 368], stated with measured quantities.

**Mix and size.** Service time depends on the operation mix and transfer size. ReFlex models this as cost = ⌈size / 4 KiB⌉ × C(type, read ratio), calibrated per device, with writes costing 10–20 read tokens on the devices it measured [ReFlex §3.2.1, PDF p. 4]. The derived ratio should therefore use cost-weighted completions (tokens per second) rather than raw operations *(inference)*.

**How many samples a window needs.** If a window is used to estimate a p-quantile of latency, the probability that n independent samples all fall below the true p-quantile is pⁿ. Requiring that probability to be at most α gives n ≥ ln α / ln p (DERIVED from the definition of a quantile). For example, p = 0.99 and α = 0.05 give n ≥ 299. A window is therefore closed by a sample count (and a deadline), never by wall-clock time alone. At a low I/O rate that means a long window, which is the right behavior: a device that is barely used cannot be judged quickly *(inference)*.

**Why not an absolute threshold** (restating 4.2): identical models, and even identical clusters, need different constants [Perseus §3.3, §3.5]; a slow fault can hide below any static timeout [ADR §1]; and NVMe fail-slow events sit at around 160 µs, far below disk-era timeouts [LXZ+22 §5.2.2, p. 1013].

---

## 5. Device state that changes performance

### 5.1 Fullness and internal write amplification

- **Rules for writing to SSDs.** Note 03 §4 summarizes HKA17: SSDs reward large or concurrent requests, locality, aligned sequential writes, grouping by death time and uniform lifetimes; two sequential streams stop being sequential once a file system interleaves them or reuses space partially; discards should be issued promptly and in large units. The chunk store's segment layout follows those rules (chunk-store design §1, §8). Not repeated here.
- **Des12: the analytic model.** Under uniformly random overwrites, write amplification depends on the spare factor S_f = (T − U)/T, where U is the space holding live data and T the physical space [Des12 §2, PDF p. 2]. Greedy victim selection, which a cited proof shows optimal for uniform random traffic [§2, PDF p. 2], gives "with uniform arrivals A = 4.82" at S_f = 0.1 and 64-page blocks. When 90% of writes go to 5% of the address space, naive greedy cleaning gives 6.24, and separating hot and cold data with an optimal split of free space gives 1.86 [§5, PDF p. 7].
  - For least-recently-written cleaning the model has a closed form in Lambert's W function, A = α / (α + W(−α e^(−α))) with α = T/U [§2, PDF p. 2].
  - **DERIVED** from that formula: A = 7.32, 5.18, 2.69, 1.99 and 1.26 at S_f = 0.07, 0.10, 0.20, 0.28 and 0.50. Greedy cleaning does better than these values (4.82 against 5.18 at S_f = 0.1).
- **DIKS20: lab measurements.** "larger datasets lead to more valid pages in each flash block, which increases the amount of data being relocated upon performing garbage collection, i.e., the WA-D", for datasets of 0.25–0.88 of drive capacity [DIKS20 §4.4, p. 371]. Other pitfalls: "Short tests lead to results that are not representative of the long-term application performance", and results "may significantly vary depending on the initial state of the drive" [§1, p. 365]. DIKS20 measured device write amplification as the ratio of the vendor's `nand_bytes_written` to `host_bytes_written` counters [§3, p. 367].
- **MMES22: the field.** Fullness and over-provisioning had little effect on write amplification; firmware dominated (§3.5).
- **Reconciliation *(inference)*.** Fullness drives write amplification when the host overwrites small units at random, so that live and dead pages mix within erase blocks [Des12; DIKS20]. It matters much less when the host writes large, log-structured units and discards whole units, as NetApp's WAFL does [MMES22] and mantle's segments do (chunk-store design §1, §8). mantle's own segment cleaner has the same cost-versus-utilization shape one level up (note 03 §9, RO92), so volume fullness drives mantle's *own* write amplification, which it measures exactly.
- **Measuring device write amplification.** NVMe's Endurance Group Information log reports "Media Units Written", which "includes data bytes written by both the host and the controller (e.g., due to garbage collection)", beside "Data Units Written", which "does not include controller writes due to internal operations such as garbage collection" [NVMe24 Fig. 225, p. 239]. The ratio of their changes over a window is the device's write amplification (DERIVED). The log is optional: a value of 0h means the controller does not report the counter. The OCP extended log has an equivalent `physical_media_units_written` field [OCP-C0].

### 5.2 Garbage-collection interference and tail latency

- "The core problem of flash performance instability is the well-known and “notorious” garbage collection (GC) process" [TTFlash §1, p. 15]. In TTFlash's simulated baseline SSD, GC slowed the 99th–99.99th percentiles 5–138×; TTFlash's in-device fixes (plane-blocking GC, rotating GC, GC-tolerant reads from in-drive parity) brought them within 1.0–2.6× of no GC [Abstract, p. 15]. TTFlash needs device internals (intra-plane copyback, RAIN, capacitor-backed RAM), so a host cannot apply it.
- **Rails.** Random writes give "stable throughput up to a certain moment, after which the performance degrades and becomes unpredictable"; "When the drive has limited free space, random writes trigger the garbage collector resulting in unpredictable performance", because the collector cannot keep up, which "turns background operations into blocking ones" [Rails §3, p. 465]. A read-only workload shows virtually no variance [Fig. 2, p. 465].
- **Heimdall** quotes GC raising latency "by up to 60×" (citing earlier work) [Heimdall §1, PDF p. 1]. LinnOS found that for SSDs "queue length is not highly correlated with delay", so the queue-length heuristics that work for disks do not transfer [LinnOS §2, p. 175].
- **NVMe Predictable Latency Mode** is the standardized host interface for this problem. An NVM Set alternates between a Deterministic Window, "during which the NVM Set is able to provide deterministic latency for read and write operations", and a Non-Deterministic Window used for background work [NVMe24 §8.1.21, p. 682]. The host must follow operating rules (limits on 4 KiB random reads, writes and time in the window), reported with remaining budgets in the Predictable Latency Per NVM Set log page [§8.1.21, pp. 682–683]. The feature is optional and requires NVM Sets. How many drives mantle will meet that support it is **UNVERIFIED**. IODA builds on this interface (§8).

### 5.3 Thermal state and throttling

- **What the device reports** (**NON-PEER-REVIEWED**, NVMe24):
  - Composite Temperature, in kelvins [Fig. 213, p. 221].
  - Two thresholds in Identify Controller: WCTEMP, the lowest temperature that "indicates an overheating condition during which controller operation continues", and CCTEMP, the lowest that indicates a critical condition, "(e.g., may prevent continued normal operation, possibility of data loss, automatic device shutdown, extreme performance throttling, or permanent damage)". The specification recommends reporting 0157h for WCTEMP [Fig. 338, p. 357], which is 343 K, or 69.85 °C (DERIVED).
  - Two SMART counters of minutes spent at or above those thresholds (Warning and Critical Composite Temperature Time) [Fig. 213, p. 223].
  - Two counters of transitions into, and seconds spent in, host-controlled thermal management states: light throttling "while minimizing the impact on performance" at TMT1, heavy throttling "regardless of the impact on performance" at TMT2 [Fig. 213, p. 224; §8.1.19, pp. 670–671].
- **What the field shows.**
  - Higher temperature raises SSD failure rates mainly where the drive does not throttle [MWKM15 §8, PDF p. 13]. MWKM15 could not measure the performance cost of throttling [§5.1, PDF p. 10].
  - In SSDs heat causes "repeated erases" and wear, which become fail-slow symptoms [GSA+18 §4.1, p. 6].
  - Passive heating of idle drives raises raw errors, and periodic reads offset it [XZQ+19 §4.3, p. 968].
  - For disks, average temperature is a weak predictor and its variability a better one [ESA+12 Obs. 2; PWB07 §3.4].
- **What is missing.** We found no peer-reviewed measurement of throughput or latency under NVMe thermal throttling. The magnitudes in vendor application notes are not evidence here. mantle must measure throttling's effect per device (§9, R10).

### 5.4 SLC write caches and sustained-write cliffs

- YS20 describes the mechanism: hybrid designs that "program a part of QLC blocks in the single-level-cell (SLC) mode" as a cache "are widely adopted in the commercial solid-state disks", and data "must be migrated to the QLC region when the free blocks in the SLC region are insufficient"; commercial parameters are "determined heuristically" and fixed [YS20 Abstract and §1, PDF p. 1]. YS20 evaluates its own policy in a trace-driven simulator; it does not measure commercial drives.
- Rails observed a related write-throughput cliff after sustained random writes on 2014-era drives [Rails §3, p. 465].
- **We found no peer-reviewed measurement of the SLC-cache size, the post-cliff sustained write rate, or how either depends on fullness in current commercial drives.** Any such number in mantle must be measured on the device (§9, R11).

### 5.5 Zoned bit recording on disks

- Disks put more sectors on longer outer tracks, so "the transfer rate also varies with sector address". VM97 measured "a drop of roughly 25% in peak transfer rate depending on head position" on a BSD file system, and found that "a simple linear model adequately estimates the performance from the few parameters normally available in disk drive spec sheets" [VM97 Abstract]. The transfer rate of outer zones exceeded inner zones "by factors ranging from 1.45 to 1.9" across a vendor's 1997 models, and "empirical evidence indicates that the lower-numbered blocks (for a SCSI command set interface) are stored on the outer tracks" [VM97 §2]. The measurable effect (23–25%) was smaller than the physical difference (36%) [VM97 §6.4].
- VM97's drives date from 1997. That modern disks keep low LBAs on outer tracks, and the size of today's inner/outer ratio, are **UNVERIFIED** here; both are cheap to measure (§9, R12).

### 5.6 Aging: retention and read disturb

- **Retention.** Retention errors dominate raw flash errors in the lab [CLHMM15 Abstract]. In the field, some drive families rewrite blocks at idle "to avoid retention problems", which inflated their write amplification to around 100 [MMES22 §3.2, p. 170]. Reading data prompts the controller's read refresh [XZQ+19 §4.3, p. 968]. Worn flash needs read retries at shifted voltages, and these show up as higher read latency [GSA+18 §4.1, p. 5].
- **Host-initiated refresh.** NVMe 2.4 defines a Host-Initiated Refresh operation (a Device Self-test code) that "performs implementation-specific refresh operations that verify the media integrity and ensure access to media", for example "rewrite of underlying media that are exhibiting a high correctable error rate". It "does not change stored user data" [NVMe24 §8.1.13, p. 652]. Identify Controller reports whether it is supported, a recommended interval in days from the last power-down (RHIRI), and its nominal duration (HIRT) [Fig. 338, pp. 378–379]. **NON-PEER-REVIEWED**; how many drives implement it is **UNVERIFIED**.
- **Read disturb.** It raises RBER but not uncorrectable errors in the field [SLM16 §9; MWKM15 §8]. A full scrub pass reads each page once, so it adds at most one read per page per pass. Whether the reads a scrub adds to each erase block matter depends on the block's page count and the controller's read-count limit, and neither is visible to the host (DERIVED; UNVERIFIED for any given drive). XZQ+19 raises the same caveat for frequent scanning [§4.3, p. 968].

---

## 6. Health telemetry: what devices report and what a process can read

### 6.1 The NVMe SMART / Health Information log page (log identifier 02h)

**NON-PEER-REVIEWED** (NVMe24 §5.2.13.1.3, Figure 213, pp. 220–225).

The log page is 512 bytes. It covers the life of the controller and "is retained across power cycles unless otherwise specified". Hosts should request it with namespace identifier FFFFFFFFh [p. 220]. The specification notes that the command, data and busy-time counters are meant for computing throughput: "the number of Read or Write commands, the amount of data read or written, and the amount of controller busy time enables both I/Os per second and bandwidth to be calculated" [p. 220]. Critical warnings can also raise an asynchronous event if the host enables them with Set Features (§6.2).

| Bytes | Field | Units and semantics (NVMe24) |
|---|---|---|
| 0 | **Critical Warning** | Bit flags describing the state at the time of the Get Log Page command, not at the time of any related event [p. 221]. Bit 0 ASCBT: available spare below its threshold. Bit 1 TTC: a temperature at or beyond an over- or under-temperature threshold. Bit 2 NDR: "the NVM subsystem reliability has been degraded due to significant media related errors or any internal error that degrades NVM subsystem reliability". Bit 3 AMRO: all media placed in read-only mode (not because of namespace write protection). Bit 4 VMBF: the volatile-memory backup device (for example the capacitors that protect the write cache) has failed. Bit 5 PMRRO: the Persistent Memory Region became read-only or unreliable. Bit 6 IPS: a personality change left settings indeterminate. Bit 7 reserved. |
| 2:1 | **Composite Temperature** | Kelvins. Implementation-specific, and "may not represent the actual temperature of any physical point in the NVM subsystem". Compare it with WCTEMP and CCTEMP (§6.2) [p. 221]. |
| 3 | **Available Spare** | "a normalized percentage (0% to 100%) of the remaining spare capacity available" [p. 221]. |
| 4 | **Available Spare Threshold** | Normalized percentage; below it an event may occur [p. 221]. |
| 5 | **Percentage Used** | "a vendor specific estimate of the percentage of NVM subsystem life used based on the actual usage and the manufacturer’s prediction of NVM life". 100 means the estimated endurance is consumed "but may not indicate an NVM subsystem failure"; values above 100 are allowed and saturate at 255; updated once per power-on hour. JEDEC JESD218B-02 defines the endurance method [p. 221]. |
| 6 | **Endurance Group Critical Warning Summary** | The OR over Endurance Groups of: read-only, degraded reliability, and available spare below threshold [p. 222]. |
| 7 | **Informative Warning** | Bit 0: a voltage measurement exceeded its threshold [p. 222]. |
| 47:32 | **Data Units Read** | Thousands of 512-byte units, rounded up: 1 means 1–1,000 units, i.e. up to 512,000 bytes. 0h means not reported [p. 222]. |
| 63:48 | **Data Units Written** | Same units; host writes only, excluding metadata [p. 222]. |
| 79:64 | **Host Read Commands** | Count [p. 222]. |
| 95:80 | **Host Write Commands** | Count of User Data Out commands [p. 223]. |
| 111:96 | **Controller Busy Time** | Minutes during which a command was outstanding to an I/O queue [p. 223]. |
| 127:112 | **Power Cycles** | Count [p. 223]. |
| 143:128 | **Power On Hours** | Hours; "may not include time that the controller was powered and in a non-operational power state" [p. 223]. |
| 159:144 | **Unexpected Power Losses** | Formerly "Unsafe Shutdowns": power lost while the controller had not reported it was ready to be powered off. Afterwards initialization may take longer, "and data corruption may occur for any NVM subsystem that is not protected against power loss" [p. 223]. |
| 175:160 | **Media and Data Integrity Errors** | "the number of occurrences where the controller detected an unrecovered data integrity error", including uncorrectable ECC, CRC failure and LBA tag mismatch [p. 223]. |
| 191:176 | **Number of Error Information Log Entries** | Over the life of the controller [p. 223]. |
| 195:192 | **Warning Composite Temperature Time** | Minutes operational with Composite Temperature ≥ WCTEMP and < CCTEMP. Always 0 if either threshold is 0h [p. 223]. |
| 199:196 | **Critical Composite Temperature Time** | Minutes operational with Composite Temperature ≥ CCTEMP [p. 223]. |
| 215:200 | **Temperature Sensors 1–8** | Kelvins each; the sensor's location and accuracy are implementation-specific, and 0h means not implemented [pp. 224–225, Fig. 214]. |
| 223:216 | **Thermal Management Temperature 1 and 2 Transition Counts** | Number of entries into light (TMT1) and heavy (TMT2) host-controlled thermal management; saturate at FFFFFFFFh; 0h means never, or not implemented [p. 224]. |
| 231:224 | **Total Time For Thermal Management Temperature 1 and 2** | Seconds spent in those states; same saturation and meaning of 0h [p. 224]. |
| 239:232 | **Operational Lifetime Energy Consumed** | Watt-hours since manufacture, rounded up; 0h means not reported [p. 224]. |
| 243:240 | **Interval Power Measurement** | Average power over the most recent second, with a scale code; 0h means not reported [p. 225]. |

The Linux kernel's `struct nvme_smart_log` names the fields through byte 231 and leaves the rest reserved [LNX `include/linux/nvme.h`]. Apple's `NVMeSMARTData` names bytes 0–5 and 32–191 and declares bytes 6–31 and 192–511 as reserved arrays, although all 512 bytes are returned [APPLE `NVMeSMARTLibExternal.h`].

### 6.2 Other NVMe structures that matter for health (NON-PEER-REVIEWED, NVMe24)

- **Identify Controller** carries WCTEMP and CCTEMP (in kelvins; 0h means not reported; devices compliant with revision 1.2 or later "shall report a non-zero value") [Fig. 338, p. 357]. It also reports an Optimal Aggregated Queue Depth, "the recommended maximum total number of outstanding I/O commands across all I/O queues on the controller for optimal operation" [Fig. 338, p. 378], a device hint to compare with the measured concurrency knee (note 03 §15.4).
- **Temperature Threshold feature (04h).** An over- and an under-temperature threshold per implemented sensor. Crossing one sets the TTC critical-warning bit. The Composite Temperature's over-threshold defaults to WCTEMP [§5.2.30.1.3, p. 462].
- **Endurance Group Information log (09h).** Per Endurance Group: critical warnings, Available Spare and Percentage Used, an **Endurance Estimate** ("the total number of data bytes that may be written to the Endurance Group over the lifetime of the Endurance Group assuming a write amplification of 1"), Data Units Read and Written, **Media Units Written**, Media and Data Integrity Errors, and total and unallocated capacity. All byte counts are in units of 10⁹ bytes, rounded up, and 0h means not reported [§5.2.13.1.10, Fig. 225, pp. 237–239].
- **Asynchronous Event Configuration (feature 0Bh).** Bits 7:0 choose which SMART critical warnings raise an asynchronous event; setting a bit to 0 means no event is sent for that warning [§5.2.30.1.6, Fig. 474, p. 468].
- **Host-controlled thermal management** (TMT1 and TMT2, see §5.3) [§8.1.19, pp. 670–671]; **Predictable Latency Mode** (§5.2) [§8.1.21, pp. 682–683]; **Host-Initiated Refresh** (§5.6) [§8.1.13, pp. 652–653].
- **OCP "SMART / Health Information Extended" log (C0h)**, defined by the OCP datacenter SSD specification and implemented in nvme-cli, **NON-PEER-REVIEWED**, semantics **UNVERIFIED** against the specification [OCP-C0]. Its fields include:
  - physical media units written and read;
  - bad user and system NAND blocks (raw and normalized);
  - XOR recovery count, uncorrectable and soft-ECC read error counts, and end-to-end detected and corrected errors;
  - refresh counts, and maximum and minimum user-data erase counts;
  - thermal-throttling event count and current throttling status;
  - PCIe correctable errors, PCIe link retraining count, incomplete shutdowns and command timeouts;
  - percent free blocks, capacitor health, media dies offline, and die-failure tolerance.

  Several of them map directly onto signals in §3–§5: bad-block growth [SLM16], RAIN/XOR reconstructions [GSA+18], wear-leveling spread [MMES22] and throttling [MWKM15].

### 6.3 ATA SMART attributes the field studies found predictive

Attribute numbers and their semantics are vendor-defined. MSS17 says so explicitly [§2.1, p. 392], and the Linux kernel's SMART temperature driver adds that "SMART attributes are not well defined" [LNX `drivers/hwmon/drivetemp.c`, **NON-PEER-REVIEWED**]. Use raw values, and interpret their levels and rates within a model [BGBW16 §4; PWB07 §3.5.3]. The names below are those in MSS17 Table 3 [p. 394], LLP+20 Table 1 [p. 153], BGBW16 §4 and KRG19 footnote 2 [p. 347].

| ID | Name (as the sources give it) | Evidence |
|---|---|---|
| 5 | Reallocated Sectors Count | Critical threshold of one, 14× 60-day failure risk [PWB07 §3.5.2, p. 23]; failure probability rising from 1.7% to about 95% with the count [RAIDShield §3.4, p. 248]; predicted a week ahead [MSS17 §3.2.1]; used by Backblaze [KRG19 fn. 2] |
| 197 | Current Pending Sector Count (PWB07's "probational" counts, *inference*) | 16× [PWB07 §3.5.4, p. 25]; seen only on failing disks [RAIDShield §3.3.2, p. 248]; Backblaze |
| 198 | Uncorrectable Sector Count (related to PWB07's offline reallocations, *inference*) | 21× for offline reallocations [PWB07 §3.5.3, p. 24]; Backblaze; a change-point indicator for Seagate [BGBW16 §4] |
| 187 | Reported Uncorrectable Errors | Predicted a week ahead [MSS17 §3.2.1]; a change-point indicator [BGBW16 §4]; Backblaze |
| 188 | Command Timeout | Used by Backblaze [KRG19 fn. 2] |
| 196 | Reallocation Event Count | Leading indicator for Hitachi [BGBW16 §4] |
| 193 | Load/Unload Cycle Count | 63% of replaced Seagate drives showed a change point [BGBW16 §4] |
| 199 | UltraDMA CRC Error Count | Indicates cables and connectors, not the drive [PWB07 §3.5.5, p. 26; XZQ+19 §1, p. 962] |
| 7, 1 | Seek Error Rate, Read Error Rate | Model-specific; seek errors useful only with model-specific thresholds [PWB07 §3.5.5, p. 25] |
| 194 | Temperature | Weak predictor on average; variability matters [ESA+12 Obs. 1–2] |
| 9 | Power-On Hours | In Google's always-powered fleet, drive age approximated it well [PWB07 §3.5.5, p. 26] |
| — | "Scan errors" (background media scan errors) | Strongest single signal, 39× [PWB07 §3.5.1, p. 23]; ATA exposes them through vendor-specific attributes or logs (**UNVERIFIED** which) |

A drive also reports a one-bit SMART status, the vendor-threshold flag whose detection rate is 3–10% [MHK05 §1, p. 784]. Windows exposes it as `PredictFailure` (§6.6).

### 6.4 Linux: access paths and privileges

All from LNX at `v7.3-rc5`; **NON-PEER-REVIEWED**.

| Path | What it returns | Privilege (from source) |
|---|---|---|
| `NVME_IOCTL_ADMIN_CMD` / `NVME_IOCTL_ADMIN64_CMD` on `/dev/nvmeN` or the namespace node, carrying Get Log Page (02h, 09h, C0h) | Any log page | `nvme_admin_cmd_allowed()` lets an unprivileged caller send only Identify with CNS values Namespace, Controller and their command-set variants; every other admin command, Get Log Page included, "return capable(CAP_SYS_ADMIN)" (`drivers/nvme/host/ioctl.c` lines 17–58, 96–119). A denied command returns `EACCES` (line 347). |
| Device node access | Needed for any ioctl | devtmpfs creates nodes "owned by root and have a default mode of 0600" unless a subsystem sets otherwise (`drivers/base/devtmpfs.c` line 12). The NVMe driver sets no mode, so distribution udev rules decide (**UNVERIFIED** which). |
| hwmon for NVMe (`/sys/class/hwmon/hwmonN/` with name `nvme`) | `temp1_input` Composite and `temp2..9_input` sensors (m°C), `temp1_alarm` (the TTC bit), `temp1_crit` (CCTEMP), `temp1_max`/`temp1_min` (Temperature Threshold feature) | `temp*_input`, `temp1_alarm` and `temp1_crit` are mode 0444; the thresholds are 0644, writable only by root (`drivers/nvme/host/hwmon.c` `nvme_hwmon_is_visible`). **Each read of an input or the alarm makes the kernel issue a Get Log Page for the SMART log** (`nvme_hwmon_read` calls `nvme_hwmon_get_smart_log`), so reading hwmon costs one admin command. |
| NVMe diagnostic counters (Linux 7.2+, ABI dated May 2026) | `diag/command_error_count` and `diag/command_retries_count` for I/O commands, under `/sys/block/nvmeXnY/` or, when the kernel is built with `CONFIG_NVME_MULTIPATH`, under each path's `/sys/block/nvmeXcYnZ/` (the driver hides them on the multipath head, `nvme_ns_diag_attrs_are_visible`); `/sys/class/nvme/nvmeX/diag/command_error_count` (admin), `reset_count`, `reconnect_count` | Mode 0644: readable by all, and "All counters can be reset by writing any value" (`Documentation/ABI/stable/sysfs-nvme`; `drivers/nvme/host/sysfs.c`). A consumer must treat a decrease as a reset. |
| Asynchronous events | A `KOBJ_CHANGE` uevent with `NVME_AEN=...` for SMART, error and vendor events (`core.c` `nvme_aen_uevent`) | Unprivileged processes can receive kobject uevents (`NL_CFG_F_NONROOT_RECV`, `lib/kobject_uevent.c`). But the driver's `nvme_enable_aen` writes the Asynchronous Event Configuration feature with only the notice bits it supports (namespace attributes, firmware activation, ANA change, discovery change: bits 8, 9, 11, 31). DERIVED from NVMe24 Fig. 474: if the controller supports any of these, that write clears bits 7:0, and SMART critical warnings raise no event. If it supports none, the feature is not written and the controller's default applies. SMART must therefore be polled. |
| `SG_IO` on `/dev/sgN` or `/dev/sdX` | SCSI commands, including ATA PASS-THROUGH (ATA_12/ATA_16) for ATA SMART READ DATA | `scsi_cmd_allowed()` allows everything with `CAP_SYS_RAWIO`. Without it, a list of read-safe commands (including INQUIRY, MODE SENSE, LOG SENSE, RECEIVE DIAGNOSTIC and READ DEFECT DATA) is allowed to anyone who can open the node, and a list of write commands if it is open for writing. ATA_12 and ATA_16 are not on the list (`drivers/scsi/scsi_ioctl.c` lines 270–365). libata does not simulate LOG SENSE (`drivers/ata/libata-scsi.c` `ata_scsi_simulate`), so ATA SMART from a SATA disk needs `CAP_SYS_RAWIO` plus node access. SAS drives' LOG SENSE pages are open to anyone with node access; which pages they implement is defined by T10 standards not read here (**UNVERIFIED**). |
| `drivetemp` hwmon driver (SATA) | Drive temperature, lifetime minimum and maximum, and limits, via SCT or SMART attributes 194/190 | Attributes are 0444 (`drivers/hwmon/drivetemp.c`). The module registers as a SCSI interface; its only alias is `platform:drivetemp`, so it is loaded only if configured (inference from the source). Its documentation warns that on some drives, reading the temperature "may reset the spin down timer" (`Documentation/hwmon/drivetemp.rst`). |
| SCSI device counters | `/sys/block/sdX/device/{iorequest_cnt, iodone_cnt, ioerr_cnt, iotmo_cnt}` | `S_IRUGO` (`drivers/scsi/scsi_sysfs.c` lines 945–959). |
| Block-layer statistics | `/sys/block/<dev>/stat`: I/Os, merges, sectors and ticks (ms waited) per direction, discards and flushes, `in_flight`, `io_ticks` ("the number of milliseconds during which the device has had I/O requests queued"), `time_in_queue` | Mode 0444 (`block/genhd.c` line 1175). The 17 fields form "a consistent snapshot" (`Documentation/block/stat.rst`). |
| Identity, queue and zone attributes | See note 02 §2.3–2.4 | World-readable |

### 6.5 macOS: the IOKit NVMe SMART interface

- **Interface (NON-PEER-REVIEWED, APPLE).** `NVMeSMARTLibExternal.h` states that "NVMeSMARTLib implements non-kernel task access to NVMe SMART data". A service advertises support through the registry property `NVMe SMART Capable`. The plug-in (`kIONVMeSMARTUserClientTypeID`, interface `kIONVMeSMARTInterfaceID`) offers `SMARTReadData` (the 512-byte log), `GetIdentifyData` and `GetLogPage(data, logPageId, numDWords)`. `ATASMARTLib.h` provides the ATA equivalent: SMART read data, read thresholds and return status, and read log.
- **smartmontools (SMT).** It finds the device from its BSD name, walks up the service plane to the first SMART-capable parent, creates the plug-in with `IOCreatePlugInInterfaceForService`, and supports only Identify and Get Log Page for NVMe ("currently only GetIdentifyData and GetLogPage are supported", `os_darwin.cpp`).
- **[obs] Unprivileged access works on the research machine.** A short C program (kept in the scratch directory, not the repository) matched services with `NVMe SMART Capable` = true, created the plug-in, and read the log as uid 501 (an administrator account, not root), with an ad-hoc-signed binary and no entitlements.
  - It found one service, `IOEmbeddedNVMeBlockDevice`.
  - `SMARTReadData` and `GetLogPage(0x02)` both returned `kIOReturnSuccess`, and their first 32 bytes were identical. The values were plausible: Composite Temperature 312 K, Available Spare 100%, threshold 99%, Percentage Used 1%, Media and Data Integrity Errors 0, and all thermal counters 0.
  - `GetLogPage(0x01)` (Error Information) returned `kIOReturnDeviceError` (0xe00002e9).

  This shows unprivileged SMART access on this machine and macOS release. Other Macs, other releases, sandboxed or hardened-runtime binaries, and external or USB-attached drives are **UNVERIFIED**.
- **Identity** comes from the unprivileged registry walk in note 02 §3.2. The `/dev/disk*` ioctls need root (note 02 §3.5) and are not needed for health.

### 6.6 Windows

**NON-PEER-REVIEWED** (MSL; IOCTL codes [ms-meta], access bits DERIVED from the codes by the `CTL_CODE` layout, note 02 §4.2).

| Interface | What it returns | Access encoded in the IOCTL, and Microsoft's stated requirements |
|---|---|---|
| `IOCTL_STORAGE_QUERY_PROPERTY` with `StorageDeviceProtocolSpecificProperty` (50) or `StorageAdapterProtocolSpecificProperty` (49), `ProtocolType = ProtocolTypeNvme`, `DataType = NVMeDataTypeLogPage` | NVMe Get Log Page results, "including SMART/health data"; also Identify and Get Features. Such queries "can be retrieved in parallel with other I/O on the NVMe drive" (Working with NVMe drives) | Code 0x002D1400: `FILE_ANY_ACCESS`. Available since Windows 10. The NVMe page states no privilege requirement. |
| `IOCTL_STORAGE_QUERY_PROPERTY` with `StorageDeviceTemperatureProperty` (52) | `STORAGE_TEMPERATURE_DATA_DESCRIPTOR`: critical and warning temperatures in °C, and per-sensor `STORAGE_TEMPERATURE_INFO` (temperature, over and under thresholds, whether events are generated) | As above |
| `IOCTL_STORAGE_QUERY_PROPERTY` with `StorageDeviceEnduranceProperty` (62) | "how many bytes have been read/write from a solid-state drive (SSD)", for NVMe devices "that implement a certain NVMe feature" (`STORAGE_PROPERTY_ID`) | As above |
| `IOCTL_STORAGE_PREDICT_FAILURE` | `STORAGE_PREDICT_FAILURE`: `PredictFailure` nonzero when the drive "is currently predicting an imminent failure", plus 512 bytes of vendor-specific data. For IDE drives the class driver checks SMART support; for SCSI drives, the Informational Exceptions Control mode page. Unsupported devices fail with `STATUS_INVALID_DEVICE_REQUEST` (WDK pages) | Code 0x002D1100: `FILE_ANY_ACCESS` |
| `IOCTL_STORAGE_PROTOCOL_COMMAND` | NVMe pass-through, "intended for sending vendor-specific commands"; spec-defined queries should use `IOCTL_STORAGE_QUERY_PROPERTY` (Working with NVMe drives) | Code 0x002DD3C0: requires read and write access to the handle |
| `SMART_RCV_DRIVE_DATA` | Legacy ATA SMART | Code 0x0007C088: requires read and write access |
| `MSFT_StorageReliabilityCounter` (WMI, `Root\Microsoft\Windows\Storage`; `Get-StorageReliabilityCounter`) | Temperature, TemperatureMax, read and write errors (total, corrected, uncorrected), Wear (%), PowerOnHours, start-stop and load-unload cycles, and maximum read, write and flush latency in ms. The class page adds that "A value greater than 10 seconds may indicate a problem with the disk or the HBA" | The class and cmdlet pages state no privilege requirement |

- **Privileges.** The documents do not settle whether an unprivileged process can use these. The CreateFile documentation says opening a physical disk or volume requires administrator rights, while also allowing a handle with zero access for querying attributes, and the `FILE_ANY_ACCESS` IOCTLs need only a handle (note 02 §4.3).
- mantle's Windows probe opens the volume by its GUID path with no access rights and issues `IOCTL_STORAGE_QUERY_PROPERTY` for other descriptors (`crates/disk/src/probe/windows.rs`). Whether the protocol-specific (NVMe log), temperature and predict-failure queries succeed through such a handle, for a non-elevated process, is **UNVERIFIED**. The Windows CI target must test it.

### 6.7 What an unprivileged process can and cannot learn

| Signal | Linux (unprivileged) | macOS (unprivileged, [obs] for NVMe) | Windows (non-elevated) |
|---|---|---|---|
| mantle's own per-I/O latency, errors, checksum failures, throughput, in-flight count | Yes | Yes | Yes |
| Device temperature | NVMe: yes (hwmon). SATA: only if `drivetemp` is loaded | NVMe: yes (SMART log) | **UNVERIFIED** (temperature property) |
| NVMe critical warnings | Temperature bit only (`temp1_alarm`) | Yes | **UNVERIFIED** |
| NVMe spare, Percentage Used, media errors, data units, thermal counters | **No** (needs `CAP_SYS_ADMIN`) | Yes | **UNVERIFIED** |
| NVMe Endurance Group log (media units written) | **No** | Probably through `GetLogPage(0x09)` (**UNVERIFIED**, not tested) | **UNVERIFIED** |
| ATA SMART attributes | **No** (needs `CAP_SYS_RAWIO` and node access) | ATASMARTLib claims non-kernel access (**UNVERIFIED**; no SATA drive to test) | **UNVERIFIED** (reliability counters; predict-failure flag) |
| Kernel I/O error, retry and timeout counters | NVMe (7.2+) and SCSI: yes | No equivalent read | No equivalent read |
| Block-layer utilization and queue time | Yes (`/sys/block/*/stat`) | No equivalent found | Not examined |

**Consequence *(inference)*.** The only signals available everywhere without privileges are the ones mantle measures itself. The field evidence makes those the most valuable signals for fail-slow detection [LXZ+22 §5.3.3; HSK16 §1], and competitive for failure prediction [LLP+20 Fig. 8]. Device telemetry adds the error-counter and endurance signals of §2–§3 where the platform allows it.

---

## 7. What production systems do with health signals

### 7.1 Proactive draining and replacement

- **RAIDShield (EMC).** Replaces a disk once its reallocated-sector count passes a threshold chosen from the measured time-to-failure distribution, the replacement lead time and a false-positive budget. This removed about 88% of the recovery incidents caused by triple failures, about 70% of all disk-related incidents. Group-level risk (ARMOR) adds disks that are individually below the threshold but jointly endanger a RAID group [RAIDShield §4.3, p. 250; §5, pp. 251–252]. The disproportionate gain comes from needing to avoid only one of the three failures that would disable a RAID-6 group [§4.3, pp. 250–251].
- **NetApp.** A third of SSD replacements are "merely preventative based on predictions" [MMES20 Finding 1, p. 141]. The paper also finds that larger drives see fewer of these predictive failures, and asks whether they need "different types of failure predictors and potentially more input from the drive on its internal issues" [MMES20 §8, p. 147].
- **Microsoft Azure (CDEF).** Ranks disks by predicted error-proneness, migrates VMs off the top r, and stops allocating to them; r minimizes the expected misclassification cost [XWL+18 §3.2, p. 486; §5, p. 489].
- **Ceph devicehealth (NON-PEER-REVIEWED, CEPH).**
  - Health metrics come from `smartctl` and are scraped once every 24 hours by default (`scrape_frequency` 86400 s), and retained for 180 days.
  - A local predictor ("a pre-trained prediction model", per-manufacturer models that the source says were built "using the open source Backblaze SMART metrics dataset") needs at least 6 days of samples. It classifies a device as good (life expectancy above 6 weeks), warning (2–6 weeks) or bad (under 2 weeks).
  - `warn_threshold` (default 86400 × 14 × 6 s, i.e. 12 weeks) raises a health warning. With `self_heal` on (the default), `mark_out_threshold` (default 86400 × 14 × 2 s, i.e. 4 weeks) marks the device's OSD out so its data migrates.
  - "If the “self heal” module marks out so many OSDs that the ratio value of mon_osd_min_up_ratio is exceeded, then the cluster raises the DEVICE_HEALTH_TOOMANY health check" rather than continuing (Device Management page).
  - Every constant here is a fixed default, not derived from measurements. The 24-hour scrape interval also matches XZQ+19's observation that daily SMART pulls are too coarse for heating events [§4.3, p. 968].

### 7.2 Changing the scrub rate

- **Prediction-driven acceleration.** Scrubbing X times faster while a sector error is predicted: with X = 2 and 2% of the time accelerated, errors were detected 1.7–1.8× sooner on disks and 1.4–1.5× on two SSD models [MSS17 §4.3, p. 399]. The authors suggest scaling the rate continuously with the predicted error probability rather than switching between two speeds [§4.4, p. 399].
- **Principles from the latent-sector-error data (OJ10, simulation on a model fit to BGPS07)** [OJ10 Table 1, PDF p. 3]:
  - "Keep scrubbing rate low during the first 60 days of operation" (errors are rare then);
  - "Increase scrubbing rate after LSE detection";
  - "Staggered scrubbing ... is superior to sequential or randomized scrubbing";
  - "Scrubbing is not free: limit scrubbing rate to avoid collateral LSEs" (usage itself causes errors in their model).

  The authors "dispute the common belief that scrubbing is most effective at maximum capacity" [§2, PDF p. 3]. They warn that "our results are highly sensitive to some disk parameters that are not always made public by disk manufacturers" [§1, PDF p. 2].
- **Staggered order and neighbourhood checks** are covered in note 03 (§10.3, SDG10; §10.1, BGPS07). The chunk store's scrubber already paces a staggered pass to its period, scrubs continuously once the volume is at risk, and reports the volume failing, to be drained whole, when its bounded list of damaged chunks fills (`crates/chunk/src/scrub.rs`; chunk-store design §9).
- **Extending scans to unused sectors.** RAIDShield made its scrubber read free space too, because reallocations there still indicate the drive's condition [§4.1, p. 249].
- **Reads as refresh for idle flash.** In XZQ+19's test, periodic reads held the heat-driven rise in raw bit errors to 1%, against 57% without them [§4.3, p. 968].

### 7.3 Avoiding writes to, and reads from, suspect devices

- Azure stops allocating to predicted-faulty disks as well as migrating away from them [XWL+18 §2.2, p. 483; §5, p. 489].
- Latency-induced probation: "intermediate servers sometimes detect situations where the system performs better by excluding a particularly slow machine, or putting it on probation", while "the system continues to issue shadow requests to these excluded servers, collecting statistics on their latency so they can be reincorporated" [TAIL, "Latency-induced probation", p. 79]. Note 04 §R4 adopts this.
- Per-I/O rejection and revocation (MittOS, LinnOS, Heimdall) and routing reads away from writers (Rails, IODA) are in §8.

### 7.4 Placement against correlated wear-out, and wear balancing

- **Diff-RAID.** Because flash "Bit Error Rate (BER) of an SSD climbs as it receives more writes", balanced RAID "can wear out devices at similar times" [Diff-RAID Abstract, PDF p. 1]. Diff-RAID distributes parity unevenly so devices age at different rates, reshuffles parity at each replacement, and in simulation with measured BER data from 12 flash chips was "more reliable than RAID-5, in some cases by multiple orders of magnitude" [Abstract, PDF p. 1].
- **Field counter-evidence.** Most drives never approach their endurance limit [MMES20 §8, p. 147]; errors grow linearly with PE cycles with no spike at the limit [SLM16 §10, p. 79]; and correlated *infant* failures are the larger threat [MMES20 §8, p. 147]. Diff-RAID's premise therefore applies only to write-heavy placements whose projected wear-out dates actually converge *(inference)*.
- **Endurance-aware balancing.** Uneven allocation that overused 15–20% of SSDs raised their error and failure rates, and a shared append-only log fixed it [XZQ+19 §1, p. 962]. Inside a drive, wear leveling is imperfect: 5% of drives report an erase ratio above 6, meaning their most-erased blocks wear out six times as fast as the average block [MMES22 §3.3, p. 171; Table 4, p. 177]; the host cannot fix this, but it can observe the spread through the OCP erase-count fields where they exist [OCP-C0].

### 7.5 Repair prioritization

- **Urgency from the survivors' state.** BGPS07 accelerates repair when the surviving disks are older or recently had errors [§6.3, p. 299] (§2.3). ARMOR ranks RAID groups by the joint probability that two or more members fail, computed from each member's counters [RAIDShield Fig. 15, p. 252].
- **Correlated failures dominate.** Failures cluster in time [SG07 §5.2; JHZK08 Abstract; LXZ+22 Finding 3]. Note 04 §A5 records Ford et al.'s result that correlated failures dominate unavailability, and their recommendation to delay recovery dynamically, based on how a failure is classified and on the cell's recent failure history.
- **Rate-limit repair and start it early.** Unthrottled transition I/O saturated whole clusters for weeks; PACEMAKER's proactive start under a peak-I/O cap kept it at or below 5% [KMS+20 Abstract; §4; §5.1.2].

### 7.6 Mitigating fail-slow devices

See §4.2: IASO restarts the slow instance and quarantines at most one [IASO §2.3, p. 51]; Alibaba's "three strikes" is untested [LXZ+22 §5.2.2, p. 1013]; GSA+18 recommends slow-to-stop conversion for recurring offenders, with the caveats listed there [§6.3, p. 11].

---

## 8. Adapting I/O to device variability at run time

| System | Mechanism | Result | What it needs | Source |
|---|---|---|---|---|
| **Hedged requests** | Send a second copy after the first has been outstanding longer than the 95th-percentile expected latency for its class; cancel the loser | "This approach limits the additional load to approximately 5% while substantially shortening the latency tail". In a BigTable benchmark, hedging after 10 ms "reduces the 99.9th-percentile latency for retrieving all 1,000 values from 1,800ms to 74ms while sending just 2% more requests" | a replica or reconstruction path; a per-class latency distribution | TAIL p. 77 |
| **Tied requests** | Enqueue on two servers that cancel each other when one starts; stagger by "two times the average network message delay" | Median −16%, "achieving nearly 40% reduction at the 99.9th-percentile latency"; "the overhead of tied requests in disk utilization is less than 1%" | server-side cancellation | TAIL p. 78 |
| **MittOS** | The OS predicts whether an I/O can meet its deadline and returns `EBUSY` at once, so the application retries elsewhere | "no-wait approach helps reduce IO completion time up to 35% compared to wait-then-speculate approaches" | a latency model per resource. For disks, one-time profiling ("Our one-time profiling takes 11 hours") plus online correction by the gap between actual and predicted time. For SSDs, per-chip queues, "impossible without white-box knowledge of the device" (it used Open-Channel SSDs) | MittOS Abstract; §4.2–4.3, PDF pp. 1, 4–5 |
| **LinnOS** | A light neural network labels each I/O fast or slow; slow ones are revoked and failed over | Average latency 9.6–79.6% better than hedging and heuristics, "with 87-97% inference accuracy and 4-6µs inference overhead" | per load-device pair training; the fast/slow boundary is the "inflection point" of that pair's latency distribution; retrain when it shifts by five percentiles | LinnOS Abstract, p. 173; §4.2–4.4, pp. 176–180 |
| **Heimdall** | ML admission policy with period-based labels (latency high and throughput low) and noise filtering | Decision accuracy from 67% to 93%; "15-35% lower average I/O latency compared to the state of the art and up to 2× faster to a baseline"; sub-µs inference, 28 KB | retraining against drift: accuracy fell to 63–82% over an 8-hour trace, and retraining when accuracy dropped below 80% took 37 retrains | Heimdall Abstract; §7, PDF pp. 1, 11 |
| **IODA** | SSDs fail an I/O on purpose when busy with background work, returning a busy-remaining-time; the host reconstructs from parity. Devices take turns in busy windows so that at most one per stripe is busy | "improves the 95–99.99th latencies by up to 75×" | firmware changes ("only adds 5 new fields to the NVMe interface"); the time window must be "less than the size of the over-provisioning space (Sp) divided by the net write load" | IODA Abstract; §3.2–3.3, PDF pp. 1, 5, 7 |
| **Gimbal** | Treats the SSD as a network: latency feedback against a threshold that adapts between a minimum and a maximum (Reno-like), separate read and write token buckets, and a write cost estimated online | "up to x6.6 better utilization and 62.6% less tail latency" | the observation that a fixed 2 ms threshold "is only effective for large IOs (like 64/128KB) but cannot capture the congestion for small IOs promptly"; write cost starts from the worst-case read/write bandwidth ratio, from pre-calibration or the device specification, and is adjusted by latency | Gimbal Abstract; §3.2–3.4, PDF pp. 1, 5–6 |
| **ReFlex** | A cost model in tokens (a 4 KiB random read is one token) and a scheduler that issues tokens at the rate the device sustains at the tail-latency objective | Writes cost 10, 20 and 16 tokens on three NVMe devices | "We calibrate the cost model for each type of Flash device", fitting curves of tail latency against load with worst-case random writes; "The model can be re-calibrated after deployment to account for performance degradation due to Flash wear-out" | ReFlex §3.2.1–3.2.2, PDF p. 4 |
| **Rails** | Two or more replicas take turns: one serves reads while another absorbs writes, then they swap and resynchronize through a cache | Reads get read-only performance; the cache must hold T × 2w of writes (for example 4000 MB) | a write buffer and a period T ≥ T_min | Rails Abstract, p. 463; §4, p. 468 |

**What transfers to mantle *(inference)*:**

1. **Hedged and tied reads are already mantle's policy** (note 04 §R4). Device health adds two things: a device flagged slow (§9, R5) is taken out of the primary-read set (probation), and its hedge threshold comes from its peers' distribution rather than its own inflated one.
2. **Per-device cost models are measured, not assumed.** ReFlex's write costs of 10–20 read tokens differ by device, and Gimbal's cost moves with device state. mantle's calibration already measures read and write throughput per device (`crates/disk/src/calibrate.rs`), and a running estimate can track drift the way Gimbal's does.
3. **Latency-feedback control of background work.** Gimbal shows that a fixed latency threshold fails for small I/Os and that an adaptive threshold between measured bounds works. The same control loop can pace mantle's background I/O (scrub, cleaning, drain, repair) against foreground latency (§9, R6).
4. **White-box prediction is out of reach; black-box classification is not.** MittOS's SSD model needs Open-Channel devices. LinnOS and Heimdall work on black-box devices, but they need training per device and load, and retraining under drift. For mantle, the simpler measured signals of §4.3 come first; a learned per-I/O classifier is a later option, justified only by measured tail benefit on mantle's own workloads.
5. **IODA's contract requires firmware support that commodity drives do not advertise.** NVMe Predictable Latency Mode is the standardized version (§5.2). Where it is present, the host can read the window state and remaining budgets from the Predictable Latency Per NVM Set log page.
6. **Rails-style read/write separation** fits replicated data. For erasure-coded data it corresponds to reconstruction reads that avoid chunks on devices currently absorbing writes, under the reconstruction budget of note 04 §R4.

---

## 9. Implications for mantle

Everything in this section is **INFERENCE / Recommendation** or **DERIVED** unless a citation says otherwise. Each rule names its measured inputs, the calculation, why the calculation is right, the literature's number with its population where one exists, and the measurement to take where none does.

### 9.1 Where every number comes from

A number mantle uses to make a device decision comes from one of three places, in this order of preference:

1. **Measured on the device or node, now.** Calibration curves, running latency and throughput distributions, error counters, and drain and repair rates. This is the only source available on every platform without privileges (§6.7).
2. **Learned from mantle's own fleet**, per *model key*: vendor, model, firmware revision and capacity. Firmware belongs in the key because it correlates with replacement rates and write amplification [MMES20 Finding 6; MMES22 §3.2]. Conditional failure probabilities are estimated as RAIDShield does: devices of the key that reached a state and then failed, divided by all devices of the key that reached the state [RAIDShield Fig. 14, p. 251].
3. **A published value, labeled with its population** (§9.5), used only until the fleet's own estimate can decide the question.

**When the fleet's own estimate takes over (derived).** Each decision below compares an estimated probability p with a boundary p* computed from measured costs. The fleet's estimate replaces the published one as soon as its confidence interval lies entirely on one side of p*. A binomial proportion interval, such as the Wilson interval, from n devices and x failures is enough. This needs no hand-picked prior weight or minimum population: when the fleet's data cannot yet decide, the cited value decides; when it can, it does. The sample sizes the literature reports (about 10,000 disks per group for a steady-state AFR [KRG19 §2.2, p. 347], "thousands" [KMS+20 §1, p. 370]) show that a small deployment will rely on published values for a long time. That is the honest consequence of having few devices, not a defect in the rule.

**What is not a device threshold.** Two inputs are product requirements, stated by the operator or the design, not tuned to hardware:

- the durability objective that redundancy and repair must meet (note 04 §A5, §R6);
- the foreground latency objective, if one exists. Without one, the objective is to be no worse than the device's own measured baseline (R6).

Everything else is derived from these and from measurements.

### 9.2 What the chunk store measures, per device

| Quantity | How | Privilege | Used by |
|---|---|---|---|
| Per-I/O latency, operation type, size, submit and complete times, in-flight count at submit | mantle's I/O layer (it issues every I/O) | none | R5, R6, R7, R10 |
| Completions and cost-weighted completions (ReFlex-style tokens) per window, mean in-flight count | derived from the above; Little's law [Little11 §1, p. 536] | none | R5 |
| Unrecovered reads (`EIO`), checksum, identity and incarnation mismatches (the chunk store's `Corrupt` condition) | chunk-store read and scrub paths (chunk-store design §7, §9) | none | R2, R3, R8 |
| Flush latency distribution | the group-commit writer already measures batch service time (`crates/chunk/src/writer.rs`) | none | R5, R11 |
| Scrub results per segment: bytes, read-latency distribution, errors, and the age of the data read | the scrubber (`crates/chunk/src/scrub.rs`) | none | R8, R10 |
| Calibration: random-read throughput and latency against depth, sequential read and write throughput, durable-write latency | `crates/disk/src/calibrate.rs`, extended with the mixes and LBA points in R11–R12 | none | R5, R6, R11, R12 |
| Kernel counters: `/sys/block/*/stat`; NVMe `diag/*` (Linux 7.2+); SCSI `io*_cnt`; hwmon temperatures and alarm | sysfs reads | none (Linux) | R2, R5, R10 |
| NVMe SMART / Health log (02h), all fields of §6.1 | IOKit on macOS; Get Log Page on Linux; `IOCTL_STORAGE_QUERY_PROPERTY` on Windows | none on macOS [obs]; `CAP_SYS_ADMIN` on Linux; **UNVERIFIED** on Windows | R2, R3, R9, R10 |
| NVMe Endurance Group log (09h): Media and Data Units Written, Endurance Estimate | as above | as above | R9, R11 |
| OCP C0h, if present: bad blocks, XOR recoveries, soft-ECC and uncorrectable counts, refresh and erase counts, throttling status | Get Log Page | as above | R2, R3, R9, R10 |
| ATA SMART raw attributes (§6.3) | SG_IO ATA pass-through on Linux; ATASMARTLib on macOS; reliability counters on Windows | `CAP_SYS_RAWIO` on Linux; **UNVERIFIED** elsewhere | R3, R4 |
| Volume fullness, cleaner write amplification, and segment usage | the chunk store's own index (chunk-store design §8) | none | R11 |

**Privileged telemetry on Linux.** Only Get Log Page and ATA pass-through need privileges. Granting `CAP_SYS_ADMIN` to the storage process would give it far more power than it needs. The alternative is a separate helper process holding only the needed capability, which issues fixed read-only commands (Get Log Page 02h, 09h and C0h; ATA SMART READ DATA) and returns bytes. Without the helper, mantle degrades to the unprivileged rows and records that it did (note 02 §6.1's provenance, `Reported`/`Measured`/`Assumed`). Whether to ship a helper is a design decision this note does not make.

**Polling cadence (derived from the specification's units).** The SMART log's time counters are in minutes (Controller Busy Time, Warning and Critical Composite Temperature Time), Power On Hours in hours, and Percentage Used is updated once per power-on hour [NVMe24 Fig. 213, pp. 221–223]. Polling faster than once a minute therefore adds no information for those fields. The event counters (media errors, error-log entries) can change at any moment, but mantle sees the same events first in its own I/O errors. Each hwmon temperature read on Linux costs one Get Log Page (§6.4). Ceph's default of once a day [CEPH] is too coarse to see thermal events [XZQ+19 §4.3, p. 968].

**Counter hygiene.** Linux's `diag` counters can be reset by root (§6.4), NVMe byte counters are rounded up to 512,000-byte or 10⁹-byte units, and SMART raw values are vendor-encoded. Every consumer works on deltas, treats a decrease as a reset, and propagates rounding. For example, a write amplification computed from Endurance Group counters over a delta of Δ units carries a relative error of up to 2/Δ (DERIVED), so ±1% needs Δ ≥ 200, i.e. 200 GB of host writes.

### 9.3 What the placement layer aggregates

- **Per model key:** the conditional failure and error probabilities of R3 and R4, with counts and intervals; the AFR by age, computed as in KRG19 [§2.1, p. 346], with one-time bulk events filtered out before any change point is accepted [KRG19 §3.3, pp. 350–351]; and the distributions of fail-slow episode length and recurrence (R5).
- **Per node:** the peer groups of R5 (devices of the same class on one node), and node-wide events such as all devices slowing together (R5, R10).
- **Per stripe or copyset:** the probability that more chunks are lost than the redundancy tolerates, computed from each member's p (the ARMOR form [RAIDShield Fig. 15, p. 252]). This drives repair order and drain urgency (R3, R6).

### 9.4 Decision rules

#### R1. Device condition: states and flags

- **States:** `Healthy` → `Suspect` → `AtRisk` → `Draining` → `Retired`.
- **Independent flags:** `Slow` (R5), `Throttled` (R10), `WearProjected` (R9), `Unobservable` (telemetry missing).
- **Transitions** are driven by R2–R5. None uses a fixed count, temperature or latency.
- **Evidence for separating conditions:** fail-slow and fail-stop are largely independent. Fail-slow drives show no SMART signal and rarely become fail-stop within months [LXZ+22 §5.3.3, Finding 10, p. 1015], and slowdowns are silent [HSK16 §1, p. 264]. A slow device is not necessarily an unreliable one, so `Slow` is a flag with its own mitigation, not a step toward `Retired`.

#### R2. Integrity events escalate at once

- **Trigger:** any unrecovered read, checksum mismatch, identity or incarnation mismatch, an increment of NVMe Media and Data Integrity Errors, or an NVMe critical warning.
- **Why no threshold:** the first event is itself the signal. After a first scan error, reallocation, offline reallocation or probational count, disks were 39, 14, 21 and 16× more likely to fail within 60 days, and the critical threshold for all four was one [PWB07 §3.5, pp. 23–25]. After an SSD's first uncorrectable error, the next month has a nearly 30% chance of another, against 2% at random [SLM16 §5.6, p. 76]. Latent sector errors cluster within 10 MB and within a month [BGPS07 §5.4, p. 296].
- **Actions:**
  - Verify the neighbourhood (the chunk store already scans ±10 MiB, chunk-store design §9) and repair from redundancy.
  - Set `Suspect` and feed the event into R3.
  - A volume whose bounded damage list fills is already reported failing and drained whole (chunk-store design §9): that is `AtRisk` with an immediate drain, and needs no probability.
  - An identity or incarnation mismatch (a lost or misdirected write) is evidence the device does not store what it acknowledged; note 03 R9.4(d), from BGS+08, recommends draining after the first.
  - NVMe critical warnings, by their specification meaning [NVMe24 Fig. 213, p. 221]:
    - AMRO (all media read-only) and NDR (reliability degraded): `AtRisk`, drain now.
    - ASCBT (spare below threshold): `AtRisk`. Spare consumption predicts replacement [MMES20 Finding 8, p. 145].
    - VMBF (the backup for volatile memory failed): the device's cache is no longer power-loss-protected. mantle already flushes to durability on every commit (CLAUDE.md rule 6), so this changes no write path, but it marks the device `Suspect`.
    - TTC: `Throttled` (R10), not a reliability state.
- **Leaving `Suspect` (derived, instead of a fixed 30 days).** The chunk store currently keeps a volume at risk until it is reopened (`scrub.rs`), and its design cites a month [BGPS07; SLM16]. A measured rule: leave `Suspect` once the model key's rate of a further integrity event, at the time elapsed since the device's last one, is no longer distinguishable from the rate of the key's devices that have had no event, that is, once its confidence interval (as in §9.1) no longer lies above that rate. The state then ends when the device's history stops distinguishing it from its peers. Note 11 §12.4 proposes the same criterion for the scrubber's at-risk window.
  - Until the fleet has such data, the published figures apply: about 55–62% of disks with errors had another within a month [BGPS07 §5.4.3, p. 296], and a month after an uncorrectable SSD error carries a 30% chance of another [SLM16 §5.6, p. 76]. Errors arrive in heavy-tailed bursts (note 03 §10.3, SDG10), so the rate is estimated as a function of elapsed time, not summarized by a mean interval.

#### R3. Failure risk from counters: when to drain (derived)

- **Inputs:**
  - the device's model key, age, and counter state: its raw error counters and each one's change over the last window;
  - the fleet's (or the literature's) probability p(h) that a device in this state fails within h;
  - the measured drain time D = (bytes on the device) / (drain rate the R6 budget allows), and the re-evaluation window w (closed by sample count, R4);
  - the stripe geometry of the data on the device, and the other members' probabilities (§9.3);
  - the durability objective (§9.1).
- **Why a probability and not a counter level:** RAIDShield's failure probability rises smoothly with the count (1.7% with none, over 50% past 40, nearly 95% at 500–600, each within a 60-day window) and time to failure shrinks with it [§3.4, p. 248]. Any fixed count trades detection against false alarms in a way that depends on the model and the fleet [§4.2, p. 250], and attribute semantics differ by model [MSS17 §2.1].
- **What a failure costs that a drain does not (derived).**
  - *I/O.* A drain moves each chunk once: one read and one write, 2 units of I/O per unit of data. Rebuilding after a failure costs 2 units for a replicated chunk (one read from a surviving replica, one write) and k + 1 for a chunk of an RS(k, m) stripe, because "To reconstruct a lost chunk, k remaining chunks from the stripe must be read" [KMS+20 §2, p. 371]. Every device leaves service eventually, by failure or by planned retirement, and either way its data moves. A drain ahead of a failure therefore saves (k − 1) units per unit of erasure-coded data and nothing for replicated data. What an early drain gives up is the rest of the device's service, all of it when the prediction was a false positive.
  - *Durability.* A failure opens a window of reduced redundancy for the rebuild time; a drain does not. The probability of loss in that window follows from the other members' p and the measured rebuild time. SG07's warning applies: independence underestimates second failures ("four times larger" within an hour than an exponential model predicts [SG07 §5.3, p. 12]), so the placement layer uses its fleet's measured correlation, or the correlated-failure model of note 04 §A5, rather than independence.
- **Decision.**
  - The durability objective and the redundancy scheme fix a *tolerated* failure probability for a device over a horizon h: the largest p(h) at which its stripes still meet the objective, given the other members' probabilities and the measured rebuild time. This is PACEMAKER's tolerated-AFR, the "AFR tolerated by the redundancy scheme" [KMS+20 §5.1.2, p. 375], applied to one device instead of a disk group. Note 04 §R6's durability simulator is where mantle computes it.
  - Evaluate both at h = w + D: leaving the device in service for one more window is safe only if it survives that window and the drain that may follow. When p(h) exceeds the tolerated value, the device's stripes no longer meet the objective with it in them, so it enters `AtRisk` and is drained. If the drain cannot finish before the expected failure time, the repair priority of its stripes rises as well (the joint risk of §9.3).
  - Below the tolerated value a drain is justified only by the I/O it saves, p(h) · (k − 1) · B for B bytes of erasure-coded data, against the service the device gives up. Drain in that order only while free capacity and the R6 budget would otherwise sit idle.
  - A replacement lead time L enters only when there is no free capacity to drain into. RAIDShield's 3-day figure was the worst-case time to replace a disk in production [§4.3, p. 250]; in mantle the interval that matters is D, the time to move the data.
- **Capacity limit.** When several devices qualify, rank them by p and drain as many as the R6 budget allows, as Azure ranks disks by predicted error-proneness and flags the top r [XWL+18 §3.2, p. 486]. Azure's cost ratio of 3:1, set "by the domain experts" [XWL+18 §3.2, p. 486], is exactly the kind of number this rule replaces with measured I/O and durability costs.
- **Published numbers**, until mantle's own data decides (§9.1):
  - probability after the first event: 39× (scan), 14× (reallocation), 21× (offline reallocation) and 16× (probational) within 60 days, on Google consumer ATA disks, 2005–2006 [PWB07];
  - failure fraction within 60 days against reallocated-sector count, 1.7% → over 50% → about 95%, for EMC SATA model A-2 [RAIDShield];
  - UE after UE, 30% vs 2% per month, on Google MLC/eMLC/SLC SSDs [SLM16].

#### R4. Error trends and model-level change

- **Per device:** use each counter's change over a window, not only its total. Weekly increases were among the most useful prediction features [MSS17 §3.1.2, p. 394], and Azure's Diff(x, t, w) = x(t) − x(t−w) [XWL+18 §3.1, p. 484]. A shift that persists is the signal that matters [BGBW16 §2]. The window is closed by a count of new samples (the arithmetic of §4.3), not a fixed time.
- **Per model key:** track AFR by age as KRG19 does, and accept a change only after filtering one-time bulk events; without the filter, HeART would have declared wear-out early [KRG19 §3.3, p. 351]. Failure rates rise gradually rather than jumping [KMS+20 §3.2, p. 373], so a model-level rise is a planning input (R6, R9) earlier than it is a per-device alarm.
- **Evaluation of any predictor mantle adopts:** split by time, never randomly. Random splits leak future and environment-specific information [XWL+18 §2.2, p. 483]. Retrain on a moving window, because distributions drift [XWL+18 §5, p. 489; Heimdall §7].

#### R5. Fail-slow detection and response

- **Statistic.** For each device, per window closed by sample count (§4.3), per operation class:
  - latency quantiles;
  - cost-weighted completion rate λ (ReFlex tokens, with write cost measured per device [ReFlex §3.2.1]);
  - mean in-flight count L.
- **Load-aware comparison, three layers**, using whichever is available:
  1. **Against peers on the same node**, which are known to share a latency-throughput relation [Perseus §3.5, p. 53]. Fit latency against cost-weighted throughput across the node's same-class devices, excluding outliers before fitting as Perseus does (PCA + DBSCAN [Perseus §4.2, p. 55], or a simpler robust fit), and compute each device's slowdown ratio as observed latency over the fit's prediction bound [Perseus §4.4, p. 56]. With balanced load, the simple ratio to the peer median works [HSK16 §1; LXZ+22 §5.1].
  2. **Against its own calibration** (derived, §4.3): the ratio λ_cal(L) / λ at the observed concurrency.
  3. **Against its own history**, when it has no peers (a laptop or a one-disk node): an adaptive high quantile of its latency, with the completion rate as a cross-check, as in ADR [§5, pp. 367–368].
- **The flagging threshold is derived from a false-flag budget, not chosen.** A device is flagged when its ratio exceeds the model's prediction bound at coverage c in m consecutive windows. If windows were independent, a healthy device would be flagged by a given run of m windows with probability (1 − c)^m (DERIVED). By Little's law, the expected number of healthy devices on probation at once is the false-flag rate times the mean probation time. Choose c and m so that this expectation fits the probation allowance: the number of devices that can leave the primary-read set together without any stripe or copyset losing more members than its reconstruction budget allows. IASO applies the simplest such allowance and "only quarantines at most one instance" [IASO §2.3, p. 51]. Windows are autocorrelated, so the realized false-flag rate (flags that clear with no fault found) is measured and c and m are corrected against it. Perseus's published choices (95% and 99.9% bounds, a 5-minute span with a 50% proportion [§4.4, pp. 55–56]) worked on Alibaba's fleet; they are a starting point to validate, not constants to copy.
- **Node-wide slowness is not a device fault.** If all peers slow together, the cause is external: power, fan, vibration, temperature or a shared controller. GSA+18 reports 39% of root causes as external, and says "the solution is to isolate the external factors, not to shutdown the slow device" [Table 1, p. 2; §6.3, p. 11]. JHZK08 found interconnects and protocol stacks behind much of what looks like disk failure [Abstract, p. 111]. mantle flags the node, not the devices.
- **Response by timescale:**
  - **Per I/O:** hedge or tie reads (note 04 §R4); the hedge delay for a flagged device comes from its healthy peers' distribution.
  - **While flagged:** `Slow` removes the device from the primary-read set and from new-write placement (R7), keeping shadow reads to measure recovery [TAIL p. 79]. The flag clears when the device's statistic returns inside the peers' prediction bound over the same m windows. This supplies the unspecified N of note 04 §R4's "N healthy shadow responses".
  - **Persistent or recurrent:** treat as R3 with p replaced by the expected fraction of the device's remaining service time spent slow, estimated from the fleet's distribution of episode lengths and recurrence. HSK16's figures are the prior: 40% and 35% of slow disks and SSDs stay slow for over an hour, and 90% and 85% recur within the same day [§1, p. 264]. Recurrent devices are GSA+18's *flip-flop* case [§6.3, p. 11].
- **Why not SMART or a fixed latency:** SMART does not see fail-slow [LXZ+22 §5.3.3]; fixed thresholds fail across loads [Perseus §3.2]; and hand-tuned peer constants did not transfer between clusters [Perseus §3.3].

#### R6. Budgets for background I/O: scrub, cleaning, drain, repair

- **Measured baseline.** The foreground latency distribution per operation class while no background I/O runs, and its run-to-run variation, from repeated calibration rounds (the calibrator already runs several rounds and reports the median, `calibrate.rs`).
- **Control.** Background I/O is paced by a feedback loop on foreground latency: increase the background rate while foreground latency stays within the baseline's measured variation band, and back off when it leaves it. This is Gimbal's delay-based control, whose adaptive threshold replaced a fixed one that failed for small I/Os [Gimbal §3.2, PDF p. 5], applied to mantle's own background work. The band comes from the measured round-to-round spread, not from a chosen percentage. Note 11 §13.3 proposes the same statistical treatment for the fixed 10% margin in `Calibration::random_read_knee`.
- **Early start under the budget (derived from KMS+20).** A drain or redundancy change of B bytes at the budgeted rate r takes B / r. It must start at least that long before the risk crosses the tolerated level, computed from the model-level trend (R4) or the device's p (R3). This is PACEMAKER's proactive rule [§5.1.2, p. 375]; without it, work arrives in bursts that consumed 100% of cluster I/O for weeks [KMS+20 Abstract, p. 369].
- **Priority within the budget.** Stripes with the least remaining margin first (note 04 §R3), then by the ARMOR-form joint risk (§9.3), then scrub. Every queue is bounded (CLAUDE.md rule 2).

#### R7. Placement and write avoidance

- **Candidates.** Devices in `Suspect`, `AtRisk`, `Draining` or flagged `Slow` or `Throttled` receive no new chunks while the failure-domain constraints (note 04 §R2) can be met without them.
- **Why (derived).** Every byte written to a device that will be drained is moved again. The expected extra I/O per byte written is 2 · P(drained before the byte is deleted). mantle can estimate that probability from the device's p (R3) and the data's measured lifetime distribution (the lifecycle and lazy-deletion statistics the metadata service keeps).
- **Among healthy devices,** use two random feasible candidates with a score (note 04 §A8, which also warns against herding on stale data). Add to the score the device's expected residual life from R3 and its projected wear (R9), both measured quantities.
- **Reads** avoid `Slow` devices as in R5, and avoid devices busy absorbing writes when a reconstruction or another replica is within the reconstruction budget (Rails [§4, p. 468]; IODA [§3.2]; note 04 §R4).

#### R8. Scrub scheduling

- **Base rate.** Each device must complete a verification pass within its period T_i. The chunk store's defaults (7 days, at most 14) follow reported practice (note 03 R9.1); note 11 §12.2 finds them to be practice at NetApp and Ceph rather than derived values, and §12.4 there bounds one volume's period from its error rate and the scrub bandwidth. The allocation below divides a shared scrub budget among devices once their error rates are measured.
- **Allocation across devices (derived).** Suppose latent errors on device i arise at rate r_i per unit time, uniformly over its capacity C_i, and a sequential pass of period T_i finds each after T_i / 2 on average. The node minimizes the total expected time errors stay undetected, Σ r_i T_i / 2, subject to the scrub bandwidth budget Σ C_i / T_i ≤ S (S from R6). The Lagrangian condition gives T_i = sqrt(2 μ C_i / r_i): **the scrub period should scale as sqrt(C_i / r_i)**, i.e. bandwidth ∝ sqrt(r_i C_i). Devices with twice the predicted error rate get periods 1/√2 as long, not half as long. Each T_i must still respect the single-volume bounds of note 11 §12.4.
  - r_i comes from R3 and R4: the device's own error history and its model key's error rates by age. This makes MSS17's two-speed acceleration continuous, which is the refinement the authors propose [MSS17 §4.4, p. 399]. The two-speed version already detected errors 1.7–1.8× sooner on disks at 2% extra time [§4.3, p. 399].
  - It also subsumes OJ10's principle to "Keep scrubbing rate low during the first 60 days of operation": young disks' measured r_i is low, so their period is long [OJ10 Table 1, PDF p. 3].
- **Order and neighbourhood.** The scrubber's staggered order and the neighbourhood checks follow note 03 R9.2 and R9.4. Scan free space as well, because reallocations found there still predict failure [RAIDShield §4.1, p. 249].
- **Flash retention (inference).** The scrubber records each segment's read latency together with the age of the data it read. On flash, worn or long-retained data needs read retries at shifted voltages [GSA+18 §4.1, p. 5; CLHMM15 Abstract], so a rise in read latency with data age, relative to younger data on the same device, is a host-visible retention signal.
  - When the signal rises, shorten that device's period for old segments, or have the cleaner relocate them. Relocation is the host-side refresh that some firmware performs itself [MMES22 §3.2].
  - Reading also prompts the drive's own read refresh [XZQ+19 §4.3, p. 968].
  - Where the device supports Host-Initiated Refresh, its recommended interval after power-off (RHIRI) is a device-provided input for devices returning from storage [NVMe24 §8.1.13, Fig. 338].
  - Heat history (R10) raises r_i for retention.
- **Never scrub harder than R6 allows.** Scrubbing can itself cause errors (OJ10's collateral LSEs [OJ10 Table 1, PDF p. 3]; read disturb on flash [XZQ+19 §4.3, p. 968; CLGHMM15]), and its cost is paid by foreground latency.

#### R9. Wear projection and balancing

- **Life consumed.** Where the Endurance Group log is readable, the fraction of life consumed is Media Units Written divided by the Endurance Estimate, since the estimate assumes a write amplification of 1 [NVMe24 Fig. 225, pp. 238–239] (DERIVED). Otherwise use Percentage Used [Fig. 213, p. 221], a vendor estimate with 1% resolution updated hourly, or OCP's `endurance_estimate` and physical media units.
- **Projection.** Fit consumption against time over a window long enough that the fit's slope interval is narrower than the decision needs (the Percentage Used resolution makes this long), and project the date consumption reaches 1.
- **Balancing (derived).** Spread new writes so that projected wear-out dates are *equal* across devices of a model key, because overused drives fail more [XZQ+19 §1, p. 962]. Do not let two members of a stripe or copyset project within D of each other (R3's drain time, plus L when the data can only move to new hardware), which is the correlated wear-out Diff-RAID guards against [Diff-RAID Abstract]. The two goals conflict only for devices sharing stripes, and there the stripe constraint wins.
- **When to act.** Only when projections fall inside the planning horizon. In NetApp's fleet 99% of systems used at most 15% of rated life [MMES20 §8, p. 147], and errors grow gradually with PE cycles [SLM16 §10], so for most deployments R9 will observe and not act.
- **Wear is not a failure predictor.** Wear does not replace R3; NetApp found infant mortality the larger correlated risk [MMES20 §8, p. 147]. Keep the model key's age-conditional rates (R4) for that.

#### R10. Temperature and thermal throttling

- **Measure:** Composite Temperature and sensors; WCTEMP and CCTEMP; the Warning and Critical Composite Temperature Time counters; the thermal-management transition counts and times [NVMe24 Fig. 213, pp. 223–224; Fig. 338, p. 357]; and, on Linux without privileges, hwmon's `temp*_input`, `temp1_alarm` and `temp1_crit` (§6.4).
- **`Throttled`** is set while a thermal-management counter advances, the TTC bit is set, or the Composite Temperature is at or above WCTEMP. While set, the device's performance baseline is its measured throttled performance, not its calibration, so R5 does not mistake throttling for a fault. R7 moves writes away. That is the response the specification itself gives for WCTEMP: "Immediate remediation is recommended (e.g., additional cooling or workload reduction)" [NVMe24 Fig. 338, p. 357]. MWKM15 could not measure what throttling costs in performance [§5.1, PDF p. 10].
- **Several devices on one node throttling or heating together** is an environment event [GSA+18 Table 1; XZQ+19 §1]: flag the node.
- **Reliability.** Do not use average temperature as a disk-failure threshold [PWB07 §3.4; ESA+12 Obs. 1]. Temperature *variability* can enter the model key's risk estimate as a covariate [ESA+12 Obs. 2]. For SSDs, heat history raises the retention error rate that R8 uses [XZQ+19 §4.3; CLHMM15].
- **Measure the performance cost:** no peer-reviewed number exists (§5.3). Record throughput and latency in each thermal state against calibration.

#### R11. Fullness, write amplification and sustained-write cliffs

- **Device write amplification** = Δ Media Units Written / Δ Data Units Written, where readable (§5.1). Track it per device against its own history and its model key's distribution. In NetApp's fleet it ran from 2 at the 10th percentile to 480 at the 99th [MMES22 §3.2, p. 170]. Very high and very low values went with higher failure rates on Microsoft's SSDs [NWJ+16 §3, PDF p. 7], and very low values on Alibaba's NVMe SSDs [LXZ+22 §1, p. 1005].
  - Its percentile within the model key's distribution enters R3 as a covariate like any counter, with the conditional failure probability estimated per percentile band; the studies say both tails matter. A rise far outside the device's own history has known causes: retention rewriting [MMES22 §3.2, p. 170], bad chips reducing over-provisioning [GSA+18 §4.1, p. 5], or firmware.
- **mantle's own write amplification** is the cleaner's relocated bytes over client bytes, measured exactly. Fullness drives it through the segment-utilization mechanics of note 03 §9, and the cleaner's watermarks should follow from measured cleaning cost, not fixed fractions (note 11 §10 derives them).
- **Keep the device's write amplification low by construction.** Large aligned segments, grouping by death time, and discard of whole freed segments (note 03 R3.1–R3.7) make the SSD's garbage collection cheap regardless of fullness. MMES22 saw no fullness effect in a log-structured fleet [§4.3, p. 174]. Random-overwrite analyses [Des12; DIKS20] do not apply to mantle's own writes, but they do apply to any shared device where other tenants write randomly.
- **Sustained-write cliffs (measure passively).** The group-commit writer measures each batch's write throughput. Detect a change point in throughput against bytes written within a burst: a level shift that persists (as in BGBW16 §2), with its size set by the burst's own variance. That point estimates the device's write-cache capacity, and the level after it is the sustained rate. Capacity planning, drain rates (R6) and group-commit sizing use the sustained rate. No peer-reviewed commercial measurement exists (§5.4), and deliberately writing until the cliff wears the device, so an explicit calibration of this kind belongs only in an operator-requested calibrate mode, as note 02 §6.3 already prescribes for the SMR check.

#### R12. Disk zones

- **Measure** sequential read throughput at several LBA offsets during calibration. VM97 found a linear model in LBA adequate, within about 8% on one drive [§4], so the number of points needed follows from the fit's residual: add points until the residual is inside the measurement's round-to-round spread.
- **Uses:**
  - R5's expected throughput for a disk depends on where it is reading, so a disk reading its inner zones is not flagged slow;
  - scrub and drain durations (R6, R8) are integrals of the zone model over the data's location;
  - metadata that is read often (the chunk store's index log sits near the start of the volume, chunk-store design §2) is on the faster outer tracks if low LBAs map outward, which is **UNVERIFIED** for modern disks (§5.5) and cheap to measure.

### 9.5 Published numbers mantle may use as priors, with their populations

| Number | Meaning | Population | Source | Use in mantle |
|---|---|---|---|---|
| 39×, 14×, 21×, 16× | 60-day failure risk after the first scan error, reallocation, offline reallocation or probational sector | >100,000 Google consumer ATA disks, 80–400 GB, 2005–2006 | PWB07 §3.5, pp. 23–25 | R2 escalation (first event); R3 prior |
| 1.7% → >50% → ~95% | Fraction failing within 60 days with 0, >40 and 500–600 reallocated sectors | EMC Data Domain SATA disks, model A-2 | RAIDShield §3.4, p. 248 | R3 prior |
| median TTF < 3 days beyond 200 RS; replacement up to 3 days | Time to failure against lead time | EMC Data Domain | RAIDShield §4.3, p. 250 | R3's horizon (RAIDShield's replacement time plays the role of mantle's D) |
| 30% vs 2% | Probability of an uncorrectable error in the month after one, against a random month | Google MLC, eMLC and SLC SSDs, six years of production | SLM16 §5.6, p. 76 | R2, R3 prior |
| 50% | Chance of hundreds of bad blocks after 2–4 | Same | SLM16 §6.1.2, p. 77 | R3 prior (bad-block count, OCP C0h) |
| 0.5 within 10 MB; 55–62% within a month | Spatial and temporal locality of latent sector errors | 1.53 million NetApp disks | BGPS07 §5.4, p. 296 | R2 neighbourhood; `Suspect` exit prior |
| 0.2% / 0.6% of drive-hours ≥ 2× peers; 40%/35% persist > 1 h; 90%/85% recur same day | Fail-slow prevalence and persistence (disk / SSD) | 458,482 disks and 4,069 SSDs, NetApp | HSK16 §1, pp. 263–264 | R5 priors |
| 1.41% in 4 months (6.05× disks); ~160 µs events | NVMe fail-slow prevalence and severity | >1 million Alibaba NVMe SSDs | LXZ+22 §1, p. 1006; §5.2.2, p. 1013 | R5 prior |
| 1.02% per year | Fail-slow annual rate | 39,000 Nutanix customer nodes | IASO Abstract, p. 47 | R5 prior |
| 1.7–1.8× (disk), 1.4–1.5× (SSD) at 2% time | MTTD improvement from doubling the scrub rate on predicted errors | Backblaze disks; Google MLC SSDs | MSS17 §4.3, p. 399 | R8 expectation |
| 5% peak (0.2–0.4% average) | Transition I/O under proactive start | Four clusters, 110K–450K disks | KMS+20 Abstract, p. 369 | R6 sanity bound, not a constant |
| 2–480 (p10–p99) | Device write amplification | ~2 million NetApp SSDs | MMES22 §3.2, p. 170 | R11 plausibility range |
| ≤ 15% of rated life for 99% of systems | Wear consumption | NetApp enterprise systems | MMES20 §8, p. 147 | R9 "observe, rarely act" |
| 3–10% at 0.1%/year false alarms | Detection by the drive's own SMART threshold | Vendor estimate cited by MHK05 | MHK05 §1, p. 784 | Why the one-bit flag (`PredictFailure`) is not enough |

### 9.6 Measurements mantle must take because no published number exists

1. Each device's calibration curves for the operation mixes mantle issues: read and write, sizes, and depths (extends `calibrate.rs`).
2. Each device's write cost relative to reads, tracked over time (ReFlex's and Gimbal's calibrations show this varies by device and state).
3. Run-to-run variation of every calibrated quantity, which sets the bands in R5, R6 and R12.
4. Drain and rebuild rates achievable under the R6 budget, per device class; and the replacement lead time L, for when capacity must be added before a drain.
5. The rate of further integrity events against time since the last one, and the rate for devices without events, per model key (R2's exit rule).
6. Fail-slow episode length and recurrence in mantle's fleet (R5), to replace HSK16's priors.
7. Performance under each thermal state (R10).
8. Sustained-write rate after the write cache is exhausted, learned passively (R11).
9. Device write amplification where readable, and mantle's cleaner write amplification (R11).
10. The zone throughput profile of each disk (R12).
11. Scrub read latency against data age on flash (R8's retention signal).
12. Which telemetry each platform actually returns without privileges, recorded per device (§6.7): the Windows queries and macOS ATA access in particular.

---

## 10. What remains unknown

**Access and privileges**

1. Whether a non-elevated Windows process can obtain NVMe log pages (`StorageDeviceProtocolSpecificProperty`), temperatures (`StorageDeviceTemperatureProperty`), the endurance property, `IOCTL_STORAGE_PREDICT_FAILURE` or `MSFT_StorageReliabilityCounter` through a volume handle opened with no access rights (§6.6; note 02 §4.3). This must be tested on the Windows CI targets, both elevated and not.
2. Whether macOS's unprivileged NVMe access [obs] holds on other Macs, other releases, sandboxed or hardened-runtime binaries, and external or USB-attached drives. Whether `ATASMARTLib` works without root, and whether `GetLogPage(0x09)` returns the Endurance Group log on Apple's controllers, were not tested.
3. Which distributions give `/dev/nvme*` or `/dev/sd*` group access by default. The kernel's default is root, mode 0600 (§6.4).
4. Whether controllers mantle will meet enable SMART critical-warning asynchronous events by default when the Linux driver leaves the feature unwritten (§6.4).

**Device behavior with no published measurement**

5. Throughput and latency under NVMe host-controlled thermal management, per drive (§5.3).
6. SLC write-cache size, the sustained rate after it fills, and their dependence on fullness, on current commercial TLC and QLC drives (§5.4).
7. How many datacenter drives implement Predictable Latency Mode, Host-Initiated Refresh, the Endurance Group log with Media Units Written, and the OCP C0h log. The OCP specification's field semantics were not read (OCP-C0).
8. Whether modern disks keep low LBAs on outer tracks, and today's inner-to-outer throughput ratio (§5.5; VM97 measured 1997 drives).
9. How much a periodic scrub read adds to a flash block's read-disturb count, which depends on block geometry and controller policy that the host cannot see (§5.6).

**Transferability of the field results**

10. Every predictor in §2–§3 was fit to one operator's fleet, models and era: consumer ATA disks of 2005–2006 (PWB07), MLC SSDs of 2010–2015 (SLM16), and so on. Whether PWB07's 60-day factors or SLM16's 30% month-after-UE figure hold for current high-capacity disks or 3D-TLC and QLC NVMe drives is **UNVERIFIED**. mantle uses them only as labeled priors until its own data decides (§9.1).
11. Whether the latency-throughput regression of Perseus, validated on Alibaba's multi-drive nodes (for example all-flash nodes of 12 SSDs [Perseus §3.5, p. 53]), works on heterogeneous nodes or on nodes with two or three devices. The Little's-law ratio (§4.3) needs no peers, but has not been evaluated in a published study.
12. The fraction of failures that neither counters nor performance signals anticipate. PWB07 found more than a third of failed disks with no SMART signal; the corresponding share with performance signals added is not reported for a general fleet.

**Choices §9 leaves to later measurement**

13. The prediction-bound coverage and window count for fail-slow flags (R5), which depend on mantle's measured false-flag rate and on how many devices per node can be on probation.
14. Whether a learned per-I/O classifier (LinnOS, Heimdall) would improve mantle's tail latency beyond hedging plus R5 enough to justify training and retraining it.
15. Whether to ship a privileged telemetry helper on Linux (§9.2).
16. The field rate of lost and misdirected writes on current drives, which also bears on the chunk store's open question about the index frame (chunk-store design §3.2).
