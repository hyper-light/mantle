# 28 — Storage classes, power and device longevity

**Status:** research input for a storage-class design record and for changes to
`docs/design/durability.md`, `chunk-store.md`, `measurement.md` and `node.md`. This is not a
decision record; §7 proposes the decisions.
**Compiled:** 2026-09-30.
**Scope:** the owner's direction of 2026-09-30, which adds S3's storage classes to mantle's scope
and asks that writes be aware of the device and of power: fast but battery-conscious on an NVMe
laptop, durable and kind to the drive on datacenter disks, archive routed to slower but more
durable storage, express to faster and more power-hungry storage. The note covers:

- S3's storage classes exactly as S3 defines them: their API surface, restore, lifecycle and
  Intelligent-Tiering rules, and the durability and availability AWS states for each (§2);
- how a class maps onto mantle's placement, encoding, write plan, read plan and migration, and
  how that collapses on one laptop with one NVMe (§3);
- power- and energy-aware writing: what each OS reports, NVMe and disk power states, the
  measured energy literature, and a policy that keeps acknowledgement exact (§4);
- device longevity: flash endurance, disk workload and cycle ratings, and how mantle budgets
  against them (§5);
- the design organized by step of scale (§6), the proposed decisions (§7), the test and
  benchmark plan (§8) and what remains unknown (§9).

Two notes being written beside this one are referenced rather than repeated: note 26 (the
concurrency model) and note 27 (upload scheduling). Where this note says a background move or
a restore runs "in its own admission class", the class's mechanism (queues, fairness,
credits) is theirs; this note states only the inputs that class needs: its deadline, its
budget and when it may be deferred.

---

## 0. How to read this note

**Citation tags.** Papers: `[KEY §section, p. N]` with the printed page, or "PDF p. N" where
the copy has none. AWS pages: `[S3UG "page"]` (User Guide) and `[S3API Operation]` (API
Reference). Specifications by section and figure; source code by path at the commit named;
datasheets by product. Earlier notes: "note 10 §x" and so on.

**Quotes** are verbatim from the fetched text, with line wraps joined by one space and
ligatures normalized.

**Evidence labels.**
- *(no label)*: a peer-reviewed paper, checked against its text.
- **primary**: AWS documentation, an OS vendor's documentation or SDK header, a specification,
  or kernel source and documentation, read directly. These say what a system does; they are
  not peer-reviewed.
- **DATASHEET**: a device vendor's data sheet or knowledge-base page. A vendor's statement of
  its own product's ratings; not peer-reviewed.
- **[obs]**: observed on the research machine (macOS 26.4.1, Apple Silicon, internal APPLE SSD
  AP8192Z, as in note 10). It shows that machine and nothing more.
- **DERIVED**: arithmetic or reasoning from stated facts, done in this note.
- **INFERENCE / Recommendation**: design reasoning for mantle.
- **UNVERIFIED**: not confirmed in a primary source.

**Method.** AWS pages were fetched on 2026-09-30 (UTC) as HTML from `docs.aws.amazon.com` and
reduced to text; every AWS quote below was checked against that text. The NVMe Base
Specification 2.4 was converted with `pdftotext -layout` and quoted from it. Linux sources are
from `torvalds/linux` master at commit `551c722f4080` (2026-09-30). macOS headers are from the
macOS 26.4 SDK in the Command Line Tools. Papers and datasheets were read from the PDFs at the
URLs in Sources. Nothing was built or run except the read-only probes marked [obs].

---

## Sources

### AWS (primary; fetched 2026-09-30)

| Key | Page | URL |
|---|---|---|
| S3UG-SC | "Understanding and managing Amazon S3 storage classes" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html |
| S3UG-SET | "Setting the storage class of an object" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/sc-howtoset.html |
| S3UG-GLC | "Understanding S3 Glacier storage classes for long-term data storage" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/glacier-storage-classes.html |
| S3UG-LCT | "Transitioning objects using Amazon S3 Lifecycle" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html |
| S3UG-ARC | "Working with archived objects" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/archived-objects.html |
| S3UG-RET | "Understanding archive retrieval options" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects-retrieval-options.html |
| S3UG-ITW | "How S3 Intelligent-Tiering works" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-overview.html |
| S3UG-ITM | "Managing S3 Intelligent-Tiering" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/intelligent-tiering-managing.html |
| S3UG-X1Z | "High performance workloads" (S3 Express One Zone) | https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-high-performance.html |
| S3UG-DIR | "Differences for directory buckets" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html |
| S3UG-DUR | "Data protection in Amazon S3" | https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html |
| S3OUT | "What is Amazon S3 on Outposts?" | https://docs.aws.amazon.com/AmazonS3/latest/s3-outposts/S3onOutposts.html |
| S3API | API Reference: `PutObject`, `CopyObject`, `CreateMultipartUpload`, `GetObject`, `HeadObject`, `RestoreObject`, `CreateSession`, `PutBucketIntelligentTieringConfiguration`, and the types `IntelligentTieringConfiguration`, `Tiering`, `RestoreStatus` | https://docs.aws.amazon.com/AmazonS3/latest/API/API_<Name>.html |

### Peer-reviewed

| Key | Citation | URL |
|---|---|---|
| PEL14 | S. Balakrishnan, R. Black, A. Donnelly, P. England, A. Glass, D. Harper, S. Legtchenko, A. Ogus, E. Peterson, A. Rowstron. "Pelican: A Building Block for Exascale Cold Data Storage." *OSDI '14*, pp. 351–365. | https://www.usenix.org/conference/osdi14/technical-sessions/presentation/balakrishnan |
| PERG08 | M. W. Storer, K. M. Greenan, E. L. Miller, K. Voruganti. "Pergamum: Replacing Tape with Energy Efficient, Reliable, Disk-Based Archival Storage." *FAST '08*. | https://www.usenix.org/legacy/event/fast08/tech/full_papers/storer/storer.pdf |
| HIB05 | Q. Zhu, Z. Chen, L. Tan, Y. Zhou, K. Keeton, J. Wilkes. "Hibernator: Helping Disk Arrays Sleep through the Winter." *SOSP '05*, pp. 177–190. Read from the author's copy. | https://www.cs.purdue.edu/homes/lintan/publications/hibernator_sosp05.pdf |
| HA20 | B. Harris, N. Altiparmak. "Ultra-Low Latency SSDs' Impact on Overall Energy Efficiency." *HotStorage '20* (peer-reviewed workshop). | https://www.usenix.org/system/files/hotstorage20_paper_harris.pdf |
| MOH17 | J. Mohan, D. Purohith, M. Halpern, V. Chidambaram, V. J. Reddi. "Storage on Your Smartphone Uses More Energy Than You Think." *HotStorage '17* (peer-reviewed workshop). | https://www.usenix.org/conference/hotstorage17/program/presentation/mohan |
| LOL14 | J. Yang, N. Plasson, G. Gillis, N. Talagala, S. Sundararaman. "Don't stack your Log on my Log." *INFLOW '14* (peer-reviewed workshop). | https://www.usenix.org/system/files/conference/inflow14/inflow14-yang.pdf |
| SS21 | J. Bornholt et al. "Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3." *SOSP '21*. As read in note 09 §2. | https://doi.org/10.1145/3477132.3483540 |

Also used through earlier notes: Desnoyers (SYSTOR '12, note 10 §5.1), MMES20/MMES22/SLM16/XZQ+19
(note 10 §3), KMS+20 PACEMAKER (note 10 §7.5), Ford et al. (note 04 §A5, note 15 §1), the
group-commit literature (note 11 §2), SepBIT and LFS (chunk-store.md §8), Skylight and ext4-lazy
(note 03 §12).

### Specifications, OS sources and documentation (primary)

| Key | Source | Where |
|---|---|---|
| NVMe24 | NVM Express Base Specification, Revision 2.4 (ratified 2026-07-31): §5.2.30.1.7 (Autonomous Power State Transition feature, Figs. 475–478), Fig. 340 (Power State Descriptor), §8.1.19 (Power Management, pp. 666–668), Figs. 213 and 225 (SMART and Endurance Group logs, as in note 10 §6) | https://nvmexpress.org/wp-content/uploads/NVM-Express-Base-Specification-Revision-2.4-Ratified-2026.07.31.pdf |
| LNX | Linux `drivers/nvme/host/core.c` (APST parameters and `nvme_configure_apst`), `drivers/powercap/powercap_sys.c`, `Documentation/ABI/testing/sysfs-class-power`, `Documentation/power/power_supply_class.rst`, `Documentation/power/powercap/powercap.rst`, `Documentation/driver-api/thermal/sysfs-api.rst`, `Documentation/userspace-api/sysfs-platform_profile.rst`, `Documentation/ABI/testing/sysfs-devices-power`; commit `551c722f4080` | https://github.com/torvalds/linux |
| IOCTL-PRIO | `ioprio_set(2)`, Linux man-pages | https://man7.org/linux/man-pages/man2/ioprio_set.2.html |
| APPLE | macOS 26.4 SDK headers: `IOKit/ps/IOPowerSources.h`, `IOKit/ps/IOPSKeys.h`, `IOKit/pwr_mgt/IOPMLib.h`, `Foundation/NSProcessInfo.h`, `libkern/OSThermalNotification.h`; man pages `getiopolicy_np(3)` and `powermetrics(1)` | local SDK and `man` |
| MSL | Microsoft Learn: `SYSTEM_POWER_STATUS`; `RegisterPowerSettingNotification`; "Power Setting GUIDs"; `PowerRegisterForEffectivePowerModeNotifications`; "Quality of Service"; `FILE_IO_PRIORITY_HINT_INFO`; `BATTERY_STATUS`; "NVMe power management" (StorNVMe) | https://learn.microsoft.com/en-us/windows/win32/power/ and https://learn.microsoft.com/en-us/windows-hardware/design/component-guidelines/power-management-for-storage-hardware-devices-nvme |

### Device vendors (DATASHEET)

| Key | Source | URL |
|---|---|---|
| WD-HC580 | Western Digital, *Ultrastar DC HC580* data sheet, © 2024 | https://documents.westerndigital.com/content/dam/doc-library/en_us/assets/public/western-digital/product/data-center-drives/ultrastar-dc-hc500-series/data-sheet-ultrastar-dc-hc580.pdf |
| SG-BC | Seagate, *BarraCuda 3.5 HDD* data sheet DS1900-14-2007GB | https://www.seagate.com/www-content/datasheets/pdfs/3-5-barracudaDS1900-14-2007GB-en_GB.pdf |
| SG-AWR | Seagate knowledge base, "Annualized Workload Rate" (005902en) | https://www.seagate.com/ca/en/support/kb/annualized-workload-rate-005902en/ |

**Named but not read:** JEDEC JESD218 (SSD endurance; also unread in note 10), the T13 ATA
Command Set's Extended Power Conditions and Advanced Power Management, Micron's 7450 product
brief (its endurance figures below come from reseller listings and are UNVERIFIED), and any
AWS statement of the media behind Glacier (none is published).

---

## 1. Findings that decide design

1. **AWS designs every class except Reduced Redundancy for the same durability.** The
   comparison table gives 99.999999999% for STANDARD, STANDARD_IA, INTELLIGENT_TIERING,
   ONEZONE_IA, EXPRESS_ONEZONE, GLACIER_IR, GLACIER and DEEP_ARCHIVE, and 99.99% for
   REDUCED_REDUNDANCY [S3UG-SC]. The Glacier page says the three Glacier classes "offer the
   same durability and resiliency as the S3 Standard storage class, but at lower storage costs"
   [S3UG-GLC]. Archive is colder, not more durable. Classes differ in availability, in the
   number of zones, in access latency and in minimum billing terms. (§2.2)
2. **The class is visible in the API in exact, testable ways.** It is set by
   `x-amz-storage-class` on PutObject, CopyObject, CreateMultipartUpload and POST; it is
   returned by HEAD and GET "for all objects except for S3 Standard storage class objects";
   GET of an object in GLACIER, DEEP_ARCHIVE or an Intelligent-Tiering archive tier answers
   403 `InvalidObjectState` until RestoreObject has made a temporary copy; the copy's progress
   and expiry appear in `x-amz-restore` [S3API GetObject, HeadObject, RestoreObject]. mantle
   today reports every object as STANDARD (`crates/s3/src/response.rs`, `STORAGE_CLASS`),
   refuses lifecycle transitions as `NotImplemented` (`crates/s3/src/lifecycle.rs`), and no
   code reads the header on a PUT. (§2.3–§2.10)
3. **Colder media detect loss later, and the durability model already says what that costs.**
   durability.md's chain repairs "at one rate covering detection and rebuild" (§2 there), and
   Ford found repair rates past a single failure "dominated by detection and trigger time"
   (note 04 §R3). An archive placement that scrubs less often or keeps disks spun down has a
   slower repair rate, so to meet the same 10⁻¹¹ it needs more parity, not less. Pelican chose
   15+3 for its spun-down racks [PEL14 §2.2.2, p. 354]. (§3.3)
4. **On a full 24 TB data-center disk, mantle's default scrub alone exceeds the drive's
   workload rating.** Seagate counts reads and writes toward the workload rate,
   "(Lifetime Writes + Lifetime Reads) * (8760 / Lifetime Power On Hours)" [SG-AWR]. The
   HC580 is rated for 550 TB a year, with "Derating of MTBF and AFR … above these parameters,
   up to 550TB/year" [WD-HC580]. Reading 24 TB every 7 days is 1,251 TB a year, 2.3 times the
   rating; every 14 days is 626 TB a year (DERIVED). On an 8 TB desktop disk rated 55 TB a
   year [SG-BC], a 7-day scrub is 7.6 times the rating. The scrub period on disks must be
   derived from the workload budget, not from practice. Pelican deferred scrubbing for this
   reason [PEL14 §4.7, p. 362]. (§5.2)
5. **Larger, fewer I/Os cost less energy per byte, on every device measured.** "for any given
   IO depth and storage device, larger requests consistently provide better overall energy
   efficiency measured as bytes per joule" [HA20 §3.8, Obs. 6]. On a phone's flash, 4 KiB
   writes each followed by `fsync` cost 12–19 times the energy per KB of 512 KiB sequential
   writes [MOH17 §3.1, Fig. 1]. No peer-reviewed measurement of energy per durable flush on a
   current NVMe drive was found; mantle must measure it. (§4.4)
6. **The OS already moves an idle NVMe drive into deep power states within about 100 ms.**
   Linux's defaults put a drive into a non-operational state whose entry plus exit latency is
   at most 15 ms after 100 ms idle, and into one up to 100 ms after 2 s [LNX `core.c`];
   Windows' Balanced scheme on battery uses 100 ms and 50 ms [MSL StorNVMe]. A writer that
   flushes single small requests more than 100 ms apart wakes the drive for each one (DERIVED).
   mantle's lever is the shape of its I/O, not the drive's power state. (§4.2)
7. **Each OS reports power source, saver mode and thermal state to an unprivileged process,
   through different interfaces.** macOS: `IOPSGetTimeRemainingEstimate`,
   `kIOPSNotifyPowerSource`, `NSProcessInfo.thermalState` and `lowPowerModeEnabled`. Linux:
   `/sys/class/power_supply/*/{type,online,status,power_now}` and thermal zones. Windows:
   `GetSystemPowerStatus`, `GUID_ACDC_POWER_SOURCE`, `GUID_POWER_SAVING_STATUS`,
   `GUID_ENERGY_SAVER_STATUS`. Apple's header states the response to each thermal state, which
   gives mantle cited rather than chosen thresholds. (§4.1)
8. **Device ratings are partly device-reported and partly not.** NVMe reports its own endurance
   (Endurance Estimate, Percentage Used; note 10 §6). Disks report none of their workload,
   power-on-hours or load/unload ratings; those come only from datasheets, which differ by a
   factor of ten between desktop and data-center models [SG-BC; WD-HC580]. (§5)
9. **A class is also a lifetime hint.** GLACIER_IR and GLACIER carry a 90-day minimum, DEEP_ARCHIVE
   180, the IA classes 30 [S3UG-SC]. The chunk store groups data by expected death time
   (chunk-store.md §8, SepBIT); a client that declares an archive class has told mantle its data
   will live months, which places it with old data from the first write. (§3.4)

---

## 2. S3's storage classes as S3 defines them

### 2.1 The values

The API's enumeration for `x-amz-storage-class` is "STANDARD | REDUCED_REDUNDANCY |
STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | OUTPOSTS | GLACIER_IR |
SNOW | EXPRESS_ONEZONE | FSX_OPENZFS | FSX_ONTAP | AWS_BACKUP_WARM | AWS_BACKUP_LOW_COST_WARM"
[S3API PutObject] (**primary**). The User Guide's console-to-API list for general purpose
buckets names nine: REDUCED_REDUNDANCY, EXPRESS_ONEZONE, DEEP_ARCHIVE, GLACIER, GLACIER_IR,
INTELLIGENT_TIERING, ONEZONE_IA, STANDARD, STANDARD_IA [S3UG-SET]. SNOW, the FSx values and the
AWS Backup values name storage outside S3 buckets. "If you don't specify the storage class when
you upload an object, Amazon S3 assigns the S3 Standard storage class" [S3UG-SC].

OUTPOSTS "is available only for objects stored in buckets on Outposts. If you try to use this
storage class with an S3 bucket in an AWS Region, an InvalidStorageClass error occurs. In
addition, if you try to use other S3 storage classes with objects stored in S3 on Outposts
buckets, the same error occurs" [S3UG-SC]. Directory buckets "only support EXPRESS_ONEZONE … in
Availability Zones and ONEZONE_IA … in Dedicated Local Zones" and "Unsupported storage class
values won't write a destination object and will respond with the HTTP status code 400 Bad
Request" [S3API GetObject, CopyObject]. For an unknown class on a browser POST, the S3 error
table gives 400 `InvalidStorageClass` (note 19 §2), and LocalStack's recordings of S3, a secondary
source, show "The storage class you specified is not valid" with `StorageClassRequested` (note 19
§8.1). What S3 answers for SNOW, the FSx and Backup values, or
EXPRESS_ONEZONE, sent to a general purpose bucket is UNVERIFIED; it must be recorded against
S3 as notes 13 and 19 recorded other answers.

### 2.2 Durability, availability and zones, as AWS states them

From the comparison table [S3UG-SC] (**primary**):

| Class | Designed for | Durability | Availability | AZs | Min duration | Min billable size |
|---|---|---|---|---|---|---|
| STANDARD | "Frequently accessed data (more than once a month) with millisecond access" | 99.999999999% | 99.99% | ≥ 3 | None | None |
| STANDARD_IA | "Long-lived, infrequently accessed data (once a month) with millisecond access" | 99.999999999% | 99.9% | ≥ 3 | 30 days | 128 KB |
| INTELLIGENT_TIERING | "Data with unknown, changing, or unpredictable access patterns" | 99.999999999% | 99.9% | ≥ 3 | None | None |
| ONEZONE_IA | "Recreatable, infrequently accessed data (once a month) with millisecond access" | 99.999999999% | 99.5% | 1 | 30 days | 128 KB |
| EXPRESS_ONEZONE | "Single-digit millisecond data access for latency-sensitive applications within a single AWS Availability Zone" | 99.999999999% | 99.95% | 1 | None | None |
| GLACIER_IR | "Long-lived, archive data accessed once a quarter with millisecond access" | 99.999999999% | 99.9% | ≥ 3 | 90 days | 128 KB |
| GLACIER | "Long-lived archive data accessed once a year with retrieval times of minutes to hours" | 99.999999999% | "99.99% (after you restore objects)" | ≥ 3 | 90 days | NA, plus 40 KB of metadata per object |
| DEEP_ARCHIVE | "Long-lived archive data accessed less than once a year with retrieval times of hours" | 99.999999999% | "99.99% (after you restore objects)" | ≥ 3 | 180 days | NA, plus 40 KB of metadata per object |
| REDUCED_REDUNDANCY | "Noncritical, frequently accessed data with millisecond access"; "Not recommended" | 99.99% | 99.99% | ≥ 3 | None | None |

What the surrounding text adds:

- **Same durability, different exposure.** "all of the storage classes except for S3 One
  Zone-IA (ONEZONE_IA) and S3 Express One Zone (EXPRESS_ONEZONE) are designed to be resilient to
  the physical loss of an Availability Zone resulting from disasters" [S3UG-SC]. ONEZONE_IA "is
  as durable as S3 Standard-IA, but it is less available and less resilient" and "the data is
  not resilient to the physical loss of the Availability Zone resulting from disasters, such as
  earthquakes and floods" [S3UG-SC]. *DERIVED:* AWS's eleven nines for the one-zone classes
  leave out the destruction of the zone; a statement of durability that includes it could not
  be the same number for one zone as for three.
- **Glacier.** "Each of these storage classes offer the same durability and resiliency as the
  S3 Standard storage class, but at lower storage costs" [S3UG-GLC]. Six classes "redundantly
  store objects on multiple devices across a minimum of three Availability Zones" and "are all
  designed to sustain data in the event of the loss of an entire Amazon S3 Availability Zone"
  [S3UG-DUR]. AWS publishes nothing about the media under Glacier.
- **Express One Zone.** "your data is redundantly stored on multiple devices within a single
  Availability Zone. S3 Express One Zone is designed to handle concurrent device failures by
  quickly detecting and repairing any lost redundancy" [S3UG-X1Z].
- **Reduced Redundancy.** "We recommend not using this storage class. The S3 Standard storage
  class is more cost-effective." "For durability, RRS objects have an average annual expected
  loss of 0.01 percent of objects. If an RRS object is lost, when requests are made to that
  object, Amazon S3 returns a 405 error" [S3UG-SC].
- **Outposts.** OUTPOSTS "is designed to store data durably and redundantly across multiple
  devices and servers on your Outposts" [S3OUT]; no durability figure is given.
- **Intelligent-Tiering** "is designed for 99.9% availability and 99.999999999% durability"
  [S3UG-ITW].

### 2.3 Where the class appears in the API

- **Set.** "You can specify the storage class on an object when you create it using the
  PutObject, POST Object Object, and CreateMultipartUpload API operations, add the
  x-amz-storage-class request header. If you don't add this header, Amazon S3 uses the default
  S3 Standard (STANDARD) storage class" [S3UG-SET]. For CopyObject, "If the x-amz-storage-class
  header is not used, the copied object will be stored in the STANDARD Storage Class by
  default", and CopyObject is how a stored object's class is changed [S3API CopyObject]: the
  default is not the source's class. "You can't change the storage class of objects stored in
  directory buckets" [S3UG-SET].
- **Copy of an archived source.** "Before using an object as a source object for the copy
  operation, you must restore a copy of it if it meets any of the following conditions: The
  storage class of the source object is GLACIER or DEEP_ARCHIVE. The storage class of the source
  object is INTELLIGENT_TIERING and it's S3 Intelligent-Tiering access tier is Archive Access or
  Deep Archive Access" [S3API CopyObject].
- **Returned.** HEAD and GET return `x-amz-storage-class`: "Amazon S3 returns this header for all
  objects except for S3 Standard storage class objects" [S3API HeadObject, GetObject]. ListObjects
  (v1, v2) and ListObjectVersions return `StorageClass` per entry, and with
  `x-amz-optional-object-attributes: RestoreStatus` a `RestoreStatus` element (note 05, ListObjectsV2
  and ListObjectVersions). ListParts carries a `StorageClass` (note 05 §4.6), and mantle's ListParts and ListMultipartUploads responses already emit one (`response.rs`).
- **Archive status.** HEAD returns `x-amz-archive-status` with "Valid Values: ARCHIVE_ACCESS |
  DEEP_ARCHIVE_ACCESS" for an Intelligent-Tiering object in an archive tier [S3API HeadObject].
- **Archived read.** GetObject's error list: "InvalidObjectState: Object is archived and
  inaccessible until restored", HTTP 403, for GLACIER, DEEP_ARCHIVE and the two Intelligent-Tiering
  archive tiers; the documented response body is `<Code>InvalidObjectState</Code><Message>The
  action is not valid for the object's storage class</Message>`. For the Intelligent-Tiering
  case "the response will include StorageClass and AccessTier elements. Access tier valid values
  are ARCHIVE_ACCESS and DEEP_ARCHIVE_ACCESS" [S3API GetObject].
- **Policies.** `s3:x-amz-storage-class` is a condition key; mantle's policy catalog already has
  it (`crates/s3/src/policy/catalog.rs`).
- **Replication.** "you can't replicate objects that are stored in the S3 Glacier Flexible
  Retrieval or S3 Glacier Deep Archive storage classes" [S3UG-SET]; lifecycle transitions skip
  objects "with a Pending or Failed replication status" [S3UG-LCT].

### 2.4 Archived objects and RestoreObject

Archived means not readable in real time: GLACIER, DEEP_ARCHIVE, and the Intelligent-Tiering
Archive Access and Deep Archive Access tiers. "Objects stored in the S3 Glacier Instant
Retrieval storage class are not archived" [S3UG-ARC].

**The request.** `POST /{Key}?restore` with a `RestoreRequest` body carrying `Days` and
`GlacierJobParameters/Tier` [S3API RestoreObject]. "The Days element is required for regular
restores", and for Intelligent-Tiering "restore requests … don't accept the Days value"
[S3API RestoreObject; S3UG-ARC]. The body also defines `Type`, `Description`,
`SelectParameters` and `OutputLocation` for the select-restore variant; mantle need implement
only the regular restore (`Type` absent), and must answer the select variant as S3 now does
(UNVERIFIED; record it).

**What happens.** For GLACIER and DEEP_ARCHIVE, "you must initiate the restore request and wait
until a temporary copy of the object is available. When a temporary copy of the restored object
is created, the object's storage class remains the same" [S3UG-ARC]. For Intelligent-Tiering,
"wait until the object is moved into the Frequent Access tier" [S3UG-ARC]. "Amazon S3 processes
only one restore request at a time per object" [S3UG-LCT].

**Responses.** "If the object is not previously restored, then Amazon S3 returns 202 Accepted in
the response. If the object is previously restored, Amazon S3 returns 200 OK", which "updates
only the restored copy's expiry time". Errors: `RestoreAlreadyInProgress`, 409; and
`GlacierExpeditedRetrievalNotAvailable`, 503, "Returned if there is insufficient capacity to
process the Expedited request"; and `ObjectAlreadyInActiveTierError`, 403, "This action is not
allowed against this storage tier" [S3API RestoreObject]. Which code S3 returns for a restore of
an object that was never archived (STANDARD, GLACIER_IR) is UNVERIFIED here.

**Status.** "If an archive copy is already restored, the header value indicates when Amazon S3 is
scheduled to delete the object copy. For example: x-amz-restore: ongoing-request="false",
expiry-date="Fri, 21 Dec 2012 00:00:00 GMT"", and while in progress `ongoing-request="true"`
[S3API HeadObject]. The Intelligent-Tiering example also shows `x-amz-restore-request-date`
[S3UG-ITM]. In listings, `RestoreStatus` carries `IsRestoreInProgress` and `RestoreExpiryDate`
[S3API RestoreStatus].

**Expiry.** "Amazon S3 calculates the expiration time of the restored object copy by adding the
number of days specified in the restoration request to the time when the requested restoration
is completed. Amazon S3 then rounds the resulting time to the next day at midnight Universal
Coordinated Time (UTC)" [S3UG-ARC]. A repeated request moves the expiry "relative to the current
time", but "You cannot update the restoration period when Amazon S3 is actively processing your
current restore request". A lifecycle expiration wins: "if you restore an object copy for 10
days, but the object is scheduled to expire in 3 days, Amazon S3 deletes the object in 3 days"
[S3API RestoreObject]. An in-progress restore can be moved to a faster tier ("restore speed
upgrade") [S3API RestoreObject].

**Tiers and their documented times** [S3UG-RET] (**primary**; "typically", not guarantees):

| Storage class or tier | Expedited | Standard (Batch Operations) | Standard | Bulk |
|---|---|---|---|---|
| GLACIER, or Intelligent-Tiering Archive Access | 1–5 minutes | Minutes–5 hours | 3–5 hours | 5–12 hours |
| DEEP_ARCHIVE, or Intelligent-Tiering Deep Archive Access | Not available | 9–12 hours | Within 12 hours | Within 48 hours |

Further: Expedited objects "under 250 megabytes in size are typically made available within 1–5
minutes, and objects 250 megabytes or larger in size are typically retrieved with up to 300
megabytes per second"; "S3 Glacier supports restore requests at a rate of 1,000 transactions per
second. If this rate is exceeded otherwise valid requests are throttled or rejected and Amazon
S3 returns a ThrottlingException error"; throughput is "up to 1–2 petabytes per day per customer
account"; "Without provisioned capacity, Expedited retrievals might not be accepted during
periods of high demand" [S3UG-RET].

### 2.5 Minimum durations and billable sizes

These are billing terms: deleting, overwriting or transitioning an object before its class's
minimum "incur[s] the normal storage usage charge plus a pro-rated charge for the remainder"
[S3UG-SC]. They change no API answer, so mantle need not enforce them to be compatible. They
matter in three places:

- **Lifecycle validation** rests on them: a transition to STANDARD_IA or ONEZONE_IA needs at least
  30 days (implemented in `lifecycle.rs` as `InfrequentAccessDays`), and "You can't create a single
  Lifecycle rule that transitions objects from one storage class to another before the minimum
  storage duration period has passed. For example, … the S3 Glacier Deep Archive transition must
  occur after at least 94 days" [S3UG-LCT]. Whether mantle's validator refuses that example as S3
  does is to be checked against S3's recorded answer (note 13 §6.9).
- **Accounting.** An operator who charges for mantle storage the way AWS does needs the per-object
  facts: class, the time it entered the class, size, and for GLACIER and DEEP_ARCHIVE the stated
  8 KB at the STANDARD rate and 32 KB at the archive rate [S3UG-LCT]. mantle can expose these as
  metrics without enforcing anything.
- **Lifetime hints** for placement (§3.4).

### 2.6 Lifecycle transitions

The supported transitions form a "waterfall" [S3UG-LCT] (**primary**):

- STANDARD → STANDARD_IA, INTELLIGENT_TIERING, ONEZONE_IA, GLACIER_IR, GLACIER, DEEP_ARCHIVE.
- STANDARD_IA → INTELLIGENT_TIERING, ONEZONE_IA, GLACIER_IR, GLACIER, DEEP_ARCHIVE.
- INTELLIGENT_TIERING, by access tier: Frequent or Infrequent → ONEZONE_IA, GLACIER_IR, GLACIER,
  DEEP_ARCHIVE; Archive Instant Access → GLACIER_IR, GLACIER, DEEP_ARCHIVE; Archive Access →
  GLACIER, DEEP_ARCHIVE; Deep Archive Access → DEEP_ARCHIVE.
- ONEZONE_IA → GLACIER, DEEP_ARCHIVE. GLACIER_IR → GLACIER, DEEP_ARCHIVE. GLACIER → DEEP_ARCHIVE.
- "The transition of objects to the S3 Glacier Deep Archive storage class can go only one way";
  leaving GLACIER or DEEP_ARCHIVE for another class is a restore followed by a CopyObject.

Constraints:

- "Starting September 2024, the default behavior prevents objects smaller than 128 KB from being
  transitioned to any storage class"; `x-amz-transition-default-minimum-object-size` restores the
  earlier default (`lifecycle.rs` already parses and returns it as `MinimumSize`).
- "S3 Lifecycle transitions objects to S3 Glacier Flexible Retrieval and S3 Glacier Deep Archive
  asynchronously. There might be a delay between the transition date in the S3 Lifecycle
  configuration rule and the date of the physical transition. You are charged at the destination
  storage class rate … starting from the date the lifecycle rule is satisfied, even if the
  physical transition has not yet occurred. … The only exception is transitions to S3
  Intelligent-Tiering, where billing changes occur after the physical transition completes."
  What HEAD reports between the rule date and the physical transition is not stated (UNVERIFIED).
- Tag filters: "S3 Lifecycle evaluates objects against tag-based filters daily", queues the action
  "for asynchronous processing", and "At execution time, Amazon S3 re-evaluates the object's
  current tags".
- "Encrypted objects remain encrypted throughout the storage class transition process."

### 2.7 Intelligent-Tiering

One class, five access tiers; "When objects move between access tiers, the storage class remains
the same (S3 Intelligent-Tiering)" [S3UG-ITW] (**primary**):

| Tier | Entered | Access |
|---|---|---|
| Frequent Access | on upload or transition; on any access from Infrequent or Archive Instant | millisecond |
| Infrequent Access | "not accessed for 30 consecutive days" | millisecond |
| Archive Instant Access | "not accessed for 90 consecutive days" | millisecond |
| Archive Access (optional) | at least 90 days without access, configurable to "a maximum of 730 days"; "same performance as the S3 Glacier Flexible Retrieval storage class" | restore |
| Deep Archive Access (optional) | at least 180 days, up to 730; "same performance as the S3 Glacier Deep Archive storage class" | restore |

- **What counts as access.** Tiering an object up, or resetting its timer: "Downloading or copying
  an object through the Amazon S3 console", "Invoking CopyObject, UploadPartCopy, or replicating
  objects with Batch Replication. In these cases, the source objects of the copy or replication
  operations are tiered up", and "Invoking GetObject, PutObject, RestoreObject, or
  CompleteMultipartUpload". Not access: "HeadObject, GetObjectTagging, PutObjectTagging,
  ListObjects, ListObjectsV2, ListObjectVersions, and UpdateObjectEncryption" (a sample, "not a
  definitive list"). `SelectObjectContent` resets the archive timers but "doesn't constitute access
  that tiers objects up" [S3UG-ITW].
- **Small objects.** "If the size of an object is less than 128 KB, it is not monitored and is
  not eligible for automatic tiering. Smaller objects are always stored in the Frequent Access
  tier" [S3UG-ITW].
- **Configuration.** `PUT /?intelligent-tiering&id=Id` with `IntelligentTieringConfiguration`:
  `Id`, `Status` (`Enabled | Disabled`), an optional `Filter` (prefix, tag, or `And`), and one or
  more `Tiering` elements each with `AccessTier` (`ARCHIVE_ACCESS | DEEP_ARCHIVE_ACCESS`) and `Days`,
  where "The minimum number of days specified for Archive Access tier must be at least 90 days and
  Deep Archive Access tier must be at least 180 days. The maximum can be up to 2 years (730 days)".
  "You can have up to 1,000 S3 Intelligent-Tiering configurations per bucket"; beyond it,
  `TooManyConfigurations`, 400 [S3API PutBucketIntelligentTieringConfiguration, Tiering]. A
  configuration is needed only for the archive tiers.
- **Restore** from an archive tier returns the object to Frequent Access; the timers then run
  again from zero [S3UG-ITM].
- **Notification.** An `s3:IntelligentTiering` event carries `destinationAccessTier` [S3UG-ITM].

### 2.8 Express One Zone and directory buckets

EXPRESS_ONEZONE lives only in directory buckets, a separate bucket type with its own API
differences [S3UG-DIR; S3UG-X1Z] (**primary**):

- **Names and endpoints.** A directory bucket's name is a base name, the zone ID and the suffix
  `--x-s3`; bucket operations go to a Regional endpoint and object operations to a Zonal endpoint;
  "Path-style requests are not supported" [S3UG-DIR; S3API CreateSession].
- **CreateSession.** Object requests authenticate with temporary credentials from
  `CreateSession`, which "are scoped to the bucket and expire after 5 minutes" and "cannot be
  extended or refreshed beyond the original specified interval"; the token travels in
  `x-amz-s3session-token`; `x-amz-create-session-mode` asks for `ReadWrite` or `ReadOnly`, and the
  `s3express:SessionMode` condition key controls it. CopyObject and HeadBucket do not use session
  credentials [S3API CreateSession].
- **Behavior.** "ListObjectsV2 does not return objects in lexicographical (alphabetical) order",
  only "/" is accepted as a delimiter, deletes remove empty parent "directories", ETags "are random
  alphanumeric strings unique to the object and not MD5 checksums", multipart part numbers must be
  consecutive, and the class cannot be changed [S3UG-DIR; S3UG-SET]. Lifecycle transitions,
  RestoreObject and Intelligent-Tiering configuration are not supported on directory buckets
  [S3UG-DIR; S3API RestoreObject; S3API PutBucketIntelligentTieringConfiguration].
- **What it promises.** "consistent, single-digit millisecond data access", 99.95% availability in
  one zone, and the option "to co-locate your object storage with your compute resources"
  [S3UG-X1Z].

A directory bucket is a second bucket type, not only a class. Its full API delta is a separate
compatibility item; the session mechanism's bounds (how many live sessions per principal and
bucket, and the 5-minute expiry as the eviction rule) belong with the admission design of
notes 26 and 27.

### 2.9 What mantle does today, and what full compatibility needs

**Today.** Every listing and response reports STANDARD; HEAD and GET send no
`x-amz-storage-class`, which is correct for STANDARD; a lifecycle configuration with a
`Transition` is validated as S3 validates it and then refused `NotImplemented` (501); the
`intelligent-tiering` and `restore` subresources are answered `501 NotImplemented` (`route.rs`,
`UNSUPPORTED`), though their actions are in the policy catalog; nothing reads
`x-amz-storage-class` on PUT, POST, copy or CreateMultipartUpload.

**Full compatibility needs (INFERENCE, each from §2.1–§2.8):**

1. Accept, validate and store the class per object version on PutObject, POST, CopyObject
   (default STANDARD, not the source's class) and CreateMultipartUpload (the upload's class is
   the object's); refuse values not valid for the bucket type with S3's recorded error.
2. Return it on HEAD and GET (except STANDARD), in every listing, in ListParts and
   ListMultipartUploads.
3. GET, and CopyObject or UploadPartCopy from a source, of an archived object without a live
   restored copy: 403 `InvalidObjectState`, with the Intelligent-Tiering elements when it applies.
4. RestoreObject with its 202/200 semantics, its three errors, one restore at a time, expiry
   rounded to the next 00:00 UTC, re-restore extending the expiry, lifecycle expiration
   overriding it, restore speed upgrade, and `x-amz-restore`, `x-amz-restore-request-date` and
   `RestoreStatus`.
5. Lifecycle transitions along the waterfall, with the 128 KB default and its header, and the
   tag re-evaluation at execution.
6. Intelligent-Tiering: the four configuration operations, per-object access tracking by the
   listed operations, the tier thresholds, `x-amz-archive-status`, restore to Frequent Access.
7. REDUCED_REDUNDANCY accepted and stored; a lost RRS object answers 405 [S3UG-SC].
8. Directory buckets with CreateSession, if mantle offers EXPRESS_ONEZONE (§2.8).
9. Event notifications for restore and Intelligent-Tiering, when mantle has notifications.

### 2.10 Where mantle can exceed S3 without breaking compatibility

None of these changes an answer a client can observe beyond what S3 permits:

- **Stricter durability for any class.** S3 states a design target, not a ceiling. A deployment
  may set a stricter per-block target for archive classes, and mantle then picks the cheapest
  scheme meeting it (durability.md §4), typically wider on the cold media where extra parity
  costs least energy per byte held.
- **Faster restores.** The documented times say "typically", so a restore that completes sooner
  is compatible. On media that is online it can complete in the time of a copy.
- **No minimum-duration penalties**, since mantle bills nothing.
- **Honest reporting** of what the placement achieved (§3.7), outside the S3 responses.

What mantle must not do is make an archived object readable by GET before a restore, even when
its bytes sit on the same device as everything else: the 403 is the contract clients and tests
depend on.

---

## 3. How a class maps onto mantle's storage

### 3.1 A class as a set of requirements

Read off §2, each class states five things mantle can act on. None is a number mantle chose.

| Class | Access | Zones (fleet) | Durability target, per year | Lifetime hint | Access hint |
|---|---|---|---|---|---|
| STANDARD | ms | ≥ 3 | 10⁻¹¹ | none | > monthly |
| STANDARD_IA | ms | ≥ 3 | 10⁻¹¹ | ≥ 30 d | monthly |
| ONEZONE_IA | ms | 1 | 10⁻¹¹ excluding zone loss | ≥ 30 d | monthly |
| INTELLIGENT_TIERING | ms, or restore in the archive tiers | ≥ 3 | 10⁻¹¹ | none | measured per object |
| EXPRESS_ONEZONE | single-digit ms, consistently | 1 | 10⁻¹¹ excluding zone loss | none | latency-critical |
| GLACIER_IR | ms | ≥ 3 | 10⁻¹¹ | ≥ 90 d | quarterly |
| GLACIER | restore: minutes to hours | ≥ 3 | 10⁻¹¹ | ≥ 90 d | yearly |
| DEEP_ARCHIVE | restore: hours | ≥ 3 | 10⁻¹¹ | ≥ 180 d | less than yearly |
| REDUCED_REDUNDANCY | ms | ≥ 3 | 10⁻⁴ of objects (AWS's design) | none | frequent |

The durability column is per object in AWS's statement; durability.md applies 10⁻¹¹ per block and
bounds an object of B blocks by B times that (durability.md §1). For RRS, "average annual expected
loss of 0.01 percent of objects" is 10⁻⁴ per object (DERIVED).

### 3.2 Media classes, from detection and measurement

mantle assumes no device class; the disk layer identifies each device from the OS and measures it
(CLAUDE.md rule 5; note 02; measurement.md). A media class is what that produces: rotational or
not, zoned or not (note 03 §12), the measured random-read service time at the operating depth,
sequential throughput, durable-write latency, and whether the device can be spun down. Eligibility
of a class for a medium is a comparison of the class's access requirement with the measurement:

- **"consistent, single-digit millisecond"** [S3UG-X1Z]: media whose measured read service time
  at the depth Express runs at stays below 10 ms at the quantile the deployment's latency
  objective names. The HC580's datasheet gives an average latency of 4.16 ms [WD-HC580] before
  seek and queueing, and S3's own storage nodes run on
  HDDs [SS21, as in note 09 §2.1]; *INFERENCE:* in practice this admits flash only, but the
  admission is by measurement, so a disk that measured within it would qualify.
- **"millisecond access"**: any online medium, disks included. S3 serves STANDARD from HDD-based
  ShardStore nodes (note 09 §2.1).
- **archived (restore)**: any medium, including disks that are spun down or powered off, as in
  Pelican, where "only 8% of the drives can be concurrently spinning" [PEL14 Abstract], or
  Pergamum, which keeps "as many as 95%, of the disks spun down" [PERG08 §1].

Note 04 §R2 already keeps copyset permutations "per storage class"; a media class is the pool those
permutations are drawn from.

### 3.3 Encoding per class

durability.md chooses, for each block, the cheapest scheme within the target, from the rates of
chunk loss, failure-domain loss, correlated bursts and repair (§2–§5 there). A class changes three
inputs:

1. **The target** (§3.1). One-zone classes are evaluated with the zone-loss rate set to zero, which
   is what AWS's eleven nines for them mean (§2.2), and their exposure to the zone is reported
   beside the result: the chain with `--zones` gives the loss probability if the zone's loss rate
   is counted, and that number is what an operator reads to decide whether ONEZONE_IA data is
   acceptable. RRS's target is 10⁻⁴ per object if the operator adopts AWS's design for it; the
   default should be STANDARD's, because AWS itself recommends against RRS and the saving is only
   in parity (INFERENCE).
2. **The failure domains** the class may use: one zone or several (§3.1), and the media pool.
3. **The repair rate, including detection.** durability.md's repair rate covers "detection and
   rebuild of one chunk". On online media, detection of a lost device is fast and of a latent
   sector error is bounded by the scrub period. On spun-down media both are bounded by how often
   the group spins up and how often it is scrubbed. *DERIVED:* if a lost chunk is found on average
   half a scrub period after it is lost, the repair rate μ is at most 1/(T_scrub/2 + T_rebuild),
   and Ford's observation that repair past a single failure is "dominated by detection and trigger
   time" (note 04 §R3) applies with more force. A colder class therefore needs more parity, or
   a cheaper detection mechanism, to meet the same target.

Two detection mechanisms cut the cost of cold detection without spinning the disk up: Pergamum
keeps per-segment algebraic signatures in NVRAM so that "inter-disk data verification [can] be
performed while the disk is powered off", and adds intra-disk redundancy so that latent errors are
repaired locally [PERG08 Abstract]. *INFERENCE:* mantle's segment and frame checksums already sit
in its index, which lives on other media than a cold data device; comparing stored checksums across
a stripe's chunks verifies the stripe's consistency at metadata cost, but cannot find a latent
sector error, which only reading the media finds. Reading is what the workload budget (§5.2) pays
for.

The scheme candidates are those `mantle-ec` tests (durability.md §4). Pelican's 15+3 [PEL14 §2.2.2]
is not among them; a cold pool whose measured rates call for a wider code needs the code added to
the tested set first.

### 3.4 Write plan per class

The write path's acknowledgement rule does not change with the class: a write is acknowledged once
it is durable on every replica the protocol requires (CLAUDE.md rule 6). The class decides where
that is and how it is batched.

- **EXPRESS_ONEZONE.** Placement on the fastest eligible pool in one zone. Copies rather than a
  code, because a copy is readable from one chunk and needs no decode on the read path; whether
  replication or a narrow code is cheaper for the target is still the durability model's answer.
  The writer runs the no-wait rule (note 11 §2.4): a lone request is flushed at once. This is
  the class the owner describes as "faster but more resource/power consumptive", and it is
  exactly that: one flush per request at light load.
- **STANDARD, INTELLIGENT_TIERING (Frequent).** Today's path: the group-commit writer, the
  durability model's scheme, copyset placement over ≥ 3 zones where the cell spans them.
- **STANDARD_IA, ONEZONE_IA, GLACIER_IR.** The same path, with the data written into the chunk
  store's stream for data that will live long. The chunk store separates the cleaner's rewrites
  into three streams by age (chunk-store.md §8, after SepBIT), because "data written once and
  deleted later … has only its age to predict when it will die". A class with a minimum duration
  is a second predictor the client supplies: an object declared GLACIER_IR is unlikely to die
  within 90 days. *INFERENCE:* writing such data directly to the oldest stream saves the cleaner
  from copying it there later; the saving is measured by the cleaning model of
  `docs/measurements/2026-09-30-chunk-cleaning.md`, run with a class-labelled trace.
- **GLACIER, DEEP_ARCHIVE (PUT directly into the class).** Archived classes may sit on media that
  is not spinning when the PUT arrives. Waiting for a spin-up inside a PUT would make the PUT's
  latency the spin-up's. The plan is Pergamum's deferred write [PERG08 Abstract] made durable:
  the PUT is acknowledged once the object is durable at the archive class's target on online
  media (a staging placement), and the move to the cold pool is a background job (§3.6). Writes
  to the cold pool are large and sequential, which is what drive-managed SMR needs ("sequential
  writes of at least 8 MiB in size are streamed", note 03 §12.3) and what host-managed zones need
  (note 03 R11.1).

### 3.5 Read plan and restore staging

- **Online classes** read as today: hedged and tied reads within the reconstruction budget (note
  04 §R4), avoiding devices flagged slow or throttled (note 10 R5, R10).
- **Archived classes** answer GET with `InvalidObjectState` unless a restored copy is live.
- **A restore** reads the object from the cold pool and writes a temporary copy to an online pool
  under the STANDARD plan; AWS bills the restored copy at the STANDARD rate [S3UG-LCT], which says
  what kind of copy it is. The object version's metadata gains a restore record: state (in
  progress or done), request date, completion time and expiry. A GET serves the restored copy.
- **Expiry and eviction.** Expiry is completion plus `Days`, rounded up to the next 00:00 UTC
  [S3UG-ARC]. At expiry the copy's blocks become unreferenced and the collector removes them
  through the same lazy, grace-period deletion every block takes (note 22 §10; chunk-store.md §8).
  The eviction rule is therefore stated by the API, not chosen: a restored copy lives exactly as
  long as the client asked. A lifecycle expiration of the object removes the copy with it.
- **Bounds.** Restored copies occupy online capacity for their requested lifetime, and a client can
  ask for many. Two bounded resources follow: the queue of restore jobs, and the online capacity
  restored copies may hold. Reaching either is a typed refusal: an Expedited request beyond its
  budget is `GlacierExpeditedRetrievalNotAvailable` (503), which S3 defines for exactly that case
  [S3API RestoreObject]; a Standard or Bulk request beyond the queue's bound is refused with the
  503 that S3 clients retry (note 25 §3), and a request rate beyond the measured capacity is
  throttled as S3 throttles restores [S3UG-RET]. The queue's bound and the capacity share are
  derived from measured restore throughput and the tiers' documented completion times (§7, D8).
- **Restore tiers as scheduling classes.** Expedited, Standard and Bulk become three admission
  classes for the cold pool with their documented times as completion objectives. Bulk work is
  batched by spin-up group, as Pelican's scheduler batches "sets of operations for the same group
  to amortize the group spin up latency over the set of operations", trading reordering against
  fairness with a bound `u` on how far a request may be overtaken [PEL14 §2.2.3, p. 356].
  *INFERENCE:* mantle takes `u` from the tier's documented completion time rather than choosing it.

### 3.6 Migration between classes

Lifecycle transitions and Intelligent-Tiering tier moves are background moves of data that is
already durable. Three rules from the evidence:

1. **Logical class and physical placement are separate fields.** The object's class (and its
   Intelligent-Tiering tier) is what the API reports and what decides GET's answer. Its placement
   is where its blocks are. S3 transitions "asynchronously" and bills from the rule date "even if
   the physical transition has not yet occurred" [S3UG-LCT], so S3 itself separates the two.
   *INFERENCE:* mantle commits the class change when its lifecycle executor acts, and the move
   follows. A GET of an object whose class is now GLACIER answers `InvalidObjectState` even if its
   bytes are still on the hot pool; the client sees S3's semantics and mantle sees a placement
   to fix.
2. **Moves are rate-capped and started early.** Unthrottled redundancy changes consumed 100% of
   cluster I/O for weeks, and PACEMAKER kept them at or below 5% by starting early under a peak
   cap (note 10 §7.5, KMS+20). Lifecycle due dates cluster: every object created on one day
   reaches its 30-day rule on one later day, and at agentic scale that is a burst of billions.
   The move scheduler spreads each day's due moves over the time until their deadline at the rate
   the background budget allows (note 10 R6), in its own admission class (notes 26, 27).
3. **Each move has a deadline from capacity, not from a constant.** A move from the hot pool frees
   hot capacity; the deadline is when the hot pool's free-space runway (the cleaner's runway rule,
   chunk-store.md §8, applied to the pool) would otherwise run out. A move that frees nothing the
   pool needs may wait for idle or AC power (§4.5) indefinitely; mantle reports the backlog.

A move is a multi-step operation: copy the blocks to the new placement, switch the placement
record, release the old blocks to the collector. A crash between steps leaves either the old or
both placements referenced, never neither; note 22 §9 covers taking over such operations.

**Intelligent-Tiering access tracking.** The thresholds are counted in "consecutive days" [S3UG-ITW],
so a day is the resolution the API needs. *DERIVED:* storing the last-access day per object version
and writing it only when an access falls on a later day than the one stored bounds the tracking
cost at one metadata write per object per day on which it is accessed, whatever the request rate;
an agent that GETs one object a million times in a day costs one write. Objects under 128 KB are
not tracked at all [S3UG-ITW]. Only the operations AWS lists count; HEAD, listing and tagging do
not.

### 3.7 One laptop with one NVMe

A laptop is a region of one cell of one node (node.md). What each class can and cannot be there:

| Holds on one device | Does not hold |
|---|---|
| Every API answer of §2.9: the class is stored and reported, archived objects refuse GET until restored, restore expiry, Intelligent-Tiering tiers, lifecycle transitions | Zones: every class is one-zone in fact |
| The write plans' batching and lifetime streams (§3.4) | Separate media pools: Express and Deep Archive share one device |
| Restore as an API: it completes as soon as the copy is written (§2.10) | A cold pool; nothing spins down |
| Protection against latent sector errors, if the scheme stripes a block across segments of the one device (intra-device redundancy, as Pergamum's intra-disk parity [PERG08 Abstract]) | Protection against the loss of the device: no scheme on one device survives it |

**What is reported.** The durability model computed with the domains actually present gives the
achieved annual loss bound per class; on one device it is bounded below by the device's own failure
rate (2.7% a year for flash at the field default, durability.md §5), far from 10⁻¹¹. mantle reports,
per class, the target, the achieved bound and the reason (one device, one zone), in its status
output and logs at startup and whenever placement changes; not in S3 responses, which have no field
for it. Whether to refuse archive or multi-zone classes on such a deployment is the operator's
choice; the default is to accept and report, because the owner's direction is that classes are
honored in the API on every step (INFERENCE).

**What the class still changes on a laptop.** Express runs the no-wait writer; archive classes take
the lifetime stream and are the first background moves deferred on battery; restored copies
expire; Intelligent-Tiering moves cost metadata writes only, since there is no colder medium to move
to.

---

## 4. Power and energy

### 4.1 What each OS reports

All of these are readable without privileges unless noted.

| Input | macOS | Linux | Windows |
|---|---|---|---|
| Power source (AC, battery, UPS) | `IOPSGetTimeRemainingEstimate` returns `kIOPSTimeRemainingUnlimited` (−2.0) on "an external power source"; `IOPSGetProvidingPowerSourceType` returns "AC Power", "Battery Power" or "UPS Power"; notify key `kIOPSNotifyPowerSource` ("com.apple.system.powersources.source"), which IOKit posts "upon connecting or disconnecting AC power to a laptop" and which Apple recommends over the time-remaining key because "your code will run less often and conserve battery life" [APPLE `IOPowerSources.h`] | `/sys/class/power_supply/<name>/type` ("Battery", "UPS", "Mains", "USB", "Wireless"), `online`, and the battery's `status` ("Unknown", "Charging", "Discharging", "Not charging", "Full") [LNX `sysfs-class-power`] | `GetSystemPowerStatus`: `ACLineStatus` 0 offline, 1 online, 255 unknown; `RegisterPowerSettingNotification` with `GUID_ACDC_POWER_SOURCE`: `PoAc`, `PoDc`, `PoHot` ("a short-term power source such as a UPS device") [MSL] |
| Saver mode | `NSProcessInfo.lowPowerModeEnabled` and `NSProcessInfoPowerStateDidChangeNotification`: "your application should attempt to reduce power usage by reducing potentially costly computation" [APPLE `NSProcessInfo.h`] | `/sys/firmware/acpi/platform_profile`, which selects a profile and "is NOT a goal of this API to allow monitoring the resulting performance" [LNX `sysfs-platform_profile.rst`] | `SYSTEM_POWER_STATUS.SystemStatusFlag` and `GUID_POWER_SAVING_STATUS` ("Applications should register for this notification and save power when battery saver is on"); `GUID_ENERGY_SAVER_STATUS` with `ENERGY_SAVER_OFF`, `ENERGY_SAVER_STANDARD` ("Save energy if the user experience impact is minimal"), `ENERGY_SAVER_HIGH_SAVINGS` (marked prerelease); `PowerRegisterForEffectivePowerModeNotifications` (Windows 10 1809+) [MSL] |
| Thermal state | `NSProcessInfo.thermalState`: Nominal, Fair, Serious, Critical, with a recommendation for each (below); `OSThermalNotification.h` defines pressure levels Nominal, Moderate, Heavy, Trapping, Sleeping on macOS and the notify name `kOSThermalNotificationPressureLevelName` [APPLE] | `/sys/class/thermal/thermal_zone*/temp`, `trip_point_*_temp`, `trip_point_*_type` [LNX `thermal/sysfs-api.rst`]; NVMe hwmon temperatures and alarm (note 10 §6.4) | No public thermal-state API for desktop processes was found (UNVERIFIED); device temperature through `StorageDeviceTemperatureProperty` (note 10 §6.6) |
| Measured power | Battery `kIOPSVoltageKey` (mV) and `kIOPSCurrentKey` (mA) [APPLE `IOPSKeys.h`]; [obs] `ioreg` shows `Voltage` 12,683 mV and `InstantAmperage` on this machine; `powermetrics` estimates CPU, GPU and ANE power, but "Average power values reported by powermetrics are estimated and may be inaccurate" [APPLE `powermetrics(1)`] | Battery `power_now` (µW) or `current_now` (µA, "not averaged/smoothed") with `voltage_now` [LNX]; RAPL `energy_uj` in powercap zones, readable by root only (`dev_attr_energy_uj.attr.mode = S_IRUSR` in `powercap_sys.c`); RAPL covers package, cores, DRAM and platform, not drives | `IOCTL_BATTERY_QUERY_STATUS` → `BATTERY_STATUS.Rate`, "in milliwatts unless the battery rate information is relative"; negative while discharging [MSL] |
| Device power | NVMe SMART "Interval Power Measurement" and "Operational Lifetime Energy Consumed", if the drive reports them (note 10 §6.1); readable through IOKit [obs, note 10 §6.5] | same fields, through Get Log Page (needs `CAP_SYS_ADMIN`, note 10 §6.4) | same, through `IOCTL_STORAGE_QUERY_PROPERTY` (privilege UNVERIFIED, note 10 §6.6) |
| Background I/O priority | `setiopolicy_np(IOPOL_TYPE_DISK, …)`: `IOPOL_UTILITY` "for short-running background work", `IOPOL_THROTTLE` "for long-running I/O intensive background work"; "If a throttleable request occurs within a small time window of a request of higher priority, the thread that issued the throttleable I/O is forced to a sleep for a short period" [APPLE `getiopolicy_np(3)`] | `ioprio_set`, `IOPRIO_CLASS_IDLE`: "get I/O time only when no one else needs the disk"; documented for CFQ; which current multiqueue schedulers honor it, and that NVMe devices default to none, is UNVERIFIED here [IOCTL-PRIO] | `SetFileInformationByHandle` with `FILE_IO_PRIORITY_HINT_INFO`: "Whether these priorities are supported and honored by the underlying drivers depends on their implementation (which is why they are called hints)" [MSL] |
| CPU efficiency for background threads | QoS classes (`NSProcessInfoThermalStateDidChangeNotification`'s note points background work to "Quality of Service levels") [APPLE] | none standard | EcoQoS via `SetProcessInformation`/`SetThreadInformation`, which "Always selects most efficient CPU frequency and schedules to efficient cores" [MSL "Quality of Service"] |

**Apple's thermal recommendations** [APPLE `NSProcessInfo.h`], verbatim:

- Nominal: "No corrective action is needed."
- Fair: "Recommendation: Defer non-user-visible activity."
- Serious: "Recommendation: reduce application's usage of CPU, GPU and I/O, if possible."
- Critical: "reduce application's usage of CPU, GPU, and I/O to the minimum level needed to respond
  to user actions."

On systems "where thermal state is unknown or unsupported", the property is always Nominal.

**Notification, not polling.** Every OS above delivers a change notification for power source and
saver mode (macOS notify keys and `NSNotification`s; Linux `power_supply` uevents, UNVERIFIED as a
delivered event for every driver; Windows power-setting notifications delivered to a window handle
or, for a service, to its `HandlerEx` with `SERVICE_CONTROL_POWEREVENT` [MSL]). mantle reads the
state once at start and then on each notification. Where no notification exists (Linux thermal zones,
NVMe temperature), it reads on the cadence the source updates at, which note 10 §9.2 derives for NVMe
SMART (no faster than once a minute for minute-resolution fields).

### 4.2 NVMe power states and APST

**The specification** (NVMe24, **primary**). A controller defines up to 32 power states, "contiguously
numbered starting with zero such that each subsequent power state consumes less than or equal to the
maximum power consumed in the previous state" [§8.1.19, p. 666]. Each has a descriptor (Fig. 340):

- Maximum Power (MP), "the sustained maximum power consumed by the NVM subsystem in this power state";
- Non-Operational State (NOPS): "the controller does not process I/O commands in this power state";
- Entry Latency (ENLAT) and Exit Latency (EXLAT), maximum, in microseconds;
- Idle Power (IDLP), "the typical power consumed … over 30 seconds in this power state when idle", and
  Active Power (ACTP), "the largest average power … over a 10 second window on a particular workload";
- Relative Read and Write Throughput and Latency, an ordering only.

"The maximum amount of time to transition between any two power states is equal to the sum of the old
state's exit latency and the new state's entry latency" [§8.1.19, p. 667]. In a non-operational state
"the controller shall autonomously transition back to the most recent operational power state to
process an I/O command" [§8.1.19.1, p. 668].

APST (Feature 0Ch) is a table of 32 entries; each gives an Idle Time Prior to Transition (ITPT, ms) and
an Idle Transition Power State (ITPS), "the power state to which the controller autonomously
transitions, after there is a continuous period of idle time in the current power state that exceeds
the time specified" [§5.2.30.1.7, Fig. 477]. APSTE's "default value … shall be cleared to '0'" [Fig.
475]: the host turns it on.

**What the OS does with it.**

- **Linux** [LNX `core.c`]: `default_ps_max_latency_us = 100000`; `apst_primary_timeout_ms = 100` with
  `apst_primary_latency_tol_us = 15000`; `apst_secondary_timeout_ms = 2000` with
  `apst_secondary_latency_tol_us = 100000`. "The default parameter values were selected based on the
  values used by Microsoft's and Intel's NVMe drivers. Yet, since we don't implement dynamic
  regeneration of the APST table in the event of switching between external and battery power, the
  timeouts and tolerances reflect a compromise between values used by Microsoft for AC and battery
  scenarios." The per-device limit is the PM QoS latency tolerance, `/sys/devices/.../power/
  pm_qos_latency_tolerance_us` [LNX `sysfs-devices-power`], writable only with privilege.
- **Windows** [MSL StorNVMe]: "For Modern Standby support, StorNVMe does not support devices with APST
  enabled"; the driver itself moves an idle device into the deepest state "where ENLAT+EXLAT is less
  than or equal to the current transition latency tolerance". Balanced scheme: primary idle timeout
  200 ms on AC and 100 ms on DC, tolerance 15 ms (AC) and 50 ms (DC); secondary 2,000/1,000 ms and
  100 ms.
- **macOS**: Apple documents no equivalent interface (UNVERIFIED).

**A measured device.** HA20's Samsung 960 reported five states: three operational at 6.04, 5.09 and
4.08 W, and two non-operational at 40.0 mW (enter 210 µs, exit 1.5 ms) and 5.0 mW (enter 2.2 ms, exit
6.0 ms) [HA20 Table 3]. *DERIVED:* under Linux's defaults its 5 mW state qualifies for the primary
tolerance (8.2 ms ≤ 15 ms), so after 100 ms idle the drive drops to 5 mW and the next I/O waits up to
6 ms to wake. A writer that flushes one small request every few hundred milliseconds wakes it every
time; one that flushes the same bytes in fewer, larger batches lets it sleep between them.

**The energy of a wake** is not in the specification: it gives powers and latencies, not transition
energy. *DERIVED, assuming the device draws no more than the operational state's MP while it
transitions (the specification bounds non-operational activity by "the maximum power advertised for
the most recent operational power state", §8.1.19.1, but does not say this of transitions):* a wake
costs at most MP₀ · (ENLAT + EXLAT), and sleeping saves energy once the idle period exceeds
`T_be = MP₀ · (ENLAT + EXLAT) / (P_idle,0 − IDLP_k)`, with `P_idle,0` the measured or reported idle
power of the operational state and `IDLP_k` that of the target state. Both powers are measured where
the drive does not report them.

**What mantle does with this (INFERENCE).** It does not program APST or power states: the OS owns
them, programming them needs privilege, and they affect every other user of the device. It shapes its
I/O so the device can sleep: batches on battery (§4.5), background work coalesced into bursts rather
than trickled, no periodic polling of the device faster than the data needs (note 10 §9.2: an NVMe
hwmon read on Linux issues an admin command each time). It measures the wake: the latency of the first
I/O after an idle gap of each length, against the steady latency, gives the device's effective EXLAT
and the idle time at which it sleeps (§8). An operator who wants Express to keep the device awake may
set the PM QoS tolerance; mantle does not do it by default.

### 4.3 Disk power: spin-down, its energy and its wear

- **Break-even.** "Disks in standby mode use considerably less energy than disks in active mode, but
  have to be spun up to full speed before they can service any requests. This incurs a significant
  energy and time penalty (e.g., 135 Joules and 10.9 seconds for IBM Ultrastar 36Z15 disks). To justify
  this penalty, the energy saved by putting the disk in standby mode has to be greater than the energy
  needed to spin it up again – which will only be true if the next request arrives after a break-even
  time. Unfortunately, this is rarely the case in intense, enterprise workloads" [HIB05 §2.1, p. 178].
- **Wear.** "the number of start/stop cycles a disk can tolerate during its service life time is still
  limited, and many disk specifications provide an expected lifetime value (e.g., the IBM Ultrastar
  36Z15 can handle a minimum of 50,000 start/stop cycles)"; at 25 changes a day that limit lasts "6
  years" [HIB05 §3.1, p. 180].
- **Datasheet values** (DATASHEET). Idle 5.5 W (SATA) for the HC580, with "Idle specification … based on
  use of Idle_A" [WD-HC580]; for the BarraCuda 8 TB, operating 5.3 W, idle 3.4 W, standby and sleep
  0.25 W, startup current 2 A [SG-BC]. The HC580 gives no standby power or start/stop rating; the
  BarraCuda gives load/unload but not start/stop.
- **Rack-scale cold storage.** Pelican provisions power and cooling for 8% of its disks spinning, and
  its scheduler batches by group to amortize spin-ups [PEL14 Abstract; §2.2.3]; its disks' spin-ups per
  year peaked at a middling request rate, 0.5 per second, and fell above it as queues allowed reordering
  [PEL14 §4.7, p. 362]. Its spin-downs are "controlled head park and unpark operations triggered by
  issuing a SATA command … not induced by sudden power failure", and "all the electronics in the drive
  are still powered" [PEL14 §4.7].
- **HA20** measured a whole server idling at 29 W, 39 W with one HDD added, and 31 W with one SATA or
  NVMe flash drive: "HDDs have a notoriously high idle power consumption without performing any work due
  to their constantly spinning disks" [HA20 §3.4, Obs. 1].

*INFERENCE:* spinning disks down is a placement decision for whole groups of archive data with a
start/stop budget (§5.2), never a per-device idle timer that mantle sets on online pools; an online
disk's idle-time head parking is the drive firmware's, and mantle's part is not to issue I/O patterns
that trigger it thousands of times a day (§5.2).

### 4.4 Energy per I/O: what is measured and what is not

- **Request size.** "for any given IO depth and storage device, larger requests consistently provide
  better overall energy efficiency measured as bytes per joule. Even though a larger request has more
  data to be transferred than a smaller request, it seems that the energy cost of transferring
  additional data in one request is less significant than the cost of managing the request itself and
  the pressure put on system software" [HA20 §3.8, Obs. 6]. For flash NVMe, "increased power
  consumption corresponds to continuously increased IO performance", so depth up to the device's
  parallelism also improves energy per I/O; for Optane it fell past the saturation depth [HA20 §3.8, Obs. 7].
- **Read/write symmetry.** The HDD and flash SSDs measured were roughly "energy symmetric"; the Optane
  drive was "energy asymmetric … writes cost more energy" [HA20 §3.5, Obs. 3; §4].
- **Durable small writes.** On an Android phone's eMMC, with each 4 KiB write followed by `fsync`,
  "random writes consume 19× more energy than sequential writes in ext4 (12× in F2FS)", the sequential
  writes being 512 KB; ext4 wrote about 70 MB at the block layer for 10 MB of random writes, F2FS about
  30 MB [MOH17 §3.1]. The method is differential: measure idle, then each component's addition with a
  hardware power monitor on the battery terminals [MOH17 §2].
- **Not found.** A peer-reviewed measurement of the energy of one durable flush (`fdatasync`,
  `F_FULLFSYNC`, FUA or a flush command) on a current NVMe drive, of laptop SSD energy per byte against
  batch size, or of the energy cost of an APST wake. These are mantle's to measure (§8).

### 4.5 The policy

Acknowledgement is unchanged by power state: on battery a write is acknowledged exactly when it is
durable, as on AC. What changes is how much is batched, what background work runs, and when.

**The writer on battery.** Note 11 §2.3 derived that "any wait rule for closed-loop submitters should
keep the total wait per batch below `S`", the measured batch service time, because waiting beyond `S`
loses to simply alternating batches. On AC the wait is chosen to minimize latency (`Δ(t)` of note 11
§2.6). *DERIVED:* on battery each request that joins a batch saves one flush's energy; the energy saved
grows with the wait while the latency cost is bounded, so within the bound `t − e ≤ S` the
energy-minimizing wait is the longest: wait until every outstanding submitter has returned, the batch
limits are reached, or `S` has elapsed. A lone request's latency at most doubles (from `S` to `2S`) and
throughput is not reduced, because the bound is the one that preserves it. A latency objective the
operator states can only shorten the wait. No new constant enters.

**Background work on battery or under saver mode.** Each background job already has a deadline from its
own design; on battery it runs only when its deadline requires it, and otherwise waits for AC or for
saver mode to end:

| Work | Its deadline, from | On battery or saver |
|---|---|---|
| Repair | the durability model's urgency: stripes at margin μ ≤ 1 at once (note 04 §R3) | never deferred at μ ≤ 1; repair with margin to spare deferred until its computed deadline (note 10 R3, R6) |
| Cleaning | the free-space runway (chunk-store.md §8) | cleaning that the runway requires runs; cleaning beyond it waits |
| Scrub | the period's upper bound (chunk-store.md §9, and §5.2's workload bound for disks) | scheduled to finish within the bound using AC time first; runs on battery only when the bound would otherwise be missed |
| Engine compaction | the engine's own write-stall triggers (note 12, note 23) | compaction a stall would force runs; the rest waits |
| Lifecycle and tier moves | the source pool's capacity runway (§3.6) | wait |
| Restores | the tier's documented completion time (§2.4) | Expedited runs; Standard and Bulk run when their objective requires |
| Calibration and benchmarks | operator request | refused on battery unless forced, since their measurement would be of a power-managed device |

**Thermal state.** The response is Apple's stated one, applied to mantle's classes of work; the same
mapping is used for Linux trip types and NVMe thresholds (note 10 R10):

| State | Source | mantle |
|---|---|---|
| Fair (macOS); a passive trip point approached (Linux) | "Defer non-user-visible activity" | background work as on battery |
| Serious; NVMe Composite Temperature ≥ WCTEMP | "reduce … CPU, GPU and I/O, if possible"; WCTEMP's "Immediate remediation is recommended (e.g., additional cooling or workload reduction)" (note 10 R10) | also: writer on its battery rule; coding pool limited to what admitted foreground work needs; device marked `Throttled` (note 10 R10) |
| Critical; NVMe ≥ CCTEMP | "the minimum level needed to respond to user actions" | foreground admission narrowed to what keeps durability and answers in-flight requests; new work refused with S3's retryable 503 (note 25 §3); repair at μ = 0 continues |

**Background CPU and I/O priority.** Background threads (cleaner, scrubber, mover, restore copier) run
at the platform's background priority where one exists (`IOPOL_THROTTLE`; `IOPRIO_CLASS_IDLE`; a low I/O
priority hint and EcoQoS on Windows), always beneath mantle's own pacing (note 10 R6), which works
whether or not the OS honors the hint.

### 4.6 Where each value comes from

- The power state, saver mode and thermal state: reported by the OS (§4.1).
- `S` and the wait rule: measured by the writer (note 11 §2).
- Deadlines: derived from the durability model, the runway rule, the scrub bound and the documented
  restore times, each cited above.
- Energy per flush, per byte, per wake; idle and sleep powers; the device's sleep idle time: measured on
  the device (§8), or reported by it (NVMe IDLP, ACTP, Interval Power Measurement).
- Thermal thresholds: reported by the device (WCTEMP, CCTEMP) or the OS (trip points), and Apple's
  states as Apple defines them.

---

## 5. Device longevity

### 5.1 Flash

- **Ratings.** A drive's endurance is stated as terabytes written (TBW) over its warranty, or drive
  writes per day (DWPD). *DERIVED, from the definitions:* DWPD = TBW / (capacity · days of warranty).
  The reseller-listed figures for one data-center model, 28,000 TB written for 15.36 TB at 1 DWPD over
  five years, satisfy it (28,000 / (15.36 × 365 × 5) = 0.999) (UNVERIFIED against the vendor's brief).
  JESD218, which defines the test, was not read.
- **What the drive reports.** Percentage Used, "a vendor specific estimate of the percentage of NVM
  subsystem life used"; the Endurance Group log's Endurance Estimate, bytes writable "assuming a write
  amplification of 1", and Media Units Written beside Data Units Written (note 10 §6.1–§6.2). Life
  consumed is Media Units Written over Endurance Estimate (note 10 R9). The rating need not come from a
  datasheet.
- **Write amplification is a product.** Device bytes written = client bytes × mantle's write
  amplification (the cleaner's relocations, measured exactly; chunk-store.md §8) × the device's own
  (Δ Media Units / Δ Data Units, where reported; note 10 R11). The chunk store is a log on top of the
  drive's own log, the case LOL14 studies: "the increased write pressure and destroyed sequentiality due
  to unaligned segment sizes, unpredictable workloads, and uncoordinated log activities such as garbage
  collection negates many of the positive affects of using a log", and "matching segment sizes between
  upper and lower logs" mitigates it "to some extent" [LOL14 §4.2; Conclusion]. LOL14 also states "the need for
  TRIM" by the upper log [LOL14 §4.5.1], which the chunk store's discard of whole freed segments supplies
  (note 03 R3; note 10 R11).
- **Field context.** Wear rarely ends an SSD in the field: 99% of NetApp systems used at most 15% of
  rated life (note 10 §1, MMES20). The longevity concern is therefore largest where writes are heavy
  relative to capacity: a laptop SSD that also holds the user's own files, and mantle's metadata
  devices.

### 5.2 Disks

**Workload rate limit.** Seagate's definition counts reads: "Annualized Workload Rate = (Lifetime Writes +
Lifetime Reads) * (8760 / Lifetime Power On Hours)", with limits of "<550 TB/yr" for Enterprise Capacity
(Nearline) and "<180 TB/yr" for Terascale (Nearline Lite); "If the value is above the WRL then the
reliability of the drive will begin to decline" [SG-AWR] (DATASHEET). Western Digital rates the HC580 for
"workloads of up to 550TB per year" and states "Derating of MTBF and AFR will occur above these parameters,
up to 550TB/year and 60°C (device reported temperature)" [WD-HC580]; whether Western Digital also counts
reads is not stated in the data sheet (UNVERIFIED), and this note assumes it does, which is the conservative
reading. The desktop BarraCuda is rated 55 TB a year [SG-BC].

**What it means for scrubbing (DERIVED).** chunk-store.md §9 reads every live record "at least every 14
days, targeting 7". For a full disk of capacity C scrubbed every T days, scrub alone transfers
C · 365 / T a year:

| Disk | Rating | 7-day scrub | 14-day scrub | Shortest period within the rating, scrub alone |
|---|---|---|---|---|
| 24 TB (HC580) | 550 TB/yr | 1,251 TB/yr (2.3×) | 626 TB/yr (1.14×) | 15.9 days |
| 8 TB (BarraCuda) | 55 TB/yr | 417 TB/yr (7.6×) | 209 TB/yr (3.8×) | 53 days |

Client reads and writes, cleaning and repair spend the same budget, so the real bound is longer. The scrub
period on disks is therefore bounded below by `T ≥ C · 365 / (share of the rating left for scrub)`, and
bounded above by the durability model's need for detection (§3.3). note 11 §12.4 bounds the period from the
error rate and bandwidth; this is a third bound, and the three can conflict: if the durability model needs
a shorter period than the workload rating allows, the class needs more parity, which is §3.3's conclusion
reached from the other side. Pelican did not scrub at all in its prototype for this reason: "Background
scrubbing increases the volume of data transferred per disk, which in itself impacts the disk AFR" [PEL14
§4.7, p. 362].

**Power-on hours.** The BarraCuda is rated for "Power-On Hours (per year) 2,400" [SG-BC]; the HC580 for
"24x7" [WD-HC580]. *DERIVED:* a desktop disk running mantle continuously exceeds its rated power-on hours
3.65 times (8,760 / 2,400). The OS reports neither rating, and the drive reports neither.

**Load/unload and start/stop.** The HC580 is rated for "Load/Unload cycles (at 40°C) 600,000"; BarraCuda
models for 600,000 or 50,000 [WD-HC580; SG-BC]. *DERIVED:* 600,000 over a five-year warranty is about 329 a
day, one every 4.4 minutes on average; 50,000 over two years is about 68 a day. Head parking on idle is the
drive's firmware policy (the HC580's idle figure is for "Idle_A"; the T13 Extended Power Conditions that
define such states were not read). A host that issues one small I/O just after each park, for example a
periodic flush or a health poll every few minutes, spends a cycle each time. Start/stop cycles are a
separate rating (HIB05's 50,000 example, §4.3) that spin-down spends.

**SMR.** Drive-managed SMR wants large sequential writes and idle time for cleaning (note 03 §12.2–12.3,
R11.7); BarraCuda 8, 6, 4, 3 and 2 TB models are SMR [SG-BC]. A desktop with such a drive is the archive
pool's natural medium and the worst medium for Express.

**What the drive reports.** "Current Seagate disk drives keep track of various drive usage such as power on
hours, lifetime writes and lifetime reads from the host computer" [SG-AWR]; which SMART attributes or logs
carry the lifetime totals is vendor-defined and UNVERIFIED here. ATA SMART attributes 4, 9, 193 (start/stop
count, power-on hours, load/unload count) are readable on Linux only with `CAP_SYS_RAWIO` (note 10 §6.4); Windows' `MSFT_StorageReliabilityCounter`
reports power-on hours and start-stop and load-unload cycles (note 10 §6.6, privilege UNVERIFIED). mantle's
own I/O counters are always available.

### 5.3 How mantle budgets, spreads and retires

- **Budgets.** For each device, an annual budget per rating: for disks, bytes read plus written against the
  workload rating; power-on hours; load/unload and start/stop cycles; for flash, media bytes written against
  the Endurance Estimate over the planned service life. Each budget is the rating, from the device where it
  reports one (NVMe), from the operator otherwise, and in the absence of both the most conservative value
  among the cited datasheets of that device class (55 TB a year and 50,000 cycles for disks [SG-BC]), the
  same rule durability.md §5 uses for failure rates: "the conservative end of its source".
- **Spending.** Foreground, repair, cleaning, scrub and moves draw on the budget. Repair at μ ≤ 1 is never
  refused for budget. Scrub's period is set from what is left (§5.2). Placement prefers, among feasible
  devices, the one whose projected budget exhaustion is latest, the same "equal projected wear-out dates"
  rule note 10 R9 gives for flash, extended to disks, with Diff-RAID's caution not to let a stripe's members
  converge (note 10 §7.4).
- **Counting cycles without privilege.** The latency of the first I/O after an idle gap reveals a park or a
  spin-down: on a disk it adds the unload/load or spin-up time to the request. mantle counts such events per
  device from its own latencies and compares the count with SMART's where SMART is readable (§8).
- **Retirement.** A device whose budget will run out before its planned retirement, or whose health state
  reaches `AtRisk` (note 10 R1–R3), is drained by the same path repair uses, with its drain started early
  enough to finish under the background budget (note 10 R6, PACEMAKER's rule). Exceeding a workload rating
  is a prior on failure rate ("Derating of MTBF and AFR"), so it enters the device's failure probability in
  note 10 R3 as a covariate, not as a hard stop.

---

## 6. Stepped complexity

Each step lists only what it adds. Every step runs the same binary and the same code path.

**Step 1: one laptop, one device.**
- Storage class, Intelligent-Tiering tier, and restore record stored per object version; every API answer
  of §2.9; restore completes as soon as its copy is written.
- One media pool; the class chooses the writer rule (Express no-wait) and the lifetime stream (§3.4).
- Power inputs of §4.1 and the battery, saver and thermal policy of §4.5.
- Flash budget from Percentage Used and Media Units Written (§5.1); mantle's and the device's write
  amplification reported.
- Achieved durability per class reported against its target (§3.7).
- Measurements: energy per flush and per byte against batch size; wake latency against idle gap (§8).

**Step 2: one node, several devices.**
- Media pools from detection and measurement (§3.2); class eligibility per pool.
- Per-device budgets for disks: workload, power-on hours, cycles (§5.2); scrub period bounded by them.
- Lifecycle and tier moves between pools as background moves (§3.6); restore staging between pools (§3.5).
- Device failure domains: a block's chunks on distinct devices; the model's achieved bound now includes
  device loss.

**Step 3: a cell.**
- Rack and host failure domains; copyset permutations per media pool (note 04 §R2).
- Cold pools with spin-down groups, Pelican-style group scheduling of restores and archive writes, and the
  start/stop budget (§3.5, §4.3).
- Restore admission bounds and refusals (§3.5); move rate caps under the cell's background budget.
- Fleet-learned failure rates per model key replace priors (note 10 §9.1).

**Step 4: a region.**
- Zones: ≥ 3-zone and one-zone classes become physically distinct (§3.1, §3.3); the zone-loss exposure of
  one-zone classes reported.
- Directory buckets and CreateSession for EXPRESS_ONEZONE, zonal placement near compute (§2.8).

**Step 5: the global fleet.**
- Lifecycle due dates of billions of objects spread across cells' budgets (§3.6, PACEMAKER).
- Intelligent-Tiering tracking at one write per object per access-day (§3.6), the only form that survives
  agents reading one object at machine speed.
- Restore request rate limits per principal derived from measured restore capacity, refused as S3 refuses
  (§2.4).

---

## 7. Proposed design

Each decision names its source and the measurement that confirms it.

**D1. The class is stored and answered exactly as S3 does.** Per object version: class; Intelligent-Tiering
tier and last-access day; restore record (state, request date, completion, expiry). Validated on PutObject,
POST, CopyObject (default STANDARD) and CreateMultipartUpload; returned on HEAD and GET except for STANDARD,
in listings, ListParts and ListMultipartUploads; GET and copy-source of an archived object without a live
restored copy answer 403 `InvalidObjectState`. *Sources:* §2.1–§2.4. *Confirmed by:* recorded S3 answers for
every class value on each operation, including the values §2.1 marks UNVERIFIED; s3-tests' `storage_class`,
`lifecycle_transition` and `restore` markers (note 05 §15.3), which need "at least two storage classes configured"
(note 13 §6.9).

**D2. Logical class and physical placement are separate.** API behavior follows the class; placement follows
the class's plan and may lag it during a move or differ from it where the deployment cannot meet it.
*Sources:* S3's asynchronous transitions billed from the rule date [S3UG-LCT]. *Confirmed by:* crash tests at
every step of a move (§8).

**D3. Durability targets per class.** 10⁻¹¹ per block-year for every class but RRS; one-zone classes evaluated
without zone loss and their zone exposure reported; RRS at STANDARD's target unless the operator adopts AWS's
10⁻⁴; any class may be given a stricter target. *Sources:* §2.2; durability.md §1, §4. *Confirmed by:*
`mantle durability` run per class with that pool's measured rates, including detection latency in the repair
rate (D4).

**D4. Detection is part of a class's repair rate.** The repair rate the model uses for a pool is
1/(mean detection time + rebuild time), with detection from the pool's measured device-loss detection time
and its scrub period. *Sources:* durability.md §2; Ford via note 04 §R3; PEL14 §2.2.2. *Confirmed by:* measured
detection times per pool, and a model run showing which codes meet the target at the scrub period D11
allows.

**D5. Media eligibility by measurement.** EXPRESS_ONEZONE only on pools whose measured read service time at
its depth meets "single-digit millisecond" at the deployment's stated quantile; millisecond classes on any
online pool; archived classes on any pool. *Sources:* §3.2. *Confirmed by:* the calibration of measurement.md
reported per pool.

**D6. Write plans per class.** Express: no-wait writer, copies, fastest pool. Online classes: today's writer.
Classes with a minimum duration: written to the long-lived stream. Archived classes: acknowledged once durable
at the class's target on an online staging placement, moved in large sequential batches. *Sources:* §3.4;
note 11 §2.4; chunk-store.md §8; PERG08. *Confirmed by:* the cleaning model on a class-labelled trace (cleaner
write amplification with and without the lifetime hint); Express latency quantiles against STANDARD's.

**D7. Restores.** Temporary copy on an online pool under the STANDARD plan; expiry at completion plus `Days`
rounded to the next 00:00 UTC; removed by the collector at expiry; one restore per object; lifecycle
expiration wins. *Sources:* §2.4; note 22. *Confirmed by:* API tests with a controlled clock, including expiry
across midnight and re-restore.

**D8. Restore admission is bounded.** One bounded queue per tier per pool; online capacity for restored copies
bounded by a share of the pool's free-space runway; refusals `GlacierExpeditedRetrievalNotAvailable`, S3's
retryable 503, and throttling. The bounds are derived from measured restore throughput so that a queued job
completes within its tier's documented time [S3UG-RET]; Bulk is batched by spin-up group with Pelican's
reorder bound taken from that time. *Sources:* §3.5; PEL14 §2.2.3. *Confirmed by:* restore completion-time
distributions per tier under load, against the documented times.

**D9. Moves are background work with deadlines from capacity.** Class change committed by the lifecycle
executor; physical move copy-switch-release; rate-capped and spread to the deadline; deadline from the source
pool's runway. *Sources:* §3.6; note 10 §7.5 (KMS+20); note 22 §9. *Confirmed by:* a simulated day on which
many objects come due, showing background I/O under the cap and no deadline missed.

**D10. Intelligent-Tiering tracking at day resolution.** Last-access day per object version ≥ 128 KB, written
only when an access falls on a later day, by the operations AWS lists. *Sources:* §2.7. *Confirmed by:* a
benchmark of metadata writes per GET under repeated access (bounded at one per object-day).

**D11. Disk workload budgets set the scrub period.** For disks, the scrub period is at least
C · 365 / (the share of the workload rating left after measured client, cleaning and repair traffic), and at
most what the durability model needs; a conflict is resolved by parity, not by exceeding the rating.
*Sources:* §5.2; SG-AWR; WD-HC580; PEL14 §4.7. *Confirmed by:* per-device annualized read+write accounting,
compared with the drive's own counters where readable.

**D12. Device budgets and spreading.** Per-device budgets for flash media writes, disk workload, power-on
hours and cycles; ratings from the device, the operator, or the most conservative cited datasheet value;
placement by latest projected exhaustion; draining before exhaustion. *Sources:* §5.3; note 10 R9, R3, R6.
*Confirmed by:* endurance accounting tests (§8).

**D13. Power inputs are read from the OS and normalized.** Source {AC, battery, UPS, unknown}, saver {off,
standard, high}, thermal {nominal, fair, serious, critical}, from the interfaces of §4.1, by notification where
one exists. *Sources:* §4.1. *Confirmed by:* integration tests on each OS toggling the inputs (where CI can)
and recorded transitions on real laptops.

**D14. On battery, the writer waits up to S and background work waits for its deadline.** Acknowledgement
unchanged. *Sources:* note 11 §2.3; HA20 Obs. 6; MOH17 §3.1; §4.5. *Confirmed by:* energy per acknowledged
byte on battery with and without the rule, at fixed client load, by the differential method (§8), and latency
quantiles showing the bound `2S` holds.

**D15. Thermal response follows the platform's stated recommendations.** Apple's four states; Linux trip
types; NVMe WCTEMP and CCTEMP. *Sources:* §4.1, §4.5; note 10 R10. *Confirmed by:* a test that forces each
state (where the OS allows simulation, e.g. Linux `emul_temp` [LNX]) and checks which work runs.

**D16. mantle does not program device power states.** It shapes I/O so devices can sleep and measures what
waking costs; PM QoS for Express is an operator option. *Sources:* §4.2. *Confirmed by:* the wake-latency
measurement (§8) on Linux and macOS, and energy with and without batching.

**D17. Classes are honored on one device, and what placement achieves is reported.** *Sources:* §3.7.
*Confirmed by:* the status output on a laptop listing per class the target, the achieved bound and the reason.

---

## 8. Test and benchmark plan

**API conformance.**
- Record S3's answers (as notes 13 and 19 did) for: every enumerated class value on PutObject, POST, CopyObject
  and CreateMultipartUpload in a general purpose bucket; RestoreObject on STANDARD, GLACIER_IR, a restore in
  progress, a restored object, an Intelligent-Tiering object with and without `Days`; GET and UploadPartCopy of
  archived sources; the 94-day example of §2.5. Replay them against mantle.
- s3-tests with `storage_class`, `lifecycle_transition` and `restore` enabled, which needs mantle to advertise
  at least two classes (note 05 §15.3; note 13 §6.9).
- A controlled-clock test for restore expiry (rounding to 00:00 UTC, re-restore, lifecycle expiration
  overriding), Intelligent-Tiering thresholds at 30, 90 and the configured archive days, and the 128 KB rules.

**Crash consistency of moves.** The chunk store's crash tests (chunk-store.md §10) extended to the move
protocol: kill between copy, switch and release, and verify on recovery that the object is readable at
exactly one class with all its blocks referenced once.

**Simulation.** The deterministic simulation (CLAUDE.md rule 8) with lifecycle bursts, restore floods beyond
the bounds (typed refusals, no unbounded queue), and power-state changes mid-batch (no acknowledgement before
durability).

**Energy on a laptop (macOS).** Differential measurement after MOH17 §2:
1. Run on battery, display at fixed brightness, network quiet. Sample battery voltage and current
   (`kIOPSVoltageKey`, `kIOPSCurrentKey`, or `ioreg` `Voltage` and `InstantAmperage`) at the gauge's update rate;
   the gauge's update period and resolution are measured first (UNVERIFIED) by holding a known CPU load.
2. Baseline: mantle idle; then a CPU-only control writing to memory; then the same client load to the device.
   The device's share is the difference. `powermetrics --samplers cpu_power` (with its stated caveat that values
   "are estimated") separates CPU power.
3. Points: client writes of 4 KiB, 64 KiB, 1 MiB and 8 MiB, at one to sixteen closed-loop writers, writer on AC
   rule and battery rule; reported as joules per acknowledged byte and per flush, in measurement.md's rounds with
   their states and intervals.
4. The NVMe SMART Interval Power Measurement and Operational Lifetime Energy Consumed fields, read through IOKit,
   where the drive reports them, as a cross-check.

On Linux laptops the same with `power_now` or `current_now × voltage_now`, and RAPL `energy_uj` as root for the
CPU; on Windows `BATTERY_STATUS.Rate`. An external meter on a desktop or server gives ground truth for the
battery method, as HA20 used a power meter.

**Wake latency.** For each device: issue one 4 KiB read after idle gaps from 1 ms to 10 s on a logarithmic
ladder, many times per gap; the first-I/O latency against the gap shows the idle time at which the device
sleeps and its effective exit latency, to compare with the reported ENLAT and EXLAT and the OS's APST table
(`nvme get-feature -f 0x0c` on Linux, as root, for the cross-check). On disks, the same ladder over minutes
finds the park timer and the spin-down timer.

**Endurance accounting.** Per device, over a long run: mantle's bytes written and read, its cleaner's relocated
bytes, and where readable the device's Data Units Written, Media Units Written, Percentage Used, power-on hours
and load/unload count, as deltas with their rounding (note 10 §9.2's counter hygiene). Checks: mantle's write
total against the device's host writes; mantle's park-event count against SMART 193; projected budget exhaustion
dates stable as the run lengthens.

**Benchmarks with baselines** (CLAUDE.md rule 8): Express put and get latency against STANDARD; restore
throughput per tier; Intelligent-Tiering metadata writes per GET; cleaner write amplification with and without
lifetime hints; energy per byte on battery and AC.

---

## 9. What remains unknown

**S3 behavior**
1. S3's answers for SNOW, FSX_OPENZFS, FSX_ONTAP, AWS_BACKUP_WARM, AWS_BACKUP_LOW_COST_WARM and EXPRESS_ONEZONE
   sent to a general purpose bucket; for RestoreObject of a never-archived object; and for the select-restore
   variant (§2.1, §2.4).
2. What HEAD reports for an object between its lifecycle due date and its physical transition (§2.6).
3. Whether mantle's lifecycle validator refuses the 94-day example as S3 does (§2.5).
4. The full API delta of directory buckets beyond §2.8, and session bounds.

**Devices and platforms**
5. Energy per durable flush, per byte against batch size, and per APST wake, on current laptop and data-center
   NVMe drives; none was found published (§4.4).
6. Whether Apple's SSDs report IDLP, ACTP or the SMART power fields, and how macOS manages their power states
   (§4.2).
7. The battery gauge's update period and resolution on each laptop, which bounds the differential method (§8).
8. Which Linux multiqueue schedulers honor `IOPRIO_CLASS_IDLE`, and whether Windows' NVMe stack honors I/O
   priority hints (§4.1).
9. Whether a public thermal-state API exists for Windows desktop processes (§4.1).
10. The T13 Extended Power Conditions semantics and each drive's park timer; whether mantle's I/O pattern on an
    online disk triggers parks, which only the wake-latency ladder and SMART 193 can show (§5.2).
11. Start/stop ratings of current data-center disks, which the datasheets read do not state (§4.3).
12. Whether exceeding a workload rating raises failure rates by a measurable amount; vendors say "derating" but
    publish no curve (§5.2).

**Model**
13. Detection times per media pool, in particular for spun-down groups, which set the archive class's code (§3.3).
14. Whether a code wider than `mantle-ec`'s tested set is needed for cold pools, and its repair cost (§3.3).
15. How much the lifetime hint reduces cleaner write amplification on real class-labelled traces (§3.4).
16. The real distribution of lifecycle due dates and restore requests at agentic scale, which sets how far moves
    must be spread and how large restore staging must be (§3.6, §3.5).
