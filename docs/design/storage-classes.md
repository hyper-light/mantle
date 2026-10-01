# Storage classes: what each S3 class promises, and where its bytes go

Status: design, 2026-09-30. Sources: docs/research/28 (storage classes, power and longevity,
cited as "28 §x"), 29 (device classes), 04 (placement and repair), 10 (device health), 15 and
durability.md (the durability model), 22 (garbage collection), 05 and 13 (S3's API); the
records this one joins: chunk-store.md, durability.md, gateway.md, metadata.md, node.md,
s3-protocol.md.

S3's storage classes are part of mantle's API from the first release, and honoured on every step
of scale: a class is stored and reported, an archived object refuses a GET until it is restored,
restores expire, Intelligent-Tiering moves objects between tiers, and lifecycle rules take
transitions, on one laptop as in a fleet (28 §3.7). What a class changes beneath the API grows
with the step: on one device, the writer's rule and the stream a write joins; across devices,
pools of media; across zones, the failure domains a class may use.

## 1. What a class is

A class states five things mantle can act on, each read off AWS's own statements and none
chosen by mantle (28 §3.1):

| Class | Access | Zones | Durability target, per year | Lifetime hint | Access hint |
|---|---|---|---|---|---|
| STANDARD | ms | ≥ 3 | 10⁻¹¹ | none | more than monthly |
| STANDARD_IA | ms | ≥ 3 | 10⁻¹¹ | ≥ 30 days | monthly |
| ONEZONE_IA | ms | 1 | 10⁻¹¹ without the zone's loss | ≥ 30 days | monthly |
| INTELLIGENT_TIERING | ms, or a restore in its archive tiers | ≥ 3 | 10⁻¹¹ | none | measured per object |
| EXPRESS_ONEZONE | "consistent, single-digit millisecond" | 1 | 10⁻¹¹ without the zone's loss | none | latency-critical |
| GLACIER_IR | ms | ≥ 3 | 10⁻¹¹ | ≥ 90 days | quarterly |
| GLACIER | a restore of minutes to hours | ≥ 3 | 10⁻¹¹ | ≥ 90 days | yearly |
| DEEP_ARCHIVE | a restore of hours | ≥ 3 | 10⁻¹¹ | ≥ 180 days | less than yearly |
| REDUCED_REDUNDANCY | ms | ≥ 3 | STANDARD's, unless the operator adopts AWS's 10⁻⁴ of objects (§4) | none | frequent |

**Archive is colder, not more durable.** AWS designs every class but Reduced Redundancy for the
same eleven nines; the Glacier classes "offer the same durability and resiliency as the S3
Standard storage class, but at lower storage costs" (28 §1, finding 1). Classes differ in
availability, zones, access latency and minimum billing terms.

**Decision: an object version's class and its placement are separate fields** (28 §3.6, D2). The
class is what the API reports and what decides a GET's answer; the placement is where its blocks
are. S3 itself separates them: it transitions "asynchronously" and bills from the rule's date
"even if the physical transition has not yet occurred" (28 §2.6). So a lifecycle executor commits
a class change when it acts and the move follows, and a GET of an object whose class is now
GLACIER answers `InvalidObjectState` even while its bytes are still on a hot pool; the client sees
S3's semantics, and mantle sees a placement still to fix. Where a deployment cannot give a class
what it asks, one zone in fact, one device, the class is still the client's and the gap is
reported (§8).

## 2. The API

**Decision: the class is stored and answered exactly as S3 does** (28 §2.3–§2.9, D1). mantle
answered every object as STANDARD, refused lifecycle transitions `NotImplemented`, served neither
`restore` nor `intelligent-tiering`, and read no `x-amz-storage-class` on a write (28 §2.9); each
changes:

- **Set** by `x-amz-storage-class` on PutObject, POST, CopyObject and CreateMultipartUpload,
  STANDARD when absent; a copy without the header is STANDARD, not its source's class, and
  CopyObject is how a stored object's class changes. An upload's class is its object's. A value
  S3 does not accept on a general purpose bucket (OUTPOSTS, SNOW, the FSx and AWS Backup values,
  and EXPRESS_ONEZONE outside a directory bucket) is refused with the error S3 answers, recorded
  against S3 before it is relied on (28 §2.1); `InvalidStorageClass` is S3's code for an unknown
  class on a browser POST.
- **Returned** on HEAD and GET, "for all objects except for S3 Standard storage class objects", as
  `StorageClass` in every listing and in ListParts and ListMultipartUploads, and with
  `x-amz-optional-object-attributes: RestoreStatus` as `RestoreStatus` in listings (28 §2.3).
- **Archived reads.** A GET of a GLACIER or DEEP_ARCHIVE version, or of an Intelligent-Tiering one
  in an archive tier, without a live restored copy answers `403 InvalidObjectState`, "The action is
  not valid for the object's storage class", with `StorageClass` and `AccessTier` for the
  Intelligent-Tiering case; so does using one as a CopyObject or UploadPartCopy source (28 §2.3).
  This holds even where the bytes sit on the same device as everything else: the 403 is the
  contract clients and tests depend on (28 §2.10).
- **RestoreObject** (`POST /{key}?restore`) with `Days` and a tier: `202 Accepted` for a version
  not restored, `200 OK` for one restored, which moves only its copy's expiry;
  `RestoreAlreadyInProgress` (409), `GlacierExpeditedRetrievalNotAvailable` (503) and
  `ObjectAlreadyInActiveTierError` (403); one restore at a time per object; `Days` refused for an
  Intelligent-Tiering object; a restore in progress upgraded to a faster tier; the status in
  `x-amz-restore` (`ongoing-request`, `expiry-date`) and `x-amz-restore-request-date`. The
  select-restore variant is answered as S3 now answers it, recorded first (28 §2.4).
- **Lifecycle transitions** along S3's waterfall, one way into DEEP_ARCHIVE, with the 128 KB
  default minimum and its header, the order between classes checked, and an object's tags
  evaluated again when the transition is executed (28 §2.6; s3-protocol.md §7).
- **Intelligent-Tiering**: its four configuration operations, up to 1,000 configurations a bucket,
  each with a filter and archive tiers of 90 to 730 days and 180 to 730; the tiers and their
  thresholds (§7); `x-amz-archive-status` on HEAD; and restore to the Frequent tier, after which
  the timers start again (28 §2.7).
- **REDUCED_REDUNDANCY** accepted and stored; a version lost under it answers 405, as S3 does
  (28 §2.2).
- **Directory buckets and EXPRESS_ONEZONE.** A directory bucket is a second kind of bucket with
  its own API: names ending in the zone and `--x-s3`, regional and zonal endpoints, no path
  style, `CreateSession` credentials scoped to the bucket that "expire after 5 minutes" and travel
  in `x-amz-s3session-token`, listings not in lexicographic order, `/` the only delimiter, ETags
  that are not MD5s, consecutive part numbers, no class changes, and no lifecycle transitions,
  restores or Intelligent-Tiering configuration (28 §2.8). Research note 28 places directory
  buckets at the region step, where zonal placement near compute first exists; the design serves
  their API at every step, since the API is honoured everywhere, and adds the zonal placement at
  the region step (§9). Live sessions are bounded per principal and bucket, their 5-minute expiry
  being the eviction rule the API itself states.
- **Policies** already know `s3:x-amz-storage-class` (28 §2.3). Notifications for restores and
  tier moves come with notifications.

Minimum durations and billable sizes are billing terms; they change no answer, and mantle
enforces none, exposing per object its class, when it entered it and its size, so an operator can
account as AWS does (28 §2.5). They also mean what §5 uses: a declared lifetime.

## 3. Media pools

**Decision: a media pool is a set of devices alike in what detection and measurement found, and a
class may use a pool only where the measurement meets the class's access requirement** (28 §3.2,
D5). mantle assumes no device class (CLAUDE.md §5): a pool is defined by whether its devices are
rotational, zoned, flash-backed or spun down, and by their measured read service time at the
operating depth, sequential rate and durable-write latency (measurement.md §9).

- **EXPRESS_ONEZONE** is placed only on pools whose measured read service time, at the depth
  Express runs at, stays within 10 ms at the quantile the deployment's latency objective names.
  A 24 TB disk's datasheet states 4.16 ms of rotation alone before seek and queueing, so in
  practice this admits flash; but the admission is the measurement, and a disk that measured
  within it would qualify.
- **Millisecond classes** use any online pool, disks included; S3 serves STANDARD from HDD-based
  storage nodes (28 §3.2).
- **Archived classes** use any pool, including disks spun down or powered off, as Pelican's racks
  keep "only 8% of the drives ... concurrently spinning" (28 §3.2). Tape is such a pool where a
  cell has it: written append-only with generation-numbered indexes, its reads batched and handed
  to the drive to order, and migrated before the last drive that reads its generation retires,
  since an LTO drive reads only its own generation and the one before (research/29 §6.8, item 20).
- **Spinning down is a decision for whole groups of archive data**, never a per-device idle timer
  on online pools (28 §4.3). A spin-down's energy pays after about two seconds of idleness, so
  energy never binds; the binding costs are the seconds of first-byte latency and the drive's
  start/stop budget, which each group spends from (chunk-store.md §9.3). Writes meant for a group
  that sleeps go to replicas on spinning disks and reach the cold copy in a later burst, which is
  write off-loading, as mantle's replication already provides (research/29 §5.7).

Copyset permutations are drawn per pool (research/04 §R2), so a block's chunks stay within its
class's pool and its failure-domain rule.

## 4. Durability per class

**Decision: every class but Reduced Redundancy targets 10⁻¹¹ per block-year, its failure domains
and repair rate taken from its pool and zones** (28 §3.3, D3–D4; durability.md §1, §4).

- **One-zone classes** are evaluated with the zone's loss rate set to zero, which is what AWS's
  eleven nines for them mean, and the chain with `--zones` reports beside it the loss
  probability if the zone's loss is counted, the number an operator reads to accept one-zone
  data.
- **Reduced Redundancy** takes STANDARD's target unless the operator adopts AWS's 10⁻⁴ of objects;
  AWS itself recommends against the class, and the saving is only parity (28 §3.3).
- **Any class may be given a stricter target,** and mantle then chooses the cheapest scheme
  meeting it: S3 states a design target, not a ceiling (28 §2.10).
- **Detection is part of the repair rate.** durability.md's repair covers "detection and rebuild
  of one chunk"; a latent error found on average half a scrub period after it occurs makes the
  rate at most `1/(T_scrub/2 + T_rebuild)`, and on spun-down media both detection and rebuild wait
  for the group to spin up. Ford found repair past one failure "dominated by detection and trigger
  time". A colder pool therefore needs more parity, not less, to meet the same target: Pelican
  chose 15+3 for its spun-down racks (28 §1, finding 3). The repair rate a pool's chain uses is
  one over its measured detection time plus its rebuild time, the detection time from the pool's
  measured device-loss detection and its scrub period (chunk-store.md §9.1).
- **Express** stores copies, chosen by the durability model among the copy schemes: a copy is read
  from one chunk with no decode, which is the class's access requirement (28 §3.4).

A cold pool whose measured rates call for a code wider than the codes `mantle-ec` tests for every
loss they tolerate needs that code added to the tested set before it is used (28 §3.3).

## 5. Write plans

**Decision: the acknowledgement rule never changes with the class; the class decides where a write
goes, which stream it joins and how it is batched** (28 §3.4, D6). A write is answered once it is
durable on every copy its scheme stores (CLAUDE.md §6).

| Class | Pool and scheme | Writer | Stream |
|---|---|---|---|
| EXPRESS_ONEZONE | the fastest eligible pool in one zone; copies | no wait: a lone request is flushed at once (research/11 §2.4), the class the owner describes as "faster but more resource/power consumptive"; on battery too, until thermal state Serious (chunk-store.md §4) | client |
| STANDARD; INTELLIGENT_TIERING in its frequent tiers; REDUCED_REDUNDANCY | the durability model's scheme over ≥ 3 zones where the cell spans them | the group-commit writer | client |
| STANDARD_IA, ONEZONE_IA, GLACIER_IR | the same | the same | the long-lived stream (chunk-store.md §8): the client has declared its data will live 30 or 90 days |
| GLACIER, DEEP_ARCHIVE, by PUT | first a staging placement on an online pool at the class's own target; then the cold pool in large sequential batches | the group-commit writer | long-lived |

A PUT into an archived class does not wait for a group to spin up, which would make its latency
the spin-up's: it is acknowledged once durable at its class's target on online media, and the
move to the cold pool is a background job (§7), Pergamum's deferred write made durable (28 §3.4).
Writes to the cold pool are large and sequential, which drive-managed SMR needs ("sequential
writes of at least 8 MiB in size are streamed") and host-managed zones require.

## 6. Read plans and restores

- **Online classes** read as every object does: hedged and degraded reads within the
  reconstruction budget, avoiding devices flagged slow or throttled (node.md §5.4).
- **Archived classes** answer GET `InvalidObjectState` unless a restored copy is live (§2).
- **A restore** (28 §3.5, D7) reads the object from its cold pool and writes a temporary copy to an
  online pool under the STANDARD plan; AWS bills that copy at STANDARD's rate, which says what kind
  of copy it is. The version's restore record gains its state, request date and completion, and a
  GET serves the copy. The copy expires at its completion plus `Days`, rounded up to the next
  00:00 UTC; a repeated restore moves the expiry from the current time; a lifecycle expiration of
  the object removes the copy with it. At expiry the copy's blocks are released and reclaimed by
  the collector's ordinary lazy deletion (metadata.md §2), so the eviction rule is the API's own:
  a copy lives exactly as long as its client asked. On online media a restore completes in the
  time of a copy, which S3's "typically" permits (28 §2.10); mantle bills nothing, so no
  minimum-duration penalty arises.
- **Restores are admitted, and bounded** (28 §3.5, D8). Each cold pool keeps one bounded queue per
  restore tier, and the online capacity restored copies may hold is a share of the online pool's
  free-space runway. Both bounds are derived from measured restore throughput so that a queued
  job completes within its tier's documented time: Expedited in 1–5 minutes for objects under
  250 MB, Standard in 3–5 hours, Bulk in 5–12 for GLACIER; DEEP_ARCHIVE in 12 and 48 hours (28
  §2.4). An Expedited request past its budget is `GlacierExpeditedRetrievalNotAvailable` (503), the
  error S3 defines for exactly that; a Standard or Bulk request past its queue is refused with S3's
  retryable 503; a request rate past the measured capacity is throttled as S3 throttles restores.
  Bulk work is batched by spin-up group, as Pelican batches "sets of operations for the same group
  to amortize the group spin up latency", with the bound on how far a request may be overtaken
  taken from its tier's documented time rather than chosen (28 §3.5). The three tiers are the cold
  pool's three admission classes (node.md §2.7).

## 7. Moves between classes, and Intelligent-Tiering

**Decision: a move is background work, rate-capped and spread to a deadline that capacity sets**
(28 §3.6, D9).

- **Copy, switch, release.** A move copies the blocks to the new placement, switches the
  placement record by compare-and-swap, and releases the old blocks to the collector; a crash
  between steps leaves the old placement or both referenced, never neither (research/22 §9).
- **Spread and capped.** Lifecycle due dates cluster: every object created on one day reaches its
  30-day rule on one later day, at agent scale a burst of billions. Unthrottled redundancy changes
  once took all of a cluster's I/O for weeks, and PACEMAKER held them at or below 5% by starting
  early under a peak cap (research/10 §7.5). The move scheduler spreads each day's due moves over
  the time to their deadline at the rate the background budget allows, in its own admission class
  beneath foreground (node.md §2.7).
- **A deadline from capacity.** A move off a hot pool frees hot capacity; its deadline is when that
  pool's free-space runway, the cleaner's rule applied to the pool (chunk-store.md §8), would
  otherwise run out. A move that frees nothing the pool needs waits for idle time and mains power
  indefinitely, and the backlog is reported (node.md §1.8).

**Intelligent-Tiering is tracked at one write per object per day of access** (28 §3.6, D10). The
API counts its thresholds in "consecutive days", so a day is the resolution it needs. A version
of 128 KB or more keeps its last-access day, written only when an access falls on a later day
than the one stored, by exactly the operations AWS lists as access: GetObject, PutObject,
RestoreObject, CompleteMultipartUpload, CopyObject and UploadPartCopy of a source, but not
HeadObject, tagging or listing; SelectObjectContent resets the archive timers without tiering up.
An agent that reads one object a million times in a day costs one metadata write. Smaller objects
stay in the frequent tier and are not tracked, as S3 states. A tier change is a class field
change, and where a colder medium exists, a move; on one device it is metadata alone.

## 8. Classes on one device, and what is reported

A laptop is a region of one cell of one node, and every class's API holds there (28 §3.7, D17):

| Holds on one device | Does not |
|---|---|
| every answer of §2: classes stored and reported, archived objects refusing GET until restored, restore expiry, Intelligent-Tiering tiers, lifecycle transitions | zones: every class is one-zone in fact |
| the write plans' batching and streams (§5) | separate pools: Express and Deep Archive share the device |
| restores, complete as soon as their copy is written | a cold pool; nothing spins down |
| latent-error protection, where a block's scheme stripes it across segments of the device | the device's loss: no scheme on one device survives it |

**What placement achieves is reported, never put in S3's answers, which have no field for it.**
The durability model, run with the domains actually present, gives each class's achieved annual
loss bound; on one device it is bounded below by the device's own failure rate, 2.7% a year for
flash at the field default (durability.md §5). mantle reports per class the target, the achieved
bound and the reason (one device, one zone), in `mantle status` and at startup and whenever
placement changes. Whether to refuse archive or multi-zone classes on such a deployment is the
operator's choice; the default accepts and reports, because classes are honoured in the API at
every step.

What a class still changes on a laptop: Express runs the no-wait writer; classes with a minimum
duration take the long-lived stream; archive classes are the first background moves deferred on
battery; restored copies expire; Intelligent-Tiering moves are metadata writes.

## 9. By step

| Step | What it adds |
|---|---|
| Laptop | the whole API of §2 on one device; the class's writer rule and stream; restores as copies; the achieved bound reported per class |
| Node | media pools from detection and measurement, and each class's eligibility (§3); moves and restore staging between pools; device failure domains, so the achieved bound counts device loss |
| Cell | rack and host domains; copysets per pool; cold pools with spin-down groups, Pelican-style batching of restores and archive writes, and their start/stop budgets; tape where present; restore admission bounds; move caps under the cell's background budget; failure rates learned per device model in place of priors |
| Region | zones: ≥ 3-zone and one-zone classes become physically distinct, and one-zone exposure is reported; EXPRESS_ONEZONE directory buckets placed in the zone near their compute |
| Fleet | lifecycle due dates of billions of objects spread across cells' budgets; restore rates per principal from measured capacity |

## 10. Testing

- **API conformance.** S3's answers recorded, as research/13 and 19 recorded others, for every
  class value on PutObject, POST, CopyObject and CreateMultipartUpload; RestoreObject on STANDARD,
  GLACIER_IR, a restore in progress, a restored object, an Intelligent-Tiering object with and
  without `Days`; GETs and UploadPartCopy of archived sources; and S3's 94-day lifecycle example
  (28 §8). Replayed against mantle. s3-tests' `storage_class`, `lifecycle_transition` and
  `restore` markers enabled, which need at least two classes advertised.
- **A controlled clock** for restore expiry, rounding to 00:00 UTC, re-restore and a lifecycle
  expiration overriding it; Intelligent-Tiering thresholds at 30, 90 and the configured archive
  days; the 128 KB rules.
- **Crash consistency of moves.** The chunk store's crash tests extended to copy, switch and
  release: after recovery every object is readable at exactly one class, all its blocks referenced
  once.
- **Simulation** with lifecycle bursts, restore floods past their bounds (typed refusals, no
  unbounded queue) and power changes mid-batch (no acknowledgement before durability).
- **Benchmarks with baselines**: Express put and get latency against STANDARD's; restore throughput
  per tier; metadata writes per GET under repeated access (one per object-day); cleaner write
  amplification with and without the lifetime hint; energy per byte on battery and mains.

## 11. Open

- S3's answers for SNOW, the FSx and Backup values and EXPRESS_ONEZONE on a general purpose
  bucket; for RestoreObject of a never-archived object; and for the select-restore variant
  (28 §9 items 1–3).
- What HEAD reports between a lifecycle rule's date and the physical transition (28 §9 item 2);
  §1's separation answers by the class committed.
- Detection times per pool, spun-down groups above all, which set an archive class's code
  (28 §9 item 13), and whether a code wider than the tested set is needed.
- How much the lifetime hint saves the cleaner on real class-labelled traces (28 §9 item 15).
- The real distribution of lifecycle due dates and restore requests at agent scale, which sets how
  far moves must spread and how large restore staging must be (28 §9 item 16).
- Lifetime classes outnumbering a drive's placement handles (research/29 §11 item 16): which
  share one.
