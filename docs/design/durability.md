# Durability: the scheme a block is stored in

Status: design, 2026-09-29; error bounds and field inputs 2026-09-30; classes, detection and
flush-unverified copies 2026-09-30; a metadata write's answer (§7) 2026-11-17. Sources:
docs/research/15 (durability models, cited as "15 §x"), docs/research/04 (erasure coding and
placement, "04 §x"), docs/research/28 (storage classes, "28 §x"), 29 (device classes, "29 §x")
and 95 (protocol-aware recovery, §7).

A block is stored as whole copies or as a Reed–Solomon code, one chunk to each failure
domain (04 §R1.2, §R2). The scheme a block uses follows from how likely each scheme is to
lose it, computed from the rates at which chunks are lost and repaired, rather than looked
up in a table. The model is `crates/ec/src/durability.rs`, and `mantle durability` runs it.

## 1. The target

**Decision: a block's annual probability of loss is at most 10⁻¹¹ by default.** This is S3's
design, "99.999999999% durability … of objects over a given year" (15 §5), applied to each
block. By the union bound, an object of B blocks is lost with probability at most B times
that (15 §6).

The per-block probability is marginal, so a chain that includes correlated events gives it
exactly (15 §6). It also bounds the expected annual fraction of data lost (15 §3), and it
meets the objections Greenan et al. raise to MTTDL (15 §2): it covers a stated year, not an
infinite horizon, and it compares systems of different sizes.

What it does not measure is how losses cluster across blocks. The copyset placement of
04 §R2 bounds that separately: the chance that an event loses any block at all (04 §A6.3).

**Per class.** Every S3 storage class but Reduced Redundancy is designed for the same eleven
nines, archive included (28 §1, finding 1), so the target is the same; a class changes the
failure domains a block may use (one zone, or several), the pool whose rates it is evaluated with,
and the pool's repair rate (§5). One-zone classes are evaluated with the zone's loss rate at
zero, as AWS's figure for them means, and their exposure to the zone reported beside it; Reduced
Redundancy keeps this target unless the operator adopts AWS's 10⁻⁴ of objects; and any class may
be given a stricter one (storage-classes.md §4).

## 2. The model

A stripe is a continuous-time Markov chain over where its surviving chunks are, in the manner
of Ford et al. (15 §1). Ford's chain counts chunks unavailable for fifteen minutes or more;
this one counts chunks lost for good, which is durability rather than availability (15 §1.1).
Its chunks are placed over the failure domains as evenly as they go, and a state is how many
domains hold each count of them: a chunk lost is lost from its own domain, and the domains
are alike, so which domain holds what does not matter, only how many hold each count. A
stripe loses chunks four ways:

- **One at a time,** each chunk at its device's failure rate.
- **A whole failure domain at a time,** each domain at its own rate, taking the chunks it
  holds then. A rack holding one chunk is one more way to lose a chunk; a zone holding three
  of RS(6,3)'s nine takes three at once, and after it the zones still holding chunks are two.
- **Several across domains at once:** events that destroy each chunk with some probability,
  so the chunks struck in each domain are binomial. Cidon et al.'s power outage is one, after
  which 1% of the nodes do not come back (04 §A6.1).
- **Repair, one chunk at a time,** at one rate covering detection and rebuild, each chunk
  rebuilt in a domain holding the fewest of the stripe's chunks, which restores the even
  placement; a lost domain is replaced and takes chunks again. This is Ford's serial repair,
  chosen "to gain more conservative estimates" (15 §1.4).

The stripe is lost once fewer chunks remain than it needs. With no more chunks than domains,
every domain holds one chunk or none, and the chain is Ford's over how many are lost. With
more, the chain once spread a degraded stripe's survivors evenly again, which the audit
showed wrong for a real placement (audit B09): RS(6,3), three chunks to each of three zones
lost at λ and nothing repaired, is lost at 5/(6λ) on average, where spreading the six
survivors gave 2/(3λ). The tests check that case exactly, and check the chain over counts
against one over labelled domains built by the same rules.

The annual loss probability is the chain's transient: the whole state's entry for loss in
e^(Qt), for t a year. It was once taken as 1 − e^(−t/M) for M the mean time to loss, where
loss is rare against repair (Keilson; 15 §4.5), but that law is an approximation of stated
order, not a bound, and without repair it is far off: in the zone case above it puts the loss
within t at 1.2λt against an exact 3λ²t² when λt is small, and below the exact value past
about 1.25 M. The probability is now computed as an enclosure (§3).

## 3. Solving it

The chain's rates span ten or more orders of magnitude. Gaussian elimination cancels the
tiny absorption rates against the large repair rates: it errs by 78% on RS(9,6) and loses
every digit on RS(12,8) (15 §4.1).

The chain is solved instead by eliminating states from its jump chain, Kohlas's method as
Hunter states it (15 §4.3). Degraded states are eliminated first, and the whole state is kept
for last. Each step adds, multiplies and divides non-negative numbers, and a state's chance
of leaving is the sum of its exits, never one minus its self-loop.

The result matches Ford's closed form for independent failures (15 §1.7) to 10⁻¹². It does
so for one to five copies and for RS(2,1) to RS(9,6), at ratios of repair to failure from
one to 10¹².

**Decision: the loss probability is an enclosure, and the upper end is what is reported and
compared with the target (audit B09b).** The transient is computed by scaling and squaring
the uniformized series, every term non-negative, every operation rounded outward, the
series' tail added to the upper end, and e^(−Λτ) enclosed as one over the weights' sum
rather than by a library exponential whose precision the standard library leaves unstated.
The exact probability of the chain lies between the two ends whatever the rates (15 §4.6
proves it). The ends are within 2×10⁻¹³ of each other without repair, 1.6×10⁻⁹ with
one-hour repair over a year, and grow in proportion to the repairs in the year: 8.4×10⁻⁴ at
3.6 ms. A scheme is never chosen on rounding in its favour.

The tests hold the enclosure to closed forms: the binomial tail without repair, from a
billionth of a lifetime to ten; the two-copy chain with repair; the zone case above; and a
bound proven for every chain, P(T ≤ t) ≤ q₀·p·t for q₀ the whole state's exit rate and p the
chance a stay in it ends in loss (15 §4.6). Against the enclosure the exponential law was
high by at most 7×10⁻⁴ at one-hour repair, so the results of §6 did not move in their two
figures.

Solving the chain exactly does not make the chain the fleet. What it leaves out is §5's.

## 4. The choice

**Decision: the cheapest scheme that meets the target.** The candidates are the schemes
mantle stores: one to three copies, and the codes `mantle-ec` tests for every loss they
tolerate, RS(2,1), RS(3,2), RS(4,2), RS(6,3), RS(8,4), RS(10,4) and RS(9,6). A candidate
must be no wider than the failure domains. Among those within the target, the one of least
overhead wins; of two that cost the same, the narrower wins, since its repair reads fewer
chunks.

When no candidate meets the target, the closest is reported with its loss probability. That
answer says the deployment needs more failure domains, zones or cells, not a different
code.

## 5. The rates

The rates are inputs. They come from mantle's own measurements once device health records
them (docs/research/10). Until then, the field data of 15 §5 and §8 gives the defaults, each
the conservative end of its source, in `mantle_ec::durability::field` (audit B09a):

| Input | Default | Why this value | Source |
|---|---|---|---|
| Disk annual failure rate | 6.3% | The worst model in Backblaze's 2025 fleet, 4.6 times its 1.36% mean; a stripe's chunks can share a model and batch. Schroeder and Gibson's "2-4% common" and up to 13% bracket it | 15 §8.1 |
| Flash annual failure rate | 2.7% | The worst four-year replacement fraction of Schroeder et al. (FAST 2016), 10.31%, as a constant hazard; above Maneas et al.'s worst model, 1.2% | 15 §8.2 |
| Power loss | once a year, destroying 1% of nodes | HDFS: "one-half to one percent of the nodes will not survive a full power-on restart"; the once-a-year is Cidon's, UNVERIFIED at his cited source | 15 §8.4 |
| Rack, zone or site loss | none | No fetched source gives a permanent-loss rate; Ford's and Dean's rack events are transient. A deployment states its own (`--zones`) | 15 §8.4 |
| Repair | one over the mean time to detect a loss plus the time to rebuild one chunk, per pool; a day unless the operator states the cell's (`durability.repair_rate`) | measured repair bandwidth and detection time; Ford's mean exceeds 20 minutes. The cell's repair holds itself to it: an away node's chunks wait one over it less the measured rebuild time (repair.md §2.3) | 04 §R3, 15 §8.4, 28 §3.3 |

**Detection is part of repair** (28 §3.3, D4). A device's loss is detected in seconds on online
media; a latent sector error only when the scrubber reads it, on average half a scrub period
after it occurs, so the repair rate of a pool is at most `1/(T_scrub/2 + T_rebuild)` for latent
errors, and on spun-down media both wait for the group to spin up. The chain therefore takes
latent errors as a loss of their own, at the rate measured on the pool's devices (research/10),
repaired at that rate. The scrub period is the device's (chunk-store.md §9.1): the longest
period at which the blocks it holds meet the target, never shorter than the device's workload and
bandwidth bounds; where those bounds exceed what the target allows, the pool needs a wider
scheme, and §4 chooses one, never a rating exceeded.

**A copy on a device whose flush mantle cannot verify is weaker** (29 §3.8, §6.2, D4): a USB
device, a cloud volume under ReadWrite host caching, or a drive whose volatile-memory backup has
failed. Thirteen of fifteen SSDs lost flushed data when their power was cut (29 §3.8). Placement
counts such a chunk toward a block's scheme only with the device's loss rate raised by its power
losses, the yearly event of the table above taken as losing the chunk, and reports the device as
flush-unverified.

**A copy counts once its own bytes are flushed, whatever shares the flush.** The model's copies
are chunks and range entries that are durable: a chunk's volume answers it only after the flush
that covers it, and a range answers a command only once the entry carrying it is durable on a
quorum and applied (CLAUDE.md §6). Since step 9 of the doors plan a volume's flush covers every
chunk the cell's operations wrote it meanwhile, and a range's entry carries every command waiting
that fits its bytes, an append out to each follower for each entry a range can hold uncommitted (node.md §1.4, §2.8; `derive::in_flight`;
research/71, step 9; research/87): more writes share each flush, and none is answered
before the flush that holds its own bytes, which a simulated cell cut from power at any moment
around a batched entry's answer checks, and a cell whose flushes are held checks directly: a
batched entry is never answered while the nodes left to flush it are one short of a quorum, nor
while its leader's own flush is held after its followers' quorum committed and applied it, and
it is answered whole once they end (`crates/node/tests/sim.rs`,
`a_batched_entry_is_answered_only_after_the_flushes_it_needs_and_never_while_they_are_held`).
Group commit changes how many flushes a copy shares, not when it counts.

What the constant-rate chain leaves out, and where the sources show which way it errs
(15 §8.5):

- Failures cluster in time: two disks within an hour four times likelier than the
  exponential, a second RAID-group replacement within a week 180 times likelier, shelf
  failures 6 to 25 times likelier in pairs. The chain is optimistic for a second loss during
  a rebuild; the burst events stand in for part of it. A stripe's chunks go to distinct
  domains, so an enclosure named as a domain removes the shelf's share.
- Hazard rises with age. The worst-model rate is pessimistic for a young fleet and may be
  optimistic for one past its fourth year.
- Latent sector errors found during a rebuild, and controller, cable and path failures, have
  no state: optimistic unless the chunk rate is measured over the whole path, as
  docs/research/10's device health will.
- Repair is exponential and serial; after a burst it queues. Optimistic after bursts.
- The burst rate and size distribution are unpublished, and no site-disaster rate exists.
  These are what `--burst` and `--zones` take from the deployment.

## 6. What the model says

These are the upper ends of the enclosures, one-hour repair (the test
`the_design_tables_choices_hold` holds the field-default rows):

| Case | Domains | Choice | Annual loss at most |
|---|---|---|---|
| 4% annual failures, independent | 12 | RS(6,3) | 1.1×10⁻¹⁴ |
| The same, with a yearly outage losing 1% of nodes (`--burst 1:1`) | 12 | none within 10⁻¹¹; RS(8,4) comes closest | 7.5×10⁻⁸ |
| The same outage, target 10⁻⁹ | 15 | RS(9,6) | 6.1×10⁻¹¹ |
| Three zones, each lost once a century (`--zones 3:0.01`) | 12 | R3, one copy a zone | 9.8×10⁻¹² |
| Field defaults: 6.3% disks, independent | 12 | RS(6,3) | 7.1×10⁻¹⁴ |
| Field defaults with the yearly power loss | 12 | none within 10⁻¹¹; RS(8,4) comes closest | 7.5×10⁻⁸ |
| The same, target 10⁻⁹ | 15 | RS(9,6) | 6.1×10⁻¹¹ |

In the zone case, RS(6,3) falls to 1.8×10⁻⁶: every zone holds three of its chunks.

Correlated loss dominates, as Ford found for availability (04 §A5). Inside one cell exposed
to cluster-wide events, eleven nines a block needs wider parity than the named profiles,
placement across zones, or copies in more than one cell (Ford's multi-cell results, 04 §A5).
Where a cell spans zones, a code that loses as many chunks with a zone as it can spare has no
margin left after one zone's loss.

## 7. A metadata write's answer: its log frame's own flush

Status: design, 2026-11-17, the owner's decision 2 of research/71 ("Step 9, after"), approved
that day. Source: research/97 (AGL+18, cited by section and page). Built: the open's account of
the frame past the last that reads (§7.7, step 1), the record in pieces (step 2), and the record's
word on how its frame was answered, with the open's restore of a frame answered at its flush
(step 3a) and the writer's setting of it (step 3c), the device's write cache (step 5), and step 4:
each log says how it answers and why, its mark refuses the binaries before answers at the flush,
and the writer answers a frame at its flush where the owner's two decisions allow (§7.5,
hyper-log `AtFlush`). Neither decision is taken, so on every device a frame is still answered
only once confirmed, as raft-log.md §6 describes.

A PUT's critical path holds seven flushes: the slowest replica's chunk, then the Block, File and
Name commands, each a frame flushed and then confirmed by a second durable write (research/71,
step 9). The confirmation exists so that recovery can tell a frame torn by a crash from one
damaged after it was answered (raft-log.md §6, step 5). This section drops it: three of the
seven flushes leave the path, and recovery does what protocol-aware recovery does with an entry
it cannot place, asking the group.

### 7.1 The rule

A frame's updates are answered at the frame's own flush. A Raft entry is acknowledged by a
member once the frame holding it is flushed there, and a command is answered once it is
committed on a quorum so acknowledged and applied (CLAUDE.md §6: `fdatasync` on Linux,
`F_FULLFSYNC` on macOS, `FlushFileBuffers` on Windows). The frame and its persist record are both
written before that flush, so the flush that answers the frame made both durable. No later write
is waited for. A frame is still written only after the frame before it is flushed, so a frame,
or a record, with a later sequence proves every earlier frame flushed (raft-log.md §3).

Where the frame holds a group the others could not resolve without (§7.5), it is answered once
confirmed, as now.

### 7.2 What recovery does with the frame past the last that reads

Call `N` the frame after the last valid one (raft-log.md §6, step 2). Damage in the middle, a
frame that fails while a later frame or record follows, is unchanged: the frame flushed and was
answered, the log reports it, writes nothing, and its replicas recover from their peers (R17;
AGL+18 §3.3.3: "CLSTORE can still conclude that it is a corruption if ei+1 or pi+1 is present").
For `N` itself:

| what reads | what it means | what recovery does | `hyper_log::Tail` |
|---|---|---|---|
| no record of `N` | `N`'s flush never ended, so it was never answered ("If pi is not present, CLSTORE can conclude that it is a crash", §3.3.3, p. 20) | the torn tail: cut | `Torn` |
| `N`'s record and its confirmation (frames answered once confirmed) | answered, and damaged since | restored and marked, as now | `Lost` |
| `N`'s record, no confirmation | torn by a crash after its record landed, or damaged after its flush answered it: "if ei is the last entry, then we cannot determine whether it was a crash or a corruption" (§3.3.3, p. 20) | while frames wait for their confirmation, never answered: the torn tail, its term and vote kept, as now. Answered at its flush: restored as `Lost` is, and the group decides (§7.3) | `Undetermined` |
| some pieces of `N`'s record, each whole, the rest whole pieces of an older record | `N`'s flush never ended | the torn tail; each group a landed piece names keeps a later term and vote, which is always safe | `Torn` |
| a piece of `N`'s record that fails its own checksum, `N` failing too | damage at rest: a piece is written atomically, so no crash tears it. Item and identifier are both faulty (§3.3.4, p. 21) | `LogError::Damaged`: the log writes nothing, and its replicas recover from their peers | |

Restored as `Lost` is: each group the record names gets back the term, vote and promise the
frame left (keeping any of them is always safe), its entries cut back to where the frame's began,
its commit cut to the entries left, and an `Uncertain` mark through the frame's last entry; a
group whose frame held proposals, which the record does not carry, is fenced `Damaged` and rebuilt
from its peers (raft-log.md §6). The restore is written in one frame before the log serves anyone,
as now, so a second crash finds the same marks.

The record in pieces is the one format change: format 5, and 6 in a sealed log, whose whole records
are format 4 (hyper-log `FORMAT_PIECES`, `FORMAT_SEALED_PIECES`). AGL+18's persist record "can be
atomically written and thus cannot be 'corrupted' due to a crash" (§3.3.3, p. 20); hyper-log's names
every group its frame holds, and five groups span two 512-byte sectors (raft-log.md §6), so a crash
can leave it part old and part new, and a record that no longer reads would not say which groups to
mark. Format 5 writes a record as pieces no larger than the device's atomic write unit, each
carrying the log's identity, the frame's sequence, its own index and the count, and a checksum of
its own; each also carries what the record confirms, the checksum of all the record's rows and the
unit it was laid in, so that pieces of two writes of one sequence, a confirmation over its record or
a restore over a lost frame's, never read as one record, and a slot is never read in another unit
than its own. The unit is read from the device, not assumed (CLAUDE.md §5): Linux's
`physical_block_size`, "the smallest unit a physical storage device can write atomically", and
`atomic_write_unit_min_bytes` where present and no larger than the physical block, so that the unit
holds for a write that asks for atomicity and one that does not (research/02 §2.4); the "Physical
Block Size" the storage device publishes in IOKit's registry on macOS, read without opening the
device, whose node is root's (research/02 §3.5); and `FILE_STORAGE_INFO`'s
`PhysicalBytesPerSectorForAtomicity` on Windows, which GetFileInformationByHandleEx gives for any
handle without privilege, where `IOCTL_STORAGE_QUERY_PROPERTY` asks a volume handle and may ask
privilege (research/02 §4.4). A device that reports none gets its logical block, and one that
reports neither 512 bytes, Linux's own rule for such a device (block/blk-settings.c,
`blk_validate_limits`): the conservative side, since a piece no larger than the true unit, at an
offset it divides, lies within one unit.

### 7.3 How the group resolves an undetermined entry

The mark is CTRL's question asked through Raft's own messages, and replica.md §4 builds it
(hyper-raft core steps R-5 and R-7):

- **No vote on an undetermined log.** A marked member judges a request for its vote against the
  last entry it acknowledged, its claim, and refuses a candidate behind it; it campaigns only on
  a quorum of each half of its configuration without its own vote. A grant is CTRL's
  `dontHave`, an unmarked member's refusal `have`, a marked member's `haveFaulty` (§3.4.3, p. 22).
- **Nothing served from it.** Its commit was cut to the entries it holds, so it applies nothing
  marked; marked entries are never skipped and applied later (§3.4.1, p. 21). It leads only once
  elected on its log, and then its mark ends.
- **Fetched from a peer that has it.** Marked, it answers a heartbeat or append that counts
  entries past its log with a refusal flagged lost, naming the last entry it holds; the leader
  sends what it lacks (R-5). Once its log reaches the mark's index it holds the leader's entries
  there, and the mark ends (Log Matching; Ongaro and Ousterhout, ATC 2014, §5.3). CTRL's Case 1.
- **Discarded only by a quorum.** A leader elected without the marked member's vote holds every
  committed entry (Leader Completeness, §5.4). Its first entry of its own term reaching the member
  ends the mark, and the member's marked entries past the leader's log are overwritten, never
  having been committed (§3.4, p. 21: "If the leader does not have an entry that the follower
  has, then the entry must be an uncommitted entry"). CTRL's Case 2, with its exact count: a
  quorum, ⌊N/2⌋ + 1, never fewer (§5.1.1, p. 26).
- **Otherwise it waits.** Where every reachable copy is marked and the rest are no quorum, the
  group waits as CTRL's does (Case 3; §3.4.2, p. 22). Each campaign is one election round on the
  election timer, refused with the typed `Uncertain`, and the range reports it to repair
  (replica.md §4). No member decides alone.

Every step is bounded: the restore is one frame, the repair is the lost range sent in appends of
the range's bounded size, and each election round is one timer's.

### 7.4 Why no answered write is lost, and no unanswered one answered

*Answered means kept.* An answered command's entry was flushed on every member of a quorum `Q`,
each in a frame whose record the same flush made durable. After any crash, on each member of `Q`
that frame reads whole and replays; or fails with a later frame after it and is restored and
marked (`Lost`); or fails as the last frame with its record whole and is restored and marked
(`Undetermined`); or fails with a record piece failing, and the log refuses to open
(`LogError::Damaged`). It is never `Torn`: that needs a record that never landed, and the flush
that answered it landed it. So every member of `Q` holds the entry, or is marked through it and
refuses every candidate behind it. A candidate without the entry needs a quorum's votes, every
quorum meets `Q`, and each member of `Q` in it refuses: by Raft's up-to-date check where it holds
the entry (§5.4.1), by its claim where it is marked. Every leader therefore holds every answered
entry, and a mark ends only by a leader's entries. A device that loses a flushed frame whole, a
lost write, leaves nothing to read under this rule as under the confirmation (raft-log.md §6); the
rule neither opens nor closes that case.

*Unanswered stays unanswered.* An answer goes only from the gateway waiting on its command, in
the process that proposed it, after the command commits and applies. A crash ends that wait and
nothing answers for it later. The entry itself may still commit after the crash, from a member
that holds it whole, as Raft lets any entry not yet known committed (Ongaro, 2014, §6.3); its
client, unanswered, retries, and the session answers the retry once. A marked entry is applied
only once repaired from a whole copy, so nothing applied after a crash rests on a frame that
might have torn.

### 7.5 Where the confirmation stays

CTRL's wait (§7.3) needs every copy of an entry faulty, which the paper calls unlikely: it takes
independent corruptions. Answered at the frame's own flush, a crash alone reaches it. A power cut
that tears the same entry's frame after its record landed leaves that copy marked; on members the
others are no quorum without, the group waits for good, with no byte damaged and nothing ever
answered. Two cases reach it:

- *A group of one or two voters.* The others are never a quorum without the member (replica.md
  §4), so one torn frame suffices. A cell of one node, a laptop's, is all such groups. Frames
  holding such a group stay answered once confirmed; the replica says so with each update it
  hands the log, from its configuration.
- *A power cut across a quorum of a group.* A cell whose nodes share one feed loses them at once
  (node.md §9.1's "every node's power cut at once"). Each group whose entry was in flight on a
  quorum, its record landed and its frame torn, waits. Two device facts close it without the
  confirmation, each read from the device (CLAUDE.md §5): a device with no volatile write cache
  (Linux `write_cache` "write through", research/02 §2.4) makes a completed write durable, so a
  record written only after its frame's write completes proves the frame landed, at the cost of
  one write's latency, not a flush; and a device whose atomic write unit holds a frame and its
  record (`RWF_ATOMIC`, research/02 §2.10) cannot tear one. On any other device the simulation's
  power cuts count the groups left waiting (§7.6), and the owner decides from that count whether
  frames there are answered at their flush or once confirmed. The owner decided on 2026-11-20,
  from the count below (one group of eight left waiting under fault (b)) and with nothing yet to
  free a group left so, that they stay confirmed, forgoing the 18% more native PUTs a second
  answering at the flush measured on the Mac Studio, whose three nodes share one power feed.

The OS's word on the cache is not enough on its own (CLAUDE.md §5). Linux's `write_cache` is the
kernel's view: "Writing to this file can change the kernels view of the device, but it doesn't
alter the device state", and an administrator who sets "write through" over a volatile cache
"will also eliminate cache flushes issued by the kernel" (sysfs-block, research/02 §2.4), since
for a queue without a volatile cache "the block layer completes empty REQ_PREFLUSH requests before
entering the driver and strips off the REQ_PREFLUSH and REQ_FUA bits" (writeback_cache_control.rst,
research/02 §2.9). So the measurement that would check the claim cannot be a write and flush timed
against a write alone: on a queue marked write-through the flush never reaches the device, and costs
nothing whether the device has no volatile cache or the attribute was set over one, agreeing with
the attribute either way. Which measurement can check it is open (§7.7, step 4); until one is cited
and built, a device the OS calls write-through or power-protected answers once confirmed, and says
so (`Answers::Unmeasured`).

The count, 2026-11-18 (`crates/node/tests/sim.rs`,
`a_frame_torn_after_its_record_lands_is_marked_and_each_group_resolves_or_waits_reported`), over
the power-cut tests' seeds 1-8: each cell size, three nodes on odd seeds and five on even, on each
of the three profiles of paths, then seeds 1 and 2 again; (c) runs a cell of one node, a laptop's,
on each seed's profile. Each run tears the frame of the range's next entry on the members its seed
draws: the frame's write lost, every sector of it (hyper-block's simulated lost write, the tear
that leaves least), its persist record's kept, the flush after them cut. The open finds the record
and not the frame, `Tail::Undetermined { at_flush: true }`, and marks the range through the entry.
A tear on a quorum cuts every other node's power once the last torn member stops, each of their
unflushed sectors kept or lost (`Crash::Random`).

| fault | seeds | groups marked | groups resolved | groups left waiting |
|---|---|---|---|---|
| (a) one member | 1-8 | 8 | 8 | 0 |
| (b) a quorum of members, the others' power cut with them | 1-8 | 8 | 7 | 1 |
| (c) every member of a one-voter group | 1-8 | 0 | 0 | 0 |

The group left waiting is seed 5's, three nodes across the planet: nodes 2 and 3 torn and marked
through entry 5, and node 1, its power cut with them, holding entries through 4: no copy whole, and
one member lacking the entry, short of the quorum of two. Each marked member reported `Uncertain`,
and for the cell's quiet period nothing past the answered entries was applied or answered. Every
other group resolved: in (a) the others are a quorum without the torn member; in (b) a member
outside the torn quorum held the entry whole, and the marks ended by it. In (c) no group marked:
the one voter's record says its frame was not answered at its flush (`Write::at_flush` needs three
voters or more), so on every seed the open found `Tail::Undetermined { at_flush: false }` and kept
the frame as the torn tail, as a frame answered once confirmed and never confirmed was never
answered; the node elected itself again and applied through every entry it answered.

The owner's two decisions are stated where the writer reads them, each a typed enum of hyper-log's
`AtFlush`, and `AtFlush::DECIDED` is the one place either changes (§7.7, step 4b):

| decision | what it decides | variants, and how a log then answers | today |
|---|---|---|---|
| (A) `write_back: WriteBackFrames` | whether a device with a volatile write cache, or one that states none, answers a frame at its flush, accepting fault (b): a group left waiting after a power cut across a quorum, reported `Uncertain`, never an answered write lost (§7.4) | `Confirmed` (`Answers::WriteBack`); `AtFlush` (`Answers::Accepted`) | `Confirmed` |
| (B) `stated: StatedCache` | whether a cache the OS states keeps a completed write, write-through or power-protected, is trusted, which no timing of a write and a flush checks | `Unmeasured` (`Answers::Unmeasured`); `Trusted` (`Answers::Kept` under condition (a), `Answers::Buffered` where the file's completed writes are not the device's) | `Unmeasured` |

So no device answers a frame at its flush today. Under (B) a frame is answered at its flush only
under condition (a): a write the file completes is the device's own completion
(`BlockFile::writes_reach_device`: for a real file what its platform's direct path promises of the
handle as it was opened, step 4c, true for Linux's `O_DIRECT` on a block device or on ext2, ext3,
ext4 or XFS where statx(2) says the file takes direct I/O and for Windows'
`FILE_FLAG_NO_BUFFERING`, never for macOS's `F_NOCACHE` or a buffered handle; true for the
simulated device that keeps every completed write, hyper-block `SimFile::keeping`), and the frame
overwrites a slot the file holds written, so its write completes before its record's is issued and
a record that landed proves its frame did. Neither fact speaks for the device's own cache: a
completed direct write may sit in a volatile one, which is why condition (a) needs the cache the OS
states keeps it, and why decision (B) is the owner's. A frame that grows the
file stays confirmed: the file system's journal commits that write's metadata only at the next
flush (fallocate(2)). Condition (b), a frame and its record in one atomic write, is not reachable
in this log's layout: the record lies in the persist area and the frame in a segment, two ranges of
the file, where an atomic write (`RWF_ATOMIC`, research/02 §2.10) is one pwritev2(2) call, which
writes one contiguous range from its offset. Under
either decision a frame holding a one- or two-voter group stays confirmed (`Update::at_flush`).

### 7.6 What changes in the node's power-cut and torn-write tests

- hyper-log: `an_unconfirmed_torn_frame_keeps_only_its_term_and_vote` and
  `an_update_is_answered_only_once_its_frame_is_confirmed` hold for frames answered once
  confirmed, and gain twins for frames answered at their flush: the same tear restores the
  frame's groups and marks them (`Tail::Undetermined`), and the update is answered before any
  confirmation is written. `a_torn_frame_without_its_persist_record_is_the_torn_tail` holds for
  both. New: a record torn across pieces is the torn tail; a piece damaged with its frame is
  `LogError::Damaged`; a crash between two frames never marks the earlier.
- The simulation (node.md §9.1): "every node's power cut at once" keeps its law, the cell applies
  through every entry it answered, and adds two: every mark ends once a quorum of the group's
  members is up and one copy reads whole or a quorum lacks the entry, and the groups that wait are
  counted and reported with `Uncertain`, never served. The held-flush test answers an entry when
  its frames' flushes end, one write sooner. A new fault tears a frame after its record lands on
  one member, then on a quorum, then on every member of a one-voter group whose frames are
  answered once confirmed, which must never mark. Built (step 3c): the range's store tears the
  next write of entries on a member the test arms, the frame's write lost and its record's landed,
  the flush cut, so the node stops on its fenced log; the node's status says whether its range is
  marked (`Status::uncertain`). The one-voter group runs on a cell of one node, whose quiet period
  is at least the node's serve period (`mantle_protocol::bounds::idle_timeout`, how long an
  exchange may make no progress before the transport judges it stalled): a lone node states no
  patience, as it waits on no peer, and the cell's floor before, a round trip of its longest path,
  passed before a client's first exchange was answered (quiet for 800 µs on seed 1's paths of one
  switch).
- Real processes: `kill -9` never tears a completed write, so the end-to-end tests keep their
  laws; they count flushes a PUT, 4 on the critical path where 7 were (research/71's
  instruments).

### 7.7 Steps, each built and tested before the next

1. The open reports what it found past the last frame (`Recovery::tail`: `Torn`, `Undetermined`,
   `Lost`), recovery unchanged. Built (hyper-raft `mantle-frame-flush`).
2. Format 5, the record in pieces of the device's atomic unit, and the open's reading of them
   (§7.2's last two rows). 2a, built (hyper-raft `mantle-frame-flush`): the record's encoding in
   pieces and its decoding, a slot's newest record whole, torn with the rows its landed pieces
   hold, or `LogError::Damaged`. 2b-i, built (hyper-raft `mantle-frame-flush`): the device's
   atomic write unit read from the OS as §7.2 states, with whether the OS stated it or it is the
   default (hyper-block `atomic::atomic_unit`). 2b-ii, built (hyper-raft `mantle-frame-flush`):
   the writer and the open take the pieces in that unit, for an unsealed log.
   - The open and the create ask the file for its device's unit (`BlockFile::atomic_unit`), and
     report it, where it was read and whether the OS stated it, with the unit the records are laid
     in (`Recovery::laid`, `Log::laid`), so a node on the 512-byte default shows.
   - A frame's record, its confirmation rewritten in place and the copy of a lost frame's record
     before its restore are each written in pieces, one write then the platform's full flush, as
     before. Frames are still answered only once confirmed. The copy fills its slot with zeros past
     its pieces: the slot may hold the pieces of a later frame's longer record that never became
     durable, which would read as the slot's newest record, torn, and hide the copy. Each of the
     writer's records names a later frame or confirmation than its slot holds, so it reads as the
     newest without.
   - The two slots in pieces lie past the two whole slots, each at an offset the larger of the
     unit and the file's block divides, their length a multiple of it: a piece stays one unit
     while the write meets a direct file's alignment where that is larger (512 against 4,096).
     Logs of whole records (format 3, 4 sealed) keep their offsets. An area that cannot hold both
     slots keeps whole records, and a unit that holds no row is refused.
   - The area's start holds the mark of pieces: a whole persist record of the other kind than the
     log's, of no groups, its padding byte the unit's base-2 logarithm. A binary from before
     format 5 reads it as a sealed record in an unsealed log and refuses the log by that format
     check, where it would otherwise read the pieces as no record.
   - An unsealed log of whole records moves to pieces at its open: each slot in pieces written
     whole, zeros and the pieces of the record its whole slot holds, so a slot holds only zeros and
     pieces as the decoder assumes; one flush; then the mark and a flush. A crash before the mark
     is durable leaves the whole records for the next open.
   - Slots are read in the unit the mark records. A torn record is no record, the tail
     `Torn`; a damaged piece is `LogError::Damaged`. New records keep the recorded unit when the
     device's differs, since the slots lie where that unit put them: a log moved to a device of a
     larger unit stays safe, its pieces inside the device's unit; one moved to a smaller unit could
     tear a piece, which would read as damage; 2b-iii lays such an area again at the open.

   2b-iii, built (hyper-raft `mantle-frame-flush`): sealed pieces, the file's block in the mark,
   and an area laid again where its pieces would tear or its slots cannot be written.
   - A sealed log lays its records in pieces too (format 6), each piece's MAC under the log's
     authentication key over the piece through its CRC, in the room the format left after it.
     Every sealed piece whose CRC holds is checked against its MAC as its slot is read: a forged
     or altered piece, or an unsealed piece in a sealed log, is `LogError::Tampered`, a piece that
     fails its CRC damage, as in an unsealed log. A sealed log of whole records (format 4) moves to
     pieces at its open, its records' MACs checked as they are read. Its mark is the reverse of an
     unsealed log's, an unsealed record in a sealed log, which a binary before format 6 refuses as
     tampering. The mark itself carries no MAC: changing it moves where the slots are read, which
     reads as damage or as no record, as zeroing a slot of whole records always could.
   - The mark records the file's block the slots were laid by, in its confirmation field, and in
     its sequence whether they lie at the area's end. The slots' offsets depend on the block, and
     a file's block can change between runs (a direct file reopened buffered takes the device's
     preferred block); the slots are found by the recorded block wherever the file's block takes
     them, each slot's offset and length a multiple of it. A binary of format 5 from before read a
     mark only of sequence and confirmation 0, so it refuses these as a record of the other kind
     rather than lay the slots by a block of its own; its own marks read by the file's block, as
     it laid them, and the open writes them again with it.
   - Where the device's unit is smaller than the recorded one, or the file's block does not take
     the recorded slots, the open lays the area again in the device's unit and the file's block,
     at the other end of the area, as the move from whole records does: each new slot written
     whole, zeros and the pieces of the record the old slot holds, one flush, then the mark and a
     flush. The new slots lie clear of the old ones and of the mark, refused (`LogError::Config`)
     where the area does not hold both; so a crash before the mark is durable leaves the old area,
     which the next open reads and lays again, and the mark, within the device's unit, is old or
     new. There a piece of the larger unit that fails its CRC may be a tear, and reads as a torn
     record, none, rather than damage.
   - Every wrapper of a real file passes its file's unit through (`BlockFile::atomic_unit`), a
     `SegmentedFile` its base's device's. mantle's log runs on a `DeviceFile` (`crates/node`'s
     `open_log`), which asks the OS.
3. Each frame records whether it was answered at its flush; the open restores an `Undetermined`
   frame so answered as `Lost` is, and the simulation's faults of §7.6 run against it. 3a, built
   (hyper-raft `mantle-frame-flush`): the record and the open.
   - A record in pieces says it in the first of the two bytes its pieces' header left as padding
     (hyper-log `format::AT_FLUSH`), under each piece's CRC; pieces that differ in it are two
     writes, never one record. A whole record cannot say it, since a torn whole record and a
     damaged one read alike: a log of whole records never marks a frame answered at its flush.
     No format change: a log's bytes are 2b-iii's but for that byte.
   - The open reads the bit from the frame's own records, its slot's and the copy a restore made,
     never the other slot's record of the frame before, so a crash between two frames never marks
     the earlier. An `Undetermined` frame so answered (`Tail::Undetermined { at_flush }`) is
     restored as a `Lost` one; a restore of it torn after its record landed is restored again so,
     by the copy.
   - The replica says with each update whether its group may be answered at the flush
     (hyper-log `Update::at_flush`, from hyper-durable `Write::at_flush`): where each half of the
     configuration it counts by and of the one applied has voters enough that the others are a
     quorum, `n - 1 > n / 2`, three or more. A frame holding a group that may not stays answered
     once confirmed (§7.5).

   3c, built (hyper-raft `mantle-frame-flush`; mantle's node simulation): the writer sets the bit,
   and the simulation's tears of §7.6 on one member, on a quorum and on every member of a
   one-voter group run against it, their laws held on seeds 1-8 and the groups left waiting
   counted (§7.5); the one-voter group never marked.
   - A frame's record, and its confirmation, say it was answered at its flush where the record is
     in pieces (format 5, 6 sealed), the only records that can say it, and every update in the
     frame allows it (`Update::at_flush`). A frame of no updates, one holding a group whose update
     does not allow it, and every frame of a log of whole records say not.
   - Frames are still answered only once confirmed, so the bit only adds marks: an `Undetermined`
     frame so recorded is restored as a lost one and its groups marked, where it was the torn
     tail. A binary of 2b reads the byte as padding and keeps such a frame the torn tail, which is
     what a frame answered once confirmed is.
4. The writer answers a frame at its flush where §7.5 allows, and the flushes a PUT are counted.
   Before any frame is answered at its flush, binaries of 2b, which read the bit's byte as padding
   and would cut such a frame as the torn tail, must refuse the log, as the mark made binaries
   before them refuse it (step 2b-ii). The device facts §7.5 names are step 5's write cache and
   step 2b-i's atomic write unit. 4a, built (hyper-raft `mantle-frame-flush` `abfca69`):
   - The open and the create read the device's write cache (`BlockFile::write_cache`) where they
     read its atomic write unit, and the log reports both with how it answers a frame's updates
     and why (`Laid::cache`, `Laid::answers`; `Log::laid`, `Recovery::laid`; mantle's
     `Node::laid`): `Whole`, records that cannot say a frame was answered at its flush;
     `WriteBack`, a volatile cache or none stated, this Mac's internal SSD among them (`FullFsync`);
     `Unmeasured`, a cache the OS states keeps a completed write, not yet checked by measuring
     (§7.5). Every one answers once confirmed: a write is still its frame's flush and its
     confirmation's.
   - The mark of pieces says, in its sequence field's next bit, that the log may hold frames
     answered at their flush (`format::Mark::at_flush`, `MARK_AT_FLUSH`). A binary from before
     (hyper-raft `f3dae3b` and earlier) read that field only as 0 or 1: it reads such a mark as
     none, then the area's start as a whole persist record of the other kind, and refuses the log
     (`LogError::Foreign` unsealed, `Tampered` sealed), where it would cut such a frame, torn after
     its record landed, as the torn tail. Such a mark always records its block. This binary reads
     it, and every re-lay and every mark written again keeps it. Nothing sets it yet.

   4b, built (hyper-raft `mantle-frame-flush` `0f5dce2`, `f9248d5`, `2b50efd`): the mechanism,
   behind the owner's decisions (§7.5's table), which answer at the flush nowhere until the owner
   takes one.
   - The policy: hyper-log `AtFlush`, decision (A) `WriteBackFrames` and decision (B)
     `StatedCache`, with `AtFlush::DECIDED` today's; a log is made or opened with it
     (`With::at_flush`, `DECIDED` by default, as every `create` and `open` takes it). `Answers`
     gains `Kept` (condition (a)), `Accepted` (decision (A)) and `Buffered` (decision (B) taken
     on a file whose completed writes are not the device's), and `Answers::at_flush`.
   - The create and the open of a log that answers at the flush set the mark's bit and flush it
     before its first frame, the restore included, so a binary of `f3dae3b` or before refuses the
     log. A mark that has the bit keeps it, whatever the log answers later.
   - The writer answers a frame at its flush where its record says so (`AT_FLUSH`: records in
     pieces, every update in the frame allowing it) and the log answers so; under condition (a)
     only for a frame in a slot the file holds written whole (`BlockFile::fills_new_space`). The
     frame's write and then its record's, one flush, then the device answers the frame before and
     this one, and writes no confirmation; the owner gives the frame's room back then. Everywhere
     else, answers come once confirmed, as before.
   - The simulated device that keeps every completed write: hyper-sim's block device told to
     (`Device::keep_completed`), each write's sectors durable as it returns, a lost write still
     never on the medium; hyper-block's `SimFile::keeping`, its cache write-through from
     `Source::Simulated` and its writes the device's (`writes_reach_device`). Tests take decision
     (B) for it alone.
   - Tests (hyper-log `tests/at_flush.rs`, `recover::tests`), all passing: a frame of three-voter
     groups answered at its flush costs one durable operation, its flush, where a confirmed one
     costs two, the flush and the confirmation's durable write, counted on the device and in the
     log's `flushes` and `durable_writes`, with no `flushed_confirmations`; the power cut at each of
     the frame's three operations (its write, its record's write, its flush) and after them, each
     with `Crash::LoseAll`, `Random` and `KeepAll`, twelve runs: an entry is answered only when
     the cut comes after its flush, every answered entry is held after the open, at most the cut
     frame's entry more, and no group is marked (no `Undetermined` or `Lost` tail), since a record
     written after its frame's write completed proves the frame landed; a write-back device
     answers at the flush only under decision (A); a one- or two-voter group's frame never, under
     either; a binary from before refuses the mark the writer itself set, at a create and at an
     open of a log made before the decision.
   - Not built: condition (b), unreachable in this layout (§7.5); a real file's
     `writes_reach_device`, built in 4c; the measurement (a) needs (§7.5); and the node's count of
     log flushes per PUT on the simulated device, which needs the node to open its log with a
     decision and the simulation's store on `SimFile::keeping`.

   4c, built (hyper-raft `mantle-frame-flush` `70e150f`, `74242cd`, `a6a701f`): a real file's
   `writes_reach_device`, so that decision (B) can reach a real device. hyper-block's
   `DeviceFile::writes_reach_device` answers whether a write the handle completes over written
   space is the device's own completion, no cache of the OS's in front of it, for the handle as it
   was opened, never as it was asked for: a `PreferDirect` open that the file system refused, or
   that landed on tmpfs (`sys::in_memory`), is buffered and answers false. The answer says nothing
   of the device's cache; the device's stated cache (step 5) is read beside it. Per platform
   (sources: research/02 §2.7-§2.10, §3.7-§3.8, §4.5-§4.6):
   - Linux, true for a direct handle on a block device, or on a file of ext2, ext3, ext4 or XFS
     that statx(2) says takes direct I/O. open(2) promises of `O_DIRECT` that "File I/O is done
     directly to/from user-space buffers", past the page cache, and no more: it "makes an effort to
     transfer data synchronously, but does not give the guarantees of the O_SYNC flag that data and
     necessary metadata are transferred", says nothing of the device's volatile cache, and lets a
     misaligned transfer "fall back to buffered I/O" (hyper-block checks every direct transfer
     aligned first). The kernel's own direct write takes the completion of "a normal write to a
     device without a volatile write cache" as written through, for "a pure overwrite that doesn't
     require any metadata updates" (fs/iomap/direct-io.c), which is condition (a) on a stated
     write-through cache. Only file systems that overwrite in place qualify (ext2-4 and XFS, by
     statfs(2) type, the set `fills_new_space` names): btrfs, f2fs and bcachefs overwrite out of
     place, a network file system's server completes the write ("the server may still cache the
     I/O"), and a file system not named is taken not to. And ext4 writes some files' direct
     transfers through the page cache (data journaling, fs-verity, inline data:
     `ext4_dio_alignment`, `ext4_dio_write_iter`), which statx(2) reports as a direct-I/O
     alignment of 0, "direct I/O is not supported on this file"; a kernel before 6.1 cannot be
     asked and answers false. `O_DSYNC`, or `RWF_DSYNC` a write (pwritev2(2)), would make each
     write durable on its own, "as though each write(2) was followed by a call to fdatasync(2)",
     whatever the cache: a FUA write or a flush each, which only `write_durable_at` pays.
   - macOS, never. fcntl(2)'s `F_NOCACHE` "Turns data caching off/on" and promises nothing of where
     a completed write is: XNU's cluster layer writes past the cache only a write of 16 KiB or more
     at a page-aligned offset from an aligned buffer, and copies any other through it
     (`vfs_cluster.c`), a log's 4 KiB frame among them; APFS's own path is closed; and fsync(2)
     leaves data in the drive's cache, which only `F_FULLFSYNC` "asks the drive to flush". This
     Mac's direct and buffered files both answer false.
   - Windows, true for a handle opened with `FILE_FLAG_NO_BUFFERING`, which disables "system
     caching of data being read from or written to the file", the caller's buffer "effectively the
     only file buffer that exists for this operation" ("File Buffering"), and "does not affect hard
     disk caching" (CreateFileW). Its metadata "may still be cached" (CreateFileW), so only an
     overwrite of written space is the disk's alone. `FILE_FLAG_WRITE_THROUGH` beside it would also
     request "a write-through of the hard disk's local hardware cache to persistent media", which
     "Not all hard disk hardware supports" (CreateFileW, "Caching Behavior"); no handle is opened
     with it.
   - Every other platform, and every buffered handle: never.
   - A duplicate answers as its original, a `SegmentedFile` its base's, asked once at open as its
     device's atomic write unit and cache are, and the wrappers of hyper-raft's benches, e2e and
     comparison pass their file's through. mantle's log runs on a `DeviceFile` (`crates/node`'s
     `open_log`), which asks the OS.
   - Tests, all passing on this Mac (hyper-block `file::tests`, `segmented::tests`; hyper-log
     `tests/at_flush.rs`): Linux's rule over each file system and statx(2) answer, run on every
     host; each platform's answer for a direct and a buffered open, the platform's own run on its
     host (macOS here: false for both); a segmented file answering its base's at create, after a
     segment is added, at open and through a second handle; and a real direct file whose device is
     stated to write through, decision (B) taken in the test: `Kept`, its mark set from the first
     flush, where the file's writes reach the device, `Buffered` where not (macOS's direct file and
     every buffered one, as run here), and `Unmeasured` under `AtFlush::DECIDED`, which is
     unchanged.
   - What decision (B) would do now: on Linux, a log on a direct file of ext4 or XFS whose queue
     states "write through" answers `Kept`, and a frame of three-voter groups in a slot the file
     holds written whole (`fills_new_space`, true there) is answered at its flush, one flush and no
     confirmation; a block device node answers `Kept` and no frame at its flush, as nothing is
     written ahead on a node (`fills_new_space` false). On Windows, a direct file on a device
     stating write-through or a battery-backed cache answers `Kept`, but no frame there is
     answered at its flush yet, as NTFS's space is not written ahead (`fills_new_space` false) and
     condition (a) answers only a frame overwriting a slot written whole. On macOS, `Buffered`,
     every frame once confirmed; this Mac's SSD states no cache (`WriteBack`), so (B) would not
     reach it either. A device stating a volatile cache stays `WriteBack` under (B) everywhere.
5. The device's write cache is read from the OS beside its atomic write unit, for step 4. Built
   (hyper-raft `mantle-frame-flush`): hyper-block `write_cache::write_cache` answers a
   `DeviceCache`, a `WriteCache` (`WriteThrough`, `PowerProtected`, `WriteBack`) with its `Source`
   and whether the OS stated it (`stated`).
   - Linux: the `write_cache` of the queue the atomic unit is read from, "write back" or "write
     through" (Documentation/ABI/stable/sysfs-block; research/02 §2.4), the kernel's view, which
     also decides whether the kernel sends the device flushes at all
     (Documentation/block/writeback_cache_control.rst), with `fua` beside it. Any other text is a
     typed corruption.
   - Windows: `IOCTL_STORAGE_QUERY_PROPERTY` for `StorageDeviceWriteCacheProperty`, asked of the
     file's volume, opened by its volume GUID path with no access rights, which CreateFileW allows
     "to query device attributes without accessing a device", so without privilege; a node is
     asked itself. In `STORAGE_WRITE_CACHE_PROPERTY` (Microsoft Learn), no cache, a write-through
     one and a disabled write-back one are write-through; an enabled write-back one is
     power-protected where `NVCacheEnabled` says "the device has a battery backup for the write
     cache", else write-back; a type the system cannot report, or a device that does not answer,
     is the default. The structure has no `IsPowerProtected` field: its battery backup is
     `NVCacheEnabled`.
   - macOS: the block storage device's "WriteCacheState" (`kIOBlockStorageDeviceWriteCacheStateKey`,
     IOBlockStorageDevice.h: true where its write cache is enabled), read from the I/O Registry
     without root, as the atomic unit's sizes are. Where the device publishes none, fcntl(2)'s
     rule stands: F_FULLFSYNC "asks the drive to flush all buffered data to the permanent storage
     device", so the cache is write-back. The M-series Mac this was built on (APPLE SSD AP8192Z,
     `IOEmbeddedNVMeBlockDevice`) publishes none on any entry from the volume to the controller:
     `WriteBack`, from `FullFsync`, not stated (2026-11-18).
   - A device the OS does not describe: write-back. Answering a write before its flush is safe
     only where the OS says the cache keeps it; a flush is safe on every device.
   - A power-protected cache counts as write-through: what loses a completed write is the cache's
     volatility, "I/O completion ... before data actually has hit the non-volatile storage"
     (writeback_cache_control.rst), and a battery-backed cache keeps what it holds.
   - Every file carries it as it carries the atomic unit (`BlockFile::write_cache`): a
     `DeviceFile` asks the OS, a `SegmentedFile` answers its base's device's, every wrapper of a
     real file passes its file's through, and every other file answers the default. Nothing is
     answered differently yet.
   - The macOS registry search released null where no entry had the key, which traps in
     CoreFoundation; every key the atomic unit reads is published on an Apple SSD, so nothing hit
     it before "WriteCacheState". Fixed at its cause (hyper-raft `33fc359`).

## 8. Open

- Rates from mantle's own device and node history, in place of the literature's
  (docs/research/10), over the whole path to a chunk, with its clustering in time: the
  chain's constant rates are the least-qualified part of every number above (§5).
- A permanent-loss rate for racks, zones and sites, and the burst frequency, which no
  fetched source states.
- Prioritized and parallel repair. Both raise durability, and Ford leaves both out (15 §1.4).
  Repair is built by margin and with measured concurrency (repair.md §5); the model still counts
  it serial, the conservative side.
- The copyset family that bounds the chance of losing any block in one event, beside this
  per-block probability (04 §A6.3, §R2).
- Choosing each block's scheme when it is sealed, with the placement service (STATUS,
  multi-machine operation).
- Detection times per pool, spun-down groups above all, and whether a cold pool's measured rates
  call for a code wider than the tested set, which would be added to `mantle-ec`'s tests first
  (storage-classes.md §4).
