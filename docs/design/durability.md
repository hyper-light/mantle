# Durability: the scheme a block is stored in

Status: design, 2026-09-29; error bounds and field inputs 2026-09-30; classes, detection and
flush-unverified copies 2026-09-30. Sources: docs/research/15 (durability models, cited as
"15 §x"), docs/research/04 (erasure coding and placement, "04 §x"), docs/research/28 (storage
classes, "28 §x") and 29 (device classes, "29 §x").

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
| Repair | one over the mean time to detect a loss plus the time to rebuild one chunk, per pool | measured repair bandwidth and detection time; Ford's mean exceeds 20 minutes | 04 §R3, 15 §8.4, 28 §3.3 |

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

## 7. Open

- Rates from mantle's own device and node history, in place of the literature's
  (docs/research/10), over the whole path to a chunk, with its clustering in time: the
  chain's constant rates are the least-qualified part of every number above (§5).
- A permanent-loss rate for racks, zones and sites, and the burst frequency, which no
  fetched source states.
- Prioritized and parallel repair. Both raise durability, and Ford leaves both out (15 §1.4).
- The copyset family that bounds the chance of losing any block in one event, beside this
  per-block probability (04 §A6.3, §R2).
- Choosing each block's scheme when it is sealed, with the placement service (STATUS,
  multi-machine operation).
- Detection times per pool, spun-down groups above all, and whether a cold pool's measured rates
  call for a code wider than the tested set, which would be added to `mantle-ec`'s tests first
  (storage-classes.md §4).
