# Durability: the scheme a block is stored in

Status: design, 2026-09-29. Sources: docs/research/15 (durability models, cited as
"15 §x") and docs/research/04 (erasure coding and placement, "04 §x").

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

The annual loss probability is 1 − e^(−t/M), for t a year and M the chain's mean time to
loss, where loss is rare against repair: a stripe returns to whole many times before it is
lost, and the time to loss is then close to exponential, to within the ratio of an
excursion's length to M (Keilson; 15 §4.5). The law is taken where that ratio, the spare
chunks' repair time over M, is within a thousandth, the precision the report keeps. Where it
is not, as with slow repair or none, the probability is computed exactly by uniformization,
whose terms are all non-negative (15 §4.4): without repair the time to loss is a sum of
exponentials, and in the zone case above the law puts the loss within t at 1.2λt against an
exact 3λ²t² when λt is small.

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
them (docs/research/10). Until then, the literature gives:

| Input | Value | Source |
|---|---|---|
| Disk annual failure rate | "2-4% common", 3.01% weighted average, up to 13% | Schroeder and Gibson (15 §5) |
| Flash annual failure rate | 0.07–1.2% by model, 0.22% on average; about 1–2.6% a year from four-year replacement rates | Maneas et al.; Schroeder et al. 2016 (15 §5) |
| Correlated loss | 1% of nodes lost in a yearly power outage, from reports of 0.5–1% | Cidon et al. (04 §A6.1) |
| Repair | detection and rebuild of one chunk | measured repair bandwidth (04 §R3) |

Failures cluster in time as well: two disks failing within an hour of each other are four
times likelier than the exponential says (Schroeder and Gibson). In a RAID group, a second
replacement follows a first within a week 180 times more often than at random (Maneas et
al.). A chain with constant rates does not hold these; the burst events stand in for them.

## 6. What the model says

These are the results of `mantle durability` with 4% annual failures, one-hour repair, and
12 failure domains:

| Case | Choice | Annual loss |
|---|---|---|
| Independent failures only | RS(6,3) | 1.1×10⁻¹⁴ |
| With a yearly outage losing 1% of nodes (`--burst 1:1`) | none within 12 domains; RS(8,4) comes closest | 7.5×10⁻⁸ |
| The same outage, 15 domains, target 10⁻⁹ | RS(9,6) | 6.1×10⁻¹¹ |
| Three zones, each lost once a century (`--zones 3:0.01`) | R3, one copy a zone | 9.8×10⁻¹² |

In the zone case, RS(6,3) falls to 1.8×10⁻⁶: every zone holds three of its chunks.

Correlated loss dominates, as Ford found for availability (04 §A5). Inside one cell exposed
to cluster-wide events, eleven nines a block needs wider parity than the named profiles,
placement across zones, or copies in more than one cell (Ford's multi-cell results, 04 §A5).
Where a cell spans zones, a code that loses as many chunks with a zone as it can spare has no
margin left after one zone's loss.

## 7. Open

- Rates from mantle's own device and node history, in place of the literature's
  (docs/research/10).
- Prioritized and parallel repair. Both raise durability, and Ford leaves both out (15 §1.4).
- The copyset family that bounds the chance of losing any block in one event, beside this
  per-block probability (04 §A6.3, §R2).
- Choosing each block's scheme when it is sealed, with the placement service (STATUS,
  multi-machine operation).
