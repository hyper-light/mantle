# 15 — Durability models: ground truth for choosing a block's code

Research note for `crates/ec/src/durability.rs` and the decision record
docs/design/durability.md. It extends note 04 (§A5 Ford et al., §A6 copysets, §R1–§R6) with
what a model of permanent loss needs:

- the structure of Ford et al.'s Markov model, which note 04 summarizes but does not state;
- the argument over MTTDL and the metrics that replace it;
- how to solve such a chain without losing its digits;
- the field failure rates that feed it;
- S3's own durability target.

Compiled 2026-09-29. Each paper was fetched and read. Formulas were transcribed from pages
rendered at 200–250 dpi, not from text extraction. The labels follow note 04:

- **DERIVED** marks a derivation or computation of ours.
- **UNVERIFIED** marks what no fetched primary text confirms.
- "PDF p." is the page within a PDF that has no printed page numbers.

---

## 1. Ford et al., "Availability in Globally Distributed Storage Systems", OSDI 2010

The PDF has 14 pages and no proceedings page numbers. The range pp. 61–74 is given by
Venkatesan et al. (MASCOTS 2011, ref. [12]), and Haystack (pp. 47–60, note 01) precedes it
in the session. This closes most of note 04's UNVERIFIED item 3.

### 1.1 States, and what the chain measures (§7, PDF p. 8)

- "The model focuses on the availability of a representative stripe. Let s be the total
  number of chunks in the stripe, and r be the minimum number of chunks needed to recover
  that stripe. … The state of a stripe is represented by the number of available chunks.
  Thus, the states are s, s−1, . . . , r, r−1 with the state r − 1 representing all of the
  unavailable states".
- "computing the MTTF of a stripe, as the mean time to reach the 'unavailable state' r − 1
  starting from state s, follows by standard methods" (§7.1, PDF p. 9, citing Resnick 1992).
- **Availability, not durability.** "We call a chunk available if the node it is stored on
  is available" (§2.2), and "in this paper we focus only on events that are 15 minutes or
  longer" (§2.1). Every stripe MTTF in Tables 3 and 4 is therefore a mean time to
  unavailability lasting 15 minutes or more, not a mean time to data loss. A durability
  chain takes permanent losses and the time to detect and rebuild them.

### 1.2 Independence (§7, PDF p. 9)

"A key assumption of the Markov model is that events occur independently and with constant
rates over time. This independence assumption, although strong, is not the same as the
assumption that individual chunks fail independently of each other. Rather, it implies that
failure events are independent of each other, but each event may involve multiple chunks."

### 1.3 Failure transitions (§7.1, PDF p. 9)

- "Let λ denote the rate of failure events affecting chunks, including node and disk
  failures. For any observed failure event we compute the probability that it affects k
  chunks out of the i available chunks in a stripe. … for failure bursts this computation
  takes into account the stripe placement strategy."
- "Averaging these probabilities over all failures events gives the probability, p_{i,j},
  that a random failure event will affect i−j out of i available chunks in a stripe. This
  gives a rate of transition from state i to state j < i, of **λ_{i,j} = λ p_{i,j}** for
  s ≥ i > j ≥ r and **λ_{i,r−1} = λ Σ_{j=0}^{r−1} p_{i,j}** for the rate of reaching the
  unavailable state." Text extraction garbles the second formula.
- Bursts enter only through p_{i,j}, averaged over the observed events. No parametric burst
  size is fitted. Burst size classes (Fig. 10): "small (0-0.001), medium (0.001-0.01), large
  (0.01-0.1)" of all nodes. §9 notes that others fit beta-binomial and biexponential burst
  sizes, and that "using an over-simplistic model for burst size, for example a single size,
  could result in 'dramatic inaccuracies'".
- **Hit probability** (§5.2, PDF p. 7): "a ratio where the numerator is the number of ways
  to place a stripe of size n in the cell such that exactly k of its chunks are affected by
  the burst, and the denominator is the total number of ways to place a stripe of size n in
  the cell. … when chunks are constrained by a placement policy, … computed using dynamic
  programming."
- DERIVED formulas:
  - Uniform placement over N nodes with a burst of b gives
    P(k) = C(b,k)·C(N−b, n−k)/C(N, n).
  - With one chunk per rack, P(k) is the coefficient of x^n t^k in
    Π_j (1 + x((N_j−b_j) + b_j t)), normalized. It is computed by a dynamic program over
    racks of non-negative integers.
  - Both match brute-force enumeration.
- Gap: for a stripe already degraded to i < s chunks, Ford does not say how a burst meets
  chunks already down. The reading used here treats the i available chunks as an i-chunk
  placement.
- Notation: λ in §7.1 is a rate of events, and in §8.2 a rate per chunk.

### 1.4 Recovery (§7.1, PDF p. 9)

- "we assume a fixed rate of ρ for recovering a single chunk, i.e. moving from a state i to
  i + 1, where r ≤ i < s. … This is justified by setting ρ to a lower bound for the rate of
  recovery … While parallel recovery of multiple chunks from a stripe is possible,
  ρ_{i,i+1} = (s − i)ρ, we model serial recovery to gain more conservative estimates of
  stripe availability."
- Prioritized recovery is possible in the model but unused: "for ease of exposition we do
  not use this added degree of freedom".
- The 15-minute delay is not a model parameter. It appears through the event filter, and
  only the trace simulation (§6) holds chunks unavailable "until 15 minutes later when the
  chunk recovery process starts".

### 1.5 Parameters behind Table 3

- Published: Table 2's component MTTFs, "Disk 10-50 years; Node 4.3 months; Rack 10.2
  years", and cells of "1000 to 7000 nodes". §4.1 bounds recovery from below: w = 120 s "is
  less than a tenth of the average time it takes our system to recover a chunk".
- Unpublished: λ, ρ, the burst rates and sizes, and the cell layout behind Table 3
  (UNVERIFIED).
- DERIVED: fitting §8.2's serial chain to the R=2 and R=3 rows of Table 3's independent
  column gives λ = 8.1345e-3 per day (chunk MTTF 4.04 months) and ρ = 66.013 per day
  (21.8 min).
  - These reproduce R=4 within 0.4%, but not R=5 (×2.03).
  - They reproduce no RS row, which is ×20 to ×3200 higher. Parallel repair fits worse.
  - The RS rows cannot be regenerated from the text, and are not test vectors.

### 1.6 Validation (§8.1)

- "Our main goal in using the model is providing a relative comparison of competing storage
  solutions, rather than a highly accurate prediction of any particular solution."
- One cell measured 1.76E+6 days against a prediction of 5E+6, the model 2.8× optimistic.
  Another measured 29.52E+8 against 5.77E+8, 5.1× pessimistic.

### 1.7 §8.2, the chain without bursts (PDF p. 10)

"the transition rate from state i ≥ r to state i − 1 is iλ, and from state i to i + 1 is ρ
for r ≥ i < s" (the PDF's typo for r ≤ i < s).

  **MTTF = (1/λ) · Σ_{k=0}^{s−r} Σ_{i=0}^{k} (ρ^i/λ^i) · 1/(s−k+i)_{(i+1)}**

"where (a)_{(b)} denotes (a)(a − 1)(a − 2) · · · (a − b + 1)". "Assuming recoveries take
much less time than node MTTF (i.e. ρ >> λ)":

  **ρ^{s−r}/λ^{s−r+1} · 1/(s)_{(s−r+1)} + O(ρ^{s−r−1}/λ^{s−r})**

"reducing recovery times by a factor of µ will increase stripe MTTF by a factor of µ² for
R = 3 and by µ⁴ for RS(9, 4)." "For RS(6, 3) with no correlated failures, a 10% reduction
in recovery time results in a 19% reduction in unavailability. However, when correlated
failures are taken into account, even a 90% reduction in recovery time results in only a 6%
reduction in unavailability."

DERIVED checks:

- (a) In exact rational arithmetic, the double sum equals the serial chain's mean
  absorption time for (s,r) ∈ {(2,1), (3,1), (4,1), (5,1), (6,4), (9,6), (13,9), (12,8),
  (5,2)}.
  - For R=2 it is (3λ+ρ)/(2λ²).
  - For R=3 it is (11λ²+4λρ+ρ²)/(6λ³).
- (b) With r = s−1 it is Greenan's (µ+(2n−1)λ)/(n(n−1)λ²) (§2).
- (c) Its leading term, times m/n clusters, is Iliadis–Venkatesan's MTTDL^clus (§3) for
  exponential rebuild.
- (d) The "19%" matches R=3 (1−0.9²). The closed form gives RS(6,3) 27.1%.

### 1.8 Multi-cell (§7.2, §8.5)

"we treat each cell as a 'chunk' in the multi-cell 'stripe' … We assume that failures at
different data centers are independent". "most cross-cell recoveries will occur in the
event of large failure bursts."

---

## 2. Greenan, Plank, Wylie, "Mean time to meaningless", HotStorage 2010

- §1: "There are three major problems with using the MTTDL as a measure of storage system
  reliability. First, the models on which the calculation depends rely on an extremely
  simplistic view of the storage system. Second, the metric does not reflect the real
  world, but is often interpreted as a real world estimate. … Finally, MTTDL values tend to
  be incomparable because each is a function of system scale and omits the (expected)
  magnitude of data loss."
- §2: **MTTDL = (µ + (2n − 1)λ)/(n(n − 1)λ²) ≈ µ/(n(n − 1)λ²)** for RAID-4.
- §3: "MTTDL literally measures the expected time to failure over an infinite interval.
  This may make the MTTDL useful for quick, relative comparisons, but the absolute
  measurements are essentially meaningless. … A system designer is most likely interested
  in the probability and extent of data loss, every year for the first 10 years of a
  system."
- §4.1: "The exponential distribution is a poor match to observed disk failure rates,
  latent sector error rates, and disk repairs."
- §4.2: "it is the earliest failure, whose rebuild is closest to completion, that governs
  repair transitions."
- §5: a metric must be "Calculable, Meaningful, Understandable, Comparable".
- §5.1–§5.2: NOMDL_t, the "expected amount of data lost per usable terabyte within mission
  time t", is MDL_t/D. The paper recommends Monte Carlo simulation to compute it.
- The counterpoint, Venkatesan, Iliadis, Fragouli, Urbanke (MASCOTS 2011, §IV):
  - MTTDL "provides meaningless results if it is associated with lifetime and misused to
    obtain absolute measurements".
  - "Nonetheless, it is useful for assessing trade-offs, for comparing schemes… no study in
    the literature disproves the validity of MTTDL as criterion in the comparison of the
    reliability of one scheme with that of another."

---

## 3. Iliadis and Venkatesan: the expected annual fraction of data loss (EAFDL)

Sources:

- I. Iliadis, V. Venkatesan, "Reliability Evaluation of Erasure Coded Systems", *Int. J.
  Advances in Telecommunications* 10(3&4):118–144, 2017. Open access; read in full.
- The metric originates in their MASCOTS 2014 paper (pp. 375–384), which is paywalled; its
  abstract was read.

**Definition** (2017, §IV):

- (15) **EAFDL = E(H)/(MTTDL·U)**. H is the data lost given a loss, U the user data, and
  the MTTDL is in years.
- (18) EAFDL ≈ nλE(Q)/U, with E(Q) = P_DL·E(H).
- (14) MTTDL ≈ 1/(nλ·P_DL), where P_DL is the probability that an exposure ends in loss.

**Clustered placement** (disjoint groups of m devices, an (l,m) MDS code, device data c,
rebuild bandwidth b, rebuild time X with E(X) = c/b):

- (68) MTTDL^clus ≈ (1/(nλ))·(b/(λc))^{m−l}·1/C(m−1, l−1)·[E(X)]^{m−l}/E(X^{m−l})
- (69) EAFDL^clus ≈ λ·(λc/b)^{m−l}·C(m, l−1)·E(X^{m−l})/[E(X)]^{m−l}
- (70) E(H)^clus = l·c/(m−l+1)

**Declustered placement:** (81)–(83), in the paper, p. 127.

**Consequences stated:**

- Random rebuild times lower MTTDL and raise EAFDL relative to deterministic ones.
- Remark 3: EAFDL does not depend on n.
- Remark 11: "the declustered placement scheme minimizes EAFDL for any n, m, l, λ, b, c,
  and rebuild time distribution".

**Assumptions:**

- "the lifetimes Y1, · · · , Yn of the n devices are independent and identically
  distributed … An extension of the analysis to also address correlated failures is part of
  future work" (§III-E).
- Failures are detected instantaneously, and the network is ample (§V-D).
- The closed forms therefore cannot take bursts; only the definition carries over.

---

## 4. Solving the chain without losing its digits

### 4.1 Why elimination fails (DERIVED)

We solved Ford's chains with the back-solved λ and ρ in IEEE binary64 and compared each
method against exact rational arithmetic. The table gives relative errors.

| Code | Exact MTTF (d) | LU, partial pivoting | Kohlas/Hunter reduction | GTH + regenerative identity |
|---|---|---|---|---|
| R=3 | 1.35e9 | 1.6e-9 | 3.5e-16 | 0 |
| R=5 | 4.45e15 | 2.8e-3 | 1.1e-16 | 1.1e-16 |
| RS(8,4) | 5.62e12 | 3.1e-5 | 0 | 3.5e-16 |
| RS(9,6) | 1.09e18 | 0.78 | 0 | 1.2e-16 |
| RS(12,8) | 3.81e22 | 1.00 | 0 | 2.2e-16 |

The row sums of −Q restricted to the transient states are the absorption rates, which are
tiny beside the other rates, so elimination cancels.

### 4.2 GTH (O'Cinneide, *Numer. Math.* 65:109–120, 1993)

- "The GTH algorithm involves no subtractions, and therefore loss of significant digits due
  to cancellation is ruled out completely" (p. 110).
- Its Step 1, α_k = Σ_{j>k} w_kj, "is what distinguishes GTH from standard Gaussian
  elimination. It allows one to avoid computing the diagonals … by subtraction" (p. 116).
- Theorem 2 (p. 117) bounds the componentwise relative error by 1.06(2φ(n)+n)u, where
  φ(n) = (2n³+6n²−8n)/3.

### 4.3 Mean first passage times (Hunter, *Special Matrices* 4(1), 2016)

- Read from arXiv:1510.01390v4, which is marked as the accepted paper.
- The paper builds on Kohlas's procedure (*Z. Oper. Res.* 30, 1986), which "considering
  the computation of the mean times to absorption" treats the chain as a Markov renewal
  process.
- **Theorem 2** eliminates state n:
  - (31) p_ij^(n−1) = p_ij^(n) + p_in^(n)·p_nj^(n)/S(n)
  - (32) µ_i^(n−1) = µ_i^(n) + p_in^(n)·µ_n^(n)/S(n)
  - Here S(n) = Σ_{j<n} p_nj^(n) = 1 − p_nn^(n), and the mean first passage times of the
    remaining states are unchanged.
- (49): with two states left, m_12 = µ_1^(2)/p_12^(2).

### 4.4 The algorithm (DERIVED from §4.2–§4.3)

- Treat the chain as a Markov renewal process:
  - q_i = Σ_{j≠i} q_ij;
  - p_ij = q_ij/q_i;
  - µ_i = 1/q_i.
- Collapse the loss states into one absorbing state A.
- Eliminate every transient state but the whole one with (31)–(32). Compute S as the sum of
  the eliminated state's exits other than to itself, never as 1 − p_nn.
- The mean time to loss is µ_s/p_sA. Only +, × and ÷ act on non-negative numbers.
- The probability of loss within a time t can be computed directly from the generator by Xue
  and Ye's method (*Math. Comp.* 82:1577–1596, 2013). It shifts to a non-negative matrix,
  whose Taylor series "involves no subtractions", and so avoids 1 − Σ over the transient
  states, which at 1e-11 keeps about five digits.

### 4.5 When the time to loss is exponential (Keilson, *Markov Chain Models: Rarity and Exponentiality*, Springer, 1979)

- A chain that returns often to a recurrent set and reaches a rare set seldom has a first
  passage time to the rare set that is close to exponential; the approximation's error is of
  the order of the ratio of the time the chain spends away from the recurrent set on each
  excursion to the mean passage time. This is the ground for the law 1 − e^(−t/M) in a
  stripe's chain, where repair returns a degraded stripe to whole (DERIVED for the stripe: an
  excursion lasts about the spare chunks' repair time).
- Without that return, as with no repair, it fails: the time to loss is then a sum of
  exponential phases, whose early distribution is far from the exponential of the same mean.
  RS(6,3) over three zones lost at λ with nothing repaired is lost after a zone loss at 3λ
  and then one at 2λ: P(T ≤ t) = 1 − 3e^(−2λt) + 2e^(−3λt) ≈ 3λ²t², where the exponential
  law with mean 5/(6λ) gives 1.2λt (DERIVED).
- Where the law does not hold, the transient is computed by uniformization, as §4.4 says:
  every term non-negative.

---

## 5. Field failure rates, and S3's target

**Schroeder and Gibson, FAST 2007, pp. 1–16:**

- "annual disk replacement rates typically exceed 1%, with 2-4% common and up to 13%
  observed on some systems" (abstract).
- "The average ARR over all data sets (weighted by the number of drives in each data set)
  is 3.01%" (§4.1).
- **Correlation:**
  - "under the Poisson distribution the probability of seeing ≥ 20 failures in a given
    month is less than 0.0024, yet we see 20 or more disk replacements in nearly 20% of all
    months in HPC1's lifetime" (§5.1).
  - "the probability of seeing two drives in the cluster fail within one hour is four times
    larger under the real data, compared to the exponential distribution" (§5.3).

**Pinheiro, Weber, Barroso, FAST 2007, pp. 17–29:** "The observed range of AFRs … varies
from 1.7%, for drives that were in their first year of operation, to over 8.6%, observed in
the 3-year old population" (§3.1). The paper has no temporal correlation analysis.

**Schroeder, Lagisetty, Merchant, FAST 2016, pp. 67–80:**

- "most models see around 5% of their drives permanently removed from the field within 4
  years after being deployed, while the worst models (MLC-B and SLC-B) see around 10%"
  (§6.3). Table 5 gives 3.78–10.31%.
- DERIVED: 1–2.6% a year, if spread evenly.

**Maneas, Mahdaviani, Emami, Schroeder, FAST 2020, pp. 137–149** (1.4 million NetApp SSDs):

- "The average ARR across the entire population is 0.22%, but rates vary widely depending
  on the drive model, from as little as 0.07% to nearly 1.2%" (p. 138).
- **Correlation:** "the empirical probability that a RAID group will experience a drive
  replacement in a random week… is equal to 0.0504%… another drive replacement within a
  week following a previous drive replacement… is equal to 9.39%, that is, more than a
  factor of 180X increase" (§6).
- "realistic data loss analysis certainly has to consider correlated failures" (§8).

**Correlated loss across a cluster.** Cidon et al. (ATC 2013) take 1% of nodes failing to
return after a power outage, citing reports of 0.5–1%, and one outage a year (note 04
§A6.1).

**S3's target:**

- "Designed to provide 99.999999999% durability and 99.99% availability of objects over a
  given year" (S3 User Guide, "Data protection in Amazon S3", fetched 2026-09-29).
- "Amazon S3's design for durability is a function of storage device failure rates and the
  rate at which S3 can detect failure and then re-replicate data on those devices" (S3
  FAQ).
- The FAQ's former example, 10,000,000 objects with one lost every 10,000 years, is no
  longer on the page (UNVERIFIED in current documents).

---

## 6. Analysis

- **The per-stripe annual loss probability is exact under bursts.** It is a marginal
  probability, so a per-stripe chain that includes correlated events gives it directly. The
  same holds for EAFDL, which is a byte-weighted average of marginal losses and is bounded
  above by the stripe's loss probability. (DERIVED.)
- **Neither marginal metric sees how losses cluster.** System MTTDL and the chance of losing
  any stripe need the joint model, the copyset combinatorics of note 04 §A6.3: Cidon's
  trade of frequency against magnitude.
- **From objects to stripes.** S3 states durability per object per year. By the union bound,
  an object of B blocks is lost with probability at most B times a block's. (DERIVED.)
- **Ford's absolute numbers are availability** (§1.1). Note 04 §A5 reads Table 3 as
  durability evidence. The relative lesson holds (correlation dominates, and a larger m
  beats a larger k at equal overhead), but the magnitudes do not transfer.

## 7. Discrepancies and gaps

1. Ford's Table 3 inputs are unpublished, and the RS rows of the independent column are not
   reproducible from the paper's chain (§1.5).
2. Ford §8.2's "19%" for RS(6,3) matches R=3 under the closed form. RS(6,3) gives 27.1%.
3. Ford §8.2 writes "r ≥ i < s" for r ≤ i < s.
4. Ford's λ is an event rate in §7.1 and a per-chunk rate in §8.2.
5. Ford Table 4 gives 6.8 MB/day of inter-cell bandwidth "per user PB" as R=2's inverse MTTF.
   But 1 PB/1.47E5 d is 6.8 GB/day, so the unit is off by 1000. Note 04 §A5 repeats it.
6. Ford §8.3's "at least two orders of magnitude, and eight in the case of RS(8,4)" does not
   hold row by row. R=2 differs by 0.53 orders and RS(9,4) by 9.58.
7. The EAFDL closed forms assume independent failures, instantaneous detection and ample
   network bandwidth.
8. Not retrieved: MASCOTS 2014, QEST 2013 and MASCOTS 2012 in full; GTH 1985; Kohlas 1986;
   Heyman and Reeves 1989; Alfa, Xue and Ye 2002.
9. Hunter's subtraction-free form of a general first-passage denominator is conjectured. The
   algorithm of §4.4 does not need it.

## Sources

- [FLP+10] D. Ford, F. Labelle, F. I. Popovici, M. Stokely, V.-A. Truong, L. Barroso,
  C. Grimes, S. Quinlan. "Availability in Globally Distributed Storage Systems." OSDI 2010,
  pp. 61–74. https://www.usenix.org/legacy/event/osdi10/tech/full_papers/Ford.pdf
- [GPW10] K. M. Greenan, J. S. Plank, J. J. Wylie. "Mean time to meaningless: MTTDL, Markov
  models, and storage system reliability." HotStorage 2010.
  https://www.usenix.org/legacy/event/hotstorage10/tech/full_papers/Greenan.pdf
- [IV17] I. Iliadis, V. Venkatesan. "Reliability Evaluation of Erasure Coded Systems."
  Int. J. Advances in Telecommunications 10(3&4):118–144, 2017.
  https://www.iariajournals.org/telecommunications/tele_v10_n34_2017_paged.pdf
- [IV14] I. Iliadis, V. Venkatesan. "Expected Annual Fraction of Data Loss as a Metric for
  Data Storage Reliability." MASCOTS 2014, pp. 375–384. doi:10.1109/MASCOTS.2014.53
- [VIFU11] V. Venkatesan, I. Iliadis, C. Fragouli, R. Urbanke. "Reliability of Clustered vs.
  Declustered Replica Placement in Data Storage Systems." MASCOTS 2011, pp. 307–317.
  doi:10.1109/MASCOTS.2011.53
- [OC93] C. A. O'Cinneide. "Entrywise perturbation theory and error analysis for Markov
  chains." Numer. Math. 65:109–120, 1993. doi:10.1007/BF01385743
- [Hun16] J. J. Hunter. "Accurate calculations of stationary distributions and mean first
  passage times in Markov renewal processes and Markov chains." Special Matrices 4(1), 2016.
  doi:10.1515/spma-2016-0015; arXiv:1510.01390v4
- [XY13] J. Xue, Q. Ye. "Computing exponentials of essentially non-negative matrices
  entrywise to high relative accuracy." Math. Comp. 82(283):1577–1596, 2013.
  doi:10.1090/S0025-5718-2013-02677-4
- [SG07] B. Schroeder, G. A. Gibson. "Disk failures in the real world: What does an MTTF of
  1,000,000 hours mean to you?" FAST 2007, pp. 1–16.
- [PWB07] E. Pinheiro, W.-D. Weber, L. A. Barroso. "Failure Trends in a Large Disk Drive
  Population." FAST 2007, pp. 17–29.
- [SLM16] B. Schroeder, R. Lagisetty, A. Merchant. "Flash Reliability in Production: The
  Expected and the Unexpected." FAST 2016, pp. 67–80.
- [MME+20] S. Maneas, K. Mahdaviani, T. Emami, B. Schroeder. "A Study of SSD Reliability in
  Large Scale Enterprise Storage Deployments." FAST 2020, pp. 137–149.
- [S3-DD] Amazon S3 User Guide, "Data protection in Amazon S3".
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html
- [S3-FAQ] Amazon S3 FAQs. https://aws.amazon.com/s3/faqs/
