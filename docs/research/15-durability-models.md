# 15 — Durability models: ground truth for choosing a block's code

Research note for `crates/ec/src/durability.rs` and the decision record
docs/design/durability.md. It extends note 04 (§A5 Ford et al., §A6 copysets, §R1–§R6) with
what a model of permanent loss needs:

- the structure of Ford et al.'s Markov model, which note 04 summarizes but does not state;
- the argument over MTTDL and the metrics that replace it;
- how to solve such a chain without losing its digits;
- the field failure rates that feed it, and what the chain leaves out of them (§8);
- a guaranteed bound on the error of solving it (§4.6);
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

### 4.6 A guaranteed enclosure of the transient (DERIVED; audit B09b)

The law of §4.5 is an approximation whose error is "of the order of" a ratio, not a bound,
and uniformization truncated at a relative tolerance (the method it replaced) bounds its tail
only if the floating-point sums are exact. What follows states a bound that holds for every
chain and every input, and is what `loss_bounds` computes.

**Facts used.**

- *Uniformization* (Jensen 1953; Grassmann, *Computers & Operations Research* 4:47–53, 1977;
  Stewart, *Introduction to the Numerical Solution of Markov Chains*, Princeton 1994, §8.4):
  for any Λ ≥ maxᵢ qᵢ, P = I + Q/Λ is stochastic and e^(Qt) = Σₙ e^(−Λt)(Λt)ⁿ/n! Pⁿ. Every
  term is non-negative. (Standard; the texts were not re-fetched for this note.)
- *Rounding* (IEEE 754-2019 §4.3.1, round to nearest): the computed result of +, ×, ÷ on
  doubles lies within half a unit in the last place of the exact result, so the exact result
  lies strictly between the neighbours `next_down` and `next_up` of the computed one (Rust
  1.98 `f64::next_up`/`next_down`: "the least number greater than self", "the greatest
  number less than self"). This holds in the subnormal range and when a positive result
  underflows to zero.
- *Sterbenz's lemma*: for y ∈ [½, 2], 1 − y is exact in floating point.

**Lemma 1 (outward rounding).** Carry each non-negative quantity as [lo, hi]. Compute lo by
the operation on the lower ends (on the upper end of a divisor) and step it down; hi by the
operation on the upper ends (the lower end of a divisor) and step it up. Since +, × and ÷ by
a positive number are monotone on non-negative numbers, the exact value stays inside at every
step. 1 − y for y ∈ [lo, hi] ⊂ [0, 1] is enclosed by [1 − hi, 1 − lo], each stepped outward
unless Sterbenz makes it exact. ∎

**Lemma 2 (the series and its tail).** Let x = Λτ ≤ ½ and S_K = Σ_{n≤K} xⁿ/n! Pⁿ. Every
Pⁿ is stochastic, so each entry of Σ_{n>K} xⁿ/n! Pⁿ is at most
r_K = Σ_{n>K} xⁿ/n! ≤ (x^K/K!)·x/(K+1)·1/(1 − x/(K+2)) ≤ 2·(x^K/K!)·x/(K+1). With
c_K = Σ_{n≤K} xⁿ/n!, e^x ∈ [c_K, c_K + r_K], so e^(−x) ∈ [1/(c_K + r_K), 1/c_K]. Hence,
entrywise, S_K/(c_K + r_K) ≤ e^(Qτ) ≤ (S_K + r_K·J)/c_K on the transient rows, with J the
matrix of ones, and the loss row is exactly the unit row. No library exponential is called;
Rust's `exp` has "unspecified precision". ∎

**Lemma 3 (squaring).** If 0 ≤ L ≤ E ≤ U entrywise then L² ≤ E² ≤ U² entrywise, as every
entry of a product of non-negative matrices is monotone in every entry. Entries of the
stochastic e^(Qτ) are at most 1, so U may be capped at 1. With τ = t/2ˢ, s squarings give
e^(Qt) = (e^(Qτ))^(2ˢ) enclosed. ∎

**Theorem.** The computed [lower, upper] for the whole state's entry of loss in e^(Qt) holds
the chain's exact P(T ≤ t). Nothing in it depends on repair being fast, loss being rare, or
the rates' spread. The generator's own entries (binomial burst probabilities, products of
counts and rates) are enclosed by Lemma 1 too, taking the input rates as exact.

**Width, measured.** Each squaring doubles the relative width and adds a few roundings, so
the width grows as about 2Λt·u (u = 2⁻⁵³): `the_enclosure_is_narrow_across_repair_rates`
measured 2×10⁻¹³ without repair, 1.6×10⁻⁹ with one-hour repair over a year, 1.5×10⁻⁶ at
3.6-second repair and 8.4×10⁻⁴ at 3.6 ms. The series stops once its tail's effect,
r_K·states·2ˢ, is below the least positive normal double, or at K = 170, past which r_K
cannot fall (170! is the largest factorial a double holds). Squarings are at most 1100,
since a finite double is below 2¹⁰²⁴.

**The exponential law, qualified.** A bound that holds for every chain and uses only the
reduced chain of §4.4: let q₀ be the whole state's exit rate and p the chance that a stay in
it ends in loss before the stripe is whole again. The stripe is lost only at the end of such
a doomed stay; departures from the whole state within t number q₀·∫₀ᵗ 1{whole} ds ≤ q₀t in
expectation (the counting process's compensator), and whether a departure is doomed depends
only on the chain after it (strong Markov property), so

  P(T ≤ t) ≤ E[doomed departures by t] ≤ q₀·p·t.

Since M = H/p with H ≥ 1/q₀ the reduced holding time, t/M ≤ q₀pt, and q₀pt/(t/M) = q₀H =
1/π₀, for π₀ the whole state's share of time in the chain restarted at loss. So where the
stripe is whole nearly all the time, the law 1 − e^(−t/M) ≤ t/M lies under a proven bound
that is within 1/π₀ − 1 of it. The test `the_enclosure_is_within_the_renewal_bound` checks
the enclosure against it. Measured against the enclosure at 4% a year and one-hour repair,
the law was high by 1.1×10⁻⁴ (two copies) to 6.9×10⁻⁴ (RS(9,6)); with bursts, by at most
3×10⁻⁶. Without repair (§4.5's zone case) it is high by orders of magnitude for small t,
and low, so optimistic, from about 1.25 M on.

The enclosure bounds the error of solving the chain, not the chain's fidelity to a fleet:
§8 states what the chain leaves out.

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

## 8. Field data for the fleet model (audit B09a)

Compiled 2026-09-30. Each source was fetched and read; tables published as images (Ford
Table 2, [SLM16] Table 5, Backblaze's tables) were read from page renders. What §5 records is
not repeated.

### 8.1 Disks: rates, age, and time between failures

**Schroeder and Gibson, FAST 2007 [SG07]:**

- **Age.** "replacement rates in our data grew constantly with age, an effect often assumed
  not to set in until after a nominal lifetime of 5 years" (abstract, p. 1). "In year 4 and
  year 5 … the actual replacement rates are 7–10 times higher than the failure rates we
  expected based on datasheet MTTF" (§4.2, p. 9).
- **Infant mortality.** Observation 6: "Early onset of wear-out seems to have a much stronger
  impact on lifecycle replacement rates than infant mortality" (p. 9).
- **Autocorrelation.** "The correlation coefficient between consecutive weeks is 0.72, and
  the correlation coefficient between consecutive months is 0.79" (§5.2, p. 11). "we observe
  strong autocorrelation even for large lags in the range of 100 weeks (nearly 2 years)"
  (p. 12). "we determine a Hurst exponent between 0.6-0.8 at the weekly granularity" (p. 12).
- **Time between replacements** (§5.3, p. 13): "we can reject the hypothesis that the
  underlying distribution is exponential or lognormal at a significance level of 0.05". "The
  data has a C2 of 2.4, which is more than two times higher than the C2 of an exponential
  distribution". Weibull shape 0.71–0.76 for HPC1, "a shape parameter less than 1, a clear
  indicator of decreasing hazard rates". These fit the gap between replacements anywhere in
  the cluster, "not the hazard rate of disk lifetime distributions" (§2.4, p. 5).
- **Model input:** none directly; behaviour a constant-rate chain cannot hold (§8.5).

**Pinheiro, Weber, Barroso, FAST 2007 [PWB07]:**

- "The higher baseline AFR for 3 and 4 year old drives is more strongly influenced by the
  underlying reliability of the particular models in that vintage than by disk drive aging
  effects" (§3.1, PDF p. 4).
- "Out of all failed drives, over 56% of them have no count in any of the four strong SMART
  signals … over 36% of all failed drives had zero counts on all variables" (§3.5.6, PDF
  p. 10).
- **Model input:** the chunk rate varies by model and vintage; SMART does not turn most
  failures into planned repair.

**Backblaze Drive Stats for 2025 [BB25]** (published 2026-02-12):

- The formula, from [BB-Q222]: "AFR = ( drive_failures / ( drive_days / 365 )) * 100" — a
  pooled failures-per-drive-year rate.
- "This leaves us with 344,196 drives divided across 30 different drive models". "This year
  finishes strong at 1.36%, down from 1.55% in 2024." "Lifetime AFR is 1.30% this quarter".
  DERIVED: the tables' totals (4,317 failures in 115,638,676 drive days; 19,890 in
  557,699,373) reproduce both under the formula.
- Per model, 2025: highest Toshiba MG08ACA16TEY 6.30% (318 failures in 1,843,841 drive
  days), then Seagate ST10000NM0086 5.66% and ST12000NM0007 5.48%; lowest 0.22%. DERIVED:
  the table has 31 rows, not the text's 30; median 1.35%, 90th percentile 5.31%. The
  three-year table's highest model-year is ST12000NM0007 at 11.38% in 2024; its totals row
  gives 1.57% for 2024, not the text's 1.55%.
- **Model input:** the disk chunk rate. A stripe whose chunks share a model sees that
  model's rate, not the fleet's.

**Jiang, Hu, Zhou, Kanevsky, FAST 2008 [JHZK08]** (about 1.8 million disks, 155,000 shelves,
44 months):

- "in low-end storage systems, the AFR for storage subsystems is about 4.6%, while the AFR
  for disks is only 0.9%, about 20% of overall AFR" (§3, p. 116); physical interconnects are
  "27-68%" of subsystem failures (§1).
- "About 48% of overall storage subsystem failures arrive at the same shelf within 10,000
  seconds of the previous failure" (§5.1, p. 121).
- "for disk failure, the observed empirical P(2) is higher than theoretical P(2) by a factor
  of 6. For other types of storage subsystem failures … by a factor of 10-25" (§5.2.2,
  p. 122).
- **Model input:** the enclosure is a correlated domain; the loss rate of the whole path to a
  chunk exceeds the disk's.

**Bairavasundaram et al., SIGMETRICS 2007 [BGPS07]:** "A total of 3.45% of 1.53 million disks
developed latent sector errors over a period of 32 months" (Table 1); "Latent sector errors
are not independent of each other" (Table 1); "the fraction of disks with errors at the end of
24 months could vary from 5% to 20% for nearline disks" (§4). **Model input:** none; a latent
error found during a rebuild has no state in the chain.

### 8.2 Flash

**Schroeder, Lagisetty, Merchant, FAST 2016 [SLM16]:**

- "for most drive models 6-9% of their population at some point required repairs, there are
  some drive models, e.g. SLC-B and SLC-C, that enter repairs at significantly higher rates
  of 30% and 26%" (§6.3, p. 77). "The vast majority (96%) of drives that go to repairs, go
  there only once in their life" (p. 78).
- "The most common type of non-transparent errors are uncorrectable errors, which affect 2–6
  out of 1,000 drive days" (§3, p. 69).
- DERIVED: the worst four-year replacement fraction in Table 5, 10.31%, is 2.68% a year under
  a constant hazard, 1 − (1 − 0.1031)^(1/4).

**Meza, Wu, Kumar, Mutlu, SIGMETRICS 2015 [MWKM15]:** their failure is not device loss — "We
refer to the occurrence of such uncorrectable errors in an SSD as an SSD failure" (§2), errors
"uncorrectable by the SSD but correctable by the host". "the lifecycle failure rates we
observe with the amount of data written to flash cells does not follow the conventional
bathtub curve" (PDF p. 6). **Model input:** none to the chunk rate; the hazard follows bytes
written, not time.

### 8.3 Recurrence

**Nightingale, Douceur, Orgovan, EuroSys 2011 [NDO11]** (about 950,000 consumer PCs): disk
subsystem crashes, at 5 days' minimum CPU time, "1 in 470" for a first and "1 in 3.4" for a
second given one (Figure 2); "observed failure inter-occurrence times are not exponential and
therefore not memoryless" (§4.5). Consumer machines, and crashes rather than loss.

### 8.4 Correlated events: domains and bursts

**Ford et al., OSDI 2010 [FLP+10]:**

- Table 2: MTTF "Disk 10-50 years", "Node 4.3 months", "Rack 10.2 years", of unavailability:
  "The vast majority of such unavailability events are transient and do not result in
  permanent data loss" (§2.1).
- "we observe that 37% of failures are part of a burst of at least 2 nodes" (§4.1, window
  120 s). Steep bursts follow "a power outage in a datacenter" (§4.2). "All our failure
  bursts of more than 20 nodes have rack affinity greater than 0.7, and those of more than 40
  nodes have affinity at least 0.9" (§4.3); high affinity also comes from "a bad batch of
  components or new storage node binary or kernel".
- Burst classes "small (0-0.001), medium (0.001-0.01), large (0.01-0.1)" of all nodes
  (Figure 10); "for all encodings except R = 1, large failure bursts are the biggest
  contributor to unavailability" (§5.2). Burst rates are not published.
- The 120 s window "is less than a tenth of the average time it takes our system to recover
  a chunk" (§4.1). DERIVED: mean chunk recovery over 20 minutes.

**Dean, LADIS 2009 keynote [Dea09], slide 10:** "Typical first year for a new cluster: ~0.5
overheating (power down most machines in <5 mins, ~1-2 days to recover) ~1 PDU failure
(~500-1000 machines suddenly disappear, ~6 hours to come back) ~1 rack-move … ~20 rack
failures (40-80 machines instantly disappear, 1-6 hours to get back) … ~1000 individual
machine failures ~thousands of hard drive failures". Slide 7: "Cluster (30+ racks)". All
transient. The list is not in *The Datacenter as a Computer*, 3rd edition [BHR18]; whether
the 1st or 2nd edition has it: UNVERIFIED.

**Shvachko, Kuang, Radia, Chansler, MSST 2010 [SKRC10]** (§IV.A): "about 0.8 percent of nodes
fail each month"; "one-half to one percent of the nodes will not survive a full power-on
restart. Statistically, and in practice, a large cluster will lose a handful of blocks during
a power-on restart."

**Chansler, ;login: 37(1), 2012 [Cha12]:** "(One percent of nodes fail each month.)" (p. 19);
on about 4,000 nodes "a few dozen nodes will not immediately restart" (p. 21). Cidon et al.
write that such outages "occur once or twice per year in a given data center [7]" (ATC 2013,
§2) citing this article, which contains no such frequency: UNVERIFIED at its cited source.

**Gunawi et al., SoCC 2016 [GHS+16]:** "POWER failures represent 6% of outages in our study"
(§5.7); outage impacts include "data loss (2%)" (§6). Shares of outages, not rates.

### 8.5 Analysis, and what remains unqualified

- **The disk rate depends on the statistic.** The fleet's 1.30–1.36% is a mean over
  drive-days; models range from 0.22% to 6.30%, one model-year reached 11.38%, and [SG07]
  saw up to 13%. mantle's default takes the worst current model (docs/design/durability.md
  §5).
- **Every correlated event with a stated size is transient** — Ford's racks, Dean's racks
  and PDUs. The only permanent correlated loss with a stated size is the power-on restart,
  0.5–1% of nodes [SKRC10]; its frequency is UNVERIFIED (§8.4).
- **No fetched primary source gives a permanent-loss rate for a rack, a zone or a site.**

What the constant-rate chain does not hold, and the direction of its error where the sources
show one:

1. Hazard rising with age [SG07], [BB25]: optimistic for an aging fleet; the worst-model
   default is pessimistic for a young one.
2. Clustering in time — Weibull shape 0.71–0.76, C² 2.4, two failures within an hour four
   times likelier than exponential, Hurst 0.6–0.8 [SG07]: optimistic for a second loss during
   rebuild. No source gives a correction for a stripe spread over domains.
3. Shelf correlation, P(2) 6 times the independent value for disks and 10–25 for other
   failures [JHZK08]: optimistic unless the enclosure is a domain.
4. Recurrence after repair [NDO11] against 96% single repair visits [SLM16]: the sources
   conflict; optimistic if a returned device counts as new.
5. Burst rate and size distribution: unpublished [FLP+10], frequency UNVERIFIED; direction
   unknown.
6. Repair time distribution: serial and exponential in the chain; evidence is a mean over
   20 minutes [FLP+10] and "a day—or more" after a power-on restart [Cha12]; optimistic after
   bursts, when recovery queues.
7. Latent sector errors during rebuild [BGPS07]: no state; optimistic.
8. Flash hazard following bytes written [MWKM15]: direction unknown.
9. Site disasters: no rate; optimistic.
10. Controller, cable and path failures, 27–68% of subsystem failures [JHZK08]: optimistic if
    the chunk rate is a drive AFR alone.

---

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
- [Jen53] A. Jensen. "Markoff chains as an aid in the study of Markoff processes."
  Skandinavisk Aktuarietidskrift 36:87–91, 1953. (Not fetched.)
- [Gra77] W. K. Grassmann. "Transient solutions in Markovian queueing systems." Computers &
  Operations Research 4(1):47–53, 1977. doi:10.1016/0305-0548(77)90007-7 (Not fetched.)
- [Ste94] W. J. Stewart. *Introduction to the Numerical Solution of Markov Chains.* Princeton
  University Press, 1994, §8.4. (Not fetched.)
- [IEEE754] IEEE Std 754-2019, "IEEE Standard for Floating-Point Arithmetic", §4.3.1.
- [Rust-f64] Rust 1.98 standard library, `f64::next_up`, `f64::next_down`, `f64::exp`.
  https://doc.rust-lang.org/1.98.0/std/primitive.f64.html
- [BB25] Backblaze. "Backblaze Drive Stats for 2025." 2026-02-12.
  https://www.backblaze.com/blog/backblaze-drive-stats-for-2025/
- [BB-Q222] Backblaze. "Backblaze Drive Stats for Q2 2022", "Computing the Annualized Failure
  Rate". https://www.backblaze.com/blog/backblaze-drive-stats-for-q2-2022/
- [JHZK08] W. Jiang, C. Hu, Y. Zhou, A. Kanevsky. "Are Disks the Dominant Contributor for
  Storage Failures?" FAST 2008, pp. 111–125.
  https://www.usenix.org/legacy/events/fast08/tech/full_papers/jiang/jiang.pdf
- [BGPS07] L. N. Bairavasundaram, G. R. Goodson, S. Pasupathy, J. Schindler. "An Analysis of
  Latent Sector Errors in Disk Drives." SIGMETRICS 2007.
  https://research.cs.wisc.edu/wind/Publications/latent-sigmetrics07.pdf
- [MWKM15] J. Meza, Q. Wu, S. Kumar, O. Mutlu. "A Large-Scale Study of Flash Memory Failures
  in the Field." SIGMETRICS 2015. doi:10.1145/2745844.2745848
- [NDO11] E. B. Nightingale, J. R. Douceur, V. Orgovan. "Cycles, Cells and Platters: An
  Empirical Analysis of Hardware Failures on a Million Consumer PCs." EuroSys 2011.
- [Dea09] J. Dean. "Designs, Lessons and Advice from Building Large Distributed Systems."
  LADIS 2009 keynote. https://www.cs.cornell.edu/projects/ladis2009/talks/dean-keynote-ladis2009.pdf
- [BHR18] L. A. Barroso, U. Hölzle, P. Ranganathan. *The Datacenter as a Computer*, 3rd ed.
  Morgan & Claypool, 2018. doi:10.2200/S00874ED3V01Y201809CAC046
- [SKRC10] K. Shvachko, H. Kuang, S. Radia, R. Chansler. "The Hadoop Distributed File
  System." MSST 2010.
- [Cha12] R. J. Chansler. "Data Availability and Durability with the Hadoop Distributed File
  System." ;login: 37(1), 2012.
  https://www.usenix.org/system/files/login/articles/chansler_0.pdf
- [CRS+13] A. Cidon, S. Rumble, R. Stutsman, S. Katti, J. Ousterhout, M. Rosenblum.
  "Copysets: Reducing the Frequency of Data Loss in Cloud Storage." USENIX ATC 2013.
- [GHS+16] H. S. Gunawi et al. "Why Does the Cloud Stop Computing? Lessons from Hundreds of
  Service Outages." SoCC 2016.
