# Measuring a point

Status: design, 2026-09-29. Sources: docs/research/21 (benchmark states, cited as "21 §x"),
docs/research/11 (operating parameters, "11 §x"); the measurements in
docs/measurements/2026-09-29-chunk-store-states.md.

A benchmark point, such as puts of one chunk size at one number of requests in flight, is
measured in rounds, each giving one throughput. This record states how many rounds a point
runs, how its rounds are judged, and what its report states. `mantle_disk::rounds` implements
it, and `mantle bench chunk` reports through it.

## 1. Why a mean and its interval are not enough

On this machine a point's rounds are often not one distribution around one value. Reads at
one request in flight run at about 15,000 a second in some rounds and a tenth of that in
others, with almost nothing between: the drive answers a stream of full flushes by stalling
every read for about a second at a time, and a one-second round catches a stall or misses it
(measurements, findings 1 and 2). A mean of such rounds lies between the two states and
describes neither, and its interval can be narrow while the mean represents nothing (21 §6.1,
Hoefler and Belli's Fig. 3). The rounds also depend on each other: each round's puts change the
state the next round's reads start in, and the drive stays slow for tens of seconds after a
large write (measurements, finding 3). "If measurements are not i.i.d., the variance and
confidence interval estimates will be biased" (Kalibera and Jones, 21 §2.1).

## 2. Order in time

**Decision: before any interval is stated, a point's rounds are checked for order in time, and
rounds found ordered carry no interval.**

- **Two checks** (21 §4). The lag-1 autocorrelation against `±1.96/√n`, the bound for
  independent data in Le Boudec's §2.3.2 and the NIST/SEMATECH e-Handbook §1.3.3.1; and the
  number of runs above and below the median against its exact distribution, rejecting in either
  2.5% tail (e-Handbook §1.3.5.13; the distribution in 21 §4). Rounds equal to the median are
  left out of the runs.
- **Persistent or alternating.** Too few runs, or a correlation above the bound, means rounds
  stay in a state, as the drive does after a large write. Too many, or a correlation below it,
  means they alternate, as reads do when a stall recurs every few rounds; Kalibera and Jones
  name both patterns among the dependencies they found (21 §2.3).
- **From ten rounds.** Below ten the runs test has no two-sided 5% rejection region (21 §4), so
  fewer rounds are reported as untested.
- Two checks at 5% each mark at most about one point in ten whose rounds are in fact
  independent; the cost of a false mark is an interval not stated.

## 3. One state or two

**Decision: whether a point's rounds fall in one state or two is Hartigan and Hartigan's dip
test at the 5% level; two states are split where the within-group sum of squares is least.**

- **Why the dip** (21 §5). It is distribution-free, and the only test found with published null
  percentage points from four observations up: Hartigan and Hartigan's Table 1, from 9,999
  uniform samples for n = 4–10, 15, 20, 30, 50, 100 and 200, interpolated linearly on `√n·dip`
  between them as its note (4) directs. The alternatives fail at tens of rounds: the mixture
  likelihood ratio "does not provide a reliable method for detecting bimodality" below 50
  observations, the bimodality coefficient has no null distribution, and a comparison of one-
  and two-component Gaussian fits called 81% of unimodal samples bimodal (21 §5.5–§5.7).
- **The computation** is their §4 algorithm, steps (i)–(vii): the greatest convex minorant and
  least concave majorant of the empirical distribution over an interval that narrows until the
  gap between them is no larger than the distance already found. Tied values keep their own
  corners of the distribution function, so the jump at the mode may divide them, as the
  definition allows. It agrees exactly with R's `diptest` and with a linear program that
  minimizes the distance over unimodal distribution functions directly, on 5,000 samples of 1
  to 120 values, tied and untied; seventeen of them are the reference values of its tests.
- **What it can see.** A minority state is visible only when it holds more than twice the
  critical dip of the rounds: about 28% of ten rounds, 21% of twenty, 18% of thirty (21 §5.3).
- **The split** takes the cut between consecutive sorted values with the least within-group sum
  of squares, only between unequal values. No separation index is computed from it: splitting
  one normal distribution at its mean gives halves 2.65 standard deviations apart (21 §5.8).
- A third state is not looked for: tens of rounds cannot show one (21 §9.3).

## 4. Intervals

**Decision: each state is summarized by its median, with the order-statistic interval of Le
Boudec's Theorem 2.1, and by its share of the rounds, with the interval of his Theorem 2.4.**

- **The median, not the mean.** Throughput rounds are rarely normal (710 of 713 configurations
  in Maricq et al.; 21 §6.3), and Hoefler and Belli's Rule 6 asks for no assumption of normality
  without a check (21 §6.1). The median's interval needs only independent rounds.
- **The ranks** are equal-tailed: `j` is the largest rank whose tail below, `B(j−1)` with `B` the
  binomial distribution function at `p = 1/2`, holds at most 2.5%, and `k = n + 1 − j`, so the
  coverage `B(k−1) − B(j−1)` is at least 95%. Ten rounds give `[x_(2), x_(9)]`, Le Boudec's own
  example. Fewer than six rounds give none (his Table A.1), and such a state is reported with
  its median and range.
- **Shares** of two states are Clopper and Pearson's interval, his Theorem 2.4; 32 of 145 give
  15.6–29.7%, his Example 2.4. The interval does not allow for the split having been chosen
  from the same rounds, so it is narrower than it should be (21 §9.8).

## 5. How many rounds

**Decision: a point runs at least ten rounds and at most thirty, and stops before thirty once
its rounds are independent and every state's interval lies within ±5% of its median.**

- **Ten** is the fewest at which order in time can be tested (§2).
- **Thirty** lets each of two states carry its own interval when the smaller holds a fifth of
  the rounds (six rounds), and lets the dip see a state of that size (21 §9.5). `--rounds` sets
  another limit, from ten to 120: `u128` holds the exact binomial sums of §2 and §4 up to 120.
- **±5%** is half the ±10% that comparisons between benchmark runs resolve (11 §16), as
  before. Stopping when an interval is narrow enough is justified only as intervals narrow
  (Chow and Robbins; 21 §6.5), which is one more reason for the minimum.
- At three one-second steps a round, sixteen points take from about eight minutes, when every
  point settles in ten rounds, to about half an hour, when none does: 33 minutes on this
  machine, where only six of 48 point-operations settled (measurements, finding 5).

## 6. What a point reports

For each operation of each point: a row for each state with its median throughput, the wider
side of the median's interval as a share of the median, its rounds out of the point's, the
share's interval when there are two states, and latency quantiles over the state's own
transfers; and, on the first row, whether the rounds persist or alternate. A state without an
interval shows a dash. No mean or coefficient of variation is reported (21 §9.6).

## 7. Open

- **Where states change in time.** Rounds found ordered are reported without an interval; the
  rounds at which a state begins and ends are not located. Changepoint segmentation, as Barrett
  et al. classify warmup, needs a penalty calibrated on the device (21 §3, §9.3 step 2).
- **Repetition above the round.** Kalibera and Jones set the repetitions at each level from a
  dimensioning experiment, and an interval covers only the variation of the levels repeated
  (21 §2.4–§2.5). One run of four fresh volumes of twenty rounds each, in one process, gives by
  their Eq. 3 an optimum of 2 to 11 rounds a volume for 13 of 18 point-operations, and no bound
  for the other 5, whose variance between volumes estimates at or below zero. But 30 of its 72
  series of twenty rounds are ordered in time, where independent rounds would give at most
  about seven,
  and the method requires independent rounds, so the counts are recorded rather than adopted
  (measurements, finding 4). A report from one volume says nothing of the variation
  between volumes, which for some points is as large as that between rounds. The full
  experiment, across processes, is about eight hours of writing per device (21 §9.2).
- **Calibration** still judges its points by the mean's t-interval over three to six
  half-second rounds, what its budget of tens of seconds allows; the judgment here needs ten
  rounds a point.
